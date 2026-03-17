"""
实时数据推送服务

基于WebSocket的实时数据推送服务，支持多客户端连接和数据分发。
"""

import asyncio
import importlib
import importlib.util
import json
import logging
import os
import time
import zlib
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

_SH = ZoneInfo('Asia/Shanghai')
from typing import Any, Callable, Optional, cast

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from websockets.legacy.server import WebSocketServerProtocol

from .config.settings import RealtimeDataConfig
from .aggregator.minute_bar_aggregator import MinuteBarAggregator
from .monitor.metrics_collector import MetricPoint, MetricsCollector
from .persistence.duckdb_sink import RealtimeDuckDBSink
from .recovery_manager import ClientRecoveryManager
from .unified_api import UnifiedDataAPI

orjson: Any = None
msgpack: Any = None
try:
    if importlib.util.find_spec("orjson") is not None:
        orjson = importlib.import_module("orjson")
except Exception:
    orjson = None
try:
    if importlib.util.find_spec("msgpack") is not None:
        msgpack = importlib.import_module("msgpack")
except Exception:
    msgpack = None


@dataclass
class ClientInfo:
    """客户端信息"""
    client_id: str
    websocket: WebSocketServerProtocol
    subscriptions: set[str]
    connect_time: float
    last_ping: float
    user_agent: str = ""
    remote_address: str = ""
    batch_mode: bool = False
    prefer_binary: bool = False
    max_batch_items: int = 200
    protocol: str = "json"
    compress: bool = False
    compress_threshold: int = 1024
    session_id: str = ""
    last_event_ts: float = 0.0


@dataclass
class PushMessage:
    """推送消息"""
    message_type: str
    data: Any
    timestamp: float
    client_ids: Optional[list[str]] = None  # None表示广播给所有客户端


class RealtimeDataPushService:
    """实时数据推送服务

    提供WebSocket服务，支持客户端订阅和实时数据推送。
    """

    def __init__(self, config: Optional[RealtimeDataConfig] = None):
        """初始化推送服务

        Args:
            config: 配置对象
        """
        self.config = config or RealtimeDataConfig()
        self.logger = logging.getLogger(__name__)

        # WebSocket配置
        ws_config = self.config.get_websocket_config()
        self.host = ws_config.get('host', 'localhost')
        self.port = ws_config.get('port', 8765)
        self.max_connections = ws_config.get('max_connections', 100)
        self.batch_push = ws_config.get('batch_push', False)
        self.max_batch_items = ws_config.get('max_batch_items', 200)
        self.serializer = ws_config.get('serializer', 'json')
        self.binary_mode = ws_config.get('binary', False)
        self.compress = ws_config.get('compress', False)
        self.compress_threshold = ws_config.get('compress_threshold', 1024)
        self.stale_timeout = ws_config.get('stale_timeout', 120)
        self.heartbeat_interval = ws_config.get('heartbeat_interval', 30)
        self.reconnect_backoff = ws_config.get('reconnect_backoff', [1, 2, 5])
        self.recovery_ttl = ws_config.get('recovery_ttl', 300)
        self.recovery_max_sessions = ws_config.get('recovery_max_sessions', 10000)
        self.recovery_cache_size = ws_config.get('recovery_cache_size', 2000)
        self.recovery_cache_window = ws_config.get('recovery_cache_window', 30)
        self._serializers: dict[str, Callable[[dict[str, Any]], object]] = {}
        self.recovery_manager = ClientRecoveryManager(
            ttl_seconds=self.recovery_ttl,
            max_sessions=self.recovery_max_sessions
        )
        self.recovery_cache: deque[dict[str, Any]] = deque(maxlen=self.recovery_cache_size)

        # 调度器配置
        scheduler_config = self.config.get_scheduler_config()
        self.update_interval = scheduler_config.get('update_interval', 3)
        self.batch_size = scheduler_config.get('batch_size', 50)

        # 数据API
        self.data_api = UnifiedDataAPI(config)

        # B+C: 实时落库、分钟聚合、质量指标
        self.rt_persist_enabled = os.environ.get("EASYXT_RT_PERSIST", "1") in ("1", "true", "True")
        self.watermark_seconds = int(float(os.environ.get("EASYXT_RT_WATERMARK_SECONDS", "5")))
        self.recompute_minutes = int(float(os.environ.get("EASYXT_RT_RECOMPUTE_MINUTES", "3")))
        self.finalize_interval_s = int(float(os.environ.get("EASYXT_RT_FINALIZE_INTERVAL_S", "12")))
        self.miss_rate_interval_s = int(float(os.environ.get("EASYXT_RT_MISS_RATE_INTERVAL_S", "30")))
        self._last_finalize_ts = 0.0
        self._last_miss_rate_ts = 0.0
        self._last_purge_ts = 0.0
        self._purge_interval_s = int(float(os.environ.get("EASYXT_RT_PURGE_INTERVAL_S", "3600")))
        self._purge_retention_days = int(float(os.environ.get("EASYXT_RT_PURGE_RETENTION_DAYS", "7")))
        self.rt_sink: Optional[RealtimeDuckDBSink] = None
        self.rt_aggregator: Optional[MinuteBarAggregator] = None
        if self.rt_persist_enabled:
            try:
                self.rt_sink = RealtimeDuckDBSink()
                self.rt_sink.ensure_tables()
                self.rt_aggregator = MinuteBarAggregator()
            except Exception as e:
                self.logger.error(f"实时落库初始化失败，将继续仅推送模式: {e}")
                self.rt_persist_enabled = False

        self.metrics_collector = MetricsCollector(collection_interval=60)

        # 客户端管理
        self.clients: dict[str, ClientInfo] = {}
        self.subscriptions: dict[str, set[str]] = {}  # symbol -> client_ids

        # 服务状态
        self.server = None
        self.is_running = False
        self.update_task = None
        self.heartbeat_task = None

        # 消息队列
        self.message_queue: asyncio.Queue = asyncio.Queue()
        self._consecutive_failures = 0

        # 统计信息
        self.stats: dict[str, Any] = {
            'total_connections': 0,
            'active_connections': 0,
            'messages_sent': 0,
            'errors': 0,
            'start_time': 0.0
        }

    async def start_server(self):
        """启动WebSocket服务器"""
        try:
            # 连接数据源
            self.logger.info("连接数据源...")
            connect_results = self.data_api.connect_all()
            available_sources = sum(1 for success in connect_results.values() if success)

            if available_sources == 0:
                self.logger.warning("没有可用的数据源，服务将以有限功能启动")
            else:
                self.logger.info(f"成功连接 {available_sources} 个数据源")

            # 启动WebSocket服务器
            handler = cast(Any, self.handle_client)
            self.server = await websockets.serve(
                handler,
                self.host,
                self.port,
                max_size=1024*1024,  # 1MB
                ping_interval=30,
                ping_timeout=10
            )

            self.is_running = True
            self.stats['start_time'] = time.time()

            # 启动数据更新任务
            self.update_task = asyncio.create_task(self.data_update_loop())

            # 启动消息处理任务
            asyncio.create_task(self.message_processor())
            self.heartbeat_task = asyncio.create_task(self.heartbeat_loop())

            self.logger.info(f"实时数据推送服务已启动: ws://{self.host}:{self.port}")

        except Exception as e:
            self.logger.error(f"启动服务器失败: {e}")
            raise

    async def stop_server(self):
        """停止WebSocket服务器"""
        self.is_running = False

        # 停止数据更新任务
        if self.update_task:
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                pass
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass

        # 关闭所有客户端连接
        if self.clients:
            await asyncio.gather(
                *[self.disconnect_client(client_id) for client_id in list(self.clients.keys())],
                return_exceptions=True
            )

        # 关闭服务器
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        # 断开数据源连接
        self.data_api.disconnect_all()

        self.logger.info("实时数据推送服务已停止")

    async def handle_client(self, websocket: WebSocketServerProtocol, path: Optional[str] = None):
        """处理客户端连接

        Args:
            websocket: WebSocket连接
            path: 请求路径
        """
        client_id = f"client_{int(time.time() * 1000)}_{id(websocket)}"

        # 检查连接数限制
        if len(self.clients) >= self.max_connections:
            await websocket.close(code=1013, reason="服务器连接数已满")
            return

        # 创建客户端信息
        client_info = ClientInfo(
            client_id=client_id,
            websocket=websocket,
            subscriptions=set(),
            connect_time=time.time(),
            last_ping=time.time(),
            remote_address=f"{websocket.remote_address[0]}:{websocket.remote_address[1]}",
            batch_mode=self.batch_push,
            prefer_binary=self.binary_mode,
            max_batch_items=self.max_batch_items,
            protocol=self.serializer,
            compress=self.compress,
            compress_threshold=self.compress_threshold
        )
        session_id = self.recovery_manager.create_session(client_id, client_info)
        client_info.session_id = session_id

        # 注册客户端
        self.clients[client_id] = client_info
        self.stats['total_connections'] += 1
        self.stats['active_connections'] = len(self.clients)

        self.logger.info("客户端连接: %s from %s", client_id, client_info.remote_address)

        # 发送欢迎消息
        await self.send_to_client(client_id, {
            'type': 'welcome',
            'client_id': client_id,
            'server_time': time.time(),
            'available_sources': self.data_api.get_available_providers(),
            'session_id': session_id,
            'recovery_ttl': self.recovery_ttl
        })

        try:
            # 处理客户端消息
            async for message in websocket:
                if isinstance(message, bytes):
                    message = message.decode("utf-8", errors="ignore")
                client_info.last_ping = time.time()
                await self.handle_client_message(client_id, message)

        except ConnectionClosed:
            self.logger.info(f"客户端断开连接: {client_id}")
        except WebSocketException as e:
            self.logger.warning(f"WebSocket异常: {client_id} - {e}")
        except Exception as e:
            self.logger.error(f"处理客户端消息异常: {client_id} - {e}")
        finally:
            # 清理客户端
            await self.disconnect_client(client_id)

    async def handle_client_message(self, client_id: str, message: str):
        """处理客户端消息

        Args:
            client_id: 客户端ID
            message: 消息内容
        """
        try:
            data = json.loads(message)
            msg_type = data.get('type')

            if msg_type == 'subscribe':
                # 订阅股票
                symbols = data.get('symbols', [])
                options = data.get('options', {}) or {}
                self._update_client_options(client_id, options)
                await self.subscribe_symbols(client_id, symbols)

            elif msg_type == 'unsubscribe':
                # 取消订阅
                symbols = data.get('symbols', [])
                await self.unsubscribe_symbols(client_id, symbols)

            elif msg_type == 'get_quotes':
                # 获取实时行情
                symbols = data.get('symbols', [])
                await self.send_quotes(client_id, symbols)

            elif msg_type == 'get_hot_stocks':
                # 获取热门股票
                count = data.get('count', 50)
                await self.send_hot_stocks(client_id, count)

            elif msg_type == 'get_concepts':
                # 获取概念数据
                count = data.get('count', 50)
                await self.send_concepts(client_id, count)

            elif msg_type == 'ping':
                # 心跳检测
                client_info = self.clients.get(client_id)
                if client_info:
                    client_info.last_ping = time.time()
                await self.send_to_client(client_id, {'type': 'pong', 'timestamp': time.time()})

            elif msg_type == 'get_status':
                # 获取服务状态
                await self.send_server_status(client_id)
            elif msg_type == 'set_options':
                options = data.get('options', {}) or {}
                self._update_client_options(client_id, options)
                await self.send_to_client(client_id, {
                    'type': 'options_updated',
                    'options': options,
                    'timestamp': time.time()
                })
                self._update_recovery_snapshot(client_id)
            elif msg_type == 'resume':
                session_id = data.get('session_id')
                await self._resume_client(client_id, session_id)

            else:
                await self.send_to_client(client_id, {
                    'type': 'error',
                    'message': f'未知消息类型: {msg_type}'
                })

        except json.JSONDecodeError:
            await self.send_to_client(client_id, {
                'type': 'error',
                'message': '消息格式错误，请发送有效的JSON'
            })
        except Exception as e:
            self.logger.error(f"处理客户端消息失败: {client_id} - {e}")
            await self.send_to_client(client_id, {
                'type': 'error',
                'message': f'处理消息失败: {str(e)}'
            })

    async def subscribe_symbols(self, client_id: str, symbols: list[str]):
        """订阅股票代码

        Args:
            client_id: 客户端ID
            symbols: 股票代码列表
        """
        client_info = self.clients.get(client_id)
        if not client_info:
            return

        # 添加订阅
        for symbol in symbols:
            client_info.subscriptions.add(symbol)

            if symbol not in self.subscriptions:
                self.subscriptions[symbol] = set()
            self.subscriptions[symbol].add(client_id)

        self.logger.info(f"客户端 {client_id} 订阅: {symbols}")

        # 发送确认消息
        await self.send_to_client(client_id, {
            'type': 'subscribe_success',
            'symbols': symbols,
            'total_subscriptions': len(client_info.subscriptions)
        })

        # 立即发送当前行情
        await self.send_quotes(client_id, symbols)
        self._update_recovery_snapshot(client_id)

    async def unsubscribe_symbols(self, client_id: str, symbols: list[str]):
        """取消订阅股票代码

        Args:
            client_id: 客户端ID
            symbols: 股票代码列表
        """
        client_info = self.clients.get(client_id)
        if not client_info:
            return

        # 移除订阅
        for symbol in symbols:
            client_info.subscriptions.discard(symbol)

            if symbol in self.subscriptions:
                self.subscriptions[symbol].discard(client_id)
                if not self.subscriptions[symbol]:
                    del self.subscriptions[symbol]

        self.logger.info(f"客户端 {client_id} 取消订阅: {symbols}")

        # 发送确认消息
        await self.send_to_client(client_id, {
            'type': 'unsubscribe_success',
            'symbols': symbols,
            'total_subscriptions': len(client_info.subscriptions)
        })
        self._update_recovery_snapshot(client_id)

    async def send_quotes(self, client_id: str, symbols: list[str]):
        """发送实时行情数据

        Args:
            client_id: 客户端ID
            symbols: 股票代码列表
        """
        try:
            quotes = self.data_api.get_realtime_quotes(symbols)

            await self.send_to_client(client_id, {
                'type': 'quotes',
                'data': quotes,
                'timestamp': time.time(),
                'count': len(quotes)
            })

        except Exception as e:
            self.logger.error(f"发送行情数据失败: {e}")
            await self.send_to_client(client_id, {
                'type': 'error',
                'message': f'获取行情数据失败: {str(e)}'
            })

    async def send_hot_stocks(self, client_id: str, count: int):
        """发送热门股票数据

        Args:
            client_id: 客户端ID
            count: 获取数量
        """
        try:
            hot_stocks = self.data_api.get_hot_stocks(count=count)

            await self.send_to_client(client_id, {
                'type': 'hot_stocks',
                'data': hot_stocks,
                'timestamp': time.time(),
                'count': len(hot_stocks)
            })

        except Exception as e:
            self.logger.error(f"发送热门股票失败: {e}")
            await self.send_to_client(client_id, {
                'type': 'error',
                'message': f'获取热门股票失败: {str(e)}'
            })

    async def send_concepts(self, client_id: str, count: int):
        """发送概念数据

        Args:
            client_id: 客户端ID
            count: 获取数量
        """
        try:
            concepts = self.data_api.get_concept_data(count=count)

            await self.send_to_client(client_id, {
                'type': 'concepts',
                'data': concepts,
                'timestamp': time.time(),
                'count': len(concepts)
            })

        except Exception as e:
            self.logger.error(f"发送概念数据失败: {e}")
            await self.send_to_client(client_id, {
                'type': 'error',
                'message': f'获取概念数据失败: {str(e)}'
            })

    async def send_server_status(self, client_id: str):
        """发送服务器状态

        Args:
            client_id: 客户端ID
        """
        try:
            health = self.data_api.health_check()

            status = {
                'server_stats': self.stats.copy(),
                'data_sources': health,
                'active_subscriptions': len(self.subscriptions),
                'uptime': time.time() - self.stats['start_time'] if self.stats['start_time'] > 0 else 0
            }

            await self.send_to_client(client_id, {
                'type': 'server_status',
                'data': status,
                'timestamp': time.time()
            })

        except Exception as e:
            self.logger.error(f"发送服务器状态失败: {e}")

    async def send_to_client(
        self,
        client_id: str,
        message: dict[str, Any],
        payload: Optional[object] = None,
        event_ts: Optional[float] = None
    ):
        """发送消息给指定客户端

        Args:
            client_id: 客户端ID
            message: 消息内容
        """
        client_info = self.clients.get(client_id)
        if not client_info:
            return

        try:
            send_payload = payload if payload is not None else self._serialize_message(
                message,
                client_info=client_info
            )
            await client_info.websocket.send(cast(Any, send_payload))
            self.stats['messages_sent'] += 1
            client_info.last_ping = time.time()
            if event_ts is not None:
                client_info.last_event_ts = max(client_info.last_event_ts, event_ts)

        except ConnectionClosed:
            self.logger.info(f"客户端已断开连接: {client_id}")
            await self.disconnect_client(client_id)
        except Exception as e:
            self.logger.error(f"发送消息失败: {client_id} - {e}")
            self.stats['errors'] += 1

    async def broadcast_message(
        self,
        message: dict[str, Any],
        client_ids: Optional[list[str]] = None,
        event_ts: Optional[float] = None
    ):
        """广播消息

        Args:
            message: 消息内容
            client_ids: 目标客户端ID列表，None表示广播给所有客户端
        """
        target_clients = client_ids or list(self.clients.keys())

        tasks = []
        payload_cache: dict[tuple[str, bool, bool, int], object] = {}
        for client_id in target_clients:
            client_info = self.clients.get(client_id)
            if not client_info:
                continue
            key = (
                client_info.protocol,
                client_info.prefer_binary,
                client_info.compress,
                client_info.compress_threshold
            )
            payload = payload_cache.get(key)
            if payload is None:
                payload = self._serialize_message(message, client_info=client_info)
                payload_cache[key] = payload
            tasks.append(self.send_to_client(client_id, message, payload, event_ts=event_ts))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def disconnect_client(self, client_id: str):
        """断开客户端连接

        Args:
            client_id: 客户端ID
        """
        client_info = self.clients.get(client_id)
        if not client_info:
            return
        self._update_recovery_snapshot(client_id)

        # 清理订阅
        for symbol in client_info.subscriptions:
            if symbol in self.subscriptions:
                self.subscriptions[symbol].discard(client_id)
                if not self.subscriptions[symbol]:
                    del self.subscriptions[symbol]

        # 移除客户端
        del self.clients[client_id]
        self.stats['active_connections'] = len(self.clients)
        self.recovery_manager.remove_client(client_id)

        # 关闭WebSocket连接
        try:
            await client_info.websocket.close()
        except Exception:
            pass

        self.logger.info(f"客户端已清理: {client_id}")

    async def data_update_loop(self):
        """数据更新循环"""
        self.logger.info("数据更新循环已启动")

        while self.is_running:
            try:
                # 获取所有订阅的股票代码
                all_symbols = list(self.subscriptions.keys())
                batch_enabled = self.batch_push or any(
                    client_info.batch_mode for client_info in self.clients.values()
                )

                if all_symbols:
                    client_batches: dict[str, list[dict[str, Any]]] = {}
                    # 分批获取数据
                    for i in range(0, len(all_symbols), self.batch_size):
                        batch_symbols = all_symbols[i:i + self.batch_size]

                        # 获取实时行情
                        quotes = self.data_api.get_realtime_quotes(batch_symbols)

                        if quotes:
                            self._handle_realtime_persistence_and_metrics(quotes)
                            if batch_enabled:
                                batch_timestamp = time.time()
                                for quote in quotes:
                                    symbol = quote.get('symbol')
                                    if symbol and symbol in self.subscriptions:
                                        subscriber_ids = self.subscriptions[symbol]
                                        for client_id in subscriber_ids:
                                            if client_id in self.clients:
                                                client_batches.setdefault(client_id, []).append(quote)
                                    self._append_recovery_event(quote, batch_timestamp)
                            else:
                                for quote in quotes:
                                    symbol = quote.get('symbol')
                                    if symbol and symbol in self.subscriptions:
                                        event_ts = time.time()
                                        subscriber_ids = list(self.subscriptions[symbol])
                                        message = {
                                            'type': 'realtime_update',
                                            'symbol': symbol,
                                            'data': quote,
                                            'timestamp': event_ts
                                        }
                                        await self.broadcast_message(message, subscriber_ids, event_ts=event_ts)
                                        self._append_recovery_event(quote, event_ts)
                    if batch_enabled and client_batches:
                        await self._flush_client_batches(client_batches)
                    self._consecutive_failures = 0

                # 等待下次更新
                await asyncio.sleep(self.update_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"数据更新循环异常: {e}")
                self._consecutive_failures += 1
                # 熔断：连续失败超过上限，停止重连，仅低频采集
                max_failures = int(os.environ.get("EASYXT_RT_MAX_CONSECUTIVE_FAILURES", "20"))
                if self._consecutive_failures >= max_failures:
                    self.logger.error(
                        f"连续失败 {self._consecutive_failures} 次，熔断触发，"
                        f"停止重连，等待 60s 后恢复"
                    )
                    await asyncio.sleep(60)
                    self._consecutive_failures = 0  # 重置后重新尝试
                else:
                    await self._maybe_reconnect_sources()
                await asyncio.sleep(1)

        self.logger.info("数据更新循环已停止")

    async def message_processor(self):
        """消息处理器"""
        while self.is_running:
            try:
                # 从队列获取消息
                message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)

                # 处理消息
                await self.broadcast_message(message.data, message.client_ids)

            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"消息处理异常: {e}")

    async def heartbeat_loop(self):
        while self.is_running:
            try:
                now = time.time()
                stale_clients = []
                for client_id, client_info in list(self.clients.items()):
                    if self.stale_timeout > 0 and (now - client_info.last_ping) > self.stale_timeout:
                        stale_clients.append(client_id)
                if stale_clients:
                    await asyncio.gather(
                        *[self.disconnect_client(cid) for cid in stale_clients],
                        return_exceptions=True
                    )
                self.recovery_manager.cleanup_expired()
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"心跳检查异常: {e}")
                await asyncio.sleep(self.heartbeat_interval)

    def _get_serializer(self, protocol: str) -> Callable[[dict[str, Any]], object]:
        cached_serializer = self._serializers.get(protocol)
        if cached_serializer is not None:
            return cached_serializer
        serializer_fn: Callable[[dict[str, Any]], object]
        if protocol == 'orjson' and orjson:
            serializer_fn = cast(Callable[[dict[str, Any]], object], orjson.dumps)
        elif protocol == 'msgpack' and msgpack:
            msgpack_mod = msgpack
            def msgpack_serializer(msg: dict[str, Any]) -> object:
                return cast(Any, msgpack_mod).packb(msg, use_bin_type=True)
            serializer_fn = msgpack_serializer
        else:
            def json_serializer(msg: dict[str, Any]) -> object:
                return json.dumps(msg, ensure_ascii=False, separators=(",", ":"), default=str)
            serializer_fn = json_serializer
        self._serializers[protocol] = serializer_fn
        return serializer_fn

    def _serialize_message(self, message: dict[str, Any], client_info: ClientInfo) -> object:
        protocol = client_info.protocol
        prefer_binary = client_info.prefer_binary or client_info.compress or protocol in {'orjson', 'msgpack'}
        serializer = self._get_serializer(protocol)
        payload = serializer(message)
        if isinstance(payload, str):
            if not prefer_binary:
                return payload
            payload_bytes = payload.encode("utf-8")
        elif isinstance(payload, (bytes, bytearray)):
            payload_bytes = bytes(payload)
        else:
            payload_bytes = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
        if client_info.compress and len(payload_bytes) >= client_info.compress_threshold:
            payload_bytes = zlib.compress(payload_bytes)
        return payload_bytes

    def _update_client_options(self, client_id: str, options: dict[str, Any]) -> None:
        client_info = self.clients.get(client_id)
        if not client_info:
            return
        if isinstance(options.get('batch'), bool):
            client_info.batch_mode = options['batch']
        if isinstance(options.get('binary'), bool):
            client_info.prefer_binary = options['binary']
        if isinstance(options.get('max_batch_items'), int):
            client_info.max_batch_items = max(1, options['max_batch_items'])
        if isinstance(options.get('protocol'), str):
            protocol = options['protocol'].lower()
            if protocol in {'json', 'orjson', 'msgpack'}:
                client_info.protocol = protocol
        if isinstance(options.get('compress'), bool):
            client_info.compress = options['compress']
        if isinstance(options.get('compress_threshold'), int):
            client_info.compress_threshold = max(1, options['compress_threshold'])
        self._update_recovery_snapshot(client_id)

    async def _resume_client(self, client_id: str, session_id: Optional[str]) -> None:
        if not session_id:
            await self.send_to_client(client_id, {
                'type': 'resume_failed',
                'message': 'session_id缺失',
                'timestamp': time.time()
            })
            return
        snapshot = self.recovery_manager.resume(session_id)
        if not snapshot:
            await self.send_to_client(client_id, {
                'type': 'resume_failed',
                'message': 'session已过期或不存在',
                'timestamp': time.time()
            })
            return
        client_info = self.clients.get(client_id)
        if not client_info:
            await self.send_to_client(client_id, {
                'type': 'resume_failed',
                'message': '客户端不存在',
                'timestamp': time.time()
            })
            return
        if client_info.session_id and client_info.session_id != session_id:
            self.recovery_manager.drop_session(client_info.session_id)
        client_info.session_id = session_id
        self.recovery_manager.bind_client(client_id, session_id)
        options = snapshot.get('options', {})
        self._update_client_options(client_id, options)
        symbols = snapshot.get('subscriptions', [])
        if symbols:
            await self.subscribe_symbols(client_id, symbols)
        await self._send_incremental_updates(client_id, snapshot)
        await self.send_to_client(client_id, {
            'type': 'resume_success',
            'session_id': session_id,
            'subscriptions': symbols,
            'timestamp': time.time()
        })

    def _update_recovery_snapshot(self, client_id: str) -> None:
        client_info = self.clients.get(client_id)
        if not client_info:
            return
        self.recovery_manager.update_snapshot(client_id, client_info)

    async def _flush_client_batches(self, client_batches: dict[str, list[dict[str, Any]]]):
        tasks = []
        for client_id, quotes in client_batches.items():
            client_info = self.clients.get(client_id)
            if not client_info:
                continue
            if not client_info.batch_mode:
                for quote in quotes:
                    symbol = quote.get('symbol')
                    event_ts = time.time()
                    message = {
                        'type': 'realtime_update',
                        'symbol': symbol,
                        'data': quote,
                        'timestamp': event_ts
                    }
                    tasks.append(self.send_to_client(client_id, message, event_ts=event_ts))
                continue
            max_items = client_info.max_batch_items or self.max_batch_items
            for i in range(0, len(quotes), max_items):
                chunk = quotes[i:i + max_items]
                event_ts = time.time()
                message = {
                    'type': 'realtime_batch',
                    'data': chunk,
                    'count': len(chunk),
                    'timestamp': event_ts
                }
                tasks.append(self.send_to_client(client_id, message, event_ts=event_ts))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _append_recovery_event(self, quote: dict[str, Any], event_ts: float) -> None:
        if not quote:
            return
        symbol = quote.get('symbol')
        if not symbol:
            return
        self.recovery_cache.append({
            'symbol': symbol,
            'data': quote,
            'timestamp': event_ts
        })

    async def _send_incremental_updates(self, client_id: str, snapshot: dict[str, Any]) -> None:
        client_info = self.clients.get(client_id)
        if not client_info:
            return
        last_event_ts = snapshot.get('last_event_ts', 0.0)
        subscriptions = set(snapshot.get('subscriptions', []))
        if not subscriptions:
            return
        cutoff = time.time() - self.recovery_cache_window
        pending = [
            item
            for item in list(self.recovery_cache)
            if item.get('timestamp', 0) > max(last_event_ts, cutoff)
            and item.get('symbol') in subscriptions
        ]
        if not pending:
            return
        if not client_info.batch_mode:
            for item in pending:
                message = {
                    'type': 'realtime_update',
                    'symbol': item.get('symbol'),
                    'data': item.get('data'),
                    'timestamp': item.get('timestamp')
                }
                await self.send_to_client(client_id, message, event_ts=item.get('timestamp'))
            return
        max_items = client_info.max_batch_items or self.max_batch_items
        for i in range(0, len(pending), max_items):
            chunk = pending[i:i + max_items]
            message = {
                'type': 'recovery_batch',
                'data': [item.get('data') for item in chunk],
                'count': len(chunk),
                'timestamp': chunk[-1].get('timestamp')
            }
            await self.send_to_client(client_id, message, event_ts=chunk[-1].get('timestamp'))

    async def _maybe_reconnect_sources(self) -> None:
        if not self.reconnect_backoff:
            return
        if self._consecutive_failures <= 0:
            return
        # 扩展退避序列：[1, 2, 5] → 实际按 index cap，后续失败按最后一级的倍数增长，最大 30s
        base_backoff = self.reconnect_backoff[
            min(self._consecutive_failures - 1, len(self.reconnect_backoff) - 1)
        ]
        delay = min(base_backoff * max(1, self._consecutive_failures // len(self.reconnect_backoff)), 30)
        self.logger.info(f"数据源重连退避 {delay}s (连续失败 {self._consecutive_failures})")
        await asyncio.sleep(delay)
        try:
            results = self.data_api.connect_all()
            success_count = sum(1 for ok in results.values() if ok)
            self.logger.info(f"数据源重连完成: {success_count}/{len(results)} 可用")
        except Exception as e:
            self.logger.error(f"数据源重连失败: {e}")

    def _handle_realtime_persistence_and_metrics(self, quotes: list[dict[str, Any]]) -> None:
        ingest_ts = datetime.now(tz=_SH)
        source_latencies: dict[str, list[float]] = {}

        for q in quotes:
            source = str(q.get("source") or "unknown")
            latency_ms = self._calc_latency_ms(q, ingest_ts)
            source_latencies.setdefault(source, []).append(latency_ms)
            self.data_api.report_source_quality(source_name=source, latency_ms=latency_ms, success=True)
            status = self.data_api.source_status.get(source)
            if status is not None and status.last_update:
                staleness_ms = max((time.time() - status.last_update) * 1000.0, 0.0)
                self._emit_metric("datasource.staleness_ms", staleness_ms, tags={"source": source})
                max_stale = float(getattr(self.data_api, "max_staleness_ms", 0.0) or 0.0)
                if max_stale > 0:
                    stale_flag = 1.0 if staleness_ms > max_stale else 0.0
                    self._emit_metric("datasource.stale_flag", stale_flag, tags={"source": source})

        for source, latency_list in source_latencies.items():
            if not latency_list:
                continue
            latency_list.sort()
            idx = min(int(len(latency_list) * 0.95), len(latency_list) - 1)
            p95 = latency_list[idx]
            self._emit_metric("data.latency_ms", p95, tags={"source": source})

        if not self.rt_persist_enabled or self.rt_sink is None:
            return

        try:
            stats = self.rt_sink.write_quotes(quotes)
            self._emit_metric("data.persist.quote_rows", float(stats.get("quote_rows", 0)))
            self._emit_metric("data.persist.orderbook_rows", float(stats.get("orderbook_rows", 0)))
        except Exception as e:
            self.logger.error(f"实时落库失败: {e}")
            self._emit_metric("source.fail_count", 1.0, tags={"source": "duckdb_sink"})
            return

        if self.rt_aggregator is None:
            return

        try:
            agg_stats = self.rt_aggregator.run_once(
                watermark_seconds=self.watermark_seconds,
                recompute_minutes=self.recompute_minutes,
            )
            self._emit_metric("data.aggregate.bar_rows", float(agg_stats.get("bar_rows", 0)))

            now = time.time()
            if now - self._last_finalize_ts >= self.finalize_interval_s:
                finalized = self.rt_aggregator.finalize_window(watermark_seconds=self.watermark_seconds)
                self._emit_metric("data.aggregate.finalized_rows", float(finalized))
                self._last_finalize_ts = now

            if now - self._last_miss_rate_ts >= self.miss_rate_interval_s:
                miss = self.rt_aggregator.compute_miss_rate(lookback_minutes=max(5, self.recompute_minutes * 5))
                self._emit_metric("data.miss_rate", float(miss.get("miss_rate", 0.0)))
                expected = float(miss.get("expected", 0))
                actual = float(miss.get("actual", 0))
                hit_rate = (actual / expected) if expected > 0 else 1.0
                self._emit_metric("backfill.hit_rate", hit_rate)
                self._last_miss_rate_ts = now
        except Exception as e:
            self.logger.error(f"分钟聚合失败: {e}")
            self._emit_metric("source.fail_count", 1.0, tags={"source": "minute_aggregator"})

        # TTL: 定时清理过期数据
        try:
            now = time.time()
            if self._purge_interval_s > 0 and now - self._last_purge_ts >= self._purge_interval_s:
                deleted = self.rt_sink.purge_expired_data(retention_days=self._purge_retention_days)
                total_deleted = sum(deleted.values())
                if total_deleted > 0:
                    self._emit_metric("data.purge.deleted_rows", float(total_deleted))
                self._last_purge_ts = now
        except Exception as e:
            self.logger.error(f"TTL清理失败: {e}")

    def _calc_latency_ms(self, quote: dict[str, Any], ingest_ts: datetime) -> float:
        raw_ts = quote.get("event_ts", quote.get("timestamp"))
        if raw_ts is None:
            return 0.0
        event_ts: Optional[datetime] = None
        try:
            if isinstance(raw_ts, datetime):
                event_ts = raw_ts
            elif isinstance(raw_ts, (int, float)):
                event_ts = datetime.fromtimestamp(float(raw_ts), tz=_SH)
            else:
                parsed = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
                if getattr(parsed, "tzinfo", None) is not None:
                    event_ts = parsed.astimezone().replace(tzinfo=None)
                else:
                    event_ts = parsed
        except Exception:
            event_ts = None
        if event_ts is None:
            return 0.0
        return max((ingest_ts - event_ts).total_seconds() * 1000.0, 0.0)

    def _emit_metric(self, name: str, value: float, tags: Optional[dict[str, str]] = None) -> None:
        try:
            self.metrics_collector._add_metric_point(
                MetricPoint(
                    timestamp=datetime.now(tz=_SH),
                    metric_name=name,
                    value=float(value),
                    tags=tags or {},
                    source="realtime_push_service",
                )
            )
        except Exception:
            return

    def get_service_stats(self) -> dict[str, Any]:
        """获取服务统计信息

        Returns:
            Dict: 统计信息
        """
        return {
            'stats': self.stats.copy(),
            'clients': len(self.clients),
            'subscriptions': len(self.subscriptions),
            'is_running': self.is_running,
            'data_sources': self.data_api.health_check()
        }


# 导出类
__all__ = ['RealtimeDataPushService', 'ClientInfo', 'PushMessage']
