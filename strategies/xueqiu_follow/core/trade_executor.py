#!/usr/bin/env python3
"""
交易执行模块
负责执行交易指令，处理交易结果，管理订单状态
"""

import asyncio
import contextlib
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional

from easy_xt import get_advanced_api
from strategies.xueqiu_follow.utils.logger import setup_logger

from .risk_manager import RiskManager


class OrderStatus(Enum):
    """订单状态枚举"""
    PENDING = "pending"          # 待提交
    SUBMITTED = "submitted"      # 已提交
    PARTIAL_FILLED = "partial_filled"  # 部分成交
    FILLED = "filled"           # 全部成交
    CANCELLED = "cancelled"     # 已撤销
    REJECTED = "rejected"       # 被拒绝
    FAILED = "failed"          # 执行失败


class OrderType(Enum):
    """订单类型枚举"""
    MARKET = "market"          # 市价单
    LIMIT = "limit"           # 限价单
    STOP = "stop"             # 止损单


class OrderEventType(Enum):
    SUBMITTED = "submitted"
    PARTIAL_FILLED = "partial_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass
class OrderEvent:
    order_id: str
    event_type: OrderEventType
    timestamp: float
    data: dict[str, Any]


class OrderStatusCache:
    def __init__(self, refresh_interval: float = 0.2):
        self.refresh_interval = refresh_interval
        self._last_refresh = 0.0
        self._cache: dict[str, dict[str, Any]] = {}
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._lock = asyncio.Lock()

    async def get_snapshot(self, account_id: Optional[str], trader_api: Any) -> dict[str, dict[str, Any]]:
        now = time.time()
        if now - self._last_refresh < self.refresh_interval:
            return dict(self._cache)
        async with self._lock:
            now = time.time()
            if now - self._last_refresh < self.refresh_interval:
                return dict(self._cache)
            if not account_id or not hasattr(trader_api, "get_today_orders"):
                self._cache = {}
                self._last_refresh = now
                return dict(self._cache)
            loop = asyncio.get_running_loop()
            orders_df = await loop.run_in_executor(
                self._executor, trader_api.get_today_orders, account_id
            )
            snapshot: dict[str, dict[str, Any]] = {}
            if orders_df is not None and not orders_df.empty:
                for _, row in orders_df.iterrows():
                    row_dict = row.to_dict()
                    order_id = str(row_dict.get("order_id") or row_dict.get("orderID") or "")
                    if not order_id:
                        continue
                    snapshot[order_id] = row_dict
            self._cache = snapshot
            self._last_refresh = time.time()
            return dict(self._cache)

    def close(self):
        self._executor.shutdown(wait=False)


class TradeExecutor:
    """交易执行器"""

    def __init__(self, qmt_config: dict[str, Any]):
        log_level = qmt_config.get("log_level", "INFO")
        self.logger = setup_logger("TradeExecutor", log_level)
        self.config = qmt_config

        # 核心组件
        self.trader_api = get_advanced_api()
        self.risk_manager: Optional[RiskManager] = None

        # 订单管理
        self.active_orders: dict[str, dict[str, Any]] = {}
        self.order_history: list[dict[str, Any]] = []
        self.execution_stats: dict[str, Any] = {
            'total_orders': 0,
            'successful_orders': 0,
            'failed_orders': 0,
            'total_volume': 0,
            'total_amount': 0
        }

        # 执行控制
        self.max_concurrent_orders = qmt_config.get('max_concurrent_orders', 10)
        self.order_timeout = qmt_config.get('order_timeout', 30)  # 秒
        self.retry_times = qmt_config.get('retry_times', 3)
        self.retry_delay = qmt_config.get('retry_delay', 1)  # 秒
        self.order_monitor_interval = qmt_config.get('order_monitor_interval', 0.1)
        self.order_cache_refresh_interval = qmt_config.get('order_cache_refresh_interval', 0.2)
        self._order_status_cache = OrderStatusCache(self.order_cache_refresh_interval)
        self._order_callbacks: dict[str, Callable[[OrderEvent], Any]] = {}
        self._order_futures: dict[str, asyncio.Future] = {}
        self._order_event_queue: asyncio.Queue[OrderEvent] = asyncio.Queue()
        self._monitor_task: Optional[asyncio.Task] = None
        self._last_order_snapshot: dict[str, dict[str, Any]] = {}
        self._risk_executor = ThreadPoolExecutor(max_workers=qmt_config.get('risk_workers', 4))
        self._qmt_to_internal: dict[str, str] = {}

    async def initialize(self):
        """初始化交易执行器"""
        try:
            self.logger.info("初始化交易执行器...")

            # 初始化交易接口
            userdata_path = self.config.get('userdata_path', 'D:/国金证券QMT交易端/userdata_mini')
            session_id = self.config.get('session_id', 'xueqiu_follow')

            if not self.trader_api.connect(userdata_path, session_id):
                raise Exception("交易服务连接失败")

            # 添加交易账户
            account_id = self.config.get('account_id')
            if account_id and not self.trader_api.add_account(account_id):
                raise Exception(f"添加交易账户失败: {account_id}")

            # 初始化风险管理器
            self.risk_manager = RiskManager(self.config.get('risk_config_path'))

            self.logger.info("交易执行器初始化完成")
            await self.start()
            return True

        except Exception as e:
            self.logger.error(f"交易执行器初始化失败: {e}")
            return False

    async def start(self):
        if self._monitor_task and not self._monitor_task.done():
            return
        self._monitor_task = asyncio.create_task(self._order_event_monitor())

    async def _order_event_monitor(self):
        while True:
            try:
                account_id = self.config.get('account_id')
                snapshot = await self._order_status_cache.get_snapshot(account_id, self.trader_api)
                for qmt_order_id, status_info in snapshot.items():
                    internal_id = self._qmt_to_internal.get(str(qmt_order_id), str(qmt_order_id))
                    current_status = self._normalize_order_status(status_info)
                    prev = self._last_order_snapshot.get(str(qmt_order_id))
                    prev_status = self._normalize_order_status(prev) if prev else None
                    if current_status and current_status != prev_status:
                        event = OrderEvent(
                            order_id=internal_id,
                            event_type=self._map_status_to_event(current_status),
                            timestamp=time.time(),
                            data={**status_info, "qmt_order_id": qmt_order_id}
                        )
                        await self._order_event_queue.put(event)
                        callback = self._order_callbacks.get(internal_id)
                        if callback:
                            asyncio.create_task(callback(event))
                        future = self._order_futures.get(internal_id)
                        if future and not future.done() and current_status in {"filled", "cancelled", "rejected", "failed"}:
                            future.set_result(event)
                self._last_order_snapshot = snapshot
                await asyncio.sleep(self.order_monitor_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"订单监控异常: {e}")
                await asyncio.sleep(self.order_monitor_interval)

    def _normalize_order_status(self, status_info: Optional[dict[str, Any]]) -> Optional[str]:
        if not status_info:
            return None
        raw = str(status_info.get("status") or status_info.get("order_status") or "").lower()
        mapping = {
            "filled": "filled",
            "已成": "filled",
            "部成": "partial_filled",
            "partial": "partial_filled",
            "partial_filled": "partial_filled",
            "已撤": "cancelled",
            "cancelled": "cancelled",
            "rejected": "rejected",
            "废单": "rejected",
            "failed": "failed"
        }
        return mapping.get(raw, raw if raw else None)

    def _map_status_to_event(self, status: str) -> OrderEventType:
        mapping = {
            "filled": OrderEventType.FILLED,
            "partial_filled": OrderEventType.PARTIAL_FILLED,
            "cancelled": OrderEventType.CANCELLED,
            "rejected": OrderEventType.REJECTED,
            "failed": OrderEventType.FAILED,
            "submitted": OrderEventType.SUBMITTED
        }
        return mapping.get(status, OrderEventType.SUBMITTED)

    def _positions_df_to_dict(self, positions_df: Any) -> dict[str, dict[str, Any]]:
        if positions_df is None or getattr(positions_df, "empty", True):
            return {}
        positions: dict[str, dict[str, Any]] = {}
        for _, row in positions_df.iterrows():
            row_dict = row.to_dict()
            code = row_dict.get("code") or row_dict.get("symbol") or row_dict.get("stock_code")
            if not code:
                continue
            positions[str(code)] = row_dict
        return positions

    async def execute_order(self, order: dict[str, Any], callback: Optional[Callable[[OrderEvent], Any]] = None) -> dict[str, Any]:
        """执行单个交易指令"""
        order_id = f"order_{int(time.time() * 1000)}_{len(self.active_orders)}"

        try:
            await self.start()
            self.logger.info(f"开始执行订单: {order_id}")

            # 订单预处理
            processed_order = await self._preprocess_order(order, order_id)
            if not processed_order:
                return self._create_order_result(order_id, OrderStatus.REJECTED, "订单预处理失败")

            # 风险检查
            risk_check = await self._check_order_risk(processed_order)
            if not risk_check['allowed']:
                return self._create_order_result(order_id, OrderStatus.REJECTED, f"风险检查失败: {risk_check['reason']}")

            # 添加到活跃订单
            self.active_orders[order_id] = processed_order
            if callback:
                self._order_callbacks[order_id] = callback

            # 执行订单
            execution_result = await self._execute_order_with_retry(processed_order)

            # 更新订单状态
            await self._update_order_status(order_id, execution_result)

            # 更新统计信息
            self._update_execution_stats(execution_result)

            return execution_result

        except Exception as e:
            self.logger.error(f"执行订单 {order_id} 失败: {e}")
            return self._create_order_result(order_id, OrderStatus.FAILED, str(e))

    async def execute_batch_orders(self, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """批量执行交易指令"""
        try:
            self.logger.info(f"开始批量执行 {len(orders)} 个订单")

            # 检查并发限制
            if len(orders) > self.max_concurrent_orders:
                self.logger.warning(f"订单数量 {len(orders)} 超过并发限制 {self.max_concurrent_orders}，将分批执行")
                return await self._execute_orders_in_batches(orders)

            # 并发执行所有订单
            tasks = [self.execute_order(order) for order in orders]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 处理异常结果
            processed_results: list[dict[str, Any]] = []
            for i, result in enumerate(results):
                if isinstance(result, BaseException):
                    error_result = self._create_order_result(
                        f"batch_order_{i}",
                        OrderStatus.FAILED,
                        str(result)
                    )
                    processed_results.append(error_result)
                else:
                    processed_results.append(result)

            self.logger.info(f"批量执行完成，成功: {sum(1 for r in processed_results if r['status'] == OrderStatus.FILLED.value)}")
            return processed_results

        except Exception as e:
            self.logger.error(f"批量执行订单失败: {e}")
            return [self._create_order_result("batch_error", OrderStatus.FAILED, str(e))]

    async def _execute_orders_in_batches(self, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """分批执行订单"""
        all_results = []
        batch_size = self.max_concurrent_orders

        for i in range(0, len(orders), batch_size):
            batch = orders[i:i + batch_size]
            self.logger.info(f"执行第 {i//batch_size + 1} 批订单，数量: {len(batch)}")

            batch_results = await self.execute_batch_orders(batch)
            all_results.extend(batch_results)

            # 批次间延迟
            if i + batch_size < len(orders):
                await asyncio.sleep(0.5)

        return all_results

    async def _preprocess_order(self, order: dict[str, Any], order_id: str) -> Optional[dict[str, Any]]:
        """订单预处理"""
        try:
            # 验证必要字段
            required_fields = ['symbol', 'action', 'volume']
            for field in required_fields:
                if field not in order:
                    self.logger.error(f"订单缺少必要字段: {field}")
                    return None

            # 标准化订单格式
            processed_order = {
                'order_id': order_id,
                'symbol': order['symbol'].upper(),
                'action': order['action'].lower(),  # buy/sell
                'volume': int(order['volume']),
                'order_type': order.get('order_type', OrderType.LIMIT.value),
                'price': float(order.get('price', 0)),
                'timestamp': datetime.now(),
                'status': OrderStatus.PENDING.value,
                'reason': order.get('reason', ''),
                'retry_count': 0
            }

            # 验证订单参数
            if processed_order['volume'] <= 0:
                self.logger.error(f"订单数量无效: {processed_order['volume']}")
                return None

            if processed_order['action'] not in ['buy', 'sell']:
                self.logger.error(f"订单操作无效: {processed_order['action']}")
                return None

            # 确保股数为100的倍数
            processed_order['volume'] = (processed_order['volume'] // 100) * 100
            if processed_order['volume'] == 0:
                self.logger.error("订单数量调整后为0")
                return None

            # 限价单必须有有效价格
            if processed_order['order_type'] == OrderType.LIMIT.value:
                if processed_order['price'] is None or processed_order['price'] <= 0:
                    self.logger.error("限价单价格无效或缺失，拒绝下单")
                    return None

            return processed_order

        except Exception as e:
            self.logger.error(f"订单预处理失败: {e}")
            return None

    async def _check_order_risk(self, order: dict[str, Any]) -> dict[str, Any]:
        """检查订单风险"""
        try:
            if not self.risk_manager:
                return {'allowed': True, 'reason': '风险管理器未初始化'}
            account_id = self.config.get('account_id')
            loop = asyncio.get_running_loop()
            account_task = loop.run_in_executor(self._risk_executor, self.trader_api.get_account_asset_detailed, account_id) if account_id and hasattr(self.trader_api, "get_account_asset_detailed") else None
            positions_task = loop.run_in_executor(self._risk_executor, self.trader_api.get_positions_detailed, account_id) if account_id and hasattr(self.trader_api, "get_positions_detailed") else None
            account_info, positions_df = await asyncio.gather(
                account_task if account_task else asyncio.sleep(0, result=None),
                positions_task if positions_task else asyncio.sleep(0, result=None)
            )
            if not account_info:
                return {'allowed': False, 'reason': '无法获取账户信息'}
            current_positions = self._positions_df_to_dict(positions_df)
            allowed, reason, risk_level = await loop.run_in_executor(
                self._risk_executor,
                self.risk_manager.validate_order,
                order['symbol'],
                order['action'],
                order['volume'],
                order['price'],
                current_positions,
                account_info
            )
            return {'allowed': allowed, 'reason': reason, 'risk_level': risk_level}

        except Exception as e:
            self.logger.error(f"风险检查失败: {e}")
            return {'allowed': False, 'reason': f'风险检查异常: {str(e)}'}

    async def _execute_order_with_retry(self, order: dict[str, Any]) -> dict[str, Any]:
        """带重试的订单执行"""
        last_error = None

        for attempt in range(self.retry_times):
            try:
                self.logger.info(f"执行订单 {order['order_id']}，第 {attempt + 1} 次尝试")

                # 更新订单状态
                order['status'] = OrderStatus.SUBMITTED.value
                order['retry_count'] = attempt

                # 调用交易API执行订单
                account_id_raw = self.config.get('account_id')
                account_id = str(account_id_raw) if account_id_raw is not None else ""
                if not account_id:
                    raise ValueError("未配置account_id")

                # 确保使用限价委托（组合跟单调仓应采用滑点限价买卖）
                order_type_str = str(order.get('order_type', OrderType.LIMIT.value)).lower()
                is_limit_order = order_type_str == 'limit'
                price_value = order['price'] if is_limit_order and order['price'] and order['price'] > 0 else 0

                self.logger.info(
                    f"订单详情: 股票={order['symbol']}, 方向={order['action']}, "
                    f"数量={order['volume']}, 价格={price_value}, 委托类型={'限价' if is_limit_order else '市价'}"
                )

                qmt_order_id = self.trader_api.sync_order(
                    account_id=account_id,
                    code=order['symbol'],
                    order_type=order['action'],
                    volume=order['volume'],
                    price=price_value,
                    price_type='limit'  # 总是使用限价委托
                )

                if qmt_order_id:
                    self._qmt_to_internal[str(qmt_order_id)] = order['order_id']
                    # 等待订单执行结果
                    execution_result = await self._wait_for_order_completion(order['order_id'], str(qmt_order_id))
                    return execution_result
                else:
                    raise Exception("QMT订单提交失败")

            except Exception as e:
                last_error = e
                self.logger.warning(f"订单执行第 {attempt + 1} 次尝试失败: {e}")

                if attempt < self.retry_times - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))  # 递增延迟

        # 所有重试都失败
        return self._create_order_result(
            order['order_id'],
            OrderStatus.FAILED,
            f"重试 {self.retry_times} 次后仍失败: {str(last_error)}"
        )

    async def _wait_for_order_completion(self, order_id: str, qmt_order_id: str) -> dict[str, Any]:
        """等待订单完成"""
        if order_id in self._order_futures and not self._order_futures[order_id].done():
            future = self._order_futures[order_id]
        else:
            future = asyncio.get_running_loop().create_future()
            self._order_futures[order_id] = future
        try:
            event: OrderEvent = await asyncio.wait_for(future, timeout=self.order_timeout)
            status = self._normalize_order_status(event.data) or event.event_type.value
            filled_volume = event.data.get('filled_volume', event.data.get('traded_volume', 0))
            filled_price = event.data.get('filled_price', event.data.get('order_price', 0))
            if status == "filled":
                order_status = OrderStatus.FILLED
            elif status in {"cancelled", "rejected"}:
                order_status = OrderStatus.CANCELLED
            else:
                order_status = OrderStatus.FAILED
            return self._create_order_result(
                order_id,
                order_status,
                f"QMT订单状态: {status}",
                qmt_order_id=qmt_order_id,
                filled_volume=filled_volume,
                filled_price=filled_price
            )
        except asyncio.TimeoutError:
            return self._create_order_result(
                order_id,
                OrderStatus.FAILED,
                f"订单执行超时 ({self.order_timeout}秒)"
            )

    def _create_order_result(self, order_id: str, status: OrderStatus, message: str = "",
                           qmt_order_id: Optional[str] = None, filled_volume: int = 0,
                           filled_price: float = 0) -> dict[str, Any]:
        """创建订单结果"""
        return {
            'order_id': order_id,
            'qmt_order_id': qmt_order_id,
            'status': status.value if isinstance(status, OrderStatus) else status,
            'message': message,
            'filled_volume': filled_volume,
            'filled_price': filled_price,
            'timestamp': datetime.now().isoformat(),
            'success': status in [OrderStatus.FILLED, OrderStatus.PARTIAL_FILLED]
        }

    async def _update_order_status(self, order_id: str, result: dict[str, Any]):
        """更新订单状态"""
        try:
            if order_id in self.active_orders:
                order = self.active_orders[order_id]
                order['status'] = result['status']
                order['qmt_order_id'] = result.get('qmt_order_id')
                order['filled_volume'] = result.get('filled_volume', 0)
                order['filled_price'] = result.get('filled_price', 0)
                order['completion_time'] = datetime.now()

                # 如果订单完成，移到历史记录
                if result['status'] in [OrderStatus.FILLED.value, OrderStatus.CANCELLED.value, OrderStatus.FAILED.value]:
                    self.order_history.append(order.copy())
                    del self.active_orders[order_id]

        except Exception as e:
            self.logger.error(f"更新订单状态失败: {e}")

    def _update_execution_stats(self, result: dict[str, Any]):
        """更新执行统计"""
        try:
            self.execution_stats['total_orders'] += 1

            if result.get('success', False):
                self.execution_stats['successful_orders'] += 1
                self.execution_stats['total_volume'] += result.get('filled_volume', 0)
                self.execution_stats['total_amount'] += result.get('filled_volume', 0) * result.get('filled_price', 0)
            else:
                self.execution_stats['failed_orders'] += 1

        except Exception as e:
            self.logger.error(f"更新执行统计失败: {e}")

    async def get_account_info(self) -> Optional[dict[str, Any]]:
        """获取账户信息（资金、持仓等）"""
        try:
            account_id = self.config.get('account_id')
            if not account_id:
                return None

            account_info = self.trader_api.get_account_asset_detailed(account_id)
            return account_info

        except Exception as e:
            self.logger.error(f"获取账户信息失败: {e}")
            return None

    async def get_order_status(self, order_id: str) -> Optional[dict[str, Any]]:
        """查询订单状态"""
        try:
            # 先查活跃订单
            if order_id in self.active_orders:
                return self.active_orders[order_id]

            # 再查历史订单
            for order in self.order_history:
                if order['order_id'] == order_id:
                    return order

            return None

        except Exception as e:
            self.logger.error(f"查询订单状态失败: {e}")
            return None

    async def cancel_order(self, order_id: str) -> bool:
        """撤销订单"""
        try:
            self.logger.info(f"撤销订单: {order_id}")

            # 检查订单是否存在
            if order_id not in self.active_orders:
                self.logger.warning(f"订单 {order_id} 不存在或已完成")
                return False

            order = self.active_orders[order_id]
            qmt_order_id = order.get('qmt_order_id')

            if not qmt_order_id:
                self.logger.warning(f"订单 {order_id} 没有QMT订单ID")
                return False

            # 调用交易API撤销订单
            account_id = self.config.get('account_id')
            if not account_id:
                return False

            # 处理QMT订单ID类型转换
            try:
                qmt_id = int(qmt_order_id) if isinstance(qmt_order_id, str) and qmt_order_id.isdigit() else qmt_order_id
            except (ValueError, TypeError):
                self.logger.error(f"无效的QMT订单ID格式: {qmt_order_id}")
                return False

            if not isinstance(qmt_id, int):
                self.logger.error(f"QMT订单ID必须为整数: {qmt_order_id}")
                return False
            success = self.trader_api.sync_cancel_order(account_id, qmt_id)

            if success:
                # 更新订单状态
                await self._update_order_status(order_id, self._create_order_result(
                    order_id, OrderStatus.CANCELLED, "用户主动撤销"
                ))
                self.logger.info(f"订单 {order_id} 撤销成功")
            else:
                self.logger.warning(f"订单 {order_id} 撤销失败")

            return success

        except Exception as e:
            self.logger.error(f"撤销订单失败: {e}")
            return False

    async def cancel_all_orders(self) -> int:
        """撤销所有活跃订单"""
        try:
            self.logger.info(f"撤销所有活跃订单，数量: {len(self.active_orders)}")

            cancelled_count = 0
            order_ids = list(self.active_orders.keys())

            for order_id in order_ids:
                if await self.cancel_order(order_id):
                    cancelled_count += 1

            self.logger.info(f"成功撤销 {cancelled_count} 个订单")
            return cancelled_count

        except Exception as e:
            self.logger.error(f"撤销所有订单失败: {e}")
            return 0

    def get_execution_stats(self) -> dict[str, Any]:
        """获取执行统计信息"""
        stats = self.execution_stats.copy()
        stats['success_rate'] = (
            stats['successful_orders'] / stats['total_orders']
            if stats['total_orders'] > 0 else 0
        )
        stats['active_orders_count'] = len(self.active_orders)
        stats['history_orders_count'] = len(self.order_history)
        return stats

    def get_active_orders(self) -> dict[str, dict[str, Any]]:
        """获取活跃订单"""
        return self.active_orders.copy()

    def get_order_history(self, limit: int = 100) -> list[dict[str, Any]]:
        """获取订单历史"""
        return self.order_history[-limit:] if limit > 0 else self.order_history.copy()

    async def close(self):
        """关闭交易执行器"""
        try:
            self.logger.info("关闭交易执行器...")

            # 撤销所有活跃订单
            if self.active_orders:
                await self.cancel_all_orders()

            # 关闭交易连接
            if hasattr(self.trader_api, 'disconnect'):
                self.trader_api.disconnect()
            if self._monitor_task:
                self._monitor_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._monitor_task
            self._order_status_cache.close()
            self._risk_executor.shutdown(wait=False)

            self.logger.info("交易执行器已关闭")

        except Exception as e:
            self.logger.error(f"关闭交易执行器失败: {e}")


async def main():
    """测试函数"""
    config = {
        'qmt_path': 'C:/QMT/',
        'account_id': 'test_account',
        'max_concurrent_orders': 5,
        'order_timeout': 30,
        'retry_times': 3
    }

    executor = TradeExecutor(config)

    try:
        # 初始化
        if not await executor.initialize():
            print("初始化失败")
            return

        # 测试单个订单
        test_order = {
            'symbol': '000001',
            'action': 'buy',
            'volume': 100,
            'order_type': 'market',
            'reason': '测试买入'
        }

        result = await executor.execute_order(test_order)
        print(f"订单执行结果: {result}")

        # 获取统计信息
        stats = executor.get_execution_stats()
        print(f"执行统计: {stats}")

    finally:
        await executor.close()


if __name__ == "__main__":
    asyncio.run(main())
