"""
EasyXT 轻量化中台服务（Phase 3）

提供统一的 HTTP + WebSocket 接口，解耦 QMT 行情/交易与前端/外部策略之间的直连依赖。

架构：
  - FastAPI 主应用
  - /health                          — 健康检查
  - /api/v1/strategies/              — 策略注册表 REST（list/get/patch status）
  - /api/v1/accounts/                — 账户注册表 REST（list/post/get/delete）
  - /api/v1/market/snapshot/{symbol} — 最新行情快照（HTTP）
  - /ws/market/{symbol}              — 实时行情推送（WebSocket，支持多客户端）

部署入口：  python -m core.api_server          （开发热重载）
           uvicorn core.api_server:app         （生产）

配置项（环境变量或 config/server_config.json）：
  EASYXT_API_HOST  默认 0.0.0.0
  EASYXT_API_PORT  默认 8765
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 配置（环境变量驱动）
# ---------------------------------------------------------------------------

_API_TOKEN: str = os.environ.get("EASYXT_API_TOKEN", "")          # 空 = 生产环境拒绝启动
_DEV_MODE: bool = os.environ.get("EASYXT_DEV_MODE", "").lower() in ("1", "true", "yes")  # 本地开发跳过鉴权
_TEST_MODE: bool = ("PYTEST_CURRENT_TEST" in os.environ) or any("pytest" in x.lower() for x in sys.argv)
_RATE_LIMIT: int = int(os.environ.get("EASYXT_RATE_LIMIT", "60"))  # 每分钟每IP上限
_WS_SEND_TIMEOUT: float = float(os.environ.get("EASYXT_WS_TIMEOUT", "0.1"))  # 慢消费者超时(秒)
_WS_MAX_QUEUE_SIZE: int = int(os.environ.get("EASYXT_WS_QUEUE_SIZE", "64"))   # 每连接队列上限（满则丢帧）

# 丢帧率告警阈值（可通过环境变量覆盖）
_DROP_RATE_WARN: float = float(os.environ.get("EASYXT_DROP_RATE_WARN", "0.01"))  # 1%  → warning
_DROP_RATE_CRIT: float = float(os.environ.get("EASYXT_DROP_RATE_CRIT", "0.05"))  # 5%  → critical
_DROP_RATE_MIN_SAMPLES: int = int(os.environ.get("EASYXT_DROP_RATE_MIN_SAMPLES", "20"))  # 1m 窗口最小样本量（不足时不判定告警）

# 构建版本信息（CI 注入，本地开发时为 "dev"）
_BUILD_VERSION: str = os.environ.get("EASYXT_BUILD_VERSION", "dev")
_COMMIT_SHA: str = os.environ.get("EASYXT_COMMIT_SHA", "unknown")

# ---------------------------------------------------------------------------
# Prometheus 指标定义（prometheus_client 可选；不可用时 /metrics 降级为 JSON）
# ---------------------------------------------------------------------------

def _init_prometheus() -> tuple[bool, Any, Any, Any, Any, Any, Any, Any]:
    """初始化 Prometheus 指标对象。返回 (enabled, registry, counter_rl, g_drop, g_drop1m, g_strat, g_queue, g_uptime)。"""
    try:
        from prometheus_client import CollectorRegistry, Counter, Gauge  # noqa: PLC0415
        reg = CollectorRegistry(auto_describe=False)
        c_rl = Counter("easyxt_rate_limit_hits_total", "累计限流命中次数", registry=reg)
        g_drop = Gauge("easyxt_ws_drop_rate", "WebSocket 全生命周期丢帧率", registry=reg)
        g_drop1m = Gauge("easyxt_ws_drop_rate_1m", "WebSocket 最近60s丢帧率（-1=样本不足）", registry=reg)
        g_strat = Gauge("easyxt_strategies_running", "当前运行中策略数", registry=reg)
        g_queue = Gauge("easyxt_ws_queue_total_len", "所有WS连接队列积压帧总数", registry=reg)
        g_up = Gauge("easyxt_uptime_seconds", "服务运行时长（秒）", registry=reg)
        return True, reg, c_rl, g_drop, g_drop1m, g_strat, g_queue, g_up
    except Exception:  # pragma: no cover
        return False, None, None, None, None, None, None, None


(
    _prom_enabled,
    _prom_registry,
    _prom_rate_limit_hits,
    _prom_ws_drop_rate,
    _prom_ws_drop_rate_1m,
    _prom_strategies_running,
    _prom_ws_queue_len,
    _prom_uptime,
) = _init_prometheus()

# ---------------------------------------------------------------------------
# 限流：滑动窗口（每 IP 每 60 秒最多 _RATE_LIMIT 次）
# ---------------------------------------------------------------------------

_rate_buckets: dict[str, deque] = {}
_rate_limit_lock = threading.Lock()   # 保护 _rate_buckets 和 _rate_limit_hits 的并发访问
_rate_limit_hits: int = 0             # 限流命中累计计数（仅增不减，供监控采集）
_cleanup_stats: dict[str, Any] = {
    "last_run_epoch": None,   # 最近一次清理任务运行的 epoch(s)，None 表示尚未运行
    "last_removed_count": 0,  # 最近一次清理删除的 IP 桶数量
    "error_count": 0,         # 清理任务累计异常次数（任务活着但反复报错时可见）
}
_datasource_health_lock = threading.Lock()
_datasource_health_interface: Any = None


def _check_rate_limit(client_ip: str) -> bool:
    """返回 True 表示放行，False 表示已超限（同时递增命中计数）。线程安全。"""
    global _rate_limit_hits
    if _RATE_LIMIT <= 0:
        return True
    now = time.monotonic()
    with _rate_limit_lock:
        bucket = _rate_buckets.setdefault(client_ip, deque())
        while bucket and now - bucket[0] > 60.0:
            bucket.popleft()
        if len(bucket) >= _RATE_LIMIT:
            _rate_limit_hits += 1
            return False
        bucket.append(now)
    return True


def _get_datasource_health_interface() -> Any:
    global _datasource_health_interface
    if _datasource_health_interface is not None:
        return _datasource_health_interface
    with _datasource_health_lock:
        if _datasource_health_interface is None:
            from data_manager.unified_data_interface import UnifiedDataInterface
            duckdb_path = os.environ.get("EASYXT_DUCKDB_PATH", "") or None
            _datasource_health_interface = UnifiedDataInterface(
                duckdb_path=duckdb_path,
                eager_init=False,
                silent_init=True,
            )
    return _datasource_health_interface


async def _cleanup_rate_buckets() -> None:
    """后台定期清理长时间无活动的 IP 限流桶，防止服务长期运行后内存无限增长。"""
    while True:
        await asyncio.sleep(300)  # 每 5 分钟扫描一次
        now = time.monotonic()
        try:
            with _rate_limit_lock:
                stale = [
                    ip for ip, bucket in _rate_buckets.items()
                    if not bucket or now - bucket[-1] > 300.0
                ]
                for ip in stale:
                    del _rate_buckets[ip]
            _cleanup_stats["last_run_epoch"] = int(time.time())
            _cleanup_stats["last_removed_count"] = len(stale)
            if stale:
                log.debug("限流桶清理: 移除 %d 个过期 IP 桶", len(stale))
        except Exception:  # pragma: no cover
            _cleanup_stats["error_count"] = _cleanup_stats.get("error_count", 0) + 1
            log.exception("限流桶清理任务异常")


# ---------------------------------------------------------------------------
# 鉴权 + 限流组合依赖（/health 端点不使用）
# ---------------------------------------------------------------------------


async def _verify_auth_and_rate(
    request: Request,
    x_api_token: str = Header(default=""),
) -> None:
    """
    FastAPI 依赖：限流 + Token 鉴权。

    - 限流：每 IP 每分钟最多 _RATE_LIMIT 次（EASYXT_RATE_LIMIT env）
    - 鉴权：比对 EASYXT_API_TOKEN env；为空时跳过（开发模式）
    """
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="请求过于频繁，请稍后再试",
        )
    if _API_TOKEN and x_api_token != _API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效或缺失的 X-API-Token",
        )

# ---------------------------------------------------------------------------
# 统一错误响应格式
# ---------------------------------------------------------------------------

_HTTP_MESSAGES: dict[int, str] = {
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    409: "Conflict",
    422: "Unprocessable Entity",
    429: "Too Many Requests",
    500: "Internal Server Error",
}

# ---------------------------------------------------------------------------
# WebSocket 广播器
# ---------------------------------------------------------------------------


class _MarketBroadcaster:
    """
    管理 WebSocket 订阅的行情广播器（per-connection 队列模型）。

    协议约定（客户端去重键：symbol + seq）：
      {"symbol": "...", "price": ..., "event_ts_ms": <ms>, "seq": <int>, "source": "..."}

    稳定性保证：
      - 每个连接独立 asyncio.Queue（上限 _WS_MAX_QUEUE_SIZE），队列满即丢帧并计数
      - broadcast 仅做 put_nowait（纯内存操作），不阻塞生产者协程
      - 每个 WS 连接有独立 drain 协程负责实际发送，发送失败后自动清理
      - seq 单调递增，客户端可检测丢帧

    可观测指标（通过 /health 暴露）：
      - drop_counts()          — 各标的累计丢帧数（队列满时丢弃）
      - drop_rate              — 全生命周期丢帧率（总丢帧 / 总尝试）
      - drop_rate_1m           — 最近 60 s 窗口丢帧率（可感知瞬时抖动）
      - drop_alert_level       — ok / warning / critical（基于 drop_rate_1m）
      - queue_depths()         — 各连接当前队列水位（可检测慢消费者积压）
      - avg_publish_latency_ms — broadcast 循环内 put_nowait 平均耗时（微秒级，用于基线监控）
    """

    # 延迟滑动窗口：取最近 N 次 broadcast 的耗时均值
    _LATENCY_WINDOW = 100
    # 时间窗口丢帧率：统计最近 _WINDOW_SECS 秒内的事件
    _WINDOW_SECS: int = 60
    _EVENT_WINDOW_MAX: int = 10_000   # 最多保留条目数（100 次/秒 × ~100 s）

    def __init__(self) -> None:
        self._channels: dict[str, set[WebSocket]] = {}
        self._seq: dict[str, int] = {}             # per-symbol 单调递增序号
        self._queues: dict[WebSocket, asyncio.Queue] = {}
        self._drain_tasks: dict[WebSocket, asyncio.Task] = {}
        self._drop_counts: dict[str, int] = {}     # per-symbol 丢帧累计
        self._total_attempted: int = 0                 # 全生命周期 put_nowait 调用总次数（含成功与丢帧）
        # publish_latency 滑动窗口（单位 ms，仅统计有订阅者时的 broadcast 耗时）
        self._latency_window: deque = deque(maxlen=self._LATENCY_WINDOW)
        # 时间窗口事件：每次有效 broadcast 追加 (monotonic_ts, attempted, dropped)
        self._event_window: deque = deque(maxlen=self._EVENT_WINDOW_MAX)

    async def _drain(self, ws: WebSocket, symbol: str) -> None:
        """每个 WS 连接的独立消耗协程：从队列取帧 → send_text。

        注意：使用 asyncio.wait({task}, timeout) 而非 asyncio.wait_for(coro, timeout)
        避免 Python 3.11 + pytest-asyncio 1.3.0 中 asyncio.wait_for 内部 call_later
        回调在事件循环关闭阶段导致的挂起问题。
        """
        queue = self._queues.get(ws)
        if queue is None:
            return
        try:
            while True:
                msg = await queue.get()
                if msg is None:   # sentinel：正常关闭
                    break
                try:
                    send_task = asyncio.ensure_future(ws.send_text(msg))
                    done, pending = await asyncio.wait(
                        {send_task}, timeout=_WS_SEND_TIMEOUT
                    )
                    if pending:
                        send_task.cancel()
                        await asyncio.gather(send_task, return_exceptions=True)
                        break   # 慢消费者：超时后退出 drain
                    else:
                        send_task.result()  # 传播发送异常
                except Exception as exc:
                    log.debug("WS 发送失败 symbol=%s error=%s", symbol, exc)
                    break         # 连接已死，退出 drain
        except asyncio.CancelledError:
            pass
        finally:
            # 幂等清理（可能已由 unsubscribe 先执行）
            self._queues.pop(ws, None)
            self._drain_tasks.pop(ws, None)
            self._channels.get(symbol, set()).discard(ws)

    async def asubscribe(self, symbol: str, ws: WebSocket) -> None:
        """订阅：创建专属队列并启动 drain 协程（需在 event loop 中调用）。"""
        self._channels.setdefault(symbol, set()).add(ws)
        queue: asyncio.Queue = asyncio.Queue(maxsize=_WS_MAX_QUEUE_SIZE)
        self._queues[ws] = queue
        self._drain_tasks[ws] = asyncio.create_task(self._drain(ws, symbol))

    def unsubscribe(self, symbol: str, ws: WebSocket) -> None:
        """退出订阅（同步）：从频道移除并取消 drain 任务。"""
        self._channels.get(symbol, set()).discard(ws)
        self._queues.pop(ws, None)
        task = self._drain_tasks.pop(ws, None)
        if task and not task.done():
            task.cancel()

    def subscriber_count(self, symbol: str) -> int:
        return len(self._channels.get(symbol, set()))

    def all_symbols(self) -> list[str]:
        return [s for s, ch in self._channels.items() if ch]

    def drop_counts(self) -> dict[str, int]:
        """返回各标的累计丢帧数（队列满时丢弃）。"""
        return dict(self._drop_counts)

    def queue_depths(self) -> dict[str, int]:
        """返回每个活跃 WS 连接的当前队列水位（key 为连接对象 id 的字符串）。"""
        return {str(id(ws)): q.qsize() for ws, q in self._queues.items()}

    @property
    def avg_publish_latency_ms(self) -> float | None:
        """最近 _LATENCY_WINDOW 次 broadcast 的平均耗时（ms），无数据时返回 None。"""
        if not self._latency_window:
            return None
        return round(sum(self._latency_window) / len(self._latency_window), 3)

    @property
    def max_publish_latency_ms(self) -> float | None:
        """最近 _LATENCY_WINDOW 次 broadcast 的最大耗时（ms），无数据时返回 None。

        用于灰度阶段感知尾延迟：单次异常帧（如 GC 停顿、事件循环阻塞）
        在均值中被稀释，但会在 max 上显现，适合告警触发基准。
        """
        if not self._latency_window:
            return None
        return round(max(self._latency_window), 3)

    @property
    def drop_rate(self) -> float:
        """
        全生命周期丢帧率 = total_drops / total_attempted。

        语义：每 100 次帧投递尝试中有多少帧被丢弃（慢消费者）。
        0.0 表示无丢帧；> 0.01（1%）建议触发告警。
        """
        total_drops = sum(self._drop_counts.values())
        if self._total_attempted == 0:
            return 0.0
        return round(total_drops / self._total_attempted, 4)

    @property
    def drop_rate_1m(self) -> float:
        """
        最近 60 s 窗口丢帧率 = drops_1m / attempted_1m。0.0 表示无数据或无丢帧。

        用途：相比全生命周期 drop_rate，1m 窗口对瞬时抖动更敏感，适合告警触发。
        样本量不足 _DROP_RATE_MIN_SAMPLES 时返回 -1.0（表示低样本状态）。
        """
        cutoff = time.monotonic() - self._WINDOW_SECS
        attempted_w = sum(a for ts, a, _ in self._event_window if ts >= cutoff)
        dropped_w   = sum(d for ts, _, d in self._event_window if ts >= cutoff)
        if attempted_w == 0:
            return 0.0
        if attempted_w < _DROP_RATE_MIN_SAMPLES:
            return -1.0   # 哨兵值：表示样本量不足，不应计入告警判断
        return round(dropped_w / attempted_w, 4)

    @property
    def drop_alert_level(self) -> str:
        """
        基于近 1 分钟丢帧率的告警级别，优先感知瞬时抖动。

        级别：
          ok            — drop_rate_1m < _DROP_RATE_WARN（默认 1%）
          ok_low_sample — 1m 内样本量 < _DROP_RATE_MIN_SAMPLES，不判定告警（默认 20）
          warning       — drop_rate_1m in [1%, 5%)
          critical      — drop_rate_1m ≥ _DROP_RATE_CRIT（默认 5%）

        阈值可通过 EASYXT_DROP_RATE_WARN / EASYXT_DROP_RATE_CRIT 环境变量覆盖。
        """
        dr1m = self.drop_rate_1m
        if dr1m < 0:              # 哨兵值：样本量不足
            return "ok_low_sample"
        if dr1m >= _DROP_RATE_CRIT:
            return "critical"
        if dr1m >= _DROP_RATE_WARN:
            return "warning"
        return "ok"

    def _next_seq(self, symbol: str) -> int:
        self._seq[symbol] = self._seq.get(symbol, 0) + 1
        return self._seq[symbol]

    async def broadcast(self, symbol: str, payload: dict) -> None:
        """
        广播行情：put_nowait 到各订阅队列，队列满则丢帧并计数。

        本方法不做任何网络 I/O，广播延迟由各连接的 drain 协程承担。
        publish_latency_ms 统计本方法从入口到全部 put_nowait 完成的耗时。
        """
        t0 = time.monotonic()
        seq = self._next_seq(symbol)
        now_ms = int(time.time() * 1000)
        out_payload = dict(payload)
        if out_payload.get("source_event_ts_ms") in (None, ""):
            src_ts = out_payload.get("event_ts_ms")
            if src_ts not in (None, ""):
                out_payload["source_event_ts_ms"] = src_ts
        if out_payload.get("event_ts_ms") in (None, ""):
            out_payload["event_ts_ms"] = now_ms
        out_payload["gateway_event_ts_ms"] = now_ms
        msg = json.dumps(
            {**out_payload, "seq": seq},
            ensure_ascii=False,
        )
        attempts = 0
        dropped = 0
        for ws in list(self._channels.get(symbol, set())):
            queue = self._queues.get(ws)
            if queue is None:
                continue
            attempts += 1
            self._total_attempted += 1   # 全生命周期计数
            try:
                queue.put_nowait(msg)
            except asyncio.QueueFull:
                dropped += 1
        if dropped:
            self._drop_counts[symbol] = self._drop_counts.get(symbol, 0) + dropped
            log.warning("广播丢帧 symbol=%s dropped=%d（队列满，慢消费者）", symbol, dropped)
        # 有效广播（至少一个订阅者）时记录延迟和时间窗口事件
        if attempts > 0:
            elapsed_ms = (time.monotonic() - t0) * 1000
            self._latency_window.append(elapsed_ms)
            self._event_window.append((t0, attempts, dropped))


broadcaster = _MarketBroadcaster()

# ---------------------------------------------------------------------------
# 线程→事件循环桥接（QMT 回调注入实时行情）
# ---------------------------------------------------------------------------

_server_loop: asyncio.AbstractEventLoop | None = None
_server_start_time: float | None = None  # monotonic 启动时刻，用于计算 uptime_s


def ingest_tick_from_thread(symbol: str, tick_data: dict) -> None:
    """
    从非异步线程（如 QMT xtdata 回调）注入实时行情，线程安全。

    使用 run_coroutine_threadsafe 将广播协程提交到服务事件循环，
    不阻塞回调线程。若服务未启动则静默丢弃。

    接入 QMT 示例::

        from core.api_server import ingest_tick_from_thread

        def on_tick(data):
            for symbol, tick in data.items():
                ingest_tick_from_thread(symbol, {
                    "price": tick["lastPrice"],
                    "volume": tick["volume"],
                    "source": "qmt_live",
                })

        from xtquant import xtdata
        xtdata.subscribe_quote("000001.SZ", period="tick", callback=on_tick)
    """
    if _server_loop is None or _server_loop.is_closed():
        return
    asyncio.run_coroutine_threadsafe(
        broadcaster.broadcast(symbol, tick_data), _server_loop
    )


# ---------------------------------------------------------------------------
# 模拟行情推送后台任务（实盘时用 ingest_tick_from_thread 替代）
# ---------------------------------------------------------------------------

_mock_tick_tasks: dict[str, asyncio.Task] = {}


async def _mock_tick_loop(symbol: str) -> None:
    """开发/测试用：每秒推送随机价格，无需 QMT 连接。"""
    import random
    base = 10.0
    while True:
        await broadcaster.broadcast(symbol, {
            "symbol": symbol,
            "price": round(base + random.uniform(-0.5, 0.5), 3),  # noqa: S311  # mock only
            "source": "mock",
        })
        await asyncio.sleep(1.0)


# ---------------------------------------------------------------------------
# Pydantic 模型
# ---------------------------------------------------------------------------


class StrategyStatusPatch(BaseModel):
    status: str  # "running" | "paused" | "stopped" | "error"


class SubscribeRequest(BaseModel):
    symbol: str
    period: str = "tick"   # "tick" | "1m" | "5m" | "1d"


class AccountRegisterBody(BaseModel):
    account_id: str
    broker: str = ""
    enabled: bool = True


# ---------------------------------------------------------------------------
# App 生命周期
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _server_loop, _server_start_time
    _server_loop = asyncio.get_event_loop()
    _server_start_time = time.monotonic()
    _cleanup_task = asyncio.create_task(_cleanup_rate_buckets())
    if not _API_TOKEN:
        if _DEV_MODE or _TEST_MODE:
            log.warning(
                "⚠️  [DEV_MODE] EASYXT_API_TOKEN 未设置，鉴权已跳过（仅限本地开发）。"
                " 生产部署必须设置 EASYXT_API_TOKEN 并移除 EASYXT_DEV_MODE=1。"
            )
        else:
            raise RuntimeError(
                "EASYXT_API_TOKEN 未设置，服务拒绝启动。\n"
                "  生产环境：设置 EASYXT_API_TOKEN=<secret>\n"
                "  本地开发：设置 EASYXT_DEV_MODE=1（不得用于生产）"
            )
    log.info(
        "EasyXT 中台服务启动 (auth=%s, dev_mode=%s, rate_limit=%d req/min, ws_timeout=%.2fs)",
        "enabled" if _API_TOKEN else "disabled(DEV)",
        _DEV_MODE,
        _RATE_LIMIT,
        _WS_SEND_TIMEOUT,
    )
    yield
    _cleanup_task.cancel()
    for task in _mock_tick_tasks.values():
        task.cancel()
    _mock_tick_tasks.clear()
    _server_loop = None
    log.info("EasyXT 中台服务关闭")


app = FastAPI(
    title="EasyXT 中台 API",
    version="1.0.0",
    description="统一行情、交易与策略管理接口层",
    lifespan=_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PATCH"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """
    统一 HTTP 错误响应格式：
      {"code": <int>, "message": <str>, "detail": <str>, "trace_id": <uuid>}

    trace_id 用于日志追踪，每次请求唯一。
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "code": exc.status_code,
            "message": _HTTP_MESSAGES.get(exc.status_code, "Error"),
            "detail": exc.detail,
            "trace_id": str(uuid.uuid4()),
        },
    )

# ---------------------------------------------------------------------------
# 健康检查
# ---------------------------------------------------------------------------


@app.get("/health", tags=["运维"])
def health_check() -> dict:
    """服务健康检查（无需鉴权，适用于负载均衡探针）。"""
    uptime = (
        round(time.monotonic() - _server_start_time, 1)
        if _server_start_time is not None
        else None
    )

    # --- registry 子检查 ---
    try:
        from strategies.registry import strategy_registry
        running_count = len(strategy_registry.list_running())
        registry_status = "ok"
    except Exception:
        running_count = -1
        registry_status = "error"

    # --- ws 子检查（内存结构，始终可用） ---
    ws_symbols = broadcaster.all_symbols()
    ws_cleanup = {
        "last_run_epoch": _cleanup_stats["last_run_epoch"],
        "last_removed_count": _cleanup_stats["last_removed_count"],
        "error_count": _cleanup_stats.get("error_count", 0),
    }
    total_queue_len = sum(broadcaster.queue_depths().values())

    # --- db 子检查（轻量探针；失败仅标记 unavailable，不影响整体状态） ---
    try:
        from data_manager.duckdb_connection_pool import get_db_manager
        get_db_manager()
        db_status = "ok"
    except Exception:
        db_status = "unavailable"

    # 聚合：注册中心异常才降级，DB 离线属软故障
    agg_status = "ok" if registry_status == "ok" else "degraded"

    return {
        "status": agg_status,
        "checks": {
            "registry": {"status": registry_status, "strategies_running": running_count},
            "ws": {"status": "ok", "symbols": ws_symbols, "cleanup": ws_cleanup,
                   "drop_counts": broadcaster.drop_counts(),
                   "drop_rate": broadcaster.drop_rate,
                   "drop_rate_1m": broadcaster.drop_rate_1m,
                   "drop_alert": broadcaster.drop_alert_level,
                   "drop_alert_thresholds": {
                       "warn": _DROP_RATE_WARN,
                       "crit": _DROP_RATE_CRIT,
                       "min_samples": _DROP_RATE_MIN_SAMPLES,
                   },
                   "queue_len": total_queue_len,
                   "publish_latency_ms": broadcaster.avg_publish_latency_ms,
                   "publish_latency_max_ms": broadcaster.max_publish_latency_ms},
            "db": {"status": db_status},
        },
        # 以下平铺字段保持向后兼容（与旧版调用方/探针保持契约）
        "server_time": int(time.time() * 1000),
        "strategies_running": running_count,
        "ws_symbols": ws_symbols,
        "auth_enabled": bool(_API_TOKEN),
        "rate_limit_hits": _rate_limit_hits,
        "uptime_s": uptime,
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


@app.get("/health/datasource", tags=["运维"])
def datasource_health_check() -> dict[str, Any]:
    payload: dict[str, Any] = {"status": "ok", "checks": {}}
    try:
        iface = _get_datasource_health_interface()
        summary = iface.data_registry.get_health_summary()
        payload["checks"]["sources"] = summary
        payload["checks"]["circuit_breaker"] = dict(getattr(iface, "_cb_state", {}) or {})
        q_counts = iface.get_quarantine_status_counts()
        payload["checks"]["quarantine"] = q_counts
        total = int(q_counts.get("total", 0) or 0)
        dead = int(q_counts.get("dead_letter", 0) or 0)
        dead_ratio = (dead / total) if total > 0 else 0.0
        payload["checks"]["quarantine"]["dead_letter_ratio"] = dead_ratio
        payload["checks"]["data_quality_incident"] = iface.get_data_quality_incident_counts()
        payload["checks"]["step6_validation"] = iface.get_step6_validation_metrics()
        dl_abs_warn = int(os.environ.get("EASYXT_QUARANTINE_DEADLETTER_WARN", "100") or 100)
        dl_ratio_warn = float(os.environ.get("EASYXT_QUARANTINE_DEADLETTER_RATIO_WARN", "0.01") or 0.01)
        step6_sample_rate = float(os.environ.get("EASYXT_STEP6_VALIDATE_SAMPLE_RATE", "1.0") or 1.0)
        canary_shadow_write = str(os.environ.get("EASYXT_CANARY_SHADOW_WRITE", "0")).lower() in ("1", "true", "yes", "on")
        canary_shadow_only = str(os.environ.get("EASYXT_CANARY_SHADOW_ONLY", "1")).lower() in ("1", "true", "yes", "on")
        payload["checks"]["thresholds"] = {
            "dead_letter_abs_warn": dl_abs_warn,
            "dead_letter_ratio_warn": dl_ratio_warn,
            "step6_validate_sample_rate": step6_sample_rate,
            "canary_shadow_write_enabled": canary_shadow_write,
            "canary_shadow_only": canary_shadow_only,
        }
        if dead >= dl_abs_warn or dead_ratio >= dl_ratio_warn:
            payload["status"] = "degraded"
    except Exception as e:
        payload["status"] = "degraded"
        payload["checks"]["error"] = str(e)
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


@app.get("/health/sla", tags=["运维"])
def sla_health_check(report_date: str = "") -> dict[str, Any]:
    """
    数据质量 SLA 报告（当日或指定日期）。

    - `report_date`: 可选，格式 YYYY-MM-DD，默认为今天。
    - `gate_pass=false` 时 status 返回 "degraded"。
    """
    payload: dict[str, Any] = {"status": "ok"}
    try:
        iface = _get_datasource_health_interface()
        payload["sla"] = iface.generate_daily_sla_report(report_date or None)
        if not payload["sla"].get("gate_pass", True):
            payload["status"] = "degraded"
    except Exception as e:
        payload["status"] = "degraded"
        payload["error"] = str(e)
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


# ---------------------------------------------------------------------------
# 策略注册表 REST API
# ---------------------------------------------------------------------------


@app.get("/api/v1/strategies/", tags=["策略管理"],
         dependencies=[Depends(_verify_auth_and_rate)])
def list_strategies(status_filter: str = "") -> list[dict]:
    """
    枚举所有已注册策略。

    - `status_filter` 可选过滤：running / stopped / error（空则返回全部）
    """
    from strategies.registry import strategy_registry

    items = strategy_registry.list_all()
    if status_filter:
        items = [i for i in items if i["status"] == status_filter]
    return items


@app.get("/api/v1/strategies/{strategy_id}", tags=["策略管理"],
         dependencies=[Depends(_verify_auth_and_rate)])
def get_strategy(strategy_id: str) -> dict:
    """获取单个策略详情。"""
    from strategies.registry import strategy_registry

    info = strategy_registry.get(strategy_id)
    if info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"策略 {strategy_id!r} 未找到",
        )
    return {
        "strategy_id": info.strategy_id,
        "account_id": info.account_id,
        "status": info.status,
        "tags": info.tags,
        "params": info.params,
        "registered_at": info.registered_at,
        "has_instance": info.strategy_obj is not None,
    }


@app.patch("/api/v1/strategies/{strategy_id}/status", tags=["策略管理"],
           dependencies=[Depends(_verify_auth_and_rate)])
def patch_strategy_status(strategy_id: str, body: StrategyStatusPatch) -> dict:
    """
    更新策略状态（状态机约束，非法转换返回 409）。

    允许值：running / paused / stopped / error
    转换规则：
      created → running | stopped
      running → paused | stopped | error
      paused  → running | stopped
      error   → running | stopped
      stopped → （终态，拒绝一切转换）
    """
    allowed = {"running", "paused", "stopped", "error"}
    if body.status not in allowed:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"非法状态值 {body.status!r}，可选：{sorted(allowed)}",
        )

    from strategies.registry import strategy_registry

    result = strategy_registry.update_status(strategy_id, body.status)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"策略 {strategy_id!r} 未找到",
        )
    ok, reason = result
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"非法状态转换: {reason}",
        )
    return {"strategy_id": strategy_id, "status": body.status, "updated": True}


@app.post("/api/v1/strategies/snapshot", tags=["策略管理"],
          dependencies=[Depends(_verify_auth_and_rate)])
def snapshot_all_strategies() -> dict:
    """触发全量策略参数快照写入 DuckDB（每次追加新记录）。"""
    from strategies.registry import strategy_registry

    written = strategy_registry.snapshot_to_db()
    return {"snapshot_written": written}


# ---------------------------------------------------------------------------
# 行情快照（HTTP）
# ---------------------------------------------------------------------------


@app.get("/api/v1/market/snapshot/{symbol}", tags=["行情"],
         dependencies=[Depends(_verify_auth_and_rate)])
def get_market_snapshot(symbol: str) -> dict:
    """
    获取标的最新行情快照。

    优先从 DuckDB 缓存读取，不可用时返回占位响应。
    """
    try:
        from data_manager import unified_data_interface
        get_latest_tick = getattr(unified_data_interface, "get_latest_tick", None)
        tick = get_latest_tick(symbol) if callable(get_latest_tick) else None
        if tick is not None:
            return {"symbol": symbol, "data": tick, "source": "duckdb"}
    except Exception:
        pass

    return {
        "symbol": symbol,
        "data": None,
        "source": "unavailable",
        "message": "行情数据暂不可用，请启动 QMT 或等待数据同步",
    }


# ---------------------------------------------------------------------------
# 账户注册表 REST API
# ---------------------------------------------------------------------------


@app.get("/api/v1/accounts/", tags=["账户管理"],
         dependencies=[Depends(_verify_auth_and_rate)])
def list_accounts_api() -> list[dict]:
    """枚举所有已注册账户。"""
    from core.account_registry import account_registry
    return account_registry.list_accounts()


@app.post("/api/v1/accounts/", tags=["账户管理"],
          dependencies=[Depends(_verify_auth_and_rate)])
def register_account_api(body: AccountRegisterBody) -> dict:
    """注册或更新账户（account_id 存在则合并更新）。"""
    from core.account_registry import account_registry
    payload = body.model_dump()
    return account_registry.register_account(payload)


@app.get("/api/v1/accounts/{account_id}", tags=["账户管理"],
         dependencies=[Depends(_verify_auth_and_rate)])
def get_account_api(account_id: str) -> dict:
    """获取单个账户详情。"""
    from core.account_registry import account_registry
    data = account_registry.get_account(account_id)
    if data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"账户 {account_id!r} 未找到",
        )
    return data


@app.delete("/api/v1/accounts/{account_id}", tags=["账户管理"],
            dependencies=[Depends(_verify_auth_and_rate)])
def delete_account_api(account_id: str) -> dict:
    """注销账户（幂等：不存在时返回 404）。"""
    from core.account_registry import account_registry
    deleted = account_registry.delete_account(account_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"账户 {account_id!r} 未找到",
        )
    return {"account_id": account_id, "deleted": True}


# ---------------------------------------------------------------------------
# 行情订阅管理（QMT xtdata）
# ---------------------------------------------------------------------------


@app.post("/api/v1/market/subscribe", tags=["行情"],
          dependencies=[Depends(_verify_auth_and_rate)])
def subscribe_symbol(req: SubscribeRequest) -> dict:
    """
    订阅标的实时行情（通过 QMT xtdata）。

    QMT 不可用时优雅降级，返回 source=mock。
    重复订阅同一标的安全幂等。
    """
    try:
        from core.qmt_feed import qmt_feed
        result = qmt_feed.subscribe(req.symbol, req.period)
    except Exception as exc:
        result = {"subscribed": False, "source": "error", "message": str(exc)}
    return {"symbol": req.symbol, "period": req.period, **result}


@app.delete("/api/v1/market/subscribe/{symbol}", tags=["行情"],
            dependencies=[Depends(_verify_auth_and_rate)])
def unsubscribe_symbol(symbol: str) -> dict:
    """取消订阅指定标的实时行情。"""
    try:
        from core.qmt_feed import qmt_feed
        result = qmt_feed.unsubscribe(symbol)
    except Exception as exc:
        result = {"unsubscribed": False, "message": str(exc)}
    return {"symbol": symbol, **result}


@app.get("/api/v1/market/subscriptions", tags=["行情"],
         dependencies=[Depends(_verify_auth_and_rate)])
def list_subscriptions() -> dict:
    """列出当前所有 QMT 实时行情订阅及统计信息。"""
    try:
        from core.qmt_feed import qmt_feed
        subs = qmt_feed.all_subscriptions()
        stats = qmt_feed.stats()
    except Exception:
        subs = []
        stats = {}
    return {"subscriptions": subs, "stats": stats}


# ---------------------------------------------------------------------------
# 行情 WebSocket
# ---------------------------------------------------------------------------


@app.websocket("/ws/market/{symbol}")
async def ws_market(
    websocket: WebSocket,
    symbol: str,
    token: str = Query(default=""),
) -> None:
    """
    实时行情推送（WebSocket）。

    鉴权：通过 ?token=<api_token> 查询参数（EASYXT_API_TOKEN 为空时不校验）。
    数据格式：{"symbol": ..., "price": ..., "event_ts_ms": <ms>, "seq": <int>, "source": ...}
    客户端去重键：symbol + seq
    接入 QMT 后用 ingest_tick_from_thread() 替代 mock 推送。
    """
    if _API_TOKEN and token != _API_TOKEN:
        await websocket.close(code=4001, reason="Unauthorized")
        return

    await websocket.accept()
    await broadcaster.asubscribe(symbol, websocket)
    log.info("WS 订阅 symbol=%s 当前订阅数=%d", symbol, broadcaster.subscriber_count(symbol))

    # QMT 未订阅该标的时启动 mock 推送（开发/测试降级）
    try:
        from core.qmt_feed import qmt_feed as _qf
        _qmt_active = _qf.is_subscribed(symbol)
    except ImportError:
        _qmt_active = False
    if not _qmt_active and (symbol not in _mock_tick_tasks or _mock_tick_tasks[symbol].done()):
        _mock_tick_tasks[symbol] = asyncio.create_task(_mock_tick_loop(symbol))

    try:
        while True:
            data = await websocket.receive_text()
            if data.strip().lower() in ("ping", '{"type":"ping"}'):
                await websocket.send_text('{"type":"pong"}')
    except WebSocketDisconnect:
        pass
    finally:
        broadcaster.unsubscribe(symbol, websocket)
        log.info("WS 断开 symbol=%s 剩余订阅数=%d", symbol, broadcaster.subscriber_count(symbol))


# ---------------------------------------------------------------------------
# 财务数据 REST API
# ---------------------------------------------------------------------------


@app.get("/api/v1/data/financial/{stock_code}", tags=["数据查询"],
         dependencies=[Depends(_verify_auth_and_rate)])
def get_financial_data(
    stock_code: str,
    start_date: str = "",
    end_date: str = "",
    table: str = "",
) -> dict[str, Any]:
    """
    查询股票财务数据（利润表 / 资产负债表 / 现金流量表）。

    - `stock_code`: EasyXT 格式，如 ``000001.SZ``
    - `start_date` / `end_date`: 可选，格式 ``YYYY-MM-DD``，筛选报告期范围
    - `table`: 可选过滤，``income`` / ``balance`` / ``cashflow``，空=返回三表
    """
    try:
        from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path
        from data_manager.financial_data_saver import FinancialDataSaver

        db_mgr = get_db_manager(resolve_duckdb_path())
        saver = FinancialDataSaver(db_mgr)
        raw = saver.load_financial_data(
            stock_code=stock_code,
            start_date=start_date or None,
            end_date=end_date or None,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"财务数据查询失败: {exc}",
        ) from exc

    def _df_to_records(df: Any) -> list[dict]:
        if df is None or (hasattr(df, "empty") and df.empty):
            return []
        try:
            return df.where(df.notna(), other=None).to_dict(orient="records")
        except Exception:
            return []

    allowed_tables = {"income", "balance", "cashflow"}
    if table and table not in allowed_tables:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"table 参数非法，可选值: {sorted(allowed_tables)}",
        )

    payload: dict[str, Any] = {
        "stock_code": stock_code,
        "start_date": start_date,
        "end_date": end_date,
        "server_time": int(time.time() * 1000),
    }
    if not table or table == "income":
        payload["income"] = _df_to_records(raw.get("income"))
    if not table or table == "balance":
        payload["balance"] = _df_to_records(raw.get("balance"))
    if not table or table == "cashflow":
        payload["cashflow"] = _df_to_records(raw.get("cashflow"))
    return payload


@app.post("/api/v1/data/financial/{stock_code}/refresh", tags=["数据查询"],
          dependencies=[Depends(_verify_auth_and_rate)])
def refresh_financial_data(
    stock_code: str,
    start_date: str = "",
    end_date: str = "",
) -> dict[str, Any]:
    """
    触发单只股票财务数据刷新（优先 QMT，降级 Tushare）。

    - `stock_code`: EasyXT 格式，如 ``000001.SZ``
    - `start_date` / `end_date`: 可选报告期范围，格式 ``YYYY-MM-DD``
    """
    try:
        from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path
        from data_manager.financial_data_saver import FinancialDataSaver

        db_mgr = get_db_manager(resolve_duckdb_path())
        saver = FinancialDataSaver(db_mgr)

        # 尝试 QMT 路径
        qmt_result: dict[str, Any] = {"success": False, "skip_reason": "not_attempted"}
        try:
            iface = _get_datasource_health_interface()
            if getattr(iface, "qmt_available", False):
                import pandas as pd

                from xtquant import xtdata  # type: ignore[import]

                raw = xtdata.get_financial_data(
                    stock_list=[stock_code],
                    table_list=["Income", "Balance", "CashFlow"],
                    start_time="",
                    end_time="",
                )
                stock_raw = (raw or {}).get(stock_code, {})
                qmt_result = saver.save_from_qmt(
                    stock_code,
                    stock_raw.get("Income", pd.DataFrame()),
                    stock_raw.get("Balance", pd.DataFrame()),
                    stock_raw.get("CashFlow", pd.DataFrame()),
                )
            else:
                qmt_result["skip_reason"] = "qmt_unavailable"
        except Exception as exc:
            qmt_result["skip_reason"] = str(exc)

        # 若 QMT 未写入任何数据，降级到 Tushare
        ts_result: dict[str, Any] = {"success": False, "skip_reason": "not_attempted"}
        qmt_wrote = qmt_result.get("success") and (
            int(qmt_result.get("income_count", 0))
            + int(qmt_result.get("balance_count", 0))
            + int(qmt_result.get("cashflow_count", 0))
        ) > 0
        if not qmt_wrote:
            ts_result = saver.save_from_tushare(
                stock_code, start_date=start_date, end_date=end_date
            )

        overall_ok = qmt_wrote or ts_result.get("success", False)
        return {
            "stock_code": stock_code,
            "success": overall_ok,
            "qmt": qmt_result,
            "tushare": ts_result,
            "server_time": int(time.time() * 1000),
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"财务数据刷新失败: {exc}",
        ) from exc


# ---------------------------------------------------------------------------
# Prometheus /metrics 端点
# ---------------------------------------------------------------------------


@app.get("/metrics", tags=["运维"], include_in_schema=False)
def prometheus_metrics() -> Response:
    """
    Prometheus 指标抓取端点（无需鉴权，适用于 Prometheus scraper）。

    当 prometheus_client 已安装时，返回标准 text/plain Prometheus 格式；
    否则降级返回 JSON 格式的关键指标（Content-Type: application/json）。

    主要指标：
      easyxt_rate_limit_hits_total   — 累计限流命中次数
      easyxt_ws_drop_rate            — WS 全生命周期丢帧率
      easyxt_ws_drop_rate_1m         — WS 近 60s 丢帧率
      easyxt_strategies_running      — 当前运行策略数
      easyxt_ws_queue_total_len      — WS 队列积压帧总数
      easyxt_uptime_seconds          — 服务运行时长
    """
    # 采集当前值
    uptime_s = (
        round(time.monotonic() - _server_start_time, 1)
        if _server_start_time is not None
        else 0.0
    )
    try:
        from strategies.registry import strategy_registry
        running_count = len(strategy_registry.list_running())
    except Exception:
        running_count = -1

    total_queue_len = sum(broadcaster.queue_depths().values())

    if _prom_enabled:
        # 同步计数器与 gauge（Counter 只增不减，rate_limit_hits 作为 gauge_since_start）
        _prom_ws_drop_rate.set(broadcaster.drop_rate)  # type: ignore[union-attr]
        _prom_ws_drop_rate_1m.set(broadcaster.drop_rate_1m)  # type: ignore[union-attr]
        _prom_strategies_running.set(max(running_count, 0))  # type: ignore[union-attr]
        _prom_ws_queue_len.set(total_queue_len)  # type: ignore[union-attr]
        _prom_uptime.set(uptime_s)  # type: ignore[union-attr]
        # rate_limit_hits 是只增计数器 —— 将全局计数同步到 prometheus Counter
        # （Counter 内部维护自己的值，这里利用 _value 对齐；仅供参考指标）
        try:
            current_prom_val = int(_prom_rate_limit_hits._value.get())  # type: ignore[union-attr]
            diff = max(0, _rate_limit_hits - current_prom_val)
            if diff > 0:
                _prom_rate_limit_hits.inc(diff)  # type: ignore[union-attr]
        except Exception:
            pass
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
        return Response(
            content=generate_latest(_prom_registry),
            media_type=CONTENT_TYPE_LATEST,
        )

    # 降级：纯文本 Prometheus 格式（无 prometheus_client）
    lines = [
        "# HELP easyxt_rate_limit_hits_total 累计限流命中次数",
        "# TYPE easyxt_rate_limit_hits_total counter",
        f"easyxt_rate_limit_hits_total {_rate_limit_hits}",
        "# HELP easyxt_ws_drop_rate WebSocket 全生命周期丢帧率",
        "# TYPE easyxt_ws_drop_rate gauge",
        f"easyxt_ws_drop_rate {broadcaster.drop_rate}",
        "# HELP easyxt_ws_drop_rate_1m WebSocket 近 60s 丢帧率",
        "# TYPE easyxt_ws_drop_rate_1m gauge",
        f"easyxt_ws_drop_rate_1m {broadcaster.drop_rate_1m}",
        "# HELP easyxt_strategies_running 当前运行中的策略数量",
        "# TYPE easyxt_strategies_running gauge",
        f"easyxt_strategies_running {max(running_count, 0)}",
        "# HELP easyxt_ws_queue_total_len WS 队列积压帧总数",
        "# TYPE easyxt_ws_queue_total_len gauge",
        f"easyxt_ws_queue_total_len {total_queue_len}",
        "# HELP easyxt_uptime_seconds 服务运行时长",
        "# TYPE easyxt_uptime_seconds gauge",
        f"easyxt_uptime_seconds {uptime_s}",
    ]
    return Response(
        content="\n".join(lines) + "\n",
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


# ---------------------------------------------------------------------------
# 直接运行入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("EASYXT_API_HOST", "127.0.0.1")
    port = int(os.environ.get("EASYXT_API_PORT", "8765"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    log.info("启动 EasyXT 中台服务 %s:%d", host, port)
    uvicorn.run("core.api_server:app", host=host, port=port, reload=False)
