"""
QMT 实盘集成测试骨架。

默认全部 skip — 需要 MiniQMT 客户端开启 + 实盘账号配置后才有意义。

运行方式（两种等价）：
    pytest tests/test_qmt_integration.py --run-integration
    pytest -m integration --run-integration

CI 分层策略：
    # 日常单测（无外部依赖）
    pytest -m "not integration"

    # 实盘连通性验证（需要 MiniQMT 环境）
    pytest -m integration --run-integration
"""

from __future__ import annotations

import json
import threading
import time
import logging

import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from core.qmt_feed import QmtFeed, qmt_feed
from core.api_server import _MarketBroadcaster, app

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 全局常量
# ---------------------------------------------------------------------------

_SYMBOL = "000001.SZ"   # 平安银行，流动性好，tick 密集
_WAIT_S  = 5            # 等待第一笔 tick 的超时秒数


# ---------------------------------------------------------------------------
# 统一前置检查 fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def qmt_preflight():
    """集成测试统一前置检查（session 级，只运行一次）。

    检查项目：
      1. xtquant.xtdata 可导入（MiniQMT 已安装）
      2. QmtFeed 可实例化（无异常）
      3. subscribe / unsubscribe 基本导通（返回 source=qmt_live）

    任何一项不满足就用 pytest.skip 并附上标准化原因，后续所有依赖此 fixture 的用例会被一并跳过。
    返回值包含耗时（preflight_ms），可用于长期观察 QMT 健康趋势。
    """
    _PRE = "[QMT前置检查失败]"
    t0 = time.monotonic()

    is_avail = QmtFeed.is_available()
    if not is_avail:
        pytest.skip(
            f"{_PRE} is_available=False — xtquant.xtdata 不可导入，"
            "请确认 MiniQMT 已安装且 xtquant 在 Python 路径中"
        )

    probe = QmtFeed()
    result: dict = {}
    try:
        result = probe.subscribe(_SYMBOL, period="tick")
    except Exception as exc:
        pytest.skip(
            f"{_PRE} is_available=True source=N/A — subscribe 抛异常：{exc}"
        )
    finally:
        probe.unsubscribe(_SYMBOL)

    source = result.get("source", "N/A")
    if source != "qmt_live":
        pytest.skip(
            f"{_PRE} is_available=True source={source} — "
            "期望 qmt_live，请确认 MiniQMT 客户端已登录并连接到行情服务器"
        )

    preflight_ms = round((time.monotonic() - t0) * 1000)
    log.info("[QMT前置检查通过] is_available=True source=%s preflight_ms=%d", source, preflight_ms)

    return {"symbol": _SYMBOL, "wait_s": _WAIT_S, "preflight_ms": preflight_ms}


# ---------------------------------------------------------------------------
# 集成测试用例
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestQmtFeedLive:
    """需要 MiniQMT 连接的实盘测试。所有用例依赖 qmt_preflight，前置检查失败则全部 skip。"""

    def test_qmt_is_available(self, qmt_preflight):
        """前置检查通过即表示 xtquant.xtdata 可导入。"""
        assert QmtFeed.is_available()

    def test_subscribe_returns_qmt_live(self, qmt_preflight):
        """实盘订阅应返回 source=qmt_live。"""
        feed = QmtFeed()
        result = feed.subscribe(_SYMBOL, period="tick")
        try:
            assert result["subscribed"] is True, f"订阅失败：{result}"
            assert result["source"] == "qmt_live"
        finally:
            feed.unsubscribe(_SYMBOL)

    def test_tick_arrives_within_timeout(self, qmt_preflight):
        """订阅后应在 _WAIT_S 秒内收到至少一笔 tick（交易时段内才有效）。"""
        feed = QmtFeed()
        feed.subscribe(_SYMBOL, period="tick")
        try:
            deadline = time.time() + _WAIT_S
            while time.time() < deadline:
                subs = feed.all_subscriptions()
                if subs and subs[0]["ingested_count"] > 0:
                    break
                time.sleep(0.2)
            subs = feed.all_subscriptions()
            assert subs, "订阅列表为空"
            if subs[0]["ingested_count"] == 0:
                pytest.skip(f"{_WAIT_S}s 内未收到 tick（当前可能不在交易时段，非错误）")
        finally:
            feed.unsubscribe(_SYMBOL)

    def test_unsubscribe_clears_state(self, qmt_preflight):
        """取消订阅后 is_subscribed 应返回 False。"""
        feed = QmtFeed()
        feed.subscribe(_SYMBOL, period="tick")
        assert feed.is_subscribed(_SYMBOL)
        result = feed.unsubscribe(_SYMBOL)
        assert result["unsubscribed"] is True
        assert not feed.is_subscribed(_SYMBOL)

    def test_stats_reflect_live_data(self, qmt_preflight):
        """已订阅时 stats.qmt_available 应为 True，相关字段类型正确。"""
        feed = QmtFeed()
        feed.subscribe(_SYMBOL, period="tick")
        try:
            time.sleep(min(2, _WAIT_S))
            stats = feed.stats()
            assert stats["qmt_available"] is True
            assert isinstance(stats["total_ingested"], int)
            assert isinstance(stats["total_errors"], int)
        finally:
            feed.unsubscribe(_SYMBOL)

    def test_singleton_qmt_feed_functional(self, qmt_preflight):
        """模块级单例 qmt_feed 应与手动实例行为一致。"""
        result = qmt_feed.subscribe(_SYMBOL, period="tick")
        try:
            assert result["subscribed"] is True
        finally:
            qmt_feed.unsubscribe(_SYMBOL)


# ---------------------------------------------------------------------------
# QMT 回调 → 广播管道 全链路集成测试
# ---------------------------------------------------------------------------

_PIPE_WAIT_S = 10   # 等待 WS 端收帧超时（秒）


@pytest.mark.integration
class TestQmtCallbackPipeline:
    """
    真实 QMT 回调 → ingest_tick_from_thread → broadcaster → WS 客户端 全链路测试。

    验证整条链路端到端可达，确认：
      1. tick 经 _on_tick 归一化后字段完整（price/volume、source=qmt_live）
      2. seq / event_ts_ms 由广播层正确填入
      3. 高频推送期间 drop_rate 可解释（有界，与 drop_counts 一致）
      4. 取消订阅后 ingested_count 停止增长
    """

    def test_qmt_tick_arrives_at_ws_client(self, qmt_preflight):
        """真实 QMT tick 经广播管道送达 WS 客户端，校验归一化字段完整性。"""
        sym = qmt_preflight["symbol"]
        received: list = []
        received_event = threading.Event()

        fresh_b = _MarketBroadcaster()
        # 阻止 ws_market 为此 symbol 启动 mock_tick_loop
        feed_gate = MagicMock()
        feed_gate.is_subscribed.return_value = True

        with patch("core.api_server.broadcaster", fresh_b), \
             patch("core.qmt_feed.qmt_feed", feed_gate):
            with TestClient(app) as tc:
                time.sleep(0.05)   # 等 lifespan 完成，_server_loop 就绪
                with tc.websocket_connect(f"/ws/market/{sym}") as ws:
                    time.sleep(0.05)   # 等 drain task 在事件循环中注册

                    def _listener():
                        """非阻塞监听线程：收到第一帧即设 Event。"""
                        try:
                            msg = ws.receive_text()
                            received.append(json.loads(msg))
                        except Exception:
                            pass
                        received_event.set()

                    listener = threading.Thread(target=_listener, daemon=True)
                    listener.start()

                    # 接入真实 QMT 订阅（回调会走 ingest_tick_from_thread → fresh_b）
                    live_feed = QmtFeed()
                    sub_result = live_feed.subscribe(sym, period="tick")
                    assert sub_result["source"] == "qmt_live", (
                        f"前置检查通过后 source 不应为 {sub_result['source']}"
                    )

                    arrived = received_event.wait(timeout=_PIPE_WAIT_S)
                    live_feed.unsubscribe(sym)

        if not arrived or not received:
            pytest.skip(
                f"{_PIPE_WAIT_S}s 内 WS 端未收到 tick（非交易时段或 QMT 推送延迟）"
            )

        msg = received[0]
        # 归一化校验
        assert msg.get("source") == "qmt_live", f"期望 source=qmt_live，实际：{msg}"
        assert "seq" in msg, f"seq 字段缺失：{msg}"
        assert "event_ts_ms" in msg, f"event_ts_ms 字段缺失：{msg}"
        assert "price" in msg or "volume" in msg, f"价格/量字段缺失：{msg}"

    def test_drop_rate_interpretable_under_live_burst(self, qmt_preflight):
        """
        高频推送期间 drop_rate 应与 drop_counts / total_attempted 严格一致，
        且广播器本身不阻塞（drop 只计数，不 block）。
        """
        sym = qmt_preflight["symbol"]
        fresh_b = _MarketBroadcaster()
        feed_gate = MagicMock()
        feed_gate.is_subscribed.return_value = True

        with patch("core.api_server.broadcaster", fresh_b), \
             patch("core.qmt_feed.qmt_feed", feed_gate):
            with TestClient(app) as tc:
                time.sleep(0.05)
                # 两个 WS 连接：一个正常速度，一个极小队列（模拟慢消费者）
                with tc.websocket_connect(f"/ws/market/{sym}") as ws_fast, \
                     tc.websocket_connect(f"/ws/market/{sym}") as ws_slow:
                    # 故意缩小 slow 连接的队列
                    _slow_ws_obj = list(fresh_b._queues.keys())[-1]
                    import asyncio
                    fresh_b._queues[_slow_ws_obj] = asyncio.Queue(maxsize=3)

                    time.sleep(0.05)
                    live_feed = QmtFeed()
                    live_feed.subscribe(sym, period="tick")
                    time.sleep(min(3, _PIPE_WAIT_S))   # 等待一批 tick 涌入
                    live_feed.unsubscribe(sym)

        total_drops = sum(fresh_b.drop_counts().values())
        if fresh_b._total_attempted == 0:
            pytest.skip(f"{_PIPE_WAIT_S}s 内无 tick 推送（非交易时段）")

        computed = round(total_drops / fresh_b._total_attempted, 4)
        assert fresh_b.drop_rate == computed, (
            f"drop_rate={fresh_b.drop_rate} 与公式结果 {computed} 不符"
        )

    def test_ingest_stops_after_unsubscribe(self, qmt_preflight):
        """取消订阅后 ingested_count 应在几秒内停止增长。"""
        sym = qmt_preflight["symbol"]
        live_feed = QmtFeed()
        live_feed.subscribe(sym, period="tick")
        time.sleep(min(2, _PIPE_WAIT_S))

        live_feed.unsubscribe(sym)
        count_at_unsub = live_feed.stats()["total_ingested"]

        # 再等 1 秒，计数不应再变
        time.sleep(1)
        count_after = live_feed.stats()["total_ingested"]

        assert count_after == count_at_unsub, (
            f"取消订阅后仍收到 {count_after - count_at_unsub} 帧（回调未清理？）"
        )
