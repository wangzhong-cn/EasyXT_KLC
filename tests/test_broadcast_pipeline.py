"""
广播管道端到端 + 可观测指标测试

覆盖范围：
  1. _MarketBroadcaster 单元测试 — queue_depths / avg_publish_latency_ms / drop_counts / seq
  2. 慢消费者隔离 — 单连接队列满不影响其他连接，丢帧计数正确累积
  3. 端到端管道 — ingest_tick_from_thread → broadcast → WS 客户端收帧
"""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

import core.api_server as srv
from core.api_server import (
    _WS_MAX_QUEUE_SIZE,
    _MarketBroadcaster,
    app,
    ingest_tick_from_thread,
)

# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------

def _run(coro):
    """在新事件循环中同步运行协程，无需 pytest-asyncio。"""
    return asyncio.run(coro)


def _fresh() -> _MarketBroadcaster:
    """返回状态干净的 _MarketBroadcaster 实例。"""
    return _MarketBroadcaster()


def _ws_with_queue(b: _MarketBroadcaster, symbol: str, maxsize: int = _WS_MAX_QUEUE_SIZE):
    """向 broadcaster 注入一个带假 WebSocket + 固定大小队列的订阅，返回 (ws_mock, queue)。"""
    ws = MagicMock()
    q: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
    b._queues[ws] = q
    b._channels.setdefault(symbol, set()).add(ws)
    return ws, q


# ---------------------------------------------------------------------------
# 1. _MarketBroadcaster 可观测指标单元测试
# ---------------------------------------------------------------------------

class TestBroadcastMetrics:
    """验证三项可观测指标：queue_depths / avg_publish_latency_ms / drop_counts。"""

    def test_queue_depths_empty_when_no_connections(self):
        assert _fresh().queue_depths() == {}

    def test_avg_latency_none_before_any_broadcast(self):
        assert _fresh().avg_publish_latency_ms is None

    def test_avg_latency_recorded_after_broadcast_with_subscriber(self):
        b = _fresh()
        _ws_with_queue(b, "SYM")
        _run(b.broadcast("SYM", {"price": 1.0}))

        lat = b.avg_publish_latency_ms
        assert lat is not None
        assert lat < 5.0, f"put_nowait 应在 5 ms 内完成，实际 {lat:.3f} ms"

    def test_avg_latency_not_recorded_without_subscribers(self):
        """无订阅者时广播不纳入基线统计（避免污染延迟均值）。"""
        b = _fresh()
        _run(b.broadcast("EMPTY", {"price": 1.0}))
        assert b.avg_publish_latency_ms is None

    def test_queue_depth_reflects_queued_messages(self):
        b = _fresh()
        ws, q = _ws_with_queue(b, "SYM")
        _run(b.broadcast("SYM", {"price": 1.0}))
        _run(b.broadcast("SYM", {"price": 2.0}))

        depths = b.queue_depths()
        assert depths[str(id(ws))] == 2

    def test_drop_count_increments_on_queue_full(self):
        b = _fresh()
        _ws_with_queue(b, "SYM", maxsize=2)   # 故意用容量为 2 的小队列

        for i in range(4):   # 广播 4 帧；前 2 入队，后 2 被丢
            _run(b.broadcast("SYM", {"price": float(i)}))

        assert b.drop_counts().get("SYM", 0) >= 2

    def test_drop_count_zero_when_not_overflowed(self):
        b = _fresh()
        _ws_with_queue(b, "SYM")
        _run(b.broadcast("SYM", {"price": 10.0}))
        assert b.drop_counts().get("SYM", 0) == 0

    def test_seq_monotonically_increases(self):
        b = _fresh()
        _, q = _ws_with_queue(b, "SYM")
        for _ in range(3):
            _run(b.broadcast("SYM", {"price": 1.0}))

        msgs = [json.loads(q.get_nowait()) for _ in range(3)]
        assert [m["seq"] for m in msgs] == [1, 2, 3]

    def test_event_ts_ms_is_populated(self):
        b = _fresh()
        _, q = _ws_with_queue(b, "SYM")
        _run(b.broadcast("SYM", {"price": 5.0}))

        msg = json.loads(q.get_nowait())
        assert "event_ts_ms" in msg
        assert abs(msg["event_ts_ms"] - int(time.time() * 1000)) < 5000

    def test_event_ts_ms_preserved_when_payload_contains_source_time(self):
        b = _fresh()
        _, q = _ws_with_queue(b, "SYM")
        _run(
            b.broadcast(
                "SYM",
                {"price": 5.0, "event_ts_ms": 1773000000123},
            )
        )
        msg = json.loads(q.get_nowait())
        assert msg["event_ts_ms"] == 1773000000123
        assert msg["source_event_ts_ms"] == 1773000000123
        assert "gateway_event_ts_ms" in msg

    def test_max_latency_none_before_any_broadcast(self):
        assert _fresh().max_publish_latency_ms is None

    def test_max_latency_recorded_after_broadcast_with_subscriber(self):
        b = _fresh()
        _ws_with_queue(b, "SYM")
        _run(b.broadcast("SYM", {"price": 1.0}))

        lat = b.max_publish_latency_ms
        assert lat is not None
        assert lat >= b.avg_publish_latency_ms, "max 必须 >= avg"
        assert lat < 5.0, f"put_nowait 应在 5 ms 内完成，实际 max={lat:.3f} ms"

    def test_max_latency_reflects_worst_sample(self):
        """手动注入延迟样本，max 应取最大值而非均值。"""
        b = _fresh()
        b._latency_window.extend([0.1, 0.2, 5.0])  # 模拟一次 5 ms 毛刺

        assert b.max_publish_latency_ms == 5.0
        assert b.avg_publish_latency_ms < 5.0

    def test_latency_window_capped_at_100(self):
        b = _fresh()
        _ws_with_queue(b, "SYM", maxsize=500)
        for _ in range(150):
            _run(b.broadcast("SYM", {"price": 1.0}))

        assert len(b._latency_window) == 100, "滑动窗口应恒定维持最近 100 次"

    def test_drop_counts_per_symbol_independent(self):
        """不同标的丢帧计数相互独立。"""
        b = _fresh()
        _ws_with_queue(b, "A", maxsize=1)
        _ws_with_queue(b, "B", maxsize=10)

        for _ in range(3):
            _run(b.broadcast("A", {"price": 1.0}))   # A 会丢
        _run(b.broadcast("B", {"price": 2.0}))        # B 不丢

        assert b.drop_counts().get("A", 0) >= 2
        assert b.drop_counts().get("B", 0) == 0


# ---------------------------------------------------------------------------
# 2. 慢消费者隔离测试
# ---------------------------------------------------------------------------

class TestSlowConsumer:
    """慢消费者（队列满）不得阻塞或影响其他订阅连接。"""

    def test_fast_consumer_unaffected_by_slow_consumer(self):
        b = _fresh()
        fast_ws, fast_q = _ws_with_queue(b, "SYM", maxsize=_WS_MAX_QUEUE_SIZE)
        slow_ws, slow_q = _ws_with_queue(b, "SYM", maxsize=2)   # 慢消费者

        # 先填满慢消费者队列（2 帧）
        for _ in range(2):
            _run(b.broadcast("SYM", {"price": 1.0}))

        # 再广播 3 帧 —— 慢消费者队列已满，应触发丢帧
        for i in range(3):
            _run(b.broadcast("SYM", {"price": float(i + 10)}))

        # 快消费者应收到全部 5 帧
        assert fast_q.qsize() == 5
        # 慢消费者仍只有 2 帧（后续 3 帧被丢弃）
        assert slow_q.qsize() == 2
        # 丢帧已被记录
        assert b.drop_counts().get("SYM", 0) >= 3

    def test_broadcast_completes_without_blocking(self):
        """验证 broadcast() 调用本身为非阻塞（put_nowait 语义）。"""
        b = _fresh()
        _ws_with_queue(b, "SYM", maxsize=1)

        _run(b.broadcast("SYM", {"price": 1.0}))   # 填满队列

        t0 = time.monotonic()
        _run(b.broadcast("SYM", {"price": 2.0}))   # 队列满 → 丢帧，不阻塞
        elapsed_ms = (time.monotonic() - t0) * 1000

        assert elapsed_ms < 100, f"broadcast 应在 100 ms 内返回，实际 {elapsed_ms:.1f} ms"
        assert b.drop_counts().get("SYM", 0) == 1

    def test_multiple_symbols_channels_isolated(self):
        """不同标的的 channel 互不干扰。"""
        b = _fresh()
        _, q_a = _ws_with_queue(b, "AAA")
        _, q_b = _ws_with_queue(b, "BBB")

        _run(b.broadcast("AAA", {"price": 11.0}))
        _run(b.broadcast("BBB", {"price": 22.0}))

        msg_a = json.loads(q_a.get_nowait())
        msg_b = json.loads(q_b.get_nowait())
        assert msg_a["price"] == 11.0
        assert msg_b["price"] == 22.0
        assert msg_a["seq"] == 1
        assert msg_b["seq"] == 1    # 序号按 symbol 各自计数


# ---------------------------------------------------------------------------
# 3. 端到端管道测试（TestClient + WebSocket）
# ---------------------------------------------------------------------------

_E2E_SYMBOL = "PIPE.E2E"   # 专用符号，避免与其他测试污染同名 channel


def _make_feed_mock() -> MagicMock:
    """返回 is_subscribed=True 的 feed mock，阻止 ws_market 启动 mock_tick_loop。"""
    feed = MagicMock()
    feed.is_subscribed.return_value = True
    return feed


class TestEndToEnd:
    """
    通过完整 HTTP+WS 栈验证 ingest_tick_from_thread → drain → 客户端收帧。

    每个测试独立创建 fresh broadcaster，通过 patch 注入，确保状态隔离。
    使用 TestClient 上下文管理器触发 lifespan（设置 _server_loop）。
    """

    def test_tick_flows_through_pipeline(self):
        """核心链路：ingest_tick_from_thread → broadcaster.broadcast → WS 客户端收帧。"""
        fresh_b = _fresh()
        feed = _make_feed_mock()

        with patch("core.api_server.broadcaster", fresh_b), \
             patch("core.qmt_feed.qmt_feed", feed):
            with TestClient(app) as tc:
                with tc.websocket_connect(f"/ws/market/{_E2E_SYMBOL}") as ws:
                    time.sleep(0.05)   # 等 drain task 在事件循环中完成注册

                    ingest_tick_from_thread(
                        _E2E_SYMBOL,
                        {"symbol": _E2E_SYMBOL, "price": 99.0, "source": "e2e_test"},
                    )
                    msg = ws.receive_text()

        data = json.loads(msg)
        assert data["price"] == 99.0
        assert data["source"] == "e2e_test"
        assert data["seq"] == 1
        assert "event_ts_ms" in data

    def test_seq_increments_across_multiple_ticks(self):
        """连续注入 3 帧，seq 应从 1 单调递增到 3。"""
        fresh_b = _fresh()
        feed = _make_feed_mock()

        with patch("core.api_server.broadcaster", fresh_b), \
             patch("core.qmt_feed.qmt_feed", feed):
            with TestClient(app) as tc:
                with tc.websocket_connect(f"/ws/market/{_E2E_SYMBOL}") as ws:
                    time.sleep(0.05)

                    for price in [1.0, 2.0, 3.0]:
                        ingest_tick_from_thread(
                            _E2E_SYMBOL, {"price": price, "source": "e2e_test"}
                        )

                    # receive_text() 依次阻塞直到每帧到达
                    msgs = [json.loads(ws.receive_text()) for _ in range(3)]

        seqs = [m["seq"] for m in msgs]
        prices = [m["price"] for m in msgs]
        assert seqs == [1, 2, 3]
        assert prices == [1.0, 2.0, 3.0]

    def test_ingest_from_thread_silent_when_loop_not_set(self):
        """_server_loop 未设置时，ingest_tick_from_thread 应静默丢弃（不抛异常）。"""
        original = srv._server_loop
        srv._server_loop = None
        try:
            # 不应抛出任何异常
            ingest_tick_from_thread(_E2E_SYMBOL, {"price": 1.0, "source": "test"})
        finally:
            srv._server_loop = original

    def test_health_exposes_queue_len_and_latency_fields(self):
        """/health 无论是否有广播，都应包含 queue_len 和 publish_latency_ms 字段。"""
        mock_reg = MagicMock()
        mock_reg.list_running.return_value = []
        mock_reg.list_all.return_value = []

        with patch("strategies.registry.strategy_registry", mock_reg):
            resp = TestClient(app).get("/health")

        assert resp.status_code == 200
        ws_check = resp.json()["checks"]["ws"]
        assert "queue_len" in ws_check, "queue_len 字段缺失"
        assert "publish_latency_ms" in ws_check, "publish_latency_ms 字段缺失"
        assert "publish_latency_max_ms" in ws_check, "publish_latency_max_ms 字段缺失"
        assert isinstance(ws_check["queue_len"], int)
        # 冷启动：无广播时为 None；有广播时为 float
        assert ws_check["publish_latency_ms"] is None or \
               isinstance(ws_check["publish_latency_ms"], float)
        assert ws_check["publish_latency_max_ms"] is None or \
               isinstance(ws_check["publish_latency_max_ms"], float)


# ---------------------------------------------------------------------------
# 4. 高频 burst 压测
# ---------------------------------------------------------------------------

class TestBurstPipeline:
    """
    短时 burst（300 帧）压力测试：
      - 守恒定律：deliver + drop == total_attempts
      - drop_rate 与 drop_counts / total_attempted 严格一致
      - 延迟在高负载下保持低位
      - queue_len 始终有界
    """

    BURST_N = 300

    def test_burst_conservation_law(self):
        """delivered + dropped == total_attempted（守恒定律），严格无漏帧。"""
        b = _fresh()
        _, fast_q = _ws_with_queue(b, "SYM", maxsize=self.BURST_N + 10)  # 不丢
        _, slow_q = _ws_with_queue(b, "SYM", maxsize=10)                 # 丢大部分

        for i in range(self.BURST_N):
            _run(b.broadcast("SYM", {"price": float(i)}))

        total_drops = b.drop_counts().get("SYM", 0)
        delivered_fast = fast_q.qsize()
        delivered_slow = slow_q.qsize()
        total_attempts = b._total_attempted

        assert total_attempts == self.BURST_N * 2, "2 个订阅者 × N 帧 = 2N 次尝试"
        assert delivered_fast == self.BURST_N, "快消费者应收到全部 N 帧"
        assert delivered_fast + delivered_slow + total_drops == total_attempts, (
            f"守恒定律失败: {delivered_fast}+{delivered_slow}+{total_drops} "
            f"!= {total_attempts}"
        )

    def test_drop_rate_matches_formula(self):
        """drop_rate 必须与 total_drops / total_attempted 完全匹配。"""
        b = _fresh()
        _ws_with_queue(b, "SYM", maxsize=10)  # 小队列，必定丢帧

        for i in range(50):
            _run(b.broadcast("SYM", {"price": float(i)}))

        total_drops = b.drop_counts().get("SYM", 0)
        expected_rate = round(total_drops / b._total_attempted, 4)
        assert b.drop_rate == expected_rate
        assert b.drop_rate > 0.0, "队列远超容量，drop_rate 应 > 0"

    def test_drop_rate_zero_when_no_overflow(self):
        b = _fresh()
        _ws_with_queue(b, "SYM", maxsize=self.BURST_N + 10)

        for i in range(self.BURST_N):
            _run(b.broadcast("SYM", {"price": float(i)}))

        assert b.drop_rate == 0.0
        assert b.drop_counts().get("SYM", 0) == 0

    def test_latency_stays_low_under_burst(self):
        """300 帧 burst 下延迟应保持低水平（全量并行负载下留出稳定裕量）。"""
        b = _fresh()
        _ws_with_queue(b, "SYM", maxsize=self.BURST_N + 10)

        for i in range(self.BURST_N):
            _run(b.broadcast("SYM", {"price": float(i)}))

        avg_lat = b.avg_publish_latency_ms
        max_lat = b.max_publish_latency_ms
        assert avg_lat is not None
        assert max_lat is not None
        assert avg_lat < 10.0, f"burst 下均值应 < 10 ms，实际 {avg_lat:.3f} ms"
        assert max_lat < 50.0, f"burst 下最大值应 < 50 ms，实际 {max_lat:.3f} ms"
        assert max_lat >= avg_lat, "max 必须 >= avg"

    def test_queue_len_bounded_under_burst(self):
        """burst 结束后所有连接队列深度不超过 maxsize。"""
        b = _fresh()
        _ws_with_queue(b, "SYM")   # 默认 _WS_MAX_QUEUE_SIZE

        for i in range(self.BURST_N):
            _run(b.broadcast("SYM", {"price": float(i)}))

        for depth in b.queue_depths().values():
            assert depth <= _WS_MAX_QUEUE_SIZE

    def test_total_attempted_counter_accuracy(self):
        """_total_attempted 计数器应等于 N × 订阅者数。"""
        b = _fresh()
        _ws_with_queue(b, "SYM")
        _ws_with_queue(b, "SYM")

        for _ in range(5):
            _run(b.broadcast("SYM", {"price": 1.0}))

        assert b._total_attempted == 5 * 2


# ---------------------------------------------------------------------------
# 5. 时间窗口 drop_rate_1m + 告警级别 drop_alert_level
# ---------------------------------------------------------------------------

import core.api_server as _srv_mod  # noqa: E402 — 仅用于补丁 _DROP_RATE_WARN/_DROP_RATE_CRIT


class TestDropRateWindow:
    """验证 drop_rate_1m（1 分钟滑动窗口）和 drop_alert_level 的正确性。"""

    def test_drop_rate_1m_zero_initially(self):
        assert _fresh().drop_rate_1m == 0.0

    def test_drop_rate_1m_reflects_current_drops(self, monkeypatch):
        """当前时刻发生的丢帧应立即体现在 drop_rate_1m 中。"""
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_MIN_SAMPLES", 1)  # 屏蔽门槛，专测窗口计算
        b = _fresh()
        _ws_with_queue(b, "SYM", maxsize=2)

        for i in range(5):   # 2 入队，3 丢弃
            _run(b.broadcast("SYM", {"price": float(i)}))

        assert b.drop_rate_1m > 0.0, "应检测到丢帧"
        # 1m 窗口与全生命周期结果吴合（本测试数据量小，全在窗口内）
        assert b.drop_rate_1m == b.drop_rate

    def test_drop_rate_1m_zero_when_no_overflow(self):
        b = _fresh()
        _ws_with_queue(b, "SYM", maxsize=100)

        for i in range(20):
            _run(b.broadcast("SYM", {"price": float(i)}))

        assert b.drop_rate_1m == 0.0

    def test_drop_rate_1m_excludes_old_events(self):
        """超出 60 s 的历史事件不应计入 1 分钟窗口。"""
        b = _fresh()
        # 人为注入一条 "70 秒前" 的事件（已在窗口外）
        old_ts = time.monotonic() - 70
        b._event_window.append((old_ts, 10, 8))   # attempted=10, dropped=8，但在窗口外

        # 当前窗口内广播几帧，不丢弃；超过 min_samples
        _ws_with_queue(b, "SYM", maxsize=100)
        for i in range(25):
            _run(b.broadcast("SYM", {"price": float(i)}))

        # 70s 前的丢帧不应被计入 1m 窗口
        assert b.drop_rate_1m == 0.0, "历史窗口外事件不应影响 drop_rate_1m"

    def test_event_window_populated_after_broadcast(self):
        b = _fresh()
        _ws_with_queue(b, "SYM")
        _run(b.broadcast("SYM", {"price": 1.0}))
        assert len(b._event_window) == 1
        ts, attempted, dropped = b._event_window[0]
        assert attempted == 1
        assert dropped == 0

    def test_event_window_not_populated_without_subscribers(self):
        """无订阅者的空广播不写入事件窗口。"""
        b = _fresh()
        _run(b.broadcast("EMPTY", {"price": 1.0}))
        assert len(b._event_window) == 0

    # ---------- 告警级别 ----------

    def test_drop_alert_ok_when_no_drops(self):
        b = _fresh()
        _ws_with_queue(b, "SYM", maxsize=100)
        for _ in range(30):   # 超过 min_samples=20
            _run(b.broadcast("SYM", {"price": 1.0}))
        assert b.drop_alert_level == "ok"

    def test_drop_alert_warning_threshold(self, monkeypatch):
        """drop_rate_1m ≥ 1% 但 < 5% 时应返回 warning。"""
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_WARN", 0.01)
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_CRIT", 0.05)

        b = _fresh()
        # 注入 drop_rate_1m ≈ 2%（attempted=100, dropped=2）
        now = time.monotonic()
        b._event_window.append((now, 100, 2))

        assert b.drop_alert_level == "warning"

    def test_drop_alert_critical_threshold(self, monkeypatch):
        """drop_rate_1m ≥ 5% 时应返回 critical。"""
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_WARN", 0.01)
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_CRIT", 0.05)

        b = _fresh()
        now = time.monotonic()
        b._event_window.append((now, 100, 6))   # drop_rate_1m = 6%

        assert b.drop_alert_level == "critical"

    def test_drop_alert_env_override(self, monkeypatch):
        """自定义阈值（如 warn=10%）应被正确应用。"""
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_WARN", 0.10)
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_CRIT", 0.20)
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_MIN_SAMPLES", 1)

        b = _fresh()
        now = time.monotonic()
        b._event_window.append((now, 100, 5))   # 5% < 10% warn → ok

        assert b.drop_alert_level == "ok"

    def test_drop_alert_ok_low_sample(self, monkeypatch):
        """1m 内样本量 < min_samples 时应返回 ok_low_sample，不得误告警。"""
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_MIN_SAMPLES", 20)

        b = _fresh()
        now = time.monotonic()
        # 仅 5 次尝试，全部丢帧——如果相信样本就会误判 critical
        b._event_window.append((now, 5, 5))

        assert b.drop_rate_1m == -1.0, "drop_rate_1m 应返回哨兵值 -1.0 表示样本不足"
        assert b.drop_alert_level == "ok_low_sample"

    def test_drop_alert_low_sample_env_override(self, monkeypatch):
        """min_samples 阈值可通过环境变量覆盖：设为 1 后 1 个样本就足够导出真实结果。"""
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_MIN_SAMPLES", 1)
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_WARN", 0.01)
        monkeypatch.setattr(_srv_mod, "_DROP_RATE_CRIT", 0.05)

        b = _fresh()
        now = time.monotonic()
        b._event_window.append((now, 1, 1))   # 100% drop_rate

        assert b.drop_rate_1m == 1.0
        assert b.drop_alert_level == "critical"
