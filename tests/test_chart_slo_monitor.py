"""
tests/test_chart_slo_monitor.py

ChartSloMonitor 行为覆盖测试。
策略：禁用后台 Timer（patch threading.Timer），手动触发 _do_eval()，
     避免竞争和测试间 timer 泄漏。
"""
from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

import gui_app.widgets.chart.chart_slo_monitor as slo_mod
from gui_app.widgets.chart.chart_slo_monitor import (
    ChartSloMonitor,
    SloWindowResult,
    _percentile,
    _now_str,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixture：每个测试创建独立 monitor 实例（绕过单例，直接实例化并 stop timer）
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def monitor():
    """创建一个 ChartSloMonitor 并立刻取消 timer，防止后台运行干扰测试。"""
    # 不使用单例，直接 __new__ + patch timer
    with patch("threading.Timer") as mock_timer_cls:
        mock_timer_cls.return_value = MagicMock()
        m = ChartSloMonitor.__new__(ChartSloMonitor)
        ChartSloMonitor.__init__(m)
    # 确保 timer 已取消（init 里启动的 mock timer）
    yield m
    m.stop()


@pytest.fixture(autouse=True)
def reset_singleton():
    """每个测试后清理单例，避免跨测试污染。"""
    ChartSloMonitor._instance = None
    yield
    ChartSloMonitor._instance = None


# ─────────────────────────────────────────────────────────────────────────────
# 纯工具函数
# ─────────────────────────────────────────────────────────────────────────────

class TestHelpers:
    def test_percentile_empty_returns_zero(self):
        assert _percentile([], 95) == 0.0

    def test_percentile_p0(self):
        assert _percentile([10, 20, 30], 0) == 10

    def test_percentile_p100_clamps(self):
        assert _percentile([10, 20, 30], 100) == 30

    def test_percentile_p95_single(self):
        assert _percentile([42.0], 95) == 42.0

    def test_percentile_sorted_correctly(self):
        data = [100, 10, 50, 30, 90]
        p50 = _percentile(data, 50)
        p95 = _percentile(data, 95)
        assert p50 <= p95

    def test_now_str_format(self):
        s = _now_str()
        assert len(s) == len("2026-03-08 12:00:00")
        assert s[4] == "-"
        assert s[7] == "-"


# ─────────────────────────────────────────────────────────────────────────────
# SloWindowResult
# ─────────────────────────────────────────────────────────────────────────────

class TestSloWindowResult:
    def test_is_ok_no_violations(self):
        r = SloWindowResult(1, "2026-01-01", 10.0, 10.0, 10.0, 0.0)
        assert r.is_ok is True

    def test_is_ok_with_violations(self):
        r = SloWindowResult(1, "2026-01-01", 200.0, 10.0, 10.0, 0.0, ["set_data P95 exceeded"])
        assert r.is_ok is False


# ─────────────────────────────────────────────────────────────────────────────
# 单例
# ─────────────────────────────────────────────────────────────────────────────

class TestSingleton:
    def test_get_instance_returns_same(self):
        with patch("threading.Timer") as mt:
            mt.return_value = MagicMock()
            a = ChartSloMonitor.get_instance()
            b = ChartSloMonitor.get_instance()
        a.stop()
        assert a is b


# ─────────────────────────────────────────────────────────────────────────────
# record_latency
# ─────────────────────────────────────────────────────────────────────────────

class TestRecordLatency:
    def test_set_data_appended(self, monitor):
        monitor.record_latency("set_data", 55.0)
        assert 55.0 in monitor._set_data_lats

    def test_update_data_appended(self, monitor):
        monitor.record_latency("update_data", 30.0)
        assert 30.0 in monitor._update_lats

    def test_update_bar_appended(self, monitor):
        monitor.record_latency("update_bar", 25.0)
        assert 25.0 in monitor._update_lats

    def test_watchdog_appended(self, monitor):
        monitor.record_latency("watchdog", 80.0)
        assert 80.0 in monitor._watchdog_lats

    def test_unknown_op_increments_op_count(self, monitor):
        before = monitor._op_count
        monitor.record_latency("unknown_op", 10.0)
        assert monitor._op_count == before + 1

    def test_op_count_incremented(self, monitor):
        monitor.record_latency("set_data", 10.0)
        assert monitor._op_count == 1


# ─────────────────────────────────────────────────────────────────────────────
# record_exception
# ─────────────────────────────────────────────────────────────────────────────

class TestRecordException:
    def test_exception_count_incremented(self, monitor):
        monitor.record_exception()
        assert monitor._exception_count == 1

    def test_op_count_incremented(self, monitor):
        monitor.record_exception()
        assert monitor._op_count == 1


# ─────────────────────────────────────────────────────────────────────────────
# _do_eval — 核心 SLO 评估逻辑
# ─────────────────────────────────────────────────────────────────────────────

class TestDoEval:
    def test_no_data_produces_ok_result(self, monitor):
        monitor._do_eval()
        latest = monitor.get_latest()
        assert latest is not None
        assert latest.is_ok

    def test_set_data_within_threshold_ok(self, monitor):
        # P95 < 120ms threshold
        for v in [10.0] * 20:
            monitor._set_data_lats.append(v)
        monitor._op_count = 20
        monitor._do_eval()
        latest = monitor.get_latest()
        assert not any("set_data" in v for v in latest.violations)

    def test_set_data_exceeds_threshold_violation(self, monitor):
        # 强制 P95 > 120ms
        for _ in range(19):
            monitor._set_data_lats.append(50.0)
        monitor._set_data_lats.append(999.0)  # P95 会被拉高
        monitor._op_count = 20
        monitor._do_eval()
        latest = monitor.get_latest()
        assert any("set_data" in v for v in latest.violations)

    def test_update_exceeds_threshold_violation(self, monitor):
        for _ in range(19):
            monitor._update_lats.append(50.0)
        monitor._update_lats.append(999.0)
        monitor._op_count = 20
        monitor._do_eval()
        latest = monitor.get_latest()
        assert any("update_data" in v for v in latest.violations)

    def test_exception_rate_violation(self, monitor):
        # KPI_EXCEPTION_RATE = 0.0，任何异常都算违约
        monitor._exception_count = 1
        monitor._op_count = 10
        monitor._do_eval()
        latest = monitor.get_latest()
        assert any("exception_rate" in v for v in latest.violations)

    def test_watchdog_baseline_set_on_first_window(self, monitor):
        monitor._watchdog_lats = [50.0, 60.0, 55.0]
        monitor._op_count = 3
        assert monitor._watchdog_p99_baseline_ms is None
        monitor._do_eval()
        assert monitor._watchdog_p99_baseline_ms is not None

    def test_watchdog_ratio_violation(self, monitor):
        # 基线 50ms，当前 P99 = 100ms (ratio=2.0 > 1.2)
        monitor._watchdog_p99_baseline_ms = 50.0
        for _ in range(10):
            monitor._watchdog_lats.append(10.0)
        monitor._watchdog_lats.append(100.0)  # P99
        monitor._op_count = 11
        monitor._do_eval()
        latest = monitor.get_latest()
        assert any("watchdog" in v for v in latest.violations)

    def test_data_cleared_after_eval(self, monitor):
        monitor._set_data_lats = [10.0, 20.0]
        monitor._update_lats = [5.0]
        monitor._watchdog_lats = [8.0]
        monitor._exception_count = 1
        monitor._op_count = 4
        monitor._do_eval()
        assert monitor._set_data_lats == []
        assert monitor._update_lats == []
        assert monitor._watchdog_lats == []
        assert monitor._exception_count == 0
        assert monitor._op_count == 0

    def test_window_id_incremented(self, monitor):
        before = monitor._window_id
        monitor._do_eval()
        assert monitor._window_id == before + 1

    def test_history_appended(self, monitor):
        monitor._do_eval()
        assert len(monitor._history) == 1

    def test_history_maxlen_10(self, monitor):
        for _ in range(15):
            monitor._do_eval()
        assert len(monitor._history) == 10


# ─────────────────────────────────────────────────────────────────────────────
# 连续违约计数 + 告警升级
# ─────────────────────────────────────────────────────────────────────────────

class TestConsecutiveViolations:
    def test_ok_window_resets_consecutive(self, monitor):
        # 先制造违约
        monitor._consecutive_violations = 2
        # 空窗口 → ok
        monitor._do_eval()
        assert monitor._consecutive_violations == 0

    def test_violation_increments_consecutive(self, monitor):
        monitor._exception_count = 1
        monitor._op_count = 1
        monitor._do_eval()
        assert monitor._consecutive_violations == 1

    def test_alert_emitted_after_consecutive_threshold(self, monitor):
        """连续 ALERT_CONSECUTIVE 次违约后 _emit_alert 应被调用。"""
        monitor._consecutive_violations = slo_mod.ALERT_CONSECUTIVE - 1
        with patch.object(monitor, "_emit_alert") as mock_emit:
            monitor._exception_count = 1
            monitor._op_count = 1
            monitor._do_eval()
            mock_emit.assert_called_once()

    def test_no_alert_before_threshold(self, monitor):
        """未达到连续阈值时不触发 _emit_alert。"""
        monitor._consecutive_violations = 0
        with patch.object(monitor, "_emit_alert") as mock_emit:
            monitor._exception_count = 1
            monitor._op_count = 1
            monitor._do_eval()
            if slo_mod.ALERT_CONSECUTIVE > 1:
                mock_emit.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# get_latest / get_status
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryInterface:
    def test_get_latest_no_data(self, monitor):
        assert monitor.get_latest() is None

    def test_get_latest_after_eval(self, monitor):
        monitor._do_eval()
        assert monitor.get_latest() is not None

    def test_get_status_no_data(self, monitor):
        s = monitor.get_status()
        assert s["status"] == "no_data"
        assert s["consecutive_violations"] == 0

    def test_get_status_ok(self, monitor):
        monitor._do_eval()
        s = monitor.get_status()
        assert s["status"] == "ok"

    def test_get_status_warning(self, monitor):
        monitor._consecutive_violations = 1
        monitor._exception_count = 1
        monitor._op_count = 1
        monitor._do_eval()
        s = monitor.get_status()
        # consecutive < ALERT_CONSECUTIVE → warning
        if slo_mod.ALERT_CONSECUTIVE > 1:
            assert s["status"] in ("warning", "alert")

    def test_get_status_alert(self, monitor):
        monitor._consecutive_violations = slo_mod.ALERT_CONSECUTIVE - 1
        monitor._exception_count = 1
        monitor._op_count = 1
        monitor._do_eval()
        s = monitor.get_status()
        assert s["status"] == "alert"

    def test_get_status_keys_present(self, monitor):
        monitor._do_eval()
        s = monitor.get_status()
        assert "window_id" in s
        assert "ts" in s
        assert "violations" in s


# ─────────────────────────────────────────────────────────────────────────────
# _emit_alert — 降级静默（signal_bus 不可用时不抛出）
# ─────────────────────────────────────────────────────────────────────────────

class TestEmitAlert:
    def test_emit_alert_no_signal_bus_no_raise(self, monitor):
        r = SloWindowResult(1, "2026-01-01", 200.0, 10.0, 10.0, 0.1, ["set_data violation"])
        import sys
        # 让 signal_bus import 失败
        with patch.dict(sys.modules, {
            "core.signal_bus": None,
            "core.events": None,
        }):
            monitor._emit_alert(r, 3)  # should not raise

    def test_emit_alert_with_signal_bus_calls_emit(self, monitor):
        r = SloWindowResult(1, "2026-01-01", 200.0, 10.0, 10.0, 0.1, ["violation"])
        mock_bus = MagicMock()
        mock_events = MagicMock()
        mock_events.CHART_SLO_ALERT = "CHART_SLO_ALERT"
        import sys
        fake_sb_mod = MagicMock()
        fake_sb_mod.signal_bus = mock_bus
        fake_ev_mod = MagicMock()
        fake_ev_mod.Events = mock_events
        with patch.dict(sys.modules, {
            "core.signal_bus": fake_sb_mod,
            "core.events": fake_ev_mod,
        }):
            monitor._emit_alert(r, 3)
        mock_bus.emit.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# stop()
# ─────────────────────────────────────────────────────────────────────────────

class TestStop:
    def test_stop_cancels_timer(self, monitor):
        mock_timer = MagicMock()
        monitor._eval_timer = mock_timer
        monitor.stop()
        mock_timer.cancel.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# _eval_window — finally 块重新调度 Timer (lines 138-144)
# ─────────────────────────────────────────────────────────────────────────────

class TestEvalWindow:
    def test_eval_window_calls_do_eval_and_reschedules_timer(self, monitor):
        """_eval_window() 的 finally 块会重新调度 Timer（覆盖 lines 138-144）。"""
        with patch("gui_app.widgets.chart.chart_slo_monitor.threading.Timer") as mock_timer_cls:
            mock_timer_instance = MagicMock()
            mock_timer_cls.return_value = mock_timer_instance
            monitor._eval_window()
        mock_timer_cls.assert_called_once()
        assert mock_timer_instance.daemon is True
        mock_timer_instance.start.assert_called_once()

    def test_eval_window_reschedules_even_if_do_eval_raises(self, monitor):
        """即使 _do_eval 抛出异常，finally 块也会重新调度 Timer。"""
        with patch.object(monitor, "_do_eval", side_effect=RuntimeError("eval failed")):
            with patch("gui_app.widgets.chart.chart_slo_monitor.threading.Timer") as mock_timer_cls:
                mock_timer_instance = MagicMock()
                mock_timer_cls.return_value = mock_timer_instance
                with pytest.raises(RuntimeError):
                    monitor._eval_window()
        mock_timer_cls.assert_called_once()
        mock_timer_instance.start.assert_called_once()
