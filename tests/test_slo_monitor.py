"""
SLO Monitor 单元测试

覆盖：
  - 无 attach 时 get_report 返回安全默认值
  - record_order 正确计算 reject_rate
  - check_slo_breached 空列表（全部达标）
  - check_slo_breached 包含违反项（db_lock / risk / order）
  - reset 清除计数器并调用 db_manager.reset_lock_metrics / risk_engine.reset_risk_stats
  - attach_risk_engine 与真实 RiskEngine 集成
  - log_report 不抛异常
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from core.slo_monitor import SLOMonitor, SLOTargets, ErrorBudget


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_db_manager(
    failure_rate: float = 0.0,
    p95_ms: float = 50.0,
    total_attempts: int = 100,
) -> MagicMock:
    mgr = MagicMock()
    mgr.get_lock_metrics.return_value = {
        "failure_rate": failure_rate,
        "p95_wait_ms": p95_ms,
        "total_attempts": total_attempts,
    }
    return mgr


def _make_risk_engine(halt_count: int = 0, total: int = 100) -> MagicMock:
    engine = MagicMock()
    pass_count = max(0, total - halt_count)
    engine.get_risk_stats.return_value = {
        "acc1": {"pass": pass_count, "halt": halt_count, "warn": 0, "limit": 0}
    }
    return engine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def monitor() -> SLOMonitor:
    return SLOMonitor()


@pytest.fixture
def strict_monitor() -> SLOMonitor:
    """所有阈值设为 0 → 任何数值都违反。"""
    targets = SLOTargets(
        db_lock_failure_rate=0.0,
        db_lock_p95_ms=0.0,
        risk_halt_rate=0.0,
        order_reject_rate=0.0,
    )
    return SLOMonitor(targets=targets)


# ---------------------------------------------------------------------------
# Default state (no attached sources)
# ---------------------------------------------------------------------------


class TestDefaultState:
    def test_get_report_structure(self, monitor: SLOMonitor):
        report = monitor.get_report()
        assert "db_lock" in report
        assert "risk" in report
        assert "orders" in report
        assert "overall_ok" in report
        assert "uptime_s" in report

    def test_overall_ok_with_no_sources(self, monitor: SLOMonitor):
        # 无数据时所有 section slo_ok = True
        report = monitor.get_report()
        assert report["overall_ok"] is True

    def test_uptime_non_negative(self, monitor: SLOMonitor):
        import time
        time.sleep(0.01)
        report = monitor.get_report()
        assert report["uptime_s"] >= 0

    def test_orders_zero_on_init(self, monitor: SLOMonitor):
        report = monitor.get_report()
        assert report["orders"]["total"] == 0
        assert report["orders"]["rejected"] == 0
        assert report["orders"]["reject_rate"] == 0.0


# ---------------------------------------------------------------------------
# record_order
# ---------------------------------------------------------------------------


class TestRecordOrder:
    def test_total_increments(self, monitor: SLOMonitor):
        monitor.record_order(rejected=False)
        monitor.record_order(rejected=False)
        assert monitor.get_report()["orders"]["total"] == 2

    def test_rejected_increments(self, monitor: SLOMonitor):
        monitor.record_order(rejected=True)
        report = monitor.get_report()
        assert report["orders"]["rejected"] == 1
        assert report["orders"]["reject_rate"] == 1.0

    def test_mixed_reject_rate(self, monitor: SLOMonitor):
        for _ in range(9):
            monitor.record_order(rejected=False)
        monitor.record_order(rejected=True)
        report = monitor.get_report()
        assert abs(report["orders"]["reject_rate"] - 0.10) < 1e-6

    def test_zero_total_rate_is_zero(self, monitor: SLOMonitor):
        assert monitor.get_report()["orders"]["reject_rate"] == 0.0


# ---------------------------------------------------------------------------
# check_slo_breached – no violations
# ---------------------------------------------------------------------------


class TestCheckSloBreachedOK:
    def test_empty_on_no_sources(self, monitor: SLOMonitor):
        assert monitor.check_slo_breached() == []

    def test_empty_with_good_metrics(self, monitor: SLOMonitor):
        monitor.attach_db_manager(_make_db_manager(failure_rate=0.0, p95_ms=100))
        monitor.attach_risk_engine(_make_risk_engine(halt_count=0, total=100))
        monitor.record_order(rejected=False)
        assert monitor.check_slo_breached() == []

    def test_returns_list_type(self, monitor: SLOMonitor):
        breaches = monitor.check_slo_breached()
        assert isinstance(breaches, list)


# ---------------------------------------------------------------------------
# check_slo_breached – violations
# ---------------------------------------------------------------------------


class TestCheckSloBreachedViolations:
    def test_db_lock_failure_rate_violation(self):
        targets = SLOTargets(db_lock_failure_rate=0.001)
        m = SLOMonitor(targets=targets)
        m.attach_db_manager(_make_db_manager(failure_rate=0.05))   # >> 0.001
        breaches = m.check_slo_breached()
        assert any("db_lock" in b for b in breaches)

    def test_db_lock_p95_violation(self):
        targets = SLOTargets(db_lock_p95_ms=200.0)
        m = SLOMonitor(targets=targets)
        m.attach_db_manager(_make_db_manager(p95_ms=500.0))   # >> 200
        breaches = m.check_slo_breached()
        assert any("db_lock" in b for b in breaches)

    def test_risk_halt_rate_violation(self):
        targets = SLOTargets(risk_halt_rate=0.01)
        m = SLOMonitor(targets=targets)
        m.attach_risk_engine(_make_risk_engine(halt_count=50, total=100))   # 50%
        breaches = m.check_slo_breached()
        assert any("risk" in b for b in breaches)

    def test_order_reject_rate_violation(self):
        targets = SLOTargets(order_reject_rate=0.05)
        m = SLOMonitor(targets=targets)
        for _ in range(10):
            m.record_order(rejected=True)    # 100% reject
        breaches = m.check_slo_breached()
        assert any("order" in b for b in breaches)

    def test_multiple_violations(self):
        targets = SLOTargets(
            db_lock_failure_rate=0.0,
            risk_halt_rate=0.0,
            order_reject_rate=0.0,
        )
        m = SLOMonitor(targets=targets)
        m.attach_db_manager(_make_db_manager(failure_rate=0.01))
        m.attach_risk_engine(_make_risk_engine(halt_count=1, total=10))
        m.record_order(rejected=True)
        breaches = m.check_slo_breached()
        assert len(breaches) >= 2


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------


class TestReset:
    def test_reset_clears_order_counters(self, monitor: SLOMonitor):
        monitor.record_order(rejected=True)
        monitor.record_order(rejected=False)
        monitor.reset()
        report = monitor.get_report()
        assert report["orders"]["total"] == 0
        assert report["orders"]["rejected"] == 0

    def test_reset_calls_db_manager_reset(self, monitor: SLOMonitor):
        mgr = _make_db_manager()
        monitor.attach_db_manager(mgr)
        monitor.reset()
        mgr.reset_lock_metrics.assert_called_once()

    def test_reset_calls_risk_engine_reset(self, monitor: SLOMonitor):
        engine = _make_risk_engine()
        monitor.attach_risk_engine(engine)
        monitor.reset()
        engine.reset_risk_stats.assert_called_once()

    def test_reset_without_attachments_is_safe(self, monitor: SLOMonitor):
        monitor.record_order()
        monitor.reset()   # should not raise even with no db/risk attached
        assert monitor.get_report()["orders"]["total"] == 0


# ---------------------------------------------------------------------------
# Integration with real RiskEngine
# ---------------------------------------------------------------------------


class TestIntegrationWithRiskEngine:
    def test_real_risk_engine_stats_reflected(self):
        from core.risk_engine import RiskEngine, RiskThresholds

        engine = RiskEngine()
        # HALT by making drawdown 50%
        engine.update_daily_high("acc", 200_000.0)
        engine.check_pre_trade(
            account_id="acc",
            code="X",
            volume=1,
            price=1.0,
            direction="buy",
            positions={},
            nav=100_000.0,
        )

        m = SLOMonitor(targets=SLOTargets(risk_halt_rate=0.5))
        m.attach_risk_engine(engine)
        report = m.get_report()
        assert report["risk"]["halt_count"] >= 1
        assert report["risk"]["total_checks"] >= 1

    def test_breach_detected_with_real_engine(self):
        from core.risk_engine import RiskEngine

        engine = RiskEngine()
        # 100% halt rate (no daily high = 0% drawdown, so actually PASS)
        # To force HALT: set high and low nav
        engine.update_daily_high("acc2", 300_000.0)
        for _ in range(5):
            engine.check_pre_trade(
                account_id="acc2",
                code="Y",
                volume=1,
                price=1.0,
                direction="buy",
                positions={},
                nav=100_000.0,   # 66% drawdown → always HALT
            )

        m = SLOMonitor(targets=SLOTargets(risk_halt_rate=0.0))
        m.attach_risk_engine(engine)
        breaches = m.check_slo_breached()
        assert any("risk" in b for b in breaches)


# ---------------------------------------------------------------------------
# log_report
# ---------------------------------------------------------------------------


class TestLogReport:
    def test_log_report_does_not_raise(self, monitor: SLOMonitor):
        monitor.attach_db_manager(_make_db_manager())
        monitor.attach_risk_engine(_make_risk_engine())
        monitor.record_order(rejected=False)
        monitor.log_report()   # should not raise

    def test_log_report_with_empty_monitor(self, monitor: SLOMonitor):
        monitor.log_report()   # no attachments, no orders — should not raise


# ---------------------------------------------------------------------------
# SLO targets defaults
# ---------------------------------------------------------------------------


class TestSLOTargetsDefaults:
    def test_default_targets(self):
        t = SLOTargets()
        assert t.db_lock_failure_rate == 0.001
        assert t.db_lock_p95_ms == 200.0
        assert t.risk_halt_rate == 0.01
        assert t.order_reject_rate == 0.05

    def test_custom_targets(self):
        t = SLOTargets(risk_halt_rate=0.02)
        assert t.risk_halt_rate == 0.02
        assert t.order_reject_rate == 0.05   # default unchanged


# ---------------------------------------------------------------------------
# ErrorBudget
# ---------------------------------------------------------------------------


class TestErrorBudget:
    def test_default_budget_seconds(self):
        """99.9% SLO → 30d * 86400 * 0.001 = 2592 秒。"""
        eb = ErrorBudget(monthly_uptime_target=0.999)
        assert abs(eb._budget_seconds - 2592.0) < 0.1

    def test_no_downtime_not_exhausted(self):
        eb = ErrorBudget()
        assert not eb.is_exhausted()
        assert eb.remaining_seconds() > 0

    def test_record_downtime_accumulates(self):
        eb = ErrorBudget()
        eb.record_downtime(100)
        eb.record_downtime(50)
        assert abs(eb._consumed_seconds - 150.0) < 1e-9

    def test_record_negative_downtime_ignored(self):
        eb = ErrorBudget()
        eb.record_downtime(-100)
        assert eb._consumed_seconds == 0.0

    def test_is_exhausted_when_over_budget(self):
        eb = ErrorBudget(monthly_uptime_target=0.999)
        eb.record_downtime(eb._budget_seconds + 1)
        assert eb.is_exhausted()

    def test_remaining_negative_when_exhausted(self):
        eb = ErrorBudget(monthly_uptime_target=0.999)
        eb.record_downtime(eb._budget_seconds + 100)
        assert eb.remaining_seconds() < 0

    def test_burn_rate_zero_with_no_downtime(self):
        import time
        eb = ErrorBudget()
        time.sleep(0.05)   # ensure elapsed > 0
        # 0 consumed → burn rate = 0
        assert eb.burn_rate() == 0.0

    def test_burn_rate_equals_one_when_on_schedule(self):
        """消耗量恰好等于在窗口期内按预算速率应消耗的量，burn_rate 应接近 1.0。"""
        import time
        eb = ErrorBudget(monthly_uptime_target=0.999)
        elapsed = 0.1
        expected_consumed = (elapsed / eb.MONTH_SECONDS) * eb._budget_seconds
        eb.record_downtime(expected_consumed)
        time.sleep(elapsed)
        # burn_rate 应接近 1.0（允许小误差）
        assert 0.5 < eb.burn_rate() < 2.0

    def test_consumed_ratio_zero_on_init(self):
        eb = ErrorBudget()
        assert eb.consumed_ratio() == 0.0

    def test_consumed_ratio_over_one_when_exhausted(self):
        eb = ErrorBudget(monthly_uptime_target=0.999)
        eb.record_downtime(eb._budget_seconds * 2)
        assert eb.consumed_ratio() > 1.0

    def test_reset_clears_consumption(self):
        eb = ErrorBudget()
        eb.record_downtime(1000)
        eb.reset()
        assert eb._consumed_seconds == 0.0
        assert not eb.is_exhausted()

    def test_as_dict_keys(self):
        eb = ErrorBudget()
        d = eb.as_dict()
        for key in ("monthly_uptime_target", "budget_seconds", "consumed_seconds",
                    "remaining_seconds", "consumed_ratio", "burn_rate", "is_exhausted"):
            assert key in d, f"as_dict() missing key: {key}"

    def test_slo_monitor_has_error_budget(self):
        m = SLOMonitor()
        assert isinstance(m.error_budget, ErrorBudget)

    def test_slo_monitor_report_includes_error_budget(self):
        m = SLOMonitor()
        report = m.get_report()
        assert "error_budget" in report
        assert "is_exhausted" in report["error_budget"]

    def test_exhausted_budget_makes_overall_not_ok(self):
        eb = ErrorBudget(monthly_uptime_target=0.999)
        eb.record_downtime(eb._budget_seconds + 1)   # exhaust budget
        m = SLOMonitor(error_budget=eb)
        report = m.get_report()
        assert report["overall_ok"] is False

    def test_exhausted_budget_appears_in_breaches(self):
        eb = ErrorBudget(monthly_uptime_target=0.999)
        eb.record_downtime(eb._budget_seconds + 1)
        m = SLOMonitor(error_budget=eb)
        breaches = m.check_slo_breached()
        assert any("error_budget" in b for b in breaches)

    def test_reset_also_resets_error_budget(self):
        m = SLOMonitor()
        m.error_budget.record_downtime(m.error_budget._budget_seconds + 1)
        assert m.error_budget.is_exhausted()
        m.reset()
        assert not m.error_budget.is_exhausted()

    def test_consumed_ratio_zero_budget_returns_zero(self):
        """monthly_uptime_target=1.0 → _budget_seconds=0 → consumed_ratio returns 0.0 (line 127)."""
        eb = ErrorBudget(monthly_uptime_target=1.0)
        assert eb._budget_seconds == 0.0
        assert eb.consumed_ratio() == 0.0


# ---------------------------------------------------------------------------
# reset() exception handlers
# ---------------------------------------------------------------------------


class TestResetExceptionHandlers:
    def test_reset_db_manager_exception_swallowed(self):
        """reset_lock_metrics() 抛出时 reset() 不传播 (lines 195-196)."""
        monitor = SLOMonitor()
        mgr = MagicMock()
        mgr.reset_lock_metrics.side_effect = RuntimeError("DB gone")
        monitor.attach_db_manager(mgr)
        monitor.reset()  # must not raise

    def test_reset_risk_engine_exception_swallowed(self):
        """reset_risk_stats() 抛出时 reset() 不传播 (lines 200-201)."""
        monitor = SLOMonitor()
        engine = MagicMock()
        engine.reset_risk_stats.side_effect = RuntimeError("engine gone")
        monitor.attach_risk_engine(engine)
        monitor.reset()  # must not raise


# ---------------------------------------------------------------------------
# get_report() exception handlers
# ---------------------------------------------------------------------------


class TestGetReportExceptionHandlers:
    def test_get_lock_metrics_exception_returns_defaults(self):
        """get_lock_metrics() 抛出时 report db_lock 返回安全默认值 (lines 260-261)."""
        monitor = SLOMonitor()
        mgr = MagicMock()
        mgr.get_lock_metrics.side_effect = RuntimeError("DB gone")
        monitor.attach_db_manager(mgr)
        report = monitor.get_report()
        assert report["db_lock"]["failure_rate"] == 0.0
        assert report["db_lock"]["slo_ok"] is True

    def test_get_risk_stats_exception_returns_defaults(self):
        """get_risk_stats() 抛出时 report risk 返回安全默认值 (lines 295-296)."""
        monitor = SLOMonitor()
        engine = MagicMock()
        engine.get_risk_stats.side_effect = RuntimeError("engine gone")
        monitor.attach_risk_engine(engine)
        report = monitor.get_report()
        assert report["risk"]["total_checks"] == 0
        assert report["risk"]["slo_ok"] is True
