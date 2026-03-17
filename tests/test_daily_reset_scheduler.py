"""Unit tests for core.daily_reset_scheduler.DailyResetScheduler."""
import threading
import time
from unittest.mock import MagicMock, call, patch

import pytest

from core.daily_reset_scheduler import DailyResetScheduler


def _make(risk=None, slo=None, account_ids=None, check_interval=5.0, **kw):
    return DailyResetScheduler(
        risk_engine=risk,
        slo_monitor=slo,
        account_ids=account_ids,
        check_interval_s=check_interval,
        **kw,
    )


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------
class TestConstructor:
    def test_defaults(self):
        s = _make()
        assert s._market_open_time == "09:25:00"
        assert s._midnight_reset_time == "00:01:00"
        assert s._account_ids == []
        assert s._thread is None

    def test_check_interval_minimum_is_5(self):
        s = _make(check_interval=1.0)
        assert s._check_interval == 5.0

    def test_custom_times(self):
        s = _make(market_open_time="09:30:00", midnight_reset_time="00:00:30")
        assert s._market_open_time == "09:30:00"
        assert s._midnight_reset_time == "00:00:30"

    def test_account_ids_stored(self):
        s = _make(account_ids=["A1", "A2"])
        assert s._account_ids == ["A1", "A2"]


# ---------------------------------------------------------------------------
# force_reset_now
# ---------------------------------------------------------------------------
class TestForceResetNow:
    def test_force_all_calls_both_resets(self):
        risk = MagicMock()
        slo = MagicMock()
        s = _make(risk=risk, slo=slo)
        s.force_reset_now("all")
        risk.reset_daily_state.assert_called_once_with(None)
        risk.reset_risk_stats.assert_called_once()
        slo.reset.assert_called_once()

    def test_force_open_only(self):
        risk = MagicMock()
        slo = MagicMock()
        s = _make(risk=risk, slo=slo)
        s.force_reset_now("open")
        risk.reset_daily_state.assert_called_once()
        risk.reset_risk_stats.assert_not_called()
        slo.reset.assert_not_called()

    def test_force_midnight_only(self):
        risk = MagicMock()
        slo = MagicMock()
        s = _make(risk=risk, slo=slo)
        s.force_reset_now("midnight")
        risk.reset_daily_state.assert_not_called()
        risk.reset_risk_stats.assert_called_once()
        slo.reset.assert_called_once()

    def test_force_reset_no_risk_engine(self):
        # must not raise even with None risk_engine/slo_monitor
        s = _make()
        s.force_reset_now("all")

    def test_force_reset_with_account_ids(self):
        risk = MagicMock()
        s = _make(risk=risk, account_ids=["ACC1", "ACC2"])
        s.force_reset_now("open")
        assert risk.reset_daily_state.call_count == 2
        risk.reset_daily_state.assert_any_call("ACC1")
        risk.reset_daily_state.assert_any_call("ACC2")

    def test_risk_engine_exception_does_not_propagate(self):
        risk = MagicMock()
        risk.reset_daily_state.side_effect = RuntimeError("boom")
        s = _make(risk=risk)
        s.force_reset_now("open")  # must not raise


# ---------------------------------------------------------------------------
# _check_and_reset  (time-mocked)
# ---------------------------------------------------------------------------
class TestCheckAndReset:
    def _scheduler_at(self, time_str: str, risk=None, slo=None, accounts=None):
        """Return a scheduler with frozen clock at given HH:MM:SS (today)."""
        from datetime import datetime
        from zoneinfo import ZoneInfo
        s = _make(
            risk=risk,
            slo=slo,
            account_ids=accounts,
            market_open_time="09:25:00",
            midnight_reset_time="00:01:00",
        )
        with patch("core.daily_reset_scheduler.datetime") as mock_dt:
            today = datetime(2025, 6, 15, *map(int, time_str.split(":")),
                             tzinfo=ZoneInfo("Asia/Shanghai"))
            mock_dt.now.return_value = today
            s._check_and_reset()
        return s

    def test_open_reset_triggered_after_market_open(self):
        risk = MagicMock()
        s = self._scheduler_at("09:26:00", risk=risk)
        risk.reset_daily_state.assert_called_once()

    def test_open_reset_not_triggered_before_market_open(self):
        risk = MagicMock()
        s = self._scheduler_at("09:24:59", risk=risk)
        risk.reset_daily_state.assert_not_called()

    def test_midnight_reset_triggered(self):
        risk = MagicMock()
        slo = MagicMock()
        s = self._scheduler_at("00:02:00", risk=risk, slo=slo)
        risk.reset_risk_stats.assert_called_once()
        slo.reset.assert_called_once()

    def test_midnight_reset_not_triggered_before_time(self):
        risk = MagicMock()
        slo = MagicMock()
        s = self._scheduler_at("00:00:59", risk=risk, slo=slo)
        risk.reset_risk_stats.assert_not_called()

    def test_dedup_prevents_double_reset(self):
        """_check_and_reset called twice in the same minute → resets only once."""
        from datetime import datetime
        from zoneinfo import ZoneInfo
        risk = MagicMock()
        s = _make(risk=risk, market_open_time="09:25:00", midnight_reset_time="00:01:00")
        frozen_now = datetime(2025, 6, 15, 9, 26, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        with patch("core.daily_reset_scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = frozen_now
            s._check_and_reset()
            s._check_and_reset()
        assert risk.reset_daily_state.call_count == 1


# ---------------------------------------------------------------------------
# start / stop
# ---------------------------------------------------------------------------
class TestStartStop:
    def test_start_creates_thread(self):
        s = _make()
        s.start()
        assert s._thread is not None and s._thread.is_alive()
        s.stop()

    def test_start_is_idempotent(self):
        s = _make()
        s.start()
        t1 = s._thread
        s.start()  # second call should be ignored
        assert s._thread is t1
        s.stop()

    def test_stop_terminates_thread(self):
        s = _make(check_interval=5.0)
        s.start()
        s.stop()
        assert not s._thread.is_alive()

    def test_stop_without_start_does_not_raise(self):
        s = _make()
        s.stop()

    def test_repr_shows_not_running_before_start(self):
        s = _make()
        assert "running=False" in repr(s)

    def test_repr_shows_running_after_start(self):
        s = _make()
        s.start()
        try:
            assert "running=True" in repr(s)
        finally:
            s.stop()
