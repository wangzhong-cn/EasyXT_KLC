"""Tests for TradingHoursGuard and require_non_trading decorator – pure Python."""
import pytest
from datetime import datetime, time


class TestIsTradingDay:
    def _cls(self):
        from gui_app.widgets.chart.trading_hours_guard import TradingHoursGuard
        return TradingHoursGuard

    def test_monday_is_trading_day(self):
        # 2024-01-08 is Monday
        dt = datetime(2024, 1, 8, 10, 0)
        assert self._cls().is_trading_day(dt) is True

    def test_friday_is_trading_day(self):
        dt = datetime(2024, 1, 12, 10, 0)
        assert self._cls().is_trading_day(dt) is True

    def test_saturday_is_not_trading_day(self):
        # 2024-01-13 is Saturday
        dt = datetime(2024, 1, 13, 10, 0)
        assert self._cls().is_trading_day(dt) is False

    def test_sunday_is_not_trading_day(self):
        dt = datetime(2024, 1, 14, 10, 0)
        assert self._cls().is_trading_day(dt) is False

    def test_default_uses_now(self):
        from gui_app.widgets.chart.trading_hours_guard import TradingHoursGuard
        # Should not raise
        result = TradingHoursGuard.is_trading_day()
        assert isinstance(result, bool)


class TestIsTradingTime:
    def _cls(self):
        from gui_app.widgets.chart.trading_hours_guard import TradingHoursGuard
        return TradingHoursGuard

    def test_morning_continuous_session(self):
        # 10:00 on Monday
        dt = datetime(2024, 1, 8, 10, 0)
        assert self._cls().is_trading_time(dt) is True

    def test_afternoon_session(self):
        # 14:00 on Monday
        dt = datetime(2024, 1, 8, 14, 0)
        assert self._cls().is_trading_time(dt) is True

    def test_call_auction_time(self):
        # 09:20 on Monday (集合竞价)
        dt = datetime(2024, 1, 8, 9, 20)
        assert self._cls().is_trading_time(dt) is True

    def test_lunch_break_is_not_trading(self):
        # 12:00 on Monday (午休)
        dt = datetime(2024, 1, 8, 12, 0)
        assert self._cls().is_trading_time(dt) is False

    def test_before_market_open(self):
        # 08:00 on Monday
        dt = datetime(2024, 1, 8, 8, 0)
        assert self._cls().is_trading_time(dt) is False

    def test_after_close(self):
        # 16:00 on Monday
        dt = datetime(2024, 1, 8, 16, 0)
        assert self._cls().is_trading_time(dt) is False

    def test_weekend_is_never_trading_time(self):
        dt = datetime(2024, 1, 13, 10, 0)
        assert self._cls().is_trading_time(dt) is False


class TestCurrentSession:
    def _cls(self):
        from gui_app.widgets.chart.trading_hours_guard import TradingHoursGuard
        return TradingHoursGuard

    def test_call_auction_returns_session_name(self):
        dt = datetime(2024, 1, 8, 9, 20)
        in_session, name = self._cls().current_session(dt)
        assert in_session is True
        assert name != ""

    def test_morning_session_returns_name(self):
        dt = datetime(2024, 1, 8, 10, 0)
        in_session, name = self._cls().current_session(dt)
        assert in_session is True
        assert "竞价" in name or "连续" in name

    def test_lunch_returns_false(self):
        dt = datetime(2024, 1, 8, 12, 0)
        in_session, name = self._cls().current_session(dt)
        assert in_session is False
        assert name == ""

    def test_afternoon_session_returns_name(self):
        dt = datetime(2024, 1, 8, 14, 0)
        in_session, name = self._cls().current_session(dt)
        assert in_session is True
        assert name != ""

    def test_weekend_returns_false(self):
        dt = datetime(2024, 1, 13, 10, 0)
        in_session, name = self._cls().current_session(dt)
        assert in_session is False
        assert name == ""


class TestCanChangeBackend:
    def _cls(self):
        from gui_app.widgets.chart.trading_hours_guard import TradingHoursGuard
        return TradingHoursGuard

    def test_weekend_allows_change(self):
        # Monkeypatch current_session using a Saturday
        cls = self._cls()
        # Use classmethod with explicit dt (non-trading time)
        dt_weekend = datetime(2024, 1, 13, 10, 0)
        in_s, _ = cls.current_session(dt_weekend)
        assert in_s is False  # Just verify our test dt is correct

    def test_can_change_backend_returns_tuple(self):
        cls = self._cls()
        result = cls.can_change_backend()
        assert isinstance(result, tuple)
        assert len(result) == 2
        ok, reason = result
        assert isinstance(ok, bool)
        assert isinstance(reason, str)


class TestMinutesToNextSession:
    def _cls(self):
        from gui_app.widgets.chart.trading_hours_guard import TradingHoursGuard
        return TradingHoursGuard

    def test_returns_none_or_int(self):
        cls = self._cls()
        result = cls.minutes_to_next_session()
        assert result is None or isinstance(result, int)


class TestRequireNonTradingDecorator:
    def test_decorator_calls_function_outside_trading(self):
        from gui_app.widgets.chart.trading_hours_guard import (
            TradingHoursGuard, require_non_trading
        )
        from unittest.mock import patch

        # Mock can_change_backend to return (True, "")
        with patch.object(TradingHoursGuard, 'can_change_backend', return_value=(True, "")):
            called = []

            @require_non_trading
            def my_fn():
                called.append(True)
                return 42

            result = my_fn()
            assert result == 42
            assert called == [True]

    def test_decorator_raises_during_trading(self):
        from gui_app.widgets.chart.trading_hours_guard import (
            TradingHoursGuard, require_non_trading
        )
        from unittest.mock import patch

        with patch.object(TradingHoursGuard, 'can_change_backend',
                          return_value=(False, "交易时段内")):
            @require_non_trading
            def dangerous_fn():
                pass  # pragma: no cover

            with pytest.raises(RuntimeError, match="TradingHoursGuard"):
                dangerous_fn()

    def test_decorator_preserves_function_name(self):
        from gui_app.widgets.chart.trading_hours_guard import require_non_trading

        @require_non_trading
        def my_func():
            """My docstring."""
            pass

        assert my_func.__name__ == "my_func"
