"""Unit tests for EnhancedIndicators pure-logic helper methods.

Tests all private helpers that contain signal/classification logic with no
external dependencies (no real data_api needed).
"""
import pandas as pd
import pytest

from easy_xt.enhanced_indicators import EnhancedIndicators


def _make_ei():
    """Return an EnhancedIndicators with a stub data_api (never called here)."""
    import types
    stub = types.SimpleNamespace()
    return EnhancedIndicators(data_api=stub)  # type: ignore[arg-type]


EI = _make_ei()


# ─────────────────────────────────────────────────────────────
# _check_macd_cross
# ─────────────────────────────────────────────────────────────
class TestCheckMacdCross:
    def _cross(self, macd_vals, signal_vals):
        return EI._check_macd_cross(
            pd.Series(macd_vals), pd.Series(signal_vals)
        )

    def test_golden_cross(self):
        # macd crossed above signal on the last bar
        assert self._cross([0.5, 1.0, 1.5], [1.0, 1.2, 1.4]) == "golden"

    def test_death_cross(self):
        # macd crossed below signal on the last bar
        assert self._cross([1.5, 1.0, 0.5], [1.0, 0.8, 0.6]) == "death"

    def test_no_cross_returns_hold(self):
        # macd consistently above signal — no new cross
        assert self._cross([2.0, 2.5, 3.0], [1.0, 1.2, 1.5]) == "hold"

    def test_too_few_points_returns_hold(self):
        assert self._cross([1.0], [0.9]) == "hold"

    def test_exactly_two_points_golden(self):
        # prev: macd(0.5) < signal(1.0), now: macd(1.5) > signal(1.0)
        assert self._cross([0.5, 1.5], [1.0, 1.0]) == "golden"

    def test_exactly_at_boundary_hold(self):
        # equal on both bars — no cross
        assert self._cross([1.0, 1.0], [1.0, 1.0]) == "hold"


# ─────────────────────────────────────────────────────────────
# _analyze_kdj_signal
# ─────────────────────────────────────────────────────────────
class TestAnalyzeKdjSignal:
    def test_all_below_20_gives_buy(self):
        assert EI._analyze_kdj_signal(k=15, d=18, j=10) == "buy"

    def test_all_above_80_gives_sell(self):
        assert EI._analyze_kdj_signal(k=85, d=82, j=90) == "sell"

    def test_golden_cross_up_gives_buy(self):
        # k > d and j > k
        assert EI._analyze_kdj_signal(k=55, d=50, j=60) == "buy"

    def test_death_cross_down_gives_sell(self):
        # k < d and j < k
        assert EI._analyze_kdj_signal(k=45, d=50, j=40) == "sell"

    def test_neutral_gives_hold(self):
        assert EI._analyze_kdj_signal(k=50, d=50, j=50) == "hold"

    def test_boundary_k_exactly_20_not_buy(self):
        # threshold is < 20, not <=
        result = EI._analyze_kdj_signal(k=20, d=20, j=20)
        assert result != "buy"


# ─────────────────────────────────────────────────────────────
# _analyze_boll_position
# ─────────────────────────────────────────────────────────────
class TestAnalyzeBollPosition:
    def test_price_above_upper(self):
        assert EI._analyze_boll_position(12.0, upper=10.0, lower=8.0, middle=9.0) == "above_upper"

    def test_price_below_lower(self):
        assert EI._analyze_boll_position(7.0, upper=10.0, lower=8.0, middle=9.0) == "below_lower"

    def test_price_in_upper_half(self):
        assert EI._analyze_boll_position(9.5, upper=10.0, lower=8.0, middle=9.0) == "upper_half"

    def test_price_in_lower_half(self):
        assert EI._analyze_boll_position(8.5, upper=10.0, lower=8.0, middle=9.0) == "lower_half"

    def test_price_exactly_at_middle_is_lower_half(self):
        # Not > middle → lower_half
        assert EI._analyze_boll_position(9.0, upper=10.0, lower=8.0, middle=9.0) == "lower_half"


# ─────────────────────────────────────────────────────────────
# _analyze_boll_signal
# ─────────────────────────────────────────────────────────────
class TestAnalyzeBollSignal:
    def test_percent_b_negative_gives_buy(self):
        assert EI._analyze_boll_signal(-0.1, "below_lower") == "buy"

    def test_percent_b_above_1_gives_sell(self):
        assert EI._analyze_boll_signal(1.1, "above_upper") == "sell"

    def test_percent_b_low_in_lower_half_gives_buy(self):
        assert EI._analyze_boll_signal(0.1, "lower_half") == "buy"

    def test_percent_b_high_in_upper_half_gives_sell(self):
        assert EI._analyze_boll_signal(0.9, "upper_half") == "sell"

    def test_middle_percent_b_gives_hold(self):
        assert EI._analyze_boll_signal(0.5, "upper_half") == "hold"

    def test_low_percent_b_but_upper_half_gives_hold(self):
        # percent_b < 0.2 but position is upper_half → hold
        assert EI._analyze_boll_signal(0.1, "upper_half") == "hold"


# ─────────────────────────────────────────────────────────────
# _analyze_rsi_signal
# ─────────────────────────────────────────────────────────────
class TestAnalyzeRsiSignal:
    def test_oversold_below_30_gives_buy(self):
        assert EI._analyze_rsi_signal(25.0, 28.0) == "buy"

    def test_overbought_above_70_gives_sell(self):
        assert EI._analyze_rsi_signal(75.0, 68.0) == "sell"

    def test_cross_above_50_gives_buy(self):
        # prev <= 50, current > 50
        assert EI._analyze_rsi_signal(52.0, 49.0) == "buy"

    def test_cross_below_50_gives_sell(self):
        # prev >= 50, current < 50
        assert EI._analyze_rsi_signal(48.0, 51.0) == "sell"

    def test_neutral_gives_hold(self):
        assert EI._analyze_rsi_signal(55.0, 53.0) == "hold"

    def test_exactly_at_50_stays_hold(self):
        # current == 50, crossing check: 50 > 50 is False → hold
        assert EI._analyze_rsi_signal(50.0, 49.0) == "hold"

    def test_boundary_30_exactly_is_hold_not_buy(self):
        # threshold is < 30, not <=
        assert EI._analyze_rsi_signal(30.0, 31.0) == "hold"

    def test_boundary_70_exactly_is_hold_not_sell(self):
        assert EI._analyze_rsi_signal(70.0, 68.0) == "hold"


# ─────────────────────────────────────────────────────────────
# _check_rsi_divergence
# ─────────────────────────────────────────────────────────────
class TestCheckRsiDivergence:
    def _divergence(self, price_vals, rsi_vals):
        return EI._check_rsi_divergence(pd.Series(price_vals), pd.Series(rsi_vals))

    def test_bullish_divergence_detected(self):
        # Price fell but RSI rose → divergence
        prices = [10, 9, 8, 9, 7]   # last < third-to-last → falling
        rsi    = [40, 42, 44, 46, 48]  # rising
        assert self._divergence(prices, rsi) == True  # noqa: E712 (numpy bool compat)

    def test_no_divergence_when_both_trend_same(self):
        # Both rising
        prices = [7, 8, 9, 10, 11]
        rsi    = [40, 45, 50, 55, 60]
        assert self._divergence(prices, rsi) == False  # noqa: E712

    def test_too_few_points_returns_false(self):
        assert self._divergence([1, 2, 3], [40, 45, 50]) is False

    def test_bearish_divergence_detected(self):
        # Price rose but RSI fell → divergence
        prices = [7, 8, 9, 10, 12]  # rising
        rsi    = [60, 58, 55, 52, 50]  # falling
        assert self._divergence(prices, rsi) == True  # noqa: E712
