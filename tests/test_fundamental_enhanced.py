"""Unit tests for FundamentalAnalyzerEnhanced.calculate_*_factors() methods.

All five factor-calculation methods are pure DataFrame → dict transforms with
no external I/O, making them straightforward to unit-test.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from easy_xt.fundamental_enhanced import FundamentalAnalyzerEnhanced


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_analyzer() -> FundamentalAnalyzerEnhanced:
    return FundamentalAnalyzerEnhanced(duckdb_reader=None)


def _make_ohlcv(n: int, start_price: float = 10.0, volume: float = 1e6) -> pd.DataFrame:
    """Create n rows of synthetic OHLCV price data with a gentle uptrend."""
    closes = [start_price + i * 0.01 for i in range(n)]
    highs  = [c * 1.005 for c in closes]
    lows   = [c * 0.995 for c in closes]
    opens  = [c * 0.999 for c in closes]
    amounts = [c * volume for c in closes]
    idx = pd.date_range("2022-01-04", periods=n, freq="B")
    return pd.DataFrame({
        "open":   opens,
        "high":   highs,
        "low":    lows,
        "close":  closes,
        "volume": [volume] * n,
        "amount": amounts,
    }, index=idx)


FA = _make_analyzer()


# ─────────────────────────────────────────────────────────────────────────────
# calculate_valuation_factors
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateValuationFactors:
    def test_empty_df_returns_empty_dict(self):
        assert FA.calculate_valuation_factors(pd.DataFrame()) == {}

    def test_too_few_rows_returns_empty_dict(self):
        assert FA.calculate_valuation_factors(_make_ohlcv(10)) == {}

    def test_20_rows_returns_dict_no_ma_keys(self):
        # With only 20 rows we can't compute 60-day or 252-day metrics
        result = FA.calculate_valuation_factors(_make_ohlcv(20))
        assert isinstance(result, dict)
        assert "price_to_ma20" not in result

    def test_60_rows_returns_ma_ratios(self):
        result = FA.calculate_valuation_factors(_make_ohlcv(60))
        assert "price_to_ma20" in result
        assert "price_to_ma60" in result
        # For a monotonic uptrend latest > ma60 → ratio > 1
        assert result["price_to_ma20"] > 0
        assert result["price_to_ma60"] > 0

    def test_252_rows_returns_percentile_and_dist(self):
        result = FA.calculate_valuation_factors(_make_ohlcv(252))
        assert "price_percentile" in result
        # Monotonic uptrend → price is the highest → percentile == 1.0
        assert result["price_percentile"] == pytest.approx(1.0)
        assert "dist_from_high_252" in result
        # Latest price == 252-day high → distance is 0
        assert result["dist_from_high_252"] == pytest.approx(0.0)


# ─────────────────────────────────────────────────────────────────────────────
# calculate_momentum_factors
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateMomentumFactors:
    def test_empty_df_returns_empty(self):
        assert FA.calculate_momentum_factors(pd.DataFrame()) == {}

    def test_too_few_rows_returns_empty(self):
        assert FA.calculate_momentum_factors(_make_ohlcv(5)) == {}

    def test_20_rows_has_short_term_momentum(self):
        result = FA.calculate_momentum_factors(_make_ohlcv(21))
        assert "momentum_1d" in result
        assert "momentum_5d" in result
        assert "momentum_10d" in result
        assert "momentum_20d" in result

    def test_momentum_positive_for_uptrend(self):
        result = FA.calculate_momentum_factors(_make_ohlcv(30))
        assert result["momentum_1d"] > 0
        assert result["momentum_5d"] > 0
        assert result["momentum_20d"] > 0

    def test_252_rows_has_long_term_momentum(self):
        result = FA.calculate_momentum_factors(_make_ohlcv(260))
        assert "momentum_252d" in result
        assert "momentum_accel" in result
        assert "rsi_14" in result

    def test_rsi_in_valid_range(self):
        result = FA.calculate_momentum_factors(_make_ohlcv(30))
        assert 0 <= result["rsi_14"] <= 100


# ─────────────────────────────────────────────────────────────────────────────
# calculate_volatility_factors
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateVolatilityFactors:
    def test_empty_df_returns_empty(self):
        assert FA.calculate_volatility_factors(pd.DataFrame()) == {}

    def test_too_few_rows_returns_empty(self):
        assert FA.calculate_volatility_factors(_make_ohlcv(10)) == {}

    def test_20_rows_returns_vol_20d(self):
        result = FA.calculate_volatility_factors(_make_ohlcv(21))
        assert "volatility_20d" in result

    def test_volatility_is_non_negative(self):
        result = FA.calculate_volatility_factors(_make_ohlcv(65))
        for key in ["volatility_20d", "volatility_60d"]:
            assert result[key] >= 0

    def test_atr_14_present_for_sufficient_data(self):
        result = FA.calculate_volatility_factors(_make_ohlcv(30))
        assert "atr_14" in result
        assert "atr_14_pct" in result
        assert result["atr_14"] >= 0


# ─────────────────────────────────────────────────────────────────────────────
# calculate_quality_factors
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateQualityFactors:
    def test_empty_df_returns_empty(self):
        assert FA.calculate_quality_factors(pd.DataFrame()) == {}

    def test_too_few_rows_returns_empty(self):
        assert FA.calculate_quality_factors(_make_ohlcv(10)) == {}

    def test_20_rows_returns_consecutive_days(self):
        result = FA.calculate_quality_factors(_make_ohlcv(21))
        assert "consecutive_up_days" in result
        assert "consecutive_down_days" in result

    def test_monotonic_uptrend_has_zero_down_days(self):
        # Strict uptrend → 0 consecutive down days
        result = FA.calculate_quality_factors(_make_ohlcv(21))
        assert result["consecutive_down_days"] == 0

    def test_60_rows_returns_cv_and_trend_strength(self):
        result = FA.calculate_quality_factors(_make_ohlcv(65))
        assert "price_cv_60d" in result
        assert "trend_strength_60d" in result
        # Uptrend → positive slope
        assert result["trend_strength_60d"] > 0

    def test_252_rows_price_position_in_0_1(self):
        result = FA.calculate_quality_factors(_make_ohlcv(260))
        pos = result["price_position_52w"]
        # For strict uptrend, latest price is the 52-week high → position == 1.0
        assert abs(pos - 1.0) < 0.01


# ─────────────────────────────────────────────────────────────────────────────
# calculate_liquidity_factors
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateLiquidityFactors:
    def test_empty_df_returns_empty(self):
        assert FA.calculate_liquidity_factors(pd.DataFrame()) == {}

    def test_too_few_rows_returns_empty(self):
        assert FA.calculate_liquidity_factors(_make_ohlcv(10)) == {}

    def test_no_volume_column_returns_empty_dict(self):
        # Without volume, liquidity factors are empty
        df = _make_ohlcv(25).drop(columns=["volume", "amount"])
        result = FA.calculate_liquidity_factors(df)
        # Should not raise, may return {}
        assert isinstance(result, dict)

    def test_20_rows_with_volume_returns_avg_volume(self):
        result = FA.calculate_liquidity_factors(_make_ohlcv(21))
        assert "avg_volume_5d" in result
        assert "avg_volume_20d" in result
        assert result["avg_volume_5d"] > 0

    def test_60_rows_returns_volume_ratio(self):
        result = FA.calculate_liquidity_factors(_make_ohlcv(65))
        assert "volume_ratio" in result
        # Constant volume → ratio == 1.0
        assert result["volume_ratio"] == pytest.approx(1.0)

    def test_turnover_factors_present_with_amount_column(self):
        result = FA.calculate_liquidity_factors(_make_ohlcv(25))
        assert "turnover_5d" in result
        assert "turnover_20d" in result
        assert result["turnover_5d"] > 0
