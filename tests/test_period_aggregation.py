"""验证派生周期 (15m/30m/60m/1w/1M) 的 OHLCV resample 聚合逻辑"""
from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture()
def resample_fn():
    """获取 UnifiedDataInterface._resample_ohlcv 静态方法"""
    from data_manager.unified_data_interface import UnifiedDataInterface
    return UnifiedDataInterface._resample_ohlcv


def _make_1m_data(n_bars: int = 60) -> pd.DataFrame:
    """生成 n_bars 根 1 分钟 K 线 (从 09:31 开始)"""
    idx = pd.date_range("2024-06-03 09:31", periods=n_bars, freq="min")
    return pd.DataFrame(
        {
            "open": range(100, 100 + n_bars),
            "high": range(101, 101 + n_bars),
            "low": range(99, 99 + n_bars),
            "close": range(100, 100 + n_bars),
            "volume": [1000] * n_bars,
            "amount": [50000.0] * n_bars,
            "stock_code": ["000001.SZ"] * n_bars,
        },
        index=idx,
    )


def _make_daily_data(n_days: int = 30) -> pd.DataFrame:
    """生成 n_days 根日 K 线"""
    idx = pd.bdate_range("2024-05-01", periods=n_days)
    return pd.DataFrame(
        {
            "open": range(10, 10 + n_days),
            "high": range(11, 11 + n_days),
            "low": range(9, 9 + n_days),
            "close": range(10, 10 + n_days),
            "volume": [100000] * n_days,
            "amount": [5000000.0] * n_days,
            "stock_code": ["000001.SZ"] * n_days,
        },
        index=idx,
    )


class TestResampleOHLCV:
    def test_15m_from_1m(self, resample_fn):
        df = _make_1m_data(60)
        result = resample_fn(df, "15min")
        assert result is not None and not result.empty
        # 09:31起60根 → 5 个 15 分钟 bucket (09:30/09:45/10:00/10:15/10:30)
        assert len(result) == 5
        # 第一根 (09:30 bucket): bars 09:31-09:44 = 14 bars
        first = result.iloc[0]
        assert first["open"] == 100  # first open
        assert first["volume"] == 14000  # 14 bars * 1000

    def test_30m_from_1m(self, resample_fn):
        df = _make_1m_data(60)
        result = resample_fn(df, "30min")
        assert result is not None
        # 09:31起60根 → 3 个 30 分钟 bucket
        assert len(result) == 3

    def test_60m_from_1m(self, resample_fn):
        df = _make_1m_data(60)
        result = resample_fn(df, "60min")
        assert result is not None
        # 09:31起60根 → 2 个 60 分钟 bucket (09:00/10:00)
        assert len(result) == 2

    def test_weekly_from_daily(self, resample_fn):
        df = _make_daily_data(30)
        result = resample_fn(df, "W")
        assert result is not None and not result.empty
        # 30 个交易日 ≈ 6 周
        assert 4 <= len(result) <= 7

    def test_monthly_from_daily(self, resample_fn):
        df = _make_daily_data(30)
        result = resample_fn(df, "ME")
        assert result is not None and not result.empty

    def test_empty_input(self, resample_fn):
        empty = pd.DataFrame()
        result = resample_fn(empty, "15min")
        assert result is not None and result.empty

    def test_none_input(self, resample_fn):
        result = resample_fn(None, "15min")
        assert result is None

    def test_stock_code_preserved(self, resample_fn):
        df = _make_1m_data(60)
        result = resample_fn(df, "15min")
        assert "stock_code" in result.columns
        assert result["stock_code"].iloc[0] == "000001.SZ"


class TestAggregationMap:
    def test_aggregation_map_exists(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        agg = UnifiedDataInterface._PERIOD_AGGREGATION
        # 自然日历周期：应在 _PERIOD_AGGREGATION 中（pandas resample）
        assert "1w" in agg
        assert "1M" in agg
        assert "1Q" in agg   # 自然季度
        assert "6M" in agg  # 半年度
        assert "1Y" in agg  # 年度
        # 日内自定义周期：应在 _INTRADAY_CUSTOM_PERIODS 中（由 PeriodBarBuilder 处理）
        intraday = UnifiedDataInterface._INTRADAY_CUSTOM_PERIODS
        assert "15m" in intraday
        assert "30m" in intraday
        assert "60m" in intraday
        assert "25m" in intraday  # 日内扬展周期
        assert "70m" in intraday
        # 多日自定义周期：应在 _MULTIDAY_CUSTOM_PERIODS 中
        multiday = UnifiedDataInterface._MULTIDAY_CUSTOM_PERIODS
        assert "5d" in multiday  # 5交易日 ≠ 1W
        assert "3M" in multiday  # 63交易日 ≠ 1Q
        # 源周期不在任何派生表中
        assert "1m" not in agg
        assert "5m" not in agg
        assert "1d" not in agg


class TestPeriodMaps:
    def test_period_table_map_source_tables(self):
        from gui_app.widgets.chart.subchart_manager import PERIOD_TABLE_MAP
        # 派生周期应指向源表
        assert PERIOD_TABLE_MAP["15m"] == "stock_1m"
        assert PERIOD_TABLE_MAP["30m"] == "stock_1m"
        assert PERIOD_TABLE_MAP["60m"] == "stock_1m"
        assert PERIOD_TABLE_MAP["1w"] == "stock_daily"
        assert PERIOD_TABLE_MAP["1M"] == "stock_daily"

    def test_period_date_col_map(self):
        from gui_app.widgets.chart.subchart_manager import PERIOD_DATE_COL_MAP
        assert PERIOD_DATE_COL_MAP["15m"] == ("stock_1m", "datetime")
        assert PERIOD_DATE_COL_MAP["1w"] == ("stock_daily", "date")
