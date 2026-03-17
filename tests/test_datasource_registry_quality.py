"""
tests/test_datasource_registry_quality.py
单元测试：data_manager.datasource_registry — DataSourceRegistry 的 4 维质量门禁逻辑
"""

import pandas as pd
import pytest

from data_manager.datasource_registry import DataSource, DataSourceRegistry


# ---------------------------------------------------------------------------
# 辅助工厂
# ---------------------------------------------------------------------------


def _make_df(
    n_rows: int = 60,
    close_invalid_pct: float = 0.0,   # 0.0 → 全部有效
    nan_close_pct: float = 0.0,
    nan_volume_pct: float = 0.0,
    include_volume: bool = True,
    include_open: bool = True,
    include_high: bool = True,
    include_low: bool = True,
) -> pd.DataFrame:
    """生成标准 OHLCV DataFrame 用于测试。"""
    dates = pd.date_range("2024-01-02", periods=n_rows, freq="B")
    closes = [100.0 + i * 0.1 for i in range(n_rows)]

    # 注入无效 close（<=0）
    n_invalid = int(n_rows * close_invalid_pct)
    for i in range(n_invalid):
        closes[i] = 0.0

    df = pd.DataFrame({"close": closes}, index=dates)

    # NaN 注入
    n_nan_close = int(n_rows * nan_close_pct)
    if n_nan_close:
        df.loc[df.index[:n_nan_close], "close"] = float("nan")

    if include_open:
        df["open"] = df["close"] - 1.0
    if include_high:
        df["high"] = df["close"] + 2.0
    if include_low:
        df["low"] = df["close"] - 2.0
    if include_volume:
        df["volume"] = [10000.0 + i * 50 for i in range(n_rows)]
        n_nan_vol = int(n_rows * nan_volume_pct)
        if n_nan_vol:
            df.loc[df.index[:n_nan_vol], "volume"] = float("nan")

    return df


def _registry(**overrides) -> DataSourceRegistry:
    """构造注册表（不连接真实数据源）。"""
    defaults = dict(
        required_fields=None,
        max_nan_rate=0.05,
        min_close_valid_rate=0.50,
        min_date_coverage=0.80,
    )
    defaults.update(overrides)
    return DataSourceRegistry(**defaults)


# ---------------------------------------------------------------------------
# 1. 必填字段检查
# ---------------------------------------------------------------------------


class TestRequiredFieldsCheck:
    def test_all_fields_present_passes(self):
        reg = _registry()
        df = _make_df()
        reason = reg._quality_check("src", df, "2024-01-02", "2024-03-30", "1d")
        assert reason is None

    def test_missing_volume_fails(self):
        reg = _registry()
        df = _make_df(include_volume=False)
        reason = reg._quality_check("src", df, "2024-01-02", "2024-03-30", "1d")
        assert reason is not None
        assert "volume" in reason.lower() or "缺少" in reason or "missing" in reason.lower()

    def test_missing_open_fails(self):
        reg = _registry()
        df = _make_df(include_open=False)
        reason = reg._quality_check("src", df, "2024-01-02", "2024-03-30", "1d")
        assert reason is not None

    def test_missing_high_fails(self):
        reg = _registry()
        df = _make_df(include_high=False)
        reason = reg._quality_check("src", df, "2024-01-02", "2024-03-30", "1d")
        assert reason is not None

    def test_missing_low_fails(self):
        reg = _registry()
        df = _make_df(include_low=False)
        reason = reg._quality_check("src", df, "2024-01-02", "2024-03-30", "1d")
        assert reason is not None

    def test_custom_required_fields_respected(self):
        """指定 required_fields=["close"] 时只检查 close。"""
        reg = _registry(required_fields=["close"])
        df = _make_df(include_volume=False, include_open=False, include_high=False)
        reason = reg._quality_check("src", df, "2024-01-02", "2024-03-30", "1d")
        assert reason is None  # 只要求 close，无需 open/high/volume

    def test_empty_dataframe_fails(self):
        reg = _registry()
        df = pd.DataFrame()
        reason = reg._quality_check("src", df, "2024-01-02", "2024-03-30", "1d")
        assert reason is not None


# ---------------------------------------------------------------------------
# 2. close 有效率检查（> 0）
# ---------------------------------------------------------------------------


class TestCloseValidityRate:
    def test_all_positive_passes(self):
        reg = _registry()
        df = _make_df(close_invalid_pct=0.0)
        reason = reg._quality_check("src", df, "2024-01-02", "2024-03-30", "1d")
        assert reason is None

    def test_60_pct_zero_close_fails_default(self):
        """默认阈值 50%：超过 60% 无效 close → 拒绝。"""
        reg = _registry(min_close_valid_rate=0.50)
        df = _make_df(n_rows=100, close_invalid_pct=0.60)
        reason = reg._quality_check("src", df, "2024-01-02", "2025-01-01", "1d")
        assert reason is not None

    def test_exactly_threshold_passes(self):
        """有效率 > min_close_valid_rate → 应通过。"""
        reg = _registry(min_close_valid_rate=0.50)
        n = 100
        # 使用约 100 个工作日的区间（2024-01-02 ~ 2024-05-22 ≈ 100 个交易日）
        start, end = "2024-01-02", "2024-05-22"
        df2 = _make_df(n_rows=n, close_invalid_pct=0.49)   # 51% 有效 > 50%
        reason2 = reg._quality_check("src", df2, start, end, "1d")
        assert reason2 is None

    def test_custom_valid_rate_threshold(self):
        reg = _registry(min_close_valid_rate=0.90)
        df = _make_df(n_rows=100, close_invalid_pct=0.15)  # 85% 有效 < 90%
        reason = reg._quality_check("src", df, "2024-01-02", "2025-01-01", "1d")
        assert reason is not None


# ---------------------------------------------------------------------------
# 3. NaN 率检查
# ---------------------------------------------------------------------------


class TestNanRateCheck:
    def test_no_nan_passes(self):
        reg = _registry(max_nan_rate=0.05)
        df = _make_df(nan_close_pct=0.0, nan_volume_pct=0.0)
        reason = reg._quality_check("src", df, "2024-01-02", "2024-03-30", "1d")
        assert reason is None

    def test_high_close_nan_fails(self):
        reg = _registry(max_nan_rate=0.05)
        df = _make_df(n_rows=100, nan_close_pct=0.10)   # 10% NaN > 5%
        reason = reg._quality_check("src", df, "2024-01-02", "2025-01-01", "1d")
        assert reason is not None

    def test_high_volume_nan_fails(self):
        reg = _registry(max_nan_rate=0.05)
        df = _make_df(n_rows=100, nan_volume_pct=0.10)   # 10% NaN > 5%
        reason = reg._quality_check("src", df, "2024-01-02", "2025-01-01", "1d")
        assert reason is not None

    def test_nan_rate_below_threshold_passes(self):
        reg = _registry(max_nan_rate=0.05)
        # 约 100 个工作日区间，与 n_rows=100 匹配，避免日期覆盖率检查误触发
        df = _make_df(n_rows=100, nan_close_pct=0.03)   # 3% NaN < 5%
        reason = reg._quality_check("src", df, "2024-01-02", "2024-05-22", "1d")
        assert reason is None

    def test_custom_higher_nan_threshold(self):
        reg = _registry(max_nan_rate=0.20)
        df = _make_df(n_rows=100, nan_close_pct=0.15)   # 15% NaN < 20%
        # 约 100 个工作日区间，避免日期覆盖率检查误触发
        reason = reg._quality_check("src", df, "2024-01-02", "2024-05-22", "1d")
        assert reason is None


# ---------------------------------------------------------------------------
# 4. 日期覆盖率检查（仅 1d 周期）
# ---------------------------------------------------------------------------


class TestDateCoverageCheck:
    def test_full_coverage_passes(self):
        reg = _registry(min_date_coverage=0.80)
        # 10 个交易日区间，提供 10 行
        df = _make_df(n_rows=10)
        # 近似 10 个工作日
        reason = reg._quality_check("src", df, "2024-01-02", "2024-01-13", "1d")
        assert reason is None

    def test_low_coverage_fails(self):
        reg = _registry(min_date_coverage=0.80)
        # 区间约 52 个工作日，但只提供 10 行 → 覆盖率 ~19%
        df = _make_df(n_rows=10)
        reason = reg._quality_check("src", df, "2024-01-02", "2024-04-01", "1d")
        assert reason is not None

    def test_non_daily_period_skips_coverage_check(self):
        """非 1d 周期不应触发覆盖率检查。"""
        reg = _registry(min_date_coverage=0.80)
        # 只有 5 行，但用 "1h" 避免覆盖率检查
        df = _make_df(n_rows=5)
        reason = reg._quality_check("src", df, "2024-01-02", "2024-03-30", "1h")
        # 只要其他检查通过（字段全、无 NaN、close 有效），1h 不检查覆盖率
        assert reason is None

    def test_custom_low_coverage_threshold(self):
        reg = _registry(min_date_coverage=0.10)   # 放宽到 10%
        df = _make_df(n_rows=10)
        # 很宽松，10 行 / 5200 个交易日 → 仍低于 10%
        # 即使如此，5 年区间覆盖率 < 10%，但这里用合理小区间
        reason = reg._quality_check("src", df, "2024-01-02", "2024-02-01", "1d")
        assert reason is None


# ---------------------------------------------------------------------------
# 5. DataSourceRegistry 构造函数参数验证
# ---------------------------------------------------------------------------


class TestRegistryInit:
    def test_default_instantiation(self):
        reg = DataSourceRegistry()
        assert reg is not None

    def test_custom_params_stored(self):
        reg = DataSourceRegistry(
            max_nan_rate=0.10,
            min_close_valid_rate=0.70,
            min_date_coverage=0.85,
        )
        assert reg._max_nan_rate == 0.10
        assert reg._min_close_valid_rate == 0.70
        assert reg._min_date_coverage == 0.85

    def test_required_fields_override(self):
        reg = DataSourceRegistry(required_fields=["close", "volume"])
        # 只需要 close + volume 即可通过
        df = _make_df(include_open=False, include_high=False, include_low=False)
        reason = reg._quality_check("src", df, "2024-01-02", "2024-03-30", "1d")
        assert reason is None


# ---------------------------------------------------------------------------
# 6. get_metrics() — 可观测性指标追踪
# ---------------------------------------------------------------------------


class TestGetMetrics:
    """验证 DataSourceRegistry.get_metrics() 准确计数 hits / misses / quality_rejects / errors。"""

    def _make_registry_with_source(self, return_df=None, raise_exc=None):
        """注册一个名为 'mock_src' 的数据源，可控制其返回结果。"""
        reg = DataSourceRegistry()
        _ret = return_df
        _exc = raise_exc

        class MockSource(DataSource):
            def __init__(self):
                super().__init__("mock_src")

            def get_data(self, symbol, start_date, end_date, period, adjust):
                if _exc:
                    raise _exc
                return _ret

        reg.register("mock_src", MockSource())
        return reg

    def test_initial_metrics_all_zero(self):
        reg = self._make_registry_with_source(return_df=_make_df())
        metrics = reg.get_metrics()
        assert "mock_src" in metrics
        m = metrics["mock_src"]
        assert m["hits"] == 0
        assert m["misses"] == 0
        assert m["errors"] == 0
        assert m["quality_rejects"] == 0

    def test_hit_increments(self):
        df = _make_df()
        reg = self._make_registry_with_source(return_df=df)
        reg.get_data("000001.SZ", "2024-01-02", "2024-03-30", "1d", "none")
        m = reg.get_metrics()["mock_src"]
        assert m["hits"] == 1
        assert m["misses"] == 0

    def test_miss_increments_for_none_return(self):
        reg = self._make_registry_with_source(return_df=None)
        reg.get_data("000001.SZ", "2024-01-02", "2024-03-30", "1d", "none")
        m = reg.get_metrics()["mock_src"]
        assert m["misses"] == 1
        assert m["hits"] == 0

    def test_quality_reject_increments(self):
        # 80% 的 close <= 0 → 触发 close valid rate 质量拒绝
        bad_df = _make_df(n_rows=50, close_invalid_pct=0.80)
        reg = self._make_registry_with_source(return_df=bad_df)
        reg.get_data("000001.SZ", "2024-01-02", "2024-03-30", "1d", "none")
        m = reg.get_metrics()["mock_src"]
        assert m["quality_rejects"] == 1
        assert m["hits"] == 0

    def test_error_increments(self):
        reg = self._make_registry_with_source(raise_exc=RuntimeError("mock error"))
        reg.get_data("000001.SZ", "2024-01-02", "2024-03-30", "1d", "none")
        m = reg.get_metrics()["mock_src"]
        assert m["errors"] == 1

    def test_latency_recorded_on_hit(self):
        reg = self._make_registry_with_source(return_df=_make_df())
        reg.get_data("000001.SZ", "2024-01-02", "2024-03-30", "1d", "none")
        m = reg.get_metrics()["mock_src"]
        assert m["last_latency_ms"] >= 0.0

    def test_metrics_snapshot_is_copy(self):
        """get_metrics() 返回的字典修改不会影响内部状态。"""
        reg = self._make_registry_with_source(return_df=_make_df())
        reg.get_data("000001.SZ", "2024-01-02", "2024-03-30", "1d", "none")
        snap1 = reg.get_metrics()
        snap1["mock_src"]["hits"] = 9999
        snap2 = reg.get_metrics()
        assert snap2["mock_src"]["hits"] == 1  # 内部计数未被污染
