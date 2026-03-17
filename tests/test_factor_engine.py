"""
tests/test_factor_engine.py
===========================
因子引擎集成测试 —— 不依赖 QMT/外部网络，全部使用内存数据。

覆盖：
  - 15 个内置因子已注册
  - FactorRegistry CRUD
  - FactorComputeEngine.compute / compute_many
  - FactorStorage（内存 DuckDB）save / load / list_available / upsert
  - UnifiedDataInterface 因子 API（mock DuckDB 连接）
"""

from __future__ import annotations

import math
import types
from unittest.mock import MagicMock, patch

import duckdb
import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def ohlcv() -> pd.DataFrame:
    """生成 150 根测试 K 线（足够热身所有内置因子）。"""
    np.random.seed(0)
    dates = pd.date_range("2023-01-03", periods=150, freq="B")
    close = 10.0 + np.cumsum(np.random.randn(150) * 0.15)
    close = np.maximum(close, 0.5)
    df = pd.DataFrame(
        {
            "open": close * (1 + np.random.uniform(-0.005, 0.005, 150)),
            "high": close * (1 + np.abs(np.random.randn(150)) * 0.005),
            "low": close * (1 - np.abs(np.random.randn(150)) * 0.005),
            "close": close,
            "volume": np.random.randint(500_000, 10_000_000, 150).astype(float),
            "amount": close * np.random.randint(500_000, 10_000_000, 150),
        },
        index=dates,
    )
    # 确保 high >= close >= low
    df["high"] = df[["high", "close"]].max(axis=1) * 1.001
    df["low"] = df[["low", "close"]].min(axis=1) * 0.999
    return df


@pytest.fixture(scope="module")
def mem_storage():
    """内存 DuckDB 中的 FactorStorage 实例。"""
    from data_manager.factor_registry import make_factor_storage

    con = duckdb.connect(":memory:")
    storage = make_factor_storage(con)
    yield storage
    con.close()


# ---------------------------------------------------------------------------
# 1. 注册中心
# ---------------------------------------------------------------------------


class TestFactorRegistry:
    def test_builtin_factors_registered(self):
        """导入 data_manager 后，15 个内置因子应已注册。"""
        import data_manager  # noqa: F401 — 触发自动注册
        from data_manager.factor_registry import factor_registry

        names = factor_registry.list_names()
        assert len(names) == 15, f"期望 15 个因子，实际 {len(names)}: {names}"

    def test_expected_factor_names(self):
        from data_manager.factor_registry import factor_registry

        expected = {
            "momentum_20d", "momentum_5d", "momentum_60d_skip5", "roc_10d",
            "volatility_20d", "volatility_60d", "atr_14d",
            "volume_ratio_20d", "turnover_zscore_20d", "obv",
            "ma_cross_5_20", "rsi_14", "macd_diff", "bollinger_pct_b",
            "high_low_ratio_20d",
        }
        assert expected == set(factor_registry.list_names())

    def test_register_and_get(self):
        from data_manager.factor_registry import FactorRegistry

        reg = FactorRegistry()

        @reg.register("test_factor", category="test", description="测试因子")
        def _f(df: pd.DataFrame) -> pd.Series:
            return df["close"]

        defn = reg.get("test_factor")
        assert defn is not None
        assert defn.category == "test"

    def test_list_by_category(self):
        from data_manager.factor_registry import factor_registry

        momentum_factors = factor_registry.list_by_category("momentum")
        assert len(momentum_factors) == 4  # momentum_20d/5d/60d_skip5, roc_10d

    def test_get_nonexistent_returns_none(self):
        from data_manager.factor_registry import factor_registry

        assert factor_registry.get("totally_nonexistent_factor_xyz") is None


# ---------------------------------------------------------------------------
# 2. 因子计算引擎
# ---------------------------------------------------------------------------


class TestFactorComputeEngine:
    def test_compute_momentum_20d(self, ohlcv):
        from data_manager.factor_registry import factor_registry, FactorComputeEngine

        engine = FactorComputeEngine(factor_registry)
        result = engine.compute("momentum_20d", ohlcv)
        assert isinstance(result, pd.Series)
        assert result.name == "momentum_20d"
        # 最后 60 个值不应全 NaN
        assert result.tail(60).notna().sum() >= 55

    def test_compute_rsi_14(self, ohlcv):
        from data_manager.factor_registry import factor_registry, FactorComputeEngine

        engine = FactorComputeEngine(factor_registry)
        result = engine.compute("rsi_14", ohlcv)
        valid = result.dropna()
        # RSI 应在 [0, 100]
        assert valid.between(0, 100).all(), "RSI 越界"

    def test_compute_atr_14d(self, ohlcv):
        from data_manager.factor_registry import factor_registry, FactorComputeEngine

        engine = FactorComputeEngine(factor_registry)
        result = engine.compute("atr_14d", ohlcv)
        valid = result.dropna()
        assert (valid >= 0).all(), "ATR 不应为负"

    def test_compute_many(self, ohlcv):
        from data_manager.factor_registry import factor_registry, FactorComputeEngine

        engine = FactorComputeEngine(factor_registry)
        names = ["momentum_20d", "volatility_20d", "rsi_14", "obv", "macd_diff"]
        result = engine.compute_many(names, ohlcv, errors="skip")
        assert isinstance(result, pd.DataFrame)
        assert set(names).issubset(set(result.columns))
        # 末 50 行每列应无 NaN
        tail = result.tail(50)
        assert tail.notna().all().all(), f"末50行含NaN:\n{tail.isna().sum()}"

    def test_compute_unknown_factor_raises(self, ohlcv):
        from data_manager.factor_registry import factor_registry, FactorComputeEngine

        engine = FactorComputeEngine(factor_registry)
        with pytest.raises((KeyError, Exception)):
            engine.compute("no_such_factor_xyz", ohlcv)

    def test_compute_many_skip_errors(self, ohlcv):
        from data_manager.factor_registry import factor_registry, FactorComputeEngine

        engine = FactorComputeEngine(factor_registry)
        # 混入一个不存在的因子，errors='skip' 时不应抛出
        names = ["momentum_20d", "no_such_factor_xyz"]
        result = engine.compute_many(names, ohlcv, errors="skip")
        assert "momentum_20d" in result.columns


# ---------------------------------------------------------------------------
# 3. FactorStorage（内存 DuckDB）
# ---------------------------------------------------------------------------


class TestFactorStorage:
    def _make_series(self, n: int = 30) -> pd.Series:
        idx = pd.date_range("2024-01-02", periods=n, freq="B")
        return pd.Series(np.random.rand(n), index=idx, name="test_factor")

    def test_save_and_load_roundtrip(self, mem_storage):
        series = self._make_series()
        n = mem_storage.save("000001.SZ", "test_factor", series, version="1.0")
        assert n == len(series), f"期望写入 {len(series)} 行，实际 {n}"

        loaded = mem_storage.load("000001.SZ", "test_factor")
        assert len(loaded) == len(series)
        assert abs(loaded.iloc[0] - series.iloc[0]) < 1e-10

    def test_upsert_replaces_existing(self, mem_storage):
        s1 = self._make_series(20)
        mem_storage.save("600000.SH", "test_factor", s1, version="1.0", if_exists="replace")

        # 新值（全部乘以 2）
        s2 = s1 * 2
        mem_storage.save("600000.SH", "test_factor", s2, version="1.1", if_exists="replace")

        loaded = mem_storage.load("600000.SH", "test_factor")
        assert abs(loaded.iloc[0] - s2.iloc[0]) < 1e-10, "Upsert 未替换旧值"

    def test_load_date_range(self, mem_storage):
        series = self._make_series(40)
        mem_storage.save("300001.SZ", "test_factor", series, version="1.0")

        mid = series.index[10]
        end = series.index[29]
        loaded = mem_storage.load(
            "300001.SZ", "test_factor",
            start_date=mid.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )
        assert len(loaded) == 20

    def test_list_available(self, mem_storage):
        series = self._make_series(10)
        mem_storage.save("000002.SZ", "factor_a", series, version="1.0")
        mem_storage.save("000002.SZ", "factor_b", series, version="2.0")

        cov = mem_storage.list_available("000002.SZ")
        assert isinstance(cov, pd.DataFrame)
        assert "factor_name" in cov.columns
        assert set(cov["factor_name"]).issuperset({"factor_a", "factor_b"})

    def test_list_available_all(self, mem_storage):
        cov = mem_storage.list_available()  # 无 symbol 过滤
        assert isinstance(cov, pd.DataFrame)
        assert len(cov) >= 1

    def test_load_empty_returns_series(self, mem_storage):
        result = mem_storage.load("NONEXISTENT.SZ", "nonexistent_factor")
        assert isinstance(result, pd.Series)
        assert result.empty


# ---------------------------------------------------------------------------
# 4. UnifiedDataInterface 因子 API（mock DuckDB 连接）
# ---------------------------------------------------------------------------


class TestUnifiedDataInterfaceFactorAPI:
    """使用 mock `con` 测试 UnifiedDataInterface 的因子方法，不需要 QMT。"""

    @pytest.fixture()
    def udi(self):
        """构造一个带有内存 DuckDB 连接的 UnifiedDataInterface 实例（最小化 mock）。"""
        from data_manager.unified_data_interface import UnifiedDataInterface

        inst = UnifiedDataInterface.__new__(UnifiedDataInterface)
        # 最小化初始化：设置日志 + DuckDB 内存连接
        import logging
        inst._logger = logging.getLogger("test_udi")
        inst._factor_storage = None
        inst.con = duckdb.connect(":memory:")
        # 建 factor_values 表（正常由 _ensure_tables_exist 完成）
        inst.con.execute("""
            CREATE TABLE IF NOT EXISTS factor_values (
                symbol      VARCHAR  NOT NULL,
                factor_name VARCHAR  NOT NULL,
                date        DATE     NOT NULL,
                value       DOUBLE,
                version     VARCHAR  DEFAULT '1.0',
                saved_at    TIMESTAMP DEFAULT now(),
                PRIMARY KEY (symbol, factor_name, date)
            )
        """)
        yield inst
        inst.con.close()

    def test_list_factors_returns_all(self, udi):
        factors = udi.list_factors()
        assert len(factors) == 15
        names = [f["name"] if isinstance(f, dict) else f.name for f in factors]
        assert "momentum_20d" in names

    def test_ensure_factor_storage_init(self, udi):
        storage = udi._ensure_factor_storage()
        assert storage is not None
        # 第二次调用应返回同一对象
        assert udi._ensure_factor_storage() is storage

    def test_save_and_load_factor(self, udi):
        idx = pd.date_range("2024-01-02", periods=20, freq="B")
        series = pd.Series(np.random.rand(20), index=idx, name="momentum_20d")

        n = udi.save_factor("000001.SZ", "momentum_20d", series, version="1.0")
        assert n == 20

        loaded = udi.load_factor("000001.SZ", "momentum_20d")
        assert len(loaded) == 20
        assert abs(series.iloc[0] - loaded.iloc[0]) < 1e-10

    def test_load_factor_with_date_range(self, udi):
        idx = pd.date_range("2024-03-01", periods=30, freq="B")
        series = pd.Series(np.random.rand(30), index=idx, name="volatility_20d")
        udi.save_factor("600000.SH", "volatility_20d", series, version="1.0")

        loaded = udi.load_factor(
            "600000.SH", "volatility_20d",
            start_date="2024-03-15",
            end_date="2024-04-01",
        )
        assert len(loaded) > 0
        assert loaded.index.min() >= pd.Timestamp("2024-03-15")

    def test_list_stored_factors(self, udi):
        idx = pd.date_range("2024-01-02", periods=5, freq="B")
        series = pd.Series([1.0] * 5, index=idx, name="x")
        udi.save_factor("000999.SZ", "x", series, version="1.0")

        cov = udi.list_stored_factors("000999.SZ")
        assert isinstance(cov, pd.DataFrame)
        assert "factor_name" in cov.columns

    def test_compute_and_save_factor(self, udi):
        """compute_and_save_factor 应 mock get_stock_data 并写入 DuckDB。"""
        dates = pd.date_range("2023-01-03", periods=100, freq="B")
        np.random.seed(42)
        close = 10 + np.cumsum(np.random.randn(100) * 0.1)
        mock_df = pd.DataFrame({
            "date": dates,
            "open":   close * 0.99,
            "high":   close * 1.01,
            "low":    close * 0.98,
            "close":  close,
            "volume": np.random.randint(1e6, 1e7, 100).astype(float),
            "amount": close * 1e6,
        })

        with patch.object(udi, "get_stock_data", return_value=mock_df):
            n = udi.compute_and_save_factor(
                "momentum_20d", "000001.SZ", "2023-01-03", "2023-06-30"
            )
        assert n > 0

        loaded = udi.load_factor("000001.SZ", "momentum_20d")
        assert not loaded.empty

    def test_compute_factor_empty_data_returns_empty_series(self, udi):
        with patch.object(udi, "get_stock_data", return_value=pd.DataFrame()):
            result = udi.compute_factor("momentum_20d", "000001.SZ", "2024-01-01", "2024-03-31")
        assert isinstance(result, pd.Series)
        assert result.empty

    def test_compute_factor_unknown_name_raises(self, udi):
        mock_df = pd.DataFrame({"date": [], "close": [], "open": [], "high": [], "low": [], "volume": [], "amount": []})
        with pytest.raises(KeyError):
            udi.compute_factor("no_such_factor_xyz", "000001.SZ", "2024-01-01", "2024-03-31")
