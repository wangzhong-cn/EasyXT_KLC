"""
tests/test_factor_registry.py
单元测试：data_manager.factor_registry
"""

import math

import pandas as pd
import pytest

from data_manager.factor_registry import (
    FactorComputeEngine,
    FactorDefinition,
    FactorRegistry,
    FactorStorage,
    make_factor_storage,
)
from data_manager.factor_registry import (
    factor_compute_engine as global_engine,
)
from data_manager.factor_registry import (
    factor_registry as global_registry,
)

# ---------------------------------------------------------------------------
# 辅助：构造简单 OHLCV DataFrame
# ---------------------------------------------------------------------------


def _make_ohlcv(n: int = 30) -> pd.DataFrame:
    dates = pd.date_range("2024-01-02", periods=n, freq="B")
    closes = [100.0 + math.sin(i * 0.2) * 5 for i in range(n)]
    return pd.DataFrame(
        {
            "open":   [c - 1.0 for c in closes],
            "high":   [c + 2.0 for c in closes],
            "low":    [c - 2.0 for c in closes],
            "close":  closes,
            "volume": [10000.0 + i * 100 for i in range(n)],
        },
        index=dates,
    )


# ---------------------------------------------------------------------------
# FactorRegistry — 注册与查询
# ---------------------------------------------------------------------------


class TestFactorRegistry:
    def setup_method(self):
        self.registry = FactorRegistry()

    def test_register_decorator(self):
        @self.registry.register("test_mo", category="momentum", version="1.0")
        def test_mo(df: pd.DataFrame) -> pd.Series:
            return df["close"] / df["close"].shift(1) - 1

        assert "test_mo" in self.registry
        defn = self.registry.get("test_mo")
        assert defn is not None
        assert defn.category == "momentum"
        assert defn.version == "1.0"

    def test_register_func(self):
        def my_factor(df: pd.DataFrame) -> pd.Series:
            return df["close"]

        self.registry.register_func(
            "my_close", my_factor, category="value", version="2.0"
        )
        assert self.registry.get("my_close") is not None

    def test_list_all_returns_metadata_only(self):
        @self.registry.register("alpha_test")
        def alpha_test(df: pd.DataFrame) -> pd.Series:
            return df["close"]

        rows = self.registry.list_all()
        assert any(r["name"] == "alpha_test" for r in rows)
        # func 不应暴露在 list_all()
        for row in rows:
            assert "func" not in row

    def test_list_by_category(self):
        @self.registry.register("a", category="momentum")
        def a(df): return df["close"]

        @self.registry.register("b", category="value")
        def b(df): return df["close"]

        mom = self.registry.list_by_category("momentum")
        assert all(r["category"] == "momentum" for r in mom)

    def test_list_names_sorted(self):
        @self.registry.register("zzz")
        def zzz(df): return df["close"]

        @self.registry.register("aaa")
        def aaa(df): return df["close"]

        names = self.registry.list_names()
        assert names == sorted(names)

    def test_overwrite_logs_warning(self, caplog):
        import logging
        @self.registry.register("dup", version="1.0")
        def dup_v1(df): return df["close"]

        with caplog.at_level(logging.WARNING, logger="data_manager.factor_registry"):
            @self.registry.register("dup", version="2.0")
            def dup_v2(df): return df["close"] * 2

        assert any("dup" in r.message for r in caplog.records)

    def test_unregister(self):
        @self.registry.register("to_delete")
        def to_delete(df): return df["close"]

        assert "to_delete" in self.registry
        removed = self.registry.unregister("to_delete")
        assert removed is True
        assert "to_delete" not in self.registry

    def test_unregister_nonexistent_returns_false(self):
        assert self.registry.unregister("never_existed") is False

    def test_len(self):
        r = FactorRegistry()
        assert len(r) == 0

        @r.register("x")
        def x(df): return df["close"]

        assert len(r) == 1

    def test_contains(self):
        r = FactorRegistry()

        @r.register("exists")
        def exists(df): return df["close"]

        assert "exists" in r
        assert "not_there" not in r


# ---------------------------------------------------------------------------
# FactorDefinition — 元数据字段
# ---------------------------------------------------------------------------


class TestFactorDefinition:
    def test_to_dict_excludes_func(self):
        def dummy(df): return df["close"]

        defn = FactorDefinition(name="test", func=dummy, category="alpha")
        d = defn.to_dict()
        assert "func" not in d
        assert d["name"] == "test"
        assert d["category"] == "alpha"

    def test_default_values(self):
        def dummy(df): return df["close"]

        defn = FactorDefinition(name="t", func=dummy)
        assert defn.category == "alpha"
        assert defn.version == "1.0"
        assert defn.tags == []


# ---------------------------------------------------------------------------
# FactorComputeEngine
# ---------------------------------------------------------------------------


class TestFactorComputeEngine:
    def setup_method(self):
        self.registry = FactorRegistry()
        self.engine = FactorComputeEngine(self.registry)
        self.df = _make_ohlcv(50)

    def test_compute_returns_series(self):
        @self.registry.register("close_copy")
        def close_copy(df: pd.DataFrame) -> pd.Series:
            return df["close"].copy()

        result = self.engine.compute("close_copy", self.df)
        assert isinstance(result, pd.Series)
        assert result.name == "close_copy"
        assert len(result) == len(self.df)

    def test_compute_index_matches_df(self):
        @self.registry.register("rn_idx")
        def rn_idx(df: pd.DataFrame) -> pd.Series:
            return df["close"].rolling(3).mean()

        result = self.engine.compute("rn_idx", self.df)
        assert list(result.index) == list(self.df.index)

    def test_compute_missing_factor_raises_keyerror(self):
        with pytest.raises(KeyError, match="not_registered"):
            self.engine.compute("not_registered", self.df)

    def test_compute_propagates_exception(self):
        @self.registry.register("broken_factor")
        def broken_factor(df: pd.DataFrame) -> pd.Series:
            raise ValueError("intentional error")

        with pytest.raises(ValueError, match="intentional error"):
            self.engine.compute("broken_factor", self.df)

    def test_compute_many_returns_dataframe(self):
        @self.registry.register("f1_cm")
        def f1_cm(df): return df["close"]

        @self.registry.register("f2_cm")
        def f2_cm(df): return df["volume"]

        result = self.engine.compute_many(["f1_cm", "f2_cm"], self.df)
        assert isinstance(result, pd.DataFrame)
        assert "f1_cm" in result.columns
        assert "f2_cm" in result.columns

    def test_compute_many_skip_on_error(self):
        @self.registry.register("ok_f")
        def ok_f(df): return df["close"]

        @self.registry.register("bad_f")
        def bad_f(df):
            raise RuntimeError("bad")

        result = self.engine.compute_many(["ok_f", "bad_f"], self.df, errors="skip")
        assert "ok_f" in result.columns
        assert "bad_f" not in result.columns

    def test_compute_many_raise_on_error(self):
        @self.registry.register("boom")
        def boom(df):
            raise RuntimeError("boom!")

        with pytest.raises(RuntimeError, match="boom!"):
            self.engine.compute_many(["boom"], self.df, errors="raise")

    def test_compute_passes_kwargs(self):
        @self.registry.register("window_ma")
        def window_ma(df: pd.DataFrame, window: int = 5) -> pd.Series:
            return df["close"].rolling(window).mean()

        result10 = self.engine.compute("window_ma", self.df, window=10)
        result5 = self.engine.compute("window_ma", self.df, window=5)
        # 前 9 行 window=10 应该是 NaN，window=5 前 4 行 NaN
        assert result10.isna().sum() > result5.isna().sum()


# ---------------------------------------------------------------------------
# 全局单例可用性
# ---------------------------------------------------------------------------


class TestGlobalSingletons:
    def test_global_registry_is_factor_registry(self):
        assert isinstance(global_registry, FactorRegistry)

    def test_global_engine_is_factor_compute_engine(self):
        assert isinstance(global_engine, FactorComputeEngine)

    def test_register_on_global_registry(self):
        @global_registry.register("_test_global_factor")
        def _test_global_factor(df): return df["close"]

        assert "_test_global_factor" in global_registry
        global_registry.unregister("_test_global_factor")  # 清理


class TestFactorStorageAndE2E:
    def test_factor_storage_duckdb_e2e_write_read(self):
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        try:
            storage = make_factor_storage(con)
            idx = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
            series = pd.Series([0.1, float("nan"), 0.3], index=idx, name="f_x")
            written = storage.save("000001.SZ", "f_x", series, version="1.0")
            assert written == 2
            loaded = storage.load("000001.SZ", "f_x", "2024-01-01", "2024-01-31")
            assert isinstance(loaded, pd.Series)
            assert len(loaded) == 2
            assert abs(float(loaded.iloc[-1]) - 0.3) < 1e-9
            listing = storage.list_available("000001.SZ")
            assert not listing.empty
            assert "factor_name" in listing.columns
        finally:
            con.close()

    def test_factor_storage_skip_mode_does_not_overwrite_existing(self):
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        try:
            storage = make_factor_storage(con)
            idx = pd.to_datetime(["2024-01-02"])
            storage.save("000001.SZ", "f_y", pd.Series([1.0], index=idx), version="1.0")
            storage.save("000001.SZ", "f_y", pd.Series([2.0], index=idx), version="2.0", if_exists="skip")
            loaded = storage.load("000001.SZ", "f_y")
            assert len(loaded) == 1
            assert abs(float(loaded.iloc[0]) - 1.0) < 1e-9
        finally:
            con.close()

    def test_factor_storage_bad_date_and_query_exception_paths(self):
        class _FakeDB:
            def execute(self, sql, params=None):
                return None

            def query(self, sql, params=None):
                raise RuntimeError("query failed")

        storage = FactorStorage(_FakeDB())
        s = pd.Series([1.0], index=["bad-date"])
        assert storage.save("000001.SZ", "f_bad", s) == 0
        loaded = storage.load("000001.SZ", "f_bad")
        assert isinstance(loaded, pd.Series)
        assert loaded.empty

    def test_compute_non_series_auto_casts_to_series(self):
        registry = FactorRegistry()

        @registry.register("arr_factor")
        def arr_factor(df: pd.DataFrame):
            return [1.0] * len(df)

        engine = FactorComputeEngine(registry)
        df = _make_ohlcv(5)
        out = engine.compute("arr_factor", df)
        assert isinstance(out, pd.Series)
        assert out.name == "arr_factor"
        assert len(out) == 5
