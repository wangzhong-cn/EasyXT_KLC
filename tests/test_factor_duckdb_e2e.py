from __future__ import annotations

import pandas as pd
import pytest

from data_manager.factor_registry import FactorComputeEngine, FactorRegistry, make_factor_storage
from easyxt_backtest.factor_backtest import FactorBacktestEngine


def test_factor_duckdb_write_read_then_backtest_e2e():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    try:
        registry = FactorRegistry()

        @registry.register("mom_1d", category="momentum", version="1.0")
        def mom_1d(df: pd.DataFrame) -> pd.Series:
            return df["close"].pct_change().fillna(0.0)

        engine = FactorComputeEngine(registry)
        storage = make_factor_storage(con)
        symbols = ["000001.SZ", "000002.SZ", "600000.SH", "600519.SH"]
        dates = pd.date_range("2026-01-01", periods=8, freq="D")

        price_rows = []
        factor_rows = []
        for i, sym in enumerate(symbols):
            close = pd.Series([10 + i + d * 0.3 for d in range(len(dates))], index=dates)
            ohlcv = pd.DataFrame({"close": close, "open": close * 0.99}, index=dates)
            factor_series = engine.compute("mom_1d", ohlcv)
            written = storage.save(sym, "mom_1d", factor_series, version="1.0")
            assert written > 0
            loaded = storage.load(sym, "mom_1d", "2026-01-01", "2026-01-31")
            assert not loaded.empty
            for dt, val in loaded.items():
                factor_rows.append((dt, sym, float(val)))
            for dt, row in ohlcv.iterrows():
                price_rows.append((dt, sym, float(row["open"]), float(row["close"])))

        factor_index = pd.MultiIndex.from_tuples(
            [(d, s) for d, s, _ in factor_rows], names=["date", "symbol"]
        )
        factor_data = pd.Series([v for _, _, v in factor_rows], index=factor_index, name="mom_1d")

        price_index = pd.MultiIndex.from_tuples(
            [(d, s) for d, s, _, _ in price_rows], names=["date", "symbol"]
        )
        price_data = pd.DataFrame(
            {
                "open": [o for _, _, o, _ in price_rows],
                "close": [c for _, _, _, c in price_rows],
            },
            index=price_index,
        )

        bt = FactorBacktestEngine()
        result = bt.backtest_long_short_portfolio(
            factor_data=factor_data,
            price_data=price_data,
            top_quantile=0.25,
            bottom_quantile=0.25,
            transaction_cost=0.0005,
        )
        assert "total_return" in result
        assert "sharpe_ratio" in result
        assert "returns" in result
        assert isinstance(result["returns"], pd.Series)
        assert len(result["returns"]) > 0
    finally:
        con.close()
