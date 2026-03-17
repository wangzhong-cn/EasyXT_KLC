import pandas as pd

from easyxt_backtest.factor_backtest import FactorBacktestEngine


def _build_sample_inputs():
    dates = pd.date_range("2026-01-01", periods=6, freq="D")
    symbols = ["000001.SZ", "000002.SZ", "600000.SH", "600519.SH"]
    f_idx = pd.MultiIndex.from_product([dates, symbols], names=["date", "symbol"])
    factor_vals = []
    for i, _ in enumerate(dates):
        factor_vals.extend([0.1 + i * 0.01, 0.2 + i * 0.01, -0.1 + i * 0.01, -0.2 + i * 0.01])
    factor = pd.Series(factor_vals, index=f_idx)
    p_idx = pd.MultiIndex.from_product([dates, symbols], names=["date", "symbol"])
    close = []
    open_ = []
    for i, _ in enumerate(dates):
        close.extend([10 + i, 20 + i * 0.8, 30 - i * 0.4, 40 - i * 0.6])
        open_.extend([10 + i * 0.9, 20 + i * 0.7, 30 - i * 0.35, 40 - i * 0.5])
    price = pd.DataFrame({"open": open_, "close": close}, index=p_idx)
    return factor, price


def test_factor_backtest_long_short_has_metrics():
    factor, price = _build_sample_inputs()
    engine = FactorBacktestEngine()
    result = engine.backtest_long_short_portfolio(
        factor_data=factor,
        price_data=price,
        top_quantile=0.25,
        bottom_quantile=0.25,
        transaction_cost=0.0005,
    )
    assert "total_return" in result
    assert "sharpe_ratio" in result
    assert "returns" in result
    assert isinstance(result["returns"], pd.Series)


def test_factor_backtest_quantile_returns():
    factor, price = _build_sample_inputs()
    engine = FactorBacktestEngine()
    result = engine.backtest_quantile_portfolio(
        factor_data=factor,
        price_data=price,
        n_quantiles=4,
    )
    assert "Q1" in result
    assert "Q4" in result
    assert "returns" in result["Q1"]
