import importlib


def test_easyxt_backtest_importable():
    mod = importlib.import_module("easyxt_backtest")
    assert hasattr(mod, "BacktestEngine")


def test_factor_backtest_export_is_optional_but_safe():
    mod = importlib.import_module("easyxt_backtest")
    assert hasattr(mod, "FactorBacktestEngine")
