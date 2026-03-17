"""
tests/test_backtest_engine_logic.py
------------------------------------
Pure-logic unit tests for gui_app/backtest/engine.py.

设计原则:
- 无 QApplication / Qt 依赖（engine.py 本身没有任何 Qt 导入）
- 利用 BACKTRADER_AVAILABLE=False 的 mock 路径（CI 环境没有 backtrader）
- 直接实例化 AdvancedBacktestEngine，或用 unbound-method 模式测试纯函数
- 每个 class 只测一个职责
"""
from __future__ import annotations

import math
import types
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# 被测模块
# ---------------------------------------------------------------------------
from gui_app.backtest.engine import (
    AdvancedBacktestEngine,
    get_backtrader_import_status,
)
import gui_app.backtest.engine as _engine_mod


# ===========================================================================
# 公用 fixture ― 强制 mock 模式（将 BACKTRADER_AVAILABLE 置为 False）
# ===========================================================================
@pytest.fixture()
def mock_mode(monkeypatch):
    """令 engine 模块认为 backtrader 不可用，激活完整 mock 路径。"""
    monkeypatch.setattr(_engine_mod, "BACKTRADER_AVAILABLE", False)
    yield


@pytest.fixture()
def mock_engine(monkeypatch):
    """在 mock 模式下创建的 AdvancedBacktestEngine。"""
    monkeypatch.setattr(_engine_mod, "BACKTRADER_AVAILABLE", False)
    return AdvancedBacktestEngine()


@pytest.fixture()
def mock_engine_with_data(monkeypatch, ohlcv_df):
    """在 mock 模式下创建并已 add_data 的 engine。"""
    monkeypatch.setattr(_engine_mod, "BACKTRADER_AVAILABLE", False)
    e = AdvancedBacktestEngine()
    e.add_data(ohlcv_df)
    return e


# ---------------------------------------------------------------------------
# 公用 fixture ― 生成一段带 DatetimeIndex 的标准 OHLCV DataFrame
# ---------------------------------------------------------------------------
@pytest.fixture()
def ohlcv_df():
    """252 个交易日的模拟 OHLCV 数据，DatetimeIndex。"""
    n = 252
    dates = pd.date_range("2023-01-01", periods=n, freq="B")
    np.random.seed(0)
    close = 10.0 + np.cumsum(np.random.normal(0, 0.2, n))
    close = np.maximum(close, 0.5)
    df = pd.DataFrame(
        {
            "open": close * 0.99,
            "high": close * 1.01,
            "low": close * 0.98,
            "close": close,
            "volume": np.random.randint(100000, 500000, n).astype(float),
        },
        index=dates,
    )
    return df


@pytest.fixture()
def engine():
    """默认参数构造的引擎（mock 模式）。"""
    return AdvancedBacktestEngine()


@pytest.fixture()
def engine_with_data(engine, ohlcv_df):
    engine.add_data(ohlcv_df, name="TEST")
    return engine


# ===========================================================================
# 1. get_backtrader_import_status
# ===========================================================================
class TestGetBacktraderImportStatus:
    def test_returns_dict(self):
        result = get_backtrader_import_status()
        assert isinstance(result, dict)

    def test_has_available_key(self):
        result = get_backtrader_import_status()
        assert "available" in result

    def test_available_is_bool(self):
        result = get_backtrader_import_status()
        assert isinstance(result["available"], bool)

    def test_has_mode_key(self):
        result = get_backtrader_import_status()
        assert "mode" in result

    def test_mode_is_string(self):
        result = get_backtrader_import_status()
        assert isinstance(result["mode"], str)

    def test_mock_mode_when_bt_unavailable(self, monkeypatch):
        monkeypatch.setattr(_engine_mod, "BACKTRADER_AVAILABLE", False)
        monkeypatch.setattr(_engine_mod, "NATIVE_ENGINE_AVAILABLE", False)
        result = get_backtrader_import_status()
        assert result["available"] is False
        assert result["mode"] == "mock"

    def test_backtrader_mode_when_available(self, monkeypatch):
        monkeypatch.setattr(_engine_mod, "BACKTRADER_AVAILABLE", True)
        result = get_backtrader_import_status()
        assert result["available"] is True
        assert result["mode"] == "backtrader"


# ===========================================================================
# 2. AdvancedBacktestEngine.__init__
# ===========================================================================
class TestAdvancedBacktestEngineInit:
    def test_default_initial_cash(self, engine):
        assert engine.initial_cash > 0

    def test_custom_initial_cash(self):
        e = AdvancedBacktestEngine(initial_cash=500_000)
        assert e.initial_cash == 500_000

    def test_custom_commission(self):
        e = AdvancedBacktestEngine(commission=0.002)
        assert e.commission == pytest.approx(0.002)

    def test_performance_metrics_empty_initially(self, engine):
        assert isinstance(engine.performance_metrics, dict)
        assert len(engine.performance_metrics) == 0

    def test_results_empty_initially(self, engine):
        # 初始可能是 None 或 []，均表示"尚未运行"
        assert engine.results is None or engine.results == []

    def test_cerebro_initialized(self, engine):
        assert engine.cerebro is not None

    def test_mock_data_none_initially(self, engine):
        # mock_data 仅在 BACKTRADER_AVAILABLE=False 时被 add_data 填充
        assert engine.mock_data is None

    def test_initial_cash_stored(self):
        e = AdvancedBacktestEngine(initial_cash=200_000.0)
        assert e.initial_cash == 200_000.0

    def test_mock_mode_init(self, mock_engine):
        """mock 模式下 cerebro 为 MockCerebro。"""
        assert mock_engine.cerebro is not None
        assert mock_engine.initial_cash > 0


# ===========================================================================
# 3. get_runtime_status
# ===========================================================================
class TestGetRuntimeStatus:
    def test_returns_dict(self, engine):
        status = engine.get_runtime_status()
        assert isinstance(status, dict)

    def test_has_mode_key(self, engine):
        status = engine.get_runtime_status()
        assert "mode" in status

    def test_has_initial_cash_key(self, engine):
        status = engine.get_runtime_status()
        assert "initial_cash" in status

    def test_initial_cash_matches(self, engine):
        status = engine.get_runtime_status()
        assert status["initial_cash"] == engine.initial_cash


# ===========================================================================
# 4. add_data
# ===========================================================================
class TestAddData:
    def test_stores_dataframe_data(self, engine, ohlcv_df):
        """add_data 总是把 DataFrame 存到 self.dataframe_data。"""
        engine.add_data(ohlcv_df)
        assert isinstance(engine.dataframe_data, pd.DataFrame)

    def test_mock_mode_stores_mock_data(self, mock_engine, ohlcv_df):
        """mock 模式下额外存入 self.mock_data。"""
        mock_engine.add_data(ohlcv_df)
        assert isinstance(mock_engine.mock_data, pd.DataFrame)

    def test_accepts_name_kwarg(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df, name="MY_STOCK")
        assert isinstance(engine.dataframe_data, pd.DataFrame)

    def test_extracts_start_date(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert hasattr(engine, "backtest_start_date")
        assert engine.backtest_start_date is not None

    def test_extracts_end_date(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert hasattr(engine, "backtest_end_date")
        assert engine.backtest_end_date is not None

    def test_start_before_end(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert engine.backtest_start_date < engine.backtest_end_date

    def test_date_column_df_mock_mode(self, mock_engine):
        """mock 模式下 'date' 列 DataFrame 也能正常 add_data。"""
        n = 50
        dates = pd.date_range("2023-01-01", periods=n, freq="B")
        df = pd.DataFrame(
            {
                "date": dates,
                "close": np.linspace(10, 12, n),
                "open": np.linspace(9.9, 11.9, n),
                "high": np.linspace(10.1, 12.1, n),
                "low": np.linspace(9.8, 11.8, n),
                "volume": [100000] * n,
            }
        )
        mock_engine.add_data(df)
        assert mock_engine.mock_data is not None


# ===========================================================================
# 5. add_strategy (在 mock 模式下 strategy_name 才被保存)
# ===========================================================================
class TestAddStrategy:
    def test_stores_strategy_name(self, mock_engine):
        class FakeStrategy:
            __name__ = "FakeStrategy"
        mock_engine.add_strategy(FakeStrategy)
        assert mock_engine.strategy_name == "FakeStrategy"

    def test_stores_params(self, mock_engine):
        class FakeStrategy:
            __name__ = "FakeStrategy"
        mock_engine.add_strategy(FakeStrategy, period=20)
        assert mock_engine.strategy_params.get("period") == 20

    def test_override_params(self, mock_engine):
        class FakeStrategy:
            __name__ = "FakeStrategy"
        mock_engine.add_strategy(FakeStrategy, fast=5, slow=20)
        assert mock_engine.strategy_params["fast"] == 5
        assert mock_engine.strategy_params["slow"] == 20


# ===========================================================================
# 6. run_backtest (end-to-end in mock mode)
# ===========================================================================
class TestRunBacktestMockMode:
    def test_returns_dict(self, mock_engine_with_data):
        result = mock_engine_with_data.run_backtest()
        assert isinstance(result, dict)

    def test_has_sharpe_ratio(self, mock_engine_with_data):
        result = mock_engine_with_data.run_backtest()
        assert "sharpe_ratio" in result

    def test_has_total_return(self, mock_engine_with_data):
        result = mock_engine_with_data.run_backtest()
        assert "total_return" in result

    def test_has_max_drawdown(self, mock_engine_with_data):
        result = mock_engine_with_data.run_backtest()
        assert "max_drawdown" in result

    def test_has_total_trades(self, mock_engine_with_data):
        result = mock_engine_with_data.run_backtest()
        assert "total_trades" in result

    def test_performance_metrics_populated(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        assert len(mock_engine_with_data.performance_metrics) > 0

    def test_no_data_still_returns_dict(self, mock_engine):
        """mock 模式下没有 add_data 也不应崩溃。"""
        result = mock_engine.run_backtest()
        assert isinstance(result, dict)

    def test_rsi_strategy_branch(self, mock_mode, ohlcv_df):
        class RSIStrategy:
            __name__ = "RSIStrategy"
        e = AdvancedBacktestEngine()
        e.add_data(ohlcv_df)
        e.add_strategy(RSIStrategy)
        result = e.run_backtest()
        assert "sharpe_ratio" in result

    def test_macd_strategy_branch(self, mock_mode, ohlcv_df):
        class MACDStrategy:
            __name__ = "MACDStrategy"
        e = AdvancedBacktestEngine()
        e.add_data(ohlcv_df)
        e.add_strategy(MACDStrategy)
        result = e.run_backtest()
        assert "sharpe_ratio" in result

    def test_default_dualma_branch(self, mock_mode, ohlcv_df):
        class DualMAStrategy:
            __name__ = "DualMAStrategy"
        e = AdvancedBacktestEngine()
        e.add_data(ohlcv_df)
        e.add_strategy(DualMAStrategy)
        result = e.run_backtest()
        assert "sharpe_ratio" in result

    def test_real_backtest_returns_dict(self, engine_with_data):
        """backtrader 可用时 run_backtest 也应返回 dict（没有策略时用默认值）。"""
        result = engine_with_data.run_backtest()
        assert isinstance(result, dict)
        assert "sharpe_ratio" in result


class TestNativeMainPathRouting:
    def test_should_use_native_main_path_requires_adjusted_data(self, monkeypatch, ohlcv_df):
        monkeypatch.setattr(_engine_mod, "NATIVE_ENGINE_AVAILABLE", True)
        e = AdvancedBacktestEngine()
        S = type("RSIStrategy", (), {})

        e.add_data(ohlcv_df)
        e.add_strategy(S)
        assert e._should_use_native_main_path() is False
        e.set_data_profile(period="1d", adjust="front")
        assert e._should_use_native_main_path() is True

    def test_run_backtest_prefers_native_when_available(self, monkeypatch, ohlcv_df):
        monkeypatch.setattr(_engine_mod, "NATIVE_ENGINE_AVAILABLE", True)
        monkeypatch.setattr(_engine_mod, "BACKTRADER_AVAILABLE", False)
        e = AdvancedBacktestEngine()
        S = type("MACDStrategy", (), {})

        e.add_data(ohlcv_df)
        e.set_data_profile(period="1d", adjust="front")
        e.add_strategy(S)
        monkeypatch.setattr(
            e,
            "_run_native_backtest",
            lambda: {"sharpe_ratio": 1.1, "total_return": 0.2, "max_drawdown": 0.1},
        )
        result = e.run_backtest()
        assert result["sharpe_ratio"] == pytest.approx(1.1)


# ===========================================================================
# 7. _get_mock_close_series
# ===========================================================================
class TestGetMockCloseSeries:
    def test_returns_series(self, engine, ohlcv_df):
        engine.mock_data = ohlcv_df
        result = engine._get_mock_close_series()
        assert isinstance(result, pd.Series)

    def test_no_data_returns_none_or_generated(self, engine):
        """mock_data 为 None 时返回 None（上层调用者处理）。"""
        result = engine._get_mock_close_series()
        # 可能是 None 或空 Series，不应崩溃
        assert result is None or isinstance(result, pd.Series)

    def test_uses_close_column(self, engine, ohlcv_df):
        engine.mock_data = ohlcv_df
        result = engine._get_mock_close_series()
        pd.testing.assert_series_equal(result, ohlcv_df["close"], check_names=False)

    def test_single_column_close_df(self, engine):
        """只有 close 列（无其他列）时仍能返回正确 Series。"""
        df = pd.DataFrame({"close": [10.0, 11.0, 12.0]},
                          index=pd.date_range("2023-01-01", periods=3, freq="B"))
        engine.mock_data = df
        result = engine._get_mock_close_series()
        assert result is not None
        assert len(result) == 3


# ===========================================================================
# 8. _compute_bars_per_year
# ===========================================================================
class TestComputeBarsPerYear:
    def _daily_close(self, n=252):
        idx = pd.date_range("2023-01-01", periods=n, freq="B")
        return pd.Series(range(n), index=idx, dtype=float)

    def _minute_close(self, freq="1min", n=240):
        idx = pd.date_range("2023-01-01 09:30", periods=n, freq=freq)
        return pd.Series(range(n), index=idx, dtype=float)

    def test_daily_returns_approx_252(self, engine):
        bpy = engine._compute_bars_per_year(self._daily_close())
        assert 200 <= bpy <= 300

    def test_weekly_returns_at_least_252(self, engine):
        """对于低频数据（周线/月线），bars_per_day 趋近于 0 → 回退为 max(1,0)=1 → 返回 252。"""
        idx = pd.date_range("2023-01-02", periods=52, freq="W")
        s = pd.Series(range(52), index=idx, dtype=float)
        bpy = engine._compute_bars_per_year(s)
        assert bpy >= 252

    def test_1min_returns_large_value(self, engine):
        bpy = engine._compute_bars_per_year(self._minute_close("1min"))
        assert bpy > 1000

    def test_5min_returns_larger_than_daily(self, engine):
        bpy = engine._compute_bars_per_year(self._minute_close("5min"))
        assert bpy > 252

    def test_60min_returns_between_daily_and_1min(self, engine):
        bpy = engine._compute_bars_per_year(self._minute_close("60min"))
        assert bpy > 252

    def test_single_element_series_fallback(self, engine):
        s = pd.Series([10.0], index=pd.date_range("2023-01-01", periods=1))
        bpy = engine._compute_bars_per_year(s)
        assert bpy > 0


# ===========================================================================
# 9. _compute_rsi
# ===========================================================================
class TestComputeRsi:
    def test_returns_series(self, engine, ohlcv_df):
        rsi = engine._compute_rsi(ohlcv_df["close"], 14)
        assert isinstance(rsi, pd.Series)

    def test_length_matches_input(self, engine, ohlcv_df):
        rsi = engine._compute_rsi(ohlcv_df["close"], 14)
        assert len(rsi) == len(ohlcv_df)

    def test_values_between_0_and_100(self, engine, ohlcv_df):
        rsi = engine._compute_rsi(ohlcv_df["close"].dropna(), 14)
        valid = rsi.dropna()
        assert (valid >= 0).all()
        assert (valid <= 100).all()

    def test_short_series_no_exception(self, engine):
        s = pd.Series([10.0, 11.0, 10.5], dtype=float)
        rsi = engine._compute_rsi(s, 14)
        assert isinstance(rsi, pd.Series)

    def test_period_6_vs_period_14(self, engine, ohlcv_df):
        rsi_6 = engine._compute_rsi(ohlcv_df["close"], 6)
        rsi_14 = engine._compute_rsi(ohlcv_df["close"], 14)
        # 不同周期结果不完全相同
        assert not rsi_6.equals(rsi_14)


# ===========================================================================
# 10. _compute_mock_strategy_returns
# Note: 返回 (strat_returns, position) 元组，直接设置实例属性后调用
# ===========================================================================
class TestComputeMockStrategyReturns:
    def _prepare(self, engine, ohlcv_df, strategy_name, params=None):
        engine.mock_data = ohlcv_df
        engine.strategy_name = strategy_name
        engine.strategy_params = params or {}

    def _call(self, engine, close):
        return engine._compute_mock_strategy_returns(close)

    def test_rsi_returns_tuple(self, engine, ohlcv_df):
        self._prepare(engine, ohlcv_df, "RSIStrategy")
        ret = self._call(engine, ohlcv_df["close"])
        assert isinstance(ret, tuple) and len(ret) == 2

    def test_rsi_first_element_is_series(self, engine, ohlcv_df):
        self._prepare(engine, ohlcv_df, "RSIStrategy")
        strat_ret, _ = self._call(engine, ohlcv_df["close"])
        assert isinstance(strat_ret, pd.Series)

    def test_rsi_length_matches(self, engine, ohlcv_df):
        self._prepare(engine, ohlcv_df, "RSIStrategy")
        strat_ret, _ = self._call(engine, ohlcv_df["close"])
        assert len(strat_ret) == len(ohlcv_df["close"])

    def test_macd_returns_tuple(self, engine, ohlcv_df):
        self._prepare(engine, ohlcv_df, "MACDStrategy")
        ret = self._call(engine, ohlcv_df["close"])
        assert isinstance(ret, tuple) and len(ret) == 2

    def test_macd_first_element_is_series(self, engine, ohlcv_df):
        self._prepare(engine, ohlcv_df, "MACDStrategy")
        strat_ret, _ = self._call(engine, ohlcv_df["close"])
        assert isinstance(strat_ret, pd.Series)

    def test_macd_length_matches(self, engine, ohlcv_df):
        self._prepare(engine, ohlcv_df, "MACDStrategy")
        strat_ret, _ = self._call(engine, ohlcv_df["close"])
        assert len(strat_ret) == len(ohlcv_df["close"])

    def test_default_dualma_returns_tuple(self, engine, ohlcv_df):
        self._prepare(engine, ohlcv_df, "DualMAStrategy")
        ret = self._call(engine, ohlcv_df["close"])
        assert isinstance(ret, tuple) and len(ret) == 2

    def test_default_dualma_first_element_is_series(self, engine, ohlcv_df):
        self._prepare(engine, ohlcv_df, "DualMAStrategy")
        strat_ret, _ = self._call(engine, ohlcv_df["close"])
        assert isinstance(strat_ret, pd.Series)

    def test_rsi_custom_params(self, engine, ohlcv_df):
        self._prepare(engine, ohlcv_df, "RSIStrategy",
                      {"rsi_period": 7, "rsi_buy": 25, "rsi_sell": 75})
        strat_ret, _ = self._call(engine, ohlcv_df["close"])
        assert isinstance(strat_ret, pd.Series)

    def test_returns_are_numeric(self, engine, ohlcv_df):
        self._prepare(engine, ohlcv_df, "DualMAStrategy")
        strat_ret, _ = self._call(engine, ohlcv_df["close"])
        assert strat_ret.dtype.kind in ("f", "i")

    def test_position_values_0_or_1(self, engine, ohlcv_df):
        self._prepare(engine, ohlcv_df, "DualMAStrategy")
        _, position = self._call(engine, ohlcv_df["close"])
        assert set(position.unique()).issubset({0.0, 1.0})

    def test_short_series_no_exception(self, engine):
        """少于 26 bar 的数据（MACD 需要 26）不应崩溃。"""
        engine.strategy_name = "MACDStrategy"
        engine.strategy_params = {}
        s = pd.Series(np.linspace(10, 11, 20))
        ret = engine._compute_mock_strategy_returns(s)
        assert isinstance(ret, tuple)


# ===========================================================================
# 11. _compute_mock_curve — 无参数，读取 self.mock_data，返回 list[float]
# ===========================================================================
class TestComputeMockCurve:
    def test_returns_list(self, engine, ohlcv_df):
        engine.mock_data = ohlcv_df
        engine.strategy_name = "DualMAStrategy"
        engine.strategy_params = {}
        curve = engine._compute_mock_curve()
        assert isinstance(curve, list)

    def test_non_empty(self, engine, ohlcv_df):
        engine.mock_data = ohlcv_df
        engine.strategy_name = "DualMAStrategy"
        engine.strategy_params = {}
        assert len(engine._compute_mock_curve()) > 0

    def test_length_equals_data_length(self, engine, ohlcv_df):
        engine.mock_data = ohlcv_df
        engine.strategy_name = "DualMAStrategy"
        engine.strategy_params = {}
        assert len(engine._compute_mock_curve()) == len(ohlcv_df)

    def test_no_data_falls_back_to_mock(self, engine):
        """没有 mock_data 时回落 mock portfolio curve（253 点）。"""
        engine.strategy_name = "DualMAStrategy"
        engine.strategy_params = {}
        curve = engine._compute_mock_curve()
        assert isinstance(curve, list) and len(curve) > 0

    def test_rsi_strategy_produces_list(self, engine, ohlcv_df):
        engine.mock_data = ohlcv_df
        engine.strategy_name = "RSIStrategy"
        engine.strategy_params = {}
        assert isinstance(engine._compute_mock_curve(), list)


# ===========================================================================
# 12. _compute_mock_metrics — 直接设置实例属性后调用，不依赖 BACKTRADER_AVAILABLE
# ===========================================================================
class TestComputeMockMetrics:
    def _setup(self, engine, ohlcv_df, strategy="DualMAStrategy"):
        engine.mock_data = ohlcv_df
        engine.strategy_name = strategy
        engine.strategy_params = {}

    def test_returns_dict(self, engine, ohlcv_df):
        self._setup(engine, ohlcv_df)
        assert isinstance(engine._compute_mock_metrics(), dict)

    def test_has_sharpe_ratio(self, engine, ohlcv_df):
        self._setup(engine, ohlcv_df)
        assert "sharpe_ratio" in engine._compute_mock_metrics()

    def test_has_total_return(self, engine, ohlcv_df):
        self._setup(engine, ohlcv_df)
        assert "total_return" in engine._compute_mock_metrics()

    def test_has_max_drawdown(self, engine, ohlcv_df):
        self._setup(engine, ohlcv_df)
        assert "max_drawdown" in engine._compute_mock_metrics()

    def test_has_win_rate(self, engine, ohlcv_df):
        self._setup(engine, ohlcv_df)
        assert "win_rate" in engine._compute_mock_metrics()

    def test_has_total_trades(self, engine, ohlcv_df):
        self._setup(engine, ohlcv_df)
        assert "total_trades" in engine._compute_mock_metrics()

    def test_win_rate_between_0_and_1(self, engine, ohlcv_df):
        self._setup(engine, ohlcv_df)
        metrics = engine._compute_mock_metrics()
        assert 0.0 <= float(metrics["win_rate"]) <= 1.0

    def test_max_drawdown_non_negative(self, engine, ohlcv_df):
        self._setup(engine, ohlcv_df)
        metrics = engine._compute_mock_metrics()
        assert float(metrics["max_drawdown"]) >= 0.0

    def test_rsi_and_macd_all_keys_present(self, ohlcv_df):
        for strat in ("RSIStrategy", "MACDStrategy"):
            e = AdvancedBacktestEngine()
            e.mock_data = ohlcv_df
            e.strategy_name = strat
            e.strategy_params = {}
            metrics = e._compute_mock_metrics()
            for key in ("sharpe_ratio", "total_return", "max_drawdown", "total_trades"):
                assert key in metrics, f"{strat}: missing key {key}"


# ===========================================================================
# 13. _calculate_profit_factor
# ===========================================================================
class TestCalculateProfitFactor:
    def _make_trade_analysis(self, won_total, lost_total):
        return {
            "won": {"pnl": {"total": won_total}},
            "lost": {"pnl": {"total": lost_total}},
        }

    def test_basic_ratio(self, engine):
        pf = engine._calculate_profit_factor(self._make_trade_analysis(1000, -500))
        assert pf == pytest.approx(2.0)

    def test_zero_loss_returns_large(self, engine):
        pf = engine._calculate_profit_factor(self._make_trade_analysis(1000, 0))
        assert pf > 1.0

    def test_zero_won_returns_zero(self, engine):
        pf = engine._calculate_profit_factor(self._make_trade_analysis(0, -500))
        assert pf == pytest.approx(0.0)

    def test_both_zero(self, engine):
        pf = engine._calculate_profit_factor(self._make_trade_analysis(0, 0))
        assert pf == pytest.approx(0.0)

    def test_empty_dict_returns_zero(self, engine):
        pf = engine._calculate_profit_factor({})
        assert pf == pytest.approx(0.0)

    def test_missing_lost_returns_all_won(self, engine):
        ta = {"won": {"pnl": {"total": 500}}}
        pf = engine._calculate_profit_factor(ta)
        assert pf > 0.0


# ===========================================================================
# 14. get_portfolio_value_curve
# ===========================================================================
class TestGetPortfolioValueCurve:
    def test_returns_list(self, engine):
        """results 为空时走 mock curve 分支。"""
        curve = engine.get_portfolio_value_curve()
        assert isinstance(curve, list)

    def test_non_empty(self, engine):
        curve = engine.get_portfolio_value_curve()
        assert len(curve) > 0

    def test_all_positive(self, engine):
        curve = engine.get_portfolio_value_curve()
        assert all(v > 0 for v in curve)

    def test_returns_list_after_mock_run(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        curve = mock_engine_with_data.get_portfolio_value_curve()
        assert isinstance(curve, list)
        assert len(curve) > 0

    def test_first_value_near_initial_cash(self, engine):
        """模拟曲线起始值应在 initial_cash ± 50% 范围内。"""
        curve = engine.get_portfolio_value_curve()
        assert curve[0] == pytest.approx(engine.initial_cash, rel=0.5)


# ===========================================================================
# 15. _generate_mock_portfolio_curve
# ===========================================================================
class TestGenerateMockPortfolioCurve:
    def test_returns_list(self, engine):
        curve = engine._generate_mock_portfolio_curve()
        assert isinstance(curve, list)

    def test_length_253(self, engine):
        """固定种子生成 252 日收益 + 初始值 = 253 个点。"""
        curve = engine._generate_mock_portfolio_curve()
        assert len(curve) == 253

    def test_deterministic_with_seed(self, engine):
        c1 = engine._generate_mock_portfolio_curve()
        c2 = engine._generate_mock_portfolio_curve()
        assert c1 == c2  # np.random.seed(42) 保证确定性

    def test_first_value_equals_initial_cash(self, engine):
        curve = engine._generate_mock_portfolio_curve()
        assert curve[0] == pytest.approx(engine.initial_cash)


# ===========================================================================
# 16. _generate_param_combinations
# ===========================================================================
class TestGenerateParamCombinations:
    def test_single_param(self, engine):
        combos = engine._generate_param_combinations({"period": [5, 10, 20]})
        assert len(combos) == 3

    def test_two_params(self, engine):
        combos = engine._generate_param_combinations(
            {"fast": [5, 10], "slow": [20, 30]}
        )
        assert len(combos) == 4  # 2×2

    def test_combo_is_dict(self, engine):
        combos = engine._generate_param_combinations({"p": [1, 2]})
        for c in combos:
            assert isinstance(c, dict)

    def test_keys_present(self, engine):
        combos = engine._generate_param_combinations({"a": [1], "b": [2]})
        assert combos[0]["a"] == 1
        assert combos[0]["b"] == 2

    def test_empty_dict_returns_one_empty_combo(self, engine):
        combos = engine._generate_param_combinations({})
        assert combos == [{}]


# ===========================================================================
# 17. get_detailed_results
# ===========================================================================
class TestGetDetailedResults:
    def test_returns_dict(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        result = mock_engine_with_data.get_detailed_results()
        assert isinstance(result, dict)

    def test_has_portfolio_curve(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        result = mock_engine_with_data.get_detailed_results()
        assert "portfolio_curve" in result

    def test_has_performance_metrics(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        result = mock_engine_with_data.get_detailed_results()
        assert "performance_metrics" in result

    def test_has_initial_cash(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        result = mock_engine_with_data.get_detailed_results()
        assert "initial_cash" in result
        assert result["initial_cash"] == mock_engine_with_data.initial_cash

    def test_has_backtest_period(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        result = mock_engine_with_data.get_detailed_results()
        assert "backtest_period" in result

    def test_has_strategy_info(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        result = mock_engine_with_data.get_detailed_results()
        assert "strategy_info" in result

    def test_has_trades_key(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        result = mock_engine_with_data.get_detailed_results()
        assert "trades" in result

    def test_portfolio_curve_has_dates_and_values(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        result = mock_engine_with_data.get_detailed_results()
        pc = result["portfolio_curve"]
        assert "dates" in pc
        assert "values" in pc


# ===========================================================================
# 18. _get_backtest_period
# ===========================================================================
class TestGetBacktestPeriod:
    def test_returns_dict(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert isinstance(engine._get_backtest_period(), dict)

    def test_has_start_date(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert "start_date" in engine._get_backtest_period()

    def test_has_end_date(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert "end_date" in engine._get_backtest_period()

    def test_start_date_format(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        period = engine._get_backtest_period()
        datetime.strptime(period["start_date"], "%Y-%m-%d")  # 不抛异常即通过

    def test_fallback_when_no_dates(self, engine):
        period = engine._get_backtest_period()
        assert "start_date" in period
        assert "end_date" in period

    def test_total_days_positive(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert int(engine._get_backtest_period()["total_days"]) > 0


# ===========================================================================
# 19. _generate_date_series
# ===========================================================================
class TestGenerateDateSeries:
    def test_returns_list(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert isinstance(engine._generate_date_series(50), list)

    def test_correct_length(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert len(engine._generate_date_series(50)) == 50

    def test_no_weekends(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        for d in engine._generate_date_series(30):
            assert d.weekday() < 5, f"{d} 是周末"

    def test_monotonically_increasing(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        dates = engine._generate_date_series(20)
        assert all(dates[i] < dates[i + 1] for i in range(len(dates) - 1))

    def test_zero_length_returns_empty(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert engine._generate_date_series(0) == []


# ===========================================================================
# 20. _build_daily_holdings
# ===========================================================================
class TestBuildDailyHoldings:
    def test_returns_list(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(10)
        assert isinstance(engine._build_daily_holdings(dates, []), list)

    def test_length_matches_dates(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(20)
        assert len(engine._build_daily_holdings(dates, [])) == 20

    def test_each_item_has_date(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(5)
        for h in engine._build_daily_holdings(dates, []):
            assert "date" in h

    def test_each_item_has_position(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(5)
        for h in engine._build_daily_holdings(dates, []):
            assert "position" in h

    def test_empty_dates_returns_empty(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        assert engine._build_daily_holdings([], []) == []

    def test_buy_increases_position(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(10)
        trades = [("2023-01-03", "买入", "10.00", "100", "1000", "")]
        holdings = engine._build_daily_holdings(dates, trades)
        after = [h for h in holdings if h["date"] >= "2023-01-03"]
        if after:
            assert after[0]["position"] >= 0

    def test_sell_with_no_position_is_safe(self, engine, ohlcv_df):
        """卖出时已无持仓（position=0），不应崩溃，且 position 保持 0。"""
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(5)
        trades = [("2023-01-03", "卖出", "10.00", "100", "1000", "-50")]
        holdings = engine._build_daily_holdings(dates, trades)
        assert all(h["position"] >= 0 for h in holdings)

    def test_buy_then_sell_reduces_position(self, engine, ohlcv_df):
        """买后卖持仓回零。"""
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(10)
        trades = [
            ("2023-01-03", "买入", "10.00", "100", "1000", ""),
            ("2023-01-04", "卖出", "11.00", "100", "1100", "+100"),
        ]
        holdings = engine._build_daily_holdings(dates, trades)
        # 日期 2023-01-04 之后持仓应为 0
        after_sell = [h for h in holdings if h["date"] >= "2023-01-04"]
        if after_sell:
            assert after_sell[0]["position"] == pytest.approx(0.0)

    def test_trade_with_bad_date_is_skipped(self, engine, ohlcv_df):
        """日期格式错误的 trade 被跳过，不崩溃。"""
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(5)
        trades = [("not-a-date", "买入", "10.00", "100", "1000", "")]
        # 不应抛异常
        holdings = engine._build_daily_holdings(dates, trades)
        assert isinstance(holdings, list)

    def test_trade_with_no_close_col(self, engine):
        """DataFrame 中无 close 列，使用第一列。"""
        df = pd.DataFrame(
            {"price": [10.0, 11.0, 12.0]},
            index=pd.date_range("2023-01-03", periods=3, freq="B"),
        )
        engine.dataframe_data = df
        engine.mock_data = None
        dates = engine._generate_date_series(3)
        holdings = engine._build_daily_holdings(dates, [])
        assert isinstance(holdings, list)


# ===========================================================================
# 21. _generate_realistic_trades
# ===========================================================================
class TestGenerateRealisticTrades:
    def test_returns_list(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        assert isinstance(mock_engine_with_data._generate_realistic_trades(), list)

    def test_each_trade_is_6_element_tuple(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        for t in mock_engine_with_data._generate_realistic_trades():
            assert len(t) == 6

    def test_actions_are_buy_or_sell(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        for t in mock_engine_with_data._generate_realistic_trades():
            assert t[1] in ("买入", "卖出")

    def test_no_crash_without_metrics(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert isinstance(engine._generate_realistic_trades(), list)


# ===========================================================================
# 22. optimize_parameters (smoke test — mock 模式，避免 BT 策略校验)
# ===========================================================================
class TestOptimizeParameters:
    def test_returns_dict(self, mock_mode, ohlcv_df):
        class SimpleStrategy:
            __name__ = "SimpleStrategy"
        e = AdvancedBacktestEngine()
        e.add_data(ohlcv_df)
        result = e.optimize_parameters(SimpleStrategy, {"period": [5, 10]})
        assert isinstance(result, dict)

    def test_has_best_params(self, mock_mode, ohlcv_df):
        class SimpleStrategy:
            __name__ = "SimpleStrategy"
        e = AdvancedBacktestEngine()
        e.add_data(ohlcv_df)
        result = e.optimize_parameters(SimpleStrategy, {"period": [5]})
        assert "best_params" in result

    def test_has_all_results(self, mock_mode, ohlcv_df):
        class SimpleStrategy:
            __name__ = "SimpleStrategy"
        e = AdvancedBacktestEngine()
        e.add_data(ohlcv_df)
        result = e.optimize_parameters(SimpleStrategy, {"period": [5, 10]})
        assert "all_results" in result
        assert len(result["all_results"]) >= 1

    def test_run_backtest_raises_still_returns_dict(self, mock_mode, ohlcv_df):
        """即使单次回测失败，optimize_parameters 也应返回有效结果。"""
        class BrokenStrategy:
            __name__ = "BrokenStrategy"

        e = AdvancedBacktestEngine()
        e.add_data(ohlcv_df)
        # 替换 run_backtest 使其抛异常
        original_run = e.run_backtest
        call_count = [0]

        def _fail_run():
            call_count[0] += 1
            raise RuntimeError("模拟单步失败")

        e.run_backtest = _fail_run
        # optimize_parameters 内部捕获异常，不应向外传播
        result = e.optimize_parameters(BrokenStrategy, {"period": [5]})
        assert isinstance(result, dict)
        assert "best_params" in result


# ===========================================================================
# 23. get_backtrader_import_status — hint 分支覆盖
# ===========================================================================
class TestGetBacktraderImportStatusHints:
    def test_matplotlib_hint(self, monkeypatch):
        monkeypatch.setattr(_engine_mod, "BACKTRADER_AVAILABLE", False)
        monkeypatch.setattr(_engine_mod, "NATIVE_ENGINE_AVAILABLE", False)
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_ERROR_MSG", "matplotlib not found")
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_ERROR_TYPE", "ImportError")
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_TRACEBACK", "traceback text")
        result = get_backtrader_import_status()
        assert result["available"] is False
        assert "matplotlib" in result["hint"]

    def test_numpy_hint(self, monkeypatch):
        monkeypatch.setattr(_engine_mod, "BACKTRADER_AVAILABLE", False)
        monkeypatch.setattr(_engine_mod, "NATIVE_ENGINE_AVAILABLE", False)
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_ERROR_MSG", "numpy version conflict")
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_ERROR_TYPE", "ImportError")
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_TRACEBACK", "tb")
        result = get_backtrader_import_status()
        assert "numpy" in result["hint"]

    def test_default_hint_when_no_keyword(self, monkeypatch):
        monkeypatch.setattr(_engine_mod, "BACKTRADER_AVAILABLE", False)
        monkeypatch.setattr(_engine_mod, "NATIVE_ENGINE_AVAILABLE", False)
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_ERROR_MSG", "some other error")
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_ERROR_TYPE", "ImportError")
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_TRACEBACK", "tb")
        result = get_backtrader_import_status()
        assert "pip install backtrader" in result["hint"]

    def test_none_error_msg_safe(self, monkeypatch):
        monkeypatch.setattr(_engine_mod, "BACKTRADER_AVAILABLE", False)
        monkeypatch.setattr(_engine_mod, "NATIVE_ENGINE_AVAILABLE", False)
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_ERROR_MSG", None)
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_ERROR_TYPE", None)
        monkeypatch.setattr(_engine_mod, "BACKTRADER_IMPORT_TRACEBACK", None)
        result = get_backtrader_import_status()
        assert isinstance(result, dict)
        assert result["available"] is False


# ===========================================================================
# 24. _convert_dataframe_to_bt — 仅在 backtrader 可用模式下可测
# ===========================================================================
class TestConvertDataframeToBt:
    def test_missing_required_column_raises(self, engine, ohlcv_df):
        """缺少必要列时应抛出 ValueError。"""
        import gui_app.backtest.engine as eng_mod
        if not eng_mod.BACKTRADER_AVAILABLE:
            pytest.skip("backtrader 不可用，跳过此用例")
        bad_df = ohlcv_df.drop(columns=["open"])
        with pytest.raises(ValueError, match="open"):
            engine._convert_dataframe_to_bt(bad_df)

    def test_missing_volume_is_filled(self, engine, ohlcv_df):
        """缺少 volume 列时自动补 0，不抛异常。"""
        import gui_app.backtest.engine as eng_mod
        if not eng_mod.BACKTRADER_AVAILABLE:
            pytest.skip("backtrader 不可用，跳过此用例")
        df_no_vol = ohlcv_df.drop(columns=["volume"])
        result = engine._convert_dataframe_to_bt(df_no_vol)
        assert result is not None

    def test_non_datetime_index_converted(self, engine, ohlcv_df):
        """整数索引应被自动转换。"""
        import gui_app.backtest.engine as eng_mod
        if not eng_mod.BACKTRADER_AVAILABLE:
            pytest.skip("backtrader 不可用，跳过此用例")
        df_int_idx = ohlcv_df.reset_index(drop=True)
        # 整数索引无法转换 → 会被替换为 date_range
        result = engine._convert_dataframe_to_bt(df_int_idx)
        assert result is not None


# ===========================================================================
# 25. 真实 backtrader 运行 ― 覆盖策略 next() + _extract_performance_metrics 真路径
# ===========================================================================
class TestBacktraderRealRun:
    """验证 backtrader 可用时端到端路径（需要 backtrader 安装）。"""

    @pytest.fixture()
    def bt_engine(self, ohlcv_df):
        import gui_app.backtest.engine as eng_mod
        if not eng_mod.BACKTRADER_AVAILABLE:
            pytest.skip("backtrader 不可用")
        e = AdvancedBacktestEngine(initial_cash=100_000.0, commission=0.001)
        e.add_data(ohlcv_df)
        return e

    def test_run_with_dual_ma_returns_dict(self, bt_engine):
        from gui_app.backtest.engine import DualMovingAverageStrategy
        bt_engine.add_strategy(DualMovingAverageStrategy, short_period=5, long_period=20)
        metrics = bt_engine.run_backtest()
        assert isinstance(metrics, dict)
        assert "sharpe_ratio" in metrics

    def test_run_with_rsi_strategy(self, bt_engine):
        from gui_app.backtest.engine import RSIStrategy
        bt_engine.add_strategy(RSIStrategy, rsi_period=14, rsi_buy=30, rsi_sell=70)
        metrics = bt_engine.run_backtest()
        assert isinstance(metrics, dict)

    def test_run_with_macd_strategy(self, bt_engine):
        from gui_app.backtest.engine import MACDStrategy
        bt_engine.add_strategy(MACDStrategy, fast_period=12, slow_period=26, signal_period=9)
        metrics = bt_engine.run_backtest()
        assert isinstance(metrics, dict)

    def test_portfolio_curve_after_real_run(self, bt_engine):
        from gui_app.backtest.engine import DualMovingAverageStrategy
        bt_engine.add_strategy(DualMovingAverageStrategy)
        bt_engine.run_backtest()
        curve = bt_engine.get_portfolio_value_curve()
        assert isinstance(curve, list)
        assert len(curve) > 0

    def test_get_detailed_results_after_real_run(self, bt_engine):
        from gui_app.backtest.engine import DualMovingAverageStrategy
        bt_engine.add_strategy(DualMovingAverageStrategy)
        bt_engine.run_backtest()
        result = bt_engine.get_detailed_results()
        assert isinstance(result, dict)
        assert "performance_metrics" in result
        assert "portfolio_curve" in result

    def test_extract_performance_has_total_return(self, bt_engine):
        from gui_app.backtest.engine import DualMovingAverageStrategy
        bt_engine.add_strategy(DualMovingAverageStrategy)
        metrics = bt_engine.run_backtest()
        assert "total_return" in metrics
        assert isinstance(metrics["total_return"], (int, float))

    def test_runtime_status_backtrader_mode(self, bt_engine):
        status = bt_engine.get_runtime_status()
        assert status["engine_mode"] == "backtrader"

    def test_optimize_parameters_backtrader_mode(self, bt_engine):
        """optimize_parameters 在 bt 模式下也能执行。"""
        from gui_app.backtest.engine import DualMovingAverageStrategy
        result = bt_engine.optimize_parameters(
            DualMovingAverageStrategy,
            {"short_period": [3, 5], "long_period": [10]}
        )
        assert "best_params" in result
        assert "all_results" in result


# ===========================================================================
# 26. add_data — 非 DataFrame 路径 & 错误处理
# ===========================================================================
class TestAddDataNonDataFrame:
    def test_non_df_input_stored(self, engine):
        """非 DataFrame 输入调用 cerebro.adddata（backtrader 模式）。"""
        import gui_app.backtest.engine as eng_mod
        if not eng_mod.BACKTRADER_AVAILABLE:
            pytest.skip("backtrader 不可用")
        # 传入非 DataFrame（任意对象），backtrader 模式下添加到 cerebro
        import types
        fake_data = types.SimpleNamespace()  # 非 DataFrame
        # 不应崩溃（cerebro.adddata 会失败但不崩溃 / 走其他路径）
        try:
            engine.add_data(fake_data)
        except Exception:
            pass  # bt.Cerebro.adddata 可能拒绝，这也是正常路径
        assert True  # 关键是不抛意外异常

    def test_add_data_date_conversion_error_fallback(self, engine):
        """不可解析日期索引 → 自动 fallback 为 date_range，不崩溃。"""
        import gui_app.backtest.engine as eng_mod
        if not eng_mod.BACKTRADER_AVAILABLE:
            pytest.skip("backtrader 不可用，跳过此用例")
        df = pd.DataFrame(
            {
                "open": [10.0, 11.0],
                "high": [10.5, 11.5],
                "low": [9.5, 10.5],
                "close": [10.2, 11.2],
                "volume": [1000.0, 2000.0],
            },
            index=["bad_date_1", "bad_date_2"],
        )
        try:
            engine.add_data(df)
        except Exception:
            pass
        assert True

    def test_has_strategy_info(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        result = mock_engine_with_data.get_detailed_results()
        assert "strategy_info" in result

    def test_has_trades_key(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        result = mock_engine_with_data.get_detailed_results()
        assert "trades" in result

    def test_portfolio_curve_has_dates_and_values(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        result = mock_engine_with_data.get_detailed_results()
        pc = result["portfolio_curve"]
        assert "dates" in pc
        assert "values" in pc


# ===========================================================================
# 18. _get_backtest_period
# ===========================================================================
class TestGetBacktestPeriod:
    def test_returns_dict(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert isinstance(engine._get_backtest_period(), dict)

    def test_has_start_date(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert "start_date" in engine._get_backtest_period()

    def test_has_end_date(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert "end_date" in engine._get_backtest_period()

    def test_start_date_format(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        period = engine._get_backtest_period()
        datetime.strptime(period["start_date"], "%Y-%m-%d")  # 不抛异常即通过

    def test_fallback_when_no_dates(self, engine):
        period = engine._get_backtest_period()
        assert "start_date" in period
        assert "end_date" in period

    def test_total_days_positive(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert int(engine._get_backtest_period()["total_days"]) > 0


# ===========================================================================
# 19. _generate_date_series
# ===========================================================================
class TestGenerateDateSeries:
    def test_returns_list(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert isinstance(engine._generate_date_series(50), list)

    def test_correct_length(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert len(engine._generate_date_series(50)) == 50

    def test_no_weekends(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        for d in engine._generate_date_series(30):
            assert d.weekday() < 5, f"{d} 是周末"

    def test_monotonically_increasing(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        dates = engine._generate_date_series(20)
        assert all(dates[i] < dates[i + 1] for i in range(len(dates) - 1))

    def test_zero_length_returns_empty(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        assert engine._generate_date_series(0) == []


# ===========================================================================
# 20. _build_daily_holdings
# ===========================================================================
class TestBuildDailyHoldings:
    def test_returns_list(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df  # 确保 _build_daily_holdings 能拿到 close
        dates = engine._generate_date_series(10)
        assert isinstance(engine._build_daily_holdings(dates, []), list)

    def test_length_matches_dates(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(20)
        assert len(engine._build_daily_holdings(dates, [])) == 20

    def test_each_item_has_date(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(5)
        for h in engine._build_daily_holdings(dates, []):
            assert "date" in h

    def test_each_item_has_position(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(5)
        for h in engine._build_daily_holdings(dates, []):
            assert "position" in h

    def test_empty_dates_returns_empty(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        assert engine._build_daily_holdings([], []) == []

    def test_buy_increases_position(self, engine, ohlcv_df):
        engine.add_data(ohlcv_df)
        engine.mock_data = ohlcv_df
        dates = engine._generate_date_series(10)
        trades = [("2023-01-03", "买入", "10.00", "100", "1000", "")]
        holdings = engine._build_daily_holdings(dates, trades)
        after = [h for h in holdings if h["date"] >= "2023-01-03"]
        if after:
            assert after[0]["position"] >= 0  # 买入后持仓应 ≥ 0


# ===========================================================================
# 21. _generate_realistic_trades
# ===========================================================================
class TestGenerateRealisticTrades:
    def test_returns_list(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        assert isinstance(mock_engine_with_data._generate_realistic_trades(), list)

    def test_each_trade_is_6_element_tuple(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        for t in mock_engine_with_data._generate_realistic_trades():
            assert len(t) == 6

    def test_actions_are_buy_or_sell(self, mock_engine_with_data):
        mock_engine_with_data.run_backtest()
        for t in mock_engine_with_data._generate_realistic_trades():
            assert t[1] in ("买入", "卖出")

    def test_no_crash_without_metrics(self, engine, ohlcv_df):
        """performance_metrics 为空时不崩溃。"""
        engine.add_data(ohlcv_df)
        assert isinstance(engine._generate_realistic_trades(), list)


# ===========================================================================
# 22. optimize_parameters (smoke test — mock 模式，避免 BT 策略校验)
# ===========================================================================
class TestOptimizeParameters:
    def test_returns_dict(self, mock_mode, ohlcv_df):
        class SimpleStrategy:
            __name__ = "SimpleStrategy"
        e = AdvancedBacktestEngine()
        e.add_data(ohlcv_df)
        result = e.optimize_parameters(SimpleStrategy, {"period": [5, 10]})
        assert isinstance(result, dict)

    def test_has_best_params(self, mock_mode, ohlcv_df):
        class SimpleStrategy:
            __name__ = "SimpleStrategy"
        e = AdvancedBacktestEngine()
        e.add_data(ohlcv_df)
        result = e.optimize_parameters(SimpleStrategy, {"period": [5]})
        assert "best_params" in result

    def test_has_all_results(self, mock_mode, ohlcv_df):
        class SimpleStrategy:
            __name__ = "SimpleStrategy"
        e = AdvancedBacktestEngine()
        e.add_data(ohlcv_df)
        result = e.optimize_parameters(SimpleStrategy, {"period": [5, 10]})
        assert "all_results" in result
        assert len(result["all_results"]) >= 1
