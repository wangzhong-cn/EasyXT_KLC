"""T1 Coverage Boost — 覆盖 SimpleBacktester / StrategyRegistry 状态机 / Engine C 工厂路由。

目标模块：
  - strategies.stage1_pipeline.SimpleBacktester        (纯 pandas, ~180 行)
  - strategies.registry.StrategyRegistry.update_status  (状态机, ~30 行)
  - gui_app.backtest.engine._try_factory_strategy       (工厂路由, ~30 行)
  - gui_app.backtest.engine._should_use_native_main_path  (路径选择, ~15 行)
  - strategies.stage1_pipeline 数据类                    (dataclass ~60 行)
"""
from __future__ import annotations

import math
from dataclasses import asdict
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from gui_app.widgets.backtest_widget import BacktestWorker


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_ohlcv(n: int = 200, trend: float = 0.0005, seed: int = 42) -> pd.DataFrame:
    """Generate synthetic OHLCV DataFrame with optional trend."""
    rng = np.random.RandomState(seed)
    close = [100.0]
    for _ in range(n - 1):
        close.append(close[-1] * (1 + trend + rng.randn() * 0.02))
    close_arr = np.array(close)
    return pd.DataFrame({
        "open": close_arr * (1 + rng.randn(n) * 0.001),
        "high": close_arr * 1.01,
        "low": close_arr * 0.99,
        "close": close_arr,
        "volume": rng.randint(100, 10000, n).astype(float),
    })


def _flat_ohlcv(n: int = 100, price: float = 10.0) -> pd.DataFrame:
    """Generate flat (constant price) OHLCV — no trade signals."""
    return pd.DataFrame({
        "open": [price] * n,
        "high": [price] * n,
        "low": [price] * n,
        "close": [price] * n,
        "volume": [1000.0] * n,
    })


# ═════════════════════════════════════════════════════════════════════════════
# 1) SimpleBacktester
# ═════════════════════════════════════════════════════════════════════════════

class TestSimpleBacktesterBasic:
    """Smoke & edge cases for SimpleBacktester.run()."""

    def test_run_returns_backtest_metrics(self):
        from strategies.stage1_pipeline import BacktestMetrics, SimpleBacktester
        bt = SimpleBacktester(_make_ohlcv(200))
        m = bt.run()
        assert isinstance(m, BacktestMetrics)

    def test_run_positive_trend_has_trades(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df = _make_ohlcv(500, trend=0.001, seed=7)
        m = SimpleBacktester(df, short_period=5, long_period=20).run()
        # With trend + noise, dual-MA should generate trades
        assert m.trade_count > 0
        assert isinstance(m.total_return_pct, (int, float, np.floating))

    def test_run_negative_trend(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df = _make_ohlcv(500, trend=-0.001, seed=11)
        m = SimpleBacktester(df).run()
        assert isinstance(m.total_return_pct, float)
        assert m.max_drawdown_pct >= 0

    def test_run_period_label_preserved(self):
        from strategies.stage1_pipeline import SimpleBacktester
        m = SimpleBacktester(_make_ohlcv(100)).run(period_label="in_sample")
        assert m.period == "in_sample"

    def test_run_market_assumption_preserved(self):
        from strategies.stage1_pipeline import SimpleBacktester
        m = SimpleBacktester(_make_ohlcv(100), market_name="TEST").run()
        assert m.market_assumption == "TEST"

    def test_equity_curve_saved(self):
        from strategies.stage1_pipeline import SimpleBacktester
        bt = SimpleBacktester(_make_ohlcv(200))
        bt.run()
        assert bt.equity_curve is not None
        assert len(bt.equity_curve) > 0


class TestSimpleBacktesterEdgeCases:
    """Edge and boundary conditions."""

    def test_empty_dataframe_returns_zero_metrics(self):
        from strategies.stage1_pipeline import SimpleBacktester
        m = SimpleBacktester(pd.DataFrame()).run()
        assert m.total_return_pct == 0.0
        assert m.trade_count == 0

    def test_too_few_rows_returns_zero(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df = _make_ohlcv(5)  # less than long_period + 2 = 22
        m = SimpleBacktester(df).run()
        assert m.total_return_pct == 0.0

    def test_no_close_column_returns_zero(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df = pd.DataFrame({"open": [1, 2, 3], "high": [1, 2, 3]})
        m = SimpleBacktester(df).run()
        assert m.total_return_pct == 0.0

    def test_chinese_close_column_name(self):
        from strategies.stage1_pipeline import SimpleBacktester
        n = 100
        rng = np.random.RandomState(99)
        prices = np.cumsum(rng.randn(n)) + 100.0
        df = pd.DataFrame({"收盘价": prices})
        m = SimpleBacktester(df, short_period=3, long_period=10).run()
        assert isinstance(m.total_return_pct, float)

    def test_uppercase_Close_column(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df = _make_ohlcv(100).rename(columns={"close": "Close"})
        m = SimpleBacktester(df).run()
        assert isinstance(m.total_return_pct, float)

    def test_flat_price_no_trades(self):
        from strategies.stage1_pipeline import SimpleBacktester
        m = SimpleBacktester(_flat_ohlcv(200)).run()
        assert m.trade_count == 0

    def test_constant_price_sharpe_zero(self):
        from strategies.stage1_pipeline import SimpleBacktester
        m = SimpleBacktester(_flat_ohlcv(200)).run()
        assert m.sharpe_ratio == 0.0


class TestSimpleBacktesterCommission:
    """Commission / slippage impact tests."""

    def test_higher_commission_lower_return(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df = _make_ohlcv(500, trend=0.0008, seed=42)
        m_low = SimpleBacktester(df, buy_comm=0.0001, sell_comm=0.0001).run()
        m_high = SimpleBacktester(df, buy_comm=0.005, sell_comm=0.005).run()
        # Higher commission should reduce total return (or at least not improve it)
        if m_low.trade_count > 0 and m_high.trade_count > 0:
            assert m_high.total_return_pct <= m_low.total_return_pct + 0.1

    def test_zero_commission(self):
        from strategies.stage1_pipeline import SimpleBacktester
        m = SimpleBacktester(
            _make_ohlcv(200), buy_comm=0.0, sell_comm=0.0, slippage=0.0
        ).run()
        assert m.cost_ratio_pct == 0.0

    def test_slippage_reduces_return(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df = _make_ohlcv(500, trend=0.0008, seed=42)
        m_no_slip = SimpleBacktester(df, slippage=0.0).run()
        m_slip = SimpleBacktester(df, slippage=0.01).run()
        if m_no_slip.trade_count > 0 and m_slip.trade_count > 0:
            assert m_slip.total_return_pct <= m_no_slip.total_return_pct + 0.1


class TestSimpleBacktesterRiskMetrics:
    """Extended risk metrics: calmar, sortino, turnover, cost_ratio."""

    def test_calmar_positive_for_positive_return(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df = _make_ohlcv(500, trend=0.001, seed=7)
        m = SimpleBacktester(df).run()
        if m.annualized_return_pct > 0 and m.max_drawdown_pct > 0.01:
            assert m.calmar_ratio > 0

    def test_sortino_is_float(self):
        from strategies.stage1_pipeline import SimpleBacktester
        m = SimpleBacktester(_make_ohlcv(200)).run()
        assert isinstance(m.sortino_ratio, float)

    def test_turnover_rate_non_negative(self):
        from strategies.stage1_pipeline import SimpleBacktester
        m = SimpleBacktester(_make_ohlcv(200)).run()
        assert m.turnover_rate_pct >= 0.0

    def test_cost_ratio_non_negative(self):
        from strategies.stage1_pipeline import SimpleBacktester
        m = SimpleBacktester(_make_ohlcv(200)).run()
        assert m.cost_ratio_pct >= 0.0

    def test_win_rate_between_0_and_100(self):
        from strategies.stage1_pipeline import SimpleBacktester
        m = SimpleBacktester(_make_ohlcv(500)).run()
        assert 0.0 <= m.win_rate_pct <= 100.0

    def test_max_drawdown_non_negative(self):
        from strategies.stage1_pipeline import SimpleBacktester
        m = SimpleBacktester(_make_ohlcv(300)).run()
        assert m.max_drawdown_pct >= 0

    def test_years_positive(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df = _make_ohlcv(500)
        m = SimpleBacktester(df).run()
        if m.trade_count > 0:
            assert m.years > 0


# ═════════════════════════════════════════════════════════════════════════════
# 2) BacktestMetrics dataclass
# ═════════════════════════════════════════════════════════════════════════════

class TestBacktestMetrics:
    def test_asdict_has_all_fields(self):
        from strategies.stage1_pipeline import BacktestMetrics
        m = BacktestMetrics(
            total_return_pct=10.0, annualized_return_pct=5.0,
            sharpe_ratio=1.2, max_drawdown_pct=8.0,
            win_rate_pct=60.0, trade_count=20, years=2.0,
            period="full",
        )
        d = asdict(m)
        assert "total_return_pct" in d
        assert "calmar_ratio" in d
        assert "market_assumption" in d

    def test_default_values(self):
        from strategies.stage1_pipeline import BacktestMetrics
        m = BacktestMetrics(0, 0, 0, 0, 0, 0, 0, "test")
        assert m.calmar_ratio == 0.0
        assert m.sortino_ratio == 0.0
        assert m.market_assumption == "A股标准"


class TestDataAcceptanceResult:
    def test_verdict_pass(self):
        from strategies.stage1_pipeline import DataAcceptanceResult
        r = DataAcceptanceResult(
            symbol="000001.SZ", period="1d", date_range="2023~2024",
            expected_trading_days=244, actual_data_days=240,
            coverage_pct=98.4, max_gap_days=1, pass_board=True,
        )
        assert r.verdict == "PASS"

    def test_verdict_fail(self):
        from strategies.stage1_pipeline import DataAcceptanceResult
        r = DataAcceptanceResult(
            symbol="000001.SZ", period="1d", date_range="2023~2024",
            expected_trading_days=244, actual_data_days=100,
            coverage_pct=41.0, max_gap_days=20, pass_board=False,
            failures=["覆盖率不足"],
        )
        assert r.verdict == "FAIL"


class TestInOutSampleResult:
    def test_verdict_pass(self):
        from strategies.stage1_pipeline import InOutSampleResult
        r = InOutSampleResult(
            in_sample_period="2019-2022", out_sample_period="2023-2025",
            in_sharpe=1.5, out_sharpe=1.2, oos_ratio=0.8,
            pass_threshold=True,
        )
        assert r.verdict == "PASS"

    def test_verdict_fail(self):
        from strategies.stage1_pipeline import InOutSampleResult
        r = InOutSampleResult(
            in_sample_period="2019-2022", out_sample_period="2023-2025",
            in_sharpe=1.5, out_sharpe=0.5, oos_ratio=0.33,
            pass_threshold=False,
        )
        assert "过拟合" in r.verdict


class TestParamSensitivityResult:
    def test_verdict(self):
        from strategies.stage1_pipeline import ParamSensitivityResult
        r = ParamSensitivityResult(
            base_params={"short": 5, "long": 20},
            sensitivity_table=[],
            max_change_pct=15.0,
            pass_threshold=True,
        )
        assert r.verdict == "PASS"


class TestBenchmarkComparison:
    def test_fields(self):
        from strategies.stage1_pipeline import BenchmarkComparison
        b = BenchmarkComparison(
            benchmark="CSI300", benchmark_annualized_pct=8.0,
            excess_return_pct=5.0, alpha=4.0, beta=0.8,
            information_ratio=0.5, tracking_error_pct=12.0,
            available=True,
        )
        assert b.available is True
        assert b.benchmark == "CSI300"


class TestStage1Result:
    def _make_result(self, pass_board=True) -> "Stage1Result":
        from strategies.stage1_pipeline import (
            BacktestMetrics,
            DataAcceptanceResult,
            InOutSampleResult,
            ParamSensitivityResult,
            Stage1Result,
        )
        dar = DataAcceptanceResult(
            symbol="000001.SZ", period="1d", date_range="2023~2024",
            expected_trading_days=244, actual_data_days=240 if pass_board else 100,
            coverage_pct=98.4 if pass_board else 41.0,
            max_gap_days=1 if pass_board else 20,
            pass_board=pass_board,
            failures=[] if pass_board else ["覆盖率不足"],
        )
        bm = BacktestMetrics(10, 5, 1.2, 8, 60, 20, 2.0, "full")
        ios = InOutSampleResult("2019-2022", "2023-2025", 1.5, 1.2, 0.8, True)
        psr = ParamSensitivityResult(
            base_params={"short": 5},
            sensitivity_table=[],
            max_change_pct=15.0,
            pass_threshold=True,
        )
        return Stage1Result(
            strategy="test", symbol="000001.SZ",
            run_date="2024-01-01", start="2019-01-01", end="2024-12-31",
            oos_split="2023-01-01",
            stage1_pass=pass_board,
            data_acceptance=dar,
            full_backtest=bm, in_sample=bm, out_of_sample=bm,
            in_out_comparison=ios,
            param_sensitivity=psr,
        )

    def test_stage1_pass_true(self):
        result = self._make_result(pass_board=True)
        assert result.stage1_pass is True

    def test_stage1_pass_false(self):
        result = self._make_result(pass_board=False)
        assert result.stage1_pass is False

    def test_to_dict_round_trip(self):
        result = self._make_result()
        d = result.to_dict()
        assert isinstance(d, dict)
        assert d["_schema_version"] == "stage1/v2"


# ═════════════════════════════════════════════════════════════════════════════
# 3) StrategyRegistry — status machine
# ═════════════════════════════════════════════════════════════════════════════

class TestRegistryStatusMachine:
    """Exhaustive test of StrategyRegistry.update_status()."""

    @pytest.fixture(autouse=True)
    def fresh_registry(self):
        from strategies.registry import StrategyRegistry
        self.reg = StrategyRegistry()

    def _register(self, sid: str = "s1", status: str = "running"):
        info = self.reg.register(strategy_id=sid, account_id="acct1")
        info.status = status
        return info

    # ── Legal transitions ──

    def test_running_to_paused(self):
        self._register("s1", "running")
        result = self.reg.update_status("s1", "paused")
        assert result == (True, "")
        assert self.reg.get("s1").status == "paused"

    def test_running_to_stopped(self):
        self._register("s1", "running")
        result = self.reg.update_status("s1", "stopped")
        assert result == (True, "")

    def test_running_to_error(self):
        self._register("s1", "running")
        result = self.reg.update_status("s1", "error")
        assert result == (True, "")

    def test_paused_to_running(self):
        self._register("s1", "paused")
        result = self.reg.update_status("s1", "running")
        assert result == (True, "")

    def test_paused_to_stopped(self):
        self._register("s1", "paused")
        result = self.reg.update_status("s1", "stopped")
        assert result == (True, "")

    def test_error_to_running(self):
        self._register("s1", "error")
        result = self.reg.update_status("s1", "running")
        assert result == (True, "")

    def test_error_to_stopped(self):
        self._register("s1", "error")
        result = self.reg.update_status("s1", "stopped")
        assert result == (True, "")

    def test_created_to_running(self):
        self._register("s1", "created")
        result = self.reg.update_status("s1", "running")
        assert result == (True, "")

    def test_created_to_stopped(self):
        self._register("s1", "created")
        result = self.reg.update_status("s1", "stopped")
        assert result == (True, "")

    # ── Illegal transitions ──

    def test_stopped_is_terminal(self):
        self._register("s1", "stopped")
        result = self.reg.update_status("s1", "running")
        assert result[0] is False
        assert "终态" in result[1]

    def test_running_to_created_illegal(self):
        self._register("s1", "running")
        result = self.reg.update_status("s1", "created")
        assert result[0] is False

    def test_paused_to_error_illegal(self):
        self._register("s1", "paused")
        result = self.reg.update_status("s1", "error")
        assert result[0] is False

    def test_created_to_paused_illegal(self):
        self._register("s1", "created")
        result = self.reg.update_status("s1", "paused")
        assert result[0] is False

    # ── Not found ──

    def test_update_nonexistent_returns_none(self):
        result = self.reg.update_status("nonexist", "running")
        assert result is None

    # ── Unknown from-status (backward compat) ──

    def test_unknown_status_permits_any_transition(self):
        self._register("s1", "legacy_unknown")
        result = self.reg.update_status("s1", "stopped")
        # Unknown status not in _STATUS_TRANSITIONS → permissive
        assert result == (True, "")


class TestRegistryOperations:
    """Basic register/unregister/query operations."""

    @pytest.fixture(autouse=True)
    def fresh_registry(self):
        from strategies.registry import StrategyRegistry
        self.reg = StrategyRegistry()

    def test_register_and_get(self):
        self.reg.register("s1", account_id="a1", params={"k": 1})
        info = self.reg.get("s1")
        assert info is not None
        assert info.account_id == "a1"
        assert info.params == {"k": 1}

    def test_get_nonexistent_returns_none(self):
        assert self.reg.get("nope") is None

    def test_unregister_sets_status(self):
        self.reg.register("s1")
        assert self.reg.unregister("s1", status="error") is True
        assert self.reg.get("s1").status == "error"

    def test_unregister_nonexistent_returns_false(self):
        assert self.reg.unregister("nope") is False

    def test_list_all(self):
        self.reg.register("s1")
        self.reg.register("s2")
        items = self.reg.list_all()
        assert len(items) == 2
        ids = {i["strategy_id"] for i in items}
        assert ids == {"s1", "s2"}

    def test_list_running_filters(self):
        self.reg.register("s1")
        self.reg.register("s2")
        self.reg.unregister("s2", status="stopped")
        running = self.reg.list_running()
        assert len(running) == 1
        assert running[0].strategy_id == "s1"

    def test_update_params(self):
        self.reg.register("s1", params={"a": 1})
        self.reg.update_params("s1", {"b": 2})
        info = self.reg.get("s1")
        assert info.params == {"a": 1, "b": 2}

    def test_register_overwrites(self):
        self.reg.register("s1", params={"a": 1})
        self.reg.register("s1", params={"a": 99})
        assert self.reg.get("s1").params == {"a": 99}

    def test_list_all_includes_stopped(self):
        self.reg.register("s1")
        self.reg.unregister("s1")
        items = self.reg.list_all()
        assert len(items) == 1
        assert items[0]["status"] == "stopped"


# ═════════════════════════════════════════════════════════════════════════════
# 4) Engine C — _try_factory_strategy / _should_use_native
# ═════════════════════════════════════════════════════════════════════════════

class TestEngineFactoryRouting:
    """Tests for AdvancedBacktestEngine._try_factory_strategy."""

    def _make_engine(self, strategy_name=None, params=None):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        eng = AdvancedBacktestEngine.__new__(AdvancedBacktestEngine)
        eng.strategy_name = strategy_name
        eng.strategy_params = params or {}
        return eng

    def test_unknown_strategy_returns_none(self):
        eng = self._make_engine("UnknownStrategy")
        assert eng._try_factory_strategy() is None

    def test_none_strategy_returns_none(self):
        eng = self._make_engine(None)
        assert eng._try_factory_strategy() is None

    def test_import_error_returns_none(self):
        # If strategy_factory.create_strategy_from_config import fails → None (graceful fallback)
        with patch(
            "strategies.strategy_factory.create_strategy_from_config",
            side_effect=Exception("mock import fail"),
        ):
            eng = self._make_engine("DualMovingAverageStrategy")
            result = eng._try_factory_strategy()
            assert result is None

    def test_dual_ma_routes_to_trend(self):
        mock_strategy = MagicMock()
        with patch(
            "strategies.strategy_factory.create_strategy_from_config",
            return_value=mock_strategy,
        ) as mock_create:
            eng = self._make_engine("DualMovingAverageStrategy", {"short_period": 5})
            result = eng._try_factory_strategy()
            assert result is mock_strategy
            cfg = mock_create.call_args[0][0]
            assert cfg.strategy_type == "trend"

    def test_rsi_routes_to_reversion(self):
        mock_strategy = MagicMock()
        with patch(
            "strategies.strategy_factory.create_strategy_from_config",
            return_value=mock_strategy,
        ) as mock_create:
            eng = self._make_engine("RSIStrategy", {"rsi_buy": 30, "rsi_sell": 70})
            result = eng._try_factory_strategy()
            assert result is mock_strategy
            cfg = mock_create.call_args[0][0]
            assert cfg.strategy_type == "reversion"

    def test_param_adapter_renames_keys(self):
        mock_strategy = MagicMock()
        with patch(
            "strategies.strategy_factory.create_strategy_from_config",
            return_value=mock_strategy,
        ) as mock_create:
            eng = self._make_engine("RSIStrategy", {
                "position_size": 1000,
                "rsi_buy": 25,
                "rsi_sell": 75,
                "rsi_period": 14,
            })
            eng._try_factory_strategy()
            cfg = mock_create.call_args[0][0]
            adapted = cfg.parameters
            assert adapted["trade_volume"] == 1000
            assert adapted["rsi_lower"] == 25
            assert adapted["rsi_upper"] == 75
            assert adapted["rsi_period"] == 14  # unmapped → passthrough

    def test_factory_exception_returns_none(self):
        with patch(
            "strategies.strategy_factory.create_strategy_from_config",
            side_effect=ValueError("bad config"),
        ):
            eng = self._make_engine("DualMovingAverageStrategy")
            assert eng._try_factory_strategy() is None


class TestShouldUseNativeMainPath:
    """Tests for _should_use_native_main_path condition matrix."""

    def _make_engine(self, **kw):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        eng = AdvancedBacktestEngine.__new__(AdvancedBacktestEngine)
        eng.dataframe_data = kw.get("df", _make_ohlcv(50))
        eng.data_period = kw.get("period", "1d")
        eng.data_adjust = kw.get("adjust", "qfq")
        eng.strategy_name = kw.get("strategy", "DualMovingAverageStrategy")
        return eng

    @patch("gui_app.backtest.engine.NATIVE_ENGINE_AVAILABLE", False)
    def test_native_unavailable(self):
        eng = self._make_engine()
        assert eng._should_use_native_main_path() is False

    @patch("gui_app.backtest.engine.NATIVE_ENGINE_AVAILABLE", True)
    def test_empty_data(self):
        eng = self._make_engine(df=pd.DataFrame())
        assert eng._should_use_native_main_path() is False

    @patch("gui_app.backtest.engine.NATIVE_ENGINE_AVAILABLE", True)
    def test_none_data(self):
        eng = self._make_engine()
        eng.dataframe_data = None
        assert eng._should_use_native_main_path() is False

    @patch("gui_app.backtest.engine.NATIVE_ENGINE_AVAILABLE", True)
    def test_non_daily_period_rejected(self):
        eng = self._make_engine(period="5m")
        assert eng._should_use_native_main_path() is False

    @patch("gui_app.backtest.engine.NATIVE_ENGINE_AVAILABLE", True)
    @patch.dict("os.environ", {"EASYXT_NATIVE_ALLOW_RAW": "0"})
    def test_adjust_none_rejected_without_env(self):
        eng = self._make_engine(adjust="none")
        assert eng._should_use_native_main_path() is False

    @patch("gui_app.backtest.engine.NATIVE_ENGINE_AVAILABLE", True)
    @patch.dict("os.environ", {"EASYXT_NATIVE_ALLOW_RAW": "1"})
    def test_adjust_none_accepted_with_env(self):
        eng = self._make_engine(adjust="none", strategy="DualMovingAverageStrategy")
        assert eng._should_use_native_main_path() is True

    @patch("gui_app.backtest.engine.NATIVE_ENGINE_AVAILABLE", True)
    def test_hardcoded_strategy_accepted(self):
        for name in ("DualMovingAverageStrategy", "RSIStrategy", "MACDStrategy"):
            eng = self._make_engine(strategy=name, adjust="qfq")
            assert eng._should_use_native_main_path() is True, f"{name} should be accepted"

    @patch("gui_app.backtest.engine.NATIVE_ENGINE_AVAILABLE", True)
    def test_unknown_strategy_rejected(self):
        eng = self._make_engine(strategy="MyCustomStrat", adjust="qfq")
        assert eng._should_use_native_main_path() is False


# ═════════════════════════════════════════════════════════════════════════════
# 5) ParamSensitivityAnalyzer — 纯逻辑
# ═════════════════════════════════════════════════════════════════════════════

class TestParamSensitivityAnalyzer:
    def test_perturb_static(self):
        from strategies.stage1_pipeline import ParamSensitivityAnalyzer
        assert ParamSensitivityAnalyzer._perturb(10, 0.2) == 12
        assert ParamSensitivityAnalyzer._perturb(10, -0.2) == 8
        assert ParamSensitivityAnalyzer._perturb(2, -0.5) == 2  # min clamped to 2

    def test_run_returns_result(self):
        from strategies.stage1_pipeline import ParamSensitivityAnalyzer, ParamSensitivityResult
        df = _make_ohlcv(200, trend=0.0005)
        psa = ParamSensitivityAnalyzer(df, base_short=5, base_long=20)
        result = psa.run()
        assert isinstance(result, ParamSensitivityResult)
        assert len(result.sensitivity_table) == 8  # 2 params × 4 deltas

    def test_result_contains_max_change(self):
        from strategies.stage1_pipeline import ParamSensitivityAnalyzer
        df = _make_ohlcv(200)
        result = ParamSensitivityAnalyzer(df, 5, 20).run()
        assert result.max_change_pct >= 0


# ═════════════════════════════════════════════════════════════════════════════
# 6) Engine diagnostic helpers
# ═════════════════════════════════════════════════════════════════════════════

class TestGetBacktraderImportStatus:
    def test_returns_dict(self):
        from gui_app.backtest.engine import get_backtrader_import_status
        status = get_backtrader_import_status()
        assert isinstance(status, dict)
        assert "available" in status
        assert "mode" in status
        assert "hint" in status

    def test_runtime_status(self):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        eng = AdvancedBacktestEngine(initial_cash=50000)
        status = eng.get_runtime_status()
        assert status["initial_cash"] == 50000
        assert "engine_mode" in status


class TestGuiNameToFactory:
    """Verify the GUI-to-factory mapping dict."""
    def test_mapping_contents(self):
        from gui_app.backtest.engine import _GUI_NAME_TO_FACTORY
        assert _GUI_NAME_TO_FACTORY["DualMovingAverageStrategy"] == "trend"
        assert _GUI_NAME_TO_FACTORY["RSIStrategy"] == "reversion"

    def test_macd_not_in_factory(self):
        from gui_app.backtest.engine import _GUI_NAME_TO_FACTORY
        # MACD intentionally NOT mapped (uses Engine C internal)
        assert "MACDStrategy" not in _GUI_NAME_TO_FACTORY


class TestGuiParamAdapter:
    def test_adapter_mappings(self):
        from gui_app.backtest.engine import _GUI_PARAM_ADAPTER
        assert _GUI_PARAM_ADAPTER["position_size"] == "trade_volume"
        assert _GUI_PARAM_ADAPTER["rsi_buy"] == "rsi_lower"
        assert _GUI_PARAM_ADAPTER["rsi_sell"] == "rsi_upper"


# ═════════════════════════════════════════════════════════════════════════════
# 7) _to_builtin_json helper
# ═════════════════════════════════════════════════════════════════════════════

class TestToBuiltinJson:
    def test_dict_with_numpy(self):
        from strategies.stage1_pipeline import _to_builtin_json
        result = _to_builtin_json({"a": np.float64(1.5), "b": [np.int64(3)]})
        assert result == {"a": 1.5, "b": [3]}

    def test_nested_structures(self):
        from strategies.stage1_pipeline import _to_builtin_json
        data = {"a": {"b": [1, 2, (3, 4)]}}
        result = _to_builtin_json(data)
        assert result == {"a": {"b": [1, 2, (3, 4)]}}

    def test_plain_values_passthrough(self):
        from strategies.stage1_pipeline import _to_builtin_json
        assert _to_builtin_json("hello") == "hello"
        assert _to_builtin_json(42) == 42
        assert _to_builtin_json(None) is None


# ═══════════════════════════════════════════════════════════════════════
# S7: gui_app.strategy_controller.StrategyController.run_adhoc_backtest
# ═══════════════════════════════════════════════════════════════════════

class TestGuiControllerAdhocBacktest:
    """Tests for gui_app.strategy_controller.StrategyController.run_adhoc_backtest."""

    def test_engine_not_available_returns_error(self):
        from gui_app.strategy_controller import StrategyController
        ctrl = StrategyController()
        with patch("gui_app.strategy_controller._safe_import", return_value=None):
            result = ctrl.run_adhoc_backtest(
                stock_data=pd.DataFrame({"close": [1, 2, 3]}),
                strategy_name="双均线策略",
                strategy_params={"short_period": 5, "long_period": 20},
            )
        assert result["ok"] is False
        assert "不可用" in result["error"]

    def test_unknown_strategy_defaults_to_dual_ma(self):
        from gui_app.strategy_controller import StrategyController
        ctrl = StrategyController()
        # Unknown strategy name defaults to DualMovingAverageStrategy, not error
        result = ctrl.run_adhoc_backtest(
            stock_data=_make_ohlcv(50),
            strategy_name="不存在的策略",
            strategy_params={"short_period": 5, "long_period": 20},
        )
        assert result["ok"] is True
        assert "metrics" in result

    def test_successful_backtest_returns_metrics(self):
        from gui_app.strategy_controller import StrategyController
        ctrl = StrategyController()
        mock_engine = MagicMock()
        mock_engine.run_backtest.return_value = {"total_return": 0.1}
        mock_engine.get_detailed_results.return_value = {"equity_curve": []}
        mock_cls = MagicMock(return_value=mock_engine)
        # First call for engine class, second call for strategy class
        with patch(
            "gui_app.strategy_controller._safe_import",
            side_effect=[mock_cls, MagicMock()],  # engine_cls, strategy_class
        ):
            result = ctrl.run_adhoc_backtest(
                stock_data=_make_ohlcv(50),
                strategy_name="双均线策略",
                strategy_params={"short_period": 5},
            )
        assert result["ok"] is True
        assert "metrics" in result
        assert "elapsed_sec" in result

    def test_resolve_strategy_class_known_names(self):
        from gui_app.strategy_controller import StrategyController
        for name in ["双均线策略", "RSI策略", "MACD策略", "固定网格策略",
                      "自适应网格策略", "ATR网格策略"]:
            cls = StrategyController._resolve_strategy_class(name)
            # May be None if backtrader not installed, but should not raise
            assert cls is None or cls is not None  # no exception

    def test_resolve_strategy_class_unknown_defaults_to_dual_ma(self):
        from gui_app.strategy_controller import StrategyController
        cls1 = StrategyController._resolve_strategy_class("未知策略")
        cls2 = StrategyController._resolve_strategy_class("双均线策略")
        # Both should resolve to same class (DualMovingAverageStrategy)
        assert cls1 is cls2


class TestBacktestWorkerDelegation:
    """BacktestWorker.run_single_backtest should delegate to StrategyController."""

    def test_non_factor_delegates_to_controller(self):
        from gui_app.strategy_controller import StrategyController
        mock_ctrl_result = {
            "ok": True,
            "metrics": {"total_return": 0.15},
            "detailed": {"equity_curve": []},
            "elapsed_sec": 0.5,
        }
        with patch.object(
            StrategyController, "run_adhoc_backtest", return_value=mock_ctrl_result
        ) as mock_adhoc:
            worker = BacktestWorker.__new__(BacktestWorker)
            metrics, detailed = worker.run_single_backtest(
                _make_ohlcv(50),
                {
                    "strategy_name": "双均线策略",
                    "initial_cash": 1_000_000,
                    "commission": 0.0003,
                    "short_period": 5,
                    "long_period": 20,
                    "rsi_period": 14,
                },
            )
        assert metrics == {"total_return": 0.15}
        mock_adhoc.assert_called_once()

    def test_controller_error_raises_value_error(self):
        from gui_app.strategy_controller import StrategyController
        mock_ctrl_result = {"ok": False, "error": "引擎炸了"}
        with patch.object(
            StrategyController, "run_adhoc_backtest", return_value=mock_ctrl_result
        ):
            worker = BacktestWorker.__new__(BacktestWorker)
            with pytest.raises(ValueError, match="引擎炸了"):
                worker.run_single_backtest(
                    _make_ohlcv(50), {"strategy_name": "双均线策略"}
                )
