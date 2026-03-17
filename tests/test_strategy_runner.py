"""Unit tests for easyxt_backtest.strategy_runner.StrategyRunner.

Covers:
  - Constructor parameter storage and defaults
  - _build_risk_engine: disabled / enable but import fails / enable success
  - _build_audit_trail: disabled / enable but import fails
  - run(): end-to-end with BacktestEngine fully mocked
"""
from __future__ import annotations

import types
from unittest.mock import MagicMock, patch

import pytest

from easyxt_backtest.engine import BacktestConfig, BacktestResult
from easyxt_backtest.strategy_runner import StrategyRunner


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_strategy(strategy_id: str = "test_strat") -> MagicMock:
    s = MagicMock()
    s.strategy_id = strategy_id
    return s


def _minimal_runner(**kwargs) -> StrategyRunner:
    defaults = dict(
        strategy=_make_strategy(),
        codes=["000001.SZ"],
        start_date="2023-01-01",
        end_date="2023-12-31",
    )
    defaults.update(kwargs)
    return StrategyRunner(**defaults)  # type: ignore[arg-type]


# ─────────────────────────────────────────────────────────────────────────────
# Constructor
# ─────────────────────────────────────────────────────────────────────────────

class TestConstructor:
    def test_required_params_stored(self):
        strat = _make_strategy("my_strat")
        runner = StrategyRunner(
            strategy=strat,
            codes=["600000.SH", "000001.SZ"],
            start_date="2022-01-01",
            end_date="2022-12-31",
        )
        assert runner.strategy is strat
        assert runner.codes == ["600000.SH", "000001.SZ"]
        assert runner.start_date == "2022-01-01"
        assert runner.end_date == "2022-12-31"

    def test_defaults(self):
        runner = _minimal_runner()
        assert runner.period == "1d"
        assert runner.adjust == "qfq"
        assert runner.duckdb_path is None
        assert runner.enable_risk_engine is True
        assert runner.enable_audit_trail is False

    def test_custom_config_stored(self):
        cfg = BacktestConfig(initial_capital=500_000.0)
        runner = _minimal_runner(config=cfg)
        assert runner.config is cfg

    def test_default_config_created_when_not_provided(self):
        runner = _minimal_runner(config=None)
        assert isinstance(runner.config, BacktestConfig)
        assert runner.config.initial_capital == 1_000_000.0

    def test_custom_options(self):
        runner = _minimal_runner(
            period="60m",
            adjust="hfq",
            duckdb_path="/tmp/test.db",
            enable_risk_engine=False,
            enable_audit_trail=True,
        )
        assert runner.period == "60m"
        assert runner.adjust == "hfq"
        assert runner.duckdb_path == "/tmp/test.db"
        assert runner.enable_risk_engine is False
        assert runner.enable_audit_trail is True


# ─────────────────────────────────────────────────────────────────────────────
# _build_risk_engine
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildRiskEngine:
    def test_disabled_returns_none(self):
        runner = _minimal_runner(enable_risk_engine=False)
        assert runner._build_risk_engine() is None

    def test_enabled_but_import_fails_returns_none(self):
        runner = _minimal_runner(enable_risk_engine=True)
        with patch.dict("sys.modules", {"core.risk_engine": None}):
            result = runner._build_risk_engine()
        assert result is None

    def test_enabled_returns_risk_engine_instance(self):
        runner = _minimal_runner(enable_risk_engine=True)
        mock_re = MagicMock()
        mock_module = types.SimpleNamespace(RiskEngine=lambda: mock_re)
        with patch.dict("sys.modules", {"core.risk_engine": mock_module}):
            result = runner._build_risk_engine()
        assert result is mock_re


# ─────────────────────────────────────────────────────────────────────────────
# _build_audit_trail
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildAuditTrail:
    def test_disabled_returns_none(self):
        runner = _minimal_runner(enable_audit_trail=False)
        assert runner._build_audit_trail() is None

    def test_enabled_but_import_fails_returns_none(self):
        runner = _minimal_runner(enable_audit_trail=True)
        with patch.dict("sys.modules", {"core.audit_trail": None}):
            result = runner._build_audit_trail()
        assert result is None

    def test_enabled_creates_and_calls_ensure_tables(self):
        runner = _minimal_runner(enable_audit_trail=True, duckdb_path="/tmp/audit.db")
        mock_at = MagicMock()
        mock_cls = MagicMock(return_value=mock_at)
        mock_module = types.SimpleNamespace(AuditTrail=mock_cls)
        with patch.dict("sys.modules", {"core.audit_trail": mock_module}):
            result = runner._build_audit_trail()
        mock_cls.assert_called_once_with(duckdb_path="/tmp/audit.db")
        mock_at.ensure_tables.assert_called_once()
        assert result is mock_at


# ─────────────────────────────────────────────────────────────────────────────
# run()
# ─────────────────────────────────────────────────────────────────────────────

class TestRun:
    def _make_result(self, **metrics) -> BacktestResult:
        base: dict = dict(
            total_return=0.15,
            sharpe=1.2,
            max_drawdown=0.08,
            trade_count=42,
        )
        base.update(metrics)
        import pandas as pd
        return BacktestResult(
            metrics=base,
            equity_curve=pd.Series(dtype=float),
            trades=pd.DataFrame(),
            final_equity=1_150_000.0,
            initial_capital=1_000_000.0,
            strategy_id="test_strat",
        )

    def test_run_returns_backtest_result(self):
        strat = _make_strategy()
        runner = _minimal_runner(
            strategy=strat,
            enable_risk_engine=False,
            enable_audit_trail=False,
        )
        expected_result = self._make_result()

        mock_engine = MagicMock()
        mock_engine.run.return_value = expected_result

        with patch(
            "easyxt_backtest.strategy_runner.BacktestEngine",
            return_value=mock_engine,
        ):
            result = runner.run()

        assert result is expected_result

    def test_run_passes_correct_args_to_engine(self):
        strat = _make_strategy("strat_x")
        runner = StrategyRunner(
            strategy=strat,
            codes=["000001.SZ", "600000.SH"],
            start_date="2022-01-01",
            end_date="2022-06-30",
            period="60m",
            adjust="hfq",
            enable_risk_engine=False,
            enable_audit_trail=False,
        )
        expected_result = self._make_result()
        mock_engine = MagicMock()
        mock_engine.run.return_value = expected_result

        with patch(
            "easyxt_backtest.strategy_runner.BacktestEngine",
            return_value=mock_engine,
        ):
            runner.run()

        mock_engine.run.assert_called_once_with(
            strategy=strat,
            codes=["000001.SZ", "600000.SH"],
            start_date="2022-01-01",
            end_date="2022-06-30",
            period="60m",
            adjust="hfq",
        )

    def test_risk_engine_passed_to_backtest_engine(self):
        runner = _minimal_runner(enable_risk_engine=True, enable_audit_trail=False)
        expected_result = self._make_result()
        mock_engine_instance = MagicMock()
        mock_engine_instance.run.return_value = expected_result

        mock_re = MagicMock()
        mock_module = types.SimpleNamespace(RiskEngine=lambda: mock_re)

        with patch.dict("sys.modules", {"core.risk_engine": mock_module}):
            with patch(
                "easyxt_backtest.strategy_runner.BacktestEngine",
                return_value=mock_engine_instance,
            ) as MockEngine:
                runner.run()

        call_kwargs = MockEngine.call_args.kwargs
        assert call_kwargs["risk_engine"] is mock_re

    def test_run_with_zero_trades_still_logs(self):
        """Exercises the log.info path with zero-valued metrics."""
        runner = _minimal_runner(enable_risk_engine=False, enable_audit_trail=False)
        result = self._make_result(
            total_return=0.0, sharpe=0.0, max_drawdown=0.0, trade_count=0
        )
        mock_engine = MagicMock()
        mock_engine.run.return_value = result

        with patch(
            "easyxt_backtest.strategy_runner.BacktestEngine",
            return_value=mock_engine,
        ):
            out = runner.run()

        assert out.metrics["trade_count"] == 0
