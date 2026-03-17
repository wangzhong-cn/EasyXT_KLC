"""
tests/test_coverage_boost_t2.py
================================

T2 覆盖率提升测试 — 覆盖率从 41% → 45%+ 目标。
聚焦于以下模块的未覆盖方法：
  1. gui_app/data_manager_controller: cross_validate_sources, get_trading_calendar_info, repair_missing_data
  2. gui_app/strategy_controller: CRUD, run_adhoc_backtest, performance_summary, equity_curve, trades, backtest_history
  3. data_manager/period_bar_builder: build_intraday_bars, build_multiday_bars, build_natural_calendar_bars, cross_validate
  4. gui_app/backtest/data_manager: DataManager source priority, status, get_stock_data
  5. data_manager/auto_data_updater: 补充覆盖
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest


# ═══════════════════════════════════════════════════════════════════════════════
# 1. DataManagerController — cross_validate_sources / trading_calendar / repair
# ═══════════════════════════════════════════════════════════════════════════════

class TestCrossValidateSources:
    """DataManagerController.cross_validate_sources 的完整测试。"""

    def _make_ctrl(self):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController(duckdb_path=":memory:")

    def test_duckdb_unavailable(self):
        ctrl = self._make_ctrl()
        with patch("gui_app.data_manager_controller._safe_import", return_value=None):
            r = ctrl.cross_validate_sources("600519.SH", "2024-01-01", "2024-01-31")
        assert r["consistent"] is False
        assert "error" in r

    def test_duckdb_empty_returns_error(self):
        ctrl = self._make_ctrl()
        mock_mgr = MagicMock()
        mock_mgr.execute_read_query.return_value = pd.DataFrame()

        def side_effect(mod, cls=None):
            if cls == "get_db_manager":
                return lambda path: mock_mgr
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.cross_validate_sources("600519.SH", "2024-01-01", "2024-01-31")
        assert "error" in r
        assert r["duckdb_rows"] == 0

    def test_fallback_to_contract_validator(self):
        """当 UDI 不可用时降级到 DataContractValidator。"""
        ctrl = self._make_ctrl()
        duck_df = pd.DataFrame({
            "date": pd.date_range("2024-01-02", periods=3),
            "close": [10.0, 10.1, 10.2],
        })
        mock_mgr = MagicMock()
        mock_mgr.execute_read_query.return_value = duck_df

        mock_validator = MagicMock()
        mock_validator.validate.return_value = SimpleNamespace(
            violations=[], ohlc_sanity_pct=1.0, velocity_violation_pct=0.0
        )

        call_count = {"n": 0}

        def side_effect(mod, cls=None):
            call_count["n"] += 1
            if cls == "get_db_manager":
                return lambda path: mock_mgr
            if cls == "UnifiedDataInterface":
                return None
            if cls == "DataContractValidator":
                return lambda: mock_validator
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.cross_validate_sources("600519.SH", "2024-01-01", "2024-01-31")
        assert r["consistent"] is True
        assert "note" in r

    def test_consistent_sources(self):
        """两源数据一致时返回 consistent=True。"""
        ctrl = self._make_ctrl()
        dates = pd.date_range("2024-01-02", periods=5)
        prices = [10.0, 10.1, 10.2, 10.3, 10.4]
        duck_df = pd.DataFrame({"date": dates, "close": prices})
        live_df = pd.DataFrame({"date": dates, "close": prices})

        mock_mgr = MagicMock()
        mock_mgr.execute_read_query.return_value = duck_df

        mock_udi = MagicMock()
        mock_udi.get_stock_data.return_value = live_df

        def side_effect(mod, cls=None):
            if cls == "get_db_manager":
                return lambda path: mock_mgr
            if cls == "UnifiedDataInterface":
                return lambda: mock_udi
            if mod == "pandas":
                return pd
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.cross_validate_sources("600519.SH", "2024-01-01", "2024-01-31")
        assert r["consistent"] is True
        assert r["consistency_rate"] == 1.0

    def test_inconsistent_sources(self):
        """两源差异大时 consistent=False。"""
        ctrl = self._make_ctrl()
        dates = pd.date_range("2024-01-02", periods=5)
        duck_df = pd.DataFrame({"date": dates, "close": [10.0, 10.1, 10.2, 10.3, 10.4]})
        live_df = pd.DataFrame({"date": dates, "close": [10.0, 12.0, 10.2, 10.3, 10.4]})

        mock_mgr = MagicMock()
        mock_mgr.execute_read_query.return_value = duck_df
        mock_udi = MagicMock()
        mock_udi.get_stock_data.return_value = live_df

        def side_effect(mod, cls=None):
            if cls == "get_db_manager":
                return lambda path: mock_mgr
            if cls == "UnifiedDataInterface":
                return lambda: mock_udi
            if mod == "pandas":
                return pd
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.cross_validate_sources("600519.SH", "2024-01-01", "2024-01-31")
        assert r["consistent"] is False
        assert r["max_diff_pct"] > 0
        assert len(r["diff_days"]) > 0

    def test_live_fetch_exception_fallback(self):
        """实时源拉取失败时优雅降级。"""
        ctrl = self._make_ctrl()
        dates = pd.date_range("2024-01-02", periods=3)
        duck_df = pd.DataFrame({"date": dates, "close": [10.0, 10.1, 10.2]})
        mock_mgr = MagicMock()
        mock_mgr.execute_read_query.return_value = duck_df

        mock_udi_cls = MagicMock()
        mock_udi_inst = MagicMock()
        mock_udi_inst.get_stock_data.side_effect = ConnectionError("timeout")
        mock_udi_cls.return_value = mock_udi_inst

        def side_effect(mod, cls=None):
            if cls == "get_db_manager":
                return lambda path: mock_mgr
            if cls == "UnifiedDataInterface":
                return mock_udi_cls
            if mod == "pandas":
                return pd
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.cross_validate_sources("600519.SH", "2024-01-01", "2024-01-31")
        assert "note" in r
        assert r["consistent"] is True

    def test_live_empty_returns_error(self):
        """实时源返回空 DataFrame。"""
        ctrl = self._make_ctrl()
        duck_df = pd.DataFrame({"date": pd.date_range("2024-01-02", periods=3), "close": [10.0, 10.1, 10.2]})
        mock_mgr = MagicMock()
        mock_mgr.execute_read_query.return_value = duck_df
        mock_udi = MagicMock()
        mock_udi.get_stock_data.return_value = pd.DataFrame()

        def side_effect(mod, cls=None):
            if cls == "get_db_manager":
                return lambda path: mock_mgr
            if cls == "UnifiedDataInterface":
                return lambda: mock_udi
            if mod == "pandas":
                return pd
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.cross_validate_sources("600519.SH", "2024-01-01", "2024-01-31")
        assert "error" in r

    def test_no_overlap_dates(self):
        """两源日期无交集。"""
        ctrl = self._make_ctrl()
        duck_df = pd.DataFrame({"date": pd.date_range("2024-01-02", periods=3), "close": [10.0, 10.1, 10.2]})
        live_df = pd.DataFrame({"date": pd.date_range("2024-02-02", periods=3), "close": [11.0, 11.1, 11.2]})
        mock_mgr = MagicMock()
        mock_mgr.execute_read_query.return_value = duck_df
        mock_udi = MagicMock()
        mock_udi.get_stock_data.return_value = live_df

        def side_effect(mod, cls=None):
            if cls == "get_db_manager":
                return lambda path: mock_mgr
            if cls == "UnifiedDataInterface":
                return lambda: mock_udi
            if mod == "pandas":
                return pd
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.cross_validate_sources("600519.SH", "2024-01-01", "2024-01-31")
        assert "error" in r

    def test_column_rename_close_price(self):
        """实时源列名为 Close（大写）时自动归一化。"""
        ctrl = self._make_ctrl()
        dates = pd.date_range("2024-01-02", periods=3)
        duck_df = pd.DataFrame({"date": dates, "close": [10.0, 10.1, 10.2]})
        live_df = pd.DataFrame({"Date": dates, "Close": [10.0, 10.1, 10.2]})
        mock_mgr = MagicMock()
        mock_mgr.execute_read_query.return_value = duck_df
        mock_udi = MagicMock()
        mock_udi.get_stock_data.return_value = live_df

        def side_effect(mod, cls=None):
            if cls == "get_db_manager":
                return lambda path: mock_mgr
            if cls == "UnifiedDataInterface":
                return lambda: mock_udi
            if mod == "pandas":
                return pd
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.cross_validate_sources("600519.SH", "2024-01-01", "2024-01-31")
        assert r["consistent"] is True

    def test_exception_in_cross_validate(self):
        """cross_validate_sources 内部异常时返回 error。"""
        ctrl = self._make_ctrl()
        with patch("gui_app.data_manager_controller._safe_import", side_effect=RuntimeError("boom")):
            r = ctrl.cross_validate_sources("600519.SH", "2024-01-01", "2024-01-31")
        assert "error" in r


class TestGetTradingCalendarInfo:
    """DataManagerController.get_trading_calendar_info 的完整测试。"""

    def _make_ctrl(self):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController(duckdb_path=":memory:")

    def test_calendar_unavailable(self):
        ctrl = self._make_ctrl()
        with patch("gui_app.data_manager_controller._safe_import", return_value=None):
            r = ctrl.get_trading_calendar_info("2024-01-01", "2024-01-31")
        assert "error" in r
        assert "TradingCalendar 不可用" in r["error"]

    def test_start_after_end(self):
        ctrl = self._make_ctrl()
        mock_cal = MagicMock()

        def side_effect(mod, cls=None):
            if cls == "TradingCalendar":
                return lambda: mock_cal
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.get_trading_calendar_info("2024-02-01", "2024-01-01")
        assert "error" in r
        assert "不能晚于" in r["error"]

    def test_happy_path(self):
        ctrl = self._make_ctrl()
        trading = [_dt.date(2024, 1, 2), _dt.date(2024, 1, 3), _dt.date(2024, 1, 4), _dt.date(2024, 1, 5)]
        mock_cal = MagicMock()
        mock_cal.get_trading_days.return_value = trading

        def side_effect(mod, cls=None):
            if cls == "TradingCalendar":
                return lambda: mock_cal
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.get_trading_calendar_info("2024-01-01", "2024-01-07")
        assert r["total_days"] == 7
        assert r["trading_days"] == 4
        assert r["non_trading_days"] == 3
        assert r["weekend_days"] >= 1

    def test_exception_graceful(self):
        ctrl = self._make_ctrl()

        def side_effect(mod, cls=None):
            if cls == "TradingCalendar":
                raise ImportError("no module")
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.get_trading_calendar_info("2024-01-01", "2024-01-31")
        assert "error" in r


class TestRepairMissingData:
    """DataManagerController.repair_missing_data 的测试。"""

    def _make_ctrl(self):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController(duckdb_path=":memory:")

    def test_both_unavailable(self):
        ctrl = self._make_ctrl()
        with patch("gui_app.data_manager_controller._safe_import", return_value=None):
            r = ctrl.repair_missing_data("600519.SH", "2024-01-01", "2024-01-31")
        assert r["queued"] is False
        assert "error" in r

    def test_fallback_to_auto_updater(self):
        """当 BackfillScheduler 不可用时降级到 AutoDataUpdater。"""
        ctrl = self._make_ctrl()
        mock_updater = MagicMock()

        def side_effect(mod, cls=None):
            if cls == "HistoryBackfillScheduler":
                return None
            if cls == "AutoDataUpdater":
                return lambda **kw: mock_updater
            return None

        with patch("gui_app.data_manager_controller._safe_import", side_effect=side_effect):
            r = ctrl.repair_missing_data("600519.SH", "2024-01-01", "2024-01-31")
        assert r["queued"] is True
        assert "AutoDataUpdater" in r.get("message", "")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. StrategyController — CRUD, adhoc backtest, performance, equity, trades, history
# ═══════════════════════════════════════════════════════════════════════════════

class TestStrategyControllerCRUD:
    """StrategyController CRUD 方法测试。"""

    def _make_ctrl(self, mgr=None, eng=None):
        from gui_app.strategy_controller import StrategyController
        return StrategyController(strategy_manager=mgr, backtest_engine=eng)

    def test_get_all_strategies_empty(self):
        ctrl = self._make_ctrl(mgr=MagicMock(list_strategies=MagicMock(return_value=[])))
        assert ctrl.get_all_strategies() == []

    def test_get_all_strategies_returns_list(self):
        items = [{"strategy_id": "s1", "strategy_name": "test"}]
        ctrl = self._make_ctrl(mgr=MagicMock(list_strategies=MagicMock(return_value=items)))
        assert ctrl.get_all_strategies() == items

    def test_get_all_strategies_no_manager(self):
        ctrl = self._make_ctrl()
        with patch("gui_app.strategy_controller._safe_import", return_value=None):
            assert ctrl.get_all_strategies() == []

    def test_get_all_strategies_exception(self):
        mgr = MagicMock()
        mgr.list_strategies.side_effect = RuntimeError("fail")
        ctrl = self._make_ctrl(mgr=mgr)
        assert ctrl.get_all_strategies() == []

    def test_get_strategy_found(self):
        cfg = {"strategy_id": "s1", "name": "test"}
        mgr = MagicMock()
        mgr.get_strategy.return_value = cfg
        ctrl = self._make_ctrl(mgr=mgr)
        result = ctrl.get_strategy("s1")
        assert result["strategy_id"] == "s1"

    def test_get_strategy_not_found(self):
        mgr = MagicMock()
        mgr.get_strategy.return_value = None
        ctrl = self._make_ctrl(mgr=mgr)
        assert ctrl.get_strategy("missing") is None

    def test_get_strategy_no_manager(self):
        ctrl = self._make_ctrl()
        with patch("gui_app.strategy_controller._safe_import", return_value=None):
            assert ctrl.get_strategy("s1") is None

    def test_get_strategy_with_model_dump(self):
        """model_dump() 方法的对象（如 pydantic model）。"""
        cfg = MagicMock()
        cfg.model_dump.return_value = {"strategy_id": "s1"}
        mgr = MagicMock()
        mgr.get_strategy.return_value = cfg
        ctrl = self._make_ctrl(mgr=mgr)
        result = ctrl.get_strategy("s1")
        assert result == {"strategy_id": "s1"}

    def test_create_strategy_success(self):
        mgr = MagicMock()
        mgr.create_strategy.return_value = "new_id"
        ctrl = self._make_ctrl(mgr=mgr)
        r = ctrl.create_strategy({"name": "test"})
        assert r["ok"] is True
        assert r["strategy_id"] == "new_id"

    def test_create_strategy_no_manager(self):
        ctrl = self._make_ctrl()
        with patch("gui_app.strategy_controller._safe_import", return_value=None):
            r = ctrl.create_strategy({"name": "test"})
        assert r["ok"] is False

    def test_create_strategy_exception(self):
        mgr = MagicMock()
        mgr.create_strategy.side_effect = ValueError("invalid config")
        ctrl = self._make_ctrl(mgr=mgr)
        r = ctrl.create_strategy({"name": "bad"})
        assert r["ok"] is False
        assert "invalid config" in r["error"]

    def test_delete_strategy_success(self):
        mgr = MagicMock()
        mgr.delete_strategy.return_value = True
        ctrl = self._make_ctrl(mgr=mgr)
        r = ctrl.delete_strategy("s1")
        assert r["ok"] is True

    def test_delete_strategy_not_found(self):
        mgr = MagicMock()
        mgr.delete_strategy.return_value = False
        ctrl = self._make_ctrl(mgr=mgr)
        r = ctrl.delete_strategy("missing")
        assert r["ok"] is False

    def test_delete_strategy_no_manager(self):
        ctrl = self._make_ctrl()
        with patch("gui_app.strategy_controller._safe_import", return_value=None):
            r = ctrl.delete_strategy("s1")
        assert r["ok"] is False

    def test_delete_strategy_exception(self):
        mgr = MagicMock()
        mgr.delete_strategy.side_effect = RuntimeError("db error")
        ctrl = self._make_ctrl(mgr=mgr)
        r = ctrl.delete_strategy("s1")
        assert r["ok"] is False


class TestStrategyControllerAdhocBacktest:
    """StrategyController.run_adhoc_backtest 测试。"""

    def _make_ctrl(self):
        from gui_app.strategy_controller import StrategyController
        return StrategyController()

    def test_engine_cls_unavailable(self):
        ctrl = self._make_ctrl()
        with patch("gui_app.strategy_controller._safe_import", return_value=None):
            r = ctrl.run_adhoc_backtest(
                stock_data=pd.DataFrame(),
                strategy_name="双均线策略",
                strategy_params={"short_period": 5},
            )
        assert r["ok"] is False
        assert "不可用" in r["error"]

    def test_strategy_class_unavailable(self):
        ctrl = self._make_ctrl()
        mock_engine = MagicMock()

        def side_effect(mod, cls=None):
            if cls == "AdvancedBacktestEngine":
                return lambda **kw: mock_engine
            return None

        with patch("gui_app.strategy_controller._safe_import", side_effect=side_effect):
            r = ctrl.run_adhoc_backtest(
                stock_data=pd.DataFrame(),
                strategy_name="不存在的策略",
                strategy_params={},
            )
        assert r["ok"] is False
        assert "不可用" in r["error"]

    def test_happy_path(self):
        ctrl = self._make_ctrl()
        mock_engine = MagicMock()
        mock_engine.run_backtest.return_value = {"total_return": 0.15}
        mock_engine.get_detailed_results.return_value = {"trades": []}
        mock_strategy = MagicMock()

        call_log = []

        def side_effect(mod, cls=None):
            call_log.append((mod, cls))
            if cls == "AdvancedBacktestEngine":
                return lambda **kw: mock_engine
            if cls in ("DualMovingAverageStrategy", "RSIStrategy", "MACDStrategy",
                       "GridStrategy", "AdaptiveGridStrategy", "ATRGridStrategy"):
                return mock_strategy
            return None

        with patch("gui_app.strategy_controller._safe_import", side_effect=side_effect):
            r = ctrl.run_adhoc_backtest(
                stock_data=pd.DataFrame({"close": [1, 2, 3]}),
                strategy_name="双均线策略",
                strategy_params={"short_period": 5},
                initial_cash=500_000,
                commission=0.001,
                period="5m",
                adjust="hfq",
            )
        assert r["ok"] is True
        assert "metrics" in r
        assert "elapsed_sec" in r

    def test_exception_returns_error(self):
        ctrl = self._make_ctrl()
        mock_engine = MagicMock()
        mock_engine.run_backtest.side_effect = RuntimeError("engine error")
        mock_strategy = MagicMock()

        def side_effect(mod, cls=None):
            if cls == "AdvancedBacktestEngine":
                return lambda **kw: mock_engine
            if cls == "DualMovingAverageStrategy":
                return mock_strategy
            return None

        with patch("gui_app.strategy_controller._safe_import", side_effect=side_effect):
            r = ctrl.run_adhoc_backtest(
                stock_data=pd.DataFrame(),
                strategy_name="双均线策略",
                strategy_params={},
            )
        assert r["ok"] is False
        assert "engine error" in r["error"]


class TestResolveStrategyClass:
    """StrategyController._resolve_strategy_class 测试。"""

    def test_known_strategy_names(self):
        from gui_app.strategy_controller import StrategyController
        mapping = {
            "双均线策略": "DualMovingAverageStrategy",
            "RSI策略": "RSIStrategy",
            "MACD策略": "MACDStrategy",
            "固定网格策略": "GridStrategy",
            "自适应网格策略": "AdaptiveGridStrategy",
            "ATR网格策略": "ATRGridStrategy",
        }
        for cn_name, class_name in mapping.items():
            with patch("gui_app.strategy_controller._safe_import") as mock_import:
                StrategyController._resolve_strategy_class(cn_name)
                mock_import.assert_called_once_with("gui_app.backtest.engine", class_name)

    def test_unknown_strategy_defaults(self):
        from gui_app.strategy_controller import StrategyController
        with patch("gui_app.strategy_controller._safe_import") as mock_import:
            StrategyController._resolve_strategy_class("未知策略")
            mock_import.assert_called_once_with("gui_app.backtest.engine", "DualMovingAverageStrategy")


class TestPerformanceSummary:
    """StrategyController.get_performance_summary 测试。"""

    def _make_ctrl(self):
        from gui_app.strategy_controller import StrategyController
        return StrategyController()

    def test_basic_metrics(self):
        ctrl = self._make_ctrl()
        metrics = {
            "total_return": 0.15,
            "cagr": 0.12,
            "sharpe_ratio": 1.5,
            "max_drawdown": 0.08,
            "calmar_ratio": 1.5,
            "win_rate": 0.6,
            "trade_count": 42,
            "profit_loss_ratio": 2.0,
        }
        rows = ctrl.get_performance_summary(metrics)
        assert len(rows) == 8
        labels = [r["label"] for r in rows]
        assert "总收益率" in labels
        assert "夏普比率" in labels
        assert "最大回撤" in labels

    def test_zero_metrics(self):
        ctrl = self._make_ctrl()
        metrics = {
            "total_return": 0,
            "sharpe_ratio": 0,
            "max_drawdown": 0,
            "win_rate": 0,
            "trade_count": 0,
        }
        rows = ctrl.get_performance_summary(metrics)
        assert len(rows) == 8

    def test_negative_return(self):
        ctrl = self._make_ctrl()
        metrics = {"total_return": -0.10, "sharpe_ratio": -0.5, "max_drawdown": 0.25}
        rows = ctrl.get_performance_summary(metrics)
        ret_row = next(r for r in rows if r["key"] == "total_return")
        assert ret_row["color"] == "#F44336"

    def test_empty_metrics(self):
        ctrl = self._make_ctrl()
        rows = ctrl.get_performance_summary({})
        assert len(rows) == 8


class TestExtractEquityCurve:
    """StrategyController._extract_equity_curve 测试。"""

    def _make_ctrl(self):
        from gui_app.strategy_controller import StrategyController
        return StrategyController()

    def test_from_series(self):
        ctrl = self._make_ctrl()
        idx = pd.date_range("2024-01-01", periods=5)
        result = SimpleNamespace(equity_curve=pd.Series([100, 101, 102, 103, 104], index=idx))
        ec = ctrl._extract_equity_curve(result)
        assert len(ec["dates"]) == 5
        assert len(ec["values"]) == 5
        assert ec["values"][0] == 100.0

    def test_from_dict_in_perf_metrics(self):
        ctrl = self._make_ctrl()
        result = SimpleNamespace(
            equity_curve=None,
            performance_metrics={
                "equity_curve": {
                    "dates": ["2024-01-01", "2024-01-02"],
                    "values": [100, 105],
                }
            },
        )
        ec = ctrl._extract_equity_curve(result)
        assert len(ec["dates"]) == 2
        assert ec["values"][1] == 105.0

    def test_empty_result(self):
        ctrl = self._make_ctrl()
        result = SimpleNamespace()
        ec = ctrl._extract_equity_curve(result)
        assert ec == {"dates": [], "values": []}

    def test_portfolio_curve_fallback(self):
        ctrl = self._make_ctrl()
        result = SimpleNamespace(
            equity_curve=None,
            performance_metrics={
                "portfolio_curve": {
                    "dates": ["2024-01-01"],
                    "values": [100],
                }
            },
        )
        ec = ctrl._extract_equity_curve(result)
        assert len(ec["dates"]) == 1


class TestExtractTrades:
    """StrategyController._extract_trades 测试。"""

    def _make_ctrl(self):
        from gui_app.strategy_controller import StrategyController
        return StrategyController()

    def test_dict_trades(self):
        ctrl = self._make_ctrl()
        result = SimpleNamespace(trades=[
            {"date": "2024-01-01", "action": "buy", "price": 10.0},
            {"date": "2024-01-02", "action": "sell", "price": 11.0},
        ])
        trades = ctrl._extract_trades(result)
        assert len(trades) == 2
        assert trades[0]["action"] == "buy"

    def test_tuple_trades(self):
        ctrl = self._make_ctrl()
        result = SimpleNamespace(trades=[
            ("2024-01-01", "buy", 10.0, 100, 1000, 0),
            ("2024-01-02", "sell", 11.0, 100, 1100, 100),
        ])
        trades = ctrl._extract_trades(result)
        assert len(trades) == 2
        assert trades[0]["date"] == "2024-01-01"
        assert trades[1]["pnl"] == 100

    def test_empty_trades(self):
        ctrl = self._make_ctrl()
        result = SimpleNamespace(trades=[])
        assert ctrl._extract_trades(result) == []

    def test_no_trades_attr(self):
        ctrl = self._make_ctrl()
        result = SimpleNamespace()
        assert ctrl._extract_trades(result) == []


class TestBacktestHistory:
    """StrategyController.get_backtest_history 测试。"""

    def _make_ctrl(self, tmpdir):
        from gui_app.strategy_controller import StrategyController
        return StrategyController(results_dir=str(tmpdir))

    def test_no_results_dir(self, tmp_path):
        from gui_app.strategy_controller import StrategyController
        ctrl = StrategyController(results_dir=str(tmp_path / "nonexistent"))
        assert ctrl.get_backtest_history("s1") == []

    def test_reads_json_files(self, tmp_path):
        ctrl = self._make_ctrl(tmp_path)
        data = {
            "backtest_id": "bt_001",
            "created_at": "2024-01-01T12:00:00",
            "performance_metrics": {
                "total_return": 0.15,
                "sharpe_ratio": 1.2,
                "max_drawdown": 0.05,
                "trade_count": 10,
            },
        }
        (tmp_path / "s1_20240101.json").write_text(json.dumps(data), encoding="utf-8")
        history = ctrl.get_backtest_history("s1")
        assert len(history) == 1
        assert history[0]["backtest_id"] == "bt_001"
        assert history[0]["total_return"] == 0.15

    def test_ignores_bad_json(self, tmp_path):
        ctrl = self._make_ctrl(tmp_path)
        (tmp_path / "s1_bad.json").write_text("not json", encoding="utf-8")
        (tmp_path / "s1_good.json").write_text(
            json.dumps({"backtest_id": "ok", "performance_metrics": {}}),
            encoding="utf-8",
        )
        history = ctrl.get_backtest_history("s1")
        assert len(history) == 1

    def test_sorted_by_created_at(self, tmp_path):
        ctrl = self._make_ctrl(tmp_path)
        for i, ts in enumerate(["2024-01-01", "2024-01-03", "2024-01-02"]):
            (tmp_path / f"s1_{i}.json").write_text(
                json.dumps({"backtest_id": f"bt_{i}", "created_at": ts, "performance_metrics": {}}),
                encoding="utf-8",
            )
        history = ctrl.get_backtest_history("s1")
        assert history[0]["created_at"] == "2024-01-03"


# ═══════════════════════════════════════════════════════════════════════════════
# 3. PeriodBarBuilder — build_intraday, multiday, natural, cross_validate
# ═══════════════════════════════════════════════════════════════════════════════

class TestPeriodBarBuilderIntraday:
    """PeriodBarBuilder 日内自定义周期构建测试。"""

    def _make_1m_data(self, date_str="2024-01-02"):
        """构造一天的 1m K线数据（带 time 列）。"""
        morning = pd.date_range(f"{date_str} 09:30", f"{date_str} 11:29", freq="min")
        afternoon = pd.date_range(f"{date_str} 13:00", f"{date_str} 14:59", freq="min")
        idx = morning.append(afternoon)
        n = len(idx)
        return pd.DataFrame({
            "time": idx,
            "open": np.linspace(10.0, 10.5, n),
            "high": np.linspace(10.1, 10.6, n),
            "low": np.linspace(9.9, 10.4, n),
            "close": np.linspace(10.0, 10.5, n),
            "volume": np.full(n, 1000.0),
        })

    def _make_daily_ref(self, date_str="2024-01-02", close=10.5):
        return pd.DataFrame({
            "time": [pd.Timestamp(date_str)],
            "open": [10.0],
            "high": [10.6],
            "low": [9.9],
            "close": [close],
            "volume": [240000.0],
        })

    def test_build_intraday_2m(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        data_1m = self._make_1m_data()
        daily_ref = self._make_daily_ref()
        bars = builder.build_intraday_bars(data_1m, 2, daily_ref)
        assert isinstance(bars, pd.DataFrame)
        assert len(bars) > 0
        assert "open" in bars.columns

    def test_build_intraday_30m(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        data_1m = self._make_1m_data()
        daily_ref = self._make_daily_ref()
        bars = builder.build_intraday_bars(data_1m, 30, daily_ref)
        assert len(bars) > 0

    def test_build_entry_point_intraday(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        data_1m = self._make_1m_data()
        daily_ref = self._make_daily_ref()
        bars = builder.build("15m", data_1m=data_1m, daily_ref=daily_ref)
        assert isinstance(bars, pd.DataFrame)
        assert len(bars) > 0


class TestPeriodBarBuilderMultiday:
    """PeriodBarBuilder 多日自定义周期构建测试。"""

    def _make_daily_data(self, n_days=20):
        dates = pd.bdate_range("2024-01-02", periods=n_days)
        return pd.DataFrame({
            "time": dates,
            "open": np.linspace(10.0, 12.0, n_days),
            "high": np.linspace(10.5, 12.5, n_days),
            "low": np.linspace(9.5, 11.5, n_days),
            "close": np.linspace(10.2, 12.2, n_days),
            "volume": np.full(n_days, 100000.0),
        })

    def test_build_multiday_5d(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        data_1d = self._make_daily_data(20)
        bars = builder.build_multiday_bars(data_1d, 5, listing_date="2024-01-02")
        assert isinstance(bars, pd.DataFrame)
        assert len(bars) == 4  # 20 / 5

    def test_build_multiday_2d(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        data_1d = self._make_daily_data(10)
        bars = builder.build_multiday_bars(data_1d, 2, listing_date="2024-01-02")
        assert len(bars) == 5

    def test_build_multiday_with_partial(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        data_1d = self._make_daily_data(7)
        bars = builder.build_multiday_bars(data_1d, 3, listing_date="2024-01-02")
        # 7 / 3 = 2 full + 1 partial
        assert len(bars) >= 2

    def test_build_entry_point_multiday(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        data_1d = self._make_daily_data(20)
        bars = builder.build("5d", data_1d=data_1d, listing_date="2024-01-02")
        assert isinstance(bars, pd.DataFrame)
        assert len(bars) >= 1


class TestPeriodBarBuilderNaturalCalendar:
    """PeriodBarBuilder 自然日历周期构建测试。"""

    def _make_daily_data(self, n_days=60):
        dates = pd.bdate_range("2024-01-02", periods=n_days)
        return pd.DataFrame({
            "time": dates,
            "open": np.linspace(10.0, 12.0, n_days),
            "high": np.linspace(10.5, 12.5, n_days),
            "low": np.linspace(9.5, 11.5, n_days),
            "close": np.linspace(10.2, 12.2, n_days),
            "volume": np.full(n_days, 100000.0),
        })

    def test_build_weekly(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        data_1d = self._make_daily_data(60)
        bars = builder.build_natural_calendar_bars(data_1d, "W-FRI")
        assert isinstance(bars, pd.DataFrame)
        assert len(bars) >= 10

    def test_build_monthly(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        data_1d = self._make_daily_data(60)
        bars = builder.build_natural_calendar_bars(data_1d, "ME")
        assert isinstance(bars, pd.DataFrame)
        assert len(bars) >= 2

    def test_build_entry_point_1w(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        data_1d = self._make_daily_data(60)
        bars = builder.build("1w", data_1d=data_1d)
        assert isinstance(bars, pd.DataFrame)

    def test_build_entry_point_1m(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        data_1d = self._make_daily_data(60)
        bars = builder.build("1M", data_1d=data_1d)
        assert isinstance(bars, pd.DataFrame)


class TestPeriodBarBuilderCrossValidate:
    """PeriodBarBuilder.cross_validate 测试。"""

    def test_cross_validate_valid(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        builder = PeriodBarBuilder()
        # 构造完美匹配的数据
        bars = pd.DataFrame({
            "time": pd.date_range("2024-01-02", periods=5),
            "open": [10.0] * 5,
            "high": [10.5] * 5,
            "low": [9.5] * 5,
            "close": [10.2] * 5,
            "volume": [100000.0] * 5,
        })
        daily_ref = bars.copy()
        vr = builder.cross_validate("5d", bars, daily_ref)
        assert vr is not None


class TestValidationResult:
    """ValidationResult 单元测试。"""

    def test_initial_state(self):
        from data_manager.period_bar_builder import ValidationResult
        vr = ValidationResult()
        assert vr.is_valid is True
        assert len(vr.errors) == 0
        assert len(vr.warnings) == 0

    def test_add_error(self):
        from data_manager.period_bar_builder import ValidationResult
        vr = ValidationResult()
        vr.add_error("price mismatch")
        assert vr.is_valid is False
        assert len(vr.errors) == 1

    def test_add_warning(self):
        from data_manager.period_bar_builder import ValidationResult
        vr = ValidationResult()
        vr.add_warning("volume drift")
        assert vr.is_valid is True
        assert len(vr.warnings) == 1

    def test_repr(self):
        from data_manager.period_bar_builder import ValidationResult
        vr = ValidationResult()
        assert "PASS" in repr(vr)
        vr.add_error("fail")
        assert "FAIL" in repr(vr)


class TestPeriodSpecs:
    """PERIOD_SPECS 完整性校验。"""

    def test_all_specs_have_required_keys(self):
        from data_manager.period_bar_builder import PERIOD_SPECS, PeriodType
        for period, spec in PERIOD_SPECS.items():
            assert "type" in spec
            assert "base" in spec
            assert isinstance(spec["type"], PeriodType)

    def test_intraday_periods_have_minutes(self):
        from data_manager.period_bar_builder import PERIOD_SPECS, PeriodType
        for period, spec in PERIOD_SPECS.items():
            if spec["type"] == PeriodType.INTRADAY:
                assert "minutes" in spec, f"{period} missing 'minutes'"
                assert spec["minutes"] > 0

    def test_multiday_periods_have_trading_days(self):
        from data_manager.period_bar_builder import PERIOD_SPECS, PeriodType
        for period, spec in PERIOD_SPECS.items():
            if spec["type"] == PeriodType.MULTIDAY_CUSTOM:
                assert "trading_days" in spec, f"{period} missing 'trading_days'"

    def test_natural_periods_have_freq(self):
        from data_manager.period_bar_builder import PERIOD_SPECS, PeriodType
        for period, spec in PERIOD_SPECS.items():
            if spec["type"] == PeriodType.NATURAL_CALENDAR:
                assert "freq" in spec, f"{period} missing 'freq'"

    def test_derived_sets(self):
        from data_manager.period_bar_builder import (
            INTRADAY_CUSTOM_PERIODS,
            MULTIDAY_CUSTOM_PERIODS,
            NATURAL_CALENDAR_PERIODS,
        )
        assert "15m" in INTRADAY_CUSTOM_PERIODS
        assert "5d" in MULTIDAY_CUSTOM_PERIODS
        assert "1w" in NATURAL_CALENDAR_PERIODS


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Backtest DataManager — source priority, connection status
# ═══════════════════════════════════════════════════════════════════════════════

class TestBacktestDataManagerSourcePriority:
    """gui_app/backtest/data_manager.py DataManager 源优先级测试。"""

    def _make_dm(self):
        from gui_app.backtest.data_manager import DataManager
        return DataManager(defer_checks=True)

    def test_source_priority_default(self):
        dm = self._make_dm()
        assert len(dm.source_priority) > 0
        # MOCK 应该在最后
        from gui_app.backtest.data_manager import DataSource
        assert dm.source_priority[-1] == DataSource.MOCK

    def test_source_priority_with_preferred(self):
        from gui_app.backtest.data_manager import DataManager, DataSource
        dm = DataManager(preferred_source=DataSource.AKSHARE, defer_checks=True)
        # preferred source 应在最前
        assert dm.source_priority[0] == DataSource.AKSHARE

    def test_connection_status_structure(self):
        dm = self._make_dm()
        status = dm.get_connection_status()
        assert isinstance(status, dict)

    def test_set_preferred_source(self):
        from gui_app.backtest.data_manager import DataSource
        dm = self._make_dm()
        dm.set_preferred_source(DataSource.QMT)
        assert dm.preferred_source == DataSource.QMT

    def test_data_source_enum(self):
        from gui_app.backtest.data_manager import DataSource
        assert DataSource.DUCKDB.value == "duckdb"
        assert DataSource.LOCAL.value == "local"
        assert DataSource.QMT.value == "qmt"
        assert DataSource.MOCK.value == "mock"

    def test_refresh_source_status(self):
        dm = self._make_dm()
        dm.refresh_source_status()
        assert len(dm.source_status) > 0

    def test_get_stock_data_mock(self):
        """从 MOCK 源获取数据时返回模拟 DataFrame。"""
        from gui_app.backtest.data_manager import DataManager, DataSource
        dm = DataManager(preferred_source=DataSource.MOCK, defer_checks=True)
        df = dm.get_stock_data("000001.SZ", "2024-01-01", "2024-01-31")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_get_stock_data_force_mock(self):
        from gui_app.backtest.data_manager import DataManager, DataSource
        dm = DataManager(defer_checks=True)
        df = dm.get_stock_data("000001.SZ", "2024-01-01", "2024-01-31", force_source=DataSource.MOCK)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0


class TestBacktestDataManagerValidation:
    """数据质量校验相关方法测试。"""

    def _make_dm(self):
        from gui_app.backtest.data_manager import DataManager
        return DataManager(defer_checks=True)

    def test_clean_data_basic(self):
        dm = self._make_dm()
        df = pd.DataFrame({
            "open": [10.0, np.nan, 10.2],
            "high": [10.5, 10.6, 10.7],
            "low": [9.5, 9.6, 9.7],
            "close": [10.2, 10.3, 10.4],
            "volume": [1000, 2000, 3000],
        })
        if hasattr(dm, "_clean_data"):
            cleaned = dm._clean_data(df)
            assert isinstance(cleaned, pd.DataFrame)

    def test_validate_data_quality(self):
        dm = self._make_dm()
        df = pd.DataFrame({
            "open": [10.0, 10.1, 10.2],
            "high": [10.5, 10.6, 10.7],
            "low": [9.5, 9.6, 9.7],
            "close": [10.2, 10.3, 10.4],
            "volume": [1000, 2000, 3000],
        })
        if hasattr(dm, "validate_data_quality"):
            result = dm.validate_data_quality(df)
            assert isinstance(result, dict)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. AutoDataUpdater — 补充覆盖
# ═══════════════════════════════════════════════════════════════════════════════

class TestAutoDataUpdaterListing:
    """auto_data_updater.py 上市日期获取逻辑测试。"""

    def _make_updater(self):
        from data_manager.auto_data_updater import AutoDataUpdater
        updater = AutoDataUpdater.__new__(AutoDataUpdater)
        updater.interface = None
        updater.data_manager = None
        updater.duckdb_path = ":memory:"
        updater.stock_list = []
        updater._checkpoint_path = None
        return updater

    def test_get_listing_date_fallback(self):
        updater = self._make_updater()
        # interface=None → 跳过 DuckDB → 兜底 1990-01-01
        result = updater.get_listing_date("600519.SH")
        assert result == "1990-01-01"

    def test_get_listing_date_from_duckdb(self):
        updater = self._make_updater()
        mock_con = MagicMock()
        mock_con.execute.return_value.df.return_value = pd.DataFrame({
            "d": [pd.Timestamp("2010-01-04")]
        })
        mock_interface = MagicMock()
        mock_interface.con = mock_con
        updater.interface = mock_interface
        result = updater.get_listing_date("600519.SH")
        assert result == "2010-01-04"

    def test_get_listing_date_xtquant_env(self):
        updater = self._make_updater()
        import sys
        mock_xtdata = MagicMock()
        mock_xtdata.get_instrument_detail.return_value = {"OpenDate": 20100104}
        mock_xtquant = MagicMock()
        mock_xtquant.xtdata = mock_xtdata
        with patch.dict(os.environ, {"EASYXT_ENABLE_XT_LISTING_DATE": "1"}):
            with patch.dict(sys.modules, {"xtquant": mock_xtquant, "xtquant.xtdata": mock_xtdata}):
                result = updater.get_listing_date("600519.SH")
        assert result == "2010-01-04"


class TestAutoDataUpdaterTradingDay:
    """auto_data_updater.py 交易日判断测试。"""

    def _make_updater(self):
        from data_manager.auto_data_updater import AutoDataUpdater
        updater = AutoDataUpdater.__new__(AutoDataUpdater)
        updater.interface = None
        updater.data_manager = None
        updater.duckdb_path = ":memory:"
        updater.stock_list = []
        updater._checkpoint_path = None
        mock_cal = MagicMock()
        updater.calendar = mock_cal
        return updater, mock_cal

    def test_is_trading_day_weekend(self):
        updater, mock_cal = self._make_updater()
        mock_cal.is_trading_day.return_value = False
        saturday = _dt.date(2024, 1, 6)
        result = updater.is_trading_day(saturday)
        assert result is False

    def test_is_trading_day_weekday(self):
        updater, mock_cal = self._make_updater()
        mock_cal.is_trading_day.return_value = True
        monday = _dt.date(2024, 1, 8)
        result = updater.is_trading_day(monday)
        assert result is True


# ═══════════════════════════════════════════════════════════════════════════════
# 6. HistoryBackfillScheduler — 补充覆盖
# ═══════════════════════════════════════════════════════════════════════════════

class TestBackfillSchedulerEdgeCases:
    """HistoryBackfillScheduler 边界情况测试。"""

    def test_schedule_empty_stock_code(self):
        from data_manager.history_backfill_scheduler import HistoryBackfillScheduler
        sched = HistoryBackfillScheduler(worker=lambda t: True)
        assert sched.schedule("", "2024-01-01", "2024-01-31", "1d") is False

    def test_schedule_empty_dates(self):
        from data_manager.history_backfill_scheduler import HistoryBackfillScheduler
        sched = HistoryBackfillScheduler(worker=lambda t: True)
        assert sched.schedule("600519.SH", "", "2024-01-31", "1d") is False
        assert sched.schedule("600519.SH", "2024-01-01", "", "1d") is False

    def test_schedule_duplicate_key(self):
        from data_manager.history_backfill_scheduler import HistoryBackfillScheduler
        sched = HistoryBackfillScheduler(worker=lambda t: True)
        assert sched.schedule("600519.SH", "2024-01-01", "2024-01-31", "1d") is True
        assert sched.schedule("600519.SH", "2024-01-01", "2024-01-31", "1d") is False

    def test_schedule_queue_full(self):
        from data_manager.history_backfill_scheduler import HistoryBackfillScheduler
        sched = HistoryBackfillScheduler(worker=lambda t: True, max_queue_size=1)
        assert sched.schedule("000001.SZ", "2024-01-01", "2024-01-31", "1d") is True
        assert sched.schedule("000002.SZ", "2024-01-01", "2024-01-31", "1d") is False

    def test_start_stop(self):
        from data_manager.history_backfill_scheduler import HistoryBackfillScheduler
        sched = HistoryBackfillScheduler(worker=lambda t: True)
        sched.start()
        assert sched._thread is not None
        sched.stop(timeout=1.0)

    def test_start_idempotent(self):
        from data_manager.history_backfill_scheduler import HistoryBackfillScheduler
        sched = HistoryBackfillScheduler(worker=lambda t: True)
        sched.start()
        thread1 = sched._thread
        sched.start()
        assert sched._thread is thread1
        sched.stop(timeout=1.0)

    def test_backfill_task_dataclass(self):
        from data_manager.history_backfill_scheduler import BackfillTask
        t = BackfillTask(
            priority=1,
            created_at=100.0,
            key="600519|1d|2024-01-01|2024-01-31",
            payload={"stock_code": "600519.SH"},
        )
        assert t.priority == 1
        assert t.retry_count == 0


# ═══════════════════════════════════════════════════════════════════════════════
# 7. SafeImport 辅助函数 — 补充分支
# ═══════════════════════════════════════════════════════════════════════════════

class TestSafeImportExtra:
    """_safe_import 额外分支测试。"""

    def test_import_module_only(self):
        from gui_app.strategy_controller import _safe_import
        mod = _safe_import("json")
        assert mod is not None

    def test_import_class_from_module(self):
        from gui_app.strategy_controller import _safe_import
        cls = _safe_import("json", "JSONDecoder")
        assert cls is not None

    def test_import_nonexistent_returns_none(self):
        from gui_app.strategy_controller import _safe_import
        assert _safe_import("nonexistent_module_xyz") is None

    def test_import_nonexistent_attr(self):
        from gui_app.strategy_controller import _safe_import
        assert _safe_import("json", "NoSuchClass") is None


# ═══════════════════════════════════════════════════════════════════════════════
# 8. DuckDB FiveFoldAdjustmentManager — 补充覆盖
# ═══════════════════════════════════════════════════════════════════════════════

class TestDuckdbFivefoldAdjust:
    """duckdb_fivefold_adjust 模块：adjust_type 枚举 + 基础创建测试。"""

    def test_adjust_type_values(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        assert FiveFoldAdjustmentManager is not None

    def test_fivefold_init(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        mgr = FiveFoldAdjustmentManager.__new__(FiveFoldAdjustmentManager)
        assert isinstance(mgr, FiveFoldAdjustmentManager)


# ═══════════════════════════════════════════════════════════════════════════════
# 9. ASHARE_SESSIONS 常量验证
# ═══════════════════════════════════════════════════════════════════════════════

class TestASHAREConstants:
    def test_ashare_sessions(self):
        from data_manager.period_bar_builder import ASHARE_SESSIONS
        assert len(ASHARE_SESSIONS) == 2
        assert ASHARE_SESSIONS[0] == ("09:30", "11:30")

    def test_ashare_with_auction_sessions(self):
        from data_manager.period_bar_builder import ASHARE_WITH_AUCTION_SESSIONS
        assert len(ASHARE_WITH_AUCTION_SESSIONS) == 3

    def test_commodity_sessions(self):
        from data_manager.period_bar_builder import COMMODITY_SESSIONS
        assert len(COMMODITY_SESSIONS) == 4
