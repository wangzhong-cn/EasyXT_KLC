"""T2e coverage boost — 覆盖 DataManagerController (14 methods) 和
BacktestEngine 纯计算方法 + BacktestDataManager mock/标准化。
目标 +486 行覆盖以突破 45%。
"""
import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
from collections import defaultdict

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ===================================================================
# 1. DataManagerController — validate_environment
# ===================================================================
class TestValidateEnvironment(unittest.TestCase):
    def _make_ctrl(self, **kw):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController(duckdb_path=":memory:", **kw)

    def test_missing_required(self):
        ctrl = self._make_ctrl()
        with patch.dict(os.environ, {}, clear=True):
            result = ctrl.validate_environment()
        assert result["valid"] is False
        assert any(i["key"] == "EASYXT_DUCKDB_PATH" and i["status"] == "missing" for i in result["items"])

    def test_present_but_invalid_path(self):
        ctrl = self._make_ctrl()
        with patch.dict(os.environ, {"EASYXT_DUCKDB_PATH": "/nonexistent/path.ddb"}, clear=True):
            result = ctrl.validate_environment()
        assert any(i["key"] == "EASYXT_DUCKDB_PATH" and i["status"] == "invalid" for i in result["items"])

    def test_all_valid(self):
        ctrl = self._make_ctrl()
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "test.ddb")
            Path(db_path).touch()
            with patch.dict(os.environ, {"EASYXT_DUCKDB_PATH": db_path}, clear=True):
                result = ctrl.validate_environment()
            assert result["valid"] is True

    def test_optional_missing_still_valid(self):
        ctrl = self._make_ctrl()
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "test.ddb")
            Path(db_path).touch()
            with patch.dict(os.environ, {"EASYXT_DUCKDB_PATH": db_path}, clear=True):
                result = ctrl.validate_environment()
            log_item = [i for i in result["items"] if i["key"] == "EASYXT_LOG_DIR"]
            assert log_item[0]["status"] == "missing"
            assert result["valid"] is True  # optional keys don't invalidate


# ===================================================================
# 2. DataManagerController — get_all_env_config
# ===================================================================
class TestGetAllEnvConfig(unittest.TestCase):
    def _make_ctrl(self):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController(duckdb_path=":memory:")

    def test_returns_groups(self):
        ctrl = self._make_ctrl()
        with patch.dict(os.environ, {}, clear=True):
            result = ctrl.get_all_env_config()
        assert "groups" in result
        assert "summary" in result
        assert result["summary"]["total"] > 0

    def test_sensitive_masked(self):
        ctrl = self._make_ctrl()
        with patch.dict(os.environ, {"EASYXT_TUSHARE_TOKEN": "super_secret_token_12345"}, clear=True):
            result = ctrl.get_all_env_config()
        for items in result["groups"].values():
            for item in items:
                if item["key"] == "EASYXT_TUSHARE_TOKEN":
                    assert "***MASKED***" in item["value"]

    def test_path_validation(self):
        ctrl = self._make_ctrl()
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "test.ddb")
            Path(db_path).touch()
            with patch.dict(os.environ, {"EASYXT_DUCKDB_PATH": db_path}, clear=True):
                result = ctrl.get_all_env_config()
            for items in result["groups"].values():
                for item in items:
                    if item["key"] == "EASYXT_DUCKDB_PATH":
                        assert item["status"] == "ok"

    def test_overall_valid_when_required_present(self):
        ctrl = self._make_ctrl()
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "test.ddb")
            Path(db_path).touch()
            with patch.dict(os.environ, {"EASYXT_DUCKDB_PATH": db_path}, clear=True):
                result = ctrl.get_all_env_config()
            assert result["overall_valid"] is True


# ===================================================================
# 3. DataManagerController — get_pipeline_status
# ===================================================================
class TestGetPipelineStatus(unittest.TestCase):
    def test_unavailable(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:", pipeline_health=None)
        with patch.object(ctrl, "_get_pipeline_health", return_value=None):
            result = ctrl.get_pipeline_status()
        assert result["overall_healthy"] is False
        assert "不可用" in result.get("error", "")

    def test_with_mock_health(self):
        mock_health = MagicMock()
        mock_health.report.return_value = {"overall_healthy": True, "checks": {}}
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:", pipeline_health=mock_health)
        result = ctrl.get_pipeline_status()
        assert result["overall_healthy"] is True

    def test_report_exception(self):
        mock_health = MagicMock()
        mock_health.report.side_effect = RuntimeError("boom")
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:", pipeline_health=mock_health)
        result = ctrl.get_pipeline_status()
        assert result["overall_healthy"] is False
        assert "boom" in result.get("error", "")


# ===================================================================
# 4. DataManagerController — run_integrity_check
# ===================================================================
class TestRunIntegrityCheck(unittest.TestCase):
    def test_checker_unavailable(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:", integrity_checker=None)
        with patch.object(ctrl, "_get_integrity_checker", return_value=None):
            result = ctrl.run_integrity_check("600519.SH", "2024-01-01", "2024-03-01")
        assert result["passed"] is False
        assert "不可用" in result.get("error", "")

    def test_passes(self):
        mock_checker = MagicMock()
        mock_checker.check_integrity.return_value = {
            "has_errors": False, "errors": [], "warnings": [], "summary": {"ok": True}
        }
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:", integrity_checker=mock_checker)
        result = ctrl.run_integrity_check("600519.SH", "2024-01-01", "2024-03-01")
        assert result["passed"] is True
        assert result["elapsed_ms"] >= 0

    def test_fails_with_errors(self):
        mock_checker = MagicMock()
        mock_checker.check_integrity.return_value = {
            "has_errors": True, "errors": ["gap detected"], "warnings": ["drift"], "summary": {}
        }
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:", integrity_checker=mock_checker)
        result = ctrl.run_integrity_check("600519.SH", "2024-01-01", "2024-03-01")
        assert result["passed"] is False
        assert "gap detected" in result["errors"]

    def test_exception_handling(self):
        mock_checker = MagicMock()
        mock_checker.check_integrity.side_effect = RuntimeError("db error")
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:", integrity_checker=mock_checker)
        result = ctrl.run_integrity_check("600519.SH", "2024-01-01", "2024-03-01")
        assert result["passed"] is False
        assert "db error" in result.get("error", "")


# ===================================================================
# 5. DataManagerController — run_batch_integrity_check
# ===================================================================
class TestRunBatchIntegrityCheck(unittest.TestCase):
    def test_checker_unavailable(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:")
        with patch.object(ctrl, "_get_integrity_checker", return_value=None):
            result = ctrl.run_batch_integrity_check(["A", "B"], "2024-01-01", "2024-03-01")
        assert result["passed"] == 0
        assert result["failed"] == 2

    def test_mixed_results(self):
        mock_checker = MagicMock()
        mock_checker.batch_check_integrity.return_value = {
            "A": {"has_errors": False},
            "B": {"has_errors": True},
        }
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:", integrity_checker=mock_checker)
        result = ctrl.run_batch_integrity_check(["A", "B"], "2024-01-01", "2024-03-01")
        assert result["passed"] == 1
        assert result["failed"] == 1


# ===================================================================
# 6. DataManagerController — get_routing_metrics
# ===================================================================
class TestGetRoutingMetrics(unittest.TestCase):
    def test_registry_unavailable(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:")
        with patch.object(ctrl, "_get_datasource_registry", return_value=None):
            result = ctrl.get_routing_metrics()
        assert result["total_sources"] == 0
        assert "不可用" in result.get("error", "")

    def test_with_registry(self):
        reg = MagicMock()
        reg.get_metrics.return_value = {
            "duckdb": {"hits": 100, "misses": 5, "errors": 0}
        }
        reg.get_health_summary.return_value = {
            "duckdb": {"available": True}
        }
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:", datasource_registry=reg)
        result = ctrl.get_routing_metrics()
        assert result["total_sources"] == 1
        assert result["healthy_sources"] == 1
        assert result["sources"]["duckdb"]["hits"] == 100


# ===================================================================
# 7. DataManagerController — save_env_to_dotenv
# ===================================================================
class TestSaveEnvToDotenv(unittest.TestCase):
    def _make_ctrl(self):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController(duckdb_path=":memory:")

    def test_whitelist_rejected(self):
        ctrl = self._make_ctrl()
        result = ctrl.save_env_to_dotenv("DANGEROUS_KEY", "value")
        assert result["ok"] is False
        assert "白名单" in result.get("error", "")

    def test_newline_rejected(self):
        ctrl = self._make_ctrl()
        result = ctrl.save_env_to_dotenv("EASYXT_DUCKDB_PATH", "val\nue")
        assert result["ok"] is False
        assert "非法字符" in result.get("error", "")

    def test_writes_new_key(self):
        ctrl = self._make_ctrl()
        with tempfile.TemporaryDirectory() as td:
            dotenv = os.path.join(td, ".env")
            result = ctrl.save_env_to_dotenv("EASYXT_DUCKDB_PATH", "/tmp/test.ddb", dotenv)
            assert result["ok"] is True
            content = Path(dotenv).read_text()
            assert 'EASYXT_DUCKDB_PATH="/tmp/test.ddb"' in content

    def test_updates_existing_key(self):
        ctrl = self._make_ctrl()
        with tempfile.TemporaryDirectory() as td:
            dotenv = os.path.join(td, ".env")
            Path(dotenv).write_text('EASYXT_DUCKDB_PATH="/old/path"\n')
            result = ctrl.save_env_to_dotenv("EASYXT_DUCKDB_PATH", "/new/path", dotenv)
            assert result["ok"] is True
            content = Path(dotenv).read_text()
            assert "/new/path" in content
            assert "/old/path" not in content

    def test_syncs_os_environ(self):
        ctrl = self._make_ctrl()
        with tempfile.TemporaryDirectory() as td:
            dotenv = os.path.join(td, ".env")
            ctrl.save_env_to_dotenv("EASYXT_LOG_DIR", "/tmp/logs", dotenv)
            assert os.environ.get("EASYXT_LOG_DIR") == "/tmp/logs"
        # Clean up
        os.environ.pop("EASYXT_LOG_DIR", None)


# ===================================================================
# 8. DataManagerController — dead letter ops
# ===================================================================
class TestDeadLetterOps(unittest.TestCase):
    def _make_ctrl(self):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController(duckdb_path=":memory:")

    def test_resolve_dead_letter_path(self):
        ctrl = self._make_ctrl()
        path = ctrl._resolve_dead_letter_path()
        assert path.endswith(".jsonl")

    def test_resolve_dead_letter_path_env(self):
        ctrl = self._make_ctrl()
        with patch.dict(os.environ, {"EASYXT_DEAD_LETTER_PATH": "/tmp/dl.jsonl"}):
            path = ctrl._resolve_dead_letter_path()
        assert path == "/tmp/dl.jsonl"

    def test_get_empty(self):
        ctrl = self._make_ctrl()
        with patch.object(ctrl, "_resolve_dead_letter_path", return_value="/nonexistent/dl.jsonl"):
            result = ctrl.get_backfill_dead_letter()
        assert result["total"] == 0
        assert result["entries"] == []

    def test_get_with_entries(self):
        ctrl = self._make_ctrl()
        with tempfile.TemporaryDirectory() as td:
            dl_path = os.path.join(td, "dl.jsonl")
            record = {
                "key": "X|1d|2024-01-01|2024-01-10",
                "payload": {"stock_code": "X", "start_date": "2024-01-01",
                             "end_date": "2024-01-10", "period": "1d"},
                "retry_count": 3, "reason": "max_retries",
                "failed_at": "2024-01-01T00:00:00Z"
            }
            Path(dl_path).write_text(json.dumps(record) + "\n")
            with patch.object(ctrl, "_resolve_dead_letter_path", return_value=dl_path):
                result = ctrl.get_backfill_dead_letter()
            assert result["total"] == 1
            assert result["entries"][0]["stock_code"] == "X"

    def test_clear(self):
        ctrl = self._make_ctrl()
        with tempfile.TemporaryDirectory() as td:
            dl_path = os.path.join(td, "dl.jsonl")
            Path(dl_path).write_text('{"key":"test"}\n')
            with patch.object(ctrl, "_resolve_dead_letter_path", return_value=dl_path):
                result = ctrl.clear_backfill_dead_letter()
            assert result["ok"] is True
            assert not os.path.exists(dl_path)

    def test_clear_nonexistent(self):
        ctrl = self._make_ctrl()
        with patch.object(ctrl, "_resolve_dead_letter_path", return_value="/nonexistent/dl.jsonl"):
            result = ctrl.clear_backfill_dead_letter()
        assert result["ok"] is True


# ===================================================================
# 9. DataManagerController — get_duckdb_summary
# ===================================================================
class TestGetDuckdbSummary(unittest.TestCase):
    def test_pool_unavailable(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:")
        with patch("gui_app.data_manager_controller._safe_import", return_value=None):
            result = ctrl.get_duckdb_summary()
        assert result["healthy"] is False

    def test_with_mock_mgr(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:")
        mock_get_db = MagicMock()
        mock_mgr = MagicMock()
        mock_get_db.return_value = mock_mgr
        # Tables query
        mock_mgr.execute_read_query.side_effect = [
            pd.DataFrame({"table_name": ["stock_daily"]}),  # tables
            pd.DataFrame({"cnt": [1000], "latest": ["2024-03-15"]}),  # daily stats
        ]
        with patch("gui_app.data_manager_controller._safe_import", return_value=mock_get_db):
            result = ctrl.get_duckdb_summary()
        assert result["healthy"] is True
        assert result["stock_daily_rows"] == 1000


# ===================================================================
# 10. DataManagerController — get_duckdb_maintenance_info
# ===================================================================
class TestGetDuckdbMaintenanceInfo(unittest.TestCase):
    def test_pool_unavailable(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:")
        with patch("gui_app.data_manager_controller._safe_import", return_value=None):
            result = ctrl.get_duckdb_maintenance_info()
        assert result["healthy"] is False

    def test_with_tables(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:")
        mock_get_db = MagicMock()
        mock_mgr = MagicMock()
        mock_get_db.return_value = mock_mgr
        calls = [
            pd.DataFrame({"table_name": ["stock_daily"]}),  # table list
            pd.DataFrame({"n": [500]}),  # count
            pd.DataFrame({"column_name": ["date", "close", "volume"]}),  # columns
            pd.DataFrame({"ld": ["2024-03-15"]}),  # latest date
        ]
        mock_mgr.execute_read_query.side_effect = calls
        with patch("gui_app.data_manager_controller._safe_import", return_value=mock_get_db):
            result = ctrl.get_duckdb_maintenance_info()
        assert result["healthy"] is True
        assert len(result["tables"]) == 1
        assert result["tables"][0]["rows"] == 500


# ===================================================================
# 11. DataManagerController — run_checkpoint
# ===================================================================
class TestRunCheckpoint(unittest.TestCase):
    def test_unavailable(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:")
        with patch("gui_app.data_manager_controller._safe_import", return_value=None):
            result = ctrl.run_checkpoint()
        assert result["ok"] is False

    def test_success(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=":memory:")
        mock_get_db = MagicMock()
        mock_mgr = MagicMock()
        mock_get_db.return_value = mock_mgr
        mock_mgr.execute_read_query.return_value = None
        with patch("gui_app.data_manager_controller._safe_import", return_value=mock_get_db):
            result = ctrl.run_checkpoint()
        assert result["ok"] is True
        assert result["elapsed_ms"] >= 0


# ===================================================================
# 12. DataManagerController — test_datasource_connectivity
# ===================================================================
class TestDatasourceConnectivity(unittest.TestCase):
    def _make_ctrl(self):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController(duckdb_path=":memory:")

    def test_duckdb_reachable(self):
        ctrl = self._make_ctrl()
        mock_get_db = MagicMock()
        mock_mgr = MagicMock()
        mock_get_db.return_value = mock_mgr
        with patch("gui_app.data_manager_controller._safe_import", return_value=mock_get_db):
            result = ctrl.test_datasource_connectivity("duckdb")
        assert result["reachable"] is True
        assert result["latency_ms"] >= 0

    def test_tushare_no_token(self):
        ctrl = self._make_ctrl()
        with patch.dict(os.environ, {}, clear=True):
            result = ctrl.test_datasource_connectivity("tushare")
        assert result["reachable"] is False
        assert "TUSHARE_TOKEN" in result.get("error", "")

    def test_unknown_source_no_registry(self):
        ctrl = self._make_ctrl()
        with patch.object(ctrl, "_get_datasource_registry", return_value=None):
            result = ctrl.test_datasource_connectivity("unknown_source")
        assert result["reachable"] is False


# ===================================================================
# 13. BacktestEngine — get_backtrader_import_status
# ===================================================================
class TestGetBacktraderImportStatus(unittest.TestCase):
    def test_returns_dict(self):
        from gui_app.backtest.engine import get_backtrader_import_status
        result = get_backtrader_import_status()
        assert isinstance(result, dict)
        assert "available" in result
        assert "mode" in result
        assert "hint" in result


# ===================================================================
# 14. BacktestEngine — _compute_bars_per_year
# ===================================================================
class TestComputeBarsPerYear(unittest.TestCase):
    def _make_engine(self):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        return AdvancedBacktestEngine(initial_cash=100000)

    def test_daily_data(self):
        e = self._make_engine()
        idx = pd.date_range("2024-01-01", periods=100, freq="D")
        close = pd.Series(np.random.randn(100) + 100, index=idx)
        result = e._compute_bars_per_year(close)
        assert 200 <= result <= 300  # ~252

    def test_minute_data(self):
        e = self._make_engine()
        idx = pd.date_range("2024-01-01 09:30", periods=100, freq="1min")
        close = pd.Series(np.random.randn(100) + 100, index=idx)
        result = e._compute_bars_per_year(close)
        assert result > 252

    def test_short_series_default(self):
        e = self._make_engine()
        idx = pd.date_range("2024-01-01", periods=2, freq="D")
        close = pd.Series([100, 101], index=idx)
        result = e._compute_bars_per_year(close)
        assert result == 252


# ===================================================================
# 15. BacktestEngine — _compute_rsi
# ===================================================================
class TestComputeRsi(unittest.TestCase):
    def _make_engine(self):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        return AdvancedBacktestEngine(initial_cash=100000)

    def test_rsi_range(self):
        e = self._make_engine()
        close = pd.Series(np.random.randn(50).cumsum() + 100)
        rsi = e._compute_rsi(close, 14)
        assert rsi.min() >= 0
        assert rsi.max() <= 100

    def test_rsi_length(self):
        e = self._make_engine()
        close = pd.Series(np.random.randn(30).cumsum() + 100)
        rsi = e._compute_rsi(close, 14)
        assert len(rsi) == 30


# ===================================================================
# 16. BacktestEngine — _compute_mock_strategy_returns
# ===================================================================
class TestComputeMockStrategyReturns(unittest.TestCase):
    def _make_engine(self, strategy="DualMovingAverageStrategy"):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        e = AdvancedBacktestEngine(initial_cash=100000)
        e.strategy_name = strategy
        e.strategy_params = {}
        return e

    def test_dual_ma(self):
        e = self._make_engine("DualMovingAverageStrategy")
        close = pd.Series(np.random.randn(100).cumsum() + 100)
        returns, position = e._compute_mock_strategy_returns(close)
        assert len(returns) == 100
        assert len(position) == 100

    def test_rsi_strategy(self):
        e = self._make_engine("RSIStrategy")
        close = pd.Series(np.random.randn(100).cumsum() + 100)
        returns, position = e._compute_mock_strategy_returns(close)
        assert len(returns) == 100

    def test_macd_strategy(self):
        e = self._make_engine("MACDStrategy")
        close = pd.Series(np.random.randn(100).cumsum() + 100)
        returns, position = e._compute_mock_strategy_returns(close)
        assert len(returns) == 100


# ===================================================================
# 17. BacktestEngine — _compute_mock_curve + _compute_mock_metrics
# ===================================================================
class TestComputeMockCurveAndMetrics(unittest.TestCase):
    def _make_engine(self):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        e = AdvancedBacktestEngine(initial_cash=100000)
        e.strategy_name = "DualMovingAverageStrategy"
        e.strategy_params = {}
        idx = pd.date_range("2024-01-01", periods=100, freq="D")
        e.mock_data = pd.DataFrame({
            "open": np.random.randn(100).cumsum() + 100,
            "high": np.random.randn(100).cumsum() + 102,
            "low": np.random.randn(100).cumsum() + 98,
            "close": np.random.randn(100).cumsum() + 100,
            "volume": np.random.randint(1000, 10000, 100),
        }, index=idx)
        return e

    def test_curve(self):
        e = self._make_engine()
        curve = e._compute_mock_curve()
        assert isinstance(curve, list)
        assert len(curve) > 0

    def test_metrics(self):
        e = self._make_engine()
        e.native_result = None  # Force fallback to inline calculation
        with patch("gui_app.backtest.engine.NATIVE_ENGINE_AVAILABLE", False):
            m = e._compute_mock_metrics()
        assert "sharpe_ratio" in m
        assert "max_drawdown" in m
        assert "total_return" in m
        assert "annualized_return" in m

    def test_metrics_empty_data(self):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        e = AdvancedBacktestEngine(initial_cash=100000)
        e.mock_data = pd.DataFrame()
        e.strategy_name = "X"
        e.strategy_params = {}
        e.native_result = None
        with patch("gui_app.backtest.engine.NATIVE_ENGINE_AVAILABLE", False):
            m = e._compute_mock_metrics()
        assert m["total_return"] == 0


# ===================================================================
# 18. BacktestEngine — _calculate_profit_factor
# ===================================================================
class TestCalculateProfitFactor(unittest.TestCase):
    def _make_engine(self):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        return AdvancedBacktestEngine(initial_cash=100000)

    def test_normal(self):
        e = self._make_engine()
        ta = {"won": {"pnl": {"total": 1000}}, "lost": {"pnl": {"total": -500}}}
        pf = e._calculate_profit_factor(ta)
        assert pf == 2.0

    def test_no_losses(self):
        e = self._make_engine()
        ta = {"won": {"pnl": {"total": 1000}}, "lost": {"pnl": {"total": 0}}}
        pf = e._calculate_profit_factor(ta)
        assert pf == float("inf")

    def test_no_profit_no_loss(self):
        e = self._make_engine()
        ta = {"won": {"pnl": {"total": 0}}, "lost": {"pnl": {"total": 0}}}
        pf = e._calculate_profit_factor(ta)
        assert pf == 0

    def test_empty_dict(self):
        e = self._make_engine()
        pf = e._calculate_profit_factor({})
        assert pf >= 0


# ===================================================================
# 19. BacktestEngine — _generate_param_combinations
# ===================================================================
class TestGenerateParamCombinations(unittest.TestCase):
    def _make_engine(self):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        return AdvancedBacktestEngine(initial_cash=100000)

    def test_basic(self):
        e = self._make_engine()
        params = {"a": [1, 2], "b": [3, 4]}
        combos = e._generate_param_combinations(params)
        assert len(combos) == 4
        assert {"a": 1, "b": 3} in combos

    def test_empty(self):
        e = self._make_engine()
        combos = e._generate_param_combinations({})
        assert len(combos) == 1  # single empty dict


# ===================================================================
# 20. BacktestEngine — _generate_mock_portfolio_curve
# ===================================================================
class TestGenerateMockPortfolioCurve(unittest.TestCase):
    def test_returns_list(self):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        e = AdvancedBacktestEngine(initial_cash=100000)
        curve = e._generate_mock_portfolio_curve()
        assert isinstance(curve, list)
        assert curve[0] == 100000
        assert len(curve) == 253  # 1 initial + 252 days


# ===================================================================
# 21. BacktestEngine — _get_backtest_period + _get_strategy_info
# ===================================================================
class TestBacktestPeriodAndStrategyInfo(unittest.TestCase):
    def _make_engine(self):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        e = AdvancedBacktestEngine(initial_cash=100000)
        e.backtest_start_date = datetime(2024, 1, 1)
        e.backtest_end_date = datetime(2024, 6, 30)
        return e

    def test_period(self):
        e = self._make_engine()
        p = e._get_backtest_period()
        assert p["start_date"] == "2024-01-01"
        assert p["end_date"] == "2024-06-30"
        assert int(p["total_days"]) > 0

    def test_period_none_dates(self):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        e = AdvancedBacktestEngine(initial_cash=100000)
        p = e._get_backtest_period()
        # Should still return valid dates
        assert "start_date" in p

    def test_strategy_info(self):
        e = self._make_engine()
        info = e._get_strategy_info()
        assert "strategy_name" in info
        assert "parameters" in info


# ===================================================================
# 22. BacktestEngine — _build_daily_holdings
# ===================================================================
class TestBuildDailyHoldings(unittest.TestCase):
    def _make_engine(self):
        from gui_app.backtest.engine import AdvancedBacktestEngine
        e = AdvancedBacktestEngine(initial_cash=100000)
        idx = pd.date_range("2024-01-01", periods=5, freq="D")
        e.mock_data = pd.DataFrame({
            "close": [100, 101, 102, 103, 104],
        }, index=idx)
        e.dataframe_data = None
        return e

    def test_no_trades(self):
        e = self._make_engine()
        dates = list(pd.date_range("2024-01-01", periods=5, freq="D"))
        holdings = e._build_daily_holdings(dates, [])
        assert len(holdings) == 5
        assert all(h["position"] == 0 for h in holdings)

    def test_with_trades(self):
        e = self._make_engine()
        dates = list(pd.date_range("2024-01-01", periods=5, freq="D"))
        trades = [
            ("2024-01-02", "买入", "101", "100", "10100", ""),
            ("2024-01-04", "卖出", "103", "50", "5150", ""),
        ]
        holdings = e._build_daily_holdings(dates, trades)
        assert len(holdings) == 5
        assert holdings[1]["position"] == 100
        assert holdings[3]["position"] == 50

    def test_empty_dates(self):
        e = self._make_engine()
        holdings = e._build_daily_holdings([], [])
        assert holdings == []


# ===================================================================
# 23. BacktestDataManager — _generate_mock_data
# ===================================================================
class TestBacktestGenerateMockData(unittest.TestCase):
    def _make_dm(self):
        from gui_app.backtest.data_manager import DataManager
        dm = object.__new__(DataManager)
        dm.preferred_source = None
        dm._connection_status = {}
        dm._logger = MagicMock()
        return dm

    def test_generates_data(self):
        dm = self._make_dm()
        df = dm._generate_mock_data("600519.SH", "2024-01-01", "2024-03-01")
        assert len(df) > 0
        assert all(c in df.columns for c in ["open", "high", "low", "close", "volume"])

    def test_price_consistency(self):
        dm = self._make_dm()
        df = dm._generate_mock_data("600519.SH", "2024-01-01", "2024-03-01")
        assert (df["low"] <= df["close"]).all()
        assert (df["high"] >= df["close"]).all()

    def test_deterministic(self):
        dm = self._make_dm()
        df1 = dm._generate_mock_data("600519.SH", "2024-01-01", "2024-03-01")
        df2 = dm._generate_mock_data("600519.SH", "2024-01-01", "2024-03-01")
        assert df1["close"].iloc[0] == df2["close"].iloc[0]


# ===================================================================
# 24. BacktestDataManager — _standardize_columns
# ===================================================================
class TestBacktestStandardizeColumns(unittest.TestCase):
    def _make_dm(self):
        from gui_app.backtest.data_manager import DataManager
        dm = object.__new__(DataManager)
        dm._logger = MagicMock()
        return dm

    def test_renames_uppercase(self):
        dm = self._make_dm()
        df = pd.DataFrame({"Open": [1], "High": [2], "Low": [0.5], "Close": [1.5], "Volume": [100]})
        result = dm._standardize_columns(df)
        assert all(c in result.columns for c in ["open", "high", "low", "close", "volume"])

    def test_fills_missing_volume(self):
        dm = self._make_dm()
        df = pd.DataFrame({"close": [10.0], "open": [9.0], "high": [11.0], "low": [8.0]})
        result = dm._standardize_columns(df)
        assert "volume" in result.columns
        assert result["volume"].iloc[0] == 0

    def test_fills_missing_price(self):
        dm = self._make_dm()
        df = pd.DataFrame({"close": [10.0], "volume": [100]})
        result = dm._standardize_columns(df)
        assert result["open"].iloc[0] == 10.0  # filled from close


# ===================================================================
# 25. DataManagerController — _safe_import (standalone)
# ===================================================================
class TestSafeImport(unittest.TestCase):
    def test_valid_module(self):
        from gui_app.data_manager_controller import _safe_import
        result = _safe_import("os")
        assert result is not None

    def test_valid_class(self):
        from gui_app.data_manager_controller import _safe_import
        result = _safe_import("os.path", "join")
        assert result is not None

    def test_invalid_module(self):
        from gui_app.data_manager_controller import _safe_import
        result = _safe_import("nonexistent_module_xyz")
        assert result is None

    def test_invalid_class(self):
        from gui_app.data_manager_controller import _safe_import
        result = _safe_import("os", "nonexistent_attr_xyz")
        assert result is None


if __name__ == "__main__":
    unittest.main()
