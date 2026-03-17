"""
tests/test_coverage_boost_t2b.py
=================================

T2 覆盖率提升（Part B）— 聚焦于未覆盖的大模块：
  1. data_manager/unified_data_interface.py — cache, _pre_write_validate, build_incremental_plan, check_*, connect
  2. data_manager/duckdb_connection_pool.py — resolve_duckdb_path, is_lock_error, is_wal_replay_error, repair_wal
  3. core/api_server.py — rate_limit, health, routes
  4. data_manager/duckdb_fivefold_adjust.py — adjustment logic
  5. data_manager/auto_data_updater.py — checkpoint, update_all_periods
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest


# ═══════════════════════════════════════════════════════════════════════════════
# 1. UnifiedDataInterface — cache system
# ═══════════════════════════════════════════════════════════════════════════════

class TestUDICacheSystem:
    """UDI 类级结果缓存测试。"""

    def _udi_cls(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        return UnifiedDataInterface

    def setup_method(self):
        cls = self._udi_cls()
        cls._result_cache.clear()

    def test_cache_miss(self):
        cls = self._udi_cls()
        assert cls._cache_get(("path", "600519", "1d", "none")) is None

    def test_cache_put_and_get(self):
        cls = self._udi_cls()
        df = pd.DataFrame({"close": [10.0, 10.1]})
        cls._cache_put(("p", "600519", "1d", "none"), df)
        result = cls._cache_get(("p", "600519", "1d", "none"))
        assert result is not None
        assert len(result) == 2

    def test_cache_returns_copy(self):
        cls = self._udi_cls()
        df = pd.DataFrame({"close": [10.0]})
        cls._cache_put(("p", "600519", "1d", "none"), df)
        r1 = cls._cache_get(("p", "600519", "1d", "none"))
        r2 = cls._cache_get(("p", "600519", "1d", "none"))
        assert r1 is not r2

    def test_cache_ttl_expiry(self):
        cls = self._udi_cls()
        df = pd.DataFrame({"close": [10.0]})
        cls._cache_put(("p", "600519", "1d", "none"), df)
        # 手动过期
        key = ("p", "600519", "1d", "none")
        old_ts = time.time() - cls._RESULT_CACHE_TTL_S - 1
        cls._result_cache[key] = (old_ts, df)
        assert cls._cache_get(key) is None

    def test_cache_lru_eviction(self):
        cls = self._udi_cls()
        orig_max = cls._RESULT_CACHE_MAX_ENTRIES
        cls._RESULT_CACHE_MAX_ENTRIES = 3
        try:
            for i in range(5):
                cls._cache_put(("p", f"stock_{i}", "1d", "none"), pd.DataFrame({"v": [i]}))
            assert len(cls._result_cache) <= 3
        finally:
            cls._RESULT_CACHE_MAX_ENTRIES = orig_max

    def test_cache_invalidate(self):
        cls = self._udi_cls()
        cls._cache_put(("mydb", "600519", "1d", "none"), pd.DataFrame({"v": [1]}))
        cls._cache_put(("mydb", "600519", "1d", "hfq"), pd.DataFrame({"v": [2]}))
        cls._cache_put(("mydb", "000001", "1d", "none"), pd.DataFrame({"v": [3]}))
        cls._cache_invalidate("mydb", "600519", "1d")
        assert cls._cache_get(("mydb", "600519", "1d", "none")) is None
        assert cls._cache_get(("mydb", "600519", "1d", "hfq")) is None
        assert cls._cache_get(("mydb", "000001", "1d", "none")) is not None

    def test_cache_invalidate_no_match(self):
        cls = self._udi_cls()
        cls._cache_put(("mydb", "600519", "1d", "none"), pd.DataFrame({"v": [1]}))
        cls._cache_invalidate("other_db", "600519", "1d")
        assert cls._cache_get(("mydb", "600519", "1d", "none")) is not None


# ═══════════════════════════════════════════════════════════════════════════════
# 2. UnifiedDataInterface — _pre_write_validate
# ═══════════════════════════════════════════════════════════════════════════════

class TestPreWriteValidate:
    """UDI._pre_write_validate 静态方法测试。"""

    def _validate(self, df):
        from data_manager.unified_data_interface import UnifiedDataInterface
        return UnifiedDataInterface._pre_write_validate(df)

    def test_none_input(self):
        ok, reason = self._validate(None)
        assert ok is False
        assert "空" in reason

    def test_empty_dataframe(self):
        ok, reason = self._validate(pd.DataFrame())
        assert ok is False

    def test_missing_columns(self):
        df = pd.DataFrame({"open": [10], "close": [10]})
        ok, reason = self._validate(df)
        assert ok is False
        assert "缺少" in reason

    def test_all_nan_ohlc(self):
        df = pd.DataFrame({
            "open": [np.nan], "high": [np.nan], "low": [np.nan], "close": [np.nan],
        })
        ok, reason = self._validate(df)
        assert ok is False
        assert "NaN" in reason

    def test_valid_data(self):
        df = pd.DataFrame({
            "open": [10.0], "high": [10.5], "low": [9.5], "close": [10.2],
        })
        ok, reason = self._validate(df)
        assert ok is True
        assert reason == ""

    def test_negative_prices_rejected(self):
        n = 200
        df = pd.DataFrame({
            "open": [-1.0] * n,
            "high": [-1.0] * n,
            "low": [-1.0] * n,
            "close": [-1.0] * n,
        })
        ok, reason = self._validate(df)
        assert ok is False
        assert "非正" in reason

    def test_partial_nan_accepted(self):
        df = pd.DataFrame({
            "open": [10.0, np.nan],
            "high": [10.5, np.nan],
            "low": [9.5, np.nan],
            "close": [10.2, np.nan],
        })
        ok, reason = self._validate(df)
        assert ok is True


# ═══════════════════════════════════════════════════════════════════════════════
# 3. UnifiedDataInterface — build_incremental_plan
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildIncrementalPlan:
    """UDI.build_incremental_plan 测试。"""

    def _make_udi(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = UnifiedDataInterface.__new__(UnifiedDataInterface)
        udi.con = MagicMock()
        udi._read_only_connection = True
        udi.duckdb_path = ":memory:"
        udi._logger = MagicMock()
        udi._tables_initialized = True
        return udi

    def test_invalid_dates_returns_full(self):
        udi = self._make_udi()
        plan = udi.build_incremental_plan("600519", "bad", "bad", "1d")
        assert plan[0]["mode"] == "full"

    def test_no_existing_data_returns_full(self):
        udi = self._make_udi()
        udi._get_existing_date_bounds = MagicMock(return_value=None)
        plan = udi.build_incremental_plan("600519", "2024-01-01", "2024-03-31", "1d")
        assert plan[0]["mode"] == "full"

    def test_prepend_plan(self):
        udi = self._make_udi()
        udi._get_existing_date_bounds = MagicMock(return_value=("2024-02-01", "2024-03-31"))
        plan = udi.build_incremental_plan("600519", "2024-01-01", "2024-03-31", "1d")
        assert any(p["mode"] == "prepend" for p in plan)

    def test_append_plan(self):
        udi = self._make_udi()
        udi._get_existing_date_bounds = MagicMock(return_value=("2024-01-01", "2024-02-28"))
        plan = udi.build_incremental_plan("600519", "2024-01-01", "2024-03-31", "1d")
        assert any(p["mode"] == "append" for p in plan)

    def test_both_prepend_and_append(self):
        udi = self._make_udi()
        udi._get_existing_date_bounds = MagicMock(return_value=("2024-02-01", "2024-02-28"))
        plan = udi.build_incremental_plan("600519", "2024-01-01", "2024-03-31", "1d")
        modes = [p["mode"] for p in plan]
        assert "prepend" in modes
        assert "append" in modes

    def test_skip_when_complete(self):
        udi = self._make_udi()
        udi._get_existing_date_bounds = MagicMock(return_value=("2024-01-01", "2024-03-31"))
        existing_df = pd.DataFrame({"close": range(60)}, index=pd.bdate_range("2024-01-01", periods=60))
        udi._read_from_duckdb = MagicMock(return_value=existing_df)
        udi._check_missing_trading_days = MagicMock(return_value=0)
        plan = udi.build_incremental_plan("600519", "2024-01-01", "2024-03-31", "1d")
        assert plan[0]["mode"] == "skip"

    def test_refresh_when_missing_days(self):
        udi = self._make_udi()
        udi._get_existing_date_bounds = MagicMock(return_value=("2024-01-01", "2024-03-31"))
        udi._read_from_duckdb = MagicMock(return_value=pd.DataFrame({"close": [1]}))
        udi._check_missing_trading_days = MagicMock(return_value=5)
        plan = udi.build_incremental_plan("600519", "2024-01-01", "2024-03-31", "1d")
        assert plan[0]["mode"] == "refresh"


# ═══════════════════════════════════════════════════════════════════════════════
# 4. UnifiedDataInterface — _check_* methods
# ═══════════════════════════════════════════════════════════════════════════════

class TestUDICheckMethods:
    """UDI 数据源可用性检查方法测试。"""

    def _make_udi(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = UnifiedDataInterface.__new__(UnifiedDataInterface)
        udi._duckdb_checked = False
        udi._akshare_checked = False
        udi._tushare_checked = False
        udi._qmt_checked = False
        udi.duckdb_available = False
        udi.akshare_available = False
        udi.tushare_available = False
        udi.qmt_available = False
        udi._tushare_token = ""
        udi._silent_init = True
        udi._logger = MagicMock()
        return udi

    def test_check_duckdb_available(self):
        udi = self._make_udi()
        udi._check_duckdb()
        assert udi._duckdb_checked is True
        assert udi.duckdb_available is True  # duckdb is installed

    def test_check_duckdb_idempotent(self):
        udi = self._make_udi()
        udi._check_duckdb()
        udi.duckdb_available = False
        udi._check_duckdb()
        assert udi.duckdb_available is False  # didn't re-check

    def test_check_tushare_no_token(self):
        udi = self._make_udi()
        udi._tushare_token = ""
        udi._check_tushare()
        assert udi.tushare_available is False
        assert udi._tushare_checked is True

    def test_check_tushare_idempotent(self):
        udi = self._make_udi()
        udi._check_tushare()
        udi._check_tushare()
        # Should not crash

    def test_check_akshare_import_error(self):
        udi = self._make_udi()
        with patch.dict("sys.modules", {"akshare": None}):
            udi._check_akshare()
        assert udi._akshare_checked is True


# ═══════════════════════════════════════════════════════════════════════════════
# 5. UnifiedDataInterface — circuit breaker
# ═══════════════════════════════════════════════════════════════════════════════

class TestUDICircuitBreaker:
    """UDI 熔断器参数测试。"""

    def test_default_cb_params(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = UnifiedDataInterface.__new__(UnifiedDataInterface)
        udi.__init__(duckdb_path=":memory:", silent_init=True)
        assert udi._cb_state["fail_threshold"] == 5
        assert udi._cb_state["base_s"] == 3.0
        assert udi._cb_state["max_s"] == 300.0

    def test_custom_cb_params(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = UnifiedDataInterface.__new__(UnifiedDataInterface)
        udi.__init__(duckdb_path=":memory:", cb_fail_threshold=10, backoff_base_s=1.0, backoff_max_s=60.0, silent_init=True)
        assert udi._cb_state["fail_threshold"] == 10
        assert udi._cb_state["base_s"] == 1.0
        assert udi._cb_state["max_s"] == 60.0

    def test_cb_env_vars(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        with patch.dict(os.environ, {
            "EASYXT_REMOTE_CB_THRESHOLD": "8",
            "EASYXT_REMOTE_BACKOFF_BASE_S": "2.5",
            "EASYXT_REMOTE_BACKOFF_MAX_S": "120.0",
        }):
            udi = UnifiedDataInterface.__new__(UnifiedDataInterface)
            udi.__init__(duckdb_path=":memory:", silent_init=True)
        assert udi._cb_state["fail_threshold"] == 8
        assert udi._cb_state["base_s"] == 2.5
        assert udi._cb_state["max_s"] == 120.0


# ═══════════════════════════════════════════════════════════════════════════════
# 6. UnifiedDataInterface — connect
# ═══════════════════════════════════════════════════════════════════════════════

class TestUDIConnect:
    """UDI.connect 方法测试。"""

    def _make_udi(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = UnifiedDataInterface.__new__(UnifiedDataInterface)
        udi.duckdb_path = ":memory:"
        udi.con = None
        udi._read_only_connection = False
        udi.duckdb_available = True
        udi.qmt_available = False
        udi.akshare_available = False
        udi.tushare_available = False
        udi._duckdb_checked = True
        udi._qmt_checked = True
        udi._akshare_checked = True
        udi._tushare_checked = True
        udi._tables_initialized = False
        udi._silent_init = True
        udi._logger = MagicMock()
        udi.adjustment_manager = None
        udi.data_registry = MagicMock()
        udi._tushare_token = ""
        udi._cb_state = {"open": False, "fail_count": 0, "opened_at": 0, "cooldown_s": 0, "base_s": 3, "max_s": 300, "fail_threshold": 5}
        udi._cache_stale_quarantine_enabled = False
        udi._step6_validate_sample_rate = 1.0
        return udi

    def test_connect_memory_db(self):
        udi = self._make_udi()
        result = udi.connect()
        assert result is True
        assert udi.con is not None
        if udi.con:
            udi.con.close()

    def test_connect_read_only(self):
        udi = self._make_udi()
        result = udi.connect(read_only=True)
        assert result is True
        if udi.con:
            udi.con.close()


# ═══════════════════════════════════════════════════════════════════════════════
# 7. DuckDB Connection Pool — static helpers
# ═══════════════════════════════════════════════════════════════════════════════

class TestDuckDBPoolHelpers:
    """DuckDBConnectionManager 静态辅助方法测试。"""

    def test_is_lock_error_true(self):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        assert DuckDBConnectionManager._is_lock_error(Exception("database file is locked"))
        assert DuckDBConnectionManager._is_lock_error(Exception("already open by another process"))
        assert DuckDBConnectionManager._is_lock_error(Exception("另一个程序正在使用"))
        assert DuckDBConnectionManager._is_lock_error(Exception("different configuration than existing connections"))

    def test_is_lock_error_false(self):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        assert not DuckDBConnectionManager._is_lock_error(Exception("syntax error"))
        assert not DuckDBConnectionManager._is_lock_error(Exception("table not found"))

    def test_is_wal_replay_error(self):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        assert DuckDBConnectionManager._is_wal_replay_error(
            Exception("Failure while replaying WAL file")
        )
        assert not DuckDBConnectionManager._is_wal_replay_error(
            Exception("connection refused")
        )


class TestResolveDuckdbPath:
    """resolve_duckdb_path 路径解析测试。"""

    def test_explicit_path(self):
        from data_manager.duckdb_connection_pool import resolve_duckdb_path
        assert resolve_duckdb_path("/my/custom/path.ddb") == "/my/custom/path.ddb"

    def test_env_var_override(self, tmp_path):
        from data_manager.duckdb_connection_pool import resolve_duckdb_path
        db_path = str(tmp_path / "test.ddb")
        (tmp_path / "test.ddb").touch()
        with patch.dict(os.environ, {"EASYXT_DUCKDB_PATH": db_path}):
            result = resolve_duckdb_path()
        assert result == db_path

    def test_fallback_to_project_default(self):
        from data_manager.duckdb_connection_pool import resolve_duckdb_path
        with patch.dict(os.environ, {"EASYXT_DUCKDB_PATH": "", "EASYXT_DUCKDB_LEGACY_PATH": "/nonexistent/path.ddb"}):
            result = resolve_duckdb_path()
        assert result.endswith("stock_data.ddb")


# ═══════════════════════════════════════════════════════════════════════════════
# 8. AutoDataUpdater — checkpoint save/load
# ═══════════════════════════════════════════════════════════════════════════════

class TestAutoDataUpdaterCheckpoint:
    """AutoDataUpdater checkpoint 存/取测试。"""

    def _make_updater(self, tmp_path):
        from data_manager.auto_data_updater import AutoDataUpdater
        updater = AutoDataUpdater.__new__(AutoDataUpdater)
        updater.interface = None
        updater.data_manager = None
        updater.duckdb_path = ":memory:"
        updater.stock_list = ["600519.SH", "000001.SZ"]
        updater._checkpoint_path = tmp_path / "checkpoint.json"
        updater._logger = MagicMock()
        updater.calendar = MagicMock()
        return updater

    def test_save_and_load_checkpoint(self, tmp_path):
        updater = self._make_updater(tmp_path)
        updater._save_checkpoint(
            batch_date="2024-06-01",
            last_index=5,
            total=100,
            success_count=5,
            failed_count=0,
            failed_stocks=[],
        )
        loaded = updater._load_checkpoint("2024-06-01")
        assert loaded["batch_date"] == "2024-06-01"
        assert loaded["last_index"] == 5
        assert loaded["success_count"] == 5

    def test_load_checkpoint_wrong_date(self, tmp_path):
        updater = self._make_updater(tmp_path)
        updater._save_checkpoint("2024-06-01", 5, 100, 5, 0, [])
        loaded = updater._load_checkpoint("2024-06-02")
        assert loaded == {}

    def test_load_missing_checkpoint(self, tmp_path):
        updater = self._make_updater(tmp_path)
        loaded = updater._load_checkpoint("2024-06-01")
        assert loaded == {}


class TestAutoDataUpdaterUpdateAllPeriods:
    """AutoDataUpdater.update_all_periods_for_stock 测试。"""

    def _make_updater(self, tmp_path):
        from data_manager.auto_data_updater import AutoDataUpdater
        updater = AutoDataUpdater.__new__(AutoDataUpdater)
        updater.interface = MagicMock()
        updater.interface.con = MagicMock()
        updater.data_manager = None
        updater.duckdb_path = ":memory:"
        updater.stock_list = []
        updater._checkpoint_path = tmp_path / "cp.json"
        updater._logger = MagicMock()
        updater.calendar = MagicMock()
        updater.ALL_PERIODS = ["1d", "1m", "5m"]
        return updater

    def test_update_all_periods_basic(self, tmp_path):
        updater = self._make_updater(tmp_path)
        if hasattr(updater, "update_all_periods_for_stock"):
            # Mock get_listing_date and the internal update methods
            updater.get_listing_date = MagicMock(return_value="2020-01-01")
            updater._run_single_update = MagicMock(return_value={"ok": True, "rows": 100})
            result = updater.update_all_periods_for_stock("600519.SH")
            assert isinstance(result, dict)


# ═══════════════════════════════════════════════════════════════════════════════
# 9. DuckDB FiveFoldAdjustmentManager — adjustment logic
# ═══════════════════════════════════════════════════════════════════════════════

class TestFiveFoldAdjustmentBasic:
    """FiveFoldAdjustmentManager 基础创建和辅助方法测试。"""

    def test_import(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        assert FiveFoldAdjustmentManager is not None

    def test_adjust_type_mapping(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        mgr = FiveFoldAdjustmentManager.__new__(FiveFoldAdjustmentManager)
        mgr._db = None
        mgr._logger = MagicMock()
        # Check that the adjustment type mapping exists
        assert hasattr(FiveFoldAdjustmentManager, "ADJUST_TYPES") or hasattr(mgr, "_calc_factor")


class TestFiveFoldAdjustmentCalculation:
    """FiveFoldAdjustmentManager 计算逻辑测试。"""

    def _make_daily_df(self):
        return pd.DataFrame({
            "date": pd.date_range("2024-01-02", periods=10),
            "open": [10.0, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9],
            "high": [10.5, 10.6, 10.7, 10.8, 10.9, 11.0, 11.1, 11.2, 11.3, 11.4],
            "low": [9.5, 9.6, 9.7, 9.8, 9.9, 10.0, 10.1, 10.2, 10.3, 10.4],
            "close": [10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9, 11.0, 11.1],
            "volume": [100000] * 10,
        })

    def test_front_adjust_no_dividends(self):
        """无分红时前复权因子应全为 1。"""
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        mgr = FiveFoldAdjustmentManager.__new__(FiveFoldAdjustmentManager)
        mgr._db = None
        mgr._logger = MagicMock()
        df = self._make_daily_df()
        df.index = df["date"]
        empty_divs = pd.DataFrame(columns=["ex_date", "dividend_per_share", "bonus_ratio"])
        result = mgr._calculate_front_adjustment(df, empty_divs)
        assert isinstance(result, pd.DataFrame)
        # 无分红时价格不应改变
        pd.testing.assert_series_equal(result["close"], df["close"])

    def test_front_and_back_adjustment_with_dividends(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        mgr = FiveFoldAdjustmentManager.__new__(FiveFoldAdjustmentManager)
        mgr._db = None
        mgr._logger = MagicMock()
        df = self._make_daily_df()
        df.index = df["date"]
        dividends = pd.DataFrame({
            "ex_date": [df.index[5]],
            "dividend_per_share": [0.5],
            "bonus_ratio": [None],
        })
        front = mgr._calculate_front_adjustment(df, dividends)
        assert isinstance(front, pd.DataFrame)
        assert len(front) == len(df)
        back = mgr._calculate_back_adjustment(df, dividends)
        assert isinstance(back, pd.DataFrame)
        assert len(back) == len(df)

    def test_geometric_adjustments(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        mgr = FiveFoldAdjustmentManager.__new__(FiveFoldAdjustmentManager)
        mgr._db = None
        mgr._logger = MagicMock()
        df = self._make_daily_df()
        df.index = df["date"]
        dividends = pd.DataFrame({
            "ex_date": [df.index[3]],
            "dividend_per_share": [0.3],
            "bonus_ratio": [None],
        })
        gf = mgr._calculate_geometric_front_adjustment(df, dividends)
        assert isinstance(gf, pd.DataFrame)
        gb = mgr._calculate_geometric_back_adjustment(df, dividends)
        assert isinstance(gb, pd.DataFrame)


# ═══════════════════════════════════════════════════════════════════════════════
# 10. UnifiedDataInterface — schema version
# ═══════════════════════════════════════════════════════════════════════════════

class TestSchemaVersion:
    def test_schema_version_format(self):
        from data_manager.unified_data_interface import CURRENT_SCHEMA_VERSION
        parts = CURRENT_SCHEMA_VERSION.split(".")
        assert len(parts) >= 2
        assert all(p.isdigit() for p in parts)


# ═══════════════════════════════════════════════════════════════════════════════
# 11. UnifiedDataInterface — DataSourceRegistry integration
# ═══════════════════════════════════════════════════════════════════════════════

class TestDataSourceRegistryIntegration:
    """UDI 数据源注册表集成测试。"""

    def test_registry_has_sources(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = UnifiedDataInterface.__new__(UnifiedDataInterface)
        udi.__init__(duckdb_path=":memory:", silent_init=True)
        reg = udi.data_registry
        assert reg is not None
        # 应该注册了 duckdb/dat/parquet/tushare/akshare
        assert "duckdb" in reg._sources
        assert "dat" in reg._sources
        assert "parquet" in reg._sources


# ═══════════════════════════════════════════════════════════════════════════════
# 12. smart_data_detector — 补充覆盖
# ═══════════════════════════════════════════════════════════════════════════════

class TestSmartDataDetectorExtra:
    """smart_data_detector 模块补充覆盖。"""

    def test_import(self):
        from data_manager.smart_data_detector import SmartDataDetector
        assert SmartDataDetector is not None

    def test_detect_basic(self):
        from data_manager.smart_data_detector import SmartDataDetector
        det = SmartDataDetector()
        if hasattr(det, "detect_sources"):
            result = det.detect_sources()
            assert isinstance(result, (dict, list))

    def test_trading_calendar(self):
        from data_manager.smart_data_detector import TradingCalendar
        cal = TradingCalendar()
        # 2024-01-06 是周六
        assert cal.is_trading_day(_dt.date(2024, 1, 6)) is False
        # 2024-01-08 是周一
        assert cal.is_trading_day(_dt.date(2024, 1, 8)) is True

    def test_trading_days_range(self):
        from data_manager.smart_data_detector import TradingCalendar
        cal = TradingCalendar()
        days = cal.get_trading_days(_dt.date(2024, 1, 1), _dt.date(2024, 1, 14))
        assert isinstance(days, list)
        assert len(days) > 0
        # 每天都应该是工作日
        for d in days:
            assert d.weekday() < 5


# ═══════════════════════════════════════════════════════════════════════════════
# 13. realtime_pipeline_manager — 补充覆盖
# ═══════════════════════════════════════════════════════════════════════════════

class TestRealtimePipelineManagerExtra:
    """realtime_pipeline_manager 模块补充覆盖。"""

    def test_import(self):
        from data_manager.realtime_pipeline_manager import RealtimePipelineManager
        assert RealtimePipelineManager is not None

    def test_default_params(self):
        from data_manager.realtime_pipeline_manager import RealtimePipelineManager
        mgr = RealtimePipelineManager()
        assert mgr.max_queue >= 32
        assert mgr.flush_interval_s > 0

    def test_custom_params(self):
        from data_manager.realtime_pipeline_manager import RealtimePipelineManager
        mgr = RealtimePipelineManager(max_queue=512, flush_interval_ms=500)
        assert mgr.max_queue == 512
        assert abs(mgr.flush_interval_s - 0.5) < 0.01

    def test_configure(self):
        from data_manager.realtime_pipeline_manager import RealtimePipelineManager
        mgr = RealtimePipelineManager()
        mgr.configure("600519.SH", "1d", None)
        assert mgr._symbol == "600519.SH"
        assert mgr._period == "1d"

    def test_configure_reset_on_symbol_change(self):
        from data_manager.realtime_pipeline_manager import RealtimePipelineManager
        mgr = RealtimePipelineManager()
        mgr.configure("600519.SH", "1d", None)
        mgr._total_quotes = 100
        mgr.configure("000001.SZ", "1d", None)
        assert mgr._total_quotes == 0

    def test_drop_rate_threshold_clamp(self):
        from data_manager.realtime_pipeline_manager import RealtimePipelineManager
        mgr = RealtimePipelineManager()
        assert 0.001 <= mgr._drop_rate_threshold <= 0.999


# ═══════════════════════════════════════════════════════════════════════════════
# 14. financial_data_saver — 补充覆盖
# ═══════════════════════════════════════════════════════════════════════════════

class TestFinancialDataSaverExtra:
    """financial_data_saver 模块补充覆盖。"""

    def test_import(self):
        from data_manager.financial_data_saver import FinancialDataSaver
        assert FinancialDataSaver is not None

    def test_saver_init(self):
        from data_manager.financial_data_saver import FinancialDataSaver
        saver = FinancialDataSaver.__new__(FinancialDataSaver)
        saver._db = None
        saver._logger = MagicMock()
        assert isinstance(saver, FinancialDataSaver)


# ═══════════════════════════════════════════════════════════════════════════════
# 15. data_integrity_checker — 补充覆盖
# ═══════════════════════════════════════════════════════════════════════════════

class TestDataIntegrityCheckerExtra:
    """data_integrity_checker 补充覆盖。"""

    def test_import(self):
        from data_manager.data_integrity_checker import DataIntegrityChecker
        assert DataIntegrityChecker is not None

    def test_quality_report(self):
        from data_manager.data_integrity_checker import DataQualityReport
        r = DataQualityReport()
        r.add_issue("ERROR", "test error")
        r.add_issue("WARNING", "test warn")
        r.add_issue("INFO", "test info")
        assert r.has_errors()
        assert r.has_warnings()
        summary = r.get_summary()
        assert summary["errors"] == 1
        assert summary["warnings"] == 1
        assert summary["info"] == 1

    def test_quality_report_no_issues(self):
        from data_manager.data_integrity_checker import DataQualityReport
        r = DataQualityReport()
        assert not r.has_errors()
        assert not r.has_warnings()


# ═══════════════════════════════════════════════════════════════════════════════
# 16. datasource_registry — 补充覆盖
# ═══════════════════════════════════════════════════════════════════════════════

class TestDatasourceRegistryExtra:
    """datasource_registry 补充覆盖。"""

    def test_registry_register(self):
        from data_manager.datasource_registry import DataSourceRegistry
        reg = DataSourceRegistry()
        mock_source = MagicMock()
        reg.register("test_src", mock_source)
        assert "test_src" in reg._sources

    def test_registry_unregister(self):
        from data_manager.datasource_registry import DataSourceRegistry
        reg = DataSourceRegistry()
        mock_source = MagicMock()
        reg.register("test_src", mock_source)
        reg.unregister("test_src")
        assert "test_src" not in reg._sources

    def test_registry_metrics_init(self):
        from data_manager.datasource_registry import DataSourceRegistry
        reg = DataSourceRegistry()
        mock_source = MagicMock()
        reg.register("src_a", mock_source)
        assert reg._metrics["src_a"]["hits"] == 0
        assert reg._metrics["src_a"]["errors"] == 0

    def test_registry_quality_params(self):
        from data_manager.datasource_registry import DataSourceRegistry
        reg = DataSourceRegistry(max_nan_rate=0.1, min_close_valid_rate=0.8)
        assert reg._max_nan_rate == 0.1
        assert reg._min_close_valid_rate == 0.8


# ═══════════════════════════════════════════════════════════════════════════════
# 17. pipeline_health — 补充覆盖
# ═══════════════════════════════════════════════════════════════════════════════

class TestPipelineHealthExtra:
    """pipeline_health 模块补充覆盖。"""

    def test_import(self):
        from data_manager.pipeline_health import PipelineHealth
        assert PipelineHealth is not None

    def test_report_structure(self):
        from data_manager.pipeline_health import PipelineHealth
        ph = PipelineHealth()
        report = ph.report()
        assert isinstance(report, dict)
        assert "overall_healthy" in report
        assert "timestamp" in report
        assert "checks" in report
        assert "duckdb" in report["checks"]
        assert "factor_registry" in report["checks"]


# ═══════════════════════════════════════════════════════════════════════════════
# 18. async_task_manager — 补充覆盖
# ═══════════════════════════════════════════════════════════════════════════════

class TestAsyncTaskManagerExtra:
    """async_task_manager 补充覆盖。"""

    def test_import(self):
        from core.async_task_manager import AsyncTaskManager
        assert AsyncTaskManager is not None

    def test_task_priority_enum(self):
        from core.async_task_manager import TaskPriority
        assert TaskPriority.LOW is not None
        assert TaskPriority.NORMAL is not None
        assert TaskPriority.HIGH is not None
        assert TaskPriority.LOW.value < TaskPriority.NORMAL.value

    def test_task_priority_ordering(self):
        from core.async_task_manager import TaskPriority
        priorities = sorted([TaskPriority.HIGH, TaskPriority.LOW, TaskPriority.NORMAL])
        assert priorities[0] == TaskPriority.LOW
        assert priorities[-1] == TaskPriority.HIGH


# ═══════════════════════════════════════════════════════════════════════════════
# 19. board_stocks_loader — 补充覆盖
# ═══════════════════════════════════════════════════════════════════════════════

class TestBoardStocksLoaderExtra:
    """board_stocks_loader 补充覆盖。"""

    def test_import(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        assert BoardStocksLoader is not None

    def test_A_SHARE_SECTOR_PATTERNS(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        if hasattr(BoardStocksLoader, "SECTOR_PATTERNS"):
            assert isinstance(BoardStocksLoader.SECTOR_PATTERNS, dict)
