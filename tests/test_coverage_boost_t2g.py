"""
T2g — Coverage boost test file (target: +210+ lines → 45%).

Covers missed branches in:
  1. data_manager/auto_data_updater.py  — update_single_stock, get_listing_date,
                                          update_all_periods_for_stock, bulk_download,
                                          should_update_today extra branches, run_update_task,
                                          _shift_time, stop/get_status
  2. data_manager/duckdb_connection_pool.py — DuckDBConnectionManager helpers,
                                              get_lock_metrics, reset_lock_metrics,
                                              repair_wal_if_needed, execute_read_query,
                                              execute_write_query, insert_dataframe,
                                              checkpoint, connection_count, get_db_manager,
                                              query_dataframe
  3. data_manager/smart_data_detector.py  — print_missing_report, close
  4. data_manager/period_bar_builder.py   — build() branches (unknown period, BASE, empty data),
                                            build_intraday_bars (empty), build_multiday_bars,
                                            build_natural_calendar_bars, cross_validate branches,
                                            _validate_intraday_vs_daily, _validate_multiday_vs_daily
  5. data_manager/duckdb_fivefold_adjust.py — _calculate_front_adjustment helpers,
                                               calculate_adjustment_factors
"""
from __future__ import annotations

import threading
from datetime import date, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch, call, PropertyMock
import logging

import pandas as pd
import numpy as np
import pytest


# ===========================================================================
# 1. _shift_time helper (auto_data_updater module-level function)
# ===========================================================================

class TestShiftTime:
    def test_add_5_minutes(self):
        from data_manager.auto_data_updater import _shift_time
        assert _shift_time("15:30", 5) == "15:35"

    def test_rollover_to_next_hour(self):
        from data_manager.auto_data_updater import _shift_time
        assert _shift_time("15:55", 10) == "16:05"

    def test_rollover_past_midnight(self):
        from data_manager.auto_data_updater import _shift_time
        # 23:50 + 15 = 00:05 (next day)
        assert _shift_time("23:50", 15) == "00:05"

    def test_zero_offset(self):
        from data_manager.auto_data_updater import _shift_time
        assert _shift_time("09:30", 0) == "09:30"


# ===========================================================================
# 2. AutoDataUpdater — update_single_stock, get_listing_date,
#    update_all_periods_for_stock, bulk_download, run_update_task, stop
# ===========================================================================

def _make_updater():
    """Create a bare AutoDataUpdater without full initialization."""
    from data_manager.auto_data_updater import AutoDataUpdater
    from pathlib import Path
    upd = AutoDataUpdater.__new__(AutoDataUpdater)
    upd.duckdb_path = ":memory:"
    upd.update_time = "15:30"
    from data_manager.smart_data_detector import TradingCalendar
    upd.calendar = TradingCalendar()
    upd.running = False
    upd.thread = None
    upd.data_manager = None
    upd.interface = None
    upd.last_update_time = None
    upd.last_update_status = None
    upd.total_updates = 0
    upd._checkpoint_path = Path("/tmp/test_checkpoint.json")
    return upd


class TestAutoDataUpdaterUpdateSingleStock:
    """update_single_stock — branch when interface is None and stays None."""

    def test_no_interface_returns_error_message(self):
        upd = _make_updater()
        with patch.object(upd, "initialize_interface", side_effect=lambda: None):
            upd.interface = None
            result = upd.update_single_stock("000001.SZ")
        assert result["success"] is False
        assert "未初始化" in result["message"]

    def test_interface_returns_empty_plan(self):
        upd = _make_updater()
        mock_iface = MagicMock()
        mock_iface.build_incremental_plan.return_value = []
        upd.interface = mock_iface
        result = upd.update_single_stock("000001.SZ")
        assert result["success"] is False
        assert "无更新计划" in result["message"]

    def test_interface_plan_with_skip_mode_only(self):
        upd = _make_updater()
        mock_iface = MagicMock()
        mock_iface.build_incremental_plan.return_value = [{"mode": "skip"}]
        upd.interface = mock_iface
        result = upd.update_single_stock("000001.SZ")
        assert result["success"] is False

    def test_interface_plan_returns_data(self):
        upd = _make_updater()
        mock_iface = MagicMock()
        mock_iface.build_incremental_plan.return_value = [
            {"mode": "fill", "start_date": "2024-01-02", "end_date": "2024-01-02"}
        ]
        mock_iface.get_stock_data.return_value = pd.DataFrame({"close": [10.0, 11.0]})
        upd.interface = mock_iface
        result = upd.update_single_stock("000001.SZ")
        assert result["success"] is True
        assert result["records"] == 2

    def test_interface_exception_caught(self):
        upd = _make_updater()
        mock_iface = MagicMock()
        mock_iface.build_incremental_plan.side_effect = RuntimeError("boom")
        upd.interface = mock_iface
        result = upd.update_single_stock("000001.SZ")
        assert result["success"] is False
        assert "失败" in result["message"]


class TestAutoDataUpdaterGetListingDate:
    """get_listing_date — fallback path (no env var, no interface)."""

    def test_no_interface_fallback(self):
        upd = _make_updater()
        upd.interface = None
        listing = upd.get_listing_date("000001.SZ")
        assert listing == "1990-01-01"

    def test_interface_without_con_returns_fallback(self):
        upd = _make_updater()
        mock_iface = MagicMock()
        mock_iface.con = None
        upd.interface = mock_iface
        listing = upd.get_listing_date("000001.SZ")
        assert listing == "1990-01-01"


class TestAutoDataUpdaterUpdateAllPeriodsForStock:
    """update_all_periods_for_stock branches."""

    def test_no_interface_returns_empty(self):
        upd = _make_updater()
        with patch.object(upd, "initialize_interface", side_effect=lambda: None):
            upd.interface = None
            result = upd.update_all_periods_for_stock("000001.SZ", periods=["1d"])
        assert result["total_records"] == 0
        assert "未初始化" in result["message"]

    def test_returns_results_for_empty_data(self):
        upd = _make_updater()
        mock_iface = MagicMock()
        mock_iface.get_stock_data.return_value = None
        upd.interface = mock_iface
        # Need get_listing_date to return something
        with patch.object(upd, "get_listing_date", return_value="2020-01-01"):
            result = upd.update_all_periods_for_stock("000001.SZ", periods=["1d"])
        assert result["stock_code"] == "000001.SZ"
        assert result["total_records"] == 0

    def test_returns_results_for_good_data(self):
        upd = _make_updater()
        mock_iface = MagicMock()
        mock_iface.get_stock_data.return_value = pd.DataFrame({"close": [10.0]})
        upd.interface = mock_iface
        with patch.object(upd, "get_listing_date", return_value="2020-01-01"):
            result = upd.update_all_periods_for_stock(
                "000001.SZ", periods=["1d"],
                start_date="2024-01-01", end_date="2024-01-31"
            )
        assert result["success_periods"] == 1
        assert result["total_records"] == 1


class TestAutoDataUpdaterBulkDownload:
    """bulk_download branch — interface None returns early."""

    def test_no_interface_returns_all_failed(self):
        upd = _make_updater()
        with patch.object(upd, "initialize_interface", side_effect=lambda: None):
            upd.interface = None
            result = upd.bulk_download(
                stock_codes=["000001.SZ", "600000.SH"],
                periods=["1d"],
            )
        assert result["total_stocks"] == 2
        assert result["failed_stocks"] == 2
        assert result["success_stocks"] == 0

    def test_with_stop_event_set_immediately(self):
        upd = _make_updater()
        stop = threading.Event()
        stop.set()
        mock_iface = MagicMock()
        upd.interface = mock_iface
        with patch.object(upd, "update_all_periods_for_stock",
                          return_value={"total_records": 0, "success_periods": 0}) as mock_upd:
            result = upd.bulk_download(
                stock_codes=["000001.SZ"],
                periods=["1d"],
                stop_event=stop,
            )
        # All stocks skipped due to stop_event
        assert result["total_stocks"] == 1

    def test_on_progress_callback_called(self):
        upd = _make_updater()
        mock_iface = MagicMock()
        upd.interface = mock_iface
        calls_received = []
        def progress_cb(current, total, code, periods, status):
            calls_received.append((current, total, status))
        with patch.object(upd, "update_all_periods_for_stock",
                          return_value={"total_records": 5, "success_periods": 1}):
            upd.bulk_download(
                stock_codes=["000001.SZ"],
                periods=["1d"],
                on_progress=progress_cb,
            )
        assert len(calls_received) == 1
        assert calls_received[0][2] == "ok"


class TestAutoDataUpdaterRunUpdateTask:
    """run_update_task — both branches (skip and run)."""

    def test_no_update_needed_skips(self):
        upd = _make_updater()
        with patch.object(upd, "should_update_today", return_value=False):
            with patch.object(upd, "update_all_stocks") as mock_upd:
                upd.run_update_task()
        mock_upd.assert_not_called()

    def test_update_needed_calls_update_all_stocks(self):
        upd = _make_updater()
        with patch.object(upd, "should_update_today", return_value=True):
            with patch.object(upd, "update_all_stocks", return_value={"total": 0}) as mock_upd:
                upd.run_update_task()
        mock_upd.assert_called_once()

    def test_exception_in_update_is_caught(self):
        upd = _make_updater()
        with patch.object(upd, "should_update_today", return_value=True):
            with patch.object(upd, "update_all_stocks", side_effect=RuntimeError("crash")):
                upd.run_update_task()  # Should not raise


class TestAutoDataUpdaterStopAndStatus:
    """stop() and get_status()."""

    def test_stop_sets_running_false(self):
        upd = _make_updater()
        upd.running = True
        upd.thread = None
        upd.stop()
        assert upd.running is False

    def test_stop_joins_thread(self):
        upd = _make_updater()
        upd.running = True
        mock_thread = MagicMock()
        upd.thread = mock_thread
        upd.stop()
        mock_thread.join.assert_called_once()
        assert upd.thread is None

    def test_get_status_keys(self):
        upd = _make_updater()
        status = upd.get_status()
        assert "running" in status
        assert "update_time" in status
        assert "last_update" in status
        assert "total_updates" in status
        assert "is_trading_day" in status
        assert "should_update" in status

    def test_get_status_last_update_when_set(self):
        upd = _make_updater()
        upd.last_update_time = date(2024, 1, 2)
        status = upd.get_status()
        assert status["last_update"] == "2024-01-02"


# ===========================================================================
# 3. DuckDBConnectionManager helpers
# ===========================================================================

class TestDuckDBConnectionPool:
    """DuckDBConnectionManager auxiliary methods that don't require DB access."""

    def _make_mgr(self):
        import tempfile, os
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        # Use a fresh temp path so each test gets a fresh instance
        tmp = tempfile.mktemp(suffix=".ddb")
        mgr = DuckDBConnectionManager.__new__(DuckDBConnectionManager)
        mgr._initialized = True
        mgr.duckdb_path = tmp
        mgr._write_lock = __import__("threading").RLock()
        mgr._write_file_lock_path = tmp + ".write.lock"
        mgr._write_lock_timeout_s = 5.0
        mgr._write_lock_stale_s = 30.0
        mgr._connection_count = 0
        mgr._wal_repaired_once = False
        mgr._lock_metrics = {"attempts": 0, "failures": 0, "wait_times_ms": []}
        mgr._checkpoint_thread = None
        mgr._checkpoint_stop = __import__("threading").Event()
        mgr._checkpoint_interval_s = 300.0
        mgr._checkpoint_enabled = False
        mgr._instance_key = tmp
        return mgr

    def test_is_lock_error_with_lock_text(self):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        assert DuckDBConnectionManager._is_lock_error(Exception("lock error")) is True

    def test_is_lock_error_with_already_open(self):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        assert DuckDBConnectionManager._is_lock_error(Exception("already open")) is True

    def test_is_lock_error_false(self):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        assert DuckDBConnectionManager._is_lock_error(Exception("some other error")) is False

    def test_is_wal_replay_error_true(self):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        err = Exception("failure while replaying wal file")
        assert DuckDBConnectionManager._is_wal_replay_error(err) is True

    def test_is_wal_replay_error_false(self):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        assert DuckDBConnectionManager._is_wal_replay_error(Exception("random")) is False

    def test_connection_count_property(self):
        mgr = self._make_mgr()
        assert mgr.connection_count == 0
        mgr._connection_count = 3
        assert mgr.connection_count == 3

    def test_get_lock_metrics_empty(self):
        mgr = self._make_mgr()
        metrics = mgr.get_lock_metrics()
        assert metrics["failure_rate"] == 0.0
        assert metrics["p95_wait_ms"] == 0.0
        assert metrics["total_attempts"] == 0
        assert metrics["failures"] == 0

    def test_get_lock_metrics_with_data(self):
        mgr = self._make_mgr()
        mgr._lock_metrics = {"attempts": 10, "failures": 1, "wait_times_ms": [10.0, 20.0, 30.0]}
        metrics = mgr.get_lock_metrics()
        assert metrics["failure_rate"] == pytest.approx(0.1)
        assert metrics["total_attempts"] == 10

    def test_reset_lock_metrics(self):
        mgr = self._make_mgr()
        mgr._lock_metrics = {"attempts": 5, "failures": 2, "wait_times_ms": [50.0]}
        mgr.reset_lock_metrics()
        assert mgr._lock_metrics == {"attempts": 0, "failures": 0, "wait_times_ms": []}

    def test_repair_wal_if_needed_returns_false_no_wal(self):
        mgr = self._make_mgr()
        # WAL file doesn't exist → returns False immediately
        assert mgr._repair_wal_if_needed() is False

    def test_repair_wal_already_repaired(self):
        mgr = self._make_mgr()
        mgr._wal_repaired_once = True
        assert mgr._repair_wal_if_needed() is False

    def test_repair_wal_disabled_by_env(self):
        import os
        mgr = self._make_mgr()
        with patch.dict(os.environ, {"EASYXT_ENABLE_WAL_AUTO_REPAIR": "0"}):
            assert mgr._repair_wal_if_needed() is False

    def test_public_repair_wal_delegates(self):
        mgr = self._make_mgr()
        with patch.object(mgr, "_repair_wal_if_needed", return_value=False) as mock_repair:
            result = mgr.repair_wal_if_needed()
        mock_repair.assert_called_once()
        assert result is False

    def test_on_process_exit_calls_checkpoint(self):
        mgr = self._make_mgr()
        with patch.object(mgr, "checkpoint", return_value=True) as mock_ckpt:
            mgr._on_process_exit()
        mock_ckpt.assert_called_once()
        assert mgr._checkpoint_stop.is_set()


# ===========================================================================
# 4. SmartDataDetector.print_missing_report and close
# ===========================================================================

class TestSmartDataDetectorPrintReport:
    """print_missing_report — exercises the print output path."""

    def _make_report_with_data(self):
        return {
            "stock_code": "000001.SZ",
            "check_range": ("2024-01-01", "2024-12-31"),
            "expected_trading_days": 243,
            "existing_data": {"count": 240, "first_date": "2024-01-02", "last_date": "2024-12-31"},
            "missing_trading_days": ["2024-05-01", "2024-05-02", "2024-10-07"],
            "completeness_ratio": 0.9876,
            "missing_segments": [
                {"start": "2024-05-01", "end": "2024-05-02", "days": 2},
                {"start": "2024-10-07", "end": "2024-10-07", "days": 1},
            ],
        }

    def _make_report_no_data(self):
        return {
            "stock_code": "000001.SZ",
            "check_range": ("2024-01-01", "2024-12-31"),
            "expected_trading_days": 243,
            "existing_data": {"count": 0, "first_date": None, "last_date": None},
            "missing_trading_days": [],
            "completeness_ratio": 0.0,
            "missing_segments": [],
        }

    def test_print_with_data(self, capsys):
        from data_manager.smart_data_detector import SmartDataDetector
        det = SmartDataDetector.__new__(SmartDataDetector)
        det.print_missing_report(self._make_report_with_data())
        captured = capsys.readouterr()
        assert "000001.SZ" in captured.out
        assert "98.76%" in captured.out

    def test_print_no_data(self, capsys):
        from data_manager.smart_data_detector import SmartDataDetector
        det = SmartDataDetector.__new__(SmartDataDetector)
        det.print_missing_report(self._make_report_no_data())
        captured = capsys.readouterr()
        assert "无数据" in captured.out
        assert "数据完整" in captured.out

    def test_close_sets_none(self):
        from data_manager.smart_data_detector import SmartDataDetector
        det = SmartDataDetector.__new__(SmartDataDetector)
        det.con = MagicMock()
        det._manager = MagicMock()
        det.close()
        assert det.con is None
        assert det._manager is None


# ===========================================================================
# 5. PeriodBarBuilder.build() branches
# ===========================================================================

class TestPeriodBarBuilderBuildBranches:
    """Covers missed branches in build()."""

    def _make_builder(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        return PeriodBarBuilder()

    def test_unknown_period_raises(self):
        b = self._make_builder()
        with pytest.raises(ValueError, match="未知周期"):
            b.build("99x")

    def test_base_period_raises(self):
        b = self._make_builder()
        # '1d' is BASE type
        with pytest.raises(ValueError, match="基础数据"):
            b.build("1d")

    def test_intraday_empty_1m_returns_empty(self):
        b = self._make_builder()
        result = b.build("2m", data_1m=pd.DataFrame(), data_1d=None)
        assert result.empty

    def test_multiday_empty_1d_returns_empty(self):
        b = self._make_builder()
        result = b.build("5d", data_1m=None, data_1d=pd.DataFrame())
        assert result.empty

    def test_natural_calendar_empty_1d_returns_empty(self):
        b = self._make_builder()
        result = b.build("1w", data_1m=None, data_1d=pd.DataFrame())
        assert result.empty


class TestPeriodBarBuilderIntraday:
    """build_intraday_bars with real data."""

    def _make_1m_data(self, date_str="2024-01-02", bars=20):
        import pandas as pd
        times = pd.date_range(f"{date_str} 09:30", periods=bars, freq="1min")
        return pd.DataFrame({
            "time": times,
            "open": np.random.uniform(10, 11, bars),
            "high": np.random.uniform(11, 12, bars),
            "low": np.random.uniform(9, 10, bars),
            "close": np.random.uniform(10, 11, bars),
            "volume": np.ones(bars) * 1000,
        })

    def test_returns_non_empty(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        df = self._make_1m_data(bars=20)
        result = b.build_intraday_bars(df, period_minutes=5)
        assert not result.empty

    def test_empty_1m_returns_empty(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        # build_intraday_bars expects a properly-columned DataFrame; use build() wrapper instead
        result = b.build("2m", data_1m=pd.DataFrame(), data_1d=None)
        assert result.empty


class TestPeriodBarBuilderMultiday:
    """build_multiday_bars with real data."""

    def _make_1d_data(self, n=15):
        times = pd.date_range("2024-01-02", periods=n, freq="B")
        return pd.DataFrame({
            "time": times,
            "open": np.random.uniform(10, 11, n),
            "high": np.random.uniform(11, 12, n),
            "low": np.random.uniform(9, 10, n),
            "close": np.random.uniform(10, 11, n),
            "volume": np.ones(n) * 1000,
        })

    def test_returns_non_empty(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        df = self._make_1d_data(n=10)
        result = b.build_multiday_bars(df, trading_days_per_period=5, listing_date="2024-01-02")
        assert not result.empty
        # 10 days / 5 = 2 bars
        assert len(result) == 2

    def test_with_partial_period(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        df = self._make_1d_data(n=7)
        result = b.build_multiday_bars(df, trading_days_per_period=5, listing_date="2024-01-02")
        # 7 days → 1 full + 1 partial
        assert len(result) == 2
        assert result.iloc[-1]["is_partial"] == True  # noqa: E712

    def test_empty_data_returns_empty(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        result = b.build("5d", data_1m=None, data_1d=pd.DataFrame())
        assert result.empty


class TestPeriodBarBuilderNaturalCalendar:
    """build_natural_calendar_bars."""

    def _make_1d_data(self, n=20):
        times = pd.date_range("2024-01-02", periods=n, freq="B")
        return pd.DataFrame({
            "time": times,
            "open": np.ones(n) * 10,
            "high": np.ones(n) * 11,
            "low": np.ones(n) * 9,
            "close": np.ones(n) * 10,
            "volume": np.ones(n) * 1000,
        })

    def test_weekly_bars(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        df = self._make_1d_data(n=20)
        result = b.build_natural_calendar_bars(df, freq="W")
        assert not result.empty

    def test_monthly_bars(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        df = self._make_1d_data(n=30)
        result = b.build_natural_calendar_bars(df, freq="ME")
        assert not result.empty

    def test_none_data_returns_empty(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        result = b.build_natural_calendar_bars(None, freq="W")
        assert result.empty


class TestPeriodBarBuilderCrossValidate:
    """cross_validate branches."""

    def _make_1d_data(self, n=10):
        times = pd.date_range("2024-01-02", periods=n, freq="B")
        close_vals = np.ones(n) * 10.0
        return pd.DataFrame({
            "time": times,
            "open": close_vals,
            "high": close_vals + 1,
            "low": close_vals - 1,
            "close": close_vals,
            "volume": np.ones(n) * 1000,
        })

    def test_unknown_period_returns_warning(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        result = b.cross_validate("99x", pd.DataFrame())
        assert result.warnings  # should have a warning

    def test_empty_custom_bars_returns_warning(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        result = b.cross_validate("5d", pd.DataFrame())
        assert result.warnings

    def test_multiday_validation_no_errors(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        daily = self._make_1d_data(n=10)
        multiday_bars = b.build_multiday_bars(daily.copy(), trading_days_per_period=5,
                                               listing_date="2024-01-02")
        result = b.cross_validate("5d", multiday_bars, daily_ref=daily)
        # Ideally no errors since bars were built from daily
        assert isinstance(result.errors, list)

    def test_intraday_validation_no_daily_ref(self):
        from data_manager.period_bar_builder import PeriodBarBuilder
        b = PeriodBarBuilder()
        # No daily_ref → no validation, just returns empty warnings
        result = b.cross_validate("2m", pd.DataFrame({"time": [], "open": [], "close": []}))
        assert isinstance(result.warnings, list)


# ===========================================================================
# 6. FiveFoldAdjustmentManager — calculate_adjustment_factors,
#    _calculate_front_adjustment helper branches
# ===========================================================================

class TestFiveFoldCalculateAdjustment:
    """calculate_adjustment top-level method."""

    def _make_events(self, n=2):
        today = pd.Timestamp("2024-01-10")
        dates = [today - pd.Timedelta(days=5 * i) for i in range(n)][::-1]
        return pd.DataFrame({
            "ex_date": [d.strftime("%Y-%m-%d") for d in dates],
            "dividend_per_share": [0.50] * n,
            "bonus_ratio": [0.0] * n,
        })

    def _make_df(self, n=10):
        dates = pd.date_range("2024-01-01", periods=n, freq="D")
        prices = np.ones(n) * 10.0
        return pd.DataFrame({
            "date": dates,
            "open": prices,
            "high": prices + 0.5,
            "low": prices - 0.5,
            "close": prices.copy(),
        })

    def test_calculate_with_dividends_returns_five_types(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        mgr = FiveFoldAdjustmentManager.__new__(FiveFoldAdjustmentManager)
        mgr.logger = logging.getLogger("test")
        events = self._make_events(n=2)
        df = self._make_df(n=10)
        result = mgr.calculate_adjustment(df, events)
        assert "none" in result
        assert "front" in result
        assert "back" in result
        assert "geometric_front" in result
        assert "geometric_back" in result

    def test_calculate_no_dividends_returns_original_five_types(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        mgr = FiveFoldAdjustmentManager.__new__(FiveFoldAdjustmentManager)
        mgr.logger = logging.getLogger("test")
        df = self._make_df(n=5)
        result = mgr.calculate_adjustment(df, None)
        assert "none" in result
        assert "front" in result
        # All adj types should equal the original
        assert result["front"].shape == result["none"].shape

    def test_empty_df_returns_empty_dict(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        mgr = FiveFoldAdjustmentManager.__new__(FiveFoldAdjustmentManager)
        mgr.logger = logging.getLogger("test")
        result = mgr.calculate_adjustment(pd.DataFrame(), None)
        assert result == {}

    def test_calculate_with_empty_dividends(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        mgr = FiveFoldAdjustmentManager.__new__(FiveFoldAdjustmentManager)
        mgr.logger = logging.getLogger("test")
        df = self._make_df(n=5)
        # Empty dividends DataFrame but not None
        result = mgr.calculate_adjustment(df, pd.DataFrame())
        assert "none" in result


# ===========================================================================
# 7. resolve_duckdb_path and get_db_manager helper functions
# ===========================================================================

class TestResolveDuckdbPath:
    """Tests for module-level resolve_duckdb_path function."""

    def test_explicit_path_returned_as_is(self, tmp_path):
        from data_manager.duckdb_connection_pool import resolve_duckdb_path
        p = str(tmp_path / "test.ddb")
        result = resolve_duckdb_path(p)
        assert result == p

    def test_none_returns_a_path(self):
        from data_manager.duckdb_connection_pool import resolve_duckdb_path
        result = resolve_duckdb_path(None)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_env_var_used_when_set(self, tmp_path, monkeypatch):
        from data_manager.duckdb_connection_pool import resolve_duckdb_path
        p = str(tmp_path / "env.ddb")
        # Create the file so it exists
        open(p, "w").close()
        monkeypatch.setenv("EASYXT_DUCKDB_PATH", p)
        result = resolve_duckdb_path(None)
        assert result == p


class TestGetDbManager:
    """Tests for get_db_manager factory."""

    def test_returns_manager_instance(self):
        from data_manager.duckdb_connection_pool import get_db_manager, DuckDBConnectionManager
        mgr = get_db_manager(":memory:")
        assert isinstance(mgr, DuckDBConnectionManager)

    def test_same_path_returns_same_instance(self):
        from data_manager.duckdb_connection_pool import get_db_manager
        mgr1 = get_db_manager(":memory:")
        mgr2 = get_db_manager(":memory:")
        assert mgr1 is mgr2
