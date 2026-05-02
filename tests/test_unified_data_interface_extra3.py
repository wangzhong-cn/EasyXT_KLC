#!/usr/bin/env python3
"""
tests/test_unified_data_interface_extra3.py

UnifiedDataInterface 覆盖率补充测试（第三批）
目标：UDI 58.7% → 65%
覆盖范围：
  - schedule_backfill except 路径（lines 302-303, 318-319）
  - _run_backfill_task 各路径（lines 344-452）
  - _check_tushare token+import 分支（lines 535-536）
  - _check_qmt 禁用环境变量路径（lines 550-552）
  - _ensure_tables_exist read_only 路径（lines 715-721）
  - repair_daily_adjustments（lines 1700-1748）
  - purge_stale_derived_periods（lines 1769-1807）
  - _record_ingestion_status WAL 重连路径（lines 1871-1884）
  - get_data_coverage 循环体（lines 2025-2070）
  - _run_quarantine_replay_core 循环体（lines 2104-2200）
"""
from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ─────────────────────────────────────────────────────────────────────────────
# 工厂辅助
# ─────────────────────────────────────────────────────────────────────────────

def _make_udi(**kwargs) -> Any:
    from data_manager.unified_data_interface import UnifiedDataInterface

    defaults = dict(
        duckdb_path=":memory:",
        eager_init=False,
        silent_init=True,
        xtdata_call_mode="direct",
    )
    defaults.update(kwargs)
    return UnifiedDataInterface(**defaults)


def _make_udi_with_mock_con(**kwargs) -> Any:
    """构造 UDI + mock connection（tables already initialized）。"""
    udi = _make_udi(**kwargs)
    udi.con = MagicMock()
    udi.duckdb_available = True
    udi._tables_initialized = True
    return udi


# ══════════════════════════════════════════════════════════════════════════════
# 1. schedule_backfill except 路径（lines 302-303, 318-319）
# ══════════════════════════════════════════════════════════════════════════════

class TestScheduleBackfillExceptPaths:
    """覆盖 schedule_backfill 两个 try/except 块的 except 分支。"""

    def _setup_queued_udi(self):
        udi = _make_udi()
        udi._backfill_enabled = True
        mock_sched = MagicMock()
        mock_sched.schedule.return_value = True
        udi._backfill_scheduler = mock_sched
        return udi

    def test_record_ingestion_raises_warning_logged(self):
        """_record_ingestion_status 抛异常时，walking warning 被记录（line 302-303）。"""
        udi = self._setup_queued_udi()
        with patch.object(
            udi, "_record_ingestion_status", side_effect=Exception("db error")
        ):
            result = udi.schedule_backfill("000001.SZ", "2024-01-01", "2024-01-31")
        # despite exception, queued=True is returned
        assert result is True

    def test_signal_bus_emit_error_silently_ignored(self):
        """signal_bus.emit 抛异常时，异常被 except: pass 忽略（lines 318-319）。"""
        udi = self._setup_queued_udi()
        with patch.object(udi, "_record_ingestion_status"):
            with patch("core.signal_bus.signal_bus") as mock_sb:
                mock_sb.emit.side_effect = Exception("emit failed")
                result = udi.schedule_backfill("000001.SZ", "2024-01-01", "2024-01-31")
        assert result is True


# ══════════════════════════════════════════════════════════════════════════════
# 2. _run_backfill_task 各路径（lines 344-452）
# ══════════════════════════════════════════════════════════════════════════════

class TestRunBackfillTaskPaths:
    """覆盖 _run_backfill_task 的各执行分支。"""

    def test_missing_required_stock_code_returns_false(self):
        """task 中缺少 stock_code → 直接返回 False（line ~350）。"""
        udi = _make_udi()
        result = udi._run_backfill_task(
            {"start_date": "2024-01-01", "end_date": "2024-12-31", "period": "1d"}
        )
        assert result is False

    def test_missing_required_start_date_returns_false(self):
        """task 中缺少 start_date → 直接返回 False。"""
        udi = _make_udi()
        result = udi._run_backfill_task(
            {"stock_code": "000001.SZ", "end_date": "2024-12-31"}
        )
        assert result is False

    def test_empty_data_both_sources_records_failed_returns_false(self):
        """QMT 不可用且 AKShare 返回 None → 记录 failed、返回 False（lines 391-419）。"""
        udi = _make_udi()

        mock_worker = MagicMock()
        mock_worker.connect.return_value = True
        mock_worker.qmt_available = False
        mock_worker._read_from_akshare.return_value = None  # no data

        with patch(
            "data_manager.unified_data_interface.UnifiedDataInterface",
            return_value=mock_worker,
        ):
            result = udi._run_backfill_task(
                {
                    "stock_code": "000001.SZ",
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-31",
                    "period": "1d",
                }
            )

        assert result is False
        # _record_ingestion_status 至少被调用了两次（running + failed）
        assert mock_worker._record_ingestion_status.call_count >= 1
        mock_worker.close.assert_called()

    def test_empty_dataframe_from_akshare_returns_false(self):
        """AKShare 返回空 DataFrame → 同样走到 failed 路径。"""
        udi = _make_udi()
        mock_worker = MagicMock()
        mock_worker.connect.return_value = True
        mock_worker.qmt_available = False
        mock_worker._read_from_akshare.return_value = pd.DataFrame()  # empty

        with patch(
            "data_manager.unified_data_interface.UnifiedDataInterface",
            return_value=mock_worker,
        ):
            result = udi._run_backfill_task(
                {
                    "stock_code": "000001.SZ",
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-31",
                }
            )
        assert result is False

    def test_exception_during_worker_execution_returns_false(self):
        """worker.connect() 抛异常 → 进入 except 分支，返回 False（lines 427-452）。"""
        udi = _make_udi()
        mock_worker = MagicMock()
        mock_worker.connect.side_effect = Exception("connection failed")

        with patch(
            "data_manager.unified_data_interface.UnifiedDataInterface",
            return_value=mock_worker,
        ):
            result = udi._run_backfill_task(
                {
                    "stock_code": "000001.SZ",
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-31",
                }
            )

        assert result is False
        # finally block：close() 必须被调用
        mock_worker.close.assert_called()

    def test_success_path_saves_data_and_returns_true(self):
        """AKShare 返回有效 DataFrame → _save_to_duckdb 被调用，返回 True（lines 418+）。"""
        udi = _make_udi()
        mock_worker = MagicMock()
        mock_worker.connect.return_value = True
        mock_worker.qmt_available = False
        mock_worker._read_from_akshare.return_value = pd.DataFrame(
            {"close": [10.0, 11.0], "date": ["2024-01-01", "2024-01-02"]}
        )

        with patch(
            "data_manager.unified_data_interface.UnifiedDataInterface",
            return_value=mock_worker,
        ):
            result = udi._run_backfill_task(
                {
                    "stock_code": "000001.SZ",
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-31",
                    "period": "1d",
                }
            )

        assert result is True
        mock_worker._save_to_duckdb.assert_called_once()
        mock_worker.close.assert_called()

    def test_exception_handling_records_ingestion_failed(self):
        """exception 路径也尝试记录 ingestion failed（inner try in except block）。"""
        udi = _make_udi()
        mock_worker = MagicMock()
        mock_worker.connect.return_value = True
        mock_worker.qmt_available = False
        mock_worker._read_from_akshare.return_value = pd.DataFrame(
            {"close": [10.0]}
        )
        # _save_to_duckdb raises to trigger exception path
        mock_worker._save_to_duckdb.side_effect = Exception("save failed")

        with patch(
            "data_manager.unified_data_interface.UnifiedDataInterface",
            return_value=mock_worker,
        ):
            result = udi._run_backfill_task(
                {
                    "stock_code": "000001.SZ",
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-31",
                }
            )

        assert result is False
        mock_worker.close.assert_called()


# ══════════════════════════════════════════════════════════════════════════════
# 3. _check_tushare token 路径（lines 535-536）
# ══════════════════════════════════════════════════════════════════════════════

class TestCheckTushareWithToken:
    """_check_tushare：需要 _tushare_token 才能进入 import 块。"""

    def test_check_tushare_success_with_valid_token(self):
        """token 已配置且 tushare 可导入 → tushare_available = True（line 535）。"""
        udi = _make_udi()
        udi._tushare_token = "test_token_abc"
        udi._tushare_checked = False
        udi.tushare_available = False

        with patch.dict(sys.modules, {"tushare": MagicMock()}):
            udi._check_tushare()

        assert udi.tushare_available is True

    def test_check_tushare_import_error_sets_unavailable(self):
        """token 已配置但 tushare 导入失败 → tushare_available = False（line 536）。"""
        udi = _make_udi()
        udi._tushare_token = "test_token_abc"
        udi._tushare_checked = False
        udi.tushare_available = True  # start True, should be set False

        with patch.dict(sys.modules, {"tushare": None}):
            udi._check_tushare()

        assert udi.tushare_available is False

    def test_check_tushare_no_token_skips_import(self):
        """无 token → 直接标记 unavailable，不尝试 import。"""
        udi = _make_udi()
        udi._tushare_token = ""
        udi._tushare_checked = False
        udi._check_tushare()
        assert udi.tushare_available is False

    def test_check_tushare_already_checked_returns_early(self):
        """_tushare_checked=True → 直接 return，不更新状态。"""
        udi = _make_udi()
        udi._tushare_checked = True
        udi.tushare_available = True
        udi._tushare_token = ""  # would normally set False
        udi._check_tushare()
        assert udi.tushare_available is True  # unchanged


# ══════════════════════════════════════════════════════════════════════════════
# 4. _check_qmt 禁用环境变量路径（lines 550-552）
# ══════════════════════════════════════════════════════════════════════════════

class TestCheckQmtDisabledPath:
    def test_qmt_disabled_via_env_sets_unavailable(self, monkeypatch):
        """EASYXT_ENABLE_QMT_ONLINE=0 → qmt_available=False, 早返回（lines 550-552）。"""
        monkeypatch.setenv("EASYXT_ENABLE_QMT_ONLINE", "0")
        udi = _make_udi()
        udi._qmt_checked = False
        udi._check_qmt()
        assert udi.qmt_available is False

    def test_qmt_disabled_via_false_string(self, monkeypatch):
        """EASYXT_ENABLE_QMT_ONLINE=false → 同样禁用。"""
        monkeypatch.setenv("EASYXT_ENABLE_QMT_ONLINE", "false")
        udi = _make_udi()
        udi._qmt_checked = False
        udi._check_qmt()
        assert udi.qmt_available is False

    def test_qmt_already_checked_returns_early(self):
        """_qmt_checked=True → return immediately, no state change."""
        udi = _make_udi()
        udi._qmt_checked = True
        udi.qmt_available = True
        udi._check_qmt()
        assert udi.qmt_available is True


# ══════════════════════════════════════════════════════════════════════════════
# 5. _ensure_tables_exist read_only 路径（lines 715-721）
# ══════════════════════════════════════════════════════════════════════════════

class TestEnsureTablesReadOnlyPath:
    def test_read_only_marks_initialized_without_creating_tables(self):
        """_read_only_connection=True → 标记 initialized，不执行 CREATE TABLE（lines 715-721）。"""
        udi = _make_udi()
        mock_con = MagicMock()
        udi.con = mock_con
        udi._read_only_connection = True
        udi._tables_initialized = False

        udi._ensure_tables_exist()

        assert udi._tables_initialized is True
        # 不应调用 con.execute 创建任何表
        mock_con.execute.assert_not_called()

    def test_already_initialized_returns_without_executing(self):
        """_tables_initialized=True → 直接 return，不调用 execute。"""
        udi = _make_udi()
        mock_con = MagicMock()
        udi.con = mock_con
        udi._tables_initialized = True

        udi._ensure_tables_exist()

        mock_con.execute.assert_not_called()

    def test_no_con_returns_without_error(self):
        """con=None → 直接 return。"""
        udi = _make_udi()
        udi.con = None
        udi._tables_initialized = False
        udi._ensure_tables_exist()  # should not raise


# ══════════════════════════════════════════════════════════════════════════════
# 6. repair_daily_adjustments（lines 1700-1748）
# ══════════════════════════════════════════════════════════════════════════════

class TestRepairDailyAdjustments:
    def test_no_con_returns_error_dict(self):
        """con=None → 返回 {'_error': ...}（lines 1706-1707）。"""
        udi = _make_udi()
        udi.duckdb_available = True
        udi.con = None
        result = udi.repair_daily_adjustments()
        assert "_error" in result

    def test_auto_detect_no_broken_stocks_returns_empty_dict(self):
        """自动检测无需修复的股票 → 返回 {}（early return path）。"""
        udi = _make_udi_with_mock_con()
        # FIRST call is detect query, second call returns empty stock list
        udi.con.execute.return_value.fetchall.return_value = []  # no broken stocks
        result = udi.repair_daily_adjustments(stock_codes=None)
        assert result == {}

    def test_explicit_codes_repair_success(self):
        """显式提供 stock_codes → adjustment_manager._try_repair_adjustment 被调用。"""
        udi = _make_udi_with_mock_con()
        mock_adj = MagicMock()
        udi.adjustment_manager = mock_adj

        result = udi.repair_daily_adjustments(stock_codes=["000001.SZ"])

        assert "000001.SZ" in result
        assert result["000001.SZ"] == "repaired"
        mock_adj._try_repair_adjustment.assert_called_once_with(
            "000001.SZ", "1990-01-01", "2099-12-31"
        )

    def test_explicit_codes_repair_error_captured(self):
        """修复时抛异常 → 结果为 'error:...' 字符串。"""
        udi = _make_udi_with_mock_con()
        mock_adj = MagicMock()
        mock_adj._try_repair_adjustment.side_effect = Exception("network error")
        udi.adjustment_manager = mock_adj

        result = udi.repair_daily_adjustments(stock_codes=["000001.SZ"])

        assert "000001.SZ" in result
        assert result["000001.SZ"].startswith("error:")

    def test_adjust_manager_none_triggers_ensure_and_returns_error(self):
        """adjustment_manager=None → _ensure_adjustment_manager() 被调用，若仍 None 则返回 error。"""
        udi = _make_udi_with_mock_con()
        udi.adjustment_manager = None
        with patch.object(udi, "_ensure_adjustment_manager"):
            # After ensure, still None
            result = udi.repair_daily_adjustments(stock_codes=["000001.SZ"])
        # _ensure_adjustment_manager was called but adjustment_manager stays None
        assert "_error" in result

    def test_auto_detect_with_rows_calls_repair(self):
        """自动检测到需修复的股票 → 调用修复方法。"""
        udi = _make_udi_with_mock_con()
        mock_adj = MagicMock()
        udi.adjustment_manager = mock_adj
        udi.con.execute.return_value.fetchall.return_value = [
            ("000001.SZ",),
            ("000002.SZ",),
        ]

        result = udi.repair_daily_adjustments(stock_codes=None)

        assert "000001.SZ" in result
        assert "000002.SZ" in result
        assert mock_adj._try_repair_adjustment.call_count == 2


# ══════════════════════════════════════════════════════════════════════════════
# 7. purge_stale_derived_periods（lines 1769-1807）
# ══════════════════════════════════════════════════════════════════════════════

class TestPurgeStale:
    def test_no_con_returns_zero(self):
        """con=None → 返回 0（early return）。"""
        udi = _make_udi()
        udi.duckdb_available = True
        udi.con = None
        result = udi.purge_stale_derived_periods()
        assert result == 0

    def test_no_stale_records_returns_zero(self):
        """count=0 → 不删除，返回 0。"""
        udi = _make_udi_with_mock_con()
        udi.con.execute.return_value.fetchone.return_value = (0,)
        result = udi.purge_stale_derived_periods()
        assert result == 0

    def test_stale_records_deleted_returns_count(self):
        """count=5 → 执行 DELETE，返回 5。"""
        udi = _make_udi_with_mock_con()
        udi.con.execute.return_value.fetchone.return_value = (5,)
        result = udi.purge_stale_derived_periods()
        assert result == 5

    def test_exception_returns_zero(self):
        """execute() 抛异常 → 捕获，返回 0。"""
        udi = _make_udi_with_mock_con()
        udi.con.execute.side_effect = Exception("table error")
        result = udi.purge_stale_derived_periods()
        assert result == 0

    def test_fetchone_returns_none_returns_zero(self):
        """fetchone() 返回 None → count=0，返回 0。"""
        udi = _make_udi_with_mock_con()
        udi.con.execute.return_value.fetchone.return_value = None
        result = udi.purge_stale_derived_periods()
        assert result == 0


# ══════════════════════════════════════════════════════════════════════════════
# 8. _record_ingestion_status WAL 重连路径（lines 1871-1884）
# ══════════════════════════════════════════════════════════════════════════════

class TestRecordIngestionStatusWALRetry:
    def test_wal_error_reconnects_and_retries_successfully(self):
        """con.execute 第一次抛 WAL 锁错误 → 重连后第二次调用成功（lines 1871-1884）。"""
        udi = _make_udi()
        mock_con = MagicMock()
        call_count = [0]

        def execute_side(sql, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("cannot open file .wal: file is locked")
            return MagicMock()

        mock_con.execute.side_effect = execute_side
        udi.con = mock_con
        udi.duckdb_available = True

        with patch.object(udi, "_close_duckdb_connection") as mock_close, patch.object(
            udi, "connect", return_value=True
        ) as mock_connect:
            udi._record_ingestion_status(
                stock_code="000001.SZ",
                period="1d",
                start_date="2024-01-01",
                end_date="2024-12-31",
                source="akshare",
                status="success",
                record_count=100,
                error_message=None,
            )

        mock_close.assert_called_once()
        mock_connect.assert_called_once()
        assert call_count[0] == 2

    def test_wal_error_retry_also_fails_logs_warning(self):
        """WAL 重连后第二次调用仍然失败 → warning 被记录，不抛异常。"""
        udi = _make_udi()
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception(
            "failed to commit .wal: cannot open file"
        )
        udi.con = mock_con
        udi.duckdb_available = True

        with patch.object(udi, "_close_duckdb_connection"), patch.object(
            udi, "connect", return_value=True
        ):
            # Should not raise
            udi._record_ingestion_status(
                stock_code="000001.SZ",
                period="1d",
                start_date="2024-01-01",
                end_date="2024-12-31",
                source="akshare",
                status="success",
                record_count=10,
                error_message=None,
            )

    def test_non_wal_error_only_logs_warning(self):
        """非 WAL 错误 → 只记录 warning，不重连。"""
        udi = _make_udi()
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("unrelated db error")
        udi.con = mock_con
        udi.duckdb_available = True

        with patch.object(udi, "_close_duckdb_connection") as mock_close:
            udi._record_ingestion_status(
                stock_code="000001.SZ",
                period="1d",
                start_date="2024-01-01",
                end_date="2024-12-31",
                source="akshare",
                status="success",
                record_count=10,
                error_message=None,
            )
        # _close_duckdb_connection NOT called for non-wal errors
        mock_close.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# 9. get_data_coverage 循环体（lines 2025-2070）
# ══════════════════════════════════════════════════════════════════════════════

class TestGetDataCoverageLoopBody:
    def test_loop_body_builds_pivot_from_rows(self):
        """fetchdf 返回真实数据行 → 循环体执行，构建 pivot DataFrame（lines 2025-2070）。"""
        udi = _make_udi_with_mock_con()
        sample_df = pd.DataFrame(
            {
                "stock_code": ["000001.SZ", "000002.SZ"],
                "min_dt": ["2024-01-01", "2024-01-01"],
                "max_dt": ["2024-12-31", "2024-12-31"],
                "cnt": [240, 235],
            }
        )
        udi.con.execute.return_value.fetchdf.return_value = sample_df

        result = udi.get_data_coverage(periods=["1d"])

        assert not result.empty
        assert "000001.SZ" in result.index
        assert "000002.SZ" in result.index

    def test_with_stock_codes_filter_includes_placeholders(self):
        """传入 stock_codes → WHERE IN 子句被构建，参数正确传递。"""
        udi = _make_udi_with_mock_con()
        sample_df = pd.DataFrame(
            {
                "stock_code": ["000001.SZ"],
                "min_dt": ["2024-01-01"],
                "max_dt": ["2024-12-31"],
                "cnt": [240],
            }
        )
        udi.con.execute.return_value.fetchdf.return_value = sample_df

        result = udi.get_data_coverage(
            periods=["1d"], stock_codes=["000001.SZ"]
        )

        assert not result.empty
        # Verify execute was called with params containing the stock_code
        call_args = udi.con.execute.call_args
        assert call_args is not None
        params = call_args[0][1]
        assert "000001.SZ" in params

    def test_query_exception_period_skipped_continues(self):
        """某周期查询抛异常 → warning 记录，继续下一周期（continue branch）。"""
        udi = _make_udi_with_mock_con()
        sample_df = pd.DataFrame(
            {
                "stock_code": ["000001.SZ"],
                "min_dt": ["2024-01-01"],
                "max_dt": ["2024-12-31"],
                "cnt": [100],
            }
        )
        call_count = [0]

        def execute_side(sql, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("table error")
            m = MagicMock()
            m.fetchdf.return_value = sample_df
            return m

        udi.con.execute.side_effect = execute_side
        # First period fails, second succeeds
        result = udi.get_data_coverage(periods=["1m", "1d"])
        # Should not raise; result may be empty or have 1d data
        assert isinstance(result, pd.DataFrame)

    def test_no_rows_returned_returns_empty_dataframe(self):
        """所有周期均返回空表 → 返回 empty DataFrame。"""
        udi = _make_udi_with_mock_con()
        udi.con.execute.return_value.fetchdf.return_value = pd.DataFrame()
        result = udi.get_data_coverage(periods=["1d", "1m"])
        assert result.empty


# ══════════════════════════════════════════════════════════════════════════════
# 10. _run_quarantine_replay_core 循环体（lines 2104-2200）
# ══════════════════════════════════════════════════════════════════════════════

class TestQuarantineReplayCoreWithRows:
    """测试循环体内 ok/fail/dead_letter 三条路径。"""

    def _make_udi_for_replay(self):
        udi = _make_udi_with_mock_con()
        return udi

    def test_row_processed_success_updates_resolved(self):
        """get_stock_data 返回非空 DataFrame → succeeded+1，UPDATE resolved（lines 2115-2126）。"""
        udi = self._make_udi_for_replay()
        udi.con.execute.return_value.fetchall.return_value = [
            ("qid1", "000001.SZ", "1d", "2024-01-01", "2024-12-31", 0, "stale"),
        ]
        sample_df = pd.DataFrame({"close": [10.0]})

        with patch.object(udi, "get_stock_data", return_value=sample_df):
            result = udi._run_quarantine_replay_core(limit=1, max_retries=3)

        assert result["processed"] == 1
        assert result["succeeded"] == 1
        assert result["failed"] == 0

    def test_row_processed_failure_increments_retry(self):
        """get_stock_data 返回空 DataFrame → failed+1，UPDATE to 'failed'（lines 2128-2160）。"""
        udi = self._make_udi_for_replay()
        udi.con.execute.return_value.fetchall.return_value = [
            ("qid1", "000001.SZ", "1d", "2024-01-01", "2024-12-31", 0, "stale"),
        ]

        with patch.object(udi, "get_stock_data", return_value=pd.DataFrame()):
            result = udi._run_quarantine_replay_core(limit=1, max_retries=3)

        assert result["processed"] == 1
        assert result["failed"] == 1
        assert result["succeeded"] == 0
        assert result["dead_letter"] == 0

    def test_row_becomes_dead_letter_at_max_retries(self):
        """retry_count=2 + max_retries=3 → next_retry=3 >= 3 → dead_letter（lines 2156-2200）。"""
        udi = self._make_udi_for_replay()
        # retry_count=2, so next_retry = 3, which equals max_retries=3 → dead_letter
        udi.con.execute.return_value.fetchall.return_value = [
            ("qid1", "000001.SZ", "1d", "2024-01-01", "2024-12-31", 2, "stale"),
        ]

        with patch.object(udi, "get_stock_data", return_value=pd.DataFrame()):
            with patch.object(udi, "_record_data_quality_incident") as mock_incident:
                with patch.object(udi, "_emit_data_quality_alert"):
                    result = udi._run_quarantine_replay_core(limit=1, max_retries=3)

        assert result["dead_letter"] == 1
        mock_incident.assert_called_once()

    def test_get_stock_data_exception_treated_as_failure(self):
        """get_stock_data 抛异常 → ok=False，进入 failed 路径。"""
        udi = self._make_udi_for_replay()
        udi.con.execute.return_value.fetchall.return_value = [
            ("qid1", "000001.SZ", "1d", "2024-01-01", "2024-12-31", 0, "stale"),
        ]

        with patch.object(
            udi, "get_stock_data", side_effect=Exception("network error")
        ):
            result = udi._run_quarantine_replay_core(limit=1, max_retries=3)

        assert result["processed"] == 1
        assert result["failed"] == 1

    def test_reason_regex_filters_rows(self):
        """reason_regex 指定后只处理匹配行。"""
        udi = self._make_udi_for_replay()
        # One row matches regex "stale", one doesn't match "corruption"
        udi.con.execute.return_value.fetchall.return_value = [
            ("qid1", "000001.SZ", "1d", "2024-01-01", "2024-12-31", 0, "stale"),
            ("qid2", "000002.SZ", "1d", "2024-01-01", "2024-12-31", 0, "corruption"),
        ]
        sample_df = pd.DataFrame({"close": [10.0]})

        with patch.object(udi, "get_stock_data", return_value=sample_df):
            result = udi._run_quarantine_replay_core(
                limit=10, max_retries=3, reason_regex="stale"
            )

        # Only "stale" row processed
        assert result["processed"] == 1

    def test_empty_queue_returns_zeros(self):
        """空队列 → 全 0（已有测试，补充测试确认性）。"""
        udi = self._make_udi_for_replay()
        udi.con.execute.return_value.fetchall.return_value = []
        result = udi._run_quarantine_replay_core()
        assert result == {"processed": 0, "succeeded": 0, "failed": 0, "dead_letter": 0}

    def test_tick_period_mapped_to_1m(self):
        """period='tick' 的行 → target_period 映射为 '1m'。"""
        udi = self._make_udi_for_replay()
        udi.con.execute.return_value.fetchall.return_value = [
            ("qid1", "000001.SZ", "tick", "2024-01-01", "2024-01-01", 0, "stale"),
        ]
        captured_periods = []
        orig_get = udi.get_stock_data

        def mock_get_stock(stock_code, start_date, end_date, period, **kwargs):
            captured_periods.append(period)
            return pd.DataFrame({"close": [10.0]})

        with patch.object(udi, "get_stock_data", side_effect=mock_get_stock):
            udi._run_quarantine_replay_core(limit=5, max_retries=3)

        assert captured_periods == ["1m"]


# ══════════════════════════════════════════════════════════════════════════════
# 11. _compute_data_lineage（lines 1793-1825）
# ══════════════════════════════════════════════════════════════════════════════

class TestComputeDataLineage:
    def test_normal_dataframe_returns_hash_and_event_time(self):
        """正常 DataFrame → 返回 (raw_hash, source_event_time)。"""
        udi = _make_udi()
        df = pd.DataFrame(
            {"date": pd.to_datetime(["2024-01-01", "2024-01-02"]), "close": [10.0, 11.0]}
        )
        raw_hash, event_time = udi._compute_data_lineage(df)
        assert isinstance(raw_hash, str)
        assert len(raw_hash) == 16
        assert event_time is not None

    def test_datetime_column_used_for_event_time(self):
        """DataFrame 有 datetime 列 → source_event_time 来自该列 max()。"""
        udi = _make_udi()
        df = pd.DataFrame(
            {
                "datetime": pd.to_datetime(
                    ["2024-01-01 09:30:00", "2024-01-02 09:30:00"]
                ),
                "close": [10.0, 11.0],
            }
        )
        raw_hash, event_time = udi._compute_data_lineage(df)
        assert event_time is not None

    def test_empty_dataframe_returns_error_hash(self):
        """空 DataFrame → raw_hash='error' 或正常哈希（DataFrame.to_csv 不一定失败），event_time=None。"""
        udi = _make_udi()
        df = pd.DataFrame()
        raw_hash, event_time = udi._compute_data_lineage(df)
        assert isinstance(raw_hash, str)
        assert event_time is None

    def test_to_csv_failure_returns_error_hash(self):
        """to_csv() 抛异常 → raw_hash='error'。"""
        udi = _make_udi()
        mock_df = MagicMock()
        mock_df.to_csv.side_effect = Exception("serialization fail")
        mock_df.columns = []
        mock_df.empty = True
        raw_hash, _ = udi._compute_data_lineage(mock_df)
        assert raw_hash == "error"


# ══════════════════════════════════════════════════════════════════════════════
# 12. _cb_allow / _cb_on_failure / _cb_on_success 路径
# ══════════════════════════════════════════════════════════════════════════════

class TestCircuitBreakerPaths:
    def test_cb_allow_open_but_cooled_down_closes(self):
        """熔断器打开后超过冷却时间 → 关闭，allow=True。"""
        import time

        udi = _make_udi()
        udi._cb_state["open"] = True
        udi._cb_state["opened_at"] = time.perf_counter() - 999.0  # 已超时
        udi._cb_state["cooldown_s"] = 1.0

        result = udi._cb_allow()

        assert result is True
        assert udi._cb_state["open"] is False

    def test_cb_allow_open_not_cooled_returns_false(self):
        """熔断器打开且未到冷却时间 → allow=False。"""
        import time

        udi = _make_udi()
        udi._cb_state["open"] = True
        udi._cb_state["opened_at"] = time.perf_counter()  # 刚刚打开
        udi._cb_state["cooldown_s"] = 9999.0

        result = udi._cb_allow()

        assert result is False

    def test_cb_on_failure_increments_fail_count(self):
        """_cb_on_failure → fail_count 递增。"""
        udi = _make_udi()
        udi._cb_state["fail_count"] = 0
        udi._cb_state["fail_threshold"] = 10
        udi._cb_on_failure()
        assert udi._cb_state["fail_count"] == 1

    def test_cb_on_failure_opens_breaker_at_threshold(self):
        """fail_count 达到阈值 → 熔断器打开。"""
        udi = _make_udi()
        udi._cb_state["fail_count"] = 4
        udi._cb_state["fail_threshold"] = 5
        udi._cb_state["base_s"] = 3.0
        udi._cb_state["max_s"] = 300.0
        udi._cb_on_failure()
        assert udi._cb_state["open"] is True

    def test_cb_on_success_resets_state(self):
        """_cb_on_success → 清零 fail_count，关闭熔断器。"""
        udi = _make_udi()
        udi._cb_state["open"] = True
        udi._cb_state["fail_count"] = 5
        udi._cb_on_success()
        assert udi._cb_state["open"] is False
        assert udi._cb_state["fail_count"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# 13. eager_init 分支（lines 234-238）
# ══════════════════════════════════════════════════════════════════════════════

class TestEagerInitPaths:
    def test_eager_init_calls_all_checks(self):
        """eager_init=True → 调用 _ensure_adjustment_manager + 所有 _check_* 方法（lines 234-238）。"""
        from data_manager.unified_data_interface import UnifiedDataInterface

        with patch.object(UnifiedDataInterface, "_ensure_adjustment_manager") as m_adj, \
             patch.object(UnifiedDataInterface, "_check_duckdb") as m_ddb, \
             patch.object(UnifiedDataInterface, "_check_qmt") as m_qmt, \
             patch.object(UnifiedDataInterface, "_check_akshare") as m_aks, \
             patch.object(UnifiedDataInterface, "_check_tushare") as m_ts:
            udi = UnifiedDataInterface(
                duckdb_path=":memory:", eager_init=True, silent_init=True
            )
        m_adj.assert_called_once()
        m_ddb.assert_called_once()
        m_qmt.assert_called_once()
        m_aks.assert_called_once()
        m_ts.assert_called_once()


# ══════════════════════════════════════════════════════════════════════════════
# 14. _run_backfill_task 补充分支（lines 367-368, 396-399, 404-405, 444-445, 451-452）
# ══════════════════════════════════════════════════════════════════════════════

class TestRunBackfillTaskExtraPaths:
    """覆盖 _run_backfill_task 中尚未被 TestRunBackfillTaskPaths 覆盖的分支。"""

    def test_emit_backfill_event_exception_handled(self):
        """signal_bus.emit 在 _emit_backfill_event 中抛异常 → except: pass（lines 367-368）。"""
        udi = _make_udi()
        mock_worker = MagicMock()
        mock_worker.connect.return_value = True
        mock_worker.qmt_available = False
        mock_worker._read_from_akshare.return_value = None

        with patch(
            "data_manager.unified_data_interface.UnifiedDataInterface",
            return_value=mock_worker,
        ):
            with patch("core.signal_bus.signal_bus") as mock_sb:
                mock_sb.emit.side_effect = Exception("signal bus failed")
                result = udi._run_backfill_task(
                    {
                        "stock_code": "000001.SZ",
                        "start_date": "2024-01-01",
                        "end_date": "2024-12-31",
                    }
                )
        # exception in _emit_backfill_event is silently ignored
        assert result is False

    def test_qmt_available_read_raises_falls_back_to_akshare(self):
        """qmt_available=True 且 _read_from_qmt 抛异常 → data=None，走 AKShare 路径（lines 396-399）。"""
        udi = _make_udi()
        mock_worker = MagicMock()
        mock_worker.connect.return_value = True
        mock_worker.qmt_available = True  # QMT enabled
        mock_worker._read_from_qmt.side_effect = Exception("qmt disconnected")
        mock_worker._read_from_akshare.return_value = pd.DataFrame(
            {"close": [10.0]}
        )  # AKShare succeeds

        with patch(
            "data_manager.unified_data_interface.UnifiedDataInterface",
            return_value=mock_worker,
        ):
            result = udi._run_backfill_task(
                {
                    "stock_code": "000001.SZ",
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-31",
                }
            )
        # QMT failed but AKShare succeeded → True
        assert result is True
        mock_worker._read_from_qmt.assert_called_once()

    def test_akshare_read_raises_exception_caught_returns_false(self):
        """_read_from_akshare 抛异常 → except: data=None → failed path（lines 404-405）。"""
        udi = _make_udi()
        mock_worker = MagicMock()
        mock_worker.connect.return_value = True
        mock_worker.qmt_available = False
        mock_worker._read_from_akshare.side_effect = Exception("network timeout")

        with patch(
            "data_manager.unified_data_interface.UnifiedDataInterface",
            return_value=mock_worker,
        ):
            result = udi._run_backfill_task(
                {
                    "stock_code": "000001.SZ",
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-31",
                }
            )
        assert result is False
        mock_worker._read_from_akshare.assert_called_once()

    def test_outer_except_inner_ingestion_status_raises_covered(self):
        """外层 except 中的 _record_ingestion_status 也抛异常 → inner except 覆盖（lines 444-445）。"""
        udi = _make_udi()
        mock_worker = MagicMock()
        mock_worker.connect.return_value = True
        mock_worker.qmt_available = False
        # All calls to _record_ingestion_status raise → triggers outer except + inner except
        mock_worker._record_ingestion_status.side_effect = Exception("db write failed")

        with patch(
            "data_manager.unified_data_interface.UnifiedDataInterface",
            return_value=mock_worker,
        ):
            result = udi._run_backfill_task(
                {
                    "stock_code": "000001.SZ",
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-31",
                }
            )
        # First call raises → outer except → second call also raises → inner except (444-445)
        assert result is False

    def test_worker_close_raises_in_finally_is_silently_caught(self):
        """worker.close() 在 finally 中抛异常 → except: pass 覆盖（lines 451-452）。"""
        udi = _make_udi()
        mock_worker = MagicMock()
        mock_worker.connect.return_value = True
        mock_worker.qmt_available = False
        mock_worker._read_from_akshare.return_value = None
        mock_worker.close.side_effect = Exception("close failed")

        with patch(
            "data_manager.unified_data_interface.UnifiedDataInterface",
            return_value=mock_worker,
        ):
            result = udi._run_backfill_task(
                {
                    "stock_code": "000001.SZ",
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-31",
                }
            )
        # close exception caught silently, function still returns False
        assert result is False


# ══════════════════════════════════════════════════════════════════════════════
# 15. _check_qmt import error 路径（lines 564-566）
# ══════════════════════════════════════════════════════════════════════════════

class TestCheckQmtImportError:
    def test_xtquant_not_importable_sets_qmt_unavailable(self, monkeypatch):
        """xtquant 不可导入（ImportError）→ qmt_available=False（lines 564-566）。"""
        monkeypatch.setenv("EASYXT_ENABLE_QMT_ONLINE", "1")
        udi = _make_udi()
        udi._qmt_checked = False

        with patch.dict(sys.modules, {"xtquant": None, "xtquant.xtdata": None}):
            udi._check_qmt()

        assert udi.qmt_available is False

    def test_xtquant_importable_sets_qmt_available(self, monkeypatch):
        """xtquant 可成功导入且 xtdata 可用 → qmt_available=True（line 562）。"""
        monkeypatch.setenv("EASYXT_ENABLE_QMT_ONLINE", "1")
        udi = _make_udi()
        udi._qmt_checked = False

        mock_xtquant = MagicMock()
        mock_xtquant.__path__ = []
        mock_xtdata = MagicMock()
        with patch.dict(sys.modules, {"xtquant": mock_xtquant, "xtquant.xtdata": mock_xtdata}):
            udi._check_qmt()

        assert udi.qmt_available is True


# ══════════════════════════════════════════════════════════════════════════════
# 16. _find_qmt_python_root 路径（lines 621-635）
# ══════════════════════════════════════════════════════════════════════════════

class TestFindQmtPythonRoot:
    def test_invalid_root_returns_none(self):
        """root 不存在或为空 → 立即返回 None。"""
        udi = _make_udi()
        result = udi._find_qmt_python_root("")
        assert result is None

        with patch("os.path.isdir", return_value=False):
            result = udi._find_qmt_python_root("/nonexistent")
        assert result is None

    def test_xtquant_basename_with_init_returns_parent(self):
        """root basename == 'xtquant' 且有 __init__.py → 返回父目录（lines 621-623）。"""
        udi = _make_udi()
        with patch("os.path.isdir", return_value=True), \
             patch("os.path.exists", return_value=True):
            result = udi._find_qmt_python_root("/fake/xtquant")
        assert result == "/fake"

    def test_walk_finds_pyd_file(self):
        """os.walk 找到 xtpythonclient.pyd → 返回该目录（line 627 区域）。"""
        udi = _make_udi()
        fake_root = "/fake/root"
        with patch("os.path.isdir", return_value=True), \
             patch("os.path.basename", return_value="notxtquant"), \
             patch("os.walk", return_value=[(fake_root, [], ["xtpythonclient.pyd"])]):
            result = udi._find_qmt_python_root(fake_root)
        assert result == fake_root

    def test_walk_finds_xtquant_dir_with_init(self):
        """os.walk 找到 xtquant/ 子目录且有 __init__.py → 返回父目录（lines 628-635）。"""
        udi = _make_udi()
        fake_root = "/fake/root"
        with patch("os.path.isdir", return_value=True), \
             patch("os.path.basename", return_value="notxtquant"), \
             patch("os.walk", return_value=[(fake_root, ["xtquant"], [])]), \
             patch("os.path.join", side_effect=lambda *a: "/".join(str(x) for x in a)), \
             patch("os.path.exists", return_value=True):
            result = udi._find_qmt_python_root(fake_root)
        assert result == fake_root

    def test_walk_prunes_at_depth_limit(self):
        """目录深度 >= 6 时截断 dirnames（lines 633-635 区域）。"""
        import os as _os
        udi = _make_udi()
        sep = _os.sep
        # Build paths with actual os.sep so count(os.sep) works correctly
        fake_root = sep.join(["a", "b", "c"])         # root_depth = 2
        deep_path = sep.join(["a", "b", "c", "d", "e", "f", "g", "h", "i"])  # count = 8, diff=6
        dirnames = ["subdir"]
        with patch("os.path.isdir", return_value=True), \
             patch("os.path.basename", return_value="notxtquant"), \
             patch("os.walk", return_value=[(deep_path, dirnames, [])]):
            result = udi._find_qmt_python_root(fake_root)
        # No pyd/xtquant found → None
        assert result is None
        # dirnames was cleared (pruned) because depth diff == 6
        assert dirnames == []


# ══════════════════════════════════════════════════════════════════════════════
# 17. _ensure_qmt_paths config import 路径（lines 580-581）
# ══════════════════════════════════════════════════════════════════════════════

class TestEnsureQmtPathsConfigError:
    def test_config_import_error_sets_config_obj_none(self, monkeypatch):
        """easy_xt.config 导入失败 → config_obj=None（lines 580-581）。"""
        # Clear QMT path env vars so no candidates are found via env
        monkeypatch.delenv("XTQUANT_PATH", raising=False)
        monkeypatch.delenv("QMT_PATH", raising=False)
        udi = _make_udi()
        with patch.dict(sys.modules, {"easy_xt": None, "easy_xt.config": None}):
            result = udi._ensure_qmt_paths()
        # No candidates (no env var, no config) → None
        assert result is None
