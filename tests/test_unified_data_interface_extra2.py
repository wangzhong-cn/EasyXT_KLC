#!/usr/bin/env python3
"""
tests/test_unified_data_interface_extra2.py

UnifiedDataInterface 覆盖率补充测试（第二批）
目标：覆盖__init__的env-var异常分支、backfill调度器、connect()异常
      路径、schema迁移方法、build_incremental_plan、quarantine重放、
      SLA报告、因子列表等低覆盖路径。
"""
from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest


def _make_udi(**kwargs) -> Any:
    """构造 UnifiedDataInterface，不触发真实 IO。"""
    from data_manager.unified_data_interface import UnifiedDataInterface

    defaults = dict(
        duckdb_path=":memory:",
        eager_init=False,
        silent_init=True,
    )
    defaults.update(kwargs)
    return UnifiedDataInterface(**defaults)


def _make_udi_with_tables(**kwargs) -> Any:
    """构造 UDI，连接内存 DuckDB 并初始化所有表。"""
    import duckdb

    udi = _make_udi(**kwargs)
    udi.con = duckdb.connect(":memory:")
    udi._read_only_connection = False
    udi.duckdb_available = True
    udi._duckdb_checked = True
    udi._tables_initialized = False
    udi._ensure_tables_exist()
    return udi


# ══════════════════════════════════════════════════════════════════════════════
# 1. __init__ 环境变量异常分支（lines 180-181、~233-254）
# ══════════════════════════════════════════════════════════════════════════════

class TestInitEnvVarEdgeCases:
    def test_invalid_backoff_base_s_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("EASYXT_REMOTE_BACKOFF_BASE_S", "not_a_float")
        udi = _make_udi()
        assert udi._cb_state["base_s"] == 3.0

    def test_invalid_backoff_max_s_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("EASYXT_REMOTE_BACKOFF_MAX_S", "not_a_float")
        udi = _make_udi()
        assert udi._cb_state["max_s"] == 300.0

    def test_invalid_step6_sample_rate_falls_back_to_1(self, monkeypatch):
        monkeypatch.setenv("EASYXT_STEP6_VALIDATE_SAMPLE_RATE", "bad_value")
        udi = _make_udi()
        assert udi._step6_validate_sample_rate == 0.05  # 解析失败时回落到内置默认值 0.05

    def test_step6_sample_rate_clamped_below_zero(self, monkeypatch):
        monkeypatch.setenv("EASYXT_STEP6_VALIDATE_SAMPLE_RATE", "-0.5")
        udi = _make_udi()
        assert udi._step6_validate_sample_rate == 0.0

    def test_step6_sample_rate_clamped_above_one(self, monkeypatch):
        monkeypatch.setenv("EASYXT_STEP6_VALIDATE_SAMPLE_RATE", "2.5")
        udi = _make_udi()
        assert udi._step6_validate_sample_rate == 1.0

    def test_canary_shadow_write_enabled_via_env(self, monkeypatch):
        monkeypatch.setenv("EASYXT_CANARY_SHADOW_WRITE", "1")
        udi = _make_udi()
        assert udi._canary_shadow_write_enabled is True

    def test_canary_shadow_write_disabled_by_default(self):
        udi = _make_udi()
        assert udi._canary_shadow_write_enabled is False

    def test_canary_shadow_only_disabled_via_env(self, monkeypatch):
        monkeypatch.setenv("EASYXT_CANARY_SHADOW_ONLY", "0")
        udi = _make_udi()
        assert udi._canary_shadow_only is False

    def test_canary_shadow_only_enabled_by_default(self):
        udi = _make_udi()
        assert udi._canary_shadow_only is True

    def test_backfill_max_queue_from_env(self, monkeypatch):
        monkeypatch.setenv("EASYXT_BACKFILL_MAX_QUEUE", "256")
        udi = _make_udi()
        assert udi._backfill_max_queue == 256

    def test_tushare_token_from_easyxt_env(self, monkeypatch):
        monkeypatch.setenv("EASYXT_TUSHARE_TOKEN", "secret_tok")
        udi = _make_udi()
        assert udi._tushare_token == "secret_tok"

    def test_tushare_token_from_tushare_env(self, monkeypatch):
        monkeypatch.delenv("EASYXT_TUSHARE_TOKEN", raising=False)
        monkeypatch.setenv("TUSHARE_TOKEN", "fallback_tok")
        udi = _make_udi()
        assert udi._tushare_token == "fallback_tok"

    def test_step6_metrics_initial_structure(self):
        udi = _make_udi()
        m = udi._step6_validation_metrics
        assert "total" in m
        assert "hard_failed" in m
        assert "sample_rate" in m

    def test_listing_date_cache_initially_empty(self):
        udi = _make_udi()
        assert isinstance(udi._listing_date_cache, dict)
        assert len(udi._listing_date_cache) == 0


# ══════════════════════════════════════════════════════════════════════════════
# 2. _ensure_backfill_scheduler 各分支
# ══════════════════════════════════════════════════════════════════════════════

class TestEnsureBackfillScheduler:
    def test_early_return_when_disabled(self):
        udi = _make_udi()
        udi._backfill_enabled = False
        udi._backfill_scheduler = None
        udi._ensure_backfill_scheduler()
        assert udi._backfill_scheduler is None

    def test_early_return_when_already_set(self):
        udi = _make_udi()
        mock_sched = MagicMock()
        udi._backfill_scheduler = mock_sched
        udi._ensure_backfill_scheduler()
        assert udi._backfill_scheduler is mock_sched  # unchanged

    def test_import_error_leaves_scheduler_none(self):
        udi = _make_udi()
        udi._backfill_enabled = True
        udi._backfill_scheduler = None
        # Patch the HistoryBackfillScheduler class to raise
        with patch(
            "data_manager.history_backfill_scheduler.HistoryBackfillScheduler",
            side_effect=RuntimeError("test-init-error"),
        ):
            udi._ensure_backfill_scheduler()
        assert udi._backfill_scheduler is None


# ══════════════════════════════════════════════════════════════════════════════
# 3. schedule_backfill 排队成功路径
# ══════════════════════════════════════════════════════════════════════════════

class TestScheduleBackfillQueued:
    def test_queued_true_calls_record_ingestion_status(self):
        udi = _make_udi()
        udi._backfill_enabled = True
        mock_sched = MagicMock()
        mock_sched.schedule.return_value = True
        udi._backfill_scheduler = mock_sched

        with patch.object(udi, "_record_ingestion_status") as mock_rec:
            result = udi.schedule_backfill("000001.SZ", "2024-01-01", "2024-01-31")

        assert result is True
        mock_rec.assert_called_once()
        _, call_kwargs = mock_rec.call_args
        assert call_kwargs.get("status") == "queued"

    def test_not_queued_skips_record(self):
        udi = _make_udi()
        udi._backfill_enabled = True
        mock_sched = MagicMock()
        mock_sched.schedule.return_value = False
        udi._backfill_scheduler = mock_sched

        with patch.object(udi, "_record_ingestion_status") as mock_rec:
            result = udi.schedule_backfill("000001.SZ", "2024-01-01", "2024-01-31")

        assert result is False
        mock_rec.assert_not_called()

    def test_queued_with_explicit_priority(self):
        udi = _make_udi()
        udi._backfill_enabled = True
        mock_sched = MagicMock()
        mock_sched.schedule.return_value = True
        udi._backfill_scheduler = mock_sched

        with patch.object(udi, "_record_ingestion_status"):
            result = udi.schedule_backfill(
                "000001.SZ", "2024-01-01", "2024-01-31", priority=50
            )

        assert result is True
        _, sched_kw = mock_sched.schedule.call_args
        assert sched_kw.get("priority") == 50


# ══════════════════════════════════════════════════════════════════════════════
# 4. _compute_backfill_priority gap_length=None 路径
# ══════════════════════════════════════════════════════════════════════════════

class TestComputeBackfillPriorityGapFromDates:
    def test_gap_from_valid_dates_when_gap_length_none(self):
        udi = _make_udi()
        p = udi._compute_backfill_priority(
            stock_code="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-06-30",
            period="1d",
            gap_length=None,
        )
        assert 0 <= p <= 100

    def test_gap_zero_treated_as_none(self):
        udi = _make_udi()
        p = udi._compute_backfill_priority(
            stock_code="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-01-31",
            period="1d",
            gap_length=0,
        )
        assert 0 <= p <= 100

    def test_invalid_dates_fall_back_to_gap_1(self):
        udi = _make_udi()
        p = udi._compute_backfill_priority(
            stock_code="000001.SZ",
            start_date="not-a-date",
            end_date="also-not-a-date",
            period="1d",
            gap_length=None,
        )
        # gap=1 → gap_weight = min(1/100,1) = 0.01 → priority = 100 - 0.01*1.0*50 ≈ 99
        assert 0 <= p <= 100


# ══════════════════════════════════════════════════════════════════════════════
# 5. connect() 异常路径
# ══════════════════════════════════════════════════════════════════════════════

class TestConnectExceptionPaths:
    def _make_udi_duckdb_ready(self):
        udi = _make_udi()
        udi.duckdb_available = True
        udi._duckdb_checked = True
        return udi

    def test_wal_replay_error_repaired_and_reconnects(self):
        """WAL replay 错误：repair 成功 → 重连。"""
        import duckdb as real_duckdb
        # Save original BEFORE patch applies, to avoid infinite recursion
        _orig_connect = real_duckdb.connect

        udi = self._make_udi_duckdb_ready()
        call_count = [0]

        def fake_connect(path, read_only=False):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("failure while replaying wal file: corruption detected")
            return _orig_connect(":memory:")

        mock_mgr = MagicMock()
        mock_mgr.duckdb_path = ":memory:"
        mock_mgr.repair_wal_if_needed.return_value = True

        with (
            patch("duckdb.connect", side_effect=fake_connect),
            patch(
                "data_manager.duckdb_connection_pool.get_db_manager",
                return_value=mock_mgr,
            ),
        ):
            result = udi.connect()

        assert result is True
        assert call_count[0] == 2
        mock_mgr.repair_wal_if_needed.assert_called_once()
        udi.close()

    def test_wal_replay_error_repair_fails_returns_false(self):
        """WAL replay 错误：repair 失败 → 连接返回 False。"""
        udi = self._make_udi_duckdb_ready()

        def fake_connect(path, read_only=False):
            raise Exception("failure while replaying wal file: critical corruption")

        mock_mgr = MagicMock()
        mock_mgr.duckdb_path = ":memory:"
        mock_mgr.repair_wal_if_needed.return_value = False

        with (
            patch("duckdb.connect", side_effect=fake_connect),
            patch(
                "data_manager.duckdb_connection_pool.get_db_manager",
                return_value=mock_mgr,
            ),
        ):
            result = udi.connect()

        assert result is False

    def test_different_configuration_error_retries_rw(self):
        """「不同配置」错误：回退到 read_only=False 重试。"""
        import duckdb as real_duckdb
        # Save original BEFORE patch applies, to avoid infinite recursion
        _orig_connect = real_duckdb.connect

        udi = self._make_udi_duckdb_ready()
        call_count = [0]

        def fake_connect(path, read_only=False):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception(
                    "different configuration than existing connections: concurrent write"
                )
            return _orig_connect(":memory:")

        mock_mgr = MagicMock()
        mock_mgr.duckdb_path = ":memory:"

        with (
            patch("duckdb.connect", side_effect=fake_connect),
            patch(
                "data_manager.duckdb_connection_pool.get_db_manager",
                return_value=mock_mgr,
            ),
        ):
            result = udi.connect(read_only=False)

        assert result is True
        assert call_count[0] == 2
        udi.close()

    def test_unrelated_exception_returns_false(self):
        """无关异常 → 连接失败返回 False。"""
        udi = self._make_udi_duckdb_ready()

        mock_mgr = MagicMock()
        mock_mgr.duckdb_path = ":memory:"

        with (
            patch("duckdb.connect", side_effect=RuntimeError("totally unrelated error")),
            patch(
                "data_manager.duckdb_connection_pool.get_db_manager",
                return_value=mock_mgr,
            ),
        ):
            result = udi.connect()

        assert result is False


# ══════════════════════════════════════════════════════════════════════════════
# 6. _get_table_columns fallback 路径
# ══════════════════════════════════════════════════════════════════════════════

class TestGetTableColumnsFallback:
    def test_first_pragma_fails_second_succeeds(self):
        udi = _make_udi()
        call_count = [0]

        def fake_execute(sql, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("column_name pragma failed")
            mock_result = MagicMock()
            mock_result.fetchall.return_value = [("stock_code",), ("date",)]
            return mock_result

        mock_con = MagicMock()
        mock_con.execute.side_effect = fake_execute
        udi.con = mock_con

        result = udi._get_table_columns("test_table")
        assert result == ["stock_code", "date"]
        assert call_count[0] == 2

    def test_both_pragmas_fail_returns_empty(self):
        udi = _make_udi()
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("all pragmas failed")
        udi.con = mock_con

        result = udi._get_table_columns("test_table")
        assert result == []

    def test_first_pragma_returns_empty_rows_fallback(self):
        """第一个 pragma 返回空行 → 继续走成功路径（返回空列表但不触发except）。"""
        udi = _make_udi()
        call_count = [0]

        def fake_execute(sql, *args, **kwargs):
            call_count[0] += 1
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []  # empty rows from first pragma
            return mock_result

        mock_con = MagicMock()
        mock_con.execute.side_effect = fake_execute
        udi.con = mock_con

        result = udi._get_table_columns("empty_table")
        # First pragma returns empty rows → goes to second pragma
        assert isinstance(result, list)


# ══════════════════════════════════════════════════════════════════════════════
# 7. Schema 迁移方法（_migrate_stock_daily_schema 等）
# ══════════════════════════════════════════════════════════════════════════════

class TestSchemaMigration:
    def test_migrate_stock_daily_adds_missing_columns(self):
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE TABLE stock_daily (
                stock_code VARCHAR NOT NULL,
                date DATE NOT NULL,
                open DECIMAL(18,6),
                high DECIMAL(18,6),
                low DECIMAL(18,6),
                close DECIMAL(18,6),
                volume DECIMAL(18,6),
                amount DECIMAL(18,6)
            )
            """
        )
        udi = _make_udi()
        udi.con = con
        udi._migrate_stock_daily_schema()
        cols = set(udi._get_table_columns("stock_daily"))
        assert "symbol_type" in cols
        assert "period" in cols
        assert "adjust_type" in cols
        assert "factor" in cols
        assert "created_at" in cols
        assert "updated_at" in cols
        con.close()

    def test_migrate_stock_daily_noop_when_complete(self):
        """表已完整 → 无需迁移（不抛异常）。"""
        import duckdb

        udi = _make_udi_with_tables()
        try:
            udi._migrate_stock_daily_schema()  # Should be no-op
        finally:
            udi.con.close()

    def test_migrate_stock_tick_adds_bid_ask_columns(self):
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE TABLE stock_tick (
                stock_code VARCHAR NOT NULL,
                datetime TIMESTAMP NOT NULL,
                price DECIMAL(18,6),
                volume BIGINT
            )
            """
        )
        udi = _make_udi()
        udi.con = con
        udi._migrate_stock_tick_schema()
        cols = set(udi._get_table_columns("stock_tick"))
        assert "lastPrice" in cols
        assert "bidPrice" in cols
        assert "askPrice" in cols
        con.close()

    def test_migrate_ingestion_status_schema_adds_lineage_cols(self):
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE TABLE data_ingestion_status (
                stock_code VARCHAR,
                period VARCHAR,
                status VARCHAR
            )
            """
        )
        udi = _make_udi()
        udi.con = con
        udi._migrate_ingestion_status_schema()
        cols = set(udi._get_table_columns("data_ingestion_status"))
        assert "schema_version" in cols
        assert "ingest_run_id" in cols
        assert "raw_hash" in cols
        con.close()

    def test_migrate_quarantine_schema_adds_replay_cols(self):
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE TABLE data_quarantine_log (
                quarantine_id VARCHAR PRIMARY KEY,
                stock_code VARCHAR,
                reason VARCHAR
            )
            """
        )
        udi = _make_udi()
        udi.con = con
        udi._migrate_quarantine_schema()
        cols = set(udi._get_table_columns("data_quarantine_log"))
        assert "replay_status" in cols
        assert "retry_count" in cols
        assert "last_error" in cols
        assert "watermark_late" in cols
        con.close()

    def test_migrate_sla_daily_schema_adds_step6_cols(self):
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE TABLE data_quality_sla_daily (
                report_date DATE PRIMARY KEY,
                completeness DOUBLE,
                consistency DOUBLE
            )
            """
        )
        udi = _make_udi()
        udi.con = con
        udi._migrate_sla_daily_schema()
        cols = set(udi._get_table_columns("data_quality_sla_daily"))
        assert "step6_total_checks" in cols
        assert "canary_shadow_write_enabled" in cols
        assert "canary_shadow_only" in cols
        con.close()

    def test_migrate_stock_daily_skips_empty_columns_list(self):
        """_get_table_columns 返回空列表时迁移方法直接返回。"""
        udi = _make_udi()
        with patch.object(udi, "_get_table_columns", return_value=[]):
            udi._migrate_stock_daily_schema()  # Should return immediately without error

    def test_migrate_quarantine_skips_empty_columns_list(self):
        udi = _make_udi()
        with patch.object(udi, "_get_table_columns", return_value=[]):
            udi._migrate_quarantine_schema()

    def test_migrate_sla_daily_skips_empty_columns_list(self):
        udi = _make_udi()
        with patch.object(udi, "_get_table_columns", return_value=[]):
            udi._migrate_sla_daily_schema()


# ══════════════════════════════════════════════════════════════════════════════
# 8. build_incremental_plan 各分支
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildIncrementalPlan:
    def test_nat_start_date_returns_full_plan(self):
        udi = _make_udi()
        udi.con = MagicMock()
        plan = udi.build_incremental_plan("000001.SZ", "not-a-date", "2024-01-31")
        assert len(plan) == 1
        assert plan[0]["mode"] == "full"

    def test_nat_end_date_returns_full_plan(self):
        udi = _make_udi()
        udi.con = MagicMock()
        plan = udi.build_incremental_plan("000001.SZ", "2024-01-01", "bad_end")
        assert len(plan) == 1
        assert plan[0]["mode"] == "full"

    def test_no_bounds_returns_full_plan(self):
        udi = _make_udi()
        udi.con = MagicMock()
        with patch.object(udi, "_get_existing_date_bounds", return_value=None):
            plan = udi.build_incremental_plan("000001.SZ", "2024-01-01", "2024-01-31")
        assert plan[0]["mode"] == "full"

    def test_nat_bounds_returns_full_plan(self):
        udi = _make_udi()
        udi.con = MagicMock()
        with patch.object(udi, "_get_existing_date_bounds", return_value=("invalid", "invalid")):
            plan = udi.build_incremental_plan("000001.SZ", "2024-01-01", "2024-01-31")
        assert plan[0]["mode"] == "full"

    def test_prepend_when_start_before_min(self):
        """请求起始早于已有最小日期 → 追加 prepend 段。"""
        udi = _make_udi()
        udi.con = MagicMock()
        with patch.object(
            udi, "_get_existing_date_bounds", return_value=("2024-01-10", "2024-01-25")
        ):
            plan = udi.build_incremental_plan("000001.SZ", "2024-01-01", "2024-01-25")
        modes = [p["mode"] for p in plan]
        assert "prepend" in modes

    def test_append_when_end_after_max(self):
        """请求结束晚于已有最大日期 → 追加 append 段。"""
        udi = _make_udi()
        udi.con = MagicMock()
        with patch.object(
            udi, "_get_existing_date_bounds", return_value=("2024-01-01", "2024-01-20")
        ):
            plan = udi.build_incremental_plan("000001.SZ", "2024-01-01", "2024-01-31")
        modes = [p["mode"] for p in plan]
        assert "append" in modes

    def test_prepend_and_append_when_both(self):
        udi = _make_udi()
        udi.con = MagicMock()
        with patch.object(
            udi, "_get_existing_date_bounds", return_value=("2024-01-10", "2024-01-20")
        ):
            plan = udi.build_incremental_plan("000001.SZ", "2024-01-01", "2024-01-31")
        modes = [p["mode"] for p in plan]
        assert "prepend" in modes
        assert "append" in modes

    def test_within_bounds_no_gaps_returns_skip(self):
        udi = _make_udi()
        udi.con = MagicMock()
        with (
            patch.object(udi, "_get_existing_date_bounds", return_value=("2024-01-01", "2024-01-31")),
            patch.object(udi, "_read_from_duckdb", return_value=pd.DataFrame({"close": [1.0] * 20})),
            patch.object(udi, "_check_missing_trading_days", return_value=0),
        ):
            plan = udi.build_incremental_plan("000001.SZ", "2024-01-05", "2024-01-20")
        assert any(p["mode"] == "skip" for p in plan)

    def test_within_bounds_with_gaps_returns_refresh(self):
        udi = _make_udi()
        udi.con = MagicMock()
        with (
            patch.object(udi, "_get_existing_date_bounds", return_value=("2024-01-01", "2024-01-31")),
            patch.object(udi, "_read_from_duckdb", return_value=pd.DataFrame({"close": [1.0] * 10})),
            patch.object(udi, "_check_missing_trading_days", return_value=5),
        ):
            plan = udi.build_incremental_plan("000001.SZ", "2024-01-05", "2024-01-20")
        assert any(p["mode"] == "refresh" for p in plan)

    def test_within_bounds_empty_duckdb_returns_full(self):
        udi = _make_udi()
        udi.con = MagicMock()
        with (
            patch.object(udi, "_get_existing_date_bounds", return_value=("2024-01-01", "2024-01-31")),
            patch.object(udi, "_read_from_duckdb", return_value=pd.DataFrame()),
        ):
            plan = udi.build_incremental_plan("000001.SZ", "2024-01-05", "2024-01-20")
        assert any(p["mode"] == "full" for p in plan)

    def test_within_bounds_read_raises_returns_full(self):
        udi = _make_udi()
        udi.con = MagicMock()
        with (
            patch.object(udi, "_get_existing_date_bounds", return_value=("2024-01-01", "2024-01-31")),
            patch.object(udi, "_read_from_duckdb", side_effect=RuntimeError("db error")),
        ):
            plan = udi.build_incremental_plan("000001.SZ", "2024-01-05", "2024-01-20")
        assert any(p["mode"] == "full" for p in plan)


# ══════════════════════════════════════════════════════════════════════════════
# 9. get_ingestion_status
# ══════════════════════════════════════════════════════════════════════════════

class TestGetIngestionStatus:
    def test_with_existing_con_and_filters(self):
        udi = _make_udi_with_tables()
        try:
            result = udi.get_ingestion_status(stock_code="000001.SZ", period="1d")
            assert isinstance(result, pd.DataFrame)
        finally:
            udi.con.close()

    def test_with_no_filters_returns_dataframe(self):
        udi = _make_udi_with_tables()
        try:
            result = udi.get_ingestion_status()
            assert isinstance(result, pd.DataFrame)
        finally:
            udi.con.close()

    def test_with_only_stock_code_filter(self):
        udi = _make_udi_with_tables()
        try:
            result = udi.get_ingestion_status(stock_code="000001.SZ")
            assert isinstance(result, pd.DataFrame)
        finally:
            udi.con.close()

    def test_no_con_auto_connects(self):
        """con=None 时自动连接后查询。"""
        udi = _make_udi()
        udi.con = None
        result = udi.get_ingestion_status()
        assert isinstance(result, pd.DataFrame)
        udi.close()


# ══════════════════════════════════════════════════════════════════════════════
# 10. get_quarantine_status_counts
# ══════════════════════════════════════════════════════════════════════════════

class TestGetQuarantineStatusCounts:
    def test_empty_table_returns_zeros(self):
        udi = _make_udi_with_tables()
        try:
            result = udi.get_quarantine_status_counts()
            assert result["total"] == 0
            assert "pending" in result
            assert "resolved" in result
            assert "dead_letter" in result
        finally:
            udi.con.close()

    def test_no_con_auto_connects(self):
        udi = _make_udi()
        udi.con = None
        result = udi.get_quarantine_status_counts()
        assert isinstance(result, dict)
        udi.close()

    def test_exception_returns_zero_dict(self):
        udi = _make_udi()
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("db error")
        udi.con = mock_con
        udi._tables_initialized = True
        result = udi.get_quarantine_status_counts()
        assert result["total"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# 11. get_data_quality_incident_counts
# ══════════════════════════════════════════════════════════════════════════════

class TestGetDataQualityIncidentCounts:
    def test_empty_table_returns_zeros(self):
        udi = _make_udi_with_tables()
        try:
            result = udi.get_data_quality_incident_counts()
            assert result["total"] == 0
            assert "critical" in result
        finally:
            udi.con.close()

    def test_exception_returns_zero_dict(self):
        udi = _make_udi()
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("db error")
        udi.con = mock_con
        udi._tables_initialized = True
        result = udi.get_data_quality_incident_counts()
        assert result["total"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# 12. get_step6_validation_metrics + _step6_should_validate
# ══════════════════════════════════════════════════════════════════════════════

class TestStep6Validation:
    def test_get_step6_metrics_basic_structure(self):
        udi = _make_udi()
        metrics = udi.get_step6_validation_metrics()
        assert "total" in metrics
        assert "sampled" in metrics
        assert "hard_failed" in metrics
        assert "hard_fail_rate" in metrics
        assert metrics["hard_fail_rate"] == 0.0

    def test_get_step6_metrics_with_positive_sampled(self):
        udi = _make_udi()
        udi._step6_validation_metrics["sampled"] = 10
        udi._step6_validation_metrics["hard_failed"] = 2
        metrics = udi.get_step6_validation_metrics()
        assert abs(metrics["hard_fail_rate"] - 0.2) < 1e-9

    def test_step6_should_validate_always_at_rate_1(self):
        udi = _make_udi()
        udi._step6_validate_sample_rate = 1.0
        assert udi._step6_should_validate("any_basis") is True

    def test_step6_should_validate_never_at_rate_0(self):
        udi = _make_udi()
        udi._step6_validate_sample_rate = 0.0
        assert udi._step6_should_validate("any_basis") is False

    def test_step6_should_validate_deterministic_at_intermediate_rate(self):
        udi = _make_udi()
        udi._step6_validate_sample_rate = 0.5
        # Same input should always give same result
        r1 = udi._step6_should_validate("test_stock_2024-01-01")
        r2 = udi._step6_should_validate("test_stock_2024-01-01")
        assert r1 == r2
        assert isinstance(r1, bool)


# ══════════════════════════════════════════════════════════════════════════════
# 13. run_quarantine_replay / run_late_event_replay 委托
# ══════════════════════════════════════════════════════════════════════════════

class TestQuarantineReplayDelegation:
    def test_run_quarantine_replay_delegates_to_core(self):
        udi = _make_udi()
        with patch.object(
            udi, "_run_quarantine_replay_core", return_value={"processed": 5}
        ) as mock_core:
            result = udi.run_quarantine_replay(limit=10, max_retries=2)
        mock_core.assert_called_once_with(limit=10, max_retries=2)
        assert result == {"processed": 5}

    def test_run_late_event_replay_passes_reason_regex(self):
        udi = _make_udi()
        with patch.object(
            udi, "_run_quarantine_replay_core", return_value={"processed": 3}
        ) as mock_core:
            result = udi.run_late_event_replay(limit=20, max_retries=3)
        assert mock_core.called
        _, kw = mock_core.call_args
        assert "reason_regex" in kw
        assert "late" in kw["reason_regex"]
        assert result == {"processed": 3}


# ══════════════════════════════════════════════════════════════════════════════
# 14. _run_quarantine_replay_core 空队列
# ══════════════════════════════════════════════════════════════════════════════

class TestRunQuarantineReplayCoreEmpty:
    def test_empty_queue_returns_zeros(self):
        udi = _make_udi_with_tables()
        try:
            result = udi._run_quarantine_replay_core(limit=10, max_retries=3)
            assert result["processed"] == 0
            assert result["succeeded"] == 0
            assert result["failed"] == 0
        finally:
            udi.con.close()

    def test_no_con_auto_connects(self):
        udi = _make_udi()
        udi.con = None
        result = udi._run_quarantine_replay_core(limit=5, max_retries=2)
        assert isinstance(result, dict)
        udi.close()

    def test_db_error_reading_rows_returns_zeros(self):
        udi = _make_udi()
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("fetch failed")
        udi.con = mock_con
        udi._tables_initialized = True
        result = udi._run_quarantine_replay_core(limit=10, max_retries=3)
        assert result["processed"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# 15. get_data_coverage
# ══════════════════════════════════════════════════════════════════════════════

class TestGetDataCoverage:
    def test_empty_tables_returns_empty_dataframe(self):
        udi = _make_udi_with_tables()
        try:
            result = udi.get_data_coverage()
            assert isinstance(result, pd.DataFrame)
        finally:
            udi.con.close()

    def test_with_period_filter(self):
        udi = _make_udi_with_tables()
        try:
            result = udi.get_data_coverage(periods=["1d"])
            assert isinstance(result, pd.DataFrame)
        finally:
            udi.con.close()

    def test_unknown_period_skipped(self):
        udi = _make_udi_with_tables()
        try:
            result = udi.get_data_coverage(periods=["unknown_period"])
            assert isinstance(result, pd.DataFrame)
        finally:
            udi.con.close()

    def test_no_con_auto_connects(self):
        udi = _make_udi()
        udi.con = None
        result = udi.get_data_coverage(periods=["1d"])
        assert isinstance(result, pd.DataFrame)
        udi.close()


# ══════════════════════════════════════════════════════════════════════════════
# 16. generate_daily_sla_report
# ══════════════════════════════════════════════════════════════════════════════

class TestGenerateDailySlaReport:
    def test_basic_report_structure(self):
        udi = _make_udi_with_tables()
        try:
            result = udi.generate_daily_sla_report()
            assert "report_date" in result
            assert "trust_score" in result
            assert "gate_pass" in result
            assert "completeness" in result
            assert "step6_total_checks" in result
        finally:
            udi.con.close()

    def test_report_with_explicit_date(self):
        udi = _make_udi_with_tables()
        try:
            result = udi.generate_daily_sla_report(report_date="2024-01-15")
            assert result["report_date"] == "2024-01-15"
        finally:
            udi.con.close()

    def test_no_con_returns_empty(self):
        udi = _make_udi()
        udi.con = None
        udi.duckdb_available = False
        udi._duckdb_checked = True
        result = udi.generate_daily_sla_report()
        assert result == {}


# ══════════════════════════════════════════════════════════════════════════════
# 17. close() 含调度器
# ══════════════════════════════════════════════════════════════════════════════

class TestCloseWithScheduler:
    def test_close_stops_and_clears_scheduler(self):
        udi = _make_udi()
        mock_sched = MagicMock()
        udi._backfill_scheduler = mock_sched
        udi.con = None
        udi.close()
        mock_sched.stop.assert_called_once()
        assert udi._backfill_scheduler is None

    def test_close_scheduler_stop_exception_handled(self):
        udi = _make_udi()
        mock_sched = MagicMock()
        mock_sched.stop.side_effect = RuntimeError("stop failed")
        udi._backfill_scheduler = mock_sched
        udi.con = None
        udi.close()  # Should not raise
        assert udi._backfill_scheduler is None

    def test_close_with_open_con_and_scheduler(self):
        import duckdb

        udi = _make_udi()
        udi.con = duckdb.connect(":memory:")
        mock_sched = MagicMock()
        udi._backfill_scheduler = mock_sched
        udi.close()
        assert udi.con is None
        assert udi._backfill_scheduler is None


# ══════════════════════════════════════════════════════════════════════════════
# 18. list_factors
# ══════════════════════════════════════════════════════════════════════════════

class TestListFactors:
    def test_list_factors_returns_list(self):
        udi = _make_udi()
        result = udi.list_factors()
        assert isinstance(result, list)

    def test_list_factors_exception_returns_empty(self):
        udi = _make_udi()
        with patch("data_manager.factor_registry.factor_registry") as mock_reg:
            mock_reg.list_all.side_effect = Exception("registry error")
            result = udi.list_factors()
        # Should not raise; returns [] on exception
        assert isinstance(result, list)
        assert result == []


# ══════════════════════════════════════════════════════════════════════════════
# 19. module-level get_stock_data 便捷函数
# ══════════════════════════════════════════════════════════════════════════════

class TestModuleLevelGetStockData:
    def test_convenience_function_returns_dataframe(self):
        from data_manager.unified_data_interface import get_stock_data

        # With duckdb_path=None → default; no real data expected but should not crash
        result = get_stock_data(
            stock_code="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-01-05",
        )
        assert isinstance(result, pd.DataFrame)


# ══════════════════════════════════════════════════════════════════════════════
# 20. _get_existing_date_bounds
# ══════════════════════════════════════════════════════════════════════════════

class TestGetExistingDateBounds:
    def test_no_con_returns_none(self):
        udi = _make_udi()
        udi.con = None
        result = udi._get_existing_date_bounds("000001.SZ", "1d")
        assert result is None

    def test_empty_table_returns_none(self):
        udi = _make_udi_with_tables()
        try:
            result = udi._get_existing_date_bounds("000001.SZ", "1d")
            assert result is None
        finally:
            udi.con.close()

    def test_db_exception_returns_none(self):
        udi = _make_udi()
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("query failed")
        udi.con = mock_con
        result = udi._get_existing_date_bounds("000001.SZ", "1d")
        assert result is None


# ══════════════════════════════════════════════════════════════════════════════
# 21. run_multiperiod_rebuild 错误路径
# ══════════════════════════════════════════════════════════════════════════════

class TestRunMultiperiodRebuild:
    def test_empty_stock_code_returns_error(self):
        udi = _make_udi_with_tables()
        try:
            result = udi.run_multiperiod_rebuild(
                "", "2024-01-01", "2024-01-31"
            )
            assert result["ok"] is False
            assert "error" in result
        finally:
            udi.con.close()

    def test_no_con_connect_fails_returns_error(self):
        udi = _make_udi()
        udi.con = None
        udi.duckdb_available = False
        udi._duckdb_checked = True
        result = udi.run_multiperiod_rebuild("000001.SZ", "2024-01-01", "2024-01-31")
        assert result["ok"] is False
        assert result.get("error") == "duckdb_connect_failed"

    def test_multiday_period_runs_cross_validate(self):
        udi = _make_udi_with_tables()
        try:
            sample = pd.DataFrame(
                {
                    "time": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
                    "open": [1.0, 1.1, 1.2],
                    "high": [1.2, 1.3, 1.4],
                    "low": [0.9, 1.0, 1.1],
                    "close": [1.1, 1.2, 1.3],
                    "volume": [100, 120, 130],
                }
            )
            builder = MagicMock()
            builder.build_multiday_bars.return_value = sample.copy()
            builder.cross_validate.return_value = None
            udi.get_stock_data = MagicMock(return_value=sample.copy())
            udi.get_listing_date = MagicMock(return_value="2024-01-02")
            udi._make_period_bar_builder = MagicMock(return_value=builder)
            udi._atomic_replace_rebuild_periods = MagicMock(return_value=(True, ""))
            udi._write_rebuild_receipt = MagicMock(return_value={"status": "success", "receipt_hash": "h"})
            result = udi.run_multiperiod_rebuild("000001.SZ", "2024-01-01", "2024-01-31", periods=["2d"])
            assert result["ok"] is True
            assert builder.cross_validate.called
        finally:
            udi.con.close()

    def test_natural_calendar_period_runs_cross_validate(self):
        udi = _make_udi_with_tables()
        try:
            sample = pd.DataFrame(
                {
                    "time": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
                    "open": [1.0, 1.1, 1.2],
                    "high": [1.2, 1.3, 1.4],
                    "low": [0.9, 1.0, 1.1],
                    "close": [1.1, 1.2, 1.3],
                    "volume": [100, 120, 130],
                }
            )
            builder = MagicMock()
            builder.build_natural_calendar_bars.return_value = sample.copy()
            builder.cross_validate.return_value = None
            udi.get_stock_data = MagicMock(return_value=sample.copy())
            udi._make_period_bar_builder = MagicMock(return_value=builder)
            udi._atomic_replace_rebuild_periods = MagicMock(return_value=(True, ""))
            udi._write_rebuild_receipt = MagicMock(return_value={"status": "success", "receipt_hash": "h"})
            result = udi.run_multiperiod_rebuild("000001.SZ", "2024-01-01", "2024-01-31", periods=["1w"])
            assert result["ok"] is True
            assert builder.cross_validate.called
            build_kwargs = builder.build_natural_calendar_bars.call_args.kwargs
            assert build_kwargs["period_code"] == "1W"
            assert build_kwargs["period_family"] == "natural_calendar"
            assert build_kwargs["threshold_version"] == "2026.04.01"
            receipt_kwargs = udi._write_rebuild_receipt.call_args.kwargs
            assert receipt_kwargs["session_profile_id"] == "CN_A"
            assert receipt_kwargs["period_registry_version"] == "2026.04.01"
            assert receipt_kwargs["threshold_registry_version"] == "2026.04.01"
            assert receipt_kwargs["period_metadata"]["1w"] == {
                "period_code": "1W",
                "period_family": "natural_calendar",
                "threshold_version": "2026.04.01",
            }
        finally:
            udi.con.close()

    def test_write_rebuild_receipt_includes_governance_metadata(self):
        udi = _make_udi_with_tables()
        try:
            with patch("pathlib.Path.mkdir"), patch("pathlib.Path.write_text"):
                payload = udi._write_rebuild_receipt(
                    rebuild_id="rid-1",
                    stock_code="000001.SZ",
                    start_date="2024-01-01",
                    end_date="2024-01-31",
                    target_periods=["1w"],
                    persisted_periods=["1d"],
                    row_stats={"1w": 4},
                    status="success",
                    error_message="",
                    session_profile_id="CN_A",
                    session_profile_version="2026.04.01",
                    auction_policy="merged_open_auction",
                    period_registry_version="2026.04.01",
                    threshold_registry_version="2026.04.01",
                    period_metadata={
                        "1w": {
                            "period_code": "1W",
                            "period_family": "natural_calendar",
                            "threshold_version": "2026.04.01",
                        }
                    },
                )
            assert payload["receipt_hash"]
            assert payload["governance"] == {
                "session_profile_id": "CN_A",
                "session_profile_version": "2026.04.01",
                "auction_policy": "merged_open_auction",
                "period_registry_version": "2026.04.01",
                "threshold_registry_version": "2026.04.01",
                "period_metadata": {
                    "1w": {
                        "period_code": "1W",
                        "period_family": "natural_calendar",
                        "threshold_version": "2026.04.01",
                    }
                },
            }
        finally:
            udi.con.close()

    def test_multiday_builder_exception_marks_failed_and_skips_atomic_replace(self):
        udi = _make_udi_with_tables()
        try:
            sample = pd.DataFrame(
                {
                    "time": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
                    "open": [1.0, 1.1, 1.2],
                    "high": [1.2, 1.3, 1.4],
                    "low": [0.9, 1.0, 1.1],
                    "close": [1.1, 1.2, 1.3],
                    "volume": [100, 120, 130],
                }
            )
            builder = MagicMock()
            builder.build_multiday_bars.side_effect = RuntimeError("listing_date gap too large")
            udi.get_stock_data = MagicMock(return_value=sample.copy())
            udi.get_listing_date = MagicMock(return_value="2024-01-02")
            udi._make_period_bar_builder = MagicMock(return_value=builder)
            udi._atomic_replace_rebuild_periods = MagicMock(return_value=(True, ""))
            udi._write_rebuild_receipt = MagicMock(return_value={"status": "failed", "receipt_hash": "h"})
            result = udi.run_multiperiod_rebuild("000001.SZ", "2024-01-01", "2024-01-31", periods=["2d"])
            assert result["ok"] is False
            assert result["atomic_replace"] is False
            assert udi._atomic_replace_rebuild_periods.call_count == 0
            assert result["failed"] >= 1
            reasons = [str(x.get("reason", "")) for x in result.get("details", [])]
            assert any("listing_date gap too large" in r for r in reasons)
        finally:
            udi.con.close()

    def test_atomic_replace_failure_reflected_in_result_and_receipt(self):
        udi = _make_udi_with_tables()
        try:
            sample = pd.DataFrame(
                {
                    "time": pd.to_datetime(["2024-01-02", "2024-01-03"]),
                    "open": [1.0, 1.1],
                    "high": [1.2, 1.3],
                    "low": [0.9, 1.0],
                    "close": [1.1, 1.2],
                    "volume": [100, 120],
                }
            )
            udi.get_stock_data = MagicMock(return_value=sample.copy())
            udi._make_period_bar_builder = MagicMock(return_value=MagicMock())
            udi._atomic_replace_rebuild_periods = MagicMock(return_value=(False, "tx_failed"))
            udi._write_rebuild_receipt = MagicMock(return_value={"status": "failed", "receipt_hash": "h2"})
            result = udi.run_multiperiod_rebuild("000001.SZ", "2024-01-01", "2024-01-31", periods=["1d"])
            assert result["ok"] is False
            assert result["atomic_replace"] is False
            assert result["atomic_error"] == "tx_failed"
            assert result["failed"] >= 1
            assert result.get("audit_receipt", {}).get("status") == "failed"
        finally:
            udi.con.close()
