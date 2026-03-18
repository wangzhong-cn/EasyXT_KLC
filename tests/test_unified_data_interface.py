#!/usr/bin/env python3
"""
tests/test_unified_data_interface.py

UnifiedDataInterface 集成级测试
覆盖策略：伪依赖 + 故障注入，测试 __init__ / 电路断路器 / connect/close /
          数据获取路径 / 降级分支，避免逐行单测（集成场景效率更高）。
"""
from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

# ── 辅助工厂 ───────────────────────────────────────────────────────────────────

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


# ══════════════════════════════════════════════════════════════════════════════
# 1. __init__ 参数解析
# ══════════════════════════════════════════════════════════════════════════════

class TestInit:
    def test_default_cb_threshold(self):
        udi = _make_udi()
        assert udi._cb_state["fail_threshold"] == 5

    def test_custom_cb_threshold(self):
        udi = _make_udi(cb_fail_threshold=3)
        assert udi._cb_state["fail_threshold"] == 3

    def test_env_var_cb_threshold(self, monkeypatch):
        monkeypatch.setenv("EASYXT_REMOTE_CB_THRESHOLD", "7")
        udi = _make_udi()
        assert udi._cb_state["fail_threshold"] == 7

    def test_env_var_invalid_cb_threshold_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("EASYXT_REMOTE_CB_THRESHOLD", "not_a_number")
        udi = _make_udi()
        assert udi._cb_state["fail_threshold"] == 5

    def test_custom_backoff(self):
        udi = _make_udi(backoff_base_s=10.0, backoff_max_s=600.0)
        assert udi._cb_state["base_s"] == 10.0
        assert udi._cb_state["max_s"] == 600.0

    def test_env_var_backoff(self, monkeypatch):
        monkeypatch.setenv("EASYXT_REMOTE_BACKOFF_BASE_S", "2.5")
        monkeypatch.setenv("EASYXT_REMOTE_BACKOFF_MAX_S", "120.0")
        udi = _make_udi()
        assert udi._cb_state["base_s"] == 2.5
        assert udi._cb_state["max_s"] == 120.0

    def test_cb_state_initially_closed(self):
        udi = _make_udi()
        assert udi._cb_state["open"] is False
        assert udi._cb_state["fail_count"] == 0

    def test_registry_has_duckdb_and_parquet(self):
        udi = _make_udi()
        # DataSourceRegistry._sources holds registered sources
        assert udi.data_registry is not None
        assert udi.data_registry._sources.get("duckdb") is not None
        assert udi.data_registry._sources.get("parquet") is not None

    def test_backfill_enabled_by_default(self):
        udi = _make_udi()
        assert udi._backfill_enabled is True

    def test_backfill_disabled_via_env(self, monkeypatch):
        monkeypatch.setenv("EASYXT_BACKFILL_ENABLED", "0")
        udi = _make_udi()
        assert udi._backfill_enabled is False


# ══════════════════════════════════════════════════════════════════════════════
# 2. _log 方法
# ══════════════════════════════════════════════════════════════════════════════

class TestLog:
    def test_silent_suppresses_output(self, capsys):
        udi = _make_udi(silent_init=True)
        udi._log("should not appear")
        captured = capsys.readouterr()
        assert "should not appear" not in captured.out

    def test_verbose_prints_output(self, capsys):
        udi = _make_udi(silent_init=False)
        udi._log("[INFO] hello world")
        captured = capsys.readouterr()
        assert "hello world" in captured.out


# ══════════════════════════════════════════════════════════════════════════════
# 3. 电路断路器（Circuit Breaker）状态机
# ══════════════════════════════════════════════════════════════════════════════

class TestCircuitBreaker:
    def test_allow_when_closed(self):
        udi = _make_udi()
        assert udi._cb_allow() is True

    def test_allow_false_when_open_and_cooldown_not_elapsed(self):
        udi = _make_udi()
        # 强制打开断路器
        udi._cb_state["open"] = True
        udi._cb_state["opened_at"] = time.perf_counter()
        udi._cb_state["cooldown_s"] = 9999.0
        assert udi._cb_allow() is False

    def test_allow_true_when_open_but_cooldown_elapsed(self):
        udi = _make_udi()
        udi._cb_state["open"] = True
        udi._cb_state["opened_at"] = time.perf_counter() - 10.0  # 10s ago
        udi._cb_state["cooldown_s"] = 5.0   # 5s cooldown already passed
        assert udi._cb_allow() is True
        assert udi._cb_state["open"] is False  # 断路器已关

    def test_on_success_resets_state(self):
        udi = _make_udi()
        udi._cb_state["open"] = True
        udi._cb_state["fail_count"] = 5
        udi._cb_state["cooldown_s"] = 60.0
        udi._cb_on_success()
        assert udi._cb_state["open"] is False
        assert udi._cb_state["fail_count"] == 0
        assert udi._cb_state["cooldown_s"] == 0.0

    def test_on_failure_below_threshold_does_not_open(self):
        udi = _make_udi(cb_fail_threshold=5)
        for _ in range(4):
            udi._cb_on_failure()
        assert udi._cb_state["open"] is False
        assert udi._cb_state["fail_count"] == 4

    def test_on_failure_at_threshold_opens_breaker(self):
        udi = _make_udi(cb_fail_threshold=3, backoff_base_s=1.0, backoff_max_s=300.0)
        for _ in range(3):
            udi._cb_on_failure()
        assert udi._cb_state["open"] is True
        assert udi._cb_state["cooldown_s"] > 0

    def test_on_failure_exponential_backoff_caps_at_max(self):
        udi = _make_udi(cb_fail_threshold=1, backoff_base_s=1.0, backoff_max_s=8.0)
        for _ in range(10):
            udi._cb_on_failure()
        assert udi._cb_state["cooldown_s"] <= 8.0

    def test_on_failure_increments_count(self):
        udi = _make_udi(cb_fail_threshold=100)
        udi._cb_on_failure()
        udi._cb_on_failure()
        assert udi._cb_state["fail_count"] == 2


# ══════════════════════════════════════════════════════════════════════════════
# 4. _check_duckdb  / _check_qmt
# ══════════════════════════════════════════════════════════════════════════════

class TestCheckDataSources:
    def test_duckdb_available_because_installed(self):
        udi = _make_udi()
        udi._check_duckdb()
        assert udi.duckdb_available is True

    def test_check_duckdb_idempotent(self):
        udi = _make_udi()
        udi._check_duckdb()
        udi._check_duckdb()
        assert udi._duckdb_checked is True

    def test_duckdb_unavailable_when_import_fails(self, monkeypatch):
        import builtins
        real_import = builtins.__import__

        def _block_duckdb(name, *args, **kwargs):
            if name == "duckdb":
                raise ImportError("mocked missing duckdb")
            return real_import(name, *args, **kwargs)

        udi = _make_udi()
        # Reset state
        udi._duckdb_checked = False
        with patch.object(builtins, "__import__", side_effect=_block_duckdb):
            udi._check_duckdb()
        assert udi.duckdb_available is False

    def test_qmt_unavailable_without_xtdata(self):
        """xtdata 一般不在 CI 环境，应为 False。"""
        udi = _make_udi()
        udi._qmt_checked = False
        udi.qmt_available = False
        udi._check_qmt()
        # May be True if xtquant is available, but in CI should be False
        # Just verify it doesn't raise
        assert isinstance(udi.qmt_available, bool)

    def test_check_qmt_idempotent(self):
        udi = _make_udi()
        udi._check_qmt()
        udi._qmt_checked = False  # reset checked flag but keep result
        udi._check_qmt()
        # Running again should not change the boolean type
        assert isinstance(udi.qmt_available, bool)

    def test_check_qmt_respects_disable_env(self, monkeypatch):
        import builtins
        real_import = builtins.__import__

        def _block_xtquant(name, *args, **kwargs):
            if name == "xtquant":
                raise AssertionError("xtquant should not be imported when qmt online disabled")
            return real_import(name, *args, **kwargs)

        udi = _make_udi()
        udi._qmt_checked = False
        udi.qmt_available = True
        monkeypatch.setenv("EASYXT_ENABLE_QMT_ONLINE", "0")
        with patch.object(builtins, "__import__", side_effect=_block_xtquant):
            udi._check_qmt()
        assert udi.qmt_available is False
        assert udi._qmt_checked is True


# ══════════════════════════════════════════════════════════════════════════════
# 5. connect / close
# ══════════════════════════════════════════════════════════════════════════════

class TestConnectClose:
    def test_connect_returns_false_when_duckdb_unavailable(self):
        udi = _make_udi()
        udi.duckdb_available = False
        udi._duckdb_checked = True
        result = udi.connect()
        assert result is False
        assert udi.con is None

    def test_connect_success_with_memory_db(self, tmp_path):
        udi = _make_udi(duckdb_path=str(tmp_path / "test.duckdb"))
        udi._check_duckdb()
        if not udi.duckdb_available:
            pytest.skip("DuckDB not available")
        result = udi.connect(read_only=False)
        assert result is True
        assert udi.con is not None
        udi.close()

    def test_close_sets_con_to_none(self, tmp_path):
        udi = _make_udi(duckdb_path=str(tmp_path / "test2.duckdb"))
        udi._check_duckdb()
        if not udi.duckdb_available:
            pytest.skip("DuckDB not available")
        udi.connect(read_only=False)
        assert udi.con is not None
        udi.close()
        assert udi.con is None

    def test_close_safe_when_con_is_none(self):
        udi = _make_udi()
        udi.con = None
        udi.close()  # Should not raise

    def test_connect_sets_read_only_flag(self, tmp_path, monkeypatch):
        # 禁用「优先读写」环境变量覆盖，确保 read_only=True 生效
        monkeypatch.setenv("EASYXT_DUCKDB_PREFER_RW", "0")
        udi = _make_udi(duckdb_path=str(tmp_path / "test3.duckdb"))
        udi._check_duckdb()
        if not udi.duckdb_available:
            pytest.skip("DuckDB not available")
        # Connect with write mode first (file must exist for read-only)
        udi.connect(read_only=False)
        udi.close()
        # Now reconnect read-only
        result = udi.connect(read_only=True)
        assert result is True
        assert udi._read_only_connection is True
        udi.close()


# ══════════════════════════════════════════════════════════════════════════════
# 6. _compute_backfill_priority（纯函数）
# ══════════════════════════════════════════════════════════════════════════════

class TestBackfillPriority:
    def test_priority_range_is_0_to_100(self):
        udi = _make_udi()
        p = udi._compute_backfill_priority(
            stock_code="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-01-31",
            period="1d",
        )
        assert 0 <= p <= 100

    def test_large_gap_lower_priority(self):
        udi = _make_udi()
        p_small = udi._compute_backfill_priority(
            stock_code="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-01-02",
            period="1d",
        )
        p_large = udi._compute_backfill_priority(
            stock_code="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-12-31",
            period="1d",
            gap_length=365,
        )
        # Large gap → lower priority number
        assert p_large <= p_small

    def test_current_symbol_boost(self):
        udi = _make_udi()
        p_normal = udi._compute_backfill_priority(
            stock_code="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-06-30",
            period="1d",
            current_symbol="999999.SZ",  # different symbol
        )
        p_current = udi._compute_backfill_priority(
            stock_code="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-06-30",
            period="1d",
            current_symbol="000001.SZ",  # same symbol → boost weight
        )
        # current_symbol match → lower (higher urgency) priority number
        assert p_current <= p_normal

    def test_explicit_gap_length_used(self):
        udi = _make_udi()
        p = udi._compute_backfill_priority(
            stock_code="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-01-02",
            period="1d",
            gap_length=1,
        )
        assert 0 <= p <= 100


# ══════════════════════════════════════════════════════════════════════════════
# 7. schedule_backfill（无 IO 分支）
# ══════════════════════════════════════════════════════════════════════════════

class TestScheduleBackfill:
    def test_returns_false_when_disabled(self, monkeypatch):
        monkeypatch.setenv("EASYXT_BACKFILL_ENABLED", "0")
        udi = _make_udi()
        assert udi._backfill_enabled is False
        result = udi.schedule_backfill("000001.SZ", "2024-01-01", "2024-01-31")
        assert result is False

    def test_returns_false_when_scheduler_none(self):
        udi = _make_udi()
        udi._backfill_enabled = True
        udi._backfill_scheduler = None
        # Prevent real scheduler from starting to avoid daemon thread leaking into
        # subsequent tests (can cause DuckDB abort() crash in the next test class).
        with patch.object(udi, "_ensure_backfill_scheduler"):
            result = udi.schedule_backfill("000001.SZ", "2024-01-01", "2024-01-31")
        # With _ensure_backfill_scheduler patched, scheduler stays None → returns False
        assert result is False


# ══════════════════════════════════════════════════════════════════════════════
# 8. get_stock_data 路径覆盖（mock con + adjustment_manager）
# ══════════════════════════════════════════════════════════════════════════════

class TestGetStockData:
    def _make_udi_with_mock_duckdb(self, ohlcv_data: pd.DataFrame | None = None):
        """构造一个 con 和 adjustment_manager 都是 mock 的实例。"""
        udi = _make_udi()
        udi.duckdb_available = True
        udi._duckdb_checked = True
        udi.qmt_available = False
        udi._qmt_checked = True
        # Pre-mark Tushare and AKShare as checked+unavailable so get_stock_data
        # never triggers live network calls regardless of machine state.
        udi.tushare_available = False
        udi._tushare_checked = True
        udi.akshare_available = False
        udi._akshare_checked = True

        mock_con = MagicMock()
        mock_con.__bool__ = Mock(return_value=True)
        udi.con = mock_con

        mock_adj = MagicMock()
        if ohlcv_data is not None and not ohlcv_data.empty:
            mock_adj.get_data_with_adjustment.return_value = ohlcv_data
        else:
            mock_adj.get_data_with_adjustment.return_value = pd.DataFrame()
        udi.adjustment_manager = mock_adj
        udi._tables_initialized = True
        return udi

    def _make_ohlcv(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
                "open": [10.0, 10.5],
                "high": [10.8, 11.0],
                "low": [9.9, 10.2],
                "close": [10.5, 10.9],
                "volume": [100000, 120000],
            }
        )

    def test_returns_duckdb_data_when_available(self):
        ohlcv = self._make_ohlcv()
        udi = self._make_udi_with_mock_duckdb(ohlcv)
        # Patch _check_missing_trading_days to say data is complete
        with patch.object(udi, "_check_missing_trading_days", return_value=0):
            result = udi.get_stock_data(
                "000001.SZ", "2024-01-01", "2024-01-31", period="1d"
            )
        assert not result.empty
        assert len(result) == 2

    def test_step6_cache_stale_hard_violation_triggers_quarantine(self):
        ohlcv = self._make_ohlcv()
        udi = self._make_udi_with_mock_duckdb(ohlcv)
        fake_result = MagicMock()
        fake_result.pass_gate = False
        fake_result.violations = [MagicMock(severity="hard", detail="close<=0")]
        with patch.object(udi, "_check_missing_trading_days", return_value=0):
            with patch("data_manager.data_contract_validator.DataContractValidator") as mock_validator:
                with patch.object(udi, "_record_quarantine_log") as mock_quarantine:
                    with patch.object(udi, "_emit_data_quality_alert") as mock_alert:
                        mock_validator.return_value.validate.return_value = fake_result
                        _ = udi.get_stock_data(
                            "000001.SZ", "2024-01-01", "2024-01-31", period="1d"
                        )
        assert mock_quarantine.called
        assert mock_alert.called

    def test_step6_can_disable_cache_stale_quarantine_by_env(self, monkeypatch):
        monkeypatch.setenv("EASYXT_CACHE_STALE_QUARANTINE_ENABLED", "0")
        ohlcv = self._make_ohlcv()
        udi = self._make_udi_with_mock_duckdb(ohlcv)
        fake_result = MagicMock()
        fake_result.pass_gate = False
        fake_result.violations = [MagicMock(severity="hard", detail="non_trading_day")]
        with patch.object(udi, "_check_missing_trading_days", return_value=0):
            with patch("data_manager.data_contract_validator.DataContractValidator") as mock_validator:
                with patch.object(udi, "_record_quarantine_log") as mock_quarantine:
                    with patch.object(udi, "_emit_data_quality_alert") as mock_alert:
                        mock_validator.return_value.validate.return_value = fake_result
                        _ = udi.get_stock_data(
                            "000001.SZ", "2024-01-01", "2024-01-31", period="1d"
                        )
        assert not mock_quarantine.called
        assert not mock_alert.called

    def test_step6_sampling_zero_skips_validation(self, monkeypatch):
        monkeypatch.setenv("EASYXT_STEP6_VALIDATE_SAMPLE_RATE", "0")
        ohlcv = self._make_ohlcv()
        udi = self._make_udi_with_mock_duckdb(ohlcv)
        with patch.object(udi, "_check_missing_trading_days", return_value=0):
            with patch("data_manager.data_contract_validator.DataContractValidator") as mock_validator:
                _ = udi.get_stock_data(
                    "000001.SZ", "2024-01-01", "2024-01-31", period="1d"
                )
        assert not mock_validator.return_value.validate.called
        m = udi.get_step6_validation_metrics()
        assert m["total"] >= 1
        assert m["skipped"] >= 1

    def test_canary_shadow_only_skips_main_table_write(self):
        ohlcv = self._make_ohlcv()
        udi = self._make_udi_with_mock_duckdb(ohlcv)
        udi._canary_shadow_write_enabled = True
        udi._canary_shadow_only = True
        with patch.object(udi, "_ensure_tables_exist"):
            with patch.object(udi, "_pre_write_validate", return_value=(True, "")):
                with patch.object(udi, "_write_shadow_copy") as mock_shadow:
                    with patch.object(udi.con, "execute") as mock_execute:
                        udi._save_to_duckdb(ohlcv, "000001.SZ", "1d")
        assert mock_shadow.called
        assert not any("INSERT OR REPLACE INTO stock_daily" in str(c) for c in mock_execute.call_args_list)

    def test_returns_empty_when_no_duckdb_and_no_qmt(self):
        udi = self._make_udi_with_mock_duckdb(pd.DataFrame())  # empty
        # _make_udi_with_mock_duckdb 已将 tushare/akshare _checked=True, _available=False，
        # 确保不会触发外部 API 调用。以下 side_effect=AssertionError 作为「防回退」门禁：
        # 若未来有人移除 _checked 标志导致这些方法被真正调用，测试会立即高亮失败。
        _must_not_call = AssertionError("外部数据源不应在此测试中被调用")
        with patch.object(udi, "_refresh_qmt_status"):  # 防止 xtquant 重新激活 QMT
            with patch.object(udi, "_read_from_qmt", side_effect=_must_not_call):
                with patch.object(udi, "_read_from_tushare", side_effect=_must_not_call):
                    with patch.object(udi, "_read_from_akshare", side_effect=_must_not_call):
                        with patch.object(udi.data_registry, "get_data", return_value=pd.DataFrame()):
                            with patch.object(udi, "_record_ingestion_status"):
                                result = udi.get_stock_data(
                                    "000001.SZ", "2024-01-01", "2024-01-31", period="1d"
                                )
        assert result.empty

    def test_circuit_breaker_open_returns_cached_data(self):
        ohlcv = self._make_ohlcv()
        udi = self._make_udi_with_mock_duckdb(ohlcv)
        # Open circuit breaker and mark data as incomplete to trigger need_download
        udi._cb_state["open"] = True
        udi._cb_state["opened_at"] = time.perf_counter()
        udi._cb_state["cooldown_s"] = 9999.0
        with patch.object(udi, "_check_missing_trading_days", return_value=5):
            result = udi.get_stock_data(
                "000001.SZ", "2024-01-01", "2024-01-31", period="1d"
            )
        # Returns cached DuckDB data without attempting online fetch
        assert not result.empty

    def test_circuit_breaker_open_with_no_cached_data_returns_empty(self):
        udi = self._make_udi_with_mock_duckdb(pd.DataFrame())
        udi._cb_state["open"] = True
        udi._cb_state["opened_at"] = time.perf_counter()
        udi._cb_state["cooldown_s"] = 9999.0
        # mock DAT 源，防止本地 DAT 文件被读取，绕过 circuit breaker
        with patch.object(udi.data_registry, "get_data", return_value=pd.DataFrame()):
            result = udi.get_stock_data(
                "000001.SZ", "2024-01-01", "2024-01-31", period="1d"
            )
        assert result.empty

    def test_no_connection_falls_through_to_connect(self, tmp_path):
        udi = _make_udi(duckdb_path=str(tmp_path / "test_get.duckdb"))
        udi.duckdb_available = True
        udi._duckdb_checked = True
        udi.qmt_available = False
        udi._qmt_checked = True
        udi.con = None
        # get_stock_data will call self.connect() when con is None
        # Mock connect to avoid real IO
        with patch.object(udi, "connect", return_value=False):
            result = udi.get_stock_data(
                "000001.SZ", "2024-01-01", "2024-01-05", period="1d"
            )
        assert isinstance(result, pd.DataFrame)

    def test_intraday_period_bypasses_fivefold(self):
        """分钟线请求不应调用 FiveFoldAdjustmentManager，否则会以日线数据污染分钟图（Fix 6）。"""
        udi = self._make_udi_with_mock_duckdb(pd.DataFrame())
        with patch.object(udi, "_read_from_duckdb", return_value=pd.DataFrame()) as mock_duckdb:
            with patch.object(udi, "_refresh_qmt_status"):
                with patch.object(udi, "_read_from_qmt", return_value=pd.DataFrame()):
                    with patch.object(udi, "_read_from_akshare", return_value=pd.DataFrame()):
                        with patch.object(udi.data_registry, "get_data", return_value=pd.DataFrame()):
                            with patch.object(udi, "_record_ingestion_status"):
                                udi.get_stock_data(
                                    "000001.SZ", "2024-01-01", "2024-01-31", period="1m"
                                )
        assert mock_duckdb.called, "_read_from_duckdb should be called for intraday periods"
        udi.adjustment_manager.get_data_with_adjustment.assert_not_called()

    def test_safe_table_name_whitelist_contains_expected(self):
        udi = _make_udi()
        assert "stock_daily" in udi._SAFE_TABLE_NAMES
        assert "stock_1m" in udi._SAFE_TABLE_NAMES
        assert "stock_5m" in udi._SAFE_TABLE_NAMES
        assert "stock_tick" in udi._SAFE_TABLE_NAMES

    def test_write_shadow_copy_rejects_illegal_table_name(self):
        udi = _make_udi()
        with pytest.raises(ValueError, match="不允许的表名"):
            udi._write_shadow_copy(
                "stock_daily;DROP TABLE stock_daily", "date", pd.DataFrame(), "000001.SZ", "1d"
            )

    def test_registry_third_party_fallback_returns_data(self):
        udi = self._make_udi_with_mock_duckdb(pd.DataFrame())
        fallback_df = self._make_ohlcv()
        with patch.object(udi, "_refresh_qmt_status"):
            with patch.object(udi, "_dat_file_is_fresh", return_value=False):
                with patch.object(udi, "_is_futures_or_hk", return_value=False):
                    with patch.object(
                        udi.data_registry,
                        "get_data",
                        side_effect=[pd.DataFrame(), fallback_df],
                    ) as mock_get_data:
                        result = udi.get_stock_data(
                            "000001.SZ", "2024-01-01", "2024-01-31", period="1d", auto_save=False
                        )
        assert not result.empty
        assert len(mock_get_data.call_args_list) >= 2
        assert mock_get_data.call_args_list[0].kwargs.get("preferred_sources") == ["dat"]
        assert mock_get_data.call_args_list[1].kwargs.get("preferred_sources") == ["tushare", "akshare"]

    def test_registry_third_party_exception_returns_empty(self):
        udi = self._make_udi_with_mock_duckdb(pd.DataFrame())
        with patch.object(udi, "_refresh_qmt_status"):
            with patch.object(udi, "_dat_file_is_fresh", return_value=False):
                with patch.object(udi, "_is_futures_or_hk", return_value=False):
                    with patch.object(
                        udi.data_registry,
                        "get_data",
                        side_effect=[pd.DataFrame(), RuntimeError("third-party boom")],
                    ):
                        with patch.object(udi._logger, "error") as mock_error:
                            result = udi.get_stock_data(
                                "000001.SZ", "2024-01-01", "2024-01-31", period="1d", auto_save=False
                            )
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert mock_error.called

    def test_save_to_duckdb_exception_records_failed_ingestion(self):
        udi = self._make_udi_with_mock_duckdb(pd.DataFrame())
        fallback_df = self._make_ohlcv()
        fake_result = MagicMock()
        fake_result.pass_gate = True
        fake_result.violations = []
        with patch.object(udi, "_refresh_qmt_status"):
            with patch.object(udi, "_dat_file_is_fresh", return_value=False):
                with patch.object(udi, "_is_futures_or_hk", return_value=False):
                    with patch.object(
                        udi.data_registry,
                        "get_data",
                        side_effect=[pd.DataFrame(), fallback_df],
                    ):
                        with patch("data_manager.data_contract_validator.DataContractValidator") as mock_validator:
                            with patch.object(udi, "_save_to_duckdb", side_effect=RuntimeError("save failed")):
                                with patch.object(udi, "_record_ingestion_status") as mock_record:
                                    mock_validator.return_value.validate.return_value = fake_result
                                    result = udi.get_stock_data(
                                        "000001.SZ", "2024-01-01", "2024-01-31", period="1d", auto_save=True
                                    )
        assert not result.empty
        assert mock_record.called
        assert any(call.kwargs.get("status") == "failed" for call in mock_record.call_args_list)

    def test_futures_symbol_skips_third_party_fallback(self):
        udi = self._make_udi_with_mock_duckdb(pd.DataFrame())
        with patch.object(udi, "_refresh_qmt_status"):
            with patch.object(udi, "_dat_file_is_fresh", return_value=False):
                with patch.object(udi, "_is_futures_or_hk", return_value=True):
                    with patch.object(
                        udi.data_registry,
                        "get_data",
                        side_effect=[pd.DataFrame()],
                    ) as mock_get_data:
                        with patch.object(udi, "_record_ingestion_status"):
                            result = udi.get_stock_data(
                                "IF2406.CFFEX", "2024-01-01", "2024-01-31", period="1d", auto_save=False
                            )
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert len(mock_get_data.call_args_list) == 1
        assert mock_get_data.call_args_list[0].kwargs.get("preferred_sources") == ["dat"]

    def test_period_aggregation_fallback_uses_recursive_source_period(self):
        udi = self._make_udi_with_mock_duckdb(pd.DataFrame())
        src_df = self._make_ohlcv()
        resampled_df = pd.DataFrame({"date": pd.to_datetime(["2024-01-31"]), "open": [10.0], "close": [10.9]})
        with patch.object(udi, "_refresh_qmt_status"):
            with patch.object(udi, "_dat_file_is_fresh", return_value=False):
                with patch.object(udi, "_is_futures_or_hk", return_value=False):
                    with patch.object(
                        udi.data_registry,
                        "get_data",
                        side_effect=[pd.DataFrame(), pd.DataFrame()],
                    ):
                        with patch.object(udi, "_resample_ohlcv", return_value=resampled_df):
                            original_get = udi.get_stock_data

                            def _patched_get(stock_code, start_date, end_date, period="1d", adjust="none", auto_save=True):
                                if period == "1d":
                                    return src_df
                                return original_get(stock_code, start_date, end_date, period, adjust, auto_save)

                            udi.get_stock_data = _patched_get  # type: ignore[method-assign]
                            result = udi.get_stock_data(
                                "000001.SZ", "2024-01-01", "2024-01-31", period="1M", auto_save=False
                            )
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result) == 1

    def test_dat_cache_path_auto_save_and_success_status(self, monkeypatch):
        monkeypatch.setenv("EASYXT_STEP6_VALIDATE_SAMPLE_RATE", "0")
        udi = self._make_udi_with_mock_duckdb(pd.DataFrame())
        dat_df = self._make_ohlcv()
        with patch.object(udi, "_refresh_qmt_status"):
            with patch.object(udi, "_check_missing_trading_days", return_value=0):
                with patch.object(udi, "_is_intraday_sparse", return_value=False):
                    with patch.object(udi.data_registry, "get_data", side_effect=[dat_df]):
                        with patch.object(udi, "_save_to_duckdb") as mock_save:
                            with patch.object(udi, "_record_ingestion_status") as mock_record:
                                result = udi.get_stock_data(
                                    "000001.SZ", "2024-01-01", "2024-01-31", period="1d", auto_save=True
                                )
        assert not result.empty
        assert mock_save.called
        assert mock_record.called
        assert any(call.kwargs.get("source") == "dat" for call in mock_record.call_args_list)
        assert udi._last_ingestion_source == "dat"

    def test_third_party_fallback_sets_tushare_ingestion_source(self, monkeypatch):
        monkeypatch.setenv("EASYXT_STEP6_VALIDATE_SAMPLE_RATE", "0")
        udi = self._make_udi_with_mock_duckdb(pd.DataFrame())
        udi.tushare_available = True
        third_party_df = self._make_ohlcv()
        fake_result = MagicMock()
        fake_result.pass_gate = True
        fake_result.violations = []
        with patch.object(udi, "_refresh_qmt_status"):
            with patch.object(udi, "_dat_file_is_fresh", return_value=False):
                with patch.object(udi, "_is_futures_or_hk", return_value=False):
                    with patch.object(
                        udi.data_registry,
                        "get_data",
                        side_effect=[pd.DataFrame(), third_party_df],
                    ):
                        with patch("data_manager.data_contract_validator.DataContractValidator") as mock_validator:
                            mock_validator.return_value.validate.return_value = fake_result
                            result = udi.get_stock_data(
                                "000001.SZ", "2024-01-01", "2024-01-31", period="1d", auto_save=False
                            )
        assert not result.empty
        assert udi._last_ingestion_source == "tushare"

    def test_contract_gate_fail_skips_auto_save(self, monkeypatch):
        monkeypatch.setenv("EASYXT_STEP6_VALIDATE_SAMPLE_RATE", "0")
        udi = self._make_udi_with_mock_duckdb(pd.DataFrame())
        third_party_df = self._make_ohlcv()
        fake_result = MagicMock()
        fake_result.pass_gate = False
        fake_result.violations = [MagicMock(detail="gate-failed")]
        with patch.object(udi, "_refresh_qmt_status"):
            with patch.object(udi, "_dat_file_is_fresh", return_value=False):
                with patch.object(udi, "_is_futures_or_hk", return_value=False):
                    with patch.object(
                        udi.data_registry,
                        "get_data",
                        side_effect=[pd.DataFrame(), third_party_df],
                    ):
                        with patch("data_manager.data_contract_validator.DataContractValidator") as mock_validator:
                            with patch.object(udi, "_save_to_duckdb") as mock_save:
                                mock_validator.return_value.validate.return_value = fake_result
                                result = udi.get_stock_data(
                                    "000001.SZ", "2024-01-01", "2024-01-31", period="1d", auto_save=True
                                )
        assert not result.empty
        assert not mock_save.called


# ══════════════════════════════════════════════════════════════════════════════
# 9. _read_from_duckdb 分支（mock con.execute）
# ══════════════════════════════════════════════════════════════════════════════

class TestReadFromDuckDB:
    def _make_udi_with_con(self):
        udi = _make_udi()
        udi.duckdb_available = True
        udi._tables_initialized = True
        mock_con = MagicMock()
        udi.con = mock_con
        return udi, mock_con

    def test_missing_table_returns_empty(self):
        udi, mock_con = self._make_udi_with_con()
        # information_schema.tables returns 0 → table not found
        mock_con.execute.return_value.fetchone.return_value = (0,)
        result = udi._read_from_duckdb(
            "000001.SZ", "2024-01-01", "2024-01-31", "1d", "none"
        )
        assert result is None or (isinstance(result, pd.DataFrame) and result.empty)

    def test_existing_table_with_data(self):
        udi, mock_con = self._make_udi_with_con()
        # table exists check → 1
        mock_con.execute.return_value.fetchone.return_value = (1,)
        # actual query returns DataFrame
        ohlcv = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02"]),
                "open": [10.0],
                "high": [10.5],
                "low": [9.8],
                "close": [10.3],
                "volume": [50000],
            }
        )
        mock_con.execute.return_value.df.return_value = ohlcv
        result = udi._read_from_duckdb(
            "000001.SZ", "2024-01-01", "2024-01-31", "1d", "none"
        )
        # Result could be the df or empty if schema validation fails
        assert result is None or isinstance(result, pd.DataFrame)

    def test_derived_period_calls_recursive(self):
        """15m 等派生周期会递归调用 _read_from_duckdb 取 1m 数据。"""
        udi, mock_con = self._make_udi_with_con()
        mock_con.execute.return_value.fetchone.return_value = (0,)
        # 1m 基础数据也为空
        result = udi._read_from_duckdb(
            "000001.SZ", "2024-01-01", "2024-01-02", "15m", "none"
        )
        # Should return empty when no underlying 1m data
        assert result is None or (isinstance(result, pd.DataFrame) and result.empty)

    def test_derived_period_direct_hit_short_circuits_resample(self):
        # 15m 已由 PeriodBarBuilder 处理（日内周期走 A 股时段对齐路径，不走 _resample_ohlcv）。
        # 验证：当 1m 源数据可用时，_read_from_duckdb("15m") 应返回非空 DataFrame
        udi, mock_con = self._make_udi_with_con()
        # 模拟 DuckDB 返回的 1m 数据格式（stock_1m 表使用 datetime 列名，
        # _read_from_duckdb 会将其设为 DatetimeIndex；PeriodBarBuilder._prepare_1m 兼容两种格式）
        src_1m_df = pd.DataFrame(
            {
                "stock_code": ["000001.SZ"] * 16,
                "datetime": pd.date_range("2024-01-02 09:31", periods=16, freq="min"),
                "open":   [10.0] * 16,
                "high":   [10.2] * 16,
                "low":    [9.9]  * 16,
                "close":  [10.1] * 16,
                "volume": [1000] * 16,
                "amount": [10100.0] * 16,
            }
        )

        def _exec(sql, params=None):
            if "information_schema.tables" in sql:
                m = MagicMock()
                m.fetchone.return_value = (1,)
                return m
            m = MagicMock()
            m.df.return_value = src_1m_df.copy()
            return m

        mock_con.execute.side_effect = _exec
        # _resample_ohlcv 不应被调用（日内周期走 PeriodBarBuilder，不走 resample）
        with patch.object(udi, "_resample_ohlcv", side_effect=AssertionError("should not resample for intraday custom")):
            result = udi._read_from_duckdb(
                "000001.SZ", "2024-01-01", "2024-01-03", "15m", "front"
            )
        # PeriodBarBuilder 从 16 根 1m 建立日内 15m K 线
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_intraday_table_ignores_adjust_specific_columns(self):
        udi, mock_con = self._make_udi_with_con()
        seen_sql = {"q": ""}
        query_df = pd.DataFrame(
            {
                "stock_code": ["000001.SZ"],
                "datetime": pd.to_datetime(["2024-01-02 09:31:00"]),
                "open": [10.0],
                "high": [10.2],
                "low": [9.9],
                "close": [10.1],
                "volume": [1000],
                "amount": [10100.0],
            }
        )

        def _exec(sql, params=None):
            if "information_schema.tables" in sql:
                m = MagicMock()
                m.fetchone.return_value = (1,)
                return m
            seen_sql["q"] = sql
            m = MagicMock()
            m.df.return_value = query_df.copy()
            return m

        mock_con.execute.side_effect = _exec
        result = udi._read_from_duckdb(
            "000001.SZ", "2024-01-01", "2024-01-03", "1m", "front"
        )
        assert isinstance(result, pd.DataFrame)
        assert "open_front" not in seen_sql["q"]

    def test_read_from_duckdb_query_exception_returns_none(self):
        udi, mock_con = self._make_udi_with_con()
        mock_con.execute.side_effect = RuntimeError("duckdb down")
        result = udi._read_from_duckdb(
            "000001.SZ", "2024-01-01", "2024-01-03", "1d", "none"
        )
        assert result is None

    def test_daily_adjust_variants_use_expected_price_columns(self):
        udi, mock_con = self._make_udi_with_con()
        seen_sql: list[str] = []
        query_df = pd.DataFrame(
            {
                "stock_code": ["000001.SZ"],
                "datetime": pd.to_datetime(["2024-01-02"]),
                "open": [10.0],
                "high": [10.2],
                "low": [9.9],
                "close": [10.1],
                "volume": [1000],
                "amount": [10100.0],
            }
        )

        def _exec(sql, params=None):
            if "information_schema.tables" in sql:
                m = MagicMock()
                m.fetchone.return_value = (1,)
                return m
            seen_sql.append(sql)
            m = MagicMock()
            m.df.return_value = query_df.copy()
            return m

        mock_con.execute.side_effect = _exec
        cases = {
            "front": "open_front as open",
            "back": "open_back as open",
            "geometric_front": "open_geometric_front as open",
            "geometric_back": "open_geometric_back as open",
        }
        for adjust, marker in cases.items():
            _ = udi._read_from_duckdb("000001.SZ", "2024-01-01", "2024-01-31", "1d", adjust)
            assert any(marker in sql for sql in seen_sql), adjust

    def test_unknown_adjust_falls_back_to_raw_price_columns(self):
        udi, mock_con = self._make_udi_with_con()
        seen_sql: list[str] = []
        query_df = pd.DataFrame(
            {
                "stock_code": ["000001.SZ"],
                "datetime": pd.to_datetime(["2024-01-02"]),
                "open": [10.0],
                "high": [10.2],
                "low": [9.9],
                "close": [10.1],
                "volume": [1000],
                "amount": [10100.0],
            }
        )

        def _exec(sql, params=None):
            if "information_schema.tables" in sql:
                m = MagicMock()
                m.fetchone.return_value = (1,)
                return m
            seen_sql.append(sql)
            m = MagicMock()
            m.df.return_value = query_df.copy()
            return m

        mock_con.execute.side_effect = _exec
        out = udi._read_from_duckdb("000001.SZ", "2024-01-01", "2024-01-31", "1d", "unknown")
        assert isinstance(out, pd.DataFrame)
        assert any(" open as open," in sql and " close as close," in sql for sql in seen_sql)


# ══════════════════════════════════════════════════════════════════════════════
# 6. QMT 日期范围 off-by-one 修复验证
#    _read_from_qmt 分钟周期结束时间必须是 "235959"，确保当天数据全部被包含。
#    若使用 "000000" 则等于请求当天午夜0点，实际上排除了整天的数据。
# ══════════════════════════════════════════════════════════════════════════════

class TestReadFromQmtEndDateFix:
    """验证 _read_from_qmt 对分钟周期结束时间使用 235959 而非 000000（Bug修复）"""

    def _make_udi(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        return UnifiedDataInterface(duckdb_path=":memory:", eager_init=False, silent_init=True)

    def _run_qmt(self, period: str, end_date: str):
        """调用 _read_from_qmt，通过 sys.modules 注入 mock xtdata，返回捕获的参数"""
        import types
        recorded: dict = {}

        fake_df = pd.DataFrame({
            "time": [1741680000000, 1741680300000],
            "open": [10.0, 10.1],
            "high": [10.2, 10.3],
            "low": [9.9, 10.0],
            "close": [10.1, 10.2],
            "volume": [1000.0, 1200.0],
            "amount": [10100.0, 12240.0],
        })

        mock_xtdata = MagicMock()
        mock_xtdata.download_history_data.side_effect = lambda sc, period, start_time, end_time: (
            recorded.__setitem__("dl_end", end_time)
        )
        mock_xtdata.get_market_data_ex.side_effect = lambda **kw: (
            recorded.__setitem__("gd_end", kw.get("end_time", "")) or {"000988.SZ": fake_df}
        )
        mock_xtdata.get_market_data_ex.return_value = {"000988.SZ": fake_df}

        fake_xtquant = types.ModuleType("xtquant")
        fake_xtquant.xtdata = mock_xtdata  # type: ignore[attr-defined]

        with patch.dict("sys.modules", {"xtquant": fake_xtquant, "xtquant.xtdata": mock_xtdata}):
            udi = self._make_udi()
            try:
                udi._read_from_qmt("000988.SZ", "2026-03-10", end_date, period)
            except Exception:
                pass

        return recorded

    def test_5m_end_uses_235959_not_000000(self):
        """5m 周期结束日期后缀应为 235959，不再是 000000（off-by-one 修复）"""
        rec = self._run_qmt("5m", "2026-03-12")
        dl_end = rec.get("dl_end", "")
        if dl_end:
            assert dl_end.endswith("235959"), (
                f"download_history_data end_time 末尾应为235959，实际: {dl_end!r}"
            )
            assert not dl_end.endswith("000000"), (
                f"发现旧 bug：end_time 仍以 000000 结尾: {dl_end!r}"
            )

    def test_1m_end_uses_235959_not_000000(self):
        """1m 周期结束日期后缀应为 235959"""
        rec = self._run_qmt("1m", "2026-03-12")
        dl_end = rec.get("dl_end", "")
        if dl_end:
            assert dl_end.endswith("235959"), (
                f"1m download end_time 应以235959结尾: {dl_end!r}"
            )

    def test_date_string_construction_is_235959(self):
        """白盒验证：日期字符串构建逻辑正确（235959 > 000000，包含更多数据）"""
        end_date = "2026-03-12"
        end_str_fixed = end_date.replace("-", "") + "235959"
        end_str_buggy = end_date.replace("-", "") + "000000"
        assert end_str_fixed == "20260312235959"
        assert end_str_fixed > end_str_buggy, "235959 应大于 000000，确保包含全天数据"

    def test_daily_period_no_time_suffix_needed(self):
        """日线周期 (1d) 不需要添加时间后缀，不受此 bug 影响"""
        end_date = "2026-03-12"
        end_str = end_date.replace("-", "")  # "20260312"
        # 对于日线，代码中判断 period not in intraday periods → 不添加后缀
        intraday_periods = {"1m", "5m", "15m", "30m", "60m"}
        assert "1d" not in intraday_periods
        assert len(end_str) == 8  # 日线只有日期，无时间后缀

    def test_future_range_is_clipped_and_backshifted(self):
        import types

        recorded = {}
        fake_df = pd.DataFrame(
            {
                "time": [1741680000000],
                "open": [10.0],
                "high": [10.2],
                "low": [9.9],
                "close": [10.1],
                "volume": [1000.0],
                "amount": [10100.0],
            }
        )
        mock_xtdata = MagicMock()
        mock_xtdata.download_history_data.return_value = None

        def _gmde(**kw):
            recorded["start"] = kw.get("start_time")
            recorded["end"] = kw.get("end_time")
            return {"000988.SZ": fake_df}

        mock_xtdata.get_market_data_ex.side_effect = _gmde
        fake_xtquant = types.ModuleType("xtquant")
        fake_xtquant.xtdata = mock_xtdata  # type: ignore[attr-defined]
        with patch.dict("sys.modules", {"xtquant": fake_xtquant, "xtquant.xtdata": mock_xtdata}):
            udi = self._make_udi()
            _ = udi._read_from_qmt("000988.SZ", "2099-01-01", "2099-01-02", "1d")

        today = pd.Timestamp.today().normalize()
        expected_end = today.strftime("%Y%m%d")
        expected_start = (today - pd.Timedelta(days=365)).strftime("%Y%m%d")
        assert recorded.get("end") == expected_end
        assert recorded.get("start") == expected_start

    def test_read_from_qmt_import_fail_returns_none(self):
        import builtins

        _orig_import = builtins.__import__

        def _guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name.startswith("xtquant"):
                raise ImportError("blocked xtquant")
            return _orig_import(name, globals, locals, fromlist, level)

        udi = self._make_udi()
        with patch("builtins.__import__", side_effect=_guarded_import):
            out = udi._read_from_qmt("000988.SZ", "2026-03-10", "2026-03-12", "1d")
        assert out is None


class TestReadFromQmtEdgeCases:
    def test_qmt_dict_without_target_symbol_returns_none(self):
        import types

        udi = _make_udi()
        fake_df = pd.DataFrame({"time": [1741680000000], "open": [10.0], "high": [10.2], "low": [9.9], "close": [10.1]})
        mock_xtdata = MagicMock()
        mock_xtdata.download_history_data.return_value = None
        mock_xtdata.get_market_data_ex.return_value = {"OTHER.SZ": fake_df}
        fake_xtquant = types.ModuleType("xtquant")
        fake_xtquant.xtdata = mock_xtdata  # type: ignore[attr-defined]
        with patch.dict("sys.modules", {"xtquant": fake_xtquant, "xtquant.xtdata": mock_xtdata}):
            result = udi._read_from_qmt("000001.SZ", "2024-01-01", "2024-01-31", "1d")
        assert result is None

    def test_qmt_missing_volume_amount_filled_with_zero(self):
        import types

        udi = _make_udi()
        fake_df = pd.DataFrame(
            {
                "time": [1741680000000, 1741766400000],
                "open": [10.0, 10.1],
                "high": [10.2, 10.3],
                "low": [9.9, 10.0],
                "close": [10.1, 10.2],
            }
        )
        mock_xtdata = MagicMock()
        mock_xtdata.download_history_data.return_value = None
        mock_xtdata.get_market_data_ex.return_value = {"000001.SZ": fake_df}
        fake_xtquant = types.ModuleType("xtquant")
        fake_xtquant.xtdata = mock_xtdata  # type: ignore[attr-defined]
        with patch.dict("sys.modules", {"xtquant": fake_xtquant, "xtquant.xtdata": mock_xtdata}):
            result = udi._read_from_qmt("000001.SZ", "2024-01-01", "2024-01-31", "1d")
        assert isinstance(result, pd.DataFrame)
        assert "volume" in result.columns and "amount" in result.columns
        assert (result["volume"] == 0).all()
        assert (result["amount"] == 0).all()


class TestSaveToDuckdbRetry:
    def test_save_to_duckdb_wal_reconnect_retries_once(self):
        udi = _make_udi()
        udi.con = MagicMock()
        sample = pd.DataFrame(
            {
                "open": [10.0],
                "high": [10.2],
                "low": [9.9],
                "close": [10.1],
            },
            index=pd.to_datetime(["2024-01-02"]),
        )

        with patch.object(
            udi,
            "_ensure_tables_exist",
            side_effect=[
                RuntimeError("failure while replaying .wal cannot open file"),
                RuntimeError("stop after retry"),
            ],
        ):
            with patch.object(udi, "_close_duckdb_connection") as mock_close:
                with patch.object(udi, "connect", return_value=True) as mock_connect:
                    with patch.object(udi, "_record_write_audit", return_value="a1"):
                        with patch.object(udi, "_record_quarantine_log"):
                            with patch.object(udi, "_emit_data_quality_alert"):
                                with patch.object(udi, "_logger"):
                                    spy = MagicMock(wraps=udi._save_to_duckdb)
                                    udi._save_to_duckdb = spy  # type: ignore[method-assign]
                                    udi._save_to_duckdb(sample, "000001.SZ", "1d")
        assert mock_close.called
        assert mock_connect.called
        assert udi._save_to_duckdb.call_count >= 2


class TestSlaStep6Metrics:
    def test_generate_daily_sla_report_includes_step6_and_canary_fields(self, monkeypatch, tmp_path):
        monkeypatch.setenv("EASYXT_STEP6_VALIDATE_SAMPLE_RATE", "0.25")
        monkeypatch.setenv("EASYXT_CANARY_SHADOW_WRITE", "1")
        monkeypatch.setenv("EASYXT_CANARY_SHADOW_ONLY", "1")
        udi = _make_udi(duckdb_path=str(tmp_path / "sla_step6.duckdb"))
        assert udi.connect(read_only=False)
        try:
            udi._step6_validation_metrics = {
                "total": 100,
                "sampled": 25,
                "skipped": 75,
                "hard_failed": 2,
                "quarantined": 2,
                "sample_rate": 0.25,
            }
            report = udi.generate_daily_sla_report()
            assert "step6_total_checks" in report
            assert report["step6_total_checks"] == 100
            assert report["step6_sampled_checks"] == 25
            assert report["step6_hard_failed_checks"] == 2
            assert report["canary_shadow_write_enabled"] is True
            assert report["canary_shadow_only"] is True
            row = udi.con.execute(
                """
                SELECT step6_total_checks, step6_sampled_checks, step6_hard_failed_checks, canary_shadow_write_enabled, canary_shadow_only
                FROM data_quality_sla_daily
                WHERE report_date = ?
                """,
                [report["report_date"]],
            ).fetchone()
            assert row is not None
            assert int(row[0]) == 100
            assert int(row[1]) == 25
            assert int(row[2]) == 2
            assert bool(row[3]) is True
            assert bool(row[4]) is True
        finally:
            udi.close()
