"""T2c – 覆盖率提升批次 C（目标 43%→45%+）

重点覆盖：
  1. unified_data_interface.py — 大量 static/class/pure-logic 方法
  2. auto_data_updater.py — 辅助函数与状态方法
  3. duckdb_connection_pool.py — 指标/WAL 相关
  4. core/api_server.py — 限流 + MarketBroadcaster 指标
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import time
from collections import deque
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ────────── UDI static / classmethod helpers ──────────────────────────────


class TestParseQmtTimeSeries:
    """UnifiedDataInterface._parse_qmt_time_series — static。"""

    @staticmethod
    def _call(series):
        from data_manager.unified_data_interface import UnifiedDataInterface
        return UnifiedDataInterface._parse_qmt_time_series(series)

    def test_14digit_int(self):
        s = pd.Series([20240315093000])
        r = self._call(s)
        assert r.iloc[0] == pd.Timestamp("2024-03-15 09:30:00")

    def test_8digit_int(self):
        s = pd.Series([20240315])
        r = self._call(s)
        assert r.iloc[0] == pd.Timestamp("2024-03-15")

    def test_millisecond_timestamp(self):
        ts_ms = int(pd.Timestamp("2024-01-01 10:00:00").timestamp() * 1000)
        r = self._call(pd.Series([ts_ms]))
        assert r.iloc[0].year == 2024

    def test_second_timestamp(self):
        ts_s = int(pd.Timestamp("2024-06-01 12:00:00").timestamp())
        r = self._call(pd.Series([ts_s]))
        assert r.iloc[0].year == 2024

    def test_none_value(self):
        r = self._call(pd.Series([None]))
        assert pd.isna(r.iloc[0])

    def test_empty_string(self):
        r = self._call(pd.Series([""]))
        assert pd.isna(r.iloc[0])

    def test_string_14digit(self):
        r = self._call(pd.Series(["20240315093000"]))
        assert r.iloc[0] == pd.Timestamp("2024-03-15 09:30:00")

    def test_string_8digit(self):
        r = self._call(pd.Series(["20240315"]))
        assert r.iloc[0] == pd.Timestamp("2024-03-15")

    def test_iso_string(self):
        r = self._call(pd.Series(["2024-03-15 09:30:00"]))
        assert r.iloc[0] == pd.Timestamp("2024-03-15 09:30:00")

    def test_mixed_series(self):
        s = pd.Series([20240101, "2024-06-01", None, ""])
        r = self._call(s)
        assert r.iloc[0] == pd.Timestamp("2024-01-01")
        assert r.iloc[1] == pd.Timestamp("2024-06-01")
        assert pd.isna(r.iloc[2])
        assert pd.isna(r.iloc[3])


class TestGetStorageTargetPeriod:
    """UDI._get_storage_target_period — static。"""

    @staticmethod
    def _call(period):
        from data_manager.unified_data_interface import UnifiedDataInterface
        return UnifiedDataInterface._get_storage_target_period(period)

    def test_15m(self):
        assert self._call("15m") == ("1m", "15m")

    def test_30m(self):
        assert self._call("30m") == ("1m", "30m")

    def test_60m(self):
        assert self._call("60m") == ("1m", "60m")

    def test_1w(self):
        assert self._call("1w") == ("1d", "1w")

    def test_1M(self):
        assert self._call("1M") == ("1d", "1M")

    def test_1d_passthrough(self):
        assert self._call("1d") == ("1d", "1d")

    def test_1m_passthrough(self):
        assert self._call("1m") == ("1m", "1m")

    def test_5m_passthrough(self):
        assert self._call("5m") == ("5m", "5m")


class TestResampleOhlcv:
    """UDI._resample_ohlcv — static。"""

    @staticmethod
    def _call(df, rule):
        from data_manager.unified_data_interface import UnifiedDataInterface
        return UnifiedDataInterface._resample_ohlcv(df, rule)

    def test_none_input(self):
        assert self._call(None, "5min") is None

    def test_empty_df(self):
        df = pd.DataFrame()
        result = self._call(df, "5min")
        assert result.empty

    def test_basic_resample(self):
        idx = pd.date_range("2024-01-02 09:30", periods=10, freq="1min")
        df = pd.DataFrame({
            "open": range(10),
            "high": range(10, 20),
            "low": range(10),
            "close": range(1, 11),
            "volume": [100] * 10,
        }, index=idx)
        result = self._call(df, "5min")
        assert len(result) == 2
        assert result["volume"].iloc[0] == 500

    def test_preserves_stock_code(self):
        idx = pd.date_range("2024-01-02 09:30", periods=5, freq="1min")
        df = pd.DataFrame({
            "open": [1]*5, "high": [2]*5, "low": [0.5]*5, "close": [1.5]*5,
            "volume": [10]*5, "stock_code": ["600519.SH"]*5,
        }, index=idx)
        result = self._call(df, "5min")
        assert result["stock_code"].iloc[0] == "600519.SH"

    def test_no_ohlcv_columns(self):
        idx = pd.date_range("2024-01-02", periods=3, freq="D")
        df = pd.DataFrame({"foo": [1, 2, 3]}, index=idx)
        result = self._call(df, "W")
        assert len(result) > 0  # 原样返回


class TestIsIndexCode:
    """UDI._is_index_code — classmethod。"""

    @staticmethod
    def _call(code):
        from data_manager.unified_data_interface import UnifiedDataInterface
        return UnifiedDataInterface._is_index_code(code)

    def test_sh_index_000(self):
        assert self._call("000001.SH") is True

    def test_sh_index_399(self):
        assert self._call("399001.SH") is True

    def test_sz_index_399(self):
        assert self._call("399001.SZ") is True

    def test_sh_stock(self):
        assert self._call("600519.SH") is False

    def test_sz_stock(self):
        assert self._call("000001.SZ") is False

    def test_csi_suffix(self):
        # CSI 后缀走 fallthrough → False
        assert self._call("000300.CSI") is False

    def test_no_suffix(self):
        assert self._call("600519") is False


class TestIsIntradaySparse:
    """UDI._is_intraday_sparse — static。"""

    @staticmethod
    def _call(data, period):
        from data_manager.unified_data_interface import UnifiedDataInterface
        return UnifiedDataInterface._is_intraday_sparse(data, period)

    def test_none_data(self):
        assert self._call(None, "1m") is True

    def test_empty_df(self):
        df = pd.DataFrame()
        assert self._call(df, "1m") is True

    def test_non_minute_period(self):
        df = pd.DataFrame({"x": [1]}, index=pd.DatetimeIndex(["2024-01-02"]))
        assert self._call(df, "1d") is False

    def test_single_day_not_sparse(self):
        # <=1 天不判定
        idx = pd.date_range("2024-01-02 09:30", periods=10, freq="1min")
        df = pd.DataFrame({"x": range(10)}, index=idx)
        assert self._call(df, "1m") is False

    def test_full_data_not_sparse(self):
        days = []
        for d in ["2024-01-02", "2024-01-03", "2024-01-04"]:
            days.extend(pd.date_range(f"{d} 09:30", periods=240, freq="1min"))
        df = pd.DataFrame({"x": range(len(days))}, index=pd.DatetimeIndex(days))
        assert self._call(df, "1m") is False

    def test_very_sparse_data(self):
        days = []
        for d in ["2024-01-02", "2024-01-03", "2024-01-04"]:
            days.extend(pd.date_range(f"{d} 09:30", periods=5, freq="1min"))
        df = pd.DataFrame({"x": range(len(days))}, index=pd.DatetimeIndex(days))
        assert self._call(df, "1m") is True

    def test_custom_minute_period(self):
        days = []
        for d in ["2024-01-02", "2024-01-03"]:
            days.extend(pd.date_range(f"{d} 09:30", periods=3, freq="3min"))
        df = pd.DataFrame({"x": range(len(days))}, index=pd.DatetimeIndex(days))
        # 3m → expected=80, each day 3 bars → very sparse
        assert self._call(df, "3m") is True


class TestIsFuturesOrHk:
    """UDI._is_futures_or_hk — static。"""

    @staticmethod
    def _call(symbol):
        from data_manager.unified_data_interface import UnifiedDataInterface
        return UnifiedDataInterface._is_futures_or_hk(symbol)

    def test_if_contract(self):
        assert self._call("IF2406.IF") is True

    def test_hk_stock(self):
        assert self._call("00700.HK") is True

    def test_sf_contract(self):
        assert self._call("AU2406.SF") is True

    def test_sh_stock(self):
        assert self._call("600519.SH") is False

    def test_no_dot(self):
        assert self._call("nodot") is False


class TestComputeDataLineage:
    """UDI._compute_data_lineage — instance method, pure logic。"""

    @staticmethod
    def _make_udi():
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = object.__new__(UnifiedDataInterface)
        return udi

    def test_basic_hash(self):
        udi = self._make_udi()
        df = pd.DataFrame({"open": [1.0, 2.0], "close": [1.1, 2.1]})
        raw_hash, evt = udi._compute_data_lineage(df)
        assert len(raw_hash) == 16
        assert all(c in "0123456789abcdef" for c in raw_hash)

    def test_datetime_column(self):
        udi = self._make_udi()
        df = pd.DataFrame({
            "open": [1.0],
            "datetime": [pd.Timestamp("2024-03-15 10:00:00")],
        })
        _, evt = udi._compute_data_lineage(df)
        assert evt == pd.Timestamp("2024-03-15 10:00:00")

    def test_date_column_fallback(self):
        udi = self._make_udi()
        df = pd.DataFrame({
            "open": [1.0],
            "date": [pd.Timestamp("2024-03-15")],
        })
        _, evt = udi._compute_data_lineage(df)
        assert evt is not None

    def test_index_fallback(self):
        udi = self._make_udi()
        df = pd.DataFrame(
            {"open": [1.0]},
            index=pd.DatetimeIndex(["2024-03-15"]),
        )
        _, evt = udi._compute_data_lineage(df)
        assert evt is not None

    def test_empty_df(self):
        udi = self._make_udi()
        df = pd.DataFrame()
        raw_hash, evt = udi._compute_data_lineage(df)
        assert isinstance(raw_hash, str)
        assert evt is None

    def test_deterministic(self):
        udi = self._make_udi()
        df = pd.DataFrame({"a": [1, 2, 3]})
        h1, _ = udi._compute_data_lineage(df)
        h2, _ = udi._compute_data_lineage(df)
        assert h1 == h2


class TestBuildQuarantineSampleJson:
    """UDI._build_quarantine_sample_json。"""

    @staticmethod
    def _make_udi():
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = object.__new__(UnifiedDataInterface)
        return udi

    def test_none_input(self):
        assert self._make_udi()._build_quarantine_sample_json(None) == ""

    def test_empty_df(self):
        assert self._make_udi()._build_quarantine_sample_json(pd.DataFrame()) == ""

    def test_normal(self):
        import json
        df = pd.DataFrame({"a": range(50)})
        result = self._make_udi()._build_quarantine_sample_json(df, limit=5)
        parsed = json.loads(result)
        assert len(parsed) == 5

    def test_limit_clamp(self):
        import json
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = self._make_udi()._build_quarantine_sample_json(df, limit=0)
        parsed = json.loads(result)
        assert len(parsed) >= 1


class TestStep6Validation:
    """UDI._step6_should_validate + get_step6_validation_metrics。"""

    @staticmethod
    def _make_udi(rate=1.0, metrics=None):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = object.__new__(UnifiedDataInterface)
        udi._step6_validate_sample_rate = rate
        udi._step6_validation_metrics = metrics or {"sampled": 0, "hard_failed": 0}
        return udi

    def test_rate_1_always_true(self):
        udi = self._make_udi(rate=1.0)
        assert udi._step6_should_validate("test_sample_basis") is True

    def test_rate_0_always_false(self):
        udi = self._make_udi(rate=0.0)
        assert udi._step6_should_validate("test_sample_basis") is False

    def test_rate_deterministic(self):
        udi = self._make_udi(rate=0.5)
        r1 = udi._step6_should_validate("same_key")
        r2 = udi._step6_should_validate("same_key")
        assert r1 == r2

    def test_metrics_no_samples(self):
        udi = self._make_udi(metrics={"sampled": 0, "hard_failed": 0})
        m = udi.get_step6_validation_metrics()
        assert m["hard_fail_rate"] == 0.0
        assert m["sample_rate"] == 1.0

    def test_metrics_with_failures(self):
        udi = self._make_udi(
            rate=0.8, metrics={"sampled": 100, "hard_failed": 5}
        )
        m = udi.get_step6_validation_metrics()
        assert m["hard_fail_rate"] == pytest.approx(0.05)
        assert m["sample_rate"] == 0.8


class TestRecordWriteAudit:
    """UDI._record_write_audit — 需 mock con。"""

    @staticmethod
    def _make_udi(con=None, read_only=False):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = object.__new__(UnifiedDataInterface)
        udi.con = con
        udi._read_only_connection = read_only
        udi._logger = MagicMock()
        return udi

    def test_no_con(self):
        udi = self._make_udi(con=None)
        result = udi._record_write_audit(
            "tbl", "600519.SH", "1d", 100, 100,
            "2024-01-01", "2024-12-31", "abc123", True, True, True,
        )
        assert result == ""

    def test_read_only(self):
        udi = self._make_udi(con=MagicMock(), read_only=True)
        result = udi._record_write_audit(
            "tbl", "600519.SH", "1d", 100, 100,
            "2024-01-01", "2024-12-31", "abc123", True, True, True,
        )
        assert result == ""

    def test_success(self):
        mock_con = MagicMock()
        udi = self._make_udi(con=mock_con, read_only=False)
        result = udi._record_write_audit(
            "tbl", "600519.SH", "1d", 100, 100,
            "2024-01-01", "2024-12-31", "abc123", True, True, True,
        )
        assert len(result) > 0  # UUID string
        mock_con.execute.assert_called_once()

    def test_exception(self):
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("db error")
        udi = self._make_udi(con=mock_con, read_only=False)
        result = udi._record_write_audit(
            "tbl", "600519.SH", "1d", 100, 100,
            "2024-01-01", "2024-12-31", "abc123", True, True, True,
        )
        assert result == ""


class TestRecordQuarantineLog:
    """UDI._record_quarantine_log — 需 mock con。"""

    @staticmethod
    def _make_udi(con=None, read_only=False):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = object.__new__(UnifiedDataInterface)
        udi.con = con
        udi._read_only_connection = read_only
        udi._logger = MagicMock()
        return udi

    def test_no_con(self):
        udi = self._make_udi(con=None)
        udi._record_quarantine_log(
            "aid", "tbl", "600519.SH", "1d", "bad_data",
            100, 50, "2024-01-01", "2024-12-31", "{}",
        )
        # 不抛异常即可

    def test_read_only(self):
        udi = self._make_udi(con=MagicMock(), read_only=True)
        udi._record_quarantine_log(
            "aid", "tbl", "600519.SH", "1d", "bad_data",
            100, 50, "2024-01-01", "2024-12-31", "{}",
        )
        udi.con.execute.assert_not_called()

    def test_success(self):
        mock_con = MagicMock()
        udi = self._make_udi(con=mock_con, read_only=False)
        udi._record_quarantine_log(
            "aid", "tbl", "600519.SH", "1d", "bad_data",
            100, 50, "2024-01-01", "2024-12-31", "{}",
        )
        mock_con.execute.assert_called_once()


class TestRecordDataQualityIncident:
    """UDI._record_data_quality_incident — 需 mock con。"""

    @staticmethod
    def _make_udi(con=None, read_only=False):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = object.__new__(UnifiedDataInterface)
        udi.con = con
        udi._read_only_connection = read_only
        udi._logger = MagicMock()
        return udi

    def test_no_con(self):
        udi = self._make_udi(con=None)
        udi._record_data_quality_incident(
            "missing_data", "warning", "600519.SH", "1d", "qid-1",
        )

    def test_with_payload(self):
        mock_con = MagicMock()
        udi = self._make_udi(con=mock_con, read_only=False)
        udi._record_data_quality_incident(
            "ohlc_anomaly", "error", "600519.SH", "1d", "qid-1",
            payload={"detail": "negative_close"},
        )
        mock_con.execute.assert_called_once()

    def test_exception_no_crash(self):
        mock_con = MagicMock()
        mock_con.execute.side_effect = RuntimeError("boom")
        udi = self._make_udi(con=mock_con, read_only=False)
        udi._record_data_quality_incident(
            "ohlc_anomaly", "error", "600519.SH", "1d", "qid-1",
        )


class TestPostWriteVerify:
    """UDI._post_write_verify — 需 mock con。"""

    @staticmethod
    def _make_udi(con=None):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = object.__new__(UnifiedDataInterface)
        udi.con = con
        udi._logger = MagicMock()
        return udi

    def test_success(self):
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchone.return_value = (100,)
        udi = self._make_udi(con=mock_con)
        ok, count = udi._post_write_verify(
            "stock_1d", "600519.SH", "1d", "date", "2024-01-01", "2024-12-31", 100,
        )
        assert ok is True
        assert count == 100

    def test_count_mismatch(self):
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchone.return_value = (50,)
        udi = self._make_udi(con=mock_con)
        ok, count = udi._post_write_verify(
            "stock_1d", "600519.SH", "1d", "date", "2024-01-01", "2024-12-31", 100,
        )
        assert ok is False
        assert count == 50

    def test_exception(self):
        mock_con = MagicMock()
        mock_con.execute.side_effect = RuntimeError("boom")
        udi = self._make_udi(con=mock_con)
        ok, count = udi._post_write_verify(
            "stock_1d", "600519.SH", "1d", "date", "2024-01-01", "2024-12-31", 100,
        )
        assert ok is False
        assert count == -1


class TestRecordSourceConflicts:
    """UDI._record_source_conflicts — 需 mock con。"""

    @staticmethod
    def _make_udi(con=None, read_only=False):
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = object.__new__(UnifiedDataInterface)
        udi.con = con
        udi._read_only_connection = read_only
        udi._logger = MagicMock()
        return udi

    def test_empty_rows(self):
        mock_con = MagicMock()
        udi = self._make_udi(con=mock_con, read_only=False)
        udi._record_source_conflicts([])
        mock_con.register.assert_not_called()

    def test_no_con(self):
        udi = self._make_udi(con=None)
        udi._record_source_conflicts([{"a": 1}])

    def test_success(self):
        mock_con = MagicMock()
        udi = self._make_udi(con=mock_con, read_only=False)
        udi._record_source_conflicts([
            {"stock_code": "600519.SH", "date": "2024-03-15", "field": "close",
             "source_a": "duckdb", "value_a": 100.0, "source_b": "qmt", "value_b": 100.5},
        ])
        mock_con.register.assert_called_once()
        mock_con.unregister.assert_called_once()


class TestLoadAkshareRouting:
    """UDI._load_akshare_routing — classmethod。"""

    def test_idempotent(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        UnifiedDataInterface._AKSHARE_ROUTING_LOADED = False
        UnifiedDataInterface._load_akshare_routing()
        assert UnifiedDataInterface._AKSHARE_ROUTING_LOADED is True
        cfg1 = UnifiedDataInterface._AKSHARE_ROUTING_CFG
        UnifiedDataInterface._load_akshare_routing()  # second call — idempotent
        assert UnifiedDataInterface._AKSHARE_ROUTING_CFG is cfg1

    def test_fallback_on_missing_file(self):
        from data_manager.unified_data_interface import UnifiedDataInterface
        UnifiedDataInterface._AKSHARE_ROUTING_LOADED = False
        with patch("builtins.open", side_effect=FileNotFoundError):
            UnifiedDataInterface._load_akshare_routing()
        cfg = UnifiedDataInterface._AKSHARE_ROUTING_CFG
        assert "index_rules" in cfg
        UnifiedDataInterface._AKSHARE_ROUTING_LOADED = False  # reset


class TestMergeData:
    """UDI._merge_data — 实例方法，合并两源数据。"""

    @staticmethod
    def _make_udi():
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = object.__new__(UnifiedDataInterface)
        udi._logger = MagicMock()
        udi._read_only_connection = False
        udi.con = None
        return udi

    def test_disjoint_merge(self):
        udi = self._make_udi()
        idx_a = pd.to_datetime(["2024-01-02", "2024-01-03"])
        idx_b = pd.to_datetime(["2024-01-04", "2024-01-05"])
        df_a = pd.DataFrame({"open": [10.0, 11.0], "high": [12.0, 13.0],
                             "low": [9.0, 10.0], "close": [11.0, 12.0],
                             "volume": [100, 200]}, index=idx_a)
        df_b = pd.DataFrame({"open": [10.0, 11.0], "high": [12.0, 13.0],
                             "low": [9.0, 10.0], "close": [11.0, 12.0],
                             "volume": [100, 200]}, index=idx_b)
        result = udi._merge_data(df_a, df_b, "600519.SH", "1d")
        assert len(result) == 4

    def test_overlap_merge(self):
        udi = self._make_udi()
        idx = pd.to_datetime(["2024-01-02", "2024-01-03"])
        df_a = pd.DataFrame({"open": [10.0, 11.0], "high": [12.0, 13.0],
                             "low": [9.0, 10.0], "close": [11.0, 12.0],
                             "volume": [100, 200]}, index=idx)
        df_b = pd.DataFrame({"open": [10.5, 11.5], "high": [12.5, 13.5],
                             "low": [9.5, 10.5], "close": [11.5, 12.5],
                             "volume": [110, 210]}, index=idx)
        result = udi._merge_data(df_a, df_b, "600519.SH", "1d")
        assert len(result) == 2


class TestDatFileFresh:
    """UDI._dat_file_is_fresh — 需 mock 文件路径。"""

    @staticmethod
    def _make_udi():
        from data_manager.unified_data_interface import UnifiedDataInterface
        udi = object.__new__(UnifiedDataInterface)
        udi._logger = MagicMock()
        return udi

    def test_qmt_base_none(self):
        udi = self._make_udi()
        with patch("data_manager.dat_binary_reader._load_qmt_base_from_config", return_value=None):
            result = udi._dat_file_is_fresh("600519.SH", "1d")
        assert result is False


# ────────── auto_data_updater ────────────────────────────────────────────


class TestShiftTime:
    """_shift_time 模块级函数。"""

    @staticmethod
    def _call(hhmm, minutes):
        from data_manager.auto_data_updater import _shift_time
        return _shift_time(hhmm, minutes)

    def test_basic_forward(self):
        assert self._call("15:30", 30) == "16:00"

    def test_wrap_midnight(self):
        assert self._call("23:50", 20) == "00:10"

    def test_negative(self):
        assert self._call("01:00", -70) == "23:50"

    def test_zero(self):
        assert self._call("12:00", 0) == "12:00"

    def test_large_offset(self):
        assert self._call("00:00", 1440) == "00:00"  # full day wrap


class TestRunAuditChainCheck:
    """_run_audit_chain_check 模块级函数。"""

    def test_success(self):
        with patch("data_manager.auto_data_updater.run_integrity_check", create=True):
            from data_manager.auto_data_updater import _run_audit_chain_check
            _run_audit_chain_check()  # 不抛异常

    def test_import_error_no_crash(self):
        from data_manager.auto_data_updater import _run_audit_chain_check
        with patch("data_manager.auto_data_updater._run_audit_chain_check.__module__"):
            pass
        # 函数内 try/except 保证不抛异常
        _run_audit_chain_check()


class TestRunCrossSourceCheck:
    """_run_cross_source_consistency_check 模块级函数。"""

    def test_no_alert(self):
        mock_run = MagicMock(return_value={"alert": False, "checked": 30})
        with patch.dict("sys.modules", {"tools.check_cross_source_consistency": MagicMock(run_check=mock_run)}):
            from importlib import reload
            import data_manager.auto_data_updater as adu
            adu._run_cross_source_consistency_check()


class TestAutoUpdaterGetStatus:
    """AutoDataUpdater.get_status。"""

    def test_initial_status(self):
        from data_manager.auto_data_updater import AutoDataUpdater
        updater = object.__new__(AutoDataUpdater)
        updater.running = False
        updater.update_time = "15:30"
        updater.last_update_time = None
        updater.last_update_status = None
        updater.total_updates = 0
        updater.calendar = MagicMock()
        updater.calendar.is_trading_day.return_value = False
        updater.is_trading_day = updater.calendar.is_trading_day
        updater.should_update_today = MagicMock(return_value=False)
        status = updater.get_status()
        assert status["running"] is False
        assert status["total_updates"] == 0


class TestAutoUpdaterStop:
    """AutoDataUpdater.stop。"""

    def test_stop_clears_thread(self):
        from data_manager.auto_data_updater import AutoDataUpdater
        updater = object.__new__(AutoDataUpdater)
        updater.running = True
        mock_thread = MagicMock()
        updater.thread = mock_thread
        updater.stop()
        assert updater.running is False
        assert updater.thread is None
        mock_thread.join.assert_called_once()


# ────────── duckdb_connection_pool 补充 ─────────────────────────────────


class TestConnectionPoolMetrics:
    """DuckDBConnectionManager 锁指标相关。"""

    @staticmethod
    def _make_mgr():
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        mgr = object.__new__(DuckDBConnectionManager)
        mgr._initialized = True
        mgr._lock_metrics = {"attempts": 100, "failures": 1, "wait_times_ms": list(range(100))}
        mgr._connection_count = 3
        return mgr

    def test_connection_count(self):
        mgr = self._make_mgr()
        assert mgr.connection_count == 3

    def test_get_lock_metrics_basic(self):
        mgr = self._make_mgr()
        m = mgr.get_lock_metrics()
        assert m["failure_rate"] == pytest.approx(0.01)
        assert m["p95_wait_ms"] == 95
        assert m["total_attempts"] == 100

    def test_get_lock_metrics_empty(self):
        mgr = self._make_mgr()
        mgr._lock_metrics = {"attempts": 0, "failures": 0, "wait_times_ms": []}
        m = mgr.get_lock_metrics()
        assert m["failure_rate"] == 0.0
        assert m["p95_wait_ms"] == 0.0

    def test_reset_lock_metrics(self):
        mgr = self._make_mgr()
        mgr.reset_lock_metrics()
        assert mgr._lock_metrics["attempts"] == 0
        assert mgr._lock_metrics["failures"] == 0
        assert mgr._lock_metrics["wait_times_ms"] == []


class TestConnectionPoolCheckpoint:
    """DuckDBConnectionManager.checkpoint。"""

    @staticmethod
    def _make_mgr():
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        mgr = object.__new__(DuckDBConnectionManager)
        mgr._initialized = True
        mgr.duckdb_path = ":memory:"
        return mgr

    def test_checkpoint_success(self):
        mgr = self._make_mgr()
        # :memory: 路径会短路返回 True，无需 mock
        assert mgr.checkpoint() is True

    def test_checkpoint_failure(self):
        mgr = self._make_mgr()
        mgr.duckdb_path = "/tmp/test.duckdb"  # 非 :memory: 路径才会走完整逻辑
        mgr.get_write_connection = MagicMock(side_effect=Exception("locked"))
        assert mgr.checkpoint() is False


class TestConnectionPoolOnProcessExit:
    """DuckDBConnectionManager._on_process_exit。"""

    def test_sets_stop_and_checkpoints(self):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        import threading
        mgr = object.__new__(DuckDBConnectionManager)
        mgr._initialized = True
        mgr._checkpoint_stop = threading.Event()
        mgr.duckdb_path = ":memory:"
        mgr.checkpoint = MagicMock(return_value=True)
        mgr._on_process_exit()
        assert mgr._checkpoint_stop.is_set()
        mgr.checkpoint.assert_called_once()


class TestRepairWalIfNeeded:
    """DuckDBConnectionManager._repair_wal_if_needed。"""

    def test_disabled_by_env(self):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        mgr = object.__new__(DuckDBConnectionManager)
        mgr._initialized = True
        with patch.dict(os.environ, {"EASYXT_ENABLE_WAL_AUTO_REPAIR": "0"}):
            assert mgr._repair_wal_if_needed() is False

    def test_already_repaired(self):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        mgr = object.__new__(DuckDBConnectionManager)
        mgr._initialized = True
        mgr._wal_repaired_once = True
        mgr.duckdb_path = ":memory:"
        assert mgr._repair_wal_if_needed() is False

    def test_no_wal_file(self, tmp_path):
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        mgr = object.__new__(DuckDBConnectionManager)
        mgr._initialized = True
        mgr._wal_repaired_once = False
        mgr._connection_count = 0
        mgr.duckdb_path = str(tmp_path / "test.db")
        with patch.dict(os.environ, {"EASYXT_ENABLE_WAL_AUTO_REPAIR": "1"}):
            assert mgr._repair_wal_if_needed() is False

    def test_repair_success(self, tmp_path):
        import threading
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        mgr = object.__new__(DuckDBConnectionManager)
        mgr._initialized = True
        mgr._wal_repaired_once = False
        mgr._connection_count = 0
        db_path = tmp_path / "test.db"
        db_path.touch()
        wal_path = tmp_path / "test.db.wal"
        wal_path.write_text("dummy wal content")
        mgr.duckdb_path = str(db_path)
        mgr._wal_repair_lock = threading.Lock()
        with patch.dict(os.environ, {"EASYXT_ENABLE_WAL_AUTO_REPAIR": "1"}):
            assert mgr._repair_wal_if_needed() is True
        assert mgr._wal_repaired_once is True
        assert not wal_path.exists()


# ────────── core/api_server.py ───────────────────────────────────────────


class TestCheckRateLimit:
    """api_server._check_rate_limit — 滑动窗口限流。"""

    def test_allow_first(self):
        from core.api_server import _check_rate_limit, _rate_buckets
        test_ip = f"test_ip_{time.monotonic()}"
        _rate_buckets.pop(test_ip, None)
        assert _check_rate_limit(test_ip) is True

    def test_rate_limit_exceeded(self):
        from core import api_server
        original = api_server._RATE_LIMIT
        try:
            api_server._RATE_LIMIT = 3
            test_ip = f"test_ip_exceed_{time.monotonic()}"
            api_server._rate_buckets.pop(test_ip, None)
            for _ in range(3):
                assert api_server._check_rate_limit(test_ip) is True
            assert api_server._check_rate_limit(test_ip) is False
        finally:
            api_server._RATE_LIMIT = original

    def test_different_ips_independent(self):
        from core.api_server import _check_rate_limit
        ip_a = f"test_a_{time.monotonic()}"
        ip_b = f"test_b_{time.monotonic()}"
        assert _check_rate_limit(ip_a) is True
        assert _check_rate_limit(ip_b) is True


class TestMarketBroadcasterMetrics:
    """_MarketBroadcaster 可观测指标方法。"""

    @staticmethod
    def _make_broadcaster():
        from core.api_server import _MarketBroadcaster
        b = _MarketBroadcaster()
        return b

    def test_initial_subscriber_count(self):
        b = self._make_broadcaster()
        assert b.subscriber_count("AAPL") == 0

    def test_all_symbols_empty(self):
        b = self._make_broadcaster()
        assert b.all_symbols() == []

    def test_drop_counts_empty(self):
        b = self._make_broadcaster()
        assert b.drop_counts() == {}

    def test_queue_depths_empty(self):
        b = self._make_broadcaster()
        assert b.queue_depths() == {}

    def test_avg_latency_none_when_empty(self):
        b = self._make_broadcaster()
        assert b.avg_publish_latency_ms is None

    def test_max_latency_none_when_empty(self):
        b = self._make_broadcaster()
        assert b.max_publish_latency_ms is None

    def test_avg_latency_with_data(self):
        b = self._make_broadcaster()
        b._latency_window.extend([10.0, 20.0, 30.0])
        assert b.avg_publish_latency_ms == pytest.approx(20.0)

    def test_max_latency_with_data(self):
        b = self._make_broadcaster()
        b._latency_window.extend([10.0, 20.0, 30.0])
        assert b.max_publish_latency_ms == pytest.approx(30.0)

    def test_drop_rate_zero(self):
        b = self._make_broadcaster()
        assert b.drop_rate == 0.0

    def test_drop_rate_nonzero(self):
        b = self._make_broadcaster()
        b._drop_counts["SZ"] = 10
        b._total_attempted = 1000
        assert b.drop_rate == pytest.approx(0.01)

    def test_drop_rate_1m_empty(self):
        b = self._make_broadcaster()
        assert b.drop_rate_1m == 0.0

    def test_drop_rate_1m_low_sample(self):
        b = self._make_broadcaster()
        now = time.monotonic()
        for i in range(5):
            b._event_window.append((now - i, 1, 0))
        assert b.drop_rate_1m == -1.0

    def test_drop_alert_level_ok(self):
        b = self._make_broadcaster()
        assert b.drop_alert_level == "ok"

    def test_drop_alert_level_low_sample(self):
        b = self._make_broadcaster()
        now = time.monotonic()
        for i in range(3):
            b._event_window.append((now - i, 1, 0))
        assert b.drop_alert_level == "ok_low_sample"

    def test_next_seq(self):
        b = self._make_broadcaster()
        s1 = b._next_seq("AAPL")
        s2 = b._next_seq("AAPL")
        assert s1 == 1
        assert s2 == 2

    def test_next_seq_different_symbols(self):
        b = self._make_broadcaster()
        assert b._next_seq("AAPL") == 1
        assert b._next_seq("GOOG") == 1
        assert b._next_seq("AAPL") == 2


class TestIngestTickFromThread:
    """api_server.ingest_tick_from_thread — 线程安全注入。"""

    def test_no_loop_no_crash(self):
        from core import api_server
        original = getattr(api_server, "_server_loop", None)
        api_server._server_loop = None
        api_server.ingest_tick_from_thread("000001.SZ", {"price": 10.0})
        api_server._server_loop = original
