from __future__ import annotations

import builtins
import sys
import types
import warnings
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gui_app.backtest.data_manager import DataManager, DataSource


def _make_manager() -> DataManager:
    mgr = DataManager.__new__(DataManager)
    mgr.preferred_source = None
    mgr.use_local_cache = True
    mgr.verbose = False
    mgr.last_source = None
    mgr.last_data_info = {}
    mgr.duckdb_connection = None
    mgr._duckdb_enabled = False
    mgr.duckdb_path = ":memory:"
    mgr.local_data_manager = MagicMock()
    mgr.source_status = {
        DataSource.DUCKDB: {"available": True, "connected": True, "message": "ok"},
        DataSource.LOCAL: {"available": True, "connected": True, "message": "ok"},
        DataSource.QMT: {"available": True, "connected": True, "message": "ok"},
        DataSource.QSTOCK: {"available": True, "connected": True, "message": "ok"},
        DataSource.AKSHARE: {"available": True, "connected": True, "message": "ok"},
        DataSource.MOCK: {"available": True, "connected": True, "message": "ok"},
    }
    mgr.source_priority = [DataSource.DUCKDB, DataSource.QMT, DataSource.QSTOCK]
    return mgr


def _ohlcv() -> pd.DataFrame:
    idx = pd.to_datetime(["2024-01-02", "2024-01-03"])
    return pd.DataFrame(
        {
            "open": [10.0, 10.2],
            "high": [10.4, 10.5],
            "low": [9.8, 10.0],
            "close": [10.3, 10.4],
            "volume": [1000, 1200],
        },
        index=idx,
    )


def test_force_source_non_daily_for_duckdb_returns_empty():
    mgr = _make_manager()
    result = mgr.get_stock_data(
        "000001.SZ",
        "2024-01-01",
        "2024-01-31",
        period="5m",
        force_source=DataSource.DUCKDB,
    )
    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert mgr.last_source == "duckdb"
    assert mgr.last_data_info["source"] == "duckdb"


def test_auto_fallback_to_next_source_and_cache_save():
    mgr = _make_manager()
    qstock_df = _ohlcv()
    with patch.object(
        mgr,
        "_get_data_from_source",
        side_effect=[RuntimeError("duckdb fail"), pd.DataFrame(), qstock_df],
    ) as mock_get:
        with patch.object(mgr, "_save_to_local_cache") as mock_save:
            result = mgr.get_stock_data("000001.SZ", "2024-01-01", "2024-01-31", period="1d")
    assert not result.empty
    assert mgr.last_source == "qstock"
    assert mgr.last_data_info["source"] == "qstock"
    assert mock_get.call_count == 3
    assert mock_save.called


def test_all_sources_fail_raise_runtime_error():
    mgr = _make_manager()
    with patch.object(mgr, "_get_data_from_source", return_value=pd.DataFrame()):
        with pytest.raises(RuntimeError, match="所有数据源不可用"):
            mgr.get_stock_data("000001.SZ", "2024-01-01", "2024-01-31", period="1d")


def test_save_to_local_cache_non_daily_is_noop():
    mgr = _make_manager()
    mgr.local_data_manager.storage.save_data = MagicMock()
    mgr._save_to_local_cache("000001.SZ", _ohlcv(), period="5m")
    assert not mgr.local_data_manager.storage.save_data.called


def test_clean_data_updates_cleaning_counters():
    mgr = _make_manager()
    mgr.last_data_info = {}
    idx = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    raw = pd.DataFrame(
        {
            "open": [10.0, -1.0, 10.0],
            "high": [11.0, 10.0, 9.0],
            "low": [9.0, 8.0, 9.5],
            "close": [10.5, 9.0, 10.2],
            "volume": [1000, 1000, -1],
        },
        index=idx,
    )
    cleaned = mgr._clean_data(raw)
    assert len(cleaned) == 1
    assert mgr.last_data_info["raw_rows"] == 3
    assert mgr.last_data_info["clean_rows"] == 1
    assert mgr.last_data_info["removed_rows"] == 2


def test_get_connection_status_prefers_first_connected_source():
    mgr = _make_manager()
    mgr.source_priority = [DataSource.QMT, DataSource.DUCKDB]
    mgr.source_status[DataSource.QMT]["connected"] = False
    mgr.source_status[DataSource.DUCKDB]["connected"] = True
    status = mgr.get_connection_status()
    assert status["active_source"] == "duckdb"
    assert status["data_source"] == "real"
    assert "DuckDB" in status["status_message"]


def test_get_connection_status_returns_none_when_no_source_available():
    mgr = _make_manager()
    for s in mgr.source_status.values():
        s["connected"] = False
    status = mgr.get_connection_status()
    assert status["active_source"] == "none"
    assert status["data_source"] == "none"
    assert "没有任何真实数据源" in status["status_message"]


def test_get_status_message_for_each_source():
    mgr = _make_manager()
    assert "DuckDB" in mgr._get_status_message(DataSource.DUCKDB)
    assert "本地缓存" in mgr._get_status_message(DataSource.LOCAL)
    assert "QMT" in mgr._get_status_message(DataSource.QMT)
    assert "QStock" in mgr._get_status_message(DataSource.QSTOCK)
    assert "AKShare" in mgr._get_status_message(DataSource.AKSHARE)
    assert "没有任何真实数据源" in mgr._get_status_message(None)


def test_standardize_columns_renames_and_fills_required():
    mgr = _make_manager()
    df = pd.DataFrame(
        {
            "Open": [10.0],
            "High": [11.0],
            "Low": [9.0],
            "Close": [10.5],
        }
    )
    out = mgr._standardize_columns(df)
    assert set(["open", "high", "low", "close", "volume"]).issubset(set(out.columns))
    assert out["volume"].iloc[0] == 0


def test_validate_data_quality_reports_empty_data_issue():
    mgr = _make_manager()
    report = mgr.validate_data_quality(pd.DataFrame())
    assert report["total_records"] == 0
    assert "数据为空" in report["issues"]


def test_validate_data_quality_detects_missing_values_and_price_relation_issue():
    mgr = _make_manager()
    idx = pd.to_datetime(["2024-01-02", "2024-01-03"])
    df = pd.DataFrame(
        {
            "open": [10.0, 10.0],
            "high": [9.0, 11.0],
            "low": [10.5, 9.0],
            "close": [10.2, None],
            "volume": [1000, 1000],
        },
        index=idx,
    )
    report = mgr.validate_data_quality(df)
    assert "存在缺失值" in report["issues"]
    assert "存在不合理的价格关系" in report["issues"]


def test_validate_data_quality_does_not_emit_pct_change_futurewarning():
    mgr = _make_manager()
    idx = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    df = pd.DataFrame(
        {
            "open": [10.0, 10.0, 10.1],
            "high": [10.5, 10.6, 10.7],
            "low": [9.8, 9.9, 10.0],
            "close": [10.2, None, 10.3],
            "volume": [1000, 1000, 1000],
        },
        index=idx,
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        mgr.validate_data_quality(df)
    assert not any(isinstance(w.message, FutureWarning) for w in caught)


def test_resample_data_empty_returns_empty():
    mgr = _make_manager()
    out = mgr.resample_data(pd.DataFrame(), "1D")
    assert out.empty


def test_resample_data_aggregates_ohlcv():
    mgr = _make_manager()
    idx = pd.date_range("2024-01-01 09:30:00", periods=4, freq="30min")
    df = pd.DataFrame(
        {
            "open": [10.0, 10.2, 10.1, 10.3],
            "high": [10.3, 10.4, 10.5, 10.6],
            "low": [9.9, 10.0, 10.0, 10.1],
            "close": [10.2, 10.1, 10.3, 10.4],
            "volume": [100, 120, 110, 130],
        },
        index=idx,
    )
    out = mgr.resample_data(df, "1H")
    assert not out.empty
    assert set(["open", "high", "low", "close", "volume"]).issubset(set(out.columns))


def test_resample_data_uppercase_h_alias_does_not_emit_futurewarning():
    mgr = _make_manager()
    idx = pd.date_range("2024-01-01 09:30:00", periods=4, freq="30min")
    df = pd.DataFrame(
        {
            "open": [10.0, 10.2, 10.1, 10.3],
            "high": [10.3, 10.4, 10.5, 10.6],
            "low": [9.9, 10.0, 10.0, 10.1],
            "close": [10.2, 10.1, 10.3, 10.4],
            "volume": [100, 120, 110, 130],
        },
        index=idx,
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        mgr.resample_data(df, "1H")
    assert not any(isinstance(w.message, FutureWarning) for w in caught)


def test_get_data_from_source_dispatches_all_sources():
    mgr = _make_manager()
    with patch.object(mgr, "_get_duckdb_data", return_value=_ohlcv()) as m1:
        with patch.object(mgr, "_get_local_data", return_value=_ohlcv()) as m2:
            with patch.object(mgr, "_get_qmt_data", return_value=_ohlcv()) as m3:
                with patch.object(mgr, "_get_qstock_data", return_value=_ohlcv()) as m4:
                    with patch.object(mgr, "_get_akshare_data", return_value=_ohlcv()) as m5:
                        assert not mgr._get_data_from_source(DataSource.DUCKDB, "000001.SZ", "2024-01-01", "2024-01-31", "1d").empty
                        assert not mgr._get_data_from_source(DataSource.LOCAL, "000001.SZ", "2024-01-01", "2024-01-31", "1d").empty
                        assert not mgr._get_data_from_source(DataSource.QMT, "000001.SZ", "2024-01-01", "2024-01-31", "1d").empty
                        assert not mgr._get_data_from_source(DataSource.QSTOCK, "000001.SZ", "2024-01-01", "2024-01-31", "1d").empty
                        assert not mgr._get_data_from_source(DataSource.AKSHARE, "000001.SZ", "2024-01-01", "2024-01-31", "1d").empty
                        with pytest.raises(RuntimeError, match="不支持的数据源"):
                            mgr._get_data_from_source(cast(Any, "mock"), "000001.SZ", "2024-01-01", "2024-01-31", "1d")
    assert m1.called and m2.called and m3.called and m4.called and m5.called


def test_safe_format_date_failure_returns_none():
    mgr = _make_manager()

    class _BadDate:
        pass

    out = mgr._safe_format_date(_BadDate())
    assert out is None


def test_get_qmt_data_import_error_returns_empty():
    mgr = _make_manager()
    real_import = builtins.__import__
    cached_xtdata = sys.modules.pop("xtquant.xtdata", None)
    cached_xtquant = sys.modules.get("xtquant")
    cached_xtquant_attr = getattr(cached_xtquant, "xtdata", None) if cached_xtquant else None
    if cached_xtquant is not None and hasattr(cached_xtquant, "xtdata"):
        delattr(cached_xtquant, "xtdata")

    def _fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "xtquant.xtdata" or (name == "xtquant" and fromlist and "xtdata" in fromlist):
            raise ImportError("xtquant missing")
        return real_import(name, globals, locals, fromlist, level)

    try:
        with patch("builtins.__import__", side_effect=_fake_import):
            df = mgr._get_qmt_data("000001.SZ", "2024-01-01", "2024-01-31", "1d", "none")
    finally:
        if cached_xtdata is not None:
            sys.modules["xtquant.xtdata"] = cached_xtdata
        if cached_xtquant is not None and cached_xtquant_attr is not None:
            setattr(cached_xtquant, "xtdata", cached_xtquant_attr)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_get_qstock_data_success_with_fake_module():
    mgr = _make_manager()
    fake_qs = types.ModuleType("qstock")
    fake_qs.get_data = lambda code, start, end: pd.DataFrame(
        {"open": [10.0], "high": [10.2], "low": [9.9], "close": [10.1], "volume": [1000]}
    )
    with patch("importlib.import_module", return_value=fake_qs):
        df = mgr._get_qstock_data("000001.SZ", "2024-01-01", "2024-01-31", "1d")
    assert not df.empty


def test_get_qstock_data_failure_returns_empty():
    mgr = _make_manager()
    with patch("importlib.import_module", side_effect=ImportError("qstock missing")):
        df = mgr._get_qstock_data("000001.SZ", "2024-01-01", "2024-01-31", "1d")
    assert df.empty


def test_get_akshare_data_retries_then_success():
    mgr = _make_manager()
    fake_ak = types.ModuleType("akshare")
    calls = {"n": 0}

    def _hist(**kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("Server disconnected")
        return pd.DataFrame(
            {
                "日期": ["2024-01-02"],
                "开盘": [10.0],
                "最高": [10.3],
                "最低": [9.8],
                "收盘": [10.1],
                "成交量": [1000],
                "成交额": [10100.0],
            }
        )

    fake_ak.stock_zh_a_hist = _hist  # type: ignore[attr-defined]
    with patch("importlib.import_module", return_value=fake_ak):
        df = mgr._get_akshare_data("000001.SZ", "2024-01-01", "2024-01-31", "1d")
    assert not df.empty
    assert calls["n"] == 3


def test_get_akshare_data_all_retries_fail_returns_empty():
    mgr = _make_manager()
    fake_ak = types.ModuleType("akshare")
    fake_ak.stock_zh_a_hist = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("timeout"))  # type: ignore[attr-defined]
    with patch("importlib.import_module", return_value=fake_ak):
        df = mgr._get_akshare_data("000001.SZ", "2024-01-01", "2024-01-31", "1d")
    assert df.empty


def test_refresh_source_status_rechecks_and_prints():
    mgr = _make_manager()
    refreshed = {k: {"available": True, "connected": True, "message": "refreshed"} for k in mgr.source_status.keys()}
    with patch.object(mgr, "_check_all_sources", return_value=refreshed):
        with patch.object(mgr, "_print_initialization_status") as mock_print:
            mgr.refresh_source_status()
    assert mgr.source_status[DataSource.DUCKDB]["message"] == "refreshed"
    assert mock_print.called


def test_print_initialization_status_silent_when_not_verbose():
    mgr = _make_manager()
    with patch("builtins.print") as mock_print:
        mgr._print_initialization_status()
    mock_print.assert_not_called()


def test_check_qstock_status_skips_probe_off_main_thread():
    mgr = _make_manager()
    with patch("threading.current_thread", return_value=MagicMock(name="worker")):
        with patch("threading.main_thread", return_value=MagicMock(name="main")):
            with patch("importlib.util.find_spec", return_value=object()):
                with patch("importlib.import_module", side_effect=AssertionError("should not import qstock")):
                    status = mgr._check_qstock_status()
    assert status["available"] is True
    assert status["connected"] is False
    assert "后台线程" in status["message"]


def test_update_local_cache_disabled_is_noop():
    mgr = _make_manager()
    mgr.local_data_manager = None
    before = dict(mgr.source_status[DataSource.LOCAL])
    mgr.update_local_cache()
    assert mgr.source_status[DataSource.LOCAL] == before


def test_update_local_cache_updates_status():
    mgr = _make_manager()
    mgr.local_data_manager = MagicMock()
    with patch.object(mgr, "_check_local_status", return_value={"available": True, "connected": False, "message": "updated"}):
        mgr.update_local_cache(symbols=["000001.SZ"])
    assert mgr.local_data_manager.update_data.called
    assert mgr.source_status[DataSource.LOCAL]["message"] == "updated"


def test_get_local_cache_status_disabled_and_enabled():
    mgr = _make_manager()
    mgr.local_data_manager = None
    assert mgr.get_local_cache_status() == {"enabled": False}
    mgr.local_data_manager = MagicMock()
    mgr.local_data_manager.get_statistics.return_value = {
        "total_symbols": 2,
        "total_records": 10,
        "total_size_mb": 1.5,
        "latest_data_date": "2024-01-31",
    }
    status = mgr.get_local_cache_status()
    assert status["enabled"] is True
    assert status["total_symbols"] == 2


def test_clear_local_cache_single_symbol_and_all_files(tmp_path: Path):
    mgr = _make_manager()
    storage = MagicMock()
    storage.root_dir = tmp_path
    mgr.local_data_manager = MagicMock()
    mgr.local_data_manager.storage = storage
    storage.delete_data.return_value = True
    with patch.object(mgr, "_check_local_status", return_value={"available": True, "connected": False, "message": "cleared"}):
        mgr.clear_local_cache("000001.SZ")
    assert storage.delete_data.called

    daily_dir = tmp_path / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    (daily_dir / "a.parquet").write_text("x", encoding="utf-8")
    (daily_dir / "b.parquet").write_text("x", encoding="utf-8")
    with patch.object(mgr, "_check_local_status", return_value={"available": True, "connected": False, "message": "cleared2"}):
        mgr.clear_local_cache(None)
    assert not (daily_dir / "a.parquet").exists()
    assert not (daily_dir / "b.parquet").exists()


def test_preload_data_continues_on_single_symbol_error():
    mgr = _make_manager()

    def _get(*args, **kwargs):
        if args[0] == "000002.SZ":
            raise RuntimeError("boom")
        return _ohlcv()

    with patch.object(mgr, "get_stock_data", side_effect=_get) as mock_get:
        mgr.preload_data(["000001.SZ", "000002.SZ", "000003.SZ"], "2024-01-01", "2024-01-31")
    assert mock_get.call_count == 3
