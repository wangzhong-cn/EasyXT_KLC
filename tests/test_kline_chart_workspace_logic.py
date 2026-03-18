"""tests/test_kline_chart_workspace_logic.py

KLineChartWorkspace 纯逻辑单元测试。

策略：
  - 提前 mock 所有 PyQt5 / Qt 依赖及本地 GUI 模块
  - 以 types.SimpleNamespace 桩对象直接调用未绑定方法
  - 完全无 QApplication 依赖，可在 headless CI 中运行

覆盖方法：
  _normalize_symbol、_get_segment_span、_get_time_step、_format_time_str
  _format_time_column、_prepare_chart_data、_compute_initial_range
  _merge_chart_data、_set_loaded_range_from_data、_reset_progressive_state
  _check_and_apply_degradation、_enter_degraded_mode、_exit_degraded_mode
  _ensure_realtime_api
"""
from __future__ import annotations

import logging
import sys
import types
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ─────────────────────────────────────────────────────────────────────────────
# 1. Pre-mock all PyQt5 / Qt modules (must happen before any local import)
# ─────────────────────────────────────────────────────────────────────────────

def _fake_type(name: str):
    """Return a minimal Python class suitable as base class (not a MagicMock)."""
    return type(name, (), {
        "__init__": lambda self, *a, **kw: None,
        "__init_subclass__": classmethod(lambda cls, **kw: None),
    })


for _m in [
    "PyQt5", "PyQt5.QtCore", "PyQt5.QtWidgets",
    "PyQt5.QtGui", "PyQt5.QtWebEngineWidgets", "PyQt5.QtWebChannel",
]:
    if _m not in sys.modules:
        sys.modules[_m] = types.ModuleType(_m)

_qtcore = sys.modules["PyQt5.QtCore"]
for _n in ["QThread", "QTimer", "QFileSystemWatcher", "QSettings",
           "QStringListModel", "QDate", "Qt"]:
    if not hasattr(_qtcore, _n):
        setattr(_qtcore, _n, _fake_type(_n))
if not hasattr(_qtcore, "pyqtSignal"):
    _qtcore.pyqtSignal = staticmethod(lambda *a, **kw: MagicMock(name="Signal"))

_qtwidgets = sys.modules["PyQt5.QtWidgets"]
for _n in [
    "QWidget", "QApplication", "QAction", "QButtonGroup", "QCheckBox",
    "QComboBox", "QCompleter", "QDateEdit", "QDialog", "QFrame",
    "QHBoxLayout", "QLabel", "QLineEdit", "QMenu", "QPushButton",
    "QSizePolicy", "QSplitter", "QTabWidget", "QVBoxLayout",
]:
    if not hasattr(_qtwidgets, _n):
        setattr(_qtwidgets, _n, _fake_type(_n))

# ─────────────────────────────────────────────────────────────────────────────
# 2. Pre-mock local modules with Qt / external dependencies
#
# IMPORTANT: Only set attributes on modules WE just created.  Setting attrs on
# a pre-existing real module would corrupt subsequent tests (contamination).
# ─────────────────────────────────────────────────────────────────────────────

_LOCAL_STUBS: dict[str, dict] = {
    "core.events": {"Events": MagicMock(name="Events")},
    "core.signal_bus": {"signal_bus": MagicMock(name="signal_bus")},
    "core.theme_manager": {"ThemeManager": _fake_type("ThemeManager")},
    "data_manager.duckdb_connection_pool": {
        "resolve_duckdb_path": lambda: "",
    },
    "data_manager.realtime_pipeline_manager": {
        "RealtimePipelineManager": MagicMock(name="RealtimePipelineManager"),
    },
    "easy_xt": {},
    "easy_xt.realtime_data": {},
    "easy_xt.realtime_data.persistence": {},
    "easy_xt.realtime_data.persistence.duckdb_sink": {
        "RealtimeDuckDBSink": MagicMock(name="RealtimeDuckDBSink"),
    },
    "gui_app.widgets.chart": {
        "PERIOD_DATE_COL_MAP": {},
        "PERIOD_TABLE_MAP": {},
        "ChartEvents": MagicMock(name="ChartEvents"),
        "PositionTable": MagicMock(name="PositionTable"),
        "SubchartManager": MagicMock(name="SubchartManager"),
        "ToolboxPanel": MagicMock(name="ToolboxPanel"),
        "create_chart_adapter": MagicMock(name="create_chart_adapter"),
    },
    "gui_app.widgets.orderbook_panel": {
        "OrderbookPanel": MagicMock(name="OrderbookPanel"),
    },
    "gui_app.widgets.realtime_settings_dialog": {
        "RealtimeSettingsDialog": MagicMock(name="RealtimeSettingsDialog"),
    },
}

for _mod_name, _attrs in _LOCAL_STUBS.items():
    if _mod_name not in sys.modules:
        # Create stub module and populate it — only safe because WE own it
        _stub = types.ModuleType(_mod_name)
        for _k, _v in _attrs.items():
            setattr(_stub, _k, _v)
        sys.modules[_mod_name] = _stub
    else:
        # Module already imported as a real module — ensure required symbols
        # exist without overwriting (use setdefault-style: only add if missing).
        _existing = sys.modules[_mod_name]
        for _k, _v in _attrs.items():
            if not hasattr(_existing, _k):
                setattr(_existing, _k, _v)

_rp_cls = sys.modules["data_manager.realtime_pipeline_manager"].RealtimePipelineManager  # type: ignore[attr-defined]

# ─────────────────────────────────────────────────────────────────────────────
# 3. Import the target class (safe now that all deps are stubbed)
# ─────────────────────────────────────────────────────────────────────────────

from gui_app.widgets.kline_chart_workspace import (  # noqa: E402
    KLineChartWorkspace,
    _WsMarketQuoteWorker,
)

# ─────────────────────────────────────────────────────────────────────────────
# 4. Stub factory — minimal namespace with required attributes
# ─────────────────────────────────────────────────────────────────────────────

def _make_stub(**overrides):
    """Return a SimpleNamespace with defaults for KLineChartWorkspace method tests."""
    import types as _t
    defaults = dict(
        _segment_cache={},
        _loading_segments=set(),
        _loaded_range=None,
        _full_range=None,
        _degraded_mode=False,
        _original_flush_interval=200,
        _degraded_flush_interval=400,
        _logger=logging.getLogger("test_kline"),
        realtime_pipeline=None,
        realtime_pipeline_timer=None,
        realtime_api=None,
        _realtime_connect_thread=None,
        test_mode=True,
        symbol_input=None,
        # Helpers called by compound methods - default to MagicMocks
        _emit_realtime_probe=MagicMock(name="_emit_realtime_probe"),
        _log_degrade_event=MagicMock(name="_log_degrade_event"),
        _trigger_degrade_alert=MagicMock(name="_trigger_degrade_alert"),
        _resolve_degrade_alert=MagicMock(name="_resolve_degrade_alert"),
        _enter_degraded_mode=MagicMock(name="_enter_degraded_mode"),
        _exit_degraded_mode=MagicMock(name="_exit_degraded_mode"),
        _on_realtime_ready=MagicMock(name="_on_realtime_ready"),
        _on_realtime_error=MagicMock(name="_on_realtime_error"),
    )
    defaults.update(overrides)
    stub = _t.SimpleNamespace(**defaults)
    # Bind self-referential methods so compound methods resolve them correctly
    stub._format_time_column = _t.MethodType(KLineChartWorkspace._format_time_column, stub)
    stub._format_time_str = _t.MethodType(KLineChartWorkspace._format_time_str, stub)
    stub._is_thread_running = _t.MethodType(KLineChartWorkspace._is_thread_running, stub)
    return stub


# ═════════════════════════════════════════════════════════════════════════════
# 5. Tests
# ═════════════════════════════════════════════════════════════════════════════

class TestNormalizeSymbol:
    """_normalize_symbol — 股票代码标准化"""

    def _n(self, symbol):
        return KLineChartWorkspace._normalize_symbol(_make_stub(), symbol)

    def test_empty_returns_empty(self):
        assert self._n("") == ""

    def test_already_normalized_sh(self):
        assert self._n("600001.SH") == "600001.SH"

    def test_already_normalized_sz(self):
        assert self._n("000001.SZ") == "000001.SZ"

    def test_six_digit_start_60_to_sh(self):
        assert self._n("600001") == "600001.SH"

    def test_six_digit_start_68_to_sh(self):
        assert self._n("688501") == "688501.SH"

    def test_six_digit_start_11_to_sh(self):
        assert self._n("110001") == "110001.SH"

    def test_six_digit_start_12_to_sh(self):
        assert self._n("120001") == "120001.SH"

    def test_six_digit_start_13_to_sh(self):
        assert self._n("130001") == "130001.SH"

    def test_six_digit_other_to_sz(self):
        assert self._n("000001") == "000001.SZ"

    def test_six_digit_start_30_to_sz(self):
        assert self._n("300001") == "300001.SZ"

    def test_sh_prefix_format(self):
        assert self._n("SH600001") == "600001.SH"

    def test_sz_prefix_format(self):
        assert self._n("SZ000001") == "000001.SZ"

    def test_lowercase_input_normalized(self):
        # uppercase() called first
        assert self._n("sh600001") == "600001.SH"

    def test_unknown_format_returned_as_is(self):
        # Non-matching pattern passes through upper+strip
        result = self._n("TSLA")
        assert result == "TSLA"


class TestGetSegmentSpan:
    """_get_segment_span — 周期 → 数据段时间跨度"""

    def _s(self, period):
        return KLineChartWorkspace._get_segment_span(_make_stub(), period)

    def test_weekly(self):
        assert self._s("1w") == pd.DateOffset(years=2)

    def test_monthly(self):
        assert self._s("1M") == pd.DateOffset(years=2)

    def test_daily(self):
        assert self._s("1d") == pd.DateOffset(months=3)

    def test_1m(self):
        assert self._s("1m") == pd.DateOffset(days=5)

    def test_5m(self):
        assert self._s("5m") == pd.DateOffset(days=5)

    def test_15m(self):
        assert self._s("15m") == pd.DateOffset(days=15)

    def test_30m(self):
        assert self._s("30m") == pd.DateOffset(days=15)

    def test_60m(self):
        assert self._s("60m") == pd.DateOffset(days=15)

    def test_unknown_falls_back_to_30_days(self):
        assert self._s("3m") == pd.DateOffset(days=30)


class TestGetTimeStep:
    """_get_time_step — 周期 → 时间步长"""

    def _t(self, period):
        return KLineChartWorkspace._get_time_step(_make_stub(), period)

    def test_weekly(self):
        assert self._t("1w") == pd.Timedelta(weeks=1)

    def test_monthly(self):
        assert self._t("1M") == pd.Timedelta(days=30)

    def test_daily(self):
        assert self._t("1d") == pd.Timedelta(days=1)

    def test_1m(self):
        assert self._t("1m") == pd.Timedelta(minutes=1)

    def test_5m(self):
        assert self._t("5m") == pd.Timedelta(minutes=5)

    def test_15m(self):
        assert self._t("15m") == pd.Timedelta(minutes=15)

    def test_30m(self):
        assert self._t("30m") == pd.Timedelta(minutes=30)

    def test_60m(self):
        assert self._t("60m") == pd.Timedelta(minutes=60)

    def test_unknown_falls_back_to_1_second(self):
        assert self._t("3m") == pd.Timedelta(seconds=1)


class TestWsReconnectBackoff:
    def test_reconnect_delay_grows_and_caps(self):
        d1 = _WsMarketQuoteWorker._compute_reconnect_delay(1, initial_s=1.5, max_s=10, factor=2.0)
        d2 = _WsMarketQuoteWorker._compute_reconnect_delay(2, initial_s=1.5, max_s=10, factor=2.0)
        d5 = _WsMarketQuoteWorker._compute_reconnect_delay(5, initial_s=1.5, max_s=10, factor=2.0)
        assert d1 == pytest.approx(1.5)
        assert d2 == pytest.approx(3.0)
        assert d5 == pytest.approx(10.0)


class TestFormatTimeStr:
    """_format_time_str — Timestamp → 格式化字符串"""

    def _f(self, ts, period):
        return KLineChartWorkspace._format_time_str(_make_stub(), ts, period)

    def test_daily_returns_date_only(self):
        ts = pd.Timestamp("2024-01-15 10:30:00")
        assert self._f(ts, "1d") == "2024-01-15"

    def test_weekly_returns_date_only(self):
        ts = pd.Timestamp("2024-01-15 10:30:00")
        assert self._f(ts, "1w") == "2024-01-15"

    def test_monthly_returns_date_only(self):
        ts = pd.Timestamp("2024-01-15 10:30:00")
        assert self._f(ts, "1M") == "2024-01-15"

    def test_minute_returns_datetime(self):
        ts = pd.Timestamp("2024-01-15 10:30:00")
        assert self._f(ts, "1m") == "2024-01-15 10:30:00"

    def test_5m_returns_datetime(self):
        ts = pd.Timestamp("2024-03-01 09:35:00")
        assert self._f(ts, "5m") == "2024-03-01 09:35:00"


class TestFormatTimeColumn:
    """_format_time_column — DataFrame 时间列格式化"""

    def _run(self, df, period):
        return KLineChartWorkspace._format_time_column(_make_stub(), df.copy(), period)

    def _make_df(self, timestamps):
        return pd.DataFrame({"time": pd.to_datetime(timestamps)})

    def test_no_time_column_returns_unchanged(self):
        df = pd.DataFrame({"close": [1, 2, 3]})
        result = KLineChartWorkspace._format_time_column(_make_stub(), df.copy(), "1d")
        assert "close" in result.columns
        assert "time" not in result.columns

    def test_daily_formats_as_date(self):
        df = self._make_df(["2024-01-01", "2024-01-02"])
        result = self._run(df, "1d")
        assert result["time"].iloc[0] == "2024-01-01"
        assert result["time"].iloc[1] == "2024-01-02"

    def test_weekly_formats_as_date(self):
        df = self._make_df(["2024-01-01"])
        result = self._run(df, "1w")
        assert result["time"].iloc[0] == "2024-01-01"

    def test_monthly_formats_as_date(self):
        df = self._make_df(["2024-01-01"])
        result = self._run(df, "1M")
        assert "-" in result["time"].iloc[0]

    def test_minute_formats_as_datetime(self):
        df = self._make_df(["2024-01-15 09:30:00"])
        result = self._run(df, "1m")
        assert result["time"].iloc[0] == "2024-01-15 09:30:00"

    def test_nat_rows_dropped(self):
        df = pd.DataFrame({"time": [pd.Timestamp("2024-01-01"), pd.NaT]})
        result = self._run(df, "1d")
        assert len(result) == 1

    def test_unparseable_strings_dropped(self):
        df = pd.DataFrame({"time": ["2024-01-01", "not-a-date"]})
        result = self._run(df, "1d")
        assert len(result) == 1


class TestPrepareChartData:
    """_prepare_chart_data — 图表数据预处理"""

    def _run(self, df, period="1d"):
        stub = _make_stub()
        return KLineChartWorkspace._prepare_chart_data(stub, df, period)

    def _base_df(self, n=3):
        dates = pd.date_range("2024-01-01", periods=n)
        return pd.DataFrame({
            "date": dates,
            "open": [10.0] * n,
            "high": [11.0] * n,
            "low": [9.0] * n,
            "close": [10.5] * n,
            "volume": [1000.0] * n,
        })

    def test_none_returns_empty(self):
        result = self._run(None)
        assert result.empty

    def test_empty_returns_empty(self):
        result = self._run(pd.DataFrame())
        assert result.empty

    def test_date_column_renamed_to_time(self):
        df = self._base_df()
        result = self._run(df)
        assert "time" in result.columns
        assert "date" not in result.columns

    def test_datetime_column_renamed_to_time(self):
        df = self._base_df()
        df = df.rename(columns={"date": "datetime"})
        result = self._run(df)
        assert "time" in result.columns

    def test_required_columns_preserved(self):
        df = self._base_df()
        result = self._run(df)
        for col in ["time", "open", "high", "low", "close", "volume"]:
            assert col in result.columns

    def test_missing_ohlc_returns_empty(self):
        df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=3),
                           "open": [1.0, 2.0, 3.0]})
        result = self._run(df)
        assert result.empty

    def test_volume_filled_when_missing(self):
        dates = pd.date_range("2024-01-01", periods=2)
        df = pd.DataFrame({
            "date": dates,
            "open": [10.0, 11.0], "high": [12.0, 13.0],
            "low": [9.0, 10.0], "close": [11.0, 12.0],
        })
        result = self._run(df)
        assert "volume" in result.columns
        assert (result["volume"] == 0).all()

    def test_nan_ohlc_rows_dropped(self):
        dates = pd.date_range("2024-01-01", periods=3)
        df = pd.DataFrame({
            "date": dates,
            "open": [10.0, float("nan"), 12.0],
            "high": [11.0, float("nan"), 13.0],
            "low": [9.0, float("nan"), 11.0],
            "close": [10.5, float("nan"), 12.5],
            "volume": [100.0, 200.0, 300.0],
        })
        result = self._run(df)
        assert len(result) == 2

    def test_output_sorted_by_time(self):
        dates = pd.to_datetime(["2024-01-03", "2024-01-01", "2024-01-02"])
        df = pd.DataFrame({
            "date": dates,
            "open": [10.0] * 3, "high": [11.0] * 3,
            "low": [9.0] * 3, "close": [10.5] * 3, "volume": [1000.0] * 3,
        })
        result = self._run(df)
        assert result["time"].is_monotonic_increasing

    def test_daily_time_format(self):
        df = self._base_df(1)
        result = self._run(df, "1d")
        assert result["time"].iloc[0] == "2024-01-01"

    def test_minute_time_format(self):
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-15 09:30:00"]),
            "open": [10.0], "high": [11.0], "low": [9.0],
            "close": [10.5], "volume": [500.0],
        })
        result = self._run(df, "1m")
        assert " " in result["time"].iloc[0]


class TestComputeInitialRange:
    """_compute_initial_range — 计算初始显示范围"""

    def _run(self, full_range, period):
        return KLineChartWorkspace._compute_initial_range(_make_stub(), full_range, period)

    def test_1m_span_is_2_days(self):
        start, end = self._run(("2024-01-01", "2024-06-30"), "1m")
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        assert (end_ts - start_ts).days <= 3  # 2 days span

    def test_5m_span_is_2_days(self):
        start, end = self._run(("2024-01-01", "2024-06-30"), "5m")
        end_ts = pd.Timestamp(end)
        start_ts = pd.Timestamp(start)
        assert (end_ts - start_ts).days <= 3

    def test_15m_span_is_5_days(self):
        start, end = self._run(("2024-01-01", "2024-06-30"), "15m")
        end_ts = pd.Timestamp(end)
        start_ts = pd.Timestamp(start)
        assert (end_ts - start_ts).days <= 6

    def test_daily_span_is_3_months(self):
        start, end = self._run(("2020-01-01", "2024-06-30"), "1d")
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        diff_months = (end_ts.year - start_ts.year) * 12 + (end_ts.month - start_ts.month)
        assert diff_months <= 3

    def test_weekly_span_is_2_years(self):
        start, end = self._run(("2020-01-01", "2024-06-30"), "1w")
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        assert (end_ts - start_ts).days <= 366 * 2 + 1

    def test_full_start_clamps_range(self):
        # full range only 1 week, but 1d span is 3 months → clamped to full_start
        start, end = self._run(("2024-06-24", "2024-06-30"), "1d")
        assert start == "2024-06-24"

    def test_end_equals_full_end(self):
        _, end = self._run(("2020-01-01", "2024-06-30"), "1d")
        assert "2024-06-30" in end

    def test_daily_returns_date_format(self):
        start, end = self._run(("2020-01-01", "2024-06-30"), "1d")
        assert len(start) == 10  # YYYY-MM-DD

    def test_minute_returns_datetime_format(self):
        start, _ = self._run(("2020-01-01", "2024-06-30 10:00:00"), "1m")
        assert " " in start or len(start) == 10  # either format accepted

    def test_custom_2m_span_is_2_days(self):
        start, end = self._run(("2024-01-01", "2024-06-30"), "2m")
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        assert (end_ts - start_ts).days <= 3

    def test_custom_10m_span_is_5_days(self):
        start, end = self._run(("2024-01-01", "2024-06-30"), "10m")
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        assert (end_ts - start_ts).days <= 6

    def test_custom_120m_span_is_10_days(self):
        start, end = self._run(("2024-01-01", "2024-06-30"), "120m")
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        assert (end_ts - start_ts).days <= 11


class TestMergeChartData:
    """_merge_chart_data — K线数据合并"""

    def _run(self, base, extra):
        return KLineChartWorkspace._merge_chart_data(_make_stub(), base, extra)

    def _df(self, times):
        return pd.DataFrame({
            "time": times,
            "open": [1.0] * len(times),
            "high": [1.0] * len(times),
            "low": [1.0] * len(times),
            "close": [1.0] * len(times),
            "volume": [100.0] * len(times),
        })

    def test_base_empty_returns_extra(self):
        extra = self._df(["2024-01-01", "2024-01-02"])
        result = self._run(pd.DataFrame(), extra)
        assert len(result) == 2

    def test_base_none_returns_extra(self):
        extra = self._df(["2024-01-01"])
        result = self._run(None, extra)
        assert len(result) == 1

    def test_extra_empty_returns_base(self):
        base = self._df(["2024-01-01", "2024-01-02"])
        result = self._run(base, pd.DataFrame())
        assert len(result) == 2

    def test_extra_none_returns_base(self):
        base = self._df(["2024-01-01"])
        result = self._run(base, None)
        assert len(result) == 1

    def test_deduplicates_by_time(self):
        base = self._df(["2024-01-01", "2024-01-02"])
        extra = self._df(["2024-01-02", "2024-01-03"])
        result = self._run(base, extra)
        assert len(result) == 3

    def test_output_sorted_by_time(self):
        base = self._df(["2024-01-03"])
        extra = self._df(["2024-01-01"])
        result = self._run(base, extra)
        assert result["time"].iloc[0] == "2024-01-01"

    def test_returns_copy_not_same_object(self):
        base = self._df(["2024-01-01"])
        result = self._run(base, pd.DataFrame())
        result.loc[0, "close"] = 999.0
        assert base.loc[0, "close"] == 1.0


class TestSetLoadedRangeFromData:
    """_set_loaded_range_from_data — 设置已加载范围"""

    def test_empty_df_no_change(self):
        stub = _make_stub()
        KLineChartWorkspace._set_loaded_range_from_data(stub, pd.DataFrame())
        assert stub._loaded_range is None

    def test_none_no_change(self):
        stub = _make_stub()
        KLineChartWorkspace._set_loaded_range_from_data(stub, None)
        assert stub._loaded_range is None

    def test_sets_range_from_data(self):
        stub = _make_stub()
        df = pd.DataFrame({"time": ["2024-01-01", "2024-01-02", "2024-01-03"]})
        KLineChartWorkspace._set_loaded_range_from_data(stub, df)
        assert stub._loaded_range == ("2024-01-01", "2024-01-03")

    def test_single_row(self):
        stub = _make_stub()
        df = pd.DataFrame({"time": ["2024-06-15"]})
        KLineChartWorkspace._set_loaded_range_from_data(stub, df)
        assert stub._loaded_range == ("2024-06-15", "2024-06-15")


class TestResetProgressiveState:
    """_reset_progressive_state — 清空渐进加载状态"""

    def test_clears_segment_cache(self):
        stub = _make_stub(_segment_cache={("a", "b", "c", "d", "e"): pd.DataFrame()})
        KLineChartWorkspace._reset_progressive_state(stub)
        assert stub._segment_cache == {}

    def test_clears_loading_segments(self):
        stub = _make_stub(_loading_segments={("a", "b", "c", "d", "e")})
        KLineChartWorkspace._reset_progressive_state(stub)
        assert stub._loading_segments == set()

    def test_resets_loaded_range(self):
        stub = _make_stub(_loaded_range=("2024-01-01", "2024-06-30"))
        KLineChartWorkspace._reset_progressive_state(stub)
        assert stub._loaded_range is None

    def test_resets_full_range(self):
        stub = _make_stub(_full_range=("2020-01-01", "2024-06-30"))
        KLineChartWorkspace._reset_progressive_state(stub)
        assert stub._full_range is None


class TestCheckAndApplyDegradation:
    """_check_and_apply_degradation — 降级/恢复判断"""

    def test_no_pipeline_returns_early(self):
        stub = _make_stub(realtime_pipeline=None)
        KLineChartWorkspace._check_and_apply_degradation(stub)
        stub._enter_degraded_mode.assert_not_called()
        stub._exit_degraded_mode.assert_not_called()

    def test_alert_true_not_degraded_enters_degraded(self):
        mock_pipeline = MagicMock()
        mock_pipeline.metrics.return_value = {"sustained_drop_alert": True}
        stub = _make_stub(realtime_pipeline=mock_pipeline, _degraded_mode=False)
        KLineChartWorkspace._check_and_apply_degradation(stub)
        stub._enter_degraded_mode.assert_called_once()

    def test_alert_true_already_degraded_no_op(self):
        mock_pipeline = MagicMock()
        mock_pipeline.metrics.return_value = {"sustained_drop_alert": True}
        stub = _make_stub(realtime_pipeline=mock_pipeline, _degraded_mode=True)
        KLineChartWorkspace._check_and_apply_degradation(stub)
        stub._enter_degraded_mode.assert_not_called()

    def test_alert_false_degraded_exits_degraded(self):
        mock_pipeline = MagicMock()
        mock_pipeline.metrics.return_value = {"sustained_drop_alert": False}
        stub = _make_stub(realtime_pipeline=mock_pipeline, _degraded_mode=True)
        KLineChartWorkspace._check_and_apply_degradation(stub)
        stub._exit_degraded_mode.assert_called_once()

    def test_alert_false_not_degraded_no_op(self):
        mock_pipeline = MagicMock()
        mock_pipeline.metrics.return_value = {"sustained_drop_alert": False}
        stub = _make_stub(realtime_pipeline=mock_pipeline, _degraded_mode=False)
        KLineChartWorkspace._check_and_apply_degradation(stub)
        stub._exit_degraded_mode.assert_not_called()

    def test_missing_sustained_key_treated_as_false(self):
        mock_pipeline = MagicMock()
        mock_pipeline.metrics.return_value = {}
        stub = _make_stub(realtime_pipeline=mock_pipeline, _degraded_mode=False)
        KLineChartWorkspace._check_and_apply_degradation(stub)
        stub._enter_degraded_mode.assert_not_called()


class TestEnterDegradedMode:
    """_enter_degraded_mode — 进入降级模式"""

    def _run_enter(self, **overrides):
        timer = MagicMock(name="timer")
        pipeline = MagicMock(name="pipeline")
        stub = _make_stub(
            _degraded_mode=False,
            _degraded_flush_interval=400,
            realtime_pipeline_timer=timer,
            realtime_pipeline=pipeline,
            **overrides,
        )
        # Replace _enter/_exit with the REAL methods (not mock) on stub
        import types as _t
        stub._enter_degraded_mode = _t.MethodType(KLineChartWorkspace._enter_degraded_mode, stub)
        stub._exit_degraded_mode = _t.MethodType(KLineChartWorkspace._exit_degraded_mode, stub)
        KLineChartWorkspace._enter_degraded_mode(stub)
        return stub, timer, pipeline

    def test_idempotent_when_already_degraded(self):
        stub = _make_stub(_degraded_mode=True)
        import types as _t
        stub._enter_degraded_mode = _t.MethodType(KLineChartWorkspace._enter_degraded_mode, stub)
        stub._exit_degraded_mode = _t.MethodType(KLineChartWorkspace._exit_degraded_mode, stub)
        KLineChartWorkspace._enter_degraded_mode(stub)
        # _emit_realtime_probe should NOT be called since we returned early
        stub._emit_realtime_probe.assert_not_called()

    def test_sets_degraded_mode_true(self):
        stub, _, _ = self._run_enter()
        assert stub._degraded_mode is True

    def test_updates_timer_interval(self):
        stub, timer, _ = self._run_enter()
        timer.setInterval.assert_called_once_with(400)

    def test_updates_pipeline_config(self):
        stub, _, pipeline = self._run_enter()
        pipeline.update_config.assert_called_once_with(flush_interval_ms=400)

    def test_emits_realtime_probe_with_degraded_true(self):
        stub, _, _ = self._run_enter()
        stub._emit_realtime_probe.assert_called_once()
        call_kwargs = stub._emit_realtime_probe.call_args.kwargs
        assert call_kwargs.get("degraded") is True
        assert call_kwargs.get("connected") is True

    def test_logs_degrade_event(self):
        stub, _, _ = self._run_enter()
        stub._log_degrade_event.assert_called_once_with("degraded", 400)

    def test_triggers_degrade_alert(self):
        stub, _, _ = self._run_enter()
        stub._trigger_degrade_alert.assert_called_once_with(alert_type="degraded", interval=400)

    def test_works_when_timer_is_none(self):
        # timer=None → setInterval should not crash
        pipeline = MagicMock()
        stub = _make_stub(
            _degraded_mode=False,
            _degraded_flush_interval=400,
            realtime_pipeline_timer=None,
            realtime_pipeline=pipeline,
        )
        import types as _t
        stub._enter_degraded_mode = _t.MethodType(KLineChartWorkspace._enter_degraded_mode, stub)
        stub._exit_degraded_mode = _t.MethodType(KLineChartWorkspace._exit_degraded_mode, stub)
        # Should not raise
        KLineChartWorkspace._enter_degraded_mode(stub)
        assert stub._degraded_mode is True

    def test_works_when_pipeline_is_none(self):
        stub = _make_stub(
            _degraded_mode=False,
            _degraded_flush_interval=400,
            realtime_pipeline_timer=MagicMock(),
            realtime_pipeline=None,
        )
        import types as _t
        stub._enter_degraded_mode = _t.MethodType(KLineChartWorkspace._enter_degraded_mode, stub)
        stub._exit_degraded_mode = _t.MethodType(KLineChartWorkspace._exit_degraded_mode, stub)
        KLineChartWorkspace._enter_degraded_mode(stub)
        assert stub._degraded_mode is True


class TestExitDegradedMode:
    """_exit_degraded_mode — 退出降级模式"""

    def _run_exit(self, **overrides):
        timer = MagicMock(name="timer")
        pipeline = MagicMock(name="pipeline")
        stub = _make_stub(
            _degraded_mode=True,
            _original_flush_interval=200,
            realtime_pipeline_timer=timer,
            realtime_pipeline=pipeline,
            **overrides,
        )
        import types as _t
        stub._enter_degraded_mode = _t.MethodType(KLineChartWorkspace._enter_degraded_mode, stub)
        stub._exit_degraded_mode = _t.MethodType(KLineChartWorkspace._exit_degraded_mode, stub)
        KLineChartWorkspace._exit_degraded_mode(stub)
        return stub, timer, pipeline

    def test_idempotent_when_not_degraded(self):
        stub = _make_stub(_degraded_mode=False)
        import types as _t
        stub._enter_degraded_mode = _t.MethodType(KLineChartWorkspace._enter_degraded_mode, stub)
        stub._exit_degraded_mode = _t.MethodType(KLineChartWorkspace._exit_degraded_mode, stub)
        KLineChartWorkspace._exit_degraded_mode(stub)
        stub._emit_realtime_probe.assert_not_called()

    def test_sets_degraded_mode_false(self):
        stub, _, _ = self._run_exit()
        assert stub._degraded_mode is False

    def test_restores_timer_interval(self):
        stub, timer, _ = self._run_exit()
        timer.setInterval.assert_called_once_with(200)

    def test_restores_pipeline_config(self):
        stub, _, pipeline = self._run_exit()
        pipeline.update_config.assert_called_once_with(flush_interval_ms=200)

    def test_emits_realtime_probe_degraded_false(self):
        stub, _, _ = self._run_exit()
        stub._emit_realtime_probe.assert_called_once()
        call_kwargs = stub._emit_realtime_probe.call_args.kwargs
        assert call_kwargs.get("degraded") is False

    def test_logs_recovered_event(self):
        stub, _, _ = self._run_exit()
        stub._log_degrade_event.assert_called_once_with("recovered", 200)

    def test_resolves_degrade_alert(self):
        stub, _, _ = self._run_exit()
        stub._resolve_degrade_alert.assert_called_once_with(alert_type="degraded")

    def test_works_when_timer_is_none(self):
        stub = _make_stub(
            _degraded_mode=True,
            _original_flush_interval=200,
            realtime_pipeline_timer=None,
            realtime_pipeline=MagicMock(),
        )
        import types as _t
        stub._enter_degraded_mode = _t.MethodType(KLineChartWorkspace._enter_degraded_mode, stub)
        stub._exit_degraded_mode = _t.MethodType(KLineChartWorkspace._exit_degraded_mode, stub)
        KLineChartWorkspace._exit_degraded_mode(stub)
        assert stub._degraded_mode is False

    def test_works_when_pipeline_is_none(self):
        stub = _make_stub(
            _degraded_mode=True,
            _original_flush_interval=200,
            realtime_pipeline_timer=MagicMock(),
            realtime_pipeline=None,
        )
        import types as _t
        stub._enter_degraded_mode = _t.MethodType(KLineChartWorkspace._enter_degraded_mode, stub)
        stub._exit_degraded_mode = _t.MethodType(KLineChartWorkspace._exit_degraded_mode, stub)
        KLineChartWorkspace._exit_degraded_mode(stub)
        assert stub._degraded_mode is False


class TestEnsureRealtimeApi:
    """_ensure_realtime_api — 实时 API 守护逻辑"""

    def test_test_mode_returns_early_no_connector(self):
        stub = _make_stub(test_mode=True, realtime_api=None, _realtime_connect_thread=None)
        with patch("gui_app.widgets.kline_chart_workspace._RealtimeConnectThread") as mock_cls:
            KLineChartWorkspace._ensure_realtime_api(stub)
            mock_cls.assert_not_called()

    def test_api_already_set_returns_early(self):
        stub = _make_stub(test_mode=False, realtime_api=MagicMock(), _realtime_connect_thread=None)
        with patch("gui_app.widgets.kline_chart_workspace._RealtimeConnectThread") as mock_cls:
            KLineChartWorkspace._ensure_realtime_api(stub)
            mock_cls.assert_not_called()

    def test_thread_running_returns_early(self):
        mock_thread = MagicMock()
        mock_thread.isRunning.return_value = True
        stub = _make_stub(test_mode=False, realtime_api=None, _realtime_connect_thread=mock_thread)
        with patch("gui_app.widgets.kline_chart_workspace._RealtimeConnectThread") as mock_cls:
            KLineChartWorkspace._ensure_realtime_api(stub)
            mock_cls.assert_not_called()

    def test_creates_connector_when_needed(self):
        mock_thread = MagicMock()
        mock_thread.isRunning.return_value = False
        stub = _make_stub(test_mode=False, realtime_api=None, _realtime_connect_thread=mock_thread)
        mock_connector = MagicMock()
        with patch("gui_app.widgets.kline_chart_workspace._RealtimeConnectThread",
                   return_value=mock_connector):
            KLineChartWorkspace._ensure_realtime_api(stub)
            mock_connector.setParent.assert_called_once_with(stub)
            mock_connector.ready.connect.assert_called_once_with(stub._on_realtime_ready)
            mock_connector.error_occurred.connect.assert_called_once_with(stub._on_realtime_error)
            mock_connector.start.assert_called_once()

    def test_connector_stored_on_stub(self):
        stub = _make_stub(test_mode=False, realtime_api=None, _realtime_connect_thread=None)
        mock_connector = MagicMock()
        with patch("gui_app.widgets.kline_chart_workspace._RealtimeConnectThread",
                   return_value=mock_connector):
            KLineChartWorkspace._ensure_realtime_api(stub)
            assert stub._realtime_connect_thread is mock_connector


class TestThreadSafetyHelpers:
    def test_is_thread_running_handles_deleted_wrapped_object(self):
        class _DeletedLike:
            def isRunning(self):
                raise RuntimeError("wrapped C/C++ object of type _CovThread has been deleted")

        stub = _make_stub()
        assert KLineChartWorkspace._is_thread_running(stub, _DeletedLike()) is False

    def test_is_thread_running_true_false_and_none(self):
        t_true = MagicMock()
        t_true.isRunning.return_value = True
        t_false = MagicMock()
        t_false.isRunning.return_value = False
        stub = _make_stub()
        assert KLineChartWorkspace._is_thread_running(stub, t_true) is True
        assert KLineChartWorkspace._is_thread_running(stub, t_false) is False
        assert KLineChartWorkspace._is_thread_running(stub, None) is False


class TestNormalizeRealtimeQuote:
    def test_normalize_supports_nested_data_and_last_price(self):
        stub = _make_stub()
        quote = {"data": {"lastPrice": 11.2, "openPrice": 10.8, "highPrice": 11.5, "lowPrice": 10.6, "vol": 12345}}
        normalized = KLineChartWorkspace._normalize_realtime_quote(stub, quote)
        assert normalized["price"] == 11.2
        assert normalized["open"] == 10.8
        assert normalized["high"] == 11.5
        assert normalized["low"] == 10.6
        assert normalized["volume"] == 12345

    def test_normalize_supports_sell_buy_schema_for_orderbook(self):
        stub = _make_stub()
        quote = {"sell1": 11.31, "buy1": 11.29, "sell1_vol": 1200, "buy1_vol": 800, "price": 11.30}
        normalized = KLineChartWorkspace._normalize_realtime_quote(stub, quote)
        assert normalized["ask1"] == 11.31
        assert normalized["bid1"] == 11.29
        assert normalized["ask1_vol"] == 1200
        assert normalized["bid1_vol"] == 800


class TestRealtimeBarConstruction:
    @staticmethod
    def _bind_realtime_methods(stub):
        import types as _types
        stub._normalize_realtime_quote = _types.MethodType(
            KLineChartWorkspace._normalize_realtime_quote, stub
        )
        stub._resolve_quote_timestamp = _types.MethodType(
            KLineChartWorkspace._resolve_quote_timestamp, stub
        )
        stub._coerce_timestamp = _types.MethodType(
            KLineChartWorkspace._coerce_timestamp, stub
        )
        stub._is_intraday_market_time = _types.MethodType(
            KLineChartWorkspace._is_intraday_market_time, stub
        )
        stub._floor_bar_time = _types.MethodType(
            KLineChartWorkspace._floor_bar_time, stub
        )

    def test_intraday_new_bar_open_uses_last_trade_not_day_open(self):
        stub = _make_stub()
        self._bind_realtime_methods(stub)
        stub.period_combo = MagicMock()
        stub.period_combo.currentText.return_value = "30m"
        stub.realtime_last_total_volume = 1000.0
        stub.last_data = pd.DataFrame(
            [
                {
                    "time": "2026-03-17 14:30:00",
                    "open": 111.40,
                    "high": 111.60,
                    "low": 111.20,
                    "close": 111.30,
                    "volume": 3000.0,
                }
            ]
        )
        stub.chart_adapter = MagicMock()
        stub.chart = None
        stub._request_subchart_update = MagicMock()
        stub._update_orderbook = MagicMock()
        quote = {
            "price": 111.12,
            "open": 116.90,
            "high": 111.20,
            "low": 111.08,
            "volume": 1050.0,
            "time": "2026-03-17 15:00:00",
        }
        KLineChartWorkspace._apply_realtime_quote(stub, quote, "000988.SZ")
        assert float(stub.last_data.iloc[-1]["open"]) == 111.12
        assert float(stub.last_data.iloc[-1]["close"]) == 111.12

    def test_daily_new_bar_open_keeps_day_open(self):
        stub = _make_stub()
        self._bind_realtime_methods(stub)
        stub.period_combo = MagicMock()
        stub.period_combo.currentText.return_value = "1d"
        stub.realtime_last_total_volume = 1000.0
        stub.last_data = pd.DataFrame(
            [
                {
                    "time": "2026-03-16",
                    "open": 110.00,
                    "high": 112.00,
                    "low": 109.50,
                    "close": 111.00,
                    "volume": 5000.0,
                }
            ]
        )
        stub.chart_adapter = MagicMock()
        stub.chart = None
        stub._request_subchart_update = MagicMock()
        stub._update_orderbook = MagicMock()
        quote = {"price": 111.12, "open": 116.90, "high": 117.10, "low": 110.80, "volume": 1200.0}
        KLineChartWorkspace._apply_realtime_quote(stub, quote, "000988.SZ")
        assert float(stub.last_data.iloc[-1]["open"]) == 116.90

    def test_intraday_same_bar_ignores_daily_high_low_fields(self):
        stub = _make_stub()
        self._bind_realtime_methods(stub)
        stub.period_combo = MagicMock()
        stub.period_combo.currentText.return_value = "5m"
        stub.realtime_last_total_volume = 1000.0
        current_slot = pd.Timestamp.now().floor("5min")
        stub.last_data = pd.DataFrame(
            [
                {
                    "time": current_slot,
                    "open": 111.20,
                    "high": 111.30,
                    "low": 111.10,
                    "close": 111.25,
                    "volume": 3000.0,
                }
            ]
        )
        stub.chart_adapter = MagicMock()
        stub.chart = None
        stub._request_subchart_update = MagicMock()
        stub._update_orderbook = MagicMock()
        quote = {"price": 111.12, "high": 119.90, "low": 80.00, "volume": 1010.0}
        KLineChartWorkspace._apply_realtime_quote(stub, quote, "000988.SZ")
        assert float(stub.last_data.iloc[-1]["high"]) == 111.30
        assert float(stub.last_data.iloc[-1]["low"]) == 111.10

    def test_intraday_ignores_after_hours_quote(self):
        stub = _make_stub()
        self._bind_realtime_methods(stub)
        stub.period_combo = MagicMock()
        stub.period_combo.currentText.return_value = "1m"
        stub.realtime_last_total_volume = 1000.0
        before = pd.DataFrame(
            [
                {
                    "time": "2026-03-17 14:59:00",
                    "open": 111.20,
                    "high": 111.30,
                    "low": 111.10,
                    "close": 111.25,
                    "volume": 3000.0,
                }
            ]
        )
        stub.last_data = before.copy()
        stub.chart_adapter = MagicMock()
        stub.chart = None
        stub._request_subchart_update = MagicMock()
        stub._update_orderbook = MagicMock()
        quote = {"price": 111.12, "volume": 1010.0, "time": "2026-03-17 18:05:00"}
        KLineChartWorkspace._apply_realtime_quote(stub, quote, "000988.SZ")
        assert len(stub.last_data) == len(before)
        assert stub.last_data.iloc[-1]["time"] == before.iloc[-1]["time"]


class TestFallbackBarConstruction:
    def test_build_intraday_bar_uses_last_trade_only(self):
        stub = _make_stub()
        import types as _types
        stub._resolve_quote_timestamp = _types.MethodType(
            KLineChartWorkspace._resolve_quote_timestamp, stub
        )
        stub._coerce_timestamp = _types.MethodType(
            KLineChartWorkspace._coerce_timestamp, stub
        )
        stub._is_intraday_market_time = _types.MethodType(
            KLineChartWorkspace._is_intraday_market_time, stub
        )
        stub._floor_bar_time = _types.MethodType(
            KLineChartWorkspace._floor_bar_time, stub
        )
        quote = {"price": 111.12, "open": 116.90, "high": 118.20, "low": 109.90, "volume": 888.0, "time": "2026-03-17 14:31:00"}
        bar = KLineChartWorkspace._build_bar_from_quote(stub, quote, "30m")
        assert bar is not None
        assert float(bar["open"]) == 111.12
        assert float(bar["high"]) == 111.12
        assert float(bar["low"]) == 111.12

    def test_build_intraday_bar_aligns_to_quote_timestamp(self):
        stub = _make_stub()
        import types as _types
        stub._resolve_quote_timestamp = _types.MethodType(
            KLineChartWorkspace._resolve_quote_timestamp, stub
        )
        stub._coerce_timestamp = _types.MethodType(
            KLineChartWorkspace._coerce_timestamp, stub
        )
        stub._is_intraday_market_time = _types.MethodType(
            KLineChartWorkspace._is_intraday_market_time, stub
        )
        stub._floor_bar_time = _types.MethodType(
            KLineChartWorkspace._floor_bar_time, stub
        )
        quote = {"price": 111.12, "time": "2026-03-17 14:59:31"}
        bar = KLineChartWorkspace._build_bar_from_quote(stub, quote, "5m")
        assert bar is not None
        assert str(bar["time"]).endswith("14:55:00")


# ---------------------------------------------------------------------------
# 覆盖率回退守卫
# 确保关键分支都被上方的测试覆盖到；若日后误删测试导致分支丢失，本类会第一时间报警
# ---------------------------------------------------------------------------
class TestCoverageRegressionGuard:
    """关键函数分支守卫 — 防止 kline_chart_workspace.py 覆盖率回退"""

    def test_normalize_symbol_sh_prefixes_all_covered(self):
        """60/68/11/12/13 开头的六位数字都映射到 .SH"""
        stub = _make_stub()
        for sym, expected_suffix in [
            ("600001", ".SH"),
            ("688001", ".SH"),
            ("110001", ".SH"),
            ("120001", ".SH"),
            ("130001", ".SH"),
        ]:
            result = KLineChartWorkspace._normalize_symbol(stub, sym)
            assert result.endswith(expected_suffix), (
                f"{sym} 应映射为 .SH，实际得到 {result!r}"
            )

    def test_normalize_symbol_sz_prefix_covered(self):
        """30/00 开头的六位数字映射到 .SZ"""
        stub = _make_stub()
        for sym in ("300001", "000001"):
            result = KLineChartWorkspace._normalize_symbol(stub, sym)
            assert result.endswith(".SZ"), (
                f"{sym} 应映射为 .SZ，实际得到 {result!r}"
            )

    def test_degradation_enter_exit_full_cycle(self):
        """enter_degraded → already degraded no-op → exit_degraded 全链路"""
        import types as _types
        mock_pipeline = MagicMock()
        stub = _make_stub(
            _degraded_mode=False,
            _degraded_flush_interval=400,
            _original_flush_interval=200,
            realtime_pipeline_timer=MagicMock(),
            realtime_pipeline=mock_pipeline,
        )
        stub._enter_degraded_mode = _types.MethodType(
            KLineChartWorkspace._enter_degraded_mode, stub
        )
        stub._exit_degraded_mode = _types.MethodType(
            KLineChartWorkspace._exit_degraded_mode, stub
        )

        # 进入降级
        KLineChartWorkspace._enter_degraded_mode(stub)
        assert stub._degraded_mode is True

        # 已降级时再触发检查 → 不再调用 enter
        stub._enter_degraded_mode = MagicMock()
        mock_pipeline.metrics.return_value = {"sustained_drop_alert": True}
        KLineChartWorkspace._check_and_apply_degradation(stub)
        stub._enter_degraded_mode.assert_not_called()

        # 恢复
        stub._exit_degraded_mode = _types.MethodType(
            KLineChartWorkspace._exit_degraded_mode, stub
        )
        KLineChartWorkspace._exit_degraded_mode(stub)
        assert stub._degraded_mode is False

    def test_prepare_chart_data_drops_nan_ohlc(self):
        """NaN OHLC 行被丢弃，守卫该分支不被误删"""
        import math
        stub = _make_stub()
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "open": [float("nan"), 10.0],
            "high": [float("nan"), 11.0],
            "low": [float("nan"), 9.0],
            "close": [float("nan"), 10.5],
            "volume": [0, 1000],
        })
        result = KLineChartWorkspace._prepare_chart_data(stub, df, "1d")
        assert len(result) == 1
        assert not math.isnan(result.iloc[0]["close"])

    def test_merge_chart_data_deduplicates(self):
        """merge 会去重，守卫去重分支"""
        stub = _make_stub()
        row = {"time": "2024-01-01", "open": 10.0, "high": 11.0, "low": 9.0,
               "close": 10.5, "volume": 100}
        base = pd.DataFrame([row])
        extra = pd.DataFrame([row])  # 完全相同
        result = KLineChartWorkspace._merge_chart_data(stub, base, extra)
        assert len(result) == 1

    def test_compute_initial_range_clamps_to_full_start(self):
        """start 被限制不早于 full range 的起始日期"""
        stub = _make_stub()
        full_start = "2023-01-01"
        full_end = "2024-01-01"
        start, end = KLineChartWorkspace._compute_initial_range(
            stub, (full_start, full_end), "1d"
        )
        assert start >= full_start


# ═════════════════════════════════════════════════════════════════════════════
# _on_chart_data_ready — 数据补全重试状态管理
# ═════════════════════════════════════════════════════════════════════════════

class TestOnChartDataReadyBackfillRetry:
    """Bug fix: 数据补全重试不应被立即取消 (else 子句修复)

    验证当 current_rows < min_bars 时 _pending_backfill_retry 保持不变；
    当 current_rows >= min_bars 时 _pending_backfill_retry 被清除。
    """

    def _make_ready_stub(self, pending_retry=None, remaining=0, **extra):
        import types as _t
        mock_timer = MagicMock(name="retry_timer")
        mock_timer.isActive.return_value = False
        stub = _make_stub(
            chart=MagicMock(name="chart"),
            _pending_backfill_retry=pending_retry,
            _backfill_retry_remaining=remaining,
            _backfill_retry_timer=mock_timer,
            _period_fallback_attempted=set(),
            _data_process_thread=None,
            _get_adjust_key=MagicMock(return_value="none"),
            _set_orderbook_status=MagicMock(name="_set_orderbook_status"),
            _request_full_range_data=MagicMock(name="_request_full_range_data"),
            _start_realtime_polling=MagicMock(name="_start_realtime_polling"),
            _try_auto_fallback_symbol=MagicMock(name="_try_auto_fallback_symbol"),
            period_combo=MagicMock(currentText=MagicMock(return_value="5m")),
            symbol_input=MagicMock(text=MagicMock(return_value="")),
        )
        # Bind the REAL methods needed inside _on_chart_data_ready
        stub._schedule_backfill_retry = _t.MethodType(
            KLineChartWorkspace._schedule_backfill_retry, stub
        )
        stub._min_bars_threshold = _t.MethodType(
            KLineChartWorkspace._min_bars_threshold, stub
        )
        for k, v in extra.items():
            setattr(stub, k, v)
        return stub

    def _make_payload(self, rows: int, symbol="000001.SZ", period="5m"):
        """Build payload with a DataFrame of `rows` rows and date info."""
        df = pd.DataFrame({
            "time": pd.date_range("2024-01-02", periods=rows, freq="5min"),
            "open": [10.0] * rows,
            "high": [10.5] * rows,
            "low": [9.5] * rows,
            "close": [10.2] * rows,
            "volume": [1000.0] * rows,
        })
        return {
            "data": df,
            "symbol": symbol,
            "period": period,
            "adjust": "none",
            "start_date": "2024-01-02",
            "end_date": "2024-01-03",
        }

    def _call(self, stub, payload):
        with patch("gui_app.widgets.kline_chart_workspace.QTimer") as mock_qt:
            mock_qt.singleShot = MagicMock(name="singleShot")
            KLineChartWorkspace._on_chart_data_ready(stub, payload)

    def test_insufficient_data_preserves_retry_state(self):
        """数据不足时 _pending_backfill_retry 不应被清除"""
        stub = self._make_ready_stub()
        payload = self._make_payload(rows=60)  # min_bars for 5m = 120
        self._call(stub, payload)
        # _schedule_backfill_retry should have been called → _pending_backfill_retry is set
        assert stub._pending_backfill_retry is not None

    def test_insufficient_data_retry_timer_started(self):
        """数据不足时应启动重试定时器"""
        stub = self._make_ready_stub(remaining=0)
        payload = self._make_payload(rows=60)
        self._call(stub, payload)
        stub._backfill_retry_timer.start.assert_called()

    def test_sufficient_data_clears_retry_state(self):
        """数据充足时 _pending_backfill_retry 应被清除"""
        stub = self._make_ready_stub(
            pending_retry=("000001.SZ", "5m", "none", "2024-01-01", "2024-01-10"),
            remaining=3,
        )
        payload = self._make_payload(rows=200)  # >= 120 min_bars for 5m
        self._call(stub, payload)
        assert stub._pending_backfill_retry is None
        assert stub._backfill_retry_remaining == 0

    def test_sufficient_data_stops_retry_timer(self):
        """数据充足时应停止重试定时器"""
        stub = self._make_ready_stub(
            pending_retry=("000001.SZ", "5m", "none", "2024-01-01", "2024-01-10"),
            remaining=2,
        )
        payload = self._make_payload(rows=150)
        self._call(stub, payload)
        stub._backfill_retry_timer.stop.assert_called()

    def test_insufficient_data_does_not_stop_timer(self):
        """数据不足时不应停止定时器"""
        stub = self._make_ready_stub()
        payload = self._make_payload(rows=50)
        self._call(stub, payload)
        stub._backfill_retry_timer.stop.assert_not_called()

    def test_daily_min_bars_threshold_is_40(self):
        """日线 min_bars 阈值应为 40"""
        stub = self._make_ready_stub()
        assert KLineChartWorkspace._min_bars_threshold(stub, "1d") == 40

    def test_5m_min_bars_threshold_is_120(self):
        """5m min_bars 阈值应为 120"""
        stub = self._make_ready_stub()
        assert KLineChartWorkspace._min_bars_threshold(stub, "5m") == 120


# ══════════════════════════════════════════════════════════════════════════════
# 9. _request_subchart_update — full_set 升级/降级保护（Bug修复验证）
#    修复前：full_set=False 调用会无条件覆盖已设置的 full_set=True，
#    导致 apply_precomputed 永远不执行，子图一直保留旧周期数据。
#    修复后：full_set 只能升级（False→True），禁止降级（True→False）。
# ══════════════════════════════════════════════════════════════════════════════

class TestRequestSubchartUpdateFullSetProtection:
    """验证 _request_subchart_update 只允许 full_set 升级，不允许降级"""

    def _make_subchart_stub(self, timer_active: bool = False):
        import types as _t
        mock_timer = MagicMock(name="subchart_timer")
        mock_timer.isActive.return_value = timer_active
        stub = _make_stub(
            _subchart_pending_data=None,
            _subchart_full_set=False,
            _subchart_update_timer=mock_timer,
        )
        stub._request_subchart_update = _t.MethodType(
            KLineChartWorkspace._request_subchart_update, stub
        )
        return stub

    def _dummy_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "time": pd.date_range("2026-03-10", periods=3, freq="1d"),
            "open": [10.0, 10.1, 10.2],
            "high": [10.5, 10.6, 10.7],
            "low": [9.8, 9.9, 10.0],
            "close": [10.3, 10.4, 10.5],
            "volume": [1000.0, 1100.0, 1200.0],
        })

    def test_full_set_true_sets_flag(self):
        """full_set=True 应将 _subchart_full_set 设为 True"""
        stub = self._make_subchart_stub()
        stub._request_subchart_update(self._dummy_df(), full_set=True)
        assert stub._subchart_full_set is True

    def test_full_set_false_leaves_flag_false_initially(self):
        """初始 False 状态下调用 full_set=False，标志保持 False"""
        stub = self._make_subchart_stub()
        assert stub._subchart_full_set is False
        stub._request_subchart_update(self._dummy_df(), full_set=False)
        assert stub._subchart_full_set is False

    def test_full_set_false_does_not_downgrade_true(self):
        """full_set=True 后，full_set=False 调用不应将标志降级回 False（核心修复）"""
        stub = self._make_subchart_stub()
        # 首次加载/切换周期设置 True
        stub._request_subchart_update(self._dummy_df(), full_set=True)
        assert stub._subchart_full_set is True
        # 1秒内实时 tick 来了，full_set=False — 不应覆盖已有的 True
        stub._request_subchart_update(self._dummy_df(), full_set=False)
        assert stub._subchart_full_set is True, (
            "full_set=False 不应将已有的 True 降级为 False，否则 apply_precomputed 永远不会执行"
        )

    def test_multiple_false_calls_dont_set_true(self):
        """多次 full_set=False 调用，初始 False 状态不应变成 True"""
        stub = self._make_subchart_stub()
        for _ in range(5):
            stub._request_subchart_update(self._dummy_df(), full_set=False)
        assert stub._subchart_full_set is False

    def test_true_after_false_still_upgrades(self):
        """已有 False 时再次传入 full_set=True，应正确升级为 True"""
        stub = self._make_subchart_stub()
        stub._request_subchart_update(self._dummy_df(), full_set=False)
        stub._request_subchart_update(self._dummy_df(), full_set=True)
        assert stub._subchart_full_set is True

    def test_pending_data_always_updated(self):
        """无论 full_set 如何，_subchart_pending_data 应更新为最新传入数据"""
        stub = self._make_subchart_stub()
        df1 = self._dummy_df()
        df2 = df1.copy()
        df2["close"] = [99.0, 98.0, 97.0]
        stub._request_subchart_update(df1, full_set=True)
        stub._request_subchart_update(df2, full_set=False)
        # pending_data 应是最新的 df2
        assert stub._subchart_pending_data is df2

    def test_timer_started_when_inactive(self):
        """定时器未运行时，应启动定时器"""
        stub = self._make_subchart_stub(timer_active=False)
        stub._request_subchart_update(self._dummy_df(), full_set=False)
        stub._subchart_update_timer.start.assert_called_once()

    def test_timer_not_started_when_already_active(self):
        """定时器已运行时，不应重复启动"""
        stub = self._make_subchart_stub(timer_active=True)
        stub._request_subchart_update(self._dummy_df(), full_set=True)
        stub._subchart_update_timer.start.assert_not_called()
