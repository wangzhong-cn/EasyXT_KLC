from __future__ import annotations

from typing import Any, cast

import pandas as pd

from core.events import Events
from gui_app.widgets.chart.chart_adapter import KLineChartAdapter
from gui_app.widgets.kline_chart_workspace import KLineChartWorkspace, _ChartDataLoadThread


def test_create_chart_widget_routes_to_kline(monkeypatch) -> None:
    class _Cfg:
        def get_backend(self):
            return "klinechart"

    class _Dummy:
        test_mode = False
        called = ""

        def _create_klinechart_widget(self, _parent):
            self.called = "kline"
            return "k"

        def _create_lwc_widget(self, _parent):
            self.called = "lwc"
            return "l"

    monkeypatch.setattr(
        "gui_app.widgets.chart.backend_config.get_chart_backend_config",
        lambda: _Cfg(),
    )
    d = _Dummy()
    result = KLineChartWorkspace._create_chart_widget(cast(Any, d), cast(Any, object()))
    assert result == "k"
    assert d.called == "kline"


def test_apply_indicator_visibility_uses_kline_rpc() -> None:
    ws = KLineChartWorkspace.__new__(KLineChartWorkspace)
    ws._logger = cast(Any, type("_L", (), {"exception": lambda *args, **kwargs: None})())
    ws.chart_adapter = KLineChartAdapter()
    ws.macd_visible = True
    ws.rsi_visible = False
    ws.vol_visible = True
    ws.kdj_visible = False
    ws.ma_visible = True
    ws.boll_visible = True

    removed: list[tuple[str, str | None]] = []
    created: list[tuple[str, bool, str | None]] = []

    def _remove_indicator(*, pane_id: str, name: str | None = None):
        removed.append((pane_id, name))

    def _create_indicator(
        name: str,
        *,
        is_stack: bool = False,
        pane_id: str | None = None,
        **_kwargs,
    ):
        created.append((name, is_stack, pane_id))

    ws.chart_adapter.remove_indicator = _remove_indicator
    ws.chart_adapter.create_indicator = _create_indicator
    KLineChartWorkspace._apply_kline_indicator_visibility(ws)
    assert ("pane_macd", None) in removed
    assert ("pane_vol", None) in removed
    assert ("candle_pane", "MA") in removed
    assert ("MACD", True, "pane_macd") in created
    assert ("VOL", True, "pane_vol") in created
    assert ("MA", False, "candle_pane") in created
    assert ("BOLL", False, "candle_pane") in created


def test_bind_range_event_with_adapter() -> None:
    ws = KLineChartWorkspace.__new__(KLineChartWorkspace)
    ws._logger = cast(Any, type("_L", (), {"exception": lambda *args, **kwargs: None})())
    ws.chart = None
    ws._range_change_bound = False

    class _A:
        def __init__(self):
            self.range_cb = None
            self.click_cb = None
            self.crosshair_cb = None

        def on_range_changed(self, cb):
            self.range_cb = cb

        def on_chart_click(self, cb):
            self.click_cb = cb

        def on_crosshair_move(self, cb):
            self.crosshair_cb = cb

    adapter = cast(Any, _A())
    ws.chart_adapter = adapter
    KLineChartWorkspace._bind_range_change_event(ws)
    assert ws._range_change_bound is True
    assert adapter.range_cb is not None
    assert adapter.click_cb is not None
    assert adapter.crosshair_cb is not None


def test_adapter_chart_click_emits_signal(monkeypatch) -> None:
    ws = KLineChartWorkspace.__new__(KLineChartWorkspace)
    ws._logger = cast(Any, type("_L", (), {"exception": lambda *args, **kwargs: None})())
    emitted: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        "gui_app.widgets.kline_chart_workspace.signal_bus.emit",
        lambda ev, **kwargs: emitted.append((ev, kwargs)),
    )
    KLineChartWorkspace._on_adapter_chart_click(ws, {"price": 12.5, "time": 123})
    assert emitted[0][0] == Events.CHART_PRICE_CLICKED
    assert emitted[0][1]["price"] == 12.5
    assert emitted[0][1]["time"] == 123


def test_adapter_range_changed_maps_to_on_range_change() -> None:
    ws = KLineChartWorkspace.__new__(KLineChartWorkspace)
    ws._loaded_range = ("2024-01-01 09:30:00", "2024-01-01 10:30:00")
    ws.period_combo = cast(Any, type("_P", (), {"currentText": lambda self: "1m"})())
    ws_any = cast(Any, ws)

    def _get_time_step(period: str):
        return pd.Timedelta(minutes=1)

    ws_any._get_time_step = _get_time_step
    captured: list[tuple[float, float]] = []

    def _on_range_change(chart: Any, bars_before: float, bars_after: float):
        captured.append((bars_before, bars_after))

    def _to_datetime_safe(value: Any):
        return pd.to_datetime(int(value), unit="s", errors="coerce")

    ws_any._on_range_change = _on_range_change
    ws_any._to_datetime_safe = _to_datetime_safe
    KLineChartWorkspace._on_adapter_range_changed(
        ws,
        {"from": 1704102000, "to": 1704105000},
    )
    assert len(captured) == 1


def test_adapter_crosshair_emits_signal(monkeypatch) -> None:
    ws = KLineChartWorkspace.__new__(KLineChartWorkspace)
    ws._logger = cast(Any, type("_L", (), {"exception": lambda *args, **kwargs: None})())
    emitted: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        "gui_app.widgets.kline_chart_workspace.signal_bus.emit",
        lambda ev, **kwargs: emitted.append((ev, kwargs)),
    )
    KLineChartWorkspace._on_adapter_crosshair_move(ws, {"price": 15.2, "time": 456, "x": 1, "y": 2})
    assert emitted[0][0] == Events.CHART_CROSSHAIR_MOVED
    assert emitted[0][1]["price"] == 15.2


def test_crosshair_status_label_updates_and_filters() -> None:
    ws = KLineChartWorkspace.__new__(KLineChartWorkspace)
    ws._logger = cast(Any, type("_L", (), {"exception": lambda *args, **kwargs: None})())
    ws.symbol_input = cast(Any, type("_S", (), {"text": lambda self: "000001.SZ"})())
    ws.period_combo = cast(Any, type("_P", (), {"currentText": lambda self: "1m"})())
    label = cast(Any, type("_LB", (), {"text_value": "", "setText": lambda self, t: setattr(self, "text_value", t)})())
    ws._crosshair_info_label = label

    KLineChartWorkspace._on_crosshair_moved(
        ws,
        symbol="000001.SZ",
        period="1m",
        time="2024-01-01 10:00:00",
        price=12.34,
    )
    assert "12.34" in label.text_value
    KLineChartWorkspace._on_crosshair_moved(ws, symbol="000002.SZ", period="1m", time="x", price=99.0)
    assert "12.34" in label.text_value


# ===========================================================================
# _fmt_crosshair_time / _fmt_crosshair_price
# ===========================================================================

class TestCrosshairFormatHelpers:
    def test_fmt_time_unix_ts_returns_datetime_string(self):
        result = KLineChartWorkspace._fmt_crosshair_time(1710000000)
        # 应为 "YYYY-MM-DD HH:MM" 格式
        import re
        assert re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", result)

    def test_fmt_time_date_string_passthrough(self):
        result = KLineChartWorkspace._fmt_crosshair_time("2024-01-02")
        assert result == "2024-01-02"

    def test_fmt_time_none_returns_empty(self):
        assert KLineChartWorkspace._fmt_crosshair_time(None) == ""

    def test_fmt_time_small_int_passthrough(self):
        assert KLineChartWorkspace._fmt_crosshair_time(42) == "42"

    def test_fmt_price_float_two_decimals(self):
        assert KLineChartWorkspace._fmt_crosshair_price(12.3456) == "12.35"

    def test_fmt_price_int(self):
        assert KLineChartWorkspace._fmt_crosshair_price(10) == "10.00"

    def test_fmt_price_none_returns_empty(self):
        assert KLineChartWorkspace._fmt_crosshair_price(None) == ""

    def test_fmt_price_invalid_returns_string(self):
        assert KLineChartWorkspace._fmt_crosshair_price("n/a") == "n/a"


def test_watchlist_page_button_sync_state() -> None:
    ws = KLineChartWorkspace.__new__(KLineChartWorkspace)
    ws.include_operation_panel = True
    ws.root_splitter = cast(Any, type("_S", (), {"sizes": lambda self: [700, 300]})())
    ws.watchlist_page_btn = cast(
        Any,
        type(
            "_B",
            (),
            {
                "__init__": lambda self: setattr(self, "checked", False),
                "setChecked": lambda self, v: setattr(self, "checked", bool(v)),
                "setStyleSheet": lambda self, _s: None,
            },
        )(),
    )
    ws.bottom_tabs = cast(
        Any,
        type(
            "_T",
            (),
            {
                "currentIndex": lambda self: 2,
                "count": lambda self: 3,
                "tabText": lambda self, i: "报价列表" if i == 2 else f"X{i}",
            },
        )(),
    )
    KLineChartWorkspace._sync_watchlist_page_btn_state(ws, 2)
    assert ws.watchlist_page_btn.checked is True


def test_chart_data_loader_uses_local_preview_as_final_fallback(monkeypatch) -> None:
    preview_df = pd.DataFrame(
        {
            "open": [10.0, 10.2, 10.4, 10.6, 10.8],
            "high": [10.3, 10.5, 10.7, 10.9, 11.1],
            "low": [9.9, 10.1, 10.3, 10.5, 10.7],
            "close": [10.1, 10.3, 10.5, 10.7, 10.9],
            "volume": [100, 120, 130, 140, 150],
        },
        index=pd.date_range("2024-01-01", periods=5, freq="D", name="date"),
    )

    class _FakeIface:
        def __init__(self, *args, **kwargs):
            self.con = object()

        def connect(self, read_only=False):
            return None

        def get_stock_data_local(self, **kwargs):
            return preview_df.copy()

        def get_stock_data(self, **kwargs):
            return pd.DataFrame()

        def get_ingestion_status(self, **kwargs):
            return pd.DataFrame()

        def close(self):
            return None

    fake_module = cast(Any, type("_M", (), {"UnifiedDataInterface": _FakeIface}))
    monkeypatch.setattr(
        "gui_app.widgets.kline_chart_workspace.importlib.import_module",
        lambda _name: fake_module,
    )

    loader = _ChartDataLoadThread(
        duckdb_path="d:/EasyXT_KLC/data/market_data.duckdb",
        symbol="002460.SZ",
        start_date="2024-01-01",
        end_date="2024-01-05",
        period="1d",
        adjust="none",
        max_bars=0,
        mode="replace",
    )
    emitted: list[dict] = []
    loader.data_ready.connect(lambda payload: emitted.append(payload))

    loader.run()

    assert len(emitted) >= 2
    assert emitted[0]["ingestion_status"] == "local_preview"
    assert not emitted[0]["data"].empty
    assert emitted[-1]["ingestion_status"] == "local_preview_fallback"
    assert not emitted[-1]["data"].empty
