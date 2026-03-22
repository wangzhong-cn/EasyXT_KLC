from __future__ import annotations

from typing import Any, cast

import pandas as pd

from gui_app.widgets.chart import chart_adapter


class _DummyChart:
    def __init__(self) -> None:
        self._interval = "1m"
        self._last_bar = object()
        self.set_calls = 0
        self.update_calls = 0
        self.marker_calls = 0

    def set(self, data: pd.DataFrame) -> None:
        self.set_calls += len(data)

    def update(self, _: pd.Series) -> None:
        self.update_calls += 1

    def marker(self, text: str) -> None:
        if text:
            self.marker_calls += 1


class _DummyBridge:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def notify(self, method: str, params: dict) -> None:
        self.calls.append((method, params))


def test_create_chart_adapter_explicit_lwc_python() -> None:
    dummy = _DummyChart()
    adapter = chart_adapter.create_chart_adapter(chart=dummy, backend="lwc_python")
    assert isinstance(adapter, chart_adapter.LwcPythonChartAdapter)


def test_create_chart_adapter_explicit_native() -> None:
    adapter = chart_adapter.create_chart_adapter(backend="native_lwc")
    assert isinstance(adapter, chart_adapter.NativeLwcChartAdapter)


def test_create_chart_adapter_explicit_klinechart() -> None:
    adapter = chart_adapter.create_chart_adapter(backend="klinechart")
    assert isinstance(adapter, chart_adapter.KLineChartAdapter)
    # KLineChartAdapter 是 NativeLwcChartAdapter 的子类（共享 WsBridge 等）
    assert isinstance(adapter, chart_adapter.NativeLwcChartAdapter)


def test_klinechart_build_html_references_correct_files() -> None:
    adapter = chart_adapter.KLineChartAdapter()
    html = adapter._build_html(9527)
    assert "klinecharts.min.js" in html
    assert "kline-bridge.js" in html
    assert "KlineBridge.init" in html
    assert "9527" in html
    # 确认不含 LWC 引用
    assert "lightweight-charts.js" not in html
    assert "ChartBridge.init" not in html


def test_rpc_protocol_has_create_indicator_constant() -> None:
    from gui_app.widgets.chart import rpc_protocol as rpc
    assert rpc.M_CREATE_INDICATOR == "chart.createIndicator"


def test_create_chart_adapter_native_downgrades_when_frozen(monkeypatch) -> None:
    dummy = _DummyChart()

    class _Cfg:
        def can_switch_now(self):
            return False, "freeze"

    monkeypatch.setattr(
        "gui_app.widgets.chart.backend_config.get_chart_backend_config",
        lambda: _Cfg(),
    )
    adapter = chart_adapter.create_chart_adapter(chart=dummy, backend="native_lwc")
    assert isinstance(adapter, chart_adapter.LwcPythonChartAdapter)


def test_lwc_update_data_recovers_when_last_bar_none() -> None:
    dummy = _DummyChart()
    dummy._last_bar = None

    def _raise_typeerror(_: pd.Series) -> None:
        raise TypeError("broken")

    dummy.update = _raise_typeerror
    adapter = chart_adapter.LwcPythonChartAdapter(dummy)
    adapter.update_data(pd.Series({"time": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}))
    assert dummy.set_calls == 1


def test_create_chart_adapter_reads_env_when_config_unavailable(monkeypatch) -> None:
    dummy = _DummyChart()
    monkeypatch.setenv("EASYXT_CHART_BACKEND", "lwc_python")
    monkeypatch.setattr(
        "gui_app.widgets.chart.backend_config.get_chart_backend_config",
        lambda: (_ for _ in ()).throw(RuntimeError("no config")),
    )
    adapter = chart_adapter.create_chart_adapter(chart=dummy)
    assert isinstance(adapter, chart_adapter.LwcPythonChartAdapter)


def test_create_chart_adapter_reads_klinechart_env_when_config_unavailable(monkeypatch) -> None:
    monkeypatch.setenv("EASYXT_CHART_BACKEND", "klinechart")
    monkeypatch.setattr(
        "gui_app.widgets.chart.backend_config.get_chart_backend_config",
        lambda: (_ for _ in ()).throw(RuntimeError("no config")),
    )
    adapter = chart_adapter.create_chart_adapter()
    assert isinstance(adapter, chart_adapter.KLineChartAdapter)


def test_native_adapter_indicator_rpc_notify() -> None:
    adapter = chart_adapter.NativeLwcChartAdapter()
    adapter._initialized = True
    bridge = _DummyBridge()
    adapter._bridge = cast(Any, bridge)
    adapter.create_indicator("MACD", is_stack=True, pane_id="pane_macd", height=90)
    adapter.remove_indicator(pane_id="pane_macd", name="MACD")
    assert len(bridge.calls) == 2
    assert bridge.calls[0][0] == "chart.createIndicator"
    assert bridge.calls[1][0] == "chart.removeIndicator"


def test_native_adapter_add_drawing_without_points_starts_interactive_draw() -> None:
    adapter = chart_adapter.NativeLwcChartAdapter()
    adapter._initialized = True
    bridge = _DummyBridge()
    adapter._bridge = cast(Any, bridge)
    drawing_id = adapter.add_drawing("hline")
    assert isinstance(drawing_id, str) and len(drawing_id) > 8
    assert len(bridge.calls) == 1
    assert bridge.calls[0][0] == "chart.startDraw"
    assert bridge.calls[0][1]["type"] == "hline"


def test_native_adapter_add_indicator_from_data_rpc_notify() -> None:
    adapter = chart_adapter.NativeLwcChartAdapter()
    adapter._initialized = True
    bridge = _DummyBridge()
    adapter._bridge = cast(Any, bridge)
    df = pd.DataFrame(
        [
            {"time": 1, "macd": 0.1},
            {"time": 2, "macd": 0.2},
        ]
    )
    adapter.add_indicator_from_data(
        "macd_fast",
        df,
        value_col="macd",
        pane="right",
        style={"color": "#ff0000"},
    )
    assert len(bridge.calls) == 1
    method, params = bridge.calls[0]
    assert method == "chart.addIndicator"
    assert params["id"] == "macd_fast"
    assert params["pane"] == "right"
    assert params["style"]["valueKey"] == "macd"
    assert len(params["data"]) == 2


def test_native_adapter_add_indicator_from_data_missing_column_noop() -> None:
    adapter = chart_adapter.NativeLwcChartAdapter()
    adapter._initialized = True
    bridge = _DummyBridge()
    adapter._bridge = cast(Any, bridge)
    df = pd.DataFrame([{"time": 1, "ema": 1.0}])
    adapter.add_indicator_from_data("ema_20", df, value_col="macd")
    assert bridge.calls == []




def test_adapter_apply_theme_dark_rpc_notify() -> None:
    adapter = chart_adapter.NativeLwcChartAdapter()
    adapter._initialized = True
    bridge = _DummyBridge()
    adapter._bridge = cast(Any, bridge)
    adapter.apply_theme("dark")
    assert len(bridge.calls) == 1
    method, params = bridge.calls[0]
    assert method == "chart.applyTheme"
    assert params["theme"]["backgroundColor"] == "#0f172a"
    assert params["theme"]["textColor"] == "#e2e8f0"


def test_adapter_apply_theme_light_rpc_notify() -> None:
    adapter = chart_adapter.KLineChartAdapter()
    adapter._initialized = True
    bridge = _DummyBridge()
    adapter._bridge = cast(Any, bridge)
    adapter.apply_theme("light")
    assert len(bridge.calls) == 1
    method, params = bridge.calls[0]
    assert method == "chart.applyTheme"
    assert params["theme"]["backgroundColor"] == "#f8fafc"
    assert params["theme"]["textColor"] == "#0f172a"


def test_adapter_apply_theme_no_bridge_is_noop() -> None:
    """apply_theme 尚未初始化时不崩溃。"""
    adapter = chart_adapter.NativeLwcChartAdapter()
    # _initialized 默认 False，不应抛异常
    adapter.apply_theme("dark")
