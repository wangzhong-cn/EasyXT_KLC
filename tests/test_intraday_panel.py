from __future__ import annotations

from core.events import Events
from gui_app.widgets.intraday.intraday_panel import IntradayPanel, IntradayTableModel


def test_intraday_model_load_and_update() -> None:
    model = IntradayTableModel()
    model.load([{"time": "09:30", "price": 10.0, "change_pct": 0.5, "volume": 1000}])
    assert model.rowCount() == 1
    assert model.columnCount() == 4
    assert model.data(model.index(0, 0)) == "09:30"
    model.update_last_quote(10.2, 2.0, 1500, "09:31:00")
    assert model.data(model.index(0, 0)) == "09:31:00"


def test_intraday_panel_symbol_and_quote(qapp) -> None:
    panel = IntradayPanel()
    panel.set_symbol("600000.SH")
    # 演示数据已移除，等待真实行情数据推送
    assert panel._model.rowCount() == 0
    panel.update_quote({"symbol": "600000.SH", "price": 9.8, "change_pct": -1.2, "volume": 2000})
    assert panel._status.text().startswith("联动中:")


def test_intraday_panel_crosshair_bidirectional(qapp, monkeypatch) -> None:
    panel = IntradayPanel()
    panel.set_symbol("600000.SH")
    emitted: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        "gui_app.widgets.intraday.intraday_panel.signal_bus.emit",
        lambda event, **payload: emitted.append((event, payload)),
    )
    panel._on_curve_crosshair_moved("09:35", 10.15)
    assert emitted
    assert emitted[0][0] == Events.CHART_CROSSHAIR_MOVED
    assert emitted[0][1]["payload"]["source"] == "intraday"
    panel._on_crosshair_event(time=1700000000, price=10.2, symbol="600000.SH", payload={"source": "lwc"})
    assert "联动中:" in panel._status.text()
