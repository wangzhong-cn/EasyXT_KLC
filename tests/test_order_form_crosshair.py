from __future__ import annotations

from typing import Any, cast

from gui_app.widgets.operation_panel.widgets.order_form import OrderForm


class _DummyCombo:
    def __init__(self, value: str) -> None:
        self._value = value

    def currentText(self) -> str:
        return self._value


class _DummyPanel:
    def __init__(self, symbol: str) -> None:
        self.stock_combo = _DummyCombo(symbol)
        self.price_value: float | None = None
        self.hint: tuple[object, object] | None = None

    def set_price(self, value: float) -> None:
        self.price_value = value

    def set_crosshair_hint(self, time_value, price_value) -> None:
        self.hint = (time_value, price_value)


def test_order_form_click_updates_price() -> None:
    form = OrderForm.__new__(OrderForm)
    panel = _DummyPanel("000001.SZ")
    form._panel = cast(Any, panel)
    OrderForm._on_chart_price_clicked(form, 12.5)
    assert panel.price_value == 12.5


def test_order_form_crosshair_updates_hint_when_symbol_matches() -> None:
    form = OrderForm.__new__(OrderForm)
    panel = _DummyPanel("000001.SZ")
    form._panel = cast(Any, panel)
    OrderForm._on_chart_crosshair_moved(form, symbol="000001.SZ", time=123, price=9.99)
    assert panel.hint == (123, 9.99)


def test_order_form_crosshair_skips_mismatch_symbol() -> None:
    form = OrderForm.__new__(OrderForm)
    panel = _DummyPanel("000001.SZ")
    form._panel = cast(Any, panel)
    OrderForm._on_chart_crosshair_moved(form, symbol="000002.SZ", time=123, price=9.99)
    assert panel.hint is None
