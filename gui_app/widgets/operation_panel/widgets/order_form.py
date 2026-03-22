from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QVBoxLayout, QWidget

from core.events import Events
from core.signal_bus import signal_bus
from gui_app.enhanced.operation_panel.order_panel import OrderPanel


class OrderForm(QWidget):
    order_submitted = pyqtSignal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._panel = OrderPanel()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._panel)
        self._panel.order_requested.connect(self._emit_order)
        signal_bus.subscribe(Events.CHART_PRICE_CLICKED, self._on_chart_price_clicked)
        signal_bus.subscribe(Events.CHART_CROSSHAIR_MOVED, self._on_chart_crosshair_moved)

    def _emit_order(self, side: str, symbol: str, price: float, volume: int):
        self.order_submitted.emit(
            {"side": side, "symbol": symbol, "price": price, "volume": volume}
        )

    def set_symbol(self, symbol: str):
        self._panel.set_stock_code(symbol)

    def _on_chart_price_clicked(self, price: float, **kwargs):
        self._panel.set_price(price)

    def _on_chart_crosshair_moved(self, **kwargs):
        event_symbol = str(kwargs.get("symbol") or "").strip()
        panel_symbol = ""
        try:
            panel_symbol = str(self._panel.stock_combo.currentText() or "").strip()
        except Exception:
            panel_symbol = ""
        if event_symbol and panel_symbol and event_symbol != panel_symbol:
            return
        self._panel.set_crosshair_hint(kwargs.get("time"), kwargs.get("price"))
