from PyQt5.QtCore import Qt, QTimer, pyqtSignal
from PyQt5.QtWidgets import QLabel, QVBoxLayout, QWidget

from core.events import Events
from core.signal_bus import signal_bus
from gui_app.widgets.kline_chart_workspace import KLineChartWorkspace


class ChartPanel(QWidget):
    symbol_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._workspace = None
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        self._layout = layout
        self._placeholder = QLabel("图表加载中...")
        self._placeholder.setAlignment(Qt.AlignCenter)
        layout.addWidget(self._placeholder)
        signal_bus.subscribe(Events.CHART_DATA_LOADED, self._on_chart_loaded)
        QTimer.singleShot(500, self._ensure_workspace)

    def _ensure_workspace(self):
        if self._workspace is not None:
            return
        self._workspace = KLineChartWorkspace(include_operation_panel=False)
        if self._placeholder is not None:
            self._layout.removeWidget(self._placeholder)
            self._placeholder.deleteLater()
            self._placeholder = None
        self._layout.addWidget(self._workspace)

    def _on_chart_loaded(self, symbol: str, **kwargs):
        if symbol:
            self.symbol_changed.emit(symbol)

    def load_symbol(self, symbol: str):
        if self._workspace is None:
            self._ensure_workspace()
        workspace = self._workspace
        if workspace is not None:
            workspace.load_symbol(symbol)

    def add_marker(self, time, price, text: str):
        if self._workspace is None or self._workspace.chart is None:
            return
        self._workspace.chart.marker(text=text)

    def mark_order(self, order: dict):
        if not order:
            return
        if self._workspace is None:
            self._ensure_workspace()
        workspace = self._workspace
        if workspace is not None:
            workspace.mark_order(
                side=order.get("side", ""),
                symbol=order.get("symbol", ""),
                price=order.get("price", 0),
                volume=order.get("volume", 0),
            )
