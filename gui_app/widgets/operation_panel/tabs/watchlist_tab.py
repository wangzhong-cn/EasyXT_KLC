from PyQt5.QtCore import Qt, QTimer, pyqtSignal
from PyQt5.QtWidgets import QListWidget, QVBoxLayout, QWidget


class WatchlistTab(QWidget):
    symbol_selected = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.list_widget = QListWidget()
        self.list_widget.setStyleSheet("""
            QListWidget {
                background-color: #1e1e1e;
                color: #ffffff;
                border: none;
            }
            QListWidget::item {
                padding: 8px;
                border-bottom: 1px solid #333;
            }
            QListWidget::item:selected {
                background-color: #2d2d2d;
                color: #4caf50;
            }
            QListWidget::item:hover {
                background-color: #252525;
            }
        """)
        self.list_widget.addItems(["000001.SZ", "600000.SH", "000002.SZ", "600036.SH"])
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.list_widget)
        self.list_widget.itemClicked.connect(self._emit_symbol)
        QTimer.singleShot(0, self._select_default)

    def _emit_symbol(self, item):
        symbol = item.text().strip() if item else ""
        if symbol:
            self.symbol_selected.emit(symbol)

    def _select_default(self):
        if self.list_widget.count() == 0:
            return
        item = self.list_widget.item(0)
        if item:
            self.list_widget.setCurrentItem(item)
            self._emit_symbol(item)

    def set_selected(self, symbol: str):
        if not symbol:
            return
        matches = self.list_widget.findItems(symbol, Qt.MatchExactly)
        if matches:
            self.list_widget.setCurrentItem(matches[0])
