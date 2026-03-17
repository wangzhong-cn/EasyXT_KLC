import importlib

from PyQt5.QtCore import Qt, QTimer, pyqtSignal
from PyQt5.QtWidgets import QLabel, QTabWidget, QVBoxLayout, QWidget


class TradingModule(QWidget):
    symbol_selected = pyqtSignal(str)
    order_submitted = pyqtSignal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._current_symbol = ""
        self._loaded_tabs: dict[int, QWidget] = {}
        self._loading_tabs: dict[int, bool] = {}
        self._factories = [
            ("交易下单", "gui_app.widgets.operation_panel.tabs.trade_tab", "TradeTab"),
            ("持仓监控", "gui_app.widgets.operation_panel.tabs.position_tab", "PositionTab"),
            ("账户信息", "gui_app.widgets.operation_panel.tabs.account_tab", "AccountTab"),
            ("自选列表", "gui_app.widgets.operation_panel.tabs.watchlist_tab", "WatchlistTab"),
            ("网格交易", "gui_app.widgets.grid_trading_widget", "GridTradingWidget"),
            ("条件单", "gui_app.widgets.conditional_order_widget", "ConditionalOrderWidget"),
        ]
        self.tab_widget = QTabWidget()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.tab_widget)
        self._init_placeholders()
        self.tab_widget.currentChanged.connect(self._on_tab_changed)
        QTimer.singleShot(0, lambda: self._on_tab_changed(0))

    def _init_placeholders(self):
        for title, _, _ in self._factories:
            placeholder = QLabel(f"点击进入 {title}...")
            placeholder.setAlignment(Qt.AlignCenter)
            placeholder.setStyleSheet("color: #666; font-size: 14px;")
            self.tab_widget.addTab(placeholder, title)

    def _on_tab_changed(self, index: int):
        if index < 0:
            return
        if index in self._loaded_tabs or self._loading_tabs.get(index):
            return
        self._loading_tabs[index] = True
        QTimer.singleShot(0, lambda: self._load_tab(index))

    def _load_tab(self, index: int):
        try:
            if index < 0 or index >= len(self._factories):
                return
            title, module_path, class_name = self._factories[index]
            module = importlib.import_module(module_path)
            widget_cls = getattr(module, class_name)
            widget = widget_cls()
            self.tab_widget.removeTab(index)
            self.tab_widget.insertTab(index, widget, title)
            self.tab_widget.setCurrentIndex(index)
            self._loaded_tabs[index] = widget
            self._connect_widget(index, widget)
        finally:
            self._loading_tabs[index] = False

    def _connect_widget(self, index: int, widget: QWidget):
        if index == 0 and hasattr(widget, "order_submitted"):
            widget.order_submitted.connect(self.order_submitted.emit)
            if self._current_symbol and hasattr(widget, "set_symbol"):
                widget.set_symbol(self._current_symbol)
        if index == 3 and hasattr(widget, "symbol_selected"):
            widget.symbol_selected.connect(self._on_symbol_selected)
            if self._current_symbol and hasattr(widget, "set_selected"):
                widget.set_selected(self._current_symbol)

    def _on_symbol_selected(self, symbol: str):
        if not symbol:
            return
        self._current_symbol = symbol
        trade_tab = self._loaded_tabs.get(0)
        if trade_tab is not None and hasattr(trade_tab, "set_symbol"):
            trade_tab.set_symbol(symbol)
        self.symbol_selected.emit(symbol)

    def set_symbol(self, symbol: str):
        if not symbol:
            return
        self._current_symbol = symbol
        trade_tab = self._loaded_tabs.get(0)
        if trade_tab is not None and hasattr(trade_tab, "set_symbol"):
            trade_tab.set_symbol(symbol)
        watchlist_tab = self._loaded_tabs.get(3)
        if watchlist_tab is not None and hasattr(watchlist_tab, "set_selected"):
            watchlist_tab.set_selected(symbol)
