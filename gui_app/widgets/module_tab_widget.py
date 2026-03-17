import importlib

from PyQt5.QtCore import Qt, QTimer, pyqtSignal
from PyQt5.QtWidgets import QLabel, QTabWidget, QVBoxLayout, QWidget


class ModuleTabWidget(QWidget):
    module_loaded = pyqtSignal(str, object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._tab_widget = QTabWidget()
        self._loaded_modules: dict[int, QWidget] = {}
        self._loading_modules: dict[int, bool] = {}
        self._factories = [
            ("交易管理", self._create_trading_module),
            ("数据管理", self._create_data_module),
            ("策略管理", self._create_strategy_module),
        ]
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._tab_widget)
        self._init_placeholders()
        self._tab_widget.currentChanged.connect(self._on_tab_changed)
        QTimer.singleShot(0, lambda: self._on_tab_changed(0))

    @property
    def tab_widget(self) -> QTabWidget:
        return self._tab_widget

    def _init_placeholders(self):
        for title, _ in self._factories:
            placeholder = QLabel(f"点击进入 {title}...")
            placeholder.setStyleSheet("color: #666; font-size: 14px;")
            placeholder.setAlignment(Qt.AlignCenter)
            self._tab_widget.addTab(placeholder, title)

    def _on_tab_changed(self, index: int):
        if index < 0:
            return
        if index in self._loaded_modules or self._loading_modules.get(index):
            return
        self._loading_modules[index] = True
        QTimer.singleShot(0, lambda: self._load_module(index))

    def _load_module(self, index: int):
        try:
            if index < 0 or index >= len(self._factories):
                return
            title, factory = self._factories[index]
            widget = factory()
            self._tab_widget.removeTab(index)
            self._tab_widget.insertTab(index, widget, title)
            self._tab_widget.setCurrentIndex(index)
            self._loaded_modules[index] = widget
            self.module_loaded.emit(title, widget)
        finally:
            self._loading_modules[index] = False

    def _create_trading_module(self) -> QWidget:
        cls = getattr(importlib.import_module("gui_app.widgets.modules.trading_module"), "TradingModule")
        return cls()

    def _create_data_module(self) -> QWidget:
        cls = getattr(importlib.import_module("gui_app.widgets.modules.data_module"), "DataModule")
        return cls()

    def _create_strategy_module(self) -> QWidget:
        cls = getattr(importlib.import_module("gui_app.widgets.modules.strategy_module"), "StrategyModule")
        return cls()

    def get_loaded_module(self, title: str) -> QWidget | None:
        for index, (tab_title, _) in enumerate(self._factories):
            if tab_title != title:
                continue
            return self._loaded_modules.get(index)
        return None
