import importlib

from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtWidgets import QLabel, QTabWidget, QVBoxLayout, QWidget


class StrategyModule(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._loaded_tabs: dict[int, QWidget] = {}
        self._loading_tabs: dict[int, bool] = {}
        self._factories = [
            ("策略管理", "gui_app.widgets.strategy_governance_panel", "StrategyGovernancePanel"),
            ("回测分析", "gui_app.widgets.backtest_widget", "BacktestWidget"),
            ("JQ2QMT", "gui_app.widgets.jq2qmt_widget", "JQ2QMTWidget"),
            ("JQ转Ptrade", "gui_app.widgets.jq_to_ptrade_widget", "JQToPtradeWidget"),
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
        finally:
            self._loading_tabs[index] = False
