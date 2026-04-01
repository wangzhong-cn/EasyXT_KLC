from PyQt5.QtCore import Qt, QTimer, pyqtSignal
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QScrollArea, QStackedWidget, QPushButton,
)


class CollapsibleSection(QWidget):
    expanded = pyqtSignal()
    collapsed = pyqtSignal()

    def __init__(self, title: str, parent=None):
        super().__init__(parent)
        self._expanded = False
        self._content_widget: QWidget | None = None

        self._header_btn = QPushButton(title)
        self._header_btn.setCursor(Qt.PointingHandCursor)
        self._header_btn.setFixedHeight(36)
        self._header_btn.setFlat(True)
        self._header_btn.clicked.connect(self.toggle)
        self._header_btn.setStyleSheet(self._btn_style())

        self._stack = QStackedWidget()
        self._stack.setVisible(False)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        layout.addWidget(self._header_btn)
        layout.addWidget(self._stack)

    def _btn_style(self) -> str:
        return (
            "QPushButton {"
            "  background: #2b2b2b;"
            "  color: #aaaaaa;"
            "  border: none;"
            "  border-bottom: 1px solid #3b3b3b;"
            "  border-left: 3px solid transparent;"
            "  padding: 0 12px;"
            "  text-align: left;"
            "  font-weight: bold;"
            "  font-size: 13px;"
            "}"
            "QPushButton:hover {"
            "  background: #333333;"
            "  color: #ffffff;"
            "}"
        )

    def _btn_active_style(self) -> str:
        return (
            "QPushButton {"
            "  background: #1e3a5f;"
            "  color: #4da6ff;"
            "  border: none;"
            "  border-bottom: 1px solid #3b3b3b;"
            "  border-left: 3px solid #4da6ff;"
            "  padding: 0 12px 0 9px;"
            "  text-align: left;"
            "  font-weight: bold;"
            "  font-size: 13px;"
            "}"
        )

    def set_content(self, widget: QWidget) -> None:
        if self._content_widget is not None:
            self._stack.removeWidget(self._content_widget)
        self._content_widget = widget
        self._stack.addWidget(widget)

    def expand(self) -> None:
        if self._expanded:
            return
        self._expanded = True
        self._stack.setVisible(True)
        self._header_btn.setStyleSheet(self._btn_active_style())
        self.expanded.emit()

    def collapse(self) -> None:
        if not self._expanded:
            return
        self._expanded = False
        self._stack.setVisible(False)
        self._header_btn.setStyleSheet(self._btn_style())
        self.collapsed.emit()

    def toggle(self) -> None:
        if self._expanded:
            self.collapse()
        else:
            self.expand()

    def is_expanded(self) -> bool:
        return self._expanded


class AccordionModuleWidget(QWidget):
    module_loaded = pyqtSignal(str, object)
    currentChanged = pyqtSignal(int)

    SECTION_TITLES = ["交易管理", "数据管理", "策略管理"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self._sections: list[CollapsibleSection] = []
        self._loaded_modules: dict[int, QWidget] = {}
        self._loading_modules: dict[int, bool] = {}
        self._active_index = 0
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setStyleSheet(
            "QScrollArea { border: none; background: transparent; }"
            "QScrollBar:vertical { width: 6px; background: #1e1e1e; }"
            "QScrollBar::handle { background: #444; border-radius: 3px; }"
        )
        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(1)

        for i, title in enumerate(self.SECTION_TITLES):
            sec = CollapsibleSection(title)
            sec.expanded.connect(lambda idx=i: self._on_section_expanded(idx))
            self._sections.append(sec)
            content_layout.addWidget(sec)

        content_layout.addStretch()
        scroll.setWidget(content)
        layout.addWidget(scroll)
        self._content = content

        QTimer.singleShot(0, lambda: self._expand_section(0))

    def _expand_section(self, index: int) -> None:
        if index < 0 or index >= len(self._sections):
            return
        for i, sec in enumerate(self._sections):
            if i == index:
                sec.expand()
                self._load_module_if_needed(i)
            else:
                sec.collapse()
        self._active_index = index
        self.currentChanged.emit(index)

    def _on_section_expanded(self, index: int) -> None:
        for i, sec in enumerate(self._sections):
            if i != index and sec.is_expanded():
                sec.collapse()
        self._active_index = index
        self._load_module_if_needed(index)

    def _load_module_if_needed(self, index: int) -> None:
        if index in self._loaded_modules or self._loading_modules.get(index):
            return
        self._loading_modules[index] = True
        QTimer.singleShot(0, lambda: self._do_load(index))

    def _do_load(self, index: int) -> None:
        try:
            if index < 0 or index >= len(self.SECTION_TITLES):
                return
            title = self.SECTION_TITLES[index]
            factory_map = {
                0: self._create_trading_module,
                1: self._create_data_module,
                2: self._create_strategy_module,
            }
            widget = factory_map[index]()
            self._sections[index].set_content(widget)
            self._loaded_modules[index] = widget
            self.module_loaded.emit(title, widget)
        finally:
            self._loading_modules[index] = False

    def _create_trading_module(self) -> QWidget:
        import importlib
        cls = getattr(
            importlib.import_module("gui_app.widgets.modules.trading_module"),
            "TradingModule",
        )
        return cls()

    def _create_data_module(self) -> QWidget:
        import importlib
        cls = getattr(
            importlib.import_module("gui_app.widgets.modules.data_module"),
            "DataModule",
        )
        return cls()

    def _create_strategy_module(self) -> QWidget:
        import importlib
        cls = getattr(
            importlib.import_module("gui_app.widgets.modules.strategy_module"),
            "StrategyModule",
        )
        return cls()

    def get_loaded_module(self, title: str) -> QWidget | None:
        for i, t in enumerate(self.SECTION_TITLES):
            if t == title:
                return self._loaded_modules.get(i)
        return None

    @property
    def tab_widget(self) -> "AccordionTabShim":
        if not hasattr(self, "_tab_shim"):
            self._tab_shim = AccordionTabShim(self)
        return self._tab_shim

    @property
    def accordion_sections(self) -> list[CollapsibleSection]:
        return self._sections


class AccordionTabShim(QWidget):
    currentChanged = pyqtSignal(int)

    def __init__(self, owner: AccordionModuleWidget):
        super().__init__(owner)
        self._owner = owner
        self._owner.currentChanged.connect(self.currentChanged.emit)

    def currentIndex(self) -> int:
        return self._owner._active_index

    def setCurrentIndex(self, index: int) -> None:
        self._owner._expand_section(index)
