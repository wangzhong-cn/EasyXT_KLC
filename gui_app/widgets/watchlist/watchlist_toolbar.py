from __future__ import annotations

from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QComboBox, QHBoxLayout, QLineEdit, QPushButton, QWidget


class WatchlistToolbar(QWidget):
    group_changed = pyqtSignal(str)
    search_changed = pyqtSignal(str)
    type_changed = pyqtSignal(str)
    add_group_clicked = pyqtSignal()
    remove_group_clicked = pyqtSignal()
    color_mode_changed = pyqtSignal(str)
    fullscreen_clicked = pyqtSignal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self.group_combo = QComboBox()
        self.group_combo.setMinimumWidth(100)
        self.group_combo.currentTextChanged.connect(self.group_changed.emit)
        layout.addWidget(self.group_combo)
        self.add_group_btn = QPushButton("+")
        self.add_group_btn.setFixedWidth(24)
        self.add_group_btn.clicked.connect(self.add_group_clicked.emit)
        layout.addWidget(self.add_group_btn)
        self.remove_group_btn = QPushButton("-")
        self.remove_group_btn.setFixedWidth(24)
        self.remove_group_btn.clicked.connect(self.remove_group_clicked.emit)
        layout.addWidget(self.remove_group_btn)

        self.type_combo = QComboBox()
        self.type_combo.addItems(["全部", "A股", "港股", "美股"])
        self.type_combo.setMinimumWidth(80)
        self.type_combo.currentTextChanged.connect(self.type_changed.emit)
        layout.addWidget(self.type_combo)

        self.color_combo = QComboBox()
        self.color_combo.addItems(["红涨绿跌", "红跌绿涨"])
        self.color_combo.setMinimumWidth(88)
        self.color_combo.currentTextChanged.connect(self.color_mode_changed.emit)
        layout.addWidget(self.color_combo)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("搜索代码/名称")
        self.search_edit.textChanged.connect(self.search_changed.emit)
        layout.addWidget(self.search_edit, 1)

        self.fullscreen_btn = QPushButton("⛶")
        self.fullscreen_btn.setToolTip("全屏报价")
        self.fullscreen_btn.setFixedWidth(30)
        self.fullscreen_btn.clicked.connect(self.fullscreen_clicked.emit)
        layout.addWidget(self.fullscreen_btn)

    def set_groups(self, names: list[str]) -> None:
        current = self.group_combo.currentText()
        self.group_combo.blockSignals(True)
        self.group_combo.clear()
        self.group_combo.addItems(names)
        if current:
            idx = self.group_combo.findText(current)
            if idx >= 0:
                self.group_combo.setCurrentIndex(idx)
        self.group_combo.blockSignals(False)
