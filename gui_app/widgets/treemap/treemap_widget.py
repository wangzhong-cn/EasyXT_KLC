"""
TreemapWidget — 行情热图主控件

布局::

    ┌──────────────────────────────────────────────────────────┐
    │  [指数: 全A ▾]  [板块: 全行业 ▾]  [排序: 市值 ▾]  [刷新]  │  ← 工具栏
    ├──────────────────────────────────────────────────────────┤
    │                                                          │
    │          QWebEngineView  (ECharts Treemap)               │
    │                                                          │
    │  ▓▓▓▓▓▓▒▒▒░░░ [灰] ░░░▒▒▒▓▓▓▓▓▓  -7% ←────→ +7%        │  ← 图例（JS 内置）
    └──────────────────────────────────────────────────────────┘

信号::

    symbol_clicked(symbol: str)  — 用户点击热图某只股票，携带六位代码

用法::

    w = TreemapWidget()
    w.symbol_clicked.connect(workspace.load_symbol)
    w.refresh()
"""
from __future__ import annotations

import logging
from typing import Optional

from PyQt5.QtCore import QSize, Qt, QTimer, pyqtSignal
from PyQt5.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

log = logging.getLogger(__name__)


class TreemapWidget(QWidget):
    """行情热图主控件（板块树状图 + 工具栏）。"""

    symbol_clicked = pyqtSignal(str)   # 携带 e.g. "600519.SH"

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._adapter: Optional[object] = None    # TreemapAdapter (延迟创建)
        self._is_native: bool = False
        self._chart_widget: Optional[QWidget] = None
        self._initialized: bool = False
        self._all_sectors: list[dict] = []

        self._build_ui()
        # 第一次 showEvent 触发懒初始化
        self._pending_init: bool = True

    # ── UI 构建 ───────────────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # 工具栏
        toolbar = self._build_toolbar()
        root.addWidget(toolbar)

        # 图表占位区（初始先放 loading label）
        self._loading_label = QLabel("热图加载中...", self)
        self._loading_label.setAlignment(Qt.AlignCenter)
        self._loading_label.setStyleSheet(
            "color: #666; font-size: 14px; background: #0c0d0f;"
        )
        self._loading_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        root.addWidget(self._loading_label)

        self._chart_placeholder_layout = root   # 保存引用以便替换

    def _build_toolbar(self) -> QWidget:
        bar = QWidget(self)
        bar.setFixedHeight(36)
        bar.setStyleSheet(
            "QWidget { background: #1a1b1f; border-bottom: 1px solid #333; }"
            "QComboBox { background: #252629; border: 1px solid #444; border-radius: 3px;"
            "            color: #ccc; padding: 1px 6px; min-width: 80px; }"
            "QComboBox::drop-down { border: none; }"
            "QComboBox QAbstractItemView { background: #252629; color: #ccc; }"
            "QPushButton { background: #252629; border: 1px solid #444; border-radius: 3px;"
            "              color: #ccc; padding: 1px 10px; }"
            "QPushButton:hover { background: #333; }"
            "QLabel { color: #888; font-size: 12px; background: transparent; border: none; }"
        )
        h = QHBoxLayout(bar)
        h.setContentsMargins(8, 4, 8, 4)
        h.setSpacing(6)

        h.addWidget(QLabel("指数:"))
        self._index_combo = QComboBox()
        self._index_combo.addItems(["全A", "沪深300", "中证500", "创业板"])
        self._index_combo.setToolTip("切换指数成分股范围")
        h.addWidget(self._index_combo)

        h.addWidget(QLabel("板块:"))
        self._sector_combo = QComboBox()
        self._sector_combo.addItem("全行业")
        self._sector_combo.setToolTip("板块筛选")
        h.addWidget(self._sector_combo)

        h.addWidget(QLabel("排序:"))
        self._sort_combo = QComboBox()
        self._sort_combo.addItems(["市值", "涨幅", "跌幅"])
        self._sort_combo.setToolTip("改变格子大小的权重维度")
        h.addWidget(self._sort_combo)

        h.addStretch()

        self._refresh_btn = QPushButton("刷新")
        self._refresh_btn.setToolTip("重新拉取行情数据")
        self._refresh_btn.clicked.connect(self.refresh)
        h.addWidget(self._refresh_btn)

        # 信号连接
        self._sector_combo.currentTextChanged.connect(self._on_sector_changed)
        self._sort_combo.currentTextChanged.connect(self._on_sort_changed)
        self._index_combo.currentTextChanged.connect(self._on_index_changed)

        return bar

    # ── 懒初始化 ──────────────────────────────────────────────────────────────

    def showEvent(self, event) -> None:
        super().showEvent(event)
        if self._pending_init:
            self._pending_init = False
            QTimer.singleShot(0, self._lazy_init)

    def _lazy_init(self) -> None:
        from .treemap_adapter import TreemapAdapter

        adapter = TreemapAdapter()
        chart_widget, is_native = adapter.initialize(parent=self, timeout=5.0)
        self._adapter = adapter
        self._is_native = is_native
        self._chart_widget = chart_widget

        if chart_widget is not None:
            # 替换 loading label
            layout = self._chart_placeholder_layout
            layout.removeWidget(self._loading_label)
            self._loading_label.deleteLater()
            self._loading_label = None
            layout.addWidget(chart_widget)

        if is_native:
            adapter.on_symbol_click(self._on_symbol_clicked)
            self._initialized = True
            QTimer.singleShot(100, self.refresh)
        else:
            log.warning("TreemapWidget: 降级模式，热图不可用")

    # ── 数据加载 ──────────────────────────────────────────────────────────────

    def refresh(self) -> None:
        """
        拉取行情数据并推送到 JS 端。

        当前禁止内置演示数据；仅等待真实行情接入。
        """
        if not self._initialized:
            return
        self._refresh_btn.setEnabled(False)
        self._refresh_btn.setText("加载中...")
        QTimer.singleShot(50, self._do_load)

    def _do_load(self) -> None:
        sectors = self._load_sectors()
        self._all_sectors = sectors
        self._rebuild_sector_filter(sectors)

        if self._adapter is not None:
            self._apply_sort(sectors)

        if self._refresh_btn is not None:
            self._refresh_btn.setEnabled(True)
            self._refresh_btn.setText("刷新")

    def _load_sectors(self) -> list[dict]:
        """
        数据源入口。演示数据已移除，返回空列表等待真实行情接入。

        TODO: 接 UnifiedDataInterface.get_sector_heatmap()
        """
        return []

    def _rebuild_sector_filter(self, sectors: list[dict]) -> None:
        """根据数据动态填充板块下拉框。"""
        names = ["全行业"] + [s["name"] for s in sectors]
        current = self._sector_combo.currentText()
        self._sector_combo.blockSignals(True)
        self._sector_combo.clear()
        self._sector_combo.addItems(names)
        idx = self._sector_combo.findText(current)
        self._sector_combo.setCurrentIndex(max(0, idx))
        self._sector_combo.blockSignals(False)

    def _apply_sort(self, sectors: list[dict]) -> None:
        """
        根据当前排序选项对 sectors/children 排序后推送数据。
        """
        import copy
        sectors = copy.deepcopy(sectors)
        sort_key = self._sort_combo.currentText()

        if sort_key == "涨幅":
            key_fn = lambda s: s.get("pct_change", 0)
            rev = True
        elif sort_key == "跌幅":
            key_fn = lambda s: s.get("pct_change", 0)
            rev = False
        else:  # 市值
            key_fn = lambda s: s.get("value", 0)
            rev = True

        for sector in sectors:
            sector["children"] = sorted(
                sector.get("children", []), key=key_fn, reverse=rev
            )

        if self._adapter is not None:
            self._adapter.set_data(sectors)

    # ── 工具栏回调 ────────────────────────────────────────────────────────────

    def _on_sector_changed(self, text: str) -> None:
        if self._adapter is None or not self._initialized:
            return
        self._adapter.set_filter("" if text == "全行业" else text)

    def _on_sort_changed(self, _text: str) -> None:
        if self._all_sectors:
            self._apply_sort(self._all_sectors)

    def _on_index_changed(self, _text: str) -> None:
        # TODO: 接入不同指数成分股列表后重新 refresh
        if self._initialized:
            self.refresh()

    # ── 股票点击 ──────────────────────────────────────────────────────────────

    def _on_symbol_clicked(self, symbol: str, name: str, pct_change: float) -> None:
        log.info(
            "TreemapWidget: 热图点击 symbol=%s name=%s pct=%.2f%%",
            symbol, name, pct_change
        )
        if symbol:
            self.symbol_clicked.emit(symbol)

    # ── 尺寸提示 ──────────────────────────────────────────────────────────────

    def sizeHint(self) -> QSize:
        return QSize(800, 500)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        if self._adapter is not None and self._is_native:
            self._adapter.trigger_resize()
