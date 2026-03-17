"""
data_coverage_widget.py — 数据覆盖率矩阵组件
=============================================

以表格形式展示 DuckDB 中每只股票在各周期下的数据覆盖情况：

    行 = 股票代码
    列 = 周期（1d / 1m / 5m / tick）
    单元格 = "start~end(N条)"，有数据=绿底，无数据=浅红底

双击某行 → 向 signal_bus 广播 SYMBOL_SELECTED 事件；
右键菜单 → "补下载该标的所有周期"（触发后台 bulk_download）。

公共接口：
    DataCoverageWidget.refresh()   — 从数据库重新加载矩阵
    DataCoverageWidget.set_filter  — 按股票代码前缀/关键词过滤行
"""
from __future__ import annotations

import logging
import threading
from typing import Callable, Optional

import pandas as pd
from PyQt5.QtCore import QThread, Qt, pyqtSignal
from PyQt5.QtGui import QBrush, QColor
from PyQt5.QtWidgets import (
    QAction,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from core.events import Events
from core.signal_bus import signal_bus

log = logging.getLogger(__name__)

_PERIODS = ["1d", "1m", "5m", "tick"]
_COL_HEADERS = ["股票代码", "日K(1d)", "分钟(1m)", "5分钟(5m)", "Tick"]

_COLOR_HAS_DATA = QColor(200, 240, 200)   # 浅绿
_COLOR_NO_DATA = QColor(255, 220, 220)    # 浅红
_COLOR_HEADER = QColor(240, 240, 240)


# ─── 后台加载线程 ──────────────────────────────────────────────────────────────

class _CoverageLoadThread(QThread):
    """在后台线程中调用 get_data_coverage()，避免阻塞 UI。"""

    finished = pyqtSignal(object)   # pd.DataFrame or None
    error = pyqtSignal(str)

    def __init__(self, interface, stock_codes=None, parent=None):
        super().__init__(parent)
        self._iface = interface
        self._stock_codes = stock_codes

    def run(self) -> None:
        try:
            df = self._iface.get_data_coverage(
                stock_codes=self._stock_codes,
                periods=_PERIODS,
            )
            self.finished.emit(df)
        except Exception as exc:
            self.error.emit(str(exc))


class _BulkDownloadThread(QThread):
    """后台执行 bulk_download，定期回调进度。"""

    progress = pyqtSignal(int, int, str)    # current, total, status_text
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)

    def __init__(self, updater, stock_codes: list[str], parent=None):
        super().__init__(parent)
        self._updater = updater
        self._stock_codes = stock_codes
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        def _on_prog(current, total, code, period, status):
            self.progress.emit(current, total, f"[{current}/{total}] {code} {status}")

        try:
            result = self._updater.bulk_download(
                stock_codes=self._stock_codes,
                on_progress=_on_prog,
                stop_event=self._stop,
            )
            self.finished.emit(result)
        except Exception as exc:
            self.error.emit(str(exc))


# ─── 主组件 ───────────────────────────────────────────────────────────────────

class DataCoverageWidget(QWidget):
    """数据覆盖率矩阵可视化组件。

    Usage::

        widget = DataCoverageWidget()
        # 可选：传入已有 interface/updater 实例，避免重复初始化
        widget = DataCoverageWidget(interface=my_iface, updater=my_updater)
    """

    def __init__(
        self,
        interface=None,
        updater=None,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self._iface = interface
        self._updater = updater
        self._coverage_df: Optional[pd.DataFrame] = None
        self._load_thread: Optional[_CoverageLoadThread] = None
        self._download_thread: Optional[_BulkDownloadThread] = None
        self._init_ui()
        self._connect_events()

    # ─── UI 构建 ──────────────────────────────────────────────────────────────

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)

        # ── 顶部工具栏 ──
        toolbar = QHBoxLayout()
        self._filter_edit = QLineEdit()
        self._filter_edit.setPlaceholderText("过滤股票代码（支持关键词）")
        self._filter_edit.setMaximumWidth(220)
        self._filter_edit.textChanged.connect(self._apply_filter)

        self._refresh_btn = QPushButton("刷新覆盖矩阵")
        self._refresh_btn.clicked.connect(self.refresh)

        self._download_all_btn = QPushButton("全周期批量补数")
        self._download_all_btn.setToolTip("对所有显示的股票执行多周期数据下载入库")
        self._download_all_btn.clicked.connect(self._on_download_all)

        self._stop_btn = QPushButton("停止")
        self._stop_btn.setEnabled(False)
        self._stop_btn.clicked.connect(self._on_stop_download)

        self._status_label = QLabel("就绪")

        toolbar.addWidget(QLabel("搜索:"))
        toolbar.addWidget(self._filter_edit)
        toolbar.addWidget(self._refresh_btn)
        toolbar.addSpacing(12)
        toolbar.addWidget(self._download_all_btn)
        toolbar.addWidget(self._stop_btn)
        toolbar.addStretch()
        toolbar.addWidget(self._status_label)
        layout.addLayout(toolbar)

        # ── 进度条 ──
        self._progress_bar = QProgressBar()
        self._progress_bar.setVisible(False)
        self._progress_bar.setTextVisible(True)
        layout.addWidget(self._progress_bar)

        # ── 覆盖矩阵表格 ──
        self._table = QTableWidget(0, len(_COL_HEADERS))
        self._table.setHorizontalHeaderLabels(_COL_HEADERS)
        self._table.horizontalHeader().setStretchLastSection(False)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        self._table.setAlternatingRowColors(False)
        self._table.doubleClicked.connect(self._on_row_double_clicked)
        self._table.setContextMenuPolicy(Qt.CustomContextMenu)
        self._table.customContextMenuRequested.connect(self._on_context_menu)
        layout.addWidget(self._table, 1)

        # 摘要行（底部）
        self._summary_label = QLabel("")
        layout.addWidget(self._summary_label)

    def _connect_events(self) -> None:
        try:
            signal_bus.subscribe(Events.DATA_INGESTION_COMPLETE, self._on_ingestion_complete)
        except Exception:
            pass

    # ─── 公共接口 ─────────────────────────────────────────────────────────────

    def refresh(self) -> None:
        """从 DuckDB 重新加载数据覆盖矩阵（异步）。"""
        iface = self._get_interface()
        if iface is None:
            self._status_label.setText("UnifiedDataInterface 未初始化")
            return

        self._status_label.setText("加载中…")
        self._refresh_btn.setEnabled(False)

        if self._load_thread and self._load_thread.isRunning():
            self._load_thread.quit()

        self._load_thread = _CoverageLoadThread(iface, parent=self)
        self._load_thread.finished.connect(self._on_coverage_loaded)
        self._load_thread.error.connect(self._on_load_error)
        self._load_thread.start()

    def set_filter(self, keyword: str) -> None:
        """外部设置过滤关键词。"""
        self._filter_edit.setText(keyword)

    # ─── 内部逻辑 ─────────────────────────────────────────────────────────────

    def _get_interface(self):
        if self._iface is not None:
            return self._iface
        try:
            from data_manager.unified_data_interface import UnifiedDataInterface
            from data_manager.duckdb_connection_pool import resolve_duckdb_path
            self._iface = UnifiedDataInterface(
                duckdb_path=resolve_duckdb_path(None), silent_init=True
            )
            if not self._iface.connect(read_only=True):
                self._iface = None
        except Exception as exc:
            log.warning("DataCoverageWidget 初始化 interface 失败: %s", exc)
            self._iface = None
        return self._iface

    def _get_updater(self):
        if self._updater is not None:
            return self._updater
        try:
            from data_manager.auto_data_updater import AutoDataUpdater
            self._updater = AutoDataUpdater()
            self._updater.initialize_interface()
        except Exception as exc:
            log.warning("DataCoverageWidget 初始化 updater 失败: %s", exc)
            self._updater = None
        return self._updater

    def _on_coverage_loaded(self, df) -> None:
        self._refresh_btn.setEnabled(True)
        if df is None or (hasattr(df, 'empty') and df.empty):
            self._status_label.setText("无数据（DuckDB 为空或未连接）")
            self._table.setRowCount(0)
            return
        self._coverage_df = df
        self._populate_table(df)
        total_stocks = len(df)
        self._status_label.setText(f"共 {total_stocks} 只标的")
        self._summary_label.setText(
            f"标的数: {total_stocks}  |  周期: {', '.join(_PERIODS)}"
        )

    def _on_load_error(self, msg: str) -> None:
        self._refresh_btn.setEnabled(True)
        self._status_label.setText(f"加载失败: {msg}")
        log.error("DataCoverageWidget 加载失败: %s", msg)

    def _populate_table(self, df: pd.DataFrame) -> None:
        """将 DataFrame 渲染到表格中。"""
        self._table.setRowCount(0)
        stocks = list(df.index)
        self._table.setRowCount(len(stocks))

        for row_idx, stock_code in enumerate(stocks):
            # 股票代码列
            code_item = QTableWidgetItem(stock_code)
            code_item.setTextAlignment(Qt.AlignCenter)
            self._table.setItem(row_idx, 0, code_item)

            # 各周期列
            for col_idx, period in enumerate(_PERIODS):
                cell_col = col_idx + 1  # 第 0 列是代码
                val = ""
                if period in df.columns:
                    cell_val = df.at[stock_code, period]
                    val = str(cell_val) if cell_val else ""

                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignCenter)
                if val:
                    item.setBackground(QBrush(_COLOR_HAS_DATA))
                else:
                    item.setBackground(QBrush(_COLOR_NO_DATA))
                    item.setText("无数据")
                self._table.setItem(row_idx, cell_col, item)

        self._table.resizeColumnsToContents()
        self._table.horizontalHeader().setMinimumSectionSize(100)

    def _apply_filter(self, keyword: str) -> None:
        """根据关键词显示/隐藏行。"""
        kw = keyword.strip().upper()
        for row in range(self._table.rowCount()):
            item = self._table.item(row, 0)
            code = item.text().upper() if item else ""
            self._table.setRowHidden(row, bool(kw and kw not in code))

    def _on_row_double_clicked(self, index) -> None:
        row = index.row()
        code_item = self._table.item(row, 0)
        if code_item is None:
            return
        stock_code = code_item.text()
        try:
            signal_bus.emit(Events.SYMBOL_SELECTED, symbol=stock_code)
        except Exception as exc:
            log.warning("广播 SYMBOL_SELECTED 失败: %s", exc)

    def _on_context_menu(self, pos) -> None:
        item = self._table.itemAt(pos)
        if item is None:
            return
        row = item.row()
        code_item = self._table.item(row, 0)
        if code_item is None:
            return
        stock_code = code_item.text()

        menu = QMenu(self)
        act_download = QAction(f"补下载 {stock_code} 所有周期", self)
        act_download.triggered.connect(lambda: self._download_single(stock_code))
        act_select = QAction(f"在图表中打开 {stock_code}", self)
        act_select.triggered.connect(
            lambda: signal_bus.emit(Events.SYMBOL_SELECTED, symbol=stock_code)
        )
        menu.addAction(act_select)
        menu.addAction(act_download)
        menu.exec_(self._table.viewport().mapToGlobal(pos))

    def _download_single(self, stock_code: str) -> None:
        updater = self._get_updater()
        if updater is None:
            QMessageBox.warning(self, "无法下载", "AutoDataUpdater 未初始化")
            return
        self._start_download([stock_code])

    def _on_download_all(self) -> None:
        if self._coverage_df is None or self._coverage_df.empty:
            QMessageBox.information(self, "提示", "请先刷新覆盖矩阵以确定标的列表。")
            return
        updater = self._get_updater()
        if updater is None:
            QMessageBox.warning(self, "无法下载", "AutoDataUpdater 未初始化")
            return
        # 取当前可见行的股票代码
        visible_codes = []
        for row in range(self._table.rowCount()):
            if not self._table.isRowHidden(row):
                item = self._table.item(row, 0)
                if item:
                    visible_codes.append(item.text())
        if not visible_codes:
            QMessageBox.information(self, "提示", "没有满足条件的标的。")
            return
        reply = QMessageBox.question(
            self, "确认批量补数",
            f"将对 {len(visible_codes)} 只标的执行全周期（1d/1m/5m）数据下载，\n"
            "此操作可能耗时较长，是否继续？",
            QMessageBox.Yes | QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return
        self._start_download(visible_codes)

    def _start_download(self, codes: list[str]) -> None:
        updater = self._get_updater()
        if updater is None:
            return
        if self._download_thread and self._download_thread.isRunning():
            return  # 已有任务在跑

        self._download_all_btn.setEnabled(False)
        self._stop_btn.setEnabled(True)
        self._progress_bar.setVisible(True)
        self._progress_bar.setRange(0, len(codes))
        self._progress_bar.setValue(0)
        self._status_label.setText(f"下载中 0/{len(codes)}…")

        self._download_thread = _BulkDownloadThread(updater, codes, parent=self)
        self._download_thread.progress.connect(self._on_download_progress)
        self._download_thread.finished.connect(self._on_download_finished)
        self._download_thread.error.connect(self._on_download_error)
        self._download_thread.start()

    def _on_download_progress(self, current: int, total: int, text: str) -> None:
        self._progress_bar.setValue(current)
        self._status_label.setText(text)

    def _on_download_finished(self, result: dict) -> None:
        self._download_all_btn.setEnabled(True)
        self._stop_btn.setEnabled(False)
        self._progress_bar.setVisible(False)
        ok = result.get('success_stocks', 0)
        fail = result.get('failed_stocks', 0)
        rec = result.get('total_records', 0)
        self._status_label.setText(f"完成: 成功={ok} 失败={fail} 总记录={rec}")
        # 完成后自动刷新覆盖矩阵
        self.refresh()

    def _on_download_error(self, msg: str) -> None:
        self._download_all_btn.setEnabled(True)
        self._stop_btn.setEnabled(False)
        self._progress_bar.setVisible(False)
        self._status_label.setText(f"下载出错: {msg}")
        log.error("bulk_download 线程错误: %s", msg)

    def _on_stop_download(self) -> None:
        if self._download_thread and self._download_thread.isRunning():
            self._download_thread.stop()
            self._status_label.setText("正在停止…")
            self._stop_btn.setEnabled(False)

    def _on_ingestion_complete(self, **kwargs) -> None:
        """收到 DATA_INGESTION_COMPLETE 事件后自动刷新矩阵。"""
        self.refresh()
