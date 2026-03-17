"""
因子分析面板
=============================
提供内置/自定义因子的浏览、计算、存储与回看功能。

布局
----
┌──────────────────────────────────────────────────────────┐
│  工具栏：股票代码 | 日期范围 | 因子选择 | 计算 / 保存    │
├────────────────┬─────────────────────────────────────────┤
│ 因子注册表     │ 因子值结果表格（计算结果 / 历史记录）    │
│ (左侧列表)     ├─────────────────────────────────────────┤
│                │ 已存储因子覆盖汇总                       │
└────────────────┴─────────────────────────────────────────┘
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from PyQt5.QtCore import (
    QAbstractTableModel,
    QDate,
    QModelIndex,
    QSortFilterProxyModel,
    Qt,
    QThread,
    pyqtSignal,
)
from PyQt5.QtGui import QColor, QFont
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QDateEdit,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QTableView,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _get_factor_registry():
    """懒导入，避免启动时依赖 DuckDB。"""
    try:
        import data_manager.builtin_factors  # noqa: F401 触发自动注册
        from data_manager.factor_registry import factor_registry
        return factor_registry
    except ImportError as e:
        log.warning("因子注册中心不可用: %s", e)
        return None


def _get_udi() -> Any:
    """从全局信号总线或项目路径获取 UnifiedDataInterface 实例（只读）。"""
    try:
        from data_manager.unified_data_interface import UnifiedDataInterface
        import os
        udi = UnifiedDataInterface(
            duckdb_path=os.environ.get("EASYXT_DUCKDB_PATH", ""),
            enable_backfill_scheduler=False,
        )
        udi.connect(read_only=True)
        return udi
    except Exception as e:
        log.warning("UnifiedDataInterface 连接失败: %s", e)
        return None


# ---------------------------------------------------------------------------
# Table models
# ---------------------------------------------------------------------------


class _PandasTableModel(QAbstractTableModel):
    """通用 pandas DataFrame → Qt table model。"""

    def __init__(self, df: pd.DataFrame, parent: QWidget | None = None):
        super().__init__(parent)
        self._df = df.reset_index() if not isinstance(df.index, pd.RangeIndex) else df.copy()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._df)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._df.columns)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid():
            return None
        value = self._df.iloc[index.row(), index.column()]
        if role == Qt.DisplayRole:
            if pd.isna(value):
                return "—"
            if isinstance(value, float):
                return f"{value:.6g}"
            return str(value)
        if role == Qt.TextAlignmentRole:
            col_name = self._df.columns[index.column()]
            if pd.api.types.is_numeric_dtype(self._df[col_name]):
                return Qt.AlignRight | Qt.AlignVCenter
            return Qt.AlignLeft | Qt.AlignVCenter
        if role == Qt.ForegroundRole:
            value_raw = self._df.iloc[index.row(), index.column()]
            if isinstance(value_raw, float) and not pd.isna(value_raw):
                if value_raw > 0:
                    return QColor("#16a34a")
                if value_raw < 0:
                    return QColor("#dc2626")
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                col = str(self._df.columns[section])
                return col.replace("_", " ").title() if col.islower() else col
            return str(section + 1)
        return None

    def update_data(self, df: pd.DataFrame) -> None:
        self.beginResetModel()
        self._df = df.reset_index() if not isinstance(df.index, pd.RangeIndex) else df.copy()
        self.endResetModel()


# ---------------------------------------------------------------------------
# Worker threads
# ---------------------------------------------------------------------------


class _ComputeWorker(QThread):
    """后台线程：计算因子并可选保存。"""

    finished = pyqtSignal(object, str)   # (result_series_or_df, error_msg)
    progress = pyqtSignal(str)

    def __init__(
        self,
        factor_names: list[str],
        stock_code: str,
        start_date: str,
        end_date: str,
        save_to_db: bool = False,
    ):
        super().__init__()
        self.factor_names = factor_names
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        self.save_to_db = save_to_db
        self._udi: Any = None

    def run(self) -> None:
        try:
            self.progress.emit("正在连接数据源…")
            self._udi = _get_udi()
            if self._udi is None:
                self.finished.emit(pd.DataFrame(), "UnifiedDataInterface 连接失败")
                return

            self.progress.emit(f"正在获取 {self.stock_code} K 线数据…")
            df_ohlcv = self._udi.get_stock_data(
                self.stock_code, self.start_date, self.end_date, "1d"
            )
            if df_ohlcv is None or df_ohlcv.empty:
                self.finished.emit(pd.DataFrame(), f"无法获取 {self.stock_code} 数据")
                return

            # 确保 DatetimeIndex
            for col in ("datetime", "date"):
                if col in df_ohlcv.columns:
                    df_ohlcv = df_ohlcv.set_index(col)
                    break
            if not isinstance(df_ohlcv.index, pd.DatetimeIndex):
                df_ohlcv.index = pd.to_datetime(df_ohlcv.index, errors="coerce")

            import data_manager.builtin_factors  # noqa: F401
            from data_manager.factor_registry import factor_registry, FactorComputeEngine

            engine = FactorComputeEngine(factor_registry)
            self.progress.emit(f"正在计算 {len(self.factor_names)} 个因子…")
            result_df = engine.compute_many(self.factor_names, df_ohlcv, errors="skip")
            # 裁剪到请求的时间范围
            result_df = result_df.loc[result_df.index >= pd.to_datetime(self.start_date)]

            if self.save_to_db and not result_df.empty:
                self.progress.emit("正在写入 DuckDB…")
                saved_rows = 0
                for name in result_df.columns:
                    series = result_df[name].dropna()
                    if not series.empty:
                        n = self._udi.save_factor(self.stock_code, name, series)
                        saved_rows += n
                self.progress.emit(f"已写入 {saved_rows} 行因子数据")

            self.finished.emit(result_df, "")

        except Exception as e:
            log.exception("因子计算失败")
            self.finished.emit(pd.DataFrame(), str(e))
        finally:
            if self._udi is not None:
                try:
                    self._udi.close()
                except Exception:
                    pass


class _LoadCoverageWorker(QThread):
    """后台线程：加载已存储因子覆盖信息。"""

    finished = pyqtSignal(object, str)   # (DataFrame, error_msg)

    def __init__(self, symbol: str | None = None):
        super().__init__()
        self.symbol = symbol

    def run(self) -> None:
        try:
            udi = _get_udi()
            if udi is None:
                self.finished.emit(pd.DataFrame(), "数据源不可用")
                return
            df = udi.list_stored_factors(self.symbol or None)
            udi.close()
            self.finished.emit(df, "")
        except Exception as e:
            log.exception("加载因子覆盖失败")
            self.finished.emit(pd.DataFrame(), str(e))


# ---------------------------------------------------------------------------
# Main widget
# ---------------------------------------------------------------------------


class FactorWidget(QWidget):
    """因子分析面板主窗口。"""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self._worker: Optional[_ComputeWorker] = None
        self._coverage_worker: Optional[_LoadCoverageWorker] = None
        self._setup_ui()
        self._load_registry()

    # ── UI 构建 ──────────────────────────────────────────────────────────────

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        # 顶部标题栏
        title = QLabel("📐 因子分析")
        title.setFont(QFont("", 13, QFont.Bold))
        root.addWidget(title)

        # 工具栏
        root.addWidget(self._build_toolbar())

        # 主体：左（因子列表）+ 右（结果 + 覆盖）
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self._build_registry_panel())
        right_tabs = self._build_right_tabs()
        splitter.addWidget(right_tabs)
        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 5)
        root.addWidget(splitter, 1)

        # 状态栏
        self._status = QLabel("就绪")
        self._status.setStyleSheet("color:#6b7280; font-size:11px;")
        root.addWidget(self._status)

    def _build_toolbar(self) -> QWidget:
        bar = QWidget()
        lay = QHBoxLayout(bar)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(8)

        lay.addWidget(QLabel("股票代码:"))
        self._code_edit = QLineEdit("000001.SZ")
        self._code_edit.setFixedWidth(110)
        self._code_edit.setPlaceholderText("如 000001.SZ")
        lay.addWidget(self._code_edit)

        lay.addWidget(QLabel("开始:"))
        self._start_edit = QDateEdit(QDate.currentDate().addDays(-365))
        self._start_edit.setCalendarPopup(True)
        self._start_edit.setDisplayFormat("yyyy-MM-dd")
        lay.addWidget(self._start_edit)

        lay.addWidget(QLabel("结束:"))
        self._end_edit = QDateEdit(QDate.currentDate())
        self._end_edit.setCalendarPopup(True)
        self._end_edit.setDisplayFormat("yyyy-MM-dd")
        lay.addWidget(self._end_edit)

        lay.addWidget(QLabel("因子:"))
        self._factor_combo = QComboBox()
        self._factor_combo.addItem("(全部)")
        self._factor_combo.setFixedWidth(150)
        self._factor_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        lay.addWidget(self._factor_combo)

        self._calc_btn = QPushButton("计算")
        self._calc_btn.setFixedWidth(70)
        self._calc_btn.clicked.connect(self._on_compute)
        lay.addWidget(self._calc_btn)

        self._save_btn = QPushButton("计算+存储")
        self._save_btn.setFixedWidth(90)
        self._save_btn.clicked.connect(self._on_compute_and_save)
        lay.addWidget(self._save_btn)

        sep = QFrame()
        sep.setFrameShape(QFrame.VLine)
        sep.setStyleSheet("color:#d1d5db;")
        lay.addWidget(sep)

        self._cov_btn = QPushButton("刷新存储覆盖")
        self._cov_btn.setFixedWidth(105)
        self._cov_btn.clicked.connect(self._on_refresh_coverage)
        lay.addWidget(self._cov_btn)

        lay.addStretch()
        return bar

    def _build_registry_panel(self) -> QWidget:
        grp = QGroupBox("因子注册表")
        lay = QVBoxLayout(grp)
        lay.setContentsMargins(4, 8, 4, 4)

        self._reg_table = QTableView()
        self._reg_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self._reg_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._reg_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self._reg_table.horizontalHeader().setStretchLastSection(True)
        self._reg_table.verticalHeader().hide()
        self._reg_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._reg_table.setAlternatingRowColors(True)
        self._reg_table.setMinimumWidth(220)

        self._reg_model = _PandasTableModel(pd.DataFrame(columns=["因子名", "类别", "描述"]))
        proxy = QSortFilterProxyModel()
        proxy.setSourceModel(self._reg_model)
        self._reg_table.setModel(proxy)
        self._reg_proxy = proxy

        lay.addWidget(self._reg_table)

        # 类别筛选
        filter_bar = QHBoxLayout()
        filter_bar.addWidget(QLabel("筛选类别:"))
        self._cat_combo = QComboBox()
        self._cat_combo.addItem("全部")
        self._cat_combo.currentTextChanged.connect(self._on_category_filter)
        filter_bar.addWidget(self._cat_combo)
        lay.addLayout(filter_bar)

        return grp

    def _build_right_tabs(self) -> QWidget:
        self._right_tabs = QTabWidget()

        # Tab 1: 计算结果
        result_panel = QWidget()
        rl = QVBoxLayout(result_panel)
        rl.setContentsMargins(4, 4, 4, 4)

        self._result_table = QTableView()
        self._result_table.setSelectionMode(QAbstractItemView.ContiguousSelection)
        self._result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self._result_table.horizontalHeader().setStretchLastSection(True)
        self._result_table.verticalHeader().hide()
        self._result_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._result_table.setAlternatingRowColors(True)

        self._result_model = _PandasTableModel(pd.DataFrame())
        result_proxy = QSortFilterProxyModel()
        result_proxy.setSourceModel(self._result_model)
        self._result_table.setModel(result_proxy)

        rl.addWidget(self._result_table)

        hint_lbl = QLabel('提示：在左侧选择一个或多个因子，输入股票代码与日期范围后点击"计算"')
        hint_lbl.setStyleSheet("color:#9ca3af; font-size:11px;")
        rl.addWidget(hint_lbl)

        self._right_tabs.addTab(result_panel, "📊 计算结果")

        # Tab 2: 存储覆盖
        cov_panel = QWidget()
        cl = QVBoxLayout(cov_panel)
        cl.setContentsMargins(4, 4, 4, 4)

        self._cov_table = QTableView()
        self._cov_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._cov_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self._cov_table.horizontalHeader().setStretchLastSection(True)
        self._cov_table.verticalHeader().hide()
        self._cov_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._cov_table.setAlternatingRowColors(True)

        self._cov_model = _PandasTableModel(pd.DataFrame(
            columns=["symbol", "factor_name", "date_from", "date_to", "row_count", "version"]
        ))
        cov_proxy = QSortFilterProxyModel()
        cov_proxy.setSourceModel(self._cov_model)
        self._cov_table.setModel(cov_proxy)

        cl.addWidget(self._cov_table)

        cov_note = QLabel('点击 [刷新存储覆盖] 加载 DuckDB 中已持久化的因子数据。')
        cov_note.setStyleSheet("color:#9ca3af; font-size:11px;")
        cl.addWidget(cov_note)

        self._right_tabs.addTab(cov_panel, "🗄️ 存储覆盖")

        return self._right_tabs

    # ── 数据加载 ─────────────────────────────────────────────────────────────

    def _load_registry(self) -> None:
        """把因子注册中心的元数据加载到左侧列表中。"""
        registry = _get_factor_registry()
        if registry is None:
            self._set_status("因子注册中心不可用")
            return

        factors = registry.list_all()
        rows = []
        cats: set[str] = set()
        for f in factors:
            if isinstance(f, dict):
                name = f.get("name", "")
                cat = f.get("category", "")
                desc = f.get("description", "")
            else:
                name = getattr(f, "name", "")
                cat = getattr(f, "category", "")
                desc = getattr(f, "description", "")
            rows.append({"因子名": name, "类别": cat, "描述": desc})
            cats.add(cat)

        df = pd.DataFrame(rows)
        self._reg_model.update_data(df)
        self._reg_table.resizeColumnsToContents()

        # 更新类别下拉
        self._cat_combo.blockSignals(True)
        self._cat_combo.clear()
        self._cat_combo.addItem("全部")
        for c in sorted(cats):
            self._cat_combo.addItem(c)
        self._cat_combo.blockSignals(False)

        # 更新因子选择下拉
        self._factor_combo.blockSignals(True)
        self._factor_combo.clear()
        self._factor_combo.addItem("(全部)")
        for row in rows:
            self._factor_combo.addItem(row["因子名"])
        self._factor_combo.blockSignals(False)

        self._set_status(f"已加载 {len(rows)} 个因子")

    def _selected_factor_names(self) -> list[str]:
        """返回当前工具栏/注册表中选择的因子名列表。"""
        combo_val = self._factor_combo.currentText()
        if combo_val and combo_val != "(全部)":
            return [combo_val]

        # 从注册表选中行取
        selected_rows = self._reg_table.selectionModel().selectedRows()
        if selected_rows:
            names = []
            for proxy_idx in selected_rows:
                src_idx = self._reg_proxy.mapToSource(proxy_idx)
                name_val = self._reg_model._df.iloc[src_idx.row()]["因子名"]
                names.append(str(name_val))
            return names

        # 全部
        registry = _get_factor_registry()
        if registry:
            return registry.list_names()
        return []

    # ── 事件槽 ──────────────────────────────────────────────────────────────

    def _on_category_filter(self, cat: str) -> None:
        if cat == "全部":
            self._reg_proxy.setFilterFixedString("")
        else:
            self._reg_proxy.setFilterKeyColumn(1)
            self._reg_proxy.setFilterFixedString(cat)

    def _on_compute(self) -> None:
        self._start_compute(save=False)

    def _on_compute_and_save(self) -> None:
        self._start_compute(save=True)

    def _start_compute(self, save: bool) -> None:
        if self._worker is not None and self._worker.isRunning():
            QMessageBox.information(self, "提示", "计算任务正在进行中，请稍候…")
            return

        factor_names = self._selected_factor_names()
        if not factor_names:
            QMessageBox.warning(self, "未选择因子", "请先在左侧选择至少一个因子，或在下拉框中选择。")
            return

        stock_code = self._code_edit.text().strip().upper()
        if not stock_code:
            QMessageBox.warning(self, "缺少股票代码", "请输入股票代码（如 000001.SZ）。")
            return

        start_date = self._start_edit.date().toString("yyyy-MM-dd")
        end_date = self._end_edit.date().toString("yyyy-MM-dd")

        self._set_busy(True)
        self._set_status(f"正在计算 {len(factor_names)} 个因子… [{stock_code} {start_date}~{end_date}]")

        self._worker = _ComputeWorker(factor_names, stock_code, start_date, end_date, save)
        self._worker.progress.connect(self._set_status)
        self._worker.finished.connect(self._on_compute_done)
        self._worker.start()

    def _on_compute_done(self, result: pd.DataFrame, error: str) -> None:
        self._set_busy(False)
        if error:
            self._set_status(f"错误: {error}")
            QMessageBox.critical(self, "计算失败", error)
            return

        if result.empty:
            self._set_status("计算完成，但结果为空（可能数据不足）")
        else:
            self._set_status(
                f"计算完成：{result.shape[1]} 个因子 × {result.shape[0]} 行 "
                f"（NaN 率: {result.isna().mean().mean():.1%}）"
            )

        self._result_model.update_data(result)
        self._result_table.resizeColumnsToContents()
        self._right_tabs.setCurrentIndex(0)

    def _on_refresh_coverage(self) -> None:
        if self._coverage_worker is not None and self._coverage_worker.isRunning():
            return
        symbol = self._code_edit.text().strip().upper() or None
        self._set_status("正在加载存储覆盖信息…")
        self._cov_btn.setEnabled(False)
        self._coverage_worker = _LoadCoverageWorker(symbol)
        self._coverage_worker.finished.connect(self._on_coverage_done)
        self._coverage_worker.start()

    def _on_coverage_done(self, df: pd.DataFrame, error: str) -> None:
        self._cov_btn.setEnabled(True)
        if error:
            self._set_status(f"覆盖加载失败: {error}")
            return
        self._cov_model.update_data(df)
        self._cov_table.resizeColumnsToContents()
        self._right_tabs.setCurrentIndex(1)
        self._set_status(f"存储覆盖已刷新：{len(df)} 条记录")

    # ── 辅助方法 ─────────────────────────────────────────────────────────────

    def _set_status(self, msg: str) -> None:
        self._status.setText(msg)
        QApplication.processEvents()

    def _set_busy(self, busy: bool) -> None:
        self._calc_btn.setEnabled(not busy)
        self._save_btn.setEnabled(not busy)
