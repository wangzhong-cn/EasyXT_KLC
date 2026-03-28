"""Sprint 7: 持仓 & 结算面板

PositionsModel   — 11 列当前持仓的 QAbstractTableModel
SettlementModel  — 9 列历史结算快照的 QAbstractTableModel
PositionsPanel   — QWidget，内含「持仓」/「结算」两个子 Tab
"""

from __future__ import annotations

import datetime
from typing import Any

from PyQt5.QtCore import QAbstractTableModel, QModelIndex, Qt, pyqtSignal
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableView,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

# ── 列定义 ────────────────────────────────────────────────────────────────────

_POS_COLUMNS: list[tuple[str, str]] = [
    ("代码", "code"),
    ("名称", "name"),
    ("持仓量", "volume"),
    ("可用", "can_use_volume"),
    ("成本", "cost_price"),
    ("现价", "current_price"),
    ("市值", "market_value"),
    ("盈亏金额", "pnl"),
    ("盈亏%", "pnl_pct"),
    ("今日盈亏", "today_pnl"),
    ("账户", "account_id"),
]

_SETTLE_COLUMNS: list[tuple[str, str]] = [
    ("日期", "date"),
    ("代码", "code"),
    ("名称", "name"),
    ("持仓量", "volume"),
    ("成本", "cost_price"),
    ("收盘价", "close_price"),
    ("市值", "market_value"),
    ("盈亏金额", "pnl"),
    ("盈亏%", "pnl_pct"),
]

_PNL_KEYS = {"pnl", "pnl_pct", "today_pnl"}
_PRICE3_KEYS = {"cost_price", "current_price", "close_price"}

# ── 持仓演示数据已移除 — 所有数据必须来自 QMT 真实账户 ──────────────────────


def _make_settlement_rows(date_str: str) -> list[dict[str, Any]]:
    """结算数据需要通过 QMT 真实交易接口获取，当前返回空列表。"""
    # TODO: 接入 easy_xt.get_api().trade.get_settlement(date_str)
    return []


# ── 通用 TableModel 基类 ──────────────────────────────────────────────────────

class _BaseTableModel(QAbstractTableModel):
    """供 PositionsModel / SettlementModel 共用的只读表格模型。"""

    _COLUMNS: list[tuple[str, str]] = []

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._rows: list[dict[str, Any]] = []

    # ── QAbstractTableModel 接口 ──────────────────────────────────────────────

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._COLUMNS)

    def headerData(self, section: int, orientation: Qt.Orientation,  # type: ignore[override]
                   role: int = Qt.DisplayRole) -> Any:
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self._COLUMNS[section][0]
        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:  # type: ignore[override]
        if not index.isValid() or index.row() >= len(self._rows):
            return None
        key = self._COLUMNS[index.column()][1]
        val = self._rows[index.row()].get(key)

        if role == Qt.DisplayRole:
            return self._fmt(key, val)

        if role == Qt.TextAlignmentRole and index.column() >= 2:
            return int(Qt.AlignRight | Qt.AlignVCenter)

        if role == Qt.ForegroundRole and key in _PNL_KEYS:
            v = val if isinstance(val, (int, float)) else 0
            if v > 0:
                return QColor(220, 60, 60)   # 涨 — 红
            if v < 0:
                return QColor(40, 180, 40)   # 跌 — 绿

        return None

    # ── 公开 API ──────────────────────────────────────────────────────────────

    def load(self, rows: list[dict[str, Any]]) -> None:
        self.beginResetModel()
        self._rows = list(rows)
        self.endResetModel()

    def clear(self) -> None:
        self.load([])

    # ── 内部格式化 ────────────────────────────────────────────────────────────

    @staticmethod
    def _fmt(key: str, val: Any) -> str:
        if val is None:
            return ""
        if isinstance(val, float):
            if key in _PRICE3_KEYS:
                return f"{val:.3f}"
            if key == "pnl_pct":
                return f"{val:+.2f}%"
            if key in _PNL_KEYS:
                return f"{val:+,.2f}"
            return f"{val:,.2f}"
        return str(val)


# ── 具体模型 ──────────────────────────────────────────────────────────────────


class PositionsModel(_BaseTableModel):
    _COLUMNS = _POS_COLUMNS


class SettlementModel(_BaseTableModel):
    _COLUMNS = _SETTLE_COLUMNS


# ── 主面板 ────────────────────────────────────────────────────────────────────


class PositionsPanel(QWidget):
    """持仓 & 结算面板 (Sprint 7)。

    - 「持仓」Tab：11 列，账户 Combo + 刷新按钮 + 底部汇总
    - 「结算」Tab：9 列，日期 Combo（最近 7 个工作日）
    - 双击持仓行发出 ``symbol_clicked`` 信号（供外部跳转 K 线图）
    - 严禁内置 Demo 数据；等待 QMT 真实持仓/结算数据注入
    """

    symbol_clicked = pyqtSignal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._setup_ui()

    # ── 界面搭建 ──────────────────────────────────────────────────────────────

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(0)

        self._tabs = QTabWidget()
        self._tabs.addTab(self._build_positions_tab(), "持仓")
        self._tabs.addTab(self._build_settlement_tab(), "结算")
        layout.addWidget(self._tabs)

    def _build_positions_tab(self) -> QWidget:
        tab = QWidget()
        vl = QVBoxLayout(tab)
        vl.setContentsMargins(2, 4, 2, 2)
        vl.setSpacing(4)

        # 工具栏
        toolbar = QWidget()
        toolbar.setFixedHeight(34)
        hl = QHBoxLayout(toolbar)
        hl.setContentsMargins(0, 0, 0, 0)
        hl.setSpacing(6)

        hl.addWidget(QLabel("账户:"))
        self._account_combo = QComboBox()
        self._account_combo.setMinimumWidth(150)
        self._account_combo.addItem("待连接账户", "pending")
        hl.addWidget(self._account_combo)
        hl.addStretch()

        self._refresh_btn = QPushButton("刷新")
        self._refresh_btn.setFixedWidth(56)
        self._refresh_btn.clicked.connect(self._on_refresh)
        hl.addWidget(self._refresh_btn)

        self._status_label = QLabel("就绪")
        self._status_label.setStyleSheet("color:#888;font-size:11px;min-width:90px;")
        hl.addWidget(self._status_label)

        vl.addWidget(toolbar)

        # 持仓表
        self._pos_model = PositionsModel()
        self._pos_view = QTableView()
        self._pos_view.setModel(self._pos_model)
        self._pos_view.horizontalHeader().setStretchLastSection(True)
        self._pos_view.setAlternatingRowColors(True)
        self._pos_view.setSelectionBehavior(QTableView.SelectRows)
        self._pos_view.setEditTriggers(QTableView.NoEditTriggers)
        self._pos_view.verticalHeader().setVisible(False)
        self._pos_view.doubleClicked.connect(self._on_row_double_clicked)
        vl.addWidget(self._pos_view, 1)

        # 底部汇总
        footer = QWidget()
        footer.setFixedHeight(22)
        fl = QHBoxLayout(footer)
        fl.setContentsMargins(4, 0, 4, 0)
        fl.setSpacing(16)
        self._count_label = QLabel("持仓: 0 只")
        self._value_label = QLabel("市值: ¥0.00")
        self._pnl_label = QLabel("盈亏: ¥0.00")
        for lbl in (self._count_label, self._value_label, self._pnl_label):
            lbl.setStyleSheet("font-size:11px;color:#aaa;")
            fl.addWidget(lbl)
        fl.addStretch()
        vl.addWidget(footer)

        return tab

    def _build_settlement_tab(self) -> QWidget:
        tab = QWidget()
        vl = QVBoxLayout(tab)
        vl.setContentsMargins(2, 4, 2, 2)
        vl.setSpacing(4)

        # 工具栏
        toolbar = QWidget()
        toolbar.setFixedHeight(34)
        hl = QHBoxLayout(toolbar)
        hl.setContentsMargins(0, 0, 0, 0)
        hl.setSpacing(6)
        hl.addWidget(QLabel("日期:"))
        self._date_combo = QComboBox()
        self._date_combo.setMinimumWidth(120)
        self._populate_date_combo()
        hl.addWidget(self._date_combo)
        hl.addStretch()
        vl.addWidget(toolbar)

        # 结算表
        self._settle_model = SettlementModel()
        self._settle_view = QTableView()
        self._settle_view.setModel(self._settle_model)
        self._settle_view.horizontalHeader().setStretchLastSection(True)
        self._settle_view.setAlternatingRowColors(True)
        self._settle_view.setSelectionBehavior(QTableView.SelectRows)
        self._settle_view.setEditTriggers(QTableView.NoEditTriggers)
        self._settle_view.verticalHeader().setVisible(False)
        vl.addWidget(self._settle_view, 1)

        self._date_combo.currentTextChanged.connect(self._on_date_changed)
        if self._date_combo.count():
            self._on_date_changed(self._date_combo.currentText())

        return tab

    def _populate_date_combo(self) -> None:
        today = datetime.date.today()
        d = today
        days: list[str] = []
        while len(days) < 7:
            if d.weekday() < 5:          # 周一-周五
                days.append(d.isoformat())
            d -= datetime.timedelta(days=1)
        self._date_combo.addItems(days)

    # ── 槽函数 ────────────────────────────────────────────────────────────────

    def _on_refresh(self) -> None:
        # TODO: easy_xt.get_api().trade.get_positions(account_id) 真实账户
        self._pos_model.load([])
        self._update_footer([])
        self._status_label.setText("等待 QMT 持仓数据")

    def _on_date_changed(self, date_str: str) -> None:
        self._settle_model.load(_make_settlement_rows(date_str))

    def _on_row_double_clicked(self, index: QModelIndex) -> None:
        row = index.row()
        if row < len(self._pos_model._rows):
            code = str(self._pos_model._rows[row].get("code", ""))
            if code:
                self.symbol_clicked.emit(code)

    def _update_footer(self, rows: list[dict[str, Any]]) -> None:
        n = len(rows)
        mv = sum(float(r.get("market_value") or 0) for r in rows)
        pnl = sum(float(r.get("pnl") or 0) for r in rows)
        self._count_label.setText(f"持仓: {n} 只")
        self._value_label.setText(f"市值: ¥{mv:,.2f}")
        color = "rgb(220,60,60)" if pnl > 0 else ("rgb(40,180,40)" if pnl < 0 else "#aaa")
        self._pnl_label.setText(f"盈亏: ¥{pnl:+,.2f}")
        self._pnl_label.setStyleSheet(f"font-size:11px;color:{color};")

    # ── 公开 API（供外部信号槽调用） ─────────────────────────────────────────

    def update_positions(self, positions: list[dict[str, Any]],
                         account_id: str = "") -> None:
        """从外部注入实时持仓数据。"""
        self._pos_model.load(positions)
        self._update_footer(positions)
        self._status_label.setText(
            f"更新 {datetime.datetime.now().strftime('%H:%M:%S')}"
        )

    def clear_data(self) -> None:
        self._pos_model.clear()
        self._update_footer([])

    def showEvent(self, a0: Any) -> None:  # type: ignore[override]
        super().showEvent(a0)
        if not self._pos_model.rowCount():
            self._on_refresh()
