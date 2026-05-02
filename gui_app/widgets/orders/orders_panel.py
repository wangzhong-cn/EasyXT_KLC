"""Sprint 8: 委托 & 成交面板

OrdersModel  — 10 列当日委托的 QAbstractTableModel
TradesModel  — 9 列当日成交的 QAbstractTableModel
OrdersPanel  — QWidget，内含「当日委托」/「当日成交」两个子 Tab
"""

from __future__ import annotations

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

_ORDER_COLUMNS: list[tuple[str, str]] = [
    ("委托编号", "order_id"),
    ("时间",     "time"),
    ("代码",     "code"),
    ("名称",     "name"),
    ("方向",     "order_type"),
    ("委托量",   "volume"),
    ("委托价",   "price"),
    ("成交量",   "traded_volume"),
    ("状态",     "status"),
    ("备注",     "remark"),
]

_TRADE_COLUMNS: list[tuple[str, str]] = [
    ("时间",   "time"),
    ("代码",   "code"),
    ("名称",   "name"),
    ("方向",   "order_type"),
    ("成交量", "volume"),
    ("成交价", "price"),
    ("成交额", "amount"),
    ("委托号", "order_id"),
    ("备注",   "remark"),
]

# 已撤/废单 → 灰色显示；已成订单保持方向颜色
_DONE_STATUSES = {"已撤", "部撤", "废单"}

# Demo 委托/成交数据已移除 — 所有数据必须来自 QMT 真实交易接口

# 颜色常量
_BUY_COLOR  = QColor(220, 60,  60)   # 买入 — 红
_SELL_COLOR = QColor(40,  180, 40)   # 卖出 — 绿
_DONE_COLOR = QColor(130, 130, 130)  # 废/撤 — 灰


# ── 通用 TableModel 基类 ──────────────────────────────────────────────────────

class _BaseTableModel(QAbstractTableModel):
    _COLUMNS: list[tuple[str, str]] = []

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._rows: list[dict[str, Any]] = []

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
        row  = self._rows[index.row()]
        key  = self._COLUMNS[index.column()][1]
        val  = row.get(key)

        if role == Qt.DisplayRole:
            return self._fmt(key, val)

        if role == Qt.TextAlignmentRole and key in {
            "volume", "price", "traded_volume", "amount", "order_id",
        }:
            return int(Qt.AlignRight | Qt.AlignVCenter)

        if role == Qt.ForegroundRole:
            # 已撤/废单行统一灰色
            status = row.get("status", "")
            if status in _DONE_STATUSES:
                return _DONE_COLOR
            # 方向颜色
            if key == "order_type":
                return _BUY_COLOR if val == "买入" else _SELL_COLOR

        return None

    def load(self, rows: list[dict[str, Any]]) -> None:
        self.beginResetModel()
        self._rows = list(rows)
        self.endResetModel()

    def clear(self) -> None:
        self.load([])

    @staticmethod
    def _fmt(key: str, val: Any) -> str:
        if val is None:
            return ""
        if isinstance(val, float):
            if key in {"price", "amount"}:
                return f"{val:,.3f}" if key == "price" else f"{val:,.2f}"
            return f"{val:,.2f}"
        return str(val)


# ── 具体模型 ──────────────────────────────────────────────────────────────────

class OrdersModel(_BaseTableModel):
    _COLUMNS = _ORDER_COLUMNS


class TradesModel(_BaseTableModel):
    _COLUMNS = _TRADE_COLUMNS


# ── 主面板 ────────────────────────────────────────────────────────────────────

class OrdersPanel(QWidget):
    """委托 & 成交面板 (Sprint 8)。

    - 「当日委托」Tab：10 列，账户 Combo + 刷新按钮 + 全撤按钮 + 底部汇总
    - 「当日成交」Tab：9 列，自动随刷新联动
    - 双击委托/成交行代码列 → ``symbol_clicked`` 信号（供外部跳转 K 线图）
    - 严禁内置 Demo 数据；等待 QMT 真实委托/成交数据注入
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
        self._tabs.addTab(self._build_orders_tab(), "当日委托")
        self._tabs.addTab(self._build_trades_tab(), "当日成交")
        layout.addWidget(self._tabs)

    def _build_orders_tab(self) -> QWidget:
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

        self._cancel_all_btn = QPushButton("全撤")
        self._cancel_all_btn.setFixedWidth(56)
        self._cancel_all_btn.setStyleSheet("color:#b45309;")
        self._cancel_all_btn.clicked.connect(self._on_cancel_all)
        hl.addWidget(self._cancel_all_btn)

        self._status_label = QLabel("就绪")
        self._status_label.setStyleSheet("color:#888;font-size:11px;min-width:90px;")
        hl.addWidget(self._status_label)

        vl.addWidget(toolbar)

        # 委托表
        self._orders_model = OrdersModel()
        self._orders_view = QTableView()
        self._orders_view.setModel(self._orders_model)
        self._orders_view.horizontalHeader().setStretchLastSection(True)
        self._orders_view.setAlternatingRowColors(True)
        self._orders_view.setSelectionBehavior(QTableView.SelectRows)
        self._orders_view.setEditTriggers(QTableView.NoEditTriggers)
        self._orders_view.verticalHeader().setVisible(False)
        self._orders_view.doubleClicked.connect(self._on_order_double_clicked)
        vl.addWidget(self._orders_view, 1)

        # 底部汇总
        footer = QWidget()
        footer.setFixedHeight(22)
        fl = QHBoxLayout(footer)
        fl.setContentsMargins(4, 0, 4, 0)
        fl.setSpacing(16)
        self._order_count_label  = QLabel("委托: 0 笔")
        self._order_filled_label = QLabel("已成: 0 笔")
        self._order_cancel_label = QLabel("撤单: 0 笔")
        for lbl in (self._order_count_label, self._order_filled_label, self._order_cancel_label):
            lbl.setStyleSheet("font-size:11px;color:#aaa;")
            fl.addWidget(lbl)
        fl.addStretch()
        vl.addWidget(footer)

        return tab

    def _build_trades_tab(self) -> QWidget:
        tab = QWidget()
        vl = QVBoxLayout(tab)
        vl.setContentsMargins(2, 4, 2, 2)
        vl.setSpacing(4)

        # 工具栏（空工具栏，与委托Tab等高对齐）
        toolbar = QWidget()
        toolbar.setFixedHeight(34)
        hl = QHBoxLayout(toolbar)
        hl.setContentsMargins(0, 0, 0, 0)
        hl.addStretch()
        self._trade_count_lbl = QLabel("")
        self._trade_count_lbl.setStyleSheet("color:#888;font-size:11px;")
        hl.addWidget(self._trade_count_lbl)
        vl.addWidget(toolbar)

        # 成交表
        self._trades_model = TradesModel()
        self._trades_view = QTableView()
        self._trades_view.setModel(self._trades_model)
        self._trades_view.horizontalHeader().setStretchLastSection(True)
        self._trades_view.setAlternatingRowColors(True)
        self._trades_view.setSelectionBehavior(QTableView.SelectRows)
        self._trades_view.setEditTriggers(QTableView.NoEditTriggers)
        self._trades_view.verticalHeader().setVisible(False)
        self._trades_view.doubleClicked.connect(self._on_trade_double_clicked)
        vl.addWidget(self._trades_view, 1)

        # 底部成交额汇总
        footer = QWidget()
        footer.setFixedHeight(22)
        fl = QHBoxLayout(footer)
        fl.setContentsMargins(4, 0, 4, 0)
        fl.setSpacing(16)
        self._trade_amount_label = QLabel("成交额: ¥0.00")
        self._trade_amount_label.setStyleSheet("font-size:11px;color:#aaa;")
        fl.addWidget(self._trade_amount_label)
        fl.addStretch()
        vl.addWidget(footer)

        return tab

    # ── 槽函数 ────────────────────────────────────────────────────────────────

    def _on_refresh(self) -> None:
        self._orders_model.load([])
        self._trades_model.load([])
        self._update_order_footer([])
        self._update_trade_footer([])
        self._status_label.setText("等待 QMT 委托/成交数据")
        self._trade_count_lbl.setText("")

    def _on_cancel_all(self) -> None:
        # TODO: 对接真实撤单 API
        self._status_label.setText("等待 QMT 撤单接口")

    def _on_order_double_clicked(self, index: QModelIndex) -> None:
        row = index.row()
        if row < len(self._orders_model._rows):
            code = str(self._orders_model._rows[row].get("code", ""))
            if code:
                self.symbol_clicked.emit(code)

    def _on_trade_double_clicked(self, index: QModelIndex) -> None:
        row = index.row()
        if row < len(self._trades_model._rows):
            code = str(self._trades_model._rows[row].get("code", ""))
            if code:
                self.symbol_clicked.emit(code)

    def _update_order_footer(self, rows: list[dict[str, Any]]) -> None:
        n = len(rows)
        filled = sum(1 for r in rows if r.get("status") in {"已成", "部成"})
        canceled = sum(1 for r in rows if r.get("status") in {"已撤", "部撤"})
        self._order_count_label.setText(f"委托: {n} 笔")
        self._order_filled_label.setText(f"已成: {filled} 笔")
        self._order_cancel_label.setText(f"撤单: {canceled} 笔")

    def _update_trade_footer(self, rows: list[dict[str, Any]]) -> None:
        total = sum(float(r.get("amount") or 0) for r in rows)
        self._trade_amount_label.setText(f"成交额: ¥{total:,.2f}")

    # ── 公开 API ──────────────────────────────────────────────────────────────

    def update_orders(self, orders: list[dict[str, Any]],
                      trades: list[dict[str, Any]] | None = None) -> None:
        """从外部注入实时委托/成交数据。"""
        import datetime
        self._orders_model.load(orders)
        self._update_order_footer(orders)
        if trades is not None:
            self._trades_model.load(trades)
            self._update_trade_footer(trades)
            self._trade_count_lbl.setText(f"共 {len(trades)} 笔成交")
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        self._status_label.setText(f"更新 {ts}")

    def clear_data(self) -> None:
        self._orders_model.clear()
        self._trades_model.clear()
        self._update_order_footer([])
        self._update_trade_footer([])
        self._status_label.setText("就绪")
        self._trade_count_lbl.setText("")

    def showEvent(self, a0: Any) -> None:  # type: ignore[override]
        super().showEvent(a0)
        if not self._orders_model.rowCount():
            self._on_refresh()
