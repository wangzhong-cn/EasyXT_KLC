from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)


class AccountPanel(QGroupBox):
    account_changed = pyqtSignal(str)

    def __init__(self):
        super().__init__("账户信息")
        self.setFixedHeight(230)
        account_layout = QVBoxLayout(self)

        selector_layout = QHBoxLayout()
        selector_layout.addWidget(QLabel("账户选择:"))
        self.account_combo = QComboBox()
        self.account_combo.setMinimumWidth(180)
        self.account_combo.currentIndexChanged.connect(self._on_account_changed)
        selector_layout.addWidget(self.account_combo)
        selector_layout.addStretch()
        account_layout.addLayout(selector_layout)

        self.account_table = QTableWidget(4, 2)
        self.account_table.setHorizontalHeaderLabels(["项目", "金额"])
        self.account_table.setVerticalHeaderLabels(["总资产", "可用资金", "持仓市值", "今日盈亏"])
        self.account_table.horizontalHeader().setStretchLastSection(True)
        self.account_table.verticalHeader().setDefaultSectionSize(30)
        self.account_table.setAlternatingRowColors(True)
        self.account_table.setSelectionBehavior(QTableWidget.SelectRows)
        self._init_table()

        account_layout.addWidget(self.account_table)

        self.rejection_table = QTableWidget(3, 2)
        self.rejection_table.setHorizontalHeaderLabels(["项目", "统计"])
        self.rejection_table.verticalHeader().setVisible(False)
        self.rejection_table.horizontalHeader().setStretchLastSection(True)
        self.rejection_table.setAlternatingRowColors(True)
        self.rejection_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.rejection_table.setFixedHeight(110)
        self._init_rejection_table()
        account_layout.addWidget(self.rejection_table)

    def _init_table(self):
        items = [
            ("总资产", "0.00"),
            ("可用资金", "0.00"),
            ("持仓市值", "0.00"),
            ("今日盈亏", "0.00"),
        ]
        for row, (item, value) in enumerate(items):
            self.account_table.setItem(row, 0, QTableWidgetItem(item))
            amount_item = QTableWidgetItem(value)
            amount_item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
            self.account_table.setItem(row, 1, amount_item)

    def _init_rejection_table(self):
        items = [
            ("拒单次数", "0"),
            ("原因分布", "无"),
            ("标的分布", "无"),
        ]
        for row, (item, value) in enumerate(items):
            self.rejection_table.setItem(row, 0, QTableWidgetItem(item))
            stats_item = QTableWidgetItem(value)
            stats_item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
            self.rejection_table.setItem(row, 1, stats_item)

    def update_rejection_stats(self, stats):
        total = int(stats.get("total", 0))
        reasons = stats.get("reasons", {})
        symbols = stats.get("symbols", {})
        items = [
            ("拒单次数", str(total)),
            ("原因分布", self._format_distribution(reasons)),
            ("标的分布", self._format_distribution(symbols)),
        ]
        for row, (_, value) in enumerate(items):
            stats_item = QTableWidgetItem(value)
            stats_item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
            self.rejection_table.setItem(row, 1, stats_item)

    def _format_distribution(self, distribution, top_n=3):
        if not distribution:
            return "无"
        sorted_items = sorted(distribution.items(), key=lambda item: item[1], reverse=True)
        return " | ".join(f"{key}:{count}" for key, count in sorted_items[:top_n])

    def set_accounts(self, accounts, current_id=None):
        self.account_combo.blockSignals(True)
        self.account_combo.clear()
        for account in accounts:
            account_id = account.get("account_id")
            account_type = account.get("account_type")
            if not account_id:
                continue
            label = str(account_id)
            if account_type is not None:
                label = f"{label} ({account_type})"
            self.account_combo.addItem(label, str(account_id))
        if current_id:
            idx = self.account_combo.findData(str(current_id))
            if idx >= 0:
                self.account_combo.setCurrentIndex(idx)
        self.account_combo.blockSignals(False)

    def update_account_info(self, account_info):
        items = [
            ("总资产", f"{account_info.get('total_asset', 0):.2f}"),
            ("可用资金", f"{account_info.get('available_cash', 0):.2f}"),
            ("持仓市值", f"{account_info.get('market_value', 0):.2f}"),
            ("今日盈亏", f"{account_info.get('today_pnl', 0):.2f}"),
        ]
        for row, (item, value) in enumerate(items):
            amount_item = QTableWidgetItem(value)
            amount_item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
            if item == "今日盈亏":
                pnl = account_info.get("today_pnl", 0)
                if pnl > 0:
                    amount_item.setForeground(QColor(255, 0, 0))
                elif pnl < 0:
                    amount_item.setForeground(QColor(0, 128, 0))
            self.account_table.setItem(row, 1, amount_item)

    def clear_data(self):
        for row in range(self.account_table.rowCount()):
            self.account_table.setItem(row, 1, QTableWidgetItem("0.00"))
        self.rejection_table.setItem(0, 1, QTableWidgetItem("0"))
        self.rejection_table.setItem(1, 1, QTableWidgetItem("无"))
        self.rejection_table.setItem(2, 1, QTableWidgetItem("无"))

    def _on_account_changed(self):
        account_id = self.account_combo.currentData()
        if account_id is None:
            return
        self.account_changed.emit(str(account_id))
