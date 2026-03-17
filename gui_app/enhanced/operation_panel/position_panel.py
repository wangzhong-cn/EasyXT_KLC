from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QGroupBox, QHeaderView, QTableWidget, QTableWidgetItem, QVBoxLayout


class PositionPanel(QGroupBox):
    def __init__(self):
        super().__init__("持仓列表")
        position_layout = QVBoxLayout(self)

        self.position_table = QTableWidget(0, 4)
        self.position_table.setHorizontalHeaderLabels(["股票代码", "持仓数量", "可用数量", "成本"])

        header = self.position_table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QHeaderView.Stretch)

        self.position_table.setAlternatingRowColors(True)
        self.position_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.position_table.setMinimumHeight(200)

        position_layout.addWidget(self.position_table)

    def update_positions(self, positions):
        self.position_table.setRowCount(len(positions))
        for row, pos in enumerate(positions):
            items = [
                pos.get("stock_code", ""),
                str(pos.get("volume", 0)),
                str(pos.get("available_volume", 0)),
                f"{pos.get('cost_price', 0):.2f}",
            ]
            for col, item in enumerate(items):
                table_item = QTableWidgetItem(item)
                if col > 0:
                    table_item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
                self.position_table.setItem(row, col, table_item)

    def clear_data(self):
        self.position_table.setRowCount(0)
