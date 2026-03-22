from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import (
    QComboBox,
    QDoubleSpinBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSpinBox,
)

from core.events import Events
from core.signal_bus import signal_bus


class OrderPanel(QGroupBox):
    order_requested = pyqtSignal(str, str, float, int)

    def __init__(self):
        super().__init__("交易操作")
        self.setFixedHeight(138)
        layout = QGridLayout(self)

        layout.addWidget(QLabel("股票代码:"), 0, 0)
        self.stock_combo = QComboBox()
        self.stock_combo.setEditable(True)
        self.stock_combo.addItems(["000001.SZ", "600000.SH", "000002.SZ", "600036.SH"])
        layout.addWidget(self.stock_combo, 0, 1)

        layout.addWidget(QLabel("数量(股):"), 1, 0)
        self.volume_spin = QSpinBox()
        self.volume_spin.setRange(100, 999999)
        self.volume_spin.setValue(100)
        self.volume_spin.setSingleStep(100)
        layout.addWidget(self.volume_spin, 1, 1)

        layout.addWidget(QLabel("价格:"), 2, 0)
        self.price_spin = QDoubleSpinBox()
        self.price_spin.setRange(0.01, 9999.99)
        self.price_spin.setValue(0.01)
        self.price_spin.setDecimals(2)
        self.price_spin.setSingleStep(0.01)
        layout.addWidget(self.price_spin, 2, 1)
        self.crosshair_hint_label = QLabel("十字: --")
        self.crosshair_hint_label.setStyleSheet("color:#7c9fbf; font-size:10px;")
        layout.addWidget(self.crosshair_hint_label, 3, 0, 1, 2)

        button_layout = QHBoxLayout()
        self.buy_btn = QPushButton("📈 买入")
        self.buy_btn.setFixedSize(100, 35)
        self.buy_btn.setStyleSheet("""
            QPushButton {
                background-color: #ff4444;
                color: white;
                border: none;
                border-radius: 3px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #ff6666;
            }
            QPushButton:pressed {
                background-color: #cc3333;
            }
        """)
        self.buy_btn.clicked.connect(lambda: self._emit_order("buy"))

        self.sell_btn = QPushButton("📉 卖出")
        self.sell_btn.setFixedSize(100, 35)
        self.sell_btn.setStyleSheet("""
            QPushButton {
                background-color: #00aa00;
                color: white;
                border: none;
                border-radius: 3px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #00cc00;
            }
            QPushButton:pressed {
                background-color: #008800;
            }
        """)
        self.sell_btn.clicked.connect(lambda: self._emit_order("sell"))

        button_layout.addWidget(self.buy_btn)
        button_layout.addWidget(self.sell_btn)
        button_layout.addStretch()
        layout.addLayout(button_layout, 0, 2, 3, 1)

        self._connect_events()

    def _connect_events(self):
        signal_bus.subscribe(Events.CHART_DATA_LOADED, self._on_chart_data_loaded)

    def _on_chart_data_loaded(self, symbol: str, **kwargs):
        self.set_stock_code(symbol)

    def set_stock_code(self, symbol: str):
        if not symbol:
            return
        self.stock_combo.setCurrentText(symbol)

    def set_price(self, price: float):
        if price is None:
            return
        try:
            self.price_spin.setValue(float(price))
        except Exception:
            return

    def set_crosshair_hint(self, time_value: object, price_value: object) -> None:
        if time_value is None and price_value is None:
            self.crosshair_hint_label.setText("十字: --")
            return
        # 时间：Unix ts → 本地时间；价格：2位小数
        if isinstance(time_value, (int, float)) and float(time_value) > 1_000_000_000:
            from datetime import datetime
            try:
                t_str = datetime.fromtimestamp(float(time_value)).strftime("%Y-%m-%d %H:%M")
            except (OSError, OverflowError, ValueError):
                t_str = str(time_value)
        else:
            t_str = str(time_value) if time_value is not None else ""
        if isinstance(price_value, (int, float)):
            p_str = f"{float(price_value):.2f}"
        elif isinstance(price_value, str):
            try:
                p_str = f"{float(price_value):.2f}"
            except (TypeError, ValueError):
                p_str = price_value
        else:
            p_str = str(price_value) if price_value is not None else ""
        parts = ([f"t={t_str}"] if t_str else []) + ([f"p={p_str}"] if p_str else [])
        self.crosshair_hint_label.setText("十字: " + ("  ".join(parts) or "--"))

    def _emit_order(self, side: str):
        symbol = self.stock_combo.currentText()
        volume = self.volume_spin.value()
        price = self.price_spin.value()
        self.order_requested.emit(side, symbol, price, volume)
