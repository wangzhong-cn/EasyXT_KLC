from typing import Any

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QFrame, QGridLayout, QLabel, QProgressBar, QVBoxLayout, QWidget


class OrderbookPanel(QFrame):
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setMinimumWidth(220)
        self.setObjectName("OrderbookPanel")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(4)
        self.status_label = QLabel("五档盘口")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.mid_label = QLabel("--")
        self.mid_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.mid_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(self.status_label)
        self.grid = QGridLayout()
        self.grid.setHorizontalSpacing(6)
        self.grid.setVerticalSpacing(2)
        layout.addLayout(self.grid)
        layout.addWidget(self.mid_label)
        self.rows: dict[tuple[str, int], dict[str, Any]] = {}
        self._build_rows()

    def _build_rows(self) -> None:
        row = 0
        for level in range(5, 0, -1):
            self._add_row("ask", level, row)
            row += 1
        for level in range(1, 6):
            self._add_row("bid", level, row)
            row += 1

    def _add_row(self, side: str, level: int, row: int) -> None:
        label = QLabel(f"{'卖' if side == 'ask' else '买'}{level}")
        price = QLabel("--")
        volume = QLabel("--")
        bar = QProgressBar()
        bar.setMaximum(100)
        bar.setTextVisible(False)
        if side == "ask":
            price.setStyleSheet("color: #d9534f;")
            bar.setStyleSheet("QProgressBar::chunk{background:#f2dede;}QProgressBar{border:0;}")
        else:
            price.setStyleSheet("color: #5cb85c;")
            bar.setStyleSheet("QProgressBar::chunk{background:#dff0d8;}QProgressBar{border:0;}")
        self.grid.addWidget(label, row, 0)
        self.grid.addWidget(price, row, 1)
        self.grid.addWidget(volume, row, 2)
        self.grid.addWidget(bar, row, 3)
        self.rows[(side, level)] = {"price": price, "volume": volume, "bar": bar}

    # source: "live" | "db" | "none" | None（不改颜色）
    _STATUS_COLORS = {
        "live": "#27ae60",   # 绿色：实时
        "db":   "#e67e22",   # 橙色：历史快照
        "none": "#c0392b",   # 红色：无数据
    }

    def set_status(self, text: str, source: str = "") -> None:
        self.status_label.setText(text or "五档盘口")
        color = self._STATUS_COLORS.get(source or "")
        if color:
            self.status_label.setStyleSheet(f"color: {color}; font-size: 11px;")
        else:
            self.status_label.setStyleSheet("font-size: 11px;")

    def update_orderbook(self, quote: dict[str, Any]) -> None:
        if not quote:
            return
        price = quote.get("price") or quote.get("last_price") or quote.get("close")
        if price not in (None, ""):
            try:
                self.mid_label.setText(f"{float(price):.2f}")
            except Exception:
                self.mid_label.setText(str(price))
        else:
            self.mid_label.setText("--")
        max_vol = 0
        for side in ("ask", "bid"):
            for level in range(1, 6):
                vol = quote.get(f"{side}{level}_vol") or quote.get(f"{side}{level}_volume")
                if vol not in (None, ""):
                    try:
                        max_vol = max(max_vol, int(vol))
                    except Exception:
                        continue
        max_vol = max(max_vol, 1)
        for side in ("ask", "bid"):
            for level in range(1, 6):
                row = self.rows.get((side, level))
                if not row:
                    continue
                price_val = quote.get(f"{side}{level}")
                vol_val = quote.get(f"{side}{level}_vol") or quote.get(f"{side}{level}_volume")
                row["price"].setText(self._format_price(price_val))
                row["volume"].setText(self._format_volume(vol_val))
                vol_int = self._to_int(vol_val)
                row["bar"].setValue(int(vol_int / max_vol * 100))

    @staticmethod
    def _format_price(value: Any) -> str:
        if value in (None, "", 0):
            return "--"
        try:
            return f"{float(value):.2f}"
        except Exception:
            return str(value)

    @staticmethod
    def _format_volume(value: Any) -> str:
        if value in (None, "", 0):
            return "--"
        try:
            return f"{int(float(value))}"
        except Exception:
            return str(value)

    @staticmethod
    def _to_int(value: Any) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(float(value))
        except Exception:
            return 0
