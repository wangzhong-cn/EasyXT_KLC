"""Sprint 9: 资金账户面板

FundsPanel — 账户资产概览（总资产/可用资金/冻结资金/持仓市值/仓位占比）
数据来源：easy_xt.get_api().trade.get_account_asset(account_id)
Demo 阶段：使用内置演示数据，TODO 对接实时 API
"""

from __future__ import annotations

import datetime
from typing import Any

from PyQt5.QtCore import Qt
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QComboBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

# ── Demo 数据 ────────────────────────────────────────────────────────────────

_DEMO_ASSET: dict[str, Any] = {
    "account_id": "1678070127",
    "cash":         123_456.78,   # 可用资金
    "frozen_cash":    5_000.00,   # 冻结资金
    "market_value": 371_543.22,   # 持仓市值
    "total_asset":  500_000.00,   # 总资产
}

# ── 颜色常量 ─────────────────────────────────────────────────────────────────

_COLOR_TOTAL    = QColor(220, 220, 220)   # 总资产  — 亮白
_COLOR_CASH     = QColor(40,  180, 40)    # 可用资金 — 绿
_COLOR_FROZEN   = QColor(220, 160,  0)   # 冻结资金 — 琥珀
_COLOR_MARKET   = QColor(60,  140, 220)   # 持仓市值 — 蓝
_COLOR_RATIO    = QColor(180, 100, 220)   # 仓位占比 — 紫
_COLOR_LABEL    = QColor(150, 150, 150)   # 标题字体 — 灰

# 格式化辅助
def _fmt_money(val: float) -> str:
    return f"¥{val:>14,.2f}"

def _fmt_pct(val: float) -> str:
    return f"{val:.2f}%"


# ── 指标卡片 ─────────────────────────────────────────────────────────────────

class _MetricCard(QFrame):
    """单个资金指标卡片（标题 + 数值）。"""

    def __init__(self, title: str, color: QColor, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setFrameShape(QFrame.StyledPanel)
        self.setMinimumHeight(70)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(4)

        self._title_lbl = QLabel(title)
        title_color = _COLOR_LABEL.name()
        self._title_lbl.setStyleSheet(
            f"color:{title_color};font-size:11px;"
        )
        layout.addWidget(self._title_lbl)

        self._value_lbl = QLabel("--")
        value_color = color.name()
        self._value_lbl.setStyleSheet(
            f"color:{value_color};font-size:16px;font-weight:bold;"
        )
        layout.addWidget(self._value_lbl)

    # ── 公开 API ──────────────────────────────────────────────────────────────

    def set_value(self, text: str) -> None:
        self._value_lbl.setText(text)

    def get_value(self) -> str:
        return self._value_lbl.text()

    def title(self) -> str:
        return self._title_lbl.text()


# ── 仓位占比进度条行 ──────────────────────────────────────────────────────────

class _PositionBar(QWidget):
    """水平进度条 + 标注，展示仓位占比。"""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        hl = QHBoxLayout(self)
        hl.setContentsMargins(0, 2, 0, 2)
        hl.setSpacing(8)

        self._label = QLabel("仓位占比")
        self._label.setFixedWidth(60)
        self._label.setStyleSheet(f"color:{_COLOR_LABEL.name()};font-size:11px;")
        hl.addWidget(self._label)

        self._bar = QProgressBar()
        self._bar.setRange(0, 100)
        self._bar.setValue(0)
        self._bar.setTextVisible(False)
        self._bar.setFixedHeight(14)
        self._bar.setStyleSheet("""
            QProgressBar{background:#2a2a2a;border-radius:4px;}
            QProgressBar::chunk{background:#8040d0;border-radius:4px;}
        """)
        hl.addWidget(self._bar, 1)

        self._pct_lbl = QLabel("0.00%")
        self._pct_lbl.setFixedWidth(52)
        self._pct_lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self._pct_lbl.setStyleSheet(f"color:{_COLOR_RATIO.name()};font-size:12px;font-weight:bold;")
        hl.addWidget(self._pct_lbl)

    def set_ratio(self, ratio: float) -> None:
        """ratio 取值 0.0–100.0"""
        clamped = max(0.0, min(100.0, ratio))
        self._bar.setValue(int(clamped))
        self._pct_lbl.setText(_fmt_pct(clamped))

    def ratio(self) -> float:
        return self._bar.value()


# ── 主面板 ───────────────────────────────────────────────────────────────────

class FundsPanel(QWidget):
    """资金账户面板 (Sprint 9)。

    显示账户总资产、可用资金、冻结资金、持仓市值、仓位占比。
    - showEvent 首次可见时自动加载 Demo 数据
    - update_asset(asset_dict) — 外部注入实时数据
    - clear_data() — 重置为 "--"
    """

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._setup_ui()
        self._loaded = False

    # ── 界面搭建 ──────────────────────────────────────────────────────────────

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 6, 8, 6)
        root.setSpacing(6)

        # 工具栏
        toolbar = QWidget()
        toolbar.setFixedHeight(36)
        hl = QHBoxLayout(toolbar)
        hl.setContentsMargins(0, 0, 0, 0)
        hl.setSpacing(8)

        hl.addWidget(QLabel("账户:"))
        self._account_combo = QComboBox()
        self._account_combo.setMinimumWidth(160)
        self._account_combo.addItem("演示账户  1678070127", "demo")
        hl.addWidget(self._account_combo)
        hl.addStretch()

        self._refresh_btn = QPushButton("刷新")
        self._refresh_btn.setFixedWidth(56)
        self._refresh_btn.clicked.connect(self._on_refresh)
        hl.addWidget(self._refresh_btn)

        self._status_lbl = QLabel("就绪")
        self._status_lbl.setStyleSheet("color:#888;font-size:11px;min-width:90px;")
        hl.addWidget(self._status_lbl)

        root.addWidget(toolbar)

        # 指标卡片网格（2 行 × 3 列）
        self._card_total  = _MetricCard("总资产",   _COLOR_TOTAL)
        self._card_cash   = _MetricCard("可用资金", _COLOR_CASH)
        self._card_frozen = _MetricCard("冻结资金", _COLOR_FROZEN)
        self._card_market = _MetricCard("持仓市值", _COLOR_MARKET)
        self._card_avail_ratio = _MetricCard("可用比例",  _COLOR_CASH)
        self._card_frozen_ratio = _MetricCard("冻结比例", _COLOR_FROZEN)

        grid = QGridLayout()
        grid.setSpacing(8)
        grid.addWidget(self._card_total,        0, 0)
        grid.addWidget(self._card_cash,          0, 1)
        grid.addWidget(self._card_frozen,        0, 2)
        grid.addWidget(self._card_market,        1, 0)
        grid.addWidget(self._card_avail_ratio,   1, 1)
        grid.addWidget(self._card_frozen_ratio,  1, 2)
        root.addLayout(grid)

        # 仓位占比进度条
        self._pos_bar = _PositionBar()
        root.addWidget(self._pos_bar)

        root.addStretch(1)

    # ── 槽函数 ────────────────────────────────────────────────────────────────

    def _on_refresh(self) -> None:
        # TODO: 对接真实 API: easy_xt.get_api().trade.get_account_asset(account_id)
        self.update_asset(_DEMO_ASSET.copy())

    # ── 核心渲染逻辑 ──────────────────────────────────────────────────────────

    def _render(self, asset: dict[str, Any]) -> None:
        total   = float(asset.get("total_asset",  0) or 0)
        cash    = float(asset.get("cash",          0) or 0)
        frozen  = float(asset.get("frozen_cash",  0) or 0)
        market  = float(asset.get("market_value", 0) or 0)

        self._card_total.set_value(_fmt_money(total))
        self._card_cash.set_value(_fmt_money(cash))
        self._card_frozen.set_value(_fmt_money(frozen))
        self._card_market.set_value(_fmt_money(market))

        if total > 0:
            avail_pct  = cash   / total * 100.0
            frozen_pct = frozen / total * 100.0
            mkt_pct    = market / total * 100.0
        else:
            avail_pct = frozen_pct = mkt_pct = 0.0

        self._card_avail_ratio.set_value(_fmt_pct(avail_pct))
        self._card_frozen_ratio.set_value(_fmt_pct(frozen_pct))
        self._pos_bar.set_ratio(mkt_pct)

        ts = datetime.datetime.now().strftime("%H:%M:%S")
        self._status_lbl.setText(f"更新 {ts}")
        self._loaded = True

    # ── 公开 API ──────────────────────────────────────────────────────────────

    def update_asset(self, asset: dict[str, Any]) -> None:
        """注入账户资产数据（实时刷新入口）。"""
        self._render(asset)

    def clear_data(self) -> None:
        """重置所有指标为 '--'。"""
        for card in (
            self._card_total, self._card_cash, self._card_frozen,
            self._card_market, self._card_avail_ratio, self._card_frozen_ratio,
        ):
            card.set_value("--")
        self._pos_bar.set_ratio(0.0)
        self._status_lbl.setText("就绪")
        self._loaded = False

    def showEvent(self, a0: Any) -> None:   # type: ignore[override]
        super().showEvent(a0)
        if not self._loaded:
            self._on_refresh()
