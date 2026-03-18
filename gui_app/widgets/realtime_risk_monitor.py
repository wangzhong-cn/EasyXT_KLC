"""
realtime_risk_monitor.py — 实时风控监控面板（R3）
=================================================

独立 QWidget，展示并持续刷新来自 RiskEngine 的关键指标：
  - 净敞口
  - 组合 HHI
  - 日内回撤（相对日内高点）
  - VaR95（基于近期收益率序列）

用法::

    from gui_app.widgets.realtime_risk_monitor import RealtimeRiskMonitor
    from core.risk_engine import RiskEngine

    monitor = RealtimeRiskMonitor()
    monitor.set_engine(RiskEngine())
    monitor.refresh("account_001", {"000001.SZ": 50000}, nav=100000.0)

信号::
    risk_halt(str)  — 当任一指标触发危险级别时发出，携带原因文本。
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

from PyQt5.QtCore import pyqtSignal
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

log = logging.getLogger(__name__)

# 指标定义：(显示名, 指标键, 预警阈值, 危险阈值, 越大越危险)
_METRIC_DEFS: list[tuple[str, str, float, float, bool]] = [
    ("净敞口",   "net_exposure",      0.70, 0.95, True),
    ("HHI",      "hhi",               0.15, 0.30, True),
    ("日内回撤", "intraday_drawdown", 0.03, 0.05, True),
    ("VaR95",    "var95",             0.01, 0.02, True),
]


class RealtimeRiskMonitor(QWidget):
    """
    实时风控指标面板。

    Signals
    -------
    risk_halt : str
        当任一指标达到危险阈值时发出，携带触发原因。
    """

    risk_halt = pyqtSignal(str)

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._engine = None
        self._last_account: str = ""
        self._last_positions: Dict[str, float] = {}
        self._last_nav: float = 0.0
        self._last_returns: Optional[List[float]] = None
        self._build_ui()

    # ------------------------------------------------------------------
    # UI 构建
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(6, 6, 6, 6)
        root.setSpacing(6)

        # 标题行
        header = QHBoxLayout()
        title = QLabel("实时风控监控")
        title.setStyleSheet("font-weight: bold; font-size: 13px;")
        header.addWidget(title)
        header.addStretch()
        self._btn_refresh = QPushButton("刷新")
        self._btn_refresh.setFixedWidth(64)
        self._btn_refresh.clicked.connect(self._on_manual_refresh)
        header.addWidget(self._btn_refresh)
        root.addLayout(header)

        # 账户标签
        self._lbl_account = QLabel("账户: —")
        root.addWidget(self._lbl_account)

        # 指标表格
        grp = QGroupBox("风险指标")
        grp_layout = QVBoxLayout(grp)
        self._table = QTableWidget()
        self._table.setColumnCount(4)
        self._table.setHorizontalHeaderLabels(["指标", "当前值", "预警/危险", "状态"])
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self._table.setRowCount(len(_METRIC_DEFS))
        grp_layout.addWidget(self._table)
        root.addWidget(grp)

        # 状态摘要
        self._lbl_status = QLabel("等待首次刷新…")
        self._lbl_status.setWordWrap(True)
        root.addWidget(self._lbl_status)

        # 初始化行标题和阈值列
        for row, (name, _, warn, danger, _) in enumerate(_METRIC_DEFS):
            self._table.setItem(row, 0, QTableWidgetItem(name))
            self._table.setItem(row, 2, QTableWidgetItem(f"{warn:.0%} / {danger:.0%}"))

    # ------------------------------------------------------------------
    # 公开 API
    # ------------------------------------------------------------------

    def set_engine(self, engine) -> None:
        """注入 ``core.risk_engine.RiskEngine`` 实例。"""
        self._engine = engine

    def refresh(
        self,
        account_id: str,
        positions: Dict[str, float],
        nav: float,
        returns: Optional[List[float]] = None,
    ) -> None:
        """
        使用最新持仓/净值刷新面板。

        Parameters
        ----------
        account_id :
            账户 ID（用于日内回撤跟踪）。
        positions :
            {标的代码: 当前市值} 字典。
        nav :
            账户总净值（含现金）。
        returns :
            近期日收益率序列，用于 VaR95 计算（可为 None）。
        """
        self._last_account = account_id
        self._last_positions = positions
        self._last_nav = nav
        self._last_returns = returns
        self._lbl_account.setText(f"账户: {account_id}")

        if self._engine is None:
            self._lbl_status.setText("⚠ 未注入 RiskEngine，无法计算指标")
            return

        try:
            metrics = self._compute_metrics(account_id, positions, nav, returns)
        except Exception as exc:  # pragma: no cover
            log.exception("风控指标计算异常: %s", exc)
            self._lbl_status.setText(f"计算异常: {exc}")
            return

        self._render_metrics(metrics)

    # ------------------------------------------------------------------
    # 内部计算
    # ------------------------------------------------------------------

    def _compute_metrics(
        self,
        account_id: str,
        positions: Dict[str, float],
        nav: float,
        returns: Optional[List[float]],
    ) -> Dict[str, float]:
        engine = self._engine
        return {
            "net_exposure": engine.get_net_exposure(positions, nav),
            "hhi": engine.get_hhi(positions, nav),
            "intraday_drawdown": engine.get_intraday_drawdown(account_id, nav),
            "var95": engine.calc_var95(returns) if returns else 0.0,
        }

    def _render_metrics(self, metrics: Dict[str, float]) -> None:
        halt_reasons: list[str] = []
        normal_count = 0

        for row, (name, key, warn, danger, big_bad) in enumerate(_METRIC_DEFS):
            value = metrics.get(key, 0.0)
            disp = f"{value:.2%}"

            if big_bad:
                if value >= danger:
                    status, color = "🔴 危险", "#F44336"
                    halt_reasons.append(f"{name}={disp}")
                elif value >= warn:
                    status, color = "🟡 预警", "#FF9800"
                else:
                    status, color = "🟢 正常", "#4CAF50"
                    normal_count += 1
            else:
                if value <= danger:
                    status, color = "🔴 偏低", "#F44336"
                    halt_reasons.append(f"{name}={disp}")
                elif value <= warn:
                    status, color = "🟡 一般", "#FF9800"
                else:
                    status, color = "🟢 良好", "#4CAF50"
                    normal_count += 1

            self._table.setItem(row, 1, QTableWidgetItem(disp))
            status_item = QTableWidgetItem(status)
            status_item.setForeground(QColor(color))
            self._table.setItem(row, 3, status_item)

        if halt_reasons:
            summary = "🔴 HALT 触发: " + "；".join(halt_reasons)
            self._lbl_status.setText(summary)
            self.risk_halt.emit(summary)
        else:
            self._lbl_status.setText(
                f"🟢 风控正常 ({normal_count}/{len(_METRIC_DEFS)} 指标达标)"
            )

    def _on_manual_refresh(self) -> None:
        if self._last_account:
            self.refresh(
                self._last_account,
                self._last_positions,
                self._last_nav,
                self._last_returns,
            )
        else:
            self._lbl_status.setText("请先通过 refresh() 传入账户数据")

    # ------------------------------------------------------------------
    # 便捷属性（供外部读取最新状态）
    # ------------------------------------------------------------------

    def get_last_metrics(self) -> Dict[str, float]:
        """返回最近一次计算的指标字典（无缓存时返回空 dict）。"""
        if self._engine is None or not self._last_account:
            return {}
        try:
            return self._compute_metrics(
                self._last_account,
                self._last_positions,
                self._last_nav,
                self._last_returns,
            )
        except Exception:
            return {}
