"""
strategy_governance_panel.py — 策略管理全量 UI 面板
====================================================

8 个功能 Tab，覆盖策略管理全流程：

    Tab 0  策略列表     _StrategyListTab      — CRUD + 状态管理
    Tab 1  回测配置     _BacktestConfigTab    — 参数/标的/区间/成本
    Tab 2  回测结果     _BacktestResultTab    — 资金曲线 + 指标卡 + 交易明细
    Tab 3  绩效分析     _PerformanceTab       — Sharpe/CAGR/Calmar/WinRate
    Tab 4  风险报告     _RiskTab              — MaxDD / VaR / 风控指标
    Tab 5  参数优化     _OptimizationTab      — 网格搜索 + 结果排名
    Tab 6  策略对比     _ComparisonTab        — 多策略绩效对比
    Tab 7  生命周期     _LifecycleTab         — 状态流转 / 版本历史

所有 IO 操作通过 StrategyController；回测通过 QThread 异步执行。
matplotlib 缺失时退化为纯文本占位符（不影响功能）。
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from PyQt5.QtCore import QDate, Qt, QThread, QTimer, pyqtSignal
from PyQt5.QtGui import QColor, QFont
from PyQt5.QtWidgets import (
    QComboBox,
    QDateEdit,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

log = logging.getLogger(__name__)

# ─── matplotlib 可选依赖 ───────────────────────────────────────────────────
_MATPLOTLIB_OK = False
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure
    _MATPLOTLIB_OK = True
except Exception:
    pass


# ─── 异步回测线程 ──────────────────────────────────────────────────────────


class _BacktestThread(QThread):
    """在后台线程运行回测，完成后通过信号推送结果。"""

    result_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)
    status_updated = pyqtSignal(str)

    def __init__(self, controller: Any, strategy_id: str, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._controller = controller
        self._strategy_id = strategy_id

    def run(self) -> None:
        try:
            self.status_updated.emit("⏳ 正在执行回测...")
            result = self._controller.run_backtest(self._strategy_id)
            self.result_ready.emit(result)
        except Exception as exc:
            self.error_occurred.emit(str(exc))


# ─── 公共辅助：指标卡 ──────────────────────────────────────────────────────


class _MetricCard(QFrame):
    """单个绩效指标卡片：标题 + 值 + 颜色标注。"""

    def __init__(self, label: str = "", value: str = "—", color: str = "#555555", parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.StyledPanel)
        self.setMinimumWidth(110)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(2)

        self._label = QLabel(label)
        self._label.setAlignment(Qt.AlignCenter)
        lf = QFont()
        lf.setPointSize(9)
        self._label.setFont(lf)
        self._label.setStyleSheet("color: #777777;")

        self._value = QLabel(value)
        self._value.setAlignment(Qt.AlignCenter)
        vf = QFont()
        vf.setPointSize(13)
        vf.setBold(True)
        self._value.setFont(vf)
        self._value.setStyleSheet(f"color: {color};")

        layout.addWidget(self._label)
        layout.addWidget(self._value)

    def update_metric(self, value: str, color: str = "#555555") -> None:
        self._value.setText(value)
        self._value.setStyleSheet(f"color: {color};")


def _build_metric_row(metrics: list[dict[str, Any]]) -> QWidget:
    """将绩效指标列表渲染为横向卡片行。"""
    w = QWidget()
    layout = QHBoxLayout(w)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(6)
    for m in metrics:
        card = _MetricCard(
            label=m.get("label", ""),
            value=m.get("value", "—"),
            color=m.get("color", "#555555"),
        )
        layout.addWidget(card)
    layout.addStretch()
    return w


# ─── 公共辅助：净值曲线图（可降级） ──────────────────────────────────────


class _EquityChart(QWidget):
    """轻量资金曲线图。matplotlib 缺失时退化为纯文本。"""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        if _MATPLOTLIB_OK:
            self._figure = Figure(figsize=(8, 3), dpi=100)
            self._canvas = FigureCanvas(self._figure)
            self._ax = self._figure.add_subplot(111)
            self._figure.subplots_adjust(left=0.07, right=0.98, top=0.88, bottom=0.14)
            layout.addWidget(self._canvas)
        else:
            self._text = QLabel("净值曲线（需安装 matplotlib）")
            self._text.setAlignment(Qt.AlignCenter)
            self._text.setStyleSheet(
                "background:#f5f5f5;border:2px dashed #ccc;border-radius:6px;padding:20px;color:#888;"
            )
            layout.addWidget(self._text)

    def plot(self, dates: list, values: list, title: str = "资金曲线") -> None:
        if not _MATPLOTLIB_OK:
            if dates and values:
                start = dates[0] if dates else ""
                end = dates[-1] if dates else ""
                final = values[-1] if values else 0
                initial = values[0] if values else 1
                ret = (final / initial - 1) * 100 if initial else 0
                self._text.setText(
                    f"净值曲线\n{start} → {end}\n"
                    f"起始: {initial:,.0f}  终值: {final:,.0f}  收益: {ret:+.2f}%"
                )
            return
        if not dates or not values:
            return
        try:
            import matplotlib.dates as mdates
            import pandas as pd

            self._ax.clear()
            ts = pd.to_datetime(dates, errors="coerce")
            vals = [float(v) for v in values]
            init = vals[0] if vals[0] != 0 else 1.0
            net = [v / init for v in vals]
            self._ax.plot(ts, net, "b-", linewidth=1.8, label="净值")
            self._ax.axhline(y=1.0, color="r", linestyle="--", alpha=0.6, lw=1)
            import numpy as np
            running_max = np.maximum.accumulate(net)
            self._ax.fill_between(ts, net, running_max, where=[r > n for r, n in zip(running_max, net)],
                                   color="#64b5f6", alpha=0.25, label="回撤区间")
            self._ax.set_title(title, fontsize=11, fontweight="bold")
            self._ax.set_xlabel("日期", fontsize=9)
            self._ax.set_ylabel("净值", fontsize=9)
            self._ax.grid(True, alpha=0.3)
            locator = mdates.AutoDateLocator(minticks=4, maxticks=8)
            self._ax.xaxis.set_major_locator(locator)
            self._ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
            self._figure.subplots_adjust(left=0.07, right=0.98, top=0.88, bottom=0.14)
            self._canvas.draw()
        except Exception as exc:
            log.debug("_EquityChart.plot failed: %s", exc)


# ─── Tab 0: 策略列表 ───────────────────────────────────────────────────────


class _StrategyListTab(QWidget):
    """策略 CRUD 操作界面。"""

    backtest_requested = pyqtSignal(str)  # strategy_id

    def __init__(self, controller: Any, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._build_ui()
        self.refresh()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        # 操作栏
        btn_row = QHBoxLayout()
        self._btn_create = QPushButton("➕ 创建策略")
        self._btn_backtest = QPushButton("▶ 执行回测")
        self._btn_delete = QPushButton("🗑 删除策略")
        self._btn_refresh = QPushButton("↻ 刷新")
        for b in (self._btn_create, self._btn_backtest, self._btn_delete, self._btn_refresh):
            btn_row.addWidget(b)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        # 表格
        self._table = QTableWidget()
        self._table.setColumnCount(7)
        self._table.setHorizontalHeaderLabels(
            ["策略ID", "策略名称", "策略类型", "数据周期", "标的数", "版本", "创建时间"]
        )
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._table)

        # 信号连接
        self._btn_create.clicked.connect(self._on_create)
        self._btn_backtest.clicked.connect(self._on_backtest)
        self._btn_delete.clicked.connect(self._on_delete)
        self._btn_refresh.clicked.connect(self.refresh)

    def refresh(self) -> None:
        strategies = self._ctrl.get_all_strategies()
        self._table.setRowCount(len(strategies))
        for row, s in enumerate(strategies):
            self._table.setItem(row, 0, QTableWidgetItem(str(s.get("strategy_id", ""))))
            self._table.setItem(row, 1, QTableWidgetItem(str(s.get("strategy_name", ""))))
            self._table.setItem(row, 2, QTableWidgetItem(str(s.get("strategy_type", ""))))
            self._table.setItem(row, 3, QTableWidgetItem(str(s.get("period", ""))))
            self._table.setItem(row, 4, QTableWidgetItem(str(s.get("symbols_count", ""))))
            self._table.setItem(row, 5, QTableWidgetItem(str(s.get("version", 1))))
            self._table.setItem(row, 6, QTableWidgetItem(str(s.get("created_at", ""))[:19]))

    def _selected_id(self) -> Optional[str]:
        rows = self._table.selectedItems()
        if not rows:
            return None
        return self._table.item(rows[0].row(), 0).text()

    def _on_create(self) -> None:
        from gui_app.widgets.strategy_governance_panel import _StrategyCreationDialog
        dlg = _StrategyCreationDialog(self._ctrl, self)
        if dlg.exec_() == QDialog.Accepted:
            data = dlg.get_config_data()
            res = self._ctrl.create_strategy(data)
            if res.get("ok"):
                QMessageBox.information(self, "成功", f"策略创建成功\nID: {res['strategy_id']}")
                self.refresh()
            else:
                QMessageBox.critical(self, "创建失败", res.get("error", "未知错误"))

    def _on_backtest(self) -> None:
        sid = self._selected_id()
        if not sid:
            QMessageBox.warning(self, "提示", "请先选择一个策略")
            return
        self.backtest_requested.emit(sid)

    def _on_delete(self) -> None:
        sid = self._selected_id()
        if not sid:
            QMessageBox.warning(self, "提示", "请先选择一个策略")
            return
        name_item = self._table.item(self._table.selectedItems()[0].row(), 1)
        name = name_item.text() if name_item else sid
        reply = QMessageBox.question(
            self, "确认删除", f"确定删除策略 '{name}'？此操作不可恢复。",
            QMessageBox.Yes | QMessageBox.No,
        )
        if reply == QMessageBox.Yes:
            res = self._ctrl.delete_strategy(sid)
            if res.get("ok"):
                self.refresh()
            else:
                QMessageBox.critical(self, "删除失败", res.get("error", "未知错误"))


# ─── 策略创建对话框 ────────────────────────────────────────────────────────


class _StrategyCreationDialog(QDialog):
    """策略创建 / 编辑对话框。"""

    def __init__(self, controller: Any, parent=None):
        super().__init__(parent)
        self.setWindowTitle("创建新策略")
        self.setModal(True)
        self.resize(620, 560)
        self._ctrl = controller
        self._build_ui()

    def _build_ui(self) -> None:
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        content = QWidget()
        scroll.setWidget(content)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(scroll)

        layout = QVBoxLayout(content)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        # 基础信息
        grp_basic = QGroupBox("基础信息")
        fl = QFormLayout(grp_basic)
        self._name_edit = QLineEdit()
        self._type_combo = QComboBox()
        for label, _ in self._ctrl.strategy_type_options():
            self._type_combo.addItem(label)
        self._base_combo = QComboBox()
        self._base_combo.addItems(self._ctrl.base_strategy_options())
        self._period_combo = QComboBox()
        self._period_combo.addItems(self._ctrl.period_options())
        fl.addRow("策略名称 *:", self._name_edit)
        fl.addRow("策略类型:", self._type_combo)
        fl.addRow("基础策略:", self._base_combo)
        fl.addRow("数据周期:", self._period_combo)
        layout.addWidget(grp_basic)

        # 参数
        grp_param = QGroupBox("策略参数")
        fl2 = QFormLayout(grp_param)
        self._fast_spin = QSpinBox()
        self._fast_spin.setRange(1, 200)
        self._fast_spin.setValue(5)
        self._slow_spin = QSpinBox()
        self._slow_spin.setRange(1, 500)
        self._slow_spin.setValue(20)
        self._adjust_combo = QComboBox()
        self._adjust_combo.addItems(["前复权", "后复权", "不复权"])
        fl2.addRow("快速均线周期:", self._fast_spin)
        fl2.addRow("慢速均线周期:", self._slow_spin)
        fl2.addRow("复权方式:", self._adjust_combo)
        layout.addWidget(grp_param)

        # 风控
        grp_risk = QGroupBox("风险控制")
        fl3 = QFormLayout(grp_risk)
        self._max_pos_spin = QDoubleSpinBox()
        self._max_pos_spin.setRange(0.01, 0.20)
        self._max_pos_spin.setValue(0.20)
        self._max_pos_spin.setSingleStep(0.05)
        self._stop_spin = QDoubleSpinBox()
        self._stop_spin.setRange(0.001, 0.03)
        self._stop_spin.setValue(0.02)
        self._stop_spin.setSingleStep(0.005)
        self._dd_spin = QDoubleSpinBox()
        self._dd_spin.setRange(0.01, 0.15)
        self._dd_spin.setValue(0.10)
        self._dd_spin.setSingleStep(0.02)
        fl3.addRow("最大仓位 (≤20%):", self._max_pos_spin)
        fl3.addRow("单日止损 (≤3%):", self._stop_spin)
        fl3.addRow("最大回撤限制 (≤15%):", self._dd_spin)
        layout.addWidget(grp_risk)

        # 标的
        grp_sym = QGroupBox("交易标的")
        sl = QVBoxLayout(grp_sym)
        self._symbols_edit = QTextEdit()
        self._symbols_edit.setPlaceholderText("每行一个股票代码，例如：\n000001.SZ\n600000.SH")
        self._symbols_edit.setMaximumHeight(90)
        sl.addWidget(self._symbols_edit)
        layout.addWidget(grp_sym)

        # 回测区间
        grp_bt = QGroupBox("回测区间")
        fl4 = QFormLayout(grp_bt)
        self._start_date = QDateEdit()
        self._start_date.setCalendarPopup(True)
        self._start_date.setDate(QDate.currentDate().addYears(-3))
        self._end_date = QDateEdit()
        self._end_date.setCalendarPopup(True)
        self._end_date.setDate(QDate.currentDate())
        fl4.addRow("开始日期:", self._start_date)
        fl4.addRow("结束日期:", self._end_date)
        layout.addWidget(grp_bt)

        # 交易成本
        grp_cost = QGroupBox("交易成本")
        fl5 = QFormLayout(grp_cost)
        self._comm_spin = QDoubleSpinBox()
        self._comm_spin.setDecimals(6)
        self._comm_spin.setRange(0, 0.01)
        self._comm_spin.setValue(0.0003)
        self._tax_spin = QDoubleSpinBox()
        self._tax_spin.setDecimals(6)
        self._tax_spin.setRange(0, 0.01)
        self._tax_spin.setValue(0.001)
        self._slip_spin = QDoubleSpinBox()
        self._slip_spin.setDecimals(2)
        self._slip_spin.setRange(0, 50)
        self._slip_spin.setValue(2.0)
        fl5.addRow("手续费率:", self._comm_spin)
        fl5.addRow("印花税率:", self._tax_spin)
        fl5.addRow("滑点 (bps):", self._slip_spin)
        layout.addWidget(grp_cost)

        # 按钮
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self._validate_and_accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _validate_and_accept(self) -> None:
        if not self._name_edit.text().strip():
            QMessageBox.warning(self, "提示", "策略名称不能为空")
            return
        if not self._symbols_edit.toPlainText().strip():
            QMessageBox.warning(self, "提示", "请至少输入一个股票代码")
            return
        self.accept()

    def get_config_data(self) -> dict[str, Any]:
        type_map = {label: val for label, val in self._ctrl.strategy_type_options()}
        adjust_map = {"前复权": "front", "后复权": "back", "不复权": "none"}
        symbols = [
            s.strip()
            for s in self._symbols_edit.toPlainText().split("\n")
            if s.strip()
        ]
        return {
            "strategy_name": self._name_edit.text().strip(),
            "strategy_type": type_map.get(self._type_combo.currentText(), "trend"),
            "base_strategy": self._base_combo.currentText(),
            "period": self._period_combo.currentText(),
            "parameters": {
                "fast_period": self._fast_spin.value(),
                "slow_period": self._slow_spin.value(),
                "adjust": adjust_map.get(self._adjust_combo.currentText(), "none"),
            },
            "risk_controls": {
                "max_position": self._max_pos_spin.value(),
                "daily_stop_loss": self._stop_spin.value(),
                "max_drawdown": self._dd_spin.value(),
            },
            "symbols": symbols,
            "backtest_range": {
                "start": self._start_date.date().toString("yyyy-MM-dd"),
                "end": self._end_date.date().toString("yyyy-MM-dd"),
            },
            "trading_cost": {
                "commission": self._comm_spin.value(),
                "tax": self._tax_spin.value(),
                "slippage_bps": self._slip_spin.value(),
            },
        }


# ─── Tab 1: 回测配置 ───────────────────────────────────────────────────────


class _BacktestConfigTab(QWidget):
    """显示/编辑当前选中策略的回测配置，点击「执行」触发异步回测。"""

    run_requested = pyqtSignal(str)  # strategy_id

    def __init__(self, controller: Any, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._current_sid: Optional[str] = None
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        # 策略选择
        sel_row = QHBoxLayout()
        sel_row.addWidget(QLabel("当前策略:"))
        self._sid_label = QLabel("（未选择）")
        self._sid_label.setStyleSheet("color:#1976D2;font-weight:bold;")
        sel_row.addWidget(self._sid_label)
        sel_row.addStretch()
        self._run_btn = QPushButton("▶ 立即执行回测")
        self._run_btn.setStyleSheet("background:#388E3C;color:white;padding:4px 16px;border-radius:4px;")
        self._run_btn.clicked.connect(self._on_run)
        sel_row.addWidget(self._run_btn)
        layout.addLayout(sel_row)

        # 配置展示
        self._config_text = QTextEdit()
        self._config_text.setReadOnly(True)
        self._config_text.setFont(QFont("Consolas", 10))
        self._config_text.setPlaceholderText("在「策略列表」选中一条策略后点击「执行回测」，此处将显示配置详情。")
        layout.addWidget(self._config_text)

        # 进度
        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        self._status_label = QLabel()
        layout.addWidget(self._progress)
        layout.addWidget(self._status_label)

    def load_strategy(self, strategy_id: str) -> None:
        """由外部设置当前strategy_id并显示配置。"""
        self._current_sid = strategy_id
        cfg = self._ctrl.get_strategy(strategy_id)
        if cfg:
            self._sid_label.setText(f"{cfg.get('strategy_name', '')} ({strategy_id})")
            import json as _json
            self._config_text.setText(_json.dumps(cfg, ensure_ascii=False, indent=2))
        else:
            self._sid_label.setText(strategy_id)
            self._config_text.setText("（加载配置失败）")

    def set_running(self, running: bool) -> None:
        self._progress.setVisible(running)
        self._run_btn.setEnabled(not running)

    def set_status(self, msg: str) -> None:
        self._status_label.setText(msg)

    def _on_run(self) -> None:
        if not self._current_sid:
            QMessageBox.warning(self, "提示", "请先在「策略列表」选择一个策略")
            return
        self.run_requested.emit(self._current_sid)


# ─── Tab 2: 回测结果 ───────────────────────────────────────────────────────


class _BacktestResultTab(QWidget):
    """资金曲线 + 绩效指标卡 + 交易明细。"""

    def __init__(self, controller: Any, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        # 骨架：上方曲线 + 指标卡，下方交易表
        splitter = QSplitter(Qt.Vertical)

        # 上半段
        top = QWidget()
        top_layout = QVBoxLayout(top)
        top_layout.setContentsMargins(0, 0, 0, 0)
        self._chart = _EquityChart()
        self._chart.setMinimumHeight(220)
        top_layout.addWidget(self._chart)
        self._metric_container = QWidget()
        self._metric_layout = QHBoxLayout(self._metric_container)
        self._metric_layout.setContentsMargins(0, 0, 0, 0)
        top_layout.addWidget(self._metric_container)
        splitter.addWidget(top)

        # 下半段：交易明细
        bottom = QWidget()
        btm_layout = QVBoxLayout(bottom)
        btm_layout.setContentsMargins(0, 0, 0, 0)
        btm_layout.addWidget(QLabel("交易明细"))
        self._trade_table = QTableWidget()
        self._trade_table.setColumnCount(6)
        self._trade_table.setHorizontalHeaderLabels(["日期", "方向", "价格", "数量", "成交额", "盈亏"])
        self._trade_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._trade_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self._trade_table.horizontalHeader().setStretchLastSection(True)
        btm_layout.addWidget(self._trade_table)
        splitter.addWidget(bottom)

        layout.addWidget(splitter)

        self._placeholder = QLabel("执行回测后，此处显示结果。")
        self._placeholder.setAlignment(Qt.AlignCenter)
        self._placeholder.setStyleSheet("color:#888;font-size:14px;")
        layout.addWidget(self._placeholder)

    def load_result(self, result: dict[str, Any]) -> None:
        """将 run_backtest 返回的 dict 渲染到界面。"""
        self._placeholder.setVisible(False)

        # 资金曲线
        ec = result.get("equity_curve", {})
        dates = ec.get("dates", [])
        values = ec.get("values", [])
        if dates and values:
            self._chart.plot(dates, values, title="策略资金曲线")

        # 绩效指标卡
        perf = result.get("performance_metrics", {})
        metrics = self._ctrl.get_performance_summary(perf)
        # 清旧卡
        while self._metric_layout.count():
            item = self._metric_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        for m in metrics:
            card = _MetricCard(
                label=m.get("label", ""),
                value=m.get("value", "—"),
                color=m.get("color", "#555555"),
            )
            self._metric_layout.addWidget(card)
        self._metric_layout.addStretch()

        # 交易明细
        trades = result.get("trades", [])
        self._trade_table.setRowCount(len(trades))
        for row, t in enumerate(trades):
            if isinstance(t, dict):
                self._trade_table.setItem(row, 0, QTableWidgetItem(str(t.get("date", ""))))
                self._trade_table.setItem(row, 1, QTableWidgetItem(str(t.get("action", t.get("direction", "")))))
                self._trade_table.setItem(row, 2, QTableWidgetItem(f"{t.get('price', 0):.4f}"))
                self._trade_table.setItem(row, 3, QTableWidgetItem(str(t.get("volume", 0))))
                self._trade_table.setItem(row, 4, QTableWidgetItem(f"{t.get('value', 0):,.2f}"))
                pnl = t.get("pnl", 0)
                pnl_item = QTableWidgetItem(f"{pnl:,.2f}")
                pnl_item.setForeground(QColor("#4CAF50") if float(pnl or 0) >= 0 else QColor("#F44336"))
                self._trade_table.setItem(row, 5, pnl_item)


# ─── Tab 3: 绩效分析 ───────────────────────────────────────────────────────


class _PerformanceTab(QWidget):
    """详细绩效分析：指标卡 + 文字说明。"""

    def __init__(self, controller: Any, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        lbl = QLabel("绩效分析（执行回测后自动更新）")
        lbl.setStyleSheet("font-weight:bold;font-size:13px;")
        layout.addWidget(lbl)

        # 指标卡区域
        self._cards_widget = QWidget()
        self._cards_layout = QHBoxLayout(self._cards_widget)
        self._cards_layout.setContentsMargins(0, 4, 0, 4)
        layout.addWidget(self._cards_widget)

        # 月度收益表
        layout.addWidget(QLabel("月度绩效概览"))
        self._monthly_table = QTableWidget()
        self._monthly_table.setColumnCount(5)
        self._monthly_table.setHorizontalHeaderLabels(["指标", "值", "行业均值*", "评级", "说明"])
        self._monthly_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._monthly_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self._monthly_table)

        self._placeholder = QLabel("执行回测后，此处显示详细绩效分析。")
        self._placeholder.setAlignment(Qt.AlignCenter)
        self._placeholder.setStyleSheet("color:#888;font-size:14px;")
        layout.addWidget(self._placeholder)

    def load_metrics(self, performance_metrics: dict[str, Any]) -> None:
        self._placeholder.setVisible(False)
        metrics = self._ctrl.get_performance_summary(performance_metrics)

        # 更新卡片
        while self._cards_layout.count():
            item = self._cards_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        for m in metrics:
            card = _MetricCard(
                label=m.get("label", ""),
                value=m.get("value", "—"),
                color=m.get("color", "#555555"),
            )
            self._cards_layout.addWidget(card)
        self._cards_layout.addStretch()

        # 指标评级表
        RATINGS = {
            "sharpe_ratio": [("优秀", 2.0, "#4CAF50"), ("良好", 1.0, "#8BC34A"), ("一般", 0.0, "#FF9800"), ("差", None, "#F44336")],
            "max_drawdown": [("优秀", None, "#4CAF50"), ("良好", 0.05, "#8BC34A"), ("一般", 0.10, "#FF9800"), ("差", 0.20, "#F44336")],
            "win_rate": [("优秀", 0.60, "#4CAF50"), ("良好", 0.50, "#8BC34A"), ("一般", 0.40, "#FF9800"), ("差", None, "#F44336")],
        }
        INDUSTRY_BENCH = {
            "sharpe_ratio": "1.20",
            "max_drawdown": "15%",
            "win_rate": "52%",
        }
        EXPLANATIONS = {
            "总收益率": "回测期间总收益",
            "年化收益(CAGR)": "年化复合增长率",
            "夏普比率": ">1 优；>2 极优",
            "最大回撤": "越小越好；策略风险",
            "Calmar 比率": "年化收益/最大回撤",
            "胜率": "盈利交易比例",
            "交易次数": "回测期内总交易次数",
            "盈亏比": "平均盈利/平均亏损",
        }
        rows = []
        for m in metrics:
            key = m.get("key", "")
            val = m.get("value", "—")
            bench = INDUSTRY_BENCH.get(key, "—")
            explanation = EXPLANATIONS.get(m.get("label", ""), "—")
            rating, rating_color = "—", "#555555"
            rows.append((m.get("label", ""), val, bench, rating, explanation, rating_color))

        self._monthly_table.setRowCount(len(rows))
        for i, (label, val, bench, rating, expl, _rc) in enumerate(rows):
            self._monthly_table.setItem(i, 0, QTableWidgetItem(label))
            self._monthly_table.setItem(i, 1, QTableWidgetItem(val))
            self._monthly_table.setItem(i, 2, QTableWidgetItem(bench))
            self._monthly_table.setItem(i, 3, QTableWidgetItem(rating))
            self._monthly_table.setItem(i, 4, QTableWidgetItem(expl))


# ─── Tab 4: 风险报告 ───────────────────────────────────────────────────────


class _RiskTab(QWidget):
    """风险指标展示面板。"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        layout.addWidget(QLabel("风险报告（执行回测后自动更新）"))

        self._table = QTableWidget()
        self._table.setColumnCount(3)
        self._table.setHorizontalHeaderLabels(["风险指标", "当前值", "风险状态"])
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self._table)

        self._text = QTextEdit()
        self._text.setReadOnly(True)
        self._text.setMaximumHeight(120)
        self._text.setPlaceholderText("风险分析摘要将在此处显示...")
        layout.addWidget(self._text)

    def load_risk(self, performance_metrics: dict[str, Any]) -> None:
        RISK_KEYS = [
            ("最大回撤", "max_drawdown", True, 0.10, 0.15),
            ("夏普比率", "sharpe_ratio", False, 1.0, 0.5),
            ("年化波动率", "annual_volatility", True, 0.20, 0.30),
            ("盈亏比", "profit_loss_ratio", False, 1.5, 1.0),
            ("胜率", "win_rate", False, 0.50, 0.40),
            ("Calmar比率", "calmar_ratio", False, 0.5, 0.3),
        ]
        rows = []
        for label, key, lower_bad, warn_threshold, danger_threshold in RISK_KEYS:
            raw = performance_metrics.get(key, None)
            if raw is None:
                continue
            val = float(raw)
            if lower_bad:
                status = "🔴 警戒" if val > danger_threshold else ("🟡 注意" if val > warn_threshold else "🟢 正常")
                disp = f"{val * 100:.2f}%" if key in ("max_drawdown", "annual_volatility", "win_rate") else f"{val:.4f}"
            else:
                status = "🔴 偏低" if val < danger_threshold else ("🟡 一般" if val < warn_threshold else "🟢 良好")
                disp = f"{val:.4f}"
            rows.append((label, disp, status))

        self._table.setRowCount(len(rows))
        for i, (label, disp, status) in enumerate(rows):
            self._table.setItem(i, 0, QTableWidgetItem(label))
            self._table.setItem(i, 1, QTableWidgetItem(disp))
            status_item = QTableWidgetItem(status)
            color = "#4CAF50" if "正常" in status or "良好" in status else ("#FF9800" if "注意" in status or "一般" in status else "#F44336")
            status_item.setForeground(QColor(color))
            self._table.setItem(i, 2, status_item)

        # 生成文字摘要
        lines = ["风险评估摘要:"]
        for label, disp, status in rows:
            lines.append(f"  {label}: {disp}  {status}")
        self._text.setText("\n".join(lines))


# ─── Tab 5: 参数优化 ───────────────────────────────────────────────────────


class _OptimizationTab(QWidget):
    """参数优化面板：说明优化方法与结果排名。"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.addWidget(QLabel("参数优化说明"))

        info = QLabel(
            "参数优化通过「回测分析」Tab 中「优化」功能执行（BacktestWidget 已内置网格搜索/Walk-Forward）。\n"
            "此面板展示优化历史结果与最优参数建议。"
        )
        info.setWordWrap(True)
        info.setStyleSheet("color:#666;font-size:12px;padding:8px;background:#f9f9f9;border-radius:4px;")
        layout.addWidget(info)

        layout.addWidget(QLabel("参数优化结果历史"))
        self._table = QTableWidget()
        self._table.setColumnCount(6)
        self._table.setHorizontalHeaderLabels(["参数方案", "总收益", "夏普比率", "最大回撤", "综合得分", "状态"])
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self._table)

        # 最优参数展示
        grp = QGroupBox("最优参数建议")
        grp_layout = QVBoxLayout(grp)
        self._best_text = QTextEdit()
        self._best_text.setReadOnly(True)
        self._best_text.setMaximumHeight(100)
        self._best_text.setPlaceholderText("执行参数优化后，最优参数将在此处展示...")
        grp_layout.addWidget(self._best_text)
        layout.addWidget(grp)

    def load_optimization(self, opt_results: list[dict[str, Any]]) -> None:
        if not opt_results:
            return
        sorted_res = sorted(opt_results, key=lambda x: x.get("score", 0), reverse=True)
        self._table.setRowCount(len(sorted_res))
        for i, r in enumerate(sorted_res):
            params = str(r.get("params", ""))
            self._table.setItem(i, 0, QTableWidgetItem(params))
            self._table.setItem(i, 1, QTableWidgetItem(f"{r.get('total_return', 0):.2%}"))
            self._table.setItem(i, 2, QTableWidgetItem(f"{r.get('sharpe_ratio', 0):.4f}"))
            self._table.setItem(i, 3, QTableWidgetItem(f"{r.get('max_drawdown', 0):.2%}"))
            self._table.setItem(i, 4, QTableWidgetItem(f"{r.get('score', 0):.4f}"))
            self._table.setItem(i, 5, QTableWidgetItem("🥇 最优" if i == 0 else ""))

        if sorted_res:
            best = sorted_res[0]
            import json as _json
            self._best_text.setText(_json.dumps(best.get("params", {}), ensure_ascii=False, indent=2))


# ─── Tab 6: 策略对比 ───────────────────────────────────────────────────────


class _ComparisonTab(QWidget):
    """多策略回测结果对比。"""

    def __init__(self, controller: Any, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._compare_data: list[dict[str, Any]] = []
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        bar = QHBoxLayout()
        bar.addWidget(QLabel("策略对比（可从回测历史中添加对比项）"))
        self._btn_clear = QPushButton("清空对比")
        self._btn_clear.clicked.connect(self._clear)
        bar.addStretch()
        bar.addWidget(self._btn_clear)
        layout.addLayout(bar)

        self._table = QTableWidget()
        self._table.setColumnCount(7)
        self._table.setHorizontalHeaderLabels([
            "策略名称", "回测ID", "总收益", "年化收益", "夏普比率", "最大回撤", "胜率"
        ])
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self._table)

        self._chart = _EquityChart()
        self._chart.setMinimumHeight(200)
        layout.addWidget(self._chart)

    def add_result(self, strategy_name: str, result: dict[str, Any]) -> None:
        """向对比列表添加一条回测结果。"""
        self._compare_data.append({"name": strategy_name, "result": result})
        self._refresh_table()

    def _refresh_table(self) -> None:
        self._table.setRowCount(len(self._compare_data))
        for i, item in enumerate(self._compare_data):
            name = item.get("name", "")
            result = item.get("result", {})
            perf = result.get("performance_metrics", {})
            self._table.setItem(i, 0, QTableWidgetItem(name))
            self._table.setItem(i, 1, QTableWidgetItem(str(result.get("backtest_id", ""))[:12]))
            self._table.setItem(i, 2, QTableWidgetItem(f"{perf.get('total_return', 0):.2%}"))
            cagr = perf.get("cagr", perf.get("annualized_return", 0))
            self._table.setItem(i, 3, QTableWidgetItem(f"{cagr:.2%}"))
            self._table.setItem(i, 4, QTableWidgetItem(f"{perf.get('sharpe_ratio', 0):.4f}"))
            self._table.setItem(i, 5, QTableWidgetItem(f"{perf.get('max_drawdown', 0):.2%}"))
            self._table.setItem(i, 6, QTableWidgetItem(f"{perf.get('win_rate', 0):.2%}"))

    def _clear(self) -> None:
        self._compare_data.clear()
        self._table.setRowCount(0)


# ─── Tab 7: 生命周期管理 ───────────────────────────────────────────────────


class _LifecycleTab(QWidget):
    """策略生命周期：状态流转 + 版本历史 + 回测历史记录。"""

    def __init__(self, controller: Any, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._current_sid: Optional[str] = None
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        # 状态流转说明
        grp_status = QGroupBox("生命周期状态概览")
        status_layout = QHBoxLayout(grp_status)
        for state, color in [("草稿", "#9E9E9E"), ("测试中", "#FF9800"), ("已激活", "#4CAF50"), ("已归档", "#607D8B")]:
            lbl = QLabel(state)
            lbl.setAlignment(Qt.AlignCenter)
            lbl.setStyleSheet(
                f"background:{color};color:white;padding:4px 12px;"
                "border-radius:4px;font-weight:bold;"
            )
            status_layout.addWidget(lbl)
        layout.addWidget(grp_status)

        # 回测历史
        layout.addWidget(QLabel("回测历史记录"))
        self._history_table = QTableWidget()
        self._history_table.setColumnCount(5)
        self._history_table.setHorizontalHeaderLabels(
            ["回测ID", "创建时间", "总收益", "夏普比率", "最大回撤"]
        )
        self._history_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._history_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self._history_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._history_table)

        self._placeholder = QLabel("选择策略后查看历史记录。")
        self._placeholder.setAlignment(Qt.AlignCenter)
        self._placeholder.setStyleSheet("color:#888;font-size:13px;")
        layout.addWidget(self._placeholder)

    def load_history(self, strategy_id: str) -> None:
        self._current_sid = strategy_id
        history = self._ctrl.get_backtest_history(strategy_id)
        if not history:
            self._placeholder.setText(f"策略 {strategy_id} 暂无历史回测记录。")
            self._history_table.setRowCount(0)
            return
        self._placeholder.setVisible(False)
        self._history_table.setRowCount(len(history))
        for row, h in enumerate(history):
            self._history_table.setItem(row, 0, QTableWidgetItem(str(h.get("backtest_id", ""))[:20]))
            self._history_table.setItem(row, 1, QTableWidgetItem(str(h.get("created_at", ""))[:19]))
            self._history_table.setItem(row, 2, QTableWidgetItem(f"{h.get('total_return', 0):.2%}"))
            self._history_table.setItem(row, 3, QTableWidgetItem(f"{h.get('sharpe_ratio', 0):.4f}"))
            self._history_table.setItem(row, 4, QTableWidgetItem(f"{h.get('max_drawdown', 0):.2%}"))


# ─── 主面板 ────────────────────────────────────────────────────────────────


class StrategyGovernancePanel(QWidget):
    """策略管理全量 UI 面板（8 个功能 Tab）。

    用法::
        panel = StrategyGovernancePanel()
        main_layout.addWidget(panel)
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_controller()
        self._current_sid: Optional[str] = None
        self._backtest_thread: Optional[_BacktestThread] = None
        self._last_result: Optional[dict[str, Any]] = None
        self._build_ui()
        self._connect_signals()
        # 自动刷新策略列表
        QTimer.singleShot(100, self._list_tab.refresh)

    def _init_controller(self) -> None:
        from gui_app.strategy_controller import StrategyController
        self._ctrl = StrategyController()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # 顶部标题栏
        title_bar = QHBoxLayout()
        title = QLabel("策略管理")
        tf = QFont()
        tf.setPointSize(14)
        tf.setBold(True)
        title.setFont(tf)
        title_bar.addWidget(title)
        title_bar.addStretch()
        self._tab_idx_label = QLabel()
        self._tab_idx_label.setStyleSheet("color:#888;font-size:12px;")
        title_bar.addWidget(self._tab_idx_label)
        layout.addLayout(title_bar)

        # 标签页
        self._tabs = QTabWidget()
        self._list_tab = _StrategyListTab(self._ctrl)
        self._config_tab = _BacktestConfigTab(self._ctrl)
        self._result_tab = _BacktestResultTab(self._ctrl)
        self._perf_tab = _PerformanceTab(self._ctrl)
        self._risk_tab = _RiskTab()
        self._optim_tab = _OptimizationTab()
        self._compare_tab = _ComparisonTab(self._ctrl)
        self._lifecycle_tab = _LifecycleTab(self._ctrl)

        self._tabs.addTab(self._list_tab, "策略列表")
        self._tabs.addTab(self._config_tab, "回测配置")
        self._tabs.addTab(self._result_tab, "回测结果")
        self._tabs.addTab(self._perf_tab, "绩效分析")
        self._tabs.addTab(self._risk_tab, "风险报告")
        self._tabs.addTab(self._optim_tab, "参数优化")
        self._tabs.addTab(self._compare_tab, "策略对比")
        self._tabs.addTab(self._lifecycle_tab, "生命周期")

        layout.addWidget(self._tabs)

    def _connect_signals(self) -> None:
        self._list_tab.backtest_requested.connect(self._on_backtest_requested)
        self._config_tab.run_requested.connect(self._on_backtest_requested)

    # ------------------------------------------------------------------
    # 回测请求处理
    # ------------------------------------------------------------------

    def _on_backtest_requested(self, strategy_id: str) -> None:
        if self._backtest_thread and self._backtest_thread.isRunning():
            QMessageBox.warning(self, "提示", "当前已有回测正在执行，请稍候...")
            return

        self._current_sid = strategy_id
        # 切到回测配置 Tab，显示进度
        self._tabs.setCurrentWidget(self._config_tab)
        self._config_tab.load_strategy(strategy_id)
        self._config_tab.set_running(True)

        # 同时加载历史
        self._lifecycle_tab.load_history(strategy_id)

        self._backtest_thread = _BacktestThread(self._ctrl, strategy_id, self)
        self._backtest_thread.result_ready.connect(self._on_backtest_done)
        self._backtest_thread.error_occurred.connect(self._on_backtest_error)
        self._backtest_thread.status_updated.connect(self._config_tab.set_status)
        self._backtest_thread.start()

    def _on_backtest_done(self, result: dict[str, Any]) -> None:
        self._config_tab.set_running(False)
        elapsed = result.get("elapsed_sec", 0)
        self._config_tab.set_status(f"✅ 回测完成，耗时 {elapsed:.2f}s")
        self._last_result = result

        # 推送到各 Tab
        self._result_tab.load_result(result)
        perf = result.get("performance_metrics", {})
        self._perf_tab.load_metrics(perf)
        self._risk_tab.load_risk(perf)

        # 自动切到回测结果 Tab
        self._tabs.setCurrentWidget(self._result_tab)

        # 添加到对比
        if self._current_sid:
            cfg = self._ctrl.get_strategy(self._current_sid)
            name = cfg.get("strategy_name", self._current_sid) if cfg else self._current_sid
            self._compare_tab.add_result(name, result)

    def _on_backtest_error(self, error_msg: str) -> None:
        self._config_tab.set_running(False)
        self._config_tab.set_status(f"❌ 回测失败")
        QMessageBox.critical(self, "回测失败", f"回测执行失败：\n{error_msg}")
