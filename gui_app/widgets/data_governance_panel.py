"""
data_governance_panel.py — 统一数据治理面板
=============================================

十个 Tab 将现有功能与治理后端整合为统一的数据基座 UI：

    Tab 0  数据下载    → 嵌入 LocalDataManagerWidget（原有功能完整保留）
    Tab 1  数据质检    → DataManagerController → DataIntegrityChecker
    Tab 2  数据路由    → DataManagerController → DataSourceRegistry 指标
    Tab 3  管道状态    → DataManagerController → PipelineHealth
    Tab 4  数据查询    → 嵌入 DuckDBDataManagerWidget（原有功能完整保留）
    Tab 5  数据对账    → 多源交叉验证兜底
    Tab 6  交易日历    → TradingCalendar 日期边界可视化
    Tab 7  数据修复    → HistoryBackfillScheduler 回填队列
    Tab 8  数据库维护  → DuckDB CHECKPOINT / 表统计
    Tab 9  环境配置    → 统一环境变量向导 + 数据源连通性测试

架构关键：
    - 业务逻辑全部在 DataManagerController（可测试，无 Qt）
    - 本文件只负责 Qt 展示和信号连接
    - Tab 切换时的自动刷新采用「首次切换后只触发一次」策略，避免重复 IO
    - 修复完成 / 环境配置保存后向 signal_bus 广播事件
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

from PyQt5.QtCore import QThread, QTimer, pyqtSignal
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSplitter,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from core.events import Events
from core.signal_bus import signal_bus
from gui_app.data_manager_controller import DataManagerController

log = logging.getLogger(__name__)


# ─── 后台任务线程（通用） ─────────────────────────────────────────────────


class _ControllerThread(QThread):
    """在后台线程中调用 DataManagerController 方法，避免阻塞 UI。"""

    result_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

    def run(self) -> None:
        try:
            result = self._fn(*self._args, **self._kwargs)
            self.result_ready.emit(result)
        except Exception as exc:
            self.error_occurred.emit(str(exc))


# ─── Tab 1：数据质检面板 ──────────────────────────────────────────────────


class _IntegrityTab(QWidget):
    """数据完整性校验 Tab。

    操作流程：
        1. 用户在文本框中输入股票代码（逗号分隔）
        2. 点击"开始质检"触发 DataIntegrityChecker.batch_check_integrity()
        3. 结果展示在表格 + 日志区
    """

    def __init__(self, controller: DataManagerController, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._thread: _ControllerThread | None = None
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)

        # ── 顶部控制栏 ──
        top = QHBoxLayout()
        top.addWidget(QLabel("股票代码（逗号分隔）："))
        self._code_input = QLineEdit()
        self._code_input.setPlaceholderText("如：000001.SZ, 600519.SH")
        top.addWidget(self._code_input, 1)

        top.addWidget(QLabel("起始日期："))
        self._start_edit = QLineEdit("2023-01-01")
        self._start_edit.setMaximumWidth(100)
        top.addWidget(self._start_edit)

        top.addWidget(QLabel("截止日期："))
        self._end_edit = QLineEdit("2024-12-31")
        self._end_edit.setMaximumWidth(100)
        top.addWidget(self._end_edit)

        self._run_btn = QPushButton("开始质检")
        self._run_btn.clicked.connect(self._run_check)
        top.addWidget(self._run_btn)
        layout.addLayout(top)

        # ── 进度条 ──
        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        layout.addWidget(self._progress)

        # ── 结果表格 ──
        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(["股票代码", "通过", "问题数", "耗时(ms)"])
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        layout.addWidget(self._table, 2)

        # ── 详情日志 ──
        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setMaximumBlockCount(2000)
        self._log.setFont(QFont("Consolas", 9))
        layout.addWidget(self._log, 1)

        # ── 状态栏 ──
        self._status = QLabel("就绪")
        layout.addWidget(self._status)

    def _run_check(self) -> None:
        raw = self._code_input.text().strip()
        if not raw:
            QMessageBox.warning(self, "提示", "请输入至少一个股票代码")
            return
        codes = [c.strip() for c in raw.split(",") if c.strip()]
        start = self._start_edit.text().strip()
        end = self._end_edit.text().strip()

        self._run_btn.setEnabled(False)
        self._progress.setVisible(True)
        self._table.setRowCount(0)
        self._log.clear()
        self._status.setText(f"正在质检 {len(codes)} 只标的…")

        self._thread = _ControllerThread(
            self._ctrl.run_batch_integrity_check, codes, start, end
        )
        self._thread.result_ready.connect(self._on_result)
        self._thread.error_occurred.connect(self._on_error)
        self._thread.start()

    def _on_result(self, result: dict) -> None:
        self._progress.setVisible(False)
        self._run_btn.setEnabled(True)
        reports = result.get("reports", {})
        self._table.setRowCount(len(reports))
        for row, (code, rep) in enumerate(reports.items()):
            passed = not rep.get("has_errors", True)
            issue_count = len(rep.get("errors", [])) + len(rep.get("warnings", []))
            elapsed = rep.get("elapsed_ms", "-")
            self._table.setItem(row, 0, QTableWidgetItem(code))
            item_pass = QTableWidgetItem("✅ 通过" if passed else "❌ 不通过")
            self._table.setItem(row, 1, item_pass)
            self._table.setItem(row, 2, QTableWidgetItem(str(issue_count)))
            self._table.setItem(row, 3, QTableWidgetItem(str(elapsed)))

            # 展示到日志
            if not passed:
                for err in rep.get("errors", []):
                    self._log.appendPlainText(f"[ERROR] {code}: {err}")
            for warn in rep.get("warnings", []):
                self._log.appendPlainText(f"[WARN]  {code}: {warn}")

        total = result.get("total", 0)
        passed_cnt = result.get("passed", 0)
        self._status.setText(
            f"完成：{passed_cnt}/{total} 通过 · {total - passed_cnt} 异常"
        )
        if result.get("error"):
            self._log.appendPlainText(f"[SYS ERROR] {result['error']}")

    def _on_error(self, msg: str) -> None:
        self._progress.setVisible(False)
        self._run_btn.setEnabled(True)
        self._status.setText(f"质检失败: {msg}")
        self._log.appendPlainText(f"[FATAL] {msg}")

    def on_alert(self, **payload) -> None:
        """由 DataGovernancePanel 在收到 DATA_QUALITY_ALERT 事件时调用。"""
        import time as _time
        ts = _time.strftime("%H:%M:%S")
        code = payload.get("stock_code") or payload.get("symbol") or "?"
        msg = payload.get("message") or payload.get("alert") or str(payload)
        self._log.appendPlainText(f"[ALERT {ts}] {code}: {msg}")


# ─── Tab 2：数据路由指标面板 ──────────────────────────────────────────────


class _RoutingTab(QWidget):
    """DataSourceRegistry 路由指标展示 Tab，支持 60s 定时自动刷新。"""

    def __init__(self, controller: DataManagerController, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._thread: _ControllerThread | None = None
        self._auto_timer = QTimer(self)
        self._auto_timer.timeout.connect(self._refresh)
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)

        btn_bar = QHBoxLayout()
        self._refresh_btn = QPushButton("刷新路由指标")
        self._refresh_btn.clicked.connect(self._refresh)
        btn_bar.addWidget(self._refresh_btn)

        self._auto_btn = QPushButton("启动自动刷新(60s)")
        self._auto_btn.setCheckable(True)
        self._auto_btn.toggled.connect(self._toggle_auto)
        btn_bar.addWidget(self._auto_btn)
        btn_bar.addStretch()
        self._healthy_label = QLabel("就绪")
        btn_bar.addWidget(self._healthy_label)
        layout.addLayout(btn_bar)

        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels(
            ["数据源", "命中", "漏命中", "报错", "质量拒绝", "延迟(ms)"]
        )
        self._table.horizontalHeader().setStretchLastSection(False)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.horizontalHeader().setSectionResizeMode(0, 2)  # Stretch first
        layout.addWidget(self._table)

        self._detail = QPlainTextEdit()
        self._detail.setReadOnly(True)
        self._detail.setMaximumHeight(160)
        self._detail.setFont(QFont("Consolas", 9))
        layout.addWidget(self._detail)

    def _toggle_auto(self, checked: bool) -> None:
        if checked:
            self._auto_timer.start(60_000)
            self._auto_btn.setText("停止自动刷新")
            self._refresh()
        else:
            self._auto_timer.stop()
            self._auto_btn.setText("启动自动刷新(60s)")

    def _refresh(self) -> None:
        self._refresh_btn.setEnabled(False)
        self._thread = _ControllerThread(self._ctrl.get_routing_metrics)
        self._thread.result_ready.connect(self._on_result)
        self._thread.error_occurred.connect(self._on_error)
        self._thread.start()

    def _on_result(self, result: dict) -> None:
        self._refresh_btn.setEnabled(True)
        sources = result.get("sources", {})
        self._table.setRowCount(len(sources))
        for row, (name, m) in enumerate(sources.items()):
            self._table.setItem(row, 0, QTableWidgetItem(name))
            self._table.setItem(row, 1, QTableWidgetItem(str(m.get("hits", 0))))
            self._table.setItem(row, 2, QTableWidgetItem(str(m.get("misses", 0))))
            self._table.setItem(row, 3, QTableWidgetItem(str(m.get("errors", 0))))
            self._table.setItem(row, 4, QTableWidgetItem(str(m.get("quality_rejects", 0))))
            latency = m.get("last_latency_ms")
            self._table.setItem(row, 5, QTableWidgetItem(str(latency) if latency is not None else "-"))

        total = result.get("total_sources", 0)
        healthy = result.get("healthy_sources", 0)
        self._healthy_label.setText(
            f"共 {total} 个数据源，{healthy} 个在线"
        )
        if result.get("error"):
            self._detail.setPlainText(f"[ERROR] {result['error']}")
        else:
            self._detail.setPlainText(
                json.dumps(result.get("sources", {}), ensure_ascii=False, indent=2)
            )

    def _on_error(self, msg: str) -> None:
        self._refresh_btn.setEnabled(True)
        self._detail.setPlainText(f"[FATAL] {msg}")


# ─── Tab 3：管道状态面板 ──────────────────────────────────────────────────


class _PipelineTab(QWidget):
    """PipelineHealth 汇总状态 Tab，支持定时自动刷新。"""

    def __init__(self, controller: DataManagerController, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._thread: _ControllerThread | None = None
        self._auto_timer = QTimer(self)
        self._auto_timer.timeout.connect(self._refresh)
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)

        # 顶部控制栏
        top = QHBoxLayout()
        self._refresh_btn = QPushButton("立即刷新")
        self._refresh_btn.clicked.connect(self._refresh)
        top.addWidget(self._refresh_btn)

        self._auto_btn = QPushButton("启动自动刷新(30s)")
        self._auto_btn.setCheckable(True)
        self._auto_btn.toggled.connect(self._toggle_auto)
        top.addWidget(self._auto_btn)
        top.addStretch()

        self._overall_label = QLabel("未刷新")
        font = QFont()
        font.setBold(True)
        self._overall_label.setFont(font)
        top.addWidget(self._overall_label)
        layout.addLayout(top)

        # DuckDB 概览卡片
        self._summary_label = QLabel("DuckDB：未知")
        layout.addWidget(self._summary_label)

        # 各子系统状态表格
        self._table = QTableWidget(0, 3)
        self._table.setHorizontalHeaderLabels(["子系统", "状态", "详情"])
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        layout.addWidget(self._table, 2)

        # 完整 JSON 报告
        self._json_view = QPlainTextEdit()
        self._json_view.setReadOnly(True)
        self._json_view.setFont(QFont("Consolas", 9))
        self._json_view.setMaximumHeight(200)
        layout.addWidget(self._json_view)

        # 环境变量检查
        env_label = QLabel("── 环境变量检查 ──")
        layout.addWidget(env_label)
        self._env_table = QTableWidget(0, 4)
        self._env_table.setHorizontalHeaderLabels(["变量名", "状态", "值（前50字符）", "说明"])
        self._env_table.horizontalHeader().setStretchLastSection(True)
        self._env_table.setEditTriggers(QTableWidget.NoEditTriggers)
        layout.addWidget(self._env_table)

    def _toggle_auto(self, checked: bool) -> None:
        if checked:
            self._auto_timer.start(30_000)
            self._auto_btn.setText("停止自动刷新")
            self._refresh()
        else:
            self._auto_timer.stop()
            self._auto_btn.setText("启动自动刷新(30s)")

    def _refresh(self) -> None:
        self._refresh_btn.setEnabled(False)
        self._thread = _ControllerThread(self._ctrl.get_pipeline_status)
        self._thread.result_ready.connect(self._on_pipeline_result)
        self._thread.error_occurred.connect(self._on_error)
        self._thread.start()

        # 同时刷新环境变量
        env_thread = _ControllerThread(self._ctrl.validate_environment)
        env_thread.result_ready.connect(self._on_env_result)
        env_thread.start()

        # 同时刷新 DuckDB 统计
        db_thread = _ControllerThread(self._ctrl.get_duckdb_summary)
        db_thread.result_ready.connect(self._on_db_summary)
        db_thread.start()

    def _on_pipeline_result(self, result: dict) -> None:
        self._refresh_btn.setEnabled(True)
        overall = result.get("overall_healthy", False)
        ts = result.get("timestamp", "")
        self._overall_label.setText(
            f"{'✅ 全部健康' if overall else '⚠️ 部分异常'}  [{ts}]"
        )
        checks = result.get("checks", {})
        self._table.setRowCount(len(checks))
        for row, (name, info) in enumerate(checks.items()):
            healthy = info.get("healthy", False)
            details = {k: v for k, v in info.items() if k != "healthy"}
            self._table.setItem(row, 0, QTableWidgetItem(name))
            self._table.setItem(row, 1, QTableWidgetItem("✅ 健康" if healthy else "❌ 异常"))
            self._table.setItem(row, 2, QTableWidgetItem(json.dumps(details, ensure_ascii=False)))

        self._json_view.setPlainText(json.dumps(result, ensure_ascii=False, indent=2))
        if result.get("error"):
            self._json_view.appendPlainText(f"\n[ERROR] {result['error']}")

    def _on_db_summary(self, result: dict) -> None:
        if result.get("healthy"):
            self._summary_label.setText(
                f"DuckDB：{result.get('table_count', 0)} 张表 · "
                f"stock_daily {result.get('stock_daily_rows', 0):,} 行 · "
                f"最新日期 {result.get('latest_date', 'N/A')} · "
                f"路径 {result.get('path', '')}"
            )
        else:
            self._summary_label.setText(f"DuckDB：异常 — {result.get('error', '')}")

    def _on_env_result(self, result: dict) -> None:
        items = result.get("items", [])
        self._env_table.setRowCount(len(items))
        for row, item in enumerate(items):
            status_icon = {"ok": "✅", "missing": "⚠️", "invalid": "❌"}.get(item["status"], "?")
            self._env_table.setItem(row, 0, QTableWidgetItem(item["key"]))
            self._env_table.setItem(row, 1, QTableWidgetItem(f"{status_icon} {item['status']}"))
            self._env_table.setItem(row, 2, QTableWidgetItem(item["value"][:50]))
            self._env_table.setItem(row, 3, QTableWidgetItem(item["note"]))

    def _on_error(self, msg: str) -> None:
        self._refresh_btn.setEnabled(True)
        self._json_view.setPlainText(f"[FATAL] {msg}")


# ─── 主面板：5 tabs ────────────────────────────────────────────────────────


class DataGovernancePanel(QWidget):
    """统一数据治理面板（13 个 Tab）。

    嵌入到 main_window 的「数据管理」Tab 位置。

    Tab  0  数据下载   → LocalDataManagerWidget（原有功能）
    Tab  1  数据质检   → DataIntegrityChecker（接收 DATA_QUALITY_ALERT 事件）
    Tab  2  数据路由   → DataSourceRegistry 路由指标（支持 60s 自动刷新）
    Tab  3  管道状态   → PipelineHealth / 环境变量
    Tab  4  数据查询   → DuckDBDataManagerWidget（原有功能）
    Tab  5  数据对账   → 多源交叉验证兜底
    Tab  6  交易日历   → TradingCalendar 日期边界可视化
    Tab  7  数据修复   → 回填调度队列（接收 BACKFILL_TASK_UPDATED 事件）
    Tab  8  数据库维护 → DuckDB CHECKPOINT / 表统计
    Tab  9  环境配置   → 统一环境变量向导 + 连通性测试
    Tab 10  实时链路   → REALTIME_PIPELINE_STATUS_UPDATED 事件流
    Tab 11  数据覆盖   → DataCoverageWidget 覆盖率矩阵 + 全周期补数
    Tab 12  数据溯源   → data_ingestion_status 逐标的入库血缘追踪
    """

    _calendar_tab: _TradingCalendarTab
    _maintenance_tab: _DatabaseMaintenanceTab
    _env_config_tab: _EnvironmentConfigTab
    _integrity_tab: _IntegrityTab
    _repair_tab: _RepairTab
    _realtime_tab: _RealtimeMonitorTab

    def __init__(self, controller: DataManagerController | None = None, parent=None):
        super().__init__(parent)
        self._ctrl = controller or DataManagerController()
        self._tabs = QTabWidget()
        self._auto_refreshed: set[int] = set()  # 首次切换后只触发一次自动刷新
        self._closed = False
        self._init_ui()
        self._connect_events()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._tabs)

        # Tab 0：数据下载（嵌入旧 LocalDataManagerWidget）
        self._tabs.addTab(self._make_download_tab(), "数据下载")

        # Tab 1：数据质检
        self._integrity_tab = _IntegrityTab(self._ctrl)
        self._tabs.addTab(self._integrity_tab, "数据质检")

        # Tab 2：数据路由
        self._routing_tab = _RoutingTab(self._ctrl)
        self._tabs.addTab(self._routing_tab, "数据路由")

        # Tab 3：管道状态
        self._pipeline_tab = _PipelineTab(self._ctrl)
        self._tabs.addTab(self._pipeline_tab, "管道状态")

        # Tab 4：数据查询（嵌入旧 DuckDBDataManagerWidget）
        self._tabs.addTab(self._make_query_tab(), "数据查询")

        # Tab 5：数据对账
        self._reconciliation_tab = _ReconciliationTab(self._ctrl)
        self._tabs.addTab(self._reconciliation_tab, "数据对账")

        # Tab 6：交易日历
        self._calendar_tab = _TradingCalendarTab(self._ctrl)
        self._tabs.addTab(self._calendar_tab, "交易日历")

        # Tab 7：数据修复
        self._repair_tab = _RepairTab(self._ctrl)
        self._tabs.addTab(self._repair_tab, "数据修复")

        # Tab 8：数据库维护
        self._maintenance_tab = _DatabaseMaintenanceTab(self._ctrl)
        self._tabs.addTab(self._maintenance_tab, "数据库维护")

        # Tab 9：环境配置
        self._env_config_tab = _EnvironmentConfigTab(self._ctrl)
        self._tabs.addTab(self._env_config_tab, "环境配置")

        # Tab 10：实时链路监控
        self._realtime_tab = _RealtimeMonitorTab(self._ctrl)
        self._tabs.addTab(self._realtime_tab, "实时链路")

        # Tab 11：数据覆盖矩阵（全周期补数入口）
        try:
            from gui_app.widgets.data_coverage_widget import DataCoverageWidget
            self._coverage_tab = DataCoverageWidget()
        except Exception as _exc:
            _lbl = QLabel(f"数据覆盖组件加载失败：{_exc}")
            _lbl.setWordWrap(True)
            self._coverage_tab = _lbl  # type: ignore[assignment]
        self._tabs.addTab(self._coverage_tab, "数据覆盖")

        # Tab 12：数据来源溯源
        self._traceability_tab = _TraceabilityTab(self._ctrl)
        self._tabs.addTab(self._traceability_tab, "数据溯源")

        # 切换 Tab 时触发自动刷新
        self._tabs.currentChanged.connect(self._on_tab_changed)

    def _connect_events(self) -> None:
        """订阅 signal_bus 事件，将实时推送转发到各 Tab。"""
        try:
            signal_bus.subscribe(
                Events.REALTIME_PIPELINE_STATUS_UPDATED, self._on_rt_pipeline_event
            )
            signal_bus.subscribe(
                Events.DATA_QUALITY_ALERT, self._on_data_quality_alert_event
            )
            signal_bus.subscribe(
                Events.BACKFILL_TASK_UPDATED, self._on_backfill_task_updated
            )
            signal_bus.subscribe(
                Events.DATA_INGESTION_COMPLETE, self._on_ingestion_complete
            )
            signal_bus.subscribe(
                Events.CROSS_VALIDATION_BATCH_DONE, self._on_batch_done
            )
        except Exception:
            pass

    def closeEvent(self, event) -> None:
        """关闭时取消所有 signal_bus 订阅，防止僵尸面板响应事件并触发后台线程。"""
        self._closed = True
        _unsub_pairs = [
            (Events.REALTIME_PIPELINE_STATUS_UPDATED, self._on_rt_pipeline_event),
            (Events.DATA_QUALITY_ALERT, self._on_data_quality_alert_event),
            (Events.BACKFILL_TASK_UPDATED, self._on_backfill_task_updated),
            (Events.DATA_INGESTION_COMPLETE, self._on_ingestion_complete),
            (Events.CROSS_VALIDATION_BATCH_DONE, self._on_batch_done),
        ]
        for evt, handler in _unsub_pairs:
            try:
                signal_bus.unsubscribe(evt, handler)
            except Exception:
                pass
        # 显式关闭内嵌子组件（子 widget 不自动调用 closeEvent）
        try:
            if hasattr(self, "_coverage_tab") and hasattr(self._coverage_tab, "close"):
                self._coverage_tab.close()
        except Exception:
            pass
        super().closeEvent(event)

    def _on_rt_pipeline_event(self, **payload) -> None:
        try:
            self._realtime_tab.on_pipeline_event(**payload)
        except Exception:
            pass

    def _on_data_quality_alert_event(self, **payload) -> None:
        try:
            self._integrity_tab.on_alert(**payload)
        except Exception:
            pass

    def _on_backfill_task_updated(self, **payload) -> None:
        try:
            self._repair_tab.on_backfill_updated(**payload)
        except Exception:
            pass

    def _on_ingestion_complete(self, **payload) -> None:
        """全周期入库完成后刷新数据覆盖矩阵 Tab。"""
        if self._closed:
            return
        try:
            if hasattr(self._coverage_tab, 'refresh'):
                self._coverage_tab.refresh()
        except Exception:
            pass

    def _on_batch_done(self, **payload) -> None:
        """定时批量交叉验证完成后，将结果推送到对账 Tab 的概要区域。"""
        if self._closed:
            return
        try:
            self._reconciliation_tab.on_batch_done(**payload)
        except Exception:
            pass

    def _make_download_tab(self) -> QWidget:
        """尝试加载 LocalDataManagerWidget，失败时显示说明标签。"""
        try:
            from gui_app.widgets.local_data_manager_widget import LocalDataManagerWidget
            return LocalDataManagerWidget()
        except Exception as exc:
            lbl = QLabel(f"数据下载组件加载失败：{exc}")
            lbl.setWordWrap(True)
            return lbl

    def _make_query_tab(self) -> QWidget:
        """尝试加载 DuckDBDataManagerWidget，失败时显示说明标签。"""
        try:
            from gui_app.widgets.duckdb_data_manager_widget import DuckDBDataManagerWidget
            return DuckDBDataManagerWidget()
        except Exception as exc:
            lbl = QLabel(f"数据查询组件加载失败：{exc}")
            lbl.setWordWrap(True)
            return lbl

    def _on_tab_changed(self, index: int) -> None:
        """切换 Tab 时按需触发首次自动刷新（每个 Tab 整个生命周期只刷新一次）。"""
        if index in self._auto_refreshed:
            return
        self._auto_refreshed.add(index)
        if index == 2 and hasattr(self, "_routing_tab"):
            self._routing_tab._refresh()
        elif index == 3 and hasattr(self, "_pipeline_tab"):
            self._pipeline_tab._refresh()
        elif index == 6 and hasattr(self, "_calendar_tab"):
            self._calendar_tab._query_current_year()
        elif index == 8 and hasattr(self, "_maintenance_tab"):
            self._maintenance_tab._refresh()
        elif index == 9 and hasattr(self, "_env_config_tab"):
            self._env_config_tab._refresh()
        elif index == 10 and hasattr(self, "_realtime_tab"):
            self._realtime_tab._refresh()
        elif index == 12 and hasattr(self, "_traceability_tab"):
            self._traceability_tab._refresh()


# ─── Tab 5：数据对账（多源交叉验证兜底） ──────────────────────────────────


class _ReconciliationTab(QWidget):
    """多源数据交叉验证 Tab。

    选择标的和日期区间，对比 DuckDB 本地存储与实时数据源，
    展示价格一致性率、最大偏差、差异日期列表。
    """

    def __init__(self, controller: DataManagerController, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._thread: _ControllerThread | None = None
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)

        top = QHBoxLayout()
        top.addWidget(QLabel("股票代码："))
        self._code_input = QLineEdit("000001.SZ")
        self._code_input.setMaximumWidth(140)
        top.addWidget(self._code_input)
        top.addWidget(QLabel("起始日期："))
        self._start_edit = QLineEdit("2024-01-01")
        self._start_edit.setMaximumWidth(100)
        top.addWidget(self._start_edit)
        top.addWidget(QLabel("截止日期："))
        self._end_edit = QLineEdit("2024-12-31")
        self._end_edit.setMaximumWidth(100)
        top.addWidget(self._end_edit)
        self._run_btn = QPushButton("开始对账")
        self._run_btn.clicked.connect(self._run)
        top.addWidget(self._run_btn)
        top.addStretch()
        layout.addLayout(top)

        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        layout.addWidget(self._progress)

        # 概要卡片
        summary_group = QGroupBox("对账概要")
        sg_layout = QFormLayout(summary_group)
        self._lbl_consistent = QLabel("—")
        self._lbl_rate = QLabel("—")
        self._lbl_max_diff = QLabel("—")
        self._lbl_rows = QLabel("—")
        self._lbl_source = QLabel("—")
        sg_layout.addRow("一致性：", self._lbl_consistent)
        sg_layout.addRow("一致率：", self._lbl_rate)
        sg_layout.addRow("最大偏差：", self._lbl_max_diff)
        sg_layout.addRow("比对行数：", self._lbl_rows)
        sg_layout.addRow("验证方式：", self._lbl_source)
        layout.addWidget(summary_group)

        # 偏差日期明细
        diff_label = QLabel("价格偏差超过 1% 的日期：")
        layout.addWidget(diff_label)
        self._diff_text = QPlainTextEdit()
        self._diff_text.setReadOnly(True)
        self._diff_text.setMaximumHeight(120)
        self._diff_text.setFont(QFont("Consolas", 9))
        layout.addWidget(self._diff_text)

        self._status = QLabel("就绪")
        layout.addWidget(self._status)

        # ── 定时批量对账结果（由 CROSS_VALIDATION_BATCH_DONE 事件驱动推送） ──
        self._batch_group = QGroupBox("上次定时批量对账（15:40 自动运行）")
        bg_layout = QFormLayout(self._batch_group)
        self._lbl_batch_time = QLabel("—")
        self._lbl_batch_total = QLabel("—")
        self._lbl_batch_passed = QLabel("—")
        self._lbl_batch_failed = QLabel("—")
        bg_layout.addRow("完成时间：", self._lbl_batch_time)
        bg_layout.addRow("验证标的数：", self._lbl_batch_total)
        bg_layout.addRow("通过：", self._lbl_batch_passed)
        bg_layout.addRow("异常：", self._lbl_batch_failed)
        self._lbl_batch_bad = QLabel("—")
        self._lbl_batch_bad.setWordWrap(True)
        bg_layout.addRow("异常标的：", self._lbl_batch_bad)
        layout.addWidget(self._batch_group)

    def on_batch_done(self, total: int = 0, passed: int = 0, failed: int = 0,
                      results: list | None = None, bad_codes: list | None = None,
                      **_kwargs) -> None:
        """DataGovernancePanel 收到 CROSS_VALIDATION_BATCH_DONE 时调用，更新批量摘要。"""
        import datetime as _dt
        now = _dt.datetime.now().strftime("%H:%M:%S")
        self._lbl_batch_time.setText(now)
        self._lbl_batch_total.setText(str(total))
        self._lbl_batch_passed.setText(str(passed))
        failed_color = "#F44336" if failed > 0 else "#4CAF50"
        self._lbl_batch_failed.setText(
            f"<span style='color:{failed_color};font-weight:bold'>{failed}</span>"
        )
        if bad_codes:
            self._lbl_batch_bad.setText("  ".join(bad_codes[:8]))
        else:
            self._lbl_batch_bad.setText("无")

    def _run(self) -> None:
        code = self._code_input.text().strip()
        if not code:
            QMessageBox.warning(self, "提示", "请输入股票代码")
            return
        self._run_btn.setEnabled(False)
        self._progress.setVisible(True)
        self._status.setText("对账中…")
        self._diff_text.clear()
        self._thread = _ControllerThread(
            self._ctrl.cross_validate_sources,
            code, self._start_edit.text().strip(), self._end_edit.text().strip()
        )
        self._thread.result_ready.connect(self._on_result)
        self._thread.error_occurred.connect(self._on_error)
        self._thread.start()

    def _on_result(self, result: dict) -> None:
        self._progress.setVisible(False)
        self._run_btn.setEnabled(True)
        if result.get("error"):
            self._status.setText(f"对账失败: {result['error']}")
            return
        consistent = result.get("consistent", False)
        rate = result.get("consistency_rate", 0.0)
        max_diff = result.get("max_diff_pct", 0.0)
        duckdb_rows = result.get("duckdb_rows", 0)
        # 兼容新字段名 akshare_rows（真正多源），也兼容旧字段 live_rows
        akshare_rows = result.get("akshare_rows", result.get("live_rows", 0))
        compared = result.get("compared_rows", min(duckdb_rows, akshare_rows))
        source = result.get("source", "unknown")
        self._lbl_consistent.setText("✅ 一致" if consistent else "⚠️ 存在差异")
        self._lbl_rate.setText(f"{rate * 100:.2f}%")
        self._lbl_max_diff.setText(f"{max_diff:.4f}%")
        self._lbl_rows.setText(f"DuckDB {duckdb_rows} 行 / 对照源 {akshare_rows} 行 / 比对 {compared} 行")
        self._lbl_source.setText(source)
        diff_days = result.get("diff_days", [])
        if diff_days:
            self._diff_text.setPlainText("\n".join(diff_days))
        else:
            self._diff_text.setPlainText("无显著差异日期")
        note = result.get("note", "")
        self._status.setText(f"对账完成 {note}")

    def _on_error(self, msg: str) -> None:
        self._progress.setVisible(False)
        self._run_btn.setEnabled(True)
        self._status.setText(f"错误: {msg}")


# ─── Tab 6：交易日历（日期边界可视化） ───────────────────────────────────


class _TradingCalendarTab(QWidget):
    """交易日历查询 Tab。

    输入日期区间，返回交易日列表、节假日统计、缺失交易日提示。
    """

    def __init__(self, controller: DataManagerController, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._thread: _ControllerThread | None = None
        self._init_ui()

    def _init_ui(self) -> None:
        import datetime as _dt
        today = _dt.date.today()
        layout = QVBoxLayout(self)

        top = QHBoxLayout()
        top.addWidget(QLabel("起始日期："))
        self._start_edit = QLineEdit(f"{today.year}-01-01")
        self._start_edit.setMaximumWidth(110)
        top.addWidget(self._start_edit)
        top.addWidget(QLabel("截止日期："))
        self._end_edit = QLineEdit(f"{today.year}-12-31")
        self._end_edit.setMaximumWidth(110)
        top.addWidget(self._end_edit)
        self._query_btn = QPushButton("查询")
        self._query_btn.clicked.connect(self._query)
        top.addWidget(self._query_btn)
        top.addStretch()
        layout.addLayout(top)

        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        layout.addWidget(self._progress)

        # 统计卡片
        stats_group = QGroupBox("统计概要")
        sg = QFormLayout(stats_group)
        self._lbl_total = QLabel("—")
        self._lbl_trade = QLabel("—")
        self._lbl_weekend = QLabel("—")
        self._lbl_holiday = QLabel("—")
        sg.addRow("日历总天数：", self._lbl_total)
        sg.addRow("交易日：", self._lbl_trade)
        sg.addRow("周末天数：", self._lbl_weekend)
        sg.addRow("节假日（工作日）：", self._lbl_holiday)
        layout.addWidget(stats_group)

        splitter = QSplitter()
        layout.addWidget(splitter, 1)

        # 交易日列表
        trade_group = QWidget()
        tg_layout = QVBoxLayout(trade_group)
        tg_layout.addWidget(QLabel("交易日列表："))
        self._trade_table = QTableWidget(0, 2)
        self._trade_table.setHorizontalHeaderLabels(["序号", "交易日"])
        self._trade_table.horizontalHeader().setStretchLastSection(True)
        self._trade_table.setEditTriggers(QTableWidget.NoEditTriggers)
        tg_layout.addWidget(self._trade_table)
        splitter.addWidget(trade_group)

        # 非交易日列表
        non_group = QWidget()
        ng_layout = QVBoxLayout(non_group)
        ng_layout.addWidget(QLabel("非交易日（节假日 + 周末）："))
        self._non_table = QTableWidget(0, 2)
        self._non_table.setHorizontalHeaderLabels(["序号", "日期"])
        self._non_table.horizontalHeader().setStretchLastSection(True)
        self._non_table.setEditTriggers(QTableWidget.NoEditTriggers)
        ng_layout.addWidget(self._non_table)
        splitter.addWidget(non_group)

    def _query_current_year(self) -> None:
        """首次切换到该 Tab 时自动触发。"""
        self._query()

    def _query(self) -> None:
        self._query_btn.setEnabled(False)
        self._progress.setVisible(True)
        self._thread = _ControllerThread(
            self._ctrl.get_trading_calendar_info,
            self._start_edit.text().strip(),
            self._end_edit.text().strip(),
        )
        self._thread.result_ready.connect(self._on_result)
        self._thread.error_occurred.connect(self._on_error)
        self._thread.start()

    def _on_result(self, result: dict) -> None:
        self._progress.setVisible(False)
        self._query_btn.setEnabled(True)
        if result.get("error"):
            QMessageBox.warning(self, "查询失败", result["error"])
            return
        self._lbl_total.setText(str(result.get("total_days", 0)))
        self._lbl_trade.setText(str(result.get("trading_days", 0)))
        self._lbl_weekend.setText(str(result.get("weekend_days", 0)))
        self._lbl_holiday.setText(str(result.get("holiday_days", 0)))

        trade_days = result.get("trading_days_list", [])
        self._trade_table.setRowCount(len(trade_days))
        for i, d in enumerate(trade_days):
            self._trade_table.setItem(i, 0, QTableWidgetItem(str(i + 1)))
            self._trade_table.setItem(i, 1, QTableWidgetItem(d))

        non_days = result.get("non_trading_list", [])
        self._non_table.setRowCount(len(non_days))
        for i, d in enumerate(non_days):
            self._non_table.setItem(i, 0, QTableWidgetItem(str(i + 1)))
            self._non_table.setItem(i, 1, QTableWidgetItem(d))

    def _on_error(self, msg: str) -> None:
        self._progress.setVisible(False)
        self._query_btn.setEnabled(True)
        QMessageBox.critical(self, "错误", msg)


# ─── Tab 7：数据修复中心 ──────────────────────────────────────────────────


class _RepairTab(QWidget):
    """缺失数据修复 Tab：触发后台回填调度队列 + 死信队列管理。"""

    def __init__(self, controller: DataManagerController, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._thread: _ControllerThread | None = None
        self._dl_thread: _ControllerThread | None = None
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)

        hint = QLabel(
            "修复操作将把指定标的加入历史数据回填队列（HistoryBackfillScheduler），"
            "后台异步补全缺失的交易日数据。"
        )
        hint.setWordWrap(True)
        layout.addWidget(hint)

        form = QFormLayout()
        self._code_input = QLineEdit()
        self._code_input.setPlaceholderText("如：000001.SZ")
        form.addRow("股票代码：", self._code_input)
        self._start_edit = QLineEdit("2024-01-01")
        form.addRow("起始日期：", self._start_edit)
        self._end_edit = QLineEdit("2024-12-31")
        form.addRow("截止日期：", self._end_edit)
        layout.addLayout(form)

        self._repair_btn = QPushButton("触发数据修复")
        self._repair_btn.clicked.connect(self._run_repair)
        layout.addWidget(self._repair_btn)

        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        layout.addWidget(self._progress)

        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setFont(QFont("Consolas", 9))
        self._log.setMaximumBlockCount(500)
        layout.addWidget(self._log, 1)

        # ── 死信队列区段 ──
        dl_group = QGroupBox("回填死信队列（失败任务）")
        dl_layout = QVBoxLayout(dl_group)

        dl_btn_bar = QHBoxLayout()
        self._dl_refresh_btn = QPushButton("刷新死信队列")
        self._dl_refresh_btn.clicked.connect(self._load_dead_letter)
        dl_btn_bar.addWidget(self._dl_refresh_btn)
        self._dl_clear_btn = QPushButton("清空死信文件")
        self._dl_clear_btn.clicked.connect(self._clear_dead_letter)
        dl_btn_bar.addWidget(self._dl_clear_btn)
        dl_btn_bar.addStretch()
        self._dl_status = QLabel("未加载")
        dl_btn_bar.addWidget(self._dl_status)
        dl_layout.addLayout(dl_btn_bar)

        self._dl_table = QTableWidget(0, 7)
        self._dl_table.setHorizontalHeaderLabels(
            ["股票代码", "起始日期", "截止日期", "周期", "失败原因", "重试次数", "失败时间"]
        )
        self._dl_table.horizontalHeader().setStretchLastSection(True)
        self._dl_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._dl_table.setMaximumHeight(180)
        dl_layout.addWidget(self._dl_table)
        layout.addWidget(dl_group)

    def _run_repair(self) -> None:
        code = self._code_input.text().strip()
        if not code:
            QMessageBox.warning(self, "提示", "请输入股票代码")
            return
        self._repair_btn.setEnabled(False)
        self._progress.setVisible(True)
        self._log.appendPlainText(
            f"[INFO] 发起修复：{code} [{self._start_edit.text().strip()} ~ {self._end_edit.text().strip()}]"
        )
        self._thread = _ControllerThread(
            self._ctrl.repair_missing_data,
            code,
            self._start_edit.text().strip(),
            self._end_edit.text().strip(),
        )
        self._thread.result_ready.connect(self._on_result)
        self._thread.error_occurred.connect(self._on_error)
        self._thread.start()

    def _on_result(self, result: dict) -> None:
        self._progress.setVisible(False)
        self._repair_btn.setEnabled(True)
        if result.get("queued"):
            self._log.appendPlainText(f"[OK]  {result.get('message', '修复任务已排队')}")
            try:
                signal_bus.emit(
                    Events.DATA_REPAIRED,
                    stock_code=self._code_input.text().strip(),
                    queued=True,
                    source="repair_tab",
                )
            except Exception:
                pass
        else:
            self._log.appendPlainText(f"[FAIL] {result.get('error', '未知错误')}")

    def _on_error(self, msg: str) -> None:
        self._progress.setVisible(False)
        self._repair_btn.setEnabled(True)
        self._log.appendPlainText(f"[FATAL] {msg}")

    # ── 死信队列操作 ──

    def _load_dead_letter(self) -> None:
        self._dl_refresh_btn.setEnabled(False)
        self._dl_status.setText("加载中…")
        self._dl_thread = _ControllerThread(self._ctrl.get_backfill_dead_letter)
        self._dl_thread.result_ready.connect(self._on_dead_letter_result)
        self._dl_thread.error_occurred.connect(self._on_dl_error)
        self._dl_thread.start()

    def _on_dead_letter_result(self, result: dict) -> None:
        self._dl_refresh_btn.setEnabled(True)
        entries = result.get("entries", [])
        self._dl_table.setRowCount(len(entries))
        for row, entry in enumerate(entries):
            self._dl_table.setItem(row, 0, QTableWidgetItem(entry.get("stock_code", "")))
            self._dl_table.setItem(row, 1, QTableWidgetItem(entry.get("start_date", "")))
            self._dl_table.setItem(row, 2, QTableWidgetItem(entry.get("end_date", "")))
            self._dl_table.setItem(row, 3, QTableWidgetItem(entry.get("period", "")))
            self._dl_table.setItem(row, 4, QTableWidgetItem(entry.get("reason", "")))
            self._dl_table.setItem(row, 5, QTableWidgetItem(str(entry.get("retry_count", 0))))
            self._dl_table.setItem(row, 6, QTableWidgetItem(entry.get("failed_at", "")))
        total = result.get("total", 0)
        path = result.get("file_path", "")
        err = result.get("error", "")
        if err:
            self._dl_status.setText(f"错误：{err[:60]}")
        else:
            self._dl_status.setText(f"共 {total} 条失败任务  ·  {path}")

    def _clear_dead_letter(self) -> None:
        if QMessageBox.question(
            self, "确认", "是否清空所有死信队列记录？此操作不可撤销。",
            QMessageBox.Yes | QMessageBox.No,
        ) != QMessageBox.Yes:
            return
        t = _ControllerThread(self._ctrl.clear_backfill_dead_letter)
        t.result_ready.connect(self._on_clear_result)
        t.error_occurred.connect(self._on_dl_error)
        t.start()

    def _on_clear_result(self, result: dict) -> None:
        if result.get("ok"):
            self._log.appendPlainText(f"[OK]  {result.get('message', '死信队列已清空')}")
            self._load_dead_letter()  # 刷新表格
        else:
            self._log.appendPlainText(f"[FAIL] {result.get('error', '清空失败')}")

    def _on_dl_error(self, msg: str) -> None:
        self._dl_refresh_btn.setEnabled(True)
        self._dl_status.setText(f"错误：{msg[:60]}")

    def on_backfill_updated(self, **payload) -> None:
        """由 DataGovernancePanel 在收到 BACKFILL_TASK_UPDATED 事件时调用，自动刷新死信表格。"""
        import time as _time
        ts = _time.strftime("%H:%M:%S")
        code = payload.get("stock_code") or ""
        status = payload.get("status") or "updated"
        self._log.appendPlainText(f"[BACKFILL {ts}] {code} {status}")
        self._load_dead_letter()


# ─── Tab 8：数据库维护（DuckDB 运维） ────────────────────────────────────


class _DatabaseMaintenanceTab(QWidget):
    """DuckDB 数据库维护 Tab：表统计、CHECKPOINT、VACUUM。"""

    def __init__(self, controller: DataManagerController, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._thread: _ControllerThread | None = None
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)

        btn_bar = QHBoxLayout()
        self._refresh_btn = QPushButton("刷新表统计")
        self._refresh_btn.clicked.connect(self._refresh)
        btn_bar.addWidget(self._refresh_btn)
        self._checkpoint_btn = QPushButton("执行 CHECKPOINT")
        self._checkpoint_btn.clicked.connect(self._run_checkpoint)
        btn_bar.addWidget(self._checkpoint_btn)
        btn_bar.addStretch()
        self._size_label = QLabel("DB 大小：N/A")
        btn_bar.addWidget(self._size_label)
        layout.addLayout(btn_bar)

        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        layout.addWidget(self._progress)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(["表名", "行数", "列数", "最新日期"])
        self._table.horizontalHeader().setStretchLastSection(False)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        layout.addWidget(self._table, 2)

        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setFont(QFont("Consolas", 9))
        self._log.setMaximumHeight(120)
        layout.addWidget(self._log)

        # ── 导出快照区段 ──
        export_group = QGroupBox("数据快照导出（stock_daily → CSV）")
        exp_layout = QVBoxLayout(export_group)
        exp_form = QFormLayout()
        self._export_codes = QLineEdit()
        self._export_codes.setPlaceholderText("如：000001.SZ, 600519.SH（逗号分隔）")
        exp_form.addRow("股票代码：", self._export_codes)
        self._export_start = QLineEdit("2024-01-01")
        exp_form.addRow("起始日期：", self._export_start)
        self._export_end = QLineEdit("2024-12-31")
        exp_form.addRow("截止日期：", self._export_end)
        exp_layout.addLayout(exp_form)
        exp_btn_bar = QHBoxLayout()
        self._export_btn = QPushButton("导出 CSV 快照")
        self._export_btn.clicked.connect(self._run_export)
        exp_btn_bar.addWidget(self._export_btn)
        exp_btn_bar.addStretch()
        self._export_status = QLabel("就绪")
        exp_btn_bar.addWidget(self._export_status)
        exp_layout.addLayout(exp_btn_bar)
        layout.addWidget(export_group)

    def _refresh(self) -> None:
        self._refresh_btn.setEnabled(False)
        self._progress.setVisible(True)
        self._thread = _ControllerThread(self._ctrl.get_duckdb_maintenance_info)
        self._thread.result_ready.connect(self._on_maintenance_result)
        self._thread.error_occurred.connect(self._on_error)
        self._thread.start()

    def _run_checkpoint(self) -> None:
        self._checkpoint_btn.setEnabled(False)
        self._progress.setVisible(True)
        self._log.appendPlainText("[INFO] 正在执行 CHECKPOINT…")
        t = _ControllerThread(self._ctrl.run_checkpoint)
        t.result_ready.connect(self._on_checkpoint_result)
        t.error_occurred.connect(self._on_error)
        t.start()

    def _run_export(self) -> None:
        raw = self._export_codes.text().strip().replace("，", ",")
        if not raw:
            QMessageBox.warning(self, "提示", "请输入股票代码")
            return
        codes = [c.strip() for c in raw.split(",") if c.strip()]
        import time as _time
        ts = _time.strftime("%Y%m%d_%H%M%S")
        out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data_export")
        output_path = os.path.join(out_dir, f"snapshot_{ts}.csv")
        self._export_btn.setEnabled(False)
        self._export_status.setText("导出中…")
        self._log.appendPlainText(f"[INFO] 导出 {len(codes)} 只标的 → {output_path}")
        t = _ControllerThread(
            self._ctrl.export_data_snapshot,
            codes,
            self._export_start.text().strip(),
            self._export_end.text().strip(),
            output_path,
        )
        t.result_ready.connect(self._on_export_result)
        t.error_occurred.connect(self._on_error)
        t.start()

    def _on_export_result(self, result: dict) -> None:
        self._export_btn.setEnabled(True)
        if result.get("ok"):
            rows = result.get("rows", 0)
            syms = result.get("symbols", 0)
            path = result.get("output_path", "")
            self._export_status.setText(f"✅ 已导出 {rows:,} 行 · {syms} 只标的")
            self._log.appendPlainText(f"[OK]  {rows:,} 行 · {syms} 只标的 → {path}")
        else:
            err = result.get("error", "未知错误")
            self._export_status.setText(f"❌ {err[:60]}")
            self._log.appendPlainText(f"[FAIL] {err}")

    def _on_maintenance_result(self, result: dict) -> None:
        self._progress.setVisible(False)
        self._refresh_btn.setEnabled(True)
        if result.get("error"):
            self._log.appendPlainText(f"[ERROR] {result['error']}")
            return
        tables = result.get("tables", [])
        self._table.setRowCount(len(tables))
        for row, t in enumerate(tables):
            self._table.setItem(row, 0, QTableWidgetItem(t.get("name", "")))
            rows_val = t.get("rows", 0)
            self._table.setItem(row, 1, QTableWidgetItem(f"{rows_val:,}" if isinstance(rows_val, int) else str(rows_val)))
            self._table.setItem(row, 2, QTableWidgetItem(str(t.get("columns", 0))))
            self._table.setItem(row, 3, QTableWidgetItem(str(t.get("last_date", "N/A"))))
        size = result.get("db_size_mb", 0.0)
        self._size_label.setText(f"DB 大小：{size} MB  共 {len(tables)} 张表")

    def _on_checkpoint_result(self, result: dict) -> None:
        self._progress.setVisible(False)
        self._checkpoint_btn.setEnabled(True)
        if result.get("ok"):
            self._log.appendPlainText(f"[OK]  {result.get('message', 'CHECKPOINT 完成')}")
        else:
            self._log.appendPlainText(f"[FAIL] {result.get('error', '失败')}")

    def _on_error(self, msg: str) -> None:
        self._progress.setVisible(False)
        self._refresh_btn.setEnabled(True)
        self._checkpoint_btn.setEnabled(True)
        self._log.appendPlainText(f"[FATAL] {msg}")


# ─── Tab 9：环境配置向导 ──────────────────────────────────────────────────


class _EnvironmentConfigTab(QWidget):
    """统一环境变量配置向导 Tab。

    功能：
    - 展示所有已知 EASYXT_ 变量，按分组显示
    - 标注缺失/无效变量
    - 支持在线编辑并写入 .env 文件
    - 数据源连通性一键测试
    """

    def __init__(self, controller: DataManagerController, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._thread: _ControllerThread | None = None
        self._env_value_map: dict[str, str] = {}
        self._wm_last_snapshot: dict[str, str] = {}
        self._wm_profile_audit_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "artifacts",
            "watermark_profile_audit.jsonl",
        )
        self._wm_profile_presets: dict[str, dict[str, str]] = {
            "balanced": {
                "EASYXT_WM_PROFILE": "balanced",
                "EASYXT_WM_WEIGHT_LATE": "0.45",
                "EASYXT_WM_WEIGHT_OOO": "0.35",
                "EASYXT_WM_WEIGHT_LATENESS": "0.20",
                "EASYXT_WM_QSCORE_FLOOR": "0.97",
                "EASYXT_WM_LOOKBACK_DAYS": "7",
            },
            "conservative": {
                "EASYXT_WM_PROFILE": "conservative",
                "EASYXT_WM_WEIGHT_LATE": "0.50",
                "EASYXT_WM_WEIGHT_OOO": "0.35",
                "EASYXT_WM_WEIGHT_LATENESS": "0.15",
                "EASYXT_WM_QSCORE_FLOOR": "0.985",
                "EASYXT_WM_LOOKBACK_DAYS": "14",
            },
            "aggressive": {
                "EASYXT_WM_PROFILE": "aggressive",
                "EASYXT_WM_WEIGHT_LATE": "0.40",
                "EASYXT_WM_WEIGHT_OOO": "0.30",
                "EASYXT_WM_WEIGHT_LATENESS": "0.30",
                "EASYXT_WM_QSCORE_FLOOR": "0.95",
                "EASYXT_WM_LOOKBACK_DAYS": "7",
            },
        }
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)

        # 顶部控制栏
        top = QHBoxLayout()
        self._refresh_btn = QPushButton("刷新配置")
        self._refresh_btn.clicked.connect(self._refresh)
        top.addWidget(self._refresh_btn)

        top.addWidget(QLabel("数据源连通测试："))
        self._conn_combo = QComboBox()
        self._conn_combo.addItems(["duckdb", "tushare", "qmt", "akshare"])
        top.addWidget(self._conn_combo)
        self._test_btn = QPushButton("测试连通性")
        self._test_btn.clicked.connect(self._test_connectivity)
        top.addWidget(self._test_btn)
        top.addStretch()
        self._overall_label = QLabel("未刷新")
        top.addWidget(self._overall_label)
        layout.addLayout(top)

        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        layout.addWidget(self._progress)

        # 变量总览表
        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(["变量名", "分组", "状态", "当前值", "说明"])
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        layout.addWidget(self._table, 3)

        # 在线编辑区
        edit_group = QGroupBox("编辑并保存到 .env")
        eg_layout = QFormLayout(edit_group)
        self._edit_key = QComboBox()
        eg_layout.addRow("变量名：", self._edit_key)
        self._edit_value = QLineEdit()
        eg_layout.addRow("新值：", self._edit_value)
        self._save_btn = QPushButton("保存到 .env")
        self._save_btn.clicked.connect(self._save_env)
        eg_layout.addRow("", self._save_btn)
        self._wm_profile_combo = QComboBox()
        self._wm_profile_combo.addItems(["balanced", "conservative", "aggressive"])
        eg_layout.addRow("Q-score模板：", self._wm_profile_combo)
        wm_btn_row = QHBoxLayout()
        self._wm_apply_btn = QPushButton("一键应用模板")
        self._wm_apply_btn.clicked.connect(self._apply_watermark_profile)
        wm_btn_row.addWidget(self._wm_apply_btn)
        self._wm_rollback_btn = QPushButton("回滚上次模板")
        self._wm_rollback_btn.clicked.connect(self._rollback_watermark_profile)
        wm_btn_row.addWidget(self._wm_rollback_btn)
        wm_btn_row.addStretch()
        eg_layout.addRow("", wm_btn_row)
        layout.addWidget(edit_group)

        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setFont(QFont("Consolas", 9))
        self._log.setMaximumHeight(100)
        layout.addWidget(self._log)

    def _refresh(self) -> None:
        self._refresh_btn.setEnabled(False)
        self._progress.setVisible(True)
        self._thread = _ControllerThread(self._ctrl.get_all_env_config)
        self._thread.result_ready.connect(self._on_env_result)
        self._thread.error_occurred.connect(self._on_error)
        self._thread.start()

    def _on_env_result(self, result: dict) -> None:
        self._progress.setVisible(False)
        self._refresh_btn.setEnabled(True)

        summary = result.get("summary", {})
        total = summary.get("total", 0)
        configured = summary.get("configured", 0)
        missing_req = summary.get("missing_required", 0)
        valid = result.get("overall_valid", False)
        self._overall_label.setText(
            f"{'✅ 配置完整' if valid else '⚠️ 缺少必填项'}  "
            f"已配置 {configured}/{total}，缺少必填 {missing_req}"
        )

        groups: dict = result.get("groups", {})
        all_items = []
        editable_keys = []
        env_map: dict[str, str] = {}
        for group_name, items in groups.items():
            for item in items:
                all_items.append((group_name, item))
                if not item.get("sensitive"):
                    editable_keys.append(item["key"])
                env_map[str(item.get("key") or "")] = str(item.get("value") or "")
        self._env_value_map = env_map
        current_profile = self._env_value_map.get("EASYXT_WM_PROFILE", "").strip().lower()
        if current_profile in self._wm_profile_presets:
            idx = self._wm_profile_combo.findText(current_profile)
            if idx >= 0:
                self._wm_profile_combo.setCurrentIndex(idx)

        self._table.setRowCount(len(all_items))
        for row, (group_name, item) in enumerate(all_items):
            status = item.get("status", "missing")
            icon = {"ok": "✅", "missing": "⚠️", "invalid": "❌"}.get(status, "?")
            req_mark = " *" if item.get("required") else ""
            self._table.setItem(row, 0, QTableWidgetItem(item["key"] + req_mark))
            self._table.setItem(row, 1, QTableWidgetItem(group_name))
            self._table.setItem(row, 2, QTableWidgetItem(f"{icon} {status}"))
            self._table.setItem(row, 3, QTableWidgetItem(item.get("value", "")))
            self._table.setItem(row, 4, QTableWidgetItem(item.get("description", "")))

        # 更新可编辑变量下拉框
        current = self._edit_key.currentText()
        self._edit_key.clear()
        self._edit_key.addItems(sorted(set(editable_keys)))
        if current in editable_keys:
            idx = self._edit_key.findText(current)
            if idx >= 0:
                self._edit_key.setCurrentIndex(idx)

    def _test_connectivity(self) -> None:
        source = self._conn_combo.currentText()
        self._test_btn.setEnabled(False)
        self._progress.setVisible(True)
        self._log.appendPlainText(f"[INFO] 测试 {source} 连通性…")
        t = _ControllerThread(self._ctrl.test_datasource_connectivity, source)
        t.result_ready.connect(self._on_connectivity_result)
        t.error_occurred.connect(self._on_error)
        t.start()

    def _on_connectivity_result(self, result: dict) -> None:
        self._progress.setVisible(False)
        self._test_btn.setEnabled(True)
        source = result.get("source", "")
        reachable = result.get("reachable", False)
        latency = result.get("latency_ms", 0.0)
        method = result.get("method", "")
        if reachable:
            self._log.appendPlainText(
                f"[OK]  {source} 可达，延迟 {latency} ms  方法: {method}"
            )
        else:
            err = result.get("error", "不可达")
            self._log.appendPlainText(f"[FAIL] {source} 不可达: {err}")

    def _save_env(self) -> None:
        key = self._edit_key.currentText().strip()
        value = self._edit_value.text().strip()
        if not key:
            QMessageBox.warning(self, "提示", "请选择要修改的变量")
            return
        if not value:
            if QMessageBox.question(
                self, "确认", f"是否确认将 {key} 设置为空字符串？",
                QMessageBox.Yes | QMessageBox.No
            ) != QMessageBox.Yes:
                return
        t = _ControllerThread(self._ctrl.save_env_to_dotenv, key, value)
        t.result_ready.connect(self._on_save_result)
        t.error_occurred.connect(self._on_error)
        t.start()

    def _on_save_result(self, result: dict) -> None:
        if result.get("ok"):
            self._log.appendPlainText(f"[OK]  {result.get('message', '保存成功')}")
            QMessageBox.information(self, "成功", result.get("message", "已保存"))
            try:
                signal_bus.emit(
                    Events.ENV_CONFIG_SAVED,
                    key=self._edit_key.currentText().strip(),
                    source="env_config_tab",
                )
            except Exception:
                pass
            self._refresh()  # 刷新显示
        else:
            self._log.appendPlainText(f"[FAIL] {result.get('error', '保存失败')}")
            QMessageBox.warning(self, "失败", result.get("error", "保存失败"))

    def _on_error(self, msg: str) -> None:
        self._progress.setVisible(False)
        self._refresh_btn.setEnabled(True)
        self._test_btn.setEnabled(True)
        self._log.appendPlainText(f"[FATAL] {msg}")

    def _snapshot_watermark_env(self) -> dict[str, str]:
        keys = set()
        for cfg in self._wm_profile_presets.values():
            keys.update(cfg.keys())
        snapshot: dict[str, str] = {}
        for key in keys:
            snapshot[key] = self._env_value_map.get(key, "")
        return snapshot

    def _apply_watermark_profile(self) -> None:
        profile = self._wm_profile_combo.currentText().strip().lower()
        cfg = self._wm_profile_presets.get(profile)
        if not cfg:
            QMessageBox.warning(self, "失败", f"未知模板: {profile}")
            return
        self._wm_last_snapshot = self._snapshot_watermark_env()
        errors: list[str] = []
        for key, value in cfg.items():
            res = self._ctrl.save_env_to_dotenv(key, value)
            if not res.get("ok"):
                errors.append(str(res.get("error") or f"{key} 保存失败"))
        if errors:
            self._log.appendPlainText(f"[FAIL] 模板应用失败: {'; '.join(errors)}")
            QMessageBox.warning(self, "失败", "\n".join(errors[:5]))
            return
        self._append_watermark_profile_audit(
            action="apply",
            profile=profile,
            before=self._wm_last_snapshot,
            after=cfg,
            success=True,
            message="ok",
        )
        self._log.appendPlainText(f"[OK]  已应用 Q-score 模板: {profile}")
        try:
            signal_bus.emit(Events.ENV_CONFIG_SAVED, key="EASYXT_WM_PROFILE", source="env_config_tab_profile")
        except Exception:
            pass
        self._refresh()

    def _rollback_watermark_profile(self) -> None:
        if not self._wm_last_snapshot:
            QMessageBox.information(self, "提示", "暂无可回滚的模板快照")
            return
        before = self._snapshot_watermark_env()
        errors: list[str] = []
        for key, value in self._wm_last_snapshot.items():
            res = self._ctrl.save_env_to_dotenv(key, value)
            if not res.get("ok"):
                errors.append(str(res.get("error") or f"{key} 回滚失败"))
        if errors:
            self._append_watermark_profile_audit(
                action="rollback",
                profile=self._wm_last_snapshot.get("EASYXT_WM_PROFILE", ""),
                before=before,
                after=self._wm_last_snapshot,
                success=False,
                message="; ".join(errors[:5]),
            )
            self._log.appendPlainText(f"[FAIL] 模板回滚失败: {'; '.join(errors)}")
            QMessageBox.warning(self, "失败", "\n".join(errors[:5]))
            return
        self._append_watermark_profile_audit(
            action="rollback",
            profile=self._wm_last_snapshot.get("EASYXT_WM_PROFILE", ""),
            before=before,
            after=self._wm_last_snapshot,
            success=True,
            message="ok",
        )
        self._log.appendPlainText("[OK]  已回滚到上次模板快照")
        try:
            signal_bus.emit(Events.ENV_CONFIG_SAVED, key="EASYXT_WM_PROFILE", source="env_config_tab_profile_rollback")
        except Exception:
            pass
        self._refresh()

    def _append_watermark_profile_audit(
        self,
        *,
        action: str,
        profile: str,
        before: dict[str, str],
        after: dict[str, str],
        success: bool,
        message: str,
    ) -> None:
        payload = {
            "ts": __import__("datetime").datetime.now().isoformat(timespec="seconds"),
            "action": str(action),
            "profile": str(profile),
            "success": bool(success),
            "message": str(message),
            "before": before,
            "after": after,
            "source": "env_config_tab",
        }
        try:
            os.makedirs(os.path.dirname(self._wm_profile_audit_path), exist_ok=True)
            with open(self._wm_profile_audit_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            pass


# ─── Tab 10：实时链路监控 ─────────────────────────────────────────────────


class _RealtimeMonitorTab(QWidget):
    """实时数据链路监控 Tab。

    通过 signal_bus 被动接收 REALTIME_PIPELINE_STATUS_UPDATED 事件，
    同时支持手动调用 DataManagerController.get_realtime_pipeline_info() 轮询。

    核心展示：
        - 链路状态卡片（连接 / 降级 / 断开）
        - 事件流表格（最近 200 条推送记录）
    """

    _MAX_EVENTS = 200  # 事件流最大保留条数

    def __init__(self, controller: DataManagerController, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._thread: _ControllerThread | None = None
        self._event_rows: list[tuple[str, str, str, str]] = []  # (ts, state, symbol, note)
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)

        # ── 顶部控制栏 ──
        top = QHBoxLayout()
        self._refresh_btn = QPushButton("手动刷新")
        self._refresh_btn.clicked.connect(self._refresh)
        top.addWidget(self._refresh_btn)
        self._clear_btn = QPushButton("清空事件流")
        self._clear_btn.clicked.connect(self._clear_events)
        top.addWidget(self._clear_btn)
        top.addStretch()
        self._conn_label = QLabel("链路状态：待检测")
        font = QFont()
        font.setBold(True)
        self._conn_label.setFont(font)
        top.addWidget(self._conn_label)
        layout.addLayout(top)

        # ── 状态卡片 ──
        card_group = QGroupBox("实时链路状态")
        card_form = QFormLayout(card_group)
        self._lbl_connected = QLabel("—")
        self._lbl_degraded = QLabel("—")
        self._lbl_symbol = QLabel("—")
        self._lbl_quote_ts = QLabel("—")
        self._lbl_reason = QLabel("—")
        self._lbl_drop_rate = QLabel("—")
        card_form.addRow("连接状态：", self._lbl_connected)
        card_form.addRow("是否降级：", self._lbl_degraded)
        card_form.addRow("当前标的：", self._lbl_symbol)
        card_form.addRow("最近 Quote：", self._lbl_quote_ts)
        card_form.addRow("断线原因：", self._lbl_reason)
        card_form.addRow("丢帧率：", self._lbl_drop_rate)
        layout.addWidget(card_group)

        # ── 事件流 ──
        layout.addWidget(QLabel("事件流（最近 200 条）："))
        self._event_table = QTableWidget(0, 4)
        self._event_table.setHorizontalHeaderLabels(["时间", "状态", "标的", "备注"])
        self._event_table.horizontalHeader().setStretchLastSection(True)
        self._event_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._event_table.setAlternatingRowColors(True)
        layout.addWidget(self._event_table, 1)

    # ── 被动接收（来自 DataGovernancePanel 的 signal_bus 转发） ──

    def on_pipeline_event(self, **payload) -> None:
        """DataGovernancePanel 收到 REALTIME_PIPELINE_STATUS_UPDATED 时调用。"""
        import time as _time
        ts = _time.strftime("%H:%M:%S")
        connected = payload.get("connected")
        degraded = bool(payload.get("degraded", False))
        symbol = str(payload.get("symbol") or "")
        reason = str(payload.get("reason") or "")
        quote_ts = str(payload.get("quote_ts") or "")
        drop_rate = payload.get("drop_rate")

        # 更新卡片
        if connected is True:
            state_text = "⚠️ 降级中" if degraded else "✅ 已连接"
            self._lbl_connected.setText("已连接")
            self._conn_label.setText(f"链路状态：{'降级中 ⚠️' if degraded else '已连接 ✅'}")
        elif connected is False:
            state_text = "❌ 未连接"
            self._lbl_connected.setText("未连接")
            self._conn_label.setText("链路状态：未连接 ⚠️")
        else:
            state_text = "? 未知"
            self._lbl_connected.setText("未知")
            self._conn_label.setText("链路状态：未知")

        self._lbl_degraded.setText("是" if degraded else "否")
        self._lbl_symbol.setText(symbol or "—")
        self._lbl_quote_ts.setText(quote_ts or "—")
        self._lbl_reason.setText(reason or "—")
        if drop_rate is not None:
            self._lbl_drop_rate.setText(f"{float(drop_rate) * 100:.2f}%")

        note = f"reason={reason}" if reason else (f"quote={quote_ts}" if quote_ts else "")
        self._append_event(ts, state_text, symbol, note)

    def _append_event(self, ts: str, state: str, symbol: str, note: str) -> None:
        self._event_rows.append((ts, state, symbol, note))
        if len(self._event_rows) > self._MAX_EVENTS:
            self._event_rows = self._event_rows[-self._MAX_EVENTS:]
        self._event_table.setRowCount(len(self._event_rows))
        row = len(self._event_rows) - 1
        self._event_table.setItem(row, 0, QTableWidgetItem(ts))
        self._event_table.setItem(row, 1, QTableWidgetItem(state))
        self._event_table.setItem(row, 2, QTableWidgetItem(symbol))
        self._event_table.setItem(row, 3, QTableWidgetItem(note))
        self._event_table.scrollToBottom()

    def _clear_events(self) -> None:
        self._event_rows.clear()
        self._event_table.setRowCount(0)

    # ── 主动轮询 ──

    def _refresh(self) -> None:
        self._refresh_btn.setEnabled(False)
        self._thread = _ControllerThread(self._ctrl.get_realtime_pipeline_info)
        self._thread.result_ready.connect(self._on_poll_result)
        self._thread.error_occurred.connect(self._on_error)
        self._thread.start()

    def _on_poll_result(self, result: dict) -> None:
        self._refresh_btn.setEnabled(True)
        if result.get("error"):
            self._conn_label.setText(f"链路状态：查询失败 — {result['error'][:40]}")
            return
        self.on_pipeline_event(**result)

    def _on_error(self, msg: str) -> None:
        self._refresh_btn.setEnabled(True)
        self._conn_label.setText(f"链路状态：错误 — {msg[:40]}")


# ─── Tab 12：数据来源溯源面板 ──────────────────────────────────────────────


class _TraceabilityTab(QWidget):
    """逐标的数据入库溯源面板。

    展示 data_ingestion_status 表中每只股票/每个周期的：
        - 实际数据来源（duckdb / dat / qmt / tushare / akshare）
        - 入库状态（success / error）
        - 记录行数
        - 数据时间跨度
        - 入库时间
        - 批次 ID (ingest_run_id)

    支持按标的代码和周期筛选，60s 自动刷新可选。
    """

    _SOURCE_COLORS: dict[str, str] = {
        "duckdb": "#4CAF50",
        "dat": "#2196F3",
        "qmt": "#FF9800",
        "tushare": "#9C27B0",
        "akshare": "#00BCD4",
    }

    def __init__(self, controller: DataManagerController, parent=None):
        super().__init__(parent)
        self._ctrl = controller
        self._thread: _ControllerThread | None = None
        self._auto_timer = QTimer(self)
        self._auto_timer.timeout.connect(self._refresh)
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)

        # ── 筛选栏 ──
        filter_bar = QHBoxLayout()
        filter_bar.addWidget(QLabel("标的代码:"))
        self._stock_input = QLineEdit()
        self._stock_input.setPlaceholderText("如 000001.SZ（留空查全部）")
        self._stock_input.setMaximumWidth(200)
        filter_bar.addWidget(self._stock_input)

        filter_bar.addWidget(QLabel("周期:"))
        self._period_combo = QComboBox()
        self._period_combo.addItems(["全部", "1d", "1m", "5m", "tick"])
        self._period_combo.setMaximumWidth(100)
        filter_bar.addWidget(self._period_combo)

        self._refresh_btn = QPushButton("查询溯源")
        self._refresh_btn.clicked.connect(self._refresh)
        filter_bar.addWidget(self._refresh_btn)

        self._auto_btn = QPushButton("自动刷新(60s)")
        self._auto_btn.setCheckable(True)
        self._auto_btn.toggled.connect(self._toggle_auto)
        filter_bar.addWidget(self._auto_btn)

        filter_bar.addStretch()
        self._summary_label = QLabel("就绪")
        filter_bar.addWidget(self._summary_label)
        layout.addLayout(filter_bar)

        # ── 溯源表格 ──
        self._table = QTableWidget(0, 8)
        self._table.setHorizontalHeaderLabels([
            "标的代码", "周期", "数据来源", "状态",
            "行数", "起始日期", "结束日期", "入库时间",
        ])
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.setAlternatingRowColors(True)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        layout.addWidget(self._table)

        # ── 来源统计摘要 ──
        self._stats_area = QPlainTextEdit()
        self._stats_area.setReadOnly(True)
        self._stats_area.setMaximumHeight(120)
        self._stats_area.setFont(QFont("Consolas", 9))
        layout.addWidget(self._stats_area)

    def _toggle_auto(self, checked: bool) -> None:
        if checked:
            self._auto_timer.start(60_000)
            self._auto_btn.setText("停止自动刷新")
            self._refresh()
        else:
            self._auto_timer.stop()
            self._auto_btn.setText("自动刷新(60s)")

    def _refresh(self) -> None:
        stock = self._stock_input.text().strip() or None
        period_text = self._period_combo.currentText()
        period = None if period_text == "全部" else period_text
        self._refresh_btn.setEnabled(False)
        self._thread = _ControllerThread(
            self._ctrl.get_ingestion_traceability,
            stock_code=stock,
            period=period,
        )
        self._thread.result_ready.connect(self._on_result)
        self._thread.error_occurred.connect(self._on_error)
        self._thread.start()

    def _on_result(self, result: dict) -> None:
        self._refresh_btn.setEnabled(True)
        if result.get("error"):
            self._stats_area.setPlainText(f"[ERROR] {result['error']}")
            self._summary_label.setText("查询失败")
            return

        records = result.get("records", [])
        self._table.setRowCount(len(records))
        source_counts: dict[str, int] = {}
        status_counts: dict[str, int] = {}

        for row, rec in enumerate(records):
            src = str(rec.get("source", ""))
            status = str(rec.get("status", ""))
            source_counts[src] = source_counts.get(src, 0) + 1
            status_counts[status] = status_counts.get(status, 0) + 1

            self._table.setItem(row, 0, QTableWidgetItem(str(rec.get("stock_code", ""))))
            self._table.setItem(row, 1, QTableWidgetItem(str(rec.get("period", ""))))

            source_item = QTableWidgetItem(src)
            color = self._SOURCE_COLORS.get(src)
            if color:
                from PyQt5.QtGui import QColor
                source_item.setForeground(QColor(color))
                source_item.setFont(QFont("", -1, QFont.Bold))
            self._table.setItem(row, 2, source_item)

            status_item = QTableWidgetItem(status)
            if status == "error":
                from PyQt5.QtGui import QColor
                status_item.setForeground(QColor("#F44336"))
            self._table.setItem(row, 3, status_item)

            self._table.setItem(row, 4, QTableWidgetItem(str(rec.get("record_count", ""))))
            self._table.setItem(row, 5, QTableWidgetItem(str(rec.get("start_date", ""))[:10]))
            self._table.setItem(row, 6, QTableWidgetItem(str(rec.get("end_date", ""))[:10]))
            self._table.setItem(row, 7, QTableWidgetItem(str(rec.get("last_updated", ""))[:19]))

        self._summary_label.setText(f"共 {len(records)} 条溯源记录")

        # 生成统计摘要
        lines = ["═══ 数据来源分布 ═══"]
        for src, cnt in sorted(source_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {src:<12s}  {cnt} 条")
        lines.append("")
        lines.append("═══ 入库状态分布 ═══")
        for st, cnt in sorted(status_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {st:<12s}  {cnt} 条")
        self._stats_area.setPlainText("\n".join(lines))

    def _on_error(self, msg: str) -> None:
        self._refresh_btn.setEnabled(True)
        self._stats_area.setPlainText(f"[FATAL] {msg}")
        self._summary_label.setText("查询异常")
