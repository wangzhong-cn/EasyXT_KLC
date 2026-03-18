#!/usr/bin/env python3
"""
EasyXT量化交易策略管理平台
基于PyQt5的专业量化交易策略参数设置和管理界面
用于策略开发、参数配置、实时监控和交易执行
"""

import importlib
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Optional, cast
from urllib import request

# 强制设置Matplotlib后端为Agg，防止在GUI线程中初始化交互式后端导致死锁
try:
    import matplotlib
    matplotlib.use("Agg")
except ImportError:
    pass

# 修复WebEngine初始化问题：必须在QApplication创建前导入
try:
    from PyQt5 import QtWebEngineWidgets
except ImportError:
    pass

from PyQt5.QtCore import QCoreApplication, QProcess, QProcessEnvironment, QSettings, Qt, QThread, QTimer, QUrl, pyqtSignal
from PyQt5.QtGui import QDesktopServices, QFont
from PyQt5.QtWidgets import (
    QApplication,
    QLabel,
    QMainWindow,
    QMessageBox,
    QStatusBar,
    QSplitter,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)
project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_path not in sys.path:
    sys.path.insert(0, project_path)

from gui_app.widgets.chart.trading_hours_guard import TradingHoursGuard


def _ensure_writable_duckdb_env() -> None:
    try:
        from data_manager.duckdb_connection_pool import resolve_duckdb_path

        resolved = resolve_duckdb_path()
    except Exception:
        resolved = os.environ.get("EASYXT_DUCKDB_PATH", "")
    if not resolved:
        return
    parent = os.path.dirname(resolved) or "."
    probe = os.path.join(parent, ".easyxt_gui_write_probe.tmp")
    try:
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        try:
            os.remove(probe)
        except OSError:
            pass
        return
    except OSError:
        fallback = os.path.join(project_path, "data", "stock_data.ddb")
        os.makedirs(os.path.dirname(fallback), exist_ok=True)
        os.environ["EASYXT_DUCKDB_PATH"] = fallback

Events = importlib.import_module("core.events").Events
signal_bus = importlib.import_module("core.signal_bus").signal_bus
ThemeManager = importlib.import_module("core.theme_manager").ThemeManager
try:
    engine_status_ui_module = importlib.import_module("gui_app.backtest.engine_status_ui")
    format_engine_status_ui = getattr(engine_status_ui_module, "format_engine_status_ui")
    build_engine_status_detail = getattr(engine_status_ui_module, "build_engine_status_detail")
    format_engine_status_log = getattr(engine_status_ui_module, "format_engine_status_log")
except Exception:
    def format_engine_status_ui(status: dict | None, label_prefix: str = "回测引擎") -> dict[str, str]:
        return {
            "text": f"{label_prefix}: 状态未知 ❓",
            "color": "#666666",
            "tooltip": "状态格式化模块不可用",
        }

    def build_engine_status_detail(status: dict | None) -> str:
        return f"状态详情不可用: {status}"

    def format_engine_status_log(status: dict | None, prefix: str = "BACKTEST_ENGINE") -> str:
        return f"[{prefix}] level=WARN mode=unknown available=None message=状态格式化模块不可用"

EASYXT_AVAILABLE = False
easy_xt = None


def check_easyxt_available():
    global EASYXT_AVAILABLE, easy_xt
    if easy_xt is not None:
        return EASYXT_AVAILABLE
    try:
        import easy_xt

        EASYXT_AVAILABLE = True
    except Exception:
        EASYXT_AVAILABLE = False
    return EASYXT_AVAILABLE


class LazyTabLoader:
    """
    懒加载标签页管理器

    功能：
    1. 首次点击标签页时才加载内容
    2. 加载过程中显示占位符
    3. 加载失败显示错误提示
    4. 已加载的标签页缓存复用
    """

    def __init__(self, tab_widget: QTabWidget):
        self._tab_widget = tab_widget
        self._loaded_tabs: dict[int, QWidget] = {}  # index -> widget
        self._loading_tabs: dict[int, bool] = {}  # index -> is_loading
        self._logger = logging.getLogger(__name__)

        # 连接标签切换信号
        self._tab_widget.currentChanged.connect(self._on_tab_changed)

    def _on_tab_changed(self, index: int):
        """标签页切换回调"""
        if index < 0:
            return

        # 如果已经加载或有加载中，跳过
        if index in self._loaded_tabs or self._loading_tabs.get(index, False):
            return

        # 触发懒加载
        self._load_tab(index)

    def _load_tab(self, index: int):
        """加载标签页"""
        self._loading_tabs[index] = True
        self._logger.info(f"懒加载标签页: {index}")

        # 在主线程中延迟加载（使用 QTimer 避免阻塞）
        QTimer.singleShot(0, lambda: self._do_load_tab(index))

    def _do_load_tab(self, index: int):
        """执行标签页加载"""
        try:
            # 获取标签页名称
            tab_name = self._tab_widget.tabText(index)

            # 根据名称加载对应的组件
            widget = self._create_tab_widget(tab_name)

            if widget is not None:
                # 替换占位符
                self._tab_widget.removeTab(index)
                self._tab_widget.insertTab(index, widget, tab_name)
                self._tab_widget.setCurrentIndex(index)

                # 缓存已加载的标签页
                self._loaded_tabs[index] = widget
                self._logger.info(f"标签页加载成功: {tab_name}")
            else:
                self._logger.warning(f"标签页创建返回None: {tab_name}")

        except Exception as e:
            self._logger.error(f"标签页加载失败: {e}", exc_info=True)
            tab_name = self._tab_widget.tabText(index)
            error_widget = QLabel(f"{tab_name} 加载失败: {e}")
            error_widget.setAlignment(Qt.AlignCenter)
            error_widget.setStyleSheet("color: #b91c1c; font-size: 13px;")
            self._tab_widget.removeTab(index)
            self._tab_widget.insertTab(index, error_widget, tab_name)
            self._tab_widget.setCurrentIndex(index)
            self._loaded_tabs[index] = error_widget

        finally:
            self._loading_tabs[index] = False

    def _create_tab_widget(self, tab_name: str) -> Optional[QWidget]:
        """根据标签页名称创建对应的组件"""
        tab_creators = {
            "专业图表工作台": self._create_chart_workspace,
            "回测分析": self._create_backtest,
            "网格交易": self._create_grid_trading,
            "条件单": self._create_conditional_order,
            "JQ2QMT": self._create_jq2qmt,
            "JQ转Ptrade": self._create_jq_to_ptrade,
            "数据管理": self._create_data_manager,
            "因子分析": self._create_factor_analysis,
            "策略管理": self._create_strategy_management,
        }

        creator = tab_creators.get(tab_name)
        if creator:
            return creator()

        # 默认返回空占位符
        return QLabel(f"功能开发中: {tab_name}")

    def _create_chart_workspace(self) -> QWidget:
        """创建图表工作台"""
        from gui_app.widgets.chart_workspace import ChartWorkspace

        return ChartWorkspace()

    def _create_backtest(self) -> QWidget:
        """创建回测分析"""
        try:
            from gui_app.widgets.backtest_widget import BacktestWidget

            return BacktestWidget()
        except Exception as e:
            return QLabel(f"回测分析加载失败: {e}")

    def _create_grid_trading(self) -> QWidget:
        """创建网格交易"""
        try:
            from gui_app.widgets.grid_trading_widget import GridTradingWidget

            return GridTradingWidget()
        except Exception as e:
            return QLabel(f"网格交易加载失败: {e}")

    def _create_conditional_order(self) -> QWidget:
        """创建条件单"""
        try:
            from gui_app.widgets.conditional_order_widget import ConditionalOrderWidget

            return ConditionalOrderWidget()
        except Exception as e:
            return QLabel(f"条件单加载失败: {e}")

    def _create_jq2qmt(self) -> QWidget:
        """创建JQ2QMT"""
        try:
            from gui_app.widgets.jq2qmt_widget import JQ2QMTWidget

            return JQ2QMTWidget()
        except Exception as e:
            return QLabel(f"JQ2QMT加载失败: {e}")

    def _create_jq_to_ptrade(self) -> QWidget:
        """创建JQ转Ptrade"""
        try:
            from gui_app.widgets.jq_to_ptrade_widget import JQToPtradeWidget

            return JQToPtradeWidget()
        except Exception as e:
            return QLabel(f"JQ转Ptrade加载失败: {e}")

    def _create_data_manager(self) -> QWidget:
        """创建统一数据治理面板（10 Tab：下载 / 质检 / 路由 / 管道状态 / 查询 / 对账 / 日历 / 修复 / 维护 / 环境配置）"""
        try:
            from gui_app.widgets.data_governance_panel import DataGovernancePanel

            return DataGovernancePanel()
        except Exception as e:
            return QLabel(f"数据管理加载失败: {e}")

    def _create_factor_analysis(self) -> QWidget:
        """创建因子分析面板"""
        try:
            from gui_app.widgets.factor_widget import FactorWidget

            return FactorWidget()
        except Exception as e:
            return QLabel(f"因子分析加载失败: {e}")

    def _create_strategy_management(self) -> QWidget:
        """创建策略管理全量面板（8 Tab：列表/配置/结果/绩效/风险/优化/对比/生命周期）"""
        try:
            from gui_app.widgets.strategy_governance_panel import StrategyGovernancePanel

            return StrategyGovernancePanel()
        except Exception as e:
            return QLabel(f"策略管理加载失败: {e}")

    def preload_tabs(self, indices: list):
        """预加载指定的标签页"""
        for index in indices:
            if index not in self._loaded_tabs and not self._loading_tabs.get(index, False):
                self._load_tab(index)

    def get_loaded_widget(self, index: int) -> Optional[QWidget]:
        """获取已加载的标签页组件"""
        return self._loaded_tabs.get(index)


class ConnectionCheckThread(QThread):
    result = pyqtSignal(bool)

    def run(self):
        try:
            try:
                import easy_xt
            except Exception:
                self.result.emit(False)
                return
            # get_api() 内部已自行使用 _xt_init_lock，无需再手动获取，
            # 否则会因 Lock 非重入导致自死锁（㊵修复）。
            try:
                api = easy_xt.get_api()
            except Exception:
                self.result.emit(False)
                return
            if os.environ.get("EASYXT_ENABLE_ACTIVE_PROBE", "0") not in ("1", "true", "True"):
                probe_mode = "passive"
            else:
                probe_mode = os.environ.get("EASYXT_CONNECTION_PROBE_MODE", "passive").strip().lower()
            if probe_mode not in ("active", "safe"):
                probe_mode = "passive"
            if probe_mode == "passive":
                self.result.emit(True)
                return
            # 轻量检测：避免每30秒反复 init_data() 导致锁竞争和实时链路抖动
            # 优先走 xtquant_broker 的 full_tick（单次调用、低成本）
            # Fix 58: 使用非阻塞锁获取，若 realtime worker 正在使用则视为连接正常
            test_codes = ["000001.SZ", "511090.SH"]
            if probe_mode == "active":
                try:
                    broker = easy_xt.get_xtquant_broker()
                    lock = getattr(broker, "_call_lock", None)
                    if lock is not None and not lock.acquire(timeout=0.3):
                        self.result.emit(True)
                        return
                    try:
                        for code in test_codes:
                            tick = broker.get_full_tick([code]) or {}
                            info = tick.get(code) if isinstance(tick, dict) else None
                            if isinstance(info, dict):
                                price = float(
                                    info.get("lastPrice")
                                    or info.get("last_price")
                                    or info.get("price")
                                    or 0
                                )
                                if price > 0:
                                    self.result.emit(True)
                                    return
                    finally:
                        if lock is not None:
                            try:
                                lock.release()
                            except RuntimeError:
                                pass
                except Exception:
                    pass

            # 兜底：使用 data.get_current_price，不再调用 init_data()
            if hasattr(api, "data"):
                for code in test_codes:
                    try:
                        price_df = api.data.get_current_price([code])
                        if price_df is not None and getattr(price_df, "empty", False) is False:
                            self.result.emit(True)
                            return
                    except Exception:
                        continue
            self.result.emit(False)
        except Exception:
            self.result.emit(False)


class MainWindow(QMainWindow):
    """主窗口"""

    def __init__(self):
        super().__init__()
        _ensure_writable_duckdb_env()
        # _logger 必须在所有调用 _p() 的代码之前初始化
        self._logger = logging.getLogger(__name__)
        os.environ["EASYXT_CONNECTION_PROBE_MODE"] = "passive"
        os.environ.setdefault("EASYXT_ENABLE_XTDATA_QUOTE_PROBE", "1")
        os.environ.setdefault("EASYXT_RT_XTDATA_ONLY", "0")
        os.environ.setdefault("EASYXT_ENABLE_XT_LISTING_DATE", "0")
        os.environ.setdefault("EASYXT_ENABLE_QMT_ONLINE", "1")
        in_session, _session_name = TradingHoursGuard.current_session()
        if in_session:
            os.environ["EASYXT_RT_XTDATA_ONLY"] = "1"
        self.executor_thread = None
        self.signal_bus = signal_bus
        self.service_process = None
        self._closing = False
        self._check_thread = None
        self._check_fail_count = 0
        self._check_base_interval = 30000
        self._service_restart_count = 0
        self._service_start_ts = 0.0
        self._SERVICE_MAX_RESTARTS = 5
        self._SERVICE_RESTART_BACKOFF = [2000, 4000, 8000, 16000, 30000]
        self._service_circuit_broken = False  # 熔断标志：True时禁止自动重启
        self._service_restart_scheduled = False
        self._service_external_manager = False
        self._service_log_last_ts = 0.0
        self._service_log_suppressed = 0
        self._service_log_suppressed_total = 0  # 生命周期累计抑制批次数
        self._service_lock_conflict = False
        self._service_diag_summary: dict[str, object] = {}
        self._profile_enabled = os.environ.get("EASYXT_PROFILE_STARTUP", "1") not in (
            "0",
            "false",
            "False",
        )
        self._t0 = time.perf_counter()
        self._marks = {}
        self._health_check_results = {}
        self._health_check_stage = "startup"
        self._last_health_check_ts = 0.0
        self._backtest_engine_status = {
            "available": None,
            "mode": "unknown",
            "error_type": None,
            "error_message": None,
            "hint": "等待回测页上报",
        }
        self._realtime_pipeline_status = {
            "connected": None,
            "reason": "等待图表模块上报",
            "quote_ts": None,
            "symbol": "",
            "source": "",
        }
        self._release_gate_status = {
            "strict_gate_pass": None,
            "P0_open_count": None,
            "active_critical_high": None,
            "duckdb_write_probe_detail": {},
            "intraday_bar_semantic_detail": {},
            "governance_nightly_detail": {},
            "watermark_quality_detail": {},
            "watermark_profile_audit_detail": {},
            "watermark_profile_approval_detail": {},
        }
        self._last_backtest_engine_log: Optional[str] = None
        self._last_realtime_probe_log: Optional[str] = None
        self._watchdog_gap_buffer = deque(maxlen=int(os.environ.get("EASYXT_WATCHDOG_BUFFER_SIZE", "240")))
        self._watchdog_stats_last_emit = time.monotonic()
        self._watchdog_stats_interval_s = float(os.environ.get("EASYXT_WATCHDOG_STATS_INTERVAL_S", "60"))
        self._watchdog_slo_p99_s = float(os.environ.get("EASYXT_WATCHDOG_P99_SLO_S", "1.2"))
        self._watchdog_log_path = os.path.join(project_path, "logs", "main_thread_latency.log")
        self._thread_watermark_threshold = int(os.environ.get("EASYXT_THREAD_WATERMARK", "180"))
        self._thread_watermark_log_path = os.path.join(project_path, "logs", "thread_watermark.log")
        self._thread_watermark_timer: Optional[QTimer] = None
        self.chart_workspace: Optional[QWidget] = None
        self.module_tab_widget: Optional[QWidget] = None
        self.theme_manager = ThemeManager()
        app = QApplication.instance()
        if app is not None:
            self.theme_manager.apply(app, "dark")
        self.signal_bus.subscribe(
            Events.BACKTEST_ENGINE_STATUS_UPDATED, self._on_backtest_engine_status_updated
        )
        self.signal_bus.subscribe(
            Events.REALTIME_PIPELINE_STATUS_UPDATED, self._on_realtime_pipeline_status_updated
        )
        self.signal_bus.subscribe(Events.DATA_QUALITY_ALERT, self._on_data_quality_alert)
        self.signal_bus.subscribe(Events.DATA_REPAIRED, self._on_data_repaired)
        self.signal_bus.subscribe(Events.ENV_CONFIG_SAVED, self._on_env_config_saved)
        self._startup_duckdb_health_gate()
        self._p("t0")
        self.init_ui()
        self._p("ui-initialized")
        # 延迟健康检查，避免 DuckDB 查询阻塞启动主线程
        QTimer.singleShot(800, lambda: self._run_health_checks(stage="startup"))
        self._schedule_health_recheck()
        self._schedule_backtest_engine_probe()
        self._init_alerts_rollup()
        QTimer.singleShot(60000, self._emit_stability_summary)  # 60s 稳定性摘要
        QTimer.singleShot(120000, self._emit_service_log_diagnostics)

    def _schedule_backtest_engine_probe(self):
        # 统一探测：不依赖打开回测页，主窗口启动后主动上报一次回测引擎状态
        QTimer.singleShot(1200, self._probe_backtest_engine_status)

    def _startup_duckdb_health_gate(self):
        gate_result: dict[str, object] = {"status": "unknown"}
        try:
            from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

            db_path = resolve_duckdb_path()
            mgr = get_db_manager(db_path)
            wal_repaired = bool(mgr.repair_wal_if_needed())
            checkpoint_ok = bool(mgr.checkpoint()) if os.path.exists(db_path) else False
            gate_result = {
                "status": "ok" if checkpoint_ok or (not os.path.exists(db_path)) else "warning",
                "code": "startup_gate",
                "path": db_path,
                "wal_repaired": wal_repaired,
                "checkpoint_ok": checkpoint_ok,
            }
        except Exception as exc:
            gate_result = {
                "status": "warning",
                "code": "startup_gate_failed",
                "message": str(exc)[:180],
            }
        self._health_check_results["duckdb_gate"] = gate_result

    def _init_alerts_rollup(self):
        self._alerts_settings = QSettings("EasyXT", "AlertsMonitor")
        self._alerts_log_offset = int(self._alerts_settings.value("alerts/log_offset", 0))
        self._alerts_last_archive_date = self._alerts_settings.value("alerts/last_archive_date", "")
        self._alerts_rollup_timer = QTimer(self)
        interval = int(os.environ.get("EASYXT_ALERTS_ROLLUP_MS", "60000"))
        self._alerts_rollup_timer.setInterval(max(10000, interval))
        self._alerts_rollup_timer.timeout.connect(self._rollup_alerts_log)
        self._alerts_rollup_timer.start()

    def _rollup_alerts_log(self):
        threading.Thread(target=self._rollup_alerts_log_worker, daemon=True).start()

    def _rollup_alerts_log_worker(self):
        log_dir = os.path.join(project_path, "logs")
        alerts_path = os.path.join(log_dir, "alerts.log")
        if not os.path.exists(alerts_path):
            return
        self._archive_alerts_log(alerts_path, log_dir)
        try:
            current_size = os.path.getsize(alerts_path)
            if self._alerts_log_offset > current_size:
                self._alerts_log_offset = 0
            with open(alerts_path, "r", encoding="utf-8") as f:
                f.seek(self._alerts_log_offset)
                data = f.read()
                self._alerts_log_offset = f.tell()
        except Exception:
            return
        lines = [line for line in data.splitlines() if line.strip()]
        if not lines:
            self._save_alerts_state()
            return
        type_counts: dict[str, int] = {}
        mode_counts: dict[str, int] = {}
        for line in lines:
            parts = line.split("\t")
            if len(parts) >= 2:
                type_counts[parts[1]] = type_counts.get(parts[1], 0) + 1
            if len(parts) >= 3:
                mode_counts[parts[2]] = mode_counts.get(parts[2], 0) + 1
        summary = {
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(lines),
            "by_type": type_counts,
            "by_mode": mode_counts,
        }
        summary_path = os.path.join(log_dir, "alerts_summary.log")
        try:
            with open(summary_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(summary, ensure_ascii=False) + "\n")
        except Exception:
            pass
        self._send_alerts_notification(summary)
        self._send_alerts_to_dashboard(summary)
        self._save_alerts_state()

    def _archive_alerts_log(self, alerts_path: str, log_dir: str):
        today = datetime.now().strftime("%Y%m%d")
        if not self._alerts_last_archive_date:
            self._alerts_last_archive_date = today
            self._save_alerts_state()
            return
        if today == self._alerts_last_archive_date:
            return
        try:
            if os.path.getsize(alerts_path) > 0:
                archive_dir = os.path.join(log_dir, "archive")
                os.makedirs(archive_dir, exist_ok=True)
                archive_name = f"alerts_{self._alerts_last_archive_date}.log"
                archive_path = os.path.join(archive_dir, archive_name)
                if os.path.exists(archive_path):
                    suffix = datetime.now().strftime("%H%M%S")
                    archive_path = os.path.join(
                        archive_dir, f"alerts_{self._alerts_last_archive_date}_{suffix}.log"
                    )
                os.replace(alerts_path, archive_path)
                self._alerts_log_offset = 0
        except Exception:
            pass
        self._alerts_last_archive_date = today
        self._save_alerts_state()

    def _send_alerts_notification(self, summary: dict[str, object]):
        url = os.environ.get("EASYXT_ALERTS_WEBHOOK_URL", "").strip()
        if not url:
            return
        payload = json.dumps(summary, ensure_ascii=False).encode("utf-8")

        def _send():
            req = request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            try:
                request.urlopen(req, timeout=5)
            except Exception:
                pass

        threading.Thread(target=_send, daemon=True).start()

    def _send_alerts_to_dashboard(self, summary: dict[str, object]):
        base = os.environ.get("EASYXT_MONITOR_DASHBOARD_URL", "").strip()
        if not base:
            return
        if base.endswith("/api/alerts/ingest"):
            url = base
        else:
            url = base.rstrip("/") + "/api/alerts/ingest"
        payload = json.dumps(summary, ensure_ascii=False).encode("utf-8")

        def _send():
            req = request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            try:
                request.urlopen(req, timeout=5)
            except Exception:
                pass

        threading.Thread(target=_send, daemon=True).start()

    def _save_alerts_state(self):
        try:
            self._alerts_settings.setValue("alerts/log_offset", int(self._alerts_log_offset))
            self._alerts_settings.setValue("alerts/last_archive_date", self._alerts_last_archive_date)
        except Exception:
            pass

    def _probe_backtest_engine_status(self):
        # 回测引擎探测包含重量级 import (backtrader 等)，推到后台线程（㊷修复）
        threading.Thread(target=self._probe_backtest_engine_status_bg, daemon=True).start()

    def _probe_backtest_engine_status_bg(self):
        status = {
            "available": False,
            "mode": "unknown",
            "error_type": None,
            "error_message": None,
            "hint": "主窗口探测中",
        }
        try:
            engine_module = importlib.import_module("gui_app.backtest.engine")
            getter = getattr(engine_module, "get_backtrader_import_status", None)
            if callable(getter):
                status = getter() or status
            else:
                engine_cls = getattr(engine_module, "AdvancedBacktestEngine", None)
                available = engine_cls is not None
                status = {
                    "available": available,
                    "mode": "backtrader" if available else "mock",
                    "error_type": None,
                    "error_message": None,
                    "hint": "状态接口不可用，使用兼容判定",
                }
        except Exception as e:
            status = {
                "available": False,
                "mode": "unknown",
                "error_type": type(e).__name__,
                "error_message": str(e),
                "hint": "主窗口探测失败，请打开回测页查看详情",
            }
        self.signal_bus.emit(Events.BACKTEST_ENGINE_STATUS_UPDATED, status=status, source="main_window")

    def _run_health_checks(self, stage: str = "runtime"):
        """健康检查：DuckDB/easyxt 在后台线程执行，chart 在主线程"""
        self._health_check_stage = stage
        # chart 检查是纯 UI 读属性，留在主线程
        try:
            self._health_check_results["chart"] = self._check_chart()
        except Exception as e:
            self._health_check_results["chart"] = {"status": "error", "message": str(e)}
        # DuckDB 和 easyxt 检查推到后台线程
        threading.Thread(
            target=self._run_health_checks_bg,
            args=(stage,),
            daemon=True,
        ).start()

    def _run_health_checks_bg(self, stage: str):
        """后台执行 DuckDB + easyxt + pipeline 检查，完成后回主线程更新 UI"""
        bg_checks = [
            ("duckdb", self._check_duckdb),
            ("easyxt", self._check_easyxt),
            ("pipeline", self._check_pipeline),
        ]
        for name, check_fn in bg_checks:
            try:
                result = check_fn()
                self._health_check_results[name] = result
            except Exception as e:
                self._health_check_results[name] = {"status": "error", "message": str(e)}
        self._last_health_check_ts = time.time()
        # 日志输出可在后台线程
        self._log_health_summary()
        # UI 标签更新必须回主线程
        QTimer.singleShot(0, self._update_health_status_label)

    def _schedule_health_recheck(self):
        # 二次健康检查：等待懒加载/异步初始化后再检查一次
        QTimer.singleShot(1800, lambda: self._run_health_checks(stage="post-lazy"))

    def _check_duckdb(self):
        try:
            from data_manager.duckdb_connection_pool import resolve_duckdb_path

            path = resolve_duckdb_path()
            wal_path = f"{path}.wal"
            wal_exists = os.path.exists(wal_path)
            if not os.path.exists(path):
                return {
                    "status": "missing",
                    "code": "db_missing",
                    "path": path,
                    "wal_exists": wal_exists,
                }
            from data_manager.duckdb_connection_pool import get_db_manager
            with get_db_manager(path).get_read_connection() as con:
                try:
                    row = con.execute("SELECT COUNT(*) FROM stock_daily").fetchone()
                    cnt = int(row[0]) if isinstance(row, tuple) and len(row) > 0 else 0
                except Exception as table_err:
                    msg = str(table_err)
                    code = "table_missing" if "stock_daily" in msg else "query_failed"
                    return {
                        "status": "warning",
                        "code": code,
                        "message": msg[:120],
                        "path": path,
                        "wal_exists": wal_exists,
                    }
            if wal_exists:
                return {
                    "status": "warning",
                    "code": "wal_present",
                    "message": "检测到WAL文件，建议空闲时检查一致性",
                    "path": path,
                    "rows": cnt,
                    "wal_exists": True,
                }
            return {
                "status": "ok",
                "code": "ok",
                "path": path,
                "rows": cnt,
                "wal_exists": False,
            }
        except Exception as e:
            msg = str(e)
            code = "open_failed"
            lowered = msg.lower()
            if "permission" in lowered or "access" in lowered:
                code = "permission"
            elif "wal" in lowered:
                code = "wal_stale"
            elif "catalog" in lowered and "stock_daily" in lowered:
                code = "table_missing"
            return {"status": "warning", "code": code, "message": msg[:120]}

    def _check_easyxt(self):
        try:
            import easy_xt

            return {"status": "ok", "version": getattr(easy_xt, "__version__", "unknown")}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _check_chart(self):
        ws = getattr(self, "chart_workspace", None)
        if ws is None:
            return {"status": "not_loaded", "reason": "workspace_missing"}
        return {"status": "ok", "lazy": False}

    def _check_pipeline(self) -> dict:
        """调用 PipelineHealth 获取因子注册表 + 数据源注册表健康摘要。"""
        try:
            from data_manager.pipeline_health import PipelineHealth
            report = PipelineHealth().report()
            checks = report.get("checks", {})
            factor_check = checks.get("factor_registry", {})
            ds_check = checks.get("datasource_registry", {})
            overall = report.get("overall_healthy", False)
            return {
                "status": "ok" if overall else "warning",
                "factors": factor_check.get("total_factors", 0),
                "by_category": factor_check.get("by_category", {}),
                "data_sources": ds_check.get("source_count", 0),
                "healthy_sources": ds_check.get("healthy_source_count", 0),
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _log_health_summary(self):
        summary = []
        for name, result in self._health_check_results.items():
            status = result.get("status", "unknown")
            if status == "ok":
                rows = result.get("rows", "")
                summary.append(f"{name}: OK ({rows} rows)" if rows else f"{name}: OK")
            else:
                code = result.get("code")
                suffix = f"/{code}" if code else ""
                summary.append(f"{name}: {status}{suffix}")
        self._logger.info("[HEALTH][%s] %s", self._health_check_stage, ' | '.join(summary))

    def _health_level(self) -> tuple[str, str]:
        # 返回 (level, text)
        if not self._health_check_results:
            return "unknown", "健康检查: 未执行"
        statuses = [r.get("status", "unknown") for r in self._health_check_results.values()]
        if "error" in statuses:
            return "error", "健康检查: 异常"
        if "warning" in statuses:
            return "warning", "健康检查: 警告"
        if all(s == "ok" for s in statuses):
            return "ok", "健康检查: 正常"
        return "unknown", "健康检查: 待确认"

    def _update_health_status_label(self):
        if not hasattr(self, "health_status") or self.health_status is None:
            return
        level, text = self._health_level()
        ts = ""
        if self._last_health_check_ts:
            ts = time.strftime("%H:%M:%S", time.localtime(self._last_health_check_ts))
        stage_text = "启动" if self._health_check_stage == "startup" else "运行"
        full_text = f"{text} ({stage_text}{'/' + ts if ts else ''})"
        self.health_status.setText(full_text)
        style_map = {
            "ok": "color:#00cc66; padding-left:8px;",
            "warning": "color:#ff9900; padding-left:8px;",
            "error": "color:#ff4444; padding-left:8px;",
            "unknown": "color:#999999; padding-left:8px;",
        }
        self.health_status.setStyleSheet(style_map.get(level, style_map["unknown"]))

    def _emit_stability_summary(self):
        """启动后 60 秒稳定性摘要：DB 访问在后台线程，UI 回主线程。"""
        threading.Thread(target=self._emit_stability_summary_bg, daemon=True).start()

    def _emit_stability_summary_bg(self):
        try:
            from data_manager.duckdb_connection_pool import resolve_duckdb_path, get_db_manager
            try:
                mgr = get_db_manager(resolve_duckdb_path())
                db_connections = getattr(mgr, "_connection_count", -1)
                wal_repaired = getattr(mgr, "_wal_repaired_once", False)
            except Exception:
                db_connections = -1
                wal_repaired = False
        except Exception:
            db_connections = -1
            wal_repaired = False

        restarts = self._service_restart_count
        circuit_broken = self._service_circuit_broken
        log_suppressed = self._service_log_suppressed_total

        circuit_flag = "⚠️ 已熔断" if circuit_broken else "正常"
        wal_flag = "修复过" if wal_repaired else "无"
        self._logger.info(
            "[STABILITY@60s] 服务重启=%s/%s 熔断=%s 日志抑制批次=%s DB连接数=%s WAL修复=%s",
            restarts, self._SERVICE_MAX_RESTARTS,
            circuit_flag, log_suppressed, db_connections, wal_flag,
        )

        tooltip = (
            f"60s稳定性摘要\n"
            f"服务重启: {restarts}/{self._SERVICE_MAX_RESTARTS}\n"
            f"熔断状态: {circuit_flag}\n"
            f"日志抑制批次: {log_suppressed}\n"
            f"DB连接数: {db_connections}\n"
            f"WAL修复: {wal_flag}"
        )
        QTimer.singleShot(0, lambda t=tooltip: self._apply_stability_tooltip(t))

    def _apply_stability_tooltip(self, tooltip: str):
        if hasattr(self, "health_status") and self.health_status is not None:
            self.health_status.setToolTip(tooltip)

    def _emit_service_log_diagnostics(self):
        threading.Thread(target=self._emit_service_log_diagnostics_worker, daemon=True).start()

    def _emit_service_log_diagnostics_worker(self):
        log_path = os.path.join(project_path, "logs", "service_manager.log")
        summary = {
            "window": "最近120秒",
            "error_lines": 0,
            "warning_lines": 0,
            "gbk_errors": 0,
            "bind_conflicts": 0,
            "restart_hints": 0,
            "top_errors": [],
        }
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()[-800:]
                error_counter: dict[str, int] = {}
                for line in lines:
                    low = line.lower()
                    if " - error - " in low:
                        summary["error_lines"] = int(summary["error_lines"]) + 1
                        key = line.strip()
                        error_counter[key] = error_counter.get(key, 0) + 1
                    if " - warning - " in low:
                        summary["warning_lines"] = int(summary["warning_lines"]) + 1
                    if "gbk" in low and "codec" in low:
                        summary["gbk_errors"] = int(summary["gbk_errors"]) + 1
                    if "10048" in low or "address already in use" in low:
                        summary["bind_conflicts"] = int(summary["bind_conflicts"]) + 1
                    if "异常退出" in line or "尝试重启" in line:
                        summary["restart_hints"] = int(summary["restart_hints"]) + 1
                top = sorted(error_counter.items(), key=lambda x: x[1], reverse=True)[:5]
                summary["top_errors"] = [f"{cnt}x {msg}" for msg, cnt in top]
            except Exception as e:
                summary["top_errors"] = [f"日志读取失败: {e}"]
        else:
            summary["top_errors"] = ["日志文件不存在"]
        diag_record = {
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "phase": "startup_120s",
            "error_lines": int(summary["error_lines"]),
            "warning_lines": int(summary["warning_lines"]),
            "gbk_errors": int(summary["gbk_errors"]),
            "bind_conflicts": int(summary["bind_conflicts"]),
            "restart_hints": int(summary["restart_hints"]),
            "top_errors": list(summary.get("top_errors") or []),
        }
        try:
            log_dir = os.path.join(project_path, "logs")
            os.makedirs(log_dir, exist_ok=True)
            diag_path = os.path.join(log_dir, "stability_diag.log")
            with open(diag_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(diag_record, ensure_ascii=False) + "\n")
            summary["diag_log"] = diag_path
        except Exception as e:
            summary["diag_log"] = f"写入失败: {e}"
        self._service_diag_summary = summary
        # UI 更新必须调度回主线程
        QTimer.singleShot(0, lambda s=summary: self._apply_service_log_diagnostics_ui(s))

    def _apply_service_log_diagnostics_ui(self, summary: dict):
        fatal_signals = int(summary["gbk_errors"]) + int(summary["bind_conflicts"]) + int(summary["restart_hints"])
        if fatal_signals > 0:
            self.service_diag_status.setText(f"服务诊断: ⚠️ {fatal_signals}")
            self.service_diag_status.setStyleSheet("color:#d32f2f; padding-left:8px; font-weight:bold;")
        else:
            self.service_diag_status.setText("服务诊断: ✅")
            self.service_diag_status.setStyleSheet("color:#00aa66; padding-left:8px;")
        tip = (
            f"ERROR: {summary['error_lines']} | WARNING: {summary['warning_lines']}\n"
            f"GBK异常: {summary['gbk_errors']} | 端口冲突: {summary['bind_conflicts']} | 重启线索: {summary['restart_hints']}\n"
            f"归档: {summary.get('diag_log', 'N/A')}"
        )
        self.service_diag_status.setToolTip(tip)
        self._logger.info(
            "[SERVICE_DIAG@120s] error=%s warning=%s gbk=%s bind10048=%s restart_hints=%s",
            summary['error_lines'], summary['warning_lines'],
            summary['gbk_errors'], summary['bind_conflicts'], summary['restart_hints'],
        )

    def _p(self, tag: str):
        now = time.perf_counter()
        self._marks[tag] = now
        if self._profile_enabled:
            dt = (now - self._t0) * 1000
            self._logger.debug("[PROFILE] %s: %.1f ms", tag, dt)
        if getattr(self, "perf_status", None):
            self._update_perf_summary()

    def init_ui(self):
        """初始化界面"""
        self.setWindowTitle("EasyXT量化交易策略管理平台")
        self.setGeometry(100, 100, 1600, 1000)
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        splitter = QSplitter(Qt.Vertical)
        splitter.setHandleWidth(6)
        splitter.setOpaqueResize(True)
        splitter.setStyleSheet("QSplitter::handle{background:#444444;}")
        from gui_app.widgets.chart_workspace import ChartWorkspace
        from gui_app.widgets.module_tab_widget import ModuleTabWidget

        self.chart_workspace = ChartWorkspace(show_operation_panel=False)
        self.module_tab_widget = ModuleTabWidget()
        splitter.addWidget(self.chart_workspace)
        splitter.addWidget(self.module_tab_widget)
        splitter.setCollapsible(0, False)
        splitter.setCollapsible(1, False)
        splitter.setStretchFactor(0, 7)
        splitter.setStretchFactor(1, 3)
        splitter.setSizes([760, 320])
        main_layout.addWidget(splitter)
        self.main_splitter = splitter
        self._tab_switch_start = None
        self.module_tab_widget.tab_widget.currentChanged.connect(self._on_tab_changed)
        self.module_tab_widget.module_loaded.connect(self._on_module_loaded)
        self._connect_workspace_and_modules()
        self._p("tabs-created")
        self.create_status_bar()
        self._p("statusbar-created")
        self.setWindowTitle("EasyXT量化交易策略管理平台")
        self.setGeometry(100, 100, 1200, 800)
        self.setMinimumSize(800, 600)
        self.module_tab_widget.tab_widget.setCurrentIndex(0)
        self._p("init-ui-complete")
        self._watchdog_last_ts = time.monotonic()
        self._watchdog_timer = QTimer(self)
        self._watchdog_timer.setInterval(500)
        self._watchdog_timer.timeout.connect(self._watchdog_tick)
        self._watchdog_timer.start()
        self._thread_watermark_timer = QTimer(self)
        self._thread_watermark_timer.setInterval(30000)
        self._thread_watermark_timer.timeout.connect(self._thread_watermark_tick)
        self._thread_watermark_timer.start()

    def _connect_workspace_and_modules(self):
        module_widget = self.module_tab_widget
        if module_widget is None:
            return
        trading_module = module_widget.get_loaded_module("交易管理")
        if trading_module is not None:
            self._bind_trading_module(trading_module)

    def _on_module_loaded(self, title: str, widget: QWidget):
        if title == "交易管理":
            self._bind_trading_module(widget)
        elif title == "策略管理":
            self._bind_strategy_module(widget)

    def _bind_trading_module(self, trading_module: QWidget):
        if self.chart_workspace is None or not hasattr(self.chart_workspace, "chart_panel"):
            return
        chart_panel = getattr(self.chart_workspace, "chart_panel", None)
        if chart_panel is None:
            return
        if hasattr(trading_module, "symbol_selected"):
            try:
                trading_module.symbol_selected.disconnect(self._on_trading_symbol_selected)
            except Exception:
                pass
            trading_module.symbol_selected.connect(self._on_trading_symbol_selected)
        if hasattr(trading_module, "order_submitted"):
            try:
                trading_module.order_submitted.disconnect(self._on_trading_order_submitted)
            except Exception:
                pass
            trading_module.order_submitted.connect(self._on_trading_order_submitted)
        if hasattr(chart_panel, "symbol_changed") and hasattr(trading_module, "set_symbol"):
            try:
                chart_panel.symbol_changed.disconnect(trading_module.set_symbol)
            except Exception:
                pass
            chart_panel.symbol_changed.connect(trading_module.set_symbol)

    def _on_trading_symbol_selected(self, symbol: str):
        if not symbol:
            return
        if self.chart_workspace is None:
            return
        chart_panel = getattr(self.chart_workspace, "chart_panel", None)
        if chart_panel is not None and hasattr(chart_panel, "load_symbol"):
            chart_panel.load_symbol(symbol)

    def _on_trading_order_submitted(self, order: dict):
        if self.chart_workspace is None or not isinstance(order, dict):
            return
        chart_panel = getattr(self.chart_workspace, "chart_panel", None)
        if chart_panel is None or not hasattr(chart_panel, "mark_order"):
            return
        normalized = dict(order)
        if "symbol" not in normalized and "stock_code" in normalized:
            normalized["symbol"] = normalized.get("stock_code")
        chart_panel.mark_order(normalized)

    def _bind_strategy_module(self, strategy_module: QWidget):
        """桥接策略管理模块（StrategyGovernancePanel）的信号。"""
        # strategy_module 是 StrategyModule（懒加载容器），
        # 其 Tab 0 为 StrategyGovernancePanel，加载后再绑定
        panel = None
        # 若模块已加载 Tab 0，直接获取
        if hasattr(strategy_module, "_loaded_tabs"):
            panel = strategy_module._loaded_tabs.get(0)
        elif hasattr(strategy_module, "tab_widget"):
            w = strategy_module.tab_widget.widget(0)
            from gui_app.widgets.strategy_governance_panel import StrategyGovernancePanel
            if isinstance(w, StrategyGovernancePanel):
                panel = w
        if panel is None:
            return
        # 回测完成后向 signal_bus 广播
        try:
            from core.signal_bus import signal_bus as _bus
            if hasattr(panel, "_result_tab"):
                # result_tab 的 load_result 被调用时回测已完成，无需额外信号
                pass
        except Exception:
            pass

    def _watchdog_tick(self):
        now = time.monotonic()
        gap = now - self._watchdog_last_ts
        self._watchdog_last_ts = now
        # Fix 64: 跳过前 2 次 tick（含初始化和首页加载），避免误报启动耗时
        if not hasattr(self, "_watchdog_skip_count"):
            self._watchdog_skip_count = 0
        if self._watchdog_skip_count < 2:
            self._watchdog_skip_count += 1
            return
        self._watchdog_gap_buffer.append(float(gap))
        if now - self._watchdog_stats_last_emit >= self._watchdog_stats_interval_s:
            self._watchdog_stats_last_emit = now
            self._emit_watchdog_latency_summary()
        # 正常 500ms 间隔，若 >1.5s 说明主线程被阻塞
        if gap > 1.5:
            self._logger.warning("[WATCHDOG] 主线程卡顿 %.1fs （期望≤0.5s）", gap)

    @staticmethod
    def _calc_percentile(sorted_values: list[float], ratio: float) -> float:
        if not sorted_values:
            return 0.0
        n = len(sorted_values)
        idx = int(round((n - 1) * ratio))
        idx = max(0, min(idx, n - 1))
        return float(sorted_values[idx])

    def _emit_watchdog_latency_summary(self):
        values = list(self._watchdog_gap_buffer)
        if not values:
            return
        sorted_values = sorted(values)
        p50 = self._calc_percentile(sorted_values, 0.50)
        p95 = self._calc_percentile(sorted_values, 0.95)
        p99 = self._calc_percentile(sorted_values, 0.99)
        max_gap = float(sorted_values[-1])
        slow_count = sum(1 for x in values if x > 1.5)
        record = {
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "samples": len(values),
            "p50_s": round(p50, 4),
            "p95_s": round(p95, 4),
            "p99_s": round(p99, 4),
            "max_s": round(max_gap, 4),
            "slow_count": int(slow_count),
            "p99_slo_s": float(self._watchdog_slo_p99_s),
        }
        status = "OK" if p99 <= self._watchdog_slo_p99_s else "WARN"
        self._logger.info(
            "[MAIN_THREAD_LATENCY] status=%s samples=%s p50=%.3fs p95=%.3fs p99=%.3fs max=%.3fs slow=%s",
            status, record['samples'], p50, p95, p99, max_gap, slow_count,
        )
        if status != "OK":
            self._logger.warning(
                "[WATCHDOG][SLO] p99=%.3fs > target=%.3fs", p99, self._watchdog_slo_p99_s
            )
        threading.Thread(
            target=self._append_json_log_line,
            args=(self._watchdog_log_path, record),
            daemon=True,
        ).start()

    def _thread_watermark_tick(self):
        active_threads = int(threading.active_count())
        record = {
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "active_threads": active_threads,
            "threshold": int(self._thread_watermark_threshold),
        }
        if active_threads > self._thread_watermark_threshold:
            self._logger.warning(
                "[THREAD_WATERMARK] active=%s > threshold=%s",
                active_threads, self._thread_watermark_threshold,
            )
        threading.Thread(
            target=self._append_json_log_line,
            args=(self._thread_watermark_log_path, record),
            daemon=True,
        ).start()

    @staticmethod
    def _append_json_log_line(path: str, payload: dict[str, object]):
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _on_tab_changed(self, index: int):
        try:
            self._tab_switch_start = time.perf_counter()
            QTimer.singleShot(0, lambda: self._measure_tab_switch(index))
        except Exception:
            pass

    def _measure_tab_switch(self, index: int):
        try:
            if self._tab_switch_start is None:
                return
            dt = (time.perf_counter() - self._tab_switch_start) * 1000
            self._marks[f"tab-{index}-shown"] = time.perf_counter()
            if hasattr(self, "perf_status") and self.perf_status:
                base = self.perf_status.text()
                extra = f" | 标签{index}:{dt:.0f}ms" if base else f"标签{index}:{dt:.0f}ms"
                self.perf_status.setText(extra if not base else base.split(" | 标签")[0] + extra)
        except Exception:
            pass

    def create_tabs(self):
        """创建各个功能标签页（懒加载模式）"""

        # 定义所有标签页
        tab_names = [
            "专业图表工作台",
            "回测分析",
            "网格交易",
            "条件单",
            "JQ2QMT",
            "JQ转Ptrade",
            "数据管理",
            "因子分析",
            "策略管理",
        ]

        # 为每个标签页创建占位符
        for tab_name in tab_names:
            placeholder = QLabel(f"点击进入 {tab_name}...")
            placeholder.setAlignment(Qt.AlignCenter)
            placeholder.setStyleSheet("color: #666; font-size: 14px;")
            self.tab_widget.addTab(placeholder, tab_name)

        # 初始化懒加载管理器
        self._lazy_loader = LazyTabLoader(self.tab_widget)

        QTimer.singleShot(0, lambda: self._lazy_loader.preload_tabs([0]))

        # 延迟预加载其他常用标签页（后台）
        preload_tabs = os.environ.get("EASYXT_PRELOAD_TABS", "0") in ("1", "true", "True")
        if preload_tabs:
            QTimer.singleShot(2000, lambda: self._lazy_loader.preload_tabs([1, 2]))

    def _init_chart_workspace(self):
        if self.chart_workspace is not None:
            return
        from gui_app.widgets.chart_workspace import ChartWorkspace

        self.chart_workspace = ChartWorkspace(show_operation_panel=False)
        if getattr(self, "main_splitter", None) is not None:
            self.main_splitter.insertWidget(0, self.chart_workspace)
            self.main_splitter.setStretchFactor(0, 7)
            self.main_splitter.setStretchFactor(1, 3)
        self._p("chart-workspace-initialized")

    def _preheat_components(self):
        if self.chart_workspace is None:
            self._init_chart_workspace()
        if self.chart_workspace is not None:
            self.chart_workspace.preheat()
        self._p("preheat-triggered")
        self._update_perf_summary()

    def _update_perf_summary(self):
        try:
            t0 = self._marks.get("t0", self._t0)
            t_ui = self._marks.get("ui-initialized")
            t_tabs = self._marks.get("tabs-created")
            t_sb = self._marks.get("statusbar-created")
            t_ws = self._marks.get("chart-workspace-initialized")
            t_pre = self._marks.get("preheat-triggered")
            parts = []

            def fmt(ms):
                return f"{ms:.0f}ms"

            if t_ui:
                parts.append(f"首帧:{fmt((t_ui - t0) * 1000)}")
            if t_ws:
                parts.append(f"工作台:{fmt((t_ws - t0) * 1000)}")
            if t_pre:
                parts.append(f"预热:{fmt((t_pre - t0) * 1000)}")
            if not parts and t_tabs and t_sb:
                parts.append(f"UI:{fmt((t_sb - t0) * 1000)}")
            text = " | ".join(parts) if parts else "启动性能: 收集中…"
            if hasattr(self, "perf_status") and self.perf_status:
                self.perf_status.setText(text)
                ref_t = t_pre or t_ws or t_ui
                if ref_t is not None:
                    self._apply_perf_status_style((ref_t - t0) * 1000)
        except Exception:
            pass

    def _apply_perf_status_style(self, total_ms: float):
        if not hasattr(self, "perf_status") or self.perf_status is None:
            return
        if total_ms <= 500:
            color = "#00cc66"
        elif total_ms <= 1200:
            color = "#ff9900"
        else:
            color = "#ff4444"
        self.perf_status.setStyleSheet(f"color: {color}; padding-left: 8px;")

    def _create_widget(self, module_path: str, class_name: str, title: str):
        try:
            module = importlib.import_module(module_path)
            widget_cls = getattr(module, class_name)
            return widget_cls()
        except Exception as e:
            label = QLabel(f"{title}载入失败: {e}")
            label.setAlignment(Qt.AlignCenter)
            return label

    def create_status_bar(self):
        """创建状态栏"""
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.connection_status = QLabel("🔴 MiniQMT未连接")
        self.connection_status.setStyleSheet("""
            QLabel {
                background-color: #ff4444;
                color: white;
                padding: 4px 8px;
                border-radius: 4px;
                font-weight: bold;
            }
            QLabel:hover {
                background-color: #ff6666;
            }
        """)
        self.status_bar.addPermanentWidget(self.connection_status)
        setattr(self.connection_status, "mousePressEvent", self.on_connection_status_clicked)
        self.service_status = QLabel("⚪ 服务待命")
        self.service_status.setToolTip("EasyXT HTTP/WebSocket 数据服务 (点击启动)")
        self.service_status.setStyleSheet("color: #666; padding-left: 8px;")
        setattr(self.service_status, "mousePressEvent", self.on_service_status_clicked)
        self.status_bar.addPermanentWidget(self.service_status)
        self.realtime_pipeline_status = QLabel("实时链路: 待检测")
        self.realtime_pipeline_status.setStyleSheet("color:#999; padding-left:8px;")
        self.realtime_pipeline_status.setToolTip("等待图表模块上报实时链路状态")
        setattr(self.realtime_pipeline_status, "mousePressEvent", self.on_realtime_pipeline_status_clicked)
        self.status_bar.addPermanentWidget(self.realtime_pipeline_status)
        self.backtest_engine_status = QLabel("回测引擎: 待检测")
        self.backtest_engine_status.setStyleSheet("color:#999; padding-left:8px;")
        self.backtest_engine_status.setToolTip("等待回测页上报引擎状态")
        setattr(self.backtest_engine_status, "mousePressEvent", self.on_backtest_engine_status_clicked)
        self.status_bar.addPermanentWidget(self.backtest_engine_status)
        self.health_status = QLabel("健康检查: 初始化中…")
        self.health_status.setStyleSheet("color:#999; padding-left:8px;")
        self.status_bar.addPermanentWidget(self.health_status)
        setattr(self.health_status, "mousePressEvent", self.on_health_status_clicked)
        self.service_diag_status = QLabel("服务诊断: 等待120s…")
        self.service_diag_status.setStyleSheet("color:#999; padding-left:8px;")
        self.service_diag_status.setToolTip("启动后120秒自动汇总 service_manager.log 异常摘要")
        setattr(self.service_diag_status, "mousePressEvent", self.on_service_diag_status_clicked)
        self.status_bar.addPermanentWidget(self.service_diag_status)
        self.perf_status = QLabel("启动性能: 收集中…")
        self.perf_status.setStyleSheet("color: #888; padding-left: 8px;")
        self.status_bar.addPermanentWidget(self.perf_status)
        setattr(self.perf_status, "mousePressEvent", self.on_perf_status_clicked)
        self.release_gate_status = QLabel("发布门禁: 待检测")
        self.release_gate_status.setStyleSheet("color:#999; padding-left:8px;")
        self.release_gate_status.setToolTip("读取 artifacts/p0_metrics_latest.json")
        setattr(self.release_gate_status, "mousePressEvent", self.on_release_gate_status_clicked)
        self.status_bar.addPermanentWidget(self.release_gate_status)
        self.status_bar.showMessage("就绪")

        # 检查MiniQMT连接状态（启动时延迟1秒检查）
        QTimer.singleShot(1000, self._start_connection_check)

        # 定期检查连接状态（每30秒检查一次）
        self.connection_check_timer = QTimer()
        self.connection_check_timer.timeout.connect(self._start_connection_check)
        self.connection_check_timer.start(self._check_base_interval)
        self.release_gate_timer = QTimer()
        self.release_gate_timer.timeout.connect(self._refresh_release_gate_status)
        self.release_gate_timer.start(int(os.environ.get("EASYXT_RELEASE_GATE_REFRESH_MS", "10000")))
        autostart_services = os.environ.get("EASYXT_AUTOSTART_SERVICES", "0") in (
            "1",
            "true",
            "True",
        )
        if autostart_services:
            QTimer.singleShot(3000, self.start_all_services)
        self._render_realtime_pipeline_status()
        self._render_backtest_engine_status()
        self._refresh_release_gate_status()

    def on_connection_status_clicked(self, event):
        """连接状态标签被点击事件"""
        print("手动刷新连接状态...")
        self._start_connection_check()

    def on_service_status_clicked(self, event):
        """服务状态标签被点击事件"""
        if self._service_external_manager and (
            not self.service_process or self.service_process.state() == QProcess.NotRunning
        ):
            modifiers = QApplication.keyboardModifiers()
            if modifiers == Qt.ControlModifier:
                reply = QMessageBox.question(
                    self,
                    "切换为本窗口管理",
                    "当前正在复用外部服务管理器。\n是否清除复用标记并由本窗口接管启动？",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No
                )
                if reply == QMessageBox.Yes:
                    self._service_external_manager = False
                    self._service_circuit_broken = False
                    self._service_restart_count = 0
                    self.start_all_services(manual=True)
            else:
                QMessageBox.information(
                    self,
                    "服务状态",
                    "当前正在复用外部服务管理器，主窗口不再重复拉起服务。\n"
                    "如需切换为本窗口接管，请按住 Ctrl 后点击此标签。"
                )
            return
        if self.service_process and self.service_process.state() != QProcess.NotRunning:
            # 服务正在运行，点击仅显示状态信息，不再提供停止选项
            # 除非按住Ctrl键强制停止（给开发调试用）
            modifiers = QApplication.keyboardModifiers()
            if modifiers == Qt.ControlModifier:
                reply = QMessageBox.question(
                    self,
                    "强制停止服务",
                    "确定要强制停止后台数据服务吗？\n这将中断HTTP和WebSocket连接。",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No
                )
                if reply == QMessageBox.Yes:
                    self.service_process.terminate()
                    self.update_service_status(False)
            else:
                QMessageBox.information(self, "服务状态", "EasyXT 后台数据服务正在运行中。\n\n此服务为系统核心组件，负责数据推送和API接口，通常无需手动干预。")
        else:
            # 手动启动服务（manual=True 重置熔断状态）
            self.start_all_services(manual=True)

    def on_perf_status_clicked(self, event):
        text = self._build_perf_detail_text()
        QMessageBox.information(self, "启动性能详情", text)

    def on_release_gate_status_clicked(self, event):
        gate = self._release_gate_status or {}
        detail = gate.get("duckdb_write_probe_detail") if isinstance(gate, dict) else {}
        detail = detail if isinstance(detail, dict) else {}
        intraday_detail = gate.get("intraday_bar_semantic_detail") if isinstance(gate, dict) else {}
        intraday_detail = intraday_detail if isinstance(intraday_detail, dict) else {}
        governance_detail = gate.get("governance_nightly_detail") if isinstance(gate, dict) else {}
        governance_detail = governance_detail if isinstance(governance_detail, dict) else {}
        watermark_detail = gate.get("watermark_quality_detail") if isinstance(gate, dict) else {}
        watermark_detail = watermark_detail if isinstance(watermark_detail, dict) else {}
        watermark_audit = gate.get("watermark_profile_audit_detail") if isinstance(gate, dict) else {}
        watermark_audit = watermark_audit if isinstance(watermark_audit, dict) else {}
        watermark_approval = gate.get("watermark_profile_approval_detail") if isinstance(gate, dict) else {}
        watermark_approval = watermark_approval if isinstance(watermark_approval, dict) else {}
        metrics_path = os.path.join(project_path, "artifacts", "p0_metrics_latest.json")
        action = str(detail.get("recommended_action") or "").strip()
        intraday_action = str(intraday_detail.get("recommended_action") or "").strip()
        governance_action = str(governance_detail.get("recommended_action") or "").strip()
        final_action = governance_action or intraday_action or action
        if final_action:
            QApplication.clipboard().setText(final_action)
        trend_items = watermark_detail.get("trend") if isinstance(watermark_detail.get("trend"), list) else []
        wm_weights = watermark_detail.get("weights") if isinstance(watermark_detail.get("weights"), dict) else {}
        wm_profile = str(watermark_detail.get("profile") or "balanced")
        trend_tail = trend_items[-3:] if trend_items else []
        trend_text = " | ".join(
            [f"{str(it.get('date') or '')}:{float(it.get('q_score', 0.0) or 0.0):.3f}" for it in trend_tail if isinstance(it, dict)]
        ) or "N/A"
        q_spark = self._score_sparkline([float(it.get("q_score", 0.0) or 0.0) for it in trend_items if isinstance(it, dict)])
        late_spark = self._score_sparkline([float(it.get("late_score", 0.0) or 0.0) for it in trend_items if isinstance(it, dict)])
        ooo_spark = self._score_sparkline([float(it.get("ooo_score", 0.0) or 0.0) for it in trend_items if isinstance(it, dict)])
        lateness_spark = self._score_sparkline([float(it.get("lateness_score", 0.0) or 0.0) for it in trend_items if isinstance(it, dict)])
        audit_recent = watermark_audit.get("recent") if isinstance(watermark_audit.get("recent"), list) else []
        audit_tail = audit_recent[-3:] if audit_recent else []
        audit_text = " | ".join(
            [
                f"{str(it.get('ts') or '')} {str(it.get('action') or '')}->{str(it.get('profile') or '')} {'OK' if bool(it.get('success', False)) else 'FAIL'}"
                for it in audit_tail
                if isinstance(it, dict)
            ]
        ) or "N/A"
        lines = [
            f"strict_gate_pass: {gate.get('strict_gate_pass')}",
            f"P0_open_count: {gate.get('P0_open_count')}",
            f"active_critical_high: {gate.get('active_critical_high')}",
            f"duckdb_write.status: {detail.get('status') or 'N/A'}",
            f"duckdb_write.db_path: {detail.get('db_path') or 'N/A'}",
            f"duckdb_write.error_type: {detail.get('error_type') or 'N/A'}",
            f"duckdb_write.message: {detail.get('message') or 'N/A'}",
            f"intraday_bar.status: {intraday_detail.get('status') or 'N/A'}",
            f"intraday_bar.anomaly_count: {intraday_detail.get('anomaly_count') or 0}",
            f"intraday_bar.message: {intraday_detail.get('message') or 'N/A'}",
            f"governance_nightly.status: {governance_detail.get('status') or 'N/A'}",
            f"governance_nightly.failed_items: {governance_detail.get('failed_items') or 0}",
            f"governance_nightly.message: {governance_detail.get('message') or 'N/A'}",
            f"watermark_quality.status: {watermark_detail.get('status') or 'N/A'}",
            f"watermark_quality.today_q_score: {float(watermark_detail.get('today_q_score', 0.0) or 0.0):.4f}",
            f"watermark_quality.q_score_floor: {float(watermark_detail.get('q_score_floor', 0.0) or 0.0):.4f}",
            f"watermark_quality.q_score_pass: {bool(watermark_detail.get('q_score_pass', False))}",
            f"watermark_quality.profile: {wm_profile}",
            f"watermark_quality.weights: late={float(wm_weights.get('late', 0.0) or 0.0):.3f}, ooo={float(wm_weights.get('ooo', 0.0) or 0.0):.3f}, lateness={float(wm_weights.get('lateness', 0.0) or 0.0):.3f}",
            f"watermark_quality.today_late_score: {float(watermark_detail.get('today_late_score', 0.0) or 0.0):.4f}",
            f"watermark_quality.today_ooo_score: {float(watermark_detail.get('today_ooo_score', 0.0) or 0.0):.4f}",
            f"watermark_quality.today_lateness_score: {float(watermark_detail.get('today_lateness_score', 0.0) or 0.0):.4f}",
            f"watermark_quality.q_score_mean_7d: {float(watermark_detail.get('q_score_mean_7d', 0.0) or 0.0):.4f}",
            f"watermark_quality.q_score_vol_7d: {float(watermark_detail.get('q_score_vol_7d', 0.0) or 0.0):.4f}",
            f"watermark_quality.late_mean_7d: {float(watermark_detail.get('late_score_mean_7d', 0.0) or 0.0):.4f}",
            f"watermark_quality.late_vol_7d: {float(watermark_detail.get('late_score_vol_7d', 0.0) or 0.0):.4f}",
            f"watermark_quality.ooo_mean_7d: {float(watermark_detail.get('ooo_score_mean_7d', 0.0) or 0.0):.4f}",
            f"watermark_quality.ooo_vol_7d: {float(watermark_detail.get('ooo_score_vol_7d', 0.0) or 0.0):.4f}",
            f"watermark_quality.lateness_mean_7d: {float(watermark_detail.get('lateness_score_mean_7d', 0.0) or 0.0):.4f}",
            f"watermark_quality.lateness_vol_7d: {float(watermark_detail.get('lateness_score_vol_7d', 0.0) or 0.0):.4f}",
            f"watermark_quality.q_spark: {q_spark}",
            f"watermark_quality.late_spark: {late_spark}",
            f"watermark_quality.ooo_spark: {ooo_spark}",
            f"watermark_quality.lateness_spark: {lateness_spark}",
            f"watermark_quality.trend(last3): {trend_text}",
            f"watermark_profile_audit.status: {watermark_audit.get('status') or 'N/A'}",
            f"watermark_profile_audit.count: {int(watermark_audit.get('count', 0) or 0)}",
            f"watermark_profile_audit.recent(last3): {audit_text}",
            f"watermark_profile_approval.required: {bool(watermark_approval.get('required', False))}",
            f"watermark_profile_approval.valid: {bool(watermark_approval.get('valid', False))}",
            f"watermark_profile_approval.release_env: {watermark_approval.get('release_env') or 'N/A'}",
            f"watermark_profile_approval.profile: {watermark_approval.get('profile') or 'N/A'}",
            f"watermark_profile_approval.approval_id: {watermark_approval.get('approval_id') or 'N/A'}",
            f"watermark_profile_approval.approver: {watermark_approval.get('approver') or 'N/A'}",
            f"watermark_profile_approval.reason: {watermark_approval.get('reason') or 'N/A'}",
            f"watermark_profile_approval.registry_path: {watermark_approval.get('registry_path') or 'N/A'}",
            f"watermark_profile_approval.approved_at: {watermark_approval.get('approved_at') or 'N/A'}",
            f"watermark_profile_approval.expires_at: {watermark_approval.get('expires_at') or 'N/A'}",
            f"watermark_profile_approval.days_to_expire: {watermark_approval.get('days_to_expire')}",
            f"watermark_profile_approval.signature_required: {bool(watermark_approval.get('signature_required', False))}",
            f"watermark_profile_approval.signature_valid: {bool(watermark_approval.get('signature_valid', False))}",
            f"watermark_profile_approval.signatures_required: {watermark_approval.get('signatures_required')}",
            f"watermark_profile_approval.signatures_valid_count: {watermark_approval.get('signatures_valid_count')}",
            f"watermark_profile_approval.max_uses: {watermark_approval.get('max_uses')}",
            f"watermark_profile_approval.used_count: {watermark_approval.get('used_count')}",
            f"watermark_profile_approval.remaining_uses: {watermark_approval.get('remaining_uses')}",
            f"watermark_profile_approval.warnings: {watermark_approval.get('warnings')}",
            f"watermark_profile_approval.risk_level: {watermark_approval.get('risk_level')}",
            f"推荐动作(已复制): {final_action or 'N/A'}",
            f"门禁文件: {metrics_path}",
        ]
        if os.path.exists(metrics_path):
            QDesktopServices.openUrl(QUrl.fromLocalFile(metrics_path))
        QMessageBox.information(self, "发布门禁详情", "\n".join(lines))

    def on_backtest_engine_status_clicked(self, event):
        status = self._backtest_engine_status or {}
        detail = build_engine_status_detail(status)
        QMessageBox.information(self, "回测引擎状态详情", detail)

    def on_realtime_pipeline_status_clicked(self, event):
        status = self._realtime_pipeline_status or {}
        detail = (
            f"connected: {status.get('connected')}\n"
            f"reason: {status.get('reason') or 'N/A'}\n"
            f"last_quote: {status.get('quote_ts') or 'N/A'}\n"
            f"symbol: {status.get('symbol') or 'N/A'}\n"
            f"source: {status.get('source') or 'N/A'}"
        )
        QMessageBox.information(self, "实时链路探针详情", detail)

    def on_service_diag_status_clicked(self, event):
        summary = self._service_diag_summary or {}
        if not summary:
            QMessageBox.information(self, "服务日志诊断", "暂无诊断摘要，请等待启动120秒后自动生成。")
            return
        lines = [
            f"时间窗口: {summary.get('window', 'N/A')}",
            f"ERROR行数: {summary.get('error_lines', 0)}",
            f"WARNING行数: {summary.get('warning_lines', 0)}",
            f"GBK编码异常: {summary.get('gbk_errors', 0)}",
            f"端口占用(10048): {summary.get('bind_conflicts', 0)}",
            f"服务异常重启线索: {summary.get('restart_hints', 0)}",
            f"外部服务复用态: {self._service_external_manager}",
        ]
        top_errors_raw = summary.get("top_errors") or []
        top_errors = top_errors_raw if isinstance(top_errors_raw, list) else [str(top_errors_raw)]
        if top_errors:
            lines.append("")
            lines.append("高频异常:")
            lines.extend([f"- {str(x)}" for x in top_errors])
        diag_path_raw = summary.get("diag_log")
        diag_path = str(diag_path_raw) if isinstance(diag_path_raw, (str, os.PathLike)) else ""
        if not diag_path or diag_path.startswith("写入失败"):
            diag_path = os.path.join(project_path, "logs", "stability_diag.log")
        recent_lines: list[str] = []
        if os.path.exists(diag_path):
            try:
                with open(diag_path, "r", encoding="utf-8", errors="ignore") as f:
                    raw_rows = [row.strip() for row in f.readlines() if row.strip()]
                for row in raw_rows[-10:]:
                    try:
                        rec = json.loads(row)
                        recent_lines.append(
                            f"{rec.get('ts', 'N/A')} | "
                            f"E{rec.get('error_lines', 0)} W{rec.get('warning_lines', 0)} "
                            f"GBK{rec.get('gbk_errors', 0)} BIND{rec.get('bind_conflicts', 0)} "
                            f"RST{rec.get('restart_hints', 0)}"
                        )
                    except Exception:
                        recent_lines.append(row[:140])
            except Exception as e:
                recent_lines.append(f"读取失败: {e}")
        else:
            recent_lines.append("未找到 stability_diag.log")
        lines.append("")
        lines.append("最近10条稳定性摘要:")
        lines.extend([f"- {item}" for item in recent_lines])
        QMessageBox.information(self, "服务日志诊断", "\n".join(lines))

    def _on_backtest_engine_status_updated(self, status: dict | None = None, **kwargs):
        if isinstance(status, dict):
            self._backtest_engine_status = status
        self._log_backtest_engine_status()
        self._render_backtest_engine_status()

    def _log_backtest_engine_status(self):
        line = format_engine_status_log(self._backtest_engine_status, prefix="BACKTEST_ENGINE")
        if line == self._last_backtest_engine_log:
            return
        self._last_backtest_engine_log = line
        print(line)

    def _render_backtest_engine_status(self):
        if not hasattr(self, "backtest_engine_status"):
            return
        status = self._backtest_engine_status or {}
        ui = format_engine_status_ui(status, label_prefix="回测引擎")
        self.backtest_engine_status.setText(ui.get("text", "回测引擎: 状态未知 ❓"))
        self.backtest_engine_status.setStyleSheet(
            f"color:{ui.get('color', '#666666')}; padding-left:8px;"
        )
        self.backtest_engine_status.setToolTip(ui.get("tooltip", ""))

    def _on_realtime_pipeline_status_updated(self, status: dict | None = None, **kwargs):
        if isinstance(status, dict):
            self._realtime_pipeline_status = status
        self._log_realtime_pipeline_status()
        self._render_realtime_pipeline_status()

    def _on_data_quality_alert(self, **payload):
        stock_code = str(payload.get("stock_code") or "").strip()
        period = str(payload.get("period") or "").strip()
        level = str(payload.get("level") or "").strip().lower()
        reason = str(payload.get("reason") or "").strip()
        text = f"数据质量告警[{level or 'unknown'}] {stock_code} {period} {reason}".strip()
        print(f"[DATA_QUALITY_ALERT] {text}")
        try:
            if hasattr(self, "status_bar") and self.status_bar is not None:
                self.status_bar.showMessage(text, 10000)
        except Exception:
            pass

    def _on_data_repaired(self, **payload):
        """数据修复任务排队成功时，在状态栏闪烁提示。"""
        code = str(payload.get("stock_code") or "").strip()
        msg = f"[数据修复] {code} 已加入回填队列" if code else "[数据修复] 修复任务已排队"
        print(f"[DATA_REPAIRED] {msg}")
        try:
            if hasattr(self, "status_bar") and self.status_bar is not None:
                self.status_bar.showMessage(msg, 4000)
        except Exception:
            pass

    def _on_env_config_saved(self, **payload):
        """环境配置保存成功时，在状态栏闪烁提示。"""
        key = str(payload.get("key") or "").strip()
        msg = f"[环境配置] {key} 已写入 .env" if key else "[环境配置] 配置已保存"
        print(f"[ENV_CONFIG_SAVED] {msg}")
        try:
            if hasattr(self, "status_bar") and self.status_bar is not None:
                self.status_bar.showMessage(msg, 4000)
        except Exception:
            pass

    def _log_realtime_pipeline_status(self):
        status = self._realtime_pipeline_status or {}
        line = (
            "[REALTIME_PIPELINE] "
            f"connected={status.get('connected')} "
            f"reason={status.get('reason') or 'N/A'} "
            f"quote_ts={status.get('quote_ts') or 'N/A'} "
            f"symbol={status.get('symbol') or 'N/A'} "
            f"degraded={status.get('degraded')}"
        )
        if line == self._last_realtime_probe_log:
            return
        self._last_realtime_probe_log = line
        print(line)

    def _render_realtime_pipeline_status(self):
        if not hasattr(self, "realtime_pipeline_status"):
            return
        status = self._realtime_pipeline_status or {}
        connected = status.get("connected")
        quote_ts = status.get("quote_ts")
        reason = status.get("reason") or ""
        symbol = status.get("symbol") or ""
        degraded = status.get("degraded")
        if connected is True:
            suffix = f" | {quote_ts}" if quote_ts else ""
            if degraded:
                text = f"实时链路: 降级中 ⚠️{suffix}"
                color = "#d32f2f"
            else:
                text = f"实时链路: 已连接 ✅{suffix}"
                color = "#00aa66"
        elif connected is False:
            text = "实时链路: 未连接 ⚠️"
            color = "#ef6c00"
        else:
            text = "实时链路: 待检测"
            color = "#999999"
        tooltip = (
            f"状态: {connected}\n"
            f"原因: {reason or 'N/A'}\n"
            f"最近Quote: {quote_ts or 'N/A'}\n"
            f"标的: {symbol or 'N/A'}\n"
            f"降级: {degraded}"
        )
        self.realtime_pipeline_status.setText(text)
        self.realtime_pipeline_status.setStyleSheet(f"color:{color}; padding-left:8px;")
        self.realtime_pipeline_status.setToolTip(tooltip)

    def _refresh_release_gate_status(self):
        metrics_path = os.path.join(project_path, "artifacts", "p0_metrics_latest.json")
        if not os.path.exists(metrics_path):
            self._release_gate_status = {"strict_gate_pass": None, "P0_open_count": None, "active_critical_high": None, "duckdb_write_probe_detail": {}, "intraday_bar_semantic_detail": {}, "governance_nightly_detail": {}, "watermark_quality_detail": {}, "watermark_profile_audit_detail": {}, "watermark_profile_approval_detail": {}}
            self._render_release_gate_status()
            return
        try:
            with open(metrics_path, encoding="utf-8") as f:
                gate = json.load(f)
            self._release_gate_status = gate if isinstance(gate, dict) else {}
        except Exception:
            self._release_gate_status = {"strict_gate_pass": None, "P0_open_count": None, "active_critical_high": None, "duckdb_write_probe_detail": {}, "intraday_bar_semantic_detail": {}, "governance_nightly_detail": {}, "watermark_quality_detail": {}, "watermark_profile_audit_detail": {}, "watermark_profile_approval_detail": {}}
        self._render_release_gate_status()

    def _render_release_gate_status(self):
        if not hasattr(self, "release_gate_status") or self.release_gate_status is None:
            return
        gate = self._release_gate_status if isinstance(self._release_gate_status, dict) else {}
        strict_pass = gate.get("strict_gate_pass")
        p0_open = gate.get("P0_open_count")
        detail = gate.get("duckdb_write_probe_detail") if isinstance(gate.get("duckdb_write_probe_detail"), dict) else {}
        intraday_detail = gate.get("intraday_bar_semantic_detail") if isinstance(gate.get("intraday_bar_semantic_detail"), dict) else {}
        governance_detail = gate.get("governance_nightly_detail") if isinstance(gate.get("governance_nightly_detail"), dict) else {}
        watermark_detail = gate.get("watermark_quality_detail") if isinstance(gate.get("watermark_quality_detail"), dict) else {}
        watermark_audit = gate.get("watermark_profile_audit_detail") if isinstance(gate.get("watermark_profile_audit_detail"), dict) else {}
        watermark_approval = gate.get("watermark_profile_approval_detail") if isinstance(gate.get("watermark_profile_approval_detail"), dict) else {}
        wm_profile = str(watermark_detail.get("profile") or "balanced")
        d_status = str(detail.get("status") or "").lower()
        i_status = str(intraday_detail.get("status") or "").lower()
        i_anomaly = int(intraday_detail.get("anomaly_count") or 0)
        g_status = str(governance_detail.get("status") or "").lower()
        g_failed = int(governance_detail.get("failed_items") or 0)
        w_pass = bool(watermark_detail.get("q_score_pass", False))
        w_q = float(watermark_detail.get("today_q_score", 0.0) or 0.0)
        q_mean = float(watermark_detail.get("q_score_mean_7d", 0.0) or 0.0)
        q_vol = float(watermark_detail.get("q_score_vol_7d", 0.0) or 0.0)
        w_trend = watermark_detail.get("trend") if isinstance(watermark_detail.get("trend"), list) else []
        q_spark = self._score_sparkline([float(it.get("q_score", 0.0) or 0.0) for it in w_trend if isinstance(it, dict)])
        audit_recent = watermark_audit.get("recent") if isinstance(watermark_audit.get("recent"), list) else []
        audit_tail = audit_recent[-3:] if audit_recent else []
        audit_text = " | ".join(
            [
                f"{str(it.get('action') or '')}->{str(it.get('profile') or '')}:{'OK' if bool(it.get('success', False)) else 'FAIL'}"
                for it in audit_tail
                if isinstance(it, dict)
            ]
        ) or "N/A"
        appr_required = bool(watermark_approval.get("required", False))
        appr_valid = bool(watermark_approval.get("valid", False))
        appr_risk = str(watermark_approval.get("risk_level") or "").lower()
        if strict_pass is True:
            if appr_risk == "warn":
                text = f"🟡 发布门禁: PASS_WITH_WARN P0={p0_open if p0_open is not None else 0} Q={w_q:.3f}/{q_mean:.3f}±{q_vol:.3f} {q_spark} A={audit_text}"
                color = "#ef6c00"
            else:
                text = f"🟢 发布门禁: PASS P0={p0_open if p0_open is not None else 0} Q={w_q:.3f}/{q_mean:.3f}±{q_vol:.3f} {q_spark} A={audit_text}"
                color = "#00aa66"
        elif strict_pass is False:
            probe_err = str(detail.get("error_type") or "gate_fail")
            if i_status == "fail":
                probe_err = f"intraday:{i_anomaly}"
            elif g_status == "fail":
                probe_err = f"governance:{g_failed}"
            elif not w_pass:
                probe_err = f"qscore:{w_q:.3f}"
            elif appr_required and not appr_valid:
                probe_err = f"approval:{watermark_approval.get('reason') or 'invalid'}"
            text = f"🔴 发布门禁: FAIL P0={p0_open if p0_open is not None else '?'} {probe_err} Q={w_q:.3f}/{q_mean:.3f}±{q_vol:.3f} A={audit_text}"
            color = "#d32f2f"
        elif d_status in ("warn", "skip", "missing"):
            text = f"🟡 发布门禁: {d_status.upper()} P0={p0_open if p0_open is not None else '?'}"
            color = "#ef6c00"
        else:
            text = "⚪ 发布门禁: 待检测"
            color = "#999999"
        tooltip = (
            f"strict_gate_pass={gate.get('strict_gate_pass')}\n"
            f"P0_open_count={gate.get('P0_open_count')}\n"
            f"active_critical_high={gate.get('active_critical_high')}\n"
            f"duckdb_status={detail.get('status')}\n"
            f"db_path={detail.get('db_path')}\n"
            f"error_type={detail.get('error_type')}\n"
            f"recommended_action={detail.get('recommended_action')}\n"
            f"intraday_status={intraday_detail.get('status')}\n"
            f"intraday_anomaly_count={intraday_detail.get('anomaly_count')}\n"
            f"intraday_message={intraday_detail.get('message')}\n"
            f"intraday_action={intraday_detail.get('recommended_action')}\n"
            f"governance_status={governance_detail.get('status')}\n"
            f"governance_failed_items={governance_detail.get('failed_items')}\n"
            f"governance_message={governance_detail.get('message')}\n"
            f"governance_action={governance_detail.get('recommended_action')}\n"
            f"watermark_status={watermark_detail.get('status')}\n"
            f"watermark_today_q_score={watermark_detail.get('today_q_score')}\n"
            f"watermark_q_score_floor={watermark_detail.get('q_score_floor')}\n"
            f"watermark_q_score_pass={watermark_detail.get('q_score_pass')}\n"
            f"watermark_profile={wm_profile}\n"
            f"watermark_weights={watermark_detail.get('weights')}\n"
            f"watermark_q_score_mean_7d={watermark_detail.get('q_score_mean_7d')}\n"
            f"watermark_q_score_vol_7d={watermark_detail.get('q_score_vol_7d')}\n"
            f"watermark_late_mean_7d={watermark_detail.get('late_score_mean_7d')}\n"
            f"watermark_late_vol_7d={watermark_detail.get('late_score_vol_7d')}\n"
            f"watermark_ooo_mean_7d={watermark_detail.get('ooo_score_mean_7d')}\n"
            f"watermark_ooo_vol_7d={watermark_detail.get('ooo_score_vol_7d')}\n"
            f"watermark_lateness_mean_7d={watermark_detail.get('lateness_score_mean_7d')}\n"
            f"watermark_lateness_vol_7d={watermark_detail.get('lateness_score_vol_7d')}\n"
            f"watermark_q_spark={q_spark}\n"
            f"watermark_trend={watermark_detail.get('trend')}\n"
            f"watermark_profile_audit_status={watermark_audit.get('status')}\n"
            f"watermark_profile_audit_count={watermark_audit.get('count')}\n"
            f"watermark_profile_audit_recent={watermark_audit.get('recent')}\n"
            f"watermark_profile_approval_required={watermark_approval.get('required')}\n"
            f"watermark_profile_approval_valid={watermark_approval.get('valid')}\n"
            f"watermark_profile_approval_env={watermark_approval.get('release_env')}\n"
            f"watermark_profile_approval_profile={watermark_approval.get('profile')}\n"
            f"watermark_profile_approval_id={watermark_approval.get('approval_id')}\n"
            f"watermark_profile_approval_approver={watermark_approval.get('approver')}\n"
            f"watermark_profile_approval_reason={watermark_approval.get('reason')}\n"
            f"watermark_profile_approval_missing={watermark_approval.get('missing_fields')}\n"
            f"watermark_profile_approval_registry={watermark_approval.get('registry_path')}\n"
            f"watermark_profile_approval_approved_at={watermark_approval.get('approved_at')}\n"
            f"watermark_profile_approval_expires_at={watermark_approval.get('expires_at')}\n"
            f"watermark_profile_approval_days_to_expire={watermark_approval.get('days_to_expire')}\n"
            f"watermark_profile_approval_signature_required={watermark_approval.get('signature_required')}\n"
            f"watermark_profile_approval_signature_valid={watermark_approval.get('signature_valid')}\n"
            f"watermark_profile_approval_signatures_required={watermark_approval.get('signatures_required')}\n"
            f"watermark_profile_approval_signatures_valid_count={watermark_approval.get('signatures_valid_count')}\n"
            f"watermark_profile_approval_max_uses={watermark_approval.get('max_uses')}\n"
            f"watermark_profile_approval_used_count={watermark_approval.get('used_count')}\n"
            f"watermark_profile_approval_remaining_uses={watermark_approval.get('remaining_uses')}\n"
            f"watermark_profile_approval_warnings={watermark_approval.get('warnings')}\n"
            f"watermark_profile_approval_risk_level={watermark_approval.get('risk_level')}\n"
            f"watermark_profile_approval_usage_log={watermark_approval.get('usage_log_file')}"
        )
        self.release_gate_status.setText(text)
        self.release_gate_status.setStyleSheet(f"color:{color}; padding-left:8px;")
        self.release_gate_status.setToolTip(tooltip)

    def _score_sparkline(self, values):
        vals = [max(0.0, min(1.0, float(v))) for v in values if v is not None]
        if not vals:
            return "N/A"
        blocks = "▁▂▃▄▅▆▇█"
        out = []
        for v in vals[-14:]:
            idx = int(round(v * (len(blocks) - 1)))
            idx = max(0, min(len(blocks) - 1, idx))
            out.append(blocks[idx])
        return "".join(out)

    def on_health_status_clicked(self, event):
        if not self._health_check_results:
            QMessageBox.information(self, "健康检查详情", "暂无健康检查数据")
            return
        lines = []
        for name, result in self._health_check_results.items():
            status = result.get("status", "unknown")
            code = result.get("code", "")
            code_text = f"/{code}" if code else ""
            lines.append(f"- {name}: {status}{code_text}")
            for key in ("message", "path", "rows", "reason", "version"):
                if key in result:
                    lines.append(f"  {key}: {result[key]}")
            if name == "pipeline":
                factors = result.get("factors", 0)
                by_cat = result.get("by_category", {})
                ds_total = result.get("data_sources", 0)
                ds_ok = result.get("healthy_sources", 0)
                lines.append(f"  因子总数: {factors}")
                if by_cat:
                    for cat, cnt in by_cat.items():
                        lines.append(f"    {cat}: {cnt}")
                lines.append(f"  数据源: {ds_ok}/{ds_total} 健康")
        ts = ""
        if self._last_health_check_ts:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._last_health_check_ts))
        header = f"阶段: {self._health_check_stage}\n时间: {ts or 'N/A'}\n"
        raw = json.dumps(self._health_check_results, ensure_ascii=False, indent=2)
        text = header + "\n" + "\n".join(lines) + "\n\n原始数据:\n" + raw
        QMessageBox.information(self, "健康检查详情", text)

    def _build_perf_detail_text(self) -> str:
        t0 = self._marks.get("t0", self._t0)
        order = [
            ("t0", "启动开始"),
            ("ui-initialized", "UI完成"),
            ("tabs-created", "标签创建"),
            ("statusbar-created", "状态栏完成"),
            ("chart-workspace-initialized", "工作台完成"),
            ("preheat-triggered", "预热触发"),
        ]
        lines = []
        last = t0
        for key, label in order:
            t = self._marks.get(key)
            if t is None:
                continue
            total_ms = (t - t0) * 1000
            step_ms = (t - last) * 1000
            lines.append(f"{label}: {total_ms:.1f}ms (Δ{step_ms:.1f}ms)")
            last = t
        if not lines:
            return "暂无性能数据"
        return "\n".join(lines)

    def update_service_status(self, running: bool):
        if self._service_external_manager and (
            not self.service_process or self.service_process.state() == QProcess.NotRunning
        ):
            self.service_status.setText("🟡 复用外部服务")
            self.service_status.setStyleSheet("color: #ef6c00; padding-left: 8px; font-weight: bold;")
            return
        if running:
            self.service_status.setText("🟢 服务运行中")
            self.service_status.setStyleSheet("color: green; padding-left: 8px; font-weight: bold;")
        else:
            self.service_status.setText("⚪ 服务待命")
            self.service_status.setStyleSheet("color: #666; padding-left: 8px;")

    def start_all_services(self, manual: bool = False):
        if self._closing:
            return
        if self.service_process and self.service_process.state() != QProcess.NotRunning:
            return
        if self._service_circuit_broken and not manual:
            return
        if manual:
            # 手动重启时重置熔断状态
            self._service_circuit_broken = False
            self._service_restart_count = 0
            self._service_lock_conflict = False
            self._service_external_manager = False
        if self._service_external_manager and not manual:
            return
        self._service_restart_scheduled = False
        self.service_process = QProcess(self)
        self.service_process.setWorkingDirectory(project_path)
        env = QProcessEnvironment.systemEnvironment()
        env.insert("EASYXT_MANAGED_WEBSOCKET", "0")
        env.insert("EASYXT_ALLOW_STANDALONE_WEBSOCKET", "0")
        env.insert("PYTHONUTF8", "1")
        env.insert("PYTHONIOENCODING", "utf-8")
        self.service_process.setProcessEnvironment(env)
        self.service_process.setProgram(sys.executable)
        self.service_process.setArguments(["-m", "easy_xt.realtime_data.service_manager"])
        self.service_process.setProcessChannelMode(QProcess.MergedChannels)
        self.service_process.readyReadStandardOutput.connect(self.on_service_output)
        self.service_process.finished.connect(self.on_service_finished)
        self.service_process.start()
        self._service_start_ts = time.monotonic()
        self.update_service_status(True)
        QTimer.singleShot(1500, self._start_connection_check)

    def _show_service_circuit_breaker_warning(self):
        QMessageBox.warning(
            self,
            "服务自动重启已熔断",
            f"后台数据服务已连续退出 {self._SERVICE_MAX_RESTARTS} 次，"
            f"自动重启已暂停以防止拖慕 GUI。\n\n"
            f"请检查日志确认根因，然后点击状态栏「服务待命」按鈕手动重启。"
        )

    def stop_all_services(self):
        if not self.service_process:
            return
        self._service_restart_scheduled = False
        if self.service_process.state() == QProcess.NotRunning:
            self.update_service_status(False)
            return
        self.service_process.terminate()
        # Fix 62: 不在主线程长时间阻塞等待进程退出
        if not self.service_process.waitForFinished(500):
            self.service_process.kill()
            self.service_process.waitForFinished(300)
        self.update_service_status(False)

    def on_service_output(self):
        if not self.service_process:
            return
        data = bytes(self.service_process.readAllStandardOutput()).decode("utf-8", errors="ignore")
        if data:
            if "检测到服务管理器已在运行" in data:
                self._service_lock_conflict = True
                self._service_external_manager = True
                self._service_circuit_broken = True
                self._service_restart_scheduled = False
                self.update_service_status(True)
            now = time.monotonic()
            too_frequent = (now - self._service_log_last_ts) < 0.15
            too_large = len(data) > 6000
            if too_frequent or too_large:
                self._service_log_suppressed += 1
                self._service_log_suppressed_total += 1
                if (now - self._service_log_last_ts) > 2.0:
                    count = self._service_log_suppressed
                    self._service_log_suppressed = 0
                    self._service_log_last_ts = now
                    self._logger.warning("[SERVICE_LOG] 日志过载，已抑制 %d 批输出", count)
                return
            self._service_log_last_ts = now
            if self._service_log_suppressed > 0:
                self._logger.info("[SERVICE_LOG] 日志恢复，之前抑制 %d 批输出", self._service_log_suppressed)
                self._service_log_suppressed = 0
            print(data, end="")

    def on_service_finished(self):
        self.update_service_status(False)
        if not self._closing:
            if self._service_lock_conflict:
                self._service_lock_conflict = False
                self._service_restart_scheduled = False
                self._service_restart_count = 0
                self.update_service_status(True)
                self._logger.info("检测到已有服务管理器实例在运行，当前窗口切换为复用模式，不再自动重启。")
                return
            run_seconds = 0.0
            if self._service_start_ts > 0:
                run_seconds = time.monotonic() - self._service_start_ts
            if run_seconds >= 15:
                self._service_restart_count = 0
            if self._service_restart_count < self._SERVICE_MAX_RESTARTS:
                backoff = self._SERVICE_RESTART_BACKOFF[
                    min(self._service_restart_count, len(self._SERVICE_RESTART_BACKOFF) - 1)
                ]
                self._service_restart_count += 1
                self._logger.info(
                    "服务进程已退出，第 %d/%d 次重启，延迟 %dms...",
                    self._service_restart_count, self._SERVICE_MAX_RESTARTS, backoff,
                )
                if not self._service_restart_scheduled:
                    self._service_restart_scheduled = True
                    QTimer.singleShot(backoff, self._restart_services_after_backoff)
            else:
                self._service_circuit_broken = True
                self._service_restart_scheduled = False
                self._logger.error(
                    "服务已达最大重启次数 (%d)，已熔断自动重启，请手动检查",
                    self._SERVICE_MAX_RESTARTS,
                )
                QTimer.singleShot(0, self._show_service_circuit_breaker_warning)

    def _restart_services_after_backoff(self):
        self._service_restart_scheduled = False
        self.start_all_services()

    def check_connection_status(self):
        """检查MiniQMT连接状态（已废弃，委托给 ConnectionCheckThread）

        旧实现包含同步 api.init_data() 会阻塞主线程数秒。
        现统一由 _start_connection_check / ConnectionCheckThread 在后台完成。
        """
        self._start_connection_check()

    def update_connection_status(self, connected: bool):
        """更新连接状态显示

        Args:
            connected: 是否已连接
        """
        if connected:
            self.connection_status.setText("🟢 MiniQMT已连接")
            self.connection_status.setStyleSheet("""
                QLabel {
                    background-color: #00cc00;
                    color: white;
                    padding: 4px 8px;
                    border-radius: 4px;
                    font-weight: bold;
                }
            """)
            self.status_bar.showMessage("MiniQMT已连接")
            self._check_fail_count = 0
            if hasattr(self, "connection_check_timer"):
                self.connection_check_timer.setInterval(self._check_base_interval)

            # 连接成功后，后台预热 xtquant_broker 单例，
            # 减少 realtime worker 首次 fast-fail 空窗期
            threading.Thread(target=self._warmup_xtquant_broker, daemon=True).start()

            # 自动启动服务（如果尚未运行，且未熔断）
            if (not self.service_process or self.service_process.state() == QProcess.NotRunning)\
                    and not self._service_circuit_broken\
                    and not self._service_external_manager\
                    and not self._service_restart_scheduled:
                # 延迟启动，避免与主线程竞争资源
                QTimer.singleShot(2000, self.start_all_services)
        else:
            self.connection_status.setText("🔴 MiniQMT未连接")
            self.connection_status.setStyleSheet("""
                QLabel {
                    background-color: #ff4444;
                    color: white;
                    padding: 4px 8px;
                    border-radius: 4px;
                    font-weight: bold;
                }
            """)
            self.status_bar.showMessage("MiniQMT未连接，请检查QMT客户端是否启动")
            self._check_fail_count = min(self._check_fail_count + 1, 5)
            if hasattr(self, "connection_check_timer"):
                backoff = min(self._check_base_interval * (2**self._check_fail_count), 300000)
                self.connection_check_timer.setInterval(backoff)
        self.signal_bus.emit(Events.CONNECTION_STATUS_CHANGED, connected=connected)

    def _warmup_xtquant_broker(self):
        """后台预热 xtquant_broker 单例，使 realtime worker 的 fast-fail 检查通过"""
        try:
            import easy_xt
            easy_xt.get_xtquant_broker()
        except Exception:
            pass

    def _start_connection_check(self):
        if self._closing:
            return
        try:
            already_running = (self._check_thread is not None and self._check_thread.isRunning())
        except RuntimeError:
            already_running = False
            self._check_thread = None
        if already_running:
            return
        self._check_thread = ConnectionCheckThread()
        self._check_thread.setParent(self)
        self._check_thread.result.connect(self.update_connection_status)
        self._check_thread.finished.connect(self._on_check_thread_finished)
        self._check_thread.finished.connect(self._check_thread.deleteLater)
        self._check_thread.start()

    def _on_check_thread_finished(self):
        self._check_thread = None

    def closeEvent(self, a0):
        """关闭事件"""
        self._closing = True

        # 停止连接检查定时器
        if hasattr(self, "connection_check_timer"):
            self.connection_check_timer.stop()
        if getattr(self, "_watchdog_timer", None):
            self._watchdog_timer.stop()
        thread_watermark_timer = getattr(self, "_thread_watermark_timer", None)
        if thread_watermark_timer is not None and hasattr(thread_watermark_timer, "stop"):
            thread_watermark_timer.stop()
        # 停止周期性告警汇总定时器，防止窗口关闭后仍触发
        if getattr(self, "_alerts_rollup_timer", None):
            self._alerts_rollup_timer.stop()

        # 清理连接检查线程
        try:
            check_running = (self._check_thread is not None and self._check_thread.isRunning())
        except RuntimeError:
            check_running = False
            self._check_thread = None
        if check_running:
            self._check_thread.requestInterruption()
            self._check_thread.quit()
            if not self._check_thread.wait(1200):
                self._logger.warning("closeEvent: ConnectionCheckThread 未在 1.2s 内退出，强制终止")
                self._check_thread.terminate()
                self._check_thread.wait(300)

        # 停止后端服务进程
        self.stop_all_services()

        a0.accept()


def main():
    """主函数"""
    import os
    import tempfile

    # ── 单实例保护 ──
    _lock_file = None
    _lock_path = os.path.join(tempfile.gettempdir(), "easyxt_gui.lock")
    try:
        _lock_file = open(_lock_path, "w")
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(_lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, IOError):
        # 已有实例运行 — QApplication/QMessageBox 已在模块顶层导入
        _tmp_app = QApplication(sys.argv)
        QMessageBox.warning(None, "EasyXT", "EasyXT 已在运行，请先关闭现有实例")
        sys.exit(1)

    if os.name == "nt":
        os.environ.setdefault("QT_QPA_PLATFORM", "windows")

    # Must import WebEngine BEFORE QApplication
    try:
        from PyQt5.QtWebEngineWidgets import QWebEngineView
    except ImportError:
        pass

    QCoreApplication.setAttribute(Qt.AA_ShareOpenGLContexts)
    app = QApplication(sys.argv)

    # ── P1: 环境完整性校验（QApplication 创建后方可显示 QMessageBox）────────
    try:
        from data_manager import validate_environment as _ve
        _env_results = _ve(raise_on_error=False)
        _env_errors = {k: v for k, v in _env_results.items() if v.startswith("ERROR")}
        if _env_errors:
            import logging as _logging
            _logging.getLogger("easyxt.startup").error("启动环境校验发现错误: %s", _env_errors)
            _err_msg = "\n".join(f"  • {k}：{v}" for k, v in _env_errors.items())
            QMessageBox.warning(
                None,
                "EasyXT — 环境配置问题",
                f"启动环境校验发现以下问题，请及时修复：\n\n{_err_msg}\n\n"
                "当前运行可能受影响，建议先修复相关配置。",
            )
    except Exception:
        pass  # 校验失败不阻断 GUI 启动

    app.setApplicationName("EasyXT量化交易策略管理平台")
    app.setApplicationVersion("3.0")
    app.setOrganizationName("EasyXT")
    font = QFont("Microsoft YaHei", 9)
    app.setFont(font)
    config_path = os.path.join(project_path, "config", "unified_config.json")
    theme_manager = ThemeManager(config_path=config_path)
    theme_manager.apply(app)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
