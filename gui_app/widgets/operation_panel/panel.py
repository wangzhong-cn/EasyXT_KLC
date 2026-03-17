import importlib
import json
import logging
import os
import platform
import socket
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from PyQt5.QtCore import Qt, QThread, QTimer, pyqtSignal
from PyQt5.QtWidgets import QLabel, QPushButton, QTabWidget, QVBoxLayout, QWidget

from core.events import Events
from core.signal_bus import signal_bus
from gui_app.widgets.operation_panel.tabs import AccountTab, PositionTab, TradeTab, WatchlistTab

_logger = logging.getLogger(__name__)


def _safe_int(env_key: str, default: int, min_val: Optional[int] = None, max_val: Optional[int] = None) -> int:
    try:
        val = int(os.environ.get(env_key, str(default)))
        if min_val is not None and val < min_val:
            val = min_val
        if max_val is not None and val > max_val:
            val = max_val
        return val
    except (ValueError, TypeError):
        return default


def _rotate_log_if_needed(file_path: Path, max_size_mb: int = 10, max_files: int = 5):
    try:
        if not file_path.exists():
            return
        size_mb = file_path.stat().st_size / (1024 * 1024)
        if size_mb < max_size_mb:
            return
        base_name = file_path.stem
        ext = file_path.suffix
        for i in range(max_files - 1, 0, -1):
            old_file = file_path.parent / f"{base_name}.{i}{ext}"
            new_file = file_path.parent / f"{base_name}.{i + 1}{ext}"
            if new_file.exists():
                new_file.unlink()
            if old_file.exists():
                old_file.rename(new_file)
        backup = file_path.parent / f"{base_name}.1{ext}"
        if backup.exists():
            backup.unlink()
        file_path.rename(backup)
        _logger.info(f"日志文件已轮转: {file_path.name}")
    except Exception as e:
        _logger.warning(f"日志轮转失败: {e}")


_LOG_CONFIG = {
    "max_file_size_mb": _safe_int("EASYXT_LOG_MAX_SIZE_MB", 10, 1, 100),
    "max_files": _safe_int("EASYXT_LOG_MAX_FILES", 5, 1, 20),
}

_PREHEAT_CONFIG = {
    "enabled": os.environ.get("EASYXT_PREHEAT_ENABLED", "1") == "1",
    "delay_ms": _safe_int("EASYXT_PREHEAT_DELAY", 5000, 0, 60000),
    "tabs": os.environ.get("EASYXT_PREHEAT_TABS", "watchlist_tab,trade_tab").split(","),
}

_RETRY_CONFIG = {
    "max_attempts": _safe_int("EASYXT_RETRY_MAX", 3, 1, 10),
    "cooldown_ms": _safe_int("EASYXT_RETRY_COOLDOWN", 5000, 1000, 60000),
}

_APP_INFO = {
    "app_version": os.environ.get("EASYXT_VERSION", "unknown"),
    "machine_id": socket.gethostname(),
    "platform": platform.platform(),
    "python_version": platform.python_version(),
}


def _create_loading_placeholder(text: str = "加载中...") -> QWidget:
    placeholder = QWidget()
    placeholder.setObjectName("lazy_loading")
    layout = QVBoxLayout(placeholder)
    layout.setContentsMargins(0, 0, 0, 0)
    label = QLabel(text)
    label.setAlignment(Qt.AlignCenter)
    label.setStyleSheet("color: #888; font-size: 14px;")
    layout.addWidget(label)
    return placeholder


def _create_error_placeholder(
    text: str = "加载失败", retry_callback=None, show_log_btn: bool = True
) -> QWidget:
    placeholder = QWidget()
    placeholder.setObjectName("lazy_error")
    layout = QVBoxLayout(placeholder)
    layout.setContentsMargins(0, 0, 0, 0)
    label = QLabel(text)
    label.setAlignment(Qt.AlignCenter)
    label.setStyleSheet("color: #e74c3c; font-size: 14px;")
    layout.addWidget(label)
    if retry_callback:
        retry_btn = QPushButton("重试")
        retry_btn.setStyleSheet("QPushButton { padding: 6px 16px; }")
        retry_btn.clicked.connect(retry_callback)
        layout.addWidget(retry_btn)
    if show_log_btn:
        import subprocess

        log_dir = str(Path.home() / ".easyxt" / "logs")
        os.makedirs(log_dir, exist_ok=True)

        def open_log_dir():
            try:
                subprocess.Popen(f'explorer "{log_dir}"', shell=True)
            except Exception:
                pass

        log_btn = QPushButton("打开日志目录")
        log_btn.setStyleSheet("QPushButton { padding: 6px 16px; }")
        log_btn.clicked.connect(open_log_dir)
        layout.addWidget(log_btn)
    layout.addStretch()
    return placeholder


class OperationPanel(QWidget):
    symbol_selected = pyqtSignal(str)
    order_submitted = pyqtSignal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.trade_tab = None
        self.position_tab = None
        self.account_tab = None
        self.watchlist_tab = None
        self.trading_interface = None
        self._trading_interface_loading = False
        self._current_symbol = None
        self._creating_tabs = set()
        self._tab_timings = {}
        self._status_label = None
        self._preheat_scheduled = False
        self._preheat_thread = None
        self._retry_counts = {}
        self._retry_timestamps = {}
        self._log_file_path = None
        self._init_ui()
        signal_bus.subscribe(Events.TRADING_INTERFACE_READY, self._on_trading_interface_ready)

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        self._core_lazy = {}
        self._add_core_lazy_tab("📊 交易下单", "trade_tab")
        self._add_core_lazy_tab("📌 持仓监控", "position_tab")
        self._add_core_lazy_tab("💼 账户信息", "account_tab")
        self._add_core_lazy_tab("⭐ 自选列表", "watchlist_tab")
        self._lazy_info = {}
        self._lazy_add_tab("网格交易", "gui_app.widgets.grid_trading_widget", "GridTradingWidget")
        self._lazy_add_tab(
            "条件单", "gui_app.widgets.conditional_order_widget", "ConditionalOrderWidget"
        )
        self._lazy_add_tab("JQ2QMT", "gui_app.widgets.jq2qmt_widget", "JQ2QMTWidget")
        self._lazy_add_tab("JQ转Ptrade", "gui_app.widgets.jq_to_ptrade_widget", "JQToPtradeWidget")
        self._add_data_panel_lazy()
        self._lazy_add_tab(
            "策略管理", "strategies.management.strategy_gui", "StrategyManagementWidget"
        )
        self.tabs.currentChanged.connect(self._on_tab_changed)
        self._status_label = QLabel("")
        self._status_label.setStyleSheet("color: #888; font-size: 12px; padding: 4px;")
        layout.addWidget(self._status_label)
        layout.addWidget(self.tabs, 1)
        QTimer.singleShot(100, self._preload_watchlist_tab)

    def _preload_watchlist_tab(self):
        for idx in range(self.tabs.count()):
            w = self.tabs.widget(idx)
            if w and w.objectName() == "lazy_core_tab" and w.property("core_key") == "watchlist_tab":
                self._do_create_core_tab(idx, "watchlist_tab")
                break

    def _connect_watchlist_signals(self):
        if self.watchlist_tab is not None:
            self.watchlist_tab.symbol_selected.connect(self.symbol_selected.emit)

    def _on_trading_interface_ready(self, **kwargs):
        if self._status_label is not None:
            status_label = self._status_label
            status_label.setText("🟢 交易通道已就绪")
            status_label.setStyleSheet("color: #27ae60; font-size: 12px; padding: 4px;")
            QTimer.singleShot(3000, lambda: status_label.setText(""))

    def on_symbol_changed(self, symbol: str):
        if not symbol:
            return
        self._current_symbol = symbol
        if self.trade_tab is not None:
            self.trade_tab.set_symbol(symbol)
        if self.watchlist_tab is not None:
            self.watchlist_tab.set_selected(symbol)

    def _classify_failure(self, e: Exception, module_path: str) -> str:
        error_type = type(e).__name__
        if isinstance(e, ImportError):
            return f"导入失败({error_type})"
        elif isinstance(e, AttributeError):
            return f"模块缺少组件({error_type})"
        elif isinstance(e, OSError):
            return f"系统错误({error_type})"
        else:
            return f"运行异常({error_type})"

    def _create_widget(self, module_path: str, class_name: str, retry_index: Optional[int] = None):
        t0 = time.perf_counter()
        try:
            module = importlib.import_module(module_path)
            widget_cls = getattr(module, class_name)
            result = widget_cls()
            elapsed = (time.perf_counter() - t0) * 1000
            if elapsed > 500:
                _logger.warning(f"Tab加载耗时 {elapsed:.0f}ms: {module_path}.{class_name}")
            return result
        except Exception as e:
            failure_type = self._classify_failure(e, module_path)
            _logger.error(f"Tab加载失败[{failure_type}]: {module_path}.{class_name} - {e}")
            error_msg = f"加载失败: {failure_type}"
            if retry_index is not None:
                retry_key = f"{module_path}.{class_name}"
                self._retry_counts[retry_key] = self._retry_counts.get(retry_key, 0) + 1
                if self._retry_counts[retry_key] >= _RETRY_CONFIG["max_attempts"]:
                    return _create_error_placeholder(
                        f"重试次数超限({_RETRY_CONFIG['max_attempts']}次)"
                    )
                return _create_error_placeholder(
                    error_msg, lambda ri=retry_index: self._retry_load_tab(ri)
                )
            return _create_error_placeholder(error_msg)

    def _retry_load_tab(self, index: int):
        retry_key = f"tab_{index}"
        now = time.time() * 1000
        last_retry = self._retry_timestamps.get(retry_key, 0)
        if now - last_retry < _RETRY_CONFIG["cooldown_ms"]:
            _logger.warning(f"重试冷却中: {retry_key}")
            return
        self._retry_timestamps[retry_key] = now
        w = self.tabs.widget(index)
        if w and w.objectName() in ("lazy_error", "lazy_loading"):
            title = self.tabs.tabText(index)
            if index in self._lazy_info:
                self.tabs.removeTab(index)
                self.tabs.insertTab(index, _create_loading_placeholder(f"{title}加载中..."), title)

    def _create_data_panel(self):
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        inner_tabs = QTabWidget()
        inner_tabs.setDocumentMode(True)
        p1 = QWidget()
        p1.setObjectName("lazy_placeholder")
        inner_tabs.addTab(p1, "DuckDB管理")
        p2 = QWidget()
        p2.setObjectName("lazy_placeholder")
        inner_tabs.addTab(p2, "本地数据管理")
        p3 = QWidget()
        p3.setObjectName("lazy_placeholder")
        inner_tabs.addTab(p3, "高级数据查看")
        info = {
            0: ("gui_app.widgets.duckdb_data_manager_widget", "DuckDBDataManagerWidget"),
            1: ("gui_app.widgets.local_data_manager_widget", "LocalDataManagerWidget"),
            2: ("gui_app.widgets.advanced_data_viewer_widget", "AdvancedDataViewerWidget"),
        }

        def on_inner_changed(index: int):
            w = inner_tabs.widget(index)
            if w and w.objectName() == "lazy_placeholder":
                mp, cn = info.get(index, ("", ""))
                new_w = self._create_widget(mp, cn)
                inner_tabs.removeTab(index)
                inner_tabs.insertTab(
                    index, new_w, ["DuckDB管理", "本地数据管理", "高级数据查看"][index]
                )
                inner_tabs.setCurrentIndex(index)

        inner_tabs.currentChanged.connect(on_inner_changed)
        layout.addWidget(inner_tabs)
        return panel

    def _create_strategy_panel(self):
        return self._create_widget("strategies.management.strategy_gui", "StrategyManagementWidget")

    def _on_order_submitted(self, order: dict):
        self.order_submitted.emit(order)
        if not order:
            return
        signal_bus.emit(
            Events.ORDER_REQUESTED,
            side=order.get("side", ""),
            symbol=order.get("symbol", ""),
            price=order.get("price", 0),
            volume=order.get("volume", 0),
        )

    def _lazy_add_tab(self, title: str, module_path: str, class_name: str):
        placeholder = _create_loading_placeholder(f"{title}加载中...")
        placeholder.setObjectName("lazy_placeholder")
        index = self.tabs.addTab(placeholder, title)
        self._lazy_info[index] = (module_path, class_name, title)

    def _on_tab_changed(self, index: int):
        w = self.tabs.widget(index)
        if not w:
            return
        if w.objectName() == "lazy_core_tab":
            tab_key = w.property("core_key")
            if tab_key in ("trade_tab", "position_tab", "account_tab"):
                self._ensure_trading_interface()
            QTimer.singleShot(0, lambda: self._do_create_core_tab(index, tab_key))
            return
        if w.objectName() not in ("lazy_placeholder", "lazy_data_panel"):
            return
        QTimer.singleShot(0, lambda: self._do_create_lazy_tab(index))
        return

    def _do_create_core_tab(self, index: int, tab_key: str):
        if tab_key in self._creating_tabs:
            return
        self._creating_tabs.add(tab_key)
        t0 = time.perf_counter()
        try:
            if self.tabs.count() <= index:
                return
            w = self.tabs.widget(index)
            if w is None or w.objectName() != "lazy_core_tab":
                return
            new_w = self._create_core_tab(tab_key)
            if new_w is None:
                return
            if tab_key == "watchlist_tab" and self.watchlist_tab is not None:
                current_symbol = getattr(self, "_current_symbol", None)
                if current_symbol:
                    self.watchlist_tab.set_selected(current_symbol)
            tab_titles = {
                "trade_tab": "📊 交易下单",
                "position_tab": "📌 持仓监控",
                "account_tab": "💼 账户信息",
                "watchlist_tab": "⭐ 自选列表",
            }
            elapsed = (time.perf_counter() - t0) * 1000
            self._tab_timings[tab_key] = elapsed
            if elapsed > 500:
                _logger.warning(f"Tab创建耗时 {elapsed:.0f}ms: {tab_key}")
            self.tabs.removeTab(index)
            self.tabs.insertTab(index, new_w, tab_titles.get(tab_key, tab_key))
            self.tabs.setCurrentIndex(index)
        finally:
            self._creating_tabs.discard(tab_key)

    def _do_create_lazy_tab(self, index: int):
        if self.tabs.count() <= index:
            return
        w = self.tabs.widget(index)
        if w is None:
            return
        if w.objectName() not in ("lazy_placeholder", "lazy_data_panel"):
            return
        if index in self._creating_tabs:
            return
        info = self._lazy_info.get(index)
        if not info:
            return
        self._creating_tabs.add(index)
        t0 = time.perf_counter()
        try:
            module_path, class_name, title = info
            if module_path == "__data_panel__":
                new_w = self._create_data_panel()
            else:
                new_w = self._create_widget(module_path, class_name, retry_index=index)
            elapsed = (time.perf_counter() - t0) * 1000
            self._tab_timings[f"{module_path}.{class_name}"] = elapsed
            if elapsed > 500:
                _logger.warning(f"Tab创建耗时 {elapsed:.0f}ms: {module_path}.{class_name}")
            self.tabs.removeTab(index)
            self.tabs.insertTab(index, new_w, title)
            self.tabs.setCurrentIndex(index)
        finally:
            self._creating_tabs.discard(index)

    def _add_data_panel_lazy(self):
        placeholder = _create_loading_placeholder("数据管理加载中...")
        placeholder.setObjectName("lazy_data_panel")
        index = self.tabs.addTab(placeholder, "数据管理")
        self._lazy_info[index] = ("__data_panel__", "__data_panel__", "数据管理")

    def _add_core_lazy_tab(self, title: str, key: str):
        placeholder = _create_loading_placeholder()
        placeholder.setObjectName("lazy_core_tab")
        placeholder.setProperty("core_key", key)
        index = self.tabs.addTab(placeholder, title)
        self._core_lazy[index] = key

    def _create_core_tab(self, tab_key: str):
        if tab_key == "trade_tab":
            if self.trade_tab is None:
                self.trade_tab = TradeTab()
                self.trade_tab.order_submitted.connect(self._on_order_submitted)
            return self.trade_tab
        if tab_key == "watchlist_tab":
            if self.watchlist_tab is None:
                self.watchlist_tab = WatchlistTab()
                self._connect_watchlist_signals()
            return self.watchlist_tab
        if tab_key == "position_tab":
            if self.position_tab is None:
                self.position_tab = PositionTab()
                if self.trading_interface is not None:
                    self.trading_interface.position_updated.connect(
                        self.position_tab.update_positions
                    )
            return self.position_tab
        if tab_key == "account_tab":
            if self.account_tab is None:
                self.account_tab = AccountTab()
                if self.trading_interface is not None:
                    self.trading_interface.account_updated.connect(self.account_tab.update_account)
            return self.account_tab
        return None

    def _ensure_trading_interface(self):
        if self.trading_interface is not None or self._trading_interface_loading:
            return
        self._trading_interface_loading = True
        QTimer.singleShot(0, self._create_trading_interface)

    def _create_trading_interface(self):
        try:
            panel_module = importlib.import_module("gui_app.trading_interface_simple")
            trading_cls = getattr(panel_module, "TradingInterface")
            self.trading_interface = trading_cls()
            self.trading_interface.hide()
            if self.account_tab is not None:
                self.trading_interface.account_updated.connect(self.account_tab.update_account)
            if self.position_tab is not None:
                self.trading_interface.position_updated.connect(self.position_tab.update_positions)
            signal_bus.emit(Events.TRADING_INTERFACE_READY)
            self._schedule_preheat()
        finally:
            self._trading_interface_loading = False

    def _schedule_preheat(self):
        if self._preheat_scheduled or not _PREHEAT_CONFIG["enabled"]:
            return
        self._preheat_scheduled = True

        class _PreheatThread(QThread):
            finished = pyqtSignal()

            def run(self):
                time.sleep(_PREHEAT_CONFIG["delay_ms"] / 1000)
                self.finished.emit()

        self._preheat_thread = _PreheatThread()
        self._preheat_thread.finished.connect(self._on_preheat_ready)
        self._preheat_thread.start()

    def _on_preheat_ready(self):
        self._preheat_common_tabs()
        self._report_timing_summary()

    def _preheat_common_tabs(self):
        tabs_to_preheat = []
        for tab_key in _PREHEAT_CONFIG["tabs"]:
            try:
                for idx in range(self.tabs.count()):
                    w = self.tabs.widget(idx)
                    if w and w.property("core_key") == tab_key:
                        tabs_to_preheat.append((idx, tab_key))
                        break
            except Exception:
                pass

        if tabs_to_preheat:
            self._process_preheat_queue(tabs_to_preheat)
        else:
            self._report_timing_summary()

    def _process_preheat_queue(self, queue: list):
        if not queue:
            self._report_timing_summary()
            return

        idx, tab_key = queue.pop(0)
        try:
            # Check if still valid placeholder
            w = self.tabs.widget(idx)
            if w and w.property("core_key") == tab_key:
                self._do_create_core_tab(idx, tab_key)
        except Exception as e:
            _logger.warning(f"预热Tab失败 {tab_key}: {e}")

        # Schedule next one with delay to yield to event loop
        QTimer.singleShot(50, lambda: self._process_preheat_queue(queue))


    def _report_timing_summary(self):
        if not self._tab_timings:
            return
        total = sum(self._tab_timings.values())
        slow_tabs = {k: v for k, v in self._tab_timings.items() if v > 500}
        if slow_tabs:
            _logger.warning(f"慢Tab汇总: {slow_tabs}, 总耗时: {total:.0f}ms")
        else:
            _logger.info(f"Tab加载汇总: 总耗时 {total:.0f}ms")
        self._persist_timing_metrics()

    def _persist_timing_metrics(self):
        try:
            log_dir = Path.home() / ".easyxt" / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            metrics_file = log_dir / "tab_timings.json"
            _rotate_log_if_needed(
                metrics_file, _LOG_CONFIG["max_file_size_mb"], _LOG_CONFIG["max_files"]
            )
            metrics = {
                "timestamp": datetime.now().isoformat(),
                "timings": self._tab_timings,
                "retry_counts": self._retry_counts,
                "total_ms": sum(self._tab_timings.values()),
            }
            metrics.update(_APP_INFO)
            with open(metrics_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(metrics, ensure_ascii=False) + "\n")
            _logger.info(f"耗时指标已落盘: {metrics_file}")
            self._log_file_path = str(metrics_file)
        except Exception as e:
            _logger.warning(f"耗时指标落盘失败: {e}")
