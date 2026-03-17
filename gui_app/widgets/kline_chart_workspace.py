#!/usr/bin/env python3
import asyncio
import importlib
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional, cast
from urllib import request

import numpy as np
import pandas as pd
from PyQt5.QtCore import (
    QDate,
    QFileSystemWatcher,
    QSettings,
    QStringListModel,
    Qt,
    QThread,
    QTimer,
    pyqtSignal,
)
from PyQt5.QtWidgets import (
    QAction,
    QApplication,
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QCompleter,
    QDateEdit,
    QDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from core.events import Events
from core.signal_bus import signal_bus
from core.theme_manager import ThemeManager
from data_manager.duckdb_connection_pool import resolve_duckdb_path
from data_manager.realtime_pipeline_manager import RealtimePipelineManager
from easy_xt.realtime_data.persistence.duckdb_sink import RealtimeDuckDBSink
from gui_app.widgets.chart import (
    PERIOD_DATE_COL_MAP,
    PERIOD_TABLE_MAP,
    ChartEvents,
    PositionTable,
    SubchartManager,
    ToolboxPanel,
    create_chart_adapter,
)
from gui_app.widgets.chart.pipeline_guard import validate_pipeline_bar_for_period
from gui_app.widgets.orderbook_panel import OrderbookPanel
from gui_app.widgets.realtime_settings_dialog import RealtimeSettingsDialog


class _RealtimeQuoteWorker(QThread):
    """
    Worker thread to fetch realtime quotes without blocking the UI
    """

    quote_ready = pyqtSignal(dict, str)
    error_occurred = pyqtSignal(str, str)

    def __init__(self, api, symbol: str):
        super().__init__()
        self.api = api
        self.symbol = symbol
        self._xtdata_only = os.environ.get("EASYXT_RT_XTDATA_ONLY", "0") in ("1", "true", "True")
        self._xtdata_probe_enabled = os.environ.get("EASYXT_ENABLE_XTDATA_QUOTE_PROBE", "0") in ("1", "true", "True")

    def run(self):
        try:
            if not self.api or not self.symbol:
                self.error_occurred.emit(self.symbol, "invalid_worker_state")
                return
            # 优先本地 xtdata（低延迟、可直接补齐五档），避免被多源对冲竞速超时拖慢
            if self._xtdata_probe_enabled:
                fallback_xt = self._fetch_quote_from_xtdata(self.symbol)
                if fallback_xt and float(fallback_xt.get("price") or 0) > 0:
                    self.quote_ready.emit(fallback_xt, self.symbol)
                    return

            if not self._xtdata_only:
                fallback = self._fetch_quote_from_easyxt(self.symbol)
                if fallback and float(fallback.get("price") or 0) > 0:
                    self.quote_ready.emit(fallback, self.symbol)
                    return

                quotes = self._fetch_quote_from_realtime_api(timeout_s=1.2)
                if quotes and isinstance(quotes, list):
                    first = quotes[0] if quotes else {}
                    if isinstance(first, dict):
                        first = self._enrich_quote_with_xt_tick(first, self.symbol)
                    if isinstance(first, dict) and float(first.get("price") or 0) > 0:
                        self.quote_ready.emit(first, self.symbol)
                        return

            self.error_occurred.emit(self.symbol, "no_quote_data")
        except Exception as e:
            self.error_occurred.emit(self.symbol, f"quote_worker_error:{type(e).__name__}")

    def _fetch_quote_from_realtime_api(self, timeout_s: float = 1.2):
        from concurrent.futures import ThreadPoolExecutor, TimeoutError

        pool = ThreadPoolExecutor(max_workers=1)
        fut = pool.submit(self.api.get_realtime_quotes, [self.symbol])
        try:
            return fut.result(timeout=timeout_s)
        except TimeoutError:
            fut.cancel()
            return []
        except Exception:
            return []
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    def _fetch_quote_from_easyxt(self, symbol: str) -> Optional[dict[str, Any]]:
        try:
            import easy_xt

            # 若 api 单例尚未初始化完成，快速返回避免阻塞 Worker（㊶修复）
            if easy_xt.api is None:
                return None

            api = easy_xt.api
            data_api = getattr(api, "data", None)
            if data_api is None:
                return None

            df = data_api.get_current_price([symbol])
            if df is None or getattr(df, "empty", True):
                return None

            row = df.iloc[0]
            quote: dict[str, Any] = {
                "symbol": symbol,
                "price": float(row.get("price", 0) or 0),
                "open": float(row.get("open", 0) or 0),
                "high": float(row.get("high", 0) or 0),
                "low": float(row.get("low", 0) or 0),
                "volume": float(row.get("volume", 0) or 0),
                "amount": float(row.get("amount", 0) or 0),
            }

            try:
                xt_obj = getattr(data_api, "xt", None)
                if self._xtdata_probe_enabled and xt_obj is not None and hasattr(xt_obj, "get_full_tick"):
                    full_tick = xt_obj.get_full_tick([symbol]) or {}
                    tick = full_tick.get(symbol) or next(iter(full_tick.values()), None)
                    if isinstance(tick, dict):
                        self._fill_orderbook_from_tick(quote, tick)
            except Exception:
                pass

            return quote
        except Exception:
            return None

    def _fetch_quote_from_xtdata(self, symbol: str) -> Optional[dict[str, Any]]:
        try:
            import easy_xt

            # 若 broker 单例尚未初始化完成，快速返回避免阻塞 Worker（㊶修复）
            if easy_xt.xtquant_broker is None:
                return None

            broker = easy_xt.xtquant_broker
            full_tick = broker.get_full_tick([symbol]) or {}
            tick = full_tick.get(symbol) or next(iter(full_tick.values()), None)
            if not isinstance(tick, dict):
                return None
            price = float(
                tick.get("lastPrice")
                or tick.get("last_price")
                or tick.get("price")
                or 0
            )
            if price <= 0:
                return None
            quote: dict[str, Any] = {
                "symbol": symbol,
                "price": price,
                "open": float(tick.get("open") or tick.get("openPrice") or price),
                "high": float(tick.get("high") or tick.get("highPrice") or price),
                "low": float(tick.get("low") or tick.get("lowPrice") or price),
                "volume": float(tick.get("volume") or tick.get("vol") or 0),
                "amount": float(tick.get("amount") or tick.get("turnover") or 0),
            }
            self._fill_orderbook_from_tick(quote, tick)
            return quote
        except Exception:
            pass
        try:
            from xtquant import xtdata

            full_tick = xtdata.get_full_tick([symbol]) or {}
            tick = full_tick.get(symbol) or next(iter(full_tick.values()), None)
            if not isinstance(tick, dict):
                return None
            price = float(
                tick.get("lastPrice")
                or tick.get("last_price")
                or tick.get("price")
                or 0
            )
            if price <= 0:
                return None
            quote: dict[str, Any] = {
                "symbol": symbol,
                "price": price,
                "open": float(tick.get("open") or tick.get("openPrice") or price),
                "high": float(tick.get("high") or tick.get("highPrice") or price),
                "low": float(tick.get("low") or tick.get("lowPrice") or price),
                "volume": float(tick.get("volume") or tick.get("vol") or 0),
                "amount": float(tick.get("amount") or tick.get("turnover") or 0),
            }
            self._fill_orderbook_from_tick(quote, tick)
            return quote
        except Exception:
            return None

    def _enrich_quote_with_xt_tick(self, quote: dict[str, Any], symbol: str) -> dict[str, Any]:
        if not self._xtdata_probe_enabled:
            return quote
        if quote.get("ask1") not in (None, 0, "", "--") and quote.get("bid1") not in (None, 0, "", "--"):
            return quote
        fallback = self._fetch_quote_from_xtdata(symbol)
        if not fallback:
            return quote
        merged = dict(quote)
        for key, value in fallback.items():
            if key.startswith("ask") or key.startswith("bid"):
                merged[key] = value
        if float(merged.get("price") or 0) <= 0 and float(fallback.get("price") or 0) > 0:
            merged["price"] = fallback["price"]
            merged["open"] = fallback.get("open")
            merged["high"] = fallback.get("high")
            merged["low"] = fallback.get("low")
            merged["volume"] = fallback.get("volume")
            merged["amount"] = fallback.get("amount")
        return merged

    def _fill_orderbook_from_tick(self, quote: dict[str, Any], tick: dict[str, Any]) -> None:
        ask_prices = tick.get("askPrice") or tick.get("ask_price") or []
        bid_prices = tick.get("bidPrice") or tick.get("bid_price") or []
        ask_vols = tick.get("askVol") or tick.get("ask_volume") or []
        bid_vols = tick.get("bidVol") or tick.get("bid_volume") or []

        for i in range(5):
            level = i + 1
            if i < len(ask_prices):
                quote[f"ask{level}"] = ask_prices[i]
            if i < len(ask_vols):
                quote[f"ask{level}_vol"] = ask_vols[i]
            if i < len(bid_prices):
                quote[f"bid{level}"] = bid_prices[i]
            if i < len(bid_vols):
                quote[f"bid{level}_vol"] = bid_vols[i]


class _WsMarketQuoteWorker(QThread):
    """
    持久化 WebSocket 行情订阅客户端（异步事件循环跑在子线程）。

    连接建立后直接由服务端推送 tick，emit quote_ready 由 _on_quote_received 消费。
    断线后每 _RECONNECT_INTERVAL 秒自动重连；stop() 可安全终止（最长 1s 响应）。
    当 EASYXT_USE_WS_QUOTE=1（默认）时替代轮询 _RealtimeQuoteWorker。
    """

    quote_ready = pyqtSignal(dict, str)
    error_occurred = pyqtSignal(str, str)

    _RECV_TIMEOUT = 1.0        # recv 超时，用于轮询 _should_stop

    def __init__(
        self,
        symbol: str,
        port: int = 8000,
        reconnect_initial_s: float = 1.5,
        reconnect_max_s: float = 15.0,
        reconnect_factor: float = 1.8,
    ):
        super().__init__()
        self.symbol = symbol
        self._port = port
        self._should_stop = threading.Event()
        self._connected = threading.Event()  # WS 握手成功且在接收中时 set
        self._reconnect_initial_s = max(float(reconnect_initial_s), 0.1)
        self._reconnect_max_s = max(float(reconnect_max_s), self._reconnect_initial_s)
        self._reconnect_factor = max(float(reconnect_factor), 1.0)

    def stop(self) -> None:
        """终止事件循环，最长 1s 响应（受 _RECV_TIMEOUT 约束）。"""
        self._should_stop.set()

    def run(self) -> None:
        asyncio.run(self._client_loop())

    @staticmethod
    def _compute_reconnect_delay(
        failure_count: int,
        initial_s: float,
        max_s: float,
        factor: float,
    ) -> float:
        n = max(int(failure_count), 1) - 1
        delay = float(initial_s) * (float(factor) ** n)
        lower = max(float(initial_s), 0.1)
        upper = max(float(max_s), lower)
        return min(max(delay, lower), upper)

    async def _client_loop(self) -> None:
        try:
            import websockets
        except ImportError:
            self.error_occurred.emit(self.symbol, "ws_unavailable:websockets_not_installed")
            return

        url = f"ws://127.0.0.1:{self._port}/ws/market/{self.symbol}"
        failure_count = 0
        while not self._should_stop.is_set():
            try:
                ws_connect = cast(Any, websockets).connect
                async with ws_connect(url, ping_interval=20, open_timeout=5) as ws:
                    failure_count = 0
                    self._connected.set()
                    while not self._should_stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=self._RECV_TIMEOUT)
                            try:
                                msg = json.loads(raw)
                            except Exception:
                                continue
                            self.quote_ready.emit(msg, self.symbol)
                        except asyncio.TimeoutError:
                            pass  # 定期检查 _should_stop
            except Exception as exc:
                if not self._should_stop.is_set():
                    failure_count += 1
                    self.error_occurred.emit(self.symbol, f"ws_conn_error:{type(exc).__name__}")
            finally:
                self._connected.clear()  # 断连时立即清除，让轮询接管
            if not self._should_stop.is_set():
                # 等待重连间隔，可被 stop() 打断（0.2s 粒度）
                delay = self._compute_reconnect_delay(
                    failure_count=failure_count,
                    initial_s=self._reconnect_initial_s,
                    max_s=self._reconnect_max_s,
                    factor=self._reconnect_factor,
                )
                loop = asyncio.get_event_loop()
                deadline = loop.time() + delay
                while not self._should_stop.is_set():
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    await asyncio.sleep(min(0.2, remaining))


class KLineChartWorkspace(QWidget):
    source_status_ready = pyqtSignal(object)
    # Fix 71: 跨线程回调 — 替换 QTimer.singleShot (native 线程中无效) 为 pyqtSignal
    _subchart_results_ready = pyqtSignal(object)   # dict
    _subchart_last_bar_ready = pyqtSignal(object)  # dict
    _merge_chart_done = pyqtSignal(object, str, str)  # (DataFrame, symbol, period)
    _backfill_event_ready = pyqtSignal(object)

    def __init__(self, include_operation_panel: bool = True):
        super().__init__()
        self._logger = logging.getLogger(__name__)
        self.include_operation_panel = include_operation_panel
        self.chart = None
        self.chart_adapter = None
        self.trading_window: Optional[QWidget] = None
        self.trading_panel: Optional[QWidget] = None
        self.interface = None
        self.duckdb_path = resolve_duckdb_path()
        self.last_bar_time = None
        self.last_close = None
        self.last_data = pd.DataFrame()
        self.last_signal_key = None
        self._last_quote_monotonic = 0.0
        self.root_splitter: Optional[QSplitter] = None
        self.top_panel: Optional[QWidget] = None
        self.bottom_tabs: Optional[QTabWidget] = None
        self.last_bottom_size = 200
        self.last_nonzero_bottom_size = 200
        self.initial_split_applied = False
        self.min_top_ratio = 0.3
        self.max_bottom_ratio = 0.7
        self.last_bottom_ratio = 0.3
        self.update_timer = QTimer(self)
        self.update_timer.setSingleShot(True)
        self.update_timer.setInterval(400)
        self.update_timer.timeout.connect(self.refresh_latest_bar)
        self.file_watcher = QFileSystemWatcher(self)
        self.file_watcher.fileChanged.connect(self._on_duckdb_changed)
        self.subchart_manager: Optional[SubchartManager] = None
        self.toolbox_panel: Optional[ToolboxPanel] = None
        self.position_table: Optional[PositionTable] = None
        self.chart_events: Optional[ChartEvents] = None
        self.cost_line = None
        self.cost_line_symbol = None
        self.full_range_check: Optional[QCheckBox] = None
        self.start_date_label: Optional[QLabel] = None
        self.end_date_label: Optional[QLabel] = None
        self.theme_manager = ThemeManager()
        self.test_mode = bool(os.environ.get("PYTEST_CURRENT_TEST"))
        self.auto_load_chart = os.environ.get("EASYXT_AUTO_LOAD_CHART", "1") in (
            "1",
            "true",
            "True",
        )
        self.macd_visible = True
        self.rsi_visible = True
        self.vol_visible = False
        self.kdj_visible = False
        self.ma_visible = True
        self.boll_visible = False
        self.initial_data_loaded = False
        self.realtime_timer: Optional[QTimer] = None
        self.realtime_api = None
        self.realtime_last_total_volume: Optional[float] = None
        self.realtime_pipeline = RealtimePipelineManager(
            max_queue=int(os.environ.get("EASYXT_RT_MAX_QUEUE", "256")),
            flush_interval_ms=int(os.environ.get("EASYXT_RT_FLUSH_MS", "200")),
        )
        self.realtime_pipeline_timer: Optional[QTimer] = None
        self._bottom_tab_loaded: dict[int, bool] = {}
        self._bottom_tab_factories: list[tuple[Callable[[], QWidget], str]] = []

        # 添加自动降级相关属性
        self._degraded_mode = False  # 当前是否处于降级模式
        self._original_flush_interval = int(
            os.environ.get("EASYXT_RT_FLUSH_MS", "200")
        )  # 原始刷新间隔
        self._degraded_flush_interval = self._original_flush_interval * 2  # 降级时刷新间隔翻倍
        self._last_metrics_time = 0.0  # 上次检查指标时间
        self._metrics_check_interval = 5.0  # 指标检查间隔（秒）
        self._bottom_preload_enabled = os.environ.get("EASYXT_BOTTOMTAB_PRELOAD", "0") in (
            "1",
            "true",
            "True",
        )
        self._bottom_preload_tabs = [
            t.strip()
            for t in os.environ.get("EASYXT_BOTTOMTAB_PRELOAD_TABS", "").split(",")
            if t.strip()
        ]
        self._chart_load_thread: Optional[QThread] = None
        self._data_process_thread: Optional[QThread] = None
        self._latest_bar_thread: Optional[QThread] = None
        self._latest_bar_pending = False
        self._last_latest_bar_ts = 0.0
        self._latest_bar_cooldown_s = 2.0
        self._realtime_connect_thread: Optional[QThread] = None
        self._progressive_enabled = True
        self._segment_cache: dict[tuple[str, str, str, str, str], pd.DataFrame] = {}
        self._loading_segments: set[tuple[str, str, str, str, str]] = set()
        self._loaded_range: Optional[tuple[str, str]] = None
        self._full_range: Optional[tuple[str, str]] = None
        self._range_change_bound = False
        self._range_change_last_ts = 0.0
        self._range_change_threshold = int(os.environ.get("EASYXT_RANGE_THRESHOLD", "80"))
        self._interface_init_thread: Optional[QThread] = None
        self._interface_ready = False
        self._auto_fallback_attempted = False
        self._fallback_thread: Optional[QThread] = None
        self._quote_worker: Optional[_RealtimeQuoteWorker] = None
        self._ws_quote_worker: Optional[_WsMarketQuoteWorker] = None
        self._use_ws_quote: bool = os.environ.get("EASYXT_USE_WS_QUOTE", "1") in ("1", "true", "True")
        self._ws_error_consecutive: int = 0
        self._ws_error_emit_threshold: int = int(os.environ.get("EASYXT_WS_ERROR_ALERT_THRESHOLD", "3"))
        self._last_realtime_probe_line: Optional[str] = None
        self._orderbook_sink: Optional[RealtimeDuckDBSink] = None
        self.orderbook_panel: Optional[OrderbookPanel] = None
        self._source_status_timer: Optional[QTimer] = None
        self._source_status_refreshing = False
        self._period_fallback_attempted: set[tuple[str, str]] = set()
        # 200ms 去抖定时器：快速切换周期/复权时只触发一次 refresh_chart_data
        self._chart_refresh_timer = QTimer(self)
        self._chart_refresh_timer.setSingleShot(True)
        self._chart_refresh_timer.setInterval(200)
        self._chart_refresh_timer.timeout.connect(self.refresh_chart_data)
        # _on_data_processed 去抖：合并快速连续的数据处理信号
        self._data_debounce_timer: Optional[QTimer] = None
        self._pending_data_payload: Optional[dict] = None
        # 去重：跳过与上次完全相同的 chart.set 调用（㊸修复）
        self._last_chart_set_shape: Optional[tuple] = None
        self._backfill_retry_timer = QTimer(self)
        self._backfill_retry_timer.setSingleShot(True)
        self._backfill_retry_timer.setInterval(
            int(os.environ.get("EASYXT_BACKFILL_RETRY_MS", "3000"))
        )
        self._backfill_retry_timer.timeout.connect(self._on_backfill_retry_timeout)
        self._pending_backfill_retry: Optional[tuple[str, str, str, str, str]] = None
        self._backfill_retry_remaining = 0
        # ㊻修复：子图指标计算节流 —— 将 pandas 计算推到后台线程，最多每秒更新一次
        self._subchart_update_timer = QTimer(self)
        self._subchart_update_timer.setSingleShot(True)
        self._subchart_update_timer.setInterval(1000)
        self._subchart_update_timer.timeout.connect(self._do_deferred_subchart_update)
        self._subchart_pending_data: Optional[pd.DataFrame] = None
        # Fix 71: 连接跨线程信号
        self._subchart_results_ready.connect(self._apply_subchart_results)
        self._subchart_last_bar_ready.connect(self._apply_subchart_last_bar)
        self._merge_chart_done.connect(self._on_merge_chart_done)
        self._backfill_event_ready.connect(self._on_backfill_event_ui)
        self._flush_in_progress = False
        self._pipeline_apply_scheduled = False
        self._pending_pipeline_result: Optional[dict[str, Any]] = None
        self._pipeline_result_lock = threading.Lock()
        self.init_ui()
        self._connect_events()
        self.source_status_ready.connect(self._apply_source_status)
        self._emit_realtime_probe(connected=None, reason="workspace_ready")

    def _emit_realtime_probe(
        self,
        connected: Optional[bool],
        reason: str = "",
        quote_ts: Optional[str] = None,
        degraded: Optional[bool] = None,
    ):
        symbol = ""
        try:
            if hasattr(self, "symbol_input") and self.symbol_input is not None:
                symbol = self.symbol_input.text().strip()
        except Exception:
            symbol = ""
        payload = {
            "connected": connected,
            "reason": reason or ("ok" if connected is True else "unknown"),
            "quote_ts": quote_ts,
            "symbol": symbol,
            "source": "kline_workspace",
            "degraded": degraded,
        }
        line = (
            f"{payload['connected']}|{payload['reason']}|{payload.get('quote_ts') or ''}|"
            f"{payload['symbol']}|{payload.get('degraded')}"
        )
        if line == self._last_realtime_probe_line:
            return
        self._last_realtime_probe_line = line
        try:
            signal_bus.emit(Events.REALTIME_PIPELINE_STATUS_UPDATED, status=payload)
        except Exception:
            pass

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(6)
        self.root_splitter = QSplitter(Qt.Vertical)
        self.root_splitter.setChildrenCollapsible(True)
        self.root_splitter.setHandleWidth(12)
        self.root_splitter.setOpaqueResize(True)
        self.root_splitter.setStyleSheet("QSplitter::handle{background:#c8c8c8;}")
        layout.addWidget(self.root_splitter, 1)

        top_panel = QWidget()
        self.top_panel = top_panel
        top_layout = QVBoxLayout(top_panel)
        top_layout.setContentsMargins(0, 0, 0, 0)
        top_layout.setSpacing(6)
        controls = self._create_chart_controls()
        controls.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        controls.setFixedHeight(42)
        top_layout.addWidget(controls)
        chart_widget = self._create_chart_widget(self)
        chart_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        chart_widget.setMinimumHeight(0)
        chart_widget.setMinimumWidth(0)
        top_layout.addWidget(chart_widget, 1)
        self.root_splitter.addWidget(top_panel)

        if self.include_operation_panel:
            self.bottom_tabs = self._create_bottom_tabs()
            self.bottom_tabs.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            self.bottom_tabs.setMinimumHeight(0)
            self.bottom_tabs.setMinimumSize(0, 0)
            self.root_splitter.addWidget(self.bottom_tabs)
            self.root_splitter.setStretchFactor(0, 8)
            self.root_splitter.setStretchFactor(1, 1)
            self.root_splitter.setSizes([1200, 300])
            self.root_splitter.setCollapsible(0, False)
            self.root_splitter.setCollapsible(1, True)
            self.root_splitter.splitterMoved.connect(self._enforce_split_limits)
            QTimer.singleShot(0, self._apply_initial_split)
        else:
            self.bottom_tabs = None
            self.root_splitter.setCollapsible(0, False)
        self._start_source_status_timer()

    def _connect_events(self):
        signal_bus.subscribe(Events.SYMBOL_SELECTED, self.load_symbol)
        signal_bus.subscribe(Events.PERIOD_CHANGED, self.change_period)
        signal_bus.subscribe(Events.ORDER_SUBMITTED, self.mark_order)
        signal_bus.subscribe(Events.POSITION_UPDATED, self._on_position_updated)
        signal_bus.subscribe(Events.BACKFILL_TASK_UPDATED, self._on_backfill_event)
        signal_bus.subscribe(Events.DATA_INGESTION_COMPLETE, self._on_ingestion_complete)

    def _on_backfill_event(self, **payload):
        try:
            self._backfill_event_ready.emit(payload or {})
        except Exception:
            pass

    def _on_backfill_event_ui(self, payload: dict):
        try:
            stock_code = str(payload.get("stock_code") or "").strip()
            period = str(payload.get("period") or "").strip()
            status = str(payload.get("status") or "").strip()
            current_symbol = self.symbol_input.text().strip() if self.symbol_input is not None else ""
            current_period = self.period_combo.currentText() if self.period_combo is not None else ""
            if not stock_code or not period:
                return
            if stock_code != current_symbol or period != current_period:
                return
            if status == "success":
                self._set_orderbook_status(f"{stock_code} 历史补数完成，刷新图表...")
                QTimer.singleShot(0, self.refresh_chart_data)
            elif status == "failed":
                err = str(payload.get("error_message") or "").strip()
                suffix = f"({err})" if err else ""
                self._set_orderbook_status(f"{stock_code} 历史补数失败{suffix}，请检查数据源与日志")
        except Exception:
            pass

    def _on_ingestion_complete(self, **payload):
        """DATA_INGESTION_COMPLETE 事件：若当前标的在入库列表中则刷新状态和图表。"""
        try:
            stock_codes = payload.get("stock_codes") or []
            current_symbol = self.symbol_input.text().strip() if self.symbol_input is not None else ""
            if current_symbol and current_symbol in stock_codes:
                self._update_data_status_label()
                QTimer.singleShot(500, self.refresh_chart_data)
        except Exception:
            pass

    def _start_auto_data_sync(self, symbol: str) -> None:
        """图表打开时自动后台同步全历史数据（增量补充缺失部分，不阻塞 UI）。"""
        if not symbol:
            return
        # 取消/忽略上一个正在运行的同步（soft cancel）
        prev = getattr(self, "_sync_thread", None)
        if self._is_thread_running(prev):
            prev.requestInterruption()

        if hasattr(self, "_data_status_label"):
            self._data_status_label.setText("本地: 同步中…")
        if hasattr(self, "adjust_combo"):
            self.adjust_combo.setEnabled(False)

        class _SyncThread(QThread):
            sync_done = pyqtSignal()

            def __init__(self, duckdb_path: str, symbol: str) -> None:
                super().__init__()
                self._duckdb_path = duckdb_path
                self._symbol = symbol

            def run(self) -> None:
                try:
                    from data_manager.auto_data_updater import AutoDataUpdater
                    updater = AutoDataUpdater(duckdb_path=self._duckdb_path)
                    updater.bulk_download(stock_codes=[self._symbol])
                except Exception as exc:
                    logging.getLogger(__name__).warning(
                        "自动同步异常 [%s]: %s", self._symbol, exc
                    )
                finally:
                    if not self.isInterruptionRequested():
                        self.sync_done.emit()

        t = _SyncThread(self.duckdb_path, symbol)
        t.setParent(self)
        t.sync_done.connect(self._on_auto_sync_done)
        t.finished.connect(t.deleteLater)
        self._sync_thread = t
        t.start()

    def _is_thread_running(self, thread_obj) -> bool:
        if thread_obj is None:
            return False
        try:
            return bool(thread_obj.isRunning())
        except RuntimeError:
            return False
        except Exception:
            return False

    def _on_auto_sync_done(self) -> None:
        """后台同步完成后，恢复复权选择器并刷新数据范围标签。"""
        if hasattr(self, "adjust_combo"):
            self.adjust_combo.setEnabled(True)
        self._update_data_status_label()
        QTimer.singleShot(500, self.refresh_chart_data)

    def _update_data_status_label(self):
        """后台查询当前标的+周期的本地数据范围，更新状态标签。"""
        if not hasattr(self, "_data_status_label"):
            return
        symbol = self.symbol_input.text().strip() if self.symbol_input is not None else ""
        period = self.period_combo.currentText() if self.period_combo is not None else "1d"
        if not symbol:
            self._data_status_label.setText("本地: --")
            return

        label_ref = self._data_status_label
        duckdb_path = self.duckdb_path

        class _CovThread(QThread):
            result_ready = pyqtSignal(str)

            def __init__(self, duckdb_path: str, symbol: str, period: str):
                super().__init__()
                self._duckdb_path = duckdb_path
                self._symbol = symbol
                self._period = period

            def run(self):
                try:
                    from data_manager.unified_data_interface import UnifiedDataInterface
                    iface = UnifiedDataInterface(duckdb_path=self._duckdb_path)
                    df = iface.get_data_coverage(
                        stock_codes=[self._symbol],
                        periods=[self._period],
                    )
                    if df.empty or self._period not in df.columns:
                        self.result_ready.emit("本地: 无数据")
                        return
                    val = str(df.at[self._symbol, self._period]) if self._symbol in df.index else ""
                    self.result_ready.emit(f"本地: {val}" if val else "本地: 无数据")
                except Exception:
                    self.result_ready.emit("本地: --")

        prev = getattr(self, "_coverage_thread", None)
        if self._is_thread_running(prev):
            prev.requestInterruption()
        t = _CovThread(duckdb_path, symbol, period)
        t.setParent(self)
        t.result_ready.connect(label_ref.setText)
        t.finished.connect(t.deleteLater)
        self._coverage_thread = t
        t.start()

    def _bind_range_change_event(self):
        if self.chart is None or self._range_change_bound:
            return
        try:
            self.chart.events.range_change += self._on_range_change
            self._range_change_bound = True
        except Exception:
            self._logger.exception("Failed to bind range change event")

    def load_symbol(self, symbol: str, **kwargs):
        if not symbol:
            return
        self.symbol_input.setText(symbol)
        # 打开标的时立即触发后台自动同步（增量补充历史数据）
        self._start_auto_data_sync(symbol)
        if self.toolbox_panel:
            self.toolbox_panel.set_symbol(symbol)
        if self.chart_events:
            self.chart_events.set_symbol(symbol)
        # 标的切换时刷新 WS 行情订阅（仅当 realtime 模式已激活）
        if self._use_ws_quote and self.realtime_timer is not None and self.realtime_timer.isActive():
            self._restart_ws_quote_worker()
        self.refresh_chart_data()

    def change_period(self, period: str, **kwargs):
        if not period:
            return
        # 同步按钮组选中状态
        btn = self._period_buttons_map.get(period)
        if btn and not btn.isChecked():
            btn.setChecked(True)
        index = self.period_combo.findText(period)
        if index >= 0:
            if index == self.period_combo.currentIndex():
                self.refresh_chart_data()
            else:
                self.period_combo.setCurrentIndex(index)
        if self.chart_events:
            self.chart_events.set_period(period)

    def mark_order(self, side: str, symbol: str, price: float, volume: int, **kwargs):
        if self.chart is None:
            return
        current_symbol = self.symbol_input.text().strip()
        if symbol != current_symbol:
            return
        normalized_side = (side or "").lower()
        marker_text = f"{'📈' if normalized_side == 'buy' else '📉'} {normalized_side.upper()}"
        if self.chart_adapter:
            self.chart_adapter.marker(marker_text)
        else:
            self.chart.marker(text=marker_text)
        if self.position_table:
            self.position_table.update_from_order(symbol, normalized_side, price, volume)

    def _on_position_updated(self, positions: list[dict], **kwargs):
        if self.chart is None:
            return
        symbol = self._normalize_symbol(self.symbol_input.text().strip())
        if not symbol:
            return
        target = None
        for pos in positions or []:
            pos_symbol = pos.get("stock_code") or pos.get("symbol") or pos.get("code")
            if pos_symbol == symbol:
                target = pos
                break
        if not target:
            if self.cost_line is not None:
                self.cost_line.delete()
                self.cost_line = None
                self.cost_line_symbol = None
            return
        cost_price = target.get("cost_price")
        if cost_price is None:
            cost_price = target.get("avg_price") or target.get("open_price")
        if cost_price is None:
            return
        if self.cost_line is None or self.cost_line_symbol != symbol:
            if self.cost_line is not None:
                self.cost_line.delete()
            self.cost_line = self.chart.horizontal_line(cost_price, text="成本线")
            self.cost_line_symbol = symbol
        else:
            self.cost_line.update(cost_price)
            self.cost_line.options(text="成本线")

    def _create_left_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)

        BacktestWidget = self._dynamic_class("gui_app.widgets.backtest_widget", "BacktestWidget")
        JQ2QMTWidget = self._dynamic_class("gui_app.widgets.jq2qmt_widget", "JQ2QMTWidget")
        JQToPtradeWidget = self._dynamic_class(
            "gui_app.widgets.jq_to_ptrade_widget", "JQToPtradeWidget"
        )
        GridTradingWidget = self._dynamic_class(
            "gui_app.widgets.grid_trading_widget", "GridTradingWidget"
        )
        ConditionalOrderWidget = self._dynamic_class(
            "gui_app.widgets.conditional_order_widget", "ConditionalOrderWidget"
        )

        tabs = QTabWidget()
        tabs.addTab(BacktestWidget(), "回测分析")
        tabs.addTab(GridTradingWidget(), "网格交易")
        tabs.addTab(ConditionalOrderWidget(), "条件单")
        tabs.addTab(JQ2QMTWidget(), "JQ2QMT")
        tabs.addTab(JQToPtradeWidget(), "JQ转Ptrade")

        layout.addWidget(tabs)
        return panel

    def _create_bottom_tabs(self) -> QTabWidget:
        tabs = QTabWidget()

        self._bottom_tab_factories = [
            (
                lambda: self._dynamic_class("gui_app.widgets.backtest_widget", "BacktestWidget")(),
                "回测分析",
            ),
            (
                lambda: self._dynamic_class(
                    "gui_app.widgets.grid_trading_widget", "GridTradingWidget"
                )(),
                "网格交易",
            ),
            (
                lambda: self._dynamic_class(
                    "gui_app.widgets.conditional_order_widget", "ConditionalOrderWidget"
                )(),
                "条件单",
            ),
            (
                lambda: self._dynamic_class("gui_app.widgets.jq2qmt_widget", "JQ2QMTWidget")(),
                "JQ2QMT",
            ),
            (
                lambda: self._dynamic_class(
                    "gui_app.widgets.jq_to_ptrade_widget", "JQToPtradeWidget"
                )(),
                "JQ转Ptrade",
            ),
            (
                lambda: self._dynamic_class(
                    "gui_app.widgets.local_data_manager_widget", "LocalDataManagerWidget"
                )(),
                "数据管理",
            ),
            (
                lambda: self._dynamic_class(
                    "gui_app.widgets.advanced_data_viewer_widget", "AdvancedDataViewerWidget"
                )(),
                "数据查看",
            ),
            (self._create_trading_panel, "交易面板"),
        ]

        for _, title in self._bottom_tab_factories:
            placeholder = QLabel(f"点击进入 {title}...")
            placeholder.setAlignment(Qt.AlignCenter)
            placeholder.setStyleSheet("color: #666; font-size: 14px;")
            tabs.addTab(placeholder, title)

        tabs.currentChanged.connect(self._on_bottom_tab_changed)
        if self._bottom_preload_enabled:
            QTimer.singleShot(200, self._preload_bottom_tabs)
        return tabs

    def _preload_bottom_tabs(self):
        if not self.bottom_tabs:
            return
        if not self._bottom_preload_tabs:
            return
        title_to_index = {}
        for idx in range(self.bottom_tabs.count()):
            title_to_index[self.bottom_tabs.tabText(idx)] = idx
        for title in self._bottom_preload_tabs:
            index = title_to_index.get(title)
            if index is None:
                continue
            self._on_bottom_tab_changed(index)

    def _relax_tab_constraints(self, tabs: QTabWidget):
        for index in range(tabs.count()):
            page = tabs.widget(index)
            if page is not None:
                self._relax_widget_constraints(page)

    def _relax_current_tab_constraints(self, index: int):
        if not self.bottom_tabs:
            return
        page = self.bottom_tabs.widget(index)
        if page is not None:
            self._relax_widget_constraints(page)

    def _on_bottom_tab_changed(self, index: int):
        if not self.bottom_tabs:
            return
        if self._bottom_tab_loaded.get(index):
            return
        self._bottom_tab_loaded[index] = True
        QTimer.singleShot(0, lambda: self._load_bottom_tab(index))

    def _load_bottom_tab(self, index: int):
        try:
            tabs = self.bottom_tabs
            if tabs is None:
                return
            if index < 0 or index >= len(self._bottom_tab_factories):
                return
            factory, title = self._bottom_tab_factories[index]
            widget = factory() if callable(factory) else None
            if widget is None:
                return
            tabs.removeTab(index)
            tabs.insertTab(index, widget, title)
            tabs.setCurrentIndex(index)
            self._relax_widget_constraints(widget)
        except Exception:
            pass

    def _relax_widget_constraints(self, widget: QWidget):
        widget.setMinimumSize(0, 0)
        widget.setMaximumHeight(16777215)
        widget.setMaximumWidth(16777215)
        for child in widget.findChildren(QWidget):
            child.setMinimumSize(0, 0)
            child.setMaximumHeight(16777215)
            child.setMaximumWidth(16777215)

    # ======================================================================
    # 全局周期列表 (KLineChart Pro 风格) — 按钮显示文本 → 内部 period key
    # ======================================================================
    _PERIOD_BUTTONS: list[tuple[str, str]] = [
        ("1m", "1m"),
        ("5m", "5m"),
        ("15m", "15m"),
        ("30m", "30m"),
        ("60m", "60m"),
        ("日K", "1d"),
        ("周K", "1w"),
        ("月K", "1M"),
        ("Tick", "tick"),
    ]

    _PERIOD_BTN_STYLE = """
        QPushButton {
            border: none; background: transparent; color: #aaa;
            padding: 3px 7px; font-size: 12px; border-radius: 3px;
        }
        QPushButton:hover { color: #fff; background: rgba(255,255,255,0.08); }
        QPushButton:checked { color: #4fc3f7; font-weight: bold; background: rgba(79,195,247,0.12); }
    """

    _TOOLBAR_LABEL_STYLE = "color: #888; font-size: 11px;"

    _INDICATOR_BTN_STYLE = """
        QPushButton {
            border: 1px solid #555; background: transparent; color: #ccc;
            padding: 3px 10px; font-size: 12px; border-radius: 3px;
        }
        QPushButton:hover { border-color: #4fc3f7; color: #fff; }
        QPushButton::menu-indicator { width: 0; height: 0; }
    """

    def _create_chart_controls(self) -> QWidget:
        panel = QWidget()
        panel.setStyleSheet("QWidget { background: #1e1e2e; }")
        layout = QHBoxLayout(panel)
        layout.setContentsMargins(6, 2, 6, 2)
        layout.setSpacing(4)

        # ── 标的搜索 ──
        self.symbol_input = QLineEdit("000001.SZ")
        self.symbol_input.setPlaceholderText("输入代码/名称")
        self.symbol_input.setFixedWidth(130)
        self.symbol_input.setStyleSheet(
            "QLineEdit { background:#2a2a3e; color:#eee; border:1px solid #444; "
            "border-radius:3px; padding:3px 6px; font-size:12px; }"
            "QLineEdit:focus { border-color:#4fc3f7; }"
        )
        self._symbol_completer_model = QStringListModel([], self)
        self._symbol_completer = QCompleter(self._symbol_completer_model, self)
        self._symbol_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._symbol_completer.setFilterMode(Qt.MatchContains)
        self._symbol_completer.setMaxVisibleItems(12)
        self.symbol_input.setCompleter(self._symbol_completer)

        layout.addWidget(self.symbol_input)
        self._add_toolbar_separator(layout)

        # ── 周期按钮组 (KLineChart Pro 风格) ──
        self._period_btn_group = QButtonGroup(self)
        self._period_btn_group.setExclusive(True)
        self._period_buttons_map: dict[str, QPushButton] = {}
        # 向后兼容：创建一个隐藏的 period_combo
        self.period_combo = QComboBox()
        self.period_combo.setVisible(False)
        for label, key in self._PERIOD_BUTTONS:
            self.period_combo.addItem(key)
        layout.addWidget(self.period_combo)

        for label, key in self._PERIOD_BUTTONS:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setStyleSheet(self._PERIOD_BTN_STYLE)
            btn.clicked.connect(lambda checked, k=key: self._on_period_btn_clicked(k))
            self._period_btn_group.addButton(btn)
            self._period_buttons_map[key] = btn
            layout.addWidget(btn)
        # 默认选中 "日K"
        if "1d" in self._period_buttons_map:
            self._period_buttons_map["1d"].setChecked(True)

        # ── 日内扩展周期下拉菜单（2m/10m/20m/25m/50m/70m/120m/125m）──
        self._intraday_extra_btn = QPushButton("日内▾")
        self._intraday_extra_btn.setStyleSheet(self._PERIOD_BTN_STYLE)
        self._intraday_extra_menu = QMenu(self)
        self._intraday_extra_menu.setStyleSheet(
            "QMenu { background:#2a2a3e; color:#ccc; border:1px solid #555; padding:4px; }"
            "QMenu::item { padding:4px 16px; }"
            "QMenu::item:selected { background:#4fc3f7; color:#000; }"
        )
        for _ilabel, _ikey in [
            ("2分", "2m"), ("10分", "10m"), ("20分", "20m"), ("25分", "25m"),
            ("50分", "50m"), ("70分", "70m"), ("2小时", "120m"), ("125分", "125m"),
        ]:
            act = self._intraday_extra_menu.addAction(_ilabel)
            act.setData(_ikey)
            act.triggered.connect(lambda _checked=False, k=_ikey: self._on_period_btn_clicked(k))
        self._intraday_extra_btn.setMenu(self._intraday_extra_menu)
        layout.addWidget(self._intraday_extra_btn)

        # ── 多日/长周期下拉菜单（2d/3d/5d/10d/25d/50d/75d/2M/3M/5M + 1Q/6M/1Y）──
        self._multiday_btn = QPushButton("多日▾")
        self._multiday_btn.setStyleSheet(self._PERIOD_BTN_STYLE)
        self._multiday_menu = QMenu(self)
        self._multiday_menu.setStyleSheet(
            "QMenu { background:#2a2a3e; color:#ccc; border:1px solid #555; padding:4px; }"
            "QMenu::item { padding:4px 16px; }"
            "QMenu::item:selected { background:#4fc3f7; color:#000; }"
            "QMenu::separator { height:1px; background:#444; margin:2px 8px; }"
        )
        for _mlabel, _mkey in [
            ("2日", "2d"), ("3日", "3d"), ("5日（≠周K）", "5d"), ("10日", "10d"),
            ("25日", "25d"), ("50日", "50d"), ("75日", "75d"),
        ]:
            act = self._multiday_menu.addAction(_mlabel)
            act.setData(_mkey)
            act.triggered.connect(lambda _checked=False, k=_mkey: self._on_period_btn_clicked(k))
        self._multiday_menu.addSeparator()
        for _mlabel, _mkey in [
            ("2月(≈42交易日)", "2M"), ("3月(≈63交易日)", "3M"), ("5月(≈105交易日)", "5M"),
        ]:
            act = self._multiday_menu.addAction(_mlabel)
            act.setData(_mkey)
            act.triggered.connect(lambda _checked=False, k=_mkey: self._on_period_btn_clicked(k))
        self._multiday_menu.addSeparator()
        for _mlabel, _mkey in [
            ("季K", "1Q"), ("半年K", "6M"), ("年K", "1Y"),
            ("2年K", "2Y"), ("3年K", "3Y"), ("5年K", "5Y"), ("10年K", "10Y"),
        ]:
            act = self._multiday_menu.addAction(_mlabel)
            act.setData(_mkey)
            act.triggered.connect(lambda _checked=False, k=_mkey: self._on_period_btn_clicked(k))
        self._multiday_btn.setMenu(self._multiday_menu)
        layout.addWidget(self._multiday_btn)

        self._add_toolbar_separator(layout)

        # ── 复权切换 ──
        self.adjust_combo = QComboBox()
        self.adjust_combo.addItems(["不复权", "前复权", "后复权"])
        self.adjust_combo.setStyleSheet(
            "QComboBox { background:#2a2a3e; color:#ccc; border:1px solid #444; "
            "border-radius:3px; padding:2px 6px; font-size:11px; min-width:60px; }"
            "QComboBox:hover { border-color:#4fc3f7; }"
            "QComboBox::drop-down { border:none; }"
            "QComboBox QAbstractItemView { background:#2a2a3e; color:#ccc; selection-background-color:#4fc3f7; }"
        )
        self._adjust_display_to_key = {"不复权": "none", "前复权": "front", "后复权": "back"}
        self._adjust_key_to_display = {v: k for k, v in self._adjust_display_to_key.items()}
        layout.addWidget(self.adjust_combo)

        self._add_toolbar_separator(layout)

        # ── 指标选择器按钮 (弹出菜单) ──
        self._indicator_btn = QPushButton("指标 ▾")
        self._indicator_btn.setStyleSheet(self._INDICATOR_BTN_STYLE)
        self._indicator_menu = QMenu(self)
        self._indicator_menu.setStyleSheet(
            "QMenu { background:#2a2a3e; color:#ccc; border:1px solid #555; padding:4px; }"
            "QMenu::item { padding:4px 16px; }"
            "QMenu::item:selected { background:#4fc3f7; color:#000; }"
            "QMenu::indicator { width:14px; height:14px; }"
            "QMenu::indicator:checked { background:#4fc3f7; border:1px solid #4fc3f7; border-radius:2px; }"
            "QMenu::indicator:unchecked { background:transparent; border:1px solid #666; border-radius:2px; }"
        )
        self._build_indicator_menu()
        self._indicator_btn.setMenu(self._indicator_menu)
        layout.addWidget(self._indicator_btn)

        self._add_toolbar_separator(layout)

        # ── 实时/自动交易/操作面板 ──
        self.auto_update_check = QCheckBox("实时")
        self.auto_update_check.setStyleSheet("QCheckBox { color:#aaa; font-size:11px; }")
        self.auto_update_check.stateChanged.connect(self._toggle_auto_update)
        self.auto_update_check.setChecked(True)
        self.auto_update_check.setVisible(False)
        self.auto_trade_check = QCheckBox("自动交易")
        self.auto_trade_check.setStyleSheet("QCheckBox { color:#aaa; font-size:11px; }")
        self.auto_trade_check.setToolTip("基于均线/突破信号触发下单，未勾选时仅打标记")
        self.position_table_check = QCheckBox("持仓")
        self.position_table_check.setStyleSheet("QCheckBox { color:#aaa; font-size:11px; }")
        self.position_table_check.setChecked(True)
        self.position_table_check.stateChanged.connect(self._toggle_position_table)

        layout.addWidget(self.auto_update_check)
        layout.addWidget(self.auto_trade_check)
        layout.addWidget(self.position_table_check)

        self._add_toolbar_separator(layout)

        # ── 主题切换 ──
        self.theme_combo = QComboBox()
        self.theme_combo.addItem("☾", "dark")
        self.theme_combo.addItem("☀", "light")
        self.theme_combo.setStyleSheet(
            "QComboBox { background:transparent; color:#aaa; border:none; font-size:14px; min-width:30px; }"
            "QComboBox::drop-down { border:none; }"
            "QComboBox QAbstractItemView { background:#2a2a3e; color:#ccc; }"
        )
        self.theme_combo.currentIndexChanged.connect(self._on_theme_changed)
        layout.addWidget(self.theme_combo)

        # ── 源状态 / 监控设置 ──
        self.source_status_label = QLabel("源: --")
        self.source_status_label.setStyleSheet("color:#666; font-size:10px;")
        self.source_status_label.setToolTip("数据源健康状态")
        layout.addWidget(self.source_status_label)

        self.rt_settings_button = QPushButton("⚙")
        self.rt_settings_button.setToolTip("实时管道监控参数设置")
        self.rt_settings_button.setStyleSheet(
            "QPushButton { border:none; color:#888; font-size:14px; padding:2px 4px; }"
            "QPushButton:hover { color:#4fc3f7; }"
        )
        self.rt_settings_button.clicked.connect(self._open_rt_settings)
        layout.addWidget(self.rt_settings_button)

        self.toggle_bottom_button = None
        if self.include_operation_panel:
            self.toggle_bottom_button = QPushButton("面板")
            self.toggle_bottom_button.setStyleSheet(
                "QPushButton { border:1px solid #555; color:#aaa; padding:2px 8px; "
                "font-size:11px; border-radius:3px; background:transparent; }"
                "QPushButton:hover { border-color:#4fc3f7; color:#fff; }"
            )
            self.toggle_bottom_button.clicked.connect(self._toggle_bottom_panel)
            layout.addWidget(self.toggle_bottom_button)

        # ── 隐藏的日期控件 (保持向后兼容) ──
        self.start_date_edit = QDateEdit(QDate.currentDate().addYears(-1))
        self.start_date_edit.setCalendarPopup(True)
        self.end_date_edit = QDateEdit(QDate.currentDate())
        self.end_date_edit.setCalendarPopup(True)
        self.start_date_label = QLabel("开始")
        self.end_date_label = QLabel("结束")
        self.start_date_label.setVisible(False)
        self.end_date_label.setVisible(False)
        self.start_date_edit.setVisible(False)
        self.end_date_edit.setVisible(False)
        self.full_range_check = None
        self.refresh_button = QPushButton("加载")
        self.refresh_button.setVisible(False)
        self.refresh_button.clicked.connect(self.refresh_chart_data)
        self.orderbook_label = QLabel("五档: --")
        self.orderbook_label.setVisible(False)
        # 为指标复选框保持向后兼容引用
        self.macd_check = QCheckBox()
        self.macd_check.setVisible(False)
        self.macd_check.setChecked(self.macd_visible)
        self.rsi_check = QCheckBox()
        self.rsi_check.setVisible(False)
        self.rsi_check.setChecked(self.rsi_visible)

        layout.addStretch(1)

        # ── 本地数据状态（开图时自动后台同步，无需手动补数）──
        self._data_status_label = QLabel("本地: --")
        self._data_status_label.setStyleSheet("color:#6a9fb5; font-size:10px; padding:0 4px;")
        self._data_status_label.setToolTip(
            "当前标的本周期在 DuckDB 中的数据范围。\n"
            "打开标的时自动后台同步全历史数据（增量补充缺失部分）；\n"
            "同步期间复权选择器暂时禁用，完成后自动恢复。"
        )
        layout.addWidget(self._data_status_label)
        self._sync_thread: Optional[QThread] = None
        self._coverage_thread: Optional[QThread] = None

        # ── 信号连接 ──
        self.symbol_input.returnPressed.connect(self.refresh_chart_data)
        self.symbol_input.editingFinished.connect(self._on_chart_params_changed)
        self.period_combo.currentIndexChanged.connect(self._on_chart_params_changed)
        self.adjust_combo.currentIndexChanged.connect(self._on_chart_params_changed)
        self._toggle_full_range()
        self._load_persisted_state()

        # 异步加载标的补全列表
        threading.Thread(target=self._load_symbol_completions, daemon=True).start()

        return panel

    # ── 指标菜单构建 ──
    def _build_indicator_menu(self):
        menu = self._indicator_menu
        # 主图叠加
        self._ma_action = QAction("MA 均线 (5/10/20/60)", menu)
        self._ma_action.setCheckable(True)
        self._ma_action.setChecked(self.ma_visible)
        self._ma_action.triggered.connect(lambda c: self._toggle_indicator("ma", c))
        menu.addAction(self._ma_action)
        self._boll_action = QAction("BOLL 布林带", menu)
        self._boll_action.setCheckable(True)
        self._boll_action.setChecked(self.boll_visible)
        self._boll_action.triggered.connect(lambda c: self._toggle_indicator("boll", c))
        menu.addAction(self._boll_action)
        menu.addSeparator()
        # 副图
        self._macd_action = QAction("MACD", menu)
        self._macd_action.setCheckable(True)
        self._macd_action.setChecked(self.macd_visible)
        self._macd_action.triggered.connect(lambda c: self._toggle_indicator("macd", c))
        menu.addAction(self._macd_action)
        self._rsi_action = QAction("RSI", menu)
        self._rsi_action.setCheckable(True)
        self._rsi_action.setChecked(self.rsi_visible)
        self._rsi_action.triggered.connect(lambda c: self._toggle_indicator("rsi", c))
        menu.addAction(self._rsi_action)
        self._vol_action = QAction("VOL 成交量", menu)
        self._vol_action.setCheckable(True)
        self._vol_action.setChecked(self.vol_visible)
        self._vol_action.triggered.connect(lambda c: self._toggle_indicator("vol", c))
        menu.addAction(self._vol_action)
        self._kdj_action = QAction("KDJ", menu)
        self._kdj_action.setCheckable(True)
        self._kdj_action.setChecked(self.kdj_visible)
        self._kdj_action.triggered.connect(lambda c: self._toggle_indicator("kdj", c))
        menu.addAction(self._kdj_action)

    def _toggle_indicator(self, name: str, checked: bool):
        setattr(self, f"{name}_visible", checked)
        # 同步复选框 (向后兼容)
        if name == "macd" and self.macd_check:
            self.macd_check.setChecked(checked)
        elif name == "rsi" and self.rsi_check:
            self.rsi_check.setChecked(checked)
        self._apply_indicator_visibility()

    def _apply_indicator_visibility(self):
        if self.subchart_manager:
            self.subchart_manager.set_visibility(
                macd=self.macd_visible,
                rsi=self.rsi_visible,
                vol=self.vol_visible,
                kdj=self.kdj_visible,
                ma=self.ma_visible,
                boll=self.boll_visible,
            )
            if self.last_data is not None and not self.last_data.empty:
                self._request_subchart_update(self.last_data, full_set=True)

    def _on_period_btn_clicked(self, period_key: str):
        """周期按钮点击 → 同步隐藏的 period_combo → 触发刷新。
        支持自定义扩展周期（日内/多日）——若 period_combo 中尚无该项则动态追加。"""
        idx = self.period_combo.findText(period_key)
        if idx < 0:
            # 自定义周期（2m/25m/5d/1Q 等）首次使用时动态加入 combo
            self.period_combo.addItem(period_key)
            idx = self.period_combo.findText(period_key)
        if idx >= 0:
            if idx == self.period_combo.currentIndex():
                self.refresh_chart_data()
            else:
                self.period_combo.setCurrentIndex(idx)
        if self.chart_events:
            self.chart_events.set_period(period_key)

    def _add_toolbar_separator(self, layout: QHBoxLayout):
        sep = QFrame()
        sep.setFrameShape(QFrame.VLine)
        sep.setStyleSheet("color: #444;")
        sep.setFixedHeight(20)
        layout.addWidget(sep)

    # ── ㊻修复: 子图指标后台计算 + 节流 ──
    def _request_subchart_update(self, data: pd.DataFrame, full_set: bool = False):
        """节流入口: 存储待更新数据，启动 1 秒节流定时器
        full_set=True  → 首次加载/切换周期，使用 compute_all + apply_precomputed (.set)
        full_set=False → 实时 tick 更新，使用 compute_last_bar + apply_last_bar (.update)
        """
        self._subchart_pending_data = data
        # 只允许升级（False→True），禁止降级（True→False）。
        # 若周期切换发出 full_set=True 后，1 秒内实时 tick 产生 full_set=False，
        # 不应覆盖 True，否则 apply_precomputed 永远不会执行，子图一直保留旧周期
        # 数据，每次 update() 触发 JS "Cannot update oldest data" 错误洪流。
        if full_set:
            self._subchart_full_set = True
        # full_set=False 时：若已有 True 等待中则保持 True，否则保持 False
        if not self._subchart_update_timer.isActive():
            self._subchart_update_timer.start()

    def _do_deferred_subchart_update(self):
        """节流定时器到期 → 将指标计算推到后台线程"""
        data = self._subchart_pending_data
        full_set = getattr(self, "_subchart_full_set", False)
        self._subchart_pending_data = None
        self._subchart_full_set = False
        manager = self.subchart_manager
        if data is None or data.empty or manager is None:
            return
        data_copy = data.copy()
        threading.Thread(
            target=self._compute_subchart_bg, args=(manager, data_copy, full_set), daemon=True
        ).start()

    def _compute_subchart_bg(self, manager: SubchartManager, data: pd.DataFrame, full_set: bool = False):
        """后台线程: 执行所有指标 pandas 计算 (Fix 71: 用 pyqtSignal 跨线程回主线程)"""
        try:
            if full_set:
                results = manager.compute_all(data)
                self._subchart_results_ready.emit(results)  # Fix 71
            else:
                # Fix 55: 增量更新 — 仅计算最后一行，用 .update() 代替 .set()
                results = manager.compute_last_bar(data)
                self._subchart_last_bar_ready.emit(results)  # Fix 71
        except Exception:
            self._logger.exception("_compute_subchart_bg failed")

    def _apply_subchart_results(self, results: dict):
        """主线程: 仅将预计算结果发送到 WebView (无 pandas 计算) — 全量 .set()"""
        if self.subchart_manager is None:
            return
        try:
            self.subchart_manager.apply_precomputed(results)
        except Exception:
            pass

    def _apply_subchart_last_bar(self, results: dict):
        """主线程: 增量 .update(单行) — Fix 55"""
        if self.subchart_manager is None:
            return
        try:
            self.subchart_manager.apply_last_bar(results)
        except Exception:
            pass

    def _load_symbol_completions(self):
        """后台线程: 从 DuckDB 加载股票代码列表供自动补全"""
        try:
            if not os.path.exists(self.duckdb_path):
                return
            from data_manager.duckdb_connection_pool import get_db_manager
            with get_db_manager(self.duckdb_path).get_read_connection() as con:
                df = con.execute(
                    "SELECT DISTINCT stock_code FROM stock_daily ORDER BY stock_code LIMIT 5000"
                ).df()
            if df is not None and not df.empty:
                codes = df["stock_code"].astype(str).tolist()
                QTimer.singleShot(0, lambda: self._symbol_completer_model.setStringList(codes))
        except Exception:
            pass

    def showEvent(self, event):
        super().showEvent(event)
        if not self.initial_data_loaded:
            self.initial_data_loaded = True
            QTimer.singleShot(0, self._load_default_chart_data)

        # Resume realtime polling if enabled
        if self.auto_update_check and self.auto_update_check.isChecked():
            if self.realtime_timer and not self.realtime_timer.isActive():
                self.realtime_timer.start(3000)

        if self.include_operation_panel:
            self._apply_initial_split()
            for delay in (0, 50, 150, 300, 600):
                QTimer.singleShot(delay, self._enforce_split_limits)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self.include_operation_panel:
            self._enforce_split_limits()

    def _apply_initial_split(self):
        if not self.include_operation_panel:
            return
        if self.initial_split_applied or not self.root_splitter:
            return
        total = self.root_splitter.size().height()
        if total <= 0:
            total = self.height()
        if total <= 0:
            total = 800
        min_top = int(total * self.min_top_ratio)
        top_size = max(int(total * 0.7), min_top)
        bottom_size = max(total - top_size, 0)
        self.root_splitter.setSizes([top_size, bottom_size])
        self.initial_split_applied = True
        QTimer.singleShot(0, self._enforce_split_limits)

    def _enforce_split_limits(self):
        if not self.include_operation_panel or not self.root_splitter:
            return
        sizes = self.root_splitter.sizes()
        if len(sizes) < 2:
            return
        total = sum(sizes)
        if total <= 0:
            total = self.root_splitter.height()
        if total <= 0:
            return
        min_top = int(total * self.min_top_ratio)
        max_bottom = int(total * self.max_bottom_ratio)
        if self.top_panel:
            self.top_panel.setMinimumHeight(min_top)
        if self.bottom_tabs:
            self.bottom_tabs.setMinimumHeight(0)
        top_size = sizes[0]
        bottom_size = sizes[1]
        if bottom_size > max_bottom:
            bottom_size = max_bottom
            top_size = max(total - bottom_size, min_top)
            self.root_splitter.setSizes([top_size, bottom_size])
        if top_size < min_top:
            top_size = min_top
            bottom_size = max(total - top_size, 0)
            if bottom_size > max_bottom:
                bottom_size = max_bottom
                top_size = max(total - bottom_size, min_top)
            self.root_splitter.setSizes([top_size, bottom_size])
        if bottom_size >= 0:
            self.last_bottom_size = bottom_size
            if total > 0:
                self.last_bottom_ratio = bottom_size / total
        if bottom_size > 0:
            self.last_nonzero_bottom_size = bottom_size
        self._sync_bottom_policy(bottom_size)
        if self.bottom_tabs:
            self._relax_current_tab_constraints(self.bottom_tabs.currentIndex())
        if bottom_size < 0:
            bottom_size = 0
            top_size = total
            self.root_splitter.setSizes([top_size, bottom_size])

    def _sync_bottom_policy(self, bottom_size: int):
        if not self.bottom_tabs:
            return
        if bottom_size <= 5:
            self.bottom_tabs.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Ignored)
            self.bottom_tabs.setMinimumHeight(0)
            self.bottom_tabs.setMinimumSize(0, 0)
            self.bottom_tabs.setMaximumHeight(0)
        else:
            self.bottom_tabs.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            self.bottom_tabs.setMinimumHeight(0)
            self.bottom_tabs.setMinimumSize(0, 0)
            self.bottom_tabs.setMaximumHeight(16777215)

    def _toggle_bottom_panel(self):
        if not self.include_operation_panel or not self.root_splitter or not self.bottom_tabs:
            return
        sizes = self.root_splitter.sizes()
        if len(sizes) < 2:
            return
        total = max(sum(sizes), 1)
        min_top = int(total * self.min_top_ratio)
        max_bottom = min(int(total * self.max_bottom_ratio), max(total - min_top, 0))
        if sizes[1] <= 5:
            target_size = (
                int(total * self.last_bottom_ratio) if self.last_bottom_ratio > 0 else max_bottom
            )
            restore_size = (
                self.last_nonzero_bottom_size if self.last_nonzero_bottom_size > 0 else target_size
            )
            bottom_size = min(max(restore_size, 0), max_bottom)
            top_size = max(total - bottom_size, min_top)
            self._sync_bottom_policy(bottom_size)
        else:
            if sizes[1] > 0:
                self.last_bottom_size = sizes[1]
                self.last_nonzero_bottom_size = sizes[1]
                self.last_bottom_ratio = sizes[1] / total
            top_size = total
            bottom_size = 0
            self._sync_bottom_policy(bottom_size)
        self.root_splitter.setSizes([top_size, bottom_size])
        self._enforce_split_limits()

    def _create_chart_widget(self, parent: QWidget) -> QWidget:
        if self.test_mode:
            frame = QFrame()
            frame.setFrameStyle(QFrame.StyledPanel)
            layout = QVBoxLayout(frame)
            label = QLabel("测试环境已禁用图表渲染")
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(label)
            return frame
        try:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            external_lwc_path = os.path.join(project_root, "external", "lightweight-charts-python")
            if external_lwc_path not in sys.path:
                sys.path.insert(0, external_lwc_path)

            from PyQt5.QtWebEngineWidgets import QWebEngineView

            _ = QWebEngineView
            QtChart = self._dynamic_class("lightweight_charts.widgets", "QtChart")
            self.chart = QtChart(parent, toolbox=True)
            self.chart_adapter = create_chart_adapter(self.chart)
            self.subchart_manager = SubchartManager(self.chart)
            # 若当前为原生后端，将 adapter 传给 ToolboxPanel 以启用原生画线持久化
            from gui_app.widgets.chart.chart_adapter import NativeLwcChartAdapter
            _native = self.chart_adapter if isinstance(self.chart_adapter, NativeLwcChartAdapter) else None
            self.toolbox_panel = ToolboxPanel(
                self.chart,
                symbol=self.symbol_input.text().strip(),
                native_adapter=_native,
            )
            self.position_table = PositionTable(self.chart)
            if self.position_table_check is not None:
                self.position_table.set_visible(self.position_table_check.isChecked())
            self.chart_events = ChartEvents(self.chart, enable_topbar=False)
            self.chart_events.bind_signal_bus()
            self._bind_range_change_event()
            self._apply_chart_theme("dark")
            if self.subchart_manager:
                self.subchart_manager.set_visibility(
                    macd=self.macd_visible, rsi=self.rsi_visible,
                    vol=self.vol_visible, kdj=self.kdj_visible,
                    ma=self.ma_visible, boll=self.boll_visible,
                )
            webview = self.chart.get_webview()
            webview.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            webview.setMinimumHeight(0)
            webview.setMinimumWidth(0)
            self.orderbook_panel = OrderbookPanel()
            splitter = QSplitter(Qt.Horizontal)
            splitter.setChildrenCollapsible(True)
            splitter.addWidget(webview)
            splitter.addWidget(self.orderbook_panel)
            splitter.setStretchFactor(0, 6)
            splitter.setStretchFactor(1, 2)
            splitter.setSizes([900, 260])
            container = QWidget()
            container_layout = QVBoxLayout(container)
            container_layout.setContentsMargins(0, 0, 0, 0)
            container_layout.setSpacing(0)
            container_layout.addWidget(splitter)
            self._load_default_chart_data()
            return container
        except Exception as exc:
            self._logger.exception("Chart initialization failed")
            frame = QFrame()
            frame.setFrameStyle(QFrame.StyledPanel)
            frame.setMinimumHeight(520)
            layout = QVBoxLayout(frame)
            label = QLabel(f"图表引擎初始化失败: {exc}")
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            label.setStyleSheet("font-size: 14px; color: #666666;")
            layout.addWidget(label)
            return frame

    def _load_default_chart_data(self):
        if self.chart is None:
            return
        if not self.auto_load_chart:
            return
        self.refresh_chart_data()

    def _create_right_panel(self) -> QWidget:
        """创建右侧面板（懒加载模式）"""
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)

        # 创建 Tab 组件（延迟初始化子组件）
        self._right_tabs = QTabWidget()

        # 为每个标签页创建占位符
        tab_names = ["数据管理", "数据查看", "交易面板"]
        for tab_name in tab_names:
            placeholder = QLabel(f"点击进入 {tab_name}...")
            placeholder.setAlignment(Qt.AlignCenter)
            placeholder.setStyleSheet("color: #666; font-size: 14px;")
            self._right_tabs.addTab(placeholder, tab_name)

        # 连接懒加载
        self._right_tabs.currentChanged.connect(self._on_right_tab_changed)

        layout.addWidget(self._right_tabs)
        return panel

    def _on_right_tab_changed(self, index: int):
        """右侧标签页切换 - 懒加载"""
        if not hasattr(self, "_right_tab_loaded"):
            self._right_tab_loaded = {}

        if index in self._right_tab_loaded:
            return  # 已经加载过

        self._right_tab_loaded[index] = True

        # 延迟加载（避免阻塞）
        QTimer.singleShot(0, lambda: self._load_right_tab(index))

    def _load_right_tab(self, index: int):
        """加载右侧标签页内容"""
        try:
            tab_names = ["数据管理", "数据查看", "交易面板"]
            tab_name = tab_names[index]

            # 创建实际组件
            if index == 0:  # 数据管理
                LocalDataManagerWidget = self._dynamic_class(
                    "gui_app.widgets.local_data_manager_widget", "LocalDataManagerWidget"
                )
                widget = LocalDataManagerWidget()
            elif index == 1:  # 数据查看
                AdvancedDataViewerWidget = self._dynamic_class(
                    "gui_app.widgets.advanced_data_viewer_widget", "AdvancedDataViewerWidget"
                )
                widget = AdvancedDataViewerWidget()
            else:  # 交易面板
                widget = self._create_trading_panel_lazy()

            # 替换占位符
            self._right_tabs.removeTab(index)
            self._right_tabs.insertTab(index, widget, tab_name)
            self._right_tabs.setCurrentIndex(index)

        except Exception as e:
            self._logger.error(f"加载右侧标签页失败: {e}", exc_info=True)

    def _create_trading_panel_lazy(self) -> QWidget:
        """延迟创建交易面板"""
        TradingInterface = self._dynamic_class(
            "gui_app.trading_interface_simple", "TradingInterface"
        )
        self.trading_window = TradingInterface()
        trading_window = cast(Any, self.trading_window)
        panel = trading_window.centralWidget()
        if panel is None:
            panel = QWidget()
        trading_window.setCentralWidget(QWidget())
        panel.setParent(self)
        self.trading_panel = panel
        return panel

    def _create_trading_panel(self) -> QWidget:
        TradingInterface = self._dynamic_class(
            "gui_app.trading_interface_simple", "TradingInterface"
        )
        self.trading_window = TradingInterface()
        trading_window = cast(Any, self.trading_window)
        panel = trading_window.centralWidget()
        if panel is None:
            panel = QWidget()
        trading_window.setCentralWidget(QWidget())
        panel.setParent(self)
        self.trading_panel = panel
        return panel

    def _toggle_auto_update(self, state: int):
        if state == Qt.CheckState.Checked:
            self.refresh_chart_data()
            self._add_watch_path()
            self._start_realtime_polling()
        else:
            self._remove_watch_path()
            self._stop_realtime_polling()

    def _toggle_position_table(self, state: int):
        if not self.position_table:
            return
        self.position_table.set_visible(state == Qt.CheckState.Checked)

    def _toggle_subcharts(self, state: int):
        self.macd_visible = self.macd_check.isChecked()
        self.rsi_visible = self.rsi_check.isChecked()
        # 同步菜单 checked 状态
        if hasattr(self, "_macd_action"):
            self._macd_action.setChecked(self.macd_visible)
        if hasattr(self, "_rsi_action"):
            self._rsi_action.setChecked(self.rsi_visible)
        self._apply_indicator_visibility()

    def _add_watch_path(self):
        if os.path.exists(self.duckdb_path):
            if self.duckdb_path not in self.file_watcher.files():
                self.file_watcher.addPath(self.duckdb_path)

    def _remove_watch_path(self):
        if self.duckdb_path in self.file_watcher.files():
            self.file_watcher.removePath(self.duckdb_path)

    def _on_duckdb_changed(self, path: str):
        if self.auto_update_check is not None and not self.auto_update_check.isChecked():
            return
        # 实时模式下由 realtime_pipeline 驱动图表更新，跳过文件变更触发的全量刷新
        if self.realtime_timer is not None and self.realtime_timer.isActive():
            return
        if os.path.exists(path):
            self._add_watch_path()
        if not self.update_timer.isActive():
            self.update_timer.start()

    def _start_realtime_polling(self):
        if self.realtime_timer is None:
            self.realtime_timer = QTimer(self)
            self.realtime_timer.setInterval(1000)
            self.realtime_timer.timeout.connect(self._poll_realtime_quote)
        if self.realtime_pipeline_timer is None:
            self.realtime_pipeline_timer = QTimer(self)
            self.realtime_pipeline_timer.setInterval(
                int(os.environ.get("EASYXT_RT_FLUSH_MS", "200"))
            )
            self.realtime_pipeline_timer.timeout.connect(self._flush_realtime_pipeline)
        if not self.realtime_timer.isActive():
            self._emit_realtime_probe(connected=None, reason="realtime_connecting")
            self._ensure_realtime_api()
            self.realtime_timer.start()
            self._remove_watch_path()
        if self.realtime_pipeline_timer is not None and not self.realtime_pipeline_timer.isActive():
            self.realtime_pipeline_timer.start()
        if self._use_ws_quote:
            self._restart_ws_quote_worker()

    def _stop_realtime_polling(self):
        if self.realtime_timer and self.realtime_timer.isActive():
            self.realtime_timer.stop()
        if self.realtime_pipeline_timer and self.realtime_pipeline_timer.isActive():
            self.realtime_pipeline_timer.stop()
        self._stop_ws_quote_worker()
        self._add_watch_path()

    def _restart_ws_quote_worker(self) -> None:
        """停止旧 WS worker（若有），以当前标的和端口重新启动。"""
        self._stop_ws_quote_worker()
        symbol = self._normalize_symbol(self.symbol_input.text().strip())
        if not symbol:
            return
        port = int(os.environ.get("EASYXT_API_PORT", "8000"))
        reconnect_initial_s = float(os.environ.get("EASYXT_WS_RECONNECT_INITIAL", "1.5"))
        reconnect_max_s = float(os.environ.get("EASYXT_WS_RECONNECT_MAX", "15"))
        reconnect_factor = float(os.environ.get("EASYXT_WS_RECONNECT_FACTOR", "1.8"))
        self._ws_quote_worker = _WsMarketQuoteWorker(
            symbol,
            port=port,
            reconnect_initial_s=reconnect_initial_s,
            reconnect_max_s=reconnect_max_s,
            reconnect_factor=reconnect_factor,
        )
        self._ws_quote_worker.quote_ready.connect(self._on_quote_received)
        self._ws_quote_worker.error_occurred.connect(self._on_quote_error)
        self._ws_quote_worker.start()

    def _stop_ws_quote_worker(self) -> None:
        """安全停止当前 WS worker，最长等待 2s；超时则强制终止防止 QThread 析构崩溃。"""
        w = self._ws_quote_worker
        if w is None:
            return
        self._ws_quote_worker = None
        w.stop()
        if self._is_thread_running(w):
            if not w.wait(2000):   # asyncio recv_timeout=1s + WS cleanup，给够 2s
                w.terminate()      # 超时则强制终止，避免 QThread Destroyed while running
                w.wait(500)

    def _flush_realtime_pipeline(self):
        if self.chart is None:
            return
        # Fix 54: flush() 内部有 _last_data.copy() 等重操作，推到后台线程
        if self._flush_in_progress:
            return
        if not self.realtime_pipeline._queue:
            # 快速短路：队列为空则无需 flush
            self._check_metrics_periodically()
            return
        self._flush_in_progress = True
        threading.Thread(target=self._bg_flush_pipeline, daemon=True).start()
        self._check_metrics_periodically()

    def _bg_flush_pipeline(self):
        try:
            result = self.realtime_pipeline.flush(force=False)
            if result:
                self._schedule_pipeline_apply(result)
        except Exception:
            pass
        finally:
            self._flush_in_progress = False

    def _bg_flush_pipeline_force(self):
        try:
            result = self.realtime_pipeline.flush(force=True)
            if result:
                self._schedule_pipeline_apply(result)
        except Exception:
            pass
        finally:
            self._flush_in_progress = False

    def _schedule_pipeline_apply(self, result: dict[str, Any]):
        with self._pipeline_result_lock:
            self._pending_pipeline_result = result
            if self._pipeline_apply_scheduled:
                return
            self._pipeline_apply_scheduled = True
        QTimer.singleShot(0, self._drain_pipeline_apply)

    def _drain_pipeline_apply(self):
        while True:
            with self._pipeline_result_lock:
                result = self._pending_pipeline_result
                self._pending_pipeline_result = None
                if result is None:
                    self._pipeline_apply_scheduled = False
                    return
            self._apply_pipeline_result(result)

    def _check_metrics_periodically(self):
        current_time = time.monotonic()
        if current_time - self._last_metrics_time >= self._metrics_check_interval:
            self._last_metrics_time = current_time
            self._check_and_apply_degradation()

    def _check_and_apply_degradation(self):
        """检查实时管道指标并应用降级/恢复策略"""
        if self.realtime_pipeline is None:
            return

        metrics = self.realtime_pipeline.metrics()
        sustained_alert = metrics.get("sustained_drop_alert", False)

        if sustained_alert and not self._degraded_mode:
            # 持续告警，进入降级模式
            self._enter_degraded_mode()
        elif not sustained_alert and self._degraded_mode:
            # 已恢复且当前处于降级模式，退出降级模式
            self._exit_degraded_mode()

    def _log_degrade_event(self, mode: str, interval_ms: int):
        symbol = ""
        try:
            if hasattr(self, "symbol_input") and self.symbol_input is not None:
                symbol = self.symbol_input.text().strip()
        except Exception:
            symbol = ""
        threading.Thread(
            target=self._log_degrade_event_worker,
            args=(mode, interval_ms, symbol),
            daemon=True,
        ).start()

    @staticmethod
    def _log_degrade_event_worker(mode: str, interval_ms: int, symbol: str):
        try:
            log_dir = os.path.join(Path(__file__).parents[2], "logs")
            os.makedirs(log_dir, exist_ok=True)
            log_path = os.path.join(log_dir, "realtime_degrade.log")
            alerts_path = os.path.join(log_dir, "alerts.log")
            ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
            line = f"{ts}\t{mode}\tinterval={interval_ms}ms\tsymbol={symbol}\n"
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(line)
            alert_line = (
                f"{ts}\tREALTIME_DEGRADE\t{mode}\tinterval={interval_ms}ms\tsymbol={symbol}\n"
            )
            with open(alerts_path, "a", encoding="utf-8") as f:
                f.write(alert_line)
        except Exception:
            pass

    def _enter_degraded_mode(self):
        """进入降级模式"""
        if self._degraded_mode:
            return  # 已经在降级模式

        self._degraded_mode = True
        new_interval = self._degraded_flush_interval

        print(f"[INFO] 进入降级模式 - 刷新间隔调整为 {new_interval}ms")  # noqa: T201

        # 更新实时管道的刷新间隔
        if self.realtime_pipeline_timer:
            self.realtime_pipeline_timer.setInterval(new_interval)

        # 更新实时管道配置
        if self.realtime_pipeline:
            self.realtime_pipeline.update_config(flush_interval_ms=new_interval)

        # 更新状态栏提示
        self._emit_realtime_probe(
            connected=True,
            reason=f"降级模式 - 间隔{new_interval}ms",
            quote_ts=pd.Timestamp.now().strftime("%H:%M:%S"),
            degraded=True,
        )
        self._log_degrade_event("degraded", new_interval)

        # 触发AlertManager告警
        self._trigger_degrade_alert(alert_type="degraded", interval=new_interval)

    def _exit_degraded_mode(self):
        """退出降级模式"""
        if not self._degraded_mode:
            return  # 不在降级模式

        self._degraded_mode = False
        original_interval = self._original_flush_interval

        print(f"[INFO] 退出降级模式 - 刷新间隔恢复为 {original_interval}ms")  # noqa: T201

        # 恢复原始刷新间隔
        if self.realtime_pipeline_timer:
            self.realtime_pipeline_timer.setInterval(original_interval)

        # 恢复实时管道配置
        if self.realtime_pipeline:
            self.realtime_pipeline.update_config(flush_interval_ms=original_interval)

        # 更新状态栏提示
        self._emit_realtime_probe(
            connected=True,
            reason=f"正常模式 - 间隔{original_interval}ms",
            quote_ts=pd.Timestamp.now().strftime("%H:%M:%S"),
            degraded=False,
        )
        self._log_degrade_event("recovered", original_interval)

        # 解决AlertManager告警
        self._resolve_degrade_alert(alert_type="degraded")

    def _post_monitor_payload(self, payload: dict):
        dashboard_url = os.environ.get("EASYXT_MONITOR_DASHBOARD_URL", "").strip()
        if not dashboard_url:
            return
        if dashboard_url.endswith("/api/alerts/ingest"):
            ingest_url = dashboard_url
        else:
            ingest_url = dashboard_url.rstrip("/") + "/api/alerts/ingest"

        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        def _send():
            req = request.Request(
                ingest_url,
                data=data,
                headers={"Content-Type": "application/json"},
            )
            try:
                request.urlopen(req, timeout=5)
            except Exception:
                pass

        threading.Thread(target=_send, daemon=True).start()

    def _trigger_degrade_alert(self, alert_type: str = "degraded", interval: int = 0):
        payload = {
            "rule_name": "realtime_degrade",
            "status": "triggered",
            "level": "warning",
            "title": f"实时管道降级 - {alert_type}",
            "message": f"实时管道进入降级模式，刷新间隔调整为 {interval}ms",
            "value": float(interval),
            "threshold": 0.0,
            "source": "kline_chart_workspace",
            "tags": {"type": alert_type, "component": "realtime_pipeline"},
        }
        self._post_monitor_payload(payload)

    def _resolve_degrade_alert(self, alert_type: str = "degraded"):
        payload = {
            "rule_name": "realtime_degrade",
            "status": "resolved",
            "message": f"实时管道恢复正常 - {alert_type}",
            "source": "kline_chart_workspace",
            "tags": {"type": alert_type, "component": "realtime_pipeline"},
        }
        self._post_monitor_payload(payload)

    def _ensure_realtime_api(self):
        if self.test_mode:
            return  # 测试环境跳过实盘 API 连接，防止 _RealtimeConnectThread 阻塞/崩溃
        if self.realtime_api is not None:
            return
        if self._is_thread_running(self._realtime_connect_thread):
            return
        connector = _RealtimeConnectThread()
        connector.setParent(self)
        connector.finished.connect(connector.deleteLater)
        connector.ready.connect(self._on_realtime_ready)
        connector.error_occurred.connect(self._on_realtime_error)
        self._realtime_connect_thread = connector
        connector.start()

    def _on_realtime_ready(self, api: Any):
        self.realtime_api = api
        self._emit_realtime_probe(connected=True, reason="realtime_api_ready")
        self._refresh_source_status()

    def _on_realtime_error(self, message: str):
        self._logger.warning(f"实时行情连接失败: {message}")
        self._emit_realtime_probe(connected=False, reason=message or "realtime_api_error")
        loaded = self._load_orderbook_snapshot_from_db(reason="realtime_api_error")
        if not loaded:
            self._set_orderbook_status(f"实时行情不可用: {message}")

    def _poll_realtime_quote(self):
        if self.chart is None or self.interface is None:
            return
        if self.auto_update_check is None or not self.auto_update_check.isChecked():
            self._emit_realtime_probe(connected=None, reason="auto_update_disabled")
            return
        # WS 推送模式激活且链路在线时跳过轮询（避免双重采集）
        # 注意：_ws_quote_worker.isRunning() 在重连期间也为 True，因此用 _connected 事件判断
        if self._ws_quote_worker is not None and self._ws_quote_worker._connected.is_set():
            if self._last_quote_monotonic > 0 and (time.monotonic() - self._last_quote_monotonic) < 2.5:
                return
        # If a quote request is already pending/running, skip this poll
        if self._is_thread_running(self._quote_worker):
            return

        self._ensure_realtime_api()
        if self.realtime_api is None:
            return

        symbol = self._normalize_symbol(self.symbol_input.text().strip())
        if not symbol:
            return

        # Fix 53: 使用本地 _normalize_symbol 代替 easy_xt.utils 的运行时 import
        normalized_symbol = symbol

        # Use worker to fetch quotes asynchronously
        self._quote_worker = _RealtimeQuoteWorker(self.realtime_api, normalized_symbol)
        self._quote_worker.quote_ready.connect(self._on_quote_received)
        self._quote_worker.error_occurred.connect(self._on_quote_error)
        self._quote_worker.start()

    def _on_quote_received(self, quote: dict, symbol: str):
        quote = self._normalize_realtime_quote(quote)
        self._ws_error_consecutive = 0
        self._last_quote_monotonic = time.monotonic()
        quote_ts = pd.Timestamp.now().strftime("%H:%M:%S")
        self._emit_realtime_probe(connected=True, reason="quote_ok", quote_ts=quote_ts)
        if self.chart is None:
            return
        period = self.period_combo.currentText() if self.period_combo is not None else "1d"
        # Fix 58: 只在 symbol/period 变化时才重新 configure，避免每秒在主线程做 tail().copy()
        cfg_key = (symbol, period)
        if not hasattr(self, "_last_configure_key") or self._last_configure_key != cfg_key:
            self.realtime_pipeline.configure(symbol=symbol, period=period, last_data=self.last_data)
            self._last_configure_key = cfg_key
        self.realtime_pipeline.enqueue_quote(quote)
        if self.last_data is None or self.last_data.empty:
            # Fix 58: force-flush 也推到后台线程，与 _bg_flush_pipeline 保持一致
            if not self._flush_in_progress:
                self._flush_in_progress = True
                threading.Thread(target=self._bg_flush_pipeline_force, daemon=True).start()

    def _apply_pipeline_result(self, result: dict[str, Any]):
        action = result.get("action")
        bar = result.get("bar")
        data = result.get("data")
        quote = result.get("quote") or {}
        if data is not None and not getattr(data, "empty", True):
            self.last_data = data
            self.last_bar_time = data.iloc[-1].get("time")
            try:
                self.last_close = float(data.iloc[-1].get("close"))
            except Exception:
                pass
        elif action == "update" and isinstance(bar, dict):
            # Fix 56: flush() 不再返回 data 全量拷贝，用 bar 就地更新 last_data
            if self.last_data is not None and not self.last_data.empty:
                last_time = self.last_data.iloc[-1].get("time")
                try:
                    new_ts = pd.to_datetime(bar.get("time"), errors="coerce")
                    last_ts = pd.to_datetime(last_time, errors="coerce")
                except Exception:
                    new_ts = pd.NaT
                    last_ts = pd.NaT
                if pd.notna(new_ts) and pd.notna(last_ts) and new_ts < last_ts:
                    return
                if last_time == bar.get("time"):
                    self.last_data.iloc[-1] = bar
                else:
                    # Fix 58: 用 loc[] 就地追加，避免 pd.concat 的 O(n) 全量拷贝
                    self.last_data.loc[len(self.last_data)] = bar
                self.last_bar_time = bar.get("time")
                try:
                    self.last_close = float(bar.get("close", 0))
                except Exception:
                    pass
        if action == "init":
            if self.chart_adapter:
                self.chart_adapter.set_data(self.last_data)
            elif self.chart is not None:
                self.chart.set(self.last_data)
            self._request_subchart_update(self.last_data, full_set=True)
        elif action == "update" and isinstance(bar, dict):
            current_period = self.period_combo.currentText() if self.period_combo is not None else "1d"
            ok_bar, guard_reason = validate_pipeline_bar_for_period(bar, current_period)
            if not ok_bar:
                self._logger.warning(
                    "pipeline bar guard rejected: symbol=%s period=%s reason=%s bar_time=%s",
                    self.symbol_input.text().strip() if self.symbol_input is not None else "",
                    current_period,
                    guard_reason,
                    bar.get("time"),
                )
                try:
                    signal_bus.emit(
                        Events.DATA_QUALITY_ALERT,
                        stock_code=self.symbol_input.text().strip() if self.symbol_input is not None else "",
                        period=current_period,
                        level="warning",
                        reason="pipeline_bar_guard_reject",
                        details={"guard_reason": guard_reason, "bar_time": str(bar.get("time"))},
                    )
                except Exception:
                    pass
                return
            if self.chart_adapter:
                self.chart_adapter.update_data(pd.Series(bar))
            elif self.chart is not None:
                self.chart.update(pd.Series(bar))
            self._request_subchart_update(self.last_data)
        metrics = result.get("metrics") or {}
        if metrics:
            reason = (
                f"queue:{metrics.get('queue_len', 0)}/{metrics.get('max_queue', 0)} "
                f"dropped:{metrics.get('dropped_quotes', 0)} "
                f"drop_rate:{metrics.get('drop_rate', 0)}% "
                f"window_drop:{metrics.get('window_drop_rate', 0)}% "
                f"window_n:{metrics.get('window_total_quotes', 0)} "
                f"threshold:{metrics.get('drop_rate_threshold', 0)}% "
                f"sustain:{metrics.get('alert_sustain_s', 0)}s "
                f"alert:{metrics.get('sustained_drop_alert', False)}"
            )
            quote_ts = pd.Timestamp.now().strftime("%H:%M:%S")
            self._emit_realtime_probe(
                connected=True, reason=reason, quote_ts=quote_ts, degraded=self._degraded_mode
            )
        self._update_orderbook(quote)

    def _on_quote_error(self, symbol: str, reason: str):
        reason_text = reason or "quote_error"
        if str(reason_text).startswith("ws_conn_error"):
            self._ws_error_consecutive += 1
            if self._ws_error_consecutive < max(1, self._ws_error_emit_threshold):
                return
        self._emit_realtime_probe(connected=False, reason=reason_text)
        self._load_orderbook_snapshot_from_db(reason=reason_text)

    def _apply_realtime_quote(self, quote: dict, symbol: str):
        quote = self._normalize_realtime_quote(quote)
        price = float(quote.get("price") or 0)
        if price <= 0:
            return
        period = self.period_combo.currentText()
        now = pd.Timestamp.now()
        if period in ("1d", "1w", "1M"):
            bar_time = now.strftime("%Y-%m-%d")
        elif period == "1m":
            bar_time = now.floor("min")
        elif period == "5m":
            bar_time = now.floor("5min")
        elif period == "15m":
            bar_time = now.floor("15min")
        elif period == "30m":
            bar_time = now.floor("30min")
        elif period == "60m":
            bar_time = now.floor("60min")
        else:
            bar_time = now
        total_volume = float(quote.get("volume") or 0)
        if (
            self.realtime_last_total_volume is None
            or total_volume < self.realtime_last_total_volume
        ):
            self.realtime_last_total_volume = total_volume
        volume_delta = max(total_volume - (self.realtime_last_total_volume or 0), 0)
        self.realtime_last_total_volume = total_volume

        if self.last_data is None or self.last_data.empty:
            return
        last_row = self.last_data.iloc[-1].copy()
        last_time = last_row.get("time")
        try:
            new_ts = pd.to_datetime(bar_time, errors="coerce")
            last_ts = pd.to_datetime(last_time, errors="coerce")
        except Exception:
            new_ts = pd.NaT
            last_ts = pd.NaT
        if pd.notna(new_ts) and pd.notna(last_ts) and new_ts < last_ts:
            return
        if last_time == bar_time:
            high = max(float(last_row["high"]), price, float(quote.get("high") or price))
            low = min(float(last_row["low"]), price, float(quote.get("low") or price))
            open_price = float(last_row["open"])
            volume = float(last_row.get("volume") or 0) + volume_delta
            updated = {
                "time": bar_time,
                "open": open_price,
                "high": high,
                "low": low,
                "close": price,
                "volume": volume,
            }
            for col, value in updated.items():
                self.last_data.at[self.last_data.index[-1], col] = value
        else:
            open_price = float(quote.get("open") or price)
            high = max(price, float(quote.get("high") or price))
            low = min(price, float(quote.get("low") or price))
            updated = {
                "time": bar_time,
                "open": open_price,
                "high": high,
                "low": low,
                "close": price,
                "volume": volume_delta,
            }
            self.last_data = pd.concat([self.last_data, pd.DataFrame([updated])], ignore_index=True)

        if self.chart_adapter:
            self.chart_adapter.update_data(pd.Series(updated))
        elif self.chart is not None:
            self.chart.update(pd.Series(updated))
        self._request_subchart_update(self.last_data)
        self.last_close = price
        self.last_bar_time = bar_time
        self._update_orderbook(quote)

    def _normalize_realtime_quote(self, quote: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(quote, dict):
            return {}
        raw = quote.get("data") if isinstance(quote.get("data"), dict) else quote
        normalized: dict[str, Any] = dict(raw)

        price = (
            raw.get("price")
            or raw.get("lastPrice")
            or raw.get("last_price")
            or raw.get("close")
            or raw.get("current")
            or 0
        )
        normalized["price"] = float(price or 0)
        normalized["open"] = float(raw.get("open") or raw.get("openPrice") or normalized["price"] or 0)
        normalized["high"] = float(raw.get("high") or raw.get("highPrice") or normalized["price"] or 0)
        normalized["low"] = float(raw.get("low") or raw.get("lowPrice") or normalized["price"] or 0)
        normalized["volume"] = float(raw.get("volume") or raw.get("vol") or 0)
        normalized["amount"] = float(raw.get("amount") or raw.get("turnover") or 0)

        ask_prices = raw.get("askPrice") or raw.get("ask_price") or []
        bid_prices = raw.get("bidPrice") or raw.get("bid_price") or []
        ask_vols = raw.get("askVol") or raw.get("ask_volume") or []
        bid_vols = raw.get("bidVol") or raw.get("bid_volume") or []
        for i in range(5):
            level = i + 1
            if i < len(ask_prices):
                normalized[f"ask{level}"] = ask_prices[i]
            if i < len(ask_vols):
                normalized[f"ask{level}_vol"] = ask_vols[i]
            if i < len(bid_prices):
                normalized[f"bid{level}"] = bid_prices[i]
            if i < len(bid_vols):
                normalized[f"bid{level}_vol"] = bid_vols[i]

            if f"ask{level}" not in normalized:
                normalized[f"ask{level}"] = raw.get(f"ask{level}") or raw.get(f"sell{level}") or raw.get(f"a{level}_p")
            if f"bid{level}" not in normalized:
                normalized[f"bid{level}"] = raw.get(f"bid{level}") or raw.get(f"buy{level}") or raw.get(f"b{level}_p")
            if f"ask{level}_vol" not in normalized:
                normalized[f"ask{level}_vol"] = (
                    raw.get(f"ask{level}_vol")
                    or raw.get(f"ask{level}_volume")
                    or raw.get(f"sell{level}_vol")
                    or raw.get(f"a{level}_v")
                )
            if f"bid{level}_vol" not in normalized:
                normalized[f"bid{level}_vol"] = (
                    raw.get(f"bid{level}_vol")
                    or raw.get(f"bid{level}_volume")
                    or raw.get(f"buy{level}_vol")
                    or raw.get(f"b{level}_v")
                )
        return normalized

    def _update_orderbook(self, quote: dict):
        quote = self._normalize_realtime_quote(quote)
        if hasattr(self, "orderbook_panel") and self.orderbook_panel is not None:
            self.orderbook_panel.update_orderbook(quote)
            return
        if not hasattr(self, "orderbook_label") or self.orderbook_label is None:
            return
        asks = []
        bids = []
        for level in range(5, 0, -1):
            price = quote.get(f"ask{level}")
            volume = quote.get(f"ask{level}_vol") or quote.get(f"ask{level}_volume")
            asks.append(self._format_orderbook_level("卖", level, price, volume))
        for level in range(1, 6):
            price = quote.get(f"bid{level}")
            volume = quote.get(f"bid{level}_vol") or quote.get(f"bid{level}_volume")
            bids.append(self._format_orderbook_level("买", level, price, volume))
        display_text = "五档: " + " ".join(asks + bids)
        self.orderbook_label.setText(display_text)

    def _set_orderbook_status(self, text: str):
        if hasattr(self, "orderbook_panel") and self.orderbook_panel is not None:
            self.orderbook_panel.set_status(text)
        if hasattr(self, "orderbook_label") and self.orderbook_label is not None:
            self.orderbook_label.setText(text)

    def _start_source_status_timer(self):
        if self._source_status_timer is not None:
            return
        self._source_status_timer = QTimer(self)
        self._source_status_timer.setInterval(5000)
        self._source_status_timer.timeout.connect(self._refresh_source_status)
        self._source_status_timer.start()
        self._refresh_source_status()

    def _refresh_source_status(self):
        """Fix 63: 将 get_source_status() 推到后台线程，避免主线程上调用 provider 对象"""
        if self._source_status_refreshing:
            return
        label = getattr(self, "source_status_label", None)
        if label is None:
            return
        if self.realtime_api is None or not hasattr(self.realtime_api, "get_source_status"):
            label.setText("源: 未连接")
            label.setStyleSheet("color:#999;")
            return
        self._source_status_refreshing = True
        api = self.realtime_api
        threading.Thread(target=self._bg_refresh_source_status, args=(api,), daemon=True).start()

    def _bg_refresh_source_status(self, api):
        """后台线程: 获取数据源状态"""
        try:
            status = api.get_source_status()
        except Exception:
            status = None
        self.source_status_ready.emit(status)

    def _apply_source_status(self, status):
        """主线程: 更新数据源状态 UI"""
        self._source_status_refreshing = False
        label = getattr(self, "source_status_label", None)
        if label is None:
            return
        if status is None:
            label.setText("源: 未知")
            label.setStyleSheet("color:#999;")
            return
        total = len(status)
        available = sum(1 for v in status.values() if v.get("available"))
        if total == 0:
            color = "#999"
            text = "源: 无"
        elif available == total:
            color = "#5cb85c"
            text = f"源: {available}/{total}"
        elif available > 0:
            color = "#f0ad4e"
            text = f"源: {available}/{total}"
        else:
            color = "#d9534f"
            text = f"源: {available}/{total}"
        label.setText(text)
        label.setStyleSheet(f"color:{color};")
        tooltip_lines = []
        for name, info in status.items():
            resp_ms = float(info.get("response_time", 0) or 0) * 1000.0
            tooltip_lines.append(
                f"{name} available={info.get('available')} "
                f"errors={info.get('error_count')} "
                f"rt={resp_ms:.1f}ms"
            )
        label.setToolTip("\n".join(tooltip_lines))

    def _load_orderbook_snapshot_from_db(self, reason: str = "") -> bool:
        """Fix 52: DuckDB 盘口查询推到后台线程，避免阻塞主线程"""
        if self.orderbook_label is None and self.orderbook_panel is None:
            return False
        symbol = self._normalize_symbol(self.symbol_input.text().strip())
        if not symbol:
            return False
        threading.Thread(
            target=self._bg_load_orderbook, args=(symbol, reason), daemon=True
        ).start()
        return True  # 异步发起，返回 True 表示已尝试

    def _bg_load_orderbook(self, symbol: str, reason: str):
        """后台线程: 执行 DuckDB 盘口查询"""
        try:
            if self._orderbook_sink is None:
                self._orderbook_sink = RealtimeDuckDBSink(duckdb_path=self.duckdb_path)
            snapshot = self._orderbook_sink.query_latest_orderbook(symbol)
            if snapshot:
                suffix = f" ({reason})" if reason else ""
                QTimer.singleShot(0, lambda s=snapshot, sf=suffix: self._apply_orderbook_snapshot(s, sf))
        except Exception:
            pass

    def _apply_orderbook_snapshot(self, snapshot: dict, suffix: str):
        """主线程: 应用盘口快照到 UI"""
        self._update_orderbook(snapshot)
        self._set_orderbook_status(f"五档盘口[回放]{suffix}")

    def _format_orderbook_level(self, side: str, level: int, price: Any, volume: Any) -> str:
        price_text = "--" if price in (None, "", 0) else f"{float(price):.2f}"
        volume_text = "--" if volume in (None, "", 0) else f"{int(volume)}"
        return f"{side}{level} {price_text}/{volume_text}"

    def _dynamic_class(self, module_path: str, class_name: str):
        module = importlib.import_module(module_path)
        return getattr(module, class_name)

    def _ensure_interface(self):
        if self.interface is None:
            UnifiedDataInterface = self._dynamic_class(
                "data_manager.unified_data_interface", "UnifiedDataInterface"
            )
            self.interface = UnifiedDataInterface(duckdb_path=self.duckdb_path)
            self._interface_ready = False
        if self._interface_ready or getattr(self.interface, "con", None) is not None:
            self._interface_ready = True
            return
        if self._is_thread_running(self._interface_init_thread):
            return

        class _InterfaceInitThread(QThread):
            ready = pyqtSignal()
            error = pyqtSignal(str)

            def __init__(self, iface):
                super().__init__()
                self._iface = iface

            def run(self):
                try:
                    self._iface.connect(read_only=False)
                    self.ready.emit()
                except Exception as exc:
                    self.error.emit(str(exc))

        thread = _InterfaceInitThread(self.interface)
        thread.setParent(self)
        thread.finished.connect(thread.deleteLater)
        thread.ready.connect(self._on_interface_ready)
        thread.error.connect(self._on_interface_error)
        self._interface_init_thread = thread
        thread.start()

    def _on_interface_ready(self):
        self._interface_ready = True

    def _on_interface_error(self, message: str):
        self._interface_ready = False
        self._set_orderbook_status(f"数据库连接失败: {message}")

    def _get_chart_dates(self, symbol: str, period: str):
        min_span_map = {
            "1m": pd.Timedelta(days=10),
            "5m": pd.Timedelta(days=20),
            "15m": pd.Timedelta(days=60),
            "30m": pd.Timedelta(days=90),
            "60m": pd.Timedelta(days=180),
            "1d": pd.Timedelta(days=365),
            "1w": pd.Timedelta(days=365 * 2),
            "1M": pd.Timedelta(days=365 * 3),
            # 自定义分钟周期：按分钟数比例延伸
            "2m": pd.Timedelta(days=12),
            "10m": pd.Timedelta(days=60),
            "20m": pd.Timedelta(days=90),
            "25m": pd.Timedelta(days=90),
            "50m": pd.Timedelta(days=120),
            "70m": pd.Timedelta(days=150),
            "120m": pd.Timedelta(days=180),
            "125m": pd.Timedelta(days=180),
            # 多日自定义周期
            "2d": pd.Timedelta(days=365 * 2),
            "3d": pd.Timedelta(days=365 * 3),
            "5d": pd.Timedelta(days=365 * 5),
            "10d": pd.Timedelta(days=365 * 8),
            "25d": pd.Timedelta(days=365 * 15),
            "50d": pd.Timedelta(days=365 * 20),
            "75d": pd.Timedelta(days=365 * 30),
            "2M": pd.Timedelta(days=365 * 30),
            "3M": pd.Timedelta(days=365 * 30),
            "5M": pd.Timedelta(days=365 * 30),
        }
        min_span = min_span_map.get(period, pd.Timedelta(days=30))
        if self.interface is not None and hasattr(self.interface, "get_stock_date_range"):
            try:
                rng = self.interface.get_stock_date_range(symbol, period)
                if rng and rng[0] and rng[1]:
                    s = pd.Timestamp(rng[0])
                    e = pd.Timestamp(rng[1])
                    # 保证图表端日期至少延伸到今天，使今日数据能被加载到数据库再展示
                    today = pd.Timestamp.today().normalize()
                    if e < today:
                        e = today
                    if (e - s) < min_span:
                        s = e - min_span
                    return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")
            except Exception:
                pass
        end_date = pd.Timestamp.today().date()
        # 判断是否为日内周期：标准日内周期 + 自定义分钟周期 (e.g. "2m","10m","120m")
        import re as _re
        _is_intraday = period in ("1m", "5m", "15m", "30m", "60m") or bool(
            _re.match(r'^\d+m$', period)
        )
        if _is_intraday:
            # 日内周期：按最大回溯窗口限制，防止拉取全历史导致数据量过大
            start_date = end_date - min_span
        else:
            # 1D/多日周期：优先取上市首日（无 10 年硬截断）
            # 保证全量历史可请求，与 multi-day 左对齐前提一致
            listing_str: str | None = None
            if self.interface is not None and hasattr(self.interface, "get_listing_date"):
                try:
                    listing_str = self.interface.get_listing_date(symbol)
                except Exception:
                    pass
            start_date = (
                pd.Timestamp(listing_str).date()
                if listing_str
                else pd.Timestamp("1990-01-01").date()  # A 股最早日期兜底
            )
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def _min_bars_threshold(self, period: str) -> int:
        return {
            "1m": 120,
            "5m": 120,
            "15m": 100,
            "30m": 80,
            "60m": 60,
            "1d": 40,
            "1w": 30,
            "1M": 24,
        }.get(period, 30)

    # 所有日级精度周期（含多日自定义），图表时间列使用纯日期格式
    _DAILY_DISPLAY_PERIODS = frozenset({
        "1d", "1w", "1M",
        "2d", "3d", "5d", "10d", "25d", "50d", "75d",
        "2M", "3M", "5M",
    })

    def _format_time_column(self, data: pd.DataFrame, period: str) -> pd.DataFrame:
        if "time" not in data.columns:
            return data
        dt_series = pd.to_datetime(data["time"], errors="coerce")
        data = data[dt_series.notna()].copy()
        dt_series = dt_series[dt_series.notna()]
        if period in KLineChartWorkspace._DAILY_DISPLAY_PERIODS:
            data["time"] = dt_series.map(lambda x: x.strftime("%Y-%m-%d"))
        else:
            data["time"] = dt_series.map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        return data

    def _prepare_chart_data(self, data: pd.DataFrame, period: str) -> pd.DataFrame:
        if data is None or data.empty:
            return pd.DataFrame()

        if "datetime" in data.columns:
            data = data.rename(columns={"datetime": "time"})
        elif "date" in data.columns:
            data = data.rename(columns={"date": "time"})
        elif data.index is not None:
            data = data.reset_index().rename(columns={"datetime": "time", "index": "time"})

        for col in ["open", "high", "low", "close"]:
            if col not in data.columns:
                return pd.DataFrame()

        if "volume" not in data.columns:
            data["volume"] = 0

        data = data.loc[:, ["time", "open", "high", "low", "close", "volume"]]
        for col in ["open", "high", "low", "close", "volume"]:
            data[col] = pd.to_numeric(data[col], errors="coerce")
        data = self._format_time_column(data, period)
        data = data[data["time"].notna()]
        data = data.dropna(subset=["open", "high", "low", "close"])
        valid_ohlc = (
            (data["high"] >= data["low"])
            & (data["high"] >= data["open"])
            & (data["high"] >= data["close"])
            & (data["low"] <= data["open"])
            & (data["low"] <= data["close"])
        )
        data = data[valid_ohlc]
        data["volume"] = data["volume"].fillna(0)
        data = data.drop_duplicates(subset=["time"], keep="last")
        data = data.sort_values("time").reset_index(drop=True)
        return data

    def _normalize_symbol(self, symbol: str) -> str:
        if not symbol:
            return ""
        symbol = symbol.upper().strip()
        # Local implementation to avoid blocking import of easy_xt
        import re

        if re.match(r"^\d{6}\.(SH|SZ)$", symbol):
            return symbol
        if re.match(r"^\d{6}$", symbol):
            if symbol.startswith(("60", "68", "11", "12", "13")):
                return f"{symbol}.SH"
            return f"{symbol}.SZ"
        if re.match(r"^(SH|SZ)\d{6}$", symbol):
            return f"{symbol[2:]}.{symbol[:2]}"
        return symbol

    def _reset_progressive_state(self):
        self._segment_cache.clear()
        self._loading_segments.clear()
        self._loaded_range = None
        self._full_range = None

    def _get_segment_span(self, period: str) -> pd.DateOffset:
        import re as _re
        custom_intraday = {"2m", "10m", "20m", "25m", "50m", "70m", "120m", "125m"}
        if period in ("1w", "1M"):
            return pd.DateOffset(years=2)
        if period == "1d":
            return pd.DateOffset(months=3)
        if period in ("1m", "5m"):
            return pd.DateOffset(days=5)
        if period in ("15m", "30m", "60m"):
            return pd.DateOffset(days=15)
        # 自定义分钟周期 (2m, 10m, 70m, 120m 等)
        _m = _re.match(r'^(\d+)m$', period)
        if _m and period in custom_intraday:
            mins = int(_m.group(1))
            if mins <= 5:
                return pd.DateOffset(days=5)
            if mins <= 60:
                return pd.DateOffset(days=15)
            return pd.DateOffset(days=30)
        # 多日自定义周期 — 按每周期交易日数估算合理分段
        _multiday_td = {
            "2d": pd.DateOffset(months=6),
            "3d": pd.DateOffset(months=6),
            "5d": pd.DateOffset(years=1),
            "10d": pd.DateOffset(years=2),
            "25d": pd.DateOffset(years=5),
            "50d": pd.DateOffset(years=10),
            "75d": pd.DateOffset(years=15),
            "2M": pd.DateOffset(years=15),
            "3M": pd.DateOffset(years=20),
            "5M": pd.DateOffset(years=30),
        }
        if period in _multiday_td:
            return _multiday_td[period]
        return pd.DateOffset(days=30)

    def _get_time_step(self, period: str) -> pd.Timedelta:
        import re as _re
        custom_intraday = {"2m", "10m", "20m", "25m", "50m", "70m", "120m", "125m"}
        if period == "1w":
            return pd.Timedelta(weeks=1)
        if period == "1M":
            return pd.Timedelta(days=30)
        if period == "1d":
            return pd.Timedelta(days=1)
        if period == "1m":
            return pd.Timedelta(minutes=1)
        if period == "5m":
            return pd.Timedelta(minutes=5)
        if period == "15m":
            return pd.Timedelta(minutes=15)
        if period == "30m":
            return pd.Timedelta(minutes=30)
        if period == "60m":
            return pd.Timedelta(minutes=60)
        # 自定义分钟周期 (2m, 10m, 70m, 125m 等)
        _m = _re.match(r'^(\d+)m$', period)
        if _m and period in custom_intraday:
            return pd.Timedelta(minutes=int(_m.group(1)))
        # 多日自定义周期
        _multiday_step = {
            "2d": pd.Timedelta(days=2),
            "3d": pd.Timedelta(days=3),
            "5d": pd.Timedelta(days=5),
            "10d": pd.Timedelta(days=10),
            "25d": pd.Timedelta(days=25),
            "50d": pd.Timedelta(days=50),
            "75d": pd.Timedelta(days=75),
            "2M": pd.Timedelta(days=42),
            "3M": pd.Timedelta(days=63),
            "5M": pd.Timedelta(days=105),
        }
        if period in _multiday_step:
            return _multiday_step[period]
        return pd.Timedelta(seconds=1)

    def _format_time_str(self, ts: pd.Timestamp, period: str) -> str:
        if period in KLineChartWorkspace._DAILY_DISPLAY_PERIODS:
            return ts.strftime("%Y-%m-%d")
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    def _compute_initial_range(self, full_range: tuple[str, str], period: str) -> tuple[str, str]:
        import re as _re
        full_start, full_end = full_range
        end_ts = pd.Timestamp(full_end)
        if period in ("1m", "5m"):
            span = pd.DateOffset(days=2)
        elif period in ("15m", "30m", "60m"):
            span = pd.DateOffset(days=5)
        elif bool(_re.match(r'^\d+m$', period)):
            # 其他自定义分钟周期 (2m,10m,20m,25m,50m,70m,120m,125m等)：按分钟数选合适窗口
            try:
                mins = int(_re.match(r'^(\d+)m$', period).group(1))  # type: ignore[union-attr]
            except Exception:
                mins = 30
            if mins <= 5:
                span = pd.DateOffset(days=2)
            elif mins <= 30:
                span = pd.DateOffset(days=5)
            else:
                span = pd.DateOffset(days=10)
        elif period == "1d":
            span = pd.DateOffset(months=3)
        elif period in ("1w", "1M"):
            span = pd.DateOffset(years=2)
        elif period in ("2d", "3d"):
            span = pd.DateOffset(months=6)
        elif period in ("5d", "10d"):
            span = pd.DateOffset(years=1)
        elif period in ("25d", "50d", "75d", "2M", "3M", "5M"):
            span = pd.DateOffset(years=5)
        else:
            span = pd.DateOffset(days=30)
        start_ts = end_ts - span
        if pd.Timestamp(full_start) > start_ts:
            start_ts = pd.Timestamp(full_start)
        return self._format_time_str(start_ts, period), self._format_time_str(end_ts, period)

    def _merge_chart_data(self, base: pd.DataFrame, extra: pd.DataFrame) -> pd.DataFrame:
        if base is None or base.empty:
            return extra.copy()
        if extra is None or extra.empty:
            return base.copy()
        merged = pd.concat([base, extra], ignore_index=True)
        merged = merged.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
        return merged

    def _set_loaded_range_from_data(self, data: pd.DataFrame):
        if data is None or data.empty:
            return
        start_time = str(data["time"].iloc[0])
        end_time = str(data["time"].iloc[-1])
        self._loaded_range = (start_time, end_time)

    def _request_segment(
        self,
        symbol: str,
        period: str,
        adjust: str,
        start_date: str,
        end_date: str,
        mode: str,
    ):
        if not symbol or self.chart is None:
            return
        try:
            if pd.Timestamp(end_date) < pd.Timestamp(start_date):
                return
        except Exception:
            return
        key = (symbol, period, adjust, start_date, end_date)
        if key in self._segment_cache:
            cached = self._segment_cache.get(key)
            self._on_chart_data_ready(
                {
                    "data": cached,
                    "symbol": symbol,
                    "period": period,
                    "mode": mode,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )
            return
        if key in self._loading_segments:
            return
        self._loading_segments.add(key)
        loader = _ChartDataLoadThread(
            duckdb_path=self.duckdb_path,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            period=period,
            adjust=adjust,
            max_bars=0,
            mode=mode,
        )
        loader.setParent(self)
        loader.finished.connect(loader.deleteLater)
        loader.data_ready.connect(self._on_chart_data_ready)
        loader.error_occurred.connect(lambda msg, k=key: self._loading_segments.discard(k))
        self._chart_load_thread = loader
        loader.start()

    def _on_range_change(self, chart, bars_before: float, bars_after: float):
        if not self._progressive_enabled:
            return
        if self.chart is None or self.last_data is None or self.last_data.empty:
            return
        if self._loaded_range is None:
            return
        now = time.time()
        if now - self._range_change_last_ts < 0.3:
            return
        self._range_change_last_ts = now
        symbol = self.symbol_input.text().strip()
        period = self.period_combo.currentText()
        adjust = self._get_adjust_key()
        span = self._get_segment_span(period)
        step = self._get_time_step(period)
        start_ts = pd.Timestamp(self._loaded_range[0])
        end_ts = pd.Timestamp(self._loaded_range[1])
        full_start_ts = pd.Timestamp(self._full_range[0]) if self._full_range else None
        full_end_ts = pd.Timestamp(self._full_range[1]) if self._full_range else None
        if bars_before < self._range_change_threshold:
            prev_end = start_ts - step
            prev_start = prev_end - span
            if full_start_ts is not None and prev_start < full_start_ts:
                prev_start = full_start_ts
            if prev_end >= prev_start:
                self._request_segment(
                    symbol,
                    period,
                    adjust,
                    self._format_time_str(prev_start, period),
                    self._format_time_str(prev_end, period),
                    "merge",
                )
        if bars_after < self._range_change_threshold:
            next_start = end_ts + step
            next_end = next_start + span
            if full_end_ts is not None and next_end > full_end_ts:
                next_end = full_end_ts
            if next_end >= next_start:
                self._request_segment(
                    symbol,
                    period,
                    adjust,
                    self._format_time_str(next_start, period),
                    self._format_time_str(next_end, period),
                    "merge",
                )

    def refresh_chart_data(self):
        if self.chart is None:
            return
        symbol = self.symbol_input.text().strip()
        period = self.period_combo.currentText()
        adjust = self._get_adjust_key()
        normalized_symbol = self._normalize_symbol(symbol)
        if normalized_symbol and normalized_symbol != symbol:
            self.symbol_input.setText(normalized_symbol)
            symbol = normalized_symbol
        self._save_persisted_state()
        if self._is_thread_running(self._chart_load_thread):
            try:
                self._chart_load_thread.requestInterruption()
                self._chart_load_thread.quit()
            except Exception:
                pass
        self._reset_progressive_state()
        self._auto_fallback_attempted = False
        if not symbol:
            return
        if not os.path.exists(self.duckdb_path):
            self._set_orderbook_status(f"数据未加载: DuckDB文件不存在 {self.duckdb_path}")
            return
        if os.path.getsize(self.duckdb_path) == 0:
            self._set_orderbook_status(f"数据未加载: DuckDB为空 {self.duckdb_path}")
            return
        self._ensure_interface()
        full_range = self._get_chart_dates(symbol, period)
        self._full_range = full_range
        start_date, end_date = self._compute_initial_range(full_range, period)
        self._set_orderbook_status("数据加载中...")
        self._update_data_status_label()
        self._request_segment(symbol, period, adjust, start_date, end_date, "replace")
        self._start_realtime_polling()

        # 在首屏数据加载完成后，异步请求完整数据范围进行后台补齐
        # 使用 QTimer.singleShot 确保在当前请求之后执行
        QTimer.singleShot(
            2000, lambda: self._request_full_range_data(symbol, period, adjust, initial_loaded=True)
        )

    def _on_chart_data_ready(self, payload: dict):
        try:
            df = payload.get("data") if isinstance(payload, dict) else None
            symbol = payload.get("symbol") if isinstance(payload, dict) else ""

            period = (
                payload.get("period")
                if isinstance(payload, dict)
                else self.period_combo.currentText()
            )
            adjust = (
                payload.get("adjust")
                if isinstance(payload, dict)
                else self._get_adjust_key()
            )
            start_date = payload.get("start_date") if isinstance(payload, dict) else None
            end_date = payload.get("end_date") if isinstance(payload, dict) else None
            period_str = str(period) if period is not None else self.period_combo.currentText()
            adjust_str = str(adjust) if adjust is not None else self._get_adjust_key()
            symbol_str = str(symbol) if symbol is not None else ""
            start_str = str(start_date) if start_date is not None else ""
            end_str = str(end_date) if end_date is not None else ""
            key = None
            if symbol_str and start_str and end_str:
                key = (symbol_str, period_str, adjust_str, start_str, end_str)
            if df is None or df.empty or self.chart is None:
                if key and key in self._loading_segments:
                    self._loading_segments.discard(key)
                if self.chart is not None and symbol_str and period_str in ("1m", "5m", "tick"):
                    fallback_key = (symbol_str, period_str)
                    if fallback_key not in self._period_fallback_attempted:
                        self._period_fallback_attempted.add(fallback_key)
                        idx = (
                            self.period_combo.findText("1d")
                            if self.period_combo is not None
                            else -1
                        )
                        if idx >= 0:
                            self.period_combo.setCurrentIndex(idx)
                            self._set_orderbook_status(f"{symbol_str} 无{period_str}历史，已自动切换到1d")
                            self.refresh_chart_data()
                            return
                if self.chart is not None and symbol_str:
                    self._start_realtime_polling()
                    self._set_orderbook_status(f"{symbol_str} 无历史数据，等待实时行情...")
                    status_text = str(payload.get("ingestion_status") or "")
                    backfill_scheduled = bool(payload.get("backfill_scheduled"))
                    backfill_pending = bool(payload.get("backfill_pending"))
                    same_pending = (
                        self._pending_backfill_retry is not None
                        and self._pending_backfill_retry[:3] == (symbol_str, period_str, adjust_str)
                        and self._backfill_retry_remaining > 0
                    )
                    if backfill_scheduled or backfill_pending or same_pending:
                        self._schedule_backfill_retry(
                            symbol_str, period_str, adjust_str, start_str, end_str
                        )
                        if status_text:
                            self._set_orderbook_status(
                                f"{symbol_str} 历史补数中({status_text})，等待数据落库..."
                            )
                        else:
                            self._set_orderbook_status(f"{symbol_str} 历史补数中，等待数据落库...")
                    return
                empty_reason = payload.get("empty_reason") if isinstance(payload, dict) else ""
                if empty_reason:
                    self._set_orderbook_status(f"数据未加载: {empty_reason}")
                else:
                    self._set_orderbook_status("数据未加载（请先导入历史数据）")
                self._try_auto_fallback_symbol(period_str)
                return

            min_bars = self._min_bars_threshold(period_str)
            current_rows = len(df) if hasattr(df, "__len__") else 0
            if current_rows < min_bars and symbol_str:
                if start_str and end_str:
                    self._schedule_backfill_retry(
                        symbol_str, period_str, adjust_str, start_str, end_str
                    )
                self._set_orderbook_status(
                    f"{symbol_str} 历史数据不足({current_rows}/{min_bars})，自动补数中..."
                )
                QTimer.singleShot(
                    300,
                    lambda s=symbol_str, p=period_str, a=adjust_str: self._request_full_range_data(
                        s, p, a, initial_loaded=False
                    ),
                )
            else:
                # 数据充足时才清除重试状态；数据不足时保留 _schedule_backfill_retry 设置的状态
                self._pending_backfill_retry = None
                self._backfill_retry_remaining = 0
                self._backfill_retry_timer.stop()
            if self._is_thread_running(self._data_process_thread):
                self._data_process_thread.requestInterruption()
                self._data_process_thread.quit()
                # 不再 .wait() 阻塞主线程；requestInterruption 已足够
            self._data_process_thread = _DataProcessThread(df, period_str, payload)
            self._data_process_thread.setParent(self)
            self._data_process_thread.finished.connect(self._data_process_thread.deleteLater)
            self._data_process_thread.processed.connect(self._on_data_processed)
            self._data_process_thread.start()
        except Exception as e:
            self._logger.exception("_on_chart_data_ready exception: %s", e)

    def _schedule_backfill_retry(
        self, symbol: str, period: str, adjust: str, start_date: str, end_date: str
    ):
        if not symbol or not period:
            return
        self._pending_backfill_retry = (symbol, period, adjust, start_date, end_date)
        if self._backfill_retry_remaining <= 0:
            self._backfill_retry_remaining = int(
                os.environ.get("EASYXT_BACKFILL_RETRY_MAX", "6")
            )
        if not self._backfill_retry_timer.isActive():
            self._backfill_retry_timer.start()

    def _on_backfill_retry_timeout(self):
        if self._pending_backfill_retry is None:
            return
        if self._backfill_retry_remaining <= 0:
            self._set_orderbook_status("历史补数超时，请在数据管理中检查下载状态")
            return
        symbol, period, adjust, _start_date, _end_date = self._pending_backfill_retry
        current_symbol = self.symbol_input.text().strip() if self.symbol_input is not None else ""
        current_period = self.period_combo.currentText() if self.period_combo is not None else ""
        current_adjust = self._get_adjust_key()
        if current_symbol != symbol or current_period != period or str(current_adjust) != str(adjust):
            self._pending_backfill_retry = None
            self._backfill_retry_remaining = 0
            return
        self._backfill_retry_remaining -= 1
        self.refresh_chart_data()
        if self._pending_backfill_retry is not None and self._backfill_retry_remaining > 0:
            self._backfill_retry_timer.start()

    def _on_data_processed(self, payload: dict):
        """去抖处理：合并快速连续的 _DataProcessThread.processed 信号"""
        self._pending_data_payload = payload
        if self._data_debounce_timer is None:
            self._data_debounce_timer = QTimer(self)
            self._data_debounce_timer.setSingleShot(True)
            self._data_debounce_timer.setInterval(80)
            self._data_debounce_timer.timeout.connect(self._apply_data_processed)
        self._data_debounce_timer.start()

    def _apply_data_processed(self):
        """实际执行 chart.set + 轻量 UI 更新，重量级计算推到后台线程"""
        payload = self._pending_data_payload
        if payload is None:
            return
        self._pending_data_payload = None
        try:
            chart_data = payload.get("processed_data")
            if chart_data is None or (hasattr(chart_data, "empty") and chart_data.empty):
                self._logger.warning("_apply_data_processed: processed data is empty")
                process_error = str(payload.get("process_error") or "").strip()
                if process_error:
                    self._set_orderbook_status(f"数据处理失败: {process_error}")
                else:
                    self._set_orderbook_status("数据处理失败")
                return

            period = payload.get("period", "1d")
            mode = payload.get("mode", "replace")
            symbol = payload.get("symbol", "")
            key = payload.get("key")
            if key:
                self._segment_cache[key] = chart_data
                self._loading_segments.discard(key)
            if mode == "merge":
                # Fix 60: merge 模式的 concat+drop_dup+sort 推到后台线程
                base_copy = self.last_data.copy() if (self.last_data is not None and not self.last_data.empty) else pd.DataFrame()
                threading.Thread(
                    target=self._bg_merge_and_set,
                    args=(base_copy, chart_data, symbol, period, key),
                    daemon=True,
                ).start()
                return
            self._finalize_chart_set(chart_data, symbol, period, mode)
        except Exception:
            pass

    def _bg_merge_and_set(self, base, extra, symbol, period, key):
        """后台线程: 合并数据后回主线程执行 chart.set (Fix 60/71)"""
        try:
            merged = self._merge_chart_data(base, extra)
            self._merge_chart_done.emit(merged, symbol, period)  # Fix 71
        except Exception:
            pass

    def _on_merge_chart_done(self, merged, symbol: str, period: str):
        """主线程: 接收合并数据后执行 chart.set (Fix 71)"""
        self._finalize_chart_set(merged, symbol, period, "merge")

    def _finalize_chart_set(self, merged, symbol, period, mode):
        """主线程: chart.set + 状态保存 (从 _apply_data_processed 提取出)"""
        try:
            if merged is None or merged.empty or self.chart is None:
                return
            if self._full_range is None or mode == "replace":
                self._full_range = (
                    str(merged["time"].iloc[0]),
                    str(merged["time"].iloc[-1]),
                )
            # ㊸修复：跳过与上次完全相同形状+末行的 chart.set 调用
            _shape = (len(merged), symbol, period, str(merged["time"].iloc[-1]),
                      float(merged["close"].iloc[-1]))
            if _shape == self._last_chart_set_shape and mode != "merge":
                self._logger.debug("_apply_data_processed: skipped duplicate chart.set")
            else:
                self._last_chart_set_shape = _shape
                if self.chart_adapter:
                    self.chart_adapter.set_data(merged)
                else:
                    self.chart.set(merged)
                self._request_subchart_update(merged, full_set=True)
            self.last_data = merged
            self.last_bar_time = merged["time"].iloc[-1]
            self.last_close = float(merged["close"].iloc[-1])
            self.realtime_pipeline.configure(symbol=symbol, period=period, last_data=self.last_data)
            self._set_loaded_range_from_data(merged)
            # 信号评估和盘口快照推到后台线程，避免主线程阻塞
            # Fix 59: 只传最后 30 行用于信号计算，避免全量 copy
            merged_tail = merged.tail(30).copy()
            threading.Thread(
                target=self._bg_evaluate_and_orderbook,
                args=(merged_tail, symbol),
                daemon=True,
            ).start()
            signal_bus.emit(Events.CHART_DATA_LOADED, symbol=symbol, period=period)
            signal_bus.emit(Events.DATA_UPDATED, symbol=symbol, period=period)
        except Exception:
            pass

    def _bg_evaluate_and_orderbook(self, data: pd.DataFrame, symbol: str):
        """后台线程：信号计算 + DuckDB查询；UI 更新通过 QTimer 回主线程"""
        try:
            signal = self._compute_signal(data)
            if signal:
                signal_key = f"{signal['time']}_{signal['name']}"
                if self.last_signal_key != signal_key:
                    QTimer.singleShot(0, lambda s=signal, k=signal_key: self._apply_signal_ui(s, k))
        except Exception:
            pass
        try:
            sym = self._normalize_symbol(symbol)
            if sym:
                if self._orderbook_sink is None:
                    self._orderbook_sink = RealtimeDuckDBSink(duckdb_path=self.duckdb_path)
                snapshot = self._orderbook_sink.query_latest_orderbook(sym)
                if snapshot:
                    QTimer.singleShot(0, lambda s=snapshot: self._apply_orderbook_bg(s, "historical_replay"))
        except Exception:
            pass

    def _apply_signal_ui(self, signal: dict, signal_key: str):
        """主线程：应用信号标记到图表"""
        if self.last_signal_key == signal_key:
            return
        self.last_signal_key = signal_key
        if self.chart:
            if self.chart_adapter:
                self.chart_adapter.marker(signal["label"])
            else:
                self.chart.marker(text=signal["label"])
        if hasattr(self, "auto_trade_check") and self.auto_trade_check and self.auto_trade_check.isChecked():
            self._execute_trade_signal(signal)

    def _apply_orderbook_bg(self, snapshot: dict, reason: str):
        """主线程：应用后台查询到的盘口快照"""
        self._update_orderbook(snapshot)
        suffix = f" ({reason})" if reason else ""
        self._set_orderbook_status(f"五档盘口[回放]{suffix}")

    def _request_full_range_data(
        self, symbol: str, period: str, adjust: str, initial_loaded: bool = False
    ):
        """请求完整的数据范围，用于后台补齐

        Args:
            symbol: 股票代码
            period: 周期
            adjust: 复权类型
            initial_loaded: 是否为首屏数据加载完成后触发
        """
        if not symbol or self.chart is None:
            return

        if self._full_range is None:
            return

        full_start, full_end = self._full_range
        initial_start, initial_end = self._compute_initial_range(self._full_range, period)

        # 按周期限制全量加载范围，避免请求多年 1m 数据导致超长等待
        # 1d / 多日自定义周期（2d/3d/5d…）：无上限，必须加载上市首日以来完整数据
        _INTRADAY_CAPS = {
            "1m":  pd.DateOffset(days=30),
            "5m":  pd.DateOffset(days=60),
            "15m": pd.DateOffset(months=6),
            "30m": pd.DateOffset(months=6),
            "60m": pd.DateOffset(years=2),
        }
        max_span = _INTRADAY_CAPS.get(period)   # None → 无上限（1d / 大周期）
        end_ts = pd.Timestamp(full_end)
        if max_span is not None:
            earliest = end_ts - max_span
            if pd.Timestamp(full_start) < earliest:
                full_start = earliest.strftime("%Y-%m-%d")

        # 检查是否需要后台补齐 - 即完整范围比初始加载范围更大
        if full_start < initial_start or full_end > initial_end:
            # 设置不同的加载策略：如果首屏已加载完成，延迟一点再加载完整数据
            if initial_loaded:
                QTimer.singleShot(
                    1200,
                    lambda: self._request_segment(
                        symbol, period, adjust, full_start, full_end, "merge"
                    ),
                )
            else:
                # 直接加载完整范围
                self._request_segment(symbol, period, adjust, full_start, full_end, "replace")

    def _try_auto_fallback_symbol(self, period: str):
        if self._auto_fallback_attempted:
            return
        if not self.interface or getattr(self.interface, "con", None) is None:
            return

        # Use background thread to find fallback symbol
        if (
            hasattr(self, "_fallback_thread")
            and self._fallback_thread
            and self._is_thread_running(self._fallback_thread)
        ):
            return

        self._fallback_thread = _FallbackSymbolThread(self.duckdb_path, period)
        self._fallback_thread.setParent(self)
        self._fallback_thread.finished.connect(self._fallback_thread.deleteLater)
        self._fallback_thread.found.connect(self._on_fallback_symbol_found)
        self._fallback_thread.start()

    def _on_fallback_symbol_found(self, fallback: str):
        if not fallback:
            return
        current = self.symbol_input.text().strip()
        if current == fallback:
            return
        self._auto_fallback_attempted = True
        self.symbol_input.setText(fallback)
        self.refresh_chart_data()

    def _load_realtime_fallback(self, symbol: str, period: str) -> bool:
        if not symbol or self.chart is None:
            return False
        self._ensure_realtime_api()
        if self.realtime_api is None:
            return False
        quotes = self.realtime_api.get_realtime_quotes([symbol])
        if not quotes:
            return False
        quote = quotes[0]
        bar = self._build_bar_from_quote(quote, period)
        if bar is None:
            return False
        fallback_data = pd.DataFrame([bar])
        if self.chart_adapter:
            self.chart_adapter.set_data(fallback_data)
        else:
            self.chart.set(fallback_data)
        self._request_subchart_update(fallback_data, full_set=True)
        self.last_data = fallback_data
        self.last_bar_time = bar["time"]
        self.last_close = float(bar["close"])
        self._update_orderbook(quote)
        self._start_realtime_polling()
        return True

    def _build_bar_from_quote(self, quote: dict, period: str) -> Optional[dict]:
        price = float(quote.get("price") or 0)
        if price <= 0:
            return None
        now = pd.Timestamp.now()
        if period in ("1d", "1w", "1M"):
            bar_time = now.strftime("%Y-%m-%d")
        elif period == "1m":
            bar_time = now.floor("min").strftime("%Y-%m-%d %H:%M:%S")
        elif period == "5m":
            bar_time = now.floor("5min").strftime("%Y-%m-%d %H:%M:%S")
        elif period == "15m":
            bar_time = now.floor("15min").strftime("%Y-%m-%d %H:%M:%S")
        elif period == "30m":
            bar_time = now.floor("30min").strftime("%Y-%m-%d %H:%M:%S")
        elif period == "60m":
            bar_time = now.floor("60min").strftime("%Y-%m-%d %H:%M:%S")
        else:
            bar_time = now.strftime("%Y-%m-%d %H:%M:%S")
        open_price = float(quote.get("open") or price)
        high = max(price, float(quote.get("high") or price))
        low = min(price, float(quote.get("low") or price))
        volume = float(quote.get("volume") or 0)
        return {
            "time": bar_time,
            "open": open_price,
            "high": high,
            "low": low,
            "close": price,
            "volume": volume,
        }

    def _on_theme_changed(self, index: int):
        if self.theme_combo is None:
            return
        theme = self.theme_combo.itemData(index) or "dark"
        self._apply_chart_theme(theme)
        app = QApplication.instance()
        if app is None:
            return
        self.theme_manager.apply(cast(QApplication, app), theme)

    def _toggle_full_range(self):
        use_full = True
        if self.start_date_edit is not None:
            self.start_date_edit.setEnabled(not use_full)
            self.start_date_edit.setVisible(False)
        if self.end_date_edit is not None:
            self.end_date_edit.setEnabled(not use_full)
            self.end_date_edit.setVisible(False)
        if self.start_date_label is not None:
            self.start_date_label.setVisible(False)
        if self.end_date_label is not None:
            self.end_date_label.setVisible(False)

    def _set_date_range(self, start_date: str, end_date: str):
        if self.start_date_edit is None or self.end_date_edit is None:
            return
        start_qdate = QDate.fromString(start_date, "yyyy-MM-dd")
        end_qdate = QDate.fromString(end_date, "yyyy-MM-dd")
        if start_qdate.isValid():
            self.start_date_edit.setDate(start_qdate)
        if end_qdate.isValid():
            self.end_date_edit.setDate(end_qdate)

    def _on_chart_params_changed(self):
        # 用 200ms 去抖：快速拖拽/连续点击时只发起一次数据请求
        self._chart_refresh_timer.start(200)

    def _load_persisted_state(self):
        settings = QSettings("EasyXT", "KLineChartWorkspace")
        saved_symbol = settings.value("symbol", "", type=str)
        saved_period = settings.value("period", "", type=str)
        saved_adjust = settings.value("adjust", "", type=str)
        if saved_symbol:
            self.symbol_input.setText(saved_symbol)
        if saved_period:
            index = self.period_combo.findText(saved_period)
            if index >= 0:
                self.period_combo.setCurrentIndex(index)
            # 同步按钮组
            btn = self._period_buttons_map.get(saved_period)
            if btn:
                btn.setChecked(True)
        if saved_adjust:
            # 兼容旧的英文 key 或新的中文显示
            display = str(self._adjust_key_to_display.get(saved_adjust, saved_adjust) or "")
            index = self.adjust_combo.findText(display)
            if index >= 0:
                self.adjust_combo.setCurrentIndex(index)

    def _get_adjust_key(self) -> str:
        """从复权 ComboBox 当前文本获取内部 key"""
        display = self.adjust_combo.currentText()
        return self._adjust_display_to_key.get(display, "none")

    def _save_persisted_state(self):
        settings = QSettings("EasyXT", "KLineChartWorkspace")
        settings.setValue("symbol", self.symbol_input.text().strip())
        settings.setValue("period", self.period_combo.currentText())
        settings.setValue("adjust", self._get_adjust_key())

    def refresh_latest_bar(self):
        if self.chart is None:
            return
        if self.auto_update_check is not None and not self.auto_update_check.isChecked():
            return
        if self._is_thread_running(self._chart_load_thread):
            return
        now = time.monotonic()
        if (
            self._last_latest_bar_ts
            and now - self._last_latest_bar_ts < self._latest_bar_cooldown_s
        ):
            return
        if self._is_thread_running(self._latest_bar_thread):
            self._latest_bar_pending = True
            return
        symbol = self.symbol_input.text().strip()
        period = self.period_combo.currentText()
        adjust = self._get_adjust_key()
        if not symbol:
            return

        if self.end_date_edit is not None:
            end_date = self.end_date_edit.date().toPyDate()
        else:
            end_date = pd.Timestamp.today().date()
        if period in ("1d", "1w", "1M"):
            start_date = end_date - pd.Timedelta(days=30)
        elif period in ("15m", "30m", "60m"):
            start_date = end_date - pd.Timedelta(days=5)
        else:
            start_date = end_date - pd.Timedelta(days=2)
        loader = _LatestBarLoadThread(
            duckdb_path=self.duckdb_path,
            symbol=symbol,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            period=period,
            adjust=adjust,
        )
        loader.setParent(self)
        loader.finished.connect(loader.deleteLater)
        loader.data_ready.connect(self._on_latest_bar_ready)
        loader.error_occurred.connect(lambda msg: None)
        self._latest_bar_thread = loader
        self._latest_bar_pending = False
        self._last_latest_bar_ts = now
        loader.start()

    def _on_latest_bar_ready(self, payload: dict):
        try:
            if self.chart is None:
                return
            df = payload.get("data") if isinstance(payload, dict) else None
            if df is None or df.empty:
                return
            period = (
                payload.get("period")
                if isinstance(payload, dict)
                else self.period_combo.currentText()
            )
            symbol = (
                payload.get("symbol")
                if isinstance(payload, dict)
                else self.symbol_input.text().strip()
            )
            # Fix 57: _prepare_chart_data 有 sort/dropna/format 等重操作，推到后台
            # Fix 63: tail(30) 保证信号计算有足够行数 (>=25)
            prev_data = self.last_data.tail(30).copy() if (self.last_data is not None and not self.last_data.empty) else pd.DataFrame()
            threading.Thread(
                target=self._bg_prepare_and_update_latest,
                args=(df, period, symbol, prev_data),
                daemon=True,
            ).start()
        finally:
            if self._latest_bar_pending:
                self._latest_bar_pending = False
                self.refresh_latest_bar()

    def _bg_prepare_and_update_latest(self, df, period, symbol, prev_data):
        """后台线程: _prepare_chart_data + 信号计算, 回主线程只做 chart.update + 状态合并"""
        try:
            chart_data = self._prepare_chart_data(df, period)
            if chart_data.empty:
                return
            last_row_dict = chart_data.iloc[-1].to_dict()
            # Fix 63: 信号计算仅使用 prev_data (线程安全的副本), 不从后台访问 self.last_data
            if prev_data is not None and not prev_data.empty:
                signal_data = pd.concat([prev_data, chart_data], ignore_index=True)
                signal_data = signal_data.drop_duplicates(subset=["time"], keep="last")
            else:
                signal_data = chart_data
            signal = self._compute_signal(signal_data)
            QTimer.singleShot(0, lambda r=last_row_dict, s=signal, sym=symbol, per=period: self._apply_latest_bar_from_bg(r, s, sym, per))
        except Exception:
            pass

    def _apply_latest_bar_from_bg(self, last_row_dict, signal, symbol, period):
        """主线程: chart.update(单行) + 就地合并 last_data + 子图更新"""
        if self.chart is None:
            return
        if self.last_data is not None and not self.last_data.empty:
            try:
                prev_ts = pd.to_datetime(self.last_data.iloc[-1].get("time"), errors="coerce")
                curr_ts = pd.to_datetime(last_row_dict.get("time"), errors="coerce")
                if pd.notna(prev_ts) and pd.notna(curr_ts) and curr_ts < prev_ts:
                    return
            except Exception:
                pass
        last_row = pd.Series(last_row_dict)
        if self.chart_adapter:
            self.chart_adapter.update_data(last_row)
        else:
            self.chart.update(last_row)
        current_close = float(last_row.get("close", 0))
        self.last_close = current_close
        self.last_bar_time = last_row.get("time")
        # Fix 63: 主线程就地合并单行到 last_data, 避免后台线程访问 self.last_data
        time_val = last_row_dict.get("time")
        if self.last_data is not None and not self.last_data.empty:
            mask = self.last_data["time"] == time_val
            if mask.any():
                idx = self.last_data.index[mask][-1]
                for k, v in last_row_dict.items():
                    self.last_data.at[idx, k] = v
            else:
                self.last_data = pd.concat(
                    [self.last_data, pd.DataFrame([last_row_dict])], ignore_index=True
                )
        else:
            self.last_data = pd.DataFrame([last_row_dict])
        self._request_subchart_update(self.last_data)
        if signal:
            signal_key = f"{signal['time']}_{signal['name']}"
            if self.last_signal_key != signal_key:
                self.last_signal_key = signal_key
                if self.chart:
                    if self.chart_adapter:
                        self.chart_adapter.marker(signal["label"])
                    else:
                        self.chart.marker(text=signal["label"])
                if hasattr(self, "auto_trade_check") and self.auto_trade_check.isChecked():
                    self._execute_trade_signal(signal)
        signal_bus.emit(Events.DATA_UPDATED, symbol=symbol, period=period)

    def _merge_latest_data(self, latest_data: pd.DataFrame) -> pd.DataFrame:
        if self.last_data is None or self.last_data.empty:
            return latest_data
        combined = pd.concat([self.last_data, latest_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=["time"], keep="last")
        combined = combined.sort_values("time").reset_index(drop=True)
        return combined

    def _evaluate_signals(self, data: pd.DataFrame):
        signal = self._compute_signal(data)
        if not signal:
            return
        signal_key = f"{signal['time']}_{signal['name']}"
        if self.last_signal_key == signal_key:
            return
        self.last_signal_key = signal_key
        if self.chart_adapter:
            self.chart_adapter.marker(signal["label"])
        elif self.chart is not None:
            self.chart.marker(text=signal["label"])
        if self.auto_trade_check.isChecked():
            self._execute_trade_signal(signal)

    def _compute_signal(self, data: pd.DataFrame) -> Optional[dict[str, Any]]:
        if data is None or data.empty:
            return None
        if len(data) < 25:
            return None

        close = pd.to_numeric(data["close"], errors="coerce").to_numpy(dtype=float)
        high = pd.to_numeric(data["high"], errors="coerce").to_numpy(dtype=float)
        low = pd.to_numeric(data["low"], errors="coerce").to_numpy(dtype=float)
        ma_fast = pd.Series(close).rolling(5).mean()
        ma_slow = pd.Series(close).rolling(20).mean()

        prev_fast = ma_fast.iloc[-2]
        prev_slow = ma_slow.iloc[-2]
        curr_fast = ma_fast.iloc[-1]
        curr_slow = ma_slow.iloc[-1]
        curr_time = data["time"].iloc[-1]
        curr_close = float(close[-1])

        if prev_fast <= prev_slow and curr_fast > curr_slow:
            return {
                "name": "ma_cross_up",
                "label": "MA Up",
                "side": "buy",
                "time": curr_time,
                "price": curr_close,
            }
        if prev_fast >= prev_slow and curr_fast < curr_slow:
            return {
                "name": "ma_cross_down",
                "label": "MA Down",
                "side": "sell",
                "time": curr_time,
                "price": curr_close,
            }

        window_high = float(np.max(high[-21:-1]))
        window_low = float(np.min(low[-21:-1]))
        if curr_close > window_high:
            return {
                "name": "breakout_up",
                "label": "Breakout Up",
                "side": "buy",
                "time": curr_time,
                "price": curr_close,
            }
        if curr_close < window_low:
            return {
                "name": "breakout_down",
                "label": "Breakout Down",
                "side": "sell",
                "time": curr_time,
                "price": curr_close,
            }
        return None

    def _execute_trade_signal(self, signal: dict[str, Any]):
        if self.trading_window is None:
            return
        trading_window = cast(Any, self.trading_window)
        if not getattr(trading_window, "is_connected", False):
            return
        volume = int(trading_window.volume_spin.value())
        price = float(signal["price"])
        symbol = self.symbol_input.text().strip()
        if signal["side"] == "buy":
            trading_window.place_order_signal(symbol, "buy", price, volume)
        elif signal["side"] == "sell":
            trading_window.place_order_signal(symbol, "sell", price, volume)

    def _apply_chart_theme(self, theme: str):
        if self.chart is None:
            return
        try:
            if theme == "dark":
                self.chart.win.style(
                    background_color="#0f172a",
                    hover_background_color="#1e293b",
                    click_background_color="#334155",
                    active_background_color="rgba(59, 130, 246, 0.7)",
                    muted_background_color="rgba(59, 130, 246, 0.3)",
                    border_color="#334155",
                    color="#e2e8f0",
                    active_color="#ffffff",
                )
            else:
                self.chart.win.style(
                    background_color="#f8fafc",
                    hover_background_color="#e2e8f0",
                    click_background_color="#cbd5f5",
                    active_background_color="rgba(59, 130, 246, 0.7)",
                    muted_background_color="rgba(59, 130, 246, 0.3)",
                    border_color="#cbd5e1",
                    color="#0f172a",
                    active_color="#0f172a",
                )
        except Exception:
            self._logger.exception("Failed to apply chart theme")

    def _open_rt_settings(self):
        """打开实时管道监控设置对话框"""
        try:
            dialog = RealtimeSettingsDialog(parent=self, pipeline_manager=self.realtime_pipeline)
            result = dialog.exec_()
            if result == QDialog.Accepted and self.realtime_pipeline:
                new_settings = dialog.get_settings()
                cfg = self.realtime_pipeline.get_config()
                self._original_flush_interval = int(cfg.get("flush_interval_ms") or 200)
                self._degraded_flush_interval = self._original_flush_interval * 2
                if self._use_ws_quote:
                    self._restart_ws_quote_worker()
                providers = getattr(self.realtime_api, "providers", None)
                if isinstance(providers, dict):
                    tdx_provider = providers.get("tdx")
                    if tdx_provider is not None and hasattr(tdx_provider, "error_log_cooldown"):
                        try:
                            tdx_provider.error_log_cooldown = float(
                                new_settings.get("tdx_error_log_cooldown", 15.0)
                            )
                        except Exception:
                            pass
        except Exception as e:
            self._logger.exception(f"打开实时设置对话框失败: {e}")

    def hideEvent(self, event):
        super().hideEvent(event)
        if self.realtime_timer and self.realtime_timer.isActive():
            self.realtime_timer.stop()

    def closeEvent(self, event):
        try:
            self._backfill_retry_timer.stop()
            self._stop_ws_quote_worker()  # WS worker 需先 stop() 再 wait
            if self.update_timer and self.update_timer.isActive():
                self.update_timer.stop()
            if self.realtime_timer and self.realtime_timer.isActive():
                self.realtime_timer.stop()
            if self.realtime_pipeline_timer and self.realtime_pipeline_timer.isActive():
                self.realtime_pipeline_timer.stop()
            if self._subchart_update_timer and self._subchart_update_timer.isActive():
                self._subchart_update_timer.stop()
            # Fix 51: 先全部 requestInterruption+quit，再统一短 wait，避免串行累积 7s+
            # 对 _realtime_connect_thread 单独处理：它可能阻塞在 TDX 网络上，
            # 先 requestInterruption+quit，加大等待窗口并记录日志，作为第二道保险。
            rct = self._realtime_connect_thread
            if self._is_thread_running(rct):
                rct.requestInterruption()
                rct.quit()
                if not rct.wait(1000):  # 给实盘连接线程 1s 宽限
                    self._logger.warning(
                        "closeEvent: _RealtimeConnectThread 未在 1s 内退出，强制终止"
                    )
                    rct.terminate()
                    rct.wait(500)
                    # 结构化事件上报：便于后续统计"强杀频率"
                    try:
                        signal_bus.emit(
                            Events.THREAD_FORCED_TERMINATE,
                            thread_name="_RealtimeConnectThread",
                            component="kline_chart_workspace",
                        )
                    except Exception:
                        pass
            threads = [
                self._chart_load_thread,
                self._latest_bar_thread,
                self._quote_worker,
                self._data_process_thread,
                getattr(self, "_sync_thread", None),
                getattr(self, "_coverage_thread", None),
                getattr(self, "_interface_init_thread", None),
                getattr(self, "_fallback_thread", None),
            ]
            for t in threads:
                if self._is_thread_running(t):
                    t.requestInterruption()
                    t.quit()
            for t in threads:
                if self._is_thread_running(t):
                    t.wait(1000)
            for t in threads:
                if self._is_thread_running(t):
                    self._logger.warning("closeEvent: 线程未及时退出，强制终止: %s", type(t).__name__)
                    t.terminate()
                    t.wait(300)
        finally:
            # native 路径：关闭时保存当前标的的画线
            if self.toolbox_panel is not None:
                try:
                    self.toolbox_panel.save_current()
                except Exception:
                    pass
            super().closeEvent(event)


class _ChartDataLoadThread(QThread):
    data_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(
        self,
        duckdb_path: str,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
        max_bars: int,
        mode: str,
    ):
        super().__init__()
        self.duckdb_path = duckdb_path
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.period = period
        self.adjust = adjust
        self.max_bars = max_bars
        self.mode = mode

    def run(self):
        iface = None
        try:
            UnifiedDataInterface = importlib.import_module(
                "data_manager.unified_data_interface"
            ).UnifiedDataInterface
            iface = UnifiedDataInterface(duckdb_path=self.duckdb_path, silent_init=True)
            try:
                iface.connect(read_only=False)
            except Exception:
                pass
            data = None
            empty_reason = ""
            backfill_scheduled = False
            backfill_pending = False
            ingestion_status = ""
            fetch_timeout_s = float(os.environ.get("EASYXT_CHART_FETCH_TIMEOUT_S", "12"))
            try:
                from concurrent.futures import ThreadPoolExecutor, TimeoutError

                with ThreadPoolExecutor(max_workers=1) as pool:
                    fut = pool.submit(
                        iface.get_stock_data,
                        stock_code=self.symbol,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        period=self.period,
                        adjust=self.adjust,
                        auto_save=True,
                    )
                    data = fut.result(timeout=fetch_timeout_s)
            except TimeoutError:
                data = None
                empty_reason = f"在线数据请求超时({fetch_timeout_s:.0f}s)"
                try:
                    gap_length = max(
                        (pd.to_datetime(self.end_date) - pd.to_datetime(self.start_date)).days,
                        1,
                    )
                except Exception:
                    gap_length = None
                try:
                    backfill_scheduled = bool(
                        iface.schedule_backfill(
                            stock_code=self.symbol,
                            start_date=self.start_date,
                            end_date=self.end_date,
                            period=self.period,
                            priority=None,
                            reason="chart_fetch_timeout",
                            current_symbol=self.symbol,
                            gap_length=gap_length,
                        )
                    )
                except Exception:
                    pass
            except Exception:
                try:
                    data = iface.get_stock_data_local(
                        stock_code=self.symbol,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        period=self.period,
                        adjust=self.adjust,
                    )
                except Exception:
                    data = None
            if self.isInterruptionRequested():
                return
            if data is not None and not getattr(data, "empty", True):
                if self.max_bars and len(data) > self.max_bars:
                    data = data.tail(self.max_bars).copy()
            if data is None or getattr(data, "empty", True):
                data = self._load_parquet_local()
            if data is None or getattr(data, "empty", True):
                # 完整级联 (DuckDB→QMT→AKShare) 均无数据，记录原因
                con = getattr(iface, "con", None)
                if con is None:
                    empty_reason = "DuckDB连接不可用"
                else:
                    table_period = {"15m": "1m", "30m": "1m", "60m": "1m", "1w": "1d", "1M": "1d"}.get(
                        self.period, self.period
                    )
                    stored_period = self.period
                    table_name, date_col = PERIOD_DATE_COL_MAP.get(table_period, ("stock_daily", "date"))
                    try:
                        table_exists = (
                            con.execute(
                                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
                            ).fetchone()[0]
                            > 0
                        )
                    except Exception:
                        table_exists = False
                    if not table_exists:
                        empty_reason = f"表不存在: {table_name}"
                    else:
                        try:
                            total = con.execute(
                                f"SELECT COUNT(*) FROM {table_name} WHERE stock_code = '{self.symbol}' AND period = '{stored_period}'"
                            ).fetchone()[0]
                        except Exception:
                            total = 0
                        if total <= 0:
                            empty_reason = f"{self.symbol} 无历史数据"
                        else:
                            try:
                                rng = con.execute(
                                    f"SELECT MIN({date_col}) AS s, MAX({date_col}) AS e FROM {table_name} WHERE stock_code = '{self.symbol}' AND period = '{stored_period}'"
                                ).fetchone()
                            except Exception:
                                rng = None
                            if rng and rng[0] and rng[1]:
                                empty_reason = f"{self.symbol} 可用区间: {rng[0]} ~ {rng[1]}"
                            else:
                                empty_reason = f"{self.symbol} 数据区间未知"
                if not empty_reason:
                    empty_reason = f"{self.symbol} 本地parquet无数据"
                try:
                    latest_status = iface.get_ingestion_status(
                        stock_code=self.symbol, period=self.period
                    )
                    if latest_status is not None and not latest_status.empty:
                        status_val = str(latest_status.iloc[0].get("status") or "")
                        ingestion_status = status_val
                        backfill_pending = status_val in ("queued", "running")
                except Exception:
                    pass
            payload = {
                "data": data if data is not None else pd.DataFrame(),
                "symbol": self.symbol,
                "period": self.period,
                "adjust": self.adjust,
                "mode": self.mode,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "empty_reason": empty_reason,
                "backfill_scheduled": backfill_scheduled,
                "backfill_pending": backfill_pending,
                "ingestion_status": ingestion_status,
            }
            self.data_ready.emit(payload)
        except Exception as e:
            self.error_occurred.emit(str(e))
        finally:
            try:
                if iface is not None:
                    iface.close()
            except Exception:
                pass

    def _load_parquet_local(self) -> Optional[pd.DataFrame]:
        base_dir = os.environ.get("EASYXT_RAW_PARQUET_ROOT", "D:/StockData/raw")
        if self.period != "1d":
            return None
        file_path = Path(base_dir) / "daily" / f"{self.symbol}.parquet"
        if not file_path.exists():
            return None
        try:
            df = pd.read_parquet(file_path)
        except Exception:
            return None
        if df is None or df.empty:
            return None
        if "date" in df.columns and "time" not in df.columns:
            df["time"] = df["date"]
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"], errors="coerce")
            df = df[df["time"].notna()]
            df = df.sort_values("time")
            if self.start_date:
                df = df[df["time"] >= pd.to_datetime(self.start_date, errors="coerce")]
            if self.end_date:
                df = df[df["time"] <= pd.to_datetime(self.end_date, errors="coerce")]
            df = df.set_index("time")
        return df


class _LatestBarLoadThread(QThread):
    data_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(
        self,
        duckdb_path: str,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
    ):
        super().__init__()
        self.duckdb_path = duckdb_path
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.period = period
        self.adjust = adjust
        self.mode = "latest"

    def run(self):
        iface = None
        try:
            UnifiedDataInterface = importlib.import_module(
                "data_manager.unified_data_interface"
            ).UnifiedDataInterface
            iface = UnifiedDataInterface(duckdb_path=self.duckdb_path, silent_init=True)
            try:
                iface.connect(read_only=False)
            except Exception:
                pass
            data = None
            try:
                data = iface.get_stock_data_local(
                    stock_code=self.symbol,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    period=self.period,
                    adjust=self.adjust,
                )
            except Exception:
                data = None
            if self.isInterruptionRequested():
                return
            payload = {
                "data": data if data is not None else pd.DataFrame(),
                "symbol": self.symbol,
                "period": self.period,
                "adjust": self.adjust,
                "mode": self.mode,
                "start_date": self.start_date,
                "end_date": self.end_date,
            }
            self.data_ready.emit(payload)
        except Exception as e:
            self.error_occurred.emit(str(e))
        finally:
            try:
                if iface is not None:
                    iface.close()
            except Exception:
                pass


class _RealtimeConnectThread(QThread):
    ready = pyqtSignal(object)
    error_occurred = pyqtSignal(str)

    def run(self):
        try:
            # 可中断策略：分段检查 isInterruptionRequested()，
            # 避免 closeEvent 发出 requestInterruption 后仍走完整个连接流程。
            if self.isInterruptionRequested():
                return
            # UnifiedDataAPI 不使用 xtquant C 扩展，无需持有 _xt_init_lock
            UnifiedDataAPI = importlib.import_module(
                "easy_xt.realtime_data.unified_api"
            ).UnifiedDataAPI
            if self.isInterruptionRequested():
                return
            api = UnifiedDataAPI()
            if self.isInterruptionRequested():
                return
            # connect_all 内部使用 ThreadPoolExecutor 并行连接各数据源（纯 socket）
            # 不持有 _xt_init_lock，避免阻塞 QuoteWorker / ConnectionCheckThread
            # connect_all 本身带整体超时保护（overall_timeout = provider.timeout + 2s），
            # 返回后再轮询一次中断标志，丢弃已过期结果。
            try:
                api.connect_all()
            except Exception:
                pass
            if self.isInterruptionRequested():
                return
            self.ready.emit(api)
        except Exception as e:
            self.error_occurred.emit(str(e))


class _DataProcessThread(QThread):
    processed = pyqtSignal(dict)

    def __init__(self, data: pd.DataFrame, period: str, payload: dict):
        super().__init__()
        self._data = data
        self._period = period
        self._payload = payload

    def run(self):
        try:
            if self.isInterruptionRequested():
                return
            result = self._process_data(self._data, self._period)
            if self.isInterruptionRequested():
                return
            payload = dict(self._payload)
            payload["processed_data"] = result
            self.processed.emit(payload)
        except Exception as exc:
            payload = dict(self._payload)
            payload["processed_data"] = pd.DataFrame()
            payload["process_error"] = str(exc)
            self.processed.emit(payload)

    def _process_data(self, data: pd.DataFrame, period: str) -> pd.DataFrame:
        if data is None or data.empty:
            return pd.DataFrame()
        data = data.copy()
        if hasattr(data, "columns"):
            data = data.loc[:, ~data.columns.duplicated()]
        if "datetime" in data.columns:
            # 先去除 QMT 原始 "time" 列（原始毫秒戳），避免 rename 后出现两个 "time" 列
            if "time" in data.columns:
                data = data.drop(columns=["time"])
            data = data.rename(columns={"datetime": "time"})
        elif "date" in data.columns:
            if "time" in data.columns:
                data = data.drop(columns=["time"])
            data = data.rename(columns={"date": "time"})
        elif data.index is not None:
            data = data.reset_index()
            # 逐步映射，避免两个源都映射到 "time" 产生重复列
            if "datetime" in data.columns:
                if "time" in data.columns:
                    data = data.drop(columns=["time"])
                data = data.rename(columns={"datetime": "time"})
            elif "index" in data.columns:
                if "time" in data.columns:
                    data = data.drop(columns=["time"])
                data = data.rename(columns={"index": "time"})
            elif "date" in data.columns:
                if "time" in data.columns:
                    data = data.drop(columns=["time"])
                data = data.rename(columns={"date": "time"})
        for col in ["open", "high", "low", "close"]:
            if col not in data.columns:
                return pd.DataFrame()
        if "volume" not in data.columns:
            data["volume"] = 0
        data = data.loc[:, ["time", "open", "high", "low", "close", "volume"]]
        for col in ["open", "high", "low", "close", "volume"]:
            data[col] = pd.to_numeric(data[col], errors="coerce")
        _daily_display = frozenset({
            "1d", "1w", "1M",
            "2d", "3d", "5d", "10d", "25d", "50d", "75d",
            "2M", "3M", "5M",
        })
        if period in _daily_display:
            dt_series = pd.to_datetime(data["time"], errors="coerce")
            data = data[dt_series.notna()].copy()
            dt_series = dt_series[dt_series.notna()]
            data["time"] = dt_series.map(lambda x: x.strftime("%Y-%m-%d"))
        else:
            dt_series = pd.to_datetime(data["time"], errors="coerce")
            data = data[dt_series.notna()].copy()
            dt_series = dt_series[dt_series.notna()]
            data["time"] = dt_series.map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        data = data[data["time"].notna()]
        data = data.dropna(subset=["open", "high", "low", "close"])
        data["volume"] = data["volume"].fillna(0)
        data = data.sort_values("time").reset_index(drop=True)
        return data


class _FallbackSymbolThread(QThread):
    found = pyqtSignal(str)

    def __init__(self, duckdb_path: str, period: str):
        super().__init__()
        self._duckdb_path = duckdb_path
        self._period = period

    def run(self):
        try:
            if self.isInterruptionRequested():
                return

            table_name = PERIOD_TABLE_MAP.get(self._period, "stock_daily")
            sql = f"""
                SELECT stock_code, COUNT(*) AS cnt
                FROM {table_name}
                GROUP BY stock_code
                ORDER BY cnt DESC
                LIMIT 1
            """
            from data_manager.duckdb_connection_pool import get_db_manager
            with get_db_manager(self._duckdb_path).get_read_connection() as con:
                df = con.execute(sql).df()
                if df is not None and not df.empty:
                    symbol = str(df["stock_code"].iloc[0])
                    self.found.emit(symbol)
        except Exception:
            pass
