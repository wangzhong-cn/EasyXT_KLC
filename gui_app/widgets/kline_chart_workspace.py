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
    QPoint,
    QSettings,
    QSize,
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
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from core.events import Events
from core.signal_bus import signal_bus
from core.theme_manager import ThemeManager
from core.thread_manager import thread_manager
from data_manager.duckdb_connection_pool import resolve_duckdb_path
from data_manager.realtime_pipeline_manager import RealtimePipelineManager
from easy_xt.realtime_data.persistence.duckdb_sink import RealtimeDuckDBSink
from gui_app.widgets.chart import (
    PERIOD_DATE_COL_MAP,
    PERIOD_TABLE_MAP,
    ChartEvents,
    KLineChartAdapter,
    NativeLwcChartAdapter,
    PositionTable,
    SubchartManager,
    ToolboxPanel,
    create_chart_adapter,
)
from gui_app.widgets.chart.pipeline_guard import validate_pipeline_bar_for_period
from gui_app.widgets.chart.trading_hours_guard import TradingHoursGuard
from gui_app.widgets.orderbook_panel import OrderbookPanel
from gui_app.widgets.realtime_settings_dialog import RealtimeSettingsDialog
from gui_app.widgets.watchlist import WatchlistWidget


class _KlcStatsPanel(QFrame):
    """KLineChart 右侧「关键数据」面板（Qt 原生，支持左右拉伸）。"""

    _FIELDS = [
        ("open", "开盘"),
        ("high", "最高"),
        ("low", "最低"),
        ("close", "收盘"),
        ("chg_pct", "涨幅%"),
        ("volume", "成交量"),
        ("amount", "成交额"),
        ("turnover", "换手率"),
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("QFrame { background: #111318; border-top: 1px solid #2B2F36; }")
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        hdr = QLabel("关键数据")
        hdr.setStyleSheet("background:#1a1c24;color:#888;font-size:10px;padding:2px 6px;")
        outer.addWidget(hdr)

        grid_w = QWidget()
        grid_w.setStyleSheet("background:#111318;")
        grid = QGridLayout(grid_w)
        grid.setContentsMargins(2, 2, 2, 2)
        grid.setSpacing(1)

        self._labels: dict[str, QLabel] = {}
        for i, (key, lbl_text) in enumerate(self._FIELDS):
            row, col = divmod(i, 2)
            cell = QWidget()
            cell.setStyleSheet("background:#111318;")
            cell_lay = QVBoxLayout(cell)
            cell_lay.setContentsMargins(4, 2, 4, 2)
            cell_lay.setSpacing(0)
            k_lbl = QLabel(lbl_text)
            k_lbl.setStyleSheet("color:#555;font-size:10px;")
            v_lbl = QLabel("--")
            v_lbl.setObjectName(f"st_{key}")
            v_lbl.setStyleSheet("color:#d8d9db;font-size:11px;")
            cell_lay.addWidget(k_lbl)
            cell_lay.addWidget(v_lbl)
            grid.addWidget(cell, row, col)
            self._labels[key] = v_lbl

        outer.addWidget(grid_w)
        outer.addStretch()

    def update_stats(self, stats: dict) -> None:
        up = "#26a69a"
        dn = "#ef5350"
        base = "#d8d9db"

        def _fp(v):
            f = float(v)
            return f"{f:.2f}" if f >= 100 else f"{f:.3f}"

        def _fvol(v):
            n = float(v)
            return f"{n / 10000:.1f}万" if n >= 10000 else str(int(n))

        def _famt(v):
            n = float(v)
            if n >= 1e8:
                return f"{n / 1e8:.2f}亿"
            if n >= 1e4:
                return f"{n / 1e4:.0f}万"
            return str(int(n))

        fmts = {
            "open": _fp,
            "high": _fp,
            "low": _fp,
            "close": _fp,
            "chg_pct": lambda v: f"{float(v):+.2f}%",
            "volume": _fvol,
            "amount": _famt,
            "turnover": lambda v: f"{float(v):.2f}%",
        }
        for key, widget in self._labels.items():
            val = stats.get(key)
            if val is None:
                continue
            try:
                text = fmts[key](val) if key in fmts else str(val)
                widget.setText(text)
                if key == "chg_pct":
                    pct = float(val)
                    color = up if pct > 0 else dn if pct < 0 else base
                    widget.setStyleSheet(f"color:{color};font-size:11px;font-weight:bold;")
            except Exception:
                pass


class _KlcTradesPanel(QFrame):
    """KLineChart 右侧「成交明细」面板（Qt 原生，支持左右拉伸）。"""

    _MAX_ROWS = 60

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("QFrame { background: #111318; border-top: 1px solid #2B2F36; }")
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        hdr = QLabel("成交明细")
        hdr.setStyleSheet("background:#1a1c24;color:#888;font-size:10px;padding:2px 6px;")
        outer.addWidget(hdr)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setStyleSheet("QScrollArea{border:none;background:#111318;}")
        self._container = QWidget()
        self._container.setStyleSheet("background:#111318;")
        self._vbox = QVBoxLayout(self._container)
        self._vbox.setContentsMargins(0, 0, 0, 0)
        self._vbox.setSpacing(0)
        self._vbox.addStretch()
        scroll.setWidget(self._container)
        outer.addWidget(scroll, 1)

        self._rows: list[QWidget] = []

    def add_tick(self, tick: dict) -> None:
        price = tick.get("price")
        ts = str(tick.get("time") or tick.get("tick_time") or "")
        ts = ts[-8:] if len(ts) > 8 else ts
        ask1 = tick.get("ask1")
        bid1 = tick.get("bid1")
        direction = tick.get("direction") or ""
        if not direction and ask1 is not None and bid1 is not None and price is not None:
            try:
                p = float(price)
                direction = "B" if p >= float(ask1) else ("S" if p <= float(bid1) else "")
            except Exception:
                pass
        price_str = f"{float(price):.3f}" if price is not None else "--"
        color = "#26a69a" if direction == "B" else "#ef5350" if direction == "S" else "#d8d9db"

        row_w = QWidget()
        row_w.setFixedHeight(16)
        row_lay = QHBoxLayout(row_w)
        row_lay.setContentsMargins(4, 0, 4, 0)
        row_lay.setSpacing(0)

        t_lbl = QLabel(ts)
        t_lbl.setStyleSheet("color:#555;font-size:10px;")
        t_lbl.setFixedWidth(52)

        p_lbl = QLabel(price_str)
        p_lbl.setStyleSheet(f"color:{color};font-size:10px;")
        p_lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

        row_lay.addWidget(t_lbl)
        row_lay.addWidget(p_lbl, 1)

        # Insert newest tick at top (before the stretch at index len(self._rows))
        self._vbox.insertWidget(0, row_w)
        self._rows.append(row_w)

        while len(self._rows) > self._MAX_ROWS:
            old = self._rows.pop(0)
            self._vbox.removeWidget(old)
            old.deleteLater()


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
        self._xtdata_probe_enabled = os.environ.get("EASYXT_ENABLE_XTDATA_QUOTE_PROBE", "0") in (
            "1",
            "true",
            "True",
        )

    def run(self):
        try:
            if not self.symbol:
                self.error_occurred.emit(self.symbol, "invalid_worker_state")
                return
            # 实盘图表严格只允许 QMT / xtdata 行情。
            fallback_xt = self._fetch_quote_from_xtdata(self.symbol)
            if fallback_xt and float(fallback_xt.get("price") or 0) > 0:
                self.quote_ready.emit(fallback_xt, self.symbol)
                return

            fallback = self._fetch_quote_from_easyxt(self.symbol)
            if fallback and float(fallback.get("price") or 0) > 0:
                self.quote_ready.emit(fallback, self.symbol)
                return

            self.error_occurred.emit(self.symbol, "no_quote_data")
        except Exception as e:
            self.error_occurred.emit(self.symbol, f"quote_worker_error:{type(e).__name__}")

    def _fetch_quote_from_realtime_api(self, timeout_s: float = 1.2):
        # 严格禁用三方/外部行情 fallback，仅保留兼容壳。
        return []

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

            from core.xtdata_lock import xtdata_submit as _xtdata_submit

            df = _xtdata_submit(data_api.get_current_price, [symbol])
            if df is None or getattr(df, "empty", True):
                return None

            row = df.iloc[0]
            quote: dict[str, Any] = {
                "symbol": symbol,
                "source": "qmt_live",
                "price": float(row.get("price", 0) or 0),
                "open": float(row.get("open", 0) or 0),
                "high": float(row.get("high", 0) or 0),
                "low": float(row.get("low", 0) or 0),
                "volume": float(row.get("volume", 0) or 0),
                "amount": float(row.get("amount", 0) or 0),
            }

            try:
                if self._xtdata_probe_enabled:
                    # 通过 broker 单例序列化访问，避免并发 BSON 崩溃
                    broker = easy_xt.get_xtquant_broker()
                    full_tick = broker.get_full_tick([symbol]) or {}
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

            # 按需懒加载 broker 单例（预热已就绪则直接复用，否则首次调用自动初始化）
            broker = easy_xt.get_xtquant_broker()
            full_tick = broker.get_full_tick([symbol]) or {}
            tick = full_tick.get(symbol) or next(iter(full_tick.values()), None)

            if not isinstance(tick, dict):
                return None
            price = float(tick.get("lastPrice") or tick.get("last_price") or tick.get("price") or 0)
            if price <= 0:
                return None
            quote: dict[str, Any] = {
                "symbol": symbol,
                "source": "qmt_live",
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
            # 不回退到无锁的直接 xtdata 调用——并发访问会导致 BSON 断言崩溃
            return None

    def _enrich_quote_with_xt_tick(self, quote: dict[str, Any], symbol: str) -> dict[str, Any]:
        if not self._xtdata_probe_enabled:
            return quote
        if quote.get("ask1") not in (None, 0, "", "--") and quote.get("bid1") not in (
            None,
            0,
            "",
            "--",
        ):
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

    _RECV_TIMEOUT = 1.0  # recv 超时，用于轮询 _should_stop

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
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._client_loop())
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            try:
                loop.close()
            except Exception:
                pass

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
                    exc_tag = type(exc).__name__
                    # websockets.InvalidStatus 携带 HTTP 状态码，追加到 reason 便于诊断
                    http_code = getattr(getattr(exc, "response", None), "status_code", None)
                    if http_code is not None:
                        exc_tag = f"{exc_tag}:{http_code}"
                    self.error_occurred.emit(self.symbol, f"ws_conn_error:{exc_tag}")
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


class _PeriodOverflowStrip(QWidget):
    """响应式周期按钮条。

    按可用宽度从左至右排列按钮；宽度不足时将溢出的按钮折叠到「»」下拉菜单，
    避免拥挤变形。内部使用 setGeometry 手动定位，不依赖 QLayout。
    """

    period_clicked = pyqtSignal(str)  # 用户点击某个周期时发出 (period_key)

    _BTN_SS = """
        QPushButton {
            border: none; background: transparent; color: #aaa;
            padding: 2px 6px; font-size: 12px; border-radius: 3px;
        }
        QPushButton:hover { color: #fff; background: rgba(255,255,255,0.08); }
        QPushButton:checked { color: #4fc3f7; font-weight: bold; background: rgba(79,195,247,0.12); }
    """
    _OVERFLOW_SS = """
        QPushButton {
            border: 1px solid #555; background: transparent; color: #aaa;
            padding: 2px 5px; font-size: 11px; border-radius: 3px;
        }
        QPushButton:hover { color: #fff; border-color: #4fc3f7; }
        QPushButton:checked { color: #4fc3f7; font-weight: bold; border-color: #4fc3f7;
            background: rgba(79,195,247,0.12); }
    """
    _MENU_SS = (
        "QMenu { background:#2a2a3e; color:#ccc; border:1px solid #444; }"
        "QMenu::item { padding:5px 20px; font-size:12px; }"
        "QMenu::item:selected { background:rgba(79,195,247,0.20); color:#fff; }"
        "QMenu::item:checked { color:#4fc3f7; font-weight:bold; }"
    )

    def __init__(self, parent=None):
        super().__init__(parent)
        self._keys: list[str] = []
        self._labels: dict[str, str] = {}
        self._current_key: str = ""
        self._btns: dict[str, QPushButton] = {}
        self._btn_grp = QButtonGroup(self)
        self._btn_grp.setExclusive(True)

        self._overflow_btn = QPushButton("»", self)
        self._overflow_btn.setCheckable(True)
        self._overflow_btn.setStyleSheet(self._OVERFLOW_SS)
        self._overflow_btn.hide()
        self._overflow_btn.clicked.connect(self._show_overflow_menu)

        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.setMinimumWidth(28)
        self.setFixedHeight(24)

    # ── Public API ────────────────────────────────────────────────────────────

    def btn_group(self) -> QButtonGroup:
        return self._btn_grp

    def buttons_map(self) -> dict:
        """返回 key→QPushButton 的 *活引用*，外部通过此字典读写按钮状态。"""
        return self._btns

    def set_periods(self, keys: list, labels: dict):
        """重建按钮列表（keys 顺序即显示顺序）。调用后由 resizeEvent 触发布局。"""
        for btn in list(self._btns.values()):
            self._btn_grp.removeButton(btn)
            btn.deleteLater()
        self._btns.clear()
        self._keys = list(keys)
        self._labels = dict(labels)

        for key in self._keys:
            lbl = labels.get(key, key)
            btn = QPushButton(lbl, self)
            btn.setCheckable(True)
            btn.setStyleSheet(self._BTN_SS)
            btn.clicked.connect(lambda _c=False, k=key: self._on_btn(k))
            self._btn_grp.addButton(btn)
            self._btns[key] = btn
            btn.hide()

        if self._current_key in self._btns:
            self._btns[self._current_key].setChecked(True)
        self._relayout()

    def set_current(self, key: str):
        """设置当前选中周期（仅更新视觉状态，不发信号）。"""
        if self._current_key and self._current_key in self._btns:
            self._btns[self._current_key].setChecked(False)
        self._current_key = key
        if key in self._btns:
            self._btns[key].setChecked(True)
        self._update_overflow_state()

    # ── Qt overrides ──────────────────────────────────────────────────────────

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._relayout()

    def sizeHint(self) -> QSize:
        fm = self.fontMetrics()
        w = sum(fm.horizontalAdvance(self._labels.get(k, k)) + 14 for k in self._keys)
        w += max(0, len(self._keys) - 1) * 2
        return QSize(max(w, 30), 24)

    def minimumSizeHint(self) -> QSize:
        return QSize(28, 24)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _natural_width(self, key: str) -> int:
        return self.fontMetrics().horizontalAdvance(self._labels.get(key, key)) + 14

    def _relayout(self):
        if not self._keys:
            self._overflow_btn.hide()
            return

        avail_w = self.width()
        sp = 2
        fm = self.fontMetrics()
        of_w = fm.horizontalAdvance("»") + 14
        h = self.height() or 24
        nat = {k: self._natural_width(k) for k in self._keys}

        # 1. Check if all buttons fit with no overflow needed
        total = sum(nat.values()) + sp * max(0, len(self._keys) - 1)
        if total <= avail_w:
            visible = list(self._keys)
        else:
            # 2. Greedy fit within (avail_w - of_w - sp)
            budget = avail_w - of_w - sp
            visible: list = []
            used = 0
            for key in self._keys:
                bw = nat[key]
                gap = sp if visible else 0
                if used + gap + bw <= budget:
                    used += gap + bw
                    visible.append(key)
                else:
                    break

        visible_set = set(visible)
        x = 0
        for key in self._keys:
            btn = self._btns[key]
            if key in visible_set:
                btn.setGeometry(x, 0, nat[key], h)
                btn.show()
                x += nat[key] + sp
            else:
                btn.hide()

        overflow_keys = [k for k in self._keys if k not in visible_set]
        if overflow_keys:
            self._overflow_btn.setGeometry(x, 0, of_w, h)
            self._overflow_btn.show()
        else:
            self._overflow_btn.hide()

        self._update_overflow_state()

    def _update_overflow_state(self):
        visible_set = {k for k in self._keys if k in self._btns and self._btns[k].isVisible()}
        in_overflow = (
            self._current_key
            and self._current_key not in visible_set
            and self._overflow_btn.isVisible()
        )
        if in_overflow:
            lbl = self._labels.get(self._current_key, self._current_key)
            self._overflow_btn.setText(f"{lbl}»")
            self._overflow_btn.setChecked(True)
        else:
            self._overflow_btn.setText("»")
            self._overflow_btn.setChecked(False)

    def _on_btn(self, key: str):
        self._current_key = key
        self._overflow_btn.setText("»")
        self._overflow_btn.setChecked(False)
        self.period_clicked.emit(key)

    def _show_overflow_menu(self):
        overflow_keys = [k for k in self._keys if k in self._btns and not self._btns[k].isVisible()]
        if not overflow_keys:
            self._overflow_btn.setChecked(False)
            return
        menu = QMenu(self)
        menu.setStyleSheet(self._MENU_SS)
        for key in overflow_keys:
            action = menu.addAction(self._labels.get(key, key))
            action.setCheckable(True)
            if key == self._current_key:
                action.setChecked(True)
            action.triggered.connect(lambda _c=False, k=key: self._on_overflow_select(k))
        pos = self._overflow_btn.mapToGlobal(self._overflow_btn.rect().bottomLeft())
        menu.exec_(pos)

    def _on_overflow_select(self, key: str):
        for btn in self._btns.values():
            btn.setChecked(False)
        if key in self._btns:
            self._btns[key].setChecked(True)
        self._current_key = key
        self._update_overflow_state()
        self.period_clicked.emit(key)


class _PeriodPickerPopup(QWidget):
    """仿同花顺「更多周期」浮动面板

    结构：
      分组标签（日内 / 日线&多日 / 长周期）
        └─ 每个可用周期一个 QCheckBox
      分隔线
      自定义行：[NNN] [分钟 | 日] [添加]
      底部：[重置默认]  [应用]

    Signals:
      applied(list[str])  — 用户点击"应用"后发出已勾选的 key 列表
    """

    applied = pyqtSignal(list)  # list[str] of period keys

    # ── 全量周期选项池 ────────────────────────────────────────────────────────
    # (显示标签, key, 分组)
    ALL_OPTS: list[tuple[str, str, str]] = [
        ("分时(Tick)", "tick", "日内"),
        ("1 分", "1m", "日内"),
        ("2 分", "2m", "日内"),
        ("5 分", "5m", "日内"),
        ("10 分", "10m", "日内"),
        ("15 分", "15m", "日内"),
        ("20 分", "20m", "日内"),
        ("25 分", "25m", "日内"),
        ("30 分", "30m", "日内"),
        ("50 分", "50m", "日内"),
        ("60 分(1H)", "60m", "日内"),
        ("70 分", "70m", "日内"),
        ("120 分(2H)", "120m", "日内"),
        ("125 分", "125m", "日内"),
        ("日线(1D)", "1d", "日线&多日"),
        ("2 日", "2d", "日线&多日"),
        ("3 日", "3d", "日线&多日"),
        ("5 日", "5d", "日线&多日"),
        ("10 日", "10d", "日线&多日"),
        ("25 日", "25d", "日线&多日"),
        ("50 日", "50d", "日线&多日"),
        ("75 日", "75d", "日线&多日"),
        ("周线(1W)", "1w", "长周期"),
        ("月线(1M)", "1M", "长周期"),
        ("2 月", "2M", "长周期"),
        ("3 月", "3M", "长周期"),
        ("5 月", "5M", "长周期"),
        ("季线(1Q)", "1Q", "长周期"),
        ("半年(6M)", "6M", "长周期"),
        ("年线(1Y)", "1Y", "长周期"),
        ("2 年", "2Y", "长周期"),
        ("3 年", "3Y", "长周期"),
        ("5 年", "5Y", "长周期"),
        ("10 年", "10Y", "长周期"),
    ]
    # 默认常驻周期（用户首次打开时的预选状态）
    DEFAULT_RESIDENT: list[str] = [
        "1m",
        "2m",
        "5m",
        "10m",
        "15m",
        "25m",
        "30m",
        "50m",
        "60m",
        "70m",
        "125m",
        "1d",
        "2d",
        "3d",
        "5d",
        "1w",
    ]

    _POPUP_SS = """
        QWidget#PeriodPicker {
            background: #1e1e2e; border: 1px solid #555; border-radius: 6px;
        }
        QLabel.section {
            color: #888; font-size: 10px; padding: 4px 0 2px 0;
        }
        QCheckBox {
            color: #ccc; font-size: 12px; spacing: 6px; padding: 2px 0;
        }
        QCheckBox::indicator { width:14px; height:14px; border-radius:2px; }
        QCheckBox::indicator:unchecked {
            border: 1px solid #666; background: transparent;
        }
        QCheckBox::indicator:checked {
            border: 1px solid #4fc3f7; background: #4fc3f7;
        }
        QLineEdit {
            background: #2a2a3e; color: #eee; border: 1px solid #555;
            border-radius: 3px; padding: 2px 5px; font-size: 12px;
        }
        QLineEdit:focus { border-color: #4fc3f7; }
        QComboBox {
            background: #2a2a3e; color: #ccc; border: 1px solid #555;
            border-radius: 3px; padding: 2px 5px; font-size: 12px;
        }
        QComboBox::drop-down { border: none; }
        QComboBox QAbstractItemView {
            background: #2a2a3e; color: #ccc;
            selection-background-color: #4fc3f7; selection-color: #000;
        }
        QPushButton#applyBtn {
            background: #4fc3f7; color: #000; border: none;
            border-radius: 3px; padding: 4px 16px; font-size: 12px; font-weight: bold;
        }
        QPushButton#applyBtn:hover { background: #67d8ff; }
        QPushButton#resetBtn {
            background: transparent; color: #888; border: 1px solid #555;
            border-radius: 3px; padding: 4px 10px; font-size: 11px;
        }
        QPushButton#resetBtn:hover { border-color: #aaa; color: #ccc; }
        QFrame#divider { color: #3a3a50; }
        QScrollArea { border: none; background: transparent; }
        QScrollBar:vertical {
            background: #1e1e2e; width: 6px; margin: 0;
        }
        QScrollBar::handle:vertical {
            background: #444; border-radius: 3px; min-height: 20px;
        }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
    """

    def __init__(self, parent=None):
        super().__init__(parent, Qt.Popup | Qt.FramelessWindowHint)
        self.setObjectName("PeriodPicker")
        self.setAttribute(Qt.WA_StyledBackground, True)
        self.setStyleSheet(self._POPUP_SS)
        self.setFixedWidth(320)
        self._checkboxes: dict[str, QCheckBox] = {}  # key → QCheckBox
        self._custom_keys: list[str] = []  # 用户手动添加的自定义周期
        self._build_ui()

    # ── 构建 UI ──────────────────────────────────────────────────────────────

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(10, 8, 10, 8)
        root.setSpacing(0)

        # 标题行
        title_row = QHBoxLayout()
        title_lbl = QLabel("时间周期设置")
        title_lbl.setStyleSheet("color:#eee; font-size:13px; font-weight:bold;")
        title_row.addWidget(title_lbl)
        title_row.addStretch()
        close_btn = QPushButton("×")
        close_btn.setFixedSize(20, 20)
        close_btn.setStyleSheet(
            "QPushButton { background: transparent; color: #888; border: none; font-size: 14px; }"
            "QPushButton:hover { color: #fff; }"
        )
        close_btn.clicked.connect(self.hide)
        title_row.addWidget(close_btn)
        root.addLayout(title_row)
        root.addSpacing(4)

        # 说明文字
        hint = QLabel("勾选的周期将显示在工具栏")
        hint.setStyleSheet("color:#666; font-size:10px;")
        root.addWidget(hint)
        root.addSpacing(6)

        # 可滚动的复选框区域
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll_widget = QWidget()
        scroll_widget.setStyleSheet("background: transparent;")
        self._scroll_layout = QVBoxLayout(scroll_widget)
        self._scroll_layout.setContentsMargins(0, 0, 0, 0)
        self._scroll_layout.setSpacing(0)
        scroll.setWidget(scroll_widget)
        scroll.setFixedHeight(300)
        self._populate_checkboxes()
        root.addWidget(scroll)

        # 分隔线
        div = QFrame()
        div.setObjectName("divider")
        div.setFrameShape(QFrame.HLine)
        div.setStyleSheet("color:#3a3a50; margin: 6px 0 4px 0;")
        root.addWidget(div)

        # 自定义输入行
        custom_row = QHBoxLayout()
        custom_row.setSpacing(4)
        self._custom_input = QLineEdit()
        self._custom_input.setPlaceholderText("数值")
        self._custom_input.setFixedWidth(60)
        self._custom_unit = QComboBox()
        self._custom_unit.addItems(["分钟", "日"])
        self._custom_unit.setFixedWidth(56)
        add_btn = QPushButton("添加")
        add_btn.setFixedWidth(46)
        add_btn.setStyleSheet(
            "QPushButton { background:#2a2a3e; color:#ccc; border:1px solid #555; "
            "border-radius:3px; padding:3px 6px; font-size:11px; }"
            "QPushButton:hover { border-color:#4fc3f7; color:#fff; }"
        )
        add_btn.clicked.connect(self._on_add_custom)
        self._custom_input.returnPressed.connect(self._on_add_custom)
        custom_lbl = QLabel("自定义：")
        custom_lbl.setStyleSheet("color:#aaa; font-size:11px;")
        custom_row.addWidget(custom_lbl)
        custom_row.addWidget(self._custom_input)
        custom_row.addWidget(self._custom_unit)
        custom_row.addWidget(add_btn)
        custom_row.addStretch()
        root.addLayout(custom_row)
        root.addSpacing(6)

        # 底部按钮行
        btn_row = QHBoxLayout()
        self._reset_btn = QPushButton("重置默认")
        self._reset_btn.setObjectName("resetBtn")
        self._reset_btn.clicked.connect(self._on_reset)
        self._apply_btn = QPushButton("应 用")
        self._apply_btn.setObjectName("applyBtn")
        self._apply_btn.clicked.connect(self._on_apply)
        btn_row.addWidget(self._reset_btn)
        btn_row.addStretch()
        btn_row.addWidget(self._apply_btn)
        root.addLayout(btn_row)

    def _make_section_label(self, text: str) -> QLabel:
        lbl = QLabel(text)
        lbl.setStyleSheet(
            "color: #6a8fa8; font-size: 10px; font-weight: bold; "
            "padding: 6px 0 2px 0; letter-spacing: 1px;"
        )
        return lbl

    def _populate_checkboxes(self):
        """按分组生成复选框。"""
        from itertools import groupby

        layout = self._scroll_layout
        # 清空
        while layout.count():
            item = layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self._checkboxes.clear()

        current_group = None
        for label, key, group in self.ALL_OPTS:
            if group != current_group:
                current_group = group
                layout.addWidget(self._make_section_label(f"▸ {group}"))
            cb = QCheckBox(label)
            self._checkboxes[key] = cb
            layout.addWidget(cb)

        # 自定义周期（用户在本 session 添加的）
        if self._custom_keys:
            layout.addWidget(self._make_section_label("▸ 自定义"))
            for ck in self._custom_keys:
                if ck not in self._checkboxes:
                    cb = QCheckBox(ck)
                    self._checkboxes[ck] = cb
                    layout.addWidget(cb)

        layout.addStretch()

    # ── 公开接口 ──────────────────────────────────────────────────────────────

    def set_checked_keys(self, keys: list[str]):
        """设置哪些周期处于勾选状态（加载时调用）。"""
        for k, cb in self._checkboxes.items():
            cb.setChecked(k in keys)

    def checked_keys(self) -> list[str]:
        """返回当前勾选的 key 列表（保持 ALL_OPTS 顺序）。"""
        all_known = [k for _, k, _ in self.ALL_OPTS] + self._custom_keys
        return [k for k in all_known if k in self._checkboxes and self._checkboxes[k].isChecked()]

    def add_custom_period(self, key: str, label: str | None = None):
        """动态追加一个自定义周期并勾选它。"""
        if key in self._checkboxes:
            self._checkboxes[key].setChecked(True)
            return
        if key not in self._custom_keys:
            self._custom_keys.append(key)
        # 重新渲染复选框区
        checked = self.checked_keys()
        self._populate_checkboxes()
        self.set_checked_keys(checked + [key])

    def show_below(self, btn: "QPushButton"):
        """在按钮正下方显示弹窗，并保证不超出屏幕边界。"""
        pos = btn.mapToGlobal(btn.rect().bottomLeft())
        screen = QApplication.primaryScreen().availableGeometry()
        x = pos.x()
        y = pos.y() + 2
        if x + self.width() > screen.right():
            x = screen.right() - self.width() - 4
        if y + self.height() > screen.bottom():
            y = pos.y() - self.height() - btn.height() - 2
        self.move(x, y)
        self.show()
        self.raise_()

    # ── 内部槽 ────────────────────────────────────────────────────────────────

    def _on_add_custom(self):
        raw = self._custom_input.text().strip()
        if not raw.isdigit() or int(raw) <= 0:
            return
        unit = "m" if self._custom_unit.currentIndex() == 0 else "d"
        key = f"{raw}{unit}"
        self.add_custom_period(key)
        self._custom_input.clear()

    def _on_reset(self):
        self.set_checked_keys(self.DEFAULT_RESIDENT)

    def _on_apply(self):
        self.applied.emit(self.checked_keys())
        self.hide()


class KLineChartWorkspace(QWidget):
    source_status_ready = pyqtSignal(object)
    # Fix 71: 跨线程回调 — 替换 QTimer.singleShot (native 线程中无效) 为 pyqtSignal
    _subchart_results_ready = pyqtSignal(object)  # dict
    _subchart_last_bar_ready = pyqtSignal(object)  # dict
    _merge_chart_done = pyqtSignal(object, str, str)  # (DataFrame, symbol, period)
    _backfill_event_ready = pyqtSignal(object)
    _realtime_adjust_anchor_signal = pyqtSignal(object)

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
        self._use_ws_quote: bool = os.environ.get("EASYXT_USE_WS_QUOTE", "0") in (
            "1",
            "true",
            "True",
        )
        self._ws_error_consecutive: int = 0
        self._ws_error_emit_threshold: int = int(
            os.environ.get("EASYXT_WS_ERROR_ALERT_THRESHOLD", "3")
        )
        self._last_realtime_probe_line: Optional[str] = None
        self._last_raw_realtime_quote: Optional[dict[str, Any]] = None
        self._realtime_adjust_key: Optional[tuple[str, str, str]] = None
        self._realtime_adjust_ratio: float = 1.0
        self._realtime_adjust_ready: bool = True
        self._xtdata_probe_enabled: bool = os.environ.get(
            "EASYXT_ENABLE_XTDATA_QUOTE_PROBE", "0"
        ) in ("1", "true", "True")
        self._last_enrich_orderbook_ts: float = 0.0  # Fix-B: WS 五档补齐防抖时间戳
        self._orderbook_sink: Optional[RealtimeDuckDBSink] = None
        self.orderbook_panel: Optional[Any] = (
            None  # OrderbookPanel in KLC path; WatchlistWidget in LWC path
        )
        self._kline_side_watchlist: Optional[WatchlistWidget] = None
        self._tab_watchlist_widget: Optional[WatchlistWidget] = None
        self._tab_intraday_widget: Optional[Any] = None
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
        self._realtime_adjust_anchor_signal.connect(self._on_realtime_adjust_anchor_ready)
        self._flush_in_progress = False
        self._pipeline_apply_scheduled = False
        self._pending_pipeline_result: Optional[dict[str, Any]] = None
        self._pipeline_result_lock = threading.Lock()
        self._is_closed = False  # guard: QTimer callbacks must check before xtdata calls
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

    def _strict_history_gate_enabled(self) -> bool:
        return os.environ.get("EASYXT_STRICT_HISTORY_GATE", "1") in ("1", "true", "True")

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
        signal_bus.subscribe(Events.CHART_CROSSHAIR_MOVED, self._on_crosshair_moved)

    @staticmethod
    def _fmt_crosshair_time(time_val: object) -> str:
        """将图表时间值格式化为可读字符串（date str / Unix ts / 其他）。"""
        if time_val is None:
            return ""
        if isinstance(time_val, (int, float)) and time_val > 1_000_000_000:
            from datetime import datetime

            try:
                return datetime.fromtimestamp(float(time_val)).strftime("%Y-%m-%d %H:%M")
            except (OSError, OverflowError, ValueError):
                pass
        return str(time_val)

    @staticmethod
    def _fmt_crosshair_price(price_val: object) -> str:
        """将价格格式化为2位小数字符串。"""
        if price_val is None:
            return ""
        if isinstance(price_val, (int, float)):
            return f"{float(price_val):.2f}"
        if isinstance(price_val, str):
            try:
                return f"{float(price_val):.2f}"
            except (TypeError, ValueError):
                return price_val
        return str(price_val)

    def _on_crosshair_moved(self, **payload):
        try:
            event_symbol = str(payload.get("symbol") or "").strip()
            symbol_input = vars(self).get("symbol_input")
            current_symbol = symbol_input.text().strip() if symbol_input is not None else ""
            if event_symbol and current_symbol and event_symbol != current_symbol:
                return
            event_period = str(payload.get("period") or "").strip()
            period_combo = vars(self).get("period_combo")
            current_period = period_combo.currentText() if period_combo is not None else ""
            if event_period and current_period and event_period != current_period:
                return
            time_val = payload.get("time")
            price_val = payload.get("price")
            if hasattr(self, "_crosshair_info_label") and self._crosshair_info_label is not None:
                if price_val is None and time_val is None:
                    self._crosshair_info_label.setText("十字: --")
                else:
                    t_str = self._fmt_crosshair_time(time_val)
                    p_str = self._fmt_crosshair_price(price_val)
                    parts = ([f"t={t_str}"] if t_str else []) + ([f"p={p_str}"] if p_str else [])
                    self._crosshair_info_label.setText("十字: " + ("  ".join(parts) or "--"))
        except Exception:
            self._logger.exception("crosshair status update failed")

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
            current_symbol = (
                self.symbol_input.text().strip() if self.symbol_input is not None else ""
            )
            current_period = (
                self.period_combo.currentText() if self.period_combo is not None else ""
            )
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
            current_symbol = (
                self.symbol_input.text().strip() if self.symbol_input is not None else ""
            )
            if current_symbol and current_symbol in stock_codes:
                self._update_data_status_label()
                QTimer.singleShot(500, self.refresh_chart_data)
        except Exception:
            pass

    def _start_auto_data_sync(self, symbol: str) -> None:
        """图表打开时自动后台同步全历史数据（增量补充缺失部分，不阻塞 UI）。"""
        if not symbol:
            return
        if os.environ.get("EASYXT_CHART_AUTO_SYNC_ON_SYMBOL_CHANGE", "1") not in (
            "1",
            "true",
            "True",
        ):
            return
        # 取消/忽略上一个正在运行的同步（soft cancel）
        prev = getattr(self, "_sync_thread", None)
        if prev is not None and self._is_thread_running(prev):
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
                    logging.getLogger(__name__).warning("自动同步异常 [%s]: %s", self._symbol, exc)
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

    def _safe_stop_thread(self, thread_obj: Optional["QThread"]) -> None:
        """requestInterruption + quit；None-safe，不阻塞（无 wait）。"""
        if thread_obj is not None and self._is_thread_running(thread_obj):
            thread_obj.requestInterruption()
            thread_obj.quit()

    def _safe_thread_wait(self, thread_obj: "QThread", msecs: int) -> bool:
        """轮询替代 QThread.wait()，防止 Windows 下 wait() 永久挂起。

        使用 time.sleep 轮询 isRunning()，不依赖 QThread.wait() 的超时实现，
        避免 Windows 上 wait() 永久阻塞以及辅助线程积累耗尽资源的问题。

        返回 True 表示线程在超时内停止，False 表示超时。
        """
        import time as _time

        if not self._is_thread_running(thread_obj):
            return True
        _deadline = _time.monotonic() + msecs / 1000.0
        _interval = 0.02  # 20ms 轮询间隔
        while _time.monotonic() < _deadline:
            if not self._is_thread_running(thread_obj):
                return True
            _time.sleep(_interval)
        return not self._is_thread_running(thread_obj)

    def _drain_owned_threads(self) -> None:
        import time as _time

        _t0 = _time.monotonic()
        _MAX_DRAIN_S = 5.0  # 全局超时

        try:
            owned_threads = [t for t in self.findChildren(QThread) if t is not None]
        except Exception:
            owned_threads = []

        for t in owned_threads:
            if _time.monotonic() - _t0 > _MAX_DRAIN_S:
                try:
                    if self._is_thread_running(t):
                        t.terminate()
                except Exception:
                    pass
                continue
            if not self._is_thread_running(t):
                continue
            try:
                t.requestInterruption()
            except Exception:
                pass
            try:
                t.quit()
            except Exception:
                pass
            try:
                _elapsed = _time.monotonic() - _t0
                _wait_ms = min(800, max(50, int((_MAX_DRAIN_S - _elapsed) * 1000)))
                if not self._safe_thread_wait(t, _wait_ms):
                    try:
                        t.terminate()
                    except Exception:
                        pass
            except Exception:
                pass

    def _on_auto_sync_done(self) -> None:
        """后台同步完成后，恢复复权选择器并刷新数据范围标签。"""
        if hasattr(self, "adjust_combo"):
            self.adjust_combo.setEnabled(True)
        self._update_data_status_label()
        QTimer.singleShot(500, self.refresh_chart_data)

    def _update_data_status_label(self):
        """后台查询当前标的+周期的本地数据范围，更新状态标签。"""
        if getattr(self, "_is_closed", False):
            return
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
            # 黄金标准1D缺口信号：(symbol, listing_date, first_stored_date)
            gap_found = pyqtSignal(str, str, str)

            def __init__(self, duckdb_path: str, symbol: str, period: str):
                super().__init__()
                self._duckdb_path = duckdb_path
                self._symbol = symbol
                self._period = period

            def run(self):
                try:
                    import pandas as _pd
                    from data_manager.unified_data_interface import UnifiedDataInterface

                    iface = UnifiedDataInterface(
                        duckdb_path=self._duckdb_path,
                        xtdata_call_mode="direct",
                    )
                    df = iface.get_data_coverage(
                        stock_codes=[self._symbol],
                        periods=[self._period],
                    )
                    if df.empty or self._period not in df.columns:
                        self.result_ready.emit("本地: 无数据")
                        return
                    val = str(df.at[self._symbol, self._period]) if self._symbol in df.index else ""
                    if not val:
                        self.result_ready.emit("本地: 无数据")
                        return
                    gate_status = iface.get_latest_gate_status(self._symbol, self._period)
                    gate_suffix = ""
                    if gate_status:
                        quality = str(gate_status.get("quality_grade") or "").strip()
                        replayable = bool(gate_status.get("replayable"))
                        lineage_complete = bool(gate_status.get("lineage_complete"))
                        gate_parts: list[str] = []
                        if quality:
                            gate_parts.append(quality)
                        if replayable:
                            gate_parts.append("可回放")
                        if lineage_complete:
                            gate_parts.append("血缘完整")
                        if gate_parts:
                            gate_suffix = " [" + " / ".join(gate_parts) + "]"
                    # ── 黄金标准1D完备性检查：首日左对齐验证 ──────────────────
                    if self._period == "1d" and not self.isInterruptionRequested():
                        try:
                            # val 格式如 "2001-02-26~2026-03-23(6005条)"
                            first_stored = val.split("~")[0].strip() if "~" in val else ""
                            if first_stored:
                                listing_date = iface.get_listing_date(self._symbol)
                                if (
                                    listing_date
                                    and listing_date > "1990-01-01"
                                    and listing_date < first_stored
                                ):
                                    # 用交易日计数（精确），而非自然日
                                    try:
                                        from data_manager.smart_data_detector import (
                                            get_trading_calendar,
                                        )

                                        _cal = get_trading_calendar()
                                        _td_list = _cal.get_trading_days(
                                            _pd.Timestamp(listing_date).date(),
                                            _pd.Timestamp(first_stored).date(),
                                        )
                                        # 减1：上市首日本身算已覆盖（缺gap内的交易日）
                                        gap_days = max(0, len(_td_list) - 1)
                                        gap_label = f"⚠️缺首日起{gap_days}个交易日"
                                    except Exception:
                                        gap_days = int(
                                            (
                                                _pd.Timestamp(first_stored)
                                                - _pd.Timestamp(listing_date)
                                            ).days
                                        )
                                        gap_label = f"⚠️缺首日起{gap_days}天"
                                    self.result_ready.emit(f"本地: {val}{gate_suffix} {gap_label}")
                                    self.gap_found.emit(self._symbol, listing_date, first_stored)
                                    return
                        except Exception:
                            pass
                    # ─────────────────────────────────────────────────────────
                    self.result_ready.emit(f"本地: {val}{gate_suffix}" if val else "本地: 无数据")
                except Exception:
                    self.result_ready.emit("本地: --")

        prev = getattr(self, "_coverage_thread", None)
        if prev is not None and self._is_thread_running(prev):
            prev.requestInterruption()
        t = _CovThread(duckdb_path, symbol, period)
        t.setParent(self)
        t.result_ready.connect(label_ref.setText)
        t.gap_found.connect(self._on_1d_gap_found)
        t.finished.connect(t.deleteLater)
        self._coverage_thread = t
        t.start()

    def _on_1d_gap_found(self, symbol: str, listing_date: str, first_stored: str) -> None:
        """1D黄金标准缺口回调：从上市首日→本地首条之间的历史缺口，自动发起多源回补。"""
        if not symbol or not listing_date or not first_stored:
            return
        # 仅处理当前活跃标的的缺口
        current_sym = self.symbol_input.text().strip() if self.symbol_input else ""
        if current_sym != symbol:
            return
        adjust = self._get_adjust_key()
        today = pd.Timestamp.today().strftime("%Y-%m-%d")
        self._logger.info(
            "1D黄金标准缺口: %s 首日%s → 本地起始%s，触发多源历史回补",
            symbol,
            listing_date,
            first_stored,
        )
        # 延迟 800ms 发起回补请求（让首屏数据先完成渲染）
        QTimer.singleShot(
            800,
            lambda s=symbol, ld=listing_date, a=adjust, td=today: self._request_segment(
                s, "1d", a, ld, td, "merge"
            ),
        )

    def _on_data_quality_updated(self, **kwargs) -> None:
        """响应 Events.DATA_QUALITY_UPDATED，更新工具栏质量指示器颜色和 tooltip。

        payload 字段（来自 cross_validate_sources 广播）：
          stock_code, consistent, consistency_rate, max_diff_pct, source
        """
        if not hasattr(self, "_quality_dot_label"):
            return
        # 仅在当前标的匹配时更新
        stock_code = kwargs.get("stock_code", "")
        current_sym = self.symbol_input.text().strip() if self.symbol_input else ""
        if stock_code and current_sym and stock_code != current_sym:
            return
        rate = float(kwargs.get("consistency_rate", -1.0))
        max_diff = float(kwargs.get("max_diff_pct", 0.0))
        source = kwargs.get("source", "unknown")
        if rate < 0:
            color, status = "#555", "未验证"
        elif rate >= 0.99:
            color, status = "#4CAF50", f"优质 {rate * 100:.1f}%"
        elif rate >= 0.90:
            color, status = "#FFC107", f"注意 {rate * 100:.1f}%"
        else:
            color, status = "#F44336", f"异常 {rate * 100:.1f}%"
        self._quality_dot_label.setStyleSheet(f"color:{color}; font-size:12px; padding:0 2px;")
        tooltip = (
            f"数据质量: {status}\n"
            f"对照源: {source}\n"
            f"最大偏差: {max_diff:.3f}%\n"
            f"(灰)未验证 (绿)≥99% (黄)≥90% (红)<90%"
        )
        self._quality_dot_label.setToolTip(tooltip)

    def _bind_range_change_event(self):
        if self._range_change_bound:
            return
        try:
            if self.chart is not None:
                self.chart.events.range_change += self._on_range_change
            elif self.chart_adapter is not None and hasattr(self.chart_adapter, "on_range_changed"):
                on_range_changed = getattr(self.chart_adapter, "on_range_changed", None)
                if callable(on_range_changed):
                    on_range_changed(self._on_adapter_range_changed)
                on_chart_click = getattr(self.chart_adapter, "on_chart_click", None)
                if callable(on_chart_click):
                    on_chart_click(self._on_adapter_chart_click)
                on_crosshair_move = getattr(self.chart_adapter, "on_crosshair_move", None)
                if callable(on_crosshair_move):
                    on_crosshair_move(self._on_adapter_crosshair_move)
            self._range_change_bound = True
        except Exception:
            self._logger.exception("Failed to bind range change event")

    def _to_datetime_safe(self, value: Any):
        if value is None:
            return pd.NaT
        try:
            return pd.to_datetime(str(value), errors="coerce")
        except Exception:
            return pd.NaT

    def _on_adapter_chart_click(self, payload: dict) -> None:
        try:
            price = payload.get("price") if isinstance(payload, dict) else None
            if price is None:
                return
            signal_bus.emit(
                Events.CHART_PRICE_CLICKED,
                price=float(price),
                time=(payload.get("time") if isinstance(payload, dict) else None),
            )
        except Exception:
            self._logger.exception("adapter chart click event handling failed")

    def _on_adapter_range_changed(self, payload: dict) -> None:
        if not isinstance(payload, dict) or self._loaded_range is None:
            return
        period = self.period_combo.currentText()
        step = self._get_time_step(period)
        if step.total_seconds() <= 0:
            return
        start_ts = pd.Timestamp(self._loaded_range[0])
        end_ts = pd.Timestamp(self._loaded_range[1])
        from_ts = self._to_datetime_safe(payload.get("from"))
        to_ts = self._to_datetime_safe(payload.get("to"))
        if pd.isna(from_ts) or pd.isna(to_ts):
            return
        bars_before = float((from_ts - start_ts) / step)
        bars_after = float((end_ts - to_ts) / step)
        self._on_range_change(None, bars_before, bars_after)

    def _on_adapter_crosshair_move(self, payload: dict) -> None:
        try:
            if not isinstance(payload, dict):
                return
            state = vars(self)
            symbol_input = state.get("symbol_input")
            period_combo = state.get("period_combo")
            symbol = ""
            if symbol_input is not None and hasattr(symbol_input, "text"):
                try:
                    symbol = str(symbol_input.text()).strip()
                except Exception:
                    symbol = ""
            period = ""
            if period_combo is not None and hasattr(period_combo, "currentText"):
                try:
                    period = str(period_combo.currentText())
                except Exception:
                    period = ""
            signal_bus.emit(
                Events.CHART_CROSSHAIR_MOVED,
                time=payload.get("time"),
                price=payload.get("price"),
                symbol=symbol,
                period=period,
                payload=payload,
            )
        except Exception:
            self._logger.exception("adapter crosshair event handling failed")

    def load_symbol(self, symbol: str, **kwargs):
        if not symbol:
            return
        self.symbol_input.setText(symbol)
        if self.orderbook_panel is not None:
            self.orderbook_panel.set_current_symbol(symbol)
        if self._tab_watchlist_widget is not None:
            self._tab_watchlist_widget.set_current_symbol(symbol)
        if self._tab_intraday_widget is not None:
            self._tab_intraday_widget.set_symbol(symbol)
        # 标的切换时立即从 DB 加载历史盘口快照（收盘/无行情时五档先有数据，早于 chart pipeline）
        self._load_orderbook_snapshot_from_db(reason="symbol_change")
        # 打开标的时立即触发后台自动同步（增量补充历史数据）
        self._start_auto_data_sync(symbol)
        if self.toolbox_panel:
            self.toolbox_panel.set_symbol(symbol)
        if self.chart_events:
            self.chart_events.set_symbol(symbol)
        # Sprint 4: 更新图表水印
        if isinstance(self.chart_adapter, NativeLwcChartAdapter):
            try:
                self.chart_adapter.set_watermark(symbol)
            except Exception:
                pass
        # 标的切换时刷新 WS 行情订阅（仅当 realtime 模式已激活）
        if (
            self._use_ws_quote
            and self.realtime_timer is not None
            and self.realtime_timer.isActive()
        ):
            self._restart_ws_quote_worker()
        self.refresh_chart_data()

    def change_period(self, period: str, **kwargs):
        if not period:
            return
        # 同步按钮组选中状态
        self._period_strip.set_current(period)
        index = self.period_combo.findText(period)
        if index >= 0:
            if index == self.period_combo.currentIndex():
                self.refresh_chart_data()
            else:
                self.period_combo.setCurrentIndex(index)
        if self.chart_events:
            self.chart_events.set_period(period)

    def mark_order(self, side: str, symbol: str, price: float, volume: int, **kwargs):
        if self.chart is None and self.chart_adapter is None:
            return
        current_symbol = self.symbol_input.text().strip()
        if symbol != current_symbol:
            return
        normalized_side = (side or "").lower()
        marker_text = f"{'📈' if normalized_side == 'buy' else '📉'} {normalized_side.upper()}"
        if self.chart_adapter:
            self.chart_adapter.marker(marker_text)
        elif self.chart is not None:
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
        if self.chart is None:  # KLine 路径：cost_line 仅 LWC 支持
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
            (self._create_watchlist_tab, "报价列表"),
            (self._create_heatmap_tab, "行情热图"),
            (self._create_intraday_tab, "分时联动"),
            (self._create_positions_tab, "持仓/结算"),
            (self._create_orders_tab, "委托/成交"),
            (self._create_funds_tab, "资金账户"),
            (self._create_risk_monitor_tab, "实时风控"),
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
        self._sync_watchlist_page_btn_state(index)
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
            self._sync_watchlist_page_btn_state(index)
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
    _TIMEZONE_OPTIONS: list[tuple[str, str]] = [
        ("北京时间", "Asia/Shanghai"),
        ("UTC", "UTC"),
        ("纽约", "America/New_York"),
        ("伦敦", "Europe/London"),
        ("东京", "Asia/Tokyo"),
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
    _WATCHLIST_PAGE_BTN_ACTIVE_STYLE = """
        QPushButton {
            border: 1px solid #2d6f9f; background: rgba(79,195,247,0.20); color: #d7f3ff;
            padding: 3px 10px; font-size: 12px; border-radius: 3px;
        }
        QPushButton:hover { border-color: #67d8ff; color: #fff; }
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

        # ── 周期按钮区（响应式：_PeriodOverflowStrip 自动折叠溢出按钮）────────
        # 隐藏 combo 用于向后兼容（period_combo.currentText() 感知当前周期）
        self.period_combo = QComboBox()
        self.period_combo.setVisible(False)
        for _, _k, _ in _PeriodPickerPopup.ALL_OPTS:
            self.period_combo.addItem(_k)
        layout.addWidget(self.period_combo)

        # _PeriodOverflowStrip 是真正的按钮容器
        self._period_strip = _PeriodOverflowStrip(self)
        self._period_strip.period_clicked.connect(self._on_period_btn_clicked)
        layout.addWidget(self._period_strip)
        # 保持 _period_btn_group / _period_buttons_map 作为活引用（向后兼容）
        self._period_btn_group: QButtonGroup = self._period_strip.btn_group()
        self._period_buttons_map: dict = self._period_strip.buttons_map()

        # "•••" 周期选择器按钮
        self._period_picker_btn = QPushButton("•••")
        self._period_picker_btn.setToolTip("时间周期设置（可自选常驻周期）")
        self._period_picker_btn.setStyleSheet(self._PERIOD_BTN_STYLE)
        self._period_picker_btn.setFixedWidth(28)
        self._period_picker_popup = _PeriodPickerPopup(self.window())
        self._period_picker_popup.applied.connect(self._on_period_picker_applied)
        self._period_picker_btn.clicked.connect(
            lambda: self._period_picker_popup.show_below(self._period_picker_btn)
        )
        layout.addWidget(self._period_picker_btn)

        # 从 QSettings 加载已保存的常驻周期列表，初始化按钮
        self._resident_period_keys: list = self._load_resident_periods()
        self._rebuild_period_buttons()
        # ── 周期按钮区 end ─────────────────────────────────────────────────────

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

        # ── 画线工具下拉按钮 (Sprint 4) ──
        self._drawing_btn = QPushButton("画线 ▾")
        self._drawing_btn.setStyleSheet(self._INDICATOR_BTN_STYLE)
        self._drawing_menu = QMenu(self)
        self._drawing_menu.setStyleSheet(
            "QMenu { background:#2a2a3e; color:#ccc; border:1px solid #555; padding:4px; }"
            "QMenu::item { padding:4px 16px; }"
            "QMenu::item:selected { background:#4fc3f7; color:#000; }"
        )
        self._build_drawing_menu()
        self._drawing_btn.setMenu(self._drawing_menu)
        layout.addWidget(self._drawing_btn)

        self.watchlist_page_btn = QPushButton("报价页")
        self.watchlist_page_btn.setCheckable(True)
        self.watchlist_page_btn.setStyleSheet(self._INDICATOR_BTN_STYLE)
        self.watchlist_page_btn.clicked.connect(self._open_watchlist_tab)
        layout.addWidget(self.watchlist_page_btn)

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

        self.timezone_combo = QComboBox()
        for tz_label, tz_key in self._TIMEZONE_OPTIONS:
            self.timezone_combo.addItem(tz_label, tz_key)
        self.timezone_combo.setStyleSheet(
            "QComboBox { background:#2a2a3e; color:#ccc; border:1px solid #444; "
            "border-radius:3px; padding:2px 6px; font-size:11px; min-width:90px; }"
            "QComboBox:hover { border-color:#4fc3f7; }"
            "QComboBox::drop-down { border:none; }"
            "QComboBox QAbstractItemView { background:#2a2a3e; color:#ccc; selection-background-color:#4fc3f7; }"
        )
        self.timezone_combo.currentIndexChanged.connect(self._on_timezone_changed)
        layout.addWidget(self.timezone_combo)

        # ── 源状态 / 监控设置 ──
        self.source_status_label = QLabel("源: --")
        self.source_status_label.setStyleSheet("color:#666; font-size:10px;")
        self.source_status_label.setToolTip("数据源健康状态")
        layout.addWidget(self.source_status_label)
        self._crosshair_info_label = QLabel("十字: --")
        self._crosshair_info_label.setStyleSheet("color:#7c9fbf; font-size:10px;")
        self._crosshair_info_label.setToolTip("十字光标当前位置（时间/价格）")
        layout.addWidget(self._crosshair_info_label)

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

        # ── 数据质量指示器（● 颜色点 + 对账标识） ──
        # 订阅 Events.DATA_QUALITY_UPDATED，实时感知多源交叉验证结果
        self._quality_dot_label = QLabel("●")
        self._quality_dot_label.setStyleSheet("color:#555; font-size:12px; padding:0 2px;")
        self._quality_dot_label.setToolTip(
            "数据质量: 未验证\n(灰)未验证 (绿)≥99%一致 (黄)≥90% (红)<90%"
        )
        layout.addWidget(self._quality_dot_label)
        # 延迟连接（确保 signal_bus 已初始化）
        try:
            from core.signal_bus import signal_bus as _sb
            from core.events import Events as _Ev

            if hasattr(_Ev, "DATA_QUALITY_UPDATED"):
                _sb.subscribe(_Ev.DATA_QUALITY_UPDATED, self._on_data_quality_updated)
        except Exception:
            pass

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
        thread_manager.run(self._load_symbol_completions, name="load_symbol_completions")

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

    # ── 画线工具菜单构建 (Sprint 4) ──

    # 画线工具分组定义: (显示名, drawing_type)
    _DRAWING_TOOL_GROUPS = [
        ("── 水平线 ──", None),
        ("水平直线", "hline"),
        ("水平射线", "hray"),
        ("水平线段", "hseg"),
        ("── 垂直线 ──", None),
        ("垂直直线", "vline"),
        ("垂直射线", "vray"),
        ("垂直线段", "vseg"),
        ("── 趋势线 ──", None),
        ("趋势线段", "tline"),
        ("射线", "rayLine"),
        ("直线", "straightLine"),
        ("── 通道 & 斐波那契 ──", None),
        ("价格通道", "priceChannel"),
        ("平行直线", "parallel"),
        ("斐波那契回撤", "fibonacci"),
        ("── 标注 ──", None),
        ("价格线", "priceLine"),
        ("标注", "annotation"),
        ("标签", "tag"),
    ]

    def _build_drawing_menu(self):
        menu = self._drawing_menu
        for label, dtype in self._DRAWING_TOOL_GROUPS:
            if dtype is None:
                # 分组标题 separator
                sep = menu.addSeparator()
                sep.setText(label)
                continue
            act = QAction(label, menu)
            act.setData(dtype)
            act.triggered.connect(
                lambda _checked=False, dt=dtype: self._on_drawing_tool_clicked(dt)
            )
            menu.addAction(act)

        # 末尾添加「清除全部画线」
        menu.addSeparator()
        clear_act = QAction("🗑 清除全部画线", menu)
        clear_act.triggered.connect(self._clear_all_drawings)
        menu.addAction(clear_act)

    def _on_drawing_tool_clicked(self, drawing_type: str):
        """在当前图表上添加画线。"""
        if not isinstance(self.chart_adapter, NativeLwcChartAdapter):
            return
        try:
            self.chart_adapter.add_drawing(drawing_type)
        except Exception:
            self._logger.exception("add drawing %s failed", drawing_type)

    def _clear_all_drawings(self):
        """清除当前图表上的全部画线。"""
        if not isinstance(self.chart_adapter, NativeLwcChartAdapter):
            return
        try:
            drawings = self.chart_adapter.get_drawings(timeout=2.0)
            for d in drawings:
                did = d.get("id")
                if did:
                    self.chart_adapter.remove_drawing(did)
        except Exception:
            self._logger.exception("clear all drawings failed")

    def _toggle_indicator(self, name: str, checked: bool):
        setattr(self, f"{name}_visible", checked)
        # 同步复选框 (向后兼容)
        if name == "macd" and self.macd_check:
            self.macd_check.setChecked(checked)
        elif name == "rsi" and self.rsi_check:
            self.rsi_check.setChecked(checked)
        self._apply_indicator_visibility()

    def _apply_indicator_visibility(self):
        if isinstance(self.chart_adapter, KLineChartAdapter):
            self._apply_kline_indicator_visibility()
            return
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

    def _apply_kline_indicator_visibility(self) -> None:
        adapter = self.chart_adapter
        if not isinstance(adapter, KLineChartAdapter):
            return
        try:
            adapter.remove_indicator(pane_id="pane_macd")
            adapter.remove_indicator(pane_id="pane_rsi")
            adapter.remove_indicator(pane_id="pane_vol")
            adapter.remove_indicator(pane_id="pane_kdj")
            adapter.remove_indicator(pane_id="candle_pane", name="MA")
            adapter.remove_indicator(pane_id="candle_pane", name="BOLL")
            if self.macd_visible:
                adapter.create_indicator("MACD", is_stack=True, pane_id="pane_macd", height=90)
            if self.rsi_visible:
                adapter.create_indicator("RSI", is_stack=True, pane_id="pane_rsi", height=90)
            if self.vol_visible:
                adapter.create_indicator(
                    "VOL", is_stack=True, pane_id="pane_vol", height=90, calc_params=[5, 10]
                )
            if self.kdj_visible:
                adapter.create_indicator("KDJ", is_stack=True, pane_id="pane_kdj", height=90)
            if self.ma_visible:
                adapter.create_indicator(
                    "MA", is_stack=False, pane_id="candle_pane", calc_params=[5, 10, 20, 60]
                )
            if self.boll_visible:
                adapter.create_indicator(
                    "BOLL", is_stack=False, pane_id="candle_pane", calc_params=[20, 2]
                )
        except Exception:
            self._logger.exception("apply kline indicator visibility failed")

    # ── 周期选择核心方法 ───────────────────────────────────────────────────────

    def _load_resident_periods(self) -> list[str]:
        """从 QSettings 读取用户保存的常驻周期列表；若无则返回默认值。"""
        settings = QSettings("EasyXT", "KLineChartWorkspace")
        saved = settings.value("resident_period_keys", None)
        if saved and isinstance(saved, list) and len(saved) > 0:
            return [str(k) for k in saved if k]
        return list(_PeriodPickerPopup.DEFAULT_RESIDENT)

    def _save_resident_periods(self, keys: list[str]):
        """将常驻周期列表持久化到 QSettings。"""
        settings = QSettings("EasyXT", "KLineChartWorkspace")
        settings.setValue("resident_period_keys", keys)

    def _rebuild_period_buttons(self):
        """通过 _PeriodOverflowStrip.set_periods() 重建常驻周期按钮，并同步 picker 弹窗。"""
        key_to_label: dict = {k: lbl for lbl, k, _ in _PeriodPickerPopup.ALL_OPTS}
        self._period_strip.set_periods(self._resident_period_keys, key_to_label)

        # 恢复选中状态：先取当前 period，再 fallback 到 1d
        current_period = self.period_combo.currentText() or "1d"
        self._period_strip.set_current(
            current_period
            if current_period in self._period_buttons_map
            else (
                "1d"
                if "1d" in self._period_buttons_map
                else (self._resident_period_keys[0] if self._resident_period_keys else "")
            )
        )

        # 同步 picker 弹窗复选框（含自定义周期）
        popup = self._period_picker_popup
        known_keys = {k for _, k, _ in _PeriodPickerPopup.ALL_OPTS}
        for ck in self._resident_period_keys:
            if ck not in known_keys:
                popup.add_custom_period(ck)
        popup.set_checked_keys(self._resident_period_keys)

    def _on_period_picker_applied(self, keys: list[str]):
        """用户在 picker 弹窗点击「应用」后的回调。"""
        if not keys:
            return
        self._resident_period_keys = keys
        self._save_resident_periods(keys)
        self._rebuild_period_buttons()

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

    def _on_resident_period_btn_clicked(self, period_key: str):
        """（兼容保留）"""
        self._on_period_btn_clicked(period_key)

    def _on_custom_period_selected(self, period_key: str):
        """（兼容保留）"""
        self._on_period_btn_clicked(period_key)

    def _on_input_custom_period(self, unit: str):
        """（兼容保留）"""
        pass

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
        thread_manager.run(
            self._compute_subchart_bg,
            args=(manager, data_copy, full_set),
            name="compute_subchart_bg",
        )

    def _compute_subchart_bg(
        self, manager: SubchartManager, data: pd.DataFrame, full_set: bool = False
    ):
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

    def showEvent(self, a0):
        super().showEvent(a0)
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

    def resizeEvent(self, a0):
        super().resizeEvent(a0)
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
        self._sync_watchlist_page_btn_state()
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
        # ── 提前检测后端，决定走哪条路径 ────────────────────────────────────
        try:
            from gui_app.widgets.chart.backend_config import get_chart_backend_config

            _backend_key = get_chart_backend_config().get_backend()
        except Exception:
            _backend_key = os.environ.get("EASYXT_CHART_BACKEND", "klinechart").strip().lower()

        if _backend_key == "klinechart":
            return self._create_klinechart_widget(parent)
        return self._create_lwc_widget(parent)

    def _create_klinechart_widget(self, parent: QWidget) -> QWidget:
        """KLineChart 路径：无 QtChart / SubchartManager，仅 adapter + webview。"""
        try:
            from PyQt5.QtWebEngineWidgets import QWebEngineView

            _ = QWebEngineView  # 确认依赖可用
            adapter = KLineChartAdapter()
            webview, _is_native = adapter.initialize(parent)
            # KLine 路径：chart 保持 None，指标 / 画线全走 adapter RPC
            self.chart = None
            self.chart_adapter = adapter
            self.subchart_manager = None
            # toolbox_panel / position_table / chart_events 保持 None（由 __init__ 初始化）
            self._bind_range_change_event()
            if self.timezone_combo is not None:
                self._on_timezone_changed(self.timezone_combo.currentIndex())
            self._apply_kline_indicator_visibility()
            webview.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            webview.setMinimumHeight(0)
            webview.setMinimumWidth(0)
            # 右侧面板：五档/成交明细/关键数据已整合进 HTML 侧边栏；Qt 侧仅保留多股票监控列表
            self.orderbook_panel = OrderbookPanel()
            self._klc_stats_panel = _KlcStatsPanel()
            self._klc_trades_panel = _KlcTradesPanel()
            self._kline_side_watchlist = WatchlistWidget(
                state_key="side_watchlist", enable_fullscreen=True
            )
            self._kline_side_watchlist.symbol_selected.connect(self.load_symbol)
            self._kline_side_watchlist.setMinimumWidth(0)
            # 右侧面板：五档盘口 → 关键数据 → 成交明细（从上到下）
            right_panel = QWidget()
            right_panel.setMinimumWidth(0)
            right_layout = QVBoxLayout(right_panel)
            right_layout.setContentsMargins(0, 0, 0, 0)
            right_layout.setSpacing(0)
            right_layout.addWidget(self.orderbook_panel, stretch=3)
            right_layout.addWidget(self._klc_stats_panel, stretch=2)
            right_layout.addWidget(self._klc_trades_panel, stretch=3)
            # 三栏水平分割器：左侧监控 | 中央图表 | 右侧盘口
            splitter = QSplitter(Qt.Horizontal)
            splitter.setChildrenCollapsible(True)
            splitter.addWidget(self._kline_side_watchlist)  # index 0: 左侧（可向左关闭）
            splitter.addWidget(webview)  # index 1: 中央图表
            splitter.addWidget(right_panel)  # index 2: 右侧（可向右关闭）
            splitter.setStretchFactor(0, 1)
            splitter.setStretchFactor(1, 6)
            splitter.setStretchFactor(2, 2)
            splitter.setSizes([160, 900, 230])
            container = QWidget()
            container_layout = QVBoxLayout(container)
            container_layout.setContentsMargins(0, 0, 0, 0)
            container_layout.setSpacing(0)
            container_layout.addWidget(splitter)
            # 首屏默认加载统一由 showEvent -> _load_default_chart_data 触发。
            # 若此处也立即 refresh_chart_data()，会在窗口真正 show 前后各触发一次，
            # 生成两个并发 _ChartDataLoadThread，并在本地数据略有缺口时同时打进
            # QMT 在线补数路径，触发 xtquant BSON 断言崩溃。
            return container
        except Exception as exc:
            self._logger.exception("KLineChart initialization failed")
            frame = QFrame()
            frame.setFrameStyle(QFrame.StyledPanel)
            frame.setMinimumHeight(520)
            layout = QVBoxLayout(frame)
            label = QLabel(f"KLineChart 初始化失败: {exc}")
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            label.setStyleSheet("font-size: 14px; color: #666666;")
            layout.addWidget(label)
            return frame

    def _create_lwc_widget(self, parent: QWidget) -> QWidget:
        """LWC 路径（lwc_python / native_lwc）：QtChart + SubchartManager 完整栈。"""
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

            _native = (
                self.chart_adapter
                if isinstance(self.chart_adapter, NativeLwcChartAdapter)
                else None
            )
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
            self.subchart_manager.set_visibility(
                macd=self.macd_visible,
                rsi=self.rsi_visible,
                vol=self.vol_visible,
                kdj=self.kdj_visible,
                ma=self.ma_visible,
                boll=self.boll_visible,
            )
            webview = self.chart.get_webview()
            webview.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            webview.setMinimumHeight(0)
            webview.setMinimumWidth(0)
            self.orderbook_panel = WatchlistWidget(
                state_key="side_watchlist", enable_fullscreen=True
            )
            self.orderbook_panel.symbol_selected.connect(self.load_symbol)
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
        if self.chart is None and self.chart_adapter is None:
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

    def _create_watchlist_tab(self) -> QWidget:
        panel = WatchlistWidget(state_key="tab_watchlist", enable_fullscreen=True)
        panel.symbol_selected.connect(self.load_symbol)
        panel.set_current_symbol(self.symbol_input.text().strip())
        self._tab_watchlist_widget = panel
        return panel

    def _create_heatmap_tab(self) -> QWidget:
        TreemapWidget = self._dynamic_class(
            "gui_app.widgets.treemap.treemap_widget", "TreemapWidget"
        )
        panel = TreemapWidget()
        panel = cast(Any, panel)
        panel.symbol_clicked.connect(self.load_symbol)
        return panel

    def _create_positions_tab(self) -> QWidget:
        PositionsPanel = self._dynamic_class(
            "gui_app.widgets.positions.positions_panel", "PositionsPanel"
        )
        panel = PositionsPanel()
        panel = cast(Any, panel)
        panel.symbol_clicked.connect(self.load_symbol)
        return panel

    def _create_intraday_tab(self) -> QWidget:
        IntradayPanel = self._dynamic_class(
            "gui_app.widgets.intraday.intraday_panel", "IntradayPanel"
        )
        panel = IntradayPanel()
        panel = cast(Any, panel)
        panel.symbol_clicked.connect(self.load_symbol)
        if hasattr(panel, "set_symbol"):
            panel.set_symbol(self.symbol_input.text().strip())
        self._tab_intraday_widget = panel
        return panel

    def _create_orders_tab(self) -> QWidget:
        OrdersPanel = self._dynamic_class("gui_app.widgets.orders.orders_panel", "OrdersPanel")
        panel = OrdersPanel()
        panel = cast(Any, panel)
        panel.symbol_clicked.connect(self.load_symbol)
        return panel

    def _create_funds_tab(self) -> QWidget:
        FundsPanel = self._dynamic_class("gui_app.widgets.funds.funds_panel", "FundsPanel")
        panel = FundsPanel()
        return panel

    def _create_risk_monitor_tab(self) -> QWidget:
        RealtimeRiskMonitor = self._dynamic_class(
            "gui_app.widgets.realtime_risk_monitor", "RealtimeRiskMonitor"
        )
        panel = RealtimeRiskMonitor()
        return panel

    def _open_watchlist_tab(self) -> None:
        if not self.include_operation_panel or self.bottom_tabs is None:
            return
        if self.root_splitter is not None:
            sizes = self.root_splitter.sizes()
            if len(sizes) >= 2 and sizes[1] <= 5:
                self._toggle_bottom_panel()
        target = None
        for i in range(self.bottom_tabs.count()):
            if self.bottom_tabs.tabText(i) == "报价列表":
                target = i
                break
        if target is None:
            return
        self._on_bottom_tab_changed(target)
        self.bottom_tabs.setCurrentIndex(target)
        self._sync_watchlist_page_btn_state(target)

    def _watchlist_tab_index(self) -> int:
        if self.bottom_tabs is None:
            return -1
        for i in range(self.bottom_tabs.count()):
            if self.bottom_tabs.tabText(i) == "报价列表":
                return i
        return -1

    def _sync_watchlist_page_btn_state(self, index: int | None = None) -> None:
        if not hasattr(self, "watchlist_page_btn") or self.watchlist_page_btn is None:
            return
        active = False
        if self.bottom_tabs is not None and self.include_operation_panel:
            idx = self.bottom_tabs.currentIndex() if index is None else index
            active = idx == self._watchlist_tab_index()
            if self.root_splitter is not None:
                sizes = self.root_splitter.sizes()
                if len(sizes) >= 2 and sizes[1] <= 5:
                    active = False
        self.watchlist_page_btn.setChecked(active)
        self.watchlist_page_btn.setStyleSheet(
            self._WATCHLIST_PAGE_BTN_ACTIVE_STYLE if active else self._INDICATOR_BTN_STYLE
        )

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
        if not self._is_realtime_session_open():
            self._emit_realtime_probe(connected=False, reason="market_closed")
            if self.realtime_timer is not None and self.realtime_timer.isActive():
                self.realtime_timer.stop()
            if self.realtime_pipeline_timer is not None and self.realtime_pipeline_timer.isActive():
                self.realtime_pipeline_timer.stop()
            self._stop_ws_quote_worker()
            self._add_watch_path()
            return
        if not self.realtime_timer.isActive():
            self._emit_realtime_probe(connected=None, reason="realtime_connecting")
            self.realtime_timer.start()
            self._remove_watch_path()
        if self.realtime_pipeline_timer is not None and not self.realtime_pipeline_timer.isActive():
            self.realtime_pipeline_timer.start()
        if self._use_ws_quote and self._is_realtime_session_open():
            self._restart_ws_quote_worker()
        else:
            self._stop_ws_quote_worker()

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
        port = int(os.environ.get("EASYXT_API_PORT", "8765"))
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
            if not self._safe_thread_wait(w, 2000):  # asyncio recv_timeout=1s + WS cleanup，给够 2s
                w.terminate()  # 超时则强制终止，避免 QThread Destroyed while running
                self._safe_thread_wait(w, 500)

    def _flush_realtime_pipeline(self):
        if self.chart is None and self.chart_adapter is None:
            return
        # Fix 54: flush() 内部有 _last_data.copy() 等重操作，推到后台线程
        if self._flush_in_progress:
            return
        if not self.realtime_pipeline._queue:
            # 快速短路：队列为空则无需 flush
            self._check_metrics_periodically()
            return
        self._flush_in_progress = True
        thread_manager.run(self._bg_flush_pipeline, name="bg_flush_pipeline")
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
        thread_manager.run(
            self._log_degrade_event_worker,
            args=(mode, interval_ms, symbol),
            name="log_degrade_event",
        )

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

        thread_manager.run(_send, name="post_monitor_payload")

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
        """已禁用：实盘图表严格禁止 UnifiedDataAPI/三方行情接入。"""
        self.realtime_api = None
        return

    def _on_realtime_ready(self, api: Any):
        self.realtime_api = None
        self._emit_realtime_probe(connected=False, reason="realtime_api_blocked")
        self._refresh_source_status()

    def _on_realtime_error(self, message: str):
        self._logger.warning(f"已禁止 realtime_api 接入: {message}")
        self._emit_realtime_probe(connected=False, reason="realtime_api_blocked")
        loaded = self._load_orderbook_snapshot_from_db(reason="realtime_api_error")
        if not loaded:
            self._set_orderbook_status(f"实时行情不可用: {message}", source="none")

    def _is_realtime_session_open(self) -> bool:
        if os.environ.get("EASYXT_ALLOW_OFFHOURS_REALTIME", "0") in ("1", "true", "True"):
            return True
        try:
            in_session, _ = TradingHoursGuard.current_session()
            return bool(in_session)
        except Exception:
            return False

    def _needs_adjusted_realtime_quote(self, period: str, adjust: str) -> bool:
        return str(adjust or "none") != "none" and period in self._DAILY_DISPLAY_PERIODS

    def _reset_realtime_adjust_anchor(self, symbol: str, period: str, adjust: str) -> None:
        self._realtime_adjust_key = (str(symbol or ""), str(period or ""), str(adjust or "none"))
        self._realtime_adjust_ratio = 1.0
        self._realtime_adjust_ready = not self._needs_adjusted_realtime_quote(period, adjust)

    def _schedule_realtime_adjust_anchor(
        self,
        symbol: str,
        period: str,
        adjust: str,
        chart_data: pd.DataFrame,
    ) -> None:
        key = (str(symbol or ""), str(period or ""), str(adjust or "none"))
        self._realtime_adjust_key = key
        if not self._needs_adjusted_realtime_quote(period, adjust):
            self._realtime_adjust_ratio = 1.0
            self._realtime_adjust_ready = True
            return
        if chart_data is None or chart_data.empty or "close" not in chart_data.columns:
            self._realtime_adjust_ratio = 1.0
            self._realtime_adjust_ready = False
            return
        try:
            displayed_close = float(chart_data["close"].iloc[-1])
        except Exception:
            displayed_close = 0.0
        if displayed_close <= 0:
            self._realtime_adjust_ratio = 1.0
            self._realtime_adjust_ready = False
            return
        start_date = str(chart_data["time"].iloc[0])[:10]
        end_date = str(chart_data["time"].iloc[-1])[:10]
        self._realtime_adjust_ratio = 1.0
        self._realtime_adjust_ready = False
        thread_manager.run(
            self._bg_compute_realtime_adjust_anchor,
            args=(key, symbol, period, start_date, end_date, displayed_close),
            name="bg_compute_realtime_adjust_anchor",
        )

    def _bg_compute_realtime_adjust_anchor(
        self,
        key: tuple[str, str, str],
        symbol: str,
        period: str,
        start_date: str,
        end_date: str,
        displayed_close: float,
    ) -> None:
        payload: dict[str, Any] = {
            "key": key,
            "ratio": 0.0,
            "raw_close": None,
            "error": "",
        }
        iface = None
        try:
            UnifiedDataInterface = importlib.import_module(
                "data_manager.unified_data_interface"
            ).UnifiedDataInterface
            iface = UnifiedDataInterface(duckdb_path=self.duckdb_path, silent_init=True)
            try:
                iface.connect(read_only=True)
            except Exception:
                pass
            raw_df = iface.get_stock_data_local(
                stock_code=symbol,
                start_date=start_date,
                end_date=end_date,
                period=period,
                adjust="none",
            )
            if raw_df is not None and not raw_df.empty and "close" in raw_df.columns:
                raw_close = float(pd.to_numeric(raw_df["close"], errors="coerce").dropna().iloc[-1])
                if raw_close > 0:
                    payload["raw_close"] = raw_close
                    payload["ratio"] = float(displayed_close) / float(raw_close)
        except Exception as exc:
            payload["error"] = str(exc)
        finally:
            try:
                if iface is not None:
                    iface.close()
            except Exception:
                pass
        self._realtime_adjust_anchor_signal.emit(payload)

    def _on_realtime_adjust_anchor_ready(self, payload: dict[str, Any]) -> None:
        key = payload.get("key") if isinstance(payload, dict) else None
        if key != self._realtime_adjust_key:
            return
        try:
            ratio = float(payload.get("ratio") or 0.0)
        except Exception:
            ratio = 0.0
        if ratio > 0:
            self._realtime_adjust_ratio = ratio
            self._realtime_adjust_ready = True
            return
        self._realtime_adjust_ratio = 1.0
        self._realtime_adjust_ready = False
        error_text = str(payload.get("error") or "") if isinstance(payload, dict) else ""
        if error_text:
            self._logger.warning("实时复权锚点计算失败: %s", error_text)

    def _apply_chart_adjustment_to_quote(
        self,
        quote: dict[str, Any],
        symbol: str,
        period: str,
    ) -> Optional[dict[str, Any]]:
        adjust = self._get_adjust_key()
        if not self._needs_adjusted_realtime_quote(period, adjust):
            return dict(quote)
        current_key = (str(symbol or ""), str(period or ""), str(adjust or "none"))
        if self._realtime_adjust_key != current_key:
            self._reset_realtime_adjust_anchor(symbol, period, adjust)
        if not self._realtime_adjust_ready or self._realtime_adjust_ratio <= 0:
            self._emit_realtime_probe(connected=True, reason="adjust_anchor_pending")
            return None
        ratio = float(self._realtime_adjust_ratio)
        adjusted = dict(quote)
        for field in (
            "price",
            "open",
            "high",
            "low",
            "prev_close",
            "last_settlement",
            "settlement",
        ):
            value = adjusted.get(field)
            if value in (None, ""):
                continue
            try:
                adjusted[field] = float(value) * ratio
            except Exception:
                continue
        return adjusted

    def _poll_realtime_quote(self):
        if (self.chart is None and self.chart_adapter is None) or self.interface is None:
            return
        if self.auto_update_check is None or not self.auto_update_check.isChecked():
            self._emit_realtime_probe(connected=None, reason="auto_update_disabled")
            return
        if not self._is_realtime_session_open():
            self._emit_realtime_probe(connected=False, reason="market_closed")
            self._stop_ws_quote_worker()
            return
        if self._use_ws_quote and self._ws_quote_worker is None:
            self._restart_ws_quote_worker()
        # WS 推送模式激活且链路在线时跳过轮询（避免双重采集）
        # 注意：_ws_quote_worker.isRunning() 在重连期间也为 True，因此用 _connected 事件判断
        if self._ws_quote_worker is not None and self._ws_quote_worker._connected.is_set():
            if (
                self._last_quote_monotonic > 0
                and (time.monotonic() - self._last_quote_monotonic) < 2.5
            ):
                return
        # If a quote request is already pending/running, skip this poll
        if self._is_thread_running(self._quote_worker):
            return

        symbol = self._normalize_symbol(self.symbol_input.text().strip())
        if not symbol:
            return

        # Fix 53: 使用本地 _normalize_symbol 代替 easy_xt.utils 的运行时 import
        normalized_symbol = symbol

        # Use worker to fetch quotes asynchronously
        self._quote_worker = _RealtimeQuoteWorker(None, normalized_symbol)
        self._quote_worker.quote_ready.connect(self._on_quote_received)
        self._quote_worker.error_occurred.connect(self._on_quote_error)
        self._quote_worker.start()

    def _on_quote_received(self, quote: dict, symbol: str):
        if not self._is_realtime_session_open():
            self._emit_realtime_probe(connected=False, reason="market_closed")
            self._stop_ws_quote_worker()
            return
        raw_quote = self._normalize_realtime_quote(quote)
        self._last_raw_realtime_quote = dict(raw_quote)
        self._ws_error_consecutive = 0
        self._last_quote_monotonic = time.monotonic()
        quote_ts = pd.Timestamp.now().strftime("%H:%M:%S")
        self._emit_realtime_probe(connected=True, reason="quote_ok", quote_ts=quote_ts)
        if self.chart is None and self.chart_adapter is None:
            return
        period = self.period_combo.currentText() if self.period_combo is not None else "1d"
        chart_quote = self._apply_chart_adjustment_to_quote(raw_quote, symbol, period)
        if chart_quote is None:
            self._update_orderbook(raw_quote)
            return
        # Fix 58: 只在 symbol/period 变化时才重新 configure，避免每秒在主线程做 tail().copy()
        cfg_key = (symbol, period)
        if not hasattr(self, "_last_configure_key") or self._last_configure_key != cfg_key:
            self.realtime_pipeline.configure(symbol=symbol, period=period, last_data=self.last_data)
            # HTAP: bar 完结时写回 DuckDB（仅 1m/5m/1d 基础周期）
            _udi = getattr(self, "data_interface", None)
            if _udi is not None:
                _sym, _p = symbol, period
                self.realtime_pipeline.on_bar_close = (
                    lambda bar, s=_sym, p=_p: _udi.upsert_realtime_bar(bar, s, p)
                )
            self._last_configure_key = cfg_key
        self.realtime_pipeline.enqueue_quote(chart_quote)
        # Fix-B: WS tick 缺少五档时，异步补齐（防抖 5s，避免频繁调用 xtdata.get_full_tick）
        if (
            self._xtdata_probe_enabled
            and raw_quote.get("ask1") in (None, 0, "", "--")
            and (time.monotonic() - self._last_enrich_orderbook_ts) > 5.0
        ):
            self._last_enrich_orderbook_ts = time.monotonic()
            _sym = symbol
            _q = dict(raw_quote)
            thread_manager.run(
                self._bg_enrich_orderbook, args=(_q, _sym), name="bg_enrich_orderbook"
            )
        # Qt 右侧面板：成交明细推送（每 tick 一次）
        _trades_panel = getattr(self, "_klc_trades_panel", None)
        if _trades_panel is not None:
            _trades_panel.add_tick(
                {
                    "price": raw_quote.get("price"),
                    "time": quote_ts,
                    "ask1": raw_quote.get("ask1"),
                    "bid1": raw_quote.get("bid1"),
                    "direction": raw_quote.get("direction") or "",
                }
            )
        if self.last_data is None or self.last_data.empty:
            # Fix 58: force-flush 也推到后台线程，与 _bg_flush_pipeline 保持一致
            if not self._flush_in_progress:
                self._flush_in_progress = True
                thread_manager.run(self._bg_flush_pipeline_force, name="bg_flush_pipeline_force")

    def _apply_pipeline_result(self, result: dict[str, Any]):
        action = result.get("action")
        bar = result.get("bar")
        data = result.get("data")
        quote = result.get("quote") or {}
        raw_quote = self._last_raw_realtime_quote or quote
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
                new_ts = self._to_datetime_safe(bar.get("time"))
                last_ts = self._to_datetime_safe(last_time)
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
            # 历史数据加载完成：从 DB 拉取盘口快照，确保收盘/离线时五档仍有数据
            self._load_orderbook_snapshot_from_db(reason="init")
        elif action == "update" and isinstance(bar, dict):
            current_period = (
                self.period_combo.currentText() if self.period_combo is not None else "1d"
            )
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
                        stock_code=self.symbol_input.text().strip()
                        if self.symbol_input is not None
                        else "",
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
        # Qt 右侧面板：关键数据推送（每次 pipeline flush 后更新）
        _stats_panel = getattr(self, "_klc_stats_panel", None)
        if _stats_panel is not None and self.last_data is not None and not self.last_data.empty:
            try:
                lb = self.last_data.iloc[-1]
                _close_raw = lb.get("close")
                close = (
                    float(_close_raw)
                    if _close_raw not in (None, "") and _close_raw == _close_raw
                    else 0
                )
                _prev_raw = (
                    self.last_data.iloc[-2].get("close") if len(self.last_data) >= 2 else None
                )
                prev_close = (
                    float(_prev_raw)
                    if _prev_raw not in (None, "") and _prev_raw == _prev_raw
                    else close
                )
                chg_pct = (close - prev_close) / prev_close * 100 if prev_close else 0.0

                def _safe(v):
                    """把 NaN/None 转为 None，让 update_stats 跳过该字段。"""
                    if v is None or v == "":
                        return None
                    try:
                        f = float(v)
                        return None if f != f else f  # f != f 当且仅当 f 是 NaN
                    except Exception:
                        return None

                _stats_panel.update_stats(
                    {
                        "open": _safe(lb.get("open")),
                        "high": _safe(lb.get("high")),
                        "low": _safe(lb.get("low")),
                        "close": close if close else None,
                        "chg_pct": round(chg_pct, 4),
                        "volume": _safe(lb.get("volume")),
                        "amount": _safe(lb.get("amount")),
                    }
                )
            except Exception:
                pass
        has_five_levels = raw_quote.get("ask1") not in (None, 0, "", "--") and raw_quote.get(
            "bid1"
        ) not in (None, 0, "", "--")
        if has_five_levels:
            quote_ts_now = pd.Timestamp.now().strftime("%H:%M:%S")
            self._set_orderbook_status(f"实时 {quote_ts_now}", source="live")
        self._update_orderbook(raw_quote)

    def _on_quote_error(self, symbol: str, reason: str):
        reason_text = reason or "quote_error"
        if str(reason_text).startswith("ws_conn_error"):
            self._ws_error_consecutive += 1
            if self._ws_error_consecutive < max(1, self._ws_error_emit_threshold):
                return
            # 达到阈值后：输出结构化诊断，方便区分「端口错误/路由不存在/服务未就绪」
            port = int(os.environ.get("EASYXT_API_PORT", "8765"))
            ws_url = f"ws://127.0.0.1:{port}/ws/market/{symbol}"
            self._logger.warning(
                f"WS行情握手持续失败 [{reason_text}] "
                f"port={port} url={ws_url} "
                f"consecutive={self._ws_error_consecutive} — "
                "InvalidStatus=路由不存在或服务返回非101; "
                "ConnectionRefused=服务未启动; 检查 EASYXT_API_PORT 与服务端路由注册"
            )
        self._emit_realtime_probe(connected=False, reason=reason_text)
        self._load_orderbook_snapshot_from_db(reason=reason_text)

    def _apply_realtime_quote(self, quote: dict, symbol: str):
        quote = self._normalize_realtime_quote(quote)
        price = float(quote.get("price") or 0)
        if price <= 0:
            return
        period = self.period_combo.currentText()
        timestamp_fields = (
            "source_event_ts_ms",
            "event_ts_ms",
            "tick_ts_ms",
            "trade_time",
            "tradeTime",
            "quote_time",
            "quoteTime",
            "update_time",
            "updateTime",
            "datetime",
            "time",
            "timestamp",
            "ts",
        )
        has_explicit_quote_ts = any(quote.get(name) not in (None, "") for name in timestamp_fields)
        quote_ts = self._resolve_quote_timestamp(quote)
        _p = str(period or "").strip()
        _is_intraday = (_p.lower().endswith("m") and _p[:-1].isdigit()) or _p.lower() == "tick"
        if _is_intraday and not self._is_intraday_market_time(quote_ts):
            return
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
        last_ts = self._to_datetime_safe(last_time)
        if _is_intraday and not has_explicit_quote_ts and pd.notna(last_ts):
            # 部分实时源不携带逐笔时间戳；此时沿用当前最后一根 bar 的时间，
            # 避免因本机时间或字符串/Timestamp 类型差异误开新 bar。
            bar_time = last_time
        else:
            bar_time = self._floor_bar_time(period, quote_ts)
        new_ts = self._to_datetime_safe(bar_time)
        if pd.notna(new_ts) and pd.notna(last_ts) and new_ts < last_ts:
            return
        same_bar = bool(pd.notna(new_ts) and pd.notna(last_ts) and new_ts == last_ts)
        if not same_bar:
            same_bar = last_time == bar_time
        if same_bar:
            is_daily = period == "1d"
            if is_daily:
                high = max(float(last_row["high"]), price, float(quote.get("high") or price))
                low = min(float(last_row["low"]), price, float(quote.get("low") or price))
            else:
                high = max(float(last_row["high"]), price)
                low = min(float(last_row["low"]), price)
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
            is_daily = period == "1d"
            if is_daily:
                open_price = float(quote.get("open") or price)
                high = max(price, float(quote.get("high") or price))
                low = min(price, float(quote.get("low") or price))
            else:
                open_price = price
                high = price
                low = price
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

    def _resolve_quote_timestamp(self, quote: dict[str, Any]) -> pd.Timestamp:
        fields = (
            "source_event_ts_ms",
            "event_ts_ms",
            "tick_ts_ms",
            "trade_time",
            "tradeTime",
            "quote_time",
            "quoteTime",
            "update_time",
            "updateTime",
            "datetime",
            "time",
            "timestamp",
            "ts",
        )
        for name in fields:
            v = quote.get(name)
            if v is None or v == "":
                continue
            ts = self._coerce_timestamp(v)
            if ts is not None:
                return ts
        return pd.Timestamp.now()

    def _coerce_timestamp(self, value: Any) -> Optional[pd.Timestamp]:
        try:
            if isinstance(value, (int, float)):
                num = float(value)
                if abs(num) > 1e14:
                    ts = pd.to_datetime(int(num), unit="us", errors="coerce")
                elif abs(num) > 1e11:
                    ts = pd.to_datetime(int(num), unit="ms", errors="coerce")
                elif abs(num) > 1e9:
                    ts = pd.to_datetime(int(num), unit="s", errors="coerce")
                else:
                    ts = pd.to_datetime(num, errors="coerce")
            else:
                ts = pd.to_datetime(value, errors="coerce")
            if pd.isna(ts):
                return None
            return (
                pd.Timestamp(ts).tz_localize(None)
                if getattr(ts, "tzinfo", None) is not None
                else pd.Timestamp(ts)
            )
        except Exception:
            return None

    def _is_intraday_market_time(self, ts: pd.Timestamp) -> bool:
        if ts.weekday() >= 5:
            return False
        t = ts.time()
        return (t >= pd.Timestamp("09:30:00").time() and t <= pd.Timestamp("11:30:00").time()) or (
            t >= pd.Timestamp("13:00:00").time() and t <= pd.Timestamp("15:00:00").time()
        )

    def _floor_bar_time(self, period: str, ts: pd.Timestamp):
        """\u5c06 tick 时间戳对齐到周期 bar 的右边界（与 QMT 历史K线时间戳对齐）。

        A股 K线时间戳采用右边界惯例（QMT/通达信合并规则）：
          - 1m 首根 time = 09:31（覆盖 09:30-09:31 含集合竞价）
          - 10m 首根 time = 09:40（覆盖 09:30-09:40）
        实时 bar_time 必须与历史 bar time 一致，否则 last_time==bar_time
        条件永远不成立，导致每次实时 tick 都创建新 bar 而不是更新当前 bar。
        使用会话对齐右边界：sess_start + (offset_bars+1) * n, capped at sess_end。
        """
        p = str(period or "").strip()
        pl = p.lower()
        # 日线+固定周期
        if p in ("1d",):
            return ts.strftime("%Y-%m-%d")
        if p in ("1w",):
            return (ts - pd.Timedelta(days=ts.weekday())).strftime("%Y-%m-%d")
        if p in ("1M", "2M", "3M", "5M", "6M", "1Q"):
            return ts.replace(day=1).strftime("%Y-%m-%d")
        if p in ("1Y", "2Y", "3Y", "5Y", "10Y"):
            return ts.replace(month=1, day=1).strftime("%Y-%m-%d")
        # 任意分钟周期 NNm —— 全日连续左对齐右边界（跨午间不截断）
        if pl.endswith("m") and pl[:-1].isdigit():
            n = int(pl[:-1])
            d = ts.strftime("%Y-%m-%d")
            am_start = pd.Timestamp(f"{d} 09:30:00")
            pm_start = pd.Timestamp(f"{d} 13:00:00")
            pm_end = pd.Timestamp(f"{d} 15:00:00")
            AM_MINUTES = 120  # 09:30-11:30
            TOTAL_MINUTES = 240  # 全日总交易分钟数
            # 计算全日连续交易分钟偏移（午间休市不计入，仅计实际交易时间）
            if ts >= pm_start:
                elapsed = AM_MINUTES + max(0, int((ts - pm_start).total_seconds()) // 60)
            else:
                elapsed = max(0, int((ts - am_start).total_seconds()) // 60)
            bar_num = elapsed // n
            right_min = min((bar_num + 1) * n, TOTAL_MINUTES)
            # 将交易分钟偏移转回真实时刻
            if right_min <= AM_MINUTES:
                right = am_start + pd.Timedelta(minutes=right_min)
            else:
                right = pm_start + pd.Timedelta(minutes=right_min - AM_MINUTES)
            right = min(right, pm_end)
            return right.strftime("%Y-%m-%d %H:%M:%S")
        # 多日周期 NNd
        if pl.endswith("d") and pl[:-1].isdigit():
            return ts.strftime("%Y-%m-%d")
        return ts

    def _normalize_realtime_quote(self, quote: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(quote, dict):
            return {}
        _data_field = quote.get("data")
        raw: dict[str, Any] = (
            cast(dict[str, Any], _data_field) if isinstance(_data_field, dict) else quote
        )
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
        normalized["open"] = float(
            raw.get("open") or raw.get("openPrice") or normalized["price"] or 0
        )
        normalized["high"] = float(
            raw.get("high") or raw.get("highPrice") or normalized["price"] or 0
        )
        normalized["low"] = float(raw.get("low") or raw.get("lowPrice") or normalized["price"] or 0)
        normalized["volume"] = float(raw.get("volume") or raw.get("vol") or 0)
        normalized["amount"] = float(raw.get("amount") or raw.get("turnover") or 0)
        ts_val = (
            raw.get("source_event_ts_ms")
            or raw.get("event_ts_ms")
            or raw.get("tick_ts_ms")
            or raw.get("timestamp")
            or raw.get("trade_time")
            or raw.get("time")
        )
        if ts_val not in (None, ""):
            normalized["timestamp"] = ts_val
        if raw.get("event_ts_ms") not in (None, ""):
            normalized["event_ts_ms"] = raw.get("event_ts_ms")
        if raw.get("source_event_ts_ms") not in (None, ""):
            normalized["source_event_ts_ms"] = raw.get("source_event_ts_ms")
        if raw.get("tick_ts_ms") not in (None, ""):
            normalized["tick_ts_ms"] = raw.get("tick_ts_ms")

        ask_prices = raw.get("askPrice") or raw.get("ask_price") or []
        bid_prices = raw.get("bidPrice") or raw.get("bid_price") or []
        ask_vols = raw.get("askVol") or raw.get("ask_vol") or raw.get("ask_volume") or []
        bid_vols = raw.get("bidVol") or raw.get("bid_vol") or raw.get("bid_volume") or []
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
                normalized[f"ask{level}"] = (
                    raw.get(f"ask{level}") or raw.get(f"sell{level}") or raw.get(f"a{level}_p")
                )
            if f"bid{level}" not in normalized:
                normalized[f"bid{level}"] = (
                    raw.get(f"bid{level}") or raw.get(f"buy{level}") or raw.get(f"b{level}_p")
                )
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
        if hasattr(self, "_tab_watchlist_widget") and self._tab_watchlist_widget is not None:
            self._tab_watchlist_widget.update_orderbook(dict(quote))
        if hasattr(self, "_tab_intraday_widget") and self._tab_intraday_widget is not None:
            self._tab_intraday_widget.update_quote(dict(quote))
        if hasattr(self, "orderbook_panel") and self.orderbook_panel is not None:
            self.orderbook_panel.update_orderbook(quote)
        if hasattr(self, "_kline_side_watchlist") and self._kline_side_watchlist is not None:
            self._kline_side_watchlist.update_orderbook(quote)
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

    def _set_orderbook_status(self, text: str, source: str = ""):
        if hasattr(self, "orderbook_panel") and self.orderbook_panel is not None:
            self.orderbook_panel.set_status(text, source)
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
        """实盘图表只展示 QMT 状态，不再展示三方源混合状态。"""
        label = getattr(self, "source_status_label", None)
        if label is None:
            return
        try:
            import easy_xt

            if easy_xt.api is not None:
                label.setText("源: QMT")
                label.setStyleSheet("color:#5cb85c;")
                return
        except Exception:
            pass
        label.setText("源: 未连接")
        label.setStyleSheet("color:#999;")

    def _bg_refresh_source_status(self, api):
        """后台线程: 获取数据源状态"""
        try:
            status = api.get_source_status()
        except Exception:
            status = None
        self.source_status_ready.emit(status)

    def _apply_source_status(self, status):
        """兼容保留：统一回退到 QMT 状态展示。"""
        self._source_status_refreshing = False
        self._refresh_source_status()
        return

    def _load_orderbook_snapshot_from_db(self, reason: str = "") -> bool:
        """Fix 52: DuckDB 盘口查询推到后台线程，避免阻塞主线程"""
        if self.orderbook_label is None and self.orderbook_panel is None:
            return False
        symbol = self._normalize_symbol(self.symbol_input.text().strip())
        if not symbol:
            return False
        thread_manager.run(self._bg_load_orderbook, args=(symbol, reason), name="bg_load_orderbook")
        return True  # 异步发起，返回 True 表示已尝试

    def _bg_load_orderbook(self, symbol: str, reason: str):
        """后台线程: 执行 DuckDB 盘口查询"""
        try:
            if self._orderbook_sink is None:
                self._orderbook_sink = RealtimeDuckDBSink(duckdb_path=self.duckdb_path)
            snapshot = self._orderbook_sink.query_latest_orderbook(symbol)
            if snapshot:
                suffix = f" ({reason})" if reason else ""
                QTimer.singleShot(
                    0, lambda s=snapshot, sf=suffix: self._apply_orderbook_snapshot(s, sf)
                )
        except Exception:
            pass

    def _apply_orderbook_snapshot(self, snapshot: dict, suffix: str):
        """主线程: 应用盘口快照到 UI"""
        self._update_orderbook(snapshot)
        self._set_orderbook_status(f"五档盘口[历史快照]{suffix}", source="db")

    def _bg_enrich_orderbook(self, quote: dict, symbol: str):
        """Fix-B: xtquant C扩展不能从 threading.Thread 调用，此方法已禁用为 no-op。
        五档补齐由 EASYXT_ENABLE_XTDATA_QUOTE_PROBE=1 时的 QThread._fetch_quote_from_xtdata 负责。"""
        return  # 禁止从 threading.Thread 调用 xtdata（会导致 BSON 断言崩溃）

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
            _re.match(r"^\d+m$", period)
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
    _DAILY_DISPLAY_PERIODS = frozenset(
        {
            "1d",
            "1w",
            "1M",
            "2d",
            "3d",
            "5d",
            "10d",
            "25d",
            "50d",
            "75d",
            "2M",
            "3M",
            "5M",
        }
    )

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
        _m = _re.match(r"^(\d+)m$", period)
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
        _m = _re.match(r"^(\d+)m$", period)
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
        elif bool(_re.match(r"^\d+m$", period)):
            # 其他自定义分钟周期 (2m,10m,20m,25m,50m,70m,120m,125m等)：按分钟数选合适窗口
            try:
                mins = int(_re.match(r"^(\d+)m$", period).group(1))  # type: ignore[union-attr]
            except Exception:
                mins = 30
            if mins <= 5:
                span = pd.DateOffset(days=2)
            elif mins <= 30:
                span = pd.DateOffset(days=5)
            else:
                span = pd.DateOffset(days=10)
        elif period == "1d":
            # 1D 日线：直接加载上市首日起的全量历史（约 5000-8000 根 K 线，DuckDB 秒级返回）
            # 不再截断为最近3个月——保证左对齐黄金标准从首日起完整显示
            start_ts = pd.Timestamp(full_start)
            return self._format_time_str(start_ts, period), self._format_time_str(end_ts, period)
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
        if getattr(self, "_is_closed", False):
            return
        if not symbol or (self.chart is None and self.chart_adapter is None):
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
        if (
            (self.chart is None and self.chart_adapter is None)
            or self.last_data is None
            or self.last_data.empty
        ):
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
        if getattr(self, "_is_closed", False):
            return
        if self.chart is None and self.chart_adapter is None:
            return
        symbol = self.symbol_input.text().strip()
        period = self.period_combo.currentText()
        adjust = self._get_adjust_key()
        normalized_symbol = self._normalize_symbol(symbol)
        if normalized_symbol and normalized_symbol != symbol:
            self.symbol_input.setText(normalized_symbol)
            symbol = normalized_symbol
        self._save_persisted_state()
        try:
            self._safe_stop_thread(self._chart_load_thread)
        except Exception:
            pass
        self._reset_realtime_adjust_anchor(symbol, period, adjust)
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
            adjust = payload.get("adjust") if isinstance(payload, dict) else self._get_adjust_key()
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
            if df is None or df.empty or (self.chart is None and self.chart_adapter is None):
                if key and key in self._loading_segments:
                    self._loading_segments.discard(key)
                strict_gate = self._strict_history_gate_enabled()
                if (
                    (not strict_gate)
                    and self.chart is not None
                    and symbol_str
                    and period_str in ("1m", "5m", "tick")
                ):
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
                            self._set_orderbook_status(
                                f"{symbol_str} 无{period_str}历史，已自动切换到1d"
                            )
                            self.refresh_chart_data()
                            return
                if (self.chart is not None or self.chart_adapter is not None) and symbol_str:
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
                    if strict_gate and not (backfill_scheduled or backfill_pending or same_pending):
                        empty_reason = (
                            payload.get("empty_reason") if isinstance(payload, dict) else ""
                        )
                        if empty_reason:
                            self._set_orderbook_status(
                                f"严格门禁: {symbol_str} 未通过真实历史源门禁（{empty_reason}）"
                            )
                        else:
                            self._set_orderbook_status(
                                f"严格门禁: {symbol_str} {period_str} 无真实历史数据，已禁止自动切股/改周期/合成K线"
                            )
                        return
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
                if not strict_gate:
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
                self._safe_stop_thread(self._data_process_thread)
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
            self._backfill_retry_remaining = int(os.environ.get("EASYXT_BACKFILL_RETRY_MAX", "6"))
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
        if (
            current_symbol != symbol
            or current_period != period
            or str(current_adjust) != str(adjust)
        ):
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
                base_copy = (
                    self.last_data.copy()
                    if (self.last_data is not None and not self.last_data.empty)
                    else pd.DataFrame()
                )
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
            if merged is None or merged.empty:
                return
            if self.chart is None and self.chart_adapter is None:
                return
            if self._full_range is None:
                # 仅在尚未从 DuckDB 获取完整范围时才用已加载数据替代
                # mode=="replace" 时不覆盖：refresh_chart_data 已从 DuckDB 查询并设置正确的全量范围
                self._full_range = (
                    str(merged["time"].iloc[0]),
                    str(merged["time"].iloc[-1]),
                )
            # ㊸修复：跳过与上次完全相同形状+末行的 chart.set 调用
            _shape = (
                len(merged),
                symbol,
                period,
                str(merged["time"].iloc[-1]),
                float(merged["close"].iloc[-1]),
            )
            if self.chart is None and self.chart_adapter is None:
                return
            if _shape == self._last_chart_set_shape and mode != "merge":
                self._logger.debug("_apply_data_processed: skipped duplicate chart.set")
            else:
                self._last_chart_set_shape = _shape
                if self.chart_adapter:
                    self.chart_adapter.set_data(merged)
                elif self.chart is not None:
                    self.chart.set(merged)
                self._request_subchart_update(merged, full_set=True)
            self.last_data = merged
            self.last_bar_time = merged["time"].iloc[-1]
            self.last_close = float(merged["close"].iloc[-1])
            self._schedule_realtime_adjust_anchor(symbol, period, self._get_adjust_key(), merged)
            self.realtime_pipeline.configure(symbol=symbol, period=period, last_data=self.last_data)
            # HTAP: 切换品种/周期后重新注入 on_bar_close
            _udi2 = getattr(self, "data_interface", None)
            if _udi2 is not None:
                _sym2, _p2 = symbol, period
                self.realtime_pipeline.on_bar_close = (
                    lambda bar, s=_sym2, p=_p2: _udi2.upsert_realtime_bar(bar, s, p)
                )
            self._last_configure_key = (symbol, period)
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
                    QTimer.singleShot(
                        0, lambda s=snapshot: self._apply_orderbook_bg(s, "historical_replay")
                    )
        except Exception:
            pass

    def _apply_signal_ui(self, signal: dict, signal_key: str):
        """主线程：应用信号标记到图表"""
        if self.last_signal_key == signal_key:
            return
        self.last_signal_key = signal_key
        if self.chart_adapter:
            self.chart_adapter.marker(signal["label"])
        elif self.chart is not None:
            self.chart.marker(text=signal["label"])
        if (
            hasattr(self, "auto_trade_check")
            and self.auto_trade_check
            and self.auto_trade_check.isChecked()
        ):
            self._execute_trade_signal(signal)

    def _apply_orderbook_bg(self, snapshot: dict, reason: str):
        """主线程：应用后台查询到的盘口快照"""
        self._update_orderbook(snapshot)
        suffix = f" ({reason})" if reason else ""
        self._set_orderbook_status(f"五档盘口[历史快照]{suffix}", source="db")

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
        if getattr(self, "_is_closed", False):
            return
        if not symbol or (self.chart is None and self.chart_adapter is None):
            return

        if self._full_range is None:
            return

        full_start, full_end = self._full_range
        initial_start, initial_end = self._compute_initial_range(self._full_range, period)

        # 按周期限制全量加载范围，避免请求多年 1m 数据导致超长等待
        # 1d / 多日自定义周期（2d/3d/5d…）：无上限，必须加载上市首日以来完整数据
        _INTRADAY_CAPS = {
            "1m": pd.DateOffset(days=30),
            "5m": pd.DateOffset(days=60),
            "15m": pd.DateOffset(months=6),
            "30m": pd.DateOffset(months=6),
            "60m": pd.DateOffset(years=2),
        }
        max_span = _INTRADAY_CAPS.get(period)  # None → 无上限（1d / 大周期）
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
        if self._strict_history_gate_enabled():
            return
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
        if self._strict_history_gate_enabled():
            return False
        if not symbol or (self.chart is None and self.chart_adapter is None):
            return False
        if not self._is_realtime_session_open():
            return False
        worker = _RealtimeQuoteWorker(None, symbol)
        quote = worker._fetch_quote_from_xtdata(symbol) or worker._fetch_quote_from_easyxt(symbol)
        if not quote:
            return False
        bar = self._build_bar_from_quote(quote, period)
        if bar is None:
            return False
        fallback_data = pd.DataFrame([bar])
        if self.chart_adapter:
            self.chart_adapter.set_data(fallback_data)
        elif self.chart is not None:
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
        quote_ts = self._resolve_quote_timestamp(quote)
        if period in ("1m", "5m", "15m", "30m", "60m") and not self._is_intraday_market_time(
            quote_ts
        ):
            return None
        floored = self._floor_bar_time(period, quote_ts)
        if isinstance(floored, pd.Timestamp):
            bar_time = floored.strftime("%Y-%m-%d %H:%M:%S")
        else:
            bar_time = str(floored)
        open_price = float(quote.get("open") or price)
        is_daily = period in ("1d", "1w", "1M")
        if is_daily:
            high = max(price, float(quote.get("high") or price))
            low = min(price, float(quote.get("low") or price))
        else:
            open_price = price
            high = price
            low = price
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

    def _on_timezone_changed(self, index: int):
        if self.timezone_combo is None:
            return
        timezone = self.timezone_combo.itemData(index)
        if not timezone:
            return
        adapter = self.chart_adapter
        if adapter is None:
            return
        try:
            set_timezone = getattr(adapter, "set_timezone", None)
            if callable(set_timezone):
                set_timezone(str(timezone))
        except Exception:
            self._logger.exception("Failed to apply chart timezone")

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
            # 同步响应式按钮条选中状态
            self._period_strip.set_current(saved_period)
        if saved_adjust:
            # 兼容旧的英文 key 或新的中文显示
            display = str(self._adjust_key_to_display.get(saved_adjust, saved_adjust) or "")
            index = self.adjust_combo.findText(display)
            if index >= 0:
                self.adjust_combo.setCurrentIndex(index)
        saved_timezone = settings.value("timezone", "Asia/Shanghai", type=str)
        if saved_timezone:
            for i in range(self.timezone_combo.count()):
                if self.timezone_combo.itemData(i) == saved_timezone:
                    self.timezone_combo.setCurrentIndex(i)
                    break

    def _get_adjust_key(self) -> str:
        """从复权 ComboBox 当前文本获取内部 key"""
        display = self.adjust_combo.currentText()
        return self._adjust_display_to_key.get(display, "none")

    def _save_persisted_state(self):
        settings = QSettings("EasyXT", "KLineChartWorkspace")
        settings.setValue("symbol", self.symbol_input.text().strip())
        settings.setValue("period", self.period_combo.currentText())
        settings.setValue("adjust", self._get_adjust_key())
        if self.timezone_combo is not None:
            settings.setValue("timezone", self.timezone_combo.currentData())

    def refresh_latest_bar(self):
        if self.chart is None and self.chart_adapter is None:
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
            if self.chart is None and self.chart_adapter is None:
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
            prev_data = (
                self.last_data.tail(30).copy()
                if (self.last_data is not None and not self.last_data.empty)
                else pd.DataFrame()
            )
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
            QTimer.singleShot(
                0,
                lambda r=last_row_dict,
                s=signal,
                sym=symbol,
                per=period: self._apply_latest_bar_from_bg(r, s, sym, per),
            )
        except Exception:
            pass

    def _apply_latest_bar_from_bg(self, last_row_dict, signal, symbol, period):
        """主线程: chart.update(单行) + 就地合并 last_data + 子图更新"""
        if self.chart is None and self.chart_adapter is None:
            return
        if self.last_data is not None and not self.last_data.empty:
            prev_ts = self._to_datetime_safe(self.last_data.iloc[-1].get("time"))
            curr_ts = self._to_datetime_safe(last_row_dict.get("time"))
            if pd.notna(prev_ts) and pd.notna(curr_ts) and curr_ts < prev_ts:
                return
        last_row = pd.Series(last_row_dict)
        if self.chart_adapter:
            self.chart_adapter.update_data(last_row)
        elif self.chart is not None:
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
                if self.chart_adapter:
                    self.chart_adapter.marker(signal["label"])
                elif self.chart is not None:
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
        if self.chart_adapter is not None:
            # Adapter 路径：通过 RPC 发送配色方案（若实现）
            try:
                apply_theme = getattr(self.chart_adapter, "apply_theme", None)
                if callable(apply_theme):
                    apply_theme(theme)
            except Exception:
                self._logger.exception("Failed to apply adapter chart theme")
            return
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

    def hideEvent(self, a0):
        super().hideEvent(a0)
        if self.realtime_timer and self.realtime_timer.isActive():
            self.realtime_timer.stop()

    def closeEvent(self, a0):
        self._is_closed = True  # guard all timer callbacks from xtdata calls
        try:
            self._drain_owned_threads()
            self._backfill_retry_timer.stop()
            if self._chart_refresh_timer and self._chart_refresh_timer.isActive():
                self._chart_refresh_timer.stop()
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
            if rct is not None and self._is_thread_running(rct):
                rct.requestInterruption()
                rct.quit()
                if not self._safe_thread_wait(rct, 1000):  # 给实盘连接线程 1s 宽限
                    self._logger.warning(
                        "closeEvent: _RealtimeConnectThread 未在 1s 内退出，强制终止"
                    )
                    rct.terminate()
                    self._safe_thread_wait(rct, 500)
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
            # Fix 7c/10a: 所有线程共享 3s 总 deadline，避免串行阻塞主线程（原来每线程 3s 可达 24s）
            import time as _time

            _deadline_t0 = _time.monotonic()
            _TOTAL_WAIT_MS = 3000
            for t in threads:
                if self._is_thread_running(t):
                    _elapsed_ms = int((_time.monotonic() - _deadline_t0) * 1000)
                    _remaining = max(50, _TOTAL_WAIT_MS - _elapsed_ms)
                    self._safe_thread_wait(t, _remaining)
            for t in threads:
                if self._is_thread_running(t):
                    self._logger.warning(
                        "closeEvent: 线程未及时退出，强制终止: %s", type(t).__name__
                    )
                    t.terminate()
                    self._safe_thread_wait(t, 300)
            self._drain_owned_threads()
        finally:
            # native 路径：关闭时保存当前标的的画线
            if self.toolbox_panel is not None:
                try:
                    self.toolbox_panel.save_current()
                except Exception:
                    pass
            # Fix 7/7c: 显式 CHECKPOINT 再关闭持久连接，将所有已提交 WAL 刷到主文件，
            # 避免下次启动出现 WAL回放异常警告。
            if self.interface is not None:
                try:
                    if getattr(self.interface, "con", None) is not None:
                        try:
                            self.interface.con.execute("CHECKPOINT")
                        except Exception:
                            pass
                except Exception:
                    pass
                try:
                    self.interface.close()
                except Exception:
                    pass
            super().closeEvent(a0)


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
            online_fetch_enabled = os.environ.get("EASYXT_CHART_ALLOW_ONLINE_FETCH", "1") in (
                "1",
                "true",
                "True",
            )
            if os.environ.get("EASYXT_XTDATA_TRACE", "0") in ("1", "true", "True"):
                print(
                    f"[XTTRACE][{threading.current_thread().name}] chart-load-thread start "
                    f"symbol={self.symbol} period={self.period} range={self.start_date}~{self.end_date}",
                    flush=True,
                )
            UnifiedDataInterface = importlib.import_module(
                "data_manager.unified_data_interface"
            ).UnifiedDataInterface
            iface = UnifiedDataInterface(
                duckdb_path=self.duckdb_path,
                silent_init=True,
                xtdata_call_mode="direct",
            )
            try:
                if os.environ.get("EASYXT_XTDATA_TRACE", "0") in ("1", "true", "True"):
                    print(
                        f"[XTTRACE][{threading.current_thread().name}] chart-load-thread iface.connect",
                        flush=True,
                    )
                iface.connect(
                    read_only=os.environ.get("EASYXT_CHART_ALLOW_ONLINE_FETCH", "0")
                    not in ("1", "true", "True")
                )
            except Exception:
                pass
            data = None
            empty_reason = ""
            backfill_scheduled = False
            backfill_pending = False
            ingestion_status = ""
            fetch_timeout_s = float(os.environ.get("EASYXT_CHART_FETCH_TIMEOUT_S", "12"))

            # 本地优先预览：在在线补数开始前先快速加载本地 DuckDB 数据，
            # 确保图表首屏立即有内容，不因 QMT 在线请求耗时而空白等待。
            # 仅用于 replace 模式（首屏/换股/换周期），merge（进度滚动）跳过。
            _local_preview_emitted = False
            _local_preview_data = None
            if online_fetch_enabled and self.mode == "replace":
                try:
                    _preview = iface.get_stock_data_local(
                        stock_code=self.symbol,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        period=self.period,
                        adjust=self.adjust,
                    )
                    if (
                        _preview is not None
                        and not getattr(_preview, "empty", True)
                        and len(_preview) >= 5
                        and not self.isInterruptionRequested()
                    ):
                        _local_preview_data = _preview
                        self.data_ready.emit(
                            {
                                "data": _preview,
                                "symbol": self.symbol,
                                "period": self.period,
                                "adjust": self.adjust,
                                "start_date": self.start_date,
                                "end_date": self.end_date,
                                "mode": self.mode,
                                "ingestion_status": "local_preview",
                                "backfill_scheduled": False,
                                "backfill_pending": False,
                                "empty_reason": "",
                            }
                        )
                        _local_preview_emitted = True
                except Exception:
                    pass

            try:
                if online_fetch_enabled:
                    # 显式开启时，允许走完整统一入口（DuckDB → QMT → 第三方）。
                    if os.environ.get("EASYXT_XTDATA_TRACE", "0") in ("1", "true", "True"):
                        print(
                            f"[XTTRACE][{threading.current_thread().name}] chart-load-thread iface.get_stock_data",
                            flush=True,
                        )
                    data = iface.get_stock_data(
                        stock_code=self.symbol,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        period=self.period,
                        adjust=self.adjust,
                        auto_save=True,
                    )
                else:
                    # 手动关闭在线补数时走本地只读路径
                    if os.environ.get("EASYXT_XTDATA_TRACE", "0") in ("1", "true", "True"):
                        print(
                            f"[XTTRACE][{threading.current_thread().name}] chart-load-thread iface.get_stock_data_local",
                            flush=True,
                        )
                    data = iface.get_stock_data_local(
                        stock_code=self.symbol,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        period=self.period,
                        adjust=self.adjust,
                    )
                    if data is None or getattr(data, "empty", True):
                        empty_reason = "本地无历史数据（默认禁用图表在线补数）"
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
            if (data is None or getattr(data, "empty", True)) and _local_preview_data is not None:
                data = _local_preview_data
                if not ingestion_status:
                    ingestion_status = "local_preview_fallback"
                empty_reason = ""
            if data is None or getattr(data, "empty", True):
                data = self._load_parquet_local()
            if data is None or getattr(data, "empty", True):
                # 完整级联 (DuckDB→QMT→AKShare) 均无数据，记录原因
                con = getattr(iface, "con", None)
                if con is None:
                    empty_reason = "DuckDB连接不可用"
                else:
                    table_period = {
                        "15m": "1m",
                        "30m": "1m",
                        "60m": "1m",
                        "1w": "1d",
                        "1M": "1d",
                    }.get(self.period, self.period)
                    stored_period = self.period
                    table_name, date_col = PERIOD_DATE_COL_MAP.get(
                        table_period, ("stock_daily", "date")
                    )
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
                iface.connect(read_only=True)
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
        elif data.index is not None and "time" not in data.columns:
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
        _daily_display = frozenset(
            {
                "1d",
                "1w",
                "1M",
                "2d",
                "3d",
                "5d",
                "10d",
                "25d",
                "50d",
                "75d",
                "2M",
                "3M",
                "5M",
            }
        )
        if period in _daily_display:
            # 整数时间戳保护：若 time 列为数值型（QMT epoch ms），先换算为 datetime
            if pd.api.types.is_numeric_dtype(data["time"]):
                from data_manager.timestamp_utils import qmt_ms_to_beijing

                dt_series = qmt_ms_to_beijing(data["time"])
            else:
                dt_series = pd.to_datetime(data["time"], errors="coerce")
            data = data[dt_series.notna()].copy()
            dt_series = dt_series[dt_series.notna()]
            data["time"] = dt_series.map(lambda x: x.strftime("%Y-%m-%d"))
        else:
            # 整数时间戳保护：若 time 列为数值型（QMT epoch ms），先换算为 datetime
            if pd.api.types.is_numeric_dtype(data["time"]):
                from data_manager.timestamp_utils import qmt_ms_to_beijing

                dt_series = qmt_ms_to_beijing(data["time"])
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
