#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import importlib
from typing import Optional, Dict, Any

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTabWidget, QSplitter, QLabel, QFrame,
    QLineEdit, QComboBox, QPushButton, QDateEdit, QCheckBox, QSizePolicy
)
from PyQt5.QtCore import Qt, QTimer, QDate, QFileSystemWatcher

import pandas as pd
import numpy as np

class KLineChartWorkspace(QWidget):
    def __init__(self):
        super().__init__()
        self.chart = None
        self.trading_window: Optional[QWidget] = None
        self.trading_panel: Optional[QWidget] = None
        self.interface = None
        self.duckdb_path = r"D:/StockData/stock_data.ddb"
        self.last_bar_time = None
        self.last_close = None
        self.last_data = pd.DataFrame()
        self.last_signal_key = None
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
        self.init_ui()

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

    def _create_left_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)

        BacktestWidget = self._dynamic_class("gui_app.widgets.backtest_widget", "BacktestWidget")
        JQ2QMTWidget = self._dynamic_class("gui_app.widgets.jq2qmt_widget", "JQ2QMTWidget")
        JQToPtradeWidget = self._dynamic_class("gui_app.widgets.jq_to_ptrade_widget", "JQToPtradeWidget")
        GridTradingWidget = self._dynamic_class("gui_app.widgets.grid_trading_widget", "GridTradingWidget")
        ConditionalOrderWidget = self._dynamic_class("gui_app.widgets.conditional_order_widget", "ConditionalOrderWidget")

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

        BacktestWidget = self._dynamic_class("gui_app.widgets.backtest_widget", "BacktestWidget")
        JQ2QMTWidget = self._dynamic_class("gui_app.widgets.jq2qmt_widget", "JQ2QMTWidget")
        JQToPtradeWidget = self._dynamic_class("gui_app.widgets.jq_to_ptrade_widget", "JQToPtradeWidget")
        GridTradingWidget = self._dynamic_class("gui_app.widgets.grid_trading_widget", "GridTradingWidget")
        ConditionalOrderWidget = self._dynamic_class("gui_app.widgets.conditional_order_widget", "ConditionalOrderWidget")
        LocalDataManagerWidget = self._dynamic_class("gui_app.widgets.local_data_manager_widget", "LocalDataManagerWidget")
        AdvancedDataViewerWidget = self._dynamic_class("gui_app.widgets.advanced_data_viewer_widget", "AdvancedDataViewerWidget")

        tabs.addTab(BacktestWidget(), "回测分析")
        tabs.addTab(GridTradingWidget(), "网格交易")
        tabs.addTab(ConditionalOrderWidget(), "条件单")
        tabs.addTab(JQ2QMTWidget(), "JQ2QMT")
        tabs.addTab(JQToPtradeWidget(), "JQ转Ptrade")
        tabs.addTab(LocalDataManagerWidget(), "数据管理")
        tabs.addTab(AdvancedDataViewerWidget(), "数据查看")
        tabs.addTab(self._create_trading_panel(), "交易面板")
        self._relax_tab_constraints(tabs)
        tabs.currentChanged.connect(self._relax_current_tab_constraints)
        return tabs

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

    def _relax_widget_constraints(self, widget: QWidget):
        widget.setMinimumSize(0, 0)
        widget.setMaximumHeight(16777215)
        widget.setMaximumWidth(16777215)
        for child in widget.findChildren(QWidget):
            child.setMinimumSize(0, 0)
            child.setMaximumHeight(16777215)
            child.setMaximumWidth(16777215)

    def _create_chart_controls(self) -> QWidget:
        panel = QWidget()
        layout = QHBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self.symbol_input = QLineEdit("000001.SZ")
        self.period_combo = QComboBox()
        self.period_combo.addItems(["1d", "1m", "5m", "tick"])
        self.adjust_combo = QComboBox()
        self.adjust_combo.addItems([
            "none", "front", "back", "geometric_front", "geometric_back"
        ])

        self.start_date_edit = QDateEdit(QDate.currentDate().addYears(-1))
        self.start_date_edit.setCalendarPopup(True)
        self.end_date_edit = QDateEdit(QDate.currentDate())
        self.end_date_edit.setCalendarPopup(True)

        self.refresh_button = QPushButton("加载")
        self.refresh_button.clicked.connect(self.refresh_chart_data)
        self.auto_update_check = QCheckBox("实时更新")
        self.auto_update_check.stateChanged.connect(self._toggle_auto_update)
        self.auto_trade_check = QCheckBox("自动交易")
        self.toggle_bottom_button = QPushButton("操作面板")
        self.toggle_bottom_button.clicked.connect(self._toggle_bottom_panel)

        layout.addWidget(QLabel("标的"))
        layout.addWidget(self.symbol_input)
        layout.addWidget(QLabel("周期"))
        layout.addWidget(self.period_combo)
        layout.addWidget(QLabel("复权"))
        layout.addWidget(self.adjust_combo)
        layout.addWidget(QLabel("开始"))
        layout.addWidget(self.start_date_edit)
        layout.addWidget(QLabel("结束"))
        layout.addWidget(self.end_date_edit)
        layout.addWidget(self.refresh_button)
        layout.addWidget(self.auto_update_check)
        layout.addWidget(self.auto_trade_check)
        layout.addWidget(self.toggle_bottom_button)
        layout.addStretch(1)
        return panel

    def showEvent(self, event):
        super().showEvent(event)
        self._apply_initial_split()
        for delay in (0, 50, 150, 300, 600):
            QTimer.singleShot(delay, self._enforce_split_limits)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._enforce_split_limits()

    def _apply_initial_split(self):
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
        if not self.root_splitter:
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
        if not self.root_splitter or not self.bottom_tabs:
            return
        sizes = self.root_splitter.sizes()
        if len(sizes) < 2:
            return
        total = max(sum(sizes), 1)
        min_top = int(total * self.min_top_ratio)
        max_bottom = min(int(total * self.max_bottom_ratio), max(total - min_top, 0))
        if sizes[1] <= 5:
            target_size = int(total * self.last_bottom_ratio) if self.last_bottom_ratio > 0 else max_bottom
            restore_size = self.last_nonzero_bottom_size if self.last_nonzero_bottom_size > 0 else target_size
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
        try:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
            external_lwc_path = os.path.join(project_root, 'external', 'lightweight-charts-python')
            if external_lwc_path not in sys.path:
                sys.path.insert(0, external_lwc_path)

            from PyQt5.QtWebEngineWidgets import QWebEngineView
            _ = QWebEngineView
            QtChart = self._dynamic_class("lightweight_charts.widgets", "QtChart")
            self.chart = QtChart(parent, toolbox=True)
            webview = self.chart.get_webview()
            webview.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            webview.setMinimumHeight(0)
            webview.setMinimumWidth(0)
            container = QWidget()
            container_layout = QVBoxLayout(container)
            container_layout.setContentsMargins(0, 0, 0, 0)
            container_layout.setSpacing(0)
            container_layout.addWidget(webview)
            self._load_default_chart_data()
            return container
        except Exception as exc:
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

        dates = pd.date_range(end=pd.Timestamp.today(), periods=200, freq='D')
        prices = np.cumsum(np.random.normal(0, 1, len(dates))) + 100
        open_price = prices + np.random.normal(0, 0.5, len(dates))
        close_price = prices + np.random.normal(0, 0.5, len(dates))
        high_price = np.maximum(open_price, close_price) + np.random.uniform(0.2, 1.2, len(dates))
        low_price = np.minimum(open_price, close_price) - np.random.uniform(0.2, 1.2, len(dates))
        volume = np.random.randint(10000, 50000, len(dates))

        df = pd.DataFrame({
            'time': dates.strftime('%Y-%m-%d'),
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume
        })
        self.chart.set(df)

    def _create_right_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)

        LocalDataManagerWidget = self._dynamic_class("gui_app.widgets.local_data_manager_widget", "LocalDataManagerWidget")
        AdvancedDataViewerWidget = self._dynamic_class("gui_app.widgets.advanced_data_viewer_widget", "AdvancedDataViewerWidget")

        tabs = QTabWidget()
        tabs.addTab(LocalDataManagerWidget(), "数据管理")
        tabs.addTab(AdvancedDataViewerWidget(), "数据查看")
        tabs.addTab(self._create_trading_panel(), "交易面板")

        layout.addWidget(tabs)
        return panel

    def _create_trading_panel(self) -> QWidget:
        TradingInterface = self._dynamic_class("gui_app.trading_interface_simple", "TradingInterface")
        self.trading_window = TradingInterface()
        panel = self.trading_window.centralWidget()
        if panel is None:
            panel = QWidget()
        self.trading_window.setCentralWidget(QWidget())
        panel.setParent(self)
        self.trading_panel = panel
        return panel

    def _toggle_auto_update(self, state: int):
        if state == Qt.CheckState.Checked:
            self.refresh_chart_data()
            self._add_watch_path()
        else:
            self._remove_watch_path()

    def _add_watch_path(self):
        if os.path.exists(self.duckdb_path):
            if self.duckdb_path not in self.file_watcher.files():
                self.file_watcher.addPath(self.duckdb_path)

    def _remove_watch_path(self):
        if self.duckdb_path in self.file_watcher.files():
            self.file_watcher.removePath(self.duckdb_path)

    def _on_duckdb_changed(self, path: str):
        if os.path.exists(path):
            self._add_watch_path()
        if not self.update_timer.isActive():
            self.update_timer.start()

    def _dynamic_class(self, module_path: str, class_name: str):
        module = importlib.import_module(module_path)
        return getattr(module, class_name)

    def _ensure_interface(self):
        if self.interface is None:
            UnifiedDataInterface = self._dynamic_class("data_manager.unified_data_interface", "UnifiedDataInterface")
            self.interface = UnifiedDataInterface(duckdb_path=self.duckdb_path)
            self.interface.connect()
            return
        if getattr(self.interface, "con", None) is None:
            self.interface.connect()

    def _get_chart_dates(self):
        start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
        end_date = self.end_date_edit.date().toString("yyyy-MM-dd")
        return start_date, end_date

    def _format_time_column(self, data: pd.DataFrame, period: str) -> pd.DataFrame:
        if "time" not in data.columns:
            return data
        if period == "1d":
            data["time"] = pd.to_datetime(data["time"]).dt.strftime("%Y-%m-%d")
        else:
            data["time"] = pd.to_datetime(data["time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
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

        data = data[["time", "open", "high", "low", "close", "volume"]]
        data = self._format_time_column(data, period)
        return data.dropna()

    def refresh_chart_data(self):
        if self.chart is None:
            return
        self._ensure_interface()
        start_date, end_date = self._get_chart_dates()
        symbol = self.symbol_input.text().strip()
        period = self.period_combo.currentText()
        adjust = self.adjust_combo.currentText()

        data = self.interface.get_stock_data(
            stock_code=symbol,
            start_date=start_date,
            end_date=end_date,
            period=period,
            adjust=adjust
        )
        chart_data = self._prepare_chart_data(data, period)
        if chart_data.empty:
            return
        self.chart.set(chart_data)
        self.last_data = chart_data
        self.last_bar_time = chart_data["time"].iloc[-1]
        self.last_close = float(chart_data["close"].iloc[-1])
        self._evaluate_signals(chart_data)

    def refresh_latest_bar(self):
        if self.chart is None or self.interface is None:
            return
        symbol = self.symbol_input.text().strip()
        period = self.period_combo.currentText()
        adjust = self.adjust_combo.currentText()

        end_date = self.end_date_edit.date().toPyDate()
        if period == "1d":
            start_date = end_date - pd.Timedelta(days=10)
        else:
            start_date = end_date - pd.Timedelta(days=2)

        data = self.interface.get_stock_data(
            stock_code=symbol,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            period=period,
            adjust=adjust
        )
        chart_data = self._prepare_chart_data(data, period)
        if chart_data.empty:
            return
        last_row = chart_data.iloc[-1]
        self.chart.update(last_row)
        self.last_data = self._merge_latest_data(chart_data)
        current_close = float(last_row["close"])
        if self.last_close is not None:
            if current_close > self.last_close:
                self.chart.marker(text="Up")
            elif current_close < self.last_close:
                self.chart.marker(text="Down")
        self.last_close = current_close
        self.last_bar_time = last_row["time"]
        self._evaluate_signals(self.last_data)

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
        self.chart.marker(text=signal["label"])
        if self.auto_trade_check.isChecked():
            self._execute_trade_signal(signal)

    def _compute_signal(self, data: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if data is None or data.empty:
            return None
        if len(data) < 25:
            return None

        close = data["close"].astype(float)
        high = data["high"].astype(float)
        low = data["low"].astype(float)
        ma_fast = close.rolling(5).mean()
        ma_slow = close.rolling(20).mean()

        prev_fast = ma_fast.iloc[-2]
        prev_slow = ma_slow.iloc[-2]
        curr_fast = ma_fast.iloc[-1]
        curr_slow = ma_slow.iloc[-1]
        curr_time = data["time"].iloc[-1]
        curr_close = close.iloc[-1]

        if prev_fast <= prev_slow and curr_fast > curr_slow:
            return {"name": "ma_cross_up", "label": "MA Up", "side": "buy", "time": curr_time, "price": curr_close}
        if prev_fast >= prev_slow and curr_fast < curr_slow:
            return {"name": "ma_cross_down", "label": "MA Down", "side": "sell", "time": curr_time, "price": curr_close}

        window_high = high.iloc[-21:-1].max()
        window_low = low.iloc[-21:-1].min()
        if curr_close > window_high:
            return {"name": "breakout_up", "label": "Breakout Up", "side": "buy", "time": curr_time, "price": curr_close}
        if curr_close < window_low:
            return {"name": "breakout_down", "label": "Breakout Down", "side": "sell", "time": curr_time, "price": curr_close}
        return None

    def _execute_trade_signal(self, signal: Dict[str, Any]):
        if self.trading_window is None:
            return
        if not getattr(self.trading_window, "is_connected", False):
            return
        volume = int(self.trading_window.volume_spin.value())
        price = float(signal["price"])
        symbol = self.symbol_input.text().strip()
        if signal["side"] == "buy":
            self.trading_window.place_order_signal(symbol, "buy", price, volume)
        elif signal["side"] == "sell":
            self.trading_window.place_order_signal(symbol, "sell", price, volume)
