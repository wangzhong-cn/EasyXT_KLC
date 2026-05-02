"""
回测窗口组件
专业的回测界面，集成Backtrader回测引擎和HTML报告生成
"""

import ast
import importlib
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, cast

import numpy as np
import pandas as pd
from PyQt5.QtCore import QDate, QSettings, Qt, QThread, QTimer, pyqtSignal
from PyQt5.QtGui import QColor, QDoubleValidator, QFont, QPainter, QPen
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QDialog,
    QDoubleSpinBox,
    QFileDialog,
    QFormLayout,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QSplitter,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from core.events import Events
from core.signal_bus import signal_bus

# 尝试导入表格渲染器
try:
    from gui_app.widgets.table_renderer import TableRenderer

    TABLE_RENDERER_AVAILABLE = True
except ImportError:
    TABLE_RENDERER_AVAILABLE = False

# 导入matplotlib用于绘制图表
MATPLOTLIB_AVAILABLE = False
try:
    # 尝试延迟导入，或在类中导入，避免主线程阻塞
    import matplotlib
    # 强制使用Agg后端，避免初始化图形界面导致主线程卡死
    # 注意：FigureCanvasQTAgg 实际上需要 Agg 后端
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    from matplotlib import font_manager
    import matplotlib.pyplot as plt
    backend_qt5agg = importlib.import_module("matplotlib.backends.backend_qt5agg")
    FigureCanvas = getattr(backend_qt5agg, "FigureCanvasQTAgg")
    from matplotlib.figure import Figure
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("[WARNING] matplotlib未安装，净值曲线将显示为占位符")

_MPL_CJK_FONT_CANDIDATES = [
    "Microsoft YaHei",
    "SimHei",
    "Noto Sans CJK SC",
    "Source Han Sans SC",
    "WenQuanYi Zen Hei",
    "Arial Unicode MS",
    "DejaVu Sans",
]


def _matplotlib_font_candidates(preferred: Optional[str] = None) -> list[str]:
    preferred_name = str(preferred or "").strip()
    fonts: list[str] = []
    if preferred_name in _MPL_CJK_FONT_CANDIDATES:
        fonts.append(preferred_name)
    fonts.extend(_MPL_CJK_FONT_CANDIDATES)
    if preferred_name and preferred_name not in fonts:
        fonts.append(preferred_name)
    return list(dict.fromkeys(fonts))


def _configure_matplotlib_fonts(preferred: Optional[str] = None) -> str:
    font_candidates = _matplotlib_font_candidates(preferred)
    primary_font = "DejaVu Sans"
    if MATPLOTLIB_AVAILABLE:
        for font_name in font_candidates:
            try:
                font_manager.findfont(font_name, fallback_to_default=False)
                primary_font = font_name
                break
            except Exception:
                continue
        ordered_fonts = [primary_font] + [f for f in font_candidates if f != primary_font]
        plt.rcParams["font.family"] = ["sans-serif"]
        plt.rcParams["font.sans-serif"] = ordered_fonts
        plt.rcParams["axes.unicode_minus"] = False
    return primary_font


if MATPLOTLIB_AVAILABLE:
    _configure_matplotlib_fonts()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
AdvancedBacktestEngineType: Optional[type[Any]] = None
DualMovingAverageStrategyType: Optional[type[Any]] = None
RSIStrategyType: Optional[type[Any]] = None
MACDStrategyType: Optional[type[Any]] = None
BacktraderImportStatusGetter: Optional[Callable[[], dict[str, Any]]] = None

try:
    engine_module = importlib.import_module("gui_app.backtest.engine")
    AdvancedBacktestEngineType = getattr(engine_module, "AdvancedBacktestEngine", None)
    DualMovingAverageStrategyType = getattr(engine_module, "DualMovingAverageStrategy", None)
    RSIStrategyType = getattr(engine_module, "RSIStrategy", None)
    MACDStrategyType = getattr(engine_module, "MACDStrategy", None)
    BacktraderImportStatusGetter = getattr(engine_module, "get_backtrader_import_status", None)
    print("[OK] 使用修复版回测引擎")
except Exception:
    try:
        engine_module = importlib.import_module("gui_app.backtest.engine")
        AdvancedBacktestEngineType = getattr(engine_module, "AdvancedBacktestEngine", None)
        DualMovingAverageStrategyType = getattr(engine_module, "DualMovingAverageStrategy", None)
        RSIStrategyType = getattr(engine_module, "RSIStrategy", None)
        MACDStrategyType = getattr(engine_module, "MACDStrategy", None)
        BacktraderImportStatusGetter = getattr(engine_module, "get_backtrader_import_status", None)
        print("[WARNING] 使用原版回测引擎")
    except Exception:
        print("[ERROR] 回测引擎导入失败")

try:
    engine_status_ui_module = importlib.import_module("gui_app.backtest.engine_status_ui")
    format_engine_status_ui = getattr(engine_status_ui_module, "format_engine_status_ui")
    build_engine_status_detail = getattr(engine_status_ui_module, "build_engine_status_detail")
    format_engine_status_log = getattr(engine_status_ui_module, "format_engine_status_log")
except Exception:
    def format_engine_status_ui(status: dict[str, Any] | None, label_prefix: str = "引擎") -> dict[str, str]:
        return {
            "text": f"{label_prefix}: 状态未知 ❓",
            "color": "#666666",
            "tooltip": "状态格式化模块不可用",
        }

    def build_engine_status_detail(status: dict[str, Any] | None) -> str:
        return f"状态详情不可用: {status}"

    def format_engine_status_log(
        status: dict[str, Any] | None, prefix: str = "BACKTEST_ENGINE"
    ) -> str:
        return f"[{prefix}] level=WARN mode=unknown available=None message=状态格式化模块不可用"

DataManagerType: Optional[type[Any]] = None
DataSourceType: Optional[type[Any]] = None
RiskAnalyzerType: Optional[type[Any]] = None

try:
    data_manager_module = importlib.import_module("gui_app.backtest.data_manager")
    DataManagerType = getattr(data_manager_module, "DataManager", None)
    DataSourceType = getattr(data_manager_module, "DataSource", None)
    risk_module = importlib.import_module("gui_app.backtest.risk_analyzer")
    RiskAnalyzerType = getattr(risk_module, "RiskAnalyzer", None)
except Exception:
    print("⚠️ 回测模块导入失败，请检查模块路径")

GridStrategyType: Optional[type[Any]] = None
AdaptiveGridStrategyType: Optional[type[Any]] = None
ATRGridStrategyType: Optional[type[Any]] = None

try:
    grid_module = importlib.import_module("strategies.grid_strategy_511380")
    GridStrategyType = getattr(grid_module, "GridStrategy", None)
    AdaptiveGridStrategyType = getattr(grid_module, "AdaptiveGridStrategy", None)
    ATRGridStrategyType = getattr(grid_module, "ATRGridStrategy", None)
except Exception:
    pass

AdvancedBacktestEngine: Optional[type] = AdvancedBacktestEngineType
DualMovingAverageStrategy: Optional[type] = DualMovingAverageStrategyType
RSIStrategy: Optional[type] = RSIStrategyType
MACDStrategy: Optional[type] = MACDStrategyType
DataManager: Optional[type] = DataManagerType
DataSource: Optional[type] = DataSourceType
RiskAnalyzer: Optional[type] = RiskAnalyzerType
GridStrategy: Optional[type] = GridStrategyType
AdaptiveGridStrategy: Optional[type] = AdaptiveGridStrategyType
ATRGridStrategy: Optional[type] = ATRGridStrategyType


class BacktestWorker(QThread):
    """回测工作线程"""

    progress_updated = pyqtSignal(int)
    status_updated = pyqtSignal(str)
    results_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    class StopRequested(Exception):
        pass

    def __init__(self, backtest_params):
        super().__init__()
        self.backtest_params = backtest_params
        self.is_running = True
        self._stop_requested = False

    def safe_int(self, value, default):
        try:
            if value is None:
                return int(default)
            return int(value)
        except Exception:
            return int(default)

    def check_stop(self):
        if self._stop_requested or self.isInterruptionRequested():
            raise BacktestWorker.StopRequested()

    def _resolve_workflow_path(self, workflow_path: str) -> str:
        path = str(workflow_path or "").strip()
        if not path:
            return ""
        if os.path.isabs(path):
            return path
        return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", path))

    def run_single_backtest(self, stock_data, params):
        strategy_name = params.get("strategy_name", "双均线策略")
        if strategy_name == "101因子工作流策略":
            from gui_app.backtest.factor_workflow_bridge import run_factor_workflow_backtest

            symbols = [str(params.get("stock_code", "")).strip()] if params.get("stock_code") else []
            multi_assets = params.get("multi_asset_list") or []
            if params.get("multi_asset_enabled") and isinstance(multi_assets, list):
                symbols = [str(s).strip() for s in multi_assets if str(s).strip()]
            strategy_params = params.get("strategy_params", {}) or {}
            workflow_path = self._resolve_workflow_path(str(strategy_params.get("workflow_path") or ""))
            res = run_factor_workflow_backtest(
                workflow_path=workflow_path,
                symbols=symbols or None,
                start_date=str(params.get("start_date") or ""),
                end_date=str(params.get("end_date") or ""),
                node_overrides=(
                    strategy_params.get("workflow_node_overrides")
                    if isinstance(strategy_params.get("workflow_node_overrides"), dict)
                    else None
                ),
            )
            detailed = dict(res.get("detailed", {}) or {})
            detailed["factor_context"] = res.get("factor_context", {})
            return res.get("metrics", {}), detailed

        # ── 通过 StrategyController 执行回测 ──────────────────────────
        from gui_app.strategy_controller import StrategyController

        strategy_params = params.get("strategy_params", {}) or {}
        if not strategy_params:
            strategy_params = self._build_default_strategy_params(strategy_name, params)

        ctrl = StrategyController()
        result = ctrl.run_adhoc_backtest(
            stock_data=stock_data,
            strategy_name=strategy_name,
            strategy_params=strategy_params,
            initial_cash=params.get("initial_cash", 1_000_000.0),
            commission=params.get("commission", 0.0003),
            period=str(params.get("period", "1d")),
            adjust=str(params.get("adjust", "none")),
        )
        if not result.get("ok"):
            raise ValueError(result.get("error", "回测失败"))
        return result["metrics"], result["detailed"]

    @staticmethod
    def _build_default_strategy_params(strategy_name: str, params: dict) -> dict:
        """当 strategy_params 为空时，从 UI 原始参数构建策略参数。"""
        if strategy_name == "RSI策略":
            return {"rsi_period": params.get("rsi_period", 14)}
        if strategy_name == "MACD策略":
            return {
                "fast_period": params.get("short_period", 12),
                "slow_period": params.get("long_period", 26),
                "signal_period": params.get("rsi_period", 9),
            }
        # 双均线策略（默认）
        return {
            "short_period": params.get("short_period", 5),
            "long_period": params.get("long_period", 20),
            "rsi_period": params.get("rsi_period", 14),
        }

    def aggregate_metrics(self, metrics_list):
        if not metrics_list:
            return {}
        keys = set()
        for m in metrics_list:
            keys.update(m.keys())
        aggregated = {}
        for k in keys:
            vals = [m.get(k) for m in metrics_list if isinstance(m.get(k), (int, float))]
            if vals:
                aggregated[k] = float(np.mean(vals))
        return aggregated

    def run_optimization(self, stock_data, params):
        method = params.get("optimize_method", "网格搜索")
        trials = self.safe_int(params.get("optimize_trials", 30), 30)
        base_short = self.safe_int(params.get("short_period", 5), 5)
        base_long = self.safe_int(params.get("long_period", 20), 20)
        base_rsi = self.safe_int(params.get("rsi_period", 14), 14)
        short_range = list(range(max(3, base_short - 2), base_short + 3))
        long_range = list(range(max(base_short + 5, base_long - 10), base_long + 11, 5))
        rsi_range = list(range(max(5, base_rsi - 4), base_rsi + 5, 2))
        candidates = []
        if method == "网格搜索":
            for short_window in short_range:
                for long_window in long_range:
                    if short_window >= long_window:
                        continue
                    for r in rsi_range:
                        candidates.append((short_window, long_window, r))
        else:
            for _ in range(max(trials, 1)):
                short_window = np.random.choice(short_range)
                long_window = np.random.choice(long_range)
                if short_window >= long_window:
                    long_window = max(long_window, short_window + 5)
                r = np.random.choice(rsi_range)
                candidates.append((int(short_window), int(long_window), int(r)))
        if trials > 0:
            candidates = candidates[:trials]
        results = []
        best_score = -float("inf")
        best_params = None
        for short_window, long_window, r in candidates:
            self.check_stop()
            local_params = dict(params)
            local_params["short_period"] = short_window
            local_params["long_period"] = long_window
            local_params["rsi_period"] = r
            metrics, _ = self.run_single_backtest(stock_data, local_params)
            score = float(metrics.get("sharpe_ratio", 0)) * float(metrics.get("total_return", 0))
            results.append(
                {
                    "params": {"short": short_window, "long": long_window, "rsi": r},
                    "total_return": metrics.get("total_return", 0),
                    "sharpe_ratio": metrics.get("sharpe_ratio", 0),
                    "max_drawdown": metrics.get("max_drawdown", 0),
                    "score": score,
                }
            )
            if score > best_score:
                best_score = score
                best_params = {"short": short_window, "long": long_window, "rsi": r}
        return {
            "best_params": best_params,
            "best_score": best_score,
            "optimization_results": results,
        }

    def run_walk_forward(self, stock_data, params):
        wf_window = self.safe_int(params.get("wf_window_days", 252), 252)
        wf_step = self.safe_int(params.get("wf_step_days", 60), 60)
        oos_ratio = float(params.get("oos_ratio", 0.2))
        if wf_window <= 0 or wf_step <= 0:
            return []
        n = len(stock_data)
        if n <= wf_window:
            return []
        results = []
        max_windows = 10
        count = 0
        for start in range(0, n - wf_window, wf_step):
            self.check_stop()
            end = start + wf_window
            window_data = stock_data.iloc[start:end]
            train_size = int(wf_window * (1 - oos_ratio))
            train_data = window_data.iloc[:train_size]
            test_data = window_data.iloc[train_size:]
            train_metrics, _ = self.run_single_backtest(train_data, params)
            test_metrics, _ = self.run_single_backtest(test_data, params)
            results.append(
                {
                    "window": f"{start}-{end}",
                    "train": f"{len(train_data)}",
                    "test": f"{len(test_data)}",
                    "return": test_metrics.get("total_return", 0),
                    "sharpe_ratio": test_metrics.get("sharpe_ratio", 0),
                    "max_drawdown": test_metrics.get("max_drawdown", 0),
                }
            )
            count += 1
            if count >= max_windows:
                break
        return results

    def run_walk_forward_sensitivity(self, stock_data, params, best_params):
        if not best_params:
            return []
        wf_window = self.safe_int(params.get("wf_window_days", 252), 252)
        wf_step = self.safe_int(params.get("wf_step_days", 60), 60)
        oos_ratio = float(params.get("oos_ratio", 0.2))
        if wf_window <= 0 or wf_step <= 0:
            return []
        n = len(stock_data)
        if n <= wf_window:
            return []
        results = []
        max_windows = 10
        count = 0
        for start in range(0, n - wf_window, wf_step):
            self.check_stop()
            end = start + wf_window
            window_data = stock_data.iloc[start:end]
            train_size = int(wf_window * (1 - oos_ratio))
            test_data = window_data.iloc[train_size:]
            if test_data.empty:
                continue
            for key, value in best_params.items():
                if not isinstance(value, (int, float)):
                    continue
                for delta in [-0.1, 0.1]:
                    self.check_stop()
                    new_value = value * (1 + delta)
                    if isinstance(value, int):
                        new_value = max(int(round(new_value)), 1)
                    local_params = dict(params)
                    strategy_params = dict(params.get("strategy_params", {}))
                    strategy_params.update(best_params)
                    strategy_params[key] = new_value
                    local_params["strategy_params"] = strategy_params
                    metrics, _ = self.run_single_backtest(test_data, local_params)
                    results.append(
                        {
                            "window": f"{start}-{end}",
                            "param": key,
                            "value": new_value,
                            "delta": f"{int(delta * 100)}%",
                            "total_return": metrics.get("total_return", 0),
                            "sharpe_ratio": metrics.get("sharpe_ratio", 0),
                            "max_drawdown": metrics.get("max_drawdown", 0),
                        }
                    )
            count += 1
            if count >= max_windows:
                break
        return results

    def compute_overfit_warnings(self, metrics, walk_forward_results, params):
        if not params.get("overfit_warning_enabled", True):
            return []
        if not walk_forward_results:
            return []
        sharpe_drop = float(params.get("overfit_sharpe_drop", 0.3))
        drawdown_increase = float(params.get("overfit_drawdown_increase", 0.2))
        in_sharpe = float(metrics.get("sharpe_ratio", 0) or 0)
        in_dd = abs(float(metrics.get("max_drawdown", 0) or 0))
        wf_sharpes = [
            float(r.get("sharpe_ratio", r.get("sharpe", 0)) or 0) for r in walk_forward_results
        ]
        wf_returns = [
            float(r.get("return", r.get("total_return", 0)) or 0) for r in walk_forward_results
        ]
        wf_dds = [
            abs(float(r.get("max_drawdown", r.get("drawdown", 0)) or 0))
            for r in walk_forward_results
        ]
        wf_sharpe_avg = sum(wf_sharpes) / len(wf_sharpes) if wf_sharpes else 0
        wf_return_avg = sum(wf_returns) / len(wf_returns) if wf_returns else 0
        wf_dd_avg = sum(wf_dds) / len(wf_dds) if wf_dds else 0
        warnings = []
        if in_sharpe > 0 and wf_sharpe_avg < in_sharpe * (1 - sharpe_drop):
            warnings.append("过拟合警戒: Walk-Forward 夏普显著下降")
        if in_dd > 0 and wf_dd_avg > in_dd * (1 + drawdown_increase):
            warnings.append("过拟合警戒: Walk-Forward 回撤显著放大")
        if wf_return_avg < 0:
            warnings.append("过拟合警戒: Walk-Forward 平均收益为负")
        return warnings

    def build_cost_analysis(self, trades, params):
        commission = float(params.get("commission", 0))
        slippage_bps = float(params.get("slippage_bps", 0))
        slippage_rate = slippage_bps / 10000.0
        turnover = 0.0
        trade_count = 0
        for t in trades:
            try:
                price = float(t[2])
                qty = float(t[3])
                turnover += abs(price * qty)
                trade_count += 1
            except Exception:
                continue
        estimated_cost = turnover * (commission + slippage_rate)
        return {
            "turnover": turnover,
            "trade_count": trade_count,
            "commission_rate": commission,
            "slippage_bps": slippage_bps,
            "estimated_cost": estimated_cost,
        }

    def extract_close_series(self, stock_data):
        for col in ["close", "Close", "收盘", "adj_close", "close_price"]:
            if col in stock_data.columns:
                return stock_data[col]
        return stock_data.iloc[:, 0]

    def get_rebalance_dates(self, index, freq: str):
        if not isinstance(index, pd.DatetimeIndex):
            index = pd.to_datetime(index)
        if freq == "日频":
            return set(index)
        if freq == "周频":
            periods = index.to_period("W")
        else:
            periods = index.to_period("M")
        grouped = pd.Series(index).groupby(periods)
        dates = grouped.max().tolist()
        if isinstance(dates, (pd.Timestamp, datetime)):
            dates_list = [dates]
        else:
            dates_list = list(dates)
        return set(pd.to_datetime(dates_list))

    def cap_and_normalize_weights(self, weights, max_weight):
        weights = weights.clip(lower=0)
        capped = weights.copy()
        if max_weight is not None:
            capped = capped.clip(upper=max_weight)
        remaining = 1.0 - capped.sum()
        if remaining <= 0:
            return capped
        available = capped.index[capped < (max_weight if max_weight is not None else 1.0)]
        while remaining > 1e-6 and len(available) > 0:
            increment = remaining / len(available)
            capped.loc[available] = capped.loc[available] + increment
            if max_weight is not None:
                capped = capped.clip(upper=max_weight)
            remaining = 1.0 - capped.sum()
            available = capped.index[capped < (max_weight if max_weight is not None else 1.0)]
        return capped

    def compute_target_weights(self, returns_window, method, max_weight):
        if returns_window.empty:
            return None
        if method == "固定比例":
            weights = pd.Series(1.0 / returns_window.shape[1], index=returns_window.columns)
        else:
            vol = returns_window.std().replace(0, np.nan)
            vol = vol.fillna(vol.mean() if not np.isnan(vol.mean()) else 1.0)
            inv_vol = 1.0 / vol
            weights = inv_vol / inv_vol.sum()
        return self.cap_and_normalize_weights(weights, max_weight)

    def build_multi_asset_portfolio(self, data_map, params):
        price_series = {}
        for symbol, df in data_map.items():
            self.check_stop()
            series = self.extract_close_series(df)
            series = series.copy()
            if not isinstance(series.index, pd.DatetimeIndex):
                series.index = pd.to_datetime(series.index)
            price_series[symbol] = series
        price_df = pd.concat(price_series, axis=1, join="inner").sort_index()
        price_df = price_df.dropna()
        if price_df.empty:
            return (
                {"dates": [], "values": []},
                {
                    "turnover": 0,
                    "trade_count": 0,
                    "commission_rate": 0,
                    "slippage_bps": 0,
                    "estimated_cost": 0,
                },
                [],
            )
        returns = price_df.pct_change().fillna(0)
        rebalance_freq = params.get("rebalance_freq", "月频")
        rebalance_dates = self.get_rebalance_dates(returns.index, rebalance_freq)
        max_weight = params.get("max_position_pct", 30) / 100.0
        method = params.get("position_sizing", "固定比例")
        lookback = 20
        weights = None
        last_weights = None
        portfolio_values = []
        turnover = 0.0
        trade_count = 0
        weight_records = []
        initial_cash = float(params.get("initial_cash", 100000))
        portfolio_value = initial_cash
        for i, date in enumerate(returns.index):
            self.check_stop()
            if date in rebalance_dates or weights is None:
                window = returns.iloc[max(0, i - lookback) : i] if i > 0 else returns.iloc[:1]
                weights = self.compute_target_weights(window, method, max_weight)
                if weights is None:
                    weights = pd.Series(1.0 / returns.shape[1], index=returns.columns)
                if last_weights is not None:
                    diff = (weights - last_weights).abs().sum()
                    turnover += diff * portfolio_value
                    trade_count += int((weights != last_weights).sum())
                last_weights = weights
                weight_records.append({"date": date, "weights": weights.to_dict()})
            daily_ret = float((weights * returns.loc[date]).sum()) if weights is not None else 0.0
            portfolio_value = portfolio_value * (1 + daily_ret)
            portfolio_values.append(portfolio_value)
        commission = float(params.get("commission", 0))
        slippage_bps = float(params.get("slippage_bps", 0))
        slippage_rate = slippage_bps / 10000.0
        estimated_cost = turnover * (commission + slippage_rate)
        cost_analysis = {
            "turnover": turnover,
            "trade_count": trade_count,
            "commission_rate": commission,
            "slippage_bps": slippage_bps,
            "estimated_cost": estimated_cost,
        }
        return (
            {"dates": list(returns.index), "values": portfolio_values},
            cost_analysis,
            weight_records,
        )

    def slice_data_map(self, data_map, start_date, end_date):
        sliced = {}
        for symbol, df in data_map.items():
            if not isinstance(df.index, pd.DatetimeIndex):
                df = df.copy()
                df.index = pd.to_datetime(df.index)
            sliced_df = df.loc[(df.index >= start_date) & (df.index <= end_date)]
            if not sliced_df.empty:
                sliced[symbol] = sliced_df
        return sliced

    def score_from_curve(self, curve):
        if RiskAnalyzer is None:
            raise ValueError("风险分析器不可用")
        risk_analyzer = cast(Any, RiskAnalyzer)()
        values = curve.get("values", [])
        metrics = risk_analyzer.analyze_portfolio(values)
        score = float(metrics.get("sharpe_ratio", 0)) * float(metrics.get("total_return", 0))
        return metrics, score

    def run_walk_forward_portfolio(self, data_map, params):
        wf_window = int(params.get("wf_window_days", 252))
        wf_step = int(params.get("wf_step_days", 60))
        oos_ratio = float(params.get("oos_ratio", 0.2))
        if wf_window <= 0 or wf_step <= 0:
            return []
        any_df = next(iter(data_map.values()))
        idx = any_df.index
        if not isinstance(idx, pd.DatetimeIndex):
            idx = pd.to_datetime(idx)
        if len(idx) <= wf_window:
            return []
        results = []
        max_windows = 10
        count = 0
        for start in range(0, len(idx) - wf_window, wf_step):
            self.check_stop()
            end = start + wf_window
            window_dates = idx[start:end]
            train_size = int(wf_window * (1 - oos_ratio))
            train_end = window_dates[train_size - 1] if train_size > 0 else window_dates[0]
            test_start = (
                window_dates[train_size] if train_size < len(window_dates) else window_dates[-1]
            )
            test_end = window_dates[-1]
            train_map = self.slice_data_map(data_map, window_dates[0], train_end)
            test_map = self.slice_data_map(data_map, test_start, test_end)
            if not train_map or not test_map:
                continue
            train_portfolio = cast(tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]], self.build_multi_asset_portfolio(train_map, params))
            train_curve, _, _ = train_portfolio
            test_portfolio = cast(tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]], self.build_multi_asset_portfolio(test_map, params))
            test_curve, _, _ = test_portfolio
            test_metrics, _ = self.score_from_curve(test_curve)
            results.append(
                {
                    "window": f"{start}-{end}",
                    "train": f"{len(window_dates[:train_size])}",
                    "test": f"{len(window_dates[train_size:])}",
                    "return": test_metrics.get("total_return", 0),
                    "sharpe_ratio": test_metrics.get("sharpe_ratio", 0),
                    "max_drawdown": test_metrics.get("max_drawdown", 0),
                    "curve": test_curve,
                }
            )
            count += 1
            if count >= max_windows:
                break
        return results

    def run_optimization_multi(self, data_map, params):
        methods = ["固定比例", "波动率目标", "风险平价"]
        base_max = float(params.get("max_position_pct", 30))
        candidates = []
        for method in methods:
            for delta in [-10, -5, 0, 5, 10]:
                max_pos = min(max(base_max + delta, 5), 50)
                candidates.append((method, max_pos))
        trials = self.safe_int(params.get("optimize_trials", 30), 30)
        candidates = candidates[: max(trials, 1)]
        results = []
        best_score = -float("inf")
        best_params = None
        for method, max_pos in candidates:
            self.check_stop()
            local_params = dict(params)
            local_params["position_sizing"] = method
            local_params["max_position_pct"] = max_pos
            portfolio_bundle = cast(tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]], self.build_multi_asset_portfolio(data_map, local_params))
            curve, _, _ = portfolio_bundle
            metrics, score = self.score_from_curve(curve)
            results.append(
                {
                    "params": {"position_sizing": method, "max_position_pct": max_pos},
                    "total_return": metrics.get("total_return", 0),
                    "sharpe_ratio": metrics.get("sharpe_ratio", 0),
                    "max_drawdown": metrics.get("max_drawdown", 0),
                    "score": score,
                }
            )
            if score > best_score:
                best_score = score
                best_params = {"position_sizing": method, "max_position_pct": max_pos}
        return {
            "best_params": best_params,
            "best_score": best_score,
            "optimization_results": results,
        }

    def score_from_metrics(self, metrics):
        total_return = float(metrics.get("total_return", 0) or 0)
        sharpe = float(metrics.get("sharpe_ratio", 0) or 0)
        max_dd = float(metrics.get("max_drawdown", 0) or 0)
        return sharpe * (1 + total_return) / (1 + max(abs(max_dd), 0))

    def run_grid_optimization(self, stock_data, params, strategy_name):
        trials = self.safe_int(params.get("optimize_trials", 30), 30)
        results = []
        best_score = -float("inf")
        best_params = None
        custom_space = (
            params.get("custom_search_space", {})
            if isinstance(params.get("custom_search_space", {}), dict)
            else {}
        )
        if strategy_name == "固定网格策略":
            space = (
                custom_space.get("fixed_grid", {})
                if isinstance(custom_space.get("fixed_grid", {}), dict)
                else {}
            )
            grid_counts = space.get("grid_count", [8, 12, 16, 20, 24])
            price_ranges = space.get("price_range", [0.02, 0.05, 0.08, 0.12])
            position_sizes = space.get("position_size", [500, 1000, 2000])
            candidates = [
                (g, r, p) for g in grid_counts for r in price_ranges for p in position_sizes
            ]
            keys = ["grid_count", "price_range", "position_size"]
        elif strategy_name == "自适应网格策略":
            space = (
                custom_space.get("adaptive_grid", {})
                if isinstance(custom_space.get("adaptive_grid", {}), dict)
                else {}
            )
            buy_th = space.get("buy_threshold", [0.005, 0.01, 0.02, 0.03])
            sell_th = space.get("sell_threshold", [0.005, 0.01, 0.02, 0.03])
            position_sizes = space.get("position_size", [500, 1000, 2000])
            max_positions = space.get("max_position", [5000, 10000, 20000])
            candidates = [
                (b, s, p, m)
                for b in buy_th
                for s in sell_th
                for p in position_sizes
                for m in max_positions
            ]
            keys = ["buy_threshold", "sell_threshold", "position_size", "max_position"]
        else:
            space = (
                custom_space.get("atr_grid", {})
                if isinstance(custom_space.get("atr_grid", {}), dict)
                else {}
            )
            atr_periods = space.get("atr_period", [14, 28, 60, 120])
            atr_multipliers = space.get("atr_multiplier", [2.0, 4.0, 6.0, 8.0])
            position_sizes = space.get("position_size", [500, 1000, 2000])
            candidates = [
                (a, m, p) for a in atr_periods for m in atr_multipliers for p in position_sizes
            ]
            keys = ["atr_period", "atr_multiplier", "position_size"]
        if trials > 0:
            candidates = candidates[:trials]
        for combo in candidates:
            self.check_stop()
            local_params = dict(params)
            param_dict = dict(zip(keys, combo))
            strategy_params = dict(params.get("strategy_params", {}))
            strategy_params.update(param_dict)
            local_params["strategy_params"] = strategy_params
            metrics, _ = self.run_single_backtest(stock_data, local_params)
            score = self.score_from_metrics(metrics)
            row = {
                "params": param_dict,
                "total_return": metrics.get("total_return", 0),
                "sharpe_ratio": metrics.get("sharpe_ratio", 0),
                "max_drawdown": metrics.get("max_drawdown", 0),
                "score": score,
            }
            results.append(row)
            if score > best_score:
                best_score = score
                best_params = param_dict
        return {
            "best_params": best_params,
            "best_score": best_score,
            "optimization_results": results,
        }

    def run_grid_sensitivity(self, stock_data, params, best_params):
        if not best_params:
            return []
        sensitivity = []
        for key, value in best_params.items():
            if not isinstance(value, (int, float)):
                continue
            for delta in [-0.1, 0.1]:
                self.check_stop()
                new_value = value * (1 + delta)
                if isinstance(value, int):
                    new_value = max(int(round(new_value)), 1)
                local_params = dict(params)
                strategy_params = dict(params.get("strategy_params", {}))
                strategy_params.update(best_params)
                strategy_params[key] = new_value
                local_params["strategy_params"] = strategy_params
                metrics, _ = self.run_single_backtest(stock_data, local_params)
                sensitivity.append(
                    {
                        "param": key,
                        "value": new_value,
                        "delta": f"{int(delta * 100)}%",
                        "total_return": metrics.get("total_return", 0),
                        "sharpe_ratio": metrics.get("sharpe_ratio", 0),
                        "max_drawdown": metrics.get("max_drawdown", 0),
                    }
                )
        return sensitivity

    def run_factor_scenario_compare(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        strategy_name = params.get("strategy_name", "")
        if strategy_name != "101因子工作流策略":
            return []
        strategy_params = params.get("strategy_params", {}) or {}
        workflow_path = self._resolve_workflow_path(str(strategy_params.get("workflow_path") or ""))
        if not workflow_path:
            return []
        node_overrides = strategy_params.get("workflow_node_overrides")
        if not isinstance(node_overrides, dict):
            return []
        scenarios = node_overrides.get("__scenarios__", [])
        if not isinstance(scenarios, list) or not scenarios:
            return []
        from gui_app.backtest.factor_workflow_bridge import run_factor_workflow_backtest

        symbols = [str(params.get("stock_code", "")).strip()] if params.get("stock_code") else []
        multi_assets = params.get("multi_asset_list") or []
        if params.get("multi_asset_enabled") and isinstance(multi_assets, list):
            symbols = [str(s).strip() for s in multi_assets if str(s).strip()]
        compare_rows: list[dict[str, Any]] = []
        for item in scenarios:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip() or f"scenario_{len(compare_rows) + 1}"
            overrides = item.get("overrides", {})
            if not isinstance(overrides, dict):
                continue
            res = run_factor_workflow_backtest(
                workflow_path=workflow_path,
                symbols=symbols or None,
                start_date=str(params.get("start_date") or ""),
                end_date=str(params.get("end_date") or ""),
                node_overrides=overrides,
            )
            m = res.get("metrics", {}) if isinstance(res, dict) else {}
            compare_rows.append(
                {
                    "params": name,
                    "total_return": float(m.get("total_return", 0.0) or 0.0),
                    "sharpe_ratio": float(m.get("sharpe_ratio", 0.0) or 0.0),
                    "max_drawdown": float(m.get("max_drawdown", 0.0) or 0.0),
                    "ic_mean": float(m.get("ic_mean", 0.0) or 0.0),
                    "ic_ir": float(m.get("ic_ir", 0.0) or 0.0),
                    "score": float(m.get("sharpe_ratio", 0.0) or 0.0)
                    * float(m.get("total_return", 0.0) or 0.0),
                    "scenario_overrides": overrides,
                }
            )
        return compare_rows

    def run(self):
        """执行回测"""
        try:
            self.check_stop()
            self.status_updated.emit("🚀 初始化回测引擎...")
            self.progress_updated.emit(10)
            self.status_updated.emit("📊 获取历史数据...")
            self.progress_updated.emit(30)
            if DataManager is None:
                raise ValueError("数据管理器不可用")
            data_manager = cast(Any, DataManager)()
            symbols = [self.backtest_params["stock_code"]]
            if self.backtest_params.get("multi_asset_enabled"):
                symbols = self.backtest_params.get("multi_asset_list") or symbols
            all_metrics = []
            all_details = []
            all_curves = []
            primary_data = None
            data_map = {}
            data_summary_items = []
            total_rows = 0
            sources = set()
            for symbol in symbols:
                self.check_stop()
                data_manager.last_source = None
                stock_data = data_manager.get_stock_data(
                    stock_code=symbol,
                    start_date=self.backtest_params["start_date"],
                    end_date=self.backtest_params["end_date"],
                    period=self.backtest_params.get("period", "1d"),
                    adjust=self.backtest_params.get("adjust", "none"),
                )
                info = data_manager.last_data_info or {}
                source = info.get("source") or data_manager.last_source or "unknown"
                rows = len(stock_data)
                raw_rows = info.get("raw_rows", rows)
                clean_rows = info.get("clean_rows", rows)
                removed_rows = info.get("removed_rows", max(raw_rows - clean_rows, 0))
                period = info.get("period", self.backtest_params.get("period", "1d"))
                adjust = info.get("adjust", self.backtest_params.get("adjust", "none"))
                start_str = "-"
                end_str = "-"
                if rows > 0:
                    if isinstance(stock_data.index, pd.DatetimeIndex):
                        idx = stock_data.index
                    elif "date" in stock_data.columns:
                        idx = pd.to_datetime(stock_data["date"], errors="coerce")
                    else:
                        idx = None
                    if idx is not None and len(idx) > 0:
                        start_ts = pd.to_datetime(idx.min(), errors="coerce")
                        end_ts = pd.to_datetime(idx.max(), errors="coerce")
                        fmt = "%Y-%m-%d" if period == "1d" else "%Y-%m-%d %H:%M"
                        if pd.notna(start_ts):
                            start_str = start_ts.strftime(fmt)
                        if pd.notna(end_ts):
                            end_str = end_ts.strftime(fmt)
                quality = data_manager.validate_data_quality(stock_data)
                missing_total = (
                    int(sum(quality.get("missing_values", {}).values())) if quality else 0
                )
                issues = quality.get("issues", []) if quality else []
                data_summary_items.append(
                    {
                        "symbol": symbol,
                        "rows": rows,
                        "raw_rows": raw_rows,
                        "clean_rows": clean_rows,
                        "removed_rows": removed_rows,
                        "start_date": start_str,
                        "end_date": end_str,
                        "source": source,
                        "period": period,
                        "adjust": adjust,
                        "missing_total": missing_total,
                        "quality_issues": issues,
                    }
                )
                total_rows += rows
                sources.add(source)
                if stock_data.empty:
                    continue
                data_map[symbol] = stock_data
                if primary_data is None:
                    primary_data = stock_data
                self.check_stop()
                metrics, detailed = self.run_single_backtest(stock_data, self.backtest_params)
                all_metrics.append(metrics)
                all_details.append(detailed)
                curve = detailed.get("portfolio_curve", {})
                if (
                    (not curve or "dates" not in curve or "values" not in curve)
                    and isinstance(detailed, dict)
                ):
                    ls = detailed.get("long_short_results", {})
                    ret_s = ls.get("returns") if isinstance(ls, dict) else None
                    if isinstance(ret_s, pd.Series) and not ret_s.empty:
                        clean = pd.to_numeric(ret_s, errors="coerce").dropna()
                        if not clean.empty:
                            initial_cash = float(self.backtest_params.get("initial_cash", 100000))
                            equity = (1.0 + clean).cumprod() * initial_cash
                            curve = {
                                "dates": [x.strftime("%Y-%m-%d") for x in equity.index],
                                "values": [float(v) for v in equity.values],
                            }
                            detailed["portfolio_curve"] = curve
                if curve and "dates" in curve and "values" in curve:
                    all_curves.append(curve)
            if not all_metrics:
                raise Exception("无法获取股票数据")

            self.status_updated.emit("🔧 配置策略参数...")
            self.progress_updated.emit(50)

            self.status_updated.emit("⚡ 执行回测计算...")
            self.progress_updated.emit(70)
            self.check_stop()
            results = self.aggregate_metrics(all_metrics)
            self.status_updated.emit("📈 分析风险指标...")
            self.progress_updated.emit(90)
            if RiskAnalyzer is None:
                raise ValueError("风险分析器不可用")
            risk_analyzer = cast(Any, RiskAnalyzer)()
            cost_analysis = {}
            weight_records = []
            if self.backtest_params.get("multi_asset_enabled") and len(data_map) > 1:
                portfolio_bundle = cast(tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]], self.build_multi_asset_portfolio(data_map, self.backtest_params))
                portfolio_curve, cost_analysis, weight_records = portfolio_bundle
                portfolio_values = portfolio_curve.get("values", [])
                risk_analysis = risk_analyzer.analyze_portfolio(portfolio_values)
                results = risk_analysis
                detailed_results = {"portfolio_curve": portfolio_curve, "trades": []}
            else:
                if all_curves:
                    min_len = min(len(c.get("values", [])) for c in all_curves)
                    dates = all_curves[0].get("dates", [])[:min_len]
                    values_matrix = [c.get("values", [])[:min_len] for c in all_curves]
                    avg_values = (
                        list(np.mean(np.array(values_matrix), axis=0)) if values_matrix else []
                    )
                    portfolio_curve = {"dates": dates, "values": avg_values}
                else:
                    portfolio_curve = {"dates": [], "values": []}
                detailed_results = all_details[0] if all_details else {}
                detailed_results["portfolio_curve"] = portfolio_curve
                portfolio_values = portfolio_curve.get("values", [])
                risk_analysis = risk_analyzer.analyze_portfolio(portfolio_values)
                trades = detailed_results.get("trades", [])
                cost_analysis = self.build_cost_analysis(trades, self.backtest_params)
            optimization_results = {}
            sensitivity_results = []
            walk_forward_sensitivity = []
            overfit_warnings = []
            walk_forward_results = []
            factor_compare_results = []
            if self.backtest_params.get("optimize_enabled"):
                self.check_stop()
                strategy_name = self.backtest_params.get("strategy_name", "")
                supported = {"双均线策略", "RSI策略", "MACD策略"}
                grid_supported = {"固定网格策略", "自适应网格策略", "ATR网格策略"}
                if self.backtest_params.get("multi_asset_enabled") and len(data_map) > 1:
                    optimization_results = self.run_optimization_multi(
                        data_map, self.backtest_params
                    )
                elif primary_data is not None and strategy_name in supported:
                    optimization_results = self.run_optimization(primary_data, self.backtest_params)
                elif primary_data is not None and strategy_name in grid_supported:
                    optimization_results = self.run_grid_optimization(
                        primary_data, self.backtest_params, strategy_name
                    )
                    sensitivity_results = self.run_grid_sensitivity(
                        primary_data, self.backtest_params, optimization_results.get("best_params")
                    )
            if self.backtest_params.get("walk_forward_enabled"):
                self.check_stop()
                if self.backtest_params.get("multi_asset_enabled") and len(data_map) > 1:
                    walk_forward_results = self.run_walk_forward_portfolio(
                        data_map, self.backtest_params
                    )
                elif primary_data is not None:
                    walk_forward_results = self.run_walk_forward(primary_data, self.backtest_params)
                    if strategy_name in grid_supported:
                        best_params = optimization_results.get("best_params")
                        walk_forward_sensitivity = self.run_walk_forward_sensitivity(
                            primary_data, self.backtest_params, best_params
                        )
            overfit_warnings = self.compute_overfit_warnings(
                results, walk_forward_results, self.backtest_params
            )
            factor_compare_results = self.run_factor_scenario_compare(self.backtest_params)

            # 合并结果
            factor_context = (
                detailed_results.get("factor_context", {})
                if isinstance(detailed_results, dict)
                else {}
            )
            final_results = {
                "performance_metrics": results,
                "detailed_results": detailed_results,
                "risk_analysis": risk_analysis,
                "portfolio_curve": portfolio_curve,
                "stock_data": primary_data,
                "backtest_params": self.backtest_params,
                "optimization_results": optimization_results.get("optimization_results", []),
                "sensitivity_results": sensitivity_results,
                "walk_forward_sensitivity": walk_forward_sensitivity,
                "overfit_warnings": overfit_warnings,
                "walk_forward_results": walk_forward_results,
                "factor_compare_results": factor_compare_results,
                "cost_analysis": cost_analysis,
                "weight_records": weight_records,
                "data_summary": {
                    "items": data_summary_items,
                    "total_rows": total_rows,
                    "sources": sorted(sources),
                    "loaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                },
                "factor_context": factor_context if isinstance(factor_context, dict) else {},
            }

            self.status_updated.emit("✅ 回测完成")
            self.progress_updated.emit(100)

            self.results_ready.emit(final_results)

        except BacktestWorker.StopRequested:
            self.status_updated.emit("⏹️ 回测已停止")
            self.progress_updated.emit(0)
        except Exception as e:
            self.error_occurred.emit(f"回测执行失败: {str(e)}")

    def stop(self):
        """停止回测"""
        self.is_running = False
        self._stop_requested = True
        self.requestInterruption()


class PortfolioChart(QWidget):
    """投资组合净值曲线图表组件"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._font_family = _configure_matplotlib_fonts(QFont().family())
        self._font_size = max(9, QFont().pointSize())
        palette_color = self.palette().window().color()
        self._bg_color = palette_color.name()
        self._bg_rgba = (
            palette_color.redF(),
            palette_color.greenF(),
            palette_color.blueF(),
            palette_color.alphaF(),
        )
        self.init_ui()
        self._plot_x = None
        self._plot_y = None
        self._plot_drawdown = None
        self._plot_benchmark = None
        self._plot_positions = None
        self._plot_trade_notes = None
        self._autoscale = True
        self._locked_xlim = None
        self._locked_ylim = None
        self._y_lock = False
        self._x_lock = False
        self._x_window_ratio = 0.35
        self._view_presets = {}
        self._current_preset_name = ""
        self._last_plot = None

    def init_ui(self):
        layout = QVBoxLayout(self)

        if MATPLOTLIB_AVAILABLE:
            _configure_matplotlib_fonts(self._font_family)
            plt.rcParams["axes.unicode_minus"] = False
            plt.rcParams["font.size"] = self._font_size
            # 创建matplotlib图表
            self.figure = Figure(figsize=(10, 6), dpi=100)
            self.canvas = FigureCanvas(self.figure)
            self.canvas.setMouseTracking(True)
            self.canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            layout.addWidget(self.canvas)

            # 初始化空图表
            self.ax = self.figure.add_subplot(111)
            self.figure.patch.set_facecolor(self._bg_rgba)
            self.ax.set_facecolor(self._bg_rgba)
            self.ax.set_title(
                "投资组合净值曲线",
                fontsize=self._font_size + 2,
                fontweight="bold",
                fontfamily=self._font_family,
            )
            self.ax.set_xlabel("日期", fontfamily=self._font_family, fontsize=self._font_size)
            self.ax.set_ylabel("净值", fontfamily=self._font_family, fontsize=self._font_size)
            self.ax.grid(True, alpha=0.3)
            self.figure.subplots_adjust(left=0.06, right=0.98, top=0.90, bottom=0.14)
            self.vline = self.ax.axvline(color="#999", lw=0.8, alpha=0.6, zorder=5)
            self.hline = self.ax.axhline(color="#999", lw=0.8, alpha=0.6, zorder=5)
            self.annot = self.ax.annotate(
                "",
                xy=(0, 0),
                xytext=(10, 10),
                textcoords="offset points",
                bbox=dict(boxstyle="round", fc="w", ec="#999", alpha=0.9),
                fontsize=self._font_size,
                fontfamily=self._font_family,
            )
            self.annot.set_visible(False)
            self.canvas.mpl_connect("motion_notify_event", self.on_mouse_move)
            self.canvas.draw()
        else:
            # 如果matplotlib不可用，显示占位符
            placeholder = QLabel("净值曲线图已生成\n(需要安装matplotlib查看图表)")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            placeholder.setStyleSheet("""
                QLabel {
                    background-color: #f0f0f0;
                    border: 2px dashed #ccc;
                    border-radius: 8px;
                    padding: 20px;
                    font-size: 14px;
                    color: #666;
                }
            """)
            layout.addWidget(placeholder)

    def plot_portfolio_curve(
        self, dates, values, initial_value=100000, benchmark=None, trades=None, daily_holdings=None
    ):
        """绘制投资组合净值曲线"""
        if not MATPLOTLIB_AVAILABLE or not dates or not values:
            return

        try:
            # 限制绘制点数，避免大量数据点导致 UI 卡死
            max_points = 2000
            if len(dates) > max_points:
                step = len(dates) // max_points
                dates = dates[::step]
                values = values[::step]
                if benchmark:
                    benchmark = benchmark[::step]

            self._last_plot = {
                "dates": dates,
                "values": values,
                "initial_value": initial_value,
                "benchmark": benchmark,
                "trades": trades,
                "daily_holdings": daily_holdings,
            }
            # 清除之前的图表
            self.ax.clear()
            self.figure.patch.set_facecolor(self._bg_rgba)
            self.ax.set_facecolor(self._bg_rgba)
            self.figure.subplots_adjust(left=0.06, right=0.98, top=0.90, bottom=0.14)
            self.vline = self.ax.axvline(color="#999", lw=0.8, alpha=0.6, zorder=5)
            self.hline = self.ax.axhline(color="#999", lw=0.8, alpha=0.6, zorder=5)
            self.annot = self.ax.annotate(
                "",
                xy=(0, 0),
                xytext=(10, 10),
                textcoords="offset points",
                bbox=dict(boxstyle="round", fc="w", ec="#999", alpha=0.9),
                fontsize=self._font_size,
                fontfamily=self._font_family,
            )
            self.annot.set_visible(False)

            value_series = pd.Series(values, dtype="float64")
            if initial_value:
                net_values = value_series / float(initial_value)
            else:
                net_values = value_series
            date_series = pd.to_datetime(pd.Series(dates), errors="coerce")
            mask = date_series.notna() & net_values.notna()
            date_series = date_series[mask]
            net_values = net_values[mask]
            if len(net_values) < 2:
                return
            date_series_np = np.asarray(date_series.values)
            order = np.argsort(date_series_np)
            date_series = date_series.iloc[order]
            net_values = net_values.iloc[order]
            line_dates = date_series.tolist()
            net_values = net_values.tolist()
            bench_values = None
            if benchmark is not None and len(benchmark) == len(values):
                bench_series = pd.Series(benchmark, dtype="float64")[mask]
                bench_series = bench_series.iloc[order]
                bench_values = bench_series.tolist()
            self._plot_x = mdates.date2num(line_dates)
            self._plot_y = net_values
            self._plot_benchmark = bench_values
            plot_x = np.asarray(self._plot_x, dtype=float)
            net_values_arr = np.asarray(net_values, dtype=float)
            self.ax.plot(plot_x, net_values_arr, "b-", linewidth=2, label="净值曲线")
            if bench_values is not None and len(bench_values) == len(net_values):
                bench_values_arr = np.asarray(bench_values, dtype=float)
                self.ax.plot(
                    plot_x, bench_values_arr, color="#9c27b0", linewidth=1.6, label="基准净值"
                )

            self.ax.axhline(y=1.0, color="r", linestyle="--", alpha=0.7, label="基准线")
            running_max = pd.Series(net_values_arr).cummax().values
            running_max_arr = np.asarray(running_max, dtype=float)
            self._plot_drawdown = net_values_arr / running_max_arr - 1.0
            self.ax.fill_between(
                plot_x,
                net_values_arr,
                running_max_arr,
                where=(running_max_arr > net_values_arr).tolist(),
                color="#64b5f6",
                alpha=0.18,
                label="回撤区间",
            )
            self.ax.fill_between(
                plot_x,
                net_values_arr,
                1.0,
                where=(net_values_arr >= 1.0).tolist(),
                color="#ef9a9a",
                alpha=0.12,
                label="上行区间",
            )

            self.ax.set_title(
                "投资组合净值曲线",
                fontsize=self._font_size + 2,
                fontweight="bold",
                fontfamily=self._font_family,
            )
            self.ax.set_xlabel("日期", fontfamily=self._font_family, fontsize=self._font_size)
            self.ax.set_ylabel("净值", fontfamily=self._font_family, fontsize=self._font_size)
            self.ax.grid(True, alpha=0.3)
            self.ax.set_aspect("auto")
            y_min, y_max, pad = self._compute_y_limits(net_values, bench_values)
            if self._autoscale:
                x_start = line_dates[0]
                x_end = line_dates[-1]
                if x_start == x_end:
                    x_end = x_end + timedelta(days=1)
                x_start_num = mdates.date2num(x_start)
                x_end_num = mdates.date2num(x_end)
                if self._x_lock and self._locked_xlim:
                    self.ax.set_xlim(self._locked_xlim)
                else:
                    self.ax.set_xlim(float(np.asarray(x_start_num).item()), float(np.asarray(x_end_num).item()))  # type: ignore[arg-type]
                if self._y_lock and self._locked_ylim:
                    self.ax.set_ylim(self._locked_ylim)
                else:
                    self.ax.set_ylim(max(0, y_min - pad), y_max + pad)
            elif self._locked_xlim and self._locked_ylim:
                self.ax.set_xlim(self._locked_xlim)
                self.ax.set_ylim(self._locked_ylim)
            self.ax.tick_params(axis="both", labelsize=self._font_size)
            self.ax.legend(prop={"family": self._font_family, "size": self._font_size})

            locator = mdates.AutoDateLocator(minticks=4, maxticks=8)
            self.ax.xaxis.set_major_locator(locator)
            self.ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))

            self.canvas.draw()
            self._plot_positions, self._plot_trade_notes = self._build_position_series(
                line_dates, trades
            )
            self._plot_holdings = self._align_holdings(line_dates, daily_holdings)

        except Exception as e:
            print(f"绘制净值曲线时出错: {e}")

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if MATPLOTLIB_AVAILABLE and hasattr(self, "canvas"):
            self.canvas.draw_idle()

    def set_zoom_lock(self, locked: bool):
        self._autoscale = not locked
        if locked and hasattr(self, "ax"):
            self._locked_xlim = self.ax.get_xlim()
            self._locked_ylim = self.ax.get_ylim()

    def reset_view(self):
        self._autoscale = True
        self._locked_xlim = None
        self._locked_ylim = None
        self._y_lock = False
        self._x_lock = False
        self._current_preset_name = ""
        self.refresh_plot()

    def center_view(self):
        if self._plot_x is None or self._plot_y is None:
            return
        line_dates = [mdates.num2date(x) for x in self._plot_x]
        y_min, y_max, _ = self._compute_y_limits(self._plot_y, self._plot_benchmark)
        half_range = max(abs(y_max - 1.0), abs(1.0 - y_min))
        pad = max(half_range * 1.1, 0.02)
        self._locked_xlim = (line_dates[0], line_dates[-1])
        self._locked_ylim = (max(0, 1.0 - pad), 1.0 + pad)
        self._autoscale = False
        self._y_lock = False
        self.refresh_plot()

    def fix_y_range(self):
        if self._plot_y is None:
            return
        if hasattr(self, "ax"):
            self._locked_ylim = self.ax.get_ylim()
        else:
            y_min, y_max, pad = self._compute_y_limits(self._plot_y, self._plot_benchmark)
            self._locked_ylim = (max(0, y_min - pad), y_max + pad)
        self._y_lock = True
        self._autoscale = True
        self.refresh_plot()

    def fix_x_range(self):
        if self._plot_x is None:
            return
        if hasattr(self, "ax"):
            self._locked_xlim = self.ax.get_xlim()
        else:
            self._locked_xlim = (self._plot_x[0], self._plot_x[-1])
        self._x_lock = True
        self._autoscale = True
        self.refresh_plot()

    def prev_window_view(self):
        if self._plot_x is None or len(self._plot_x) < 2:
            return
        start_idx, end_idx = self._resolve_window_indices()
        span = max(2, end_idx - start_idx)
        new_end = max(span, start_idx)
        new_start = max(0, new_end - span)
        self._apply_window_indices(new_start, new_end)

    def next_window_view(self):
        if self._plot_x is None or len(self._plot_x) < 2:
            return
        start_idx, end_idx = self._resolve_window_indices()
        span = max(2, end_idx - start_idx)
        new_start = min(len(self._plot_x) - span, end_idx)
        new_end = min(len(self._plot_x), new_start + span)
        self._apply_window_indices(new_start, new_end)

    def save_view_preset(self, name: str, tag: str = ""):
        if not name or not hasattr(self, "ax"):
            return
        self._view_presets[name] = {
            "xlim": self.ax.get_xlim(),
            "ylim": self.ax.get_ylim(),
            "y_lock": self._y_lock,
            "x_lock": self._x_lock,
            "tag": tag,
        }
        self._current_preset_name = name

    def apply_view_preset(self, name: str):
        preset = self._view_presets.get(name)
        if not preset:
            return
        self._locked_xlim = preset.get("xlim")
        self._locked_ylim = preset.get("ylim")
        self._y_lock = bool(preset.get("y_lock"))
        self._x_lock = bool(preset.get("x_lock"))
        self._autoscale = False
        self._current_preset_name = name
        self.refresh_plot()

    def export_view_presets(self):
        exported = {}
        for name, preset in self._view_presets.items():
            xlim = preset.get("xlim")
            ylim = preset.get("ylim")
            exported[name] = {
                "xlim": [float(xlim[0]), float(xlim[1])] if xlim else None,
                "ylim": [float(ylim[0]), float(ylim[1])] if ylim else None,
                "y_lock": bool(preset.get("y_lock")),
                "x_lock": bool(preset.get("x_lock")),
                "tag": str(preset.get("tag") or ""),
            }
        return exported

    def import_view_presets(self, presets: dict):
        if not isinstance(presets, dict):
            return
        cleaned = {}
        for name, preset in presets.items():
            if not isinstance(preset, dict):
                continue
            xlim = preset.get("xlim")
            ylim = preset.get("ylim")
            if isinstance(xlim, (list, tuple)) and len(xlim) == 2:
                xlim = (float(xlim[0]), float(xlim[1]))
            else:
                xlim = None
            if isinstance(ylim, (list, tuple)) and len(ylim) == 2:
                ylim = (float(ylim[0]), float(ylim[1]))
            else:
                ylim = None
            cleaned[name] = {
                "xlim": xlim,
                "ylim": ylim,
                "y_lock": bool(preset.get("y_lock")),
                "x_lock": bool(preset.get("x_lock")),
                "tag": str(preset.get("tag") or ""),
            }
        self._view_presets = cleaned

    def get_current_preset_name(self):
        return self._current_preset_name

    def get_preset_tag(self, name: str):
        preset = self._view_presets.get(name)
        if not preset:
            return ""
        return str(preset.get("tag") or "")

    def rename_view_preset(self, old_name: str, new_name: str):
        if not old_name or not new_name or old_name not in self._view_presets:
            return False
        if new_name in self._view_presets:
            return False
        self._view_presets[new_name] = self._view_presets.pop(old_name)
        if self._current_preset_name == old_name:
            self._current_preset_name = new_name
        return True

    def delete_view_preset(self, name: str):
        if name in self._view_presets:
            del self._view_presets[name]
            if self._current_preset_name == name:
                self._current_preset_name = ""
            return True
        return False

    def set_window_ratio(self, ratio: float):
        if ratio <= 0:
            return
        self._x_window_ratio = max(0.1, min(0.9, float(ratio)))

    def refresh_plot(self):
        last_plot = getattr(self, "_last_plot", None)
        if isinstance(last_plot, dict):
            self.plot_portfolio_curve(
                last_plot["dates"],
                last_plot["values"],
                last_plot["initial_value"],
                last_plot["benchmark"],
                last_plot["trades"],
                last_plot["daily_holdings"],
            )

    def _resolve_window_indices(self):
        x_vals = self._plot_x
        if x_vals is None:
            return 0, 0
        total = len(x_vals)
        span = max(2, int(total * self._x_window_ratio))
        if self._locked_xlim:
            x_min, x_max = self._locked_xlim
            start_idx = int(np.searchsorted(x_vals, x_min, side="left"))
            end_idx = int(np.searchsorted(x_vals, x_max, side="right"))
            if end_idx - start_idx < 2:
                start_idx = max(0, total - span)
                end_idx = total
        else:
            start_idx = max(0, total - span)
            end_idx = total
        return start_idx, end_idx

    def _apply_window_indices(self, start_idx: int, end_idx: int):
        x_vals = self._plot_x
        if x_vals is None:
            return
        if start_idx < 0 or end_idx > len(x_vals) or end_idx - start_idx < 2:
            return
        self._locked_xlim = (x_vals[start_idx], x_vals[end_idx - 1])
        self._x_lock = True
        self._autoscale = True
        self.refresh_plot()

    def _compute_y_limits(self, net_values, bench_values):
        y_candidates = list(net_values)
        if bench_values is not None and len(bench_values) == len(net_values):
            y_candidates += list(bench_values)
        y_array = np.array(y_candidates, dtype=float)
        y_array = y_array[np.isfinite(y_array)]
        if y_array.size < 2:
            return 0.95, 1.05, 0.02
        if y_array.size >= 5:
            y_min = float(np.quantile(y_array, 0.02))
            y_max = float(np.quantile(y_array, 0.98))
        else:
            y_min = float(np.min(y_array))
            y_max = float(np.max(y_array))
        y_min = min(y_min, 1.0)
        y_max = max(y_max, 1.0)
        pad = max((y_max - y_min) * 0.12, 0.015)
        return y_min, y_max, pad

    def _build_position_series(self, line_dates, trades):
        if not trades:
            return None, None
        trade_map = {}
        for trade in trades:
            if len(trade) < 4:
                continue
            date_str = trade[0]
            try:
                dt = pd.to_datetime(date_str).date()
            except Exception:
                continue
            trade_map.setdefault(dt, []).append(trade)
        positions = []
        notes = []
        current_pos = 0
        last_note = ""
        for dt in line_dates:
            d = dt.date()
            if d in trade_map:
                for trade in trade_map[d]:
                    action = trade[1]
                    size = float(trade[3]) if str(trade[3]).strip() != "" else 0
                    if action == "买入":
                        current_pos += size
                    elif action == "卖出":
                        current_pos -= size
                    price = trade[2]
                    pnl = trade[5] if len(trade) > 5 else ""
                    last_note = f"{action} {size:.0f} @ {price} {pnl}".strip()
            positions.append(current_pos)
            notes.append(last_note)
        return positions, notes

    def _align_holdings(self, line_dates, daily_holdings):
        if not daily_holdings:
            return None
        if len(daily_holdings) == len(line_dates):
            return daily_holdings
        holdings_map = {}
        for item in daily_holdings:
            try:
                d = pd.to_datetime(item.get("date")).date()
            except Exception:
                continue
            holdings_map[d] = item
        aligned = []
        for dt in line_dates:
            aligned.append(holdings_map.get(dt.date()))
        return aligned

    def on_mouse_move(self, event):
        if not MATPLOTLIB_AVAILABLE:
            return
        if event.inaxes != self.ax or self._plot_x is None or self._plot_y is None:
            if hasattr(self, "annot"):
                self.annot.set_visible(False)
                self.canvas.draw_idle()
            return
        x = event.xdata
        if x is None:
            return
        idx = int(np.argmin(np.abs(self._plot_x - x)))
        x_val = self._plot_x[idx]
        y_val = self._plot_y[idx]
        self.vline.set_xdata([x_val, x_val])
        self.hline.set_ydata([y_val, y_val])
        dt = mdates.num2date(x_val).strftime("%Y-%m-%d")
        pnl_pct = (y_val - 1.0) * 100
        dd_pct = float(self._plot_drawdown[idx] * 100) if self._plot_drawdown is not None else 0.0
        bench_val = self._plot_benchmark[idx] if self._plot_benchmark is not None else None
        pos_text = "N/A"
        mv_text = ""
        cost_text = ""
        fpnl_text = ""
        trade_text = ""
        if self._plot_positions is not None:
            pos_text = f"{self._plot_positions[idx]:.0f}"
        if getattr(self, "_plot_holdings", None) is not None:
            holdings = cast(list[dict[str, Any] | None], self._plot_holdings)
            holding = holdings[idx] if idx < len(holdings) else None  # type: ignore[index, arg-type]
            if holding:
                mv_text = f"市值:{holding.get('market_value', 0):,.2f}"
                cost_text = f"成本:{holding.get('cost_basis', 0):,.2f}"
                fpnl_text = f"浮动:{holding.get('floating_pnl', 0):,.2f}"
        if self._plot_trade_notes is not None:
            trade_text = self._plot_trade_notes[idx]
        self.annot.xy = (x_val, y_val)
        bench_text = f" | 基准:{bench_val:.4f}" if bench_val is not None else ""
        trade_line = f"\n最近交易: {trade_text}" if trade_text else ""
        hold_line = ""
        if mv_text or cost_text or fpnl_text:
            hold_line = f"\n{mv_text} | {cost_text} | {fpnl_text}"
        self.annot.set_text(
            f"{dt}  净值:{y_val:.4f} | 盈亏:{pnl_pct:+.2f}% | 回撤:{dd_pct:+.2f}%{bench_text}\n持仓:{pos_text}{hold_line}{trade_line}"
        )
        self.annot.set_visible(True)
        self.canvas.draw_idle()


class SparklineWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.data = []
        self.line_color = QColor("#4CAF50")
        self.setMinimumHeight(0)
        self.setMaximumHeight(16777215)

    def set_data(self, data: list[float], color: Optional[QColor] = None):
        self.data = data or []
        if color is not None:
            self.line_color = color
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        rect = self.rect()
        painter.fillRect(rect, QColor("#ffffff"))
        if len(self.data) < 2:
            return
        values = np.array(self.data, dtype=float)
        if np.all(np.isnan(values)):
            return
        values = np.nan_to_num(values, nan=float(np.nanmean(values)))
        min_v = float(np.min(values))
        max_v = float(np.max(values))
        span = max(max_v - min_v, 1e-9)
        pen = QPen(self.line_color, 1.2)
        painter.setPen(pen)
        w = rect.width()
        h = rect.height()
        points = []
        for i, v in enumerate(values):
            x = rect.left() + (i / (len(values) - 1)) * w
            y = rect.bottom() - ((v - min_v) / span) * h
            points.append((x, y))
        for i in range(1, len(points)):
            painter.drawLine(
                int(points[i - 1][0]), int(points[i - 1][1]), int(points[i][0]), int(points[i][1])
            )


class MetricInlineCard(QFrame):
    value_label: QLabel
    sparkline: SparklineWidget
    sparkline_color: QColor


class MetricCard(QFrame):
    value_label: QLabel


class MultiLineChart(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        if MATPLOTLIB_AVAILABLE:
            self.figure = Figure(figsize=(10, 4), dpi=100)
            self.canvas = FigureCanvas(self.figure)
            layout.addWidget(self.canvas)
            self.ax = self.figure.add_subplot(111)
            self.ax.grid(True, alpha=0.3)
            self.canvas.draw()
        else:
            placeholder = QLabel("图表已生成\n(需要安装matplotlib查看图表)")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            placeholder.setStyleSheet("""
                QLabel {
                    background-color: #f0f0f0;
                    border: 2px dashed #ccc;
                    border-radius: 8px;
                    padding: 20px;
                    font-size: 14px;
                    color: #666;
                }
            """)
            layout.addWidget(placeholder)

    def plot_series(self, series_map: dict[str, list[float]], title: str, ylabel: str):
        if not MATPLOTLIB_AVAILABLE:
            return
        self.ax.clear()
        if not series_map:
            self.ax.set_title(title, fontsize=12, fontweight="bold")
            self.ax.text(
                0.5, 0.5, "暂无数据", ha="center", va="center", transform=self.ax.transAxes
            )
            self.canvas.draw()
            return
        for label, values in series_map.items():
            if not values:
                continue
            self.ax.plot(range(len(values)), values, linewidth=1.8, label=str(label))
        self.ax.set_title(title, fontsize=12, fontweight="bold")
        self.ax.set_ylabel(ylabel)
        self.ax.set_xlabel("时间")
        self.ax.grid(True, alpha=0.3)
        self.ax.legend()
        self.figure.tight_layout()
        self.canvas.draw()


class BarChart(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        if MATPLOTLIB_AVAILABLE:
            self.figure = Figure(figsize=(10, 4), dpi=100)
            self.canvas = FigureCanvas(self.figure)
            layout.addWidget(self.canvas)
            self.ax = self.figure.add_subplot(111)
            self.ax.grid(True, axis="y", alpha=0.3)
            self.canvas.draw()
        else:
            placeholder = QLabel("图表已生成\n(需要安装matplotlib查看图表)")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            placeholder.setStyleSheet("""
                QLabel {
                    background-color: #f0f0f0;
                    border: 2px dashed #ccc;
                    border-radius: 8px;
                    padding: 20px;
                    font-size: 14px;
                    color: #666;
                }
            """)
            layout.addWidget(placeholder)

    def plot_bars(self, labels: list[str], values: list[float], title: str, ylabel: str):
        if not MATPLOTLIB_AVAILABLE:
            return
        self.ax.clear()
        if not values:
            self.ax.set_title(title, fontsize=12, fontweight="bold")
            self.ax.text(
                0.5, 0.5, "暂无数据", ha="center", va="center", transform=self.ax.transAxes
            )
            self.canvas.draw()
            return
        positions = range(len(values))
        self.ax.bar(positions, values, color="#4CAF50")
        self.ax.set_xticks(list(positions))
        self.ax.set_xticklabels(labels, rotation=0)
        self.ax.set_title(title, fontsize=12, fontweight="bold")
        self.ax.set_ylabel(ylabel)
        self.ax.set_xlabel("排名")
        self.ax.grid(True, axis="y", alpha=0.3)
        self.figure.tight_layout()
        self.canvas.draw()


class DataManagerInitThread(QThread):
    ready = pyqtSignal(object)
    error_occurred = pyqtSignal(str)

    def run(self):
        try:
            if DataManager is None:
                raise RuntimeError("DataManager不可用")
            manager = DataManager(defer_checks=True)
            self.ready.emit(manager)
        except Exception as e:
            self.error_occurred.emit(str(e))


class DataSourceRefreshThread(QThread):
    finished_refresh = pyqtSignal()

    def __init__(self, manager):
        super().__init__()
        self.manager = manager

    def run(self):
        try:
            if self.manager is not None:
                self.manager.refresh_source_status()
        finally:
            self.finished_refresh.emit()


class StrategyScanThread(QThread):
    ready = pyqtSignal(list)
    error_occurred = pyqtSignal(str)

    def run(self):
        try:
            records = BacktestWidget.scan_strategy_files()
            self.ready.emit(records)
        except Exception as e:
            self.error_occurred.emit(str(e))




# ---------------------------------------------------------------------------
# 模块级映射常量
# 与 adjust_combo / period_combo 的 addItems() 文字严格对应。
# 任何 UI 标签改动必须同步更新此处，否则 test_backtest_widget.py 契约测试会失败。
# ---------------------------------------------------------------------------
ADJUST_TEXT_MAP: dict[str, str] = {
    "不复权 (原始价格)": "none",
    "前复权 (短期回测)": "front",
    "后复权 (长期回测)": "back",
}
PERIOD_TEXT_MAP: dict[str, str] = {
    "日线(1d)": "1d",
    "60分钟(1h)": "1h",
    "30分钟(30m)": "30m",
    "15分钟(15m)": "15m",
    "5分钟(5m)": "5m",
    "1分钟(1m)": "1m",
}


class BacktestWidget(QWidget):
    """
    回测窗口主组件

    功能特性：
    1. 回测参数配置界面
    2. 实时回测进度显示
    3. 回测结果可视化
    4. HTML报告生成和导出
    5. 参数优化功能
    """
    total_return_card: MetricInlineCard
    annual_return_card: MetricInlineCard
    sharpe_card: MetricInlineCard
    drawdown_card: MetricInlineCard

    def __init__(self, parent=None):
        super().__init__(parent)
        self.backtest_worker = None
        self.current_results = None
        self.data_manager = None
        self._data_manager_thread = None
        self._source_refresh_thread = None
        self._strategy_scan_thread = None
        self._pending_preview_params = None
        self.strategy_registry = self.build_strategy_registry()
        self._last_backtest_engine_log: Optional[str] = None
        self.strategy_param_widgets = {}
        self.strategy_scan_records = []
        self.strategy_editor_path = None
        self._crosshair_last_t: float = 0.0

        self.init_ui()
        self.setup_connections()
        self._connect_events()
        self._init_data_manager_async()
        self._init_strategy_scan_async()
        self.update_connection_status()
        self.update_backtest_engine_status()

    def init_ui(self):
        """初始化用户界面"""
        self.setWindowTitle("📊 专业回测系统")
        self.setMinimumSize(0, 0)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # 主布局
        main_layout = QHBoxLayout(self)

        # 创建分割器
        splitter = QSplitter(Qt.Horizontal)
        splitter.setHandleWidth(6)
        splitter.setChildrenCollapsible(False)
        splitter.setOpaqueResize(True)
        self.main_splitter = splitter
        main_layout.addWidget(splitter)

        # 左侧参数配置面板
        left_panel = self.create_parameter_panel()
        left_panel.setMinimumWidth(360)
        splitter.addWidget(left_panel)

        # 右侧结果显示面板
        right_panel = self.create_results_panel()
        right_panel.setMinimumWidth(640)
        splitter.addWidget(right_panel)
        splitter.setCollapsible(0, False)
        splitter.setCollapsible(1, False)

        # 设置分割比例
        splitter.setSizes([400, 800])
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 2)
        self.load_splitter_state()
        splitter.splitterMoved.connect(self.save_splitter_state)

        # 应用样式
        self.apply_styles()

    def create_parameter_panel(self) -> QWidget:
        """创建参数配置面板"""
        panel = QWidget()
        layout = QVBoxLayout(panel)

        status_bar = self.create_top_status_bar()
        layout.addWidget(status_bar)

        # 标题
        title_label = QLabel("🔧 回测参数配置")
        title_label.setFont(QFont("Arial", 14, QFont.Bold))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_label)

        basic_group = self.create_basic_params_group()
        layout.addWidget(basic_group)

        strategy_group = self.create_strategy_params_group()
        layout.addWidget(strategy_group)

        # 控制按钮
        control_group = self.create_control_buttons()
        layout.addWidget(control_group)

        layout.addStretch()
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setWidget(panel)
        return scroll

    def create_top_status_bar(self) -> QWidget:
        bar = QFrame()
        bar.setFrameStyle(QFrame.Box)
        bar.setStyleSheet("""
            QFrame {
                background-color: #ffffff;
                border: 1px solid #e0e0e0;
                border-radius: 8px;
            }
        """)
        layout = QHBoxLayout(bar)
        layout.setContentsMargins(8, 4, 8, 4)
        layout.setSpacing(10)
        self.data_status_label = QLabel("数据: 未加载")
        self.data_status_label.setStyleSheet("color: #666; font-weight: bold;")
        self.backtest_engine_label = QLabel("引擎: 检测中...")
        self.backtest_engine_label.setStyleSheet("color: #666; font-weight: bold;")
        self.backtest_engine_label.setToolTip("点击查看回测引擎状态详情")
        cast(Any, self.backtest_engine_label).mousePressEvent = self.on_backtest_engine_label_clicked
        self.status_label = QLabel("💤 等待开始...")
        self.status_label.setStyleSheet("color: #333;")
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setMinimumHeight(0)
        self.progress_bar.setTextVisible(False)
        layout.addWidget(self.data_status_label, 2)
        layout.addWidget(self.backtest_engine_label, 2)
        layout.addWidget(self.status_label, 3)
        layout.addWidget(self.progress_bar, 2)
        return bar

    def create_basic_params_group(self) -> QGroupBox:
        """创建基础参数组"""
        group = QGroupBox("📊 基础参数")
        layout = QGridLayout(group)

        # 股票代码
        layout.addWidget(QLabel("股票代码:"), 0, 0)
        self.stock_code_edit = QLineEdit("000001.SZ")
        layout.addWidget(self.stock_code_edit, 0, 1)

        layout.addWidget(QLabel("参数预设:"), 0, 2)
        self.preset_combo = QComboBox()
        self.preset_combo.addItems(["自定义", "短期趋势", "中期趋势", "均值回归", "稳健低频"])
        layout.addWidget(self.preset_combo, 0, 3)
        self.apply_preset_btn = QPushButton("应用预设")
        self.apply_preset_btn.clicked.connect(self.apply_parameter_preset)
        layout.addWidget(self.apply_preset_btn, 0, 4)

        # 开始日期
        layout.addWidget(QLabel("开始日期:"), 1, 0)
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setDate(QDate.currentDate().addYears(-1))
        self.start_date_edit.setCalendarPopup(True)
        layout.addWidget(self.start_date_edit, 1, 1)

        layout.addWidget(QLabel("K线周期:"), 1, 2)
        self.period_combo = QComboBox()
        self.period_combo.addItems(
            ["日线(1d)", "60分钟(1h)", "30分钟(30m)", "15分钟(15m)", "5分钟(5m)", "1分钟(1m)"]
        )
        self.period_combo.currentTextChanged.connect(self.update_period_hint)
        layout.addWidget(self.period_combo, 1, 3)

        self.period_hint_label = QLabel("")
        self.period_hint_label.setStyleSheet("color: #FF9800;")
        layout.addWidget(self.period_hint_label, 2, 2, 1, 3)
        self.update_period_hint(self.period_combo.currentText())

        # 结束日期
        layout.addWidget(QLabel("结束日期:"), 2, 0)
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setDate(QDate.currentDate())
        self.end_date_edit.setCalendarPopup(True)
        layout.addWidget(self.end_date_edit, 2, 1)

        # 初始资金
        layout.addWidget(QLabel("初始资金:"), 3, 0)
        self.initial_cash_spin = QDoubleSpinBox()
        self.initial_cash_spin.setRange(10000, 10000000)
        self.initial_cash_spin.setValue(100000)
        self.initial_cash_spin.setSuffix(" 元")
        layout.addWidget(self.initial_cash_spin, 3, 1)

        # 手续费率
        layout.addWidget(QLabel("手续费率:"), 4, 0)
        self.commission_spin = QDoubleSpinBox()
        self.commission_spin.setRange(0.0001, 0.01)
        self.commission_spin.setValue(0.001)
        self.commission_spin.setDecimals(4)
        self.commission_spin.setSuffix("%")
        layout.addWidget(self.commission_spin, 4, 1)

        # 复权类型选择（第4行第2列）
        layout.addWidget(QLabel("复权类型:"), 4, 2)
        self.adjust_combo = QComboBox()
        self.adjust_combo.addItems(["不复权 (原始价格)", "前复权 (短期回测)", "后复权 (长期回测)"])
        self.adjust_combo.setCurrentIndex(0)
        self.adjust_combo.setToolTip(
            "不复权：实时交易\n"
            "前复权：当前价真实，适合短期回测（1年内）\n"
            "后复权：历史价真实，适合长期回测（3年以上）"
        )
        layout.addWidget(self.adjust_combo, 4, 3)

        # 数据源选择（第5行）
        layout.addWidget(QLabel("数据源选择:"), 5, 0)
        self.data_source_combo = QComboBox()
        self.data_source_combo.addItems(
            [
                "自动选择 (DuckDB→本地缓存→QMT→QStock→AKShare)",
                "强制QMT",
                "强制QStock",
                "强制AKShare",
                "强制DuckDB",
                "强制本地缓存",
            ]
        )
        self.data_source_combo.currentTextChanged.connect(self.on_data_source_changed)
        layout.addWidget(self.data_source_combo, 5, 1)

        # 数据源状态（第5行第2列）
        layout.addWidget(QLabel("数据源状态:"), 5, 2)
        self.data_source_label = QLabel("检测中...")
        self.data_source_label.setStyleSheet("color: orange; font-weight: bold;")
        layout.addWidget(self.data_source_label, 5, 3)

        self.refresh_connection_btn = QPushButton("📥 加载")
        self.refresh_connection_btn.clicked.connect(self.load_data_preview)
        self.refresh_connection_btn.setToolTip("加载数据并生成摘要")
        layout.addWidget(self.refresh_connection_btn, 5, 4)

        return group

    def create_strategy_params_group(self) -> QGroupBox:
        group = QGroupBox("🎯 策略管理")
        layout = QGridLayout(group)

        layout.addWidget(QLabel("策略类型:"), 0, 0)
        self.strategy_combo = QComboBox()
        self.strategy_combo.addItems(list(self.strategy_registry.keys()))
        layout.addWidget(self.strategy_combo, 0, 1)

        layout.addWidget(QLabel("回测支持:"), 0, 2)
        self.strategy_status_label = QLabel("")
        layout.addWidget(self.strategy_status_label, 0, 3)

        self.refresh_strategy_list_btn = QPushButton("🔄 刷新清单")
        layout.addWidget(self.refresh_strategy_list_btn, 0, 4)

        self.strategy_tabs = QTabWidget()
        layout.addWidget(self.strategy_tabs, 1, 0, 1, 5)

        params_tab = QWidget()
        params_layout = QVBoxLayout(params_tab)
        self.strategy_param_stack = QStackedWidget()
        params_layout.addWidget(self.strategy_param_stack)

        for name, cfg in self.strategy_registry.items():
            form_widget = self.create_strategy_param_form(name, cfg)
            self.strategy_param_stack.addWidget(form_widget)

        self.strategy_tabs.addTab(params_tab, "策略参数")

        catalog_tab = QWidget()
        catalog_layout = QVBoxLayout(catalog_tab)
        self.strategy_catalog_table = QTableWidget()
        self.strategy_catalog_table.setColumnCount(4)
        self.strategy_catalog_table.setHorizontalHeaderLabels(
            ["策略名称", "类型", "文件", "回测支持"]
        )
        catalog_header = self.strategy_catalog_table.horizontalHeader()
        if catalog_header is not None:
            catalog_header.setSectionResizeMode(2, QHeaderView.Stretch)
        self.strategy_catalog_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.strategy_catalog_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        catalog_layout.addWidget(self.strategy_catalog_table)
        self.strategy_tabs.addTab(catalog_tab, "策略清单")

        editor_tab = QWidget()
        editor_layout = QVBoxLayout(editor_tab)
        editor_toolbar = QHBoxLayout()
        self.strategy_load_btn = QPushButton("导入策略文件")
        self.strategy_save_btn = QPushButton("保存策略")
        self.strategy_validate_btn = QPushButton("校验代码")
        editor_toolbar.addWidget(self.strategy_load_btn)
        editor_toolbar.addWidget(self.strategy_save_btn)
        editor_toolbar.addWidget(self.strategy_validate_btn)
        editor_toolbar.addStretch()
        editor_layout.addLayout(editor_toolbar)
        self.strategy_editor = QTextEdit()
        self.strategy_editor.setFont(QFont("Consolas", 10))
        editor_layout.addWidget(self.strategy_editor)
        self.strategy_tabs.addTab(editor_tab, "策略编辑")

        advanced_tab = QWidget()
        advanced_layout = QGridLayout(advanced_tab)

        self.optimize_checkbox = QCheckBox("启用参数优化")
        advanced_layout.addWidget(self.optimize_checkbox, 0, 0, 1, 2)

        self.benchmark_checkbox = QCheckBox("基准比较")
        advanced_layout.addWidget(self.benchmark_checkbox, 1, 0, 1, 2)

        self.risk_analysis_checkbox = QCheckBox("详细风险分析")
        self.risk_analysis_checkbox.setChecked(True)
        advanced_layout.addWidget(self.risk_analysis_checkbox, 2, 0, 1, 2)

        self.walk_forward_checkbox = QCheckBox("Walk-Forward 分段回测")
        advanced_layout.addWidget(self.walk_forward_checkbox, 3, 0, 1, 2)

        advanced_layout.addWidget(QLabel("样本外比例:"), 4, 0)
        self.oos_ratio_spin = QDoubleSpinBox()
        self.oos_ratio_spin.setRange(0.1, 0.5)
        self.oos_ratio_spin.setSingleStep(0.05)
        self.oos_ratio_spin.setValue(0.2)
        self.oos_ratio_spin.setSuffix("")
        advanced_layout.addWidget(self.oos_ratio_spin, 4, 1)

        advanced_layout.addWidget(QLabel("窗口长度(天):"), 4, 2)
        self.wf_window_spin = QSpinBox()
        self.wf_window_spin.setRange(60, 2000)
        self.wf_window_spin.setValue(252)
        advanced_layout.addWidget(self.wf_window_spin, 4, 3)

        advanced_layout.addWidget(QLabel("步长(天):"), 4, 4)
        self.wf_step_spin = QSpinBox()
        self.wf_step_spin.setRange(20, 500)
        self.wf_step_spin.setValue(60)
        advanced_layout.addWidget(self.wf_step_spin, 4, 5)

        self.optimize_method_combo = QComboBox()
        self.optimize_method_combo.addItems(["网格搜索", "贝叶斯优化", "随机搜索"])
        advanced_layout.addWidget(QLabel("参数优化方法:"), 5, 0)
        advanced_layout.addWidget(self.optimize_method_combo, 5, 1)

        advanced_layout.addWidget(QLabel("优化次数:"), 5, 2)
        self.optimize_trials_spin = QSpinBox()
        self.optimize_trials_spin.setRange(10, 500)
        self.optimize_trials_spin.setValue(50)
        advanced_layout.addWidget(self.optimize_trials_spin, 5, 3)

        self.overfit_warning_checkbox = QCheckBox("过拟合警戒")
        self.overfit_warning_checkbox.setChecked(True)
        advanced_layout.addWidget(self.overfit_warning_checkbox, 6, 0, 1, 2)

        advanced_layout.addWidget(QLabel("夏普下降阈值:"), 6, 2)
        self.overfit_sharpe_drop_spin = QDoubleSpinBox()
        self.overfit_sharpe_drop_spin.setRange(0.05, 0.8)
        self.overfit_sharpe_drop_spin.setSingleStep(0.05)
        self.overfit_sharpe_drop_spin.setValue(0.3)
        advanced_layout.addWidget(self.overfit_sharpe_drop_spin, 6, 3)

        advanced_layout.addWidget(QLabel("回撤放大阈值:"), 6, 4)
        self.overfit_drawdown_increase_spin = QDoubleSpinBox()
        self.overfit_drawdown_increase_spin.setRange(0.05, 1.0)
        self.overfit_drawdown_increase_spin.setSingleStep(0.05)
        self.overfit_drawdown_increase_spin.setValue(0.2)
        advanced_layout.addWidget(self.overfit_drawdown_increase_spin, 6, 5)

        advanced_layout.addWidget(QLabel("固定网格搜索空间:"), 7, 0)
        self.fixed_grid_space_edit = QLineEdit()
        self.fixed_grid_space_edit.setPlaceholderText(
            "grid_count=8,12,16; price_range=0.02,0.05; position_size=500,1000"
        )
        advanced_layout.addWidget(self.fixed_grid_space_edit, 7, 1, 1, 5)

        advanced_layout.addWidget(QLabel("自适应网格搜索空间:"), 8, 0)
        self.adaptive_grid_space_edit = QLineEdit()
        self.adaptive_grid_space_edit.setPlaceholderText(
            "buy_threshold=0.005,0.01; sell_threshold=0.005,0.01; position_size=500,1000; max_position=5000,10000"
        )
        advanced_layout.addWidget(self.adaptive_grid_space_edit, 8, 1, 1, 5)

        advanced_layout.addWidget(QLabel("ATR网格搜索空间:"), 9, 0)
        self.atr_grid_space_edit = QLineEdit()
        self.atr_grid_space_edit.setPlaceholderText(
            "atr_period=14,28,60; atr_multiplier=2,4,6; position_size=500,1000"
        )
        advanced_layout.addWidget(self.atr_grid_space_edit, 9, 1, 1, 5)

        advanced_layout.addWidget(QLabel("交易成本模型:"), 10, 0)
        self.cost_model_combo = QComboBox()
        self.cost_model_combo.addItems(["固定佣金", "阶梯佣金", "冲击成本"])
        advanced_layout.addWidget(self.cost_model_combo, 10, 1)

        advanced_layout.addWidget(QLabel("滑点(bps):"), 10, 2)
        self.slippage_spin = QDoubleSpinBox()
        self.slippage_spin.setRange(0.0, 50.0)
        self.slippage_spin.setValue(5.0)
        advanced_layout.addWidget(self.slippage_spin, 10, 3)

        advanced_layout.addWidget(QLabel("仓位控制:"), 11, 0)
        self.position_sizing_combo = QComboBox()
        self.position_sizing_combo.addItems(["固定比例", "波动率目标", "风险平价"])
        advanced_layout.addWidget(self.position_sizing_combo, 11, 1)

        advanced_layout.addWidget(QLabel("单票上限(%):"), 11, 2)
        self.max_position_spin = QDoubleSpinBox()
        self.max_position_spin.setRange(1.0, 30.0)
        self.max_position_spin.setValue(30.0)
        advanced_layout.addWidget(self.max_position_spin, 11, 3)

        advanced_layout.addWidget(QLabel("再平衡周期:"), 11, 4)
        self.rebalance_combo = QComboBox()
        self.rebalance_combo.addItems(["日频", "周频", "月频"])
        advanced_layout.addWidget(self.rebalance_combo, 11, 5)

        self.multi_asset_checkbox = QCheckBox("多标的组合回测")
        advanced_layout.addWidget(self.multi_asset_checkbox, 12, 0, 1, 2)

        advanced_layout.addWidget(QLabel("标的列表:"), 12, 2)
        self.multi_asset_edit = QLineEdit()
        self.multi_asset_edit.setPlaceholderText("000001.SZ, 600000.SH")
        advanced_layout.addWidget(self.multi_asset_edit, 12, 3, 1, 3)

        self.strategy_tabs.addTab(advanced_tab, "优化与风控")

        self.refresh_strategy_catalog_table()
        self.on_strategy_changed(self.strategy_combo.currentText())

        return group

    def build_strategy_registry(self) -> dict[str, Any]:
        registry = {}
        registry["双均线策略"] = {
            "class": DualMovingAverageStrategy,
            "params": [
                {
                    "key": "short_period",
                    "label": "短期均线",
                    "type": "int",
                    "min": 3,
                    "max": 200,
                    "value": 5,
                },
                {
                    "key": "long_period",
                    "label": "长期均线",
                    "type": "int",
                    "min": 10,
                    "max": 400,
                    "value": 20,
                },
                {
                    "key": "rsi_period",
                    "label": "RSI周期",
                    "type": "int",
                    "min": 5,
                    "max": 50,
                    "value": 14,
                },
            ],
        }
        registry["RSI策略"] = {
            "class": RSIStrategy,
            "params": [
                {
                    "key": "rsi_period",
                    "label": "RSI周期",
                    "type": "int",
                    "min": 5,
                    "max": 50,
                    "value": 14,
                }
            ],
        }
        registry["MACD策略"] = {
            "class": MACDStrategy,
            "params": [
                {
                    "key": "fast_period",
                    "label": "快线周期",
                    "type": "int",
                    "min": 3,
                    "max": 50,
                    "value": 12,
                },
                {
                    "key": "slow_period",
                    "label": "慢线周期",
                    "type": "int",
                    "min": 10,
                    "max": 200,
                    "value": 26,
                },
                {
                    "key": "signal_period",
                    "label": "信号周期",
                    "type": "int",
                    "min": 3,
                    "max": 50,
                    "value": 9,
                },
            ],
        }
        registry["固定网格策略"] = {
            "class": GridStrategy,
            "params": [
                {
                    "key": "grid_count",
                    "label": "网格数量",
                    "type": "int",
                    "min": 5,
                    "max": 50,
                    "value": 15,
                },
                {
                    "key": "price_range",
                    "label": "价格区间",
                    "type": "float",
                    "min": 0.01,
                    "max": 0.2,
                    "value": 0.05,
                    "step": 0.005,
                },
                {
                    "key": "position_size",
                    "label": "每格数量",
                    "type": "int",
                    "min": 100,
                    "max": 20000,
                    "value": 1000,
                },
                {"key": "base_price", "label": "基准价格", "type": "optional_float"},
                {"key": "enable_trailing", "label": "动态调整", "type": "bool", "value": True},
                {
                    "key": "trailing_period",
                    "label": "调整周期",
                    "type": "int",
                    "min": 1,
                    "max": 60,
                    "value": 5,
                },
            ],
        }
        registry["自适应网格策略"] = {
            "class": AdaptiveGridStrategy,
            "params": [
                {
                    "key": "buy_threshold",
                    "label": "买入阈值",
                    "type": "float",
                    "min": 0.001,
                    "max": 0.1,
                    "value": 0.01,
                    "step": 0.001,
                },
                {
                    "key": "sell_threshold",
                    "label": "卖出阈值",
                    "type": "float",
                    "min": 0.001,
                    "max": 0.1,
                    "value": 0.01,
                    "step": 0.001,
                },
                {
                    "key": "position_size",
                    "label": "每次数量",
                    "type": "int",
                    "min": 100,
                    "max": 20000,
                    "value": 1000,
                },
                {"key": "base_price", "label": "基准价格", "type": "optional_float"},
                {
                    "key": "max_position",
                    "label": "最大持仓",
                    "type": "int",
                    "min": 1000,
                    "max": 500000,
                    "value": 10000,
                },
            ],
        }
        registry["ATR网格策略"] = {
            "class": ATRGridStrategy,
            "params": [
                {
                    "key": "atr_period",
                    "label": "ATR周期",
                    "type": "int",
                    "min": 14,
                    "max": 600,
                    "value": 300,
                },
                {
                    "key": "atr_multiplier",
                    "label": "ATR倍数",
                    "type": "float",
                    "min": 0.5,
                    "max": 20.0,
                    "value": 6.0,
                    "step": 0.5,
                },
                {
                    "key": "position_size",
                    "label": "每次数量",
                    "type": "int",
                    "min": 100,
                    "max": 20000,
                    "value": 1000,
                },
                {"key": "base_price", "label": "基准价格", "type": "optional_float"},
                {"key": "enable_trailing", "label": "动态调整", "type": "bool", "value": True},
                {
                    "key": "trailing_period",
                    "label": "调整周期",
                    "type": "int",
                    "min": 1,
                    "max": 120,
                    "value": 20,
                },
            ],
        }
        registry["101因子工作流策略"] = {
            "class": object,
            "params": [
                {
                    "key": "workflow_path",
                    "label": "工作流文件路径",
                    "type": "path",
                    "value": "101因子/101因子分析平台/workflow.json",
                },
                {
                    "key": "workflow_node_overrides",
                    "label": "节点参数覆盖(JSON)",
                    "type": "json",
                    "value": {
                        "backtester": {"n_quantiles": 5},
                        "ic_analyzer": {"periods": 1},
                    },
                }
            ],
        }
        return registry

    @staticmethod
    def scan_strategy_files() -> list[dict[str, str]]:
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        strategies_dir = os.path.join(root_dir, "strategies")
        records: list[dict[str, str]] = []
        if not os.path.isdir(strategies_dir):
            return records
        for dirpath, _, filenames in os.walk(strategies_dir):
            if any(key in dirpath for key in ["_backup", "external", "_seed", "legacy"]):
                continue
            for filename in filenames:
                if not filename.endswith(".py"):
                    continue
                if filename.startswith("__"):
                    continue
                file_path = os.path.join(dirpath, filename)
                try:
                    with open(file_path, encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                except Exception:
                    continue
                for match in re.finditer(r"class\\s+(\\w+)\\s*\\(([^)]*)\\)", content):
                    class_name = match.group(1)
                    base_text = match.group(2)
                    if "BaseStrategy" in base_text:
                        stype = "BaseStrategy"
                    elif "bt.Strategy" in base_text or "backtrader" in base_text:
                        stype = "Backtrader"
                    else:
                        if "Strategy" not in class_name:
                            continue
                        stype = "未知"
                    records.append({"name": class_name, "type": stype, "file": file_path})
        return records

    def refresh_strategy_catalog_table(self):
        records = []
        source_map = {
            "双均线策略": os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "backtest", "engine.py")
            ),
            "RSI策略": os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "backtest", "engine.py")
            ),
            "MACD策略": os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "backtest", "engine.py")
            ),
            "固定网格策略": os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__), "..", "..", "strategies", "grid_strategy_511380.py"
                )
            ),
            "自适应网格策略": os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__), "..", "..", "strategies", "grid_strategy_511380.py"
                )
            ),
            "ATR网格策略": os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__), "..", "..", "strategies", "grid_strategy_511380.py"
                )
            ),
        }
        for name, cfg in self.strategy_registry.items():
            records.append(
                {
                    "name": name,
                    "type": "Backtrader",
                    "file": source_map.get(name, ""),
                    "enabled": cfg.get("class") is not None,
                }
            )
        registry_names = set(self.strategy_registry.keys())
        for item in self.strategy_scan_records:
            if item["name"] in registry_names:
                continue
            records.append(
                {"name": item["name"], "type": item["type"], "file": item["file"], "enabled": False}
            )
        self.strategy_catalog_table.setRowCount(len(records))
        for row, item in enumerate(records):
            self.strategy_catalog_table.setItem(row, 0, QTableWidgetItem(item["name"]))
            self.strategy_catalog_table.setItem(row, 1, QTableWidgetItem(item["type"]))
            self.strategy_catalog_table.setItem(row, 2, QTableWidgetItem(item["file"]))
            self.strategy_catalog_table.setItem(
                row, 3, QTableWidgetItem("是" if item["enabled"] else "否")
            )

    def create_strategy_param_form(self, name: str, cfg: dict[str, Any]) -> QWidget:
        form_widget = QWidget()
        layout = QGridLayout(form_widget)
        self.strategy_param_widgets[name] = {}
        row = 0
        for param in cfg.get("params", []):
            label = QLabel(f"{param['label']}:")
            layout.addWidget(label, row, 0)
            widget: Any
            if param["type"] == "int":
                widget = QSpinBox()
                widget.setRange(param.get("min", -(10**9)), param.get("max", 10**9))
                widget.setValue(param.get("value", 0))
            elif param["type"] == "float":
                widget = QDoubleSpinBox()
                widget.setRange(param.get("min", -1e9), param.get("max", 1e9))
                widget.setSingleStep(param.get("step", 0.01))
                widget.setDecimals(6)
                widget.setValue(param.get("value", 0.0))
            elif param["type"] == "bool":
                widget = QCheckBox()
                widget.setChecked(bool(param.get("value", False)))
            elif param["type"] == "optional_float":
                widget = QLineEdit()
                widget.setPlaceholderText("留空使用首日收盘价")
                validator = QDoubleValidator()
                validator.setBottom(0)
                widget.setValidator(validator)
            elif param["type"] == "path":
                box = QWidget()
                box_layout = QHBoxLayout(box)
                box_layout.setContentsMargins(0, 0, 0, 0)
                box_layout.setSpacing(6)
                line = QLineEdit()
                line.setText(str(param.get("value", "")))
                line.setPlaceholderText("请选择工作流文件")
                btn = QPushButton("浏览")
                btn.setFixedWidth(64)

                def _pick_file(target: QLineEdit = line) -> None:
                    filename, _ = QFileDialog.getOpenFileName(
                        self,
                        "选择工作流文件",
                        target.text().strip() or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
                        "JSON Files (*.json);;All Files (*)",
                    )
                    if filename:
                        try:
                            rel = os.path.relpath(
                                filename,
                                os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
                            )
                            target.setText(rel if not rel.startswith("..") else filename)
                        except Exception:
                            target.setText(filename)

                btn.clicked.connect(_pick_file)
                box_layout.addWidget(line)
                box_layout.addWidget(btn)
                widget = box
                self.strategy_param_widgets[name][param["key"]] = line
                layout.addWidget(widget, row, 1)
                row += 1
                continue
            elif param["type"] == "json":
                box = QWidget()
                box_layout = QVBoxLayout(box)
                box_layout.setContentsMargins(0, 0, 0, 0)
                box_layout.setSpacing(6)
                widget = QTextEdit()
                widget.setFixedHeight(88)
                default_v = param.get("value", {})
                if isinstance(default_v, dict):
                    widget.setPlainText(json.dumps(default_v, ensure_ascii=False, indent=2))
                else:
                    widget.setPlainText(str(default_v))
                btn = QPushButton("节点编辑")
                btn.setFixedWidth(90)
                scenario_btn = QPushButton("场景管理")
                scenario_btn.setFixedWidth(90)

                def _open_node_editor(
                    _checked: bool = False,
                    target_json: QTextEdit = widget,
                    target_strategy: str = name,
                ) -> None:
                    params_widgets = self.strategy_param_widgets.get(target_strategy, {})
                    workflow_widget = params_widgets.get("workflow_path")
                    workflow_path = workflow_widget.text().strip() if isinstance(workflow_widget, QLineEdit) else ""
                    workflow_path = self._resolve_workflow_path_for_ui(workflow_path)
                    node_templates = self._read_workflow_node_templates(workflow_path)
                    if not node_templates:
                        QMessageBox.warning(self, "节点编辑", f"无法读取工作流节点参数: {workflow_path}")
                        return
                    current_text = target_json.toPlainText().strip()
                    current_overrides: dict[str, Any] = {}
                    if current_text:
                        try:
                            parsed = json.loads(current_text)
                            if isinstance(parsed, dict):
                                current_overrides = parsed
                        except Exception:
                            current_overrides = {}
                    dialog = QDialog(self)
                    dialog.setWindowTitle("工作流节点参数编辑")
                    dialog.resize(700, 480)
                    d_layout = QVBoxLayout(dialog)
                    tabs = QTabWidget()
                    d_layout.addWidget(tabs)
                    fields: dict[tuple[str, str], QLineEdit] = {}
                    for node_type in sorted(node_templates.keys()):
                        tab = QWidget()
                        form = QFormLayout(tab)
                        merged = dict(node_templates.get(node_type, {}))
                        ov = current_overrides.get(node_type, {})
                        if isinstance(ov, dict):
                            merged.update(ov)
                        for k, v in merged.items():
                            line = QLineEdit()
                            line.setText(self._format_node_param_value(v))
                            form.addRow(str(k), line)
                            fields[(node_type, str(k))] = line
                        tabs.addTab(tab, node_type)
                    btn_row = QHBoxLayout()
                    save_btn = QPushButton("保存")
                    cancel_btn = QPushButton("取消")
                    btn_row.addStretch(1)
                    btn_row.addWidget(save_btn)
                    btn_row.addWidget(cancel_btn)
                    d_layout.addLayout(btn_row)

                    def _save() -> None:
                        payload: dict[str, dict[str, Any]] = {}
                        for (n_type, p_key), editor in fields.items():
                            payload.setdefault(n_type, {})
                            payload[n_type][p_key] = self._parse_node_param_text(editor.text().strip())
                        target_json.setPlainText(json.dumps(payload, ensure_ascii=False, indent=2))
                        dialog.accept()

                    save_btn.clicked.connect(_save)
                    cancel_btn.clicked.connect(dialog.reject)
                    dialog.exec_()

                def _open_scenario_editor(
                    _checked: bool = False,
                    target_json: QTextEdit = widget,
                    target_strategy: str = name,
                ) -> None:
                    params_widgets = self.strategy_param_widgets.get(target_strategy, {})
                    workflow_widget = params_widgets.get("workflow_path")
                    workflow_path = workflow_widget.text().strip() if isinstance(workflow_widget, QLineEdit) else ""
                    workflow_path = self._resolve_workflow_path_for_ui(workflow_path)
                    node_templates = self._read_workflow_node_templates(workflow_path)
                    if not node_templates:
                        QMessageBox.warning(self, "场景管理", f"无法读取工作流节点参数: {workflow_path}")
                        return
                    current_text = target_json.toPlainText().strip()
                    current_overrides: dict[str, Any] = {}
                    if current_text:
                        try:
                            parsed = json.loads(current_text)
                            if isinstance(parsed, dict):
                                current_overrides = parsed
                        except Exception:
                            current_overrides = {}
                    dialog = QDialog(self)
                    dialog.setWindowTitle("场景参数管理")
                    dialog.resize(860, 520)
                    d_layout = QVBoxLayout(dialog)
                    tips = QLabel("通过表格维护场景，无需手写 __scenarios__")
                    d_layout.addWidget(tips)
                    table = QTableWidget()
                    table.setColumnCount(4)
                    table.setHorizontalHeaderLabels(["场景名", "节点类型", "参数键", "参数值"])
                    table.horizontalHeader().setStretchLastSection(True)
                    d_layout.addWidget(table)
                    rows = self._extract_scenarios_from_overrides(current_overrides)
                    table.setRowCount(len(rows))
                    for i, r in enumerate(rows):
                        table.setItem(i, 0, QTableWidgetItem(r.get("name", "")))
                        table.setItem(i, 1, QTableWidgetItem(r.get("node_type", "")))
                        table.setItem(i, 2, QTableWidgetItem(r.get("param_key", "")))
                        table.setItem(i, 3, QTableWidgetItem(r.get("param_value", "")))
                    action = QHBoxLayout()
                    add_btn = QPushButton("新增行")
                    del_btn = QPushButton("删除行")
                    action.addWidget(add_btn)
                    action.addWidget(del_btn)
                    action.addStretch(1)
                    d_layout.addLayout(action)
                    tpl_box = QWidget()
                    tpl_layout = QGridLayout(tpl_box)
                    tpl_layout.setContentsMargins(0, 0, 0, 0)
                    tpl_layout.addWidget(QLabel("模板生成"), 0, 0)
                    tpl_layout.addWidget(QLabel("节点"), 0, 1)
                    tpl_layout.addWidget(QLabel("参数"), 0, 2)
                    tpl_layout.addWidget(QLabel("起始"), 0, 3)
                    tpl_layout.addWidget(QLabel("结束"), 0, 4)
                    tpl_layout.addWidget(QLabel("步长"), 0, 5)
                    tpl_layout.addWidget(QLabel("前缀"), 0, 6)
                    tpl_node = QComboBox()
                    tpl_node.addItems(sorted(node_templates.keys()))
                    tpl_param = QComboBox()
                    tpl_start = QLineEdit("5")
                    tpl_end = QLineEdit("20")
                    tpl_step = QLineEdit("5")
                    tpl_prefix = QLineEdit("scenario")
                    tpl_make = QPushButton("一键生成")
                    tpl_replace = QCheckBox("清空后重建")
                    tpl_replace.setChecked(True)
                    tpl_layout.addWidget(tpl_node, 1, 1)
                    tpl_layout.addWidget(tpl_param, 1, 2)
                    tpl_layout.addWidget(tpl_start, 1, 3)
                    tpl_layout.addWidget(tpl_end, 1, 4)
                    tpl_layout.addWidget(tpl_step, 1, 5)
                    tpl_layout.addWidget(tpl_prefix, 1, 6)
                    tpl_layout.addWidget(tpl_make, 1, 7)
                    tpl_layout.addWidget(tpl_replace, 1, 8)
                    preset_row = QHBoxLayout()
                    preset_row.addWidget(QLabel("常用预设"))
                    preset_q = QPushButton("分层Q(5-20)")
                    preset_ic = QPushButton("IC周期(1-10)")
                    preset_row.addWidget(preset_q)
                    preset_row.addWidget(preset_ic)
                    preset_row.addStretch(1)
                    tpl_layout.addLayout(preset_row, 2, 1, 1, 7)
                    d_layout.addWidget(tpl_box)

                    def _refresh_param_combo() -> None:
                        node_type = tpl_node.currentText().strip()
                        keys = sorted((node_templates.get(node_type, {}) or {}).keys())
                        tpl_param.clear()
                        tpl_param.addItems([str(k) for k in keys])

                    _refresh_param_combo()
                    tpl_node.currentTextChanged.connect(_refresh_param_combo)

                    def _add_row() -> None:
                        row_idx = table.rowCount()
                        table.insertRow(row_idx)
                        default_node = sorted(node_templates.keys())[0] if node_templates else "backtester"
                        default_param = (
                            sorted(node_templates.get(default_node, {}).keys())[0]
                            if node_templates.get(default_node)
                            else "n_quantiles"
                        )
                        default_value = node_templates.get(default_node, {}).get(default_param, "")
                        table.setItem(row_idx, 0, QTableWidgetItem(f"scenario_{row_idx + 1}"))
                        table.setItem(row_idx, 1, QTableWidgetItem(default_node))
                        table.setItem(row_idx, 2, QTableWidgetItem(default_param))
                        table.setItem(row_idx, 3, QTableWidgetItem(self._format_node_param_value(default_value)))

                    def _del_row() -> None:
                        idx = table.currentRow()
                        if idx >= 0:
                            table.removeRow(idx)

                    def _generate_template_rows() -> None:
                        node_type = tpl_node.currentText().strip()
                        param_key = tpl_param.currentText().strip()
                        prefix = tpl_prefix.text().strip() or "scenario"
                        try:
                            start_v = float(tpl_start.text().strip())
                            end_v = float(tpl_end.text().strip())
                            step_v = float(tpl_step.text().strip())
                        except Exception:
                            QMessageBox.warning(self, "场景管理", "模板生成参数必须是数字")
                            return
                        rows_gen = self._build_template_scenarios(
                            node_type=node_type,
                            param_key=param_key,
                            start_value=start_v,
                            end_value=end_v,
                            step_value=step_v,
                            name_prefix=prefix,
                        )
                        if not rows_gen:
                            QMessageBox.warning(self, "场景管理", "模板生成结果为空，请检查范围与步长")
                            return
                        existing_rows: list[dict[str, str]] = []
                        for i in range(table.rowCount()):
                            existing_rows.append(
                                {
                                    "name": (table.item(i, 0).text() if table.item(i, 0) else "").strip(),
                                    "node_type": (table.item(i, 1).text() if table.item(i, 1) else "").strip(),
                                    "param_key": (table.item(i, 2).text() if table.item(i, 2) else "").strip(),
                                    "param_value": (table.item(i, 3).text() if table.item(i, 3) else "").strip(),
                                }
                            )
                        merged_rows = self._combine_template_rows(
                            existing_rows=existing_rows,
                            generated_rows=rows_gen,
                            replace_mode=tpl_replace.isChecked(),
                        )
                        table.setRowCount(0)
                        for row_item in merged_rows:
                            i = table.rowCount()
                            table.insertRow(i)
                            table.setItem(i, 0, QTableWidgetItem(row_item["name"]))
                            table.setItem(i, 1, QTableWidgetItem(row_item["node_type"]))
                            table.setItem(i, 2, QTableWidgetItem(row_item["param_key"]))
                            table.setItem(i, 3, QTableWidgetItem(row_item["param_value"]))

                    def _apply_template_preset(preset_key: str) -> None:
                        node_type, param_key, start_v, end_v, step_v, prefix = (
                            self._template_preset_for_key(preset_key)
                        )
                        idx = tpl_node.findText(node_type)
                        if idx >= 0:
                            tpl_node.setCurrentIndex(idx)
                        _refresh_param_combo()
                        pidx = tpl_param.findText(param_key)
                        if pidx >= 0:
                            tpl_param.setCurrentIndex(pidx)
                        tpl_start.setText(str(start_v))
                        tpl_end.setText(str(end_v))
                        tpl_step.setText(str(step_v))
                        tpl_prefix.setText(prefix)

                    add_btn.clicked.connect(_add_row)
                    del_btn.clicked.connect(_del_row)
                    tpl_make.clicked.connect(_generate_template_rows)
                    preset_q.clicked.connect(lambda: _apply_template_preset("quantile"))
                    preset_ic.clicked.connect(lambda: _apply_template_preset("ic_period"))
                    btn_row = QHBoxLayout()
                    save_btn = QPushButton("保存")
                    cancel_btn = QPushButton("取消")
                    btn_row.addStretch(1)
                    btn_row.addWidget(save_btn)
                    btn_row.addWidget(cancel_btn)
                    d_layout.addLayout(btn_row)

                    def _save_scenarios() -> None:
                        rows_payload: list[dict[str, str]] = []
                        for i in range(table.rowCount()):
                            row_item = {
                                "name": (table.item(i, 0).text() if table.item(i, 0) else "").strip(),
                                "node_type": (table.item(i, 1).text() if table.item(i, 1) else "").strip(),
                                "param_key": (table.item(i, 2).text() if table.item(i, 2) else "").strip(),
                                "param_value": (table.item(i, 3).text() if table.item(i, 3) else "").strip(),
                            }
                            if row_item["name"] and row_item["node_type"] and row_item["param_key"]:
                                rows_payload.append(row_item)
                        merged = self._merge_scenarios_into_overrides(current_overrides, rows_payload)
                        target_json.setPlainText(json.dumps(merged, ensure_ascii=False, indent=2))
                        dialog.accept()

                    save_btn.clicked.connect(_save_scenarios)
                    cancel_btn.clicked.connect(dialog.reject)
                    dialog.exec_()

                btn.clicked.connect(_open_node_editor)
                scenario_btn.clicked.connect(_open_scenario_editor)
                box_layout.addWidget(widget)
                row_btn = QHBoxLayout()
                row_btn.addStretch(1)
                row_btn.addWidget(btn)
                row_btn.addWidget(scenario_btn)
                box_layout.addLayout(row_btn)
                widget = box
                self.strategy_param_widgets[name][param["key"]] = box_layout.itemAt(0).widget()
                layout.addWidget(widget, row, 1)
                row += 1
                continue
            else:
                widget = QLineEdit()
                widget.setText(str(param.get("value", "")))
            layout.addWidget(widget, row, 1)
            self.strategy_param_widgets[name][param["key"]] = widget
            row += 1
        layout.setColumnStretch(1, 1)
        return form_widget

    def get_strategy_params_from_ui(self, strategy_name: str) -> dict[str, Any]:
        params: dict[str, Any] = {}
        widgets = self.strategy_param_widgets.get(strategy_name, {})
        for key, widget in widgets.items():
            if isinstance(widget, QSpinBox):
                params[key] = widget.value()
            elif isinstance(widget, QDoubleSpinBox):
                params[key] = widget.value()
            elif isinstance(widget, QCheckBox):
                params[key] = widget.isChecked()
            elif isinstance(widget, QLineEdit):
                text = widget.text().strip()
                if text == "":
                    params[key] = None
                else:
                    try:
                        params[key] = float(text)
                    except ValueError:
                        params[key] = text
            elif isinstance(widget, QTextEdit):
                text = widget.toPlainText().strip()
                if text == "":
                    params[key] = None
                else:
                    try:
                        params[key] = json.loads(text)
                    except Exception:
                        params[key] = text
            else:
                params[key] = None
        return params

    def _resolve_workflow_path_for_ui(self, workflow_path: str) -> str:
        path = str(workflow_path or "").strip()
        if not path:
            return ""
        if os.path.isabs(path):
            return path
        return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", path))

    def _read_workflow_node_templates(self, workflow_path: str) -> dict[str, dict[str, Any]]:
        if not workflow_path or not os.path.exists(workflow_path):
            return {}
        try:
            with open(workflow_path, encoding="utf-8") as f:
                data = json.load(f)
            nodes = data.get("nodes", {})
            if not isinstance(nodes, dict):
                return {}
            out: dict[str, dict[str, Any]] = {}
            for _, node_data in nodes.items():
                if not isinstance(node_data, dict):
                    continue
                node_type = str(node_data.get("node_type", "")).strip()
                if not node_type:
                    continue
                params = node_data.get("params", {})
                if not isinstance(params, dict):
                    continue
                out.setdefault(node_type, {})
                for k, v in params.items():
                    if k not in out[node_type]:
                        out[node_type][str(k)] = v
            return out
        except Exception:
            return {}

    def _format_node_param_value(self, value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    def _parse_node_param_text(self, text: str) -> Any:
        if text == "":
            return ""
        lowered = text.lower()
        if lowered in ("true", "false"):
            return lowered == "true"
        if lowered in ("none", "null"):
            return None
        try:
            return json.loads(text)
        except Exception:
            pass
        try:
            return ast.literal_eval(text)
        except Exception:
            pass
        try:
            if "." in text:
                return float(text)
            return int(text)
        except Exception:
            return text

    def _extract_scenarios_from_overrides(
        self, overrides: dict[str, Any]
    ) -> list[dict[str, str]]:
        if not isinstance(overrides, dict):
            return []
        scenarios = overrides.get("__scenarios__", [])
        if not isinstance(scenarios, list):
            return []
        rows: list[dict[str, str]] = []
        for item in scenarios:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            ov = item.get("overrides", {})
            if not name or not isinstance(ov, dict):
                continue
            for node_type, params in ov.items():
                if not isinstance(params, dict):
                    continue
                for k, v in params.items():
                    rows.append(
                        {
                            "name": name,
                            "node_type": str(node_type),
                            "param_key": str(k),
                            "param_value": self._format_node_param_value(v),
                        }
                    )
        return rows

    def _merge_scenarios_into_overrides(
        self, current_overrides: dict[str, Any], rows: list[dict[str, str]]
    ) -> dict[str, Any]:
        base = dict(current_overrides or {})
        if "__scenarios__" in base:
            base.pop("__scenarios__", None)
        grouped: dict[str, dict[str, dict[str, Any]]] = {}
        for row in rows:
            name = str(row.get("name", "")).strip()
            node_type = str(row.get("node_type", "")).strip()
            key = str(row.get("param_key", "")).strip()
            val_text = str(row.get("param_value", "")).strip()
            if not name or not node_type or not key:
                continue
            grouped.setdefault(name, {})
            grouped[name].setdefault(node_type, {})
            grouped[name][node_type][key] = self._parse_node_param_text(val_text)
        scenarios: list[dict[str, Any]] = []
        for name in sorted(grouped.keys()):
            scenarios.append({"name": name, "overrides": grouped[name]})
        base["__scenarios__"] = scenarios
        return base

    def _build_template_scenarios(
        self,
        node_type: str,
        param_key: str,
        start_value: float,
        end_value: float,
        step_value: float,
        name_prefix: str = "scenario",
    ) -> list[dict[str, str]]:
        if not node_type or not param_key:
            return []
        if step_value == 0:
            return []
        rows: list[dict[str, str]] = []
        step = abs(float(step_value))
        if end_value < start_value:
            step = -step
        current = float(start_value)
        guard = 0
        while (step > 0 and current <= end_value + 1e-12) or (
            step < 0 and current >= end_value - 1e-12
        ):
            guard += 1
            if guard > 1000:
                break
            is_int = float(current).is_integer()
            val = int(current) if is_int else round(float(current), 8)
            rows.append(
                {
                    "name": f"{name_prefix}_{param_key}_{val}",
                    "node_type": str(node_type),
                    "param_key": str(param_key),
                    "param_value": str(val),
                }
            )
            current += step
        return rows

    def _template_preset_for_key(self, preset_key: str) -> tuple[str, str, int, int, int, str]:
        key = str(preset_key or "").strip().lower()
        if key == "ic_period":
            return ("ic_analyzer", "periods", 1, 10, 1, "icp")
        return ("backtester", "n_quantiles", 5, 20, 5, "q")

    def _combine_template_rows(
        self,
        existing_rows: list[dict[str, str]],
        generated_rows: list[dict[str, str]],
        replace_mode: bool,
    ) -> list[dict[str, str]]:
        if replace_mode:
            return list(generated_rows)
        return list(existing_rows) + list(generated_rows)

    def set_param_value_for_all(self, key: str, value: Any):
        for widgets in self.strategy_param_widgets.values():
            widget = widgets.get(key)
            if widget is None:
                continue
            if isinstance(widget, QSpinBox):
                widget.setValue(int(value))
            elif isinstance(widget, QDoubleSpinBox):
                widget.setValue(float(value))

    def on_strategy_changed(self, strategy_name: str):
        if strategy_name in self.strategy_registry:
            index = list(self.strategy_registry.keys()).index(strategy_name)
            self.strategy_param_stack.setCurrentIndex(index)
            enabled = self.strategy_registry[strategy_name].get("class") is not None
            self.strategy_status_label.setText("是" if enabled else "否")
            self.strategy_status_label.setStyleSheet(
                "color: #2e7d32;" if enabled else "color: #c62828;"
            )
        else:
            self.strategy_status_label.setText("否")
            self.strategy_status_label.setStyleSheet("color: #c62828;")

    def load_strategy_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "选择策略文件", "", "Python Files (*.py)")
        if not file_path:
            return
        try:
            with open(file_path, encoding="utf-8", errors="ignore") as f:
                content = f.read()
            self.strategy_editor.setPlainText(content)
            self.strategy_editor_path = file_path
        except Exception as e:
            QMessageBox.warning(self, "读取失败", str(e))

    def save_strategy_file(self):
        file_path = self.strategy_editor_path
        if not file_path:
            file_path, _ = QFileDialog.getSaveFileName(
                self, "保存策略文件", "", "Python Files (*.py)"
            )
            if not file_path:
                return
            self.strategy_editor_path = file_path
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(self.strategy_editor.toPlainText())
        except Exception as e:
            QMessageBox.warning(self, "保存失败", str(e))

    def validate_strategy_code(self):
        code = self.strategy_editor.toPlainText()
        if not code.strip():
            QMessageBox.warning(self, "校验失败", "策略内容为空")
            return
        try:
            ast.parse(code)
            QMessageBox.information(self, "校验通过", "策略代码语法正常")
        except SyntaxError as e:
            QMessageBox.warning(self, "校验失败", f"{e.msg} (line {e.lineno})")

    def create_advanced_params_group(self) -> QGroupBox:
        """创建高级参数组"""
        group = QGroupBox("⚙️ 高级选项")
        layout = QGridLayout(group)

        # 参数优化
        self.optimize_checkbox = QCheckBox("启用参数优化")
        layout.addWidget(self.optimize_checkbox, 0, 0, 1, 2)

        # 基准比较
        self.benchmark_checkbox = QCheckBox("基准比较")
        layout.addWidget(self.benchmark_checkbox, 1, 0, 1, 2)

        # 风险分析
        self.risk_analysis_checkbox = QCheckBox("详细风险分析")
        self.risk_analysis_checkbox.setChecked(True)
        layout.addWidget(self.risk_analysis_checkbox, 2, 0, 1, 2)

        return group

    def create_extension_params_group(self) -> QGroupBox:
        """创建可扩展功能组"""
        group = QGroupBox("🧩 可扩展功能")
        layout = QGridLayout(group)

        self.walk_forward_checkbox = QCheckBox("Walk-Forward 分段回测")
        layout.addWidget(self.walk_forward_checkbox, 0, 0, 1, 2)

        layout.addWidget(QLabel("样本外比例:"), 1, 0)
        self.oos_ratio_spin = QDoubleSpinBox()
        self.oos_ratio_spin.setRange(0.1, 0.5)
        self.oos_ratio_spin.setSingleStep(0.05)
        self.oos_ratio_spin.setValue(0.2)
        self.oos_ratio_spin.setSuffix("")
        layout.addWidget(self.oos_ratio_spin, 1, 1)

        layout.addWidget(QLabel("窗口长度(天):"), 1, 2)
        self.wf_window_spin = QSpinBox()
        self.wf_window_spin.setRange(60, 2000)
        self.wf_window_spin.setValue(252)
        layout.addWidget(self.wf_window_spin, 1, 3)

        layout.addWidget(QLabel("步长(天):"), 1, 4)
        self.wf_step_spin = QSpinBox()
        self.wf_step_spin.setRange(20, 500)
        self.wf_step_spin.setValue(60)
        layout.addWidget(self.wf_step_spin, 1, 5)

        self.optimize_method_combo = QComboBox()
        self.optimize_method_combo.addItems(["网格搜索", "贝叶斯优化", "随机搜索"])
        layout.addWidget(QLabel("参数优化方法:"), 2, 0)
        layout.addWidget(self.optimize_method_combo, 2, 1)

        layout.addWidget(QLabel("优化次数:"), 2, 2)
        self.optimize_trials_spin = QSpinBox()
        self.optimize_trials_spin.setRange(10, 500)
        self.optimize_trials_spin.setValue(50)
        layout.addWidget(self.optimize_trials_spin, 2, 3)

        layout.addWidget(QLabel("交易成本模型:"), 3, 0)
        self.cost_model_combo = QComboBox()
        self.cost_model_combo.addItems(["固定佣金", "阶梯佣金", "冲击成本"])
        layout.addWidget(self.cost_model_combo, 3, 1)

        layout.addWidget(QLabel("滑点(bps):"), 3, 2)
        self.slippage_spin = QDoubleSpinBox()
        self.slippage_spin.setRange(0.0, 50.0)
        self.slippage_spin.setValue(5.0)
        layout.addWidget(self.slippage_spin, 3, 3)

        layout.addWidget(QLabel("仓位控制:"), 4, 0)
        self.position_sizing_combo = QComboBox()
        self.position_sizing_combo.addItems(["固定比例", "波动率目标", "风险平价"])
        layout.addWidget(self.position_sizing_combo, 4, 1)

        layout.addWidget(QLabel("单票上限(%):"), 4, 2)
        self.max_position_spin = QDoubleSpinBox()
        self.max_position_spin.setRange(1.0, 30.0)
        self.max_position_spin.setValue(30.0)
        layout.addWidget(self.max_position_spin, 4, 3)

        layout.addWidget(QLabel("再平衡周期:"), 4, 4)
        self.rebalance_combo = QComboBox()
        self.rebalance_combo.addItems(["日频", "周频", "月频"])
        layout.addWidget(self.rebalance_combo, 4, 5)

        self.multi_asset_checkbox = QCheckBox("多标的组合回测")
        layout.addWidget(self.multi_asset_checkbox, 5, 0, 1, 2)

        layout.addWidget(QLabel("标的列表:"), 5, 2)
        self.multi_asset_edit = QLineEdit()
        self.multi_asset_edit.setPlaceholderText("000001.SZ, 600000.SH")
        layout.addWidget(self.multi_asset_edit, 5, 3, 1, 3)

        return group

    def create_control_buttons(self) -> QGroupBox:
        """创建控制按钮组"""
        group = QGroupBox("🎮 操作控制")
        layout = QGridLayout(group)

        self.start_button = QPushButton("🚀 开始回测")
        self.start_button.setMinimumHeight(0)
        self.start_button.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                border-radius: 5px;
                font-size: 14px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:pressed {
                background-color: #3d8b40;
            }
        """)
        layout.addWidget(self.start_button, 0, 0)

        self.stop_button = QPushButton("⏹️ 停止回测")
        self.stop_button.setMinimumHeight(0)
        self.stop_button.setEnabled(False)
        self.stop_button.setStyleSheet("""
            QPushButton {
                background-color: #f44336;
                color: white;
                border: none;
                border-radius: 5px;
                font-size: 14px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #da190b;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        layout.addWidget(self.stop_button, 0, 1)

        self.export_button = QPushButton("📄 导出HTML报告")
        self.export_button.setMinimumHeight(0)
        self.export_button.setEnabled(False)
        self.export_button.setStyleSheet("""
            QPushButton {
                background-color: #2196F3;
                color: white;
                border: none;
                border-radius: 5px;
                font-size: 14px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #1976D2;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        layout.addWidget(self.export_button, 0, 2)

        return group

    def create_progress_group(self) -> QGroupBox:
        """创建进度显示组"""
        group = QGroupBox("📊 执行状态")
        layout = QVBoxLayout(group)

        # 状态标签
        self.status_label = QLabel("💤 等待开始...")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.status_label)

        # 进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        return group

    def create_results_panel(self) -> QWidget:
        """创建结果显示面板"""
        panel = QWidget()
        layout = QVBoxLayout(panel)

        # 标题
        title_label = QLabel("📈 回测结果分析")
        title_label.setFont(QFont("Arial", 14, QFont.Bold))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_label)

        # 创建标签页
        self.results_tabs = QTabWidget()

        # 性能概览标签页
        self.overview_tab = self.create_overview_tab()
        self.results_tabs.addTab(self.overview_tab, "📊 性能概览")

        # 详细指标标签页
        self.metrics_tab = self.create_metrics_tab()
        self.results_tabs.addTab(self.metrics_tab, "📈 详细指标")

        # 风险分析标签页
        self.risk_tab = self.create_risk_tab()
        self.results_tabs.addTab(self.risk_tab, "⚠️ 风险分析")

        # 交易记录标签页
        self.trades_tab = self.create_trades_tab()
        self.results_tabs.addTab(self.trades_tab, "💼 交易记录")

        self.config_tab = self.create_config_tab()
        self.results_tabs.addTab(self.config_tab, "🧾 配置摘要")

        self.optimize_tab = self.create_optimize_tab()
        self.results_tabs.addTab(self.optimize_tab, "🧠 参数优化")

        self.walk_forward_tab = self.create_walk_forward_tab()
        self.results_tabs.addTab(self.walk_forward_tab, "🧪 Walk-Forward")

        self.cost_tab = self.create_cost_tab()
        self.results_tabs.addTab(self.cost_tab, "💹 成本与归因")

        self.data_load_tab = self.create_data_load_tab()
        self.results_tabs.addTab(self.data_load_tab, "📥 数据加载")

        layout.addWidget(self.results_tabs)

        return panel

    def create_overview_tab(self) -> QWidget:
        """创建性能概览标签页"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        metrics_layout = QHBoxLayout()
        self.total_return_card = self.create_metric_inline("总收益率", "0.00%", "#4CAF50")
        self.annual_return_card = self.create_metric_inline("年化收益率", "0.00%", "#2196F3")
        self.sharpe_card = self.create_metric_inline("夏普比率", "0.00", "#FF9800")
        self.drawdown_card = self.create_metric_inline("最大回撤", "0.00%", "#f44336")
        metrics_layout.addWidget(self.total_return_card)
        metrics_layout.addWidget(self.annual_return_card)
        metrics_layout.addWidget(self.sharpe_card)
        metrics_layout.addWidget(self.drawdown_card)
        layout.addLayout(metrics_layout)

        tools_layout = QHBoxLayout()
        self.zoom_lock_checkbox = QCheckBox("锁定缩放")
        self.zoom_lock_checkbox.setChecked(False)
        self.zoom_lock_checkbox.toggled.connect(self.on_zoom_lock_changed)
        self.reset_view_btn = QPushButton("重置视图")
        self.reset_view_btn.setMinimumHeight(0)
        self.reset_view_btn.clicked.connect(self.reset_chart_view)
        self.center_view_btn = QPushButton("居中视图")
        self.center_view_btn.setMinimumHeight(0)
        self.center_view_btn.clicked.connect(self.center_chart_view)
        self.fix_y_btn = QPushButton("固定Y区间")
        self.fix_y_btn.setMinimumHeight(0)
        self.fix_y_btn.clicked.connect(self.fix_y_range)
        self.fix_x_btn = QPushButton("锁定X区间")
        self.fix_x_btn.setMinimumHeight(0)
        self.fix_x_btn.clicked.connect(self.fix_x_range)
        self.prev_window_btn = QPushButton("上一窗口")
        self.prev_window_btn.setMinimumHeight(0)
        self.prev_window_btn.clicked.connect(self.prev_window_view)
        self.next_window_btn = QPushButton("下一窗口")
        self.next_window_btn.setMinimumHeight(0)
        self.next_window_btn.clicked.connect(self.next_window_view)
        self.window_ratio_combo = QComboBox()
        self.window_ratio_combo.addItems(["20%", "50%", "80%"])
        self.window_ratio_combo.setCurrentText("50%")
        self.window_ratio_combo.currentTextChanged.connect(self.on_window_ratio_changed)
        self.save_view_btn = QPushButton("保存视图")
        self.save_view_btn.setMinimumHeight(0)
        self.save_view_btn.clicked.connect(self.save_view_preset)
        self.rename_view_btn = QPushButton("重命名")
        self.rename_view_btn.setMinimumHeight(0)
        self.rename_view_btn.clicked.connect(self.rename_view_preset)
        self.delete_view_btn = QPushButton("删除预设")
        self.delete_view_btn.setMinimumHeight(0)
        self.delete_view_btn.clicked.connect(self.delete_view_preset)
        self.preset_combo = QComboBox()
        self.preset_combo.setMinimumWidth(140)
        self.preset_combo.currentTextChanged.connect(self.apply_view_preset)
        tools_layout.addStretch(1)
        tools_layout.addWidget(self.zoom_lock_checkbox)
        tools_layout.addWidget(self.reset_view_btn)
        tools_layout.addWidget(self.center_view_btn)
        tools_layout.addWidget(self.fix_y_btn)
        tools_layout.addWidget(self.fix_x_btn)
        tools_layout.addWidget(self.prev_window_btn)
        tools_layout.addWidget(self.next_window_btn)
        tools_layout.addWidget(self.window_ratio_combo)
        tools_layout.addWidget(self.save_view_btn)
        tools_layout.addWidget(self.rename_view_btn)
        tools_layout.addWidget(self.delete_view_btn)
        tools_layout.addWidget(self.preset_combo)
        layout.addLayout(tools_layout)

        self.portfolio_chart = PortfolioChart()
        layout.addWidget(self.portfolio_chart)
        layout.setStretch(0, 0)
        layout.setStretch(1, 0)
        layout.setStretch(2, 1)
        self.load_view_presets()

        return tab

    def create_metric_inline(self, title: str, value: str, color: str) -> MetricInlineCard:
        card = MetricInlineCard()
        card.setFrameStyle(QFrame.Box)
        card.setStyleSheet(f"""
            QFrame {{
                border: 1px solid {color};
                border-radius: 8px;
                background-color: white;
                padding: 6px 8px;
            }}
        """)
        layout = QHBoxLayout(card)
        layout.setContentsMargins(6, 2, 6, 2)
        title_label = QLabel(title)
        title_label.setAlignment(
            cast(Qt.AlignmentFlag, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        )
        title_label.setStyleSheet(f"color: {color}; font-weight: bold; font-size: 12px;")
        value_label = QLabel(value)
        value_label.setAlignment(
            cast(Qt.AlignmentFlag, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        )
        value_label.setStyleSheet(f"color: {color}; font-weight: bold; font-size: 16px;")
        sparkline = SparklineWidget()
        sparkline.setFixedWidth(90)
        layout.addWidget(title_label)
        layout.addStretch(1)
        layout.addWidget(value_label)
        layout.addWidget(sparkline)
        card.value_label = value_label
        card.sparkline = sparkline
        card.sparkline_color = QColor(color)
        return card

    def create_metric_inline_legacy(self, title: str, value: str, color: str) -> QFrame:
        """创建内联指标卡片"""
        card = QFrame()
        card.setFrameStyle(QFrame.StyledPanel | QFrame.Raised)
        card.setStyleSheet(f"""
            QFrame {{
                background-color: white;
                border-radius: 4px;
                border-left: 4px solid {color};
            }}
        """)
        layout = QHBoxLayout(card)
        layout.setContentsMargins(10, 5, 10, 5)

        title_label = QLabel(title)
        title_label.setStyleSheet("color: #666; font-size: 12px;")
        layout.addWidget(title_label)

        value_label = QLabel(value)
        value_label.setStyleSheet(f"color: {color}; font-weight: bold; font-size: 16px;")
        value_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        layout.addWidget(value_label)

        # 保存引用以便更新
        cast(Any, card).value_label = value_label
        return card

    def create_metric_card(self, title: str, value: str, color: str) -> QFrame:
        """创建指标卡片"""
        card = MetricCard()
        card.setFrameStyle(QFrame.Box)
        card.setStyleSheet(f"""
            QFrame {{
                border: 2px solid {color};
                border-radius: 10px;
                background-color: white;
                padding: 10px;
            }}
        """)

        layout = QVBoxLayout(card)

        # 标题
        title_label = QLabel(title)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setStyleSheet(f"color: {color}; font-weight: bold; font-size: 12px;")
        layout.addWidget(title_label)

        # 数值
        value_label = QLabel(value)
        value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        value_label.setStyleSheet(f"color: {color}; font-weight: bold; font-size: 24px;")
        layout.addWidget(value_label)

        # 保存引用以便更新
        card.value_label = value_label

        return card

    def create_metrics_tab(self) -> QWidget:
        """创建详细指标标签页"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # 指标表格
        self.metrics_table = QTableWidget()
        self.metrics_table.setColumnCount(2)
        self.metrics_table.setHorizontalHeaderLabels(["指标名称", "数值"])
        metrics_header = self.metrics_table.horizontalHeader()
        if metrics_header is not None:
            metrics_header.setStretchLastSection(True)

        layout.addWidget(self.metrics_table)

        return tab

    def create_risk_tab(self) -> QWidget:
        """创建风险分析标签页"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # 风险报告文本
        self.risk_report_text = QTextEdit()
        self.risk_report_text.setReadOnly(True)
        self.risk_report_text.setFont(QFont("Consolas", 10))

        layout.addWidget(self.risk_report_text)

        return tab

    def create_trades_tab(self) -> QWidget:
        """创建交易记录标签页"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # 交易记录表格
        self.trades_table = QTableWidget()
        self.trades_table.setColumnCount(6)
        self.trades_table.setHorizontalHeaderLabels(
            ["日期", "操作", "价格", "数量", "金额", "收益"]
        )
        trades_header = self.trades_table.horizontalHeader()
        if trades_header is not None:
            trades_header.setStretchLastSection(True)

        layout.addWidget(self.trades_table)

        return tab

    def create_config_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.addWidget(QLabel("回测参数"))
        self.config_summary_text = QTextEdit()
        self.config_summary_text.setReadOnly(True)
        self.config_summary_text.setFont(QFont("Consolas", 10))
        layout.addWidget(self.config_summary_text)
        layout.addWidget(QLabel("数据摘要"))
        self.data_summary_text = QTextEdit()
        self.data_summary_text.setReadOnly(True)
        self.data_summary_text.setFont(QFont("Consolas", 10))
        layout.addWidget(self.data_summary_text)
        return tab

    def create_optimize_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        action_row = QHBoxLayout()
        self.copy_best_scenario_btn = QPushButton("复制最佳场景参数")
        action_row.addWidget(self.copy_best_scenario_btn)
        action_row.addStretch(1)
        layout.addLayout(action_row)
        self.optimize_table = QTableWidget()
        self.optimize_table.setColumnCount(6)
        self.optimize_table.setHorizontalHeaderLabels(
            ["参数组合", "收益", "夏普", "回撤", "IC均值", "ICIR"]
        )
        optimize_header = self.optimize_table.horizontalHeader()
        if optimize_header is not None:
            optimize_header.setStretchLastSection(True)
        layout.addWidget(self.optimize_table)
        layout.addWidget(QLabel("优化排名图"))
        self.optimize_rank_chart = BarChart()
        layout.addWidget(self.optimize_rank_chart)
        layout.addWidget(QLabel("参数敏感性分析"))
        self.optimize_sensitivity_text = QTextEdit()
        self.optimize_sensitivity_text.setReadOnly(True)
        self.optimize_sensitivity_text.setFont(QFont("Consolas", 10))
        layout.addWidget(self.optimize_sensitivity_text)
        return tab

    def create_walk_forward_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        self.walk_forward_table = QTableWidget()
        self.walk_forward_table.setColumnCount(6)
        self.walk_forward_table.setHorizontalHeaderLabels(
            ["窗口", "训练期", "测试期", "收益", "夏普", "回撤"]
        )
        walk_forward_header = self.walk_forward_table.horizontalHeader()
        if walk_forward_header is not None:
            walk_forward_header.setStretchLastSection(True)
        layout.addWidget(self.walk_forward_table)
        layout.addWidget(QLabel("分段净值曲线"))
        self.walk_forward_curve_chart = MultiLineChart()
        layout.addWidget(self.walk_forward_curve_chart)
        return tab

    def create_cost_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        self.weights_table = QTableWidget()
        self.weights_table.setColumnCount(0)
        self.weights_table.setHorizontalHeaderLabels([])
        layout.addWidget(QLabel("再平衡权重表"))
        layout.addWidget(self.weights_table)
        layout.addWidget(QLabel("组合权重图"))
        self.weights_chart = MultiLineChart()
        layout.addWidget(self.weights_chart)
        layout.addWidget(QLabel("换手率图"))
        self.turnover_chart = MultiLineChart()
        layout.addWidget(self.turnover_chart)
        layout.addWidget(QLabel("因子分层净值图"))
        self.factor_quantile_chart = MultiLineChart()
        layout.addWidget(self.factor_quantile_chart)
        layout.addWidget(QLabel("IC时序图"))
        self.ic_series_chart = MultiLineChart()
        layout.addWidget(self.ic_series_chart)
        self.cost_text = QTextEdit()
        self.cost_text.setReadOnly(True)
        self.cost_text.setFont(QFont("Consolas", 10))
        layout.addWidget(self.cost_text)
        return tab

    def create_data_load_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        self.data_load_table = QTableWidget()
        self.data_load_table.setColumnCount(6)
        self.data_load_table.setHorizontalHeaderLabels(
            ["标的", "状态", "条数", "起止时间", "数据源", "周期"]
        )
        data_load_header = self.data_load_table.horizontalHeader()
        if data_load_header is not None:
            data_load_header.setStretchLastSection(True)
        layout.addWidget(self.data_load_table)
        layout.addWidget(QLabel("加载日志"))
        self.data_load_log = QTextEdit()
        self.data_load_log.setReadOnly(True)
        self.data_load_log.setFont(QFont("Consolas", 10))
        layout.addWidget(self.data_load_log)
        return tab

    def setup_connections(self):
        """设置信号连接"""
        self.start_button.clicked.connect(self.start_backtest)
        self.stop_button.clicked.connect(self.stop_backtest)
        self.export_button.clicked.connect(self.export_html_report)
        self.strategy_combo.currentTextChanged.connect(self.on_strategy_changed)
        self.refresh_strategy_list_btn.clicked.connect(self.refresh_strategy_catalog_table_async)
        self.strategy_load_btn.clicked.connect(self.load_strategy_file)
        self.strategy_save_btn.clicked.connect(self.save_strategy_file)
        self.strategy_validate_btn.clicked.connect(self.validate_strategy_code)
        self.optimize_checkbox.toggled.connect(self.update_optimize_controls)
        self.overfit_warning_checkbox.toggled.connect(self.update_overfit_controls)
        self.walk_forward_checkbox.toggled.connect(self.update_walk_forward_controls)
        self.multi_asset_checkbox.toggled.connect(self.update_multi_asset_controls)
        self.copy_best_scenario_btn.clicked.connect(self.copy_best_factor_scenario_params)
        self.update_optimize_controls(self.optimize_checkbox.isChecked())
        self.update_overfit_controls(self.overfit_warning_checkbox.isChecked())
        self.update_walk_forward_controls(self.walk_forward_checkbox.isChecked())
        self.update_multi_asset_controls(self.multi_asset_checkbox.isChecked())

    def _connect_events(self):
        signal_bus.subscribe(Events.CHART_DATA_LOADED, self.on_chart_data_loaded)
        signal_bus.subscribe(Events.CHART_CROSSHAIR_MOVED, self._on_crosshair_moved)

    def on_chart_data_loaded(self, symbol: str, **kwargs):
        if not symbol:
            return
        self.stock_code_edit.setText(symbol)

    _CROSSHAIR_THROTTLE_S: float = max(
        0.01,
        int(os.environ.get("EASYXT_CROSSHAIR_THROTTLE_MS", "100")) / 1000.0,
    )  # 可通过环境变量 EASYXT_CROSSHAIR_THROTTLE_MS（毫秒整数）覆盖

    def _on_crosshair_moved(self, **kwargs: object) -> None:
        """响应十字线移动事件，在 trades_table 中高亮最近的交易行（100ms 节流）。"""
        sym = kwargs.get("symbol", "")
        if sym and sym != self.stock_code_edit.text():
            return
        time_val = kwargs.get("time")
        if time_val is None:
            return
        now = time.monotonic()
        if now - self._crosshair_last_t < self._CROSSHAIR_THROTTLE_S:
            return
        self._crosshair_last_t = now
        self._highlight_trades_row_at_time(time_val)

    def _highlight_trades_row_at_time(self, time_val: object) -> None:
        """在 trades_table 中找到最接近 time_val 的行并选中、滚动到视图。"""
        row_count = self.trades_table.rowCount()
        if row_count == 0:
            return
        if isinstance(time_val, (int, float)) and float(time_val) > 1_000_000_000:
            from datetime import datetime
            try:
                target = datetime.fromtimestamp(float(time_val)).strftime("%Y-%m-%d")
            except (OSError, OverflowError, ValueError):
                return
        else:
            target = str(time_val)[:10]
        if len(target) < 10:
            return
        # 找最近且不超过 target 的行（按日期列0的前10字符比较）
        best_row = -1
        best_date = ""
        for row in range(row_count):
            item = self.trades_table.item(row, 0)
            if item is None:
                continue
            cell = item.text()[:10]
            if len(cell) < 10:
                continue
            if cell <= target and (best_date == "" or cell >= best_date):
                best_date = cell
                best_row = row
        # 全部日期都晚于 target 时，回退到最早行
        if best_row == -1:
            for row in range(row_count):
                item = self.trades_table.item(row, 0)
                if item is None:
                    continue
                cell = item.text()[:10]
                if len(cell) < 10:
                    continue
                if best_date == "" or cell <= best_date:
                    best_date = cell
                    best_row = row
        if best_row >= 0:
            self.trades_table.selectRow(best_row)
            scroll_item = self.trades_table.item(best_row, 0)
            if scroll_item is not None:
                self.trades_table.scrollToItem(scroll_item)

    def update_optimize_controls(self, enabled: bool):
        self.optimize_method_combo.setEnabled(enabled)
        self.optimize_trials_spin.setEnabled(enabled)
        self.fixed_grid_space_edit.setEnabled(enabled)
        self.adaptive_grid_space_edit.setEnabled(enabled)
        self.atr_grid_space_edit.setEnabled(enabled)
        self.overfit_warning_checkbox.setEnabled(True)

    def update_overfit_controls(self, enabled: bool):
        self.overfit_sharpe_drop_spin.setEnabled(enabled)
        self.overfit_drawdown_increase_spin.setEnabled(enabled)

    def update_walk_forward_controls(self, enabled: bool):
        self.oos_ratio_spin.setEnabled(enabled)
        self.wf_window_spin.setEnabled(enabled)
        self.wf_step_spin.setEnabled(enabled)

    def update_multi_asset_controls(self, enabled: bool):
        self.multi_asset_edit.setEnabled(enabled)

    def update_period_hint(self, text: str):
        hints = {
            "日线(1d)": "日线周期：支持本地缓存与DuckDB，建议跨度 1-10 年",
            "60分钟(1h)": "60分钟：优先QMT/QStock/AKShare，建议跨度 6-24 个月",
            "30分钟(30m)": "30分钟：优先QMT/QStock/AKShare，建议跨度 3-12 个月",
            "15分钟(15m)": "15分钟：优先QMT/QStock/AKShare，建议跨度 1-6 个月",
            "5分钟(5m)": "5分钟：优先QMT/QStock/AKShare，建议跨度 1-3 个月",
            "1分钟(1m)": "1分钟：仅建议短窗口(2-20个交易日)，数据量大且耗时",
        }
        self.period_hint_label.setText(hints.get(text, ""))

    def apply_parameter_preset(self):
        preset = self.preset_combo.currentText()
        today = QDate.currentDate()
        if preset == "短期趋势":
            self.start_date_edit.setDate(today.addYears(-1))
            self.set_param_value_for_all("short_period", 5)
            self.set_param_value_for_all("long_period", 20)
            self.set_param_value_for_all("rsi_period", 14)
        elif preset == "中期趋势":
            self.start_date_edit.setDate(today.addYears(-3))
            self.set_param_value_for_all("short_period", 10)
            self.set_param_value_for_all("long_period", 60)
            self.set_param_value_for_all("rsi_period", 14)
        elif preset == "均值回归":
            self.start_date_edit.setDate(today.addYears(-2))
            self.set_param_value_for_all("short_period", 7)
            self.set_param_value_for_all("long_period", 30)
            self.set_param_value_for_all("rsi_period", 6)
        elif preset == "稳健低频":
            self.start_date_edit.setDate(today.addYears(-5))
            self.set_param_value_for_all("short_period", 20)
            self.set_param_value_for_all("long_period", 120)
            self.set_param_value_for_all("rsi_period", 14)
        self.end_date_edit.setDate(today)

    def update_config_summary(self, params: dict[str, Any], factor_context: Optional[dict[str, Any]] = None):
        lines = [
            f"股票代码: {params.get('stock_code')}",
            f"时间范围: {params.get('start_date')} ~ {params.get('end_date')}",
            f"K线周期: {params.get('period', '1d')}",
            f"初始资金: {params.get('initial_cash'):,.0f} 元",
            f"手续费率: {params.get('commission'):.4%}",
            f"复权类型: {params.get('adjust')}",
            f"策略: {params.get('strategy_name', self.strategy_combo.currentText())}",
            f"短期均线: {params.get('short_period')}",
            f"长期均线: {params.get('long_period')}",
            f"RSI周期: {params.get('rsi_period')}",
            f"参数优化: {'开启' if params.get('optimize_enabled') else '关闭'}",
            f"基准比较: {'开启' if params.get('benchmark_enabled') else '关闭'}",
            f"风险分析: {'开启' if params.get('risk_analysis_enabled') else '关闭'}",
            f"Walk-Forward: {'开启' if params.get('walk_forward_enabled') else '关闭'}",
            f"过拟合警戒: {'开启' if params.get('overfit_warning_enabled') else '关闭'}",
            f"夏普下降阈值: {params.get('overfit_sharpe_drop'):.0%}",
            f"回撤放大阈值: {params.get('overfit_drawdown_increase'):.0%}",
            f"样本外比例: {params.get('oos_ratio'):.0%}",
            f"窗口长度(天): {params.get('wf_window_days')}",
            f"步长(天): {params.get('wf_step_days')}",
            f"优化方法: {params.get('optimize_method')}",
            f"优化次数: {params.get('optimize_trials')}",
            f"成本模型: {params.get('cost_model')}",
            f"滑点(bps): {params.get('slippage_bps')}",
            f"仓位控制: {params.get('position_sizing')}",
            f"单票上限: {params.get('max_position_pct')}%",
            f"再平衡周期: {params.get('rebalance_freq')}",
            f"多标的组合: {'开启' if params.get('multi_asset_enabled') else '关闭'}",
        ]
        strategy_params = params.get("strategy_params", {})
        if strategy_params:
            details = ", ".join([f"{k}={v}" for k, v in strategy_params.items()])
            lines.append(f"策略参数: {details}")
        if params.get("multi_asset_enabled"):
            symbols = params.get("multi_asset_list", [])
            lines.append(f"标的列表: {', '.join(symbols) if symbols else '未填写'}")
        if params.get("optimize_enabled"):
            lines.append("提示: 参数优化需注意样本外验证与过拟合风险")
        if params.get("walk_forward_enabled"):
            lines.append("提示: Walk-Forward 将按分段生成样本内外绩效")
        custom_space = params.get("custom_search_space", {})
        if custom_space:
            lines.append(f"自定义搜索空间: {custom_space}")
        fc = factor_context or {}
        if isinstance(fc, dict):
            ic_summary = fc.get("ic_summary", {})
            if isinstance(ic_summary, dict) and ic_summary:
                lines.append(
                    "IC摘要: "
                    f"ic_mean={float(ic_summary.get('ic_mean', 0.0)):.4f}, "
                    f"ic_ir={float(ic_summary.get('ic_ir', 0.0)):.4f}, "
                    f"ic_prob={float(ic_summary.get('ic_prob', 0.0)):.4f}"
                )
            workflow_meta = fc.get("workflow_meta", {})
            if isinstance(workflow_meta, dict) and workflow_meta:
                lines.append(
                    "工作流: "
                    f"{workflow_meta.get('name', '-')}"
                    f" | 节点数={workflow_meta.get('node_count', '-')}"
                    f" | 连接数={workflow_meta.get('connection_count', '-')}"
                )
            nodes = fc.get("workflow_nodes", [])
            if isinstance(nodes, list) and nodes:
                lines.append("节点参数摘要:")
                for node in nodes[:8]:
                    if not isinstance(node, dict):
                        continue
                    ntype = str(node.get("type", ""))
                    nparams = node.get("params", {})
                    if not isinstance(nparams, dict):
                        continue
                    keyvals = ", ".join(f"{k}={v}" for k, v in list(nparams.items())[:4])
                    lines.append(f"  - {ntype}: {keyvals}")
        self.config_summary_text.setPlainText("\n".join(lines))

    def update_data_summary(self, summary: dict[str, Any]):
        self.data_summary_text.setPlainText(self.build_data_summary_text(summary))

    def update_data_load_summary(self, summary: dict[str, Any]):
        if not hasattr(self, "data_status_label"):
            return
        if not summary:
            self.data_status_label.setText("数据: 未加载")
            self.data_status_label.setToolTip("")
            return
        total_rows = summary.get("total_rows", 0)
        sources = summary.get("sources", [])
        loaded_at = summary.get("loaded_at", "-")
        source_text = ",".join(sources) if sources else "-"
        self.data_status_label.setText(f"数据: {total_rows} 条 | 源: {source_text}")
        self.data_status_label.setToolTip(f"{loaded_at}\n{self.build_data_summary_text(summary)}")

    def build_data_summary_text(self, summary: dict[str, Any]) -> str:
        if not summary:
            return "等待数据加载..."
        items = summary.get("items", [])
        total_rows = summary.get("total_rows", 0)
        sources = summary.get("sources", [])
        loaded_at = summary.get("loaded_at", "-")
        lines = [
            f"加载时间: {loaded_at}",
            f"总数据条数: {total_rows}",
            f"数据源集合: {', '.join(sources) if sources else '-'}",
        ]
        for item in items:
            raw_rows = item.get("raw_rows", item.get("rows", 0))
            clean_rows = item.get("clean_rows", item.get("rows", 0))
            removed_rows = item.get("removed_rows", max(raw_rows - clean_rows, 0))
            issues = item.get("quality_issues", [])
            missing_total = item.get("missing_total", 0)
            issue_text = "无" if not issues else ";".join(issues)
            lines.append(
                f"{item.get('symbol')}: {item.get('rows', 0)} 条 | "
                f"{item.get('start_date', '-')}"
                f" ~ {item.get('end_date', '-')} | "
                f"{item.get('source', '-')}"
                f" | {item.get('period', '-')}"
                f" | 复权:{item.get('adjust', '-')}"
                f" | 清洗:{raw_rows}->{clean_rows}(-{removed_rows})"
                f" | 校验:{issue_text},缺失:{missing_total}"
            )
        return "\n".join(lines)

    def append_load_log(self, text: str):
        if not hasattr(self, "data_load_log"):
            return
        current = self.data_load_log.toPlainText()
        if current:
            self.data_load_log.setPlainText(current + "\n" + text)
        else:
            self.data_load_log.setPlainText(text)

    def update_optimize_tab(self, results: dict[str, Any]):
        data = results.get("optimization_results", []) if isinstance(results, dict) else []
        if not data and isinstance(results, dict):
            factor_compare = results.get("factor_compare_results", [])
            if isinstance(factor_compare, list) and factor_compare:
                data = factor_compare
        if not data:
            self.optimize_table.setRowCount(1)
            self.optimize_table.setItem(0, 0, QTableWidgetItem("暂无结果"))
            self.optimize_table.setItem(0, 1, QTableWidgetItem("-"))
            self.optimize_table.setItem(0, 2, QTableWidgetItem("-"))
            self.optimize_table.setItem(0, 3, QTableWidgetItem("-"))
            self.optimize_table.setItem(0, 4, QTableWidgetItem("-"))
            self.optimize_table.setItem(0, 5, QTableWidgetItem("-"))
            self.update_optimize_rank_chart([])
            if hasattr(self, "optimize_sensitivity_text"):
                lines = ["暂无敏感性分析"]
                wf_sensitivity = (
                    results.get("walk_forward_sensitivity", []) if isinstance(results, dict) else []
                )
                if wf_sensitivity:
                    lines = ["Walk-Forward 多阶段敏感性(均值)："]
                    grouped: dict[str, list[dict[str, Any]]] = {}
                    for row in wf_sensitivity:
                        key = row.get("param")
                        if not key:
                            continue
                        grouped.setdefault(key, []).append(row)
                    for key, rows in grouped.items():
                        ret_avg = sum(float(r.get("total_return", 0) or 0) for r in rows) / len(
                            rows
                        )
                        sharpe_avg = sum(float(r.get("sharpe_ratio", 0) or 0) for r in rows) / len(
                            rows
                        )
                        dd_avg = sum(float(r.get("max_drawdown", 0) or 0) for r in rows) / len(rows)
                        lines.append(
                            f"{key} | 收益:{ret_avg:.4f} | 夏普:{sharpe_avg:.4f} | 回撤:{dd_avg:.4f}"
                        )
                warnings = results.get("overfit_warnings", []) if isinstance(results, dict) else []
                if warnings:
                    lines.append("过拟合警戒：")
                    lines.extend(warnings)
                self.optimize_sensitivity_text.setPlainText("\n".join(lines))
            return
        factor_compare_mode = bool(
            isinstance(results, dict)
            and isinstance(results.get("factor_compare_results", []), list)
            and len(results.get("factor_compare_results", [])) > 0
        )
        table_rows: list[list[str]] = []
        for row in data:
            if isinstance(row, dict):
                params = row.get("params", row.get("param", ""))
                table_rows.append(
                    [
                        str(params),
                        str(row.get("return", row.get("total_return", "-"))),
                        str(row.get("sharpe", row.get("sharpe_ratio", "-"))),
                        str(row.get("drawdown", row.get("max_drawdown", "-"))),
                        str(row.get("ic_mean", "-")),
                        str(row.get("ic_ir", "-")),
                    ]
                )
            else:
                table_rows.append([str(row), "-", "-", "-", "-", "-"])
        self._render_table_rows(self.optimize_table, table_rows)
        if factor_compare_mode:
            for i, row in enumerate(data):
                if not isinstance(row, dict):
                    continue
                ic_mean = float(row.get("ic_mean", 0.0) or 0.0)
                ic_ir = float(row.get("ic_ir", 0.0) or 0.0)
                mean_item = self.optimize_table.item(i, 4)
                ir_item = self.optimize_table.item(i, 5)
                if mean_item is not None:
                    mean_item.setForeground(QColor("#2e7d32") if ic_mean >= 0 else QColor("#c62828"))
                if ir_item is not None:
                    ir_item.setForeground(QColor("#1565c0") if ic_ir >= 0 else QColor("#ef6c00"))
        self.update_optimize_rank_chart(data)
        if hasattr(self, "optimize_sensitivity_text"):
            sensitivity = (
                results.get("sensitivity_results", []) if isinstance(results, dict) else []
            )
            factor_compare = (
                results.get("factor_compare_results", []) if isinstance(results, dict) else []
            )
            if not sensitivity:
                if factor_compare:
                    lines = ["节点参数组对比："]
                    for row in factor_compare:
                        lines.append(
                            f"{row.get('params')} | 收益:{float(row.get('total_return', 0) or 0):.4f} "
                            f"| 夏普:{float(row.get('sharpe_ratio', 0) or 0):.4f} "
                            f"| 回撤:{float(row.get('max_drawdown', 0) or 0):.4f} "
                            f"| IC均值:{float(row.get('ic_mean', 0) or 0):.4f} "
                            f"| ICIR:{float(row.get('ic_ir', 0) or 0):.4f}"
                        )
                    self.optimize_sensitivity_text.setPlainText("\n".join(lines))
                else:
                    self.optimize_sensitivity_text.setPlainText("暂无敏感性分析")
            else:
                lines = ["参数敏感性(±10%)："]
                for row in sensitivity:
                    lines.append(
                        f"{row.get('param')}={row.get('value')}({row.get('delta')}) "
                        f"| 收益:{float(row.get('total_return', 0) or 0):.4f} "
                        f"| 夏普:{float(row.get('sharpe_ratio', 0) or 0):.4f} "
                        f"| 回撤:{float(row.get('max_drawdown', 0) or 0):.4f}"
                    )
                wf_sensitivity = (
                    results.get("walk_forward_sensitivity", []) if isinstance(results, dict) else []
                )
                if wf_sensitivity:
                    lines.append("Walk-Forward 多阶段敏感性(均值)：")
                    grouped = {}
                    for row in wf_sensitivity:
                        key = row.get("param")
                        if not key:
                            continue
                        grouped.setdefault(key, []).append(row)
                    for key, group_rows in grouped.items():
                        ret_avg = sum(
                            float(r.get("total_return", 0) or 0) for r in group_rows
                        ) / len(group_rows)
                        sharpe_avg = sum(
                            float(r.get("sharpe_ratio", 0) or 0) for r in group_rows
                        ) / len(group_rows)
                        dd_avg = sum(
                            float(r.get("max_drawdown", 0) or 0) for r in group_rows
                        ) / len(group_rows)
                        lines.append(
                            f"{key} | 收益:{ret_avg:.4f} | 夏普:{sharpe_avg:.4f} | 回撤:{dd_avg:.4f}"
                        )
                warnings = results.get("overfit_warnings", []) if isinstance(results, dict) else []
                if warnings:
                    lines.append("过拟合警戒：")
                    lines.extend(warnings)
                self.optimize_sensitivity_text.setPlainText("\n".join(lines))

    def copy_best_factor_scenario_params(self) -> None:
        if not isinstance(self.current_results, dict):
            QMessageBox.information(self, "复制参数", "暂无回测结果可复制")
            return
        rows = self.current_results.get("factor_compare_results", [])
        if not isinstance(rows, list) or not rows:
            QMessageBox.information(self, "复制参数", "当前结果不包含场景对比")
            return
        best = max(
            [r for r in rows if isinstance(r, dict)],
            key=lambda r: float(r.get("score", 0.0) or 0.0),
            default=None,
        )
        if not isinstance(best, dict):
            QMessageBox.information(self, "复制参数", "未找到可复制的场景参数")
            return
        payload = best.get("scenario_overrides", {})
        if not isinstance(payload, dict):
            QMessageBox.information(self, "复制参数", "最佳场景不包含参数覆盖")
            return
        payload_text = json.dumps(payload, ensure_ascii=False, indent=2)
        clipboard = QApplication.clipboard()
        if clipboard is not None:
            clipboard.setText(payload_text)
        strategy_name = self.strategy_combo.currentText()
        target = self.strategy_param_widgets.get(strategy_name, {}).get("workflow_node_overrides")
        if isinstance(target, QTextEdit):
            target.setPlainText(payload_text)
        QMessageBox.information(
            self,
            "复制参数",
            f"已复制最佳场景参数: {best.get('params', 'best')}\n并同步写入当前参数编辑区",
        )

    def update_walk_forward_tab(self, results: dict[str, Any]):
        data = results.get("walk_forward_results", []) if isinstance(results, dict) else []
        if not data:
            self.walk_forward_table.setRowCount(1)
            for j, text in enumerate(["暂无结果", "-", "-", "-", "-", "-"]):
                self.walk_forward_table.setItem(0, j, QTableWidgetItem(text))
            if hasattr(self, "walk_forward_curve_chart"):
                self.walk_forward_curve_chart.plot_series({}, "分段净值曲线", "净值")
            return
        rows = []
        for row in data:
            if isinstance(row, dict):
                items = [
                    row.get("window", ""),
                    row.get("train", ""),
                    row.get("test", ""),
                    row.get("return", row.get("total_return", "")),
                    row.get("sharpe", row.get("sharpe_ratio", "")),
                    row.get("drawdown", row.get("max_drawdown", "")),
                ]
            else:
                items = list(row) if isinstance(row, (list, tuple)) else [str(row)]
                items = (items + [""] * 6)[:6]
            rows.append([str(val) for val in items])
        self._render_table_rows(self.walk_forward_table, rows)
        series_map = {}
        for row in data:
            if not isinstance(row, dict):
                continue
            curve = row.get("curve", {})
            values = curve.get("values", [])
            if values:
                series_map[f"窗口{row.get('window', '')}"] = values
        if hasattr(self, "walk_forward_curve_chart"):
            self.walk_forward_curve_chart.plot_series(series_map, "分段净值曲线", "净值")

    def update_cost_text(self, data: dict[str, Any]):
        if "estimated_cost" in data:
            lines = [
                f"估算成交次数: {data.get('trade_count')}",
                f"估算成交额: {data.get('turnover', 0):,.2f}",
                f"手续费率: {data.get('commission_rate', 0):.4%}",
                f"滑点(bps): {data.get('slippage_bps', 0)}",
                f"估算成本: {data.get('estimated_cost', 0):,.2f}",
            ]
        else:
            lines = [
                f"成本模型: {data.get('cost_model')}",
                f"手续费率: {data.get('commission'):.4%}",
                f"滑点(bps): {data.get('slippage_bps')}",
                f"仓位控制: {data.get('position_sizing')}",
                f"单票上限: {data.get('max_position_pct')}%",
                f"再平衡周期: {data.get('rebalance_freq')}",
            ]
        self.cost_text.setPlainText("\n".join(lines))

    def update_weights_tab(self, records: list[dict[str, Any]]):
        if not records:
            self.weights_table.setColumnCount(1)
            self.weights_table.setHorizontalHeaderLabels(["日期"])
            self.weights_table.setRowCount(1)
            self.weights_table.setItem(0, 0, QTableWidgetItem("暂无结果"))
            self.update_weight_charts([])
            return
        symbols = list(records[0].get("weights", {}).keys())
        self.weights_table.setColumnCount(1 + len(symbols))
        self.weights_table.setHorizontalHeaderLabels(["日期"] + symbols)
        rows = []
        for i, record in enumerate(records):
            date = record.get("date")
            if isinstance(date, (datetime, pd.Timestamp)):
                date_text = date.strftime("%Y-%m-%d")
            else:
                date_text = str(date)
            weights = record.get("weights", {})
            row = [date_text]
            for symbol in symbols:
                val = weights.get(symbol, 0)
                row.append(f"{val:.4%}" if isinstance(val, (int, float)) else str(val))
            rows.append(row)
        self._render_table_rows(self.weights_table, rows)
        self.update_weight_charts(records)

    def update_optimize_rank_chart(self, data: list[dict[str, Any]]):
        if not hasattr(self, "optimize_rank_chart"):
            return
        if not data:
            self.optimize_rank_chart.plot_bars([], [], "优化排名图", "组合评分")
            return
        scores = []
        for row in data:
            if not isinstance(row, dict):
                continue
            score = row.get("score")
            if score is None:
                score = float(row.get("sharpe_ratio", 0)) * float(row.get("total_return", 0))
            scores.append(float(score))
        if not scores:
            self.optimize_rank_chart.plot_bars([], [], "优化排名图", "组合评分")
            return
        top_n = min(10, len(scores))
        sorted_scores = sorted(scores, reverse=True)[:top_n]
        labels = [str(i + 1) for i in range(len(sorted_scores))]
        self.optimize_rank_chart.plot_bars(labels, sorted_scores, "优化排名图", "组合评分")

    def update_weight_charts(self, records: list[dict[str, Any]]):
        if not hasattr(self, "weights_chart") or not hasattr(self, "turnover_chart"):
            return
        if not records:
            self.weights_chart.plot_series({}, "组合权重时序", "权重")
            self.turnover_chart.plot_series({}, "换手率时序", "换手率")
            return
        weight_series: dict[str, list[float]] = {}
        for record in records:
            weights = record.get("weights", {})
            for sym, val in weights.items():
                weight_series.setdefault(sym, []).append(float(val))
        turnover_series = []
        last = None
        for record in records:
            current = record.get("weights", {})
            if last is not None:
                keys = set(last.keys()) | set(current.keys())
                diff = 0.0
                for key in keys:
                    diff += abs(float(current.get(key, 0)) - float(last.get(key, 0)))
                turnover_series.append(diff)
            last = current
        self.weights_chart.plot_series(weight_series, "组合权重时序", "权重")
        self.turnover_chart.plot_series({"换手率": turnover_series}, "换手率时序", "换手率")

    def update_factor_analysis_visuals(self, factor_context: dict[str, Any]):
        if not hasattr(self, "factor_quantile_chart") or not hasattr(self, "ic_series_chart"):
            return
        if not isinstance(factor_context, dict):
            self.factor_quantile_chart.plot_series({}, "因子分层净值图", "净值")
            self.ic_series_chart.plot_series({}, "IC时序图", "IC")
            return
        if not factor_context:
            self.factor_quantile_chart.plot_series({}, "因子分层净值图", "净值")
            self.ic_series_chart.plot_series({}, "IC时序图", "IC")
            return
        q_map: dict[str, list[float]] = {}
        detailed = self.current_results.get("detailed_results", {}) if isinstance(self.current_results, dict) else {}
        if isinstance(detailed, dict):
            quantiles = detailed.get("quantile_results", {})
            if isinstance(quantiles, dict):
                for q_name, q_val in quantiles.items():
                    if not isinstance(q_val, dict):
                        continue
                    rs = q_val.get("returns")
                    if isinstance(rs, pd.Series) and not rs.empty:
                        curve = (1.0 + pd.to_numeric(rs, errors="coerce").dropna()).cumprod()
                        q_map[str(q_name)] = [float(v) for v in curve.values]
        self.factor_quantile_chart.plot_series(q_map, "因子分层净值图", "净值")
        ic_map: dict[str, list[float]] = {}
        ic_summary = factor_context.get("ic_summary", {})
        if isinstance(ic_summary, dict):
            ic_mean = float(ic_summary.get("ic_mean", 0.0) or 0.0)
            ic_ir = float(ic_summary.get("ic_ir", 0.0) or 0.0)
            ic_prob = float(ic_summary.get("ic_prob", 0.0) or 0.0)
            ic_map = {
                "IC均值": [ic_mean, ic_mean, ic_mean],
                "ICIR": [ic_ir, ic_ir, ic_ir],
                "IC正值占比": [ic_prob, ic_prob, ic_prob],
            }
        self.ic_series_chart.plot_series(ic_map, "IC时序图", "IC")

    def on_data_source_changed(self, text: str):
        """数据源选择改变时的处理"""
        if DataSource is None or self.data_manager is None:
            return
        data_source = cast(Any, DataSource)
        dm = cast(Any, self.data_manager)

        # 根据选择设置数据源
        if "强制DuckDB" in text:
            dm.set_preferred_source(data_source.DUCKDB)
        elif "强制本地缓存" in text:
            dm.set_preferred_source(data_source.LOCAL)
        elif "强制QMT" in text:
            dm.set_preferred_source(data_source.QMT)
        elif "强制QStock" in text:
            dm.set_preferred_source(data_source.QSTOCK)
        elif "强制AKShare" in text:
            dm.set_preferred_source(data_source.AKSHARE)
        else:  # 自动选择
            dm.set_preferred_source(None)

        # 更新状态显示
        self.update_connection_status()

    def apply_styles(self):
        """应用样式"""
        self.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 2px solid #cccccc;
                border-radius: 5px;
                margin-top: 1ex;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
            QTabWidget::pane {
                border: 1px solid #cccccc;
                border-radius: 5px;
            }
            QTabBar::tab {
                background-color: #f0f0f0;
                padding: 8px 16px;
                margin-right: 2px;
                border-top-left-radius: 5px;
                border-top-right-radius: 5px;
            }
            QTabBar::tab:selected {
                background-color: white;
                border-bottom: 2px solid #2196F3;
            }
        """)

    def load_splitter_state(self):
        settings = QSettings("EasyXT", "BacktestWidget")
        state = settings.value("main_splitter_state")
        if state:
            self.main_splitter.restoreState(state)

    def save_splitter_state(self, *args):
        settings = QSettings("EasyXT", "BacktestWidget")
        settings.setValue("main_splitter_state", self.main_splitter.saveState())

    def on_zoom_lock_changed(self, checked: bool):
        if hasattr(self, "portfolio_chart"):
            self.portfolio_chart.set_zoom_lock(checked)
            self.portfolio_chart.refresh_plot()

    def load_view_presets(self):
        if not hasattr(self, "portfolio_chart"):
            return
        settings = QSettings("EasyXT", "BacktestWidget")
        raw = settings.value("chart_view_presets", "")
        presets = {}
        if raw:
            try:
                presets = json.loads(raw)
            except Exception:
                presets = {}
        self.portfolio_chart.import_view_presets(presets)
        preset_names = list(presets.keys())
        self.preset_combo.blockSignals(True)
        self.preset_combo.clear()
        for name in preset_names:
            self.preset_combo.addItem(name)
            tag = self.portfolio_chart.get_preset_tag(name)
            if tag:
                index = self.preset_combo.findText(name)
                self.preset_combo.setItemData(index, tag, Qt.ToolTipRole)
        current_name = settings.value("chart_view_preset_current", "")
        if current_name and current_name in presets:
            self.preset_combo.setCurrentText(current_name)
            self.portfolio_chart.apply_view_preset(current_name)
        ratio_text = settings.value("chart_view_window_ratio", "")
        if ratio_text in ["20%", "50%", "80%"]:
            self.window_ratio_combo.setCurrentText(ratio_text)
        else:
            self.window_ratio_combo.setCurrentText("50%")
        self.on_window_ratio_changed(self.window_ratio_combo.currentText())
        self.preset_combo.blockSignals(False)

    def save_view_presets(self):
        if not hasattr(self, "portfolio_chart"):
            return
        settings = QSettings("EasyXT", "BacktestWidget")
        presets = self.portfolio_chart.export_view_presets()
        settings.setValue("chart_view_presets", json.dumps(presets, ensure_ascii=False))
        settings.setValue(
            "chart_view_preset_current", self.portfolio_chart.get_current_preset_name()
        )
        settings.setValue("chart_view_window_ratio", self.window_ratio_combo.currentText())

    def reset_chart_view(self):
        if hasattr(self, "portfolio_chart"):
            self.zoom_lock_checkbox.setChecked(False)
            self.portfolio_chart.reset_view()

    def center_chart_view(self):
        if hasattr(self, "portfolio_chart"):
            self.zoom_lock_checkbox.setChecked(True)
            self.portfolio_chart.center_view()

    def fix_y_range(self):
        if hasattr(self, "portfolio_chart"):
            self.portfolio_chart.fix_y_range()

    def fix_x_range(self):
        if hasattr(self, "portfolio_chart"):
            self.portfolio_chart.fix_x_range()

    def prev_window_view(self):
        if hasattr(self, "portfolio_chart"):
            self.zoom_lock_checkbox.setChecked(True)
            self.portfolio_chart.prev_window_view()

    def next_window_view(self):
        if hasattr(self, "portfolio_chart"):
            self.zoom_lock_checkbox.setChecked(True)
            self.portfolio_chart.next_window_view()

    def save_view_preset(self):
        if not hasattr(self, "portfolio_chart"):
            return
        name, ok = QInputDialog.getText(self, "保存视图范围", "请输入预设名称:")
        if not ok:
            return
        preset_name = str(name).strip()
        if not preset_name:
            return
        tag = self.build_view_preset_tag()
        self.portfolio_chart.save_view_preset(preset_name, tag)
        if self.preset_combo.findText(preset_name) == -1:
            self.preset_combo.addItem(preset_name)
        tag = self.portfolio_chart.get_preset_tag(preset_name)
        if tag:
            index = self.preset_combo.findText(preset_name)
            self.preset_combo.setItemData(index, tag, Qt.ToolTipRole)
        self.preset_combo.setCurrentText(preset_name)
        self.save_view_presets()

    def apply_view_preset(self, name: str):
        if hasattr(self, "portfolio_chart") and name:
            self.zoom_lock_checkbox.setChecked(True)
            self.portfolio_chart.apply_view_preset(name)
            self.save_view_presets()

    def rename_view_preset(self):
        if not hasattr(self, "portfolio_chart"):
            return
        old_name = self.preset_combo.currentText()
        if not old_name:
            return
        new_name, ok = QInputDialog.getText(
            self, "重命名预设", "请输入新的预设名称:", text=old_name
        )
        if not ok:
            return
        new_name = str(new_name).strip()
        if not new_name or new_name == old_name:
            return
        if not self.portfolio_chart.rename_view_preset(old_name, new_name):
            return
        idx = self.preset_combo.findText(old_name)
        if idx >= 0:
            self.preset_combo.setItemText(idx, new_name)
            tag = self.portfolio_chart.get_preset_tag(new_name)
            if tag:
                self.preset_combo.setItemData(idx, tag, Qt.ToolTipRole)
        self.preset_combo.setCurrentText(new_name)
        self.save_view_presets()

    def delete_view_preset(self):
        if not hasattr(self, "portfolio_chart"):
            return
        name = self.preset_combo.currentText()
        if not name:
            return
        if not self.portfolio_chart.delete_view_preset(name):
            return
        idx = self.preset_combo.findText(name)
        if idx >= 0:
            self.preset_combo.removeItem(idx)
        self.save_view_presets()

    def on_window_ratio_changed(self, text: str):
        if not hasattr(self, "portfolio_chart"):
            return
        mapping = {"20%": 0.2, "50%": 0.5, "80%": 0.8}
        ratio = mapping.get(text, 0.5)
        self.portfolio_chart.set_window_ratio(ratio)
        self.save_view_presets()

    def build_view_preset_tag(self):
        strategy = self.strategy_combo.currentText() if hasattr(self, "strategy_combo") else ""
        period = self.period_combo.currentText() if hasattr(self, "period_combo") else ""
        if hasattr(self, "multi_asset_checkbox") and self.multi_asset_checkbox.isChecked():
            symbols = self.multi_asset_edit.text().strip()
            symbol_text = symbols if symbols else "多标的"
        else:
            symbol_text = (
                self.stock_code_edit.text().strip() if hasattr(self, "stock_code_edit") else ""
            )
        parts = [p for p in [strategy, symbol_text, period] if p]
        return " | ".join(parts)

    def start_backtest(self):
        """开始回测"""
        try:
            self.update_backtest_engine_status()
            engine_status = getattr(self, "_backtest_engine_status", {})
            if isinstance(engine_status, dict) and not bool(engine_status.get("available", False)):
                hint = engine_status.get("hint") or engine_status.get("error_message") or "当前没有可用的真实回测引擎，已禁止模拟/降级回测。"
                QMessageBox.critical(
                    self,
                    "错误",
                    f"回测引擎不可用，已禁止模拟/降级回测。\n\n{hint}",
                )
                return

            # 检查引擎是否可用
            if AdvancedBacktestEngine is None:
                QMessageBox.critical(self, "错误", "回测引擎不可用，请检查模块安装")
                return

            # 验证参数
            if not self.validate_parameters():
                return

            # 获取参数
            params = self.get_backtest_parameters()
            self.update_config_summary(params)
            self.update_cost_text(params)
            self.update_data_summary({})
            self.update_optimize_tab({})
            self.update_walk_forward_tab({})
            self.update_weights_tab([])
            self.update_factor_analysis_visuals({})

            # 显示回测参数信息
            print("📊 开始回测:")
            print(f"  股票代码: {params['stock_code']}")
            print(f"  时间范围: {params['start_date']} ~ {params['end_date']}")
            print(f"  初始资金: {params['initial_cash']:,.0f} 元")
            print(f"  复权类型: {params['adjust']}")  # ← 添加复权信息
            print(f"  K线周期: {params['period']}")

            # 更新UI状态
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(True)
            self.export_button.setEnabled(False)

            # 重置进度
            self.progress_bar.setValue(0)
            self.status_label.setText("🚀 准备开始回测...")

            # 创建并启动工作线程
            self.backtest_worker = BacktestWorker(params)
            self.backtest_worker.progress_updated.connect(self.update_progress)
            self.backtest_worker.status_updated.connect(self.update_status)
            self.backtest_worker.results_ready.connect(self.handle_results)
            self.backtest_worker.error_occurred.connect(self.handle_error)
            self.backtest_worker.finished.connect(self.backtest_finished)

            self.backtest_worker.start()

        except Exception as e:
            QMessageBox.critical(self, "错误", f"启动回测失败: {str(e)}")
            self.backtest_finished()

    def stop_backtest(self):
        """停止回测"""
        if self.backtest_worker and self.backtest_worker.isRunning():
            self.backtest_worker.stop()
            self.status_label.setText("⏹️ 正在停止回测...")
            self.stop_button.setEnabled(False)
            QTimer.singleShot(2000, self.force_stop_backtest)

    def force_stop_backtest(self):
        if self.backtest_worker and self.backtest_worker.isRunning():
            self.backtest_worker.terminate()
            self.backtest_worker.wait(1000)
            self.status_label.setText("⛔ 回测已强制停止")

    def validate_parameters(self) -> bool:
        """验证参数"""
        # 检查股票代码
        if not self.stock_code_edit.text().strip():
            QMessageBox.warning(self, "参数错误", "请输入股票代码")
            return False

        # 检查日期范围
        start_date = self.start_date_edit.date().toPyDate()
        end_date = self.end_date_edit.date().toPyDate()

        if start_date >= end_date:
            QMessageBox.warning(self, "参数错误", "开始日期必须早于结束日期")
            return False

        strategy_name = self.strategy_combo.currentText()
        strategy_cfg = self.strategy_registry.get(strategy_name, {})
        if strategy_cfg.get("class") is None:
            QMessageBox.warning(self, "参数错误", "策略不可用，请检查依赖或选择其他策略")
            return False
        strategy_params = self.get_strategy_params_from_ui(strategy_name)
        if strategy_name in ["双均线策略", "MACD策略"]:
            if strategy_name == "MACD策略":
                fast_period = strategy_params.get("fast_period", 0)
                slow_period = strategy_params.get("slow_period", 1)
                if fast_period >= slow_period:
                    QMessageBox.warning(self, "参数错误", "快线周期必须小于慢线周期")
                    return False
            elif strategy_params.get("short_period", 0) >= strategy_params.get("long_period", 1):
                QMessageBox.warning(self, "参数错误", "短期周期必须小于长期周期")
                return False
        if strategy_name == "固定网格策略":
            if strategy_params.get("grid_count", 0) < 2:
                QMessageBox.warning(self, "参数错误", "网格数量需大于1")
                return False
            if strategy_params.get("price_range", 0) <= 0:
                QMessageBox.warning(self, "参数错误", "价格区间需大于0")
                return False
        if strategy_name == "自适应网格策略":
            if (
                strategy_params.get("buy_threshold", 0) <= 0
                or strategy_params.get("sell_threshold", 0) <= 0
            ):
                QMessageBox.warning(self, "参数错误", "阈值需大于0")
                return False
        if strategy_name == "ATR网格策略":
            if strategy_params.get("atr_period", 0) <= 1:
                QMessageBox.warning(self, "参数错误", "ATR周期需大于1")
                return False
            if strategy_params.get("atr_multiplier", 0) <= 0:
                QMessageBox.warning(self, "参数错误", "ATR倍数需大于0")
                return False
        if strategy_name == "101因子工作流策略":
            workflow_path = str(strategy_params.get("workflow_path") or "").strip()
            if not workflow_path:
                QMessageBox.warning(self, "参数错误", "请配置101因子工作流文件路径")
                return False
            overrides = strategy_params.get("workflow_node_overrides")
            if overrides is not None and not isinstance(overrides, dict):
                QMessageBox.warning(self, "参数错误", "节点参数覆盖需为JSON对象")
                return False
            if isinstance(overrides, dict) and "__scenarios__" in overrides:
                scenarios = overrides.get("__scenarios__")
                if not isinstance(scenarios, list):
                    QMessageBox.warning(self, "参数错误", "__scenarios__ 必须为数组")
                    return False
                for item in scenarios:
                    if not isinstance(item, dict):
                        QMessageBox.warning(self, "参数错误", "__scenarios__ 项必须为对象")
                        return False
                    if not isinstance(item.get("overrides", {}), dict):
                        QMessageBox.warning(self, "参数错误", "场景overrides必须为对象")
                        return False
            if not os.path.isabs(workflow_path):
                workflow_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "..", workflow_path)
                )
            if not os.path.exists(workflow_path):
                QMessageBox.warning(self, "参数错误", f"工作流文件不存在: {workflow_path}")
                return False

        if self.multi_asset_checkbox.isChecked():
            symbols = [s.strip() for s in self.multi_asset_edit.text().split(",") if s.strip()]
            if not symbols:
                QMessageBox.warning(self, "参数错误", "多标的组合回测需要填写标的列表")
                return False

        if self.walk_forward_checkbox.isChecked():
            total_days = (end_date - start_date).days
            if total_days <= 0:
                QMessageBox.warning(self, "参数错误", "日期范围不足以进行分段回测")
                return False
            if self.wf_window_spin.value() >= total_days:
                QMessageBox.warning(self, "参数错误", "窗口长度需小于总回测天数")
                return False
            if self.wf_step_spin.value() >= self.wf_window_spin.value():
                QMessageBox.warning(self, "参数错误", "步长需小于窗口长度")
                return False

        return True

    def validate_data_parameters(self) -> bool:
        if not self.stock_code_edit.text().strip():
            QMessageBox.warning(self, "参数错误", "请输入股票代码")
            return False
        start_date = self.start_date_edit.date().toPyDate()
        end_date = self.end_date_edit.date().toPyDate()
        if start_date >= end_date:
            QMessageBox.warning(self, "参数错误", "开始日期必须早于结束日期")
            return False
        if self.multi_asset_checkbox.isChecked():
            symbols = [s.strip() for s in self.multi_asset_edit.text().split(",") if s.strip()]
            if not symbols:
                QMessageBox.warning(self, "参数错误", "多标的组合回测需要填写标的列表")
                return False
        return True

    def get_backtest_parameters(self) -> dict[str, Any]:
        """获取回测参数（包含复权类型）"""
        # 获取复权类型（映射表见模块级 ADJUST_TEXT_MAP / PERIOD_TEXT_MAP）
        adjust_text = self.adjust_combo.currentText()
        adjust = ADJUST_TEXT_MAP.get(adjust_text, "none")
        period_text = self.period_combo.currentText()
        period = PERIOD_TEXT_MAP.get(period_text, "1d")

        strategy_name = self.strategy_combo.currentText()
        strategy_params = self.get_strategy_params_from_ui(strategy_name)
        short_period = strategy_params.get("short_period", strategy_params.get("fast_period"))
        long_period = strategy_params.get("long_period", strategy_params.get("slow_period"))
        rsi_period = strategy_params.get("rsi_period", strategy_params.get("signal_period"))

        def parse_space(text: str):
            result: dict[str, list[float]] = {}
            if not text:
                return result
            parts = [p.strip() for p in text.split(";") if p.strip()]
            for part in parts:
                if "=" not in part:
                    continue
                key, values = part.split("=", 1)
                key = key.strip()
                items = [v.strip() for v in values.split(",") if v.strip()]
                parsed = []
                for v in items:
                    try:
                        if "." in v:
                            parsed.append(float(v))
                        else:
                            parsed.append(int(v))
                    except Exception:
                        continue
                if parsed:
                    result[key] = parsed
            return result

        custom_search_space = {
            "fixed_grid": parse_space(self.fixed_grid_space_edit.text().strip()),
            "adaptive_grid": parse_space(self.adaptive_grid_space_edit.text().strip()),
            "atr_grid": parse_space(self.atr_grid_space_edit.text().strip()),
        }
        return {
            "stock_code": self.stock_code_edit.text().strip(),
            "start_date": self.start_date_edit.date().toPyDate().strftime("%Y-%m-%d"),
            "end_date": self.end_date_edit.date().toPyDate().strftime("%Y-%m-%d"),
            "initial_cash": self.initial_cash_spin.value(),
            "commission": self.commission_spin.value() / 100,  # 转换为小数
            "short_period": short_period,
            "long_period": long_period,
            "rsi_period": rsi_period,
            "strategy_name": strategy_name,
            "strategy_params": strategy_params,
            "adjust": adjust,  # ← 添加复权参数
            "period": period,
            "optimize_enabled": self.optimize_checkbox.isChecked(),
            "benchmark_enabled": self.benchmark_checkbox.isChecked(),
            "risk_analysis_enabled": self.risk_analysis_checkbox.isChecked(),
            "walk_forward_enabled": self.walk_forward_checkbox.isChecked(),
            "overfit_warning_enabled": self.overfit_warning_checkbox.isChecked(),
            "overfit_sharpe_drop": self.overfit_sharpe_drop_spin.value(),
            "overfit_drawdown_increase": self.overfit_drawdown_increase_spin.value(),
            "oos_ratio": self.oos_ratio_spin.value(),
            "wf_window_days": self.wf_window_spin.value(),
            "wf_step_days": self.wf_step_spin.value(),
            "optimize_method": self.optimize_method_combo.currentText(),
            "optimize_trials": self.optimize_trials_spin.value(),
            "custom_search_space": custom_search_space,
            "cost_model": self.cost_model_combo.currentText(),
            "slippage_bps": self.slippage_spin.value(),
            "position_sizing": self.position_sizing_combo.currentText(),
            "max_position_pct": self.max_position_spin.value(),
            "rebalance_freq": self.rebalance_combo.currentText(),
            "multi_asset_enabled": self.multi_asset_checkbox.isChecked(),
            "multi_asset_list": [
                s.strip() for s in self.multi_asset_edit.text().split(",") if s.strip()
            ],
        }

    def update_progress(self, value: int):
        """更新进度"""
        self.progress_bar.setValue(value)

    def update_status(self, status: str):
        """更新状态"""
        self.status_label.setText(status)

    def handle_results(self, results: dict[str, Any]):
        """处理回测结果（分步渲染，避免主线程长时间阻塞）"""
        self.current_results = results
        steps: list[Callable[[], None]] = []
        steps.append(lambda: self.update_overview_tab(results))
        steps.append(lambda: self.update_metrics_tab(results))
        steps.append(lambda: self.update_risk_tab(results))
        steps.append(lambda: self.update_trades_tab(results))
        steps.append(lambda: self.update_optimize_tab(results))
        steps.append(lambda: self.update_walk_forward_tab(results))
        steps.append(lambda: self.update_data_summary(results.get("data_summary", {})))
        steps.append(lambda: self.update_factor_analysis_visuals(results.get("factor_context", {})))
        backtest_params = results.get("backtest_params", {})
        if backtest_params:
            def update_config_summary_step() -> None:
                self.update_config_summary(backtest_params, results.get("factor_context", {}))

            def update_cost_text_step() -> None:
                self.update_cost_text(backtest_params)

            steps.append(update_config_summary_step)
            steps.append(update_cost_text_step)
        cost_analysis = results.get("cost_analysis", {})
        if cost_analysis:
            def update_cost_analysis_step() -> None:
                self.update_cost_text(cost_analysis)

            steps.append(update_cost_analysis_step)
        weight_records = results.get("weight_records", [])
        def update_weights_step() -> None:
            self.update_weights_tab(weight_records)

        steps.append(update_weights_step)
        steps.append(lambda: self._finalize_results(results))
        self._result_update_queue = steps
        self._process_next_result_step()

    def _process_next_result_step(self):
        try:
            if not getattr(self, "_result_update_queue", None):
                return
            if not self._result_update_queue:
                return
            step = self._result_update_queue.pop(0)
            try:
                step()
            except Exception:
                pass
            QTimer.singleShot(0, self._process_next_result_step)
        except Exception:
            pass

    def _finalize_results(self, results: dict[str, Any]):
        try:
            self.export_button.setEnabled(True)
            backtest_params = (
                results.get("backtest_params", {}) if isinstance(results, dict) else {}
            )
            symbol = (
                backtest_params.get("stock_code") if isinstance(backtest_params, dict) else None
            )
            signal_bus.emit(Events.STRATEGY_STOPPED, results=results, symbol=symbol)
        except Exception:
            pass

    def handle_error(self, error_msg: str):
        """处理错误"""
        QMessageBox.critical(self, "回测错误", error_msg)
        self.status_label.setText(f"❌ 回测失败: {error_msg}")

    def backtest_finished(self):
        """回测完成"""
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)

        if self.backtest_worker:
            self.backtest_worker.deleteLater()
            self.backtest_worker = None

    def update_overview_tab(self, results: dict[str, Any]):
        """更新性能概览标签页"""
        metrics = results.get("performance_metrics", {})

        # 更新指标卡片 - 使用更高精度显示
        self.total_return_card.value_label.setText(f"{metrics.get('total_return', 0):.4%}")
        self.annual_return_card.value_label.setText(f"{metrics.get('annualized_return', 0):.4%}")
        self.sharpe_card.value_label.setText(f"{metrics.get('sharpe_ratio', 0):.3f}")
        self.drawdown_card.value_label.setText(f"{metrics.get('max_drawdown', 0):.4%}")

        # 更新净值曲线图表
        try:
            portfolio_curve = results.get("portfolio_curve", {})
            if portfolio_curve and "dates" in portfolio_curve and "values" in portfolio_curve:
                dates = portfolio_curve["dates"]
                values = portfolio_curve["values"]
                initial_value = results.get("backtest_params", {}).get("initial_cash", 100000)
                benchmark = None
                min_len = None
                trades = []
                stock_data = results.get("stock_data")
                if (
                    isinstance(stock_data, pd.DataFrame)
                    and "close" in stock_data.columns
                    and len(stock_data) > 0
                ):
                    close = pd.to_numeric(stock_data["close"], errors="coerce").dropna()
                    if len(close) > 1:
                        bench = (close / close.iloc[0]).tolist()
                        min_len = min(len(bench), len(values))
                        benchmark = bench[:min_len]
                        dates = dates[:min_len]
                        values = values[:min_len]
                daily_holdings = []
                detailed = results.get("detailed_results", {})
                if isinstance(detailed, dict):
                    trades = detailed.get("trades", [])
                    daily_holdings = detailed.get("daily_holdings", [])
                    if (
                        isinstance(daily_holdings, list)
                        and min_len is not None
                        and len(daily_holdings) >= min_len
                    ):
                        daily_holdings = daily_holdings[:min_len]
                self.portfolio_chart.plot_portfolio_curve(
                    dates, values, initial_value, benchmark, trades, daily_holdings
                )
                net_values = [v / initial_value for v in values] if initial_value else values
                sample = net_values[-80:] if len(net_values) > 80 else net_values
                for card in [
                    self.total_return_card,
                    self.annual_return_card,
                    self.sharpe_card,
                    self.drawdown_card,
                ]:
                    if hasattr(card, "sparkline"):
                        card.sparkline.set_data(sample, getattr(card, "sparkline_color", None))
            else:
                print("⚠️ 没有找到有效的投资组合曲线数据")
        except Exception as e:
            print(f"更新净值曲线时出错: {e}")

    def update_metrics_tab(self, results: dict[str, Any]):
        """更新详细指标标签页"""
        metrics = results.get("performance_metrics", {})
        risk_metrics = results.get("risk_analysis", {})

        # 合并所有指标
        all_metrics = {**metrics, **risk_metrics}
        detailed_results = results.get("detailed_results", {})
        backtest_summary = (
            detailed_results.get("backtest_summary", {})
            if isinstance(detailed_results, dict)
            else {}
        )
        if isinstance(backtest_summary, dict):
            ls = backtest_summary.get("long_short", {})
            if isinstance(ls, dict):
                for k, v in ls.items():
                    if isinstance(v, (int, float)):
                        all_metrics[f"factor_ls_{k}"] = v
        factor_context = results.get("factor_context", {})
        if isinstance(factor_context, dict):
            ic_summary = factor_context.get("ic_summary", {})
            if isinstance(ic_summary, dict):
                for k in ("ic_mean", "ic_ir", "ic_prob"):
                    if isinstance(ic_summary.get(k), (int, float)):
                        all_metrics[f"factor_{k}"] = float(ic_summary[k])
        rows = [
            [self.format_metric_name(key), self.format_metric_value(key, value)]
            for key, value in all_metrics.items()
        ]
        self._render_table_rows(self.metrics_table, rows)

    def update_risk_tab(self, results: dict[str, Any]):
        """更新风险分析标签页"""
        risk_analysis = results.get("risk_analysis", {})

        # 生成风险报告
        if RiskAnalyzer is None:
            return
        risk_analyzer = RiskAnalyzer()
        risk_report = risk_analyzer.generate_risk_report(risk_analysis)

        factor_context = results.get("factor_context", {})
        extra_lines = []
        if isinstance(factor_context, dict):
            ic_summary = factor_context.get("ic_summary", {})
            if isinstance(ic_summary, dict) and ic_summary:
                extra_lines.append(
                    "因子IC: "
                    f"mean={float(ic_summary.get('ic_mean', 0.0)):.4f}, "
                    f"ir={float(ic_summary.get('ic_ir', 0.0)):.4f}, "
                    f"prob={float(ic_summary.get('ic_prob', 0.0)):.4f}"
                )
            workflow_meta = factor_context.get("workflow_meta", {})
            if isinstance(workflow_meta, dict) and workflow_meta:
                extra_lines.append(
                    "工作流: "
                    f"{workflow_meta.get('name', '-')}"
                    f" | node={workflow_meta.get('node_count', '-')}"
                    f" | edge={workflow_meta.get('connection_count', '-')}"
                )
        if extra_lines:
            risk_report = "\n".join(extra_lines) + "\n\n" + risk_report
        self.risk_report_text.setPlainText(risk_report)

    def update_trades_tab(self, results: dict[str, Any]):
        """更新交易记录标签页（分批渲染避免卡顿）"""
        # 从回测结果中提取真实的交易记录
        detailed_results = results.get("detailed_results", {})
        trades_data = detailed_results.get("trades", [])
        if not trades_data and isinstance(detailed_results, dict):
            ls = detailed_results.get("long_short_results", {})
            summary = detailed_results.get("backtest_summary", {})
            if isinstance(ls, dict) and isinstance(summary, dict):
                q_count = len(detailed_results.get("quantile_results", {}) or {})
                s_ls = summary.get("long_short", {}) if isinstance(summary.get("long_short", {}), dict) else {}
                trades_data = [
                    (
                        "因子回测",
                        "多空组合",
                        f"年化:{float(s_ls.get('annual_return', 0.0)):.2%}",
                        f"夏普:{float(s_ls.get('sharpe_ratio', 0.0)):.3f}",
                        f"回撤:{abs(float(s_ls.get('max_drawdown', 0.0))):.2%}",
                        f"分层:{q_count}",
                    )
                ]

        # 如果没有交易记录，显示提示信息
        if not trades_data:
            trades_data = [("无交易记录", "请检查策略参数", "", "", "", "")]

        # 使用分批渲染避免卡顿
        if TABLE_RENDERER_AVAILABLE and len(trades_data) > 100:
            # 大数据量使用分批渲染
            self._render_trades_batch(trades_data)
        else:
            # 小数据量直接渲染
            self._render_trades_direct(trades_data)

    def _render_trades_direct(self, trades_data: list):
        """直接渲染交易记录（适用于小数据量）"""
        self.trades_table.setRowCount(len(trades_data))

        for i, trade in enumerate(trades_data):
            for j, value in enumerate(trade):
                item = QTableWidgetItem(str(value))
                # 根据操作类型设置颜色
                if j == 1:  # 操作列
                    if str(value) == "买入":
                        item.setBackground(QColor(220, 255, 220))  # 浅绿色
                    elif str(value) == "卖出":
                        item.setBackground(QColor(255, 220, 220))  # 浅红色
                # 根据收益设置颜色
                elif j == 5 and str(value).startswith(("+", "-")):  # 收益列
                    if str(value).startswith("+"):
                        item.setBackground(QColor(220, 255, 220))  # 浅绿色
                    elif str(value).startswith("-"):
                        item.setBackground(QColor(255, 220, 220))  # 浅红色

                self.trades_table.setItem(i, j, item)

    def _render_trades_batch(self, trades_data: list):
        """分批渲染交易记录（适用于大数据量）"""
        # 禁用排序以提高性能
        self.trades_table.setSortingEnabled(False)

        # 准备样式数据
        row_colors: list[Optional[QColor]] = []
        for trade in trades_data:
            # 根据操作类型确定行颜色
            if len(trade) > 1:
                op = str(trade[1])
                if op == "买入":
                    row_colors.append(QColor(220, 255, 220))
                elif op == "卖出":
                    row_colors.append(QColor(255, 220, 220))
                else:
                    row_colors.append(None)
            else:
                row_colors.append(None)

        # 设置行数
        self.trades_table.setRowCount(len(trades_data))

        # 分批渲染，每批50行
        batch_size = 50
        total = len(trades_data)

        def render_batch(start_idx: int):
            end_idx = min(start_idx + batch_size, total)

            for i in range(start_idx, end_idx):
                trade = trades_data[i]
                for j, value in enumerate(trade):
                    item = QTableWidgetItem(str(value))

                    # 设置行颜色
                    color = row_colors[i]
                    if color is not None:
                        item.setBackground(color)

                    # 收益列颜色
                    if j == 5 and i < len(trades_data):
                        value_str = str(trade[5]) if len(trade) > 5 else ""
                        if value_str.startswith("+"):
                            item.setBackground(QColor(220, 255, 220))
                        elif value_str.startswith("-"):
                            item.setBackground(QColor(255, 220, 220))

                    self.trades_table.setItem(i, j, item)

            # 如果还有更多数据，安排下一批渲染
            if end_idx < total:
                QTimer.singleShot(1, lambda: render_batch(end_idx))
            else:
                # 渲染完成，重新启用排序
                self.trades_table.setSortingEnabled(True)

        # 开始第一批次渲染
        render_batch(0)

    def _render_table_rows(self, table: QTableWidget, rows: list[list[Any]]):
        if TABLE_RENDERER_AVAILABLE and len(rows) > 100:
            renderer = TableRenderer(table=table, batch_size=50, render_delay_ms=1)
            renderer.render_batch(rows)
            return
        table.setRowCount(len(rows))
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                table.setItem(i, j, QTableWidgetItem(str(value)))

    def format_metric_name(self, key: str) -> str:
        """格式化指标名称"""
        name_mapping = {
            "total_return": "总收益率",
            "annualized_return": "年化收益率",
            "volatility": "年化波动率",
            "sharpe_ratio": "夏普比率",
            "max_drawdown": "最大回撤",
            "win_rate": "胜率",
            "profit_factor": "盈利因子",
            "sqn": "SQN指标",
            "sortino_ratio": "索提诺比率",
            "calmar_ratio": "卡尔马比率",
            "var_95": "95% VaR",
            "cvar_95": "95% CVaR",
        }
        return name_mapping.get(key, key.replace("_", " ").title())

    def format_metric_value(self, key: str, value: Any) -> str:
        """格式化指标数值"""
        if isinstance(value, (int, float)):
            if (
                "return" in key
                or "drawdown" in key
                or "var" in key
                or "cvar" in key
                or "rate" in key
            ):
                return f"{value:.4%}"
            elif "ratio" in key or "factor" in key or "sqn" in key:
                return f"{value:.3f}"
            else:
                return f"{value:.2f}"
        else:
            return str(value)

    def export_html_report(self):
        """导出HTML报告"""
        if not self.current_results:
            QMessageBox.warning(self, "导出失败", "没有可导出的回测结果")
            return

        # 选择保存路径
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "保存HTML报告",
            f"回测报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
            "HTML文件 (*.html)",
        )

        if file_path:
            try:
                # 生成HTML报告
                html_content = self.generate_html_report(self.current_results)

                # 保存文件
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(html_content)

                QMessageBox.information(self, "导出成功", f"HTML报告已保存到:\n{file_path}")

            except Exception as e:
                QMessageBox.critical(self, "导出失败", f"保存HTML报告失败: {str(e)}")

    def generate_html_report(self, results: dict[str, Any]) -> str:
        """生成HTML报告"""
        # 这里应该使用专业的HTML模板生成器
        # 目前返回简单的HTML内容

        metrics = results.get("performance_metrics", {})
        risk_analysis = results.get("risk_analysis", {})
        params = results.get("backtest_params", {})
        optimization_results = results.get("optimization_results", [])
        sensitivity_results = results.get("sensitivity_results", [])
        walk_forward_sensitivity = results.get("walk_forward_sensitivity", [])
        overfit_warnings = results.get("overfit_warnings", [])
        walk_forward_results = results.get("walk_forward_results", [])
        cost_analysis = results.get("cost_analysis", {})
        weight_records = results.get("weight_records", [])

        def render_kv_row(name, value):
            return f"<tr><td>{name}</td><td>{value}</td></tr>"

        def render_line_chart(series_dict, width=900, height=280):
            if not series_dict:
                return "<p>暂无图表数据</p>"
            all_values = []
            for values in series_dict.values():
                all_values.extend(values)
            if not all_values:
                return "<p>暂无图表数据</p>"
            min_val = min(all_values)
            max_val = max(all_values)
            if max_val == min_val:
                max_val = min_val + 1e-6
            colors = ["#2196F3", "#4CAF50", "#FF9800", "#9C27B0", "#f44336", "#00BCD4", "#795548"]
            svg_lines = []
            for i, (label, values) in enumerate(series_dict.items()):
                if not values:
                    continue
                points = []
                n = len(values)
                for idx, val in enumerate(values):
                    x = idx / max(n - 1, 1) * (width - 40) + 20
                    y = height - 20 - (val - min_val) / (max_val - min_val) * (height - 40)
                    points.append(f"{x:.2f},{y:.2f}")
                color = colors[i % len(colors)]
                points_str = " ".join(points)
                svg_lines.append(
                    f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{points_str}" />'
                )
            legend = " ".join(
                [
                    f'<span style="margin-right:12px;color:{colors[i % len(colors)]}">{label}</span>'
                    for i, label in enumerate(series_dict.keys())
                ]
            )
            return (
                f'<div class="chart-legend">{legend}</div><svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
                + "".join(svg_lines)
                + "</svg>"
            )

        def render_bar_chart(items, width=900, height=260):
            if not items:
                return "<p>暂无图表数据</p>"
            values = [v for _, v in items]
            max_val = max(values) if values else 1.0
            if max_val == 0:
                max_val = 1.0
            bar_width = (width - 40) / len(items)
            bars = []
            labels = []
            for i, (label, value) in enumerate(items):
                x = 20 + i * bar_width
                h = (value / max_val) * (height - 40)
                y = height - 20 - h
                bars.append(
                    f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width - 6:.2f}" height="{h:.2f}" fill="#4CAF50" />'
                )
                labels.append(
                    f'<text x="{x + (bar_width - 6) / 2:.2f}" y="{height - 5}" font-size="10" text-anchor="middle">{i + 1}</text>'
                )
            return (
                f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
                + "".join(bars)
                + "".join(labels)
                + "</svg>"
            )

        def render_curve_blocks(rows):
            if not rows:
                return "<p>暂无分段曲线</p>"
            blocks = []
            for row in rows:
                curve = row.get("curve", {})
                values = curve.get("values", [])
                if not values:
                    continue
                series = {"净值": values}
                chart = render_line_chart(series, width=360, height=140)
                blocks.append(
                    f"<div class='chart-card'>"
                    f"<div class='chart-title'>窗口 {row.get('window', '')}</div>{chart}</div>"
                )
            return (
                f"<div class='chart-grid'>{''.join(blocks)}</div>"
                if blocks
                else "<p>暂无分段曲线</p>"
            )

        weight_series: dict[str, list[float]] = {}
        for record in weight_records:
            weights = record.get("weights", {})
            for sym, val in weights.items():
                weight_series.setdefault(sym, []).append(float(val))

        turnover_series = []
        if len(weight_records) >= 2:
            last = weight_records[0].get("weights", {})
            for record in weight_records[1:]:
                current = record.get("weights", {})
                keys = set(last.keys()) | set(current.keys())
                diff = 0.0
                for key in keys:
                    diff += abs(float(current.get(key, 0)) - float(last.get(key, 0)))
                turnover_series.append(diff)
                last = current

        weight_stats = {}
        if weight_series:
            weight_vols = []
            for series in weight_series.values():
                if len(series) <= 1:
                    continue
                mean_val = sum(series) / len(series)
                var = sum((v - mean_val) ** 2 for v in series) / max(len(series) - 1, 1)
                weight_vols.append(var**0.5)
            weight_stats = {
                "平均权重波动": sum(weight_vols) / len(weight_vols) if weight_vols else 0,
                "最大权重波动": max(weight_vols) if weight_vols else 0,
                "平均换手率": sum(turnover_series) / len(turnover_series) if turnover_series else 0,
                "最大换手率": max(turnover_series) if turnover_series else 0,
            }

        optimization_scores = []
        for row in optimization_results:
            score = row.get("score")
            if score is None:
                score = float(row.get("sharpe_ratio", 0)) * float(row.get("total_return", 0))
            optimization_scores.append((str(row.get("params", {})), float(score)))
        optimization_scores = sorted(optimization_scores, key=lambda x: x[1], reverse=True)[:10]

        def render_weight_table(records):
            if not records:
                return "<p>暂无权重记录</p>"
            symbols = list(records[0].get("weights", {}).keys())
            header = "<tr><th>日期</th>" + "".join([f"<th>{s}</th>" for s in symbols]) + "</tr>"
            rows = []
            for record in records:
                date = record.get("date")
                date_text = date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)
                weights = record.get("weights", {})
                cells = [f"<td>{date_text}</td>"]
                for s in symbols:
                    val = weights.get(s, 0)
                    cells.append(f"<td>{val:.4%}</td>")
                rows.append("<tr>" + "".join(cells) + "</tr>")
            return f"<table>{header}{''.join(rows)}</table>"

        def render_optimization_table(rows):
            if not rows:
                return "<p>暂无参数优化结果</p>"
            header = "<tr><th>参数组合</th><th>收益</th><th>夏普</th><th>回撤</th></tr>"
            body = []
            for row in rows:
                params_str = row.get("params", {})
                body.append(
                    "<tr>"
                    f"<td>{params_str}</td>"
                    f"<td>{row.get('total_return', 0):.4%}</td>"
                    f"<td>{row.get('sharpe_ratio', 0):.3f}</td>"
                    f"<td>{row.get('max_drawdown', 0):.4%}</td>"
                    "</tr>"
                )
            return f"<table>{header}{''.join(body)}</table>"

        def render_sensitivity_table(rows):
            if not rows:
                return "<p>暂无敏感性分析</p>"
            header = "<tr><th>参数</th><th>取值</th><th>偏移</th><th>收益</th><th>夏普</th><th>回撤</th></tr>"
            body = []
            for row in rows:
                body.append(
                    "<tr>"
                    f"<td>{row.get('param', '')}</td>"
                    f"<td>{row.get('value', '')}</td>"
                    f"<td>{row.get('delta', '')}</td>"
                    f"<td>{float(row.get('total_return', 0) or 0):.4%}</td>"
                    f"<td>{float(row.get('sharpe_ratio', 0) or 0):.3f}</td>"
                    f"<td>{float(row.get('max_drawdown', 0) or 0):.4%}</td>"
                    "</tr>"
                )
            return f"<table>{header}{''.join(body)}</table>"

        def render_overfit_warnings(items):
            if not items:
                return "<p>无过拟合警戒提示</p>"
            rows = "".join([f"<li>{item}</li>" for item in items])
            return f"<ul>{rows}</ul>"

        def render_wf_sensitivity_table(rows):
            if not rows:
                return "<p>暂无Walk-Forward敏感性</p>"
            header = "<tr><th>窗口</th><th>参数</th><th>取值</th><th>偏移</th><th>收益</th><th>夏普</th><th>回撤</th></tr>"
            body = []
            for row in rows:
                body.append(
                    "<tr>"
                    f"<td>{row.get('window', '')}</td>"
                    f"<td>{row.get('param', '')}</td>"
                    f"<td>{row.get('value', '')}</td>"
                    f"<td>{row.get('delta', '')}</td>"
                    f"<td>{float(row.get('total_return', 0) or 0):.4%}</td>"
                    f"<td>{float(row.get('sharpe_ratio', 0) or 0):.3f}</td>"
                    f"<td>{float(row.get('max_drawdown', 0) or 0):.4%}</td>"
                    "</tr>"
                )
            return f"<table>{header}{''.join(body)}</table>"

        def render_walk_forward_table(rows):
            if not rows:
                return "<p>暂无Walk-Forward结果</p>"
            header = "<tr><th>窗口</th><th>训练期</th><th>测试期</th><th>收益</th><th>夏普</th><th>回撤</th></tr>"
            body = []
            for row in rows:
                body.append(
                    "<tr>"
                    f"<td>{row.get('window', '')}</td>"
                    f"<td>{row.get('train', '')}</td>"
                    f"<td>{row.get('test', '')}</td>"
                    f"<td>{row.get('return', 0):.4%}</td>"
                    f"<td>{row.get('sharpe_ratio', 0):.3f}</td>"
                    f"<td>{row.get('max_drawdown', 0):.4%}</td>"
                    "</tr>"
                )
            return f"<table>{header}{''.join(body)}</table>"

        html_template = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>回测报告 - {params.get("stock_code", "N/A")}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ text-align: center; margin-bottom: 30px; }}
        .section {{ margin-bottom: 30px; }}
        .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }}
        .metric-card {{ border: 1px solid #ddd; padding: 15px; border-radius: 5px; text-align: center; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #2196F3; }}
        .metric-label {{ color: #666; margin-top: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .subtle {{ color: #666; font-size: 12px; }}
        .chart-grid {{ display: flex; flex-wrap: wrap; gap: 12px; }}
        .chart-card {{ border: 1px solid #e0e0e0; padding: 8px; border-radius: 6px; background: #fff; }}
        .chart-title {{ font-weight: bold; margin-bottom: 4px; }}
        .chart-legend span {{ font-size: 12px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 回测报告</h1>
        <p>股票代码: {params.get("stock_code", "N/A")} |
           回测期间: {params.get("start_date", "N/A")} ~ {params.get("end_date", "N/A")}</p>
        <p>生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    </div>

    <div class="section">
        <h2>📈 关键指标</h2>
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-value">{metrics.get("total_return", 0):.4%}</div>
                <div class="metric-label">总收益率</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{metrics.get("sharpe_ratio", 0):.2f}</div>
                <div class="metric-label">夏普比率</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{metrics.get("max_drawdown", 0):.4%}</div>
                <div class="metric-label">最大回撤</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{metrics.get("win_rate", 0):.2%}</div>
                <div class="metric-label">胜率</div>
            </div>
        </div>
    </div>

    <div class="section">
        <h2>📊 详细指标</h2>
        <table>
            <tr><th>指标名称</th><th>数值</th></tr>
            <tr><td>总收益率</td><td>{metrics.get("total_return", 0):.4%}</td></tr>
            <tr><td>年化收益率</td><td>{metrics.get("annualized_return", 0):.4%}</td></tr>
            <tr><td>年化波动率</td><td>{metrics.get("volatility", 0):.4%}</td></tr>
            <tr><td>夏普比率</td><td>{metrics.get("sharpe_ratio", 0):.3f}</td></tr>
            <tr><td>最大回撤</td><td>{metrics.get("max_drawdown", 0):.4%}</td></tr>
            <tr><td>胜率</td><td>{metrics.get("win_rate", 0):.2%}</td></tr>
            <tr><td>盈利因子</td><td>{metrics.get("profit_factor", 0):.2f}</td></tr>
            {render_kv_row("95% VaR", f"{risk_analysis.get('var_95', 0):.4%}")}
            {render_kv_row("95% CVaR", f"{risk_analysis.get('cvar_95', 0):.4%}")}
        </table>
    </div>

    <div class="section">
        <h2>⚙️ 回测参数</h2>
        <table>
            <tr><th>参数名称</th><th>参数值</th></tr>
            <tr><td>股票代码</td><td>{params.get("stock_code", "N/A")}</td></tr>
            <tr><td>回测期间</td><td>{params.get("start_date", "N/A")} ~ {params.get("end_date", "N/A")}</td></tr>
            <tr><td>K线周期</td><td>{params.get("period", "1d")}</td></tr>
            <tr><td>初始资金</td><td>{params.get("initial_cash", 0):,.0f} 元</td></tr>
            <tr><td>手续费率</td><td>{params.get("commission", 0):.4f}</td></tr>
            <tr><td>短期均线</td><td>{params.get("short_period", "N/A")} 日</td></tr>
            <tr><td>长期均线</td><td>{params.get("long_period", "N/A")} 日</td></tr>
            <tr><td>RSI周期</td><td>{params.get("rsi_period", "N/A")} 日</td></tr>
            {render_kv_row("仓位控制", params.get("position_sizing", "N/A"))}
            {render_kv_row("单票上限", f"{params.get('max_position_pct', 'N/A')}%")}
            {render_kv_row("再平衡频率", params.get("rebalance_freq", "N/A"))}
        </table>
    </div>

    <div class="section">
        <h2>💹 成本与归因</h2>
        <table>
            {render_kv_row("估算成交次数", cost_analysis.get("trade_count", 0))}
            {render_kv_row("估算成交额", f"{cost_analysis.get('turnover', 0):,.2f}")}
            {render_kv_row("手续费率", f"{cost_analysis.get('commission_rate', 0):.4%}")}
            {render_kv_row("滑点(bps)", cost_analysis.get("slippage_bps", 0))}
            {render_kv_row("估算成本", f"{cost_analysis.get('estimated_cost', 0):,.2f}")}
        </table>
        <p class="subtle">成本估算依据再平衡换手与滑点假设计算</p>
    </div>

    <div class="section">
        <h2>🧾 再平衡权重表</h2>
        {render_weight_table(weight_records)}
        <h3>权重稳定性统计</h3>
        <table>
            {render_kv_row("平均权重波动", f"{weight_stats.get('平均权重波动', 0):.4f}")}
            {render_kv_row("最大权重波动", f"{weight_stats.get('最大权重波动', 0):.4f}")}
            {render_kv_row("平均换手率", f"{weight_stats.get('平均换手率', 0):.4f}")}
            {render_kv_row("最大换手率", f"{weight_stats.get('最大换手率', 0):.4f}")}
        </table>
        <h3>权重时序图</h3>
        {render_line_chart(weight_series, 900, 260)}
        <h3>换手率时序图</h3>
        {render_line_chart({"换手率": turnover_series}, 900, 220)}
    </div>

    <div class="section">
        <h2>🧠 参数优化结果</h2>
        {render_optimization_table(optimization_results)}
        <h4>参数敏感性分析</h4>
        {render_sensitivity_table(sensitivity_results)}
        <h4>Walk-Forward 多阶段敏感性</h4>
        {render_wf_sensitivity_table(walk_forward_sensitivity)}
        <h4>过拟合警戒</h4>
        {render_overfit_warnings(overfit_warnings)}
        <h3>优化排名图</h3>
        {render_bar_chart(optimization_scores, 900, 260)}
    </div>

    <div class="section">
        <h2>🧪 Walk-Forward 分段结果</h2>
        {render_walk_forward_table(walk_forward_results)}
        <h3>分段净值曲线</h3>
        {render_curve_blocks(walk_forward_results)}
    </div>

    <div class="section">
        <h2>📝 免责声明</h2>
        <p>本报告仅供参考，不构成投资建议。历史业绩不代表未来表现，投资有风险，入市需谨慎。</p>
    </div>
</body>
</html>
        """

        return html_template

    def update_connection_status(self):
        """更新连接状态显示"""
        try:
            if self.data_manager is None:
                self.data_source_label.setText("⏳ 数据源初始化中...")
                self.data_source_label.setStyleSheet("color: #666666; font-weight: bold;")
                return
            status = self.data_manager.get_connection_status()
            active_source = status.get("active_source", "none")

            # 根据活跃数据源设置显示
            if active_source == "qmt":
                self.data_source_label.setText("✅ QMT已连接 (真实数据)")
                self.data_source_label.setStyleSheet("color: green; font-weight: bold;")
            elif active_source == "duckdb":
                self.data_source_label.setText("✅ DuckDB数据库 (真实数据)")
                self.data_source_label.setStyleSheet("color: green; font-weight: bold;")
            elif active_source == "local":
                self.data_source_label.setText("✅ 本地缓存 (真实数据)")
                self.data_source_label.setStyleSheet("color: green; font-weight: bold;")
            elif active_source == "qstock":
                self.data_source_label.setText("✅ QStock已连接 (真实数据)")
                self.data_source_label.setStyleSheet("color: green; font-weight: bold;")
            elif active_source == "akshare":
                self.data_source_label.setText("✅ AKShare已连接 (真实数据)")
                self.data_source_label.setStyleSheet("color: green; font-weight: bold;")
            elif active_source == "none":
                self.data_source_label.setText("❌ 无真实数据源")
                self.data_source_label.setStyleSheet("color: #d32f2f; font-weight: bold;")
            else:
                # 未知数据源
                self.data_source_label.setText(f"❓ 数据源: {active_source}")
                self.data_source_label.setStyleSheet("color: gray; font-weight: bold;")

            # 显示详细状态信息
            source_status = status.get("source_status", {})
            status_details = []
            for source_name, source_info in source_status.items():
                if source_info["available"]:
                    if source_info["connected"]:
                        status_details.append(f"{source_name.upper()}:✅")
                    else:
                        status_details.append(f"{source_name.upper()}:⚠️")
                else:
                    status_details.append(f"{source_name.upper()}:❌")

            tooltip_text = (
                "数据源状态:\
"
                + "\
".join(status_details)
            )
            self.data_source_label.setToolTip(tooltip_text)

        except Exception as e:
            self.data_source_label.setText("❓ 状态检测失败")
            self.data_source_label.setStyleSheet("color: gray; font-weight: bold;")
            print(f"连接状态检测失败: {e}")

    def update_backtest_engine_status(self):
        """更新回测引擎状态显示（Backtrader/模拟模式+原因）。"""
        if not hasattr(self, "backtest_engine_label"):
            return
        status: dict[str, Any] = {}
        try:
            if callable(BacktraderImportStatusGetter):
                status = BacktraderImportStatusGetter() or {}
            else:
                status = {
                    "available": AdvancedBacktestEngine is not None,
                    "mode": "backtrader" if AdvancedBacktestEngine is not None else "mock",
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
                "hint": "读取回测引擎状态失败",
            }

        self._backtest_engine_status = status
        self._log_backtest_engine_status()
        try:
            signal_bus.emit(
                Events.BACKTEST_ENGINE_STATUS_UPDATED,
                status=status,
                source="backtest_widget",
            )
        except Exception:
            pass
        ui = format_engine_status_ui(status, label_prefix="引擎")
        self.backtest_engine_label.setText(ui.get("text", "引擎: 状态未知 ❓"))
        self.backtest_engine_label.setStyleSheet(
            f"color: {ui.get('color', '#666666')}; font-weight: bold;"
        )
        self.backtest_engine_label.setToolTip(ui.get("tooltip", ""))

    def _log_backtest_engine_status(self):
        status = getattr(self, "_backtest_engine_status", None)
        line = format_engine_status_log(status, prefix="BACKTEST_ENGINE")
        if line == self._last_backtest_engine_log:
            return
        self._last_backtest_engine_log = line
        print(line)

    def on_backtest_engine_label_clicked(self, event):
        status = getattr(self, "_backtest_engine_status", None)
        if not isinstance(status, dict):
            self.update_backtest_engine_status()
            status = getattr(self, "_backtest_engine_status", {})
        detail = build_engine_status_detail(status)
        QMessageBox.information(self, "回测引擎状态详情", detail)

    def _init_data_manager_async(self):
        if self.data_manager is not None:
            return
        if self._data_manager_thread and self._data_manager_thread.isRunning():
            return
        self._data_manager_thread = DataManagerInitThread()
        self._data_manager_thread.setParent(self)
        self._data_manager_thread.ready.connect(self._on_data_manager_ready)
        self._data_manager_thread.error_occurred.connect(lambda msg: None)
        self._data_manager_thread.start()

    def _on_data_manager_ready(self, manager):
        self.data_manager = manager
        self.update_connection_status()
        self._refresh_source_status_async()
        if self._pending_preview_params:
            params = self._pending_preview_params
            self._pending_preview_params = None
            self._load_data_preview_with_manager(params)

    def _refresh_source_status_async(self):
        if self.data_manager is None:
            return
        if self._source_refresh_thread and self._source_refresh_thread.isRunning():
            return
        self._source_refresh_thread = DataSourceRefreshThread(self.data_manager)
        self._source_refresh_thread.setParent(self)
        self._source_refresh_thread.finished_refresh.connect(self.update_connection_status)
        self._source_refresh_thread.start()

    def _init_strategy_scan_async(self):
        if self._strategy_scan_thread and self._strategy_scan_thread.isRunning():
            return
        self._strategy_scan_thread = StrategyScanThread()
        self._strategy_scan_thread.setParent(self)
        self._strategy_scan_thread.ready.connect(self._on_strategy_scan_ready)
        self._strategy_scan_thread.error_occurred.connect(lambda msg: None)
        self._strategy_scan_thread.start()

    def _on_strategy_scan_ready(self, records: list):
        self.strategy_scan_records = records or []
        if hasattr(self, "strategy_catalog_table"):
            self.refresh_strategy_catalog_table()

    def refresh_strategy_catalog_table_async(self):
        self._init_strategy_scan_async()

    def load_data_preview(self):
        if not self.validate_data_parameters():
            return
        params = self.get_backtest_parameters()
        if not self.data_manager:
            self._pending_preview_params = params
            self._init_data_manager_async()
            self.status_label.setText("⏳ 数据源初始化中...")
            if hasattr(self, "data_status_label"):
                self.data_status_label.setText("数据: 初始化中...")
            return
        self._load_data_preview_with_manager(params)

    def _load_data_preview_with_manager(self, params: dict):
        if self.data_manager is None:
            return
        dm = cast(Any, self.data_manager)
        self.status_label.setText("📥 正在加载数据...")
        self.progress_bar.setValue(0)
        if hasattr(self, "data_status_label"):
            self.data_status_label.setText("数据: 加载中...")
        self.update_data_load_summary({})
        if hasattr(self, "data_load_log"):
            self.data_load_log.setPlainText("")
        symbols = [params["stock_code"]]
        if params.get("multi_asset_enabled"):
            symbols = params.get("multi_asset_list") or symbols
        if hasattr(self, "data_load_table"):
            table_rows = [
                [symbol, "等待", "-", "-", "-", params.get("period", "1d")]
                for symbol in symbols
            ]
            self._render_table_rows(self.data_load_table, table_rows)
        self.append_load_log(f"加载开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        items = []
        total_rows = 0
        sources = set()
        for idx, symbol in enumerate(symbols):
            dm.last_source = None
            if hasattr(self, "data_load_table"):
                self.data_load_table.setItem(idx, 1, QTableWidgetItem("加载中"))
            self.append_load_log(f"[{idx + 1}/{len(symbols)}] 拉取 {symbol}")
            data_raw = dm.get_stock_data(
                stock_code=symbol,
                start_date=params["start_date"],
                end_date=params["end_date"],
                period=params.get("period", "1d"),
                adjust=params.get("adjust", "none"),
            )
            data = (
                data_raw
                if isinstance(data_raw, pd.DataFrame)
                else pd.DataFrame(data_raw)
            )
            data = cast(pd.DataFrame, data)
            info = dm.last_data_info or {}
            source = info.get("source") or dm.last_source or "unknown"
            row_count = len(data)
            raw_rows = info.get("raw_rows", row_count)
            clean_rows = info.get("clean_rows", row_count)
            removed_rows = info.get("removed_rows", max(raw_rows - clean_rows, 0))
            adjust = info.get("adjust", params.get("adjust", "none"))
            period = info.get("period", params.get("period", "1d"))
            start_str = "-"
            end_str = "-"
            if row_count > 0:
                if isinstance(data.index, pd.DatetimeIndex):
                    idx_series = data.index
                elif "date" in data.columns:
                    idx_series = pd.to_datetime(data["date"], errors="coerce")
                else:
                    idx_series = None
                if idx_series is not None and len(idx_series) > 0:
                    start_ts = pd.to_datetime(idx_series.min(), errors="coerce")
                    end_ts = pd.to_datetime(idx_series.max(), errors="coerce")
                    fmt = "%Y-%m-%d" if period == "1d" else "%Y-%m-%d %H:%M"
                    if pd.notna(start_ts):
                        start_str = start_ts.strftime(fmt)
                    if pd.notna(end_ts):
                        end_str = end_ts.strftime(fmt)
            quality = dm.validate_data_quality(data)
            missing_total = int(sum(quality.get("missing_values", {}).values())) if quality else 0
            issues = quality.get("issues", []) if quality else []
            if hasattr(self, "data_load_table"):
                self.data_load_table.setItem(
                    idx, 1, QTableWidgetItem("完成" if row_count > 0 else "空数据")
                )
                self.data_load_table.setItem(idx, 2, QTableWidgetItem(str(row_count)))
                self.data_load_table.setItem(idx, 3, QTableWidgetItem(f"{start_str} ~ {end_str}"))
                self.data_load_table.setItem(idx, 4, QTableWidgetItem(source))
                self.data_load_table.setItem(idx, 5, QTableWidgetItem(period))
            self.append_load_log(
                f"{symbol} | {row_count} 条 | {start_str} ~ {end_str} | {source} | {period} | 复权:{adjust} | 清洗:{raw_rows}->{clean_rows}(-{removed_rows})"
            )
            items.append(
                {
                    "symbol": symbol,
                    "rows": row_count,
                    "raw_rows": raw_rows,
                    "clean_rows": clean_rows,
                    "removed_rows": removed_rows,
                    "start_date": start_str,
                    "end_date": end_str,
                    "source": source,
                    "period": period,
                    "adjust": adjust,
                    "missing_total": missing_total,
                    "quality_issues": issues,
                }
            )
            total_rows += row_count
            sources.add(source)
            self.progress_bar.setValue(int((idx + 1) / max(len(symbols), 1) * 100))
        summary = {
            "items": items,
            "total_rows": total_rows,
            "sources": sorted(sources),
            "loaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.update_data_load_summary(summary)
        self.update_data_summary(summary)
        self.append_load_log(f"加载完成: {summary.get('loaded_at')}")
        self.status_label.setText("✅ 数据加载完成")

    def refresh_connection_status(self):
        """刷新连接状态"""
        self.data_source_label.setText("🔄 检测中...")
        self.data_source_label.setStyleSheet("color: blue; font-weight: bold;")
        self.update_backtest_engine_status()

        # 刷新数据管理器状态
        if self.data_manager:
            self.data_manager.refresh_source_status()
        else:
            self._init_data_manager_async()
            return

        # 更新状态显示
        QTimer.singleShot(1000, self.update_connection_status)  # 延迟1秒更新

    def closeEvent(self, event):
        try:
            if self.backtest_worker and self.backtest_worker.isRunning():
                self.backtest_worker.stop()
                self.backtest_worker.wait(1000)
            if self._data_manager_thread and self._data_manager_thread.isRunning():
                self._data_manager_thread.requestInterruption()
                self._data_manager_thread.quit()
                self._data_manager_thread.wait(1000)
            if self._source_refresh_thread and self._source_refresh_thread.isRunning():
                self._source_refresh_thread.requestInterruption()
                self._source_refresh_thread.quit()
                self._source_refresh_thread.wait(1000)
            if self._strategy_scan_thread and self._strategy_scan_thread.isRunning():
                self._strategy_scan_thread.requestInterruption()
                self._strategy_scan_thread.quit()
                self._strategy_scan_thread.wait(1000)
        finally:
            super().closeEvent(event)


if __name__ == "__main__":
    import sys

    from PyQt5.QtWidgets import QApplication

    app = QApplication(sys.argv)

    # 创建回测窗口
    backtest_widget = BacktestWidget()
    backtest_widget.show()

    sys.exit(app.exec_())
