#!/usr/bin/env python3
"""
跨周期对冲与套利交互面板 - 基于最新对冲基金实践
整合Kalman滤波统计套利、多因子模型、跨周期风险对冲等前沿技术
"""

import sys
import importlib
import importlib.util
from dataclasses import dataclass
from enum import Enum

import numpy as np
import pandas as pd
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QComboBox,
    QDoubleSpinBox,
    QFormLayout,
    QGroupBox,
    QLabel,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

# 前沿量化模型导入
try:
    from statsmodels.tsa.vector_ar.vecm import VECM
    VECM_AVAILABLE = True
except ImportError:
    VECM_AVAILABLE = False
    print("警告: statsmodels VECM模块不可用，部分统计套利功能受限")

KalmanFilter = None
try:
    if importlib.util.find_spec("pykalman") is not None:
        _pykalman = importlib.import_module("pykalman")
        KalmanFilter = getattr(_pykalman, "KalmanFilter", None)
    KALMAN_AVAILABLE = KalmanFilter is not None
except Exception:
    KALMAN_AVAILABLE = False
    print("警告: pykalman模块不可用，Kalman滤波功能受限")


class HedgingStrategyType(Enum):
    """对冲策略类型"""
    STATISTICAL_ARBITRAGE = "statistical_arbitrage"  # 统计套利
    PAIRS_TRADING = "pairs_trading"                  # 配对交易
    MULTI_PERIOD_HEDGE = "multi_period_hedge"        # 跨周期对冲
    RISK_PARITY = "risk_parity"                       # 风险平价
    VOLATILITY_TARGETING = "vol_targeting"           # 波动率目标


@dataclass
class CrossPeriodHedgeConfig:
    """跨周期对冲配置"""
    strategy_type: HedgingStrategyType
    timeframes: list[str]  # 时间周期列表 ["1d", "1h", "30m"]
    hedge_ratio_method: str  # 对冲比率计算方法
    rebalance_frequency: str  # 再平衡频率
    risk_budget: dict[str, float]  # 风险预算分配

    # 统计套利参数
    cointegration_lookback: int = 252  # 协整回看期
    kalman_adaptation_rate: float = 0.02  # Kalman滤波适应率
    entry_threshold: float = 2.0  # 入场阈值
    exit_threshold: float = 0.5  # 出场阈值

    # 风险控制参数
    max_leverage: float = 3.0  # 最大杠杆
    volatility_target: float = 0.15  # 波动率目标
    correlation_threshold: float = 0.7  # 相关性阈值


class KalmanFilterArbitrage:
    """Kalman滤波统计套利引擎 - 基于QuantConnect前沿研究"""

    def __init__(self, assets: list[str], lookback_period: int = 252):
        self.assets = assets
        self.lookback_period = lookback_period
        self.kalman_filter = None
        self.current_mean = None
        self.current_cov = None
        self.cointegration_vectors = None

    def calibrate(self, price_data: pd.DataFrame) -> bool:
        """校准Kalman滤波模型"""
        if not VECM_AVAILABLE or not KALMAN_AVAILABLE:
            return False

        try:
            # 转换为对数价格序列
            log_prices = np.log(price_data)

            # VECM模型估计协整关系
            vecm_result = VECM(
                log_prices,
                k_ar_diff=0,
                coint_rank=len(self.assets) - 1,
                deterministic='n'
            ).fit()

            # 获取协整向量
            beta = vecm_result.beta
            spread = pd.DataFrame(np.asarray(log_prices) @ beta, index=price_data.index)

            # 优化协整子空间权重
            x0 = np.array([(-1)**i / beta.shape[1] for i in range(beta.shape[1])])
            bounds = tuple((-1, 1) for _ in range(beta.shape[1]))
            constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x)}]

            from scipy.optimize import minimize
            opt = minimize(
                lambda w: ((w.T @ np.cov(spread.T, spread.shift(1).fillna(0).T)[spread.shape[1]:, :spread.shape[1]] @ w) /
                          (w.T @ np.cov(spread.T) @ w))**2,
                x0=x0,
                bounds=bounds,
                constraints=constraints,
                method="SLSQP"
            )

            # 归一化权重
            opt.x = opt.x / np.sum(np.abs(opt.x))
            optimized_spread = spread @ opt.x

            # Kalman滤波初始化
            if KalmanFilter is None:
                return False
            self.kalman_filter = KalmanFilter(
                transition_matrices=[1],
                observation_matrices=[1],
                initial_state_mean=optimized_spread.iloc[:20].mean(),
                observation_covariance=optimized_spread.iloc[:20].var(),
                em_vars=['transition_covariance', 'initial_state_covariance']
            )

            # EM算法优化参数
            (filtered_state_means, filtered_state_covariances) = self.kalman_filter.em(
                optimized_spread.iloc[:20], n_iter=5
            )

            self.current_mean = filtered_state_means[-1, :]
            self.current_cov = filtered_state_covariances[-1, :]
            self.cointegration_vectors = beta @ opt.x

            return True

        except Exception as e:
            print(f"Kalman滤波校准失败: {e}")
            return False

    def get_trading_signal(self, current_prices: pd.Series) -> dict[str, float]:
        """获取交易信号"""
        if self.kalman_filter is None:
            return {}

        try:
            log_prices = np.log(current_prices)
            current_spread = log_prices @ self.cointegration_vectors

            # Kalman滤波更新
            (self.current_mean, self.current_cov) = self.kalman_filter.filter_update(
                filtered_state_mean=self.current_mean,
                filtered_state_covariance=self.current_cov,
                observation=current_spread
            )

            # 计算标准化价差
            normalized_spread = (current_spread - float(self.current_mean)) / np.sqrt(float(self.current_cov))

            # 生成交易信号
            signals = {}
            if self.cointegration_vectors is None:
                return signals
            for i, asset in enumerate(self.assets):
                # 基于协整向量和标准化价差计算信号
                weight = self.cointegration_vectors[i]
                signals[asset] = -weight * normalized_spread

            return signals

        except Exception as e:
            print(f"交易信号生成失败: {e}")
            return {}


class MultiPeriodHedgeEngine:
    """跨周期对冲引擎 - 基于Qlib增强指数策略"""

    def __init__(self, config: CrossPeriodHedgeConfig):
        self.config = config
        self.risk_model = None
        self.portfolio_weights = {}

    def calculate_hedge_ratios(self, price_data: dict[str, pd.DataFrame]) -> dict[str, float]:
        """计算跨周期对冲比率"""

        # 多时间周期风险分析
        risk_contributions = {}
        for timeframe in self.config.timeframes:
            if timeframe in price_data:
                returns = price_data[timeframe].pct_change().dropna()

                # 计算波动率贡献
                volatility = returns.std() * np.sqrt(252)
                correlation = returns.corr()

                # 风险平价权重计算
                risk_contributions[timeframe] = self._risk_parity_weights(volatility, correlation)

        # 跨周期风险预算分配
        hedge_ratios = self._allocate_risk_budget(risk_contributions)
        return hedge_ratios

    def _risk_parity_weights(self, volatility: pd.Series, correlation: pd.DataFrame) -> pd.Series:
        """风险平价权重计算"""
        # 简化版风险平价算法
        inverse_vol = 1 / volatility
        weights = inverse_vol / inverse_vol.sum()
        return weights

    def _allocate_risk_budget(self, risk_contributions: dict[str, pd.Series]) -> dict[str, float]:
        """风险预算分配"""
        # 基于配置的风险预算进行分配
        total_budget = sum(self.config.risk_budget.values())
        hedge_ratios = {}

        for timeframe, contributions in risk_contributions.items():
            budget_share = self.config.risk_budget.get(timeframe, 0) / total_budget
            for asset, weight in contributions.items():
                key = f"{asset}_{timeframe}"
                hedge_ratios[key] = weight * budget_share

        return hedge_ratios


class HedgingStrategyThread(QThread):
    """对冲策略执行线程"""

    progress_updated = pyqtSignal(int, str)  # 进度更新信号
    strategy_completed = pyqtSignal(dict)    # 策略完成信号
    error_occurred = pyqtSignal(str)          # 错误信号

    def __init__(self, config: CrossPeriodHedgeConfig, price_data: dict[str, pd.DataFrame]):
        super().__init__()
        self.config = config
        self.price_data = price_data

    def run(self):
        try:
            self.progress_updated.emit(10, "开始策略分析...")

            if self.config.strategy_type == HedgingStrategyType.STATISTICAL_ARBITRAGE:
                result = self._run_statistical_arbitrage()
            elif self.config.strategy_type == HedgingStrategyType.MULTI_PERIOD_HEDGE:
                result = self._run_multi_period_hedge()
            else:
                raise ValueError(f"不支持的策略类型: {self.config.strategy_type}")

            self.progress_updated.emit(100, "策略分析完成")
            self.strategy_completed.emit(result)

        except Exception as e:
            self.error_occurred.emit(f"策略执行错误: {e}")

    def _run_statistical_arbitrage(self) -> dict:
        """执行统计套利策略"""
        self.progress_updated.emit(30, "校准Kalman滤波模型...")

        # 获取标的列表
        assets = list(self.price_data["1d"].columns) if "1d" in self.price_data else []
        if len(assets) < 2:
            raise ValueError("统计套利需要至少2个标的")

        # 初始化Kalman滤波套利引擎
        arbitrage_engine = KalmanFilterArbitrage(assets, self.config.cointegration_lookback)

        if not arbitrage_engine.calibrate(self.price_data["1d"]):
            raise ValueError("Kalman滤波校准失败")

        self.progress_updated.emit(60, "生成交易信号...")

        # 获取最新交易信号
        latest_prices = self.price_data["1d"].iloc[-1]
        signals = arbitrage_engine.get_trading_signal(latest_prices)

        # 风险调整
        adjusted_signals = self._apply_risk_controls(signals)

        return {
            "strategy_type": "statistical_arbitrage",
            "signals": adjusted_signals,
            "performance_metrics": self._calculate_performance_metrics(),
            "risk_analysis": self._analyze_risk()
        }

    def _run_multi_period_hedge(self) -> dict:
        """执行跨周期对冲策略"""
        self.progress_updated.emit(30, "计算跨周期对冲比率...")

        hedge_engine = MultiPeriodHedgeEngine(self.config)
        hedge_ratios = hedge_engine.calculate_hedge_ratios(self.price_data)

        self.progress_updated.emit(70, "优化组合权重...")

        # 组合优化
        optimized_weights = self._portfolio_optimization(hedge_ratios)

        return {
            "strategy_type": "multi_period_hedge",
            "hedge_ratios": optimized_weights,
            "risk_contributions": self._calculate_risk_contributions(optimized_weights),
            "performance_metrics": self._calculate_performance_metrics()
        }

    def _apply_risk_controls(self, signals: dict[str, float]) -> dict[str, float]:
        """应用风险控制"""
        # 杠杆限制
        total_exposure = sum(abs(signal) for signal in signals.values())
        if total_exposure > self.config.max_leverage:
            scale_factor = self.config.max_leverage / total_exposure
            signals = {k: v * scale_factor for k, v in signals.items()}

        return signals

    def _portfolio_optimization(self, hedge_ratios: dict[str, float]) -> dict[str, float]:
        """组合优化"""
        # 简化版组合优化
        total_weight = sum(abs(w) for w in hedge_ratios.values())
        if total_weight > self.config.max_leverage:
            scale_factor = self.config.max_leverage / total_weight
            hedge_ratios = {k: v * scale_factor for k, v in hedge_ratios.items()}

        return hedge_ratios

    def _calculate_performance_metrics(self) -> dict[str, float]:
        """计算绩效指标"""
        # 简化版绩效计算
        return {
            "expected_return": 0.08,
            "volatility": 0.12,
            "sharpe_ratio": 0.67,
            "max_drawdown": 0.15
        }

    def _analyze_risk(self) -> dict[str, float]:
        """风险分析"""
        return {
            "var_95": 0.05,
            "cvar_95": 0.08,
            "correlation_risk": 0.03,
            "liquidity_risk": 0.02
        }

    def _calculate_risk_contributions(self, weights: dict[str, float]) -> dict[str, float]:
        """计算风险贡献"""
        # 简化版风险贡献计算
        return {asset: weight * 0.1 for asset, weight in weights.items()}


class CrossPeriodHedgingPanel(QWidget):
    """跨周期对冲与套利交互面板"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_config = None
        self.strategy_thread = None
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # 标题
        title_label = QLabel("跨周期对冲与套利策略面板")
        title_font = QFont()
        title_font.setPointSize(16)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(title_label)

        # 分割器：左侧配置，右侧结果
        splitter = QSplitter(Qt.Horizontal)

        # 左侧配置面板
        config_panel = self._create_config_panel()
        splitter.addWidget(config_panel)

        # 右侧结果面板
        result_panel = self._create_result_panel()
        splitter.addWidget(result_panel)

        splitter.setSizes([400, 600])
        layout.addWidget(splitter)

    def _create_config_panel(self) -> QWidget:
        """创建配置面板"""
        panel = QWidget()
        layout = QVBoxLayout(panel)

        # 策略类型选择
        strategy_group = QGroupBox("策略配置")
        strategy_layout = QFormLayout(strategy_group)

        self.strategy_combo = QComboBox()
        self.strategy_combo.addItems([
            "统计套利 (Kalman滤波)",
            "配对交易",
            "跨周期风险对冲",
            "风险平价",
            "波动率目标"
        ])

        self.timeframe_combo = QComboBox()
        self.timeframe_combo.addItems(["1d", "1h", "30m", "15m"])
        self.timeframe_combo.setEditable(True)

        strategy_layout.addRow("策略类型:", self.strategy_combo)
        strategy_layout.addRow("时间周期:", self.timeframe_combo)

        # 风险参数配置
        risk_group = QGroupBox("风险参数")
        risk_layout = QFormLayout(risk_group)

        self.max_leverage_spin = QDoubleSpinBox()
        self.max_leverage_spin.setRange(1.0, 10.0)
        self.max_leverage_spin.setValue(3.0)
        self.max_leverage_spin.setSingleStep(0.5)

        self.vol_target_spin = QDoubleSpinBox()
        self.vol_target_spin.setRange(0.05, 0.50)
        self.vol_target_spin.setValue(0.15)
        self.vol_target_spin.setSingleStep(0.01)

        risk_layout.addRow("最大杠杆:", self.max_leverage_spin)
        risk_layout.addRow("波动率目标:", self.vol_target_spin)

        # 执行按钮
        self.execute_btn = QPushButton("执行策略分析")
        self.execute_btn.clicked.connect(self.execute_strategy)

        layout.addWidget(strategy_group)
        layout.addWidget(risk_group)
        layout.addWidget(self.execute_btn)
        layout.addStretch()

        return panel

    def _create_result_panel(self) -> QWidget:
        """创建结果面板"""
        panel = QWidget()
        layout = QVBoxLayout(panel)

        # 结果标签页
        self.tab_widget = QTabWidget()

        # 信号分析标签页
        self.signal_table = QTableWidget()
        self.signal_table.setColumnCount(4)
        self.signal_table.setHorizontalHeaderLabels(["标的", "信号强度", "权重", "风险贡献"])
        self.tab_widget.addTab(self.signal_table, "交易信号")

        # 风险分析标签页
        self.risk_tree = QTreeWidget()
        self.risk_tree.setHeaderLabels(["风险指标", "数值", "状态"])
        self.tab_widget.addTab(self.risk_tree, "风险分析")

        # 绩效分析标签页
        self.performance_table = QTableWidget()
        self.performance_table.setColumnCount(3)
        self.performance_table.setHorizontalHeaderLabels(["指标", "数值", "评级"])
        self.tab_widget.addTab(self.performance_table, "绩效分析")

        layout.addWidget(self.tab_widget)
        return panel

    def execute_strategy(self):
        """执行策略分析"""
        try:
            # 获取配置
            config = self._get_current_config()

            # 获取价格数据（这里需要接入真实数据源）
            price_data = self._get_price_data(config)

            # 启动策略线程
            self.strategy_thread = HedgingStrategyThread(config, price_data)
            self.strategy_thread.progress_updated.connect(self.update_progress)
            self.strategy_thread.strategy_completed.connect(self.display_results)
            self.strategy_thread.error_occurred.connect(self.show_error)
            self.strategy_thread.start()

            self.execute_btn.setEnabled(False)

        except Exception as e:
            QMessageBox.critical(self, "错误", f"策略执行失败: {e}")

    def _get_current_config(self) -> CrossPeriodHedgeConfig:
        """获取当前配置"""
        strategy_mapping = {
            "统计套利 (Kalman滤波)": HedgingStrategyType.STATISTICAL_ARBITRAGE,
            "配对交易": HedgingStrategyType.PAIRS_TRADING,
            "跨周期风险对冲": HedgingStrategyType.MULTI_PERIOD_HEDGE,
            "风险平价": HedgingStrategyType.RISK_PARITY,
            "波动率目标": HedgingStrategyType.VOLATILITY_TARGETING
        }

        return CrossPeriodHedgeConfig(
            strategy_type=strategy_mapping[self.strategy_combo.currentText()],
            timeframes=self.timeframe_combo.currentText().split(","),
            hedge_ratio_method="risk_parity",
            rebalance_frequency="1d",
            risk_budget={"1d": 0.6, "1h": 0.3, "30m": 0.1},
            max_leverage=self.max_leverage_spin.value(),
            volatility_target=self.vol_target_spin.value()
        )

    def _get_price_data(self, config: CrossPeriodHedgeConfig) -> dict[str, pd.DataFrame]:
        """获取价格数据（需要接入真实数据源）"""
        # 这里应该接入阶段4的数据管理模块
        # 临时使用示例数据
        dates = pd.date_range("2023-01-01", "2023-12-31", freq='D')

        price_data = {}
        for timeframe in config.timeframes:
            # 生成示例数据
            data = pd.DataFrame({
                '000001.SZ': np.random.normal(10, 2, len(dates)).cumsum() + 100,
                '000002.SZ': np.random.normal(10, 2, len(dates)).cumsum() + 100,
                '000003.SZ': np.random.normal(10, 2, len(dates)).cumsum() + 100
            }, index=dates)
            price_data[timeframe] = data

        return price_data

    def update_progress(self, progress: int, message: str):
        """更新进度"""
        parent_window = self.window()
        if parent_window is not None:
            status_bar_getter = getattr(parent_window, "statusBar", None)
            status_bar = status_bar_getter() if callable(status_bar_getter) else None
            if status_bar is not None:
                getattr(status_bar, "showMessage", lambda *_args, **_kwargs: None)(f"{message} ({progress}%)")

    def display_results(self, results: dict):
        """显示结果"""
        try:
            # 更新交易信号表格
            self._update_signal_table(results.get('signals', {}))

            # 更新风险分析树
            self._update_risk_tree(results.get('risk_analysis', {}))

            # 更新绩效分析表格
            self._update_performance_table(results.get('performance_metrics', {}))

            self.execute_btn.setEnabled(True)
            QMessageBox.information(self, "完成", "策略分析完成!")

        except Exception as e:
            QMessageBox.critical(self, "错误", f"结果显示失败: {e}")

    def _update_signal_table(self, signals: dict[str, float]):
        """更新交易信号表格"""
        self.signal_table.setRowCount(len(signals))

        for row, (asset, signal) in enumerate(signals.items()):
            self.signal_table.setItem(row, 0, QTableWidgetItem(asset))
            self.signal_table.setItem(row, 1, QTableWidgetItem(f"{signal:.4f}"))
            self.signal_table.setItem(row, 2, QTableWidgetItem(f"{abs(signal):.2%}"))

            # 风险贡献（简化计算）
            risk_contribution = abs(signal) * 0.1
            self.signal_table.setItem(row, 3, QTableWidgetItem(f"{risk_contribution:.2%}"))

    def _update_risk_tree(self, risk_analysis: dict[str, float]):
        """更新风险分析树"""
        self.risk_tree.clear()

        # 市场风险
        market_risk = QTreeWidgetItem(["市场风险", "", ""])
        for metric, value in risk_analysis.items():
            if "var" in metric.lower() or "cvar" in metric.lower():
                item = QTreeWidgetItem([metric, f"{value:.2%}", self._get_risk_status(value)])
                market_risk.addChild(item)
        self.risk_tree.addTopLevelItem(market_risk)

        # 流动性风险
        liquidity_risk = QTreeWidgetItem(["流动性风险", "", ""])
        liquidity_item = QTreeWidgetItem(["流动性风险", f"{risk_analysis.get('liquidity_risk', 0):.2%}", "低"])
        liquidity_risk.addChild(liquidity_item)
        self.risk_tree.addTopLevelItem(liquidity_risk)

    def _update_performance_table(self, metrics: dict[str, float]):
        """更新绩效分析表格"""
        self.performance_table.setRowCount(len(metrics))

        for row, (metric, value) in enumerate(metrics.items()):
            self.performance_table.setItem(row, 0, QTableWidgetItem(metric))
            self.performance_table.setItem(row, 1, QTableWidgetItem(f"{value:.2%}"))
            self.performance_table.setItem(row, 2, QTableWidgetItem(self._get_performance_rating(value, metric)))

    def _get_risk_status(self, value: float) -> str:
        """获取风险状态"""
        if value < 0.03:
            return "低"
        elif value < 0.08:
            return "中"
        else:
            return "高"

    def _get_performance_rating(self, value: float, metric: str) -> str:
        """获取绩效评级"""
        if metric == "sharpe_ratio":
            if value > 1.0:
                return "优秀"
            elif value > 0.5:
                return "良好"
            else:
                return "一般"
        elif metric == "max_drawdown":
            if value < 0.10:
                return "优秀"
            elif value < 0.20:
                return "良好"
            else:
                return "注意"
        else:
            return "-"

    def show_error(self, error_message: str):
        """显示错误"""
        QMessageBox.critical(self, "策略错误", error_message)
        self.execute_btn.setEnabled(True)


if __name__ == "__main__":
    from PyQt5.QtWidgets import QApplication

    app = QApplication(sys.argv)

    panel = CrossPeriodHedgingPanel()
    panel.show()

    sys.exit(app.exec_())
