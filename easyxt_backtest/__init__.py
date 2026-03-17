"""
easyxt_backtest — 原生事件驱动回测引擎

替代 backtrader 外部依赖，实现完全自主可控的量化回测基础设施。

核心模块：
  engine.py          — 事件驱动回测循环（逐 bar，无前视偏差）
  performance.py     — 夏普/卡玛/最大回撤/月归因等绩效指标
  strategy_runner.py — BaseStrategy 生命周期驱动入口

快速入门::

    from easyxt_backtest.strategy_runner import StrategyRunner
    from strategies.my_strategy import MyStrategy

    result = StrategyRunner(
        strategy=MyStrategy("demo"),
        codes=["000001.SZ", "600000.SH"],
        start_date="2022-01-01",
        end_date="2024-12-31",
    ).run()

    print(result.metrics)
"""

from easyxt_backtest.engine import BacktestConfig, BacktestEngine, BacktestResult
from easyxt_backtest.performance import calc_all_metrics
from easyxt_backtest.strategy_runner import StrategyRunner

try:
    from easyxt_backtest.factor_backtest import FactorBacktestConfig, FactorBacktestEngine
except Exception:
    FactorBacktestConfig = None
    FactorBacktestEngine = None

__all__ = [
    "BacktestConfig",
    "BacktestEngine",
    "BacktestResult",
    "StrategyRunner",
    "calc_all_metrics",
]
if FactorBacktestConfig is not None and FactorBacktestEngine is not None:
    __all__.extend(["FactorBacktestConfig", "FactorBacktestEngine"])
