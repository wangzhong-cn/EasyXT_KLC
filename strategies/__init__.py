"""
strategies 包 —— 统一策略入口

所有策略应继承 ``base_strategy.BaseStrategy``，使用统一的生命周期协议。
通过 ``StrategyController`` 可一站式完成配置→实例化→注册→回测→注销。
"""

from strategies.base_strategy import BarData, BaseStrategy, OrderData, StrategyContext, TickData
from strategies.strategy_controller import StrategyController

__all__ = [
    "BaseStrategy",
    "BarData",
    "TickData",
    "OrderData",
    "StrategyContext",
    "StrategyController",
]
