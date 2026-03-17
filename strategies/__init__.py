"""
strategies 包 —— 统一策略入口

所有策略应继承 ``base_strategy.BaseStrategy``，使用统一的生命周期协议。
"""

from strategies.base_strategy import BaseStrategy, BarData, OrderData, StrategyContext

__all__ = [
    "BaseStrategy",
    "BarData",
    "OrderData",
    "StrategyContext",
]
