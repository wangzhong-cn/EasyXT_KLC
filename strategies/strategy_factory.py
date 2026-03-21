"""
策略工厂 —— 从 StrategyConfig.strategy_type 映射到 BaseStrategy 子类

将 management/backtest_engine.py 中硬编码的 5 种策略类型
统一映射为原生 BaseStrategy 实例，使 StrategyRunner / 引擎A 可以驱动它们。

用法::

    from strategies.strategy_factory import create_strategy_from_config
    from strategies.management.strategy_manager import StrategyConfig

    config = StrategyConfig(strategy_type="trend", ...)
    strategy = create_strategy_from_config(config)
    # strategy 是 BaseStrategy 子类实例，可交给 StrategyRunner
"""

from __future__ import annotations

from typing import Any, Dict

from strategies.base_strategy import BaseStrategy


# 策略类型 → (模块路径, 类名) 的延迟映射，避免循环导入
_STRATEGY_TYPE_MAP: Dict[str, tuple[str, str]] = {
    "trend": (
        "strategies.trend_following.dual_ma_strategy",
        "DualMovingAverageStrategy",
    ),
    "reversion": (
        "strategies.trend_following.rsi_reversion_strategy",
        "RSIReversionStrategy",
    ),
    "grid": (
        "strategies.grid_trading.fixed_grid_strategy",
        "FixedGridStrategy",
    ),
    "conditional": (
        "strategies.conditional_orders.conditional_stop_strategy",
        "ConditionalStopStrategy",
    ),
    "factor": (
        "strategies.trend_following.momentum_factor_strategy",
        "MomentumFactorStrategy",
    ),
    "hedge": (
        "strategies.xueqiu_follow_adapter",
        "XueqiuFollowStrategyAdapter",
    ),
}


def create_strategy_from_config(config: Any) -> BaseStrategy:
    """
    根据 StrategyConfig 创建对应的 BaseStrategy 实例。

    Parameters
    ----------
    config : StrategyConfig
        包含 strategy_type, strategy_id, parameters 等字段。

    Returns
    -------
    BaseStrategy
        可被 StrategyRunner / easyxt_backtest.BacktestEngine 驱动的策略实例。

    Raises
    ------
    ValueError
        未知的 strategy_type。
    """
    strategy_type = getattr(config, "strategy_type", "")
    entry = _STRATEGY_TYPE_MAP.get(strategy_type)
    if entry is None:
        raise ValueError(
            f"未知策略类型 '{strategy_type}'。"
            f"支持: {list(_STRATEGY_TYPE_MAP.keys())}"
        )

    module_path, class_name = entry
    import importlib
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)

    strategy_id = getattr(config, "strategy_id", strategy_type)
    if strategy_type == "hedge":
        params = getattr(config, "parameters", {}) or {}
        if isinstance(params, dict):
            config_file = str(params.get("config_file", "config/default.json"))
            lazy_engine = bool(params.get("lazy_engine", True))
            return cls(strategy_id=strategy_id, config_file=config_file, lazy_engine=lazy_engine)
    return cls(strategy_id=strategy_id)
