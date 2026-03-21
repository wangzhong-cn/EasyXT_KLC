from __future__ import annotations

import logging
from typing import Any, Optional

from strategies.base_strategy import BarData, BaseStrategy, StrategyContext


class XueqiuFollowStrategyAdapter(BaseStrategy):
    def __init__(self, strategy_id: str, *, config_file: str = "config/default.json", lazy_engine: bool = True) -> None:
        super().__init__(strategy_id)
        self._config_file = config_file
        self._lazy_engine = bool(lazy_engine)
        self._engine: Optional[Any] = None
        self._last_bar: Optional[BarData] = None
        self._adapter_logger = logging.getLogger(f"strategy.xueqiu_adapter.{strategy_id}")

    def on_init(self, context: StrategyContext) -> None:
        if self._lazy_engine:
            return
        self._ensure_engine()

    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        self._last_bar = bar

    def _ensure_engine(self) -> Optional[Any]:
        if self._engine is not None:
            return self._engine
        try:
            from strategies.xueqiu_follow.core.config_manager import ConfigManager
            from strategies.xueqiu_follow.core.strategy_engine import StrategyEngine

            cfg = ConfigManager(config_file=self._config_file)
            self._engine = StrategyEngine(cfg)
            return self._engine
        except Exception as e:
            self._adapter_logger.warning("xueqiu follow adapter 初始化引擎失败: %s", e)
            return None
