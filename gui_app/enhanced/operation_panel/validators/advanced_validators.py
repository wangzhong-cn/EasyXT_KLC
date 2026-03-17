from collections.abc import Iterable
from fnmatch import fnmatch
from typing import Callable

from . import OrderData, OrderValidationResult
from .validator_chain import BaseValidator


class DailyLossValidator(BaseValidator):
    def __init__(self, get_daily_loss: Callable[[], float], get_total_asset: Callable[[], float], max_daily_loss_ratio: float):
        self.get_daily_loss = get_daily_loss
        self.get_total_asset = get_total_asset
        self.max_daily_loss_ratio = max_daily_loss_ratio

    def validate(self, order: OrderData) -> OrderValidationResult:
        total_asset = self.get_total_asset()
        if total_asset <= 0:
            return OrderValidationResult.valid()
        daily_loss = self.get_daily_loss()
        if daily_loss / total_asset > self.max_daily_loss_ratio:
            return OrderValidationResult.invalid(
                "DAILY_LOSS_LIMIT",
                f"单日亏损已达上限: {daily_loss / total_asset:.2%}",
                {"max_ratio": self.max_daily_loss_ratio},
            )
        return OrderValidationResult.valid()


class ConcentrationValidator(BaseValidator):
    def __init__(
        self,
        get_position_value: Callable[[str], float],
        get_total_asset: Callable[[], float],
        max_concentration_ratio: float,
    ):
        self.get_position_value = get_position_value
        self.get_total_asset = get_total_asset
        self.max_concentration_ratio = max_concentration_ratio

    def validate(self, order: OrderData) -> OrderValidationResult:
        if order.side != "buy":
            return OrderValidationResult.valid()
        total_asset = self.get_total_asset()
        if total_asset <= 0:
            return OrderValidationResult.valid()
        current_value = self.get_position_value(order.symbol)
        target_ratio = (current_value + order.price * order.volume) / total_asset
        if target_ratio > self.max_concentration_ratio:
            return OrderValidationResult.invalid(
                "CONCENTRATION_LIMIT",
                f"标的集中度超限: {target_ratio:.2%}",
                {"max_ratio": self.max_concentration_ratio},
            )
        return OrderValidationResult.valid()


class BlacklistValidator(BaseValidator):
    def __init__(self, blacklist_patterns: Iterable[str]):
        self.blacklist_patterns = [pattern for pattern in blacklist_patterns if pattern]

    def validate(self, order: OrderData) -> OrderValidationResult:
        for pattern in self.blacklist_patterns:
            if fnmatch(order.symbol, pattern):
                return OrderValidationResult.invalid(
                    "BLACKLIST_SYMBOL",
                    f"标的在黑名单中: {order.symbol}",
                    {"pattern": pattern},
                )
        return OrderValidationResult.valid()
