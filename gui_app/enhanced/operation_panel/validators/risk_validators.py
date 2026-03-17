from typing import Callable

from . import OrderData, OrderValidationResult
from .validator_chain import BaseValidator


class RiskOrderValidator(BaseValidator):
    def __init__(
        self,
        get_available_cash: Callable[[], float],
        get_total_asset: Callable[[], float],
        get_position_volume: Callable[[str], int],
        get_daily_order_count: Callable[[], int],
        max_position_ratio: float = 0.3,
        max_daily_orders: int = 100,
    ):
        self.get_available_cash = get_available_cash
        self.get_total_asset = get_total_asset
        self.get_position_volume = get_position_volume
        self.get_daily_order_count = get_daily_order_count
        self.max_position_ratio = max_position_ratio
        self.max_daily_orders = max_daily_orders

    def validate(self, order: OrderData) -> OrderValidationResult:
        daily_count = self.get_daily_order_count()
        if daily_count >= self.max_daily_orders:
            return OrderValidationResult.invalid(
                "DAILY_ORDER_LIMIT",
                f"单日下单次数已达上限: {daily_count}",
            )

        if order.side == "buy":
            available_cash = self.get_available_cash()
            total_asset = self.get_total_asset()
            order_value = order.price * order.volume
            if available_cash and order_value > available_cash:
                return OrderValidationResult.invalid(
                    "INSUFFICIENT_CASH",
                    f"可用资金不足: {available_cash:.2f}",
                    {"required": order_value},
                )
            if total_asset:
                current_position_value = self.get_position_volume(order.symbol) * order.price
                target_ratio = (current_position_value + order_value) / total_asset
                if target_ratio > self.max_position_ratio:
                    return OrderValidationResult.invalid(
                        "POSITION_LIMIT",
                        f"持仓比例超限: {target_ratio:.2%}",
                        {"max_ratio": self.max_position_ratio},
                    )
        else:
            position_volume = self.get_position_volume(order.symbol)
            if position_volume < order.volume:
                return OrderValidationResult.invalid(
                    "INSUFFICIENT_POSITION",
                    f"可用持仓不足: {position_volume}",
                )

        return OrderValidationResult.valid()
