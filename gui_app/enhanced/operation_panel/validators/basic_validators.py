from . import OrderData, OrderValidationResult
from .validator_chain import BaseValidator


class BasicOrderValidator(BaseValidator):
    def validate(self, order: OrderData) -> OrderValidationResult:
        symbol = order.symbol.strip()
        if not symbol:
            return OrderValidationResult.invalid("EMPTY_SYMBOL", "交易标的不能为空")
        if order.price <= 0:
            return OrderValidationResult.invalid("INVALID_PRICE", f"价格必须大于0: {order.price}")
        if order.volume <= 0:
            return OrderValidationResult.invalid("INVALID_VOLUME", f"数量必须大于0: {order.volume}")
        if order.side not in {"buy", "sell"}:
            return OrderValidationResult.invalid("INVALID_SIDE", f"方向非法: {order.side}")
        return OrderValidationResult.valid()
