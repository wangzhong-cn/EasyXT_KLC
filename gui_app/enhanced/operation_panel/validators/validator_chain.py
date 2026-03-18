from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Optional

from . import OrderData, OrderValidationResult


class BaseValidator(ABC):
    @abstractmethod
    def validate(self, order: OrderData) -> OrderValidationResult:
        raise NotImplementedError


class ValidatorChain:
    """委托验证链。

    R5 扩展：可注入 ``core.risk_engine.RiskEngine``，在普通验证链之前先执行
    预交易风控检查，消除 GUI 侧与回测侧的双轨验证问题。
    """

    def __init__(self, validators: Iterable[BaseValidator], risk_engine: Optional[object] = None):
        self.validators: list[BaseValidator] = list(validators)
        self._risk_engine = risk_engine  # optional RiskEngine

    def set_risk_engine(self, engine: object) -> None:
        """注入 RiskEngine 实例（可在构造后调用）。"""
        self._risk_engine = engine

    def validate(
        self,
        order: OrderData,
        *,
        account_id: str = "",
        nav: float = 0.0,
        positions: Optional[dict] = None,
    ) -> OrderValidationResult:
        """
        执行验证链。

        如果注入了 RiskEngine 且提供了 account_id / nav，则先执行预交易风控；
        风控被阻断（LIMIT / HALT）时直接返回失败，不再继续普通验证链。

        Parameters
        ----------
        order :
            待验证的委托数据。
        account_id :
            账户 ID，用于 RiskEngine 日内回撤跟踪。
        nav :
            账户净值，用于集中度 / 净敞口检查。
        positions :
            当前持仓市值字典，用于前瞻集中度检查。
        """
        # 1. RiskEngine 预检（R5 联动）
        if self._risk_engine is not None and account_id and nav > 0:
            try:
                from core.risk_engine import RiskAction

                risk_result = self._risk_engine.check_pre_trade(  # type: ignore[union-attr]
                    account_id=account_id,
                    code=order.symbol,
                    volume=order.volume,
                    price=order.price,
                    direction=order.side,
                    positions=positions or {},
                    nav=nav,
                )
                if risk_result.blocked:
                    return OrderValidationResult.invalid(
                        "RISK_ENGINE_BLOCKED",
                        risk_result.reason,
                        {"risk_action": risk_result.action.value, **risk_result.metrics},
                    )
            except Exception:
                pass  # RiskEngine 调用失败不阻断普通验证

        # 2. 普通验证链
        for validator in self.validators:
            result = validator.validate(order)
            if not result.is_valid:
                return result
        return OrderValidationResult.valid()
