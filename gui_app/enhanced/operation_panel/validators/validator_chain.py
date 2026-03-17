from abc import ABC, abstractmethod
from collections.abc import Iterable

from . import OrderData, OrderValidationResult


class BaseValidator(ABC):
    @abstractmethod
    def validate(self, order: OrderData) -> OrderValidationResult:
        raise NotImplementedError


class ValidatorChain:
    def __init__(self, validators: Iterable[BaseValidator]):
        self.validators: list[BaseValidator] = list(validators)

    def validate(self, order: OrderData) -> OrderValidationResult:
        for validator in self.validators:
            result = validator.validate(order)
            if not result.is_valid:
                return result
        return OrderValidationResult.valid()
