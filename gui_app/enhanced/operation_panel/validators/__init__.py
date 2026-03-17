from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class OrderData:
    symbol: str
    side: str
    price: float
    volume: int


@dataclass(frozen=True)
class OrderValidationResult:
    is_valid: bool
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    details: Optional[dict] = None

    @classmethod
    def valid(cls):
        return cls(is_valid=True)

    @classmethod
    def invalid(cls, code: str, message: str, details: Optional[dict] = None):
        return cls(is_valid=False, error_code=code, error_message=message, details=details)


__all__ = ["OrderData", "OrderValidationResult"]
