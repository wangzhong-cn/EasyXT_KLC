import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from gui_app.enhanced.operation_panel import (
    BasicOrderValidator,
    BlacklistValidator,
    ConcentrationValidator,
    DailyLossValidator,
    OrderData,
    RiskOrderValidator,
    ValidatorChain,
)


def build_validator(
    available_cash=10000.0,
    total_asset=100000.0,
    position_volume=0,
    position_value=0.0,
    daily_loss=0.0,
    blacklist=(),
    daily_orders=0,
    max_position_ratio=0.3,
    max_concentration_ratio=0.1,
    max_daily_loss_ratio=0.05,
    max_daily_orders=3,
):
    return ValidatorChain([
        BasicOrderValidator(),
        BlacklistValidator(blacklist),
        DailyLossValidator(
            get_daily_loss=lambda: daily_loss,
            get_total_asset=lambda: total_asset,
            max_daily_loss_ratio=max_daily_loss_ratio,
        ),
        ConcentrationValidator(
            get_position_value=lambda _: position_value,
            get_total_asset=lambda: total_asset,
            max_concentration_ratio=max_concentration_ratio,
        ),
        RiskOrderValidator(
            get_available_cash=lambda: available_cash,
            get_total_asset=lambda: total_asset,
            get_position_volume=lambda _: position_volume,
            get_daily_order_count=lambda: daily_orders,
            max_position_ratio=max_position_ratio,
            max_daily_orders=max_daily_orders,
        ),
    ])


def test_basic_validation_rejects_empty_symbol():
    validator = build_validator()
    result = validator.validate(OrderData(symbol="", side="buy", price=10.0, volume=100))
    assert not result.is_valid
    assert result.error_code == "EMPTY_SYMBOL"


def test_basic_validation_rejects_invalid_price():
    validator = build_validator()
    result = validator.validate(OrderData(symbol="000001.SZ", side="buy", price=0, volume=100))
    assert not result.is_valid
    assert result.error_code == "INVALID_PRICE"


def test_basic_validation_rejects_invalid_volume():
    validator = build_validator()
    result = validator.validate(OrderData(symbol="000001.SZ", side="buy", price=10, volume=0))
    assert not result.is_valid
    assert result.error_code == "INVALID_VOLUME"


def test_risk_validation_rejects_daily_limit():
    validator = build_validator(daily_orders=3, max_daily_orders=3)
    result = validator.validate(OrderData(symbol="000001.SZ", side="buy", price=10, volume=100))
    assert not result.is_valid
    assert result.error_code == "DAILY_ORDER_LIMIT"


def test_risk_validation_rejects_insufficient_cash():
    validator = build_validator(available_cash=500, total_asset=100000)
    result = validator.validate(OrderData(symbol="000001.SZ", side="buy", price=10, volume=100))
    assert not result.is_valid
    assert result.error_code == "INSUFFICIENT_CASH"


def test_risk_validation_rejects_position_limit():
    validator = build_validator(
        available_cash=10000,
        total_asset=1000,
        position_volume=10,
        max_position_ratio=0.3,
        max_concentration_ratio=1.0,
    )
    result = validator.validate(OrderData(symbol="000001.SZ", side="buy", price=50, volume=10))
    assert not result.is_valid
    assert result.error_code == "POSITION_LIMIT"


def test_risk_validation_rejects_insufficient_position():
    validator = build_validator(position_volume=50)
    result = validator.validate(OrderData(symbol="000001.SZ", side="sell", price=10, volume=100))
    assert not result.is_valid
    assert result.error_code == "INSUFFICIENT_POSITION"


def test_validation_accepts_valid_order():
    validator = build_validator(position_volume=200)
    result = validator.validate(OrderData(symbol="000001.SZ", side="sell", price=10, volume=100))
    assert result.is_valid


def test_validation_rejects_blacklist():
    validator = build_validator(blacklist=["000001.SZ", "ST*"])
    result = validator.validate(OrderData(symbol="000001.SZ", side="buy", price=10, volume=100))
    assert not result.is_valid
    assert result.error_code == "BLACKLIST_SYMBOL"


def test_validation_rejects_daily_loss_limit():
    validator = build_validator(daily_loss=6000, total_asset=100000, max_daily_loss_ratio=0.05)
    result = validator.validate(OrderData(symbol="000002.SZ", side="buy", price=10, volume=100))
    assert not result.is_valid
    assert result.error_code == "DAILY_LOSS_LIMIT"


def test_validation_rejects_concentration_limit():
    validator = build_validator(position_value=15000, total_asset=100000, max_concentration_ratio=0.1)
    result = validator.validate(OrderData(symbol="000003.SZ", side="buy", price=50, volume=200))
    assert not result.is_valid
    assert result.error_code == "CONCENTRATION_LIMIT"


def test_basic_validation_rejects_invalid_side():
    """BasicOrderValidator line 15: INVALID_SIDE when side not in {buy, sell}."""
    validator = BasicOrderValidator()
    result = validator.validate(OrderData(symbol="000001.SZ", side="long", price=10.0, volume=100))
    assert not result.is_valid
    assert result.error_code == "INVALID_SIDE"


def test_daily_loss_validator_zero_total_asset_is_valid():
    """DailyLossValidator line 18: total_asset <= 0 → valid."""
    v = DailyLossValidator(
        get_daily_loss=lambda: 9999.0,
        get_total_asset=lambda: 0.0,
        max_daily_loss_ratio=0.05,
    )
    result = v.validate(OrderData(symbol="X", side="buy", price=1.0, volume=1))
    assert result.is_valid


def test_concentration_validator_zero_total_asset_on_buy_is_valid():
    """ConcentrationValidator line 45: side=='buy' + total_asset<=0 → valid."""
    v = ConcentrationValidator(
        get_position_value=lambda _: 0.0,
        get_total_asset=lambda: 0.0,
        max_concentration_ratio=0.1,
    )
    result = v.validate(OrderData(symbol="X", side="buy", price=1.0, volume=1))
    assert result.is_valid
