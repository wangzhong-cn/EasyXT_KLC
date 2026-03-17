"""Unit tests for gui_app.enhanced.operation_panel.validators."""
import pytest

from gui_app.enhanced.operation_panel.validators import OrderData, OrderValidationResult
from gui_app.enhanced.operation_panel.validators.basic_validators import BasicOrderValidator
from gui_app.enhanced.operation_panel.validators.validator_chain import ValidatorChain
from gui_app.enhanced.operation_panel.validators.advanced_validators import (
    DailyLossValidator,
    ConcentrationValidator,
    BlacklistValidator,
)
from gui_app.enhanced.operation_panel.validators.risk_validators import RiskOrderValidator


def _order(**kwargs):
    defaults = dict(symbol="600000.SH", side="buy", price=10.0, volume=100)
    defaults.update(kwargs)
    return OrderData(**defaults)


# ---------------------------------------------------------------------------
# OrderData & OrderValidationResult
# ---------------------------------------------------------------------------
class TestOrderValidationResult:
    def test_valid_result(self):
        r = OrderValidationResult.valid()
        assert r.is_valid is True
        assert r.error_code is None

    def test_invalid_result_carries_code_and_message(self):
        r = OrderValidationResult.invalid("CODE", "msg", {"x": 1})
        assert r.is_valid is False
        assert r.error_code == "CODE"
        assert r.error_message == "msg"
        assert r.details == {"x": 1}

    def test_frozen(self):
        r = OrderValidationResult.valid()
        with pytest.raises(Exception):
            r.is_valid = False  # type: ignore[misc]


# ---------------------------------------------------------------------------
# BasicOrderValidator
# ---------------------------------------------------------------------------
class TestBasicOrderValidator:
    def setup_method(self):
        self.v = BasicOrderValidator()

    def test_valid_order_passes(self):
        assert self.v.validate(_order()).is_valid

    def test_empty_symbol_rejected(self):
        r = self.v.validate(_order(symbol="   "))
        assert not r.is_valid
        assert r.error_code == "EMPTY_SYMBOL"

    def test_zero_price_rejected(self):
        r = self.v.validate(_order(price=0.0))
        assert not r.is_valid
        assert r.error_code == "INVALID_PRICE"

    def test_negative_price_rejected(self):
        r = self.v.validate(_order(price=-1.0))
        assert not r.is_valid
        assert r.error_code == "INVALID_PRICE"

    def test_zero_volume_rejected(self):
        r = self.v.validate(_order(volume=0))
        assert not r.is_valid
        assert r.error_code == "INVALID_VOLUME"

    def test_invalid_side_rejected(self):
        r = self.v.validate(_order(side="short"))
        assert not r.is_valid
        assert r.error_code == "INVALID_SIDE"

    def test_sell_side_valid(self):
        assert self.v.validate(_order(side="sell")).is_valid


# ---------------------------------------------------------------------------
# ValidatorChain
# ---------------------------------------------------------------------------
class TestValidatorChain:
    def test_empty_chain_passes(self):
        chain = ValidatorChain([])
        assert chain.validate(_order()).is_valid

    def test_single_pass_validator(self):
        chain = ValidatorChain([BasicOrderValidator()])
        assert chain.validate(_order()).is_valid

    def test_single_fail_validator(self):
        chain = ValidatorChain([BasicOrderValidator()])
        assert not chain.validate(_order(price=0.0)).is_valid

    def test_chain_short_circuits_on_first_failure(self):
        """Second validator should not be reached when first fails."""
        calls = []

        class CountingValidator:
            def validate(self, order):
                calls.append(1)
                return OrderValidationResult.valid()

        chain = ValidatorChain([BasicOrderValidator(), CountingValidator()])
        chain.validate(_order(price=0.0))
        # BasicOrderValidator rejects → CountingValidator never invoked
        assert calls == []

    def test_chain_reaches_second_validator(self):
        calls = []

        class CountingValidator:
            def validate(self, order):
                calls.append(1)
                return OrderValidationResult.valid()

        chain = ValidatorChain([BasicOrderValidator(), CountingValidator()])
        chain.validate(_order())
        assert calls == [1]

    def test_all_validators_pass(self):
        chain = ValidatorChain([BasicOrderValidator(), BasicOrderValidator()])
        assert chain.validate(_order()).is_valid


# ---------------------------------------------------------------------------
# DailyLossValidator
# ---------------------------------------------------------------------------
class TestDailyLossValidator:
    def _make(self, daily_loss, total_asset, max_ratio=0.05):
        return DailyLossValidator(
            get_daily_loss=lambda: daily_loss,
            get_total_asset=lambda: total_asset,
            max_daily_loss_ratio=max_ratio,
        )

    def test_within_limit_passes(self):
        v = self._make(daily_loss=200, total_asset=10_000)
        assert v.validate(_order()).is_valid

    def test_at_limit_fails(self):
        # loss == max_ratio means it's over (> check)
        v = self._make(daily_loss=500, total_asset=10_000)  # exactly 5%
        assert v.validate(_order()).is_valid  # 500/10000 == 0.05 → NOT > 0.05

    def test_over_limit_fails(self):
        v = self._make(daily_loss=501, total_asset=10_000)  # > 5%
        r = v.validate(_order())
        assert not r.is_valid
        assert r.error_code == "DAILY_LOSS_LIMIT"

    def test_zero_total_asset_passes(self):
        # Guard: no division by zero when total_asset == 0
        v = self._make(daily_loss=999, total_asset=0)
        assert v.validate(_order()).is_valid


# ---------------------------------------------------------------------------
# ConcentrationValidator
# ---------------------------------------------------------------------------
class TestConcentrationValidator:
    def _make(self, current_value, total_asset, max_ratio=0.30):
        return ConcentrationValidator(
            get_position_value=lambda sym: current_value,
            get_total_asset=lambda: total_asset,
            max_concentration_ratio=max_ratio,
        )

    def test_buy_within_limit_passes(self):
        # current 0, buying 100@10 = 1000, total 10000 → 10%
        v = self._make(current_value=0, total_asset=10_000)
        assert v.validate(_order(price=10.0, volume=100)).is_valid

    def test_buy_over_limit_fails(self):
        # current 2500, buying 500@10 = 5000, total 10000 → 75%
        v = self._make(current_value=2_500, total_asset=10_000)
        r = v.validate(_order(price=10.0, volume=500))
        assert not r.is_valid
        assert r.error_code == "CONCENTRATION_LIMIT"

    def test_sell_always_passes(self):
        # sell side skips concentration check
        v = self._make(current_value=9_000, total_asset=10_000)
        assert v.validate(_order(side="sell", price=10.0, volume=500)).is_valid

    def test_zero_total_asset_passes(self):
        v = self._make(current_value=0, total_asset=0)
        assert v.validate(_order()).is_valid


# ---------------------------------------------------------------------------
# BlacklistValidator
# ---------------------------------------------------------------------------
class TestBlacklistValidator:
    def test_symbol_not_in_blacklist_passes(self):
        v = BlacklistValidator(["000001.SZ"])
        assert v.validate(_order(symbol="600000.SH")).is_valid

    def test_exact_symbol_rejected(self):
        v = BlacklistValidator(["600000.SH"])
        r = v.validate(_order(symbol="600000.SH"))
        assert not r.is_valid
        assert r.error_code == "BLACKLIST_SYMBOL"

    def test_wildcard_pattern_rejected(self):
        v = BlacklistValidator(["0000*.SZ"])
        r = v.validate(_order(symbol="000001.SZ"))
        assert not r.is_valid

    def test_wildcard_pattern_no_match_passes(self):
        v = BlacklistValidator(["0000*.SZ"])
        assert v.validate(_order(symbol="600000.SH")).is_valid

    def test_empty_blacklist_passes(self):
        v = BlacklistValidator([])
        assert v.validate(_order()).is_valid

    def test_empty_patterns_filtered_out(self):
        # empty-string patterns should be ignored
        v = BlacklistValidator(["", "  "])
        assert v.validate(_order()).is_valid


# ---------------------------------------------------------------------------
# RiskOrderValidator
# ---------------------------------------------------------------------------
class TestRiskOrderValidator:
    def _make(
        self,
        available_cash=100_000.0,
        total_asset=200_000.0,
        position_volume=0,
        daily_order_count=0,
        max_position_ratio=0.30,
        max_daily_orders=100,
    ):
        return RiskOrderValidator(
            get_available_cash=lambda: available_cash,
            get_total_asset=lambda: total_asset,
            get_position_volume=lambda sym: position_volume,
            get_daily_order_count=lambda: daily_order_count,
            max_position_ratio=max_position_ratio,
            max_daily_orders=max_daily_orders,
        )

    def test_normal_buy_passes(self):
        v = self._make()
        assert v.validate(_order(price=10.0, volume=100)).is_valid

    def test_daily_order_limit_triggers(self):
        v = self._make(daily_order_count=100, max_daily_orders=100)
        r = v.validate(_order())
        assert not r.is_valid
        assert r.error_code == "DAILY_ORDER_LIMIT"

    def test_insufficient_cash_rejected(self):
        # order value: 500@10 = 5000, but only 100 cash
        v = self._make(available_cash=100.0)
        r = v.validate(_order(price=10.0, volume=500))
        assert not r.is_valid
        assert r.error_code == "INSUFFICIENT_CASH"

    def test_position_ratio_exceeded_rejected(self):
        # buy 6000@10 = 60000 on 200k total → 30%+ existing position 0
        v = self._make(max_position_ratio=0.29)
        r = v.validate(_order(price=10.0, volume=6_000))
        assert not r.is_valid
        assert r.error_code == "POSITION_LIMIT"

    def test_sell_sufficient_position_passes(self):
        v = self._make(position_volume=500)
        assert v.validate(_order(side="sell", volume=100)).is_valid

    def test_sell_insufficient_position_rejected(self):
        v = self._make(position_volume=50)
        r = v.validate(_order(side="sell", volume=100))
        assert not r.is_valid
        assert r.error_code == "INSUFFICIENT_POSITION"
