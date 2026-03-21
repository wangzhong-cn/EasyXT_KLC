"""
tests/test_operation_panel_smoke.py — operation_panel 组件 smoke 测试（T4）
==========================================================================

使用 pytest-qt 的 qtbot fixture 创建并操作 GUI 组件，
覆盖 validators 的全部逻辑路径和 panel 组件的公开接口。

环境要求：condaenv myenv 中 PyQt5 已安装（已确认可 import）。
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# 确保 PyQt5 可用；如果无 display（headless CI），跳过 GUI widget 测试
# ---------------------------------------------------------------------------

def _qt_available() -> bool:
    try:
        from PyQt5.QtWidgets import QApplication  # noqa: F401
        import pytestqt  # noqa: F401
        return True
    except Exception:
        return False


_QT_SKIP = pytest.mark.skipif(
    not _qt_available(), reason="PyQt5 not available"
)


# ===========================================================================
# validators/__init__.py — OrderData / OrderValidationResult
# ===========================================================================

class TestOrderDataModel:
    def test_order_data_immutable(self):
        from gui_app.enhanced.operation_panel.validators import OrderData
        o = OrderData(symbol="000001.SZ", side="buy", price=10.5, volume=100)
        with pytest.raises((AttributeError, TypeError)):
            o.price = 99.0  # type: ignore[misc]

    def test_valid_result(self):
        from gui_app.enhanced.operation_panel.validators import OrderValidationResult
        r = OrderValidationResult.valid()
        assert r.is_valid is True
        assert r.error_code is None

    def test_invalid_result(self):
        from gui_app.enhanced.operation_panel.validators import OrderValidationResult
        r = OrderValidationResult.invalid("CODE_X", "message", {"k": 1})
        assert r.is_valid is False
        assert r.error_code == "CODE_X"
        assert r.details == {"k": 1}


# ===========================================================================
# validator_chain.py — ValidatorChain (R5 扩展)
# ===========================================================================

class TestValidatorChain:
    def _make_chain(self, validators=(), risk_engine=None):
        from gui_app.enhanced.operation_panel.validators.validator_chain import ValidatorChain
        return ValidatorChain(validators, risk_engine=risk_engine)

    def _make_order(self, symbol="000001.SZ", side="buy", price=10.0, volume=100):
        from gui_app.enhanced.operation_panel.validators import OrderData
        return OrderData(symbol=symbol, side=side, price=price, volume=volume)

    def test_empty_chain_returns_valid(self):
        chain = self._make_chain()
        result = chain.validate(self._make_order())
        assert result.is_valid is True

    def test_first_failing_validator_stops_chain(self):
        from gui_app.enhanced.operation_panel.validators import OrderValidationResult
        from gui_app.enhanced.operation_panel.validators.validator_chain import BaseValidator

        class FailValidator(BaseValidator):
            def validate(self, o):
                return OrderValidationResult.invalid("FAIL", "always fail")

        class NeverCalledValidator(BaseValidator):
            def validate(self, o):
                raise AssertionError("should not be called")

        chain = self._make_chain([FailValidator(), NeverCalledValidator()])
        result = chain.validate(self._make_order())
        assert not result.is_valid
        assert result.error_code == "FAIL"

    def test_all_pass_returns_valid(self):
        from gui_app.enhanced.operation_panel.validators import OrderValidationResult
        from gui_app.enhanced.operation_panel.validators.validator_chain import BaseValidator

        class PassValidator(BaseValidator):
            def validate(self, o):
                return OrderValidationResult.valid()

        chain = self._make_chain([PassValidator(), PassValidator()])
        assert chain.validate(self._make_order()).is_valid is True

    def test_risk_engine_blocked_prevents_chain(self):
        """R5: RiskEngine 阻断后不进行后续验证。"""
        from gui_app.enhanced.operation_panel.validators import OrderValidationResult
        from gui_app.enhanced.operation_panel.validators.validator_chain import BaseValidator

        mock_engine = MagicMock()
        risk_result = MagicMock()
        risk_result.blocked = True
        risk_result.reason = "HALT: intraday_drawdown exceeded"
        risk_result.action = MagicMock()
        risk_result.action.value = "halt"
        risk_result.metrics = {"net_exposure": 0.96}
        mock_engine.check_pre_trade.return_value = risk_result

        class NeverCalled(BaseValidator):
            def validate(self, o):
                raise AssertionError("chain should not run")

        chain = self._make_chain([NeverCalled()], risk_engine=mock_engine)
        result = chain.validate(
            self._make_order(),
            account_id="acc_001",
            nav=100000.0,
            positions={"000001.SZ": 50000},
        )
        assert not result.is_valid
        assert result.error_code == "RISK_ENGINE_BLOCKED"
        assert "halt" in result.details["risk_action"]

    def test_risk_engine_pass_continues_chain(self):
        """R5: RiskEngine 通过后继续正常验证链。"""
        mock_engine = MagicMock()
        risk_result = MagicMock()
        risk_result.blocked = False
        mock_engine.check_pre_trade.return_value = risk_result

        from gui_app.enhanced.operation_panel.validators import OrderValidationResult
        from gui_app.enhanced.operation_panel.validators.validator_chain import BaseValidator

        class PassValidator(BaseValidator):
            def validate(self, o):
                return OrderValidationResult.valid()

        chain = self._make_chain([PassValidator()], risk_engine=mock_engine)
        result = chain.validate(
            self._make_order(),
            account_id="acc_001",
            nav=100000.0,
        )
        assert result.is_valid

    def test_no_risk_check_without_account_id(self):
        """R5: 未提供 account_id 时跳过 RiskEngine（向后兼容）。"""
        mock_engine = MagicMock()
        from gui_app.enhanced.operation_panel.validators import OrderValidationResult
        from gui_app.enhanced.operation_panel.validators.validator_chain import BaseValidator

        class PassValidator(BaseValidator):
            def validate(self, o):
                return OrderValidationResult.valid()

        chain = self._make_chain([PassValidator()], risk_engine=mock_engine)
        chain.validate(self._make_order())  # no account_id
        mock_engine.check_pre_trade.assert_not_called()

    def test_set_risk_engine_after_construction(self):
        chain = self._make_chain()
        assert chain._risk_engine is None
        mock_engine = MagicMock()
        chain.set_risk_engine(mock_engine)
        assert chain._risk_engine is mock_engine

    def test_risk_engine_exception_does_not_block_chain(self):
        """R5: RiskEngine 抛异常时降级容错，继续普通验证链。"""
        mock_engine = MagicMock()
        mock_engine.check_pre_trade.side_effect = RuntimeError("engine down")

        from gui_app.enhanced.operation_panel.validators import OrderValidationResult
        from gui_app.enhanced.operation_panel.validators.validator_chain import BaseValidator

        class PassValidator(BaseValidator):
            def validate(self, o):
                return OrderValidationResult.valid()

        chain = self._make_chain([PassValidator()], risk_engine=mock_engine)
        result = chain.validate(self._make_order(), account_id="a", nav=100000.0)
        assert result.is_valid  # 降级容错 → 链正常运行


# ===========================================================================
# advanced_validators.py
# ===========================================================================

class TestAdvancedValidators:
    def _make_order(self, side="buy"):
        from gui_app.enhanced.operation_panel.validators import OrderData
        return OrderData(symbol="000001.SZ", side=side, price=10.0, volume=100)

    def test_daily_loss_within_limit_passes(self):
        from gui_app.enhanced.operation_panel.validators.advanced_validators import DailyLossValidator
        v = DailyLossValidator(
            get_daily_loss=lambda: 500.0,
            get_total_asset=lambda: 100000.0,
            max_daily_loss_ratio=0.02,
        )
        assert v.validate(self._make_order()).is_valid

    def test_daily_loss_exceeds_limit_fails(self):
        from gui_app.enhanced.operation_panel.validators.advanced_validators import DailyLossValidator
        v = DailyLossValidator(
            get_daily_loss=lambda: 3000.0,
            get_total_asset=lambda: 100000.0,
            max_daily_loss_ratio=0.02,
        )
        result = v.validate(self._make_order())
        assert not result.is_valid
        assert result.error_code == "DAILY_LOSS_LIMIT"

    def test_daily_loss_zero_total_asset_passes(self):
        from gui_app.enhanced.operation_panel.validators.advanced_validators import DailyLossValidator
        v = DailyLossValidator(
            get_daily_loss=lambda: 999.0,
            get_total_asset=lambda: 0.0,
            max_daily_loss_ratio=0.01,
        )
        assert v.validate(self._make_order()).is_valid

    def test_concentration_sell_skipped(self):
        from gui_app.enhanced.operation_panel.validators.advanced_validators import ConcentrationValidator
        v = ConcentrationValidator(
            get_position_value=lambda s: 30000.0,
            get_total_asset=lambda: 100000.0,
            max_concentration_ratio=0.30,
        )
        assert v.validate(self._make_order(side="sell")).is_valid

    def test_concentration_buy_within_limit_passes(self):
        from gui_app.enhanced.operation_panel.validators.advanced_validators import ConcentrationValidator
        v = ConcentrationValidator(
            get_position_value=lambda s: 10000.0,
            get_total_asset=lambda: 100000.0,
            max_concentration_ratio=0.30,
        )
        assert v.validate(self._make_order()).is_valid  # 10000+(10*100)=11000 → 11%

    def test_concentration_exceeds_limit_blocked(self):
        from gui_app.enhanced.operation_panel.validators.advanced_validators import ConcentrationValidator
        v = ConcentrationValidator(
            get_position_value=lambda s: 29000.0,
            get_total_asset=lambda: 100000.0,
            max_concentration_ratio=0.30,
        )
        # 29000 + 10*200 = 31000 → 31% > 30%
        from gui_app.enhanced.operation_panel.validators import OrderData
        order = OrderData(symbol="000001.SZ", side="buy", price=10.0, volume=200)
        result = v.validate(order)
        assert not result.is_valid


# ===========================================================================
# basic_validators.py — BasicOrderValidator smoke
# ===========================================================================

class TestBasicValidators:
    """BasicOrderValidator 覆盖：价格/数量/方向/标的四类校验。"""

    def _make_order(self, symbol="000001.SZ", side="buy", price=10.0, volume=100):
        from gui_app.enhanced.operation_panel.validators import OrderData
        return OrderData(symbol=symbol, side=side, price=price, volume=volume)

    def _validator(self):
        from gui_app.enhanced.operation_panel.validators.basic_validators import BasicOrderValidator
        return BasicOrderValidator()

    def test_valid_order_passes(self):
        assert self._validator().validate(self._make_order()).is_valid

    def test_zero_price_fails(self):
        result = self._validator().validate(self._make_order(price=0.0))
        assert not result.is_valid
        assert result.error_code == "INVALID_PRICE"

    def test_negative_price_fails(self):
        result = self._validator().validate(self._make_order(price=-1.0))
        assert not result.is_valid

    def test_zero_volume_fails(self):
        result = self._validator().validate(self._make_order(volume=0))
        assert not result.is_valid
        assert result.error_code == "INVALID_VOLUME"

    def test_empty_symbol_fails(self):
        result = self._validator().validate(self._make_order(symbol=""))
        assert not result.is_valid
        assert result.error_code == "EMPTY_SYMBOL"

    def test_sell_side_valid(self):
        assert self._validator().validate(self._make_order(side="sell")).is_valid

    def test_invalid_side_fails(self):
        result = self._validator().validate(self._make_order(side="short"))
        assert not result.is_valid
        assert result.error_code == "INVALID_SIDE"


# ===========================================================================
# panel widgets — QApplication smoke（需要 PyQt5）
# ===========================================================================

@_QT_SKIP
class TestPositionPanelSmoke:
    def test_import_and_create(self, qtbot):
        from gui_app.enhanced.operation_panel.position_panel import PositionPanel
        panel = PositionPanel()
        qtbot.addWidget(panel)
        assert panel is not None

    def test_update_positions(self, qtbot):
        from gui_app.enhanced.operation_panel.position_panel import PositionPanel
        panel = PositionPanel()
        qtbot.addWidget(panel)
        panel.update_positions([
            {"stock_code": "000001.SZ", "volume": 100, "cost_price": 10.0,
             "market_value": 1050.0, "profit": 50.0},
        ])

    def test_clear_data(self, qtbot):
        from gui_app.enhanced.operation_panel.position_panel import PositionPanel
        panel = PositionPanel()
        qtbot.addWidget(panel)
        panel.clear_data()  # 不应抛异常


@_QT_SKIP
class TestAccountPanelSmoke:
    def test_import_and_create(self, qtbot):
        from gui_app.enhanced.operation_panel.account_panel import AccountPanel
        panel = AccountPanel()
        qtbot.addWidget(panel)
        assert panel is not None

    def test_set_accounts(self, qtbot):
        from gui_app.enhanced.operation_panel.account_panel import AccountPanel
        panel = AccountPanel()
        qtbot.addWidget(panel)
        panel.set_accounts(
            [{"account_id": "ACC_001", "account_type": "stock"},
             {"account_id": "ACC_002", "account_type": "stock"}],
            current_id="ACC_001",
        )

    def test_clear_data(self, qtbot):
        from gui_app.enhanced.operation_panel.account_panel import AccountPanel
        panel = AccountPanel()
        qtbot.addWidget(panel)
        panel.clear_data()
