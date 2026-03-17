import os
import sys

import pandas as pd
import pytest
from PyQt5.QtWidgets import QDialog, QMessageBox

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from strategies.management.backtest_engine import BacktestEngine
from strategies.management.strategy_gui import StrategyCreationDialog, StrategyManagementWidget
from strategies.management.strategy_manager import StrategyConfig, StrategyManager


def _build_base_config():
    return {
        "strategy_name": "测试策略",
        "strategy_type": "trend",
        "base_strategy": "MovingAverageCrossover",
        "parameters": {"fast_period": 5, "slow_period": 20, "adjust": "none"},
        "risk_controls": {
            "max_position": 0.2,
            "daily_stop_loss": 0.03,
            "max_drawdown": 0.15,
        },
        "symbols": ["000001.SZ"],
        "period": "1d",
        "backtest_range": {"start": "2023-01-01", "end": "2023-01-30"},
        "trading_cost": {"commission": 0.0003, "tax": 0.001, "slippage_bps": 0},
    }


def _build_price_series(values):
    return pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=len(values), freq="D"),
            "close": values,
        }
    )


def test_strategy_config_risk_controls_validation():
    config = _build_base_config()
    config["risk_controls"]["max_position"] = 0.25
    with pytest.raises(ValueError):
        StrategyConfig(strategy_id="x", **config)


def test_strategy_config_invalid_period():
    config = _build_base_config()
    config["period"] = "10m"
    with pytest.raises(ValueError):
        StrategyConfig(strategy_id="x", **config)


def test_strategy_manager_crud(tmp_path):
    manager = StrategyManager(config_dir=str(tmp_path))
    config_data = _build_base_config()
    strategy_id = manager.create_strategy(config_data)
    assert manager.get_strategy(strategy_id) is not None

    listed = manager.list_strategies()
    assert len(listed) == 1
    assert listed[0]["version"] == 1
    assert listed[0]["symbols_count"] == 1

    updated = manager.update_strategy(strategy_id, {"parameters": {"fast_period": 8}})
    assert updated is True
    assert manager.get_strategy(strategy_id).version == 2

    deleted = manager.delete_strategy(strategy_id)
    assert deleted is True
    assert manager.get_strategy(strategy_id) is None


def test_strategy_manager_update_invalid_id(tmp_path):
    manager = StrategyManager(config_dir=str(tmp_path))
    assert manager.update_strategy("missing", {"parameters": {"fast_period": 8}}) is False


def test_strategy_manager_update_invalid_risk_controls(tmp_path):
    manager = StrategyManager(config_dir=str(tmp_path))
    config_data = _build_base_config()
    strategy_id = manager.create_strategy(config_data)
    updated = manager.update_strategy(strategy_id, {"risk_controls": {"max_position": 0.3}})
    assert updated is False


def test_backtest_engine_runs_with_mock_data(tmp_path, monkeypatch):
    from gui_app.backtest import data_manager as dm

    def mock_get_stock_data(self, stock_code, start_date, end_date, period="1d", adjust="none"):
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        return pd.DataFrame(
            {
                "date": dates,
                "open": 100 + pd.Series(range(30)).astype(float),
                "high": 101 + pd.Series(range(30)).astype(float),
                "low": 99 + pd.Series(range(30)).astype(float),
                "close": 100 + pd.Series(range(30)).astype(float),
                "volume": 1000.0,
            }
        )

    monkeypatch.setattr(dm.DataManager, "get_stock_data", mock_get_stock_data, raising=True)
    monkeypatch.setattr(
        dm.DataManager,
        "_check_qstock_status",
        lambda self: {"available": False, "connected": False, "message": "mocked"},
    )

    engine = BacktestEngine()
    engine.results_dir = tmp_path
    config = StrategyConfig(strategy_id="test", **_build_base_config())
    result = engine.run_backtest(config)

    assert "total_return" in result.performance_metrics
    assert len(result.equity_curve) > 0
    result_file = tmp_path / f"{result.backtest_id}.json"
    assert result_file.exists()


def test_backtest_engine_empty_data_raises(tmp_path, monkeypatch):
    from gui_app.backtest import data_manager as dm

    def mock_get_stock_data(self, stock_code, start_date, end_date, period="1d", adjust="none"):
        return pd.DataFrame()

    monkeypatch.setattr(dm.DataManager, "get_stock_data", mock_get_stock_data, raising=True)
    monkeypatch.setattr(
        dm.DataManager,
        "_check_qstock_status",
        lambda self: {"available": False, "connected": False, "message": "mocked"},
    )

    engine = BacktestEngine()
    engine.results_dir = tmp_path
    config = StrategyConfig(strategy_id="test", **_build_base_config())
    with pytest.raises(RuntimeError):
        engine.run_backtest(config)


def test_backtest_engine_signal_trend_buy_sell():
    engine = BacktestEngine()
    config = StrategyConfig(strategy_id="test", **_build_base_config())
    data_buy = _build_price_series([10, 10, 10, 10, 11, 12, 13, 14, 15, 16, 17, 18])
    signal, _ = engine._generate_signal(config, data_buy, {"grid_last_price": None, "stop_trading": False})
    assert signal in {"buy", "hold"}

    data_sell = _build_price_series([20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9])
    signal, _ = engine._generate_signal(config, data_sell, {"grid_last_price": None, "stop_trading": False})
    assert signal in {"sell", "hold"}


def test_backtest_engine_signal_reversion():
    engine = BacktestEngine()
    config = StrategyConfig(strategy_id="test", **_build_base_config())
    config.strategy_type = "reversion"
    config.parameters["rsi_period"] = 5
    config.parameters["rsi_lower"] = 30
    config.parameters["rsi_upper"] = 70

    data_buy = _build_price_series([10, 9, 8, 7, 6, 5, 4])
    signal, _ = engine._generate_signal(config, data_buy, {"grid_last_price": None, "stop_trading": False})
    assert signal in {"buy", "hold"}

    data_sell = _build_price_series([1, 2, 3, 4, 5, 6, 7, 8])
    signal, _ = engine._generate_signal(config, data_sell, {"grid_last_price": None, "stop_trading": False})
    assert signal in {"sell", "hold"}


def test_backtest_engine_signal_grid():
    engine = BacktestEngine()
    config = StrategyConfig(strategy_id="test", **_build_base_config())
    config.strategy_type = "grid"
    config.parameters["grid_pct"] = 0.05
    data = _build_price_series([10, 10, 10, 10, 10, 10])

    state = {"grid_last_price": None, "stop_trading": False}
    signal, state = engine._generate_signal(config, data, state)
    assert signal == "hold"
    assert state["grid_last_price"] == 10

    data_down = _build_price_series([10, 9, 9, 9, 9, 9])
    signal, state = engine._generate_signal(config, data_down, state)
    assert signal in {"buy", "hold"}

    data_up = _build_price_series([10, 11, 11, 11, 11, 11])
    signal, state = engine._generate_signal(config, data_up, state)
    assert signal in {"sell", "hold"}


def test_backtest_engine_signal_conditional_and_factor():
    engine = BacktestEngine()
    config = StrategyConfig(strategy_id="test", **_build_base_config())

    config.strategy_type = "conditional"
    config.parameters["entry_above"] = 10
    config.parameters["exit_below"] = 8
    data = _build_price_series([9, 10, 11, 12])
    signal, _ = engine._generate_signal(config, data, {"grid_last_price": None, "stop_trading": False})
    assert signal in {"buy", "hold"}

    data_down = _build_price_series([9, 8, 7])
    signal, _ = engine._generate_signal(config, data_down, {"grid_last_price": None, "stop_trading": False})
    assert signal in {"sell", "hold"}

    config.strategy_type = "factor"
    config.parameters["momentum_lookback"] = 3
    data_up = _build_price_series([10, 11, 12, 13])
    signal, _ = engine._generate_signal(config, data_up, {"grid_last_price": None, "stop_trading": False})
    assert signal in {"buy", "hold"}

    data_down = _build_price_series([13, 12, 11, 10])
    signal, _ = engine._generate_signal(config, data_down, {"grid_last_price": None, "stop_trading": False})
    assert signal in {"sell", "hold"}


def test_backtest_engine_execute_trade_branches():
    engine = BacktestEngine()
    config_data = _build_base_config()
    config_data["parameters"]["fast_period"] = 2
    config_data["parameters"]["slow_period"] = 3
    config = StrategyConfig(strategy_id="test", **config_data)
    data = pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=8, freq="D"),
            "close": [10, 10, 10, 11, 12, 11, 10, 9],
        }
    )
    result = engine._execute_backtest(data, config)
    actions = [trade["action"] for trade in result.trades]
    assert "buy" in actions
    assert "sell" in actions


def test_backtest_engine_stop_loss_trigger():
    engine = BacktestEngine()
    config_data = _build_base_config()
    config_data["parameters"]["fast_period"] = 2
    config_data["parameters"]["slow_period"] = 3
    config_data["risk_controls"] = {"max_position": 0.2, "daily_stop_loss": 0.001, "max_drawdown": 0.01}
    config = StrategyConfig(strategy_id="test", **config_data)
    data = pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=6, freq="D"),
            "close": [10, 10, 10, 11, 6, 5],
        }
    )
    result = engine._execute_backtest(data, config)
    actions = [trade["action"] for trade in result.trades]
    assert "sell" in actions


def test_strategy_creation_dialog_get_config_data(qapp):
    dialog = StrategyCreationDialog()
    dialog.name_edit.setText("策略A")
    dialog.type_combo.setCurrentText("趋势跟踪")
    dialog.base_strategy_combo.setCurrentText("MA_Cross")
    dialog.fast_period_spin.setValue(5)
    dialog.slow_period_spin.setValue(20)
    dialog.adjust_combo.setCurrentText("前复权")
    dialog.max_position_spin.setValue(0.1)
    dialog.daily_stop_loss_spin.setValue(0.02)
    dialog.max_drawdown_spin.setValue(0.1)
    dialog.symbols_edit.setPlainText("000001.SZ\n000002.SZ")
    dialog.period_combo.setCurrentText("1d")

    config = dialog.get_config_data()

    assert config["strategy_name"] == "策略A"
    assert config["strategy_type"] == "trend"
    assert config["parameters"]["adjust"] == "front"
    assert config["symbols"] == ["000001.SZ", "000002.SZ"]
    assert config["risk_controls"]["max_position"] == 0.1
    assert config["risk_controls"]["daily_stop_loss"] == 0.02
    assert config["risk_controls"]["max_drawdown"] == 0.1


def test_strategy_management_widget_loads_and_deletes(qapp, monkeypatch):
    def mock_list_strategies():
        return [
            {
                "strategy_id": "id-1",
                "strategy_name": "策略A",
                "strategy_type": "trend",
                "created_at": "2024-01-01",
                "version": 1,
                "symbols_count": 2,
            }
        ]

    monkeypatch.setattr("strategies.management.strategy_gui.strategy_manager.list_strategies", mock_list_strategies)
    widget = StrategyManagementWidget()
    widget.load_strategies()

    assert widget.strategy_table.rowCount() == 1
    assert widget.strategy_table.item(0, 0).text() == "id-1"

    monkeypatch.setattr("strategies.management.strategy_gui.strategy_manager.delete_strategy", lambda strategy_id: True)
    monkeypatch.setattr("strategies.management.strategy_gui.QMessageBox.question", lambda *args, **kwargs: QMessageBox.Yes)
    monkeypatch.setattr("strategies.management.strategy_gui.QMessageBox.information", lambda *args, **kwargs: None)
    monkeypatch.setattr("strategies.management.strategy_gui.QMessageBox.critical", lambda *args, **kwargs: None)

    widget.strategy_table.selectRow(0)
    widget.delete_strategy()
    widget.close()


def test_strategy_management_widget_create_strategy_failure(qapp, monkeypatch):
    class StubDialog:
        def __init__(self, parent=None):
            pass

        def exec_(self):
            return QDialog.Accepted

        def get_config_data(self):
            return {
                "strategy_name": "策略A",
                "strategy_type": "trend",
                "base_strategy": "MA_Cross",
                "parameters": {"fast_period": 5, "slow_period": 20, "adjust": "none"},
                "risk_controls": {"max_position": 0.2, "daily_stop_loss": 0.03, "max_drawdown": 0.15},
                "symbols": ["000001.SZ"],
                "period": "1d",
                "backtest_range": {"start": "2023-01-01", "end": "2023-01-30"},
                "trading_cost": {"commission": 0.0003, "tax": 0.001, "slippage_bps": 0},
            }

    monkeypatch.setattr("strategies.management.strategy_gui.StrategyCreationDialog", StubDialog)
    monkeypatch.setattr("strategies.management.strategy_gui.strategy_manager.create_strategy", lambda _: (_ for _ in ()).throw(ValueError("失败")))
    monkeypatch.setattr("strategies.management.strategy_gui.QMessageBox.critical", lambda *args, **kwargs: None)

    widget = StrategyManagementWidget()
    widget.create_strategy()
    widget.close()


def test_strategy_management_widget_run_backtest_failure(qapp, monkeypatch):
    def mock_list_strategies():
        return [
            {
                "strategy_id": "id-1",
                "strategy_name": "策略A",
                "strategy_type": "trend",
                "created_at": "2024-01-01",
                "version": 1,
                "symbols_count": 2,
            }
        ]

    monkeypatch.setattr("strategies.management.strategy_gui.strategy_manager.list_strategies", mock_list_strategies)
    monkeypatch.setattr("strategies.management.strategy_gui.strategy_manager.get_strategy", lambda strategy_id: StrategyConfig(strategy_id=strategy_id, **_build_base_config()))
    monkeypatch.setattr("strategies.management.strategy_gui.backtest_engine.run_backtest", lambda _: (_ for _ in ()).throw(RuntimeError("失败")))
    monkeypatch.setattr("strategies.management.strategy_gui.QMessageBox.critical", lambda *args, **kwargs: None)

    widget = StrategyManagementWidget()
    widget.load_strategies()
    widget.strategy_table.selectRow(0)
    widget.run_backtest()
    widget.close()
