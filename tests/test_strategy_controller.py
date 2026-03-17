"""StrategyController 单元测试。"""

import pytest
from unittest.mock import patch, MagicMock
from strategies.strategy_controller import StrategyController


class TestStrategyControllerInit:
    """初始化与属性延迟加载。"""

    def test_defaults(self):
        ctrl = StrategyController()
        assert ctrl._config_dir == "strategies/configs"
        assert ctrl._enable_risk is True
        assert ctrl._enable_audit is False
        assert ctrl._manager is None
        assert ctrl._registry is None

    def test_custom_params(self):
        ctrl = StrategyController(
            config_dir="/tmp/cfgs",
            duckdb_path="/tmp/db",
            enable_risk_engine=False,
            enable_audit_trail=True,
            risk_config={"max_position": 0.1},
        )
        assert ctrl._config_dir == "/tmp/cfgs"
        assert ctrl._duckdb_path == "/tmp/db"
        assert ctrl._enable_risk is False
        assert ctrl._enable_audit is True
        assert ctrl._risk_config == {"max_position": 0.1}

    def test_lazy_manager(self):
        ctrl = StrategyController()
        mgr = ctrl.manager
        from strategies.management.strategy_manager import StrategyManager
        assert isinstance(mgr, StrategyManager)
        assert ctrl.manager is mgr  # 缓存

    def test_lazy_registry(self):
        ctrl = StrategyController()
        reg = ctrl.registry
        from strategies.registry import StrategyRegistry
        assert isinstance(reg, StrategyRegistry)
        assert ctrl.registry is reg  # 缓存


class TestRunBacktest:
    """run_backtest 方法 —— 从 StrategyManager 加载配置。"""

    def _make_ctrl(self):
        ctrl = StrategyController()
        ctrl._manager = MagicMock()
        ctrl._registry = MagicMock()
        return ctrl

    def test_strategy_not_found(self):
        ctrl = self._make_ctrl()
        ctrl._manager.get_strategy.return_value = None
        with pytest.raises(ValueError, match="策略.*不存在"):
            ctrl.run_backtest("missing", ["000001.SZ"], "2024-01-01", "2024-12-31")

    @patch("strategies.strategy_factory.create_strategy_from_config")
    def test_happy_path(self, mock_factory):
        ctrl = self._make_ctrl()
        cfg = MagicMock()
        cfg.strategy_type = "trend"
        cfg.strategy_id = "test-1"
        cfg.parameters = {"short_period": 5}
        cfg.risk_controls = {"max_position": 0.2}
        cfg.symbols = ["000001.SZ"]
        cfg.period = "1d"
        ctrl._manager.get_strategy.return_value = cfg

        mock_strategy = MagicMock()
        mock_factory.return_value = mock_strategy
        ctrl._enable_risk = False
        ctrl._enable_audit = False

        with patch("easyxt_backtest.strategy_runner.StrategyRunner") as MockRunner, \
             patch("easyxt_backtest.engine.BacktestConfig"):
            mock_runner_inst = MagicMock()
            mock_runner_inst.run.return_value = {"equity": [100]}
            MockRunner.return_value = mock_runner_inst

            result = ctrl.run_backtest("test-1", ["000001.SZ"], "2024-01-01", "2024-12-31")

        assert result == {"equity": [100]}
        ctrl._registry.register.assert_called_once()
        ctrl._registry.unregister.assert_called_once_with("test-1", status="stopped")

    @patch("strategies.strategy_factory.create_strategy_from_config")
    def test_param_overrides(self, mock_factory):
        ctrl = self._make_ctrl()
        cfg = MagicMock()
        cfg.strategy_type = "trend"
        cfg.strategy_id = "test-2"
        cfg.parameters = {"short_period": 5, "long_period": 20}
        cfg.risk_controls = {}
        cfg.symbols = ["000001.SZ"]
        cfg.period = "1d"
        ctrl._manager.get_strategy.return_value = cfg

        mock_factory.return_value = MagicMock()

        with patch("easyxt_backtest.strategy_runner.StrategyRunner") as MockRunner, \
             patch("easyxt_backtest.engine.BacktestConfig"):
            MockRunner.return_value.run.return_value = {}
            ctrl.run_backtest("test-2", ["000001.SZ"], "2024-01-01", "2024-12-31",
                              param_overrides={"short_period": 10})

        # 验证工厂收到合并后的参数
        call_cfg = mock_factory.call_args[0][0]
        assert call_cfg.parameters == {"short_period": 10, "long_period": 20}


class TestRunBacktestByType:
    """run_backtest_by_type —— 直接按类型。"""

    @patch("strategies.strategy_factory.create_strategy_from_config")
    def test_auto_id(self, mock_factory):
        ctrl = StrategyController()
        ctrl._registry = MagicMock()
        mock_factory.return_value = MagicMock()

        with patch("easyxt_backtest.strategy_runner.StrategyRunner") as MockRunner, \
             patch("easyxt_backtest.engine.BacktestConfig"):
            MockRunner.return_value.run.return_value = {}
            ctrl.run_backtest_by_type(
                "reversion", ["000001.SZ"], "2024-01-01", "2024-12-31",
            )

        reg_call = ctrl._registry.register.call_args
        sid = reg_call[1]["strategy_id"]
        assert sid.startswith("reversion_")

    @patch("strategies.strategy_factory.create_strategy_from_config")
    def test_error_marks_error(self, mock_factory):
        ctrl = StrategyController()
        ctrl._registry = MagicMock()
        mock_factory.return_value = MagicMock()

        with patch("easyxt_backtest.strategy_runner.StrategyRunner") as MockRunner, \
             patch("easyxt_backtest.engine.BacktestConfig"):
            MockRunner.return_value.run.side_effect = RuntimeError("boom")
            with pytest.raises(RuntimeError, match="boom"):
                ctrl.run_backtest_by_type(
                    "trend", ["000001.SZ"], "2024-01-01", "2024-12-31",
                    strategy_id="fail-1",
                )

        ctrl._registry.unregister.assert_called_once_with("fail-1", status="error")


class TestListAPIs:
    """list_strategies / list_running。"""

    def test_list_strategies_delegates(self):
        ctrl = StrategyController()
        ctrl._manager = MagicMock()
        ctrl._manager.list_strategies.return_value = [{"id": "a"}]
        assert ctrl.list_strategies() == [{"id": "a"}]

    def test_list_running(self):
        ctrl = StrategyController()
        ctrl._registry = MagicMock()
        info = MagicMock()
        info.strategy_id = "r1"
        info.account_id = "acc"
        info.status = "running"
        info.tags = ["trend"]
        ctrl._registry.list_running.return_value = [info]
        result = ctrl.list_running()
        assert len(result) == 1
        assert result[0]["strategy_id"] == "r1"


class TestValidate:
    """Stage1 验收集成。"""

    @patch("strategies.stage1_pipeline.Stage1Runner")
    def test_validate_delegates(self, MockStage1):
        mock_result = MagicMock()
        mock_result.stage1_pass = True
        MockStage1.return_value.run.return_value = mock_result

        ctrl = StrategyController()
        result = ctrl.validate(
            strategy_name="DualMA",
            symbol="000001.SZ",
            start_date="2023-01-01",
            end_date="2024-12-31",
            oos_split="2024-06-01",
        )

        assert result.stage1_pass is True
        MockStage1.assert_called_once_with(
            strategy="DualMA",
            symbol="000001.SZ",
            start="2023-01-01",
            end="2024-12-31",
            oos_split="2024-06-01",
            short_period=5,
            long_period=20,
            benchmark="CSI300",
            dry_run=False,
        )
