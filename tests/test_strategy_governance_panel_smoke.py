"""
tests/test_strategy_governance_panel_smoke.py
=============================================

StrategyGovernancePanel + StrategyController 无头 smoke 测试。
使用 offscreen Qt 平台（QT_QPA_PLATFORM=offscreen），不需要真实显示器。
"""
from __future__ import annotations

import os
import warnings
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ─── StrategyController（纯 Python，无需 qapp） ───────────────────────────


class TestStrategyControllerSmoke:
    """在没有任何外部服务的情况下，Controller 所有方法都应可调用且不抛出异常。"""

    def setup_method(self):
        os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
        from gui_app.strategy_controller import StrategyController
        self.ctrl = StrategyController(
            strategy_manager=None,
            backtest_engine=None,
        )

    def test_import_succeeds(self):
        from gui_app.strategy_controller import StrategyController  # noqa
        assert StrategyController is not None

    def test_get_all_strategies_no_crash(self):
        result = self.ctrl.get_all_strategies()
        assert isinstance(result, list)

    def test_get_strategy_missing_returns_none(self):
        result = self.ctrl.get_strategy("nonexistent_id")
        assert result is None

    def test_create_strategy_no_manager_returns_error(self):
        result = self.ctrl.create_strategy({"strategy_name": "test"})
        assert isinstance(result, dict)
        assert result.get("ok") is False
        assert "error" in result

    def test_delete_strategy_no_manager_returns_error(self):
        result = self.ctrl.delete_strategy("nonexistent_id")
        assert isinstance(result, dict)
        assert result.get("ok") is False

    def test_run_backtest_no_manager_returns_error(self):
        result = self.ctrl.run_backtest("nonexistent_id")
        assert isinstance(result, dict)
        assert result.get("ok") is False

    def test_get_backtest_history_no_results_dir(self):
        result = self.ctrl.get_backtest_history("some_strategy")
        assert isinstance(result, list)

    def test_get_performance_summary_all_zeros(self):
        summary = self.ctrl.get_performance_summary({
            "total_return": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "win_rate": 0.0,
            "calmar_ratio": 0.0,
        })
        assert isinstance(summary, list)
        labels = [s["label"] for s in summary]
        assert "夏普比率" in labels
        assert "最大回撤" in labels
        assert "胜率" in labels

    def test_get_performance_summary_with_values(self):
        summary = self.ctrl.get_performance_summary({
            "total_return": 0.35,
            "sharpe_ratio": 1.8,
            "max_drawdown": 0.08,
            "win_rate": 0.55,
            "calmar_ratio": 1.2,
            "cagr": 0.18,
            "trade_count": 47,
            "profit_loss_ratio": 1.6,
        })
        assert len(summary) >= 6
        # 验证夏普比率颜色标注（>1 = green）
        sharpe_entry = next((s for s in summary if s["label"] == "夏普比率"), None)
        assert sharpe_entry is not None
        assert "#4CAF50" in sharpe_entry["color"]

    def test_strategy_type_options_returns_list(self):
        from gui_app.strategy_controller import StrategyController
        opts = StrategyController.strategy_type_options()
        assert len(opts) >= 6
        labels = [o[0] for o in opts]
        assert "趋势跟踪" in labels
        assert "均值回归" in labels

    def test_period_options_returns_list(self):
        from gui_app.strategy_controller import StrategyController
        opts = StrategyController.period_options()
        assert "1d" in opts

    def test_base_strategy_options_returns_list(self):
        from gui_app.strategy_controller import StrategyController
        opts = StrategyController.base_strategy_options()
        assert isinstance(opts, list)
        assert len(opts) >= 3

    def test_extract_equity_curve_empty(self):
        result = self.ctrl._extract_equity_curve(MagicMock(equity_curve=None, performance_metrics={}))
        assert isinstance(result, dict)
        assert "dates" in result
        assert "values" in result

    def test_extract_trades_empty(self):
        mock_result = MagicMock()
        mock_result.trades = []
        result = self.ctrl._extract_trades(mock_result)
        assert isinstance(result, list)

    def test_extract_trades_dicts(self):
        mock_result = MagicMock()
        mock_result.trades = [
            {"date": "2023-01-01", "action": "买入", "price": 10.0, "volume": 100, "value": 1000, "pnl": 0},
            {"date": "2023-02-01", "action": "卖出", "price": 11.0, "volume": 100, "value": 1100, "pnl": 100},
        ]
        result = self.ctrl._extract_trades(mock_result)
        assert len(result) == 2
        assert result[0]["action"] == "买入"

    def test_controller_with_mock_manager(self):
        """注入 mock 管理器，验证 CRUD 流程。"""
        mock_mgr = MagicMock()
        mock_mgr.list_strategies.return_value = [
            {"strategy_id": "s1", "strategy_name": "测试策略", "strategy_type": "trend",
             "period": "1d", "symbols_count": 2, "version": 1, "created_at": "2024-01-01"}
        ]
        mock_mgr.get_strategy.return_value = MagicMock(strategy_name="测试策略")
        mock_mgr.create_strategy.return_value = "s_new"
        mock_mgr.delete_strategy.return_value = True

        ctrl = self._make_ctrl(strategy_manager=mock_mgr)
        strategies = ctrl.get_all_strategies()
        assert len(strategies) == 1
        assert strategies[0]["strategy_name"] == "测试策略"

        res = ctrl.create_strategy({"strategy_name": "新策略"})
        assert res["ok"] is True
        assert res["strategy_id"] == "s_new"

        res = ctrl.delete_strategy("s1")
        assert res["ok"] is True

    def _make_ctrl(self, **kwargs):
        from gui_app.strategy_controller import StrategyController
        return StrategyController(**kwargs)


# ─── StrategyGovernancePanel Qt smoke（需要 qapp fixture） ────────────────


class TestStrategyGovernancePanelSmoke:
    """验证面板可在 offscreen 环境正常实例化，8 个 Tab 都存在。"""

    def test_import_succeeds(self):
        from gui_app.widgets.strategy_governance_panel import StrategyGovernancePanel  # noqa
        assert StrategyGovernancePanel is not None

    def test_import_tab_classes(self):
        from gui_app.widgets.strategy_governance_panel import (  # noqa
            _BacktestConfigTab,
            _BacktestResultTab,
            _BacktestThread,
            _ComparisonTab,
            _EquityChart,
            _LifecycleTab,
            _MetricCard,
            _OptimizationTab,
            _PerformanceTab,
            _RiskTab,
            _StrategyCreationDialog,
            _StrategyListTab,
        )

    def test_panel_instantiates_8_tabs(self, qapp):
        """面板应包含 8 个功能 Tab。"""
        from gui_app.widgets.strategy_governance_panel import StrategyGovernancePanel
        mock_ctrl = MagicMock()
        mock_ctrl.get_all_strategies.return_value = []
        # patch _init_controller 以注入 mock
        with patch.object(StrategyGovernancePanel, "_init_controller", lambda self: setattr(self, "_ctrl", mock_ctrl)):
            panel = StrategyGovernancePanel()
        assert panel._tabs.count() == 8

    def test_strategy_list_tab_instantiates(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _StrategyListTab
        mock_ctrl = MagicMock()
        mock_ctrl.get_all_strategies.return_value = []
        tab = _StrategyListTab(mock_ctrl)
        assert tab._table is not None

    def test_backtest_config_tab_instantiates(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _BacktestConfigTab
        mock_ctrl = MagicMock()
        tab = _BacktestConfigTab(mock_ctrl)
        assert tab._config_text is not None

    def test_backtest_result_tab_instantiates(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _BacktestResultTab
        mock_ctrl = MagicMock()
        mock_ctrl.get_performance_summary.return_value = []
        tab = _BacktestResultTab(mock_ctrl)
        assert tab._trade_table is not None

    def test_performance_tab_instantiates(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _PerformanceTab
        mock_ctrl = MagicMock()
        mock_ctrl.get_performance_summary.return_value = []
        tab = _PerformanceTab(mock_ctrl)
        assert tab._monthly_table is not None

    def test_risk_tab_instantiates(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _RiskTab
        tab = _RiskTab()
        assert tab._table is not None

    def test_optimization_tab_instantiates(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _OptimizationTab
        tab = _OptimizationTab()
        assert tab._table is not None

    def test_comparison_tab_instantiates(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _ComparisonTab
        mock_ctrl = MagicMock()
        tab = _ComparisonTab(mock_ctrl)
        assert tab._table is not None

    def test_lifecycle_tab_instantiates(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _LifecycleTab
        mock_ctrl = MagicMock()
        tab = _LifecycleTab(mock_ctrl)
        assert tab._history_table is not None

    def test_metric_card_instantiates(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _MetricCard
        card = _MetricCard("夏普比率", "1.56", "#4CAF50")
        assert card._label.text() == "夏普比率"
        assert card._value.text() == "1.56"

    def test_metric_card_update(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _MetricCard
        card = _MetricCard("最大回撤", "5.2%", "#F44336")
        card.update_metric("8.3%", "#FF9800")
        assert card._value.text() == "8.3%"

    def test_equity_chart_instantiates(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _EquityChart
        chart = _EquityChart()
        assert chart is not None

    def test_equity_chart_plot_empty(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _EquityChart
        chart = _EquityChart()
        chart.plot([], [], title="测试")  # 应不崩溃

    def test_equity_chart_plot_with_data(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _EquityChart
        chart = _EquityChart()
        dates = ["2023-01-01", "2023-06-01", "2023-12-31"]
        values = [1_000_000, 1_100_000, 1_250_000]
        chart.plot(dates, values, title="资金曲线")  # 应不崩溃

    def test_equity_chart_plot_with_data_has_no_glyph_warning(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _EquityChart
        chart = _EquityChart()
        dates = ["2023-01-01", "2023-06-01", "2023-12-31"]
        values = [1_000_000, 1_100_000, 1_250_000]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            chart.plot(dates, values, title="资金曲线")
        assert not any("Glyph" in str(w.message) for w in caught)

    def test_backtest_result_tab_load_result(self, qapp):
        """load_result 应完整渲染而不崩溃。"""
        from gui_app.widgets.strategy_governance_panel import _BacktestResultTab
        mock_ctrl = MagicMock()
        mock_ctrl.get_performance_summary.return_value = [
            {"label": "总收益率", "value": "35.00%", "color": "#4CAF50", "key": "total_return"},
            {"label": "夏普比率", "value": "1.80", "color": "#4CAF50", "key": "sharpe_ratio"},
        ]
        tab = _BacktestResultTab(mock_ctrl)
        tab.load_result({
            "ok": True,
            "backtest_id": "bt_001",
            "equity_curve": {
                "dates": ["2023-01-01", "2023-12-31"],
                "values": [1_000_000, 1_350_000],
            },
            "performance_metrics": {"total_return": 0.35, "sharpe_ratio": 1.8},
            "trades": [
                {"date": "2023-02-01", "action": "买入", "price": 10.5, "volume": 100, "value": 1050, "pnl": 0},
                {"date": "2023-08-01", "action": "卖出", "price": 12.0, "volume": 100, "value": 1200, "pnl": 150},
            ],
        })
        assert tab._trade_table.rowCount() == 2

    def test_performance_tab_load_metrics(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _PerformanceTab
        mock_ctrl = MagicMock()
        mock_ctrl.get_performance_summary.return_value = [
            {"label": "夏普比率", "value": "1.60", "color": "#4CAF50", "key": "sharpe_ratio"},
        ]
        tab = _PerformanceTab(mock_ctrl)
        tab.load_metrics({"sharpe_ratio": 1.60})
        # 应不崩溃

    def test_risk_tab_load_risk(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _RiskTab
        tab = _RiskTab()
        tab.load_risk({
            "max_drawdown": 0.08,
            "sharpe_ratio": 1.5,
            "win_rate": 0.55,
            "calmar_ratio": 0.9,
        })
        assert tab._table.rowCount() > 0

    def test_optimization_tab_load(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _OptimizationTab
        tab = _OptimizationTab()
        tab.load_optimization([
            {"params": {"fast": 5, "slow": 20}, "total_return": 0.25, "sharpe_ratio": 1.2, "max_drawdown": 0.10, "score": 0.30},
            {"params": {"fast": 8, "slow": 30}, "total_return": 0.18, "sharpe_ratio": 0.9, "max_drawdown": 0.12, "score": 0.16},
        ])
        assert tab._table.rowCount() == 2

    def test_comparison_tab_add_result(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _ComparisonTab
        mock_ctrl = MagicMock()
        tab = _ComparisonTab(mock_ctrl)
        tab.add_result("策略A", {
            "backtest_id": "bt001",
            "performance_metrics": {"total_return": 0.30, "sharpe_ratio": 1.5, "max_drawdown": 0.10, "win_rate": 0.55, "cagr": 0.15},
        })
        tab.add_result("策略B", {
            "backtest_id": "bt002",
            "performance_metrics": {"total_return": 0.20, "sharpe_ratio": 1.2, "max_drawdown": 0.12, "win_rate": 0.50, "cagr": 0.10},
        })
        assert tab._table.rowCount() == 2

    def test_lifecycle_tab_load_history_empty(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _LifecycleTab
        mock_ctrl = MagicMock()
        mock_ctrl.get_backtest_history.return_value = []
        tab = _LifecycleTab(mock_ctrl)
        tab.load_history("strategy_001")
        assert tab._history_table.rowCount() == 0

    def test_lifecycle_tab_load_history_with_data(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _LifecycleTab
        mock_ctrl = MagicMock()
        mock_ctrl.get_backtest_history.return_value = [
            {"backtest_id": "bt001", "created_at": "2024-01-01 10:00:00", "total_return": 0.25, "sharpe_ratio": 1.3, "max_drawdown": 0.09},
            {"backtest_id": "bt002", "created_at": "2024-02-01 10:00:00", "total_return": 0.31, "sharpe_ratio": 1.5, "max_drawdown": 0.07},
        ]
        tab = _LifecycleTab(mock_ctrl)
        tab.load_history("strategy_001")
        assert tab._history_table.rowCount() == 2

    def test_strategy_list_tab_refresh_with_data(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _StrategyListTab
        mock_ctrl = MagicMock()
        mock_ctrl.get_all_strategies.return_value = [
            {
                "strategy_id": "sid1",
                "strategy_name": "测试均线策略",
                "strategy_type": "trend",
                "period": "1d",
                "symbols_count": 3,
                "version": 2,
                "created_at": "2024-01-15T10:30:00",
            }
        ]
        tab = _StrategyListTab(mock_ctrl)
        assert tab._table.rowCount() == 1
        assert tab._table.item(0, 1).text() == "测试均线策略"

    def test_backtest_thread_instantiates(self, qapp):
        from gui_app.widgets.strategy_governance_panel import _BacktestThread
        mock_ctrl = MagicMock()
        thread = _BacktestThread(mock_ctrl, "s1")
        assert thread._strategy_id == "s1"

    def test_backtest_thread_error_on_manager_unavailable(self, qapp):
        """回测线程应通过 error_occurred 信号传递错误，不直接抛出。"""
        from gui_app.widgets.strategy_governance_panel import _BacktestThread
        mock_ctrl = MagicMock()
        mock_ctrl.run_backtest.return_value = {"ok": False, "error": "策略管理器不可用"}
        errors = []
        results = []
        thread = _BacktestThread(mock_ctrl, "s1")
        thread.error_occurred.connect(errors.append)
        thread.result_ready.connect(results.append)
        thread.run()
        # 返回结果（ok=False）
        assert len(results) == 1
        assert results[0].get("ok") is False

    def test_strategy_module_updated_factory(self):
        """strategy_module 应指向 StrategyGovernancePanel 而非旧的 StrategyManagementWidget。"""
        from gui_app.widgets.modules.strategy_module import StrategyModule
        module = StrategyModule.__new__(StrategyModule)
        # 手动初始化工厂列表检查
        module._factories = None  # 触发 __new__ 不调用 __init__
        # 重新读取工厂配置
        import importlib
        src = importlib.import_module("gui_app.widgets.modules.strategy_module")
        content = open(src.__file__, encoding="utf-8").read()
        assert "StrategyGovernancePanel" in content
        assert "strategy_governance_panel" in content
