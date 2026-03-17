import types

from gui_app.backtest.factor_workflow_bridge import run_factor_workflow_backtest


class _FakeNode:
    def __init__(self, node_type, params=None):
        self.node_type = node_type
        self.params = params or {}


class _FakeWorkflowEngine:
    def __init__(self):
        self.nodes = {
            "n1": _FakeNode("data_loader", {"symbols": ["000001.SZ"]}),
            "n2": _FakeNode("ic_analyzer", {"periods": 1}),
            "n3": _FakeNode("backtester", {"top_quantile": 0.2}),
            "n4": _FakeNode("performance_analyzer", {}),
        }
        self.execution_order = ["n1", "n2", "n3", "n4"]

    def load_workflow(self, filepath):
        return {"name": "demo", "node_count": 4, "connection_count": 3, "path": filepath}

    def execute_workflow(self):
        return {
            "n2": {"summary": {"ic_mean": 0.03, "ic_ir": 0.9, "ic_prob": 0.62}},
            "n3": {
                "long_short_results": {
                    "total_return": 0.18,
                    "annual_return": 0.12,
                    "sharpe_ratio": 1.6,
                    "max_drawdown": -0.08,
                    "win_rate": 0.56,
                    "long_short_spread": 0.002,
                },
                "quantile_results": {"Q1": {}, "Q5": {}},
                "summary": {"long_short": {"annual_return": 0.12}},
            },
            "n4": {"metrics": {"annual_return": 0.11, "sharpe_ratio": 1.4}},
        }


def test_run_factor_workflow_backtest_extracts_context(monkeypatch):
    fake_module = types.ModuleType("workflow.engine")
    fake_module.WorkflowEngine = _FakeWorkflowEngine
    monkeypatch.setitem(__import__("sys").modules, "workflow.engine", fake_module)
    result = run_factor_workflow_backtest(
        workflow_path="dummy.json",
        symbols=["000002.SZ"],
        start_date="2025-01-01",
        end_date="2025-12-31",
        node_overrides={"backtester": {"n_quantiles": 7}},
    )
    assert result["metrics"]["annualized_return"] == 0.12
    assert result["metrics"]["ic_mean"] == 0.03
    assert result["metrics"]["ic_ir"] == 0.9
    fc = result.get("factor_context", {})
    assert fc.get("workflow_meta", {}).get("name") == "demo"
    assert len(fc.get("workflow_nodes", [])) == 4
    nodes = fc.get("workflow_nodes", [])
    backtester = [n for n in nodes if n.get("type") == "backtester"]
    assert backtester
    assert backtester[0].get("params", {}).get("n_quantiles") == 7
