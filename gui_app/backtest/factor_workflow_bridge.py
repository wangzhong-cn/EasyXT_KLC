from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any


def _resolve_factor_platform_root(explicit_root: str | None = None) -> str:
    if explicit_root:
        return explicit_root
    root = Path(__file__).resolve().parents[2]
    target = root / "101因子" / "101因子分析平台"
    return str(target)


def run_factor_workflow_backtest(
    workflow_path: str,
    symbols: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    node_overrides: dict[str, dict[str, Any]] | None = None,
    platform_root: str | None = None,
) -> dict[str, Any]:
    factor_root = _resolve_factor_platform_root(platform_root)
    src_root = os.path.join(factor_root, "src")
    if src_root not in sys.path:
        sys.path.insert(0, src_root)
    from workflow.engine import WorkflowEngine

    if not workflow_path:
        workflow_path = os.path.join(factor_root, "workflow.json")
    engine = WorkflowEngine()
    workflow_meta = engine.load_workflow(workflow_path) or {}
    override_map = node_overrides or {}
    for node in engine.nodes.values():
        if node.node_type != "data_loader":
            node.params.update(override_map.get(str(getattr(node, "node_type", "")), {}))
            continue
        if symbols:
            node.params["symbols"] = list(symbols)
        if start_date:
            node.params["start_date"] = start_date
        if end_date:
            node.params["end_date"] = end_date
        node.params.update(override_map.get("data_loader", {}))
    results = engine.execute_workflow()
    backtest_result = {}
    perf_result = {}
    ic_result = {}
    workflow_nodes: list[dict[str, Any]] = []
    for node_id in engine.execution_order:
        node = engine.nodes.get(node_id)
        if not node:
            continue
        workflow_nodes.append(
            {
                "id": str(node_id),
                "type": str(getattr(node, "node_type", "")),
                "params": dict(getattr(node, "params", {}) or {}),
            }
        )
        node_result = results.get(node_id)
        if node.node_type == "backtester" and isinstance(node_result, dict):
            backtest_result = node_result
        if node.node_type == "performance_analyzer" and isinstance(node_result, dict):
            perf_result = node_result
        if node.node_type == "ic_analyzer" and isinstance(node_result, dict):
            ic_result = node_result
    ls = backtest_result.get("long_short_results", {}) if isinstance(backtest_result, dict) else {}
    summary = backtest_result.get("summary", {}) if isinstance(backtest_result, dict) else {}
    perf_metrics = perf_result.get("metrics", {}) if isinstance(perf_result, dict) else {}
    ic_summary = ic_result.get("summary", {}) if isinstance(ic_result, dict) else {}
    metrics = {
        "total_return": float(ls.get("total_return", perf_metrics.get("total_return", 0.0)) or 0.0),
        "annualized_return": float(ls.get("annual_return", perf_metrics.get("annual_return", 0.0)) or 0.0),
        "sharpe_ratio": float(ls.get("sharpe_ratio", perf_metrics.get("sharpe_ratio", 0.0)) or 0.0),
        "max_drawdown": abs(float(ls.get("max_drawdown", perf_metrics.get("max_drawdown", 0.0)) or 0.0)),
        "win_rate": float(ls.get("win_rate", perf_metrics.get("win_rate", 0.0)) or 0.0),
        "long_short_spread": float(ls.get("long_short_spread", 0.0) or 0.0),
        "ic_mean": float(ic_summary.get("ic_mean", 0.0) or 0.0),
        "ic_ir": float(ic_summary.get("ic_ir", 0.0) or 0.0),
    }
    return {
        "metrics": metrics,
        "detailed": {
            "backtest_summary": summary,
            "long_short_results": ls,
            "quantile_results": backtest_result.get("quantile_results", {}) if isinstance(backtest_result, dict) else {},
            "performance": perf_metrics,
            "ic_summary": ic_summary,
            "workflow": {"meta": workflow_meta, "nodes": workflow_nodes},
        },
        "factor_context": {
            "workflow_meta": workflow_meta,
            "workflow_nodes": workflow_nodes,
            "ic_summary": ic_summary,
        },
    }
