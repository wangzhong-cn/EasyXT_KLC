from __future__ import annotations

import json

from tools.strategy_impact_gate import evaluate_strategy_impact


def test_evaluate_strategy_impact_pass(tmp_path):
    results_dir = tmp_path / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    stage1 = {
        "strategy": "双均线策略",
        "symbol": "000001.SZ",
        "backtest_metrics": {
            "annualized_return_pct": 12.0,
            "max_drawdown_pct": 8.0,
            "sharpe_ratio": 1.2,
        },
    }
    (results_dir / "stage1_demo_2026-03-15.json").write_text(json.dumps(stage1), encoding="utf-8")
    baseline = {
        "strategies": [
            {
                "strategy": "双均线策略",
                "symbol": "000001.SZ",
                "annualized_return_pct": 10.0,
                "max_drawdown_pct": 7.2,
                "sharpe_ratio": 1.1,
            }
        ]
    }
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    out = evaluate_strategy_impact(
        baseline_path=baseline_path,
        results_dir=results_dir,
        delta_return_threshold=3.0,
        delta_mdd_threshold=1.5,
        enforce_sharpe_sign=True,
    )
    assert out["available"] is True
    assert out["gate_pass"] is True
    assert out["checks"]["delta_return_pass"] is True
    assert out["checks"]["delta_mdd_pass"] is True
    assert out["checks"]["sharpe_sign_pass"] is True


def test_evaluate_strategy_impact_fail_on_sharpe_sign_flip(tmp_path):
    results_dir = tmp_path / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    stage1 = {
        "strategy": "双均线策略",
        "symbol": "000001.SZ",
        "backtest_metrics": {
            "annualized_return_pct": 11.0,
            "max_drawdown_pct": 7.0,
            "sharpe_ratio": -0.3,
        },
    }
    (results_dir / "stage1_demo_2026-03-16.json").write_text(json.dumps(stage1), encoding="utf-8")
    baseline = {
        "strategies": [
            {
                "strategy": "双均线策略",
                "symbol": "000001.SZ",
                "annualized_return_pct": 10.8,
                "max_drawdown_pct": 6.8,
                "sharpe_ratio": 0.6,
            }
        ]
    }
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    out = evaluate_strategy_impact(
        baseline_path=baseline_path,
        results_dir=results_dir,
        delta_return_threshold=3.0,
        delta_mdd_threshold=1.5,
        enforce_sharpe_sign=True,
    )
    assert out["available"] is True
    assert out["gate_pass"] is False
    assert out["checks"]["sharpe_sign_pass"] is False
