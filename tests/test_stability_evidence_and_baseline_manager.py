from __future__ import annotations

import json
from argparse import Namespace

from tools import stability_evidence_board as evidence
from tools import strategy_impact_baseline_manager as bmgr
from tools.p0_trend_update import _merge_governance


def test_merge_governance_includes_strategy_impact_fields():
    row = {"ts": "2026-03-15T00:00:00Z"}
    gov = {
        "strategy_impact": {
            "available": True,
            "gate_pass": False,
            "delta": {
                "annualized_return_pct": -2.0,
                "max_drawdown_pct": 1.2,
                "sharpe_sign_changed": True,
            },
        }
    }
    out = _merge_governance(row, gov)
    assert out["strategy_impact_available"] is True
    assert out["strategy_impact_gate_pass"] is False
    assert out["strategy_impact_delta_return"] == -2.0
    assert out["strategy_impact_delta_mdd"] == 1.2
    assert out["strategy_impact_sharpe_sign_changed"] is True


def test_stability_evidence_board_peak_ready_logic():
    rows = []
    for i in range(20):
        rows.append(
            {
                "ts": f"2026-03-{i+1:02d}T00:00:00Z",
                "strict_gate_pass": True,
                "step6_hard_fail_rate": 0.01,
                "strategy_impact_available": True,
                "strategy_impact_gate_pass": True,
            }
        )
    payload = evidence.build_payload(
        rows,
        window_days=30,
        step6_hard_fail_rate_max=0.05,
        require_strategy_impact=True,
    )
    assert payload["peak_ready"] is True
    assert payload["consecutive_compliant_days"] == 20
    assert payload["compliance_ratio_pct"] == 100.0


def test_stability_evidence_board_includes_period_validation_summary():
    rows = [
        {
            "ts": "2026-03-01T00:00:00Z",
            "strict_gate_pass": True,
            "step6_hard_fail_rate": 0.01,
            "strategy_impact_available": True,
            "strategy_impact_gate_pass": True,
        }
    ]
    payload = evidence.build_payload(
        rows,
        window_days=30,
        step6_hard_fail_rate_max=0.05,
        require_strategy_impact=True,
        period_validation_summary={"report_exists": True, "rows": 12, "failed_rows": 2, "last_failed_period": "25m"},
    )
    assert payload["period_validation"]["failed_rows"] == 2
    assert payload["governance"]["session_profile_id"] == "CN_A"
    assert payload["governance"]["threshold_registry_version"] == "2026.04.01"
    md = evidence.render_md(payload)
    assert "period_validation_failed_rows" in md


def test_baseline_manager_updates_file_and_ledger(tmp_path, monkeypatch):
    baseline_path = tmp_path / "baseline.json"
    results_dir = tmp_path / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "stage1_demo.json").write_text(
        json.dumps(
            {
                "strategy": "双均线策略",
                "symbol": "000001.SZ",
                "backtest_metrics": {
                    "annualized_return_pct": 12.3,
                    "max_drawdown_pct": 8.1,
                    "sharpe_ratio": 1.3,
                },
            }
        ),
        encoding="utf-8",
    )
    ledger_path = tmp_path / "ledger.jsonl"
    monkeypatch.setattr(bmgr, "LEDGER_PATH", ledger_path)
    monkeypatch.setattr(
        bmgr.argparse.ArgumentParser,
        "parse_args",
        lambda self: Namespace(
            baseline=baseline_path,
            results_dir=results_dir,
            approval_id="OA-20260315-001",
            approver="qa@example.com",
            reason="nightly baseline update",
            from_latest_stage1=True,
            strategy="",
            symbol="",
            annualized_return_pct=0.0,
            max_drawdown_pct=0.0,
            sharpe_ratio=0.0,
        ),
    )
    rc = bmgr.main()
    assert rc == 0
    payload = json.loads(baseline_path.read_text(encoding="utf-8"))
    assert payload["strategies"][0]["strategy"] == "双均线策略"
    assert payload["_meta"]["approval_id"] == "OA-20260315-001"
    lines = ledger_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
