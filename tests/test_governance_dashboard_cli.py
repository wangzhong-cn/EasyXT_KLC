from __future__ import annotations

import json
import pathlib

from tools import governance_strategy_dashboard as dashboard


def test_main_supports_custom_strategy_impact_path(tmp_path, monkeypatch):
    impact_path = tmp_path / "impact.json"
    impact_path.write_text(
        json.dumps(
            {
                "available": True,
                "gate_pass": False,
                "delta": {
                    "annualized_return_pct": -2.4,
                    "max_drawdown_pct": 1.6,
                    "sharpe_sign_changed": True,
                },
                "checks": {
                    "delta_return_pass": True,
                    "delta_mdd_pass": False,
                    "sharpe_sign_pass": False,
                },
            }
        ),
        encoding="utf-8",
    )
    out_path = tmp_path / "dashboard.md"
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "peak_ready": False,
                "consecutive_compliant_days": 11,
            }
        ),
        encoding="utf-8",
    )
    peak_gate_path = tmp_path / "peak_gate.json"
    peak_gate_path.write_text(
        json.dumps(
            {
                "level": "warn",
                "gap_to_fail_days": 3,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard, "REPORT_DIR", pathlib.Path(tmp_path))
    rc = dashboard.main(
        argv=[
            "--out", str(out_path),
            "--strategy-impact", str(impact_path),
            "--stability-evidence", str(evidence_path),
            "--peak-release-gate", str(peak_gate_path),
        ]
    )
    assert rc == 0
    text = out_path.read_text(encoding="utf-8")
    assert "联动分析 4：策略影响门禁" in text
    assert "❌ FAIL" in text
    assert "peak_release_gate(SSOT)" in text
