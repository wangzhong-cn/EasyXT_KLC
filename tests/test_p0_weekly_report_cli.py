from __future__ import annotations

import json
import pathlib

from tools import p0_weekly_report as weekly


def test_main_supports_custom_strategy_impact_path(tmp_path, monkeypatch):
    history_path = tmp_path / "history.json"
    history_path.write_text(
        json.dumps(
            [
                {
                    "ts": "2026-03-15T00:00:00Z",
                    "strict_gate_pass": True,
                    "P0_open_count": 0,
                    "active_critical_high": 0,
                }
            ]
        ),
        encoding="utf-8",
    )
    impact_path = tmp_path / "impact.json"
    impact_path.write_text(
        json.dumps(
            {
                "available": True,
                "gate_pass": False,
                "delta": {
                    "annualized_return_pct": -3.2,
                    "max_drawdown_pct": 1.9,
                    "sharpe_sign_changed": True,
                },
            }
        ),
        encoding="utf-8",
    )
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "peak_ready": False,
                "consecutive_compliant_days": 10,
            }
        ),
        encoding="utf-8",
    )
    peak_gate_path = tmp_path / "peak_gate.json"
    peak_gate_path.write_text(
        json.dumps(
            {
                "level": "warn",
                "gap_to_fail_days": 4,
            }
        ),
        encoding="utf-8",
    )
    out_path = tmp_path / "weekly.md"
    monkeypatch.setattr(weekly, "REPORT_DIR", pathlib.Path(tmp_path))
    monkeypatch.setattr(weekly, "TOUCH_EVENTS_PATH", pathlib.Path(tmp_path / "touch.json"))
    rc = weekly.main(
        argv=[
            "--out", str(out_path),
            "--window-days", "7",
            "--history", str(history_path),
            "--strategy-impact", str(impact_path),
            "--stability-evidence", str(evidence_path),
            "--peak-release-gate", str(peak_gate_path),
        ]
    )
    assert rc == 0
    text = out_path.read_text(encoding="utf-8")
    assert "策略影响门禁 gate_pass" in text
    assert "❌ FAIL" in text
    assert "距峰值还差 4 天" in text
    assert "峰值发布门禁(SSOT)" in text
