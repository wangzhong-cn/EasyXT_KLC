from __future__ import annotations

import json
import pathlib
from datetime import datetime, timedelta, timezone

from tools import p0_weekly_report as weekly


def test_main_supports_custom_strategy_impact_path(tmp_path, monkeypatch):
    _recent_ts = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    history_path = tmp_path / "history.json"
    history_path.write_text(
        json.dumps(
            [
                {
                    "ts": _recent_ts,
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
                "period_validation_failed_items": 1,
                "max_period_validation_failed_items": 0,
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
    assert "主表头RAG评分" in text
    assert "PV[🟡 WARN（1>0）]" in text
    assert "RAG[🟡 YELLOW]" in text
    assert "主表头门禁契约" in text
    assert "主表头契约健康" in text
    assert "GATE_DETAIL[" in text
    assert "HEALTHY" in text
