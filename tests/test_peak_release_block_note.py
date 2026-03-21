from __future__ import annotations

from tools.peak_release_block_note import build_note, render_md


def test_build_note_from_peak_gate_and_evidence():
    note = build_note(
        {
            "level": "fail",
            "release_env": "prod",
            "consecutive_compliant_days": 6,
            "compliance_ratio_pct": 81.5,
            "warn_consecutive_days": 7,
            "fail_consecutive_days": 14,
            "gap_to_fail_days": 8,
            "peak_ready": False,
            "period_validation_failed_items": 3,
            "max_period_validation_failed_items": 0,
            "period_validation_gate_pass": False,
        },
        {"consecutive_compliant_days": 6, "compliance_ratio_pct": 81.5, "peak_ready": False},
        {"window_days": 7, "records": 7, "gate_pass_days": 5},
    )
    assert note["level"] == "fail"
    assert note["release_env"] == "prod"
    assert note["gap_to_fail_days"] == 8
    assert note["period_validation_gate_pass"] is False
    assert note["recent_summary"]["records"] == 7
    assert "阻断生产放量" in note["action"]


def test_render_md_contains_key_fields():
    md = render_md(
        {
            "title": "峰值发布门禁说明（WARN）",
            "generated_at": "2026-03-15T00:00:00Z",
            "level": "warn",
            "release_env": "preprod",
            "peak_ready": False,
            "consecutive_compliant_days": 10,
            "compliance_ratio_pct": 93.2,
            "warn_consecutive_days": 7,
            "fail_consecutive_days": 14,
            "gap_to_fail_days": 4,
            "period_validation_failed_items": 0,
            "max_period_validation_failed_items": 0,
            "period_validation_gate_pass": True,
            "recent_summary": {
                "window_days": 7,
                "records": 7,
                "gate_pass_days": 6,
                "step6_hard_fail_rate_avg": 0.01,
                "step6_hard_fail_rate_max": 0.03,
                "strategy_impact_pass_days": 6,
            },
            "action": "维持灰度发布",
        }
    )
    assert "峰值发布门禁说明（WARN）" in md
    assert "release_env" in md
    assert "preprod" in md
    assert "gap_to_fail_days" in md
    assert "period_validation_gate_pass" in md
    assert "最近证据摘要" in md
    assert "strategy_impact_pass_days" in md
