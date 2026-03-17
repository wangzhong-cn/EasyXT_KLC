from __future__ import annotations

from tools.p0_trend_update import _make_row, _merge_governance


def test_merge_governance_attaches_step6_and_canary():
    row = _make_row(
        {
            "strict_gate_pass": True,
            "P0_open_count": 0,
            "active_critical_high": 0,
            "checks": [],
        },
        "2026-03-15T00:00:00Z",
    )
    gov = {
        "step6_validation": {
            "total": 100,
            "sampled": 25,
            "skipped": 75,
            "hard_failed": 2,
            "hard_fail_rate": 0.08,
        },
        "sla": {
            "canary_shadow_write_enabled": True,
            "canary_shadow_only": False,
        },
    }
    out = _merge_governance(row, gov)
    assert out["step6_total"] == 100
    assert out["step6_sampled"] == 25
    assert out["step6_hard_failed"] == 2
    assert abs(out["step6_hard_fail_rate"] - 0.08) < 1e-9
    assert out["canary_shadow_write_enabled"] is True
    assert out["canary_shadow_only"] is False
