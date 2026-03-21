from __future__ import annotations

from tools.p0_trend_update import _make_row, _merge_governance, _render_dashboard


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


def test_make_row_includes_period_validation_metrics():
    row = _make_row(
        {
            "strict_gate_pass": False,
            "P0_open_count": 1,
            "active_critical_high": 1,
            "period_validation_detail": {"status": "fail", "failed_items": 3},
            "checks": [],
        },
        "2026-03-16T00:00:00Z",
    )
    assert row["period_validation_status"] == "fail"
    assert row["period_validation_failed_items"] == 3


def test_make_row_includes_gate_contract_fields_and_dashboard_renders_them():
    row = _make_row(
        {
            "strict_gate_pass": True,
            "P0_open_count": 0,
            "active_critical_high": 0,
            "period_validation_detail": {"status": "pass", "failed_items": 0},
            "gate_contract_valid": True,
            "gate_contract_version": 1,
            "gate_contract_error": "",
            "gate_contract_rag": "RAG[🟢 GREEN]",
            "gate_detail_tag": "GATE_DETAIL[v=1|rag=RAG[🟢 GREEN]|pv_detail=PV_DETAIL[v=1|pv=PV[✅ PASS]|failed=0|max=0|msg=N%2FA|action=N%2FA]]",
            "checks": [],
        },
        "2026-03-17T00:00:00Z",
    )
    assert row["gate_contract_valid"] is True
    assert row["gate_contract_version"] == 1
    assert row["gate_contract_rag"] == "RAG[🟢 GREEN]"
    assert row["contract_health"] == "HEALTHY"
    md = _render_dashboard([row])
    assert "gate_contract_valid" in md
    assert "gate_contract_version" in md
    assert "gate_contract_ok" in md
    assert "contract_health" in md
    assert "HEALTHY" in md


def test_contract_health_broken_when_contract_invalid():
    row = _make_row(
        {
            "strict_gate_pass": False,
            "P0_open_count": 1,
            "active_critical_high": 1,
            "period_validation_detail": {"status": "fail", "failed_items": 2},
            "gate_contract_valid": False,
            "gate_contract_version": 1,
            "gate_contract_error": "invalid_gate_detail_format",
            "gate_contract_rag": "",
            "gate_detail_tag": "GATE_DETAIL[broken]",
            "checks": [],
        },
        "2026-03-18T00:00:00Z",
    )
    assert row["contract_health"] == "BROKEN"
    md = _render_dashboard([row])
    assert "BROKEN" in md
