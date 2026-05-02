from __future__ import annotations

from tools.peak_release_gate import evaluate_peak_release_gate


def test_peak_release_gate_pass():
    out = evaluate_peak_release_gate(
        {"peak_ready": True, "consecutive_compliant_days": 15, "compliance_ratio_pct": 98.0},
        warn_consecutive_days=7,
        fail_consecutive_days=14,
        max_period_validation_failed_items=0,
        release_env="prod",
    )
    assert out["level"] == "pass"
    assert out["release_env"] == "prod"
    assert out["gap_to_fail_days"] == 0
    assert out["governance"]["session_profile_id"] == "CN_A"
    assert out["governance"]["period_registry_version"] == "2026.04.01"


def test_peak_release_gate_warn():
    out = evaluate_peak_release_gate(
        {"peak_ready": False, "consecutive_compliant_days": 9, "compliance_ratio_pct": 90.0},
        warn_consecutive_days=7,
        fail_consecutive_days=14,
        max_period_validation_failed_items=0,
        release_env="preprod",
    )
    assert out["level"] == "warn"
    assert out["release_env"] == "preprod"
    assert out["gap_to_fail_days"] == 5


def test_peak_release_gate_fail():
    out = evaluate_peak_release_gate(
        {"peak_ready": False, "consecutive_compliant_days": 3, "compliance_ratio_pct": 70.0},
        warn_consecutive_days=7,
        fail_consecutive_days=14,
        max_period_validation_failed_items=0,
    )
    assert out["level"] == "fail"
    assert out["gap_to_fail_days"] == 11


def test_peak_release_gate_fails_on_period_validation_over_threshold():
    out = evaluate_peak_release_gate(
        {
            "peak_ready": True,
            "consecutive_compliant_days": 20,
            "compliance_ratio_pct": 99.0,
            "period_validation": {"failed_rows": 3},
        },
        warn_consecutive_days=7,
        fail_consecutive_days=14,
        max_period_validation_failed_items=0,
    )
    assert out["level"] == "fail"
    assert out["period_validation_gate_pass"] is False
    assert out["period_validation_failed_items"] == 3


def test_peak_release_gate_passes_when_period_validation_within_threshold():
    out = evaluate_peak_release_gate(
        {
            "peak_ready": True,
            "consecutive_compliant_days": 20,
            "compliance_ratio_pct": 99.0,
            "period_validation": {"failed_rows": 1},
        },
        warn_consecutive_days=7,
        fail_consecutive_days=14,
        max_period_validation_failed_items=1,
    )
    assert out["level"] == "pass"
    assert out["period_validation_gate_pass"] is True
