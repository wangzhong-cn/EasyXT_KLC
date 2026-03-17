from __future__ import annotations

from tools.peak_release_gate import evaluate_peak_release_gate


def test_peak_release_gate_pass():
    out = evaluate_peak_release_gate(
        {"peak_ready": True, "consecutive_compliant_days": 15, "compliance_ratio_pct": 98.0},
        warn_consecutive_days=7,
        fail_consecutive_days=14,
        release_env="prod",
    )
    assert out["level"] == "pass"
    assert out["release_env"] == "prod"
    assert out["gap_to_fail_days"] == 0


def test_peak_release_gate_warn():
    out = evaluate_peak_release_gate(
        {"peak_ready": False, "consecutive_compliant_days": 9, "compliance_ratio_pct": 90.0},
        warn_consecutive_days=7,
        fail_consecutive_days=14,
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
    )
    assert out["level"] == "fail"
    assert out["gap_to_fail_days"] == 11
