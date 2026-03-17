from __future__ import annotations

from tools.governance_strategy_dashboard import build_dashboard
from tools.p0_weekly_report import _build_report


def test_dashboard_renders_strategy_impact_section():
    md = build_dashboard(
        history=[{"ts": "2026-03-15T00:00:00Z", "strict_gate_pass": True, "P0_open_count": 0, "active_critical_high": 0}],
        stage1_results=[],
        touch_events=[],
        recon=None,
        strategy_impact={
            "available": True,
            "gate_pass": False,
            "delta": {
                "annualized_return_pct": -2.5,
                "max_drawdown_pct": 1.8,
                "sharpe_sign_changed": True,
            },
            "checks": {
                "delta_return_pass": True,
                "delta_mdd_pass": False,
                "sharpe_sign_pass": False,
            },
        },
        stability_evidence={
            "peak_ready": False,
            "consecutive_compliant_days": 9,
        },
        peak_release_gate={
            "level": "warn",
            "gap_to_fail_days": 5,
        },
        generated_at="2026-03-15T00:00:00Z",
    )
    assert "联动分析 4：策略影响门禁" in md
    assert "ΔR（年化收益百分点）" in md
    assert "Sharpe_sign 翻转" in md
    assert "❌ FAIL" in md
    assert "peak_release_gate(SSOT)" in md
    assert "WARN（gap 5d）" in md


def test_weekly_report_includes_strategy_impact_action_item():
    text = _build_report(
        records_all=[{"ts": "2026-03-15T00:00:00Z", "strict_gate_pass": True, "P0_open_count": 0, "active_critical_high": 0}],
        records_week=[{"ts": "2026-03-15T00:00:00Z", "strict_gate_pass": True, "P0_open_count": 0, "active_critical_high": 0}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
        touched_pr_count=0,
        strategy_impact_latest={
            "available": True,
            "gate_pass": False,
            "delta": {
                "annualized_return_pct": -4.2,
                "max_drawdown_pct": 2.1,
                "sharpe_sign_changed": True,
            },
        },
        stability_evidence_latest={
            "peak_ready": False,
            "consecutive_compliant_days": 8,
        },
        peak_release_gate_latest={
            "level": "warn",
            "gap_to_fail_days": 6,
        },
    )
    assert "策略影响门禁 gate_pass" in text
    assert "策略影响门禁未通过" in text
    assert "距峰值还差 6 天" in text
    assert "峰值发布门禁(SSOT)" in text


def test_dashboard_fallbacks_to_stability_evidence_when_ssot_missing():
    md = build_dashboard(
        history=[{"ts": "2026-03-15T00:00:00Z", "strict_gate_pass": True, "P0_open_count": 0, "active_critical_high": 0}],
        stage1_results=[],
        touch_events=[],
        recon=None,
        strategy_impact=None,
        stability_evidence={
            "peak_ready": False,
            "consecutive_compliant_days": 10,
        },
        peak_release_gate=None,
        generated_at="2026-03-15T00:00:00Z",
    )
    assert "peak_release_gate(SSOT)" in md
    assert "WARN（gap 4d）" in md
