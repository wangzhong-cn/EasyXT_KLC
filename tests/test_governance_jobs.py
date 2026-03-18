from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from tools import governance_jobs


def _wm_defaults() -> dict:
    """Watermark args defaults — mirrors argparse defaults in governance_jobs.main()."""
    return dict(
        watermark_event_file="artifacts/realtime_watermark_events.jsonl",
        watermark_profile="balanced",
        watermark_profile_audit_file="artifacts/watermark_profile_audit.jsonl",
        watermark_profile_audit_max_entries=5,
        release_env="preprod",
        watermark_approval_required_profiles="aggressive",
        watermark_approval_id="",
        watermark_approver="",
        watermark_approval_registry_file="artifacts/watermark_approval_registry.json",
        watermark_approval_max_age_days=7,
        watermark_approval_signing_key="",
        watermark_approval_require_signature=False,
        watermark_approval_multisig_threshold=1,
        watermark_approval_signers="",
        watermark_approval_usage_log_file="artifacts/watermark_approval_usage.jsonl",
        watermark_approval_expiry_warn_days=2,
        watermark_approval_usage_warn_ratio=0.8,
        strict_watermark_qscore=False,
        strict_watermark_profile_approval=False,
        strict_watermark_profile_risk=False,
        strict_late_replay=False,
        strict_rebuild=False,
        late_limit=100,
        late_max_retries=3,
        late_reason_regex=r"(late|out_of_order|watermark|stale|reorder)",
        periods="",
        start_date="",
        end_date="",
        rebuild_lookback_days=7,
        rebuild_auto_symbol_limit=50,
        stock_code=None,
    )


_WM_QUALITY_OK = {"qscore": 1.0, "gate_pass": True, "profile": "balanced",
                  "total_events": 0, "late_fraction": 0.0, "ooo_fraction": 0.0,
                  "avg_lateness_ms": 0.0}
_WM_AUDIT_OK = {"entries": [], "anomalies": []}
_WM_APPROVAL_OK = {"approved": True, "gate_pass": True, "reason": "ok"}


def test_sla_job_outputs_step6_validation():
    fake_ui = SimpleNamespace(
        connect=lambda read_only=False: True,
        close=lambda: None,
        run_quarantine_replay=lambda limit, max_retries: {"processed": 0, "dead_letter": 0},
        generate_daily_sla_report=lambda report_date=None: {"gate_pass": True, "trust_score": 0.99},
        get_step6_validation_metrics=lambda: {"total": 10, "sampled": 10, "hard_failed": 1, "hard_fail_rate": 0.1},
    )
    args = SimpleNamespace(
        job="sla",
        limit=50,
        max_retries=3,
        report_date=None,
        duckdb_path=None,
        strict_sla=False,
        strict_dead_letter=False,
        strict_strategy_impact=False,
        strategy_impact_baseline=None,
        strategy_impact_results_dir=None,
        strategy_impact_delta_return=3.0,
        strategy_impact_delta_mdd=1.5,
        strategy_impact_enforce_sharpe_sign=False,
        strict_strategy_impact_baseline_meta=False,
        **_wm_defaults(),
    )
    with patch("tools.governance_jobs.argparse.ArgumentParser.parse_args", return_value=args):
        with patch("tools.governance_jobs.UnifiedDataInterface", return_value=fake_ui):
            with patch("tools.governance_jobs.evaluate_strategy_impact", return_value={"available": False, "gate_pass": True}):
                with patch("tools.governance_jobs._summarize_watermark_events", return_value=_WM_QUALITY_OK):
                    with patch("tools.governance_jobs._summarize_watermark_profile_audit", return_value=_WM_AUDIT_OK):
                        with patch("tools.governance_jobs._validate_watermark_profile_approval", return_value=_WM_APPROVAL_OK):
                            rc = governance_jobs.main()
    assert rc == 0


def test_all_job_strict_sla_still_honors_gate_pass():
    fake_ui = SimpleNamespace(
        connect=lambda read_only=False: True,
        close=lambda: None,
        run_quarantine_replay=lambda limit, max_retries: {"processed": 3, "dead_letter": 0},
        generate_daily_sla_report=lambda report_date=None: {"gate_pass": False, "trust_score": 0.7},
        get_step6_validation_metrics=lambda: {"total": 5, "sampled": 1, "hard_failed": 0, "hard_fail_rate": 0.0},
        run_late_event_replay=lambda limit, max_retries, reason_regex: {"replayed": 0},
    )
    args = SimpleNamespace(
        job="all",
        limit=50,
        max_retries=3,
        report_date=None,
        duckdb_path=None,
        strict_sla=True,
        strict_dead_letter=False,
        strict_strategy_impact=False,
        strategy_impact_baseline=None,
        strategy_impact_results_dir=None,
        strategy_impact_delta_return=3.0,
        strategy_impact_delta_mdd=1.5,
        strategy_impact_enforce_sharpe_sign=False,
        strict_strategy_impact_baseline_meta=False,
        **_wm_defaults(),
    )
    with patch("tools.governance_jobs.argparse.ArgumentParser.parse_args", return_value=args):
        with patch("tools.governance_jobs.UnifiedDataInterface", return_value=fake_ui):
            with patch("tools.governance_jobs.evaluate_strategy_impact", return_value={"available": False, "gate_pass": True}):
                with patch("tools.governance_jobs._summarize_watermark_events", return_value=_WM_QUALITY_OK):
                    with patch("tools.governance_jobs._summarize_watermark_profile_audit", return_value=_WM_AUDIT_OK):
                        with patch("tools.governance_jobs._validate_watermark_profile_approval", return_value=_WM_APPROVAL_OK):
                            with patch("tools.governance_jobs._run_batch_multiperiod_rebuild", return_value={"mode": "single", "ok": True}):
                                rc = governance_jobs.main()
    assert rc == 2


def test_strict_strategy_impact_blocks_when_gate_fails():
    fake_ui = SimpleNamespace(
        connect=lambda read_only=False: True,
        close=lambda: None,
        run_quarantine_replay=lambda limit, max_retries: {"processed": 0, "dead_letter": 0},
        generate_daily_sla_report=lambda report_date=None: {"gate_pass": True, "trust_score": 0.99},
        get_step6_validation_metrics=lambda: {"total": 1, "sampled": 1, "hard_failed": 0, "hard_fail_rate": 0.0},
        run_late_event_replay=lambda limit, max_retries, reason_regex: {"replayed": 0},
    )
    args = SimpleNamespace(
        job="all",
        limit=50,
        max_retries=3,
        report_date=None,
        duckdb_path=None,
        strict_sla=False,
        strict_dead_letter=False,
        strict_strategy_impact=True,
        strategy_impact_baseline=None,
        strategy_impact_results_dir=None,
        strategy_impact_delta_return=3.0,
        strategy_impact_delta_mdd=1.5,
        strategy_impact_enforce_sharpe_sign=True,
        strict_strategy_impact_baseline_meta=False,
        **_wm_defaults(),
    )
    with patch("tools.governance_jobs.argparse.ArgumentParser.parse_args", return_value=args):
        with patch("tools.governance_jobs.UnifiedDataInterface", return_value=fake_ui):
            with patch("tools.governance_jobs.evaluate_strategy_impact", return_value={"available": True, "gate_pass": False}):
                with patch("tools.governance_jobs._summarize_watermark_events", return_value=_WM_QUALITY_OK):
                    with patch("tools.governance_jobs._summarize_watermark_profile_audit", return_value=_WM_AUDIT_OK):
                        with patch("tools.governance_jobs._validate_watermark_profile_approval", return_value=_WM_APPROVAL_OK):
                            with patch("tools.governance_jobs._run_batch_multiperiod_rebuild", return_value={"mode": "single", "ok": True}):
                                rc = governance_jobs.main()
    assert rc == 4


def test_strict_strategy_impact_baseline_meta_blocks_when_missing(tmp_path):
    fake_ui = SimpleNamespace(
        connect=lambda read_only=False: True,
        close=lambda: None,
        run_quarantine_replay=lambda limit, max_retries: {"processed": 0, "dead_letter": 0},
        generate_daily_sla_report=lambda report_date=None: {"gate_pass": True, "trust_score": 0.99},
        get_step6_validation_metrics=lambda: {"total": 1, "sampled": 1, "hard_failed": 0, "hard_fail_rate": 0.0},
        run_late_event_replay=lambda limit, max_retries, reason_regex: {"replayed": 0},
    )
    baseline_path = tmp_path / "strategy_impact_baseline.json"
    baseline_path.write_text('{"schema_version":"strategy-impact/v1","strategies":[]}', encoding="utf-8")
    args = SimpleNamespace(
        job="all",
        limit=50,
        max_retries=3,
        report_date=None,
        duckdb_path=None,
        strict_sla=False,
        strict_dead_letter=False,
        strict_strategy_impact=False,
        strategy_impact_baseline=str(baseline_path),
        strategy_impact_results_dir=None,
        strategy_impact_delta_return=3.0,
        strategy_impact_delta_mdd=1.5,
        strategy_impact_enforce_sharpe_sign=False,
        strict_strategy_impact_baseline_meta=True,
        **_wm_defaults(),
    )
    with patch("tools.governance_jobs.argparse.ArgumentParser.parse_args", return_value=args):
        with patch("tools.governance_jobs.UnifiedDataInterface", return_value=fake_ui):
            with patch("tools.governance_jobs.evaluate_strategy_impact", return_value={"available": False, "gate_pass": True}):
                with patch("tools.governance_jobs._summarize_watermark_events", return_value=_WM_QUALITY_OK):
                    with patch("tools.governance_jobs._summarize_watermark_profile_audit", return_value=_WM_AUDIT_OK):
                        with patch("tools.governance_jobs._validate_watermark_profile_approval", return_value=_WM_APPROVAL_OK):
                            with patch("tools.governance_jobs._run_batch_multiperiod_rebuild", return_value={"mode": "single", "ok": True}):
                                rc = governance_jobs.main()
    assert rc == 5
