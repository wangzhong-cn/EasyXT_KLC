from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from tools import governance_jobs


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
    )
    with patch("tools.governance_jobs.argparse.ArgumentParser.parse_args", return_value=args):
        with patch("tools.governance_jobs.UnifiedDataInterface", return_value=fake_ui):
            with patch("tools.governance_jobs.evaluate_strategy_impact", return_value={"available": False, "gate_pass": True}):
                rc = governance_jobs.main()
    assert rc == 0


def test_all_job_strict_sla_still_honors_gate_pass():
    fake_ui = SimpleNamespace(
        connect=lambda read_only=False: True,
        close=lambda: None,
        run_quarantine_replay=lambda limit, max_retries: {"processed": 3, "dead_letter": 0},
        generate_daily_sla_report=lambda report_date=None: {"gate_pass": False, "trust_score": 0.7},
        get_step6_validation_metrics=lambda: {"total": 5, "sampled": 1, "hard_failed": 0, "hard_fail_rate": 0.0},
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
    )
    with patch("tools.governance_jobs.argparse.ArgumentParser.parse_args", return_value=args):
        with patch("tools.governance_jobs.UnifiedDataInterface", return_value=fake_ui):
            with patch("tools.governance_jobs.evaluate_strategy_impact", return_value={"available": False, "gate_pass": True}):
                rc = governance_jobs.main()
    assert rc == 2


def test_strict_strategy_impact_blocks_when_gate_fails():
    fake_ui = SimpleNamespace(
        connect=lambda read_only=False: True,
        close=lambda: None,
        run_quarantine_replay=lambda limit, max_retries: {"processed": 0, "dead_letter": 0},
        generate_daily_sla_report=lambda report_date=None: {"gate_pass": True, "trust_score": 0.99},
        get_step6_validation_metrics=lambda: {"total": 1, "sampled": 1, "hard_failed": 0, "hard_fail_rate": 0.0},
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
    )
    with patch("tools.governance_jobs.argparse.ArgumentParser.parse_args", return_value=args):
        with patch("tools.governance_jobs.UnifiedDataInterface", return_value=fake_ui):
            with patch("tools.governance_jobs.evaluate_strategy_impact", return_value={"available": True, "gate_pass": False}):
                rc = governance_jobs.main()
    assert rc == 4


def test_strict_strategy_impact_baseline_meta_blocks_when_missing(tmp_path):
    fake_ui = SimpleNamespace(
        connect=lambda read_only=False: True,
        close=lambda: None,
        run_quarantine_replay=lambda limit, max_retries: {"processed": 0, "dead_letter": 0},
        generate_daily_sla_report=lambda report_date=None: {"gate_pass": True, "trust_score": 0.99},
        get_step6_validation_metrics=lambda: {"total": 1, "sampled": 1, "hard_failed": 0, "hard_fail_rate": 0.0},
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
    )
    with patch("tools.governance_jobs.argparse.ArgumentParser.parse_args", return_value=args):
        with patch("tools.governance_jobs.UnifiedDataInterface", return_value=fake_ui):
            with patch("tools.governance_jobs.evaluate_strategy_impact", return_value={"available": False, "gate_pass": True}):
                rc = governance_jobs.main()
    assert rc == 5
