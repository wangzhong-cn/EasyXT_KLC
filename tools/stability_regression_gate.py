from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class GroupResult:
    group: str
    run_index: int
    return_code: int
    passed: int
    failed: int
    duration_sec: float
    command: list[str]


GROUPS: dict[str, dict[str, object]] = {
    "derived_period_chain": {
        "paths": [
            "tests/test_unified_data_interface.py",
            "tests/test_unified_data_interface_extra.py",
            "tests/test_unified_session_profile_rules.py",
        ],
        "kexpr": "derived_period or get_stock_data_local or get_stock_date_range_derived or resolve_session_profile_for_symbol",
    },
    "write_observability_chain": {
        "paths": [
            "tests/test_unified_data_interface.py",
            "tests/test_unified_data_interface_extra.py",
            "tests/test_p0_period_validation_gate.py",
            "tests/test_stability_evidence_and_baseline_manager.py",
            "tests/test_p0_trend_update.py",
            "tests/test_peak_release_gate.py",
            "tests/test_peak_release_block_note.py",
            "tests/test_release_rag_policy.py",
            "tests/test_data_governance_panel_smoke.py",
        ],
        "kexpr": "post_write_verify_failed or quarantine or data_quality_alert or write_audit or period_validation_report_check or stability_evidence_board_includes_period_validation_summary or make_row_includes_period_validation_metrics or peak_release_gate or period_validation_gate_pass or release_rag_policy or render_release_gate_status",
    },
    "fallback_recovery_chain": {
        "paths": [
            "tests/test_unified_data_interface.py",
            "tests/test_unified_data_interface_extra.py",
        ],
        "kexpr": "_check_missing_trading_days or fallback or wal_reconnect or import_fail",
    },
    "convergence_contract_chain": {
        "paths": [
            "tests/test_convergence_contract.py",
            "tests/test_period_bar_builder_v2.py",
        ],
        # 全部契约测试：base_data + intraday + multiday + cross_source + aggregation_chain
        "kexpr": "TestBaseDataStructure or TestIntradayConvergence or TestMultidayLeftAlign or TestCrossSourceConsistency or TestGetListingDate or TestAggregationChain or test_intraday_metadata_columns_and_session_profile or test_anchor_none_does_not_force_daily_close or test_cross_validate_emits_jsonl_report or test_session_profile_file_override",
    },
    "thread_lifecycle_chain": {
        "paths": [
            "tests/test_kline_chart_workspace_logic.py",
            "tests/test_chart_fallback.py",
            "tests/test_pipeline_bar_guard.py",
        ],
        "kexpr": "TestThreadSafetyHelpers or TestEnsureRealtimeApi or TestComputeInitialRange or TestNativeLwcFallback",
    },
    "timestamp_contract_chain": {
        "paths": [
            "tests/test_broadcast_pipeline.py",
            "tests/test_realtime_pipeline_manager.py",
            "tests/test_kline_chart_workspace_logic.py",
        ],
        "kexpr": "event_ts_ms or source_event_ts_ms or resolve_timestamp_prefers_source_event_ts_ms or event_ts_ms_priority_over_time_field",
    },
}


def _parse_pytest_counts(output: str) -> tuple[int, int]:
    passed = 0
    failed = 0
    m_pass = re.search(r"(\d+)\s+passed", output)
    if m_pass:
        passed = int(m_pass.group(1))
    m_fail = re.search(r"(\d+)\s+failed", output)
    if m_fail:
        failed = int(m_fail.group(1))
    return passed, failed


def _run_once(
    python_exe: str,
    repo_root: Path,
    group_name: str,
    group_conf: dict[str, object],
    run_index: int,
) -> GroupResult:
    paths = [str(p) for p in group_conf["paths"]]  # type: ignore[index]
    kexpr = str(group_conf["kexpr"])  # type: ignore[index]
    cmd = [
        python_exe,
        "-m",
        "pytest",
        *paths,
        "-k",
        kexpr,
        "-q",
        "--tb=short",
    ]
    started = datetime.now()
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )
    duration = (datetime.now() - started).total_seconds()
    merged_out = (proc.stdout or "") + "\n" + (proc.stderr or "")
    passed, failed = _parse_pytest_counts(merged_out)
    if proc.returncode != 0 and failed == 0:
        failed = 1
    return GroupResult(
        group=group_name,
        run_index=run_index,
        return_code=proc.returncode,
        passed=passed,
        failed=failed,
        duration_sec=duration,
        command=cmd,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--group", type=str, default="all")
    parser.add_argument("--python", type=str, default=sys.executable)
    parser.add_argument(
        "--output",
        type=str,
        default="artifacts/stability_regression_gate_latest.json",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    out_path = (repo_root / args.output).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.group == "all":
        selected = GROUPS
    else:
        if args.group not in GROUPS:
            print(f"unknown group: {args.group}")
            return 2
        selected = {args.group: GROUPS[args.group]}

    results: list[GroupResult] = []
    overall_ok = True

    for run_idx in range(1, max(args.runs, 1) + 1):
        for gname, gconf in selected.items():
            res = _run_once(args.python, repo_root, gname, gconf, run_idx)
            results.append(res)
            print(
                f"[run {run_idx}] {gname}: rc={res.return_code} passed={res.passed} failed={res.failed} dur={res.duration_sec:.1f}s"
            )
            if res.return_code != 0:
                overall_ok = False

    by_group: dict[str, list[GroupResult]] = {}
    for r in results:
        by_group.setdefault(r.group, []).append(r)

    stability_ok = True
    for gname, rows in by_group.items():
        passed_set = {r.passed for r in rows}
        failed_set = {r.failed for r in rows}
        if len(passed_set) > 1 or len(failed_set) > 1:
            stability_ok = False
            overall_ok = False
            print(f"[unstable] {gname}: passed={sorted(passed_set)} failed={sorted(failed_set)}")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "runs": max(args.runs, 1),
        "group": args.group,
        "overall_ok": overall_ok,
        "stability_ok": stability_ok,
        "results": [asdict(r) for r in results],
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"written: {out_path}")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
