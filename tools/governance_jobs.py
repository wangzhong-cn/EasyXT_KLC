#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_manager.unified_data_interface import UnifiedDataInterface
from tools.strategy_impact_gate import evaluate_strategy_impact


def _validate_strategy_impact_baseline_metadata(path: Path | None) -> dict[str, object]:
    if path is None:
        return {"checked": False, "valid": True, "reason": "baseline_not_provided"}
    if not path.exists():
        return {"checked": True, "valid": False, "reason": "baseline_missing", "path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"checked": True, "valid": False, "reason": "baseline_parse_error", "path": str(path)}
    if not isinstance(payload, dict):
        return {"checked": True, "valid": False, "reason": "baseline_not_object", "path": str(path)}
    meta = payload.get("_meta", {})
    if not isinstance(meta, dict):
        return {"checked": True, "valid": False, "reason": "meta_missing", "path": str(path)}
    required = ("approval_id", "approver", "updated_at", "reason_excerpt")
    missing = [k for k in required if not str(meta.get(k, "")).strip()]
    if missing:
        return {
            "checked": True,
            "valid": False,
            "reason": "meta_required_fields_missing",
            "missing_fields": missing,
            "path": str(path),
        }
    return {"checked": True, "valid": True, "reason": "ok", "path": str(path)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run governance replay and SLA jobs.")
    parser.add_argument("--job", choices=["replay", "sla", "all"], default="all")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--report-date", default=None)
    parser.add_argument("--duckdb-path", default=None)
    parser.add_argument("--strict-sla", action="store_true")
    parser.add_argument("--strict-dead-letter", action="store_true")
    parser.add_argument("--strict-strategy-impact", action="store_true")
    parser.add_argument("--strategy-impact-baseline", default=None)
    parser.add_argument("--strategy-impact-results-dir", default=None)
    parser.add_argument("--strategy-impact-delta-return", type=float, default=3.0)
    parser.add_argument("--strategy-impact-delta-mdd", type=float, default=1.5)
    parser.add_argument("--strategy-impact-enforce-sharpe-sign", action="store_true")
    parser.add_argument("--strict-strategy-impact-baseline-meta", action="store_true")
    args = parser.parse_args()

    ui = UnifiedDataInterface(duckdb_path=args.duckdb_path, silent_init=True)
    if not ui.connect(read_only=False):
        print(json.dumps({"ok": False, "error": "duckdb_connect_failed"}, ensure_ascii=False))
        return 1
    result: dict[str, object] = {"ok": True}
    try:
        if args.job in ("replay", "all"):
            result["replay"] = ui.run_quarantine_replay(
                limit=max(args.limit, 1), max_retries=max(args.max_retries, 1)
            )
        if args.job in ("sla", "all"):
            result["sla"] = ui.generate_daily_sla_report(report_date=args.report_date)
            result["step6_validation"] = ui.get_step6_validation_metrics()
            baseline_path = Path(args.strategy_impact_baseline) if args.strategy_impact_baseline else None
            results_dir = Path(args.strategy_impact_results_dir) if args.strategy_impact_results_dir else None
            kwargs: dict[str, object] = {
                "delta_return_threshold": float(args.strategy_impact_delta_return),
                "delta_mdd_threshold": float(args.strategy_impact_delta_mdd),
                "enforce_sharpe_sign": bool(args.strategy_impact_enforce_sharpe_sign),
            }
            if baseline_path is not None:
                kwargs["baseline_path"] = baseline_path
            if results_dir is not None:
                kwargs["results_dir"] = results_dir
            result["strategy_impact"] = evaluate_strategy_impact(**kwargs)
            result["strategy_impact_baseline_meta"] = _validate_strategy_impact_baseline_metadata(baseline_path)
    finally:
        ui.close()
    print(json.dumps(result, ensure_ascii=False))
    if args.strict_sla and isinstance(result.get("sla"), dict):
        gate_pass = bool((result.get("sla") or {}).get("gate_pass", False))
        if not gate_pass:
            return 2
    if args.strict_dead_letter and isinstance(result.get("replay"), dict):
        dead_letter = int((result.get("replay") or {}).get("dead_letter", 0) or 0)
        if dead_letter > 0:
            return 3
    if args.strict_strategy_impact and isinstance(result.get("strategy_impact"), dict):
        impact = result.get("strategy_impact") or {}
        if bool(impact.get("available", False)) and not bool(impact.get("gate_pass", True)):
            return 4
    if args.strict_strategy_impact_baseline_meta and isinstance(result.get("strategy_impact_baseline_meta"), dict):
        meta_check = result.get("strategy_impact_baseline_meta") or {}
        if not bool(meta_check.get("valid", False)):
            return 5
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
