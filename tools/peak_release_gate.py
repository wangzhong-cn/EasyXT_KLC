from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from data_manager.governance_metadata import build_governance_snapshot

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVIDENCE = PROJECT_ROOT / "artifacts" / "stability_evidence_30d.json"
DEFAULT_OUT = PROJECT_ROOT / "artifacts" / "peak_release_gate_latest.json"


def evaluate_peak_release_gate(
    evidence: dict[str, Any],
    *,
    warn_consecutive_days: int,
    fail_consecutive_days: int,
    max_period_validation_failed_items: int,
    release_env: str = "",
    governance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    peak_ready = bool(evidence.get("peak_ready", False))
    consec = int(evidence.get("consecutive_compliant_days", 0) or 0)
    compliance_ratio = float(evidence.get("compliance_ratio_pct", 0.0) or 0.0)
    period_validation = evidence.get("period_validation") if isinstance(evidence.get("period_validation"), dict) else {}
    period_failed_items = int(period_validation.get("failed_rows", 0) or 0)
    period_validation_pass = period_failed_items <= int(max_period_validation_failed_items)
    if not period_validation_pass:
        level = "fail"
    elif peak_ready and consec >= fail_consecutive_days:
        level = "pass"
    elif consec >= warn_consecutive_days:
        level = "warn"
    else:
        level = "fail"
    governance_payload = governance if isinstance(governance, dict) else {}
    if not governance_payload:
        governance_payload = build_governance_snapshot(trade_date=evidence.get("generated_at"))
    return {
        "level": level,
        "release_env": str(release_env or ""),
        "peak_ready": peak_ready,
        "consecutive_compliant_days": consec,
        "compliance_ratio_pct": compliance_ratio,
        "warn_consecutive_days": int(warn_consecutive_days),
        "fail_consecutive_days": int(fail_consecutive_days),
        "gap_to_fail_days": max(0, int(fail_consecutive_days) - consec),
        "period_validation_failed_items": period_failed_items,
        "max_period_validation_failed_items": int(max_period_validation_failed_items),
        "period_validation_gate_pass": period_validation_pass,
        "governance": governance_payload,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="峰值发布门禁判定器")
    parser.add_argument("--evidence", type=Path, default=DEFAULT_EVIDENCE)
    parser.add_argument("--warn-consecutive-days", type=int, default=7)
    parser.add_argument("--fail-consecutive-days", type=int, default=14)
    parser.add_argument("--max-period-validation-failed-items", type=int, default=0)
    parser.add_argument("--release-env", type=str, default="")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--out-json", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    if not args.evidence.exists():
        payload = {"level": "fail", "reason": "evidence_missing", "path": str(args.evidence)}
    else:
        try:
            evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
        except Exception:
            payload = {"level": "fail", "reason": "evidence_parse_error", "path": str(args.evidence)}
        else:
            payload = evaluate_peak_release_gate(
                evidence if isinstance(evidence, dict) else {},
                warn_consecutive_days=max(1, args.warn_consecutive_days),
                fail_consecutive_days=max(1, args.fail_consecutive_days),
                max_period_validation_failed_items=max(0, args.max_period_validation_failed_items),
                release_env=str(args.release_env or ""),
                governance=(evidence.get("governance") if isinstance(evidence, dict) else {}),
            )
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    if args.strict and payload.get("level") == "fail":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
