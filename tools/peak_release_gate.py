from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVIDENCE = PROJECT_ROOT / "artifacts" / "stability_evidence_30d.json"
DEFAULT_OUT = PROJECT_ROOT / "artifacts" / "peak_release_gate_latest.json"


def evaluate_peak_release_gate(
    evidence: dict[str, Any],
    *,
    warn_consecutive_days: int,
    fail_consecutive_days: int,
    release_env: str = "",
) -> dict[str, Any]:
    peak_ready = bool(evidence.get("peak_ready", False))
    consec = int(evidence.get("consecutive_compliant_days", 0) or 0)
    compliance_ratio = float(evidence.get("compliance_ratio_pct", 0.0) or 0.0)
    if peak_ready and consec >= fail_consecutive_days:
        level = "pass"
    elif consec >= warn_consecutive_days:
        level = "warn"
    else:
        level = "fail"
    return {
        "level": level,
        "release_env": str(release_env or ""),
        "peak_ready": peak_ready,
        "consecutive_compliant_days": consec,
        "compliance_ratio_pct": compliance_ratio,
        "warn_consecutive_days": int(warn_consecutive_days),
        "fail_consecutive_days": int(fail_consecutive_days),
        "gap_to_fail_days": max(0, int(fail_consecutive_days) - consec),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="峰值发布门禁判定器")
    parser.add_argument("--evidence", type=Path, default=DEFAULT_EVIDENCE)
    parser.add_argument("--warn-consecutive-days", type=int, default=7)
    parser.add_argument("--fail-consecutive-days", type=int, default=14)
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
                release_env=str(args.release_env or ""),
            )
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    if args.strict and payload.get("level") == "fail":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
