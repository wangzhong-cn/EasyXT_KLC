from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from data_manager.governance_metadata import build_governance_snapshot

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HISTORY_PATH = PROJECT_ROOT / "artifacts" / "p0_trend_history.json"
OUT_MD = PROJECT_ROOT / "artifacts" / "stability_evidence_30d.md"
OUT_JSON = PROJECT_ROOT / "artifacts" / "stability_evidence_30d.json"
PERIOD_VALIDATION_PATH = PROJECT_ROOT / "artifacts" / "period_validation_report.jsonl"


def _load_history(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, list) else []
    except Exception:
        return []


def _is_compliant(
    row: dict[str, Any],
    *,
    step6_hard_fail_rate_max: float,
    require_strategy_impact: bool,
) -> bool:
    gate_ok = bool(row.get("strict_gate_pass", False))
    s6_rate = float(row.get("step6_hard_fail_rate", 0.0) or 0.0)
    s6_ok = s6_rate <= step6_hard_fail_rate_max
    if not require_strategy_impact:
        strategy_ok = True
    else:
        strategy_ok = bool(row.get("strategy_impact_available", False)) and bool(
            row.get("strategy_impact_gate_pass", False)
        )
    return gate_ok and s6_ok and strategy_ok


def _consecutive_compliant_days(
    rows: list[dict[str, Any]],
    *,
    step6_hard_fail_rate_max: float,
    require_strategy_impact: bool,
) -> int:
    cnt = 0
    for row in reversed(rows):
        if _is_compliant(
            row,
            step6_hard_fail_rate_max=step6_hard_fail_rate_max,
            require_strategy_impact=require_strategy_impact,
        ):
            cnt += 1
            continue
        break
    return cnt


def _summarize_period_validation(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"report_exists": False, "rows": 0, "failed_rows": 0, "last_failed_period": ""}
    rows = 0
    failed_rows = 0
    last_failed_period = ""
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return {"report_exists": True, "rows": 0, "failed_rows": 0, "last_failed_period": ""}
    for raw in lines[-1000:]:
        s = raw.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        rows += 1
        if bool(obj.get("is_valid", True)) is False:
            failed_rows += 1
            last_failed_period = str(obj.get("period") or last_failed_period)
    return {
        "report_exists": True,
        "rows": rows,
        "failed_rows": failed_rows,
        "last_failed_period": last_failed_period,
    }


def build_payload(
    rows: list[dict[str, Any]],
    *,
    window_days: int,
    step6_hard_fail_rate_max: float,
    require_strategy_impact: bool,
    period_validation_summary: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    pv = period_validation_summary or {"report_exists": False, "rows": 0, "failed_rows": 0, "last_failed_period": ""}
    governance = build_governance_snapshot(trade_date=datetime.now(tz=timezone.utc))
    tail = rows[-window_days:] if window_days > 0 else rows
    if not tail:
        return {
            "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window_days": window_days,
            "record_count": 0,
            "compliance_ratio_pct": 0.0,
            "consecutive_compliant_days": 0,
            "step6_hard_fail_rate_max": step6_hard_fail_rate_max,
            "require_strategy_impact": require_strategy_impact,
            "peak_ready": False,
            "peak_ready_rule": "consecutive_compliant_days>=14 and compliance_ratio_pct>=95",
            "period_validation": pv,
            "governance": governance,
            "daily": [],
        }
    compliant = [
        _is_compliant(
            r,
            step6_hard_fail_rate_max=step6_hard_fail_rate_max,
            require_strategy_impact=require_strategy_impact,
        )
        for r in tail
    ]
    ratio = (sum(1 for x in compliant if x) * 100.0) / len(compliant)
    consec = _consecutive_compliant_days(
        tail,
        step6_hard_fail_rate_max=step6_hard_fail_rate_max,
        require_strategy_impact=require_strategy_impact,
    )
    peak_ready = consec >= 14 and ratio >= 95.0
    return {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days": window_days,
        "record_count": len(tail),
        "compliance_ratio_pct": round(ratio, 2),
        "consecutive_compliant_days": consec,
        "step6_hard_fail_rate_max": step6_hard_fail_rate_max,
        "require_strategy_impact": require_strategy_impact,
        "peak_ready": peak_ready,
        "peak_ready_rule": "consecutive_compliant_days>=14 and compliance_ratio_pct>=95",
        "period_validation": pv,
        "governance": governance,
        "daily": [
            {
                "date": str(r.get("ts", ""))[:10],
                "strict_gate_pass": bool(r.get("strict_gate_pass", False)),
                "step6_hard_fail_rate": float(r.get("step6_hard_fail_rate", 0.0) or 0.0),
                "strategy_impact_available": bool(r.get("strategy_impact_available", False)),
                "strategy_impact_gate_pass": bool(r.get("strategy_impact_gate_pass", False)),
                "period_validation_failed_items": int(r.get("period_validation_failed_items", 0) or 0),
                "compliant": _is_compliant(
                    r,
                    step6_hard_fail_rate_max=step6_hard_fail_rate_max,
                    require_strategy_impact=require_strategy_impact,
                ),
            }
            for r in tail
        ],
    }


def render_md(payload: dict[str, Any]) -> str:
    lines = [
        "# 30天稳定证据板",
        "",
        f"> 生成时间: {payload.get('generated_at', '?')}",
        "",
        "| 指标 | 值 |",
        "|---|---|",
        f"| record_count | {payload.get('record_count', 0)} |",
        f"| compliance_ratio_pct | {payload.get('compliance_ratio_pct', 0.0)}% |",
        f"| consecutive_compliant_days | {payload.get('consecutive_compliant_days', 0)} |",
        f"| peak_ready | {'✅ YES' if payload.get('peak_ready', False) else '❌ NO'} |",
        f"| peak_ready_rule | {payload.get('peak_ready_rule', '')} |",
        f"| period_validation_failed_rows | {int((payload.get('period_validation') or {}).get('failed_rows', 0) if isinstance(payload.get('period_validation'), dict) else 0)} |",
        "",
        "## Daily",
        "",
        "| date | gate | step6_hard_fail_rate | pv_failed | strategy_impact | compliant |",
        "|---|---|---:|---:|---|---|",
    ]
    for d in payload.get("daily", []):
        gate = "✅" if d.get("strict_gate_pass") else "❌"
        impact = "✅" if (d.get("strategy_impact_available") and d.get("strategy_impact_gate_pass")) else "❌"
        comp = "✅" if d.get("compliant") else "❌"
        lines.append(
            f"| {d.get('date', '?')} | {gate} | {float(d.get('step6_hard_fail_rate', 0.0)):.4f} | {int(d.get('period_validation_failed_items', 0) or 0)} | {impact} | {comp} |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="30天稳定证据板生成器")
    parser.add_argument("--history", type=Path, default=HISTORY_PATH)
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--step6-hard-fail-rate-max", type=float, default=0.05)
    parser.add_argument("--no-require-strategy-impact", action="store_true")
    parser.add_argument("--period-validation-report", type=Path, default=PERIOD_VALIDATION_PATH)
    parser.add_argument("--out-md", type=Path, default=OUT_MD)
    parser.add_argument("--out-json", type=Path, default=OUT_JSON)
    args = parser.parse_args()

    rows = _load_history(args.history)
    payload = build_payload(
        rows,
        window_days=args.window_days,
        step6_hard_fail_rate_max=args.step6_hard_fail_rate_max,
        require_strategy_impact=not args.no_require_strategy_impact,
        period_validation_summary=_summarize_period_validation(args.period_validation_report),
    )
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text(render_md(payload), encoding="utf-8")
    args.out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] stability evidence board updated: {args.out_md}")
    print(f"[OK] stability evidence json updated: {args.out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
