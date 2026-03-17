from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PEAK_GATE = PROJECT_ROOT / "artifacts" / "peak_release_gate_latest.json"
DEFAULT_EVIDENCE = PROJECT_ROOT / "artifacts" / "stability_evidence_30d.json"
DEFAULT_TREND_HISTORY = PROJECT_ROOT / "artifacts" / "p0_trend_history.json"
DEFAULT_OUT_MD = PROJECT_ROOT / "artifacts" / "peak_release_block_note_latest.md"
DEFAULT_OUT_JSON = PROJECT_ROOT / "artifacts" / "peak_release_block_note_latest.json"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _load_history(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
    except Exception:
        return []
    return []


def _recent_summary(history: list[dict[str, Any]], recent_days: int) -> dict[str, Any]:
    if recent_days <= 0:
        return {"window_days": 0, "records": 0}
    cutoff = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    rows: list[dict[str, Any]] = []
    for r in history:
        ts = str(r.get("ts", "") or "")
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            continue
        if dt >= cutoff - timedelta(days=recent_days - 1):
            rows.append(r)
    if not rows:
        return {"window_days": recent_days, "records": 0}
    gate_pass_days = sum(1 for r in rows if bool(r.get("strict_gate_pass", False)))
    step6_rates = [float(r.get("step6_hard_fail_rate", 0.0) or 0.0) for r in rows]
    strategy_pass_days = sum(
        1
        for r in rows
        if bool(r.get("strategy_impact_available", False)) and bool(r.get("strategy_impact_gate_pass", False))
    )
    return {
        "window_days": recent_days,
        "records": len(rows),
        "gate_pass_days": gate_pass_days,
        "step6_hard_fail_rate_avg": round(sum(step6_rates) / len(step6_rates), 6),
        "step6_hard_fail_rate_max": round(max(step6_rates), 6),
        "strategy_impact_pass_days": strategy_pass_days,
    }


def build_note(peak_gate: dict[str, Any], evidence: dict[str, Any], summary: dict[str, Any] | None = None) -> dict[str, Any]:
    level = str(peak_gate.get("level", "") or "").lower()
    env = str(peak_gate.get("release_env", "") or "")
    gap = int(peak_gate.get("gap_to_fail_days", 0) or 0)
    warn_days = int(peak_gate.get("warn_consecutive_days", 7) or 7)
    fail_days = int(peak_gate.get("fail_consecutive_days", 14) or 14)
    consec = int(peak_gate.get("consecutive_compliant_days", evidence.get("consecutive_compliant_days", 0)) or 0)
    ratio = float(peak_gate.get("compliance_ratio_pct", evidence.get("compliance_ratio_pct", 0.0)) or 0.0)
    peak_ready = bool(peak_gate.get("peak_ready", evidence.get("peak_ready", False)))
    if level == "pass":
        action = "允许推进发布，保持每日证据巡检。"
    elif level == "warn":
        action = "维持灰度发布，禁止扩大生产放量，持续补齐连续达标天数。"
    else:
        action = "阻断生产放量，优先修复门禁项并在下一周期复核。"
    title = f"峰值发布门禁说明（{level.upper() if level else 'UNKNOWN'}）"
    return {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "title": title,
        "level": level or "unknown",
        "release_env": env,
        "peak_ready": peak_ready,
        "consecutive_compliant_days": consec,
        "compliance_ratio_pct": round(ratio, 2),
        "warn_consecutive_days": warn_days,
        "fail_consecutive_days": fail_days,
        "gap_to_fail_days": gap,
        "recent_summary": summary or {},
        "action": action,
    }


def render_md(note: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# {note.get('title', '峰值发布门禁说明')}",
            "",
            f"> 生成时间: {note.get('generated_at', '?')}",
            "",
            "| 字段 | 值 |",
            "|---|---|",
            f"| level | {note.get('level', 'unknown')} |",
            f"| release_env | {note.get('release_env', '') or 'N/A'} |",
            f"| peak_ready | {'true' if bool(note.get('peak_ready', False)) else 'false'} |",
            f"| consecutive_compliant_days | {note.get('consecutive_compliant_days', 0)} |",
            f"| compliance_ratio_pct | {note.get('compliance_ratio_pct', 0.0)} |",
            f"| warn_consecutive_days | {note.get('warn_consecutive_days', 0)} |",
            f"| fail_consecutive_days | {note.get('fail_consecutive_days', 0)} |",
            f"| gap_to_fail_days | {note.get('gap_to_fail_days', 0)} |",
            "",
            "## 最近证据摘要",
            "",
            "| 字段 | 值 |",
            "|---|---|",
            f"| window_days | {(note.get('recent_summary', {}) or {}).get('window_days', 0)} |",
            f"| records | {(note.get('recent_summary', {}) or {}).get('records', 0)} |",
            f"| gate_pass_days | {(note.get('recent_summary', {}) or {}).get('gate_pass_days', 0)} |",
            f"| step6_hard_fail_rate_avg | {(note.get('recent_summary', {}) or {}).get('step6_hard_fail_rate_avg', 0.0)} |",
            f"| step6_hard_fail_rate_max | {(note.get('recent_summary', {}) or {}).get('step6_hard_fail_rate_max', 0.0)} |",
            f"| strategy_impact_pass_days | {(note.get('recent_summary', {}) or {}).get('strategy_impact_pass_days', 0)} |",
            "",
            "## 处置建议",
            "",
            f"- {note.get('action', '')}",
            "",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="生成峰值发布门禁阻断说明模板")
    parser.add_argument("--peak-gate", type=Path, default=DEFAULT_PEAK_GATE)
    parser.add_argument("--evidence", type=Path, default=DEFAULT_EVIDENCE)
    parser.add_argument("--trend-history", type=Path, default=DEFAULT_TREND_HISTORY)
    parser.add_argument("--recent-days", type=int, default=7)
    parser.add_argument("--out-md", type=Path, default=DEFAULT_OUT_MD)
    parser.add_argument("--out-json", type=Path, default=DEFAULT_OUT_JSON)
    args = parser.parse_args()

    peak_gate = _load_json(args.peak_gate)
    evidence = _load_json(args.evidence)
    history = _load_history(args.trend_history)
    summary = _recent_summary(history, max(1, args.recent_days))
    note = build_note(peak_gate, evidence, summary)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text(render_md(note), encoding="utf-8")
    args.out_json.write_text(json.dumps(note, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] peak release block note markdown: {args.out_md}")
    print(f"[OK] peak release block note json: {args.out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
