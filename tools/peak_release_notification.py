from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PEAK_GATE = PROJECT_ROOT / "artifacts" / "peak_release_gate_latest.json"
DEFAULT_BLOCK_NOTE = PROJECT_ROOT / "artifacts" / "peak_release_block_note_latest.json"
DEFAULT_OUT_MD = PROJECT_ROOT / "artifacts" / "peak_release_notification_latest.md"
DEFAULT_OUT_JSON = PROJECT_ROOT / "artifacts" / "peak_release_notification_latest.json"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _build_fixed_templates(
    *,
    release_env: str,
    level: str,
    gap_days: int,
    consec: int,
    fail_days: int,
    ratio: float,
) -> tuple[str, dict[str, str]]:
    preprod_warn = (
        f"【预发预警】峰值发布门禁预警：当前连续达标 {consec}/{fail_days} 天，"
        f"gap={gap_days} 天，达标率={ratio:.2f}%。维持灰度，不扩大生产放量。"
    )
    prod_block = (
        f"【生产阻断】峰值发布门禁阻断：当前连续达标 {consec}/{fail_days} 天，"
        f"gap={gap_days} 天，达标率={ratio:.2f}%。立即停止放量并按阻断说明修复。"
    )
    templates = {"preprod_warn": preprod_warn, "prod_block": prod_block}
    if release_env == "prod" and level == "fail":
        selected = "prod_block"
    else:
        selected = "preprod_warn"
    return selected, templates


def build_notification(peak_gate: dict[str, Any], block_note: dict[str, Any]) -> dict[str, Any]:
    level = str(peak_gate.get("level", block_note.get("level", "unknown")) or "unknown").lower()
    release_env = str(peak_gate.get("release_env", block_note.get("release_env", "")) or "")
    gap_days = int(peak_gate.get("gap_to_fail_days", block_note.get("gap_to_fail_days", 0)) or 0)
    consec = int(peak_gate.get("consecutive_compliant_days", block_note.get("consecutive_compliant_days", 0)) or 0)
    fail_days = int(peak_gate.get("fail_consecutive_days", block_note.get("fail_consecutive_days", 14)) or 14)
    ratio = float(peak_gate.get("compliance_ratio_pct", block_note.get("compliance_ratio_pct", 0.0)) or 0.0)
    summary = block_note.get("recent_summary", {}) if isinstance(block_note.get("recent_summary"), dict) else {}

    if level == "pass":
        headline = "峰值发布门禁通过"
    elif level == "warn":
        headline = "峰值发布门禁预警"
    elif level == "fail":
        headline = "峰值发布门禁阻断"
    else:
        headline = "峰值发布门禁状态未知"
    title = f"{headline}（{release_env or 'N/A'}）"

    action = str(block_note.get("action", "") or "")
    selected_key, fixed_templates = _build_fixed_templates(
        release_env=release_env,
        level=level,
        gap_days=gap_days,
        consec=consec,
        fail_days=fail_days,
        ratio=ratio,
    )
    msg = (
        f"{headline} | env={release_env or 'N/A'} | level={level} | "
        f"consecutive={consec}/{fail_days} | gap={gap_days}d | ratio={ratio:.2f}%"
    )
    email_subject = f"[{release_env or 'N/A'}] {headline}"
    email_body = "\n".join(
        [
            title,
            "",
            f"- level: {level}",
            f"- consecutive_compliant_days: {consec}",
            f"- fail_consecutive_days: {fail_days}",
            f"- gap_to_fail_days: {gap_days}",
            f"- compliance_ratio_pct: {ratio:.2f}",
            f"- action: {action}",
            f"- recent_records: {summary.get('records', 0)}",
            f"- recent_gate_pass_days: {summary.get('gate_pass_days', 0)}",
        ]
    )
    return {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "title": title,
        "level": level,
        "release_env": release_env,
        "consecutive_compliant_days": consec,
        "fail_consecutive_days": fail_days,
        "gap_to_fail_days": gap_days,
        "compliance_ratio_pct": round(ratio, 2),
        "message": msg,
        "email_subject": email_subject,
        "email_body": email_body,
        "block_note_action": action,
        "selected_template_key": selected_key,
        "fixed_templates": fixed_templates,
        "selected_message": fixed_templates.get(selected_key, ""),
    }


def render_md(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# 峰值发布门禁通知模板",
            "",
            f"> 生成时间: {payload.get('generated_at', '?')}",
            "",
            "| 字段 | 值 |",
            "|---|---|",
            f"| title | {payload.get('title', '')} |",
            f"| level | {payload.get('level', '')} |",
            f"| release_env | {payload.get('release_env', '') or 'N/A'} |",
            f"| consecutive_compliant_days | {payload.get('consecutive_compliant_days', 0)} |",
            f"| fail_consecutive_days | {payload.get('fail_consecutive_days', 0)} |",
            f"| gap_to_fail_days | {payload.get('gap_to_fail_days', 0)} |",
            f"| compliance_ratio_pct | {payload.get('compliance_ratio_pct', 0.0)} |",
            "",
            "## 飞书/IM 消息",
            "",
            f"- {payload.get('message', '')}",
            "",
            "## 固定文案（直接可发）",
            "",
            f"- selected_template_key: {payload.get('selected_template_key', '')}",
            f"- selected_message: {payload.get('selected_message', '')}",
            f"- preprod_warn: {(payload.get('fixed_templates', {}) or {}).get('preprod_warn', '')}",
            f"- prod_block: {(payload.get('fixed_templates', {}) or {}).get('prod_block', '')}",
            "",
            "## 邮件主题",
            "",
            f"- {payload.get('email_subject', '')}",
            "",
            "## 邮件正文",
            "",
            payload.get("email_body", ""),
            "",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="生成峰值发布门禁通知模板")
    parser.add_argument("--peak-gate", type=Path, default=DEFAULT_PEAK_GATE)
    parser.add_argument("--block-note", type=Path, default=DEFAULT_BLOCK_NOTE)
    parser.add_argument("--out-md", type=Path, default=DEFAULT_OUT_MD)
    parser.add_argument("--out-json", type=Path, default=DEFAULT_OUT_JSON)
    args = parser.parse_args()

    peak_gate = _load_json(args.peak_gate)
    block_note = _load_json(args.block_note)
    payload = build_notification(peak_gate, block_note)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text(render_md(payload), encoding="utf-8")
    args.out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] peak release notification markdown: {args.out_md}")
    print(f"[OK] peak release notification json: {args.out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
