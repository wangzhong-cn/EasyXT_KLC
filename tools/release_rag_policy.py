from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote, unquote


def period_validation_summary(
    stability_evidence: dict[str, Any] | None,
    peak_release_gate: dict[str, Any] | None,
) -> tuple[int, int]:
    gate = peak_release_gate if isinstance(peak_release_gate, dict) else {}
    evidence = stability_evidence if isinstance(stability_evidence, dict) else {}
    period = evidence.get("period_validation") if isinstance(evidence.get("period_validation"), dict) else {}
    failed = int(gate.get("period_validation_failed_items", period.get("failed_rows", 0)) or 0)
    max_allowed = int(gate.get("max_period_validation_failed_items", 0) or 0)
    return failed, max_allowed


def period_validation_status(failed: int, max_allowed: int) -> str:
    if failed <= max_allowed:
        return "✅ PASS"
    if failed <= max_allowed + 2:
        return f"🟡 WARN（{failed}>{max_allowed}）"
    return f"❌ FAIL（{failed}>{max_allowed}）"


def period_validation_tag(failed: int, max_allowed: int) -> str:
    return f"PV[{period_validation_status(failed, max_allowed)}]"


def period_validation_detail_tag(
    failed: int,
    max_allowed: int,
    *,
    message: str = "",
    action: str = "",
) -> str:
    msg = str(message or "").replace("\n", " ").strip() or "N/A"
    act = str(action or "").replace("\n", " ").strip() or "N/A"
    msg_escaped = quote(msg, safe="")
    act_escaped = quote(act, safe="")
    return (
        f"PV_DETAIL[v=1|pv={period_validation_tag(failed, max_allowed)}"
        f"|failed={int(failed)}|max={int(max_allowed)}|msg={msg_escaped}|action={act_escaped}]"
    )


def header_rag_status(
    strict_pass: bool,
    p0_open: int,
    ach: int,
    peak_level: str,
    period_failed: int,
    period_max_allowed: int,
) -> str:
    risk_count = 0
    if not strict_pass:
        risk_count += 1
    if p0_open > 0:
        risk_count += 1
    if ach > 0:
        risk_count += 1
    if str(peak_level).lower() == "fail":
        risk_count += 1
    if period_failed > period_max_allowed:
        risk_count += 1
    if risk_count == 0:
        return "🟢 GREEN"
    if risk_count <= 2:
        return "🟡 YELLOW"
    return "🔴 RED"


def rag_level(rag_status: str) -> str:
    text = str(rag_status or "").upper()
    if "GREEN" in text:
        return "GREEN"
    if "YELLOW" in text:
        return "YELLOW"
    if "RED" in text:
        return "RED"
    return "UNKNOWN"


def rag_emoji(rag_status: str) -> str:
    level = rag_level(rag_status)
    if level == "GREEN":
        return "🟢"
    if level == "YELLOW":
        return "🟡"
    if level == "RED":
        return "🔴"
    return "⚪"


def rag_badge(rag_status: str) -> str:
    level = rag_level(rag_status)
    return f"{rag_emoji(rag_status)} {level}"


def rag_tag(rag_status: str) -> str:
    return f"RAG[{rag_badge(rag_status)}]"


def gate_detail_tag(
    rag_status: str,
    failed: int,
    max_allowed: int,
    *,
    message: str = "",
    action: str = "",
) -> str:
    return f"GATE_DETAIL[v=1|rag={rag_tag(rag_status)}|pv_detail={period_validation_detail_tag(failed, max_allowed, message=message, action=action)}]"


def parse_period_validation_detail_tag(tag: str) -> dict[str, Any]:
    text = str(tag or "")
    m = re.fullmatch(
        r"PV_DETAIL\[v=(?P<v>\d+)\|pv=(?P<pv>PV\[.*?\])\|failed=(?P<failed>-?\d+)\|max=(?P<max>-?\d+)\|msg=(?P<msg>.*?)\|action=(?P<action>.*?)\]",
        text,
    )
    if not m:
        return {"ok": False, "error": "invalid_pv_detail_format", "raw": text}
    return {
        "ok": True,
        "version": int(m.group("v")),
        "pv": m.group("pv"),
        "failed": int(m.group("failed")),
        "max": int(m.group("max")),
        "msg": unquote(m.group("msg")),
        "action": unquote(m.group("action")),
        "raw": text,
    }


def parse_gate_detail_tag(tag: str) -> dict[str, Any]:
    text = str(tag or "")
    m = re.fullmatch(
        r"GATE_DETAIL\[v=(?P<v>\d+)\|rag=(?P<rag>RAG\[.*?\])\|pv_detail=(?P<pv_detail>PV_DETAIL\[.*\])\]",
        text,
    )
    if not m:
        return {"ok": False, "error": "invalid_gate_detail_format", "raw": text}
    pv_detail_text = m.group("pv_detail")
    pv_parsed = parse_period_validation_detail_tag(pv_detail_text)
    if not bool(pv_parsed.get("ok", False)):
        return {
            "ok": False,
            "error": "invalid_pv_detail_in_gate_detail",
            "version": int(m.group("v")),
            "rag": m.group("rag"),
            "pv_detail_raw": pv_detail_text,
            "pv_detail": pv_parsed,
            "raw": text,
        }
    return {
        "ok": True,
        "version": int(m.group("v")),
        "rag": m.group("rag"),
        "pv_detail": pv_parsed,
        "raw": text,
    }


def rag_color(rag_status: str) -> str:
    level = rag_level(rag_status)
    if level == "GREEN":
        return "#00aa66"
    if level == "YELLOW":
        return "#ef6c00"
    if level == "RED":
        return "#d32f2f"
    return "#999999"
