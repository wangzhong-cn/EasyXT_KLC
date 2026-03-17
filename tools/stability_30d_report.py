#!/usr/bin/env python3
"""
tools/stability_30d_report.py — 30天回归稳定性观测器

"放量前稳定性建设"阶段第一步：证明"修复有效且稳定"，不是一次性通过。

功能：
  1. 30天窗口内每日 strict_pass / P0_open_count / active_critical_high 趋势
  2. 周维度汇总（SLA 达成率、P0 收敛斜率）
  3. "门禁通过但业务异常"反例检测（读取 logs/business_anomaly_log.jsonl）
  4. 输出:
     - artifacts/stability_30d_latest.md   （人可读 Markdown 报告）
     - artifacts/stability_30d_latest.json （机器可读，供 dashboard 读取）

SLA 门槛：strict_pass 30天达成率 ≥ 95% 才符合放量前稳定性要求。

用法：
  python tools/stability_30d_report.py
  python tools/stability_30d_report.py --window-days 30
  python tools/stability_30d_report.py --out artifacts/my_report.md

"门禁通过但业务异常"反例日志（手工追加）：
  写入路径: logs/business_anomaly_log.jsonl
  格式示例（每行一条 JSON）：
    {"ts": "2026-03-10T08:00:00Z", "strategy": "双均线策略",
     "anomaly": "回撤超过15%但数据质量门禁未检出异常", "severity": "high"}
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
HISTORY_PATH = PROJECT_ROOT / "artifacts" / "p0_trend_history.json"
ANOMALY_LOG  = PROJECT_ROOT / "logs" / "business_anomaly_log.jsonl"
REPORT_DIR   = PROJECT_ROOT / "artifacts"


# ─────────────────────────────────────────────────────────────────────────────
# Anomaly 日志 Schema v1（固定结构，避免 dashboard 解析漂移）
# ─────────────────────────────────────────────────────────────────────────────

#: 允许的事件类型（可扩展，未知值会触发 _schema_warnings）
ANOMALY_EVENT_TYPES: frozenset[str] = frozenset({
    "drawdown_spike",   # 回撤突然变大
    "signal_miss",      # 信号明显缺失
    "data_gap",         # 数据空窗导致策略异常
    "latency_spike",    # 下单/数据延迟导致异常
    "position_error",   # 持仓/风控异常
    "unclassified",     # 未分类（默认）
})

#: 允许的严重度级别
ANOMALY_SEVERITIES: frozenset[str] = frozenset({"low", "medium", "high", "critical"})

# Canonical schema 字段说明（文档用）：
#   timestamp  : str  — ISO 8601 UTC（必填；旧格式兼容 "ts"）
#   symbol     : str  — 涉及的股票代码（可选）
#   strategy   : str  — 策略名称（必填）
#   event_type : str  — ANOMALY_EVENT_TYPES 之一（必填）
#   severity   : str  — ANOMALY_SEVERITIES  之一（必填）
#   context    : dict — {"description": str, "reporter": str, ...}（必填）


# ─────────────────────────────────────────────────────────────────────────────
# 字段兼容层：p0_trend_history.json 使用 P0_open/ach 和 P0_open_count/active_critical_high 两套命名
# ─────────────────────────────────────────────────────────────────────────────

def _p0(r: dict[str, Any]) -> int:
    return int(r.get("P0_open_count", r.get("P0_open", 0)) or 0)


def _ach(r: dict[str, Any]) -> int:
    return int(r.get("active_critical_high", r.get("ach", 0)) or 0)


def _gate(r: dict[str, Any]) -> bool:
    return bool(r.get("strict_gate_pass", r.get("strict_pass", False)))


def _ts(r: dict[str, Any]) -> datetime | None:
    raw = r.get("ts") or r.get("timestamp") or r.get("saved_at") or ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)  # naive → UTC
        return dt
    except Exception:
        return None


def _date_str(r: dict[str, Any]) -> str:
    ts = _ts(r)
    return ts.strftime("%Y-%m-%d") if ts else "?"


def _step6_sampled(r: dict[str, Any]) -> int:
    return int(r.get("step6_sampled", 0) or 0)


def _step6_fail_rate(r: dict[str, Any]) -> float:
    return float(r.get("step6_hard_fail_rate", 0.0) or 0.0)


def _canary_mode(r: dict[str, Any]) -> str:
    enabled = bool(r.get("canary_shadow_write_enabled", False))
    only = bool(r.get("canary_shadow_only", True))
    if not enabled:
        return "off"
    return "shadow_only" if only else "shadow_and_main"


# ─────────────────────────────────────────────────────────────────────────────
# 数据加载与预处理
# ─────────────────────────────────────────────────────────────────────────────

def _load_history() -> list[dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []
    try:
        return json.loads(HISTORY_PATH.read_text(encoding="utf-8")) or []
    except Exception:
        return []


def _window(records: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
    return [r for r in records if (_ts(r) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff]


def _dedupe_daily(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """同一天取最后一条（夜间多次扫描时取最新一轮）。"""
    seen: dict[str, dict[str, Any]] = {}
    for r in records:
        seen[_date_str(r)] = r
    return [seen[k] for k in sorted(seen)]


def _load_anomalies(window_days: int) -> list[dict[str, Any]]:
    """读取"门禁通过但业务异常"反例日志（文件不存在时返回空列表）。"""
    if not ANOMALY_LOG.exists():
        return []
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=window_days)
    anomalies: list[dict[str, Any]] = []
    try:
        for line in ANOMALY_LOG.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
                # 兼容旧格式："ts"→"timestamp"
                raw = ev.get("timestamp") or ev.get("ts", "")
                ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if ts >= cutoff:
                    anomalies.append(_validate_anomaly(ev))
            except Exception:
                continue
    except Exception:
        pass
    return anomalies


def _validate_anomaly(ev: dict[str, Any]) -> dict[str, Any]:
    """
    规范化并验证 anomaly 日志条目，向前兼容旧格式（ts/anomaly 字段）。
    验证失败时添加 _schema_warnings 字段，不丢弃记录。
    """
    out = dict(ev)
    warnings: list[str] = []

    # 字段规范化：旧格式 "ts" → "timestamp"
    if "timestamp" not in out and "ts" in out:
        out["timestamp"] = out.pop("ts")

    # 旧格式 "anomaly" → context.description
    if "context" not in out:
        desc     = out.pop("anomaly", "")
        reporter = out.pop("reporter", "manual")
        out["context"] = {"description": str(desc), "reporter": str(reporter)}
    elif not isinstance(out["context"], dict):
        out["context"] = {"description": str(out["context"])}

    # 必填字段检查
    if not out.get("timestamp"):
        warnings.append("缺少 timestamp 字段")
    if not out.get("strategy"):
        warnings.append("缺少 strategy 字段")

    # event_type：缺失时默认 unclassified；未知值记录警告但保留原值
    if not out.get("event_type"):
        out["event_type"] = "unclassified"
    elif out["event_type"] not in ANOMALY_EVENT_TYPES:
        warnings.append(f"未知 event_type={out['event_type']!r}，不在支持列表中")

    # severity 校验
    if not out.get("severity"):
        warnings.append("缺少 severity 字段")
    elif out["severity"] not in ANOMALY_SEVERITIES:
        warnings.append(
            f"无效 severity={out['severity']!r}，"
            f"应为 {sorted(ANOMALY_SEVERITIES)} 之一"
        )

    if warnings:
        out["_schema_warnings"] = warnings
        print(f"[ANOMALY_SCHEMA_WARN] {warnings}", file=sys.stderr)

    return out


# ─────────────────────────────────────────────────────────────────────────────
# 指标计算
# ─────────────────────────────────────────────────────────────────────────────

def _sla(records: list[dict[str, Any]]) -> str:
    if not records:
        return "N/A"
    passed = sum(1 for r in records if _gate(r))
    return f"{passed}/{len(records)} ({passed * 100 // len(records)}%)"


def _slope(records: list[dict[str, Any]], field_fn) -> str:
    vals = [field_fn(r) for r in records]
    if len(vals) < 2:
        return "数据不足"
    delta = vals[-1] - vals[0]
    if delta < 0:
        return f"↓ 收敛 {-delta}（{vals[0]}→{vals[-1]}）✓"
    elif delta == 0:
        return f"→ 持平 {vals[-1]}"
    else:
        return f"↑ 上升 {delta}（{vals[0]}→{vals[-1]}）⚠"


def _weekly_chunks(records: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """按每 7 条切分周维度块（最后不足 7 条也归入一组）。"""
    return [records[i:i + 7] for i in range(0, len(records), 7) if records[i:i + 7]]


# ─────────────────────────────────────────────────────────────────────────────
# 报告构建
# ─────────────────────────────────────────────────────────────────────────────

def _daily_table(records: list[dict[str, Any]]) -> str:
    lines = [
        "### 每日明细",
        "",
        "| 日期 | strict_pass | P0_open_count | active_critical_high | step6_sampled | step6_hard_fail_rate | canary_mode |",
        "|------|-------------|---------------|----------------------|---------------|----------------------|-------------|",
    ]
    for r in records:
        g = "✅" if _gate(r) else "❌"
        lines.append(
            f"| {_date_str(r)} | {g} | {_p0(r)} | {_ach(r)} | {_step6_sampled(r)} | "
            f"{_step6_fail_rate(r):.4f} | {_canary_mode(r)} |"
        )
    return "\n".join(lines)


def _weekly_table(records: list[dict[str, Any]]) -> str:
    chunks = _weekly_chunks(records)
    lines = [
        "### 周维度汇总",
        "",
        "| 周序 | 日期范围 | SLA（strict_pass） | P0 斜率 | active_ch 斜率 | step6采样均值 | step6失败率均值 |",
        "|------|----------|-------------------|---------|----------------|--------------|----------------|",
    ]
    for i, chunk in enumerate(chunks, 1):
        start = _date_str(chunk[0])
        end   = _date_str(chunk[-1])
        s6_avg = sum(_step6_sampled(r) for r in chunk) / len(chunk)
        s6_fail_avg = sum(_step6_fail_rate(r) for r in chunk) / len(chunk)
        lines.append(
            f"| W{i} | {start} ~ {end} | {_sla(chunk)} "
            f"| {_slope(chunk, _p0)} | {_slope(chunk, _ach)} "
            f"| {s6_avg:.2f} | {s6_fail_avg:.4f} |"
        )
    return "\n".join(lines)


def _anomaly_section(anomalies: list[dict[str, Any]]) -> str:
    if not anomalies:
        return (
            "### 门禁通过但业务异常（反例检测）\n\n"
            "> 检测周期内**无反例记录** ✅\n\n"
            "> 如需手工追加反例，向 `logs/business_anomaly_log.jsonl` 追加一行:\n"
            "> ```json\n"
            '> {"timestamp":"2026-03-10T08:00:00Z","strategy":"\u53cc\u5747\u7ebf\u7b56\u7565",\n'
            '> "event_type":"drawdown_spike","severity":"high",\n'
            '> "context":{"description":"\u8ff0\u8ff0\u5f02\u5e38\u60c5\u51b5","reporter":"manual"}}\n'
            "> ```"
        )
    lines = [
        "### 门禁通过但业务异常（反例检测）",
        "",
        f"> ⚠️ 本周期发现 **{len(anomalies)}** 条反例，需人工复核",
        "",
        "| 日期 | 策略 | 类型 | 描述 | 严重度 |",
        "|------|------|------|------|--------|",
    ]
    for ev in anomalies:
        ts    = (ev.get("timestamp") or ev.get("ts") or "?")[:10]
        strat = ev.get("strategy", "unknown")
        etype = ev.get("event_type", "unclassified")
        ctx   = ev.get("context", {})
        desc  = (ctx.get("description", "") if isinstance(ctx, dict) else str(ctx))[:55]
        sev   = ev.get("severity", "?")
        lines.append(f"| {ts} | {strat} | {etype} | {desc} | {sev} |")

    warn_count = sum(1 for ev in anomalies if "_schema_warnings" in ev)
    if warn_count:
        lines.append("")
        lines.append(f"> ⚠️ {warn_count} 条记录存在 schema 警告，请检查 stderr 输出。")

    return "\n".join(lines)


def build_report(
    records: list[dict[str, Any]],
    anomalies: list[dict[str, Any]],
    window_days: int,
    generated_at: str,
) -> str:
    if not records:
        return (
            "# 30天回归稳定性报告\n\n"
            f"> 生成时间: {generated_at}\n\n"
            "> **暂无数据** — 请先执行夜间巡检并运行 `python tools/p0_trend_update.py`。"
        )

    latest = records[-1]
    passed_days = sum(1 for r in records if _gate(r))
    sla_pct = passed_days * 100 // len(records)
    gate_emoji = "✅" if _gate(latest) else "❌"
    s6_worsen_days = _count_consecutive_step6_worsening(records)

    if sla_pct >= 95:
        conclusion = f"SLA = {sla_pct}%，系统运行稳定 ✅，符合放量前稳定性要求。"
    elif sla_pct >= 80:
        conclusion = f"SLA = {sla_pct}%，尚未达到 95% 放量门槛，需持续观测。⚠️"
    else:
        conclusion = f"SLA = {sla_pct}%，低于 80% 警戒线，禁止放量 ❌。"

    lines = [
        "# 30天回归稳定性报告",
        "",
        f"> 生成时间: {generated_at}  ",
        f"> 观测窗口: 最近 {window_days} 天（共 {len(records)} 条日均记录）",
        "",
        "## 当前状态（最新快照）",
        "",
        "| 指标 | 值 | 目标 |",
        "|------|----|------|",
        f"| strict_pass | {gate_emoji} | 每日 = true |",
        f"| P0_open_count | {_p0(latest)} | = 0 |",
        f"| active_critical_high | {_ach(latest)} | = 0 |",
        f"| step6_sampled | {_step6_sampled(latest)} | 采样命中 > 0 |",
        f"| step6_hard_fail_rate | {_step6_fail_rate(latest):.4f} | 越低越好 |",
        f"| canary_mode | {_canary_mode(latest)} | shadow_only / shadow_and_main / off |",
        f"| step6 连续恶化天数 | {s6_worsen_days} | 越低越好（建议 ≤ 1） |",
        f"| 30天 SLA 达成率 | {passed_days}/{len(records)} ({sla_pct}%) | ≥ 95% |",
        f"| 业务异常反例 | {len(anomalies)} 条 | = 0 |",
        "",
        f"**结论**: {conclusion}",
        "",
        _daily_table(records),
        "",
        _weekly_table(records),
        "",
        _anomaly_section(anomalies),
        "",
        "---",
        "",
        "> 数据来源: `artifacts/p0_trend_history.json`  ",
        "> 反例日志: `logs/business_anomaly_log.jsonl`  ",
        "> 下次更新: 下次夜间巡检后（每日 02:00 UTC）",
    ]
    return "\n".join(lines)


def build_json(
    records: list[dict[str, Any]],
    anomalies: list[dict[str, Any]],
    window_days: int,
    generated_at: str,
) -> dict[str, Any]:
    if not records:
        return {
            "generated_at": generated_at, "window_days": window_days,
            "record_count": 0, "sla_pct": 0, "sla_pass": False,
            "anomaly_count": len(anomalies),
        }
    passed = sum(1 for r in records if _gate(r))
    latest = records[-1]
    sla_pct = passed * 100 // len(records)
    s6_worsen_days = _count_consecutive_step6_worsening(records)
    return {
        "generated_at": generated_at,
        "window_days": window_days,
        "record_count": len(records),
        "strict_pass_latest": _gate(latest),
        "P0_open_count_latest": _p0(latest),
        "active_critical_high_latest": _ach(latest),
        "step6_sampled_latest": _step6_sampled(latest),
        "step6_hard_fail_rate_latest": _step6_fail_rate(latest),
        "canary_mode_latest": _canary_mode(latest),
        "sla_pct": sla_pct,
        "sla_pass": sla_pct >= 95,
        "anomaly_count": len(anomalies),
        "anomalies": anomalies,
        "consecutive_gate_fails": _count_consecutive_fails(records),
        "consecutive_step6_worsening_days": s6_worsen_days,
        "daily": [
            {
                "date": _date_str(r),
                "strict_pass": _gate(r),
                "P0_open_count": _p0(r),
                "active_critical_high": _ach(r),
                "step6_sampled": _step6_sampled(r),
                "step6_hard_fail_rate": _step6_fail_rate(r),
                "canary_mode": _canary_mode(r),
            }
            for r in records
        ],
    }


def _count_consecutive_fails(records: list[dict[str, Any]]) -> int:
    """从记录末尾往前数连续失败天数。"""
    count = 0
    for r in reversed(records):
        if not _gate(r):
            count += 1
        else:
            break
    return count


def _count_consecutive_step6_worsening(records: list[dict[str, Any]], eps: float = 1e-12) -> int:
    """从末尾向前统计 step6_hard_fail_rate 连续恶化天数。"""
    if len(records) < 2:
        return 0
    count = 0
    prev = _step6_fail_rate(records[-1])
    prev_sampled = _step6_sampled(records[-1])
    if prev_sampled <= 0:
        return 0
    for r in reversed(records[:-1]):
        cur = _step6_fail_rate(r)
        cur_sampled = _step6_sampled(r)
        if cur_sampled <= 0:
            break
        if prev > cur + eps:
            count += 1
            prev = cur
            continue
        break
    return count


# ─────────────────────────────────────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    step6_warn_default = int(os.environ.get("EASYXT_STEP6_WARN_DAYS", "3") or 3)
    step6_fail_default = int(os.environ.get("EASYXT_STEP6_FAIL_DAYS", "5") or 5)
    parser = argparse.ArgumentParser(description="30天回归稳定性观测器")
    parser.add_argument("--window-days",        type=int, default=30, help="观测窗口天数（默认 30）")
    parser.add_argument("--consecutive-fail-days", type=int, default=2, dest="consecutive_fail_days",
                        help="CI 阻断门槛：连续几天低于阈值才触发 exit 1（默认 2，减少单日噪声误杀）")
    parser.add_argument("--step6-warn-days", type=int, default=step6_warn_default, dest="step6_warn_days")
    parser.add_argument("--step6-fail-days", type=int, default=step6_fail_default, dest="step6_fail_days")
    parser.add_argument("--out",      type=pathlib.Path, default=None, help="Markdown 输出路径")
    parser.add_argument("--json-out", type=pathlib.Path, default=None, help="JSON 输出路径")
    args = parser.parse_args(argv)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    md_out   = args.out      or (REPORT_DIR / "stability_30d_latest.md")
    json_out = args.json_out or (REPORT_DIR / "stability_30d_latest.json")

    history   = _load_history()
    windowed  = _window(history, args.window_days)
    records   = _dedupe_daily(windowed)
    anomalies = _load_anomalies(args.window_days)

    generated_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    md_content   = build_report(records, anomalies, args.window_days, generated_at)
    json_content = build_json(records, anomalies, args.window_days, generated_at)

    # 将 consecutive_fail_threshold 写入 JSON 便于 dashboard 消费
    json_content["consecutive_fail_threshold"] = args.consecutive_fail_days
    json_content["step6_warn_days"] = args.step6_warn_days
    json_content["step6_fail_days"] = args.step6_fail_days

    md_out.write_text(md_content, encoding="utf-8")
    json_out.write_text(json.dumps(json_content, indent=2, ensure_ascii=False), encoding="utf-8")

    sla               = json_content.get("sla_pct", 0)
    p0                = json_content.get("P0_open_count_latest", "?")
    ach               = json_content.get("active_critical_high_latest", "?")
    n                 = json_content.get("record_count", 0)
    gate              = "✅" if json_content.get("strict_pass_latest", False) else "❌"
    anom              = json_content.get("anomaly_count", 0)
    consec_fails      = json_content.get("consecutive_gate_fails", 0)
    s6_worsen_days    = json_content.get("consecutive_step6_worsening_days", 0)

    print(f"[稳定性报告] 窗口={args.window_days}d 记录={n}条 gate={gate} "
          f"P0={p0} ACH={ach} SLA={sla}% 反例={anom}条 连续失败={consec_fails}天 "
          f"Step6连续恶化={s6_worsen_days}天")
    print(f"  → Markdown: {md_out}")
    print(f"  → JSON:     {json_out}")

    sla_failing       = sla < 80 and n > 0
    consecutive_block = consec_fails >= args.consecutive_fail_days
    step6_warn = s6_worsen_days >= args.step6_warn_days and n > 0
    step6_fail = s6_worsen_days >= args.step6_fail_days and n > 0
    if sla_failing and consecutive_block:
        print(
            f"[CI BLOCK] SLA={sla}% < 80% 且连续 {consec_fails} 天失败 "
            f"(≥ 阈值 {args.consecutive_fail_days})，触发 CI 阻断",
            file=sys.stderr,
        )
    elif sla_failing:
        print(
            f"[CI WARN] SLA={sla}% < 80% 但仅连续 {consec_fails} 天 "
            f"(< 阈值 {args.consecutive_fail_days})，暂不阻断",
            file=sys.stderr,
        )
    if step6_fail:
        print(
            f"[CI BLOCK] Step6 连续恶化 {s6_worsen_days} 天 "
            f"(≥ 阈值 {args.step6_fail_days})，触发 CI 阻断",
            file=sys.stderr,
        )
    elif step6_warn:
        print(
            f"[CI WARN] Step6 连续恶化 {s6_worsen_days} 天 "
            f"(≥ 告警阈值 {args.step6_warn_days})，请关注数据质量趋势",
            file=sys.stderr,
        )
    if (sla_failing and consecutive_block) or step6_fail:
        return 1
    return 0
