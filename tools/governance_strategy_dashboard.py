#!/usr/bin/env python3
"""
tools/governance_strategy_dashboard.py — 治理-业务双轨联动看板

"放量前稳定性建设"阶段第三步：避免治理与业务割裂，让"治理服务收益"可量化。

双轨联动分析：
  1. 数据质量波动 vs 策略回撤变化
     数据质量指标（ACH / P0_open）变动↑ ↔ Stage 1 通过率↓？
     若相关，说明数据治理直接影响策略可发布能力。

  2. allowlist 触碰 PR 数 vs 发布效率
     allowlist 触碰增加 → Stage 1 完成率是否同步下降？
     若相关，说明技术债清理直接提升发布速度。

数据来源：
  - 治理指标: artifacts/p0_trend_history.json
  - 策略指标: strategies/results/stage1_*.json
  - PR触碰记录: artifacts/p0_allowlist_touch_events.json（可选）

输出：
  artifacts/governance_strategy_dashboard.md

用法：
  python tools/governance_strategy_dashboard.py
  python tools/governance_strategy_dashboard.py --out artifacts/my_dashboard.md
"""
from __future__ import annotations

import argparse
import json
import pathlib
from datetime import datetime, timezone
from typing import Any

PROJECT_ROOT  = pathlib.Path(__file__).parent.parent
HISTORY_PATH  = PROJECT_ROOT / "artifacts" / "p0_trend_history.json"
RESULTS_DIR   = PROJECT_ROOT / "strategies" / "results"
REPORT_DIR    = PROJECT_ROOT / "artifacts"
TOUCH_EVENTS  = PROJECT_ROOT / "artifacts" / "p0_allowlist_touch_events.json"
RECON_LATEST  = PROJECT_ROOT / "artifacts" / "source_reconciliation_latest.json"
STRATEGY_IMPACT_LATEST = PROJECT_ROOT / "artifacts" / "strategy_impact_latest.json"
STABILITY_EVIDENCE_LATEST = PROJECT_ROOT / "artifacts" / "stability_evidence_30d.json"
PEAK_RELEASE_GATE_LATEST = PROJECT_ROOT / "artifacts" / "peak_release_gate_latest.json"


# ─────────────────────────────────────────────────────────────────────────────
# 字段兼容层（与 stability_30d_report.py 保持一致）
# ─────────────────────────────────────────────────────────────────────────────

def _p0(r: dict[str, Any]) -> int:
    return int(r.get("P0_open_count", r.get("P0_open", 0)) or 0)


def _ach(r: dict[str, Any]) -> int:
    return int(r.get("active_critical_high", r.get("ach", 0)) or 0)


def _gate(r: dict[str, Any]) -> bool:
    return bool(r.get("strict_gate_pass", r.get("strict_pass", False)))


def _date_str(r: dict[str, Any]) -> str:
    raw = r.get("ts") or r.get("timestamp") or r.get("saved_at") or ""
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except Exception:
        return "?"


# ─────────────────────────────────────────────────────────────────────────────
# 数据加载
# ─────────────────────────────────────────────────────────────────────────────

def _load_history() -> list[dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []
    try:
        return json.loads(HISTORY_PATH.read_text(encoding="utf-8")) or []
    except Exception:
        return []


def _load_stage1_results() -> list[dict[str, Any]]:
    if not RESULTS_DIR.exists():
        return []
    results = []
    for f in sorted(RESULTS_DIR.glob("stage1_*.json")):
        try:
            results.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            continue
    return results


def _load_touch_events() -> list[dict[str, Any]]:
    if not TOUCH_EVENTS.exists():
        return []
    try:
        return json.loads(TOUCH_EVENTS.read_text(encoding="utf-8")) or []
    except Exception:
        return []


def _load_reconciliation_latest() -> dict[str, Any] | None:
    if not RECON_LATEST.exists():
        return None
    try:
        return json.loads(RECON_LATEST.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_strategy_impact_latest(path: pathlib.Path | None = None) -> dict[str, Any] | None:
    p = path or STRATEGY_IMPACT_LATEST
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_stability_evidence_latest(path: pathlib.Path | None = None) -> dict[str, Any] | None:
    p = path or STABILITY_EVIDENCE_LATEST
    if not p.exists():
        return None
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _load_peak_release_gate_latest(path: pathlib.Path | None = None) -> dict[str, Any] | None:
    p = path or PEAK_RELEASE_GATE_LATEST
    if not p.exists():
        return None
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _confidence_label(n: int) -> str:
    """基于 Stage 1 样本数量返回置信度标签（样本过少时联动分析结论不可靠）。"""
    if n == 0:
        return "⚪ 无数据（结论不可用）"
    if n < 3:
        return f"🔴 低置信度（样本仅 {n} 条，慎用结论）"
    if n < 10:
        return f"🟡 中置信度（样本 {n} 条，趋势初步可见）"
    return f"🟢 高置信度（样本 {n} 条，结论可信）"


# ─────────────────────────────────────────────────────────────────────────────
# 联动分析 1：数据质量波动 vs 策略回撤
# ─────────────────────────────────────────────────────────────────────────────

def _section_quality_vs_drawdown(
    history: list[dict[str, Any]],
    stage1_results: list[dict[str, Any]],
) -> str:
    lines: list[str] = [
        "## 联动分析 1：数据质量波动 vs 策略回撤",
        "",
    ]

    if not history:
        return "\n".join(lines + ["> 暂无治理数据 — 请先运行夜间巡检。"])

    # 治理趋势表（最近 14 条）
    lines += [
        "### 治理指标（最近 14 条日均记录）",
        "",
        "| 日期 | strict_pass | P0_open | active_ch |",
        "|------|-------------|---------|-----------|",
    ]
    # 去重：同一天取最后一条
    seen: dict[str, dict[str, Any]] = {}
    for r in history:
        seen[_date_str(r)] = r
    deduped = [seen[k] for k in sorted(seen)]
    for r in deduped[-14:]:
        g = "✅" if _gate(r) else "❌"
        lines.append(f"| {_date_str(r)} | {g} | {_p0(r)} | {_ach(r)} |")

    lines.append("")

    if not stage1_results:
        lines += [
            "### 策略数据（尚未就绪）",
            "",
            "> **操作**: 运行以下命令完成第一条策略的 Stage 1 评测：",
            "> ```bash",
            "> python strategies/stage1_pipeline.py \\",
            '>     --strategy 双均线策略 --symbol 000001.SZ \\',
            '>     --start 2019-01-01 --end 2025-12-31 \\',
            '>     --oos-split 2023-01-01',
            "> ```",
            "",
            "> 完成后再运行 `python tools/governance_strategy_dashboard.py` 刷新此看板。",
        ]
        return "\n".join(lines)

    # 策略指标汇总表
    lines += [
        "### Stage 1 策略评测结果",
        "",
        "| 策略 | 运行日期 | Pass | 年化% | 夏普 | 最大回撤% | OOS比 | 参数敏感% |",
        "|------|----------|------|-------|------|-----------|-------|-----------|",
    ]
    for r in stage1_results:
        s_pass  = "✅" if r.get("stage1_pass", False) else "❌"
        bm      = r.get("backtest_metrics", {})
        io      = r.get("in_out_sample", {})
        sens    = r.get("param_sensitivity", {})
        lines.append(
            f"| {r.get('strategy','?')} | {r.get('run_date','?')} | {s_pass} "
            f"| {bm.get('annualized_return_pct','?')} "
            f"| {bm.get('sharpe_ratio','?')} "
            f"| {bm.get('max_drawdown_pct','?')} "
            f"| {io.get('oos_ratio','?')} "
            f"| {sens.get('max_change_pct','?')} |"
        )

    lines.append("")

    # 定性联动分析
    ach_stable     = all(_ach(r) == 0 for r in deduped[-30:])
    gate_stable    = all(_gate(r) for r in deduped[-30:])
    any_s1_fail    = any(not r.get("stage1_pass", True) for r in stage1_results)
    all_s1_pass    = stage1_results and not any_s1_fail

    lines.append("### 联动定性分析")
    lines.append(f"> {_confidence_label(len(stage1_results))}")
    lines.append("")
    if ach_stable and gate_stable and all_s1_pass:
        lines.append(
            "✅ **数据质量持续稳定期间，Stage 1 策略通过率 100%**  \n"
            "数据治理对策略可发布能力形成正向支撑，核心逻辑成立。"
        )
    elif ach_stable and any_s1_fail:
        lines.append(
            "⚠️ **数据质量稳定，但 Stage 1 存在未通过项**  \n"
            "策略失败原因非数据质量问题，请检查参数敏感性或 OOS 比率。"
        )
    elif not ach_stable and any_s1_fail:
        lines.append(
            "🔴 **数据质量波动期间出现 Stage 1 未通过** — 强相关信号，需排查数据修复优先级。"
        )
    else:
        lines.append(
            "📊 **指标积累中** — 随记录增加，"
            "此处将自动呈现数据质量↔策略表现的定量相关性。"
        )

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 联动分析 2：allowlist 触碰 PR 数 vs 发布效率
# ─────────────────────────────────────────────────────────────────────────────

def _section_allowlist_vs_deploy(
    touch_events: list[dict[str, Any]],
    stage1_results: list[dict[str, Any]],
) -> str:
    touch_prs    = len({ev.get("pr") for ev in touch_events if ev.get("pr")})
    s1_done      = sum(1 for r in stage1_results if r.get("stage1_pass", False))
    s1_total     = len(stage1_results)
    deploy_rate  = f"{s1_done}/{s1_total}" if s1_total else "0/0（尚未运行 Stage 1）"

    lines: list[str] = [
        "## 联动分析 2：allowlist 触碰 PR 数 vs 发布效率",
        "",
        f"> {_confidence_label(s1_total)}",
        "",
        "| 指标 | 值 | 目标 |",
        "|------|----|------|",
        f"| allowlist 触碰 PR 数（历史累计） | {touch_prs} | 趋势下降 |",
        f"| Stage 1 通过率 | {deploy_rate} | = 100% |",
        "",
    ]

    if touch_prs == 0 and s1_done == s1_total and s1_total > 0:
        lines.append(
            "✅ **无 allowlist 触碰 + Stage 1 全通过** — 技术债清理与发布效率双优。"
        )
    elif touch_prs > 5:
        lines.append(
            f"⚠️ **allowlist 触碰 PR 累计 {touch_prs} 个** — 技术债压力偏高，"
            "建议优先消减 allowlist 条目以降低发布摩擦。  \n"
            "参考命令: `python tools/p0_gate_check.py --summary --verbose | grep allowlist`"
        )
    elif s1_total == 0:
        lines.append(
            "> 策略尚未完成 Stage 1 评测，发布效率暂无数据。  \n"
            "> 运行 Stage 1 后此处将呈现 PR 触碰频率 vs 发布周期的定量关系。"
        )
    else:
        lines.append(
            "📊 **指标积累中** — 随 Stage 1 数据增加，此处将形成量化关联分析。"
        )

    return "\n".join(lines)


def _section_source_reconciliation(recon: dict[str, Any] | None) -> str:
    lines: list[str] = [
        "## 联动分析 3：双源离线对账健康度（QMT vs AKShare）",
        "",
    ]
    if not recon:
        lines.append("> 暂无对账数据 — 运行 `python tools/data_reconciliation_audit.py` 生成后自动展示。")
        return "\n".join(lines)

    summary = recon.get("summary", {})
    total = int(summary.get("total_symbols", 0) or 0)
    passed = int(summary.get("passed_symbols", 0) or 0)
    failed = int(summary.get("failed_symbols", 0) or 0)
    qmt_ok = bool(recon.get("qmt_available", False))
    ak_ok = bool(recon.get("akshare_available", False))
    pass_rate = (passed / total * 100.0) if total > 0 else 0.0

    lines += [
        "| 指标 | 值 |",
        "|------|----|",
        f"| QMT 可用性 | {'✅' if qmt_ok else '❌'} |",
        f"| AKShare 可用性 | {'✅' if ak_ok else '❌'} |",
        f"| 对账通过率 | {passed}/{total} ({pass_rate:.1f}%) |",
        f"| 失败标的数 | {failed} |",
        "",
    ]

    failures = [x for x in recon.get("results", []) if not x.get("pass_reconciliation", False)]
    if failures:
        lines.append("### 失败标的（最多 10 条）")
        lines.append("")
        lines.append("| Symbol | Overlap | Close P95 | Volume P95 |")
        lines.append("|---|---:|---:|---:|")
        for item in failures[:10]:
            cp95 = "-" if item.get("close_rel_p95") is None else f"{float(item['close_rel_p95']):.6f}"
            vp95 = "-" if item.get("volume_rel_p95") is None else f"{float(item['volume_rel_p95']):.6f}"
            lines.append(
                f"| {item.get('symbol','?')} | {float(item.get('overlap_ratio', 0.0)):.4f} | {cp95} | {vp95} |"
            )
        lines.append("")
    return "\n".join(lines)


def _section_strategy_impact(impact: dict[str, Any] | None) -> str:
    lines: list[str] = [
        "## 联动分析 4：策略影响门禁（ΔR / ΔMDD / Sharpe_sign）",
        "",
    ]
    if not impact:
        lines.append("> 暂无策略影响数据 — 夜间治理任务将自动产出 `artifacts/strategy_impact_latest.json`。")
        return "\n".join(lines)
    available = bool(impact.get("available", False))
    gate_pass = bool(impact.get("gate_pass", True))
    lines += [
        "| 指标 | 值 |",
        "|------|----|",
        f"| available | {'✅' if available else 'N/A'} |",
        f"| gate_pass | {'✅ PASS' if gate_pass else '❌ FAIL'} |",
    ]
    if not available:
        reason = str(impact.get("reason", "unknown"))
        lines += ["", f"> 策略影响门禁当前不可评估，原因: `{reason}`"]
        return "\n".join(lines)
    d = impact.get("delta", {}) if isinstance(impact.get("delta"), dict) else {}
    d_ret = float(d.get("annualized_return_pct", 0.0) or 0.0)
    d_mdd = float(d.get("max_drawdown_pct", 0.0) or 0.0)
    sflip = bool(d.get("sharpe_sign_changed", False))
    lines += [
        f"| ΔR（年化收益百分点） | {d_ret:+.2f} |",
        f"| ΔMDD（百分点） | {d_mdd:+.2f} |",
        f"| Sharpe_sign 翻转 | {'⚠️ 是' if sflip else '✅ 否'} |",
        "",
        "| 检查项 | 结果 |",
        "|------|------|",
    ]
    checks = impact.get("checks", {}) if isinstance(impact.get("checks"), dict) else {}
    lines.append(f"| ΔR 阈值门禁 | {'✅' if bool(checks.get('delta_return_pass', False)) else '❌'} |")
    lines.append(f"| ΔMDD 阈值门禁 | {'✅' if bool(checks.get('delta_mdd_pass', False)) else '❌'} |")
    lines.append(f"| Sharpe_sign 门禁 | {'✅' if bool(checks.get('sharpe_sign_pass', False)) else '❌'} |")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 主体：拼装完整看板
# ─────────────────────────────────────────────────────────────────────────────

def build_dashboard(
    history: list[dict[str, Any]],
    stage1_results: list[dict[str, Any]],
    touch_events: list[dict[str, Any]],
    recon: dict[str, Any] | None,
    strategy_impact: dict[str, Any] | None,
    stability_evidence: dict[str, Any] | None,
    peak_release_gate: dict[str, Any] | None,
    generated_at: str,
) -> str:
    latest_gate = _gate(history[-1]) if history else False
    latest_p0   = _p0(history[-1])   if history else "?"
    latest_ach  = _ach(history[-1])  if history else "?"
    s1_pass_cnt = sum(1 for r in stage1_results if r.get("stage1_pass", False))
    s1_total    = len(stage1_results)
    peak_level = str((peak_release_gate or {}).get("level", "") or "").lower()
    peak_gap_days = int((peak_release_gate or {}).get("gap_to_fail_days", 0) or 0)
    release_env = str((peak_release_gate or {}).get("release_env", "") or "").strip()
    if peak_level == "":
        peak_ready = bool((stability_evidence or {}).get("peak_ready", False))
        peak_consec_days = int((stability_evidence or {}).get("consecutive_compliant_days", 0) or 0)
        peak_gap_days = max(0, 14 - peak_consec_days)
        if peak_ready:
            peak_level = "pass"
        elif peak_consec_days >= 7:
            peak_level = "warn"
        else:
            peak_level = "fail"
    if peak_level == "pass":
        peak_status = "✅ PASS"
    elif peak_level == "warn":
        peak_status = f"🟡 WARN（gap {peak_gap_days}d）"
    elif peak_level == "fail":
        peak_status = f"❌ FAIL（gap {peak_gap_days}d）"
    else:
        peak_status = "N/A"
    if release_env:
        peak_status = f"{peak_status} @ {release_env}"

    header = "\n".join([
        "# 治理-业务双轨联动看板",
        "",
        f"> 生成时间: {generated_at}  ",
        f"| 治理记录: {len(history)} 条 ｜ Stage 1 结果: {s1_total} 条 ｜ {_confidence_label(s1_total)}",
        "",
        "## 当前红绿板（一眼判断）",
        "",
        "| 轨道 | 指标 | 状态 |",
        "|------|------|------|",
        f"| 治理轨 | strict_pass | {'✅ PASS' if latest_gate else '❌ FAIL'} |",
        f"| 治理轨 | P0_open_count | {'✅ 0' if latest_p0 == 0 else f'❌ {latest_p0}'} |",
        f"| 治理轨 | active_critical_high | {'✅ 0' if latest_ach == 0 else f'⚠️ {latest_ach}'} |",
        f"| 策略轨 | Stage 1 通过率 | "
        f"{'✅' if s1_pass_cnt == s1_total and s1_total > 0 else '⏳'} "
        f"{s1_pass_cnt}/{s1_total} |",
        f"| 峰值轨 | peak_release_gate(SSOT) | {peak_status} |",
    ])

    section1 = _section_quality_vs_drawdown(history, stage1_results)
    section2 = _section_allowlist_vs_deploy(touch_events, stage1_results)
    section3 = _section_source_reconciliation(recon)
    section4 = _section_strategy_impact(strategy_impact)

    footer = "\n".join([
        "---",
        "",
        "> **数据来源**",
        "> - 治理指标: `artifacts/p0_trend_history.json`",
        "> - 策略指标: `strategies/results/stage1_*.json`",
        "> - PR 触碰: `artifacts/p0_allowlist_touch_events.json`",
        "> - 双源对账: `artifacts/source_reconciliation_latest.json`",
        "> - 策略影响: `artifacts/strategy_impact_latest.json`",
        "> - 峰值证据: `artifacts/stability_evidence_30d.json`",
        "> - 峰值发布门禁: `artifacts/peak_release_gate_latest.json`",
        ">",
        "> **刷新方式**: 夜间巡检后自动更新，或手动运行:",
        "> `python tools/data_reconciliation_audit.py --config config/data_reconciliation_audit.json --out-dir artifacts`",
        "> `python tools/governance_strategy_dashboard.py`",
    ])

    return "\n\n".join([header, section1, section2, section3, section4, footer])


# ─────────────────────────────────────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="治理-业务双轨联动看板生成器")
    parser.add_argument("--out", type=pathlib.Path, default=None, help="输出路径（默认 artifacts/governance_strategy_dashboard.md）")
    parser.add_argument(
        "--strategy-impact",
        type=pathlib.Path,
        default=STRATEGY_IMPACT_LATEST,
        help="策略影响门禁 JSON 路径（默认 artifacts/strategy_impact_latest.json）",
    )
    parser.add_argument(
        "--stability-evidence",
        type=pathlib.Path,
        default=STABILITY_EVIDENCE_LATEST,
        help="稳定证据 JSON 路径（默认 artifacts/stability_evidence_30d.json）",
    )
    parser.add_argument(
        "--peak-release-gate",
        type=pathlib.Path,
        default=PEAK_RELEASE_GATE_LATEST,
        help="峰值发布门禁 JSON 路径（默认 artifacts/peak_release_gate_latest.json）",
    )
    args = parser.parse_args(argv)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = args.out or (REPORT_DIR / "governance_strategy_dashboard.md")

    history        = _load_history()
    stage1_results = _load_stage1_results()
    touch_events   = _load_touch_events()
    recon_latest   = _load_reconciliation_latest()
    strategy_impact_latest = _load_strategy_impact_latest(args.strategy_impact)
    stability_evidence_latest = _load_stability_evidence_latest(args.stability_evidence)
    peak_release_gate_latest = _load_peak_release_gate_latest(args.peak_release_gate)
    generated_at   = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    content = build_dashboard(
        history,
        stage1_results,
        touch_events,
        recon_latest,
        strategy_impact_latest,
        stability_evidence_latest,
        peak_release_gate_latest,
        generated_at,
    )
    out.write_text(content, encoding="utf-8")

    touch_prs = len({ev.get("pr") for ev in touch_events if ev.get("pr")})
    print(f"[Dashboard] 已生成: {out}")
    print(f"  治理记录={len(history)}条 | Stage1={len(stage1_results)}条 | allowlist触碰PR={touch_prs}个")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
