#!/usr/bin/env python3
"""
P0 治理周报自动生成

读取 artifacts/p0_trend_history.json，计算近 7 天指标，输出 Markdown 周报。

用法：
  python tools/p0_weekly_report.py [--out PATH] [--window-days N]

输出：
  artifacts/p0_weekly_YYYYMMDD.md（默认）
  同时写入 artifacts/p0_weekly_latest.md（始终覆盖，方便 CI 直接引用）

周报内容：
  - P0_open_count / active_critical_high 趋势
  - allowlist 总量及过期条目量
  - strict_gate_pass SLA 达成率（周期内）
  - 斜率分析（是否在持续收敛）
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

HISTORY_PATH = pathlib.Path("artifacts/p0_trend_history.json")
REPORT_DIR   = pathlib.Path("artifacts")
TOUCH_EVENTS_PATH = pathlib.Path("artifacts/p0_allowlist_touch_events.json")
STRATEGY_IMPACT_LATEST_PATH = pathlib.Path("artifacts/strategy_impact_latest.json")
STABILITY_EVIDENCE_LATEST_PATH = pathlib.Path("artifacts/stability_evidence_30d.json")
PEAK_RELEASE_GATE_LATEST_PATH = pathlib.Path("artifacts/peak_release_gate_latest.json")


def _ts_of(r: dict[str, Any]) -> str:
    return str(r.get("ts") or r.get("timestamp") or r.get("saved_at") or "")


def _p0_of(r: dict[str, Any]) -> int:
    return int(r.get("P0_open_count", r.get("P0_open", 0)) or 0)


def _ach_of(r: dict[str, Any]) -> int:
    return int(r.get("active_critical_high", r.get("ach", 0)) or 0)


def _recon_pass_rate_pct_of(r: dict[str, Any]) -> float:
    rec = r.get("reconciliation", {})
    if isinstance(rec, dict):
        return float(rec.get("pass_rate", 0.0) or 0.0) * 100.0
    return 0.0


def _recon_failed_of(r: dict[str, Any]) -> int:
    rec = r.get("reconciliation", {})
    if isinstance(rec, dict):
        return int(rec.get("failed_symbols", 0) or 0)
    return 0


def _load_history(path: pathlib.Path) -> list[dict[str, Any]]:
    if not path.exists():
        print(f"[WARN] 趋势历史文件不存在: {path}")
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[ERROR] 读取趋势历史失败: {e}")
        return []


def _filter_window(records: list[dict[str, Any]], window_days: int) -> list[dict[str, Any]]:
    """返回最近 window_days 天（含今天）的记录。"""
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=window_days)
    result = []
    for r in records:
        ts_str = _ts_of(r)
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if ts >= cutoff:
                result.append(r)
        except Exception:
            pass
    return result


def _sla_rate(records: list[dict[str, Any]]) -> float:
    """strict_gate_pass SLA 达成率（百分比）。"""
    if not records:
        return 0.0
    passed = sum(1 for r in records if r.get("strict_gate_pass", False))
    return passed * 100.0 / len(records)


def _latest_allowlist_count(records: list[dict[str, Any]]) -> tuple[int, int, int]:
    """返回最新记录的 (allowlist_total, allowlist_expired, allowlist_due_90d)。"""
    if not records:
        return 0, 0, 0
    latest = records[-1]
    total   = latest.get("allowlist_total",   latest.get("allowlist_suppressed", 0))
    expired = latest.get("allowlist_expired", 0)
    due_90d = latest.get("allowlist_due_90d", 0)
    return total, expired, due_90d


def _load_touch_pr_count(path: pathlib.Path, window_days: int) -> int:
    """返回最近 window_days 天内触碰 allowlist 覆盖文件的 PR 数（去重）。-1 表示数据不可用。"""
    if not path.exists():
        return -1
    try:
        events = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return -1
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=window_days)
    pr_set: set[int] = set()
    for ev in events:
        ts_str = ev.get("ts", "")
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if ts >= cutoff:
                pr = ev.get("pr", 0)
                if pr:
                    pr_set.add(int(pr))
        except Exception:
            pass
    return len(pr_set)


def _load_strategy_impact_latest(path: pathlib.Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _load_stability_evidence_latest(path: pathlib.Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _load_peak_release_gate_latest(path: pathlib.Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _slope_analysis(records: list[dict[str, Any]], field: str) -> str:
    """计算 field 从 records[0] 到 records[-1] 的变化趋势描述。"""
    if len(records) < 2:
        return "数据不足，无法分析趋势"
    if field == "P0_open_count":
        first = _p0_of(records[0])
        last = _p0_of(records[-1])
    elif field == "active_critical_high":
        first = _ach_of(records[0])
        last = _ach_of(records[-1])
    elif field == "recon_pass_rate_pct":
        first = _recon_pass_rate_pct_of(records[0])
        last = _recon_pass_rate_pct_of(records[-1])
    elif field == "recon_failed_symbols":
        first = _recon_failed_of(records[0])
        last = _recon_failed_of(records[-1])
    else:
        first = records[0].get(field, None)
        last = records[-1].get(field, None)
    if first is None or last is None:
        return "字段缺失"
    delta = last - first
    if delta < 0:
        return f"↓ 下降 {abs(delta)}（{first} → {last}），持续收敛 ✓"
    elif delta == 0:
        return f"→ 持平 {last}（无变化）"
    else:
        return f"↑ 上升 {abs(delta)}（{first} → {last}），需关注 ⚠️"


def _build_report(
    records_all: list[dict[str, Any]],
    records_week: list[dict[str, Any]],
    window_days: int,
    generated_at: str,
    touched_pr_count: int = -1,
    strategy_impact_latest: dict[str, Any] | None = None,
    stability_evidence_latest: dict[str, Any] | None = None,
    peak_release_gate_latest: dict[str, Any] | None = None,
) -> str:
    sla = _sla_rate(records_week)
    al_total, al_expired, al_due_90d = _latest_allowlist_count(records_all)
    slope_p0  = _slope_analysis(records_week, "P0_open_count")
    slope_ach = _slope_analysis(records_week, "active_critical_high")
    recon_records_week = [r for r in records_week if isinstance(r.get("reconciliation"), dict)]
    recon_records_all = [r for r in records_all if isinstance(r.get("reconciliation"), dict)]
    recon_scope = recon_records_week if recon_records_week else recon_records_all

    # 提取最新对账摘要（供「下一步行动」动态整改任务使用）
    _latest_recon_row = recon_scope[-1] if recon_scope else {}
    _latest_recon_d = (
        _latest_recon_row.get("reconciliation", {})
        if isinstance(_latest_recon_row.get("reconciliation", {}), dict)
        else {}
    )
    _recon_fail_count = int(_latest_recon_d.get("failed_symbols", 0) or 0)
    _recon_qmt_ok     = _latest_recon_d.get("qmt_available", None)
    _recon_ak_ok      = _latest_recon_d.get("akshare_available", None)

    # 最近 14 条(或全部)的明细表格
    detail_rows = records_all[-14:]

    lines = [
        "# P0 治理周报",
        "",
        f"> 生成时间: {generated_at}  ",
        f"> 统计窗口: 最近 {window_days} 天（含今天，共 {len(records_week)} 条记录）  ",
        f"> 历史趋势: 共 {len(records_all)} 条记录",
        "",
        "## 本周关键指标",
        "",
        "| 指标 | 值 |",
        "|------|----|",
    ]

    if records_week:
        latest = records_week[-1]
        latest_recon_row = recon_scope[-1] if recon_scope else {}
        latest_recon = latest_recon_row.get("reconciliation", {}) if isinstance(latest_recon_row.get("reconciliation", {}), dict) else {}
        recon_total = int(latest_recon.get("total_symbols", 0) or 0)
        recon_pass = int(latest_recon.get("passed_symbols", 0) or 0)
        recon_fail = int(latest_recon.get("failed_symbols", 0) or 0)
        recon_rate = float(latest_recon.get("pass_rate", 0.0) or 0.0) * 100.0
        qmt_ok = latest_recon.get("qmt_available", None)
        ak_ok = latest_recon.get("akshare_available", None)
        lines += [
            f"| P0_open_count（最新） | {_p0_of(latest)} |",
            f"| active_critical_high（最新） | {_ach_of(latest)} |",
            f"| strict_gate_pass SLA | {sla:.1f}% ({sum(1 for r in records_week if r.get('strict_gate_pass', False))}/{len(records_week)} 次通过) |",
            f"| allowlist 条目总量 | {al_total} |",
            f"| allowlist 过期条目 | {al_expired} |",
            f"| allowlist 90 天内到期 | {al_due_90d} |",
            f"| 本周触碰豁免文件的 PR 数 | {'N/A (无 CI 记录)' if touched_pr_count < 0 else touched_pr_count} |",
            f"| 双源对账通过率（最新） | {recon_pass}/{recon_total} ({recon_rate:.1f}%) |",
            f"| 双源对账失败标的（最新） | {recon_fail} |",
            f"| 双源源可用性（最新） | QMT={'✅' if qmt_ok else '❌' if qmt_ok is not None else 'N/A'} / AKShare={'✅' if ak_ok else '❌' if ak_ok is not None else 'N/A'} |",
        ]
        if isinstance(strategy_impact_latest, dict):
            si_available = bool(strategy_impact_latest.get("available", False))
            si_pass = bool(strategy_impact_latest.get("gate_pass", True))
            si_delta = strategy_impact_latest.get("delta", {}) if isinstance(strategy_impact_latest.get("delta"), dict) else {}
            si_dr = float(si_delta.get("annualized_return_pct", 0.0) or 0.0)
            si_dmdd = float(si_delta.get("max_drawdown_pct", 0.0) or 0.0)
            si_sflip = bool(si_delta.get("sharpe_sign_changed", False))
            lines += [
                f"| 策略影响门禁 available | {'✅' if si_available else 'N/A'} |",
                f"| 策略影响门禁 gate_pass | {'✅ PASS' if si_pass else '❌ FAIL'} |",
                f"| 策略影响 ΔR/ΔMDD | {si_dr:+.2f} / {si_dmdd:+.2f} |",
                f"| 策略影响 Sharpe_sign 翻转 | {'⚠️ 是' if si_sflip else '✅ 否'} |",
            ]
        if isinstance(stability_evidence_latest, dict):
            peak_ready = bool(stability_evidence_latest.get("peak_ready", False))
            consec_days = int(stability_evidence_latest.get("consecutive_compliant_days", 0) or 0)
            gap_days = max(0, 14 - consec_days)
            lines += [
                f"| 峰值证据 peak_ready | {'✅ READY' if peak_ready else '⏳ NO'} |",
                f"| 峰值证据连续达标天数 | {consec_days}（距峰值还差 {gap_days} 天） |",
            ]
        if isinstance(peak_release_gate_latest, dict):
            level = str(peak_release_gate_latest.get("level", "") or "").lower()
            gap = int(peak_release_gate_latest.get("gap_to_fail_days", 0) or 0)
            if level == "pass":
                level_text = "✅ PASS"
            elif level == "warn":
                level_text = f"🟡 WARN（gap {gap}d）"
            elif level == "fail":
                level_text = f"❌ FAIL（gap {gap}d）"
            else:
                level_text = "N/A"
            lines.append(f"| 峰值发布门禁(SSOT) | {level_text} |")
    else:
        lines += ["| （本周无数据） | — |"]

    lines += [
        "",
        "## 趋势分析",
        "",
        f"- **P0_open_count**: {slope_p0}",
        f"- **active_critical_high**: {slope_ach}",
        "",
        "## 双源对账健康度（最近窗口）",
        "",
        f"- **对账通过率趋势**: {_slope_analysis(recon_scope, 'recon_pass_rate_pct') if recon_scope else '数据不足，无法分析趋势'}",
        f"- **失败标的趋势**: {_slope_analysis(recon_scope, 'recon_failed_symbols') if recon_scope else '数据不足，无法分析趋势'}",
        "",
        "## 明细（最近 14 条）",
        "",
        "| 时间 | P0_open | crit/high | gate_pass | allowlist |",
        "|------|---------|-----------|-----------|-----------|",
    ]

    for r in detail_rows:
        ts = _ts_of(r)[:10]
        p0  = _p0_of(r)
        ach = _ach_of(r)
        gp  = "PASS" if r.get("strict_gate_pass", False) else "FAIL"
        al  = r.get("allowlist_total", r.get("allowlist_suppressed", "?"))
        lines.append(f"| {ts} | {p0} | {ach} | {gp} | {al} |")

    # SLO 告警（连续 3 天 active_critical_high 不下降且 > 0）
    if len(records_week) >= 4:
        recent_ach = [_ach_of(r) for r in records_week[-4:]]
        stalled = all(recent_ach[i + 1] >= recent_ach[i] for i in range(3)) and recent_ach[-1] > 0
        if stalled:
            lines += [
                "",
                "> ⚠️ **SLO 告警**: `active_critical_high` 已连续 3 天未下降，",
                f"> 当前值 = **{recent_ach[-1]}**，请指定清债负责人并在 72h 内合并修复 PR。",
            ]
    # allowlist 预警：90 天内到期 或 本周触碰 PR 过多
    if al_due_90d > 0:
        lines += [
            "",
            f"> 📅 **到期预警**: `allowlist` 有 **{al_due_90d}** 条将在 90 天内到期，",
            "> 请提前确认对应违规是否已修复或续期（附 issue/PR 链接）。",
        ]
    if touched_pr_count > 2:
        lines += [
            "",
            f"> 🔍 **豁免压力**: 本周已有 **{touched_pr_count}** 个 PR 触碰安全扫描豁免覆盖文件，",
            "> 建议复审豁免理由或加入修复辭单。",
        ]
    # ── 动态整改任务：根据各门禁指标状态自动生成待办项 ──────────────────
    action_items: list[str] = []

    # 双源对账相关
    if _recon_qmt_ok is False:
        action_items.append(
            "- [ ] 🔴 **QMT 可用性恢复**：检查 miniquote/xtquant 服务状态，"
            "恢复后用 qmt311 重跑对账 `python tools/data_reconciliation_audit.py`"
        )
    if _recon_ak_ok is False:
        action_items.append(
            "- [ ] ⚠️ **AKShare 连接异常**：检查网络及 `pip install akshare --upgrade`，"
            "修复后重跑对账"
        )
    if _recon_fail_count > 0:
        action_items.append(
            f"- [ ] 🔴 **对账失败整改**（{_recon_fail_count} 个标的失败）："
            "用 qmt311 重跑 `python tools/data_reconciliation_audit.py --strict`，"
            "对 close_rel_p95 超阈值标的逐一确认并触发数据修复"
        )

    # allowlist 健康
    if al_expired > 0:
        action_items.append(
            f"- [ ] 📅 **过期 allowlist 清理**：{al_expired} 条已过期，"
            "确认对应违规已修复后移除或更新到期日（附 issue/PR 链接）"
        )
    if al_due_90d > 0:
        action_items.append(
            f"- [ ] 📅 **allowlist 即将到期**：{al_due_90d} 条 90 天内到期，"
            "提前评估是否已修复并在 PR 中续期或移除"
        )
    if al_total > 12:
        action_items.append(
            f"- [ ] ⚠️ **allowlist 超出目标**：当前 {al_total} 条，目标 ≤12，"
            "优先消减过期/已修复条目"
        )

    # 门禁 SLA
    if records_week and sla < 100.0:
        fail_cnt_gate = sum(1 for r in records_week if not r.get("strict_gate_pass", False))
        action_items.append(
            f"- [ ] 🔴 **门禁 SLA 未达标**：本周 {fail_cnt_gate} 次 strict_gate 未通过，"
            "排查失败原因并修复至 data_contract_passed_rate = 100%"
        )
    if isinstance(strategy_impact_latest, dict):
        si_available = bool(strategy_impact_latest.get("available", False))
        si_pass = bool(strategy_impact_latest.get("gate_pass", True))
        si_reason = str(strategy_impact_latest.get("reason", "unknown"))
        si_delta = strategy_impact_latest.get("delta", {}) if isinstance(strategy_impact_latest.get("delta"), dict) else {}
        si_dr = float(si_delta.get("annualized_return_pct", 0.0) or 0.0)
        si_dmdd = float(si_delta.get("max_drawdown_pct", 0.0) or 0.0)
        si_sflip = bool(si_delta.get("sharpe_sign_changed", False))
        if not si_available:
            action_items.append(
                f"- [ ] ⚠️ **策略影响门禁不可评估**：reason={si_reason}，请补齐 baseline 与最新 Stage1 结果"
            )
        elif not si_pass:
            action_items.append(
                f"- [ ] 🔴 **策略影响门禁未通过**：ΔR={si_dr:+.2f}、ΔMDD={si_dmdd:+.2f}、Sharpe_sign_flip={'是' if si_sflip else '否'}，"
                "请复核数据口径/参数变更并更新 baseline 或回滚发布"
            )
    if isinstance(stability_evidence_latest, dict):
        peak_ready = bool(stability_evidence_latest.get("peak_ready", False))
        consec_days = int(stability_evidence_latest.get("consecutive_compliant_days", 0) or 0)
        gap_days = max(0, 14 - consec_days)
        if not peak_ready:
            action_items.append(
                f"- [ ] 🟡 **冲刺峰值进度未达标**：连续达标 {consec_days} 天，距峰值还差 {gap_days} 天，"
                "保持 strict_gate/step6/strategy_impact 三轨稳定并持续跟踪"
            )
    if isinstance(peak_release_gate_latest, dict):
        level = str(peak_release_gate_latest.get("level", "") or "").lower()
        gap = int(peak_release_gate_latest.get("gap_to_fail_days", 0) or 0)
        if level == "fail":
            action_items.append(
                f"- [ ] 🔴 **峰值发布门禁失败(SSOT)**：距阻断阈值仍差 {gap} 天，禁止生产放量，继续预发观察"
            )
        elif level == "warn":
            action_items.append(
                f"- [ ] 🟡 **峰值发布门禁预警(SSOT)**：距阻断阈值还差 {gap} 天，维持灰度并每日复核证据板"
            )

    # 固定常规任务（无论状态如何始终出现）
    action_items.append("- [ ] lineage 回填验证（生产 DB 路径确认后执行）")
    action_items.append("- [ ] CI ops-metrics 分支打通")

    lines += ["", "## 下一步行动", ""] + action_items

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="P0 治理周报自动生成")
    parser.add_argument("--out", default="", help="报告输出路径（默认按日期命名）")
    parser.add_argument("--window-days", type=int, default=7, help="统计窗口天数（默认 7）")
    parser.add_argument("--history", default=str(HISTORY_PATH), help="趋势历史 JSON 路径")
    parser.add_argument(
        "--strategy-impact",
        default=str(STRATEGY_IMPACT_LATEST_PATH),
        help="策略影响门禁 JSON 路径（默认 artifacts/strategy_impact_latest.json）",
    )
    parser.add_argument(
        "--stability-evidence",
        default=str(STABILITY_EVIDENCE_LATEST_PATH),
        help="稳定证据 JSON 路径（默认 artifacts/stability_evidence_30d.json）",
    )
    parser.add_argument(
        "--peak-release-gate",
        default=str(PEAK_RELEASE_GATE_LATEST_PATH),
        help="峰值发布门禁 JSON 路径（默认 artifacts/peak_release_gate_latest.json）",
    )
    args = parser.parse_args(argv)

    records_all = _load_history(pathlib.Path(args.history))
    records_week = _filter_window(records_all, args.window_days)

    now_utc = datetime.now(tz=timezone.utc)
    generated_at = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_str     = now_utc.strftime("%Y%m%d")

    touched_pr_count = _load_touch_pr_count(TOUCH_EVENTS_PATH, args.window_days)
    strategy_impact_latest = _load_strategy_impact_latest(pathlib.Path(args.strategy_impact))
    stability_evidence_latest = _load_stability_evidence_latest(pathlib.Path(args.stability_evidence))
    peak_release_gate_latest = _load_peak_release_gate_latest(pathlib.Path(args.peak_release_gate))
    report_text = _build_report(
        records_all,
        records_week,
        args.window_days,
        generated_at,
        touched_pr_count,
        strategy_impact_latest,
        stability_evidence_latest,
        peak_release_gate_latest,
    )

    # 写入带日期的版本
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = pathlib.Path(args.out) if args.out else REPORT_DIR / f"p0_weekly_{date_str}.md"
    out_path.write_text(report_text, encoding="utf-8")
    print(f"[OK] 周报已写入: {out_path}")

    # 同时写 latest（供 CI badge/飞书 webhook 直接引用）
    latest_path = REPORT_DIR / "p0_weekly_latest.md"
    latest_path.write_text(report_text, encoding="utf-8")
    print(f"[OK] latest 已更新: {latest_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
