"""
batch_diff.py — Stage 1 批量评测结果差异报告生成器

比较最近两次 batch 运行结果，输出：
  • 组级指标变化（pass_rate、median_sharpe、accessibility_pct 等）
  • 标的级状态翻转（PASS ↔ FAIL）
  • 阈值触碰变化（accessibility / tradeability 告警是否新增或消除）
  • SLIPPAGE_SENSITIVITY 字段一致性确认
  • 整体结论：改善 / 恶化 / 无变化

用法：
    python tools/batch_diff.py                              # 自动从 strategies/results 取最近两份
    python tools/batch_diff.py --dir artifacts              # 指定目录
    python tools/batch_diff.py --file-a A.json --file-b B.json --out-dir artifacts
    python tools/batch_diff.py --dir artifacts --output-stdout  # 只输出到终端
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DEFAULT_SEARCH_DIRS = [
    PROJECT_ROOT / "strategies" / "results",
    PROJECT_ROOT / "artifacts",
]


# ─────────────────────────────────────────────────────────────────────────────
# 差异数据结构
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class GroupDiff:
    label:           str
    group_key:       str
    # 绝对值
    pass_rate_a:     float;  pass_rate_b:     float
    median_sharpe_a: float;  median_sharpe_b: float
    access_pct_a:    float;  access_pct_b:    float
    access_warn_a:   float;  access_warn_b:   float
    td_fail_a:       int;    td_fail_b:       int
    verdict_a:       str;    verdict_b:       str
    confidence_a:    str;    confidence_b:    str
    # 派生
    pass_rate_delta:     float = 0.0
    median_sharpe_delta: float = 0.0
    access_delta:        float = 0.0
    # alert 变化
    access_alert_a: bool = False  # A 中触发告警
    access_alert_b: bool = False  # B 中触发告警
    td_alert_a:     bool = False
    td_alert_b:     bool = False


@dataclass
class AssetDiff:
    group: str
    symbol: str
    name:   str
    pass_a: bool | None   # None = 该文件中不存在
    pass_b: bool | None
    sharpe_a: float
    sharpe_b: float
    error_a: str
    error_b: str
    # 翻转标志
    flipped: bool = False      # PASS ↔ FAIL（不含 error 情况）
    new_error: bool = False    # B 中新增 error
    error_fixed: bool = False  # A 中有 error B 中恢复


@dataclass
class BatchDiff:
    file_a: str
    file_b: str
    date_a: str
    date_b: str
    total_passed_a: int
    total_passed_b: int
    total_assets_a: int
    total_assets_b: int
    slippage_changed: bool
    groups: list[GroupDiff] = field(default_factory=list)
    assets: list[AssetDiff] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# 数据加载 & 查找
# ─────────────────────────────────────────────────────────────────────────────

def _find_latest_two(search_dirs: list[pathlib.Path]) -> tuple[pathlib.Path, pathlib.Path]:
    """在搜索目录中找最近两份 batch_stage1_*.json 文件。"""
    all_files: list[pathlib.Path] = []
    for d in search_dirs:
        all_files.extend(sorted(d.glob("batch_stage1_*.json")))
    # 去重并按名称排序（文件名含日期，字典序即时间序）
    unique = sorted({f.name: f for f in all_files}.values(), key=lambda p: p.name)
    if len(unique) < 2:
        raise FileNotFoundError(
            f"需要至少 2 份 batch_stage1_*.json，仅找到 {len(unique)} 份。\n"
            f"搜索目录: {[str(d) for d in search_dirs]}\n"
            f"先运行 `python strategies/stage1_batch.py` 产生历史记录。"
        )
    return unique[-2], unique[-1]


def _load(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


# ─────────────────────────────────────────────────────────────────────────────
# 差异计算
# ─────────────────────────────────────────────────────────────────────────────

def _compute_diff(a: dict[str, Any], b: dict[str, Any], fa: pathlib.Path, fb: pathlib.Path) -> BatchDiff:
    # slippage_sensitivity 一致性
    slip_a = a.get("slippage_sensitivity", {})
    slip_b = b.get("slippage_sensitivity", {})
    slippage_changed = slip_a != slip_b

    # 组级 diff（以 label 为 key 对齐）
    groups_a = {g["label"]: g for g in a.get("groups", [])}
    groups_b = {g["label"]: g for g in b.get("groups", [])}
    all_labels = sorted(set(groups_a) | set(groups_b))

    group_diffs: list[GroupDiff] = []
    for label in all_labels:
        ga = groups_a.get(label, {})
        gb = groups_b.get(label, {})
        if not ga or not gb:
            continue  # 一侧缺失，跳过
        pr_a  = float(ga.get("pass_rate_pct", 0))
        pr_b  = float(gb.get("pass_rate_pct", 0))
        ms_a  = float(ga.get("median_sharpe", 0))
        ms_b  = float(gb.get("median_sharpe", 0))
        acc_a = float(ga.get("accessibility_pct", 100))
        acc_b = float(gb.get("accessibility_pct", 100))
        aw_a  = float(ga.get("accessibility_warn_pct", 80))
        aw_b  = float(gb.get("accessibility_warn_pct", 80))
        td_a  = int(ga.get("tradeability_fail_count", 0))
        td_b  = int(gb.get("tradeability_fail_count", 0))
        gd = GroupDiff(
            label           = label,
            group_key       = ga.get("group", ""),
            pass_rate_a     = pr_a,  pass_rate_b = pr_b,
            median_sharpe_a = ms_a,  median_sharpe_b = ms_b,
            access_pct_a    = acc_a, access_pct_b = acc_b,
            access_warn_a   = aw_a,  access_warn_b = aw_b,
            td_fail_a       = td_a,  td_fail_b = td_b,
            verdict_a       = ga.get("verdict", "?"),
            verdict_b       = gb.get("verdict", "?"),
            confidence_a    = ga.get("confidence", "?"),
            confidence_b    = gb.get("confidence", "?"),
        )
        gd.pass_rate_delta     = round(pr_b  - pr_a,  1)
        gd.median_sharpe_delta = round(ms_b  - ms_a,  4)
        gd.access_delta        = round(acc_b - acc_a,  1)
        gd.access_alert_a      = acc_a < aw_a
        gd.access_alert_b      = acc_b < aw_b
        gd.td_alert_a          = td_a > 0
        gd.td_alert_b          = td_b > 0
        group_diffs.append(gd)

    # 标的级 diff（以 symbol+group 为 key）
    def _key(ar: dict[str, Any]) -> str:
        return f"{ar.get('group', '')}::{ar.get('symbol', '')}"

    assets_a = {_key(r): r for r in a.get("assets", [])}
    assets_b = {_key(r): r for r in b.get("assets", [])}
    all_keys = sorted(set(assets_a) | set(assets_b))

    asset_diffs: list[AssetDiff] = []
    for k in all_keys:
        ra = assets_a.get(k)
        rb = assets_b.get(k)
        pa = bool(ra.get("stage1_pass")) if ra else None
        pb = bool(rb.get("stage1_pass")) if rb else None
        ea = (ra.get("error") or "") if ra else "（文件中不存在）"
        eb = (rb.get("error") or "") if rb else "（文件中不存在）"
        sa = float(ra.get("sharpe_full", 0)) if ra else 0.0
        sb = float(rb.get("sharpe_full", 0)) if rb else 0.0
        grp = (ra or rb or {}).get("group", "?")  # type: ignore[union-attr]
        sym = k.split("::")[1]
        nm  = (ra or rb or {}).get("name", sym)  # type: ignore[union-attr]
        ad = AssetDiff(
            group=grp, symbol=sym, name=nm,
            pass_a=pa, pass_b=pb,
            sharpe_a=sa, sharpe_b=sb,
            error_a=ea, error_b=eb,
        )
        # 翻转检测（只有两侧都有有效运行结果）
        if pa is not None and pb is not None and not ea and not eb:
            ad.flipped = pa != pb
        ad.new_error   = (not ea) and bool(eb)
        ad.error_fixed = bool(ea) and not bool(eb)
        asset_diffs.append(ad)

    return BatchDiff(
        file_a          = fa.name,
        file_b          = fb.name,
        date_a          = a.get("run_date", fa.stem),
        date_b          = b.get("run_date", fb.stem),
        total_passed_a  = int(a.get("total_passed", 0)),
        total_passed_b  = int(b.get("total_passed", 0)),
        total_assets_a  = int(a.get("total_assets", 0)),
        total_assets_b  = int(b.get("total_assets", 0)),
        slippage_changed = slippage_changed,
        groups          = group_diffs,
        assets          = asset_diffs,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Markdown 渲染
# ─────────────────────────────────────────────────────────────────────────────

def _delta_str(val: float, unit: str = "", invert: bool = False) -> str:
    """将 delta 值格式化为带方向符号的字符串，颜色靠 unicode 表情区分。"""
    if abs(val) < 1e-9:
        return "→ 0"
    is_positive_good = not invert
    if val > 0:
        icon = "🟢 ▲" if is_positive_good else "🔴 ▲"
    else:
        icon = "🔴 ▼" if is_positive_good else "🟢 ▼"
    return f"{icon}{abs(val):.2g}{unit}"


def _verdict_icon(v: str) -> str:
    return {"GO": "✅", "HOLD": "🟡", "NO-GO": "❌"}.get(v, "?") + f" {v}"


def render_diff_markdown(diff: BatchDiff) -> str:
    lines: list[str] = []
    a = lines.append

    a(f"# Stage 1 Batch 周度差异报告")
    a(f"")
    a(f"| 项目 | 值 |")
    a(f"|------|----|")
    a(f"| 生成时间 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} |")
    a(f"| 对比基准 | `{diff.file_a}` （{diff.date_a}）|")
    a(f"| 对比目标 | `{diff.file_b}` （{diff.date_b}）|")

    # 总体变化
    total_delta = diff.total_passed_b - diff.total_passed_a
    delta_icon  = "🟢" if total_delta > 0 else ("🔴" if total_delta < 0 else "🟡")
    a(f"| 总通过数变化 | {diff.total_passed_a} → {diff.total_passed_b}"
      f"  {delta_icon} {'+' if total_delta >= 0 else ''}{total_delta} |")
    if diff.slippage_changed:
        a(f"| ⚠️ SLIPPAGE_SENSITIVITY | 两版本不一致！需确认是否为计划内变更 |")
    else:
        a(f"| SLIPPAGE_SENSITIVITY | ✅ 两版本一致 |")
    a(f"")

    # ── 组级变化表 ──
    a("## 分组指标变化")
    a("")
    a("| 分组 | 可达率 | Δ可达 | 通过率 | Δ通过 | 夏普Δ | 成交失败 | 判定 | 置信度 |")
    a("|------|--------|-------|--------|-------|-------|----------|------|--------|")
    for g in diff.groups:
        # 可达率
        acc_flag = "🔴" if g.access_alert_b else ("🟢" if not g.access_alert_a and not g.access_alert_b else "🟡")
        acc_s = f"{g.access_pct_a:.0f}%→{g.access_pct_b:.0f}%"
        pr_s  = f"{g.pass_rate_a:.0f}%→{g.pass_rate_b:.0f}%"
        td_s  = f"{g.td_fail_a}→{g.td_fail_b}"
        # 判定变化
        if g.verdict_a != g.verdict_b:
            verd = f"{_verdict_icon(g.verdict_a)} → {_verdict_icon(g.verdict_b)}"
        else:
            verd = _verdict_icon(g.verdict_b)
        # 置信度变化
        if g.confidence_a != g.confidence_b:
            conf = f"{g.confidence_a}→{g.confidence_b}"
        else:
            conf = g.confidence_b
        a(f"| {g.label} "
          f"| {acc_flag}{acc_s} "
          f"| {_delta_str(g.access_delta, '%')} "
          f"| {pr_s} "
          f"| {_delta_str(g.pass_rate_delta, '%')} "
          f"| {_delta_str(g.median_sharpe_delta)} "
          f"| {td_s} "
          f"| {verd} "
          f"| {conf} |")
    a(f"")

    # ── 告警状态变化 ──
    new_alerts  = [g for g in diff.groups if not g.access_alert_a and g.access_alert_b]
    clear_alerts = [g for g in diff.groups if g.access_alert_a and not g.access_alert_b]
    if new_alerts or clear_alerts:
        a("## 可达率告警变化")
        a("")
        if new_alerts:
            a("### 🔴 新增告警（需关注）")
            for g in new_alerts:
                a(f"- **{g.label}**: 可达率从 {g.access_pct_a:.0f}% 降至 {g.access_pct_b:.0f}%"
                  f"，已低于阈值 {g.access_warn_b:.0f}%")
            a("")
        if clear_alerts:
            a("### 🟢 告警已消除")
            for g in clear_alerts:
                a(f"- **{g.label}**: 可达率从 {g.access_pct_a:.0f}% 恢复至 {g.access_pct_b:.0f}%")
            a("")

    # ── 标的状态翻转 ──
    flipped    = [ad for ad in diff.assets if ad.flipped]
    new_errors = [ad for ad in diff.assets if ad.new_error]
    err_fixed  = [ad for ad in diff.assets if ad.error_fixed]

    if flipped or new_errors or err_fixed:
        a("## 标的状态变化")
        a("")
    if new_errors:
        a("### 🔴 新增运行错误（需立即排查）")
        a("")
        a("| 分组 | 代码 | 名称 | 错误摘要 |")
        a("|------|------|------|----------|")
        for ad in new_errors:
            a(f"| {ad.group} | {ad.symbol} | {ad.name} | {ad.error_b[:60]} |")
        a("")
    if err_fixed:
        a("### 🟢 运行错误已修复")
        a("")
        a("| 分组 | 代码 | 名称 |")
        a("|------|------|------|")
        for ad in err_fixed:
            a(f"| {ad.group} | {ad.symbol} | {ad.name} |")
        a("")
    if flipped:
        a("### 🔄 通过/失败状态翻转")
        a("")
        a("| 分组 | 代码 | 名称 | 前 | 后 | 夏普变化 |")
        a("|------|------|------|-----|-----|--------|")
        for ad in flipped:
            before = "✅ PASS" if ad.pass_a else "❌ FAIL"
            after  = "✅ PASS" if ad.pass_b else "❌ FAIL"
            sh_d   = round(ad.sharpe_b - ad.sharpe_a, 3)
            sh_s   = f"{'+' if sh_d >= 0 else ''}{sh_d:.3f}"
            a(f"| {ad.group} | {ad.symbol} | {ad.name} | {before} | {after} | {sh_s} |")
        a("")

    if not flipped and not new_errors and not err_fixed:
        a("## 标的状态变化")
        a("")
        a("> ✅ 无标的状态翻转、无新增/消除错误")
        a("")

    # ── SLIPPAGE_SENSITIVITY 详情（若变化） ──
    if diff.slippage_changed:
        a("## ⚠️ SLIPPAGE_SENSITIVITY 版本不一致")
        a("")
        a("> 两次运行使用了不同的滑点阈值配置。请确认：")
        a("> 1. 该变更是否为正式 PR 合并的计划内变更？")
        a("> 2. 历史回测结果是否需要在新阈值下重新执行？")
        a("> 3. 对应的 `issue_ref` 是否已在 stage1_universe.yaml `_change_log` 中记录？")
        a("")

    # ── 结论 ──
    a("## 综合结论")
    a("")
    deteriorated = [g for g in diff.groups if g.pass_rate_delta < -5 or g.access_delta < -5]
    improved     = [g for g in diff.groups if g.pass_rate_delta > 5  or g.access_delta > 5]

    if not deteriorated and not improved and not flipped and not new_errors:
        a("> 🟡 **无显著变化** — 各分组核心指标维持稳定，参数基线可暂时保持不变。")
    if improved:
        a("> 🟢 **改善分组**: " + "、".join(g.label for g in improved))
    if deteriorated or new_errors:
        a("> 🔴 **需关注**: " + "、".join(g.label for g in deteriorated))
        if new_errors:
            a(f"> 🔴 **新增错误**: {len(new_errors)} 个标的出现运行时错误，请检查数据源。")
    a("")
    a("---")
    a(f"*本报告由 `tools/batch_diff.py` 自动生成，数据来源：{diff.file_a} vs {diff.file_b}*")
    a("")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 机器可读 JSON 摘要（供看板、告警聚合系统消费）
# ─────────────────────────────────────────────────────────────────────────────

def _alert_level(diff: BatchDiff) -> str:
    """OK / WARN / CRITICAL。"""
    new_errors    = sum(1 for ad in diff.assets if ad.new_error)
    new_acc_alerts = sum(1 for g in diff.groups if not g.access_alert_a and g.access_alert_b)
    flip_count    = sum(1 for ad in diff.assets if ad.flipped)
    if new_errors and new_acc_alerts:
        return "CRITICAL"
    if new_errors or (new_acc_alerts > 0 and diff.slippage_changed):
        return "CRITICAL"
    if new_acc_alerts > 0 or diff.slippage_changed or flip_count > 0:
        return "WARN"
    return "OK"


def to_json_dict(diff: BatchDiff) -> dict[str, Any]:
    """将 BatchDiff 序列化为 JSON 友好的 dict。
    字段命名遵循 snake_case；数值全部保留两位小数以便聚合。
    """
    return {
        "schema_version": "1.0",
        "generator":      "batch_diff.py@v1",
        "generated_at":   datetime.now().isoformat(timespec="seconds"),
        "file_a":         diff.file_a,
        "file_b":         diff.file_b,
        "date_a":         diff.date_a,
        "date_b":         diff.date_b,
        "alert_level":    _alert_level(diff),
        "slippage_consistent": not diff.slippage_changed,
        "total_passed_a":      diff.total_passed_a,
        "total_passed_b":      diff.total_passed_b,
        "total_passed_delta":  diff.total_passed_b - diff.total_passed_a,
        "flip_count":          sum(1 for ad in diff.assets if ad.flipped),
        "new_error_count":     sum(1 for ad in diff.assets if ad.new_error),
        "error_fixed_count":   sum(1 for ad in diff.assets if ad.error_fixed),
        "groups": [
            {
                "label":                 g.label,
                "group_key":             g.group_key,
                "pass_rate_a":           round(g.pass_rate_a, 2),
                "pass_rate_b":           round(g.pass_rate_b, 2),
                "pass_rate_delta":       round(g.pass_rate_delta, 2),
                "median_sharpe_a":       round(g.median_sharpe_a, 4),
                "median_sharpe_b":       round(g.median_sharpe_b, 4),
                "median_sharpe_delta":   round(g.median_sharpe_delta, 4),
                "access_pct_a":          round(g.access_pct_a, 1),
                "access_pct_b":          round(g.access_pct_b, 1),
                "access_delta":          round(g.access_delta, 1),
                "access_warn_pct":       round(g.access_warn_b, 1),
                "access_alert_active":   g.access_alert_b,
                "access_alert_new":      not g.access_alert_a and g.access_alert_b,
                "access_alert_cleared":  g.access_alert_a and not g.access_alert_b,
                "td_fail_a":             g.td_fail_a,
                "td_fail_b":             g.td_fail_b,
                "verdict_a":             g.verdict_a,
                "verdict_b":             g.verdict_b,
                "verdict_changed":       g.verdict_a != g.verdict_b,
                "confidence_a":          g.confidence_a,
                "confidence_b":          g.confidence_b,
            }
            for g in diff.groups
        ],
        # 仅含有状态变化的标的（减小 JSON 体积）
        "state_changes": [
            {
                "symbol":       ad.symbol,
                "name":         ad.name,
                "group":        ad.group,
                "pass_a":       ad.pass_a,
                "pass_b":       ad.pass_b,
                "sharpe_a":     round(ad.sharpe_a, 4),
                "sharpe_b":     round(ad.sharpe_b, 4),
                "sharpe_delta": round(ad.sharpe_b - ad.sharpe_a, 4),
                "new_error":    ad.new_error,
                "error_fixed":  ad.error_fixed,
                "error_b":      ad.error_b[:120] if ad.error_b else "",
            }
            for ad in diff.assets
            if ad.flipped or ad.new_error or ad.error_fixed
        ],
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stage 1 Batch 差异报告")
    parser.add_argument("--file-a", type=pathlib.Path, default=None,
                        help="较旧的 batch JSON（不指定则自动查找）")
    parser.add_argument("--file-b", type=pathlib.Path, default=None,
                        help="较新的 batch JSON（不指定则自动查找）")
    parser.add_argument("--dir", type=pathlib.Path, default=None,
                        help="从指定目录自动查找最近两份 JSON")
    parser.add_argument("--out-dir", type=pathlib.Path, default=None,
                        help="diff 报告输出目录（默认=较新 JSON 所在目录）")
    parser.add_argument("--output-stdout", action="store_true",
                        help="只输出到终端，不写文件")
    args = parser.parse_args(argv)

    # 确定两个文件
    if args.file_a and args.file_b:
        fa, fb = args.file_a, args.file_b
    else:
        search_dirs = [args.dir] if args.dir else DEFAULT_SEARCH_DIRS
        try:
            fa, fb = _find_latest_two(search_dirs)
        except FileNotFoundError as e:
            print(f"[ERROR] {e}", file=sys.stderr)
            return 1

    if not fa.exists():
        print(f"[ERROR] 文件不存在: {fa}", file=sys.stderr)
        return 1
    if not fb.exists():
        print(f"[ERROR] 文件不存在: {fb}", file=sys.stderr)
        return 1

    print(f"[batch_diff] 对比: {fa.name}  VS  {fb.name}")

    a_data = _load(fa)
    b_data = _load(fb)
    diff   = _compute_diff(a_data, b_data, fa, fb)
    md     = render_diff_markdown(diff)

    if args.output_stdout:
        print(md)
        return 0

    # 写文件
    out_dir = args.out_dir or fb.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    date_str = diff.date_b or datetime.now().strftime("%Y-%m-%d")
    out_path = out_dir / f"diff_report_{date_str}.md"
    out_path.write_text(md, encoding="utf-8")
    print(f"[batch_diff] diff 报告已写入: {out_path}")

    # 机器可读 JSON 摘要（与 .md 同名同目录，供看板和告警聚合消费）
    json_path = out_dir / f"diff_report_{date_str}.json"
    json_path.write_text(
        json.dumps(to_json_dict(diff), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[batch_diff] JSON 摘要已写入: {json_path}")

    # 终端摘要
    has_issue = (diff.slippage_changed or
                 any(ad.new_error for ad in diff.assets) or
                 any(g.access_alert_b for g in diff.groups))
    if has_issue:
        print(f"[batch_diff] ⚠️  发现需关注项，请查阅报告。")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
