"""
stage1_batch.py — Stage 1 多标的跨资产批量评测器

读取 config/stage1_universe.yaml，按分组跑完所有标的，
输出：
  • strategies/results/batch_stage1_<日期>.json  — 机器可读详情
  • strategies/results/batch_stage1_<日期>.md    — 人工报告（含 GO/HOLD/NO-GO 判定）

用法：
    python strategies/stage1_batch.py
    python strategies/stage1_batch.py --universe config/stage1_universe.yaml
    python strategies/stage1_batch.py --groups stocks etf          # 只跑指定分组
    python strategies/stage1_batch.py --dry-run                    # 仅数据验收
    python strategies/stage1_batch.py --out-dir strategies/results
"""
from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Optional

# ── 可选 PyYAML；无则回退到内置简单解析 ─────────────────────────────────────
try:
    import yaml as _yaml
    def _load_yaml(path: pathlib.Path) -> dict[str, Any]:
        with open(path, encoding="utf-8") as f:
            return _yaml.safe_load(f)  # type: ignore[no-any-return]
except ImportError:
    def _load_yaml(path: pathlib.Path) -> dict[str, Any]:  # type: ignore[misc]
        raise RuntimeError(
            "缺少 PyYAML：请先 pip install pyyaml，或使用 conda install pyyaml"
        )

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
UNIVERSE_PATH = PROJECT_ROOT / "config" / "stage1_universe.yaml"

# ── Python 版本卫兵：QMT datacenter 只兼容 3.11 ─────────────────────────────────
_PY = sys.version_info[:2]
if _PY != (3, 11):
    print(
        f"[WARN] Python {_PY[0]}.{_PY[1]} 检测到（需要 3.11）。"
        "QMT datacenter.cp311 二进制不兼容，QMT 数据源将不可用。"
        " 修复: conda activate qmt311  或  .\\run_batch.ps1",
        file=sys.stderr,
    )
RESULTS_DIR   = PROJECT_ROOT / "strategies" / "results"

# ─────────────────────────────────────────────────────────────────────────────
# 成交可行性：滑点敏感性分层阈值（单侧百分比）
# ─────────────────────────────────────────────────────────────────────────────
SLIPPAGE_SENSITIVITY: dict[str, dict[str, float]] = {
    # tier → warn / fail （单侧绝对滑点）
    "equity":            {"warn": 0.002, "fail": 0.005},
    "etf":               {"warn": 0.001, "fail": 0.003},
    "commodity_futures": {"warn": 0.004, "fail": 0.010},
    "index_futures":     {"warn": 0.001, "fail": 0.003},
}

# ─────────────────────────────────────────────────────────────────────────────
# 数据结构
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AssetResult:
    group:            str
    symbol:           str          # 实际使用的代码（优先 continuous_symbol）
    original_symbol:  str          # YAML 中的原始 symbol
    name:             str
    stage1_pass:      bool
    summary:          str
    sharpe_full:      float
    annualized_pct:   float
    max_drawdown_pct: float
    calmar_ratio:     float
    sortino_ratio:    float
    oos_ratio:        float
    alpha:            Optional[float] = None
    beta:             Optional[float] = None
    ir:               Optional[float] = None
    benchmark_source: str = ""
    bench_data_range: str = ""
    error:            str = ""                                # 非空 = 运行时异常，结果无效
    tradeability_pass:  bool = True
    tradeability_flags: list[str] = field(default_factory=list)  # FAIL/WARN/INFO 消息列表
    rollover_policy:    str = ""                             # 换月规则摘要（期货适用）


@dataclass
class GroupSummary:
    group:           str
    label:           str
    total:           int
    passed:          int
    pass_rate_pct:   float
    median_sharpe:   float
    p25_sharpe:      float
    p75_sharpe:      float
    median_max_dd:   float
    median_oos_ratio: float
    median_alpha:    Optional[float]
    median_ir:       Optional[float]
    verdict:         str           # "GO" / "HOLD" / "NO-GO"
    note:            str = ""
    accessibility_pct:       float = 100.0  # (total - error_count) / total × 100
    tradeability_fail_count: int = 0        # 成交可行性 hard-fail 标的数
    confidence:              str = "MEDIUM" # HIGH / MEDIUM / LOW / INSUFFICIENT_DATA
    confidence_reason:       str = ""
    accessibility_warn_pct:  float = 80.0             # 从 YAML thresholds 外置
    failure_reasons:         dict[str, int] = field(default_factory=dict)  # 排障 topN


@dataclass
class BatchReport:
    run_date:       str
    universe_file:  str
    total_assets:   int
    total_passed:   int
    groups:         list[GroupSummary]
    assets:         list[AssetResult]
    pipeline_version: str = ""
    git_commit:     str = ""
    generated_at:   str = ""


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _safe_median(vals: list[float]) -> float:
    return round(statistics.median(vals), 4) if vals else 0.0

def _safe_quantile(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = (len(s) - 1) * q
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    return round(s[lo] + (s[hi] - s[lo]) * (idx - lo), 4)

def _group_verdict(pass_rate: float, median_sharpe: float) -> tuple[str, str]:
    """
    综合通过率和夏普中位数给出组级判定。
    GO   : pass_rate >= 0.5  且 median_sharpe > 0
    HOLD : pass_rate >= 0.3  或  median_sharpe > 0（但未同时达标）
    NO-GO: 其余
    """
    if pass_rate >= 50.0 and median_sharpe > 0:
        return "GO", "组内过半标的通过、夏普中位数为正，可进入规模化运行"
    if pass_rate >= 30.0 or median_sharpe > 0:
        return "HOLD", "组内有价值信号但覆盖率不足，需追加样本或调参"
    return "NO-GO", "组内通过率低且夏普中位数非正，当前策略在该资产类别无显著优势"


def _check_tradeability(cfg: dict[str, Any]) -> tuple[bool, list[str]]:
    """静态成交可行性约束检查（不依赖实时行情，基于配置参数）。
    返回 (hard_pass, flags)：flags 内容为 prefix|msg：FAIL / WARN / INFO。
    """
    flags: list[str] = []
    hard_fail = False

    td       = cfg.get("tradeability") or {}
    slippage = float(cfg.get("slippage", 0.0))
    tier     = td.get("slippage_tier") or cfg.get("slippage_tier") or "equity"
    thr      = SLIPPAGE_SENSITIVITY.get(tier, SLIPPAGE_SENSITIVITY["equity"])

    # 1. 滑点敏感性分层
    if slippage >= thr["fail"]:
        flags.append(f"FAIL|滑点 {slippage:.4f} 超出 {tier} 层容忍上限 {thr['fail']:.4f}")
        hard_fail = True
    elif slippage >= thr["warn"]:
        flags.append(f"WARN|滑点 {slippage:.4f} 接近 {tier} 层警戞线 {thr['warn']:.4f}")

    # 2. 日均成交额门槛
    min_to = td.get("min_daily_turnover_cny")
    if min_to is None:
        flags.append("WARN|未配置 min_daily_turnover_cny，无法做容量约束校验")
    else:
        max_part = float(td.get("max_participation_rate") or 0.10)
        cap_cny  = float(min_to) * max_part
        flags.append(
            f"INFO|容量上限估算: {cap_cny/1e6:.1f}M CNY"
            f"（日均成交{float(min_to)/1e6:.0f}M × 占比{max_part*100:.0f}%）"
        )

    # 3. 单笔占比上限
    max_part_cfg = td.get("max_participation_rate")
    if max_part_cfg is None:
        flags.append("WARN|未配置 max_participation_rate（建议股票≤10% ETF≤5% 期货≤8%）")
    elif float(max_part_cfg) > 0.20:
        flags.append(
            f"FAIL|max_participation_rate {float(max_part_cfg)*100:.0f}% > 20%，市场冲击风险过高"
        )
        hard_fail = True

    return (not hard_fail), flags


def _format_rollover_policy(cfg: dict[str, Any]) -> str:
    """提取换月规则配置并格式化为人可读摘要。非期货资产返回空串。"""
    rollover = cfg.get("rollover") or {}
    if not rollover and not cfg.get("continuous_symbol"):
        return ""
    method   = rollover.get("method", "unspecified")
    days     = rollover.get("days_before_expiry")
    vol_rat  = rollover.get("volume_ratio_threshold")
    avoid    = rollover.get("avoid_open_on_rollover", False)
    add_slip = rollover.get("rollover_slippage_add", 0.0)
    parts = [f"method={method}"]
    if days:      parts.append(f"到期前{days}日切换")
    if vol_rat:   parts.append(f"次月成交量比>{vol_rat}时提前")
    if avoid:     parts.append("换月日禁开仓")
    if add_slip:  parts.append(f"换月附加滑点+{float(add_slip)*100:.2f}%")
    return " | ".join(parts)


def _compute_confidence(total: int, error_count: int, pass_rate_pct: float) -> tuple[str, str]:
    """基于样本量、错误率、通过率分布计算组级置信标签。"""
    if total == 0 or error_count >= total:
        return "INSUFFICIENT_DATA", "无有效结果"
    error_rate = error_count / total * 100
    valid_n    = total - error_count
    if valid_n >= 4 and error_rate == 0 and 10.0 <= pass_rate_pct <= 90.0:
        return "HIGH", f"样本充足（n={valid_n}）、无异常、通过率分布合理"
    if valid_n >= 3 and error_rate <= 25.0:
        return "MEDIUM", f"样本较充足（n={valid_n}），错误率={error_rate:.0f}%"
    if valid_n >= 2 and error_rate < 50.0:
        return "LOW", f"样本偏少（n={valid_n}）或错误率偏高（{error_rate:.0f}%），结论仅供参考"
    return "INSUFFICIENT_DATA", f"有效样本不足（n={valid_n}，error={error_rate:.0f}%）"


def _categorize_failure_reason(msg: str) -> str:
    """将错误消息或 Flag 内容归类为便于排障的短标签。"""
    m = msg.lower()
    if any(k in m for k in ("滑点", "slippage", "participation", "流动", "turnover")):
        return "流动性/成交约束"
    if any(k in m for k in ("数据", "data", "gap", "coverage", "missing", "缺口", "空窗")):
        return "数据缺口"
    if any(k in m for k in ("rollover", "continuous", "换月", "主力", "合约")):
        return "换月映射缺失"
    if any(k in m for k in ("config", "配置", "yaml", "keyerror", "valueerror")):
        return "配置错误"
    return "回测异常"


# ─────────────────────────────────────────────────────────────────────────────
# 宇宙解析
# ─────────────────────────────────────────────────────────────────────────────

def _build_run_configs(universe: dict[str, Any]) -> list[dict[str, Any]]:
    """
    展开 YAML 为扁平的 run_config 列表，每条对应一个 symbol × group 的 Stage1Runner 调用。
    字段优先级: asset > group > defaults
    """
    defaults = universe.get("defaults", {})
    groups   = universe.get("groups", {})
    configs: list[dict[str, Any]] = []

    for group_key, group_cfg in groups.items():
        assets = group_cfg.get("assets", [])
        for asset in assets:
            # 三层合并：defaults → group → asset
            cfg: dict[str, Any] = {**defaults}
            for k, v in group_cfg.items():
                if k != "assets":
                    cfg[k] = v
            cfg.update(asset)
            cfg["group"] = group_key

            # 期货优先使用连续主力代码，若找不到时回退到 symbol
            cfg["actual_symbol"] = cfg.get("continuous_symbol") or cfg["symbol"]
            if not cfg.get("label"):
                cfg["label"] = group_key
            configs.append(cfg)

    return configs


# ─────────────────────────────────────────────────────────────────────────────
# 单标的运行（包裹 Stage1Runner，捕获异常）
# ─────────────────────────────────────────────────────────────────────────────

def _run_one(cfg: dict[str, Any], dry_run: bool) -> AssetResult:
    actual_symbol   = cfg["actual_symbol"]
    original_symbol = cfg["symbol"]
    name            = cfg.get("name", actual_symbol)
    group           = cfg["group"]

    # 若 continuous_symbol 与 symbol 不同，日志提示
    if actual_symbol != original_symbol:
        print(f"  [INFO] {name}: 使用连续合约 {actual_symbol}（原始: {original_symbol}）")

    try:
        from strategies.stage1_pipeline import Stage1Runner
        runner = Stage1Runner(
            strategy    = f"MA_Cross_{group}",
            symbol      = actual_symbol,
            start       = cfg.get("start", "2020-01-01"),
            end         = cfg.get("end",   "2024-12-31"),
            oos_split   = cfg.get("oos_split", "2023-01-01"),
            short_period= int(cfg.get("short_period", 5)),
            long_period = int(cfg.get("long_period", 20)),
            benchmark   = cfg.get("benchmark", "CSI300"),
            buy_comm    = float(cfg.get("buy_comm",  0.0003)),
            sell_comm   = float(cfg.get("sell_comm", 0.0013)),
            slippage    = float(cfg.get("slippage",  0.001)),
            market_name = cfg.get("market_name", "标准"),
            dry_run     = dry_run,
        )
        result = runner.run()
        bm = result.benchmark_comparison
        td_pass, td_flags = _check_tradeability(cfg)

        return AssetResult(
            group            = group,
            symbol           = actual_symbol,
            original_symbol  = original_symbol,
            name             = name,
            stage1_pass      = result.stage1_pass,
            summary          = result.summary,
            sharpe_full      = result.full_backtest.sharpe_ratio,
            annualized_pct   = result.full_backtest.annualized_return_pct,
            max_drawdown_pct = result.full_backtest.max_drawdown_pct,
            calmar_ratio     = result.full_backtest.calmar_ratio,
            sortino_ratio    = result.full_backtest.sortino_ratio,
            oos_ratio        = result.in_out_comparison.oos_ratio,
            alpha            = bm.alpha          if bm and bm.available else None,
            beta             = bm.beta           if bm and bm.available else None,
            ir               = bm.information_ratio if bm and bm.available else None,
            benchmark_source = (bm.benchmark_source if bm and bm.benchmark_source
                                else cfg.get("benchmark_source", "")),
            bench_data_range = bm.bench_data_range  if bm else "",
            tradeability_pass  = td_pass,
            tradeability_flags = td_flags,
            rollover_policy    = _format_rollover_policy(cfg),
        )
    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        print(f"  [ERROR] {name} ({actual_symbol}): {err_msg}", file=sys.stderr)
        return AssetResult(
            group=group, symbol=actual_symbol, original_symbol=original_symbol,
            name=name, stage1_pass=False,
            summary=f"运行失败: {err_msg}",
            sharpe_full=0.0, annualized_pct=0.0, max_drawdown_pct=0.0,
            calmar_ratio=0.0, sortino_ratio=0.0, oos_ratio=0.0,
            error=err_msg,
            rollover_policy=_format_rollover_policy(cfg),
        )


# ─────────────────────────────────────────────────────────────────────────────
# 分组聚合
# ─────────────────────────────────────────────────────────────────────────────

def _aggregate_group(
    group_key: str,
    label: str,
    results: list[AssetResult],
    thresholds: dict[str, Any] | None = None,
) -> GroupSummary:
    valid     = [r for r in results if not r.error]
    passed    = [r for r in valid if r.stage1_pass]
    pass_rate = round(len(passed) / len(valid) * 100.0, 1) if valid else 0.0

    sharpes   = [r.sharpe_full for r in valid]
    dds       = [r.max_drawdown_pct for r in valid]
    oos_rats  = [r.oos_ratio for r in valid]
    alphas    = [r.alpha for r in valid if r.alpha is not None]
    irs       = [r.ir    for r in valid if r.ir    is not None]

    med_sharpe = _safe_median(sharpes)
    verdict, note = _group_verdict(pass_rate, med_sharpe)

    error_count     = len(results) - len(valid)
    accessibility   = round((len(valid) / len(results) * 100.0) if results else 0.0, 1)
    td_fail_count   = sum(1 for r in valid if not r.tradeability_pass)
    confidence, conf_reason = _compute_confidence(len(results), error_count, pass_rate)

    # failure reason aggregation（排障 topN：错误异常 + FAIL 级 tradeability flag）
    reason_counts: dict[str, int] = {}
    for r in results:
        if r.error:
            cat = _categorize_failure_reason(r.error)
            reason_counts[cat] = reason_counts.get(cat, 0) + 1
        for flag in r.tradeability_flags:
            if flag.startswith("FAIL"):
                cat = _categorize_failure_reason(flag)
                reason_counts[cat] = reason_counts.get(cat, 0) + 1
    failure_reasons = dict(sorted(reason_counts.items(), key=lambda x: -x[1]))
    access_warn = float((thresholds or {}).get("accessibility_warn_pct", 80.0))

    return GroupSummary(
        group           = group_key,
        label           = label,
        total           = len(results),
        passed          = len(passed),
        pass_rate_pct   = pass_rate,
        median_sharpe   = med_sharpe,
        p25_sharpe      = _safe_quantile(sharpes, 0.25),
        p75_sharpe      = _safe_quantile(sharpes, 0.75),
        median_max_dd   = _safe_median(dds),
        median_oos_ratio= _safe_median(oos_rats),
        median_alpha    = _safe_median(alphas)  if alphas else None,
        median_ir       = _safe_median(irs)     if irs    else None,
        verdict         = verdict,
        note            = note,
        accessibility_pct       = accessibility,
        tradeability_fail_count = td_fail_count,
        confidence              = confidence,
        confidence_reason       = conf_reason,
        accessibility_warn_pct  = access_warn,
        failure_reasons         = failure_reasons,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Markdown 报告
# ─────────────────────────────────────────────────────────────────────────────

def _render_markdown(report: BatchReport) -> str:
    lines: list[str] = []
    a = lines.append

    a(f"# Stage 1 批量评测报告")
    a(f"")
    a(f"| 项目 | 值 |")
    a(f"|------|----|")
    a(f"| 生成时间 | {report.generated_at} |")
    a(f"| Pipeline 版本 | `{report.pipeline_version}` |")
    a(f"| Git Commit | `{report.git_commit}` |")
    a(f"| 标的总数 | {report.total_assets} |")
    a(f"| Stage 1 通过 | {report.total_passed} / {report.total_assets} "
      f"（{round(report.total_passed/report.total_assets*100,1) if report.total_assets else 0}%）|")
    a(f"")

    # 分组汇总表
    a("## 分组汇总")
    a("")
    a("| 分组 | 标的数 | 可达率 | 通过率 | 夏普中位 | P25~P75 | 最大回撤中位 | OOS 比中位 | Alpha 中位 | IR 中位 | 成交失败 | 置信度 | 判定 |")
    a("|------|--------|--------|--------|----------|---------|--------------|------------|-----------|---------|----------|--------|------|")
    for g in report.groups:
        alpha_s   = f"{g.median_alpha:.2f}%" if g.median_alpha is not None else "N/A"
        ir_s      = f"{g.median_ir:.2f}"     if g.median_ir    is not None else "N/A"
        verdict_icon = {"GO": "✅ GO", "HOLD": "🟡 HOLD", "NO-GO": "❌ NO-GO"}[g.verdict]
        conf_icon = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🟠", "INSUFFICIENT_DATA": "⚪"}[g.confidence]
        td_s = f"{g.tradeability_fail_count}个失败" if g.tradeability_fail_count else "✔"
        a(f"| {g.label} | {g.total} | {g.accessibility_pct:.0f}% | {g.pass_rate_pct}% "
          f"| {g.median_sharpe:.2f} | {g.p25_sharpe:.2f}~{g.p75_sharpe:.2f} "
          f"| {g.median_max_dd:.1f}% | {g.median_oos_ratio:.2f} "
          f"| {alpha_s} | {ir_s} | {td_s} | {conf_icon} {g.confidence} | {verdict_icon} |")
    a(f"")

    # 每组明细
    for g in report.groups:
        a(f"## {g.label}")
        a(f"")
        group_assets = [ar for ar in report.assets if ar.group == g.group]

        # OOS 参考序列说明（期货/有明确 benchmark_source 时展示，消除解释歧义）
        bm_sources = [ar.benchmark_source for ar in group_assets if ar.benchmark_source]
        if bm_sources:
            unique_sources = list(dict.fromkeys(bm_sources))  # 保序去重
            for src in unique_sources:
                a(f"> **OOS 参考序列**: {src}")
            a(f"")
        a("| 标的 | 名称 | 通过 | 年化% | 夏普 | Calmar | Sortino | 最大回撤% | OOS 比 | Alpha% | Beta | IR | 成交可行 | 换月规则 |")
        a("|------|------|------|-------|------|--------|---------|-----------|--------|--------|------|-----|--------|--------|")
        for ar in group_assets:
            if ar.error:
                a(f"| {ar.symbol} | {ar.name} | ❌ ERROR | — | — | — | — | — | — | — | — | — | — | {ar.error[:40]} |")
                continue
            pass_s   = "✅" if ar.stage1_pass else "❌"
            td_s     = "✅" if ar.tradeability_pass else "❌"
            td_flag  = next((f.split("|", 1)[1] for f in ar.tradeability_flags
                             if f.startswith("FAIL") or f.startswith("WARN")), "")
            alpha_s  = f"{ar.alpha:.2f}" if ar.alpha is not None else "N/A"
            beta_s   = f"{ar.beta:.3f}"  if ar.beta  is not None else "N/A"
            ir_s     = f"{ar.ir:.2f}"    if ar.ir    is not None else "N/A"
            roll_s   = ar.rollover_policy[:35] if ar.rollover_policy else "N/A"
            a(f"| {ar.symbol} | {ar.name} | {pass_s} "
              f"| {ar.annualized_pct:.1f} | {ar.sharpe_full:.2f} "
              f"| {ar.calmar_ratio:.2f} | {ar.sortino_ratio:.2f} "
              f"| {ar.max_drawdown_pct:.1f} | {ar.oos_ratio:.2f} "
              f"| {alpha_s} | {beta_s} | {ir_s} "
              f"| {td_s} {td_flag[:20] if td_flag else ''} | {roll_s} |")
        a(f"")
        a(f"> **判定**: {g.verdict} — {g.note}")
        a(f"> **置信度**: {g.confidence} — {g.confidence_reason}")
        if g.failure_reasons:
            top3 = list(g.failure_reasons.items())[:3]
            reasons_str = "；".join(f"{k}（{v}次）" for k, v in top3)
            a(f"> **排障 Top{len(top3)}**：{reasons_str}")
        if g.tradeability_fail_count:
            a(f"> ⚠️ **成交可行性失败**: {g.tradeability_fail_count} 个标的存在滑点超限或占比预警，仿真盈利却无法成交")
        a(f"")

    # 期货换月与成交可行性附录
    a("## 附：期货换月处理原则")
    a("")
    a("- **商品期货**：优先使用 `continuous_symbol`（复权连续主力）；"
      "换月由 `rollover.method` 控制（到期前N日 / 成交量比触发），换月日禁止新开仓。")
    a("- **股指期货**：`IFM0/ICM0/IMM0` 连续主力；T-3 日换月，换月日禁止新开仓。")
    a("- **换月附加滑点**：已在各标的 `rollover_policy` 字段记录，回测时应叠加到普通滑点之上。")
    a("")
    a("## 附：成交可行性约束参考")
    a("")
    a("| 层级 | 滑点警戒 | 滑点上限 | 适用场景 |")
    a("|------|--------|--------|--------|")
    for t, v in SLIPPAGE_SENSITIVITY.items():
        a(f"| {t} | {v['warn']*100:.2f}% | {v['fail']*100:.2f}% | 见 YAML tradeability.slippage_tier |")
    a("")
    a("> 超出「滑点上限」→ FAIL（tradeability_pass=False）；"
      "超出「警戒」→ WARN。日均成交额和占比阈值见各组 `tradeability` 配置块。")
    a("")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────────────────────────────────────

def run_batch(
    universe_path: pathlib.Path,
    groups_filter: list[str] | None,
    dry_run: bool,
    out_dir: pathlib.Path,
) -> BatchReport:
    universe = _load_yaml(universe_path)
    configs  = _build_run_configs(universe)

    if groups_filter:
        configs = [c for c in configs if c["group"] in groups_filter]
        if not configs:
            print(f"[WARN] 指定的 --groups {groups_filter} 在宇宙中未找到任何标的", file=sys.stderr)

    # 从 stage1_pipeline 取签名
    try:
        from strategies.stage1_pipeline import _pipeline_signature
        sig = _pipeline_signature()
    except Exception:
        sig = {"pipeline_version": "unknown", "git_commit": "unknown",
               "generated_at": datetime.now().isoformat(timespec="seconds")}

    run_date = datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'='*60}")
    print(f" Stage 1 批量评测  |  标的数: {len(configs)}")
    print(f" Pipeline: {sig['pipeline_version']}  commit: {sig['git_commit']}")
    print(f"{'='*60}")

    all_results: list[AssetResult] = []
    # 按 group 顺序逐标的运行
    group_order: list[str] = []
    for cfg in configs:
        g = cfg["group"]
        if g not in group_order:
            group_order.append(g)

    for group_key in group_order:
        group_configs = [c for c in configs if c["group"] == group_key]
        label = group_configs[0].get("label", group_key)
        print(f"\n── 分组: {label} （{len(group_configs)} 个标的）──")
        for cfg in group_configs:
            name = cfg.get("name", cfg["actual_symbol"])
            print(f"\n[{group_key}] {name} ({cfg['actual_symbol']})")
            ar = _run_one(cfg, dry_run)
            all_results.append(ar)

    # 分组聚合
    group_summaries: list[GroupSummary] = []
    for group_key in group_order:
        group_assets = [r for r in all_results if r.group == group_key]
        group_cfgs   = [c for c in configs if c["group"] == group_key]
        label        = group_cfgs[0].get("label", group_key) if group_cfgs else group_key
        thresholds_cfg = group_cfgs[0].get("thresholds") or {} if group_cfgs else {}
        gs = _aggregate_group(group_key, label, group_assets, thresholds_cfg)
        group_summaries.append(gs)
        verdict_icon = {"GO": "✅ GO", "HOLD": "🟡 HOLD", "NO-GO": "❌ NO-GO"}[gs.verdict]
        print(f"\n  [{group_key}] 通过率={gs.pass_rate_pct}%  "
              f"夏普中位={gs.median_sharpe:.2f}  "
              f"最大回撤中位={gs.median_max_dd:.1f}%  "
              f"OOS比中位={gs.median_oos_ratio:.2f}  "
              f"判定={verdict_icon}")

    total_passed = sum(r.stage1_pass for r in all_results if not r.error)
    report = BatchReport(
        run_date         = run_date,
        universe_file    = str(universe_path),
        total_assets     = len(all_results),
        total_passed     = total_passed,
        groups           = group_summaries,
        assets           = all_results,
        pipeline_version = sig["pipeline_version"],
        git_commit       = sig["git_commit"],
        generated_at     = sig["generated_at"],
    )

    # 写 JSON
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"batch_stage1_{run_date}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        # asdict 不直接支持嵌套 Optional，手工序列化
        json.dump({
            "run_date":         report.run_date,
            "universe_file":    report.universe_file,
            "pipeline_version": report.pipeline_version,
            "git_commit":       report.git_commit,
            "generated_at":     report.generated_at,
            "total_assets":     report.total_assets,
            "total_passed":     report.total_passed,
            "slippage_sensitivity": SLIPPAGE_SENSITIVITY,
            "groups":           [asdict(g) for g in report.groups],
            "assets":           [asdict(a) for a in report.assets],
        }, f, ensure_ascii=False, indent=2)
    print(f"\n[Batch] JSON 结果: {json_path}")

    # 写 Markdown
    md_path = out_dir / f"batch_stage1_{run_date}.md"
    md_path.write_text(_render_markdown(report), encoding="utf-8")
    print(f"[Batch] Markdown 报告: {md_path}")

    # 打印组汇总
    print(f"\n{'='*70}")
    print(f" 批量评测汇总")
    print(f" 总通过: {total_passed}/{len(all_results)}")
    for g in group_summaries:
        icon    = {"GO": "✅", "HOLD": "🟡", "NO-GO": "❌"}[g.verdict]
        conf    = {"HIGH": "H", "MEDIUM": "M", "LOW": "L", "INSUFFICIENT_DATA": "?"}[g.confidence]
        td_warn = f"  ⚠️ {g.tradeability_fail_count}成交失败" if g.tradeability_fail_count else ""
        acc_warn = "  🔴可达率低!" if g.accessibility_pct < 80.0 else ""
        print(f"  {icon} [{conf}] {g.label:22s}  {g.passed}/{g.total}"
              f"  夏普={g.median_sharpe:.2f}  可达={g.accessibility_pct:.0f}%"
              f"{td_warn}{acc_warn}  {g.verdict}")
    print(f"{'='*70}")

    # 可达率低于外置阈值时控制台告警（阈值从 YAML thresholds.accessibility_warn_pct 读取）
    low_access = [g for g in group_summaries if g.accessibility_pct < g.accessibility_warn_pct]
    if low_access:
        print(f"\n[ACCESSIBILITY ALERT] 以下分组数据可达率低于告警阈值：")
        for g in low_access:
            print(f"  ⚠️  {g.label}: {g.accessibility_pct:.0f}%"
                  f" < 阈值{g.accessibility_warn_pct:.0f}%"
                  f"（{g.total - int(g.total * g.accessibility_pct / 100)}/{g.total} 标的不可达）")

    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stage 1 多标的跨资产批量评测")
    parser.add_argument("--universe", type=pathlib.Path, default=UNIVERSE_PATH,
                        help="宇宙定义文件路径（默认 config/stage1_universe.yaml）")
    parser.add_argument("--groups", nargs="+", metavar="GROUP",
                        help="只跑指定分组，如 --groups stocks etf")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅数据验收，跳过回测")
    parser.add_argument("--out-dir", type=pathlib.Path, default=RESULTS_DIR,
                        dest="out_dir",
                        help="输出目录（默认 strategies/results/）")
    args = parser.parse_args(argv)

    report = run_batch(
        universe_path  = args.universe,
        groups_filter  = args.groups,
        dry_run        = args.dry_run,
        out_dir        = args.out_dir,
    )
    # 全部通过返回 0；有未通过标的但无全部 NO-GO 返回 1；全 NO-GO 返回 2
    if report.total_passed == report.total_assets:
        return 0
    if any(g.verdict == "GO" for g in report.groups):
        return 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
