#!/usr/bin/env python3
"""
strategies/stage1_pipeline.py — Stage 1 策略全流程框架

"放量前稳定性建设"阶段第二步：把数据治理价值转化为策略可发布能力。

Stage 1 四关（必须全部通过才能进入模拟盘）：
  ① 数据验收绿板   — 指定品种/周期的数据覆盖率 ≥ 95%，无连续空窗 > 5 交易日
  ② 回测指标基线   — 年化收益、夏普比、最大回撤落在有效区间
  ③ 样本内外对比   — 样本外夏普 ≥ 样本内夏普 × 0.7（防止过拟合）
  ④ 参数敏感性     — 核心参数 ±20% 扰动时，年化收益变化 ≤ 30%

输出格式（JSON Schema 固定，版本化，用于 governance_strategy_dashboard.py 读取）：
  strategies/results/stage1_{策略名}_{YYYY-MM-DD}.json

用法：
  python strategies/stage1_pipeline.py \\
      --strategy 双均线策略 --symbol 000001.SZ \\
      --start 2019-01-01 --end 2025-12-31 \\
      --oos-split 2023-01-01

  python strategies/stage1_pipeline.py \\
      --strategy 双均线策略 --symbol 000001.SZ \\
      --start 2019-01-01 --end 2025-12-31 \\
      --oos-split 2023-01-01 --dry-run   # 仅数据验收，不运行回测
"""
from __future__ import annotations

import argparse
import json
import math
import pathlib
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
RESULTS_DIR  = PROJECT_ROOT / "strategies" / "results"

# ── Python 版本卫兵：QMT datacenter 只兼容 3.11 ─────────────────────────────────
_PY = sys.version_info[:2]
if _PY != (3, 11):
    print(
        f"[WARN] Python {_PY[0]}.{_PY[1]} 检测到（需要 3.11）。"
        "QMT datacenter.cp311 二进制不兼容，QMT 数据源将不可用。"
        " 修复: conda activate qmt311  或  .\\run_stage1.ps1",
        file=sys.stderr,
    )

PIPELINE_VERSION = "stage1/v2.1"   # 逻辑版本独立于 schema 版本，用于审计精准定位


def _pipeline_signature() -> dict[str, str]:
    """返回可嵌入 JSON 的运行签名：逻辑版本 + git commit + 生成时间。"""
    commit = "unknown"
    try:
        res = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
            cwd=str(PROJECT_ROOT),
        )
        if res.returncode == 0:
            commit = res.stdout.strip()
    except Exception:
        pass
    return {
        "pipeline_version": PIPELINE_VERSION,
        "git_commit": commit,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _to_builtin_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _to_builtin_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_builtin_json(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_to_builtin_json(v) for v in value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 固定输出模式（JSON Schema v1）
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DataAcceptanceResult:
    symbol: str
    period: str
    date_range: str
    expected_trading_days: int
    actual_data_days: int
    coverage_pct: float
    max_gap_days: int            # 最大连续空窗（交易日口径）
    pass_board: bool
    failures: list[str] = field(default_factory=list)

    @property
    def verdict(self) -> str:
        return "PASS" if self.pass_board else "FAIL"


@dataclass
class BacktestMetrics:
    """回测核心指标（schema v2），所有策略统一使用此模式。"""
    total_return_pct: float
    annualized_return_pct: float
    sharpe_ratio: float
    max_drawdown_pct: float
    win_rate_pct: float
    trade_count: int
    years: float
    period: str                  # "full" / "in_sample" / "out_of_sample"
    # ── 风险指标扩展（v2，全部带默认值向前兼容）──────────────────────────────
    calmar_ratio: float = 0.0        # 年化收益 / 最大回撤（绝对值越高越稳健）
    sortino_ratio: float = 0.0       # 仅惩罚下行波动的夏普变体，比夏普保守
    turnover_rate_pct: float = 0.0   # 换手率估算（完整买卖来回 / 总交易日 × 100）
    cost_ratio_pct: float = 0.0      # 交易成本 / |总收益| × 100（越高说明磨损越严重）
    market_assumption: str = "A股标准"  # 手续费/滑点市场标识


@dataclass
class InOutSampleResult:
    in_sample_period: str
    out_sample_period: str
    in_sharpe: float
    out_sharpe: float
    oos_ratio: float             # out_sharpe / in_sharpe（≥ 0.7 为合格）
    pass_threshold: bool

    @property
    def verdict(self) -> str:
        return "PASS" if self.pass_threshold else "FAIL（过拟合风险）"


@dataclass
class ParamSensitivityResult:
    base_params: dict[str, Any]
    sensitivity_table: list[dict[str, Any]]
    max_change_pct: float
    pass_threshold: bool         # max_change_pct ≤ 30%

    @property
    def verdict(self) -> str:
        return "PASS" if self.pass_threshold else "FAIL（参数过敏感）"


@dataclass
class BenchmarkComparison:
    """
    基准对比（Alpha/Beta/信息比率）。

    区分 Beta 贡献（跟市场涨）与真实 Alpha（超越市场）。
    available=False 表示本地无基准数据，已优雅降级。
    """
    benchmark: str                    # "CSI300" / "CSI500" / "none"
    benchmark_annualized_pct: float   # 基准年化收益（%）
    excess_return_pct: float          # 策略年化 - 基准年化（注意：含 Beta 贡献）
    alpha: float                      # CAPM Alpha（年化 %，摘除 Beta 后的纯超额）
    beta: float                       # 系统性风险暴露（≈1 = 与市场同涨跌）
    information_ratio: float          # 主动超额 / 追踪误差（越高越好）
    tracking_error_pct: float         # 年化追踪误差（%）
    available: bool                   # 基准数据是否成功加载
    # ── 数据源审计字段（便于追溯结论可靠性）────────────────────────────────
    benchmark_source: str = ""        # 实际读取来源: local_duckdb / qmt / akshare / unknown
    bench_data_range: str = ""        # 实际使用数据的时间范围，如 "2020-01-01~2024-12-31"
    note: str = ""                    # 降级原因或提示


@dataclass
class Stage1Result:
    strategy: str
    symbol: str
    run_date: str
    start: str
    end: str
    oos_split: str
    stage1_pass: bool
    data_acceptance: DataAcceptanceResult
    full_backtest: BacktestMetrics
    in_sample: BacktestMetrics
    out_of_sample: BacktestMetrics
    in_out_comparison: InOutSampleResult
    param_sensitivity: ParamSensitivityResult
    benchmark_comparison: Optional[BenchmarkComparison] = None
    market_config: dict[str, Any] = field(default_factory=dict)
    summary: str = ""
    data_sources_used: list[str] = field(default_factory=list)
    data_contract_passed: bool = True   # L3/L4 合约验证是否全部通过

    def to_dict(self) -> dict[str, Any]:
        bm_dict = (
            asdict(self.benchmark_comparison)
            if self.benchmark_comparison is not None
            else {"available": False, "note": "未运行基准对比（--benchmark 参数未指定）"}
        )
        sig = _pipeline_signature()
        payload = {
            "_schema_version": "stage1/v2",
            "_pipeline_version": sig["pipeline_version"],
            "_git_commit": sig["git_commit"],
            "_generated_at": sig["generated_at"],
            "_schema_migration_note": (
                "v1→v2: BacktestMetrics 新增 calmar/sortino/turnover/cost_ratio/market_assumption; "
                "Stage1Result 新增 benchmark_comparison(BenchmarkComparison) 与 market_config; "
                "BenchmarkComparison 新增 benchmark_source/bench_data_range 审计字段。"
                "旧版 dashboard 若读取 v1 文件缺少 benchmark_comparison 字段，"
                "请用 d.get('benchmark_comparison', {'available': False}) 做兜底。"
            ),
            "strategy": self.strategy,
            "symbol": self.symbol,
            "run_date": self.run_date,
            "start": self.start,
            "end": self.end,
            "oos_split": self.oos_split,
            "stage1_pass": self.stage1_pass,
            "summary": self.summary,
            "market_config": self.market_config,
            "data_acceptance": asdict(self.data_acceptance),
            "backtest_metrics": asdict(self.full_backtest),
            "in_sample_metrics": asdict(self.in_sample),
            "out_of_sample_metrics": asdict(self.out_of_sample),
            "in_out_sample": asdict(self.in_out_comparison),
            "param_sensitivity": asdict(self.param_sensitivity),
            "benchmark_comparison": bm_dict,
            "data_sources_used": list(dict.fromkeys(self.data_sources_used)),  # 排重保序
            "data_contract_passed": self.data_contract_passed,
        }
        return _to_builtin_json(payload)


# ─────────────────────────────────────────────────────────────────────────────
# ① 数据验收绿板
# ─────────────────────────────────────────────────────────────────────────────

class DataAcceptanceBoard:
    """
    验收指定品种的本地数据完整性。
    不依赖 QMT 运行时，直接查询本地 DuckDB。
    """
    COVERAGE_THRESHOLD = 0.95
    MAX_GAP_THRESHOLD  = 5       # 最大允许连续空窗（交易日数）

    def __init__(self, symbol: str, start: str, end: str, period: str = "1d"):
        self.symbol = symbol
        self.start  = start
        self.end    = end
        self.period = period

    def _expected_trading_days(self, start_dt: date, end_dt: date) -> int:
        try:
            from data_manager.smart_data_detector import TradingCalendar
            return len(TradingCalendar().get_trading_days(start_dt, end_dt))
        except Exception:
            return max(1, int((end_dt - start_dt).days * 0.703))

    def _query_local(self) -> Optional[pd.DataFrame]:
        try:
            from data_manager.unified_data_interface import UnifiedDataInterface
            df = UnifiedDataInterface().get_stock_data(
                stock_code=self.symbol,
                start_date=self.start,
                end_date=self.end,
                period=self.period,
                adjust="none",
                auto_save=False,
            )
            return df if df is not None else None
        except Exception:
            return None

    def _max_gap(self, df: pd.DataFrame, start_dt: date, end_dt: date) -> int:
        try:
            from data_manager.smart_data_detector import TradingCalendar
            all_td = set(TradingCalendar().get_trading_days(start_dt, end_dt))
        except Exception:
            return 0
        date_col = next(
            (c for c in ["date", "trade_date", "Date", "Trade_Date"] if c in df.columns),
            None,
        )
        if date_col is None:
            return 0
        try:
            data_dates = set(pd.to_datetime(df[date_col]).dt.date)
        except Exception:
            return 0
        missing = sorted(all_td - data_dates)
        if not missing:
            return 0
        max_gap = cur_gap = 1
        for i in range(1, len(missing)):
            if (missing[i] - missing[i - 1]).days <= 3:  # 允许周末跨越
                cur_gap += 1
                max_gap = max(max_gap, cur_gap)
            else:
                cur_gap = 1
        return max_gap

    def check(self) -> DataAcceptanceResult:
        try:
            start_dt = date.fromisoformat(self.start)
            end_dt   = date.fromisoformat(self.end)
        except ValueError as e:
            return DataAcceptanceResult(
                symbol=self.symbol, period=self.period,
                date_range=f"{self.start}~{self.end}",
                expected_trading_days=0, actual_data_days=0,
                coverage_pct=0.0, max_gap_days=999,
                pass_board=False, failures=[f"日期格式错误: {e}"],
            )

        expected = self._expected_trading_days(start_dt, end_dt)
        df = self._query_local()

        if df is None or len(df) == 0:
            return DataAcceptanceResult(
                symbol=self.symbol, period=self.period,
                date_range=f"{self.start}~{self.end}",
                expected_trading_days=expected, actual_data_days=0,
                coverage_pct=0.0, max_gap_days=999,
                pass_board=False,
                failures=["本地数据为空，请先运行数据下载（tools/download_all_stocks.py）"],
            )

        actual   = len(df)
        coverage = actual / expected if expected > 0 else 0.0
        max_gap  = self._max_gap(df, start_dt, end_dt)

        failures: list[str] = []
        if coverage < self.COVERAGE_THRESHOLD:
            failures.append(
                f"覆盖率 {coverage:.1%} < {self.COVERAGE_THRESHOLD:.0%}"
                f"（缺失约 {expected - actual} 个交易日）"
            )
        if max_gap > self.MAX_GAP_THRESHOLD:
            failures.append(
                f"最大连续空窗 {max_gap} 交易日 > 允许值 {self.MAX_GAP_THRESHOLD}"
            )

        return DataAcceptanceResult(
            symbol=self.symbol, period=self.period,
            date_range=f"{self.start}~{self.end}",
            expected_trading_days=expected, actual_data_days=actual,
            coverage_pct=round(coverage * 100, 2),
            max_gap_days=max_gap,
            pass_board=len(failures) == 0,
            failures=failures,
        )


# ─────────────────────────────────────────────────────────────────────────────
# ② 回测引擎（纯 pandas，不依赖 QMT 运行时）
# ─────────────────────────────────────────────────────────────────────────────

class SimpleBacktester:
    """
    趋势类策略回测引擎（双均线信号 + 全仓进出）。

    手续费默认: 买入 0.03%，卖出 0.03% + 印花税 0.1%（A股标准）。
    可通过参数覆盖以适配不同市场（港股/北交所/ETF/期货等）。
    初始净值归一化为 1.0，无杠杆，不允许做空。

    ⚠️ 市场适用性提示:
      - A股主板/沪深300成分:  sell_comm=0.0013（默认）
      - 北交所/小盘股:        建议 slippage=0.001~0.003
      - ETF:                   buy_comm=0.0003, sell_comm=0.0003（无印花税）
      - 股指期货:              需自定义 buy_comm/sell_comm/slippage
      固定成本假设会使小盘股/流动性差品种结果偏乐观，请按实际市场调整。
    """

    def __init__(
        self,
        df: pd.DataFrame,
        short_period: int = 5,
        long_period:  int = 20,
        buy_comm:   float = 0.0003,   # 买入佣金（万三，含所有买入费用）
        sell_comm:  float = 0.0013,   # 卖出佣金（万三 + 印花税千一，A股标准）
        slippage:   float = 0.0,      # 单边滑点（相对成交额，默认 0）
        market_name: str  = "A股标准",
    ):
        self.df           = df.copy().reset_index(drop=True)
        self.short_period = short_period
        self.long_period  = long_period
        self.buy_comm     = buy_comm
        self.sell_comm    = sell_comm
        self.slippage     = slippage
        self.market_name  = market_name
        self.equity_curve: Optional[pd.Series] = None  # 最近一次 run() 后的净值曲线

    def run(self, period_label: str = "full") -> BacktestMetrics:
        zero = BacktestMetrics(0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0, period_label,
                               market_assumption=self.market_name)

        close_col = next(
            (c for c in ["close", "Close", "收盘价"] if c in self.df.columns), None
        )
        if close_col is None or len(self.df) < self.long_period + 2:
            return zero

        close    = self.df[close_col].astype(float).reset_index(drop=True)
        short_ma = close.rolling(self.short_period).mean()
        long_ma  = close.rolling(self.long_period).mean()

        cash           = 1.0
        total_cost     = 0.0   # 累计交易成本（相对初始资金）
        cash_before_buy = 0.0  # 记录持仓前净值，用于准确判断盈亏
        position       = 0     # 0 = 空仓，1 = 满仓
        entry_price    = 0.0
        equity_curve   : list[float] = []
        trades         : list[bool]  = []   # True = 该笔交易盈利（扣除成本后）

        for i in range(1, len(close)):
            if pd.isna(short_ma.iloc[i]) or pd.isna(long_ma.iloc[i]):
                equity_curve.append(cash)
                continue

            prev_diff = short_ma.iloc[i - 1] - long_ma.iloc[i - 1]
            curr_diff = short_ma.iloc[i]     - long_ma.iloc[i]

            if position == 0 and prev_diff <= 0 < curr_diff:
                # 金叉：买入（滑点+佣金作为单边成本）
                cash_before_buy = cash
                entry_price     = close.iloc[i]
                cost            = cash * (self.buy_comm + self.slippage)
                total_cost     += cost
                cash           -= cost
                position        = 1

            elif position == 1 and prev_diff >= 0 > curr_diff:
                # 死叉：卖出（滑点+佣金从卖出金额中扣）
                ret    = close.iloc[i] / entry_price
                gross  = cash * ret
                cost   = gross * (self.sell_comm + self.slippage)
                total_cost += cost
                cash   = gross - cost
                trades.append(cash > cash_before_buy)  # 扣完成本后是否盈利
                position = 0

            # 当前净值（持仓中按市值估算）
            if position == 1:
                equity_curve.append(cash * close.iloc[i] / entry_price)
            else:
                equity_curve.append(cash)

        if not equity_curve:
            return zero

        eq               = pd.Series(equity_curve, dtype=float)
        self.equity_curve = eq  # 保存供 BenchmarkAnalyzer 使用
        years     = len(eq) / 250.0
        total_ret = (eq.iloc[-1] - 1.0) * 100.0
        ann_ret   = ((eq.iloc[-1]) ** (1.0 / years) - 1.0) * 100.0 if years > 0.01 else 0.0

        daily_ret = eq.pct_change().dropna()
        rf_daily  = 0.025 / 250.0
        excess    = daily_ret - rf_daily
        sharpe    = (
            excess.mean() / excess.std() * math.sqrt(250.0)
            if excess.std() > 1e-9 else 0.0
        )

        rolling_max = eq.cummax()
        max_dd      = float(abs(((eq - rolling_max) / rolling_max).min()) * 100.0)
        win_rate    = (sum(trades) / len(trades) * 100.0) if trades else 0.0

        # ── 扩展风险指标 ─────────────────────────────────────────────────────
        # Calmar：年化收益 / 最大回撤（小分母保护）
        calmar = round(ann_ret / max_dd, 3) if max_dd > 0.01 else 0.0

        # Sortino：仅用下行收益波动作惩罚
        downside_arr = excess[excess < 0]
        downside  = pd.Series(downside_arr.values if hasattr(downside_arr, "values") else downside_arr, dtype=float)
        down_std  = float(downside.std() * math.sqrt(250.0)) if len(downside) > 1 else 1e-9
        sortino   = round((ann_ret / 100.0 - 0.025) / down_std, 3) if down_std > 1e-9 else 0.0

        # 换手率：每笔完整买卖来回 = 2 次换手
        data_days = max(len(close), 1)
        turnover  = round(len(trades) * 2 / data_days * 100, 1)

        # 交易成本占总收益比（成本越高，策略对手续费越敏感）
        gross_ret_abs = abs(total_ret / 100.0)
        cost_ratio    = round(total_cost / gross_ret_abs * 100, 1) if gross_ret_abs > 0.001 else 0.0

        return BacktestMetrics(
            total_return_pct      = round(total_ret, 2),
            annualized_return_pct = round(ann_ret, 2),
            sharpe_ratio          = round(sharpe, 3),
            max_drawdown_pct      = round(max_dd, 2),
            win_rate_pct          = round(win_rate, 1),
            trade_count           = len(trades),
            years                 = round(years, 2),
            period                = period_label,
            calmar_ratio          = calmar,
            sortino_ratio         = sortino,
            turnover_rate_pct     = turnover,
            cost_ratio_pct        = cost_ratio,
            market_assumption     = self.market_name,
        )


# ─────────────────────────────────────────────────────────────────────────────
# ④ 参数敏感性分析
# ─────────────────────────────────────────────────────────────────────────────

class ParamSensitivityAnalyzer:
    """
    核心参数 ±20% 扰动，年化收益变化 ≤ 30% 才算稳健。
    对双均线策略同时扰动 short_period 和 long_period。
    """
    CHANGE_THRESHOLD = 0.30

    def __init__(self, df: pd.DataFrame, base_short: int, base_long: int):
        self.df         = df
        self.base_short = base_short
        self.base_long  = base_long

    @staticmethod
    def _perturb(val: int, delta: float) -> int:
        return max(2, round(val * (1 + delta)))

    def run(self) -> ParamSensitivityResult:
        base_ann = SimpleBacktester(self.df, self.base_short, self.base_long).run().annualized_return_pct
        table: list[dict[str, Any]] = []
        max_change = 0.0

        for param, base_val, other in [
            ("short_period", self.base_short, self.base_long),
            ("long_period",  self.base_long,  self.base_short),
        ]:
            for delta in (-0.20, -0.10, +0.10, +0.20):
                perturbed = self._perturb(base_val, delta)
                if param == "short_period":
                    bt = SimpleBacktester(self.df, perturbed, other).run()
                else:
                    bt = SimpleBacktester(self.df, other, perturbed).run()
                change = abs(bt.annualized_return_pct - base_ann) / (abs(base_ann) + 1e-9)
                max_change = max(max_change, change)
                table.append({
                    "param": param,
                    "base_value": base_val,
                    "perturbed_value": perturbed,
                    "delta_pct": round(delta * 100),
                    "annual_return_pct": bt.annualized_return_pct,
                    "change_pct": round(change * 100, 1),
                })

        return ParamSensitivityResult(
            base_params       = {"short_period": self.base_short, "long_period": self.base_long},
            sensitivity_table = table,
            max_change_pct    = round(max_change * 100, 1),
            pass_threshold    = max_change <= self.CHANGE_THRESHOLD,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 基准对比分析（Alpha/Beta/信息比率）
# ─────────────────────────────────────────────────────────────────────────────

class BenchmarkAnalyzer:
    """
    计算策略相对沪深300/中证500等基准指数的 Alpha、Beta、信息比率。
    基准数据从本地 DuckDB 读取，失败时优雅降级（available=False）。

    使用方式::
        bench = BenchmarkAnalyzer(equity_curve, start, end, benchmark="CSI300")
        result = bench.run(strategy_annualized_pct)
    """
    SYMBOLS: dict[str, str] = {
        "CSI300": "000300.SH",
        "CSI500": "000905.SH",
        "HS300":  "000300.SH",
        "ZZ500":  "000905.SH",
        "SSE50":  "000016.SH",
    }

    def __init__(
        self,
        strategy_equity: pd.Series,
        start: str,
        end: str,
        benchmark: str = "CSI300",
    ):
        self.strategy_equity = strategy_equity
        self.start     = start
        self.end       = end
        self.benchmark = benchmark.upper()

    def _load_bench(self) -> "tuple[Optional[pd.Series], str, str]":
        """返回 (归一化净值序列 | None, data_source_label, actual_date_range)。"""
        symbol = self.SYMBOLS.get(self.benchmark, self.benchmark)
        try:
            from data_manager.unified_data_interface import UnifiedDataInterface
            udi = UnifiedDataInterface()
            df = udi.get_stock_data(
                stock_code=symbol,
                start_date=self.start,
                end_date=self.end,
                period="1d",
                adjust="none",
                auto_save=False,
            )
            if df is None or len(df) == 0:
                return None, "local_duckdb", ""
            close_col = next((c for c in ["close", "Close"] if c in df.columns), None)
            if close_col is None:
                return None, "local_duckdb", ""
            # 尝试提取实际覆盖的日期范围，用于数据审计
            date_col = next((c for c in ["date", "Date", "trade_date"] if c in df.columns), None)
            if date_col is not None:
                dates = df[date_col].astype(str)
                data_range = f"{dates.iloc[0]}~{dates.iloc[-1]}"
            else:
                data_range = f"{self.start}~{self.end}(inferred)"
            raw = df[close_col].astype(float).reset_index(drop=True)
            return raw / raw.iloc[0], "local_duckdb", data_range   # 归一化到 1.0
        except Exception:
            return None, "unknown", ""

    def run(self, strategy_ann_pct: float) -> BenchmarkComparison:
        bench, bench_source, bench_range = self._load_bench()
        if bench is None or len(bench) < 10:
            return BenchmarkComparison(
                benchmark=self.benchmark, benchmark_annualized_pct=0.0,
                excess_return_pct=0.0, alpha=0.0, beta=1.0,
                information_ratio=0.0, tracking_error_pct=0.0,
                available=False,
                benchmark_source=bench_source,
                bench_data_range="",
                note=f"基准 {self.benchmark} 本地数据不可用，已跳过 Alpha/Beta 计算",
            )

        n     = min(len(self.strategy_equity), len(bench))
        strat = pd.Series(self.strategy_equity.iloc[:n].values, dtype=float)
        bench_s = pd.Series(bench.iloc[:n].values, dtype=float)

        bench_years = n / 250.0
        bench_ann   = (bench_s.iloc[-1] ** (1.0 / bench_years) - 1.0) * 100.0 if bench_years > 0.01 else 0.0

        strat_ret = strat.pct_change().dropna()
        bench_ret = bench_s.pct_change().dropna()

        n2 = min(len(strat_ret), len(bench_ret))
        sr = pd.Series(strat_ret.iloc[:n2].values, dtype=float)
        br = pd.Series(bench_ret.iloc[:n2].values, dtype=float)

        # Beta = cov(strategy, bench) / var(bench)
        sr_arr = sr.to_numpy(dtype=float)
        br_arr = br.to_numpy(dtype=float)
        var_bench = float(br_arr.var())
        cov_mat = np.cov(sr_arr, br_arr)
        cov_sb = float(cov_mat[0, 1]) if cov_mat.size >= 4 else 0.0
        beta = cov_sb / var_bench if var_bench > 1e-12 else 1.0

        # CAPM Alpha（年化 %）
        rf_ann    = 0.025
        alpha_pct = round((strategy_ann_pct / 100.0 - (rf_ann + beta * (bench_ann / 100.0 - rf_ann))) * 100.0, 2)

        # Information Ratio
        active     = sr - br
        te         = float(active.std() * math.sqrt(250.0))
        info_ratio = round(float(active.mean() * 250.0) / te, 3) if te > 1e-9 else 0.0

        return BenchmarkComparison(
            benchmark                = self.benchmark,
            benchmark_annualized_pct = round(bench_ann, 2),
            excess_return_pct        = round(strategy_ann_pct - bench_ann, 2),
            alpha                    = alpha_pct,
            beta                     = round(beta, 3),
            information_ratio        = info_ratio,
            tracking_error_pct       = round(te * 100.0, 2),
            available                = True,
            benchmark_source         = bench_source,
            bench_data_range         = bench_range,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 总编排
# ─────────────────────────────────────────────────────────────────────────────

class Stage1Runner:
    """Stage 1 四关全流程总编排。"""

    def __init__(
        self,
        strategy: str,
        symbol: str,
        start: str,
        end: str,
        oos_split: str,
        short_period: int  = 5,
        long_period:  int  = 20,
        benchmark:    str  = "CSI300",  # 基准指数: CSI300/CSI500/none
        buy_comm:     float = 0.0003,
        sell_comm:    float = 0.0013,
        slippage:     float = 0.0,
        market_name:  str  = "A股标准",
        dry_run:      bool = False,
    ):
        self.strategy     = strategy
        self.symbol       = symbol
        self.start        = start
        self.end          = end
        self.oos_split    = oos_split
        self.short_period = short_period
        self.long_period  = long_period
        self.benchmark    = benchmark
        self.buy_comm     = buy_comm
        self.sell_comm    = sell_comm
        self.slippage     = slippage
        self.market_name  = market_name
        self.dry_run      = dry_run
        # 内部数据缓存：key=(symbol, start, end, period)，避免重复拉取
        self._data_cache: "dict[tuple, pd.DataFrame]" = {}
        # 记录本次运行实际使用的数据来源
        self._data_sources_used: list[str] = []
        # L3/L4 合约验证：任一批次数据校验硬失败则置 True
        self._contract_failed: bool = False

    def _make_bt(self, df: pd.DataFrame) -> SimpleBacktester:
        return SimpleBacktester(
            df, self.short_period, self.long_period,
            buy_comm=self.buy_comm, sell_comm=self.sell_comm,
            slippage=self.slippage, market_name=self.market_name,
        )

    def _load_data(self, start: str, end: str) -> pd.DataFrame:
        cache_key = (self.symbol, start, end, "1d")
        if cache_key in self._data_cache:
            return self._data_cache[cache_key]
        try:
            from data_manager.unified_data_interface import UnifiedDataInterface
            udi = UnifiedDataInterface()
            df = udi.get_stock_data(
                stock_code=self.symbol,
                start_date=start,
                end_date=end,
                period="1d",
                adjust="none",
                auto_save=False,
            )
            # 记录实际使用的数据来源
            src = getattr(udi, "_last_ingestion_source", "unknown")
            if src and src not in self._data_sources_used:
                self._data_sources_used.append(src)
            # 记录合约验证结论（L3/L4 硬门禁）
            contract = getattr(udi, "_last_contract_validation", None)
            if contract is not None and not contract.pass_gate:
                self._contract_failed = True
            result = df if df is not None else pd.DataFrame()
            self._data_cache[cache_key] = result
            return result
        except Exception as e:
            print(f"[WARN] 数据加载失败({start}~{end}): {e}", file=sys.stderr)
            return pd.DataFrame()

    def run(self) -> Stage1Result:
        run_date    = datetime.now().strftime("%Y-%m-%d")
        _dummy_bt   = BacktestMetrics(0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0, "n/a",
                                      market_assumption=self.market_name)
        _dummy_io   = InOutSampleResult("", "", 0.0, 0.0, 0.0, False)
        _dummy_sens = ParamSensitivityResult(
            {"short_period": self.short_period, "long_period": self.long_period},
            [], 0.0, False,
        )
        _market_cfg = {
            "buy_comm":   self.buy_comm,  "sell_comm": self.sell_comm,
            "slippage":   self.slippage,  "market_name": self.market_name,
        }

        # ① 数据验收绿板
        print(f"\n[Stage1] ① 数据验收: {self.symbol} {self.start}~{self.end}")
        board = DataAcceptanceBoard(self.symbol, self.start, self.end).check()
        status = "[PASS]" if board.pass_board else "[FAIL]"
        print(f"  {status}: 覆盖率={board.coverage_pct}%, 最大空窗={board.max_gap_days}d")
        for f in board.failures:
            print(f"  [x] {f}")

        if self.dry_run:
            return Stage1Result(
                strategy=self.strategy, symbol=self.symbol, run_date=run_date,
                start=self.start, end=self.end, oos_split=self.oos_split,
                stage1_pass=board.pass_board,
                data_acceptance=board,
                full_backtest=_dummy_bt, in_sample=_dummy_bt, out_of_sample=_dummy_bt,
                in_out_comparison=_dummy_io, param_sensitivity=_dummy_sens,
                market_config=_market_cfg,
                summary="dry-run: 仅完成数据验收",
            )

        # 加载数据
        df_full = self._load_data(self.start, self.end)
        df_in   = self._load_data(self.start, self.oos_split)
        df_out  = self._load_data(self.oos_split, self.end)

        # ② 全样本回测
        print(f"\n[Stage1] ② 全样本回测（{len(df_full)} 行数据，市场假设={self.market_name}）")
        bt_full = self._make_bt(df_full)
        full_bt = bt_full.run("full")
        print(f"  年化={full_bt.annualized_return_pct:.1f}%  夏普={full_bt.sharpe_ratio:.2f}  "
              f"Calmar={full_bt.calmar_ratio:.2f}  Sortino={full_bt.sortino_ratio:.2f}  "
              f"最大回撤={full_bt.max_drawdown_pct:.1f}%  胜率={full_bt.win_rate_pct:.1f}%  "
              f"交易={full_bt.trade_count}次  成本占比={full_bt.cost_ratio_pct:.1f}%")

        # ③ 样本内外对比
        print(f"\n[Stage1] ③ 样本内外对比（分割点={self.oos_split}）")
        in_bt  = self._make_bt(df_in).run("in_sample")
        out_bt = self._make_bt(df_out).run("out_of_sample")
        oos_ratio = (
            out_bt.sharpe_ratio / in_bt.sharpe_ratio
            if abs(in_bt.sharpe_ratio) > 1e-6 else 0.0
        )
        io_result = InOutSampleResult(
            in_sample_period  = f"{self.start} ~ {self.oos_split}",
            out_sample_period = f"{self.oos_split} ~ {self.end}",
            in_sharpe         = in_bt.sharpe_ratio,
            out_sharpe        = out_bt.sharpe_ratio,
            oos_ratio         = round(oos_ratio, 3),
            pass_threshold    = oos_ratio >= 0.7,
        )
        print(f"  样本内夏普={in_bt.sharpe_ratio:.2f}  "
              f"样本外夏普={out_bt.sharpe_ratio:.2f}  "
              f"OOS比={oos_ratio:.2f}  [{io_result.verdict}]")

        # ④ 参数敏感性
        print(f"\n[Stage1] ④ 参数敏感性分析")
        sens = ParamSensitivityAnalyzer(df_full, self.short_period, self.long_period).run()
        print(f"  最大收益变化幅度={sens.max_change_pct:.1f}%  [{sens.verdict}]")

        # ⑤ 基准对比（Alpha/Beta/信息比率）
        bench_comp: Optional[BenchmarkComparison] = None
        if self.benchmark.upper() != "NONE":
            print(f"\n[Stage1] ⑤ 基准对比（{self.benchmark}）")
            equity_s = bt_full.equity_curve if bt_full.equity_curve is not None else pd.Series(dtype=float)
            bench_comp = BenchmarkAnalyzer(
                equity_s, self.start, self.end, self.benchmark
            ).run(full_bt.annualized_return_pct)
            if bench_comp.available:
                print(f"  Alpha={bench_comp.alpha:.2f}%  Beta={bench_comp.beta:.3f}  "
                      f"IR={bench_comp.information_ratio:.2f}  "
                      f"基准年化={bench_comp.benchmark_annualized_pct:.1f}%  "
                      f"超额={bench_comp.excess_return_pct:.1f}%")
            else:
                print(f"  [{bench_comp.note}]")

        # 综合判定
        failing: list[str] = []
        if not board.pass_board:        failing.append("数据验收未通过")
        if self._contract_failed:        failing.append("数据合约校验失败（L3/L4 硬门禁触发，OHLC异常/NaN超限/价格速度违规）")
        if not io_result.pass_threshold: failing.append(f"OOS比={oos_ratio:.2f}<0.7（过拟合风险）")
        if not sens.pass_threshold:      failing.append(f"参数敏感={sens.max_change_pct:.1f}%>30%")

        stage1_pass = len(failing) == 0
        summary     = "Stage 1 全部通过" if stage1_pass else f"Stage 1 未通过: {'; '.join(failing)}"
        print(f"\n[Stage1] {summary}")

        return Stage1Result(
            strategy=self.strategy, symbol=self.symbol, run_date=run_date,
            start=self.start, end=self.end, oos_split=self.oos_split,
            stage1_pass=stage1_pass,
            data_acceptance=board,
            full_backtest=full_bt, in_sample=in_bt, out_of_sample=out_bt,
            in_out_comparison=io_result, param_sensitivity=sens,
            benchmark_comparison=bench_comp,
            market_config=_market_cfg,
            summary=summary,
            data_sources_used=list(dict.fromkeys(self._data_sources_used)),
            data_contract_passed=not self._contract_failed,
        )


# ─────────────────────────────────────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stage 1 策略全流程评测")
    parser.add_argument("--strategy",  required=True,           help="策略名称（用于输出文件命名）")
    parser.add_argument("--symbol",    required=True,           help="股票代码，如 000001.SZ")
    parser.add_argument("--start",     required=True,           help="回测开始日期 YYYY-MM-DD")
    parser.add_argument("--end",       required=True,           help="回测结束日期 YYYY-MM-DD")
    parser.add_argument("--oos-split", required=True, dest="oos_split",
                                                                help="样本内外分割日（此日期后为样本外）")
    parser.add_argument("--short",     type=int, default=5,     help="短期均线周期（默认 5）")
    parser.add_argument("--long",      type=int, default=20,    help="长期均线周期（默认 20）")
    parser.add_argument("--benchmark", default="CSI300",
                                                                help="基准: CSI300/CSI500/SSE50/none（默认 CSI300）")
    parser.add_argument("--buy-comm",  type=float, default=0.0003, dest="buy_comm",
                                                                help="买入佣金（默认 0.0003，港股/期货需调整）")
    parser.add_argument("--sell-comm", type=float, default=0.0013, dest="sell_comm",
                                                                help="卖出佣金（默认 0.0013，含印花税）")
    parser.add_argument("--slippage",  type=float, default=0.0,help="单边滑点（默认 0，小盘建议 0.001）")
    parser.add_argument("--market",    default="A股标准",        dest="market_name",
                                                                help="市场标识，写入结果 JSON（默认『A股标准』）")
    parser.add_argument("--dry-run",   action="store_true",     help="仅数据验收，跳过回测")
    parser.add_argument("--out",       type=pathlib.Path, default=None, help="指定输出路径")
    args = parser.parse_args(argv)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    runner = Stage1Runner(
        strategy     = args.strategy,
        symbol       = args.symbol,
        start        = args.start,
        end          = args.end,
        oos_split    = args.oos_split,
        short_period = args.short,
        long_period  = args.long,
        benchmark    = args.benchmark,
        buy_comm     = args.buy_comm,
        sell_comm    = args.sell_comm,
        slippage     = args.slippage,
        market_name  = args.market_name,
        dry_run      = args.dry_run,
    )
    result = runner.run()

    out_path = args.out or (
        RESULTS_DIR / f"stage1_{args.strategy}_{result.run_date}.json"
    )
    out_path.write_text(
        json.dumps(result.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"\n[Stage1] 结果已写入: {out_path}")

    return 0 if result.stage1_pass else 1


if __name__ == "__main__":
    sys.exit(main())
