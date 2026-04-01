"""
数据合约验证器（L3 数据契约归一化 + L4 数据质量硬门禁）

职责：
  对任意来源的 DataFrame 执行两层检查：
  - L3（字段规范性）：关键列存在、NaN 率、正价格率
  - L4（统计合理性）：OHLC 逻辑关系、A 股价格速度约束、成交量零值率

验证层级：
  Hard gate（任何一项违规 → pass_gate=False，上游需阻断/告警）:
    - 关键列存在性  : open / high / low / close 四列必须存在
    - OHLC 合理性   : high >= max(open,close), low <= min(open,close), 合规率 >= 99%
    - 价格正值率    : 全部 OHLC 列 > 0, 正值率 >= 99%
    - 关键列 NaN 率 : close/open/high/low 各列 NaN 率 < 1%
    - 价格速度极值  : |日收益率| > 21% 的行占比 < 1%（A 股涨跌停 +1% 缓冲）

  Soft gate（不阻断，仅告警）:
    - 成交量零值率 > 5%
    - 零星价格速度异常（行占比 < 1%）

不依赖数据库，纯 pandas 计算，可用于任何阶段的离线验证。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ─────────────────────────────────────── 阈值常量 ─────────────────────────────
OHLC_SANITY_MIN_PCT    = 0.99   # OHLC 关系合规率最低要求
POSITIVE_PRICE_MIN_PCT = 0.99   # 正价格率最低要求
NAN_RATIO_HARD_MAX     = 0.01   # 关键列 NaN 率硬限（1%）
VELOCITY_HARD_LIMIT    = 0.21   # A 股涨跌停 + 1% 缓冲（硬判阈值）
VELOCITY_HARD_ROW_PCT  = 0.01   # 超速行占比超此值则触发硬失败
VOLUME_ZERO_SOFT_MAX   = 0.05   # 成交量零值率软限（5%）


# ─────────────────────────────────────── 数据结构 ─────────────────────────────

@dataclass
class ContractViolation:
    """单条合约违规记录。"""
    check: str          # 检查项名称（供调用方精确匹配）
    severity: str       # "hard" | "soft"
    detail: str         # 人类可读描述
    value: float = 0.0  # 发生率或计数（便于与阈值对比）


@dataclass
class ContractValidationResult:
    """数据合约验证结论（per-DataFrame）。"""
    symbol: str
    source: str
    rows: int                                    # 总行数

    # ── OHLC 指标 ──────────────────────────────────────────────────────────
    ohlc_sanity_pct: float = 1.0                 # OHLC 关系合规率
    positive_price_pct: float = 1.0              # 正价格率
    nan_ratios: dict = field(default_factory=dict)  # 各关键列 NaN 率

    # ── 速度指标 ────────────────────────────────────────────────────────────
    velocity_violation_count: int = 0            # |pct_change| > LIMIT 的行数
    velocity_violation_pct: float = 0.0          # 占比

    # ── 成交量指标 ──────────────────────────────────────────────────────────
    volume_zero_pct: float = 0.0

    # ── 门禁结论 ────────────────────────────────────────────────────────────
    pass_gate: bool = True
    violations: list = field(default_factory=list)   # list[ContractViolation]

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "source": self.source,
            "rows": self.rows,
            "ohlc_sanity_pct": round(self.ohlc_sanity_pct, 4),
            "positive_price_pct": round(self.positive_price_pct, 4),
            "nan_ratios": {k: round(v, 4) for k, v in self.nan_ratios.items()},
            "velocity_violation_count": self.velocity_violation_count,
            "velocity_violation_pct": round(self.velocity_violation_pct, 4),
            "volume_zero_pct": round(self.volume_zero_pct, 4),
            "pass_gate": self.pass_gate,
            "violations": [
                {
                    "check": v.check,
                    "severity": v.severity,
                    "detail": v.detail,
                    "value": round(v.value, 4),
                }
                for v in self.violations
            ],
        }


# ─────────────────────────────── 交易日历惰性单例 ─────────────────────────────
# 模块级单例，首次调用 _get_trading_calendar() 时懒加载，避免循环导入和启动时网络请求。
_trading_calendar_instance: Optional[Any] = None


def _get_trading_calendar() -> Any:
    """返回 TradingCalendar 的模块级惰性单例。"""
    global _trading_calendar_instance
    if _trading_calendar_instance is None:
        from data_manager.smart_data_detector import TradingCalendar  # 延迟导入避免循环
        _trading_calendar_instance = TradingCalendar()
    return _trading_calendar_instance


# ─────────────────────────────────────── 验证器主体 ───────────────────────────

class DataContractValidator:
    """
    数据合约验证器。

    对 DataFrame 执行 L3（字段规范性）+ L4（统计合理性）检查。
    不依赖数据库，纯 pandas 计算。适用于任何来源的数据。

    用法::

        validator = DataContractValidator()
        result = validator.validate(df, symbol="000001.SZ", source="akshare")
        if not result.pass_gate:
            raise DataContractError(result)
    """

    def validate(
        self,
        df: pd.DataFrame,
        symbol: str,
        source: str = "unknown",
        period: str = "1d",
    ) -> ContractValidationResult:
        """
        验证单一 DataFrame 的数据合约。

        Args:
            df     : 待验证的行情 DataFrame
            symbol : 标的代码（日志/报告用）
            source : 数据来源标识（日志/报告用）
            period : K线周期（1m/5m/15m/30m/60m/1d/1w/1M）

        Returns:
            ContractValidationResult；pass_gate=False 表示硬门禁触发。
        """
        # ── 空数据快速失败 ────────────────────────────────────────────────────
        if df is None or df.empty:
            return ContractValidationResult(
                symbol=symbol,
                source=source,
                rows=0,
                pass_gate=False,
                violations=[
                    ContractViolation(
                        check="non_empty",
                        severity="hard",
                        detail="DataFrame 为空，无法完成合约验证",
                        value=0.0,
                    )
                ],
            )

        rows = len(df)
        violations: list[ContractViolation] = []

        # ── 1. 列发现（大小写不敏感 + 常见别名支持）────────────────────────
        col_map = self._detect_columns(df)

        # ── 2. 关键列 NaN 率（硬门禁）───────────────────────────────────────
        nan_ratios: dict[str, float] = {}
        for std_col in ("open", "high", "low", "close"):
            real_col = col_map.get(std_col)
            if real_col is None:
                nan_ratios[std_col] = 1.0
                violations.append(
                    ContractViolation(
                        check=f"column_exists_{std_col}",
                        severity="hard",
                        detail=f"关键列 '{std_col}' 缺失（候选列: {list(df.columns)[:10]}）",
                        value=1.0,
                    )
                )
            else:
                ratio = float(df[real_col].isna().sum()) / rows
                nan_ratios[std_col] = ratio
                if ratio > NAN_RATIO_HARD_MAX:
                    violations.append(
                        ContractViolation(
                            check=f"nan_{std_col}",
                            severity="hard",
                            detail=(
                                f"'{std_col}' NaN 率 {ratio:.2%}"
                                f" > 阈值 {NAN_RATIO_HARD_MAX:.0%}"
                                f" ({int(ratio * rows)}/{rows} 行)"
                            ),
                            value=ratio,
                        )
                    )

        # ── 3. OHLC 逻辑关系 + 正价格（硬门禁）─────────────────────────────
        ohlc_sanity_pct = 1.0
        positive_price_pct = 1.0

        o, h, low_col, c = (col_map.get(k) for k in ("open", "high", "low", "close"))
        if all(x is not None for x in (o, h, low_col, c)):
            try:
                subset = df[[o, h, low_col, c]].dropna()
                n = len(subset)
                if n > 0:
                    # 3a. OHLC 关系：H >= max(O,C), L <= min(O,C)
                    ohlc_ok = (
                        (subset[h] >= subset[[o, c]].max(axis=1))
                        & (subset[low_col] <= subset[[o, c]].min(axis=1))
                    )
                    ohlc_sanity_pct = float(ohlc_ok.sum()) / n
                    if ohlc_sanity_pct < OHLC_SANITY_MIN_PCT:
                        bad = n - int(ohlc_ok.sum())
                        violations.append(
                            ContractViolation(
                                check="ohlc_sanity",
                                severity="hard",
                                detail=(
                                    f"OHLC 关系违规 {bad} 行（{1 - ohlc_sanity_pct:.2%}）"
                                    f" > 允许上限 {1 - OHLC_SANITY_MIN_PCT:.0%}"
                                ),
                                value=ohlc_sanity_pct,
                            )
                        )

                    # 3b. 正价格：全部 OHLC > 0
                    pos_ok = (subset[[o, h, low_col, c]] > 0).all(axis=1)
                    positive_price_pct = float(pos_ok.sum()) / n
                    if positive_price_pct < POSITIVE_PRICE_MIN_PCT:
                        bad_p = n - int(pos_ok.sum())
                        violations.append(
                            ContractViolation(
                                check="positive_price",
                                severity="hard",
                                detail=(
                                    f"非正价格 {bad_p} 行（{1 - positive_price_pct:.2%}）"
                                    f" > 允许上限 {1 - POSITIVE_PRICE_MIN_PCT:.0%}"
                                ),
                                value=positive_price_pct,
                            )
                        )
            except Exception as exc:
                violations.append(
                    ContractViolation(
                        check="ohlc_compute",
                        severity="hard",
                        detail=f"OHLC 计算异常: {exc}",
                        value=0.0,
                    )
                )

        # ── 4. 价格速度（A 股涨跌停约束）────────────────────────────────────
        velocity_violation_count = 0
        velocity_violation_pct = 0.0
        velocity_limit = {
            "1m": 0.12,
            "5m": 0.12,
            "15m": 0.15,
            "30m": 0.18,
            "60m": 0.21,
            "1d": VELOCITY_HARD_LIMIT,
            "1w": 0.45,
            "1M": 0.80,
        }.get(period, VELOCITY_HARD_LIMIT)
        period_label = {"1d": "日", "1w": "周", "1M": "月"}.get(period, period)

        if c is not None:
            try:
                close_series = df[c].dropna()
                if len(close_series) > 1:
                    pct_chg = close_series.pct_change(fill_method=None).abs()
                    mask = pct_chg > velocity_limit
                    velocity_violation_count = int(mask.sum())
                    velocity_violation_pct = velocity_violation_count / len(close_series)

                    if velocity_violation_pct > VELOCITY_HARD_ROW_PCT:
                        violations.append(
                            ContractViolation(
                                check="price_velocity",
                                severity="hard",
                                detail=(
                                    f"{velocity_violation_count} 行{period_label}收益率"
                                    f" > {velocity_limit:.0%}（{period} 阈值），"
                                    f"占比 {velocity_violation_pct:.2%}"
                                    f" > 允许上限 {VELOCITY_HARD_ROW_PCT:.0%}"
                                ),
                                value=velocity_violation_pct,
                            )
                        )
                    elif velocity_violation_count > 0:
                        violations.append(
                            ContractViolation(
                                check="price_velocity_info",
                                severity="soft",
                                detail=(
                                    f"发现 {velocity_violation_count} 行{period_label}收益率"
                                    f" > {velocity_limit:.0%}（建议人工确认）"
                                ),
                                value=velocity_violation_pct,
                            )
                        )
            except Exception:
                pass  # 速度检查失败不阻断收益

        # ── 5. 成交量零值率（软告警）────────────────────────────────────────
        volume_zero_pct = 0.0
        v_col = col_map.get("volume")
        if v_col is not None:
            try:
                vol = df[v_col].dropna()
                if len(vol) > 0:
                    volume_zero_pct = float((vol == 0).sum()) / len(vol)
                    if volume_zero_pct > VOLUME_ZERO_SOFT_MAX:
                        violations.append(
                            ContractViolation(
                                check="volume_zero",
                                severity="soft",
                                detail=(
                                    f"成交量零值率 {volume_zero_pct:.2%}"
                                    f" > 软限 {VOLUME_ZERO_SOFT_MAX:.0%}"
                                ),
                                value=volume_zero_pct,
                            )
                        )
            except Exception:
                pass

        # ── 5. 非交易日行检查（日线/周线/月线周期，硬门禁）────────────────────
        # 仅对 1d/1w/1M 检查：分钟/小时线允许跨交易时段缓冲，无需过滤。
        if period in ("1d", "1w", "1M"):
            _DT_CANDIDATES = ("datetime", "date", "trade_date", "trading_date", "time")
            _cols_lower = {col.lower(): col for col in df.columns}
            dt_col = next(
                (_cols_lower[cand] for cand in _DT_CANDIDATES if cand in _cols_lower),
                None,
            )
            if dt_col is not None:
                try:
                    calendar = _get_trading_calendar()
                    date_series = pd.to_datetime(df[dt_col], errors="coerce").dt.date
                    non_td_mask = date_series.apply(
                        lambda d: d is not None and not pd.isna(d) and not calendar.is_trading_day(d)
                    )
                    non_td_count = int(non_td_mask.sum())
                    if non_td_count > 0:
                        sample = list(date_series[non_td_mask].head(3).astype(str))
                        violations.append(
                            ContractViolation(
                                check="non_trading_day",
                                severity="soft",
                                detail=(
                                    f"发现 {non_td_count} 行非交易日数据"
                                    f"（周期={period}，前3例: {sample}）"
                                ),
                                value=float(non_td_count),
                            )
                        )
                except Exception as exc:
                    logger.warning("非交易日检查失败，已跳过: %s", exc)

        # ── 门禁判决 ─────────────────────────────────────────────────────────
        hard_violations = [v for v in violations if v.severity == "hard"]
        pass_gate = len(hard_violations) == 0

        result = ContractValidationResult(
            symbol=symbol,
            source=source,
            rows=rows,
            ohlc_sanity_pct=ohlc_sanity_pct,
            positive_price_pct=positive_price_pct,
            nan_ratios=nan_ratios,
            velocity_violation_count=velocity_violation_count,
            velocity_violation_pct=velocity_violation_pct,
            volume_zero_pct=volume_zero_pct,
            pass_gate=pass_gate,
            violations=violations,
        )

        # ── 日志输出 ──────────────────────────────────────────────────────────
        if not pass_gate:
            logger.error(
                "DataContract HARD-FAIL [%s | %s | %d rows]: %s",
                symbol,
                source,
                rows,
                "; ".join(v.detail for v in hard_violations),
            )
        elif violations:
            soft_msgs = "; ".join(v.detail for v in violations)
            logger.warning(
                "DataContract SOFT-WARN [%s | %s | %d rows]: %s",
                symbol,
                source,
                rows,
                soft_msgs,
            )
        else:
            logger.debug(
                "DataContract OK [%s | %s | %d rows]",
                symbol,
                source,
                rows,
            )

        return result

    # ─────────────────────────────────── 辅助方法 ────────────────────────────

    def _detect_columns(self, df: pd.DataFrame) -> dict[str, Optional[str]]:
        """
        自动识别 DataFrame 中的标准列名（大小写不敏感 + 常见别名）。

        Returns:
            dict mapping standard name → actual column name (None if not found)
        """
        cols_lower: dict[str, str] = {c.lower(): c for c in df.columns}
        aliases: dict[str, list[str]] = {
            "open":   ["open_price", "openprice"],
            "high":   ["high_price", "highprice"],
            "low":    ["low_price", "lowprice"],
            "close":  ["close_price", "closeprice", "price"],
            "volume": ["vol", "成交量"],
            "amount": ["turnover", "成交额"],
        }
        result: dict[str, Optional[str]] = {}
        for std in ("open", "high", "low", "close", "volume", "amount"):
            found: Optional[str] = cols_lower.get(std)
            if found is None:
                for alias in aliases.get(std, []):
                    if alias in cols_lower:
                        found = cols_lower[alias]
                        break
            result[std] = found
        return result
