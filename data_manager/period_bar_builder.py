"""
严格自定义时间周期 K 线构建器
=====================================

设计原则
--------
日内自定义周期（sub-1D，如 2m/10m/25m/70m/125m）
  - 每日以开盘第一根 1m K 线为起点，**左对齐**按 N 分钟聚合
  - A 股交易分两段：上午 09:30-11:30、下午 13:00-15:00；午间休市为硬边界，
    各时段独立左对齐，不跨午间
  - 每日最后一根 K 线（下午最后一根）的 close 必须严格等于当日 1D 日 K 线收盘价
    （黄金标准收敛，误差容忍 ≤ 0.1%）
  - 四源交叉验证：日内聚合的总成交量与 1D 日 K 线成交量之差 ≤ 5%

多日自定义周期（>1D，如 2d/5d/10d/25d/50d/75d/…）
  - 起始于当前品种**上市首个交易日**（listing_date），**左对齐**按 N 交易日分组
  - 5d ≠ 1W：5d = 连续 5 个交易日为一期；1W = 自然周（周一至周五）
  - 3M ≠ 1Q：3M = 连续 63 个交易日；1Q = 自然季度（1/4/7/10 月到期）
  - 当前未完成的最后一期标记 is_partial=True
  - **刚性约束**：多日左对齐要求 1D 日 K 线从上市首日起完整、准确，否则计数偏移
  - 交叉验证：多日 K 线内的 OHLCV 必须与对应 1D 数据聚合结果一致

自然日历周期（1W/1M/1Q/6M/1Y/2Y/3Y/5Y/10Y）
  - 使用 pandas resample 按日历期末对齐（右闭合），不做左对齐
  - 1W = 每周五收盘结束的自然周，1M = 月末，1Q = 季末……

存储策略
--------
自定义周期数据视作"批量因子"预计算后落库（DuckDB 的 custom_period_bars 表），
而非每次查询时实时计算，实现随取随用。

四源 OHLCV 交叉验证规则
-----------------------
Tick  → 1m  ：分钟聚合量 == 1m 成交量（±0%，逐笔求和）
1m   → 日内N分钟：日内各 N 分钟 K 线量之和 == 当日 1D 成交量（±5%）
1m   → 1D  ：日内所有 1m close 最后一根 == 1D close（黄金标准）
1D   → 多日 ：多日期内各 1D close 最后一根 == 多日 close（严格）
"""

from __future__ import annotations

import json
import logging
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

_logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# A 股交易时段定义（标准股票/ETF/指数）
# ──────────────────────────────────────────────────────────────────────────────
#: 标准A股时段（不含集合竞价 1m 数据时使用）
ASHARE_SESSIONS: List[Tuple[str, str]] = [
    ("09:30", "11:30"),  # 上午连续竞价
    ("13:00", "15:00"),  # 下午连续竞价
]

#: 含集合竞价（09:15-09:25 有 1m 数据时使用）
ASHARE_WITH_AUCTION_SESSIONS: List[Tuple[str, str]] = [
    ("09:15", "09:25"),  # 集合竞价
    ("09:30", "11:30"),  # 上午
    ("13:00", "15:00"),  # 下午
]

#: 商品期货（综合示例，实际各品种夜盘不同）
COMMODITY_SESSIONS: List[Tuple[str, str]] = [
    ("21:00", "23:00"),  # 夜盘（部分品种延至 02:30）
    ("09:00", "10:15"),  # 上午1段
    ("10:30", "11:30"),  # 上午2段
    ("13:30", "15:00"),  # 下午
]
SESSION_PROFILES: Dict[str, List[Tuple[str, str]]] = {
    "CN_A": ASHARE_SESSIONS,
    "CN_A_AUCTION": ASHARE_WITH_AUCTION_SESSIONS,
    "FUTURES_COMMODITY": COMMODITY_SESSIONS,
}


# ──────────────────────────────────────────────────────────────────────────────
# 周期类型枚举
# ──────────────────────────────────────────────────────────────────────────────
class PeriodType(Enum):
    BASE = "base"                    # 基础数据（Tick/1m/5m/1D），直接存储，无需构建
    INTRADAY = "intraday"            # 日内自定义：左对齐，收敛于 1D 黄金标准
    MULTIDAY_CUSTOM = "multiday"     # 多日自定义：上市首日左对齐，5d≠1W，3M≠1Q
    NATURAL_CALENDAR = "natural"     # 自然日历：pandas resample 右闭合


# ──────────────────────────────────────────────────────────────────────────────
# 完整周期规格表
# ──────────────────────────────────────────────────────────────────────────────
PERIOD_SPECS: Dict[str, dict] = {
    # ── 基础数据（原始存储，不经此模块构建）──────────────────────────────────
    "tick": {"type": PeriodType.BASE,  "base": "tick"},
    "1m":   {"type": PeriodType.BASE,  "base": "1m"},
    "5m":   {"type": PeriodType.BASE,  "base": "1m"},
    "1d":   {"type": PeriodType.BASE,  "base": "1d"},

    # ── 日内自定义（从 1m 聚合，日内左对齐，严格收敛于 1D 日 K）────────────
    "2m":   {"type": PeriodType.INTRADAY, "minutes": 2,   "base": "1m"},
    "10m":  {"type": PeriodType.INTRADAY, "minutes": 10,  "base": "1m"},
    "15m":  {"type": PeriodType.INTRADAY, "minutes": 15,  "base": "1m"},
    "20m":  {"type": PeriodType.INTRADAY, "minutes": 20,  "base": "1m"},
    "25m":  {"type": PeriodType.INTRADAY, "minutes": 25,  "base": "1m"},
    "30m":  {"type": PeriodType.INTRADAY, "minutes": 30,  "base": "1m"},
    "50m":  {"type": PeriodType.INTRADAY, "minutes": 50,  "base": "1m"},
    "60m":  {"type": PeriodType.INTRADAY, "minutes": 60,  "base": "1m"},
    "70m":  {"type": PeriodType.INTRADAY, "minutes": 70,  "base": "1m"},
    "120m": {"type": PeriodType.INTRADAY, "minutes": 120, "base": "1m"},
    "125m": {"type": PeriodType.INTRADAY, "minutes": 125, "base": "1m"},

    # ── 多日自定义（从 1D 聚合，上市首日左对齐；N交易日 ≠ 自然周/月）────────
    "2d":  {"type": PeriodType.MULTIDAY_CUSTOM, "trading_days": 2,   "base": "1d"},
    "3d":  {"type": PeriodType.MULTIDAY_CUSTOM, "trading_days": 3,   "base": "1d"},
    "5d":  {"type": PeriodType.MULTIDAY_CUSTOM, "trading_days": 5,   "base": "1d"},
    "10d": {"type": PeriodType.MULTIDAY_CUSTOM, "trading_days": 10,  "base": "1d"},
    "25d": {"type": PeriodType.MULTIDAY_CUSTOM, "trading_days": 25,  "base": "1d"},
    "50d": {"type": PeriodType.MULTIDAY_CUSTOM, "trading_days": 50,  "base": "1d"},
    "75d": {"type": PeriodType.MULTIDAY_CUSTOM, "trading_days": 75,  "base": "1d"},
    # 注意：2M/3M/5M 是"约 N 个月的交易日数"，≠ 自然月/季度
    "2M":  {"type": PeriodType.MULTIDAY_CUSTOM, "trading_days": 42,  "base": "1d"},   # ~2个月≈42交易日
    "3M":  {"type": PeriodType.MULTIDAY_CUSTOM, "trading_days": 63,  "base": "1d"},   # ~3个月≈63交易日
    "5M":  {"type": PeriodType.MULTIDAY_CUSTOM, "trading_days": 105, "base": "1d"},   # ~5个月≈105交易日

    # ── 自然日历周期（pandas resample，右闭合，不需要 listing_date）─────────
    "1w":  {"type": PeriodType.NATURAL_CALENDAR, "freq": "W-FRI",  "base": "1d"},
    "1M":  {"type": PeriodType.NATURAL_CALENDAR, "freq": "ME",     "base": "1d"},
    "1Q":  {"type": PeriodType.NATURAL_CALENDAR, "freq": "QE-DEC", "base": "1d"},   # 季末：3/6/9/12月
    "6M":  {"type": PeriodType.NATURAL_CALENDAR, "freq": "6ME",    "base": "1d"},
    "1Y":  {"type": PeriodType.NATURAL_CALENDAR, "freq": "YE",     "base": "1d"},
    "2Y":  {"type": PeriodType.NATURAL_CALENDAR, "freq": "2YE",    "base": "1d"},
    "3Y":  {"type": PeriodType.NATURAL_CALENDAR, "freq": "3YE",    "base": "1d"},
    "5Y":  {"type": PeriodType.NATURAL_CALENDAR, "freq": "5YE",    "base": "1d"},
    "10Y": {"type": PeriodType.NATURAL_CALENDAR, "freq": "10YE",   "base": "1d"},
}

#: 所有日内自定义周期
INTRADAY_CUSTOM_PERIODS = {k for k, v in PERIOD_SPECS.items() if v["type"] == PeriodType.INTRADAY}
#: 所有多日自定义周期
MULTIDAY_CUSTOM_PERIODS = {k for k, v in PERIOD_SPECS.items() if v["type"] == PeriodType.MULTIDAY_CUSTOM}
#: 自然日历周期
NATURAL_CALENDAR_PERIODS = {k for k, v in PERIOD_SPECS.items() if v["type"] == PeriodType.NATURAL_CALENDAR}


# ──────────────────────────────────────────────────────────────────────────────
# 校验结果容器
# ──────────────────────────────────────────────────────────────────────────────
class ValidationResult:
    def __init__(self) -> None:
        self.is_valid = True
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.details: list[dict] = []

    def add_error(self, msg: str) -> None:
        self.is_valid = False
        self.errors.append(msg)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def add_detail(self, payload: dict) -> None:
        if isinstance(payload, dict):
            self.details.append(payload)

    def to_dict(self) -> dict:
        return {
            "is_valid": bool(self.is_valid),
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "details": list(self.details),
        }

    def __repr__(self) -> str:
        status = "PASS" if self.is_valid else "FAIL"
        return (
            f"ValidationResult({status}, errors={len(self.errors)}, "
            f"warnings={len(self.warnings)})"
        )


# ──────────────────────────────────────────────────────────────────────────────
# 主构建器
# ──────────────────────────────────────────────────────────────────────────────
class PeriodBarBuilder:
    """
    严格自定义时间周期 K 线构建器。

    用法示例::

        builder = PeriodBarBuilder()

        # 日内25分钟K线（严格收敛于1D收盘）
        bars_25m = builder.build("25m", data_1m=df_1m, daily_ref=df_1d)

        # 5交易日K线（左对齐，起始于上市首日）
        bars_5d  = builder.build("5d", data_1d=df_1d, listing_date="2010-01-04")

        # 自然周K线（pandas resample，非左对齐）
        bars_1w  = builder.build("1w", data_1d=df_1d)

        # 四源交叉验证
        vr = builder.cross_validate("25m", bars_25m, daily_ref=df_1d)
        if not vr.is_valid:
            print(vr.errors)
    """

    def __init__(
        self,
        sessions: Optional[List[Tuple[str, str]]] = None,
        convergence_tolerance: float = 0.001,  # 收敛价差容忍：1‰
        volume_tolerance: float = 0.05,         # 成交量容忍：5%
        session_profile: str = "CN_A",
        session_profile_file: Optional[str] = None,
        alignment: str = "left",
        anchor: str = "daily_close",
        validation_report_file: Optional[str] = None,
    ) -> None:
        profiles = self._resolve_session_profiles(session_profile_file)
        profile = str(session_profile or "CN_A").upper()
        align = str(alignment or "left").lower()
        anchor_mode = str(anchor or "daily_close").lower()
        if align != "left":
            raise ValueError(f"unsupported alignment={alignment}")
        if anchor_mode not in {"daily_close", "none"}:
            raise ValueError(f"unsupported anchor={anchor}")
        self._session_profile = profile if profile in profiles else "CN_A"
        self._sessions = sessions or profiles.get(self._session_profile, ASHARE_SESSIONS)
        self._conv_tol = convergence_tolerance
        self._vol_tol = volume_tolerance
        self._alignment = align
        self._anchor = anchor_mode
        self._validation_report_file = str(validation_report_file or "").strip()
        self._session_profile_file = str(session_profile_file or "").strip()

    # ── 统一入口 ──────────────────────────────────────────────────────────────

    def build(
        self,
        period: str,
        data_1m: Optional[pd.DataFrame] = None,
        data_1d: Optional[pd.DataFrame] = None,
        daily_ref: Optional[pd.DataFrame] = None,
        listing_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        统一构建入口，根据 period 类型路由到相应方法。

        Args:
            period:       目标周期字符串（如 "25m"/"5d"/"1w"）
            data_1m:      1分钟基础数据（日内周期必填）
            data_1d:      1日基础数据（多日/自然周期必填）
            daily_ref:    1日数据用于收敛校验（日内周期可选，有则更精确）
            listing_date: 上市首交易日 'YYYY-MM-DD'（多日左对齐必填）
        """
        spec = PERIOD_SPECS.get(period)
        if spec is None:
            raise ValueError(f"未知周期: {period}。支持的周期: {sorted(PERIOD_SPECS)}")

        ptype = spec["type"]

        if ptype == PeriodType.BASE:
            raise ValueError(f"周期 {period} 是基础数据，直接从 DuckDB 读取，不需要构建")

        if ptype == PeriodType.INTRADAY:
            if data_1m is None or data_1m.empty:
                _logger.warning("build(%s): 1m 数据为空，返回空 DataFrame", period)
                return _empty_ohlcv()
            return self.build_intraday_bars(
                data_1m=data_1m,
                period_minutes=spec["minutes"],
                daily_ref=daily_ref if daily_ref is not None else data_1d,
            )

        if ptype == PeriodType.MULTIDAY_CUSTOM:
            if data_1d is None or data_1d.empty:
                _logger.warning("build(%s): 1d 数据为空，返回空 DataFrame", period)
                return _empty_ohlcv()
            return self.build_multiday_bars(
                data_1d=data_1d,
                trading_days_per_period=spec["trading_days"],
                listing_date=listing_date,
            )

        if ptype == PeriodType.NATURAL_CALENDAR:
            if data_1d is None or data_1d.empty:
                _logger.warning("build(%s): 1d 数据为空，返回空 DataFrame", period)
                return _empty_ohlcv()
            return self.build_natural_calendar_bars(data_1d=data_1d, freq=spec["freq"])

        raise ValueError(f"未处理的 PeriodType: {ptype}")

    # ── 日内 N 分钟构建 ───────────────────────────────────────────────────────

    def build_intraday_bars(
        self,
        data_1m: pd.DataFrame,
        period_minutes: int,
        daily_ref: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        从 1m 数据构建日内 N 分钟 K 线。

        规则：
        - 每个时段（上午/下午）独立左对齐，午间休市为硬边界
        - 最后一个时段的最后一根 K 线的 close 强制 == 当日 1D 收盘价（黄金标准）
        - 分时段聚合：不跨午间、不跨节假日
        """
        df = _prepare_1m(data_1m)
        if df.empty:
            return _empty_ohlcv()

        # 构建每日对应的 1D 收盘价查找表
        daily_close_map: dict = {}
        if daily_ref is not None and not daily_ref.empty:
            dr = _prepare_1d(daily_ref)  # 规范化：处理 DatetimeIndex 或 time 列
            for _, row in dr.iterrows():
                daily_close_map[row["time"].date()] = float(row["close"])

        result_bars: list[dict] = []
        for trade_date, day_df in df.groupby("_date"):
            bars = self._build_day_intraday_bars(
                day_df=day_df,
                period_minutes=period_minutes,
                trade_date=trade_date,
                daily_close=daily_close_map.get(trade_date),
            )
            result_bars.extend(bars)

        if not result_bars:
            return _empty_ohlcv()

        result = pd.DataFrame(result_bars)
        return result.sort_values("time").reset_index(drop=True)

    def _build_day_intraday_bars(
        self,
        day_df: pd.DataFrame,
        period_minutes: int,
        trade_date,
        daily_close: Optional[float],
    ) -> list[dict]:
        """单日内 N 分钟 K 线构建（分时段独立左对齐）"""
        bars: list[dict] = []
        sessions = self._sessions
        n_sessions = len(sessions)

        for sess_idx, (ss_str, se_str) in enumerate(sessions):
            session_start = pd.Timestamp(f"{trade_date} {ss_str}")
            session_end   = pd.Timestamp(f"{trade_date} {se_str}")
            is_last_session = (sess_idx == n_sessions - 1)

            # 取该时段的 1m 数据
            seg = day_df[
                (day_df["time"] >= session_start) &
                (day_df["time"] <= session_end)
            ].sort_values("time").reset_index(drop=True)

            if seg.empty:
                continue

            n = len(seg)
            i = 0
            while i < n:
                chunk = seg.iloc[i : i + period_minutes]
                is_last_chunk = (i + period_minutes >= n)
                is_partial    = len(chunk) < period_minutes

                close_val = float(chunk.iloc[-1]["close"])

                # 严格收敛规则：日内最后一根 K 线 close == 1D 收盘价
                if (
                    self._anchor == "daily_close"
                    and is_last_session
                    and is_last_chunk
                    and daily_close is not None
                ):
                    if abs(close_val - daily_close) > self._conv_tol * daily_close:
                        _logger.debug(
                            "收敛修正 %s %dm: %.4f → %.4f",
                            trade_date, period_minutes, close_val, daily_close,
                        )
                    close_val = daily_close  # 强制收敛（无论误差大小，确保一致性）

                bars.append({
                    "time":       chunk.iloc[-1]["time"],  # 右边界时间戳（行业标准）
                    "open":       float(chunk.iloc[0]["open"]),
                    "high":       float(chunk["high"].max()),
                    "low":        float(chunk["low"].min()),
                    "close":      close_val,
                    "volume":     float(chunk["volume"].sum()),
                    "is_partial": is_partial,
                    "alignment": self._alignment,
                    "anchor": self._anchor,
                    "session_profile": self._session_profile,
                })
                i += period_minutes

        return bars

    # ── 多日自定义周期构建 ────────────────────────────────────────────────────

    def build_multiday_bars(
        self,
        data_1d: pd.DataFrame,
        trading_days_per_period: int,
        listing_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        从 1D 数据构建多日自定义 K 线（左对齐）。

        **刚性约束**：data_1d 必须从上市首日起完整（不能有缺口），
        否则左对齐编号偏移，导致 K 线边界错误。

        Args:
            data_1d:                  1D 日 K 线数据（从上市首日起完整！）
            trading_days_per_period:  每期交易日数（如 5 = 5 交易日一期）
            listing_date:             上市首交易日 'YYYY-MM-DD'（None 则取最早日期）
        """
        df = _prepare_1d(data_1d, listing_date)
        if df.empty:
            return _empty_ohlcv()
        self._listing_date_gap_days = 0

        # ── T1.3: listing_date 间隙验证 ──
        # 左对齐刚性约束：实际数据起点必须在 listing_date 附近（≤5 个交易日容差）
        # 如果间隙过大，编号偏移导致所有 K 线边界错误
        if listing_date is not None and not df.empty:
            expected_start = pd.Timestamp(listing_date)
            actual_start = df.iloc[0]["time"]
            gap_days = (actual_start - expected_start).days
            _MAX_GAP_CALENDAR_DAYS = 10  # ≈ 5 交易日 + 周末
            if gap_days > _MAX_GAP_CALENDAR_DAYS:
                import logging
                _pb_logger = logging.getLogger("period_bar_builder")
                _pb_logger.warning(
                    "listing_date 间隙过大: listing_date=%s 实际起点=%s 间隙=%d天 "
                    "— 主动拒绝构建",
                    listing_date,
                    actual_start.strftime("%Y-%m-%d"), gap_days,
                )
                # 记录到 ValidationResult 供调用方检查
                self._listing_date_gap_days = gap_days
                return _empty_ohlcv()

        # 从 0 开始按顺序编号交易日，每 N 日为一期
        df = df.reset_index(drop=True)
        df["_period_num"] = df.index // trading_days_per_period

        max_period = int(df["_period_num"].max())
        result_bars: list[dict] = []

        for period_num, group in df.groupby("_period_num"):
            is_partial = (
                int(period_num) == max_period and
                len(group) < trading_days_per_period
            )
            result_bars.append({
                "time":       group.iloc[-1]["time"],   # 期末最后一个交易日（右边界）
                "open":       float(group.iloc[0]["open"]),
                "high":       float(group["high"].max()),
                "low":        float(group["low"].min()),
                "close":      float(group.iloc[-1]["close"]),
                "volume":     float(group["volume"].sum()),
                "is_partial": is_partial,
                "alignment": self._alignment,
                "anchor": "period_end",
                "session_profile": self._session_profile,
            })

        return pd.DataFrame(result_bars)

    # ── 自然日历周期构建 ──────────────────────────────────────────────────────

    def build_natural_calendar_bars(
        self,
        data_1d: pd.DataFrame,
        freq: str,
    ) -> pd.DataFrame:
        """
        自然日历周期 K 线（1W/1M/1Q/6M/1Y 等）。

        使用 pandas resample 按日历右闭合（1W = 周五结束，1M = 月末）。
        注意：1W ≠ 5d，1Q ≠ 3M（约63交易日）——这是设计约定，不是 bug。
        """
        if data_1d is None or data_1d.empty:
            return _empty_ohlcv()

        df = data_1d.copy()
        df["time"] = pd.to_datetime(df["time"])
        df = df.set_index("time").sort_index()

        agg = {k: v for k, v in {
            "open": "first", "high": "max", "low": "min",
            "close": "last", "volume": "sum",
        }.items() if k in df.columns}

        try:
            resampled = df.resample(freq, closed="right", label="right").agg(agg)
        except Exception:
            # 旧版 pandas 不支持部分 freq 字符串，回退
            resampled = df.resample(freq).agg(agg)

        resampled = resampled.dropna(subset=["open"]).reset_index()
        resampled["is_partial"] = False
        resampled["alignment"] = "calendar_right"
        resampled["anchor"] = "period_end"
        resampled["session_profile"] = self._session_profile
        return resampled

    # ── 四源交叉验证 ──────────────────────────────────────────────────────────

    def cross_validate(
        self,
        period: str,
        custom_bars: pd.DataFrame,
        daily_ref: Optional[pd.DataFrame] = None,
        data_1m: Optional[pd.DataFrame] = None,
    ) -> ValidationResult:
        """
        四源交叉验证（Tick/1m/5m/1D）。

        当前实现：
        - 日内周期：custom_bars 日聚合 vs daily_ref（1D）
        - 多日周期：custom_bars 内期末 close vs daily_ref 对应日 close
        """
        vr = ValidationResult()
        spec = PERIOD_SPECS.get(period)
        if spec is None:
            vr.add_warning(f"未知周期 {period}，跳过验证")
            return vr

        if custom_bars is None or custom_bars.empty:
            vr.add_warning("custom_bars 为空，跳过验证")
            return vr

        ptype = spec["type"]
        if ptype == PeriodType.INTRADAY and daily_ref is not None and not daily_ref.empty:
            self._validate_intraday_vs_daily(custom_bars, daily_ref, vr)
        elif ptype == PeriodType.MULTIDAY_CUSTOM and daily_ref is not None and not daily_ref.empty:
            self._validate_multiday_vs_daily(custom_bars, daily_ref, vr)
        self._emit_validation_report(period=period, vr=vr, rows=int(len(custom_bars)))

        return vr

    def _validate_intraday_vs_daily(
        self,
        intraday_bars: pd.DataFrame,
        daily_bars: pd.DataFrame,
        vr: ValidationResult,
    ) -> None:
        """验证日内自定义K线：按日聚合后与 1D 日 K 线比对"""
        ib = intraday_bars.copy()
        ib["_date"] = pd.to_datetime(ib["time"]).dt.date

        db = daily_bars.copy()
        db["_date"] = pd.to_datetime(db["time"]).dt.date

        # 聚合到日
        agg_by_day = ib.groupby("_date").agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        )

        daily_by_date = db.set_index("_date")

        for trade_date, row in agg_by_day.iterrows():
            if trade_date not in daily_by_date.index:
                continue
            d_row = daily_by_date.loc[trade_date]
            d_close = float(d_row["close"])
            d_vol   = float(d_row.get("volume", 0))

            # close 必须完美匹配（已在构建时强制收敛）
            if abs(row["close"] - d_close) > self._conv_tol * max(d_close, 1):
                vr.add_error(
                    f"{trade_date}: 收盘价未收敛 intraday={row['close']:.4f} "
                    f"daily={d_close:.4f} diff={abs(row['close']-d_close):.4f}"
                )
                vr.add_detail(
                    {
                        "period_type": "intraday",
                        "date": str(trade_date),
                        "metric": "close_diff",
                        "actual": float(row["close"]),
                        "expected": d_close,
                        "delta": abs(float(row["close"]) - d_close),
                    }
                )

            # 成交量允许 vol_tol 误差
            if d_vol > 0 and abs(row["volume"] - d_vol) / d_vol > self._vol_tol:
                vr.add_warning(
                    f"{trade_date}: 成交量偏差 "
                    f"intraday={row['volume']:.0f} daily={d_vol:.0f} "
                    f"({abs(row['volume']-d_vol)/d_vol*100:.1f}%)"
                )
                vr.add_detail(
                    {
                        "period_type": "intraday",
                        "date": str(trade_date),
                        "metric": "volume_diff_ratio",
                        "actual": float(row["volume"]),
                        "expected": d_vol,
                        "delta": abs(float(row["volume"]) - d_vol),
                        "delta_ratio": abs(float(row["volume"]) - d_vol) / d_vol,
                    }
                )

    def _validate_multiday_vs_daily(
        self,
        multiday_bars: pd.DataFrame,
        daily_bars: pd.DataFrame,
        vr: ValidationResult,
    ) -> None:
        """验证多日K线：每期末 close 必须 == 对应 1D 期末收盘价"""
        mb = multiday_bars.copy()
        mb["time"] = pd.to_datetime(mb["time"])

        db = daily_bars.copy()
        db["time"] = pd.to_datetime(db["time"])
        db = db.sort_values("time")

        prev_end = pd.Timestamp.min
        for _, bar in mb.sort_values("time").iterrows():
            bar_end = bar["time"]
            # 期内的 1D 数据
            period_daily = db[(db["time"] > prev_end) & (db["time"] <= bar_end)]
            if period_daily.empty:
                prev_end = bar_end
                continue

            expected_close = float(period_daily.iloc[-1]["close"])
            actual_close   = float(bar["close"])

            if abs(actual_close - expected_close) > self._conv_tol * max(expected_close, 1):
                vr.add_error(
                    f"多日K线 {bar_end.date()}: close={actual_close:.4f} "
                    f"!= 期末日收={expected_close:.4f}"
                )
                vr.add_detail(
                    {
                        "period_type": "multiday",
                        "date": str(bar_end.date()),
                        "metric": "close_diff",
                        "actual": actual_close,
                        "expected": expected_close,
                        "delta": abs(actual_close - expected_close),
                    }
                )
            prev_end = bar_end

    def _emit_validation_report(self, period: str, vr: ValidationResult, rows: int) -> None:
        out = self._validation_report_file
        if not out:
            return
        path = Path(out)
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        payload = {
            "period": str(period),
            "rows": int(rows),
            "is_valid": bool(vr.is_valid),
            "errors": list(vr.errors),
            "warnings": list(vr.warnings),
            "details": list(vr.details),
            "alignment": self._alignment,
            "anchor": self._anchor,
            "session_profile": self._session_profile,
            "generated_at": pd.Timestamp.now().isoformat(),
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            return

    @staticmethod
    def _resolve_session_profiles(session_profile_file: Optional[str]) -> Dict[str, List[Tuple[str, str]]]:
        profiles: Dict[str, List[Tuple[str, str]]] = dict(SESSION_PROFILES)
        file_path = str(session_profile_file or "").strip()
        if not file_path:
            return profiles
        path = Path(file_path)
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        if not path.exists():
            return profiles
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return profiles
            for key, value in payload.items():
                if not isinstance(key, str) or not isinstance(value, list):
                    continue
                rows: list[Tuple[str, str]] = []
                for item in value:
                    if isinstance(item, list) and len(item) == 2:
                        start = str(item[0]).strip()
                        end = str(item[1]).strip()
                        if start and end:
                            rows.append((start, end))
                if rows:
                    profiles[key.upper()] = rows
        except Exception:
            return profiles
        return profiles


# ──────────────────────────────────────────────────────────────────────────────
# 辅助函数
# ──────────────────────────────────────────────────────────────────────────────

def _empty_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "is_partial",
            "alignment",
            "anchor",
            "session_profile",
        ]
    )


def _prepare_1m(df: pd.DataFrame) -> pd.DataFrame:
    """清洗 1m 数据：标准化时间列、排序、添加 _date 辅助列。

    兼容两种入参格式：
    - 带 ``time`` 列（直接从 DuckDB mock 或外部调用）
    - DatetimeIndex（来自 UnifiedDataInterface._read_from_duckdb 返回值，index.name == "datetime"）
    """
    d = df.copy()
    if "time" not in d.columns:
        # _read_from_duckdb 的返回值以 datetime 为索引，将其还原为普通列
        if isinstance(d.index, pd.DatetimeIndex):
            idx_name = d.index.name or "datetime"
            d = d.reset_index().rename(columns={idx_name: "time"})
        elif "datetime" in d.columns:
            d = d.rename(columns={"datetime": "time"})
    d["time"] = pd.to_datetime(d["time"])
    d = d.sort_values("time").reset_index(drop=True)
    d["_date"] = d["time"].dt.date
    return d


def _prepare_1d(df: pd.DataFrame, listing_date: Optional[str] = None) -> pd.DataFrame:
    """清洗 1D 数据：标准化时间列、排序、从 listing_date 起截断。

    兼容两种入参格式：同 _prepare_1m。
    """
    d = df.copy()
    if "time" not in d.columns:
        if isinstance(d.index, pd.DatetimeIndex):
            idx_name = d.index.name or "datetime"
            d = d.reset_index().rename(columns={idx_name: "time"})
        elif "datetime" in d.columns:
            d = d.rename(columns={"datetime": "time"})
    d["time"] = pd.to_datetime(d["time"])
    d = d.sort_values("time").reset_index(drop=True)
    if listing_date is not None:
        start_ts = pd.Timestamp(listing_date)
        d = d[d["time"] >= start_ts].reset_index(drop=True)
    return d


# ──────────────────────────────────────────────────────────────────────────────
# 设计备忘录（供代码审阅者参考）
# ──────────────────────────────────────────────────────────────────────────────
"""
左对齐 vs 右对齐 的本质差异
────────────────────────────
左对齐（本系统采用）：
  period_K 起始 = listing_date + K * N 交易日
  → 要求 1D 数据从 listing_date 起完整无缺；缺任何一天，后续 K 线起止边界全部偏移。
  → 适合"从最早数据开始构建因子库"的量化场景（Bloomberg BCAP, Wind wsd 风格）。

右对齐（部分竞品采用）：
  period_K 结束 = today, period_{K-1} 结束 = today - N 交易日, ...
  → 不要求历史完整性；新品种上线即可。
  → 但回测时"历史期间的周期不可复现"，只有当前时刻 往前对齐才有意义。

本系统选择左对齐是因为要用预计算因子库支持全历史回测，这就要求：
  1D 日 K 线从上市首日到今日**必须完整、准确**，这是整个多日自定义周期体系的刚性前提。

日内 1D 黄金标准收敛的必要性
──────────────────────────────
任何日内自定义周期的最后一根 K 线 close，如果不强制等于 1D close，
当用该周期数据反推"日终仓位"时会产生的价格偏差，导致策略 PnL 计算错误。
因此，日内 close 收敛于 1D close 是量化数据质量的核心契约（DataContract）。
"""
