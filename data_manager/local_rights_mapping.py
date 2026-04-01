"""
局部除权因子映射引擎 (LocalRightsMappingEngine)

核心原则（不可妥协）：
  - 禁止全局前/后复权：前复权将未来除权信息反向注入历史价格，破坏时间因果性。
  - 局部正向映射：对分析窗口内每根K线，只累积"截至该K线时间点已发生"的除权
    事件因子，使跨除权事件的价格可直接比较，同时保持拓扑不变性。

数学定义：
  rights_factor(T) = ∏ { factor_k | ex_date_k ≤ T, ex_date_k ≥ window_start }
  close_mapped(T)  = close_raw(T) × rights_factor(T)
  同理适用于 open/high/low。

因子定义（A 股标准）：
  除权因子 factor = prev_close / 理论除权价
    理论除权价 = (prev_close × 10 − cash_per_10_shares) / (10 + bonus_ratio)
  当无送转股时（bonus_ratio=0）：factor = prev_close / (prev_close − cash_per_share)
  当无现金分红时（cash=0）    ：factor = (10 + bonus_ratio) / 10
  factor 始终 ≥ 1.0（除权后价格被向上复原至可比水平）。

用法示例：
    from data_manager.local_rights_mapping import LocalRightsMappingEngine

    engine = LocalRightsMappingEngine()

    # 方案A：直接传入预计算因子列表
    events = [(1704067200000, 1.012), (1706745600000, 1.025)]
    result_df = engine.map(bars_df, events)

    # 方案B：从分红数据自动计算因子
    events = engine.events_from_dividends(dividends_df)
    result_df = engine.map(bars_df, events)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pandas as pd

log = logging.getLogger(__name__)


class LocalRightsMappingEngine:
    """局部除权因子映射引擎 — 体系数据层的唯一合法价格映射工具。

    所有结构识别（StructureEngine）、信号生成（SignalGenerator）、
    回撤计算（DrawdownTracker）均必须使用 ``close_mapped`` 而非 ``close_raw``。
    下单执行时再反向换算为 ``close_raw``。
    """

    # ── 公开 API ──────────────────────────────────────────────────────────────

    def map(
        self,
        bars_df: "pd.DataFrame",
        ex_rights_events: list[tuple[int, float]],
        window_start_ms: int | None = None,
    ) -> "pd.DataFrame":
        """对 K 线 DataFrame 执行局部除权因子映射。

        参数：
            bars_df:
                必须包含列 ``trade_date``（毫秒 UTC 整数，升序排列）以及
                ``close_raw``（不复权收盘价）。若同时含 ``open_raw`` /
                ``high_raw`` / ``low_raw``，则一并映射。
            ex_rights_events:
                预计算好的除权事件列表，每项为 ``(event_ts_ms, factor)``。
                ``factor`` 必须 ≥ 1.0（用于将除权后价格向上复原）。
                时间戳为除权生效第一个交易日的毫秒时间戳。
            window_start_ms:
                分析窗口起始时间（毫秒）。仅窗口起始时间之后的除权事件才
                被纳入累积因子。``None`` 时使用 ``bars_df.trade_date.min()``。

        返回：
            在 ``bars_df`` 基础上追加列：
            ``rights_factor``, ``close_mapped``，以及（若有原始列）
            ``open_mapped``, ``high_mapped``, ``low_mapped``。
            不修改原始 DataFrame。

        异常：
            ValueError: ``bars_df`` 缺少必要列，或包含空数据。
        """
        import pandas as pd

        if bars_df is None or bars_df.empty:
            raise ValueError("bars_df 不能为空")
        if "trade_date" not in bars_df.columns:
            raise ValueError("bars_df 必须包含 trade_date 列（毫秒 UTC 整数）")
        if "close_raw" not in bars_df.columns:
            raise ValueError("bars_df 必须包含 close_raw 列")

        if window_start_ms is None:
            window_start_ms = int(bars_df["trade_date"].min())

        # 筛选窗口内的事件并升序排列（仅保留 ≥ window_start 的事件）
        filtered = sorted(
            [(ts, f) for ts, f in ex_rights_events if ts >= window_start_ms],
            key=lambda x: x[0],
        )

        # 计算逐 K 线的累积因子（O(n) 双指针扫描）
        trade_dates: np.ndarray = bars_df["trade_date"].to_numpy(dtype=np.int64)
        rights_factors: np.ndarray = np.ones(len(trade_dates), dtype=np.float64)

        running = 1.0
        event_idx = 0
        for i, ts in enumerate(trade_dates):
            # 消费所有事件时间戳 ≤ 当前 K 线时间戳
            while event_idx < len(filtered) and filtered[event_idx][0] <= ts:
                factor = filtered[event_idx][1]
                if factor < 1.0:
                    log.warning(
                        "除权因子 %.6f < 1.0（事件: %d），已自动修正为 1.0，"
                        "请检查数据源是否混入了前复权因子",
                        factor, filtered[event_idx][0],
                    )
                    factor = 1.0
                running *= factor
                event_idx += 1
            rights_factors[i] = running

        result = bars_df.copy()
        result["rights_factor"] = rights_factors
        result["close_mapped"] = result["close_raw"].to_numpy(dtype=np.float64) * rights_factors

        for col in ("open", "high", "low"):
            raw_col = f"{col}_raw"
            if raw_col in bars_df.columns:
                result[f"{col}_mapped"] = (
                    result[raw_col].to_numpy(dtype=np.float64) * rights_factors
                )

        log.debug(
            "LocalRightsMappingEngine.map: %d 行，%d 个除权事件生效，"
            "最终累积因子 %.6f",
            len(result), event_idx, running,
        )
        return result

    @staticmethod
    def events_from_dividends(
        dividends_df: "pd.DataFrame",
    ) -> list[tuple[int, float]]:
        """从分红数据 DataFrame 计算除权事件列表。

        参数：
            dividends_df:
                必须包含以下列（缺失列按 0 处理）：
                - ``ex_date_ms``     : 除权生效日毫秒时间戳（整数）
                - ``prev_close``     : 除权前最后一个交易日收盘价（不复权）
                - ``cash_per_share`` : 每股现金分红（元）— 可选，默认 0
                - ``bonus_per_10``   : 每10股送转合计（股数）— 可选，默认 0

        返回：
            ``[(ex_date_ms, factor), ...]`` 列表，按 ``ex_date_ms`` 升序排列。
            factor = prev_close × (10 + bonus) / (10 × (prev_close − cash))，
            factor ≥ 1.0。不满足条件的事件（如 prev_close ≤ 0）被静默忽略。

        用法：
            分红数据可来自 QMT ``xtdata.get_divid_factors()`` 经格式化后传入：
              - 将 ex_date（str 或 date）转毫秒时间戳存入 ex_date_ms
              - 将分红/送转信息填入对应列
        """
        if dividends_df is None or dividends_df.empty:
            return []

        events: list[tuple[int, float]] = []
        for _, row in dividends_df.iterrows():
            try:
                ex_date_ms = int(row["ex_date_ms"])
                prev_close = float(row["prev_close"])
                if prev_close <= 0:
                    continue
                cash = float(row.get("cash_per_share") or 0)
                bonus = float(row.get("bonus_per_10") or 0)

                # 理论除权价 = prev_close × 10 / (10 + bonus) − cash
                # 等价形式：(prev_close × 10 − cash × 10) / (10 + bonus)
                denominator = 10.0 * (prev_close - cash)
                if denominator <= 0:
                    continue
                numerator = prev_close * (10.0 + bonus)
                factor = numerator / denominator  # ≥ 1.0

                if factor < 1.0:
                    log.warning(
                        "events_from_dividends: 计算得 factor=%.6f < 1.0 "
                        "(prev_close=%.4f, cash=%.4f, bonus=%.2f)，已跳过",
                        factor, prev_close, cash, bonus,
                    )
                    continue
                events.append((ex_date_ms, factor))
            except Exception:
                log.debug("events_from_dividends: 跳过异常行 %s", dict(row))

        events.sort(key=lambda x: x[0])
        return events

    @staticmethod
    def reverse_mapped_to_raw(
        mapped_price: float,
        entry_ts: int,
        ex_rights_events: list[tuple[int, float]],
        window_start_ms: int = 0,
    ) -> float:
        """将 mapped 价格反向换算为 raw 价格（下单执行时使用）。

        参数：
            mapped_price:  结构分析层使用的映射后价格。
            entry_ts:      下单时刻的毫秒时间戳。
            ex_rights_events: 同 ``map()`` 的 events 参数。
            window_start_ms: 同 ``map()`` 的 window_start_ms 参数。

        返回：
            raw_price = mapped_price / rights_factor(entry_ts)
        """
        running = 1.0
        for ts, factor in sorted(ex_rights_events, key=lambda x: x[0]):
            if ts < window_start_ms:
                continue
            if ts > entry_ts:
                break
            running *= max(factor, 1.0)
        if running <= 0:
            return mapped_price
        return mapped_price / running
