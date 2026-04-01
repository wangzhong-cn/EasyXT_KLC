from __future__ import annotations

from typing import Any

import pandas as pd

from data_manager.timestamp_contract import (
    TIMESTAMP_CONTRACT_VERSION,
    normalize_timestamp_frame,
)

CANONICAL_MINUTE_VERSION = "2026.04.01"


def normalize_canonical_1m(
    df: pd.DataFrame,
    *,
    auction_policy: str = "merged_open_auction",
    source_rule_kind: str = "auto",
    source_tz: str | None = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    working = normalize_timestamp_frame(
        df,
        period="1m",
        time_column="time" if "time" in df.columns else None,
        source_tz=source_tz,
    )
    if "time" not in working.columns:
        if "datetime" in working.columns:
            working["time"] = working["datetime"]
        elif isinstance(working.index, pd.DatetimeIndex):
            working["time"] = pd.DatetimeIndex(working.index)
        else:
            raise ValueError("canonical_1m 缺少可归一的时间列")
    working["time"] = pd.to_datetime(working["time"], errors="coerce")
    working = working[working["time"].notna()].copy()
    working = working.sort_values("time").reset_index(drop=True)
    working["_trade_date"] = working["time"].dt.date
    working["_clock"] = working["time"].dt.strftime("%H:%M")
    applied_rule_kind = str(source_rule_kind or "auto").strip() or "auto"
    merged_any = False
    if str(auction_policy or "").strip() == "merged_open_auction":
        working, merged_any = _merge_open_auction(working)
    if applied_rule_kind == "auto":
        applied_rule_kind = "normalized_from_split" if merged_any else "qmt_merged"
    working["auction_policy"] = str(auction_policy or "unknown").strip() or "unknown"
    working["source_rule_kind"] = applied_rule_kind
    working["canonical_minute_version"] = CANONICAL_MINUTE_VERSION
    working["timestamp_contract_version"] = TIMESTAMP_CONTRACT_VERSION
    return working.drop(columns=["_trade_date", "_clock"], errors="ignore")


def _merge_open_auction(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    merged = df.copy()
    drop_indexes: list[int] = []
    merged_any = False
    for trade_date, day_df in merged.groupby("_trade_date", sort=False):
        auction_rows = day_df[day_df["_clock"] == "09:25"]
        regular_rows = day_df[day_df["_clock"] == "09:30"]
        if auction_rows.empty or regular_rows.empty:
            continue
        auction = auction_rows.iloc[-1]
        regular_index = int(regular_rows.index[0])
        merged.loc[regular_index, "open"] = _coalesce_numeric(auction.get("open"), merged.loc[regular_index, "open"])
        merged.loc[regular_index, "high"] = _merge_high_low(
            auction.get("high"),
            merged.loc[regular_index, "high"],
            mode="high",
        )
        merged.loc[regular_index, "low"] = _merge_high_low(
            auction.get("low"),
            merged.loc[regular_index, "low"],
            mode="low",
        )
        merged.loc[regular_index, "volume"] = _coalesce_numeric(auction.get("volume"), 0.0) + _coalesce_numeric(
            merged.loc[regular_index, "volume"],
            0.0,
        )
        drop_indexes.extend(list(auction_rows.index))
        merged_any = True
    if drop_indexes:
        merged = merged.drop(index=drop_indexes).sort_values("time").reset_index(drop=True)
    return merged, merged_any


def _coalesce_numeric(primary: Any, fallback: Any) -> float:
    if primary is not None and not pd.isna(primary):
        return float(primary)
    if fallback is not None and not pd.isna(fallback):
        return float(fallback)
    return 0.0


def _merge_high_low(left: Any, right: Any, *, mode: str) -> float:
    lv = _coalesce_numeric(left, right)
    rv = _coalesce_numeric(right, left)
    if mode == "high":
        return max(lv, rv)
    return min(lv, rv)


__all__ = [
    "CANONICAL_MINUTE_VERSION",
    "normalize_canonical_1m",
]
