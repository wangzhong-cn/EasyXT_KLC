from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from data_manager.timestamp_utils import dat_s_to_beijing, qmt_ms_to_beijing

TIMESTAMP_CONTRACT_VERSION = "2026.04.01"
NORMALIZED_TIMEZONE = "Asia/Shanghai"


def normalize_timestamp_scalar(
    value: Any,
    *,
    source_time_kind: str | None = None,
    source_tz: str | None = None,
) -> pd.Timestamp:
    if value is None or value is pd.NaT:
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is not None:
            return value.tz_convert(NORMALIZED_TIMEZONE).tz_localize(None)
        return value
    inferred_kind = str(source_time_kind or detect_source_time_kind(value)).strip()
    if inferred_kind == "epoch_ms":
        return qmt_ms_to_beijing(pd.Series([value])).iloc[0]
    if inferred_kind == "epoch_s":
        return pd.Timestamp(dat_s_to_beijing([value])[0])
    parsed = pd.to_datetime(value, errors="coerce")
    if parsed is pd.NaT:
        return pd.NaT
    if getattr(parsed, "tzinfo", None) is not None:
        return parsed.tz_convert(NORMALIZED_TIMEZONE).tz_localize(None)
    if str(source_tz or "").strip().upper() == "UTC":
        return parsed.tz_localize("UTC").tz_convert(NORMALIZED_TIMEZONE).tz_localize(None)
    return parsed


def normalize_timestamp_series(
    series: pd.Series,
    *,
    source_time_kind: str | None = None,
    source_tz: str | None = None,
) -> pd.Series:
    inferred_kind = str(source_time_kind or detect_source_time_kind(series)).strip()
    if inferred_kind == "epoch_ms":
        return qmt_ms_to_beijing(series)
    if inferred_kind == "epoch_s":
        return pd.Series(dat_s_to_beijing(series), index=series.index)
    return series.apply(
        lambda value: normalize_timestamp_scalar(
            value,
            source_time_kind=inferred_kind or None,
            source_tz=source_tz,
        )
    )


def normalize_timestamp_frame(
    df: pd.DataFrame,
    *,
    period: str | None = None,
    time_column: str | None = None,
    source_time_kind: str | None = None,
    source_tz: str | None = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    out = df.copy()
    resolved_column = time_column or _resolve_time_column(out, period=period)
    detected_kind = str(source_time_kind or "unknown").strip() or "unknown"
    if resolved_column == "__index__":
        if detected_kind == "unknown":
            detected_kind = detect_source_time_kind(pd.Series(out.index, index=out.index))
        normalized = normalize_timestamp_series(
            pd.Series(out.index, index=out.index),
            source_time_kind=detected_kind,
            source_tz=source_tz,
        )
        out.index = pd.DatetimeIndex(normalized)
    elif resolved_column:
        if detected_kind == "unknown":
            detected_kind = detect_source_time_kind(out[resolved_column])
        normalized = normalize_timestamp_series(
            out[resolved_column],
            source_time_kind=detected_kind,
            source_tz=source_tz,
        )
        out[resolved_column] = normalized
    else:
        return out
    out["source_time_kind"] = detected_kind
    out["source_tz"] = str(source_tz or "unknown").strip() or "unknown"
    out["normalized_tz"] = NORMALIZED_TIMEZONE
    out["timestamp_contract_version"] = TIMESTAMP_CONTRACT_VERSION
    out["normalized_at"] = pd.Timestamp.now().isoformat(timespec="seconds")
    return out


def detect_source_time_kind(value: Any) -> str:
    sample = value
    if isinstance(value, pd.Series):
        non_null = value.dropna()
        if non_null.empty:
            return "unknown"
        sample = non_null.iloc[0]
    if isinstance(sample, pd.Timestamp):
        return "tz_aware" if sample.tzinfo is not None else "local_naive"
    if isinstance(sample, str):
        text = sample.strip()
        if not text:
            return "unknown"
        if text.isdigit():
            return detect_source_time_kind(int(text))
        try:
            parsed = pd.to_datetime(text, errors="coerce")
        except Exception:
            return "local_string"
        if parsed is pd.NaT:
            return "local_string"
        return "tz_aware" if getattr(parsed, "tzinfo", None) is not None else "local_string"
    if isinstance(sample, (int, float, np.integer, np.floating)) and not pd.isna(sample):
        ivalue = int(sample)
        digits = len(str(abs(ivalue)))
        if digits >= 13:
            return "epoch_ms"
        if digits >= 10:
            return "epoch_s"
    return "unknown"


def _resolve_time_column(df: pd.DataFrame, *, period: str | None = None) -> str | None:
    candidates = ["time", "datetime", "date"]
    if str(period or "").strip() == "1d":
        candidates = ["date", "time", "datetime"]
    for name in candidates:
        if name in df.columns:
            return name
    if isinstance(df.index, pd.DatetimeIndex):
        return "__index__"
    return None


__all__ = [
    "NORMALIZED_TIMEZONE",
    "TIMESTAMP_CONTRACT_VERSION",
    "detect_source_time_kind",
    "normalize_timestamp_frame",
    "normalize_timestamp_scalar",
    "normalize_timestamp_series",
]
