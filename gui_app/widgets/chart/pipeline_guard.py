from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

_INTRADAY_MINUTES = {
    "1m": 1,
    "2m": 2,
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "20m": 20,
    "25m": 25,
    "30m": 30,
    "50m": 50,
    "60m": 60,
    "70m": 70,
    "120m": 120,
    "125m": 125,
    "1h": 60,
    "2h": 120,
}

_DATE_ONLY_PERIODS = {
    "1d",
    "2d",
    "3d",
    "5d",
    "10d",
    "25d",
    "50d",
    "75d",
    "1w",
    "1mth",
    "2mth",
    "3mth",
    "5mth",
    "6mth",
    "1y",
    "2y",
    "3y",
    "5y",
    "10y",
    "1M",
    "2M",
    "3M",
    "5M",
    "6M",
}


def validate_pipeline_bar_for_period(bar: dict[str, Any], period: str) -> tuple[bool, str]:
    if not isinstance(bar, dict):
        return False, "bar_not_dict"
    p_raw = str(period or "").strip()
    p_lower = p_raw.lower()
    ts = pd.to_datetime(bar.get("time"), errors="coerce")
    if pd.isna(ts):
        return False, "invalid_time"
    if p_lower in _INTRADAY_MINUTES:
        if ts.second != 0 or ts.microsecond != 0:
            return False, "intraday_time_with_seconds"
        minutes = _INTRADAY_MINUTES[p_lower]
        if ts.minute % minutes != 0:
            return False, "intraday_time_not_aligned"
    elif p_raw in _DATE_ONLY_PERIODS or p_lower in _DATE_ONLY_PERIODS:
        if ts.hour != 0 or ts.minute != 0 or ts.second != 0 or ts.microsecond != 0:
            return False, "date_period_not_midnight"
    else:
        return False, "unsupported_period"
    for key in ("open", "high", "low", "close"):
        val = bar.get(key)
        if val is None:
            return False, f"missing_{key}"
        try:
            if not np.isfinite(float(val)):
                return False, f"invalid_{key}"
        except Exception:
            return False, f"invalid_{key}"
    return True, "ok"
