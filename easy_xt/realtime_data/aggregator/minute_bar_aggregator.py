from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_SH = ZoneInfo('Asia/Shanghai')
from typing import Any, Optional

import pandas as pd

from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path


class MinuteBarAggregator:
    """Aggregate raw quotes into 1-minute bars with late-arrival recomputation."""

    def __init__(self, duckdb_path: Optional[str] = None):
        self.duckdb_path = resolve_duckdb_path(duckdb_path)
        self._db = get_db_manager(self.duckdb_path)

    def run_once(
        self,
        watermark_seconds: int = 5,
        recompute_minutes: int = 3,
    ) -> dict[str, Any]:
        now = datetime.now(tz=_SH)
        window_end = now - timedelta(seconds=max(1, int(watermark_seconds)))
        window_start = window_end - timedelta(minutes=max(1, int(recompute_minutes)))

        with self._db.get_read_connection() as con:
            raw_df = con.execute(
                """
                SELECT symbol, source, event_ts, seq_no, price, volume, amount
                FROM stock_raw_quote
                WHERE event_ts >= ? AND event_ts < ?
                ORDER BY symbol, source, event_ts, seq_no
                """,
                [window_start, window_end],
            ).df()

        if raw_df.empty:
            return {"bar_rows": 0, "window_start": window_start, "window_end": window_end}

        event_ts_series = pd.to_datetime(raw_df["event_ts"], errors="coerce")
        raw_df["event_ts"] = event_ts_series
        raw_df["bar_minute"] = event_ts_series.map(
            lambda ts: ts.replace(second=0, microsecond=0) if pd.notna(ts) else pd.NaT
        )
        raw_df["seq_no"] = raw_df["seq_no"].fillna(0).astype("int64")

        records: list[dict[str, Any]] = []
        for (symbol, source, bar_minute), grp in raw_df.groupby(["symbol", "source", "bar_minute"]):
            grp = grp.sort_values(["event_ts", "seq_no"])
            first = grp.iloc[0]
            last = grp.iloc[-1]
            records.append(
                {
                    "symbol": symbol,
                    "source": source,
                    "bar_minute": bar_minute.to_pydatetime(),
                    "open": self._to_float(first.get("price")),
                    "high": self._to_float(grp["price"].max()),
                    "low": self._to_float(grp["price"].min()),
                    "close": self._to_float(last.get("price")),
                    "volume": int(grp["volume"].fillna(0).sum()),
                    "amount": float(grp["amount"].fillna(0).sum()),
                    "trades": int(len(grp)),
                    "first_event_ts": first["event_ts"].to_pydatetime(),
                    "last_event_ts": last["event_ts"].to_pydatetime(),
                    "is_final": False,
                    "updated_at": datetime.now(tz=_SH),
                }
            )

        bar_df = pd.DataFrame(records)
        with self._db.get_write_connection() as con:
            con.register("agg_bar_upsert", bar_df)
            con.execute(
                """
                DELETE FROM stock_bar_1m AS t
                USING agg_bar_upsert AS s
                WHERE t.symbol = s.symbol
                  AND t.source = s.source
                  AND t.bar_minute = s.bar_minute
                """
            )
            con.execute(
                """
                INSERT INTO stock_bar_1m
                SELECT * FROM agg_bar_upsert
                """
            )
            con.unregister("agg_bar_upsert")

        return {
            "bar_rows": len(bar_df),
            "window_start": window_start,
            "window_end": window_end,
        }

    def finalize_window(self, watermark_seconds: int = 5) -> int:
        cutoff = datetime.now(tz=_SH) - timedelta(seconds=max(1, int(watermark_seconds)))
        with self._db.get_write_connection() as con:
            pending = con.execute(
                """
                SELECT COUNT(*)
                FROM stock_bar_1m
                WHERE is_final = FALSE
                  AND bar_minute < date_trunc('minute', ?)
                """,
                [cutoff],
            ).fetchone()
            con.execute(
                """
                UPDATE stock_bar_1m
                SET is_final = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE is_final = FALSE
                  AND bar_minute < date_trunc('minute', ?)
                """,
                [cutoff],
            )
        return int(pending[0] if pending else 0)

    def compute_miss_rate(self, lookback_minutes: int = 60) -> dict[str, Any]:
        lookback = max(1, int(lookback_minutes))
        now = datetime.now(tz=_SH)
        start = now - timedelta(minutes=lookback)
        end = now

        with self._db.get_read_connection() as con:
            active_row = con.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT symbol, source
                    FROM stock_raw_quote
                    WHERE event_ts >= ? AND event_ts < ?
                )
                """,
                [start, end],
            ).fetchone()
            actual_row = con.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT symbol, source, bar_minute
                    FROM stock_bar_1m
                    WHERE bar_minute >= date_trunc('minute', ?)
                      AND bar_minute < date_trunc('minute', ?)
                )
                """,
                [start, end],
            ).fetchone()
        active = int(active_row[0]) if active_row else 0
        actual = int(actual_row[0]) if actual_row else 0

        expected = active * lookback
        if expected <= 0:
            return {"miss_rate": 0.0, "actual": actual, "expected": 0}
        miss_rate = max(0.0, 1.0 - (float(actual) / float(expected)))
        return {"miss_rate": miss_rate, "actual": actual, "expected": expected}

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None
