from __future__ import annotations

import logging
import json
import socket
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_SH = ZoneInfo('Asia/Shanghai')
from typing import Any, Optional

import pandas as pd

from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

logger = logging.getLogger(__name__)


class RealtimeDuckDBSink:
    """Persist realtime quote/orderbook records into DuckDB with idempotent upserts."""

    def __init__(self, duckdb_path: Optional[str] = None):
        self.duckdb_path = resolve_duckdb_path(duckdb_path)
        self._db = get_db_manager(self.duckdb_path)
        self._tables_ready = False

    def ensure_tables(self) -> None:
        if self._tables_ready:
            return
        with self._db.get_write_connection() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_raw_quote (
                    symbol VARCHAR,
                    source VARCHAR,
                    event_ts TIMESTAMP,
                    ingest_ts TIMESTAMP,
                    seq_no BIGINT DEFAULT 0,
                    price DECIMAL(12,4),
                    open DECIMAL(12,4),
                    high DECIMAL(12,4),
                    low DECIMAL(12,4),
                    prev_close DECIMAL(12,4),
                    change DECIMAL(12,4),
                    change_pct DOUBLE,
                    volume BIGINT,
                    amount DOUBLE,
                    turnover_rate DOUBLE,
                    bid1 DECIMAL(12,4),
                    ask1 DECIMAL(12,4),
                    bid1_vol BIGINT,
                    ask1_vol BIGINT,
                    schema_version INTEGER DEFAULT 1,
                    data_version VARCHAR,
                    adapter_version VARCHAR,
                    collector_host VARCHAR,
                    trace_id VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, source, event_ts, seq_no)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_raw_orderbook_l1_5 (
                    symbol VARCHAR,
                    source VARCHAR,
                    event_ts TIMESTAMP,
                    ingest_ts TIMESTAMP,
                    side VARCHAR,
                    level INTEGER,
                    price DECIMAL(12,4),
                    volume BIGINT,
                    schema_version INTEGER DEFAULT 1,
                    data_version VARCHAR,
                    adapter_version VARCHAR,
                    collector_host VARCHAR,
                    trace_id VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, source, event_ts, side, level)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_bar_1m (
                    symbol VARCHAR,
                    source VARCHAR,
                    bar_minute TIMESTAMP,
                    open DECIMAL(12,4),
                    high DECIMAL(12,4),
                    low DECIMAL(12,4),
                    close DECIMAL(12,4),
                    volume BIGINT,
                    amount DOUBLE,
                    trades BIGINT,
                    first_event_ts TIMESTAMP,
                    last_event_ts TIMESTAMP,
                    is_final BOOLEAN DEFAULT FALSE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, source, bar_minute)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS data_metadata (
                    entity_type VARCHAR,
                    entity_key VARCHAR,
                    schema_version INTEGER,
                    data_version VARCHAR,
                    source VARCHAR,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (entity_type, entity_key)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS realtime_reject_log (
                    symbol VARCHAR,
                    source VARCHAR,
                    event_ts TIMESTAMP,
                    ingest_ts TIMESTAMP,
                    reject_reason VARCHAR,
                    payload_json VARCHAR,
                    trace_id VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        self._tables_ready = True

    def write_quotes(self, quotes: list[dict[str, Any]]) -> dict[str, Any]:
        self.ensure_tables()
        if not quotes:
            return {"quote_rows": 0, "orderbook_rows": 0, "latency_ms": []}

        now = datetime.now(tz=_SH)
        host = socket.gethostname()
        quote_rows: list[dict[str, Any]] = []
        orderbook_rows: list[dict[str, Any]] = []
        reject_rows: list[dict[str, Any]] = []
        latencies: list[float] = []

        for idx, quote in enumerate(quotes, start=1):
            symbol = str(quote.get("symbol") or "").strip()
            if not symbol:
                continue
            source = str(quote.get("source") or "unknown").strip()
            event_ts = self._to_event_ts(quote, now)
            ingest_ts = now
            seq_raw = quote.get("seq_no")
            seq_no = int(seq_raw) if seq_raw not in (None, "") else idx
            trace_id = str(quote.get("trace_id") or "")
            data_version = str(quote.get("data_version") or "")
            adapter_version = str(quote.get("adapter_version") or "realtime_push_v1")

            latency_ms = max((ingest_ts - event_ts).total_seconds() * 1000.0, 0.0)
            latencies.append(latency_ms)

            parsed_price = self._to_float(quote.get("price"))
            parsed_open = self._to_float(quote.get("open"))
            parsed_high = self._to_float(quote.get("high"))
            parsed_low = self._to_float(quote.get("low"))
            parsed_close = parsed_price
            parsed_volume = self._to_int(quote.get("volume"))
            gate_errors: list[str] = []
            if parsed_price is None or parsed_price <= 0:
                gate_errors.append("price_non_positive")
            if parsed_volume is not None and parsed_volume < 0:
                gate_errors.append("volume_negative")
            ohlc_values = [parsed_open, parsed_high, parsed_low, parsed_close]
            if all(v is not None for v in ohlc_values):
                if not (parsed_high >= max(parsed_open, parsed_close) and parsed_low <= min(parsed_open, parsed_close)):
                    gate_errors.append("ohlc_inconsistent")
            if gate_errors:
                reject_rows.append(
                    {
                        "symbol": symbol,
                        "source": source,
                        "event_ts": event_ts,
                        "ingest_ts": ingest_ts,
                        "reject_reason": "|".join(gate_errors),
                        "payload_json": json.dumps(quote, ensure_ascii=False, default=str),
                        "trace_id": trace_id,
                    }
                )
                continue

            quote_rows.append(
                {
                    "symbol": symbol,
                    "source": source,
                    "event_ts": event_ts,
                    "ingest_ts": ingest_ts,
                    "seq_no": seq_no,
                    "price": parsed_price,
                    "open": parsed_open,
                    "high": parsed_high,
                    "low": parsed_low,
                    "prev_close": self._to_float(quote.get("last_close") or quote.get("prev_close")),
                    "change": self._to_float(quote.get("change")),
                    "change_pct": self._to_float(quote.get("change_pct")),
                    "volume": parsed_volume,
                    "amount": self._to_float(quote.get("amount") or quote.get("turnover")),
                    "turnover_rate": self._to_float(quote.get("turnover_rate")),
                    "bid1": self._to_float(quote.get("bid1")),
                    "ask1": self._to_float(quote.get("ask1")),
                    "bid1_vol": self._to_int(quote.get("bid1_vol") or quote.get("bid1_volume")),
                    "ask1_vol": self._to_int(quote.get("ask1_vol") or quote.get("ask1_volume")),
                    "schema_version": 1,
                    "data_version": data_version,
                    "adapter_version": adapter_version,
                    "collector_host": host,
                    "trace_id": trace_id,
                }
            )

            for side in ("bid", "ask"):
                for level in range(1, 6):
                    price = self._to_float(quote.get(f"{side}{level}"))
                    volume = self._to_int(
                        quote.get(f"{side}{level}_vol") or quote.get(f"{side}{level}_volume")
                    )
                    if price is None and volume is None:
                        continue
                    orderbook_rows.append(
                        {
                            "symbol": symbol,
                            "source": source,
                            "event_ts": event_ts,
                            "ingest_ts": ingest_ts,
                            "side": side,
                            "level": level,
                            "price": price,
                            "volume": volume,
                            "schema_version": 1,
                            "data_version": data_version,
                            "adapter_version": adapter_version,
                            "collector_host": host,
                            "trace_id": trace_id,
                        }
                    )

        if not quote_rows and not reject_rows:
            return {"quote_rows": 0, "orderbook_rows": 0, "rejected_rows": 0, "latency_ms": latencies}

        with self._db.get_write_connection() as con:
            if reject_rows:
                reject_df = pd.DataFrame(reject_rows)
                con.register("rt_reject_rows", reject_df)
                con.execute(
                    """
                    INSERT INTO realtime_reject_log (
                        symbol, source, event_ts, ingest_ts, reject_reason, payload_json, trace_id
                    )
                    SELECT
                        symbol, source, event_ts, ingest_ts, reject_reason, payload_json, trace_id
                    FROM rt_reject_rows
                    """
                )
                con.unregister("rt_reject_rows")
                logger.warning("实时门禁拒绝 %s 条 quote", len(reject_rows))
            if not quote_rows:
                return {
                    "quote_rows": 0,
                    "orderbook_rows": 0,
                    "rejected_rows": len(reject_rows),
                    "latency_ms": latencies,
                }
            quote_df = pd.DataFrame(quote_rows)
            con.register("rt_quote_upsert", quote_df)
            con.execute(
                """
                DELETE FROM stock_raw_quote AS t
                USING rt_quote_upsert AS s
                WHERE t.symbol = s.symbol
                  AND t.source = s.source
                  AND t.event_ts = s.event_ts
                  AND t.seq_no = s.seq_no
                """
            )
            con.execute(
                """
                INSERT INTO stock_raw_quote (
                    symbol, source, event_ts, ingest_ts, seq_no,
                    price, open, high, low, prev_close,
                    change, change_pct, volume, amount, turnover_rate,
                    bid1, ask1, bid1_vol, ask1_vol,
                    schema_version, data_version, adapter_version,
                    collector_host, trace_id
                )
                SELECT
                    symbol, source, event_ts, ingest_ts, seq_no,
                    price, open, high, low, prev_close,
                    change, change_pct, volume, amount, turnover_rate,
                    bid1, ask1, bid1_vol, ask1_vol,
                    schema_version, data_version, adapter_version,
                    collector_host, trace_id
                FROM rt_quote_upsert
                """
            )
            con.unregister("rt_quote_upsert")

            if orderbook_rows:
                ob_df = pd.DataFrame(orderbook_rows)
                con.register("rt_ob_upsert", ob_df)
                con.execute(
                    """
                    DELETE FROM stock_raw_orderbook_l1_5 AS t
                    USING rt_ob_upsert AS s
                    WHERE t.symbol = s.symbol
                      AND t.source = s.source
                      AND t.event_ts = s.event_ts
                      AND t.side = s.side
                      AND t.level = s.level
                    """
                )
                con.execute(
                    """
                    INSERT INTO stock_raw_orderbook_l1_5 (
                        symbol, source, event_ts, ingest_ts, side,
                        level, price, volume, schema_version,
                        data_version, adapter_version, collector_host,
                        trace_id
                    )
                    SELECT
                        symbol, source, event_ts, ingest_ts, side,
                        level, price, volume, schema_version,
                        data_version, adapter_version, collector_host,
                        trace_id
                    FROM rt_ob_upsert
                    """
                )
                con.unregister("rt_ob_upsert")

        return {
            "quote_rows": len(quote_rows),
            "orderbook_rows": len(orderbook_rows),
            "rejected_rows": len(reject_rows),
            "latency_ms": latencies,
        }

    def query_latest_orderbook(self, symbol: str, source: Optional[str] = None) -> dict[str, Any]:
        self.ensure_tables()
        symbol = str(symbol or "").strip()
        if not symbol:
            return {}

        with self._db.get_read_connection() as con:
            if source:
                row = con.execute(
                    """
                    SELECT event_ts
                    FROM stock_raw_orderbook_l1_5
                    WHERE symbol = ? AND source = ?
                    ORDER BY event_ts DESC
                    LIMIT 1
                    """,
                    [symbol, source],
                ).fetchone()
            else:
                row = con.execute(
                    """
                    SELECT event_ts, source
                    FROM stock_raw_orderbook_l1_5
                    WHERE symbol = ?
                    ORDER BY event_ts DESC
                    LIMIT 1
                    """,
                    [symbol],
                ).fetchone()
                if row and source is None:
                    source = row[1]

            if not row:
                return {}
            event_ts = row[0]
            if source:
                df = con.execute(
                    """
                    SELECT side, level, price, volume
                    FROM stock_raw_orderbook_l1_5
                    WHERE symbol = ? AND source = ? AND event_ts = ?
                    ORDER BY side, level
                    """,
                    [symbol, source, event_ts],
                ).df()
            else:
                df = con.execute(
                    """
                    SELECT side, level, price, volume
                    FROM stock_raw_orderbook_l1_5
                    WHERE symbol = ? AND event_ts = ?
                    ORDER BY side, level
                    """,
                    [symbol, event_ts],
                ).df()

        if df.empty:
            return {}

        snapshot: dict[str, Any] = {"symbol": symbol, "source": source or "unknown", "event_ts": event_ts}
        for _, r in df.iterrows():
            side = str(r["side"])
            level = int(r["level"])
            snapshot[f"{side}{level}"] = float(r["price"]) if pd.notna(r["price"]) else None
            snapshot[f"{side}{level}_vol"] = int(r["volume"]) if pd.notna(r["volume"]) else None
        return snapshot

    def purge_expired_data(self, retention_days: int = 7) -> dict[str, int]:
        """Delete data older than retention_days from realtime tables.

        Returns dict with deleted row counts per table.
        """
        self.ensure_tables()
        cutoff = datetime.now(tz=_SH) - timedelta(days=max(retention_days, 1))
        deleted: dict[str, int] = {}
        tables = ["stock_raw_quote", "stock_raw_orderbook_l1_5", "stock_bar_1m"]
        ts_col = {"stock_raw_quote": "event_ts", "stock_raw_orderbook_l1_5": "event_ts", "stock_bar_1m": "bar_minute"}
        with self._db.get_write_connection() as con:
            for table in tables:
                col = ts_col[table]
                before = con.execute(
                    "SELECT COUNT(*) FROM " + table + " WHERE " + col + " < ?",
                    [cutoff],
                ).fetchone()
                before_count = int(before[0]) if before else 0
                if before_count > 0:
                    con.execute("DELETE FROM " + table + " WHERE " + col + " < ?", [cutoff])
                deleted[table] = before_count
        total = sum(deleted.values())
        if total > 0:
            logger.info("TTL清理: retention=%dd cutoff=%s deleted=%s", retention_days, cutoff.isoformat(), deleted)
        return deleted

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(float(value))
        except Exception:
            return None

    @staticmethod
    def _to_event_ts(quote: dict[str, Any], fallback: datetime) -> datetime:
        for key in ("event_ts", "timestamp", "ts", "datetime", "time"):
            value = quote.get(key)
            if value is None:
                continue
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    return value.replace(tzinfo=_SH)
                return value.astimezone(_SH)
            try:
                if isinstance(value, (int, float)):
                    return datetime.fromtimestamp(float(value), tz=_SH)
                parsed = pd.to_datetime(value)
                if pd.notna(parsed):
                    dt = parsed.to_pydatetime()
                    if dt.tzinfo is None:
                        return dt.replace(tzinfo=_SH)
                    return dt.astimezone(_SH)
            except Exception:
                continue
        return fallback
