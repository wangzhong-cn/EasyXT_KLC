from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from core.state_store import (
    DuckDBFederationExecutor,
    DuckDBFederationPlanner,
    ShardCatalog,
    ShardFamilyConfig,
    resolve_federation_attach_budget,
    resolve_state_root,
)


@dataclass(frozen=True, slots=True)
class MarketStorageTopology:
    state_root: str
    sqlite_root: str
    duckdb_shadow_path: str
    attach_budget: int
    primary_write_engine: str
    duckdb_shadow_inline_write: bool


def resolve_market_storage_topology(
    *,
    state_root: str | Path | None = None,
    duckdb_shadow_path: str | Path | None = None,
    attach_budget: int | None = None,
) -> MarketStorageTopology:
    resolved_state_root = resolve_state_root(state_root)
    sqlite_root = resolved_state_root / "market_primary"
    shadow_path = Path(duckdb_shadow_path) if duckdb_shadow_path else resolved_state_root / "duckdb_shadow" / "market_shadow.duckdb"
    shadow_path.parent.mkdir(parents=True, exist_ok=True)
    return MarketStorageTopology(
        state_root=str(resolved_state_root),
        sqlite_root=str(sqlite_root),
        duckdb_shadow_path=str(shadow_path),
        attach_budget=attach_budget or resolve_federation_attach_budget(),
        primary_write_engine=os.environ.get("EASYXT_PRIMARY_WRITE_ENGINE", "sqlite").strip().lower() or "sqlite",
        duckdb_shadow_inline_write=str(os.environ.get("EASYXT_DUCKDB_SHADOW_INLINE_WRITE", "1")).lower() in ("1", "true", "yes", "on"),
    )


class SQLitePrimaryMarketStore:
    _FAMILY_BY_PERIOD = {
        "1d": ShardFamilyConfig(
            name="market_1d",
            table_name="bars_1d",
            shard_kind="symbol_prefix",
            time_column="event_ts",
            symbol_column="stock_code",
            symbol_prefix_len=3,
        ),
        "1m": ShardFamilyConfig(
            name="market_1m",
            table_name="bars_1m",
            shard_kind="symbol_prefix",
            time_column="event_ts",
            symbol_column="stock_code",
            symbol_prefix_len=3,
        ),
        "5m": ShardFamilyConfig(
            name="market_5m",
            table_name="bars_5m",
            shard_kind="symbol_prefix",
            time_column="event_ts",
            symbol_column="stock_code",
            symbol_prefix_len=3,
        ),
        "tick": ShardFamilyConfig(
            name="market_tick",
            table_name="bars_tick",
            shard_kind="symbol_prefix",
            time_column="event_ts",
            symbol_column="stock_code",
            symbol_prefix_len=3,
        ),
    }
    _DDL_BY_PERIOD = {
        "1d": """
        CREATE TABLE IF NOT EXISTS bars_1d (
            stock_code TEXT NOT NULL,
            period TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            trade_date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            symbol_type TEXT,
            source TEXT,
            created_at TEXT,
            updated_at TEXT,
            PRIMARY KEY(stock_code, period, event_ts)
        );
        CREATE INDEX IF NOT EXISTS idx_bars_1d_code_ts ON bars_1d(stock_code, event_ts);
        """,
        "1m": """
        CREATE TABLE IF NOT EXISTS bars_1m (
            stock_code TEXT NOT NULL,
            period TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            trade_date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            symbol_type TEXT,
            source TEXT,
            created_at TEXT,
            updated_at TEXT,
            PRIMARY KEY(stock_code, period, event_ts)
        );
        CREATE INDEX IF NOT EXISTS idx_bars_1m_code_ts ON bars_1m(stock_code, event_ts);
        """,
        "5m": """
        CREATE TABLE IF NOT EXISTS bars_5m (
            stock_code TEXT NOT NULL,
            period TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            trade_date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            symbol_type TEXT,
            source TEXT,
            created_at TEXT,
            updated_at TEXT,
            PRIMARY KEY(stock_code, period, event_ts)
        );
        CREATE INDEX IF NOT EXISTS idx_bars_5m_code_ts ON bars_5m(stock_code, event_ts);
        """,
        "tick": """
        CREATE TABLE IF NOT EXISTS bars_tick (
            stock_code TEXT NOT NULL,
            period TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            trade_date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            symbol_type TEXT,
            source TEXT,
            created_at TEXT,
            updated_at TEXT,
            PRIMARY KEY(stock_code, period, event_ts)
        );
        CREATE INDEX IF NOT EXISTS idx_bars_tick_code_ts ON bars_tick(stock_code, event_ts);
        """,
    }

    def __init__(self, *, topology: MarketStorageTopology | None = None) -> None:
        self.topology = topology or resolve_market_storage_topology()
        self.sqlite_root = Path(self.topology.sqlite_root)
        self.sqlite_root.mkdir(parents=True, exist_ok=True)
        self.catalog = ShardCatalog(self.sqlite_root)
        for period, family in self._FAMILY_BY_PERIOD.items():
            self.catalog.register_family(family)

    def write_bars(
        self,
        stock_code: str,
        period: str,
        data: pd.DataFrame,
        *,
        source: str | None = None,
    ) -> dict[str, Any]:
        normalized = self._normalize_dataframe(stock_code=stock_code, period=period, data=data, source=source)
        if normalized.empty:
            return {"written_rows": 0, "written_shards": 0, "logical_seq": None}
        family = self._FAMILY_BY_PERIOD.get(period)
        if family is None:
            return {"written_rows": 0, "written_shards": 0, "logical_seq": None}

        shards: dict[str, dict[str, Any]] = {}
        for row in normalized.to_dict("records"):
            shard = self.catalog.resolve_write_shard(
                family.name,
                event_time=str(row["event_ts"]),
                symbol=stock_code,
            )
            shard_bucket = shards.setdefault(
                shard.db_path,
                {
                    "shard": shard,
                    "rows": [],
                },
            )
            shard_bucket["rows"].append(row)

        last_seq: int | None = None
        written_rows = 0
        for db_path, bucket in shards.items():
            shard = bucket["shard"]
            rows = bucket["rows"]
            con = self._connect_sqlite(db_path)
            try:
                con.executescript(self._DDL_BY_PERIOD[period])
                quoted_cols = [
                    "stock_code",
                    "period",
                    "event_ts",
                    "trade_date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "symbol_type",
                    "source",
                    "created_at",
                    "updated_at",
                ]
                placeholders = ", ".join("?" for _ in quoted_cols)
                updates = ", ".join(
                    f"{col}=excluded.{col}"
                    for col in quoted_cols
                    if col not in {"stock_code", "period", "event_ts"}
                )
                sql = (
                    f"INSERT INTO {family.table_name} ({', '.join(quoted_cols)}) "
                    f"VALUES ({placeholders}) "
                    "ON CONFLICT(stock_code, period, event_ts) DO UPDATE SET "
                    f"{updates}"
                )
                con.execute("BEGIN IMMEDIATE")
                con.executemany(
                    sql,
                    [
                        tuple(row.get(col) for col in quoted_cols)
                        for row in rows
                    ],
                )
                con.commit()
            except Exception:
                con.rollback()
                raise
            finally:
                con.close()
            last_seq = self.catalog.allocate_logical_seq()
            self.catalog.mark_shard_write(shard.shard_id, logical_seq=last_seq, row_delta=len(rows))
            written_rows += len(rows)

        return {
            "written_rows": written_rows,
            "written_shards": len(shards),
            "logical_seq": last_seq,
        }

    def read_bars(
        self,
        stock_code: str,
        period: str,
        start_date: str,
        end_date: str,
        *,
        limit: int | None = None,
    ) -> pd.DataFrame:
        family = self._FAMILY_BY_PERIOD.get(period)
        if family is None:
            return pd.DataFrame()
        planner = DuckDBFederationPlanner(self.catalog, attach_budget=self.topology.attach_budget)
        executor = DuckDBFederationExecutor(planner)
        result = executor.execute_family_query(
            family.name,
            start_time=self._range_start(period, start_date),
            end_time=self._range_end(period, end_date),
            symbol=stock_code,
            selected_columns=[
                "stock_code",
                "period",
                "event_ts",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "symbol_type",
                "source",
            ],
            where_sql="stock_code = ? AND period = ? AND event_ts >= ? AND event_ts <= ?",
            order_by="event_ts ASC",
            query_params=(
                stock_code,
                period,
                self._range_start(period, start_date),
                self._range_end(period, end_date),
            ),
            limit=limit,
        )
        if not result.rows:
            return pd.DataFrame()
        df = pd.DataFrame(result.rows)
        df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce")
        df = df[df["event_ts"].notna()].copy()
        if df.empty:
            return df
        if period == "1d":
            if "trade_date" in df.columns:
                trade_dt = pd.to_datetime(df["trade_date"], errors="coerce")
                df["date"] = trade_dt.fillna(df["event_ts"])
            else:
                df["date"] = df["event_ts"]
            df = df.set_index("date").sort_index()
        else:
            df = df.set_index("event_ts").sort_index()
            df.index.name = "datetime"
        return df

    @staticmethod
    def _connect_sqlite(db_path: str) -> sqlite3.Connection:
        con = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA busy_timeout=30000")
        return con

    @staticmethod
    def _range_start(period: str, value: str) -> str:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return value
        return ts.strftime("%Y-%m-%dT00:00:00") if period == "1d" else ts.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def _range_end(period: str, value: str) -> str:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return value
        return ts.strftime("%Y-%m-%dT23:59:59") if period == "1d" else ts.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def _normalize_dataframe(
        *,
        stock_code: str,
        period: str,
        data: pd.DataFrame,
        source: str | None = None,
    ) -> pd.DataFrame:
        if data is None or data.empty:
            return pd.DataFrame()
        df = data.copy()
        if df.index.name and df.index.name in df.columns:
            df = df.reset_index(drop=True)
        else:
            df = df.reset_index()
        if period == "1d":
            if "date" not in df.columns and "datetime" in df.columns:
                df = df.rename(columns={"datetime": "date"})
            if "date" not in df.columns and "index" in df.columns:
                df = df.rename(columns={"index": "date"})
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df[df["date"].notna()].copy()
            df["event_ts"] = df["date"].dt.strftime("%Y-%m-%dT00:00:00")
            df["trade_date"] = df["date"].dt.strftime("%Y-%m-%d")
        else:
            if "datetime" not in df.columns and "time" in df.columns:
                df = df.rename(columns={"time": "datetime"})
            if "datetime" not in df.columns and "index" in df.columns:
                df = df.rename(columns={"index": "datetime"})
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df = df[df["datetime"].notna()].copy()
            df["event_ts"] = df["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S")
            df["trade_date"] = df["datetime"].dt.strftime("%Y-%m-%d")
        if df.empty:
            return df
        now_iso = pd.Timestamp.now().isoformat(timespec="seconds")
        df["stock_code"] = stock_code
        df["period"] = period
        df["source"] = source or "unified_data_interface"
        df["created_at"] = now_iso
        df["updated_at"] = now_iso
        for col in ("open", "high", "low", "close", "volume", "amount"):
            if col not in df.columns:
                df[col] = None
        if "symbol_type" not in df.columns:
            df["symbol_type"] = "stock"
        return df[
            [
                "stock_code",
                "period",
                "event_ts",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "symbol_type",
                "source",
                "created_at",
                "updated_at",
            ]
        ]


class SQLitePrimaryMarketSource:
    def __init__(self, store: SQLitePrimaryMarketStore):
        self.store = store
        self.name = "sqlite_primary"

    def get_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
    ) -> pd.DataFrame:
        if adjust != "none" and period != "1d":
            adjust = "none"
        return self.store.read_bars(symbol, period, start_date, end_date)

    def health(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "available": True,
            "sqlite_root": self.store.topology.sqlite_root,
            "attach_budget": self.store.topology.attach_budget,
        }
