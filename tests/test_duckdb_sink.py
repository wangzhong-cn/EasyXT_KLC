from data_manager.duckdb_connection_pool import get_db_manager
from easy_xt.realtime_data.persistence.duckdb_sink import RealtimeDuckDBSink


def test_duckdb_sink_write_and_query_orderbook(tmp_path):
    db_path = tmp_path / "test_sink.ddb"
    sink = RealtimeDuckDBSink(duckdb_path=str(db_path))
    quotes = [
        {
            "symbol": "000001.SZ",
            "source": "tdx",
            "timestamp": 1710000000,
            "price": 10.5,
            "volume": 1000,
            "amount": 10500.0,
            "bid1": 10.4,
            "ask1": 10.6,
            "bid1_vol": 500,
            "ask1_vol": 400,
        }
    ]
    stats = sink.write_quotes(quotes)
    assert stats["quote_rows"] == 1
    assert stats["orderbook_rows"] >= 2

    snapshot = sink.query_latest_orderbook("000001.SZ", source="tdx")
    assert snapshot.get("bid1") == 10.4
    assert snapshot.get("ask1") == 10.6

    manager = get_db_manager(db_path)
    with manager.get_read_connection() as con:
        count = con.execute("SELECT COUNT(*) FROM stock_raw_quote").fetchone()[0]
    assert count == 1


def test_duckdb_sink_idempotent_and_latency(tmp_path):
    db_path = tmp_path / "test_sink_idem.ddb"
    sink = RealtimeDuckDBSink(duckdb_path=str(db_path))
    quote = {
        "symbol": "000001.SZ",
        "source": "tdx",
        "timestamp": 1710000000,
        "price": 10.5,
        "volume": 1000,
        "amount": 10500.0,
    }
    sink.write_quotes([quote])
    sink.write_quotes([quote])

    manager = get_db_manager(db_path)
    with manager.get_read_connection() as con:
        count = con.execute("SELECT COUNT(*) FROM stock_raw_quote").fetchone()[0]
        row = con.execute(
            "SELECT event_ts, ingest_ts FROM stock_raw_quote WHERE symbol='000001.SZ'"
        ).fetchone()
    assert count == 1
    assert row[0] is not None
    assert row[1] is not None
    assert row[1] >= row[0]


def test_duckdb_sink_full_5level_orderbook_round_trip(tmp_path):
    """Write full 5-level bid/ask data and verify query_latest_orderbook returns all levels with volumes."""
    db_path = tmp_path / "test_sink_5level.ddb"
    sink = RealtimeDuckDBSink(duckdb_path=str(db_path))
    quote = {
        "symbol": "600519.SH",
        "source": "eastmoney",
        "timestamp": 1710000000,
        "price": 1800.0,
        "volume": 5000,
        "bid1": 1799.0, "bid1_vol": 100,
        "bid2": 1798.0, "bid2_vol": 200,
        "bid3": 1797.0, "bid3_vol": 300,
        "bid4": 1796.0, "bid4_vol": 400,
        "bid5": 1795.0, "bid5_vol": 500,
        "ask1": 1801.0, "ask1_vol": 110,
        "ask2": 1802.0, "ask2_vol": 220,
        "ask3": 1803.0, "ask3_vol": 330,
        "ask4": 1804.0, "ask4_vol": 440,
        "ask5": 1805.0, "ask5_vol": 550,
    }
    stats = sink.write_quotes([quote])
    assert stats["orderbook_rows"] == 10  # 5 bid + 5 ask

    snapshot = sink.query_latest_orderbook("600519.SH")
    for level in range(1, 6):
        assert snapshot.get(f"bid{level}") is not None, f"bid{level} missing"
        assert snapshot.get(f"ask{level}") is not None, f"ask{level} missing"
        assert snapshot.get(f"bid{level}_vol") is not None and snapshot[f"bid{level}_vol"] > 0, f"bid{level}_vol missing"
        assert snapshot.get(f"ask{level}_vol") is not None and snapshot[f"ask{level}_vol"] > 0, f"ask{level}_vol missing"
    assert snapshot["bid1"] == 1799.0
    assert snapshot["ask5"] == 1805.0
    assert snapshot["bid5_vol"] == 500
    assert snapshot["ask3_vol"] == 330


def test_duckdb_sink_purge_expired_data(tmp_path):
    """Verify TTL purge deletes old data and retains recent data."""
    import time
    db_path = tmp_path / "test_sink_purge.ddb"
    sink = RealtimeDuckDBSink(duckdb_path=str(db_path))

    # Write old data (timestamp 10 days ago)
    old_ts = time.time() - 10 * 86400
    sink.write_quotes([{
        "symbol": "000001.SZ", "source": "tdx", "timestamp": old_ts,
        "price": 10.0, "volume": 100, "bid1": 9.9, "ask1": 10.1,
        "bid1_vol": 50, "ask1_vol": 60,
    }])
    # Write recent data (now)
    sink.write_quotes([{
        "symbol": "000002.SZ", "source": "tdx", "timestamp": time.time(),
        "price": 20.0, "volume": 200, "bid1": 19.9, "ask1": 20.1,
        "bid1_vol": 70, "ask1_vol": 80,
    }])

    manager = get_db_manager(str(db_path))
    with manager.get_read_connection() as con:
        before_count = con.execute("SELECT COUNT(*) FROM stock_raw_quote").fetchone()[0]
    assert before_count == 2

    deleted = sink.purge_expired_data(retention_days=7)
    assert deleted["stock_raw_quote"] == 1  # old record deleted
    assert deleted["stock_raw_orderbook_l1_5"] >= 2  # old orderbook rows deleted

    with manager.get_read_connection() as con:
        after_count = con.execute("SELECT COUNT(*) FROM stock_raw_quote").fetchone()[0]
    assert after_count == 1  # only recent record remains


def test_tdx_pytdx_field_name_mapping():
    """Verify TDX provider correctly maps pytdx field names (bid_vol1 → bid1_vol)."""
    from easy_xt.realtime_data.providers.tdx_provider import TdxDataProvider

    provider = TdxDataProvider.__new__(TdxDataProvider)
    provider.logger = __import__("logging").getLogger("test")

    # Simulate raw pytdx output with bid_volN / ask_volN field names
    raw_pytdx_quote = {
        "code": "000001", "name": "平安银行",
        "price": 10.5, "last_close": 10.3,
        "vol": 1000, "amount": 10500.0,
        "high": 10.8, "low": 10.2, "open": 10.4,
        "bid1": 10.4, "ask1": 10.6,
        "bid_vol1": 500, "ask_vol1": 400,
        "bid2": 10.3, "ask2": 10.7,
        "bid_vol2": 600, "ask_vol2": 350,
        "bid3": 10.2, "ask3": 10.8,
        "bid_vol3": 700, "ask_vol3": 300,
        "bid4": 10.1, "ask4": 10.9,
        "bid_vol4": 800, "ask_vol4": 250,
        "bid5": 10.0, "ask5": 11.0,
        "bid_vol5": 900, "ask_vol5": 200,
    }

    result = provider._format_quote_data(raw_pytdx_quote)
    assert result is not None
    # Verify prices pass through correctly
    assert result["bid1"] == 10.4
    assert result["ask5"] == 11.0
    # Verify volumes mapped from pytdx bid_volN format
    assert result["bid1_vol"] == 500
    assert result["ask1_vol"] == 400
    assert result["bid2_vol"] == 600
    assert result["bid5_vol"] == 900
    assert result["ask5_vol"] == 200
