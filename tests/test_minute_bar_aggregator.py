import time

from data_manager.duckdb_connection_pool import get_db_manager
from easy_xt.realtime_data.aggregator.minute_bar_aggregator import MinuteBarAggregator
from easy_xt.realtime_data.persistence.duckdb_sink import RealtimeDuckDBSink


def test_minute_bar_aggregation_and_miss_rate(tmp_path):
    db_path = tmp_path / "test_agg.ddb"
    sink = RealtimeDuckDBSink(duckdb_path=str(db_path))
    now = time.time()
    quotes = [
        {"symbol": "000001.SZ", "source": "tdx", "timestamp": now - 2, "price": 10.0, "volume": 100},
        {"symbol": "000001.SZ", "source": "tdx", "timestamp": now - 1, "price": 10.2, "volume": 200},
    ]
    sink.write_quotes(quotes)

    aggregator = MinuteBarAggregator(duckdb_path=str(db_path))
    result = aggregator.run_once(watermark_seconds=1, recompute_minutes=1)
    assert result["bar_rows"] >= 1

    miss = aggregator.compute_miss_rate(lookback_minutes=1)
    assert miss["expected"] >= 1
    assert 0.0 <= miss["miss_rate"] <= 1.0


def test_minute_bar_late_arrival_and_finalize(tmp_path):
    db_path = tmp_path / "test_agg_late.ddb"
    sink = RealtimeDuckDBSink(duckdb_path=str(db_path))
    base = time.time() - 120
    sink.write_quotes(
        [
            {"symbol": "000001.SZ", "source": "tdx", "timestamp": base + 1, "price": 10.0, "volume": 100},
            {"symbol": "000001.SZ", "source": "tdx", "timestamp": base + 2, "price": 10.2, "volume": 200},
        ]
    )
    aggregator = MinuteBarAggregator(duckdb_path=str(db_path))
    aggregator.run_once(watermark_seconds=1, recompute_minutes=5)

    sink.write_quotes(
        [{"symbol": "000001.SZ", "source": "tdx", "timestamp": base + 50, "price": 10.8, "volume": 50}]
    )
    aggregator.run_once(watermark_seconds=1, recompute_minutes=5)

    manager = get_db_manager(str(db_path))
    with manager.get_read_connection() as con:
        close_price = con.execute(
            """
            SELECT close
            FROM stock_bar_1m
            WHERE symbol='000001.SZ' AND source='tdx'
            ORDER BY bar_minute DESC
            LIMIT 1
            """
        ).fetchone()[0]
    assert float(close_price) == 10.8

    finalized = aggregator.finalize_window(watermark_seconds=1)
    assert finalized >= 1
    finalized_again = aggregator.finalize_window(watermark_seconds=1)
    assert finalized_again >= 0

    with manager.get_read_connection() as con:
        is_final = con.execute(
            """
            SELECT is_final
            FROM stock_bar_1m
            WHERE symbol='000001.SZ' AND source='tdx'
            ORDER BY bar_minute DESC
            LIMIT 1
            """
        ).fetchone()[0]
    assert bool(is_final) is True
