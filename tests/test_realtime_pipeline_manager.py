import pandas as pd

from data_manager.realtime_pipeline_manager import RealtimePipelineManager


class TestRealtimePipelineManagerEventTime:
    def test_intraday_bar_uses_quote_timestamp_bucket(self):
        mgr = RealtimePipelineManager()
        mgr.configure(
            symbol="000988.SZ",
            period="5m",
            last_data=pd.DataFrame(
                [
                    {
                        "time": "2026-03-17 14:50:00",
                        "open": 111.2,
                        "high": 111.3,
                        "low": 111.1,
                        "close": 111.25,
                        "volume": 3000.0,
                    }
                ]
            ),
        )
        quote = {"price": 111.12, "volume": 1010.0, "time": "2026-03-17 14:59:31"}
        mgr.enqueue_quote(quote)
        result = mgr.flush(force=True)
        assert result is not None
        assert str(result["bar"]["time"]).endswith("14:55:00")

    def test_intraday_ignores_after_hours_quote(self):
        mgr = RealtimePipelineManager()
        mgr.configure(
            symbol="000988.SZ",
            period="1m",
            last_data=pd.DataFrame(
                [
                    {
                        "time": "2026-03-17 14:59:00",
                        "open": 111.2,
                        "high": 111.3,
                        "low": 111.1,
                        "close": 111.25,
                        "volume": 3000.0,
                    }
                ]
            ),
        )
        mgr.enqueue_quote({"price": 111.12, "volume": 1010.0, "time": "2026-03-17 18:05:00"})
        result = mgr.flush(force=True)
        assert result is None

    def test_event_ts_ms_priority_over_time_field(self):
        mgr = RealtimePipelineManager()
        mgr.configure(
            symbol="000988.SZ",
            period="1m",
            last_data=pd.DataFrame(
                [
                    {
                        "time": "2026-03-17 14:59:00",
                        "open": 111.2,
                        "high": 111.3,
                        "low": 111.1,
                        "close": 111.25,
                        "volume": 3000.0,
                    }
                ]
            ),
        )
        quote = {
            "price": 111.18,
            "volume": 1100.0,
            "time": "2026-03-17 18:05:00",
            "event_ts_ms": int(pd.Timestamp("2026-03-17 14:59:31").timestamp() * 1000),
        }
        mgr.enqueue_quote(quote)
        result = mgr.flush(force=True)
        assert result is not None
        assert str(result["bar"]["time"]).endswith("14:59:00")
