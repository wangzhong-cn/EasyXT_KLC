import time

from data_manager.realtime_pipeline_manager import RealtimePipelineManager


def test_update_config_max_queue_trims():
    manager = RealtimePipelineManager(max_queue=64, flush_interval_ms=200)
    for i in range(100):
        manager.enqueue_quote({"price": 1.0 + i, "volume": i})
    manager.update_config(max_queue=32)
    assert manager.max_queue == 32
    assert len(manager._queue) <= 32


def test_sustained_drop_alert():
    manager = RealtimePipelineManager(max_queue=32, flush_interval_ms=200)
    manager.update_config(drop_rate_threshold=0.1, window_seconds=1.0, alert_sustain_s=0.1)
    for i in range(100):
        manager.enqueue_quote({"price": 1.0 + i, "volume": i})
    metrics = manager.metrics()
    assert metrics["window_threshold_exceeded"] is True
    time.sleep(0.2)
    metrics = manager.metrics()
    assert metrics["sustained_drop_alert"] is True
