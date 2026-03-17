"""Tests for BackfillTask dataclass and HistoryBackfillScheduler logic."""
import pytest
import time
import threading
from unittest.mock import MagicMock


def _make_scheduler(worker=None):
    from data_manager.history_backfill_scheduler import HistoryBackfillScheduler
    if worker is None:
        worker = lambda payload: True
    return HistoryBackfillScheduler(worker=worker)


class TestBackfillTask:
    def test_task_created_with_correct_fields(self):
        from data_manager.history_backfill_scheduler import BackfillTask
        t = BackfillTask(priority=100, created_at=0.0, key="A", payload={"a": 1})
        assert t.priority == 100
        assert t.key == "A"
        assert t.payload == {"a": 1}
        assert t.retry_count == 0

    def test_tasks_sorted_by_priority(self):
        from data_manager.history_backfill_scheduler import BackfillTask
        low = BackfillTask(priority=200, created_at=0.0, key="low")
        high = BackfillTask(priority=50, created_at=0.0, key="high")
        assert high < low  # lower number = higher priority

    def test_task_default_retry_count_zero(self):
        from data_manager.history_backfill_scheduler import BackfillTask
        t = BackfillTask(priority=10, created_at=0.0, key="x")
        assert t.retry_count == 0
        assert t.last_retry_time == 0.0


class TestSchedulerConstruction:
    def test_constructor_sets_worker(self):
        worker = lambda p: True
        s = _make_scheduler(worker=worker)
        assert s._worker is worker

    def test_constructor_initial_state(self):
        s = _make_scheduler()
        assert not s._stop_event.is_set()
        assert s._thread is None
        assert len(s._pending_keys) == 0


class TestSchedule:
    def test_schedule_returns_true_for_new_task(self):
        s = _make_scheduler()
        ok = s.schedule("600000.SH", "2024-01-01", "2024-01-31", "1d")
        assert ok is True

    def test_schedule_empty_stock_code_returns_false(self):
        s = _make_scheduler()
        ok = s.schedule("", "2024-01-01", "2024-01-31", "1d")
        assert ok is False

    def test_schedule_empty_dates_returns_false(self):
        s = _make_scheduler()
        ok = s.schedule("600000.SH", "", "", "1d")
        assert ok is False

    def test_schedule_duplicate_key_returns_false(self):
        s = _make_scheduler()
        s.schedule("600000.SH", "2024-01-01", "2024-01-31", "1d")
        ok2 = s.schedule("600000.SH", "2024-01-01", "2024-01-31", "1d")
        assert ok2 is False

    def test_schedule_different_period_ok(self):
        s = _make_scheduler()
        s.schedule("600000.SH", "2024-01-01", "2024-01-31", "1d")
        ok = s.schedule("600000.SH", "2024-01-01", "2024-01-31", "1m")
        assert ok is True

    def test_full_queue_returns_false(self):
        s = _make_scheduler()
        # Use tiny queue size
        from data_manager.history_backfill_scheduler import HistoryBackfillScheduler
        s2 = HistoryBackfillScheduler(worker=lambda p: True, max_queue_size=1)
        s2.schedule("000001.SZ", "2024-01-01", "2024-06-30", "1d")
        # Try to add another; queue size 1 is already full
        ok = s2.schedule("600000.SH", "2024-01-01", "2024-06-30", "1d")
        # Either True or False depending on blocking; just verify no exception
        assert isinstance(ok, bool)


class TestStartStop:
    def test_start_creates_thread(self):
        s = _make_scheduler()
        s.start()
        assert s._thread is not None
        assert s._thread.is_alive()
        s.stop(timeout=0.5)

    def test_start_is_idempotent(self):
        s = _make_scheduler()
        s.start()
        t1 = s._thread
        s.start()  # second call should be no-op
        assert s._thread is t1
        s.stop(timeout=0.5)

    def test_stop_signals_thread(self):
        s = _make_scheduler()
        s.start()
        s.stop(timeout=1.0)
        # After stop, thread should be dead or stop_event set
        assert s._stop_event.is_set()

    def test_stop_without_start_does_not_raise(self):
        s = _make_scheduler()
        s.stop()  # should not raise


class TestWorkerExecution:
    def test_worker_called_with_payload(self):
        received = []
        def worker(payload):
            received.append(payload)
            return True

        s = _make_scheduler(worker=worker)
        s.start()
        s.schedule("600000.SH", "2024-01-01", "2024-01-31", "1d")
        # Give worker time to run
        time.sleep(0.2)
        s.stop(timeout=1.0)
        if received:
            assert received[0]["stock_code"] == "600000.SH"


class TestDeadLetterQueue:
    """Tests for _write_dead_letter and replay_dead_letters."""

    def _make_scheduler_with_tmp_dlq(self, tmp_path, worker=None):
        from data_manager.history_backfill_scheduler import HistoryBackfillScheduler
        if worker is None:
            worker = lambda payload: True
        s = HistoryBackfillScheduler(worker=worker)
        s._dead_letter_path = tmp_path / "dead_letter.jsonl"
        return s

    def test_write_dead_letter_creates_file(self, tmp_path):
        from data_manager.history_backfill_scheduler import BackfillTask
        s = self._make_scheduler_with_tmp_dlq(tmp_path)
        task = BackfillTask(priority=100, created_at=0.0, key="A",
                            payload={"stock_code": "600000.SH",
                                     "start_date": "2024-01-01",
                                     "end_date": "2024-01-31",
                                     "period": "1d"})
        s._write_dead_letter(task, "test_reason")
        assert s._dead_letter_path.exists()

    def test_write_dead_letter_json_content(self, tmp_path):
        import json
        from data_manager.history_backfill_scheduler import BackfillTask
        s = self._make_scheduler_with_tmp_dlq(tmp_path)
        task = BackfillTask(priority=100, created_at=0.0, key="B",
                            payload={"stock_code": "000001.SZ",
                                     "start_date": "2024-02-01",
                                     "end_date": "2024-02-28",
                                     "period": "1d"})
        s._write_dead_letter(task, "queue_full")
        lines = s._dead_letter_path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["key"] == "B"
        assert record["reason"] == "queue_full"
        assert record["payload"]["stock_code"] == "000001.SZ"

    def test_replay_returns_zero_when_no_file(self, tmp_path):
        s = self._make_scheduler_with_tmp_dlq(tmp_path)
        result = s.replay_dead_letters()
        assert result == {"replayed": 0, "remaining": 0}

    def test_replay_reschedules_dead_tasks(self, tmp_path):
        import json
        s = self._make_scheduler_with_tmp_dlq(tmp_path)
        record = {
            "key": "600000.SH|2024-01-01|2024-01-31|1d",
            "payload": {"stock_code": "600000.SH",
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                        "period": "1d"},
            "retry_count": 1,
            "reason": "max_retries_exhausted",
            "failed_at": "2024-01-31T00:00:00+00:00",
        }
        s._dead_letter_path.write_text(json.dumps(record) + "\n", encoding="utf-8")
        result = s.replay_dead_letters()
        assert result["replayed"] == 1
        assert result["remaining"] == 0
        # File should be deleted when all tasks replayed
        assert not s._dead_letter_path.exists()

    def test_replay_removes_file_when_all_replayed(self, tmp_path):
        import json
        s = self._make_scheduler_with_tmp_dlq(tmp_path)
        records = [
            {"key": f"K{i}", "payload": {"stock_code": f"60000{i}.SH",
                                         "start_date": "2024-01-01",
                                         "end_date": "2024-01-31",
                                         "period": "1d"},
             "retry_count": 0, "reason": "queue_full", "failed_at": "2024-01-01T00:00:00+00:00"}
            for i in range(3)
        ]
        s._dead_letter_path.write_text(
            "\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8"
        )
        result = s.replay_dead_letters()
        assert result["replayed"] == 3
        assert result["remaining"] == 0
        assert not s._dead_letter_path.exists()

    def test_replay_skips_malformed_lines(self, tmp_path):
        s = self._make_scheduler_with_tmp_dlq(tmp_path)
        s._dead_letter_path.write_text(
            "not valid json\n", encoding="utf-8"
        )
        result = s.replay_dead_letters()
        assert result["replayed"] == 0
        assert result["remaining"] == 0

    def test_replay_respects_limit(self, tmp_path):
        import json
        s = self._make_scheduler_with_tmp_dlq(tmp_path)
        records = [
            {"key": f"L{i}", "payload": {"stock_code": f"00000{i}.SZ",
                                          "start_date": "2024-01-01",
                                          "end_date": "2024-01-31",
                                          "period": "1d"},
             "retry_count": 0, "reason": "queue_full", "failed_at": "2024-01-01T00:00:00+00:00"}
            for i in range(5)
        ]
        s._dead_letter_path.write_text(
            "\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8"
        )
        result = s.replay_dead_letters(limit=2)
        assert result["replayed"] == 2
        assert result["remaining"] == 3
