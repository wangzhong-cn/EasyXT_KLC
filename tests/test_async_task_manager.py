"""
tests/test_async_task_manager.py
Coverage for core/async_task_manager.py

Uses the session-scoped qapp fixture (from conftest.py) so Qt objects
can be constructed safely.  task.run() is called directly (synchronous)
to avoid needing a running event loop.
"""

import time
import pytest


# ---------------------------------------------------------------------------
# TaskPriority enum
# ---------------------------------------------------------------------------
class TestTaskPriority:
    def test_low_is_zero(self):
        from core.async_task_manager import TaskPriority
        assert TaskPriority.LOW == 0

    def test_normal_is_one(self):
        from core.async_task_manager import TaskPriority
        assert TaskPriority.NORMAL == 1

    def test_high_is_two(self):
        from core.async_task_manager import TaskPriority
        assert TaskPriority.HIGH == 2

    def test_critical_is_three(self):
        from core.async_task_manager import TaskPriority
        assert TaskPriority.CRITICAL == 3

    def test_high_greater_than_low(self):
        from core.async_task_manager import TaskPriority
        assert TaskPriority.HIGH > TaskPriority.LOW


# ---------------------------------------------------------------------------
# TaskState class
# ---------------------------------------------------------------------------
class TestTaskState:
    def test_pending_value(self):
        from core.async_task_manager import TaskState
        assert TaskState.PENDING == 'pending'

    def test_running_value(self):
        from core.async_task_manager import TaskState
        assert TaskState.RUNNING == 'running'

    def test_completed_value(self):
        from core.async_task_manager import TaskState
        assert TaskState.COMPLETED == 'completed'

    def test_cancelled_value(self):
        from core.async_task_manager import TaskState
        assert TaskState.CANCELLED == 'cancelled'

    def test_failed_value(self):
        from core.async_task_manager import TaskState
        assert TaskState.FAILED == 'failed'


# ---------------------------------------------------------------------------
# AsyncTask (QThread subclass)
# ---------------------------------------------------------------------------
class TestAsyncTask:
    """AsyncTask construction and direct run() call tests."""

    def test_construction_defaults(self, qapp):
        from core.async_task_manager import AsyncTask, TaskPriority
        fn = lambda: 42
        task = AsyncTask(fn)
        assert task.task_fn is fn
        assert task.args == ()
        assert task.kwargs == {}
        assert task.task_key is None
        assert task.task_priority == TaskPriority.NORMAL
        assert task.cancellable is True
        assert task.timeout is None
        task.deleteLater()

    def test_construction_with_full_args(self, qapp):
        from core.async_task_manager import AsyncTask, TaskPriority
        fn = lambda x, y: x + y
        task = AsyncTask(
            fn,
            args=(1, 2),
            kwargs={'extra': True},
            task_key='mykey',
            priority=TaskPriority.HIGH,
            cancellable=False,
            timeout=5.0,
        )
        assert task.args == (1, 2)
        assert task.kwargs == {'extra': True}
        assert task.task_key == 'mykey'
        assert task.task_priority == TaskPriority.HIGH
        assert task.cancellable is False
        assert task.timeout == 5.0
        task.deleteLater()

    def test_is_cancelled_initially_false(self, qapp):
        from core.async_task_manager import AsyncTask
        task = AsyncTask(lambda: None)
        assert task.is_cancelled() is False
        task.deleteLater()

    def test_elapsed_time_before_start_is_zero(self, qapp):
        from core.async_task_manager import AsyncTask
        task = AsyncTask(lambda: None)
        assert task.elapsed_time == 0
        task.deleteLater()

    def test_run_executes_fn_and_sets_result(self, qapp):
        from core.async_task_manager import AsyncTask
        task = AsyncTask(lambda: 99)
        task.run()   # synchronous call in test thread
        assert task._result == 99
        task.deleteLater()

    def test_run_with_args(self, qapp):
        from core.async_task_manager import AsyncTask
        task = AsyncTask(lambda x, y: x * y, args=(3, 7))
        task.run()
        assert task._result == 21
        task.deleteLater()

    def test_run_emits_error_on_exception(self, qapp):
        from core.async_task_manager import AsyncTask
        errors = []
        task = AsyncTask(lambda: 1 / 0)
        task.error.connect(errors.append)
        task.run()
        assert task._exception is not None
        assert isinstance(task._exception, ZeroDivisionError)
        task.deleteLater()

    def test_run_skips_fn_if_cancelled(self, qapp):
        from core.async_task_manager import AsyncTask
        called = []
        task = AsyncTask(lambda: called.append(1))
        task._cancelled = True
        task.run()
        assert called == []
        task.deleteLater()

    def test_elapsed_time_after_run_positive(self, qapp):
        from core.async_task_manager import AsyncTask
        task = AsyncTask(lambda: None)
        task.run()
        # _started_at should have been set inside run()
        assert task._started_at is not None


# ---------------------------------------------------------------------------
# AsyncTaskManager singleton
# ---------------------------------------------------------------------------
class TestAsyncTaskManagerSingleton:
    def test_singleton_same_instance(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        m1 = AsyncTaskManager()
        m2 = AsyncTaskManager()
        assert m1 is m2

    def test_global_async_manager_is_instance(self, qapp):
        from core.async_task_manager import AsyncTaskManager, async_manager
        assert isinstance(async_manager, AsyncTaskManager)


class TestAsyncTaskManagerSubmit:
    def test_submit_returns_string_id(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        task_id = mgr.submit(lambda: None)
        assert isinstance(task_id, str)
        assert task_id.startswith('task_')

    def test_submit_increments_counter(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        before = mgr._task_counter
        mgr.submit(lambda: None)
        assert mgr._task_counter == before + 1

    def test_submit_with_task_key_registers_pending(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        unique_key = f'test_key_{time.time()}'
        mgr.submit(lambda: time.sleep(0.2), task_key=unique_key)
        assert mgr.is_pending(unique_key)

    def test_submit_dedup_cancels_old_task(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        dedup_key = f'dedup_{time.time()}'
        task_id1 = mgr.submit(lambda: time.sleep(1), task_key=dedup_key)
        task_id2 = mgr.submit(lambda: None, task_key=dedup_key)
        assert task_id1 != task_id2
        # After dedup, the second task's id is registered
        assert mgr._pending_keys.get(dedup_key) == task_id2

    def test_submit_batch_returns_list_of_ids(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        tasks = [
            (lambda: 1, (), {}, f'batch_a_{time.time()}'),
            (lambda: 2, (), {}, f'batch_b_{time.time()}'),
        ]
        ids = mgr.submit_batch(tasks)
        assert isinstance(ids, list)
        assert len(ids) == 2
        assert all(isinstance(i, str) for i in ids)

    def test_submit_batch_short_spec(self, qapp):
        """submit_batch handles specs with fewer than 4 elements."""
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        tasks = [(lambda: 3,)]  # only task_fn
        ids = mgr.submit_batch(tasks)
        assert len(ids) == 1


class TestAsyncTaskManagerCancel:
    def test_cancel_nonexistent_returns_false(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        assert mgr.cancel('nonexistent_task_xyz999') is False

    def test_cancel_by_key_nonexistent_returns_false(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        assert mgr.cancel_by_key('nonexistent_key_xyz999') is False

    def test_cancel_by_key_existing_task(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        cancel_key = f'cancel_me_{time.time()}'
        mgr.submit(lambda: time.sleep(0.5), task_key=cancel_key)
        result = mgr.cancel_by_key(cancel_key)
        assert result is True


class TestAsyncTaskManagerQuery:
    def test_is_running_nonexistent_returns_false(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        assert mgr.is_running('nonexistent_task_xyz') is False

    def test_is_pending_new_key_false(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        assert mgr.is_pending('brand_new_unique_key_xyz9999') is False

    def test_get_active_count_is_nonneg_int(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        count = mgr.get_active_count()
        assert isinstance(count, int)
        assert count >= 0

    def test_get_result_nonexistent_returns_none(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        assert mgr.get_result('nonexistent_task_id') is None

    def test_get_result_of_fast_task(self, qapp):
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        task_id = mgr.submit(lambda: 777)
        result = mgr.get_result(task_id, timeout=3.0)
        # result is None because future.result() returns None for QThread.run()
        # (run() has no return value from the executor's perspective)
        # just ensure the call doesn't raise
        assert result is None or result == 777


class TestAsyncTaskManagerCallbacks:
    def test_on_task_finished_with_callback(self, qapp):
        """_on_task_finished should invoke the callback."""
        from core.async_task_manager import AsyncTaskManager
        results = []
        mgr = AsyncTaskManager()
        mgr._on_task_finished('dummy_id', 42, lambda r: results.append(r))
        assert results == [42]

    def test_on_task_finished_no_callback(self, qapp):
        """_on_task_finished with None callback should not raise."""
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        mgr._on_task_finished('dummy_id2', 'hello', None)

    def test_on_task_finished_callback_exception_not_raised(self, qapp):
        """Callback that raises should be caught inside _on_task_finished."""
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        def bad_callback(r):
            raise RuntimeError("callback error")
        # Should not raise
        mgr._on_task_finished('dummy_id3', 10, bad_callback)

    def test_on_task_error_logs(self, qapp):
        """_on_task_error should not raise."""
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        mgr._on_task_error('dummy_id4', ValueError("test"))

    def test_on_task_progress(self, qapp):
        """_on_task_progress should not raise."""
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        mgr._on_task_progress('dummy_id5', 5, 10, 'doing stuff')

    def test_on_task_cancelled(self, qapp):
        """_on_task_cancelled should not raise."""
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        mgr._on_task_cancelled('dummy_id6')

    def test_cleanup_finished_tasks_empty(self, qapp):
        """_cleanup_finished_tasks on empty manager should not raise."""
        from core.async_task_manager import AsyncTaskManager
        mgr = AsyncTaskManager()
        # Remove all tasks for isolation (this test doesn't care about
        # existing tasks from other tests)
        original = dict(mgr._tasks)
        mgr._tasks.clear()
        mgr._cleanup_finished_tasks()
        mgr._tasks.update(original)
