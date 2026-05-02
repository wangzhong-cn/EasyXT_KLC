"""
ThreadManager — daemon threading.Thread 生命周期管理器（Phase 2 PR-1）

将散落在各模块的 ``threading.Thread(daemon=True).start()`` 调用收归到单一注册点，
提供统一的状态查看、优雅收敛、异常回调接口。

**不**处理 QThread —— QThread 生命周期由
``core.safe_thread_runner.ThreadLifecycleMixin`` 管理。

使用方式::

    from core.thread_manager import thread_manager

    # 替代: threading.Thread(target=self._bg_flush, daemon=True).start()
    thread_manager.run(self._bg_flush, name="flush_pipeline")

    # 带位置参数:
    thread_manager.run(self._bg_refresh, args=(symbol,), name="refresh_source_status")

    # 带关键字参数 + 错误回调:
    thread_manager.run(
        self._bg_load, kwargs={"symbol": symbol},
        name="load_orderbook",
        error_cb=lambda exc: log.warning("load_orderbook failed: %s", exc),
    )

    # 优雅停止（应用退出时）:
    thread_manager.join_all(timeout=3.0)

注意事项
--------
* 所有通过 ``run()`` 启动的线程均为 ``daemon=True``，进程退出时自动回收。
* ``join_all()`` 超时后仍存活的线程会被记录警告，但**不**强杀（不调用 ``terminate()``），
  因为 daemon 线程会随进程结束自然消亡。
* ``run()`` 是线程安全的，可从任意线程调用。
"""

from __future__ import annotations

import logging
import threading
import time
import weakref
from collections import defaultdict
from collections.abc import Callable
from typing import Any

log = logging.getLogger(__name__)


class _ManagedThread(threading.Thread):
    """Daemon thread that auto-deregisters from :class:`ThreadManager` on completion."""

    def __init__(
        self,
        manager: ThreadManager,
        target: Callable[..., Any],
        name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        error_cb: Callable[[Exception], None] | None,
    ) -> None:
        super().__init__(target=self._run, daemon=True, name=name)
        self._manager = manager
        self._fn = target          # avoid overwriting threading.Thread._target
        self._fn_args = args       # avoid overwriting threading.Thread._args
        self._fn_kwargs = kwargs   # avoid overwriting threading.Thread._kwargs
        self._error_cb = error_cb

    def _run(self) -> None:
        try:
            self._fn(*self._fn_args, **self._fn_kwargs)
        except Exception as exc:
            log.exception("ThreadManager: unhandled error in thread %r", self.name)
            if self._error_cb is not None:
                try:
                    self._error_cb(exc)
                except Exception:
                    log.exception("ThreadManager: error_cb raised in thread %r", self.name)
        finally:
            self._manager._deregister(self)


class ThreadManager:
    """
    Centralized registry for daemon threads.

    同一 *name* 允许并发持有多个活跃线程（burst 场景），但推荐为每类后台任务
    选取有意义的 name 以便在 :meth:`status` 中快速定位问题。

    线程按 name 分桶（``defaultdict[str, set[WeakRef]]``），完成后自动清理。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # name → weak references to live _ManagedThread objects
        self._registry: dict[str, set[weakref.ref[_ManagedThread]]] = defaultdict(set)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        target: Callable[..., Any],
        name: str = "unnamed",
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        error_cb: Callable[[Exception], None] | None = None,
    ) -> _ManagedThread:
        """Start *target* in a registered daemon thread and return it.

        Parameters
        ----------
        target:
            Callable to invoke in the background thread.
        name:
            Logical name, e.g. ``"flush_pipeline"`` or ``"rollup_alerts_log"``.
        args:
            Positional arguments forwarded to *target*.
        kwargs:
            Keyword arguments forwarded to *target*.
        error_cb:
            Optional callable invoked (in the worker thread) when *target*
            raises an unhandled exception.  Receives the exception as the
            sole argument.
        """
        t = _ManagedThread(
            manager=self,
            target=target,
            name=name,
            args=args,
            kwargs=kwargs or {},
            error_cb=error_cb,
        )
        with self._lock:
            self._registry[name].add(weakref.ref(t))
        t.start()
        return t

    def active_count(self) -> int:
        """Number of currently running managed daemon threads."""
        return len(self._live_threads())

    def status(self) -> list[dict[str, object]]:
        """Snapshot of all active threads, suitable for /health endpoints."""
        return [
            {"name": t.name, "ident": t.ident, "daemon": t.daemon}
            for t in self._live_threads()
        ]

    def join_all(self, timeout: float = 5.0) -> None:
        """Best-effort graceful wait for all managed threads.

        Threads that outlive *timeout* are abandoned (daemon=True, they die
        with the process).  A WARNING is emitted listing their names.
        """
        deadline = time.monotonic() + timeout
        for t in self._live_threads():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            t.join(timeout=max(0.0, remaining))
        still_alive = [t.name for t in self._live_threads()]
        if still_alive:
            log.warning(
                "ThreadManager.join_all: %d thread(s) still alive after %.1fs: %s",
                len(still_alive),
                timeout,
                still_alive,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _deregister(self, thread: _ManagedThread) -> None:
        """Called by the thread itself on completion."""
        ref = weakref.ref(thread)
        with self._lock:
            bucket = self._registry.get(thread.name)
            if bucket:
                bucket.discard(ref)
                # prune any additional dead refs while we hold the lock
                bucket -= {r for r in bucket if r() is None}

    def _live_threads(self) -> list[_ManagedThread]:
        """Return a snapshot list of currently alive threads."""
        result: list[_ManagedThread] = []
        with self._lock:
            for bucket in self._registry.values():
                for ref in list(bucket):
                    t = ref()
                    if t is not None and t.is_alive():
                        result.append(t)
        return result


# ---------------------------------------------------------------------------
# Module-level singleton — import and use directly
# ---------------------------------------------------------------------------
thread_manager = ThreadManager()
