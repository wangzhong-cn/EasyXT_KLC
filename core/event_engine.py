"""
EventEngine — 策略层事件分发引擎（Phase 3 提炼自 KLineChartWorkspace）

架构定位
--------
* 负责策略级事件（BarEvent / TickEvent / OrderEvent / SignalEvent）的
  **队列化、异步单线程分发**，提供与 Python ``asyncio`` 相同的"单线程执行—多订阅者"语义。
* 与现有 :mod:`core.signal_bus` 并存：
  - Qt 界面事件（跨 widget 通信）继续走 signal_bus；
  - 策略逻辑事件（数据→策略→信号→风控）走 EventEngine。
* 可通过 :meth:`EventEngine.make_signal_bus_bridge` 把某类事件同时转发到 signal_bus，
  实现"一次 ``put`` → 两路送达"，保证对旧代码的向后兼容。

典型用法
--------
::

    from core.event_engine import event_engine
    from core.event_types import BarEvent

    # 注册处理器（线程安全，可在启动前也可在运行中注册）
    event_engine.register("bar", my_strategy.on_bar)
    event_engine.start()

    # 在 QThread 或 easy_xt/realtime_data 中生产事件
    event_engine.put(BarEvent(symbol="000001.SZ", period="1m", close=10.5))

    # 应用退出时
    event_engine.stop()

线程安全说明
------------
* ``put()`` —— 任意线程调用安全（底层 ``queue.Queue.put_nowait``）。
* ``register()`` / ``unregister()`` —— 持锁操作，任意线程调用安全。
* **处理器（handler）在 EventEngine 后台线程顺序执行**，
  不准直接修改 Qt 控件；如需更新 UI，请用
  ``QTimer.singleShot(0, callback)`` 或 Qt 信号/槽。

队列满处理
----------
默认队列容量 1000。队列满时 ``put()`` 返回 ``False`` 并记录 WARNING，
事件被丢弃而不阻塞调用方线程（back-pressure 机制）。
"""

from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Callable
from typing import Any

log = logging.getLogger(__name__)

# 支持任何带 ``type`` 字段的事件 dataclass，也兼容旧式 ``Event``
_AnyEvent = Any


class EventEngine:
    """Queue-based, single-background-thread event dispatch engine.

    Parameters
    ----------
    maxsize:
        Maximum number of events that can be queued before ``put()`` starts
        dropping events.  Default is 1000.
    """

    def __init__(self, maxsize: int = 1000) -> None:
        self._queue: queue.Queue[_AnyEvent | None] = queue.Queue(maxsize=maxsize)
        self._handlers: dict[str, list[Callable[[_AnyEvent], None]]] = {}
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the dispatch loop in a background daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._dispatch_loop,
            daemon=True,
            name="EventEngine",
        )
        self._thread.start()
        log.info("EventEngine started")

    def stop(self, timeout: float = 3.0) -> None:
        """Drain the queue then stop the dispatch thread.

        In-flight events are dispatched before the thread exits.
        """
        if not self._running:
            return
        self._running = False
        # sentinel: wakes the blocked queue.get()
        self._queue.put_nowait(None)
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        log.info("EventEngine stopped")

    @property
    def is_running(self) -> bool:
        """``True`` iff the dispatch loop is active."""
        return self._running

    # ------------------------------------------------------------------
    # Handler registration
    # ------------------------------------------------------------------

    def register(self, event_type: str, handler: Callable[[_AnyEvent], None]) -> None:
        """Subscribe *handler* to events of *event_type*.

        Idempotent — registering the same handler twice has no effect.
        """
        with self._lock:
            handlers = self._handlers.setdefault(event_type, [])
            if handler not in handlers:
                handlers.append(handler)

    def unregister(self, event_type: str, handler: Callable[[_AnyEvent], None]) -> None:
        """Remove *handler* from the subscriber list for *event_type*."""
        with self._lock:
            handlers = self._handlers.get(event_type, [])
            if handler in handlers:
                handlers.remove(handler)

    # ------------------------------------------------------------------
    # Event production
    # ------------------------------------------------------------------

    def put(self, event: _AnyEvent) -> bool:
        """Enqueue *event* for dispatch.

        Returns
        -------
        bool
            ``True`` if the event was enqueued successfully; ``False`` if the
            queue was full and the event was dropped.
        """
        try:
            self._queue.put_nowait(event)
            return True
        except queue.Full:
            event_type = getattr(event, "type", "?")
            log.warning("EventEngine queue full; dropping %s event", event_type)
            return False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _dispatch_loop(self) -> None:
        """Main loop running in the background daemon thread."""
        while self._running:
            try:
                event = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if event is None:   # sentinel → stop
                self._queue.task_done()
                break
            self._dispatch(event)
            self._queue.task_done()

        # drain any remaining events on clean shutdown
        while not self._queue.empty():
            try:
                event = self._queue.get_nowait()
                if event is not None:
                    self._dispatch(event)
                self._queue.task_done()
            except queue.Empty:
                break

    def _dispatch(self, event: _AnyEvent) -> None:
        event_type: str | None = getattr(event, "type", None)
        if event_type is None:
            return
        with self._lock:
            handlers = list(self._handlers.get(event_type, []))
        for handler in handlers:
            try:
                handler(event)
            except Exception:
                log.exception(
                    "EventEngine: unhandled error in handler %r for event %s",
                    handler,
                    event_type,
                )

    # ------------------------------------------------------------------
    # Convenience: signal_bus bridge
    # ------------------------------------------------------------------

    @staticmethod
    def make_signal_bus_bridge(signal_bus_event: str) -> Callable[[_AnyEvent], None]:
        """Return a handler that forwards an EventEngine event to signal_bus.

        This lets existing code that subscribes to signal_bus continue to
        receive notifications while new code uses the typed EventEngine API.

        Usage::

            from core.event_engine import event_engine
            from core.events import Events

            bridge = event_engine.make_signal_bus_bridge(Events.DATA_UPDATED)
            event_engine.register("bar", bridge)

        The forwarded signal_bus payload is ``{"event": <EventType>}``.
        """
        from core.signal_bus import signal_bus  # lazy import to avoid circular dependency

        def _bridge(evt: _AnyEvent) -> None:
            signal_bus.emit(signal_bus_event, data=evt)

        return _bridge


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
event_engine = EventEngine()
