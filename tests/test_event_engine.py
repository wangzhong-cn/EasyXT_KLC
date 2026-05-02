"""
tests/test_event_engine.py — Phase 3 EventEngine / event_types 单元测试
"""
from __future__ import annotations

import threading
import time

import pytest

from core.event_engine import EventEngine
from core.event_types import BarEvent, Event, OrderEvent, SignalEvent, TickEvent

# ---------------------------------------------------------------------------
# EventEngine — 生命周期
# ---------------------------------------------------------------------------

def test_start_and_stop():
    """start() 后 is_running 为 True；stop() 后为 False。"""
    eng = EventEngine()
    assert not eng.is_running
    eng.start()
    assert eng.is_running
    eng.stop(timeout=1.0)
    assert not eng.is_running


def test_start_idempotent():
    """多次调用 start() 不应创建多个线程。"""
    eng = EventEngine()
    eng.start()
    eng.start()  # second call should be no-op
    assert eng.is_running
    eng.stop()


def test_stop_drains_queue():
    """stop() 应在退出前把队列中的剩余事件全部分发完。"""
    results: list[str] = []
    done = threading.Event()

    def handler(evt: BarEvent):
        results.append(evt.symbol)
        if len(results) == 3:
            done.set()

    eng = EventEngine()
    eng.start()
    eng.register("bar", handler)
    for sym in ("A", "B", "C"):
        eng.put(BarEvent(symbol=sym))
    eng.stop(timeout=2.0)
    assert len(results) == 3


# ---------------------------------------------------------------------------
# EventEngine — 事件分发
# ---------------------------------------------------------------------------

def test_handler_called_for_matching_type():
    """注册的 handler 应收到对应 type 的事件。"""
    received: list[BarEvent] = []
    done = threading.Event()

    def on_bar(evt: BarEvent):
        received.append(evt)
        done.set()

    eng = EventEngine()
    eng.register("bar", on_bar)
    eng.start()
    eng.put(BarEvent(symbol="000001.SZ", period="1m", close=10.5))
    done.wait(timeout=2.0)
    eng.stop()

    assert len(received) == 1
    assert received[0].symbol == "000001.SZ"
    assert received[0].close == pytest.approx(10.5)


def test_handler_not_called_for_other_type():
    """不同 type 的事件不应触发 handler。"""
    tick_received: list[TickEvent] = []

    def on_tick(evt: TickEvent):
        tick_received.append(evt)

    eng = EventEngine()
    eng.register("tick", on_tick)
    eng.start()
    done = threading.Event()

    def on_bar(_evt: BarEvent):
        done.set()

    eng.register("bar", on_bar)
    eng.put(BarEvent(symbol="000001.SZ"))
    done.wait(timeout=2.0)
    eng.stop()
    assert tick_received == []


def test_multiple_handlers_all_called():
    """同一 type 的多个 handler 都应被调用（EventEngine 顺序分发）。"""
    results: list[int] = []
    done = threading.Event()

    def h1(_evt: BarEvent) -> None:
        results.append(1)

    def h2(_evt: BarEvent) -> None:
        results.append(2)
        done.set()  # h2 last: signal completion

    eng = EventEngine()
    eng.register("bar", h1)
    eng.register("bar", h2)
    eng.start()
    eng.put(BarEvent())
    done.wait(timeout=2.0)
    eng.stop()
    assert sorted(results) == [1, 2]


def test_register_twice_is_idempotent():
    """同一 handler 注册两次，只应执行一次。"""
    count = 0
    done = threading.Event()

    def h(_evt):
        nonlocal count
        count += 1
        done.set()

    eng = EventEngine()
    eng.register("bar", h)
    eng.register("bar", h)  # duplicate
    eng.start()
    eng.put(BarEvent())
    done.wait(timeout=2.0)
    eng.stop()
    assert count == 1


def test_unregister_stops_delivery():
    """unregister 后对应 handler 不应再收到事件。"""
    received: list[int] = []

    def h(_evt):
        received.append(1)

    eng = EventEngine()
    eng.register("bar", h)
    eng.unregister("bar", h)
    eng.start()
    eng.put(BarEvent())
    time.sleep(0.1)  # give engine time to process
    eng.stop()
    assert received == []


def test_queue_full_drops_event(caplog):
    """队列满时 put() 应返回 False 并记录 WARNING，不阻塞。"""
    import logging

    eng = EventEngine(maxsize=1)
    # Fill the queue without starting the engine (no consumer)
    eng._queue.put_nowait(BarEvent())  # occupies the only slot
    with caplog.at_level(logging.WARNING, logger="core.event_engine"):
        result = eng.put(BarEvent(symbol="DROP_ME"))
    assert result is False
    assert "queue full" in caplog.text.lower()
    eng.start()
    eng.stop()


def test_handler_exception_does_not_crash_engine():
    """handler 抛异常不应导致引擎停止，后续事件仍应正常分发。"""
    results: list[str] = []
    done = threading.Event()

    def bad_handler(_evt):
        raise RuntimeError("boom")

    def good_handler(evt: BarEvent):
        results.append(evt.symbol)
        done.set()

    eng = EventEngine()
    eng.register("bar", bad_handler)
    eng.register("bar", good_handler)
    eng.start()
    eng.put(BarEvent(symbol="SURVIVOR"))
    done.wait(timeout=2.0)
    eng.stop()
    assert results == ["SURVIVOR"]


# ---------------------------------------------------------------------------
# EventEngine — signal_bus bridge
# ---------------------------------------------------------------------------

def test_make_signal_bus_bridge_forwards_to_signal_bus():
    """bridge handler 应把事件转发到 signal_bus。"""
    from core.events import Events
    from core.signal_bus import signal_bus

    forwarded: list[object] = []
    done = threading.Event()

    def sb_handler(data: object = None, **_kwargs: object) -> None:
        forwarded.append(data)
        done.set()

    signal_bus.subscribe(Events.DATA_UPDATED, sb_handler)
    bridge = EventEngine.make_signal_bus_bridge(Events.DATA_UPDATED)

    eng = EventEngine()
    eng.register("bar", bridge)
    eng.start()
    eng.put(BarEvent(symbol="BRIDGE_TEST"))
    done.wait(timeout=2.0)
    eng.stop()
    signal_bus.unsubscribe(Events.DATA_UPDATED, sb_handler)

    assert len(forwarded) == 1
    assert isinstance(forwarded[0], BarEvent)


# ---------------------------------------------------------------------------
# event_types dataclasses
# ---------------------------------------------------------------------------

def test_bar_event_defaults():
    evt = BarEvent()
    assert evt.type == "bar"
    assert evt.symbol == ""
    assert evt.close == pytest.approx(0.0)


def test_tick_event_fields():
    evt = TickEvent(symbol="600036.SH", price=12.34)
    assert evt.type == "tick"
    assert evt.price == pytest.approx(12.34)


def test_order_event_fields():
    evt = OrderEvent(order_id="ORD001", direction="buy", status="filled", filled_volume=100)
    assert evt.type == "order"
    assert evt.filled_volume == pytest.approx(100.0)


def test_signal_event_meta_default():
    evt = SignalEvent(strategy_id="grid_v1")
    assert evt.meta == {}


def test_generic_event():
    evt = Event(type="custom", data={"key": "value"})
    assert evt.type == "custom"
    assert evt.data["key"] == "value"


# ---------------------------------------------------------------------------
# Singleton smoke test
# ---------------------------------------------------------------------------

def test_module_singleton_importable():
    """module-level event_engine singleton should be importable."""
    from core.event_engine import event_engine as eng  # noqa: PLC0415
    assert eng is not None
    assert isinstance(eng, EventEngine)
