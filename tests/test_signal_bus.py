import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.signal_bus import SignalBus


def test_signal_bus_emit():
    bus = SignalBus()
    received = []

    def handler(x):
        received.append(x)

    bus.subscribe("test", handler)
    bus.emit("test", x=1)
    assert received == [1]


def test_signal_bus_unsubscribe_removes_event():
    bus = SignalBus()
    called = []

    def handler():
        called.append(True)

    bus.subscribe("event", handler)
    bus.unsubscribe("event", handler)
    bus.emit("event")
    assert called == []
    assert "event" not in bus._subscribers


def test_signal_bus_request_collects_results():
    bus = SignalBus()

    def handler_a(x):
        return x + 1

    def handler_b(x):
        return x * 2

    bus.subscribe("req", handler_a)
    bus.subscribe("req", handler_b)
    results = bus.request("req", x=3)
    assert results == [4, 6]


def test_signal_bus_request_handles_exception():
    bus = SignalBus()

    def handler_ok():
        return "ok"

    def handler_fail():
        raise ValueError("boom")

    bus.subscribe("req", handler_fail)
    bus.subscribe("req", handler_ok)
    results = bus.request("req")
    assert results == ["ok"]


def test_emit_swallows_handler_exception():
    """emit() 的 except 块（lines 28-29）吞掉 handler 抛出的异常。"""
    bus = SignalBus()

    def bad_handler(**kwargs):
        raise RuntimeError("handler exploded")

    bus.subscribe("ev", bad_handler)
    bus.emit("ev", x=1)  # must not raise
