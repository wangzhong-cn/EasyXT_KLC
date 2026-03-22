from __future__ import annotations

import asyncio
import json

from gui_app.widgets.chart.ws_bridge import WsBridge, WsBridgeError


def test_call_sync_raises_when_not_connected() -> None:
    bridge = WsBridge()
    try:
        bridge.call_sync("chart.getDrawings", {}, timeout=0.1)
    except WsBridgeError as exc:
        assert "Not connected" in str(exc)
    else:
        raise AssertionError("expected WsBridgeError")


def test_notify_noop_when_not_connected() -> None:
    bridge = WsBridge()
    bridge.notify("chart.setData", {"bars": []})


def test_dispatch_rpc_result_sets_future() -> None:
    bridge = WsBridge()
    loop = asyncio.new_event_loop()
    try:
        bridge._loop = loop
        fut: asyncio.Future[object] = loop.create_future()
        bridge._pending[7] = fut
        bridge._dispatch(json.dumps({"jsonrpc": "2.0", "id": 7, "result": {"ok": True}}))
        assert fut.done() is True
        assert fut.result() == {"ok": True}
    finally:
        loop.close()


def test_dispatch_rpc_error_sets_exception() -> None:
    bridge = WsBridge()
    loop = asyncio.new_event_loop()
    try:
        bridge._loop = loop
        fut: asyncio.Future[object] = loop.create_future()
        bridge._pending[8] = fut
        bridge._dispatch(
            json.dumps(
                {"jsonrpc": "2.0", "id": 8, "error": {"code": -32000, "message": "boom"}}
            )
        )
        assert fut.done() is True
        try:
            fut.result()
        except WsBridgeError as exc:
            assert "boom" in str(exc)
        else:
            raise AssertionError("expected WsBridgeError")
    finally:
        loop.close()


def test_event_handler_registration_and_clear() -> None:
    bridge = WsBridge()

    def _handler(_: dict) -> None:
        return None

    bridge.on("chart.click", _handler)
    assert "chart.click" in bridge._handlers
    assert len(bridge._handlers["chart.click"]) == 1
    bridge.off("chart.click", _handler)
    assert bridge._handlers["chart.click"] == []
    bridge.on("chart.click", _handler)
    bridge.clear_handlers()
    assert bridge._handlers == {}
