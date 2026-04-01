"""
WsBridge — 本地 WebSocket JSON-RPC 2.0 服务端
用于 NativeLwcChartAdapter (Python) ↔ chart-bridge.js (JS) 的双向通信。

设计原则：
  - asyncio 事件循环运行在独立的 daemon 线程中
  - notify()   : 单向推送，非阻塞，适合 setData / updateBar 等高频操作
  - call_sync(): 阻塞调用（带超时），适合 getDrawings / takeScreenshot 等低频操作
  - JS → Python 事件通过 QTimer.singleShot(0, fn) 路由回 Qt 主线程
  - stop()     : 优雅关闭，清理所有资源
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
from typing import Any, Callable

import websockets
import websockets.exceptions

log = logging.getLogger(__name__)


class WsBridgeError(Exception):
    """WS 调用失败时抛出（超时、未连接、RPC 错误）"""


class WsBridge:
    """
    本地 WebSocket 服务端，监听 127.0.0.1 随机端口。

    生命周期::

        bridge = WsBridge()
        port = bridge.start()               # 启动后台线程，返回实际端口
        ok   = bridge.wait_connect(5.0)     # 等待 JS 客户端握手
        bridge.notify("chart.setData", {"bars": [...]})   # 非阻塞推送
        result = bridge.call_sync("chart.getDrawings", {}) # 阻塞调用
        bridge.on("chart.click", my_handler)               # 注册事件
        bridge.stop()                       # 清理
    """

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server: Any | None = None         # websockets.WebSocketServer
        self._ws: Any | None = None              # websockets.WebSocketServerProtocol
        self._port: int = 0

        self._server_ready = threading.Event()
        self._client_connected: dict[str, threading.Event] = {}  # chart_id → connected event
        self._thread: threading.Thread | None = None
        self._stop_event: asyncio.Event | None = None  # created inside event loop

        self._pending: dict[int, "asyncio.Future[Any]"] = {}
        self._msg_id: int = 0
        self._handlers: dict[str, list["Callable[..., Any]"]] = {}
        self._client_lock = threading.Lock()

        # ── 多客户端支持 ──
        self._clients: dict[str, Any] = {}  # chart_id → websockets.WebSocketServerProtocol

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> int:
        """启动 WebSocket 服务后台线程。返回实际监听端口（OS 分配）。"""
        self._thread = threading.Thread(
            target=self._thread_main, name="WsBridge", daemon=True
        )
        self._thread.start()
        self._server_ready.wait(timeout=5.0)
        return self._port

    def wait_connect(self, timeout: float = 5.0) -> bool:
        """阻塞等待 JS 客户端连接（chart_id="default"），返回是否在超时内连上。"""
        return self.wait_connect_for("default", timeout)

    def wait_connect_for(self, chart_id: str, timeout: float = 5.0) -> bool:
        """阻塞等待指定 chart_id 的 JS 客户端连接。"""
        evt = self._client_connected.get(chart_id)
        if evt is None:
            evt = threading.Event()
            self._client_connected[chart_id] = evt
        return evt.wait(timeout=timeout)

    def stop(self) -> None:
        """优雅关闭服务端，释放端口和线程。"""
        loop = self._loop
        if loop is not None and not loop.is_closed():
            if self._stop_event is not None:
                # 通知事件循环退出 _serve_forever；async-with 会等 Server._close() 完成
                try:
                    loop.call_soon_threadsafe(self._stop_event.set)
                except RuntimeError:
                    pass  # loop already closed between check and call
            else:
                try:
                    loop.call_soon_threadsafe(loop.stop)
                except RuntimeError:
                    pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        with self._client_lock:
            self._client_connected.clear()
            self._clients.clear()
        self._server_ready.clear()

    @property
    def port(self) -> int:
        return self._port

    @property
    def is_connected(self) -> bool:
        with self._client_lock:
            return "default" in self._clients

    def is_connected_to(self, chart_id: str) -> bool:
        with self._client_lock:
            return chart_id in self._clients

    # ── Sending ───────────────────────────────────────────────────────────────

    def notify(self, method: str, params: "dict[str, Any]", chart_id: str = "default") -> None:
        """
        发送 JSON-RPC 通知（无需响应）。非阻塞，可在 Qt 主线程安全调用。
        若未连接则静默忽略。
        """
        if not self._loop:
            return
        with self._client_lock:
            ws = self._clients.get(chart_id)
        if not ws:
            return
        msg = json.dumps({"jsonrpc": "2.0", "method": method, "params": params})
        asyncio.run_coroutine_threadsafe(self._send_raw(msg, ws), self._loop)

    def call_sync(
        self,
        method: str,
        params: "dict[str, Any]",
        timeout: float = 3.0,
        chart_id: str = "default",
    ) -> Any:
        """
        发送 JSON-RPC 请求并阻塞等待响应。仅用于低频操作（截图、获取画线等）。
        超时或失败时抛出 WsBridgeError。
        """
        if not self._loop:
            raise WsBridgeError("Not connected")
        with self._client_lock:
            ws = self._clients.get(chart_id)
        if not ws:
            raise WsBridgeError(f"Not connected to chart_id={chart_id!r}")
        future = asyncio.run_coroutine_threadsafe(
            self._rpc_call(method, params, timeout, ws), self._loop
        )
        try:
            return future.result(timeout=timeout + 1.0)
        except WsBridgeError:
            raise
        except Exception as exc:
            raise WsBridgeError(str(exc)) from exc

    # ── Events ────────────────────────────────────────────────────────────────

    def on(self, event: str, handler: "Callable[..., Any]") -> None:
        """注册 JS→Python 事件处理器。handler 在 Qt 主线程中调用。"""
        self._handlers.setdefault(event, []).append(handler)

    def off(self, event: str, handler: "Callable[..., Any] | None" = None) -> None:
        """注销事件处理器。handler=None 时清除该事件的全部处理器。"""
        if handler is None:
            self._handlers.pop(event, None)
        else:
            self._handlers[event] = [
                h for h in self._handlers.get(event, []) if h is not handler
            ]

    def clear_handlers(self) -> None:
        """清除全部事件处理器（adapter destroy 时调用）。"""
        self._handlers.clear()

    # ── Internal ──────────────────────────────────────────────────────────────

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._serve_forever())
        except Exception:
            log.exception("WsBridge: server thread exited with error")
        finally:
            # 清理所有未完成的 pending futures
            for _mid, fut in list(self._pending.items()):
                if not fut.done():
                    fut.cancel()
            self._pending.clear()
            try:
                self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            except Exception:
                pass
            self._loop.close()

    async def _serve_forever(self) -> None:
        self._stop_event = asyncio.Event()
        async with websockets.serve(
            self._handle_client, "127.0.0.1", 0
        ) as server:
            self._server = server
            self._port = server.sockets[0].getsockname()[1]
            log.debug("WsBridge: listening on 127.0.0.1:%d", self._port)
            self._server_ready.set()
            await self._stop_event.wait()  # 等待 stop() 触发优雅退出
        # async-with __aexit__ 已完整等待 Server._close() 就绪

    async def _handle_client(
        self, ws: Any  # websockets.WebSocketServerProtocol
    ) -> None:
        chart_id = "default"
        try:
            async for raw in ws:
                if chart_id == "default":
                    try:
                        msg = json.loads(raw)
                        cid = msg.get("params", {}).get("chart_id")
                        if cid and isinstance(cid, str):
                            chart_id = cid
                    except (json.JSONDecodeError, Exception):
                        pass
                with self._client_lock:
                    self._clients[chart_id] = ws
                    evt = self._client_connected.get(chart_id)
                    if evt is None:
                        evt = threading.Event()
                        self._client_connected[chart_id] = evt
                    evt.set()
                log.debug("WsBridge: client connected chart_id=%s from %s", chart_id, ws.remote_address)
                self._dispatch(raw, ws)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            with self._client_lock:
                if self._clients.get(chart_id) is ws:
                    self._clients.pop(chart_id, None)
                evt = self._client_connected.get(chart_id)
                if evt:
                    evt.clear()
            log.debug("WsBridge: client disconnected (chart_id=%s)", chart_id)

    def _dispatch(self, raw: str, ws: Any = None) -> None:
        """解析收到的 JSON-RPC 消息，路由到对应的 pending future 或事件处理器。"""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("WsBridge: invalid JSON (first 120 chars): %s", raw[:120])
            return

        msg_id = msg.get("id")

        # ── 响应（对应我们发出的 call_sync）
        if msg_id is not None and msg_id in self._pending:
            fut = self._pending.pop(msg_id)
            if not fut.done():
                if "error" in msg:
                    fut.set_exception(
                        WsBridgeError(msg["error"].get("message", "RPC error"))
                    )
                else:
                    fut.set_result(msg.get("result"))
            return

        # ── 通知/事件（JS → Python）
        method = msg.get("method")
        if not method:
            return
        params = msg.get("params", {})
        handlers = list(self._handlers.get(method, []))
        if not handlers:
            return

        # 切回 Qt 主线程执行
        try:
            from PyQt5.QtCore import QTimer
        except ImportError:
            for h in handlers:
                try:
                    h(params)
                except Exception:
                    log.exception("WsBridge: handler error for '%s'", method)
            return

        for h in handlers:
            def _invoke(fn=h, p=params) -> None:  # type: ignore[assignment]
                try:
                    fn(p)
                except Exception:
                    log.exception("WsBridge: handler error for '%s'", method)

            QTimer.singleShot(0, _invoke)

    async def _send_raw(self, msg: str, ws: Any) -> None:
        try:
            await ws.send(msg)
        except Exception:
            log.debug("WsBridge: send failed (client may have disconnected)")

    async def _rpc_call(
        self, method: str, params: "dict[str, Any]", timeout: float, ws: Any
    ) -> Any:
        if self._loop is None:
            raise WsBridgeError("Not connected")
        self._msg_id += 1
        msg_id = self._msg_id
        fut: "asyncio.Future[Any]" = self._loop.create_future()
        self._pending[msg_id] = fut

        payload = json.dumps(
            {"jsonrpc": "2.0", "id": msg_id, "method": method, "params": params}
        )
        try:
            await ws.send(payload)
        except Exception as exc:
            self._pending.pop(msg_id, None)
            raise WsBridgeError(f"send failed: {exc}") from exc

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError as exc:
            self._pending.pop(msg_id, None)
            raise WsBridgeError(f"Timeout waiting for '{method}'") from exc
