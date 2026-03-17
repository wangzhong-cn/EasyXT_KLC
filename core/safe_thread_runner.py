"""
ThreadLifecycleMixin — QThread 退出安全协议的代码化实现

将 ``closeEvent`` 模板（分层超时 + 强杀 + 结构化事件上报）沉淀为可复用 Mixin，
避免各 QWidget 子类手写偏差。

使用方式::

    class MyWidget(ThreadLifecycleMixin, QWidget):
        def __init__(self):
            super().__init__()
            # 注册网络型线程（超时 1s，超时后 terminate + 上报）
            self._register_network_thread("_realtime_connect_thread", wait_ms=1000)
            # 注册普通型线程（超时 200ms，静默）
            self._register_thread("_chart_load_thread")
            self._register_thread("_data_process_thread")
            self._register_thread("_quote_worker")

        def closeEvent(self, event):
            self._stop_all_threads()   # 一行完成所有线程清理
            super().closeEvent(event)

线程分类规则（见 docs/05_thread_exit_safety_spec.md）：
- 网络型（wait_ms ≥ 1000）：TDX / xtquant 网络连接，超时后 terminate + THREAD_FORCED_TERMINATE 事件
- 普通型（wait_ms = 200）  ：DuckDB 查询、pandas 计算，超时静默
"""

from __future__ import annotations

import logging
from typing import List, Optional, Tuple

log = logging.getLogger(__name__)


class ThreadLifecycleMixin:
    """
    为 ``QWidget`` 子类提供标准化 ``QThread`` 退出协议。

    退出协议（``_stop_all_threads()``）：

    1. **网络型线程**（逐一处理）：
       ``requestInterruption`` → ``quit`` → ``wait(N ms)`` →
       超时则 ``terminate`` + ``wait(500)`` + 发出 ``THREAD_FORCED_TERMINATE`` 事件

    2. **普通型线程**（批量处理）：
       先全部 ``requestInterruption + quit``，再统一 ``wait(200 ms)``，
       避免串行累积超时（N × 200 ms）

    不自动 override ``closeEvent``，由子类主动调用 ``_stop_all_threads()``，
    保持对 ``closeEvent`` 逻辑的显式控制。
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # (attr_name, wait_ms)
        self._network_thread_attrs: List[Tuple[str, int]] = []
        self._regular_thread_attrs: List[str] = []

    def _register_network_thread(self, attr_name: str, wait_ms: int = 1000) -> None:
        """注册网络型线程属性名。

        超时后执行 ``terminate()`` + 发出 ``Events.THREAD_FORCED_TERMINATE``。

        Args:
            attr_name: 线程在 ``self`` 上的属性名，例如 ``"_realtime_connect_thread"``
            wait_ms:   等待超时（毫秒）。网络型建议 ≥ 1000。
        """
        self._network_thread_attrs.append((attr_name, wait_ms))

    def _register_thread(self, attr_name: str) -> None:
        """注册普通型线程属性名（200 ms wait，超时静默）。

        Args:
            attr_name: 线程在 ``self`` 上的属性名
        """
        self._regular_thread_attrs.append(attr_name)

    def _stop_all_threads(self) -> None:
        """标准退出协议。

        在 ``closeEvent`` 的 ``try`` 块内最开始调用，在 ``super().closeEvent(event)`` 之前。

        等价于手写 closeEvent 模板（见 docs/05_thread_exit_safety_spec.md 第四节），
        但无需各处重复实现。
        """
        logger = getattr(self, "_logger", log)
        component = self.__class__.__name__

        # ── 步骤 1：网络型线程，逐一处理（可能需要 terminate） ──────────────
        for attr_name, wait_ms in self._network_thread_attrs:
            try:
                from PyQt5.QtCore import QThread  # noqa: PLC0415 — lazy import
                t: Optional[QThread] = getattr(self, attr_name, None)
                if t is None or not t.isRunning():
                    continue
                t.requestInterruption()
                t.quit()
                if not t.wait(wait_ms):
                    logger.warning(
                        "closeEvent: %s.%s 未在 %dms 内退出，强制终止",
                        component,
                        attr_name,
                        wait_ms,
                    )
                    t.terminate()
                    t.wait(500)
                    try:
                        from core.events import Events  # noqa: PLC0415
                        from core.signal_bus import signal_bus  # noqa: PLC0415
                        signal_bus.emit(
                            Events.THREAD_FORCED_TERMINATE,
                            thread_name=attr_name,
                            component=component,
                        )
                    except Exception:
                        pass
            except Exception:
                logger.exception("_stop_all_threads: 处理网络型线程 %s 时出错", attr_name)

        # ── 步骤 2：普通型线程，先全部发中断，再统一 wait（避免串行累积） ──
        running = []
        for attr_name in self._regular_thread_attrs:
            try:
                from PyQt5.QtCore import QThread  # noqa: PLC0415
                t = getattr(self, attr_name, None)
                if t is not None and t.isRunning():
                    t.requestInterruption()
                    t.quit()
                    running.append(t)
            except Exception:
                logger.exception("_stop_all_threads: 发送中断到 %s 时出错", attr_name)

        for t in running:
            try:
                wait_func = getattr(t, "wait", None)
                if callable(wait_func):
                    wait_func(200)
            except Exception:
                pass
