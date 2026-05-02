"""
ChartRouter — 多图表路由中心

管理 NativeLwcChartAdapter 实例注册，支持通过 chart_id 路由 RPC 调用。

单例模式：整个进程共享一个 Router。

使用示例::

    from gui_app.widgets.chart.chart_router import chart_router

    # 适配器注册（adapter 初始化时）
    chart_router.register("main", adapter)

    # Python → JS：通过 chart_id 路由
    chart_router.notify("main", "chart.setData", {"bars": [...], "chart_id": "main"})
    result = chart_router.call_sync("main", "chart.getDrawings", {})

    # 适配器注销（adapter destroy 时）
    chart_router.unregister("main")
"""
from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from gui_app.widgets.chart.chart_adapter import NativeLwcChartAdapter

log = logging.getLogger(__name__)


class ChartRouter:
    """
    全局多图表路由中心。
    每个 NativeLwcChartAdapter 实例拥有唯一 chart_id 并注册到此处。
    """

    _instance: "ChartRouter | None" = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._adapters: dict[str, "NativeLwcChartAdapter"] = {}
        self._register_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "ChartRouter":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def register(self, chart_id: str, adapter: "NativeLwcChartAdapter") -> None:
        with self._register_lock:
            if chart_id in self._adapters:
                log.warning("ChartRouter: chart_id=%r 已注册，将被替换", chart_id)
            self._adapters[chart_id] = adapter
            log.info("ChartRouter: 注册 chart_id=%r", chart_id)

    def unregister(self, chart_id: str) -> None:
        with self._register_lock:
            self._adapters.pop(chart_id, None)
            log.info("ChartRouter: 注销 chart_id=%r", chart_id)

    def get_adapter(self, chart_id: str) -> "NativeLwcChartAdapter | None":
        return self._adapters.get(chart_id)

    def notify(self, chart_id: str, method: str, params: dict[str, Any]) -> None:
        adapter = self._adapters.get(chart_id)
        if adapter is None:
            log.warning("ChartRouter: chart_id=%r 未注册，跳过 notify %s", chart_id, method)
            return
        params_with_id = dict(params, chart_id=chart_id)
        adapter._bridge_notify(method, params_with_id)

    def call_sync(
        self, chart_id: str, method: str, params: dict[str, Any], timeout: float = 3.0
    ) -> Any:
        adapter = self._adapters.get(chart_id)
        if adapter is None:
            log.warning("ChartRouter: chart_id=%r 未注册，跳过 call_sync %s", chart_id, method)
            raise RuntimeError(f"chart_id={chart_id!r} 未注册")
        params_with_id = dict(params, chart_id=chart_id)
        return adapter._bridge_call_sync(method, params_with_id, timeout)

    @property
    def registered_ids(self) -> list[str]:
        return list(self._adapters.keys())

    def clear(self) -> None:
        with self._register_lock:
            self._adapters.clear()


chart_router = ChartRouter.get_instance()
