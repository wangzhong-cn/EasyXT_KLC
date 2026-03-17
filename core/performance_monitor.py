#!/usr/bin/env python3
"""
性能监控器 - 统一打点与慢查询日志

功能：
1. 启动性能打点
2. 标签页切换打点
3. 慢查询日志（>500ms）
4. 性能统计面板

使用示例：
    from core.performance_monitor import perf_monitor, PerfEvent

    # 记录事件
    perf_monitor.record(PerfEvent.TAB_SWITCH, duration_ms)

    # 获取统计
    stats = perf_monitor.get_stats()
"""

import logging
import time
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

_SH = ZoneInfo('Asia/Shanghai')
from enum import Enum


class PerfEvent(Enum):
    """性能事件类型"""

    APP_STARTUP = "app_startup"
    TAB_SWITCH = "tab_switch"
    DATA_LOAD = "data_load"
    QUERY_EXECUTE = "query_execute"
    CHART_RENDER = "chart_render"
    REALTIME_UPDATE = "realtime_update"


class PerformanceMonitor:
    """
    性能监控器 - 单例模式

    功能：
    1. 记录性能事件
    2. 统计慢查询
    3. 生成性能报告
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._logger = logging.getLogger(__name__)

        # 配置
        self._slow_query_threshold_ms = 500  # 慢查询阈值
        self._max_events = 1000  # 最大记录事件数

        # 事件记录
        self._events: list[dict] = []
        self._event_counts: dict[str, int] = defaultdict(int)
        self._event_durations: dict[str, list[float]] = defaultdict(list)

        # 当前计时
        self._timers: dict[str, float] = {}

        self._logger.info("PerformanceMonitor initialized")

    def start_timer(self, name: str):
        """开始计时"""
        self._timers[name] = time.perf_counter()

    def stop_timer(self, name: str, event_type: Optional[PerfEvent] = None) -> float:
        """停止计时并记录"""
        if name not in self._timers:
            return 0

        start = self._timers[name]
        duration_ms = (time.perf_counter() - start) * 1000

        if event_type:
            self.record(event_type, duration_ms)

        del self._timers[name]
        return duration_ms

    def record(self, event_type: PerfEvent, duration_ms: float, metadata: Optional[dict] = None):
        """记录性能事件"""
        event = {
            "type": event_type.value,
            "duration_ms": duration_ms,
            "timestamp": datetime.now(tz=_SH).isoformat(),
            "metadata": metadata or {},
        }

        # 添加到列表
        self._events.append(event)

        # 限制列表大小
        if len(self._events) > self._max_events:
            self._events = self._events[-self._max_events :]

        # 统计
        self._event_counts[event_type.value] += 1
        self._event_durations[event_type.value].append(duration_ms)

        # 慢查询日志
        if duration_ms > self._slow_query_threshold_ms:
            self._logger.warning(
                f"[SLOW] {event_type.value}: {duration_ms:.1f}ms "
                f"(threshold: {self._slow_query_threshold_ms}ms)"
            )

    def get_stats(self) -> dict:
        """获取性能统计"""
        stats = {
            "total_events": len(self._events),
            "event_counts": dict(self._event_counts),
            "event_stats": {},
        }

        # 计算每个事件类型的统计
        for event_type, durations in self._event_durations.items():
            if durations:
                sorted_durations = sorted(durations)
                n = len(sorted_durations)
                stats["event_stats"][event_type] = {
                    "count": n,
                    "avg_ms": sum(durations) / n,
                    "min_ms": min(durations),
                    "max_ms": max(durations),
                    "p50_ms": sorted_durations[n // 2],
                    "p95_ms": sorted_durations[int(n * 0.95)],
                    "p99_ms": sorted_durations[int(n * 0.99)] if n >= 100 else sorted_durations[-1],
                }

        return stats

    def get_slow_queries(self, limit: int = 10) -> list[dict]:
        """获取最慢的查询"""
        slow_events = [e for e in self._events if e["duration_ms"] > self._slow_query_threshold_ms]
        return sorted(slow_events, key=lambda x: x["duration_ms"], reverse=True)[:limit]

    def get_recent_events(self, limit: int = 20) -> list[dict]:
        """获取最近的事件"""
        return self._events[-limit:]

    def clear(self):
        """清空记录"""
        self._events.clear()
        self._event_counts.clear()
        self._event_durations.clear()
        self._logger.info("PerformanceMonitor cleared")

    def set_threshold(self, threshold_ms: int):
        """设置慢查询阈值"""
        self._slow_query_threshold_ms = threshold_ms

    def get_summary_text(self) -> str:
        """获取摘要文本"""
        stats = self.get_stats()
        lines = ["性能统计:"]

        for event_type, event_stat in stats.get("event_stats", {}).items():
            lines.append(
                f"  {event_type}: "
                f"次数={event_stat['count']} "
                f"平均={event_stat['avg_ms']:.0f}ms "
                f"P95={event_stat['p95_ms']:.0f}ms"
            )

        if not stats.get("event_stats"):
            lines.append("  (暂无数据)")

        return "\n".join(lines)


# 全局单例
perf_monitor = PerformanceMonitor()


class PerfTimer:
    """性能计时器上下文管理器"""

    def __init__(self, name: str, event_type: Optional[PerfEvent] = None, metadata: Optional[dict] = None):
        self.name = name
        self.event_type = event_type
        self.metadata = metadata
        self.start_time: Optional[float] = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time is None:
            return False
        duration_ms = (time.perf_counter() - self.start_time) * 1000
        if self.event_type:
            perf_monitor.record(self.event_type, duration_ms, self.metadata)
        return False


def log_performance(func):
    """性能日志装饰器"""

    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        duration_ms = (time.perf_counter() - start) * 1000
        perf_monitor.record(PerfEvent.QUERY_EXECUTE, duration_ms, {"function": func.__name__})
        return result

    return wrapper
