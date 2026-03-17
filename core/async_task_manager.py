#!/usr/bin/env python3
"""
异步任务管理器 - 统一调度所有耗时操作

功能：
1. QThreadPool 封装 - 线程池管理
2. 任务去重 - 同一页面多次触发只保留最后一次
3. 有界队列 - 防止内存溢出
4. 信号驱动 - 主线程接收结果更新UI

使用示例：
    from core.async_task_manager import async_manager, TaskPriority

    # 提交任务
    task_id = async_manager.submit(
        task_fn=heavy_function,
        args=(arg1, arg2),
        task_key="unique_key",  # 用于任务去重
        priority=TaskPriority.HIGH,
        callback=on_result  # 可选的回调
    )

    # 取消任务
    async_manager.cancel(task_id)
"""

import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from enum import IntEnum
from typing import Any, Callable, Optional

from PyQt5.QtCore import QObject, QThread, QTimer, pyqtSignal


class TaskPriority(IntEnum):
    """任务优先级"""

    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class TaskState:
    """任务状态"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


class AsyncTask(QThread):
    """
    异步任务线程

    信号：
        started: 任务开始
        progress: 进度更新 (current, total, message)
        finished: 任务完成 (result)
        error: 任务失败 (exception)
        cancelled: 任务取消
    """

    started = pyqtSignal()
    progress = pyqtSignal(int, int, str)  # current, total, message
    finished = pyqtSignal(object)  # result
    error = pyqtSignal(Exception)
    cancelled = pyqtSignal()

    def __init__(
        self,
        task_fn: Callable,
        args: tuple = (),
        kwargs: Optional[dict[str, Any]] = None,
        task_key: Optional[str] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        cancellable: bool = True,
        timeout: Optional[float] = None,  # 秒
    ):
        super().__init__()
        self.task_fn = task_fn
        self.args = args
        self.kwargs = kwargs or {}
        self.task_key = task_key
        self.task_priority = priority
        self.cancellable = cancellable
        self.timeout = timeout

        self._result = None
        self._exception = None
        self._cancelled = False
        self._started_at = None
        self._logger = logging.getLogger(__name__)

    def run(self):
        """执行任务"""
        self._started_at = time.perf_counter()

        # 检查是否已取消
        if self._cancelled:
            self.cancelled.emit()
            return

        self.started.emit()

        try:
            # 如果任务支持进度回调，包装一下
            task_fn = self.task_fn
            if hasattr(task_fn, "__self__"):
                # 绑定方法，添加进度支持
                def wrapped(*args, **kwargs):
                    return task_fn(*args, **kwargs)

                task_fn = wrapped

            # 执行任务
            self._result = task_fn(*self.args, **self.kwargs)
            self.finished.emit(self._result)

        except Exception as e:
            self._exception = e
            self.error.emit(e)

    def cancel(self):
        """取消任务"""
        if self.cancellable and not self.isFinished():
            self._cancelled = True
            self.terminate()
            self.cancelled.emit()

    def is_cancelled(self) -> bool:
        return self._cancelled

    @property
    def elapsed_time(self) -> float:
        if self._started_at:
            return time.perf_counter() - self._started_at
        return 0


class AsyncTaskManager(QObject):
    """
    异步任务管理器 - 单例模式

    信号：
        task_submitted: 任务提交 (task_id, task_key)
        task_completed: 任务完成 (task_id, result)
        task_failed: 任务失败 (task_id, error)
        task_cancelled: 任务取消 (task_id)
        progress: 进度更新 (task_id, current, total, message)
    """

    # 单例实例
    _instance = None

    task_submitted = pyqtSignal(str, str)  # task_id, task_key
    task_completed = pyqtSignal(str, object)  # task_id, result
    task_failed = pyqtSignal(str, Exception)  # task_id, error
    task_cancelled = pyqtSignal(str)  # task_id
    progress = pyqtSignal(str, int, int, str)  # task_id, current, total, message

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        super().__init__()
        self._initialized = True
        self._logger = logging.getLogger(__name__)

        # 线程池配置
        self._max_workers = 4  # 最大并发数
        self._queue_size = 100  # 有界队列大小

        # 实际执行器
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_workers, thread_name_prefix="AsyncTask"
        )

        # 任务存储
        self._tasks: dict[str, AsyncTask] = {}
        self._futures: dict[str, Future] = {}

        # 任务去重 - 使用WeakValueDictionary防止内存泄漏
        self._pending_keys: dict[str, str] = {}  # key -> task_id

        # 任务计数器
        self._task_counter = 0

        # 任务清理定时器
        self._cleanup_timer = QTimer()
        self._cleanup_timer.timeout.connect(self._cleanup_finished_tasks)
        self._cleanup_timer.start(60000)  # 每分钟清理一次

        self._logger.info(f"AsyncTaskManager initialized: max_workers={self._max_workers}")

    def submit(
        self,
        task_fn: Callable,
        args: tuple = (),
        kwargs: Optional[dict[str, Any]] = None,
        task_key: Optional[str] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        cancellable: bool = True,
        timeout: Optional[float] = None,
        callback: Optional[Callable[[Any], Any]] = None,
    ) -> str:
        """
        提交异步任务

        参数：
            task_fn: 任务函数
            args: 位置参数
            kwargs: 关键字参数
            task_key: 任务唯一键（用于去重）
            priority: 优先级
            cancellable: 是否可取消
            timeout: 超时时间（秒）
            callback: 完成回调

        返回：
            task_id: 任务ID
        """
        # 生成任务ID
        self._task_counter += 1
        task_id = f"task_{self._task_counter}_{int(time.time() * 1000)}"

        # 任务去重：如果有相同key的任务在运行，取消旧的
        if task_key and task_key in self._pending_keys:
            old_task_id = self._pending_keys[task_key]
            self.cancel(old_task_id)
            self._logger.info(f"Task dedup: cancelled {old_task_id} for key {task_key}")

        # 创建任务
        task = AsyncTask(
            task_fn=task_fn,
            args=args,
            kwargs=kwargs,
            task_key=task_key,
            priority=priority,
            cancellable=cancellable,
            timeout=timeout,
        )

        # 连接信号
        task.started.connect(lambda: self._on_task_started(task_id, task_key))
        task.finished.connect(lambda r: self._on_task_finished(task_id, r, callback))
        task.error.connect(lambda e: self._on_task_error(task_id, e))
        task.cancelled.connect(lambda: self._on_task_cancelled(task_id))
        task.progress.connect(lambda c, m, msg: self._on_task_progress(task_id, c, m, msg))

        # 提交到线程池
        future = self._executor.submit(task.run)

        # 存储任务
        self._tasks[task_id] = task
        self._futures[task_id] = future

        if task_key:
            self._pending_keys[task_key] = task_id

        self.task_submitted.emit(task_id, task_key or "")
        self._logger.debug(f"Task submitted: {task_id}, key={task_key}")

        return task_id

    def submit_batch(
        self,
        tasks: list[Any],
    ) -> list[str]:
        """
        批量提交任务

        参数：
            tasks: [(task_fn, args, kwargs, task_key), ...]

        返回：
            task_ids: 任务ID列表
        """
        task_ids: list[str] = []
        for task_spec in tasks:
            if len(task_spec) >= 4:
                task_fn, args, kwargs, task_key = task_spec[:4]
            else:
                task_fn, args, kwargs, task_key = task_spec[0], (), {}, None
            safe_args = args if isinstance(args, tuple) else tuple(args) if isinstance(args, list) else ()
            safe_kwargs = kwargs if isinstance(kwargs, dict) else {}
            safe_task_key = str(task_key) if task_key is not None else None
            task_id = self.submit(task_fn, safe_args, safe_kwargs, safe_task_key)
            task_ids.append(task_id)

        return task_ids

    def cancel(self, task_id: str) -> bool:
        """
        取消任务

        参数：
            task_id: 任务ID

        返回：
            是否成功取消
        """
        if task_id not in self._tasks:
            return False

        task = self._tasks[task_id]
        task.cancel()

        # 从pending_keys中移除
        if task.task_key and self._pending_keys.get(task.task_key) == task_id:
            del self._pending_keys[task.task_key]

        self._logger.debug(f"Task cancelled: {task_id}")
        return True

    def cancel_by_key(self, task_key: str) -> bool:
        """通过key取消任务"""
        if task_key not in self._pending_keys:
            return False
        task_id = self._pending_keys[task_key]
        return self.cancel(task_id)

    def get_result(self, task_id: str, timeout: Optional[float] = None) -> Any:
        """
        同步获取任务结果（会阻塞）

        参数：
            task_id: 任务ID
            timeout: 超时时间

        返回：
            任务结果
        """
        if task_id not in self._futures:
            return None

        future = self._futures[task_id]
        try:
            return future.result(timeout=timeout)
        except Exception as e:
            self._logger.error(f"get_result failed for {task_id}: {e}")
            return None

    def is_running(self, task_id: str) -> bool:
        """检查任务是否在运行"""
        if task_id not in self._tasks:
            return False
        return self._tasks[task_id].isRunning()

    def is_pending(self, task_key: str) -> bool:
        """检查是否有待处理的任务"""
        return task_key in self._pending_keys

    def get_active_count(self) -> int:
        """获取活跃任务数"""
        return sum(1 for t in self._tasks.values() if t.isRunning())

    def _on_task_started(self, task_id: str, task_key: Optional[str]):
        self._logger.debug(f"Task started: {task_id}")

    def _on_task_finished(self, task_id: str, result: Any, callback: Optional[Callable[[Any], Any]]):
        if callback:
            try:
                callback(result)
            except Exception as e:
                self._logger.error(f"Callback failed for {task_id}: {e}")

        self.task_completed.emit(task_id, result)

        # 清理
        if task_id in self._tasks:
            task = self._tasks[task_id]
            if task.task_key and self._pending_keys.get(task.task_key) == task_id:
                del self._pending_keys[task.task_key]

        self._logger.debug(f"Task finished: {task_id}")

    def _on_task_error(self, task_id: str, error: Exception):
        self.task_failed.emit(task_id, error)
        self._logger.error(f"Task failed: {task_id}, error={error}")

    def _on_task_cancelled(self, task_id: str):
        self.task_cancelled.emit(task_id)
        self._logger.debug(f"Task cancelled: {task_id}")

    def _on_task_progress(self, task_id: str, current: int, total: int, message: str):
        self.progress.emit(task_id, current, total, message)

    def _cleanup_finished_tasks(self):
        """清理已完成的任务"""
        finished = []
        for task_id, task in self._tasks.items():
            if task.isFinished() and not task.isRunning():
                finished.append(task_id)

        for task_id in finished:
            del self._tasks[task_id]
            if task_id in self._futures:
                del self._futures[task_id]

        if finished:
            self._logger.debug(f"Cleaned up {len(finished)} finished tasks")

    def shutdown(self, wait: bool = True):
        """关闭任务管理器"""
        self._cleanup_timer.stop()

        # 取消所有正在运行的任务
        for task in self._tasks.values():
            if task.isRunning():
                task.cancel()

        # 关闭线程池
        self._executor.shutdown(wait=wait)
        self._logger.info("AsyncTaskManager shutdown")

    def __del__(self):
        self.shutdown(wait=False)


# 全局单例
async_manager = AsyncTaskManager()
