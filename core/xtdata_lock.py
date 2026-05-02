"""
xtquant C 扩展线程亲和性隔离层。

仓库内已有多处注释和回归修复表明：xtquant / xtdata 不能从 GUI 主线程、
threading.Thread 或 ThreadPoolExecutor 工作线程调用；唯一稳定路径是
在 QThread.run() 中直接执行。否则会触发：
    Assertion failed: u < 1000000, file bsonobj.cpp, line 1388

因此这里优先提供一个单例 QThread 执行器：所有 xtdata API 调用通过
xtdata_submit() 投递到该执行器的 run() 循环中顺序执行。

补充说明：
        - 生产/Qt 环境下，稳定路径仍然首选 QThread。
        - 无 PyQt5 的 hermetic / 数据层环境下，会降级到普通后台线程，
            主要用于保证核心模块可导入、非 GUI pytest 可运行；这不等价于
            宣称 plain-thread xtquant 调用在生产中与 QThread 同等安全。

使用方式:
    from core.xtdata_lock import xtdata_submit
    result = xtdata_submit(xtdata.get_full_tick, ["000001.SZ"])
"""
import queue
import threading
import traceback
from typing import Any, Callable

try:
    from PyQt5.QtCore import QThread
except ImportError:
    QThread = None


xtdata_call_lock = threading.RLock()
QT_THREAD_EXECUTOR_AVAILABLE = QThread is not None


_Task = tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any], threading.Event, dict[str, Any]]
_request_queue: "queue.Queue[_Task]" = queue.Queue()


def _executor_loop(worker: Any) -> None:
    worker._python_thread_id = threading.get_ident()
    while True:
        func, args, kwargs, event, box = _request_queue.get()
        try:
            with xtdata_call_lock:
                box["value"] = func(*args, **kwargs)
        except BaseException as e:
            box["error"] = e
        finally:
            event.set()


if QThread is not None:

    class _XtdataExecutorThread(QThread):
        """专用 xtdata 执行线程：优先使用 QThread 保持既有稳定路径。"""

        def __init__(self) -> None:
            super().__init__()
            self._python_thread_id: int | None = None

        def run(self) -> None:
            _executor_loop(self)

        def in_executor_thread(self) -> bool:
            return self._python_thread_id == threading.get_ident()

else:

    class _XtdataExecutorThread(threading.Thread):
        """无 Qt 环境下的降级执行线程。

        注意：仓库对 xtquant/xtdata 的稳定路径仍然首选 QThread。
        这里的 plain-thread fallback 主要用于无 PyQt5 的 hermetic / 数据层环境，
        以便核心模块可以被导入并运行不触达 Qt 的测试/辅助路径。
        """

        def __init__(self) -> None:
            super().__init__(daemon=True, name="xtdata-thread-executor")
            self._python_thread_id: int | None = None

        def setObjectName(self, name: str) -> None:
            self.name = name

        def run(self) -> None:
            _executor_loop(self)

        def in_executor_thread(self) -> bool:
            return self._python_thread_id == threading.get_ident()


_worker_thread = _XtdataExecutorThread()
_worker_thread.setObjectName("xtdata-qthread-executor")
_worker_thread.start()


def xtdata_submit(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """在单例执行线程中执行 *func*，阻塞调用方直到完成。

    若当前已经位于该执行线程（嵌套调用），则直接执行以避免死锁。
    """
    if _worker_thread.in_executor_thread():
        return func(*args, **kwargs)
    box: dict[str, Any] = {}
    event = threading.Event()
    _request_queue.put((func, args, kwargs, event, box))
    if not event.wait(timeout=10):
        caller_stack = ''.join(traceback.format_stack()[:-1])
        print(
            f"\n[xtdata_submit TIMEOUT] func={func.__name__!r}\n"
            f"Caller stack:\n{caller_stack}",
            flush=True,
        )
        raise TimeoutError(
            f"xtdata_submit 超时（10s）：{func.__name__!r} 未完成，"
            "_XtdataExecutorThread 可能已终止或卡死"
        )
    err = box.get("error")
    if err is not None:
        raise err
    return box.get("value")


# 向后兼容：旧代码仍可直接 import xtdata_call_lock。
