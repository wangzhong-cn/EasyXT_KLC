"""
xtquant C 扩展线程亲和性隔离层。

仓库内已有多处注释和回归修复表明：xtquant / xtdata 不能从 GUI 主线程、
threading.Thread 或 ThreadPoolExecutor 工作线程调用；唯一稳定路径是
在 QThread.run() 中直接执行。否则会触发：
    Assertion failed: u < 1000000, file bsonobj.cpp, line 1388

因此这里提供一个单例 QThread 执行器：所有 xtdata API 调用通过
xtdata_submit() 投递到该 QThread 的 run() 循环中顺序执行。

使用方式:
    from core.xtdata_lock import xtdata_submit
    result = xtdata_submit(xtdata.get_full_tick, ["000001.SZ"])
"""
import queue
import threading
from typing import Any, Callable

from PyQt5.QtCore import QThread


xtdata_call_lock = threading.RLock()


_Task = tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any], threading.Event, dict[str, Any]]
_request_queue: "queue.Queue[_Task]" = queue.Queue()


class _XtdataExecutorThread(QThread):
    """专用 xtdata 执行线程：所有 xtdata 调用都在 run() 主循环中执行。"""

    def __init__(self) -> None:
        super().__init__()
        self._python_thread_id: int | None = None

    def run(self) -> None:
        self._python_thread_id = threading.get_ident()
        while True:
            func, args, kwargs, event, box = _request_queue.get()
            try:
                with xtdata_call_lock:
                    box["value"] = func(*args, **kwargs)
            except BaseException as e:
                box["error"] = e
            finally:
                event.set()

    def in_executor_thread(self) -> bool:
        return self._python_thread_id == threading.get_ident()


_worker_thread = _XtdataExecutorThread()
_worker_thread.setObjectName("xtdata-qthread-executor")
_worker_thread.start()


def xtdata_submit(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """在单例 QThread 中执行 *func*，阻塞调用方直到完成。

    若当前已经位于该执行线程（嵌套调用），则直接执行以避免死锁。
    """
    if _worker_thread.in_executor_thread():
        return func(*args, **kwargs)
    box: dict[str, Any] = {}
    event = threading.Event()
    _request_queue.put((func, args, kwargs, event, box))
    event.wait()
    err = box.get("error")
    if err is not None:
        raise err
    return box.get("value")


# 向后兼容：旧代码仍可直接 import xtdata_call_lock。
