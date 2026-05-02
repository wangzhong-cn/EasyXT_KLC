import importlib
import threading
from typing import Any, Optional

from core.xtdata_lock import xtdata_call_lock as _xtdata_call_lock  # noqa: F401 — 向后兼容
from core.xtdata_lock import xtdata_submit as _xtdata_submit


class XtQuantBroker:
    def __init__(self):
        self._xtdata = None

    def _ensure_xtdata(self):
        if self._xtdata is not None:
            return self._xtdata
        self._xtdata = importlib.import_module("xtquant.xtdata")
        return self._xtdata

    def call_xtdata(self, method_name: str, *args, **kwargs):
        def _work():
            xtdata = self._ensure_xtdata()
            method = getattr(xtdata, method_name)
            return method(*args, **kwargs)
        return _xtdata_submit(_work)

    def get_full_tick(self, codes: list[str]):
        return self.call_xtdata("get_full_tick", codes)

    def get_market_data(self, **kwargs):
        return self.call_xtdata("get_market_data", **kwargs)


_broker_instance: Optional[XtQuantBroker] = None
_broker_lock = threading.Lock()


def get_xtquant_broker() -> XtQuantBroker:
    global _broker_instance
    if _broker_instance is not None:
        return _broker_instance
    with _broker_lock:
        if _broker_instance is None:
            _broker_instance = XtQuantBroker()
        return _broker_instance
