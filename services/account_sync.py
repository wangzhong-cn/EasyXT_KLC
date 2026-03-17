from __future__ import annotations

import threading
import time
from typing import Any

from core.account_registry import account_registry


def sync_accounts_once() -> dict[str, Any]:
    accounts = account_registry.list_accounts()
    return {
        "synced": len(accounts),
        "accounts": accounts,
        "source": "registry",
        "synced_at_ms": int(time.time() * 1000),
    }


class AccountSyncService:
    def __init__(self, interval_sec: float = 30.0) -> None:
        self.interval_sec = max(float(interval_sec), 1.0)
        self._running = False
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._last_result: dict[str, Any] | None = None

    def sync_once(self) -> dict[str, Any]:
        self._last_result = sync_accounts_once()
        return self._last_result

    def start(self) -> bool:
        if self._running:
            return False
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        return True

    def stop(self) -> bool:
        if not self._running:
            return False
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1.0)
        self._running = False
        return True

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self.sync_once()
            self._stop_event.wait(self.interval_sec)
