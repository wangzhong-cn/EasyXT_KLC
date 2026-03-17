from __future__ import annotations

import threading
import time
from copy import deepcopy
from typing import Any


class AccountRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._accounts: dict[str, dict[str, Any]] = {}

    def register_account(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = str(payload.get("account_id", "")).strip()
        if not account_id:
            raise ValueError("account_id 不能为空")
        with self._lock:
            merged = dict(self._accounts.get(account_id, {}))
            merged.update(payload)
            merged["account_id"] = account_id
            merged.setdefault("enabled", True)
            merged["updated_at_ms"] = int(time.time() * 1000)
            if "created_at_ms" not in merged:
                merged["created_at_ms"] = merged["updated_at_ms"]
            self._accounts[account_id] = merged
            return deepcopy(merged)

    def get_account(self, account_id: str) -> dict[str, Any] | None:
        key = str(account_id).strip()
        if not key:
            return None
        with self._lock:
            item = self._accounts.get(key)
            return deepcopy(item) if item is not None else None

    def list_accounts(self) -> list[dict[str, Any]]:
        with self._lock:
            return [deepcopy(v) for v in self._accounts.values()]

    def delete_account(self, account_id: str) -> bool:
        key = str(account_id).strip()
        if not key:
            return False
        with self._lock:
            if key not in self._accounts:
                return False
            del self._accounts[key]
            return True


account_registry = AccountRegistry()
