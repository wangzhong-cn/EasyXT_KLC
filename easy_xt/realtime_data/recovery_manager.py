import time
import uuid
from typing import Any, Optional


class ClientRecoveryManager:
    def __init__(self, ttl_seconds: int = 300, max_sessions: int = 10000):
        self.ttl_seconds = ttl_seconds
        self.max_sessions = max_sessions
        self.snapshots: dict[str, dict[str, Any]] = {}
        self.client_sessions: dict[str, str] = {}

    def create_session(self, client_id: str, client_info: Any) -> str:
        session_id = uuid.uuid4().hex
        self.client_sessions[client_id] = session_id
        self.snapshots[session_id] = self._build_snapshot(client_info)
        self._trim()
        return session_id

    def bind_client(self, client_id: str, session_id: str) -> None:
        self.client_sessions[client_id] = session_id

    def update_snapshot(self, client_id: str, client_info: Any) -> None:
        session_id = self.client_sessions.get(client_id)
        if not session_id:
            session_id = self.create_session(client_id, client_info)
        self.snapshots[session_id] = self._build_snapshot(client_info)

    def resume(self, session_id: str) -> Optional[dict[str, Any]]:
        snapshot = self.snapshots.get(session_id)
        if not snapshot:
            return None
        if self._is_expired(snapshot):
            self.snapshots.pop(session_id, None)
            return None
        snapshot["last_seen"] = time.time()
        return snapshot

    def remove_client(self, client_id: str) -> None:
        self.client_sessions.pop(client_id, None)

    def drop_session(self, session_id: str) -> None:
        self.snapshots.pop(session_id, None)

    def cleanup_expired(self) -> None:
        now = time.time()
        expired = [
            session_id
            for session_id, snapshot in self.snapshots.items()
            if now - snapshot.get("timestamp", now) > self.ttl_seconds
        ]
        for session_id in expired:
            self.snapshots.pop(session_id, None)

    def _build_snapshot(self, client_info: Any) -> dict[str, Any]:
        return {
            "subscriptions": list(client_info.subscriptions),
            "options": {
                "batch": client_info.batch_mode,
                "binary": client_info.prefer_binary,
                "max_batch_items": client_info.max_batch_items,
                "protocol": client_info.protocol,
                "compress": client_info.compress,
                "compress_threshold": client_info.compress_threshold
            },
            "last_event_ts": getattr(client_info, "last_event_ts", 0.0),
            "timestamp": time.time()
        }

    def _is_expired(self, snapshot: dict[str, Any]) -> bool:
        return (time.time() - snapshot.get("timestamp", time.time())) > self.ttl_seconds

    def _trim(self) -> None:
        if len(self.snapshots) <= self.max_sessions:
            return
        items = sorted(self.snapshots.items(), key=lambda item: item[1].get("timestamp", 0))
        overflow = len(self.snapshots) - self.max_sessions
        for session_id, _ in items[:overflow]:
            self.snapshots.pop(session_id, None)
