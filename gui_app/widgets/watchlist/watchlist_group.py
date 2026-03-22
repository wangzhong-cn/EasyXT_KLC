from __future__ import annotations

import gzip
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable


class WatchlistGroupManager:
    _action_log: list[dict] = []
    _subscribers: list[Callable[[dict], None]] = []
    _loaded_log_paths: set[str] = set()
    _ACTION_LOG_MAX_ENTRIES: int = 5_000
    _LOG_ROTATE_MAX_BYTES: int = 1_048_576
    _LOG_ROTATE_KEEP_FILES: int = 20

    def __init__(self, file_path: str | None = None) -> None:
        default_path = Path.home() / ".easyxt" / "watchlist_groups.json"
        self.file_path = Path(file_path) if file_path else default_path
        self.log_path = self.file_path.parent / "watchlist_group_actions.jsonl"
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._groups: dict[str, list[str]] = {"默认": []}
        self._load()
        self._load_action_log_from_disk()

    def group_names(self) -> list[str]:
        return list(self._groups.keys())

    def symbols(self, group: str) -> list[str]:
        return list(self._groups.get(group, []))

    def add_group(self, group: str, *, source: str = "") -> None:
        if not group:
            return
        if group not in self._groups:
            self._groups[group] = []
            self._save()
            self._record_action("add_group", group, {"size": 0, "source": source})

    def rename_group(self, old: str, new: str, *, source: str = "") -> None:
        if not old or not new or old not in self._groups or new in self._groups:
            return
        self._groups[new] = self._groups.pop(old)
        self._save()
        self._record_action("rename_group", new, {"old": old, "source": source})

    def remove_group(self, group: str, *, source: str = "") -> None:
        if not group or group == "默认":
            return
        if group in self._groups:
            size = len(self._groups.get(group, []))
            self._groups.pop(group)
            self._save()
            self._record_action("remove_group", group, {"size": size, "source": source})

    def set_symbols(self, group: str, symbols: list[str], *, source: str = "") -> None:
        if not group:
            return
        self._groups[group] = [s for s in symbols if s]
        self._save()
        self._record_action("set_symbols", group, {"size": len(self._groups[group]), "source": source})

    @classmethod
    def subscribe_actions(cls, callback: Callable[[dict], None]) -> None:
        if callback not in cls._subscribers:
            cls._subscribers.append(callback)

    @classmethod
    def unsubscribe_actions(cls, callback: Callable[[dict], None]) -> None:
        if callback in cls._subscribers:
            cls._subscribers.remove(callback)

    @classmethod
    def get_action_log(cls) -> list[dict]:
        return list(cls._action_log)

    def clear_action_log(self) -> int:
        removed = len(self.__class__._action_log)
        self.__class__._action_log.clear()
        try:
            if self.log_path.exists():
                self.log_path.unlink()
            pattern = f"{self.log_path.stem}.*{self.log_path.suffix}"
            for old in self.log_path.parent.glob(pattern):
                if old == self.log_path:
                    continue
                old.unlink()
            pattern_gz = f"{self.log_path.stem}.*{self.log_path.suffix}.gz"
            for old in self.log_path.parent.glob(pattern_gz):
                old.unlink()
        except Exception:
            pass
        self.__class__._loaded_log_paths.discard(str(self.log_path))
        return removed

    def _load(self) -> None:
        if not self.file_path.exists():
            return
        try:
            data = json.loads(self.file_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                parsed = {str(k): [str(x) for x in (v or [])] for k, v in data.items()}
                if parsed:
                    self._groups = parsed
                if "默认" not in self._groups:
                    self._groups["默认"] = []
        except Exception:
            pass

    def _save(self) -> None:
        try:
            self.file_path.write_text(
                json.dumps(self._groups, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _record_action(self, action: str, group: str, payload: dict) -> None:
        entry = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "action": action,
            "group": group,
            "payload": dict(payload or {}),
        }
        self.__class__._action_log.append(entry)
        self._trim_action_log()
        try:
            self._rotate_log_if_needed()
            with self.log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            pass
        for cb in list(self.__class__._subscribers):
            try:
                cb(entry)
            except Exception:
                pass

    def _rotate_log_if_needed(self) -> None:
        if not self.log_path.exists():
            return
        stat = self.log_path.stat()
        now = datetime.now()
        mtime = datetime.fromtimestamp(stat.st_mtime)
        over_size = stat.st_size >= self._LOG_ROTATE_MAX_BYTES
        over_day = mtime.date() != now.date()
        if not over_size and not over_day:
            return
        suffix = now.strftime("%Y%m%d_%H%M%S")
        rotated = self.log_path.with_name(f"{self.log_path.stem}.{suffix}{self.log_path.suffix}")
        counter = 1
        while rotated.exists():
            rotated = self.log_path.with_name(
                f"{self.log_path.stem}.{suffix}_{counter}{self.log_path.suffix}"
            )
            counter += 1
        self.log_path.rename(rotated)
        self._compress_rotated_log(rotated)
        self._prune_rotated_logs()

    def _prune_rotated_logs(self) -> None:
        pattern = f"{self.log_path.stem}.*{self.log_path.suffix}"
        pattern_gz = f"{self.log_path.stem}.*{self.log_path.suffix}.gz"
        archives = [p for p in self.log_path.parent.glob(pattern) if p != self.log_path]
        archives.extend(self.log_path.parent.glob(pattern_gz))
        archives.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        keep = max(0, int(self._LOG_ROTATE_KEEP_FILES))
        for old in archives[keep:]:
            try:
                old.unlink()
            except Exception:
                pass

    def archive_uncompressed_logs(self, older_than_days: int = 0) -> int:
        cutoff = datetime.now() - timedelta(days=max(0, int(older_than_days)))
        count = 0
        pattern = f"{self.log_path.stem}.*{self.log_path.suffix}"
        for fp in self.log_path.parent.glob(pattern):
            if fp == self.log_path or fp.name.endswith(".gz"):
                continue
            try:
                mtime = datetime.fromtimestamp(fp.stat().st_mtime)
                if older_than_days > 0 and mtime > cutoff:
                    continue
                self._compress_rotated_log(fp)
                count += 1
            except Exception:
                continue
        self._prune_rotated_logs()
        return count

    def _load_action_log_from_disk(self) -> None:
        path_key = str(self.log_path)
        if path_key in self.__class__._loaded_log_paths:
            return
        entries: list[dict] = []
        files = self._list_log_files()
        for fp in files:
            try:
                if fp.name.endswith(".gz"):
                    with gzip.open(fp, mode="rt", encoding="utf-8") as gzf:
                        content = gzf.read()
                else:
                    content = fp.read_text(encoding="utf-8")
                for line in content.splitlines():
                    text = line.strip()
                    if not text:
                        continue
                    obj = json.loads(text)
                    if isinstance(obj, dict):
                        entries.append(obj)
            except Exception:
                continue
        if entries:
            self.__class__._action_log.extend(entries)
            self._trim_action_log()
        self.__class__._loaded_log_paths.add(path_key)

    def _list_log_files(self) -> list[Path]:
        files: list[Path] = []
        pattern = f"{self.log_path.stem}.*{self.log_path.suffix}"
        files.extend([p for p in self.log_path.parent.glob(pattern) if p != self.log_path])
        pattern_gz = f"{self.log_path.stem}.*{self.log_path.suffix}.gz"
        files.extend(self.log_path.parent.glob(pattern_gz))
        if self.log_path.exists():
            files.append(self.log_path)
        files.sort(key=lambda p: p.stat().st_mtime)
        return files

    def _trim_action_log(self) -> None:
        cap = max(100, int(self._ACTION_LOG_MAX_ENTRIES))
        if len(self.__class__._action_log) > cap:
            self.__class__._action_log[:] = self.__class__._action_log[-cap:]

    def _compress_rotated_log(self, rotated: Path) -> None:
        try:
            gz_path = rotated.with_name(f"{rotated.name}.gz")
            with rotated.open("rb") as src, gzip.open(gz_path, "wb") as dst:
                dst.write(src.read())
            rotated.unlink()
        except Exception:
            pass
