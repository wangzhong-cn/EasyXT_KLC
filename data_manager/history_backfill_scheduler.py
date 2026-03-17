#!/usr/bin/env python3
import json
import logging
import os
import queue
import threading
import time
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


@dataclass(order=True)
class BackfillTask:
    priority: int
    created_at: float
    key: str = field(compare=False)
    payload: dict[str, Any] = field(compare=False, default_factory=dict)
    retry_count: int = field(compare=False, default=0)  # 重试次数
    last_retry_time: float = field(compare=False, default=0.0)  # 最后重试时间


class HistoryBackfillScheduler:
    def __init__(self, worker: Callable[[dict[str, Any]], bool], max_queue_size: int = 512):
        self._worker = worker
        self._queue: queue.PriorityQueue[BackfillTask] = queue.PriorityQueue(maxsize=max_queue_size)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._pending_keys: set[str] = set()
        self._logger = logging.getLogger(__name__)
        self._max_retries = 5  # 最大重试次数
        self._base_backoff = 2  # 基础回退时间（秒）
        self._max_backoff = 300  # 最大回退时间（秒），5分钟
        self._timers: dict[str, threading.Timer] = {}
        self._timer_lock = threading.Lock()

        # 持久化死信队列文件（JSONL，可通过环境变量 EASYXT_DEAD_LETTER_PATH 覆盖）
        _dlp = os.environ.get("EASYXT_DEAD_LETTER_PATH", "").strip()
        self._dead_letter_path: Path = (
            Path(_dlp) if _dlp else Path(__file__).parent / "backfill_dead_letter.jsonl"
        )

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="HistoryBackfillScheduler", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 1.0) -> None:
        self._stop_event.set()
        with self._timer_lock:
            for timer in self._timers.values():
                try:
                    timer.cancel()
                except Exception:
                    pass
            self._timers.clear()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def schedule(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        period: str,
        priority: int = 100,
        reason: str = "manual",
    ) -> bool:
        stock_code = str(stock_code or "").strip()
        period = str(period or "1d").strip()
        if not stock_code or not start_date or not end_date:
            return False

        key = f"{stock_code}|{period}|{start_date}|{end_date}"
        with self._lock:
            if key in self._pending_keys:
                return False
            self._pending_keys.add(key)

        task = BackfillTask(
            priority=int(priority),
            created_at=time.monotonic(),
            key=key,
            payload={
                "stock_code": stock_code,
                "start_date": start_date,
                "end_date": end_date,
                "period": period,
                "reason": reason,
            },
        )

        try:
            self._queue.put_nowait(task)
            return True
        except queue.Full:
            with self._lock:
                self._pending_keys.discard(key)
            self._logger.warning("补数任务队列已满，写入死信队列: %s", key)
            self._write_dead_letter(task, "queue_full")
            return False

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                task = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                success = self._worker(task.payload)
                if not success:
                    # 如果工作函数返回失败，尝试进行回退重试
                    self._handle_task_failure(task)
                else:
                    # 成功则清理任务
                    with self._lock:
                        self._pending_keys.discard(task.key)
            except Exception:
                self._logger.exception("补数任务执行失败: %s", task.key)
                # 发生异常也进行回退重试
                self._handle_task_failure(task)
            finally:
                self._queue.task_done()

    def _handle_task_failure(self, task: BackfillTask):
        """处理任务失败，实现指数回退重试"""
        if task.retry_count >= self._max_retries:
            self._logger.warning("补数任务超过最大重试次数，写入死信队列: %s", task.key)
            with self._lock:
                self._pending_keys.discard(task.key)
            self._write_dead_letter(task, "max_retries_exhausted")
            return

        # 计算下次重试时间（指数回退）
        backoff_time = min(self._base_backoff * (2**task.retry_count), self._max_backoff)
        jitter = random.uniform(0.8, 1.2)
        backoff_time = min(backoff_time * jitter, self._max_backoff)

        # 更新任务信息
        new_task = BackfillTask(
            priority=task.priority + 10,  # 增加优先级（数字越大优先级越低）
            created_at=task.created_at,
            key=task.key,
            payload=task.payload,
            retry_count=task.retry_count + 1,
            last_retry_time=time.time(),
        )

        self._logger.warning(
            "补数任务失败，将在 %.1f 秒后重试 (第 %d/%d 次): %s",
            backoff_time,
            new_task.retry_count,
            self._max_retries,
            task.key,
        )

        # 使用定时器来实现延迟重试
        timer = threading.Timer(backoff_time, self._retry_task, args=[new_task])
        with self._timer_lock:
            existing = self._timers.pop(task.key, None)
            if existing:
                try:
                    existing.cancel()
                except Exception:
                    pass
            self._timers[task.key] = timer
        timer.start()

    def _retry_task(self, task: BackfillTask):
        """重试任务"""
        try:
            # 将任务重新加入队列
            self._queue.put_nowait(task)
        except queue.Full:
            self._logger.warning("重试队列已满，写入死信队列: %s", task.key)
            with self._lock:
                self._pending_keys.discard(task.key)
            self._write_dead_letter(task, "retry_queue_full")
        finally:
            with self._timer_lock:
                self._timers.pop(task.key, None)

    # ── 死信队列持久化 ────────────────────────────────────────────────────────

    def _write_dead_letter(self, task: BackfillTask, reason: str) -> None:
        """将失败任务追加写入死信队列文件（JSONL 格式）。"""
        record = {
            "key": task.key,
            "payload": task.payload,
            "retry_count": task.retry_count,
            "reason": reason,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self._dead_letter_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._dead_letter_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception:
            self._logger.exception("死信队列写入失败，任务永久丢失: %s", task.key)

    def replay_dead_letters(self, limit: int = 200) -> dict[str, int]:
        """从死信队列文件中重新调度任务，最多处理 limit 条。

        成功重入队列的条目从文件中删除；队列仍满时停止处理。
        返回 {"replayed": n, "remaining": m}。
        """
        if not self._dead_letter_path.exists():
            return {"replayed": 0, "remaining": 0}

        try:
            with open(self._dead_letter_path, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f if ln.strip()]
        except Exception:
            self._logger.exception("读取死信队列文件失败")
            return {"replayed": 0, "remaining": 0}

        replayed = 0
        remaining_lines: list[str] = []

        for line in lines:
            if replayed >= limit:
                remaining_lines.append(line)
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                self._logger.warning("死信队列行解析失败，跳过: %.120s", line)
                continue

            payload = record.get("payload", {})
            ok = self.schedule(
                stock_code=payload.get("stock_code", ""),
                start_date=payload.get("start_date", ""),
                end_date=payload.get("end_date", ""),
                period=payload.get("period", "1d"),
                priority=50,  # 重放任务给予较高优先级
                reason="dead_letter_replay",
            )
            if ok:
                replayed += 1
            else:
                # schedule() 返回 False 表示队列仍满或重复 key
                remaining_lines.append(line)

        # 原子写回剩余条目
        try:
            tmp = self._dead_letter_path.with_suffix(".tmp")
            if remaining_lines:
                with open(tmp, "w", encoding="utf-8") as f:
                    f.write("\n".join(remaining_lines) + "\n")
                tmp.replace(self._dead_letter_path)
            else:
                self._dead_letter_path.unlink(missing_ok=True)
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
        except Exception:
            self._logger.exception("更新死信队列文件失败")

        return {"replayed": replayed, "remaining": len(remaining_lines)}

    def get_dead_letter_stats(self) -> dict[str, int]:
        """返回死信队列统计信息：总条目数及各原因计数。"""
        if not self._dead_letter_path.exists():
            return {"total": 0}
        counts: dict[str, int] = {}
        total = 0
        try:
            with open(self._dead_letter_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    total += 1
                    try:
                        rec = json.loads(line)
                        reason = rec.get("reason", "unknown")
                        counts[reason] = counts.get(reason, 0) + 1
                    except json.JSONDecodeError:
                        counts["parse_error"] = counts.get("parse_error", 0) + 1
        except Exception:
            self._logger.exception("读取死信队列统计失败")
        return {"total": total, **counts}
