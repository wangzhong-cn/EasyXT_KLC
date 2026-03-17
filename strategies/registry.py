"""
策略注册中心（Phase 1）

解决的问题：
  - 无法枚举当前所有运行中的策略
  - 策略与账户的绑定关系不透明
  - 无版本化快照：无法回溯某策略 T 时刻的参数

功能：
  - register/unregister：注册/注销策略实例
  - get/list: 查询单个 / 枚举所有
  - snapshot：将当前所有策略参数快照写入 DuckDB（按需调用）

使用示例::

    from strategies.registry import strategy_registry

    # 注册策略（在策略启动时调用）
    strategy_registry.register(
        strategy_id="ma_cross_v1",
        strategy_obj=my_strategy,
        account_id="88001234",
        params={"fast": 5, "slow": 20},
        tags=["trend", "daily"],
    )

    # 查询所有运行中的策略
    for info in strategy_registry.list_all():
        print(info["strategy_id"], info["status"])

    # 注销策略（在停止时调用）
    strategy_registry.unregister("ma_cross_v1")

    # 参数版本快照（按需调用，写入 DuckDB）
    strategy_registry.snapshot_to_db()
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# DuckDB 建表 DDL（幂等）
_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS strategy_snapshots (
    snapshot_id   VARCHAR PRIMARY KEY,
    strategy_id   VARCHAR NOT NULL,
    account_id    VARCHAR NOT NULL DEFAULT '',
    status        VARCHAR NOT NULL DEFAULT 'running',   -- running | stopped | error
    params_json   VARCHAR NOT NULL DEFAULT '{}',        -- 策略参数 JSON
    tags_json     VARCHAR NOT NULL DEFAULT '[]',        -- 标签 JSON 数组
    snapshotted_at BIGINT NOT NULL,                     -- UTC 毫秒时间戳
    host          VARCHAR NOT NULL DEFAULT ''
);
"""

# 状态机：合法转换表
# unknown/legacy from-states 不在表中时保持许可（向后兼容）
_STATUS_TRANSITIONS: Dict[str, frozenset] = {
    "created":  frozenset({"running", "stopped"}),
    "running":  frozenset({"paused", "stopped", "error"}),
    "paused":   frozenset({"running", "stopped"}),
    "stopped":  frozenset(),                          # 终态，不可转换
    "error":    frozenset({"running", "stopped"}),
}


@dataclass
class StrategyInfo:
    """已注册策略的元信息。"""

    strategy_id: str
    strategy_obj: Optional[Any]          # 策略实例（可为 None，仅作元信息登记）
    account_id: str = ""
    params: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    status: str = "running"              # created | running | paused | stopped | error
    registered_at: float = field(default_factory=time.time)


class StrategyRegistry:
    """
    线程安全的策略注册中心。

    支持：
      - 注册/注销策略实例
      - 查询单个策略 / 枚举所有策略
      - 热更新策略状态 / 参数
      - 将当前快照持久化到 DuckDB（审计溯源用）
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # strategy_id -> StrategyInfo
        self._registry: Dict[str, StrategyInfo] = {}

    # ------------------------------------------------------------------
    # Register / Unregister
    # ------------------------------------------------------------------

    def register(
        self,
        strategy_id: str,
        strategy_obj: Optional[Any] = None,
        account_id: str = "",
        params: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
    ) -> StrategyInfo:
        """
        注册策略。若已存在同 ID 则覆盖（允许重启重注册）。

        Args:
            strategy_id: 策略唯一 ID（建议 "<name>_v<version>"）
            strategy_obj: 策略实例（BaseStrategy 子类），可为 None
            account_id: 绑定的交易账户 ID
            params: 策略运行参数字典（用于版本快照）
            tags: 标签列表（如 ["trend", "daily"]）

        Returns:
            注册后的 StrategyInfo 对象。
        """
        info = StrategyInfo(
            strategy_id=strategy_id,
            strategy_obj=strategy_obj,
            account_id=account_id,
            params=params or {},
            tags=tags or [],
            status="running",
        )
        with self._lock:
            existed = strategy_id in self._registry
            self._registry[strategy_id] = info

        action = "更新注册" if existed else "新注册"
        log.info(
            "策略注册中心 [%s]: %s | account=%s | tags=%s | params_keys=%s",
            action, strategy_id, account_id, tags, list((params or {}).keys()),
        )
        return info

    def unregister(self, strategy_id: str, status: str = "stopped") -> bool:
        """
        注销策略（不从注册表删除，只更新状态为 stopped/error）。

        保留历史记录以供审计溯源，仅改变 status 字段。

        Args:
            strategy_id: 策略 ID
            status: 注销后的状态，通常为 "stopped" 或 "error"

        Returns:
            True 表示找到并更新，False 表示未找到。
        """
        with self._lock:
            if strategy_id not in self._registry:
                log.warning("策略注册中心: 注销失败，未找到策略 %s", strategy_id)
                return False
            self._registry[strategy_id].status = status

        log.info("策略注册中心: 策略 %s 已注销 status=%s", strategy_id, status)
        return True

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get(self, strategy_id: str) -> Optional[StrategyInfo]:
        """获取单个策略信息，未找到返回 None。"""
        with self._lock:
            return self._registry.get(strategy_id)

    def list_all(self) -> List[Dict[str, Any]]:
        """
        枚举所有已注册策略（包含已停止的）。

        Returns:
            列表，每项为字典：strategy_id, account_id, status, tags, params_summary, registered_at
        """
        with self._lock:
            result = []
            for sid, info in self._registry.items():
                result.append({
                    "strategy_id": sid,
                    "account_id": info.account_id,
                    "status": info.status,
                    "tags": info.tags,
                    "params_keys": list(info.params.keys()),
                    "registered_at": info.registered_at,
                    "has_instance": info.strategy_obj is not None,
                })
        return result

    def list_running(self) -> List[StrategyInfo]:
        """仅返回状态为 running 的策略。"""
        with self._lock:
            return [info for info in self._registry.values() if info.status == "running"]

    def update_status(self, strategy_id: str, new_status: str):
        """
        状态机约束更新策略状态。

        Returns:
            None            — 策略未找到
            (True, "")      — 更新成功
            (False, reason) — 非法状态转换，reason 为说明文字
        """
        with self._lock:
            if strategy_id not in self._registry:
                return None
            info = self._registry[strategy_id]
            current = info.status
            allowed = _STATUS_TRANSITIONS.get(current)
            if allowed is not None and new_status not in allowed:
                reason = (
                    f"当前状态 {current!r} 不允许转换到 {new_status!r}，"
                    f"合法目标：{sorted(allowed) if allowed else '（终态）'}"
                )
                log.warning("策略 %s 非法状态转换: %s -> %s", strategy_id, current, new_status)
                return (False, reason)
            info.status = new_status
        log.info("策略 %s 状态变更: %s -> %s", strategy_id, current, new_status)
        return (True, "")

    def update_params(self, strategy_id: str, params: Dict[str, Any]) -> None:
        """热更新策略参数（不触发重启）。"""
        with self._lock:
            if strategy_id in self._registry:
                self._registry[strategy_id].params.update(params)
                log.info("策略注册中心: 热更新参数 %s keys=%s", strategy_id, list(params.keys()))

    # ------------------------------------------------------------------
    # Snapshot persistence (DuckDB)
    # ------------------------------------------------------------------

    def snapshot_to_db(self, db_manager: Optional[Any] = None) -> int:
        """
        将当前所有策略的参数快照写入 DuckDB strategy_snapshots 表。

        每次调用写入所有已注册策略的一条新快照记录（追加，不覆盖历史）。

        Args:
            db_manager: DuckDBConnectionManager 实例。若为 None，则尝试
                        通过 data_manager.duckdb_connection_pool.get_db_manager() 获取。

        Returns:
            本次写入的记录数。
        """
        if db_manager is None:
            try:
                from data_manager.duckdb_connection_pool import get_db_manager
                db_manager = get_db_manager()
            except Exception as e:
                log.error("策略注册中心: 获取 DB 管理器失败，跳过快照: %s", e)
                return 0

        import os
        host = os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME") or ""
        ts = int(time.time() * 1000)

        with self._lock:
            items = list(self._registry.items())

        if not items:
            log.debug("策略注册中心: 无已注册策略，跳过快照")
            return 0

        written = 0
        try:
            with db_manager.get_write_connection() as con:
                # 幂等建表
                con.execute(_SNAPSHOT_DDL)
                for sid, info in items:
                    snap_id = str(uuid.uuid4())
                    try:
                        params_json = json.dumps(info.params, ensure_ascii=False, default=str)
                    except Exception:
                        params_json = "{}"
                    try:
                        tags_json = json.dumps(info.tags, ensure_ascii=False)
                    except Exception:
                        tags_json = "[]"
                    con.execute(
                        """
                        INSERT INTO strategy_snapshots
                            (snapshot_id, strategy_id, account_id, status,
                             params_json, tags_json, snapshotted_at, host)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [snap_id, sid, info.account_id, info.status,
                         params_json, tags_json, ts, host],
                    )
                    written += 1
        except Exception as e:
            log.error("策略注册中心: 快照写入 DuckDB 失败: %s", e)

        log.info("策略注册中心: 快照写入 %d 条记录", written)
        return written

    def load_snapshots_from_db(
        self,
        strategy_id: Optional[str] = None,
        limit: int = 20,
        db_manager: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        """
        从 DuckDB 读取历史快照（用于版本回溯）。

        Args:
            strategy_id: 若指定则只查询该策略的历史，否则查询全部。
            limit: 最大返回条数（按时间倒序）。
            db_manager: 同 snapshot_to_db()。

        Returns:
            快照记录列表（字典格式）。
        """
        if db_manager is None:
            try:
                from data_manager.duckdb_connection_pool import get_db_manager
                db_manager = get_db_manager()
            except Exception as e:
                log.error("策略注册中心: 获取 DB 管理器失败: %s", e)
                return []

        try:
            with db_manager.get_read_connection() as con:
                if strategy_id:
                    rows = con.execute(
                        """
                        SELECT snapshot_id, strategy_id, account_id, status,
                               params_json, tags_json, snapshotted_at, host
                        FROM strategy_snapshots
                        WHERE strategy_id = ?
                        ORDER BY snapshotted_at DESC
                        LIMIT ?
                        """,
                        [strategy_id, limit],
                    ).fetchall()
                else:
                    rows = con.execute(
                        """
                        SELECT snapshot_id, strategy_id, account_id, status,
                               params_json, tags_json, snapshotted_at, host
                        FROM strategy_snapshots
                        ORDER BY snapshotted_at DESC
                        LIMIT ?
                        """,
                        [limit],
                    ).fetchall()

            result = []
            for row in rows:
                snap_id, sid, acct, status, params_json, tags_json, ts_ms, h = row
                try:
                    params = json.loads(params_json or "{}")
                except Exception:
                    params = {}
                try:
                    tags = json.loads(tags_json or "[]")
                except Exception:
                    tags = []
                result.append({
                    "snapshot_id": snap_id,
                    "strategy_id": sid,
                    "account_id": acct,
                    "status": status,
                    "params": params,
                    "tags": tags,
                    "snapshotted_at_ms": ts_ms,
                    "host": h,
                })
            return result

        except Exception as e:
            # 表不存在时静默返回空列表（首次运行前尚未快照）
            if "strategy_snapshots" in str(e):
                return []
            log.error("策略注册中心: 读取历史快照失败: %s", e)
            return []

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        with self._lock:
            total = len(self._registry)
            running = sum(1 for i in self._registry.values() if i.status == "running")
        return f"<StrategyRegistry total={total} running={running}>"


# 全局单例：整个进程共享同一个注册中心
strategy_registry = StrategyRegistry()
