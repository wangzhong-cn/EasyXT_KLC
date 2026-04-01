"""SQLite 分片状态存储基础设施。

本包用于承载零运维主线中的 operational state：

- ``ShardCatalog``：统一管理表族、分片文件、逻辑序号与状态边界
- ``ShardedSQLite``：对外提供按表族路由的 SQLite (WAL) 写入/跨分片读取能力

当前阶段刻意保持接口小而清晰，后续 backup/restore、manifest 与 DuckDB
联邦读层都应建立在这里的协议之上，而不是各写各的文件工具。
"""

from .shard_catalog import ShardCatalog, ShardFamilyConfig, ShardRef
from .sharded_sqlite import ShardedSQLite, WriteReceipt
from .backup_restore import (
    BackupFileRecord,
    BackupManifest,
    BackupVerificationReport,
    RestoreReceipt,
    SQLiteBackupManager,
)
from .federation_planner import (
    AttachBudgetExceededError,
    DuckDBFederationPlanner,
    FederationBinding,
    FederationPlan,
)
from .federation_executor import DuckDBFederationExecutor, FederationQueryResult
from .system_read_models import FrontendEventsReadModel, read_frontend_events_read_model
from .system_status import (
    SystemStateSnapshot,
    get_system_state_snapshot,
    resolve_federation_attach_budget,
    resolve_state_backup_root,
    resolve_state_root,
)

__all__ = [
    "ShardCatalog",
    "ShardFamilyConfig",
    "ShardRef",
    "ShardedSQLite",
    "WriteReceipt",
    "BackupFileRecord",
    "BackupManifest",
    "BackupVerificationReport",
    "RestoreReceipt",
    "SQLiteBackupManager",
    "AttachBudgetExceededError",
    "DuckDBFederationPlanner",
    "FederationBinding",
    "FederationPlan",
    "DuckDBFederationExecutor",
    "FederationQueryResult",
    "FrontendEventsReadModel",
    "read_frontend_events_read_model",
    "SystemStateSnapshot",
    "get_system_state_snapshot",
    "resolve_federation_attach_budget",
    "resolve_state_backup_root",
    "resolve_state_root",
]