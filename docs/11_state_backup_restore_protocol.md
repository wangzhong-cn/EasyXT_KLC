# 状态主线备份 / 恢复 / 校验协议

> 目标：把 SQLite(WAL) 主状态从“能写”推进到“能备份、能恢复、能校验、能说明恢复到了哪个版本”。

**代码落点**: `core/state_store/backup_restore.py`
**测试落点**: `tests/test_state_store_backup_restore.py`
**状态**: ✅ 已落地最小生产协议

---

## 一句话原则

> **备份必须用 SQLite 官方 backup API；恢复必须走离线目标根目录；校验必须对文件存在性、checksum 与 integrity_check 三重确认。**

---

## 当前已落地的能力

`SQLiteBackupManager` 已提供：

- `create_backup(...)`
- `verify_backup(...)`
- `restore_backup(...)`

同时，状态页读模型现在已经开始消费备份结果中的时间元信息：

- `/api/v1/system/state-status`
- `apps/tauri-shell/src/routes/SystemRoute.tsx`

并且 `shadow manifest` 中的失败细节也开始进入前端可观察面板：

- `shadow_failed_stage`
- `shadow_error`

也就是说，`backup_last_success_at` 不再只是协议字段，而是已经进入新壳可观察面板。

并配套：

- `BackupManifest`
- `BackupFileRecord`
- `BackupVerificationReport`
- `RestoreReceipt`

---

## 为什么备份不能裸复制 `.db`

因为当前主状态默认使用 SQLite WAL 模式。

这意味着：

- 单独复制 `.db` 文件不一定包含最新提交
- `.db-wal` / `.db-shm` 可能仍承载未 checkpoint 内容
- 恢复时可能得到“文件存在，但状态不一致”的假备份

所以当前协议选择：

### 备份阶段

- 使用 SQLite 官方 backup API
- 输出一个静态一致的备份 SQLite 文件

### 恢复阶段

- 面对的是已经一致的备份文件
- 采用 `copy2 + fsync(best-effort) + os.replace` 原子替换到目标路径

---

## Manifest 当前字段

`manifest.json` 当前至少包含：

| 字段 | 含义 |
| ---- | ---- |
| `format_version` | manifest 格式版本 |
| `backup_id` | 备份 ID |
| `created_at` | 创建时间 |
| `source_root` | 源状态根目录 |
| `logical_seq_watermark` | 备份时的逻辑序号水位 |
| `catalog_relative_path` | catalog 备份相对路径 |
| `file_count` | 文件总数 |
| `files[]` | 每个 SQLite 文件的元信息 |

`files[]` 中当前包含：

| 字段 | 含义 |
| ---- | ---- |
| `role` | `catalog` / `shard` |
| `relative_path` | 相对备份根目录路径 |
| `family_name` | 所属表族 |
| `shard_id` | 分片 ID |
| `size_bytes` | 文件大小 |
| `sha256` | SHA-256 校验值 |
| `logical_seq_end` | 该文件对应逻辑序号末尾 |
| `row_count` | catalog 中记录的行数 |

---

## 校验协议

当前 `verify_backup(...)` 会做三层检查：

### 1. 文件存在性

- manifest 记录的每个文件都必须存在

### 2. 文件一致性

- `size_bytes` 匹配
- `sha256` 匹配

### 3. SQLite 完整性

- 对每个备份文件执行 `PRAGMA integrity_check`
- 必须返回 `ok`

如果任一项失败：

- `BackupVerificationReport.ok = False`
- `errors[]` 中明确列出失败原因

---

## 恢复协议

### 关键语义

恢复目标必须是：

- **离线目标根目录**
- 没有被提前打开的 catalog / shard 连接

因为恢复提交阶段使用的是：

- 临时文件写入
- 原子替换 `os.replace`

如果目标路径已被占用，系统会拒绝替换。

### 恢复流程

1. 读取 manifest
2. （默认）先执行 verify
3. 对每个备份 SQLite 文件复制到临时目标文件
4. 对临时文件做 best-effort flush
5. 原子替换到正式目标路径
6. 清理目标 `.db-wal` / `.db-shm` 残留侧文件

---

## 当前目录语义

```text
runtime/
  state/
    catalog/
      shard_catalog.db
    shards/
      frontend_events/
        2026-03.sqlite3
        2026-04.sqlite3

backups/
  backup-20260331T120000Z/
    manifest.json
    catalog/
      shard_catalog.db
    shards/
      frontend_events/
        2026-03.sqlite3
        2026-04.sqlite3
```

---

## 当前未做但后续可以追加的内容

- `last_good_backup` 指针
- manifest 签名
- 备份保留策略 / 自动轮转
- 更细粒度 restore（按 family / shard）
- restore 后自动比对 catalog 快照

---

## 对团队的直接结论

以后不要再把状态备份理解成：

- `copy xxx.db backup.db`

当前仓库里已经有更严格也更正确的协议与代码入口：

- `SQLiteBackupManager.create_backup()`
- `SQLiteBackupManager.verify_backup()`
- `SQLiteBackupManager.restore_backup()`

---

## 一句话总结

> **现在的状态主线不再只有写路径；它已经开始具备零运维场景下最关键的恢复底线。**
