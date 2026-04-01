# DuckDB 联邦读层 Planner 说明

> 目标：先把“应该 attach 哪些分片、最多 attach 多少、如何生成联邦 SQL”变成显式 planner，而不是在读路径里边查边猜。

**代码落点**:

- `core/state_store/federation_planner.py`
- `core/state_store/federation_executor.py`
- `tools/verify_federation_sqlite_runtime.py`

**测试落点**:

- `tests/test_duckdb_federation_planner.py`
- `tests/test_duckdb_federation_executor.py`

**状态**: ✅ 已落地 planner / executor，并已补充真实环境探针

---

## 一句话原则

> **DuckDB 联邦读层的首要优化不是“并行 ATTACH”或“神奇缓存”，而是：少 attach、准 attach、受预算约束地 attach。**

---

## 当前已落地的对象

### `DuckDBFederationPlanner`

负责：

- 根据 `ShardCatalog` 裁剪分片
- 按时间范围或 `symbol` 选择所需 shard
- 执行 attach budget 约束
- 生成 DuckDB `ATTACH ... (TYPE SQLITE)` 语句
- 生成 `UNION ALL` 读 SQL 草案

### `FederationPlan`

包含：

- `family_name`
- `attach_budget`
- `bindings[]`
- `pruned_shard_count`

### `FederationBinding`

每个 binding 当前包含：

- `alias`
- `shard_id`
- `family_name`
- `table_name`
- `db_path`
- `logical_seq_end`

### `AttachBudgetExceededError`

当计划附加的 SQLite 分片数量超出预算时直接抛出。

### `DuckDBFederationExecutor`

负责：

- 调用 planner 获取 `FederationPlan`
- 执行 `ATTACH ... (TYPE SQLITE)`
- 执行 `UNION ALL` 查询
- 在多 shard `UNION ALL` 查询下按占位符数量展开 `query_params`
- 返回结果行、row_count 与 `latest_logical_seq`

当前这层的意义不是一次性把所有读模型都迁完，而是把“可执行联邦读”从设计推进到真正模块。

---

## 为什么先做 planner，而不是直接执行 DuckDB 查询

因为当前阶段最重要的是先把以下决策显式化：

1. 哪些 shard 被选中了
2. 为什么是这些 shard
3. attach 数量有没有失控
4. 生成的联邦 SQL 是什么

如果这些都还是隐式行为，那后续：

- API
- 同步状态页
- 读模型层
- benchmark

都没有稳定基础。

---

## 当前支持的裁剪能力

### 1. `time_monthly`

用于：

- 审计事件
- 前端事件
- 作业运行日志
- 其他时间型状态表

planner 会根据：

- `start_time`
- `end_time`

去裁剪只需要的月份分片。

### 2. `symbol_prefix`

用于：

- 按标的聚簇的状态分片

planner 会根据：

- `symbol`

选择对应 prefix shard。

---

## attach budget 语义

这是当前 planner 最重要的硬约束之一。

如果某次读请求需要 attach 的 shard 数量超过预算：

- 直接抛 `AttachBudgetExceededError`
- 不偷偷继续 attach 全部文件

这样做的目的不是保守，而是：

- 迫使上层继续裁剪时间范围/标的范围
- 防止“看起来能跑，实际上 attach 失控”

---

## 当前 SQL 生成与执行能力

### `build_attach_sql(plan)`

生成类似：

```sql
ATTACH '.../2026-03.sqlite3' AS s0 (TYPE SQLITE);
ATTACH '.../2026-04.sqlite3' AS s1 (TYPE SQLITE);
```

### `build_union_sql(plan, ...)`

生成类似：

```sql
SELECT * FROM s0.frontend_events
UNION ALL
SELECT * FROM s1.frontend_events
ORDER BY event_ts
```

注意：

- planner 仍然独立存在
- 但当前阶段已经有 `DuckDBFederationExecutor` 可消费这些 planner 产物并执行查询

---

## 当前测试覆盖了什么

### 1. 月分片裁剪

- 范围只落在 2026-03 时，只选择 `2026-03` shard

### 2. symbol prefix 裁剪

- `BTCUSDT` 只落到 `BTC` 作用域

### 3. attach budget 约束

- 需要 attach 3 个 shard，预算 2 时直接失败

### 4. 多 shard 参数绑定展开

- 当同一个 `where_sql` 被复制到多个 shard 的 `UNION ALL` 查询中时，executor 会按 `?` 实际数量展开 `query_params`
- 这样可以避免运行时出现“SQL 占位符数量 > 传入参数数量”的真实环境错误

---

## 发布前真实环境验证

单元测试只能证明 planner / executor 的逻辑形状正确；**发布前还必须确认当前运行环境真的支持**：

1. DuckDB `LOAD sqlite`
2. `ATTACH ... (TYPE SQLITE)`
3. `read_frontend_events_read_model()` 真实走 federation executor
4. `/api/v1/system/frontend-events` 真实返回 200

为此，仓库现在提供了发布前探针：

- `tools/verify_federation_sqlite_runtime.py`

### 推荐执行方式

当前仓库已迁移到 `uv` / `.venv` 工作流，发布前优先执行：

```powershell
uv run python tools/verify_federation_sqlite_runtime.py
```

如需直接指定当前项目虚拟环境，也可以执行：

```powershell
.\.venv\Scripts\python.exe tools\verify_federation_sqlite_runtime.py
```

### 探针做了什么

1. 在临时 `state_root` 中创建 `frontend_events` 分片 SQLite
2. 直接探测 DuckDB `LOAD sqlite` 与 `ATTACH ... (TYPE SQLITE)`
3. 通过 `read_frontend_events_read_model()` 走真实 executor 链路
4. 通过 FastAPI `TestClient` 访问 `/api/v1/system/frontend-events`

### 通过判据

- 进程退出码为 `0`
- 输出 JSON 中：
  - `probe.attach_type_sqlite_ok == true`
  - `read_model.source == "federation_executor"`
  - `api_status_code == 200`
  - `api_body.returned` 与预期记录数一致

### 两个重要备注

1. 探针会在进程内临时设置 `EASYXT_DEV_MODE=1` 来启动本地 `TestClient` 生命周期，**这只是本地验证便捷措施，不代表生产环境可以跳过 `EASYXT_API_TOKEN`**。
2. Windows 上若仅出现临时目录清理警告，但退出码仍为 `0`，则核心结论仍以验证通过为准；这类告警通常只是 SQLite / DuckDB 文件句柄释放略慢。

---

## 接下来最自然的下一步

现在 executor 已经开始落地，下一层最顺的演进是：

### 1. API 暴露

把 planner/executor 结果接到：

- `core/api_server.py`
- Tauri `SystemRoute` / 结构读模型页

当前已经启动的实际接线包括：

- `/api/v1/system/frontend-events` —— 通过 federation executor 读取 `frontend_events` 状态读模型
- `SystemRoute` —— 已可并行消费状态快照与最近状态事件

### 2. 常用模板固化

- 结构事件读模板
- 前端事件读模板
- 同步审计读模板

### 3. 只读连接复用 / 生命周期治理

- 明确 executor 的只读连接策略
- 明确 attach / detach 的请求级生命周期
- 在不引入运维复杂度的前提下控制联邦读开销

---

## 一句话总结

> **DuckDB 联邦读层现在已经从“原则口号”变成了 planner + executor：它不仅会裁剪分片、限制 ATTACH，还已经能执行联邦读并返回结果与 logical sequence 元信息。**
