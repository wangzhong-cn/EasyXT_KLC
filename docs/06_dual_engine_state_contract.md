# EasyXT 双引擎状态契约

> 定义 SQLite (WAL) 与 DuckDB 在当前阶段的默认职责边界，以及 shadow 原子同步协议。

**版本**: v0.1
**最后更新**: 2026-03-31
**状态**: ✅ 当前默认技术契约

---

## 一句话定义

- **SQLite (WAL)**：默认主写状态
- **DuckDB**：默认只读影子与分析引擎
- **Shadow 原子同步**：从主写状态到只读视图的一致性提交协议

一句话原则：

> **写入先落 SQLite，分析只读走 DuckDB，只有原子提交完成的影子版本才允许被前端读取。**

---

## 为什么采用双引擎

### 选择 SQLite (WAL) 的原因

SQLite 更适合作为当前阶段的默认在线可写主状态：

- 本地单机默认可用
- 事务明确
- WAL 模式稳定
- 易于调试与回放
- 与前端状态模型契合
- 不强行引入分布式复杂度

### 保留 DuckDB 的原因

DuckDB 仍然非常适合：

- 宽表分析
- 结构/Layer 4 聚合
- 历史查询
- 只读 summary/detail 查询
- Notebook / CLI / 离线实验

### TimescaleDB 已排除的原因

- 零运维是当前不可突破的底线
- Docker / PostgreSQL / 网络配置会引入额外运维复杂度税
- 团队当前最核心的问题是可观察性，不是数据库层分布式扩展
- 应用层分片 + DuckDB 联邦查询足以覆盖当前与下一阶段需求

---

## 权威性规则

## Rule 1：SQLite 是唯一默认权威写源

以下内容默认写入 SQLite：

- UI 状态
- 作业状态
- 同步状态
- 用户触发的 Bayesian preview/apply 记录
- 前端可观察事件
- 结构监控页的操作历史
- 版本水位 / manifest 元数据

### SQLite 路径禁止事项

- 新功能默认直接写 DuckDB
- 将 DuckDB 当作在线真相源
- 通过 UI 内存状态绕过 SQLite 事务语义

---

## Rule 2：DuckDB 只承担读模型

DuckDB 默认只做：

- 结构列表读模型
- 结构详情只读分析视图
- Layer 4 summary / 聚合
- 历史宽表查询
- 离线实验底座

### DuckDB 路径禁止事项

- 将用户前端动作直接写入 DuckDB 作为主状态
- 在影子未提交完成时暴露新版本给前端

---

## Rule 3：前端必须能看到版本与同步状态

每个读请求至少要能追溯：

- 当前 SQLite 主状态版本
- 当前 DuckDB 影子版本
- 最近一次 shadow 提交时间
- last_good_version
- 当前同步状态（idle / syncing / failed / degraded）

没有这些可见信号，就不算完成前端可观察性建设。

---

## Shadow 原子同步协议

## 目标

保证 DuckDB 只读侧永远只暴露“完整、可验证、可回滚”的版本。

## 基本流程

1. 用户/服务写入 SQLite 事务
2. 事务成功后生成新的逻辑版本号 `logical_seq`
3. 同步器根据 `logical_seq` 构建 shadow 数据
4. 将 shadow 写入临时路径
5. `fsync` 落盘
6. 原子替换目标 shadow 文件
7. 更新 manifest
8. 将新版本标记为当前可读版本

如果任一步失败：

- 继续保留 `last_good_version`
- 前端只能读到旧影子
- 同步状态必须显示为失败/降级

---

## 原子同步必须满足的 4 项约束

### 1. 临时文件写入

禁止直接覆盖当前可读 shadow 文件。

### 2. 持久化刷盘

写入后必须执行 flush / fsync，保证数据完整落盘。

### 3. 原子替换

提交阶段必须使用原子替换语义，不能让前端看到半写文件。

### 4. manifest 更新晚于 shadow 替换

只有 shadow 成功提交后，manifest 才能更新当前版本指针。

---

## Manifest 契约

建议 manifest 至少包含以下字段：

| 字段 | 含义 |
| ---- | ---- |
| `version_id` | 当前影子版本 ID |
| `logical_seq` | 来源 SQLite 逻辑序号 |
| `created_at` | 影子版本生成时间 |
| `checksum` | 影子文件校验值 |
| `shadow_path` | 当前影子路径 |
| `last_good_version` | 最近一次成功版本 |
| `status` | `idle/syncing/failed/degraded/ready` |
| `error` | 最近失败原因 |

---

## 推荐的目录语义

```text
runtime/
  state/
    sqlite/
      easyxt_state.db
      easyxt_state.db-wal
    duckdb_shadow/
      current/
        structures.duckdb
        manifest.json
      versions/
        000001/
          structures.duckdb
          manifest.json
        000002/
          structures.duckdb
          manifest.json
```

### 说明

- `sqlite/`：主写状态
- `duckdb_shadow/current/`：当前对前端可见的只读影子
- `duckdb_shadow/versions/`：保留可回滚版本

---

## 零运维扩展路线

### 路线定义

```text
SQLite (WAL) 单库
  -> SQLite 分片（按时间/标的）
    -> DuckDB 多文件 ATTACH / 联邦查询
```

### 核心原则

- 通过应用层路由决定写入哪个 SQLite 分片
- 通过 shard catalog 只把必要分片暴露给 DuckDB
- 不引入任何外部数据库服务
- 不引入 Docker 依赖

---

## 技术警戒线

### 1. WAL 备份不能裸复制单个 `.db`

在 SQLite WAL 模式下，**只复制主 `.db` 文件不是完整备份**。必须满足以下之一：

- 使用 SQLite 官方 backup API
- 先做受控 checkpoint，再复制 `.db` 与相关 WAL 状态
- 明确记录备份来源版本与校验值

### 2. DuckDB 联邦查询不要依赖“神奇查询缓存”

当前优化重点不应是假设 DuckDB 自带一个足够可靠的通用查询缓存开关，而应优先做：

- shard catalog 裁剪
- 限制 ATTACH 文件数量
- 复用 shard 集合
- 预构建常用视图/查询模板

### 3. DuckDB 的 SQLite 访问能力必须随应用一起交付

如果运行环境需要额外下载 SQLite 扩展，零运维目标就会被破坏。必须确保：

- 相关能力已随应用打包
- 离线环境可直接运行
- 不依赖首次联网下载扩展

### 4. 分片策略必须按表族定义，不能一刀切

并不是所有表都应该按同一种规则分片。至少需要区分：

- 实时写入表
- 审计/事件表
- 只读宽表/分析表

---

---

## SQLite 建议承载的表（P0/P1）

### P0 必备

- `ui_sessions`
- `job_runs`
- `bayes_actions`
- `sync_manifest`
- `sync_failures`
- `frontend_events`

### P1 可扩展

- `structure_projection_queue`
- `signal_projection_queue`
- `audit_projection_queue`
- `api_request_log`
- `user_preferences`

---

## 读写流程定义

## 写流程

```text
Frontend/Tauri
  -> Service API
    -> SQLite transaction commit
      -> enqueue shadow sync
        -> build DuckDB shadow
          -> atomic publish
            -> update manifest
```

## 读流程

```text
Frontend/Tauri
  -> Service API
    -> read current manifest
      -> open current DuckDB shadow (read-only)
        -> return payload + version metadata
```

---

## 前端必须可见的状态字段

每次关键读写请求都建议返回：

- `sqlite_logical_seq`
- `duckdb_shadow_version`
- `sync_status`
- `last_good_version`
- `sync_lag_ms`
- `server_time`

前端页面上必须有一个位置能直接显示这些值。

---

## 失败语义

## 可恢复失败

例如：

- shadow 构建失败
- checksum 校验失败
- 临时文件写入失败
- manifest 更新失败

处理要求：

- 保持旧 `last_good_version`
- 读侧不中断
- 状态标记为 `failed` 或 `degraded`
- 前端明确显示失败阶段

## 不允许的失败语义

以下行为属于不可接受：

- 影子写一半就被前端读到
- manifest 指向损坏版本
- 当前读版本与 checksum 不一致
- 无法知道当前读模型版本

---

## 与 Tauri 的契约关系

Tauri 前端不应依赖“后台正在发生什么”的隐式假设，而应显式消费以下契约：

- 当前状态来源是谁
- 当前读到的是哪个 shadow 版本
- 最近一次同步是否成功
- 失败发生在哪个阶段

如果前端看不到这些信息，就说明双引擎架构尚未真正交付。

---

## TimescaleDB 不在主线中

当前架构明确排除：

- Docker 化 PostgreSQL/TimescaleDB 依赖
- 为未来分布式预埋复杂实现
- 以数据库层分布式替代应用层分片

如无新的组织级硬约束，本仓库默认不再把 TimescaleDB 作为演进方向。

---

## 验收标准

## P0 验收

- SQLite WAL 成为默认写路径
- DuckDB 明确为只读影子
- manifest/version 对前端可见
- 影子同步失败时保留 last_good_version
- 前端可直接验证当前读取的是哪个版本

## P1 验收

- 关键结构/Bayesian/审计页面全部消费统一同步状态
- 支持回滚到指定 shadow 版本
- 可统计同步失败率与滞后

---

## 一句话总结

> **SQLite (WAL) 是当前阶段的主写状态，DuckDB 是当前阶段的只读影子与分析模型；零运维、应用层分片与 shadow 原子同步是这条路线的三条不可动摇约束。**
