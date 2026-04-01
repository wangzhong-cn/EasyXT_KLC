# 周期事实与快照目标数据模型总表

> 面向未来五年的量化平台底座，统一定义周期事实、因子快照、结构快照与回执对象的落库边界。

**版本**: v0.1  
**最后更新**: 2026-04-01  
**状态**: ✅ 目标数据模型草案  
**上位文档**:
- [18_period_canonical_contract.md](file:///d:/EasyXT_KLC/docs/18_period_canonical_contract.md)
- [19_period_canonical_implementation_blueprint.md](file:///d:/EasyXT_KLC/docs/19_period_canonical_implementation_blueprint.md)

---

## 1. 一句话定义

本模型回答的不是“要不要把所有 K 线落库”，而是：

> **哪些对象必须落库、以什么层次落库、以什么主键落库、以什么版本字段落库。**

核心原则只有一条：

> **任何可被重新计算、重新解释、重新发布的对象，都不能只以“当前值”存在，必须以“事实 + 版本 + 血缘 + 回执”的形式存在。**

---

## 2. 顶层分层

整个数据面统一拆成 7 层：

| Layer | 名称 | 角色 | 是否允许覆盖 |
|------|------|------|-------------|
| L0 | Raw Fact Store | 上游事实原件 | 否 |
| L1 | Canonical Fact Store | 系统唯一周期事实层 | 否 |
| L2 | Derived Period Store | 派生周期事实层 | 否 |
| L3 | Period Factor Snapshot Store | 周期窗口因子快照层 | 否 |
| L4 | Structure Evidence Store | 结构证据层 | 否 |
| L5 | Structure Snapshot Store | 结构判定快照层 | 否，新增版本替代 |
| L6 | Receipt Store | 修复 / 回放 / 发布回执层 | 否，新增版本替代 |

这里的“否”不是说数据永不修正，而是说：

> **任何修正都不能原地覆盖，只能生成新版本对象。**

---

## 3. 命名规则

## 3.1 周期码分族

必须正式区分两类周期：

### 交易日系

- `2d`
- `3d`
- `5d`
- `10d`
- `25d`
- `50d`
- `75d`
- `2M_TRD`
- `3M_TRD`
- `5M_TRD`

### 自然日历系

- `1W`
- `1M`
- `2M_CAL`
- `3M_CAL`
- `5M_CAL`
- `1Q`
- `6M`
- `1Y`
- `2Y`
- `3Y`
- `5Y`
- `10Y`

## 3.2 命名禁令

禁止继续混用：

- `2M` 既表示 42 个交易日，又表示自然两个月
- `1w` 和 `5d` 被认为同义

周期 alias 必须进入 period registry 单点治理。

---

## 4. L0：Raw Fact Store

## 4.1 职责

L0 只保存“上游原件”，不做 canonical 修正，不做结构判定，不做展示映射。

## 4.2 目标表

| 表名 | 职责 | 主键 | 必填版本字段 | 说明 |
|------|------|------|-------------|------|
| `raw_tick_fact` | 原始 tick 成交事实 | `source_name, stock_code, trade_ts_raw, ingest_batch_id` | `raw_schema_version` | 原始 tick，不做复权 |
| `raw_1m_fact` | 原始 1m bar | `source_name, stock_code, bar_ts_raw, ingest_batch_id` | `raw_schema_version` | 保留上游分钟规则 |
| `raw_5m_fact` | 原始 5m bar | `source_name, stock_code, bar_ts_raw, ingest_batch_id` | `raw_schema_version` | 原始 5m |
| `raw_1d_fact` | 原始日线 | `source_name, stock_code, trade_date, ingest_batch_id` | `raw_schema_version` | 原始 1d |
| `raw_corporate_action` | 原始公司行动事件 | `source_name, stock_code, event_date, event_seq` | `corp_action_schema_version` | 分红、送转、拆并 |
| `raw_source_manifest` | 原始批次元信息 | `ingest_batch_id` | `manifest_schema_version` | 来源、文件、校验和 |

## 4.3 最低字段要求

Raw 表至少带：

- `source_name`
- `source_rule_kind`
- `source_time_kind`
- `source_tz`
- `ingest_batch_id`
- `raw_payload_hash`
- `ingested_at`

---

## 5. L1：Canonical Fact Store

## 5.1 职责

L1 是系统唯一周期事实层，负责：

- 时间戳 canonical gate
- session profile 解析
- 集合竞价规则归一
- 原始分钟规则归一

## 5.2 数学定义

对分钟层：

$$
canonical\_1m =
f(
raw\_1m/raw\_tick,
session\_profile\_version,
auction\_policy,
timestamp\_contract\_version,
source\_rule\_kind
)
$$

## 5.3 目标表

| 表名 | 职责 | 主键 | 必填版本字段 | 说明 |
|------|------|------|-------------|------|
| `canonical_1m_fact` | 统一分钟事实层 | `stock_code, period_code, bar_ts, canonical_version_id` | `timestamp_contract_version, session_profile_version, auction_policy_version` | 所有日内周期的唯一基座 |
| `canonical_1d_fact` | 统一日线事实层 | `stock_code, period_code, trade_date, canonical_version_id` | `timestamp_contract_version, session_profile_version` | 多日和自然周期的唯一基座 |
| `canonical_fact_lineage` | canonical 血缘明细 | `lineage_id` | `lineage_schema_version` | 记录输入源和归一策略 |

## 5.4 canonical 主链路必填字段

| 字段 | 说明 |
|------|------|
| `canonical_version_id` | 当前 canonical 事实版本 |
| `timestamp_contract_version` | 时间戳契约版本 |
| `session_profile_id` | session profile 标识 |
| `session_profile_version` | session profile 版本 |
| `auction_policy` | 竞价归并策略 |
| `auction_policy_version` | 竞价策略版本 |
| `source_rule_kind` | 原始规则类型 |
| `source_grade` | 数据源等级 |
| `lineage_id` | 血缘锚点 |
| `normalized_at` | 归一时间 |

---

## 6. L2：Derived Period Store

## 6.1 职责

L2 保存从 canonical 层派生出的周期事实。

它不是 raw，也不是展示层，而是：

> **注册周期在特定版本制度下生成的正式事实对象。**

## 6.2 目标表

| 表名 | 职责 | 主键 | 必填版本字段 | 说明 |
|------|------|------|-------------|------|
| `derived_intraday_period_fact` | 日内自定义周期事实 | `stock_code, period_code, period_end, derived_version_id` | `period_registry_version, session_profile_version, threshold_version, bar_builder_version` | 来源固定为 canonical 1m |
| `derived_multiday_period_fact` | 多日左对齐周期事实 | `stock_code, period_code, period_end, derived_version_id` | `period_registry_version, threshold_version, bar_builder_version` | 来源固定为 canonical 1d |
| `derived_calendar_period_fact` | 自然日历周期事实 | `stock_code, period_code, period_end, derived_version_id` | `period_registry_version, threshold_version, bar_builder_version` | 自然周/月/季/年 |

## 6.3 derived 最低字段

- `period_code`
- `period_family`
- `window_start`
- `window_end`
- `is_partial`
- `base_fact_version_id`
- `threshold_version`
- `bar_builder_version`
- `lineage_id`

---

## 7. L3：Period Factor Snapshot Store

## 7.1 职责

这一层不是简单的“给每根 bar 加一个 `rights_factor` 列”，而是：

> **对某个周期窗口内的因子语义做不可变快照。**

## 7.2 为什么必须独立成层

因为一个周期窗口内可能：

- 没有除权事件
- 有一次除权事件
- 有多次 corporate action

所以“这个 bar 对应的因子语义”不是单一标量，而是窗口上下文对象。

## 7.3 目标表

| 表名 | 职责 | 主键 | 必填版本字段 | 说明 |
|------|------|------|-------------|------|
| `daily_rights_factor` | 日级因子维表 | `stock_code, trading_date, factor_version` | `factor_version, factor_source_version` | 原始因子维度 |
| `period_factor_snapshot` | 周期窗口因子快照 | `stock_code, period_code, period_end, factor_snapshot_version` | `factor_policy_version, mapping_policy_version, factor_source_version` | 周期窗口对应的因子切片 |

## 7.4 `period_factor_snapshot` 最低字段

- `window_start`
- `window_end`
- `start_factor`
- `end_factor`
- `factor_window_hash`
- `ex_event_count`
- `has_ex_event_in_window`
- `factor_policy_version`
- `mapping_policy_version`
- `factor_source_version`
- `lineage_id`

---

## 8. L4：Structure Evidence Store

## 8.1 职责

L4 不是结构结论，而是结构判定所依赖的证据层。

目标是保证：

> **结构结果不是“黑箱标签”，而是能回放的证据组合。**

## 8.2 目标表

| 表名 | 职责 | 主键 | 必填版本字段 | 说明 |
|------|------|------|-------------|------|
| `structure_evidence_window` | 结构识别输入窗口 | `evidence_id` | `structure_policy_version, threshold_version` | 对应某个结构计算窗口 |
| `structure_evidence_point` | P0/P1/P2/P3 证据点 | `evidence_id, point_role, point_seq` | `structure_policy_version` | 结构关键点序列 |

## 8.3 最低字段

- `symbol`
- `period_code`
- `window_start`
- `window_end`
- `derived_version_id`
- `factor_snapshot_version`
- `structure_policy_version`
- `threshold_version`
- `session_profile_version`
- `evidence_hash`

---

## 9. L5：Structure Snapshot Store

## 9.1 职责

L5 保存“在某个规则版本下得到的结构判定快照”。

结构快照不是唯一真值，而是：

> **在特定 policy / factor / threshold / period 版本下的不可变结论。**

## 9.2 目标表

| 表名 | 职责 | 主键 | 必填版本字段 | 说明 |
|------|------|------|-------------|------|
| `structure_snapshot` | 结构判定快照 | `snapshot_id` | `structure_policy_version, threshold_version, factor_snapshot_version` | 当前判定对象 |
| `structure_snapshot_link` | 结构快照关系 | `snapshot_id, relation_type, target_snapshot_id` | `relation_schema_version` | superseded / invalidated / confirmed |

## 9.3 最低字段

- `snapshot_status`
- `symbol`
- `period_code`
- `evidence_id`
- `factor_snapshot_version`
- `structure_policy_version`
- `threshold_version`
- `lineage_anchor`
- `replay_receipt_id`
- `published_at`

---

## 10. L6：Receipt Store

## 10.1 职责

Receipt 不是审计附件，而是主链路对象。

它要回答：

- 在什么版本规则下
- 对什么范围
- 做了什么修复 / 回放 / 发布
- 产出了什么数据集
- 为什么放行或拒绝

## 10.2 目标表

| 表名 | 职责 | 主键 | 必填版本字段 | 说明 |
|------|------|------|-------------|------|
| `repair_receipt` | 数据修复回执 | `receipt_id` | `session_profile_version, threshold_version, factor_version` | repair 结果 |
| `replay_receipt` | 重放回执 | `receipt_id` | `timestamp_contract_version, threshold_version, structure_policy_version` | replay 结果 |
| `publish_gate_receipt` | 发布门禁回执 | `receipt_id` | `gate_policy_version` | 放行 / 驳回依据 |

## 10.3 Receipt 最低字段

- `symbol`
- `period_code`
- `range_start`
- `range_end`
- `session_profile_version`
- `timestamp_contract_version`
- `threshold_version`
- `factor_version`
- `mapping_policy_version`
- `source_rule_kind`
- `result_status`
- `lineage_anchor`
- `produced_dataset_hash`
- `created_at`

---

## 11. 统一版本字段字典

以下版本字段应成为系统标准字典：

| 字段 | 说明 |
|------|------|
| `timestamp_contract_version` | 时间戳契约版本 |
| `session_profile_version` | 时段 profile 版本 |
| `auction_policy_version` | 竞价归并策略版本 |
| `period_registry_version` | 周期注册表版本 |
| `threshold_version` | 容差阈值版本 |
| `factor_version` | 因子版本 |
| `factor_source_version` | 因子来源版本 |
| `mapping_policy_version` | 映射策略版本 |
| `structure_policy_version` | 结构算法版本 |
| `bar_builder_version` | 周期构建器版本 |
| `gate_policy_version` | 发布门禁策略版本 |

---

## 12. 统一主键设计原则

## 12.1 事实层主键

事实层主键必须由：

- 业务主键
- 时间边界
- 版本对象

三者共同组成。

## 12.2 快照层主键

快照层主键应优先使用：

- `snapshot_id`
- `receipt_id`
- `lineage_id`

避免用自然键反复覆盖。

## 12.3 禁止事项

- 用 `stock_code + period + date` 直接覆盖旧快照
- 用“当前最新版本”替代不可变历史对象

---

## 13. 三层状态 × 四级质量矩阵

所有前端/API 状态统一从这些表推导，不再手工拼装。

### 三层状态

- 源覆盖
- 派生覆盖
- 校验覆盖

### 四级质量

- `golden`
- `partial_trust`
- `degraded`
- `unknown`

建议最终由统一读模型推导：

| 输入 | 输出 |
|------|------|
| `contract_pass ∧ cross_source_pass ∧ tick_verified` | `golden` |
| `contract_pass ∧ cross_source_pass ∧ ¬tick_verified` | `partial_trust` |
| `contract_pass ∧ ¬cross_source_pass` | `degraded` |
| 其余情况 | `unknown` |

---

## 14. 热 / 温 / 冷三级存储建议

## 14.1 冷层

适合：

- `raw_tick_fact`
- `raw_source_manifest`
- 大量历史 replay 原件

特点：

- 容量最大
- 查询频率最低
- 以压缩与归档为优先

## 14.2 温层

适合：

- `canonical_1m_fact`
- `canonical_1d_fact`
- `derived_*_fact`
- `daily_rights_factor`
- `period_factor_snapshot`

特点：

- 承担绝大部分计算与回测读取
- 是平台主工作层

## 14.3 热层

适合：

- `structure_snapshot`
- 最新 `repair_receipt / replay_receipt / publish_gate_receipt`
- UI / API 高频读模型

特点：

- 响应优先
- 数据量最小
- 保留近期窗口

---

## 15. 与当前仓库最需要先补的 5 张表

如果按性价比排序，我建议第一批先落：

1. `session_profile_registry` 对应配置与解析器
2. `period_registry`
3. `period_threshold_registry`
4. `daily_rights_factor`
5. `period_factor_snapshot`

原因很简单：

- 前三者负责“立法”
- 后两者负责把“分钟因子语义”从口头约束变成可落库事实

---

## 16. 实施顺序建议

### Step 1

先实现注册表：

- session profile
- period registry
- threshold registry

### Step 2

再实现 canonical gate 与 canonical fact schema。

### Step 3

然后落地 factor snapshot / structure evidence / receipt。

### Step 4

最后用这些表驱动 UI / API / gate / replay。

---

## 17. 最终判断

这份目标数据模型的核心意义，不是“多建几张表”，而是把整个量化平台的对象分清：

- 什么是事实
- 什么是快照
- 什么是因子
- 什么是结构证据
- 什么是回执

只有先把这些对象层次钉死，后续：

- 因子库
- 结构库
- 周期库
- 回放链
- 发布门禁

才不会继续互相污染。
