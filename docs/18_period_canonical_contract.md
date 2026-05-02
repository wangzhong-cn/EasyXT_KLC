# EasyXT 周期 Canonical 契约

> 定义 Tick / 1m / 5m / 1d / 派生周期在 EasyXT_KLC 中的唯一事实标准。

**版本**: v0.1  
**最后更新**: 2026-04-01  
**状态**: ✅ 当前约束草案 / 后续实现基准  
**适用范围**: `data_manager/`、`core/`、`gui_app/`、回测与前端状态层

---

## 1. 宗旨

本契约用于统一以下五类问题：

- 时间戳归一化
- session profile 解析
- 集合竞价 canonical 规则
- 派生周期构建规则
- 质量 / lineage / 发布门禁语义

一句话原则：

> **先把 raw 数据归一为 canonical 事实层，再构建 derived 周期，最后做 display 映射；任何绕过 canonical gate 的实现都不允许进入生产主路径。**

---

## 2. 分层模型

系统内所有周期数据统一分为四层：

### 2.1 Raw

原始输入层，仅表示“上游给了什么”：

- tick
- 1m
- 5m
- 1d
- 第三方 CSV / parquet / dat / API 原始返回

Raw 层允许存在：

- 上游竞价展示差异
- 上游时间戳类型差异
- 上游除权口径差异
- 质量等级差异

### 2.2 Canonical

系统唯一事实层，要求：

- 时间戳已完成统一归一
- session profile 已确定
- auction policy 已确定
- source_rule_kind 已确定
- quality / lineage 元数据已齐备

### 2.3 Derived

从 canonical 层派生出的周期层：

- 日内自定义周期：`2m/10m/25m/...`
- 多日左对齐周期：`2d/3d/5d/10d/75d/...`
- 自然日历周期：`1w/1M/1Q/1Y/...`

Derived 层禁止直接读取 raw 分钟规则进行拼接。

### 2.4 Display

展示层允许做：

- 前复权 / 后复权映射
- 局部结构价映射
- UI 状态聚合
- 分级质量徽章展示

Display 层禁止反向污染 canonical / raw 存储。

---

## 3. 时间戳契约

## 3.1 内部唯一时间标准

系统内部计算、存储、排序、会话归属统一使用：

- **Asia/Shanghai 语义**
- **naive `pd.Timestamp`**
- 即：**北京时间、无 tzinfo**

这是当前仓库已存在的事实基线。

## 3.2 当前仓库映射

当前实现已具备时间戳契约雏形：

- `data_manager/timestamp_utils.py`
- `data_manager/dat_binary_reader.py`
- `data_manager/unified_data_interface.py`

当前已明确：

- QMT API `ms` → 北京时间 naive
- DAT `s` → 北京时间 naive
- DuckDB 存储使用北京时间 naive `Timestamp`

## 3.3 Canonical Gate

任何 raw 时间进入 canonical 层前，必须完成：

1. 识别原始时间类型
2. 识别原始时区语义
3. 统一转换为北京时间
4. 去除 tzinfo
5. 过滤非法时间
6. 标记契约版本

## 3.4 必填审计字段

canonical / derived 层新增以下标准字段：

| 字段 | 含义 |
| ---- | ---- |
| `source_time_kind` | `epoch_ms / epoch_s / local_string / tz_aware / local_naive` |
| `source_tz` | 原始时区语义 |
| `normalized_tz` | 固定为 `Asia/Shanghai` |
| `timestamp_contract_version` | 时间戳契约版本 |
| `normalized_at` | 执行归一化的系统时间 |

## 3.5 禁止事项

- 在各个 reader 中重复硬编码 `+28800`
- 允许 tz-aware `Timestamp` 混入 canonical 层
- 展示层自行修正时间戳并反写主链路

---

## 4. Session Profile 契约

## 4.1 当前仓库现状

当前仓库已有：

- `config/session_profiles.json`
- `config/session_profile_rules.json`
- `UnifiedDataInterface._resolve_session_profile_for_symbol()`

但现状仍是**平面 profile**：

- 无 `profile_version`
- 无 `effective_from / effective_to`
- 无 `market_scope` 显式字段
- 无 `auction_policy`
- 无按 `trade_date` 解析历史版本的能力

## 4.2 必须升级为版本化 profile

每个 profile 至少包含：

| 字段 | 说明 |
| ---- | ---- |
| `profile_id` | 逻辑标识 |
| `profile_version` | 不可变版本号 |
| `effective_from` | 生效起点 |
| `effective_to` | 生效终点 |
| `timezone` | 固定 `Asia/Shanghai` 或特殊市场定义 |
| `sessions` | 交易时段列表 |
| `auction_policy` | 竞价归并规则 |
| `market_scope` | 交易所 / 市场 / 品类范围 |

## 4.3 标准解析函数

以后 session profile 解析必须收敛为：

```python
resolve_profile(symbol, trade_date, exchange, instrument_type)
```

禁止继续使用：

```python
resolve_profile(symbol)
```

## 4.4 回测固定版本

任何周期构建、修复、回放、审计回执都必须固化：

- `session_profile_id`
- `session_profile_version`

否则历史重放会发生静默漂移。

## 4.5 版本治理规则

- 历史版本不可修改
- 只能新增版本
- profile 变更必须触发契约测试
- profile 变更必须评估影响范围与重建成本

---

## 5. 集合竞价 Canonical 规则

## 5.1 A 股 baseline

A 股 canonical `1m` 默认采用：

- **merged auction rule**
- `09:25` 开盘集合竞价成交并入首根常规分钟 bar
- 不生成独立 `09:30` 竞价 bar

对于当前仓库，以 DAT 直读验证结果为准：

- 首根日盘分钟 bar 时间戳为 `09:30`
- 集合竞价成交量并入首根分钟 bar

## 5.2 Tick 事实边界

Tick 层必须保留：

- `09:25:00` 开盘集合竞价最终成交
- 深市 / 部分市场收盘集合竞价最终成交（若源可见）

但 Level1 不包含竞价逐笔委托簿，这部分不得伪装成 tick 成交事实。

## 5.3 拆分源归一化

若上游源提供：

- 独立 `09:30` 竞价 bar
- 其它拆分竞价展示规则

必须先在 canonical 层归一化为 merged rule，再进入派生周期构建。

## 5.4 必填来源字段

分钟级 canonical bar 必须记录：

| 字段 | 含义 |
| ---- | ---- |
| `auction_policy` | `merged_open_auction / split_open_auction / no_auction` |
| `source_rule_kind` | `qmt_merged / third_party_split / normalized_from_split / rebuilt_from_tick` |

---

## 6. 周期构建契约

## 6.1 日内自定义周期

日内自定义周期规则：

- 只能从 **canonical 1m** 派生
- 必须左对齐
- 午间休市是自然间隙，不是新的 anchor
- 末 bar 必须收敛到 `1D` 黄金标准

必须携带字段：

- `base_period = 1m`
- `alignment_policy_version`
- `session_profile_version`
- `source_grade`
- `bar_builder_version`

## 6.2 多日自定义周期

多日周期规则：

- 只能从 **canonical 1d** 派生
- 从 `listing_date` 左对齐
- `5d != 1w`
- `3M != 1Q`
- 最后一期允许 `is_partial = true`

## 6.3 自然日历周期

自然周期规则：

- 只能从 **canonical 1d** 派生
- 采用自然周 / 月 / 季 / 年边界
- 与交易日左对齐体系语义独立

## 6.4 周期注册表

系统必须维护单一 period registry，至少包含：

- `period_code`
- `layer`
- `base_source`
- `alignment`
- `anchor`
- `precompute_default`
- `ui_visible_default`
- `validation_level`
- `coverage_mode`

禁止 builder、调度器、UI、测试各写一份周期宇宙。

---

## 7. Tick 契约

Tick 在系统中的正式角色定义为：

1. **Verifier**
2. **Rebuilder**
3. **Arbitrator**

但 Tick 不是系统唯一强制主源。

没有 Tick 时：

- 系统仍可运行
- 但质量等级下降
- 不允许宣称结果已达到最高可信级别

---

## 8. 因子与复权契约

## 8.1 Raw minute 不存衍生复权语义

`stock_1m / stock_5m / tick` 尽量保持 raw 事实：

- 原始价格
- 原始量额
- 原始时间

## 8.2 复权因子单独维表管理

建议使用独立维表，例如：

- `daily_rights_factor`

最小主键：

- `stock_code`
- `trading_date`

建议字段：

- `rights_factor`
- `factor_version`
- `event_type`
- `source`
- `effective_from`
- `effective_to`

## 8.3 Display / Structure 映射层

结构分析与局部除权映射必须通过：

- raw minute
- factor dimension
- mapping policy

生成视图，不得直接污染 raw minute 主表。

---

## 9. 质量与状态语义契约

## 9.1 三层状态

前端与 API 统一拆分：

### 源覆盖

- 原始数据是否存在
- 本地起止范围
- 缺口数

### 派生覆盖

- 当前周期是否已生成
- 是否预计算完成
- 是否可推导但未物化

### 校验覆盖

- `contract_pass`
- `cross_source_pass`
- `tick_verified`
- `lineage_complete`

## 9.2 统一质量等级

全系统统一为：

- `golden`
- `partial_trust`
- `degraded`
- `unknown`

不允许前端、审计、修复、回测使用多套不同命名。

## 9.3 阈值版本化

所有跨源容差必须版本化：

- `ohlc_tolerance_version`
- `volume_tolerance_version`
- `bar_count_tolerance_version`

否则无法区分“数据坏了”还是“阈值变了”。

---

## 10. Lineage 与回放契约

每个 canonical / derived 结果至少记录：

- `lineage_id`
- `source_grade`
- `session_profile_version`
- `timestamp_contract_version`
- `auction_policy`
- `source_rule_kind`
- `factor_version`
- `threshold_version`
- `bar_builder_version`

每次 repair / rebuild / replay 必须生成 receipt：

- 输入范围
- 使用规则版本
- 结果状态
- 触发人 / 触发任务
- 输出数据集锚点

---

## 11. 发布门禁

一个周期结果想被定义为“生产可发布”，至少满足：

1. `contract_pass`
2. `cross_source_pass`
3. `lineage_complete`
4. `replayable`

任何一项缺失，都只能视为：

- 可运行
- 可展示
- 待人工复核

不得称为“生产放行”。

---

## 12. 与仓库现状的差距

### 已有雏形

- 时间戳基础契约已存在
- DAT / QMT 时间归一入口已存在
- 集合竞价 merged 规则已有实证文档
- builder / audit / repair / lineage 已有雏形

### 仍然缺失

- session profile 版本化
- 按 `trade_date` 解析 profile
- `auction_policy` / `source_rule_kind` 标准字段
- 统一 period registry
- threshold registry
- repair / replay receipt 标准化
- Tauri / Qt 状态语义统一

---

## 13. 强制规则摘要

### Rule A

时间戳必须先过 canonical gate，才能进入任何 bar builder。

### Rule B

session profile 必须版本化，且按历史日期解析。

### Rule C

上游拆分竞价规则必须先归一，再做派生周期。

### Rule D

自定义分钟周期只能从 canonical `1m` 派生。

### Rule E

多日周期只能从 canonical `1d` 派生。

### Rule F

复权因子不污染 raw minute 主表。

### Rule G

没有 lineage / threshold / profile version 的结果不得进入生产发布门禁。
