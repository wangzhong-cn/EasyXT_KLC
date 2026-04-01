# 周期 Canonical 契约实现蓝图

> 将 `18_period_canonical_contract.md` 转换为仓库可执行改造计划。

**版本**: v0.1  
**最后更新**: 2026-04-01  
**状态**: ✅ 当前实施蓝图

---

## 1. 目标

本蓝图只做一件事：

> 把“时间戳、session、auction、tick、factor、quality、lineage、gate”收敛到同一套制度化中间层。

---

## 2. 现状判断

当前仓库不是从零开始，而是处在“雏形齐全、制度层缺失”的阶段。

### 已有基础

- 时间戳契约工具：`data_manager/timestamp_utils.py`
- bar builder：`data_manager/period_bar_builder.py`
- 统一数据接口：`data_manager/unified_data_interface.py`
- 质量门禁 / lineage 雏形：`docs/lineage_spec.md`、UDI 写入审计链
- 集合竞价验证结论：`docs/黄金铁律_集合竞价与分钟线数据规则.md`

### 核心短板

- 没有版本化 session profile registry
- 没有标准化 threshold registry
- 没有 canonical 元字段 schema
- 没有 repair / replay receipt 协议
- 没有前端统一质量状态语义
- 契约测试仍未完全由 registry 驱动

---

## 3. 分阶段落地

## Phase A：立法层

目标：先把制度层固定下来。

### A1. Session Profile Registry

新增：

- `config/session_profiles.v1.json`
- `config/session_profile_versions.json`

建议结构：

```json
{
  "profiles": [
    {
      "profile_id": "CN_A",
      "profile_version": "2026.04.01",
      "effective_from": "1990-12-19",
      "effective_to": null,
      "timezone": "Asia/Shanghai",
      "auction_policy": "merged_open_auction",
      "market_scope": ["SH", "SZ"],
      "sessions": [["09:30", "11:30"], ["13:00", "15:00"]]
    }
  ]
}
```

### A2. Threshold Registry

新增：

- `config/period_thresholds.json`

建议字段：

- `threshold_version`
- `ohlc_tolerance`
- `volume_tolerance`
- `bar_count_tolerance`
- `cross_source_overlap_min`

### A3. Period Registry

新增：

- `config/period_registry.json`

建议字段：

- `period_code`
- `layer`
- `base_source`
- `alignment`
- `anchor`
- `precompute_default`
- `ui_visible_default`
- `validation_level`
- `coverage_mode`

---

## Phase B：解析层

目标：让所有运行时逻辑不再直接读平面配置。

### B1. Session Resolver

新增模块建议：

- `data_manager/session_profile_registry.py`

核心 API：

```python
resolve_profile(symbol, trade_date, exchange, instrument_type)
```

返回：

- `profile_id`
- `profile_version`
- `auction_policy`
- `sessions`
- `timezone`

### B2. Timestamp Canonical Gate

建议新增模块：

- `data_manager/timestamp_contract.py`

职责：

- 原始时间识别
- 北京时间归一
- tz 去除
- 契约版本标记
- 统一 metadata 产出

### B3. Threshold Resolver

建议新增：

- `data_manager/threshold_registry.py`

核心 API：

```python
resolve_thresholds(period, market, source_grade, as_of_date)
```

---

## Phase C：数据模型层

目标：把契约元信息正式落表，而不是只存在内存里。

### C1. Canonical Minute / Daily 元字段

对 canonical / derived 输出统一补齐：

- `timestamp_contract_version`
- `session_profile_id`
- `session_profile_version`
- `auction_policy`
- `source_rule_kind`
- `source_grade`
- `threshold_version`
- `bar_builder_version`
- `lineage_id`

### C2. 因子维表

建议新增：

- `daily_rights_factor`

建议字段：

- `stock_code`
- `trading_date`
- `rights_factor`
- `factor_version`
- `event_type`
- `source`
- `effective_from`
- `effective_to`
- `created_at`

### C3. Repair Receipt 表

建议新增：

- `period_repair_receipts`

建议字段：

- `receipt_id`
- `symbol`
- `period`
- `range_start`
- `range_end`
- `session_profile_version`
- `timestamp_contract_version`
- `threshold_version`
- `factor_version`
- `action_kind`
- `result_status`
- `lineage_anchor`
- `created_at`

---

## Phase D：Builder 层

目标：让构建器成为“执法层”，而不是隐式规则堆。

### D1. PeriodBarBuilder 改造

`data_manager/period_bar_builder.py` 需要收敛：

- 输入必须为 canonical 1m / 1d
- 输出必须携带 registry 版本信息
- 不再仅返回 `session_profile` 字符串
- 必须同时返回 `session_profile_version`

### D2. Auction Normalize Step

新增独立步骤：

- `normalize_auction_rule(raw_df, auction_policy, source_rule_kind)`

目的：

- 把 `third_party_split` 归一为 `merged_open_auction`
- 保证 builder 永远只处理 canonical minute

### D3. Tick 仲裁接口

建议新增：

- `data_manager/tick_verifier.py`

职责：

- 从 tick 验证 1m
- 必要时重建 1m
- 给出 `golden / partial_trust / degraded`

---

## Phase E：UDI 层

目标：让 UDI 从“统一接口”升级成“统一契约入口”。

### E1. UnifiedDataInterface 收敛点

`data_manager/unified_data_interface.py` 需要补：

- `resolve_profile(..., trade_date=...)`
- `timestamp canonical metadata`
- `auction policy metadata`
- `source_rule_kind`
- `threshold_version`

### E2. 本地读接口分层

建议显式拆出：

- `get_raw_data_local()`
- `get_canonical_data_local()`
- `get_derived_data_local()`

避免现在“读到什么算什么”的模糊层次。

### E3. 写入审计增强

写入 receipt 中加入：

- `session_profile_version`
- `timestamp_contract_version`
- `threshold_version`
- `source_rule_kind`

---

## Phase F：前端与 API

目标：状态语义完全对齐。

### F1. API 扩展

返回给前端的 payload 统一增加：

- `source_coverage`
- `derived_coverage`
- `validation_coverage`
- `quality_grade`
- `lineage_complete`
- `replayable`

### F2. Qt / Tauri 对齐

Qt 与未来前端统一使用：

- `golden`
- `partial_trust`
- `degraded`
- `unknown`

同时展示：

- 本地起止范围
- 缺口数
- 派生覆盖状态
- 自动补齐状态
- 当前 profile version

---

## Phase G：测试矩阵

目标：让契约与测试永不分家。

### G1. 参数来源统一

测试矩阵应从以下 registry 自动生成：

- period registry
- session profile registry
- threshold registry
- auction policy registry

### G2. 必备测试组

#### 时间戳

- epoch ms → 北京时间
- epoch s → 北京时间
- tz-aware → naive Beijing
- 无效时间过滤

#### Session

- symbol + trade_date 路由正确
- 历史版本不漂移
- 新旧版本切换边界正确

#### Auction

- merged 规则收敛
- split 源归一收敛
- 首根分钟成交量吸收一致

#### Derived

- 自定义分钟左对齐
- 多日 listing_date 左对齐
- 自然周期右闭合

#### Tick

- tick 验证通过
- tick 验证失败降级
- tick 重建 receipt 可回放

#### Gate

- contract_pass
- cross_source_pass
- lineage_complete
- replayable

---

## 4. 建议新增文件

建议优先新增：

- `docs/18_period_canonical_contract.md`
- `config/period_registry.json`
- `config/period_thresholds.json`
- `config/session_profile_versions.json`
- `data_manager/session_profile_registry.py`
- `data_manager/threshold_registry.py`
- `data_manager/timestamp_contract.py`
- `data_manager/tick_verifier.py`
- `tests/test_session_profile_registry.py`
- `tests/test_period_registry_contract.py`
- `tests/test_threshold_registry.py`
- `tests/test_auction_canonical_contract.py`

---

## 5. 建议优先级

### P0

- Session profile 版本化
- Timestamp canonical gate 升级为不可绕过入口
- Auction normalize step
- Canonical 元字段 schema

### P1

- Period registry
- Threshold registry
- Tick verifier / rebuild receipt
- API / 前端状态语义统一

### P2

- Tauri 全量状态接线
- 回放编排器
- 自动重建调度器参数化

---

## 6. 完成标准

满足以下条件，才算“契约真正落地”：

1. 配置已版本化
2. 运行时解析按历史日期执行
3. builder 只吃 canonical 输入
4. 输出带完整版本字段
5. gate 能据此做放行判断
6. 测试矩阵由 registry 驱动
7. 前端能完整展示质量与版本语义

---

## 7. 当前建议

下一步最值得优先实施的是：

### Step 1

先做 `session_profile_versions.json + session_profile_registry.py`

### Step 2

再做 `period_registry.json + threshold_registry.py`

### Step 3

最后改 `PeriodBarBuilder / UnifiedDataInterface / 前端状态层`

也就是说：

> **先把规则注册表做好，再去改 builder 与 UI；否则实现仍会继续长成多套方言。**
