# QMT 本地编排内核合同 v0.1

> 状态：2026-04-27 评审草案
>
> 目标：冻结 QMT 本地编排内核的第一版正式合同，作为 `P0 / P0.5` 改造的共同基线。

---

## 1. 合同版本

- `schema_version = 1`
- `api_contract_version = 1`
- `route_policy_version = 1`
- `event_contract_version = 1`
- `ui_contract_version = 1`

## 2. 首页硬约束

1. **事实层**负责真相、证据、可复验。
2. **意图层**负责绑定、策略、人工覆盖。
3. **运行态层**负责会话、健康、TTL、stale。
4. **交易通道**必须一等建模。
5. **历史路由**与**交易路由**必须物理分离。
6. **先定合同，再做 P0 改造，再做手动闭环，最后做自动化**。

## 3. 首页禁令

1. 禁止新增模糊路径字段作为主真相。
2. 禁止自动探测事实直接覆盖意图层。
3. 禁止运行态状态无时间语义落库。
4. 禁止未经过冲突判定的自动绑定直接写正式绑定。

## 4. 六张读模型

### 4.1 `qmt_layouts`

- 层级：事实层
- 主键：`layout_id`
- 核心字段：
  - `fingerprint`
  - `install_root`
  - `exe_path`
  - `bin_path`
  - `python_roots[]`
  - `xtquant_roots[]`
  - `userdata_roots[]`
  - `datadir_paths[]`
  - `data_paths[]`
  - `datas_paths[]`
  - `cfg_paths[]`
  - `log_paths[]`
- 证据字段：
  - `discovered_from[]`
  - `scan_root`
  - `raw_hints`
  - `source_process`
  - `evidence_ts`
  - `confidence_score`

### 4.2 `qmt_local_assets`

- 层级：事实层
- 主键：`asset_id`
- 外键：`layout_id`
- 核心字段：
  - `userdata_path`
  - `datadir_path`
  - `data_path`
  - `datas_path`
  - `cfg_path`
  - `market_coverage`
  - `period_coverage`
  - `instrument_family_coverage`
- 质量字段：
  - `latest_market_day`
  - `latest_modified_at`
  - `readable_sample_rate`
  - `continuity_gap_rate`
  - `parse_failure_rate`
  - `integrity_flags[]`
  - `tick_score`
  - `m1_score`
  - `m5_score`
  - `d1_score`
  - `stability_score`
  - `read_speed_ms_p50`
  - `read_speed_ms_p95`

### 4.3 `qmt_account_probes`

- 层级：事实层
- 主键：`probe_id`
- 外键：`layout_id`、`channel_id`
- 核心字段：
  - `userdata_path`
  - `broker_id`
  - `account_id`
  - `account_type`
  - `login_status`
  - `reachable`
- probe 字段：
  - `probe_method`
  - `probe_latency_ms`
  - `raw_account_infos`
  - `probe_success`
  - `probe_error_code`
  - `probe_error_message`

### 4.4 `account_bindings`

- 层级：意图层
- 主键：`binding_id`
- 外键：`broker_account_id`、`probe_id`、`channel_id`、`asset_id`
- 核心字段：
  - `session_anchor_key`
  - `binding_scope`
  - `priority`
  - `manual_override`
  - `sticky_until`
  - `intent_source`
  - `change_reason`
  - `updated_by`
  - `updated_at`
- 风险字段：
  - `confidence_score`
  - `conflict_flags[]`
  - `approval_required`
  - `approval_state`

### 4.5 `gateway_sessions`

- 层级：运行态层
- 主键：`session_id`
- 会话锚点：`session_anchor_key = userdata_path`
- 核心字段：
  - `layout_id`
  - `userdata_path`
  - `connected_accounts[]`
  - `current_route_claims[]`
- 一等嵌套对象：`channel_profile`
  - `channel_id`
  - `broker_id`
  - `broker_guess`
  - `channel_kind`
  - `cfg_fingerprint`
  - `supported_account_types[]`
  - `server_endpoint_hint`
  - `login_entry_hint`
- 运行态字段：
  - `process_status`
  - `login_status`
  - `session_health`
  - `connected`
  - `authenticated`
  - `latency_ms_p50`
  - `latency_ms_p95`
  - `last_heartbeat_at`
  - `last_success_at`
  - `last_error`
  - `retry_count`

### 4.6 `route_policies`

- 层级：意图层
- 主键：`policy_id`
- 核心字段：
  - `policy_kind`
  - `purpose`
  - `market`
  - `period`
  - `instrument_family`
  - `account_id`
- 路由字段：
  - `preferred_candidates[]`
  - `fallback_candidates[]`
  - `score_formula_version`
  - `quality_thresholds`
  - `switch_policy`
- 防抖字段：
  - `min_hold_seconds`
  - `switch_cooldown_seconds`
  - `max_switch_per_day`
  - `route_freeze_until`

## 5. 状态枚举

- `layout_status`: `discovered | normalized | validated | rejected | stale`
- `asset_status`: `detected | profiled | scored | approved | degraded | quarantined | stale`
- `probe_status`: `pending | running | succeeded | partial | failed | stale`
- `binding_status`: `draft | proposed | confirmed | conflicted | disabled | rejected`
- `session_status`: `launch_ready | launching | process_alive | login_pending | connected | healthy | degraded | disconnected | retrying | quarantined | stale`
- `policy_status`: `draft | active | sticky | disabled | superseded`
- `freshness_state`: `fresh | stale | unknown`

## 6. 冲突矩阵

| 冲突码 | 触发条件 | 等级 | 系统动作 |
| --- | --- | --- | --- |
| `ACCOUNT_MULTI_USERDATA` | 同一 `trade_account` 出现在多个 `userdata` | `blocking` | 禁止自动确认绑定 |
| `USERDATA_MULTI_BROKER` | 同一 `userdata` 映射到多个券商 | `blocking` | 标记异常，不自动建会话 |
| `ASSET_MULTI_PRIMARY_HISTORY` | 同一资产被多个主历史策略宣称为主用 | `warning` | 不自动切换 |
| `CHANNEL_MULTI_PRIMARY_TRADE` | 同一通道被多个高优先交易策略争抢 | `manual_review_required` | 限制自动切换 |
| `LAYOUT_PATH_INCONSISTENT` | `exe/cfg/userdata` 指向不一致 | `blocking` | 标记布局无效 |
| `PROBE_MULTI_BINDING` | 一个 probe 账户被多个正式账户认领 | `blocking` | 禁止写正式绑定 |
| `SESSION_STALE_BUT_POLICY_ACTIVE` | 会话 stale 但仍为主交易路由 | `blocking` | 触发 route 复评估 |
| `PRIMARY_ROUTE_DEGRADED` | 主路由失健康 | `warning` | 尝试备用 |
| `AUTO_BIND_LOW_CONFIDENCE` | 自动绑定分数低于阈值 | `manual_review_required` | 仅给建议 |
| `MANUAL_OVERRIDE_CONFLICT` | 两个手工覆盖冲突 | `blocking` | 维持旧值不覆盖 |
| `ROUTE_SWITCH_THROTTLED` | 超出切换频率阈值 | `warning` | 冻结当前路由 |

## 7. 路由快照

`route_decision_snapshot` 作为单独审计对象，至少包含：

- `snapshot_id`
- `policy_id`
- `policy_version`
- `route_policy_version`
- `algorithm_version`
- `evaluated_at`
- `triggered_by_event`
- `purpose`
- `market`
- `period`
- `instrument_family`
- `account_id`
- `candidate_ids[]`
- `winner`
- `runner_up`
- `score_breakdown`
- `rejection_reasons[]`
- `decision_reason`
- `effective_from`
- `effective_to`
- `stale_at`

## 8. 策略消费者稳定接口

- 交易：
  - `resolve_trade_channel(account_id, purpose)`
  - `resolve_trade_session(account_id, purpose)`
  - `get_trade_route_snapshot(account_id, purpose)`
- 历史：
  - `resolve_history_asset(symbol, period, purpose)`
  - `resolve_history_source(symbol, period, purpose)`
  - `get_history_route_snapshot(symbol, period, purpose)`
- 校验：
  - `explain_route_decision(snapshot_id)`
  - `validate_binding(binding_id)`
  - `list_conflicts(scope)`

## 9. P0 / P0.5 改造锚点

### 9.1 后端

- `data_manager/datasource_discovery.py`
  - 现状：输出松散 QMT 候选与评分。
  - P0：补 `layout / asset` 投影所需字段，退役模糊路径语义。
- `easy_xt/load_config.py`
  - 现状：`qmt_path`、`qmt_exe_path`、`userdata_path` 混用。
  - P0：仅保留兼容映射，新语义以显式字段为准。
- `easy_xt/config.py`
  - 现状：配置层仍承接旧字段语义。
  - P0：输出新字段结构，旧字段仅做兼容。
- `core/api_server.py`
  - 现状：已有本地扫描、真实账户 probe、按 `userdata_path` 复用交易会话。
  - P0：统一读模型投影，保留旧接口兼容。
- `core/broker_accounts.py`
  - 现状：平台账户配置已稳定。
  - P0：保持配置职责，不让 probe 事实直接覆盖正式账户。

### 9.2 前端

- `apps/tauri-shell/src/components/data-interfaces/InterfaceRegistryPanel.tsx`
  - P0.5：先读 6 张模型摘要与冲突摘要，不急着做一键闭环。
- `apps/tauri-shell/src/components/system/AccountManagementPanel.tsx`
  - P0.5：收敛为平台账户视角 + binding draft 审阅入口。
- `apps/tauri-shell/src/components/workbench/BottomTerminalTabs.tsx`
  - P0.5：承接 sessions / policies / conflicts 摘要。

## 10. 当前仓库事实对齐

- `docs/21_interface_management_workbench_blueprint.md`
  - 已明确 `Workbench-first` 与 `gateway-sessions / route-policies` 蓝图方向。
- `docs/35_tauri_startup_login_and_migration_status_2026-04-11.md`
  - 已明确当前是主壳迁移态，不是 Qt 全量等价替代。
- `core/api_server.py`
  - 已具备 `discover_trading_accounts()` 与 `_get_trade_api()` 的主链基础。
- `data_manager/datasource_discovery.py`
  - 已具备本地扫描与基础评分主链。

## 11. 反模式

- 继续把 `qmt_path` 当主真相。
- 让 probe 结果直接覆盖正式账户配置。
- 让 session 状态无时间语义落库。
- 在未经过冲突判定时直接自动绑定。
- 用同一条“全局最佳路径”同时服务历史与交易。
- 让上层策略直接读取底层复杂对象而不走解析接口。

> 名义三层、实现一层，是本项目最大的架构风险。
