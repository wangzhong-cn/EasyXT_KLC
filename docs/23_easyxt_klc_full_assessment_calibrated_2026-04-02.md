# EasyXT_KLC 全量评估与行动指南（2026-04-02，v2.1 校准版）

> **基于当前代码快照、七层母本文档复核、关键子集 pytest 复跑，以及前后端最新实现状态的校准报告。**
> 版本：v2.1（校准版）
> 生成日期：2026-04-02
> 前序版本：`22_easyxt_klc_full_assessment_2026-04-02.md`（v2.0，阶段性评估版）

---

## 0. 这份校准版解决什么问题

v2.0 的大方向判断并没有错，但其中部分数字、部分完成度表述、以及若干根因归类，已经被当前代码与 pytest 结果证明需要校准。

本版的目标不是推翻 v2.0，而是把它从“阶段性强判断”收敛成“当前可直接引用的证据版结论”。

### 本版的 3 条使用规则

1. **保留 v2.0 的战略判断**：不需要 Rust / TypeScript 全量重构，继续走 Python 后端 + TypeScript/Tauri 前端 + Rust 壳的渐进式架构。
2. **回收过满表述**：凡是当前代码或 pytest 已直接推翻的结论，统一降级或改写。
3. **只引用本轮能复核的证据**：本版不再把历史全量执行数字直接当成“当前事实”。

---

## 1. 核心结论前置

### 一句话判断

> **制度层组件已经进入运行时，但项目还没有进入“完全闭环、可以宣告稳定”的阶段。当前最真实的状态是：制度层已立法，部分开始执法，关键接口、统一状态语义、测试清债和文档事实校准仍在进行中。**

### 我当前确认成立的四条总判断

1. **不需要全量重构。** 当前混合架构方向正确。
2. **制度层不是纸面方案。** `session_profile_registry`、`period_registry`、`threshold_registry`、`canonical_minute`、`governance_jobs` 已真实进入代码与测试。
3. **Qt 架构病判断依旧成立。** 42 个 `.dmp` 仍在，Qt 继续冻结、Tauri 继续接管是正确路线。
4. **当前最大维护债仍是 God Object。** 而且比 v2.0 报告时更大。

### 本轮校准后最重要的提醒

> **v2.0 不能再被当作“最新事实底稿”直接引用；它更适合作为阶段性判断稿，而 v2.1 才是当前推荐阅读版本。**

---

## 2. 七层公理化架构原文定位（校准后）

v2.0 将“七层体系文件”单点指向 `严谨审查结论与完备公理化体系构建.md`，这个方向不算错，但不够完整。

### 当前应采用的三层母本定位

| 文件 | 角色 | 当前判断 |
|------|------|----------|
| `结构量化交易体系底层架构与实操方案.txt` | **七层/八层总架构母本** | 最贴近“从科学哲学到算法工具精密定义”的原始总稿 |
| `严谨审查结论与完备公理化体系构建.md` | **形式化纠偏母本** | 负责 ε 阈值、对偶定理、三层分离公理化修订 |
| `四点三线 N 字原子结构理论 - 数学化与工具化落地手册txt.txt` | **N 字工程化手册** | 负责一阶逻辑、递推规则与工程实现映射 |

### 前端状态校准结论

> **如果只允许选一个“七层公理化架构从科学哲学到算法工具”的主文件，应优先指向 `结构量化交易体系底层架构与实操方案.txt`；其余两份应作为严格数学化与工程手册补充引用。**

---

## 3. 当前最新证据快照（2026-04-02 晚间校准）

以下数字全部来自本轮重新核对，不再沿用 v2.0 的历史口径。

| 指标 | 当前值 | 证据 |
|------|--------|------|
| pytest 收集总数 | **4483** | `python -m pytest tests/ --co -q` |
| 制度层核心回归 | **46 passed / 0 failed** | `test_session_profile_registry*`, `test_period_registry.py`, `test_threshold_registry.py`, `test_canonical_contracts.py`, `test_governance_jobs.py`, `test_custom_period_bars_cache.py` |
| 关键失败子集 | **211 collected / 196 passed / 15 failed** | `test_api_server_smoke.py`, `test_phase2_day3_account_api.py`, `test_trade_risk_integration.py`, `test_udi_hotpaths.py`, `test_unified_data_interface.py`, `test_unified_data_interface_extra3.py` |
| strategies + realtime 代表性样本 | **35 collected / 34 passed / 1 failed** | `test_strategy_factory.py`, `test_strategy_controller.py`, `test_unified_api_auto_recovery.py`, `test_minute_bar_aggregator.py`, `test_duckdb_sink.py` |
| `core/api_server.py` 行数 | **3377** | 当前仓库测量 |
| `data_manager/unified_data_interface.py` 行数 | **8372** | 当前仓库测量 |
| `gui_app/main_window.py` 行数 | **3072** | 当前仓库测量 |
| 根目录 `.dmp` 数量 | **42** | 当前仓库测量 |
| 主仓库 `warnings.filterwarnings(` 数量 | **13** | 仅统计 `core/`、`data_manager/`、`easy_xt/`、`gui_app/`、`strategies/`、`tools/`、`tests/` |
| `easy_xt/realtime_data/` Python 文件数 | **45** | 当前仓库测量 |
| `strategies/` Python 文件数 | **92** | 当前仓库测量 |
| `strategies/xueqiu_follow/` Python 文件数 | **31** | 当前仓库测量 |
| `strategies/jq2qmt/` Python 文件数 | **14** | 当前仓库测量 |

### 两个重要说明

1. **本轮没有重跑全量执行套件。** 所以 v2.0 中的 `4398 passed / 17 failed` 不应再继续作为“当前事实”引用。
2. **本轮已经重跑了最关键的失败子集。** 因此本文对当前残留问题的判断，是建立在最新失败面板上的。

---

## 4. 已经证实成立的结论

## 4.1 制度层组件已经进入运行时

以下组件当前都能在代码中定位，并且有针对性测试支撑：

| 组件 | 当前状态 |
|------|----------|
| `SessionProfileRegistry` | 已在 `data_manager/session_profile_registry.py` 中实现，真实配置测试通过 |
| `PeriodRegistry` | 已在 `data_manager/period_registry.py` 中实现并进入 UDI |
| `ThresholdRegistry` | 已在 `data_manager/threshold_registry.py` 中实现 |
| `canonical_minute` | 已在 `data_manager/canonical_minute.py` 中实现，且有 `CANONICAL_MINUTE_VERSION` |
| `governance_jobs` | 已具备 strict rebuild / receipt 校验逻辑 |
| `governance_hash` | 已进入 UDI 的 DDL、缓存键、读路径校验 |

### 架构路线结论

> **“制度层仍是文档概念”这句话已经不成立。**
>
> 更准确的说法是：**制度层组件已落地，但统一强制入口、外围测试与完整闭环尚未全部完成。**

## 4.2 Tauri 主线已经超过“骨架阶段”

当前 `apps/tauri-shell/src/App.tsx` 已经不是单页占位，而是一个包含以下主路由的容器：

- `workbench`
- `data`
- `structure`
- `heatmap`
- `ideas`
- `system`

### 当前已确认的前端进展

| 模块 | 当前状态 |
|------|----------|
| `DataRoute` | 已接治理概览、交易日历、traceability、repair 计划/receipt timeline |
| `WorkbenchRoute` | 已接 `fetchIngestionGateStatus()`、`triggerGolden1dRepair()`、事件桥状态、性能状态、质量状态 |
| `SystemRoute` | 已存在并纳入主导航 |
| `App.tsx` | 已形成 6 route 的统一壳层 |

### 校准结论

> **“WorkbenchRoute 只是空骨架”已经过时。**
>
> 更准确的说法是：**WorkbenchRoute 已进入真实联调阶段，但交易账户、统一状态语义与 Error Boundary 等仍待补齐。**

## 4.3 不需要全量重构的判断更稳了

当前仓库事实进一步强化了这个结论：

- 后端已有大量 FastAPI / UDI / 治理逻辑沉淀
- 前端已有多 Route、治理页、工作台、状态面板
- 图表链路已有质量状态、repair 触发、事件桥、性能面板
- Qt 已冻结，Tauri 已接管新增可视化方向

### Account API 结论

> **当前最需要的是“补齐缺口”，不是“更换语言”。**

---

## 5. v2.0 需要回调的关键结论

这一节是 v2.1 的核心价值：把 v2.0 里已经过期、过满或不够精确的地方，明确校准掉。

| v2.0 表述 | v2.1 校准 | 证据 |
|-----------|-----------|------|
| 七层体系文件 = `严谨审查结论与完备公理化体系构建.md` | 应升级为“三份母本并引”：总架构母本 + 形式化纠偏母本 + N 字工程手册 | 当前三份原文已复核 |
| `4419` collected / `4398` passed / `17` failed 是当前事实 | 当前只确认 `4483` collected；本轮未重跑全量执行，因此不再直接引用旧的全量通过/失败数字 | 当前 collect-only + 本轮定向回归 |
| `api_server.py` 2693 行 | 当前是 **3377 行** | 当前仓库测量 |
| `unified_data_interface.py` 8059 行 | 当前是 **8372 行** | 当前仓库测量 |
| Account API 是“待补小尾巴” | 当前仍只有 `GET/DELETE /api/v1/accounts/{account_id}`，`GET/POST /api/v1/accounts/` 仍缺失 | 当前 `api_server.py` + 账户测试失败 |
| Trade risk 失败主要依赖 Account API 级联 | 当前 4 个 trade risk 失败更直接指向 `easy_xt/trade_api.py` 落到 `order_future` 路径 | 当前 `test_trade_risk_integration.py` 失败信息 |
| `easy_xt/realtime_data/` 与 `strategies/` 整包零覆盖 | 这个表述过宽；更准确是“已有代表性测试，但高风险子树仍明显偏薄” | 当前 34/35 代表样本通过；grep 命中多个相关测试 |
| Canonical 层缺失 | 当前应改为“Canonical 1m 基础层已建立，但尚未提升为不可绕过的统一入口” | `canonical_minute.py` + UDI 接入 |
| Tauri 仅骨架阶段 | 当前应改为“DataRoute / WorkbenchRoute 已进入真实联调，但统一状态语义仍未完全收口” | `App.tsx`、`DataRoute.tsx`、`WorkbenchRoute.tsx` |

---

## 6. 当前确认仍未收口的问题基线

## 6.1 Account API 仍然只补了一半

### 当前代码现状

在 `core/api_server.py` 中，当前可确认存在：

- `GET /api/v1/accounts/{account_id}`
- `DELETE /api/v1/accounts/{account_id}`

但当前仍缺：

- `GET /api/v1/accounts/`
- `POST /api/v1/accounts/`

### 当前测试证据

以下测试仍然直接失败：

- `tests/test_api_server_smoke.py`
- `tests/test_phase2_day3_account_api.py`

失败表现集中为：

- 访问 `/api/v1/accounts/` 返回 `404`
- create/list/upsert 相关用例不通过

### TradeAPI 结论

> **Account API 不是历史问题，而是当前问题。**

## 6.2 TradeAPI 的股票/期货路径仍有兼容问题

当前 4 个 trade risk 失败并不是“只是账户 API 连锁反应”，而是更直接地表现为：

- `easy_xt/trade_api.py` 的 `buy()` / `sell()` 在 mock 股票 backend 场景下落到 `order_future`
- 报错为：`期货接口不可用，请确认连接的是期货账户`

### UDI 结论

> **这条线的当前根因优先级应上移到 `trade_api.py`，而不是继续把它笼统归为 Account API 级联。**

## 6.3 UDI 仍有 4 个关键红点

本轮最新关键子集回归中，UDI 相关仍失败 4 项：

1. `test_derived_period_cache_key_changes_with_governance_signature`
2. `test_circuit_breaker_open_returns_cached_data`
3. `test_registry_third_party_exception_returns_empty`
4. `test_wal_error_reconnects_and_retries_successfully`

### 当前根因拆分

- **3 项**是 `_evaluate_cross_source_gate()` 对 `MagicMock` 数值比较不稳，导致 `TypeError`
- **1 项**是 derived-period governance key 语义与旧测试预期不一致，需要明确到底以 registry 为准，还是继续允许 env override 驱动 key 变化

### 安全与可观测性结论

> **UDI 不是“只剩风格问题”，而是仍有会直接影响回归结果的真实收口项。**

## 6.4 安全与可观测性债务依旧存在

当前仍能直接确认：

- `CORSMiddleware` 仍为 `allow_origins=["*"]`
- WebSocket 市场订阅仍通过 `token: str = Query(default="")` 读取 token
- 主仓库源码中仍有 **13** 处 `warnings.filterwarnings(`
- 根目录 `.dmp` 仍为 **42** 个

### God Object 结论

> **这部分不属于文档口径问题，而是当前仍存在的运行时债务。**

## 6.5 God Object 比 v2.0 时更严重

| 文件 | 当前行数 | 当前判断 |
|------|----------|----------|
| `core/api_server.py` | **3377** | 比 v2.0 继续膨胀，治理接口增多但未拆分 |
| `data_manager/unified_data_interface.py` | **8372** | 当前全仓最大维护债 |
| `gui_app/main_window.py` | **3072** | Legacy 冻结，不建议再投入拆分预算 |

### 测试覆盖校准结论

> **God Object 不是“仍待处理”，而是“正在继续增长”。**

---

## 7. 测试盲区表述的精确化

v2.0 中“`easy_xt/realtime_data/` 和 `strategies/` 零覆盖”的表述过于粗糙，当前应改为更精确的版本。

## 7.1 当前不能再说“整包零覆盖”

本轮代表性样本测试显示：

- `tests/test_strategy_factory.py` ✅
- `tests/test_strategy_controller.py` ✅
- `tests/test_unified_api_auto_recovery.py` ✅
- `tests/test_minute_bar_aggregator.py` ✅
- `tests/test_duckdb_sink.py` ❌（失败原因是 `pytdx` 缺失，不是断言逻辑失败）

此外，测试目录中还能直接 grep 到：

- `easy_xt.realtime_data.monitor.*`
- `easy_xt.realtime_data.persistence.*`
- `easy_xt.realtime_data.unified_api`
- `strategies.base_strategy`
- `strategies.registry`
- `strategies.strategy_controller`
- 多个 native / example / management 策略测试

### 结论

> **不能再写“整包零覆盖”。**

## 7.2 当前真正的高风险盲区

更准确的高风险子树应是：

| 子树 | Python 文件数 | 当前风险判断 |
|------|---------------|--------------|
| `easy_xt/realtime_data/websocket_server.py` / `push_service.py` / `service_manager.py` | 属于 `realtime_data/` 45 文件主链 | 核心服务链仍需针对性冒烟测试 |
| `strategies/xueqiu_follow/` | **31** | 实盘跟单子系统，测试明显偏薄 |
| `strategies/jq2qmt/` | **14** | 中转/桥接链路，测试明显偏薄 |

### 补充说明

`tests/test_duckdb_sink.py` 当前有 1 个失败，根因是：

- `easy_xt.realtime_data.providers.tdx_provider` 依赖 `pytdx`
- 当前环境缺该包，触发 `ModuleNotFoundError`

这说明当前代表性测试并非完全空白，但**环境依赖与服务链路径的稳定性仍需补强**。

---

## 8. 前端最新状态（v2.1 校准）

## 8.1 DataRoute 已经不只是“治理骨架”

当前 `DataRoute` 已承载：

- 治理概览
- Trading calendar
- Traceability
- Golden 1D repair plans
- Receipt timeline / reject reason / trend

## 8.2 WorkbenchRoute 已进入真数据联调阶段

当前 `WorkbenchRoute` 已明确接入：

- `fetchIngestionGateStatus()`
- `triggerGolden1dRepair()`
- 图表运行态快照
- 事件桥状态
- Gate / quality / replayable / lineage 状态
- repair plan 与后台编排操作
- 性能专项状态

### 前端能力校准结论

> **前端当前真正缺的不是“有没有页面”，而是“统一状态语义与最终用户可解释性是否完全收口”。**

## 8.3 `/api/v1/data/coverage/{symbol}` 仍未出现，但状态能力已部分存在

这点需要更精确地说：

- **统一 coverage 端点仍未出现** —— 这一点 v2.0 没说错
- 但当前并不是完全没有状态语义能力，因为已有：
  - `/api/v1/data-quality/ingestion-status`
  - `/api/v1/chart/bars` 的 `quality`
  - DataRoute / WorkbenchRoute 的状态消费逻辑

### 状态 API 结论

> **当前问题不是“完全没有状态 API”，而是“缺少统一的 per-symbol coverage 契约与单入口服务”。**

---

## 9. 最新行动基线（按优先级）

## 9.1 P0：本周必须收口

1. **补齐 Account API**
   - 新增 `GET /api/v1/accounts/`
   - 新增 `POST /api/v1/accounts/`
2. **修复 TradeAPI 股票/期货路径兼容**
   - 避免 mock 股票 backend 落入 `order_future`
3. **修复 UDI 的 3 个 `MagicMock` 数值比较错误**
   - `_evaluate_cross_source_gate()` 对 mocked numeric 值做显式数值化/兜底
4. **澄清 governance key 语义**
   - 决定 derived-period cache key 以 registry 为准还是继续支持 env override 影响 key
5. **收安全与可观测性债务**
   - WS token 改 Header / subprotocol
   - CORS 收窄
   - 逐步清理 `warnings.filterwarnings(`

## 9.2 P1：1-2 周内完成

1. **`api_server.py` 按 APIRouter 拆分**
2. **`unified_data_interface.py` 按缓存 / gate / ingestion 三职责拆分**
3. **把 canonical gate 提升为不可绕过入口**
4. **统一前端状态服务**
   - 统一 `ingestion-status`、`chart quality`、DataRoute、WorkbenchRoute 的状态语义
5. **补高风险子树测试**
   - `xueqiu_follow`
   - `jq2qmt`
   - realtime 三核心服务文件

## 9.3 P2：2-4 周内完成

1. `daily_rights_factor`
2. `period_factor_snapshot`
3. `structure_evidence`
4. `structure_snapshot`
5. `repair_receipt` / `replay_receipt` / `publish_gate_receipt`
6. registry-driven CI / release gate

---

## 10. 最终判断（校准版）

### 10.1 当前最准确的项目阶段描述

> **EasyXT_KLC 现在已经不是“制度层未落地”的项目，而是“制度层主组件已落地，但运行时闭环、统一入口、关键测试与文档校准尚未全部完成”的项目。**

### 10.2 当前最值得肯定的部分

- 七层 / 公理化相关母本文档链条完整
- 制度层五件套已进入代码与测试
- Tauri 不再是纯验证壳，而是已经承接多条业务可视化主线
- 不需要全量重构的判断，当前比之前更稳

### 10.3 当前最不能继续模糊处理的部分

- Account API 实际完成度
- TradeAPI 当前失败根因
- God Object 继续增长
- 安全与可观测性债务
- “整包零覆盖”这种不精确表述

### 10.4 最后一条结论

> **v2.0 适合作为阶段性判断稿；v2.1 才是当前应对外引用的证据版评估。**
>
> 现在最缺的不是再写一份更宏大的路线图，而是把已经找到的缺口——账户接口、TradeAPI 路径、UDI 红点、统一状态语义和高风险子树测试——一刀一刀收干净。

---

## 附：本版引用的直接证据来源

- 七层/公理化原文：
  - `结构量化交易体系底层架构与实操方案.txt`
  - `严谨审查结论与完备公理化体系构建.md`
  - `四点三线 N 字原子结构理论 - 数学化与工具化落地手册txt.txt`
- 当前前端状态：
  - `apps/tauri-shell/src/App.tsx`
  - `apps/tauri-shell/src/routes/DataRoute.tsx`
  - `apps/tauri-shell/src/routes/WorkbenchRoute.tsx`
- 当前后端/数据层状态：
  - `core/api_server.py`
  - `data_manager/unified_data_interface.py`
- 当前测试证据：
  - `tests/test_api_server_smoke.py`
  - `tests/test_phase2_day3_account_api.py`
  - `tests/test_trade_risk_integration.py`
  - `tests/test_udi_hotpaths.py`
  - `tests/test_unified_data_interface.py`
  - `tests/test_unified_data_interface_extra3.py`
  - `tests/test_session_profile_registry.py`
  - `tests/test_session_profile_registry_real_config.py`
  - `tests/test_period_registry.py`
  - `tests/test_threshold_registry.py`
  - `tests/test_canonical_contracts.py`
  - `tests/test_governance_jobs.py`
  - `tests/test_custom_period_bars_cache.py`
  - `tests/test_strategy_factory.py`
  - `tests/test_strategy_controller.py`
  - `tests/test_unified_api_auto_recovery.py`
  - `tests/test_minute_bar_aggregator.py`
  - `tests/test_duckdb_sink.py`
