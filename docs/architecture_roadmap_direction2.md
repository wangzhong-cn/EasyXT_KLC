# EasyXT 架构演进路线图：方向2（全链路管控/量化中台）→ 方向3（云原生）

> 生成日期: 2026-03
> 基于: 85K+ 行代码、500+ 文件深度审计结果

---

## 一、当前架构评估结论（2026-03-08 更新）

### 1.1 架构选择结论

**结论保持不变：在 QMT (MiniQMT) 单进程约束下，EasyXT + lightweight-charts 仍是当前最优组合。**

| 维度 | 当前状态 | 说明 |
|------|---------|------|
| QMT 兼容性 | ✅ 最优 | xtquant 仍是 QMT 唯一 Python 接入口，单进程/线程约束明确 |
| 图表渲染 | ✅ 可生产 | LWC + PyQt5 嵌入链路稳定，已具备降级与健康监控机制 |
| 数据持久化 | ✅ 可生产 | DuckDB 连接池、WAL 修复、统一数据接口已落地 |
| 风控审计 | ✅ 已接入 | RiskEngine + AuditTrail 已贯通策略/交易关键链路 |
| 可观测性 | ✅ 灰度可用 | `/health` 已包含 drop_rate_1m / drop_alert / queue_len / publish_latency(avg/max) |
| 测试与质量门禁 | ✅ 达标 | 覆盖率已达 40%+，全量回归稳定（持续 0 failed） |

### 1.2 里程碑完成度

| 阶段 | 状态 | 核心结果 |
|------|------|---------|
| Phase 0 基础加固 | ✅ 已关闭 | 覆盖率门槛达成、治理门禁落地、测试污染根因修复 |
| Phase 1 轻量中台雏形 | ✅ 基本完成 | FastAPI + WS 广播 + 鉴权限流 + 契约冻结 + 回放测试 |
| Phase 2 机构化能力 | 🟡 未全面启动 | 多账户治理/组合级风控/审批流仍待系统化落地 |
| Phase 3 云原生预留 | 🟡 进行中 | 接口抽象与可观测先行，容器化与服务化按需推进 |

### 1.3 已关闭的关键技术债

| 类别 | 结果 |
|------|------|
| 全局状态污染 | ✅ `data_manager` 模块污染已修复，新增专项回归测试 |
| 导入语义坑位 | ✅ `xtquant.xtdata` 导入改为 `importlib`，测试 helper 统一封装 |
| 监控盲区 | ✅ 新增 `publish_latency_max_ms` 与 `drop_rate_1m` 告警语义 |
| 契约治理 | ✅ OpenAPI 基线冻结 + 变更审计日志 + 并发写保护 |
| 灰度运行手册 | ✅ `gray_monitor.ps1` 可直接落地执行（含 DryRunOnce、连续 critical 判定） |

---

## 二、方向 2 执行路线图：全链路管控 / 量化中台（滚动更新）

### Phase 0：基础设施加固（已完成）

**状态**：✅ 关闭  
**完成判据**：覆盖率 ≥ 40%、全量回归稳定、治理门禁可运行。

### Phase 1：策略治理层（当前主线）

**目标**：完成“可治理”到“可验收”闭环，支持小流量灰度实盘。

```
┌──────────────────────────────────────────────────────────┐
│                    策略治理层 (新增)                       │
├──────────────┬───────────────┬───────────────────────────┤
│ 策略注册中心  │ 策略状态机     │ 策略版本/审计             │
│ (registry)   │ (state FSM)   │ (versioning + audit)      │
├──────────────┴───────────────┴───────────────────────────┤
│                    风控引擎 (新增)                         │
├──────────────┬───────────────┬───────────────────────────┤
│ 预交易风控    │ 持仓限额管理   │ 回撤熔断器               │
│ (pre-trade)  │ (position)    │ (drawdown circuit)        │
├──────────────┴───────────────┴───────────────────────────┤
│              现有层：EasyXT + LWC + DuckDB                │
└──────────────────────────────────────────────────────────┘
```

| 任务 | 状态 | 交付物 |
|------|------|--------|
| P1-1: 策略基类标准化 | ✅ | `strategies/base_strategy.py` 生命周期与风控/审计钩子 |
| P1-2: 风控前检接入交易链路 | ✅ | `easy_xt/trade_api.py` 前置风控检查与拒单路径 |
| P1-3: 策略注册中心 | ✅ | `strategies/registry.py`（状态机、快照、查询） |
| P1-4: API/WS 中台雏形 | ✅ | `core/api_server.py`（鉴权、限流、广播、健康检查） |
| P1-5: QMT 回调桥接 | 🟡 | `_on_tick -> ingest_tick_from_thread -> WS` 已打通，灰度基线持续采样中 |
| P1-6: 实盘灰度 runbook | ✅ | `gray_monitor.ps1` + 决策矩阵（连续判定） |

### Phase 2：机构级能力（下一阶段）

**目标**：多账户治理 + 组合级风控 + 审批流 + 数据对账合规闭环。

| 任务 | 优先级 | 交付物 |
|------|--------|--------|
| P2-1: 账户注册中台 | P0 | 多 QMT 实例/多账户净值同步与隔离模型 |
| P2-2: 组合级风险引擎 | P0 | 组合 VaR、跨账户限额、账户维度告警 |
| P2-3: 策略灰度发布 | P1 | 策略版本/审批/发布与回滚闭环 |
| P2-4: 数据溯源与对账 | P1 | 审计链查询 API + T+1 自动对账 |

### Phase 2 启动周执行计划（按天拆分，可直接执行）

| 天数 | 当日目标 | 交付物 | 验收命令 |
|------|---------|--------|---------|
| Day 1 | 账户域模型落地 | `core/account_registry.py`（账户注册、状态、标签） | `python -m pytest tests/ -k "account_registry" -q` |
| Day 2 | 多账户净值同步 | `services/account_sync.py`（QMT→内存/DB 同步） | `python -m pytest tests/ -k "account_sync" -q` |
| Day 3 | 账户维度风控聚合 | `core/risk_engine.py` 扩展 account_id 维度聚合 | `python -m pytest tests/test_risk_engine.py -q` |
| Day 4 | 组合级 VaR 与限额 | `core/portfolio_risk.py`（组合 VaR、集中度） | `python -m pytest tests/ -k "portfolio_risk" -q` |
| Day 5 | 策略状态/审批骨架 | `core/api_server.py` 增加审批流最小端点 | `python -m pytest tests/test_api_server.py -q` |
| Day 6 | 数据溯源与 T+1 对账骨架 | `tools/daily_audit.py` 扩展对账输出字段 | `python -m pytest tests/ -k "daily_audit" -q` |
| Day 7 | 集成回归与灰度准入评审 | Phase 2 启动周报告（Markdown） | `python -m pytest tests/ -q` |

**启动周退出标准**：
- 多账户注册/同步链路可运行（至少 2 个账户模拟数据）
- account_id 维度风险统计可查询（API 或日志可见）
- 组合级 VaR 指标可输出并可被健康检查读取
- 全量回归保持 0 failed

### Phase 3：云原生与接口开放（按需推进）

**目标**：在不破坏现有桌面稳定性的前提下，逐步服务化。

| 任务 | 优先级 | 交付物 |
|------|--------|--------|
| P3-1: API 网关化 | P1 | 策略/风控/订单统一 API 边界 |
| P3-2: 指标与追踪 | P1 | Prometheus 指标 + 链路追踪（signal→order→fill） |
| P3-3: 部署标准化 | P2 | Docker Compose/K8s 试运行模板 |

---

## 三、方向 3 接口预留（云原生演进）

### 3.1 抽象接口设计原则

方向2 建设时预留以下接口，使未来迁移到云原生架构时不需要重写核心逻辑：

```python
# 1. 数据源抽象 — 已有 datasource_registry.py，扩展为协议
class DataSourceProtocol(Protocol):
    def get_kline(self, code: str, period: str, count: int) -> pd.DataFrame: ...
    def get_realtime(self, codes: list[str]) -> list[dict]: ...
    # 云原生时: 实现 GrpcDataSource / KafkaDataSource

# 2. 交易通道抽象 — 已有 TradeAPI，提取接口
class BrokerProtocol(Protocol):
    def buy(self, code: str, volume: int, price: float) -> Optional[int]: ...
    def sell(self, code: str, volume: int, price: float) -> Optional[int]: ...
    # 云原生时: 实现 CtpBroker / SimBroker / PaperBroker

# 3. 事件总线抽象 — 已有 signal_bus.py
class EventBusProtocol(Protocol):
    def emit(self, event: str, data: Any) -> None: ...
    def on(self, event: str, handler: Callable) -> None: ...
    # 云原生时: 替换为 Redis Pub/Sub / NATS / Kafka

# 4. 存储抽象 — 新增
class StorageProtocol(Protocol):
    def save_strategy_state(self, strategy_id: str, state: dict) -> None: ...
    def load_strategy_state(self, strategy_id: str) -> Optional[dict]: ...
    # 当前: DuckDB 实现; 云原生时: PostgreSQL / Redis
```

### 3.2 云原生演进路径

```
Phase A (方向2完成后):
  EasyXT (单机) → API Server (FastAPI) → 前端 (LWC standalone)
  - 将 EasyXT 核心逻辑包装为 REST/WebSocket API
  - LWC 独立运行在浏览器，通过 WS 接收数据
  - DuckDB → PostgreSQL + TimescaleDB

Phase B (QMT 开放后):
  QMT 云 API → 策略引擎集群 (K8s) → 前端 (LWC + 低代码)
  - 策略容器化，每策略一个 Pod
  - 风控引擎独立服务化
  - Kafka 事件流替代 signal_bus
```

---

## 四、优先级总览

| 阶段 | 核心交付 | 前置条件 | 估计复杂度 |
|------|---------|---------|-----------|
| **Phase 0** | 基础加固 + 测试覆盖 40% | 无 | 低 |
| **Phase 1** | 策略治理 + 风控引擎 | Phase 0 | 高 |
| **Phase 2** | 数据质量 + 合规审计 | Phase 0 | 中 |
| **Phase 3** | 监控可观测性 | Phase 1 | 中 |
| **方向3 Phase A** | API Server + 前端分离 | Phase 1-3 | 高 |
| **方向3 Phase B** | 完全云原生 | QMT 开放云 API | 极高 |

---

## 五、当前还缺什么？（按当前状态重排）

| 缺口 | 严重程度 | 说明 |
|------|---------|------|
| ⚠️ 多账户治理未完成 | **关键** | account_id 维度的净值同步、风险隔离、监控聚合尚未系统化 |
| ⚠️ 组合级风控未服务化 | **关键** | 当前以单策略/单账户为主，组合 VaR 与统一限额仍需中台化 |
| ⚠️ 策略管理 UI 仍偏基础 | 高 | 生命周期可查询，但可视化运维与审批流未完成 |
| ⚠️ 数据血缘/对账自动化不足 | 中 | 审计链可用，但 T+1 自动对账与追溯 API 需补齐 |
| ℹ️ 非核心目录历史技术债 | 低 | 学习实例/外部目录仍有异常处理粗糙点，不阻断主链路 |

---

## 六、硬约束（生产不可跨越的红线）

> 以下 4 条约束是**不可协商的**生产边界，任何违反均须立即回滚，与功能里程碑无关。

| 约束编号 | 约束名称 | 规则内容 | 违反后果 |
|---------|---------|---------|---------|
| **HC-1** | **发布冻结规则** | 交易时段（09:15–15:00）**禁止**部署任何结构性改动，包括：通信协议变更、线程模型变更、风控核心逻辑变更。非结构性 hotfix（如日志级别、UI 文本）需单独评审后方可豁免 | 立即回滚；记录违规事件；调查责任链 |
| **HC-2** | **回滚 SLA** | 任何告警触发阈值后，**≤ 30 秒**内必须完成自动降级或回滚。当前降级机制：`_enter_degraded_mode()`（刷新间隔翻倍）→ 若持续 5 分钟仍告警，自动回滚配置到上一版本快照 | 触发 P0 事故报告；纳入月度 SLO 燃尽统计 |
| **HC-3** | **时钟统一** | 全链路时间戳**统一使用 UTC epoch milliseconds（`int`）**，禁止使用本地时区字符串（`"2026-02-24 14:30:00"`）作为事件时间。适用范围：审计日志（`core/audit_trail.py`）、风控事件、图表事件（`core/events.py`）、DuckDB 落库字段（审计类表 `created_at/submitted_at/filled_at`）。调试展示层可做本地化转换，但**存储层禁止** | 数据修复成本极高；拒绝合并含本地时区字符串的审计字段 |
| **HC-4** | **许可合规门禁** | `lightweight-charts-python`（louisnw01）使用 **MIT License**（非 Apache 2.0）；其依赖的底层 `lightweight-charts`（TradingView）使用 Apache 2.0，需在 NOTICE 中归因。CI 门禁（`tools/check_license_compliance.py`）要求：① `external/lightweight-charts-python/LICENSE` 必须存在且包含 "MIT License" 及版权声明；② `external/lightweight-charts-python/NOTICE` 推荐存在（含 TradingView Apache 2.0 归因）；③ 每次更新 LWC 版本时，CI 必须验证 LICENSE/NOTICE 同步更新；④ 禁止在任何发布包中删除或修改许可证内容 | CI 失败阻断发布；法务风险 |

### 6.1 发布冻结期间的紧急流程

```
紧急 hotfix 申请（交易时段内）:
  1. 工程师提交 [EMERGENCY] 标题的 PR，描述变更范围
  2. 技术负责人 + 风控负责人双重 approve（≥ 2 人）
  3. 确认变更不涉及：通信协议 / 线程模型 / 风控逻辑
  4. 在隔离沙盒环境验证后，滚动部署（不中断现有连接）
  5. 部署后 15 分钟内确认无告警，否则立即回滚
```

### 6.2 回滚触发条件（HC-2 细化）

| 触发事件 | 自动动作 | 人工确认 |
|---------|---------|---------|
| `sustained_drop_alert == True` 持续 ≥ 30s | 进入降级模式（`_enter_degraded_mode`） | 无需 |
| 降级状态持续 ≥ 5 分钟 | 推送告警至 `alerts.log` + AlertManager | OnCall 工程师确认降级是否足够 |
| 风控 HALT 事件 | 停止下单信号，保留仓位 | 风控负责人手动解除 |
| 进程崩溃（`STATUS_STACK_BUFFER_OVERRUN`） | 进程守护程序自动重启（30s 内） | SRE 确认重启原因 |

---

## 七、制度化运营

> 以下两项制度是**免疫系统**的强制执行机制，确保风险不随时间累积漂移。

### 7.1 故障演练（每周固定）

**目的**：验证"可控、可回滚"机制在真实场景下可用，而非仅停留在文档。

**执行频率**：每周固定时间窗（建议周五下午盘后，15:30–17:00）

**演练脚本覆盖的 4 个场景**：

| 场景编号 | 场景名称 | 演练方式 | 验收标准 |
|---------|---------|---------|---------|
| **D-1** | WS 断连恢复 | 手动断开 WebSocket 连接，观察重连 | ≤ 5s 内自动重连，图表不冻结 |
| **D-2** | DuckDB 锁超时 | 在读取时强制写入锁，触发超时 | ≤ 3s 内锁超时释放，不死锁；告警推送成功 |
| **D-3** | 风控 HALT | 触发日内回撤熔断（mock 价格跌超阈值） | 下单信号停止；仓位保持不变；HALT 事件可在日志中追溯 |
| **D-4** | 线程强杀后恢复 | 模拟 `_RealtimeConnectThread` 卡死（mock `connect_all()` 阻塞 10s） | `THREAD_FORCED_TERMINATE` 事件发出；30s 内系统重新初始化成功 |

**演练记录要求**：每次演练输出《演练报告》（Markdown），包含：触发时间、预期结果、实际结果、与上次的差异、改进项。存放于 `docs/drill_reports/YYYY-MM-DD.md`。

**升级标准**：连续 2 次演练中同一场景未通过 → 列为 P1 事项，下周一前修复。

---

### 7.2 风险参数校准闭环（每月）

**目的**：通过历史回放对风控阈值进行再标定，避免阈值因市场环境变化而产生"风控漂移"（误拦截正常交易 / 漏拦截真实风险）。

**执行频率**：每月第一个交易周结束后（建议每月第4个周五）

**校准流程**：

```
每月校准流程:
  1. 数据拉取：拉取上月全量交易记录 + 实盘行情（DuckDB 审计表 + xtquant 历史）
  2. 历史回放：在 backtrader 框架中以当前风控参数重跑上月所有交易信号
  3. 指标计算：
     - 误拦截率 = 被风控拦截的"正常交易" / 总触发风控次数
     - 漏拦截率 = 未被风控拦截的"风险交易" / 总风险事件数
  4. 阈值分析：绘制 (误拦截率, 漏拦截率) 的 ROC 曲线，选择最优工作点
  5. 参数更新：若任一指标偏离目标值 ±20%，提交参数变更 PR
  6. 审批：风控参数变更需技术负责人 + 风控负责人双重 approve
  7. 记录：输出《月度风控校准报告》，存放于 docs/risk_calibration/YYYY-MM.md
```

**目标阈值**：

| 指标 | 长期目标 | 告警线（需校准） |
|------|---------|----------------|
| 误拦截率 | ≤ 5% | > 10% |
| 漏拦截率 | ≤ 2% | > 5% |
| 单月风控 HALT 次数 | ≤ 3 次 | > 10 次 |

**防止漂移的关键**：阈值不仅依赖于策略参数，还受市场波动率影响。当市场进入高波动期（如指数单日振幅 > 3%），需对阈值做临时宽松处理并记录。

### 7.3 CI 文档同步检查

**目的**：防止"改了代码、没改规范"导致文档与实现背离。

**规则**：在 CI 中检查以下关键模块改动时，PR 必须附带以下之一：
- 对应规范文档的更新（diff 可见）
- PR 说明中明确写明豁免理由（`[doc-exempt] 理由`）

| 关键模块 | 对应规范文档 |
|---------|------------|
| `gui_app/widgets/kline_chart_workspace.py`（QThread 相关） | `docs/05_thread_exit_safety_spec.md` |
| `core/audit_trail.py` | `docs/architecture_roadmap_direction2.md` 九、审计不可抵赖 |
| `core/events.py` | 使用该事件的规范文档 |
| `core/safe_thread_runner.py` | `docs/05_thread_exit_safety_spec.md` 第九节 |

**实现**：CI 增加 `scripts/check_doc_sync.py` — 检测 modified files 中是否包含受控模块，若有则验证 PR 是否附带文档变更或豁免声明。

### 7.4 演练结果治理闭环（incident_id 机制）

**目的**：确保演练失败项不沉默，阻断下一次结构性发布。

**机制**：
1. 每次 D-1~D-4 演练失败，生成 `incident_id`（格式：`DRILL-YYYYMMDD-D{场景号}`）
2. 未关闭的 `incident_id` 自动记录到 `docs/drill_reports/open_incidents.md`
3. **结构性发布（L2/L3 变更）部署前，CI 检查 `open_incidents.md` 是否为空**；若有未关闭项则阻断发布
4. 演练修复验证通过后，工程师手动关闭 `incident_id`（在 `open_incidents.md` 标记 `[CLOSED]`）

---

## 八、变更风险分级（L1/L2/L3）

> 补充 HC-1（发布冻结）的操作细则：通过分级避免"一刀切"，精确匹配审批和回归深度。

### 8.1 三级分类定义

| 等级 | 名称 | 变更范围示例 | 交易时段限制 |
|------|------|------------|-------------|
| **L1** | 配置变更 | 日志级别、UI 文本、环境变量阈值、feature flag | **允许**（无需审批，但需回滚方案） |
| **L2** | 逻辑变更 | 业务逻辑、算法、策略参数、数据库 schema | **禁止**（需盘前/盘后部署，1人 approve） |
| **L3** | 协议变更 | 通信协议、线程模型、风控核心逻辑、DuckDB DDL 变更 | **严格禁止**（需双重 approve + 完整回归） |

### 8.2 每个等级的回归深度要求

| 等级 | 最低回归要求 | SLO 联动 |
|------|------------|---------|
| **L1** | 变更模块的单元测试通过 | 不消耗 error budget |
| **L2** | 全量 `pytest tests/ -q` 通过 + 5 分钟冒烟测试 | 消耗 error budget（按影响时间计） |
| **L3** | 全量回归 + TestKLineWorkspaceExitStability + 故障演练 D-1~D-4 全部通过 | 消耗 error budget；触发月度校准 |

### 8.3 分级判定规则（自动化检查基础）

```
L3 关键词（任一匹配 → 自动升级为 L3）:
  - 修改文件包含: kline_chart_workspace.py (QThread 相关行)
  - 修改文件包含: core/events.py (新增/删除事件)
  - 修改文件包含: core/audit_trail.py (_DDL 变量)
  - PR title 包含: [thread-model], [protocol], [risk-engine]

L2 关键词（任一匹配 → 自动升级为 L2，除非已是 L3）:
  - 修改文件包含: strategies/, core/risk_engine.py
  - 修改文件包含: data_manager/unified_data_interface.py
  - 变更行数 >= 100（启发式，供人工复核）

否则: L1
```

---

## 九、高价值增强专项

### 9.1 审计不可抵赖（已实施：2026-03）

**实施内容**（见 `core/audit_trail.py`）：

| 字段 | 类型 | 语义 | 验证方式 |
|------|------|------|---------|
| `entry_hash` | VARCHAR | SHA-256（核心字段）| 字段级篡改检测 |
| `prev_hash` | VARCHAR | 前一条记录的 `entry_hash` | `verify_chain_integrity()` 链式检测 |
| `batch_hash` | VARCHAR | 同批次关联写入的组合哈希 | 跨表关联验证（signal→order→fill） |
| `sig_version` | INTEGER | 审计链规范版本（0=旧记录，1=v1） | 版本切换时的向后兼容保护 |

**不可抵赖性保证**：
- 删除任意一条记录 → 后续所有记录的 `prev_hash` 断链（可通过 `verify_chain_integrity()` 检测）
- 修改任意字段 → `entry_hash` 校验失败
- 跨表关联篡改 → `batch_hash` 不一致（fills 的 `batch_hash` 包含 signal_id）
- 监管核验时通过 `sig_version` 定位适用的验证规则

**定期运行**（建议月度 T+1 对账时调用）：
```python
result = audit_trail.verify_chain_integrity()
# 输出: {"ok": True/False, "signals": {"tampered": 0, "chain_breaks": 0, ...}, ...}
```

---

### 9.2 数据契约测试（Schema Contract Test）

**目标**：防止 EasyXT→中间层→LWC 消息体字段漂移导致线上隐性故障（字段改名/类型变更/删除后前端静默丢数据）。

**受保护的数据流边界**：

```
EasyXT (xtquant)  →  UnifiedDataAPI  →  RealtimePipeline  →  LWC (chart.update)
      ↑                    ↑                   ↑                    ↑
  contract_v1.json    contract_v2.json    contract_v3.json    contract_v4.json
```

**实施路径**：

1. **定义 contract schema**（JSON Schema 格式）：

```json
// config/contracts/realtime_quote_v1.json
{
  "$schema": "http://json-schema.org/draft-07/schema",
  "title": "RealtimeQuote",
  "required": ["symbol", "price", "volume"],
  "properties": {
    "symbol": {"type": "string"},
    "price":  {"type": "number", "minimum": 0},
    "volume": {"type": "number", "minimum": 0},
    "bid1":   {"type": ["number", "null"]},
    "ask1":   {"type": ["number", "null"]}
  }
}
```

2. **在 pytest 中加入契约测试**（`tests/test_schema_contracts.py`）：
   - 对 `_RealtimeQuoteWorker` 的输出、`RealtimePipeline.flush()` 的输出做 schema 验证
   - 对 `_ChartDataLoadThread` 的 `data_ready` payload 做 schema 验证

3. **CI 门禁**：`contract_*.json` 变更时，必须附带 schema major version 升级（`v1→v2`），等同于 L2 变更

---

### 9.3 SLO 预算联动发布

**目标**：当 error budget 消耗超阈值时，自动进入"只修不发"模式，避免在系统已降级时继续堆叠新变更。

**规则**：

| Error Budget 消耗率 | 发布限制 | 解除条件 |
|--------------------|---------|---------|
| < 50% | 正常发布 | — |
| 50%–80% | L3 变更需额外 approve（3人） | budget 恢复到 < 50% |
| > 80% | **自动冻结 L2/L3 发布**（仅允许 L1 hotfix） | budget 恢复到 < 50%，或紧急豁免流程 |
| 100%（耗尽） | **全量冻结**（仅允许 rollback） | SRE + 技术负责人联合解除 |

**实施**：

```python
# core/slo_monitor.py — 现有 ErrorBudget 扩展
class ErrorBudget:
    def release_gate(self) -> str:
        """返回当前允许的最高发布等级。"""
        ratio = self.consumed_ratio()
        if ratio >= 1.0:
            return "rollback_only"   # 全量冻结
        if ratio >= 0.8:
            return "L1_only"         # 只允许配置变更
        if ratio >= 0.5:
            return "L2_max"          # L3 需额外审批
        return "unrestricted"
```

**CI 集成**：部署脚本在执行前查询 `ErrorBudget.release_gate()`，如返回值不允许当前变更等级，则阻断并输出当前 budget 消耗率。

---

## 十、三大机制量化验收指标

> 硬约束代码化 / 故障演练常态化 / 参数校准工具化，各自的定量验收目标。

### 10.1 硬约束代码化

| 指标 | 目标值 | 测量方式 |
|------|--------|---------|
| 交易时段结构性改动（L2/L3）拦截率 | **100%** | CI 分级判定脚本覆盖所有 L2/L3 关键词 + 人工审查 |
| 许可检查漏检率（LWC NOTICE） | **0** | CI `check_license_compliance.py` 在每次 LWC 版本变更时运行 |
| 回滚 SLA 达标率（≤30s） | **≥ 99%/月** | 从告警触发到降级完成的时间戳差值，记录到 `logs/sla_events.log` |
| 时间戳合规率（UTC epoch_ms） | **100%** | `audit_trail.verify_chain_integrity()` 每月运行，审计表无本地时区字符串 |

### 10.2 故障演练常态化

| 指标 | 目标值 | 测量方式 |
|------|--------|---------|
| 周演练执行率 | **100%** | `docs/drill_reports/` 文件数 ÷ 已过去的周数 |
| 演练失败项次周修复率 | **≥ 95%** | DRILL incident_id 关闭率，统计周期为失败后 7 天内 |
| 连续通过率（D-1~D-4 全部） | **目标: 连续 4 周 100%** | 演练报告中无 FAILED 项 |
| 误拦截/漏拦截率连续两月恶化 | **自动触发重审** | 月度校准报告中的 delta 值超过 ±10 个百分点 |

### 10.3 参数校准工具化

| 指标 | 目标值 | 工具 |
|------|--------|-----|
| 月度校准按时完成率 | **100%** | `docs/risk_calibration/` 月报文件 ≥ 每月 1 份 |
| 误拦截率 | **≤ 5%**（告警线 10%） | 历史回放脚本输出 `false_block_rate` |
| 漏拦截率 | **≤ 2%**（告警线 5%） | 历史回放脚本输出 `miss_rate` |
| 两月连续恶化触发自动预警 | **100% 覆盖** | 校准脚本对比上月 delta，超阈值写入 `logs/alerts.log` |

---

## 十一、阶段退出条件

> 明确每个阶段"何时算完成、何时禁止进入下阶段"，防止范围蔓延和未完成即推进。

| 阶段 | 进入前提 | **退出条件（满足所有才算完成）** | 禁止进入下阶段的条件 |
|------|---------|-------------------------------|---------------------|
| **Phase 0** | 无 | ① 全量测试覆盖 ≥ 40%；② `pytest tests/ -q` exit 0；③ 无 bare-except 在核心路径（trade_api/config/duckdb_pool） | 测试覆盖 < 40% 或有关键 bare-except 未修复 |
| **Phase 1** | Phase 0 退出条件满足 | ① 风控引擎上线且预交易拒单率可测量；② 策略基类标准化，所有实盘策略继承 BaseStrategy；③ 回撤熔断 D-3 演练通过；④ 月度误拦截率 ≤ 10% | 无风控引擎 / D-3 演练未通过 / 全量测试失败 |
| **Phase 2** | Phase 0 退出条件满足（可与 Phase 1 并行） | ① T+1 对账引擎上线，每日对账差异 = 0；② `verify_chain_integrity()` ok = True；③ `batch_hash` 链式验证通过率 = 100% | `verify_chain_integrity()` 发现 tampered 记录 |
| **Phase 3** | Phase 1 + Phase 2 退出条件满足 | ① SLO 仪表盘输出 Prometheus 指标；② Error Budget 联动发布门禁上线；③ `release_gate()` 在 CI 中生效 | Error budget 消耗 > 80%（系统不稳定，不适合部署新监控层） |
| **方向3 Phase A** | Phase 1-3 全部完成 | ① FastAPI server 通过负载测试（100 req/s）；② LWC 通过 WS 连接消费数据，无 DuckDB 直连依赖；③ schema contract 测试全部通过 | Phase 1-3 任一未完成；或 error budget 消耗 > 50% |
| **方向3 Phase B** | Phase A 完成 + QMT 开放云 API | ① 策略在 K8s 中稳定运行 2 周；② Kafka 替代 signal_bus 后所有事件单元测试通过 | Phase A 未完成或 QMT 云 API 未开放 |
