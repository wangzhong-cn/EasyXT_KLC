# 发布治理与变更分级规范

> 本文档是当前仓库 **发布治理 / L1-L3 变更分级 / SLO 联动发布** 的独立规范。
>
> 自 2026-03-31 起，`tools/check_change_level.py` 与相关文档不再把 `architecture_roadmap_direction2.md` 作为硬依赖来源。

## 适用范围

本文档约束以下发布相关动作：

- 结构性改动的时段冻结
- L1 / L2 / L3 变更分级
- 最低回归深度
- SLO budget 联动发布门禁
- 紧急 hotfix 的申请与回滚路径

## 当前 CI / 工具的冻结时间窗

当前自动门禁工具采用的默认冻结时间窗为：

- **09:25–15:05（本地时间）**

这样做的意图是覆盖：

- 开盘前后的临界窗口
- 收盘前后的敏感窗口
- 发布操作对实盘观察面板、风控、线程模型产生抖动的高风险时段

> 说明：这里记录的是**当前工具实际执行口径**，用于与 `tools/check_change_level.py` 保持一致。

## L1 / L2 / L3 三级分级定义

| 等级 | 名称 | 变更范围示例 | 交易敏感时段限制 |
| ---- | ---- | ------------ | ---------------- |
| **L1** | 配置变更 | 日志级别、UI 文本、环境变量阈值、feature flag、文档说明 | 允许 |
| **L2** | 逻辑变更 | 业务逻辑、算法、策略参数、数据库 schema、数据处理逻辑 | 禁止 |
| **L3** | 协议变更 | 通信协议、线程模型、风控核心逻辑、DuckDB DDL、核心事件契约 | 严格禁止 |

## 最低回归要求

| 等级 | 最低回归要求 | SLO 联动 |
| ---- | ------------ | -------- |
| **L1** | 变更模块的单元测试通过 | 不消耗 error budget |
| **L2** | 全量 `pytest tests/ -q` 通过 + 5 分钟冒烟测试 | 消耗 error budget |
| **L3** | 全量回归 + `TestKLineWorkspaceExitStability` + 故障演练 D-1~D-4 | 消耗 error budget，触发更严格审批 |

## 自动判级基础规则

### 路径级高优先级规则

以下路径默认直接提升等级：

- **L3**
  - `core/events.py`
  - `core/safe_thread_runner.py`
  - `gui_app/widgets/kline_chart_workspace.py`
  - `core/audit_trail.py`
  - `core/risk_engine.py`
- **L2**
  - `core/`
  - `gui_app/`
  - `strategies/`
  - `data_manager/`
  - `easy_xt/`

### 语义升级规则

即使路径本身看起来只是 L1，以下语义也会触发升级：

- 升级到 **L3** 的典型关键词
  - `risk_threshold`
  - `protocol`
  - `DDL`
  - `CREATE TABLE`
  - `ALTER TABLE`
  - `QThread ... run`
  - `terminate()`
- 升级到 **L2** 的典型关键词
  - `order_id`
  - `signal_id`
  - `fill_id`
  - `submit_order`
  - `cancel_order`
  - `strategy_id`
  - `backtest`

> 解释：路径分级是主规则，语义升级用于捕获“路径看似无害，但实际改了核心行为”的情况。

## SLO budget 联动发布门禁

| budget 状态 | 发布门禁 |
| ----------- | -------- |
| `unrestricted` | 正常发布 |
| `L2_max` | L3 变更需额外审批 |
| `L1_only` | 仅允许 L1 变更 |
| `rollback_only` | 仅允许回滚 |

当前 `tools/check_change_level.py` 的行为口径：

- `rollback_only`：直接阻断
- `L1_only`：阻断 L2/L3
- `L2_max`：对 L3 给出警告，需人工确认

## 紧急 hotfix 流程

交易敏感窗口内，如确需处理线上问题，遵循以下最小流程：

1. 提交 `[EMERGENCY]` 变更说明
2. 明确变更范围与回滚方案
3. 确认不涉及线程模型 / 风控核心逻辑 / 核心协议
4. 在隔离环境验证后滚动实施
5. 发布后持续观察，若出现异常立即回滚

## 回滚触发线

| 触发事件 | 自动动作 | 人工确认 |
| -------- | -------- | -------- |
| 持续告警达到阈值 | 进入降级模式 | 可延后 |
| 降级持续超过阈值 | 推送告警并准备回滚 | 需要 |
| 风控 HALT 事件 | 停止下单信号 | 需要 |
| 进程崩溃 / 线程模型异常 | 自动重启或回滚到上一快照 | 需要 |

## 与仓库实现的对应关系

- 自动判级脚本：`../tools/check_change_level.py`
- 线程/关闭安全规范：`05_thread_exit_safety_spec.md`
- P0 治理清单：`p0_gate_checklist.md`
- 稳定性门禁：`stability_regression_gate.md`

## 备注

> `architecture_roadmap_direction2.md` 的完整正文已归档到 `archive/architecture_roadmap_direction2_v1.md`，原路径仅保留兼容说明页；
> 发布治理与分级规则的当前维护入口，已经切到本文档。
