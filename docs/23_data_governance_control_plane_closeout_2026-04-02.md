# 数据治理控制面阶段性收口说明（2026-04-02）

> 目标：对本轮数据治理控制面增量建设做阶段性封版，明确已完成边界、当前可用能力与后续可选项，避免继续扩展影响主线任务推进。

---

## 1. 收口结论

本轮数据治理控制面建设到此为止，建议按“阶段性可交付”收口，不再继续向热重载、复杂事件流、深度配置中心方向扩展。

当前状态已经满足以下目标：

- 治理状态可视化
- receipt timeline / lineage drilldown / traceability 联动
- severity / SLA impact 分层
- symbol / period 趋势拆分
- 服务端阈值持久化
- action rulebook 外置化
- 治理动作审计日志
- 多格式治理快照导出
- rulebook schema 校验

结论：**本分支能力已形成完整闭环，适合先封版，回归主线。**

---

## 2. 已完成能力边界

### 2.1 后端能力

已落地能力：

- `receipt timeline` 过滤与时间窗口
- `lineage_anchor` 明细钻取
- `traceability_records` 反查
- `gate_reject_reason -> severity -> SLA impact` 映射
- `symbol / period` 维度治理趋势聚合
- SLA 阈值服务端持久化
- action rulebook 外置加载
- rulebook schema 校验
- governance action audit JSONL 留痕
- snapshot 导出（json / jsonl / csv）
- repair / replay 快捷触发接口

对应核心文件：

- `core/api_server.py`
- `data_manager/unified_data_interface.py`
- `config/governance_action_rulebook.json`

### 2.2 前端能力

已落地能力：

- Governance overview 聚合展示
- Timeline 过滤器
- Lineage drilldown 面板
- Traceability 联动跳转
- Workbench 图表联动
- SLA 阈值编辑与保存
- rulebook 元数据与校验状态展示
- action audit 表格展示
- action audit 前端筛选
- 多格式快照导出

对应核心文件：

- `apps/tauri-shell/src/routes/DataRoute.tsx`
- `apps/tauri-shell/src/lib/api.ts`
- `apps/tauri-shell/src/lib/routeBridge.ts`
- `apps/tauri-shell/src/routes/WorkbenchRoute.tsx`

---

## 3. 当前交付口径

本阶段交付口径定义为：

- **运行态可看**：治理状态、趋势、拒绝原因、SLA 影响都可见
- **链路可追**：timeline → lineage → traceability → chart 已打通
- **动作可做**：repair / replay / traceability / workbench 可联动执行
- **变更可记**：阈值更新、治理动作、导出快照均可留痕
- **制度可配**：rulebook 与阈值具备配置化、版本化基础

这已经足以支撑主线开发中的数据治理观测、故障定位、回溯审计，不需要继续在本轮内扩大范围。

---

## 4. 本轮不继续展开的事项

以下事项明确列入“后续可选项”，本次收口不再继续推进：

- rulebook 热重载
- 配置异常主动告警
- 审计日志分页与时间区间高级过滤
- snapshot 定向导出（按 symbol / lineage_anchor）
- 实时事件流回灌治理面板
- 更强的服务端配置中心能力

原因：

- 这些能力属于“治理平台深化”，不是当前主线的关键依赖
- 继续推进会扩大测试面与维护面
- 对当前主线收益边际下降，容易分散研发注意力

---

## 5. 回归验证结论

截至本次收口，相关验证结果为：

- pytest：`125 passed`
- ruff：`All checks passed`
- Tauri build：`tsc && vite build` 通过

结论：**当前收口版本处于可交付、可继续承接主线开发的稳定状态。**

---

## 6. 建议执行策略

建议后续按以下策略处理：

- 将本治理控制面视为“已封版的辅助治理子系统”
- 主线任务优先，不再在本分支继续加功能
- 仅在以下场景重新打开本分支：
  - 主线确实依赖新的治理能力
  - 实盘前需要补更强审计/导出
  - 线上事故复盘暴露新的制度缺口

---

## 7. 一句话结论

> **本轮数据治理控制面建设已经达到阶段目标，建议立即收口并回到主线任务，不再继续横向扩展。**
