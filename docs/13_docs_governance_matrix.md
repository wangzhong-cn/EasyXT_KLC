# docs 治理矩阵（第一轮）

> 目标：在**不粗暴误删**的前提下，明确当前仓库文档的 canonical 边界，给后续合并、归档与删除建立唯一矩阵。

**最后更新**: 2026-03-31
**状态**: ✅ 第一轮基线建立完成
**执行原则**: 先保留主线、再合并重复、再归档历史、最后才考虑删除

---

## 治理原则

1. **被仓库指令直接引用的文档，不做激进处理。**
2. **当前正在承载代码契约的文档，视为 canonical。**
3. **历史进度/诊断/路线草图，优先归档而非直接删除。**
4. **第一轮不做硬删除；先通过矩阵统一边界。**

---

## 保留（KEEP）

这些文档当前仍然承担主线入口、架构契约或开发红线职责。

- `00_README_FIRST.md`：主导航，当前 docs 的唯一主入口
- `01_architecture_design.md`：架构总纲，2026-03 校正后仍是总线入口
- `02_modules_overview.md`：模块概览，仍有 PyQt / 旧架构背景说明价值
- `03_migration_guide.md`：迁移入口，老用户迁移仍需要，但已加路线校正提示
- `04_development_standards.md`：开发规范，仓库与 agent 指令共同依赖
- `05_tauri_incremental_replacement_blueprint.md`：当前执行蓝图，前端替换主线
- `05_thread_exit_safety_spec.md`：GUI 红线，线程/退出安全权威规范
- `06_dual_engine_state_contract.md`：状态契约，SQLite / DuckDB 权责边界
- `08_gui_app_legacy_freeze.md`：旧壳冻结规则，PyQt legacy 收口红线
- `09_frontend_reference_atlas.md`：前端参考图纸，新壳目标态模板
- `10_chart_facade_v2_draft.md`：图表草案，当前可讨论、可落地的切口
- `11_state_backup_restore_protocol.md`：状态恢复协议，已有代码落地
- `12_duckdb_federation_read_planner.md`：联邦读层协议，planner + executor 已启动
- `16_qt_pyqt5_retirement_execution_checklist.md`：Qt 退出主线后的执行清单与下线条件
- `17_gui_app_replacement_inventory.md`：gui_app 关键替代对象、落点与首批 backlog
- `14_release_governance_controls.md`：发布治理、变更分级与 SLO 联动门禁
- `15_phase_exit_criteria.md`：Phase 退出条件的独立规范来源
- `lineage_spec.md`：数据血缘规范，当前治理底层契约
- `p0_gate_checklist.md`：P0 门禁，当前放行标准
- `p0_monthly_review_template.md`：月度治理模板，运营与治理固定模板
- `data_infra_slo_sli_spec.md`：SLO/SLI 模板，质量治理指标基线
- `stability_regression_gate.md`：回归门禁，稳定性分组与执行口径
- `strategy_gate_soft_to_hard_rollout.md`：发布治理，策略门禁演进文档
- `黄金铁律_集合竞价与分钟线数据规则.md`：领域铁律，DAT / 交易时段规则基线
- `adr/ADR-0001-单一数据口径与血缘主路径.md`：当前唯一正式架构决策记录
- `adr/ADR-0002-Qt主线退役与迁移预算重分配.md`：Qt 退出主线投资的正式决议

---

## 合并（MERGE）

这些文档不是马上删除，而是应该逐步并入更权威的目标文档。

- `README.md` → `00_README_FIRST.md`
  - 理由：两者导航职责重叠，且 `README.md` 含大量旧链接 / 死链
  - 当前动作：已改为别名页，继续保留原路径以兼容旧链接
- `docs/DEVELOPMENT_RULES.md` → 根目录 `development_rules.md`
  - 理由：两者同名但内容不一致；当前真正被指令 / CI 引用的是根目录版本
  - 当前动作：已改为兼容页，统一指向根目录 `development_rules.md`

---

## 归档（ARCHIVE）

这些文档保留历史价值，但不应继续作为主线入口。

- `REFACTOR_PROGRESS.md`
  - 归档理由：14 阶段完成态历史快照
  - 风险：仍保留历史排障价值
  - 当前动作：已迁入 `docs/archive/REFACTOR_PROGRESS_v14_final.md`，原路径保留说明页
- `data_infrastructure_diagnosis.md`
  - 归档理由：v1 诊断结果，已被 v2 覆盖
  - 风险：可能仍有历史对照用途
  - 当前动作：已迁入 `docs/archive/data_infrastructure_diagnosis_v1.md`，原路径保留说明页
- `architecture_roadmap_direction2.md`
  - 归档理由：方向级路线草图，非当前执行契约
  - 风险：仍保留综合路线背景价值
  - 当前动作：已迁入 `docs/archive/architecture_roadmap_direction2_v1.md`，原路径保留说明页
- `EasyXT_vs_EasyXT_KLC_完整对比分析.md`
  - 归档理由：一次性对标报告
  - 风险：仍有阶段性回溯价值
  - 当前动作：已迁入 `docs/archive/EasyXT_vs_EasyXT_KLC_完整对比分析_v1.md`，原路径保留说明页

---

## 删除候选（DELETE-CANDIDATE）

第一轮采取保守策略：

> **当前没有直接执行的硬删除项。**

只有在满足以下条件时，才进入删除候选：

1. 无仓库指令引用
2. 无主导航引用
3. 内容已被更新文档完全覆盖
4. 先经历一轮“归档观察期”

---

## 第一批已启动动作

本轮已执行的保守治理动作：

- 修正 `00_README_FIRST.md` 的死链误导，收敛到现有文档集合
- 为 `01_architecture_design.md` 增加当前主线推荐阅读顺序
- 为 `03_migration_guide.md` 增加醒目的路线校正提醒
- 为 `11` / `12` 文档同步当前状态页与 federation API 新接线
- 建立本矩阵，作为后续归档/合并唯一基线
- 将 `docs/README.md` 收敛为 `00_README_FIRST.md` 的别名页
- 将 `REFACTOR_PROGRESS.md`、`data_infrastructure_diagnosis.md` 的正文迁入 `docs/archive/`
- 将 `docs/DEVELOPMENT_RULES.md` 收敛为指向根目录规则文档的兼容页
- 补充 `docs/adr/README.md`，建立 ADR 目录入口
- 将旧路线图中的发布治理 / Phase 退出条件拆出为独立规范文档
- 将 Qt 主线退役与预算重分配收敛为独立 ADR 与执行清单
- 将 gui_app 关键替代对象盘点收敛为独立迁移清单
- 将 `EasyXT_vs_EasyXT_KLC_完整对比分析.md` 迁入 `docs/archive/`
- 将 `architecture_roadmap_direction2.md` 迁入 `docs/archive/`

---

## 下一批建议动作

1. 继续清点归档文档的残余背景性引用，必要时将指向旧路径的说明改为直指 `docs/archive/`
2. 视需要将 `docs/adr/README.md` 扩展为 ADR 模板与提交流程说明

---

## 一句话总结

> 第一轮 docs 治理已经开始：**先定 canonical 边界，再逐步收口重复入口与历史文档；当前阶段不做粗暴删除。**
