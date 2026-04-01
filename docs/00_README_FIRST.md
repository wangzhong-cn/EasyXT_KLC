# EasyXT 文档中心

> 📚 完整的量化交易平台文档体系

**最后更新**: 2026-04-02
**版本**: v3.3 (评估校准版)
**仓库**: https://gitee.com/TradersTV/easy-xt_-klc

---

## 📖 文档导航

### 🎯 快速开始

| 文档 | 描述 | 适合人群 |
|------|------|----------|
| [架构设计](01_architecture_design.md) | 整体架构和技术路线 | 开发者、架构师 |
| [模块说明](02_modules_overview.md) | 三大核心模块详解 | 所有用户 |
| [迁移指南](03_migration_guide.md) | 从旧版到新版迁移 | 老用户 |
| [开发规范](04_development_standards.md) | 代码开发和贡献规范 | 开发者 |
| [Tauri 增量替换蓝图](05_tauri_incremental_replacement_blueprint.md) | 当前默认执行路线 | 开发者、架构师 |
| [双引擎状态契约](06_dual_engine_state_contract.md) | SQLite/DuckDB 边界与同步协议 | 开发者、架构师 |
| [gui_app Legacy Freeze](08_gui_app_legacy_freeze.md) | 旧 Qt 壳冻结与退役规则 | 开发者、架构师 |
| [前端参考图纸总览](09_frontend_reference_atlas.md) | 参考截图沉淀出的布局与组件模板 | 开发者、设计师 |
| [Chart Facade v2 草案](10_chart_facade_v2_draft.md) | 图表统一接口与升级闸门 | 开发者、架构师 |
| [状态主线备份恢复协议](11_state_backup_restore_protocol.md) | SQLite 主状态的 backup/restore/verify 约束 | 开发者、架构师 |
| [DuckDB 联邦读层 Planner](12_duckdb_federation_read_planner.md) | shard pruning / attach budget / union SQL 规划 | 开发者、架构师 |
| [Qt / PyQt5 退役执行清单](16_qt_pyqt5_retirement_execution_checklist.md) | Qt 退出主线后的预算护栏、替代优先级与下线条件 | 开发者、架构师 |
| [gui_app 替代清单与首批迁移 Backlog](17_gui_app_replacement_inventory.md) | 关键 Qt 模块盘点、替代落点与首批 backlog | 开发者、架构师 |
| [周期 Canonical 契约](18_period_canonical_contract.md) | Tick / 1m / 1d / 派生周期的系统宪法 | 开发者、架构师 |
| [周期 Canonical 实现蓝图](19_period_canonical_implementation_blueprint.md) | 契约到实现的阶段化落地路径 | 开发者、架构师 |
| [周期事实与快照目标数据模型](20_period_fact_schema_and_storage_model.md) | 未来因子库 / 结构库 / 回执库的总表设计 | 开发者、架构师 |
| [EasyXT_KLC 项目全面优化报告（最终全量版）](21_easyxt_klc_final_full_report_2026-04-01.md) | 综合前期审查、当前推进、纠正说明与后续路线图 | 开发者、架构师、管理者 |
| [EasyXT_KLC 全量评估与行动指南 v2.0](22_easyxt_klc_full_assessment_2026-04-02.md) | 阶段性评估版：制度层落地确认、实测验证与首版优先级路线图 | 开发者、架构师、管理者 |
| [EasyXT_KLC 全量评估与行动指南 v2.1（校准版）](23_easyxt_klc_full_assessment_calibrated_2026-04-02.md) | 当前推荐阅读：校准七层母本、代码规模、测试口径与残留问题基线 | 开发者、架构师、管理者 |

### 🧭 当前治理状态

- [文档治理矩阵](13_docs_governance_matrix.md)：当前 docs 保留 / 合并 / 归档 / 删除候选清单（✅ 当前基线）
- [ADR-0002：Qt 主线退役与迁移预算重分配](adr/ADR-0002-Qt主线退役与迁移预算重分配.md)：正式决议 Qt 退出主线投资（✅ 已接受）
- [docs/archive/README](archive/README.md)：历史文档归档区说明（✅ 已建立）

### 🧱 当前主线专题文档

- **前端替换主线**： [Tauri 增量替换蓝图](05_tauri_incremental_replacement_blueprint.md)（✅ 当前默认路线）
- **状态主线契约**： [双引擎状态契约](06_dual_engine_state_contract.md)（✅ 当前权威）
- **旧壳冻结规则**： [gui_app Legacy Freeze](08_gui_app_legacy_freeze.md)（✅ 已生效）
- **Qt 退役执行面板**： [Qt / PyQt5 退役执行清单](16_qt_pyqt5_retirement_execution_checklist.md)（✅ 立即执行）
- **gui_app 替代靶单**： [gui_app 替代清单与首批迁移 Backlog](17_gui_app_replacement_inventory.md)（✅ 初版建立）
- **前端参考图纸**： [前端参考图纸总览](09_frontend_reference_atlas.md)（✅ 当前参考）
- **状态恢复底线**： [状态主线备份恢复协议](11_state_backup_restore_protocol.md)（✅ 已落地）
- **联邦读层**： [DuckDB 联邦读层 Planner](12_duckdb_federation_read_planner.md)（✅ planner + executor）
- **周期系统宪法**： [周期 Canonical 契约](18_period_canonical_contract.md)（✅ 已建立）
- **周期实施蓝图**： [周期 Canonical 实现蓝图](19_period_canonical_implementation_blueprint.md)（✅ 已建立）
- **目标数据模型**： [周期事实与快照目标数据模型](20_period_fact_schema_and_storage_model.md)（✅ 当前数据层总表）
- **项目总报告**： [EasyXT_KLC 项目全面优化报告（最终全量版）](21_easyxt_klc_final_full_report_2026-04-01.md)（✅ 综合结论与路线图）
- **全量评估v2.0**： [EasyXT_KLC 全量评估与行动指南 v2.0](22_easyxt_klc_full_assessment_2026-04-02.md)（📌 历史阶段性评估快照）
- **全量评估v2.1（校准版）**： [EasyXT_KLC 全量评估与行动指南 v2.1（校准版）](23_easyxt_klc_full_assessment_calibrated_2026-04-02.md)（✅ 当前推荐阅读：最新证据口径）

### 🛡️ 质量与治理文档

- [开发规范](04_development_standards.md)：日常编码、类型、测试与提交流程
- [项目开发红线](DEVELOPMENT_RULES.md)：兼容入口页，已统一指向根目录 `development_rules.md`
- [P0 门禁清单](p0_gate_checklist.md)：当前稳定性与放行门槛
- [稳定性回归门禁](stability_regression_gate.md)：回归套件分组与执行口径
- [数据基础设施 SLO/SLI](data_infra_slo_sli_spec.md)：质量治理模板与指标字典
- [数据血缘规范](lineage_spec.md)：lineage 字段与数据主路径规范

---

## 🗂️ 项目结构说明

```
D:\EasyXT_KLC\
├── docs/                           # 📚 文档中心（本目录）
│   ├── 00_README_FIRST.md         # 文档导航（本文件）
│   ├── 01_architecture_design.md  # 架构设计
│   ├── 02_modules_overview.md     # 模块概览
│   ├── 03_migration_guide.md      # 迁移指南
│   ├── 04_development_standards.md # 开发规范
│   ├── adr/                       # 架构决策记录
│   ├── archive/                   # 历史归档区（治理中）
│   ├── assets/                    # 参考图纸与原始资产目录说明
│   └── drill_reports/             # 演练记录 / open incidents
│
├── gui_app/                        # 🖥️ GUI 应用（保留）
│   ├── main_window.py             # 主窗口（保留）
│   ├── trading_interface_simple.py # 交易界面（保留）
│   └── widgets/                   # Widget 组件（保留）
│
├── strategies/                     # 📈 策略库（保留）
│   ├── grid_trading/              # 网格策略
│   ├── trend_following/           # 趋势跟踪
│   ├── conditional_orders/        # 条件单
│   └── ...                        # 其他策略
│
├── 101 因子/                        # 🔬 因子库（保留）
│   └── 101 因子分析平台/            # 因子分析平台
│
├── easy_xt/                        # 🔌 API 封装（保留）
├── data_manager/                   # 💾 数据管理（保留）
└── quant_platform/                 # 🆕 新平台（规划中）
    ├── modules/                   # 模块实现
    ├── integrations/              # 集成层
    └── config/                    # 配置
```

---

## 🛡️ 保护性原则

本次重构遵循严格的保护性原则：

### ✅ 绝对保护

1. **所有现有功能 100% 保留**
   - ✅ `gui_app/` 目录所有文件保留
   - ✅ `strategies/` 目录所有策略保留
   - ✅ `101 因子/` 目录所有因子保留
   - ✅ `easy_xt/` 目录所有 API 保留

2. **所有操作界面不丢失**
   - ✅ 主窗口界面保留
   - ✅ 交易界面保留
   - ✅ 回测界面保留
   - ✅ 所有 Widget 组件保留

3. **所有配置文件向后兼容**
   - ✅ 现有配置文件继续有效
   - ✅ 新增配置可选使用
   - ✅ 新旧配置可以共存

### 🔄 渐进式升级

1. **封装而非替换** - 新功能封装现有功能
2. **扩展而非修改** - 新界面作为现有界面的增强
3. **并行而非覆盖** - 新旧版本并行运行
4. **可选而非强制** - 用户可选择使用新旧界面

---

## 📋 版本说明

### v3.0 (重构规划版) - 当前版本

**核心改进**:
- 🆕 新增三大核心模块（交易/数据/策略）
- 🆕 图表交易一体化设计
- 🆕 DuckDB 数据库深度集成
- 🆕 因子库统一管理
- 🛡️ 保护性重构，所有现有功能保留

**兼容性**:
- ✅ 完全兼容 v2.x 所有功能
- ✅ 新旧版本可以并行运行
- ✅ 配置文件向后兼容

### v2.x (经典版) - 稳定版本

**核心功能**:
- ✅ GUI 交易界面
- ✅ 策略回测框架
- ✅ 网格交易策略
- ✅ JQ2QMT 集成
- ✅ 雪球跟单策略

**状态**: 持续维护，Bug 修复

---

## 🔗 相关链接

- **Gitee 仓库**: https://gitee.com/TradersTV/easy-xt_-klc
- **GitHub 仓库**: https://github.com/wangzhong-cn/EasyXT_KLC
- **知识星球**: quant-king299
- **官方网站**: ptqmt.com

---

## 📞 支持与反馈

### 获取帮助

1. **查看文档**: 本目录下的完整文档
2. **查看示例**: `examples/` 目录下的示例代码
3. **提交 Issue**: Gitee 仓库 Issue 区
4. **社区讨论**: 知识星球社区

### 反馈建议

欢迎通过以下方式反馈：

- 📧 Email: wan_zhon@foxmail.com
- 💬 知识星球：
- 🐛 Issue: Gitee Issue 追踪

---

## 📝 更新日志

### 2026-02-23
- ✅ 创建完整文档体系
- ✅ 完成架构设计文档
- ✅ 完成三大模块文档
- ✅ 完成迁移指南
- 🔄 开始代码重构

### 2026-02-20
- ✅ 确定保护性重构原则
- ✅ 完成技术路线规划
- ✅ Git 仓库配置完成

---

**EasyXT 量化交易平台**
*让量化交易更简单，让策略开发更高效*

---

## 📚 文档阅读顺序建议

### 第一次进入仓库

1. 📖 [文档治理矩阵](13_docs_governance_matrix.md) - 了解当前 canonical 文档边界
2. 📖 [架构设计](01_architecture_design.md) - 看清总线与当前默认路线
3. 📖 [Tauri 增量替换蓝图](05_tauri_incremental_replacement_blueprint.md) - 进入实际执行主线

### 开发者

1. 📖 [架构设计](01_architecture_design.md) - 了解整体架构
2. 📖 [Tauri 增量替换蓝图](05_tauri_incremental_replacement_blueprint.md) - 当前默认执行路线
3. 📖 [双引擎状态契约](06_dual_engine_state_contract.md) - 明确 SQLite/DuckDB 权责边界
4. 📖 [gui_app Legacy Freeze](08_gui_app_legacy_freeze.md) - 旧壳收口规则
5. 📖 [Qt / PyQt5 退役执行清单](16_qt_pyqt5_retirement_execution_checklist.md) - 预算护栏与退役步骤
6. 📖 [前端参考图纸总览](09_frontend_reference_atlas.md) - 新前端目标态模板
7. 📖 [Chart Facade v2 草案](10_chart_facade_v2_draft.md) - 图表统一接口草案
8. 📖 [状态主线备份恢复协议](11_state_backup_restore_protocol.md) - 恢复底线与 verify 协议
9. 📖 [DuckDB 联邦读层 Planner](12_duckdb_federation_read_planner.md) - 联邦读层第一版执行边界
10. 📖 [gui_app 替代清单与首批迁移 Backlog](17_gui_app_replacement_inventory.md) - 具体迁移对象与优先级
11. 📖 [开发规范](04_development_standards.md) - 代码规范

### 老用户迁移

1. 📖 [迁移指南](03_migration_guide.md) - 迁移步骤
2. 📖 [Tauri 增量替换蓝图](05_tauri_incremental_replacement_blueprint.md) - 新默认路线
3. 📖 [双引擎状态契约](06_dual_engine_state_contract.md) - 数据层约束
4. 📖 [状态主线备份恢复协议](11_state_backup_restore_protocol.md) - 备份/恢复/校验底线

---

**开始阅读**: [→ 架构设计文档](01_architecture_design.md)
