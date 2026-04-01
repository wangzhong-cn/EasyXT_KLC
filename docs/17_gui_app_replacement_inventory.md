# gui_app 替代清单与首批迁移 Backlog

> 目标：把 `gui_app/` 的“哪些东西值得先迁、哪些只保留 legacy 支持”盘成一张可执行清单。

**状态**: ✅ 初版建立
**最后更新**: 2026-03-31
**关联文档**:

- `docs/08_gui_app_legacy_freeze.md`
- `docs/16_qt_pyqt5_retirement_execution_checklist.md`
- `docs/adr/ADR-0002-Qt主线退役与迁移预算重分配.md`

---

## 一句话原则

> **不是把所有 PyQt 文件机械搬到 Tauri，而是先把业务逻辑迁入纯 Python + API 层，再让 Tauri 接管展示和交互。**

---

## 分类口径

本清单将 `gui_app/` 中的关键文件按以下类别归类：

- `system-observability`：系统状态、健康检查、数据治理、事件可观察性
- `structure-bayesian`：结构监控、因子分析、Bayesian 预览/写回
- `chart-workbench`：K 线图工作区、图表适配与实时桥接
- `trading-workbench`：策略管理、回测、交易工作台
- `legacy-support-only`：短期仅保留兼容，不再投资扩张

---

## 关键文件清单

| # | 文件路径 | 当前职责 | 分类 | 建议替代落点 |
| --- | --- | --- | --- | --- |
| 1 | `gui_app/main_window.py` | Qt 主窗口、Tab 装配、运行时状态与线程入口 | system-observability | `apps/tauri-shell/src/App.tsx` + `src/routes/` |
| 2 | `gui_app/main.py` | `QApplication` 初始化、主题应用、窗口启动 | system-observability | Tauri `main.tsx` + Vite/Tauri 启动链 |
| 3 | `gui_app/widgets/kline_chart_workspace.py` | K 线图工作区、实时桥接、指标/标注管理 | chart-workbench | `apps/tauri-shell/src/chart/chartFacadeV2.ts` + WorkbenchRoute |
| 4 | `gui_app/widgets/structure_monitor_panel.py` | 结构表、信号表、回撤面板、Bayesian 调用、详情联动 | structure-bayesian | 新建 `StructureRoute.tsx` + `/api/v1/structures/**` |
| 5 | `gui_app/widgets/strategy_governance_panel.py` | 策略 CRUD、回测、绩效、风险、生命周期管理 | trading-workbench | 扩展 `WorkbenchRoute.tsx` + `/api/v1/strategies/**` |
| 6 | `gui_app/widgets/data_governance_panel.py` | 数据下载、质检、路由、管道、交易日历等多标签治理面板 | system-observability | 新建 `DataRoute.tsx` + DataManagerController 对应 API |
| 7 | `gui_app/strategy_controller.py` | 纯 Python 策略业务逻辑 | trading-workbench | 保留后端逻辑，前端改走 API |
| 8 | `gui_app/data_manager_controller.py` | 纯 Python 数据治理控制逻辑 | system-observability | 保留后端逻辑，前端改走 API |
| 9 | `gui_app/widgets/factor_widget.py` | 因子注册、计算、覆盖查询 | structure-bayesian | 新建 `FactorRoute.tsx` 或并入 WorkbenchRoute |
| 10 | `gui_app/widgets/backtest_widget.py` | 回测参数配置、回测结果展示 | trading-workbench | WorkbenchRoute 子标签页或独立 Route |
| 11 | `gui_app/widgets/local_data_manager_widget.py` | 本地数据下载与管理的 Qt 壳 | system-observability | 并入 `DataRoute.tsx` |
| 12 | `gui_app/trading_interface_simple.py` | 简化交易界面、订单/持仓/账户组合面板 | legacy-support-only | 先保留 legacy 支持，后续再决定是否迁移 |
| 13 | `gui_app/xt_probe_worker.py` | xtquant 健康探针 | system-observability | 后端 `/health` 或预检 API |

---

## 替代优先级

### P0：先替代最关键的系统与结构入口

1. `main.py` / `main_window.py`
2. `data_governance_panel.py`
3. `structure_monitor_panel.py`

### P1：再替代工作台与策略管理

1. `strategy_governance_panel.py`
2. `backtest_widget.py`
3. `factor_widget.py`

### P2：最后处理高风险图表与低优先级 legacy 界面

1. `kline_chart_workspace.py`
2. `trading_interface_simple.py`

> 这里故意把图表工作区放后面，不是因为它不重要，而是它技术风险最高，必须在 facade / license / offline packaging 闸门清晰之后再推进。

---

## 首批迁移 Backlog（建议顺序）

### Backlog 1：冻结旧壳装配入口

- 目标：明确 `gui_app/main.py` 与 `gui_app/main_window.py` 只保留兼容维护，不再接新功能
- 验收：新 UI 需求不再落到 Qt 主窗口装配链

### Backlog 2：建立 `DataRoute.tsx`

- 目标：承接数据治理面板的系统可观察性功能
- 依赖：复用 `gui_app/data_manager_controller.py` 已有控制逻辑和后端 API
- 验收：能在 Tauri 中查看数据下载、质检、路由、交易日历等核心信息

### Backlog 3：建立 `StructureRoute.tsx`

- 目标：承接结构监控、信号、回撤面板与 Bayesian 操作入口
- 依赖：`/api/v1/structures/**`、相关 Bayesian API
- 验收：结构查询、状态筛选、信号查看、Bayesian 预览/写回在 Tauri 中可用

### Backlog 4：扩展 `WorkbenchRoute.tsx`

- 目标：承接策略 CRUD、回测、绩效与风险管理
- 依赖：`gui_app/strategy_controller.py` 对应 API
- 验收：策略工作流不再依赖 Qt 面板

### Backlog 5：建立 `FactorRoute.tsx`

- 目标：承接因子分析与覆盖查询
- 依赖：补齐 `/api/v1/factors/**`
- 验收：因子查询和触发计算可以脱离 Qt

### Backlog 6：验证 `Chart Facade v2`

- 目标：确认图表外壳迁移路线是否可行
- 依赖：`docs/10_chart_facade_v2_draft.md` 中列出的升级闸门
- 验收：图表能在 Tauri 中离线加载、切换符号/周期、承接指标/标注

### Backlog 7：迁移 `kline_chart_workspace.py` 工作区逻辑

- 目标：在 facade 验证通过后，再逐步迁移图表工作区
- 验收：实时桥接、布局、标注等关键工作区能力在新壳中可接受地替代

---

## 直接保留后端、不要再包 Qt 壳的模块

以下模块本身已经更接近“可被 API 消费的纯 Python 逻辑”，应优先保留为后端能力，而不是继续维护其 Qt 外壳：

- `gui_app/strategy_controller.py`
- `gui_app/data_manager_controller.py`

这两个文件的正确未来不是“继续长 UI”，而是“变成更稳定的 API / service 层输入”。

---

## 需要继续跟踪的高风险项

| 风险项 | 风险说明 | 建议策略 |
| --- | --- | --- |
| `kline_chart_workspace.py` | 文件巨大、实时桥接复杂、涉及图表核心资产 | 延后迁移，先做 facade 闸门验证 |
| `strategy_governance_panel.py` | 工作流跨度大，含策略、回测、绩效、风控多个域 | 先拆成 API 能力，再做 Workbench 聚合 |
| `data_governance_panel.py` | 标签页多、系统可观察性密集 | 作为 P0 替代对象优先落到新壳 |
| `structure_monitor_panel.py` | 已含 API 读模式，适合优先迁移 | 作为新结构页首选候选 |

---

## 一句话总结

> **真正值得先迁的不是“所有 Qt 文件”，而是那些能最快把主验证路径从 Qt 壳中剥离出来的系统页、结构页和工作台骨架。**
