# EasyXT Tauri Shell

> EasyXT 新前端主壳骨架（Tauri + React + TypeScript）。

当前目标不是一次性替换全部旧界面，而是先建立：

- 可观察的新桌面壳
- 图表/交易/研究/同步状态的标准布局
- 与 `core/api_server.py` 对接的前端消费边界
- `Chart Facade v2` 的统一前端接口

## 当前包含的内容

- `src/App.tsx`：应用主壳与顶层导航
- `src/routes/`：四个 P0 页面骨架
  - `WorkbenchRoute`：交易工作台
  - `HeatmapRoute`：热力图视图
  - `IdeasRoute`：观点 / 社区 / 脚本卡片流
  - `SystemRoute`：同步状态 / 影子版本 / 告警面板
- `src/chart/chartFacadeV2.ts`：图表门面接口草案
- `src-tauri/`：Tauri 桌面容器骨架

## 参考文档

- `docs/05_tauri_incremental_replacement_blueprint.md`
- `docs/06_dual_engine_state_contract.md`
- `docs/08_gui_app_legacy_freeze.md`
- `docs/09_frontend_reference_atlas.md`
- `docs/10_chart_facade_v2_draft.md`

## 设计原则

1. **主壳先行**：先统一布局、路由、状态可视化，再迁移具体业务细节。
2. **图表资产保留**：KLineChart 为核心资产，优先通过 `Chart Facade v2` 适配。
3. **旧壳冻结**：新增可视化能力不再向 `gui_app/` 扩张。
4. **边界优先**：前端消费 HTTP / IPC 边界，不直接耦合 Qt 对象生命周期。

## 预计下一步

- 接入真实 `FastAPI` 只读接口
- 将 `WorkbenchRoute` 的图表占位区替换为 KLineChart 适配器
- 暴露同步状态页的真实 `sqlite_logical_seq / duckdb_shadow_version / sync_status`
- 建立工作区预设与用户布局持久化
