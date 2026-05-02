# gui_app Legacy Freeze 政策

> 目标：把 `gui_app/` 从“继续承载未来功能的主战场”切换为“正在收口并退役的 legacy shell”。

**状态**: ✅ 立即生效
**生效日期**: 2026-03-31

---

## 为什么必须 freeze

仓库现状已经反复证明：

- `gui_app` 是当前主要崩溃面
- `PyQt5 + QThread + QWebEngine + xtquant` 是高风险组合
- 项目根目录已有 42 个 `.dmp` 文件，不可再按“小瑕疵”处理
- 新能力若继续直接堆进旧壳，只会把未来迁移成本继续放大

一句话：

> **从现在起，`gui_app` 只允许维持与收口，不再承担未来主线演进。**

---

## Freeze 适用范围

以下路径默认进入 legacy freeze：

- `gui_app/main_window.py`
- `gui_app/trading_interface_simple.py`
- `gui_app/widgets/**`
- 所有新增 `QThread` / `QWebEngine` / GUI 内直接数据库访问路径

---

## 允许做的事情（Allowed）

### 1. Bug fix

- 崩溃修复
- 线程退出安全修复
- 明确的 UI 错误修复
- 兼容层调整

### 2. 护栏与回归

- smoke 测试
- logic 测试
- 线程退出安全测试
- API 只读模式回归

### 3. 最小维护性改造

- API 化/读侧化接线
- 现有线程风险收口
- 清理明显重复或危险代码

---

## 禁止做的事情（Disallowed）

### 1. 新重功能不得再进 gui_app

- 新复杂面板
- 新工作台主流程
- 新研究/社区/热力图主界面
- 新实验型可视化

### 2. 不得再扩张 Qt 线程债务

- 新 `QThread` 工作流
- 新 `QWebEngine` 业务耦合
- 新的 GUI 内长生命周期后台任务
- GUI 内直接承载 xtquant 风险边界

### 3. 不得再扩大 GUI 直连数据库细节

- GUI 直接拼 SQL
- GUI 直接承担 DuckDB/SQLite 路由逻辑
- GUI 内混合“写状态 + 分析查询 + 展示”

---

## 新能力应该去哪里

从现在开始，新增能力默认走下面这条路径：

- **前端壳**：`apps/tauri-shell/`
- **服务边界**：`core/api_server.py`
- **状态主线**：`core/state_store/`
- **只读分析/影子**：DuckDB / API 读侧

一句话：

> **新能力进入 Tauri / API / 状态主线，不再回流进 Qt 重壳。**

---

## 与 KLineChart 的关系

`gui_app` freeze **不等于** 图表资产废弃。

恰恰相反：

- KLineChart 是核心资产，必须保
- 迁移的是外壳，不是图表语义
- 图表升级应通过 `Chart Facade v2` 吸收差异

---

## 例外审批规则

只有在以下情况同时满足时，才允许在 `gui_app/` 做较大改动：

1. 属于崩溃修复或退出安全修复
2. 无 Tauri/API 替代路径可用
3. 改动目标是**收口债务**，不是扩张能力
4. 有针对性测试回归

否则默认拒绝。

---

## 退出条件

当以下条件成立时，`gui_app` 可进入进一步退役阶段：

- Tauri shell 已提供 P0 页面
- 图表工作台已有 Web/Tauri 版本
- 结构监控/同步状态/研究视图已可从新壳消费
- xtquant 主要风险边界已从 GUI 壳中迁出

---

## 一句话总结

> **`gui_app` 从今天起是 legacy shell：允许修、允许护栏、允许兼容，但不再允许承担未来主线。**
