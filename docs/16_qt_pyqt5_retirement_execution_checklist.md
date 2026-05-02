# Qt / PyQt5 退役执行清单

> 目标：把“Qt / PyQt5 退出主线”从战略判断落成团队执行面板。

**状态**: ✅ 立即生效
**最后更新**: 2026-03-31
**关联决议**: `docs/adr/ADR-0002-Qt主线退役与迁移预算重分配.md`

---

## 一句话原则

> **Qt / PyQt5 不再拿战略预算；只保留保命维护，其余资源优先投入 Tauri / API / state_store。**

---

## 预算护栏

| 工作方向 | 目标投入占比 | 说明 |
| ---------- | -------------- | ------ |
| `apps/tauri-shell/` + `core/api_server.py` + `core/state_store/` | 70% ~ 80% | 新主线建设 |
| `.venv` / headless / 非 GUI pytest / API 回归 | 10% ~ 20% | 去 GUI 化开发与验证主路径 |
| `gui_app/` / `Qt / PyQt5` legacy 维护 | ≤ 10% | 仅保命与迁移解耦 |

> 如果某个迭代里 Qt 工作重新吃掉 50% 以上前端/运行时预算，说明团队已经偏离本清单。

---

## Qt 工作项准入规则

### 允许继续做的事

- `P0` 启动崩溃 / 关闭崩溃 / 线程泄漏修复
- 明确阻断当前用户使用的故障修复
- 为迁移服务的解耦工作
  - API 化
  - 状态外提
  - 图表 facade / adapter 抽象

### 默认禁止继续做的事

- 在 `gui_app/` 中新增主功能
- 新增复杂交互工作流
- 新增 QThread / QWebEngine 债务
- 为“看起来更顺手”继续把新需求塞进 Qt 壳

### 所有 Qt 工单必须先被归类为以下之一

| 类型 | 含义 | 处理原则 |
| ------ | ------ | ---------- |
| `survival fix` | 不修会直接阻断现有用户 | 可做 |
| `migration unblocker` | 不修会阻断替代迁移 | 可做 |
| `feature request` | 新能力或体验优化 | 默认拒绝 / 转 Tauri |

---

## 立即执行项（本周）

### A. 巩固默认开发路径

- [ ] 选择性提交 `.venv` 稳定化补丁（不要和无关改动混提）
- [ ] 团队默认 VS Code 解释器切到 `.venv`
- [ ] 非 GUI pytest 默认走 `.venv`
- [ ] 保留 `conda` 作为 legacy GUI / Qt 验证环境
- [ ] 清理 `SSL_CERT_DIR` 等 conda 残留环境变量

### B. 冻结 legacy GUI 主线

- [ ] 明确 `gui_app/` 为 maintenance-only
- [ ] 新 UI 需求默认禁止落到 `gui_app/`
- [ ] 评审 Qt 工单时强制执行 `survival fix / migration unblocker / reject` 分类
- [ ] 将“Qt 修复不得占据主线预算”纳入评审共识

### C. 建立 replacement backlog

- [ ] 盘点仍只能通过 Qt 完成的用户路径
- [ ] 为每条路径指定新归宿（Tauri route / API / state_store）
- [ ] 明确 P0 替代优先级：系统状态、结构监控、Bayesian 操作、图表工作区

---

## 迁移优先级

### P0：先替代“最需要被验证”的页面

- [ ] 系统状态页（SQLite / shadow version / sync status）
- [ ] 结构监控只读页
- [ ] Bayesian preview / apply 页
- [ ] 最近状态事件 / 读模型可观察页

### P1：替代“高频使用但不该继续长在 Qt 里”的页面

- [ ] 图表工作区外壳
- [ ] 工作区预设 / 布局持久化
- [ ] 交易侧边栏与底部持仓 / 委托 / 成交读视图

### P2：清理剩余 legacy 依赖

- [ ] 盘点仍直接依赖 `PyQt5` 的非 GUI 模块
- [ ] 将 GUI 之外的状态与逻辑继续外提
- [ ] 把 API / headless 路径变成默认 CI 主验证面

---

## 建议的模块迁移映射表

| 旧路径/能力 | 新归宿 | 状态 |
| ------------- | -------- | ------ |
| `gui_app/main_window.py` 系统态可观察信息 | `apps/tauri-shell` + `/api/v1/system/*` | 进行中 |
| `gui_app/widgets/structure_monitor_panel.py` | Tauri 结构页 + Bayesian 读写 API | 进行中 |
| `gui_app/widgets/kline_chart_workspace.py` 外壳 | `Chart Facade v2` + Tauri workbench | 待推进 |
| 线程/进程状态显示 | API / state_store / frontend events | 进行中 |

> 说明：这里的“状态”建议在后续迭代中持续更新，而不是写完就不管。

---

## 回归与验证策略

### 默认回归主战场

- `.venv`
- 非 GUI pytest
- API / state_store / data_manager / structure 层验证

### legacy GUI 仅保留的最小回归

- 启动 smoke
- closeEvent / 线程退出安全
- 关键崩溃回归

### 不再作为默认成功标准的内容

- “Qt 体验是不是还能继续抛光”
- “某个旧 Widget 是否能再承载更多新需求”

---

## 物理退役前必须满足的条件

### Stage A：退出主线投资

- [ ] 团队已接受本清单与 ADR-0002
- [ ] Qt 不再承接新主功能
- [ ] `.venv` 成为默认开发解释器

### Stage B：完成关键替代

- [ ] Tauri / API 已替代关键系统状态与结构页
- [ ] 主要可观察性路径不再依赖 Qt
- [ ] 默认回归主路径不再需要 GUI 环境

### Stage C：默认用户路径切换

- [ ] 主要用户操作路径已有新壳替代入口
- [ ] legacy GUI 降级为兼容入口，而非主入口

### Stage D：物理下线 Qt / PyQt5

- [ ] 没有关键业务流程只能通过 `gui_app/` 完成
- [ ] 团队日常开发、测试、调试不再依赖 Qt 环境
- [ ] 关键图表与工作台能力已完成可接受替代
- [ ] 剩余 Qt 模块只剩可删除或可归档资产

> 只有在 Stage D 条件满足后，才讨论真正移除 `PyQt5`、删除 `gui_app/` 或下线 legacy GUI 构建链路。

---

## 一句话总结

> **Qt / PyQt5 的正确退场方式不是继续投入主线修复，而是：冻结、替代、减配、退役。**
