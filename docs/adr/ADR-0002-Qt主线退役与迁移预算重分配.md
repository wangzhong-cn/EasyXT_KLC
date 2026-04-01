# ADR-0002：Qt 主线退役与迁移预算重分配

- 状态：Accepted
- 日期：2026-03-31
- 决策人：Frontend / Quant Platform / Runtime Stability
- 关联文档：[Tauri 增量替换蓝图](file:///d:/EasyXT_KLC/docs/05_tauri_incremental_replacement_blueprint.md)、[gui_app Legacy Freeze](file:///d:/EasyXT_KLC/docs/08_gui_app_legacy_freeze.md)、[Qt / PyQt5 退役执行清单](file:///d:/EasyXT_KLC/docs/16_qt_pyqt5_retirement_execution_checklist.md)

## 背景

当前仓库已经明确：

- 新前端主线应迁往 `apps/tauri-shell/`；
- `gui_app/` 已进入 legacy freeze；
- 但实际投入仍长期被 `Qt / PyQt5 / QThread / QWebEngine` 故障修复吞噬。

当大部分研发时间持续投入旧桌面壳的生命周期修复、线程退出安全、GUI 兼容性时，会出现三个直接问题：

1. 新主线（Tauri / API / state_store）推进速度过慢；
2. 旧壳不断被“保命修复”重新包装成事实主线；
3. 团队默认开发与测试路径持续绑在 GUI 运行环境上，难以形成 headless 主战场。

与此同时，仓库已经完成一项关键前提：`.venv` 下的非 GUI pytest 入口已经可以作为默认开发验证路径之一。这意味着“先把开发/测试主路径从 Qt 环境剥离”已经具备现实基础。

## 决策

1. **Qt / PyQt5 退出主线投资序列。**
   - 自本 ADR 生效起，`gui_app/` 不再承载未来主线功能投资；
   - `Qt / PyQt5` 仅保留最小保命维护预算，不再占据战略预算。

2. **新 UI / 新交互 / 新状态可视化默认落点统一切换到：**
   - `apps/tauri-shell/`
   - `core/api_server.py`
   - `core/state_store/`

3. **开发与测试主路径默认去 GUI 化。**
   - 默认开发解释器：`D:\EasyXT_KLC\.venv\Scripts\python.exe`
   - 默认非 GUI pytest：`.venv` + headless 路径
   - `conda` 仅作为 legacy GUI / Qt 验证环境保留，直到退役条件满足。

4. **Qt 维护预算设置硬上限。**
   - 常态下，`Qt / PyQt5` 维护工作量不得超过前端与运行时总投入的 **10%**；
   - 只有以下情形允许例外：
     - P0 启动崩溃 / 关闭崩溃
     - 阻断当前用户基本使用
     - 明确服务于迁移解耦的改动

5. **Qt 退役执行以仓库清单驱动。**
   - 具体执行顺序、预算分配、替代优先级与下线条件，以 `docs/16_qt_pyqt5_retirement_execution_checklist.md` 为准。

## 非目标

- 不在本 ADR 中要求立刻物理删除 `gui_app/` 或卸载 `PyQt5`。
- 不在本 ADR 中要求一次性重写全部 GUI 功能。
- 不在本 ADR 中否定 KLineChart 等既有资产；图表资产应通过 facade / adapter 迁移，而不是简单抛弃。
- 不在本 ADR 中暂停所有 legacy 修复；保命级修复仍允许继续。

## 影响

- 好处：
  - 资源正式从旧壳修复转向新主线建设；
  - `.venv` / headless / API 成为默认工程重心；
  - 团队对“什么值得继续修、什么不值得”有统一边界。

- 代价：
  - 短期内需同时维护 legacy 与 replacement 两条线；
  - 某些 Qt 体验问题会被明确降级为“可接受遗留问题”。

- 风险：
  - 若新壳替代推进不及时，用户仍会短期依赖 legacy GUI；
  - 团队若没有执行预算约束，旧壳仍可能重新侵占主线资源。

## 执行与验收

1. 仓库文档落地：
   - 建立本 ADR；
   - 建立 `Qt / PyQt5` 退役执行清单；
   - 将二者接入 docs 主导航。

2. 开发默认路径切换：
   - 团队默认解释器切到 `.venv`；
   - 非 GUI pytest 走 `.venv`；
   - GUI / Qt 测试降级为 legacy 验证专线。

3. 需求准入切换：
   - 新 UI 需求默认不得进入 `gui_app/`；
   - 新可观察性页面优先进入 `apps/tauri-shell/`。

4. 预算守门：
   - 对新增 Qt 工作项进行 triage，必须归类为：
     - `survival fix`
     - `migration unblocker`
     - `reject / defer`

5. 退出条件：
   - 当结构监控、系统状态、关键工作台路径完成 Tauri/API 替代，且团队日常开发/回归不再依赖 Qt 环境时，再进入“物理下线”阶段。

## 验收记录

| 步骤 | 执行时间 | 结果 | 说明 |
| ------ | ---------- | ------ | ------ |
| ADR 建立 | 2026-03-31 | ✅ | 正式把 Qt 退出主线投资写入仓库决议 |
| 执行清单建立 | 2026-03-31 | ✅ | 将“退役”从口号转成可追踪 checklist |
| 文档导航接入 | 2026-03-31 | ✅ | 主导航与 ADR 导航已纳入新决议入口 |

## 回滚 / 例外策略

- 若新壳替代阶段性受阻，不回滚本 ADR；
- 可临时提高 legacy 维护投入，但必须满足：
  - 有明确的用户阻塞或迁移阻塞证据；
  - 变更归类为 `survival fix` 或 `migration unblocker`；
  - 例外结束后恢复预算上限。

## 后续动作

- 按执行清单建立 replacement backlog；
- 把默认 CI / 回归重心继续迁向 `.venv` + headless；
- 为“物理退役 Qt”补充明确退出条件与残余依赖盘点。
