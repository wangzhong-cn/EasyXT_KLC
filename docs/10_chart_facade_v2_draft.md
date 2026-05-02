# Chart Facade v2 草案

> 目标：为 KLineChart v9.8.12 与 KLineChart Pro 0.1.1 提供统一的前端图表门面接口，避免升级图表内核时把业务代码一起拖下水。

**代码落点**: `apps/tauri-shell/src/chart/chartFacadeV2.ts`
**状态**: ✅ 草案已落地，可作为团队讨论基线

---

## 为什么需要 Facade

当前仓库已经证明两件事：

1. **KLineChart 是核心资产，必须保**
2. **外壳不该再与图表实现细节深耦合**

所以图表升级正确的切口不是：

- 在业务代码里到处分支 `v9.8.12` / `Pro 0.1.1`

而是：

- 用统一 facade 层吸收图表实现差异

---

## v2 负责的能力范围

### 数据与上下文

- `setSymbol(symbol)`
- `setInterval(interval)`
- `setBars(bars)`
- `appendBars(bars)`

### 可视元素

- `setIndicators(indicators)`
- `setOverlays(overlays)`
- `setAnnotations(annotations)`
- `setTradeMarks(marks)`

### 运行状态

- `setTheme(theme)`
- `snapshot()`

### 工作区能力

- `saveLayout(presetId)`
- `loadLayout(presetId)`
- `exportImage()`

---

## 当前草案的意义

它不是最终实现，而是先把这些问题定死：

1. 新壳以后如何调用图表
2. 图表升级应该影响哪一层
3. 哪些能力必须是内核无关的

---

## 升级闸门

在把 `KLineChart Pro 0.1.1` 设为主实现前，必须先通过：

1. License / 商用边界核对
2. 离线打包能力验证
3. 事件桥 API 差异吸收
4. 多图布局与标注性能验证

如果闸门未过：

- 仍然继续走 Tauri 壳
- 但暂时保持旧图表内核 adapter

换句话说：

> **主线先切到 Web/Tauri，不必在第一天就硬切 Pro。**

---

## 与业务代码的边界

业务层以后只应该知道：

- symbol
- interval
- bars
- indicators
- overlays
- annotations
- trade marks
- layout

业务层不应该知道：

- 当前图表 JS 内核具体版本
- 某个底层 overlay API 的私有差异
- 某个内核要求的特殊 JSON 拼装细节

---

## 与当前仓库的关系

### 旧实现

- `gui_app/widgets/chart/chart_adapter.py`

### 新实现方向

- `apps/tauri-shell/src/chart/chartFacadeV2.ts`

前者解决的是 Qt 内部的图表适配；
后者解决的是新壳中的前端图表统一接口。

---

## 一句话总结

> **Chart Facade v2 不是“图表功能再包一层”，而是 EasyXT 图表资产从 Qt 时代迁移到 Tauri/Web 时代的结构性边界。**
