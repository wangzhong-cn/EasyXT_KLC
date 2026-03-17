# EasyXT 项目级线程退出安全规范

> 版本: 1.0
> 生效日期: 2026-03
> 适用范围: 所有在 `KLineChartWorkspace` 及其子组件中创建的 `QThread` 子类
> 维护者: 后端工程师（在更新任何 QThread 实现时必须同步更新本文档）

---

## 一、背景与根因分析

### 1.1 事故现象

在 Windows 平台上，关闭主窗口时以概率性出现进程崩溃：

```
STATUS_STACK_BUFFER_OVERRUN (0xC0000409)
```

崩溃 dump 分析追溯到 Qt 运行时在析构 `QThread` 对象时，该线程底层 OS 线程仍处于运行状态，导致 C++ 析构函数触发未定义行为。

### 1.2 根本原因

| 原因 | 具体问题 |
|------|---------|
| **线程过早析构** | `KLineChartWorkspace` 被 GC 回收时，其成员 `_RealtimeConnectThread` 等线程仍在运行 |
| **TDX 连接阻塞** | `_ensure_realtime_api()` 在测试环境中触发 `_RealtimeConnectThread`，调用 `connect_all()` 约需 7 秒，但 `closeEvent` 的 `wait()` 默认超时仅 200ms |
| **测试环境无守卫** | `pytest` 运行时未阻止实盘 API 连接，导致测试用例触发耗时网络操作 |
| **无强杀可观测性** | `terminate()` 调用后无日志、无事件记录，问题难以定位 |

### 1.3 已验证的修复

核心修复已于 `kline_chart_workspace.py` 中实施，完整测试结果：**276 passed, 1 skipped, exit 0**（`pytest tests/ -q`）。

---

## 二、四项核心原则

### 原则 1：测试环境守卫（test_mode guard）

凡是会触发实盘网络连接（TDX / TDX++ / xtquant）的逻辑，**必须**在入口处检查 `self.test_mode`，如果在测试模式下则立即返回。

```python
# ✅ 标准做法（kline_chart_workspace.py 第1653行）
def _ensure_realtime_api(self):
    if self.test_mode:
        return  # 测试环境跳过实盘 API 连接，防止 _RealtimeConnectThread 阻塞/崩溃
    if self.realtime_api is not None:
        return
    # ... 启动 _RealtimeConnectThread
```

```python
# ✅ test_mode 的标准初始化（__init__ 中）
self.test_mode = bool(os.environ.get("PYTEST_CURRENT_TEST"))
```

`pytest` 运行时会自动设置 `PYTEST_CURRENT_TEST` 环境变量，**此变量无需手动设置**。

---

### 原则 2：分段可中断（Interruptible Checkpoints）

所有耗时的 `QThread.run()` 实现**必须**在每个重操作的前后插入 `isInterruptionRequested()` 检查点。规则：**每个可能阻塞 > 100ms 的操作前必须设一个检查点**。

```python
# ✅ 标准做法（_RealtimeConnectThread.run()）
def run(self):
    try:
        if self.isInterruptionRequested(): return          # 检查点 1：进入前
        UnifiedDataAPI = importlib.import_module(...).UnifiedDataAPI
        if self.isInterruptionRequested(): return          # 检查点 2：模块加载后
        api = UnifiedDataAPI()
        if self.isInterruptionRequested(): return          # 检查点 3：对象创建后
        try:
            api.connect_all()                              # 耗时操作（约7秒）
        except Exception:
            pass
        if self.isInterruptionRequested(): return          # 检查点 4：耗时操作后
        self.ready.emit(api)
    except Exception as e:
        self.error_occurred.emit(str(e))
```

**最少检查点数量**：
- 轻量线程（DuckDB 读取）：首尾各 1 个，共 2 个
- 网络连接线程：每个 I/O 调用前后，共 ≥ 4 个
- 长循环线程：每次迭代至少 1 个

---

### 原则 3：分层超时等待（Tiered Wait Timeouts）

`closeEvent` 中等待各 QThread 退出时**必须**根据线程的最长可能运行时间分配等待超时。**严禁**对所有线程使用统一的长超时（会导致关闭窗口时卡顿）。

| 线程类型 | 等待时间 | 理由 |
|---------|---------|------|
| 网络连接型（`_RealtimeConnectThread`） | **1000ms** | `connect_all()` 平均 7s，中断后最多 1s 才能响应 |
| 数据库读取型（`_ChartDataLoadThread` 等） | **200ms** | DuckDB OLAP 查询快速，200ms 宽裕 |
| 数据处理型（`_DataProcessThread`） | **200ms** | pandas 计算在内存中执行 |
| 行情获取型（`_RealtimeQuoteWorker`） | **200ms** | HTTP/TDX 请求有 1.2s 内部超时 |
| 搜索/辅助型 | **200ms** | 轻量操作 |

---

### 原则 4：强杀可观测（Terminate Observability）

当 `wait(timeout)` 返回 `False`（线程未及时退出）时，**必须**通过事件总线记录强杀事件，**禁止**静默调用 `terminate()` 后不留记录。

```python
# ✅ 标准做法
if not rct.wait(1000):
    self._logger.warning(
        "closeEvent: _RealtimeConnectThread 未在 1s 内退出，强制终止"
    )
    rct.terminate()
    rct.wait(500)
    try:
        signal_bus.emit(
            Events.THREAD_FORCED_TERMINATE,
            thread_name="_RealtimeConnectThread",
            component="kline_chart_workspace",
        )
    except Exception:
        pass
```

`Events.THREAD_FORCED_TERMINATE` 已定义于 `core/events.py`，订阅者可用于：
- 生产监控统计强杀频次
- SRE 报警触发
- 故障排查时的事件溯源

---

## 三、项目全线程清单

> 来源：`gui_app/widgets/kline_chart_workspace.py`（截至 2026-03）

| # | 线程类 | 属性名 | 职责 | closeEvent 等待 | 中断检查点 | 备注 |
|---|--------|--------|------|----------------|-----------|------|
| 1 | `_RealtimeConnectThread` | `_realtime_connect_thread` | TDX 实盘 API 连接（约 7s） | **1000ms + terminate** | 4 个 | 唯一需要 terminate 的线程 |
| 2 | `_ChartDataLoadThread` | `_chart_load_thread` | DuckDB K线数据加载 | 200ms | 1 个（数据读取后） | 重复 key 已有 cache 跳过 |
| 3 | `_DataProcessThread` | `_data_process_thread` | DataFrame 格式化/准备 | 200ms | 无（快速内存操作） | 去抖 80ms 定时器保护 |
| 4 | `_LatestBarLoadThread` | `_latest_bar_thread` | 最新K线增量 DuckDB 读取 | 200ms | 1 个（数据读取后） | 2s 冷却防抖 |
| 5 | `_RealtimeQuoteWorker` | `_quote_worker` | 实时行情拉取（TDX/xtdata） | 200ms | 无（内部 1.2s timeout） | 每 1s 触发，运行中跳过 |
| 6 | `_FallbackSymbolThread` | `_fallback_thread` | DuckDB 找备用标的 | 200ms | 无（快速 SQL） | 自动备用回退 |
| 7 | `_InterfaceInitThread`（内部类） | `_interface_init_thread` | DuckDB 连接初始化 | 200ms（未显式等待） | 无 | 在 `_ensure_interface()` 内定义 |

---

## 四、标准 closeEvent 模板

以下是经过验证的 `closeEvent` 完整写法，**新增 QWidget 时必须参照此模板**：

```python
def closeEvent(self, event):
    try:
        # 步骤 1: 停止所有定时器（先停，防止在等待线程期间触发新任务）
        for timer in [self.update_timer, self.realtime_timer,
                      self.realtime_pipeline_timer, self._subchart_update_timer]:
            if timer and timer.isActive():
                timer.stop()

        # 步骤 2: 对网络型线程单独处理（超时 + 强杀 + 事件上报）
        rct = self._realtime_connect_thread
        if rct is not None and rct.isRunning():
            rct.requestInterruption()
            rct.quit()
            if not rct.wait(1000):              # 网络型：1s 宽限
                self._logger.warning("closeEvent: %s 未退出，强制终止", type(rct).__name__)
                rct.terminate()
                rct.wait(500)
                try:
                    signal_bus.emit(
                        Events.THREAD_FORCED_TERMINATE,
                        thread_name=type(rct).__name__,
                        component=self.__class__.__name__,
                    )
                except Exception:
                    pass

        # 步骤 3: 对其他线程先全部发送中断信号（不阻塞），再统一 wait
        other_threads = [
            self._chart_load_thread,
            self._latest_bar_thread,
            self._quote_worker,
            self._data_process_thread,
            getattr(self, "_fallback_thread", None),
        ]
        for t in other_threads:
            if t is not None and t.isRunning():
                t.requestInterruption()
                t.quit()
        for t in other_threads:
            if t is not None and t.isRunning():
                t.wait(200)                     # 其他型：200ms

    finally:
        super().closeEvent(event)               # 必须最后调用
```

**关键点**：
- 步骤 3 中所有线程**先全部发送中断信号，再统一 `wait()`**，而不是逐一 `requestInterruption → wait`，可避免串行累积超时（`N × 200ms`）
- `finally` 确保 `super().closeEvent(event)` 永远被调用

---

## 五、执行清单（新增 QThread 时必须完成）

在合并新增 `QThread` 的代码前，必须逐条确认以下清单：

### 5.1 线程实现

- [ ] **C1** `run()` 已添加 `isInterruptionRequested()` 检查点（网络/I/O 操作前后各 1 个）
- [ ] **C2** `run()` 中的每个耗时 I/O 操作都被 `try/except` 包裹，不会因异常导致线程泄漏
- [ ] **C3** 如果线程内有循环，循环体内至少有 1 个 `if self.isInterruptionRequested(): break`
- [ ] **C4** 线程不持有 Python 对象的强引用（信号 `emit` 时传值，不传 `self`）

### 5.2 父 Widget 端

- [ ] **C5** 线程属性已在父 `__init__` 中声明为 `Optional[QThread] = None`（避免悬空引用）
- [ ] **C6** 线程对象已通过 `thread.setParent(self)` 绑定父子关系
- [ ] **C7** `closeEvent` 中已按网络型/普通型分类处理（参照第四节模板）
- [ ] **C8** 网络型线程的等待超时 ≥ 1000ms；普通型线程 ≤ 200ms

### 5.3 测试要求

- [ ] **C9** 已为新线程添加至少 1 个 `closeEvent` 稳定性测试（参照 `TestKLineWorkspaceExitStability`）
- [ ] **C10** 测试覆盖"创建→立即关闭"场景（不等线程结束就关闭窗口）
- [ ] **C11** 如线程触发实盘连接，已通过 `test_mode` 守卫在测试环境中跳过

### 5.4 文档更新

- [ ] **C12** 本文档第三节《项目全线程清单》已更新，添加新线程的条目
- [ ] **C13** 如需使用 `Events.THREAD_FORCED_TERMINATE`，已确认 `core/events.py` 中存在该常量

---

## 六、验收标准

### 6.1 自动化测试验收

新增 QThread 必须在 `tests/test_trading_flow_integration.py` 中添加对应的稳定性测试组，验收标准为：

| 测试用例名称模式 | 通过标准 |
|---------------|---------|
| `test_single_create_close_no_exception` | 创建/关闭不抛出任何异常，进程不崩溃 |
| `test_repeated_create_close_N_times` | 连续 5 次创建/关闭均正常，无内存泄漏迹象 |
| `test_no_network_thread_started_on_create` | `test_mode=True` 时不启动实盘连接线程 |
| `test_all_qthreads_stopped_after_close` | `closeEvent` 后 100ms 内所有线程 `isRunning() == False` |
| `test_thread_forced_terminate_event_on_stuck_thread` | 模拟卡死线程，验证 `THREAD_FORCED_TERMINATE` 事件被发出 |

参见已有实现：[tests/test_trading_flow_integration.py](../tests/test_trading_flow_integration.py) — `TestKLineWorkspaceExitStability` 类（第35-39个测试用例）。

### 6.2 生产指标验收

| 指标 | 目标值 | 测量方式 |
|------|--------|---------|
| 正常关闭路径强杀频次 | **0 次/天** | 订阅 `THREAD_FORCED_TERMINATE` 事件，聚合计数 |
| 窗口关闭耗时（用户感知） | **< 1.5s** | 从 `closeEvent` 入口到 `super().closeEvent()` 出口 |
| 测试套件通过率 | **100%**（`tests/` 目录） | `pytest tests/ -q` exit 0 |
| 测试运行时有 `_RealtimeConnectThread` 启动 | **0 次** | pytest 运行时监控 `PYTEST_CURRENT_TEST` 守卫 |

### 6.3 代码审查验收

PR 中出现以下任意情形时，**审查人必须 Block 合并**：

1. 新增的 `QThread` 子类 `run()` 中没有任何 `isInterruptionRequested()` 检查
2. `closeEvent` 中对网络型线程使用 ≤ 200ms 的等待（会导致必然强杀）
3. `closeEvent` 中缺少 `finally: super().closeEvent(event)` 保护
4. 新增的实盘连接调用路径没有 `test_mode` 守卫
5. 使用裸 `terminate()` 而没有后续的 `wait()` 和事件上报

---

## 七、反模式汇总（禁止行为）

### ❌ 反模式 1：无检查点的长耗时 run()

```python
# ❌ 禁止：整个 run() 无任何可中断点
def run(self):
    api = build_heavy_connection()   # 可能阻塞 7+ 秒
    data = api.fetch_all()           # 可能阻塞 30+ 秒
    self.result.emit(data)
```

### ❌ 反模式 2：逐一串行等待

```python
# ❌ 禁止：串行 wait，三个线程最差需要 3 × 200ms = 600ms
for t in threads:
    t.requestInterruption()
    t.quit()
    t.wait(200)             # 每次都等，串行累积
```

### ❌ 反模式 3：静默强杀

```python
# ❌ 禁止：强杀后不记录
if not t.wait(1000):
    t.terminate()           # 完全静默，无法统计和定位问题
```

### ❌ 反模式 4：测试中触发实盘连接

```python
# ❌ 禁止：没有 test_mode 守卫
def _ensure_realtime_api(self):
    connector = _RealtimeConnectThread()   # 直接启动，测试中会阻塞 7 秒
    connector.start()
```

### ❌ 反模式 5：closeEvent 中不调用 super()

```python
# ❌ 禁止：直接 accept，跳过 Qt 的析构链
def closeEvent(self, event):
    self._stop_things()
    event.accept()          # 未调用 super().closeEvent(event)
```

### ❌ 反模式 6：对所有线程使用统一的长超时

```python
# ❌ 禁止：对轻量线程等待 5s，造成窗口关闭时明显卡顿
for t in threads:
    if t.isRunning():
        t.wait(5000)        # 5s 对于 DuckDB 查询线程完全没必要
```

---

## 八、历史事故记录

| 日期 | 现象 | 根因 | 修复方案 | 验证 |
|------|------|------|---------|------|
| 2026-02 | Windows `STATUS_STACK_BUFFER_OVERRUN` 进程崩溃，crash dump 可复现 | `_ensure_realtime_api()` 在测试环境中启动 `_RealtimeConnectThread`；`QThread` 对象析构时线程仍在运行 TDX `connect_all()` | 1. `test_mode` 守卫；2. `closeEvent` 分层超时 + `terminate()` + `THREAD_FORCED_TERMINATE` 事件；3. `_RealtimeConnectThread.run()` 4段可中断 | `TestKLineWorkspaceExitStability` 5 个用例全部通过；全量套件 276 passed, 1 skipped |

---

## 九、ThreadLifecycleMixin（代码化安全模板）

> 对应代码：[core/safe_thread_runner.py](../core/safe_thread_runner.py)

将第四节 `closeEvent` 模板沉淀为 `ThreadLifecycleMixin`，**新增包含 QThread 的 QWidget 应优先继承此 Mixin**，而非手写分层超时逻辑，以消除手写偏差。

### 9.1 使用方式

```python
from core.safe_thread_runner import ThreadLifecycleMixin
from PyQt5.QtWidgets import QWidget

class MyWidget(ThreadLifecycleMixin, QWidget):
    def __init__(self):
        super().__init__()
        # 注册网络型线程（超时 1s → terminate + 事件上报）
        self._register_network_thread("_realtime_connect_thread", wait_ms=1000)
        # 注册普通型线程（超时 200ms → 静默）
        self._register_thread("_chart_load_thread")
        self._register_thread("_data_process_thread")
        self._register_thread("_quote_worker")

    def closeEvent(self, event):
        try:
            self._stop_all_threads()   # 一行完成所有线程清理
        finally:
            super().closeEvent(event)  # 必须调用
```

### 9.2 方法说明

| 方法 | 说明 |
|------|------|
| `_register_network_thread(attr, wait_ms=1000)` | 注册网络型线程属性名；超时后 `terminate` + `THREAD_FORCED_TERMINATE` 事件 |
| `_register_thread(attr)` | 注册普通型线程属性名；200ms `wait`，超时静默 |
| `_stop_all_threads()` | 执行完整退出协议（网络型逐一处理，普通型批量处理） |

### 9.3 与手写模板的等价关系

`_stop_all_threads()` 严格等价于第四节的标准 `closeEvent` 模板：
- 网络型：`requestInterruption → quit → wait(N) → terminate（超时）→ wait(500) → THREAD_FORCED_TERMINATE`
- 普通型：先全部 `requestInterruption + quit`，再统一 `wait(200)`（批量，非串行）

### 9.4 新增 QThread 的检查清单补充

在已有 C1–C13 执行清单基础上，如使用 `ThreadLifecycleMixin`，追加：

- [ ] **C14** 新线程属性已在 `__init__` 中调用 `_register_network_thread` 或 `_register_thread` 注册
- [ ] **C15** `closeEvent` 调用了 `self._stop_all_threads()` 而非手写分层超时逻辑
- [ ] **C16** `closeEvent` 中保留了 `finally: super().closeEvent(event)` 保护

### 9.5 不适用场景（回退到手写模板）

以下情况继续使用手写模板：
1. 需要在网络型线程退出后执行额外的清理操作（如关闭 DB 连接）
2. 需要在各线程退出之间插入同步点
3. 测试环境需要 mock `_stop_all_threads` 行为

---

## 十、相关文档

- [architecture_roadmap_direction2.md](./architecture_roadmap_direction2.md) — 全架构演进路线图（含硬约束）
- [04_development_standards.md](./04_development_standards.md) — 通用开发规范
- `core/events.py` — 事件常量定义（含 `THREAD_FORCED_TERMINATE`）
- `tests/test_trading_flow_integration.py` — `TestKLineWorkspaceExitStability` 类（验收测试参考实现）
- `gui_app/widgets/kline_chart_workspace.py` — 规范的参考实现（`closeEvent` + `_RealtimeConnectThread`）
