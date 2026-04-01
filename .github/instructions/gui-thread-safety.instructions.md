---
description: "Use when editing PyQt5 GUI code, QThread workers, closeEvent logic, KLineChart workspace, realtime connections, chart loading, xtquant access, or files under gui_app/. Covers test_mode guards, interruptible checkpoints, tiered waits, and xtdata safety in EasyXT_KLC."
name: "EasyXT GUI Thread Safety"
applyTo: "gui_app/**/*.py"
---
# EasyXT GUI Thread Safety

- 先看 `docs/05_thread_exit_safety_spec.md`；涉及线程退出、实时连接、图表工作区时，以 `gui_app/widgets/kline_chart_workspace.py` 为首选参考实现。
- 任何会触发实盘连接、QMT/xtquant、WS 行情或耗时探测的 GUI 路径，都必须保留 `test_mode` / `PYTEST_CURRENT_TEST` 守卫，避免测试环境误启真实连接。
- 新增或修改 `QThread.run()` 时，对每个可能阻塞超过约 100ms 的操作前后加入 `isInterruptionRequested()` 检查点；网络/I/O 操作要包裹异常处理，避免线程泄漏。
- `closeEvent()` 遵守分层超时：先停定时器；网络连接型线程约 1000ms，普通数据/处理线程约 200ms；普通线程先批量 `requestInterruption()+quit()`，再统一 `wait()`，最后仍要 `finally: super().closeEvent(event)`。
- 线程超时后的 `terminate()` 不能静默发生；按规范记录日志并上报 `THREAD_FORCED_TERMINATE` 事件。新增含线程的 QWidget 时，优先复用 `core/safe_thread_runner.py` 中的 `ThreadLifecycleMixin`。
- 所有 `xtdata` 调用都必须经过 `core/xtdata_lock.py` 的统一共享机制（优先 `xtdata_submit()`）；不要在 GUI 主线程、`threading.Thread` 或 `ThreadPoolExecutor` 中直接调用 xtquant/xtdata。
- GUI 启动和图表数据加载默认优先本地数据源（DuckDB / DAT / parquet）；不要把 `xtdata.get_market_data_ex()`、`xtdata.get_local_data()` 这类历史读取 API 重新引入 GUI 主流程。
- 修改 K 线图加载、backend 路由、WsBridge 握手或 chart adapter 时，优先触发 `kline-chart-debug` skill；若出现 `bsonobj.cpp`、`access violation`、DLL 缺失或并发崩溃，优先触发 `xtquant-crash-debug` skill。
- 不要为了“看见效果”在 GUI helper 或线程路径里加无条件 `print(...)`；沿用仓库现有 logger + 显式开关模式，避免测试和运行期输出噪音反扑。
