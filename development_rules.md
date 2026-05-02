前置开发规则与参考资料

必须优先参考的官方资料
- EasyXT: https://github.com/quant-king299/EasyXT
- lightweight-charts-python: https://github.com/TriangleTraders/lightweight-charts-python
- QMT xtdata: https://dict.thinktrader.net/nativeApi/xtdata.html?id=5M2071#%E6%8E%A5%E5%8F%A3%E6%A6%82%E8%BF%B0

---

## 铁律 0：严禁模拟市场数据（最高优先级，不得以任何理由违背）

**本项目坚决禁止在任何代码、测试或工具脚本中使用虚构/模拟/伪造的市场行情数据。**

### 禁止范围（以下行为均属违规）

| 违规类型 | 典型示例 |
|----------|----------|
| 在测试中构造假 OHLCV 数据 | `pd.DataFrame({'close': [10.0, 15.0], 'volume': [1000, 1200]})` |
| 硬编码任何价格/成交量/涨跌幅数值 | `close = 100.0; volume = 50000` |
| 用随机数生成行情序列 | `np.random.randn(100)` 假装是价格 |
| 用"样本数据"做算法验证 | `sample_df = ...`（含任何非真实 A 股 OHLCV） |
| mock 行情查询函数返回假数据 | `patch('...get_kline', return_value=fake_df)` |

### 合规替代方案

1. **测试 Fixture（首选）**：在 `tests/fixtures/real_market_data.py` 中注册真实历史行情。
2. **DuckDB 测试库**：`tests/fixtures/market.duckdb` 存放真实 A 股历史数据切片，由 `conftest.py` 提供。
3. **mock 的 return_value 数据来源要求**：
   - 允许 mock 基础设施层（DuckDB 连接、文件 I/O、网络 I/O 等），但 `return_value` 中含有 OHLCV 内容时，**必须使用 `tests/fixtures/real_market_data.py` 中的真实数据**，绝不可硬编码价格/成交量。
   - `side_effect=Exception(...)` 不受此限制（测试错误路径，不涉及数据内容）。
   - `return_value=pd.DataFrame()` 不受此限制（空库行为测试）。
   - 正确示例：`patch.object(checker, '_query_df', return_value=get_000001_sz_daily())`
   - 错误示例：`patch.object(checker, '_query_df', return_value=pd.DataFrame({'close': [10.0, 15.0]}))`
4. **行为测试**：若测试目标是"当数据库为空时如何处理"，使用**真实空库**，不伪造数据。
5. **二进制编解码测试豁免**：仅限测试 DAT/二进制文件格式的解码算法本身（如 ÷1000 整数编码、UTC+8 时间偏移、OHLC 字节布局），允许使用整数常量作为编码参数。必须在注释中明确说明"此值为格式契约常量（QMT ×1000 格式整数），非市场价格"。

### 违规检测

CI 将运行 `tools/check_no_fake_market_data.py` 扫描以下关键字组合：
- `DataFrame` + 含硬编码价格/成交量数值的字典
- `return_value=` + 包含 `close/open/high/low/volume` 的 DataFrame
- 测试文件中出现 `[10.0`, `[100.0`, `[1000,` 等典型假价格

### 根因说明（写入于 2026-03-11）

> 背景：`TestDataIntegrityCheckerBasic` 中测试构造了 `{'close': [10.0, 15.0]}` 等假 OHLCV，
> 并通过 `mock checker.detector.con.execute` 注入——但 `_query_df` 实际调用路径是
> `checker.detector._manager.get_read_connection()` 上下文管理器，导致 mock 完全失效，
> 5 个测试悄无声息地测了错误的代码路径，掩盖了真实 bug。
>
> 根本原因：**假数据 + 错误 mock 层 = 测试绿了但代码路径未覆盖**。
> 正确做法：用真实历史数据 + 在正确层（`patch.object(checker, '_query_df', ...)`）替换基础设施。

---

### 铁律 0 补充：桩化边界——红线 vs 白线（写入于 2026-03-21）

铁律 0 禁止的是**业务金融数据层的模拟**。但测试不得不隔离外部依赖。
本节正向定义"什么可以桩化"（白线）与"什么绝对不可以"（红线），消除灰区。

#### 红线（禁止桩化 / 禁止伪造）

| 类别 | 含义 | 举例 |
|------|------|------|
| 行情数据 | OHLCV、Tick、成交量、涨跌幅 | `pd.DataFrame({'close': [10.0, 15.0]})` |
| 交易结果 | 收益率、回撤、盈亏、持仓成本 | `return_value={'profit': 0.05}` |
| 风控判定 | VaR 数值、熔断触发条件 | `mock_var.return_value = 0.02` |
| 订单状态 | 撮合结果、成交回报、委托状态 | `order.status = 'filled'`（硬编码非真实回报） |

> **判定标准**：如果这个值会影响交易决策的正确性判断，就是红线。

#### 白线（允许桩化 / 鼓励隔离）

| 类别 | 含义 | 举例 |
|------|------|------|
| 模块可用性 | SDK/插件是否已安装 | `patch.dict("sys.modules", {"xtquant": None})` |
| GUI fixture | Qt 组件实例化、widget 渲染 | `qtbot.addWidget(panel)` / `_qt_available()` 检查 |
| 网络 I/O | HTTP 请求、WebSocket 连接 | `patch("requests.get", side_effect=Timeout)` |
| 文件 I/O | 读写磁盘、路径解析 | `patch("builtins.open", side_effect=PermissionError)` |
| 线程/进程调度 | 守护线程启动、线程池提交 | `patch.object(udi, "_ensure_backfill_scheduler")` |
| 时间控制 | 冻结当前时间做确定性断言 | `freezegun.freeze_time("2026-03-21")` |
| 数据库连接 | 连接对象本身（非查询结果中的业务数据） | `MagicMock(spec=duckdb.DuckDBPyConnection)` |

> **判定标准**：如果这个值只影响"测试能否执行"而非"业务逻辑是否正确"，就是白线。

#### 灰区处理规则

当一个桩同时涉及基础设施和业务数据时（例如 mock 一个返回 DataFrame 的数据库查询），遵循以下原则：

1. **基础设施层可以 mock**：连接对象、cursor 对象、网络传输层。
2. **return_value 中包含 OHLCV 内容时，必须使用 `tests/fixtures/real_market_data.py` 中的真实数据**（已在铁律 0 合规替代方案第 3 条定义）。
3. `side_effect=Exception(...)` 不受红线约束（测试错误路径，不涉及数据内容）。
4. `return_value=pd.DataFrame()` 空 DataFrame 不受红线约束（空库行为测试）。

#### 已验证的白线实例

| 文件 | 桩化内容 | 归属 |
|------|----------|------|
| `test_convergence_contract.py` | `patch.dict("sys.modules", {"xtquant": fake_mod, "xtquant.xtdata": fake_xtdata_mod})` | 白线：模块可用性 |
| `test_operation_panel_smoke.py` | `_qt_available()` 检查 PyQt5 + pytestqt 安装 | 白线：GUI fixture |
| `test_data_manager_units.py` | `patch.dict("sys.modules", {"xtquant": None})` | 白线：模块可用性 |
| `conftest.py` | `_clear_tushare_token_env` session fixture | 白线：环境变量隔离 |

#### 违规处理

- 在红线上使用桩/伪造数据：**视同铁律 0 违规**，必须在合并前修复。
- 在白线上缺少桩（导致环境耦合）：作为 P2 治理项，鼓励补桩但不阻断合并。
- 灰区争议：提交 PR 时在 description 中说明理由，由 review 定性。

#### PR 提交前自检清单

每次提交涉及 `tests/` 变更的 PR 时，在 description 中勾选以下项：

```markdown
### 红线 / 白线自检
- [ ] 本 PR 是否触及红线数据桩化？（Yes → 必须附证据路径，说明为什么不可避免，并由 review 审批）
- [ ] 本 PR 新增的 mock/stub 是否均属于白线范畴？（参照 development_rules.md 铁律 0 补充）
- [ ] 如涉及灰区（基础设施 mock 但 return_value 含业务数据），是否使用了 `tests/fixtures/real_market_data.py`？
```

#### 门禁自动扫描

`p0_gate_check.py` 内置 `--check fake_ohlcv` 红线巡检（warn 级），扫描 tests/ 下硬编码的 OHLCV 赋值模式：

```bash
python tools/p0_gate_check.py --check fake_ohlcv   # 单项扫描
python tools/p0_gate_check.py --summary              # 全量汇总中自动包含
```

> 当前为 **warn 级（不阻断）**，用于早期预警。待伪造实例清零后可升级为 fail 级阻断。

---

## DuckDB 空库根因分析（写入于 2026-03-xx）

### 根因：DuckDB 为何为空（767 万条记录去哪了？）

**背景**：本项目 `D:/StockData/stock_data.ddb` 文件虽然存在，但所有表均为 0 行。

**根因链（共 6 层）**：

| 层级 | 根因 | 位置 |
|------|------|------|
| 1 | **下载脚本从未执行** | `tools/download_all_stocks.py`、`tools/download_qmt_data.py` |
| 2 | **需要 MiniQMT 守护进程运行且已登录券商** | `xtdata.download_history_data()` 必须通过 MiniQMT 代理 |
| 3 | **两阶段管道未触发**：QMT→.dat 是第一阶段；.dat→DuckDB 是第二阶段 | `dat_binary_reader.py` + `universal_data_importer.py` |
| 4 | **`auto_data_updater.update_single_stock()` 是残留桩代码** | 注释："简化处理，实际应使用 import_bonds_to_duckdb.py" |
| 5 | **DuckDB 文件由 `resolve_duckdb_path()` 自动创建（空库），但未填充** | `duckdb_connection_pool.py` |
| 6 | **767 万条记录非预打包** — 是运行全量下载 5 年 × 5000 支股票后的预期结果 | `QUICK_START.md`：DuckDB 是可选加速方案 |

**完整数据管道**（手工执行顺序）：
```
1. 启动 MiniQMT（XtItClient.exe）并登录券商账号
2. conda activate qmt311
3. python tools/download_all_stocks.py   # 下载全量 A 股日线/分钟线到 .dat 文件（约 30-90 分钟）
4. python tools/migrate_qmt_db.py        # 或 tools/download_qmt_data.py 将 .dat 导入 DuckDB
5. python tools/_check_duckdb.py         # 验证入库行数
```

**替代方案（无 MiniQMT）**：
```
1. 设置 TUSHARE_TOKEN 环境变量
2. python tools/download_tushare.py      # 通过 Tushare API 直接获取并写入 DuckDB
```

### 数据管道审计结论（2026-03 更新）

| 维度 | 状态 | 说明 |
|------|------|------|
| 多源数据接入 | ✅ 已实现 | DuckDB→QMT→DAT→Tushare→AKShare 五层 fallback，含 circuit breaker；DAT 含时效门限 `EASYXT_DAT_STALE_HOURS` |
| 交叉验证兜底 | ✅ 已实现 | `tools/check_cross_source_consistency.py` 每日收盘后 10 分钟抽检 DuckDB vs DAT，`auto_data_updater` 自动调度 |
| 统一环境变量 | ✅ 已实现 | 全部 13 个 `EASYXT_*` 变量已归集至 `.env.example`（含 TUSHARE_TOKEN / API_TOKEN / DAT_STALE_HOURS / 隔离队列告警阈值 / 跨源抽检参数） |
| 交易日期边界 | ✅ 已验证 | 时区违例仅存于 `external/EasyXT*/`（存档副本），生产代码 `easy_xt/data_api.py` 与 `auto_data_updater.py` 已全部使用 `ZoneInfo('Asia/Shanghai')` |
| 数据格式归一 | ✅ 已实现 | AKShare 中文列名、QMT ms 时间戳、DAT ×1000 整数均有转换器 |
| 统一门禁入库 | ✅ 已实现 | `auto_data_updater.update_single_stock()` 对接 `build_incremental_plan + get_stock_data(auto_save=True)` 链路 |
| 数据血缘完整 | ✅ 已实现 | `financial_data_saver` 已集成至 `auto_data_updater` 调度（收盘后 20 分钟，仅季报披露月执行） |

**当前无 P1 待修复项。**

P2 增强方向：

- 跨源一致性检查扩展至 Tushare/AKShare（当前仅 DuckDB vs DAT）
- `financial_data_saver` 添加 Tushare 补充路径（QMT 不可用时降级）


- 任何系统错误与性能问题，首先对照以上三处官方文档与核心代码路径定位原因
- QMT xtdata 必须由 MiniQMT 运行提供服务，数据下载与获取由 MiniQMT 执行并回传
- 历史行情必须先调用 download_ 系列补充本地数据，再使用 get_ 系列读取
- 实时数据使用 subscribe_ 系列
- 优先使用 download_history_data2，按官方参数约定处理时间格式与参数形态

架构一致性
- 图表渲染使用 lightweight-charts，业务操作面板沿用 EasyXT UI
- 数据源统一经 DuckDB，本地优先、必要时回退 QMT 并回写入库
- 任何 UI 触发的数据请求必须避免阻塞主线程
- **读链路不自愈，修复链路默认开启**：`/api/v1/chart/bars`、Qt/Tauri 图表、策略/回测等读路径只消费本地事实与质量状态，禁止在读取请求内顺手联网补历史或写库。
- **Golden 1D 修复必须异步、可观察、可重试、可死信、可复审**：诊断层只签发 repair plan；后台编排层将可自动修复项入 `HistoryBackfillScheduler`；回填成功后必须重新跑 `Golden1dAuditor` 复审，`manual_review/blocked` 问题不得伪装成 queued/complete。

---

## 阶段进度记录

### Phase 0（基础硬指标）— 已关闭 [2026-03-08]

**退出条件**（全部达成）：
- ✅ 覆盖率：TOTAL 40.1%（已超过 40% 门槛）
- ✅ 全量稳定：1637 passed，1 skipped，0 failed
- ✅ 风控体系：VaR 在线更新、回撤熔断集成 EasyXT buy/sell，每日调度器已完成
- ✅ 策略框架：BaseStrategy + strategies/registry.py 策略注册中台已完成
- ✅ 实盘链路：QMT 账户 1678070127 验证脚本跑通（资产读取 + 风控拦截 + 审计日志）

**关键经验（写入 CI 知识库）**：
> `gui_app/backtest/data_manager.py` 原本在 `DataManager.__init__` 中执行
> `del sys.modules["data_manager"]`，导致后续测试大规模"伪失败"。
> 根因修复（2026-03-08）：改用 save→临时替换→finally restore 模式，
> 同时将 `sys.path` 插入行为纳入 finally 清理，从源头消除全局污染。
> 专项回归测试：`tests/test_data_manager_module_integrity.py`（4用例）。

---

### Phase 3（轻量化中台）— 进行中 [2026-03 起]

**目标**：
1. FastAPI + WebSocket 中台服务（统一行情、交易 API 接口层）
2. 策略生命周期管理 REST API（基于 DuckDB 策略注册表）
3. 内置示例策略重构（适配规范化 BaseStrategy）

**已完成**：
- ✅ `core/api_server.py`：FastAPI + WebSocket 中台服务
  - `_MarketBroadcaster`：per-symbol seq 递增 + event_ts_ms 注入 + asyncio.wait_for 超时隔离
  - `ingest_tick_from_thread()`：QMT 回调线程安全桥接（run_coroutine_threadsafe）
  - `_verify_auth_and_rate`：Token 鉴权（X-API-Token）+ 滑动窗口限流（60 req/min/IP）
  - 所有 REST 端点加 `Depends(_verify_auth_and_rate)`，/health 豁免
  - 状态 PATCH 对接 update_status() 元组返回值（409 非法转换 / 404 未找到）
- ✅ `strategies/registry.py`：update_status 状态机约束（created→running→paused→stopped，返回 Optional[tuple]）
- ✅ `strategies/examples/ma_cross_strategy.py` + `bollinger_strategy.py`：参数边界校验 + NaN/零价格防护
- ✅ 测试覆盖：TestAuth / TestStateMachine / TestRateLimit + TestMACrossStrategyBoundary + TestBollingerMeanRevStrategyBoundary

**Phase 3 收尾项（本轮完成）**：
- ✅ `tests/test_pipeline_smoke.py`：真实链路冒烟（broadcaster seq/慢消费者/registry 生命周期/HTTP 链路/WS 鉴权），15 用例
- ✅ `tests/fixtures/openapi_schema.json`：OpenAPI 契约基线（Golden Master），首次运行自动生成；后续运行 diff 检查端点删除及 HTTP 方法删除
- ✅ `tests/test_openapi_contract.py`：4 用例（端点删除防护 / HTTP 方法删除防护 / 核心端点存在性 / /health 响应字段）
- ✅ `tests/test_strategy_replay.py`：固定历史行情回放，12 用例（MA Cross + Bollinger，确定性 / 零信号 / 数据不足 / 已知触发点）
- ✅ `core/api_server.py`：`_rate_limit_hits` 计数器（全局累计，不复位），暴露于 `/health` 的 `rate_limit_hits` 字段
- ✅ `core/api_server.py`：统一错误格式 `{code, message, detail, trace_id}`（自定义 `@app.exception_handler(HTTPException)`）
- ✅ `tests/test_api_server.py`：TestRateLimit 新增命中计数 / TestErrorFormat（5 用例：404/409/401/429/trace_id 唯一性）

**Phase 3 验收门槛**（全部满足方可关闭）：

| 指标 | 标准 |
|------|------|
| REST/WS 稳定性 | `pytest tests/test_api_server.py` 全绿，0 错误 |
| 策略状态一致性 | `update_status` 状态机全覆盖，非法转换返回 409 |
| 参数防护 | 非法参数在 `on_init` 抛 `ValueError`，NaN/零价格在 `on_bar` 静默跳过 |
| 端到端链路 | `/health` → `/api/v1/strategies/` → `PATCH status` → `/api/v1/strategies/snapshot` 完整链路可通 |
| 鉴权 & 限流 | 无 Token 返回 401，超限返回 429，/health 始终可访问 |
| 全量回归 | `pytest --tb=short -q` 通过率 ≥ pre-Phase3 基线（16 + 16 策略测试 + 新增边界/中台测试）|

**Phase 3 已关闭 [2026-03-15]**：所有验收门槛达成。测试基线 2277 passed / 10 skipped，P0 gate `strict_pass = true`。

---

### Phase 4（GUI 功能完善）— 已关闭 [2026-03-15 起 / 验收通过]

**目标**：完成 Stage 2.2 原生图表画线工具；完善本地缓存管理；保持 P0 gate 绿色。

**已完成**：
- ✅ `gui_app/backtest/data_manager.py`：`clear_local_cache(symbol=None)` 全量实现
  - 单标的：调用 `storage.delete_data(symbol, 'daily')`
  - 全量：glob `root_dir/daily/*.parquet` 逐一 `unlink()`
  - 完成后刷新 `source_status[DataSource.LOCAL]`
- ✅ `gui_app/chart_native/chart-bridge.js`：Stage 2.2 Primitives API 完整实现
  - `chart.addDrawing`：hline / tline / vline 三种类型
  - `chart.removeDrawing`：`removePriceLine` / `detachPrimitive` 清理
  - `chart.loadDrawings`：批量恢复（换标的时调用）
  - `chart.getDrawings`：返回纯元数据（剥离内部引用，供持久化）
  - `_makeTrendLinePrimitive`、`_makeVerticalLinePrimitive`、`_applyLineDash` helper
  - 事件回推：`chart.drawingCreated` / `chart.drawingDeleted`
- ✅ `gui_app/widgets/chart/rpc_protocol.py`：`build_add_drawing` 重写（hline/tline/vline 平铺参数）；新增 `build_remove_drawing`
- ✅ `gui_app/widgets/chart/chart_adapter.py`：`NativeLwcChartAdapter` 新增公开画线 API
  - `add_drawing(type, **kwargs) → id`
  - `remove_drawing(id)`
  - `load_drawings(drawings)`
  - `get_drawings(timeout) → list`
  - `on_drawing_created/deleted/updated(callback)`

**Phase 4 已关闭 [2026-03-18]**：所有验收门槛达成。测试基线 3764 passed / 1 skipped，P0 gate `strict_pass = true`，画线 API 无 TODO。

---

### Phase 5（覆盖率提升 + 测试稳定性）— 进行中

**目标**：整体覆盖率从 50.5% 提升至 ≥60%；消除所有已知 flaky/crash 根因；测试基线 ≥3844 passed。

**当前状态 [2026-03-18]**：
- 总覆盖率：50.5%（16409/32490 语句），来源：`cov_combined.json`（v7.13.4，2026-03-18T03:18）
- 测试基线：3844 passed / 1 skipped（排除 6 个 QMT 依赖文件 + `test_trading_flow_integration.py`）
- UDI 测试 crash 已修复：`test_unified_data_interface.py` 75 tests pass（原来 Fatal Python error: Aborted）

**已完成**：
- ✅ 修复 `tests/test_unified_data_interface.py::TestScheduleBackfill::test_returns_false_when_scheduler_none`
  - 根因：`_ensure_backfill_scheduler` 启动真实 daemon 线程泄漏，污染后续 DuckDB 操作导致 `abort()`
  - 方案：`with patch.object(udi, "_ensure_backfill_scheduler"):` 阻止真实线程启动
- ✅ 修复 `tests/test_coverage_boost_t2d.py::TestBuildBarFromQuote::test_bar_has_ohlcv`
  - 根因：用 `"5m"` 周期调用 `_build_bar_from_quote`，盘中时间门控返回 `None`（非交易时段）
  - 方案：改用 `"1d"` 周期绕过 `_is_intraday_market_time` 门控

**主要覆盖率机会（非 GUI）**：
| 模块 | 当前覆盖率 | 语句数 | 目标 |
|------|-----------|--------|------|
| `data_manager/unified_data_interface.py` | 54.2% | 2569 | ≥65% |
| `data_manager/duckdb_fivefold_adjust.py` | 71.7% | 269 | ≥85% |
| `data_manager/pipeline_health.py` | 76.7% | 73 | ≥90% |
| `core/api_server.py` | 79.0% | 509 | ≥88% |

**稳定套件定义**（6 个文件永久排除，原因：需要 QMT 进程）：
```
--ignore=tests\test_realtime_pipeline.py
--ignore=tests\test_qmt_feed.py
--ignore=tests\test_qmt_integration.py
--ignore=tests\test_tick_stress.py
--ignore=tests\test_realtime_pipeline_manager.py
--ignore=tests\test_triple_source_manager.py
```
`test_trading_flow_integration.py`：单独运行 exit -1073740791（Windows Qt/asyncio 崩溃，pre-existing），不计入稳定套件。

---

### 背景

2026-03 测试治理专项暴露两类高频陷阱，已导致多轮 flaky/环境污染失败。
本节将历史根因固化为可执行规范，配合 `.github/workflows/hermetic-tests.yml` 形成"文档+测试+CI"三重保险。

---

### 铁律 1：禁止用 `patch builtins.__import__` 拦截 xtquant

**错误写法（禁止）**：

```python
with patch("builtins.__import__", side_effect=lambda name, *a, **kw:
           (_ for _ in ()).throw(ImportError()) if "xtquant" in name
           else __import__(name, *a, **kw)):
    ...
```

**根因**：`else __import__(name, ...)` 中的 `__import__` 在 patch 激活期间解析为已被替换的版本，
产生无限递归 → `RecursionError` → 若被 `except Exception: pass` 吞掉，断言静默跳过，
测试看似通过但实际未执行任何有效验证（flaky 根因）。

**正确写法（强制）**：

```python
import data_manager.board_stocks_loader as bsl_mod
with patch.dict("sys.modules", {"xtquant": None, "xtquant.xtdata": None}):
    loader = bsl_mod.BoardStocksLoader()
assert loader.available is False   # with 块外断言，sys.modules 已恢复
```

`sys.modules[key] = None` 触发 Python import 机制直接抛 `ImportError`，无需任何 `__import__` 拦截。
**回归保护**：`tests/test_data_manager_units.py::TestBoardStocksLoaderNoQMT::test_init_no_double_import_patching_needed`

---

### 铁律 2：外部源 mock 必须用 `side_effect=AssertionError`，不得用 `return_value=DataFrame()`

测试"当 QMT/Tushare/AKShare 均不可用时返回空 DataFrame"的用例，
若将外部源 mock 设置为 `return_value=pd.DataFrame()`，测试可以通过——
但这掩盖了"代码路径根本没有进入外部源分支"的情况，无法区分以下两种场景：
- 场景 A：外部源被正确跳过（符合预期）
- 场景 B：外部源被调用了但返回空（测试意外静默容错，掩盖回归）

**正确写法（强制）**：

```python
_must_not_call = AssertionError("外部数据源不应在此测试中被调用")
with patch.object(udi, "_read_from_qmt",     side_effect=_must_not_call):
with patch.object(udi, "_read_from_tushare", side_effect=_must_not_call):
with patch.object(udi, "_read_from_akshare", side_effect=_must_not_call):
    result = udi.get_stock_data(...)
assert result.empty
```

若未来有人错误移除 `_tushare_checked=True` 等状态标志，测试立即以 `AssertionError` 失败，暴露回归。

---

### 铁律 3：构造 UDI mock 实例必须固化全部数据源状态标志

`_make_udi_with_mock_duckdb` 或类似工厂方法在构造 `UnifiedDataInterface` mock 实例时，
**必须**同时固化以下 6 个状态标志，防止 `get_stock_data` 进入实时检测逻辑：

```python
udi.qmt_available     = False
udi._qmt_checked      = True
udi.tushare_available = False
udi._tushare_checked  = True
udi.akshare_available = False
udi._akshare_checked  = True
```

漏掉任何一个 `_checked=True`，对应的 `_check_xxx()` 方法在测试运行时会探测本机环境，
导致"本机有 QMT/Tushare token 时测试走在线路径返回真实数据"的环境污染。

---

### 铁律 4：Session 级 token 环境变量由 conftest 统一清空

**不要在每个测试中单独 monkeypatch token 变量**。
`tests/conftest.py` 中已有 session 级 autouse fixture `_clear_tushare_token_env`，
测试 session 启动时自动清空 `EASYXT_TUSHARE_TOKEN` 和 `TUSHARE_TOKEN`，
session 结束后恢复（对持续开发的本地机器友好）。

CI 层面，`.github/workflows/hermetic-tests.yml` 在 workflow 级别也设置了空值（双保险）。

若需在测试中临时恢复 token（如测试 token 解析逻辑），使用 pytest `monkeypatch.setenv`
（函数级作用域，不影响其他用例）。

---

### CI 专项门禁

`.github/workflows/hermetic-tests.yml` 在以下情况触发：
- PR 到 `main/master/develop`
- 直接 push 修改了 `tests/conftest.py`、相关测试文件或 `data_manager/` 核心模块

**关键特性**：
- `fail-fast: true`：第一个用例失败立即终止，避免后续用例掩盖根因
- runner 级 `TUSHARE_TOKEN=""` 覆盖：防止 GitHub Secrets 意外注入
- 前置检查：验证 `xtquant` 未安装、token 变量为空，否则以 `::error` 注解终止

任何 hermetic 测试失败代表测试隔离性受损，**必须在合并前修复，不接受临时跳过**。

---

### P0-C SQL 防御规范（参数化 + allowlist）

涉及数据库读写的代码必须遵循以下约束：

1. 值参数一律使用占位符绑定（DuckDB `?`），禁止将 `stock_code`、日期区间、条件值拼接进 SQL 字符串。
2. `_build_stock_daily_delete_sql` 统一返回 `(sql, params)`，调用方必须执行 `con.execute(sql, params)`。
3. 动态表名仅允许来自白名单（如 `_ALLOWED_WRITE_TABLES`、合法周期集合），非白名单直接拒绝。
4. `pragma_table_info`、`information_schema` 等元数据查询同样要求表名先经 allowlist 校验。
5. 允许保留的字符串插值仅限 schema 常量路径，必须在代码审查中给出来源证明，不得来自 UI/外部输入。
6. 变更后必须运行 `python -m ruff check --select S608`，确保高危注入点为 0。

---

## 跨平台 CLI 命令速查

在 **Windows PowerShell** 环境下执行测试或查看日志时，Unix 命令不可用，请使用以下等价写法：

| 需求 | bash / zsh | Windows PowerShell |
|------|-----------|-------------------|
| 查看输出最后 N 行 | `cmd \| tail -n 20` | `cmd \| Select-Object -Last 20` |
| 查看输出前 N 行 | `cmd \| head -n 20` | `cmd \| Select-Object -First 20` |
| 过滤含关键字的行 | `cmd \| grep "pattern"` | `cmd \| Select-String "pattern"` |
| 计行数 | `wc -l file.txt` | `(Get-Content file.txt).Count` |
| 递归删除目录 | `rm -rf dir/` | `Remove-Item -Recurse -Force dir/` |
| 查看文件末尾（实时跟踪） | `tail -f file.log` | `Get-Content file.log -Wait` |
| 设置环境变量（当前会话） | `export VAR=val` | `$env:VAR = "val"` |

**推荐测试调用模板（PowerShell）**：
```powershell
# 运行指定测试文件，只显示最后 20 行
python -m pytest tests\test_stability_tools.py tests\test_stage1_batch.py -q --tb=short 2>&1 | Select-Object -Last 20

# 带覆盖率
python -m pytest tests\ --cov=. --cov-report=term-missing -q 2>&1 | Select-Object -Last 40

# 实时查看 CI log 文件
Get-Content logs\ci.log -Wait
```

> **约定**：项目文档中所有命令行示例统一给出 PowerShell 版本；需要 bash 版本时在同一行用注释标注 `# Linux/macOS`。

---

## 运营协议（Stage 1 实样本周）

### 协议 1：每日运行日志一行制

每日 `daily-batch` 结束后，**手动追加一行**到 `artifacts/run_log.txt`，格式固定：

```
YYYY-MM-DD | run_id=<GITHUB_RUN_ID> | commit=<SHA[:7]> | generator=batch_diff.py@v1 | alert=<OK|WARN|CRITICAL> | flip=<N> | new_err=<N>
```

字段全部来自 `diff_report_*.json`：`alert_level`、`flip_count`、`new_error_count`。
`run_id` / `commit` 从 CI Job Summary 顶部取，5 秒完成记录。

---

### 协议 2：周末阈值决策树

```
读取 diff_report_*.json 的 state_changes[]
  │
  ├─ new_error 为 true 的条目数 > 0 ？
  │    YES → 本周只允许：
  │            · 提高 min_sharpe
  │            · 降低 max_drawdown
  │            · 提高 min_trading_days
  │          禁止：
  │            · 向 assets[] 新增标的
  │            · 降低任何现有 thresholds 值
  │
  └─ NO  → 可正常调整，但：
            · 每次只改一个维度
            · 下周验证后再改第二个
            · 每次改动必须在 _change_log 写 issue_ref
```

---

### 协议 3：周五复盘三张表（5分钟标准格式）

**表 1 — 状态翻转表**（数据源：`state_changes[]` 按 `group` 分组计数）

| 分组 | pass→fail | fail→pass | 净变化 |
|------|-----------|-----------|--------|
| *(按组填写)* | | | |

**表 2 — 新增错误 TopN**（数据源：`state_changes[]` 过滤 `new_error=true`）

| symbol | name | group | severity | error_b（前 120 字） | 首次出现日期 |
|--------|------|-------|----------|----------------------|--------------|
| *(TopN)* | | | CRITICAL / WARN | | |

> `severity` 来源：若所在分组 `access_alert_active=true` 或 `slippage_consistent=false` → CRITICAL；其余 → WARN。

**表 3 — 阈值影响表**（数据源：`groups[].pass_rate_delta` + `_change_log.issue_ref`）

| 改动项 | 改动前 | 改动后 | pass_rate Δ | median_sharpe Δ | accessibility Δ | 决策 |
|--------|--------|--------|-------------|-----------------|-----------------|------|
| *(按改动填写)* | | | | | | 收紧/保持/回滚 |

> `决策` 列在复盘结束时填写，直接形成下周执行单，不二次讨论。

---

## T+0 基线冻结与观测协议（2026-03-21 起）

### 当前状态：🔒 代码冻结期（T+1 ~ T+3）

**基线锚定**：

| 指标 | T+0 锚定值 |
|------|-----------|
| 测试通过数 | 4704 passed / 10 skipped / 0 failed |
| P0 门禁 | 16 checks: 15 OK + 1 WARN (fake_ohlcv=452) |
| strict_pass | True |
| P0_open_count | 0 |
| watchdog_consecutive | ≤2（阈值 3） |
| fake_ohlcv 锚点 | 452（delta 口径：增量 ≤0 合规） |

**冻结规则**：T+1 ~ T+3 期间（2026-03-22 ~ 2026-03-24）禁止任何功能性代码变更。每日仅执行：

1. `python tools/p0_gate_check.py` — 确认 strict_pass 不退化
2. `python -m pytest tests/ -q --tb=short` — 确认通过数 ≥4704
3. 结果填入观测日志

**硬触发规则**：
- watchdog_consecutive ≥ 3 → 立即快照 + 保留证据
- fake_ohlcv 增量 > 0（超过锚点 452）→ 定位新增来源并回滚

**详细协议**：见 `artifacts/OBSERVATION_PROTOCOL_20260321.md`

**T+4 解冻序列**（如连续 3 天绿灯）：
1. 续期 `artifacts/watermark_approval_registry.json` → AP-RISK-WARN expires_at
2. 修复 `tests/test_coverage_boost_t2f.py:350` → `freq="T"` 改 `freq="min"`
3. 运行全量门禁 → 确认 16/16
4. 门禁通过 → 正式解冻
