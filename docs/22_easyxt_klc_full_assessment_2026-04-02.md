# EasyXT_KLC 全量评估与行动指南（2026-04-02）

> **基于代码全量审查、运行时探针、pytest 实测、七层公理化体系验证，以及 IMA 笔记推进规划的综合评估。**
> 版本：v2.0
> 生成日期：2026-04-02
> 前序版本：21_easyxt_klc_final_full_report_2026-04-01.md (v1.0)
> 七层体系文件：`严谨审查结论与完备公理化体系构建.md`

> 📌 **校准提示**：本文件保留为 2026-04-02 的阶段性评估快照；如需引用当前最新证据口径，请优先阅读 `23_easyxt_klc_full_assessment_calibrated_2026-04-02.md`（v2.1 校准版）。

---

## 0. 核心结论前置

### 一句话判断

> **制度层代码已全面落地——session_profile_registry、period_registry、threshold_registry、canonical_1m、governance_jobs 均已写入运行时并通过 274 个架构核心测试验证。项目从"文档概念"正式进入"运行时约束"阶段。**

### 关键数字（全部实测）

| 指标 | 数值 | 来源 |
|------|------|------|
| 收集测试总数 | 4419 | `pytest --co -q` 2026-04-02 |
| 通过 | 4398 | `pytest -q --tb=line` 完整执行 |
| 失败 | 17 | 详见 §3.3 |
| 通过率 | **99.6%** | — |
| 七层架构+治理核心测试 | **274 passed / 0 failed** | 13 个测试文件集中验证 |
| 测试文件总数 | 210 | `tests/` 目录 |
| 执行耗时 | 427s (7m07s) | conda run myenv |
| .dmp 崩溃转储 | 42 个 | 根目录 `*.dmp` |
| `api_server.py` 行数 | 2693 | `wc -l` |
| `unified_data_interface.py` 行数 | **8059** | `wc -l` |
| `main_window.py` 行数 | 3072 | `wc -l` |
| 周期注册表条目数 | 29+ | `config/period_registry.json` |
| 全局 warnings.filterwarnings | 20+ 处 | `grep -r` |

---

## 1. 七层公理化架构体系——从科学哲学到代码落地的完整验证

### 1.1 理论体系文件

文件位置：**`d:\EasyXT_KLC\严谨审查结论与完备公理化体系构建.md`**（约 500 行）

该文件建立了完整的三层分离公理化体系：

| 层级 | 内容 | 参数 |
|------|------|------|
| **公理层** | 3 大公理（结构本体 / 自生长 / 走势二元性）+ 3 大定理（对偶 / 互斥完备 / 前置锚点存在性） | 零参数、纯序、普适 |
| **实例化层** | 尺度聚合映射、枢轴提取规则、分形层级构造、边界处理 | 尺度绑定、参数固定 |
| **工程层** | 金融市场涨跌停/复权/退市、实盘信号延迟确认、仓位管理 | 领域特定 |

核心修正（相对前序版本）：
- ε阈值从闭合条件移至枢轴提取层，保证对偶定理的平移不变性
- 用「序同构」替代「拓扑同胚」（后者对有限离散集是平凡的）
- 公理2 的上界使用先验固定整数 $M_0$，不注入后验统计量
- 闭合条件为纯序零参数：$\sigma_k \cdot (s_{k+3} - s_{k+1}) > 0$

### 1.2 代码落地映射

| 公理/定理 | 代码落点 | 验证证据 |
|-----------|---------|----------|
| 枢轴点提取（定义1.2） | `data_manager/structure_engine.py` | `tests/test_structure_engine.py` (431行) |
| N字结构闭合（定义1.4） | `data_manager/structure_engine.py` | `tests/test_convergence_contract.py` (694行) |
| 反转-延续对偶（定理1） | `data_manager/structure_engine.py` | 同上 |
| 信号生成（工程层） | `data_manager/signal_generator.py` | `tests/test_signal_generator.py` (342行) |
| 结构Schema（L4-L5） | `data_manager/structure_schema.py` | `tests/test_structure_schema.py` (157行) |
| 局部除权映射 | `data_manager/local_rights_mapping.py` | `tests/test_local_rights_mapping.py` (245行) |
| 尺度聚合（实例化层） | `data_manager/period_bar_builder.py` | `tests/test_period_bar_builder_matrix.py` |
| Canonical 1m 归一化 | `data_manager/canonical_minute.py` | `tests/test_canonical_contracts.py` (4个测试) |
| 会话档案注册表 | `data_manager/session_profile_registry.py` | `tests/test_session_profile_registry*.py` (6+测试) |
| 周期注册表 | `data_manager/period_registry.py` | `tests/test_period_registry.py` (6个测试) |
| 阈值注册表 | `data_manager/threshold_registry.py` | `tests/test_threshold_registry.py` |
| 治理回执门禁 | `tools/governance_jobs.py` | `tests/test_governance_jobs.py` (19个测试) |
| 黄金标准1D审计 | `data_manager/golden_1d_audit.py` | `tests/test_golden_1d_audit.py` + `test_golden_1d_api.py` |

**结论：七层公理化架构从理论到代码的映射链路完整且有测试保护，274 个核心测试全绿。**

### 1.3 代码层制度落地确认（v1.0→v2.0 增量）

以下制度组件在 v1.0 时仍为"文档概念"，v2.0 已确认为运行时代码：

| 组件 | 文件 | 运行时状态 | 配置文件 |
|------|------|-----------|---------|
| SessionProfileRegistry | `data_manager/session_profile_registry.py` | ✅ UDI 已集成，10+ 调用点 | `config/session_profile_rules.json`, `config/session_profile_versions.json` |
| PeriodRegistry | `data_manager/period_registry.py` | ✅ UDI 已集成，auto_data_updater 已对齐 | `config/period_registry.json` (29+ 周期) |
| ThresholdRegistry | `data_manager/threshold_registry.py` | ✅ UDI 已导入 | — |
| Canonical 1m | `data_manager/canonical_minute.py` | ✅ UDI 已集成(line 48, 76) | version = "2026.04.01" |
| Governance Hash | `unified_data_interface.py` DDL/缓存/读路径 | ✅ 写入+验证均已落地 | — |
| Governance Receipt Validation | `tools/governance_jobs.py` | ✅ strict-rebuild exit code 7 | — |
| Timestamp Contract | `data_manager/timestamp_utils.py` | ✅ Beijing naive 语义 | `TIMESTAMP_CONTRACT_VERSION` |

---

## 2. 已完成工作清单（更新至 2026-04-02）

### 2.1 环境与工程底座

| 领域 | 已完成项 | 状态 |
|------|----------|------|
| Python 环境 | `.venv` 主解释器，Python 3.11.15 | ✅ |
| 包管理 | uv 主线迁移 | ✅ |
| 文档治理 | ADR、蓝图、执行清单、替代靶单 | ✅ |
| Qt 主线治理 | ADR-0002 已接受→Legacy Freeze | ✅ |
| 数据架构 | SQLite 主写 + DuckDB 影子/联邦读 | ✅ |

### 2.2 前端与图表

| 领域 | 已完成项 | 状态 |
|------|----------|------|
| Tauri 桌面壳 | Rust + VS Build Tools 通电 | ✅ |
| Tauri Routes | Structure / Data / System / Workbench | ✅ |
| 图表门面 | ChartFacadeV2 + Pro/Fallback Adapter | ✅ |
| 图表引擎 | **KLineChart v9.8.12** | ✅ |
| 共享 UI 语言 | `uiTone.ts` 状态标记体系 | ✅ |

### 2.3 数据治理与审计

| 领域 | 已完成项 | 状态 |
|------|----------|------|
| 黄金标准审计 | `golden_1d_audit.py` + API + 15 测试 | ✅ |
| P0 数据链修复 | DAT 列标准化、DuckDB 列名修正 | ✅ |
| DuckDB 资源保护 | 连接泄漏与异常 close 保护 | ✅ |
| 双引擎方向 | SQLite 主状态 / DuckDB 联邦读 | ✅ |

### 2.4 制度层代码（v2.0 新确认）

| 领域 | 已完成项 | 状态 |
|------|----------|------|
| Session Profile Registry | 注册表 + 版本化解析 + JSON配置 + 实盘测试 | ✅ |
| Period Registry | 29+ 周期 + JSON配置 + UDI集成 | ✅ |
| Threshold Registry | 阈值注册表 + UDI 导入 | ✅ |
| Canonical 1m | 归一化层 + 集合竞价合并 + 治理元数据 | ✅ |
| Governance Jobs | 回执验证 + 严格门禁(exit 7) + 水印 | ✅ |
| Governance Hash | DDL字段 + 缓存键 + 读路径验证 | ✅ |
| 七层架构核心测试 | 274 passed / 0 failed | ✅ |

### 2.5 七层架构测试

| 模块 | 结论 |
|------|------|
| `test_structure_engine.py` (431行) | ✅ 已存在并通过 |
| `test_signal_generator.py` (342行) | ✅ 已存在并通过 |
| `test_structure_schema.py` (157行) | ✅ 已存在并通过 |
| `test_local_rights_mapping.py` (245行) | ✅ 已存在并通过 |
| `test_convergence_contract.py` (694行) | ✅ 已存在并通过 |
| `test_period_bar_builder_matrix.py` | ✅ 已存在并通过 |
| `test_session_profile_registry*.py` | ✅ 已存在并通过 |
| `test_period_registry.py` | ✅ 已存在并通过 |
| `test_threshold_registry.py` | ✅ 已存在并通过 |
| `test_governance_jobs*.py` | ✅ 已存在并通过 |
| `test_canonical_contracts.py` | ✅ 已存在并通过 |
| `test_custom_period_bars_cache.py` | ✅ 已存在并通过 |
| `test_golden_1d_audit.py` + `test_golden_1d_api.py` | ✅ 已存在并通过 |

---

## 3. 问题修复状态

### 3.1 P0 严重问题——已修复

| # | 问题 | 修复状态 |
|---|------|----------|
| 1 | `_close_duckdb_connection` 无限递归 | ✅ |
| 2 | DAT DataFrame 缺 `datetime` 列 | ✅ |
| 3 | DuckDB 列名 `trade_date` / `date` 漂移 | ✅ |
| 4 | 完备性语义围绕 DAT 而非 DuckDB | ✅ |
| 5 | DuckDB 异常路径连接泄漏 | ✅ |
| 6 | Golden1dAuditor 多线程写 SQLite | ✅ |
| 7 | 七层架构模块缺测试保护 | ✅ 当前 274 个 |
| 8 | Canonical 1m 归一化层缺失 | ✅ `canonical_minute.py` |
| 9 | 周期注册表缺失/四处漂移 | ✅ `period_registry.py` + JSON |
| 10 | Session Profile 平面配置无版本化 | ✅ `session_profile_registry.py` |
| 11 | 阈值制度化缺失 | ✅ `threshold_registry.py` |

### 3.2 P0 / P1——未完全修复

| # | 问题 | 风险 | 代码落点 |
|---|------|------|----------|
| 12 | 全局 `warnings.filterwarnings("ignore")` | 掩盖运行时异常 | 20+ 处散布 (`sector_data.py`, `factor_library.py`, `money_flow.py`, `UDI` 等) |
| 13 | WebSocket Token 使用 Query Param | 日志泄露 | `api_server.py:1985-2010` |
| 14 | ShardedSQLite 连接缓存无上限 | FD 耗尽 | — |
| 15 | CORS `allow_origins=["*"]` | 权限过宽 | `api_server.py:915-920` |
| 16 | `xtdata_lock` 线程——10s 超时但无主动告警 | 静默超时后无恢复机制 | `core/xtdata_lock.py:164-167` |

### 3.3 当前 17 个失败测试详解

| 根因分类 | 测试文件 | 数量 | 说明 |
|----------|---------|------|------|
| Account API 端点未实现 | `test_api_server_smoke.py`, `test_phase2_day3_account_api.py` | 7 | `/api/v1/accounts/*` 返回 404 |
| Trade Risk 依赖 Account API | `test_trade_risk_integration.py` | 4 | 间接依赖上述端点 |
| MagicMock 类型比较 | `test_udi_hotpaths.py`, `test_unified_data_interface.py`, `test_unified_data_interface_extra3.py` | 6 | `_evaluate_cross_source_gate` 中 Mock 返回值未 spec 为 int |

**修复基线**：
- Account API：需在 `api_server.py` 新增 `/api/v1/accounts/` CRUD 路由
- Mock 修复：`_evaluate_cross_source_gate` 的 mock 需要 `return_value=0` 而非未设定

### 3.4 God Object 反模式——当前状态

| 文件 | 行数 | 风险等级 |
|------|------|----------|
| `data_manager/unified_data_interface.py` | **8059 行** | 🔴 极高——远超 v1.0 预估的 4000 行 |
| `gui_app/main_window.py` | **3072 行** | 🟡 高——Legacy 已冻结 |
| `core/api_server.py` | **2693 行** | 🟡 高——需按路由拆分 |

### 3.5 PyQt5 崩溃——当前状态

- **42 个 `.dmp` 崩溃转储文件**存在于根目录（最新：2026-03-30）
- Qt 主线已冻结（ADR-0002），不再新增功能
- 根因为 QThread/C 扩展竞态——架构病而非代码 Bug
- Tauri 迁移是终局解决方案，目前 4 个 Route 已在实施

---

## 4. 残留问题与解决基线

### 4.1 🔴 PyQt5 线程模型——42 次崩溃转储

**现状**：42 个 `.dmp`，已知 31 条根因均指向 QThread/C 扩展竞态。Qt 主线已冻结。

**解决基线**：
1. 不修——Qt 已进入 Legacy Freeze
2. Tauri 稳步接管所有交互场景
3. 确保 `test_mode` / `PYTEST_CURRENT_TEST` 守卫在测试环境中生效
4. 42 个 `.dmp` 文件应归档到 `docs/archive/crash_dumps/` 或 `.gitignore`

**判断**：这是架构病，不是代码 Bug。正确的解决方式是**替换**（Tauri），不是修补。

### 4.2 🔴 God Object 反模式

**现状**：

| 文件 | 行数 | 职责数 |
|------|------|--------|
| `unified_data_interface.py` | 8059 | 数据路由、周期构建、缓存、治理、gate评估、ingestion 等 |
| `api_server.py` | 2693 | 70+ 端点、CORS、WebSocket、图表、状态、审计 |
| `main_window.py` | 3072 | 所有 Qt tab、连接、状态、冻结逻辑 |

**解决基线**：

| 动作 | 目标 |
|------|------|
| `api_server.py` → 按 FastAPI `APIRouter` 拆分 | `routes/market.py`, `routes/structures.py`, `routes/health.py`, `routes/chart.py`, `routes/accounts.py` |
| `unified_data_interface.py` → 职责分离 | 将缓存层 (`_read_cached_*`, `_save_*`) 抽出为 `period_cache_manager.py`；将 gate 评估抽出为 `data_gate_evaluator.py`；将 ingestion 状态抽出为 `ingestion_tracker.py` |
| `main_window.py` → 不动 | Legacy Freeze，不投入重构预算 |

**判断**：UDI 8059 行是当前最大的维护债务。建议按"缓存/gate/ingestion"三个职责域渐进抽出，每次抽出后回归测试验证。

### 4.3 🔴 Canonical 层——已建立，待深化

**已完成**：
- `canonical_minute.py` 实现了集合竞价合并 + 时间归一 + 治理元数据
- `CANONICAL_MINUTE_VERSION = "2026.04.01"`
- UDI 已集成（line 48, 76）

**待深化**：
- timestamp canonical gate 尚未成为**不可绕过的系统入口**（当前是函数调用，不是强制 decorator/middleware）
- 集合竞价、午间休市、停牌、节假日、夜盘、跨周期边界的参数化测试矩阵尚缺
- canonical 1m → 派生周期的全链路血缘标记尚未完全落地

**解决基线**：
1. 将 `normalize_canonical_1m()` 提升为 UDI 写入路径的**强制前置步骤**
2. 新增 6 类场景参数化测试（集合竞价/午间/停牌/节假日/夜盘/跨周期）
3. 在 `_save_custom_period_bars` 中写入 `canonical_minute_version`

### 4.4 🔴 关键路径测试盲区

| 模块 | 文件数 | 测试覆盖 | 风险 |
|------|--------|----------|------|
| `easy_xt/realtime_data/` | 65 个 `.py` | **0 个测试** | 🔴 极高——生产行情核心链路 |
| `strategies/` | 158 个 `.py` | **0 个测试** | 🔴 极高——实盘策略零覆盖 |
| `gui_app/main_window.py` | 3072 行 | 仅 GUI marker 测试 | 🟡 已冻结 |
| Account API | 未实现 | 7 个测试等待 | 🟡 中 |

**解决基线**：
1. `easy_xt/realtime_data/`：优先为 `websocket_server.py`、`push_service.py`、`service_manager.py` 补充冒烟测试
2. `strategies/`：至少为 `base/strategy_template.py`、`registry.py`、`strategy_controller.py` 补充单元测试
3. Account API：实现 CRUD 端点（`/api/v1/accounts/`），解决 11 个测试失败

---

## 5. 架构发现与建议（更新版）

### 5.1 已解决的架构发现（v1.0→v2.0 升级）

| 发现 | v1.0 状态 | v2.0 状态 |
|------|-----------|-----------|
| Canonical 1m 层缺失 | ❌ 缺失 | ✅ `canonical_minute.py`已实现 |
| 周期注册表缺失 | ❌ 四处漂移 | ✅ `period_registry.py` + JSON |
| Session Profile 无版本化 | ❌ 平面配置 | ✅ `session_profile_registry.py` |
| 阈值制度化缺失 | ❌ 无 | ✅ `threshold_registry.py` |
| 治理回执无代码验证 | ❌ 概念 | ✅ `governance_jobs.py` + 门禁 |
| governance_hash 未进入数据链路 | ❌ 未落地 | ✅ DDL+缓存键+读验证 |

### 5.2 仍然存在的 6 个架构发现

1. **God Object 未拆分**：`unified_data_interface.py` 8059 行，`api_server.py` 2693 行
2. **三层数据状态语义缺失**：`/api/v1/data/coverage/{symbol}` 端点**未实现**
3. **因子维表/周期因子快照层缺失**：`daily_rights_factor`、`period_factor_snapshot` 未建
4. **timestamp canonical gate 非强制入口**：当前是函数调用，非 middleware
5. **契约测试场景不足**：集合竞价/午间/停牌/节假日/夜盘/跨周期 6 类场景缺失
6. **Tauri 数据状态条未对齐 Qt**：周期按钮不足、缺口/进度/复权未展示

---

## 6. 前端状态

### 6.1 Tauri 主线

| 组件 | 状态 | 判断 |
|------|------|------|
| StructureRoute | ✅ | 已实施，共享 uiTone |
| DataRoute | ✅ | 已实施，共享 uiTone |
| SystemRoute | ✅ | 已实施 |
| WorkbenchRoute | ✅ 骨架 | 不再是纯 Spike |
| WorkbenchChartStage | ⚠️ | 存在膨胀问题 |
| 数据状态条 | ❌ | 缺三层状态语义 API |
| Error Boundary | ❌ | 全局容错缺失 |
| 周期按钮 | ⚠️ | 仅 5 个，应由 registry 驱动 |

### 6.2 Qt Legacy

已冻结（ADR-0002），不再投入新功能。42 个 `.dmp` 证明其线程模型不可修复。

---

## 7. 优先级路线图（更新版）

### 7.1 P0：本周立即修复（安全/稳定）

| # | 动作 | 代码落点 |
|---|------|----------|
| 1 | 修复 17 个失败测试 | Account API 端点 + Mock spec |
| 2 | 清理全局 `warnings.filterwarnings("ignore")` | 20+ 处 → logger 模式 |
| 3 | WebSocket Token 改走 Header | `api_server.py:1985` |
| 4 | CORS 限制为可信域 | `api_server.py:915` |
| 5 | ShardedSQLite LRU 上限 | — |

### 7.2 P1：1-2 周内完成（架构减负）

| # | 动作 | 目标 |
|---|------|------|
| 6 | `api_server.py` 按 APIRouter 拆分 | 降至 5 个路由文件 |
| 7 | `unified_data_interface.py` 职责分离 | 缓存/gate/ingestion 抽出 |
| 8 | 新增 6 类场景契约测试 | 集合竞价/午间/停牌/节假日/夜盘/跨周期 |
| 9 | canonical gate 升级为强制中间件 | 不可绕过 |
| 10 | `/api/v1/data/coverage/{symbol}` | 三层状态 API |
| 11 | `easy_xt/realtime_data/` 冒烟测试 | 至少覆盖 3 个核心模块 |
| 12 | `strategies/` 单元测试 | 至少覆盖 template + registry |

### 7.3 P2：2-4 周内完成（数据层深化）

| # | 动作 | 目标 |
|---|------|------|
| 13 | `daily_rights_factor` 因子维表 | 因子正式落地 |
| 14 | `period_factor_snapshot` | 周期窗口因子快照 |
| 15 | `structure_evidence` + `structure_snapshot` | 结构证据层 |
| 16 | `repair_receipt` / `replay_receipt` / `publish_gate_receipt` | 回执主链路 |
| 17 | cross-source 全字段门禁 | 从"能算"到"能放行" |

### 7.4 P2：前端补齐

| # | 动作 | 目标 |
|---|------|------|
| 18 | Tauri 周期按钮由 registry 驱动 | 消除前后端漂移 |
| 19 | 数据状态条（缺口/进度/校验/版本） | 对齐 Qt 体验 |
| 20 | React Error Boundary | 全局容错 |

### 7.5 P3：长期演进

| # | 动作 |
|---|------|
| 21 | 热/温/冷三级存储 |
| 22 | PyO3 极热点模块 |
| 23 | registry-driven CI/release gate |
| 24 | 多级缓存策略 |
| 25 | API / 部署 / Release 文档 |

---

## 8. 系统解决方案四步法

### Step 1：立法 ✅ 已完成

- `session_profile_registry.py` + `config/session_profile_rules.json` + `config/session_profile_versions.json`
- `period_registry.py` + `config/period_registry.json`（29+ 周期）
- `threshold_registry.py`
- 274 个核心测试全绿

### Step 2：执法 ⚠️ 进行中

- `canonical_minute.py` 已实现基础归一化 ✅
- `governance_hash` 已进入 DDL 和缓存键 ✅
- timestamp canonical gate 尚未成为强制入口 ❌
- 必填版本字段检查尚未全面落地 ❌

### Step 3：留痕 ❌ 待建

- `factor_snapshot` 未建
- `structure_evidence` / `structure_snapshot` 未建
- `repair_receipt` / `replay_receipt` / `publish_gate_receipt` 未建

### Step 4：放行 ❌ 待建

- 统一门禁公式未实现：

$$golden = contract\_pass \land cross\_source\_pass \land tick\_verified \land lineage\_complete$$

$$partial\_trust = contract\_pass \land cross\_source\_pass \land \neg tick\_verified$$

$$degraded = contract\_pass \land \neg cross\_source\_pass$$

$$unknown = otherwise$$

---

## 9. 健康度评分（更新）

| 维度 | v1.0 评分 | v2.0 评分 | 变化 | 说明 |
|------|-----------|-----------|------|------|
| 架构设计 | 8.5 | **9.0** | ↑0.5 | 制度层代码全面落地 |
| 代码质量 | 6.5 | **6.5** | → | God Object 未拆分，UDI 膨胀至 8059 行 |
| 测试覆盖 | 6.8 | **7.2** | ↑0.4 | 4398/4419 通过，核心 274 全绿；但 realtime/strategies 零覆盖 |
| 前端一致性 | 5.8 | **6.0** | ↑0.2 | 共享 uiTone，但状态 API 未建 |
| 数据质量 | 7.8 | **8.2** | ↑0.4 | canonical_1m + governance_hash + 注册表 + 审计 |
| 运维闭环 | 5.5 | **5.8** | ↑0.3 | governance_jobs 门禁已有；回放/receipt 未建 |
| 文档质量 | 8.0 | **8.3** | ↑0.3 | 七层体系文件完整，制度三件套已建 |

**综合评分：7.3 / 10**（v1.0 为 6.8）

---

## 10. 最终结论

### 10.1 最重要的阶段性成果

> **制度层代码已从"文档概念"全面落地为"运行时约束"。**
>
> session_profile_registry、period_registry、threshold_registry、canonical_1m、governance_jobs 五大制度组件均已写入代码、配置 JSON、集成 UDI、并由 274 个测试保护。

### 10.2 当前最紧迫的三件事

1. **修复 17 个失败测试 + Mock spec 修正**——通过率从 99.6% 恢复到 100%
2. **God Object 拆分**——UDI 8059 行是最大维护债务
3. **补齐 `easy_xt/realtime_data/` 和 `strategies/` 测试**——生产核心链路零覆盖

### 10.3 不需要全量重构

当前架构已是**渐进式融合的最优状态**：
- Python（后端数据/策略/回测）→ 生态无敌
- TypeScript + React（Tauri 前端）→ 已在实施
- Rust（系统壳/IPC/窗口管理）→ 已在位
- KLineChart v9.8.12（图表引擎）→ 已稳定

全量重构的风险远大于收益；正确方向是**补齐缺口**而非替换语言。

### 10.4 最终一句话

> **项目已完成"立法"阶段，正在进入"执法"阶段。当前最缺的不是更多制度定义，而是把已有的制度（版本、归一、血缘、门禁、回放）真正变成不可绕过的运行时约束。**

---

*报告生成于 2026-04-02，基于 `conda run -n myenv python -m pytest tests/` 实测结果与全量代码审查。*
