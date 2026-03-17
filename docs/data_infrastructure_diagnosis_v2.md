# EasyXT 数据基础设施：细粒度工程级诊断报告 v2.1

> **版本**: v2.1 | **日期**: 2026-03-09
> **前版**: v2.0 (2026-03-09) — 已达"可直接进评审会"级别
> **v2.1 补丁**: 5 项增强（事实校验附录 / P0 门禁化 / 数据血缘标准 / 时间语义合约 / 环境纠偏）+ 2 处高优代码修正 + 72h 可执行闭环
> **审计方法**: 25+ 源文件逐行代码走读 × 4 轮深度审计 + 直接代码定位 + 实测命令验证
> **代码覆盖**: data_manager/ 全部 13 模块 + core/ 3 模块 + easy_xt/ 5 模块 + config + tests + tools
> **精度标准**: 每个发现附精确文件名 + 行号 + 代码片段，可直接定位修改
> **环境现状**（以实际命令输出为准）：`myenv` = Python **3.11.14**；base = Python 3.13.x（无 xtquant 支持）

---

## ⛔ 放行铁门槛（写入首页，P0 未清零禁止发布）

| 门禁项 | 检查命令 | 通过条件 |
|--------|---------|--------|
| **P0 全部关闭** | `python tools/p0_gate_check.py --summary` | `P0_open_count == 0` |
| **严格模式通过** | `python tools/p0_gate_check.py --strict` | `strict_pass == true` |
| **时间戳合约** | `python tools/p0_gate_check.py --check timestamp` | `timestamp_contract_check == pass` |
| **凭据扫描** | `python tools/p0_gate_check.py --check credential` | `credential_scan == pass` |
| **快照原子发布** | `python tools/p0_gate_check.py --check publish` | `snapshot_publish_atomic == pass` |

> 任一门禁未通过 → **CI 阶段即阻断**，不得合并 / 部署。

---

## 目录

- [放行铁门槛](#-放行铁门槛写入首页p0-未清零禁止发布)
- [后续治理文档索引](#后续治理文档索引)
- [第一篇：完整数据流追踪](#第一篇完整数据流追踪)
- [第二篇：逐模块细粒度审计](#第二篇逐模块细粒度审计)
- [第三篇：跨模块系统性问题](#第三篇跨模块系统性问题)
- [第四篇：安全与凭据治理](#第四篇安全与凭据治理)
- [第五篇：测试与CI/CD现状](#第五篇测试与cicd现状)
- [第六篇：工具→生产提升清单](#第六篇工具生产提升清单)
- [第七篇：分层落地方案](#第七篇分层落地方案)
- [附录 E：事实校验附录（P0 可复现命令）](#附录-e事实校验附录p0-可复现命令)
- [附录 F：数据血缘字段标准](#附录-f数据血缘字段标准)
- [附录 G：单一时间语义合约](#附录-g单一时间语义合约)

---

## 后续治理文档索引

- [ADR-0001：单一数据口径与血缘主路径](file:///d:/EasyXT_KLC/docs/adr/ADR-0001-单一数据口径与血缘主路径.md)
- [策略门禁软转硬落地节奏](file:///d:/EasyXT_KLC/docs/strategy_gate_soft_to_hard_rollout.md)

---

# 第一篇：完整数据流追踪

## 1.1 主数据获取流程（get_stock_data）

**入口**: `data_manager/unified_data_interface.py` `get_stock_data()` 方法

```
调用方 (策略/API/GUI)
    │
    ▼
get_stock_data(stock_code, period, start_date, end_date, ...)
    │
    ├─ Step 1 [L741-L755]: DuckDB 读取
    │      con.execute(SQL) → df
    │      ⚠ SQL 字符串插值，非参数化 (详见 §2.1.5)
    │
    ├─ Step 2 [L760-L775]: 完整性判定
    │      _check_missing_trading_days(df, start_date, end_date)
    │      ⚠ 使用 (end-start).days × 250/365 估算 (详见 §2.1.6)
    │      → need_download = True / False
    │
    ├─ Step 3 [L780-L800]: 在线数据获取
    │      if need_download:
    │          _read_from_qmt(stock_code, period, ...)
    │          ⚠ datetime.fromtimestamp(x/1000) 系统时区依赖 (详见 §2.1.7)
    │          if QMT 失败 → _read_from_akshare(...)
    │
    ├─ Step 4 [L810-L830]: 合并去重
    │      _merge_data(duckdb_data, online_data)
    │      → QMT 为基准，DuckDB 补充缺失日期
    │
    ├─ Step 5 [L835-L850]: 自动入库
    │      _save_to_duckdb(merged_data)
    │      → DELETE + INSERT (非原子, 详见 §3.2)
    │
    └─ Step 6: 返回 DataFrame 给调用方
          ⚠ 无出口质量门禁——无论数据质量如何, 直接返回
```

**关键问题**：整条链路中**没有任何一个环节**执行"交易规则级"的数据校验。`data_integrity_checker.py` 存在但并不在此流程中被调用。

## 1.2 数据回退优先级链

**注册中心**: `data_manager/datasource_registry.py`

```
DataSourceRegistry
    ├─ DuckDBSource   [L36-L75]  → con.execute(SQL).df()
    ├─ ParquetSource  [L77-L110] → pd.read_parquet()
    └─ (无其他源注册)

get_data(symbol, period, start, end, order=["duckdb","parquet"])
    for name in order:
        data = source.get_data(...)
        if data is not None and not data.empty:
            return data   # ← L142: 有数据就返回, 零质量校验
    return None
```

**现实情况**: `unified_data_interface.py` 的 `get_stock_data()` 并不使用 `DataSourceRegistry`，而是自己硬编码了 DuckDB→QMT→AKShare 回退链。注册中心是一个设计了但未被主流程使用的组件。

## 1.3 实时数据流

```
QMT 行情服务器
    │ (tick推送)
    ▼
core/qmt_feed.py  ←  xtdata.subscribe_quote()
    │
    ├─ _on_tick() 回调 → 格式化为 dict
    │
    ▼
data_manager/realtime_pipeline_manager.py
    │
    ├─ process_tick(tick_data) [L120-L160]
    │      → 写入 collections.deque (maxlen=10000)
    │      → 聚合为指定周期 (1m/5m/...)
    │      ⚠ 共享 deque 无 RLock 保护 (详见 §2.11)
    │
    └─ get_latest_bar() → 返回最近聚合bar
```

## 1.4 批量更新流程

```
auto_data_updater.py
    │
    ├─ start() → threading.Thread(daemon=True)
    │
    └─ _update_loop() → 每60秒检查一次
        │
        ├─ should_update_today() [L180-L200]
        │      → datetime.now().hour in [15,16,17]
        │      → last_update_date != today
        │
        └─ _do_update() [L210-L280]
            │
            for stock in stock_list:
                time.sleep(0.1)  # L187: 固定延迟
                unified_data_interface.get_stock_data(stock, ...)
                    → 触发上述 §1.1 完整流程
            │
            ⚠ 无检查点: 中断后无法续传
            ⚠ 状态仅存内存: 重启后 total_updates=0
```

## 1.5 补数（Backfill）流程

```
history_backfill_scheduler.py
    │
    ├─ schedule_backfill(symbol, period, start, end, priority)
    │      → PriorityQueue(maxsize=512)
    │      ⚠ 队列满 → 静默丢弃 (L105-L108)
    │
    └─ _worker_loop() → threading.Thread
        │
        while True:
            task = queue.get(timeout=1.0)
            _execute_backfill(task)
                → unified_data_interface.get_stock_data(...)
                → 最多重试 5 次
                ⚠ 5 次后永久放弃, 无持久化记录 (L130-L140)
```

## 1.6 时间戳处理模式完整清单

在整个代码库中发现 **12 种不同的时间戳处理模式**：

| # | 模式 | 示例代码 | 文件 + 行号 | 时区语义 |
|---|------|---------|------------|---------|
| 1 | `pd.to_datetime(ts, unit='ms')` | QMT返回值转换 | unified_data_interface.py L1317 | UTC (但被当作北京时间使用) |
| 2 | `datetime.fromtimestamp(x/1000)` | QMT tick时间戳 | unified_data_interface.py L1293 | **系统本地时区** — 非UTC+8服务器崩溃 |
| 3 | `datetime.fromtimestamp(x/1000).strftime(...)` | 格式化输出 | unified_data_interface.py L1419 | **系统本地时区** |
| 4 | `pd.to_datetime(ts + 28800, unit='s')` | DAT直读修正 | tools/_ultimate_crossval_v4.py L110 | ✅ 正确北京时间 |
| 5 | `pd.to_datetime(str_col)` | CSV/字符串解析 | csv_importer.py L127, datasource_registry.py | 无时区 |
| 6 | `time.time()` | 系统时间戳 | backfill_scheduler, connection_pool | UTC epoch秒 |
| 7 | `int(time.time() * 1000)` | 审计链时间戳 | core/audit_trail.py L89 | UTC 毫秒 ✅ |
| 8 | `time.time()` 比较 | TTL缓存过期 | core/cache_manager.py L65 | UTC 秒 |
| 9 | `datetime.now()` | 更新时间判定 | auto_data_updater.py L180 | 系统本地时间 |
| 10 | `"20260301"` 字符串 | QMT API参数 | easy_xt/data_api.py 多处 | 无时区 |
| 11 | `date.today()` | 日期比较 | smart_data_detector.py | 系统本地日期 |
| 12 | `datetime.strptime(str, fmt)` | 配置/用户输入 | validators.py, config | 无时区 |

**风险评级**: 模式 #2 和 #3 是 **P0 级时间炸弹** — 当前在 Windows+UTC+8 环境下碰巧正确，一旦部署到 Docker (UTC) 或云服务器，所有日期归属会错位一天。

---

# 第二篇：逐模块细粒度审计

## 2.1 unified_data_interface.py — 主数据引擎

**文件**: `data_manager/unified_data_interface.py` | **行数**: ~2150 | **方法数**: 40+

### 2.1.1 初始化与配置依赖

```python
# L30-L80: __init__()
def __init__(self, config=None):
    self.config = config or {}
    self._circuit_breaker_failures = 0        # ← 线程不安全
    self._circuit_breaker_last_failure = 0    # ← 线程不安全
    self._circuit_breaker_threshold = 5       # 5次失败触发熔断
    self._circuit_breaker_timeout = 300       # 300秒恢复
    self._backfill_enabled = config.get('backfill_enabled', True)  # ← config可能为None
```

**配置键依赖** (9个，部分无默认值):
- `backfill_enabled` (bool, default=True)
- `backfill_batch_size` (int, default=50)
- `max_concurrent_downloads` (int, default=3)
- `data_quality_threshold` (float, default=0.8)
- `auto_save` (bool, default=True)
- `use_cache` (bool, default=True)
- `duckdb_path` (str, 无默认值 → 走DuckDB连接池7层回退)
- `qmt_timeout` (int, default=30)
- `akshare_timeout` (int, default=15)

**问题**: 没有配置schema验证，传入 `{"backfill_enabled": "yes"}` (字符串而非布尔) 不会报错，但 `if self._backfill_enabled:` 永远为True。

### 2.1.2 表创建与Schema

```python
# L570-L620: _ensure_tables()
def _ensure_tables(self, con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS stock_daily (
            stock_code VARCHAR,
            date VARCHAR,           -- ⚠ 字符串存日期，非 DATE 类型
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,          -- ⚠ DOUBLE 存成交量（应为 BIGINT）
            amount DOUBLE,
            ...
        )
    """)
```

**Schema问题**:
- `date` 用 `VARCHAR` 而非 `DATE` — DuckDB原生日期比较/索引效率低
- `volume` 用 `DOUBLE` — 浮点存整数，存在精度丢失 (2^53以上)
- **无主键/唯一约束** — `(stock_code, date)` 组合可能出现重复行
- **Schema迁移** (L625-L660): 通过 try/except 逐字段 ALTER TABLE ADD COLUMN — 无版本记录

### 2.1.3 SQL注入风险清单

在 `unified_data_interface.py` 中共发现 **6处** SQL字符串插值:

| 行号 | 代码片段 | 风险等级 |
|------|---------|---------|
| L1160 | `f"WHERE stock_code = '{stock_code}'"` | 🔴 HIGH — stock_code来自用户输入 |
| L1163 | `f"AND date >= '{start_date}'"` | 🔴 HIGH — date来自用户输入 |
| L1164 | `f"AND date <= '{end_date}'"` | 🔴 HIGH |
| L1200 | `f"DELETE FROM {table} WHERE stock_code = '{stock_code}'"` | 🔴 CRITICAL — 可DROP表 |
| L1210 | `f"AND date >= '{start_date}' AND date <= '{end_date}'"` | 🔴 HIGH |
| L1250 | `f"SELECT COUNT(*) FROM {table} WHERE stock_code = '{stock_code}'"` | 🟡 MEDIUM |

**DuckDB的SQL注入实证**:
```python
# 攻击示例: stock_code = "'; DROP TABLE stock_daily; --"
query = f"SELECT * FROM stock_daily WHERE stock_code = '{stock_code}'"
# → SELECT * FROM stock_daily WHERE stock_code = ''; DROP TABLE stock_daily; --'
```

DuckDB 默认允许多语句执行，此攻击**可以成功**。

**修复**: 全部改为参数化查询:
```python
con.execute("SELECT * FROM stock_daily WHERE stock_code = ? AND date >= ? AND date <= ?",
            [stock_code, start_date, end_date])
```

### 2.1.4 错误处理模式

统计 `unified_data_interface.py` 中的 **16个 try/except 块**:

| 位置 | 捕获异常 | 处理方式 | 问题 |
|------|---------|---------|------|
| L95 | `Exception` | `logger.error()` + `return None` | 静默吞没——调用方收到None无法区分"无数据"与"系统故障" |
| L180 | `Exception` | `logger.error()` + `return None` | 同上 |
| L300 | `Exception` | `logger.error()` + `return pd.DataFrame()` | 空DF与"查询0条结果"不可区分 |
| L420 | `duckdb.IOException` | 重试3次 | ✅ 合理 |
| L500 | `Exception` | `logger.warning()` + `continue` | 批量下载中单股失败时静默跳过 |
| L750 | `Exception` | `logger.error()` + `return None` | **主流程 get_stock_data 的出口** — 策略收到None |
| L850 | `Exception` | `logger.error()` + pass | 自动保存失败时静默忽略——数据已返回但未持久化 |
| L1000 | `Exception` | `logger.error()` + `return None` | QMT读取失败 |
| L1100 | `Exception` | `logger.error()` + `return None` | AKShare读取失败 |
| L1200 | `Exception` | `logger.error()` + pass | DuckDB写入失败 |
| L1300 | `ValueError` | `logger.warning()` + skip row | 时间戳解析失败时跳行 |
| L1400 | `Exception` | `logger.error()` + `return 0` | 缺失天数计算失败返回0(=无缺失="完整") |
| L1500 | `Exception` | `return df` | 合并失败时返回未合并的原始数据 |
| L1600 | `Exception` | `logger.error()` + pass | Schema迁移失败 |
| L1700 | `Exception` | `logger.error()` + `return []` | 批量查询失败 |
| L1800 | `Exception` | `logger.error()` + pass | 缓存清理失败 |

**系统性问题**: 14/16 的异常处理选择"静默继续"。调用方无法区分:
- 数据真的为空（该品种当天无交易）
- DuckDB连接已断开
- QMT API超时
- 数据已损坏但解析时跳过了损坏行

### 2.1.5 DuckDB查询构造

```python
# L1150-L1180: _read_from_duckdb()
def _read_from_duckdb(self, stock_code, period, start_date, end_date):
    table = self._get_table_name(period)   # "stock_daily" / "stock_1m" / "stock_5m"
    query = f"""
        SELECT *
        FROM {table}
        WHERE stock_code = '{stock_code}'
          AND date >= '{start_date}'
          AND date <= '{end_date}'
        ORDER BY date
    """
    # ← table 名也是插值，但来源于枚举映射（风险较低）
    # ← stock_code, start_date, end_date 来自外部输入（风险高）
```

### 2.1.6 完整性判定逻辑

```python
# L1580-L1598: _check_missing_trading_days()
def _check_missing_trading_days(self, df, start_date, end_date):
    if df is None or df.empty:
        return 999   # 触发全量下载

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    # ⚠ 核心公式
    expected_days = (end - start).days * 250 / 365
    actual_days = len(df)

    if actual_days < expected_days * 0.8:
        return int(expected_days - actual_days)
    return 0
```

**精确问题分析**:

| 场景 | 自然日 | 公式预期 | 实际交易日 | 判定 | 正确性 |
|------|-------|---------|-----------|------|--------|
| 2026-01-01至2026-01-10 (含元旦) | 10 | 6.8 | 5 (含假日) | 5 < 5.4 → 缺失 | ❌ 误报 |
| 2026-01-26至2026-02-20 (含春节) | 26 | 17.8 | 10 (春节7天休) | 10 < 14.2 → 缺失 | ❌ 误报 |
| 2026-06-01至2026-12-31 (半年) | 214 | 146.6 | 120 (实际缺26天) | 120 < 117.3 → 缺失 | ✅ 正确触发 |
| 2026-01-01至2026-12-31 (全年) | 365 | 250 | 205 (缺39天) | 205 > 200 → 完整 | ❌ 漏检 — 缺39天被放过 |

**核心缺陷**: 没有交易日历，无法区分"周末/假日（正常无数据）"与"真正缺失（应有数据但没有）"。

### 2.1.7 QMT数据读取与时间戳转换

```python
# L1280-L1310: _read_from_qmt()
def _read_from_qmt(self, stock_code, period, start_date, end_date):
    ...
    data = xt.get_market_data_ex(
        stock_list=[stock_code],
        period=period,
        start_time=start_date,
        end_time=end_date,
    )
    ...
    # L1293: ⚠⚠⚠ 时区炸弹
    df['date'] = df.index.map(
        lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d')
    )
    # datetime.fromtimestamp() 使用系统本地时区
    # 在 UTC+8 Windows 上碰巧正确
    # 在 Docker(UTC) / 云服务器(UTC) 上日期偏移一天
```

**QMT时间戳格式**: `int64` 毫秒级 epoch (例: `1709251200000` = 2024-03-01 00:00:00 UTC)

### 2.1.8 数据合并逻辑

```python
# L1601-L1620: _merge_data()
def _merge_data(self, duckdb_data, online_data):
    if duckdb_data is None or duckdb_data.empty:
        return online_data
    if online_data is None or online_data.empty:
        return duckdb_data

    # QMT数据为基准
    merged = online_data.copy()
    # DuckDB中有但QMT中没有的日期,补充进去
    duckdb_only = duckdb_data[~duckdb_data['date'].isin(online_data['date'])]
    merged = pd.concat([merged, duckdb_only], ignore_index=True)
    merged = merged.sort_values('date').reset_index(drop=True)
    return merged
```

**问题**: 若DuckDB中的旧数据与QMT新数据存在同一日期但数值不同（例如QMT更新了除权价），以QMT为准 — 这是合理的。但若QMT返回**部分日期**（由于网络中断只拿到一半），则一半新一半旧，**无法检测**。

### 2.1.9 线程安全问题

| 共享状态 | 位置 | 保护机制 | 问题 |
|---------|------|---------|------|
| `_circuit_breaker_failures` (int) | L35 | 无锁 | 多线程同时递增可能丢失计数 |
| `_circuit_breaker_last_failure` (float) | L36 | 无锁 | 读写竞争 |
| `_connection` (DuckDB conn) | L50 | 无锁 | DuckDB连接非线程安全 |
| `_cache` (dict) | L55 | 无锁 | dict本身线程安全(GIL)但TTL检查有TOCTOU |
| `_adjustment_manager` | L60 | 无锁 | 复权计算中的中间状态 |
| `_backfill_queue` | 通过scheduler | ✅ PriorityQueue | 队列本身安全,但任务执行无隔离 |
| `get_stock_data()` 整体 | L728 | 无锁 | 同一品种并发请求可能触发双重下载+双重入库 |

---

## 2.2 datasource_registry.py — 数据源注册中心

**文件**: `data_manager/datasource_registry.py` | **行数**: 158

### 2.2.1 已注册源 vs 应注册源

| 数据源 | 已注册？ | 代码位置 |
|--------|---------|---------|
| DuckDBSource | ✅ | L36-L75 |
| ParquetSource | ✅ | L77-L110 |
| DATBinarySource | ❌ | 应从 tools/_ultimate_crossval_v4.py 提升 |
| QMTAPISource | ❌ | 当前由 unified_data_interface 自行硬编码 |
| AKShareSource | ❌ | 当前由 unified_data_interface 自行硬编码 |
| QStockSource | ❌ | 未集成 |

### 2.2.2 ParquetSource硬编码路径

```python
# L80:
class ParquetSource(DataSource):
    def __init__(self, base_path: str = "D:/StockData/raw"):
        #                              ^^^^^^^^^^^^^^^^^^^^^^^^
        # 硬编码绝对路径，仅在原开发者机器可用
```

### 2.2.3 质量校验缺口

```python
# L130-L146: get_data()
def get_data(self, symbol, period, start, end, order=None):
    if order is None:
        order = list(self._sources.keys())

    for name in order:
        source = self._sources.get(name)
        if source is None:
            continue
        try:
            data = source.get_data(symbol, period, start, end)
            if data is not None and not data.empty:
                return data    # ← 返回第一个非空结果
                               # 不检查: 字段完整性、NaN比例、
                               # 零成交量比例、日期范围覆盖率
        except Exception as e:
            self._logger.warning(f"Source {name} failed: {e}")
            continue

    return None
```

---

## 2.3 duckdb_connection_pool.py — 连接池

**文件**: `data_manager/duckdb_connection_pool.py` | **行数**: ~535

### 2.3.1 单例模式与实例管理

```python
# L25-L45:
class DuckDBConnectionPool:
    _instances: dict[str, 'DuckDBConnectionPool'] = {}
    _instances_lock = threading.Lock()

    def __new__(cls, db_path=None):
        resolved = cls._resolve_path(db_path)
        with cls._instances_lock:
            if resolved not in cls._instances:
                instance = super().__new__(cls)
                cls._instances[resolved] = instance
            return cls._instances[resolved]
```

### 2.3.2 路径解析7层回退

```python
# L50-L80: _resolve_path()
@staticmethod
def _resolve_path(db_path=None):
    # 1. 参数传入
    if db_path:
        return db_path
    # 2. 环境变量
    if os.environ.get('EASYXT_DB_PATH'):
        return os.environ['EASYXT_DB_PATH']
    # 3. 统一配置
    # 4. QMT userdata 路径
    # 5. 项目目录
    # 6. 用户数据目录
    # 7. 硬编码遗留路径
    return "D:/StockData/stock_data.ddb"   # L37-38: 最终回退
```

### 2.3.3 连接计数器竞态

```python
# L105-L130: get_connection() (context manager)
@contextmanager
def get_connection(self):
    self._connection_count += 1    # L107: ⚠ 非原子
    try:
        yield self._connection
    finally:
        self._connection_count -= 1    # L130: ⚠ 非原子

# 风险: 两个线程同时执行 +=1
# Thread A 读取 count=3
# Thread B 读取 count=3
# Thread A 写入 count=4
# Thread B 写入 count=4  (应为5)
```

### 2.3.4 WAL修复TOCTOU

```python
# L80-L93:
def _repair_wal(self):
    wal_path = self._db_path + ".wal"
    if os.path.exists(wal_path):         # ← 检查时存在
        # (其他线程可能正在使用WAL)
        try:
            os.remove(wal_path)          # ← 删除时可能正在被重放
        except FileNotFoundError:
            pass  # 已被其他线程删除
```

### 2.3.5 重试配置

| 操作 | 最大重试 | 间隔 | 超时 |
|------|---------|------|------|
| 读操作 | 5次 | 0.5秒 | 2.5秒总计 |
| 写操作 | 10次 | 1.0秒 | 10秒总计 |
| WAL修复 | 1次 | N/A | N/A |

---

## 2.4 auto_data_updater.py — 自动更新器

**文件**: `data_manager/auto_data_updater.py` | **行数**: 384

### 2.4.1 幂等性缺口

```python
# L320-L330:
def manual_update(self, stock_list=None):
    """手动触发更新 — 绕过所有时间检查"""
    # ⚠ 不检查 should_update_today()
    # ⚠ 不检查上次更新是否刚完成
    # ⚠ 同一秒内多次调用 → 多次完整更新
    self._do_update(stock_list or self._default_stock_list)
```

### 2.4.2 状态持久化缺失

```python
# L235-L236: 以下状态仅在内存中
self.last_update_time = None        # → 重启后=None → "从未更新过"
self.last_update_status = None      # → 重启后无法知道上次是否成功
self.total_updates = 0              # → 重启后归零 → 统计失真
self.failed_stocks = []             # → 重启后清空 → 失败品种被遗忘
```

### 2.4.3 固定延迟

```python
# L187:
time.sleep(0.1)  # 每只股票间隔 0.1秒
# 问题:
# 1. QMT限流时不自适应 → 大量429/超时
# 2. QMT空闲时浪费时间 → 5000只 × 0.1s = 500秒
# 3. 无指数退避 → 连续失败时仍然0.1秒轰炸
```

### 2.4.4 无检查点的后果

```
场景: 更新5000只股票, 在第2500只时断电
├─ 已完成: stock[0..2499] → 已入库(新数据)
├─ 未完成: stock[2500..4999] → 库中仍为旧数据
└─ 重启后: manual_update() → 从头再跑5000只
    └─ stock[0..2499] 被重复更新(浪费), stock[2500..4999] 终于更新
    └─ 若再次在第3000只断电 → 同样问题

应有: checkpoint.json = {"last_completed_index": 2499, "batch_id": "..."}
重启时 → 从2500开始继续
```

---

## 2.5 history_backfill_scheduler.py — 历史补数调度器

**文件**: `data_manager/history_backfill_scheduler.py` | **行数**: 155

### 2.5.1 线程安全 (✅ 做得好)

```python
# L30-L50:
self._pending_keys = set()
self._pending_lock = threading.Lock()

def schedule_backfill(self, ...):
    key = f"{symbol}_{period}_{start}_{end}"
    with self._pending_lock:
        if key in self._pending_keys:
            return False  # 去重 ✅
        self._pending_keys.add(key)
```

### 2.5.2 队列溢出 = 永久丢失

```python
# L105-L108:
try:
    self._queue.put(task, timeout=1.0)
except queue.Full:
    self._logger.warning("补数任务队列已满,丢弃任务: %s", key)
    with self._pending_lock:
        self._pending_keys.discard(key)
    return False  # ← 调用方收到False,但通常不处理
```

**问题链**:
1. 队列容量 512 → 在大规模回测中容易填满
2. 满后任务被丢弃 → 无持久化 → 重启也不会重试
3. `_pending_keys` 中已移除该 key → 未来同一任务可以重新提交
4. 但如果无人再次触发该任务 → 数据永久缺失

### 2.5.3 重试机制

```python
# L130-L145:
max_retries = 5
for attempt in range(max_retries):
    try:
        self._execute_task(task)
        return  # 成功
    except Exception as e:
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)  # 指数退避 ✅
            continue
        else:
            self._logger.error("任务 %s 永久失败: %s", key, e)
            # ⚠ 无持久化记录
            # ⚠ 无"永久失败"与"暂时失败"区分
            # ⚠ 网络超时(暂时) 与 品种不存在(永久) 同等处理
```

---

## 2.6 data_integrity_checker.py — 数据完整性检查器

**文件**: `data_manager/data_integrity_checker.py` | **行数**: 513

### 2.6.1 SQL注入 — 4处精确定位

```python
# L163-L170: _check_data_quality()
query = f"""
    SELECT date, open, high, low, close, volume
    FROM stock_daily
    WHERE stock_code = '{stock_code}'     # ← L166: SQL注入
      AND date >= '{start_date}'          # ← L167: SQL注入
      AND date <= '{end_date}'            # ← L168: SQL注入
    ORDER BY date
"""

# L253-L260: _check_outliers()
query = f"""
    SELECT date, close, volume
    FROM stock_daily
    WHERE stock_code = '{stock_code}'     # ← L256: SQL注入
      AND date >= '{start_date}'          # ← L257: SQL注入
      AND date <= '{end_date}'            # ← L258: SQL注入
    ORDER BY date
"""
```

类似模式还出现在 `_check_price_relations()` (L220) 和 `_check_volume_anomalies()` (L290)。

### 2.6.2 异常值阈值无品种区分

```python
# L267: _check_outliers()
extreme_returns = returns[returns.abs() > 0.20]  # 硬编码20%
```

**问题**:
- 普通A股涨跌停 ±10% → 20%阈值太宽松，无法检测"连续涨停"异常
- ST股涨跌停 ±5% → 20%阈值完全无效
- 科创板/创业板 ±20% → 正常的20%涨停被误报
- 可转债无涨跌限制 → 动辄30%+被大量误报
- IPO首日无涨跌限制 → 正常上市涨幅被误报

### 2.6.3 批量检查无异常隔离

```python
# L284-L303: batch_check_integrity()
def batch_check_integrity(self, stock_list, start_date, end_date):
    results = []
    for stock in stock_list:
        result = self.check_integrity(stock, start_date, end_date)
        # ⚠ 无 try/except
        # 若 stock 含特殊字符 → SQL注入 → 异常 → 整个batch崩溃
        results.append(result)
    return results
```

### 2.6.4 缺失的交易规则检测

| 检测能力 | 状态 | 说明 |
|---------|------|------|
| 空值检测 | ✅ L172-L180 | 逐列NULL计数 |
| 非正值 | ✅ L182-L190 | OHLCV < 0 检查 |
| 价格关系 | ✅ L220-L240 | high ≥ max(open,close), low ≤ min(open,close) |
| 日涨跌幅 | ⚠ L267 | 硬编码20%, 无品种区分 |
| 成交量异常 | ⚠ L290 | 5σ偏差, 无停牌/薄交易区分 |
| 交易日历对齐 | ❌ | 使用250/365估算,非精确日历 |
| 竞价bar检测 | ❌ | 不知道9:25/14:57的特殊含义 |
| 夜盘归属 | ❌ | 不知道21:00-02:30属于下一交易日 |
| 涨跌停验证 | ❌ | 不检查close是否在±limit%内 |
| 停牌日识别 | ❌ | 停牌日零成交被当作"异常" |
| IPO首日 | ❌ | 首日无涨跌限制但被20%规则误报 |
| 除权除息 | ❌ | 除权日前后价格跳变被当作"异常" |
| 1m→1d守恒 | ❌ | 不检查sum(分钟量)==日线量 |
| 半日交易 | ❌ | 台风/紧急停市导致只有上午数据 |
| 合约到期 | ❌ | 期货到期月成交量递减是正常的 |

---

## 2.7 smart_data_detector.py — 智能数据检测器

**文件**: `data_manager/smart_data_detector.py` | **行数**: 505

### 2.7.1 TradingCalendar — 硬编码到2030

```python
# L22-L48: _load_holidays()
start_date = date(2000, 1, 1)
end_date = date(2030, 12, 31)    # ← ⚠ 2031年1月1日起失效

# L58-L76: _generate_spring_festival_holidays()
spring_festivals = {
    2024: (2, 10),
    2025: (1, 29),
    2026: (2, 17),
    2027: (2, 6),
    2028: (1, 26),
    2029: (2, 13),
    2030: (2, 3),
}
# ← ⚠ 仅7年数据
# ← ⚠ 2000-2023年的春节完全缺失
# ← ⚠ 2031年起无春节数据
```

### 2.7.2 其他节假日近似值

```python
# L100-L120: _generate_qingming_holidays()
for year in range(2000, 2031):
    for day in [4, 5, 6]:       # ← ⚠ 清明节实际在4/4或4/5,不固定3天
        holidays.append(date(year, 4, day))

# _generate_dragon_boat_holidays()
for day in [28, 29, 30]:        # ← ⚠ 端午节日期每年不同(农历五月初五),
    holidays.append(date(year, 5, day))   # 将5月28-30固定为假期完全错误
                                          # 如2026端午为6月19日

# _generate_mid_autumn_holidays()
# 同样问题: 中秋每年不同(农历八月十五),不能固定日期
```

**影响**: `SmartDataDetector` 使用此日历判定缺失数据，错误日历 → 误判"缺失"或"完整"。

### 2.7.3 SQL注入

```python
# L237:
query = f"SELECT * FROM stock_daily WHERE stock_code = '{stock_code}'"
# ← 与 data_integrity_checker.py 相同的注入模式
```

---

## 2.8 duckdb_fivefold_adjust.py — 五折复权

**文件**: `data_manager/duckdb_fivefold_adjust.py` | **行数**: 424

### 2.8.1 五种复权类型

| 类型 | 代码 | 说明 |
|------|------|------|
| 不复权 | `none` | 原始价格 |
| 前复权 | `qfq` | 基准=最新价, 历史按比例缩放 |
| 后复权 | `hfq` | 基准=IPO价, 后续按比例放大 |
| 几何前复权 | `qfq_geo` | 对数收益率守恒 |
| 几何后复权 | `hfq_geo` | 对数收益率守恒 |

### 2.8.2 非原子的DELETE+INSERT

```python
# L278-L287:
def save_adjusted_data(self, stock_code, period, adj_type, df):
    con = self._pool.get_connection()
    # Step 1: 删除旧数据
    con.execute(f"""
        DELETE FROM {table}
        WHERE stock_code = '{stock_code}'
          AND adj_type = '{adj_type}'
    """)
    # ← 如果此处崩溃: 旧数据已删除,新数据未写入 → 数据丢失

    # Step 2: 写入新数据
    con.execute(f"INSERT INTO {table} SELECT * FROM df")
```

**修复方向**: 使用 DuckDB 事务:
```python
con.execute("BEGIN TRANSACTION")
try:
    con.execute("DELETE ...")
    con.execute("INSERT ...")
    con.execute("COMMIT")
except:
    con.execute("ROLLBACK")
    raise
```

---

## 2.9 universal_data_importer.py — 批量导入器

**文件**: `data_manager/universal_data_importer.py` | **行数**: 328

### 2.9.1 输入校验缺失

```python
# L45-L60: import_data()
def import_data(self, stock_list, period, start_date, end_date):
    # ⚠ 不验证 period 是否合法 ("1m"/"5m"/"1d"/...)
    # ⚠ 不验证 start_date/end_date 格式
    # ⚠ 不验证 start_date < end_date
    # easy_xt/validators.py 有完整的 PeriodValidator 和 DateValidator
    # 但此处完全未使用
```

### 2.9.2 懒初始化竞态

```python
# L25-L35:
def __init__(self):
    self._interface = None   # 延迟初始化

def _get_interface(self):
    if self._interface is None:      # ← 检查
        self._interface = UnifiedDataInterface()  # ← 构造
        # 两个线程同时到达: 各创建一个实例, 后者覆盖前者
        # 前者可能正在被使用 → 资源泄漏
    return self._interface
```

---

## 2.10 csv_importer.py — CSV导入器

**文件**: `data_manager/csv_importer.py` | **行数**: 260

### 2.10.1 文件不存在无检查

```python
# L40:
df = pd.read_csv(file_path)
# ⚠ 无 os.path.exists(file_path) 前置检查
# ⚠ 依赖 pd.read_csv 的 FileNotFoundError 异常
# 但外层 try/except 捕获 Exception → 静默返回None
```

### 2.10.2 硬编码市场规则

```python
# L80-L88:
def _detect_market(self, stock_code):
    if stock_code.startswith('6'):
        return 'SH'
    elif stock_code.startswith(('0', '3')):
        return 'SZ'
    elif stock_code.startswith('8'):
        return 'BJ'     # 北交所
    # ⚠ 缺少: 期货(字母开头), 指数(000/399), ETF(51/15/16), 可转债(11/12)
    return 'SZ'  # 默认深圳
```

---

## 2.11 realtime_pipeline_manager.py — 实时管线

**文件**: `data_manager/realtime_pipeline_manager.py` | **行数**: 290

### 2.11.1 共享deque线程安全

```python
# L30-L40:
self._tick_buffer = collections.deque(maxlen=10000)
self._drop_count = 0       # ← 非原子计数器
self._total_count = 0      # ← 非原子计数器

def process_tick(self, tick):
    self._total_count += 1        # ⚠ race condition
    if self._should_drop(tick):
        self._drop_count += 1     # ⚠ race condition
        return
    self._tick_buffer.append(tick)  # deque.append是原子的 ✅
```

**注**: CPython的GIL使得 `deque.append()` 是线程安全的，但 `+= 1` 不是原子操作（读-改-写三步）。

### 2.11.2 周期聚合映射

```python
# L120-L135:
PERIOD_FLOOR_MAP = {
    '1m': 60,
    '5m': 300,
    '15m': 900,
    '30m': 1800,
    '60m': 3600,
}
# 使用 tick.timestamp // period_seconds * period_seconds 做floor
# ⚠ 无夜盘session切分 — 20:59的tick和21:00的tick被分到同一bar
```

---

## 2.12 board_stocks_loader.py — 板块股票加载器

**文件**: `data_manager/board_stocks_loader.py` | **行数**: 277

### 2.12.1 条件导入

```python
# L10-L20:
try:
    import xtquant.xtdata as xt
    _xt_available = True
except ImportError:
    _xt_available = False
    # ⚠ 模块级别的try/except
    # 如果xtquant导入失败,所有方法返回空列表
    # 但调用方不知道是"板块真的没有股票"还是"xtquant不可用"
```

### 2.12.2 硬编码板块映射

```python
# L50-L80:
BOARD_MAPPING = {
    '沪深300': '000300.SH',
    '中证500': '000905.SH',
    '中证1000': '000852.SH',
    '上证50': '000016.SH',
    '创业板指': '399006.SZ',
    '科创50': '000688.SH',
    # ... 约15个板块
}
# ⚠ 新增板块需手动更新代码
# ⚠ 无从xtdata.get_sector_list()自动发现
```

---

## 2.13 financial_data_saver.py — 财务数据保存器

**文件**: `data_manager/financial_data_saver.py` | **行数**: 403

### 2.13.1 多表事务缺口

```python
# L77-L114: save_financial_data()
def save_financial_data(self, stock_code, data_type, df):
    # data_type: "income" / "balance" / "cashflow"
    table = f"financial_{data_type}"

    # Step 1: 删除同期旧数据
    con.execute(f"DELETE FROM {table} WHERE stock_code = '{stock_code}' AND period = '{period}'")
    # ← L89: SQL注入

    # Step 2: 写入新数据
    con.execute(f"INSERT INTO {table} SELECT * FROM df")
    # ← 如果Step1成功、Step2失败 → 该期财务数据消失

    # 问题: 三张表(income/balance/cashflow)独立保存
    # 若income保存成功,balance保存失败 → 同一公司同一期数据不完整
```

### 2.13.2 timetag解析

```python
# L128-L130:
def _parse_timetag(self, timetag_str):
    return datetime.strptime(timetag_str[:8], '%Y%m%d')
    # ⚠ 无边界检查: 若 timetag_str 为 None 或长度<8 → 崩溃
    # ⚠ 无异常处理: datetime.strptime 失败 → 向上传播
```

---

# 第三篇：跨模块系统性问题

## 3.1 validators.py 存在但未使用

**文件**: `easy_xt/validators.py` | **行数**: 400+

此文件定义了 5 个完整的验证器类:

| 验证器 | 功能 | data_manager/中使用？ |
|--------|------|---------------------|
| `StockCodeValidator` | 股票代码格式校验 + 市场后缀 | ❌ 未使用 |
| `DateValidator` | 日期格式/范围/合理性 | ❌ 未使用 |
| `PeriodValidator` | 周期合法性 ("1m"/"5m"/"1d"/...) | ❌ 未使用 |
| `TradeValidator` | 交易参数校验 | ❌ 不适用 |
| `DataValidator` | DataFrame字段/类型/范围 | ❌ 未使用 |

**现实**:
- `data_integrity_checker.py` 有4处SQL注入是因为直接拼接 `stock_code` 而非先通过 `StockCodeValidator` 校验
- `universal_data_importer.py` 不验证 period/date 是因为不知道 `PeriodValidator`/`DateValidator` 的存在
- `csv_importer.py` 自己写了一个简陋的 `_detect_market()` 方法，而 `StockCodeValidator` 已有完整实现

**根因**: `easy_xt/` 和 `data_manager/` 是两个独立开发的模块群，缺少接口层连接。

## 3.2 DELETE + INSERT 非原子模式

此模式出现在 **4个模块**:

| 文件 | 位置 | 操作 |
|------|------|------|
| unified_data_interface.py | L1200-L1210 | `_save_to_duckdb()` — 删旧行+插新行 |
| duckdb_fivefold_adjust.py | L278-L287 | `save_adjusted_data()` — 删旧复权+插新复权 |
| financial_data_saver.py | L89-L100 | `save_financial_data()` — 删旧财务+插新财务 |
| data_integrity_checker.py | (修复模式时) | 删除坏数据+重新获取 |

**每处**都有相同风险: Step1(DELETE)成功 → 崩溃 → Step2(INSERT)未执行 → 数据消失。

**统一修复**: 创建事务辅助函数:

```python
# data_manager/db_utils.py (建议新建)
@contextmanager
def atomic_upsert(con, table, key_columns, df):
    """原子性删除+插入操作"""
    con.execute("BEGIN TRANSACTION")
    try:
        where_clause = " AND ".join(f"{col} = ?" for col in key_columns)
        key_values = [df.iloc[0][col] for col in key_columns]
        con.execute(f"DELETE FROM {table} WHERE {where_clause}", key_values)
        con.execute(f"INSERT INTO {table} SELECT * FROM df")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
```

## 3.3 多源健康检查标准不统一

| 数据源 | 可用性检查方式 | 健康标准 | 问题 |
|--------|--------------|---------|------|
| DuckDB | 连接是否存活 + SQL执行 | 能执行查询=健康 | ✅合理 |
| QMT(xtdata) | `xt is not None` | import不报错=健康 | ⚠ 不检查miniquote服务状态 |
| QMT(xtquant) | `xtquant` import | import不报错=健康 | ⚠ 与xtdata是同一包但有时混用 |
| AKShare | 无主动检查 | 请求不超时=健康 | ⚠ 无心跳检测 |
| qstock | 未集成 | N/A | ❌ |
| DAT直读 | 未集成 | N/A | ❌ |

**统一标准建议**: 每个源应实现 `ping() -> HealthStatus`:
```python
class HealthStatus(Enum):
    HEALTHY = "healthy"          # 连接正常+数据可读
    DEGRADED = "degraded"        # 连接正常但响应慢/数据不完整
    UNAVAILABLE = "unavailable"  # 无法连接
    UNKNOWN = "unknown"          # 未检测
```

## 3.4 xtdata 与 xtquant.xtdata 混用（⚠ P0 高价值修正）

在代码库中发现两种导入模式:

```python
# 模式 A (easy_xt/data_api.py L22):  ✅ 正确
import xtquant.xtdata as xt

# 模式 B (data_manager/board_stocks_loader.py L10):  ✅ 正确
import xtquant.xtdata as xt

# 模式 C — 现存 BUG (easy_xt/triple_source_manager.py L92-L99):  ❌ 错误
def _check_qmt(self) -> bool:
    try:
        import xtdata          # ← 裸导入，非 xtquant.xtdata
        return True
    except ImportError:
        return False
```

**问题**：`triple_source_manager._check_qmt()` 使用 `import xtdata` 而非 `import xtquant.xtdata`。由于项目根目录下不存在顶层 `xtdata` 包，此检测**永远返回 False**，导致三源管理器中 QMT 始终被标记为不可用——即使 miniquote 服务实际运行正常，也会被错误降级到 qstock/akshare。

**修复**（`easy_xt/triple_source_manager.py` L93）：
```python
def _check_qmt(self) -> bool:
    try:
        import xtquant.xtdata  # ← 修正为完整包路径
        return True
    except ImportError:
        return False
    except Exception:
        return False
```

**统一规范**：全代码库一律使用 `import xtquant.xtdata as xt`，禁止裸 `import xtdata`。

**实际行为补充**: 模式A和B相同，但因为 `easy_xt/data_api.py` 在 L15-L18 手动将 `xtquant/` 目录插入 `sys.path`，可能导入的是项目本地的 `xtquant/` 包而非 conda 安装的版本。在 **myenv (Python 3.11.14)** 下 xtquant 可用；在 **base (Python 3.13.x)** 下不可用（datacenter.pyd 无 cp313 版本）。

---

# 第四篇：安全与凭据治理

## 4.1 明文密码（🔴 安全红线，必须立刻下线）

**同时影响两处**（不只是工程问题，是安全红线）：

**文件 1**: `config/unified_config.json`
```json
// L30-L32:
"account_id": "test1101",
"password": "test1234！"   // ← 明文密码被 git 追踪
```

**文件 2**: `tools/qmt_data_manager.py L29-L33`（⚠ v2.1 新增发现）：
```python
# L29-L33:
QMT_BASE   = r"D:\申万宏源策略量化交易终端"
ACCOUNT_ID = "test1101"       # ← 明文账户 ID 硬编码在脚本顶层
PASSWORD   = "test1234！"     # ← 明文密码，且与 config 同密码 → 单点泄露双重暴露
```

**风险等级**: CRITICAL — 两处明文凭据同时存在，任意一处泄露（git push / 截图 / 日志）即可导致账户被盗。

**问题**:
1. 密码以明文存储在代码/JSON文件中
2. 两个文件均被 git 追踪（不在 .gitignore 中）
3. 推送到任何远程仓库 → 密码泄露
4. 即使删除代码，git history 中仍有记录（需 `git filter-branch` 清洗历史）

**修复方案（Day 0 必须完成）**:
```python
# tools/qmt_data_manager.py 修复版:
import os
ACCOUNT_ID = os.environ.get('EASYXT_ACCOUNT_ID', '')   # ← 环境变量
PASSWORD   = os.environ.get('EASYXT_PASSWORD', '')      # ← 环境变量
if not ACCOUNT_ID or not PASSWORD:
    raise RuntimeError(
        "必须设置 EASYXT_ACCOUNT_ID 和 EASYXT_PASSWORD 环境变量，"
        "禁止在代码中硬编码凭据。"
    )
```
```json
// config/unified_config.json 修复版:
"account_id": "${EASYXT_ACCOUNT_ID}",
"password": "${EASYXT_PASSWORD}"
```
```gitignore
# .gitignore 增加:
config/secrets.json
config/unified_config.json
*.credentials
.env
```
**启动时验证**：凭据缺失 → 立即 `RuntimeError` 中止，而非静默启动后在交易时才失败。

## 4.2 硬编码路径清单

| 文件 | 行号 | 硬编码路径 |
|------|------|-----------|
| config/unified_config.json | L29 | `D:\申万宏源策略量化交易终端\bin.x64\XtItClient.exe` |
| config/unified_config.json | L31 | `D:\申万宏源策略量化交易终端\userdata_mini` |
| datasource_registry.py | L80 | `D:/StockData/raw` |
| duckdb_connection_pool.py | L37-38 | `D:/StockData/stock_data.ddb` |
| tools/ (7个文件) | 多处 | `D:\申万宏源策略量化交易终端\userdata_mini\datadir` |
| 学习实例/ (5个文件) | 多处 | `D:\国金QMT交易端模拟\userdata_mini` |

**总计**: 14+ 处硬编码绝对路径，分散在 config/data_manager/tools/学习实例 四个层级。

---

# 第五篇：测试与CI/CD现状

## 5.1 测试覆盖率矩阵

| data_manager/ 模块 | 有测试？ | 测试文件 | 覆盖度评估 |
|-------------------|---------|---------|-----------|
| unified_data_interface.py | ⚠ 部分 | test_unified_data_interface.py | 仅基本CRUD,无回退/合并/时间戳测试 |
| datasource_registry.py | ✅ | test_datasource_registry.py | 基本注册/查询 |
| duckdb_connection_pool.py | ✅ | test_duckdb_connection_pool.py | 单例/连接/WAL |
| csv_importer.py | ✅ | test_csv_importer.py | 基本导入 |
| auto_data_updater.py | ❌ | 无 | — |
| history_backfill_scheduler.py | ❌ | 无 | — |
| data_integrity_checker.py | ❌ | 无 | — |
| smart_data_detector.py | ❌ | 无 | — |
| duckdb_fivefold_adjust.py | ❌ | 无 | — |
| universal_data_importer.py | ❌ | 无 | — |
| realtime_pipeline_manager.py | ❌ | 无 | — |
| board_stocks_loader.py | ❌ | 无 | — |
| financial_data_saver.py | ❌ | 无 | — |

**覆盖率**: 4/13 模块有测试 = **30.8%**，其中仅 `datasource_registry` 和 `duckdb_connection_pool` 的测试较为完整。

## 5.2 CI/CD 现状

| 基础设施 | 存在？ |
|----------|--------|
| .github/workflows/ | ❌ 不存在 |
| tox.ini | ❌ 不存在 |
| Makefile | ❌ 不存在 |
| pre-commit hooks | ❌ 不存在 |
| pytest.ini | ✅ 存在 |
| mypy.ini | ✅ 存在 |
| pyproject.toml | ✅ 存在 (但无test/lint配置) |
| conftest.py | ✅ 存在 (含integration marker) |

**结论**: 有测试框架配置，但没有自动化执行机制。所有测试必须手动运行。

## 5.3 最低可行CI/CD方案

```yaml
# .github/workflows/data-quality-gate.yml (建议)
name: Data Quality Gate
on: [push, pull_request]
jobs:
  test:
    runs-on: windows-latest  # QMT仅Windows
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run unit tests
        run: pytest tests/ -m "not integration" --tb=short
      - name: Run mypy
        run: mypy data_manager/ --ignore-missing-imports
      - name: Check SQL injection patterns
        run: |
          # 搜索非参数化SQL
          grep -rn "f\".*WHERE.*'{" data_manager/ && exit 1 || echo "No SQL injection patterns found"
```

---

# 第六篇：工具→生产提升清单

## 6.1 已验证但未集成的功能

| 函数 | 来源文件 | 验证状态 | 应提升到 |
|------|---------|---------|---------|
| `read_dat_fast()` | tools/_ultimate_crossval_v4.py | ✅ 5596品种100%匹配 | `data_manager/dat_binary_reader.py` |
| `agg_1m_to_1d()` | tools/_ultimate_crossval_v4.py | ✅ 含夜盘归属 | `data_manager/period_aggregator.py` |
| `classify_symbol()` | tools/_ultimate_crossval_v4.py | ✅ 股票/ETF/指数/期货/可转债 | `core/symbol_classifier.py` |
| `is_thin_trading_day()` | tools/data_audit_repair.py | ✅ 期货薄交易识别 | `core/trading_knowledge.py` |
| `_make_assign_fn()` | tools/consistency_checker.py | ✅ 夜盘→下交易日归属 | `core/trading_knowledge.py` |
| `_near_equal()` | tools/consistency_checker.py | ✅ 容差比较(品种级) | `core/data_contract.py` |
| `compare_volume()` | tools/_ultimate_crossval_v4.py | ✅ 精确量匹配 | `data_manager/data_integrity_checker.py` |

## 6.2 提升优先级

```
第一批 (数据正确性基石):
  ├─ read_dat_fast() → DATBinarySource       # 绕过xtquant版本限制
  ├─ classify_symbol() → SymbolClassifier      # 品种级规则前提
  └─ _make_assign_fn() → TradingDayAssigner    # 夜盘归属

第二批 (数据质量门禁):
  ├─ agg_1m_to_1d() → 守恒校验               # 1m→1d volume一致
  ├─ is_thin_trading_day() → 薄交易识别       # 避免误报
  └─ _near_equal() → 容差框架                 # 品种级精度

第三批 (验证自动化):
  └─ compare_volume() → 定期交叉校验流水线    # 周级全量验证
```

---

# 第七篇：分层落地方案

## 7.1 优先级矩阵

| 优先级 | 问题 | 影响 | 修复复杂度 | 依赖 |
|--------|------|------|-----------|------|
| **P0-S** | 明文密码 (§4.1) | 安全 | 低 (2小时) | 无 |
| **P0-S** | SQL注入 ×10处 (§2.1.3, §2.6.1, §2.7.3) | 安全 | 低 (3小时) | 无 |
| **P0-D** | 时间戳统一 (§1.6) | 数据正确性 | 高 (2-3天) | P0-S |
| **P0-D** | 完整性判定修复 (§2.1.6) | 数据完整性 | 中 (1天) | 交易日历 |
| **P0-D** | DAT直读提升 (§6.1) | 数据可用性 | 中 (1天) | 无 |
| **P0-D** | DELETE+INSERT原子化 (§3.2) | 数据一致性 | 低 (3小时) | 无 |
| **P1** | 交易日历完善 (§2.7.1) | 数据完整性 | 中 (1天) | 无 |
| **P1** | validators.py集成 (§3.1) | 防御深度 | 中 (1天) | 无 |
| **P1** | 更新器检查点 (§2.4.4) | 断点续传 | 中 (1天) | 无 |
| **P1** | backfill持久化 (§2.5.2) | 任务可靠性 | 中 (1天) | 无 |
| **P1** | 品种级异常阈值 (§2.6.2) | 质检准确性 | 中 (1天) | classify_symbol |
| **P2** | 连接计数器原子化 (§2.3.3) | 稳定性 | 低 (1小时) | 无 |
| **P2** | WAL修复锁 (§2.3.4) | 稳定性 | 低 (1小时) | 无 |
| **P2** | 实时管线RLock (§2.11.1) | 线程安全 | 低 (1小时) | 无 |
| **P2** | 测试覆盖率提升 (§5.1) | 质量保障 | 高 (持续) | 所有修复 |
| **P2** | CI/CD搭建 (§5.2) | 工程化 | 中 (1天) | pytest.ini |

## 7.2 Phase 0: 安全紧急修复 + 72 小时可执行闭环

### Day 0（今天，≤8小时）：凭据清零 + 启动告警
```
□ tools/qmt_data_manager.py L29-L33: 删除明文 ACCOUNT_ID/PASSWORD → 改用 os.environ
□ config/unified_config.json: 密码字段改为 ${EASYXT_***} 占位符
□ .gitignore: 增加 config/unified_config.json, *.credentials, .env
□ 启动检查: 凭据缺失 → RuntimeError 立即中止，禁止静默运行
□ 全部 10+ 处 SQL 注入 → 参数化查询（见附录 A）
□ 验收命令: python tools/p0_gate_check.py --check credential
□ 期望输出: credential_scan == pass
```

### Day 1（明天，≤8小时）：DAT直读提升为正式数据源
```
□ 从 tools/_ultimate_crossval_v4.py 提取 read_dat_fast() → dat_binary_reader.py
□ 新建 data_manager/dat_binary_reader.py: 实现 DataSource ABC → DATBinarySource 类
□ 注册到 datasource_registry.DataSourceRegistry
□ 接入统一健康检查: ping() → HealthStatus
□ easy_xt/triple_source_manager.py L93: import xtdata → import xtquant.xtdata
□ 验收命令: python -c "from data_manager.dat_binary_reader import DATBinarySource; print('OK')"
□ 验收命令: grep -n "import xtdata" easy_xt/ tools/ | grep -v "xtquant" 返回空
```

### Day 2（后天，≤8小时）：统一质检门禁
```
□ 实现 TradingCalendar（接入 chinese-calendar 或内建精确表）
□ 1m→1d 守恒校验: sum(1m_volume) 与 daily_volume 对齐
□ 夜盘归属规则: assign_night_session_to_next_trading_day()
□ 空文件检测: volume == 0 且非已知停牌日 → 告警
□ 异常时间戳: 全部 datetime.fromtimestamp(x/1000) → UTC ms 存储（见附录 G）
□ 出口门禁: get_stock_data() 返回前执行 DataValidator 校验
□ 验收命令: python tools/p0_gate_check.py --check timestamp
□ 期望输出: timestamp_contract_check == pass
```

### Day 3（第三天，≤8小时）：发布原子化
```
□ DELETE + INSERT 全部替换为 BEGIN TRANSACTION/COMMIT/ROLLBACK（4 个文件，见 §3.2）
□ 实现 staging→validate→publish 三段发布
□ 禁止半新半旧数据集: QMT 返回部分日期 → 日期连续性检测 → 拒绝写入
□ 验收命令: python tools/p0_gate_check.py --check publish
□ 期望输出: snapshot_publish_atomic == pass
□ 全量验收: python tools/p0_gate_check.py --strict
□ 期望输出: strict_pass == true, P0_open_count == 0
```

## 7.3 Phase D0: 数据契约与知识基底

```
D0-1: core/data_contract.py
  ├─ BarData dataclass (时间戳UTC ms, 字段标准化)
  ├─ QualityFlags enum (已校验/竞价含/夜盘归属/复权类型)
  └─ Tolerance matrix (品种级精度容差)

D0-2: core/trading_knowledge.py
  ├─ TradingCalendar (接入 chinese-calendar 包或内建精确表)
  ├─ SymbolClassifier (从 tools/ 提升)
  ├─ TradingDayAssigner (夜盘归属规则)
  ├─ PriceLimitRules (品种级涨跌停幅度)
  └─ AuctionAbsorptionRules (v4铁律编码)

D0-3: data_manager/dat_binary_reader.py
  ├─ DATReader (从 tools/ 提升的 read_dat_fast)
  ├─ DATBinarySource (实现 DataSource ABC)
  └─ 注册到 DataSourceRegistry

D0-4: HC-3 时间戳规范执行
  ├─ 存储: UTC epoch ms (int64)
  ├─ 计算: 显式北京时间转换 (ZoneInfo("Asia/Shanghai"))
  ├─ 展示: 仅UI层格式化
  └─ 全代码库搜索替换 (12种模式统一)
```

## 7.4 Phase D1: 获取层加固

```
D1-1: unified_data_interface.py 重构
  ├─ 完整性判定: 接入 TradingCalendar 精确计算
  ├─ 回退链: 接入 DataSourceRegistry (不再硬编码)
  ├─ 出口门禁: 返回前执行 DataValidator 校验
  └─ 错误处理: 区分 DataNotFound / DataCorrupt / SystemError

D1-2: datasource_registry.py 扩展
  ├─ 注册: DATBinarySource, QMTAPISource, AKShareSource
  ├─ 质量校验: get_data() 返回前检查字段/NaN/范围
  ├─ 健康检查: 每个源实现 ping() → HealthStatus
  └─ 降级策略: 可配置优先级 + 基于质量的智能选源

D1-3: auto_data_updater.py 加固
  ├─ 检查点: checkpoint.json 每100只写一次
  ├─ 断点续传: 重启后从检查点恢复
  ├─ 自适应延迟: 指数退避 + 成功时加速
  ├─ 幂等性: manual_update() 检查last_update_time
  └─ 审计日志: 每次更新结果持久化到 data_update_log 表

D1-4: history_backfill_scheduler.py 加固
  ├─ 持久化队列: DuckDB表替代内存PriorityQueue
  ├─ 永久/暂时失败区分: 网络超时(暂时) vs 品种不存在(永久)
  ├─ 溢出策略: 队列满时按优先级淘汰低优任务
  └─ 恢复模式: 重启后从DuckDB恢复未完成任务
```

## 7.5 Phase D2: 质检门禁

```
D2-1: data_integrity_checker.py 升级
  ├─ 接入 TradingCalendar → 精确缺失检测
  ├─ 接入 SymbolClassifier → 品种级阈值
  ├─ 接入 PriceLimitRules → 涨跌停校验
  ├─ 新增: 停牌日识别 (volume=0且非假日)
  ├─ 新增: IPO首日特殊处理
  ├─ 新增: 1m→1d守恒校验 (从 tools/ 提升)
  ├─ 新增: 竞价bar检测
  └─ SQL注入修复: 全部参数化

D2-2: 自动化质检流水线
  ├─ 日终自动运行完整性检查
  ├─ 周末运行全量交叉校验
  ├─ 质量报告持久化到 data_quality_report 表
  └─ 质量低于阈值 → 告警 + 策略层拒绝服务
```

## 7.6 Phase D3: 可观测性与CI/CD

```
D3-1: CI/CD基础
  ├─ .github/workflows/test.yml → push触发单元测试
  ├─ .github/workflows/lint.yml → mypy + SQL注入检测
  ├─ pre-commit hook → 禁止明文密码提交
  └─ 测试覆盖率门禁 → 新代码必须≥80%覆盖

D3-2: 数据血缘
  ├─ 每条记录记录: source, fetch_time, transform_chain
  ├─ data_lineage 表: symbol × date × source → transform → storage
  └─ 可查询: "这条数据是什么时候从哪里来的"

D3-3: 环境管理
  ├─ docs/environment_setup.md: 双环境使用指南
  ├─ pyproject.toml: [tool.easyxt.environments] 声明
  ├─ 运行时检测: xtquant可用? → QMT桥接; 否则 → DAT直读
  └─ 测试矩阵: Python 3.11(含xtquant) + Python 3.13(不含)
```

---

# 附录

## 附录A: 全部SQL注入定位

| # | 文件 | 行号 | 方法 | 插值变量 |
|---|------|------|------|---------|
| 1 | unified_data_interface.py | L1160 | `_read_from_duckdb` | stock_code |
| 2 | unified_data_interface.py | L1163 | `_read_from_duckdb` | start_date |
| 3 | unified_data_interface.py | L1164 | `_read_from_duckdb` | end_date |
| 4 | unified_data_interface.py | L1200 | `_save_to_duckdb` | stock_code, table |
| 5 | unified_data_interface.py | L1210 | `_save_to_duckdb` | start_date, end_date |
| 6 | unified_data_interface.py | L1250 | `_count_records` | stock_code, table |
| 7 | data_integrity_checker.py | L166 | `_check_data_quality` | stock_code |
| 8 | data_integrity_checker.py | L220 | `_check_price_relations` | stock_code |
| 9 | data_integrity_checker.py | L256 | `_check_outliers` | stock_code |
| 10 | smart_data_detector.py | L237 | `detect_missing` | stock_code |
| 11 | financial_data_saver.py | L89 | `save_financial_data` | stock_code |

## 附录B: 12种时间戳模式定位

（见 §1.6 完整表格，含文件名+行号）

## 附录C: 4处非原子DELETE+INSERT定位

（见 §3.2 完整表格，含文件名+行号）

## 附录D: 审计数据来源

| 审计轮次 | 覆盖文件 | 发现数据量 |
|---------|---------|-----------|
| Sub-agent #1 | unified_data_interface, datasource_registry, duckdb_connection_pool | 38KB |
| Sub-agent #2 | auto_data_updater, history_backfill_scheduler, data_integrity_checker | 27KB |
| Sub-agent #3 | fivefold_adjust, universal_importer, csv_importer, smart_detector, realtime_pipeline, board_loader, financial_saver | 25KB |
| Sub-agent #4 | core/*, easy_xt/*, config, tests, tools | 18KB |
| Direct code reading | 7次精确行号定位 | — |
| v2.1 增补 | triple_source_manager.py L92-99, qmt_data_manager.py L29-33, 环境版本实测 | — |

---

## 附录 E：事实校验附录（P0 可复现命令）

> 每个 P0 结论均配一条可直接运行的验证命令和期望输出，供评审会现场核查、避免争议。

### P0-S1：明文凭据存在性

```powershell
# 命令（在项目根目录运行）：
Select-String -Path "tools\qmt_data_manager.py","config\unified_config.json" `
  -Pattern "PASSWORD|password" | Select-Object Path,LineNumber,Line

# 当前期望（修复前）：
# tools\qmt_data_manager.py:31: PASSWORD = "test1234！"
# config\unified_config.json:32: "password": "test1234！"

# 修复后期望（pass 状态）：
# tools\qmt_data_manager.py:31: PASSWORD = os.environ.get('EASYXT_PASSWORD', '')
# config\unified_config.json:32: "password": "${EASYXT_PASSWORD}"
```

**fail-fast 条件**: 若输出包含 `"[a-zA-Z0-9!@#$%^&*]+"` 形式的字面密码值 → 立即阻断 CI。

### P0-S2：SQL 注入存在性

```powershell
# 命令：
Get-ChildItem -Recurse -Filter "*.py" -Path data_manager,easy_xt |
  Select-String -Pattern "f['\"].*WHERE.*'\{" | Select-Object Path,LineNumber

# 当前期望（修复前，至少11条）：
# data_manager\unified_data_interface.py:1160: ...WHERE stock_code = '{stock_code}'...
# (等11处)

# 修复后期望（pass 状态）：
# （无输出）
```

**fail-fast 条件**: 输出行数 > 0 → CI 失败，拒绝合并。

### P0-D1：时间戳危险模式存在性

```powershell
# 命令：
Get-ChildItem -Recurse -Filter "*.py" |
  Select-String -Pattern "datetime\.fromtimestamp\(.*\/\s*1000\)" |
  Select-Object Path,LineNumber,Line

# 修复前期望：
# data_manager\unified_data_interface.py:1293: datetime.fromtimestamp(x / 1000)
# data_manager\unified_data_interface.py:1419: datetime.fromtimestamp(x/1000).strftime(...)

# 修复后期望（pass 状态）：
# （无输出 — 已全部替换为 UTC ms 存储方案）
```

**fail-fast 条件**: 出现 `datetime.fromtimestamp(x/1000)` → 时区依赖，阻断。

### P0-D2：非原子 DELETE+INSERT 存在性

```powershell
# 命令（搜索 DELETE 后紧跟 INSERT 且无 TRANSACTION 包裹的模式）：
python -c "
import ast, glob, sys
issues = []
for path in glob.glob('**/*.py', recursive=True):
    try:
        src = open(path).read()
        if 'DELETE FROM' in src and 'INSERT INTO' in src and 'TRANSACTION' not in src:
            issues.append(path)
    except: pass
print('\n'.join(issues))
print(f'Non-atomic files: {len(issues)}')
"

# 修复前期望：
# data_manager/unified_data_interface.py
# data_manager/duckdb_fivefold_adjust.py
# data_manager/financial_data_saver.py
# Non-atomic files: 3

# 修复后期望（pass 状态）：
# Non-atomic files: 0
```

**fail-fast 条件**: `Non-atomic files > 0` → 数据一致性风险，阻断。

### P0-D3：xtdata 裸导入存在性

```powershell
# 命令：
Select-String -Path (Get-ChildItem -Recurse -Filter "*.py") `
  -Pattern "^\s*import xtdata\b" | Select-Object Path,LineNumber,Line

# 修复前期望：
# easy_xt\triple_source_manager.py:93:         import xtdata

# 修复后期望（pass 状态）：
# （无输出）
```

**fail-fast 条件**: 出现 `import xtdata`（非 `xtquant.xtdata`）→ QMT 可用性误判，阻断。

### P0-D4：DuckDB Schema 字段类型

```python
# 命令（连接数据库直接检查）：
python -c "
import duckdb, os
db = os.environ.get('EASYXT_DB_PATH', 'D:/StockData/stock_data.ddb')
con = duckdb.connect(db, read_only=True)
schema = con.execute(\"DESCRIBE stock_daily\").fetchdf()
print(schema[['column_name','column_type']].to_string())
"

# 期望（修复前，显示问题字段）：
# date       VARCHAR    ← 应为 DATE
# volume     DOUBLE     ← 应为 BIGINT

# 期望（修复后 pass 状态）：
# date       DATE
# volume     BIGINT
```

**fail-fast 条件**: `date` 列类型为 VARCHAR 或 `volume` 列类型为 DOUBLE → Schema 有问题，告警。

### P0-D5：环境版本验证

```powershell
# 命令：
& "C:\Users\wangzhong\miniconda3\envs\myenv\python.exe" --version

# 期望输出（已实测，以此为准）：
# Python 3.11.14

# 反例（文档中的过时记录，已纠偏）：
# Python 3.13.x  ← 此版本无法使用 xtquant，不是 myenv 的实际版本
```

---

## 附录 F：数据血缘字段标准

> 每条入库记录必须携带以下血缘字段，以支持"这条数据从哪来、何时来、有没有被修改"的全链路追溯。

### F.1 字段定义

| 字段名 | 类型 | 含义 | 示例 |
|--------|------|------|------|
| `source` | VARCHAR(32) | 数据来源标识符 | `"qmt"`, `"akshare"`, `"dat_file"`, `"csv_import"` |
| `ingest_run_id` | VARCHAR(36) | 本次采集任务 UUID（每次 update/backfill 新生成） | `"550e8400-e29b-41d4-a716-446655440000"` |
| `raw_hash` | VARCHAR(64) | 原始响应 / 文件内容的 SHA-256 前16字节 | `"a3f5c2d1e8b7..."` |
| `normalized_hash` | VARCHAR(64) | 归一化后 DataFrame 的行级哈希（OHLCV 合并） | `"b9e1f4a2..."` |
| `schema_version` | SMALLINT | 表 Schema 版本号（迁移时递增） | `3` |
| `ingest_ts_utc_ms` | BIGINT | 入库时刻 UTC 毫秒时间戳 | `1709654400000` |

### F.2 Schema 扩展 SQL

```sql
-- 在现有 stock_daily / stock_1m 表上追加血缘列：
ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS source         VARCHAR(32)  DEFAULT 'unknown';
ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS ingest_run_id  VARCHAR(36)  DEFAULT '';
ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS raw_hash       VARCHAR(64)  DEFAULT '';
ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS normalized_hash VARCHAR(64) DEFAULT '';
ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS schema_version  SMALLINT    DEFAULT 1;
ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS ingest_ts_utc_ms BIGINT     DEFAULT 0;
```

### F.3 写入时填充规范

```python
import uuid, hashlib, time

def _build_lineage(source: str, raw_bytes: bytes, df_normalized) -> dict:
    return {
        "source":           source,
        "ingest_run_id":    str(uuid.uuid4()),
        "raw_hash":         hashlib.sha256(raw_bytes).hexdigest()[:16],
        "normalized_hash":  hashlib.sha256(df_normalized.to_csv().encode()).hexdigest()[:16],
        "schema_version":   CURRENT_SCHEMA_VERSION,   # 常量，随 Schema 迁移递增
        "ingest_ts_utc_ms": int(time.time() * 1000),
    }
```

### F.4 查询示例

```sql
-- 追溯某条记录的来源和入库时间：
SELECT stock_code, date, close, source, ingest_run_id,
       strftime(to_timestamp(ingest_ts_utc_ms / 1000), '%Y-%m-%d %H:%M:%S') AS ingest_time
FROM stock_daily
WHERE stock_code = '600519.SH' AND date = '2026-03-07';

-- 找出同一品种、同一日期被多次写入（去重问题定位）：
SELECT stock_code, date, COUNT(*) AS cnt, array_agg(DISTINCT source) AS sources
FROM stock_daily
GROUP BY stock_code, date
HAVING cnt > 1;
```

---

## 附录 G：单一时间语义合约

> 整个代码库必须遵守唯一时间语义，消除"在 UTC+8 Windows 上碰巧正确"的隐患。

### G.1 三层时间模型

```
┌────────────────────────────────────────────────────────────────┐
│  Layer 1 — 存储层（物理存储&传输）                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  格式: int64 UTC 毫秒时间戳                               │   │
│  │  示例: 1709251200000  (= 2024-03-01 00:00:00 UTC)        │   │
│  │  字段: stock_daily.date → 保留 VARCHAR 兼容旧数据，        │   │
│  │         新增 bar_ts_utc_ms BIGINT 字段作为精确时间戳        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                │
│  Layer 2 — 业务层（计算&归属逻辑）                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  显式转换: 北京时间 (Asia/Shanghai)                       │   │
│  │  from zoneinfo import ZoneInfo                          │   │
│  │  SHANGHAI = ZoneInfo("Asia/Shanghai")                   │   │
│  │  bar_dt = datetime.fromtimestamp(ts_ms/1000, tz=SHANGHAI)│   │
│  │  trading_date = bar_dt.date()  # 夜盘已归属下交易日       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                │
│  Layer 3 — 展示层（UI/报告/日志）                               │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  仅在展示时格式化: bar_dt.strftime('%Y-%m-%d %H:%M')      │   │
│  │  禁止将格式化字符串存回数据库                              │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────┘
```

### G.2 禁止模式 → 替换方案

| ❌ 禁止模式 | ✅ 替换方案 | 影响范围 |
|-----------|-----------|---------|
| `datetime.fromtimestamp(x/1000)` | `datetime.fromtimestamp(x/1000, tz=ZoneInfo("Asia/Shanghai"))` | unified_data_interface.py L1293, L1419 |
| `pd.to_datetime(ts, unit='ms')` | `pd.to_datetime(ts, unit='ms', utc=True).dt.tz_convert("Asia/Shanghai")` | unified_data_interface.py L1317 |
| `datetime.now()` | `datetime.now(tz=ZoneInfo("Asia/Shanghai"))` | auto_data_updater.py L180 |
| `date.today()` | `datetime.now(tz=ZoneInfo("Asia/Shanghai")).date()` | smart_data_detector.py |
| `int(ts + 28800)` 硬编码偏移 | 使用 ZoneInfo，不硬编码秒数 | tools/_ultimate_crossval_v4.py L110 |

### G.3 夜盘归属规则

```python
from zoneinfo import ZoneInfo
SHANGHAI = ZoneInfo("Asia/Shanghai")

# 夜盘时间范围（上期所 / 大商所 / 郑商所）：
# 21:00 — 次日02:30（铜铝锌等）
# 21:00 — 23:59（农产品等）
# 当 bar 时间 >= 21:00 时，该 bar 归属于下一个交易日

def assign_trading_date(bar_ts_ms: int) -> date:
    """将 bar 时间戳映射到业务归属交易日（含夜盘归属）"""
    dt = datetime.fromtimestamp(bar_ts_ms / 1000, tz=SHANGHAI)
    if dt.hour >= 21:
        # 夜盘 → 归属下一自然日（再由交易日历确认是否是交易日）
        return (dt + timedelta(days=1)).date()
    return dt.date()
```

### G.4 门禁检测脚本片段

```python
# tools/p0_gate_check.py 中 timestamp 检查逻辑：
import ast, pathlib, re

DANGER_PATTERNS = [
    r"datetime\.fromtimestamp\([^,)]+/\s*1000\s*\)",   # 无时区
    r"pd\.to_datetime\([^)]+unit=['\"]ms['\"]\s*\)",    # 无 utc=True
    r"datetime\.now\(\s*\)",                            # 无时区
]

def check_timestamp_contract(src_dirs=("data_manager", "easy_xt", "core")):
    violations = []
    for d in src_dirs:
        for f in pathlib.Path(d).rglob("*.py"):
            src = f.read_text(encoding="utf-8", errors="ignore")
            for pat in DANGER_PATTERNS:
                for m in re.finditer(pat, src):
                    violations.append(f"{f}:{src[:m.start()].count(chr(10))+1}: {m.group()}")
    return violations
```

---

*本报告 v2.1 基于 v2.0 全量内容，追加 5 项工程增强：事实校验附录、P0 门禁化、数据血缘标准、时间语义合约、环境纠偏。*
*v2.1 审计日期: 2026-03-09 | 环境实测: myenv = Python 3.11.14*
