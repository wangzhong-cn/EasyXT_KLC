# 数据血缘字段规范 (Data Lineage Specification)

> 版本: 1.0
> 生效日期: 2026-03-10
> 适用范围: `data_manager/unified_data_interface.py` → `data_ingestion_status` 表

---

## 一、字段列表总览

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `schema_version` | VARCHAR | 是 | 字段集版本，默认 `'1.0'`，新增字段须升版本 |
| `ingest_run_id` | VARCHAR | 是 | 每次写入的唯一 UUID，由 `uuid.uuid4()` 自动生成 |
| `raw_hash` | VARCHAR | 是 | 写入前 DataFrame 的内容指纹（SHA-256 前 16 位十六进制） |
| `source_event_time` | TIMESTAMP | 否 | DataFrame 中最大业务时间戳（可为 NULL，见第四节） |

---

## 二、`schema_version` 升级策略与兼容矩阵

### 2.1 版本格式
`MAJOR.MINOR`，例如 `1.0`、`1.1`、`2.0`。

### 2.2 升级触发条件

| 变更类型 | 版本影响 | 示例 |
|----------|----------|------|
| 新增可空列（向后兼容） | MINOR +1 | `1.0 → 1.1` |
| 新增非空列 / 列默认值变更 | MINOR +1 | `1.1 → 1.2` |
| 删除列 / 列重命名 / 类型不兼容变更 | MAJOR +1 | `1.x → 2.0` |
| 仅逻辑变更（无列结构变动） | 不升版本 | — |

### 2.3 迁移流程（MINOR 升级）

```sql
-- 1. 新增列（幂等 ALTER TABLE，已存在则忽略）
ALTER TABLE data_ingestion_status ADD COLUMN IF NOT EXISTS new_col VARCHAR;

-- 2. 回填默认值（历史行补零/空，不强制 NOT NULL）
UPDATE data_ingestion_status SET new_col = 'default' WHERE new_col IS NULL;

-- 3. 更新写入代码中的 schema_version 默认值常量
-- CURRENT_SCHEMA_VERSION = "1.1"  -- unified_data_interface.py
```

### 2.4 MAJOR 升级流程（破坏性变更）

```
1. 创建新表 data_ingestion_status_v2，包含全新列集合
2. 从旧表迁移兼容行（ETL脚本）
3. 新写入路径指向 _v2，旧读取路径保留 N 周双读窗口
4. 窗口关闭后 DROP 旧表，rename _v2 → data_ingestion_status
```

### 2.5 兼容矩阵（截至本规范生效）

| schema_version | 列集合 | 读写兼容 | 状态 |
|---------------|--------|----------|------|
| `< 1.0` (历史) | 无血缘字段 | 只读（回填脚本补齐） | 已迁移 |
| `1.0` (当前) | schema_version + ingest_run_id + raw_hash + source_event_time | 读写 | **默认** |
| `1.1` (规划) | +`pipeline_tag` VARCHAR（批次标签） | 预留，未启用 | 待定 |

---

## 三、`raw_hash` 哈希口径规范

### 3.1 算法
```
raw_hash = SHA-256( DataFrame.to_csv(index=True, encoding="utf-8") ).hexdigest()[:16]
```

**精确口径（必须严格遵守，否则哈希不可重现）**：

| 口径项 | 规定值 | 说明 |
|--------|--------|------|
| 序列化方法 | `DataFrame.to_csv(index=True)` | 含 index 列，列顺序按 DataFrame 自然顺序 |
| 列排序 | **不排序**（保留 DataFrame 原始列序） | 上游代码决定列序，不做额外 sort |
| 空值表示 | `to_csv` 默认空字符串 `""` | 不替换 NaN/None |
| 浮点精度 | `to_csv` 默认精度（float_format=None） | 不做四舍五入/截断 |
| 字符串编码 | `UTF-8`，errors="replace" | 不允许 latin-1 回退 |
| 哈希截断 | 取前 **16 位**十六进制（64 位熵） | |
| 错误降级 | 序列化失败时写入字面量 `"error"` | 审计时需标注降级原因 |

### 3.2 重新计算方法（审计核验）

```python
import hashlib, pandas as pd

def recompute_raw_hash(df: pd.DataFrame) -> str:
    """按官方口径重新计算 raw_hash，用于审计核验。"""
    serialized = df.to_csv(index=True).encode("utf-8", errors="replace")
    return hashlib.sha256(serialized).hexdigest()[:16]
```

### 3.3 典型值示例

| 场景 | raw_hash 示例 |
|------|--------------|
| 正常写入（500行1d数据） | `a3f2e1b9d48c7601` |
| 空 DataFrame | `9a48a4d0f1b8e2c3` |
| 序列化异常降级 | `error` |

---

## 四、`source_event_time` 可空原因字典

`source_event_time` 代表本次写入数据集中最大的业务时间戳（如行情日期/分钟时间）。
该字段允许为 `NULL`，每种可空情形须在审计注释中明确标注原因。

### 4.1 可空原因代码表

| 原因代码 | 说明 | 出现场景 |
|----------|------|----------|
| `HIST_BACKFILL` | 历史行回填，无法追溯原始业务时间 | `lineage_backfill.py` 执行时 |
| `SRC_MISSING` | 上游数据源未提供时间戳字段 | 第三方数据接口无 `datetime`/`date` 列 |
| `HALT_NO_EVENT` | 停牌/停市日，无行情事件发生 | 停牌股票补空行时 |
| `EMPTY_DF` | DataFrame 为空，无法提取时间 | 下载结果为空 |
| `PARSE_ERROR` | 时间戳解析异常（`pd.to_datetime` 失败） | 源数据时间格式异常 |
| `INDEX_FALLBACK_FAIL` | 尝试从 index 提取时间也失败 | 非时间类 index，且无时间列 |

### 4.2 审计查询模板

```sql
-- 非空率检查（target > 95%）
SELECT
    COUNT(*) AS total,
    COUNT(source_event_time) AS has_event_time,
    ROUND(COUNT(source_event_time) * 100.0 / COUNT(*), 2) AS fill_rate_pct
FROM data_ingestion_status;

-- NULL 行分布（按 stock_code 排序，辅助定位问题数据源）
SELECT stock_code, COUNT(*) AS null_rows
FROM data_ingestion_status
WHERE source_event_time IS NULL
GROUP BY stock_code
ORDER BY null_rows DESC
LIMIT 20;

-- 最近 30 天新入库行中的 NULL 比率
SELECT
    DATE_TRUNC('day', created_at) AS ingest_date,
    COUNT(*) AS rows,
    COUNT(source_event_time) AS has_time,
    ROUND(COUNT(source_event_time) * 100.0 / COUNT(*), 2) AS rate
FROM data_ingestion_status
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1
ORDER BY 1 DESC;
```

### 4.3 SLA 目标

| 数据集类型 | source_event_time 非空率目标 |
|------------|------------------------------|
| 新增写入（非回填） | ≥ 95% |
| 历史回填行 | 允许为 NULL（原因 = HIST_BACKFILL） |
| 全量汇总 | ≥ 70%（历史存量拉低合理范围） |

---

## 五、版本变更历史

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| 1.0 | 2026-03-10 | 初始版本，定义 4 个血缘字段、raw_hash 口径、source_event_time 可空原因表 |
