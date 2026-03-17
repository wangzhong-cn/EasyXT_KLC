# EasyXT_KLC 环境变量参考

## 数据源与路由
- `EASYXT_TUSHARE_TOKEN`: Tushare Token，配置后启用 Tushare 历史数据兜底。
- `TUSHARE_TOKEN`: Tushare Token 兼容变量（低优先级）。
- `XTQUANT_PATH`: QMT/xtquant 路径。
- `QMT_PATH`: QMT 路径兼容变量。

## DuckDB 与可靠性
- `EASYXT_DUCKDB_PATH`: DuckDB 文件路径。
- `EASYXT_DUCKDB_PREFER_RW`: `1/0`，是否优先读写连接。
- `EASYXT_ENABLE_WAL_AUTO_REPAIR`: `1/0`，是否在 WAL 回放异常时自动修复。
- `EASYXT_ENABLE_AUTO_CHECKPOINT`: `1/0`，是否开启定时 CHECKPOINT。
- `EASYXT_CHECKPOINT_INTERVAL_S`: 自动 CHECKPOINT 周期（秒）。

## 回填与容错
- `EASYXT_BACKFILL_ENABLED`: `1/0`，是否启用历史补数调度。
- `EASYXT_BACKFILL_MAX_QUEUE`: 历史补数队列上限。
- `EASYXT_REMOTE_CB_THRESHOLD`: 远程数据源熔断阈值（失败次数）。
- `EASYXT_REMOTE_BACKOFF_BASE_S`: 熔断基准退避时间（秒）。
- `EASYXT_REMOTE_BACKOFF_MAX_S`: 熔断最大退避时间（秒）。

## 数据治理
- `EASYXT_SOURCE_CONFLICT_DELTA`: 多源收盘价冲突阈值，默认 `0.02`（2%）。
- `EASYXT_CHART_FETCH_TIMEOUT_S`: 图表在线拉取超时（秒）。

## 接口服务
- `EASYXT_API_PORT`: 本地 API/WS 端口，默认 `8000`。
