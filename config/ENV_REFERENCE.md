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
- `EASYXT_DUCKDB_CRASH_SCAN_EXTRA_PATHS`: DuckDB 崩溃签名扫描附加路径（`;` 分隔）。
- `EASYXT_DUCKDB_CRASH_SCAN_MAX_AGE_HOURS`: DuckDB 崩溃签名扫描时间窗口（小时），默认 `24`。
- `EASYXT_DUCKDB_CRASH_BASELINE`: DuckDB 崩溃签名基线路径，默认 `artifacts/duckdb_crash_baseline.json`。
- 基线样例文件：`config/duckdb_crash_baseline.example.json`。

## 回填与容错
- `EASYXT_BACKFILL_ENABLED`: `1/0`，是否启用历史补数调度。
- `EASYXT_BACKFILL_MAX_QUEUE`: 历史补数队列上限。
- `EASYXT_REMOTE_CB_THRESHOLD`: 远程数据源熔断阈值（失败次数）。
- `EASYXT_REMOTE_BACKOFF_BASE_S`: 熔断基准退避时间（秒）。
- `EASYXT_REMOTE_BACKOFF_MAX_S`: 熔断最大退避时间（秒）。

## 数据治理
- `EASYXT_SOURCE_CONFLICT_DELTA`: 多源收盘价冲突阈值，默认 `0.02`（2%）。
- `EASYXT_CHART_FETCH_TIMEOUT_S`: 图表在线拉取超时（秒）。
- `EASYXT_RT_EVENT_WATERMARK_S`: 实时事件时间 watermark（秒），默认 `120`。
- `EASYXT_RT_DROP_OOO_SEQUENCE`: `1/0`，是否丢弃乱序 sequence 事件。
- `EASYXT_WM_WEIGHT_LATE`: Q-score 的 Late 子分权重，默认 `0.45`。
- `EASYXT_WM_WEIGHT_OOO`: Q-score 的 OOO 子分权重，默认 `0.35`。
- `EASYXT_WM_WEIGHT_LATENESS`: Q-score 的 Lateness 子分权重，默认 `0.20`。
- `EASYXT_WM_PROFILE`: Q-score 模板，`balanced/conservative/aggressive`，默认 `balanced`。
- 模板建议：
  - `balanced`: `late=0.45, ooo=0.35, lateness=0.20, floor=0.97, lookback=7`
  - `conservative`: `late=0.50, ooo=0.35, lateness=0.15, floor=0.985, lookback=14`
  - `aggressive`: `late=0.40, ooo=0.30, lateness=0.30, floor=0.95, lookback=7`
- `EASYXT_WM_QSCORE_FLOOR`: Q-score 发布阈值，默认 `0.97`。
- `EASYXT_WM_LOOKBACK_DAYS`: Q-score 趋势统计天数，默认 `7`。
- `EASYXT_WM_APPROVAL_REQUIRED_PROFILES`: 生产环境需审批模板列表（逗号分隔），默认 `aggressive`。
- `EASYXT_WM_APPROVAL_ID`: 模板切换审批单号（当命中需审批模板时必填）。
- `EASYXT_WM_APPROVER`: 模板切换审批人（当命中需审批模板时必填）。
- `EASYXT_WM_APPROVAL_REGISTRY_PATH`: 审批白名单文件路径，默认 `artifacts/watermark_approval_registry.json`。
- `EASYXT_WM_APPROVAL_MAX_AGE_DAYS`: 审批单最大有效天数，默认 `7`。
- `EASYXT_WM_APPROVAL_REQUIRE_SIGNATURE`: `1/0`，是否强制审批签名校验，默认 `1`。
- `EASYXT_WM_APPROVAL_MULTISIG_THRESHOLD`: 审批多签门槛 M，默认 `1`。
- `EASYXT_WM_APPROVAL_SIGNERS`: 审批签署人白名单（逗号分隔，可选）。
- `EASYXT_WM_APPROVAL_EXPIRY_WARN_DAYS`: 审批过期预警天数，默认 `2`。
- `EASYXT_WM_APPROVAL_USAGE_WARN_RATIO`: 审批用量预警比例，默认 `0.8`。
- `EASYXT_WM_APPROVAL_WARN_BLOCK`: `1/0`，审批预警是否阻断发布，默认 `0`。
- `EASYXT_WM_APPROVAL_USAGE_LOG_PATH`: 审批使用日志路径，默认 `artifacts/watermark_approval_usage.jsonl`。
- `EASYXT_WM_APPROVAL_SIGNING_KEY`: 审批签名密钥（HMAC-SHA256，建议仅在 CI Secret 配置）。
- `EASYXT_SESSION_PROFILE`: 周期构建会话模板，支持 `CN_A` / `CN_A_AUCTION` / `FUTURES_COMMODITY`，默认 `CN_A`。
- `EASYXT_SESSION_PROFILE_FILE`: 周期会话模板 JSON 文件路径，默认 `config/session_profiles.json`。
- `EASYXT_SESSION_PROFILE_RULES_FILE`: 标的到会话模板映射规则文件路径，默认 `config/session_profile_rules.json`。
- `EASYXT_PERIOD_ALIGNMENT`: 周期对齐方式，默认 `left`。
- `EASYXT_PERIOD_ANCHOR`: 日内收敛锚点，支持 `daily_close` / `none`，默认 `daily_close`。
- `EASYXT_PERIOD_VALIDATION_REPORT_PATH`: 周期交叉校验报告 JSONL 路径，默认 `artifacts/period_validation_report.jsonl`。
- `EASYXT_PERIOD_VALIDATION_FAIL_BLOCK`: `1/0`，周期校验报告出现失败时是否阻断 P0，默认 `1`。
- `EASYXT_PEAK_MAX_PERIOD_VALIDATION_FAILED_ITEMS`: 峰值发布门禁允许的周期失败行数上限，默认 `0`。
- 审批白名单样例文件：`config/watermark_approval_registry.example.json`。

## 接口服务
- `EASYXT_API_PORT`: 本地 API/WS 端口，默认 `8000`。

## GUI 连接探测
- `EASYXT_ENABLE_ACTIVE_PROBE`: `1/0`，是否启用 GUI 连接主动探测，默认 `0`（被动探测）。
- `EASYXT_CONNECTION_PROBE_MODE`: 主动探测模式，支持 `active` / `safe` / `passive`，默认 `passive`。
- `EASYXT_ENABLE_QMT_ONLINE`: `1/0`，GUI 默认 `0`（优先稳定性，避免启动阶段触发 QMT 在线链路）。
- `EASYXT_ENABLE_BROKER_WARMUP`: `1/0`，连接成功后是否后台预热 xtquant broker，默认 `0`。
- `EASYXT_HEALTH_IMPORT_EASYXT`: `1/0`，健康检查是否主动导入 `easy_xt`，默认 `0`（仅缓存检测）。
- `EASYXT_ENABLE_QTWEBENGINE_SAFE_CACHE`: `1/0`，是否启用 QtWebEngine 安全缓存目录自动切换，默认 `1`。
- `EASYXT_QTWEBENGINE_CACHE_ROOT`: 自定义 QtWebEngine 安全缓存根目录（需可写）。
- `EASYXT_QTWEBENGINE_DISABLE_GPUCACHE`: `1/0`，是否禁用 GPU 程序/着色器磁盘缓存，默认 `1`。
- `EASYXT_ENABLE_PROBE_SUBPROCESS_ISOLATION`: `1/0`，连接探测是否启用子进程隔离，默认 `1`。
- `EASYXT_PROBE_SUBPROCESS_TIMEOUT_S`: 子进程探测超时秒数，默认 `6`。
- `EASYXT_PROBE_SYMBOLS`: 子进程探测标的列表（逗号分隔），默认 `000001.SZ,511090.SH`。
- `EASYXT_RT_FUSE_FAIL_THRESHOLD`: 实时链路失败熔断触发阈值（连续失败次数），默认 `8`。
- `EASYXT_RT_FUSE_COOLDOWN_S`: 实时链路熔断冷却秒数，默认 `45`。
- `EASYXT_RT_FUSE_LOG_INTERVAL_S`: 熔断期日志最小间隔秒数，默认 `10`。
- `EASYXT_ENABLE_AUTO_CHECKPOINT`: `1/0`，DuckDB 自动 checkpoint 后台线程开关；GUI 默认 `0`。
- `EASYXT_CHECKPOINT_ON_PROCESS_EXIT`: `1/0`，进程退出时是否执行 checkpoint，GUI 默认 `0`。
- `EASYXT_CHECKPOINT_NONBLOCKING`: `1/0`，checkpoint 非阻塞写锁模式，默认 `1`。
- `EASYXT_CHECKPOINT_SKIP_WHEN_BUSY`: `1/0`，有活跃连接时跳过 checkpoint，默认 `1`。
