# Phase 1 核心缺口实施单（代码级）

## 目标
- 补齐执行层算法能力（先 TWAP）
- 为 Tick 回测与多资产交易留出接口位
- 固化可复现验证命令，防止终端历史污染

## 任务拆分

### P1-A TWAP 执行器
- 新增 `easy_xt/execution_algorithms.py`
  - `TwapPlan(total_volume, slices, min_lot).build()`
- 新增 `ExtendedAPI.execute_twap(...)`
  - 参数：`account_id/code/side/total_volume/slices/interval_sec/price/price_type/min_lot/dry_run`
  - 输出：`planned_volumes/order_ids/submitted_volumes/feasible/message`
  - 失败子单记录 warning，不阻断后续子单
- 新增 `ExtendedAPI.execute_vwap(...)`
  - 参数：`account_id/code/side/total_volume/volume_profile/interval_sec/price/price_type/min_lot/dry_run`
  - 行为：按成交量分布切片；分布不可用时自动回退 TWAP
  - 输出：`fallback_to_twap/planned_volumes/order_ids`

### P1-B Tick 回测（进行中）
- `easyxt_backtest` 新增 `TickEvent`
- `BacktestEngine.run(..., period="tick")` 分支
- 滑点/延迟模型与成交撮合规则
- 当前已完成最小闭环：`period="tick"` 时调用 `strategy.on_tick(...)`，并支持 `price/last/last_price` 字段成交
- 已新增 tick 专属参数：
  - `tick_slippage_bps`（tick路径滑点，单位bp）
  - `tick_latency_ticks`（tick路径延迟成交tick数）
  - `tick_use_orderbook`（按 `bid1/ask1` 进行可成交性约束撮合）
  - `tick_participation_rate`（单tick参与率上限，支持部分成交与排队续撮合）
  - `tick_max_wait_ticks`（订单最长等待tick数，超时自动撤单）
  - `tick_cancel_retry_max`（撤单后最大重挂次数）
  - `tick_cancel_retry_price_bps`（每次重挂限价调整bp）
- 队列撮合优先级（tick）：
  - 价格优先：买单高价优先、卖单低价优先
  - 时间优先：同价按入队先后成交
  - 同tick盘口容量逐单消耗：`ask1_vol/bid1_vol` 不可被重复占用
  - 多档盘口（L1~L5）跨档成交：按档位成交量加权计算成交价

### P1-C 多资产交易（下一步）
- 新增资产类型抽象：`asset_type`（stock/future/option/convertible）
- `TradeAPI` 抽象统一下单接口，先打通期货最小闭环

## 验证命令
- 单测子集：
  - `pytest tests/test_twap_execution.py -q`
  - `pytest tests/test_vwap_execution.py -q`
- 组合相关：
  - `powershell -ExecutionPolicy Bypass -File tools/run_optimizer_ci_subset.ps1`

## 验收口径
- TWAP：能稳定拆单、支持 dry_run、无效参数显式拒绝
- 回测：新增能力不破坏现有 bar 回测链路
- 交易：新增资产类型不影响现有 A 股路径
