# EasyXT 开发规则参考文档

## 项目架构

```
EasyXT = 
  PyQt5 GUI + lightweight-charts图表 + QMT(xtquant)数据/交易 + DuckDB本地数据库
```

## 参考项目

1. **EasyXT**: https://github.com/quant-king299/EasyXT
2. **lightweight-charts**: https://github.com/TriangleTraders/lightweight-charts-python
3. **QMT xtdata**: https://dict.thinktrader.net/nativeApi/xtdata.html?id=5M2071#%E6%8E%A5%E5%8F%A3%E6%A6%82%E8%BF%B0

---

## 刚性开发规则

### 规则1: 数据获取必须走QMT

**xtdata使用原则**:
- `download_history_data` - 下载历史数据到本地（必须先下载）
- `get_market_data` - 从本地缓存获取数据
- `subscribe_quote` - 订阅实时数据

**正确流程**:
```python
# 1. 先下载（如果本地没有）
xtdata.download_history_data("000001.SZ", "1d", "20240101", "20241231")

# 2. 再获取
data = xtdata.get_market_data(["000001.SZ"], period="1d")
```

### 规则2: 图表渲染层使用lightweight-charts

**K线渲染**:
- 使用PyQtWebEngineView加载lightweight-charts
- 数据格式转换为JSON后发送到前端

### 规则3: 本地数据存DuckDB

**用途**:
- 历史数据缓存
- 财务数据存储
- 回测结果存储

---

## 常见问题

### 问题1: 数据获取卡顿

**原因**: 在主线程同步调用QMT接口

**解决方案**:
- 异步下载: `download_history_data2` (批量版本)
- 后台线程: 使用QThread
- 增量下载: `incrementally=True`

### 问题2: K线图表卡顿

**原因**: 
- 数据量大 - 解决方案：分页加载
- 频繁刷新 - 解决方案：节流(throttle)
- WebEngine频繁重建 - 解决方案：复用实例

### 问题3: DuckDB查询慢

**解决方案**:
- 添加索引
- 使用连接池
- 分页查询

---

## 性能优化 Checklist

- [ ] 数据下载在后台线程执行
- [ ] 图表数据分页加载（先显示前1000条）
- [ ] 图表实例复用，不重复创建
- [ ] 实时数据节流刷新（最大1次/秒）
- [ ] DuckDB查询使用连接池
- [ ] 大数据表格使用虚拟化(QTableView)

---

## 运营协议（Stage 1 实样本周）

### 协议1：每日运行日志一行制

每日 batch 结束后，固定记录一行到 `artifacts/run_log.txt`：

```text
{date} | run_id={GITHUB_RUN_ID} | commit={GITHUB_SHA[:7]} | generator={diff_report.generator} | pass_rate={x.xx} | med_sharpe={x.xx}
```

执行要求：
- `run_id` 使用 CI 环境变量 `GITHUB_RUN_ID`
- `commit` 使用 `GITHUB_SHA` 前 7 位
- `generator` 取自 `diff_report_*.json` 的 `generator` 字段
- 不新增脚本，人工复制一行即可

### 协议2：周末阈值决策树（仅阈值微调）

每周复盘仅允许调整 `stage1_universe.yaml` 的 `thresholds`，不改告警链路与并发链路。

决策规则：
- 若 `state_changes[]` 中新增 `CRITICAL > 0`：仅允许收紧阈值，不允许扩标的池
- 若 `accessibility_pct` 连续 2 天低于组阈值：优先排查数据源，不先改策略
- 若 `tradeability_fail_count` 突增：先校准滑点/成交约束，再考虑放宽阈值
- 若 `median_sharpe` 日波动 > 0.3：先提高样本过滤（如 `min_trading_days`），再做参数微调

审计要求：
- 任何阈值修改必须附 `issue_ref`
- PR 自动触发 freeze check，未满足规则不予放行
