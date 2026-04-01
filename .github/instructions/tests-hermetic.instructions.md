---
description: "Use when writing or updating pytest tests, fixtures, mocks, coverage checks, regression subsets, hermetic tests, or any file under tests/. Covers market-data red lines, conftest isolation, pytest markers, and Windows test execution in EasyXT_KLC."
name: "EasyXT Hermetic Test Guidelines"
applyTo: "tests/**/*.py"
---
# EasyXT Hermetic Test Guidelines

- 先看 `development_rules.md` 与 `tests/conftest.py`；测试规则以这两处为准，避免在测试里重新发明隔离策略。
- **严禁伪造市场数据。** 不要硬编码 OHLCV、成交量、收益率或用随机数伪装行情；需要业务数据时，优先使用 `tests/fixtures/real_market_data.py` 或真实空数据场景。
- 允许 mock 白线基础设施（模块可用性、网络 I/O、文件 I/O、环境变量、时间控制、数据库连接对象）；如果 `return_value` 含有业务行情内容，必须来自真实 fixture，而不是手写 DataFrame。
- 避免用 `patch("builtins.__import__", ...)` 拦截 `xtquant`；要模拟缺模块，优先用 `patch.dict("sys.modules", {"xtquant": None, "xtquant.xtdata": None})`。
- 构造 `UnifiedDataInterface` 的测试替身时，显式固定数据源状态，避免误探测本机环境：`qmt_available/_qmt_checked`、`tushare_available/_tushare_checked`、`akshare_available/_akshare_checked`。
- `tests/conftest.py` 会话级默认关闭自动 checkpoint、QMT 在线模式和 WS 行情，并清理 `data_manager`/线程污染；不要绕过这些保护，除非测试明确只在局部用 `monkeypatch` 恢复。
- 新测试优先做与改动路径强相关的回归，合理使用 `pytest.ini` 中的 marker；`integration` 测试默认跳过，只有在明确需要外部系统时才配合 `--run-integration`。
- Windows 下运行测试优先使用 conda 环境；当任务涉及 pytest、coverage、回归子集或失败分析时，优先触发 `test-coverage` skill。
- 若测试失败表现为进程崩溃、无输出退出、`bsonobj.cpp`、`access violation` 或 DLL 问题，优先切换到 `xtquant-crash-debug` skill，而不是继续加随机 sleep 或放宽断言。
