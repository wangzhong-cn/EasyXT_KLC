"""时间戳契约层（Timestamp Contract Layer）
=========================================
全系统统一时间戳处理规范 —— 四层契约：

┌──────────────┬──────────────────────────────────────────────────────┐
│ 层次         │ 规范                                                   │
├──────────────┼──────────────────────────────────────────────────────┤
│ 输入层       │ UTC epoch ms（QMT API）/ UTC epoch s（DAT 二进制）     │
│              │ 或 Asia/Shanghai 字符串（AKShare / CSV）               │
│ 存储层       │ DuckDB 中以北京时间 naive Timestamp 存储               │
│              │ （即 Timestamp('2024-01-02 09:30:00')，无 tzinfo）     │
│ 计算层       │ pd.Timestamp naive，代表北京时间，参与日期归属 / 排序   │
│ 展示层       │ strftime("%Y-%m-%d %H:%M:%S")，北京时间字符串          │
└──────────────┴──────────────────────────────────────────────────────┘

背景（v4 DAT 验证实证，5597 品种）：
  QMT DAT 文件存储 UTC epoch seconds：
    日线 DAT time 字段 = 当天 00:00 CST ≡ 前一天 16:00 UTC。
    若直接 pd.to_datetime(ts, unit='s') → UTC naive → 日期归属到"昨天"。
    必须先 +28800（UTC+8 偏移秒数）才转 Timestamp，才能得到正确的北京日期。

  QMT API (get_market_data_ex) 的 time 列 = UTC epoch milliseconds：
    2024-01-02 09:30:00 CST = 2024-01-02 01:30:00 UTC = 1704138600000 ms
    直接 pd.to_datetime(unit="ms") → Timestamp('2024-01-02 01:30:00')（时间错8h）
    需 +28800000 ms 才得到正确北京时间 Timestamp('2024-01-02 09:30:00')。

  修改本文件必须同步更新：
    - data_manager/dat_binary_reader.py（导入 dat_s_to_beijing）
    - data_manager/unified_data_interface.py（导入 qmt_ms_to_beijing）
    - tests/test_timestamp_utils.py（P0 验收用例）
"""

from __future__ import annotations

from typing import Union

import numpy as np
import pandas as pd

# ── 契约常量（固定，修改需全系统同步） ──────────────────────────────────────────
UTC8_OFFSET_S: int = 28_800          # 8 小时，单位：秒
UTC8_OFFSET_MS: int = 28_800_000     # 8 小时，单位：毫秒

# A 股市场最早上市日期（1990-12-19 上证开市）；任何早于此日期的时间戳均为错误数据
# Unix epoch 0 → +8h → 1970-01-01 08:00，必须过滤掉
_MIN_VALID_DATE: pd.Timestamp = pd.Timestamp("1990-01-01")
_MIN_VALID_MS: int = int(_MIN_VALID_DATE.timestamp() * 1000)  # epoch ms 下界


# ── 核心转换函数 ─────────────────────────────────────────────────────────────

def qmt_ms_to_beijing(ts_series: "pd.Series") -> "pd.Series":
    """QMT API 毫秒时间戳 → 北京时间 naive Timestamp Series。

    QMT ``get_market_data_ex()`` 的 ``time`` 列为 UTC epoch milliseconds。
    本函数加 UTC+8 偏移后转 pd.Timestamp，返回可直接用于 DuckDB 存储
    或日期比较的北京时间序列（无 tzinfo）。

    Args:
        ts_series: 整数毫秒 epoch Series（QMT time 列）。

    Returns:
        pd.Series[pd.Timestamp]，北京时间，naive（无 tzinfo）。

    示例::

        ts = pd.Series([1704138600000])   # 2024-01-02 01:30:00 UTC
        qmt_ms_to_beijing(ts)             # → Timestamp('2024-01-02 09:30:00')
    """
    result = pd.to_datetime(
        ts_series.astype(np.int64) + UTC8_OFFSET_MS, unit="ms", errors="coerce"
    )
    # 过滤 Unix epoch 零值及早于 A 股开市的无效时间戳（防止 1970-01-01 污染 DuckDB）
    result = result.where(result >= _MIN_VALID_DATE, other=pd.NaT)
    return result


def dat_s_to_beijing(
    ts_array: Union["np.ndarray", "pd.Series"]
) -> "pd.DatetimeIndex":
    """DAT 文件秒时间戳 → 北京时间 DatetimeIndex（全系统唯一授权来源）。

    QMT DAT 二进制文件存储 UTC epoch seconds。
    +28800 秒 → 北京时间 naive Timestamp。

    此函数是全系统 DAT 时间戳转换的**唯一授权来源**。
    ``dat_binary_reader.py`` 必须调用本函数，不得自行硬编码 ``+28800``。

    Args:
        ts_array: uint32 / int64 数组，UTC epoch seconds（来自 DAT 文件）。

    Returns:
        pd.DatetimeIndex，北京时间，naive（无 tzinfo）。

    示例::

        ts = np.array([1704124800], dtype=np.uint32)  # 2024-01-01 16:00 UTC
        dat_s_to_beijing(ts)                           # → Timestamp('2024-01-02 00:00:00')
    """
    result = pd.to_datetime(
        np.asarray(ts_array, dtype=np.int64) + UTC8_OFFSET_S, unit="s", errors="coerce"
    )
    # 过滤 Unix epoch 零值及早于 A 股开市的无效时间戳（防止 1970-01-01 污染 DuckDB）
    # 注意：保持 DatetimeIndex 长度不变，将无效条目置为 NaT，供上层调用方过滤
    result = result.where(result >= _MIN_VALID_DATE, other=pd.NaT)  # type: ignore[union-attr]
    return result  # type: ignore[return-value]


def assert_no_tz(ts: "pd.Timestamp", label: str = "") -> None:
    """断言 Timestamp 是 naive（无时区），用于数据管道入口的契约检查。

    捕获隐式"把 UTC aware Timestamp 当北京时间使用"的错误。
    在关键数据入口处调用；生产代码不应在热路径中使用（阻塞式断言）。

    Args:
        ts:    待检查的 Timestamp。
        label: 调用方标识，用于错误消息定位。

    Raises:
        ValueError: 当 ts 含 tzinfo 时。
    """
    if ts is pd.NaT:
        return
    if ts.tzinfo is not None:
        raise ValueError(
            f"[时间戳契约违反] {label!r} 应为 naive 北京时间，"
            f"实际含时区信息：tzinfo={ts.tzinfo}"
        )
