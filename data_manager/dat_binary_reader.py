"""DAT 二进制直读器（生产版）
=====================================
将 tools/_ultimate_crossval_v4.py 中 v4 全量验证（5597 品种，100% 格式一致）
确认过的 read_dat_fast() 提升为生产组件。

完全不依赖 xtquant / miniquote——任何 Python 版本均可运行。

技术规格（v4 验证确认）：

    文件头：    8 字节（跳过）
    记录大小：  64 字节 / 条
    字段布局：
        [0-3]   uint32  timestamp（UTC epoch seconds）
        [4-7]   uint32  open  × 1000
        [8-11]  uint32  high  × 1000
        [12-15] uint32  low   × 1000
        [16-19] uint32  close × 1000
        [20-23] uint32  padding（始终为 0）
        [24-27] uint32  volume（手 / lots）
        [28-63] metadata（preClose, openInterest 等，跳过）
    时区修正：  ts + 28800 → 北京时间（UTC+8），确保日期归属正确

路径规则：
    {qmt_base}/datadir/{MARKET}/{PERIOD_SECS}/{CODE}.DAT
    市场代码：SH / SZ / SF / DF / IF / ZF / HK
    周期秒数：60=1m / 300=5m / 86400=1d

使用示例：
    from data_manager.dat_binary_reader import read_dat, DATBinaryReader

    # 函数式接口
    df = read_dat("600519.SH", period="1d", start_date="2023-01-01")

    # 类接口（用于 DataSourceRegistry）
    reader = DATBinaryReader()
    df = reader.get_data("600519.SH", "2023-01-01", "2024-12-31", "1d", "none")
"""

import json
import logging
import struct
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# 导入全系统唯一授权 DAT 时间戳转换函数
from data_manager.timestamp_utils import (  # noqa: E402
    UTC8_OFFSET_S,
    dat_s_to_beijing,
)

_logger = logging.getLogger(__name__)

# ─── 格式常量（v4 验证确认） ───────────────────────────────────────────────────
HEADER_SIZE  = 8
RECORD_SIZE  = 64
MIN_TIMESTAMP = 1262304000   # 2010-01-01 00:00:00 UTC
MAX_TIMESTAMP = 1893456000   # 2030-01-01 00:00:00 UTC
UTC8_OFFSET   = 28800        # 秒，UTC+8

# 周期名 → 秒数
PERIOD_SECONDS: dict[str, int] = {
    "1m":  60,
    "5m":  300,
    "1d":  86400,
}

# ─── 路径工具 ─────────────────────────────────────────────────────────────────

def _load_qmt_base_from_config() -> Optional[Path]:
    """从 config/unified_config.json 读取 QMT 数据目录。失败时静默返回 None。"""
    try:
        config_path = (
            Path(__file__).resolve().parents[1] / "config" / "unified_config.json"
        )
        if not config_path.exists():
            return None
        with config_path.open("r", encoding="utf-8") as fh:
            cfg = json.load(fh)
        raw = (
            cfg.get("environment", {}).get("qmt_userdata_path")
            or cfg.get("settings", {}).get("account", {}).get("qmt_userdata_path")
        )
        if not raw:
            qmt_path = cfg.get("settings", {}).get("account", {}).get("qmt_path")
            if qmt_path:
                qmt_path_str = str(qmt_path)
                if qmt_path_str.lower().endswith(".exe"):
                    raw = str(Path(qmt_path_str).parent.parent / "userdata_mini")
                else:
                    raw = str(Path(qmt_path_str) / "userdata_mini")
        if not raw:
            return None
        base = Path(raw) / "datadir"
        return base if base.exists() else None
    except Exception as exc:
        _logger.debug("读取 QMT 基础路径失败（忽略）: %s", exc)
        return None


def _symbol_to_market_code(symbol: str) -> tuple[str, str]:
    """
    标准代码 → (market, code)

    "600519.SH" → ("SH", "600519")
    "rb2510.SF" → ("SF", "rb2510")
    "600519"    → ("SH", "600519")   （无后缀按首字符推断）
    """
    if "." in symbol:
        code, suffix = symbol.rsplit(".", 1)
        return suffix.upper(), code
    # 无后缀兜底：首字符 6 → SH，其余 → SZ
    return ("SH" if symbol.startswith("6") else "SZ"), symbol


def _build_dat_path(qmt_base: Path, symbol: str, period: str) -> Optional[Path]:
    """构建 DAT 文件绝对路径，文件不存在时返回 None。"""
    period_secs = PERIOD_SECONDS.get(period)
    if period_secs is None:
        _logger.debug("不支持的周期: %s", period)
        return None
    market, code = _symbol_to_market_code(symbol)
    dat_path = qmt_base / market / str(period_secs) / f"{code}.DAT"
    return dat_path if dat_path.exists() else None


# ─── 核心读取 ─────────────────────────────────────────────────────────────────

def _read_dat_numpy(dat_path: Path) -> pd.DataFrame:
    """
    numpy 向量化读取单个 DAT 文件。

    返回 DataFrame（index: 北京时间 Timestamp，name="date"）
            columns: open  high  low  close  volume
    空文件 / 读取异常时返回空 DataFrame。
    """
    try:
        fsize = dat_path.stat().st_size
    except OSError as exc:
        _logger.warning("DAT 文件无法访问: %s — %s", dat_path, exc)
        return pd.DataFrame()

    if fsize <= HEADER_SIZE:
        return pd.DataFrame()

    n_records = (fsize - HEADER_SIZE) // RECORD_SIZE
    if n_records < 1:
        return pd.DataFrame()

    try:
        with dat_path.open("rb") as fh:
            fh.seek(HEADER_SIZE)
            raw = fh.read(n_records * RECORD_SIZE)
    except OSError as exc:
        _logger.warning("DAT 文件读取失败: %s — %s", dat_path, exc)
        return pd.DataFrame()

    # numpy 结构化 dtype
    dt = np.dtype([
        ("ts",     "<u4"),
        ("open",   "<u4"),
        ("high",   "<u4"),
        ("low",    "<u4"),
        ("close",  "<u4"),
        ("pad",    "<u4"),
        ("volume", "<u4"),
        ("rest",   "V36"),   # metadata 字段，暂不使用
    ])

    try:
        arr = np.frombuffer(raw[: n_records * RECORD_SIZE], dtype=dt)
    except ValueError as exc:
        _logger.warning("DAT 解析失败（格式错误）: %s — %s", dat_path, exc)
        return pd.DataFrame()

    # 过滤：时间范围有效 + 成交量 > 0（剔除停牌空记录）
    mask = (
        (arr["ts"] >= MIN_TIMESTAMP) &
        (arr["ts"] <= MAX_TIMESTAMP) &
        (arr["volume"] > 0)
    )
    valid = arr[mask]

    if len(valid) == 0:
        return pd.DataFrame()

    # === 关键步骤：+28800 将 UTC epoch 转为北京时间 ===
    # 背景：QMT DAT 存储 UTC epoch，若不加偏移量则 16:00 UTC = 北京 00:00，
    #       导致日内数据归属到前一天（v4 验证已实证此问题）
    # 调用 timestamp_utils.dat_s_to_beijing（全系统唯一授权来源，不可绕过）
    timestamps = dat_s_to_beijing(valid["ts"])

    df = pd.DataFrame(
        {
            "open":   valid["open"].astype(np.float64)   / 1000,
            "high":   valid["high"].astype(np.float64)   / 1000,
            "low":    valid["low"].astype(np.float64)    / 1000,
            "close":  valid["close"].astype(np.float64)  / 1000,
            "volume": valid["volume"].astype(np.int64),
        },
        index=timestamps,
    )
    df.index.name = "date"
    # 去重（保留最后一条，以实践中合约替换场景为准）
    df = df[~df.index.duplicated(keep="last")]
    return df.sort_index()


# ─── 公开函数式接口 ────────────────────────────────────────────────────────────

def read_dat(
    symbol: str,
    period: str = "1d",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    qmt_base: Optional[Path] = None,
) -> pd.DataFrame:
    """
    从 QMT DAT 文件直读指定品种数据。

    不依赖 xtquant / miniquote，直接以 numpy 解码二进制文件。
    适用于任何 Python 版本（无 .pyd 版本约束）。

    Args:
        symbol:     标准代码，如 "600519.SH"、"rb2510.SF"
        period:     "1m" | "5m" | "1d"
        start_date: "20200101" 或 "2020-01-01"（含当日）
        end_date:   "20261231" 或 "2026-12-31"（含当日）
        qmt_base:   覆盖默认路径（测试 / 自定义路径用）

    Returns:
        DataFrame（index=北京时间 Timestamp，columns=[open,high,low,close,volume]）
        文件不存在或无有效数据时返回空 DataFrame。
    """
    _base = qmt_base if qmt_base is not None else _load_qmt_base_from_config()
    if _base is None:
        _logger.debug("QMT 数据目录不可用，跳过 DAT 直读: %s", symbol)
        return pd.DataFrame()

    dat_path = _build_dat_path(_base, symbol, period)
    if dat_path is None:
        _logger.debug("DAT 文件不存在: %s  period=%s", symbol, period)
        return pd.DataFrame()

    df = _read_dat_numpy(dat_path)
    if df.empty:
        return df

    # 日期过滤
    if start_date:
        sd = pd.to_datetime(start_date, errors="coerce")
        if sd is not pd.NaT:
            df = df[df.index >= sd]
    if end_date:
        ed = pd.to_datetime(end_date, errors="coerce")
        if ed is not pd.NaT:
            df = df[df.index <= ed]

    return df


# ─── 面向 DataSourceRegistry 的类接口 ─────────────────────────────────────────

class DATBinaryReader:
    """
    面向 DataSourceRegistry 的 DAT 直读适配器。

    注册方式::

        from data_manager.dat_binary_reader import DATBinaryReader
        registry.register("dat", DATBinaryReader())

    注意：
        - DAT 文件存储的是未复权价格；adjust 参数当前被忽略。
        - 数据可用性取决于本地 QMT 安装路径是否配置正确。
    """

    def __init__(self, qmt_base: Optional[Path] = None):
        self._qmt_base: Optional[Path] = (
            qmt_base if qmt_base is not None else _load_qmt_base_from_config()
        )

    def is_available(self) -> bool:
        """QMT 数据目录是否可访问"""
        return self._qmt_base is not None and Path(self._qmt_base).exists()

    def get_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str = "1d",
        adjust: str = "none",
    ) -> pd.DataFrame:
        """
        实现与 DataSource.get_data() 相同签名，可直接注册到 DataSourceRegistry。

        adjust 参数在 DAT 直读中暂不支持（返回未复权原始数据）。
        """
        return read_dat(symbol, period, start_date, end_date, self._qmt_base)

    def health(self) -> dict:
        """返回可读性健康状态，供监控 / 日志使用"""
        return {
            "name":      "dat_binary",
            "available": self.is_available(),
            "qmt_base":  str(self._qmt_base) if self._qmt_base else None,
        }
