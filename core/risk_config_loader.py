"""
风控配置加载器 —— 将 JSON 配置映射到 RiskEngine

从配置文件（production_template.json / 自定义 JSON）读取风控阈值，
创建 ``RiskEngine`` 实例。支持兼容旧 ``settings.risk`` 字段名。

用法::

    from core.risk_config_loader import load_risk_engine

    # 从配置文件加载
    engine = load_risk_engine("config/production_template.json")

    # 从字典加载
    engine = load_risk_engine({"concentration_limit": 0.25, "intraday_drawdown_halt": 0.04})
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union

from core.risk_engine import RiskEngine, RiskThresholds

log = logging.getLogger(__name__)

# 旧配置字段到新 RiskThresholds 字段的映射
_LEGACY_KEY_MAP: Dict[str, str] = {
    "max_position_ratio": "concentration_limit",
    "stop_loss_ratio": "intraday_drawdown_warn",
    "max_total_exposure": "net_exposure_limit",
}


def _normalize_risk_dict(raw: Dict[str, Any]) -> Dict[str, Any]:
    """将旧版配置字段名映射到 RiskThresholds 字段名。"""
    result: Dict[str, Any] = {}
    for key, value in raw.items():
        mapped = _LEGACY_KEY_MAP.get(key, key)
        if mapped in RiskThresholds.__dataclass_fields__:
            result[mapped] = float(value)
        elif key not in ("blacklist",):  # blacklist 不是阈值参数
            log.debug("忽略未知风控配置键: %s", key)
    return result


def load_thresholds(
    source: Union[str, Path, Dict[str, Any]],
) -> RiskThresholds:
    """
    从 JSON 文件路径或字典加载 ``RiskThresholds``。

    Parameters
    ----------
    source
        - 文件路径（str / Path）→ 读取 JSON，取 ``settings.risk`` 或根级别
        - 字典 → 直接解析

    Returns
    -------
    RiskThresholds
        解析后的阈值实例。字段缺失时使用默认值。
    """
    if isinstance(source, (str, Path)):
        path = Path(source)
        if not path.exists():
            log.warning("风控配置文件不存在: %s，使用默认阈值", path)
            return RiskThresholds()
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        # 支持嵌套和扁平两种格式
        risk_section = data.get("settings", {}).get("risk", data.get("risk", data))
    else:
        risk_section = source

    if not isinstance(risk_section, dict):
        log.warning("风控配置格式异常，使用默认阈值")
        return RiskThresholds()

    normalized = _normalize_risk_dict(risk_section)
    return RiskThresholds(**normalized)


def load_risk_engine(
    source: Union[str, Path, Dict[str, Any], None] = None,
) -> RiskEngine:
    """
    创建并返回配置好的 ``RiskEngine`` 实例。

    Parameters
    ----------
    source
        配置来源（文件路径、字典、或 None）。
        None 时使用默认阈值。

    Returns
    -------
    RiskEngine
        已配置阈值的风控引擎实例。
    """
    if source is None:
        return RiskEngine()
    thresholds = load_thresholds(source)
    log.info("风控引擎加载配置: %s", thresholds)
    return RiskEngine(thresholds=thresholds)
