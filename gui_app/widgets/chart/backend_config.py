"""
backend_config.py — 图表后端灰度开关配置中心

功能：
  - 从 config/unified_config.json 的 chart.engine 节点读取配置
  - 支持全局后端设置 + 按账户/策略白名单灰度
  - 支持交易时段变更冻结
  - 提供单例 ChartBackendConfig，供 create_chart_adapter 调用

配置示例（写入 unified_config.json 的 chart 键）::

    "chart": {
        "engine": {
            "default_backend": "klinechart",
            "native_lwc_whitelist": {
                "accounts": ["demo_001", "paper_test"],
                "strategies": ["grid_v2", "momentum_alpha"]
            },
            "freeze_during_trading": true,
            "ws_handshake_timeout_s": 5.0
        }
    }

环境变量优先级（高→低）：
  EASYXT_CHART_BACKEND → whitelist 匹配 → default_backend
"""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# 项目根目录
_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _ROOT / "config" / "unified_config.json"
_LOCK = threading.Lock()
_INSTANCE: ChartBackendConfig | None = None


class ChartBackendConfig:
    """
    图表后端灰度配置，线程安全单例。

    get_backend(account_id, strategy_id) → "lwc_python" | "native_lwc" | "klinechart"
    can_switch_now()                     → (bool, reason: str)
    """

    # 默认值（当配置文件缺失对应键时使用）
    _DEFAULTS: dict[str, Any] = {
        "default_backend": "klinechart",
        "native_lwc_whitelist": {"accounts": [], "strategies": []},
        "klinechart_whitelist": {"accounts": [], "strategies": []},
        "freeze_during_trading": True,
        "ws_handshake_timeout_s": 5.0,
    }

    def __init__(self, cfg: dict[str, Any]) -> None:
        self._default = str(cfg.get("default_backend", self._DEFAULTS["default_backend"]))
        wl = cfg.get("native_lwc_whitelist") or {}
        wl_kline = cfg.get("klinechart_whitelist") or {}
        self._wl_accounts: frozenset[str] = frozenset(
            str(a).strip().lower() for a in (wl.get("accounts") or [])
        )
        self._wl_strategies: frozenset[str] = frozenset(
            str(s).strip().lower() for s in (wl.get("strategies") or [])
        )
        self._wl_kline_accounts: frozenset[str] = frozenset(
            str(a).strip().lower() for a in (wl_kline.get("accounts") or [])
        )
        self._wl_kline_strategies: frozenset[str] = frozenset(
            str(s).strip().lower() for s in (wl_kline.get("strategies") or [])
        )
        self._freeze_trading: bool = bool(
            cfg.get("freeze_during_trading", self._DEFAULTS["freeze_during_trading"])
        )
        self.ws_handshake_timeout_s: float = float(
            cfg.get("ws_handshake_timeout_s", self._DEFAULTS["ws_handshake_timeout_s"])
        )
        log.debug(
            "ChartBackendConfig loaded: default=%s wl_accounts=%d wl_strategies=%d freeze=%s",
            self._default,
            len(self._wl_accounts),
            len(self._wl_strategies),
            self._freeze_trading,
        )

    # ── 主接口 ──────────────────────────────────────────────────────────────

    def get_backend(
        self,
        account_id: str | None = None,
        strategy_id: str | None = None,
    ) -> str:
        """
        返回应使用的后端名称。

        优先级：
          1. 环境变量 EASYXT_CHART_BACKEND（运维临时覆盖）
          2. 白名单命中 → native_lwc
          3. 配置文件 default_backend
        """
        env_override = os.environ.get("EASYXT_CHART_BACKEND", "").strip().lower()
        if env_override in ("lwc_python", "native_lwc", "klinechart"):
            log.debug("ChartBackendConfig: env override → %s", env_override)
            return env_override

        if account_id and account_id.strip().lower() in self._wl_kline_accounts:
            log.debug("ChartBackendConfig: account '%s' in kline whitelist → klinechart", account_id)
            return "klinechart"

        if strategy_id and strategy_id.strip().lower() in self._wl_kline_strategies:
            log.debug("ChartBackendConfig: strategy '%s' in kline whitelist → klinechart", strategy_id)
            return "klinechart"

        if account_id and account_id.strip().lower() in self._wl_accounts:
            log.debug("ChartBackendConfig: account '%s' in whitelist → native_lwc", account_id)
            return "native_lwc"

        if strategy_id and strategy_id.strip().lower() in self._wl_strategies:
            log.debug("ChartBackendConfig: strategy '%s' in whitelist → native_lwc", strategy_id)
            return "native_lwc"

        return self._default

    def can_switch_now(self) -> tuple[bool, str]:
        """
        判断当前是否允许切换后端（交易时段冻结检查）。

        Returns:
            (True, "") 可以切换
            (False, reason) 不允许切换，reason 说明原因
        """
        if not self._freeze_trading:
            return True, ""
        from .trading_hours_guard import TradingHoursGuard
        in_session, session_name = TradingHoursGuard.current_session()
        if in_session:
            return False, f"交易时段 [{session_name}] 内禁止切换图表后端"
        return True, ""

    def add_account_to_whitelist(self, account_id: str) -> None:
        """运行时动态添加账户到白名单（不持久化）。"""
        self._wl_accounts = self._wl_accounts | {account_id.strip().lower()}
        log.info("ChartBackendConfig: 添加账户白名单: %s", account_id)

    def remove_account_from_whitelist(self, account_id: str) -> None:
        """运行时从白名单移除账户。"""
        self._wl_accounts = self._wl_accounts - {account_id.strip().lower()}
        log.info("ChartBackendConfig: 移除账户白名单: %s", account_id)

    def to_dict(self) -> dict[str, Any]:
        """导出当前配置快照（用于调试/状态上报）。"""
        return {
            "default_backend": self._default,
            "wl_accounts_count": len(self._wl_accounts),
            "wl_strategies_count": len(self._wl_strategies),
            "wl_kline_accounts_count": len(self._wl_kline_accounts),
            "wl_kline_strategies_count": len(self._wl_kline_strategies),
            "freeze_during_trading": self._freeze_trading,
            "ws_handshake_timeout_s": self.ws_handshake_timeout_s,
            "env_override": os.environ.get("EASYXT_CHART_BACKEND", ""),
        }


# ── 加载 / 单例 ────────────────────────────────────────────────────────────────

def _load_from_file() -> dict[str, Any]:
    """从 unified_config.json 读取 chart.engine 节（不存在则返回空 dict）。"""
    try:
        text = _CONFIG_PATH.read_text(encoding="utf-8")
        data = json.loads(text)
        chart_cfg = data.get("chart") or {}
        engine_cfg = chart_cfg.get("engine") or {}
        return dict(engine_cfg)
    except FileNotFoundError:
        log.warning("ChartBackendConfig: 配置文件不存在: %s", _CONFIG_PATH)
        return {}
    except (json.JSONDecodeError, OSError) as exc:
        log.error("ChartBackendConfig: 配置文件读取失败: %s", exc)
        return {}


def get_chart_backend_config() -> ChartBackendConfig:
    """返回全国唯一配置实例（懒初始化，线程安全）。"""
    global _INSTANCE
    if _INSTANCE is not None:
        return _INSTANCE
    with _LOCK:
        if _INSTANCE is not None:
            return _INSTANCE
        _INSTANCE = ChartBackendConfig(_load_from_file())
    return _INSTANCE


def reload_chart_backend_config() -> ChartBackendConfig:
    """重新从文件加载配置（运维热更新时调用）。"""
    global _INSTANCE
    with _LOCK:
        _INSTANCE = ChartBackendConfig(_load_from_file())
    log.info("ChartBackendConfig: 配置已热重载")
    return _INSTANCE
