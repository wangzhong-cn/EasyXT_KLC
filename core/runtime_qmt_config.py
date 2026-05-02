from __future__ import annotations

"""统一管理 `config/unified_config.json` 中的运行时 QMT 配置。"""

from datetime import date
import json
from pathlib import Path
from typing import Any

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "unified_config.json"


def _normalize_payload(data: dict[str, Any], config_path: Path) -> dict[str, Any]:
    account_cfg = data.get("settings", {}).get("account", {}) if isinstance(data, dict) else {}
    return {
        "config_path": str(config_path),
        "exists": config_path.exists(),
        "qmt_path": str(account_cfg.get("qmt_path") or account_cfg.get("qmt_exe_path") or "").strip(),
        "qmt_userdata_path": str(account_cfg.get("qmt_userdata_path") or account_cfg.get("userdata_path") or "").strip(),
        "last_updated": str(data.get("last_updated") or "").strip() or None,
    }


def read_runtime_qmt_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """读取 unified_config.json 中的运行时 QMT 主配置。"""

    path = Path(config_path) if config_path else _DEFAULT_CONFIG_PATH
    if not path.exists():
        return {
            "config_path": str(path),
            "exists": False,
            "qmt_path": "",
            "qmt_userdata_path": "",
            "last_updated": None,
        }

    raw = path.read_text(encoding="utf-8-sig")
    data = json.loads(raw) if raw.strip() else {}
    if not isinstance(data, dict):
        data = {}
    return _normalize_payload(data, path)


def write_runtime_qmt_config(
    *,
    qmt_path: str | None = None,
    qmt_userdata_path: str | None = None,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    """更新 unified_config.json 中的运行时 QMT 配置。"""

    path = Path(config_path) if config_path else _DEFAULT_CONFIG_PATH
    path.parent.mkdir(parents=True, exist_ok=True)

    data: dict[str, Any]
    if path.exists():
        raw = path.read_text(encoding="utf-8-sig")
        loaded = json.loads(raw) if raw.strip() else {}
        data = loaded if isinstance(loaded, dict) else {}
    else:
        data = {}

    settings = data.setdefault("settings", {})
    if not isinstance(settings, dict):
        settings = {}
        data["settings"] = settings
    account = settings.setdefault("account", {})
    if not isinstance(account, dict):
        account = {}
        settings["account"] = account

    updated_fields: list[str] = []

    normalized_qmt_path = str(qmt_path or "").strip()
    normalized_userdata_path = str(qmt_userdata_path or "").strip()
    if not normalized_qmt_path and not normalized_userdata_path:
        raise ValueError("至少需要提供 qmt_path 或 qmt_userdata_path")

    if normalized_qmt_path and str(account.get("qmt_path") or "").strip() != normalized_qmt_path:
        account["qmt_path"] = normalized_qmt_path
        updated_fields.append("qmt_path")

    if normalized_userdata_path and str(account.get("qmt_userdata_path") or "").strip() != normalized_userdata_path:
        account["qmt_userdata_path"] = normalized_userdata_path
        updated_fields.append("qmt_userdata_path")

    data["last_updated"] = date.today().isoformat()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    payload = read_runtime_qmt_config(path)
    payload["updated_fields"] = updated_fields
    return payload
