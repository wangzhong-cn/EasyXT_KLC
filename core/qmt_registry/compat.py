from __future__ import annotations

"""QMT 路径旧语义兼容层。

统一把历史配置中的 ``qmt_path`` / ``qmt_exe_path`` / ``qmt_userdata_path``
以及环境变量中的 QMT 相关入口解析成显式字段，避免旧语义继续散落在
``easy_xt.load_config`` / ``easy_xt.config`` 等模块里。
"""

from dataclasses import asdict, dataclass, field
import os
from pathlib import Path
from typing import Any, Mapping

_QMT_EXECUTABLE_NAMES: tuple[str, ...] = (
    "xtitclient.exe",
    "xtminiqmt.exe",
    "miniqmt.exe",
    "qmt.exe",
    "qmtclient.exe",
)
_QMT_USERDATA_DIR_NAMES: tuple[str, ...] = ("userdata_mini", "userdata")
_QMT_KNOWN_CHILD_DIRS: tuple[str, ...] = ("datadir", "data", "datas", "cfg", "python", "log")


@dataclass(slots=True)
class ResolvedQmtPaths:
    """显式 QMT 路径视图。"""

    install_root: str = ""
    exe_path: str = ""
    userdata_path: str = ""
    datadir_path: str = ""
    data_path: str = ""
    datas_path: str = ""
    cfg_path: str = ""
    xtquant_path: str = ""
    detected_from: list[str] = field(default_factory=list)
    compat_notes: list[str] = field(default_factory=list)
    raw_inputs: dict[str, Any] = field(default_factory=dict)

    @property
    def detected_path(self) -> str:
        if self.install_root:
            return self.install_root
        if self.userdata_path:
            return _display_path(Path(self.userdata_path).parent)
        return ""

    def has_any(self) -> bool:
        return any(
            (
                self.install_root,
                self.exe_path,
                self.userdata_path,
                self.datadir_path,
                self.data_path,
                self.datas_path,
                self.cfg_path,
                self.xtquant_path,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _display_path(path: Path | None) -> str:
    if path is None:
        return ""
    return str(path).replace("\\", "/")


def _path_exists(path: Path | None) -> bool:
    if path is None:
        return False
    try:
        return path.exists()
    except OSError:
        return False


def _normalize_path(value: Any) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return Path(os.path.expandvars(text)).expanduser()
    except Exception:
        return None


def _append_unique(values: list[str], value: str) -> None:
    if value and value not in values:
        values.append(value)


def _maybe_existing_child(parent: Path, name: str) -> Path | None:
    candidate = parent / name
    return candidate if _path_exists(candidate) else None


def _resolve_userdata_dir(install_root: Path) -> Path | None:
    for dir_name in _QMT_USERDATA_DIR_NAMES:
        candidate = install_root / dir_name
        if _path_exists(candidate):
            return candidate
    return None


def _resolve_executable(install_root: Path) -> Path | None:
    candidates = [install_root]
    bin_dir = install_root / "bin.x64"
    if _path_exists(bin_dir):
        candidates.append(bin_dir)
    for root in candidates:
        for exe_name in _QMT_EXECUTABLE_NAMES:
            candidate = root / exe_name
            if _path_exists(candidate):
                return candidate
    return None


def _derive_from_userdata(userdata_path: Path) -> dict[str, str]:
    install_root = userdata_path.parent
    data = {
        "install_root": _display_path(install_root),
        "userdata_path": _display_path(userdata_path),
    }
    for child_name, field_name in (
        ("datadir", "datadir_path"),
        ("data", "data_path"),
        ("datas", "datas_path"),
        ("cfg", "cfg_path"),
    ):
        child = _maybe_existing_child(userdata_path, child_name)
        if child is not None:
            data[field_name] = _display_path(child)
    python_dir = _maybe_existing_child(userdata_path, "python")
    if python_dir is not None:
        xtquant_dir = _maybe_existing_child(python_dir, "xtquant")
        if xtquant_dir is not None:
            data["xtquant_path"] = _display_path(xtquant_dir)
    exe_path = _resolve_executable(install_root)
    if exe_path is not None:
        data["exe_path"] = _display_path(exe_path)
    return data


def _derive_from_install_root(install_root: Path) -> dict[str, str]:
    data = {"install_root": _display_path(install_root)}
    userdata_path = _resolve_userdata_dir(install_root)
    if userdata_path is not None:
        data.update(_derive_from_userdata(userdata_path))
    exe_path = _resolve_executable(install_root)
    if exe_path is not None:
        data["exe_path"] = _display_path(exe_path)
    return data


def _infer_from_path(path: Path, *, kind: str) -> dict[str, str]:
    lower_name = path.name.lower()
    if kind == "xtquant_path":
        data = {"xtquant_path": _display_path(path)}
        if lower_name == "xtquant" and path.parent.name.lower() == "python":
            grand_parent = path.parent.parent
            if grand_parent.name.lower() in {item.lower() for item in _QMT_USERDATA_DIR_NAMES}:
                data.update(_derive_from_userdata(grand_parent))
        return data

    if path.suffix.lower() == ".exe":
        candidate_roots = [path.parent]
        if path.parent.name.lower().startswith("bin") and path.parent.parent != path.parent:
            candidate_roots.append(path.parent.parent)
        for candidate_root in candidate_roots:
            derived = _derive_from_install_root(candidate_root)
            if derived.get("userdata_path"):
                derived["exe_path"] = _display_path(path)
                return derived
        return {
            "install_root": _display_path(candidate_roots[-1]),
            "exe_path": _display_path(path),
        }

    if lower_name in {item.lower() for item in _QMT_USERDATA_DIR_NAMES}:
        return _derive_from_userdata(path)

    if lower_name == "datadir" and path.parent.name.lower() in {item.lower() for item in _QMT_USERDATA_DIR_NAMES}:
        data = _derive_from_userdata(path.parent)
        data["datadir_path"] = _display_path(path)
        return data

    if lower_name in {"data", "datas", "cfg"} and path.parent.name.lower() in {item.lower() for item in _QMT_USERDATA_DIR_NAMES}:
        data = _derive_from_userdata(path.parent)
        data[f"{lower_name}_path"] = _display_path(path)
        return data

    if lower_name == "python" and path.parent.name.lower() in {item.lower() for item in _QMT_USERDATA_DIR_NAMES}:
        return _derive_from_userdata(path.parent)

    if lower_name == "xtquant" and path.parent.name.lower() == "python":
        return _infer_from_path(path, kind="xtquant_path")

    return _derive_from_install_root(path)


def _set_if_empty(target: ResolvedQmtPaths, field_name: str, value: str, source: str) -> None:
    if not value:
        return
    current = getattr(target, field_name)
    if current:
        if current != value:
            _append_unique(
                target.compat_notes,
                f"字段 {field_name} 保留首个值 {current}，忽略来自 {source} 的候选 {value}",
            )
        return
    setattr(target, field_name, value)


def _merge_inferred(target: ResolvedQmtPaths, inferred: Mapping[str, str], source: str) -> None:
    for field_name in (
        "install_root",
        "exe_path",
        "userdata_path",
        "datadir_path",
        "data_path",
        "datas_path",
        "cfg_path",
        "xtquant_path",
    ):
        _set_if_empty(target, field_name, str(inferred.get(field_name, "") or ""), source)
    _append_unique(target.detected_from, source)


def _apply_candidate(target: ResolvedQmtPaths, *, kind: str, value: Any, source: str) -> None:
    raw_text = str(value or "").strip()
    if not raw_text:
        return
    target.raw_inputs[source] = raw_text
    path = _normalize_path(raw_text)
    if path is None:
        return

    if kind in {
        "qmt_install_root",
        "qmt_exe_path",
        "qmt_userdata_path",
        "qmt_datadir_path",
        "qmt_data_path",
        "qmt_datas_path",
        "qmt_cfg_path",
        "qmt_path",
        "xtquant_path",
    }:
        if kind == "qmt_install_root":
            _merge_inferred(target, _derive_from_install_root(path), source)
            return
        if kind == "qmt_exe_path":
            _merge_inferred(target, _infer_from_path(path, kind=kind), source)
            return
        if kind == "qmt_userdata_path":
            _merge_inferred(target, _derive_from_userdata(path), source)
            return
        if kind == "qmt_datadir_path":
            data = {"datadir_path": _display_path(path)}
            if path.parent.name.lower() in {item.lower() for item in _QMT_USERDATA_DIR_NAMES}:
                data.update(_derive_from_userdata(path.parent))
                data["datadir_path"] = _display_path(path)
            _merge_inferred(target, data, source)
            return
        if kind in {"qmt_data_path", "qmt_datas_path", "qmt_cfg_path"}:
            field_name = kind.replace("qmt_", "")
            data = {field_name: _display_path(path)}
            if path.parent.name.lower() in {item.lower() for item in _QMT_USERDATA_DIR_NAMES}:
                data.update(_derive_from_userdata(path.parent))
                data[field_name] = _display_path(path)
            _merge_inferred(target, data, source)
            return
        if kind == "xtquant_path":
            _merge_inferred(target, _infer_from_path(path, kind=kind), source)
            return
        _merge_inferred(target, _infer_from_path(path, kind="qmt_path"), source)


def resolve_legacy_qmt_paths(
    account_config: Mapping[str, Any] | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> ResolvedQmtPaths:
    """把旧配置与环境变量中的 QMT 路径统一解析为显式字段。"""

    account_config = account_config or {}
    env_map = os.environ if env is None else env
    resolved = ResolvedQmtPaths()

    candidate_order: list[tuple[str, str, Any]] = [
        ("env:QMT_EXE_PATH", "qmt_exe_path", env_map.get("QMT_EXE_PATH")),
        ("env:QMT_EXE", "qmt_exe_path", env_map.get("QMT_EXE")),
        ("env:QMT_USERDATA_PATH", "qmt_userdata_path", env_map.get("QMT_USERDATA_PATH")),
        ("env:QMT_USERDATA", "qmt_userdata_path", env_map.get("QMT_USERDATA")),
        ("env:QMT_PATH", "qmt_path", env_map.get("QMT_PATH")),
        ("env:XTQUANT_PATH", "xtquant_path", env_map.get("XTQUANT_PATH")),
        ("config:qmt_install_root", "qmt_install_root", account_config.get("qmt_install_root")),
        ("config:qmt_exe_path", "qmt_exe_path", account_config.get("qmt_exe_path")),
        ("config:qmt_exe", "qmt_exe_path", account_config.get("qmt_exe")),
        ("config:qmt_userdata_path", "qmt_userdata_path", account_config.get("qmt_userdata_path")),
        ("config:userdata_path", "qmt_userdata_path", account_config.get("userdata_path")),
        ("config:qmt_datadir_path", "qmt_datadir_path", account_config.get("qmt_datadir_path")),
        ("config:qmt_data_path", "qmt_data_path", account_config.get("qmt_data_path")),
        ("config:qmt_datas_path", "qmt_datas_path", account_config.get("qmt_datas_path")),
        ("config:qmt_cfg_path", "qmt_cfg_path", account_config.get("qmt_cfg_path")),
        ("config:qmt_path", "qmt_path", account_config.get("qmt_path")),
    ]

    for source, kind, value in candidate_order:
        _apply_candidate(resolved, kind=kind, value=value, source=source)

    if resolved.userdata_path and not resolved.install_root:
        _set_if_empty(resolved, "install_root", _display_path(Path(resolved.userdata_path).parent), "compat:userdata_parent")
    if resolved.install_root and not resolved.userdata_path:
        _merge_inferred(resolved, _derive_from_install_root(Path(resolved.install_root)), "compat:install_root_expand")

    if resolved.datadir_path and not resolved.userdata_path:
        datadir_path = Path(resolved.datadir_path)
        if datadir_path.parent.name.lower() in {item.lower() for item in _QMT_USERDATA_DIR_NAMES}:
            _merge_inferred(resolved, _derive_from_userdata(datadir_path.parent), "compat:datadir_parent")

    if resolved.has_any() and not resolved.detected_from:
        _append_unique(resolved.detected_from, "compat")

    return resolved


def apply_resolved_qmt_paths(settings: dict[str, Any], resolved: ResolvedQmtPaths) -> None:
    """把显式路径回写到 EasyXT 当前 settings 结构。"""

    trade_settings = settings.setdefault("trade", {})
    qmt_settings = settings.setdefault("qmt", {})

    if resolved.userdata_path:
        trade_settings["userdata_path"] = resolved.userdata_path
        qmt_settings["userdata_path"] = resolved.userdata_path
    if resolved.install_root:
        qmt_settings["install_root"] = resolved.install_root
        qmt_settings["detected_path"] = resolved.install_root
    elif resolved.detected_path:
        qmt_settings["detected_path"] = resolved.detected_path
    if resolved.exe_path:
        qmt_settings["exe_path"] = resolved.exe_path
    if resolved.datadir_path:
        qmt_settings["datadir_path"] = resolved.datadir_path
    if resolved.data_path:
        qmt_settings["data_path"] = resolved.data_path
    if resolved.datas_path:
        qmt_settings["datas_path"] = resolved.datas_path
    if resolved.cfg_path:
        qmt_settings["cfg_path"] = resolved.cfg_path
    if resolved.xtquant_path:
        qmt_settings["xtquant_path"] = resolved.xtquant_path

    qmt_settings["compat_detected_from"] = list(resolved.detected_from)
    qmt_settings["compat_notes"] = list(resolved.compat_notes)
    qmt_settings["compat_raw_inputs"] = dict(resolved.raw_inputs)
