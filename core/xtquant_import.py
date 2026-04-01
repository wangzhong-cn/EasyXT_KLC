from __future__ import annotations

import contextlib
import importlib
import io
import logging
import os
import warnings
from collections.abc import Iterator
from types import ModuleType

log = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "yes", "on"}


def xtquant_import_noise_enabled() -> bool:
    return str(os.environ.get("EASYXT_XTQUANT_IMPORT_VERBOSE", "")).strip().lower() in _TRUTHY


def _xtquant_import_trace_enabled() -> bool:
    return str(os.environ.get("EASYXT_XTQUANT_IMPORT_TRACE", "")).strip().lower() in _TRUTHY


@contextlib.contextmanager
def suppress_xtquant_import_noise() -> Iterator[None]:
    """抑制 xtquant 导入期的 stdout/stderr 与已知非关键 warning。"""
    if xtquant_import_noise_enabled():
        yield
        return

    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r".*pkg_resources is deprecated as an API.*",
            category=UserWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message=r".*pkg_resources package is slated for removal.*",
            category=UserWarning,
        )
        try:
            with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
                yield
        finally:
            if _xtquant_import_trace_enabled():
                captured = (stdout_buffer.getvalue() + stderr_buffer.getvalue()).strip()
                if captured:
                    log.debug("xtquant import noise suppressed: %s", captured.replace("\n", " | "))


def import_xtquant_module(module_name: str) -> ModuleType:
    """静默导入 xtquant 相关模块，默认吞掉导入期噪音。"""
    with suppress_xtquant_import_noise():
        return importlib.import_module(module_name)


def import_xtquant_package() -> ModuleType:
    return import_xtquant_module("xtquant")


def import_xtdata_module() -> ModuleType:
    return import_xtquant_module("xtquant.xtdata")