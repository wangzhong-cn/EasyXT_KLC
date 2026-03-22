from __future__ import annotations

from gui_app.widgets.chart.backend_config import ChartBackendConfig


def test_klinechart_whitelist_takes_priority() -> None:
    cfg = ChartBackendConfig(
        {
            "default_backend": "lwc_python",
            "klinechart_whitelist": {"accounts": ["A001"], "strategies": ["s1"]},
            "native_lwc_whitelist": {"accounts": ["A002"], "strategies": ["s2"]},
        }
    )
    assert cfg.get_backend(account_id="A001") == "klinechart"
    assert cfg.get_backend(strategy_id="s1") == "klinechart"
    assert cfg.get_backend(account_id="A002") == "native_lwc"
    assert cfg.get_backend(strategy_id="s2") == "native_lwc"
