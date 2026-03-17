from __future__ import annotations

from data_manager.unified_data_interface import UnifiedDataInterface


def test_unified_data_interface_constructs():
    udi = UnifiedDataInterface(duckdb_path=":memory:", silent_init=True)
    assert udi is not None
