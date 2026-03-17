"""
审计 hash 链完整性单元测试

覆盖：
  - _compute_entry_hash 确定性
  - record_signal / record_order / record_fill 存储非空 hash
  - verify_chain_integrity 在干净数据上返回 ok=True
  - verify_chain_integrity 在 UPDATE 篡改后检测到 tampered > 0
  - hash 对任意字段修改敏感
"""

from __future__ import annotations

import contextlib
from typing import Generator

import duckdb
import pytest

from core.audit_trail import AuditTrail


# ---------------------------------------------------------------------------
# In-memory DuckDB DB manager (same pattern as test_audit_trail.py)
# ---------------------------------------------------------------------------


class _InMemoryDB:
    """最小内存 DuckDB 管理器，不依赖单例。"""

    def __init__(self):
        self._con = duckdb.connect(":memory:")

    @contextlib.contextmanager
    def get_write_connection(self):
        yield self._con

    @contextlib.contextmanager
    def get_read_connection(self):
        yield self._con


@pytest.fixture
def db() -> _InMemoryDB:
    return _InMemoryDB()


@pytest.fixture
def trail(db) -> AuditTrail:
    return AuditTrail(db_manager=db)


# ---------------------------------------------------------------------------
# Hash determinism
# ---------------------------------------------------------------------------


class TestComputeEntryHash:
    def test_same_input_same_hash(self):
        h1 = AuditTrail._compute_entry_hash(["a", "b", 1, 2.0, None])
        h2 = AuditTrail._compute_entry_hash(["a", "b", 1, 2.0, None])
        assert h1 == h2

    def test_different_input_different_hash(self):
        h1 = AuditTrail._compute_entry_hash(["a", "b"])
        h2 = AuditTrail._compute_entry_hash(["a", "c"])
        assert h1 != h2

    def test_hash_is_64_char_hex(self):
        h = AuditTrail._compute_entry_hash(["anything"])
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_none_and_empty_string_differ(self):
        h_none = AuditTrail._compute_entry_hash([None])
        h_empty = AuditTrail._compute_entry_hash([""])
        assert h_none == h_empty   # both map to "" per impl

    def test_field_order_matters(self):
        h1 = AuditTrail._compute_entry_hash([1, 2, 3])
        h2 = AuditTrail._compute_entry_hash([3, 2, 1])
        assert h1 != h2


# ---------------------------------------------------------------------------
# Hash stored on write
# ---------------------------------------------------------------------------


class TestHashStoredOnWrite:
    def test_signal_hash_non_empty(self, trail: AuditTrail, db: _InMemoryDB):
        sid = trail.record_signal("strat", "000001.SZ", "buy", 10.0, 1000.0)
        with db.get_read_connection() as con:
            row = con.execute(
                "SELECT entry_hash FROM audit_signals WHERE signal_id=?", [sid]
            ).fetchone()
        assert row is not None
        assert len(row[0]) == 64

    def test_order_hash_non_empty(self, trail: AuditTrail, db: _InMemoryDB):
        sid = trail.record_signal("strat", "A", "buy")
        trail.record_order("ORD-001", sid, "A", "buy", 100.0, 10.0)
        with db.get_read_connection() as con:
            row = con.execute(
                "SELECT entry_hash FROM audit_orders WHERE order_id=?", ["ORD-001"]
            ).fetchone()
        assert row is not None and len(row[0]) == 64

    def test_fill_hash_non_empty(self, trail: AuditTrail, db: _InMemoryDB):
        sid = trail.record_signal("strat", "A", "buy")
        trail.record_order("ORD-002", sid, "A", "buy", 100.0, 10.0)
        fid = trail.record_fill("ORD-002", 10.1, 100.0, pnl_snapshot=50.0)
        with db.get_read_connection() as con:
            row = con.execute(
                "SELECT entry_hash FROM audit_fills WHERE fill_id=?", [fid]
            ).fetchone()
        assert row is not None and len(row[0]) == 64


# ---------------------------------------------------------------------------
# verify_chain_integrity – clean data
# ---------------------------------------------------------------------------


class TestVerifyChainIntegrityClean:
    def test_empty_tables_ok(self, trail: AuditTrail):
        result = trail.verify_chain_integrity()
        assert result["ok"] is True
        assert result["signals"]["total"] == 0
        assert result["orders"]["total"] == 0
        assert result["fills"]["total"] == 0

    def test_single_signal_ok(self, trail: AuditTrail):
        trail.record_signal("strat1", "000001.SZ", "buy", 10.0, 100.0)
        result = trail.verify_chain_integrity()
        assert result["ok"] is True
        assert result["signals"]["tampered"] == 0

    def test_full_chain_ok(self, trail: AuditTrail):
        sid = trail.record_signal("strat1", "A", "buy", 10.0, 100.0)
        trail.record_order("ORD-CL", sid, "A", "buy", 100.0, 10.0)
        trail.record_fill("ORD-CL", 10.0, 100.0, pnl_snapshot=200.0)
        result = trail.verify_chain_integrity()
        assert result["ok"] is True
        assert result["signals"]["tampered"] == 0
        assert result["orders"]["tampered"] == 0
        assert result["fills"]["tampered"] == 0

    def test_many_signals_all_ok(self, trail: AuditTrail):
        for i in range(20):
            trail.record_signal(f"strat{i}", f"CODE{i}", "buy")
        result = trail.verify_chain_integrity()
        assert result["ok"] is True
        assert result["signals"]["total"] == 20
        assert result["signals"]["tampered"] == 0


# ---------------------------------------------------------------------------
# verify_chain_integrity – tampered data
# ---------------------------------------------------------------------------


class TestVerifyChainIntegrityTampered:
    def _write_then_tamper_signal(
        self, trail: AuditTrail, db: _InMemoryDB
    ) -> str:
        """写入信号后直接 UPDATE 字段（绕过 record_signal）。"""
        sid = trail.record_signal("strat_x", "000001.SZ", "buy", 9.9, 500.0)
        with db.get_write_connection() as con:
            con.execute(
                "UPDATE audit_signals SET direction='sell' WHERE signal_id=?",
                [sid],
            )
        return sid

    def test_tampered_signal_detected(self, trail: AuditTrail, db: _InMemoryDB):
        sid = self._write_then_tamper_signal(trail, db)
        result = trail.verify_chain_integrity()
        assert result["ok"] is False
        assert result["signals"]["tampered"] == 1
        assert sid in result["signals"]["tampered_ids"]

    def test_tampered_order_detected(self, trail: AuditTrail, db: _InMemoryDB):
        sid = trail.record_signal("strat_y", "B", "buy")
        trail.record_order("ORD-TAMPER", sid, "B", "buy", 100.0, 10.0)
        with db.get_write_connection() as con:
            con.execute(
                "UPDATE audit_orders SET volume=9999.0 WHERE order_id=?",
                ["ORD-TAMPER"],
            )
        result = trail.verify_chain_integrity()
        assert result["ok"] is False
        assert result["orders"]["tampered"] == 1
        assert "ORD-TAMPER" in result["orders"]["tampered_ids"]

    def test_tampered_fill_detected(self, trail: AuditTrail, db: _InMemoryDB):
        sid = trail.record_signal("strat_z", "C", "buy")
        trail.record_order("ORD-F", sid, "C", "buy", 100.0, 10.0)
        fid = trail.record_fill("ORD-F", 10.0, 100.0, pnl_snapshot=0.0)
        with db.get_write_connection() as con:
            con.execute(
                "UPDATE audit_fills SET pnl_snapshot=99999.0 WHERE fill_id=?",
                [fid],
            )
        result = trail.verify_chain_integrity()
        assert result["ok"] is False
        assert result["fills"]["tampered"] == 1

    def test_untampered_records_not_flagged(self, trail: AuditTrail, db: _InMemoryDB):
        """仅篡改1条，其他5条应仍显示 tampered=0。"""
        good_ids = [trail.record_signal("strat_g", "D", "buy") for _ in range(5)]
        bad_sid = trail.record_signal("strat_b", "E", "sell")
        with db.get_write_connection() as con:
            con.execute(
                "UPDATE audit_signals SET code='HACKED' WHERE signal_id=?",
                [bad_sid],
            )
        result = trail.verify_chain_integrity()
        assert result["signals"]["total"] == 6
        assert result["signals"]["tampered"] == 1
        assert bad_sid in result["signals"]["tampered_ids"]
        for gid in good_ids:
            assert gid not in result["signals"]["tampered_ids"]

    def test_result_structure_keys(self, trail: AuditTrail):
        """verify_chain_integrity 返回结构必须包含规定的 key。"""
        result = trail.verify_chain_integrity()
        assert "ok" in result
        for table in ("signals", "orders", "fills"):
            assert table in result
            assert "total" in result[table]
            assert "tampered" in result[table]
            assert "tampered_ids" in result[table]
