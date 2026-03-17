"""
Gate 3 – 审计链路单元测试

覆盖：
  - record_signal / record_order / record_fill 写入正确
  - get_chain 返回完整链路
  - update_order_status 正确同步
  - 幂等性（INSERT OR IGNORE on duplicate order_id）
  - get_signals_by_strategy 聚合查询

使用内存 DuckDB（:memory:），不依赖真实数据库文件。
"""

import time
import uuid

import duckdb
import pytest

from core.audit_trail import AuditChain, AuditTrail


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class _MemDBManager:
    """最小内存 DuckDB 管理器，供测试注入。"""

    def __init__(self):
        self._con = duckdb.connect(":memory:")
        import threading
        self._lock = threading.Lock()

    # 模拟 get_write_connection（contextmanager）
    from contextlib import contextmanager

    @contextmanager
    def get_write_connection(self):
        with self._lock:
            yield self._con

    @contextmanager
    def get_read_connection(self):
        yield self._con


@pytest.fixture
def trail():
    return AuditTrail(db_manager=_MemDBManager())


# ---------------------------------------------------------------------------
# record_signal
# ---------------------------------------------------------------------------

class TestRecordSignal:
    def test_returns_uuid(self, trail):
        sid = trail.record_signal("s1", "000001.SZ", "buy", price_hint=10.5, volume_hint=100)
        assert isinstance(sid, str) and len(sid) == 36  # UUID format

    def test_custom_signal_id(self, trail):
        custom_id = "fixed-id-001"
        sid = trail.record_signal("s1", "000001.SZ", "sell", signal_id=custom_id)
        assert sid == custom_id

    def test_fields_persisted(self, trail):
        sid = trail.record_signal("strat_A", "600000.SH", "buy", price_hint=8.0, volume_hint=500)
        chain = trail.get_chain(sid)
        assert chain.signal is not None
        assert chain.signal.strategy_id == "strat_A"
        assert chain.signal.code == "600000.SH"
        assert chain.signal.direction == "buy"
        assert chain.signal.price_hint == pytest.approx(8.0)


# ---------------------------------------------------------------------------
# record_order
# ---------------------------------------------------------------------------

class TestRecordOrder:
    def test_order_linked_to_signal(self, trail):
        sid = trail.record_signal("strat_B", "000002.SZ", "buy")
        oid = str(uuid.uuid4())
        trail.record_order(oid, sid, "000002.SZ", "buy", 200, 12.0)
        chain = trail.get_chain(sid)
        assert len(chain.orders) == 1
        assert chain.orders[0].order_id == oid
        assert chain.orders[0].signal_id == sid
        assert chain.orders[0].status == "submitted"

    def test_duplicate_order_id_ignored(self, trail):
        sid = trail.record_signal("strat_B", "A", "buy")
        oid = "dup-order"
        trail.record_order(oid, sid, "A", "buy", 100, 5.0)
        trail.record_order(oid, sid, "A", "buy", 100, 5.0)  # duplicate
        chain = trail.get_chain(sid)
        assert len(chain.orders) == 1


# ---------------------------------------------------------------------------
# record_fill
# ---------------------------------------------------------------------------

class TestRecordFill:
    def test_fill_linked_to_order(self, trail):
        sid = trail.record_signal("strat_C", "000003.SZ", "buy")
        oid = str(uuid.uuid4())
        trail.record_order(oid, sid, "000003.SZ", "buy", 100, 15.0)
        fid = trail.record_fill(oid, filled_price=15.1, filled_volume=100, pnl_snapshot=1234.5)
        chain = trail.get_chain(sid)
        assert len(chain.fills) == 1
        assert chain.fills[0].fill_id == fid
        assert chain.fills[0].order_id == oid
        assert chain.fills[0].pnl_snapshot == pytest.approx(1234.5)

    def test_fill_updates_order_status(self, trail):
        sid = trail.record_signal("strat_C", "X", "buy")
        oid = str(uuid.uuid4())
        trail.record_order(oid, sid, "X", "buy", 10, 1.0)
        trail.record_fill(oid, 1.0, 10)
        chain = trail.get_chain(sid)
        assert chain.orders[0].status == "filled"

    def test_custom_fill_id(self, trail):
        sid = trail.record_signal("strat_C", "Y", "sell")
        oid = str(uuid.uuid4())
        trail.record_order(oid, sid, "Y", "sell", 50, 20.0)
        fid = trail.record_fill(oid, 20.0, 50, fill_id="custom-fill-001")
        assert fid == "custom-fill-001"


# ---------------------------------------------------------------------------
# update_order_status
# ---------------------------------------------------------------------------

class TestUpdateOrderStatus:
    def test_cancelled_status(self, trail):
        sid = trail.record_signal("strat_D", "Z", "buy")
        oid = str(uuid.uuid4())
        trail.record_order(oid, sid, "Z", "buy", 100, 5.0)
        trail.update_order_status(oid, "cancelled")
        chain = trail.get_chain(sid)
        assert chain.orders[0].status == "cancelled"


# ---------------------------------------------------------------------------
# Full chain
# ---------------------------------------------------------------------------

class TestFullChain:
    def test_complete_chain(self, trail):
        sid = trail.record_signal("strat_E", "000001.SZ", "buy", price_hint=10.0, volume_hint=1000)
        oid = str(uuid.uuid4())
        trail.record_order(oid, sid, "000001.SZ", "buy", 1000, 10.0)
        fid = trail.record_fill(oid, 10.05, 1000, pnl_snapshot=-500.0)
        chain = trail.get_chain(sid)
        assert chain.signal is not None
        assert len(chain.orders) == 1
        assert len(chain.fills) == 1
        assert chain.fills[0].filled_price == pytest.approx(10.05)

    def test_missing_signal_returns_empty(self, trail):
        chain = trail.get_chain("nonexistent-id")
        assert chain.signal is None
        assert chain.orders == []
        assert chain.fills == []


# ---------------------------------------------------------------------------
# get_signals_by_strategy
# ---------------------------------------------------------------------------

class TestGetSignalsByStrategy:
    def test_returns_list(self, trail):
        for i in range(3):
            trail.record_signal("strat_F", f"00000{i}.SZ", "buy")
        results = trail.get_signals_by_strategy("strat_F")
        assert isinstance(results, list)
        assert len(results) == 3

    def test_limit_respected(self, trail):
        for i in range(10):
            trail.record_signal("strat_G", f"X{i}", "buy")
        results = trail.get_signals_by_strategy("strat_G", limit=3)
        assert len(results) <= 3

    def test_unknown_strategy_empty(self, trail):
        results = trail.get_signals_by_strategy("no_such_strategy")
        assert results == []


# ---------------------------------------------------------------------------
# AuditTrail() without explicit db_manager (lines 168-169)
# ---------------------------------------------------------------------------

class TestAuditTrailDefaultInit:
    def test_init_without_db_manager_uses_get_db_manager(self):
        """AuditTrail() with no explicit db_manager triggers the lazy import+call (lines 168-169)."""
        from unittest.mock import patch, MagicMock
        mock_manager = _MemDBManager()
        with patch(
            "data_manager.duckdb_connection_pool.get_db_manager",
            return_value=mock_manager,
        ):
            t = AuditTrail()  # db_manager=None → uses get_db_manager()
        assert t._db is mock_manager


# ---------------------------------------------------------------------------
# Exception handling paths — each Write/Read method (lines 250-251, 286-287,
# 332-333, 344-345, 377-378, 402-404)
# ---------------------------------------------------------------------------

def _make_failing_trail():
    """Bypass __init__, inject a DB that always throws on context entry."""
    from unittest.mock import MagicMock
    from contextlib import contextmanager

    trail = object.__new__(AuditTrail)

    @contextmanager
    def _raise(*a, **kw):
        raise RuntimeError("DB unavailable")
        yield  # noqa: unreachable

    db = MagicMock()
    db.get_write_connection.side_effect = RuntimeError("DB unavailable")
    db.get_read_connection.side_effect = RuntimeError("DB unavailable")
    trail._db = db
    return trail


class TestExceptionHandlingPaths:
    """单个 Broken DB 存根即可覆盖多个 except 路径。"""

    def test_record_signal_db_exception_returns_uuid(self):
        t = _make_failing_trail()
        sid = t.record_signal("s", "X", "buy")
        assert isinstance(sid, str) and len(sid) == 36  # UUID even on failure

    def test_record_order_db_exception_no_raise(self):
        t = _make_failing_trail()
        t.record_order("oid", "sid", "X", "buy", 100, 5.0)  # must not raise

    def test_record_fill_db_exception_returns_fid(self):
        t = _make_failing_trail()
        fid = t.record_fill("oid", 5.0, 100)
        assert isinstance(fid, str)  # UUID or custom id

    def test_update_order_status_db_exception_no_raise(self):
        t = _make_failing_trail()
        t.update_order_status("oid", "cancelled")  # must not raise

    def test_get_chain_db_exception_returns_empty(self):
        t = _make_failing_trail()
        chain = t.get_chain("nonexistent")
        assert chain.signal is None
        assert chain.orders == []

    def test_get_signals_by_strategy_db_exception_returns_empty_list(self):
        t = _make_failing_trail()
        result = t.get_signals_by_strategy("strat")
        assert result == []

    def test_verify_chain_integrity_db_exception_returns_not_ok(self):
        t = _make_failing_trail()
        result = t.verify_chain_integrity()
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# verify_chain_integrity — tampered fields & chain-break paths
# (lines 485, 495-501, 502-509)
# ---------------------------------------------------------------------------

class TestVerifyChainIntegrity:

    def test_clean_chain_is_ok(self, trail):
        sid = trail.record_signal("s", "X", "buy")
        result = trail.verify_chain_integrity()
        assert result["ok"] is True
        assert result["signals"]["tampered"] == 0

    def test_detects_tampered_field(self, trail):
        """UPDATE a field so entry_hash no longer matches → tampered_ids non-empty."""
        sid = trail.record_signal("s", "000001.SZ", "buy", price_hint=10.0)
        # Corrupt the 'code' field to break entry_hash
        with trail._db.get_write_connection() as con:
            con.execute(
                "UPDATE audit_signals SET code='TAMPERED' WHERE signal_id=?", [sid]
            )
        result = trail.verify_chain_integrity()
        assert result["ok"] is False
        assert sid in result["signals"]["tampered_ids"]

    def test_detects_chain_break(self, trail):
        """Write two signals, corrupt the second's prev_hash → chain_break detected."""
        sid1 = trail.record_signal("s", "A", "buy")
        sid2 = trail.record_signal("s", "B", "buy")
        with trail._db.get_write_connection() as con:
            con.execute(
                "UPDATE audit_signals SET prev_hash='wrong_hash' WHERE signal_id=?", [sid2]
            )
        result = trail.verify_chain_integrity()
        assert result["ok"] is False
        assert result["signals"]["chain_breaks"] >= 1


# ---------------------------------------------------------------------------
# Phase 2: account_id 多账户治理
# ---------------------------------------------------------------------------


class TestAccountIdEnrichment:
    """P2-4: 审计三表 account_id 维度增强。"""

    def test_signal_account_id_stored(self, trail):
        sid = trail.record_signal("s1", "600519.SH", "buy", account_id="ACC_001")
        chain = trail.get_chain(sid)
        assert chain.signal is not None
        assert chain.signal.account_id == "ACC_001"

    def test_signal_account_id_default_empty(self, trail):
        sid = trail.record_signal("s1", "600519.SH", "buy")
        chain = trail.get_chain(sid)
        assert chain.signal.account_id == ""

    def test_order_account_id(self, trail):
        sid = trail.record_signal("s1", "600519.SH", "buy")
        trail.record_order("O1", sid, "600519.SH", "buy", 100, 1800.0, account_id="ACC_A")
        chain = trail.get_chain(sid)
        assert chain.orders[0].account_id == "ACC_A"

    def test_fill_account_id(self, trail):
        sid = trail.record_signal("s1", "600519.SH", "buy")
        trail.record_order("O2", sid, "600519.SH", "buy", 100, 1800.0)
        trail.record_fill("O2", 1801.0, 100, pnl_snapshot=50.0, account_id="ACC_B")
        chain = trail.get_chain(sid)
        assert chain.fills[0].account_id == "ACC_B"

    def test_get_signals_by_account(self, trail):
        trail.record_signal("s1", "600519.SH", "buy", account_id="ACC_001")
        trail.record_signal("s2", "000001.SZ", "sell", account_id="ACC_001")
        trail.record_signal("s3", "600036.SH", "buy", account_id="ACC_002")
        results = trail.get_signals_by_account("ACC_001")
        assert len(results) == 2

    def test_get_signals_by_account_empty(self, trail):
        trail.record_signal("s1", "600519.SH", "buy", account_id="ACC_001")
        results = trail.get_signals_by_account("NONEXISTENT")
        assert len(results) == 0

    def test_get_signals_by_account_limit(self, trail):
        for i in range(5):
            trail.record_signal("s1", f"00000{i}.SZ", "buy", account_id="ACC_X")
        results = trail.get_signals_by_account("ACC_X", limit=3)
        assert len(results) == 3

    def test_get_signals_by_account_db_exception(self):
        t = _make_failing_trail()
        result = t.get_signals_by_account("ACC_001")
        assert result == []

    def test_schema_version_bumped(self):
        from core.audit_trail import _SCHEMA_VERSION
        assert _SCHEMA_VERSION == 3

    def test_account_id_column_in_all_tables(self, trail):
        """验证迁移后三表都有 account_id 列。"""
        with trail._db.get_write_connection() as con:
            for table in ("audit_signals", "audit_orders", "audit_fills"):
                cols = [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
                assert "account_id" in cols, f"{table} 缺少 account_id 列"
