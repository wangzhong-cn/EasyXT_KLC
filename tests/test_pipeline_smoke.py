"""
真实链路冒烟测试（Phase 3）

不使用 MagicMock 替代核心对象，验证真实组件端到端协同工作：

  1. _MarketBroadcaster：seq 递增、event_ts_ms 注入、慢消费者自动丢弃、多标的独立
  2. StrategyRegistry：状态机完整生命周期（running→paused→stopped 终态拒绝）
  3. HTTP 链路：通过 TestClient 使用真实 StrategyRegistry 实例验证状态变更链
  4. WS 鉴权拒绝：错误 token 时服务端应拒绝握手
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from core.api_server import _MarketBroadcaster, app
from strategies.registry import StrategyRegistry

client = TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# 1. 真实 _MarketBroadcaster 管道
# ---------------------------------------------------------------------------

class TestRealBroadcaster:
    """Real broadcaster pipeline — no mocks on core broadcast logic."""

    @pytest.mark.asyncio
    async def test_broadcast_injects_seq_and_event_ts_ms(self):
        """连续两次广播：seq 应从 1 递增到 2，event_ts_ms 存在且单调不减。"""
        received: list = []

        class FakeWS:
            async def send_text(self, text: str) -> None:
                received.append(json.loads(text))

        bc = _MarketBroadcaster()
        await bc.asubscribe("SMOKE.SZ", FakeWS())

        await bc.broadcast("SMOKE.SZ", {"symbol": "SMOKE.SZ", "price": 10.5})
        await asyncio.sleep(0.05)  # drain task 执行 send_text（asyncio.wait 需足够事件循环迭代）
        await bc.broadcast("SMOKE.SZ", {"symbol": "SMOKE.SZ", "price": 10.6})
        await asyncio.sleep(0.05)

        assert len(received) == 2
        assert received[0]["seq"] == 1
        assert received[1]["seq"] == 2
        assert "event_ts_ms" in received[0]
        assert received[1]["event_ts_ms"] >= received[0]["event_ts_ms"]

    @pytest.mark.asyncio
    async def test_original_payload_fields_preserved(self):
        """广播不应丢失原始 payload 中的自定义字段。"""
        received: list = []

        class FakeWS:
            async def send_text(self, text: str) -> None:
                received.append(json.loads(text))

        bc = _MarketBroadcaster()
        await bc.asubscribe("PRES.SZ", FakeWS())
        await bc.broadcast("PRES.SZ", {
            "symbol": "PRES.SZ", "price": 9.99, "volume": 12345, "source": "qmt_live"
        })
        await asyncio.sleep(0.05)  # drain task 执行 send_text（asyncio.wait 需足够事件循环迭代）

        assert received[0]["price"] == 9.99
        assert received[0]["volume"] == 12345
        assert received[0]["source"] == "qmt_live"

    @pytest.mark.asyncio
    async def test_slow_consumer_auto_dropped(self):
        """慢消费者超时后应从订阅列表中自动移除，不影响其他消费者。"""
        received_fast: list = []

        class SlowWS:
            async def send_text(self, text: str) -> None:
                await asyncio.sleep(100)

        class FastWS:
            async def send_text(self, text: str) -> None:
                received_fast.append(json.loads(text))

        bc = _MarketBroadcaster()
        await bc.asubscribe("DUAL.SZ", SlowWS())
        await bc.asubscribe("DUAL.SZ", FastWS())
        assert bc.subscriber_count("DUAL.SZ") == 2

        with patch("core.api_server._WS_SEND_TIMEOUT", 0.01):
            await bc.broadcast("DUAL.SZ", {"price": 1.0})
            await asyncio.sleep(0.05)  # 等待 slow timeout + drain

        # Slow consumer dropped, fast consumer still received
        assert bc.subscriber_count("DUAL.SZ") == 1
        assert len(received_fast) == 1

    @pytest.mark.asyncio
    async def test_per_symbol_seq_counters_are_independent(self):
        """不同标的的 seq 计数器应相互独立，各自从 1 开始。"""
        rcv: dict = {"A": [], "B": []}

        class FakeWS:
            def __init__(self, key: str) -> None:
                self._key = key

            async def send_text(self, text: str) -> None:
                rcv[self._key].append(json.loads(text))

        bc = _MarketBroadcaster()
        await bc.asubscribe("AAA.SZ", FakeWS("A"))
        await bc.asubscribe("BBB.SZ", FakeWS("B"))

        await bc.broadcast("AAA.SZ", {})
        await bc.broadcast("AAA.SZ", {})
        await bc.broadcast("BBB.SZ", {})
        await asyncio.sleep(0.05)  # drain tasks 执行 send_text（asyncio.wait 需足够事件循环迭代）

        assert rcv["A"][0]["seq"] == 1
        assert rcv["A"][1]["seq"] == 2
        assert rcv["B"][0]["seq"] == 1  # BBB 的计数器独立

    def test_no_subscribers_broadcast_is_noop(self):
        """无订阅者时广播不应抛异常。"""
        bc = _MarketBroadcaster()
        asyncio.run(bc.broadcast("NOBODY.SZ", {"price": 1.0}))  # should not raise


# ---------------------------------------------------------------------------
# 2. 真实 StrategyRegistry 状态机生命周期
# ---------------------------------------------------------------------------

class TestRealRegistryLifecycle:
    """Full lifecycle using a fresh StrategyRegistry instance — no mocks."""

    def setup_method(self) -> None:
        self.reg = StrategyRegistry()
        self.reg.register("smoke_v1", None, account_id="88001234", params={"k": 1})

    def test_initial_status_is_running(self) -> None:
        assert self.reg.get("smoke_v1").status == "running"

    def test_running_to_paused(self) -> None:
        result = self.reg.update_status("smoke_v1", "paused")
        assert result == (True, "")
        assert self.reg.get("smoke_v1").status == "paused"

    def test_paused_back_to_running(self) -> None:
        self.reg.update_status("smoke_v1", "paused")
        result = self.reg.update_status("smoke_v1", "running")
        assert result == (True, "")
        assert self.reg.get("smoke_v1").status == "running"

    def test_running_to_error_and_recover(self) -> None:
        self.reg.update_status("smoke_v1", "error")
        result = self.reg.update_status("smoke_v1", "running")
        assert result == (True, "")

    def test_running_to_stopped(self) -> None:
        result = self.reg.update_status("smoke_v1", "stopped")
        assert result == (True, "")
        assert self.reg.get("smoke_v1").status == "stopped"

    def test_stopped_is_terminal_rejects_all_transitions(self) -> None:
        """stopped 是终态，任何转换都应返回 (False, reason)。"""
        self.reg.update_status("smoke_v1", "stopped")
        for target in ("running", "paused", "error", "stopped"):
            ok, reason = self.reg.update_status("smoke_v1", target)
            assert ok is False
            assert "stopped" in reason

    def test_not_found_returns_none(self) -> None:
        assert self.reg.update_status("ghost_id", "running") is None


# ---------------------------------------------------------------------------
# 3. HTTP 端到端链路（TestClient + 真实 StrategyRegistry 实例）
# ---------------------------------------------------------------------------

class TestHTTPChainSmoke:
    """End-to-end HTTP chain using a real StrategyRegistry (DuckDB snapshot skipped)."""

    def test_full_status_patch_chain(self) -> None:
        """running→paused→running→stopped，进入终态后 running 应返回 409。"""
        real_reg = StrategyRegistry()
        real_reg.register("chain_v1", None, account_id="88001234")

        with patch("strategies.registry.strategy_registry", real_reg):
            # running → paused
            r1 = client.patch(
                "/api/v1/strategies/chain_v1/status", json={"status": "paused"}
            )
            assert r1.status_code == 200, r1.json()

            # paused → running
            r2 = client.patch(
                "/api/v1/strategies/chain_v1/status", json={"status": "running"}
            )
            assert r2.status_code == 200

            # running → stopped (terminal)
            r3 = client.patch(
                "/api/v1/strategies/chain_v1/status", json={"status": "stopped"}
            )
            assert r3.status_code == 200

            # stopped → running (illegal → 409)
            r4 = client.patch(
                "/api/v1/strategies/chain_v1/status", json={"status": "running"}
            )
            assert r4.status_code == 409
            assert "trace_id" in r4.json()    # Error format验证

    def test_get_and_list_use_real_registry(self) -> None:
        """GET /strategies/ 应返回真实注册表中的条目。"""
        real_reg = StrategyRegistry()
        real_reg.register("list_v1", None, account_id="99001234", params={}, tags=["smoke"])

        with patch("strategies.registry.strategy_registry", real_reg):
            resp = client.get("/api/v1/strategies/")
            assert resp.status_code == 200
            ids = [s["strategy_id"] for s in resp.json()]
            assert "list_v1" in ids

            resp2 = client.get("/api/v1/strategies/list_v1")
            assert resp2.status_code == 200
            assert resp2.json()["tags"] == ["smoke"]

    def test_ws_auth_rejects_invalid_token(self) -> None:
        """WS 携带错误 token 时服务端应在握手阶段拒绝（引发异常或 disconnect）。"""
        with patch("core.api_server._API_TOKEN", "secret-token"):
            rejected = False
            try:
                with client.websocket_connect(
                    "/ws/market/TEST.SZ?token=wrongtoken"
                ) as ws:
                    ws.receive_json()
            except Exception:
                rejected = True
            assert rejected, "预期 WS 连接被拒绝，但未抛出异常"
