"""
OpenAPI Schema 契约冻结测试（Phase 3）

策略：Golden Master + 破坏性变更拦截
  - 首次运行：自动生成 tests/fixtures/openapi_schema.json 作为基线（跳过比对）
  - 后续运行：加载已保存基线，检查：
      1. 已有端点未被删除（破坏性变更 → 测试失败）
      2. 每个已有端点的 HTTP 方法未减少
      3. 新增端点允许（向后兼容，仅打印 warning）

如何更新基线：
  删除 tests/fixtures/openapi_schema.json 然后重新运行测试，
  新基线会自动生成并写入。
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.api_server import app

_FIXTURE_DIR = Path(__file__).parent / "fixtures"
_FIXTURE_PATH = _FIXTURE_DIR / "openapi_schema.json"


class TestOpenAPIContract:
    def test_no_endpoints_removed(self) -> None:
        """
        确保已注册的端点不被删除（删除端点 = 破坏性变更）。

        首次运行时生成基线文件并跳过比对；后续运行时执行 diff 检查。
        """
        current_schema = app.openapi()
        current_paths = set(current_schema["paths"].keys())

        if not _FIXTURE_PATH.exists():
            _FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
            _FIXTURE_PATH.write_text(
                json.dumps(current_schema, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            pytest.skip(
                "首次运行：已生成 OpenAPI 基线 → "
                f"{_FIXTURE_PATH.relative_to(Path.cwd())}  "
                "请将其加入版本控制后再重新运行以启用契约检查。"
            )

        saved_schema = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
        saved_paths = set(saved_schema["paths"].keys())

        # 新增端点是兼容变更，仅记录
        added = current_paths - saved_paths
        if added:
            print(f"\n[contract] 新增端点（兼容变更）: {sorted(added)}")

        # 删除端点是破坏性变更 → 失败
        removed = saved_paths - current_paths
        assert not removed, (
            f"以下端点被删除（破坏性变更），请同步更新基线或恢复端点:\n"
            + "\n".join(f"  - {p}" for p in sorted(removed))
        )

    def test_no_http_methods_removed(self) -> None:
        """确保已有端点的 HTTP 方法不减少（如 PATCH 被移除 → 破坏性变更）。"""
        if not _FIXTURE_PATH.exists():
            pytest.skip("基线文件不存在，请先运行 test_no_endpoints_removed 生成基线。")

        current_schema = app.openapi()
        saved_schema = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))

        for path in set(saved_schema["paths"]) & set(current_schema["paths"]):
            saved_methods = set(saved_schema["paths"][path].keys())
            curr_methods = set(current_schema["paths"][path].keys())
            removed_methods = saved_methods - curr_methods
            assert not removed_methods, (
                f"端点 {path} 的以下 HTTP 方法被删除（破坏性变更）: "
                f"{removed_methods}"
            )

    def test_schema_has_expected_core_endpoints(self) -> None:
        """当前 schema 必须包含 Phase 3 定义的全部核心端点。"""
        current_paths = set(app.openapi()["paths"].keys())
        required = {
            "/health",
            "/api/v1/strategies/",
            "/api/v1/strategies/{strategy_id}",
            "/api/v1/strategies/{strategy_id}/status",
            "/api/v1/strategies/snapshot",
            "/api/v1/market/snapshot/{symbol}",
        }
        missing = required - current_paths
        assert not missing, f"缺少核心端点: {missing}"

    def test_health_response_has_expected_shape(self) -> None:
        """
        /health 的响应 schema 应包含 status / server_time / auth_enabled /
        rate_limit_hits 字段（通过实际 HTTP 请求验证，不依赖 openapi schema 解析）。
        """
        from fastapi.testclient import TestClient
        from unittest.mock import MagicMock, patch

        mock_reg = MagicMock()
        mock_reg.list_running.return_value = []

        with patch("strategies.registry.strategy_registry", mock_reg):
            c = TestClient(app, raise_server_exceptions=True)
            resp = c.get("/health")

        assert resp.status_code == 200
        body = resp.json()
        for field in ("status", "server_time", "strategies_running",
                      "ws_symbols", "auth_enabled", "rate_limit_hits",
                      "uptime_s", "build_version", "commit_sha"):
            assert field in body, f"/health 缺少字段: {field}"
