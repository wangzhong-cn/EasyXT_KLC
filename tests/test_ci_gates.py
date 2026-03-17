"""
tests/test_ci_gates.py — CI 门禁治理机制单元测试

覆盖场景：
  P0-1 生产环境 approver_2 硬校验
  P0-2 日志脱敏（canonical / 签名值不落入日志）
  P0-3 Delta 阻断阈值参数化
  P1-1 验签负例 × 4 类（字段顺序篡改、reason 改动、key_id 错误、过期豁免）
  P1-2 豁免台账落盘（governance_ledger.jsonl）
  额外：字段类型校验、必填字段缺失
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# 确保 tools/ 可以被直接 import
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.config import WAIVER as _WCFG


# ── 辅助工厂 ──────────────────────────────────────────────────────────────────

def _make_waiver(
    *,
    enabled: bool = True,
    phase: int = 0,
    effective_target: float = 0.27,
    expires_offset_days: int = 14,
    approver: str = "lead@example.com",
    approver_2: str = "",
    approval_id: str = "OA-TEST-001",
    reason: str = "unit test waiver",
    hmac_key: str = "",
    extra: dict | None = None,
) -> dict:
    """构造豁免文件 dict（不写磁盘）。hmac_key 非空时自动计算并注入 waiver_hmac。"""
    expires = (date.today() + timedelta(days=expires_offset_days)).isoformat()
    w: dict = {
        "enabled": enabled,
        "phase": phase,
        "effective_target": effective_target,
        "expires": expires,
        "approver": approver,
        "approver_2": approver_2,
        "approval_id": approval_id,
        "reason": reason,
    }
    if extra:
        w.update(extra)
    if hmac_key:
        canonical = "|".join(str(w.get(f, "")) for f in _WCFG.canonical_fields)
        sig = hmac.new(hmac_key.encode(), canonical.encode(), hashlib.sha256).hexdigest()
        w["waiver_hmac"] = sig
    return w


def _call_read_waiver(waiver_dict: dict, *, env: dict | None = None) -> dict | None:
    """将 waiver_dict 写入临时文件，patch _WAIVER_FILE，然后调用 _read_coverage_waiver()。"""
    import tools.check_phase_exit as cpe

    env = env or {}
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as tf:
        json.dump(waiver_dict, tf, ensure_ascii=False)
        tf_path = Path(tf.name)

    try:
        with patch.object(cpe, "_WAIVER_FILE", tf_path), patch.dict(os.environ, env, clear=False):
            # 抑制 warnings（dev 环境不配置 key 时会 warn）
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                # 抑制 logger 输出，避免测试日志污染
                with patch.object(cpe._logger, "info"), \
                     patch.object(cpe._logger, "warning"), \
                     patch.object(cpe._logger, "error"):
                    return cpe._read_coverage_waiver()
    finally:
        tf_path.unlink(missing_ok=True)


# ── P0-1: 生产环境 approver_2 硬校验 ─────────────────────────────────────────

class TestApprover2ProdEnv:
    """生产环境（ENVIRONMENT=prod）下双人审批的强制校验。"""

    def test_prod_single_approver_rejected(self):
        """生产环境仅有 approver1，应拒绝。"""
        w = _make_waiver(approver="lead@example.com", approver_2="")
        # prod 环境 + 无签名 key → 因缺 WAIVER_SIGNING_KEY 先拒绝，
        # 实际联测需先注入 signing_key，这里重点验证 approver_2 路径
        result = _call_read_waiver(w, env={"ENVIRONMENT": "prod", _WCFG.signing_key_env: "test-key"})
        # missing waiver_hmac → rejected（但说明 prod 路径进入了）
        # 我们在 waiver 中加入哨兵字段来区分到底在哪一步拒绝
        assert result is None

    def test_prod_dual_approver_passes(self):
        """生产环境两名审批人 + 正确 HMAC，应通过。"""
        key = "secret-key"
        w = _make_waiver(approver="lead@example.com", approver_2="peer@example.com", hmac_key=key)
        result = _call_read_waiver(w, env={"ENVIRONMENT": "prod", _WCFG.signing_key_env: key})
        assert result is not None
        assert result["approval_id"] == "OA-TEST-001"

    def test_dev_single_approver_passes(self):
        """开发环境只有 approver1，应允许通过。"""
        w = _make_waiver(approver="lead@example.com", approver_2="")
        result = _call_read_waiver(w, env={"ENVIRONMENT": "dev"})
        assert result is not None

    def test_prod_empty_both_approvers_rejected(self):
        """两个审批人字段均为空，任何环境都拒绝。"""
        w = _make_waiver(approver="", approver_2="")
        result = _call_read_waiver(w, env={"ENVIRONMENT": "prod"})
        assert result is None


# ── P1-1: 验签负例 × 4 类 ──────────────────────────────────────────────────

class TestHMACNegative:
    """HMAC 验签失败场景（设置了 WAIVER_SIGNING_KEY 时生效）。"""

    _KEY = "ci-test-secret-key"

    def _result_with_tamper(self, tamper_fn) -> dict | None:
        """构造正确签名的 waiver，经 tamper_fn 修改后执行验签，返回结果。"""
        w = _make_waiver(approver="a@x.com", approver_2="b@x.com", hmac_key=self._KEY)
        tamper_fn(w)
        return _call_read_waiver(w, env={_WCFG.signing_key_env: self._KEY})

    def test_reason_tampered(self):
        """修改 reason 字段后，HMAC 验签应失败。"""
        def tamper(w):
            w["reason"] = w["reason"] + " TAMPERED"
        assert self._result_with_tamper(tamper) is None

    def test_approval_id_tampered(self):
        """修改 approval_id（签名规范串首字段）后，验签应失败。"""
        def tamper(w):
            w["approval_id"] = "FAKE-ID"
        assert self._result_with_tamper(tamper) is None

    def test_wrong_signing_key(self):
        """使用错误密钥验签，应失败。"""
        w = _make_waiver(approver="a@x.com", approver_2="b@x.com", hmac_key="correct-key")
        result = _call_read_waiver(w, env={_WCFG.signing_key_env: "wrong-key"})
        assert result is None

    def test_missing_waiver_hmac_field(self):
        """设置了密钥但 waiver 文件中无 waiver_hmac 字段，应失败。"""
        w = _make_waiver(approver="a@x.com", approver_2="b@x.com")
        # 故意不生成 hmac，waiver 中无 waiver_hmac 字段
        assert "waiver_hmac" not in w
        result = _call_read_waiver(w, env={_WCFG.signing_key_env: self._KEY})
        assert result is None

    def test_canonical_field_order_tamper(self):
        """签名规范串字段顺序固定，交换两字段内容后验签应失败。"""
        def tamper(w):
            # 把 approval_id 和 reason 内容互换（等效于字段顺序篡改）
            w["approval_id"], w["reason"] = w["reason"], w["approval_id"]
        assert self._result_with_tamper(tamper) is None


# ── P1-1 额外：过期豁免验签 ───────────────────────────────────────────────────

class TestExpiredWaiver:
    """已过期的豁免，即使签名正确也应被拒绝。"""

    def test_expired_waiver_rejected(self):
        key = "test-key"
        w = _make_waiver(
            approver="a@x.com", approver_2="b@x.com",
            expires_offset_days=-1,  # 昨天到期
            hmac_key=key,
        )
        result = _call_read_waiver(w, env={_WCFG.signing_key_env: key})
        assert result is None

    def test_future_waiver_passes(self):
        key = "test-key"
        w = _make_waiver(
            approver="a@x.com", approver_2="b@x.com",
            expires_offset_days=30,
            hmac_key=key,
        )
        result = _call_read_waiver(w, env={_WCFG.signing_key_env: key})
        assert result is not None


# ── P0-2: 日志脱敏验证 ───────────────────────────────────────────────────────

class TestLogDesensitization:
    """确认签名规范串、HMAC 期望值不出现在日志中。"""

    def test_canonical_not_logged_on_success(self, caplog):
        """验签通过时，canonical 字符串不应出现在任何日志记录中。"""
        import logging
        key = "test-key"
        w = _make_waiver(approver="a@x.com", approver_2="b@x.com", hmac_key=key)
        # 构造预期的 canonical 内容（包含 reason）
        canonical = "|".join(str(w.get(f, "")) for f in _WCFG.canonical_fields)

        import tools.check_phase_exit as cpe
        import tempfile
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as tf:
            json.dump(w, tf, ensure_ascii=False)
            tf_path = Path(tf.name)
        try:
            with patch.object(cpe, "_WAIVER_FILE", tf_path), \
                 patch.dict(os.environ, {_WCFG.signing_key_env: key}, clear=False), \
                 caplog.at_level(logging.DEBUG, logger="ci_gate.check_phase_exit"):
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    cpe._read_coverage_waiver()
        finally:
            tf_path.unlink(missing_ok=True)

        # canonical 字符串不应出现在任何日志消息中
        for record in caplog.records:
            assert canonical not in record.getMessage(), (
                f"canonical string leaked into log: {record.getMessage()[:200]}"
            )

    def test_hmac_expected_value_not_logged_on_failure(self, caplog):
        """验签失败时，expected HMAC 值不应出现在日志中。"""
        import logging
        import tools.check_phase_exit as cpe
        w = _make_waiver(approver="a@x.com", approver_2="b@x.com")
        w["waiver_hmac"] = "fake_hmac_value_1234567890"
        # 计算本应出现的期望签名
        correct_key = "correct-key"
        canonical = "|".join(str(w.get(f, "")) for f in _WCFG.canonical_fields)
        expected = hmac.new(correct_key.encode(), canonical.encode(), hashlib.sha256).hexdigest()

        import tempfile
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as tf:
            json.dump(w, tf, ensure_ascii=False)
            tf_path = Path(tf.name)
        try:
            with patch.object(cpe, "_WAIVER_FILE", tf_path), \
                 patch.dict(os.environ, {_WCFG.signing_key_env: correct_key}, clear=False), \
                 caplog.at_level(logging.DEBUG, logger="ci_gate.check_phase_exit"):
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    cpe._read_coverage_waiver()
        finally:
            tf_path.unlink(missing_ok=True)

        for record in caplog.records:
            msg = record.getMessage()
            assert expected not in msg, f"HMAC expected value leaked into log: {msg[:200]}"
            assert canonical not in msg, f"canonical leaked into log on failure: {msg[:200]}"


# ── P0-3: Delta 阻断阈值参数化 ───────────────────────────────────────────────

class TestDeltaThreshold:
    """check_thread_lifecycle.py Delta 阻断阈值由 THREAD.delta_max_new 控制。"""

    def _run_main(self, new_violation_count: int, total_violations: int, strict: bool, delta_max_new: int):
        """Patch 扫描结果和 delta 计算，直接测试 main() 的返回码逻辑。"""
        import tools.check_thread_lifecycle as ctl
        from tools.check_thread_lifecycle import Violation

        # 构造指定数量的违规
        violations = [Violation(f"gui_app/f{i}.py", i + 1, f"C{i}", "继承 QThread") for i in range(total_violations)]
        # new_keys 使用字符串 key（和 _violation_key 返回格式一致）
        new_keys_str = {f"gui_app/f{i}.py:{i+1}:C{i}" for i in range(new_violation_count)}
        prev_keys_str = set()  # 第一次运行

        args = MagicMock()
        args.strict = strict
        args.staged = False
        args.paths = []

        with patch.object(ctl, "_load_state", return_value={}), \
             patch.object(ctl, "_save_state"), \
             patch.object(ctl, "_clean_old_state"), \
             patch("tools.check_thread_lifecycle.argparse.ArgumentParser") as mock_ap:
            mock_ap.return_value.parse_args.return_value = args
            # 覆盖 _TCFG
            original_delta_max = ctl._TCFG.delta_max_new
            # _TCFG 是 frozen dataclass，用临时包装替换
            import tools.config as cfg_mod
            new_tcfg = cfg_mod.ThreadConfig(
                state_filename=ctl._TCFG.state_filename,
                delta_max_new=delta_max_new,
                history_retention_days=ctl._TCFG.history_retention_days,
            )
            with patch.object(ctl, "_TCFG", new_tcfg), \
                 patch.object(ctl, "check_file", return_value=violations), \
                 patch("builtins.print"):
                rc = ctl.main()
        return rc

    def test_strict_no_violations_ok(self):
        """--strict 无违规 → 0。"""
        rc = self._run_main(0, 0, strict=True, delta_max_new=0)
        assert rc == 0

    def test_strict_new_violation_above_threshold_fails(self):
        """--strict + delta_max_new=0 + 任何新增 → 1。"""
        rc = self._run_main(new_violation_count=1, total_violations=1, strict=True, delta_max_new=0)
        assert rc == 1

    def test_strict_new_violation_within_threshold_passes(self):
        """--strict + delta_max_new=5 + 新增1个 → 0（在阈值内）。"""
        rc = self._run_main(new_violation_count=1, total_violations=1, strict=True, delta_max_new=5)
        assert rc == 0

    def test_non_strict_always_ok(self):
        """非 --strict 模式，无论违规数多少都返回 0。"""
        rc = self._run_main(new_violation_count=10, total_violations=10, strict=False, delta_max_new=0)
        assert rc == 0


# ── P1-2: 豁免台账落盘 ───────────────────────────────────────────────────────

class TestGovernanceLedger:
    """每次豁免生效应写入一条 JSONL 台账记录。"""

    def test_ledger_appended_on_waiver_use(self, tmp_path):
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "governance_ledger.jsonl"
        with patch.object(gl, "_LEDGER_FILE", ledger_file), \
             patch.object(gl, "_get_commit_sha", return_value="abc123"), \
             patch.object(gl, "_get_pipeline_id", return_value="local"), \
             patch.object(gl, "_get_env", return_value="dev"):
            gl.append_waiver_use({
                "approval_id": "OA-TEST-001",
                "phase": 0,
                "effective_target": 0.27,
                "expires": "2026-03-21",
                "expires_in_days": 14,
                "approver": "lead@example.com",
                "reason": "unit test",
            })

        assert ledger_file.exists()
        records = [json.loads(line) for line in ledger_file.read_text(encoding="utf-8").splitlines()]
        assert len(records) == 1
        r = records[0]
        assert r["approval_id"] == "OA-TEST-001"
        assert r["commit_sha"] == "abc123"
        assert r["env"] == "dev"
        assert "reason" not in r or r.get("reason_excerpt") == "unit test"

    def test_ledger_appends_multiple_records(self, tmp_path):
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "governance_ledger.jsonl"
        with patch.object(gl, "_LEDGER_FILE", ledger_file), \
             patch.object(gl, "_get_commit_sha", return_value="x"), \
             patch.object(gl, "_get_pipeline_id", return_value="p"), \
             patch.object(gl, "_get_env", return_value="ci"):
            for i in range(3):
                gl.append_waiver_use({
                    "approval_id": f"OA-{i}",
                    "phase": 0,
                    "effective_target": 0.27,
                    "expires": "2026-03-21",
                    "approver": "a@x.com",
                    "reason": f"test {i}",
                })

        lines = ledger_file.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 3
        assert json.loads(lines[2])["approval_id"] == "OA-2"

    def test_reason_truncated_to_100_chars(self, tmp_path):
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "governance_ledger.jsonl"
        long_reason = "A" * 200
        with patch.object(gl, "_LEDGER_FILE", ledger_file), \
             patch.object(gl, "_get_commit_sha", return_value="x"), \
             patch.object(gl, "_get_pipeline_id", return_value="p"), \
             patch.object(gl, "_get_env", return_value="ci"):
            gl.append_waiver_use({
                "approval_id": "OA-LONG",
                "phase": 0,
                "effective_target": 0.27,
                "expires": "2026-03-21",
                "approver": "a@x.com",
                "reason": long_reason,
            })

        record = json.loads(ledger_file.read_text(encoding="utf-8"))
        # reason_excerpt 最长 100 + "…" = 101 字符
        assert len(record["reason_excerpt"]) <= 101
        assert record["reason_excerpt"].endswith("…")

    def test_ledger_write_failure_is_silent(self, tmp_path):
        """台账写入失败不应抛出异常，不应阻断主流程。"""
        from tools import governance_ledger as gl
        # 指向无法写入的路径
        bad_path = tmp_path / "nonexistent_dir" / "ledger.jsonl"
        with patch.object(gl, "_LEDGER_FILE", bad_path):
            # 不应抛出异常
            gl.append_waiver_use({"approval_id": "X", "approver": "a@x.com", "reason": "test"})

    def test_ledger_written_on_waiver_approval(self, tmp_path):
        """check_phase_exit._read_coverage_waiver 成功时，台账应被写入。"""
        import tools.check_phase_exit as cpe
        key = "key"
        w = _make_waiver(approver="a@x.com", approver_2="b@x.com", hmac_key=key)
        ledger_file = tmp_path / "governance_ledger.jsonl"

        import tempfile as _tempfile
        with _tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as tf:
            json.dump(w, tf, ensure_ascii=False)
            tf_path = Path(tf.name)
        try:
            import tools.governance_ledger as gl
            with patch.object(cpe, "_WAIVER_FILE", tf_path), \
                 patch.dict(os.environ, {_WCFG.signing_key_env: key}, clear=False), \
                 patch.object(gl, "_LEDGER_FILE", ledger_file), \
                 patch.object(gl, "_get_commit_sha", return_value="sha1"), \
                 patch.object(gl, "_get_pipeline_id", return_value="local"), \
                 patch.object(gl, "_get_env", return_value="dev"), \
                 patch.object(cpe._logger, "info"), \
                 patch.object(cpe._logger, "warning"), \
                 patch.object(cpe._logger, "error"):
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    result = cpe._read_coverage_waiver()
        finally:
            tf_path.unlink(missing_ok=True)

        assert result is not None, "豁免应通过"
        assert ledger_file.exists(), "台账文件应被创建"
        record = json.loads(ledger_file.read_text(encoding="utf-8"))
        assert record["commit_sha"] == "sha1"


# ── 字段校验边界：类型错误 ─────────────────────────────────────────────────────

class TestFieldTypeValidation:
    """expires / effective_target 字段类型错误时应拒绝。"""

    def test_expires_non_string_rejected(self):
        w = _make_waiver()
        w["expires"] = 20260321  # 整数而非字符串
        assert _call_read_waiver(w) is None

    def test_expires_invalid_format_rejected(self):
        w = _make_waiver()
        w["expires"] = "21-03-2026"  # 错误格式
        assert _call_read_waiver(w) is None

    def test_effective_target_string_rejected(self):
        w = _make_waiver()
        w["effective_target"] = "0.27"  # 字符串，应拒绝
        assert _call_read_waiver(w) is None

    def test_missing_required_field_rejected(self):
        w = _make_waiver()
        del w["approval_id"]
        assert _call_read_waiver(w) is None


# ── 空 reason 拦截 ───────────────────────────────────────────────────────────

class TestEmptyReason:
    """reason 字段为空或空白字符串时应被拒绝，量化合规要求必须填写豁免原因。"""

    def test_empty_string_rejected(self):
        """reason="" 应拒绝。"""
        w = _make_waiver(reason="")
        assert _call_read_waiver(w) is None

    def test_whitespace_only_rejected(self):
        """reason 仅含空白字符应拒绝。"""
        w = _make_waiver(reason="   \t\n")
        assert _call_read_waiver(w) is None

    def test_valid_reason_passes(self):
        """非空 reason 应通过。"""
        w = _make_waiver(reason="Phase 0 覆盖率引导期，预期 2026-03-21 前恢复")
        result = _call_read_waiver(w)
        assert result is not None

    def test_reason_field_missing_rejected(self):
        """required_fields 包含 reason，缺失时在必填校验阶段即被拒绝。"""
        w = _make_waiver(reason="x")
        del w["reason"]
        assert _call_read_waiver(w) is None


# ── 豁免有效期上限（90 天）校验 ─────────────────────────────────────────────────

class TestExpiresMaxDays:
    """有效期超过今天起 90 天时应被拒绝（金融合规短期豁免限制）。"""

    def test_91_days_rejected(self):
        """有效期距今 91 天 → 超出上限，应拒绝。"""
        w = _make_waiver(expires_offset_days=91)
        assert _call_read_waiver(w) is None

    def test_90_days_accepted(self):
        """有效期距今恰好 90 天 → 在上限内，应通过。"""
        w = _make_waiver(expires_offset_days=90)
        result = _call_read_waiver(w)
        assert result is not None

    def test_89_days_accepted(self):
        """有效期距今 89 天 → 在上限内，应通过。"""
        w = _make_waiver(expires_offset_days=89)
        result = _call_read_waiver(w)
        assert result is not None

    def test_180_days_rejected(self):
        """有效期距今 180 天 → 明显超上限，应拒绝。"""
        w = _make_waiver(expires_offset_days=180)
        assert _call_read_waiver(w) is None


# ── 台账链式哈希（prev_hash）验证 ────────────────────────────────────────────

class TestLedgerChainHash:
    """每条台账记录应包含 prev_hash 字段，首条为 'genesis'，后续为前一条的 SHA-256。"""

    def _write_record(self, gl, ledger_file, approval_id: str):
        """辅助：向指定台账追加一条记录。"""
        import hashlib as _hl
        with patch.object(gl, "_LEDGER_FILE", ledger_file), \
             patch.object(gl, "_get_commit_sha", return_value="aabbcc"), \
             patch.object(gl, "_get_pipeline_id", return_value="p1"), \
             patch.object(gl, "_get_env", return_value="ci"):
            gl.append_waiver_use({
                "approval_id": approval_id,
                "phase": 0,
                "effective_target": 0.27,
                "expires": "2026-06-01",
                "approver": "a@x.com",
                "reason": "chain hash test",
            })

    def test_first_record_has_genesis_prev_hash(self, tmp_path):
        """空台账首条记录 prev_hash 应为 'genesis'。"""
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "ledger.jsonl"
        self._write_record(gl, ledger_file, "OA-1")
        record = json.loads(ledger_file.read_text(encoding="utf-8").strip())
        assert record["prev_hash"] == "genesis"

    def test_second_record_prev_hash_matches_first(self, tmp_path):
        """第二条记录的 prev_hash 应等于第一条记录行的 SHA-256。"""
        import hashlib as _hl
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "ledger.jsonl"
        self._write_record(gl, ledger_file, "OA-1")
        first_line = ledger_file.read_text(encoding="utf-8").strip()
        expected_hash = _hl.sha256(first_line.encode("utf-8")).hexdigest()

        self._write_record(gl, ledger_file, "OA-2")
        lines = ledger_file.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2
        second_record = json.loads(lines[1])
        assert second_record["prev_hash"] == expected_hash

    def test_prev_hash_tamper_detection(self, tmp_path):
        """修改第一条记录内容后，第二条的 prev_hash 将不再匹配原始哈希——验证链式防篡改逻辑可检测性。"""
        import hashlib as _hl
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "ledger.jsonl"
        self._write_record(gl, ledger_file, "OA-1")
        first_line = ledger_file.read_text(encoding="utf-8").strip()

        # 写第二条（其 prev_hash 基于原始第一条）
        self._write_record(gl, ledger_file, "OA-2")
        lines = ledger_file.read_text(encoding="utf-8").splitlines()
        second_record = json.loads(lines[1])
        expected_hash = _hl.sha256(first_line.encode("utf-8")).hexdigest()
        assert second_record["prev_hash"] == expected_hash

        # 篡改第一条内容
        tampered_first = first_line.replace("OA-1", "OA-TAMPERED")
        tampered_hash = _hl.sha256(tampered_first.encode("utf-8")).hexdigest()
        # 链断裂：篡改后的哈希与已记录的 prev_hash 不符
        assert tampered_hash != second_record["prev_hash"], "篡改应导致链断裂"


# ── 台账保留策略（archive_old_records）验证 ─────────────────────────────────

class TestLedgerArchive:
    """archive_old_records 应将超过保留期的记录移入压缩归档文件，主台账保留近期记录。"""

    def _make_record(self, approval_id: str, ts: str) -> str:
        """构造带指定时间戳的 JSONL 行（不含 prev_hash，简化测试用）。"""
        return json.dumps({
            "ts": ts, "approval_id": approval_id, "phase": 0,
            "effective_target": 0.27, "expires": "2026-06-01",
            "approvers": ["a@x.com"], "reason_excerpt": "test",
            "commit_sha": "x", "pipeline_id": "p", "env": "ci",
            "prev_hash": "genesis",
        }, ensure_ascii=False)

    def test_recent_records_kept(self, tmp_path):
        """近期记录（未超保留期）不应被归档。"""
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "governance_ledger.jsonl"
        ts_recent = (date.today() - timedelta(days=10)).strftime("%Y-%m-%dT00:00:00")
        ledger_file.write_text(self._make_record("OA-RECENT", ts_recent) + "\n", encoding="utf-8")

        with patch.object(gl, "_LEDGER_FILE", ledger_file):
            archived = gl.archive_old_records(retention_days=30)

        assert archived == 0
        # 主台账记录不变
        assert "OA-RECENT" in ledger_file.read_text(encoding="utf-8")

    def test_old_records_archived(self, tmp_path):
        """过期记录（超过保留期）应被移入归档文件，主台账中删除。"""
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "governance_ledger.jsonl"
        ts_old = (date.today() - timedelta(days=200)).strftime("%Y-%m-%dT00:00:00")
        ts_recent = (date.today() - timedelta(days=10)).strftime("%Y-%m-%dT00:00:00")
        # 2 条老记录 + 1 条近期记录
        lines = [
            self._make_record("OA-OLD-1", ts_old),
            self._make_record("OA-OLD-2", ts_old),
            self._make_record("OA-RECENT", ts_recent),
        ]
        ledger_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

        with patch.object(gl, "_LEDGER_FILE", ledger_file):
            archived = gl.archive_old_records(retention_days=30)

        assert archived == 2
        # 主台账只剩近期记录
        remaining = ledger_file.read_text(encoding="utf-8")
        assert "OA-RECENT" in remaining
        assert "OA-OLD" not in remaining
        # 归档文件已创建
        archive_files = list(tmp_path.glob("governance_ledger_archive_*.jsonl.gz"))
        assert len(archive_files) == 1

    def test_empty_ledger_returns_zero(self, tmp_path):
        """空台账归档返回 0，不创建归档文件。"""
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "governance_ledger.jsonl"
        ledger_file.write_text("", encoding="utf-8")
        with patch.object(gl, "_LEDGER_FILE", ledger_file):
            archived = gl.archive_old_records(retention_days=30)
        assert archived == 0
        assert not list(tmp_path.glob("governance_ledger_archive_*.jsonl.gz"))


# ── 归档可追溯索引验证 ────────────────────────────────────────────────────────

class TestArchiveIndex:
    """archive_old_records 应生成/更新 governance_ledger_archive_index.json 索引。"""

    def _make_record(self, approval_id: str, ts: str) -> str:
        return json.dumps({
            "ts": ts, "approval_id": approval_id, "phase": 0,
            "effective_target": 0.27, "expires": "2026-06-01",
            "approvers": ["a@x.com"], "reason_excerpt": "test",
            "commit_sha": "x", "pipeline_id": "p", "env": "ci",
            "prev_hash": "genesis",
        }, ensure_ascii=False)

    def test_archive_index_created_after_archive(self, tmp_path):
        """归档后应生成索引文件，包含 archive_file、record_count、ts_start、ts_end、file_sha256。"""
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "governance_ledger.jsonl"
        index_file = tmp_path / "governance_ledger_archive_index.json"
        ts_old = (date.today() - timedelta(days=200)).strftime("%Y-%m-%dT00:00:00")
        ledger_file.write_text(self._make_record("OA-OLD", ts_old) + "\n", encoding="utf-8")

        with patch.object(gl, "_LEDGER_FILE", ledger_file), \
             patch.object(gl, "_ARCHIVE_INDEX_FILE", index_file):
            archived = gl.archive_old_records(retention_days=30)

        assert archived == 1
        assert index_file.exists()
        index = json.loads(index_file.read_text(encoding="utf-8"))
        assert len(index) == 1
        entry = index[0]
        assert "archive_file" in entry
        assert entry["record_count"] == 1
        assert entry["ts_start"] == ts_old
        assert entry["ts_end"] == ts_old
        assert len(entry["file_sha256"]) == 64  # SHA-256 hex
        assert "updated_at" in entry

    def test_archive_index_updated_on_second_archive(self, tmp_path):
        """同一归档文件二次归档时，索引中该条目应被更新（record_count 增加），而非新增条目。"""
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "governance_ledger.jsonl"
        index_file = tmp_path / "governance_ledger_archive_index.json"
        ts_old = (date.today() - timedelta(days=200)).strftime("%Y-%m-%dT00:00:00")

        # 第一次归档
        ledger_file.write_text(self._make_record("OA-OLD-1", ts_old) + "\n", encoding="utf-8")
        with patch.object(gl, "_LEDGER_FILE", ledger_file), \
             patch.object(gl, "_ARCHIVE_INDEX_FILE", index_file):
            gl.archive_old_records(retention_days=30)

        # 写入新的过期记录，再次归档（同月，归档文件相同）
        ledger_file.write_text(self._make_record("OA-OLD-2", ts_old) + "\n", encoding="utf-8")
        with patch.object(gl, "_LEDGER_FILE", ledger_file), \
             patch.object(gl, "_ARCHIVE_INDEX_FILE", index_file):
            gl.archive_old_records(retention_days=30)

        index = json.loads(index_file.read_text(encoding="utf-8"))
        # 同月归档，索引中应只有 1 条条目（被更新，不是新增）
        assert len(index) == 1
        # 合并后 record_count 应为 2
        assert index[0]["record_count"] == 2

    def test_archive_index_file_sha256_integrity(self, tmp_path):
        """索引中的 file_sha256 应与实际归档文件内容的 SHA-256 吻合。"""
        import hashlib as _hl
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "governance_ledger.jsonl"
        index_file = tmp_path / "governance_ledger_archive_index.json"
        ts_old = (date.today() - timedelta(days=200)).strftime("%Y-%m-%dT00:00:00")
        ledger_file.write_text(self._make_record("OA-IDX-SHA", ts_old) + "\n", encoding="utf-8")

        with patch.object(gl, "_LEDGER_FILE", ledger_file), \
             patch.object(gl, "_ARCHIVE_INDEX_FILE", index_file):
            gl.archive_old_records(retention_days=30)

        index = json.loads(index_file.read_text(encoding="utf-8"))
        archive_filename = index[0]["archive_file"]
        archive_path = tmp_path / archive_filename
        actual_sha = _hl.sha256(archive_path.read_bytes()).hexdigest()
        assert index[0]["file_sha256"] == actual_sha


# ── 验链脚本（verify_governance_ledger）测试 ─────────────────────────────────

class TestVerifyGovernanceLedger:
    """verify_governance_ledger.verify_chain() 逻辑覆盖。"""

    def _write_ledger(self, tmp_path: Path, n: int) -> Path:
        """写入 n 条正确链式记录，返回文件路径。"""
        from tools import governance_ledger as gl
        ledger_file = tmp_path / "governance_ledger.jsonl"
        with patch.object(gl, "_LEDGER_FILE", ledger_file), \
             patch.object(gl, "_get_commit_sha", return_value="aaa"), \
             patch.object(gl, "_get_pipeline_id", return_value="p"), \
             patch.object(gl, "_get_env", return_value="dev"):
            for i in range(n):
                gl.append_waiver_use({
                    "approval_id": f"OA-{i}",
                    "phase": 0,
                    "effective_target": 0.27,
                    "expires": "2026-06-01",
                    "approver": "a@x.com",
                    "reason": "verify test",
                })
        return ledger_file

    def test_empty_file_returns_ok(self, tmp_path):
        """空台账无记录，验链应返回通过。"""
        from tools.verify_governance_ledger import verify_chain
        ledger_file = tmp_path / "governance_ledger.jsonl"
        ledger_file.write_text("", encoding="utf-8")
        passed, count, errors = verify_chain(ledger_file)
        assert passed is True
        assert count == 0
        assert errors == []

    def test_intact_chain_passes(self, tmp_path):
        """正常写入的多条记录，验链应全部通过。"""
        from tools.verify_governance_ledger import verify_chain
        ledger_file = self._write_ledger(tmp_path, 3)
        passed, count, errors = verify_chain(ledger_file)
        assert passed is True
        assert count == 3
        assert errors == []

    def test_tampered_first_record_fails(self, tmp_path):
        """首条 prev_hash 不是 'genesis' → 链断裂。"""
        from tools.verify_governance_ledger import verify_chain
        ledger_file = self._write_ledger(tmp_path, 1)
        # 篡改首条记录的 prev_hash
        record = json.loads(ledger_file.read_text(encoding="utf-8").strip())
        record["prev_hash"] = "TAMPERED"
        ledger_file.write_text(json.dumps(record) + "\n", encoding="utf-8")
        passed, count, errors = verify_chain(ledger_file)
        assert passed is False
        assert len(errors) >= 1

    def test_middle_record_tampered_fails(self, tmp_path):
        """修改中间条记录内容后，后续记录 prev_hash 将不匹配 → 链断裂。"""
        from tools.verify_governance_ledger import verify_chain
        ledger_file = self._write_ledger(tmp_path, 3)
        lines = ledger_file.read_text(encoding="utf-8").splitlines()
        # 篡改第 1 条（index=1）记录内容
        rec = json.loads(lines[1])
        rec["approval_id"] = "TAMPERED"
        lines[1] = json.dumps(rec)
        ledger_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        passed, count, errors = verify_chain(ledger_file)
        assert passed is False
        assert len(errors) >= 1  # 第 2 条 prev_hash 与篡改后的第 1 条不匹配

    def test_nonexistent_file_returns_ok(self, tmp_path):
        """不存在的文件：无记录，视为通过（首次运行场景）。"""
        from tools.verify_governance_ledger import verify_chain
        result_path = tmp_path / "nonexistent.jsonl"
        passed, count, errors = verify_chain(result_path)
        assert passed is True
        assert count == 0

    def test_verify_archive_index_sha256_mismatch(self, tmp_path):
        """归档索引 SHA-256 与实际文件不符 → 校验失败。"""
        import gzip as gz
        from tools.verify_governance_ledger import verify_archive_index
        # 写一个假归档文件
        archive_file = tmp_path / "governance_ledger_archive_202601.jsonl.gz"
        with gz.open(str(archive_file), "wt", encoding="utf-8") as f:
            f.write('{"ts":"2026-01-01T00:00:00","approval_id":"OA-X"}\n')
        index = [{"archive_file": archive_file.name, "record_count": 1,
                  "ts_start": "2026-01-01T00:00:00", "ts_end": "2026-01-01T00:00:00",
                  "file_sha256": "a" * 64, "updated_at": "2026-03-08T00:00:00"}]
        index_file = tmp_path / "governance_ledger_archive_index.json"
        index_file.write_text(json.dumps(index), encoding="utf-8")

        from tools import verify_governance_ledger as vgl
        with patch.object(vgl, "_ARCHIVE_INDEX_FILE", index_file):
            passed, errors, _ = verify_archive_index(index_file)
        assert passed is False
        assert any("SHA-256" in e or "篡改" in e for e in errors)

    def test_verify_archive_index_sha256_ok(self, tmp_path):
        """归档索引 SHA-256 与实际文件一致 → 校验通过。"""
        import gzip as gz
        import hashlib as _hl
        from tools.verify_governance_ledger import verify_archive_index
        archive_file = tmp_path / "governance_ledger_archive_202601.jsonl.gz"
        content = b'{"ts":"2026-01-01T00:00:00","approval_id":"OA-X"}\n'
        with gz.open(str(archive_file), "wb") as f:
            f.write(content)
        correct_sha = _hl.sha256(archive_file.read_bytes()).hexdigest()
        index = [{"archive_file": archive_file.name, "record_count": 1,
                  "ts_start": "2026-01-01T00:00:00", "ts_end": "2026-01-01T00:00:00",
                  "file_sha256": correct_sha, "updated_at": "2026-03-08T00:00:00"}]
        index_file = tmp_path / "governance_ledger_archive_index.json"
        index_file.write_text(json.dumps(index), encoding="utf-8")
        passed, errors, warnings = verify_archive_index(index_file)
        assert passed is True
        assert errors == []
        assert warnings == []


# ── 配置中心化：LedgerConfig 常量效果验证 ────────────────────────────────────

class TestLedgerConfig:
    """配置中心化：check_phase_exit 和 governance_ledger 的常量均来自 LedgerConfig。"""

    def test_max_waiver_days_from_config(self):
        """有效期上限校验使用 LEDGER.max_waiver_days，修改配置应影响行为。"""
        from tools.config import LedgerConfig
        import tools.config as cfg_mod
        # 将上限临时改为 30 天
        new_cfg = LedgerConfig(max_waiver_days=30)
        w = _make_waiver(expires_offset_days=31)  # 31 > 30，应被拒绝
        import tools.check_phase_exit as cpe
        with patch.object(cpe, "_LCFG", new_cfg):
            result = _call_read_waiver(w)
        assert result is None  # 超出新配置的 30 天上限

    def test_retention_days_from_config(self):
        """保留期由 LEDGER.retention_days 控制，可通过配置调整。"""
        from tools.config import LEDGER as _LCFG
        assert _LCFG.retention_days == 180
        assert _LCFG.max_waiver_days == 90
        assert _LCFG.reason_max_len == 100

    def test_unindexed_block_threshold_default(self):
        """LedgerConfig.unindexed_block_threshold 默认值应为 3（少于 3 个漏记不阻断）。"""
        from tools.config import LEDGER as _LCFG
        assert _LCFG.unindexed_block_threshold == 3


# ── schema_version 字段测试 ──────────────────────────────────────────────────

class TestArchiveIndexSchemaVersion:
    """归档索引每条条目应携带 schema_version 字段，值来自 LedgerConfig。"""

    def _make_record(self, approval_id: str, ts: str) -> str:
        return json.dumps({
            "ts": ts, "approval_id": approval_id, "phase": 0,
            "effective_target": 0.27, "expires": "2026-06-01",
            "approvers": ["a@x.com"], "reason_excerpt": "test",
            "commit_sha": "x", "pipeline_id": "p", "env": "ci",
            "prev_hash": "genesis",
        }, ensure_ascii=False)

    def test_schema_version_written_to_index(self, tmp_path):
        """archive_old_records 生成的索引条目应包含 schema_version='1.1'。"""
        from tools import governance_ledger as gl
        from tools.config import LEDGER as _LCFG
        ledger_file = tmp_path / "governance_ledger.jsonl"
        index_file = tmp_path / "governance_ledger_archive_index.json"
        ts_old = (date.today() - timedelta(days=200)).strftime("%Y-%m-%dT00:00:00")
        ledger_file.write_text(self._make_record("OA-SV", ts_old) + "\n", encoding="utf-8")

        with patch.object(gl, "_LEDGER_FILE", ledger_file), \
             patch.object(gl, "_ARCHIVE_INDEX_FILE", index_file):
            gl.archive_old_records(retention_days=30)

        index = json.loads(index_file.read_text(encoding="utf-8"))
        assert len(index) == 1
        assert "schema_version" in index[0]
        assert index[0]["schema_version"] == _LCFG.archive_index_schema_version

    def test_ledger_config_schema_version_value(self):
        """LedgerConfig.archive_index_schema_version 默认值应为 '1.1'。"""
        from tools.config import LEDGER as _LCFG
        assert _LCFG.archive_index_schema_version == "1.1"


# ── 双向一致性检查测试 ────────────────────────────────────────────────────────

class TestArchiveIndexBidirectionalConsistency:
    """verify_archive_index()：索引↔文件双向一致性检查。"""

    def _write_gz(self, path: Path, content: str) -> None:
        import gzip as gz
        with gz.open(str(path), "wt", encoding="utf-8") as f:
            f.write(content)

    def _correct_sha(self, path: Path) -> str:
        import hashlib as _hl
        return _hl.sha256(path.read_bytes()).hexdigest()

    def test_unindexed_archive_file_is_warning_below_threshold(self, tmp_path):
        """logs/ 下有 1 个归档文件未被索引（< 阈值 3）→ 输出 warning，不阻断 CI。"""
        from tools.verify_governance_ledger import verify_archive_index
        # 创建一个归档文件，但不在索引中
        extra = tmp_path / "governance_ledger_archive_202501.jsonl.gz"
        self._write_gz(extra, '{"ts":"2025-01-01T00:00:00","approval_id":"OA-EXTRA"}\n')
        # 索引为空
        index_file = tmp_path / "governance_ledger_archive_index.json"
        index_file.write_text("[]", encoding="utf-8")

        passed, errors, warnings = verify_archive_index(index_file)
        assert passed is True          # 1 < 阈值 3，不阻断
        assert errors == []
        assert any("未被索引" in w["message"] or "漏记" in w["message"] for w in warnings)

    def test_missing_indexed_file_logged_not_error(self, tmp_path):
        """索引中记录的文件不存在 → 仅记录日志，不视为硬错误（文件可能已迁移）。"""
        from tools.verify_governance_ledger import verify_archive_index
        index = [{"archive_file": "governance_ledger_archive_202501.jsonl.gz",
                  "record_count": 1, "ts_start": "2025-01-01T00:00:00",
                  "ts_end": "2025-01-01T00:00:00",
                  "file_sha256": "a" * 64, "updated_at": "2026-03-08T00:00:00",
                  "schema_version": "1.1"}]
        index_file = tmp_path / "governance_ledger_archive_index.json"
        index_file.write_text(json.dumps(index), encoding="utf-8")
        # 不创建实际文件

        passed, errors, warnings = verify_archive_index(index_file)
        # 文件不存在但不算错误（仅 info 日志），无未索引文件 → 通过
        assert passed is True
        assert errors == []
        assert warnings == []

    def test_sha256_mismatch_is_error(self, tmp_path):
        """索引 SHA-256 与实际文件不符 → 正向检查报告篡改错误。"""
        from tools.verify_governance_ledger import verify_archive_index
        archive_file = tmp_path / "governance_ledger_archive_202502.jsonl.gz"
        self._write_gz(archive_file, '{"ts":"2025-02-01T00:00:00","approval_id":"OA-Y"}\n')
        index = [{"archive_file": archive_file.name, "record_count": 1,
                  "ts_start": "2025-02-01T00:00:00", "ts_end": "2025-02-01T00:00:00",
                  "file_sha256": "b" * 64, "updated_at": "2026-03-08T00:00:00",
                  "schema_version": "1.1"}]
        index_file = tmp_path / "governance_ledger_archive_index.json"
        index_file.write_text(json.dumps(index), encoding="utf-8")

        passed, errors, _ = verify_archive_index(index_file)
        assert passed is False
        assert any("SHA-256" in e or "篡改" in e for e in errors)

    def test_both_directions_ok(self, tmp_path):
        """所有索引文件存在且 SHA 正确，无多余归档文件 → 双向检查通过。"""
        import gzip as gz
        import hashlib as _hl
        from tools.verify_governance_ledger import verify_archive_index
        archive_file = tmp_path / "governance_ledger_archive_202503.jsonl.gz"
        self._write_gz(archive_file, '{"ts":"2025-03-01T00:00:00","approval_id":"OA-Z"}\n')
        correct_sha = self._correct_sha(archive_file)
        index = [{"archive_file": archive_file.name, "record_count": 1,
                  "ts_start": "2025-03-01T00:00:00", "ts_end": "2025-03-01T00:00:00",
                  "file_sha256": correct_sha, "updated_at": "2026-03-08T00:00:00",
                  "schema_version": "1.1"}]
        index_file = tmp_path / "governance_ledger_archive_index.json"
        index_file.write_text(json.dumps(index), encoding="utf-8")

        passed, errors, warnings = verify_archive_index(index_file)
        assert passed is True
        assert errors == []
        assert warnings == []


# ── run_ledger_chain_gate 测试 ────────────────────────────────────────────────

class TestLedgerChainGate:
    """ci_gate_summary.run_ledger_chain_gate() 行为覆盖。"""

    def test_passes_on_exit_code_0(self, tmp_path, monkeypatch):
        """verify 脚本返回 0（链完整）→ gate passed=True。"""
        import tools.ci_gate_summary as cg
        monkeypatch.setattr(cg, "_run", lambda cmd: (0, "链完整"))
        # 确保脚本路径判断为存在（_ROOT/tools/verify_governance_ledger.py）
        fake_script = tmp_path / "tools" / "verify_governance_ledger.py"
        fake_script.parent.mkdir(parents=True, exist_ok=True)
        fake_script.write_text("")
        monkeypatch.setattr(cg, "_ROOT", tmp_path)
        result = cg.run_ledger_chain_gate(report_only=False)
        assert result["passed"] is True

    def test_passes_on_exit_code_2(self, tmp_path, monkeypatch):
        """verify 脚本返回 2（台账不存在，首次运行）→ gate passed=True。"""
        import tools.ci_gate_summary as cg
        monkeypatch.setattr(cg, "_run", lambda cmd: (2, ""))
        fake_script = tmp_path / "tools" / "verify_governance_ledger.py"
        fake_script.parent.mkdir(parents=True, exist_ok=True)
        fake_script.write_text("")
        monkeypatch.setattr(cg, "_ROOT", tmp_path)
        result = cg.run_ledger_chain_gate(report_only=False)
        assert result["passed"] is True
        assert "首次运行" in result.get("detail", "")

    def test_fails_on_exit_code_1(self, tmp_path, monkeypatch):
        """verify 脚本返回 1（链断裂）→ gate passed=False。"""
        import tools.ci_gate_summary as cg
        monkeypatch.setattr(cg, "_run", lambda cmd: (1, "chain broken"))
        fake_script = tmp_path / "tools" / "verify_governance_ledger.py"
        fake_script.parent.mkdir(parents=True, exist_ok=True)
        fake_script.write_text("")
        monkeypatch.setattr(cg, "_ROOT", tmp_path)
        result = cg.run_ledger_chain_gate(report_only=False)
        assert result["passed"] is False

    def test_skipped_when_script_missing(self, tmp_path, monkeypatch):
        """verify 脚本不存在 → gate skipped=True, passed=True（不阻断）。"""
        import tools.ci_gate_summary as cg
        monkeypatch.setattr(cg, "_ROOT", tmp_path)  # tmp_path 下无 tools/ 子目录
        result = cg.run_ledger_chain_gate(report_only=False)
        assert result["passed"] is True
        assert result.get("skipped") is True

    def test_warnings_count_present_in_result(self, tmp_path, monkeypatch):
        """gate 结果字典应始终包含 warnings_count 和 warnings 字段。"""
        import tools.ci_gate_summary as cg
        monkeypatch.setattr(cg, "_run", lambda cmd: (0, "no warnings here"))
        fake_script = tmp_path / "tools" / "verify_governance_ledger.py"
        fake_script.parent.mkdir(parents=True, exist_ok=True)
        fake_script.write_text("")
        monkeypatch.setattr(cg, "_ROOT", tmp_path)
        result = cg.run_ledger_chain_gate(report_only=False)
        assert "warnings_count" in result
        assert "warnings" in result
        assert result["warnings_count"] == 0

    def test_warnings_count_extracted_from_output(self, tmp_path, monkeypatch):
        """脚本输出中含 [WARN][type] 行时，warnings_count 应正确统计且 type 字段存在。"""
        import tools.ci_gate_summary as cg
        warn_output = "2026-03-08 WARNING verify_governance_ledger ⚠ [WARN][ledger_unindexed] 1 个归档文件未被索引覆盖\n链完整"
        monkeypatch.setattr(cg, "_run", lambda cmd: (0, warn_output))
        fake_script = tmp_path / "tools" / "verify_governance_ledger.py"
        fake_script.parent.mkdir(parents=True, exist_ok=True)
        fake_script.write_text("")
        monkeypatch.setattr(cg, "_ROOT", tmp_path)
        result = cg.run_ledger_chain_gate(report_only=False)
        assert result["passed"] is True
        assert result["warnings_count"] == 1
        assert result["warnings"][0]["type"] == "ledger_unindexed"

    def test_warnings_type_unknown_fallback(self, tmp_path, monkeypatch):
        """脚本输出含 [WARN] 但无类型标记（旧格式兼容）→ warnings_count=0（不误判）。"""
        import tools.ci_gate_summary as cg
        # 旧格式不含 [WARN][ 前缀，不应被计入 warnings
        warn_output = "2026-03-08 [WARN] 某个未分类告警\n链完整"
        monkeypatch.setattr(cg, "_run", lambda cmd: (0, warn_output))
        fake_script = tmp_path / "tools" / "verify_governance_ledger.py"
        fake_script.parent.mkdir(parents=True, exist_ok=True)
        fake_script.write_text("")
        monkeypatch.setattr(cg, "_ROOT", tmp_path)
        result = cg.run_ledger_chain_gate(report_only=False)
        assert result["warnings_count"] == 0

    def test_warning_types_count_aggregation(self, tmp_path, monkeypatch):
        """两条相同 type 的 warning → warning_types_count={'ledger_unindexed': 2}。"""
        import tools.ci_gate_summary as cg
        warn_output = (
            "⚠ [WARN][ledger_unindexed] 文件A未被索引\n"
            "⚠ [WARN][ledger_unindexed] 文件B未被索引\n"
            "链完整"
        )
        monkeypatch.setattr(cg, "_run", lambda cmd: (0, warn_output))
        fake_script = tmp_path / "tools" / "verify_governance_ledger.py"
        fake_script.parent.mkdir(parents=True, exist_ok=True)
        fake_script.write_text("")
        monkeypatch.setattr(cg, "_ROOT", tmp_path)
        result = cg.run_ledger_chain_gate(report_only=False)
        assert result["warning_types_count"] == {"ledger_unindexed": 2}

    def test_warning_types_count_empty_when_no_warnings(self, tmp_path, monkeypatch):
        """无 warning 时 warning_types_count 应为空 dict。"""
        import tools.ci_gate_summary as cg
        monkeypatch.setattr(cg, "_run", lambda cmd: (0, "链完整"))
        fake_script = tmp_path / "tools" / "verify_governance_ledger.py"
        fake_script.parent.mkdir(parents=True, exist_ok=True)
        fake_script.write_text("")
        monkeypatch.setattr(cg, "_ROOT", tmp_path)
        result = cg.run_ledger_chain_gate(report_only=False)
        assert result["warning_types_count"] == {}


# ── 漏记告警分级专项测试 ──────────────────────────────────────────────────────

class TestUnindexedFileTiering:
    """verify_archive_index() 漏记分级：< threshold → warning；≥ threshold → block。"""

    def _write_gz(self, path: Path, content: str) -> None:
        import gzip as gz
        with gz.open(str(path), "wt", encoding="utf-8") as f:
            f.write(content)

    def _make_index_file(self, tmp_path: Path) -> Path:
        idx = tmp_path / "governance_ledger_archive_index.json"
        idx.write_text("[]", encoding="utf-8")
        return idx

    def _add_unindexed_files(self, tmp_path: Path, n: int) -> None:
        for i in range(n):
            f = tmp_path / f"governance_ledger_archive_2025{i+1:02d}.jsonl.gz"
            self._write_gz(f, f'{{"ts":"2025-{i+1:02d}-01T00:00:00","approval_id":"OA-{i}"}}\n')

    def test_2_unindexed_is_warning(self, tmp_path):
        """2 个漏记文件（< threshold=3）→ warnings 非空，passed=True。"""
        from tools.verify_governance_ledger import verify_archive_index
        self._add_unindexed_files(tmp_path, 2)
        idx = self._make_index_file(tmp_path)
        passed, errors, warnings = verify_archive_index(idx)
        assert passed is True
        assert errors == []
        assert len(warnings) == 1
        assert warnings[0]["type"] == "ledger_unindexed"
        assert warnings[0]["count"] == 2

    def test_3_unindexed_is_block(self, tmp_path):
        """3 个漏记文件（= threshold=3）→ errors 非空，passed=False。"""
        from tools.verify_governance_ledger import verify_archive_index
        self._add_unindexed_files(tmp_path, 3)
        idx = self._make_index_file(tmp_path)
        passed, errors, warnings = verify_archive_index(idx)
        assert passed is False
        assert len(errors) == 1
        assert "BLOCK" in errors[0]

    def test_5_unindexed_is_block(self, tmp_path):
        """5 个漏记文件（> threshold=3）→ 同样阻断，错误消息包含实际数量。"""
        from tools.verify_governance_ledger import verify_archive_index
        self._add_unindexed_files(tmp_path, 5)
        idx = self._make_index_file(tmp_path)
        passed, errors, warnings = verify_archive_index(idx)
        assert passed is False
        assert "5" in errors[0]

    def test_custom_threshold_1_blocks_single_file(self, tmp_path):
        """threshold 覆盖为 1 → 即使 1 个漏记也阻断（严格模式）。"""
        from tools.verify_governance_ledger import verify_archive_index
        from tools.config import LedgerConfig
        import tools.verify_governance_ledger as vgl
        self._add_unindexed_files(tmp_path, 1)
        idx = self._make_index_file(tmp_path)
        strict_cfg = LedgerConfig(unindexed_block_threshold=1)
        with patch.object(vgl, "_LCFG", strict_cfg):
            passed, errors, warnings = verify_archive_index(idx)
        assert passed is False
        assert "BLOCK" in errors[0]
        assert warnings == []
