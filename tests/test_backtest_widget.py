import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from PyQt5.QtWidgets import QApplication, QPushButton

from gui_app.widgets.backtest_widget import BacktestWidget, BacktestWorker


def test_on_chart_data_loaded_updates_symbol(qapp):
    widget = BacktestWidget()
    try:
        widget.stock_code_edit.setText("600000.SH")
        widget.on_chart_data_loaded("000001.SZ")
        assert widget.stock_code_edit.text() == "000001.SZ"
    finally:
        widget.close()


def test_on_chart_data_loaded_ignores_empty(qapp):
    widget = BacktestWidget()
    try:
        widget.stock_code_edit.setText("600000.SH")
        widget.on_chart_data_loaded("")
        assert widget.stock_code_edit.text() == "600000.SH"
    finally:
        widget.close()


def test_strategy_registry_contains_factor_workflow_strategy(qapp):
    widget = BacktestWidget()
    try:
        registry = widget.strategy_registry
        assert "101因子工作流策略" in registry
        cfg = registry["101因子工作流策略"]
        params = cfg.get("params", [])
        assert len(params) == 2
        assert params[0].get("key") == "workflow_path"
        assert params[0].get("type") == "path"
        assert params[1].get("key") == "workflow_node_overrides"
        assert params[1].get("type") == "json"
    finally:
        widget.close()


def test_update_config_summary_contains_factor_context(qapp):
    widget = BacktestWidget()
    try:
        params = widget.get_backtest_parameters()
        params["strategy_name"] = "101因子工作流策略"
        factor_context = {
            "ic_summary": {"ic_mean": 0.02, "ic_ir": 0.8, "ic_prob": 0.6},
            "workflow_meta": {"name": "wf_demo", "node_count": 5, "connection_count": 4},
            "workflow_nodes": [{"type": "data_loader", "params": {"symbols": ["000001.SZ"]}}],
        }
        widget.update_config_summary(params, factor_context)
        text = widget.config_summary_text.toPlainText()
        assert "IC摘要" in text
        assert "工作流: wf_demo" in text
        assert "节点参数摘要" in text
    finally:
        widget.close()


def test_read_workflow_node_templates(qapp):
    widget = BacktestWidget()
    tmp = ""
    try:
        payload = {
            "nodes": {
                "a": {"node_type": "data_loader", "params": {"symbols": ["000001.SZ"], "start_date": "2025-01-01"}},
                "b": {"node_type": "backtester", "params": {"n_quantiles": 5, "top_quantile": 0.2}},
            }
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
            tmp = f.name
        templates = widget._read_workflow_node_templates(tmp)
        assert "data_loader" in templates
        assert "backtester" in templates
        assert templates["backtester"]["n_quantiles"] == 5
    finally:
        try:
            if tmp:
                os.unlink(tmp)
        except Exception:
            pass
        widget.close()


def test_run_factor_scenario_compare(monkeypatch):
    worker = BacktestWorker({})

    def _fake_runner(**kwargs):
        ov = kwargs.get("node_overrides", {})
        q = float((ov.get("backtester", {}) or {}).get("n_quantiles", 0))
        return {
            "metrics": {
                "total_return": q / 100.0,
                "sharpe_ratio": q / 10.0,
                "max_drawdown": 0.1,
                "ic_mean": 0.02,
                "ic_ir": 0.7,
            }
        }

    monkeypatch.setattr(
        "gui_app.backtest.factor_workflow_bridge.run_factor_workflow_backtest",
        _fake_runner,
    )
    params = {
        "strategy_name": "101因子工作流策略",
        "stock_code": "000001.SZ",
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "strategy_params": {
            "workflow_path": "101因子/101因子分析平台/workflow.json",
            "workflow_node_overrides": {
                "__scenarios__": [
                    {"name": "q5", "overrides": {"backtester": {"n_quantiles": 5}}},
                    {"name": "q10", "overrides": {"backtester": {"n_quantiles": 10}}},
                ]
            },
        },
    }
    rows = worker.run_factor_scenario_compare(params)
    assert len(rows) == 2
    assert rows[0]["params"] == "q5"
    assert rows[1]["params"] == "q10"
    assert rows[1]["total_return"] > rows[0]["total_return"]
    assert rows[0]["scenario_overrides"]["backtester"]["n_quantiles"] == 5


def test_copy_best_factor_scenario_params(qapp, monkeypatch):
    widget = BacktestWidget()
    try:
        widget.strategy_combo.setCurrentText("101因子工作流策略")
        popup_messages = []

        def _fake_info(*args, **kwargs):
            popup_messages.append((args, kwargs))
            return 0

        monkeypatch.setattr("gui_app.widgets.backtest_widget.QMessageBox.information", _fake_info)
        widget.current_results = {
            "factor_compare_results": [
                {
                    "params": "s1",
                    "score": 0.1,
                    "scenario_overrides": {"backtester": {"n_quantiles": 5}},
                },
                {
                    "params": "s2",
                    "score": 0.2,
                    "scenario_overrides": {"backtester": {"n_quantiles": 10}},
                },
            ]
        }
        widget.copy_best_factor_scenario_params()
        target = widget.strategy_param_widgets["101因子工作流策略"]["workflow_node_overrides"]
        text = target.toPlainText()
        payload = json.loads(text)
        assert payload["backtester"]["n_quantiles"] == 10
        cb = QApplication.clipboard()
        assert cb is not None
        assert '"n_quantiles": 10' in cb.text()
        assert popup_messages
    finally:
        widget.close()


def test_merge_and_extract_scenarios_helpers(qapp):
    widget = BacktestWidget()
    try:
        current = {"backtester": {"n_quantiles": 5}}
        rows = [
            {"name": "s1", "node_type": "backtester", "param_key": "n_quantiles", "param_value": "7"},
            {"name": "s1", "node_type": "ic_analyzer", "param_key": "periods", "param_value": "1"},
            {"name": "s2", "node_type": "backtester", "param_key": "n_quantiles", "param_value": "10"},
        ]
        merged = widget._merge_scenarios_into_overrides(current, rows)
        assert merged["backtester"]["n_quantiles"] == 5
        assert "__scenarios__" in merged
        flat = widget._extract_scenarios_from_overrides(merged)
        names = sorted(set(x["name"] for x in flat))
        assert names == ["s1", "s2"]
        s1_rows = [x for x in flat if x["name"] == "s1" and x["node_type"] == "backtester"]
        assert s1_rows
        assert s1_rows[0]["param_key"] == "n_quantiles"
    finally:
        widget.close()


def test_build_template_scenarios(qapp):
    widget = BacktestWidget()
    try:
        rows = widget._build_template_scenarios(
            node_type="backtester",
            param_key="n_quantiles",
            start_value=5,
            end_value=20,
            step_value=5,
            name_prefix="grid",
        )
        assert len(rows) == 4
        assert rows[0]["name"] == "grid_n_quantiles_5"
        assert rows[-1]["param_value"] == "20"
    finally:
        widget.close()


def test_build_template_scenarios_descending(qapp):
    widget = BacktestWidget()
    try:
        rows = widget._build_template_scenarios(
            node_type="ic_analyzer",
            param_key="periods",
            start_value=3,
            end_value=1,
            step_value=1,
            name_prefix="desc",
        )
        assert len(rows) == 3
        assert rows[0]["param_value"] == "3"
        assert rows[-1]["param_value"] == "1"
    finally:
        widget.close()


def test_template_preset_for_key(qapp):
    widget = BacktestWidget()
    try:
        q = widget._template_preset_for_key("quantile")
        assert q == ("backtester", "n_quantiles", 5, 20, 5, "q")
        ic = widget._template_preset_for_key("ic_period")
        assert ic == ("ic_analyzer", "periods", 1, 10, 1, "icp")
    finally:
        widget.close()


def test_combine_template_rows_replace_and_append(qapp):
    widget = BacktestWidget()
    try:
        existing = [{"name": "old", "node_type": "backtester", "param_key": "n_quantiles", "param_value": "5"}]
        generated = [{"name": "new", "node_type": "backtester", "param_key": "n_quantiles", "param_value": "10"}]
        replaced = widget._combine_template_rows(existing, generated, replace_mode=True)
        appended = widget._combine_template_rows(existing, generated, replace_mode=False)
        assert len(replaced) == 1
        assert replaced[0]["name"] == "new"
        assert len(appended) == 2
        assert appended[0]["name"] == "old"
        assert appended[1]["name"] == "new"
    finally:
        widget.close()


def test_click_node_editor_button_does_not_bind_bool_as_textedit(qapp, monkeypatch):
    widget = BacktestWidget()
    try:
        widget.strategy_combo.setCurrentText("101因子工作流策略")
        monkeypatch.setattr(widget, "_resolve_workflow_path_for_ui", lambda _: "dummy.json")
        monkeypatch.setattr(
            widget,
            "_read_workflow_node_templates",
            lambda _: {"backtester": {"n_quantiles": 5}},
        )
        monkeypatch.setattr("gui_app.widgets.backtest_widget.QDialog.exec_", lambda self: 0)
        buttons = [b for b in widget.findChildren(QPushButton) if b.text() == "节点编辑"]
        assert buttons
        buttons[0].click()
    finally:
        widget.close()


# =============================================================================
# 纯静态映射契约（零 Qt 依赖）
#
# 检验三层链路的文字一致性：
#   adjust_combo.addItems()  ←→  ADJUST_TEXT_MAP key   ←→  engine.adjust_map value
#   period_combo.addItems()  ←→  PERIOD_TEXT_MAP key   ←→  已知 canonical 周期集合
#
# 任何 UI 标签改动、任意一层 fallback 触发，以下任意一条失败。
# =============================================================================

# 与 backtest_widget.py addItems() 互为镜像（双向约束）
_ADJUST_ITEMS_IN_UI = [
    "不复权 (原始价格)",
    "前复权 (短期回测)",
    "后复权 (长期回测)",
]
_PERIOD_ITEMS_IN_UI = [
    "日线(1d)",
    "60分钟(1h)",
    "30分钟(30m)",
    "15分钟(15m)",
    "5分钟(5m)",
    "1分钟(1m)",
]
# gui_app/backtest/engine.py _run_native_backtest() 内 adjust_map 的接受键集合——动态读取，
# 避免 engine.py 改了 map 后此测试仍用过时的硬编码集合静默通过。
def _live_engine_adjust_accepts() -> frozenset[str]:
    """从 engine.py 源码动态读取 _run_native_backtest() 内的 adjust_map，返回其全部 key 的 frozenset。

    范围限定在 _run_native_backtest 函数体内，避免同名局部变量误命中。
    """
    import ast
    import pathlib

    src = (pathlib.Path(__file__).parent.parent / "gui_app" / "backtest" / "engine.py").read_text(encoding="utf-8")
    tree = ast.parse(src)

    # 找到 _run_native_backtest 函数定义
    target_func: ast.FunctionDef | None = None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "_run_native_backtest":
            target_func = node  # type: ignore[assignment]
            break

    if target_func is None:
        raise RuntimeError("engine.py 中找不到 _run_native_backtest 函数，请检查 _live_engine_adjust_accepts()")

    # 在函数体内找 adjust_map = {...} 赋值
    for node in ast.walk(target_func):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "adjust_map"
            and isinstance(node.value, ast.Dict)
        ):
            return frozenset(
                k.value
                for k in node.value.keys
                if isinstance(k, ast.Constant) and isinstance(k.value, str)
            )

    raise RuntimeError(
        "engine.py 的 _run_native_backtest() 中找不到 adjust_map 赋值，请检查 _live_engine_adjust_accepts()"
    )


_ENGINE_ADJUST_ACCEPTS = _live_engine_adjust_accepts()
_KNOWN_PERIOD_CANONICALS = {"1d", "1h", "30m", "15m", "5m", "1m"}


class TestBacktestWidgetMappingContract:
    """
    ADJUST_TEXT_MAP / PERIOD_TEXT_MAP 与 addItems() 文字及下游引擎的一致性契约。
    不实例化任何 QWidget，Ubuntu headless CI 可直接运行。
    """

    # ------ adjust ------

    def test_adjust_map_covers_all_combo_items(self):
        """每个 combo 显示项都必须是 ADJUST_TEXT_MAP 的 key。"""
        from gui_app.widgets.backtest_widget import ADJUST_TEXT_MAP

        for item in _ADJUST_ITEMS_IN_UI:
            assert item in ADJUST_TEXT_MAP, (
                f"adjust_combo 显示项 '{item}' 在 ADJUST_TEXT_MAP 中找不到对应 key。\n"
                "请同步更新 gui_app/widgets/backtest_widget.py 中的 ADJUST_TEXT_MAP。"
            )

    def test_adjust_map_has_no_orphan_keys(self):
        """ADJUST_TEXT_MAP 中不应存在 addItems() 里没有的幽灵 key（防反向遗忘）。"""
        from gui_app.widgets.backtest_widget import ADJUST_TEXT_MAP

        orphans = set(ADJUST_TEXT_MAP.keys()) - set(_ADJUST_ITEMS_IN_UI)
        assert not orphans, (
            f"ADJUST_TEXT_MAP 存在未对应任何 combo 项的多余 key: {orphans}"
        )

    def test_adjust_canonical_values_understood_by_engine(self):
        """ADJUST_TEXT_MAP 的每个 canonical value 必须被引擎 adjust_map 认识。"""
        from gui_app.widgets.backtest_widget import ADJUST_TEXT_MAP

        for ui_text, canonical in ADJUST_TEXT_MAP.items():
            assert canonical in _ENGINE_ADJUST_ACCEPTS, (
                f"UI 标签 '{ui_text}' → canonical='{canonical}' "
                f"不在引擎接受集合 {_ENGINE_ADJUST_ACCEPTS} 中。"
            )

    def test_adjust_canonical_values_are_normalized_strings(self):
        """canonical value 必须是 strip().lower() 后不变的非空字符串（避免空格/大小写引入 fallback）。"""
        from gui_app.widgets.backtest_widget import ADJUST_TEXT_MAP

        for ui_text, canonical in ADJUST_TEXT_MAP.items():
            assert (
                isinstance(canonical, str)
                and canonical
                and canonical == canonical.strip().lower()
            ), (
                f"ADJUST_TEXT_MAP['{ui_text}'] = {canonical!r} "
                "不是标准化 canonical 字符串（小写、无前后空格）。"
            )

    # ------ period ------

    def test_period_map_covers_all_combo_items(self):
        """每个 combo 显示项都必须是 PERIOD_TEXT_MAP 的 key。"""
        from gui_app.widgets.backtest_widget import PERIOD_TEXT_MAP

        for item in _PERIOD_ITEMS_IN_UI:
            assert item in PERIOD_TEXT_MAP, (
                f"period_combo 显示项 '{item}' 在 PERIOD_TEXT_MAP 中找不到对应 key。\n"
                "请同步更新 gui_app/widgets/backtest_widget.py 中的 PERIOD_TEXT_MAP。"
            )

    def test_period_map_has_no_orphan_keys(self):
        """PERIOD_TEXT_MAP 中不应存在 addItems() 里没有的幽灵 key。"""
        from gui_app.widgets.backtest_widget import PERIOD_TEXT_MAP

        orphans = set(PERIOD_TEXT_MAP.keys()) - set(_PERIOD_ITEMS_IN_UI)
        assert not orphans, (
            f"PERIOD_TEXT_MAP 存在未对应任何 combo 项的多余 key: {orphans}"
        )

    def test_period_canonical_values_in_known_set(self):
        """PERIOD_TEXT_MAP 的 canonical value 必须属于已知周期格式集合。"""
        from gui_app.widgets.backtest_widget import PERIOD_TEXT_MAP

        for ui_text, canonical in PERIOD_TEXT_MAP.items():
            assert canonical in _KNOWN_PERIOD_CANONICALS, (
                f"PERIOD_TEXT_MAP['{ui_text}'] = '{canonical}' "
                f"不在已知 canonical 周期集合 {_KNOWN_PERIOD_CANONICALS} 中。"
            )
