from __future__ import annotations

import gzip
from datetime import datetime
from pathlib import Path

from PyQt5.QtCore import Qt
from PyQt5.QtGui import QColor

from gui_app.widgets.watchlist.watchlist_group import WatchlistGroupManager
from gui_app.widgets.watchlist.watchlist_model import WatchlistModel
from gui_app.widgets.watchlist.watchlist_widget import WatchlistWidget


def test_watchlist_model_upsert_quote_updates_row() -> None:
    model = WatchlistModel()
    model.set_symbols([{"name": "平安银行", "symbol": "000001.SZ"}])
    model.upsert_quote(
        "000001.SZ",
        {
            "price": 10.1,
            "change": 0.2,
            "change_pct": 2.0,
            "bid1": 10.09,
            "ask1": 10.11,
            "prev_close": 9.9,
            "open": 10.0,
            "high": 10.3,
            "low": 9.8,
            "volume": 10000,
        },
    )
    assert model.rowCount() == 1
    assert model.data(model.index(0, 0)) == "平安银行"
    assert model.data(model.index(0, 2)) == "10.10"
    assert model.data(model.index(0, 4)) == "2.00"


def test_watchlist_widget_update_and_search(qapp) -> None:
    widget = WatchlistWidget()
    widget.set_current_symbol("000001.SZ")
    widget.update_orderbook(
        {
            "symbol": "000001.SZ",
            "name": "平安银行",
            "price": 10.2,
            "prev_close": 10.0,
            "open": 10.1,
            "high": 10.4,
            "low": 10.0,
            "bid1": 10.19,
            "ask1": 10.21,
            "volume": 1200,
        }
    )
    assert widget.model.rowCount() >= 1
    widget._on_search_changed("000001")
    assert widget.proxy.rowCount() >= 1
    widget._on_search_changed("NOT_EXIST")
    assert widget.proxy.rowCount() == 0


def test_watchlist_group_manager_persistence(tmp_path: Path) -> None:
    path = tmp_path / "groups.json"
    mgr = WatchlistGroupManager(str(path))
    mgr.add_group("A股关注")
    mgr.set_symbols("A股关注", ["000001.SZ", "600000.SH"])
    mgr2 = WatchlistGroupManager(str(path))
    assert "A股关注" in mgr2.group_names()
    assert mgr2.symbols("A股关注") == ["000001.SZ", "600000.SH"]


def test_watchlist_widget_click_emits_symbol(qapp) -> None:
    widget = WatchlistWidget()
    widget.set_current_symbol("000001.SZ")
    emitted: list[str] = []
    widget.symbol_selected.connect(lambda s: emitted.append(s))
    idx = widget.proxy.index(0, 0)
    widget._on_row_clicked(idx)
    assert emitted[0] == "000001.SZ"


def test_watchlist_widget_type_filter(qapp) -> None:
    widget = WatchlistWidget()
    widget.set_current_symbol("000001.SZ")
    widget.update_orderbook({"symbol": "000001.SZ", "name": "平安银行", "price": 10.0, "prev_close": 9.9})
    widget.update_orderbook({"symbol": "00700.HK", "name": "腾讯控股", "price": 300.0, "prev_close": 298.0})
    widget._on_type_changed("A股")
    assert widget.proxy.rowCount() >= 1
    first_symbol = str(widget.proxy.index(0, 1).data())
    assert first_symbol.endswith(".SZ") or first_symbol.endswith(".SH")


def test_watchlist_model_color_mode_switch() -> None:
    model = WatchlistModel()
    model.set_symbols([{"symbol": "000001.SZ", "change": 1.0, "change_pct": 1.0}])
    red_up = model.data(model.index(0, 3), role=Qt.ForegroundRole)
    assert isinstance(red_up, QColor)
    model.set_color_mode("红跌绿涨")
    green_up = model.data(model.index(0, 3), role=Qt.ForegroundRole)
    assert isinstance(green_up, QColor)
    assert green_up != red_up


def test_watchlist_widget_state_persistence(qapp) -> None:
    widget = WatchlistWidget(state_key="pytest_watchlist_state")
    widget.toolbar.search_edit.setText("700")
    widget._on_type_changed("港股")
    widget._on_color_mode_changed("红跌绿涨")
    widget._on_group_changed("默认")
    widget._save_state()
    widget2 = WatchlistWidget(state_key="pytest_watchlist_state")
    assert widget2.toolbar.search_edit.text() == "700"
    assert widget2.toolbar.type_combo.currentText() == "港股"
    assert widget2.toolbar.color_combo.currentText() == "红跌绿涨"


def test_watchlist_widget_fullscreen_toggle(qapp) -> None:
    widget = WatchlistWidget(state_key="pytest_watchlist_fullscreen")
    widget._toggle_fullscreen_view()
    assert widget._fullscreen_dialog is not None
    assert widget._fullscreen_widget is not None
    widget._toggle_fullscreen_view()
    assert widget._fullscreen_dialog is None


def test_watchlist_group_action_log_shared(tmp_path: Path) -> None:
    path = tmp_path / "groups.json"
    mgr1 = WatchlistGroupManager(str(path))
    mgr2 = WatchlistGroupManager(str(path))
    before = len(WatchlistGroupManager.get_action_log())
    mgr1.add_group("共享分组")
    mgr2.set_symbols("共享分组", ["000001.SZ"])
    after = len(WatchlistGroupManager.get_action_log())
    assert after >= before + 2


def test_watchlist_widget_action_log_visible(qapp) -> None:
    widget = WatchlistWidget(state_key="pytest_action_log")
    widget._on_action_filter_changed("全部")
    before = widget.action_log_list.count()
    widget.group_manager.add_group("日志分组")
    assert widget.action_log_list.count() >= before
    has_add_group = False
    for i in range(widget.action_log_list.count()):
        item = widget.action_log_list.item(i)
        if item is not None and "add_group" in item.text():
            has_add_group = True
            break
    assert has_add_group


def test_watchlist_widget_action_filter_and_export(tmp_path: Path, qapp) -> None:
    widget = WatchlistWidget(state_key="pytest_action_filter")
    widget.group_manager.add_group("筛选分组")
    widget.group_manager.set_symbols("筛选分组", ["000001.SZ"])
    widget._on_action_filter_changed("add_group")
    assert widget.action_log_list.count() >= 1
    for i in range(widget.action_log_list.count()):
        item = widget.action_log_list.item(i)
        assert item is not None
        assert "add_group" in item.text()
    out = tmp_path / "action_log.csv"
    exported = widget.export_action_log_csv(str(out))
    assert exported == str(out)
    text = out.read_text(encoding="utf-8-sig")
    assert "ts,action,group,payload,source_view,action_filter,time_filter,operator,exported_at" in text
    sidecar = Path(str(out) + ".sha256")
    assert sidecar.exists() is True
    sidecar_lines = sidecar.read_text(encoding="utf-8").splitlines()
    assert sidecar_lines[0].endswith(f"*{out.name}")
    verify = WatchlistWidget.verify_csv_with_sidecar(str(out))
    assert verify["ok"] is True


def test_watchlist_widget_time_filter_window() -> None:
    now = datetime(2026, 1, 10, 12, 0, 0)
    recent = {"ts": "2026-01-08T08:00:00"}
    old = {"ts": "2025-12-20T08:00:00"}
    assert WatchlistWidget._in_time_window(recent, "7天", now=now) is True
    assert WatchlistWidget._in_time_window(old, "7天", now=now) is False
    assert WatchlistWidget._in_time_window(old, "30天", now=now) is True


def test_watchlist_group_clear_action_log(tmp_path: Path) -> None:
    path = tmp_path / "groups.json"
    mgr = WatchlistGroupManager(str(path))
    mgr.add_group("to-clear")
    (tmp_path / "watchlist_group_actions.20260101_000000.jsonl").write_text("{}", encoding="utf-8")
    (tmp_path / "watchlist_group_actions.20260101_000001.jsonl.gz").write_text("{}", encoding="utf-8")
    assert mgr.log_path.exists() is True
    removed = mgr.clear_action_log()
    assert removed >= 1
    assert mgr.log_path.exists() is False
    assert list(tmp_path.glob("watchlist_group_actions.*.jsonl")) == []
    assert list(tmp_path.glob("watchlist_group_actions.*.jsonl.gz")) == []


def test_watchlist_group_log_rotate_by_size(tmp_path: Path) -> None:
    path = tmp_path / "groups.json"
    mgr = WatchlistGroupManager(str(path))
    mgr._LOG_ROTATE_MAX_BYTES = 32
    mgr.log_path.write_text("x" * 64, encoding="utf-8")
    mgr.add_group("rotate-size")
    rotated = list(tmp_path.glob("watchlist_group_actions.*.jsonl.gz"))
    assert len(rotated) >= 1
    assert mgr.log_path.exists() is True


def test_watchlist_group_log_rotate_prune_keep_files(tmp_path: Path) -> None:
    path = tmp_path / "groups.json"
    mgr = WatchlistGroupManager(str(path))
    mgr._LOG_ROTATE_KEEP_FILES = 1
    for i in range(3):
        (tmp_path / f"watchlist_group_actions.20260101_00000{i}.jsonl").write_text("x", encoding="utf-8")
    mgr._LOG_ROTATE_MAX_BYTES = 1
    mgr.log_path.write_text("xx", encoding="utf-8")
    mgr.add_group("rotate-prune")
    rotated = sorted(tmp_path.glob("watchlist_group_actions.*.jsonl"))
    rotated_gz = sorted(tmp_path.glob("watchlist_group_actions.*.jsonl.gz"))
    assert len(rotated) + len(rotated_gz) <= 1


def test_watchlist_group_source_attached(tmp_path: Path) -> None:
    mgr = WatchlistGroupManager(str(tmp_path / "groups.json"))
    before = len(WatchlistGroupManager.get_action_log())
    mgr.add_group("source-group", source="test_view")
    log = WatchlistGroupManager.get_action_log()[before]
    assert log["payload"]["source"] == "test_view"


def test_watchlist_group_load_action_log_from_disk(tmp_path: Path) -> None:
    path = tmp_path / "groups.json"
    log_file = tmp_path / "watchlist_group_actions.jsonl"
    log_file.write_text(
        '{"ts":"2026-01-01T09:00:00","action":"add_group","group":"A","payload":{"size":0}}\n',
        encoding="utf-8",
    )
    before = len(WatchlistGroupManager.get_action_log())
    mgr = WatchlistGroupManager(str(path))
    _ = mgr
    after = len(WatchlistGroupManager.get_action_log())
    assert after >= before + 1


def test_watchlist_group_load_action_log_from_gz_disk(tmp_path: Path) -> None:
    path = tmp_path / "groups.json"
    log_file = tmp_path / "watchlist_group_actions.20260101_000000.jsonl.gz"
    with gzip.open(log_file, "wt", encoding="utf-8") as f:
        f.write('{"ts":"2026-01-01T10:00:00","action":"set_symbols","group":"B","payload":{"size":2}}\n')
    before = len(WatchlistGroupManager.get_action_log())
    mgr = WatchlistGroupManager(str(path))
    _ = mgr
    after = len(WatchlistGroupManager.get_action_log())
    assert after >= before + 1


def test_watchlist_widget_default_export_name(qapp) -> None:
    widget = WatchlistWidget(state_key="pytest_default_export")
    name = widget._build_default_export_name(digest_prefix="abcd1234")
    assert name.endswith(".csv")
    assert "abcd1234_watchlist_action_log_" in name


def test_watchlist_group_archive_uncompressed_logs(tmp_path: Path) -> None:
    path = tmp_path / "groups.json"
    mgr = WatchlistGroupManager(str(path))
    old_file = tmp_path / "watchlist_group_actions.20260101_000000.jsonl"
    old_file.write_text('{"ts":"2026-01-01T09:00:00","action":"x","group":"g","payload":{}}\n', encoding="utf-8")
    count = mgr.archive_uncompressed_logs(older_than_days=0)
    assert count >= 1
    assert old_file.exists() is False
    assert (tmp_path / "watchlist_group_actions.20260101_000000.jsonl.gz").exists() is True


def test_watchlist_widget_integrity_digest_stable() -> None:
    logs = [
        {"ts": "2026-01-01T10:00:00", "action": "add_group", "group": "A", "payload": {"size": 0}},
        {"ts": "2026-01-01T10:05:00", "action": "set_symbols", "group": "A", "payload": {"size": 1}},
    ]
    d1 = WatchlistWidget._build_integrity_digest(logs)
    d2 = WatchlistWidget._build_integrity_digest(logs)
    assert d1 == d2
    assert len(d1) == 64


def test_watchlist_widget_verify_csv_with_sidecar_fail(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    (tmp_path / "bad.csv.sha256").write_text("deadbeef *bad.csv\n", encoding="utf-8")
    result = WatchlistWidget.verify_csv_with_sidecar(str(csv_path))
    assert result["ok"] is False
    assert result["reason"] == "digest_mismatch"
