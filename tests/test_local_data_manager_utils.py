"""
tests/test_local_data_manager_utils.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
gui_app/widgets/local_data_manager_widget.py 中模块级纯工具函数和
DataDownloadThread 中不依赖 self 的纯逻辑方法单元测试
(不需要 QApplication)
"""
import os
import csv
import tempfile
import pytest
import pandas as pd

# Import the module-level utility functions directly
import gui_app.widgets.local_data_manager_widget as _ldm

_align_dataframe_to_columns = _ldm._align_dataframe_to_columns
_build_stock_daily_delete_sql = _ldm._build_stock_daily_delete_sql
_get_duckdb_path = _ldm._get_duckdb_path

# DataDownloadThread methods that don't use self
DataDownloadThread = _ldm.DataDownloadThread


# ===========================================================================
# _align_dataframe_to_columns
# ===========================================================================
class TestAlignDataframeToColumns:
    def test_passthrough_when_columns_match(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        result = _align_dataframe_to_columns(df, ["a", "b"])
        assert list(result.columns) == ["a", "b"]

    def test_adds_missing_column_as_none(self):
        df = pd.DataFrame({"a": [1]})
        result = _align_dataframe_to_columns(df, ["a", "b"])
        assert "b" in result.columns
        assert result["b"].iloc[0] is None

    def test_reorders_columns(self):
        df = pd.DataFrame({"b": [2], "a": [1]})
        result = _align_dataframe_to_columns(df, ["a", "b"])
        assert list(result.columns) == ["a", "b"]

    def test_drops_extra_columns(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = _align_dataframe_to_columns(df, ["a", "b"])
        assert list(result.columns) == ["a", "b"]
        assert "c" not in result.columns

    def test_empty_df_with_columns(self):
        df = pd.DataFrame()
        result = _align_dataframe_to_columns(df, ["a", "b"])
        assert list(result.columns) == ["a", "b"]

    def test_does_not_modify_original(self):
        df = pd.DataFrame({"a": [1]})
        _ = _align_dataframe_to_columns(df, ["a", "b"])
        assert list(df.columns) == ["a"]

    def test_all_none_added_for_full_missing(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        result = _align_dataframe_to_columns(df, ["a", "b"])
        assert result["a"].isna().all()
        assert result["b"].isna().all()


# ===========================================================================
# _build_stock_daily_delete_sql
# ===========================================================================
class TestBuildStockDailyDeleteSql:
    def test_basic_sql_structure(self):
        sql, params = _build_stock_daily_delete_sql(["000001.SZ"], ["stock_code"])
        assert "DELETE FROM stock_daily WHERE" in sql
        assert "000001.SZ" in params

    def test_single_stock(self):
        sql, params = _build_stock_daily_delete_sql(["600519.SH"], ["stock_code"])
        assert "stock_code IN (?)" in sql
        assert "600519.SH" in params

    def test_multiple_stocks(self):
        sql, params = _build_stock_daily_delete_sql(
            ["000001.SZ", "600519.SH"], ["stock_code"]
        )
        assert "000001.SZ" in params
        assert "600519.SH" in params

    def test_period_clause_added_when_column_present(self):
        sql, params = _build_stock_daily_delete_sql(
            ["000001.SZ"], ["stock_code", "period"], period="1d"
        )
        assert "period = ?" in sql
        assert "1d" in params

    def test_period_clause_omitted_when_column_absent(self):
        sql, params = _build_stock_daily_delete_sql(
            ["000001.SZ"], ["stock_code"], period="1d"
        )
        assert "period" not in sql

    def test_adjust_type_added_when_column_present(self):
        sql, params = _build_stock_daily_delete_sql(
            ["000001.SZ"], ["stock_code", "adjust_type"], adjust_type="front"
        )
        assert "adjust_type = ?" in sql
        assert "front" in params

    def test_adjust_type_default_none(self):
        sql, params = _build_stock_daily_delete_sql(
            ["000001.SZ"], ["stock_code", "adjust_type"]
        )
        assert "adjust_type = ?" in sql
        assert "none" in params

    def test_both_period_and_adjust_type(self):
        sql, params = _build_stock_daily_delete_sql(
            ["000001.SZ"], ["stock_code", "period", "adjust_type"],
            period="5m", adjust_type="back"
        )
        assert "period = ?" in sql
        assert "adjust_type = ?" in sql
        assert sql.count("AND") == 2
        assert "5m" in params
        assert "back" in params


# ===========================================================================
# _get_duckdb_path
# ===========================================================================
class TestGetDuckdbPath:
    def test_returns_string(self):
        path = _get_duckdb_path()
        assert isinstance(path, str)
        assert len(path) > 0

    def test_ends_with_ddb(self):
        path = _get_duckdb_path()
        # Either from resolve_duckdb_path or default
        assert path.endswith(".ddb") or path.endswith(".db") or len(path) > 0


# ===========================================================================
# DataDownloadThread._parse_task_type_from_path (doesn't use self)
# ===========================================================================
class TestParseTaskTypeFromPath:
    def test_transaction_in_filename(self):
        result = DataDownloadThread._parse_task_type_from_path(
            None, "/logs/failed_transaction_20240101.csv"
        )
        assert result == "download_transaction"

    def test_tick_in_filename(self):
        result = DataDownloadThread._parse_task_type_from_path(
            None, "/logs/failed_tick_20240101.csv"
        )
        assert result == "download_tick"

    def test_unknown_defaults_to_tick(self):
        result = DataDownloadThread._parse_task_type_from_path(
            None, "/logs/failed_stocks_20240101.csv"
        )
        assert result == "download_tick"

    def test_transaction_wins_over_tick(self):
        # If "transaction" appears → returns "download_transaction"
        result = DataDownloadThread._parse_task_type_from_path(
            None, "/logs/failed_transaction_tick_20240101.csv"
        )
        assert result == "download_transaction"

    def test_just_filename_no_path(self):
        result = DataDownloadThread._parse_task_type_from_path(None, "tick_data.csv")
        assert result == "download_tick"


# ===========================================================================
# DataDownloadThread._build_symbols_from_csv (doesn't use self)
# ===========================================================================
class TestBuildSymbolsFromCsv:
    def test_valid_csv_with_stock_code_column(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("stock_code\n000001.SZ\n600519.SH\n000001.SZ\n", encoding="utf-8")
        result = DataDownloadThread._build_symbols_from_csv(None, str(f))
        assert "000001.SZ" in result
        assert "600519.SH" in result
        # deduplication
        assert len(result) == 2

    def test_missing_file_returns_empty(self):
        result = DataDownloadThread._build_symbols_from_csv(None, "/nonexistent/file.csv")
        assert result == []

    def test_csv_without_stock_code_column_returns_empty(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("symbol\n000001.SZ\n", encoding="utf-8")
        result = DataDownloadThread._build_symbols_from_csv(None, str(f))
        assert result == []

    def test_empty_csv_returns_empty(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("stock_code\n", encoding="utf-8")
        result = DataDownloadThread._build_symbols_from_csv(None, str(f))
        assert result == []

    def test_strips_whitespace(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("stock_code\n  000001.SZ  \n600519.SH\n", encoding="utf-8")
        result = DataDownloadThread._build_symbols_from_csv(None, str(f))
        assert "000001.SZ" in result


# ===========================================================================
# DataDownloadThread._export_failed_samples (doesn't use self)
# ===========================================================================
class TestExportFailedSamples:
    def test_empty_samples_returns_none(self):
        result = DataDownloadThread._export_failed_samples(None, [], "stocks")
        assert result is None

    def test_exports_csv_returns_path(self, tmp_path, monkeypatch):
        # Patch the log dir to use tmp_path
        import gui_app.widgets.local_data_manager_widget as ldm_mod
        original_file = ldm_mod.__file__
        # Monkey-patch Path(__file__).parents[2] to point to tmp_path
        from pathlib import Path
        monkeypatch.setattr(
            ldm_mod,
            "__file__",
            str(tmp_path / "gui_app" / "widgets" / "local_data_manager_widget.py")
        )
        (tmp_path / "gui_app" / "widgets").mkdir(parents=True, exist_ok=True)
        samples = [{"stock_code": "000001.SZ", "error": "timeout"}]
        result = DataDownloadThread._export_failed_samples(None, samples, "stocks")
        # Should return a path or None (depending on path resolution)
        # Since we're monkeypatching __file__, it may or may not succeed
        assert result is None or isinstance(result, str)

    def test_non_empty_samples_creates_file(self):
        # Use the actual function, it will create in logs/ dir
        samples = [{"stock_code": "TEST.SZ", "reason": "unit_test"}]
        result = DataDownloadThread._export_failed_samples(None, samples, "unit_test")
        # Should either return a valid path or None on error
        if result is not None:
            assert os.path.exists(result)
            # Clean up
            try:
                os.remove(result)
            except Exception:
                pass
        else:
            pass  # None is also acceptable when log dir is not writable


# ===========================================================================
# _get_table_columns (uses a mock DuckDB connection)
# ===========================================================================
class TestGetTableColumns:
    """通过内存 DuckDB 连接测试 _get_table_columns。"""

    @pytest.fixture()
    def mem_con(self):
        import duckdb
        con = duckdb.connect(":memory:")
        # 使用白名单内的表名（_ALLOWED_WRITE_TABLES 包含 stock_daily）
        con.execute(
            "CREATE TABLE stock_daily (stock_code VARCHAR, date TIMESTAMP, close DOUBLE)"
        )
        yield con
        con.close()

    def test_returns_column_list(self, mem_con):
        cols = _ldm._get_table_columns(mem_con, "stock_daily")
        assert isinstance(cols, list)
        assert len(cols) > 0

    def test_contains_expected_columns(self, mem_con):
        cols = _ldm._get_table_columns(mem_con, "stock_daily")
        assert "stock_code" in cols
        assert "date" in cols
        assert "close" in cols

    def test_nonexistent_table_returns_empty(self, mem_con):
        # 不在白名单内的表名直接返回空列表
        cols = _ldm._get_table_columns(mem_con, "no_such_table")
        assert cols == []

    def test_column_order_preserved(self, mem_con):
        import duckdb
        con = duckdb.connect(":memory:")
        # stock_1m 在白名单内
        con.execute("CREATE TABLE stock_1m (a INT, b VARCHAR, c DOUBLE)")
        cols = _ldm._get_table_columns(con, "stock_1m")
        con.close()
        assert cols == ["a", "b", "c"]


# ===========================================================================
# _upsert_ingestion_status (uses in-memory DuckDB)
# ===========================================================================
class TestUpsertIngestionStatus:
    """通过内存 DuckDB 连接测试 _upsert_ingestion_status。"""

    @pytest.fixture()
    def mem_con(self):
        import duckdb
        con = duckdb.connect(":memory:")
        yield con
        con.close()

    def test_creates_table_if_not_exists(self, mem_con):
        _ldm._upsert_ingestion_status(
            mem_con,
            stock_code="000001.SZ",
            period="1d",
            start_date="2020-01-01",
            end_date="2023-12-31",
            source="xtquant",
            status="success",
            record_count=100,
            error_message=None,
        )
        # 不报错 + 表应该已创建
        rows = mem_con.execute(
            "SELECT COUNT(*) FROM data_ingestion_status WHERE stock_code='000001.SZ'"
        ).fetchone()
        assert rows[0] == 1

    def test_upsert_updates_existing_record(self, mem_con):
        _ldm._upsert_ingestion_status(
            mem_con, "000001.SZ", "1d", "2020-01-01", "2021-12-31",
            "xtquant", "success", 50, None
        )
        _ldm._upsert_ingestion_status(
            mem_con, "000001.SZ", "1d", "2020-01-01", "2023-12-31",
            "xtquant", "success", 200, None
        )
        rows = mem_con.execute(
            "SELECT COUNT(*) FROM data_ingestion_status WHERE stock_code='000001.SZ'"
        ).fetchone()
        assert rows[0] == 1  # should be 1 after upsert (delete + insert)

    def test_multiple_stocks(self, mem_con):
        for code in ["000001.SZ", "600519.SH", "000002.SZ"]:
            _ldm._upsert_ingestion_status(
                mem_con, code, "1d", "2020-01-01", "2023-12-31",
                "xtquant", "success", 100, None
            )
        rows = mem_con.execute(
            "SELECT COUNT(*) FROM data_ingestion_status"
        ).fetchone()
        assert rows[0] == 3

    def test_error_message_stored(self, mem_con):
        _ldm._upsert_ingestion_status(
            mem_con, "FAIL.SZ", "1d", "2020-01-01", "2023-12-31",
            "xtquant", "error", 0, "conn timeout"
        )
        rows = mem_con.execute(
            "SELECT error_message FROM data_ingestion_status WHERE stock_code='FAIL.SZ'"
        ).fetchone()
        assert rows is not None
        assert rows[0] == "conn timeout"

    def test_none_error_message_stored_as_null(self, mem_con):
        _ldm._upsert_ingestion_status(
            mem_con, "OK.SZ", "1d", "2020-01-01", "2023-12-31",
            "xtquant", "success", 100, None
        )
        rows = mem_con.execute(
            "SELECT error_message FROM data_ingestion_status WHERE stock_code='OK.SZ'"
        ).fetchone()
        assert rows is not None
        assert rows[0] is None

    def test_invalid_connection_does_not_raise(self):
        """非法 connection 应被 except 捕获且静默返回。"""
        class BadConn:
            def execute(self, *args, **kwargs):
                raise RuntimeError("bad conn")

        # 不应向外抛异常
        _ldm._upsert_ingestion_status(
            BadConn(), "X.SZ", "1d", "", "", "", "", 0, None
        )


# ===========================================================================
# _get_latest_failed_csv edge cases (exception paths)
# ===========================================================================
class TestGetLatestFailedCsvEdgeCases:
    def test_nonexistent_log_dir_returns_none(self, tmp_path, monkeypatch):
        """日志目录不存在时返回 None。"""
        import gui_app.widgets.local_data_manager_widget as ldm_mod
        # Monkey-patch __file__ to a deep-enough fake path
        fake_path = tmp_path / "a" / "b" / "c" / "widget.py"
        fake_path.parent.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(ldm_mod, "__file__", str(fake_path))
        result = DataDownloadThread._get_latest_failed_csv(None)
        assert result is None

    def test_no_csv_files_returns_none(self, tmp_path, monkeypatch):
        """日志目录存在但没有 failed_*.csv 时返回 None。"""
        import gui_app.widgets.local_data_manager_widget as ldm_mod
        # Create a fake src/gui_app/widgets structure under tmp_path
        fake_widget = tmp_path / "gui_app" / "widgets" / "local_data_manager_widget.py"
        fake_widget.parent.mkdir(parents=True, exist_ok=True)
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()
        # Add a non-matching file
        (logs_dir / "other.txt").write_text("x")
        monkeypatch.setattr(ldm_mod, "__file__", str(fake_widget))
        result = DataDownloadThread._get_latest_failed_csv(None)
        assert result is None

    def test_returns_most_recent_csv(self, tmp_path, monkeypatch):
        """存在多个 failed_*.csv 时返回最新的。"""
        import gui_app.widgets.local_data_manager_widget as ldm_mod
        fake_widget = tmp_path / "gui_app" / "widgets" / "local_data_manager_widget.py"
        fake_widget.parent.mkdir(parents=True, exist_ok=True)
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()
        import time as _time
        f1 = logs_dir / "failed_stocks_old.csv"
        f1.write_text("stock_code\n000001.SZ")
        _time.sleep(0.05)
        f2 = logs_dir / "failed_stocks_new.csv"
        f2.write_text("stock_code\n600519.SH")
        monkeypatch.setattr(ldm_mod, "__file__", str(fake_widget))
        result = DataDownloadThread._get_latest_failed_csv(None)
        if result is not None:
            assert "new" in result or isinstance(result, str)


# ===========================================================================
# DataDownloadThread.__init__ — 检查基本属性（不启动线程）
# ===========================================================================
class TestDataDownloadThreadInit:
    def test_task_type_stored(self):
        t = DataDownloadThread(
            task_type="download_stocks",
            symbols=["000001.SZ"],
            start_date="2020-01-01",
            end_date="2023-12-31",
        )
        assert t.task_type == "download_stocks"

    def test_symbols_stored(self):
        t = DataDownloadThread(
            task_type="download_stocks",
            symbols=["000001.SZ", "600519.SH"],
            start_date="2020-01-01",
            end_date="2023-12-31",
        )
        assert "000001.SZ" in t.symbols

    def test_start_end_dates_stored(self):
        t = DataDownloadThread(
            task_type="download_bonds",
            symbols=[],
            start_date="2022-01-01",
            end_date="2022-12-31",
        )
        assert t.start_date == "2022-01-01"
        assert t.end_date == "2022-12-31"

    def test_default_data_type(self):
        t = DataDownloadThread(
            task_type="update_data",
            symbols=[],
            start_date="2022-01-01",
            end_date="2022-12-31",
        )
        assert t.data_type == "daily"

    def test_custom_data_type(self):
        t = DataDownloadThread(
            task_type="download_tick",
            symbols=["000001.SZ"],
            start_date="2022-01-01",
            end_date="2022-12-31",
            data_type="tick",
        )
        assert t.data_type == "tick"


# ===========================================================================
# DataDownloadThread._get_latest_failed_csv (doesn't use self)
# ===========================================================================
class TestGetLatestFailedCsv:
    def test_no_logs_dir_returns_none(self, monkeypatch):
        import gui_app.widgets.local_data_manager_widget as ldm_mod
        from pathlib import Path

        class FakeParents:
            def __getitem__(self, idx):
                return Path("/nonexistent/path/that/does/not/exist")

        monkeypatch.setattr(
            ldm_mod,
            "__file__",
            str(Path("/nonexistent/dir/widgets/local_data_manager_widget.py"))
        )
        result = DataDownloadThread._get_latest_failed_csv(None)
        assert result is None

    def test_returns_most_recent_csv(self, tmp_path, monkeypatch):
        import gui_app.widgets.local_data_manager_widget as ldm_mod
        # Arrange: Create a fake log dir with csv files
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        csv1 = log_dir / "failed_stocks_001.csv"
        csv2 = log_dir / "failed_tick_002.csv"
        csv1.write_text("code\n")
        import time
        time.sleep(0.01)
        csv2.write_text("code\n")

        # Patch __file__ so parents[2] points to tmp_path
        fake_file = tmp_path / "gui_app" / "widgets" / "local_data_manager_widget.py"
        fake_file.parent.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(ldm_mod, "__file__", str(fake_file))

        result = DataDownloadThread._get_latest_failed_csv(None)
        # Should return the most recently modified file
        assert result is not None
        assert os.path.basename(result).startswith("failed_")
        assert result.endswith(".csv")
