#!/usr/bin/env python3
"""
tests/test_financial_data_e2e.py

FinancialDataSaver E2E 集成测试
——使用真实 DuckDB 内存库完成完整链路验证——

覆盖范围：
- 表创建（financial_income / balance / cashflow）
- save_from_qmt() 写入 + 读回验证（三张表完整链路）
- UPSERT 语义（同一 stock_code+report_date 重复写入覆盖旧数据）
- 多标的数据隔离
- load_financial_data() 查询接口
- 空 / None 输入不写入任何数据
- 字段值准确性（revenue、net_profit、gross_profit 计算结果）
"""
from __future__ import annotations

import contextlib
from typing import Any, Optional

import pandas as pd
import pytest

try:
    import duckdb  # type: ignore[import]
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not _DUCKDB_AVAILABLE,
    reason="duckdb 未安装，跳过 E2E 测试",
)


# ---------------------------------------------------------------------------
# 真实 DuckDB 内存适配器
# ---------------------------------------------------------------------------

class _InMemoryDbManager:
    """最小化的内存 DuckDB 适配器，满足 FinancialDataSaver 的接口约定。"""

    def __init__(self) -> None:
        self.con = duckdb.connect(":memory:")

    def execute_write_query(self, query: str, params: Optional[tuple] = None) -> Any:
        if params:
            return self.con.execute(query, list(params))
        return self.con.execute(query)

    def execute_read_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        if params:
            return self.con.execute(query, list(params)).df()
        return self.con.execute(query).df()

    def insert_dataframe(self, table_name: str, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        # 与真实 DuckDBConnectionManager.insert_dataframe 保持一致
        try:
            cols_df = self.con.execute(
                f"PRAGMA table_info('{table_name}')"
            ).df()
        except Exception:
            return 0
        if cols_df.empty:
            return 0
        columns = cols_df["name"].tolist()
        df_insert = df.copy()
        for col in columns:
            if col not in df_insert.columns:
                df_insert[col] = None
        df_insert = df_insert[columns]
        self.con.register("_insert_tmp", df_insert)
        self.con.execute(f"INSERT INTO {table_name} SELECT * FROM _insert_tmp")
        self.con.unregister("_insert_tmp")
        return len(df_insert)

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.con.close()


# ---------------------------------------------------------------------------
# 测试数据工厂
# ---------------------------------------------------------------------------

def _income_df(n: int = 2) -> pd.DataFrame:
    return pd.DataFrame({
        "m_timetag":                    [20231231, 20230930][:n],
        "m_anntime":                    [20240130, 20231030][:n],
        "revenue":                      [1_000_000_000.0, 950_000_000.0][:n],
        "operating_revenue":            [1_000_000_000.0, 950_000_000.0][:n],
        "total_operating_cost":         [800_000_000.0,   760_000_000.0][:n],
        "net_profit_incl_min_int_inc":  [200_000_000.0,   185_000_000.0][:n],
        "net_profit_excl_min_int_inc":  [190_000_000.0,   178_000_000.0][:n],
        "oper_profit":                  [210_000_000.0,   195_000_000.0][:n],
        "tot_profit":                   [220_000_000.0,   200_000_000.0][:n],
        "s_fa_eps_basic":               [0.50,  0.45][:n],
        "s_fa_eps_diluted":             [0.49,  0.44][:n],
    })


def _balance_df(n: int = 1) -> pd.DataFrame:
    return pd.DataFrame({
        "m_timetag":              [20231231, 20230930][:n],
        "m_anntime":              [20240130, 20231030][:n],
        "tot_assets":             [5_000_000_000.0, 4_800_000_000.0][:n],
        "tot_liab":               [3_000_000_000.0, 2_900_000_000.0][:n],
        "total_equity":           [2_000_000_000.0, 1_900_000_000.0][:n],
        "total_current_assets":   [2_000_000_000.0, 1_900_000_000.0][:n],
        "total_current_liability":[1_500_000_000.0, 1_400_000_000.0][:n],
        "fix_assets":             [1_000_000_000.0,   950_000_000.0][:n],
        "intang_assets":          [  200_000_000.0,   180_000_000.0][:n],
    })


def _cashflow_df(n: int = 1) -> pd.DataFrame:
    return pd.DataFrame({
        "m_timetag":                [20231231, 20230930][:n],
        "m_anntime":                [20240130, 20231030][:n],
        "net_cash_flows_oper_act":  [ 300_000_000.0,  280_000_000.0][:n],
        "net_cash_flows_inv_act":   [-100_000_000.0, -90_000_000.0][:n],
        "net_cash_flows_fnc_act":   [ -50_000_000.0, -40_000_000.0][:n],
        "cash_cash_equ_beg_period": [1_000_000_000.0, 850_000_000.0][:n],
        "cash_cash_equ_end_period": [1_150_000_000.0, 1_000_000_000.0][:n],
    })


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db() -> _InMemoryDbManager:
    manager = _InMemoryDbManager()
    yield manager
    manager.close()


@pytest.fixture
def saver(db: _InMemoryDbManager):
    from data_manager.financial_data_saver import FinancialDataSaver
    return FinancialDataSaver(db)


# ---------------------------------------------------------------------------
# 1. 表创建
# ---------------------------------------------------------------------------

class TestTableCreation:
    def test_three_tables_created(self, db: _InMemoryDbManager):
        from data_manager.financial_data_saver import FinancialDataSaver
        FinancialDataSaver(db)
        tables = db.con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).df()["table_name"].tolist()
        assert "financial_income" in tables
        assert "financial_balance" in tables
        assert "financial_cashflow" in tables

    def test_income_has_primary_key_columns(self, saver, db: _InMemoryDbManager):
        cols = db.con.execute("PRAGMA table_info('financial_income')").df()["name"].tolist()
        assert "stock_code" in cols
        assert "report_date" in cols

    def test_cashflow_has_operating_cf_column(self, saver, db: _InMemoryDbManager):
        cols = db.con.execute("PRAGMA table_info('financial_cashflow')").df()["name"].tolist()
        assert "operating_cash_flow" in cols


# ---------------------------------------------------------------------------
# 2. save_from_qmt — 收入表完整链路
# ---------------------------------------------------------------------------

class TestSaveIncomeE2E:
    def test_rows_written_to_duckdb(self, saver, db: _InMemoryDbManager):
        result = saver.save_from_qmt("000001.SZ", _income_df(2))
        assert result["success"] is True
        assert result["income_count"] == 2
        count = db.con.execute(
            "SELECT COUNT(*) as n FROM financial_income WHERE stock_code = '000001.SZ'"
        ).df()["n"].iloc[0]
        assert count == 2

    def test_revenue_value_correct(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", _income_df(1))
        row = db.con.execute(
            "SELECT revenue FROM financial_income WHERE stock_code = '000001.SZ' LIMIT 1"
        ).df()
        assert not row.empty
        assert row["revenue"].iloc[0] == pytest.approx(1_000_000_000.0)

    def test_net_profit_stored(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", _income_df(1))
        row = db.con.execute(
            "SELECT net_profit FROM financial_income WHERE stock_code = '000001.SZ'"
        ).df()
        assert row["net_profit"].iloc[0] == pytest.approx(200_000_000.0)

    def test_gross_profit_calculated_and_stored(self, saver, db: _InMemoryDbManager):
        # gross_profit = revenue - total_operating_cost = 1e9 - 8e8 = 2e8
        saver.save_from_qmt("000001.SZ", _income_df(1))
        row = db.con.execute(
            "SELECT gross_profit FROM financial_income WHERE stock_code = '000001.SZ'"
        ).df()
        assert row["gross_profit"].iloc[0] == pytest.approx(200_000_000.0)

    def test_net_margin_calculated(self, saver, db: _InMemoryDbManager):
        # net_margin = net_profit / revenue * 100 = 200e6 / 1e9 * 100 = 20.0
        saver.save_from_qmt("000001.SZ", _income_df(1))
        row = db.con.execute(
            "SELECT net_margin FROM financial_income WHERE stock_code = '000001.SZ'"
        ).df()
        assert row["net_margin"].iloc[0] == pytest.approx(20.0)

    def test_data_source_is_qmt(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", _income_df(1))
        row = db.con.execute(
            "SELECT data_source FROM financial_income WHERE stock_code = '000001.SZ'"
        ).df()
        assert row["data_source"].iloc[0] == "QMT"

    def test_report_date_formatted(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", _income_df(1))
        row = db.con.execute(
            "SELECT report_date FROM financial_income WHERE stock_code = '000001.SZ'"
        ).df()
        assert row["report_date"].iloc[0] == "2023-12-31"


# ---------------------------------------------------------------------------
# 3. save_from_qmt — 资产负债表完整链路
# ---------------------------------------------------------------------------

class TestSaveBalanceE2E:
    def test_balance_row_written(self, saver, db: _InMemoryDbManager):
        result = saver.save_from_qmt("000001.SZ", _income_df(1), _balance_df(1))
        assert result["success"] is True
        assert result["balance_count"] == 1
        count = db.con.execute(
            "SELECT COUNT(*) as n FROM financial_balance WHERE stock_code = '000001.SZ'"
        ).df()["n"].iloc[0]
        assert count == 1

    def test_total_assets_correct(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", None, _balance_df(1))
        row = db.con.execute(
            "SELECT total_assets FROM financial_balance WHERE stock_code = '000001.SZ'"
        ).df()
        assert row["total_assets"].iloc[0] == pytest.approx(5_000_000_000.0)

    def test_debt_ratio_calculated(self, saver, db: _InMemoryDbManager):
        # debt_ratio = 3e9 / 5e9 * 100 = 60.0
        saver.save_from_qmt("000001.SZ", None, _balance_df(1))
        row = db.con.execute(
            "SELECT debt_to_asset_ratio FROM financial_balance WHERE stock_code = '000001.SZ'"
        ).df()
        assert row["debt_to_asset_ratio"].iloc[0] == pytest.approx(60.0)

    def test_current_ratio_calculated(self, saver, db: _InMemoryDbManager):
        # current_ratio = 2e9 / 1.5e9 ≈ 1.333
        saver.save_from_qmt("000001.SZ", None, _balance_df(1))
        row = db.con.execute(
            "SELECT current_ratio FROM financial_balance WHERE stock_code = '000001.SZ'"
        ).df()
        assert row["current_ratio"].iloc[0] == pytest.approx(2e9 / 1.5e9)


# ---------------------------------------------------------------------------
# 4. save_from_qmt — 现金流量表完整链路
# ---------------------------------------------------------------------------

class TestSaveCashflowE2E:
    def test_cashflow_row_written(self, saver, db: _InMemoryDbManager):
        result = saver.save_from_qmt("000001.SZ", None, None, _cashflow_df(1))
        assert result["success"] is True
        assert result["cashflow_count"] == 1
        count = db.con.execute(
            "SELECT COUNT(*) as n FROM financial_cashflow WHERE stock_code = '000001.SZ'"
        ).df()["n"].iloc[0]
        assert count == 1

    def test_operating_cf_stored(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", None, None, _cashflow_df(1))
        row = db.con.execute(
            "SELECT operating_cash_flow FROM financial_cashflow WHERE stock_code = '000001.SZ'"
        ).df()
        assert row["operating_cash_flow"].iloc[0] == pytest.approx(300_000_000.0)

    def test_net_cash_flow_sum(self, saver, db: _InMemoryDbManager):
        # net_cf = 300e6 + (-100e6) + (-50e6) = 150e6
        saver.save_from_qmt("000001.SZ", None, None, _cashflow_df(1))
        row = db.con.execute(
            "SELECT net_cash_flow FROM financial_cashflow WHERE stock_code = '000001.SZ'"
        ).df()
        assert row["net_cash_flow"].iloc[0] == pytest.approx(150_000_000.0)

    def test_cash_end_stored(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", None, None, _cashflow_df(1))
        row = db.con.execute(
            "SELECT cash_equivalents_end FROM financial_cashflow WHERE stock_code = '000001.SZ'"
        ).df()
        assert row["cash_equivalents_end"].iloc[0] == pytest.approx(1_150_000_000.0)


# ---------------------------------------------------------------------------
# 5. UPSERT 语义（同一 PK 覆盖旧数据）
# ---------------------------------------------------------------------------

class TestUpsertSemantics:
    def test_duplicate_report_date_overwrites(self, saver, db: _InMemoryDbManager):
        first = _income_df(1).copy()
        first["revenue"] = [500_000_000.0]
        saver.save_from_qmt("000001.SZ", first)

        second = _income_df(1).copy()  # revenue = 1_000_000_000
        saver.save_from_qmt("000001.SZ", second)

        count = db.con.execute(
            "SELECT COUNT(*) as n FROM financial_income "
            "WHERE stock_code = '000001.SZ' AND report_date = '2023-12-31'"
        ).df()["n"].iloc[0]
        assert count == 1  # 只留一条

        row = db.con.execute(
            "SELECT revenue FROM financial_income "
            "WHERE stock_code = '000001.SZ' AND report_date = '2023-12-31'"
        ).df()
        # 第二次写入的 revenue 生效
        assert row["revenue"].iloc[0] == pytest.approx(1_000_000_000.0)

    def test_different_report_dates_both_kept(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", _income_df(2))  # 2023-12-31 + 2023-09-30
        count = db.con.execute(
            "SELECT COUNT(*) as n FROM financial_income WHERE stock_code = '000001.SZ'"
        ).df()["n"].iloc[0]
        assert count == 2


# ---------------------------------------------------------------------------
# 6. 多标的数据隔离
# ---------------------------------------------------------------------------

class TestMultiStockIsolation:
    def test_two_stocks_independent(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", _income_df(1))
        saver.save_from_qmt("600000.SH", _income_df(1))

        count_sz = db.con.execute(
            "SELECT COUNT(*) as n FROM financial_income WHERE stock_code = '000001.SZ'"
        ).df()["n"].iloc[0]
        count_sh = db.con.execute(
            "SELECT COUNT(*) as n FROM financial_income WHERE stock_code = '600000.SH'"
        ).df()["n"].iloc[0]
        assert count_sz == 1
        assert count_sh == 1

    def test_deleting_one_stock_leaves_other(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", _income_df(1))
        saver.save_from_qmt("600000.SH", _income_df(1))
        db.con.execute("DELETE FROM financial_income WHERE stock_code = '000001.SZ'")
        count_sh = db.con.execute(
            "SELECT COUNT(*) as n FROM financial_income WHERE stock_code = '600000.SH'"
        ).df()["n"].iloc[0]
        assert count_sh == 1


# ---------------------------------------------------------------------------
# 7. 空 / None 输入不污染数据库
# ---------------------------------------------------------------------------

class TestEmptyInput:
    def test_none_income_nothing_written(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", None)
        count = db.con.execute(
            "SELECT COUNT(*) as n FROM financial_income"
        ).df()["n"].iloc[0]
        assert count == 0

    def test_empty_df_income_nothing_written(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", pd.DataFrame())
        count = db.con.execute(
            "SELECT COUNT(*) as n FROM financial_income"
        ).df()["n"].iloc[0]
        assert count == 0

    def test_all_none_success_true(self, saver, db: _InMemoryDbManager):
        result = saver.save_from_qmt("000001.SZ", None, None, None)
        assert result["success"] is True
        assert result["income_count"] == 0
        assert result["balance_count"] == 0
        assert result["cashflow_count"] == 0


# ---------------------------------------------------------------------------
# 8. load_financial_data — 读回接口
# ---------------------------------------------------------------------------

class TestLoadFinancialData:
    def test_load_income_roundtrip(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", _income_df(2))
        loaded = saver.load_financial_data("000001.SZ")
        assert loaded["income"] is not None and not loaded["income"].empty
        assert len(loaded["income"]) == 2

    def test_load_balance_roundtrip(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", None, _balance_df(1))
        loaded = saver.load_financial_data("000001.SZ")
        assert loaded["balance"] is not None and not loaded["balance"].empty

    def test_load_cashflow_roundtrip(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", None, None, _cashflow_df(1))
        loaded = saver.load_financial_data("000001.SZ")
        assert loaded["cashflow"] is not None and not loaded["cashflow"].empty

    def test_load_date_range_filter(self, saver, db: _InMemoryDbManager):
        saver.save_from_qmt("000001.SZ", _income_df(2))
        loaded = saver.load_financial_data("000001.SZ", start_date="2023-12-01")
        # 只有 report_date='2023-12-31' 满足 >= '2023-12-01'
        assert len(loaded["income"]) == 1

    def test_load_nonexistent_stock_returns_empty(self, saver, db: _InMemoryDbManager):
        loaded = saver.load_financial_data("NONEXISTENT")
        assert loaded["income"] is None or loaded["income"].empty

    def test_three_tables_full_roundtrip(self, saver, db: _InMemoryDbManager):
        """三张表同时写入、同时读回的完整端到端验证。"""
        saver.save_from_qmt(
            "000001.SZ", _income_df(2), _balance_df(1), _cashflow_df(1)
        )
        loaded = saver.load_financial_data("000001.SZ")
        assert loaded["income"] is not None and len(loaded["income"]) == 2
        assert loaded["balance"] is not None and len(loaded["balance"]) >= 1
        assert loaded["cashflow"] is not None and len(loaded["cashflow"]) >= 1
