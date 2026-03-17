#!/usr/bin/env python3
"""
财务数据保存到DuckDB模块
将QMT的财务数据保存到本地DuckDB数据库
"""

from typing import Optional

import pandas as pd


class FinancialDataSaver:
    """财务数据保存器"""

    def __init__(self, db_manager):
        """
        初始化财务数据保存器

        Args:
            db_manager: DuckDB数据库管理器实例
        """
        self.db_manager = db_manager
        self._create_tables()

    def _create_tables(self):
        """创建财务数据表"""
        # 创建利润表数据表
        self.db_manager.execute_write_query("""
            CREATE TABLE IF NOT EXISTS financial_income (
                stock_code VARCHAR(20),
                report_date VARCHAR(10),
                announce_date VARCHAR(10),
                revenue DOUBLE,
                operating_revenue DOUBLE,
                total_operating_cost DOUBLE,
                net_profit DOUBLE,
                net_profit_parent DOUBLE,
                gross_profit DOUBLE,
                operating_profit DOUBLE,
                total_profit DOUBLE,
                eps_basic DOUBLE,
                eps_diluted DOUBLE,
                roe DOUBLE,
                roa DOUBLE,
                net_margin DOUBLE,
                gross_margin DOUBLE,
                operating_margin DOUBLE,
                data_source VARCHAR(20),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (stock_code, report_date)
            )
        """)

        # 创建资产负债表数据表
        self.db_manager.execute_write_query("""
            CREATE TABLE IF NOT EXISTS financial_balance (
                stock_code VARCHAR(20),
                report_date VARCHAR(10),
                announce_date VARCHAR(10),
                total_assets DOUBLE,
                total_liabilities DOUBLE,
                total_equity DOUBLE,
                current_assets DOUBLE,
                current_liabilities DOUBLE,
                fixed_assets DOUBLE,
                intangible_assets DOUBLE,
                debt_to_asset_ratio DOUBLE,
                current_ratio DOUBLE,
                quick_ratio DOUBLE,
                data_source VARCHAR(20),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (stock_code, report_date)
            )
        """)

        # 创建现金流量表数据表
        self.db_manager.execute_write_query("""
            CREATE TABLE IF NOT EXISTS financial_cashflow (
                stock_code VARCHAR(20),
                report_date VARCHAR(10),
                announce_date VARCHAR(10),
                operating_cash_flow DOUBLE,
                investing_cash_flow DOUBLE,
                financing_cash_flow DOUBLE,
                net_cash_flow DOUBLE,
                cash_equivalents_begin DOUBLE,
                cash_equivalents_end DOUBLE,
                data_source VARCHAR(20),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (stock_code, report_date)
            )
        """)

        # 创建索引以提高查询性能
        try:
            self.db_manager.execute_write_query("CREATE INDEX IF NOT EXISTS idx_income_stock ON financial_income(stock_code)")
            self.db_manager.execute_write_query("CREATE INDEX IF NOT EXISTS idx_balance_stock ON financial_balance(stock_code)")
            self.db_manager.execute_write_query("CREATE INDEX IF NOT EXISTS idx_cashflow_stock ON financial_cashflow(stock_code)")
        except Exception:
            # 索引可能已存在，忽略错误
            pass

    def save_from_qmt(self, stock_code: str, income_df: pd.DataFrame,
                      balance_df: Optional[pd.DataFrame] = None,
                      cashflow_df: Optional[pd.DataFrame] = None) -> dict:
        """
        保存QMT财务数据到DuckDB

        Args:
            stock_code: 股票代码
            income_df: 利润表DataFrame
            balance_df: 资产负债表DataFrame
            cashflow_df: 现金流量表DataFrame

        Returns:
            保存结果统计
        """
        result = {
            'stock_code': stock_code,
            'income_count': 0,
            'balance_count': 0,
            'cashflow_count': 0,
            'success': False,
            'error': None
        }

        try:
            # 保存利润表数据
            if income_df is not None and not income_df.empty:
                income_records = self._prepare_income_data(stock_code, income_df)
                if income_records:
                    income_df_save = pd.DataFrame(income_records)
                    self._save_to_table('financial_income', income_df_save)
                    result['income_count'] = len(income_records)

            # 保存资产负债表数据
            if balance_df is not None and not balance_df.empty:
                balance_records = self._prepare_balance_data(stock_code, balance_df)
                if balance_records:
                    balance_df_save = pd.DataFrame(balance_records)
                    self._save_to_table('financial_balance', balance_df_save)
                    result['balance_count'] = len(balance_records)

            # 保存现金流量表数据
            if cashflow_df is not None and not cashflow_df.empty:
                cashflow_records = self._prepare_cashflow_data(stock_code, cashflow_df)
                if cashflow_records:
                    cashflow_df_save = pd.DataFrame(cashflow_records)
                    self._save_to_table('financial_cashflow', cashflow_df_save)
                    result['cashflow_count'] = len(cashflow_records)

            result['success'] = True

        except Exception as e:
            result['error'] = str(e)

        return result

    def _prepare_income_data(self, stock_code: str, df: pd.DataFrame) -> list[dict]:
        """准备利润表数据"""
        records = []

        for _, row in df.iterrows():
            # 获取时间戳并转换为日期
            timetag = row.get('m_timetag')
            if pd.isna(timetag):
                continue

            report_date = self._format_timetag(timetag)
            announce_date = self._format_timetag(row.get('m_anntime', timetag))

            # 提取数据
            revenue = row.get('revenue', 0)
            if pd.isna(revenue) or revenue == 0:
                revenue = row.get('operating_revenue', 0)

            operating_cost = row.get('total_operating_cost', 0)
            net_profit = row.get('net_profit_incl_min_int_inc', 0)
            net_profit_parent = row.get('net_profit_excl_min_int_inc', 0)
            gross_profit = revenue - operating_cost if revenue > 0 else 0

            operating_profit = row.get('oper_profit', 0)
            total_profit = row.get('tot_profit', 0)

            eps_basic = row.get('s_fa_eps_basic', 0)
            eps_diluted = row.get('s_fa_eps_diluted', 0)

            # 计算比率（需要从资产负债表获取，暂时设为NULL）
            roe = None
            roa = None

            # 计算利润率
            net_margin = (net_profit / revenue * 100) if revenue > 0 else None
            gross_margin = (gross_profit / revenue * 100) if revenue > 0 else None
            operating_margin = (operating_profit / revenue * 100) if revenue > 0 else None

            records.append({
                'stock_code': stock_code,
                'report_date': report_date,
                'announce_date': announce_date,
                'revenue': revenue,
                'operating_revenue': row.get('operating_revenue', 0),
                'total_operating_cost': operating_cost,
                'net_profit': net_profit,
                'net_profit_parent': net_profit_parent,
                'gross_profit': gross_profit,
                'operating_profit': operating_profit,
                'total_profit': total_profit,
                'eps_basic': eps_basic,
                'eps_diluted': eps_diluted,
                'roe': roe,
                'roa': roa,
                'net_margin': net_margin,
                'gross_margin': gross_margin,
                'operating_margin': operating_margin,
                'data_source': 'QMT'
            })

        return records

    def _prepare_balance_data(self, stock_code: str, df: pd.DataFrame) -> list[dict]:
        """准备资产负债表数据"""
        records = []

        for _, row in df.iterrows():
            # 获取时间戳并转换为日期
            timetag = row.get('m_timetag')
            if pd.isna(timetag):
                continue

            report_date = self._format_timetag(timetag)
            announce_date = self._format_timetag(row.get('m_anntime', timetag))

            # 提取数据
            total_assets = row.get('tot_assets', 0)
            total_liabilities = row.get('tot_liab', 0)
            total_equity = row.get('total_equity', 0)
            if pd.isna(total_equity) or total_equity == 0:
                total_equity = total_assets - total_liabilities

            current_assets = row.get('total_current_assets', 0)
            current_liabilities = row.get('total_current_liability', 0)

            fixed_assets = row.get('fix_assets', 0)
            intangible_assets = row.get('intang_assets', 0)

            # 计算比率
            debt_to_asset_ratio = (total_liabilities / total_assets * 100) if total_assets > 0 else None
            current_ratio = (current_assets / current_liabilities) if current_liabilities > 0 else None
            quick_ratio = None  # 需要更详细的流动资产数据

            records.append({
                'stock_code': stock_code,
                'report_date': report_date,
                'announce_date': announce_date,
                'total_assets': total_assets,
                'total_liabilities': total_liabilities,
                'total_equity': total_equity,
                'current_assets': current_assets,
                'current_liabilities': current_liabilities,
                'fixed_assets': fixed_assets,
                'intangible_assets': intangible_assets,
                'debt_to_asset_ratio': debt_to_asset_ratio,
                'current_ratio': current_ratio,
                'quick_ratio': quick_ratio,
                'data_source': 'QMT'
            })

        return records

    def _prepare_cashflow_data(self, stock_code: str, df: pd.DataFrame) -> list[dict]:
        """准备现金流量表数据"""
        records = []

        for _, row in df.iterrows():
            # 获取时间戳并转换为日期
            timetag = row.get('m_timetag')
            if pd.isna(timetag):
                continue

            report_date = self._format_timetag(timetag)
            announce_date = self._format_timetag(row.get('m_anntime', timetag))

            # 提取数据
            operating_cf = row.get('net_cash_flows_oper_act', 0)
            investing_cf = row.get('net_cash_flows_inv_act', 0)
            financing_cf = row.get('net_cash_flows_fnc_act', 0)

            net_cf = operating_cf + investing_cf + financing_cf

            cash_begin = row.get('cash_cash_equ_beg_period', 0)
            cash_end = row.get('cash_cash_equ_end_period', 0)

            records.append({
                'stock_code': stock_code,
                'report_date': report_date,
                'announce_date': announce_date,
                'operating_cash_flow': operating_cf,
                'investing_cash_flow': investing_cf,
                'financing_cash_flow': financing_cf,
                'net_cash_flow': net_cf,
                'cash_equivalents_begin': cash_begin,
                'cash_equivalents_end': cash_end,
                'data_source': 'QMT'
            })

        return records

    def save_from_tushare(
        self,
        stock_code: str,
        start_date: str = "",
        end_date: str = "",
    ) -> dict:
        """通过 Tushare pro 接口获取财务数据并写入 DuckDB（QMT 不可用时的降级路径）。

        Tushare 股票代码格式：``000001.SZ`` / ``600000.SH``
        映射规则：``000001.SZ`` → ts_code ``000001.SZ``（直接使用，EasyXT 格式与 Tushare 相同）

        Args:
            stock_code: 股票代码（EasyXT 格式，如 ``000001.SZ``）
            start_date:  查询起始报告期，格式 ``YYYY-MM-DD``，空字符串=不限
            end_date:    查询截止报告期，格式 ``YYYY-MM-DD``，空字符串=不限

        Returns:
            结果 dict，结构与 ``save_from_qmt`` 相同。
        """
        import os

        result: dict = {
            "stock_code": stock_code,
            "income_count": 0,
            "balance_count": 0,
            "cashflow_count": 0,
            "success": False,
            "source": "tushare",
            "error": None,
        }

        token = (
            os.environ.get("EASYXT_TUSHARE_TOKEN", "").strip()
            or os.environ.get("TUSHARE_TOKEN", "").strip()
        )
        if not token:
            result["error"] = "EASYXT_TUSHARE_TOKEN 未设置，跳过 Tushare 财务数据更新"
            return result

        try:
            import tushare as ts  # type: ignore[import]
        except ImportError:
            result["error"] = "tushare 未安装，请执行 pip install tushare"
            return result

        try:
            pro = ts.pro_api(token)
        except Exception as exc:
            result["error"] = f"Tushare pro_api 初始化失败: {exc}"
            return result

        # Tushare 日期格式 YYYYMMDD，EasyXT 日期格式 YYYY-MM-DD
        def _to_ts_date(d: str) -> str:
            return d.replace("-", "") if d else ""

        ts_code = stock_code  # 均为 000001.SZ 格式，无需转换
        ts_start = _to_ts_date(start_date)
        ts_end = _to_ts_date(end_date)

        # ── 利润表 ──────────────────────────────────────────────────────────
        try:
            kwargs: dict = {"ts_code": ts_code}
            if ts_start:
                kwargs["start_date"] = ts_start
            if ts_end:
                kwargs["end_date"] = ts_end
            income_raw = pro.income(**kwargs)
            if income_raw is not None and not income_raw.empty:
                records = self._prepare_income_data_tushare(stock_code, income_raw)
                if records:
                    self._save_to_table("financial_income", pd.DataFrame(records))
                    result["income_count"] = len(records)
        except Exception as exc:
            result["error"] = (result["error"] or "") + f"[income:{exc}]"

        # ── 资产负债表 ───────────────────────────────────────────────────────
        try:
            kwargs = {"ts_code": ts_code}
            if ts_start:
                kwargs["start_date"] = ts_start
            if ts_end:
                kwargs["end_date"] = ts_end
            balance_raw = pro.balancesheet(**kwargs)
            if balance_raw is not None and not balance_raw.empty:
                records = self._prepare_balance_data_tushare(stock_code, balance_raw)
                if records:
                    self._save_to_table("financial_balance", pd.DataFrame(records))
                    result["balance_count"] = len(records)
        except Exception as exc:
            result["error"] = (result["error"] or "") + f"[balance:{exc}]"

        # ── 现金流量表 ───────────────────────────────────────────────────────
        try:
            kwargs = {"ts_code": ts_code}
            if ts_start:
                kwargs["start_date"] = ts_start
            if ts_end:
                kwargs["end_date"] = ts_end
            cashflow_raw = pro.cashflow(**kwargs)
            if cashflow_raw is not None and not cashflow_raw.empty:
                records = self._prepare_cashflow_data_tushare(stock_code, cashflow_raw)
                if records:
                    self._save_to_table("financial_cashflow", pd.DataFrame(records))
                    result["cashflow_count"] = len(records)
        except Exception as exc:
            result["error"] = (result["error"] or "") + f"[cashflow:{exc}]"

        result["success"] = (
            result["income_count"] > 0
            or result["balance_count"] > 0
            or result["cashflow_count"] > 0
        )
        return result

    # ── Tushare 字段映射辅助方法 ─────────────────────────────────────────────

    def _ts_date(self, val) -> Optional[str]:
        """将 Tushare YYYYMMDD 字符串转为 YYYY-MM-DD，None/NaN 返回 None。"""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:]}"
        return s[:10] if s else None

    def _fv(self, row, *keys) -> float:
        """从行中依序尝试 keys，返回第一个有效 float，默认 0。"""
        for k in keys:
            v = row.get(k)
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                try:
                    return float(v)
                except (TypeError, ValueError):
                    continue
        return 0.0

    def _prepare_income_data_tushare(self, stock_code: str, df: pd.DataFrame) -> list[dict]:
        """将 Tushare income() 结果映射到 financial_income 表字段。"""
        records = []
        for _, row in df.iterrows():
            report_date = self._ts_date(row.get("end_date"))
            if not report_date:
                continue
            announce_date = self._ts_date(row.get("ann_date")) or report_date
            revenue = self._fv(row, "revenue", "total_revenue")
            op_cost = self._fv(row, "total_operate_cost", "oper_cost")
            net_profit = self._fv(row, "n_income", "net_profit")
            net_profit_parent = self._fv(row, "n_income_attr_p", "n_income")
            gross_profit = revenue - op_cost if revenue > 0 else 0.0
            op_profit = self._fv(row, "operate_profit", "oper_profit")
            total_profit = self._fv(row, "total_profit")
            eps_basic = self._fv(row, "basic_eps")
            eps_diluted = self._fv(row, "diluted_eps")
            net_margin = (net_profit / revenue * 100) if revenue > 0 else None
            gross_margin = (gross_profit / revenue * 100) if revenue > 0 else None
            op_margin = (op_profit / revenue * 100) if revenue > 0 else None
            records.append({
                "stock_code": stock_code,
                "report_date": report_date,
                "announce_date": announce_date,
                "revenue": revenue,
                "operating_revenue": revenue,
                "total_operating_cost": op_cost,
                "net_profit": net_profit,
                "net_profit_parent": net_profit_parent,
                "gross_profit": gross_profit,
                "operating_profit": op_profit,
                "total_profit": total_profit,
                "eps_basic": eps_basic,
                "eps_diluted": eps_diluted,
                "roe": None,
                "roa": None,
                "net_margin": net_margin,
                "gross_margin": gross_margin,
                "operating_margin": op_margin,
                "data_source": "Tushare",
            })
        return records

    def _prepare_balance_data_tushare(self, stock_code: str, df: pd.DataFrame) -> list[dict]:
        """将 Tushare balancesheet() 结果映射到 financial_balance 表字段。"""
        records = []
        for _, row in df.iterrows():
            report_date = self._ts_date(row.get("end_date"))
            if not report_date:
                continue
            announce_date = self._ts_date(row.get("ann_date")) or report_date
            total_assets = self._fv(row, "total_assets")
            total_liab = self._fv(row, "total_liab")
            total_equity = self._fv(row, "total_hldr_eqy_inc_min_int", "total_equity")
            if total_equity == 0.0 and total_assets > 0:
                total_equity = total_assets - total_liab
            cur_assets = self._fv(row, "total_cur_assets")
            cur_liab = self._fv(row, "total_cur_liab")
            fix_assets = self._fv(row, "fix_assets")
            intang = self._fv(row, "intan_assets")
            d2a = (total_liab / total_assets * 100) if total_assets > 0 else None
            cur_ratio = (cur_assets / cur_liab) if cur_liab > 0 else None
            records.append({
                "stock_code": stock_code,
                "report_date": report_date,
                "announce_date": announce_date,
                "total_assets": total_assets,
                "total_liabilities": total_liab,
                "total_equity": total_equity,
                "current_assets": cur_assets,
                "current_liabilities": cur_liab,
                "fixed_assets": fix_assets,
                "intangible_assets": intang,
                "debt_to_asset_ratio": d2a,
                "current_ratio": cur_ratio,
                "quick_ratio": None,
                "data_source": "Tushare",
            })
        return records

    def _prepare_cashflow_data_tushare(self, stock_code: str, df: pd.DataFrame) -> list[dict]:
        """将 Tushare cashflow() 结果映射到 financial_cashflow 表字段。"""
        records = []
        for _, row in df.iterrows():
            report_date = self._ts_date(row.get("end_date"))
            if not report_date:
                continue
            announce_date = self._ts_date(row.get("ann_date")) or report_date
            op_cf = self._fv(row, "n_cashflow_act")
            inv_cf = self._fv(row, "n_cashflow_inv_act")
            fin_cf = self._fv(row, "n_cash_flows_fnc_act")
            net_cf = op_cf + inv_cf + fin_cf
            cash_begin = self._fv(row, "c_cash_equ_beg_period")
            cash_end = self._fv(row, "c_cash_equ_end_period")
            records.append({
                "stock_code": stock_code,
                "report_date": report_date,
                "announce_date": announce_date,
                "operating_cash_flow": op_cf,
                "investing_cash_flow": inv_cf,
                "financing_cash_flow": fin_cf,
                "net_cash_flow": net_cf,
                "cash_equivalents_begin": cash_begin,
                "cash_equivalents_end": cash_end,
                "data_source": "Tushare",
            })
        return records

    def _format_timetag(self, timetag) -> Optional[str]:
        """格式化时间戳为日期字符串"""
        if pd.isna(timetag):
            return None

        if isinstance(timetag, (int, float)):
            timetag_str = str(int(timetag))
            if len(timetag_str) == 8:
                return f"{timetag_str[0:4]}-{timetag_str[4:6]}-{timetag_str[6:8]}"
        return str(timetag)[:10]

    def _save_to_table(self, table_name: str, df: pd.DataFrame):
        """保存DataFrame到表（使用UPSERT）"""
        if df is None or df.empty:
            return

        df = df.dropna(subset=['stock_code', 'report_date'])
        if 'announce_date' in df.columns:
            df = df.sort_values(['stock_code', 'report_date', 'announce_date'])
        df = df.drop_duplicates(subset=['stock_code', 'report_date'], keep='last')

        # 先删除已存在的数据
        for _, row in df.iterrows():
            stock_code = row['stock_code']
            report_date = row['report_date']
            self.db_manager.execute_write_query(
                "DELETE FROM " + table_name + " WHERE stock_code = ? AND report_date = ?",
                (stock_code, report_date),
            )

        # 插入新数据
        self.db_manager.insert_dataframe(table_name, df)

    def load_financial_data(self, stock_code: str, start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> dict:
        """
        从DuckDB加载财务数据

        Args:
            stock_code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）

        Returns:
            包含income, balance, cashflow的字典
        """
        result = {
            'income': None,
            'balance': None,
            'cashflow': None
        }

        try:
            # 查询利润表
            income_params = [stock_code]
            income_cond = "WHERE stock_code = ?"
            if start_date:
                income_cond += " AND report_date >= ?"
                income_params.append(start_date)
            if end_date:
                income_cond += " AND report_date <= ?"
                income_params.append(end_date)
            income_query = "SELECT * FROM financial_income " + income_cond + " ORDER BY report_date DESC"
            result['income'] = self.db_manager.execute_read_query(income_query, tuple(income_params))

            # 查询资产负债表
            balance_params = [stock_code]
            balance_cond = "WHERE stock_code = ?"
            if start_date:
                balance_cond += " AND report_date >= ?"
                balance_params.append(start_date)
            if end_date:
                balance_cond += " AND report_date <= ?"
                balance_params.append(end_date)
            balance_query = "SELECT * FROM financial_balance " + balance_cond + " ORDER BY report_date DESC"
            result['balance'] = self.db_manager.execute_read_query(balance_query, tuple(balance_params))

            # 查询现金流量表
            cashflow_params = [stock_code]
            cashflow_cond = "WHERE stock_code = ?"
            if start_date:
                cashflow_cond += " AND report_date >= ?"
                cashflow_params.append(start_date)
            if end_date:
                cashflow_cond += " AND report_date <= ?"
                cashflow_params.append(end_date)
            cashflow_query = "SELECT * FROM financial_cashflow " + cashflow_cond + " ORDER BY report_date DESC"
            result['cashflow'] = self.db_manager.execute_read_query(cashflow_query, tuple(cashflow_params))

        except Exception as e:
            print(f"Error loading financial data: {e}")

        return result

    def get_financial_summary(self, stock_code: str) -> pd.DataFrame:
        """
        获取财务指标汇总（用于在数据查看器中显示）

        Args:
            stock_code: 股票代码

        Returns:
            包含报告期、净资产收益率、毛利率、净利率、资产负债率的DataFrame
        """
        query = """
            SELECT
                i.report_date as '报告期',
                i.roe as '净资产收益率',
                i.gross_margin as '毛利率',
                i.net_margin as '净利率',
                b.debt_to_asset_ratio as '资产负债率'
            FROM financial_income i
            LEFT JOIN financial_balance b ON i.stock_code = b.stock_code AND i.report_date = b.report_date
            WHERE i.stock_code = ?
            ORDER BY i.report_date DESC
        """

        try:
            return self.db_manager.execute_read_query(query, (stock_code,))
        except Exception as e:
            print(f"Error getting financial summary: {e}")
            return pd.DataFrame()
