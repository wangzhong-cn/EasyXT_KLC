import sys
sys.path.insert(0,'d:/EasyXT_KLC')
import duckdb

try:
    con = duckdb.connect('d:/EasyXT_KLC/data/stock_data.ddb', read_only=True)
    tables = con.execute('SHOW TABLES').fetchdf()
    print('tables:', tables.iloc[:,0].tolist())
    for tbl in ['stock_daily', 'kline_1d', 'ohlcv_1d']:
        try:
            cnt = con.execute('SELECT COUNT(*) FROM ' + tbl).fetchone()[0]
            row = con.execute('SELECT stock_code, COUNT(*) cnt FROM ' + tbl + " WHERE stock_code='000988.SZ' GROUP BY stock_code").fetchdf()
            print(tbl + ': total=' + str(cnt) + ', 000988=' + repr(row.to_dict()))
        except Exception as e:
            print(tbl + ': ' + str(e))
    con.close()
except Exception as e:
    print('ERROR:', e)
