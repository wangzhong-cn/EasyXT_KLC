import duckdb, pandas as pd
con = duckdb.connect('data/stock_data.ddb', read_only=True)
tables = con.execute('SHOW TABLES').fetchdf()
print('tables:', list(tables.iloc[:,0]))
try:
    df = con.execute(\"SELECT * FROM stock_daily WHERE stock_code='000988.SZ' ORDER BY time LIMIT 5\").fetchdf()
    print('cols:', list(df.columns))
    print(df[['time']].head())
except Exception as e:
    print('error:', e)
con.close()
