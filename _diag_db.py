import sys
sys.path.insert(0, '.')
import duckdb

con = duckdb.connect('data/easyxt.duckdb', read_only=True)

print('=== stock_1m schema ===')
print(con.execute('DESCRIBE stock_1m').fetchdf().to_string())

print()
print('=== stock_1m count ===')
print(con.execute('SELECT COUNT(*) FROM stock_1m').fetchone())

print()
print('=== stock_1m top codes ===')
print(con.execute("SELECT stock_code, COUNT(*) as cnt FROM stock_1m GROUP BY stock_code ORDER BY cnt DESC LIMIT 5").fetchdf().to_string())

print()
# Check for 000988 or any available code
sample_code = con.execute("SELECT stock_code FROM stock_1m LIMIT 1").fetchone()
if sample_code:
    code = sample_code[0]
    print(f'=== sample from {code} ===')
    print(con.execute(f"SELECT * FROM stock_1m WHERE stock_code='{code}' ORDER BY time LIMIT 5").fetchdf().to_string())
    print()
    print(f'=== time column type check ===')
    print(con.execute(f"SELECT typeof(time) FROM stock_1m WHERE stock_code='{code}' LIMIT 1").fetchone())

print()
print('=== stock_daily schema ===')
print(con.execute('DESCRIBE stock_daily').fetchdf().to_string())
sample = con.execute("SELECT stock_code FROM stock_daily LIMIT 1").fetchone()
if sample:
    code = sample[0]
    print(f'=== stock_daily sample from {code} ===')
    print(con.execute(f"SELECT * FROM stock_daily WHERE stock_code='{code}' ORDER BY date LIMIT 3").fetchdf().to_string())

con.close()
