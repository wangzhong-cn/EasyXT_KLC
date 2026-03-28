import sys
sys.path.insert(0, '.')
import duckdb

con = duckdb.connect('data/easyxt.duckdb', read_only=True)

print('=== custom_period_bars schema ===')
print(con.execute('DESCRIBE custom_period_bars').fetchdf().to_string())
print()
print('=== custom_period_bars count ===')
print(con.execute('SELECT COUNT(*) FROM custom_period_bars').fetchone())
print()
print('=== records in custom_period_bars ===')
try:
    print(con.execute("SELECT stock_code, period, COUNT(*) as cnt FROM custom_period_bars GROUP BY stock_code, period ORDER BY cnt DESC LIMIT 10").fetchdf().to_string())
except Exception as e:
    print('Error:', e)
print()

# Check stock_daily count per code
print('=== stock_daily codes with most data ===')
print(con.execute("SELECT stock_code, COUNT(*) as cnt FROM stock_daily GROUP BY stock_code ORDER BY cnt DESC LIMIT 5").fetchdf().to_string())
print()

# Check 000988.SZ daily data
print('=== 000988.SZ daily data (last 3) ===')
try:
    r = con.execute("SELECT * FROM stock_daily WHERE stock_code='000988.SZ' ORDER BY date DESC LIMIT 3").fetchdf()
    print(r.to_string())
except Exception as e:
    print('Error:', e)
print()

# Check if custom_period_bars has 000988.SZ
print('=== custom_period_bars sample ===')
try:
    sample = con.execute("SELECT * FROM custom_period_bars LIMIT 3").fetchdf()
    print(sample.to_string())
    print('time column dtype:', sample['time'].dtype if 'time' in sample.columns else 'no time column')
except Exception as e:
    print('Error:', e)

# Check stock_5m
print()
print('=== stock_5m count ===')
print(con.execute('SELECT COUNT(*) FROM stock_5m').fetchone())

con.close()
