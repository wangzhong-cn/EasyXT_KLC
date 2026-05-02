import sys
sys.path.insert(0, '.')
import duckdb

con = duckdb.connect('data/easyxt.duckdb', read_only=True)

print('=== data_ingestion_status counts ===')
try:
    df = con.execute("SELECT period, status, source, COUNT(*) as cnt FROM data_ingestion_status GROUP BY period, status, source ORDER BY cnt DESC LIMIT 20").fetchdf()
    print(df.to_string())
except Exception as e:
    print('Error:', e)
print()

print('=== data_ingestion_status failed records ===')
try:
    df = con.execute("SELECT stock_code, period, status, source, error_message, created_at FROM data_ingestion_status WHERE status='failed' ORDER BY created_at DESC LIMIT 20").fetchdf()
    print(df.to_string())
except Exception as e:
    print('Error:', e)
print()

print('=== 1m records ===')
try:
    df = con.execute("SELECT stock_code, period, source, status, error_message, record_count, created_at FROM data_ingestion_status WHERE period='1m' ORDER BY created_at DESC LIMIT 15").fetchdf()
    print(df.to_string())
except Exception as e:
    print('Error:', e)
print()

# Check what's in stock_5m
print('=== stock_5m summary ===')
try:
    df = con.execute("SELECT stock_code, period, MIN(datetime) as first_dt, MAX(datetime) as last_dt, COUNT(*) as cnt FROM stock_5m GROUP BY stock_code, period ORDER BY cnt DESC LIMIT 10").fetchdf()
    print(df.to_string())
except Exception as e:
    print('Error:', e)
print()

# Check write_audit_log
print('=== write_audit_log recent ===')
try:
    df = con.execute("DESCRIBE write_audit_log").fetchdf()
    print(df.to_string())
    df2 = con.execute("SELECT * FROM write_audit_log ORDER BY created_at DESC LIMIT 10").fetchdf()
    print(df2.to_string())
except Exception as e:
    print('Error:', e)

con.close()
