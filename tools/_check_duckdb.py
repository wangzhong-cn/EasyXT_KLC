"""Quick DuckDB inventory check"""
import duckdb, os

DB_PATH = 'D:/StockData/stock_data.ddb'
if not os.path.exists(DB_PATH):
    print(f"DB not found: {DB_PATH}")
    exit()

con = duckdb.connect(DB_PATH, read_only=True)

# List all tables
tables = con.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
).fetchall()
print(f"Tables: {[t[0] for t in tables]}")

for t in tables:
    tn = t[0]
    cnt_row = con.execute(f'SELECT COUNT(*) FROM "{tn}"').fetchone()
    cnt = int(cnt_row[0]) if cnt_row else 0
    cols = con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name=?",
        [tn]
    ).fetchall()
    print(f"  {tn}: {cnt:,} rows")
    print(f"    cols: {[c[0] for c in cols]}")

    # Sample a few rows
    if cnt > 0:
        sample = con.execute(f'SELECT * FROM "{tn}" LIMIT 3').df()
        print(f"    sample:\n{sample.to_string()}")

# Check unique stock codes in largest table
for tn in [t[0] for t in tables]:
    cnt_row = con.execute(f'SELECT COUNT(*) FROM "{tn}"').fetchone()
    cnt = int(cnt_row[0]) if cnt_row else 0
    if cnt > 100:
        try:
            codes = con.execute(
                f'SELECT DISTINCT stock_code FROM "{tn}" ORDER BY stock_code LIMIT 50'
            ).fetchall()
            print(f"\n  [{tn}] {len(codes)} unique codes (first 50): {[c[0] for c in codes[:20]]}")
        except:
            pass
        try:
            periods = con.execute(
                f'SELECT DISTINCT period FROM "{tn}"'
            ).fetchall()
            print(f"  [{tn}] periods: {[p[0] for p in periods]}")
        except:
            pass

con.close()
