"""One-shot SF probe: list tables + identify clinical_notes_long candidate."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

ctx, cur = get_cursor()

print("=== Tables in THYROID_VALIDATION.PUBLIC ===")
cur.execute("SHOW TABLES IN THYROID_VALIDATION.PUBLIC")
tables = cur.fetchall()
for r in tables:
    print(f"  {r[1]:60s}  rows={r[3]:>10}")

# Look for note-text-bearing tables in any DB the role can see
print("\n=== Note-text-bearing tables (any DB visible to role) ===")
cur.execute("""
    SELECT table_catalog, table_schema, table_name, row_count
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE table_name ILIKE '%note%' OR table_name ILIKE '%clinical%'
    ORDER BY row_count DESC NULLS LAST
    LIMIT 20
""")
for r in cur.fetchall():
    print(f"  {r[0]}.{r[1]}.{r[2]:50s}  rows={r[3]}")

ctx.close()
