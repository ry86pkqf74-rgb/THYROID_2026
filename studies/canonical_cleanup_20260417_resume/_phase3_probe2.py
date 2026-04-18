"""Probe DuckDB capabilities for Phase 3."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from _md_connect import connect_locked  # type: ignore

con = connect_locked()

# Try view_column_usage
print("--- view_column_usage availability ---")
try:
    n = con.execute(
        "SELECT COUNT(*) FROM information_schema.view_column_usage"
    ).fetchone()
    print(f"view_column_usage: {n}")
except Exception as e:
    print(f"NO view_column_usage: {e}")

print("--- view_table_usage availability ---")
try:
    n = con.execute(
        "SELECT COUNT(*) FROM information_schema.view_table_usage"
    ).fetchone()
    print(f"view_table_usage: {n}")
except Exception as e:
    print(f"NO view_table_usage: {e}")

print("--- duckdb_dependencies ---")
try:
    n = con.execute("SELECT COUNT(*) FROM duckdb_dependencies()").fetchone()
    print(f"duckdb_dependencies: {n}")
    sample = con.execute("SELECT * FROM duckdb_dependencies() LIMIT 3").fetchall()
    keys = [d[0] for d in con.description]
    print(f"keys: {keys}")
    for r in sample:
        print(r)
except Exception as e:
    print(f"NO duckdb_dependencies: {e}")

# Sample of all main objects (tables + views)
print("--- main objects ---")
mains = con.execute(
    "SELECT table_name, table_type FROM information_schema.tables "
    "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
    "AND table_schema='main' ORDER BY 2, 1"
).fetchall()
print(f"n_main = {len(mains)}")
for t, tt in mains:
    print(f"  {tt}  {t}")

# Get info on extant views
print("--- main views ---")
mvs = con.execute(
    "SELECT table_name, view_definition "
    "FROM information_schema.views "
    "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
    "AND table_schema='main'"
).fetchall()
for n, d in mvs:
    print(f"  view {n}: {d[:200]}...")
