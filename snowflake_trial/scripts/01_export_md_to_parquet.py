"""
Export key MotherDuck tables to local Parquet for Snowflake load.

Run from /Users/ros/THyroid 2026/ with .venv activated:
    python snowflake_trial/scripts/01_export_md_to_parquet.py

Reads from: md:thyroid_canonical_publication_v1_0
Writes to:  /Users/ros/THyroid 2026/snowflake_trial/parquet/<table>.parquet
"""
import duckdb, time
from pathlib import Path

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/parquet")
OUT.mkdir(parents=True, exist_ok=True)

# Priority tables for validation prompts 1-12 + cross-validation 13-21
# Pull canonical_patient_master plus the highest-value detail tables
TABLES = [
    "canonical_patient_master",
    # mig_267 / mig_270: histology SSOT for Snowflake JOINs (replaces scattered ILIKE grouping)
    "canonical_histology_lookup_v1",
    "canonical_fna_events_v1",
    "canonical_molecular_genetics_v2",
    "canonical_rai_episodes_v1",
    "canonical_labs_thyroglobulin_v1",
    "canonical_path_malignant_events_v1",
    "canonical_path_gland_events_v1",
    "canonical_operative_events_v1",
    "canonical_complications_events_v1",
    "canonical_us_exam_events_v1",
    "canonical_recurrence_events_v1",
    "canonical_invasion_events_v1",
    # mig_260 / CF-mig260f: live CPM no longer carries v12/imaging/preop TIRADS cols;
    # Snowflake Prompt 7 joins this rollup for max_tirads_category_ever.
    "canonical_us_patient_master_VIEW_v2",
]

con = duckdb.connect("md:thyroid_canonical_publication_v1_0")

# Discover which of these actually exist (tables + views — cupm_v2 is a VIEW)
existing_tables = {
    r[0]
    for r in con.sql(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE database_name='thyroid_canonical_publication_v1_0' AND schema_name='main'"
    ).fetchall()
}
existing_views = {
    r[0]
    for r in con.sql(
        "SELECT DISTINCT table_name FROM information_schema.views "
        "WHERE table_catalog = 'thyroid_canonical_publication_v1_0' "
        "AND table_schema = 'main'"
    ).fetchall()
}
existing = existing_tables | existing_views

print(f"=== {len(existing)} main tables in publication DB ===")

manifest = []
for t in TABLES:
    if t not in existing:
        print(f"SKIP {t} (not found)")
        continue
    out = OUT / f"{t}.parquet"
    t0 = time.time()
    n = con.sql(f"SELECT COUNT(*) FROM main.{t}").fetchone()[0]
    con.sql(
        f"COPY (SELECT * FROM main.{t}) TO '{out}' "
        f"(FORMAT 'parquet', COMPRESSION 'zstd')"
    )
    sz = out.stat().st_size / 1024 / 1024
    dt = time.time() - t0
    manifest.append((t, n, sz, dt))
    print(f"OK   {t:50s} rows={n:>7,}  size={sz:>7.1f}MB  t={dt:>5.1f}s")

print("\n=== manifest ===")
for t, n, sz, dt in manifest:
    print(f"{t},{n},{sz:.2f},{dt:.2f}")
