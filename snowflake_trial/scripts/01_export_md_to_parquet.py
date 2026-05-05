"""
Export key MotherDuck tables to local Parquet for Snowflake load.

Run from repo root with .venv activated:
    python snowflake_trial/scripts/01_export_md_to_parquet.py

Reads from: thyroid_canonical_publication_v1_0 (via scripts._md_connect.connect_locked)
Writes to:  <repo>/snowflake_trial/parquet/<table>.parquet
"""
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))
from _md_connect import connect_locked  # noqa: E402

OUT = REPO_ROOT / "snowflake_trial" / "parquet"
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

con = connect_locked()

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

# mig_289: manuscript_workspace cohort views (already-flat, cross-schema export)
COHORT_VIEWS = [
    ("manuscript_workspace.cohort_m044_ajcc_ete_v1",       "cohort_m044_ajcc_ete_v1"),
    ("manuscript_workspace.cohort_m037_ln_metastasis_v1",  "cohort_m037_ln_metastasis_v1"),
    ("manuscript_workspace.cohort_m025_tirads_performance_v1", "cohort_m025_tirads_performance_v1"),
    ("manuscript_workspace.cohort_m032_descriptive_25yr_v1", "cohort_m032_descriptive_25yr_v1"),
    ("main.cohort_m038_massive_goiter_v1",                 "cohort_m038_massive_goiter_v1"),
]
print("\n=== mig_289 cohort views ===")
for src, name in COHORT_VIEWS:
    out = OUT / f"{name}.parquet"
    t0 = time.time()
    try:
        n = con.sql(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
        con.sql(
            f"COPY (SELECT * FROM {src}) TO '{out}' "
            f"(FORMAT 'parquet', COMPRESSION 'zstd')"
        )
        sz = out.stat().st_size / 1024 / 1024
        dt = time.time() - t0
        manifest.append((name, n, sz, dt))
        print(f"OK   {name:50s} rows={n:>7,}  size={sz:>7.1f}MB  t={dt:>5.1f}s")
    except Exception as e:
        print(f"SKIP {name}: {e}")

print("\n=== manifest ===")
for t, n, sz, dt in manifest:
    print(f"{t},{n},{sz:.2f},{dt:.2f}")
