"""N4 + N5 — SF AI_CLASSIFY pilots: Hashimoto + Graves disease.

Same pattern as N1/N2/N3. Per feedback_nlp_refresh_on_snowflake.md.
PHI policy: notes processed in-database; no note text in reports/logs/CSVs.
"""
from __future__ import annotations
import os, sys
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
import duckdb

REPORTS = Path("/Users/ros/THyroid 2026/snowflake_trial/reports")
PARQ = Path("/Users/ros/THyroid 2026/snowflake_trial/parquet")
PARQ.mkdir(parents=True, exist_ok=True)
MD_TOKEN = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
assert MD_TOKEN

# Probe corpus sizes first
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={MD_TOKEN}")
hashi_n = md.execute("""
SELECT COUNT(*) FROM main.clinical_notes_long
WHERE LOWER(note_text) LIKE '%hashimoto%' OR LOWER(note_text) LIKE '%lymphocytic thyroiditis%'
   OR LOWER(note_text) LIKE '%autoimmune thyroiditis%' OR LOWER(note_text) LIKE '%chronic thyroiditis%'
   OR LOWER(note_text) LIKE '%aitd%'
""").fetchone()[0]
graves_n = md.execute("""
SELECT COUNT(*) FROM main.clinical_notes_long
WHERE LOWER(note_text) LIKE '%graves%' OR LOWER(note_text) LIKE '%hyperthyroid%'
   OR LOWER(note_text) LIKE '%methimazole%' OR LOWER(note_text) LIKE '%ptu%'
   OR LOWER(note_text) LIKE '%propylthiouracil%' OR LOWER(note_text) LIKE '%toxic goiter%'
   OR LOWER(note_text) LIKE '%thyrotoxic%'
""").fetchone()[0]
md.close()
print(f"Corpus sizes: Hashimoto={hashi_n} notes / Graves={graves_n} notes")


def run_pilot(slice_name, sql_filter, classes, decision_label, full=False):
    parq_file = PARQ / f"_nlp_{slice_name}_{'full' if full else 'pilot'}_notes.parquet"
    report = REPORTS / f"{slice_name}_{'full' if full else 'pilot'}.md"
    notes_table = f"NLP_{slice_name.upper()}_{'FULL' if full else 'PILOT'}_NOTES"
    results_table = f"NLP_{slice_name.upper()}_{'FULL' if full else 'PILOT'}_RESULTS_v1"

    print(f"\n=== {slice_name.upper()} {'full-scale' if full else 'pilot'} ===")

    md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={MD_TOKEN}")
    limit_clause = "" if full else "ORDER BY RANDOM() LIMIT 100"
    md.execute(f"""
COPY (SELECT research_id, note_type, CAST(note_index AS INTEGER) AS note_index,
             SUBSTR(note_text, 1, 8000) AS note_text
      FROM main.clinical_notes_long WHERE {sql_filter} {limit_clause})
TO '{parq_file}' (FORMAT 'parquet')
""")
    md.close()

    ctx, cur = get_cursor()
    cur.execute("USE DATABASE THYROID_VALIDATION")
    cur.execute("USE SCHEMA PUBLIC")
    cur.execute(f"CREATE OR REPLACE TABLE {notes_table} (RESEARCH_ID VARCHAR, NOTE_TYPE VARCHAR, NOTE_INDEX INTEGER, NOTE_TEXT VARCHAR)")
    cur.execute("CREATE STAGE IF NOT EXISTS COWORK_STAGE")
    cur.execute(f"PUT 'file://{parq_file}' @COWORK_STAGE/{slice_name}_{'full' if full else 'pilot'}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    cur.execute(f"""
COPY INTO {notes_table} (RESEARCH_ID, NOTE_TYPE, NOTE_INDEX, NOTE_TEXT)
FROM (SELECT $1:research_id::VARCHAR, $1:note_type::VARCHAR,
             $1:note_index::INTEGER, $1:note_text::VARCHAR
      FROM @COWORK_STAGE/{slice_name}_{'full' if full else 'pilot'}/_nlp_{slice_name}_{'full' if full else 'pilot'}_notes.parquet)
FILE_FORMAT = (TYPE = PARQUET)
""")
    cur.execute(f"SELECT COUNT(*) FROM {notes_table}")
    n_loaded = cur.fetchone()[0]
    print(f"  loaded {n_loaded:,}")

    classes_sql = "[" + ", ".join(f"'{c}'" for c in classes) + "]"
    t0 = datetime.now()
    cur.execute(f"""
CREATE OR REPLACE TABLE {results_table} AS
SELECT RESEARCH_ID, NOTE_TYPE, NOTE_INDEX,
       AI_CLASSIFY(NOTE_TEXT, {classes_sql}) AS classification_raw,
       classification_raw:labels[0]::VARCHAR AS {decision_label},
       CURRENT_TIMESTAMP AS classified_at,
       'AI_CLASSIFY_snowflake_cortex_20260504' AS llm_model
FROM {notes_table}
""")
    elapsed = (datetime.now() - t0).total_seconds()
    print(f"  AI_CLASSIFY: {elapsed:.1f}s")

    cur.execute(f"SELECT {decision_label}, COUNT(*) FROM {results_table} GROUP BY 1 ORDER BY 2 DESC")
    rows = cur.fetchall()
    actionable_classes = [c for c in classes if 'unknown' not in c.lower() and 'not_mentioned' not in c.lower()]
    actionable_filter = "(" + " OR ".join(f"{decision_label} = '{c}'" for c in actionable_classes) + ")"
    cur.execute(f"SELECT COUNT(*), COUNT_IF({actionable_filter}), COUNT_IF({decision_label} IS NULL) FROM {results_table}")
    total, actionable, null_class = cur.fetchone()
    cur.execute(f"SELECT COUNT(DISTINCT RESEARCH_ID) FROM {results_table} WHERE {actionable_filter}")
    n_pts = cur.fetchone()[0]

    lines = [
        f"# {slice_name} {'full-scale' if full else 'pilot'} — SF AI_CLASSIFY",
        f"**Generated:** {datetime.now().isoformat()}",
        f"**Cohort:** {total:,} notes",
        f"**Elapsed:** {elapsed:.1f}s",
        f"**Distinct pts with ≥1 actionable:** {n_pts:,}",
        "",
        f"## Yield: {actionable:,}/{total:,} = {100*actionable/total:.1f}% actionable",
        "",
        "| status | n |",
        "|---|---:|",
        *[f"| {s} | {n:,} |" for s, n in rows],
    ]
    report.write_text("\n".join(lines))
    print(f"  yield: {100*actionable/total:.1f}% actionable / {n_pts:,} distinct pts")
    ctx.close()
    return {"slice": slice_name, "yield_pct": 100*actionable/total, "n_pts": n_pts, "elapsed": elapsed, "rows": rows}


N4_FILTER = """(LOWER(note_text) LIKE '%hashimoto%' OR LOWER(note_text) LIKE '%lymphocytic thyroiditis%' OR LOWER(note_text) LIKE '%autoimmune thyroiditis%' OR LOWER(note_text) LIKE '%chronic thyroiditis%' OR LOWER(note_text) LIKE '%aitd%')"""
N5_FILTER = """(LOWER(note_text) LIKE '%graves%' OR LOWER(note_text) LIKE '%hyperthyroid%' OR LOWER(note_text) LIKE '%methimazole%' OR LOWER(note_text) LIKE '%ptu%' OR LOWER(note_text) LIKE '%propylthiouracil%' OR LOWER(note_text) LIKE '%toxic goiter%' OR LOWER(note_text) LIKE '%thyrotoxic%')"""

# Pilots first
n4 = run_pilot("HASHIMOTO", N4_FILTER,
    ['hashimoto_present','hashimoto_absent','hashimoto_unknown_or_not_mentioned'], 'hashimoto_status')
n5 = run_pilot("GRAVES", N5_FILTER,
    ['graves_present','graves_absent','graves_unknown_or_not_mentioned'], 'graves_status')

print("\n=== PILOT SUMMARY ===")
print(f"  N4 Hashimoto: {n4['yield_pct']:.1f}% actionable / {n4['n_pts']} pts")
print(f"  N5 Graves:    {n5['yield_pct']:.1f}% actionable / {n5['n_pts']} pts")

# Auto-scale if both ≥70%
if n4['yield_pct'] >= 70 and n5['yield_pct'] >= 70:
    print("\n=== Both pilots cleared 70% gate — running full-scale ===")
    n4f = run_pilot("HASHIMOTO", N4_FILTER,
        ['hashimoto_present','hashimoto_absent','hashimoto_unknown_or_not_mentioned'], 'hashimoto_status', full=True)
    n5f = run_pilot("GRAVES", N5_FILTER,
        ['graves_present','graves_absent','graves_unknown_or_not_mentioned'], 'graves_status', full=True)
    print("\n=== FULL-SCALE SUMMARY ===")
    print(f"  N4 full: {n4f['yield_pct']:.1f}% actionable / {n4f['n_pts']} pts")
    print(f"  N5 full: {n5f['yield_pct']:.1f}% actionable / {n5f['n_pts']} pts")
else:
    print(f"\n  ⚠ One or both pilots below 70% — surface to Logan before scaling")
