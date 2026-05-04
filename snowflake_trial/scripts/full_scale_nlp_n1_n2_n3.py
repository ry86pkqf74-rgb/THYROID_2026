"""Full-scale N1+N2+N3 SF AI_CLASSIFY runs.

Logan-ratified 2026-05-04: accept 49% N3 yield (600 VI-positive in 4,113 malig is
sufficient signal). Full-scale runs feed mig_281 promotion to MD canonicals.

Same pipeline as N1/N2/N3 pilots but with full corpus per slice. PHI policy:
notes processed in-database; no note text in reports.
"""
from __future__ import annotations
import os, sys
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
import duckdb

REPORT_DIR = Path("/Users/ros/THyroid 2026/snowflake_trial/reports")
PARQ_DIR = Path("/Users/ros/THyroid 2026/snowflake_trial/parquet")
PARQ_DIR.mkdir(parents=True, exist_ok=True)
MD_TOKEN = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
assert MD_TOKEN

def run_full(slice_name, sql_filter, classes, decision_label):
    parq = PARQ_DIR / f"_nlp_{slice_name}_full_notes.parquet"
    report = REPORT_DIR / f"{slice_name}_full.md"
    notes_table = f"NLP_{slice_name.upper()}_FULL_NOTES"
    results_table = f"NLP_{slice_name.upper()}_FULL_RESULTS_v1"

    print(f"\n=== {slice_name.upper()} full-scale ===")

    # 1. Export full corpus
    md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={MD_TOKEN}")
    md.execute(f"""
COPY (
  SELECT research_id, note_type, note_index, SUBSTR(note_text, 1, 8000) AS note_text
  FROM main.clinical_notes_long WHERE {sql_filter}
) TO '{parq}' (FORMAT 'parquet')
""")
    n_export = duckdb.connect().execute(f"SELECT COUNT(*) FROM '{parq}'").fetchone()[0]
    md.close()
    print(f"  exported {n_export:,} notes")

    # 2. PUT + COPY INTO SF
    ctx, cur = get_cursor()
    cur.execute("USE DATABASE THYROID_VALIDATION")
    cur.execute("USE SCHEMA PUBLIC")
    cur.execute(f"CREATE OR REPLACE TABLE {notes_table} (RESEARCH_ID VARCHAR, NOTE_TYPE VARCHAR, NOTE_INDEX INTEGER, NOTE_TEXT VARCHAR)")
    cur.execute("CREATE STAGE IF NOT EXISTS COWORK_STAGE")
    cur.execute(f"PUT 'file://{parq}' @COWORK_STAGE/{slice_name}_full/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    cur.execute(f"""
COPY INTO {notes_table} (RESEARCH_ID, NOTE_TYPE, NOTE_INDEX, NOTE_TEXT)
FROM (SELECT $1:research_id::VARCHAR, $1:note_type::VARCHAR, $1:note_index::INTEGER, $1:note_text::VARCHAR
      FROM @COWORK_STAGE/{slice_name}_full/_nlp_{slice_name}_full_notes.parquet)
FILE_FORMAT = (TYPE = PARQUET)
""")
    cur.execute(f"SELECT COUNT(*) FROM {notes_table}")
    n_loaded = cur.fetchone()[0]
    print(f"  loaded {n_loaded:,} notes to SF")

    # 3. AI_CLASSIFY full corpus
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
    print(f"  AI_CLASSIFY elapsed: {elapsed:.1f}s")

    # 4. Yield
    cur.execute(f"SELECT {decision_label}, COUNT(*) FROM {results_table} GROUP BY 1 ORDER BY 2 DESC")
    rows = cur.fetchall()
    actionable_classes = [c for c in classes if 'unknown' not in c.lower() and 'not_mentioned' not in c.lower()]
    actionable_filter = "(" + " OR ".join(f"{decision_label} = '{c}'" for c in actionable_classes) + ")"
    cur.execute(f"SELECT COUNT(*), COUNT_IF({actionable_filter}), COUNT_IF({decision_label} IS NULL) FROM {results_table}")
    total, actionable, null_class = cur.fetchone()

    # Distinct patients with at least one actionable extraction (uplift signal)
    cur.execute(f"SELECT COUNT(DISTINCT RESEARCH_ID) FROM {results_table} WHERE {actionable_filter}")
    n_pts_actionable = cur.fetchone()[0]

    lines = [
        f"# {slice_name} full-scale — SF AI_CLASSIFY",
        f"**Generated:** {datetime.now().isoformat()}",
        f"**Cohort:** {total:,} notes from `main.clinical_notes_long` (full keyword-positive corpus)",
        f"**AI_CLASSIFY elapsed:** {elapsed:.1f}s",
        f"**Distinct patients with ≥1 actionable extraction:** {n_pts_actionable:,}",
        "",
        "## Yield",
        f"| Total notes | Actionable | NULL |",
        f"|---:|---:|---:|",
        f"| {total:,} | {actionable:,} ({100*actionable/total:.1f}%) | {null_class} |",
        "",
        f"## Distribution",
        f"| {decision_label} | n |",
        "|---|---:|",
        *[f"| {s} | {n:,} |" for s, n in rows],
        "",
        "## Tables in Snowflake",
        f"- `THYROID_VALIDATION.PUBLIC.{notes_table}` (PHI text)",
        f"- `THYROID_VALIDATION.PUBLIC.{results_table}` ← consumed by Cursor mig_281",
    ]
    report.write_text("\n".join(lines))
    print(f"  report -> {report}")
    ctx.close()

    return {"slice": slice_name, "total": total, "actionable": actionable, "n_pts_actionable": n_pts_actionable, "elapsed": elapsed}


N1_FILTER = """(LOWER(note_text) LIKE '%smoking%' OR LOWER(note_text) LIKE '%tobacco%' OR LOWER(note_text) LIKE '%pack-year%' OR LOWER(note_text) LIKE '%pack year%')"""
N2_FILTER = """(LOWER(note_text) LIKE '%family history%thyroid%' OR LOWER(note_text) LIKE '%mother%thyroid cancer%' OR LOWER(note_text) LIKE '%father%thyroid cancer%' OR LOWER(note_text) LIKE '%sister%thyroid cancer%' OR LOWER(note_text) LIKE '%brother%thyroid cancer%' OR LOWER(note_text) LIKE '%fmtc%' OR LOWER(note_text) LIKE '%men2%')"""
N3_FILTER = """(LOWER(note_text) LIKE '%vascular invasion%' OR LOWER(note_text) LIKE '%lvi%' OR LOWER(note_text) LIKE '%lymphovascular%' OR LOWER(note_text) LIKE '%lymph-vascular%')"""

results = []
results.append(run_full("SMOKING", N1_FILTER,
    ['never_smoker','former_smoker','current_smoker','unknown_or_not_mentioned'], "smoking_status"))
results.append(run_full("FAMILY_HX_THYROID", N2_FILTER,
    ['family_hx_thyroid_cancer_present','family_hx_thyroid_cancer_absent','family_hx_unknown_or_not_mentioned'], "family_hx_status"))
results.append(run_full("VASC_INVASION", N3_FILTER,
    ['vascular_invasion_present','vascular_invasion_absent','vascular_invasion_focal','vascular_invasion_extensive','vascular_invasion_unknown_or_not_mentioned'], "vasc_invasion_status"))

print("\n=== SUMMARY ===")
for r in results:
    print(f"  {r['slice']:25s} {r['total']:>6,} notes / {r['actionable']:>5,} actionable / {r['n_pts_actionable']:>5,} pts / {r['elapsed']:.1f}s")
print("=== READY FOR mig_281 PROMOTION ===")
