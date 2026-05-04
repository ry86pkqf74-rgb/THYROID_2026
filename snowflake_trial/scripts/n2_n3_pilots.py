"""N2 + N3 — SF AI_CLASSIFY pilots: family-hx-thyroid + vasc invasion residual.

Mirrors N1 smoking pilot pattern. Per feedback_nlp_refresh_on_snowflake.md.
PHI policy: notes processed in-database; no note text in reports/logs/CSVs.
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
assert MD_TOKEN, "MOTHERDUCK_TOKEN env var required"


def run_pilot(slice_name: str, sql_filter: str, sf_table: str, classes: list[str], decision_label: str) -> dict:
    """Run one pilot slice end-to-end and write report."""
    parq = PARQ_DIR / f"_nlp_{slice_name}_pilot_notes.parquet"
    report = REPORT_DIR / f"{slice_name}_pilot.md"

    print(f"\n=== {slice_name.upper()} pilot ===")

    # 1. Export 100 notes from MD
    md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={MD_TOKEN}")
    md.execute(f"""
COPY (
  SELECT research_id, note_type, note_index,
         SUBSTR(note_text, 1, 8000) AS note_text
  FROM main.clinical_notes_long
  WHERE {sql_filter}
  ORDER BY RANDOM() LIMIT 100
) TO '{parq}' (FORMAT 'parquet')
""")
    md.close()
    print(f"  exported -> {parq}")

    # 2. PUT + COPY INTO SF
    ctx, cur = get_cursor()
    cur.execute("USE DATABASE THYROID_VALIDATION")
    cur.execute("USE SCHEMA PUBLIC")
    notes_table = f"NLP_{slice_name.upper()}_PILOT_NOTES"
    results_table = f"NLP_{slice_name.upper()}_PILOT_RESULTS_v1"

    cur.execute(f"""
CREATE OR REPLACE TABLE {notes_table} (
  RESEARCH_ID VARCHAR, NOTE_TYPE VARCHAR, NOTE_INDEX INTEGER, NOTE_TEXT VARCHAR
)
""")
    cur.execute("CREATE STAGE IF NOT EXISTS COWORK_STAGE")
    cur.execute(f"PUT 'file://{parq}' @COWORK_STAGE/{slice_name}_pilot/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    cur.execute(f"""
COPY INTO {notes_table} (RESEARCH_ID, NOTE_TYPE, NOTE_INDEX, NOTE_TEXT)
FROM (
  SELECT $1:research_id::VARCHAR, $1:note_type::VARCHAR,
         $1:note_index::INTEGER, $1:note_text::VARCHAR
  FROM @COWORK_STAGE/{slice_name}_pilot/_nlp_{slice_name}_pilot_notes.parquet
) FILE_FORMAT = (TYPE = PARQUET)
""")

    # 3. AI_CLASSIFY
    classes_sql = "[" + ", ".join(f"'{c}'" for c in classes) + "]"
    t0 = datetime.now()
    cur.execute(f"""
CREATE OR REPLACE TABLE {results_table} AS
SELECT RESEARCH_ID, NOTE_TYPE, NOTE_INDEX,
       AI_CLASSIFY(NOTE_TEXT, {classes_sql}) AS classification_raw,
       classification_raw:labels[0]::VARCHAR AS {decision_label},
       CURRENT_TIMESTAMP AS classified_at,
       'AI_CLASSIFY_default_model' AS llm_model
FROM {notes_table}
""")
    elapsed = (datetime.now() - t0).total_seconds()
    print(f"  AI_CLASSIFY elapsed: {elapsed:.1f}s")

    # 4. Yield report
    cur.execute(f"SELECT {decision_label}, COUNT(*) FROM {results_table} GROUP BY 1 ORDER BY 2 DESC")
    rows = cur.fetchall()

    actionable_classes = [c for c in classes if 'unknown' not in c.lower() and 'not_mentioned' not in c.lower()]
    actionable_filter = "(" + " OR ".join(f"{decision_label} = '{c}'" for c in actionable_classes) + ")"
    cur.execute(f"""
SELECT COUNT(*),
       COUNT_IF({actionable_filter}),
       COUNT_IF({decision_label} IS NULL)
FROM {results_table}
""")
    total, actionable, null_class = cur.fetchone()

    lines = [
        f"# {slice_name} pilot — SF AI_CLASSIFY",
        f"**Generated:** {datetime.now().isoformat()}",
        f"**Cohort:** 100 random notes matching slice filter from `main.clinical_notes_long`",
        f"**AI_CLASSIFY elapsed:** {elapsed:.1f}s",
        "",
        "## Yield",
        "",
        f"| Total | Actionable | NULL |",
        f"|---:|---:|---:|",
        f"| {total} | {actionable} ({100*actionable/total:.1f}%) | {null_class} |",
        "",
        "## Distribution",
        "",
        f"| {decision_label} | n |",
        "|---|---:|",
        *[f"| {s} | {n} |" for s, n in rows],
        "",
        f"## Tables created in Snowflake",
        "",
        f"- `THYROID_VALIDATION.PUBLIC.{notes_table}` (PHI text — keep in SF)",
        f"- `THYROID_VALIDATION.PUBLIC.{results_table}` (research_id + classification only)",
        "",
        f"PHI policy: no note text in this report.",
    ]
    report.write_text("\n".join(lines))
    print(f"  report -> {report}")
    ctx.close()

    return {
        "slice": slice_name,
        "actionable_pct": 100 * actionable / total,
        "elapsed_s": elapsed,
        "rows": rows,
        "total": total,
        "actionable": actionable,
    }


# === N2 — Family-hx-thyroid ===
N2_FILTER = """(
    LOWER(note_text) LIKE '%family history%thyroid%'
 OR LOWER(note_text) LIKE '%mother%thyroid cancer%'
 OR LOWER(note_text) LIKE '%father%thyroid cancer%'
 OR LOWER(note_text) LIKE '%sister%thyroid cancer%'
 OR LOWER(note_text) LIKE '%brother%thyroid cancer%'
 OR LOWER(note_text) LIKE '%fmtc%'
 OR LOWER(note_text) LIKE '%men2%'
)"""

# === N3 — Vasc invasion residual ===
N3_FILTER = """(
    LOWER(note_text) LIKE '%vascular invasion%'
 OR LOWER(note_text) LIKE '%lvi%'
 OR LOWER(note_text) LIKE '%lymphovascular%'
 OR LOWER(note_text) LIKE '%lymph-vascular%'
)"""

results = []
results.append(run_pilot(
    "FAMILY_HX_THYROID",
    N2_FILTER,
    "NLP_FAMILY_HX_THYROID_PILOT_RESULTS_v1",
    [
        'family_hx_thyroid_cancer_present',
        'family_hx_thyroid_cancer_absent',
        'family_hx_unknown_or_not_mentioned',
    ],
    "family_hx_status",
))
results.append(run_pilot(
    "VASC_INVASION",
    N3_FILTER,
    "NLP_VASC_INVASION_PILOT_RESULTS_v1",
    [
        'vascular_invasion_present',
        'vascular_invasion_absent',
        'vascular_invasion_focal',
        'vascular_invasion_extensive',
        'vascular_invasion_unknown_or_not_mentioned',
    ],
    "vasc_invasion_status",
))

print("\n=== SUMMARY ===")
for r in results:
    print(f"  {r['slice']:30s}  actionable={r['actionable_pct']:.1f}%  elapsed={r['elapsed_s']:.1f}s")
print("=== DONE ===")
