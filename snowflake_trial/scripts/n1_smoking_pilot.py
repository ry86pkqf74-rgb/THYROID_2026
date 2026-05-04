"""N1 — SF AI_CLASSIFY smoking pilot (100 notes).

Replaces the deprecated H200 path for mig_272. Per
feedback_nlp_refresh_on_snowflake.md (Logan-ratified 2026-05-03).

Pipeline:
  1. Export 100 smoking-keyword-positive notes from MD (PMH-class slices) → Parquet
  2. PUT to SF stage + COPY INTO THYROID_VALIDATION.PUBLIC.NLP_SMOKING_PILOT_NOTES
  3. AI_CLASSIFY each note for smoking_status in {never, former, current, unknown}
  4. Write results to NLP_SMOKING_PILOT_RESULTS_v1
  5. Report yield rate (% notes that produced an actionable classification)

PHI policy: notes processed in-database only; never printed to chat/log/CSV.
Output to /Users/ros/THyroid 2026/snowflake_trial/reports/n1_smoking_pilot.md
contains research_ids + classification only — no note text.
"""
from __future__ import annotations
import sys, os, json
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent))

from _sf_client import get_cursor
import duckdb

REPORT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/n1_smoking_pilot.md")
PARQUET = Path("/Users/ros/THyroid 2026/snowflake_trial/parquet/_nlp_smoking_pilot_notes.parquet")
PARQUET.parent.mkdir(parents=True, exist_ok=True)

MD_TOKEN = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
assert MD_TOKEN, "MOTHERDUCK_TOKEN env var required"

# === Step 1: Export 100 smoking-keyword-positive notes from MD ===
print("=== Step 1: Export 100 PMH-slice smoking-keyword notes from MD ===")
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={MD_TOKEN}")
md.execute(f"""
COPY (
  SELECT
    research_id,
    note_type,
    note_index,
    SUBSTR(note_text, 1, 8000) AS note_text  -- cap to 8K chars (AI_CLASSIFY token-limit)
  FROM main.clinical_notes_long
  WHERE (
    LOWER(note_text) LIKE '%smoking%'
    OR LOWER(note_text) LIKE '%tobacco%'
    OR LOWER(note_text) LIKE '%pack-year%'
    OR LOWER(note_text) LIKE '%pack year%'
  )
  ORDER BY RANDOM()
  LIMIT 100
) TO '{PARQUET}' (FORMAT 'parquet')
""")
md.close()
n_rows = duckdb.connect().execute(f"SELECT COUNT(*) FROM '{PARQUET}'").fetchone()[0]
print(f"  Exported {n_rows} notes -> {PARQUET}")

# === Step 2: PUT + COPY INTO SF ===
print("=== Step 2: PUT + COPY INTO Snowflake ===")
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")

# Drop + recreate pilot table (keep VARCHAR for note_text since this is a one-off)
cur.execute("""
CREATE OR REPLACE TABLE NLP_SMOKING_PILOT_NOTES (
  RESEARCH_ID VARCHAR,
  NOTE_TYPE   VARCHAR,
  NOTE_INDEX  INTEGER,
  NOTE_TEXT   VARCHAR
)
""")

cur.execute("CREATE STAGE IF NOT EXISTS COWORK_STAGE")
cur.execute(f"PUT 'file://{PARQUET}' @COWORK_STAGE/n1_pilot/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
cur.execute(f"""
COPY INTO NLP_SMOKING_PILOT_NOTES (RESEARCH_ID, NOTE_TYPE, NOTE_INDEX, NOTE_TEXT)
FROM (
  SELECT $1:research_id::VARCHAR,
         $1:note_type::VARCHAR,
         $1:note_index::INTEGER,
         $1:note_text::VARCHAR
  FROM @COWORK_STAGE/n1_pilot/_nlp_smoking_pilot_notes.parquet
)
FILE_FORMAT = (TYPE = PARQUET)
""")
cur.execute("SELECT COUNT(*) FROM NLP_SMOKING_PILOT_NOTES")
n_loaded = cur.fetchone()[0]
print(f"  Loaded {n_loaded} notes to NLP_SMOKING_PILOT_NOTES")

# === Step 3: AI_CLASSIFY ===
print("=== Step 3: AI_CLASSIFY smoking_status ===")
t0 = datetime.utcnow()
cur.execute("""
CREATE OR REPLACE TABLE NLP_SMOKING_PILOT_RESULTS_v1 AS
SELECT
  RESEARCH_ID,
  NOTE_TYPE,
  NOTE_INDEX,
  AI_CLASSIFY(NOTE_TEXT,
    ['never_smoker', 'former_smoker', 'current_smoker', 'unknown_or_not_mentioned']
  ) AS classification_raw,
  classification_raw:labels[0]::VARCHAR AS smoking_status,
  CURRENT_TIMESTAMP AS classified_at,
  'AI_CLASSIFY_default_model' AS llm_model
FROM NLP_SMOKING_PILOT_NOTES
""")
elapsed = (datetime.utcnow() - t0).total_seconds()
print(f"  AI_CLASSIFY elapsed: {elapsed:.1f}s")

# === Step 4: Yield report ===
print("=== Step 4: Yield report ===")
cur.execute("""
SELECT smoking_status, COUNT(*) AS n
FROM NLP_SMOKING_PILOT_RESULTS_v1
GROUP BY smoking_status ORDER BY n DESC
""")
rows = cur.fetchall()

cur.execute("""
SELECT
  COUNT(*) AS total,
  COUNT_IF(smoking_status IN ('never_smoker','former_smoker','current_smoker')) AS actionable,
  COUNT_IF(smoking_status = 'unknown_or_not_mentioned') AS unknown,
  COUNT_IF(smoking_status IS NULL) AS null_class
FROM NLP_SMOKING_PILOT_RESULTS_v1
""")
total, actionable, unknown, null_class = cur.fetchone()

# === Step 5: Markdown report ===
report = [
    "# N1 — SF AI_CLASSIFY Smoking Pilot",
    f"**Generated:** {datetime.utcnow().isoformat()}Z",
    f"**Cohort:** 100 random smoking-keyword-positive notes from `main.clinical_notes_long`",
    f"**Pipeline:** MD export → SF PUT/COPY → AI_CLASSIFY → results table",
    f"**Elapsed (AI_CLASSIFY only):** {elapsed:.1f}s",
    "",
    "## Yield",
    "",
    f"| Total | Actionable (never/former/current) | Unknown | NULL |",
    f"|---:|---:|---:|---:|",
    f"| {total} | {actionable} ({100*actionable/total:.1f}%) | {unknown} ({100*unknown/total:.1f}%) | {null_class} |",
    "",
    "## Distribution",
    "",
    "| smoking_status | n |",
    "|---|---:|",
    *[f"| {s} | {n} |" for s, n in rows],
    "",
    "## Tables created in Snowflake",
    "",
    "- `THYROID_VALIDATION.PUBLIC.NLP_SMOKING_PILOT_NOTES` (100 rows; PHI text — keep in SF)",
    "- `THYROID_VALIDATION.PUBLIC.NLP_SMOKING_PILOT_RESULTS_v1` (research_id + classification only)",
    "",
    "## Decision gate",
    "",
    f"- **Actionable yield {100*actionable/total:.1f}%**: ",
    "  - >= 70% → green-light full smoking refresh on all 3,541 smoking-keyword notes",
    "  - 40-70% → tune prompt (more granular categories, packing pack-year buckets)",
    "  - <40% → re-scope; AI_CLASSIFY may not be the right primitive (try AI_COMPLETE structured-extraction)",
    "",
    "PHI policy: no note text written to this report or any committed file.",
]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text("\n".join(report))
print(f"  Report -> {REPORT}")

ctx.close()
print("=== DONE ===")
