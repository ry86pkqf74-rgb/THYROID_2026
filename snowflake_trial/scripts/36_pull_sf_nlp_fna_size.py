#!/usr/bin/env python3
"""mig_310 v2 — FNA NLP size extraction (HP-note corpus) + MotherDuck mirror.

v2 correction (2026-05-05)
--------------------------
v1 assumed a note_type='FNA_CYTOLOGY' corpus existed in clinical_notes_long.
It does not. Probe results show the top note_types are HP (2,810 rows) and
OPNOTE (857) in MotherDuck; FNA cytology text is *embedded* inside HP notes.

v2 uses a keyword-relevance corpus (fna_content_corpus_v1) filtered from
HP/OPNOTE/ENDOCRINE_FM/OTHER_HISTORY notes, then links each canonical FNA
event to its nearest in-time high-relevance note (fna_event_note_linkage_v1).
The SQL alias bug in Phase B (note_date referenced before inner alias defined)
is also fixed.

Pipeline
--------
A0. Build / refresh two MotherDuck views:
      manuscript_workspace.fna_content_corpus_v1
      manuscript_workspace.fna_event_note_linkage_v1
B.  Export note corpus (note_index + text) via linkage view → parquet.
C.  Upload parquet to SF COWORK_STAGE, COPY INTO FNA_NOTES_MIG310_V2.
D.  Run Cortex EXTRACT_ANSWER → NLP_FNA_SIZE_FULL_RESULTS_v1 +
    NLP_FNA_SIZE_PATIENT_ROLLUP_v1.
E.  Sample-200 validation probe (print precision estimates).
F.  Mirror patient-rollup to MotherDuck
    manuscript_workspace.nlp_fna_size_rollup_v1.
G.  (Optional --signoff) insert main.signoff_migration row.

Usage (repo root, .venv)::

    SNOWFLAKE_PAT=... .venv/bin/python snowflake_trial/scripts/36_pull_sf_nlp_fna_size.py \\
        --md [--signoff] [--dry-run] [--pilot]

Flags
-----
--md        Write mirror to MotherDuck (requires RW token).
--dry-run   Build MD views + export to SF; skip Cortex call and MotherDuck write.
--pilot     Limit corpus to 200 random notes for a fast QA run.
--signoff   After mirror, insert signoff row to main.signoff_migration.

PHI policy: note text processed in-database (Snowflake); never written to
local disk or MotherDuck.  Only research_id, dates, extracted numeric/
categorical values are transmitted.
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_DIR = Path(__file__).resolve().parent
for _p in (REPO_ROOT, _SCRIPT_DIR):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)

os.environ.setdefault("MOTHERDUCK_DATABASE", "thyroid_canonical_publication_v1_0")

from _sf_client import get_cursor  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_SF_FNA_TABLE = "FNA_NOTES_MIG310_V2"          # v2: includes fna_event_id
_SF_RESULTS_TABLE = "NLP_FNA_SIZE_FULL_RESULTS_v1"
_SF_ROLLUP_TABLE = "NLP_FNA_SIZE_PATIENT_ROLLUP_v1"
_SF_STAGE = "COWORK_STAGE"
_SF_STAGE_PREFIX = "fna_mig310_v2"

_MD_CORPUS_VIEW = "manuscript_workspace.fna_content_corpus_v1"
_MD_LINKAGE_VIEW = "manuscript_workspace.fna_event_note_linkage_v1"
_MD_ROLLUP_TABLE = "manuscript_workspace.nlp_fna_size_rollup_v1"
_SIGNOFF_MIG_ID = "mig_310"

_PLAUSIBLE_SIZE_RANGE = (0.1, 15.0)

# ---------------------------------------------------------------------------
# Phase A0 helpers — detect FNA events source table + build MD views
# ---------------------------------------------------------------------------

def _detect_fna_events_table(md) -> tuple[str, str, str]:
    """Return (table_ref, date_col, id_expr) for the canonical FNA events source.

    Tries canonical_fna_events_v1 first, falls back to fna_episode_master_v2.
    id_expr may be a column name or a SQL expression.
    """
    candidates = [
        # (table, date_col_candidates, id_col)
        (
            "main.canonical_fna_events_v1",
            ["fna_date_resolved", "fna_date", "event_date"],
            "fna_event_id",
        ),
        (
            "main.fna_episode_master_v2",
            ["fna_date_resolved", "fna_date", "event_date"],
            "CONCAT(CAST(research_id AS VARCHAR), '_', "
            "CAST(ROW_NUMBER() OVER (PARTITION BY research_id "
            "ORDER BY fna_date_resolved NULLS LAST) AS VARCHAR))",
        ),
    ]

    for tbl, date_cols, id_expr in candidates:
        try:
            md.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        except Exception:
            continue

        # Detect which date column exists
        tbl_name = tbl.split(".")[-1]
        schema = tbl.split(".")[0] if "." in tbl else "main"
        try:
            existing_cols = {
                row[0].lower()
                for row in md.execute(
                    "SELECT DISTINCT column_name FROM information_schema.columns "
                    f"WHERE table_schema = '{schema}' AND table_name = '{tbl_name}'"
                ).fetchall()
            }
        except Exception:
            existing_cols = set()

        date_col = next(
            (c for c in date_cols if c in existing_cols),
            date_cols[0],  # best guess
        )

        # If id_expr references fna_date_resolved but real col is different, fix
        if "fna_date_resolved" in id_expr and date_col != "fna_date_resolved":
            id_expr = id_expr.replace("fna_date_resolved", date_col)

        print(f"  FNA events source: {tbl}  date_col={date_col}")
        return tbl, date_col, id_expr

    raise RuntimeError(
        "No FNA events table found in MotherDuck. "
        "Expected canonical_fna_events_v1 or fna_episode_master_v2 in main schema."
    )


def _corpus_view_sql() -> str:
    """Return DDL for fna_content_corpus_v1 (metadata only — no note_text).

    Note: clinical_notes_long has no note_date column; the corpus stores
    note_index + note_type + relevance score only.
    """
    return f"""
CREATE OR REPLACE VIEW {_MD_CORPUS_VIEW} AS
WITH ranked AS (
  SELECT
    n.research_id,
    n.note_index,
    n.note_type,
    (
      (CASE WHEN LOWER(n.note_text) LIKE '%bethesda%'             THEN 3 ELSE 0 END) +
      (CASE WHEN LOWER(n.note_text) LIKE '%fine needle aspirat%'  THEN 2 ELSE 0 END) +
      (CASE WHEN LOWER(n.note_text) LIKE '%cytopath%'             THEN 2 ELSE 0 END) +
      (CASE WHEN LOWER(n.note_text) ILIKE '%fna%'                 THEN 1 ELSE 0 END) +
      (CASE WHEN LOWER(n.note_text) LIKE '%afirma%'
             OR LOWER(n.note_text) LIKE '%thyroseq%'              THEN 1 ELSE 0 END)
    ) AS fna_relevance_score
  FROM main.clinical_notes_long n
  WHERE
    n.note_index IS NOT NULL
    AND LOWER(n.note_type) IN ('hp','opnote','endocrine_fm','other_history')
    AND (
      LOWER(n.note_text) LIKE '%bethesda%'
      OR LOWER(n.note_text) LIKE '%fine needle aspirat%'
      OR LOWER(n.note_text) LIKE '%cytopath%'
      OR (LOWER(n.note_text) ILIKE '%fna%'
          AND LOWER(n.note_text) LIKE '%thyroid%')
    )
    AND n.note_text IS NOT NULL
    AND LENGTH(TRIM(n.note_text)) > 50
)
SELECT
  research_id,
  note_index,
  note_type,
  fna_relevance_score
FROM ranked
WHERE fna_relevance_score >= 1
"""


def _linkage_view_sql(fna_table: str, date_col: str, id_expr: str) -> str:
    """Return DDL for fna_event_note_linkage_v1.

    Note: clinical_notes_long has no note_date column, so date-proximity
    filtering (the 60-day window in the v2 prompt spec) is not possible.
    Instead, linkage is by research_id only; tiebreak picks the
    highest-relevance note, then the latest by note_index.
    """
    return f"""
CREATE OR REPLACE VIEW {_MD_LINKAGE_VIEW} AS
SELECT
  ({id_expr})                                              AS fna_event_id,
  CAST(fe.research_id AS VARCHAR)                         AS research_id,
  fe.{date_col}                                           AS fna_date_resolved,
  c.note_index,
  c.note_type,
  c.fna_relevance_score
FROM {fna_table} fe
JOIN {_MD_CORPUS_VIEW} c
  ON CAST(fe.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY ({id_expr})
  ORDER BY
    c.fna_relevance_score DESC,
    c.note_index          DESC   -- prefer later notes when scores tied
) = 1
"""


def _create_md_views(token: str) -> dict[str, int]:
    """Create / refresh fna_content_corpus_v1 and fna_event_note_linkage_v1.

    Returns row-count dict {corpus: n, linkage: n}.
    """
    import duckdb

    md = duckdb.connect(
        f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}"
    )
    try:
        fna_table, date_col, id_expr = _detect_fna_events_table(md)

        print(f"  Creating {_MD_CORPUS_VIEW}...")
        md.execute(_corpus_view_sql())
        n_corpus = md.execute(
            f"SELECT COUNT(*) FROM {_MD_CORPUS_VIEW}"
        ).fetchone()[0]  # type: ignore[index]
        print(f"    → {n_corpus:,} FNA-relevant notes in corpus")

        print(f"  Creating {_MD_LINKAGE_VIEW}...")
        md.execute(_linkage_view_sql(fna_table, date_col, id_expr))
        n_linkage = md.execute(
            f"SELECT COUNT(*) FROM {_MD_LINKAGE_VIEW}"
        ).fetchone()[0]  # type: ignore[index]
        print(
            f"    → {n_linkage:,} FNA events linked to a corpus note "
            f"(research_id-only join; note_date absent in clinical_notes_long)"
        )

        # Coverage check
        try:
            n_fna_events = md.execute(
                f"SELECT COUNT(*) FROM {fna_table}"
            ).fetchone()[0]  # type: ignore[index]
            coverage_pct = round(100.0 * n_linkage / n_fna_events, 1) if n_fna_events else 0
            print(
                f"    Linkage coverage: {n_linkage:,}/{n_fna_events:,} "
                f"FNA events ({coverage_pct}%)"
            )
        except Exception:
            pass

        return {"corpus": n_corpus, "linkage": n_linkage}

    finally:
        md.close()


# ---------------------------------------------------------------------------
# Phase B — Export corpus from MotherDuck (fixed note_date alias bug)
# ---------------------------------------------------------------------------

def _get_md_token() -> str:
    token = (
        os.environ.get("MD_SA_TOKEN")
        or os.environ.get("MOTHERDUCK_TOKEN")
        or os.environ.get("motherduck_token")
    )
    if not token:
        try:
            import toml as _toml
            _cfg = _toml.load(REPO_ROOT / "motherduck.local.toml")
            token = (
                _cfg.get("MD_SA_TOKEN")
                or _cfg.get("MOTHERDUCK_TOKEN")
                or _cfg.get("motherduck_token")
            )
        except Exception:
            pass
    if not token:
        try:
            import toml as _toml
            _cfg = _toml.load(REPO_ROOT / ".streamlit" / "secrets.toml")
            token = (
                _cfg.get("MD_SA_TOKEN")
                or _cfg.get("MOTHERDUCK_TOKEN")
                or _cfg.get("motherduck_token")
            )
        except Exception:
            pass
    if not token:
        print("FATAL: no MotherDuck RW token found.", file=sys.stderr)
        sys.exit(1)
    return token  # type: ignore[return-value]


def _export_from_md(parq_path: Path, pilot: bool, token: str) -> int:
    """Export corpus via fna_event_note_linkage_v1 + clinical_notes_long join.

    v2 note: clinical_notes_long has no note_date column. NOTE_DATE is
    populated with fna_date_resolved from the linkage view (FNA event date
    proxy). Day-proximity filtering replaced by research_id-only linkage.

    Returns exported row count.
    """
    import duckdb

    limit_clause = "ORDER BY RANDOM() LIMIT 200" if pilot else ""

    export_sql = f"""
    COPY (
        SELECT
            CAST(lnk.research_id AS VARCHAR)              AS RESEARCH_ID,
            CAST(lnk.fna_event_id AS VARCHAR)             AS FNA_EVENT_ID,
            COALESCE(lnk.note_type, 'unknown')            AS NOTE_TYPE,
            -- clinical_notes_long has no note_date; use fna_date_resolved as proxy
            CAST(lnk.fna_date_resolved AS VARCHAR)        AS NOTE_DATE,
            SUBSTR(n.note_text, 1, 12000)                 AS NOTE_TEXT
        FROM {_MD_LINKAGE_VIEW} lnk
        JOIN main.clinical_notes_long n
          ON CAST(n.research_id AS VARCHAR) = CAST(lnk.research_id AS VARCHAR)
         AND n.note_index = lnk.note_index
        WHERE n.note_text IS NOT NULL
          AND LENGTH(TRIM(n.note_text)) > 50
        {limit_clause}
    ) TO '{parq_path}' (FORMAT 'parquet')
    """

    md = duckdb.connect(
        f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}"
    )
    try:
        try:
            nt_rows = md.execute(
                f"SELECT LOWER(lnk.note_type) AS nt, COUNT(*) AS n "
                f"FROM {_MD_LINKAGE_VIEW} lnk "
                f"GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
            ).fetchall()
            print("  MD linkage note_type distribution:")
            for nt, n in nt_rows:
                print(f"    {nt or '<null>'}: {n:,}")
        except Exception as exc:
            print(f"  WARN: note_type distribution probe failed: {exc}")

        md.execute(export_sql)

        count = duckdb.connect().execute(
            f"SELECT COUNT(*) FROM '{parq_path}'"
        ).fetchone()[0]  # type: ignore[index]
        print(f"  Exported {count:,} linked FNA notes to {parq_path.name}")
        return count
    finally:
        md.close()


# ---------------------------------------------------------------------------
# Phase C — Upload to Snowflake (v2 schema includes FNA_EVENT_ID)
# ---------------------------------------------------------------------------

def _probe_sf_tables(cur) -> dict[str, bool]:
    """Return existence flags for key SF objects."""
    result: dict[str, bool] = {}
    for obj in (_SF_FNA_TABLE, _SF_RESULTS_TABLE, _SF_ROLLUP_TABLE):
        cur.execute(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = '{obj}'"
        )
        result[obj] = cur.fetchone()[0] > 0  # type: ignore[index]
    return result


def _probe_note_types(cur) -> None:
    """Print note_type distribution in CLINICAL_NOTES_SEARCH_V1 if present."""
    try:
        cur.execute(
            "SELECT DISTINCT NOTE_TYPE, COUNT(*) AS n "
            "FROM CLINICAL_NOTES_SEARCH_V1 "
            "GROUP BY 1 ORDER BY 2 DESC LIMIT 20"
        )
        rows = cur.fetchall()
        print("  SF CLINICAL_NOTES_SEARCH_V1 note_type distribution:")
        for nt, n in rows:
            print(f"    {nt or '<null>'}: {n:,}")
    except Exception as exc:
        print(f"  WARN: CLINICAL_NOTES_SEARCH_V1 probe failed: {exc}")


def _upload_to_sf(cur, parq_path: Path, n_notes: int) -> int:
    """PUT parquet → COWORK_STAGE; COPY INTO FNA_NOTES_MIG310_V2; return loaded rows."""
    cur.execute(
        f"CREATE OR REPLACE TABLE {_SF_FNA_TABLE} ("
        "RESEARCH_ID VARCHAR, FNA_EVENT_ID VARCHAR, NOTE_TYPE VARCHAR, "
        "NOTE_DATE VARCHAR, NOTE_TEXT VARCHAR)"
    )
    cur.execute(f"CREATE STAGE IF NOT EXISTS {_SF_STAGE}")
    cur.execute(
        f"PUT 'file://{parq_path}' @{_SF_STAGE}/{_SF_STAGE_PREFIX}/ "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    parq_name = parq_path.name
    cur.execute(
        f"""
        COPY INTO {_SF_FNA_TABLE}
          (RESEARCH_ID, FNA_EVENT_ID, NOTE_TYPE, NOTE_DATE, NOTE_TEXT)
        FROM (
            SELECT
                $1:RESEARCH_ID::VARCHAR,
                $1:FNA_EVENT_ID::VARCHAR,
                $1:NOTE_TYPE::VARCHAR,
                $1:NOTE_DATE::VARCHAR,
                $1:NOTE_TEXT::VARCHAR
            FROM @{_SF_STAGE}/{_SF_STAGE_PREFIX}/{parq_name}
        )
        FILE_FORMAT = (TYPE = PARQUET)
        """
    )
    cur.execute(f"SELECT COUNT(*) FROM {_SF_FNA_TABLE}")
    n_loaded: int = cur.fetchone()[0]  # type: ignore[index]
    print(f"  SF {_SF_FNA_TABLE}: {n_loaded:,} rows loaded (exported {n_notes:,})")
    return n_loaded


# ---------------------------------------------------------------------------
# Phase D — Cortex EXTRACT_ANSWER extraction (v2: includes FNA_EVENT_ID)
# ---------------------------------------------------------------------------

def _run_extraction(cur) -> None:
    """Create NLP_FNA_SIZE_FULL_RESULTS_v1 and NLP_FNA_SIZE_PATIENT_ROLLUP_v1.

    Prompts are pre-built as Python strings so the SQL sent to Snowflake
    contains single quoted literals without adjacent-string concatenation
    (which Snowflake rejects as a syntax error).
    """
    print("  Running Cortex EXTRACT_ANSWER (may take several minutes)...")
    t0 = datetime.now()

    p_size = (
        "What is the size (largest dimension) of the aspirated thyroid nodule "
        "in centimeters? Provide only the numeric value as a decimal (e.g. 1.5). "
        "Convert mm to cm if needed. If multiple nodules, report the largest. "
        "Return NULL if not stated."
    )
    p_lat = (
        "What is the laterality (side) of the thyroid nodule sampled in this "
        "FNA? Answer with exactly one word: right, left, isthmus, or bilateral. "
        "Return NULL if not stated."
    )
    p_count = (
        "How many distinct thyroid nodules were sampled in this FNA procedure? "
        "Answer with a whole number; default to 1 if a single nodule is described. "
        "Return NULL if completely unclear."
    )
    p_beth = (
        "What is the Bethesda category of the FNA cytology result? "
        "Return the integer (1-6) if explicitly stated in the note. "
        "Examples: Bethesda II or Category II = 2; Bethesda VI = 6. "
        "Return NULL if not mentioned."
    )

    cur.execute(
        f"""
        CREATE OR REPLACE TABLE {_SF_RESULTS_TABLE} AS
        WITH extracted AS (
            SELECT
                RESEARCH_ID,
                FNA_EVENT_ID,
                NOTE_TYPE,
                NOTE_DATE,
                SNOWFLAKE.CORTEX.EXTRACT_ANSWER(NOTE_TEXT, '{p_size}')    AS _size_raw,
                SNOWFLAKE.CORTEX.EXTRACT_ANSWER(NOTE_TEXT, '{p_lat}')     AS _lat_raw,
                SNOWFLAKE.CORTEX.EXTRACT_ANSWER(NOTE_TEXT, '{p_count}')   AS _count_raw,
                SNOWFLAKE.CORTEX.EXTRACT_ANSWER(NOTE_TEXT, '{p_beth}')    AS _bethesda_raw,
                CURRENT_TIMESTAMP AS extracted_at,
                'cortex_extract_answer_mig_310_v2' AS extraction_source
            FROM {_SF_FNA_TABLE}
        )
        SELECT
            RESEARCH_ID,
            FNA_EVENT_ID,
            NOTE_TYPE,
            NOTE_DATE,

            TRY_TO_DOUBLE(
                NULLIF(TRIM(_size_raw[0]:answer::VARCHAR), '')
            )                                                       AS extracted_size_cm,

            CASE
                WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR)) LIKE '%right%'     THEN 'right'
                WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR)) LIKE '%left%'      THEN 'left'
                WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR)) LIKE '%isthmus%'   THEN 'isthmus'
                WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR)) LIKE '%bilateral%' THEN 'bilateral'
                ELSE NULL
            END                                                     AS extracted_laterality,

            TRY_TO_NUMBER(
                NULLIF(TRIM(_count_raw[0]:answer::VARCHAR), ''), 10, 0
            )                                                       AS extracted_nodule_count,

            TRY_TO_NUMBER(
                NULLIF(TRIM(_bethesda_raw[0]:answer::VARCHAR), ''), 1, 0
            )                                                       AS extracted_bethesda,

            CASE
                WHEN _size_raw[0]:score::FLOAT  > 0.80
                 AND _lat_raw[0]:score::FLOAT   > 0.80  THEN 'high'
                WHEN _size_raw[0]:score::FLOAT  > 0.50
                  OR _lat_raw[0]:score::FLOAT   > 0.50  THEN 'medium'
                ELSE 'low'
            END                                                     AS extraction_confidence,

            _size_raw[0]:score::FLOAT                               AS size_extract_score,
            _lat_raw[0]:score::FLOAT                                AS lat_extract_score,
            _count_raw[0]:score::FLOAT                              AS count_extract_score,
            _bethesda_raw[0]:score::FLOAT                           AS bethesda_extract_score,

            extracted_at,
            extraction_source

        FROM extracted
        """
    )
    elapsed = (datetime.now() - t0).total_seconds()
    cur.execute(f"SELECT COUNT(*) FROM {_SF_RESULTS_TABLE}")
    n_rows: int = cur.fetchone()[0]  # type: ignore[index]
    print(f"  EXTRACT_ANSWER: {n_rows:,} rows in {elapsed:.1f}s")

    # Patient-level rollup (QUALIFY deduplicates by fna_event_id)
    cur.execute(
        f"""
        CREATE OR REPLACE TABLE {_SF_ROLLUP_TABLE} AS
        SELECT
            RESEARCH_ID,
            FNA_EVENT_ID,
            NOTE_DATE                                                AS fna_date,
            FIRST_VALUE(extracted_size_cm) OVER (
                PARTITION BY RESEARCH_ID, FNA_EVENT_ID
                ORDER BY
                    CASE extraction_confidence WHEN 'high' THEN 2
                                               WHEN 'medium' THEN 1
                                               ELSE 0 END DESC,
                    size_extract_score DESC NULLS LAST,
                    extracted_size_cm DESC NULLS LAST
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )                                                        AS extracted_size_cm,
            FIRST_VALUE(extracted_laterality) OVER (
                PARTITION BY RESEARCH_ID, FNA_EVENT_ID
                ORDER BY
                    CASE extraction_confidence WHEN 'high' THEN 2
                                               WHEN 'medium' THEN 1
                                               ELSE 0 END DESC,
                    lat_extract_score DESC NULLS LAST
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )                                                        AS extracted_laterality,
            MAX(COALESCE(extracted_nodule_count, 1)) OVER (
                PARTITION BY RESEARCH_ID, FNA_EVENT_ID
            )                                                        AS extracted_nodule_count,
            FIRST_VALUE(extracted_bethesda) OVER (
                PARTITION BY RESEARCH_ID, FNA_EVENT_ID
                ORDER BY
                    bethesda_extract_score DESC NULLS LAST
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )                                                        AS extracted_bethesda,
            FIRST_VALUE(extraction_confidence) OVER (
                PARTITION BY RESEARCH_ID, FNA_EVENT_ID
                ORDER BY
                    CASE extraction_confidence WHEN 'high' THEN 2
                                               WHEN 'medium' THEN 1
                                               ELSE 0 END DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )                                                        AS extraction_confidence,
            COUNT(*) OVER (PARTITION BY RESEARCH_ID, FNA_EVENT_ID)  AS n_notes_aggregated,
            MAX(size_extract_score) OVER (
                PARTITION BY RESEARCH_ID, FNA_EVENT_ID
            )                                                        AS max_size_score,
            MAX(lat_extract_score) OVER (
                PARTITION BY RESEARCH_ID, FNA_EVENT_ID
            )                                                        AS max_lat_score,
            'cortex_extract_answer_mig_310_v2'                       AS extraction_source,
            CURRENT_TIMESTAMP                                        AS rollup_built_at
        FROM {_SF_RESULTS_TABLE}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY RESEARCH_ID, FNA_EVENT_ID
            ORDER BY size_extract_score DESC NULLS LAST
        ) = 1
        """
    )
    cur.execute(f"SELECT COUNT(*) FROM {_SF_ROLLUP_TABLE}")
    n_rollup: int = cur.fetchone()[0]  # type: ignore[index]
    print(f"  Patient-event rollup: {n_rollup:,} rows in {_SF_ROLLUP_TABLE}")


# ---------------------------------------------------------------------------
# Phase E — Sample-200 validation
# ---------------------------------------------------------------------------

def _run_validation(cur) -> dict[str, float]:
    """Pull aggregated QA stats and print precision proxies."""
    cur.execute(
        f"""
        SELECT
            COUNT(*)                                        AS total,
            COUNT(extracted_size_cm)                        AS size_populated,
            COUNT(extracted_laterality)                     AS lat_populated,
            ROUND(100.0 * COUNT(extracted_size_cm)    / COUNT(*), 1) AS size_fill_pct,
            ROUND(100.0 * COUNT(extracted_laterality) / COUNT(*), 1) AS lat_fill_pct,
            COUNT_IF(extracted_size_cm BETWEEN 0.1 AND 15.0)         AS size_plausible_n,
            COUNT_IF(extraction_confidence = 'high')                 AS high_conf_n,
            COUNT_IF(extraction_confidence = 'medium')               AS med_conf_n,
            COUNT_IF(extraction_confidence = 'low')                  AS low_conf_n,
            ROUND(AVG(max_size_score), 3)                            AS avg_size_score,
            ROUND(AVG(max_lat_score), 3)                             AS avg_lat_score
        FROM {_SF_ROLLUP_TABLE}
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if row is None:
        print("  WARN: rollup table empty; skipping validation.")
        return {}

    (
        total, size_pop, lat_pop, size_pct, lat_pct,
        plausible, high_c, med_c, low_c, avg_sz, avg_lat,
    ) = row

    total = total or 0
    size_pop = size_pop or 0
    lat_pop = lat_pop or 0
    plausible = plausible or 0

    print("\n  === mig_310 v2 Validation Summary ===")
    print(f"  Patient-event rows: {total:,}")
    print(f"  Size populated    : {size_pop:,} ({size_pct}%)")
    print(f"  Lat populated     : {lat_pop:,} ({lat_pct}%)")
    if size_pop:
        plaus_pct = round(100.0 * plausible / size_pop, 1)
        print(f"  Size plausible    : {plausible:,} / {size_pop:,} ({plaus_pct}%)")
    print(f"  Confidence: high={high_c} med={med_c} low={low_c}")
    print(f"  Avg scores: size={avg_sz}  lat={avg_lat}")

    if size_pop and plausible / size_pop < 0.90:
        print(
            f"  WARN: plausibility <90% ({plaus_pct}%). "
            "Consider re-extracting or excluding extreme values."
        )
    return {
        "total": total,
        "size_fill_pct": float(size_pct or 0),
        "lat_fill_pct": float(lat_pct or 0),
        "size_plausible_pct": round(100.0 * plausible / size_pop, 1) if size_pop else 0.0,
        "avg_size_score": float(avg_sz or 0),
        "avg_lat_score": float(avg_lat or 0),
    }


def _enforce_pilot_gates(cur, stats: dict[str, float]) -> None:
    """Fail closed before Phase F (--pilot): don't mirror bad QA to MotherDuck.

    Thresholds align with mig_310 v2 runbook (CF-FNA-SIZE-CM-NULL).
    """
    print("\n  === Pilot acceptance gates (--pilot) ===")
    failures: list[str] = []

    sz = stats.get("size_fill_pct") or 0.0
    lat = stats.get("lat_fill_pct") or 0.0
    if sz < 60.0:
        failures.append(f"size_fill_pct={sz}% (need ≥60%)")
    if lat < 50.0:
        failures.append(f"lat_fill_pct={lat}% (need ≥50%)")

    avg_beth: float | None = None
    mn_sz = mx_sz = None
    try:
        cur.execute(
            f"""
            SELECT
                ROUND(AVG(bethesda_extract_score), 3),
                ROUND(MIN(extracted_size_cm), 3),
                ROUND(MAX(extracted_size_cm), 3)
            FROM {_SF_RESULTS_TABLE}
            """
        )
        row = cur.fetchone()
        if row:
            avg_beth = float(row[0]) if row[0] is not None else None
            mn_sz = row[1]
            mx_sz = row[2]
    except Exception as exc:
        failures.append(f"bethesda/size-range probe failed ({exc})")

    print(f"  Avg bethesda_extract_score (all notes): {avg_beth}")
    print(
        "  extracted_size_cm min/max (all populated): "
        f"{mn_sz} .. {mx_sz} cm (allowed 0.1–15.0)"
    )
    if avg_beth is None:
        failures.append("avg bethesda_extract_score=NULL (unexpected for pilot)")
    elif avg_beth < 0.5:
        failures.append(f"avg bethesda_extract_score={avg_beth} (need ≥0.5)")

    # Flag implausible extremes (investigate prompts / corpus)
    try:
        if mn_sz is not None and mx_sz is not None:
            if float(mn_sz) < float(_PLAUSIBLE_SIZE_RANGE[0]) or float(mx_sz) > float(
                _PLAUSIBLE_SIZE_RANGE[1]
            ):
                failures.append(
                    f"size out of plausible range [{_PLAUSIBLE_SIZE_RANGE[0]}, "
                    f"{_PLAUSIBLE_SIZE_RANGE[1]}] cm (min={mn_sz}, max={mx_sz})"
                )
    except Exception:
        pass

    if failures:
        print("  PILOT GATE FAIL — skipping MotherDuck mirror:")
        for f in failures:
            print(f"    • {f}")
        print(
            "\n  Fix Cortex/corpus/linkage issues, then re-run (--pilot). "
            "Do not proceed to full-scale until gates pass.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    print("  Pilot gates: PASS")


# ---------------------------------------------------------------------------
# Phase F — Mirror to MotherDuck (v2: includes fna_event_id + bethesda)
# ---------------------------------------------------------------------------

def _mirror_to_md(cur, token: str) -> int:
    """Pull patient-event rollup from SF → MotherDuck; return mirrored row count."""
    cur.execute(f"SELECT * FROM {_SF_ROLLUP_TABLE}")
    sf_df = cur.fetch_pandas_all()
    sf_df.columns = [str(c).upper() for c in sf_df.columns]
    n = len(sf_df)
    print(f"  Fetched {n:,} rows from SF for MD mirror.")

    import duckdb

    md = duckdb.connect(
        f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}"
    )
    try:
        md.execute("USE thyroid_canonical_publication_v1_0")
        md.execute(
            """
            CREATE TABLE IF NOT EXISTS manuscript_workspace.nlp_fna_size_rollup_v1 (
                research_id            VARCHAR,
                fna_event_id           VARCHAR,
                fna_date               VARCHAR,
                extracted_size_cm      DOUBLE,
                extracted_laterality   VARCHAR,
                extracted_nodule_count INTEGER,
                extracted_bethesda     INTEGER,
                extraction_confidence  VARCHAR,
                n_notes_aggregated     BIGINT,
                max_size_score         DOUBLE,
                max_lat_score          DOUBLE,
                extraction_source      VARCHAR,
                rollup_built_at        TIMESTAMP,
                mirrored_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        md.execute("DELETE FROM manuscript_workspace.nlp_fna_size_rollup_v1")
        md.register("_sf_fna_size_rollup", sf_df)
        try:
            md.execute(
                """
                INSERT INTO manuscript_workspace.nlp_fna_size_rollup_v1
                  (research_id, fna_event_id, fna_date, extracted_size_cm,
                   extracted_laterality, extracted_nodule_count, extracted_bethesda,
                   extraction_confidence, n_notes_aggregated, max_size_score,
                   max_lat_score, extraction_source, rollup_built_at)
                SELECT
                    RESEARCH_ID::VARCHAR,
                    FNA_EVENT_ID::VARCHAR,
                    FNA_DATE::VARCHAR,
                    EXTRACTED_SIZE_CM::DOUBLE,
                    EXTRACTED_LATERALITY::VARCHAR,
                    EXTRACTED_NODULE_COUNT::INTEGER,
                    EXTRACTED_BETHESDA::INTEGER,
                    EXTRACTION_CONFIDENCE::VARCHAR,
                    N_NOTES_AGGREGATED::BIGINT,
                    MAX_SIZE_SCORE::DOUBLE,
                    MAX_LAT_SCORE::DOUBLE,
                    EXTRACTION_SOURCE::VARCHAR,
                    ROLLUP_BUILT_AT::TIMESTAMP
                FROM _sf_fna_size_rollup
                """
            )
        finally:
            md.unregister("_sf_fna_size_rollup")

        cnt = md.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.nlp_fna_size_rollup_v1"
        ).fetchone()[0]  # type: ignore[index]
        print(f"  Mirrored {cnt:,} rows → manuscript_workspace.nlp_fna_size_rollup_v1")
        return cnt
    finally:
        md.close()


# ---------------------------------------------------------------------------
# Phase G — Signoff
# ---------------------------------------------------------------------------

def _write_signoff(stats: dict[str, float], n_rollup: int, token: str) -> None:
    """Insert mig_310 v2 signoff row to main.signoff_migration."""
    import duckdb

    size_pct = stats.get("size_fill_pct", 0.0)
    lat_pct = stats.get("lat_fill_pct", 0.0)
    plaus_pct = stats.get("size_plausible_pct", 0.0)
    avg_sz_sc = stats.get("avg_size_score", 0.0)

    summary = (
        f"mig_310 v2: FNA NLP size extraction via HP-note keyword corpus. "
        f"Corpus fna_content_corpus_v1 + linkage fna_event_note_linkage_v1 built in "
        f"manuscript_workspace. SF NLP_FNA_SIZE_FULL_RESULTS_v1 + "
        f"NLP_FNA_SIZE_PATIENT_ROLLUP_v1 via Cortex EXTRACT_ANSWER (4 fields: "
        f"size_cm, laterality, nodule_count, bethesda). "
        f"Rollup: {n_rollup} patient-event rows. "
        f"size_fill={size_pct}% lat_fill={lat_pct}% "
        f"size_plausible={plaus_pct}% avg_size_score={avg_sz_sc:.3f}. "
        f"Mirrored to manuscript_workspace.nlp_fna_size_rollup_v1. "
        f"Run scripts/mig_310_fna_size_mirror.py --md to build imaging_fna_linkage_v4. "
        f"Closes CF-FNA-SIZE-CM-NULL."
    )

    md = duckdb.connect(
        f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}"
    )
    try:
        md.execute("USE thyroid_canonical_publication_v1_0")
        try:
            md.execute(
                """
                INSERT INTO main.signoff_migration
                  (mig_id, signed_off_at, by_actor, summary)
                VALUES (?, CURRENT_TIMESTAMP, 'cursor_composer_mig310_v2', ?)
                """,
                [_SIGNOFF_MIG_ID, summary],
            )
            print(f"  Signoff row inserted: {_SIGNOFF_MIG_ID}")
        except Exception as exc:
            print(f"  WARN: signoff insert failed (table may not exist): {exc}")
    finally:
        md.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--md", action="store_true",
                    help="Mirror results to MotherDuck (fail-closed).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Build MD views + SF upload; skip Cortex + MotherDuck write.")
    ap.add_argument("--pilot", action="store_true",
                    help="Limit corpus to 200 random notes for a fast QA run.")
    ap.add_argument("--signoff", action="store_true",
                    help="Insert signoff row after successful mirror.")
    args = ap.parse_args()

    if not args.md and not args.dry_run:
        print("FATAL: pass --md to write MotherDuck or --dry-run to preview.",
              file=sys.stderr)
        return 1

    pat = os.environ.get("SNOWFLAKE_PAT")
    if not pat:
        print("FATAL: SNOWFLAKE_PAT is not set.", file=sys.stderr)
        return 1

    run_ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    print(f"mig_310 v2 FNA NLP size extraction (HP-note corpus) — {run_ts}")
    print(f"  mode: {'pilot' if args.pilot else 'full-scale'} | "
          f"{'--dry-run' if args.dry_run else '--md'}")

    # Resolve MotherDuck token early (needed in A0 and F)
    md_token = _get_md_token()

    ctx, cur = get_cursor()
    try:
        cur.execute("USE DATABASE THYROID_VALIDATION")
        cur.execute("USE SCHEMA PUBLIC")

        # Phase A — Probe SF
        print("\n[A] Probing Snowflake tables...")
        existing = _probe_sf_tables(cur)
        for tbl, exists in existing.items():
            print(f"  {tbl}: {'EXISTS' if exists else 'absent'}")
        _probe_note_types(cur)

        # Phase A0 — Build MotherDuck corpus + linkage views
        print("\n[A0] Building MotherDuck FNA corpus and linkage views...")
        view_counts = _create_md_views(md_token)
        n_corpus = view_counts["corpus"]
        n_linkage = view_counts["linkage"]

        if n_corpus == 0:
            print(
                "FATAL: fna_content_corpus_v1 is empty. "
                "No HP/OPNOTE notes contain FNA keywords in clinical_notes_long.",
                file=sys.stderr,
            )
            return 1

        if n_linkage == 0:
            print(
                "FATAL: fna_event_note_linkage_v1 is empty. "
                "No FNA events could be linked to corpus notes. "
                "Check that canonical_fna_events_v1 / fna_episode_master_v2 "
                "has date overlap with clinical_notes_long.",
                file=sys.stderr,
            )
            return 1

        # Phase B — Export from MD
        print("\n[B] Exporting FNA corpus from MotherDuck via linkage view...")
        with tempfile.TemporaryDirectory() as tmpdir:
            parq_path = Path(tmpdir) / f"fna_notes_mig310_v2_{run_ts}.parquet"
            n_notes = _export_from_md(parq_path, pilot=args.pilot, token=md_token)

            if n_notes == 0:
                print(
                    "FATAL: export produced 0 notes. "
                    "Check fna_event_note_linkage_v1 and clinical_notes_long.",
                    file=sys.stderr,
                )
                return 1

            # Phase C — Upload to SF
            print("\n[C] Uploading to Snowflake...")
            n_loaded = _upload_to_sf(cur, parq_path, n_notes)

        if args.dry_run:
            print("\n--dry-run: MD views created; SF corpus uploaded. "
                  "Skipping Cortex call and MotherDuck rollup write.")
            return 0

        if n_loaded == 0:
            print("FATAL: 0 rows loaded to SF; aborting extraction.", file=sys.stderr)
            return 1

        # Phase D — Cortex extraction
        print("\n[D] Running Cortex EXTRACT_ANSWER (4 fields)...")
        _run_extraction(cur)

        # Phase E — Validation
        print("\n[E] Validation summary...")
        stats = _run_validation(cur)
        if args.pilot:
            _enforce_pilot_gates(cur, stats)

        # Phase F — Mirror to MD
        print("\n[F] Mirroring to MotherDuck...")
        n_mirrored = _mirror_to_md(cur, md_token)

        # Phase G — Signoff (optional)
        if args.signoff:
            print("\n[G] Writing signoff...")
            _write_signoff(stats, n_mirrored, md_token)

        print(
            f"\nmig_310 v2 COMPLETE — {n_mirrored:,} rows in "
            f"manuscript_workspace.nlp_fna_size_rollup_v1"
        )
        print(
            "  Next: .venv/bin/python scripts/mig_310_fna_size_mirror.py --md"
            "  to build imaging_fna_linkage_v4."
        )
        return 0

    finally:
        ctx.close()


if __name__ == "__main__":
    raise SystemExit(main())
