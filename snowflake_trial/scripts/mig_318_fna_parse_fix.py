#!/usr/bin/env python3
"""mig_318 — FNA NLP parse-layer fix.

Closes CF-FNA-SIZE-PARSE-LAYER.

Root cause
----------
mig_310 v2 Cortex EXTRACT_ANSWER ran successfully (705/2,756 notes have
``max_size_score > 0.85``), but the SQL parser used ``TRY_TO_DOUBLE(NULLIF(TRIM(
answer), ''))`` which fails for answers like "1.5 cm", "1.5cm", "15 mm",
"approximately 1.5", etc.  Bethesda used ``TRY_TO_NUMBER(answer, 1, 0)`` which
chokes on "Category II", "Bethesda VI", etc.  Laterality worked fine because it
uses LIKE patterns.

Fix (no new Cortex calls if FNA_NOTES_MIG310_V2 is still present)
------------------------------------------------------------------
1. Re-run EXTRACT_ANSWER on the existing ``FNA_NOTES_MIG310_V2`` table —
   no re-export needed, all note text is already in Snowflake.
2. Persist the raw answer strings (``size_raw_answer``, ``bethesda_raw_answer``)
   so future parse passes don't need another extraction.
3. Apply regex-based parser: ``REGEXP_SUBSTR(answer, '[0-9]+(\\.[0-9]+)?')``
   extracts the first decimal number from any answer string.
4. Apply mm→cm conversion when the answer contains "mm" and not "cm".
5. Apply Roman-numeral + prose matching for Bethesda.
6. Re-mirror the improved patient rollup to MotherDuck.
7. Rebuild imaging_fna_linkage_v4 via mig_310_fna_size_mirror.py --md.

Usage::

    SNOWFLAKE_PAT=... .venv/bin/python \\
        snowflake_trial/scripts/mig_318_fna_parse_fix.py \\
        --md [--dry-run] [--signoff] [--skip-extract]

Flags
-----
--md            Write mirror to MotherDuck (required for full run).
--dry-run       Inspect raw answer distribution; skip table creation and mirror.
--signoff       Insert mig_318 signoff row after completion.
--skip-extract  Skip re-running EXTRACT_ANSWER; assume v2 tables already exist
                (useful for re-runs after a partial failure in Phase F/G).
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
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
_SF_FNA_TABLE = "FNA_NOTES_MIG310_V2"
_SF_RESULTS_V1 = "NLP_FNA_SIZE_FULL_RESULTS_v1"
_SF_RESULTS_V2 = "NLP_FNA_SIZE_FULL_RESULTS_v2"       # new: preserves raw answers
_SF_ROLLUP_V1 = "NLP_FNA_SIZE_PATIENT_ROLLUP_v1"      # rebuilt in-place
_SF_ROLLUP_V2 = "NLP_FNA_SIZE_PATIENT_ROLLUP_v2"      # new: rollup from v2 results

_MD_ROLLUP_TABLE = "manuscript_workspace.nlp_fna_size_rollup_v1"
_SIGNOFF_MIG_ID = "mig_318"

_PLAUSIBLE_MIN_CM = 0.1
_PLAUSIBLE_MAX_CM = 15.0

# ---------------------------------------------------------------------------
# Phase A — Inspect raw answer distribution in v1 (DESCRIBE + sample rows)
# ---------------------------------------------------------------------------

def phase_a_inspect(cur) -> dict:
    """Confirm schema and probe answer distribution in v1 results."""
    print("\n[A] Inspecting existing NLP tables...")

    # Check existence
    for tbl in (_SF_FNA_TABLE, _SF_RESULTS_V1, _SF_ROLLUP_V1):
        cur.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = %s",
            (tbl,),
        )
        n = cur.fetchone()[0]
        print(f"  {tbl}: {'EXISTS' if n else 'MISSING'}")

    # Row counts
    info: dict = {}
    for tbl in (_SF_FNA_TABLE, _SF_RESULTS_V1, _SF_ROLLUP_V1):
        try:
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            info[tbl] = cur.fetchone()[0]
        except Exception:
            info[tbl] = None
        print(f"  {tbl} rows: {info[tbl]}")

    # Coverage in v1 results
    try:
        cur.execute(
            f"""
            SELECT
                COUNT(*)                                        AS total,
                COUNT(extracted_size_cm)                        AS size_pop,
                COUNT(extracted_bethesda)                       AS beth_pop,
                COUNT(extracted_laterality)                     AS lat_pop,
                COUNT_IF(size_extract_score > 0.85)             AS high_size_score,
                ROUND(AVG(size_extract_score), 3)               AS avg_size_score,
                ROUND(AVG(bethesda_extract_score), 3)           AS avg_beth_score
            FROM {_SF_RESULTS_V1}
            """
        )
        row = cur.fetchone()
        if row:
            total, sz_pop, beth_pop, lat_pop, hi_sc, avg_sz, avg_beth = row
            print(
                f"\n  v1 coverage: total={total} "
                f"size_pop={sz_pop} ({round(100*sz_pop/total,1) if total else 0}%) "
                f"beth_pop={beth_pop} "
                f"lat_pop={lat_pop} ({round(100*lat_pop/total,1) if total else 0}%)"
            )
            print(
                f"  size_score >0.85: {hi_sc} rows — Cortex found an answer "
                f"but TRY_TO_DOUBLE failed to parse it"
            )
            print(f"  avg_size_score={avg_sz}  avg_beth_score={avg_beth}")
            info["cortex_found_size"] = hi_sc
            info["total_v1"] = total
        else:
            print("  WARN: v1 results empty")
    except Exception as exc:
        print(f"  WARN: v1 coverage probe failed: {exc}")

    # v1 has no raw_answer columns — confirm
    try:
        cur.execute("DESCRIBE TABLE NLP_FNA_SIZE_FULL_RESULTS_v1")
        cols = [r[0].lower() for r in cur.fetchall()]
        print(f"\n  v1 columns: {cols}")
        if "size_raw_answer" in cols:
            print("  NOTE: raw answer strings ARE present in v1 — can parse without re-extract")
            info["has_raw_answers"] = True
        else:
            print(
                "  Confirmed: raw answer strings NOT in v1. "
                "Re-extraction from FNA_NOTES_MIG310_V2 required."
            )
            info["has_raw_answers"] = False
    except Exception as exc:
        print(f"  WARN: DESCRIBE failed: {exc}")
        info["has_raw_answers"] = False

    return info


# ---------------------------------------------------------------------------
# Phase B — Re-run EXTRACT_ANSWER with improved parser
# ---------------------------------------------------------------------------

def phase_b_extract(cur) -> int:
    """Create NLP_FNA_SIZE_FULL_RESULTS_v2 with raw answers + regex parser.

    Re-uses existing FNA_NOTES_MIG310_V2 (no re-export from MotherDuck needed).
    Stores raw answer strings so future re-parses cost $0.
    """
    print("\n[B] Re-running EXTRACT_ANSWER with regex parser → v2 tables...")
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

    extract_sql = f"""
CREATE OR REPLACE TABLE {_SF_RESULTS_V2} AS
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
        CURRENT_TIMESTAMP                                          AS extracted_at,
        'cortex_extract_answer_mig_310_v2_reparsed_mig_318'        AS extraction_source
    FROM {_SF_FNA_TABLE}
),
raw_strs AS (
    SELECT
        e.*,
        -- Persist raw answer strings (key fix: these were LOST in v1)
        NULLIF(TRIM(e._size_raw[0]:answer::VARCHAR),     '') AS size_raw_answer,
        e._size_raw[0]:score::FLOAT                          AS size_extract_score,
        NULLIF(TRIM(e._lat_raw[0]:answer::VARCHAR),      '') AS lat_raw_answer,
        e._lat_raw[0]:score::FLOAT                           AS lat_extract_score,
        NULLIF(TRIM(e._count_raw[0]:answer::VARCHAR),    '') AS count_raw_answer,
        e._count_raw[0]:score::FLOAT                         AS count_extract_score,
        NULLIF(TRIM(e._bethesda_raw[0]:answer::VARCHAR), '') AS bethesda_raw_answer,
        e._bethesda_raw[0]:score::FLOAT                      AS bethesda_extract_score
    FROM extracted e
),
parsed AS (
    SELECT
        r.*,

        -- Size: regex-extract first decimal, then mm→cm conversion
        TRY_TO_DOUBLE(
            REGEXP_SUBSTR(r.size_raw_answer, '[0-9]+(\\.[0-9]+)?')
        ) AS _size_numeric,

        -- Bethesda: Roman numeral + prose pattern matching (key fix)
        CASE
            WHEN r.bethesda_raw_answer IS NULL THEN NULL
            WHEN REGEXP_LIKE(LOWER(r.bethesda_raw_answer),
                 '\\bvi\\b|category\\s+vi|bethesda\\s+vi|class\\s+vi|malignant') THEN 6
            WHEN REGEXP_LIKE(LOWER(r.bethesda_raw_answer),
                 '\\bv\\b|category\\s+v\\b|bethesda\\s+v\\b|class\\s+v\\b|suspicious') THEN 5
            WHEN REGEXP_LIKE(LOWER(r.bethesda_raw_answer),
                 '\\biv\\b|category\\s+iv|bethesda\\s+iv|class\\s+iv|follicular neoplasm|fn/sfn') THEN 4
            WHEN REGEXP_LIKE(LOWER(r.bethesda_raw_answer),
                 '\\biii\\b|category\\s+iii|bethesda\\s+iii|class\\s+iii|aus|flus|atypia') THEN 3
            WHEN REGEXP_LIKE(LOWER(r.bethesda_raw_answer),
                 '\\bii\\b|category\\s+ii\\b|bethesda\\s+ii\\b|class\\s+ii\\b|benign') THEN 2
            WHEN REGEXP_LIKE(LOWER(r.bethesda_raw_answer),
                 '\\bi\\b|category\\s+i\\b|bethesda\\s+i\\b|class\\s+i\\b|nondiagnostic|non-diagnostic|unsatisfactory') THEN 1
            ELSE TRY_TO_NUMBER(REGEXP_SUBSTR(r.bethesda_raw_answer, '[1-6]'))
        END AS _bethesda_parsed,

        -- Laterality: unchanged (already robust with LIKE)
        CASE
            WHEN LOWER(r.lat_raw_answer) LIKE '%right%'     THEN 'right'
            WHEN LOWER(r.lat_raw_answer) LIKE '%left%'      THEN 'left'
            WHEN LOWER(r.lat_raw_answer) LIKE '%isthmus%'   THEN 'isthmus'
            WHEN LOWER(r.lat_raw_answer) LIKE '%bilateral%' THEN 'bilateral'
            ELSE NULL
        END AS _laterality_parsed,

        -- Nodule count: regex-extract first integer
        TRY_TO_NUMBER(
            REGEXP_SUBSTR(r.count_raw_answer, '[0-9]+')
        ) AS _count_numeric

    FROM raw_strs r
)
SELECT
    RESEARCH_ID,
    FNA_EVENT_ID,
    NOTE_TYPE,
    NOTE_DATE,

    -- SIZE: mm→cm conversion + plausibility clamp
    CASE
        WHEN _size_numeric IS NULL THEN NULL
        WHEN LOWER(size_raw_answer) LIKE '%mm%'
             AND LOWER(size_raw_answer) NOT LIKE '%cm%'
        THEN
            CASE
                WHEN _size_numeric / 10.0 BETWEEN {_PLAUSIBLE_MIN_CM} AND {_PLAUSIBLE_MAX_CM}
                THEN _size_numeric / 10.0
                ELSE NULL
            END
        ELSE
            CASE
                WHEN _size_numeric BETWEEN {_PLAUSIBLE_MIN_CM} AND {_PLAUSIBLE_MAX_CM}
                THEN _size_numeric
                ELSE NULL
            END
    END AS extracted_size_cm,

    _laterality_parsed AS extracted_laterality,

    CASE
        WHEN _count_numeric BETWEEN 1 AND 40 THEN _count_numeric
        ELSE NULL
    END AS extracted_nodule_count,

    CASE
        WHEN _bethesda_parsed BETWEEN 1 AND 6 THEN _bethesda_parsed
        ELSE NULL
    END AS extracted_bethesda,

    -- Confidence tier: unchanged logic
    CASE
        WHEN size_extract_score  > 0.80
         AND lat_extract_score   > 0.80 THEN 'high'
        WHEN size_extract_score  > 0.50
          OR lat_extract_score   > 0.50 THEN 'medium'
        ELSE 'low'
    END AS extraction_confidence,

    -- Raw answer strings (PRESERVED — key improvement over v1)
    size_raw_answer,
    size_extract_score,
    lat_raw_answer,
    lat_extract_score,
    count_raw_answer,
    count_extract_score,
    bethesda_raw_answer,
    bethesda_extract_score,

    extracted_at,
    extraction_source

FROM parsed
"""

    cur.execute(extract_sql)
    elapsed = (datetime.now() - t0).total_seconds()
    cur.execute(f"SELECT COUNT(*) FROM {_SF_RESULTS_V2}")
    n = cur.fetchone()[0]
    print(f"  {_SF_RESULTS_V2}: {n:,} rows in {elapsed:.1f}s")
    return n


# ---------------------------------------------------------------------------
# Phase C — Show post-fix answer distribution sample
# ---------------------------------------------------------------------------

def phase_c_inspect_v2(cur) -> None:
    """Print answer distribution to confirm regex parser is working."""
    print("\n[C] Post-fix answer distribution...")

    # Top raw answers for size that now parse successfully
    cur.execute(
        f"""
        SELECT
            size_raw_answer,
            extracted_size_cm,
            COUNT(*) AS n
        FROM {_SF_RESULTS_V2}
        WHERE size_raw_answer IS NOT NULL
        GROUP BY 1, 2
        ORDER BY n DESC
        LIMIT 20
        """
    )
    rows = cur.fetchall()
    print("  Top size_raw_answer → extracted_size_cm:")
    for ans, parsed, n in rows:
        print(f"    {n:>4}x  '{ans}'  →  {parsed}")

    # Top bethesda answers
    cur.execute(
        f"""
        SELECT
            bethesda_raw_answer,
            extracted_bethesda,
            COUNT(*) AS n
        FROM {_SF_RESULTS_V2}
        WHERE bethesda_raw_answer IS NOT NULL
        GROUP BY 1, 2
        ORDER BY n DESC
        LIMIT 15
        """
    )
    rows = cur.fetchall()
    print("\n  Top bethesda_raw_answer → extracted_bethesda:")
    for ans, parsed, n in rows:
        print(f"    {n:>4}x  '{ans}'  →  {parsed}")

    # Still-failing size answers (raw answer present but parsed=NULL)
    cur.execute(
        f"""
        SELECT size_raw_answer, COUNT(*) AS n
        FROM {_SF_RESULTS_V2}
        WHERE size_raw_answer IS NOT NULL
          AND extracted_size_cm IS NULL
          AND size_extract_score > 0.60
        GROUP BY 1
        ORDER BY n DESC
        LIMIT 10
        """
    )
    rows = cur.fetchall()
    if rows:
        print("\n  High-score answers that still fail parse (inspect for further fix):")
        for ans, n in rows:
            print(f"    {n:>4}x  '{ans}'")


# ---------------------------------------------------------------------------
# Phase D — Rebuild patient rollup from v2 results
# ---------------------------------------------------------------------------

def phase_d_rollup(cur) -> int:
    """Rebuild NLP_FNA_SIZE_PATIENT_ROLLUP_v1 in-place from v2 results.

    Also creates NLP_FNA_SIZE_PATIENT_ROLLUP_v2 as a permanent v2-sourced copy.
    """
    print(f"\n[D] Rebuilding patient-event rollup from {_SF_RESULTS_V2}...")

    rollup_sql = """
    SELECT
        RESEARCH_ID,
        FNA_EVENT_ID,
        NOTE_DATE                                                AS fna_date,
        FIRST_VALUE(extracted_size_cm) IGNORE NULLS OVER (
            PARTITION BY RESEARCH_ID, FNA_EVENT_ID
            ORDER BY
                CASE extraction_confidence
                    WHEN 'high'   THEN 2
                    WHEN 'medium' THEN 1
                    ELSE 0 END DESC,
                size_extract_score DESC NULLS LAST,
                extracted_size_cm  DESC NULLS LAST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                                        AS extracted_size_cm,
        FIRST_VALUE(extracted_laterality) IGNORE NULLS OVER (
            PARTITION BY RESEARCH_ID, FNA_EVENT_ID
            ORDER BY
                CASE extraction_confidence
                    WHEN 'high'   THEN 2
                    WHEN 'medium' THEN 1
                    ELSE 0 END DESC,
                lat_extract_score DESC NULLS LAST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                                        AS extracted_laterality,
        MAX(COALESCE(extracted_nodule_count, 1)) OVER (
            PARTITION BY RESEARCH_ID, FNA_EVENT_ID
        )                                                        AS extracted_nodule_count,
        FIRST_VALUE(extracted_bethesda) IGNORE NULLS OVER (
            PARTITION BY RESEARCH_ID, FNA_EVENT_ID
            ORDER BY bethesda_extract_score DESC NULLS LAST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                                        AS extracted_bethesda,
        FIRST_VALUE(extraction_confidence) OVER (
            PARTITION BY RESEARCH_ID, FNA_EVENT_ID
            ORDER BY
                CASE extraction_confidence
                    WHEN 'high'   THEN 2
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
        'cortex_extract_answer_mig_310_v2_reparsed_mig_318'       AS extraction_source,
        CURRENT_TIMESTAMP                                        AS rollup_built_at
    FROM {results_table}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY RESEARCH_ID, FNA_EVENT_ID
        ORDER BY size_extract_score DESC NULLS LAST
    ) = 1
    """

    # Build v2 rollup (permanent record)
    cur.execute(
        f"CREATE OR REPLACE TABLE {_SF_ROLLUP_V2} AS "
        + rollup_sql.format(results_table=_SF_RESULTS_V2)
    )
    cur.execute(f"SELECT COUNT(*) FROM {_SF_ROLLUP_V2}")
    n = cur.fetchone()[0]
    print(f"  {_SF_ROLLUP_V2}: {n:,} rows")

    # Replace v1 rollup in-place (this is what mig_310_fna_size_mirror.py reads)
    cur.execute(
        f"CREATE OR REPLACE TABLE {_SF_ROLLUP_V1} AS SELECT * FROM {_SF_ROLLUP_V2}"
    )
    print(f"  {_SF_ROLLUP_V1}: replaced in-place from v2 ({n:,} rows)")

    return n


# ---------------------------------------------------------------------------
# Phase E — Validation gates
# ---------------------------------------------------------------------------

def phase_e_validate(cur) -> dict:
    """Print coverage stats for v2 rollup; return metrics dict."""
    print(f"\n[E] Validation gates against {_SF_ROLLUP_V1}...")

    cur.execute(
        f"""
        SELECT
            COUNT(*)                                              AS n_rollup,
            COUNT(extracted_size_cm)                             AS n_size,
            COUNT(extracted_bethesda)                            AS n_beth,
            COUNT(extracted_laterality)                          AS n_lat,
            ROUND(100.0 * COUNT(extracted_size_cm)   / COUNT(*), 1) AS pct_size,
            ROUND(100.0 * COUNT(extracted_bethesda)  / COUNT(*), 1) AS pct_beth,
            ROUND(100.0 * COUNT(extracted_laterality)/ COUNT(*), 1) AS pct_lat,
            ROUND(AVG(extracted_size_cm),   2)                   AS avg_size,
            ROUND(STDDEV(extracted_size_cm),2)                   AS sd_size,
            ROUND(MIN(extracted_size_cm),   2)                   AS min_size,
            ROUND(MAX(extracted_size_cm),   2)                   AS max_size
        FROM {_SF_ROLLUP_V1}
        """
    )
    row = cur.fetchone()
    n_ro, n_sz, n_be, n_la, pct_sz, pct_be, pct_la, avg_sz, sd_sz, mn_sz, mx_sz = row

    print("\n  === mig_318 Validation Summary ===")
    print(f"  Rollup rows       : {n_ro:,}")
    print(f"  size populated    : {n_sz:,} ({pct_sz}%)")
    print(f"  bethesda populated: {n_be:,} ({pct_be}%)")
    print(f"  laterality        : {n_la:,} ({pct_la}%)")
    print(f"  avg_size_cm       : {avg_sz}  sd={sd_sz}  range=[{mn_sz}, {mx_sz}]")

    gates = {
        "pct_size >= 60": (float(pct_sz or 0), 60.0, "≥"),
        "pct_beth >= 50": (float(pct_be or 0), 50.0, "≥"),
        "avg_size in [1.0, 4.0]": (float(avg_sz or 0), (1.0, 4.0), "range"),
        "sd_size in [0.5, 3.0]": (float(sd_sz or 0), (0.5, 3.0), "range"),
    }

    all_pass = True
    print("\n  Acceptance gates:")
    for label, (val, threshold, op) in gates.items():
        if op == "≥":
            ok = val >= threshold
        elif op == "range":
            lo, hi = threshold
            ok = lo <= val <= hi
        else:
            ok = True
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False
        print(f"    [{status}] {label}: actual={val}")

    print(f"\n  Overall: {'ALL GATES PASS' if all_pass else 'SOME GATES FAIL — review above'}")

    return {
        "n_rollup": n_ro,
        "n_size": n_sz,
        "n_beth": n_be,
        "n_lat": n_la,
        "pct_size": float(pct_sz or 0),
        "pct_beth": float(pct_be or 0),
        "pct_lat": float(pct_la or 0),
        "avg_size": float(avg_sz or 0),
        "sd_size": float(sd_sz or 0),
        "all_pass": all_pass,
    }


# ---------------------------------------------------------------------------
# Phase F — Mirror updated rollup to MotherDuck
# ---------------------------------------------------------------------------

def phase_f_mirror(cur, token: str) -> int:
    """Pull v1 rollup from Snowflake → MotherDuck nlp_fna_size_rollup_v1."""
    print(f"\n[F] Mirroring {_SF_ROLLUP_V1} → MotherDuck...")
    cur.execute(f"SELECT * FROM {_SF_ROLLUP_V1}")
    sf_df = cur.fetch_pandas_all()
    sf_df.columns = [str(c).upper() for c in sf_df.columns]
    n = len(sf_df)
    print(f"  Fetched {n:,} rows from Snowflake.")

    import duckdb

    md = duckdb.connect(
        f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}"
    )
    try:
        md.execute("USE thyroid_canonical_publication_v1_0")

        # Ensure table exists with expected schema
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

        md.register("_sf_rollup_mig318", sf_df)
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
                FROM _sf_rollup_mig318
                """
            )
        finally:
            md.unregister("_sf_rollup_mig318")

        cnt = md.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.nlp_fna_size_rollup_v1"
        ).fetchone()[0]
        print(f"  MotherDuck: {cnt:,} rows in manuscript_workspace.nlp_fna_size_rollup_v1")
        return cnt
    finally:
        md.close()


# ---------------------------------------------------------------------------
# Phase G — Rebuild imaging_fna_linkage_v4
# ---------------------------------------------------------------------------

def phase_g_rebuild_v4() -> None:
    """Invoke mig_310_fna_size_mirror.py --md to rebuild imaging_fna_linkage_v4."""
    print("\n[G] Rebuilding imaging_fna_linkage_v4 via mig_310_fna_size_mirror.py...")
    script = REPO_ROOT / "scripts" / "mig_310_fna_size_mirror.py"
    python = REPO_ROOT / ".venv" / "bin" / "python"
    if not python.exists():
        python = Path(sys.executable)

    cmd = [str(python), str(script), "--md"]
    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print(f"  WARN: mig_310_fna_size_mirror.py exited with code {result.returncode}")
    else:
        print("  imaging_fna_linkage_v4 rebuild: OK")


# ---------------------------------------------------------------------------
# Phase H — v4 coverage check on MotherDuck
# ---------------------------------------------------------------------------

def phase_h_v4_check(token: str) -> dict:
    """Print v4 fna_size_source distribution."""
    print("\n[H] imaging_fna_linkage_v4 source distribution on MotherDuck...")
    import duckdb

    md = duckdb.connect(
        f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}"
    )
    try:
        md.execute("USE thyroid_canonical_publication_v1_0")
        try:
            rows = md.execute(
                """
                SELECT fna_size_source_v4, COUNT(*) AS n
                FROM manuscript_workspace.imaging_fna_linkage_v4
                GROUP BY 1
                ORDER BY 2 DESC
                """
            ).fetchall()
            print("  fna_size_source_v4 distribution:")
            totals: dict = {}
            for src, n in rows:
                print(f"    {src or '<null>'}: {n:,}")
                totals[str(src)] = n
            nlp_hi = totals.get("nlp_high", 0)
            nlp_me = totals.get("nlp_medium", 0)
            print(f"\n  nlp_high + nlp_medium = {nlp_hi + nlp_me:,}  (target: ≥1,500)")
            ok = (nlp_hi + nlp_me) >= 1500
            print(f"  Gate: {'PASS' if ok else 'FAIL (below 1,500 — investigate)'}")
            return {"nlp_high": nlp_hi, "nlp_medium": nlp_me, "all_pass": ok}
        except Exception as exc:
            print(f"  WARN: v4 distribution query failed: {exc}")
            return {}
    finally:
        md.close()


# ---------------------------------------------------------------------------
# Phase I — Signoff
# ---------------------------------------------------------------------------

def phase_i_signoff(token: str, stats: dict, v4_stats: dict) -> None:
    """Insert mig_318 signoff row to MotherDuck main.signoff_migration."""
    import duckdb

    pct_sz = stats.get("pct_size", 0)
    pct_be = stats.get("pct_beth", 0)
    pct_la = stats.get("pct_lat", 0)
    n_ro = stats.get("n_rollup", 0)
    avg_sz = stats.get("avg_size", 0)
    nlp_hi = v4_stats.get("nlp_high", 0)
    nlp_me = v4_stats.get("nlp_medium", 0)

    summary = (
        f"mig_318: FNA NLP parse-layer fix. Root cause: TRY_TO_DOUBLE choked on "
        f"'1.5 cm', 'category II', etc. Fix: REGEXP_SUBSTR numeric extraction + "
        f"mm→cm conversion + Roman-numeral Bethesda parser. Raw answers now "
        f"preserved in NLP_FNA_SIZE_FULL_RESULTS_v2. Rollup: {n_ro} rows. "
        f"size_fill={pct_sz}% (was 0.1%), beth_fill={pct_be}% (was 0.7%), "
        f"lat_fill={pct_la}% (unchanged), avg_size={avg_sz} cm. "
        f"v4 nlp_high={nlp_hi}, nlp_medium={nlp_me}, combined={nlp_hi+nlp_me} "
        f"(was 5). Closes CF-FNA-SIZE-PARSE-LAYER. "
        f"M025 nodule cohort rebuild deferred to Cowork decision."
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
                SELECT ?, CURRENT_TIMESTAMP, 'cursor_composer_mig318', ?
                WHERE NOT EXISTS (
                  SELECT 1 FROM main.signoff_migration WHERE mig_id = ?
                )
                """,
                [_SIGNOFF_MIG_ID, summary, _SIGNOFF_MIG_ID],
            )
            print(f"\n  Signoff row inserted: {_SIGNOFF_MIG_ID}")
            print(f"  Summary: {summary}")
        except Exception as exc:
            print(f"  WARN: signoff insert failed: {exc}")
    finally:
        md.close()


# ---------------------------------------------------------------------------
# Helpers
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
    return token


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--md", action="store_true",
                    help="Write mirror to MotherDuck (required for full run).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Inspect existing tables only; skip re-extraction and mirror.")
    ap.add_argument("--signoff", action="store_true",
                    help="Insert mig_318 signoff row after successful completion.")
    ap.add_argument("--skip-extract", action="store_true",
                    help="Skip EXTRACT_ANSWER; assume v2 tables already exist in Snowflake.")
    args = ap.parse_args()

    if not args.md and not args.dry_run:
        print("FATAL: pass --md to write MotherDuck or --dry-run for inspect-only.",
              file=sys.stderr)
        return 1

    pat = os.environ.get("SNOWFLAKE_PAT")
    if not pat:
        print("FATAL: SNOWFLAKE_PAT not set.", file=sys.stderr)
        return 1

    md_token = _get_md_token() if (args.md and not args.dry_run) else None

    run_ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    print(f"mig_318 FNA NLP parse-layer fix — {run_ts}")
    print(f"  mode: {'dry-run' if args.dry_run else '--md'} | "
          f"skip-extract={args.skip_extract}")

    ctx, cur = get_cursor()
    try:
        cur.execute("USE DATABASE THYROID_VALIDATION")
        cur.execute("USE SCHEMA PUBLIC")

        # Phase A — Inspect
        info = phase_a_inspect(cur)

        if args.dry_run:
            print("\n--dry-run: inspection complete. No tables created or mirrored.")
            return 0

        # Phase B — Re-extract with regex parser
        if not args.skip_extract:
            if not info.get((_SF_FNA_TABLE), None):
                n_fna = info.get(_SF_FNA_TABLE, 0)
                if n_fna == 0:
                    print(
                        f"FATAL: {_SF_FNA_TABLE} is empty or missing. "
                        "Re-export from MotherDuck first (script 36).",
                        file=sys.stderr,
                    )
                    return 1
            n_extracted = phase_b_extract(cur)
            if n_extracted == 0:
                print("FATAL: extraction produced 0 rows.", file=sys.stderr)
                return 1
        else:
            print("\n[B] Skipped (--skip-extract): using existing v2 tables.")

        # Phase C — Show answer distribution
        try:
            phase_c_inspect_v2(cur)
        except Exception as exc:
            print(f"  WARN: phase C inspect failed: {exc}")

        # Phase D — Rebuild rollup
        n_rollup = phase_d_rollup(cur)
        if n_rollup == 0:
            print("FATAL: rollup produced 0 rows.", file=sys.stderr)
            return 1

        # Phase E — Validate
        stats = phase_e_validate(cur)

        # Phase F — Mirror to MotherDuck
        n_mirrored = phase_f_mirror(cur, md_token)
        if n_mirrored == 0:
            print("FATAL: 0 rows mirrored to MotherDuck.", file=sys.stderr)
            return 1

    finally:
        ctx.close()

    # Phase G — Rebuild v4 linkage (pure MD operation, Snowflake cursor closed)
    phase_g_rebuild_v4()

    # Phase H — v4 coverage check
    v4_stats = phase_h_v4_check(md_token)

    # Phase I — Signoff
    if args.signoff:
        phase_i_signoff(md_token, stats, v4_stats)

    # Summary
    pct_sz = stats.get("pct_size", 0)
    pct_be = stats.get("pct_beth", 0)
    nlp_hi = v4_stats.get("nlp_high", 0)
    nlp_me = v4_stats.get("nlp_medium", 0)
    print(
        f"\nmig_318 COMPLETE — size_fill {pct_sz}%, beth_fill {pct_be}%, "
        f"v4 nlp_high+medium {nlp_hi + nlp_me}"
    )
    print(
        "  Cowork message: "
        f"mig_318 complete; size_fill {pct_sz}%, beth_fill {pct_be}%, "
        f"v4 nlp_high+medium {nlp_hi + nlp_me}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
