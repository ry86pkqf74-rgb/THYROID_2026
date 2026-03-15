#!/usr/bin/env python3
"""
97_episode_linkage_audit.py  —  Multi-surgery episode linkage integrity audit

Identifies every patient with >1 distinct surgery date, builds a canonical
multi-surgery cohort, then audits ALL clinical artifacts (notes, labs, RAI,
molecular, FNA, imaging, pathology, complications) against the temporal
surgery windows to detect mislinks, ambiguities, and orphaned artifacts.

Output tables  (all deployed to MotherDuck target env)
─────────────────────────────────────────────────────────
  multi_surgery_episode_cohort_v1      — one row per surgery per patient
  val_episode_artifact_assignment_v1   — per-artifact → surgery assignment
  val_episode_mislink_candidates_v1    — detected temporal/laterality mislinks
  val_episode_linkage_integrity_v1     — per-patient linkage quality grade
  val_episode_key_propagation_v1       — surgery-episode-id propagation status
  val_episode_linkage_summary_v1       — aggregate KPI summary
  val_episode_ambiguity_review_v1      — artifacts equidistant between surgeries

Usage
─────
  .venv/bin/python scripts/97_episode_linkage_audit.py --env dev
  .venv/bin/python scripts/97_episode_linkage_audit.py --env prod --dry-run

Workflow: deploy to dev → validate → promote qa → prod via scripts/95.
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import duckdb
import pandas as pd

# ── helpers ────────────────────────────────────────────────────────────────

def section(title: str) -> None:
    print(f"\n{'=' * 78}")
    print(f"  {title}")
    print(f"{'=' * 78}")


def get_token() -> str:
    for key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN"):
        tok = os.environ.get(key)
        if tok:
            return tok
    # fallback: secrets.toml
    candidates = [
        Path.home() / ".streamlit" / "secrets.toml",
        ROOT / ".streamlit" / "secrets.toml",
        Path("/Users/ros/Desktop/FInal Cleaned Thyroid Data/.streamlit/secrets.toml"),
    ]
    for p in candidates:
        if p.exists():
            try:
                import toml
                return toml.load(str(p))["MOTHERDUCK_TOKEN"]
            except Exception:
                continue
    raise RuntimeError("No MOTHERDUCK_TOKEN found in env or secrets.toml")


def resolve_db(env: str) -> str:
    m = {"dev": "thyroid_research_2026_dev",
         "qa": "thyroid_research_2026_qa",
         "prod": "thyroid_research_2026"}
    return m.get(env, env)


def src_prefix(env: str) -> str:
    """Return the qualified prefix for reading source tables from prod.
    
    In workspace mode, all databases in the account are accessible by name.
    No ATTACH needed — just qualify with 'thyroid_research_2026.' for dev/qa.
    """
    return "thyroid_research_2026." if env in ("dev", "qa") else ""


def connect(env: str) -> duckdb.DuckDBPyConnection:
    tok = get_token()
    db = resolve_db(env)
    con = duckdb.connect(f"md:{db}?motherduck_token={tok}")
    print(f"  ✓ Connected to md:{db}")
    if env in ("dev", "qa"):
        print(f"  ℹ Source tables read from thyroid_research_2026.* (workspace mode)")
    return con


def tbl_exists(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 0")
        return True
    except Exception:
        return False


def safe_exec(con: duckdb.DuckDBPyConnection, sql: str, label: str = "") -> int:
    try:
        con.execute(sql)
        try:
            n = con.fetchone()
            return n[0] if n else 0
        except Exception:
            return 0
    except Exception as e:
        print(f"  ⚠ {label}: {e}")
        return -1


def materialize_df(con: duckdb.DuckDBPyConnection, df: pd.DataFrame,
                   table_name: str) -> int:
    """Write a DataFrame to MotherDuck via parquet intermediary."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = f.name
    df.to_parquet(tmp, index=False)
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(
        f"CREATE TABLE {table_name} AS "
        f"SELECT * FROM read_parquet('{tmp}')"
    )
    os.unlink(tmp)
    return len(df)


# ── SQL builders: accept {S} = source prefix ─────────────────────────────
def COHORT_SQL(S: str) -> str:
    return f"""
CREATE OR REPLACE TABLE multi_surgery_episode_cohort_v1 AS
WITH distinct_surgeries AS (
    SELECT
        research_id,
        TRY_CAST(surg_date AS DATE) AS surgery_date,
        LOWER(COALESCE(thyroid_procedure, 'unknown'))  AS procedure_raw,
        LOWER(COALESCE(completion, ''))                 AS completion_flag,
        LOWER(COALESCE(reop, ''))                       AS reop_flag,
        CASE
            WHEN LOWER(thyroid_procedure) LIKE '%right%' OR LOWER(thyroid_procedure) LIKE '%(rl)%' THEN 'right'
            WHEN LOWER(thyroid_procedure) LIKE '%left%'  OR LOWER(thyroid_procedure) LIKE '%(ll)%' THEN 'left'
            WHEN LOWER(thyroid_procedure) LIKE '%total%' OR LOWER(thyroid_procedure) LIKE '%bilateral%' THEN 'bilateral'
            WHEN LOWER(thyroid_procedure) LIKE '%isthmus%' THEN 'isthmus'
            ELSE 'unspecified'
        END AS laterality,
        CASE
            WHEN LOWER(thyroid_procedure) LIKE '%total%' THEN 'total_thyroidectomy'
            WHEN LOWER(thyroid_procedure) LIKE '%lobectomy%' OR LOWER(thyroid_procedure) LIKE '%(rl)%'
                 OR LOWER(thyroid_procedure) LIKE '%(ll)%' THEN 'lobectomy'
            WHEN LOWER(thyroid_procedure) LIKE '%subtotal%' THEN 'subtotal'
            WHEN LOWER(thyroid_procedure) LIKE '%isthmus%' THEN 'isthmusectomy'
            ELSE 'other'
        END AS procedure_normalized,
        gender AS sex,
        race,
        age
    FROM {S}path_synoptics
    WHERE surg_date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id, TRY_CAST(surg_date AS DATE)
        ORDER BY CASE WHEN thyroid_procedure IS NOT NULL THEN 0 ELSE 1 END,
                 CASE WHEN completion IN ('yes','y','completion') THEN 0 ELSE 1 END
    ) = 1
),
multi AS (
    SELECT research_id
    FROM distinct_surgeries
    GROUP BY research_id
    HAVING COUNT(*) > 1
),
ranked AS (
    SELECT
        ds.*,
        ROW_NUMBER() OVER (PARTITION BY ds.research_id ORDER BY ds.surgery_date) AS surgery_rank,
        COUNT(*)     OVER (PARTITION BY ds.research_id)                          AS total_surgeries,
        LAG(ds.surgery_date) OVER (PARTITION BY ds.research_id ORDER BY ds.surgery_date) AS prev_surgery_date,
        LEAD(ds.surgery_date) OVER (PARTITION BY ds.research_id ORDER BY ds.surgery_date) AS next_surgery_date,
        DATEDIFF('day',
            LAG(ds.surgery_date) OVER (PARTITION BY ds.research_id ORDER BY ds.surgery_date),
            ds.surgery_date
        ) AS days_since_prev_surgery,
        CASE
            WHEN LOWER(COALESCE(ds.completion_flag,'')) IN ('yes','y','completion') THEN TRUE
            ELSE FALSE
        END AS is_completion
    FROM distinct_surgeries ds
    JOIN multi m ON ds.research_id = m.research_id
)
SELECT
    research_id,
    surgery_date,
    surgery_rank,
    total_surgeries,
    procedure_raw,
    procedure_normalized,
    laterality,
    is_completion,
    completion_flag,
    reop_flag,
    sex,
    race,
    age,
    prev_surgery_date,
    next_surgery_date,
    days_since_prev_surgery,
    CASE WHEN next_surgery_date IS NOT NULL
         THEN surgery_date + INTERVAL (DATEDIFF('day', surgery_date, next_surgery_date) / 2) DAY
         ELSE NULL
    END AS midpoint_to_next,
    CASE WHEN prev_surgery_date IS NOT NULL
         THEN prev_surgery_date + INTERVAL (DATEDIFF('day', prev_surgery_date, surgery_date) / 2) DAY
         ELSE TRY_CAST('1900-01-01' AS DATE)
    END AS window_start,
    CASE WHEN next_surgery_date IS NOT NULL
         THEN surgery_date + INTERVAL (DATEDIFF('day', surgery_date, next_surgery_date) / 2) DAY
         ELSE TRY_CAST('2099-12-31' AS DATE)
    END AS window_end,
    CURRENT_TIMESTAMP AS audit_ts
FROM ranked
ORDER BY research_id, surgery_rank
"""


# ── SQL: artifact assignment ──────────────────────────────────────────────
def ARTIFACT_ASSIGNMENT_SQL(S: str) -> str:
    return f"""
CREATE OR REPLACE TABLE val_episode_artifact_assignment_v1 AS

-- clinical notes
WITH note_assignments AS (
    SELECT
        cn.research_id,
        'clinical_note'          AS artifact_domain,
        cn.note_type             AS artifact_subtype,
        cn.note_row_id           AS artifact_id,
        TRY_CAST(cn.note_date AS DATE) AS artifact_date,
        c.surgery_date,
        c.surgery_rank,
        c.laterality             AS surgery_laterality,
        ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), c.surgery_date)) AS day_gap,
        CASE
            WHEN cn.note_type = 'op_note'
                 AND ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), c.surgery_date)) <= 1
                 THEN 'exact_match'
            WHEN ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), c.surgery_date)) <= 7
                 THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), c.surgery_date)) <= 30
                 THEN 'plausible'
            WHEN ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), c.surgery_date)) <= 180
                 THEN 'weak'
            ELSE 'unlinked'
        END AS assignment_confidence,
        TRY_CAST(cn.note_date AS DATE) >= c.window_start
            AND TRY_CAST(cn.note_date AS DATE) < c.window_end AS in_window
    FROM {S}clinical_notes_long cn
    JOIN multi_surgery_episode_cohort_v1 c
        ON TRY_CAST(cn.research_id AS INTEGER) = c.research_id
    WHERE cn.note_date IS NOT NULL
       AND TRIM(CAST(cn.note_date AS VARCHAR)) != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cn.research_id, cn.note_row_id
        ORDER BY day_gap,
                 CASE assignment_confidence
                     WHEN 'exact_match'     THEN 1
                     WHEN 'high_confidence'  THEN 2
                     WHEN 'plausible'        THEN 3
                     WHEN 'weak'             THEN 4
                     ELSE 5
                 END
    ) = 1
),

-- thyroglobulin / anti-Tg / PTH / calcium labs
lab_assignments AS (
    SELECT
        TRY_CAST(l.research_id AS INTEGER) AS research_id,
        'lab'                             AS artifact_domain,
        l.analyte_group                   AS artifact_subtype,
        l.research_id || '_' || COALESCE(CAST(l.lab_date AS VARCHAR),'unk')
            || '_' || l.analyte_group     AS artifact_id,
        TRY_CAST(l.lab_date AS DATE)      AS artifact_date,
        c.surgery_date,
        c.surgery_rank,
        c.laterality                      AS surgery_laterality,
        ABS(DATEDIFF('day', TRY_CAST(l.lab_date AS DATE), c.surgery_date)) AS day_gap,
        CASE
            WHEN ABS(DATEDIFF('day', TRY_CAST(l.lab_date AS DATE), c.surgery_date)) <= 7
                 THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', TRY_CAST(l.lab_date AS DATE), c.surgery_date)) <= 90
                 THEN 'plausible'
            WHEN ABS(DATEDIFF('day', TRY_CAST(l.lab_date AS DATE), c.surgery_date)) <= 365
                 THEN 'weak'
            ELSE 'unlinked'
        END AS assignment_confidence,
        TRY_CAST(l.lab_date AS DATE) >= c.window_start
            AND TRY_CAST(l.lab_date AS DATE) < c.window_end AS in_window
    FROM {S}longitudinal_lab_canonical_v1 l
    JOIN multi_surgery_episode_cohort_v1 c
        ON TRY_CAST(l.research_id AS INTEGER) = c.research_id
    WHERE l.lab_date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY l.research_id, l.lab_date, l.analyte_group
        ORDER BY day_gap
    ) = 1
),

-- RAI treatment episodes
rai_assignments AS (
    SELECT
        r.research_id,
        'rai'                            AS artifact_domain,
        'rai_treatment'                  AS artifact_subtype,
        CAST(r.rai_episode_id AS VARCHAR) AS artifact_id,
        COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)) AS artifact_date,
        c.surgery_date,
        c.surgery_rank,
        c.laterality                     AS surgery_laterality,
        ABS(DATEDIFF('day',
            COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)),
            c.surgery_date
        )) AS day_gap,
        CASE
            WHEN ABS(DATEDIFF('day',
                COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)),
                c.surgery_date)) <= 30
                 THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day',
                COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)),
                c.surgery_date)) <= 180
                 THEN 'plausible'
            WHEN ABS(DATEDIFF('day',
                COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)),
                c.surgery_date)) <= 365
                 THEN 'weak'
            ELSE 'unlinked'
        END AS assignment_confidence,
        COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)) >= c.window_start
            AND COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)) < c.window_end
            AS in_window
    FROM {S}rai_treatment_episode_v2 r
    JOIN multi_surgery_episode_cohort_v1 c ON r.research_id = c.research_id
    WHERE COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY r.research_id, r.rai_episode_id
        ORDER BY day_gap
    ) = 1
),

-- molecular tests
mol_assignments AS (
    SELECT
        mol.research_id,
        'molecular'                       AS artifact_domain,
        COALESCE(mol.platform, 'unknown') AS artifact_subtype,
        CAST(mol.molecular_episode_id AS VARCHAR) AS artifact_id,
        mol.test_date_native              AS artifact_date,
        c.surgery_date,
        c.surgery_rank,
        c.laterality                      AS surgery_laterality,
        ABS(DATEDIFF('day', mol.test_date_native, c.surgery_date)) AS day_gap,
        CASE
            WHEN ABS(DATEDIFF('day', mol.test_date_native, c.surgery_date)) <= 30
                 THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', mol.test_date_native, c.surgery_date)) <= 180
                 THEN 'plausible'
            WHEN ABS(DATEDIFF('day', mol.test_date_native, c.surgery_date)) <= 365
                 THEN 'weak'
            ELSE 'unlinked'
        END AS assignment_confidence,
        mol.test_date_native >= c.window_start
            AND mol.test_date_native < c.window_end AS in_window
    FROM {S}molecular_test_episode_v2 mol
    JOIN multi_surgery_episode_cohort_v1 c ON mol.research_id = c.research_id
    WHERE mol.test_date_native IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY mol.research_id, mol.molecular_episode_id
        ORDER BY day_gap
    ) = 1
),

-- FNA episodes
fna_assignments AS (
    SELECT
        f.research_id,
        'fna'                                AS artifact_domain,
        'fna_cytology'                       AS artifact_subtype,
        CAST(f.fna_episode_id AS VARCHAR)    AS artifact_id,
        f.fna_date_native                    AS artifact_date,
        c.surgery_date,
        c.surgery_rank,
        c.laterality                         AS surgery_laterality,
        ABS(DATEDIFF('day', f.fna_date_native, c.surgery_date)) AS day_gap,
        CASE
            WHEN ABS(DATEDIFF('day', f.fna_date_native, c.surgery_date)) <= 14
                 THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', f.fna_date_native, c.surgery_date)) <= 90
                 THEN 'plausible'
            WHEN ABS(DATEDIFF('day', f.fna_date_native, c.surgery_date)) <= 365
                 THEN 'weak'
            ELSE 'unlinked'
        END AS assignment_confidence,
        f.fna_date_native >= c.window_start
            AND f.fna_date_native < c.window_end AS in_window
    FROM {S}fna_episode_master_v2 f
    JOIN multi_surgery_episode_cohort_v1 c ON f.research_id = c.research_id
    WHERE f.fna_date_native IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY f.research_id, f.fna_episode_id
        ORDER BY day_gap
    ) = 1
),

-- imaging
img_assignments AS (
    SELECT
        i.research_id,
        'imaging'                            AS artifact_domain,
        'us_nodule'                          AS artifact_subtype,
        CAST(i.nodule_id AS VARCHAR)         AS artifact_id,
        i.exam_date_native                   AS artifact_date,
        c.surgery_date,
        c.surgery_rank,
        c.laterality                         AS surgery_laterality,
        ABS(DATEDIFF('day', i.exam_date_native, c.surgery_date)) AS day_gap,
        CASE
            WHEN ABS(DATEDIFF('day', i.exam_date_native, c.surgery_date)) <= 14
                 THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', i.exam_date_native, c.surgery_date)) <= 90
                 THEN 'plausible'
            WHEN ABS(DATEDIFF('day', i.exam_date_native, c.surgery_date)) <= 365
                 THEN 'weak'
            ELSE 'unlinked'
        END AS assignment_confidence,
        i.exam_date_native >= c.window_start
            AND i.exam_date_native < c.window_end AS in_window
    FROM {S}imaging_nodule_long_v2 i
    JOIN multi_surgery_episode_cohort_v1 c ON i.research_id = c.research_id
    WHERE i.exam_date_native IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY i.research_id, i.nodule_id
        ORDER BY day_gap
    ) = 1
)

-- UNION ALL domains
SELECT research_id, artifact_domain, artifact_subtype, artifact_id,
       artifact_date, surgery_date, surgery_rank, surgery_laterality,
       day_gap, assignment_confidence, in_window,
       CURRENT_TIMESTAMP AS audit_ts
FROM note_assignments
UNION ALL
SELECT research_id, artifact_domain, artifact_subtype, artifact_id,
       artifact_date, surgery_date, surgery_rank, surgery_laterality,
       day_gap, assignment_confidence, in_window,
       CURRENT_TIMESTAMP AS audit_ts
FROM lab_assignments
UNION ALL
SELECT research_id, artifact_domain, artifact_subtype, artifact_id,
       artifact_date, surgery_date, surgery_rank, surgery_laterality,
       day_gap, assignment_confidence, in_window,
       CURRENT_TIMESTAMP AS audit_ts
FROM rai_assignments
UNION ALL
SELECT research_id, artifact_domain, artifact_subtype, artifact_id,
       artifact_date, surgery_date, surgery_rank, surgery_laterality,
       day_gap, assignment_confidence, in_window,
       CURRENT_TIMESTAMP AS audit_ts
FROM mol_assignments
UNION ALL
SELECT research_id, artifact_domain, artifact_subtype, artifact_id,
       artifact_date, surgery_date, surgery_rank, surgery_laterality,
       day_gap, assignment_confidence, in_window,
       CURRENT_TIMESTAMP AS audit_ts
FROM fna_assignments
UNION ALL
SELECT research_id, artifact_domain, artifact_subtype, artifact_id,
       artifact_date, surgery_date, surgery_rank, surgery_laterality,
       day_gap, assignment_confidence, in_window,
       CURRENT_TIMESTAMP AS audit_ts
FROM img_assignments
"""


# ── SQL: mislink detection ────────────────────────────────────────────────
def MISLINK_SQL(S: str) -> str:
    return f"""
CREATE OR REPLACE TABLE val_episode_mislink_candidates_v1 AS

WITH surg_path_check AS (
    SELECT
        sp.research_id,
        sp.surgery_episode_id    AS linked_surgery_id,
        sp.path_surgery_id       AS linked_path_id,
        sp.linkage_confidence_tier,
        sp.linkage_score,
        c.surgery_date           AS cohort_surg_date,
        c.surgery_rank           AS cohort_surg_rank,
        sp.surg_date             AS linkage_surg_date,
        ABS(DATEDIFF('day',
            COALESCE(TRY_CAST(sp.surg_date AS DATE), c.surgery_date),
            c.surgery_date
        )) AS date_discrepancy_days,
        CASE
            WHEN ABS(DATEDIFF('day',
                COALESCE(TRY_CAST(sp.surg_date AS DATE), c.surgery_date),
                c.surgery_date)) = 0 THEN 'correct'
            WHEN ABS(DATEDIFF('day',
                COALESCE(TRY_CAST(sp.surg_date AS DATE), c.surgery_date),
                c.surgery_date)) <= 3 THEN 'minor_mismatch'
            ELSE 'mislink_candidate'
        END AS verdict
    FROM {S}surgery_pathology_linkage_v3 sp
    JOIN multi_surgery_episode_cohort_v1 c ON sp.research_id = c.research_id
),

path_rai_check AS (
    SELECT
        pr.research_id,
        pr.surgery_episode_id,
        pr.rai_episode_id,
        pr.linkage_confidence_tier,
        pr.linkage_score,
        aa.surgery_rank AS audit_assigned_rank,
        pr.rai_date AS linkage_rai_date,
        aa.surgery_date  AS audit_surgery_date,
        ABS(DATEDIFF('day', TRY_CAST(pr.rai_date AS DATE), aa.surgery_date)) AS date_discrepancy_days,
        CASE
            WHEN pr.rai_date IS NULL THEN 'no_date'
            WHEN ABS(DATEDIFF('day', TRY_CAST(pr.rai_date AS DATE), aa.surgery_date)) <= 7 THEN 'correct'
            WHEN ABS(DATEDIFF('day', TRY_CAST(pr.rai_date AS DATE), aa.surgery_date)) <= 30 THEN 'minor_mismatch'
            ELSE 'mislink_candidate'
        END AS verdict
    FROM {S}pathology_rai_linkage_v3 pr
    JOIN multi_surgery_episode_cohort_v1 c ON pr.research_id = c.research_id
    LEFT JOIN val_episode_artifact_assignment_v1 aa
        ON pr.research_id = aa.research_id
        AND CAST(pr.rai_episode_id AS VARCHAR) = aa.artifact_id
        AND aa.artifact_domain = 'rai'
),

preop_check AS (
    SELECT
        ps.research_id,
        ps.preop_episode_id,
        ps.surgery_episode_id AS linked_surgery_id,
        ps.linkage_confidence_tier,
        ps.linkage_score,
        aa.surgery_rank AS audit_assigned_rank,
        CASE
            WHEN aa.surgery_rank IS NOT NULL
                 AND aa.surgery_rank != COALESCE(ps.surgery_episode_id, 0) THEN 'mislink_candidate'
            WHEN aa.surgery_rank IS NULL THEN 'unresolved'
            ELSE 'correct'
        END AS verdict
    FROM {S}preop_surgery_linkage_v3 ps
    JOIN multi_surgery_episode_cohort_v1 c ON ps.research_id = c.research_id
    LEFT JOIN val_episode_artifact_assignment_v1 aa
        ON ps.research_id = aa.research_id
        AND CAST(ps.preop_episode_id AS VARCHAR) = aa.artifact_id
        AND aa.artifact_domain = 'fna'
)

SELECT research_id, 'surgery_pathology' AS linkage_domain,
       linked_surgery_id AS source_episode_id,
       linked_path_id AS target_episode_id,
       linkage_confidence_tier, linkage_score,
       date_discrepancy_days, verdict,
       CURRENT_TIMESTAMP AS audit_ts
FROM surg_path_check
WHERE verdict != 'correct'

UNION ALL

SELECT research_id, 'pathology_rai' AS linkage_domain,
       CAST(surgery_episode_id AS VARCHAR) AS source_episode_id,
       CAST(rai_episode_id AS VARCHAR) AS target_episode_id,
       linkage_confidence_tier, linkage_score,
       date_discrepancy_days, verdict,
       CURRENT_TIMESTAMP AS audit_ts
FROM path_rai_check
WHERE verdict NOT IN ('correct', 'no_date')

UNION ALL

SELECT research_id, 'preop_surgery' AS linkage_domain,
       CAST(preop_episode_id AS VARCHAR) AS source_episode_id,
       CAST(linked_surgery_id AS VARCHAR) AS target_episode_id,
       linkage_confidence_tier, linkage_score,
       NULL AS date_discrepancy_days, verdict,
       CURRENT_TIMESTAMP AS audit_ts
FROM preop_check
WHERE verdict != 'correct'
"""


# ── SQL: per-patient integrity grade ──────────────────────────────────────
def INTEGRITY_SQL(S: str) -> str:
    return """
CREATE OR REPLACE TABLE val_episode_linkage_integrity_v1 AS
WITH per_patient AS (
    SELECT
        c.research_id,
        c.total_surgeries,
        COUNT(DISTINCT aa.artifact_id) AS total_artifacts,
        SUM(CASE WHEN aa.assignment_confidence = 'exact_match'     THEN 1 ELSE 0 END) AS exact_ct,
        SUM(CASE WHEN aa.assignment_confidence = 'high_confidence' THEN 1 ELSE 0 END) AS high_ct,
        SUM(CASE WHEN aa.assignment_confidence = 'plausible'       THEN 1 ELSE 0 END) AS plaus_ct,
        SUM(CASE WHEN aa.assignment_confidence = 'weak'            THEN 1 ELSE 0 END) AS weak_ct,
        SUM(CASE WHEN aa.assignment_confidence = 'unlinked'        THEN 1 ELSE 0 END) AS unlinked_ct,
        SUM(CASE WHEN aa.in_window THEN 1 ELSE 0 END) AS in_window_ct,
        COUNT(DISTINCT ml.research_id) FILTER (WHERE ml.verdict = 'mislink_candidate') AS mislink_ct
    FROM multi_surgery_episode_cohort_v1 c
    LEFT JOIN val_episode_artifact_assignment_v1 aa ON c.research_id = aa.research_id
    LEFT JOIN val_episode_mislink_candidates_v1 ml ON c.research_id = ml.research_id
    GROUP BY c.research_id, c.total_surgeries
)
SELECT
    research_id,
    total_surgeries,
    total_artifacts,
    exact_ct, high_ct, plaus_ct, weak_ct, unlinked_ct,
    in_window_ct,
    mislink_ct,
    ROUND(
        CASE WHEN total_artifacts > 0
             THEN (exact_ct + high_ct + plaus_ct) * 100.0 / total_artifacts
             ELSE 0
        END, 1
    ) AS pct_confident,
    CASE
        WHEN mislink_ct > 0 THEN 'REVIEW_REQUIRED'
        WHEN total_artifacts = 0 THEN 'NO_ARTIFACTS'
        WHEN (exact_ct + high_ct) * 1.0 / GREATEST(total_artifacts, 1) >= 0.8 THEN 'GREEN'
        WHEN (exact_ct + high_ct + plaus_ct) * 1.0 / GREATEST(total_artifacts, 1) >= 0.6 THEN 'YELLOW'
        ELSE 'RED'
    END AS integrity_grade,
    CURRENT_TIMESTAMP AS audit_ts
FROM per_patient
ORDER BY
    CASE
        WHEN mislink_ct > 0 THEN 0
        WHEN (exact_ct + high_ct) * 1.0 / GREATEST(total_artifacts, 1) < 0.5 THEN 1
        ELSE 2
    END,
    mislink_ct DESC, total_artifacts DESC
"""


# ── SQL: key propagation audit ────────────────────────────────────────────
def KEY_PROPAGATION_SQL(S: str) -> str:
    return f"""
CREATE OR REPLACE TABLE val_episode_key_propagation_v1 AS
WITH tables_with_seid AS (
    SELECT 'operative_episode_detail_v2' AS table_name,
           research_id, surgery_episode_id,
           COUNT(*) AS n_rows
    FROM {S}operative_episode_detail_v2
    WHERE research_id IN (SELECT DISTINCT research_id FROM multi_surgery_episode_cohort_v1)
    GROUP BY 1, 2, 3

    UNION ALL

    SELECT 'episode_analysis_resolved_v1_dedup' AS table_name,
           research_id, surgery_episode_id,
           COUNT(*) AS n_rows
    FROM {S}episode_analysis_resolved_v1_dedup
    WHERE research_id IN (SELECT DISTINCT research_id FROM multi_surgery_episode_cohort_v1)
    GROUP BY 1, 2, 3

    UNION ALL

    SELECT 'tumor_episode_master_v2' AS table_name,
           research_id, surgery_episode_id,
           COUNT(*) AS n_rows
    FROM {S}tumor_episode_master_v2
    WHERE research_id IN (SELECT DISTINCT research_id FROM multi_surgery_episode_cohort_v1)
    GROUP BY 1, 2, 3
),
cohort_expected AS (
    SELECT research_id, surgery_rank, total_surgeries
    FROM multi_surgery_episode_cohort_v1
)
SELECT
    t.table_name,
    COUNT(DISTINCT t.research_id)  AS patients_present,
    (SELECT COUNT(DISTINCT research_id) FROM multi_surgery_episode_cohort_v1) AS patients_expected,
    SUM(t.n_rows)                  AS total_rows,
    COUNT(DISTINCT t.surgery_episode_id) AS distinct_episode_ids,
    SUM(CASE WHEN t.surgery_episode_id IS NULL THEN t.n_rows ELSE 0 END)   AS null_episode_id_rows,
    SUM(CASE WHEN t.surgery_episode_id = 1 AND ce.total_surgeries > 1
             THEN t.n_rows ELSE 0 END) AS all_ones_multi_surg,
    CURRENT_TIMESTAMP AS audit_ts
FROM tables_with_seid t
LEFT JOIN cohort_expected ce ON t.research_id = ce.research_id
    AND t.surgery_episode_id = ce.surgery_rank
GROUP BY 1
ORDER BY 1
"""


# ── SQL: ambiguity review ────────────────────────────────────────────────
def AMBIGUITY_SQL(S: str) -> str:
    return f"""
CREATE OR REPLACE TABLE val_episode_ambiguity_review_v1 AS
WITH all_candidates AS (
    SELECT
        cn.research_id,
        'clinical_note' AS artifact_domain,
        cn.note_type    AS artifact_subtype,
        cn.note_row_id  AS artifact_id,
        TRY_CAST(cn.note_date AS DATE) AS artifact_date,
        c.surgery_date,
        c.surgery_rank,
        ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), c.surgery_date)) AS day_gap
    FROM {S}clinical_notes_long cn
    JOIN multi_surgery_episode_cohort_v1 c
        ON TRY_CAST(cn.research_id AS INTEGER) = c.research_id
    WHERE cn.note_date IS NOT NULL
       AND TRIM(CAST(cn.note_date AS VARCHAR)) != ''

    UNION ALL

    SELECT
        TRY_CAST(l.research_id AS INTEGER) AS research_id,
        'lab' AS artifact_domain,
        l.analyte_group AS artifact_subtype,
        l.research_id || '_' || COALESCE(CAST(l.lab_date AS VARCHAR),'unk')
            || '_' || l.analyte_group AS artifact_id,
        TRY_CAST(l.lab_date AS DATE) AS artifact_date,
        c.surgery_date,
        c.surgery_rank,
        ABS(DATEDIFF('day', TRY_CAST(l.lab_date AS DATE), c.surgery_date)) AS day_gap
    FROM {S}longitudinal_lab_canonical_v1 l
    JOIN multi_surgery_episode_cohort_v1 c
        ON TRY_CAST(l.research_id AS INTEGER) = c.research_id
    WHERE l.lab_date IS NOT NULL
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY research_id, artifact_id ORDER BY day_gap) AS rn,
           COUNT(*) OVER (PARTITION BY research_id, artifact_id) AS n_candidates
    FROM all_candidates
),
ambiguous AS (
    SELECT a.*, b.surgery_date AS alt_surgery_date, b.surgery_rank AS alt_surgery_rank,
           b.day_gap AS alt_day_gap,
           ABS(a.day_gap - b.day_gap) AS gap_difference
    FROM ranked a
    JOIN ranked b ON a.research_id = b.research_id
        AND a.artifact_id = b.artifact_id
        AND a.rn = 1 AND b.rn = 2
    WHERE ABS(a.day_gap - b.day_gap) <= 14  -- nearly equidistant
)
SELECT
    research_id, artifact_domain, artifact_subtype, artifact_id,
    artifact_date,
    surgery_date AS primary_surgery_date,
    surgery_rank AS primary_surgery_rank,
    day_gap AS primary_day_gap,
    alt_surgery_date,
    alt_surgery_rank,
    alt_day_gap,
    gap_difference,
    'ambiguous_requires_review' AS verdict,
    CURRENT_TIMESTAMP AS audit_ts
FROM ambiguous
ORDER BY gap_difference, research_id
"""


# ── SQL: aggregate summary ────────────────────────────────────────────────
def SUMMARY_SQL(S: str) -> str:
    return """
CREATE OR REPLACE TABLE val_episode_linkage_summary_v1 AS
SELECT
    'multi_surgery_patients'    AS metric,
    (SELECT COUNT(DISTINCT research_id) FROM multi_surgery_episode_cohort_v1)::VARCHAR AS value
UNION ALL SELECT 'total_surgery_episodes',
    (SELECT COUNT(*) FROM multi_surgery_episode_cohort_v1)::VARCHAR
UNION ALL SELECT 'total_artifacts_assigned',
    (SELECT COUNT(*) FROM val_episode_artifact_assignment_v1)::VARCHAR
UNION ALL SELECT 'pct_exact_or_high_confidence',
    (SELECT ROUND(SUM(CASE WHEN assignment_confidence IN ('exact_match','high_confidence')
        THEN 1 ELSE 0 END) * 100.0 / GREATEST(COUNT(*),1), 1)
     FROM val_episode_artifact_assignment_v1)::VARCHAR
UNION ALL SELECT 'pct_in_window',
    (SELECT ROUND(SUM(CASE WHEN in_window THEN 1 ELSE 0 END) * 100.0
        / GREATEST(COUNT(*),1), 1) FROM val_episode_artifact_assignment_v1)::VARCHAR
UNION ALL SELECT 'mislink_candidates',
    (SELECT COUNT(*) FROM val_episode_mislink_candidates_v1
     WHERE verdict = 'mislink_candidate')::VARCHAR
UNION ALL SELECT 'ambiguous_artifacts',
    (SELECT COUNT(*) FROM val_episode_ambiguity_review_v1)::VARCHAR
UNION ALL SELECT 'patients_green',
    (SELECT COUNT(*) FROM val_episode_linkage_integrity_v1
     WHERE integrity_grade = 'GREEN')::VARCHAR
UNION ALL SELECT 'patients_yellow',
    (SELECT COUNT(*) FROM val_episode_linkage_integrity_v1
     WHERE integrity_grade = 'YELLOW')::VARCHAR
UNION ALL SELECT 'patients_red',
    (SELECT COUNT(*) FROM val_episode_linkage_integrity_v1
     WHERE integrity_grade = 'RED')::VARCHAR
UNION ALL SELECT 'patients_review_required',
    (SELECT COUNT(*) FROM val_episode_linkage_integrity_v1
     WHERE integrity_grade = 'REVIEW_REQUIRED')::VARCHAR
UNION ALL SELECT 'patients_no_artifacts',
    (SELECT COUNT(*) FROM val_episode_linkage_integrity_v1
     WHERE integrity_grade = 'NO_ARTIFACTS')::VARCHAR
UNION ALL SELECT 'audit_timestamp',
    CAST(CURRENT_TIMESTAMP AS VARCHAR)
"""


# ── main orchestration ────────────────────────────────────────────────────
TABLE_NAMES = [
    "multi_surgery_episode_cohort_v1",
    "val_episode_artifact_assignment_v1",
    "val_episode_mislink_candidates_v1",
    "val_episode_linkage_integrity_v1",
    "val_episode_key_propagation_v1",
    "val_episode_ambiguity_review_v1",
    "val_episode_linkage_summary_v1",
]


def run_audit(con: duckdb.DuckDBPyConnection, env: str, dry_run: bool) -> dict:
    """Execute the full audit pipeline and return summary dict."""
    results = {}
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")

    S = src_prefix(env)
    steps = [
        ("multi_surgery_episode_cohort_v1", COHORT_SQL(S)),
        ("val_episode_artifact_assignment_v1", ARTIFACT_ASSIGNMENT_SQL(S)),
        ("val_episode_mislink_candidates_v1", MISLINK_SQL(S)),
        ("val_episode_linkage_integrity_v1", INTEGRITY_SQL(S)),
        ("val_episode_key_propagation_v1", KEY_PROPAGATION_SQL(S)),
        ("val_episode_ambiguity_review_v1", AMBIGUITY_SQL(S)),
        ("val_episode_linkage_summary_v1", SUMMARY_SQL(S)),
    ]

    for name, sql in steps:
        section(f"{'[DRY-RUN] ' if dry_run else ''}Creating {name}")
        if dry_run:
            print(f"  Would execute SQL for {name} ({len(sql)} chars)")
            results[name] = {"status": "dry_run", "rows": 0}
            continue

        t0 = time.time()
        try:
            con.execute(sql)
            n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            elapsed = time.time() - t0
            print(f"  ✓ {name}: {n:,} rows  ({elapsed:.1f}s)")
            results[name] = {"status": "ok", "rows": n, "elapsed_s": round(elapsed, 1)}
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  ✗ {name}: {e}")
            results[name] = {"status": "error", "error": str(e), "elapsed_s": round(elapsed, 1)}

    # Run ANALYZE on all new tables
    if not dry_run:
        section("Running ANALYZE on audit tables")
        for name, _ in steps:
            if results.get(name, {}).get("status") == "ok":
                try:
                    con.execute(f"ANALYZE {name}")
                    print(f"  ✓ ANALYZE {name}")
                except Exception as e:
                    print(f"  ⚠ ANALYZE {name}: {e}")

    # Fetch summary KPIs
    if not dry_run:
        section("Summary KPIs")
        try:
            kpis = con.execute(
                "SELECT metric, value FROM val_episode_linkage_summary_v1"
            ).fetchall()
            for metric, value in kpis:
                print(f"  {metric:40s} {value}")
                results[f"kpi_{metric}"] = value
        except Exception as e:
            print(f"  ⚠ Could not read summary: {e}")

    return results


def export_csvs(con: duckdb.DuckDBPyConnection, export_dir: Path) -> list[str]:
    """Export all audit tables to CSV."""
    export_dir.mkdir(parents=True, exist_ok=True)
    exported = []
    for tbl in TABLE_NAMES:
        if tbl_exists(con, tbl):
            out = export_dir / f"{tbl}.csv"
            try:
                df = con.execute(f"SELECT * FROM {tbl}").fetchdf()
                df.to_csv(out, index=False)
                exported.append(str(out))
                print(f"  ✓ Exported {tbl} → {out.name}  ({len(df)} rows)")
            except Exception as e:
                print(f"  ⚠ Export {tbl}: {e}")
    # Write manifest
    manifest = {
        "generated": datetime.datetime.now().isoformat(),
        "tables": TABLE_NAMES,
        "files": [str(p) for p in exported],
    }
    mf = export_dir / "manifest.json"
    mf.write_text(json.dumps(manifest, indent=2))
    exported.append(str(mf))
    return exported


def main():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", default="dev",
                        choices=["dev", "qa", "prod"],
                        help="Target MotherDuck environment")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print SQL plan without executing")
    parser.add_argument("--export", action="store_true",
                        help="Export audit tables to CSV after creation")
    args = parser.parse_args()

    section(f"Episode Linkage Integrity Audit — env={args.env}")
    print(f"  Dry-run: {args.dry_run}")
    print(f"  Target DB: {resolve_db(args.env)}")

    con = connect(args.env)
    results = run_audit(con, args.env, args.dry_run)

    if args.export and not args.dry_run:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        export_dir = ROOT / "exports" / f"episode_linkage_audit_{ts}"
        section("Exporting CSVs")
        export_csvs(con, export_dir)

    con.close()

    # Print final status
    section("FINAL STATUS")
    ok = sum(1 for v in results.values() if isinstance(v, dict) and v.get("status") == "ok")
    err = sum(1 for v in results.values() if isinstance(v, dict) and v.get("status") == "error")
    print(f"  Tables created OK: {ok}")
    print(f"  Tables errored:    {err}")

    if err > 0:
        print("\n  ⚠ Some tables failed — check output above")
        sys.exit(1)
    else:
        print("\n  ✓ All audit tables created successfully")
        sys.exit(0)


if __name__ == "__main__":
    main()
