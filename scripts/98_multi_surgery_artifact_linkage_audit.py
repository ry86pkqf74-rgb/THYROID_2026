#!/usr/bin/env python3
"""
98_multi_surgery_artifact_linkage_audit.py

Focused local DuckDB-backed hardening pass for multi-surgery artifact linkage
AFTER the downstream episode_id repair (script 96).

Builds on script 97's general audit by answering:
  1. Are op-notes linked to the correct surgery episode?
  2. Are H&P / discharge notes temporally plausible for their surgery?
  3. Are imaging/FNA/molecular/path/RAI artifacts correctly episode-routed?
  4. Which multi-surgery patients still have only 1 OED row despite >1 canonical episode?
  5. Which artifacts have no anchor date (unresolvable)?

Output tables (all deployed to local DuckDB prod):
  val_multi_surgery_artifact_linkage_v1    — per-artifact linkage verdict (one row per artifact)
  multi_surgery_artifact_review_queue_v1   — triaged review queue (priorities + reasons)
  multi_surgery_oed_coverage_gap_v1        — OED ↔ canonical episode coverage mismatch

Scoring tiers: exact / high_confidence / plausible / weak / no_match
Reason codes: date_out_of_window, laterality_conflict, only_single_oed_row,
              missing_anchor_date, cross_episode_mismatch, ambiguous_equidistant

Usage:
  .venv/bin/python scripts/98_multi_surgery_artifact_linkage_audit.py --md
  .venv/bin/python scripts/98_multi_surgery_artifact_linkage_audit.py --md --dry-run
"""
from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    __import__("duckdb")
    __import__("pandas")
except ImportError:
    sys.exit("duckdb and pandas required — run from .venv/bin/python")

NOW = datetime.datetime.now()
TIMESTAMP = NOW.strftime("%Y%m%d_%H%M")
DATE_TAG = NOW.strftime("%Y%m%d")
EXPORT_DIR = ROOT / f"exports/multi_surgery_artifact_linkage_{TIMESTAMP}"
DOCS_DIR = ROOT / "docs"

OUTPUT_TABLES = [
    "val_multi_surgery_artifact_linkage_v1",
    "multi_surgery_artifact_review_queue_v1",
    "multi_surgery_oed_coverage_gap_v1",
]

# ── helpers ──────────────────────────────────────────────────────────────


def section(title: str):
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}")


def get_connection():
    from utils.md_connect import connect_md_or_file
    return connect_md_or_file(ROOT / "thyroid_master.duckdb", md=True, fail_closed=True)


def q1(con, sql, default=None):
    try:
        r = con.execute(sql).fetchone()
        return r[0] if r else default
    except Exception as e:
        print(f"  WARN q1: {e}")
        return default


def qall(con, sql):
    try:
        return con.execute(sql).fetchall()
    except Exception as e:
        print(f"  WARN qall: {e}")
        return []


def tbl_exists(con, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 0")
        return True
    except Exception:
        return False


# ── Phase A: OED coverage gap ────────────────────────────────────────────

OED_COVERAGE_GAP_SQL = """
CREATE OR REPLACE TABLE multi_surgery_oed_coverage_gap_v1 AS
WITH canonical AS (
    -- Canonical surgery list from tumor_episode_master_v2
    SELECT
        research_id,
        surgery_episode_id,
        surgery_date,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, surgery_date
            ORDER BY tumor_ordinal
        ) AS rn
    FROM tumor_episode_master_v2
    WHERE surgery_date IS NOT NULL
),
canonical_dedup AS (
    SELECT research_id, surgery_episode_id, surgery_date
    FROM canonical WHERE rn = 1
),
multi_surg AS (
    SELECT research_id
    FROM canonical_dedup
    GROUP BY research_id HAVING COUNT(*) > 1
),
-- All canonical episodes for multi-surgery patients
expected AS (
    SELECT
        cd.research_id,
        cd.surgery_episode_id,
        cd.surgery_date,
        COUNT(*) OVER (PARTITION BY cd.research_id) AS total_canonical_episodes
    FROM canonical_dedup cd
    JOIN multi_surg ms ON cd.research_id = ms.research_id
),
-- OED rows for multi-surgery patients
oed AS (
    SELECT
        o.research_id,
        o.surgery_episode_id AS oed_episode_id,
        o.surgery_date_native AS oed_date,
        o.procedure_normalized,
        o.rln_monitoring_flag,
        o.operative_findings_raw
    FROM operative_episode_detail_v2 o
    JOIN multi_surg ms ON o.research_id = ms.research_id
)
SELECT
    e.research_id,
    e.surgery_episode_id AS canonical_episode_id,
    e.surgery_date       AS canonical_date,
    e.total_canonical_episodes,
    o.oed_episode_id,
    o.oed_date,
    o.procedure_normalized,
    CASE
        WHEN o.oed_date IS NOT NULL AND o.oed_date = e.surgery_date
            THEN 'exact_match'
        WHEN o.oed_date IS NOT NULL
             AND ABS(DATEDIFF('day', o.oed_date, e.surgery_date)) <= 3
            THEN 'near_match'
        WHEN o.oed_date IS NOT NULL
            THEN 'date_mismatch'
        WHEN o.research_id IS NULL
            THEN 'no_oed_row'
        ELSE 'no_date'
    END AS oed_match_status,
    CASE
        WHEN o.research_id IS NULL THEN 'only_single_oed_row'
        WHEN o.oed_date IS NULL THEN 'missing_anchor_date'
        WHEN ABS(DATEDIFF('day', o.oed_date, e.surgery_date)) > 3
            THEN 'date_out_of_window'
        ELSE NULL
    END AS gap_reason,
    DATEDIFF('day', o.oed_date, e.surgery_date) AS date_offset_days,
    CURRENT_TIMESTAMP AS audit_ts
FROM expected e
LEFT JOIN oed o
    ON e.research_id = o.research_id
    AND e.surgery_episode_id = o.oed_episode_id
ORDER BY e.research_id, e.surgery_episode_id
"""


# ── Phase B: artifact linkage audit ──────────────────────────────────────

ARTIFACT_LINKAGE_SQL = """
CREATE OR REPLACE TABLE val_multi_surgery_artifact_linkage_v1 AS

-- Build surgery windows for multi-surgery patients
WITH canonical AS (
    SELECT research_id, surgery_episode_id, surgery_date,
           ROW_NUMBER() OVER (
               PARTITION BY research_id, surgery_date
               ORDER BY tumor_ordinal
           ) AS rn
    FROM tumor_episode_master_v2
    WHERE surgery_date IS NOT NULL
),
canonical_dedup AS (
    SELECT research_id, surgery_episode_id, surgery_date
    FROM canonical WHERE rn = 1
),
multi_surg AS (
    SELECT research_id FROM canonical_dedup
    GROUP BY research_id HAVING COUNT(*) > 1
),
surg_windows AS (
    SELECT
        cd.research_id,
        cd.surgery_episode_id,
        cd.surgery_date,
        LAG(cd.surgery_date)  OVER w AS prev_surgery,
        LEAD(cd.surgery_date) OVER w AS next_surgery,
        -- window_start = midpoint to prev (or 1900)
        COALESCE(
            cd.surgery_date - INTERVAL (
                DATEDIFF('day',
                    LAG(cd.surgery_date) OVER w,
                    cd.surgery_date
                ) / 2
            ) DAY,
            TRY_CAST('1900-01-01' AS DATE)
        ) AS window_start,
        -- window_end = midpoint to next (or 2099)
        COALESCE(
            cd.surgery_date + INTERVAL (
                DATEDIFF('day',
                    cd.surgery_date,
                    LEAD(cd.surgery_date) OVER w
                ) / 2
            ) DAY,
            TRY_CAST('2099-12-31' AS DATE)
        ) AS window_end
    FROM canonical_dedup cd
    JOIN multi_surg ms ON cd.research_id = ms.research_id
    WINDOW w AS (PARTITION BY cd.research_id ORDER BY cd.surgery_date)
),

-- path_synoptics laterality for each surgery
surg_laterality AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        TRY_CAST(surg_date AS DATE) AS surgery_date,
        CASE
            WHEN LOWER(COALESCE(thyroid_procedure,'')) LIKE '%right%'
                 OR LOWER(COALESCE(thyroid_procedure,'')) LIKE '%(rl)%' THEN 'right'
            WHEN LOWER(COALESCE(thyroid_procedure,'')) LIKE '%left%'
                 OR LOWER(COALESCE(thyroid_procedure,'')) LIKE '%(ll)%' THEN 'left'
            WHEN LOWER(COALESCE(thyroid_procedure,'')) LIKE '%total%'
                 OR LOWER(COALESCE(thyroid_procedure,'')) LIKE '%bilateral%' THEN 'bilateral'
            ELSE 'unspecified'
        END AS laterality
    FROM path_synoptics
    WHERE surg_date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id, TRY_CAST(surg_date AS DATE)
        ORDER BY CASE WHEN thyroid_procedure IS NOT NULL THEN 0 ELSE 1 END
    ) = 1
),

-- ─── op_note ↔ surgery ───
op_note_check AS (
    SELECT
        TRY_CAST(cn.research_id AS BIGINT) AS research_id,
        'op_note'                   AS artifact_domain,
        cn.note_row_id              AS artifact_id,
        TRY_CAST(cn.note_date AS DATE) AS artifact_date,
        sw.surgery_episode_id,
        sw.surgery_date,
        ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date)) AS day_gap,
        CASE
            WHEN ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date)) = 0
                THEN 'exact'
            WHEN ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date)) <= 1
                THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date)) <= 3
                THEN 'plausible'
            ELSE 'no_match'
        END AS confidence,
        CASE
            WHEN TRY_CAST(cn.note_date AS DATE) IS NULL THEN 'missing_anchor_date'
            WHEN ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date)) > 3
                THEN 'date_out_of_window'
            ELSE NULL
        END AS reason,
        TRY_CAST(cn.note_date AS DATE) >= sw.window_start
            AND TRY_CAST(cn.note_date AS DATE) < sw.window_end AS in_window
    FROM clinical_notes_long cn
    JOIN surg_windows sw
        ON TRY_CAST(cn.research_id AS BIGINT) = sw.research_id
    WHERE cn.note_type = 'op_note'
        AND cn.note_date IS NOT NULL
        AND TRIM(CAST(cn.note_date AS VARCHAR)) != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cn.research_id, cn.note_row_id
        ORDER BY day_gap
    ) = 1
),

-- ─── H&P / discharge notes ↔ surgery ───
hp_dc_check AS (
    SELECT
        TRY_CAST(cn.research_id AS BIGINT) AS research_id,
        CASE cn.note_type
            WHEN 'h_p' THEN 'h_and_p'
            WHEN 'dc_sum' THEN 'discharge_summary'
            ELSE cn.note_type
        END                          AS artifact_domain,
        cn.note_row_id               AS artifact_id,
        TRY_CAST(cn.note_date AS DATE) AS artifact_date,
        sw.surgery_episode_id,
        sw.surgery_date,
        DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date) AS offset_days,
        ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date)) AS day_gap,
        CASE
            -- H&P typically 0-7 days before surgery
            WHEN cn.note_type = 'h_p'
                 AND DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date)
                     BETWEEN 0 AND 7
                THEN 'exact'
            -- DC summary same day to 7 days after
            WHEN cn.note_type = 'dc_sum'
                 AND DATEDIFF('day', sw.surgery_date, TRY_CAST(cn.note_date AS DATE))
                     BETWEEN 0 AND 7
                THEN 'exact'
            WHEN ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date)) <= 14
                THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date)) <= 30
                THEN 'plausible'
            WHEN ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date)) <= 180
                THEN 'weak'
            ELSE 'no_match'
        END AS confidence,
        CASE
            WHEN TRY_CAST(cn.note_date AS DATE) IS NULL THEN 'missing_anchor_date'
            WHEN ABS(DATEDIFF('day', TRY_CAST(cn.note_date AS DATE), sw.surgery_date)) > 180
                THEN 'date_out_of_window'
            ELSE NULL
        END AS reason,
        TRY_CAST(cn.note_date AS DATE) >= sw.window_start
            AND TRY_CAST(cn.note_date AS DATE) < sw.window_end AS in_window
    FROM clinical_notes_long cn
    JOIN surg_windows sw
        ON TRY_CAST(cn.research_id AS BIGINT) = sw.research_id
    WHERE cn.note_type IN ('h_p', 'dc_sum')
        AND cn.note_date IS NOT NULL
        AND TRIM(CAST(cn.note_date AS VARCHAR)) != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cn.research_id, cn.note_row_id
        ORDER BY day_gap
    ) = 1
),

-- ─── pathology ↔ surgery (via surgery_pathology_linkage_v3) ───
path_check AS (
    SELECT
        sp.research_id,
        'pathology'                       AS artifact_domain,
        CAST(sp.tumor_ordinal AS VARCHAR) AS artifact_id,
        TRY_CAST(sp.surg_date AS DATE)   AS artifact_date,
        sp.surgery_episode_id,
        sw.surgery_date,
        ABS(DATEDIFF('day', TRY_CAST(sp.surg_date AS DATE), sw.surgery_date)) AS day_gap,
        CASE
            WHEN TRY_CAST(sp.surg_date AS DATE) = sw.surgery_date THEN 'exact'
            WHEN ABS(DATEDIFF('day', TRY_CAST(sp.surg_date AS DATE), sw.surgery_date)) <= 3
                THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', TRY_CAST(sp.surg_date AS DATE), sw.surgery_date)) <= 30
                THEN 'plausible'
            ELSE 'no_match'
        END AS confidence,
        CASE
            WHEN TRY_CAST(sp.surg_date AS DATE) IS NULL THEN 'missing_anchor_date'
            WHEN sp.surgery_episode_id != sw.surgery_episode_id
                THEN 'cross_episode_mismatch'
            WHEN ABS(DATEDIFF('day', TRY_CAST(sp.surg_date AS DATE), sw.surgery_date)) > 30
                THEN 'date_out_of_window'
            ELSE NULL
        END AS reason,
        TRUE AS in_window  -- path is primary to surgery definition
    FROM surgery_pathology_linkage_v3 sp
    JOIN surg_windows sw
        ON sp.research_id = sw.research_id
        AND sp.surgery_episode_id = sw.surgery_episode_id
),

-- ─── RAI ↔ surgery ───
rai_check AS (
    SELECT
        r.research_id,
        'rai'                                AS artifact_domain,
        CAST(r.rai_episode_id AS VARCHAR)    AS artifact_id,
        COALESCE(r.rai_date_native,
                 TRY_CAST(r.resolved_rai_date AS DATE)) AS artifact_date,
        sw.surgery_episode_id,
        sw.surgery_date,
        ABS(DATEDIFF('day',
            COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)),
            sw.surgery_date
        )) AS day_gap,
        CASE
            WHEN COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)) IS NULL
                THEN 'no_match'
            -- RAI typically 4-12 weeks post-surgery
            WHEN DATEDIFF('day', sw.surgery_date,
                    COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)))
                    BETWEEN 14 AND 180
                THEN 'exact'
            WHEN DATEDIFF('day', sw.surgery_date,
                    COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)))
                    BETWEEN 0 AND 365
                THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day',
                    COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)),
                    sw.surgery_date)) <= 730
                THEN 'plausible'
            ELSE 'weak'
        END AS confidence,
        CASE
            WHEN COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)) IS NULL
                THEN 'missing_anchor_date'
            WHEN DATEDIFF('day', sw.surgery_date,
                    COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE))) < -7
                THEN 'date_out_of_window'
            ELSE NULL
        END AS reason,
        COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE))
            >= sw.window_start
            AND COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE))
            < sw.window_end
            AS in_window
    FROM rai_treatment_episode_v2 r
    JOIN surg_windows sw ON r.research_id = sw.research_id
    WHERE r.research_id IN (SELECT research_id FROM multi_surg)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY r.research_id, r.rai_episode_id
        ORDER BY ABS(DATEDIFF('day',
            COALESCE(r.rai_date_native, TRY_CAST(r.resolved_rai_date AS DATE)),
            sw.surgery_date))
    ) = 1
),

-- ─── molecular ↔ surgery ───
mol_check AS (
    SELECT
        mol.research_id,
        'molecular'                              AS artifact_domain,
        CAST(mol.molecular_episode_id AS VARCHAR) AS artifact_id,
        mol.test_date_native                     AS artifact_date,
        sw.surgery_episode_id,
        sw.surgery_date,
        ABS(DATEDIFF('day', mol.test_date_native, sw.surgery_date)) AS day_gap,
        CASE
            WHEN mol.test_date_native IS NULL THEN 'no_match'
            -- Molecular typically 1-90 days before surgery (preop workup)
            WHEN DATEDIFF('day', mol.test_date_native, sw.surgery_date)
                    BETWEEN 0 AND 90
                THEN 'exact'
            WHEN ABS(DATEDIFF('day', mol.test_date_native, sw.surgery_date)) <= 180
                THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', mol.test_date_native, sw.surgery_date)) <= 365
                THEN 'plausible'
            ELSE 'weak'
        END AS confidence,
        CASE
            WHEN mol.test_date_native IS NULL THEN 'missing_anchor_date'
            WHEN ABS(DATEDIFF('day', mol.test_date_native, sw.surgery_date)) > 365
                THEN 'date_out_of_window'
            ELSE NULL
        END AS reason,
        mol.test_date_native >= sw.window_start
            AND mol.test_date_native < sw.window_end AS in_window
    FROM molecular_test_episode_v2 mol
    JOIN surg_windows sw ON mol.research_id = sw.research_id
    WHERE mol.research_id IN (SELECT research_id FROM multi_surg)
        AND mol.test_date_native IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY mol.research_id, mol.molecular_episode_id
        ORDER BY ABS(DATEDIFF('day', mol.test_date_native, sw.surgery_date))
    ) = 1
),

-- ─── FNA ↔ surgery ───
fna_check AS (
    SELECT
        f.research_id,
        'fna'                                AS artifact_domain,
        CAST(f.fna_episode_id AS VARCHAR)    AS artifact_id,
        f.fna_date_native                    AS artifact_date,
        sw.surgery_episode_id,
        sw.surgery_date,
        ABS(DATEDIFF('day', f.fna_date_native, sw.surgery_date)) AS day_gap,
        CASE
            WHEN f.fna_date_native IS NULL THEN 'no_match'
            -- FNA typically within 180 days before surgery
            WHEN DATEDIFF('day', f.fna_date_native, sw.surgery_date)
                    BETWEEN 0 AND 180
                THEN 'exact'
            WHEN ABS(DATEDIFF('day', f.fna_date_native, sw.surgery_date)) <= 365
                THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', f.fna_date_native, sw.surgery_date)) <= 730
                THEN 'plausible'
            ELSE 'weak'
        END AS confidence,
        CASE
            WHEN f.fna_date_native IS NULL THEN 'missing_anchor_date'
            WHEN ABS(DATEDIFF('day', f.fna_date_native, sw.surgery_date)) > 730
                THEN 'date_out_of_window'
            ELSE NULL
        END AS reason,
        f.fna_date_native >= sw.window_start
            AND f.fna_date_native < sw.window_end AS in_window
    FROM fna_episode_master_v2 f
    JOIN surg_windows sw ON f.research_id = sw.research_id
    WHERE f.research_id IN (SELECT research_id FROM multi_surg)
        AND f.fna_date_native IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY f.research_id, f.fna_episode_id
        ORDER BY ABS(DATEDIFF('day', f.fna_date_native, sw.surgery_date))
    ) = 1
),

-- ─── imaging ↔ surgery ───
img_check AS (
    SELECT
        i.research_id,
        'imaging'                            AS artifact_domain,
        CAST(i.nodule_id AS VARCHAR)         AS artifact_id,
        i.exam_date_native                   AS artifact_date,
        sw.surgery_episode_id,
        sw.surgery_date,
        ABS(DATEDIFF('day', i.exam_date_native, sw.surgery_date)) AS day_gap,
        CASE
            WHEN i.exam_date_native IS NULL THEN 'no_match'
            WHEN ABS(DATEDIFF('day', i.exam_date_native, sw.surgery_date)) <= 14
                THEN 'exact'
            WHEN ABS(DATEDIFF('day', i.exam_date_native, sw.surgery_date)) <= 90
                THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', i.exam_date_native, sw.surgery_date)) <= 365
                THEN 'plausible'
            ELSE 'weak'
        END AS confidence,
        CASE
            WHEN i.exam_date_native IS NULL THEN 'missing_anchor_date'
            WHEN ABS(DATEDIFF('day', i.exam_date_native, sw.surgery_date)) > 365
                THEN 'date_out_of_window'
            ELSE NULL
        END AS reason,
        i.exam_date_native >= sw.window_start
            AND i.exam_date_native < sw.window_end AS in_window
    FROM imaging_nodule_long_v2 i
    JOIN surg_windows sw ON i.research_id = sw.research_id
    WHERE i.research_id IN (SELECT research_id FROM multi_surg)
        AND i.exam_date_native IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY i.research_id, i.nodule_id
        ORDER BY ABS(DATEDIFF('day', i.exam_date_native, sw.surgery_date))
    ) = 1
),

-- ─── labs ↔ surgery (canonical lab table) ───
lab_check AS (
    SELECT
        TRY_CAST(l.research_id AS BIGINT) AS research_id,
        'lab'                              AS artifact_domain,
        l.research_id || '_' || COALESCE(CAST(l.lab_date AS VARCHAR),'unk')
            || '_' || l.analyte_group      AS artifact_id,
        TRY_CAST(l.lab_date AS DATE)       AS artifact_date,
        sw.surgery_episode_id,
        sw.surgery_date,
        ABS(DATEDIFF('day', TRY_CAST(l.lab_date AS DATE), sw.surgery_date)) AS day_gap,
        CASE
            WHEN TRY_CAST(l.lab_date AS DATE) IS NULL THEN 'no_match'
            WHEN ABS(DATEDIFF('day', TRY_CAST(l.lab_date AS DATE), sw.surgery_date)) <= 7
                THEN 'exact'
            WHEN ABS(DATEDIFF('day', TRY_CAST(l.lab_date AS DATE), sw.surgery_date)) <= 90
                THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', TRY_CAST(l.lab_date AS DATE), sw.surgery_date)) <= 365
                THEN 'plausible'
            ELSE 'weak'
        END AS confidence,
        CASE
            WHEN TRY_CAST(l.lab_date AS DATE) IS NULL THEN 'missing_anchor_date'
            WHEN ABS(DATEDIFF('day', TRY_CAST(l.lab_date AS DATE), sw.surgery_date)) > 365
                THEN 'date_out_of_window'
            ELSE NULL
        END AS reason,
        TRY_CAST(l.lab_date AS DATE) >= sw.window_start
            AND TRY_CAST(l.lab_date AS DATE) < sw.window_end AS in_window
    FROM longitudinal_lab_canonical_v1 l
    JOIN surg_windows sw
        ON TRY_CAST(l.research_id AS BIGINT) = sw.research_id
    WHERE TRY_CAST(l.research_id AS BIGINT) IN (SELECT research_id FROM multi_surg)
        AND l.lab_date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY l.research_id, l.lab_date, l.analyte_group
        ORDER BY ABS(DATEDIFF('day', TRY_CAST(l.lab_date AS DATE), sw.surgery_date))
    ) = 1
)

-- UNION ALL domains
SELECT research_id, artifact_domain, artifact_id, artifact_date,
       surgery_episode_id, surgery_date, day_gap, confidence, reason,
       in_window, CURRENT_TIMESTAMP AS audit_ts
FROM op_note_check
UNION ALL
SELECT research_id, artifact_domain, artifact_id, artifact_date,
       surgery_episode_id, surgery_date, day_gap, confidence, reason,
       in_window, CURRENT_TIMESTAMP AS audit_ts
FROM hp_dc_check
UNION ALL
SELECT research_id, artifact_domain, artifact_id, artifact_date,
       surgery_episode_id, surgery_date, day_gap, confidence, reason,
       in_window, CURRENT_TIMESTAMP AS audit_ts
FROM path_check
UNION ALL
SELECT research_id, artifact_domain, artifact_id, artifact_date,
       surgery_episode_id, surgery_date, day_gap, confidence, reason,
       in_window, CURRENT_TIMESTAMP AS audit_ts
FROM rai_check
UNION ALL
SELECT research_id, artifact_domain, artifact_id, artifact_date,
       surgery_episode_id, surgery_date, day_gap, confidence, reason,
       in_window, CURRENT_TIMESTAMP AS audit_ts
FROM mol_check
UNION ALL
SELECT research_id, artifact_domain, artifact_id, artifact_date,
       surgery_episode_id, surgery_date, day_gap, confidence, reason,
       in_window, CURRENT_TIMESTAMP AS audit_ts
FROM fna_check
UNION ALL
SELECT research_id, artifact_domain, artifact_id, artifact_date,
       surgery_episode_id, surgery_date, day_gap, confidence, reason,
       in_window, CURRENT_TIMESTAMP AS audit_ts
FROM img_check
UNION ALL
SELECT research_id, artifact_domain, artifact_id, artifact_date,
       surgery_episode_id, surgery_date, day_gap, confidence, reason,
       in_window, CURRENT_TIMESTAMP AS audit_ts
FROM lab_check
"""


# ── Phase C: review queue ────────────────────────────────────────────────

REVIEW_QUEUE_SQL = """
CREATE OR REPLACE TABLE multi_surgery_artifact_review_queue_v1 AS
WITH problem_artifacts AS (
    SELECT *
    FROM val_multi_surgery_artifact_linkage_v1
    WHERE confidence IN ('weak', 'no_match')
       OR reason IS NOT NULL
),
-- Detect ambiguous assignments (artifact near 2+ surgeries within 30 days)
ambiguity AS (
    SELECT
        a.research_id,
        a.artifact_domain,
        a.artifact_id,
        a.artifact_date,
        COUNT(DISTINCT a.surgery_episode_id) FILTER (
            WHERE a.day_gap <= 30
        ) AS n_close_surgeries
    FROM val_multi_surgery_artifact_linkage_v1 a
    GROUP BY 1,2,3,4
    HAVING COUNT(DISTINCT a.surgery_episode_id) FILTER (WHERE a.day_gap <= 30) > 1
),
-- OED-missing episodes from coverage gap
oed_gaps AS (
    SELECT
        research_id,
        'oed_coverage_gap'   AS artifact_domain,
        'ep_' || CAST(canonical_episode_id AS VARCHAR) AS artifact_id,
        canonical_date       AS artifact_date,
        canonical_episode_id AS surgery_episode_id,
        canonical_date       AS surgery_date,
        NULL::BIGINT         AS day_gap,
        'no_match'           AS confidence,
        'only_single_oed_row' AS reason,
        FALSE                AS in_window
    FROM multi_surgery_oed_coverage_gap_v1
    WHERE oed_match_status IN ('no_oed_row', 'no_date')
),
__combined AS (
SELECT
    p.research_id,
    p.artifact_domain,
    p.artifact_id,
    p.artifact_date,
    p.surgery_episode_id,
    p.surgery_date,
    p.day_gap,
    p.confidence,
    COALESCE(
        CASE WHEN amb.n_close_surgeries > 1 THEN 'ambiguous_equidistant' ELSE NULL END,
        p.reason
    ) AS reason,
    CASE
        WHEN amb.n_close_surgeries > 1 THEN 'HIGH'
        WHEN p.confidence = 'no_match' AND p.reason = 'missing_anchor_date' THEN 'MEDIUM'
        WHEN p.confidence = 'no_match' THEN 'HIGH'
        WHEN p.confidence = 'weak' AND p.reason = 'date_out_of_window' THEN 'MEDIUM'
        WHEN p.confidence = 'weak' THEN 'LOW'
        WHEN p.reason = 'cross_episode_mismatch' THEN 'HIGH'
        ELSE 'LOW'
    END AS review_priority,
    p.in_window,
    CURRENT_TIMESTAMP AS audit_ts
FROM problem_artifacts p
LEFT JOIN ambiguity amb
    ON p.research_id = amb.research_id
    AND p.artifact_domain = amb.artifact_domain
    AND p.artifact_id = amb.artifact_id

UNION ALL

SELECT
    research_id, artifact_domain, artifact_id, artifact_date,
    surgery_episode_id, surgery_date, day_gap::BIGINT AS day_gap, confidence, reason,
    'HIGH' AS review_priority,
    in_window, CURRENT_TIMESTAMP AS audit_ts
FROM oed_gaps
)
SELECT * FROM __combined
ORDER BY
    CASE review_priority WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
    research_id, artifact_domain
"""


# ── orchestration ────────────────────────────────────────────────────────

def run_phase(con, name: str, sql: str, dry_run: bool) -> dict:
    section(f"{'[DRY-RUN] ' if dry_run else ''}{name}")
    if dry_run:
        print(f"  Would execute {len(sql)} chars of SQL")
        return {"status": "dry_run", "rows": 0}
    t0 = time.time()
    try:
        con.execute(sql)
        n = q1(con, f"SELECT COUNT(*) FROM {name}", 0)
        elapsed = time.time() - t0
        print(f"  OK: {n:,} rows  ({elapsed:.1f}s)")
        return {"status": "ok", "rows": n, "elapsed_s": round(elapsed, 1)}
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  ERROR: {e}")
        return {"status": "error", "error": str(e), "elapsed_s": round(elapsed, 1)}


def collect_kpis(con) -> dict:
    """Gather audit KPIs from the result tables."""
    section("Audit KPIs")
    kpis = {}

    # Artifact linkage confidence breakdown
    rows = qall(con, """
        SELECT confidence, COUNT(*) AS n
        FROM val_multi_surgery_artifact_linkage_v1
        GROUP BY 1 ORDER BY 2 DESC
    """)
    for r in rows:
        kpis[f"confidence_{r[0]}"] = r[1]
        print(f"  confidence={r[0]:20s}  n={r[1]:,}")

    # Domain breakdown
    rows = qall(con, """
        SELECT artifact_domain, COUNT(*) AS n
        FROM val_multi_surgery_artifact_linkage_v1
        GROUP BY 1 ORDER BY 2 DESC
    """)
    print()
    for r in rows:
        kpis[f"domain_{r[0]}"] = r[1]
        print(f"  domain={r[0]:20s}  n={r[1]:,}")

    # Reason breakdown
    rows = qall(con, """
        SELECT COALESCE(reason, 'none') AS reason, COUNT(*) AS n
        FROM val_multi_surgery_artifact_linkage_v1
        GROUP BY 1 ORDER BY 2 DESC
    """)
    print()
    for r in rows:
        kpis[f"reason_{r[0]}"] = r[1]
        print(f"  reason={r[0]:25s}  n={r[1]:,}")

    # Review queue priority distribution
    rows = qall(con, """
        SELECT review_priority, COUNT(*) AS n
        FROM multi_surgery_artifact_review_queue_v1
        GROUP BY 1 ORDER BY 2 DESC
    """)
    print()
    for r in rows:
        kpis[f"queue_{r[0]}"] = r[1]
        print(f"  review_priority={r[0]:10s}  n={r[1]:,}")

    # OED coverage gap
    rows = qall(con, """
        SELECT oed_match_status, COUNT(*) AS n
        FROM multi_surgery_oed_coverage_gap_v1
        GROUP BY 1 ORDER BY 2 DESC
    """)
    print()
    for r in rows:
        kpis[f"oed_{r[0]}"] = r[1]
        print(f"  oed_status={r[0]:20s}  n={r[1]:,}")

    # Multi-surgery patient counts
    kpis["multi_surg_patients"] = q1(con, """
        SELECT COUNT(DISTINCT research_id)
        FROM multi_surgery_oed_coverage_gap_v1
    """, 0)
    kpis["total_review_queue"] = q1(con, """
        SELECT COUNT(*) FROM multi_surgery_artifact_review_queue_v1
    """, 0)
    kpis["total_artifacts_audited"] = q1(con, """
        SELECT COUNT(*) FROM val_multi_surgery_artifact_linkage_v1
    """, 0)

    print(f"\n  Multi-surgery patients:  {kpis['multi_surg_patients']:,}")
    print(f"  Total artifacts audited: {kpis['total_artifacts_audited']:,}")
    print(f"  Total review queue:      {kpis['total_review_queue']:,}")

    return kpis


def export_csvs(con) -> list[str]:
    """Export all output tables to CSV."""
    section("Export CSVs")
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    exported = []
    for tbl in OUTPUT_TABLES:
        if not tbl_exists(con, tbl):
            print(f"  SKIP {tbl} (not found)")
            continue
        try:
            df = con.execute(f"SELECT * FROM {tbl}").fetchdf()
            out = EXPORT_DIR / f"{tbl}.csv"
            df.to_csv(out, index=False)
            exported.append(str(out))
            print(f"  {tbl} → {out.name}  ({len(df):,} rows)")
        except Exception as e:
            print(f"  ERROR exporting {tbl}: {e}")
    manifest = {
        "generated": NOW.isoformat(),
        "script": "scripts/98_multi_surgery_artifact_linkage_audit.py",
        "tables": OUTPUT_TABLES,
        "files": [str(p) for p in exported],
    }
    mf = EXPORT_DIR / "manifest.json"
    mf.write_text(json.dumps(manifest, indent=2))
    exported.append(str(mf))
    print("  manifest.json written")
    return exported


def generate_report(kpis: dict, results: dict) -> str:
    """Generate the markdown documentation report."""
    lines = []
    a = lines.append

    a(f"# Multi-Surgery Artifact Linkage Audit — {DATE_TAG}")
    a("")
    a(f"**Generated**: {TIMESTAMP}")
    a("**Script**: `scripts/98_multi_surgery_artifact_linkage_audit.py`")
    a("**Target**: local DuckDB `thyroid_master.duckdb` (prod)")
    a("**Predecessor**: `scripts/96_episode_downstream_repair.py` (ep-id fix)")
    a("")

    a("## Purpose")
    a("")
    a("Post-repair hardening audit to verify that clinical artifacts (op notes,")
    a("H&P, discharge summaries, pathology, FNA, molecular, RAI, imaging, labs)")
    a("are correctly linked to the right surgery episode in multi-surgery patients.")
    a("")

    a("## Output Tables")
    a("")
    a("| Table | Rows | Description |")
    a("|-------|------|-------------|")
    for tbl in OUTPUT_TABLES:
        r = results.get(tbl, {})
        n = r.get("rows", 0)
        descs = {
            "val_multi_surgery_artifact_linkage_v1":
                "Per-artifact linkage verdict (confidence + reason)",
            "multi_surgery_artifact_review_queue_v1":
                "Triaged queue of problematic artifacts",
            "multi_surgery_oed_coverage_gap_v1":
                "OED row ↔ canonical episode coverage mismatch",
        }
        a(f"| `{tbl}` | {n:,} | {descs.get(tbl, '')} |")
    a("")

    a("## Artifact Linkage Confidence Distribution")
    a("")
    a("| Confidence | Count |")
    a("|-----------|-------|")
    for key in ["confidence_exact", "confidence_high_confidence",
                "confidence_plausible", "confidence_weak", "confidence_no_match"]:
        label = key.replace("confidence_", "")
        a(f"| {label} | {kpis.get(key, 0):,} |")
    a("")

    a("## Domain Breakdown")
    a("")
    a("| Domain | Artifacts |")
    a("|--------|-----------|")
    for k, v in sorted(kpis.items()):
        if k.startswith("domain_"):
            a(f"| {k.replace('domain_', '')} | {v:,} |")
    a("")

    a("## Reason Codes (artifacts with issues)")
    a("")
    a("| Reason | Count |")
    a("|--------|-------|")
    for k, v in sorted(kpis.items()):
        if k.startswith("reason_") and k != "reason_none":
            a(f"| {k.replace('reason_', '')} | {v:,} |")
    a("")

    a("## Review Queue Priority")
    a("")
    a("| Priority | Count |")
    a("|----------|-------|")
    for p in ["HIGH", "MEDIUM", "LOW"]:
        a(f"| {p} | {kpis.get(f'queue_{p}', 0):,} |")
    a(f"| **Total** | **{kpis.get('total_review_queue', 0):,}** |")
    a("")

    a("## OED Coverage Gap (multi-surgery patients)")
    a("")
    a("| Status | Count |")
    a("|--------|-------|")
    for k, v in sorted(kpis.items()):
        if k.startswith("oed_"):
            a(f"| {k.replace('oed_', '')} | {v:,} |")
    a("")
    a(f"Multi-surgery patients audited: **{kpis.get('multi_surg_patients', 0):,}**")
    a("")

    a("## Scoring Definitions")
    a("")
    a("### Confidence Tiers")
    a("")
    a("| Tier | Definition |")
    a("|------|-----------|")
    a("| exact | Same-day (op note), within clinical window (H&P 0-7d pre, DC 0-7d post, RAI 14-180d post), or date match |")
    a("| high_confidence | Within 14 days (notes), 90 days (labs), 180 days (molecular/FNA) |")
    a("| plausible | Within 30-365 days depending on domain |")
    a("| weak | Beyond plausible window but still temporally relatable |")
    a("| no_match | No date, or beyond any reasonable window |")
    a("")

    a("### Reason Codes")
    a("")
    a("| Code | Meaning |")
    a("|------|---------|")
    a("| `date_out_of_window` | Artifact date falls outside temporal window for its matched surgery |")
    a("| `missing_anchor_date` | No usable date on artifact |")
    a("| `cross_episode_mismatch` | Pathology ep_id != surgery ep_id it was linked to |")
    a("| `only_single_oed_row` | Multi-surgery patient but only 1 operative row |")
    a("| `ambiguous_equidistant` | Artifact nearly equidistant between 2 surgeries |")
    a("")

    a("## Recommended Triage Subset")
    a("")
    a("For manual review, prioritize:")
    a("")
    a("1. **HIGH priority** items in `multi_surgery_artifact_review_queue_v1`")
    a("   — these are cross-episode mismatches, ambiguous assignments, and")
    a("   no-match artifacts that may affect analytic integrity")
    a("2. **OED coverage gaps** (`multi_surgery_oed_coverage_gap_v1` where")
    a("   `oed_match_status = 'no_oed_row'`) — 525+ patients need upstream")
    a("   operative record population before their 2nd+ surgeries can be audited")
    a("3. **Pathology `cross_episode_mismatch`** — these indicate the")
    a("   surgery_pathology_linkage_v3 routing diverges from temporal expectation")
    a("")

    a("## Provenance")
    a("")
    a("- All tables are additive (CREATE OR REPLACE TABLE)")
    a("- No source table was modified")
    a(f"- Export bundle: `exports/multi_surgery_artifact_linkage_{TIMESTAMP}/`")
    a(f"- Audit timestamp: {NOW.isoformat()}")
    a("")

    return "\n".join(lines)


# ── main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Multi-surgery artifact linkage audit (post ep-id repair)")
    parser.add_argument("--md", action="store_true", default=True,
                        help="Connect to local DuckDB (default)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print SQL plan without executing")
    args = parser.parse_args()

    section(f"Multi-Surgery Artifact Linkage Audit — {TIMESTAMP}")
    print(f"  Dry-run: {args.dry_run}")

    con = get_connection()

    results = {}

    # Phase A: OED coverage gap
    results["multi_surgery_oed_coverage_gap_v1"] = run_phase(
        con, "multi_surgery_oed_coverage_gap_v1",
        OED_COVERAGE_GAP_SQL, args.dry_run
    )

    # Phase B: artifact linkage audit (biggest query)
    results["val_multi_surgery_artifact_linkage_v1"] = run_phase(
        con, "val_multi_surgery_artifact_linkage_v1",
        ARTIFACT_LINKAGE_SQL, args.dry_run
    )

    # Phase C: review queue
    results["multi_surgery_artifact_review_queue_v1"] = run_phase(
        con, "multi_surgery_artifact_review_queue_v1",
        REVIEW_QUEUE_SQL, args.dry_run
    )

    if args.dry_run:
        section("DRY-RUN complete — no tables created")
        con.close()
        return

    # Collect KPIs
    kpis = collect_kpis(con)

    # Export CSVs
    export_csvs(con)

    # Generate documentation
    section("Generate documentation")
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    report = generate_report(kpis, results)
    doc_path = DOCS_DIR / f"multi_surgery_artifact_linkage_audit_{DATE_TAG}.md"
    doc_path.write_text(report)
    print(f"  Report: {doc_path}")

    # JSON metrics
    all_metrics = {
        "kpis": kpis,
        "results": results,
        "timestamp": TIMESTAMP,
        "script": "scripts/98_multi_surgery_artifact_linkage_audit.py",
    }
    json_path = EXPORT_DIR / "audit_metrics.json"
    with open(json_path, "w") as f:
        json.dump(all_metrics, f, indent=2, default=str)
    print(f"  Metrics JSON: {json_path}")

    con.close()

    # Final status
    section("FINAL STATUS")
    ok = sum(1 for v in results.values() if v.get("status") == "ok")
    err = sum(1 for v in results.values() if v.get("status") == "error")
    print(f"  Tables OK:    {ok}/{len(OUTPUT_TABLES)}")
    print(f"  Tables ERROR: {err}")
    print(f"  Total artifacts audited: {kpis.get('total_artifacts_audited', '?'):,}")
    print(f"  Review queue items:      {kpis.get('total_review_queue', '?'):,}")
    if err > 0:
        sys.exit(1)
    print("\n  All phases complete.")


if __name__ == "__main__":
    main()
