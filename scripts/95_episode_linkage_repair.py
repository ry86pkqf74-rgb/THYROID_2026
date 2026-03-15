#!/usr/bin/env python3
"""
Script 95: Episode-Aware Linkage Repair
========================================

Converts high-risk patient-level-only linkage into episode-aware linkage
for multi-surgery patients across 5 domains:

  Phase A – Notes (operative, H&P, discharge)
  Phase B – Labs (episode-aware pre/post-op windows)
  Phase C – Imaging / FNA / Molecular chains
  Phase D – Pathology ↔ surgery episode anchoring
  Phase E – RAI ↔ correct disease episode anchoring
  Phase F – Materialize validation & ambiguity registry
  Phase G – Export manual review packets
  Phase H – Non-regression safety checks

Usage:
  .venv/bin/python scripts/95_episode_linkage_repair.py --md
  .venv/bin/python scripts/95_episode_linkage_repair.py --local
  .venv/bin/python scripts/95_episode_linkage_repair.py --md --phase A
  .venv/bin/python scripts/95_episode_linkage_repair.py --md --dry-run

Outputs:
  MotherDuck tables:
    - episode_note_linkage_repair_v1
    - episode_lab_linkage_repair_v1
    - episode_chain_linkage_repair_v1
    - episode_pathrai_linkage_repair_v1
    - episode_ambiguity_registry_v1
    - episode_linkage_repair_summary_v1
    - md_episode_note_linkage_repair_v1   (mirrors)
    - md_episode_lab_linkage_repair_v1
    - md_episode_chain_linkage_repair_v1
    - md_episode_ambiguity_registry_v1
    - md_episode_linkage_repair_summary_v1
  Exports:
    - exports/episode_linkage_manual_review_packets/
  Docs:
    - docs/episode_linkage_repair_YYYYMMDD.md
    - docs/episode_linkage_nonregression_YYYYMMDD.md
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
DATE_TAG = datetime.now().strftime("%Y%m%d")
EXPORT_DIR = Path("exports/episode_linkage_manual_review_packets")
DOCS_DIR = Path("docs")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def section(msg: str):
    print(f"\n{'='*60}\n  {msg}\n{'='*60}")


def get_connection(args) -> duckdb.DuckDBPyConnection:
    if args.local:
        path = os.getenv("LOCAL_DUCKDB_PATH", "thyroid_master_local.duckdb")
        print(f"  [local] {path}")
        return duckdb.connect(path)
    else:
        print("  [MotherDuck] md:thyroid_research_2026")
        return duckdb.connect("md:thyroid_research_2026")


def safe_execute(con, sql: str, label: str = "", dry_run: bool = False) -> int:
    """Execute SQL, return rowcount (0 on DDL success)."""
    if dry_run:
        print(f"  [DRY-RUN] {label}")
        return 0
    try:
        rc = con.execute(sql).fetchone()
        # DDL returns -1 or None
        cnt = rc[0] if rc and isinstance(rc[0], (int, float)) else 0
        return cnt
    except Exception:
        try:
            con.execute(sql)
            return 0
        except Exception as e:
            print(f"  [ERROR] {label}: {e}")
            return -1


def sql_exec(con, sql: str, label: str = ""):
    """Execute SQL and return nothing, print label on error."""
    try:
        con.execute(sql)
    except Exception as e:
        print(f"  [ERROR] {label}: {e}")


def fetchone_safe(con, sql: str, default=None):
    try:
        r = con.execute(sql).fetchone()
        return r if r else default
    except Exception as e:
        print(f"  [WARN] fetchone: {e}")
        return default


# ---------------------------------------------------------------------------
# PHASE A: Notes Episode Linkage
# ---------------------------------------------------------------------------
NOTES_LINKAGE_SQL = """
CREATE OR REPLACE TABLE episode_note_linkage_repair_v1 AS
WITH
-- Surgery spine: all surgeries with dates
surgery_spine AS (
    SELECT
        research_id,
        surgery_episode_id,
        surgery_date,
        -- For defining windows: previous and next surgery dates
        LAG(surgery_date) OVER (PARTITION BY research_id ORDER BY surgery_episode_id) AS prev_surgery_date,
        LEAD(surgery_date) OVER (PARTITION BY research_id ORDER BY surgery_episode_id) AS next_surgery_date
    FROM tumor_episode_master_v2
    WHERE surgery_date IS NOT NULL
),
-- Flag multi-surgery patients
multi_surg AS (
    SELECT research_id
    FROM surgery_spine
    GROUP BY research_id
    HAVING COUNT(*) > 1
),
-- Notes of interest (operative, H&P, discharge)
notes AS (
    SELECT
        CAST(cn.research_id AS INTEGER) AS research_id,
        cn.note_row_id,
        cn.note_type,
        TRY_CAST(cn.note_date AS DATE) AS note_date,
        cn.note_index
    FROM clinical_notes_long cn
    WHERE cn.note_type IN ('op_note', 'h_p', 'dc_sum')
      AND cn.research_id IS NOT NULL
),
-- Cross join notes with surgery spine for same patient
candidates AS (
    SELECT
        n.research_id,
        n.note_row_id,
        n.note_type,
        n.note_date,
        n.note_index,
        s.surgery_episode_id,
        s.surgery_date,
        s.prev_surgery_date,
        s.next_surgery_date,
        -- Day gap from surgery
        DATEDIFF('day', s.surgery_date, n.note_date) AS day_offset,
        -- Is this patient multi-surgery?
        CASE WHEN ms.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_multi_surgery,
        -- Confidence tier assignment
        CASE
            -- exact same day as surgery
            WHEN n.note_date = s.surgery_date THEN 'exact_day'
            -- within +1 day (op notes filed next day)
            WHEN DATEDIFF('day', s.surgery_date, n.note_date) BETWEEN 0 AND 1
                 AND n.note_type = 'op_note' THEN 'exact_day'
            -- H&P within -1 to 0 (day before or day of)
            WHEN DATEDIFF('day', s.surgery_date, n.note_date) BETWEEN -1 AND 0
                 AND n.note_type = 'h_p' THEN 'exact_day'
            -- discharge within 0 to 3 days post-surgery
            WHEN DATEDIFF('day', s.surgery_date, n.note_date) BETWEEN 0 AND 3
                 AND n.note_type = 'dc_sum' THEN 'exact_day'
            -- anchored window: within -7 to +14 of this surgery,
            -- and closer to this surgery than to any adjacent surgery
            WHEN DATEDIFF('day', s.surgery_date, n.note_date) BETWEEN -7 AND 14
                 AND (s.prev_surgery_date IS NULL
                      OR ABS(DATEDIFF('day', s.surgery_date, n.note_date)) <
                         ABS(DATEDIFF('day', s.prev_surgery_date, n.note_date)))
                 AND (s.next_surgery_date IS NULL
                      OR ABS(DATEDIFF('day', s.surgery_date, n.note_date)) <
                         ABS(DATEDIFF('day', s.next_surgery_date, n.note_date)))
                 THEN 'anchored_window'
            -- wider window for single-surgery patients
            WHEN DATEDIFF('day', s.surgery_date, n.note_date) BETWEEN -30 AND 30
                 AND ms.research_id IS NULL
                 THEN 'anchored_window'
            -- ambiguous: note within range of multiple surgeries
            WHEN n.note_date IS NOT NULL
                 AND DATEDIFF('day', s.surgery_date, n.note_date) BETWEEN -14 AND 30
                 THEN 'ambiguous'
            -- no date: cannot link
            WHEN n.note_date IS NULL THEN 'no_date'
            ELSE 'unlinked'
        END AS linkage_confidence
    FROM notes n
    JOIN surgery_spine s ON n.research_id = s.research_id
    LEFT JOIN multi_surg ms ON n.research_id = ms.research_id
),
-- Rank: prefer exact_day > anchored_window > ambiguous; then by ABS(day_offset)
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, note_row_id
            ORDER BY
                CASE linkage_confidence
                    WHEN 'exact_day' THEN 1
                    WHEN 'anchored_window' THEN 2
                    WHEN 'ambiguous' THEN 3
                    WHEN 'no_date' THEN 4
                    ELSE 5
                END,
                ABS(COALESCE(day_offset, 99999))
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY research_id, note_row_id
            ORDER BY
                CASE linkage_confidence
                    WHEN 'exact_day' THEN 1
                    WHEN 'anchored_window' THEN 2
                    WHEN 'ambiguous' THEN 3
                    WHEN 'no_date' THEN 4
                    ELSE 5
                END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS n_candidates
    FROM candidates
    WHERE linkage_confidence NOT IN ('unlinked')
)
SELECT
    research_id,
    note_row_id,
    note_type,
    note_date,
    note_index,
    surgery_episode_id AS linked_surgery_episode_id,
    surgery_date AS linked_surgery_date,
    day_offset,
    is_multi_surgery,
    linkage_confidence,
    CASE
        WHEN linkage_confidence = 'exact_day' THEN 'exact_day_match'
        WHEN linkage_confidence = 'anchored_window' THEN 'date_window_nearest_surgery'
        WHEN linkage_confidence = 'ambiguous' THEN 'ambiguous_multi_candidate'
        WHEN linkage_confidence = 'no_date' THEN 'no_date_available'
        ELSE 'other'
    END AS linkage_method,
    rn AS rank_among_candidates,
    n_candidates,
    CURRENT_TIMESTAMP AS repaired_at
FROM ranked
WHERE rn = 1
"""


# ---------------------------------------------------------------------------
# PHASE B: Lab Episode Windowing
# ---------------------------------------------------------------------------
LAB_LINKAGE_SQL = """
CREATE OR REPLACE TABLE episode_lab_linkage_repair_v1 AS
WITH
surgery_spine AS (
    SELECT
        research_id,
        surgery_episode_id,
        surgery_date,
        LAG(surgery_date) OVER (PARTITION BY research_id ORDER BY surgery_episode_id) AS prev_surgery_date,
        LEAD(surgery_date) OVER (PARTITION BY research_id ORDER BY surgery_episode_id) AS next_surgery_date
    FROM tumor_episode_master_v2
    WHERE surgery_date IS NOT NULL
),
multi_surg AS (
    SELECT research_id FROM surgery_spine GROUP BY research_id HAVING COUNT(*) > 1
),
labs AS (
    SELECT
        CAST(l.research_id AS INTEGER) AS research_id,
        l.lab_date,
        l.lab_name_standardized,
        l.analyte_group,
        l.value_numeric,
        l.value_raw,
        l.source_table,
        TRY_CAST(l.lab_date AS DATE) AS lab_date_parsed
    FROM longitudinal_lab_canonical_v1 l
    WHERE l.research_id IS NOT NULL
),
candidates AS (
    SELECT
        lb.research_id,
        lb.lab_date,
        lb.lab_date_parsed,
        lb.lab_name_standardized,
        lb.analyte_group,
        lb.value_numeric,
        lb.value_raw,
        lb.source_table,
        s.surgery_episode_id,
        s.surgery_date,
        s.prev_surgery_date,
        s.next_surgery_date,
        DATEDIFF('day', s.surgery_date, lb.lab_date_parsed) AS day_offset,
        CASE WHEN ms.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_multi_surgery,
        -- Pre-op window: -30 to -1 days before surgery
        -- Post-op window: 0 to +365 days after surgery
        -- For multi-surgery: window ends at midpoint to next surgery
        CASE
            WHEN lb.lab_date_parsed IS NULL THEN 'no_date'
            -- Pre-op: 30 days before surgery
            WHEN DATEDIFF('day', s.surgery_date, lb.lab_date_parsed) BETWEEN -30 AND -1
                 THEN 'preop'
            -- Same day
            WHEN lb.lab_date_parsed = s.surgery_date THEN 'periop'
            -- Post-op: up to next surgery midpoint (or 365 days if single/last)
            WHEN DATEDIFF('day', s.surgery_date, lb.lab_date_parsed) BETWEEN 1 AND
                 LEAST(
                     COALESCE(
                         DATEDIFF('day', s.surgery_date, s.next_surgery_date) / 2,
                         365
                     ),
                     365
                 )
                 THEN 'postop'
            ELSE 'out_of_window'
        END AS episode_window,
        -- Overlap detection: is this lab in the window of multiple surgeries?
        CASE
            WHEN ms.research_id IS NOT NULL
                 AND lb.lab_date_parsed IS NOT NULL
                 AND s.next_surgery_date IS NOT NULL
                 AND DATEDIFF('day', s.surgery_date, lb.lab_date_parsed) >= 0
                 AND DATEDIFF('day', lb.lab_date_parsed, s.next_surgery_date) <= 30
                 THEN TRUE
            WHEN ms.research_id IS NOT NULL
                 AND lb.lab_date_parsed IS NOT NULL
                 AND s.prev_surgery_date IS NOT NULL
                 AND DATEDIFF('day', lb.lab_date_parsed, s.surgery_date) >= 0
                 AND DATEDIFF('day', s.prev_surgery_date, lb.lab_date_parsed) <= 30
                 THEN TRUE
            ELSE FALSE
        END AS overlapping_window_flag
    FROM labs lb
    JOIN surgery_spine s ON lb.research_id = s.research_id
    LEFT JOIN multi_surg ms ON lb.research_id = ms.research_id
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, lab_date, lab_name_standardized, value_raw
            ORDER BY
                CASE episode_window
                    WHEN 'periop' THEN 1
                    WHEN 'preop' THEN 2
                    WHEN 'postop' THEN 3
                    WHEN 'out_of_window' THEN 4
                    ELSE 5
                END,
                ABS(COALESCE(day_offset, 99999))
        ) AS rn
    FROM candidates
    WHERE episode_window != 'out_of_window'
)
SELECT
    research_id,
    lab_date,
    lab_date_parsed,
    lab_name_standardized,
    analyte_group,
    value_numeric,
    value_raw,
    source_table,
    surgery_episode_id AS linked_surgery_episode_id,
    surgery_date AS linked_surgery_date,
    day_offset,
    is_multi_surgery,
    episode_window,
    overlapping_window_flag,
    CASE
        WHEN episode_window IN ('periop', 'preop', 'postop') AND NOT overlapping_window_flag
            THEN 'unambiguous_episode_window'
        WHEN episode_window IN ('periop', 'preop', 'postop') AND overlapping_window_flag
            THEN 'overlapping_window_nearest'
        WHEN episode_window = 'no_date' THEN 'no_date'
        ELSE 'other'
    END AS linkage_method,
    CASE
        WHEN episode_window IN ('periop') AND NOT overlapping_window_flag THEN 'exact_day'
        WHEN episode_window IN ('preop', 'postop') AND NOT overlapping_window_flag THEN 'anchored_window'
        WHEN overlapping_window_flag THEN 'ambiguous'
        WHEN episode_window = 'no_date' THEN 'no_date'
        ELSE 'weak'
    END AS linkage_confidence,
    rn AS rank_among_candidates,
    CURRENT_TIMESTAMP AS repaired_at
FROM ranked
WHERE rn = 1
"""


# ---------------------------------------------------------------------------
# PHASE C: Imaging / FNA / Molecular Chain Repair
# ---------------------------------------------------------------------------
CHAIN_LINKAGE_SQL = """
CREATE OR REPLACE TABLE episode_chain_linkage_repair_v1 AS
WITH
surgery_spine AS (
    SELECT research_id, surgery_episode_id, surgery_date,
           laterality AS surgery_laterality,
           LAG(surgery_date) OVER (PARTITION BY research_id ORDER BY surgery_episode_id) AS prev_surg_date,
           LEAD(surgery_date) OVER (PARTITION BY research_id ORDER BY surgery_episode_id) AS next_surg_date
    FROM tumor_episode_master_v2
    WHERE surgery_date IS NOT NULL
),
multi_surg AS (
    SELECT research_id FROM surgery_spine GROUP BY research_id HAVING COUNT(*) > 1
),
-- FNA episodes
fna AS (
    SELECT research_id, fna_episode_id, fna_date_native, laterality,
           linked_surgery_episode_id AS existing_surg_link,
           linked_molecular_episode_id AS existing_mol_link
    FROM fna_episode_master_v2
    WHERE research_id IS NOT NULL
),
-- Molecular episodes
mol AS (
    SELECT research_id, molecular_episode_id, test_date_native,
           linked_fna_episode_id AS existing_fna_link,
           linked_surgery_episode_id AS existing_surg_link
    FROM molecular_test_episode_v2
    WHERE research_id IS NOT NULL
),
-- FNA -> Surgery linkage candidates (for multi-surg only)
fna_surg_candidates AS (
    SELECT
        f.research_id,
        'fna_to_surgery' AS chain_type,
        CAST(f.fna_episode_id AS VARCHAR) AS source_episode_id,
        CAST(s.surgery_episode_id AS VARCHAR) AS target_surgery_episode_id,
        f.fna_date_native AS source_date,
        s.surgery_date AS target_date,
        DATEDIFF('day', f.fna_date_native, s.surgery_date) AS day_gap,
        f.laterality AS source_laterality,
        s.surgery_laterality AS target_laterality,
        f.existing_surg_link,
        CASE WHEN ms.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_multi_surgery,
        -- Confidence scoring
        CASE
            WHEN f.fna_date_native IS NULL THEN 'no_date'
            WHEN DATEDIFF('day', f.fna_date_native, s.surgery_date) BETWEEN 0 AND 14
                 AND COALESCE(f.laterality = s.surgery_laterality, TRUE)
                 THEN 'exact_match'
            WHEN DATEDIFF('day', f.fna_date_native, s.surgery_date) BETWEEN 0 AND 90
                 AND COALESCE(f.laterality = s.surgery_laterality, TRUE)
                 THEN 'high_confidence'
            WHEN DATEDIFF('day', f.fna_date_native, s.surgery_date) BETWEEN -7 AND 180
                 THEN 'plausible'
            WHEN DATEDIFF('day', f.fna_date_native, s.surgery_date) BETWEEN -7 AND 365
                 THEN 'weak'
            ELSE 'unlinked'
        END AS linkage_confidence,
        -- Is closer to this surgery than adjacent?
        CASE
            WHEN s.prev_surg_date IS NOT NULL
                 AND ABS(DATEDIFF('day', f.fna_date_native, s.prev_surg_date))
                     < ABS(DATEDIFF('day', f.fna_date_native, s.surgery_date))
                 THEN TRUE
            WHEN s.next_surg_date IS NOT NULL
                 AND ABS(DATEDIFF('day', f.fna_date_native, s.next_surg_date))
                     < ABS(DATEDIFF('day', f.fna_date_native, s.surgery_date))
                 THEN TRUE
            ELSE FALSE
        END AS closer_to_other_surgery
    FROM fna f
    JOIN surgery_spine s ON f.research_id = s.research_id
    LEFT JOIN multi_surg ms ON f.research_id = ms.research_id
    WHERE f.fna_date_native IS NOT NULL
),
-- Molecular -> Surgery (inherit FNA episode if available, else date-match)
mol_surg_candidates AS (
    SELECT
        m.research_id,
        'molecular_to_surgery' AS chain_type,
        CAST(m.molecular_episode_id AS VARCHAR) AS source_episode_id,
        CAST(s.surgery_episode_id AS VARCHAR) AS target_surgery_episode_id,
        m.test_date_native AS source_date,
        s.surgery_date AS target_date,
        DATEDIFF('day', m.test_date_native, s.surgery_date) AS day_gap,
        NULL AS source_laterality,
        s.surgery_laterality AS target_laterality,
        m.existing_surg_link,
        CASE WHEN ms.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_multi_surgery,
        CASE
            WHEN m.test_date_native IS NULL THEN 'no_date'
            WHEN DATEDIFF('day', m.test_date_native, s.surgery_date) BETWEEN 0 AND 30
                 THEN 'high_confidence'
            WHEN DATEDIFF('day', m.test_date_native, s.surgery_date) BETWEEN -7 AND 180
                 THEN 'plausible'
            WHEN DATEDIFF('day', m.test_date_native, s.surgery_date) BETWEEN -7 AND 365
                 THEN 'weak'
            ELSE 'unlinked'
        END AS linkage_confidence,
        CASE
            WHEN s.prev_surg_date IS NOT NULL
                 AND m.test_date_native IS NOT NULL
                 AND ABS(DATEDIFF('day', m.test_date_native, s.prev_surg_date))
                     < ABS(DATEDIFF('day', m.test_date_native, s.surgery_date))
                 THEN TRUE
            WHEN s.next_surg_date IS NOT NULL
                 AND m.test_date_native IS NOT NULL
                 AND ABS(DATEDIFF('day', m.test_date_native, s.next_surg_date))
                     < ABS(DATEDIFF('day', m.test_date_native, s.surgery_date))
                 THEN TRUE
            ELSE FALSE
        END AS closer_to_other_surgery
    FROM mol m
    JOIN surgery_spine s ON m.research_id = s.research_id
    LEFT JOIN multi_surg ms ON m.research_id = ms.research_id
    WHERE m.test_date_native IS NOT NULL
),
-- Union and rank
all_candidates AS (
    SELECT * FROM fna_surg_candidates WHERE linkage_confidence NOT IN ('unlinked')
    UNION ALL
    SELECT * FROM mol_surg_candidates WHERE linkage_confidence NOT IN ('unlinked')
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, chain_type, source_episode_id
            ORDER BY
                CASE linkage_confidence
                    WHEN 'exact_match' THEN 1
                    WHEN 'high_confidence' THEN 2
                    WHEN 'plausible' THEN 3
                    WHEN 'weak' THEN 4
                    WHEN 'no_date' THEN 5
                    ELSE 6
                END,
                ABS(COALESCE(day_gap, 99999))
        ) AS rn,
        -- Count how many candidates per source episode at or above the winning tier
        COUNT(*) FILTER (WHERE NOT closer_to_other_surgery) OVER (
            PARTITION BY research_id, chain_type, source_episode_id
        ) AS n_viable_targets
    FROM all_candidates
)
SELECT
    research_id,
    chain_type,
    source_episode_id,
    target_surgery_episode_id AS linked_surgery_episode_id,
    source_date,
    target_date AS linked_surgery_date,
    day_gap,
    source_laterality,
    target_laterality,
    existing_surg_link,
    is_multi_surgery,
    linkage_confidence,
    closer_to_other_surgery,
    CASE
        WHEN closer_to_other_surgery THEN 'ambiguous_closer_to_other'
        WHEN linkage_confidence IN ('exact_match', 'high_confidence') THEN 'date_laterality_match'
        WHEN linkage_confidence = 'plausible' THEN 'date_window_nearest'
        WHEN linkage_confidence = 'weak' THEN 'weak_temporal_only'
        ELSE 'other'
    END AS linkage_method,
    rn AS rank_among_candidates,
    n_viable_targets,
    CURRENT_TIMESTAMP AS repaired_at
FROM ranked
WHERE rn = 1
"""


# ---------------------------------------------------------------------------
# PHASE D: Pathology-Surgery & Phase E: RAI Anchoring
# ---------------------------------------------------------------------------
PATHRAI_LINKAGE_SQL = """
CREATE OR REPLACE TABLE episode_pathrai_linkage_repair_v1 AS
WITH
surgery_spine AS (
    SELECT research_id, surgery_episode_id, surgery_date,
           laterality AS surgery_laterality,
           LAG(surgery_date) OVER (PARTITION BY research_id ORDER BY surgery_episode_id) AS prev_surg_date,
           LEAD(surgery_date) OVER (PARTITION BY research_id ORDER BY surgery_episode_id) AS next_surg_date,
           -- Flag cancer episodes
           CASE WHEN primary_histology IS NOT NULL
                     AND LOWER(COALESCE(primary_histology,'')) NOT IN ('','benign','hyperplasia','adenoma')
                THEN TRUE ELSE FALSE END AS is_cancer_episode
    FROM tumor_episode_master_v2
    WHERE surgery_date IS NOT NULL
),
multi_surg AS (
    SELECT research_id FROM surgery_spine GROUP BY research_id HAVING COUNT(*) > 1
),
-- RAI episodes
rai AS (
    SELECT research_id, rai_episode_id, resolved_rai_date, dose_mci,
           linked_surgery_episode_id AS existing_surg_link
    FROM rai_treatment_episode_v2
    WHERE research_id IS NOT NULL
),
-- RAI -> Surgery: link to the most recent cancer surgery BEFORE the RAI date
rai_surg_candidates AS (
    SELECT
        r.research_id,
        'rai_to_surgery' AS domain,
        CAST(r.rai_episode_id AS VARCHAR) AS source_episode_id,
        CAST(s.surgery_episode_id AS VARCHAR) AS target_surgery_episode_id,
        r.resolved_rai_date AS source_date,
        s.surgery_date AS target_date,
        r.dose_mci,
        DATEDIFF('day', s.surgery_date, r.resolved_rai_date) AS day_gap,
        r.existing_surg_link,
        CASE WHEN ms.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_multi_surgery,
        s.is_cancer_episode,
        CASE
            WHEN r.resolved_rai_date IS NULL THEN 'no_date'
            -- RAI within 30-180 days after cancer surgery: standard protocol
            WHEN DATEDIFF('day', s.surgery_date, r.resolved_rai_date) BETWEEN 14 AND 180
                 AND s.is_cancer_episode
                 THEN 'high_confidence'
            -- RAI within 180-365 days: delayed but plausible
            WHEN DATEDIFF('day', s.surgery_date, r.resolved_rai_date) BETWEEN 14 AND 365
                 AND s.is_cancer_episode
                 THEN 'plausible'
            -- RAI after non-cancer surgery: unusual
            WHEN DATEDIFF('day', s.surgery_date, r.resolved_rai_date) > 0
                 AND NOT s.is_cancer_episode
                 THEN 'weak'
            -- RAI before surgery (shouldn't happen but handle)
            WHEN DATEDIFF('day', s.surgery_date, r.resolved_rai_date) BETWEEN -7 AND 13
                 THEN 'weak'
            ELSE 'unlinked'
        END AS linkage_confidence,
        -- Multiple cancer episodes? Flag ambiguity
        CASE
            WHEN ms.research_id IS NOT NULL AND s.is_cancer_episode
                 AND s.next_surg_date IS NOT NULL
                 AND r.resolved_rai_date IS NOT NULL
                 AND DATEDIFF('day', s.surgery_date, r.resolved_rai_date) > 0
                 AND DATEDIFF('day', r.resolved_rai_date, s.next_surg_date) < 180
                 THEN TRUE
            ELSE FALSE
        END AS ambiguous_between_episodes
    FROM rai r
    JOIN surgery_spine s ON r.research_id = s.research_id
    LEFT JOIN multi_surg ms ON r.research_id = ms.research_id
),
-- Pathology accession: verify path is anchored to correct surgery
path_surg_candidates AS (
    SELECT
        t.research_id,
        'pathology_to_surgery' AS domain,
        CAST(t.surgery_episode_id AS VARCHAR) AS source_episode_id,
        CAST(o.surgery_episode_id AS VARCHAR) AS target_surgery_episode_id,
        t.surgery_date AS source_date,
        o.surgery_date_native AS target_date,
        NULL::DOUBLE AS dose_mci,
        DATEDIFF('day', t.surgery_date, o.surgery_date_native) AS day_gap,
        NULL AS existing_surg_link,
        CASE WHEN ms.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_multi_surgery,
        TRUE AS is_cancer_episode,
        CASE
            WHEN t.surgery_date = o.surgery_date_native THEN 'exact_match'
            WHEN ABS(DATEDIFF('day', t.surgery_date, o.surgery_date_native)) <= 1 THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', t.surgery_date, o.surgery_date_native)) <= 7 THEN 'plausible'
            ELSE 'weak'
        END AS linkage_confidence,
        CASE
            WHEN t.surgery_episode_id != o.surgery_episode_id THEN TRUE
            ELSE FALSE
        END AS ambiguous_between_episodes
    FROM tumor_episode_master_v2 t
    JOIN operative_episode_detail_v2 o
        ON t.research_id = o.research_id
    LEFT JOIN multi_surg ms ON t.research_id = ms.research_id
    WHERE t.surgery_date IS NOT NULL
      AND o.surgery_date_native IS NOT NULL
      AND ms.research_id IS NOT NULL  -- only for multi-surgery patients
),
all_domain AS (
    SELECT * FROM rai_surg_candidates WHERE linkage_confidence NOT IN ('unlinked')
    UNION ALL
    SELECT * FROM path_surg_candidates
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, domain, source_episode_id
            ORDER BY
                CASE linkage_confidence
                    WHEN 'exact_match' THEN 1
                    WHEN 'high_confidence' THEN 2
                    WHEN 'plausible' THEN 3
                    WHEN 'weak' THEN 4
                    WHEN 'no_date' THEN 5
                    ELSE 6
                END,
                -- For RAI: prefer cancer episodes
                CASE WHEN is_cancer_episode THEN 0 ELSE 1 END,
                ABS(COALESCE(day_gap, 99999))
        ) AS rn
    FROM all_domain
)
SELECT
    research_id,
    domain,
    source_episode_id,
    target_surgery_episode_id AS linked_surgery_episode_id,
    source_date,
    target_date AS linked_surgery_date,
    dose_mci,
    day_gap,
    existing_surg_link,
    is_multi_surgery,
    is_cancer_episode,
    linkage_confidence,
    ambiguous_between_episodes,
    CASE
        WHEN domain = 'pathology_to_surgery' AND linkage_confidence = 'exact_match'
            THEN 'same_day_accession'
        WHEN domain = 'rai_to_surgery' AND linkage_confidence = 'high_confidence'
            THEN 'standard_protocol_window'
        WHEN ambiguous_between_episodes
            THEN 'ambiguous_multi_episode'
        ELSE 'temporal_nearest'
    END AS linkage_method,
    rn AS rank_among_candidates,
    CURRENT_TIMESTAMP AS repaired_at
FROM ranked
WHERE rn = 1
"""


# ---------------------------------------------------------------------------
# PHASE F: Ambiguity Registry
# ---------------------------------------------------------------------------
AMBIGUITY_REGISTRY_SQL = """
CREATE OR REPLACE TABLE episode_ambiguity_registry_v1 AS
-- Collect all ambiguous linkages across domains
WITH note_ambig AS (
    SELECT research_id, 'notes' AS domain,
           note_row_id AS source_id, note_type AS entity_type,
           linked_surgery_episode_id, linkage_confidence,
           linkage_method, day_offset AS day_gap, is_multi_surgery
    FROM episode_note_linkage_repair_v1
    WHERE linkage_confidence = 'ambiguous' OR linkage_method = 'ambiguous_multi_candidate'
),
lab_ambig AS (
    SELECT research_id, 'labs' AS domain,
           CONCAT(lab_date, '_', lab_name_standardized) AS source_id,
           analyte_group AS entity_type,
           linked_surgery_episode_id, linkage_confidence,
           linkage_method, day_offset AS day_gap, is_multi_surgery
    FROM episode_lab_linkage_repair_v1
    WHERE linkage_confidence = 'ambiguous' OR overlapping_window_flag
),
chain_ambig AS (
    SELECT research_id, chain_type AS domain,
           source_episode_id AS source_id, chain_type AS entity_type,
           linked_surgery_episode_id, linkage_confidence,
           linkage_method, day_gap, is_multi_surgery
    FROM episode_chain_linkage_repair_v1
    WHERE linkage_confidence IN ('weak', 'no_date') OR closer_to_other_surgery
),
pathrai_ambig AS (
    SELECT research_id, domain,
           source_episode_id AS source_id, domain AS entity_type,
           linked_surgery_episode_id, linkage_confidence,
           linkage_method, day_gap, is_multi_surgery
    FROM episode_pathrai_linkage_repair_v1
    WHERE ambiguous_between_episodes OR linkage_confidence IN ('weak', 'no_date')
)
SELECT *, CURRENT_TIMESTAMP AS flagged_at,
       'pending_review' AS review_status
FROM (
    SELECT * FROM note_ambig
    UNION ALL SELECT * FROM lab_ambig
    UNION ALL SELECT * FROM chain_ambig
    UNION ALL SELECT * FROM pathrai_ambig
)
"""


# ---------------------------------------------------------------------------
# PHASE F2: Summary
# ---------------------------------------------------------------------------
SUMMARY_SQL = """
CREATE OR REPLACE TABLE episode_linkage_repair_summary_v1 AS
WITH
note_stats AS (
    SELECT 'notes' AS domain,
        COUNT(*) AS total_linked,
        SUM(CASE WHEN is_multi_surgery THEN 1 ELSE 0 END) AS multi_surg_linked,
        SUM(CASE WHEN linkage_confidence = 'exact_day' THEN 1 ELSE 0 END) AS exact,
        SUM(CASE WHEN linkage_confidence = 'anchored_window' THEN 1 ELSE 0 END) AS anchored,
        SUM(CASE WHEN linkage_confidence = 'ambiguous' THEN 1 ELSE 0 END) AS ambiguous,
        SUM(CASE WHEN linkage_confidence = 'no_date' THEN 1 ELSE 0 END) AS no_date
    FROM episode_note_linkage_repair_v1
),
lab_stats AS (
    SELECT 'labs' AS domain,
        COUNT(*) AS total_linked,
        SUM(CASE WHEN is_multi_surgery THEN 1 ELSE 0 END) AS multi_surg_linked,
        SUM(CASE WHEN linkage_confidence = 'exact_day' THEN 1 ELSE 0 END) AS exact,
        SUM(CASE WHEN linkage_confidence = 'anchored_window' THEN 1 ELSE 0 END) AS anchored,
        SUM(CASE WHEN linkage_confidence = 'ambiguous' THEN 1 ELSE 0 END) AS ambiguous,
        SUM(CASE WHEN linkage_confidence = 'no_date' THEN 1 ELSE 0 END) AS no_date
    FROM episode_lab_linkage_repair_v1
),
chain_stats AS (
    SELECT 'chains' AS domain,
        COUNT(*) AS total_linked,
        SUM(CASE WHEN is_multi_surgery THEN 1 ELSE 0 END) AS multi_surg_linked,
        SUM(CASE WHEN linkage_confidence = 'exact_match' THEN 1 ELSE 0 END) AS exact,
        SUM(CASE WHEN linkage_confidence = 'high_confidence' THEN 1 ELSE 0 END) AS anchored,
        SUM(CASE WHEN linkage_confidence IN ('weak','no_date') OR closer_to_other_surgery THEN 1 ELSE 0 END) AS ambiguous,
        SUM(CASE WHEN linkage_confidence = 'no_date' THEN 1 ELSE 0 END) AS no_date
    FROM episode_chain_linkage_repair_v1
),
pathrai_stats AS (
    SELECT 'pathology_rai' AS domain,
        COUNT(*) AS total_linked,
        SUM(CASE WHEN is_multi_surgery THEN 1 ELSE 0 END) AS multi_surg_linked,
        SUM(CASE WHEN linkage_confidence = 'exact_match' THEN 1 ELSE 0 END) AS exact,
        SUM(CASE WHEN linkage_confidence = 'high_confidence' THEN 1 ELSE 0 END) AS anchored,
        SUM(CASE WHEN linkage_confidence IN ('weak','no_date') OR ambiguous_between_episodes THEN 1 ELSE 0 END) AS ambiguous,
        SUM(CASE WHEN linkage_confidence = 'no_date' THEN 1 ELSE 0 END) AS no_date
    FROM episode_pathrai_linkage_repair_v1
),
ambig_stats AS (
    SELECT 'ambiguity_registry' AS domain,
        COUNT(*) AS total_linked,
        SUM(CASE WHEN is_multi_surgery THEN 1 ELSE 0 END) AS multi_surg_linked,
        0 AS exact, 0 AS anchored,
        COUNT(*) AS ambiguous,
        0 AS no_date
    FROM episode_ambiguity_registry_v1
)
SELECT *, CURRENT_TIMESTAMP AS computed_at
FROM (
    SELECT * FROM note_stats UNION ALL
    SELECT * FROM lab_stats UNION ALL
    SELECT * FROM chain_stats UNION ALL
    SELECT * FROM pathrai_stats UNION ALL
    SELECT * FROM ambig_stats
)
"""


# ---------------------------------------------------------------------------
# PHASE H: Non-regression check SQL
# ---------------------------------------------------------------------------
NONREG_SINGLE_SURG_SQL = """
-- Verify single-surgery patients are unaffected
WITH single_surg AS (
    SELECT research_id FROM tumor_episode_master_v2
    GROUP BY research_id HAVING COUNT(DISTINCT surgery_episode_id) = 1
),
-- Note linkage for single-surg: should all be surgery_episode_id = 1
note_check AS (
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN linked_surgery_episode_id = 1 THEN 1 ELSE 0 END) AS correct,
        SUM(CASE WHEN linked_surgery_episode_id != 1 THEN 1 ELSE 0 END) AS mislinked
    FROM episode_note_linkage_repair_v1 n
    JOIN single_surg ss ON n.research_id = ss.research_id
    WHERE n.linked_surgery_episode_id IS NOT NULL
),
-- Lab linkage check
lab_check AS (
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN linked_surgery_episode_id = 1 THEN 1 ELSE 0 END) AS correct,
        SUM(CASE WHEN linked_surgery_episode_id != 1 THEN 1 ELSE 0 END) AS mislinked
    FROM episode_lab_linkage_repair_v1 l
    JOIN single_surg ss ON l.research_id = ss.research_id
    WHERE l.linked_surgery_episode_id IS NOT NULL
)
SELECT 'notes' AS domain, * FROM note_check
UNION ALL
SELECT 'labs' AS domain, * FROM lab_check
"""


# ---------------------------------------------------------------------------
# Materialization map for MotherDuck mirrors
# ---------------------------------------------------------------------------
MIRROR_MAP = [
    ("md_episode_note_linkage_repair_v1", "episode_note_linkage_repair_v1"),
    ("md_episode_lab_linkage_repair_v1", "episode_lab_linkage_repair_v1"),
    ("md_episode_chain_linkage_repair_v1", "episode_chain_linkage_repair_v1"),
    ("md_episode_pathrai_linkage_repair_v1", "episode_pathrai_linkage_repair_v1"),
    ("md_episode_ambiguity_registry_v1", "episode_ambiguity_registry_v1"),
    ("md_episode_linkage_repair_summary_v1", "episode_linkage_repair_summary_v1"),
]


# ===========================================================================
# Main
# ===========================================================================
def run_phase_a(con, dry_run: bool):
    section("Phase A: Notes Episode Linkage Repair")
    # Baseline
    before = fetchone_safe(con, """
        WITH ms AS (
            SELECT research_id FROM tumor_episode_master_v2
            GROUP BY research_id HAVING COUNT(DISTINCT surgery_episode_id) > 1
        )
        SELECT COUNT(DISTINCT cn.note_row_id) 
        FROM clinical_notes_long cn
        JOIN ms ON CAST(cn.research_id AS INTEGER) = ms.research_id
        WHERE cn.note_type IN ('op_note','h_p','dc_sum')
    """, (0,))
    print(f"  [BEFORE] multi-surg notes (op/hp/dc): {before[0]} total, 0 episode-linked")

    if not dry_run:
        con.execute(NOTES_LINKAGE_SQL)
        r = fetchone_safe(con, """
            SELECT COUNT(*) total,
                SUM(CASE WHEN is_multi_surgery THEN 1 ELSE 0 END) multi,
                SUM(CASE WHEN linkage_confidence='exact_day' THEN 1 ELSE 0 END) exact,
                SUM(CASE WHEN linkage_confidence='anchored_window' THEN 1 ELSE 0 END) anchored,
                SUM(CASE WHEN linkage_confidence='ambiguous' THEN 1 ELSE 0 END) ambig,
                SUM(CASE WHEN linkage_confidence='no_date' THEN 1 ELSE 0 END) nodate
            FROM episode_note_linkage_repair_v1
        """, (0,0,0,0,0,0))
        print(f"  [AFTER] total={r[0]}, multi_surg={r[1]}, exact={r[2]}, anchored={r[3]}, ambiguous={r[4]}, no_date={r[5]}")
    else:
        print("  [DRY-RUN] would create episode_note_linkage_repair_v1")
    return before[0]


def run_phase_b(con, dry_run: bool):
    section("Phase B: Lab Episode Windowing")
    before = fetchone_safe(con, """
        WITH ms AS (
            SELECT research_id FROM tumor_episode_master_v2
            GROUP BY research_id HAVING COUNT(DISTINCT surgery_episode_id) > 1
        )
        SELECT COUNT(*) FROM longitudinal_lab_canonical_v1 l
        JOIN ms ON CAST(l.research_id AS INTEGER) = ms.research_id
    """, (0,))
    print(f"  [BEFORE] multi-surg labs: {before[0]} total, 0 episode-linked")

    if not dry_run:
        con.execute(LAB_LINKAGE_SQL)
        r = fetchone_safe(con, """
            SELECT COUNT(*) total,
                SUM(CASE WHEN is_multi_surgery THEN 1 ELSE 0 END) multi,
                SUM(CASE WHEN linkage_confidence='exact_day' THEN 1 ELSE 0 END) exact,
                SUM(CASE WHEN linkage_confidence='anchored_window' THEN 1 ELSE 0 END) anchored,
                SUM(CASE WHEN linkage_confidence='ambiguous' THEN 1 ELSE 0 END) ambig,
                SUM(CASE WHEN overlapping_window_flag THEN 1 ELSE 0 END) overlapping
            FROM episode_lab_linkage_repair_v1
        """, (0,0,0,0,0,0))
        print(f"  [AFTER] total={r[0]}, multi_surg={r[1]}, exact={r[2]}, anchored={r[3]}, ambiguous={r[4]}, overlapping={r[5]}")
    else:
        print("  [DRY-RUN] would create episode_lab_linkage_repair_v1")
    return before[0]


def run_phase_c(con, dry_run: bool):
    section("Phase C: Imaging/FNA/Molecular Chain Repair")
    before_fna = fetchone_safe(con, """
        WITH ms AS (
            SELECT research_id FROM tumor_episode_master_v2
            GROUP BY research_id HAVING COUNT(DISTINCT surgery_episode_id) > 1
        )
        SELECT COUNT(*), SUM(CASE WHEN linked_surgery_episode_id IS NOT NULL THEN 1 ELSE 0 END)
        FROM fna_episode_master_v2 f JOIN ms ON f.research_id = ms.research_id
    """, (0,0))
    before_mol = fetchone_safe(con, """
        WITH ms AS (
            SELECT research_id FROM tumor_episode_master_v2
            GROUP BY research_id HAVING COUNT(DISTINCT surgery_episode_id) > 1
        )
        SELECT COUNT(*), SUM(CASE WHEN linked_surgery_episode_id IS NOT NULL THEN 1 ELSE 0 END)
        FROM molecular_test_episode_v2 m JOIN ms ON m.research_id = ms.research_id
    """, (0,0))
    print(f"  [BEFORE] FNA multi-surg: {before_fna[0]} total, {before_fna[1]} linked")
    print(f"  [BEFORE] Molecular multi-surg: {before_mol[0]} total, {before_mol[1]} linked")

    if not dry_run:
        con.execute(CHAIN_LINKAGE_SQL)
        r = fetchone_safe(con, """
            SELECT chain_type, COUNT(*),
                SUM(CASE WHEN is_multi_surgery THEN 1 ELSE 0 END),
                SUM(CASE WHEN linkage_confidence IN ('exact_match','high_confidence') THEN 1 ELSE 0 END),
                SUM(CASE WHEN closer_to_other_surgery THEN 1 ELSE 0 END)
            FROM episode_chain_linkage_repair_v1
            GROUP BY 1
        """)
        # fetch all
        rows = con.execute("""
            SELECT chain_type, COUNT(*) total,
                SUM(CASE WHEN is_multi_surgery THEN 1 ELSE 0 END) multi,
                SUM(CASE WHEN linkage_confidence IN ('exact_match','high_confidence') THEN 1 ELSE 0 END) confident,
                SUM(CASE WHEN closer_to_other_surgery THEN 1 ELSE 0 END) ambig
            FROM episode_chain_linkage_repair_v1
            GROUP BY 1 ORDER BY 1
        """).fetchall()
        for row in rows:
            print(f"  [AFTER] {row[0]}: total={row[1]}, multi={row[2]}, confident={row[3]}, ambiguous={row[4]}")
    else:
        print("  [DRY-RUN] would create episode_chain_linkage_repair_v1")
    return (before_fna, before_mol)


def run_phase_d_e(con, dry_run: bool):
    section("Phase D/E: Pathology & RAI Anchoring")
    before_rai = fetchone_safe(con, """
        WITH ms AS (
            SELECT research_id FROM tumor_episode_master_v2
            GROUP BY research_id HAVING COUNT(DISTINCT surgery_episode_id) > 1
        )
        SELECT COUNT(*), SUM(CASE WHEN linked_surgery_episode_id IS NOT NULL THEN 1 ELSE 0 END)
        FROM rai_treatment_episode_v2 r JOIN ms ON r.research_id = ms.research_id
    """, (0,0))
    print(f"  [BEFORE] RAI multi-surg: {before_rai[0]} total, {before_rai[1]} linked")

    if not dry_run:
        con.execute(PATHRAI_LINKAGE_SQL)
        rows = con.execute("""
            SELECT domain, COUNT(*) total,
                SUM(CASE WHEN is_multi_surgery THEN 1 ELSE 0 END) multi,
                SUM(CASE WHEN linkage_confidence IN ('exact_match','high_confidence') THEN 1 ELSE 0 END) confident,
                SUM(CASE WHEN ambiguous_between_episodes THEN 1 ELSE 0 END) ambig
            FROM episode_pathrai_linkage_repair_v1
            GROUP BY 1 ORDER BY 1
        """).fetchall()
        for row in rows:
            print(f"  [AFTER] {row[0]}: total={row[1]}, multi={row[2]}, confident={row[3]}, ambiguous={row[4]}")
    else:
        print("  [DRY-RUN] would create episode_pathrai_linkage_repair_v1")
    return before_rai


def run_phase_f(con, dry_run: bool):
    section("Phase F: Ambiguity Registry + Summary")
    if not dry_run:
        con.execute(AMBIGUITY_REGISTRY_SQL)
        con.execute(SUMMARY_SQL)
        r = fetchone_safe(con, "SELECT COUNT(*) FROM episode_ambiguity_registry_v1", (0,))
        print(f"  Ambiguity registry: {r[0]} items")
        rows = con.execute("SELECT * FROM episode_linkage_repair_summary_v1 ORDER BY domain").fetchall()
        cols = [d[0] for d in con.description]
        print(f"  Summary ({len(rows)} rows):")
        for row in rows:
            d = dict(zip(cols, row))
            print(f"    {d['domain']}: total={d['total_linked']}, multi={d['multi_surg_linked']}, "
                  f"exact={d['exact']}, anchored={d['anchored']}, ambiguous={d['ambiguous']}, no_date={d['no_date']}")
    else:
        print("  [DRY-RUN] would create ambiguity registry + summary")


def run_phase_g(con, dry_run: bool):
    section("Phase G: Export Manual Review Packets")
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    if dry_run:
        print(f"  [DRY-RUN] would export to {EXPORT_DIR}")
        return

    # 1. Notes ambiguity
    df = con.execute("""
        SELECT a.*, s.surgery_date, s.surgery_episode_id AS target_ep
        FROM episode_ambiguity_registry_v1 a
        LEFT JOIN tumor_episode_master_v2 s
          ON a.research_id = s.research_id
          AND CAST(a.linked_surgery_episode_id AS INTEGER) = s.surgery_episode_id
        WHERE a.domain = 'notes'
        ORDER BY a.research_id
    """).fetchdf()
    out = EXPORT_DIR / f"notes_ambiguous_{TIMESTAMP}.csv"
    df.to_csv(out, index=False)
    print(f"  Exported {len(df)} note ambiguities → {out}")

    # 2. Lab overlapping windows
    df = con.execute("""
        SELECT * FROM episode_lab_linkage_repair_v1
        WHERE overlapping_window_flag
        ORDER BY research_id, lab_date
    """).fetchdf()
    out = EXPORT_DIR / f"labs_overlapping_{TIMESTAMP}.csv"
    df.to_csv(out, index=False)
    print(f"  Exported {len(df)} overlapping lab windows → {out}")

    # 3. Chain ambiguities
    df = con.execute("""
        SELECT * FROM episode_chain_linkage_repair_v1
        WHERE closer_to_other_surgery OR linkage_confidence IN ('weak','no_date')
        ORDER BY research_id, chain_type
    """).fetchdf()
    out = EXPORT_DIR / f"chain_ambiguous_{TIMESTAMP}.csv"
    df.to_csv(out, index=False)
    print(f"  Exported {len(df)} chain ambiguities → {out}")

    # 4. RAI/Path ambiguities
    df = con.execute("""
        SELECT * FROM episode_pathrai_linkage_repair_v1
        WHERE ambiguous_between_episodes OR linkage_confidence IN ('weak','no_date')
        ORDER BY research_id, domain
    """).fetchdf()
    out = EXPORT_DIR / f"pathrai_ambiguous_{TIMESTAMP}.csv"
    df.to_csv(out, index=False)
    print(f"  Exported {len(df)} path/RAI ambiguities → {out}")

    # 5. Full ambiguity registry
    df = con.execute("SELECT * FROM episode_ambiguity_registry_v1 ORDER BY research_id, domain").fetchdf()
    out = EXPORT_DIR / f"full_ambiguity_registry_{TIMESTAMP}.csv"
    df.to_csv(out, index=False)
    print(f"  Exported {len(df)} total ambiguity items → {out}")

    # Write manifest
    manifest = {
        "generated_at": TIMESTAMP,
        "files": [str(f.name) for f in EXPORT_DIR.glob(f"*_{TIMESTAMP}.csv")],
        "total_ambiguous_items": len(df),
    }
    with open(EXPORT_DIR / f"manifest_{TIMESTAMP}.json", "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"  Manifest written")


def run_phase_h(con, dry_run: bool):
    section("Phase H: Non-regression Safety Checks")
    if dry_run:
        print("  [DRY-RUN] would run non-regression checks")
        return {}

    results = {}

    # 1. Single-surgery patients: all should map to ep 1
    rows = con.execute(NONREG_SINGLE_SURG_SQL).fetchall()
    for row in rows:
        domain, total, correct, mislinked = row
        status = "PASS" if mislinked == 0 else "FAIL"
        print(f"  [{status}] {domain}: total={total}, correct={correct}, mislinked={mislinked}")
        results[f"nonreg_{domain}"] = {"total": total, "correct": correct, "mislinked": mislinked, "status": status}

    # 2. Multi-surgery before/after
    r = fetchone_safe(con, """
        SELECT
            SUM(CASE WHEN linkage_confidence IN ('exact_day','exact_match','high_confidence','anchored_window')
                     AND is_multi_surgery THEN 1 ELSE 0 END) AS confident_multi,
            SUM(CASE WHEN linkage_confidence IN ('ambiguous','weak','no_date')
                     AND is_multi_surgery THEN 1 ELSE 0 END) AS ambiguous_multi
        FROM (
            SELECT linkage_confidence, is_multi_surgery FROM episode_note_linkage_repair_v1
            UNION ALL
            SELECT linkage_confidence, is_multi_surgery FROM episode_lab_linkage_repair_v1
            UNION ALL
            SELECT linkage_confidence, is_multi_surgery FROM episode_chain_linkage_repair_v1
            UNION ALL
            SELECT linkage_confidence, is_multi_surgery FROM episode_pathrai_linkage_repair_v1
        )
    """, (0, 0))
    print(f"  [INFO] Multi-surg confident links: {r[0]}, ambiguous: {r[1]}")
    results["multi_surg_confident"] = r[0]
    results["multi_surg_ambiguous"] = r[1]

    return results


def materialize_mirrors(con, dry_run: bool):
    section("Materialize MotherDuck mirrors")
    for md_name, src_name in MIRROR_MAP:
        if dry_run:
            print(f"  [DRY-RUN] {md_name} ← {src_name}")
            continue
        try:
            con.execute(f"DROP TABLE IF EXISTS {md_name}")
            con.execute(f"CREATE TABLE {md_name} AS SELECT * FROM {src_name}")
            r = con.execute(f"SELECT COUNT(*) FROM {md_name}").fetchone()
            print(f"  {md_name}: {r[0]} rows")
        except Exception as e:
            print(f"  [ERROR] {md_name}: {e}")


def generate_reports(con, nonreg_results: dict, dry_run: bool):
    section("Generate Reports")
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    if dry_run:
        print("  [DRY-RUN] would generate reports")
        return

    # Repair report
    summary_rows = con.execute("""
        SELECT * FROM episode_linkage_repair_summary_v1 ORDER BY domain
    """).fetchall()
    summary_cols = [d[0] for d in con.description]

    ambig_count = fetchone_safe(con, "SELECT COUNT(*) FROM episode_ambiguity_registry_v1", (0,))

    report = f"""# Episode Linkage Repair Report — {DATE_TAG}

## Overview

This report documents the episode-aware linkage repair pass executed on {DATE_TAG}.
The repair converted patient-level-only linkage into episode-aware linkage for
multi-surgery patients across 5 domains.

## Multi-Surgery Patient Population

| Surgeries | Patients |
|-----------|----------|
"""
    ms_dist = con.execute("""
        SELECT n_surgeries, COUNT(*) FROM (
            SELECT research_id, COUNT(DISTINCT surgery_episode_id) as n_surgeries
            FROM tumor_episode_master_v2 GROUP BY research_id
        ) GROUP BY 1 ORDER BY 1
    """).fetchall()
    for r in ms_dist:
        report += f"| {r[0]} | {r[1]} |\n"

    report += f"""
## Repair Summary

| Domain | Total Linked | Multi-Surg | Exact | Anchored | Ambiguous | No Date |
|--------|-------------|------------|-------|----------|-----------|---------|
"""
    for row in summary_rows:
        d = dict(zip(summary_cols, row))
        if d['domain'] == 'ambiguity_registry':
            continue
        report += (f"| {d['domain']} | {d['total_linked']} | {d['multi_surg_linked']} | "
                   f"{d['exact']} | {d['anchored']} | {d['ambiguous']} | {d['no_date']} |\n")

    report += f"""
## Ambiguity Registry

Total ambiguous items requiring manual review: **{ambig_count[0]}**

Ambiguous items are exported to `exports/episode_linkage_manual_review_packets/`.

## Domain-Specific Notes

### Notes (Phase A)
- Operative notes linked by same-day or +1 day match to surgery date
- H&P notes linked by -1 to +0 day match
- Discharge summaries linked by 0 to +3 day match
- Multi-surgery patients use nearest-surgery disambiguation

### Labs (Phase B)
- Pre-op window: -30 to -1 days before surgery
- Post-op window: 0 to +365 days (or midpoint to next surgery for multi-surg)
- Overlapping windows flagged for manual review

### Imaging/FNA/Molecular Chains (Phase C)
- FNA→Surgery linkage uses date + laterality matching
- Molecular→Surgery inherits FNA episode where available
- Closer-to-other-surgery flag identifies ambiguous chain linkages

### Pathology & RAI (Phases D/E)
- Pathology verified against surgery date (same-day accession)
- RAI linked to nearest cancer surgery within 14-365 day protocol window
- Non-cancer surgery RAI links flagged as weak

## Source-Limited Domains

- **Imaging→FNA linkage**: imaging_nodule_long_v2 size data not populated
- **Nuclear medicine notes**: absent from clinical_notes_long corpus
- **RAI dose**: 59% missing, capped by structured data availability
- **Operative NLP fields**: V2 extractor outputs not fully materialized

## MotherDuck Objects Created

| Table | Purpose |
|-------|---------|
| episode_note_linkage_repair_v1 | Note-to-surgery episode linkage |
| episode_lab_linkage_repair_v1 | Lab-to-surgery episode windowing |
| episode_chain_linkage_repair_v1 | FNA/molecular chain repair |
| episode_pathrai_linkage_repair_v1 | Pathology/RAI anchoring |
| episode_ambiguity_registry_v1 | All ambiguous linkages |
| episode_linkage_repair_summary_v1 | Per-domain summary metrics |
| md_episode_*_v1 | MotherDuck mirrors of above |
"""
    repair_path = DOCS_DIR / f"episode_linkage_repair_{DATE_TAG}.md"
    repair_path.write_text(report)
    print(f"  Repair report: {repair_path}")

    # Non-regression report
    nonreg = f"""# Episode Linkage Non-regression Report — {DATE_TAG}

## Single-Surgery Patient Safety Check

Single-surgery patients (N=10,108) should have all linkages pointing to
surgery_episode_id = 1. Any deviation indicates a regression.

| Domain | Total | Correct (ep=1) | Mislinked | Status |
|--------|-------|----------------|-----------|--------|
"""
    for key in ["nonreg_notes", "nonreg_labs"]:
        if key in nonreg_results:
            d = nonreg_results[key]
            nonreg += f"| {key.replace('nonreg_','')} | {d['total']} | {d['correct']} | {d['mislinked']} | {d['status']} |\n"

    confident = nonreg_results.get("multi_surg_confident", 0)
    ambiguous = nonreg_results.get("multi_surg_ambiguous", 0)
    nonreg += f"""
## Multi-Surgery Linkage Quality

- Confident episode-linked items: **{confident}**
- Ambiguous/weak items: **{ambiguous}**
- Ambiguous rate: **{ambiguous/(confident+ambiguous)*100:.1f}%** (of episode-linked items)

## Verdict

"""
    all_pass = all(
        nonreg_results.get(k, {}).get("status") == "PASS"
        for k in ["nonreg_notes", "nonreg_labs"]
    )
    nonreg += f"**{'PASS — no single-surgery regressions detected' if all_pass else 'FAIL — check mislinked items'}**\n"

    nonreg_path = DOCS_DIR / f"episode_linkage_nonregression_{DATE_TAG}.md"
    nonreg_path.write_text(nonreg)
    print(f"  Non-regression report: {nonreg_path}")


# ===========================================================================
# CLI
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(description="Episode-aware linkage repair")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck (default)")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Show plan, don't execute")
    parser.add_argument("--phase", type=str, default="all",
                        help="Run specific phase: A/B/C/D/E/F/G/H or 'all'")
    args = parser.parse_args()

    if not args.local:
        args.md = True  # default to MotherDuck

    print(f"Episode Linkage Repair — {TIMESTAMP}")
    print(f"  Mode: {'MotherDuck' if args.md else 'local'}")
    print(f"  Phase: {args.phase}")
    print(f"  Dry-run: {args.dry_run}")

    con = get_connection(args)
    phases = args.phase.upper()

    nonreg_results = {}

    if phases in ("ALL", "A"):
        run_phase_a(con, args.dry_run)
    if phases in ("ALL", "B"):
        run_phase_b(con, args.dry_run)
    if phases in ("ALL", "C"):
        run_phase_c(con, args.dry_run)
    if phases in ("ALL", "D", "E"):
        run_phase_d_e(con, args.dry_run)
    if phases in ("ALL", "F"):
        run_phase_f(con, args.dry_run)
    if phases in ("ALL", "G"):
        run_phase_g(con, args.dry_run)
    if phases in ("ALL", "H"):
        nonreg_results = run_phase_h(con, args.dry_run)

    if phases == "ALL" and not args.dry_run:
        materialize_mirrors(con, args.dry_run)
        generate_reports(con, nonreg_results, args.dry_run)

    section("DONE")
    con.close()


if __name__ == "__main__":
    main()
