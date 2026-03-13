#!/usr/bin/env python3
"""
47_mrn_crosswalk_demographics_v3.py — Permanent MRN Crosswalk + Demographics v3

Institutionalizes the EUH_MRN crosswalk as a permanent, reproducible
materialized table, builds a linkage_master_v1 research_id → canonical_research_id
mapping, and rebuilds demographics_harmonized_v3 through the crosswalk with
orphan flagging, edge-case rescue, and full traceability.

Tables created:
  mrn_crosswalk_v1              — all MRN ↔ research_id pairs across 4 raw sources
  linkage_master_v1             — research_id → canonical_research_id mapping
  demographics_harmonized_v3    — cross-source demographics with orphan flags
  qa_missing_demographics_v3    — residual gaps post-v3

Usage:
  python scripts/47_mrn_crosswalk_demographics_v3.py --md
  python scripts/47_mrn_crosswalk_demographics_v3.py --local
  python scripts/47_mrn_crosswalk_demographics_v3.py --md --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import textwrap
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("mrn_crosswalk_v3")

ROOT = Path(__file__).resolve().parent.parent
MD_DATABASE = "thyroid_research_2026"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 1: mrn_crosswalk_v1 — Permanent MRN spine from all raw sources
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MRN_CROSSWALK_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE mrn_crosswalk_v1 AS
    WITH raw_mrn_pairs AS (
        -- Source 1: raw_path_synoptics (9,489 patients, has Gender/Race/DOB)
        SELECT
            TRY_CAST("Research ID number" AS INT) AS research_id,
            TRIM(CAST("EUH_MRN" AS VARCHAR))      AS euh_mrn,
            TRIM(CAST("TEC_MRN" AS VARCHAR))       AS tec_mrn,
            TRY_CAST("DOB" AS DATE)                AS dob,
            UPPER(TRIM(CAST("PATIENT_FIRST_NM" AS VARCHAR))) AS first_name,
            UPPER(TRIM(CAST("PATIENT_LAST_NM"  AS VARCHAR))) AS last_name,
            CAST("Gender" AS VARCHAR)              AS gender_raw,
            CAST("Race" AS VARCHAR)                AS race_raw,
            'raw_path_synoptics' AS source_table
        FROM raw_path_synoptics
        WHERE "EUH_MRN" IS NOT NULL
          AND TRIM(CAST("EUH_MRN" AS VARCHAR)) NOT IN ('', 'nan', 'None')

        UNION ALL

        -- Source 2: raw_clinical_notes (9,484 patients, has DOB)
        SELECT
            TRY_CAST("Research ID number" AS INT),
            TRIM(CAST("EUH_MRN" AS VARCHAR)),
            TRIM(CAST("TEC_MRN" AS VARCHAR)),
            TRY_CAST("DOB" AS DATE),
            UPPER(TRIM(CAST("PATIENT_FIRST_NM" AS VARCHAR))),
            UPPER(TRIM(CAST("PATIENT_LAST_NM"  AS VARCHAR))),
            NULL,
            NULL,
            'raw_clinical_notes'
        FROM raw_clinical_notes
        WHERE "EUH_MRN" IS NOT NULL
          AND TRIM(CAST("EUH_MRN" AS VARCHAR)) NOT IN ('', 'nan', 'None')

        UNION ALL

        -- Source 3: raw_complications (9,488 patients, has Name)
        SELECT
            TRY_CAST("Research ID number" AS INT),
            TRIM(CAST("EUH_MRN" AS VARCHAR)),
            TRIM(CAST("TEC_MRN" AS VARCHAR)),
            NULL,
            UPPER(TRIM(CAST("PATIENT_FIRST_NM" AS VARCHAR))),
            UPPER(TRIM(CAST("PATIENT_LAST_NM"  AS VARCHAR))),
            NULL,
            NULL,
            'raw_complications'
        FROM raw_complications
        WHERE "EUH_MRN" IS NOT NULL
          AND TRIM(CAST("EUH_MRN" AS VARCHAR)) NOT IN ('', 'nan', 'None')

        UNION ALL

        -- Source 4: raw_operative_details (7,941 patients, has DOB)
        SELECT
            TRY_CAST("Research ID number" AS INT),
            TRIM(CAST("EUH_MRN" AS VARCHAR)),
            TRIM(CAST("TEC_MRN" AS VARCHAR)),
            TRY_CAST("Date of birth" AS DATE),
            UPPER(TRIM(CAST("PATIENT_LAST_NM"  AS VARCHAR))),
            UPPER(TRIM(CAST("PATIENT_FIRST_NM" AS VARCHAR))),
            NULL,
            NULL,
            'raw_operative_details'
        FROM raw_operative_details
        WHERE "EUH_MRN" IS NOT NULL
          AND TRIM(CAST("EUH_MRN" AS VARCHAR)) NOT IN ('', 'nan', 'None')
    ),

    -- Deduplicate to one row per (research_id, euh_mrn, source_table)
    deduped AS (
        SELECT
            research_id,
            euh_mrn,
            tec_mrn,
            dob,
            first_name,
            last_name,
            gender_raw,
            race_raw,
            source_table,
            ROW_NUMBER() OVER (
                PARTITION BY research_id, euh_mrn, source_table
                ORDER BY dob NULLS LAST, gender_raw NULLS LAST
            ) AS rn
        FROM raw_mrn_pairs
        WHERE research_id IS NOT NULL
          AND research_id > 0
    ),

    -- Per-MRN: count data volume per research_id to pick canonical
    mrn_rid_volume AS (
        SELECT
            euh_mrn,
            research_id,
            COUNT(DISTINCT source_table) AS n_sources,
            MAX(CASE WHEN dob IS NOT NULL THEN 1 ELSE 0 END) AS has_dob,
            MAX(CASE WHEN gender_raw IS NOT NULL
                      AND TRIM(gender_raw) NOT IN ('', 'nan') THEN 1 ELSE 0 END) AS has_gender,
            MAX(CASE WHEN race_raw IS NOT NULL
                      AND TRIM(race_raw) NOT IN ('', 'nan') THEN 1 ELSE 0 END) AS has_race,
            BOOL_OR(source_table = 'raw_path_synoptics') AS in_path_synoptics
        FROM deduped WHERE rn = 1
        GROUP BY euh_mrn, research_id
    ),

    -- Pick canonical research_id per MRN: prefer path_synoptics, then highest volume
    canonical AS (
        SELECT
            euh_mrn,
            research_id AS canonical_research_id,
            ROW_NUMBER() OVER (
                PARTITION BY euh_mrn
                ORDER BY
                    in_path_synoptics DESC,
                    (has_dob + has_gender + has_race) DESC,
                    n_sources DESC,
                    research_id ASC
            ) AS canon_rank
        FROM mrn_rid_volume
    ),

    -- Build final crosswalk: one row per (research_id, euh_mrn)
    final AS (
        SELECT DISTINCT
            d.research_id,
            d.euh_mrn,
            d.tec_mrn,
            c.canonical_research_id,
            d.dob,
            d.first_name,
            d.last_name,
            d.gender_raw,
            d.race_raw,
            LIST_DISTINCT(LIST(d.source_table)) AS source_tables,
            CASE
                WHEN d.research_id = c.canonical_research_id THEN 'direct'
                ELSE 'mrn_crosswalk'
            END AS linkage_method,
            CASE
                WHEN d.research_id = c.canonical_research_id THEN 1.0
                ELSE 0.95
            END AS confidence
        FROM deduped d
        JOIN canonical c ON d.euh_mrn = c.euh_mrn AND c.canon_rank = 1
        WHERE d.rn = 1
        GROUP BY
            d.research_id, d.euh_mrn, d.tec_mrn,
            c.canonical_research_id, d.dob, d.first_name, d.last_name,
            d.gender_raw, d.race_raw,
            CASE WHEN d.research_id = c.canonical_research_id THEN 'direct' ELSE 'mrn_crosswalk' END,
            CASE WHEN d.research_id = c.canonical_research_id THEN 1.0 ELSE 0.95 END
    )

    SELECT
        research_id,
        euh_mrn,
        tec_mrn,
        canonical_research_id,
        dob,
        first_name,
        last_name,
        gender_raw,
        race_raw,
        source_tables,
        linkage_method,
        confidence
    FROM final
    ORDER BY euh_mrn, research_id
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 2: linkage_master_v1 — Single source of truth for RID mapping
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LINKAGE_MASTER_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE linkage_master_v1 AS
    WITH patient_spine AS (
        SELECT DISTINCT CAST(research_id AS INT) AS research_id
        FROM master_cohort
        WHERE research_id IS NOT NULL
    ),

    -- Crosswalk entries: group by research_id, pick best canonical
    xwalk AS (
        SELECT
            research_id,
            canonical_research_id,
            euh_mrn,
            linkage_method,
            confidence,
            ROW_NUMBER() OVER (
                PARTITION BY research_id
                ORDER BY confidence DESC, canonical_research_id ASC
            ) AS rn
        FROM mrn_crosswalk_v1
    ),

    -- Old stg_mrn_crosswalk_demographics entries (for backward compat)
    old_xwalk AS (
        SELECT
            research_id,
            ad_research_id AS canonical_research_id,
            mrn AS euh_mrn,
            'mrn_crosswalk' AS linkage_method,
            0.95 AS confidence
        FROM stg_mrn_crosswalk_demographics
    )

    SELECT
        sp.research_id,
        COALESCE(xw.canonical_research_id, ox.canonical_research_id, sp.research_id)
            AS canonical_research_id,
        COALESCE(xw.euh_mrn, ox.euh_mrn) AS euh_mrn,
        CASE
            WHEN xw.linkage_method = 'mrn_crosswalk'
                 OR ox.linkage_method = 'mrn_crosswalk'
            THEN 'mrn_crosswalk'
            WHEN xw.linkage_method = 'direct' THEN 'direct'
            ELSE 'identity'
        END AS linkage_method,
        COALESCE(xw.confidence, ox.confidence, 1.0) AS confidence,
        (xw.research_id IS NOT NULL OR ox.research_id IS NOT NULL) AS has_mrn
    FROM patient_spine sp
    LEFT JOIN xwalk xw ON sp.research_id = xw.research_id AND xw.rn = 1
    LEFT JOIN old_xwalk ox ON sp.research_id = ox.research_id
        AND xw.research_id IS NULL
    ORDER BY sp.research_id
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 3: demographics_harmonized_v3 — Full rebuild through crosswalk
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DEMOGRAPHICS_V3_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE demographics_harmonized_v3 AS
    WITH patient_spine AS (
        SELECT DISTINCT CAST(research_id AS INT) AS research_id
        FROM master_cohort
        WHERE research_id IS NOT NULL
    ),

    lm AS (
        SELECT research_id, canonical_research_id, euh_mrn,
               linkage_method, has_mrn
        FROM linkage_master_v1
    ),

    -- P1: benign_pathology
    bp AS (
        SELECT CAST(research_id AS INT) AS rid,
               FIRST(age_at_surgery ORDER BY surgery_date DESC NULLS LAST) AS age_at_surgery,
               FIRST(sex ORDER BY surgery_date DESC NULLS LAST) AS sex,
               FIRST(TRY_CAST(surgery_date AS DATE) ORDER BY surgery_date DESC NULLS LAST) AS surgery_date
        FROM benign_pathology
        WHERE age_at_surgery IS NOT NULL
        GROUP BY CAST(research_id AS INT)
    ),

    -- P2: tumor_pathology
    tp AS (
        SELECT CAST(research_id AS INT) AS rid,
               FIRST(age_at_surgery ORDER BY surgery_date DESC NULLS LAST) AS age_at_surgery,
               FIRST(sex ORDER BY surgery_date DESC NULLS LAST) AS sex
        FROM tumor_pathology
        WHERE age_at_surgery IS NOT NULL
        GROUP BY CAST(research_id AS INT)
    ),

    -- P3: path_synoptics (age/gender/race/surg_date)
    ps AS (
        SELECT CAST(research_id AS INT) AS rid,
               FIRST(TRY_CAST(CAST(age AS VARCHAR) AS INT)) AS age_ps,
               FIRST(CASE
                   WHEN LOWER(CAST(gender AS VARCHAR)) IN ('male', 'm') THEN 'Male'
                   WHEN LOWER(CAST(gender AS VARCHAR)) IN ('female', 'f') THEN 'Female'
                   ELSE NULL
               END) AS sex_ps,
               FIRST(CAST(race AS VARCHAR)) FILTER (
                   WHERE race IS NOT NULL AND TRIM(CAST(race AS VARCHAR)) != ''
               ) AS race_ps,
               FIRST(TRY_CAST(surg_date AS DATE)) AS surg_date_ps
        FROM path_synoptics
        GROUP BY CAST(research_id AS INT)
    ),

    -- P4: thyroid_weights DOB + surgery date
    tw AS (
        SELECT CAST(research_id AS INT) AS rid,
               FIRST(dob) AS dob_tw,
               FIRST(TRY_CAST(date_of_surgery AS DATE)) AS surg_date_tw
        FROM thyroid_weights
        WHERE dob IS NOT NULL
        GROUP BY CAST(research_id AS INT)
    ),

    -- P5: operative_details surgery date
    od AS (
        SELECT CAST(research_id AS INT) AS rid,
               FIRST(
                   COALESCE(
                       TRY_CAST(surg_date AS DATE),
                       TRY_STRPTIME(
                           REGEXP_EXTRACT(CAST(surg_date AS VARCHAR),
                                          '(\\d{1,2}/\\d{1,2}/\\d{4})', 1),
                           '%m/%d/%Y'
                       )::DATE
                   )
                   ORDER BY COALESCE(
                       TRY_CAST(surg_date AS DATE),
                       TRY_STRPTIME(
                           REGEXP_EXTRACT(CAST(surg_date AS VARCHAR),
                                          '(\\d{1,2}/\\d{1,2}/\\d{4})', 1),
                           '%m/%d/%Y'
                       )::DATE
                   )
               ) AS surg_date_od
        FROM operative_details
        GROUP BY CAST(research_id AS INT)
    ),

    -- P6: thyroglobulin_labs
    tg AS (
        SELECT CAST(research_id AS INT) AS rid,
               FIRST(TRY_CAST(dob AS DATE)) AS dob_tg,
               FIRST(CASE
                   WHEN LOWER(TRIM(CAST(gender AS VARCHAR))) IN ('male', 'm') THEN 'Male'
                   WHEN LOWER(TRIM(CAST(gender AS VARCHAR))) IN ('female', 'f') THEN 'Female'
                   ELSE NULL
               END) AS sex_tg,
               FIRST(CAST(race AS VARCHAR)) FILTER (
                   WHERE race IS NOT NULL AND TRIM(CAST(race AS VARCHAR)) != ''
               ) AS race_tg
        FROM thyroglobulin_labs
        GROUP BY CAST(research_id AS INT)
    ),

    -- P7: anti_thyroglobulin_labs
    atg AS (
        SELECT CAST(research_id AS INT) AS rid,
               FIRST(TRY_CAST(dob AS DATE)) AS dob_atg,
               FIRST(CASE
                   WHEN LOWER(TRIM(CAST(gender AS VARCHAR))) IN ('male', 'm') THEN 'Male'
                   WHEN LOWER(TRIM(CAST(gender AS VARCHAR))) IN ('female', 'f') THEN 'Female'
                   ELSE NULL
               END) AS sex_atg,
               FIRST(CAST(race AS VARCHAR)) FILTER (
                   WHERE race IS NOT NULL AND TRIM(CAST(race AS VARCHAR)) != ''
               ) AS race_atg
        FROM anti_thyroglobulin_labs
        GROUP BY CAST(research_id AS INT)
    ),

    -- P8: Cross-file Excel DOB recovery
    excel_dob AS (
        SELECT research_id AS rid,
               dob_resolved AS dob_excel,
               age_at_surgery AS age_excel,
               gender_excel AS sex_excel,
               race_excel,
               dob_resolution
        FROM stg_dob_excel_recovery
    ),

    -- P9: Old MRN crosswalk (stg_mrn_crosswalk_demographics, 570 patients)
    old_xwalk AS (
        SELECT research_id AS rid,
               sex AS sex_xwalk,
               race AS race_xwalk,
               age_at_surgery AS age_xwalk,
               dob AS dob_xwalk
        FROM stg_mrn_crosswalk_demographics
    ),

    -- P10 (NEW): Cross-MRN demographics recovery via mrn_crosswalk_v1
    -- For patients missing sex/race, find demographics from MRN-linked records
    crossmrn_gender AS (
        SELECT DISTINCT
            xw.research_id AS rid,
            FIRST(CASE
                WHEN LOWER(TRIM(ps_linked.gender_raw)) IN ('male', 'm') THEN 'Male'
                WHEN LOWER(TRIM(ps_linked.gender_raw)) IN ('female', 'f') THEN 'Female'
                ELSE NULL
            END) AS sex_crossmrn
        FROM mrn_crosswalk_v1 xw
        JOIN mrn_crosswalk_v1 ps_linked
            ON xw.euh_mrn = ps_linked.euh_mrn
            AND ps_linked.gender_raw IS NOT NULL
            AND TRIM(ps_linked.gender_raw) NOT IN ('', 'nan')
            AND ps_linked.research_id != xw.research_id
        GROUP BY xw.research_id
    ),

    crossmrn_race AS (
        SELECT DISTINCT
            xw.research_id AS rid,
            FIRST(CAST(ps_linked.race_raw AS VARCHAR)) FILTER (
                WHERE ps_linked.race_raw IS NOT NULL
                  AND TRIM(CAST(ps_linked.race_raw AS VARCHAR)) NOT IN ('', 'nan')
            ) AS race_crossmrn
        FROM mrn_crosswalk_v1 xw
        JOIN mrn_crosswalk_v1 ps_linked
            ON xw.euh_mrn = ps_linked.euh_mrn
            AND ps_linked.race_raw IS NOT NULL
            AND TRIM(CAST(ps_linked.race_raw AS VARCHAR)) NOT IN ('', 'nan')
            AND ps_linked.research_id != xw.research_id
        GROUP BY xw.research_id
    ),

    crossmrn_dob AS (
        SELECT DISTINCT
            xw.research_id AS rid,
            FIRST(ps_linked.dob) FILTER (WHERE ps_linked.dob IS NOT NULL) AS dob_crossmrn
        FROM mrn_crosswalk_v1 xw
        JOIN mrn_crosswalk_v1 ps_linked
            ON xw.euh_mrn = ps_linked.euh_mrn
            AND ps_linked.dob IS NOT NULL
            AND ps_linked.research_id != xw.research_id
        GROUP BY xw.research_id
    ),

    -- P11 (NEW): Note-date fallback for patients with DOB but no surgery date
    note_date_fallback AS (
        SELECT CAST(research_id AS INT) AS rid,
               MIN(TRY_CAST(note_date AS DATE)) AS earliest_note_date
        FROM clinical_notes_long
        WHERE note_date IS NOT NULL
        GROUP BY CAST(research_id AS INT)
    ),

    -- P12 (NEW): Lab specimen date fallback (for patients with DOB but no surgery/note)
    lab_date_fallback AS (
        SELECT CAST(research_id AS INT) AS rid,
               MIN(TRY_CAST(specimen_collect_dt AS DATE)) AS earliest_lab_date
        FROM thyroglobulin_labs
        WHERE specimen_collect_dt IS NOT NULL
        GROUP BY CAST(research_id AS INT)
    ),

    -- P13 (NEW): Canonical-RID demographics inheritance
    -- For MRN-crosswalk patients, pull demographics from canonical research_id
    canonical_demo AS (
        SELECT
            lm2.research_id AS rid,
            bp2.age_at_surgery AS age_canonical,
            COALESCE(bp2.sex, tp2.sex, ps2.sex_ps) AS sex_canonical,
            ps2.race_ps AS race_canonical,
            bp2.surgery_date AS surg_canonical
        FROM linkage_master_v1 lm2
        JOIN (
            SELECT CAST(research_id AS INT) AS rid,
                   FIRST(age_at_surgery ORDER BY surgery_date DESC NULLS LAST) AS age_at_surgery,
                   FIRST(sex ORDER BY surgery_date DESC NULLS LAST) AS sex,
                   FIRST(TRY_CAST(surgery_date AS DATE) ORDER BY surgery_date DESC NULLS LAST) AS surgery_date
            FROM benign_pathology WHERE age_at_surgery IS NOT NULL
            GROUP BY CAST(research_id AS INT)
        ) bp2 ON lm2.canonical_research_id = bp2.rid
        LEFT JOIN (
            SELECT CAST(research_id AS INT) AS rid,
                   FIRST(sex ORDER BY surgery_date DESC NULLS LAST) AS sex
            FROM tumor_pathology WHERE sex IS NOT NULL
            GROUP BY CAST(research_id AS INT)
        ) tp2 ON lm2.canonical_research_id = tp2.rid
        LEFT JOIN (
            SELECT CAST(research_id AS INT) AS rid,
                   FIRST(CASE WHEN LOWER(CAST(gender AS VARCHAR)) IN ('male','m') THEN 'Male'
                              WHEN LOWER(CAST(gender AS VARCHAR)) IN ('female','f') THEN 'Female'
                              ELSE NULL END) AS sex_ps,
                   FIRST(CAST(race AS VARCHAR)) FILTER (WHERE race IS NOT NULL AND TRIM(CAST(race AS VARCHAR)) != '') AS race_ps
            FROM path_synoptics GROUP BY CAST(research_id AS INT)
        ) ps2 ON lm2.canonical_research_id = ps2.rid
        WHERE lm2.research_id != lm2.canonical_research_id
    ),

    -- Orphan detection: patients with no data in ANY of 21 source tables
    orphan_check AS (
        SELECT sp.research_id,
            CASE WHEN
                bp.rid IS NULL AND tp.rid IS NULL AND ps.rid IS NULL
                AND tw.rid IS NULL AND od.rid IS NULL AND tg.rid IS NULL
                AND atg.rid IS NULL AND ex.rid IS NULL AND ox.rid IS NULL
                AND lm.has_mrn = FALSE
            THEN TRUE ELSE FALSE END AS is_orphan_flag
        FROM patient_spine sp
        LEFT JOIN bp ON sp.research_id = bp.rid
        LEFT JOIN tp ON sp.research_id = tp.rid
        LEFT JOIN ps ON sp.research_id = ps.rid
        LEFT JOIN tw ON sp.research_id = tw.rid
        LEFT JOIN od ON sp.research_id = od.rid
        LEFT JOIN tg ON sp.research_id = tg.rid
        LEFT JOIN atg ON sp.research_id = atg.rid
        LEFT JOIN excel_dob ex ON sp.research_id = ex.rid
        LEFT JOIN old_xwalk ox ON sp.research_id = ox.rid
        LEFT JOIN lm ON sp.research_id = lm.research_id
    ),

    harmonized AS (
        SELECT
            sp.research_id,
            lm.canonical_research_id,
            lm.linkage_method,
            lm.euh_mrn,
            oc.is_orphan_flag,

            -- Best surgery date
            COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od)
                AS best_surgery_date,

            -- Best DOB: resolved Excel > crosswalk > cross-MRN > thyroid_weights > labs
            COALESCE(ex.dob_excel, ox.dob_xwalk, cmd.dob_crossmrn, tw.dob_tw, tg.dob_tg, atg.dob_atg)
                AS best_dob,

            -- AGE: structured > Excel DOB-derived > old MRN crosswalk > cross-MRN DOB-derived > note-date fallback
            COALESCE(
                TRY_CAST(bp.age_at_surgery AS INT),
                TRY_CAST(tp.age_at_surgery AS INT),
                ps.age_ps,
                ex.age_excel,
                ox.age_xwalk,
                -- DOB-derived age using surgery date
                CASE WHEN COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg) IS NOT NULL
                      AND COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od) IS NOT NULL
                     THEN DATE_DIFF('year',
                              COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg),
                              COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od))
                          - CASE WHEN
                              MONTH(COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od))
                                < MONTH(COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg))
                              OR (MONTH(COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od))
                                  = MONTH(COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg))
                                  AND DAY(COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od))
                                    < DAY(COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg)))
                            THEN 1 ELSE 0 END
                     ELSE NULL
                END,
                -- DOB-derived age using note_date as last resort
                CASE WHEN COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg) IS NOT NULL
                      AND COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od) IS NULL
                      AND ndf.earliest_note_date IS NOT NULL
                     THEN DATE_DIFF('year',
                              COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg),
                              ndf.earliest_note_date)
                     ELSE NULL
                END,
                -- DOB-derived age using lab specimen date as last resort
                CASE WHEN COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg) IS NOT NULL
                      AND COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od) IS NULL
                      AND ndf.earliest_note_date IS NULL
                      AND ldf.earliest_lab_date IS NOT NULL
                     THEN DATE_DIFF('year',
                              COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg),
                              ldf.earliest_lab_date)
                     ELSE NULL
                END,
                -- Canonical-RID age inheritance for MRN-crosswalk patients
                TRY_CAST(cd.age_canonical AS INT)
            ) AS age_at_surgery,

            CASE
                WHEN bp.age_at_surgery IS NOT NULL THEN 'benign_pathology'
                WHEN tp.age_at_surgery IS NOT NULL THEN 'tumor_pathology'
                WHEN ps.age_ps IS NOT NULL          THEN 'path_synoptics'
                WHEN ex.age_excel IS NOT NULL        THEN 'excel_dob_' || COALESCE(ex.dob_resolution, 'resolved')
                WHEN ox.age_xwalk IS NOT NULL       THEN 'mrn_crosswalk_v0'
                WHEN COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg) IS NOT NULL
                     AND COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od) IS NOT NULL
                     THEN CASE
                         WHEN cmd.dob_crossmrn IS NOT NULL AND ox.dob_xwalk IS NULL
                              THEN 'crossmrn_dob_derived'
                         WHEN tw.dob_tw IS NOT NULL THEN 'thyroid_weights_dob'
                         WHEN tg.dob_tg IS NOT NULL THEN 'thyroglobulin_labs_dob'
                         ELSE 'anti_tg_labs_dob'
                     END
                WHEN COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg) IS NOT NULL
                     AND COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od) IS NULL
                     AND ndf.earliest_note_date IS NOT NULL
                     THEN 'note_date_fallback'
                WHEN COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg) IS NOT NULL
                     AND COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od) IS NULL
                     AND ndf.earliest_note_date IS NULL
                     AND ldf.earliest_lab_date IS NOT NULL
                     THEN 'lab_specimen_date_fallback'
                WHEN cd.age_canonical IS NOT NULL
                     THEN 'canonical_rid_inheritance'
                ELSE NULL
            END AS age_source,

            CASE
                WHEN cd.age_canonical IS NOT NULL
                     AND COALESCE(TRY_CAST(bp.age_at_surgery AS INT), TRY_CAST(tp.age_at_surgery AS INT),
                                  ps.age_ps, ex.age_excel, ox.age_xwalk) IS NULL
                     THEN 'canonical_rid_inheritance'
                WHEN COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg) IS NOT NULL
                     AND COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od) IS NULL
                     AND ndf.earliest_note_date IS NOT NULL
                     THEN 'note_date_fallback'
                WHEN COALESCE(ox.dob_xwalk, cmd.dob_crossmrn, ex.dob_excel, tw.dob_tw, tg.dob_tg, atg.dob_atg) IS NOT NULL
                     AND COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od) IS NULL
                     AND ldf.earliest_lab_date IS NOT NULL
                     THEN 'lab_specimen_date_fallback'
                WHEN COALESCE(bp.surgery_date, ps.surg_date_ps, tw.surg_date_tw, od.surg_date_od) IS NOT NULL
                     THEN 'surgery_date'
                ELSE NULL
            END AS age_derivation_method,

            -- SEX: structured > Excel > old crosswalk > canonical > cross-MRN > labs
            COALESCE(bp.sex, tp.sex, ps.sex_ps, ex.sex_excel, ox.sex_xwalk,
                     cd.sex_canonical, cmg.sex_crossmrn, tg.sex_tg, atg.sex_atg) AS sex,

            CASE
                WHEN bp.sex IS NOT NULL       THEN 'benign_pathology'
                WHEN tp.sex IS NOT NULL       THEN 'tumor_pathology'
                WHEN ps.sex_ps IS NOT NULL    THEN 'path_synoptics'
                WHEN ex.sex_excel IS NOT NULL THEN 'excel_all_diagnoses'
                WHEN ox.sex_xwalk IS NOT NULL THEN 'mrn_crosswalk_v0'
                WHEN cd.sex_canonical IS NOT NULL THEN 'canonical_rid_inheritance'
                WHEN cmg.sex_crossmrn IS NOT NULL THEN 'crossmrn_recovery'
                WHEN tg.sex_tg IS NOT NULL    THEN 'thyroglobulin_labs'
                WHEN atg.sex_atg IS NOT NULL  THEN 'anti_tg_labs'
                ELSE NULL
            END AS sex_source,

            -- RACE: path_synoptics > Excel > old crosswalk > canonical > cross-MRN > labs
            COALESCE(ps.race_ps, ex.race_excel, ox.race_xwalk,
                     cd.race_canonical, cmr.race_crossmrn, tg.race_tg, atg.race_atg) AS race,

            CASE
                WHEN ps.race_ps IS NOT NULL     THEN 'path_synoptics'
                WHEN ex.race_excel IS NOT NULL  THEN 'excel_all_diagnoses'
                WHEN ox.race_xwalk IS NOT NULL  THEN 'mrn_crosswalk_v0'
                WHEN cd.race_canonical IS NOT NULL THEN 'canonical_rid_inheritance'
                WHEN cmr.race_crossmrn IS NOT NULL THEN 'crossmrn_recovery'
                WHEN tg.race_tg IS NOT NULL     THEN 'thyroglobulin_labs'
                WHEN atg.race_atg IS NOT NULL   THEN 'anti_tg_labs'
                ELSE NULL
            END AS race_source,

            -- Provenance
            COALESCE(lm.linkage_method, 'identity') AS demographics_linkage_method

        FROM patient_spine sp
        LEFT JOIN lm          ON sp.research_id = lm.research_id
        LEFT JOIN orphan_check oc ON sp.research_id = oc.research_id
        LEFT JOIN bp          ON sp.research_id = bp.rid
        LEFT JOIN tp          ON sp.research_id = tp.rid
        LEFT JOIN ps          ON sp.research_id = ps.rid
        LEFT JOIN tw          ON sp.research_id = tw.rid
        LEFT JOIN od          ON sp.research_id = od.rid
        LEFT JOIN tg          ON sp.research_id = tg.rid
        LEFT JOIN atg         ON sp.research_id = atg.rid
        LEFT JOIN excel_dob ex ON sp.research_id = ex.rid
        LEFT JOIN old_xwalk ox ON sp.research_id = ox.rid
        LEFT JOIN crossmrn_gender cmg ON sp.research_id = cmg.rid
        LEFT JOIN crossmrn_race  cmr ON sp.research_id = cmr.rid
        LEFT JOIN crossmrn_dob   cmd ON sp.research_id = cmd.rid
        LEFT JOIN note_date_fallback ndf ON sp.research_id = ndf.rid
        LEFT JOIN lab_date_fallback ldf ON sp.research_id = ldf.rid
        LEFT JOIN canonical_demo cd ON sp.research_id = cd.rid
    )

    SELECT
        research_id,
        canonical_research_id,
        linkage_method,
        euh_mrn,
        is_orphan_flag,
        age_at_surgery,
        age_source,
        age_derivation_method,
        sex,
        sex_source,
        race,
        race_source,
        demographics_linkage_method,
        best_surgery_date,
        best_dob
    FROM harmonized
    ORDER BY research_id
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 4: qa_missing_demographics_v3
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

QA_MISSING_V3_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE qa_missing_demographics_v3 AS
    SELECT
        research_id,
        canonical_research_id,
        linkage_method,
        is_orphan_flag,
        age_at_surgery,
        age_source,
        age_derivation_method,
        sex,
        sex_source,
        race,
        race_source,
        CASE WHEN age_at_surgery IS NULL THEN 'MISSING_AGE' ELSE 'OK' END AS age_flag,
        CASE WHEN sex IS NULL THEN 'MISSING_SEX' ELSE 'OK' END AS sex_flag,
        CASE WHEN race IS NULL THEN 'MISSING_RACE' ELSE 'OK' END AS race_flag,
        COALESCE(age_source, 'none') || '+' ||
        COALESCE(sex_source, 'none') || '+' ||
        COALESCE(race_source, 'none') AS source_priority
    FROM demographics_harmonized_v3
    WHERE age_at_surgery IS NULL
       OR sex IS NULL
       OR race IS NULL
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_connection(use_md: bool):
    import duckdb
    if use_md:
        token = os.getenv("MOTHERDUCK_TOKEN")
        if not token:
            try:
                import toml
                token = toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
            except Exception:
                pass
        if not token:
            raise RuntimeError("MOTHERDUCK_TOKEN not set")
        return duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={token}")
    else:
        db_path = ROOT / "thyroid_master.duckdb"
        return duckdb.connect(str(db_path))


def timed_execute(con, sql: str, label: str) -> float:
    t0 = time.perf_counter()
    con.execute(sql)
    elapsed = time.perf_counter() - t0
    log.info(f"  {label}: {elapsed:.2f}s")
    return elapsed


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Verification suite
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_verification(con) -> dict:
    results = {}

    # Coverage
    row = con.execute("""
    SELECT
        COUNT(DISTINCT research_id) AS total_patients,
        SUM(CASE WHEN age_at_surgery IS NULL THEN 1 ELSE 0 END) AS missing_age,
        SUM(CASE WHEN sex IS NULL THEN 1 ELSE 0 END) AS missing_sex,
        SUM(CASE WHEN race IS NULL THEN 1 ELSE 0 END) AS missing_race,
        SUM(CASE WHEN is_orphan_flag THEN 1 ELSE 0 END) AS true_orphans,
        COUNT(CASE WHEN linkage_method = 'mrn_crosswalk' THEN 1 END) AS mrn_linked,
        COUNT(CASE WHEN demographics_linkage_method = 'crossmrn_recovery'
                    OR age_source = 'crossmrn_dob_derived'
                    OR sex_source = 'crossmrn_recovery'
                    OR race_source = 'crossmrn_recovery' THEN 1 END) AS recovered_via_mrn
    FROM demographics_harmonized_v3
    """).fetchone()
    results["total"] = row[0]
    results["missing_age"] = row[1]
    results["missing_sex"] = row[2]
    results["missing_race"] = row[3]
    results["orphans"] = row[4]
    results["mrn_linked"] = row[5]
    results["recovered_via_mrn"] = row[6]

    log.info("\n  ═══ DEMOGRAPHICS v3 COVERAGE ═══")
    log.info(f"  Total patients:     {results['total']:,}")
    log.info(f"  Missing age:        {results['missing_age']:,}  "
             f"({100*(results['total']-results['missing_age'])/results['total']:.2f}% coverage)")
    log.info(f"  Missing sex:        {results['missing_sex']:,}  "
             f"({100*(results['total']-results['missing_sex'])/results['total']:.2f}% coverage)")
    log.info(f"  Missing race:       {results['missing_race']:,}  "
             f"({100*(results['total']-results['missing_race'])/results['total']:.2f}% coverage)")
    log.info(f"  True orphans:       {results['orphans']:,}")
    log.info(f"  MRN-linked:         {results['mrn_linked']:,}")
    log.info(f"  Recovered via MRN:  {results['recovered_via_mrn']:,}")

    # Non-orphan coverage
    non_orphan = results["total"] - results["orphans"]
    non_orphan_age = con.execute("""
        SELECT SUM(CASE WHEN age_at_surgery IS NULL THEN 1 ELSE 0 END)
        FROM demographics_harmonized_v3 WHERE NOT is_orphan_flag
    """).fetchone()[0]
    non_orphan_sex = con.execute("""
        SELECT SUM(CASE WHEN sex IS NULL THEN 1 ELSE 0 END)
        FROM demographics_harmonized_v3 WHERE NOT is_orphan_flag
    """).fetchone()[0]
    non_orphan_race = con.execute("""
        SELECT SUM(CASE WHEN race IS NULL THEN 1 ELSE 0 END)
        FROM demographics_harmonized_v3 WHERE NOT is_orphan_flag
    """).fetchone()[0]
    results["non_orphan_total"] = non_orphan
    results["non_orphan_missing_age"] = non_orphan_age
    results["non_orphan_missing_sex"] = non_orphan_sex
    results["non_orphan_missing_race"] = non_orphan_race

    log.info(f"\n  ═══ NON-ORPHAN COVERAGE ({non_orphan:,} patients) ═══")
    log.info(f"  Missing age:        {non_orphan_age:,}  "
             f"({100*(non_orphan-non_orphan_age)/non_orphan:.2f}%)")
    log.info(f"  Missing sex:        {non_orphan_sex:,}  "
             f"({100*(non_orphan-non_orphan_sex)/non_orphan:.2f}%)")
    log.info(f"  Missing race:       {non_orphan_race:,}  "
             f"({100*(non_orphan-non_orphan_race)/non_orphan:.2f}%)")

    # Age source distribution
    log.info("\n  ═══ AGE SOURCE DISTRIBUTION ═══")
    rows = con.execute("""
        SELECT COALESCE(age_source, 'MISSING') AS src, COUNT(*) AS n
        FROM demographics_harmonized_v3
        GROUP BY 1 ORDER BY n DESC
    """).fetchall()
    for src, n in rows:
        log.info(f"    {src}: {n:,}")

    # Crosswalk stats
    log.info("\n  ═══ MRN CROSSWALK v1 STATS ═══")
    xw_row = con.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT research_id) AS distinct_rids,
            COUNT(DISTINCT euh_mrn) AS distinct_mrns,
            COUNT(CASE WHEN linkage_method = 'mrn_crosswalk' THEN 1 END) AS crosswalk_links,
            COUNT(DISTINCT canonical_research_id) AS canonical_rids
        FROM mrn_crosswalk_v1
    """).fetchone()
    log.info(f"    Total rows:       {xw_row[0]:,}")
    log.info(f"    Distinct RIDs:    {xw_row[1]:,}")
    log.info(f"    Distinct MRNs:    {xw_row[2]:,}")
    log.info(f"    Crosswalk links:  {xw_row[3]:,}")
    log.info(f"    Canonical RIDs:   {xw_row[4]:,}")

    # Linkage master stats
    log.info("\n  ═══ LINKAGE MASTER v1 ═══")
    lm_rows = con.execute("""
        SELECT linkage_method, COUNT(*) AS n
        FROM linkage_master_v1
        GROUP BY 1 ORDER BY n DESC
    """).fetchall()
    for method, n in lm_rows:
        log.info(f"    {method}: {n:,}")

    # Orphan list
    log.info("\n  ═══ TRUE ORPHAN LIST ═══")
    orphans = con.execute("""
        SELECT research_id, linkage_method, demographics_linkage_method
        FROM demographics_harmonized_v3
        WHERE is_orphan_flag = TRUE
        ORDER BY research_id
    """).fetchall()
    results["orphan_list"] = orphans
    for rid, lm, dlm in orphans:
        log.info(f"    RID={rid:>6d}  linkage={lm}  demo_method={dlm}")

    # Before/after comparison
    log.info("\n  ═══ BEFORE/AFTER COMPARISON ═══")
    v2 = con.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN age_at_surgery IS NULL THEN 1 ELSE 0 END) AS miss_age,
            SUM(CASE WHEN sex IS NULL THEN 1 ELSE 0 END) AS miss_sex,
            SUM(CASE WHEN race IS NULL THEN 1 ELSE 0 END) AS miss_race
        FROM demographics_harmonized_v2
    """).fetchone()
    log.info(f"  {'Metric':<20s} {'v2':>8s} {'v3':>8s} {'Delta':>8s}")
    log.info(f"  {'-'*48}")
    log.info(f"  {'Missing age':<20s} {v2[1]:>8,} {results['missing_age']:>8,} {results['missing_age']-v2[1]:>+8,}")
    log.info(f"  {'Missing sex':<20s} {v2[2]:>8,} {results['missing_sex']:>8,} {results['missing_sex']-v2[2]:>+8,}")
    log.info(f"  {'Missing race':<20s} {v2[3]:>8,} {results['missing_race']:>8,} {results['missing_race']-v2[3]:>+8,}")

    return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Permanent MRN Crosswalk + Demographics v3"
    )
    parser.add_argument("--md", action="store_true", help="Use MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only")
    args = parser.parse_args()

    use_md = args.md and not args.local
    if not args.md and not args.local:
        log.error("Specify --md or --local")
        sys.exit(1)

    log.info("=" * 72)
    log.info("  MRN CROSSWALK v1 + DEMOGRAPHICS v3")
    log.info(f"  Target: {'MotherDuck' if use_md else 'Local DuckDB'}")
    log.info("=" * 72)

    if args.dry_run:
        log.info("\n[DRY RUN] SQL statements:\n")
        for label, sql in [
            ("mrn_crosswalk_v1", MRN_CROSSWALK_SQL),
            ("linkage_master_v1", LINKAGE_MASTER_SQL),
            ("demographics_harmonized_v3", DEMOGRAPHICS_V3_SQL),
            ("qa_missing_demographics_v3", QA_MISSING_V3_SQL),
        ]:
            log.info(f"\n{'─'*40} {label} {'─'*40}")
            print(sql)
        return

    con = get_connection(use_md)

    try:
        log.info("\n  Phase 1: Building mrn_crosswalk_v1")
        timed_execute(con, MRN_CROSSWALK_SQL, "mrn_crosswalk_v1")

        log.info("\n  Phase 2: Building linkage_master_v1")
        timed_execute(con, LINKAGE_MASTER_SQL, "linkage_master_v1")

        log.info("\n  Phase 3: Building demographics_harmonized_v3")
        timed_execute(con, DEMOGRAPHICS_V3_SQL, "demographics_harmonized_v3")

        log.info("\n  Phase 4: Building qa_missing_demographics_v3")
        timed_execute(con, QA_MISSING_V3_SQL, "qa_missing_demographics_v3")

        log.info("\n  Phase 5: Verification")
        results = run_verification(con)

        log.info("\n" + "=" * 72)
        log.info("  MRN CROSSWALK + DEMOGRAPHICS v3 COMPLETE")
        log.info("=" * 72)

    finally:
        con.close()


if __name__ == "__main__":
    main()
