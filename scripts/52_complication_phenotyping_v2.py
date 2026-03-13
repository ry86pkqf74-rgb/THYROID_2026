#!/usr/bin/env python3
"""
52_complication_phenotyping_v2.py -- Structured thyroid complication phenotyping

Extends the Phase 2 complication refinement pipeline (complications_refined_pipeline.py)
with manuscript-grade phenotyping that explicitly classifies:
  - note_mention_flag
  - suspected_flag
  - confirmed_flag
  - transient_flag
  - permanent_flag
  - surgery_related_flag
  - historical_only_flag
  - timing_window relative to surgery
  - final_complication_status

Hypocalcemia/Hypoparathyroidism specific classification:
  - biochemical_only   : lab evidence (PTH<15 or Ca<8.0), no treatment
  - treatment_requiring: documented calcium/calcitriol supplements within 60d
  - transient          : normalised within 6 months
  - permanent          : persistent >6 months

RLN/Voice specific classification:
  - Preserves existing 3-tier from rln_refined_pipeline.py
  - Adds transient/permanent based on temporal follow-up notes

Output tables:
  complication_phenotype_v1       -- long-format (patient x complication entity)
  complication_patient_summary_v1 -- wide-format per-patient flags + status
  complication_discrepancy_report_v1 -- raw vs confirmed counts per entity

Run after rln_refined_pipeline.py and complications_refined_pipeline.py outputs
are deployed to MotherDuck.
Supports --md, --local, --dry-run flags.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))

TODAY = datetime.now().strftime("%Y%m%d_%H%M")

ENTITIES = [
    "hypocalcemia",
    "hypoparathyroidism",
    "rln_injury",
    "vocal_cord_paralysis",
    "vocal_cord_paresis",
    "hematoma",
    "seroma",
    "chyle_leak",
    "wound_infection",
]


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def _get_token() -> str:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if token:
        return token
    secrets = ROOT / ".streamlit" / "secrets.toml"
    if secrets.exists():
        try:
            import toml
            return toml.load(str(secrets))["MOTHERDUCK_TOKEN"]
        except Exception:
            pass
    raise RuntimeError(
        "MOTHERDUCK_TOKEN not set. Export it or add to .streamlit/secrets.toml."
    )


def connect_md() -> duckdb.DuckDBPyConnection:
    token = _get_token()
    return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")


def connect_local() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# complication_phenotype_v1 (long-format)
# ─────────────────────────────────────────────────────────────────────────────
PHENOTYPE_LONG_SQL = """
CREATE OR REPLACE TABLE complication_phenotype_v1 AS
WITH

-- ── Surgery dates (first surgery per patient) ─────────────────────────────
surgery_dates AS (
    SELECT research_id,
           MIN(TRY_CAST(surg_date AS DATE)) AS first_surgery_date
    FROM path_synoptics
    GROUP BY research_id
),

-- ── Raw NLP mentions (all polarity) ───────────────────────────────────────
nlp_mentions AS (
    SELECT
        research_id,
        entity_value_norm                               AS entity_name,
        present_or_negated                              AS nlp_polarity,
        inferred_event_date                             AS event_date,
        note_type,
        -- Exclude same-day h&p boilerplate consent mentions
        (present_or_negated = 'present'
         AND note_type NOT IN ('h_p','consent')) AS valid_mention
    FROM enriched_note_entities_complications
    WHERE entity_value_norm IN (
        'hypocalcemia','hypoparathyroidism','rln_injury',
        'vocal_cord_paralysis','vocal_cord_paresis',
        'hematoma','seroma','chyle_leak','wound_infection'
    )
),

-- ── Refined complication flags (Phase 2 pipeline output) ──────────────────
refined AS (
    SELECT
        research_id,
        entity_name,
        entity_is_confirmed,
        entity_tier,            -- 1=confirmed, 2=probable, 3=uncertain
        entity_evidence_strength,
        source_tier_label,
        detection_date
    FROM extracted_complications_refined_v5
),

-- ── RLN injury refined (Phase 2 RLN pipeline output) ─────────────────────
rln_refined AS (
    SELECT
        research_id,
        'rln_injury'            AS entity_name,
        (tier IN (1,2))         AS entity_is_confirmed,
        tier                    AS entity_tier,
        confidence_level        AS entity_evidence_strength,
        source_label            AS source_tier_label,
        detection_date
    FROM extracted_rln_injury_refined_v2
),

-- ── Post-op lab evidence (PTH, calcium) for hypocalcemia phenotyping ───────
postop_labs AS (
    SELECT
        research_id,
        lab_type,
        result_numeric,
        collection_date,
        -- Classify abnormal values
        CASE
            WHEN lab_type = 'pth' AND result_numeric < 15   THEN 'low_pth'
            WHEN lab_type = 'calcium' AND result_numeric < 8.0 THEN 'low_calcium'
            ELSE NULL
        END AS lab_abnormality_class,
        -- Days post-surgery (requires join to surgery_dates; done below)
        collection_date         AS lab_date
    FROM extracted_postop_labs_expanded_v1
    WHERE result_numeric IS NOT NULL
),

-- ── Medication evidence (calcium, calcitriol supplements) ─────────────────
calcium_meds AS (
    SELECT DISTINCT
        research_id,
        MIN(inferred_event_date) AS first_supplement_date
    FROM enriched_note_entities_medications
    WHERE LOWER(entity_value_norm) IN (
        'calcium','calcitriol','calcium_carbonate','calcium_citrate',
        'vitamin_d','ergocalciferol','cholecalciferol','calcitrol'
    )
    AND present_or_negated = 'present'
    GROUP BY research_id
),

-- ── Follow-up note evidence for transient/permanent classification ─────────
followup_evidence AS (
    SELECT
        research_id,
        CASE
            WHEN LOWER(entity_value_norm) IN ('vocal_cord_paralysis','vocal_cord_paresis','rln_injury')
                 THEN 'rln_followup'
            WHEN LOWER(entity_value_norm) IN ('hypocalcemia','hypoparathyroidism')
                 THEN 'calcium_followup'
            ELSE NULL
        END AS followup_type,
        -- "resolved", "improved", "normal" = transient
        BOOL_OR(LOWER(entity_value_norm) LIKE '%resolv%'
                OR LOWER(entity_value_norm) LIKE '%normaliz%'
                OR LOWER(entity_value_norm) LIKE '%improv%')
            AS has_resolution_note,
        -- "permanent", "persistent", "chronic" = permanent
        BOOL_OR(LOWER(entity_value_norm) LIKE '%permanent%'
                OR LOWER(entity_value_norm) LIKE '%persistent%'
                OR LOWER(entity_value_norm) LIKE '%chronic%')
            AS has_permanence_note,
        MAX(inferred_event_date) AS last_followup_date
    FROM enriched_note_entities_problem_list
    WHERE entity_value_norm IS NOT NULL
    GROUP BY research_id,
        CASE
            WHEN LOWER(entity_value_norm) IN ('vocal_cord_paralysis','vocal_cord_paresis','rln_injury')
                 THEN 'rln_followup'
            WHEN LOWER(entity_value_norm) IN ('hypocalcemia','hypoparathyroidism')
                 THEN 'calcium_followup'
            ELSE NULL
        END
),

-- ── Aggregate NLP mention counts per patient per entity ───────────────────
nlp_agg AS (
    SELECT
        research_id,
        entity_name,
        COUNT(*) FILTER (WHERE nlp_polarity = 'present') AS n_present_mentions,
        COUNT(*) FILTER (WHERE valid_mention)             AS n_valid_mentions,
        BOOL_OR(valid_mention)                            AS note_mention_flag,
        MIN(CASE WHEN valid_mention THEN event_date END)  AS first_valid_mention_date
    FROM nlp_mentions
    GROUP BY research_id, entity_name
),

-- ── Combine refined + RLN sources ─────────────────────────────────────────
all_refined AS (
    SELECT research_id, entity_name, entity_is_confirmed, entity_tier,
           entity_evidence_strength, source_tier_label, detection_date
    FROM refined
    UNION ALL
    SELECT research_id, entity_name, entity_is_confirmed, entity_tier,
           entity_evidence_strength, source_tier_label, detection_date
    FROM rln_refined
    WHERE NOT EXISTS (
        SELECT 1 FROM refined r2
        WHERE r2.research_id = rln_refined.research_id
          AND r2.entity_name = 'rln_injury'
    )
),

-- ── Best refined evidence per patient per entity ──────────────────────────
refined_best AS (
    SELECT *
    FROM all_refined
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id, entity_name
        ORDER BY entity_tier ASC, detection_date ASC NULLS LAST
    ) = 1
),

-- ── Patient spine: all (patient, entity) combinations with any evidence ───
patient_entity_spine AS (
    SELECT DISTINCT research_id, entity_name FROM nlp_agg
    UNION
    SELECT DISTINCT research_id, entity_name FROM refined_best
),

-- ── Post-op lab nadir per patient ─────────────────────────────────────────
pth_nadir AS (
    SELECT l.research_id,
           MIN(l.result_numeric) AS pth_nadir,
           MIN(CASE WHEN l.result_numeric < 15 THEN
               DATEDIFF('day', s.first_surgery_date, l.lab_date) END)
               AS days_to_low_pth
    FROM postop_labs l
    JOIN surgery_dates s USING (research_id)
    WHERE l.lab_type = 'pth'
      AND DATEDIFF('day', s.first_surgery_date, l.lab_date) BETWEEN 0 AND 30
    GROUP BY l.research_id
),
ca_nadir AS (
    SELECT l.research_id,
           MIN(l.result_numeric) AS ca_nadir,
           MIN(CASE WHEN l.result_numeric < 8.0 THEN
               DATEDIFF('day', s.first_surgery_date, l.lab_date) END)
               AS days_to_low_ca
    FROM postop_labs l
    JOIN surgery_dates s USING (research_id)
    WHERE l.lab_type = 'calcium'
      AND DATEDIFF('day', s.first_surgery_date, l.lab_date) BETWEEN 0 AND 30
    GROUP BY l.research_id
),

-- ── Main phenotype assembly ───────────────────────────────────────────────
phenotype AS (
    SELECT
        pes.research_id,
        pes.entity_name,
        sd.first_surgery_date,

        -- Raw mention flags
        COALESCE(na.note_mention_flag, FALSE)           AS note_mention_flag,
        COALESCE(na.n_present_mentions, 0)              AS n_raw_nlp_mentions,
        COALESCE(na.n_valid_mentions, 0)                AS n_valid_nlp_mentions,
        na.first_valid_mention_date,

        -- Refined evidence
        rb.entity_is_confirmed                          AS confirmed_flag,
        (rb.entity_tier <= 2)                           AS suspected_flag,
        rb.entity_tier                                  AS evidence_tier,
        rb.entity_evidence_strength,
        rb.source_tier_label,
        rb.detection_date,

        -- Timing relative to surgery
        CASE
            WHEN rb.detection_date IS NOT NULL AND sd.first_surgery_date IS NOT NULL
                 THEN DATEDIFF('day', sd.first_surgery_date, rb.detection_date)
            WHEN na.first_valid_mention_date IS NOT NULL AND sd.first_surgery_date IS NOT NULL
                 THEN DATEDIFF('day', sd.first_surgery_date, na.first_valid_mention_date)
            ELSE NULL
        END AS timing_days_post_surgery,

        -- Surgery-related flag (event within 365 days after surgery)
        CASE
            WHEN COALESCE(
                rb.detection_date,
                na.first_valid_mention_date
            ) >= sd.first_surgery_date
            AND DATEDIFF('day', sd.first_surgery_date, COALESCE(
                rb.detection_date,
                na.first_valid_mention_date
            )) <= 365
            THEN TRUE ELSE FALSE
        END AS surgery_related_flag,

        -- Historical only: event predates surgery
        CASE
            WHEN COALESCE(
                rb.detection_date, na.first_valid_mention_date
            ) < sd.first_surgery_date THEN TRUE
            ELSE FALSE
        END AS historical_only_flag,

        -- ── Hypocalcemia/Hypoparathyroidism specific ──────────────────
        -- Biochemical evidence
        (pn.pth_nadir IS NOT NULL AND pn.pth_nadir < 15) AS biochemical_low_pth,
        pn.pth_nadir,
        pn.days_to_low_pth,
        (cn.ca_nadir IS NOT NULL AND cn.ca_nadir < 8.0)  AS biochemical_low_ca,
        cn.ca_nadir,
        cn.days_to_low_ca,
        -- Treatment requiring: supplements documented within 60 days
        (cm.first_supplement_date IS NOT NULL
         AND sd.first_surgery_date IS NOT NULL
         AND DATEDIFF('day', sd.first_surgery_date, cm.first_supplement_date)
             BETWEEN 0 AND 60)   AS treatment_requiring_flag,
        cm.first_supplement_date,

        -- ── RLN/Voice resolution ──────────────────────────────────────
        COALESCE(fv.has_resolution_note, FALSE)         AS voice_resolution_noted,
        COALESCE(fv.has_permanence_note, FALSE)         AS voice_permanence_noted,
        fv.last_followup_date

    FROM patient_entity_spine pes
    LEFT JOIN surgery_dates sd USING (research_id)
    LEFT JOIN nlp_agg na USING (research_id, entity_name)
    LEFT JOIN refined_best rb USING (research_id, entity_name)
    LEFT JOIN pth_nadir pn USING (research_id)
    LEFT JOIN ca_nadir cn USING (research_id)
    LEFT JOIN calcium_meds cm USING (research_id)
    LEFT JOIN followup_evidence fv
        ON fv.research_id = pes.research_id
        AND (
            (pes.entity_name IN ('rln_injury','vocal_cord_paralysis','vocal_cord_paresis')
             AND fv.followup_type = 'rln_followup')
            OR
            (pes.entity_name IN ('hypocalcemia','hypoparathyroidism')
             AND fv.followup_type = 'calcium_followup')
        )
),

-- ── Transient / Permanent classification ─────────────────────────────────
transience AS (
    SELECT *,
        CASE
            -- RLN: transient if documented resolution within 6 months
            WHEN entity_name IN ('rln_injury','vocal_cord_paralysis','vocal_cord_paresis')
                 AND voice_resolution_noted
                 AND NOT voice_permanence_noted     THEN TRUE
            WHEN entity_name IN ('rln_injury','vocal_cord_paralysis','vocal_cord_paresis')
                 AND last_followup_date IS NOT NULL
                 AND first_surgery_date IS NOT NULL
                 AND DATEDIFF('day', first_surgery_date, last_followup_date) < 180
                 AND NOT voice_permanence_noted     THEN NULL  -- insufficient follow-up
            -- Hypocalcemia/Hypoparathyroidism: transient if supplements stopped <6mo
            WHEN entity_name IN ('hypocalcemia','hypoparathyroidism')
                 AND treatment_requiring_flag
                 AND (
                     (last_followup_date IS NOT NULL
                      AND DATEDIFF('day', first_surgery_date, last_followup_date) < 180)
                     OR biochemical_low_ca = FALSE
                 )                                 THEN TRUE
            ELSE NULL
        END AS transient_flag,
        CASE
            -- RLN: permanent if explicitly documented
            WHEN entity_name IN ('rln_injury','vocal_cord_paralysis','vocal_cord_paresis')
                 AND voice_permanence_noted         THEN TRUE
            -- Hypocalcemia: permanent if supplements continued >6 months
            WHEN entity_name IN ('hypocalcemia','hypoparathyroidism')
                 AND treatment_requiring_flag
                 AND last_followup_date IS NOT NULL
                 AND DATEDIFF('day', first_surgery_date, last_followup_date) > 180
                 AND (pth_nadir IS NOT NULL AND pth_nadir < 15)
                                                   THEN TRUE
            ELSE FALSE
        END AS permanent_flag
    FROM phenotype
)

-- ── Final assembly ────────────────────────────────────────────────────────
SELECT
    t.research_id,
    t.entity_name                                       AS complication_entity,
    t.note_mention_flag,
    t.n_raw_nlp_mentions,
    t.n_valid_nlp_mentions,
    COALESCE(t.suspected_flag, FALSE)                   AS suspected_flag,
    COALESCE(t.confirmed_flag, FALSE)                   AS confirmed_flag,
    COALESCE(t.transient_flag, FALSE)                   AS transient_flag,
    COALESCE(t.permanent_flag, FALSE)                   AS permanent_flag,
    COALESCE(t.surgery_related_flag, FALSE)             AS surgery_related_flag,
    COALESCE(t.historical_only_flag, FALSE)             AS historical_only_flag,
    t.timing_days_post_surgery,
    CASE
        WHEN t.timing_days_post_surgery IS NULL          THEN 'unknown'
        WHEN t.timing_days_post_surgery < 0              THEN 'pre_surgery'
        WHEN t.timing_days_post_surgery <= 30            THEN '0_30d'
        WHEN t.timing_days_post_surgery <= 180           THEN '31_180d'
        WHEN t.timing_days_post_surgery <= 365           THEN '181_365d'
        ELSE 'gt_365d'
    END AS timing_window,
    -- Final complication status
    CASE
        WHEN NOT COALESCE(t.confirmed_flag, FALSE)
             AND NOT COALESCE(t.suspected_flag, FALSE)   THEN 'absent_or_unconfirmed'
        WHEN t.permanent_flag                            THEN 'confirmed_permanent'
        WHEN t.transient_flag                            THEN 'confirmed_transient'
        WHEN COALESCE(t.confirmed_flag, FALSE)
             AND t.transient_flag IS NULL                THEN 'confirmed_duration_unknown'
        WHEN COALESCE(t.suspected_flag, FALSE)           THEN 'probable'
        ELSE 'historical_or_incidental'
    END AS final_complication_status,
    -- Analysis eligible: surgery-related, within 365 days, confirmed or probable
    (COALESCE(t.surgery_related_flag, FALSE)
     AND COALESCE(t.suspected_flag OR t.confirmed_flag, FALSE)
     AND NOT COALESCE(t.historical_only_flag, FALSE))    AS analysis_eligible_flag,
    -- Phenotype specifics
    t.biochemical_low_pth,
    t.pth_nadir,
    t.biochemical_low_ca,
    t.ca_nadir,
    t.treatment_requiring_flag,
    t.voice_resolution_noted,
    t.voice_permanence_noted,
    t.evidence_tier,
    t.source_tier_label,
    t.detection_date,
    t.first_surgery_date,
    CURRENT_TIMESTAMP AS phenotyped_at,
    'v1' AS phenotype_version
FROM transience t
"""


# ─────────────────────────────────────────────────────────────────────────────
# complication_patient_summary_v1 (wide-format)
# ─────────────────────────────────────────────────────────────────────────────
PATIENT_SUMMARY_SQL = """
CREATE OR REPLACE TABLE complication_patient_summary_v1 AS
SELECT
    research_id,
    -- Per-entity confirmed status
    COALESCE(MAX(CASE WHEN complication_entity = 'hypocalcemia'
        THEN final_complication_status END), 'absent') AS hypocalcemia_status,
    COALESCE(MAX(CASE WHEN complication_entity = 'hypoparathyroidism'
        THEN final_complication_status END), 'absent') AS hypoparathyroidism_status,
    COALESCE(MAX(CASE WHEN complication_entity IN
        ('rln_injury','vocal_cord_paralysis','vocal_cord_paresis')
        THEN final_complication_status END), 'absent') AS rln_status,
    COALESCE(MAX(CASE WHEN complication_entity = 'hematoma'
        THEN final_complication_status END), 'absent') AS hematoma_status,
    COALESCE(MAX(CASE WHEN complication_entity = 'seroma'
        THEN final_complication_status END), 'absent') AS seroma_status,
    COALESCE(MAX(CASE WHEN complication_entity = 'chyle_leak'
        THEN final_complication_status END), 'absent') AS chyle_leak_status,
    COALESCE(MAX(CASE WHEN complication_entity = 'wound_infection'
        THEN final_complication_status END), 'absent') AS wound_infection_status,
    -- Aggregate flags
    BOOL_OR(confirmed_flag AND surgery_related_flag)    AS any_confirmed_complication_flag,
    BOOL_OR(analysis_eligible_flag)                     AS any_analysis_eligible_complication,
    COUNT(CASE WHEN confirmed_flag AND surgery_related_flag THEN 1 END)
                                                        AS n_confirmed_complications,
    -- Worst timing window (earliest post-op complication)
    MIN(CASE WHEN surgery_related_flag AND confirmed_flag
             THEN timing_days_post_surgery END)         AS earliest_complication_days,
    -- Biochemical hypo flags
    BOOL_OR(biochemical_low_pth)                        AS has_low_pth_flag,
    BOOL_OR(biochemical_low_ca)                         AS has_low_calcium_flag,
    BOOL_OR(treatment_requiring_flag AND complication_entity
            IN ('hypocalcemia','hypoparathyroidism'))   AS calcium_supplement_required,
    -- RLN flags
    BOOL_OR(complication_entity IN (
        'rln_injury','vocal_cord_paralysis','vocal_cord_paresis')
        AND permanent_flag)                             AS rln_permanent_flag,
    BOOL_OR(complication_entity IN (
        'rln_injury','vocal_cord_paralysis','vocal_cord_paresis')
        AND transient_flag)                             AS rln_transient_flag,
    CURRENT_TIMESTAMP                                   AS summarized_at
FROM complication_phenotype_v1
GROUP BY research_id
"""


# ─────────────────────────────────────────────────────────────────────────────
# complication_discrepancy_report_v1 -- raw vs confirmed counts
# ─────────────────────────────────────────────────────────────────────────────
DISCREPANCY_REPORT_SQL = """
CREATE OR REPLACE TABLE complication_discrepancy_report_v1 AS
SELECT
    complication_entity,
    COUNT(DISTINCT research_id)             AS total_patients_with_any_mention,
    COUNT(DISTINCT CASE WHEN note_mention_flag THEN research_id END)
                                            AS raw_nlp_mention_patients,
    SUM(n_raw_nlp_mentions)                 AS total_raw_nlp_mentions,
    COUNT(DISTINCT CASE WHEN suspected_flag THEN research_id END)
                                            AS suspected_patients,
    COUNT(DISTINCT CASE WHEN confirmed_flag THEN research_id END)
                                            AS confirmed_patients,
    COUNT(DISTINCT CASE WHEN analysis_eligible_flag THEN research_id END)
                                            AS analysis_eligible_patients,
    COUNT(DISTINCT CASE WHEN permanent_flag THEN research_id END)
                                            AS permanent_patients,
    COUNT(DISTINCT CASE WHEN transient_flag THEN research_id END)
                                            AS transient_patients,
    COUNT(DISTINCT CASE WHEN surgery_related_flag AND confirmed_flag
                         THEN research_id END)
                                            AS surgery_related_confirmed,
    COUNT(DISTINCT CASE WHEN historical_only_flag THEN research_id END)
                                            AS historical_only_patients,
    -- Precision estimate: confirmed / (raw nlp mention patients)
    CASE
        WHEN COUNT(DISTINCT CASE WHEN note_mention_flag THEN research_id END) = 0
        THEN NULL
        ELSE ROUND(100.0 *
            COUNT(DISTINCT CASE WHEN confirmed_flag THEN research_id END) /
            COUNT(DISTINCT CASE WHEN note_mention_flag THEN research_id END),
            1)
    END AS estimated_nlp_precision_pct,
    CURRENT_TIMESTAMP AS reported_at
FROM complication_phenotype_v1
GROUP BY complication_entity
ORDER BY confirmed_patients DESC
"""


def _ensure_stub_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create empty stubs for optional upstream tables to prevent SQL failures."""

    if not table_available(con, "extracted_complications_refined_v5"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE extracted_complications_refined_v5 AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS entity_name,
       NULL::BOOLEAN AS entity_is_confirmed, NULL::INTEGER AS entity_tier,
       NULL::VARCHAR AS entity_evidence_strength, NULL::VARCHAR AS source_tier_label,
       NULL::DATE AS detection_date
WHERE 1=0
""")

    if not table_available(con, "extracted_rln_injury_refined_v2"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE extracted_rln_injury_refined_v2 AS
SELECT NULL::INTEGER AS research_id, NULL::INTEGER AS tier,
       NULL::VARCHAR AS confidence_level, NULL::VARCHAR AS source_label,
       NULL::DATE AS detection_date
WHERE 1=0
""")

    if not table_available(con, "extracted_postop_labs_expanded_v1"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE extracted_postop_labs_expanded_v1 AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS lab_type,
       NULL::DOUBLE AS result_numeric, NULL::DATE AS collection_date
WHERE 1=0
""")

    if not table_available(con, "enriched_note_entities_medications"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE enriched_note_entities_medications AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS entity_value_norm,
       NULL::VARCHAR AS present_or_negated, NULL::DATE AS inferred_event_date
WHERE 1=0
""")

    if not table_available(con, "enriched_note_entities_problem_list"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE enriched_note_entities_problem_list AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS entity_value_norm,
       NULL::VARCHAR AS present_or_negated, NULL::DATE AS inferred_event_date
WHERE 1=0
""")

    if not table_available(con, "enriched_note_entities_complications"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE enriched_note_entities_complications AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS entity_value_norm,
       NULL::VARCHAR AS present_or_negated, NULL::DATE AS inferred_event_date,
       NULL::VARCHAR AS note_type
WHERE 1=0
""")

    if not table_available(con, "path_synoptics"):
        pq = ROOT / "processed" / "path_synoptics.parquet"
        if pq.exists():
            con.execute(
                "CREATE OR REPLACE TABLE path_synoptics AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )
        else:
            con.execute("""
CREATE OR REPLACE TEMP TABLE path_synoptics AS
SELECT NULL::INTEGER AS research_id, NULL::DATE AS surg_date
WHERE 1=0
""")


def build_phenotype_tables(con: duckdb.DuckDBPyConnection,
                           dry_run: bool = False) -> None:
    section("Building complication phenotype tables")

    # Availability check
    for tbl in ["enriched_note_entities_complications",
                "extracted_complications_refined_v5",
                "extracted_rln_injury_refined_v2"]:
        avail = table_available(con, tbl)
        print(f"  {tbl}: {'present' if avail else 'missing (stub will be used)'}")

    if dry_run:
        print("  [DRY-RUN] Would create complication_phenotype_v1, "
              "complication_patient_summary_v1, complication_discrepancy_report_v1")
        return

    _ensure_stub_tables(con)

    print("\n  Building complication_phenotype_v1 (long-format)...")
    con.execute(PHENOTYPE_LONG_SQL)
    r = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM complication_phenotype_v1"
    ).fetchone()
    print(f"    complication_phenotype_v1: {r[0]:,} rows, {r[1]:,} patients")

    print("  Building complication_patient_summary_v1 (wide-format)...")
    con.execute(PATIENT_SUMMARY_SQL)
    r = con.execute(
        "SELECT COUNT(*), SUM(CASE WHEN any_confirmed_complication_flag THEN 1 ELSE 0 END) "
        "FROM complication_patient_summary_v1"
    ).fetchone()
    print(f"    complication_patient_summary_v1: {r[0]:,} patients, "
          f"{r[1]:,} with confirmed complications")

    print("  Building complication_discrepancy_report_v1...")
    con.execute(DISCREPANCY_REPORT_SQL)
    disc = con.execute("SELECT * FROM complication_discrepancy_report_v1").fetchdf()
    print(disc[["complication_entity", "raw_nlp_mention_patients",
                "confirmed_patients", "analysis_eligible_patients",
                "estimated_nlp_precision_pct"]].to_string(index=False))

    print("\n  [DONE] Complication phenotype tables created")


def main() -> None:
    p = argparse.ArgumentParser(description="52_complication_phenotyping_v2.py")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--md", action="store_true", help="Connect to MotherDuck")
    g.add_argument("--local", action="store_true", help="Use local DuckDB (default)")
    p.add_argument("--dry-run", action="store_true", help="Audit only, no writes")
    args = p.parse_args()

    if args.md:
        section("Connecting to MotherDuck")
        con = connect_md()
    else:
        section("Connecting to local DuckDB")
        con = connect_local()

    try:
        build_phenotype_tables(con, dry_run=args.dry_run)
    finally:
        con.close()

    print("\n[COMPLETE] 52_complication_phenotyping_v2.py finished")


if __name__ == "__main__":
    main()
