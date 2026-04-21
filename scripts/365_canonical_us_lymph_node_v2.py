#!/usr/bin/env python3
"""Script 365 — Build main.canonical_us_lymph_node_v2 (Phase 5).

Ultrasound-sourced LN findings ONLY. The schema enforces this contract via
  * source_modality VARCHAR NOT NULL CHECK (source_modality = 'US')
  * us_ln_* prefixed identifier columns (us_ln_id, us_ln_index_within_exam)
  * COMMENT ON TABLE explicitly stating modality scope.

Future modality-specific tables (canonical_ct_lymph_node_v2,
canonical_petct_lymph_node_v2, canonical_mr_lymph_node_v2,
canonical_nucmed_lymph_node_v2) and the existing pathology table
(canonical_lymph_node_master_v1) live separately. NEVER union into this v2.

NO LLM in this pass. Three rule-based US-only sources:
  1. ultrasound_reports.lymph_node_assessment (one row per non-empty)
  2. us_nodules_tirads.us_<k>_impression matching LN keyword regex
     (we only have us_1_impression in this table; us_2.. are not present)
  3. CPM lnus_* columns for the 61 patients with structured US LN data
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

SCRIPT_TAG = "Script 365"
TARGET = f"{PUBLICATION_DB}.main.canonical_us_lymph_node_v2"
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"365_us_lymph_node_v2_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


# DDL with explicit modality CHECK constraint
DDL_SQL = f"""
CREATE OR REPLACE TABLE {TARGET} (
    research_id                  INTEGER,
    us_exam_id                   VARCHAR,
    exam_date                    DATE,
    us_ln_index_within_exam      INTEGER,
    us_ln_id                     VARCHAR,
    source_modality              VARCHAR NOT NULL CHECK (source_modality = 'US'),
    laterality                   VARCHAR,
    neck_level                   VARCHAR,
    region                       VARCHAR,
    size_cm_max                  DOUBLE,
    short_axis_mm                DOUBLE,
    long_axis_mm                 DOUBLE,
    shape                        VARCHAR,
    echogenicity                 VARCHAR,
    hilum_preserved              BOOLEAN,
    calcifications               VARCHAR,
    cystic_component             BOOLEAN,
    vascularity_pattern          VARCHAR,
    extranodal_extension_on_us   BOOLEAN,
    suspicious_flag              BOOLEAN,
    suspicion_level              VARCHAR,
    biopsy_recommended           BOOLEAN,
    evidence_text                VARCHAR,
    source_note_type             VARCHAR,
    source_report_id             VARCHAR,
    llm_model                    VARCHAR,
    confidence                   DOUBLE,
    extracted_at                 TIMESTAMP,
    nlp_backfill_pending         BOOLEAN
);
"""


# Source 1: ultrasound_reports.lymph_node_assessment
SRC1_SQL = f"""
INSERT INTO {TARGET}
SELECT
    TRY_CAST(research_id AS INTEGER) AS research_id,
    md5(research_id || '|' || COALESCE(ultrasound_date,'')) AS us_exam_id,
    TRY_CAST(ultrasound_date AS DATE) AS exam_date,
    1 AS us_ln_index_within_exam,
    md5(research_id || '|' || COALESCE(ultrasound_date,'') || '|us_ln1') AS us_ln_id,
    'US' AS source_modality,
    NULL::VARCHAR AS laterality,
    NULL::VARCHAR AS neck_level,
    NULL::VARCHAR AS region,
    NULL::DOUBLE  AS size_cm_max,
    NULL::DOUBLE  AS short_axis_mm,
    NULL::DOUBLE  AS long_axis_mm,
    NULL::VARCHAR AS shape,
    NULL::VARCHAR AS echogenicity,
    NULL::BOOLEAN AS hilum_preserved,
    NULL::VARCHAR AS calcifications,
    NULL::BOOLEAN AS cystic_component,
    NULL::VARCHAR AS vascularity_pattern,
    NULL::BOOLEAN AS extranodal_extension_on_us,
    NULL::BOOLEAN AS suspicious_flag,
    NULL::VARCHAR AS suspicion_level,
    NULL::BOOLEAN AS biopsy_recommended,
    lymph_node_assessment AS evidence_text,
    'ultrasound_report'   AS source_note_type,
    md5(research_id || '|' || COALESCE(ultrasound_date,'')) AS source_report_id,
    NULL::VARCHAR AS llm_model,
    NULL::DOUBLE  AS confidence,
    CURRENT_TIMESTAMP AS extracted_at,
    TRUE AS nlp_backfill_pending
FROM {PUBLICATION_DB}.main.ultrasound_reports
WHERE lymph_node_assessment IS NOT NULL
  AND TRIM(lymph_node_assessment) <> ''
  AND TRY_CAST(research_id AS INTEGER) IS NOT NULL;
"""

# Source 2: us_nodules_tirads.us_1_impression with LN keyword
SRC2_SQL = f"""
INSERT INTO {TARGET}
SELECT
    TRY_CAST(research_id AS INTEGER) AS research_id,
    md5(research_id || '|' || COALESCE(us_1_date,'')) AS us_exam_id,
    TRY_CAST(us_1_date AS DATE) AS exam_date,
    1 AS us_ln_index_within_exam,
    md5(research_id || '|' || COALESCE(us_1_date,'') || '|usnt_ln1') AS us_ln_id,
    'US', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    us_1_impression AS evidence_text,
    'us_nodules_tirads'  AS source_note_type,
    md5(research_id || '|' || COALESCE(us_1_date,'')) AS source_report_id,
    NULL, NULL, CURRENT_TIMESTAMP, TRUE
FROM {PUBLICATION_DB}.main.us_nodules_tirads
WHERE us_1_impression IS NOT NULL
  AND TRIM(us_1_impression) <> ''
  AND regexp_matches(LOWER(us_1_impression),
       '(\\blymph\\b|\\bln\\b|\\bnode\\b|adenopath)')
  AND TRY_CAST(research_id AS INTEGER) IS NOT NULL
  AND TRY_CAST(research_id AS INTEGER) NOT IN (
      SELECT research_id FROM {TARGET}
      WHERE source_note_type = 'ultrasound_report'
  );
"""

# Source 3: CPM lnus_* — patients with dedicated US LN exam
# Probe shows lnus_has_dedicated_exam (BOOLEAN), lnus_last_date (DATE),
# lnus_n_exams (INTEGER), lnus_normal_ln_any (BOOLEAN),
# lnus_has_size_measurement (BOOLEAN), lnus_impression_last (VARCHAR),
# lnus_indication_first (VARCHAR), lnus_source (VARCHAR).
# Suspicious flag rebuild: not(normal_ln_any) when has_dedicated AND has_size_measurement.
SRC3_SQL = f"""
INSERT INTO {TARGET}
SELECT
    research_id,
    md5(CAST(research_id AS VARCHAR) || '|' || COALESCE(CAST(lnus_last_date AS VARCHAR),''))
        AS us_exam_id,
    lnus_last_date AS exam_date,
    1 AS us_ln_index_within_exam,
    md5(CAST(research_id AS VARCHAR) || '|' || COALESCE(CAST(lnus_last_date AS VARCHAR),'')
        || '|cpm_lnus1') AS us_ln_id,
    'US' AS source_modality,
    NULL::VARCHAR AS laterality,
    NULL::VARCHAR AS neck_level,
    NULL::VARCHAR AS region,
    NULL::DOUBLE  AS size_cm_max,
    NULL::DOUBLE  AS short_axis_mm,
    NULL::DOUBLE  AS long_axis_mm,
    NULL::VARCHAR AS shape,
    NULL::VARCHAR AS echogenicity,
    NULL::BOOLEAN AS hilum_preserved,
    NULL::VARCHAR AS calcifications,
    NULL::BOOLEAN AS cystic_component,
    NULL::VARCHAR AS vascularity_pattern,
    NULL::BOOLEAN AS extranodal_extension_on_us,
    CASE
      WHEN lnus_normal_ln_any IS TRUE THEN FALSE
      WHEN lnus_normal_ln_any IS FALSE THEN TRUE
      ELSE NULL
    END AS suspicious_flag,
    CASE
      WHEN lnus_normal_ln_any IS TRUE THEN 'benign'
      WHEN lnus_normal_ln_any IS FALSE THEN 'suspicious'
      ELSE 'indeterminate'
    END AS suspicion_level,
    NULL::BOOLEAN AS biopsy_recommended,
    lnus_impression_last AS evidence_text,
    'cpm_lnus'           AS source_note_type,
    'cpm:'||CAST(research_id AS VARCHAR) AS source_report_id,
    NULL::VARCHAR AS llm_model,
    NULL::DOUBLE  AS confidence,
    CURRENT_TIMESTAMP AS extracted_at,
    FALSE AS nlp_backfill_pending
FROM {PUBLICATION_DB}.main.canonical_patient_master
WHERE lnus_has_dedicated_exam IS TRUE
  AND lnus_has_size_measurement IS TRUE
  AND lnus_last_date IS NOT NULL;
"""


COMMENT_SQL = (
    f"COMMENT ON TABLE {TARGET} IS "
    f"'Ultrasound-sourced lymph node findings per (research_id, us_exam_id). "
    f"Grain: one row per LN observation on a US exam. NOT for "
    f"pathology/CT/PET-CT/MR/nucmed LN — those live in parallel "
    f"canonical_<modality>_lymph_node_v2 tables. source_modality is fixed to "
    f"US (CHECK constraint enforced). Built {RUN_TS} by Script 365 from "
    f"ultrasound_reports.lymph_node_assessment + us_nodules_tirads.us_1_impression "
    f"(LN-keyword filter) + CPM lnus_* (US-sourced only). No LLM run.';"
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    if not args.commit:
        log("dry-run only.")
        return 0

    log(f"  CREATE OR REPLACE TABLE {TARGET}")
    con.execute(DDL_SQL)
    log("  insert source 1: ultrasound_reports.lymph_node_assessment")
    con.execute(SRC1_SQL)
    n1 = con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()[0]
    log(f"    after src1: {n1}")
    log("  insert source 2: us_nodules_tirads.us_1_impression (LN regex)")
    con.execute(SRC2_SQL)
    n2 = con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()[0]
    log(f"    after src2: {n2}  (+{n2 - n1})")
    log("  insert source 3: CPM lnus_* (US-only)")
    con.execute(SRC3_SQL)
    n3 = con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()[0]
    log(f"    after src3: {n3}  (+{n3 - n2})")

    con.execute(COMMENT_SQL)

    # Verification
    distinct_modality = [
        r[0] for r in con.execute(
            f"SELECT DISTINCT source_modality FROM {TARGET}"
        ).fetchall()
    ]
    if distinct_modality != ['US']:
        raise SystemExit(
            f"Modality contract violation: source_modality has values "
            f"{distinct_modality}; must be exactly ['US']"
        )
    by_src = dict(con.execute(
        f"SELECT source_note_type, COUNT(*) FROM {TARGET} GROUP BY 1"
    ).fetchall())
    n_pending = con.execute(
        f"SELECT COUNT(*) FROM {TARGET} WHERE nlp_backfill_pending"
    ).fetchone()[0]
    log(f"  by_source={by_src}  pending={n_pending}  modality={distinct_modality}")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS, "target": TARGET,
        "rows": n3, "by_source": by_src,
        "pending": n_pending, "modality": distinct_modality,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
