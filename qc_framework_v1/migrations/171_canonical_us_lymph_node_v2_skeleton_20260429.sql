-- mig_171 canonical_us_lymph_node_v2 BUILD design skeleton
-- Batch: mig_171_canonical_us_lymph_node_v2_build_20260429
-- Date: 2026-04-29
-- Posture: DESIGN + SKELETON ONLY. Do not execute until Logan ratifies and mig_171b is authorized.
-- Governance: no INSERTs, no registry writes, no canonical_patient_master updates in this file.
-- Purpose: declare the proposed events and patient-rollup grains for the future mig_171b build.

-- §4a Events grain: one row per US lymph-node observation per ultrasound exam.
-- research_id is VARCHAR to align with canonical_patient_master and avoid the mig_170 key dtype trap.
-- Clinical dates are DATE; build/provenance timestamps are TIMESTAMP.
CREATE TABLE IF NOT EXISTS main.canonical_us_lymph_node_events_v2 (
  research_id                 VARCHAR     NOT NULL,
  us_exam_id                  VARCHAR     NOT NULL,
  ln_event_id                 VARCHAR     PRIMARY KEY,
  ln_index                    INTEGER,
  side                        VARCHAR,
  neck_level                  VARCHAR,
  region                      VARCHAR,
  size_short_mm               DOUBLE,
  size_long_mm                DOUBLE,
  size_max_mm                 DOUBLE,
  size_max_cm                 DOUBLE,
  shape                       VARCHAR,
  echogenicity                VARCHAR,
  hilum_preserved             BOOLEAN,
  calcifications              VARCHAR,
  cystic_component            BOOLEAN,
  vascularity_pattern         VARCHAR,
  extranodal_extension_on_us  BOOLEAN,
  suspicious_flag             BOOLEAN,
  suspicion_level             VARCHAR,
  biopsy_recommended          BOOLEAN,
  fna_of_ln_mentioned         BOOLEAN,
  washout_tg_mentioned        BOOLEAN,
  source_modality             VARCHAR     NOT NULL DEFAULT 'US' CHECK (source_modality = 'US'),
  source_table                VARCHAR     NOT NULL,
  source_row_id               VARCHAR,
  source_note_type            VARCHAR,
  source_report_id            VARCHAR,
  evidence_text               VARCHAR,
  confidence                  DOUBLE,
  llm_model                   VARCHAR,
  exam_date                   DATE        NOT NULL,
  date_confidence             DOUBLE,
  date_source_keyword         VARCHAR,
  extracted_at                TIMESTAMP,
  build_ts                    TIMESTAMP   DEFAULT CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  build_migration             VARCHAR     DEFAULT 'mig_171b'
);

-- §4b Patient rollup grain: one row per canonical patient.
-- Contains US-imaging LN rollups plus explicit bridge fields for the nine PM tp_* CF columns.
-- The tp_* fields are design placeholders for mig_171b/mig_171c ratification; no values are populated here.
CREATE TABLE IF NOT EXISTS main.canonical_us_lymph_node_patient_rollup_v2 (
  research_id                         VARCHAR PRIMARY KEY,
  has_us_ln_findings                  BOOLEAN,
  n_us_ln_events                      INTEGER,
  n_us_ln_exams                       INTEGER,
  first_us_ln_exam_date               DATE,
  last_us_ln_exam_date                DATE,
  any_us_ln_suspicious                BOOLEAN,
  n_us_ln_suspicious                  INTEGER,
  max_us_ln_short_axis_mm             DOUBLE,
  max_us_ln_long_axis_mm              DOUBLE,
  max_us_ln_size_mm                   DOUBLE,
  max_us_ln_size_cm                   DOUBLE,
  us_ln_sides_observed                VARCHAR,
  us_ln_levels_observed               VARCHAR,
  any_us_ln_extranodal_extension      BOOLEAN,
  any_us_ln_biopsy_recommended        BOOLEAN,
  any_us_ln_fna_mentioned             BOOLEAN,
  any_us_ln_washout_tg_mentioned      BOOLEAN,
  n_us_ln_events_textual_only         INTEGER,
  n_us_ln_events_high_confidence      INTEGER,

  -- Nine CF-mig150-TP-UPSTREAM-NOT-IN-MAIN bridge fields from canonical_patient_master.
  -- These are not populated by this skeleton; mig_171b must ratify exact source precedence,
  -- and mig_171c must re-derive CPM values only after verification.
  tp_central_examined                 DOUBLE,
  tp_central_positive_total           DOUBLE,
  tp_ln_central_positive              BOOLEAN,
  tp_ln_ene                           BOOLEAN,
  tp_ln_examined                      DOUBLE,
  tp_ln_largest_deposit_cm            DOUBLE,
  tp_ln_lateral_positive              BOOLEAN,
  tp_ln_levels_involved               VARCHAR,
  tp_ln_positive                      BOOLEAN,

  source_coverage_notes               VARCHAR,
  build_ts                            TIMESTAMP DEFAULT CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  build_migration                     VARCHAR   DEFAULT 'mig_171b'
);
