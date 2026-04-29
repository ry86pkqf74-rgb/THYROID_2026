-- =============================================================================
-- Migration 141 — canonical_patient_master SURVIVAL CLUSTER sign-off (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane 30 — Survival + follow-up thematic slice (**25** cols per Cowork predicate).
-- batch_id: mig_141_patient_master_survival_cluster_20260429
--
-- Predicate (information_schema ∩ registry, not_started only):
--   %surviv% | %mortal% | death% | followup% | vital_status | voice_followup% |
--   prm_followup% | prm_tg_adequate%
--
-- Pre-apply probes (MotherDuck RW `thyroid_canonical_publication_v1_0`, 2026-04-29):
--   * Predicate cardinality: **25** cols **not_started** (matches Cowork survey).
--   * Cohort parity: canonical_patient_master = 10,871 rows / distinct research_id.
--   * Clinical **DATE** policy: `death_date`, `followup_or_death_date` — **DATE** in live
--     `information_schema` (no CF-mig141 PM retype needed for these two).
--   * **Gate 4** (verified requires verified_by + verification_method + batch_id + verified_ts):
--     **0** violations on pre-existing verified CPM rows.
--   * **BOOLEAN cohort-uniformity** (Lane 30 slice): `survival_event=0` common (alive/censored);
--     `death_occurred=TRUE` = **192**; `survival_eligible_flag` near-universal TRUE (**10,870**/1);
--     `prm_followup_has_complications` TRUE **10,862** / FALSE **9**; `prm_tg_adequate_followup`
--     TRUE **1,823** — no degenerate all-TRUE survival_event / all-FALSE death_occurred pattern.
--   * **Mortality crossover** (`canonical_complications_events_v1`, `complication_type='mortality'`):
--     **1** event row with `finding_status='present'` → **1** distinct patient; **1**/1 agrees with
--     `death_occurred=TRUE` on CPM. Peri-op/event-grain complications mortality ≠ long-term mortality
--     SSOT; **191** deceased on CPM without a complications mortality row —
--     precedence: **canonical_survival_followup_v1** vital/death lineage + Chart/Notes integration
--     (**Script 364B / 364 / 221a family**); not a blocker for crossover gate (presence-layer empty
--     set ⇒ no ≥5% PM-vs-present-event discordance on the complications-positive subset).
--   * **CPM vs `canonical_survival_followup_v1` naive scalar join** — informational (expected):
--       `followup_days` ≠ `days_from_first_surgery_to_last_contact` on **~2,090** patients — CPM follows
--       **Script 233** extended last-contact union (FOD/Tg/RAI/surveillance/NLP lanes), not bare 364B
--       LAB/OP+LKA-only spine (see COMMENT on `canonical_patient_master.followup_days`; cf.
--       `feedback_clinical_dates_calendar_only.md` / CF-100 family).
--       `lower(trim(vital_status))` vs `lower(trim(vital_status_current))`: **191**/+**2**
--       discordant pairs (PM death/integration ahead of staged SSOT label onalive/deceased parity in
--       subset) — documented **CF-mig141-CPM-VITAL-vs-SSOT-PARITY** under `vital_status` notes.
--
-- SSOT lineage (verification contract):
--   * **canonical_survival_followup_v1** — mig_123 (`123_canonical_survival_followup_v1_signoff.sql`,
--     Script **364B**); authoritative **vital_status_current**, **death_date**, **death_date_source**,
--     **last_known_alive_date**, **days_from_first_surgery_to_last_contact**, first_surgery anchors.
--   * **canonical_complications_events_v1** — mig_98/mig_99 mortality category (mortality crossover
--     QA per `qc_framework_v1/migrations/135_*` complications cluster); peri-op/long-term semantics
--     segregated (`project_complications_events_verified_2026-04-28.md`).
--   * **extended follow-up / OS** — Script **233**/`218_followup_recovery.py` last-contact recomputation +
--     death integration scripts (`death_integration_script` VARCHAR provenance stamp).
--
-- Active parallel lanes (do not touch in adjacent commits): mig_137 molecular, mig_138 recurrence,
-- mig_140 ETE sibling, mig_142 RAI, mig_143 small-clusters bundle.
--
-- Applied on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 141a — 1 col — provenance VARCHAR (integration script stamp)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_provenance_skip',
    batch_id            = 'mig_141_patient_master_survival_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_141 survival cluster (Lane 30). Provenance VARCHAR only — '
                          || 'death pipeline script tag (integration lineage SSOT via PM build ledger); '
                          || 'no row-level rederivation gate.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name='death_integration_script';


-- -----------------------------------------------------------------------------
-- 141b — 1 col — outcome flag — survival SSOT × complications mortality presence cross-check
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'cross_check_mortality_crossover_survival_complications',
    batch_id            = 'mig_141_patient_master_survival_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_141 survival cluster (Lane 30). death_occurred BOOLEAN replay vs '
                          || 'canonical_survival_followup_v1 death spine + complications_v1 '
                          || 'complication_type=mortality finding_status=present (1 patient / 1 row); '
                          || 'event-grain complications ≠ exhaustive long-term death capture.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name='death_occurred';


-- -----------------------------------------------------------------------------
-- 141c — 8 cols — vital + survival integration vs mig_123 SSOT (calendar-first)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_survival_followup_v1',
    batch_id            = 'mig_141_patient_master_survival_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_141 survival cluster (Lane 30). Patient-grain replay vs '
                          || 'canonical_survival_followup_v1 (mig_123/364B); OS metrics anchored '
                          || 'first_surgery_date; NULL overall_survival_* where no contact endpoint per '
                          || 'builder. vital_status: see CF-mig141-CPM-VITAL-vs-SSOT-PARITY where PM '
                          || 'integration precedes label parity with vital_status_current alone.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'vital_status',
    'death_date',
    'death_days_from_surg',
    'death_source',
    'survival_eligible_flag',
    'survival_event',
    'overall_survival_days',
    'overall_survival_years'
  );


-- -----------------------------------------------------------------------------
-- 141d — 10 cols — follow-up chain (multi-source last-contact union + recovery metadata)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_aggregate_followup_per_source',
    batch_id            = 'mig_141_patient_master_survival_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_141 survival cluster (Lane 30). Follow-up stack vs Script 233/218 '
                          || 'union + canonical_survival_followup_v1 spine (not naive equality to '
                          || 'days_from_first_surgery_to_last_contact alone); DATE-typed calendar '
                          || 'anchors for followup_or_death_date per feedback_clinical_dates_calendar_only.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'followup_all_sources',
    'followup_category',
    'followup_completeness_score',
    'followup_days',
    'followup_n_contact_sources',
    'followup_or_death_date',
    'followup_or_death_days_from_surg',
    'followup_or_death_years',
    'followup_recovery_method',
    'followup_years'
  );


-- -----------------------------------------------------------------------------
-- 141e — 5 cols — PRM adequacy + voice follow-up completeness (labs × complications × voice cluster)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'prm_rule_followup_adequacy_chain',
    batch_id            = 'mig_141_patient_master_survival_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_141 survival cluster (Lane 30). PRM follow-up adequacy chain vs '
                          || 'thyroglobulin_lab_canonical_v1 / longitudinal_lab_canonical_v1 (Lane 25) + '
                          || 'complication phenotyping (mig_135) + voice/RLN follow-up slice (mig_98c); '
                          || 'INTEGER/BOOLEAN counts per PRM v12 rollup rules.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'prm_followup_clinical_events',
    'prm_followup_has_complications',
    'prm_followup_tg_labs',
    'prm_tg_adequate_followup',
    'voice_followup_completeness'
  );


-- -----------------------------------------------------------------------------
-- 141 — refresh canonical_patient_master row on canonical_table_signoff_registry_v1
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 AS ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_141: SURVIVAL thematic cluster CLOSED (25 cols verified).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'       THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'           THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


COMMIT;


-- =============================================================================
-- end migration 141 — CPM survival cluster (25 verified; Gate 4 complete)
-- =============================================================================
