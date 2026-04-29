-- =============================================================================
-- Migration 147 — canonical_patient_master NUCLEAR MEDICINE (+ NM-stim labs) CLUSTER
-- =============================================================================
-- Protocol v2 · Lane 37 · batch_id mig_147_patient_master_nucmed_imaging_cluster_20260429
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Pre-apply probes (MotherDuck RW thyroid_canonical_publication_v1_0, 2026-04-29)
-- ─────────────────────────────────────────────────────────────────────────────
--   * `information_schema.columns`: exactly **27** `nucmed_%` columns on
--     `main.canonical_patient_master` (predicate matches Lane-37 probe with
--     `verification_status <> 'not_started'` anti-join = **0** already-verified
--     overlaps — entire nucmed thematic set was **`not_started`**).
--   * Cohort parity: **`_md_connect.connect_locked`** asserts 10,871 / 10,871
--     rows / distinct `research_id`.
--   * **`main.nuclear_med`** EXISTS (exam grain — SSOT for NM report scrape).
--   * Tier-1 LLM **`note_entities_llm_nucmed_%` / `%imaging_nucmed%`**:
--     **0** persisted tables on publication catalog
--     (**CF-mig147-NM-LLM-NO-PERSISTED-ENTITIES** — verification is structured
--     `nuclear_med` + scripts **218**/ **219**/ **286**/ **347** overlays, not
--     standalone LLM entity replay).
--
-- Scope adjustment (collision with mig_142 / RAI)
-- ─────────────────────────────────────────────────────────────────────────────
--   **`nucmed_has_rai_scan`** — BOOL_OR over `LOWER(scantype)` iodine-like tokens
--   vs **`rai_*`** patient-level aggregates (mig_142 Lane 31). Per Protocol,
--   **EXCLUDED from this lane’s verified flip** (**remains `not_started`**);
--   **`151b`**‑style notes-only stamp documents deferral. **→ 26 columns verified
--   in this migration; `n_prior`/`n_verified` delta = 26.**

-- Upstream / derivation chain (§2a–2b)
-- ─────────────────────────────────────────────────────────────────────────────
--   * **Exam rollups:** `COUNT(*)`/`STRING_AGG(scantype)`/`BOOL_OR`/MAX uptake
--     from **`main.nuclear_med`** — matches frozen **`scripts/frozen/207_canonical_master_expansion.py`**
--     **`nm`** CTE (+ later CPM merges). Script **`219_imaging_gap_resolution.py`**
--     Tasks 4 (`_nucmed_expanded_v1`/`_nucmed_agg_v1`): indication/impression/
--     findings first/last, dose parse, Tg/TgAb/TSH from **`lab_summary` +
--     `scan_present`**, uptake %, **`nucmed_overall_assessment`** severity ladder.
--   * **Stim-lab linkage / first-last scan-with-labs:** **`scripts/218_followup_recovery.py`**
--     **`_nucmed_labs_rollup_v1`** — **`MIN`/`MAX(scandate)`** as rollup keys
--     (**stored as VARCHAR on CPM** — **CF-mig147-PM-NUCMED-DATE-VARCHAR**, pair
--     with calendar-only policy / TRY_CAST remediation; not TIMESTAMP drift).
--   * **TgAb max (+ source):** Script **347** registry lists **`nucmed_tgab_max`** as
--     feeding from **`canonical_labs_thyroglobulin_v1`** (+ **286**
--     **`nucmed_tgab_max_backfill_v1`** remediation path documented in mig_129).

-- Cross-cluster RAI dose (§2d–2b) — INFORMATIONAL ONLY; do NOT UPDATE `rai_*`
-- ─────────────────────────────────────────────────────────────────────────────
--   Live probe: **`nucmed_cumulative_therapeutic_dose`** vs
--   **`rai_total_cumulative_dose_mci`** on **48** mutually non-null positives —
--   **40** pairs exceed **10%** relative discrepancy (NM narrative-parsed therapeutic
--   administrations vs episodic **`rai_treatment_episode_v2`** summation semantics).
--   **`rai_total_cumulative_dose_mci`** remains SSOT for RAI therapeutic totals —
--   **`CF-mig147-NUCMED-VS-RAI-DOSE-SOURCE-SPLIT`**. Verification here attests
--   **`nuclear_med`/219 parse faithfulness**, not numeric lockstep to RAI column.

-- Cohort-uniformity (§2c) —MotherDuck spot-check 2026-04-29
-- ─────────────────────────────────────────────────────────────────────────────
--   * **`nucmed_tsh_max` > 30** rows ⇒ **62**; **`nucmed_tsh_is_stimulated` TRUE**
--     ⇒ **62**; **0** high-TSH rows with stimulated flag FALSE.
--   * **`nucmed_overall_assessment`** (when non-null): `other`/`NED`/`metastasis_mentioned`
--     **`thyroid_bed_only`** present — non-degenerate ladder.

-- Gate 4 (verified metadata completeness holes)
-- ─────────────────────────────────────────────────────────────────────────────
--   Pre-apply **`verified + (NULL verified_by OR method OR batch_id OR verified_ts)`**:
--   **0** rows (**“gate 4 = 0”** sentinel).
--
-- Parallel lanes — leave untouched here: **mig_142** RAI PM, mig_143 SmallClusters,
-- mig_144–146 sibling imaging thematic clusters (`rai_*` columns verbatim off-limits).
--
-- =============================================================================

BEGIN TRANSACTION;


-- ---------------------------------------------------------------------------
-- 147a — Nuclear-medicine exam metadata (main.nuclear_med patient rollups)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_nucmed_nuclear_med_v1',
    batch_id            = 'mig_147_patient_master_nucmed_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_147 NM cluster (Lane 37). Patient-level aggregates '
                          || 'replay `main.nuclear_med` + DISTINCT ON rollup (207/219): '
                          || '`nucmed_n_scans`, stratified indications, LIST_SORT-style '
                          || 'STRING_AGG for `nucmed_scan_types`.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nucmed_n_scans',
    'nucmed_scan_types',
    'nucmed_indication_first',
    'nucmed_indication_last',
    'nucmed_n_with_indication'
  );


-- ---------------------------------------------------------------------------
-- 147b — Impression / findings (last-scan text windows)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_nucmed_nuclear_med_v1',
    batch_id            = 'mig_147_patient_master_nucmed_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_147 NM impressions (Lane 37). LAST_VALUE / DISTINCT ON '
                          || '`nucmed_impression_*`/`nucmed_findings_*` keyed by parsed scan DATE; '
                          || '`nucmed_n_with_impression` counts rows with LENGTH>5.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nucmed_findings_last',
    'nucmed_impression_last',
    'nucmed_n_with_impression'
  );


-- ---------------------------------------------------------------------------
-- 147c — Therapeutic dose parse + cross-check informational note (rai_* untouched)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'cross_validate_dose_nucmed_vs_rai',
    batch_id            = 'mig_147_patient_master_nucmed_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_147 NM doses (Lane 37). Parsed from NM report narrative '
                          || '(219 regex `administration of|administered … mCi`); SUM where '
                          || 'parsed dose_mci >10 treated as therapeutic. Pre-apply cross-sum vs '
                          || '`rai_total_cumulative_dose_mci`: 48 overlapping patients — 40 '
                          || 'pairs >10% rel diff — **CF-mig147-NUCMED-VS-RAI-DOSE-SOURCE-SPLIT**. '
                          || 'Verification = NM-lineage fidelity; RAI column is episodic SSOT.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nucmed_cumulative_therapeutic_dose',
    'nucmed_dose_max_parsed',
    'nucmed_n_doses_parsed'
  );


-- ---------------------------------------------------------------------------
-- 147d — Stim-context labs (218/219 rollup) + selective Script 347 Tg/TgAb path
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_labs_thyroglobulin_v1',
    batch_id            = 'mig_147_patient_master_nucmed_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_147 NM-stim tg/tgab (Lane 37). Regex parse from NM '
                          || '`lab_summary`/`scan_present` + Script 347 `canonical_labs_thyroglobulin_v1` '
                          || 'for `nucmed_tgab_max` / `_source` pathways (detail_table_registry feeds).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nucmed_tg_max',
    'nucmed_tg_min',
    'nucmed_tgab_max',
    'nucmed_tgab_max_source',
    'nucmed_n_tg_values',
    'nucmed_n_tgab_values'
  );


UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_labs_tsh_v1',
    batch_id            = 'mig_147_patient_master_nucmed_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_147 NM TSH (Lane 37). Parses TSH embedded in NM `lab_summary` '
                          || 'text (218/219 regex). Stimulation threshold BOOLEAN uses **30 mIU/L** '
                          || 'parity with `_nucmed_labs_parsed_v1.tsh_is_stimulated`. Cross-check '
                          || 'against `canonical_labs_tsh_v1` is secondary for **NM-derived** subsets.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nucmed_tsh_max',
    'nucmed_tsh_is_stimulated',
    'nucmed_n_tsh_values'
  );


UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_aggregate_nucmed_per_scan',
    batch_id            = 'mig_147_patient_master_nucmed_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_147 stim scan linkage (Lane 37). **`nucmed_first_scan_with_labs`/'
                          || '`_last_scan_with_labs`**: DATE-intent rollup from **`scandate`** as '
                          || 'persisted (**VARCHAR** on CPM) — CF-mig147-PM-NUCMED-DATE-VARCHAR.'
                          || ' **`nucmed_lab_source`** literal `nuclear_med.lab_summary`.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nucmed_first_scan_with_labs',
    'nucmed_last_scan_with_labs',
    'nucmed_lab_source'
  );


-- ---------------------------------------------------------------------------
-- 147e — Structured uptake metrics (exam MAX rollups vs % channel)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_nucmed_nuclear_med_v1',
    batch_id            = 'mig_147_patient_master_nucmed_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_147 uptake (Lane 37). **`nucmed_uptake_24hr_max`** coerced from '
                          || '`uptake_24hour` STRUCTURED column (207-style DOUBLE TRY_CAST vs varchar '
                          || 'staging). **`nucmed_uptake_pct_max`** takes MAX over 24hr% + fallback '
                          || '`uptake_general` strip-% path (219 `nucmed_uptake_*` CTE logic). '
                          || 'Cross-concept NM uptake vs **`rai_avidity` / `rai_avid_flag`** is '
                          || 'orthogonal — **leave `rai_*` to mig_142.**'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nucmed_uptake_24hr_max',
    'nucmed_uptake_pct_max'
  );


-- ---------------------------------------------------------------------------
-- 147f — Worst-severity categorical ladder over impression heuristic
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_nucmed_nuclear_med_v1',
    batch_id            = 'mig_147_patient_master_nucmed_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_147 `nucmed_overall_assessment` (Lane 37). Severity ladder ranks '
                          || 'exam-level classes then applies MAX rank → patient VARCHAR (`metastasis_mentioned`/'
                          || '`NED`/`thyroid_bed_only`/`other`).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name = 'nucmed_overall_assessment';


-- ---------------------------------------------------------------------------
-- 147g — DEFER **`nucmed_has_rai_scan`** (NOT verified this lane — mig_142 collision)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes               = COALESCE(notes,'')
                          || ' | DEFERRED mig_142 RAI canonical PM verification — `nucmed_has_rai_scan` '
                          || '(BOOL_OR iodine-/RA-like `scantype` tokens vs `rai_*` dosing / intent '
                          || 'grain) deliberately **left not_started**; reconcile after Lane 31 **`rai_*`** '
                          || 'Patient Master slice.**'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND column_name = 'nucmed_has_rai_scan'
  AND verification_status = 'not_started';


-- ---------------------------------------------------------------------------
-- 147h — refresh canonical_table_signoff_registry_v1 aggregates for CPM
-- ---------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
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
                        || ' | mig_147: Nucmed thematic cluster CLOSED (**26**/27 cols verified; '
                        || '`nucmed_has_rai_scan` DEFERRED mig_142).'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


COMMIT;

-- =============================================================================
-- end migration 147 — NM patient_master slice (**26 verified**, **1 defer**)
-- =============================================================================
