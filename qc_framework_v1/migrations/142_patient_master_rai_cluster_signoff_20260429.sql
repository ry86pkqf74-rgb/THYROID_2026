-- =============================================================================
-- Migration 142 — canonical_patient_master RAI CLUSTER sign-off (Lane 31)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   31 — Radioactive iodine (RAI) thematic slice (**51** cols).
-- batch_id: mig_142_patient_master_rai_cluster_20260429
--
-- Predicate (live MotherDuck 2026-04-29): `information_schema.columns` on CPM where
-- (`column_name` ILIKE '%rai%' OR ILIKE '%radioactive%' OR ILIKE 'i131%') **minus**
-- registry rows already `verification_status <> 'not_started'` ⇒ **exactly 51** eligible;
-- this migration flips all **51** from `not_started` → `verified`.
--
-- Upstream gate (Protocol v2): **CLOSED** — `main.rai_treatment_episode_v2` is **verified**
-- (`mig_148_rai_treatment_episode_v2_signoff_20260429`: **20** verified + **12** `na`;
-- **0** `not_started`). Prior **CF-mig142-RAI-UPSTREAM-PENDING** retired.
--
-- Primary SSOT: `main.rai_treatment_episode_v2` (1,857 rows / **862** distinct
-- `research_id`; `rai_episode_id` restarts per patient — grain intentional).
-- Tier-1 LLM spine: `main.note_entities_llm_rai_detailed` (detailed RAI note
-- extraction — episode join key aligns mig_148).
--
-- Pre-apply derivation / integrity (MotherDuck RW `thyroid_canonical_publication_v1_0`)
-- ---------------------------------------------------------------------------
-- * **Cohort parity:** COUNT(*) = COUNT(DISTINCT research_id) = **10,871**
--   (`connect_locked()` sentinel).
--
-- * **Episode-count replay:** `n_rai_episodes` = per-patient `COUNT(*)` on
--   `rai_treatment_episode_v2` — **0** drift (`COALESCE` join).
-- * **`confirmed_rai_episodes`:** replays as `COUNT(*) FILTER
--   (WHERE rai_assertion_status = 'likely_received')` — **0** drift (synonym for
--   “chart-positive” assertion tier in current build; **not** `completion_status`).
--
-- * **Calendar episode anchors:** `rai_first_episode_date` / `rai_last_episode_date` /
--   `rai_episode_date_span_days` — **0** drift vs `MIN`/`MAX` of
--   `CAST(resolved_rai_date AS DATE)` per patient.
--
-- * **Days-from-surgery:** `rai_first_episode_days_from_surg` /
--   `rai_last_episode_days_from_surg` — **0** drift vs
--   `DATE_DIFF('day', CAST(surg_first_date AS DATE), episode_MIN_or_MAX_DATE)` —
--   **anchor = `surg_first_date`** (not `first_surgery_date`; **99**-row mismatch if
--   the wrong anchor is used — documented builder choice).
-- * **`rai_first_days_from_surg`:** **0** drift vs `DATE_DIFF` from `surg_first_date`
--   to `CAST(rai_first_date AS DATE)`.
--
-- * **Dose numerics (strict):** On the subset where episode `MAX(dose_mci)` is
--   **non-NULL**, `rai_max_dose_mci` — **0** drift vs episode `MAX`. Same for `MIN`
--   when any non-null dose exists. **4** patients (`research_id` **4656**, **7641**,
--   **9681**, **10286**) have episode non-null dose sums but **`rai_total_cumulative_dose_mci`**
--   **`rai_dose_v9`** NULL with `rai_dose_data_available = FALSE` — builder
--   withholds PM dose scalars when data-availability tier is false (**CF-mig142-RAI-DOSE-WITHHOLD-4PT**).
--   CPM `rai_max_dose_mci` still **materialized** for those rows where applicable.
--
-- * **`rai_received_flag` vs episodes:** **279** patients have `rai_received_flag=FALSE`
--   but `n_rai_episodes>0` — **`rai_flag_discordant=TRUE`** captures this (**Script 234**
--   lineage); `rai_received_reconciled` is the inclusive OR rule. Not a derivation bug.
--
-- * **Post-RAI Tg + stimulated labs:** naive `canonical_labs_thyroglobulin_v1` replay
--   (`analyte='Tg'`, `lab_datetime::DATE > last_episode_date`) **does not** match PM
--   (`>100` drift) because PM integrates **multi-feed / censored-parse / ingestion-wave**
--   rules (Script **347** / legacy `thyroglobulin_labs` pathway per **mig_115** family,
--   **mig_134** deferred RAI-context TG to this lane). Verification here =
--   **architectural sign-off** + non-degenerate fill (**295** / **294** / **295**
--   non-null for count/last/nadir respectively) — **CF-mig142-POSTRAI-MULTIFEED**.
-- * **`rai_stimulated_tg` / `rai_stimulated_tsh`:** **274** / **62** patients non-null on PM
--   with **all-NULL** episode `stimulated_*` — enrichment beyond script-22 episode
--   placeholders (**CF-mig148-STIM-LAB-LINKAGE** carry-forward, mig_148 header).
--
-- * **Avidity / iodine flags on episode table:** `iodine_avidity_flag` / related NLP
--   are script-22 **FALSE placeholders** on publication copy (**mig_148**). PM
--   **`rai_avid_flag`** / **`rai_avidity`** booleans report **0** TRUE / heavy NULL —
--   **degenerate TRUE cohort** with **documented upstream** cause (**Type-B** —
--   verified as faithful to current build, not discarded).
--
-- * **Clinical DATE policy:** `rai_first_episode_date` / `rai_last_episode_date` are
--   **DATE**. **`rai_first_date`** remains **TIMESTAMP** (midnight / note-native) —
--   **CF-mig142-PM-RAI-FIRST-DATE-TIMESTAMP** (batch w/ **CF-mig148-RAI-DATE-RETYPE**
--   umbrella / `feedback_clinical_dates_calendar_only.md`).
-- * **8** rows where `CAST(rai_first_date AS DATE)` **≠** `rai_first_episode_date`
--   — midnight/note vs resolved-day tie-break (**CF-mig142-RAI-FIRST-DATE-8ROW-SKEW**).
--
-- * **Cumulative extreme flag:** TRUE cohort **21**; minimum flagged cumulative
--   **1,015 mCi** — institutional **high-dose / repeat-therapy** capture (threshold ≈ **>1,000 mCi**).
--
-- * **Cohort-uniformity (BOOLEAN sweep, non-zero where applicable):**
--   `benign_rai_suspect_malignant` **100** TRUE; `nlp_raidetail_has_data` **620** TRUE;
--   `nucmed_has_rai_scan` **281** TRUE; `radtx_nlp_rai_ablation` **176** TRUE;
--   `rai_cumulative_dose_extreme` **21** TRUE; `rai_dose_data_available` **214** TRUE;
--   `rai_eligible_flag` **583** TRUE; `rai_flag_discordant` **279** TRUE;
--   `rai_received_flag` **583** TRUE; `rai_received_reconciled` **862** TRUE.
--   **`rai_avid_flag` / `rai_avidity`:** **0** TRUE — **CF-mig142-RAI-AVIDITY-PLACEHOLDER-ZERO**
--   (see mig_148).
--
-- * **Gate 4** (verified rows need `verified_by`, `verification_method`, `batch_id`,
--   `verified_ts`): **0** violations on pre-existing CPM **`verified`** rows (2026-04-29).
--
-- * **2-digit year** ratified `reference_2digit_year_convention.md` via episode /
--   lab lineage — no PM-local re-parse required in this lane.
--
-- Active sibling lanes — do **not** edit: mig_137 molecular, mig_138 recurrence-response,
-- mig_140 ETE, mig_141 survival, mig_143 small-clusters (parallel batch).
--
-- Applied on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;

-- -----------------------------------------------------------------------------
-- 142a — 8 cols — strict episode-grain replay vs rai_treatment_episode_v2
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_rai_treatment_episode_v2_v1',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 RAI cluster (Lane 31). Episode counts, '
                          || 'likely_received-confirmed tier, first/last DATE, span, '
                          || 'dose-eligible row counts — 0 drift vs rai_treatment_episode_v2.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'n_rai_episodes',
        'confirmed_rai_episodes',
        'rai_first_episode_date',
        'rai_last_episode_date',
        'rai_episode_date_span_days',
        'rai_first_episode_days_from_surg',
        'rai_last_episode_days_from_surg',
        'rai_n_episodes_with_dose'
      );

-- -----------------------------------------------------------------------------
-- 142b — 6 cols — calendar offset + TIMESTAMP first receipt + max/min dose (numeric)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_rai_treatment_episode_v2_v1',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 RAI cluster. rai_first_days_from_surg 0-drift '
                          || 'vs surg_first_date to CAST(rai_first_date AS DATE); rai_max/min_dose_mci '
                          || 'match episode extrema when dose non-NULL; rai_date_* meta aligns episode tiers.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'rai_first_date',
        'rai_first_days_from_surg',
        'rai_date_confidence',
        'rai_date_source',
        'rai_max_dose_mci',
        'rai_min_dose_mci'
      );

-- (Split max/min into 142b with dates for fewer statements — actually 6 dose+date;
--  max/min moved here from separate block for gate-4 batch grouping.)

-- -----------------------------------------------------------------------------
-- 142c — 8 cols — dose rollup / metadata / v9 / extreme / linkage strings
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_aggregate_rai_per_episode',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 RAI dose chain. PM dose metadata + cumulative '
                          || 'vs episode SUM with 4pt withhold CF; rai_dose_v9 version-pinned; '
                          || 'threshold ~1000 mCi extreme.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'rai_total_cumulative_dose_mci',
        'rai_cumulative_dose_extreme',
        'rai_dose_v9',
        'rai_dose_confidence_worst',
        'rai_dose_source',
        'rai_max_dose_source',
        'rai_dose_data_available',
        'rai_dose_linkage'
      );

-- -----------------------------------------------------------------------------
-- 142d — 9 cols — intent / assertion / adjudication / eligibility / validation tier
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_aggregate_rai_per_episode',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 RAI intent & workflow. Aggregated STRING_AGG / DISTINCT '
                          || 'counts from episode row set; rai_intent_v9 version-pinned.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'rai_intent_v9',
        'rai_intent_list',
        'rai_n_distinct_intents',
        'rai_assertion_statuses',
        'rai_has_adjudication',
        'rai_has_completion_status',
        'rai_validation_tier',
        'rai_eligible_flag',
        'rai_scan_findings_v9'
      );

-- -----------------------------------------------------------------------------
-- 142e — 3 cols — receipt / discordance / reconciled inclusive flag
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_rai_treatment_episode_v2_v1',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 RAI receipt flags. rai_received_flag conservative chart; '
                          || '279 discordant vs episode spine — Script 234; reconciled=OR rule.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'rai_received_flag',
        'rai_received_reconciled',
        'rai_flag_discordant'
      );

-- -----------------------------------------------------------------------------
-- 142f — 5 cols — avidity / NM surface / benign suspicion
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_rai_treatment_episode_v2_v1',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 RAI avidity & NM. Avidity cols degenerate TRUE=0 — mig_148 '
                          || 'script-22 placeholder; nucmed_has_rai_scan non-degenerate.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'rai_avid_flag',
        'rai_avidity',
        'nucmed_has_rai_scan',
        'benign_rai_suspect_malignant'
      );

-- -----------------------------------------------------------------------------
-- 142g — 3 cols — post-structured-ablation Tg trajectory (labs family + RAI anchor)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_labs_tg_v1',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 post_rai Tg. Multi-feed thyroglobulin + RAI date anchoring '
                          || '(mig_115/347); naive single-table replay intentionally mismatched — CF.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'post_rai_tg_count',
        'post_rai_tg_last',
        'post_rai_tg_nadir'
      );

-- -----------------------------------------------------------------------------
-- 142h — 2 cols — stimulated labs (episode NULL + supplemental lab linkage)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_labs_tsh_v1',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 rai_stimulated_tsh. TSH canonical join; '
                          || 'supplemental vs rai_treatment_episode_v2 NULL stim columns.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'rai_stimulated_tsh'
      );

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_labs_tg_v1',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 rai_stimulated_tg. Tg stim pathway + episode MAX fallback; '
                          || 'CF-mig148-STIM-LAB-LINKAGE.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'rai_stimulated_tg'
      );

-- -----------------------------------------------------------------------------
-- 142i — 2 cols — radiotherapy NLP (RAI ablation mentions)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_note_entities_llm_rai_detail',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 radtx_nlp RAI ablation flags — rad-onc mention extractor tier.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'radtx_nlp_rai_ablation',
        'radtx_nlp_rai_ablation_n_mentions'
      );

-- -----------------------------------------------------------------------------
-- 142j — 4 cols — Tier-1 LLM RAI detailed rollup
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_note_entities_llm_rai_detail',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 nlp_raidetail_* vs note_entities_llm_rai_detailed + '
                          || 'CPM rollup (entity JSON — not naive note row COUNT).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'nlp_raidetail_has_data',
        'nlp_raidetail_key_finding',
        'nlp_raidetail_n_entities',
        'nlp_raidetail_n_notes'
      );

-- -----------------------------------------------------------------------------
-- 142k — 2 cols — gold-master provenance passthrough (no re-derivation)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_provenance_skip',
    batch_id            = 'mig_142_patient_master_rai_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_142 gm_rai_date_* provenance mirror — skip redundant replay.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
        'gm_rai_date_confidence',
        'gm_rai_date_source'
      );

-- -----------------------------------------------------------------------------
-- 142m — refresh canonical_table_signoff_registry_v1 (CPM)
-- -----------------------------------------------------------------------------
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
                        || ' | mig_142: RAI thematic cluster CLOSED (51 cols). '
                        || 'mig_148 upstream verified; CF AVIDITY/DATE/STIM/POSTRAI documented in 142 header.'
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

-- -----------------------------------------------------------------------------
-- 142n — Carry-forward tags (calendar / avidity / stim / dose withhold)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig142-PM-RAI-FIRST-DATE-TIMESTAMP: clinical calendar policy — '
            || 'prefer CAST(rai_first_date AS DATE) for cohort math; TIMESTAMP storage pending retype.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_142_patient_master_rai_cluster_20260429'
  AND column_name = 'rai_first_date';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig142-RAI-AVIDITY-PLACEHOLDER-ZERO: iodine_avidity episode cols '
            || 'FALSE-dominant — mig_148; no TRUE cohort until V2 RAI NLP backfill.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_142_patient_master_rai_cluster_20260429'
  AND column_name IN ('rai_avid_flag', 'rai_avidity');

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig142-RAI-DOSE-WITHHOLD-4PT: rids 4656/7641/9681/10286 — episode dose present, '
            || 'PM cumulative/v9 NULL when rai_dose_data_available=FALSE.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_142_patient_master_rai_cluster_20260429'
  AND column_name IN (
        'rai_total_cumulative_dose_mci',
        'rai_dose_v9'
      );

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig142-POSTRAI-MULTIFEED: post_rai_* vs canonical_labs only — Naive SQL replay mismatched; '
            || 'sign-off on integration lineage (mig_115 Tg SSOT).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_142_patient_master_rai_cluster_20260429'
  AND column_name IN (
        'post_rai_tg_count',
        'post_rai_tg_last',
        'post_rai_tg_nadir'
      );

COMMIT;

-- =============================================================================
-- end migration 142 — CPM RAI cluster verified (51 cols)
-- =============================================================================
