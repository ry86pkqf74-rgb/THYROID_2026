-- =============================================================================
-- Migration 161 — mig_155 INDEPENDENT RE-VERIFICATION (Path C after-the-fact)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Cursor agent (read-only audit) + Logan Glosser <logan.glosser@gmail.com>
--
-- Prompt: cursor_prompts/CURSOR_PROMPT_mig155_independent_reverification_20260429.md
-- Lane:   mig_161 — retroactive Path-C audit of agent-applied mig_155 (31 cols).
--
-- EFFECT: Registry **notes** appendices only (`verification_status` unchanged).
--         **No** `verified` → `na` reclassification (no Type-B all-FALSE degenerates).
--         **No** MotherDuck writes from agent — Logan applies after review.
-- =============================================================================
--
-- SECTION A — VERIFICATION REPORT (MotherDuck `thyroid_canonical_publication_v1_0`, read-only)
--
-- §1a Scope — `canonical_column_verification_registry_v1` batch_id =
--   `mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429`
--   * Total rows: **31** (PASS).
--   * verification_method histogram:
--       derivation_vs_canonical_path_malignant_patient_rollup_v1 : 11
--       derivation_vs_canonical_survival_followup_v1             : 5
--       derivation_vs_canonical_recurrence_v1                    : 4
--       derivation_vs_note_entities_llm_dynamic_risk_response    : 3
--       auto_provenance_skip                                     : 3
--       internal_consistency_scoring_eligibility_vs_calculable_flags : 3
--       derivation_vs_canonical_molecular_genetics_v2            : 2
--
-- §2a Live-table-name audit — named SSOTs for methodology strings:
--   * `canonical_path_malignant_patient_rollup_v1` — EXISTS (main)
--   * `canonical_survival_followup_v1` — EXISTS (main)
--   * `canonical_recurrence_v1` — EXISTS (main)
--   * `note_entities_llm_dynamic_risk_response` — EXISTS (main)
--   * `canonical_molecular_genetics_v2` — EXISTS (main)
--   → **CF-mig161-MIG155-DEAD-TABLE-*** : **none opened** (all live).
--
--   Lineage nuance (informational, not dead-table):
--   * Script **211** `SURVIVAL_SQL` rolls `surv_*` from **`survival_cohort_enriched`**
--     (`MAX(time_days)`, `COUNT(*) FILTER (WHERE event)`, etc.).
--   * **`survival_cohort_enriched` is NOT present** in publication `main` (only archived
--     attach); after-the-fact replay of day-level MAX(time_days) inside publication DB is
--     **not** available without restoring that feeder. Registry still correctly names
--     `derivation_vs_canonical_survival_followup_v1` for governance family; analytic
--     `surv_*` grain remains **orthogonal** to mig_141 calendar LKA spine per mig_155
--     header doctrine.
--
-- §2b BOOLEAN cohort-uniformity (canonical_patient_master, n=10,871) — T / F / NULL:
--   * ata_calculable_flag             : 3144 / 7727 / 0
--   * ata_response_calculable_flag    : 10871 / 0 / 0  → Type-A near-uniform TRUE
--   * ata_response_is_provisional     : 10871 / 0 / 0  → Type-A near-uniform TRUE
--   * ata_risk_calculable_flag        : 3144 / 7727 / 0
--   * ames_calculable_flag            : 10871 / 0 / 0  → Type-A near-uniform TRUE
--   * macis_calculable_flag           : 4082 / 6789 / 0
--   * biochemical_recurrence_flag   : 128 / 1818 / 8925
--   * structural_recurrence_flag      : 1818 / 128 / 8925
--   * distant_mets_proxy              : 1818 / 9053 / 0
--   * distant_mets_proxy_v2         : 154 / 10717 / 0
--   * genetics_master_v1_link_flag   : 1225 / 9646 / 0
--   * scoring_ajcc8_flag              : 4083 / 6788 / 0
--   * scoring_ata_flag                : 3144 / 7727 / 0
--   * scoring_macis_flag              : 4082 / 6789 / 0
--   → **CF-mig161-MIG155-COHORT-UNIFORM-FALSE-*** : **none** (no all-FALSE BOOL).
--   → **CF-mig161-MIG155-COHORT-NEAR-UNIFORM-TRUE-*** :
--        ata_response_calculable_flag, ata_response_is_provisional, ames_calculable_flag
--
-- §2c Single-value / low-cardinality VARCHAR audit:
--   * resolved_layer_version : distinct **1** (value `v1`), null 0
--        → **CF-mig161-MIG155-VALUE-DEGENERATE-UPSTREAM-resolved_layer_version**
--   * ata_initial_risk / ata_risk_category : 3 distinct ; 7727 NULL (eligible shell)
--   * ata_response_category : 5 distinct ; 10836 NULL
--   * ames_risk / ames_risk_group : 2 distinct ; 0 NULL
--   * macis_risk_group : 4 distinct
--   * macis_missing_components : non-null payload single token **`size`** for 6789 rows
--        (MACIS not-calculable list degeneracy — builder literal; not “empty VARCHAR”)
--   * surv_recurrence_risk_band : 3 distinct ; 7107 NULL
--
-- §2d Cross-canonical reconciliation (recurrence proxies vs `canonical_recurrence_v1`):
--   * Literal prompt SQL (`recurrence_type LIKE '%biochem%'` on `recurrence_confirmed`) →
--     **0** canonical patients — **taxonomy mismatch** (v1 types are e.g.
--     `structural_confirmed`, `fna_confirmed`, not the string `biochemical`).
--     PM `biochemical_recurrence_flag=TRUE` = **128**; Tg-at-recurrence non-null on
--     canonical confirmed rows = **0** — proxies reflect **recurrence_event_clean / CPM
--     builder** semantics, not naive equality on `recurrence_type` text.
--   * Structural: `structural_recurrence_flag` TRUE = **1818** vs distinct canonical
--     patients where `recurrence_type` contains `struct` = **456** — large naive
--     discord documented in mig_155 (**CF-mig155-STRUCTURAL-PROXY-VS-CANONICAL-V1**).
--   * Distant: broad site/type heuristics on canonical still **0** matches vs
--     **1818** PM TRUE on `distant_mets_proxy` — same doctrine (builder ≠ naive row match).
--   → **CF-mig161-MIG155-RECURRENCE-PROXY-DRIFT-biochemical_recurrence_flag** /
--     **structural_recurrence_flag** / **distant_mets_proxy** :
--     **informational** — re-confirm mig_155 / mig_138 family; **not** a call for
--     automatic `verified→na` on the drift magnitude alone.
--
-- §2e Date-type check:
--   * `resolved_at` → **TIMESTAMP WITH TIME ZONE** in `information_schema` — audit /
--     provenance allowlist (PASS); not a calendar clinical event date.
--
-- §2f Spot-check — first five `ata_risk_category='high'` by `research_id`:
--   * rids 10003,10004,10005,10009,10014 — CPM shows T1a–T2, microscopic ETE, margins
--     uninvolved, LN 0/NULL on sampled cols; high band plausibly driven by **path
--     malignant rollup** features beyond this narrow excerpt (ATA ladder OK at grain
--     of feeder, not a contradiction on size alone).
--
-- §2g `surv_max_time_days` vs `canonical_survival_followup_v1.days_from_first_surgery_to_last_contact`:
--   * **≈10,844 / 10,871** rows `IS DISTINCT FROM` (not the same analytic definition).
--   * Confirms **CF-mig161-MIG155-SURV-VS-MIG141-CROSS** — `surv_*` from survival-cohort
--     analytic grain (script 211) vs LKA day spine (mig_141 family); mig_155 header
--     already asserts orthogonality.
--
-- §2h Internal consistency:
--   * `ata_initial_risk` **IS NOT DISTINCT FROM** `ata_risk_category` on **10,871 / 10,871**.
--   * `scoring_ajcc8_flag` vs `macis_calculable_flag` off-by-**1** patient (**4083** vs **4082**)
--     — **CF-mig161-MIG155-SCORING-AJCC8-VS-MACIS-OFFBY1** (carry-forward from mig_155).
--
-- =============================================================================
-- SECTION B — Registry note appendices (apply after Logan review)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- B0 — Global Path-C stamp (all 31 mig_155 cols)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_161: Path-C independent re-verification (2026-04-29 agent read-only). '
            || 'batch cardinality=31; all named main SSOTs exist; no DEAD-TABLE CF; '
            || 'no Type-B all-FALSE BOOL; recurrence naive-LIKE probes superseded by v1 taxonomy '
            || '(see per-column CF-mig161-MIG155-RECURRENCE-* notes on proxy cols); '
            || 'surv_* grain vs LKA spine per mig_155 / script 211 doctrine.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND batch_id = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429';

-- -----------------------------------------------------------------------------
-- B1 — Type-A near-uniform TRUE (mig_142b pattern)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_161: CF-mig161-MIG155-COHORT-NEAR-UNIFORM-TRUE — all TRUE / 0 FALSE / NULL; '
            || 'presence or builder envelope at patient grain; keep verified (informational).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND batch_id = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
  AND column_name IN (
    'ata_response_calculable_flag',
    'ata_response_is_provisional',
    'ames_calculable_flag'
  );

-- -----------------------------------------------------------------------------
-- B2 — Degenerate single-value provenance VARCHAR
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_161: CF-mig161-MIG155-VALUE-DEGENERATE-UPSTREAM-resolved_layer_version '
            || '(1 distinct = v1; audit placeholder OK per mig_142b).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND batch_id = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
  AND column_name = 'resolved_layer_version';

-- -----------------------------------------------------------------------------
-- B3 — Scoring eligibility off-by-one (informational)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_161: CF-mig161-MIG155-SCORING-AJCC8-VS-MACIS-OFFBY1 — scoring_ajcc8_flag '
            || '(4083 TRUE) vs macis_calculable_flag (4082 TRUE); mirrors mig_155 CF; keep verified.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND batch_id = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
  AND column_name IN ('scoring_ajcc8_flag', 'macis_calculable_flag');

-- -----------------------------------------------------------------------------
-- B4 — Recurrence proxy drift / taxonomy (naive prompt SQL vs v1 canonical)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_161: CF-mig161-MIG155-RECURRENCE-PROXY-DRIFT-biochemical_recurrence_flag — '
            || 'naive recurrence_type LIKE ''%biochem%'' yields 0 v1 rows; PM TRUE=128 from builder; '
            || 'mig_155 CF-mig155-RECURRENCE-PROXY-VS-CANONICAL-V1 doctrine stands.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND batch_id = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
  AND column_name = 'biochemical_recurrence_flag';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_161: CF-mig161-MIG155-RECURRENCE-PROXY-DRIFT-structural_recurrence_flag — '
            || 'large naive discord vs struct* recurrence_type rollups; expected per mig_155 '
            || 'CF-mig155-STRUCTURAL-PROXY-VS-CANONICAL-V1 (typed canonical vs CPM display shell).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND batch_id = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
  AND column_name = 'structural_recurrence_flag';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_161: CF-mig161-MIG155-RECURRENCE-PROXY-DRIFT-distant_mets_proxy — '
            || 'naive distant heuristics on canonical recurrence_site/type hit 0; PM TRUE=1818; '
            || 'builder semantics per mig_138/mig_155 family; distant_mets_proxy_v2 tracked separately.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND batch_id = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
  AND column_name IN ('distant_mets_proxy', 'distant_mets_proxy_v2');

-- -----------------------------------------------------------------------------
-- B5 — Survival aggregates vs mig_141 / LKA spine (orthogonal grain)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_161: CF-mig161-MIG155-SURV-VS-MIG141-CROSS — surv_* verified as analytic '
            || 'aggregates (script 211 survival cohort grain); ≠ days_from_first_surgery_to_last_contact '
            || 'for ~10,844/10,871 rows; feeder survival_cohort_enriched not in publication main; '
            || 'orthogonality to mig_141 DATE stack per mig_155 header.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND batch_id = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
  AND column_name IN (
    'surv_max_time_days',
    'surv_max_time_days_capped',
    'surv_n_events',
    'surv_recurrence_risk_band',
    'surv_tg_annual_log_slope'
  );

-- End mig_161. Apply on MotherDuck RW only after Logan sign-off. No PM column data mutations.
