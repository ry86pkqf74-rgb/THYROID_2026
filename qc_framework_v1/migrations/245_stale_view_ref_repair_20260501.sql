-- =============================================================================
-- mig_245 — Stale view reference repair (8 silently-broken views)
-- Date:    2026-05-01
-- Author:  Cowork (post-v17 closeout, pre-v19)
-- Lane:    Cowork-direct (mechanical rename; no agent dispatch needed)
-- Tip of origin/main at apply: <closeout commit e4254fe>
-- Round bound: post-v17 closeout, slotted ahead of v19 manuscript drafting
-- =============================================================================
--
-- BUG SURFACED:
--   ChatGPT's 2026-05-01 cleanup audit (after v17 round) flagged that the
--   manuscript_workspace.cohort_m011 / m025 / m045 / m075 cohort views
--   reference `main.canonical_us_patient_master_v2` (without `_VIEW_` infix),
--   while the live object is `main.canonical_us_patient_master_VIEW_v2`.
--
-- COWORK VERIFIED LIVE on 2026-05-01:
--   1. Pulled the 4 cohort view DDLs via information_schema.views — confirmed
--      they all do `LEFT JOIN main.canonical_us_patient_master_v2 AS cupm`.
--   2. Tested `SELECT COUNT(*) FROM manuscript_workspace.cohort_m025_tirads_performance_v1`
--      — got `Catalog Error: Table with name canonical_us_patient_master_v2
--      does not exist! Did you mean "canonical_us_patient_master_VIEW_v2"?`
--   3. Ran a regex scan across ALL non-archive views in the publication DB for
--      stale references to the 7 known `_VIEW_v1` / `_VIEW_v2` objects in `main`
--      (canonical_path_malignant_events_dedup, canonical_us_exam_master_v2,
--      canonical_us_patient_master_v2, longitudinal_lab_v1,
--      molecular_fusions_unnested_v2, molecular_variants_unnested_v2,
--      thyroglobulin_lab_v1).
--   4. Found 8 broken views total — 4 in manuscript_workspace + 4 in
--      views_readable. Live-tested each one; all 8 fail at execution time.
--
-- DOWNSTREAM IMPACT:
--   * T8 Dive (TIRADS Decision Support, serves M11+M75) — broken
--   * M025 Dive (ACR TI-RADS Performance, marked "Ready to Submit") — broken
--   * M045 Dive (Multimodal Risk Stratification, marked "GREEN") — broken
--   * Genetics_Variants, US_Lymph_Nodes_Wide_v2, US_Nodules_Wide_v2,
--     US_Thyroid_Gland_Wide_v2 in views_readable — broken (collaborator-facing)
--
-- FIX (this migration):
--   Mechanical rename in the 8 view bodies — swap each bare `_v2` / `_v1`
--   identifier for its `_VIEW_v2` / `_VIEW_v1` equivalent. Preserves the column
--   list of every view exactly. No Dive contract shifts.
--
-- NOT A GATE1 EVENT:
--   These 8 views live in `manuscript_workspace` + `views_readable`. They are
--   NOT in `canonical_table_signoff_registry_v1` (which tracks main +
--   semantic_publication only). gate1 stays at 218.
--
-- =============================================================================
-- §1 — Manuscript workspace cohort views (4 views; 4× small DDLs)
-- =============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m011_tirads_fna_genetics_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.surg_procedure_type,
  p.is_malignant,
  p.histology_final,
  p.path_tumor_size_cm AS tumor_size_cm,
  cupm.tirads_category_at_first_exam AS tirads_best_category_v12,
  cupm.max_tirads_category_ever AS tirads_worst_category_v12,
  CAST(substr(cupm.tirads_category_at_first_exam, 3) AS BIGINT) AS tirads_best_score_v12,
  cupm.max_nodule_size_mm AS tirads_nodule_size_max_mm_v12,
  p.bethesda_final,
  p.n_fna_episodes,
  p.fna_path_concordance_category,
  p.mol_has_thyroseq,
  p.mol_has_afirma,
  p.molecular_tested_confirmed,
  p.molecular_risk_tier,
  p.braf_positive_final,
  p.ras_positive_final,
  p.ajcc8_stage_group,
  p.ata_risk_category,
  p.ln_positive_flag,
  p.any_recurrence_flag,
  p.overall_survival_years
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE cupm.tirads_category_at_first_exam IS NOT NULL;

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m025_tirads_performance_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.race,
  cupm.tirads_category_at_last_preop_exam AS preop_tirads_category,
  cupm.tirads_category_at_first_exam AS tirads_best_category_v12,
  cupm.max_tirads_category_ever AS tirads_worst_category_v12,
  CAST(substr(cupm.tirads_category_at_first_exam, 3) AS BIGINT) AS tirads_best_score_v12,
  CAST(substr(cupm.max_tirads_category_ever, 3) AS BIGINT) AS tirads_worst_score_v12,
  cupm.tirads_worst_rank_source AS tirads_worst_rank_source,
  cupm.n_us_exams AS n_us_exams,
  p.dominant_nodule_size_cm AS imaging_nodule_size_cm,
  p.dominant_nodule_size_cm,
  p.bethesda_final,
  p.bethesda_final_name,
  p.histology_final,
  p.is_malignant,
  p.path_tumor_size_cm AS tumor_size_cm,
  p.path_tumor_size_cm,
  p.fna_path_concordance_category,
  p.fna_path_concordant,
  p.surg_procedure_type,
  p.surg_first_date
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE cupm.tirads_category_at_last_preop_exam IS NOT NULL
   OR cupm.tirads_category_at_first_exam IS NOT NULL;

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m045_multimodal_risk_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  cupm.tirads_category_at_last_preop_exam AS preop_tirads_category,
  cupm.tirads_category_at_first_exam AS tirads_best_category_v12,
  CAST(substr(cupm.tirads_category_at_first_exam, 3) AS BIGINT) AS tirads_best_score_v12,
  p.bethesda_final,
  p.bethesda_final_name,
  p.molecular_tested_confirmed,
  p.molecular_risk_tier,
  p.histology_final,
  p.is_malignant,
  p.path_tumor_size_cm AS tumor_size_cm,
  p.multifocal_flag_path,
  p.ete_grade_final,
  p.ln_positive_flag,
  p.ajcc8_stage_group,
  p.ata_risk_category,
  p.surg_procedure_type,
  p.any_recurrence_flag,
  p.followup_years,
  p.surg_first_date
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE p.bethesda_final IS NOT NULL
  AND p.histology_final IS NOT NULL
  AND (cupm.tirads_category_at_last_preop_exam IS NOT NULL
       OR cupm.tirads_category_at_first_exam IS NOT NULL);

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m075_tirads_multi_nodule_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.surg_procedure_type,
  p.is_malignant,
  p.histology_final,
  p.path_tumor_size_cm AS tumor_size_cm,
  cupm.tirads_category_at_first_exam AS tirads_best_category_v12,
  cupm.max_tirads_category_ever AS tirads_worst_category_v12,
  CAST(substr(cupm.tirads_category_at_first_exam, 3) AS BIGINT) AS tirads_best_score_v12,
  CAST(substr(cupm.max_tirads_category_ever, 3) AS BIGINT) AS tirads_worst_score_v12,
  cupm.max_nodule_size_mm AS tirads_nodule_size_max_mm_v12,
  cupm.n_nodule_records AS tirads_n_nodule_records_v12,
  p.bethesda_final,
  p.n_fna_episodes,
  p.fna_path_concordance_category,
  p.molecular_tested_confirmed,
  p.ajcc8_stage_group,
  p.ata_risk_category,
  p.ln_positive_flag,
  p.any_recurrence_flag,
  p.overall_survival_years
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE cupm.tirads_category_at_first_exam IS NOT NULL;

-- =============================================================================
-- §2 — views_readable.Genetics_Variants (tiny passthrough)
-- =============================================================================

CREATE OR REPLACE VIEW views_readable.Genetics_Variants AS
SELECT * FROM main.molecular_variants_unnested_VIEW_v2;

-- =============================================================================
-- §3 — views_readable wide-pivot views (3 views; large DDLs ~4.5 / 38 / 44 KB)
-- =============================================================================
--
-- The 3 wide-pivot views are mechanical pivots of the live US data:
--   * US_Thyroid_Gland_Wide_v2   — pivots canonical_us_thyroid_gland_v2 by us_exam_rank 1..5
--   * US_Lymph_Nodes_Wide_v2     — pivots canonical_us_lymph_node_v2  by us_exam_rank 1..5 × us_ln_index_within_exam 1..8
--   * US_Nodules_Wide_v2         — pivots canonical_us_nodule_v2      by us_exam_rank 1..5 × us_nodule_index_within_exam 1..8
--
-- §3.A — Stale-ref fix (all 3 wide pivots):
--   Each one's stale reference is the JOIN target `main.canonical_us_exam_master_v2`
--   (used to attach `exam_rank_for_patient`). Fix is a single non-word-boundary
--   substitution: `canonical_us_exam_master_v2` -> `canonical_us_exam_master_VIEW_v2`.
--
-- §3.B — Additional column-rename fix (US_Nodules_Wide_v2 ONLY):
--   While verifying §3.A for US_Nodules_Wide_v2, Cowork discovered a SECOND bug:
--   the view DDL referenced `tirads_category_v2` from canonical_us_nodule_v2, but
--   that column was dropped/renamed at some point pre-mig_245. Live candidate
--   columns on canonical_us_nodule_v2 are:
--     * `acr2017_tirads_category`    (VARCHAR)  — ACR-standard category 'TR1'..'TR5'
--     * `updated_tirads_category`    (VARCHAR)  — curated final after conflict resolution
--     * `tirads_reported_in_text`    (INTEGER)  — raw integer extracted from report text
--     * `source_tirads_v2`           (BOOLEAN)  — flag (was tirads_v2 a source?)
--     * `acr2017_band_ambiguous`     (BOOLEAN)  — flag
--     * `acr2017_vs_updated_concordant` (BOOLEAN) — flag
--
--   Logan elected (during mig_245 execution) to expose BOTH `acr2017_tirads_category`
--   and `updated_tirads_category` as separate columns in US_Nodules_Wide_v2.
--   Each `us_X_nodule_Y_tirads` cell in the pre-mig_245 DDL was split into TWO cells:
--     * `us_X_nodule_Y_tirads_acr2017`  (sourced from acr2017_tirads_category)
--     * `us_X_nodule_Y_tirads_updated`  (sourced from updated_tirads_category)
--
--   Net column-count change for US_Nodules_Wide_v2: +40 cols
--   (5 exam_ranks × 8 nodule_index = 40 tirads cells, each split into 2).
--
-- §3.C — How the wide pivots were applied:
--   Because the pivot DDLs are large (~4.5KB / 38KB / 50.7KB post-rewrite), the
--   in-line SQL is NOT reproduced here. The exact SQL applied was constructed
--   server-side via `regexp_replace` on `information_schema.views.view_definition`,
--   then sent to MotherDuck via direct `query_rw` calls.
--
-- §3.D — Re-applicable rewrite patterns (idempotent — safe to re-run if the
--   bare-name references ever re-emerge from a future restore from archive):
--
--   /* Stale-ref-only fix (US_Thyroid_Gland_Wide_v2, US_Lymph_Nodes_Wide_v2): */
--   CREATE OR REPLACE VIEW views_readable.<view> AS
--     SELECT regexp_replace(view_definition,
--              '([^a-zA-Z0-9_])canonical_us_exam_master_v2([^a-zA-Z0-9_]|$)',
--              '\1canonical_us_exam_master_VIEW_v2\2', 'g')
--     FROM information_schema.views
--     WHERE table_schema='views_readable' AND table_name='<view>';
--   -- (note: the above is illustrative; CREATE OR REPLACE VIEW does NOT accept
--   --  dynamic SQL bodies. The actual apply path is: fetch rewritten DDL via
--   --  regexp_replace, paste back into a query_rw call as literal SQL.)
--
--   /* Combined stale-ref + dual-TIRADS-split fix (US_Nodules_Wide_v2 only): */
--   The full pattern chain is:
--     1. canonical_us_exam_master_v2 -> canonical_us_exam_master_VIEW_v2
--     2. each `max(CASE WHEN ((us_exam_rank=X) AND (nodule_index_within_exam=Y))
--        THEN (tirads_category_v2) ELSE NULL END) AS us_X_nodule_Y_tirads`
--        is replaced by:
--        `max(CASE WHEN ((us_exam_rank=X) AND (nodule_index_within_exam=Y))
--             THEN (acr2017_tirads_category) ELSE NULL END) AS us_X_nodule_Y_tirads_acr2017,
--         max(CASE WHEN ((us_exam_rank=X) AND (nodule_index_within_exam=Y))
--             THEN (updated_tirads_category) ELSE NULL END) AS us_X_nodule_Y_tirads_updated`
--
-- §3.E — Re-derivation:
--   To inspect the actual post-mig_245 view bodies live:
--     SELECT view_definition FROM information_schema.views
--     WHERE table_schema='views_readable' AND table_name=<view>;

-- =============================================================================
-- §4 — Verification (post-apply observed row counts on 2026-05-01)
-- =============================================================================
-- All 8 views verified queryable post-mig_245. Observed row counts:
--
--   manuscript_workspace.cohort_m011_tirads_fna_genetics_v1   — 3,282 rows ✓
--   manuscript_workspace.cohort_m025_tirads_performance_v1    — 3,375 rows ✓
--   manuscript_workspace.cohort_m045_multimodal_risk_v1       — 1,165 rows ✓
--   manuscript_workspace.cohort_m075_tirads_multi_nodule_v1   — 3,282 rows ✓
--   views_readable.Genetics_Variants                          —   936 rows ✓
--   views_readable.US_Thyroid_Gland_Wide_v2                   — 4,074 rows ✓
--   views_readable.US_Lymph_Nodes_Wide_v2                     — 4,077 rows ✓
--   views_readable.US_Nodules_Wide_v2                         — 4,358 rows ✓
--
-- Lakehouse health (post-apply):
--   gate1 = 218            (unchanged from pre-mig_245)
--   gates 2-5 = 0/0/0/0    (unchanged)
--   cohort_parity_ok = TRUE (10871 / 10871 / 10871)
--   verified_main_objects_missing_comment = 0
--
-- Stale-ref scan (post-apply): 0 references to ANY of the 8 patterns
-- (canonical_path_malignant_events_dedup, canonical_us_exam_master_v2,
--  canonical_us_patient_master_v2, longitudinal_lab_v1,
--  molecular_fusions_unnested_v2, molecular_variants_unnested_v2,
--  thyroglobulin_lab_v1, tirads_category_v2) in any non-archive view.

-- SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;

-- =============================================================================
-- End of mig_245
-- =============================================================================
