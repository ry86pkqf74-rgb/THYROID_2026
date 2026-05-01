-- mig_237 — table-comment refresh: 28 missing + 2 stale = 30 COMMENT ON TABLE stmts
-- run_id / batch: mig_237_canonical_table_comments_refresh_20260501
-- Source: ChatGPT cleanup audit 2026-05-01 (verified live by Cowork);
--         see qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md §A claim 4.
-- Target DB: thyroid_canonical_publication_v1_0
-- COWORK-DIRECT (Cowork orchestrator); pure metadata, no row changes, no archive snapshot.
--
-- Rationale:
--   Of the 62 verified canonical_* main objects, 28 had NULL table comment and 2 had
--   stale comments (FNA "8,119 rows" vs live 8,050; frozen rollup "VARCHAR pending
--   CF-119" vs date columns now DATE). This migration applies the [domain=X; grain=Y]
--   convention from the existing comments (e.g. canonical_frozen_section_patient_rollup_v1).
--   Row counts are intentionally OMITTED from comments to avoid the same staleness in 60d.
--
-- Pre-snapshot: N/A (metadata-only).
-- Post-apply: every verified canonical_* main object has a non-NULL comment.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §A 28 missing comments
-- =============================================================================
COMMENT ON TABLE main.canonical_cervical_ln_clinical_events_v1 IS
'[domain=cervical_ln_clinical; grain=event] — clinical (non-imaging) cervical lymph node assessments extracted from clinic notes; one row per (research_id, encounter); upstream of canonical_cervical_ln_clinical_patient_rollup_v1.';

COMMENT ON TABLE main.canonical_cervical_ln_clinical_patient_rollup_v1 IS
'[domain=cervical_ln_clinical; grain=per_patient] — patient-level rollup of canonical_cervical_ln_clinical_events_v1 with first/last assessment dates and aggregate flags.';

COMMENT ON TABLE main.canonical_column_verification_registry_v1 IS
'[domain=governance; grain=per_column] — column-level verification registry for the publication lakehouse; one row per (schema_name, table_name, column_name); referenced by gates 4 and 5 of the v2 audit (cowork_verification_suite_20260430.md).';

COMMENT ON TABLE main.canonical_esophageal_invasion_events_v1 IS
'[domain=esophageal_invasion; grain=event] — pathology-reported esophageal invasion events; one row per surgery x finding inherited from canonical_path_malignant_events_v1.';

COMMENT ON TABLE main.canonical_esophageal_invasion_patient_rollup_v1 IS
'[domain=esophageal_invasion; grain=per_patient] — patient-level rollup of canonical_esophageal_invasion_events_v1.';

COMMENT ON TABLE main.canonical_ete_inline_adjudication_v1 IS
'[domain=ete; grain=adjudication_decision] — inline adjudication queue for ETE-related disagreements (mig_64 closeout); one row per disagreement with adjudicator decision and rationale.';

COMMENT ON TABLE main.canonical_ete_subgrade_events_v1 IS
'[domain=ete_subgrade; grain=event] — fine-grained ETE subgrade events (microETE T3b, gross T4a, T4b invasion); one row per surgery; upstream of canonical_ete_subgrade_patient_rollup_v1.';

COMMENT ON TABLE main.canonical_ete_subgrade_patient_rollup_v1 IS
'[domain=ete_subgrade; grain=per_patient] — patient-level rollup of canonical_ete_subgrade_events_v1; surfaces final any_pT3b / any_pT4a / any_pT4b flags used by manuscript Tables 2 and 4.';

COMMENT ON TABLE main.canonical_frozen_section_events_v1 IS
'[domain=frozen_section; grain=event] — frozen-section pathology events extracted from operative reports; source for canonical_frozen_section_patient_rollup_v1 (rebuilt by mig_119).';

COMMENT ON TABLE main.canonical_operative_events_v1 IS
'[domain=operative; grain=event] — surgical encounter events; one row per (research_id, surgery_id) with date, procedure, complications, and surgeon metadata; cohort 10,871 patients map across one or more rows.';

COMMENT ON TABLE main.canonical_operative_patient_rollup_v1 IS
'[domain=operative; grain=per_patient] — patient-level rollup of canonical_operative_events_v1; first/last surgery dates, total counts, primary procedure flag.';

COMMENT ON TABLE main.canonical_path_benign_patient_rollup_v1 IS
'[domain=path_benign; grain=per_patient] — patient-level rollup of benign pathology findings (one row per research_id); complement to canonical_path_malignant_patient_rollup_v1.';

COMMENT ON TABLE main.canonical_path_gland_patient_rollup_v1 IS
'[domain=path_gland; grain=per_patient] — patient-level rollup of gland-level pathology characterization (gland weight, multifocality, lymphocytic infiltrate); excludes per-tumor detail.';

COMMENT ON TABLE main.canonical_path_indeterminate_events_v1 IS
'[domain=path_indeterminate; grain=event] — landing table for pathology with indeterminate malignancy classification (mig_207); used for manuscript caveat tracking and adjudication review.';

COMMENT ON TABLE main.canonical_path_malignant_events_v1 IS
'[domain=path_malignant; grain=per_tumor_per_surgery] — pathology malignant tumor events; SOURCE-DISTINCT DUPLICATES RETAINED for provenance — analytic SQL MUST use canonical_path_malignant_events_dedup_VIEW_v1 (mig_212) for per-tumor counts. Includes is_borderline_or_benign_with_staging (mig_229) — 27 quarantined FTUMP/adenoma-with-N1*/M1 rows.';

COMMENT ON TABLE main.canonical_path_malignant_patient_rollup_v1 IS
'[domain=path_malignant; grain=per_patient] — patient-level rollup of canonical_path_malignant_events_v1 (uses dedup view upstream).';

COMMENT ON TABLE main.canonical_pathology_clinical_events_v1 IS
'[domain=pathology_clinical; grain=event] — clinical pathology events (non-surgical biopsy / FNA reads); one row per encounter.';

COMMENT ON TABLE main.canonical_pathology_clinical_patient_rollup_v1 IS
'[domain=pathology_clinical; grain=per_patient] — patient-level rollup of canonical_pathology_clinical_events_v1.';

COMMENT ON TABLE main.canonical_patient_master IS
'[domain=patient_master; grain=per_patient] — wide canonical patient master (CPM); cohort 10,871 distinct research_id; 1,630 columns governed by canonical_column_verification_registry_v1 (1,607 verified / 23 na / 0 failed post mig_235). Read path for any patient-level analytic that needs cross-domain features. NEVER write to this table outside an explicit migration.';

COMMENT ON TABLE main.canonical_pmh_patient_rollup_v1 IS
'[domain=pmh; grain=per_patient] — past medical history rollup; one row per research_id with comorbidity / medication / family-history flags; sourced from clinical NLP pipeline.';

COMMENT ON TABLE main.canonical_recurrence_v1 IS
'[domain=recurrence; grain=per_patient_raw] — RAW recurrence base table; manuscript analyses MUST use canonical_recurrence_resolved_v1 (mig_62) which applies the dual-track schema (path_proven vs imaging_only_unconfirmed) and quarantines 132 implausible-date rows.';

COMMENT ON TABLE main.canonical_t4b_invasion_events_v1 IS
'[domain=t4b_invasion; grain=event] — explicit T4b (great vessel / prevertebral / mediastinal) invasion events; finer than ete_subgrade.';

COMMENT ON TABLE main.canonical_t4b_invasion_patient_rollup_v1 IS
'[domain=t4b_invasion; grain=per_patient] — patient-level rollup of canonical_t4b_invasion_events_v1.';

COMMENT ON TABLE main.canonical_table_signoff_registry_v1 IS
'[domain=governance; grain=per_table] — table-level signoff registry; one row per published canonical / safe-view object; gate1 of the v2 audit reads from this table (table_status=verified).';

COMMENT ON TABLE main.canonical_us_lymph_node_events_v2 IS
'[domain=us_lymph_node; grain=event] — US lymph node events (per-node, per-exam) v2 (post mig_171b shell); source for canonical_us_lymph_node_patient_rollup_v2; upstream of manuscript_workspace.vw_ln_*_safe_VIEW_v1 (Lane LN mig_224-229).';

COMMENT ON TABLE main.canonical_us_lymph_node_patient_rollup_v2 IS
'[domain=us_lymph_node; grain=per_patient] — patient-level rollup of canonical_us_lymph_node_events_v2; 10,871 patients (cohort parity).';

COMMENT ON TABLE main.canonical_us_thyroid_gland_events_v2 IS
'[domain=us_thyroid_gland; grain=event] — US thyroid gland events (per-exam) v2 (post mig_194 option B); source for canonical_us_thyroid_gland_patient_rollup_v2.';

COMMENT ON TABLE main.canonical_us_thyroid_gland_patient_rollup_v2 IS
'[domain=us_thyroid_gland; grain=per_patient] — patient-level rollup of canonical_us_thyroid_gland_events_v2; 10,871 patients (cohort parity).';

-- =============================================================================
-- §B 2 stale comments (rewrite)
-- =============================================================================
COMMENT ON TABLE main.canonical_fna_events_v1 IS
'[domain=fna; grain=event] — FNA event table. Clean views: _date_clean (mig_42), _dts_clean (mig_43), _dedup (mig_44). Dedup view surfaces fna_row_rank + fna04_duplicate_flag over (rid, date, specimen_location, laterality, bethesda_final_num); tie-break = bethesda_final_num DESC, fna_event_id ASC. 16 dup-rows queued under FNA04. Refreshed mig_237 2026-05-01 — removed stale row count (live count drifted from 8,119 to 8,050 over the v14 round).';

COMMENT ON TABLE main.canonical_frozen_section_patient_rollup_v1 IS
'[domain=frozen_section; grain=per_patient_wide] — source: Script 360 Phase 8 logic, rebuilt by mig_119 from verified main.canonical_frozen_section_events_v1; 12-slot wide rollup. CF-119-FROZEN-ROLLUP-DATE-RETYPE: CLOSED — date columns retyped from VARCHAR to DATE; refreshed mig_237 2026-05-01.';

-- =============================================================================
-- §C Acceptance assertion (run after apply)
-- =============================================================================
-- ASSERT: zero verified canonical_* main objects with NULL comment
WITH verified_objects AS (
  SELECT DISTINCT table_name FROM main.canonical_column_verification_registry_v1
  WHERE table_name LIKE 'canonical_%'
)
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS'
            ELSE 'FAIL: ' || COUNT(*)::VARCHAR || ' canonical_* main objects still missing comment' END AS assert_zero_missing_comments
FROM duckdb_tables() t
JOIN verified_objects v ON v.table_name = t.table_name
WHERE t.schema_name = 'main' AND t.comment IS NULL;
