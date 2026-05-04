-- mig_296 — Protocol v2 pre-snapshot (baseline 2026-05-04)
-- Targets: "Thyroid 2026 UPdated".archive_pub_v1_0
-- Canonical DB session: thyroid_canonical_publication_v1_0 (USE main.* / manuscript_workspace.*)

-- Recurrence canonicals (mig_269 family)
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_events_v1_baseline_20260504 AS
SELECT * FROM main.canonical_recurrence_events_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_patient_rollup_v1_baseline_20260504 AS
SELECT * FROM main.canonical_recurrence_patient_rollup_v1;

-- mig_285/286 augmented cohort views — materialize row-level snapshot
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.cohort_m032_descriptive_25yr_v1_baseline_20260504 AS
SELECT * FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.cohort_m037_ln_metastasis_v1_baseline_20260504 AS
SELECT * FROM manuscript_workspace.cohort_m037_ln_metastasis_v1;

-- mig_288 CPM tirads_resolved (+ related NLP column)
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_tirads_resolved_baseline_20260504 AS
SELECT research_id, tirads_resolved, nlp_tirads_max_category
FROM main.canonical_patient_master;

-- mig_287 smoking enum snapshot
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_smoking_clean_baseline_20260504 AS
SELECT research_id, pmhx_nlp_smoking_status, nsqip_smoker
FROM main.canonical_patient_master;

-- Verify baseline row counts match live sources (events/rollup/views/CPM) before INSERT.
-- Recommended check: UNION ALL COUNT(*) archive vs source for each baseline table.

-- Registry signoff (run only after verification)
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_296', CURRENT_TIMESTAMP, 'cursor_composer_mig296',
 'mig_296: Pre-snapshot baseline of 6 recently-built objects to archive_pub_v1_0 (canonical_recurrence_events_v1, canonical_recurrence_patient_rollup_v1, cohort_m032/m037 v1 augmented views, CPM tirads_resolved+nlp_tirads_max_category, CPM smoking enum). Defensive Protocol v2 hygiene before next major round.');
