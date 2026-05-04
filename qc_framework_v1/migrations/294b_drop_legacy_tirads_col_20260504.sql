-- mig_294b — Drop legacy canonical_patient_master.nlp_tirads_max_category (mig_294 retry)
-- SSOT for clean TIRADS on CPM: tirads_resolved (mig_288). Legacy column was dirty free-text / 345 distinct.
-- Apply against: thyroid_canonical_publication_v1_0 (USE + connect_locked)

-- §1 Pre-snapshot (archive DB)
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_nlp_tirads_max_category_pre_mig294b_20260504 AS
SELECT research_id, nlp_tirads_max_category
FROM main.canonical_patient_master;

-- §2 Drop legacy column
ALTER TABLE main.canonical_patient_master DROP COLUMN nlp_tirads_max_category;

-- §3 Registry signoff (run only after verifying column absent)
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_294b', CURRENT_TIMESTAMP, 'cursor_composer_mig294b_retry_of_294',
 'mig_294b: Dropped canonical_patient_master.nlp_tirads_max_category (mig_294 retry). Pre-snapshot to archive_pub_v1_0.cpm_nlp_tirads_max_category_pre_mig294b_20260504. Consumer audit: 0 views (information_schema.views + duckdb_views). Closes CF-mig282-LEGACY-NLP-COL.');
