-- mig_261 — path_synoptics CAP label normalization + surg_date → DATE (2026-05-01)
-- Apply via: scripts/mig_261_path_synoptics_label_norm.py --apply
-- Archive: "Thyroid 2026 UPdated".archive_pub_v1_0.path_synoptics_pre_mig261_20260501

-- Pre-snapshot (subset + aligner — full apply uses Python for stable row keys):
-- CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.path_synoptics_pre_mig261_20260501 AS ...

-- Categorical UPDATEs (tumor_1..5 LVI / ETE / histologic_type + focality):
-- UPDATE main.path_synoptics SET tumor_focality = LOWER(TRIM(REPLACE(tumor_focality, chr(10), ''))) WHERE tumor_focality IS NOT NULL;
-- UPDATE ... tumor_N_lymphatic_invasion CASE typo-map ... 
-- UPDATE ... tumor_N_extrathyroidal_extension CASE 'extesive' -> 'extensive' + LOWER/TRIM strip ';'
-- UPDATE ... tumor_N_histologic_type = LOWER(TRIM(...))

-- Retype:
-- ALTER TABLE main.path_synoptics ALTER COLUMN surg_date SET DATA TYPE DATE USING CAST(surg_date AS DATE);

-- Signoff:
-- INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES ('mig_261', ...);
