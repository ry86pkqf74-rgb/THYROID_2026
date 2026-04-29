-- mig_179b — Cowork-authored notes-only follow-up to mig_179
-- Closes 4 CFs opened by the mig_177 read-only review and stamps mig_179 build provenance
-- on canonical_invasion_events_v1 + canonical_invasion_patient_rollup_v1 verified col rows.
-- CFs closed:
--   CF-mig177-EVENTS-LYMPH_VASCULAR_COMBINED-MISS — combined CAP now duplicates into vasc + lymph
--   CF-mig177-EVENTS-LYMPHATIC_PRESENT_SEPARATE_MISS — newer-CAP separate Lymphatic Invasion: Present now caught
--   CF-mig177-EVENTS-VOCAB-FOACL-EXTRENSIVE-INDETERMINENT-CA-X — vocab additions in path_syn_lvi_classified CTE
--   CF-mig177-ROLLUP-VASC-ALIAS-LVI — rollup rebuilt from refreshed events; lymphatic axis no longer aliased to vasc
--
-- Posture: Cowork applies. Registry-only writes; no data writes. Pre-snapshot per §A.
-- Target DB: thyroid_canonical_publication_v1_0

USE thyroid_canonical_publication_v1_0;

-- §A pre-snapshot of affected registry rows
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig179b_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig179b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name='main'
  AND table_name IN ('canonical_invasion_events_v1','canonical_invasion_patient_rollup_v1');

-- §B append closure note on canonical_invasion_events_v1 verified col rows
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_179: canonical_invasion_events_v1 LVI re-extract applied 2026-04-29 (commit pending). ' ||
            'Supplemental rows: structured_mig179 source_kind, build_script=179, extraction_run_id=mig_179_events_rebuild_lvi_extraction_20260429. ' ||
            'lymphatic_microscopic PRESENT 1233→5969 rows (780→989 patients); vascular_microscopic PRESENT 2883→4978 rows (1109→1178 patients) due to combined-CAP duplication. ' ||
            'capsular + perineural axes unchanged. ' ||
            'Closes CF-mig177-EVENTS-LYMPH_VASCULAR_COMBINED-MISS (combined CAP now duplicates), CF-mig177-EVENTS-LYMPHATIC_PRESENT_SEPARATE_MISS (newer-CAP separate Lymphatic now caught), CF-mig177-EVENTS-VOCAB-FOACL-EXTRENSIVE-INDETERMINENT-CA-X (vocab additions in path_syn_lvi_classified CTE).'
WHERE schema_name='main'
  AND table_name='canonical_invasion_events_v1'
  AND verification_status='verified';

-- §C append closure note on canonical_invasion_patient_rollup_v1 verified col rows
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_179: canonical_invasion_patient_rollup_v1 rebuilt from refreshed events 2026-04-29 (build_script=179). ' ||
            'any_lymphatic_microscopic_anywhere TRUE 780→989 patients; any_vascular_microscopic_anywhere TRUE 1109→1178 patients. ' ||
            'Other axes (capsular, perineural, ETE family, rln, soft_tissue, airway, tracheal, esophageal) unchanged. ' ||
            'Closes CF-mig177-ROLLUP-VASC-ALIAS-LVI (rollup re-derived from corrected events; 0 mismatches between rollup and direct re-derive).'
WHERE schema_name='main'
  AND table_name='canonical_invasion_patient_rollup_v1'
  AND verification_status='verified';

-- §D append note on canonical_table_signoff_registry_v1 for both tables (mig_179 build provenance)
UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_179_2026-04-29: events table additively rebuilt to fix LVI extractor combined-CAP miss + newer-CAP separate Lymphatic miss; rollup re-derived from refreshed events. Verified status preserved (no schema change; per-column verification logic still holds). 4 CF closures (LYMPH_VASCULAR_COMBINED-MISS, LYMPHATIC_PRESENT_SEPARATE_MISS, VOCAB-FOACL/EXTRENSIVE/INDETERMINENT/CA-X, ROLLUP-VASC-ALIAS-LVI) appended to col-row notes. Pre-snapshots in archive_pub_v1_0.canonical_invasion_events_v1_pre_mig177events_20260429 + canonical_invasion_patient_rollup_v1_pre_mig177events_20260429.'
WHERE table_name IN ('canonical_invasion_events_v1','canonical_invasion_patient_rollup_v1');

-- §E post-state verification (read-only)
SELECT
  table_name,
  COUNT(*) FILTER (WHERE notes ILIKE '%mig_179%') AS rows_with_mig179_note,
  COUNT(*) FILTER (WHERE verification_status='verified') AS n_verified
FROM main.canonical_column_verification_registry_v1
WHERE schema_name='main' AND table_name IN ('canonical_invasion_events_v1','canonical_invasion_patient_rollup_v1')
GROUP BY table_name
ORDER BY table_name;
