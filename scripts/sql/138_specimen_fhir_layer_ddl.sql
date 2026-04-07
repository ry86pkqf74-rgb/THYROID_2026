-- Specimen + FHIR pipeline split across two SQL files (canonical identity first):
--
--   1) scripts/sql/139_specimen_identity_layer_ddl.sql
--      main.specimen_master_v1, specimen_tumor_focus_v1, specimen_source_xref_v1,
--      qa.specimen_merge_review_queue_v1, qa.val_specimen_contract_v1
--
--   2) scripts/sql/138_specimen_fhir_tail_ddl.sql
--      main.fhir_*_v1
--
--   3) scripts/sql/140_specimen_genomics_binding_ddl.sql + scripts/140_md_specimen_genomics_binding.py
--      main.specimen_genomic_assay_v1, qa.specimen_genomic_link_review_v1, qa.val_specimen_genomic_binding_v1
--
-- Orchestration: scripts/139_md_specimen_identity_layer.py (identity only);
--                scripts/138_md_specimen_fhir_layer.py runs (1) → (2) → (3).

SELECT 'use 139_specimen_identity_layer_ddl.sql + 138_specimen_fhir_tail_ddl.sql' AS _deprecated_monolith_pointer;
