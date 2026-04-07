-- Optional: run after 128_multimodal_contract_mm_v1.py
-- Summarizes contract row counts and validation failure totals for release notes.

SELECT 'dim_patient_mm_v1' AS tbl, COUNT(*) AS n FROM mm_contract_dev.dim_patient_mm_v1
UNION ALL SELECT 'fact_surgery_mm_v1', COUNT(*) FROM mm_contract_dev.fact_surgery_mm_v1
UNION ALL SELECT 'fact_tumor_mm_v1', COUNT(*) FROM mm_contract_dev.fact_tumor_mm_v1
UNION ALL SELECT 'fact_imaging_mm_v1', COUNT(*) FROM mm_contract_dev.fact_imaging_mm_v1
UNION ALL SELECT 'fact_fna_mm_v1', COUNT(*) FROM mm_contract_dev.fact_fna_mm_v1
UNION ALL SELECT 'fact_genetics_mm_v1', COUNT(*) FROM mm_contract_dev.fact_genetics_mm_v1
UNION ALL SELECT 'link_surgery_path_mm_v1', COUNT(*) FROM mm_contract_dev.link_surgery_path_mm_v1
UNION ALL SELECT 'link_surgery_path_primary', COUNT(*) FROM mm_contract_dev.link_surgery_path_mm_v1 WHERE is_primary_link
ORDER BY 1;

SELECT 'val_nodes_invariant_mm_v1' AS val_tbl, COUNT(*) AS fail_rows FROM mm_contract_dev.val_nodes_invariant_mm_v1
UNION ALL SELECT 'val_side_lobe_mismatch_mm_v1', COUNT(*) FROM mm_contract_dev.val_side_lobe_mismatch_mm_v1
UNION ALL SELECT 'val_preop_temporal_order_mm_v1', COUNT(*) FROM mm_contract_dev.val_preop_temporal_order_mm_v1
UNION ALL SELECT 'val_ambiguous_multimodal_linkage_mm_v1', COUNT(*) FROM mm_contract_dev.val_ambiguous_multimodal_linkage_mm_v1
UNION ALL SELECT 'val_multitumor_expansion_mm_v1', COUNT(*) FROM mm_contract_dev.val_multitumor_expansion_mm_v1
ORDER BY 1;
