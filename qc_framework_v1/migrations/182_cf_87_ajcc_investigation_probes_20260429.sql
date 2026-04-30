-- mig_182 — CF-87-AJCC investigation probes (READ-ONLY)
-- Target DB: thyroid_canonical_publication_v1_0
-- No DDL/DML in this artifact.

SELECT schema_name, table_name, column_name, batch_id, SUBSTR(notes, 1, 400) AS notes_excerpt
FROM main.canonical_column_verification_registry_v1
WHERE notes ILIKE '%CF-87-AJCC%' OR notes ILIKE '%CF-87%AJCC%' OR notes ILIKE '%AJCC%drift%'
ORDER BY table_name, column_name;

SELECT t_stage_ajcc8, n_stage_ajcc8, m_stage_ajcc8, stage_group_ajcc8, COUNT(*) AS n_rows
FROM main.canonical_path_malignant_events_v1
GROUP BY 1,2,3,4
ORDER BY n_rows DESC;

SELECT ajcc8_t_stage, dominant_tumor_ajcc8_t_stage, ajcc8_stage_group, dominant_tumor_ajcc8_stage_group, COUNT(*) AS n_patients
FROM main.canonical_patient_master
GROUP BY 1,2,3,4
ORDER BY n_patients DESC;
