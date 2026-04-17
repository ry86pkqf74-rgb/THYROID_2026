[11:36:01.925Z] ==============================================================================
[11:36:01.925Z] # Script 266b run
[11:36:01.925Z] started_at: 2026-04-17T11:36:01.925651+00:00
[11:36:01.925Z] mode      : DRY-RUN
[11:36:01.925Z] phase     : all
[11:36:03.015Z] connected to thyroid_canonical_publication_v1_0
[11:36:03.016Z] 
## Phase 0 — preflight + sequencing gates
[11:36:03.071Z]   connected_to        : thyroid_canonical_publication_v1_0
[11:36:03.071Z]   cpm_n_patients      : 10871
[11:36:03.071Z]   ctc_n_rows          : 11106
[11:36:03.600Z]   GATE 266a applied   : data_dictionary_v266a exists = True
[11:36:03.686Z]   GATE big-cleanup    : ajcc8_t_stage=True corrected=False v2=True DEPRECATED__present=True
[11:36:03.765Z]   CTC has un-versioned t_stage : True
[11:36:03.765Z]   CTC has un-versioned n_stage : True
[11:36:03.765Z]   CTC has un-versioned m_stage : True
[11:36:03.765Z]   CTC has un-versioned overall_stage : True
[11:36:03.765Z]   CTC required inputs missing : none
[11:36:03.765Z] 
## Phase 1 — snapshot CTC + CPM to archive_pub_v1_0
[11:36:03.765Z]   PLAN: CTAS thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 -> "Thyroid 2026 UPdated"."archive_pub_v1_0".'canonical_tumor_characteristics_v1_pre266b_20260417T113603Z'
[11:36:03.765Z]   PLAN: CTAS thyroid_canonical_publication_v1_0.main.canonical_patient_master -> "Thyroid 2026 UPdated"."archive_pub_v1_0".'canonical_patient_master_pre266b_20260417T113603Z'
[11:36:03.765Z] 
## Phase 2 — ALTER TABLE ADD COLUMN on CTC (14 columns)
[11:36:03.839Z]   PLAN [add:t_stage_ajcc7]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN t_stage_ajcc7 VARCHAR
[11:36:03.839Z]   PLAN [add:n_stage_ajcc7]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN n_stage_ajcc7 VARCHAR
[11:36:03.839Z]   PLAN [add:m_stage_ajcc7]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN m_stage_ajcc7 VARCHAR
[11:36:03.839Z]   PLAN [add:overall_stage_ajcc7]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN overall_stage_ajcc7 VARCHAR
[11:36:03.839Z]   PLAN [add:stage_group_ajcc7]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN stage_group_ajcc7 VARCHAR
[11:36:03.839Z]   PLAN [add:t_stage_ajcc8]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN t_stage_ajcc8 VARCHAR
[11:36:03.839Z]   PLAN [add:n_stage_ajcc8]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN n_stage_ajcc8 VARCHAR
[11:36:03.839Z]   PLAN [add:m_stage_ajcc8]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN m_stage_ajcc8 VARCHAR
[11:36:03.839Z]   PLAN [add:overall_stage_ajcc8]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN overall_stage_ajcc8 VARCHAR
[11:36:03.839Z]   PLAN [add:stage_group_ajcc8]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN stage_group_ajcc8 VARCHAR
[11:36:03.839Z]   PLAN [add:ajcc7_stage_calculable_flag]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN ajcc7_stage_calculable_flag BOOLEAN
[11:36:03.839Z]   PLAN [add:ajcc8_stage_calculable_flag]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN ajcc8_stage_calculable_flag BOOLEAN
[11:36:03.839Z]   PLAN [add:staging_source_note]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN staging_source_note VARCHAR
[11:36:03.839Z]   PLAN [add:stage_migration_7_to_8]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1 ADD COLUMN stage_migration_7_to_8 VARCHAR
[11:36:03.839Z] 
## Phase 3 — derive per-tumor AJCC7/AJCC8 (pandas + UPDATE CTC)
[11:36:03.839Z]   pulling per-tumor frame ...
[11:36:04.147Z]   rows: 18610  patients: 8422
[11:36:04.553Z]   ete classification audit -> 266b_ete_classification_audit.csv (51 unique (stl_text, source) pairs)
[11:36:04.556Z]   ete source counts: {'uncalculable:no_stl_no_adjudication': 9599, 'stl_per_tumor:unclassified_present': 4652, 'cpm_patient_level:broadcast_to_dominant:microscopic': 3371, 'stl_per_tumor:microscopic': 675, 'cpm_patient_level:broadcast_to_dominant:gross': 189, 'ete_adjudication_v1:unable_to_determine': 58, 'ete_adjudication_v1:absent': 42, 'cpm_patient_level:broadcast_to_dominant:absent': 14, 'ete_adjudication_v1:gross': 8, 'ete_adjudication_v1:microscopic': 2}
[11:36:06.687Z]   staging frame ready: 18610 rows
[11:36:06.687Z]   distribution snapshot (AJCC8 T per-tumor):
[11:36:06.688Z]     {'T1a': 7452, None: 5425, 'T1b': 2633, 'T2': 1941, 'T3a': 962, 'T3b': 197}
[11:36:06.688Z]   distribution snapshot (AJCC7 T per-tumor):
[11:36:06.688Z]     {'T1a': 7452, None: 5425, 'T1b': 2633, 'T2': 1941, 'T3': 1159}
[11:36:06.689Z]   calculable flags:
[11:36:06.689Z]     ajcc8_stage_calculable_flag=TRUE : 12092
[11:36:06.689Z]     ajcc7_stage_calculable_flag=TRUE : 13089
[11:36:06.689Z]   PLAN: would register stg as DuckDB temp view + 14-column UPDATE.
[11:36:06.705Z] 
## Phase 4 — CREATE TABLE tumor_stage_heterogeneity_v1
[11:36:06.705Z]   PLAN [ctas_heterogeneity]: CREATE OR REPLACE TABLE thyroid_canonical_publication_v1_0.main.tumor_stage_heterogeneity_v1 AS
[11:36:06.705Z]   PLAN [comment_het]: COMMENT ON TABLE thyroid_canonical_publication_v1_0.main.tumor_stage_heterogeneity_v1 IS 'Script 266b (2026-04-17). Patient-grain rollup of per-tumor AJCC stage
[11:36:06.705Z] 
## Phase 5 — ALTER CPM ADD COLUMN + UPDATE from heterogeneity
[11:36:06.788Z]   PLAN [add:dominant_tumor_ajcc7_t_stage]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN dominant_tumor_ajcc7_t_stage VARCHAR
[11:36:06.788Z]   PLAN [add:dominant_tumor_ajcc7_n_stage]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN dominant_tumor_ajcc7_n_stage VARCHAR
[11:36:06.788Z]   PLAN [add:dominant_tumor_ajcc7_m_stage]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN dominant_tumor_ajcc7_m_stage VARCHAR
[11:36:06.789Z]   PLAN [add:dominant_tumor_ajcc7_stage_group]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN dominant_tumor_ajcc7_stage_group VARCHAR
[11:36:06.789Z]   PLAN [add:dominant_tumor_ajcc8_t_stage]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN dominant_tumor_ajcc8_t_stage VARCHAR
[11:36:06.789Z]   PLAN [add:dominant_tumor_ajcc8_n_stage]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN dominant_tumor_ajcc8_n_stage VARCHAR
[11:36:06.789Z]   PLAN [add:dominant_tumor_ajcc8_m_stage]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN dominant_tumor_ajcc8_m_stage VARCHAR
[11:36:06.789Z]   PLAN [add:dominant_tumor_ajcc8_stage_group]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN dominant_tumor_ajcc8_stage_group VARCHAR
[11:36:06.789Z]   PLAN [add:tumor_stage_heterogeneous_t_ajcc8_flag]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN tumor_stage_heterogeneous_t_ajcc8_flag BOOLEAN
[11:36:06.789Z]   PLAN [add:tumor_stage_heterogeneous_overall_ajcc8_flag]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN tumor_stage_heterogeneous_overall_ajcc8_flag BOOLEAN
[11:36:06.789Z]   PLAN [add:n_tumors_ajcc7_staged]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN n_tumors_ajcc7_staged INTEGER
[11:36:06.789Z]   PLAN [add:n_tumors_ajcc8_staged]: ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master ADD COLUMN n_tumors_ajcc8_staged INTEGER
[11:36:06.789Z]   PLAN [update_cpm_dominant]: UPDATE thyroid_canonical_publication_v1_0.main.canonical_patient_master AS cpm
[11:36:06.789Z]   PLAN [comment:dominant_tumor_ajcc7_t_stage]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.dominant_tumor_ajcc7_t_stage IS 'Script 266b (2026-04-17). Sourced from main.
[11:36:06.789Z]   PLAN [comment:dominant_tumor_ajcc7_n_stage]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.dominant_tumor_ajcc7_n_stage IS 'Script 266b (2026-04-17). Sourced from main.
[11:36:06.789Z]   PLAN [comment:dominant_tumor_ajcc7_m_stage]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.dominant_tumor_ajcc7_m_stage IS 'Script 266b (2026-04-17). Sourced from main.
[11:36:06.789Z]   PLAN [comment:dominant_tumor_ajcc7_stage_group]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.dominant_tumor_ajcc7_stage_group IS 'Script 266b (2026-04-17). Sourced from m
[11:36:06.789Z]   PLAN [comment:dominant_tumor_ajcc8_t_stage]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.dominant_tumor_ajcc8_t_stage IS 'Script 266b (2026-04-17). Sourced from main.
[11:36:06.789Z]   PLAN [comment:dominant_tumor_ajcc8_n_stage]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.dominant_tumor_ajcc8_n_stage IS 'Script 266b (2026-04-17). Sourced from main.
[11:36:06.789Z]   PLAN [comment:dominant_tumor_ajcc8_m_stage]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.dominant_tumor_ajcc8_m_stage IS 'Script 266b (2026-04-17). Sourced from main.
[11:36:06.789Z]   PLAN [comment:dominant_tumor_ajcc8_stage_group]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.dominant_tumor_ajcc8_stage_group IS 'Script 266b (2026-04-17). Sourced from m
[11:36:06.789Z]   PLAN [comment:tumor_stage_heterogeneous_t_ajcc8_flag]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.tumor_stage_heterogeneous_t_ajcc8_flag IS 'Script 266b (2026-04-17). Sourced 
[11:36:06.789Z]   PLAN [comment:tumor_stage_heterogeneous_overall_ajcc8_flag]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.tumor_stage_heterogeneous_overall_ajcc8_flag IS 'Script 266b (2026-04-17). So
[11:36:06.789Z]   PLAN [comment:n_tumors_ajcc7_staged]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.n_tumors_ajcc7_staged IS 'Script 266b (2026-04-17). Sourced from main.tumor_s
[11:36:06.789Z]   PLAN [comment:n_tumors_ajcc8_staged]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.n_tumors_ajcc8_staged IS 'Script 266b (2026-04-17). Sourced from main.tumor_s
[11:36:06.789Z] 
## Phase 6 — validation suite
[11:36:06.789Z]   PLAN: would run validations against post-apply state.
[11:36:06.789Z] 
## Phase 7 — acceptance gates + view smoke check
[11:36:06.789Z]   PLAN: skip in dry-run.
[11:36:06.790Z] 
elapsed: 4.9s
[11:36:06.790Z] DONE
