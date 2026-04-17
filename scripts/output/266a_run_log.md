[08:19:35.403Z] ==============================================================================
[08:19:35.403Z] # Script 266a run
[08:19:35.403Z] started_at: 2026-04-17T08:19:35.403637+00:00
[08:19:35.403Z] mode      : APPLY
[08:19:35.403Z] phase     : all
[08:19:36.386Z] connected to thyroid_canonical_publication_v1_0
[08:19:36.386Z] 
## Phase 0 — connect + decision-log seed
[08:19:36.988Z]   connected_to        : thyroid_canonical_publication_v1_0
[08:19:36.988Z]   cpm_n_patients      : 10871
[08:19:36.988Z]   cpm_n_columns       : 1499
[08:19:36.988Z] 
## Phase 1 — snapshot v240 dictionary + registry to archive
[08:19:40.188Z]   snapshot dict     -> "Thyroid 2026 UPdated"."archive_pub_v1_0"."data_dictionary_v240_pre266a_20260417T081936Z"
[08:19:40.188Z]   snapshot registry -> "Thyroid 2026 UPdated"."archive_pub_v1_0"."detail_table_registry_v1_pre266a_20260417T081936Z"
[08:19:40.189Z] 
## Phase 2 — build data_dictionary_v266a + apply updates
[08:19:40.223Z]   v240 rows: 1529
[08:19:40.223Z]   EXEC [ctas_v266a]: CREATE OR REPLACE TABLE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a AS SELECT * FROM thyroid_canonical_publication_v1_0.main.data_dictionary_v
[08:19:40.535Z]   EXEC [comment_v266a]: COMMENT ON TABLE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a IS 'Script 266a (2026-04-17). CTAS from data_dictionary_v240 with 266a governance
[08:19:40.776Z]   EXEC [update:n_tumors]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'deprecated',   replacement_column_name = 'n_tumors_path',   description = '
[08:19:40.891Z]   EXEC [update:n_tumors_v10]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'deprecated',   replacement_column_name = 'n_tumors_path',   description = '
[08:19:40.933Z]   EXEC [update:gm_path_stage_raw]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'deprecated',   replacement_column_name = NULL,   description = 'Deprecated 
[08:19:40.971Z]   EXEC [update:gm_path_m_stage_raw]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'deprecated',   replacement_column_name = 'path_m_stage_raw',   description 
[08:19:41.009Z]   EXEC [update:ajcc7_m_stage]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'authoritative',   replacement_column_name = NULL,   description = 'AJCC7 M-
[08:19:41.054Z]   EXEC [update:ajcc8_m_stage]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'authoritative',   replacement_column_name = NULL,   description = 'AJCC8 M-
[08:19:41.092Z]   EXEC [update:has_left_tumor]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'authoritative',   replacement_column_name = NULL,   description = 'Derived 
[08:19:41.130Z]   EXEC [update:has_right_tumor]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'authoritative',   replacement_column_name = NULL,   description = 'Derived 
[08:19:41.247Z]   EXEC [update:has_isthmus_tumor]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'authoritative',   replacement_column_name = NULL,   description = 'Derived 
[08:19:41.383Z]   EXEC [update:n_tumors_ete_present]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'authoritative',   replacement_column_name = NULL,   description = 'Per-pati
[08:19:41.420Z]   EXEC [update:n_tumors_lvi_present]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'authoritative',   replacement_column_name = NULL,   description = 'Per-pati
[08:19:41.457Z]   EXEC [update:n_tumors_margin_involved]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'authoritative',   replacement_column_name = NULL,   description = 'Per-pati
[08:19:41.494Z]   EXEC [update:n_tumors_margin_uninvolved]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'authoritative',   replacement_column_name = NULL,   description = 'Per-pati
[08:19:41.532Z]   EXEC [update:n_tumors_with_size]: UPDATE thyroid_canonical_publication_v1_0.main.data_dictionary_v266a SET   status = 'authoritative',   replacement_column_name = NULL,   description = 'Per-pati
[08:19:41.635Z]   v266a rows: 1529  (must == v240 rows 1529)
[08:19:41.635Z]   deprecated-target rows post-update: 4 (target>=4)
[08:19:41.635Z] 
## Phase 3 — COMMENT ON COLUMN on CPM
[08:19:41.635Z]   EXEC [comment:n_tumors]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.n_tumors IS 'Deprecated 2026-04-17 by Script 266a. Matches tumor_episode_mast
[08:19:41.986Z]   EXEC [comment:n_tumors_v10]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.n_tumors_v10 IS 'Deprecated 2026-04-17 by Script 266a. Matches synoptic_tumor
[08:19:42.245Z]   EXEC [comment:gm_path_stage_raw]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.gm_path_stage_raw IS 'Deprecated 2026-04-17 by Script 266a. 0% populated. See
[08:19:42.606Z]   EXEC [comment:gm_path_m_stage_raw]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.gm_path_m_stage_raw IS 'Deprecated 2026-04-17 by Script 266a. 36.84% populate
[08:19:42.883Z]   EXEC [comment:ajcc7_m_stage]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.ajcc7_m_stage IS 'AJCC7 M-stage at patient level. 100% populated by M0-defaul
[08:19:43.341Z]   EXEC [comment:ajcc8_m_stage]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.ajcc8_m_stage IS 'AJCC8 M-stage at patient level. 100% populated by M0-defaul
[08:19:43.653Z]   EXEC [comment:has_left_tumor]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.has_left_tumor IS 'Derived 36.65% population (3,984/10,871). Feeder registere
[08:19:43.939Z]   EXEC [comment:has_right_tumor]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.has_right_tumor IS 'Derived 36.65% population (3,984/10,871). Feeder register
[08:19:44.285Z]   EXEC [comment:has_isthmus_tumor]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.has_isthmus_tumor IS 'Derived 36.65% population (3,984/10,871). Feeder regist
[08:19:44.686Z]   EXEC [comment:n_tumors_ete_present]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.n_tumors_ete_present IS 'Per-patient count of tumors with extrathyroidal exte
[08:19:44.940Z]   EXEC [comment:n_tumors_lvi_present]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.n_tumors_lvi_present IS 'Per-patient count of tumors with lymphovascular inva
[08:19:45.278Z]   EXEC [comment:n_tumors_margin_involved]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.n_tumors_margin_involved IS 'Per-patient count of tumors with positive margin
[08:19:45.679Z]   EXEC [comment:n_tumors_margin_uninvolved]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.n_tumors_margin_uninvolved IS 'Per-patient count of tumors with negative marg
[08:19:45.957Z]   EXEC [comment:n_tumors_with_size]: COMMENT ON COLUMN thyroid_canonical_publication_v1_0.main.canonical_patient_master.n_tumors_with_size IS 'Per-patient count of tumors with a recorded size. Feed
[08:19:46.244Z] 
## Phase 4 — register has_*_tumor against tumor_pathology in registry
[08:19:46.272Z]   before raw : histology_final, path_tumor_size_cm, path staging columns
[08:19:46.272Z]   before norm: ihc_braf_confidence_v13;ihc_braf_note_type_v13;ihc_braf_result_v13
[08:19:46.272Z]   after  raw : has_isthmus_tumor;has_left_tumor;has_right_tumor;histology_final, path_tumor_size_cm, path staging columns
[08:19:46.272Z]   after  norm: has_isthmus_tumor;has_left_tumor;has_right_tumor;ihc_braf_confidence_v13;ihc_braf_note_type_v13;ihc_braf_result_v13
[08:19:46.272Z]   EXEC [registry_update_tumor_pathology]: UPDATE thyroid_canonical_publication_v1_0.manuscript_workspace.detail_table_registry_v1 SET   feeds_master_columns = 'has_isthmus_tumor;has_left_tumor;has_right
[08:19:46.348Z]   tokens verified present in registry after update.
[08:19:46.348Z] 
## Phase 5 — rebuild cpm_unmapped_triage_v266a
[08:19:46.642Z]   unmapped CPM cols (live registry view): 0
[08:19:46.642Z]   EXEC [ctas_triage_v266a]: CREATE OR REPLACE TABLE thyroid_canonical_publication_v1_0.manuscript_workspace.cpm_unmapped_triage_v266a AS
[08:19:47.069Z]   EXEC [comment_triage_v266a]: COMMENT ON TABLE thyroid_canonical_publication_v1_0.manuscript_workspace.cpm_unmapped_triage_v266a IS 'Script 266a: triage of CPM columns with no feeder in cano
[08:19:47.429Z]   bucket counts (post-rebuild):
[08:19:47.540Z]   staging-adjacent still in C: []
[08:19:47.540Z] 
## Phase 6 — acceptance gates + view smoke check
[08:19:47.698Z]   GATE v240_rows=1529 v266a_rows=1529
[08:19:47.726Z]   GATE deprecated_targets=4 (must >= 4)
[08:19:47.755Z]   GATE c_bucket=0 (must <= 165)
[08:19:47.786Z]   GATE staging_adjacent_in_C=[]
[08:19:47.831Z]   smoke-checking 65 views ...
[08:19:51.042Z]   smoke result: ok=65 error=0
[08:19:52.151Z] 
elapsed: 16.7s
[08:19:52.151Z] DONE
