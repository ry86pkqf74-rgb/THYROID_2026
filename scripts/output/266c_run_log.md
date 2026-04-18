[23:59:54.672Z] ==============================================================================
[23:59:54.672Z] # Script 266c run
[23:59:54.672Z] started_at: 2026-04-17T23:59:54.672967+00:00
[23:59:54.673Z] mode      : APPLY
[23:59:54.673Z] phase     : 1
[23:59:55.681Z] connected to thyroid_canonical_publication_v1_0
[23:59:55.681Z] 
## Phase 0 — preflight + sequencing gates + view pre-gate
[23:59:55.748Z]   connected_to        : thyroid_canonical_publication_v1_0
[23:59:55.749Z]   cpm_n_patients      : 10871
[23:59:55.749Z]   ctc_n_rows          : 11106
[23:59:55.749Z]   tpath_n_rows        : 4290
[23:59:55.749Z]   psyn_n_rows         : 11688
[23:59:55.749Z]   tem_n_rows          : 11691
[23:59:56.822Z]   GATE 266a applied   : data_dictionary_v266a exists
[23:59:57.141Z]   GATE 266b CPM cols missing : none
[23:59:57.913Z]   GATE 266b CTC cols missing : none
[23:59:58.201Z]   GATE big-cleanup    : ajcc8_t_stage_corrected=False ajcc8_t_stage_v2=True DEPRECATED__present=True
[23:59:58.279Z]   view-dep PRE-GATE   : 0 view(s) reference CTC or TEM
[23:59:58.411Z]   pre-state path_synoptics cols : 271
[23:59:58.411Z]   pre-state tumor_pathology cols: 249
[23:59:58.412Z] 
## Phase 1 — snapshot CTC + TEM + tpath + psyn + CPM to archive
[00:00:00.187Z]   snapshot -> "Thyroid 2026 UPdated"."archive_pub_v1_0"."canonical_tumor_characteristics_v1_pre266c_20260417T235958Z"
[00:00:01.874Z]   snapshot -> "Thyroid 2026 UPdated"."archive_pub_v1_0"."tumor_episode_master_v2_pre266c_20260417T235958Z"
[00:00:04.429Z]   snapshot -> "Thyroid 2026 UPdated"."archive_pub_v1_0"."tumor_pathology_pre266c_20260417T235958Z"
[00:00:08.212Z]   snapshot -> "Thyroid 2026 UPdated"."archive_pub_v1_0"."path_synoptics_pre266c_20260417T235958Z"
[00:00:12.655Z]   snapshot -> "Thyroid 2026 UPdated"."archive_pub_v1_0"."canonical_patient_master_pre266c_20260417T235958Z"
[00:00:12.930Z] 
elapsed: 18.3s
[00:00:12.931Z] DONE
