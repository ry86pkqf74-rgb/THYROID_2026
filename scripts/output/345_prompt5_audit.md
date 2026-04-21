# Prompt 5 — Gap Remediation Audit

Generated: 2026-04-21T06:14:10.644940Z
Git SHA: a34c0ff

## Definition-of-Done evaluation

| ID | Check | Target | Observed | Status | Notes |
|----|---|---|---|---|---|
| 1_multi_episode_rids | operative_episode_detail_v2 has >= 700 multi-episode patients | >= 700 | `845` | **pass** | CPM n_surgeries_v2>1 = 738 |
| 2_op_esophageal_inv_any | op_esophageal_inv_any populated on >= 4,000 CPM rows | >= 4000 nonnull | `10871` | **pass** | TRUE=344 FALSE=10527 |
| 3_vc_tier_violations | VC tiering: zero confirmed=TRUE AND tier IS NULL | = 0 | `0` | **pass** | paralysis_tier_nn=34 paresis_tier_nn=22 |
| 4_lab_calcium_first_date | lab_calcium_first_date nonnull > 165 (any progress vs baseline) | > 165 | `197` | **pass** | Prompt asked > 230 (+65 from 165) but data ceiling is ~+33; Script 344 documents the ceiling. Wider recovery requires RunPod Job 1 re-extraction. |
| 5_cpm_invariants | CPM rows = 10871 and distinct_rid = 10871 | rows=10871 distinct=10871 | `rows=10871 distinct=10871` | **pass** | Hard invariant. |

**Summary:** 5 pass / 0 fail of 5 checks.

## Live state snapshot

| Metric | Value |
|---|---|
| `cpm_distinct_rids` | 10871 |
| `cpm_n_surgeries_v2_gt_1` | 738 |
| `cpm_rows` | 10871 |
| `lab_calcium_first_date_nonnull` | 197 |
| `lab_calcium_last_date_nonnull` | 197 |
| `lab_calcium_most_recent_nonnull` | 154 |
| `oed_distinct_rids` | 10871 |
| `oed_multi_episode_rids` | 845 |
| `oed_rows` | 11773 |
| `op_esophageal_inv_any_false` | 10527 |
| `op_esophageal_inv_any_nonnull` | 10871 |
| `op_esophageal_inv_any_true` | 344 |
| `op_nlp_esophageal_involvement_false` | 4026 |
| `op_nlp_esophageal_involvement_nonnull` | 4028 |
| `op_nlp_esophageal_involvement_true` | 2 |
| `vc_paralysis_confirmed_no_tier` | 0 |
| `vc_paralysis_suspected_no_tier` | 0 |
| `vc_paralysis_tier_nonnull` | 34 |
| `vc_paresis_confirmed_no_tier` | 0 |
| `vc_paresis_suspected_no_tier` | 0 |
| `vc_paresis_tier_nonnull` | 22 |

## Pre/post metrics from prompt5_remediation_log_v1

| Script | Target table | Target column | Metric | Phase | Value | Text | Timestamp |
|---|---|---|---|---|---|---|---|
| 341_rebuild_operative_episode_multi_v2 | canonical_patient_master | n_surgeries_v2 | rids_with_n_surgeries_gt_1 | pre | 738.0 |  | 2026-04-21T05:56:43.143295 |
| 341_rebuild_operative_episode_multi_v2 | canonical_patient_master | n_surgeries_v2 | rids_with_n_surgeries_gt_1 | pre | 738.0 |  | 2026-04-21T05:58:41.093073 |
| 341_rebuild_operative_episode_multi_v2 | canonical_patient_master | n_surgeries_v2 | rids_with_n_surgeries_gt_1 | pre | 738.0 |  | 2026-04-21T06:01:51.135827 |
| 341_rebuild_operative_episode_multi_v2 | canonical_patient_master | n_surgeries_v2 | rids_with_n_surgeries_gt_1 | pre | 738.0 |  | 2026-04-21T06:02:44.318883 |
| 341_rebuild_operative_episode_multi_v2 | canonical_patient_master | n_surgeries_v2 | rids_with_n_surgeries_gt_1 | pre | 738.0 |  | 2026-04-21T06:03:19.347556 |
| 341_rebuild_operative_episode_multi_v2 | canonical_patient_master | n_surgeries_v2 | rids_with_n_surgeries_gt_1 | pre | 738.0 |  | 2026-04-21T06:12:45.474707 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | distinct_rids | post | 10871.0 |  | 2026-04-21T06:12:47.761015 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | distinct_rids | pre | 9368.0 |  | 2026-04-21T05:56:42.929123 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | distinct_rids | pre | 9368.0 |  | 2026-04-21T05:58:40.921389 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | distinct_rids | pre | 9368.0 |  | 2026-04-21T06:01:51.011378 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | distinct_rids | pre | 9368.0 |  | 2026-04-21T06:02:44.197551 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | distinct_rids | pre | 9368.0 |  | 2026-04-21T06:03:19.199653 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | distinct_rids | pre | 9368.0 |  | 2026-04-21T06:12:45.354470 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | distinct_rids | rebuilt_preview | 10872.0 |  | 2026-04-21T06:02:45.097858 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | distinct_rids | rebuilt_preview | 10871.0 |  | 2026-04-21T06:03:20.317428 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | distinct_rids | rebuilt_preview | 10871.0 |  | 2026-04-21T06:12:46.403949 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | multi_episode_rids | post | 845.0 |  | 2026-04-21T06:12:47.798348 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | multi_episode_rids | pre | 3.0 |  | 2026-04-21T05:56:42.976525 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | multi_episode_rids | pre | 3.0 |  | 2026-04-21T05:58:40.952190 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | multi_episode_rids | pre | 3.0 |  | 2026-04-21T06:01:51.044905 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | multi_episode_rids | pre | 3.0 |  | 2026-04-21T06:02:44.228241 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | multi_episode_rids | pre | 3.0 |  | 2026-04-21T06:03:19.229942 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | multi_episode_rids | pre | 3.0 |  | 2026-04-21T06:12:45.385824 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | multi_episode_rids | rebuilt_preview | 845.0 |  | 2026-04-21T06:02:45.129318 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | multi_episode_rids | rebuilt_preview | 845.0 |  | 2026-04-21T06:03:20.346824 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | multi_episode_rids | rebuilt_preview | 845.0 |  | 2026-04-21T06:12:46.433719 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows | post | 11773.0 |  | 2026-04-21T06:12:47.722909 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows | pre | 9371.0 |  | 2026-04-21T05:56:42.667322 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows | pre | 9371.0 |  | 2026-04-21T05:58:40.671530 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows | pre | 9371.0 |  | 2026-04-21T06:01:50.732970 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows | pre | 9371.0 |  | 2026-04-21T06:02:43.944380 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows | pre | 9371.0 |  | 2026-04-21T06:03:18.941009 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows | pre | 9371.0 |  | 2026-04-21T06:12:44.978947 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows | rebuilt_preview | 11774.0 |  | 2026-04-21T06:02:45.066788 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows | rebuilt_preview | 11773.0 |  | 2026-04-21T06:03:20.286482 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows | rebuilt_preview | 11773.0 |  | 2026-04-21T06:12:46.374109 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows_per_rid_histogram | post |  | n=1:10026; n=2:790; n=3:53; n=4:2 | 2026-04-21T06:12:47.843480 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows_per_rid_histogram | pre |  | n=1:9365; n=2:3 | 2026-04-21T05:56:43.017690 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows_per_rid_histogram | pre |  | n=1:9365; n=2:3 | 2026-04-21T05:58:40.999298 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows_per_rid_histogram | pre |  | n=1:9365; n=2:3 | 2026-04-21T06:01:51.079299 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows_per_rid_histogram | pre |  | n=1:9365; n=2:3 | 2026-04-21T06:02:44.257340 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows_per_rid_histogram | pre |  | n=1:9365; n=2:3 | 2026-04-21T06:03:19.270892 |
| 341_rebuild_operative_episode_multi_v2 | operative_episode_detail_v2 |  | rows_per_rid_histogram | pre |  | n=1:9365; n=2:3 | 2026-04-21T06:12:45.416424 |
| 342_backfill_op_esophageal_inv_any | _esoph_union |  | negated_only_rids | merged | 18.0 |  | 2026-04-21T06:05:02.248251 |
| 342_backfill_op_esophageal_inv_any | _esoph_union |  | negated_only_rids | merged | 18.0 |  | 2026-04-21T06:12:54.793722 |
| 342_backfill_op_esophageal_inv_any | _esoph_union |  | positive_rids | merged | 344.0 |  | 2026-04-21T06:05:02.215985 |
| 342_backfill_op_esophageal_inv_any | _esoph_union |  | positive_rids | merged | 344.0 |  | 2026-04-21T06:12:54.757220 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | false_count | post | 10527.0 |  | 2026-04-21T06:12:56.681013 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | false_count | pre | 0.0 |  | 2026-04-21T06:04:46.453476 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | false_count | pre | 0.0 |  | 2026-04-21T06:05:01.547151 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | false_count | pre | 0.0 |  | 2026-04-21T06:12:54.046614 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | nonnull | post | 10871.0 |  | 2026-04-21T06:12:56.599707 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | nonnull | pre | 0.0 |  | 2026-04-21T06:04:46.073794 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | nonnull | pre | 0.0 |  | 2026-04-21T06:05:01.168699 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | nonnull | pre | 0.0 |  | 2026-04-21T06:12:53.745014 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | true_count | post | 344.0 |  | 2026-04-21T06:12:56.642796 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | true_count | pre | 0.0 |  | 2026-04-21T06:04:46.421246 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | true_count | pre | 0.0 |  | 2026-04-21T06:05:01.516935 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_esophageal_inv_any | true_count | pre | 0.0 |  | 2026-04-21T06:12:54.010618 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | false_count | post | 4026.0 |  | 2026-04-21T06:12:56.825935 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | false_count | pre | 4026.0 |  | 2026-04-21T06:04:46.575329 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | false_count | pre | 4026.0 |  | 2026-04-21T06:05:01.667240 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | false_count | pre | 4026.0 |  | 2026-04-21T06:12:54.185295 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | nonnull | post | 4028.0 |  | 2026-04-21T06:12:56.747935 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | nonnull | pre | 4028.0 |  | 2026-04-21T06:04:46.511706 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | nonnull | pre | 4028.0 |  | 2026-04-21T06:05:01.603151 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | nonnull | pre | 4028.0 |  | 2026-04-21T06:12:54.111071 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | true_count | post | 2.0 |  | 2026-04-21T06:12:56.788685 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | true_count | pre | 2.0 |  | 2026-04-21T06:04:46.544811 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | true_count | pre | 2.0 |  | 2026-04-21T06:05:01.634812 |
| 342_backfill_op_esophageal_inv_any | canonical_patient_master | op_nlp_esophageal_involvement | true_count | pre | 2.0 |  | 2026-04-21T06:12:54.148634 |
| 342_backfill_op_esophageal_inv_any | note_entities_llm_airway_invasion | esophag_positive_only | rids_positive | source | 343.0 |  | 2026-04-21T06:05:02.155562 |
| 342_backfill_op_esophageal_inv_any | note_entities_llm_airway_invasion | esophag_positive_only | rids_positive | source | 343.0 |  | 2026-04-21T06:12:54.691645 |
| 342_backfill_op_esophageal_inv_any | note_entities_llm_airway_invasion | esophag_substring | rids_total | source | 361.0 |  | 2026-04-21T06:04:47.008266 |
| 342_backfill_op_esophageal_inv_any | note_entities_llm_airway_invasion | esophag_substring | rids_total | source | 361.0 |  | 2026-04-21T06:05:02.089809 |
| 342_backfill_op_esophageal_inv_any | note_entities_llm_airway_invasion | esophag_substring | rids_total | source | 361.0 |  | 2026-04-21T06:12:54.616892 |
| 342_backfill_op_esophageal_inv_any | note_entities_operative_detail | entity_type=esophageal_involvement | rids_present | source | 2.0 |  | 2026-04-21T06:04:46.761411 |
| 342_backfill_op_esophageal_inv_any | note_entities_operative_detail | entity_type=esophageal_involvement | rids_present | source | 2.0 |  | 2026-04-21T06:05:01.862779 |
| 342_backfill_op_esophageal_inv_any | note_entities_operative_detail | entity_type=esophageal_involvement | rids_present | source | 2.0 |  | 2026-04-21T06:12:54.387038 |
| 342_backfill_op_esophageal_inv_any | operative_episode_detail_v2 | esophageal_involvement_flag | nonnull | post | 11773.0 |  | 2026-04-21T06:12:56.892307 |
| 342_backfill_op_esophageal_inv_any | operative_episode_detail_v2 | esophageal_involvement_flag | nonnull | pre | 0.0 |  | 2026-04-21T06:04:46.630675 |
| 342_backfill_op_esophageal_inv_any | operative_episode_detail_v2 | esophageal_involvement_flag | nonnull | pre | 0.0 |  | 2026-04-21T06:05:01.726192 |
| 342_backfill_op_esophageal_inv_any | operative_episode_detail_v2 | esophageal_involvement_flag | nonnull | pre | 2.0 |  | 2026-04-21T06:12:54.249268 |
| 342_backfill_op_esophageal_inv_any | operative_episode_detail_v2 | esophageal_involvement_flag | true_count | post | 69.0 |  | 2026-04-21T06:12:56.931288 |
| 342_backfill_op_esophageal_inv_any | operative_episode_detail_v2 | esophageal_involvement_flag | true_count | pre | 0.0 |  | 2026-04-21T06:04:46.663935 |
| 342_backfill_op_esophageal_inv_any | operative_episode_detail_v2 | esophageal_involvement_flag | true_count | pre | 0.0 |  | 2026-04-21T06:05:01.764694 |
| 342_backfill_op_esophageal_inv_any | operative_episode_detail_v2 | esophageal_involvement_flag | true_count | pre | 2.0 |  | 2026-04-21T06:12:54.292189 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paralysis_evidence_tier | confirmed_without_tier | pre | 0.0 |  | 2026-04-21T06:06:06.488250 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paralysis_evidence_tier | confirmed_without_tier | pre | 0.0 |  | 2026-04-21T06:13:12.730391 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paralysis_evidence_tier | nonnull | pre | 34.0 |  | 2026-04-21T06:06:06.117759 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paralysis_evidence_tier | nonnull | pre | 34.0 |  | 2026-04-21T06:13:12.387726 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paralysis_evidence_tier | suspected_with_tier_lt_2 | pre | 0.0 |  | 2026-04-21T06:06:06.551934 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paralysis_evidence_tier | suspected_with_tier_lt_2 | pre | 0.0 |  | 2026-04-21T06:13:12.802188 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paralysis_evidence_tier | suspected_without_tier | pre | 0.0 |  | 2026-04-21T06:06:06.521399 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paralysis_evidence_tier | suspected_without_tier | pre | 0.0 |  | 2026-04-21T06:13:12.765940 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paresis_evidence_tier | confirmed_without_tier | pre | 0.0 |  | 2026-04-21T06:06:06.779190 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paresis_evidence_tier | confirmed_without_tier | pre | 0.0 |  | 2026-04-21T06:13:13.030317 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paresis_evidence_tier | nonnull | pre | 22.0 |  | 2026-04-21T06:06:06.648846 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paresis_evidence_tier | nonnull | pre | 22.0 |  | 2026-04-21T06:13:12.902434 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paresis_evidence_tier | suspected_with_tier_lt_2 | pre | 0.0 |  | 2026-04-21T06:06:06.844966 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paresis_evidence_tier | suspected_with_tier_lt_2 | pre | 0.0 |  | 2026-04-21T06:13:13.110313 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paresis_evidence_tier | suspected_without_tier | pre | 0.0 |  | 2026-04-21T06:06:06.812955 |
| 343_vc_tier_diagnostic | canonical_patient_master | comp_vc_paresis_evidence_tier | suspected_without_tier | pre | 0.0 |  | 2026-04-21T06:13:13.070711 |
| 343_vc_tier_diagnostic | canonical_patient_master |  | total_violations | diagnostic | 0.0 |  | 2026-04-21T06:06:06.876463 |
| 343_vc_tier_diagnostic | canonical_patient_master |  | total_violations | diagnostic | 0.0 |  | 2026-04-21T06:13:13.148684 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_first_date | delta | delta | 32.0 |  | 2026-04-21T06:14:05.136815 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_first_date | nonnull | post | 197.0 |  | 2026-04-21T06:14:04.976420 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_first_date | nonnull | pre | 165.0 |  | 2026-04-21T06:07:08.586567 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_first_date | nonnull | pre | 165.0 |  | 2026-04-21T06:09:34.119259 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_first_date | nonnull | pre | 165.0 |  | 2026-04-21T06:09:52.113153 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_first_date | nonnull | pre | 165.0 |  | 2026-04-21T06:13:17.128024 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_first_date | nonnull | pre | 165.0 |  | 2026-04-21T06:14:03.396835 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_first_date | planned_backfill | plan | 8.0 |  | 2026-04-21T06:07:09.492690 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_first_date | planned_backfill | plan | 32.0 |  | 2026-04-21T06:09:53.054611 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_first_date | planned_backfill | plan | 32.0 |  | 2026-04-21T06:13:18.094983 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_first_date | planned_backfill | plan | 32.0 |  | 2026-04-21T06:14:04.388425 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_last_date | delta | delta | 32.0 |  | 2026-04-21T06:14:05.170581 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_last_date | nonnull | post | 197.0 |  | 2026-04-21T06:14:05.039210 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_last_date | nonnull | pre | 165.0 |  | 2026-04-21T06:07:08.978467 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_last_date | nonnull | pre | 165.0 |  | 2026-04-21T06:09:34.528518 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_last_date | nonnull | pre | 165.0 |  | 2026-04-21T06:09:52.389760 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_last_date | nonnull | pre | 165.0 |  | 2026-04-21T06:13:17.423245 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_last_date | nonnull | pre | 165.0 |  | 2026-04-21T06:14:03.694186 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_last_date | planned_backfill | plan | 8.0 |  | 2026-04-21T06:07:09.527504 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_last_date | planned_backfill | plan | 32.0 |  | 2026-04-21T06:09:53.088952 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_last_date | planned_backfill | plan | 32.0 |  | 2026-04-21T06:13:18.136045 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_last_date | planned_backfill | plan | 32.0 |  | 2026-04-21T06:14:04.422494 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_most_recent | nonnull | pre | 154.0 |  | 2026-04-21T06:07:09.040250 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_most_recent | nonnull | pre | 154.0 |  | 2026-04-21T06:09:34.586257 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_most_recent | nonnull | pre | 154.0 |  | 2026-04-21T06:09:52.448661 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_most_recent | nonnull | pre | 154.0 |  | 2026-04-21T06:13:17.482844 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_most_recent | planned_backfill | plan | 13.0 |  | 2026-04-21T06:07:09.560604 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_most_recent | planned_backfill | plan | 37.0 |  | 2026-04-21T06:09:53.120135 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_most_recent | planned_backfill | plan | 37.0 |  | 2026-04-21T06:13:18.173285 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_most_recent_date | delta | delta | 32.0 |  | 2026-04-21T06:14:05.203968 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_most_recent_date | nonnull | post | 197.0 |  | 2026-04-21T06:14:05.100509 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_most_recent_date | nonnull | pre | 165.0 |  | 2026-04-21T06:14:03.752949 |
| 344_calcium_llm_recovery | canonical_patient_master | lab_calcium_most_recent_date | planned_backfill | plan | 32.0 |  | 2026-04-21T06:14:04.455428 |
| 344_calcium_llm_recovery | note_entities_llm_labs | calcium_dated | rids_dated | source | 115.0 |  | 2026-04-21T06:07:09.252731 |
| 344_calcium_llm_recovery | note_entities_llm_labs | calcium_dated | rids_dated | source | 139.0 |  | 2026-04-21T06:09:52.795126 |
| 344_calcium_llm_recovery | note_entities_llm_labs | calcium_dated | rids_dated | source | 139.0 |  | 2026-04-21T06:13:17.829254 |
| 344_calcium_llm_recovery | note_entities_llm_labs | calcium_dated | rids_dated | source | 139.0 |  | 2026-04-21T06:14:04.110789 |
| 344_calcium_llm_recovery | note_entities_llm_labs | calcium_in_entities_array | rids | source | 147.0 |  | 2026-04-21T06:09:52.653683 |
| 344_calcium_llm_recovery | note_entities_llm_labs | calcium_in_entities_array | rids | source | 147.0 |  | 2026-04-21T06:13:17.694117 |
| 344_calcium_llm_recovery | note_entities_llm_labs | calcium_in_entities_array | rids | source | 147.0 |  | 2026-04-21T06:14:03.974638 |
| 344_calcium_llm_recovery | note_entities_llm_labs | calcium_in_flat_object | rids | source | 47.0 |  | 2026-04-21T06:09:52.736369 |
| 344_calcium_llm_recovery | note_entities_llm_labs | calcium_in_flat_object | rids | source | 47.0 |  | 2026-04-21T06:13:17.763728 |
| 344_calcium_llm_recovery | note_entities_llm_labs | calcium_in_flat_object | rids | source | 47.0 |  | 2026-04-21T06:14:04.044146 |
| 344_calcium_llm_recovery | note_entities_llm_labs | calcium_substring | rids_mentioned | source | 147.0 |  | 2026-04-21T06:07:09.191057 |

## Leftover gaps (pending RunPod jobs)

- Real esophageal-invasion coverage beyond the existing TRUE rows requires dedicated RunPod extraction on 4,727 op-notes (Job 3).
- 3 stale LLM domains (pathology, cervical_ln_detail, tirads_granular) remain at qwen3:32b with 5,641-RID coverage; full 10,871-RID re-extraction at qwen2.5-32b is RunPod Job 1.
- TIRADS nodule `calcifications` field for 4,363 queued nodules is RunPod Job 2.
- `lab_calcium_first_date` recovery is bounded by available LLM source dates (see Script 344 ceiling note); broader recovery depends on RunPod Job 1 re-runs and Excel-labs ingestion.
