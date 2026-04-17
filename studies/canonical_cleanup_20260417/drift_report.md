# Drift report (preflight) — canonical cleanup 20260417

_Generated 2026-04-17T09:25:58.386993+00:00; database=`thyroid_canonical_publication_v1_0`._

## Cited vs observed counts

| Object | Cited (markdowns) | Observed (live) | Delta | Flag |
|---|---:|---:|---:|---|
| `canonical_patient_master` | 10,871 | 10,871 | +0 | OK |
| `operative_episode_detail_v2` | 9,371 | 9,371 | +0 | OK |
| `complication_phenotype_v1` | 5,978 | 5,978 | +0 | OK |
| `thyroglobulin_lab_canonical_v1` | 76,971 | 74,258 | -2,713 | DRIFT |

## Additional pre-state row counts

| Object | Rows |
|---|---:|
| `fna_episode_master_v2` | 8,119 |
| `rai_treatment_episode_v2` | 1,857 |
| `canonical_us_nodule_characteristics_v1` | 37,016 |
| `synoptic_tumor_long_v1` | 11,103 |

## Phase-relevant column presence on canonical_patient_master

### ajcc8_columns_present
- `ajcc8_t_stage`: present
- `ajcc8_t_stage_v2`: present
- `ajcc8_t_stage_corrected`: present

### multifocal_columns_present
- `multifocal_flag_path`: present
- `DEPRECATED__path_multifocal_flag`: absent
- `path_multifocal_flag`: absent
- `nlp_path_multifocal_mentioned`: present

### complication_columns_present
- `comp_vc_paralysis_confirmed`: present
- `comp_vc_paresis_confirmed`: present
- `comp_hematoma_confirmed`: present
- `comp_seroma_confirmed`: present
- `comp_chyle_leak_confirmed`: present
- `comp_wound_infection_confirmed`: present
- `any_confirmed_complication_flag`: present
- `comp_hypoparathyroidism_permanent`: present
- `comp_hypopara_permanent_source`: absent
- `lateral_neck_dissected`: present
- `lateral_neck_dissected_structured_or_nlp`: absent
- `cpm_built_at`: absent
- `worst_bethesda_source`: present
- `rai_max_dose_mci`: present
- `rai_dose_v9`: present
- `n_fna_episodes`: present
- `max_tirads_ever`: present

## FNA episode-count distribution (n >= 10)

| n_episodes | n_patients |
|---:|---:|
| 11 | 2 |
| 12 | 3 |

CPM patients currently with n_fna_episodes IN (11,12): **5**

manuscript_workspace VIEW count: **65** (expected 65)

## Preflight assertions

| Key | Status | Observed | Expected |
|---|---|---|---|
| `a_cpm_rowcount` | PASS | 10871 | 10871 |
| `b_cpm_distinct_research_id` | PASS | 10871 | 10871 |
| `c_col_r_class_true` | PASS | True | True |
| `c_col_ete_grade_final_v2` | PASS | True | True |
| `d_operative_episode_detail_v2_rows` | PASS | 9371 | '9371 +/- 5' |
| `e_complication_phenotype_v1_rows` | PASS | 5978 | '5978 +/- 50' |
| `f_us_tirads_columns` | PASS | ['tirads_acr_recalculated', 'tirads_reported'] | ['tirads_reported', 'tirads_acr_recalculated'] |
| `g_thyroglobulin_lab_canonical_v1_rows` | PASS | 74258 | '74258 +/- 500' |
| `h_vc_paralysis_recalibration_v236_exists` | PASS | 1 | 1 |
| `i_archive_pub_v1_0_table_count` | PASS | 182 | '> 100' |

## Result

**All preflight assertions PASSED.**
