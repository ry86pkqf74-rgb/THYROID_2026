# NLP Entity Cross-Validation Report
> Generated: 2026-04-15 04:32:08 UTC  
> Database: `thyroid_ete_fix_20260413`  
> Confidence threshold: 0.5 (entity filter), 0.7 (novel-finding flag)
> **CANONICAL DATA IS READ-ONLY — no modifications made.**

## 1. Summary Table

| Domain | Source table | Notes w/ entities | Patients | Entities | Mean conf | Concordance | Discordant | Novel findings |
|---|---|---|---|---|---|---|---|
| RECURRENCE | note_entities_llm_recurrence | 145 | 143 | 302 | 0.902 | 69.0% | 44 | 0 |
| PATHOLOGY | note_entities_llm_pathology (fleet) | 6,286 | 2,826 | 26,827 | 0.901 | 90.6% | 232 | 13 |
| TIRADS | note_entities_llm_tirads_granular (fleet) | 2,172 | 1,716 | 13,480 | 0.854 | 89.0% | 55 | 5 |
| CERVICAL LN | note_entities_llm_cervical_ln_detail (fleet) | 1,479 | 1,149 | 2,916 | 0.843 | 91.0% | 113 | 75 |
| VASCULAR INVASION | note_entities_llm_vascular_invasion | 1,035 | 986 | 4,109 | 0.918 | 53.9% | 201 | 171 |
| TG KINETICS | note_entities_llm_tg_kinetics | 61 | 61 | 173 | 0.937 | 93.5% | 5 | 3 |

### Concordance Overview
```
RECURRENCE           █████████████░░░░░░░  69.0%
PATHOLOGY            ██████████████████░░  90.6%
TIRADS               █████████████████░░░  89.0%
CERVICAL LN          ██████████████████░░  91.0%
VASCULAR INVASION    ██████████░░░░░░░░░░  53.9%
TG KINETICS          ██████████████████░░  93.5%
```

## 2. Per-Domain Detail

### 2.1  RECURRENCE
**Source:** `note_entities_llm_recurrence` | **Model:** `qwen3:32b`

- Notes with entities: **145**
- Patients with entities: **143**
- Total entities (conf ≥ 0.5): **302**
- Mean confidence: **0.902**
- Concordance vs canonical: **69.0%** (98 concordant, 44 discordant)
- Novel findings (NLP only, no structured counterpart): **0**

#### Entity Type Breakdown
| entity_type | count |
|---|---|
| `structural_recurrence` | 155 |
| `disease_free` | 59 |
| `biochemical_persistence` | 25 |
| `biochemical_recurrence` | 23 |
| `distant_recurrence` | 23 |
| `surveillance_impression` | 10 |
| `rai_refractory` | 7 |

#### Top Discordant Patients (NLP ≠ Canonical)
| research_id | NLP says | Canonical says | Confidence |
|---|---|---|---|
| 2365 | recurrence_present | recurrence_confirmed=False | 0.900 |
| 3694 | recurrence_present | recurrence_confirmed=False | 0.950 |
| 9954 | recurrence_present | recurrence_confirmed=False | 0.800 |
| 3097 | recurrence_present | recurrence_confirmed=False | 0.950 |
| 3072 | recurrence_present | recurrence_confirmed=False | 0.950 |
| 3651 | recurrence_present | recurrence_confirmed=False | 0.950 |
| 3695 | recurrence_present | recurrence_confirmed=False | 0.980 |
| 3328 | recurrence_present | recurrence_confirmed=False | 0.980 |
| 8188 | recurrence_present | recurrence_confirmed=False | 0.950 |
| 10871 | recurrence_present | recurrence_confirmed=False | 0.900 |
| 8164 | recurrence_present | recurrence_confirmed=False | 0.980 |
| 6014 | recurrence_present | recurrence_confirmed=False | 0.950 |
| 3911 | recurrence_present | recurrence_confirmed=False | 0.950 |
| 6694 | recurrence_present | recurrence_confirmed=False | 0.900 |
| 9890 | recurrence_present | recurrence_confirmed=False | 0.950 |
| 3359 | recurrence_present | recurrence_confirmed=False | 0.900 |
| 2940 | recurrence_present | recurrence_confirmed=False | 0.950 |
| 3229 | recurrence_present | recurrence_confirmed=False | 0.900 |
| 10556 | recurrence_present | recurrence_confirmed=False | 0.800 |
| 3682 | recurrence_present | recurrence_confirmed=False | 0.850 |

### 2.2  PATHOLOGY
**Source:** `note_entities_llm_pathology (fleet)` | **Model:** `qwen3:32b + qwen3:14b`

- Notes with entities: **6,286**
- Patients with entities: **2,826**
- Total entities (conf ≥ 0.5): **26,827**
- Mean confidence: **0.901**
- Concordance vs canonical: **90.6%** (2,224 concordant, 232 discordant)
- Novel findings (NLP only, no structured counterpart): **13**

#### Entity Type Breakdown
| entity_type | count |
|---|---|
| `surgical_pathology` | 4,307 |
| `tumor_size` | 3,308 |
| `lymphovascular_invasion` | 2,836 |
| `lymph_node_pathology` | 2,605 |
| `fna_cytology` | 2,287 |
| `margin_status` | 2,229 |
| `extrathyroidal_extension` | 1,999 |
| `benign_pathology` | 1,878 |
| `multifocality` | 1,572 |
| `bethesda_class` | 1,104 |
| `molecular_testing` | 964 |
| `perineural_invasion` | 689 |
| `tumor_variant` | 684 |
| `frozen_section` | 244 |
| `procedure_performed` | 9 |

#### Top Discordant Patients (NLP ≠ Canonical)
| research_id | NLP says | Canonical says | Confidence |
|---|---|---|---|
| 5607 | malignant_histology | is_malignant=False | 0.950 |
| 8822 | malignant_histology | is_malignant=False | 0.990 |
| 6755 | malignant_histology | is_malignant=False | 0.950 |
| 6558 | malignant_histology | is_malignant=False | 0.950 |
| 7534 | malignant_histology | is_malignant=False | 0.950 |
| 9494 | malignant_histology | is_malignant=False | 0.980 |
| 7362 | malignant_histology | is_malignant=False | 0.950 |
| 9480 | malignant_histology | is_malignant=False | 0.980 |
| 7553 | malignant_histology | is_malignant=False | 0.980 |
| 8164 | malignant_histology | is_malignant=False | 0.950 |
| 6415 | malignant_histology | is_malignant=False | 0.950 |
| 7555 | malignant_histology | is_malignant=False | 0.950 |
| 9405 | malignant_histology | is_malignant=False | 0.980 |
| 9514 | malignant_histology | is_malignant=False | 0.950 |
| 6127 | malignant_histology | is_malignant=False | 0.950 |
| 6658 | malignant_histology | is_malignant=False | 0.950 |
| 7946 | malignant_histology | is_malignant=False | 0.980 |
| 6734 | malignant_histology | is_malignant=False | 0.950 |
| 8005 | malignant_histology | is_malignant=False | 0.950 |
| 11214 | malignant_histology | is_malignant=False | 0.980 |

#### Novel Findings (NLP present, canonical NULL)
| research_id | NLP says | Canonical says | Sub-domain |
|---|---|---|---|
| 9914 | ETE_present | ete_grade=nan | ETE |
| 10926 | ETE_present | ete_grade=nan | ETE |
| 9677 | ETE_present | ete_grade=nan | ETE |
| 3180 | ETE_present | ete_grade=nan | ETE |
| 1458 | ETE_present | ete_grade=nan | ETE |
| 7392 | ETE_present | ete_grade=nan | ETE |
| 9780 | ETE_present | ete_grade=nan | ETE |
| 9013 | ETE_present | ete_grade=nan | ETE |
| 11449 | ETE_present | ete_grade=nan | ETE |
| 6458 | ETE_present | ete_grade=nan | ETE |
| 1167 | ETE_present | ete_grade=nan | ETE |
| 8188 | ETE_present | ete_grade=nan | ETE |
| 10530 | ETE_present | ete_grade=nan | ETE |

### 2.3  TIRADS
**Source:** `note_entities_llm_tirads_granular (fleet)` | **Model:** `qwen3:32b + qwen3:14b`

- Notes with entities: **2,172**
- Patients with entities: **1,716**
- Total entities (conf ≥ 0.5): **13,480**
- Mean confidence: **0.854**
- Concordance vs canonical: **89.0%** (444 concordant, 55 discordant)
- Novel findings (NLP only, no structured counterpart): **5**

#### TIRADS Category Confusion (NLP score, canonical score) → count
```
  NLP=1, canonical=1): 9
  NLP=1, canonical=3): 1
  NLP=1, canonical=4): 5
  NLP=1, canonical=5): 1
  NLP=2, canonical=1): 6
  NLP=2, canonical=2): 15
  NLP=2, canonical=4): 11
  NLP=2, canonical=5): 7
  NLP=3, canonical=1): 11
  NLP=3, canonical=2): 4
  NLP=3, canonical=3): 32
  NLP=3, canonical=4): 114
  NLP=3, canonical=5): 13
  NLP=4, canonical=1): 3
  NLP=4, canonical=2): 3
  NLP=4, canonical=3): 4
  NLP=4, canonical=4): 108
  NLP=4, canonical=5): 77
  NLP=5, canonical=4): 9
  NLP=5, canonical=5): 66
```

#### Entity Type Breakdown
| entity_type | count |
|---|---|
| `nodule_dimensions` | 4,852 |
| `nodule_identifier` | 2,306 |
| `tirads_composition` | 1,210 |
| `tirads_echogenicity` | 972 |
| `nodule_stability` | 963 |
| `tirads_recommendation` | 886 |
| `tirads_category` | 648 |
| `tirads_margin` | 442 |
| `tirads_shape` | 344 |
| `tirads_echogenic_foci` | 337 |
| `tirads_total_points` | 235 |
| `tirads_vascularity` | 231 |
| `us_visit_number` | 31 |
| `nodule_growth_rate` | 13 |
| `nodule_volume` | 7 |

#### Top Discordant Patients (NLP ≠ Canonical)
| research_id | NLP says | Canonical says | Confidence |
|---|---|---|---|
| 10000 | TR3 | TR1 (best_category_v12=TR1_Benign) | 0.800 |
| 10005 | TR3 | TR1 (best_category_v12=TR1_Benign) | 0.900 |
| 10056 | TR2 | TR4 (best_category_v12=TR4_Moderately_Suspicious) | 0.950 |
| 10095 | TR3 | TR1 (best_category_v12=TR1_Benign) | 0.900 |
| 10248 | TR3 | TR5 (best_category_v12=TR5_Highly_Suspicious) | 0.950 |
| 10441 | TR1 | TR4 (best_category_v12=TR4_Moderately_Suspicious) | 0.950 |
| 10448 | TR4 | TR1 (best_category_v12=TR1_Benign) | 0.950 |
| 10542 | TR1 | TR3 (best_category_v12=TR3_Mildly_Suspicious) | 0.900 |
| 10560 | TR2 | TR4 (best_category_v12=TR4_Moderately_Suspicious) | 0.950 |
| 10574 | TR3 | TR1 (best_category_v12=TR1_Benign) | 0.900 |
| 10683 | TR3 | TR5 (best_category_v12=TR5_Highly_Suspicious) | 0.800 |
| 10737 | TR3 | TR1 (best_category_v12=TR1_Benign) | 0.950 |
| 10909 | TR3 | TR5 (best_category_v12=TR5_Highly_Suspicious) | 0.950 |
| 10967 | TR3 | TR1 (best_category_v12=TR1_Benign) | 0.750 |
| 11017 | TR3 | TR1 (best_category_v12=TR1_Benign) | 0.950 |
| 11049 | TR3 | TR5 (best_category_v12=TR5_Highly_Suspicious) | 0.950 |
| 11173 | TR3 | TR1 (best_category_v12=TR1_Benign) | 0.900 |
| 11318 | TR2 | TR5 (best_category_v12=TR5_Highly_Suspicious) | 0.900 |
| 11329 | TR3 | TR5 (best_category_v12=TR5_Highly_Suspicious) | 0.950 |
| 11344 | TR1 | TR4 (best_category_v12=TR4_Moderately_Suspicious) | 0.650 |

#### Novel Findings (NLP present, canonical NULL)
| research_id | NLP says | Canonical says | Sub-domain |
|---|---|---|---|
| 10933 | TR4 | NULL | tirads |
| 10941 | TR5 | NULL | tirads |
| 11055 | TR4 | NULL | tirads |
| 11177 | TR5 | NULL | tirads |
| 11475 | TR5 | NULL | tirads |

### 2.4  CERVICAL LN
**Source:** `note_entities_llm_cervical_ln_detail (fleet)` | **Model:** `qwen3:32b + qwen3:14b`

- Notes with entities: **1,479**
- Patients with entities: **1,149**
- Total entities (conf ≥ 0.5): **2,916**
- Mean confidence: **0.843**
- Concordance vs canonical: **91.0%** (1,149 concordant, 113 discordant)
- Novel findings (NLP only, no structured counterpart): **75**

- NLP sensitivity vs structured LN positive: **86.1%**
- NLP PPV vs structured LN positive: **100.0%**
- TP/TN/FP/FN: 699/150/0/113

#### Entity Type Breakdown
| entity_type | count |
|---|---|
| `ln_level` | 2,068 |
| `ln_number_per_level` | 511 |
| `ln_size` | 142 |
| `fna_of_ln` | 105 |
| `suspicious_features_count` | 38 |
| `ln_laterality` | 30 |
| `ln_morphology` | 8 |
| `microcalcifications_ln` | 8 |
| `cystic_change` | 6 |

#### Top Discordant Patients (NLP ≠ Canonical)
| research_id | NLP says | Canonical says | Confidence |
|---|---|---|---|
| 793 | no_ln_involvement | ln_rollup_any_positive=True | 0.980 |
| 5765 | no_ln_involvement | ln_rollup_any_positive=True | 0.900 |
| 7646 | no_ln_involvement | ln_rollup_any_positive=True | 0.800 |
| 9708 | no_ln_involvement | ln_rollup_any_positive=True | 0.700 |
| 5053 | no_ln_involvement | ln_rollup_any_positive=True | 0.800 |
| 4572 | no_ln_involvement | ln_rollup_any_positive=True | 0.900 |
| 5001 | no_ln_involvement | ln_rollup_any_positive=True | 0.800 |
| 6794 | no_ln_involvement | ln_rollup_any_positive=True | 0.950 |
| 4946 | no_ln_involvement | ln_rollup_any_positive=True | 0.700 |
| 4477 | no_ln_involvement | ln_rollup_any_positive=True | 0.950 |
| 8834 | no_ln_involvement | ln_rollup_any_positive=True | 0.980 |
| 6715 | no_ln_involvement | ln_rollup_any_positive=True | 0.950 |
| 4433 | no_ln_involvement | ln_rollup_any_positive=True | 0.980 |
| 10937 | no_ln_involvement | ln_rollup_any_positive=True | 0.800 |
| 11035 | no_ln_involvement | ln_rollup_any_positive=True | 0.980 |
| 9353 | no_ln_involvement | ln_rollup_any_positive=True | 0.950 |
| 4293 | no_ln_involvement | ln_rollup_any_positive=True | 0.980 |
| 7471 | no_ln_involvement | ln_rollup_any_positive=True | 0.950 |
| 7577 | no_ln_involvement | ln_rollup_any_positive=True | 0.950 |
| 9701 | no_ln_involvement | ln_rollup_any_positive=True | 0.980 |

#### Novel Findings (NLP present, canonical NULL)
| research_id | NLP says | Canonical says | Sub-domain |
|---|---|---|---|
| 1130 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 10582 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 10976 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 538 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 129 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 76 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 8043 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 8 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 683 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 334 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 1262 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 272 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 248 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 3487 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 96 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 7530 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 702 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 8585 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 221 | ln_level_identified | all_ln_fields=NULL | cervical_ln |
| 483 | ln_level_identified | all_ln_fields=NULL | cervical_ln |

### 2.5  VASCULAR INVASION
**Source:** `note_entities_llm_vascular_invasion` | **Model:** `qwen3:32b`

- Notes with entities: **1,035**
- Patients with entities: **986**
- Total entities (conf ≥ 0.5): **4,109**
- Mean confidence: **0.918**
- Concordance vs canonical: **53.9%** (235 concordant, 201 discordant)
- Novel findings (NLP only, no structured counterpart): **171**

#### Entity Type Breakdown
| entity_type | count |
|---|---|
| `soft_tissue_invasion` | 654 |
| `vascular_invasion` | 634 |
| `capsular_invasion` | 611 |
| `ptnm_stage` | 403 |
| `vessel_count` | 251 |
| `synoptic_report` | 251 |
| `perineural_invasion_detailed` | 249 |
| `dedifferentiation` | 225 |
| `necrosis` | 199 |
| `mitotic_rate` | 186 |
| `tall_cell_percentage` | 183 |
| `ki67_index` | 182 |
| `vascular_invasion_type` | 71 |
| `entity_date` | 4 |
| `lymphatic_invasion` | 3 |

#### Top Discordant Patients (NLP ≠ Canonical)
| research_id | NLP says | Canonical says | Confidence |
|---|---|---|---|
| 3502 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.980 |
| 5472 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.950 |
| 11952 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.990 |
| 6193 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.970 |
| 6898 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.950 |
| 5975 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.980 |
| 5306 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.980 |
| 11261 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.920 |
| 11808 | vascular_invasion_absent | vascular_invasion_grade=indeterminate | 0.950 |
| 7234 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.980 |
| 9648 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.990 |
| 6097 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.980 |
| 10937 | vascular_invasion_absent | vascular_invasion_grade=extensive | 0.900 |
| 11104 | vascular_invasion_absent | vascular_invasion_grade=focal | 0.980 |
| 6669 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.980 |
| 6552 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.980 |
| 5490 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.970 |
| 9715 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.990 |
| 9489 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.880 |
| 7159 | vascular_invasion_absent | vascular_invasion_grade=present_ungraded | 0.950 |

#### Novel Findings (NLP present, canonical NULL)
| research_id | NLP says | Canonical says | Sub-domain |
|---|---|---|---|
| 2521 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2353 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2020 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2346 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2455 | vascular_invasion_present | grade=nan | vascular_invasion |
| 7876 | vascular_invasion_present | grade=nan | vascular_invasion |
| 6458 | vascular_invasion_present | grade=nan | vascular_invasion |
| 1970 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2033 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2393 | vascular_invasion_present | grade=nan | vascular_invasion |
| 1967 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2467 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2516 | vascular_invasion_present | grade=nan | vascular_invasion |
| 8439 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2402 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2487 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2371 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2367 | vascular_invasion_present | grade=nan | vascular_invasion |
| 2357 | vascular_invasion_present | grade=nan | vascular_invasion |
| 8967 | vascular_invasion_present | grade=nan | vascular_invasion |

### 2.6  TG KINETICS
**Source:** `note_entities_llm_tg_kinetics` | **Model:** `qwen3:32b`

- Notes with entities: **61**
- Patients with entities: **61**
- Total entities (conf ≥ 0.5): **173**
- Mean confidence: **0.937**
- Concordance vs canonical: **93.5%** (72 concordant, 5 discordant)
- Novel findings (NLP only, no structured counterpart): **3**

#### Entity Type Breakdown
| entity_type | count |
|---|---|
| `tg_value` | 131 |
| `anti_tg_value` | 25 |
| `paired_tsh` | 9 |
| `tg_context` | 4 |
| `tg_assay_method` | 2 |
| `tg_detection_limit` | 1 |
| `tg_trend` | 1 |

#### Top Discordant Patients (NLP ≠ Canonical)
| research_id | NLP says | Canonical says | Confidence |
|---|---|---|---|
| 10145 | tg_value=132.1 (elevated) | tg_peak=0.90 | 0.980 |
| 10157 | tg_value=12.9 (elevated) | tg_peak=0.90 | 0.980 |
| 11215 | tg_value=63.0 (elevated) | tg_peak=0.90 | 0.850 |
| 11463 | tg_value=387.0 (elevated) | tg_peak=0.70 | 0.800 |
| 7580 | tg_value=10.0 (elevated) | tg_peak=0.10 | 0.700 |

#### Novel Findings (NLP present, canonical NULL)
| research_id | NLP says | Canonical says | Sub-domain |
|---|---|---|---|
| 8379 | tg_value=15.5 | tg_n_measurements=NULL/0 | tg_kinetics |
| 6592 | tg_value=10.7 | tg_n_measurements=NULL/0 | tg_kinetics |
| 9448 | tg_value=? | tg_n_measurements=NULL/0 | tg_kinetics |

## 3. NLP-Only Flags (No Structured Data Counterpart, conf ≥ 0.7)

These patients have **high-confidence NLP entities** but NULL in the corresponding canonical column. Highest priority for manual review / future gap-filling.

### PATHOLOGY — 13 NLP-only patients (conf ≥ 0.7, canonical NULL)

| research_id | entity_type | entity_value | confidence | canonical_field | canonical_value |
|---|---|---|---|---|---|
| 9914 | `extrathyroidal_extension` | present | 0.980 | `ETE` | ete_grade=nan |
| 10926 | `tumor_size` | 5.3 cm | 0.980 | `ETE` | ete_grade=nan |
| 9677 | `fna_cytology` | AUS | 0.980 | `ETE` | ete_grade=nan |
| 3180 | `surgical_pathology` | papillary thyroid carcinoma | 0.980 | `ETE` | ete_grade=nan |
| 1458 | `multifocality` | six soft tissue nodules | 0.980 | `ETE` | ete_grade=nan |
| 7392 | `surgical_pathology` | benign | 0.950 | `ETE` | ete_grade=nan |
| 9780 | `surgical_pathology` | Nodular follicular disease/nodular hyperplasia with dominant | 0.980 | `ETE` | ete_grade=nan |
| 9013 | `surgical_pathology` | Left thyroid lobe and left substernal goiter removed | 0.980 | `ETE` | ete_grade=nan |
| 11449 | `bethesda_class` | AUS/FLUS | 0.980 | `ETE` | ete_grade=nan |
| 6458 | `tumor_size` | 5.8 cm | 0.990 | `ETE` | ete_grade=nan |
| 1167 | `surgical_pathology` | Follicular tumor of uncertain malignant potential | 0.950 | `ETE` | ete_grade=nan |
| 8188 | `surgical_pathology` | papillary thyroid carcinoma | 0.980 | `ETE` | ete_grade=nan |
| 10530 | `tumor_size` | 3.2 cm | 0.950 | `ETE` | ete_grade=nan |

### TIRADS — 5 NLP-only patients (conf ≥ 0.7, canonical NULL)

| research_id | entity_type | entity_value | confidence | canonical_field | canonical_value |
|---|---|---|---|---|---|
| 10933 | `tirads_category` | TR4 | 0.950 | `tirads_best_category_v12` | NULL |
| 10941 | `tirads_category` | TR5 | 0.950 | `tirads_best_category_v12` | NULL |
| 11055 | `tirads_category` | TR4 | 0.950 | `tirads_best_category_v12` | NULL |
| 11177 | `tirads_category` | TR5 | 0.950 | `tirads_best_category_v12` | NULL |
| 11475 | `tirads_category` | TR5 | 0.950 | `tirads_best_category_v12` | NULL |

### CERVICAL LN — 42 NLP-only patients (conf ≥ 0.7, canonical NULL)

| research_id | entity_type | entity_value | confidence | canonical_field | canonical_value |
|---|---|---|---|---|---|
| 1130 | `ln_level` | pretracheal lymph node, 0.1-0.7 cm | 0.950 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 10582 | `ln_level` | bilateral level VI lymph node 0.8 x 0.7 x 0.5 cm | 0.800 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 10976 | `ln_level` | central lymph node | 0.800 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 538 | `ln_level` | pretracheal lymph nodes (level VI), 4 nodes, reactive (0/4) | 0.950 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 129 | `ln_level` | pretracheal lymph nodes (level VI), 3 nodes identified | 0.900 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 76 | `ln_level` | pretracheal lymph node (level VI) | 0.900 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 8043 | `ln_level` | left cervical lymphadenopathy | 0.800 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 8 | `ln_level` | left level VI lymph node 0.7 x 0.5 x 0.3 cm | 0.950 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 683 | `ln_level` | pretracheal lymph node (level VI), 5 nodes identified | 0.900 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 334 | `ln_level` | pretracheal lymph node (level VI) | 0.900 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 1262 | `ln_level` | pretracheal lymph node excision, extensive cautery artifact | 0.700 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 272 | `ln_level` | pretracheal lymph node | 0.800 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 248 | `ln_level` | pretracheal lymph node, 1 node, negative for malignancy | 0.950 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 3487 | `ln_level` | pretracheal lymph node (level VI) | 0.800 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 96 | `ln_level` | pretracheal lymph node, biopsy negative for carcinoma | 0.800 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 7530 | `ln_level` | right possible lymph node 1.4 x 1.1 x 0.7 cm | 0.700 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 702 | `ln_level` | pretracheal lymph node (level VI) | 0.900 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 8585 | `ln_level` | left superior parathyroid vs. lymph node | 0.800 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 221 | `ln_level` | left superficial neck node | 0.800 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 483 | `ln_level` | pretracheal lymph node 1.0 x 0.5 x 0.3 cm | 0.800 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 8185 | `ln_level` | neck level I | 0.900 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 2837 | `ln_level` | right level II | 0.900 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 2431 | `ln_level` | isthmus lymph node (level VI) | 0.900 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 559 | `ln_level` | level VI (pretracheal) lymph node, 1 node, negative for meta | 0.900 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 5552 | `ln_level` | left and right neck | 0.950 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 6630 | `ln_level` | left neck lymph node | 0.950 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 277 | `ln_level` | pretracheal lymph node, benign reactive | 0.700 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 8986 | `ln_level` | left level III | 0.900 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 519 | `ln_level` | level VI | 0.800 | `ln_rollup_any_positive / ln_positive_flag` | NULL |
| 9331 | `ln_level` | possible lymph node at apical portion 0.5 x 0.4 x 0.3 cm | 0.700 | `ln_rollup_any_positive / ln_positive_flag` | NULL |

### VASCULAR INVASION — 50 NLP-only patients (conf ≥ 0.7, canonical NULL)

| research_id | entity_type | entity_value | confidence | canonical_field | canonical_value |
|---|---|---|---|---|---|
| 2521 | `vascular_invasion` | present (3 nerves involved) | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2353 | `vascular_invasion` | yes | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2020 | `vascular_invasion` | present | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2346 | `vascular_invasion` | present | 0.990 | `vascular_invasion_grade` | NULL/absent |
| 2455 | `vascular_invasion` | 4 | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 7876 | `vascular_invasion` | absent | 0.970 | `vascular_invasion_grade` | NULL/absent |
| 6458 | `vascular_invasion` | absent | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 1970 | `vascular_invasion` | present (3 nerves involved) | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2033 | `vascular_invasion` | absent | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2393 | `vascular_invasion` | 4 | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 1967 | `vascular_invasion` | strap muscle invasion | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2467 | `vascular_invasion` | yes | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2516 | `vascular_invasion` | present | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 8439 | `vascular_invasion` | 4 | 0.950 | `vascular_invasion_grade` | NULL/absent |
| 2402 | `vascular_invasion` | 4 | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2487 | `vascular_invasion` | 4 | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2371 | `vascular_invasion` | yes | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2367 | `vascular_invasion` | present | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2357 | `vascular_invasion` | 4 | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 8967 | `vascular_invasion` | absent | 0.970 | `vascular_invasion_grade` | NULL/absent |
| 2533 | `vascular_invasion` | strap muscle invasion | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 9490 | `vascular_invasion` | present | 0.970 | `vascular_invasion_grade` | NULL/absent |
| 2384 | `vascular_invasion` | absent | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 11759 | `vascular_invasion` | absent | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2395 | `vascular_invasion` | focal | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2492 | `vascular_invasion` | yes | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2363 | `vascular_invasion` | focal | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2541 | `vascular_invasion` | yes | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2475 | `vascular_invasion` | strap muscle invasion | 0.980 | `vascular_invasion_grade` | NULL/absent |
| 2444 | `vascular_invasion` | present | 0.980 | `vascular_invasion_grade` | NULL/absent |

### TG KINETICS — 3 NLP-only patients (conf ≥ 0.7, canonical NULL)

| research_id | entity_type | entity_value | confidence | canonical_field | canonical_value |
|---|---|---|---|---|---|
| 8379 | `tg_value` | tg_value=15.5 | 0.950 | `tg_n_measurements` | NULL/0 |
| 6592 | `tg_value` | tg_value=10.7 | 0.980 | `tg_n_measurements` | NULL/0 |
| 9448 | `tg_value` | tg_value=? | 0.950 | `tg_n_measurements` | NULL/0 |

**Total NLP-only high-confidence flags across all domains: 113**

## 4. Recommendations

### Which NLP entities are trustworthy enough to backfill canonical gaps?

- **RECURRENCE** (69.0% concordance): ⚠️ CONDITIONAL
  → Use with caution. Manual review of discordant cases recommended before backfill.
- **PATHOLOGY** (90.6% concordance): ✅ TRUSTWORTHY
  → Candidates for NLP-assisted backfill. Novel findings (13) can be routed to structured gap-fill (future script).
- **TIRADS** (89.0% concordance): ✅ TRUSTWORTHY
  → Candidates for NLP-assisted backfill. Novel findings (5) can be routed to structured gap-fill (future script).
- **CERVICAL LN** (91.0% concordance): ✅ TRUSTWORTHY
  → Candidates for NLP-assisted backfill. Novel findings (75) can be routed to structured gap-fill (future script).
- **VASCULAR INVASION** (53.9% concordance): ❌ UNRELIABLE
  → Do NOT backfill without manual adjudication. High discordance suggests entity type mismatch or boilerplate contamination.
- **TG KINETICS** (93.5% concordance): ✅ TRUSTWORTHY
  → Candidates for NLP-assisted backfill. Novel findings (3) can be routed to structured gap-fill (future script).

### Domains with too many discordances to trust without review

- **PATHOLOGY**: 232 discordant cases — root-cause investigation recommended before any backfill.
- **TIRADS**: 55 discordant cases — root-cause investigation recommended before any backfill.
- **CERVICAL LN**: 113 discordant cases — root-cause investigation recommended before any backfill.
- **VASCULAR INVASION**: 201 discordant cases — root-cause investigation recommended before any backfill.

### Novel findings warranting manual review

- **PATHOLOGY**: 13 patients with NLP evidence but no structured data — highest value for data enrichment. Cross-reference with source notes before promotion.
- **CERVICAL LN**: 75 patients with NLP evidence but no structured data — highest value for data enrichment. Cross-reference with source notes before promotion.
- **VASCULAR INVASION**: 171 patients with NLP evidence but no structured data — highest value for data enrichment. Cross-reference with source notes before promotion.

---
*Report generated by `scripts/209_nlp_entity_crossvalidation.py`*  
*Database: `thyroid_ete_fix_20260413` | Run: 2026-04-15 04:32:08 UTC*