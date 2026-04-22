# Script 389 — Pre-state probe report

Generated: 2026-04-22T21:00:17.875247+00:00
PUB DB: `thyroid_canonical_publication_v1_0` · Archive DB: `thyroid_canonical_publication_v1_0`

## 0A · Preflight

* `pub_object_count` = `289`
* `n_388_log_rows` = `9`
* `n_386_workspace_tables` = `0`
* `n_386_legacy_log_rows` = `28`
* `has_386_close_out` = `True`
* `preflight_missing` = `[]`

## 0B · `canonical_us_nodule_v2` source-flag partition

**Classifier reset 2026-04-22.** The original prompt's baselines (`18,310 / 17,090 / 2,152 / 27` for `clean_llm_parsed / clean_non_llm / zombie_parent / llm_parsed_but_blob`) were not reproducible against live state — direct MotherDuck probe showed no combination of source-flags or `location_raw` content signals yields that partition.  They were phantom from compressed context.  The classifier is now grounded in the four boolean flags that actually do partition the table: `is_aggregate_row`, `source_tirads_llm`, `source_base`, `nlp_backfill_pending`.  Baselines below are frozen from the 2026-04-22 probe.  The Phase 2 DELETE step that the prior classifier targeted is RETIRED — none of the four buckets are "zombie" in the structural sense; `needs_backfill` rows are legitimate entries awaiting NLP extraction, not remnants to delete.

| bucket | actual | expected | Δ % |
|---|---|---|---|
| `clean_dual_source` | 26,402 | 26,402 | +0.00% |
| `clean_base_only` | 8,919 | 8,919 | +0.00% |
| `needs_backfill` | 2,117 | 2,117 | +0.00% |
| `aggregate_rollup` | 141 | 141 | +0.00% |
| **TOTAL** | **37,579** | **37,579** | +0.00% |

Classifier expression (source-flag partition; PHI-safe):

```sql
CASE
      WHEN is_aggregate_row = TRUE
        THEN 'aggregate_rollup'
      WHEN source_tirads_llm = TRUE AND source_base = TRUE
        THEN 'clean_dual_source'
      WHEN (source_tirads_llm = TRUE
            AND COALESCE(source_base, FALSE) = FALSE)
        OR (COALESCE(source_tirads_llm, FALSE) = FALSE
            AND source_base = TRUE)
        THEN 'clean_base_only'
      ELSE
        'needs_backfill'
    END
```

## 0C · US view-stack probe

* `canonical_us_exam_master_VIEW_v2` total rows: 11,759
* `canonical_us_exam_master_VIEW_v2` phantom rows (NULL exam_date + NULL findings): 0 (expected ≈ 6,792)
* CPM CTE in exam_master body (load-bearing for `is_preop_exam`, NOT a row-shape driver): `True`
* `canonical_us_patient_master_VIEW_v2` total rows: 4,360
* `has_any_us=TRUE` patients: 4,360
* Bug rows (has_any_us=TRUE + both dates NULL): 0 (expected ≈ 6,499)

### Phantom root cause — NULL `exam_date` in source tables

After the GROUP BY `(research_id, exam_date)` in each source CTE, NULL-date rows collapse into `(research_id, NULL)` pairs.  The UNION of those pairs is exactly the phantom set in `canonical_us_exam_master_VIEW_v2`.  Phase 2D fix: add `WHERE exam_date IS NOT NULL` to every source CTE BEFORE aggregation; CPM CTE + `is_preop_exam` column stay intact.

| source table | total rows | NULL exam_date | expected | drift OK |
|---|---|---|---|---|
| `canonical_us_thyroid_gland_v2` | 13,578 | 6,785 | 6,785 | ok |
| `canonical_us_nodule_v2` | 37,579 | 2,231 | 2,231 | ok |
| `canonical_us_lymph_node_v2` | 6,801 | 0 | 0 | ok |
| **total source NULL-exam_date** | — | **9,016** | ≈ 6,792 (after UNION) | — |

### Boolean-literal census

Every `CAST('t' AS BOOLEAN)` / `CAST('f' AS BOOLEAN)` / bare `TRUE` / `FALSE` in the live view bodies, with the nearest downstream `AS <alias>` (heuristic, ≤64 chars).  `has_any_us` in patient_master is the known bug; everything else is expected (the gland_agg / ln_agg CTEs intentionally emit `TRUE AS has_gland_findings` / `TRUE AS has_us_ln_findings` inside aggregations that group by `(research_id, exam_date)`, so each row by construction represents an exam where that modality had findings).

#### `canonical_us_exam_master_VIEW_v2` (14 literals; aliases: ['BOOLEAN', 'any_nlp_backfill_pending_on_exam', 'has_gland_findings', 'has_us_ln_findings', 'is_preop_exam', 'n_abnormal_us_ln_on_exam'])

| literal | downstream_alias | offset |
|---|---|---|
| `CAST('f' AS BOOLEAN)` | `BOOLEAN` | 1309 |
| `CAST('f' AS BOOLEAN)` | `—` | 1333 |
| `CAST('f' AS BOOLEAN)` | `BOOLEAN` | 1758 |
| `CAST('f' AS BOOLEAN)` | `—` | 1782 |
| `CAST('t' AS BOOLEAN)` | `has_gland_findings` | 1993 |
| `CAST('t' AS BOOLEAN)` | `has_us_ln_findings` | 2317 |
| `CAST('f' AS BOOLEAN)` | `n_abnormal_us_ln_on_exam` | 2442 |
| `CAST('f' AS BOOLEAN)` | `has_gland_findings` | 3308 |
| `CAST('f' AS BOOLEAN)` | `has_us_ln_findings` | 3384 |
| `CAST('t' AS BOOLEAN)` | `BOOLEAN` | 3705 |
| `CAST('f' AS BOOLEAN)` | `is_preop_exam` | 3732 |
| `CAST('f' AS BOOLEAN)` | `BOOLEAN` | 3815 |
| `CAST('f' AS BOOLEAN)` | `BOOLEAN` | 3878 |
| `CAST('f' AS BOOLEAN)` | `any_nlp_backfill_pending_on_exam` | 3941 |

#### `canonical_us_patient_master_VIEW_v2` (2 literals; aliases: ['has_any_us', 'tirads_category_at_last_preop_exam'])

| literal | downstream_alias | offset |
|---|---|---|
| `CAST('t' AS BOOLEAN)` | `has_any_us` | 89 |
| `CAST('t' AS BOOLEAN)` | `tirads_category_at_last_preop_exam` | 1416 |

### Live view body — exam_master
```sql
CREATE VIEW canonical_us_exam_master_VIEW_v2 AS WITH nodule_agg AS (SELECT research_id, exam_date, any_value(us_exam_id) AS us_exam_id_nodule, count_star() AS n_nodules_on_exam, max(size_cm_max) AS largest_nodule_cm, (bool_or((lower(COALESCE(laterality, '')) = 'right')) AND bool_or((lower(COALESCE(laterality, '')) = 'left'))) AS bilateral_flag, (bool_or((lower(COALESCE(laterality, '')) = 'isthmus')) OR bool_or((lower(COALESCE(location_raw, '')) ~~ '%isthmus%'))) AS isthmus_nodule_flag, max(acr2017_tirads_category) AS worst_tirads_category_this_exam, max(acr2017_tirads_points) AS worst_tirads_points_this_exam, min(acr2017_tirads_category) AS best_tirads_category_this_exam, sum(CASE  WHEN ((upper(acr2017_tirads_category) = 'TR5')) THEN (1) ELSE 0 END) AS count_tr5, sum(CASE  WHEN ((upper(acr2017_tirads_category) = 'TR4')) THEN (1) ELSE 0 END) AS count_tr4, sum(CASE  WHEN ((upper(acr2017_tirads_category) = 'TR3')) THEN (1) ELSE 0 END) AS count_tr3, sum(CASE  WHEN ((upper(acr2017_tirads_category) = 'TR2')) THEN (1) ELSE 0 END) AS count_tr2, sum(CASE  WHEN ((upper(acr2017_tirads_category) = 'TR1')) THEN (1) ELSE 0 END) AS count_tr1, bool_or(nlp_backfill_pending) AS any_nodule_pending_on_exam FROM thyroid_canonical_publication_v1_0.main.canonical_us_nodule_v2 WHERE ((COALESCE(is_aggregate_row, CAST('f' AS BOOLEAN)) = CAST('f' AS BOOLEAN)) AND (exam_date IS NOT NULL)) GROUP BY research_id, exam_date), nodule_2nd AS (SELECT research_id, exam_date, nth_value(size_cm_max, 2) OVER (PARTITION BY research_id, exam_date ORDER BY size_cm_max DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_largest_nodule_cm FROM thyroid_canonical_publication_v1_0.main.canonical_us_nodule_v2 WHERE ((COALESCE(is_aggregate_row, CAST('f' AS BOOLEAN)) = CAST('f' AS BOOLEAN)) AND (exam_date IS NOT NULL)) QUALIFY (row_number() OVER (PARTITION BY research_id, exam_date) = 1)), gland_agg AS (SELECT research_id, exam_date, any_value(us_exam_id) AS us_exam_id_gland, CAST('t' AS BOOLEAN) AS has_gland_findings, bool_or(nlp_backfill_pending) AS any_gland_pending_on_exam FROM thyroid_canonical_publication_v1_0.main.canonical_us_thyroid_gland_v2 WHERE (exam_date IS NOT NULL) GROUP BY research_id, exam_date), ln_agg AS (SELECT research_id, exam_date, any_value(us_exam_id) AS us_exam_id_ln, CAST('t' AS BOOLEAN) AS has_us_ln_findings, count_star() AS n_us_ln_total_on_exam, sum(CASE  WHEN (COALESCE(suspicious_flag, CAST('f' AS BOOLEAN))) THEN (1) ELSE 0 END) AS n_abnormal_us_ln_on_exam, bool_or(nlp_backfill_pending) AS any_us_ln_pending_on_exam FROM thyroid_canonical_publication_v1_0.main.canonical_us_lymph_node_v2 WHERE (exam_date IS NOT NULL) GROUP BY research_id, exam_date), exams AS (((SELECT research_id, exam_date FROM nodule_agg) UNION (SELECT research_id, exam_date FROM gland_agg)) UNION (SELECT research_id, exam_date FROM ln_agg))SELECT exams.research_id, COALESCE(n.us_exam_id_nodule, g.us_exam_id_gland, l.us_exam_id_ln) AS us_exam_id, exams.exam_date, n.n_nodules_on_exam, n.largest_nodule_cm, n2.second_largest_nodule_cm, n.bilateral_flag, n.isthmus_nodule_flag, n.worst_tirads_category_this_exam, n.worst_tirads_points_this_exam, n.best_tirads_category_this_exam, n.count_tr5, n.count_tr4, n.count_tr3, n.count_tr2, n.count_tr1, COALESCE(g.has_gland_findings, CAST('f' AS BOOLEAN)) AS has_gland_findings, COALESCE(l.has_us_ln_findings, CAST('f' AS BOOLEAN)) AS has_us_ln_findings, l.n_us_ln_total_on_exam, l.n_abnormal_us_ln_on_exam, row_number() OVER (PARTITION BY exams.research_id ORDER BY exams.exam_date NULLS LAST) AS exam_rank_for_patient, CASE  WHEN (((cp.first_surgery_date_v2 IS NOT NULL) AND (exams.exam_date <= cp.first_surgery_date_v2))) THEN (CAST('t' AS BOOLEAN)) ELSE CAST('f' AS BOOLEAN) END AS is_preop_exam, (COALESCE(n.any_nodule_pending_on_exam, CAST('f' AS BOOLEAN)) OR COALESCE(g.any_gland_pending_on_exam, CAST('f' AS BOOLEAN)) OR COALESCE(l.any_us_ln_pending_on_exam, CAST('f' AS BOOLEAN))) AS any_nlp_backfill_pending_on_exam FROM exams LEFT JOIN nodule_agg AS n USING (research_id, exam_date) LEFT JOIN nodule_2nd AS n2 USING (research_id, exam_date) LEFT JOIN gland_agg AS g USING (research_id, exam_date) LEFT JOIN ln_agg AS l USING (research_id, exam_date) LEFT JOIN thyroid_canonical_publication_v1_0.main.canonical_patient_master AS cp ON ((cp.research_id = exams.research_id));
```

### Live view body — patient_master
```sql
CREATE VIEW canonical_us_patient_master_VIEW_v2 AS WITH exam_agg AS (SELECT research_id, CAST('t' AS BOOLEAN) AS has_any_us, count_star() AS n_us_exams, min(exam_date) AS first_us_date, max(exam_date) AS last_us_date, bool_or(is_preop_exam) AS preop_us_available_flag, max(worst_tirads_category_this_exam) AS max_tirads_category_ever, max(worst_tirads_points_this_exam) AS max_tirads_points_ever, sum(n_nodules_on_exam) AS n_nodules_total_across_exams, bool_or(bilateral_flag) AS bilateral_disease_flag_ever, (sum(CASE  WHEN ((n_nodules_on_exam >= 2)) THEN (1) ELSE 0 END) > 0) AS multifocal_flag_ever, bool_or(has_us_ln_findings) AS has_us_ln_findings_ever, bool_or(has_gland_findings) AS has_gland_findings_ever, (sum(COALESCE(n_abnormal_us_ln_on_exam, 0)) > 0) AS any_suspicious_us_ln_ever, min(CASE  WHEN (((n_abnormal_us_ln_on_exam IS NOT NULL) AND (n_abnormal_us_ln_on_exam > 0))) THEN (exam_date) ELSE NULL END) AS first_abnormal_us_ln_date, bool_or(any_nlp_backfill_pending_on_exam) AS any_nlp_backfill_pending_for_patient FROM main.canonical_us_exam_master_VIEW_v2 GROUP BY 1), nodule_first_last AS (SELECT e.research_id, any_value(e.worst_tirads_category_this_exam ORDER BY e.exam_date) FILTER (WHERE (e.exam_rank_for_patient = 1)) AS tirads_category_at_first_exam, any_value(e.worst_tirads_category_this_exam ORDER BY e.exam_date DESC) FILTER (WHERE (CAST(e.is_preop_exam AS BOOLEAN) IS NOT DISTINCT FROM CAST('t' AS BOOLEAN))) AS tirads_category_at_last_preop_exam, min(CASE  WHEN ((upper(e.worst_tirads_category_this_exam) IN ('TR4', 'TR5'))) THEN (e.exam_date) ELSE NULL END) AS first_high_risk_tirads_date FROM main.canonical_us_exam_master_VIEW_v2 AS e GROUP BY 1), nodule_agg AS (SELECT research_id, max(COALESCE("nullif"(greatest(COALESCE(length_mm, 0), COALESCE(width_mm, 0), COALESCE(height_mm, 0)), 0), (size_cm_max * 10.0))) AS max_nodule_size_mm, count_star() AS n_nodule_records FROM main.canonical_us_nodule_v2 GROUP BY 1)SELECT e.research_id, e.has_any_us, e.n_us_exams, e.first_us_date, e.last_us_date, e.preop_us_available_flag, e.max_tirads_category_ever, e.max_tirads_points_ever, nfl.tirads_category_at_first_exam, nfl.tirads_category_at_last_preop_exam, e.n_nodules_total_across_exams, e.bilateral_disease_flag_ever, e.multifocal_flag_ever, nfl.first_high_risk_tirads_date, e.has_us_ln_findings_ever, e.any_suspicious_us_ln_ever, e.first_abnormal_us_ln_date, e.has_gland_findings_ever, e.any_nlp_backfill_pending_for_patient, bf.imaging_laterality_rollup_v2, bf.pathology_vs_imaging_laterality_concordant_v2, bf.tumor_pathology_laterality_v2, bf.any_fna_recommended_report_ever, bf.any_fna_recommended_report_source, bf.tirads_worst_rank_ever, bf.tirads_worst_rank_source, na.max_nodule_size_mm, na.n_nodule_records FROM exam_agg AS e LEFT JOIN nodule_first_last AS nfl USING (research_id) LEFT JOIN main.cupm_v2_canonical_backfill_v1 AS bf USING (research_id) LEFT JOIN nodule_agg AS na USING (research_id);
```

## 0D · Dependent views

### `canonical_us_exam_master_VIEW_v2` (1 dependents)
* `main.canonical_us_patient_master_VIEW_v2`

### `canonical_us_patient_master_VIEW_v2` (0 dependents)
* (no dependents)

### `canonical_complications_patient_rollup_v1` (1 dependents)
* `views_readable.complications_patient_rollup_VIEW_v1`

## 0E · Complications events audit

Cells in (source_table × source_kind × source_evidence_type × finding_status × evidence_strength) breakdown: **61**

| source_table | source_kind | source_evidence_type | finding_status | evidence_strength | n_rows | n_patients | n_pt_type |
|---|---|---|---|---|---|---|---|
| `note_entities_complications` | `entity_legacy` | `milky_drain_visual` | `present` | `possible` | 2,988 | 1,576 | 1,576 |
| `complication_phenotype_v1` | `structured` | `milky_drain_visual` | `absent` | `possible` | 1,568 | 1,568 | 1,568 |
| `note_entities_complications` | `entity_legacy` | `nlp_proxy` | `present` | `possible` | 1,087 | 732 | 737 |
| `complication_phenotype_v1` | `structured` | `aspiration_or_clinical_observation` | `absent` | `possible` | 842 | 842 | 842 |
| `complication_phenotype_v1` | `structured` | `nlp_proxy` | `absent` | `possible` | 775 | 750 | 772 |
| `note_entities_complications` | `entity_legacy` | `aspiration_or_clinical_observation` | `present` | `possible` | 711 | 653 | 653 |
| `note_entities_complications` | `entity_legacy` | `aspiration_or_clinical_observation` | `present` | `probable` | 640 | 593 | 593 |
| `note_entities_complications` | `entity_legacy` | `clinical_diagnosis` | `present` | `possible` | 540 | 425 | 425 |
| `complication_phenotype_v1` | `structured` | `clinical_diagnosis` | `absent` | `possible` | 370 | 370 | 370 |
| `note_entities_complications` | `entity_legacy` | `drain_output_or_clinical_observation` | `present` | `possible` | 274 | 141 | 141 |
| `complication_phenotype_v1` | `structured` | `drain_output_or_clinical_observation` | `absent` | `possible` | 200 | 200 | 200 |
| `note_entities_complications` | `entity_legacy` | `drain_output_or_clinical_observation` | `absent` | `possible` | 129 | 93 | 93 |
| `extracted_complications_refined_v5` | `refined_extraction` | `structured_chart` | `present` | `probable` | 56 | 28 | 56 |
| `note_entities_complications` | `entity_legacy` | `nlp_proxy` | `absent` | `possible` | 51 | 47 | 47 |
| `note_entities_complications` | `entity_legacy` | `operative_note` | `present` | `possible` | 40 | 23 | 23 |
| `note_entities_complications` | `entity_legacy` | `milky_drain_visual` | `absent` | `possible` | 35 | 29 | 29 |
| `complication_phenotype_v1` | `structured` | `nlp_proxy` | `present` | `possible` | 34 | 34 | 34 |
| `extracted_rln_injury_refined_v2` | `refined_extraction` | `nlp_proxy` | `present` | `possible` | 34 | 34 | 34 |
| `extracted_complications_refined_v5` | `refined_extraction` | `clinical_diagnosis` | `present` | `possible` | 34 | 34 | 34 |
| `extracted_complications_refined_v5` | `refined_extraction` | `nlp_proxy` | `present` | `possible` | 34 | 34 | 34 |
| `extracted_rln_injury_refined_v2` | `refined_extraction` | `nlp_proxy` | `suspected` | `possible` | 33 | 33 | 33 |
| `extracted_complications_refined_v5` | `refined_extraction` | `nlp_proxy` | `suspected` | `possible` | 33 | 33 | 33 |
| `complication_phenotype_v1` | `structured` | `laryngoscopy_direct` | `present` | `definitive` | 32 | 32 | 32 |
| `extracted_complications_refined_v5` | `refined_extraction` | `clinical_diagnosis` | `suspected` | `possible` | 31 | 31 | 31 |
| `complication_phenotype_v1` | `structured` | `clinical_diagnosis` | `present` | `possible` | 29 | 29 | 29 |
| `complication_phenotype_v1` | `structured` | `structured_chart` | `present` | `probable` | 28 | 28 | 28 |
| `complication_phenotype_v1` | `structured` | `structured_chart` | `present` | `possible` | 28 | 28 | 28 |
| `complication_phenotype_v1` | `structured` | `nlp_proxy` | `indeterminate` | `possible` | 24 | 23 | 23 |
| `complication_phenotype_v1` | `structured` | `milky_drain_visual` | `present` | `possible` | 20 | 20 | 20 |
| `extracted_complications_refined_v5` | `refined_extraction` | `milky_drain_visual` | `present` | `possible` | 20 | 20 | 20 |
| `note_entities_complications` | `entity_legacy` | `intraop_observed` | `present` | `definitive` | 20 | 17 | 17 |
| `extracted_complications_refined_v5` | `refined_extraction` | `chart_documented` | `present` | `probable` | 16 | 16 | 16 |
| `extracted_rln_injury_refined_v2` | `refined_extraction` | `chart_documented` | `present` | `probable` | 16 | 16 | 16 |
| `complication_phenotype_v1` | `structured` | `clinical_diagnosis` | `suspected` | `possible` | 16 | 16 | 16 |
| `complication_phenotype_v1` | `structured` | `chart_documented` | `present` | `probable` | 16 | 16 | 16 |
| `extracted_complications_refined_v5` | `refined_extraction` | `drain_output_or_clinical_observation` | `suspected` | `possible` | 15 | 15 | 15 |
| `complication_phenotype_v1` | `structured` | `drain_output_or_clinical_observation` | `suspected` | `possible` | 15 | 15 | 15 |
| `complication_phenotype_v1` | `structured` | `treatment_initiated` | `absent` | `possible` | 12 | 12 | 12 |
| `extracted_complications_refined_v5` | `refined_extraction` | `drain_output_or_clinical_observation` | `present` | `possible` | 10 | 10 | 10 |
| `note_entities_complications` | `entity_legacy` | `clinical_diagnosis` | `absent` | `possible` | 10 | 8 | 8 |
| `complication_phenotype_v1` | `structured` | `drain_output_or_clinical_observation` | `present` | `possible` | 10 | 10 | 10 |
| `note_entities_complications` | `entity_legacy` | `operative_note` | `present` | `probable` | 6 | 2 | 2 |
| `complication_phenotype_v1` | `structured` | `postop_laryngoscopy` | `present` | `definitive` | 6 | 6 | 6 |
| `canonical_labs_calcium_v1` | `lab_threshold_met` | `lab_threshold_met` | `present` | `definitive` | 6 | 5 | 5 |
| `extracted_rln_injury_refined_v2` | `refined_extraction` | `postop_laryngoscopy` | `present` | `definitive` | 6 | 6 | 6 |
| `extracted_complications_refined_v5` | `refined_extraction` | `postop_laryngoscopy` | `present` | `definitive` | 6 | 6 | 6 |
| `complication_phenotype_v1` | `structured` | `replacement_therapy_only` | `absent` | `possible` | 5 | 5 | 5 |
| `complication_phenotype_v1` | `structured` | `replacement_therapy_only` | `suspected` | `possible` | 5 | 5 | 5 |
| `complication_phenotype_v1` | `structured` | `treatment_initiated` | `present` | `possible` | 5 | 5 | 5 |
| `complication_phenotype_v1` | `structured` | `treatment_initiated` | `suspected` | `possible` | 4 | 4 | 4 |
| `complication_phenotype_v1` | `structured` | `replacement_therapy_only` | `present` | `possible` | 4 | 4 | 4 |
| `complication_phenotype_v1` | `structured` | `aspiration_or_clinical_observation` | `suspected` | `possible` | 4 | 4 | 4 |
| `extracted_complications_refined_v5` | `refined_extraction` | `aspiration_or_clinical_observation` | `suspected` | `possible` | 4 | 4 | 4 |
| `complication_phenotype_v1` | `structured` | `treatment_initiated` | `absent` | `probable` | 3 | 3 | 3 |
| `complication_phenotype_v1` | `structured` | `chart_documented` | `present` | `possible` | 3 | 3 | 3 |
| `extracted_complications_refined_v5` | `refined_extraction` | `chart_documented` | `present` | `possible` | 3 | 3 | 3 |
| `extracted_rln_injury_refined_v2` | `refined_extraction` | `chart_documented` | `present` | `possible` | 3 | 3 | 3 |
| `note_entities_complications` | `entity_legacy` | `aspiration_or_clinical_observation` | `absent` | `possible` | 2 | 2 | 2 |
| `canonical_survival_followup_v1` | `survival_join` | `registry_match` | `present` | `definitive` | 1 | 1 | 1 |
| `note_entities_complications` | `entity_legacy` | `intraop_observed` | `absent` | `definitive` | 1 | 1 | 1 |
| `complication_phenotype_v1` | `structured` | `replacement_therapy_only` | `present` | `probable` | 1 | 1 | 1 |

### Rule-vs-current deltas (by complication_type)

#### Rule A

| complication_type | current any_evidence TRUE | rule A any_evidence TRUE | flip TRUE→FALSE | flip FALSE→TRUE |
|---|---|---|---|---|
| `rln_injury` | 709 | 709 | 0 | 0 |
| `vocal_cord_paralysis` | 107 | 107 | 0 | 0 |
| `hypocalcemia_clinical` | 9 | 9 | 0 | 0 |
| `hypoparathyroidism` | 425 | 425 | 0 | 0 |
| `hematoma` | 169 | 169 | 0 | 0 |
| `seroma` | 873 | 873 | 0 | 0 |
| `chyle_leak` | 1,576 | 1,576 | 0 | 0 |
| `wound_infection` | 0 | 0 | 0 | 0 |
| `pneumothorax` | 0 | 0 | 0 | 0 |
| `airway_complication` | 0 | 0 | 0 | 0 |
| `wound_dehiscence` | 0 | 0 | 0 | 0 |
| `mortality` | 1 | 1 | 0 | 0 |

**Rule A totals:** TRUE→FALSE = 0 · FALSE→TRUE = 0

#### Rule B

| complication_type | current any_evidence TRUE | rule B any_evidence TRUE | flip TRUE→FALSE | flip FALSE→TRUE |
|---|---|---|---|---|
| `rln_injury` | 709 | 74 | 635 | 0 |
| `vocal_cord_paralysis` | 107 | 48 | 59 | 0 |
| `hypocalcemia_clinical` | 9 | 9 | 0 | 0 |
| `hypoparathyroidism` | 425 | 425 | 0 | 0 |
| `hematoma` | 169 | 169 | 0 | 0 |
| `seroma` | 873 | 873 | 0 | 0 |
| `chyle_leak` | 1,576 | 1,576 | 0 | 0 |
| `wound_infection` | 0 | 0 | 0 | 0 |
| `pneumothorax` | 0 | 0 | 0 | 0 |
| `airway_complication` | 0 | 0 | 0 | 0 |
| `wound_dehiscence` | 0 | 0 | 0 | 0 |
| `mortality` | 1 | 1 | 0 | 0 |

**Rule B totals:** TRUE→FALSE = 694 · FALSE→TRUE = 0

#### Rule C

| complication_type | current any_evidence TRUE | rule C any_evidence TRUE | flip TRUE→FALSE | flip FALSE→TRUE |
|---|---|---|---|---|
| `rln_injury` | 709 | 74 | 635 | 0 |
| `vocal_cord_paralysis` | 107 | 48 | 59 | 0 |
| `hypocalcemia_clinical` | 9 | 9 | 0 | 0 |
| `hypoparathyroidism` | 425 | 425 | 0 | 0 |
| `hematoma` | 169 | 169 | 0 | 0 |
| `seroma` | 873 | 873 | 0 | 0 |
| `chyle_leak` | 1,576 | 1,576 | 0 | 0 |
| `wound_infection` | 0 | 0 | 0 | 0 |
| `pneumothorax` | 0 | 0 | 0 | 0 |
| `airway_complication` | 0 | 0 | 0 | 0 |
| `wound_dehiscence` | 0 | 0 | 0 | 0 |
| `mortality` | 1 | 1 | 0 | 0 |

**Rule C totals:** TRUE→FALSE = 694 · FALSE→TRUE = 0

### Audit case — research_id `9340`

#### `rln_injury`
* Rollup `ever_rln_injury_any_evidence` = `True`
| source_kind | source_evidence_type | finding_status | evidence_strength | n |
|---|---|---|---|---|
| `structured` | `nlp_proxy` | `absent` | `possible` | 1 |
| `entity_legacy` | `nlp_proxy` | `present` | `possible` | 1 |

#### `hypoparathyroidism`
* Rollup `ever_hypoparathyroidism_any_evidence` = `True`
| source_kind | source_evidence_type | finding_status | evidence_strength | n |
|---|---|---|---|---|
| `entity_legacy` | `clinical_diagnosis` | `present` | `possible` | 1 |
| `structured` | `clinical_diagnosis` | `absent` | `possible` | 1 |

## Plan-review gate

To apply, write a plan-approval file at `scripts/output/389_plan_approval.txt` containing one of:

```
RULE=A
# or RULE=B  /  RULE=C
INCLUDE_FINDING_STATUS_ABSENT_IN_any_evidence=FALSE
INCLUDE_ENTITY_LEGACY_NLP_PROXY_IN_any_evidence=FALSE
```

Then re-run with `--apply`.

