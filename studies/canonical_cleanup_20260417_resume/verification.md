# Phase 5 verification — replay suite + invariants

_Generated 2026-04-18T05:12:46.091766+00:00_

## CPM invariant

- canonical_patient_master rows: **10871** (expected 10,871)
- canonical_patient_master distinct research_id: **10871** (expected 10,871)

## Drill-down row-count floors

| table | observed | range | result |
|:---|---:|:---|:---|
| `operative_episode_detail_v2` | 9,371 | 9,366..9,376 | OK |
| `complication_phenotype_v1` | 5,978 | 5,928..6,028 | OK |
| `fna_episode_master_v2` | 8,119 | 5,000..30,000 | OK |
| `rai_treatment_episode_v2` | 1,857 | 1..100,000 | OK |
| `synoptic_tumor_long_v1` | 11,103 | 5,000..30,000 | OK |
| `thyroglobulin_lab_canonical_v1` | 74,258 | 73,758..74,758 | OK |
| `longitudinal_lab_canonical_v1` | 75,247 | 73,000..76,000 | OK |

## Replay queries

### Q1_cpm_cardinality

- **Expected:** n=10871, n_distinct=10871
- **Result:** `[{"n": 10871, "n_distinct": 10871}]`
- **OK:** True

### Q2_cpm_built_at_non_null

- **Expected:** n_null = 0
- **Result:** `[{"n_null": 0}]`
- **OK:** True

### Q3_ajcc8_columns_present

- **Expected:** both columns present
- **Result:** `[{"column_name": "ajcc8_t_stage"}, {"column_name": "ajcc8_t_stage_with_microete_t3b_DEPRECATED"}]`
- **OK:** True

### Q4_lateral_nd_columns_present

- **Expected:** both columns present
- **Result:** `[{"column_name": "lateral_neck_dissected"}, {"column_name": "lateral_neck_dissected_structured_or_nlp"}]`
- **OK:** True

### Q5_per_entity_comp_confirmed_cols

- **Expected:** all 6 present
- **Result:** `[{"column_name": "comp_chyle_leak_confirmed"}, {"column_name": "comp_hematoma_confirmed"}, {"column_name": "comp_seroma_confirmed"}, {"column_name": "comp_vc_paralysis_confirmed"}, {"column_name": "comp_vc_paresis_confirmed"}, {"column_name": "comp_wound_infection_confirmed"}]`
- **OK:** True

### Q6_vc_paralysis_recalibration_v236

- **Expected:** n=1
- **Result:** `[{"n": 1}]`
- **OK:** True

### Q7_tg_flag_split

- **Expected:** n_true=60385, n_false=13873, n_null=0
- **Result:** `[{"n_true": 60385, "n_false": 13873, "n_null": 0}]`
- **OK:** True

### Q7b_long_flag_split

- **Expected:** n_true=61374, n_false=13873, n_null=0
- **Result:** `[{"n_true": 61374, "n_false": 13873, "n_null": 0}]`
- **OK:** True

### Q8_tg_cancer_only_view

- **Expected:** n=60385
- **Result:** `[{"n": 60385}]`
- **OK:** True

### Q8b_long_cancer_only_view

- **Expected:** n=61374
- **Result:** `[{"n": 61374}]`
- **OK:** True

### Q9_audit_table_distribution

- **Expected:** LIVE=120 (118 main + 2 manuscript_workspace audit-trail)
- **Result:** `[{"status": "LIVE", "n": 120}]`
- **OK:** True

### Q9b_audit_lineage_v266a

- **Expected:** 1 row with v240 lineage in notes
- **Result:** `[{"object_name": "data_dictionary_v266a", "notes": "replaces data_dictionary_v240 (archived to \"Thyroid 2026 UPdated\".archive_pub_v1_0 by 266c Phase 5 archive sweep 2026-04-18; lineage preserved here)"}]`
- **OK:** True

### Q10_path_tumor_size_invariant_view

- **Expected:** n=80 (held; documented in correction queue)
- **Result:** `[{"n": 80}]`
- **OK:** True

### Q11_correction_queue_scope

- **Expected:** F1=75 TEM-confirmed, F2=5 non-TEM
- **Result:** `[{"subbucket": "F1", "n": 75}, {"subbucket": "F2", "n": 5}]`
- **OK:** True

### Q12_hypopara_queue_status

- **Expected:** indeterminate_requires_chart_review=4
- **Result:** `[{"status": "indeterminate_requires_chart_review", "n": 4}]`
- **OK:** True

### Q13_provenance_ledger

- **Expected:** 6 rows total (1 placeholder + 5 phases)
- **Result:** `[{"run_id": "canonical_cleanup_20260417", "phases_applied": "1.1,1.2,1.3,1.4-verify,1.5a,1.5b,1.6-verify,1.7-verify,1.8,2.1,2.2,2.3,3.1-audit,3.2-doc,4.1,4.2,4.3-verify,4.4,4.5,4.6-pregate-FAILED,5.1-inventory,5.2-classify,5.3-noop,6.1-cpm_built_at,6.2-provenance", "critical_findings_cleared": "PART2-1.1-tirads(verify-only,already-canonical),PART2-2.1-vc-s236(1.1+1.2+1.8),PART2-3.1-rai-dose(1.5),PROMPT18-2.1-vc-crossref(1.1+1.2)", "high_findings_cleared": "PART2-2.1-fna-broadcast(1.7-verify-already-canonical),PART2-2.2-bethesda(4.3-verify),PART2-3.3-tg-counts(1.6-verify-already-canonical),PART`
- **OK:** True

