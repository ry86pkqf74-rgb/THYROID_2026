# PRM v12 — BigQuery-native rebuild scope map (Script 207 `prm.*` columns)

**Date:** 2026-05-14  
**Audience:** ASM207 / `scripts/207_canonical_master_expansion.py` (128 `prm.*` projection columns)  
**Related mirror driver:** `scripts/351_prm_v12_motherduck_to_bq_mirror.py` (MotherDuck publication SSOT → `pub_workspace.patient_refined_master_clinical_v12`, WRITE_TRUNCATE, audited)

## MotherDuck reachability (Task 1)

- **Status:** **REACHABLE** via `scripts/_md_connect.connect_locked()` and repo MotherDuck RW token resolution (`motherduck_client.get_token()`, including `motherduck.local.toml`).
- **Publication catalog probe:** On `thyroid_canonical_publication_v1_0.main`, **`patient_refined_master_clinical_v8` … `v12` were absent** at verification time — the extraction_audit_engine **patient-refined ladder has not been materialized onto publication MotherDuck** in this environment. Legacy attach name `Thyroid 2026` returned “database/share not found” for this account.
- **Mirror implication:** Task 2 hydrate is **blocked until** PRM v12 exists on publication MotherDuck (deploy phases through Phase 13), then run `351_prm_v12_motherduck_to_bq_mirror.py`.

## How PRM v12 is assembled (MotherDuck SSOT mental model)

Chained masters (each extends the prior):

| Step | Output table | Primary builder |
|------|----------------|-----------------|
| Phase 10 | `patient_refined_master_clinical_v9` | `llm_extraction/extraction_audit_engine_v8.py` — `build_master_clinical_v9_sql()` ← `patient_refined_master_clinical_v8` + `extracted_staging_recovery_v1` |
| Phase 11 | `patient_refined_master_clinical_v10` | `llm_extraction/extraction_audit_engine_v9.py` — `build_master_clinical_v10_sql()` ← `v9` + Phase 11 imaging/molecular extracts |
| Phase 12 | `patient_refined_master_clinical_v11` | `llm_extraction/extraction_audit_engine_v10.py` — `_build_master_v11()` ← `v10` + `extracted_tirads_validated_v1` |
| Phase 13 | **`patient_refined_master_clinical_v12`** | `llm_extraction/extraction_audit_engine_v11.py` — `_build_master_v12()` ← `v11` + vascular/IHC/RAS Phase 13 extracts |

Older phases (≤ v8) populate oncology staging, recurrence, completion, voice, labs, RAI, molecular panel, ENE multisource, etc.; **Script 207 does not enumerate those internals** — only the surviving **128 columns** surfaced into `prm.*` joins.

## BigQuery feeding-ground snapshot (leaf mirror posture)

“**In BQ**” below uses **`studies/bq_pub_object_list_snapshot_20260514.json`** `datasets.pub_canonical` object IDs as a **proxy** for objects historically mirrored from MotherDuck/Bulk loads (many workloads also publish under `pub_workspace`; treat mismatches via live `INFORMATION_SCHEMA`). Typical posture:

- **Present (examples):** `path_synoptics`, `clinical_notes_long`, `note_entities_*`, `molecular_test_episode_v2`, `extracted_tirads_validated_v1`, `extracted_braf_recovery_v1`, `extracted_ras_patient_summary_v1`, `extracted_fna_bethesda_v1`, `extracted_postop_labs_expanded_v1`, `rai_treatment_episode_v2`, `operative_episode_detail_v2`, imaging masters, `canonical_*` rollups.
- **Usually MotherDuck-only until mirrored:** ladder intermediates such as `extracted_staging_recovery_v1`, `extracted_vascular_grading_v13`, `extracted_ihc_braf_v13`, `extracted_ras_resolved_v13`, `extracted_ene_multisource_v1`, `extracted_completion_reasons_v1`, many Phase 10 margin/LN/multi-tumor staging recovery tables — **BQ-native port requires either bulk mirror jobs or SQL re-expression against canonical feeds.**

---

## Column-by-column scope (128 Script 207 `prm.*` fields)

Legend:

- **Builder:** earliest master step where the column appears or is last overwritten.
- **Immediate inputs:** tables/views joined in that builder’s SQL (not exhaustive for inherited `SELECT v_prev.*`).
- **BQ-native posture:** qualitative leaf feasibility using snapshot proxy above.

### Block E — TIRADS v12 + nodule size v11

| prm column | Builder | Immediate inputs | BQ-native posture |
|------------|---------|-------------------|-------------------|
| `tirads_best_score_v12` … `tirads_nodule_size_max_mm_v12` (12 cols) | Phase 12 `_build_master_v11` | `patient_refined_master_clinical_v10`, **`extracted_tirads_validated_v1`** | `extracted_tirads_validated_v1` **present** in snapshot; reconcile scoring joins to US/imaging masters |
| `imaging_nodule_size_cm_v11` | Phase 11 `build_master_clinical_v10_sql` | **`extracted_nodule_sizes_v1`** (+ inherits `v9`) | NLP/note-derived sizing — often **MD-intermediate-heavy**; check `note_entities_*` / imaging NLP mirrors |

### Block I — FNA expanded

| prm column | Builder | Immediate inputs | BQ-native posture |
|------------|---------|-------------------|-------------------|
| `n_fna_episodes`, `cross_fna_concordance`, `fna_confidence`, `worst_bethesda_num`, `bethesda_final_name` | Phase 7 master extensions (carried through `v8`→`v12`) | **`extracted_fna_bethesda_v1`**, `fna_episode_master_v2`, cytology bridges | **`extracted_fna_bethesda_v1`** present in snapshot; episode joins mirrored |

### Block J — ENE multisource

| prm column | Builder | Immediate inputs | BQ-native posture |
|------------|---------|-------------------|-------------------|
| `ene_positive` … `ene_record_count_v9` (16 cols) | Phase 9b / Phase 9 overlays on earlier master | **`extracted_ene_multisource_v1`**, path_synoptic NLP arms, imaging modality NLP | **`extracted_ene_multisource_v1` typically MD-only** until mirrored; path/US/PET/RAI arms split across canonical imaging + notes |

### Block L — Molecular expanded v7/v11/v13

| prm column | Builder | Immediate inputs | BQ-native posture |
|------------|---------|-------------------|-------------------|
| `molecular_tested_v7` … `tp53_positive_v7`, fusion flags | Phase 7 `patient_refined_master_clinical_v7` lineage | **`extracted_molecular_panel_v1`**, molecular episodes | Molecular mirrors partial (`molecular_test_episode_v2`, genetics tables present); panel rollup SQL still ladder-specific |
| `braf_positive_v7`, `braf_status_v7`, … `ras_positive_v7` | Phase 7 | Structured molecular + refined staging flags | Same |
| `ras_positive_v11` … `ras_allele_freq_v11`, `braf_recovered_*_v11`, `preop_sweep_genes_found_v11` | Phase 11 `build_master_clinical_v10_sql` | **`extracted_ras_patient_summary_v1`**, **`extracted_braf_recovery_v1`**, **`extracted_preop_sweep_v1`** | **`extracted_ras_patient_summary_v1`**, **`extracted_braf_recovery_v1`** present in snapshot; **`extracted_preop_sweep_v1` MD-only** unless mirrored |
| `ihc_braf_*_v13`, `ras_resolved_*_v13` | Phase 13 `_build_master_v12` | **`extracted_ihc_braf_v13`**, **`extracted_ras_resolved_v13`** | **Typically MD-only** (thin clinical yield) |
| `tert_variant_v9`, `tert_platforms_v9`, `tert_test_count_v9`, `tert_tested` | Phase 9 TERT refinement overlays | Molecular episodes + TERT HGVS parsers | Requires molecular mirror parity |

### Block M — RAI expanded

| prm column | Builder | Immediate inputs | BQ-native posture |
|------------|---------|-------------------|-------------------|
| `confirmed_rai_episodes` … `rai_scan_findings_v9` | Phase 8–9 RAI overlays (`extracted_rai_validated_v1`, `extracted_rai_dose_refined_v1`, scan NLP) | **`rai_treatment_episode_v2`**, **`note_entities_llm_rai_detailed`**, labs/Tg bridges | **`rai_treatment_episode_v2`** mirrored; dose NLP refinements often **MD-intermediate** |

### Block N — Labs (PRM subset only)

| prm column | Builder | Immediate inputs | BQ-native posture |
|------------|---------|-------------------|-------------------|
| `pth_nadir`, `pth_nadir_30d`, `pth_nadir_days_postop`, `calcium_nadir_30d`, `calcium_nadir_days_postop` | Phase 9 lab expansion | **`extracted_postop_labs_expanded_v1`**, clinical_events NLP | **`extracted_postop_labs_expanded_v1`** present in snapshot; join canonical labs rollups as SSOT evolves |

### Block Q — Pathology invasion / margin / lateral / multi-tumor (staging recovery)

| prm column | Builder | Immediate inputs | BQ-native posture |
|------------|---------|-------------------|-------------------|
| `capsular_invasion_refined`, `capsular_invasion_v6` | Phase 6 staging flags (`patient_refined_staging_flags_v3` lineage) | **`synoptic_tumor_long_v1`**, **`path_synoptics`**, CTC bridges | Canonical pathology objects mirrored (`synoptic_tumor_long_v1`, `path_synoptics`) |
| `vascular_who_2022_grade` | Phase 6 invasion profile | Structured synoptic quantify fields | Same |
| `vasc_grade_final_v13`, `vasc_vessel_count_v13`, `vasc_source_final_v13`, `vasc_confidence_final_v13`, `lvi_grade_final_v13` | Phase 13 `_build_master_v12` | **`extracted_vascular_grading_v13`** (+ inherits prior vascular columns) | **`extracted_vascular_grading_v13` MD-only** until mirrored |
| `pni_positive`, `pni_refined_v6` | Phase 6–7 refinements | Path synoptic + NLP | Mixed — mirrors exist for synoptics/notes |
| `margin_r_classification` | Phase 6 margins refined | **`extracted_margins_refined_v1`** | Margin intermediates usually MD ladder |
| `margin_r_class_v10`, `n_tumors_v10`, `max_tumor_size_cm_v10`, `worst_ete_v10`, `total_ln_positive_v10` | Phase 10 `build_master_clinical_v9_sql` | **`extracted_staging_recovery_v1`** (fed by margin/invasion/lateral/multi-tumor Phase 10 tables) | **`extracted_staging_recovery_v1` MD-only** until mirrored |

### Block S — Voice outcomes

| prm column | Builder | Immediate inputs | BQ-native posture |
|------------|---------|-------------------|-------------------|
| `voice_outcome_category`, `has_voice_data`, `voice_followup_completeness`, `voice_data_confidence`, `days_to_first_laryngoscopy`, `days_to_last_laryngoscopy` | Phase 8 voice NLP (`extracted_longterm_outcomes_v1` lineage) | **`note_entities_llm_functional_outcomes`**, complications / laryngoscopy structured feeds | Notes mirrors exist (`note_entities_llm_functional_outcomes` in snapshot); specialized voice rollup SQL still ladder-authored |

### Block T — Lateral neck detail

| prm column | Builder | Immediate inputs | BQ-native posture |
|------------|---------|-------------------|-------------------|
| `lateral_neck_dissected_v10`, `lateral_detection_method`, `lateral_levels_v10`, `lateral_side_v10`, `lateral_source_v10` | Phase 10 lateral neck detector → `extracted_staging_recovery_v1` | **`operative_episode_detail_v2`**, **`note_entities_operative_detail`** | Structured operative mirror present; NLP lateral detector outputs bundled in **`extracted_staging_recovery_v1` (MD-only** typically) |

### Block U — Completion thyroidectomy reasoning

| prm column | Builder | Immediate inputs | BQ-native posture |
|------------|---------|-------------------|-------------------|
| `completion_reason`, `completion_reason_confidence`, `completion_histology_type`, `completion_t_stage`, `completion_prior_histology`, `completion_braf_positive`, `completion_tert_positive` | Phase 8 completion classifier | **`extracted_completion_reasons_v1`** (+ molecular/path overlays) | **`extracted_completion_reasons_v1` MD-only** unless mirrored; upstream diagnosis text from structured path |

---

## Quantitative summary for port planning

| Category | Approx. columns (of 128) | Notes |
|---------|--------------------------|-------|
| Tier A — leaf feeds already mirrored or canonical-adjacent | ~55–65 | TIRADS validated table, molecular subset extracts, FNA Bethesda, post-op labs expanded, heavy use of `path_synoptics` / `synoptic_tumor_long_v1` / `clinical_notes_long` / `rai_treatment_episode_v2` |
| Tier B — depends on MD staging-recovery unions (`extracted_staging_recovery_v1`, multisource ENE, completion reasons, vascular grading v13) | ~35–45 | Needs explicit BQ recreation or bulk mirror jobs |
| Tier C — low-density NLP overlays (voice, IHC BRAF, RAS resolution v13) | ~10–15 | Thin yields but schema-specialized |

## Operational checklist

1. Materialize ladder on **`thyroid_canonical_publication_v1_0`** through **`audit_and_refine_phase13`** (after prerequisites `patient_refined_master_clinical_v11`).
2. Validate **`COUNT(*) = COUNT(DISTINCT research_id) = 10_871`** against canonical spine parity rules used by `_md_connect`.
3. Run **`scripts/351_prm_v12_motherduck_to_bq_mirror.py`** (writes provenance row to **`pub_workspace.md_mirror_load_audit_v1`**).
4. Re-run ASM207 / Script 207 assembly gates against **`pub_workspace.patient_refined_master_clinical_v12`**.


## Appendix — Script 207 `prm.*` columns (128) → phase bucket

| # | `prm` column | Phase bucket |
|---|--------------|--------------|
| 1 | `tirads_best_score_v12` | P12_master_v11 |
| 2 | `tirads_worst_score_v12` | P12_master_v11 |
| 3 | `tirads_best_category_v12` | P12_master_v11 |
| 4 | `tirads_worst_category_v12` | P12_master_v11 |
| 5 | `tirads_source_v12` | P12_master_v11 |
| 6 | `tirads_reliability_v12` | P12_master_v11 |
| 7 | `tirads_n_sources_v12` | P12_master_v11 |
| 8 | `tirads_n_nodule_records_v12` | P12_master_v11 |
| 9 | `tirads_concordant_count_v12` | P12_master_v11 |
| 10 | `tirads_mismatch_count_v12` | P12_master_v11 |
| 11 | `tirads_has_acr_recalc_v12` | P12_master_v11 |
| 12 | `tirads_nodule_size_max_mm_v12` | P12_master_v11 |
| 13 | `imaging_nodule_size_cm_v11` | P11_master_v10 |
| 14 | `n_fna_episodes` | P7_master_chain |
| 15 | `cross_fna_concordance` | P7_master_chain |
| 16 | `fna_confidence` | P7_master_chain |
| 17 | `worst_bethesda_num` | P7_master_chain |
| 18 | `bethesda_final_name` | P7_master_chain |
| 19 | `ene_positive` | P9b_ene_multisource |
| 20 | `best_ene_grade` | OTHER |
| 21 | `ene_grade_v9` | P9b_ene_multisource |
| 22 | `ene_levels_v9` | P9b_ene_multisource |
| 23 | `ene_deposit_cm` | P9b_ene_multisource |
| 24 | `ene_path_synoptic` | P9b_ene_multisource |
| 25 | `ene_path_nlp` | P9b_ene_multisource |
| 26 | `ene_path_levels` | P9b_ene_multisource |
| 27 | `ene_op_intraop` | P9b_ene_multisource |
| 28 | `ene_ct` | P9b_ene_multisource |
| 29 | `ene_us` | P9b_ene_multisource |
| 30 | `ene_pet` | P9b_ene_multisource |
| 31 | `ene_rai_scan` | P9b_ene_multisource |
| 32 | `ene_n_sources` | P9b_ene_multisource |
| 33 | `ene_path_ct_concordance` | P9b_ene_multisource |
| 34 | `ene_record_count_v9` | P9b_ene_multisource |
| 35 | `molecular_tested_v7` | P7_molecular_panel |
| 36 | `high_risk_molecular_v7` | P7_molecular_panel |
| 37 | `n_molecular_tests_v7` | P7_molecular_panel |
| 38 | `molecular_platforms_v7` | P7_molecular_panel |
| 39 | `alk_positive_v7` | P7_molecular_panel |
| 40 | `ret_positive_v7` | P7_molecular_panel |
| 41 | `ntrk_positive_v7` | P7_molecular_panel |
| 42 | `tp53_positive_v7` | P7_molecular_panel |
| 43 | `eif1ax_positive` | P7_molecular_panel |
| 44 | `pax8_pparg_positive` | P7_molecular_panel |
| 45 | `any_fusion_positive` | P7_molecular_panel |
| 46 | `braf_positive_v7` | P7_molecular_panel |
| 47 | `braf_status_v7` | P7_molecular_panel |
| 48 | `tert_positive_v7` | P7_molecular_panel |
| 49 | `tert_status_v7` | P7_molecular_panel |
| 50 | `ras_positive_v7` | P7_molecular_panel |
| 51 | `ras_positive_v11` | P11_master_v10 |
| 52 | `nras_positive_v11` | P11_master_v10 |
| 53 | `hras_positive_v11` | P11_master_v10 |
| 54 | `kras_positive_v11` | P11_master_v10 |
| 55 | `ras_primary_subtype_v11` | P11_master_v10 |
| 56 | `ras_protein_change_v11` | P11_master_v10 |
| 57 | `ras_allele_freq_v11` | P11_master_v10 |
| 58 | `braf_recovered_status_v11` | P11_master_v10 |
| 59 | `braf_recovered_variant_v11` | P11_master_v10 |
| 60 | `braf_detection_method_v11` | P11_master_v10 |
| 61 | `ihc_braf_result_v13` | P13_master_v12 |
| 62 | `ihc_braf_note_type_v13` | P13_master_v12 |
| 63 | `ihc_braf_confidence_v13` | P13_master_v12 |
| 64 | `ras_resolved_gene_v13` | P13_master_v12 |
| 65 | `ras_resolved_variant_v13` | P13_master_v12 |
| 66 | `ras_resolved_af_v13` | P13_master_v12 |
| 67 | `ras_resolution_source_v13` | P13_master_v12 |
| 68 | `ras_resolution_confidence_v13` | P13_master_v12 |
| 69 | `tert_variant_v9` | P9_tert_overlay |
| 70 | `tert_platforms_v9` | P9_tert_overlay |
| 71 | `tert_test_count_v9` | P9_tert_overlay |
| 72 | `tert_tested` | P9_tert_overlay |
| 73 | `preop_sweep_genes_found_v11` | P11_master_v10 |
| 74 | `confirmed_rai_episodes` | P8_P9_RAI |
| 75 | `n_rai_episodes` | P8_P9_RAI |
| 76 | `rai_dose_v9` | P8_P9_RAI |
| 77 | `rai_intent_v9` | P8_P9_RAI |
| 78 | `rai_avidity` | P8_P9_RAI |
| 79 | `rai_avid_flag` | P8_P9_RAI |
| 80 | `rai_validation_tier` | P8_P9_RAI |
| 81 | `rai_dose_source` | P8_P9_RAI |
| 82 | `rai_dose_linkage` | P8_P9_RAI |
| 83 | `max_stimulated_tg` | P8_P9_RAI |
| 84 | `rai_stimulated_tg` | P8_P9_RAI |
| 85 | `rai_stimulated_tsh` | P8_P9_RAI |
| 86 | `post_rai_tg_nadir` | P8_P9_RAI |
| 87 | `post_rai_tg_last` | P8_P9_RAI |
| 88 | `post_rai_tg_count` | P8_P9_RAI |
| 89 | `rai_scan_findings_v9` | P8_P9_RAI |
| 90 | `pth_nadir` | P9_postop_labs |
| 91 | `pth_nadir_30d` | P9_postop_labs |
| 92 | `pth_nadir_days_postop` | P9_postop_labs |
| 93 | `calcium_nadir_30d` | P9_postop_labs |
| 94 | `calcium_nadir_days_postop` | P9_postop_labs |
| 95 | `capsular_invasion_refined` | P6_staging_flags |
| 96 | `capsular_invasion_v6` | P6_staging_flags |
| 97 | `vascular_who_2022_grade` | P6_staging_flags |
| 98 | `vasc_grade_final_v13` | P13_vascular_grade |
| 99 | `vasc_vessel_count_v13` | P13_vascular_grade |
| 100 | `vasc_source_final_v13` | P13_vascular_grade |
| 101 | `vasc_confidence_final_v13` | P13_vascular_grade |
| 102 | `lvi_grade_final_v13` | P13_vascular_grade |
| 103 | `pni_positive` | P6_staging_flags |
| 104 | `pni_refined_v6` | P6_staging_flags |
| 105 | `margin_r_classification` | P6_staging_flags |
| 106 | `margin_r_class_v10` | P10_staging_recovery |
| 107 | `n_tumors_v10` | P10_staging_recovery |
| 108 | `max_tumor_size_cm_v10` | P10_staging_recovery |
| 109 | `worst_ete_v10` | P10_staging_recovery |
| 110 | `total_ln_positive_v10` | P10_staging_recovery |
| 111 | `voice_outcome_category` | P8_voice |
| 112 | `has_voice_data` | P8_voice |
| 113 | `voice_followup_completeness` | P8_voice |
| 114 | `voice_data_confidence` | P8_voice |
| 115 | `days_to_first_laryngoscopy` | P8_voice |
| 116 | `days_to_last_laryngoscopy` | P8_voice |
| 117 | `lateral_neck_dissected_v10` | P10_staging_recovery |
| 118 | `lateral_detection_method` | P10_staging_recovery |
| 119 | `lateral_levels_v10` | P10_staging_recovery |
| 120 | `lateral_side_v10` | P10_staging_recovery |
| 121 | `lateral_source_v10` | P10_staging_recovery |
| 122 | `completion_reason` | P8_completion |
| 123 | `completion_reason_confidence` | P8_completion |
| 124 | `completion_histology_type` | P8_completion |
| 125 | `completion_t_stage` | P8_completion |
| 126 | `completion_prior_histology` | P8_completion |
| 127 | `completion_braf_positive` | P8_completion |
| 128 | `completion_tert_positive` | P8_completion |
