# Operative Pathology Consolidation — Investigation Report
**Database:** `thyroid_canonical_publication_v1_0`
**Date:** 2026-04-21
**Scope:** Consolidate operative pathology section of master canonical table into two event-grain tables — one benign, one malignant — with linkage to research_id, surgery date, and source.

---

## 1. Why the current state is wrong

The canonical DB currently has **two patient-grain diagnosis tables** that are mutually exclusive by design, plus **two event-grain tumor tables** that overlap heavily, plus the **wide-format synoptic** source with every benign flag already populated. The current partition hides clinically real co-occurrence.

### 1.1 Existing tables and their grains

| Table | Rows | Patients | Grain | Has surgery_date? | Has specimen_id? |
|---|---:|---:|---|:-:|:-:|
| `main.canonical_benign_diagnosis_v1` | 6,818 | 6,734 | **patient** | **no** | **no** |
| `main.canonical_malignant_diagnosis_v1` | 4,441 | 4,137 | **patient** | **no** | **no** |
| `main.canonical_diagnosis_unified_v1` | 11,028 | 10,871 | patient | no | no |
| `main.canonical_tumor_characteristics_v1` | 11,106 | 8,422 | per-tumor per-surgery | yes (`surgery_date`) | yes |
| `main.tumor_episode_master_v2` | 11,691 | 10,871 | per-tumor per-surgery | yes | no (has `surgery_episode_id`) |
| `main.synoptic_tumor_long_v1` | 11,103 | 8,422 | per-tumor per-synoptic | yes (`surg_date`) | no |
| `main.path_synoptics` | 11,688 | 10,871 | **per synoptic report** | yes (`surg_date`) | via specimen_master |
| `main.specimen_master_v1` | 10,139 | 8,422 | per specimen | via `procedure_date_day` | yes (`specimen_id`) |
| `main.operative_episode_detail_v2` | 11,773 | 10,871 | per surgery | yes | no |

### 1.2 Three concrete problems

1. **Mutually exclusive patient partition.** 6,734 patients in `canonical_benign_diagnosis_v1` and 4,137 patients in `canonical_malignant_diagnosis_v1` — **zero overlap**. The archive comment on `rai_benign_histology_recovery_v234` explicitly states *"canonical_benign/malignant_diagnosis_v1 partition patients correctly"*, so the design intent was exclusivity.

2. **That intent is clinically wrong.** 1,804 of 11,688 synoptic reports (15.4%) contain **both** a malignant `tumor_1_histologic_type` AND a benign co-finding (MNG, Hashimoto's, follicular adenoma, etc.). Papillary thyroid carcinoma in a background of MNG + chronic lymphocytic thyroiditis is the modal case, and every one of those malignant patients is currently **missing their benign background** in the diagnosis tables. Only `path_synoptics` (raw) has it all together.

3. **No event grain on benign or malignant diagnosis.** The diagnosis canonicals key on `research_id` only. A patient with 2 surgeries gets 1 row with an arbitrary primary diagnosis. 845 patients have ≥2 operative episodes and 763 have ≥2 synoptic reports — the current tables collapse those.

### 1.3 Two event-grain tumor tables overlap

`canonical_tumor_characteristics_v1` (50 cols, 11,106 rows, 8,422 pts) and `tumor_episode_master_v2` (37 cols, 11,691 rows, 10,871 pts) share the same PK `(research_id, surgery_episode_id, tumor_ordinal)` and the same 25-ish pathology columns (histology, variant, size, invasion, margin, nodes, laterality, multifocality). The meaningful difference:

- `canonical_tumor_characteristics_v1` is richer (AJCC7 + AJCC8 full staging, data_completeness_pct, synoptic_row_ix, specimen_id).
- `tumor_episode_master_v2` adds discordance flags (`histology_discordance_flag`, `t_stage_discordance_flag`, `consult_precedence_flag`) and `procedure_raw`, and covers all 10,871 patients (with 7,240 null-histology placeholder rows for benign-only patients).

These should be one table.

### 1.4 `path_synoptics` already holds the full benign vocabulary

The raw wide-format synoptic (311 columns, 11,688 rows, 10,871 patients) has one row per pathology report and already has every benign flag populated:

| Condition | Populated (of 11,688 reports) |
|---|---:|
| multinodular_goiter | 6,208 (53%) |
| adenomatoid_nodules | 1,177 |
| lymphocytic_thyroiditis | 1,067 |
| chronic_lymphocytic_thyroiditis | 976 |
| follicular_adenoma | 924 |
| graves | 588 |
| colloid_nodule | 380 |
| hurthle_cell_oncocytic_adenoma | 268 |
| hashimoto_thyroiditis | 251 |
| thyroglossal_duct_cyst | 229 |
| adenomatous_hyperplasia | 205 |
| follicular_hyperplasia | 164 |
| cystic_degeneration | 144 |
| path_diagnosis_summary (free-text) | 11,686 |

The consolidation is mostly **materializing these into a thin event-grain table** with proper linkage keys and coded values, not re-extracting data.

---

## 2. Proposed target state

### 2.1 Two new canonical event-grain tables

#### `main.canonical_path_malignant_events_v1`
- **Grain:** one row per malignant tumor focus per surgery
- **PK:** `(research_id, surgery_episode_id, tumor_ordinal)`
- **Filter:** only rows where a malignant tumor is documented (primary_histology IS NOT NULL and resolved to a malignant bucket, or is_malignant = TRUE)
- **Linkage columns:** `research_id`, `surgery_episode_id`, `surgery_date`, `path_surgery_id`, `specimen_id`, `synoptic_row_ix`, `accession_or_source_id`, `source_tables`
- **Body (carried forward from `canonical_tumor_characteristics_v1`):** `primary_histology`, `histology_variant`, `histology_source`, `histology_full_descriptor`, `laterality`, `site`, `size_greatest_dimension_cm`, `extrathyroidal_extension`, `gross_ete`, `lymphatic_invasion`, `vascular_invasion`, `angioinvasion_quantify`, `perineural_invasion`, `capsular_invasion`, `margin_status`, `ln_examined`, `ln_involved`, `nodal_disease_positive_count`, `nodal_disease_total_count`, `extranodal_extension`, `number_of_tumors`, `multifocality_flag`
- **Staging:** full AJCC7 + AJCC8 block (T/N/M/overall/stage_group/calculable_flag/migration) already in CTC_v1
- **Carry from TEM_v2:** `histology_discordance_flag`, `t_stage_discordance_flag`, `consult_precedence_flag`, `procedure_raw`
- **Drop:** the `*_deprecated_un_versioned_20260417` staging columns (already marked deprecated per Script 266c)
- **Source of build:** UNION/merge of `canonical_tumor_characteristics_v1` + `tumor_episode_master_v2`, resolving overlap with `canonical_tumor_characteristics_v1` taking precedence (it's richer), filling from TEM_v2 for discordance + procedure_raw
- **Expected count:** ~6,700 rows across ~4,137 patients (only malignant-tumor-bearing)

#### `main.canonical_path_benign_events_v1` — **NEW**
- **Grain:** one row per synoptic pathology report (= one row per surgery in the path section)
- **PK:** `(research_id, surgery_episode_id)` with `synoptic_row_ix` as alternate
- **Includes all 10,871 patients / ~11,688 synoptic reports** — malignant patients retain their benign background findings on the same surgery's row
- **Linkage columns:** `research_id`, `surgery_episode_id`, `surgery_date`, `synoptic_row_ix`, `specimen_id`, `accession_or_source_id`, `source_table`
- **Body (BOOLEAN/tri-state `has_*` flags coded from `path_synoptics` strings):**
  - Structural / nodular: `has_multinodular_goiter`, `has_substernal_mng`, `has_colloid_nodule`, `has_colloid_cyst`, `has_adenomatoid_nodules`, `has_hyperplastic_nodules`, `has_follicular_nodule`, `has_hurthle_cell_nodule`, `has_cystic_degeneration`
  - Benign neoplasms: `has_follicular_adenoma`, `has_hurthle_cell_oncocytic_adenoma`, `has_hyalinizing_trabecular_adenoma`, `has_atypical_adenoma`
  - Hyperplasia: `has_adenomatous_hyperplasia`, `has_follicular_hyperplasia`, `has_papillary_hyperplasia`, `has_c_cell_hyperplasia`
  - Autoimmune / inflammatory: `has_hashimoto`, `has_chronic_lymphocytic_thyroiditis`, `has_lymphocytic_thyroiditis`, `has_palpation_thyroiditis`, `has_dequervain_thyroiditis`, `has_autoimmune_thyroiditis`, `has_riedels_thyroiditis`, `has_chronic_thyroiditis`, `has_chronic_inflammation`
  - Diffuse disease: `has_graves`
  - Ectopic / other: `has_thymic_tissue`, `has_thyroglossal_duct_cyst`, `has_hurthle_cell_change`, `has_hurthle_cell_metaplasia`, `has_intrathyroidal_parathyroid` (from existing benign canonical)
- **Raw text backing fields:** `path_diagnosis_summary_raw`, `synoptic_diagnosis_raw`, `microscopic_description_raw`, `other_findings_raw` — for audit / NLP fallback
- **Rollup flag:** `has_any_benign_finding`, `benign_finding_count`
- **Source of build:** `path_synoptics` (primary) + `note_entities_llm_pathology` (fill where structured synoptic absent) + `note_entities_llm_synoptic_pathology_enrichment` (fill gaps)
- **Expected count:** ~11,688 rows / 10,871 patients

### 2.2 Patient-grain rollups (matches existing `canonical_*_patient_rollup_v1` pattern from FNA/frozen-section)

- **`main.canonical_path_malignant_patient_rollup_v1`** — one row per malignant patient with dominant tumor, worst stage, earliest malignant surgery_date, n_surgeries_with_malignancy. Replaces `canonical_malignant_diagnosis_v1`.
- **`main.canonical_path_benign_patient_rollup_v1`** — one row per patient with any-surgery roll-up (`has_ever_mng`, `has_ever_hashimoto`, etc.), first/last surgery date. Replaces `canonical_benign_diagnosis_v1` — **now covers all 10,871 patients** including those with concomitant malignancy.

### 2.3 Readable VIEWs (schema `views_readable`, per existing convention)

- `Pathology_Malignant_Events` → `main.canonical_path_malignant_events_v1`
- `Pathology_Benign_Events` → `main.canonical_path_benign_events_v1`
- `Pathology_Malignant_Patient` → `main.canonical_path_malignant_patient_rollup_v1`
- `Pathology_Benign_Patient` → `main.canonical_path_benign_patient_rollup_v1`

### 2.4 Deprecate / archive (per standard `archive_pub_v1_0` pattern with timestamp suffix)

| Table | Replaced by | Reason |
|---|---|---|
| `main.canonical_benign_diagnosis_v1` | `canonical_path_benign_patient_rollup_v1` + `canonical_path_benign_events_v1` | Patient-grain only, no dates, hides concomitant-with-malignant cases |
| `main.canonical_malignant_diagnosis_v1` | `canonical_path_malignant_patient_rollup_v1` + `canonical_path_malignant_events_v1` | Patient-grain only, no surgery linkage, thin (12 cols) |
| `main.canonical_diagnosis_unified_v1` | VIEW that UNIONs the two patient rollups | Redundant; can be a view |
| `main.tumor_episode_master_v2` | Merged into `canonical_path_malignant_events_v1` | 80% duplicate of `canonical_tumor_characteristics_v1` |
| `main.synoptic_tumor_long_v1` | `canonical_path_malignant_events_v1` | Same grain, less rich (22 cols) |
| `main.canonical_tumor_characteristics_v1` | **RENAME in-place to** `canonical_path_malignant_events_v1` | Rename only, not drop; body becomes the new canonical |

### 2.5 Keep as-is (upstream raw or adjacent domain)

- `main.path_synoptics` — raw source of truth (**do not modify**)
- `main.specimen_master_v1`, `main.specimen_tumor_focus_v1`, `main.specimen_source_xref_v1` — linkage
- `main.canonical_frozen_section_events_v1` + `_patient_rollup_v1` — own Tier-2 domain (Script 360)
- `main.path_outcome_classification_v1` — FNA→path concordance (own purpose)
- `main.ete_adjudication_v1`, `main.tumor_stage_heterogeneity_v1` — adjudication / derived
- `main.patient_completion_oed_path_linkage_v1` — cross-domain linkage
- `main.thyroid_sizes`, `main.thyroid_weights` — non-diagnosis gland measurements (could be folded into benign_events if you want)
- `main.specimen_genomic_assay_v1` — molecular domain
- `main.note_entities_llm_pathology`, `main.note_entities_llm_synoptic_pathology_enrichment` — raw NLP
- `main.operative_episode_detail_v2` — surgery spine

---

## 3. Linkage guarantees under the new design

Every row in **both** new event tables carries:
- `research_id` — patient identity
- `surgery_episode_id` — the surgery event (from `operative_episode_detail_v2`)
- `surgery_date` — resolved date
- `specimen_id` and/or `synoptic_row_ix` — specimen identity
- `accession_or_source_id` — pathology accession number (from `specimen_master_v1`)
- `source_table` / `source_tables` — provenance
- `source_script` — build script + git sha

Patients with ≥2 surgeries get ≥2 rows per table (one per surgery). Patients with both benign and malignant findings on the **same** surgery appear on one row in each table — keyed on the same `surgery_episode_id`. Joining the two tables on `(research_id, surgery_episode_id)` reassembles the full per-surgery picture.

---

## 4. Known gotchas to resolve in the build script

1. **Linkage completeness.** Only 7,816 of 10,139 specimens (77%) have BOTH `surgery_episode_id` AND `synoptic_row_ix` populated in `specimen_master_v1`. The benign_events build must handle the 23% with one-sided linkage (probably fall back to `(research_id, surg_date)` cluster matching).

2. **Type coercion.** `path_synoptics.research_id` is VARCHAR; `specimen_master_v1.research_id` is BIGINT; `canonical_tumor_characteristics_v1.research_id` is INTEGER. Consolidation script must cast uniformly (standardize on INTEGER — the v1_0 publication convention).

3. **Thyroiditis vocabulary collapse.** `lymphocytic_thyroiditis` (1,067) / `chronic_lymphocytic_thyroiditis` (976) / `hashimoto_thyroiditis` (251) often describe the same disease with different clinical shorthand. Decide: keep as separate flags (preserve clinician intent) or collapse to a single `has_hashimoto_or_cld_thyroiditis`. My recommendation: keep separate flags AND add a unified `has_any_chronic_thyroiditis` rollup flag.

4. **Raw-source rule.** Per the existing `path_synoptics` comments on deprecated columns, raw-source columns (names with `tumor_N_*`) are **not renamed** even when deprecated. The new tables can drop the deprecated staging columns, but must not touch `path_synoptics`.

5. **Registry update.** `manuscript_workspace.detail_table_registry_v1` must be updated to register the two new canonical tables, update `feeds_master_columns` for each, and mark the deprecated tables. This matches the Script 281 / Script 347 close-out pattern.

6. **CPM column feeders.** The existing CPM columns fed by `canonical_benign_diagnosis_v1` and `canonical_malignant_diagnosis_v1` (e.g., `has_mng`, `has_hashimoto`, `histology_base_canonical`, `is_malignant`) must be repointed to the new patient rollups. This is a `canonical_patient_master` UPDATE, not a schema change.

---

## 5. Expected outcome

- **From:** 7 patient-grain and event-grain tables touching op-path diagnosis/tumor, non-overlapping benign/malignant partition, patients with both diagnoses losing the co-occurring findings.
- **To:** 2 event-grain canonicals + 2 patient-grain rollups + 4 readable views. Full linkage (`research_id`, `surgery_episode_id`, `surgery_date`, `specimen_id`, `accession`, `source_table`) on every event row. Patients with ≥2 surgeries get one row per surgery per table. Patients with benign + malignant in the same surgery appear on one row in each table, joinable on `surgery_episode_id`.

---

## 6. Questions for sign-off before I write the Cursor prompt

1. **Benign event grain**: one row per synoptic report (wide boolean flags, ~30 cols), or long-format one row per benign finding? I'd go **wide** — easier to query, matches `path_synoptics` shape, 30 cols is fine.

2. **Thyroiditis collapse**: keep `has_hashimoto`, `has_chronic_lymphocytic_thyroiditis`, `has_lymphocytic_thyroiditis` as three separate flags plus a unified `has_any_chronic_thyroiditis` rollup — or collapse to one?

3. **Parathyroid glands 1-6 data** (42 cols in `path_synoptics`): leave alone in `path_synoptics` (my recommendation) or fold into benign_events? Your existing parathyroid domain is `note_entities_llm_parathyroid_detail` / Script 284 — I'd leave it there.

4. **Gland size / weight data** (`thyroid_sizes`, `thyroid_weights`): leave as separate small tables (my recommendation) or absorb into benign_events? Current tables are per-patient and already feeding CPM.

5. **Rename vs new build**: take `canonical_tumor_characteristics_v1` and rename to `canonical_path_malignant_events_v1` with ALTER TABLE (fast, preserves history), or rebuild from scratch? Rename is faster; rebuild is cleaner but costs a few more archive snapshots. I'd rename.

6. **Include AJCC staging in the new tables**: yes, keep AJCC7 + AJCC8 block in malignant_events (already there in `canonical_tumor_characteristics_v1`) — confirming.

Confirm yes/no or edits on these and I'll write the Cursor prompt.
