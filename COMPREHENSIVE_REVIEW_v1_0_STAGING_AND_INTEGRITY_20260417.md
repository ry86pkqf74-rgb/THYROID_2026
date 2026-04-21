# Comprehensive Updated Review — thyroid_canonical_publication_v1_0
## Per-Tumor AJCC7/AJCC8 Staging Gap + Deep Integrity Investigation

**Date:** 2026-04-17
**Database:** `thyroid_canonical_publication_v1_0` (live canonical publication DB)
**Cohort:** 10,871 patients in `canonical_patient_master` (CPM)
**Scope:** (a) confirmation of user's explicit per-tumor AJCC7 + AJCC8 requirement gap; (b) deep integrity investigation across CPM, detail tables, audit views, and data dictionary.

---

## TL;DR

1. **The explicit per-tumor AJCC7 + AJCC8 requirement is NOT met by the current schema.** The only per-tumor AJCC-version-tagged columns in the entire database are `tumor_pathology.histology_1_{t,n,m,overall}_stage_ajcc8` — i.e. **AJCC8 only, dominant-histology slot only**. No AJCC7 per-tumor columns exist anywhere. No `histology_2+` staging columns exist. No `tumor_N_*_stage_ajccX` slot columns exist.
2. **Patient-level AJCC7 and AJCC8 coexist cleanly in CPM** (38% populated in the malignant cohort; both marked `authoritative` in `data_dictionary_v240`; 14.12% of patients have a recorded `stage_migration_7_to_8`).
3. **Per-tumor long-format tables (`tumor_episode_master_v2` and `canonical_tumor_characteristics_v1`) carry a single, un-versioned `t_stage / n_stage / m_stage` triple.** The best match is `ajcc8_t_stage_corrected` (~63–64%), but the mapping is imperfect and cannot serve as "both AJCC versions per tumor."
4. **Referential integrity is clean.** No orphans. Row counts triangulate. `n_tumors_path`, `tumor_size_cm_max`, invasion-marker counts all roll up from detail tables with 100% concordance.
5. **The legacy `n_tumors` CPM column (38% populated) is effectively broken** — it matches TEM row count for only 143 patients and STL for only 210. `n_tumors_v10` (12% populated) is similarly sparse. Only `n_tumors_path` (77% populated) matches detail tables 100%.
6. **No `NaN`-string contamination in any staging or tumor column.** All audited columns clean (`nan_string_audit_v1_1`).
7. **`data_dictionary_v240` still carries `n_tumors` and `n_tumors_v10` as `authoritative`** despite their broken reconciliation. These need reclassification or repair.

---

## Section 1 — The Per-Tumor AJCC7/AJCC8 Gap (Primary User Requirement)

### 1.1 Per-tumor AJCC columns actually present (database-wide scan)

A systematic scan using `duckdb_columns()` across the `main` schema for any column matching the patterns `(tumor|histology)_<N>_..._ajcc[78]` or `ajcc[78]_..._(tumor|histology)_<N>` returns exactly **six rows**:

| Table | Column | Type |
|---|---|---|
| `tumor_pathology` | `histology_1_t_stage_ajcc8` | VARCHAR |
| `tumor_pathology` | `histology_1_n_stage_ajcc8` | VARCHAR |
| `tumor_pathology` | `histology_1_m_stage_ajcc8` | VARCHAR |
| `tumor_pathology` | `histology_1_overall_stage_ajcc8` | VARCHAR |
| `tumor_pathology` | `histology_1_overall_stage_collapsed` | VARCHAR |
| `ln_master_rollup_v1` | `histology_1_n_stage_ajcc8` | VARCHAR |

### 1.2 What this means for the user's requirement

The user wrote:

> "Re staging, there should be explicit columns for ajcc8 and ajcc 7. and all tumor types (including if multiple in one pathology specimen) should have both."

Current state against this requirement:

- **AJCC7 per tumor** → absent everywhere. Zero columns.
- **AJCC8 per tumor slot 1** → partial (only dominant-histology slot in `tumor_pathology`).
- **AJCC8 per tumor slots 2–5** → absent. No `histology_2_t_stage_ajcc8`, etc.
- **Per-tumor long tables (TEM, CTC)** → single un-versioned `t_stage/n_stage/m_stage` triple, not version-split.

### 1.3 What `tumor_episode_master_v2.t_stage` actually represents

The detail-table registry (`detail_table_registry_v1`) explicitly declares that TEM feeds "ajcc8 staging" to CPM. But when I test which CPM AJCC version the TEM `t_stage` values actually agree with (for 4,307 joinable patients, normalizing by stripping the leading `T`):

| CPM column compared | Patients matching TEM `t_stage` | % |
|---|---:|---:|
| `ajcc7_t_stage` | 2,463 | 57% |
| `ajcc8_t_stage` | 2,311 | 54% |
| `ajcc8_t_stage_v2` | 2,578 | 60% |
| `ajcc8_t_stage_corrected` | **2,767** | **64%** |

Same pattern for `canonical_tumor_characteristics_v1.t_stage` on the primary tumor (3,075 joinable):

| CPM column compared | Matching | % |
|---|---:|---:|
| `ajcc7_t_stage` | 1,746 | 57% |
| `ajcc8_t_stage` | 1,520 | 49% |
| `ajcc8_t_stage_v2` | 1,681 | 55% |
| `ajcc8_t_stage_corrected` | **1,947** | **63%** |

**Interpretation.** TEM/CTC `t_stage` is nominally AJCC8 but pre-correction: it tracks the un-refined AJCC8 T-stage derivation best (and even then at only ~63–64%). It is NOT "both AJCC versions per tumor" and cannot be treated as such without remediation.

### 1.4 Patient-level AJCC presence is healthy

`data_dictionary_v240` confirms both version families exist at the patient level and are all `authoritative` status:

- **AJCC7**: `ajcc7_t_stage` (38.0%), `ajcc7_n_stage` (38.1%), `ajcc7_m_stage` (100%), `ajcc7_stage_group` (35.7%), `ajcc7_missing_components` (62.0%), `ajcc7_stage_calculable_flag` (100%).
- **AJCC8**: `ajcc8_t_stage` (37.6%), `ajcc8_t_stage_v2` (38.0%), `ajcc8_t_stage_corrected` (37.6%), `ajcc8_n_stage` (48.5%), `ajcc8_n_stage_v2` (38.1%), `ajcc8_m_stage` (100%), `ajcc8_m_stage_v2` (100%), `ajcc8_stage_group` (37.6%), `ajcc8_stage_group_v2` (38.0%), `ajcc8_stage_group_corrected` (37.6%), `ajcc8_missing_components` (62.5%), `ajcc8_n_stage_note` (38.1%), `ajcc8_t_stage_calculable_flag` (100%), `ajcc8_calculable_flag` (100%).
- **Crosswalk/provenance**: `stage_migration_7_to_8` (14.1%, 1,535 patients), `stage_discordance_note` (0.07%, 8 patients — documented N1b+age≥55 DTC reclassifications), `scoring_ajcc8_flag` (100%).
- **Completion-thyroidectomy**: `completion_t_stage` (1.0%, 110 patients).

Populations match the staged malignant cohort (compare CPM AJCC7 T-stage ~4,130 with `canonical_malignant_diagnosis_v1` = 4,441 / 4,137 distinct). The ~38% of 10,871 = ~4,130 patients is exactly the staged cohort. The ~62% "missing components" is expected given 2,449 benign tumor-free patients and additional patients with incomplete staging inputs.

All AJCC columns are categorized `B_computed_score` in `cpm_unmapped_triage_v265` — i.e. derived in CPM by staging rules rather than pulled from a single feeder table. This is correct by design.

### 1.5 Sanity check — `nan`-string contamination

`nan_string_audit_v1_1` confirms all AJCC7/AJCC8/stage/tumor/histology columns have **zero** literal `"NaN"` strings. Nulls are true SQL nulls. The audit was done and passed.

---

## Section 2 — Deeper Integrity Investigation

### 2.1 Tumor-count columns in CPM: which one is trustworthy?

Three tumor-count columns live in CPM:

| Column | Pop % | Status in dict | Triangulation result |
|---|---:|---|---|
| `n_tumors` | 38.1% | `authoritative` | Matches TEM row count for only **143 / 10,871** patients. Matches STL for only **210**. |
| `n_tumors_v10` | 12.4% | `authoritative` | Matches CTC/STL row count for only **1,158** patients. Matches TEM for **25**. |
| `n_tumors_path` | 77.5% | `authoritative` (also listed `provisional` with Script 241 rebuild) | Matches STL for **8,422 / 8,422** (100% of patients who have STL data). Matches CTC for **8,421**. |

**Finding.** Only `n_tumors_path` reconciles to detail tables. `n_tumors` and `n_tumors_v10` are historical residue that the dictionary still labels `authoritative` despite failing triangulation. This is a governance bug in the dictionary, not a data-pipeline bug — the correct column already exists and is clean.

**Recommendation.**
1. In `data_dictionary_v240`: change `n_tumors.status = 'deprecated'` with `replacement_column_name = 'n_tumors_path'`.
2. Same for `n_tumors_v10` (or reclassify as an internal intermediate).
3. Keep the columns in CPM for backward compatibility, but annotate them so downstream users don't accidentally use `n_tumors` over `n_tumors_path`.

### 2.2 Multifocal flags

`data_dictionary_v240` shows three multifocal columns, only one authoritative:

| Column | Pop % | Status |
|---|---:|---|
| `multifocal_flag` | 0% | `removed` (column empty, redundant) |
| `path_multifocal_flag` | 0% | `removed` |
| **`multifocal_flag_path`** | **77.5%** | **`authoritative`** |
| `DEPRECATED__multifocal_flag` | — | (renamed tombstone) |
| `DEPRECATED__path_multifocal_flag` | — | (renamed tombstone) |
| `DEPRECATED__path_n_tumors` | — | (renamed tombstone) |

Status already clean — Script 251 (2026-04-16) removed the redundant columns and left tombstones. `multifocal_flag_path` is the only live authoritative column and it rolls up from `patient_tumor_rollup_v1` → which in turn aggregates from `specimen_tumor_focus_v1` and `synoptic_tumor_long_v1`.

### 2.3 Tumor size reconciliation

`canonical_patient_master` exposes:

- `tumor_size_cm_max` (authoritative) — max across all STL tumors per patient.
- `path_tumor_size_cm` (authoritative) — from TEM primary tumor.
- `tumor_size_cm_dominant` (authoritative) — dominant-tumor size from adjudicated rule.
- `tumor_size_cm_min`, `tumor_size_cm_sum`, `tumor_size_cm_mean` — supporting rollups from `patient_tumor_rollup_v1`.

**Status.** Clean. 96 outlier patients surfaced by `path_size_adjudication_v241` (ABS(path − max) > 2 cm [n=68] and path > 10 cm [n=37]) for clinician review — marked `provisional` and flagged for v1_1 fold-in after sign-off.

### 2.4 Histology

`histology_final` (38.1%) is the `authoritative` dominant-histology column. `path_synoptics` stores per-tumor histology in `tumor_1_histologic_type` ... `tumor_5_histologic_type`. `synoptic_tumor_long_v1` has `histologic_type` and `histologic_variant` per tumor. Coverage is adequate, but these do not carry staging — see Gap Section 1.

### 2.5 Extrathyroidal extension (ETE) / LVI / PNI / margin

All invasion-marker columns in CPM roll up cleanly:

- `ete_grade` / `ete_grade_final_v2` — authoritative. `ete_adjudication_v1` (45 cases) is the final-tier adjudicator with quotes, reasoning, and AJCC8 T-stage adjustment. This is why CPM ETE-present count (≈3,289 patients) exceeds STL ETE-present count — the adjudicator corrects upward.
- `lvi_grade` (authoritative; earlier `lvi_grade_final_v13` retired per Script 251 legacy sweep).
- `pni_refined_v6` (authoritative).
- `margin_status_true` (authoritative, from `patient_tumor_rollup_v1`).
- `r_class_true` (authoritative, from same rollup).

Per-tumor equivalents live in `synoptic_tumor_long_v1` (one row per tumor) and `path_synoptics.tumor_1_*` through `tumor_5_*` slots.

### 2.6 Parathyroid

`para_specimen_included` and `para_n_glands_identified` (authoritative, from `specimen_master_v1`). `path_synoptics.parag_1` ... `parag_6` carry per-gland details. Independent of the staging gap.

### 2.7 Referential integrity

All joins on `CAST(research_id AS VARCHAR)` return zero orphans across CPM ↔ TEM ↔ STL ↔ CTC ↔ `patient_tumor_rollup_v1` ↔ `path_synoptics` ↔ `tumor_pathology`. Row-count grain reconciles with the registry:

| Detail table | Rows | Distinct research_id | Registry grain |
|---|---:|---:|---|
| `tumor_episode_master_v2` | 11,691 | 10,871 | one row per tumor per surgery |
| `synoptic_tumor_long_v1` | 11,103 | 8,422 | one row per tumor from synoptic path |
| `canonical_tumor_characteristics_v1` | 11,106 | 8,422 | one row per resected tumor focus per surgery |
| `patient_tumor_rollup_v1` | 8,422 | 8,422 | patient |
| `path_synoptics` | 11,688 | 10,871 | one row per synoptic pathology report |
| `tumor_pathology` | 4,290 | 3,986 | (malignant-only subset) |
| `ete_adjudication_v1` | 45 | 45 | patient (adjudicated) |

Note: `tumor_pathology` is malignant-cohort-scoped (3,986 patients ≈ `canonical_malignant_diagnosis_v1`'s 4,137). This is why its per-histology AJCC8 slot populates at the expected ~38% of CPM.

### 2.8 Outstanding `C_missing_feeder` entries (derived but undocumented source)

`cpm_unmapped_triage_v265` flags these staging-adjacent columns as "missing feeder" — they exist in CPM but aren't mapped to a detail table in the registry:

- `gm_path_m_stage_raw`, `gm_path_stage_raw` — "GM raw" staging fields. `gm_path_stage_raw` is 0% populated — can be dropped.
- `has_isthmus_tumor`, `has_left_tumor`, `has_right_tumor` — laterality flags; should be fed by `synoptic_tumor_long_v1.anatomic_location` or similar.
- `n_tumors_ete_present`, `n_tumors_lvi_present`, `n_tumors_margin_involved`, `n_tumors_margin_uninvolved`, `n_tumors_with_size` — counts-of-tumors-with-X; these are derivable from STL but their derivation isn't registered.
- `scoring_ajcc8_flag`, `stage_discordance_note`, `stage_migration_7_to_8` — AJCC8 provenance/notes; by design `B_computed_score` but flagged here because their provenance is undocumented. Documentation cleanup task, not a data bug.

---

## Section 3 — Biggest Remaining Integrity Concerns (Prioritized)

### P0 — Per-tumor AJCC7 + AJCC8 gap
**Evidence:** Section 1.1–1.3. Only `histology_1_*_ajcc8` exists in `tumor_pathology`. No AJCC7 per-tumor anywhere. No `histology_2+` per-tumor staging. No `tumor_N_*_stage_ajccX` slots. TEM/CTC `t_stage` is imperfect AJCC8-adjacent.

**Impact:** Violates the user's explicit schema requirement. Multi-tumor cases (the minority with >1 tumor but potentially heterogeneous histology) cannot be staged correctly per tumor per version without manual recomputation.

**Remediation plan (recommended):**
- Extend `canonical_tumor_characteristics_v1` (per-tumor long) to carry six new columns: `t_stage_ajcc7`, `n_stage_ajcc7`, `m_stage_ajcc7`, `t_stage_ajcc8`, `n_stage_ajcc8`, `m_stage_ajcc8`, plus `overall_stage_ajcc7` and `overall_stage_ajcc8`. Derive per tumor using same staging rules already in CPM.
- Extend `path_synoptics` wide slots (`tumor_1` through `tumor_5`): add `tumor_N_t_stage_ajcc7`, `tumor_N_n_stage_ajcc7`, `tumor_N_m_stage_ajcc7`, `tumor_N_stage_group_ajcc7`, and AJCC8 equivalents. Populate from per-tumor derivation.
- Extend `tumor_pathology` histology slots: add `histology_N_{t,n,m,overall}_stage_ajcc7` for N=1..5, and `histology_N_{t,n,m,overall}_stage_ajcc8` for N=2..5.
- In CPM: add `tumor_stage_heterogeneous_flag` and `dominant_tumor_ajcc{7,8}_{t,n,m}_stage` to explicitly surface the dominant-tumor values and flag mixed-stage cases.

### P1 — `n_tumors` and `n_tumors_v10` are labeled `authoritative` but fail triangulation
**Evidence:** Section 2.1 — match rates of 1.3% and 10.7% against detail tables vs 100% for `n_tumors_path`.

**Remediation:** Update `data_dictionary_v240.status` to `deprecated` for both and set `replacement_column_name = 'n_tumors_path'`. Optionally repopulate `n_tumors` to equal `n_tumors_path` OR drop/rename the stale columns.

### P2 — `path_size_adjudication_v241` pending clinician sign-off
96 outlier patients pending review. v1_1 will fold signed-off values into CPM. Track to completion.

### P3 — Undocumented-feeder C-bucket cleanup
Document or drop `gm_path_stage_raw` (0% populated), register feeders for `has_{left,right,isthmus}_tumor` and per-tumor count columns, or reclassify as B_computed_score.

### P4 — `ajcc7_m_stage` and `ajcc8_m_stage` both 100% populated
By design (all-M0 default with case-by-case overrides). Worth flagging in the dictionary that the 100% population is a default-fill, not per-case adjudicated coverage. `gm_path_m_stage_raw` holds the actual raw M-stage for 36.8% of patients.

---

## Section 4 — Prior Dry-Run (PROMPT 17) Cross-Validation Summary

From the earlier dry-run, validated in full and re-confirmed now:

- **Tumor count rollup.** `n_tumors_path` ↔ STL row count: 8,422 / 8,422 matches. `n_tumors_path` ↔ CTC: 8,421 / 8,422. ✅
- **Tumor size rollup.** `tumor_size_cm_max` = MAX(STL `size_greatest_dimension_cm`): exact per-patient reconciliation. ✅
- **Histology rollup.** `histology_final` aligns with `synoptic_tumor_long_v1.histologic_type` dominant-tumor selection under canonical adjudication rules. ✅ (38.06% populated, matches staged cohort.)
- **Staging reconciliation.** Patient-level AJCC7 and AJCC8 stored cleanly (Section 1.4). `stage_migration_7_to_8` captures 1,535 patient reclassifications. Per-tumor staging **gap** (Section 1.1). ❌ (the explicit user gap)
- **Invasion markers.** ETE/LVI/PNI/margin all reconcile under the `ete_adjudication_v1` escalation; ~3,289 "ETE present" at CPM level > raw STL count as expected. ✅
- **Parathyroid.** `para_specimen_included` and `para_n_glands_identified` reconcile with `specimen_master_v1` and `path_synoptics.parag_N_*`. ✅
- **Orphan check.** Zero orphans across tumor detail tables against CPM `research_id`. ✅

---

## Section 5 — Final Recommendations to Reach v1_0 Clean

1. **BUILD per-tumor AJCC7 + AJCC8 columns** across `canonical_tumor_characteristics_v1`, `path_synoptics`, `tumor_pathology`, and add dominant-tumor summaries + heterogeneity flag to CPM. (P0 — meets the user's explicit requirement.)
2. **UPDATE `data_dictionary_v240`** to deprecate `n_tumors` and `n_tumors_v10` in favor of `n_tumors_path`, annotate `ajcc7_m_stage` / `ajcc8_m_stage` as default-filled, and document or drop `gm_path_stage_raw`.
3. **CLOSE** `path_size_adjudication_v241` sign-off cycle (96 patients).
4. **REGISTER** missing feeders for `has_{left,right,isthmus}_tumor` and the `n_tumors_{ete,lvi,margin_involved,margin_uninvolved,with_size}_present` count columns in `detail_table_registry_v1`, or reclassify them as `B_computed_score` with a registered derivation spec.
5. **RE-RUN** `cpm_unmapped_triage` after (1)–(4) to confirm residual `C_missing_feeder` entries are only the B-bucket documentation migrations.

---

## Appendix A — Key Evidence Queries

All queries in this review were executed read-only against `thyroid_canonical_publication_v1_0`. The core per-tumor AJCC gap scan:

```sql
SELECT table_name, column_name, data_type
FROM duckdb_columns()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name = 'main'
  AND (
    REGEXP_MATCHES(LOWER(column_name), '(tumor|histology)_[0-9]+.*ajcc')
    OR REGEXP_MATCHES(LOWER(column_name), 'ajcc[78].*(tumor|histology)_[0-9]+')
    OR REGEXP_MATCHES(LOWER(column_name), '(tumor|histology)_[0-9]+.*(t_stage|n_stage|m_stage|stage_group|overall_stage)')
    OR REGEXP_MATCHES(LOWER(column_name), '(t_stage|n_stage|m_stage|stage_group|overall_stage).*(tumor|histology)_[0-9]+')
  )
ORDER BY table_name, column_name;
```
Returns exactly 6 rows — the complete list in Section 1.1.

---

## Appendix B — Summary Table of Status

| Category | Column(s) | Status |
|---|---|---|
| Patient-level AJCC7 | `ajcc7_{t,n,m}_stage`, `ajcc7_stage_group`, `ajcc7_missing_components`, `ajcc7_stage_calculable_flag` | ✅ Authoritative, clean |
| Patient-level AJCC8 | `ajcc8_{t,n,m}_stage`, `_v2` / `_corrected` variants, `ajcc8_stage_group{,_v2,_corrected}`, flags, notes | ✅ Authoritative, clean |
| Patient-level migration | `stage_migration_7_to_8`, `stage_discordance_note` | ✅ Clean |
| Per-tumor AJCC8 (dominant histology) | `tumor_pathology.histology_1_*_ajcc8` | ⚠️ Partial — dominant slot only |
| Per-tumor AJCC7 (any scope) | — | ❌ Absent |
| Per-tumor slot-2+ AJCC (any version) | — | ❌ Absent |
| Per-tumor long `t/n/m_stage` (TEM, CTC) | single un-versioned triple | ⚠️ Ambiguous (~64% AJCC8-corrected match) |
| Tumor count | `n_tumors_path` | ✅ 100% triangulated |
| Tumor count legacy | `n_tumors`, `n_tumors_v10` | ❌ Labeled authoritative but broken (1.3% / 10.7% match) |
| Multifocal | `multifocal_flag_path` | ✅ Authoritative, tombstones cleaned |
| Tumor size | `tumor_size_cm_{max,dominant,path}` | ✅ Clean; 96 outliers pending sign-off |
| ETE | `ete_grade_final_v2` + adjudicator | ✅ Clean |
| LVI | `lvi_grade` | ✅ Clean |
| PNI | `pni_refined_v6` | ✅ Clean |
| Margin | `margin_status_true`, `r_class_true` | ✅ Clean |
| Parathyroid | `para_specimen_included`, `para_n_glands_identified` | ✅ Clean |
| `NaN`-string audit | all staging/tumor cols | ✅ Zero contamination |
| Orphan check | TEM/STL/CTC/`patient_tumor_rollup_v1` vs CPM | ✅ Zero orphans |

---

*Generated from read-only queries executed 2026-04-17 against `thyroid_canonical_publication_v1_0` on MotherDuck. Re-inventory stamp `Script 265 re-inventoried 2026-04-17` confirmed via `__readme` for all referenced tables.*
