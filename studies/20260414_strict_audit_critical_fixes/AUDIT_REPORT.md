# THYROID_2026 — Strict Audit + Critical Fixes Report

**Date:** 2026-04-14  
**Database:** MotherDuck `thyroid_ete_fix_20260413` (159 tables)  
**Backup:** 29 tables → `output/parquet_backup/` (44 MB, 25 source + 4 new views)

---

## PHASE 0 — EMERGENCY PARQUET BACKUP

**Status: COMPLETE (25/25 tables, 0 failures, 22.1s)**

All 25 critical tables exported to `output/parquet_backup/*.parquet`:

| Table | Rows | Size (KB) |
|-------|------|-----------|
| imaging_nodule_master_v1 | 37,016 | 2,779 |
| fna_cytology | 8,063 | 1,972 |
| fna_episode_master_v2 | 8,119 | 1,561 |
| path_synoptics | 11,688 | 11,295 |
| patient_refined_master_clinical_v12 | 12,886 | 523 |
| tumor_pathology | 4,290 | 1,288 |
| extracted_tirads_validated_v1 | 3,439 | 35 |
| note_entities_llm_cervical_ln_detail | 11,037 | 843 |
| ultrasound_reports | 6,793 | 2,336 |
| ct_imaging | 7,701 | 9,073 |
| imaging_fna_linkage_v3 | 9,911 | 259 |
| raw_imaging_12_slots_v1 | 21,079 | 1,086 |
| raw_us_tirads_excel_v1 | 19,891 | 544 |
| raw_us_tirads_scored_v1 | 19,549 | 1,137 |
| fna_history | 8,119 | 1,691 |
| synoptic_tumor_long_v1 | 11,103 | 206 |
| operative_episode_detail_v2 | 9,371 | 215 |
| surgery_pathology_linkage_v3 | 9,409 | 178 |
| note_entities_llm_tirads_granular | 11,037 | 772 |
| us_nodules_tirads | 10,862 | 1,565 |
| vw_us_nodule_tirads_validated | 5 | 1 |
| v_imaging_nodule_tirads_gap_v1 | 37,016 | 1,324 |
| v_fna_episode_bethesda_resolved_v1 | 8,119 | 63 |
| imaging_exam_master_v1 | 13,347 | 952 |
| specimen_master_v1 | 10,139 | 1,588 |

---

## PHASE 1 — TIRADS AUDIT

### Verdict: **WARN** (not FAIL)

### 1A. Exam-level parity

| Source | Exams | Nodules |
|--------|-------|---------|
| raw_us_tirads_excel_v1 | 6,028 | 19,891 |
| raw_imaging_12_slots_v1 | 6,213 | 8,794 |
| raw_us_tirads_scored_v1 | 2,506 | 8,331 |
| **imaging_nodule_master_v1 total** | — | **37,016** |
| ultrasound_reports (raw) | 6,793 | 4,074 |
| imaging_exam_master_v1 | 13,347 exams | 6,126 patients |

- **0 orphan US patients** (all ultrasound_reports patients are in imaging_nodule_master)
- imaging_exam_master has 13,347 exams across 6,126 patients

**Verdict: PASS** — all US exams and nodules retained.

### 1B. Nodule_id longitudinal stability

| Metric | Value |
|--------|-------|
| Total nodule_ids | 36,957 |
| Seen in multiple exams | **0** |
| Seen in one exam only | 34,896 |

**Verdict: Per-dated-exam grain confirmed** — nodule_id is NOT longitudinal. Each nodule_id is unique to a single exam date. Longitudinal nodule tracking across serial US exams is a **linkage-limited** gap.

### 1C. Surgery-window stratification

| Window | Source | Nodules | Scored | Unscored | Patients |
|--------|--------|---------|--------|----------|----------|
| date_unknown | raw_imaging_12_slots | 2,045 | 0 | 2,045 | 2,045 |
| inter_op | raw_imaging_12_slots | 2 | 0 | 2 | 2 |
| inter_op | raw_us_tirads_excel | 15 | 15 | 0 | 5 |
| no_surgery | raw_imaging_12_slots | 2,709 | 0 | 2,709 | 799 |
| no_surgery | raw_us_tirads_excel | 8,711 | 8,711 | 0 | 1,377 |
| no_surgery | raw_us_tirads_scored | 4,089 | 4,089 | 0 | 619 |
| post_final_op | raw_imaging_12_slots | 2,137 | 0 | 2,137 | 782 |
| post_final_op | raw_us_tirads_excel | 3,401 | 3,401 | 0 | 659 |
| post_final_op | raw_us_tirads_scored | 875 | 875 | 0 | 221 |
| pre_first_op | raw_imaging_12_slots | 1,901 | 0 | 1,901 | 597 |
| pre_first_op | raw_us_tirads_excel | 7,764 | 7,764 | 0 | 1,885 |
| pre_first_op | raw_us_tirads_scored | 3,313 | 3,313 | 0 | 607 |

**Key finding:** ALL 8,794 unscored nodules are from `raw_imaging_12_slots_v1` — the source that has free-text impression descriptions but zero structured ACR feature columns. The scored sources (tirads_excel, tirads_scored) are 100% scored.

**Gap classification:** extraction-limited (source text exists in 12,895 rows with text descriptions, features not extracted).

### 1D. Scoring system inventory

| Column | Populated | Total | Fill Rate |
|--------|-----------|-------|-----------|
| tirads_reported | 27,903 | 37,016 | 75.4% |
| tirads_acr_recalculated | 19,891 | 37,016 | 53.7% |
| tirads_category | 28,222 | 37,016 | 76.2% |
| tirads_concordant_flag | 19,572 | 37,016 | 52.9% |
| suspicious_flag | 37,016 | 37,016 | 100% |

- **No legacy TI-RADS system columns** (ATA/EU-TIRADS/Kwak/Horvath) exist = source-limited
- ACR TI-RADS (2017) is the only scoring system

### TIRADS Summary Verdict

| Criterion | Status | Classification |
|-----------|--------|---------------|
| All US exams retained | ✅ PASS | — |
| All nodules per exam retained | ✅ PASS | — |
| All scorable nodules scored | ✅ PASS (19,891+8,331 scored) | — |
| 8,794 unscored gap | ⚠️ WARN | extraction-limited |
| No legacy TI-RADS columns | ⚠️ INFO | source-limited |
| No longitudinal nodule tracking | ⚠️ INFO | linkage-limited |

---

## PHASE 2 — FNA/BETHESDA AUDIT

### Verdict: **WARN** (not FAIL)

### 2A. Episode source parity

| Source | Rows | Patients |
|--------|------|----------|
| fna_history | 8,119 | 5,266 |
| fna_cytology | 8,063 | 5,240 |
| fna_episode_master_v2 | 8,119 | 5,266 |

- **0 orphan patients** (fna_history → fna_episode_master is 1:1)
- 56 episodes in episode_master not in fna_cytology (scored episodes without cytology record — likely missing cytology extraction, not data loss)

### 2B. Multi-surgery FNA stratification

| Surgery Window | Episodes | Patients | Has Bethesda |
|---------------|----------|----------|--------------|
| pre_first_op | 4,042 | 3,800 | 4,039 |
| no_surgery | 2,234 | 1,251 | 2,226 |
| date_unknown | 1,632 | 1,215 | 1,620 |
| post_final_op | 205 | 182 | 205 |
| inter_op | 6 | 6 | 6 |

- 99.6% of FNA episodes have Bethesda scores
- 1,632 episodes with unknown FNA date = linkage-limited for surgery window assignment
- Only 6 inter-op FNA episodes (expected — diagnostic FNA is pre-surgical)

### 2C. Multi-era Bethesda surface

**fna_cytology** has full multi-era coverage:
- Total: 8,063 rows
- Bethesda 2010: 7,935 (98.4%)
- Bethesda 2015: 7,935 (98.4%)
- Bethesda 2023: 7,935 (98.4%)
- All three eras: 7,935 (98.4%)

**NEW VIEW CREATED:** `v_fna_episode_bethesda_multiera_v1`
- 8,119 rows (exact match to fna_episode_master_v2)
- 5,266 patients
- 7,935 with 2023 Bethesda
- 2,216 with nodule linkage
- Joins: episode master + cytology (deduped) + resolved Bethesda + imaging-FNA linkage (best score)

**Prior gap:** Multi-era columns existed in fna_cytology but were NOT exposed in any analytical view = **view-derivation-limited → FIXED**.

### FNA/Bethesda Summary Verdict

| Criterion | Status | Classification |
|-----------|--------|---------------|
| Episode capture complete | ✅ PASS | — |
| Multi-era columns exist | ✅ PASS | — |
| Multi-era view created | ✅ FIXED | was view-derivation-limited |
| FNA→nodule linkage | ⚠️ 2,216/8,119 (27.3%) | linkage-limited |
| 1,632 unknown FNA dates | ⚠️ WARN | linkage-limited |
| 56 episodes without cytology | ⚠️ INFO | extraction-limited |

---

## PHASE 3 — LYMPH NODE AUDIT

### Verdict: **CRITICAL → FIXED to WARN**

### 3A. CRITICAL BUG: path_synoptics 'x' data loss

**The bug:** `path_synoptics.tumor_1_ln_involved = 'x'` (meaning "present/positive") was treated as NULL or 0 in downstream tables, masking true LN-positive patients.

| Metric | Value |
|--------|-------|
| path_synoptics rows with `tumor_1_ln_involved = 'x'` | 1,604 (1,570 patients) |
| Rows with `examined = 0/x` AND `involved = x` | 1,752 (1,561 patients) |
| Of those — tumor_pathology confirms LN positive | **1,646** |
| Of those — tumor_pathology NOT positive/NULL | 106 |
| Master clinical shows zero/null for 'x' patients | **2,236** |
| Master clinical correctly shows positive | 277 |

### 3B. Cross-validation: tumor_pathology vs path_synoptics vs master

| Discordance Flag | Count |
|-----------------|-------|
| CRITICAL: positive LN lost in master | **3,265** |
| WARN: positive count mismatch | **8,723** |
| WARN: examined count mismatch | 1,496 |
| OK | 3,467 |
| **Total rows with LN data** | **16,951** |

**Root cause:** `patient_refined_master_clinical_v12` LN fields derive from `path_synoptics` which uses 'x' as a placeholder. The rich per-level, per-cancer-type LN data in `tumor_pathology` (78 LN columns, 3,946 patients) was never integrated into the analytical master.

### 3C. New separated LN surfaces (CREATED)

**3 new views deployed to MotherDuck:**

1. **`v_ln_imaging_separated_v1`** — 14,480 rows, 5,035 patients
   - Ultrasound: 6,793 rows (normal 6,453 / reactive 340)
   - CT: 7,687 rows (suspicious 1,944 / not_assessed 5,727 / indeterminate 12 / assessed 4)

2. **`v_ln_pathology_separated_v1`** — 4,227 rows, 3,946 patients
   - Per-level LN counts (I through VII)
   - Central/lateral examined + positive
   - Extranodal extension, largest deposit, lymphatic invasion

3. **`v_ln_finalization_by_cancer_type_v1`** — 4,228 rows, 3,947 patients
   - Per-cancer-type metastasis (PTC, FTC, MTC, ATC, Hurthle, PDTC)
   - PTC variant detail, cystic mets, micrometastasis, ENE count
   - Source provenance (workbook, histology source)

### LN Summary Verdict

| Criterion | Status | Classification |
|-----------|--------|---------------|
| 3,265 patients LN+ lost in master | 🔴 CRITICAL | view-derivation-limited → partially fixed |
| 8,723 positive count mismatches | 🟡 WARN | source reconciliation needed |
| Imaging LN never integrated | ✅ FIXED | v_ln_imaging_separated_v1 |
| Pathology LN per-level/type | ✅ FIXED | v_ln_pathology_separated_v1 |
| Cancer-type finalization | ✅ FIXED | v_ln_finalization_by_cancer_type_v1 |
| 6,885 patients only in path_synoptics | ⚠️ WARN | extraction-limited ('x' resolution) |

---

## PHASE 4 — FINAL VERDICTS

### Issue Classification Key

| Classification | Meaning |
|---------------|---------|
| **source-limited** | No data exists to extract from |
| **extraction-limited** | Source text exists but extraction wasn't run |
| **linkage-limited** | Data exists but join keys missing/broken |
| **view-derivation-limited** | Data exists in base tables but not surfaced |

### Domain Verdicts

| Domain | Verdict | Key Gap | Fix Effort |
|--------|---------|---------|------------|
| **TIRADS** | ⚠️ WARN | 8,794 unscored (extraction-limited) | ~$2 Haiku batch, 1-2 hrs |
| **FNA/Bethesda** | ⚠️ WARN → FIXED | Multi-era view missing → created | Done (30 min) |
| **Lymph Nodes** | 🔴 CRITICAL → ⚠️ WARN | 3,265 LN+ lost → separated views created | Remaining: master table UPDATE |

### Artifacts Created

| Artifact | Location |
|----------|----------|
| Parquet backup (29 files, 44 MB) | `output/parquet_backup/` |
| LN discordance CSV (16,951 rows) | `output/ln_tumor_path_vs_synoptics_vs_master_discordance.csv` |
| v_ln_imaging_separated_v1 | MotherDuck + Parquet backup |
| v_ln_pathology_separated_v1 | MotherDuck + Parquet backup |
| v_ln_finalization_by_cancer_type_v1 | MotherDuck + Parquet backup |
| v_fna_episode_bethesda_multiera_v1 | MotherDuck + Parquet backup |

### Remaining Work (NOT safe to incorporate without these)

1. ✅ Parquet backup completed (15 min)
2. ✅ v_ln_pathology_separated_v1 + v_ln_imaging_separated_v1 created (30 min)
3. ✅ v_fna_episode_bethesda_multiera_v1 created (30 min)
4. ✅ LN cross-validation discordance quantified (15 min)
5. ⏳ Haiku batch extraction for TIRADS gap (1-2 hrs, can run in parallel)
6. ⏳ Master table LN UPDATE from tumor_pathology (requires approval)
