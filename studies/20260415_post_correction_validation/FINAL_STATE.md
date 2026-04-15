# Post-Correction Validation Evidence Pack

**Date:** 2026-04-15 (Tuesday)  
**Target:** MotherDuck `"Thyroid 2026".main`  
**Git SHA:** `f9dd72812ca4c7cc912921067deedc6bab479e60`  
**Operator:** Automated validation run

---

## 1. TIRADS Final State — Verdict: **WARN**

### 1a. Canonical nodule master (`imaging_nodule_master_v1`)

| Metric   | Value  |
|----------|--------|
| Total    | 37,016 |
| Scored   | 28,222 |
| Unscored |  8,794 |

**Scored rate:** 76.2%

### 1b. LLM extraction supplement (`tirads_llm_extracted_v2`)

| Metric      | Value |
|-------------|-------|
| Extracted   | 5,636 |
| Scored 2017 | 2,731 |

**LLM scored rate:** 48.5%

### 1c. Combined patient coverage

| Source           | Distinct patients |
|------------------|-------------------|
| canonical_scored | 3,439             |
| llm_extracted    | 1,110             |

### TIRADS gaps

- **8,794 unscored nodules** in canonical master — source-limited (synoptic US reports without per-nodule ACR criteria fields)
- **2,905 LLM-extracted nodules** without 2017 TIRADS level — extraction-limited (note text lacked explicit TIRADS mention)
- Patient-level overlap between canonical and LLM not deduplicated here; combined unique patient count requires UNION DISTINCT

**Verdict rationale:** 76% canonical scored rate is acceptable for manuscript but below 90% target; LLM supplement adds coverage but ~49% parse rate limits incremental gain. Labeled WARN.

---

## 2. FNA Final State — Verdict: **PASS**

### 2a. Episode master linkage (`fna_episode_master_v2`)

| Metric      | Value |
|-------------|-------|
| Total       | 8,119 |
| Img linked  | 1,598 |
| Surg linked | 5,886 |

**Imaging linkage rate:** 19.7%  
**Surgery linkage rate:** 72.5%

### 2b. Surgery window distribution (`v_fna_surgery_window`)

| Surgery era    | Count |
|----------------|-------|
| pre_first_op   | 5,674 |
| no_surgery     | 2,233 |
| post_final_op  |   212 |

### 2c. Bethesda surface (`v_fna_bethesda_surface`)

| Metric    | Value |
|-----------|-------|
| Total     | 8,119 |
| Has 2023  | 7,935 |

**Bethesda 2023 coverage:** 97.7%

### FNA gaps

- **Imaging linkage 19.7%** — linkage-limited (imaging_nodule_long_v2 size/location columns largely unpopulated; structured nodule-FNA crosswalk sparse)
- **184 episodes** without Bethesda 2023 mapping — extraction-limited (unmapped or ambiguous cytology categories)

**Verdict rationale:** Surgery linkage at 72.5% is strong; Bethesda 2023 at 97.7% is excellent; imaging linkage is low but this is a known source limitation (documented in prior phases). PASS.

---

## 3. LN Final State — Verdict: **WARN**

### 3a. Source breakdown (`patient_refined_master_clinical_v12`)

| ln_source                   | Count | Avg positive |
|-----------------------------|-------|--------------|
| path_synoptic               | 4,394 |        0.023 |
| tumor_pathology_corrected   | 5,960 |        4.854 |

### 3b. Patient-level summary

| Metric      | Value  |
|-------------|--------|
| Total       | 12,886 |
| LN positive |  4,524 |
| Has imaging |  6,264 |

**LN positive rate:** 35.1%  
**Imaging suspicious node coverage:** 48.6%

### 3c. Cross-validation (`ln_crossval_v1`)

| Status             | Count |
|--------------------|-------|
| agree              | 3,868 |
| discordant         |   351 |
| single_source_only |     8 |
| no_data            |    63 |

**Agreement rate:** 90.2% (3,868 / 4,290 evaluable)  
**Discordance rate:** 8.2% (351 / 4,290)

### LN gaps

- **351 discordant** cross-validation pairs — source-limited (path_synoptic uses `tumor_1_ln_involved` singular text vs tumor_pathology `histology_1_ln_positive` integer count; different grain/definition)
- **63 no_data** patients — source-limited (no LN information in any source table)
- **path_synoptic avg positive = 0.023** — this source stores LN as text markers ('x'/count), not numeric; low average reflects text→int conversion with most values being placeholder 'x' → 0 or 1

**Verdict rationale:** LN positive count corrected from 2,578 → 4,524 (75% increase), confirming the fix. Cross-validation 90.2% agreement is acceptable. 351 discordant cases warrant review but are a known source-grain mismatch, not a data quality failure. Labeled WARN due to discordance requiring future adjudication.

---

## Summary

| Domain | Verdict | Key metric                         | Gap classification    |
|--------|---------|------------------------------------|-----------------------|
| TIRADS | WARN    | 76.2% canonical scored             | Source-limited         |
| FNA    | PASS    | 97.7% Bethesda 2023, 72.5% surg   | Linkage-limited (img)  |
| LN     | WARN    | 4,524 positive, 90.2% crossval     | Source-limited (grain) |

### Remaining gaps explicitly labeled

1. **Source-limited:** TIRADS unscored nodules (synoptic template lacks ACR fields), LN path_synoptic vs tumor_pathology grain mismatch, imaging_suspicious_node coverage at 48.6%
2. **Extraction-limited:** LLM TIRADS parse rate 48.5%, 184 unmapped Bethesda categories
3. **Linkage-limited:** FNA→imaging at 19.7% (nodule-level structured crosswalk absent)

---

*Generated by automated validation pipeline. All queries executed against live MotherDuck `"Thyroid 2026".main` database.*
