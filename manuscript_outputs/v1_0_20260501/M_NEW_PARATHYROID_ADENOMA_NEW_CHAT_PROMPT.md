# Cowork Chat Bootstrap Prompt — Parathyroid Adenoma + Co-existing Thyroid Characteristics Manuscript

**Generated:** 2026-05-04 by Cowork at HEAD `3c186c7`.
**Purpose:** Self-contained prompt to drop into a fresh Cowork chat to start a new manuscript on parathyroid adenoma + co-existing thyroid characteristics. All MD/SF infrastructure already in place; cohort already partially built (M082 view).

---

## 0. Paste-this-first message into new Cowork chat

> I'm Logan Glosser, Emory thyroid surgery researcher. I want to start a new manuscript: **parathyroid adenoma patients — what are their co-existing thyroid characteristics + diagnoses + workup patterns?**
>
> Read `manuscript_outputs/v1_0_20260501/M_NEW_PARATHYROID_ADENOMA_NEW_CHAT_PROMPT.md` end-to-end before any tool use. Auto-memory at `feedback_role_split_writing.md` (Cowork does numbers, separate chats do prose) + `feedback_nlp_refresh_on_snowflake.md` (Snowflake AI for NLP).
>
> **Cohort already exists** as `manuscript_workspace.cohort_m082_parathyroid_tumors_v1` (n=1,399). 404 of those have parathyroid adenoma. Start with the §3 probe queries to confirm cohort + headline numbers, then build out the manuscript pipeline per §4.

---

## 1. Why this manuscript

**Research question:** Among patients undergoing thyroid surgery at Emory who also had parathyroid pathology identified, what are the patterns in:
1. Patient demographics (age, sex, race)
2. Parathyroid tumor characteristics (adenoma vs hyperplasia, gland weight, cellularity, location)
3. Co-existing thyroid characteristics (size/weight, multinodular/single, ultrasound findings)
4. Co-existing thyroid diagnoses (malignant vs benign histology, molecular testing patterns)
5. Imaging workup (US, sestamibi, 4D-CT, MRI)
6. FNA characteristics (Bethesda category, # of nodules biopsied)
7. Lab workup (PTH, calcium, vitamin D, alkaline phosphatase)
8. Surgical management (concurrent thyroidectomy + parathyroidectomy vs separate)
9. Post-op outcomes (hypocalcemia, hypoparathyroidism, recurrence)

**Hypothesis-generating, not pre-specified.** Goal is to surface clinically meaningful patterns that inform pre-op workup or surgical planning for patients with concurrent parathyroid + thyroid pathology.

**Why now:** Parathyroid adenoma is common (1-3% of thyroid surgery cohorts), and the literature on co-existing thyroid pathology is fragmented. Emory's 10,871-patient cohort has 404 adenoma patients — large enough for meaningful subgroup analysis.

---

## 2. Cohort scope (already in place)

| Source | Description | n |
|---|---|---:|
| `manuscript_workspace.cohort_m082_parathyroid_tumors_v1` | Parathyroid tumor cohort (any type) | **1,399** |
| Subset: `para_abnormality_type ILIKE '%adenoma%'` | Adenoma cohort (incl. hyperplasia+adenoma mixed) | **404** |
| `manuscript_workspace.cohort_m042_incidental_parathyroid_v1` | Incidental parathyroid (related; sub-analysis) | 4,798 |
| `main.canonical_parathyroid_events_v1` | Event-level parathyroid encounters | 8,697 |
| `main.canonical_parathyroid_patient_rollup_v1` | Patient-level rollup | 4,443 |

**Recommended primary cohort:** the 404 adenoma patients within M082.

### Headline numbers (Cowork-probed 2026-05-04)

| Variable | Value |
|---|---:|
| n_adenoma | 404 |
| Co-existing thyroid malignancy | 124 (30.7%) |
| Female | 330 (81.7%) |
| Median age | 58 |
| Median PTH | 129.5 ng/L |
| Median calcium | 11.0 mg/dL |
| Median parathyroid gland weight | 1.0 g |

### Para abnormality breakdown

| Type | n | Thyroid malig (n) | % malig |
|---|---:|---:|---:|
| (NULL — no para abnormality) | 717 | 385 | 53.7% |
| **adenoma** | **286** | **90** | **31.5%** |
| hyperplasia | 278 | 99 | 35.6% |
| hyperplasia + adenoma (mixed) | 118 | 34 | 28.8% |

---

## 3. First-message probe queries (Cowork should run these)

### 3.1 Confirm cohort + key columns

```sql
-- Headline cohort
SELECT COUNT(*) AS n_adenoma,
       COUNT_IF(is_malignant) AS n_thyroid_malig,
       ROUND(100.0*COUNT_IF(is_malignant)/COUNT(*), 1) AS pct_malig
FROM manuscript_workspace.cohort_m082_parathyroid_tumors_v1
WHERE LOWER(para_abnormality_type) LIKE '%adenoma%';

-- Cohort schema (34 cols)
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='manuscript_workspace' AND table_name='cohort_m082_parathyroid_tumors_v1'
ORDER BY ordinal_position;
```

### 3.2 Demographics + thyroid co-features

```sql
WITH adenoma AS (
  SELECT * FROM manuscript_workspace.cohort_m082_parathyroid_tumors_v1
  WHERE LOWER(para_abnormality_type) LIKE '%adenoma%'
)
SELECT
  COUNT(*) AS n,
  ROUND(MEDIAN(age_at_surgery), 0) AS age_p50,
  COUNT_IF(LOWER(sex) = 'female') AS n_female,
  COUNT_IF(is_malignant) AS n_malig,
  -- Histology breakdown
  COUNT_IF(histology_final ILIKE '%ptc%' OR histology_final ILIKE '%papillary%') AS n_ptc,
  COUNT_IF(histology_final ILIKE '%ftc%' OR histology_final ILIKE '%follicular%') AS n_ftc,
  COUNT_IF(histology_final ILIKE '%niftp%') AS n_niftp,
  COUNT_IF(histology_final ILIKE '%hashimoto%' OR histology_final ILIKE '%lymphocytic%') AS n_hashimoto,
  COUNT_IF(histology_final ILIKE '%multinodular%' OR histology_final ILIKE '%goiter%') AS n_mng_goiter,
  COUNT_IF(histology_final ILIKE '%adenoma%' AND histology_final NOT ILIKE '%parathyroid%') AS n_thy_adenoma
FROM adenoma;
```

### 3.3 Pull richer covariates from CPM

The cohort view has only 34 cols. Pull demographics, imaging, FNA, molecular from CPM via JOIN:

```sql
SELECT
  -- Imaging
  pm.us_first_exam_date IS NOT NULL AS has_us,
  pm.imaging_tirads_source,
  pm.nlp_tirads_max_category,  -- (note: dropped post-mig_294b; use tirads_resolved)
  pm.tirads_resolved,
  -- FNA
  pm.bethesda_final, pm.bethesda_category,
  pm.fna_bethesda_final,
  -- Molecular
  pm.molecular_tested, pm.braf_positive, pm.ras_positive, pm.tert_positive,
  -- Thyroid characteristics
  pm.gland_weight_final_g,
  pm.tumor_size_cm_max,
  pm.bilateral_disease_flag,
  pm.syn_multinodular_goiter,
  pm.syn_hashimoto, pm.syn_graves,
  -- Outcomes
  pm.any_recurrence_flag, pm.death_occurred,
  pm.followup_years
FROM manuscript_workspace.cohort_m082_parathyroid_tumors_v1 c
JOIN main.canonical_patient_master pm USING (research_id)
WHERE LOWER(c.para_abnormality_type) LIKE '%adenoma%';
```

### 3.4 Surgical management patterns

```sql
WITH adenoma AS (
  SELECT * FROM manuscript_workspace.cohort_m082_parathyroid_tumors_v1
  WHERE LOWER(para_abnormality_type) LIKE '%adenoma%'
)
SELECT
  surg_procedure_type,
  COUNT(*) AS n,
  COUNT_IF(is_malignant) AS n_thyroid_malig,
  ROUND(100.0*COUNT_IF(is_malignant)/COUNT(*), 1) AS pct_malig,
  COUNT_IF(comp_hypocalcemia_confirmed) AS n_hypocal,
  COUNT_IF(comp_hypoparathyroidism_confirmed) AS n_hypopara,
  COUNT_IF(comp_hypoparathyroidism_permanent) AS n_perm_hypopara
FROM adenoma
GROUP BY surg_procedure_type
ORDER BY n DESC;
```

---

## 4. Manuscript structure (proposed)

### Tables (build script: `build_m_new_tables.py`)

| # | Title |
|---|---|
| 1 | Demographics + para tumor characteristics by adenoma type (single / mixed) |
| 2 | Co-existing thyroid pathology (malig vs benign × histology subtype) |
| 3 | Lab workup (PTH/calcium/Vit D) + imaging workup (US/sestamibi/4D-CT) |
| 4 | FNA + molecular testing patterns |
| 5 | Surgical management + complications |
| Supp S1 | Sub-cohort: incidental parathyroid (M042 cohort) |
| Supp S2 | Comparison adenoma vs hyperplasia |
| Supp S3 | Trends over era (2005-2010 / 2010-2015 / 2015-2020 / 2020-2025) |

### Figures

| # | Title |
|---|---|
| 1 | Cohort flow diagram (CONSORT-style) |
| 2 | Histology distribution (pie or bar) — co-existing thyroid pathology |
| 3 | Imaging workup heatmap (US × sestamibi × 4D-CT × MRI) |
| 4 | Forest plot of pre-op factors associated with thyroid malignancy in adenoma cohort |
| 5 | Era × thyroid malig rate within adenoma cohort |

### Sub-analyses (post-hoc, hypothesis-generating)

1. **PTH × thyroid malig** — does higher PTH correlate with thyroid malig? (Hypothesis: hypercalcemia inflammation may co-segregate with thyroid neoplasia)
2. **Gland weight × thyroid pathology** — heavier parathyroid gland = more aggressive thyroid disease?
3. **Concurrent vs separate surgery** — does combined parathyroid + thyroid operation affect complication rates?
4. **Imaging discordance** — how often does sestamibi show parathyroid lesion that thyroid US missed?
5. **Molecular testing yield** — among adenoma + thyroid nodule, what's the mutation rate?

---

## 5. Workflow (per Cowork's existing pattern)

Mirror the M032/M037/M025/M044/M038/M004 workflow:

1. **Cowork (this chat):** probe → render Tables 1-5 + Supp + figures-data → lock numbers → write `M_NEW_READY_FOR_WRITING_BRIEF.md` with locked numbers
2. **Cursor mig (next round):** scaffold `M_NEW_submission_package_v1_0/` with .docx/.xlsx structure + reproducibility SQL + validation report
3. **Logan:** open separate writing chat (Claude or ChatGPT) per `feedback_role_split_writing.md`; feed it the brief + .docx + .xlsx; chat writes prose
4. **Cowork:** run cross-manuscript reconciliation against the 7 existing manuscripts to check internal consistency

---

## 6. Outstanding decisions (Logan)

1. **Final M-number:** M046 / M047 / M083? Cowork's existing manuscript_workspace has M082 cohort already; new manuscript would be M083+ to avoid collision.
2. **Primary cohort:** All 404 adenoma patients OR sub-restrict to single-adenoma (286) excluding the mixed hyperplasia/adenoma (118)?
3. **Primary outcome:** Descriptive only OR include logreg of "thyroid malignancy as outcome" with parathyroid features as predictors?
4. **Era stratification:** Same 5-bucket era as M032 (1999-2004 / 2005-2009 / 2010-2014 / 2015-2019 / 2020-2025) or 3-bucket?
5. **Comparator group:** Just descriptive (no comparator) OR compare adenoma vs hyperplasia OR compare to broader thyroid surgery cohort?

---

## 7. Memory + reference

- **Auto-memory** (auto-loaded by new Cowork chat):
  - `feedback_role_split_writing.md` — Cowork does numbers, separate chats do prose
  - `feedback_nlp_refresh_on_snowflake.md` — SF AI SQL for NLP work
  - `reference_archive_pub_v1_0_location.md` — pre-snapshot location
  - `reference_snowflake_access.md` — SF auth recipe
  - `reference_canonical_naming_convention.md` — canonical table naming

- **Existing infra:**
  - `THYROID_VALIDATION.PUBLIC.VALIDATE_ALL_COHORTS()` SP — can extend to add M-NEW checks
  - `THYROID_VALIDATION.PUBLIC.COHORT_SUMMARY_DASHBOARD` — extend to include M-NEW
  - `manuscript_workspace.cohort_m082_parathyroid_tumors_v1` — primary cohort source

- **7 existing manuscript briefs** for pattern reference:
  - `manuscript_outputs/v1_0_20260501/M044_READY_FOR_WRITING_BRIEF_v1_1.md`
  - `manuscript_outputs/v1_0_20260501/M038_DESCRIPTIVE_READY_FOR_WRITING_BRIEF.md`
  - `manuscript_outputs/v1_0_20260501/M032_READY_FOR_WRITING_BRIEF.md`
  - `manuscript_outputs/v1_0_20260501/M037_READY_FOR_WRITING_BRIEF.md`
  - `manuscript_outputs/v1_0_20260501/M025_READY_FOR_WRITING_BRIEF.md`
  - `manuscript_outputs/v1_0_20260501/M004_READY_FOR_WRITING_BRIEF.md`
  - `manuscript_outputs/v1_0_20260501/M038_DEFINITION_READY_FOR_WRITING_BRIEF.md`

---

## 8. Calendar

- **PAT expires 2026-05-08** (4 days from now)
- **Snowflake trial converts 2026-05-29** (25 days)

Bootstrap window: complete tables + figures-data + brief in next 1-2 sessions; submission package via Cursor mig; writing chat opens after.

---

**End of bootstrap prompt.** Drop this whole file into a fresh Cowork chat and send the §0 message. The chat will pick up from there.
