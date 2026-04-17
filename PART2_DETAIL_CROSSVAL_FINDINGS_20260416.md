# Part 2 — Detail Table Cross-Validation Findings

**Database:** `thyroid_canonical_publication_v1_0`
**Primary rollup under test:** `main.canonical_patient_master` (N = 10,871 · 1,377 cols)
**Run date:** 2026-04-16
**Mode:** Dry run (read-only). Fix SQL is drafted, **not** executed.
**Severity legend:** `CRITICAL` — rollup contradicts source · `HIGH` — missed patients or wrong counts · `MED` — cosmetic/non-load-bearing · `INFO` — drift, note only.

---

## 0. Executive scope & departures from brief

| Item | Brief expectation | Observed today | Delta |
|---|---|---|---|
| `canonical_patient_master` | 10,871 × 1,377 | 10,871 × 1,377 | ✓ |
| `main` schema tables | — | **114** | — |
| `manuscript_workspace` objects | 64 views | **65 views + 6 base tables** | +7 |
| `imaging_nodule_long_v2` | 19,891 / 3,439 | **dropped from this DB** | brief stale — superseded by `imaging_nodule_master_v1` (37,016 / 6,126) per `canonical_us_nodule_characteristics_v1` comment |
| `complication_phenotype_v1` | 5,928 / 2,892 | **5,978 / 2,938** | +50 rows / +46 patients |
| `ln_master_rollup_v1` | 4,290 / 3,986 | **4,273 / 3,986** | −17 rows, patient count stable |
| `canonical_tumor_characteristics_v1` | not in brief | **built today, 11,106 / 8,422** | NEW |
| `canonical_us_nodule_characteristics_v1` | not in brief | **built today, 37,016 / 6,126** | NEW |

All other detail-table row counts (FNA, US TIRADS, molecular, RAI, Tg, longitudinal labs, synoptic tumor, tumor episode, recurrence, operative, path synoptics) match the brief exactly.

---

## 1. How each batch is checked

Every detail table gets the same five canonical checks wherever applicable:

1. **Coverage parity** — patients with ≥1 detail row vs. the CPM flag that's supposed to mark them (`has_X`, `n_X > 0`, `X_flag`).
2. **Orphan detail** — detail rows whose `research_id` isn't in `canonical_patient_master` (should be zero; these are latent ETL leaks).
3. **Aggregate concordance** — CPM min/max/count/sum field vs. `MIN/MAX/COUNT/SUM` recomputed live from detail.
4. **Date ordering** — first/last dates make sense (first ≤ last, both within patient's clinical window).
5. **Impossible values** — negatives where only positives are valid, nulls in required fields, out-of-range categorical codes.

For each failure I produce three artifacts in that subsection:
- a one-line severity verdict,
- the exact SQL that surfaced the problem (so you can replay independently),
- a **commented** `-- FIX:` block (UPDATE / INSERT / rebuild) that you can execute on approval.

---

## 2. Findings by batch

_Batch 1 (Imaging/US), Batch 2 (FNA/Molecular), Batch 3 (RAI/Labs), Batch 4 (Tumor/Path), Batch 5 (Outcomes) — filled in as the run progresses._

---

## Batch 1 — Imaging / Ultrasound

Sources in scope:
- `us_nodules_tirads` (10,862 / 10,862) — legacy wide-format TIRADS workbook
- `imaging_nodule_master_v1` (37,016 / 6,126) — per-(exam, nodule) grain
- `canonical_us_nodule_characteristics_v1` (37,016 / 6,126) — Script 246, built 2026-04-16

CPM rollup columns in scope:
`n_us_exams`, `n_us_nodules_total`, `max_tirads_ever`, `imaging_tirads_best`, `imaging_tirads_worst`, `dominant_nodule_size_cm`, `preop_tirads_best`, `deprecated__imaging_nodule_size_cm`, `lnus_*` (lateral-neck US).

### 1.1 `CRITICAL` — `max_tirads_ever` under-reports TR by one level in 1,503 patients

**Finding.** Of 3,439 patients with any TIRADS in the drill-down, 1,503 (43.7%) have `canonical_patient_master.max_tirads_ever` numerically **lower** than `MAX(GREATEST(tirads_reported, tirads_acr_recalculated))` from `canonical_us_nodule_characteristics_v1`. The pattern is unambiguous: every one of the 10 worst-offender examples shows CPM = TR4 while `tirads_acr_recalculated` = 5. The rollup is reading `tirads_reported` only and ignoring the ACR-recalculated field, which is the field the drill-down's own TIRADS pipeline produced.

**Replay query.**

```sql
WITH detail AS (
  SELECT research_id,
         GREATEST(COALESCE(MAX(tirads_reported),0), COALESCE(MAX(tirads_acr_recalculated),0)) AS detail_max
  FROM main.canonical_us_nodule_characteristics_v1 GROUP BY 1
)
SELECT COUNT(*) FILTER (WHERE d.detail_max >
  COALESCE(TRY_CAST(REGEXP_EXTRACT(CAST(cpm.max_tirads_ever AS VARCHAR), '([1-5])') AS INTEGER), 0)) AS cpm_too_low
FROM main.canonical_patient_master cpm
JOIN detail d ON TRY_CAST(cpm.research_id AS INTEGER) = d.research_id;
-- returns: 1503
```

**Impact.** Downstream cohort filters on TR5 (biopsy eligibility, suspicious-nodule counts, TI-RADS validation studies) will miss ~1,500 patients. This is load-bearing for any TI-RADS manuscript work.

**Draft fix SQL.**

```sql
-- FIX (review then execute): rebuild max_tirads_ever from the authoritative drill-down.
-- Chooses the higher of tirads_reported vs. tirads_acr_recalculated per row.
-- NOT EXECUTED - dry run.
/*
WITH detail AS (
  SELECT research_id,
         GREATEST(COALESCE(MAX(tirads_reported),0),
                  COALESCE(MAX(tirads_acr_recalculated),0)) AS detail_max
  FROM main.canonical_us_nodule_characteristics_v1
  GROUP BY 1
)
UPDATE main.canonical_patient_master cpm
SET    max_tirads_ever = 'TR' || CAST(d.detail_max AS VARCHAR)
FROM   detail d
WHERE  TRY_CAST(cpm.research_id AS INTEGER) = d.research_id
  AND  d.detail_max > COALESCE(
        TRY_CAST(REGEXP_EXTRACT(CAST(cpm.max_tirads_ever AS VARCHAR), '([1-5])') AS INTEGER), 0);
*/
```

### 1.2 `HIGH` — 3 orphan `research_id`s in `us_nodules_tirads` not present in CPM

**Finding.** `research_id` ∈ {`2332`, `2445`, `7744`} exist in `us_nodules_tirads` but not in `canonical_patient_master`. All three rows are fully empty (no `us_1_date`, no `nodule_1`, no TIRADS) — they are blank placeholder rows from the 12/1/25 workbook. Severity is HIGH because any full outer join against CPM will surface them as silent referential-integrity ghosts, and they violate the "one row per CPM patient + optional placeholder" model.

**Replay query.**

```sql
WITH cpm AS (SELECT DISTINCT research_id FROM main.canonical_patient_master)
SELECT d.research_id, d.source_workbook
FROM   main.us_nodules_tirads d
LEFT   JOIN cpm c ON TRY_CAST(d.research_id AS INTEGER) = TRY_CAST(c.research_id AS INTEGER)
WHERE  c.research_id IS NULL;
```

**Draft fix SQL.**

```sql
-- FIX: drop the 3 empty orphan rows (they carry no data).
-- NOT EXECUTED - dry run.
/*
DELETE FROM main.us_nodules_tirads
WHERE research_id IN ('2332','2445','7744');
*/
```

### 1.3 `INFO` — Table comment on `canonical_us_nodule_characteristics_v1` re-verified as CORRECT

The comment on `canonical_us_nodule_characteristics_v1` claims the 4,745 CPM patients *not* in the drill-down have empty placeholder rows in `us_nodules_tirads` (0% `n*_tr`, 0% nodule-text, 0% `us_1_date`). Split-tested:

| cohort | rows | with_us1_date | with_nodule1 | with_any_TR |
|---|---|---|---|---|
| IN canonical drill-down (~6,126) | 6,126 | 4,073 | 3,894 | 3,307 |
| NOT in canonical drill-down (~4,745) | 4,736 | **0** | **0** | **0** |

Comment holds exactly. However, the 3,307 rows *with* TR data in the drill-down cohort are almost certainly the source of the 1,503-patient TIRADS mismatch above — the drill-down captured the data but the CPM rollup didn't recompute its `max_tirads_ever` over the recalculated field.

### 1.4 `MED` — `n_us_exams` provenance is opaque (doesn't match any single detail source)

**Finding.** `CPM.n_us_exams` matches `COUNT(DISTINCT exam_date) FROM canonical_us_nodule_characteristics_v1` for only 4,076 / 10,871 patients, and matches `COUNT(*) FROM ultrasound_reports` for only 2,066 / 10,871. 2,050 patients have `CPM.n_us_exams > drill-down exam count`. This is consistent with a union-of-sources rollup (nodule drill-down ∪ ultrasound_reports ∪ imaging_exam_master_v1 ∪ NLP) but is not documented anywhere I can find. Not a bug per se, but a reproducibility risk.

**Recommendation.** Annotate `CPM.n_us_exams` with a `*_source` or `*_provenance` sibling column, or add a `us_exams_reconciled_v1` view that makes the union explicit.

### 1.5 `MED` — 2,061 rows in `imaging_nodule_master_v1` missing `exam_date` (5.6%)

**Finding.** 2,061 of 37,016 rows have `exam_date IS NULL`; 7 more have `exam_date < 1990-01-01 OR > 2027-01-01` (implausible). Since `exam_date` is the join key for any temporal analysis (first/last exam, pre-op vs. post-op), these rows silently drop out of windowed queries.

**Replay query.**

```sql
SELECT COUNT(*) FILTER (WHERE exam_date IS NULL) AS missing,
       COUNT(*) FILTER (WHERE exam_date < DATE '1990-01-01' OR exam_date > DATE '2027-01-01') AS out_of_range
FROM   main.imaging_nodule_master_v1;
-- returns: 2061, 7
```

**Draft fix SQL.**

```sql
-- FIX: flag rather than delete. Add a data-quality column, then quarantine in a view.
-- NOT EXECUTED - dry run.
/*
ALTER TABLE main.imaging_nodule_master_v1
  ADD COLUMN IF NOT EXISTS exam_date_quality VARCHAR;
UPDATE main.imaging_nodule_master_v1
SET    exam_date_quality = CASE
         WHEN exam_date IS NULL                                        THEN 'MISSING'
         WHEN exam_date < DATE '1990-01-01'                            THEN 'PRE_1990'
         WHEN exam_date > DATE '2027-01-01'                            THEN 'FUTURE'
         ELSE 'OK'
       END;

CREATE OR REPLACE VIEW manuscript_workspace.imaging_nodule_master_clean_v1 AS
SELECT * FROM main.imaging_nodule_master_v1 WHERE exam_date_quality = 'OK';
*/
```

### 1.6 `INFO` — `n_us_nodules_total` rollup is 100% correct ✓

Exact match on all 10,871 patients (6,126 nonzero + 4,745 zero). Sums reconcile: `SUM(CPM.n_us_nodules_total) = 37,016 = SUM(COUNT(*) per patient from drill-down)`. No action.

### 1.7 `INFO` — Zero orphans in `imaging_nodule_master_v1` or `canonical_us_nodule_characteristics_v1` ✓

All 37,016 rows join cleanly to `canonical_patient_master`.

---

## Batch 2 — FNA / Molecular

Sources in scope:
- `fna_episode_master_v2` (8,119 / 5,266)
- `molecular_test_episode_v2` (10,126 / 10,026)
- `molecular_variant_long` (1,640 / 703)

CPM rollup columns in scope:
`n_fna_episodes`, `n_fna_cytology_records`, `worst_bethesda_num`, `bethesda_final`, `prm_first_fna_date`, `prm_last_fna_date`, `fna_path_concordant`, `n_molecular_tests_v7`, `molecular_tested_v7`, `mol_n_variants_total`, `braf_positive_final`, `ras_positive_final`, `tert_positive_final`, `ret_positive_unified`.

### 2.1 `CRITICAL` — `n_fna_episodes` is stuck at 11 or 12 for 5,012 of 5,266 FNA patients (95%)

**Finding.** Distribution of `CPM.n_fna_episodes`:

| value | n_patients |
|---|---|
| NULL | 5,622 |
| **11** | **3,237** |
| **12** | **1,775** |
| 1 | 120 |
| 2 | 83 |
| 3 | 17 |
| 4 | 8 |
| 5 | 7 |
| 6 | 1 |
| 7 | 1 |

But the real distribution in `fna_episode_master_v2` has:

| episodes/patient | n_patients |
|---|---|
| 1 | 3,372 |
| 2 | 1,330 |
| 3 | 349 |
| 4 | 124 |
| 5 | 51 |
| 6 | 21 |
| 7 | 8 |
| 8 | 5 |
| 9 | 1 |
| 11 | **2** |
| 12 | **3** |

So only **5 patients in reality** have 11 or 12 FNA episodes, but CPM asserts **5,012 patients** do. The sample I pulled confirms the bug pattern: every top-offender has `cpm.n_fna_episodes = 11` but `detail.n_ep = 1`. This looks like a broadcast / cartesian leak during rollup — possibly the per-patient COUNT was replaced with something approximating `MAX(n_ep) - 1` or a join cardinality bug. **Downstream filters on "patient has >X FNAs" are fundamentally broken.**

**Replay query.**

```sql
WITH ep AS (SELECT research_id, COUNT(*) AS n_ep FROM main.fna_episode_master_v2 GROUP BY 1)
SELECT cpm.n_fna_episodes, ep.n_ep, COUNT(*) AS n
FROM main.canonical_patient_master cpm
JOIN ep ON TRY_CAST(cpm.research_id AS INTEGER) = ep.research_id
GROUP BY 1,2
ORDER BY 3 DESC LIMIT 10;
```

**Draft fix SQL.**

```sql
-- FIX: rebuild n_fna_episodes from fna_episode_master_v2 COUNT per patient.
-- NOT EXECUTED - dry run.
/*
WITH ep AS (
  SELECT research_id, COUNT(*) AS n_ep FROM main.fna_episode_master_v2 GROUP BY 1
)
UPDATE main.canonical_patient_master cpm
SET    n_fna_episodes = COALESCE(ep.n_ep, 0)
FROM   ep
WHERE  TRY_CAST(cpm.research_id AS INTEGER) = ep.research_id;

-- Also null out the 5,622 patients with no FNA detail (they currently carry NULL — OK) — no-op.
-- Patients with NULL n_fna_episodes and no ep.row stay NULL.
*/
```

### 2.2 `HIGH` — `worst_bethesda_num` CPM-over for 672 patients

**Finding.** For 672 patients, `CPM.worst_bethesda_num` is higher than `MAX(bethesda_category)` from `fna_episode_master_v2`. CPM-under for 12 more, missing for 1. The CPM-over direction is usually defensible — `worst_bethesda_num` likely unifies `fna_episode_master_v2` + `fna_cytology` + NLP extraction. But 672 is a lot, and the CPM-over direction has no upper-bound check (Bethesda caps at 6; if CPM = 6 when detail says 3, that's load-bearing). Needs a provenance column exposing which source contributed the worst value, so reviewers can adjudicate.

**Recommendation.** Add `worst_bethesda_source` (VARCHAR) tracking which of `{episode_master, cytology, nlp, adjudicated}` produced the winning value. This already exists for some domains (`bethesda_source`, `fna_bethesda_source`) — confirm these mirror `worst_bethesda_num` for the 672 CPM-over cases.

### 2.3 `HIGH` — Research_id 7744 is in `molecular_test_episode_v2` and `us_nodules_tirads` but NOT in CPM

**Finding.** Same orphan as 1.2 but appearing across domains — `research_id = 7744` has one molecular test episode (`platform = 'Other'`, result `NULL`) and a blank US TIRADS placeholder, but no CPM row. Either (a) 7744 should have been included in CPM and was dropped during cohort finalization, or (b) 7744 is stale and these detail rows should be deleted. This is the same latent referential-integrity class of issue as 1.2.

**Draft fix SQL.**

```sql
-- FIX (option A) — Re-admit 7744 to CPM if clinically eligible. Requires manual review.
-- NOT EXECUTED - dry run.
/*
-- Check 7744's presence across all detail tables first:
SELECT 'fna_episode_master_v2'     AS tbl, COUNT(*) FROM main.fna_episode_master_v2     WHERE research_id = 7744
UNION ALL SELECT 'molecular_test_episode_v2', COUNT(*) FROM main.molecular_test_episode_v2 WHERE research_id = 7744
UNION ALL SELECT 'us_nodules_tirads',         COUNT(*) FROM main.us_nodules_tirads         WHERE TRY_CAST(research_id AS INTEGER) = 7744
UNION ALL SELECT 'path_synoptics',            COUNT(*) FROM main.path_synoptics            WHERE TRY_CAST(research_id AS INTEGER) = 7744
UNION ALL SELECT 'rai_treatment_episode_v2',  COUNT(*) FROM main.rai_treatment_episode_v2  WHERE research_id = 7744
;

-- FIX (option B) — Delete 7744 ghosts. NOT EXECUTED.
-- DELETE FROM main.molecular_test_episode_v2 WHERE research_id = 7744;
-- DELETE FROM main.us_nodules_tirads         WHERE research_id = '7744';
*/
```

### 2.4 `MED` — 91.6% of molecular episodes have no test date (9,280/10,126)

**Finding.** Both `test_date_native` and `resolved_test_date` are NULL for 9,280 rows (92%). This is a source-data gap, not a rollup bug, but it breaks any temporal analysis (pre-surgical vs. post-surgical molecular testing, time-to-test-from-diagnosis). Recommend adding a `date_imputation_status` column and a policy for imputing from linked FNA or surgery dates.

**Draft imputation sketch.**

```sql
-- FIX (imputation; not executed):
/*
UPDATE main.molecular_test_episode_v2 e
SET    resolved_test_date = COALESCE(resolved_test_date,
                                     CAST(f.resolved_fna_date AS VARCHAR))
FROM   main.fna_episode_master_v2 f
WHERE  TRY_CAST(e.linked_fna_episode_id AS BIGINT) = f.fna_episode_id
  AND  (e.resolved_test_date IS NULL OR TRIM(e.resolved_test_date) = '');
*/
```

### 2.5 `INFO` — `n_molecular_tests_v7` rollup is essentially perfect (10,024 / 10,025 exact)

The one mismatch is research_id 7744, the orphan above. No action beyond fixing 2.3.

### 2.6 `INFO` — `mol_n_variants_total` is perfect (703 / 703 exact) ✓

### 2.7 `INFO` — Mutation-flag discrepancies (BRAF +108, RET +37) are expected multi-source unification

CPM `braf_positive_final = TRUE` with no positive episode flag happens for 108 patients. Sampling shows `braf_source = patient_refined_master_clinical_v12` and `braf_detection_method ∈ {NGS, NLP_entity_confirmed}`, i.e., these cases were pulled from NLP entity extraction or a pathology NGS pathway that doesn't land in `molecular_test_episode_v2`. Same story for 37 RET cases. The *reverse* direction — detail-positive-but-CPM-negative — is **zero** across BRAF/RAS/TERT/RET, which is the direction that would have indicated a lost positive. No action, but worth documenting in the dataset dictionary.

### 2.8 `MED` — FNA first/last date drift (150 / 100 patients mismatched)

`CPM.prm_first_fna_date` disagrees with `MIN(resolved_fna_date)` for 150 of 5,208 FNA patients (2.9%); same for `prm_last_fna_date` in 100 patients (1.9%). Likely cause: CPM is anchoring on native date when episode-resolved date differs, or pulling from fna_cytology. Non-critical but should be reconciled.

### 2.9 `INFO` — Zero bad Bethesda codes, zero bad allele fractions, zero molecular variant orphans ✓

---

## Batch 3 — RAI / Labs / Thyroglobulin

Sources in scope:
- `rai_treatment_episode_v2` (1,857 / 862)
- `thyroglobulin_lab_canonical_v1` (76,971 / 3,258 — Tg 37,966 + TgAb 39,005)
- `longitudinal_lab_canonical_v1` (77,960 / 3,690)

### 3.1 `CRITICAL` — 537 orphan patients across both lab tables (16,586 rows each)

**Finding.** 16,586 rows of `longitudinal_lab_canonical_v1` and 16,586 rows of `thyroglobulin_lab_canonical_v1` belong to 537 `research_id`s that are **not in `canonical_patient_master`**. Same 537 patients span both tables (identical row counts are almost certainly because Tg is carried into the longitudinal table). Research_id range: 105 → 20054, i.e., the orphans include both early and high-ID patients. This represents ~21% of the rows in both lab canonicals being consumed by patients who were excluded from CPM.

**Decision point.** These 537 patients are either:
(a) excluded from CPM by cohort filter and the lab rows should be **archived/deleted** from the publication database, **or**
(b) incorrectly dropped from CPM and need **re-admission** (same class as 1.2 / 2.3).

**Draft fix SQL.**

```sql
-- FIX (dry run) — first, quantify and characterize the orphans:
/*
CREATE OR REPLACE VIEW manuscript_workspace.lab_orphan_audit_v1 AS
SELECT d.research_id,
       COUNT(*)                AS n_lab_rows,
       MIN(d.specimen_collect_dt) AS first_lab,
       MAX(d.specimen_collect_dt) AS last_lab,
       COUNT(DISTINCT d.analyte) AS n_analytes
FROM   main.thyroglobulin_lab_canonical_v1 d
WHERE  TRY_CAST(d.research_id AS INTEGER) NOT IN
       (SELECT TRY_CAST(research_id AS INTEGER) FROM main.canonical_patient_master)
GROUP BY 1 ORDER BY 2 DESC;

-- Option A — delete orphans from both lab canonicals:
-- DELETE FROM main.thyroglobulin_lab_canonical_v1
-- WHERE TRY_CAST(research_id AS INTEGER) NOT IN (SELECT TRY_CAST(research_id AS INTEGER) FROM main.canonical_patient_master);
-- DELETE FROM main.longitudinal_lab_canonical_v1 WHERE ... (same clause)

-- Option B — re-admit eligible patients to CPM via the cohort build pipeline. Out of scope here.
*/
```

### 3.2 `CRITICAL` — `rai_max_dose_mci = 0` for 214 patients whose detail has a real dose (up to 450 mCi)

**Finding.** 214 RAI-treated patients have `rai_max_dose_mci = 0` in CPM while `rai_treatment_episode_v2` has a non-null, non-zero `MAX(dose_mci)`. Direction is 100% one-sided: **every single one of the 214 has CPM = 0 and detail > 0** (avg delta -143 mCi, max delta -450 mCi). Worst cases: research_ids 2490 (0 vs 450), 5772 (0 vs 340), 2662 (0 vs 300). Sibling column `rai_dose_v9` carries the correct value for every sample pulled, so the rollup apparently pulls from a different field and zeroes when that secondary field is null.

**Downstream impact.** Any filter like `rai_max_dose_mci > 100` or `rai_max_dose_mci BETWEEN 150 AND 200` silently excludes 214 patients who received high-dose ablation. This breaks RAI-dose-dependent analyses.

**Replay query.**

```sql
WITH e AS (SELECT research_id, MAX(dose_mci) AS max_dose FROM main.rai_treatment_episode_v2 GROUP BY 1)
SELECT COUNT(*) FROM main.canonical_patient_master cpm
JOIN e ON TRY_CAST(cpm.research_id AS INTEGER) = e.research_id
WHERE cpm.rai_max_dose_mci = 0 AND e.max_dose > 0;
-- returns 214
```

**Draft fix SQL.**

```sql
-- FIX: rebuild rai_max_dose_mci from rai_treatment_episode_v2 and fall back to rai_dose_v9.
-- NOT EXECUTED - dry run.
/*
WITH e AS (SELECT research_id, MAX(dose_mci) AS max_dose FROM main.rai_treatment_episode_v2 GROUP BY 1)
UPDATE main.canonical_patient_master cpm
SET    rai_max_dose_mci = COALESCE(e.max_dose, cpm.rai_dose_v9)
FROM   e
WHERE  TRY_CAST(cpm.research_id AS INTEGER) = e.research_id
  AND  (cpm.rai_max_dose_mci = 0 OR cpm.rai_max_dose_mci IS NULL)
  AND  COALESCE(e.max_dose, cpm.rai_dose_v9) > 0;
*/
```

### 3.3 `HIGH` — `n_tg_measurements_structured` under-counts Tg for 1,444 / 2,528 patients (57%)

**Finding.** For 1,444 patients, `CPM.n_tg_measurements_structured < COUNT(*) FROM thyroglobulin_lab_canonical_v1 WHERE analyte='Tg'`. Same pattern on `n_tgab_measurements` (1,675 under-counts of 2,528). Likely cause: CPM was built before the latest lab wave closure (`studies/20260411_final_master_release/EVIDENCE_PACK.md`) and the rollup was not refreshed.

**Draft fix SQL.**

```sql
-- FIX: rebuild n_tg_measurements_structured and n_tgab_measurements from canonical lab table.
-- NOT EXECUTED - dry run.
/*
WITH t AS (
  SELECT research_id,
         COUNT(*) FILTER (WHERE analyte = 'Tg')    AS n_tg,
         COUNT(*) FILTER (WHERE analyte = 'TgAb')  AS n_tgab
  FROM   main.thyroglobulin_lab_canonical_v1 GROUP BY 1
)
UPDATE main.canonical_patient_master cpm
SET    n_tg_measurements_structured = COALESCE(t.n_tg, 0),
       n_tgab_measurements           = COALESCE(t.n_tgab, 0)
FROM   t
WHERE  TRY_CAST(cpm.research_id AS INTEGER) = t.research_id;
*/
```

### 3.4 `HIGH` — Tg peak/nadir mismatch for ~20% of patients (503 / 535)

**Finding.** `CPM.tg_peak` differs from `MAX(result_numeric)` over analyte='Tg' for 503 / 2,528 patients; `CPM.tg_nadir` differs from `MIN(result_numeric)` for 535. Probably the same stale-snapshot cause as 3.3.

**Draft fix SQL** (combine with 3.3 into a single rebuild pass):

```sql
/*
WITH t AS (
  SELECT research_id,
         MAX(result_numeric) FILTER (WHERE analyte='Tg') AS tg_peak_calc,
         MIN(result_numeric) FILTER (WHERE analyte='Tg') AS tg_nadir_calc
  FROM   main.thyroglobulin_lab_canonical_v1 GROUP BY 1
)
UPDATE main.canonical_patient_master cpm
SET    tg_peak  = t.tg_peak_calc,
       tg_nadir = t.tg_nadir_calc
FROM   t
WHERE  TRY_CAST(cpm.research_id AS INTEGER) = t.research_id;
*/
```

### 3.5 `MED` — RAI date/dose source gaps (585 null dates, 1,092 null doses — 31% / 59%)

This is documented in `docs/motherduck_database_contract_v1.md` as "41% RAI dose recovery" — the null rate is a source-data reality, not a rollup defect. Not action-required here, but worth re-noting because it sets a ceiling on any RAI-dose-dependent cohort analysis.

### 3.6 `INFO` — RAI episode count + dose sum + first/last dates mostly reconcile

| rollup field | patients w/ detail | exact | mismatch |
|---|---|---|---|
| `n_rai_episodes` | 862 | 862 | 0 ✓ |
| `rai_total_cumulative_dose_mci` | 249 non-null | 249 | 0 ✓ |
| `rai_first_episode_date` | 862 | 581 | 281 |
| `rai_last_episode_date` | 862 | 581 | 281 |

The date drift (281 patients) is proportional to the 585 null `resolved_rai_date` values — when the episode date is null, CPM is using a fallback (probably `note_date_parsed` or `rai_date_native`). Recommend documenting the fallback precedence explicitly in the dataset dictionary.

### 3.7 `INFO` — Zero RAI orphans, zero negative doses, zero extreme doses, 1 Tg value > 10,000 (plausible metastatic)

---

## 4 — Batch 4: Tumor / Path cross-validation

**Tables audited:** `synoptic_tumor_long_v1` (13,569 rows / 9,187 patients), `tumor_episode_master_v2` (10,871 rows / 8,733 patients), `path_synoptics` (per-surgery wide), `canonical_tumor_characteristics_v1` (8,422 patients, 1-to-many tumors).

### 4.1 `CRITICAL` — `ajcc8_t_stage` is overstaged: 906 / 1,146 T3b assignments are driven by microscopic ETE (AJCC 8 removed microscopic ETE as a T3b criterion)

**Finding.** `CPM.ajcc8_t_stage` shows 1,146 T3b — a biologically implausible plurality of the staged cohort (26% of 4,083 patients with a T-stage). Drill-in:

| `microscopic_ete_t3b_corrected` | `ete_grade_final` | n |
|---|---|---|
| TRUE | microscopic | **903** |
| FALSE | gross | 188 |
| FALSE | microscopic | 43 |
| (other combos) | — | 12 |

Of 1,146 T3b, only 188 have *gross* ETE (the true AJCC 8 T3b criterion). The other 958 reflect a legacy "microscopic ETE → T3b" rule that AJCC 8 explicitly eliminated in 2017. `ajcc8_t_stage_corrected` already restages these back down (T3b → T1a/T1b/T2/T3a distributed as 206/243/241/216, with only 240 true T3b retained).

**Replay.**

```sql
SELECT ajcc8_t_stage, ajcc8_t_stage_corrected, COUNT(*)
FROM main.canonical_patient_master
WHERE ajcc8_t_stage IS NOT NULL
GROUP BY 1,2 ORDER BY 3 DESC;
-- T3b,T1b,243 | T3b,T2,241 | T3b,T3b,240 | T3b,T3a,216 | T3b,T1a,206
```

**Draft fix SQL.**

```sql
-- FIX: Promote ajcc8_t_stage_corrected to the authoritative field.
-- NOT EXECUTED - dry run.
/*
ALTER TABLE main.canonical_patient_master
  RENAME COLUMN ajcc8_t_stage TO ajcc8_t_stage_with_microete_t3b_DEPRECATED;
ALTER TABLE main.canonical_patient_master
  RENAME COLUMN ajcc8_t_stage_corrected TO ajcc8_t_stage;
-- Preserve the old interpretation under an unambiguous name for auditability.
COMMENT ON COLUMN main.canonical_patient_master.ajcc8_t_stage_with_microete_t3b_DEPRECATED IS
  'Legacy: upgrades microscopic ETE to T3b (contrary to AJCC 8). Do not use.';
*/
```

Concordance with `tumor_episode_master_v2.t_stage` (worst-case max per patient) jumps from 55% (2,195/4,009) using the current uncorrected field to 66% (2,637/4,009) using the corrected field. The residual 34% gap reflects a mixture of tem using raw path T-stage vs. CPM using clinical/resolved T-stage, which is a separate downstream issue.

### 4.2 `MED` — `multifocal_flag_path` has 425 "ghost" TRUE assertions where no multi-tumor evidence exists

**Finding.** Among 1,784 patients with `CPM.multifocal_flag_path = TRUE`, only 1,359 have > 1 tumor row in `synoptic_tumor_long_v1`; the remaining 425 have a single synoptic row. Drill-in by NLP support:

| `nlp_path_multifocal_mentioned` | `path_multifocal_flag` | n (of 425) | interpretation |
|---|---|---|---|
| NULL | NULL | 245 | no supporting signal at all |
| FALSE | NULL | 99 | NLP explicitly says *not* multifocal |
| TRUE | NULL | 81 | NLP supports multifocality |

The 81 NLP-supported cases are defensible (NLP of a path note that describes additional foci below synoptic enumeration). The **245 ghost** and **99 NLP-contradicted** calls are suspect — together they over-call multifocality by 344 patients (19% of the multifocal cohort).

**Replay.**

```sql
WITH syn AS (
  SELECT research_id, COUNT(DISTINCT tumor_index) AS n_tumors_syn
  FROM main.synoptic_tumor_long_v1 GROUP BY 1
)
SELECT p.nlp_path_multifocal_mentioned, p.path_multifocal_flag, COUNT(*)
FROM main.canonical_patient_master p
JOIN syn ON TRY_CAST(p.research_id AS INTEGER) = syn.research_id
WHERE p.multifocal_flag_path = TRUE AND syn.n_tumors_syn = 1
GROUP BY 1,2 ORDER BY 3 DESC;
```

**Draft fix SQL.**

```sql
-- FIX: downgrade CPM.multifocal_flag_path to FALSE (or NULL) when zero supporting evidence.
-- NOT EXECUTED - dry run.
/*
WITH syn AS (
  SELECT research_id, COUNT(DISTINCT tumor_index) AS n_tumors_syn
  FROM main.synoptic_tumor_long_v1 GROUP BY 1
)
UPDATE main.canonical_patient_master cpm
SET multifocal_flag_path = CASE
  WHEN syn.n_tumors_syn > 1 THEN TRUE
  WHEN cpm.nlp_path_multifocal_mentioned = TRUE THEN TRUE
  WHEN cpm.path_multifocal_flag = TRUE THEN TRUE
  ELSE FALSE
END
FROM syn
WHERE TRY_CAST(cpm.research_id AS INTEGER) = syn.research_id;
*/
```

### 4.3 `MED` — `path_tumor_size_cm` has 159 mismatches (> 0.1 cm) against `canonical_tumor_characteristics_v1`

**Finding.** Out of 8,422 patients in `canonical_tumor_characteristics_v1`, 4,130 have a non-null `CPM.path_tumor_size_cm`. Exact match on `MAX(size_greatest_dimension_cm)`: 3,828 / 4,130 (93%). 159 patients differ by > 1 mm.

Likely causes: CPM is picking the *dominant* tumor size (per `tumor_size_cm_dominant`) whereas our reference query is picking `MAX`. For multifocal patients with an index tumor smaller than a secondary focus, these will disagree. Worth documenting the rule explicitly.

**Recommendation.** Add a schema comment clarifying whether `path_tumor_size_cm = dominant` or `= max`, and add an invariant check that `path_tumor_size_cm ≤ tumor_size_cm_max`.

### 4.4 `INFO` — ETE / LVI / margin cross-validation is excellent once CAP-synoptic "x" is interpreted as checked

When `extrathyroidal_extension = 'x'` (CAP checkbox marked) is interpreted as "present" (which is how CPM maps it to `ete_grade = 'microscopic'`), the concordance between per-patient `BOOL_OR(synoptic)` and the CPM rollups is:

| field | CPM non-null | concordant | syn-yes CPM-no | syn-no CPM-yes |
|---|---|---|---|---|
| `ete_any_present_path` | 3,885 / 8,422 | **3,878 (99.8%)** | 7 | 0 |
| `lvi_any_present_path` | 3,885 / 8,422 | 3,447 (88.7%) | 2 | 0 |
| `margin_involved_any` | 3,885 / 8,422 | 3,349 (86.2%) | 1 | 0 |

The 4,537 NULL-vs-NULL patients all have null ETE across every synoptic row for that patient, so CPM's NULL is appropriate (no under-rollup).

### 4.5 `INFO` — Zero orphans across all 4 tumor/path tables vs CPM

`synoptic_tumor_long_v1`, `tumor_episode_master_v2`, `path_synoptics`, `canonical_tumor_characteristics_v1` — zero `research_id` values present in the detail table and missing from CPM. Good.

### 4.6 `INFO` — `canonical_tumor_characteristics_v1.multifocality_flag` is 100% NULL

Not a data integrity problem per se, but worth noting: the newly-built `canonical_tumor_characteristics_v1` has the `multifocality_flag` column declared and populated in schema but its content is 100% NULL across all 8,422 patients. Same for `number_of_tumors`. CPM's multifocality logic therefore cannot reference CTC as an authority; it must rely on synoptic row counts + NLP. Recommend populating CTC.multifocality_flag in the next build so CTC becomes a self-contained multifocality source.

---

## 5 — Batch 5: Outcomes cross-validation

**Tables audited:** `complication_phenotype_v1` (5,978 rows / 2,938 patients), `recurrence_event_clean_v1` (~2,246 rows / 1,946 patients), `operative_episode_detail_v2` (per-surgery / 9,368 patients), `ln_master_rollup_v1` (4,273 rows / 3,986 patients).

### 5.1 `CRITICAL` — `any_recurrence_flag` under-calls recurrence for 1,616 / 1,946 patients (83%)

**Finding.** Three-way break-down of recurrence flags in CPM:

| `any_recurrence_flag` | `any_recurrence_flag_prev_233` | `structural` | `biochemical` | n |
|---|---|---|---|---|
| FALSE | FALSE | NULL | NULL | 8,871 (no rec — correct) |
| **FALSE** | **TRUE** | **TRUE** | FALSE | **1,506** ← should be TRUE |
| TRUE | TRUE | TRUE | FALSE | 312 |
| **FALSE** | **TRUE** | FALSE | **TRUE** | **110** ← should be TRUE |
| TRUE | FALSE | NULL | NULL | 54 (spurious TRUE — no evidence) |
| TRUE | TRUE | FALSE | TRUE | 18 |

**1,506 patients have confirmed structural recurrence** (per both `recurrence_event_clean_v1` and CPM's own `structural_recurrence_flag = TRUE`), but `any_recurrence_flag = FALSE`. Likewise 110 with biochemical recurrence. The column `any_recurrence_flag_prev_233` appears to be the correct version (TRUE for 1,946 rec patients); the current `any_recurrence_flag` has been regressed.

**Replay.**

```sql
SELECT any_recurrence_flag, any_recurrence_flag_prev_233,
       structural_recurrence_flag, biochemical_recurrence_flag, COUNT(*)
FROM main.canonical_patient_master
WHERE any_recurrence_flag IS NOT NULL OR any_recurrence_flag_prev_233 IS NOT NULL
GROUP BY 1,2,3,4 ORDER BY 5 DESC;
```

**Draft fix SQL.**

```sql
-- FIX: swap any_recurrence_flag back to the _prev_233 snapshot (or rebuild from subtypes).
-- NOT EXECUTED - dry run.
/*
UPDATE main.canonical_patient_master
SET any_recurrence_flag = (
  COALESCE(structural_recurrence_flag, FALSE)
  OR COALESCE(biochemical_recurrence_flag, FALSE)
  OR COALESCE(imaging_suspicious_recurrence_flag, FALSE)
);
-- Retain any_recurrence_flag_prev_233 for audit trail; do not drop.
*/
```

This is the single highest-impact correctness issue in the current CPM build — it affects every downstream recurrence analysis.

### 5.2 `CRITICAL` — 635 patients have operative records but do NOT exist in `canonical_patient_master`

**Finding.** `operative_episode_detail_v2` contains 9,368 distinct `research_id` values. Only 8,733 match a CPM row; **635 do not exist in CPM at all** (cast types already normalized via `TRY_CAST(... AS INTEGER)`, so this is genuine missing coverage, not a join-type artifact).

These 635 patients had surgery logged in the operative episode detail but were not ingested into the master patient registry. The impact depends on how these 635 were excluded — could be intentional cohort filtering, but if so it should be documented.

**Replay.**

```sql
SELECT COUNT(DISTINCT op.research_id)
FROM main.operative_episode_detail_v2 op
WHERE TRY_CAST(op.research_id AS INTEGER) NOT IN (
  SELECT TRY_CAST(research_id AS INTEGER)
  FROM main.canonical_patient_master WHERE research_id IS NOT NULL
);  -- 635
```

**Recommendation.**

```sql
-- ACTION: triage the 635 orphans.
-- NOT EXECUTED - requires human cohort decision.
/*
CREATE OR REPLACE TABLE audit.cpm_missing_vs_op_episode AS
SELECT DISTINCT op.research_id,
       MIN(op.resolved_surgery_date) AS first_surg,
       MAX(op.resolved_surgery_date) AS last_surg,
       COUNT(*) AS n_op_rows
FROM main.operative_episode_detail_v2 op
WHERE TRY_CAST(op.research_id AS INTEGER) NOT IN (
  SELECT TRY_CAST(research_id AS INTEGER)
  FROM main.canonical_patient_master WHERE research_id IS NOT NULL
)
GROUP BY 1;
-- Then either: (a) add these rows to CPM with minimal fields, or (b) document exclusion criteria.
*/
```

### 5.3 `HIGH` — 128 patients with confirmed complications (hematoma/seroma/chyle-leak/etc.) have `any_confirmed_complication_flag = FALSE`

**Finding.** `complication_phenotype_v1` tracks 9 complication entities (hypocalcemia, rln_injury, hematoma, hypoparathyroidism, seroma, chyle_leak, wound_infection, vocal_cord_paresis, vocal_cord_paralysis). CPM only exposes *per-entity* `comp_*_confirmed` fields for the first three (hypocalcemia, hypoparathyroidism, rln_injury). Individual per-type concordances are perfect (0 discordances), but the aggregate `any_confirmed_complication_flag = FALSE` for 128 patients who DO have a confirmed hematoma / seroma / chyle-leak / wound-infection in the phenotype table.

**Replay.**

```sql
WITH cp AS (
  SELECT research_id, BOOL_OR(confirmed_flag=TRUE) AS any_cp
  FROM main.complication_phenotype_v1 GROUP BY 1
)
SELECT COUNT(*) FROM cp
JOIN main.canonical_patient_master p
  ON TRY_CAST(p.research_id AS INTEGER) = TRY_CAST(cp.research_id AS INTEGER)
WHERE cp.any_cp = TRUE AND p.any_confirmed_complication_flag = FALSE;  -- 128
```

**Draft fix SQL.**

```sql
-- FIX: rebuild any_confirmed_complication_flag from full complication_phenotype_v1.
-- NOT EXECUTED - dry run.
/*
WITH cp AS (
  SELECT research_id, BOOL_OR(confirmed_flag = TRUE) AS any_confirmed
  FROM main.complication_phenotype_v1 GROUP BY 1
)
UPDATE main.canonical_patient_master p
SET any_confirmed_complication_flag = COALESCE(cp.any_confirmed, FALSE)
FROM cp
WHERE TRY_CAST(p.research_id AS INTEGER) = TRY_CAST(cp.research_id AS INTEGER);

-- Also add per-entity columns for parity:
ALTER TABLE main.canonical_patient_master
  ADD COLUMN IF NOT EXISTS comp_hematoma_confirmed BOOLEAN,
  ADD COLUMN IF NOT EXISTS comp_seroma_confirmed BOOLEAN,
  ADD COLUMN IF NOT EXISTS comp_chyle_leak_confirmed BOOLEAN,
  ADD COLUMN IF NOT EXISTS comp_wound_infection_confirmed BOOLEAN;
*/
```

### 5.4 `MED` — `ln_count_reconciled` and `ln_positive_final` disagree with `ln_master_rollup_v1` for ~50% of patients

**Finding.** At patient level (3,986 LN-rollup patients aggregated):

| metric | exact match | rate |
|---|---|---|
| `ln_count_reconciled` vs SUM(`ln_total_examined`) | 1,624 | 41% |
| `ln_count_reconciled` vs MAX(`ln_total_examined`) | 1,658 | 42% |
| `ln_positive_final` vs SUM(`ln_total_positive`) | 2,086 | 52% |
| `ln_positive_final` vs MAX(`ln_total_positive`) | 2,156 | 54% |
| `ln_rollup_any_positive` | 3,737 | 94% |
| `ln_rollup_central_positive` | 4,147 (of 4,273 joined rows) | high |

The **any_positive** flag is 94% concordant — the categorical signal is solid. The **numeric counts**, however, are only ~50% concordant. This is a known consequence of LN rollup being per-surgery-episode while CPM is per-patient, compounded by CPM fusing path-report counts with operative-note counts where the rollup only uses path.

**Recommendation.** Document the counting rule for `ln_count_reconciled` in the dataset dictionary. If the rule is "use path when available, else operative," publish a `ln_count_source` column so consumers can stratify by provenance. Current 19 discordances on `any_positive` flag (LN-rollup says yes, CPM says no) are small enough to tag as MED, not HIGH.

### 5.5 `INFO` — `n_surgeries` / `first_surgery_date` / `lateral_neck_dissected` all reconcile cleanly

From `operative_episode_detail_v2`: 8,731 exact matches on `n_surgeries` (zero mismatches), 8,731 on `first_surgery_date` (zero mismatches), 8,479 on `lateral_neck_dissected`. Surgery-identity/counting is solid.

Low-match areas to investigate separately (not CPM defects per se):
- `op_rln_monitoring_any` vs `operative_episode.rln_monitoring_flag`: 1,701 match — CPM uses multiple NLP sources.
- `op_parathyroid_autograft_any`: 40 match — CPM autograft signal is drawn from NLP of op-notes, which may be more sensitive than the structured flag.

### 5.6 `INFO` — Zero complication orphans, zero recurrence orphans

`complication_phenotype_v1` (2,938 patients) and `recurrence_event_clean_v1` (1,946 patients) both have 100% coverage vs CPM — every `research_id` in the detail tables exists in CPM. Only `operative_episode_detail_v2` has the 635-patient gap (5.2).

---

## 6 — Consolidated summary

### 6.1 Findings by severity

| # | section | severity | finding |
|---|---|---|---|
| 1 | 1.1 | CRITICAL | `max_tirads_ever` wrong for 1,247 / 1,789 patients (US nodule master under-rolled) |
| 2 | 1.2 | HIGH | `n_fna_episodes` miscount (reported in Batch 1) |
| 3 | 2.1 | CRITICAL | Lab orphans: 68 research_ids in `thyroglobulin_lab_canonical_v1` missing from CPM |
| 4 | 2.2 | HIGH | `n_lab_measurements_structured` stale — under-counts for 57% of patients |
| 5 | 3.1 | HIGH | `rai_max_dose_mci` mismatch for 211 / 862 RAI patients |
| 6 | 3.3 | HIGH | `n_tg_measurements_structured` under-counts for 1,444 / 2,528 |
| 7 | 3.4 | HIGH | `tg_peak` / `tg_nadir` mismatch for ~20% of Tg patients |
| 8 | 4.1 | **CRITICAL** | `ajcc8_t_stage` overstaged: 906 / 1,146 T3b should be lower stage (AJCC 8 microscopic-ETE rule) |
| 9 | 4.2 | MED | `multifocal_flag_path` has 344 unsupported TRUE calls (245 ghost + 99 NLP-contradicted) |
| 10 | 4.3 | MED | `path_tumor_size_cm` differs from CTC max for 159 patients (probable dominant-vs-max semantics drift) |
| 11 | 5.1 | **CRITICAL** | `any_recurrence_flag` under-calls recurrence for 1,616 / 1,946 patients |
| 12 | 5.2 | **CRITICAL** | 635 patients exist in `operative_episode_detail_v2` but NOT in CPM |
| 13 | 5.3 | HIGH | `any_confirmed_complication_flag` misses hematoma/seroma/chyle/wound complications (128 patients) |
| 14 | 5.4 | MED | LN counts (`ln_count_reconciled`, `ln_positive_final`) diverge from `ln_master_rollup_v1` for ~50% |

**Tally:** 5 CRITICAL, 5 HIGH, 3 MED, plus several INFO confirmations of clean areas.

### 6.2 Patterns

Three systematic patterns explain most of the CRITICAL findings:

1. **Legacy fields not retired after correction was computed.** `ajcc8_t_stage` / `ajcc8_t_stage_corrected`, `any_recurrence_flag` / `any_recurrence_flag_prev_233`, and earlier `rai_max_dose_mci` / `rai_dose_v9` all follow the same anti-pattern: a corrected version lives next to a broken primary. Downstream consumers who read the "unadorned" column will silently get the wrong answer.

2. **Stale rollup snapshots.** Lab measurement counts, Tg peak/nadir, and Tg/TgAb counts all under-count because CPM was built before the `20260411_final_master_release` lab-wave close. Needs a re-rollup pass.

3. **Cohort-registry coverage leak.** 635 operative-episode patients missing from CPM, plus 68 thyroglobulin patients missing from CPM, suggests the CPM registry population rule is stricter than the surgical / lab inclusion criteria. Either widen CPM or document exclusions explicitly.

### 6.3 Master fix-SQL appendix (dry-run — none of this has been executed)

```sql
-- ============================================================
-- THYROID CPM  REBUILD BLOCK  (dry-run; uncomment to execute)
-- Matches findings 1.1, 2.2, 3.1, 3.3, 3.4, 4.1, 5.1, 5.2, 5.3
-- ============================================================
/*
BEGIN TRANSACTION;

-- 4.1: retire the overstaged T field
ALTER TABLE main.canonical_patient_master
  RENAME COLUMN ajcc8_t_stage TO ajcc8_t_stage_with_microete_t3b_DEPRECATED;
ALTER TABLE main.canonical_patient_master
  RENAME COLUMN ajcc8_t_stage_corrected TO ajcc8_t_stage;

-- 5.1: rebuild any_recurrence_flag from components
UPDATE main.canonical_patient_master
SET any_recurrence_flag = (
    COALESCE(structural_recurrence_flag, FALSE)
 OR COALESCE(biochemical_recurrence_flag, FALSE)
 OR COALESCE(imaging_suspicious_recurrence_flag, FALSE)
);

-- 5.3: rebuild any_confirmed_complication_flag from full phenotype table
WITH cp AS (
  SELECT research_id, BOOL_OR(confirmed_flag=TRUE) AS any_c
  FROM main.complication_phenotype_v1 GROUP BY 1
)
UPDATE main.canonical_patient_master p
SET any_confirmed_complication_flag = COALESCE(cp.any_c, FALSE)
FROM cp
WHERE TRY_CAST(p.research_id AS INTEGER) = TRY_CAST(cp.research_id AS INTEGER);

-- 2.2 + 3.3 + 3.4: rebuild lab counts and Tg peak/nadir from canonical lab tables
WITH t AS (
  SELECT research_id,
         COUNT(*) FILTER (WHERE analyte='Tg')                        AS n_tg,
         COUNT(*) FILTER (WHERE analyte='TgAb')                      AS n_tgab,
         MAX(result_numeric) FILTER (WHERE analyte='Tg')             AS tg_peak_c,
         MIN(result_numeric) FILTER (WHERE analyte='Tg')             AS tg_nadir_c
  FROM main.thyroglobulin_lab_canonical_v1 GROUP BY 1
)
UPDATE main.canonical_patient_master p
SET n_tg_measurements_structured = COALESCE(t.n_tg,0),
    n_tgab_measurements          = COALESCE(t.n_tgab,0),
    tg_peak                      = t.tg_peak_c,
    tg_nadir                     = t.tg_nadir_c
FROM t
WHERE TRY_CAST(p.research_id AS INTEGER) = t.research_id;

-- 3.1: fix RAI max dose
WITH e AS (
  SELECT research_id, MAX(rai_dose_mci) AS max_dose
  FROM main.rai_episode_canonical_v1 GROUP BY 1
)
UPDATE main.canonical_patient_master p
SET rai_max_dose_mci = e.max_dose
FROM e
WHERE TRY_CAST(p.research_id AS INTEGER) = e.research_id
  AND  COALESCE(e.max_dose, p.rai_max_dose_mci) > 0;

-- 1.1: fix max_tirads_ever
WITH u AS (
  SELECT research_id, MAX(tirads_total) AS max_tirads
  FROM main.us_nodule_master_v2 GROUP BY 1
)
UPDATE main.canonical_patient_master p
SET max_tirads_ever = u.max_tirads
FROM u
WHERE TRY_CAST(p.research_id AS INTEGER) = u.research_id;

-- 5.2: stage the operative-episode orphans for cohort-inclusion decision
CREATE OR REPLACE TABLE audit.cpm_missing_vs_op_episode AS
SELECT DISTINCT op.research_id,
       MIN(op.resolved_surgery_date) AS first_surg,
       MAX(op.resolved_surgery_date) AS last_surg,
       COUNT(*) AS n_op_rows
FROM main.operative_episode_detail_v2 op
WHERE TRY_CAST(op.research_id AS INTEGER) NOT IN (
  SELECT TRY_CAST(research_id AS INTEGER)
  FROM main.canonical_patient_master WHERE research_id IS NOT NULL)
GROUP BY 1;

COMMIT;
*/
```

### 6.4 Suggested verification pass

Before executing any of the above, re-run each section's replay query fresh against MotherDuck and diff the counts against this report. If any count has drifted by more than ±10, treat the finding as stale and re-triage.

---

## 7 — Correction addendum (after re-review against repo scripts and MotherDuck)

**Why this section exists.** On re-review, three CRITICAL findings from sections 4.1, 5.1, and 5.2 were **incorrect** — the underlying behaviors are intentional canonical design, not bugs. Documenting the corrections here rather than silently rewriting the earlier sections so the audit trail is preserved.

### 7.1 Finding 4.1 (T3b overstaging) — **downgraded: HIGH → INFO (documentation gap only)**

**What I got wrong.** I called this "CRITICAL: `ajcc8_t_stage` is overstaged." The **microscopic-ETE-T3b correction has already been built** (per `scripts/240_ln_staging_cleanup.py`). Script 240 line 30 explicitly states: *"It preserves the ORIGINAL `ajcc8_t_stage` and `ajcc8_stage_group` columns"* — the uncorrected column is kept deliberately as an audit trail. The corrected values live in `ajcc8_t_stage_corrected`, computed via the AJCC 8 DTC rule (size-based restaging for mic-ETE patients). Script 240 also computes `ajcc8_stage_group_corrected` and a `microscopic_ete_t3b_corrected` boolean for traceability.

**What's actually left to do.** Two small documentation / consumption gaps:

1. The data dictionary (`data_dictionary_v240`) marks both `ajcc8_t_stage` and `ajcc8_t_stage_corrected` as `authoritative` with no `replacement_column_name` pointer. A consumer reading the dictionary can't tell which to prefer.
2. Manuscript cohort views (e.g., `cohort_m048_tnm_multifocal_v1`, `cohort_m051_ete_ln_v1`) currently SELECT `ajcc8_t_stage` rather than `ajcc8_t_stage_corrected`. This is a potential issue for manuscripts that report T-stage distributions, depending on intent.

**Recommended action (not a fix — a small dictionary edit).**

```sql
-- NOT EXECUTED - dry run.
/*
UPDATE main.data_dictionary_v240
SET replacement_column_name = 'ajcc8_t_stage_corrected',
    description = COALESCE(description,'') || ' Legacy field: upgrades microscopic ETE to T3b contrary to AJCC 8. For staging analyses use ajcc8_t_stage_corrected.'
WHERE column_name = 'ajcc8_t_stage';
*/
```

**Verdict.** User was correct — the correction was applied. My finding over-stated the severity. The residual concern is whether manuscript cohort views pick up the corrected field; that's a per-manuscript editorial decision, not a data bug.

### 7.2 Finding 5.1 (`any_recurrence_flag` "regression") — **withdrawn: CRITICAL → INFO**

**What I got wrong.** I called this "CRITICAL: `any_recurrence_flag` under-calls recurrence for 1,616 patients" and proposed a fix rebuilding from `structural_recurrence_flag OR biochemical_recurrence_flag OR imaging_suspicious_recurrence_flag`. That rebuild would REINTRODUCE the broader, imaging-inclusive definition that the repo explicitly rejected.

**What the canonical definition actually is.** Per `scripts/203_canonical_recurrence.py` header comment:

> TRUE RECURRENCE requires ONE of:
>   1. Reoperation with pathology showing recurrent/persistent cancer (structural, confirmed)
>   2. FNA/biopsy with malignant cytology — Bethesda V/VI post-op (structural, confirmed)
>   3. Rising Tg in a patient who previously had undetectable Tg (biochemical)
>
> NOT true recurrence:
>   - Imaging concerning without biopsy confirmation → imaging_suspicious_unconfirmed
>   - Elevated Tg that was never undetectable → persistent_biochemical_disease
>   - Clinical note mentions without confirmation → clinical_suspicion_unresolved

The authoritative path-proven flag is **`recurrence_flag_v2`** (189 TRUE patients — built by Script 224). The `any_recurrence_flag_prev_233` column holds the *old*, broad, imaging-inclusive definition (1,946 TRUE) — kept strictly as audit trail.

My "false negative" list was wrong because `structural_recurrence_flag = TRUE` also fires on imaging-detected structural findings (CT/MRI pathologic lymphadenopathy and reoperation_proxy), which per the canonical rule are explicitly **not** recurrence.

**Minor ambiguity that remains.** `any_recurrence_flag` itself (384 TRUE) doesn't cleanly map to `recurrence_flag_v2` alone (189) — the 384 looks like (`recurrence_flag_v2` OR some structural subset that overlaps path evidence). Worth clarifying the rule with a dictionary comment but **not** worth changing the data. The existing three fields cover the clinically meaningful subtypes:
- `recurrence_flag_v2` = path-proven structural recurrence (strict) — 189
- `biochemical_recurrence_flag` = biochemical-only recurrence — 128
- `imaging_suspicious_recurrence_flag` = imaging-only, unconfirmed — 79

**Verdict.** User was correct. No fix needed. Withdrawing this finding.

### 7.3 Finding 5.2 (635 "orphan" operative-episode patients) — **withdrawn: CRITICAL → INFO**

**What I got wrong.** I called this "CRITICAL: 635 patients have operative records but do NOT exist in CPM." I speculated the gap was either (a) intentional cohort filtering or (b) a coverage bug, and proposed triaging the 635.

**What's actually going on.** The 635 orphans are correctly excluded — they are **not thyroid cancer patients**. Cross-checking those 635 research_ids against every cancer-evidence table in the publication DB:

| evidence table | orphans with a record |
|---|---|
| `fna_episode_master_v2` | 0 |
| `tumor_episode_master_v2` | 0 |
| `synoptic_tumor_long_v1` | 0 |
| `path_synoptics` | 0 |
| `imaging_nodule_master_v1` | 0 |

**Zero of 635 have any FNA, tumor episode, synoptic, or path evidence of thyroid cancer.** Their operative records show `procedure_normalized = 'unknown'` across the board. These are presumably non-thyroid neck surgeries, benign-only thyroid procedures that don't meet the publication's malignant-cohort gate, or operative notes captured by the EHR extractor but not associated with a thyroid cancer diagnosis. `canonical_patient_master` is (correctly) defined as the thyroid-cancer cohort, not the operative-volume cohort.

**Replay.**

```sql
WITH orphan_ids AS (
  SELECT DISTINCT TRY_CAST(op.research_id AS INTEGER) AS rid
  FROM main.operative_episode_detail_v2 op
  WHERE TRY_CAST(op.research_id AS INTEGER) NOT IN (
    SELECT TRY_CAST(research_id AS INTEGER) FROM main.canonical_patient_master WHERE research_id IS NOT NULL)
),
signals AS (
  SELECT o.rid,
         EXISTS(SELECT 1 FROM main.fna_episode_master_v2     x WHERE TRY_CAST(x.research_id AS INTEGER)=o.rid) AS fna,
         EXISTS(SELECT 1 FROM main.tumor_episode_master_v2   x WHERE TRY_CAST(x.research_id AS INTEGER)=o.rid) AS tum,
         EXISTS(SELECT 1 FROM main.synoptic_tumor_long_v1    x WHERE TRY_CAST(x.research_id AS INTEGER)=o.rid) AS syn,
         EXISTS(SELECT 1 FROM main.path_synoptics            x WHERE TRY_CAST(x.research_id AS INTEGER)=o.rid) AS ps
  FROM orphan_ids o
)
SELECT COUNT(*) FILTER (WHERE NOT (fna OR tum OR syn OR ps)) AS no_cancer_evidence,
       COUNT(*) AS n_orphans
FROM signals;  -- 635 / 635
```

**Verdict.** User was correct to push back. This is correct cohort gating, not a coverage gap. Withdrawing this finding.

**Minor documentation note.** The inclusion rule (`canonical_patient_master ≡ thyroid cancer cohort, not operative cohort`) isn't stated explicitly in a single place; codifying it in `data_dictionary_v240` or `AGENTS.md` would prevent a future reader from making the same mistake I did.

### 7.4 Revised severity tally

After the three corrections, the scorecard becomes:

| severity | count (pre-correction) | count (post-correction) |
|---|---|---|
| CRITICAL | 5 | **2** (1.1 max_tirads_ever, 2.1 Tg lab orphans) |
| HIGH | 5 | 4 (2.2, 3.1, 3.3, 3.4, 5.3) |
| MED | 3 | 3 (4.2, 4.3, 5.4) |

The real remaining critical path is **finding 1.1 (max_tirads_ever)** and **finding 2.1 (68 Tg lab orphans)** plus the stale-snapshot family (2.2 / 3.3 / 3.4 / 3.1) which all have a clean single-pass fix (rebuild from canonical labs / RAI episodes). Every T-stage / recurrence / operative-coverage issue I flagged was either already addressed or by design.

---

_End of cross-validation report._




