# Schema & Linkage Audit — Molecular Utilization (THYROID_2026)

**Generated:** 2026-03-26 (live local DuckDB `thyroid_master.duckdb`)  
**Repo reference:** tag `v2026.03.13`, Zenodo [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510)

---

## 1. Document review summary

| Source | Relevance |
|--------|-----------|
| [`data_dictionary.md`](../data_dictionary.md) / CSV | Episode tables: `molecular_test_episode_v2` (`overall_result_class`, `platform`, dates, flags); `fna_episode_master_v2` (`bethesda_category`, linkage columns). `tumor_pathology` malignant-focused fields including `histology_1_type`. |
| [`MANUSCRIPT_READY_CHECKLIST.md`](../MANUSCRIPT_READY_CHECKLIST.md) | Confirms local DuckDB DB name, `val_*` validation layer, provenance caveats (non-Tg lab dates 0%, etc.). |
| [`docs/FINAL_REPO_STATUS_20260313.md`](../docs/FINAL_REPO_STATUS_20260313.md) | Manuscript cohort ready; **molecular date accuracy ~45% day-level** noted — analyses should surface `date_status` where relevant. |
| [`docs/database_hardening_audit_20260313.md`](../docs/database_hardening_audit_20260313.md) | V3 linkage tables are the documented production chain (`fna_molecular_linkage_v3`, `preop_surgery_linkage_v3`, `surgery_pathology_linkage_v3`). |
| [`scripts/78_final_hardening.py`](../scripts/78_final_hardening.py) | Imaging–FNA relaxed union, lab validation; supports re-linking upstream of analysis. |
| [`scripts/95_episode_linkage_repair.py`](../scripts/95_episode_linkage_repair.py) | Episode-aware chain repairs (notes, labs, imaging/FNA/molecular, path/surgery, RAI). |
| [`scripts/96_episode_downstream_repair.py`](../scripts/96_episode_downstream_repair.py) | Propagates `surgery_episode_id` into operative + v3 linkage alignment. |
| [`scripts/97_episode_linkage_audit.py`](../scripts/97_episode_linkage_audit.py), [`scripts/98_final_verification_pass.py`](../scripts/98_final_verification_pass.py) | Multi-surgery audits + truth snapshot exports. |

---

## 2. Live table inventory (local DuckDB)

| Table | Rows (live) | Column count (information_schema) | Notes |
|-------|-------------|-----------------------------------|--------|
| `manuscript_cohort_v1` | 10,871 | ~300 | Frozen resolved layer; patient grain. Bethesda column: `fna_bethesda_final`. |
| `fna_episode_master_v2` | 59,620 | 36 | **~12 distinct `fna_episode_id` values** repeated per patient — **always join on (`research_id`, `fna_episode_id`)**. No nodule size column at episode level in this build. |
| `molecular_test_episode_v2` | 10,126 | 84 | ThyroSeq / Afirma / Other; `resolved_test_date` + `test_date_native`. |
| `fna_molecular_linkage_v2` | **0** | 10 | **Empty — not a failure signal**; use v3. |
| `fna_molecular_linkage_v3` | **708** | Molecular–FNA pairs with scoring (`score_rank`, `analysis_eligible_link_flag`). |
| `preop_surgery_linkage_v3` | 3,591 | ~34 cols — FNA/molecular → surgery. |
| `surgery_pathology_linkage_v3` | 9,409 | ~36 cols — surgery → tumor episode. |
| `operative_episode_detail_v2` | 9,371 | 78 `procedure_normalized`, dates. |
| `tumor_pathology` | 4,290 | Malignant-pathology–leaning table (narrow vs full surgical cohort). |
| `tumor_episode_master_v2` | 11,691 | Episode surgery/tumor grain. |
| `val_episode_linkage_completeness_v1` | 5 | One row per linkage family (`fna_molecular`, `preop_surgery`, `surgery_pathology`, `pathology_rai`, `imaging_fna`). |

\*Typo fix: 708 rows, not 70832.

---

## 3. Column listings (how to reproduce)

Run on local DuckDB:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'main' AND table_name = 'manuscript_cohort_v1'
ORDER BY ordinal_position;
```

Repeat for: `fna_episode_master_v2`, `molecular_test_episode_v2`, `fna_molecular_linkage_v2`, `operative_episode_detail_v2`, `tumor_pathology`, `tumor_episode_master_v2`, `val_episode_linkage_completeness_v1`.

(Full column dumps omitted here for length; they are available from `information_schema` at any time.)

---

## 4. Sample QA results (live)

### 4.1 Platform × `overall_result_class` (ThyroSeq + Afirma)

Top patterns (molecular rows, any Bethesda): `ThyroSeq` + `other` dominates; `Afirma` + `suspicious` common; small counts for `positive`, `negative`, `non_diagnostic`, `cancelled`. See study SQL exports for full frequency table.

### 4.2 Direct linkage column fill (episode table)

**Among FNA rows with `bethesda_category` ∈ {3,4,5}** (`n` ≈ 19,565 duplicate rows if not deduped — use `DISTINCT (research_id, fna_episode_id)`):

- `linked_molecular_episode_id` non-null: **~2.8%**
- `linked_surgery_episode_id` non-null: **~6.3%**

Interpretation: **episode direct link columns are sparse**; **v3 linkage tables** and **patient-level temporal joins** are required for manuscript-grade utilization.

**Among molecular rows with platform ∈ {ThyroSeq, Afirma}** (`n` = 859):

- `linked_fna_episode_id` non-null: **~39%**
- `linked_surgery_episode_id` non-null: **0%**

### 4.3 `val_episode_linkage_completeness_v1` (2026-03-26)

| linkage_type | total_rows | linked | unlinked | linked_pct |
|--------------|------------|--------|----------|------------|
| fna_molecular | 708 | 708 | 0 | 100% |
| preop_surgery | 3,591 | 3,591 | 0 | 100% |
| surgery_pathology | 9,409 | 9,409 | 0 | 100% |
| pathology_rai | 23 | 23 | 0 | 100% |
| imaging_fna | 0 | 0 | 0 | (empty) |

### 4.4 Episode-level “Bethesda III–V + v3 surgery + v3 surgery–pathology” bridge

Unique **(`research_id`, `fna_episode_id`)** episodes meeting FNA Bethesda 3–5 + `preop_surgery_linkage_v3` (`preop_type='fna'`, `score_rank=1`, eligible) + `surgery_pathology_linkage_v3` (`score_rank=1`, eligible):

- **n_fna_episodes ≈ 138**, **n_patients ≈ 135**

### 4.5 FNA–molecular v3 overlap on that operated episode subset

**0** episodes had a matching (`research_id`, `fna_episode_id`) row in `fna_molecular_linkage_v3` with `score_rank=1` and `analysis_eligible_link_flag` in this snapshot.

**Conclusion:** For ThyroSeq vs Afirma utilization in **operated indeterminate** patients, **`manuscript_cohort_v1` + `molecular_test_episode_v2`** preoperative temporal join (see `indeterminate_molecular_cohort_v1`) is the **recoverable** path; **do not** rely on v3 FNA–molecular linkage alone for this subcohort.

### 4.6 Patient-level analytic cohort (primary study view)

`indeterminate_molecular_cohort_v1`:

- **n = 641** patients (`fna_bethesda_final` ∈ {3,4,5}, `surg_first_date` present, `histology_final` non-null)
- **n preop molecular-tested (ThyroSeq/Afirma)** = **69** (~**10.8%**)

Cross-check vs manuscript-only filter:

- `manuscript_cohort_v1` Bethesda 3–5 (any surgery histology): 1,617 patients; with surgery 1,163; with `histology_final` 641 aligned with study filter.

### 4.7 `tumor_pathology` join trap

`INNER JOIN tumor_pathology` on `research_id` alone **drops benign / non-malignant final histology** patients (table is not full surgical pathology). **Do not** require `tumor_pathology` for ROM denominator if the question is all surgical outcomes; use **`histology_final`** (or full synoptic layer) instead.

---

## 5. Gaps and recommendations

| Issue | Risk | Mitigation |
|-------|------|------------|
| Sparse FNA–molecular v3 on operated indeterminate chain | Underestimates utilization if v3-only | Use patient-level preop molecular join (implemented in study views). |
| `fna_episode_id` not globally unique | Cartesian joins if key omitted | Always include `research_id` in joins. |
| Malignant flag | Keyword logic on `histology_final` may misclassify edge narratives | Sensitivity analysis with `tumor_pathology.histology_1_type`; manual review of “Benign molecular / malignant surgery” cells. |
| Molecular dates | ~45% day-level precision (per FINAL_REPO_STATUS) | Stratify by `date_status` in supplement; prefer `resolved_test_date` then `test_date_native`. |
| Imaging–FNA linkage empty | Sankey imaging leg unavailable | `imaging_fna` row count 0 in `val_*`; do not promise imaging→FNA edges. |

---

## 6. Provenance

- **Code / SQL:** `studies/molecular_utilization_2026/sql/01_views_and_cohort.sql`
- **Verification:** `sql/02_local DuckDB_verification.sql`
- **Repository tag:** `v2026.03.13`
- **Archive DOI:** `10.5281/zenodo.18945510`

---

## 7. Transparency checklist (recommended)

1. Publish `mol_result_class_map_v1` mapping as supplement.  
2. Publish cohort flow: manuscript filter → 641; tested 69.  
3. Publish patient-level vs episode-level N (§4.4 vs §4.6) in supplement.  
4. Archive anonymized Parquet exports of `indeterminate_molecular_cohort_v1` with Zenodo bundle update.
