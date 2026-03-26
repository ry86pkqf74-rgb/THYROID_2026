# Data quality and edge cases — lobectomy vs total (2–4 cm, N0)

Generated with cohort build 2026-03-25 against MotherDuck `thyroid_research_2026`.

## Critical limitations

1. **Preoperative molecular testing (ThyroSeq/Afirma)**  
   In the final analytic cohort (N=574), only **21** patients had a molecular episode with `platform IN ('ThyroSeq','Afirma')` dated **before** first thyroid surgery. Multivariable models therefore have very low power for platform-specific effects; interaction models are **singular** and were not estimated.

2. **Completion thyroidectomy**  
   `operative_episode_detail_v2` contains **one row for ~99.97% of patients**; structured second-procedure capture is effectively absent. A text-based inference from sequential `tumor_episode_master_v2.procedure_raw` rows yielded **zero** completion cases inside this 2–4 cm imaging-N0 cohort (globally only **seven** patients match the pattern in the whole database). **Completion rates should not be interpreted from this pipeline** without external operative text mining or chart review.

3. **Preoperative lymph node negativity**  
   Negativity is operationalised as **no preoperative CT or MRI** with `pathologic_lymph_nodes = TRUE` on an exam dated on or before the surgery anchor. Patients **without** preoperative cross-sectional imaging remain eligible (absence of positive imaging, not proven radiologic N0). Neck ultrasound LN staging is not uniformly encoded in structured tables.

4. **Tumor size**  
   Primary size uses the maximum `imaging_nodule_master_v1.max_dimension_cm` on preoperative US rows, else `patient_analysis_resolved_v1.imaging_nodule_size_cm`. Pathologic size is retained for **sensitivity** (`exact_size_cm_path_sensitivity`); discordance between imaging and final pathology size is expected.

5. **Bethesda and histology missingness**  
   ~51% of patients fall in `bethesda_III_vs_IV_V = other_NA` (missing or non-classifiable FNA tier in the resolved layer). **48%** have indeterminate `path_malignant_flag` (missing histology text). Concordance (κ) used a **binary** “genetics high-risk” (preop suspicious/positive/high-risk marker) vs “pathology high-risk” (malignant histology keyword logic **or** ATA intermediate/high).

6. **Boolean / text typing**  
   MotherDuck may return boolean-like molecular fields inconsistently; the pipeline normalises `high_risk_marker_flag` via string comparison where needed.

7. **Multiple comparisons**  
   Univariable screening used **Benjamini–Hochberg FDR** on the prespecified feature list. The **primary** inferential focus is the multivariable logistic model for surgery type (pre-specified); ancillary κ estimates are exploratory.

8. **Pathologic size sensitivity (N=96)**  
   Re-fitting the full model on patients with `path_tumor_size_cm` ∈ \[2, 4\] failed to converge (quasi-separation with molecular dummies). See `tables/logistic_path_size_sensitivity_skipped.txt`.

## Provenance

- **Cohort SQL:** [`sql/01_cohort_base.sql`](../../studies/lobectomy_molecular_202603/sql/01_cohort_base.sql)  
- **Code & frozen release:** Git tag `v2026.03.10-publication-ready`; Zenodo DOI [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510)  
- **Database:** MotherDuck share / token per `motherduck_client.py`
