# Reviewer attack sheet — submission v1 (2026-03-26)

Anticipated critiques and **documented** responses (evidence in this folder). **Do not** overclaim causality in rebuttals.

---

## 1. “This is just single-center descriptive practice variation.”

**Response:** Framed as **associational** retrospective cohort with **pre-specified** exclusions and **multivariable** adjustment for measured preoperative variables; **0%** research_id mismatch between CSV and in-memory cohort in `validation_report.md`; strict vs broad nodal sensitivity (**635**) reported. **Limitation accepted:** generalizability not established.

---

## 2. “You cannot infer that molecular testing drives extent choice.”

**Response:** **Agree.** Testing was **sparse (20/558)**; `has_mol` was **not** significant in primary adjusted models (**aOR 0.606**); we **do not** claim a causal effect of testing. Molecular **2×2** counts are **descriptive** only—**not** sensitivity/specificity.

---

## 3. “Bethesda and FNA linkage missingness biases results.”

**Response:** **Partially agree** as limitation. **149/558** missing Bethesda (`missingness_summary.csv`); pipeline coerces missing to “not ≥4” for the binary classifier — **documented** in code; may **attenuate or distort** association with extent. Formal missing-data sensitivity analyses are **not** in frozen outputs — **gap** flagged in `MANUSCRIPT_GAP_LIST.md`.

---

## 4. “Pathology-sized 2–4 cm cohort is the right gold standard; you have N=0.”

**Response:** **Acknowledge.** Current freeze: **path_sensitivity_n = 0** after strict exclusions (`analysis_manifest.json`, `cohort_build_log.md`). Manuscript **does not** claim pathology corroboration. Imaging-based size reflects the **pre-surgical** information set; **limitation** explicit.

---

## 5. “Completion thyroidectomy is probably underestimated; you report 0.”

**Response:** **Acknowledge** measurement dependence. Completion flags have **heavy missingness** (`missingness_summary.csv`); **zero** observed events among **238** lobectomy patients in `table7_completion_thyroidectomy.csv` under current operationalization. Discussion cites **external** completion literature **only** with a **NEEDS AUTHOR CHECK** citation (ref 12). Framed as **pipeline-defined**, not definitive surgical history.

---

## 6. “Logistic model on molecular subset is garbage (n=20, separation).”

**Response:** **Agree for inference.** Multivariable molecular ORs are **not** primary claims; `model_summary_final.csv` flags **separation** for completion model. Primary story is **primary + broad** models with **Table 6** descriptive counts only.

---

## 7. “Why no multiplicity adjustment across many tests?”

**Response:** **Fair.** Univariable battery is **screening**; primary inference emphasized on **pre-specified** multivariable models. **Formal** FDR/Bonferroni across all contrasts **not** in current outputs — add in revision or soften language to “hypothesis-generating screen.”

---

## 8. “Confounding by indication, surgeon, era, patient preference.”

**Response:** **Acknowledge.** Not all are measurable in structured tables. **Surgery year** spans **2013–2023** in data (**not** modeled as primary result). Discussion separates **supported** associations from **unmeasured** determinants.

---

## 9. “STROBE flow figure missing.”

**Response:** **Addressed in package.** **Figure 1** (`fig_cohort_flow.png`) visualizes pipeline counts; legend documents **truncated** labels and pathology-arm zeros (`figure_legends_v1.md`). Editors may still request a **reformatted** CONSORT-style diagram—see `AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`.

---

## 10. “Ethics / IRB?”

**Response:** **Author must supply** — not present in artifact folder (`AUTHOR_INPUTS_REQUIRED_20260326.md`).
