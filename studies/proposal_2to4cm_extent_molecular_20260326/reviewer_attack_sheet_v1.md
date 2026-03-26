# Reviewer attack sheet — submission v1

Anticipated critiques and **documented** responses (evidence in this folder). **Do not** overclaim causality in rebuttals.

---

## 1. “This is just single-center descriptive practice variation.”

**Response:** Framed as **associational** retrospective cohort with **pre-specified** exclusions and **multivariable** adjustment for measured preoperative variables; **0%** research_id mismatch between CSV and in-memory cohort in `validation_report.md`; strict vs broad nodal sensitivity (**635**) reported. **Limitation accepted:** generalizability not established.

---

## 2. “You cannot infer that molecular testing drives extent choice.”

**Response:** **Agree.** Testing was **sparse (20/558)**; `has_mol` was **not** significant in primary adjusted models; we **do not** claim a causal effect of testing. Molecular subgroup tables are **exploratory** only.

---

## 3. “Bethesda and FNA linkage missingness biases results.”

**Response:** **Partially agree** as limitation. **149/558** missing Bethesda (`missingness_summary.csv`); pipeline coerces missing to “not ≥4” for the binary classifier — **documented** in code; may **attenuate or distort** association with extent. Sensitivity analyses for missing Bethesda (e.g. complete-case only on FNA-linked rows) are **not** in frozen outputs — **gap** flagged in `MANUSCRIPT_GAP_LIST.md`.

---

## 4. “Pathology-sized 2–4 cm cohort is the right gold standard; you have N=0.”

**Response:** **Acknowledge.** Current freeze: **path_sensitivity_n = 0** after strict exclusions (`analysis_manifest.json`, `cohort_build_log.md`). Manuscript **does not** claim pathology corroboration. Imaging-based size reflects **pre-surgical** information set; **limitation** explicit.

---

## 5. “Completion thyroidectomy is probably underestimated; you report 0.”

**Response:** **Acknowledge** measurement dependence. Completion flags have **heavy missingness** on broad/primary frames for completion columns in `missingness_summary.csv`; **zero** observed events among **238** lobectomy patients in `table7_completion_thyroidectomy.csv` under current operationalization. Framed as **pipeline-defined**, not definitive long-term surgical history absent chart audit.

---

## 6. “Logistic model on molecular subset is garbage (n=20, separation).”

**Response:** **Agree for inference.** We **deprecate** multivariable molecular ORs for primary claims; `model_summary_final.csv` flags **separation** for completion model; molecular subset file shows **unstable** statistics. Primary story is **primary + broad** models with **Table 6** descriptive concordance only.

---

## 7. “Why no multiplicity adjustment across many tests?”

**Response:** **Fair.** Univariable battery is **screening**; primary inference emphasized on **pre-specified** multivariable models. **Formal** FDR/Bonferroni across all contrasts **not** in current outputs — either add in revision or soften language to “hypothesis-generating screen.”

---

## 8. “Confounding by indication, surgeon, era, patient preference.”

**Response:** **Acknowledge.** Not all are measurable in structured tables. **Surgery year** spans **2013–2023** in data (**not** modeled as primary result). Discussion separates **supported** associations from **unmeasured** determinants.

---

## 9. “STROBE flow figure missing.”

**Response:** **Accurate gap.** Provide a cleaned flow diagram in revision; until then cite `cohort_flow.csv` cautiously (`MANUSCRIPT_STATE_AUDIT.md` notes ambiguous zeros).

---

## 10. “Ethics / IRB?”

**Response:** **Author must supply** — not present in artifact folder.
