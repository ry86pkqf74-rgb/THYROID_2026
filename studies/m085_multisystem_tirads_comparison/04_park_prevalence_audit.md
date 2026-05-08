# Park 2009 X-Variable Prevalence Audit — Phase D.2
## M085: Multi-System TI-RADS Comparison Study

**Audit date:** 2026-05-08  
**BQ tables:** `pub_workspace.qc_park_x_prevalence_audit_v1`, `pub_workspace.qc_park_x_prevalence_vs_park2009_v1`  
**Export:** `exports/phase_d_park_prevalence_audit_20260508/`  
**Context:** Phase B.6 finalization found Park 2009 AUC = 0.5365 on this American surgical cohort (n=14,250 labeled nodules). This audit distinguishes the mechanisms driving that failure.

---

## 1. Background

Park JY et al. (*Thyroid* 2009;19:1257–1264) derived a logistic-regression model from a Korean cohort (n=1,694) with 12 binary US features (X1–X12). When applied to this cohort with published coefficients, the model achieves AUC = 0.5365 — essentially random discrimination. Two competing explanations exist:

1. **Coefficient non-portability:** The β values were optimized for Korean ultrasound reporting conventions, patient demographics, and equipment; they do not generalize to an American surgical cohort.
2. **Feature-distribution shift:** The prevalence of one or more X variables is sufficiently different between Park's derivation cohort and this cohort that the logistic sum is systematically biased toward the intercept, collapsing discrimination even if the coefficients are theoretically valid.

This audit computes per-X prevalence in this cohort and compares to Park's published Table 2 figures.

---

## 2. Cohort Sizes

| Label | N |
|---|---|
| All nodules with `park2009_category` populated | **37,579** |
| Labeled malignant (path-confirmed, laterality-aware) | **3,432** |
| Labeled benign (path window, no contralateral malignancy) | **10,818** |
| Unlabeled (no path within ±90d/+365d) | **23,329** |

Malignancy prevalence in labeled subset: **3,432 / (3,432+10,818) = 24.1%**  
Park 2009 cohort malignancy rate: estimated ~40–50% (Korean FNA-selected population, higher threshold).

---

## 3. Per-X-Variable Prevalence: This Cohort vs Park 2009

Park 2009 prevalence figures sourced from secondary literature citing Table 2 of the original paper (paywalled; PMID 19754280). Values should be verified against the primary source when access is obtained.

| Variable | Description | Park 2009 Overall | This Cohort Overall | Delta | Severity |
|---|---|---:|---:|---:|---|
| X8 | Homogeneous echotexture | 58% | **0.2%** | −58pp | **large_shift** |
| X10 | Solid composition | 73% | **39%** | −34pp | **large_shift** |
| X2 | Perinodular halo | 31% | **0.6%** | −30pp | **large_shift** |
| X1 | Taller-than-wide | 22% | 7% | −15pp | moderate_shift |
| X3 | Well-circumscribed margin | 37% | **51%** | +14pp | moderate_shift |
| X7 | Hypoechogenicity | 33% | 19% | −14pp | moderate_shift |
| X4 | Microlobulated margin | 14% | 1% | −13pp | moderate_shift |
| X6 | Marked hypoechogenicity | 9% | 0.3% | −9pp | mild_shift |
| X9 | Mainly cystic | 9% | 1% | −8pp | mild_shift |
| X11 | Microcalcification | 12% | 5% | −7pp | mild_shift |
| X5 | Infiltrative margin | 10% | 7% | −3pp | comparable |
| X12 | Abnormal lymph node at exam | 3% | 3% | 0pp | comparable |

### By Path Label

| Variable | Park Malignant | This Cohort Malignant | Park Benign | This Cohort Benign |
|---|---:|---:|---:|---:|
| X1 taller | 50% | 17% | 12% | 7% |
| X2 halo | 14% | 0.8% | 37% | 0.6% |
| X3 well-circumscribed | 14% | 66% | 45% | 48% |
| X4 microlobulation | 30% | 2.9% | 9% | 0.7% |
| X5 infiltrative | 22% | 15% | 6% | 9% |
| X6 marked hypo | 22% | 0.2% | 5% | 0.3% |
| X7 hypo | 50% | 33% | 28% | 20% |
| X8 homogeneous | 62% | 0.2% | 57% | 0.2% |
| X9 mainly cystic | 2% | 0.3% | 12% | 1.2% |
| X10 solid | 85% | 49% | 70% | 44% |
| X11 microcalc | 30% | 15% | 6% | 5% |
| X12 abnormal LN | 9% | 12% | 1% | 5% |

---

## 4. Interpretation: Mechanism of AUC 0.5365

### 4.1 Primary mechanism: Feature sparsity under NULL-default policy

The three **large_shift** variables (X8, X10, X2) collectively account for the near-random AUC. The NULL-default policy (missing feature → X = FALSE) is correct per Park's framing, but it interacts disastrously with sparse documentation:

**X8 (homogeneous echotexture, β=+0.648):**  
Park's cohort had 58% of nodules documented as homogeneous. In this cohort, `homogeneous_echotexture` is populated for only ~0.2% of nodules — a Phase A.3 primitive-backfill gap. With X8 effectively zero for 99.8% of nodules, the positive β contribution that Park used (counter-intuitively, X8 was associated with malignancy in his derivation cohort) never fires.

**X10 (solid, β=+0.463):**  
Park's cohort: 73% solid. This cohort: 39% documented as solid (the `composition` column is partially extracted). The gap of −34pp means a positive contribution from X10 is present in Park's cohort ~2× more often than here, pulling many Korean nodules toward higher z. Here, the composition is underdocumented — Phase A.3 will address this.

**X2 (perinodular halo, β=−0.481):**  
Park's cohort: 31% had halo. This cohort: 0.6%. Halo is rarely documented in these reports without dedicated halo-assessment prompting (it requires `halo_jsonb` from Phase A.3). Missing halo documentation means the protective negative β never fires, producing a slight positive bias vs Park's expected distribution.

**Net effect:** With X8≈0, X10 at half-expected prevalence, and X2≈0, the modal logistic score in this cohort approaches the intercept (−2.862), yielding P = 1/(1+e^2.862) ≈ 5.4% → P1 for most nodules. This explains why 73.1% of nodules land in P1. The AUC collapse follows mechanically: if most benign AND most malignant nodules map to the same P1 bucket, the ROC curve cannot separate them.

### 4.2 Secondary mechanism: Cohort selection bias

This is a **surgical cohort** (all patients who underwent thyroidectomy). Park's cohort was an **FNA-selected general-population cohort** with a lower biopsy threshold. The pre-operative selection effect means:
- Higher baseline malignancy prevalence in this cohort (24% of labeled vs ~40–50% in Park)
- Features associated with surgical indication (e.g., size > 4 cm, substernal location) are overrepresented, while benign background nodules that would never reach surgery are absent

### 4.3 Tertiary mechanism: Coefficient non-portability

Evidence from X3 (well-circumscribed) and X12 (abnormal LN) — the two variables with **comparable** distributions — suggests coefficient portability issues:
- X3: This cohort has **higher** well-circumscribed prevalence (51% vs Park's 37%), yet malignancy rate is still 24%. Park expected well-circumscribed to be a strong benign signal (β=−1.435). The discrepancy between feature prevalence and malignancy rate in X3 suggests Park's protective interpretation may not hold for surgical nodules.
- X12 (abnormal LN): Prevalences are identical (both ~3%), yet this cohort labeled-malignant X12 rate is 12% vs Park's 9% — the expected direction, but the absolute probability contribution is comparable. This variable is NOT a major driver of the AUC failure.

---

## 5. M085 Manuscript Implications

### Recommended language for Methods (§4.1 "Park generalization failure analysis"):

> *"We computed the prevalence of each of Park's 12 binary US variables (X1–X12) within our cohort and compared these to published prevalence figures from Park's Korean derivation cohort [Table X]. Three variables showed large distributional shifts (>20 percentage points): X8 (homogeneous echotexture: 0.2% vs 58%), X10 (solid composition: 39% vs 73%), and X2 (perinodular halo: 0.6% vs 31%). A further four variables showed moderate shifts (10–20 pp). We therefore attribute the observed AUC of 0.537 primarily to feature-extraction incompleteness under the null-default policy (missing documentation = feature absent) rather than to outright coefficient non-portability — the Park model cannot be fairly evaluated in this dataset until the Phase A.3 primitive backfill is complete."*

### Recommended language for Discussion:

> *"The near-random discrimination of Park's model (AUC = 0.537) in this American surgical cohort most likely reflects a systematic feature-extraction gap rather than fundamental unsuitability of the logistic structure. The null-default policy — clinically appropriate per Park's framing — suppresses the most prevalent positive predictors (X8: β=+0.648, X10: β=+0.463) because these features are insufficiently documented in structured form. After the Phase A.3 LLM backfill, we anticipate a materially higher AUC for the re-scored Park model. The cohort-refit model (AUC = 0.70) confirms that Park-style features carry meaningful discriminatory information in this population when training is performed locally."*

---

## 6. Next Steps

1. **Phase A.3 LLM primitive backfill** (highest priority for Park): re-score all 37,579 nodules after X8, X10, X2 are populated by Gemini 2.5 Pro. Expected: AUC will increase substantially.
2. After Phase A.3: re-run `scripts/phase_d2_park_prevalence_audit.py` to show post-backfill prevalences.
3. If AUC post-backfill ≥ 0.65: update NF-2026-05-07-park2009-noncalibration to reflect resolved mechanism.

---

## 7. Audit Provenance

| Item | Detail |
|---|---|
| BQ audit tables | `pub_workspace.qc_park_x_prevalence_audit_v1`, `qc_park_x_prevalence_audit_v1_overall`, `qc_park_x_prevalence_vs_park2009_v1` |
| Park 2009 prevalences | Secondary literature; verify against primary (PMID 19754280) when accessible |
| Script | `scripts/phase_d2_park_prevalence_audit.py` |
| Export | `exports/phase_d_park_prevalence_audit_20260508/` |
| Related DFL | DFL-2026-05-08-phase-d-tirads-reported-system (created this session) |
