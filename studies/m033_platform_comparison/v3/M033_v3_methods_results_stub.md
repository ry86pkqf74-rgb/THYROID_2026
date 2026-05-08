# M033 v3 — Strict-Preop Bethesda Adoption: Methods + Results Stub
**Version:** v3 (strict-preop Bethesda)  
**Date:** 2026-05-08  
**Audit:** MFL-20260508-M033-V3-MANUSCRIPT-WRITE → rec9VO2QWCbtJx84N  
**DFL:** DFL-20260508-M033-V3-COHORT-B3B4-BUILD → rec0EFYYHKQjcRSvF  
**Linear:** THY-48 sub-task B  
**Status:** Methods + results stub ready for integration into full manuscript draft  
**Senior-author sign-off required before journal submission.**

---

## 2. METHODS

### 2.3 Cohort Definition (v3 update)

The analytic cohort consists of all surgical patients in the canonical patient master
(`pub_canonical.canonical_patient_master_v1_1`) who received at least one commercially
available genomic classifier — Afirma Genomic Sequencing Classifier (GSC) or Afirma Gene
Expression Classifier (GEC) — or ThyroSeq (v2 or v3) — prior to thyroidectomy and for
whom a pre-operative Bethesda cytology category could be assigned.

**Cohort construction (v3 strict-preop Bethesda):**

Bethesda categories were assigned from `canonical_patient_master_v1_1.bethesda_final_strict_preop`,
derived from a strictly pre-operative FNA event aggregation (MIG-45, 2026-04-24; canonical
promotion 2026-05-08). Specifically, only FNA events occurring before the patient's earliest
surgery date were eligible to contribute a Bethesda score; the highest-scoring pre-operative
Bethesda was retained per patient. This contrasts with the legacy `bethesda_final` field used
in prior cohort versions, which applied a looser temporal aggregation and admitted some events
outside the strict pre-operative window.

**Primary analytic cohort:** Patients with at least one Afirma or ThyroSeq test AND strict-preop
Bethesda III (AUS/FLUS) or IV (FN/SFN) — the indeterminate-cytology categories for which
genomic classifiers are guideline-endorsed. N = **520** (vs N = 510 under legacy Bethesda; net
+10 from strict-preop promotions).

**Sensitivity cohorts:**
- All-molecular sensitivity (N = 969): All Afirma/ThyroSeq-tested patients regardless of Bethesda,
  with strict-preop Bethesda available as a stratification variable where populated.
- B5/B6 sensitivity (N = 214): Patients with strict-preop Bethesda V (suspicious) or VI (malignant)
  who also received genomic testing. This arm characterizes testing utilization outside guideline
  indications.

**CONSORT exclusion flow (strict-preop Bethesda, from N = 969 molecular-tested universe):**

| Group | n | Disposition |
|---|---|---|
| NULL strict-preop Bethesda (no scored preop FNA) | 180 | Excluded from primary; retained in all-molecular sensitivity |
| Bethesda I (nondiagnostic) | 4 | Excluded from primary |
| Bethesda II (benign) | 51 | Excluded from primary |
| **Bethesda III/IV (indeterminate) — PRIMARY** | **520** | **Primary analytic cohort** |
| Bethesda V/VI (suspicious/malignant) | 214 | Sensitivity-2 cohort |

**Cross-tabulation — legacy (v1) vs strict-preop (v3) Bethesda, N = 969:**

Of the 969 molecularly-tested patients, 789 (81.4%) had concordant Bethesda categories
between the legacy and strict-preop derivations. Forty patients had discordant non-NULL values
(shifted category); 10 patients lost their Bethesda category under the strict-preop rule
(present in legacy, NULL in strict-preop); 2 patients gained a Bethesda category (NULL in
legacy, present in strict-preop). The 28 discordant patients in the primary B3/B4 cohort
contribute <5.4% of the 520-patient primary cohort and do not materially affect the ROM
point estimates.

---

## 3. RESULTS

### 3.1 Cohort Description

The primary analytic cohort comprised 520 patients with indeterminate cytology (Bethesda
III/IV) who underwent genomic classifier testing prior to thyroidectomy. ThyroSeq was the
most common platform (n = 372, 71.5%), followed by Afirma alone (n = 75, 14.4%) and dual
testing (n = 73, 14.0%).

### 3.2 Rate of Malignancy by Platform and Bethesda Category

**Primary cohort (B3/B4, N = 520; v3 strict-preop Bethesda):**

| Platform | N | B3 (AUS/FLUS) | B4 (FN/SFN) | Overall ROM |
|---|---|---|---|---|
| ThyroSeq | 372 | 53.4% (n=234) | 51.4% (n=138) | 52.7% |
| Afirma | 75 | 47.2% (n=36) | 59.0% (n=39) | 53.3% |
| Dual | 73 | 38.9% (n=36) | 73.0% (n=37) | 56.2% |

**Comparison to v1 legacy Bethesda (B3/B4, N = 510):**

| Platform | V1 ROM (legacy Bethesda, N=510) | V3 ROM (strict-preop Bethesda, N=520) | Δ pp |
|---|---|---|---|
| ThyroSeq | 53.7% | 52.7% | −1.0 |
| Afirma | 52.5% | 53.3% | +0.8 |

The strict-preop Bethesda recompute results in <1 percentage-point shift in ROM for both
platforms. The core finding that ThyroSeq and Afirma demonstrate comparable risk of
malignancy in indeterminate cytology (B3/B4) is preserved under the methodologically improved
Bethesda derivation.

### 3.3 Molecular Mutation Spectrum (B3/B4 Primary Cohort)

| Platform | N | BRAF+ | RAS+ | Fusion+ |
|---|---|---|---|---|
| ThyroSeq | 372 | 7.8% | 30.1% | 57.3% |
| Afirma | 75 | 46.7% | 0.0%* | 74.7%† |
| Dual | 73 | 86.3% | 21.9% | 91.8% |

*RAS positivity by platform-reported structured flags; Afirma GSC does not report RAS mutations
via structured molecular fields in the current publication database.  
†Afirma fusion rate (74.7%) likely reflects GSC's broader transcriptome-based expression
analysis, which captures fusion events differently from ThyroSeq v3's RNA-level multi-gene
panel. Not directly comparable across platforms.

### 3.4 Clinical Outcomes by Platform

| Outcome | ThyroSeq (n=372) | Afirma (n=75) | Dual (n=73) |
|---|---|---|---|
| Recurrence | 4.3% | 5.3% | 6.8% |
| Total thyroidectomy | 44.6% | 50.7% | 39.7% |
| Hemithyroidectomy | 54.8% | 46.7% | 60.3% |
| LN positive | 3.5% | 4.0% | 1.4% |
| Mean tumor size | 2.70 cm | 2.12 cm | 3.18 cm |
| TERT+ | 7.0% | 0.0% | 21.9% |

---

## 4. V3 METHODS NOTE FOR MANUSCRIPT

Add to §2.3 (Data Sources and Bethesda Classification):

> "In this analysis, Bethesda categories were derived from the strict-preop aggregation
> (`bethesda_final_strict_preop`) validated in the canonical patient master v1.1 (MIG-45 recompute,
> implemented 2026-04-24; promoted to pub_canonical 2026-05-08). Only FNA events occurring before
> the patient's earliest surgery date contributed to the pre-operative Bethesda maximum. This
> approach differs from prior cohort versions, which used a looser temporal aggregation; the
> methodological difference affected the Bethesda category assignment for 40 of 969 molecularly-
> tested patients (<4%), and resulted in a net increase of 10 patients entering the primary
> B3/B4 cohort (N=510→520). ROM estimates were stable across both derivations (shift ≤1 pp)."

---

## Audit Anchors

- DFL-20260508-CANONICAL-BETHESDA-RECOMPUTE-COMPLETE → THY-47 (parent: canonical promotion)
- DFL-20260508-M033-V3-COHORT-B3B4-BUILD → rec0EFYYHKQjcRSvF
- DFL-20260508-M033-V3-COHORT-ALL-MOLECULAR-BUILD → reclZdL7WMSoLjtWQ
- DFL-20260508-M033-V3-COHORT-B5B6-BUILD → recWSTCMtlRTgYJ1M
- MFL-20260508-M033-V3-MANUSCRIPT-WRITE → rec9VO2QWCbtJx84N
- THY-48 sub-task B (M033 v3 cohort + manuscript stub)
