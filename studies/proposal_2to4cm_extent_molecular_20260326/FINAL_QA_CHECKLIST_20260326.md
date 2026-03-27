# Final pre-submission QA checklist — 2026-03-26

**Scope:** `studies/proposal_2to4cm_extent_molecular_20260326/` — `manuscript_submission_v1.md`, `abstract_structured_v1.md`, figure package, references.

---

## 1. Numeric claims vs `CLAIM_SOURCE_LEDGER.md`

| Claim | Manuscript / abstract | Ledger | Result |
|-------|----------------------|--------|--------|
| Primary N | 558 | 558 | **PASS** |
| Lobectomy / total | 238 / 320 | same | **PASS** |
| Initial total % | 57.3% (320/558) | same | **PASS** |
| Broad N; total; % | 635; 375; 59.1% | same | **PASS** |
| Pathology sensitivity N | 0 | 0 | **PASS** |
| Preop molecular | 20/558 (3.6%) | same | **PASS** |
| Completion | 0/238 (ever, 30/90/365) | table7 | **PASS** |
| Bethesda missing | 149/558 (26.7%) | noted in ledger | **PASS** |
| Mean age lob / tot | 56.6 / 52.9 yr | 56.59 / 52.93 | **PASS** (display rounding) |
| Female % | 191/238, 257/320, 80.3% | same | **PASS** |
| Mean Bethesda | 2.8 / 4.0 | table1 | **PASS** |
| Molecular % arms | 4.2% / 3.1% | 4.20% / 3.13% | **PASS** (display) |
| Concordance 2×2 | 9,11,0,0 n=20 | ledger | **PASS** |
| ThyroSeq / Afirma n | 8 / 12 | ledger | **PASS** |
| Parsimonious ORs | 2.743, 0.986, 0.974, 0.606 | same CIs | **PASS** |
| Parsimonious p | 1.74×10⁻⁶, 0.026, ~0.91, 0.29 | 1.74e-06, 0.0257, 0.905, 0.295 | **PASS** (display rounding) |
| Extended bilateral / TIRADS | 2.005, 0.958; p 0.0023, 0.68 | 0.00229, 0.684 | **PASS** (display) |
| Broad ORs / p | 2.765, 0.984; ~2.6×10⁻⁷, 0.0053 | 2.56e-07; 0.00525 | **PASS** (display) |
| Univariable p | age ~0.007; sex 1.0; Bethesda ~6×10⁻⁷; mol 0.66; bilateral 0.048 | 0.00715, 1.0, 6.02e-07, 0.655, 0.0475 | **PASS** |

**Source files spot-aligned:** Cohort and OR claims trace to files named in manuscript (`patient_level_dataset.csv`, `table1_by_initial_extent.csv`, `univariable_tests.csv`, `logistic_*.csv`, `table6_*`, `table7_*`). **No re-read of every CSV cell** was required beyond ledger reconciliation.

---

## 2. Citations vs `references_working_20260326.md`

| In-text IDs | Present in `references_working` numbered list 1–12? | Result |
|-------------|------------------------------------------------------|--------|
| [1]–[10] | Yes (items **1.**–**10.**; former **11–12** retired) | **PASS** |

---

## 3. Figures vs `figure_legends_v1.md` and files on disk

| Callout | File | In “kept main” table? | File exists | Result |
|---------|------|------------------------|-------------|--------|
| Figure 1 | `fig_cohort_flow.png` | Yes | Yes | **PASS** |
| Figure 2 | `fig_forest_total_vs_lobectomy.png` | Yes | Yes | **PASS** |

Legend text for Figure 2 grammar corrected in this QA pass (“binary outcome” phrasing).

---

## 4. Required limitations themes

| Theme | Where addressed | Result |
|-------|-----------------|--------|
| Observational / no causal claims | Limitations bullet + Methods/Intro framing | **PASS** |
| Single database / generalizability | Limitations bullet | **PASS** |
| Bethesda / FNA–imaging / molecular missingness | Methods Missing data + Limitations bullet (explicit FNA/imaging linkage) | **PASS** |
| Pathology-sized sensitivity N=0 | Results + Limitations | **PASS** |
| Completion operationalization + missingness | Results + Discussion + **dedicated Limitations bullet** | **PASS** |
| Tiny molecular subset (n=20) | Results + Limitations | **PASS** |

---

## 5. Causal wording pass

| Check | Result |
|-------|--------|
| Removed / softened “influenced by,” “effect sizes,” “influence extent” in interpretive prose | **DONE** (Introduction, Discussion) |
| Retained explicit **negations** of causality where appropriate | **OK** (e.g., “does not estimate … causal effects”) |

---

## 6. Outstanding (non-numeric) items

| Item | Owner |
|------|--------|
| Complete **NEEDS AUTHOR CHECK** on refs 3–4, 6–12 | Authors |
| Replace placeholder text for ref **12** before leaning on pooled completion statistics | Authors |
| Figure 1 publication relabel (truncated export) | Authors (`AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`) |

---

**Numeric / internal-consistency QA:** **PASS**  
**External submission readiness:** **CONDITIONAL** (bibliography + optional Figure 1 production)
