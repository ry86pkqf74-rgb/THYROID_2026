# M044 ETE Manuscript — Synthesis Summary & Flags

**Cohort lock:** strict-DTC v1.1, 2026-05-04 | n = 3,578 | path-proven events = 105
**Version A reviewed:** `M044_ETE_Revision_Package_v2.docx` (Claude — strengthened Discussion + response-to-reviewers + sensitivity analysis package; not a full manuscript)
**Version B reviewed:** `M044_Thyroid_Manuscript_FIXED_v2.{docx,md,pdf}` (ChatGPT — full IMRAD manuscript)
**Locked source materials cross-checked:** `M044_ETE_LaTeX_Tables.tex`, `M044_Thyroid_MainTables_FIXED_v2.tex`, `M044_ETE_eMethods_strict_dtc_v1_1.docx`, `M044_ETE_Expanded_Analysis_v1.md.docx`, `M044_Thyroid_Supplement_ResponsePackage_FIXED_v2.docx`, three Excel deliverables (Table 1 Demographics, Table 3 Primary Regression, Table 4 No/Negative Audit), Elicit CT imaging report PDF.

---

## 1. Key differences between Version A (Claude / Revision Package v2) and Version B (ChatGPT / FIXED_v2)

### What Version B (ChatGPT) does better
- **Complete IMRAD manuscript** (Title, Abstract, Introduction, Methods, Results, Discussion, Conclusion, References, Tables, Figure legends, Supplement outline). Version A is not a manuscript at all — it is a strengthened Discussion plus response-to-reviewers package.
- **Cleaner Methods section** with explicit cohort-lock language, prespecified endpoint hierarchy, covariate list, and a clear rationale for binary logistic regression vs. time-to-event.
- **Calmer, more measured tone** suitable for a Thyroid / JAMA Otolaryngol HNS submission. Avoids overclaiming.
- **Better integration of CT imaging into the Introduction** as setup for the pathology-anchored study, not just as a Discussion add-on.
- **Cleaner table presentation** (single Table 1, single Table 2 primary regression, one sensitivity-summary table, one no/negative audit table).

### What Version A (Claude / Revision Package v2) does better
- **Five-line confounded-comparator audit** for the no/negative ETE subgroup is more explicit and quantitative. It walks through (i) 8.1× follow-up differential, (ii) universal repeat surgery with longer median interval to second surgery, (iii) margin/RAI/AGES enrichment, (iv) atypical referral-pathway composition (≥2 surgeries 26.5%, Nx 23.5%), and (v) a zero-follow-up sensitivity that preserves the gross signal while leaving the no/negative contrast non-significant.
- **Quantitative power discussion** with explicit numbers: >90 % power for OR = 2.0 and ~70 % power for OR = 1.5 in the gross-vs-microscopic contrast; <30 % power for OR = 2.5 in the no/negative contrast.
- **EPV = 5.8 limitation** stated explicitly with four mitigators (the primary inference is the 2-df ETE contrast, quasi-separated strata are transparently dropped, sensitivity analyses with reduced covariate sets give similar estimates, the gross-vs-microscopic direction and significance are stable).
- **Zero-follow-up sensitivity numbers** are reported with results: aOR 1.79 (1.17–2.72), p = 0.007 with FU > 0; aOR 2.10 (1.30–3.39), p = 0.002 with FU ≥ 1 year. Version B mentions the analysis but does not report the result.
- **Stronger CT clinical-pathway framing**: the rule-in/rule-out asymmetry is connected to the AJCC 8 staging logic in a single coherent paragraph with the radiomics outlook (AUC 0.78–0.84).
- **More explicit RAI confounding-by-indication language** with the RAI-itself coefficient (aOR 3.72, p < 0.001) embedded in the narrative.

### Conflicts that needed resolution
- **No factual conflicts** between A and B on the locked headline numbers (1.77 / 1.15–2.71 / p = 0.009; 2.72 / 0.80–9.30 / p = 0.111; 1.93 / 1.27–2.92 / p = 0.002; etc.). Both are faithful to the locked deliverables.
- The two drafts differ in **emphasis and depth of the Discussion**. The synthesis below resolves this by adopting Version B as the structural backbone and grafting the Version A strengthened paragraphs into the Discussion (no/negative audit, power, EPV, zero-follow-up sensitivity, CT clinical pathway).
- **Audit follow-up IQR** for non-recurrent no/negative patients differs between sources by a small amount — see Flags below.

---

## 2. Remaining flags (numerical / source discrepancies)

| # | Item | Excel deliverable (locked) | Manuscript / LaTeX / Revision Package | Resolution in final synthesis |
|---|------|----------------------------|---------------------------------------|--------------------------------|
| 1 | No/negative audit, follow-up IQR, non-recurred (n = 64) | 1.20 (0.00–**5.91**) | 1.20 (0.00–**5.89**) | Final manuscript and Tables use **1.20 (0.00–5.89)** to match the LaTeX source and both prior drafts. The 0.02-year (~7-day) discrepancy in the Table 4 Excel should be reconciled in the dataset; flagged here for the analyst. |
| 2 | "≥2 surgeries" overall row in Table 1 | 539 (15.1 %), p = 0.029 | 539 (15.1 %), p = 0.029 | Consistent. Use as in source. |
| 3 | AGES median for recurred vs non-recurred no/negative ETE | Not in Table 4 audit Excel | 8.0 vs 5.9 (Revision Package); 8.00 (7.45–8.90) vs 5.90 (3.00–7.30) (Expanded Analysis Table 7) | Use the Expanded Analysis values (medians with IQR) in the Discussion narrative. |
| 4 | Crude no/neg vs micro OR | Crude row Table 3 sheet "Logistic regression" → 2.37 (0.72–7.84), p = 0.156 | Same | Consistent. |
| 5 | RAI-itself coefficient | +RAI sheet "Logistic regression" → 3.72 (2.45–5.64), p < 0.001 | Same | Consistent; cited verbatim. |
| 6 | Histology cells: High-grade DTC × no/negative ETE | 0 patients (Table 1 Demographics) | Both drafts | Consistent. Quasi-separation correctly handled. |
| 7 | Tumor-size complete-case drop | 6 dropped → primary N = 3,572 | Both drafts | Consistent. |
| 8 | Median FU among path-proven recurrences (no/neg) | 13.75 (9.68–13.94) y from Expanded Analysis Table 2 | Audit table reports composite-recurrence FU 9.68 (4.53–13.84) | Both numbers are correct for different denominators (path-proven vs composite). Final manuscript uses 9.68 (composite, n = 4) in the audit paragraph and notes the path-proven subset where relevant. |
| 9 | Imaging-only-unconfirmed n in no/neg ETE | 1 (Table 1) | 1 (Table 1) | Consistent. |
| 10 | Stage III count | 8 (Table 1 Demographics, p = 0.004) | Not displayed in either manuscript | Optional row; included in the eTable 1 expanded baseline. |

No claim in either draft conflicts with the locked Excel/eMethods/LaTeX sources beyond the small Item-1 follow-up-IQR discrepancy noted.

---

## 3. Source-of-truth tabulation (used to verify the final manuscript)

| Endpoint / Coefficient | Value | 95 % CI | p | Source |
|------------------------|-------|---------|---|--------|
| Strict-DTC cohort | 3,578 | – | – | eMethods §1; Table 1 cover |
| Path-proven events | 105 | – | – | eMethods §4.1; Table 3 cover |
| Crude gross vs micro | 2.68 | 1.80–3.99 | < 0.001 | Table 3 "Logistic regression" |
| **Primary gross vs micro (no RAI)** | **1.77** | **1.15–2.71** | **0.009** | Table 3 "Logistic regression" |
| Primary no/neg vs micro | 2.72 | 0.80–9.30 | 0.111 | Table 3 "Logistic regression" |
| ln(1 + size) | 1.93 | 1.27–2.92 | 0.002 | Table 3 |
| N1b vs N0 | 2.24 | 0.83–6.03 | 0.110 | Table 3 |
| FTC vs PTC | 0.31 | 0.14–0.71 | 0.006 | Table 3 |
| Lymphatic extensive vs missing | 2.45 | 0.92–6.52 | 0.073 | Table 3 |
| Vascular focal vs missing | 2.25 | 1.07–4.73 | 0.033 | Table 3 |
| +RAI gross vs micro | 1.40 | 0.90–2.16 | 0.136 | Table 3 sensitivity |
| RAI itself | 3.72 | 2.45–5.64 | < 0.001 | Table 3 sensitivity |
| +BRAF/TERT gross vs micro | 1.78 | 1.16–2.73 | 0.008 | Table 3 sensitivity |
| +LN topography gross vs micro | 1.99 | 1.31–3.02 | 0.001 | Expanded Analysis 3A; LaTeX eTable 1 |
| Central LN+ | 1.78 | 1.04–3.06 | 0.036 | Expanded Analysis 3A |
| Lateral LN+ | 1.85 | 1.05–3.24 | 0.033 | Expanded Analysis 3A |
| ETE × LN interaction | 0.87 | 0.59–1.28 | 0.485 | Expanded Analysis 3B |
| PTC-only gross vs micro | 1.97 | 1.26–3.07 | 0.003 | Expanded Analysis 3C |
| Composite-LVI gross vs micro | 2.04 | 1.35–3.09 | < 0.001 | Expanded Analysis 3D |
| FU > 0 sensitivity gross vs micro | 1.79 | 1.17–2.72 | 0.007 | Revision Package v2 §6 |
| FU ≥ 1 y sensitivity gross vs micro | 2.10 | 1.30–3.39 | 0.002 | Revision Package v2 §6 |

---

*Companion deliverables in this folder:*
- `M044_ETE_FINAL_Manuscript_v3.md` and `M044_ETE_FINAL_Manuscript_v3.docx` — the synthesized definitive manuscript.
- `M044_ETE_FINAL_Tables_v3.tex` — LaTeX-ready Tables 1–4.
- `M044_ETE_FINAL_Synthesis_Summary.md` — this document.
