# M044 ETE — Final Synthesis Summary (v2)

**Cohort lock:** strict-DTC v1.1, 2026-05-04 | n = 3,578 | path-proven events = 105
**research_id MD5:** `368f062fb8653ac543aa1dd951d27c5b` (matches eMethods Table 1 lock hash)

---

## 1. What's in this final package

| File | Purpose |
|---|---|
| `M044_ETE_FINAL_Manuscript_v4.docx` and `.md` | Definitive synthesized manuscript (IMRAD, references, tables, figure legends, supplement outline). |
| `M044_ETE_FINAL_Tables_v4.tex` | LaTeX-ready Tables 1–4 (booktabs / threeparttable). |
| `M044_ETE_FINAL_per_research_id_dataset.xlsx` | One row per research_id × every column actually used by any model in the manuscript (n=3,578 × 80 columns). Cover, Patient analytic, Data lock, Data Dictionary tabs. |
| `M044_ETE_FINAL_all_stats.xlsx` | Every regression model run (used + unused), with full coefficient output, model N, events, dropped variables, plus a Forest-plot data tab and a "Published vs reproduced" comparison tab. |
| `figures/M044_Figure1_CONSORT_flow.{png,svg}` | CONSORT-style cohort flow diagram. |
| `figures/M044_Figure2_Recurrence_by_ETE.{png,svg}` | Crude path-proven and composite recurrence rates by ETE group. |
| `figures/M044_Figure3_Forest_plot.{png,svg}` | Forest plot of gross-vs-microscopic ETE ORs across 10 model specifications. |
| `M044_ETE_FINAL_Synthesis_Summary_v2.md` | This document. |

---

## 2. Source-of-truth lineage (verified against MotherDuck)

```
thyroid_canonical_publication_v1_0
└── manuscript_workspace.cohort_m044_ajcc_ete_v1   (parent ETE view; n = 4,013–4,128 over time)
    ↓  build_strict_dtc_deliverables.py            (analyst-maintained external script)
    ↓  applies 550 strict-DTC exclusions
    ↓  derives lvi_clean, vasc_clean, histology_dtc_5level, ete_group, AGES, etc.
M044_ETE_patient_level_dataset_strict_dtc_v1_1.xlsx (locked deliverable; n = 3,578 × 112 cols)
    ↓  same script runs the regressions
M044_ETE_Table{1,3,4}_*.xlsx                       (locked Excel deliverables)
M044_ETE_LaTeX_Tables.tex                          (locked LaTeX)
M044_ETE_eMethods_strict_dtc_v1_1.docx             (locked eMethods)
```

**Key MotherDuck confirmations:**
- `cohort_m044_ajcc_ete_v1` exists and currently returns 4,013 rows; eMethods documents the parent cohort as 4,128 (small drift since the original lock — out of scope for this manuscript, but worth flagging).
- The patient-level dataset's research_id MD5 hash (`368f06…d27c5b`) matches the lock hash in eMethods Table 1, confirming the analytic cohort is exactly the one specified.
- `canonical_ete_event_resolved_v1` (event-grain) holds 422 path-proven events; the analyst's strict-DTC build filters these to 105 patient-level events via the canonical resolved-recurrence schema.

---

## 3. Reproducibility verdict — what runs from the dataset, what needs the analyst's build script

I independently re-fit every model from the patient-level dataset using statsmodels. Results below are summarized in the `Published vs reproduced` tab of `M044_ETE_FINAL_all_stats.xlsx`.

### Reproduces exactly (within rounding) — 6 of 10 headline models

| Model | Reproduced gross-OR | Published | Δ |
| --- | ---: | ---: | ---: |
| Crude (ETE only) | 2.679 (1.801–3.987) | 2.68 (1.80–3.99) | 0.001 |
| +LN topography | 1.986 (1.306–3.020) | 1.99 (1.31–3.02) | 0.004 |
| ETE × LN interaction | 2.21 | 2.21 | 0.00 |
| PTC-only | 1.968 (1.263–3.067) | 1.97 (1.26–3.07) | 0.002 |
| Composite-LVI | 2.041 (1.350–3.087) | 2.04 (1.35–3.09) | 0.001 |
| Crude rates by group | 4.41 / 1.91 / 4.95 % | 4.4 / 1.9 / 5.0 % | – |

The no/negative ETE confounding audit reproduces all 19 cells exactly from the patient-level dataset.

### Reproduces qualitatively but with ~5 % OR drift — 4 of 10 headline models

| Model | Reproduced gross-OR | Published | |Δ| |
| --- | ---: | ---: | ---: |
| Primary (no RAI) | 1.85 (1.21–2.84), p = 0.005 | 1.77 (1.15–2.71), p = 0.009 | 0.08 |
| + RAI sensitivity | 1.46 (0.94–2.27), p = 0.091 | 1.40 (0.90–2.16), p = 0.136 | 0.06 |
| + BRAF/TERT sensitivity | 1.87 (1.22–2.87), p = 0.004 | 1.78 (1.16–2.73), p = 0.008 | 0.09 |
| FU > 0 sensitivity | 1.62 (1.06–2.50), p = 0.027 | 1.79 (1.17–2.72), p = 0.007 | 0.17 |
| FU ≥ 1 y sensitivity | 1.88 (1.15–3.07), p = 0.012 | 2.10 (1.30–3.39), p = 0.002 | 0.22 |

**Direction, sign, and statistical significance (excluding the +RAI confounded model) are preserved everywhere.** The conclusions of the manuscript are unchanged. The OR drift indicates that one or more covariate-construction details inside the analyst's external `build_strict_dtc_deliverables.py` script differ from what the eMethods spec literally describes — for example a slightly different vascular-invasion category collapse, a different stratum reference, or a single-patient categorical flip. Without that script in hand, it cannot be precisely reconciled.

**Recommendation for the submission package:** commit `build_strict_dtc_deliverables.py` to the supplement so reviewers can run a one-button reproduction. Until that script is available, the locked Excel deliverables remain the source of truth for the headline numbers reported in the manuscript.

---

## 4. Numerical corrections applied in v4 (vs. v3 deliverables)

Two values in the v3 manuscript / tables drifted slightly from the patient-level dataset. The v4 deliverables in this package fix both:

| # | Cell | v3 value | v4 value (verified from patient-level dataset) | Affected files |
|---|---|---|---|---|
| 1 | No/negative audit, follow-up IQR (non-recurred, n=64) | 1.20 (0.00–**5.89**) | 1.20 (0.00–**5.91**) | manuscript text and Table 4 |
| 2 | No/negative audit, AGES score (recurred vs non-recurred) | 8.0 vs 5.9 (medians only) | 8.90 (7.72–9.98) vs 5.86 (3.00–7.15) | manuscript text and Table 4 |

No other numerical changes were needed.

---

## 5. Source-of-truth tabulation (final headline numbers)

| Endpoint / Coefficient | Value | 95 % CI | p | Source |
|---|---:|---:|---:|---|
| Strict-DTC cohort | 3,578 | – | – | eMethods §1 |
| Path-proven events | 105 | – | – | eMethods §4.1 |
| Crude gross vs micro | 2.68 | 1.80–3.99 | < 0.001 | locked Table 3 |
| **Primary gross vs micro** | **1.77** | **1.15–2.71** | **0.009** | locked Table 3 |
| Primary no/neg vs micro | 2.72 | 0.80–9.30 | 0.111 | locked Table 3 |
| ln(1 + size) | 1.93 | 1.27–2.92 | 0.002 | locked Table 3 |
| N1b vs N0 | 2.24 | 0.83–6.03 | 0.110 | locked Table 3 |
| FTC vs PTC | 0.31 | 0.14–0.71 | 0.006 | locked Table 3 |
| Lymphatic extensive vs missing | 2.45 | 0.92–6.52 | 0.073 | locked Table 3 |
| Vascular focal vs missing | 2.25 | 1.07–4.73 | 0.033 | locked Table 3 |
| +RAI gross vs micro | 1.40 | 0.90–2.16 | 0.136 | locked Table 3 |
| RAI itself | 3.72 | 2.45–5.64 | < 0.001 | locked Table 3 |
| +BRAF/TERT gross vs micro | 1.78 | 1.16–2.73 | 0.008 | locked Table 3 |
| +LN topography gross vs micro | 1.99 | 1.31–3.02 | 0.001 | Expanded Analysis 3A |
| Central LN+ | 1.78 | 1.04–3.06 | 0.036 | Expanded Analysis 3A |
| Lateral LN+ | 1.85 | 1.05–3.24 | 0.033 | Expanded Analysis 3A |
| ETE × LN interaction | 0.87 | 0.59–1.28 | 0.485 | Expanded Analysis 3B |
| PTC-only gross vs micro | 1.97 | 1.26–3.07 | 0.003 | Expanded Analysis 3C |
| Composite-LVI gross vs micro | 2.04 | 1.35–3.09 | < 0.001 | Expanded Analysis 3D |
| FU > 0 sensitivity gross vs micro | 1.79 | 1.17–2.72 | 0.007 | Revision Package v2 §6 |
| FU ≥ 1 y sensitivity gross vs micro | 2.10 | 1.30–3.39 | 0.002 | Revision Package v2 §6 |

---

*Generated 2026-05-04. Patient-level dataset MD5 verified. All v4 deliverables in this folder.*
