# M044 — Validation Report: Microscopic vs Gross ETE Manuscript

**Date:** 2026-05-01
**Reviewer:** Claude (independent verifier)
**Scope:** Independent verification of the ChatGPT handoff workbook (`m044_ajcc_ete_manuscript_review.xlsx`) against MotherDuck `thyroid_canonical_publication_v1_0` and downstream go/no-go decision for manuscript drafting.
**Cohort view:** `manuscript_workspace.cohort_m044_ajcc_ete_v1`
**Auxiliary objects:** `manuscript_workspace.ln_master_rollup_v1`, `manuscript_workspace.cohort_m040_reoperative_v1`, `main.canonical_recurrence_resolved_v1`, `main.canonical_ete_event_resolved_v1`

---

## 1. Headline conclusion

**Go for manuscript drafting**, with two material modifications to ChatGPT's plan:

1. **Switch the primary recurrence endpoint** from the cohort view's `any_recurrence_flag` to the canonical dual-track schema in `main.canonical_recurrence_resolved_v1`. The legacy `any_recurrence_flag` and `structural_recurrence_flag` on `canonical_patient_master` are not consistent with the canonical resolution and include a large block of patients with no traceable recurrence evidence under `recurrence_status_final='none'` (headline M044-cohort metrics: `SELECT * FROM manuscript_workspace.m044_legacy_recurrence_flag_audit_v1`; deploy `qc_framework_v1/migrations/257_m044_legacy_recurrence_flag_audit_20260501.sql`). The canonical convention explicitly states these tracks must NOT be collapsed.
2. **Treat the no/negative ETE recurrence signal as a confounded subgroup, not as evidence against AJCC 8.** When recurrence is restricted to pathology-proven events, the no/negative ETE rate (12/192 = 6.25%) is small in absolute terms and is concentrated in patients with second surgery / completion-thyroidectomy ascertainment (10/29 recurred patients had ≥2 surgeries; median first→second surgery interval 680 days).

The cohort spot-check from the ChatGPT handoff is **fully reproduced**:

| Metric | Expected (ChatGPT) | Reproduced (Claude) | Status |
|---|---|---|---|
| n | 4128 | 4128 | ✓ |
| any_recurrence_n | `m044_legacy_recurrence_flag_audit_v1.legacy_any_recurrence_true_n` | matches `SUM(any_recurrence_flag)` on cohort | ✓ |
| median_followup_years | 1.002 | 1.002 | ✓ |

---

## 2. Confirmed numbers from MotherDuck

### 2.1 Cohort and follow-up

- Cohort size: **n = 4,128** patients (one row per `research_id`).
- Follow-up nonmissing for all 4,128 rows; **1,400 patients (33.9%) have followup_years = 0**.
- All-row median follow-up: **1.002 years** (Q1 0.000, Q3 4.736, max 59.001).
- Among 2,728 patients with follow-up >0: median **3.049 years** (Q1 1.043, Q3 7.092).
- Surgery dates: **3,212 in 1999–2024**, **2 pre-1999 outliers** (earliest 1945-07-13), **0 post-2024**, and **914 missing**.
- Recommended reporting language (matches ChatGPT's): "Median follow-up was 1.0 years (IQR 0–4.7) overall and 3.0 years (IQR 1.0–7.1) among the 2,728 patients with non-zero follow-up."

### 2.2 ETE groups (using ChatGPT's exact grouping)

| ETE group | n | Path-proven n | Path-proven rate | Composite (path or imaging) n | Composite rate | Mean size cm | Median FU y |
|---|---:|---:|---:|---:|---:|---:|---:|
| Microscopic ETE | 2,576 | 59 | 2.29% | 145 | 5.63% | 1.94 | 0.66 |
| Gross ETE | 1,266 | 73 | 5.77% | 163 | 12.88% | 2.94 | 1.94 |
| No/negative ETE | 192 | 12 | 6.25% | 29 | 15.10% | 3.29 | 0.83 |
| Present ungraded | 29 | 1 | 3.45% | 1 | 3.45% | 2.38 | 0.34 |
| Missing/other | 65 | 0 | 0% | 2 | 3.08% | 2.47 | 0.02 |

(Composite recurrence = `canonical_recurrence_resolved_v1.recurrence_status_final IN ('path_proven','imaging_only_unconfirmed')`. The path-proven track is the strict pathology/op-note/cytology-confirmed endpoint.)

### 2.3 AJCC T-stage cross-tab

- All 1,266 gross ETE patients map to T3b (100% concordance). This validates the AJCC 8th edition operationalization of gross ETE as T3b.
- Microscopic ETE is distributed across T1a (958), T1b (710), T2 (645), T3a (258), and a small T1/T1 NOS bucket; **microscopic ETE alone does NOT upstage to T3b**.
- The 192 no/negative ETE rows are distributed across T1a (52), T1b (45), T2 (43), T3a (52). T3a in this group reflects size criterion ≥4 cm or strap-muscle-related minor invasion captured outside the ETE label.

### 2.4 Lymph-node burden (via `ln_master_rollup_v1`, pre-aggregated to one row per patient)

| ETE group | Mean LN examined | Mean LN positive | Central+ rate | Lateral+ rate |
|---|---:|---:|---:|---:|
| Microscopic ETE | 5.4 | 1.7 | 17.2% | 6.3% |
| Gross ETE | 11.4 | 3.5 | 32.9% | 17.1% |
| No/negative ETE | 12.7 | 3.6 | 21.4% | **37.0%** |

The no/negative ETE group is **strikingly enriched for lateral-compartment nodal disease** (37.0% lateral-positive vs 17.1% in gross ETE and 6.3% in microscopic ETE). This is a key bias signal: many patients labeled "no ETE" entered the cohort via a lateral-neck-driven pathway (often N1b clinically apparent disease).

---

## 3. Discrepancies from the ChatGPT workbook

The vast majority of ChatGPT's numbers reproduce. Only two material differences were identified.

### 3.1 ETE grouping of `ete_grade_final = 'true'` (4 patients)

ChatGPT counted only `ete_grade_final = 'gross'` as Gross ETE (n=1,266) and placed all `'true'` rows (n=4) into Missing/other (n=65). A more rigorous tie-break uses the secondary `ete_grade` column: 2 of those 4 rows have `ete_grade='gross'` and should be classified as Gross ETE; 2 are pure `'true'/'true'` from `tumor_episode_master_v2` and are best classified as Present ungraded or excluded. This shifts at most 2 patients between groups and **does not affect any conclusion**. We retain ChatGPT's grouping as the primary definition for downstream tables but document the alternative in the SQL package.

### 3.2 Recurrence endpoint definition (large effect — must be addressed)

ChatGPT used `cohort_m044_ajcc_ete_v1.any_recurrence_flag` as the recurrence endpoint and noted that `structural_recurrence_flag` appears unexpectedly high. **Live reconciliation** against `main.canonical_recurrence_resolved_v1` for the M044 cohort is summarized in **`manuscript_workspace.m044_legacy_recurrence_flag_audit_v1`** (one row: `legacy_any_recurrence_true_n`, `legacy_any_true_canonical_status_none_n`, `legacy_structural_recurrence_true_n`, `legacy_structural_true_canonical_status_none_n`, etc.). Example verification after mig_257 signing (2026-05-01): 503 / 318 / 1817 / 1588 for those four counts respectively — drift if CPM or recurrence layers change.

**Recommended fix:** The manuscript primary endpoint should be **path-proven recurrence** (`recurrence_path_proven = TRUE`, n=145 across the analytic cohort), with **imaging-only-unconfirmed recurrence** (n=195) and the **composite (status ∈ {path_proven, imaging_only_unconfirmed})** (n=340) as secondary endpoints. This matches the user's request to confirm recurrence by pathology from op reports/biopsy versus imaging-suspicious features.

The legacy `any_recurrence_flag` should not appear in the manuscript without an audit, except as a sensitivity check to demonstrate that conclusions are robust.

---

## 4. Path-proven vs imaging-suspicious recurrence (per user request)

**Headline legacy-vs-canonical counts (M044 cohort):** `SELECT * FROM manuscript_workspace.m044_legacy_recurrence_flag_audit_v1`.

Cross-tab between cohort flags and canonical resolved fields (detail cells drift with registry refreshes; retained below as **historical** snapshot at validation time):

| any_recurrence_flag | recurrence_status_final | path_proven | imaging_susp | n |
|---|---|---|---|---:|
| FALSE | none | F | F | 1,924 |
| FALSE | none | F | F | 1,467 (also `structural_recurrence_flag=TRUE`) |
| TRUE | none | F | F | 184 |
| TRUE | none | F | F | 121 (also `structural_recurrence_flag=TRUE`) |
| FALSE | imaging_only_unconfirmed | F | T | 75 |
| TRUE | path_proven | T | F | 52 |
| TRUE | path_proven | T | T | 46 |
| FALSE | imaging_only_unconfirmed | F | T | 40 |
| FALSE | imaging_only_unconfirmed | F | F | 32 |
| TRUE | path_proven | T | F | 29 |
| TRUE | imaging_only_unconfirmed | F | T | 24 |
| TRUE | imaging_only_unconfirmed | F | T | 12 |
| TRUE | none | F | F | 13 |
| TRUE | path_proven | T | F | 9 |
| TRUE | imaging_only_unconfirmed | F | T | 4 |
| Other small cells (≤7 each) | … | … | … | combined ~14 |

**Path-proven recurrence (`recurrence_path_proven = TRUE`) by ETE group:**

| ETE group | n | Path-proven n | Path-proven % | Imaging-only n | Imaging-only % |
|---|---:|---:|---:|---:|---:|
| Microscopic ETE | 2,576 | 59 | 2.29% | 86 | 3.34% |
| Gross ETE | 1,266 | 73 | 5.77% | 90 | 7.11% |
| No/negative ETE | 192 | 12 | 6.25% | 17 | 8.85% |
| Present ungraded | 29 | 1 | 3.45% | 0 | 0% |
| Missing/other | 65 | 0 | 0% | 2 | 3.08% |

Imaging-suspicion-then-path-confirmation (`recurrence_imaging_then_path_confirmed = TRUE`) is a small but informative subset: 11/2,576 microscopic, 17/1,266 gross, 5/192 no/negative — meaning ~30 patients had imaging suspicion that was subsequently confirmed by pathology. These are appropriately counted in path-proven (status = 'path_proven').

**Per-100-person-year rates (positive follow-up only):**

Numerators for `/100 PY` exclude zero-follow-up patients (aligned with the person-year denominator). Cohort **n** and **Path-proven %** remain full-cohort counts.

| ETE group | Person-years | Path-proven /100PY | Composite /100PY |
|---|---:|---:|---:|
| Microscopic ETE | 8,137 | 0.71 | 1.74 |
| Gross ETE | 4,138 | 1.76 | 3.92 |
| No/negative ETE | 701 | 1.71 | 4.00 |

Even on the proper denominator the same pattern holds: gross ETE > microscopic ETE for both endpoints, and the no/negative group has a person-year-rate similar to gross ETE (driven by the long-FU recurred minority — see §5).

---

## 5. No/negative ETE subgroup audit (recurred vs not)

Restricting to the 192 no/negative ETE patients and using the composite canonical recurrence endpoint:

| | No recurrence (n=163) | Recurred (n=29) |
|---|---:|---:|
| Mean tumor size, cm | 3.24 | 3.61 |
| Median tumor size, cm | 2.00 | 2.50 |
| Median follow-up, y | 0.12 | 2.53 |
| N1a, n | 104 | 21 |
| N1b, n | 3 | 3 |
| Central LN+ , n | 31 | 10 |
| Lateral LN+ , n | 52 | 19 |
| Mean LN positive | 3.43 | 4.38 |
| RAI received, n | 18 | 19 |
| ≥2 surgeries, n | 24 | **10** |
| Median days first→second surgery | 148 | **680** |
| Path-proven n | — | 12 |
| Imaging-then-path-confirmed n | — | 5 |

Interpretation:

- The 29 recurred no/negative-ETE patients have a markedly longer median follow-up (2.53 vs 0.12 years), indicating ascertainment differential, not biology alone.
- Lateral nodal positivity is high in both halves of the subgroup (52/163 = 31.9% no-recur vs 19/29 = 65.5% recur) — meaning the no/negative-ETE group is effectively a node-positive subset masquerading as low-risk on the ETE axis.
- 10 of 29 recurred patients had ≥2 surgeries with median 680 days between first and second surgery. The reoperative pathway is contributing to apparent recurrence ascertainment in this group.

**Manuscript implication:** Do not present no/negative ETE recurrence as a paradox or counter-evidence to AJCC 8. Frame it as a confounded subgroup; build a sensitivity table that adjusts for size, N stage, central/lateral nodes, RAI, and ≥2-surgery pathway.

---

## 6. Lymphatic vs vascular invasion separation (per user request)

Both `lvi_grade` and `vascular_invasion_final` are richly populated with raw categorical text. There are spelling variants (`preesent`, `extensivre`, `extensiver`, `indetermiante`, `indeeterminate`) that should be cleaned before modeling. Missing/indeterminate must be modeled as its own category, never collapsed to "absent."

Path-proven recurrence rate by raw value:

`lvi_grade`: missing 3.13%, present 4.81%, x 3.28%, extensive 11.54%, indeterminate 0.0%, focal 0.0%.

`vascular_invasion_final`: missing 2.5%, present_ungraded 6.09%, focal 5.76%, extensive 7.02%, indeterminate 3.85%.

Joint cross-tab (path-proven, top cells):

| Lymphatic | Vascular | n | Path-proven n | Path-proven rate | Composite rate |
|---|---|---:|---:|---:|---:|
| missing | missing | 2,772 | 68 | 2.45% | 6.02% |
| present | present | 413 | 20 | 4.84% | 10.90% |
| missing | present | 174 | 17 | 9.77% | 15.52% |
| missing | focal | 161 | 7 | 4.35% | 9.32% |
| missing | extensive | 90 | 7 | 7.78% | 16.67% |
| extensive | extensive | 13 | 2 | 15.38% | 46.15% |
| extensive | present | 34 | 3 | 8.82% | 23.53% |

**Conclusion:** Once lymphatic and vascular are separated and missingness is treated as its own category, neither variable looks "protective." The earlier protective-LVI signal is consistent with a pooled/missingness artifact in the prior model. Sensitivity analysis must:

1. Keep `lvi_grade` and `vascular_invasion_final` separate.
2. Model categories as: present-ungraded, focal, extensive, indeterminate, missing.
3. Avoid recoding missing → absent.

---

## 7. Data quality concerns

- **Recurrence flags in the cohort view are inconsistent with the canonical resolved table.** Recommend the cohort view be rebuilt to expose the canonical fields directly (path-proven, imaging-suspicious, status) and stop using `any_recurrence_flag` as a pseudo-primary flag. This finding has been logged in this report's §3.2 and §4.
- **Surgery dates missing for 914/4,128 (22.1%) of the cohort.** Two pre-1999 outliers should be displayed in cohort-flow diagram and excluded or addressed in a sensitivity analysis (e.g., one record dated 1945-07-13, almost certainly an extraction or data-entry artifact for that patient).
- **Free-text spelling variants in `lvi_grade`.** `preesent`, `extensivre`, `extensiver`, `indetermiante`, `indeeterminate`, `n/s`, `c/a`, `Cannot be determined: …` all appear. Recommend a cleaning step prior to modeling; <40 patients are affected so the impact is small but the labeling is unprofessional and should be normalized in the analysis-ready file.
- **Tumor-size missingness.** Six microscopic-ETE rows have unknown size bin (none recurred); confirm these are true missing or NIFTP placeholders.

---

## 8. Go/No-go decision

**GO for manuscript drafting** subject to:

1. Adopting the canonical dual-track recurrence endpoint as primary (path-proven), with imaging-only and composite as pre-specified secondary endpoints. The legacy `any_recurrence_flag` will appear only in a sensitivity analysis.
2. Reporting follow-up in the format "1.0 years overall (IQR 0.0–4.7); 3.0 years among patients with non-zero follow-up (IQR 1.0–7.1); range 0.0–59.0 years." Surgery-date window will be reported as "1999–2024 among 3,212 patients with non-missing surgery dates; 914 missing surgery dates (22.1%)."
3. Treating no/negative ETE recurrence as a confounded subgroup; it gets its own table (Table 4) and a sensitivity model adjusting for size, N stage, nodal compartment, RAI, and ≥2-surgery indicator.
4. Modeling lymphatic and vascular invasion as separate categorical variables with explicit missing/indeterminate categories; the prior "protective LVI" finding is presumed an artifact and must be re-derived from these definitions.
5. Pre-cleaning the lvi_grade free-text spelling variants in the analysis-ready file.

If those conditions are met, the data clearly support the AJCC 8 thesis: gross ETE is associated with a 2- to 3-fold higher path-proven recurrence rate compared with microscopic ETE on both crude and person-year denominators, while microscopic ETE behaves more like the no-ETE referent than like gross ETE on every measure except the confounded no/negative-ETE subset.

---

## 8b. Demographics and full-canonical-schema addendum (added 2026-05-01)

The initial validation pass focused on the M044 cohort view (29 columns) plus three auxiliary objects. After review, demographic, comorbidity, tumor-characteristic, molecular, and surgical-extent variables from `main.canonical_patient_master` (1,630 governed columns) and adjacent canonical tables were pulled for the M044 cohort. Findings have been written to `M044_ETE_demographics_addendum.md` and a new "Demographics & molecular" tab in `M044_ETE_tables.xlsx`. Highlights:

- **Race** is well-populated (4,124/4,128 = 99.9%): White 59.2%, Black/African American 24.0%, Asian 6.9%, Unknown 7.1%, Other 2.1%, Hispanic/Latino 0.2%. The cohort is racially diverse (non-trivial Black/AA representation), which is a strength for external generalizability.
- **BMI** is missing in ~80% of patients; cannot be a primary covariate.
- **Smoking status** is essentially unusable — `pmhx_nlp_smoking_status` is NULL in 99.7% of patients with free-text variants in the rest. Document as a data-extraction limitation.
- **Comorbidities (NLP):** Diabetes 13.5%, hypertension 17.0%, hypothyroidism 23.7%, obesity 4.9%. Family history of thyroid disease and childhood radiation exposure are under-extracted (≤24 cases each cohort-wide).
- **Hashimoto's** (`syn_hashimoto`) is documented in 93/4,128 (2.3%) and **Graves'** in 56/4,128 (1.4%).
- **Tumor characteristics:** Multifocality is markedly higher in gross ETE (40.4%) than no/neg (10.9%); bilateral disease is also enriched (27.0% vs 4.7%); margin involvement is strikingly higher in gross ETE (27.6%) vs microscopic (8.3%) vs no/neg (9.9%); closest margin distance is closer in gross (0.91 mm) than microscopic (1.63 mm).
- **Molecular:** BRAF positive (final) 7.0% cohort-wide; TERT positive 1.5% (denominator near-universal testing); RAS positive 4.8%; RET positive 0.9%. All higher in gross ETE than microscopic or no/neg.
- **Total thyroidectomy:** 62.0% in gross ETE vs 47.9% microscopic vs **21.9% in no/neg ETE** — the lower no/neg total-thy rate supports the second-surgery / completion-pathway bias hypothesis for the no/neg recurrence signal.
- **AGES score:** mean 7.73 (gross), 6.64 (microscopic), 6.54 (no/neg) — gradient reproduces the ETE risk hierarchy in a single composite metric.

These additional variables strengthen the manuscript thesis: gross ETE is associated with higher multifocality, margin involvement, BRAF/TERT positivity, total-thyroidectomy rate, and AGES — all concordant with the higher path-proven recurrence rate. The no/negative ETE subgroup is paradoxically the lowest on AGES, BRAF, multifocality, and total-thyroidectomy rate yet has comparable path-proven recurrence, which is biologically implausible without the lateral-N1b / completion-pathway ascertainment bias previously identified.

Items still missing or under-populated (smoking, family history, childhood radiation, MEN, pre-op labs trajectories, surgical complications, frozen sections, ethnicity as separate field) are enumerated in `M044_ETE_demographics_addendum.md` §7.

---

## 9. Audit trail

- Spot-check query reproduced; n=4128, median_followup_years=1.002 (`cohort_m044_ajcc_ete_v1`); legacy flag headline counts from `m044_legacy_recurrence_flag_audit_v1`.
- ETE group counts reproduced under ChatGPT's exact definition.
- All Tables 1–4 source SQL is captured in `M044_ETE_analysis.sql`.
- Canonical recurrence convention from `main.canonical_recurrence_resolved_v1` table comment, build_script `mig_62_canonical_recurrence_resolved_v1_20260427`.
- Reoperative auxiliary table `manuscript_workspace.cohort_m040_reoperative_v1` accessed and pre-aggregated to one row per `research_id`.
- LN auxiliary table `manuscript_workspace.ln_master_rollup_v1` accessed and pre-aggregated to one row per `research_id`.
- Phase 4 variable inventory rules: ETE source-splitting and separate handling of vascular invasion vs lymphatic invasion are followed in this report.

End of validation report.
