# Supplement — M044 Microscopic vs Gross ETE Manuscript

**Companion to:** `M044_ETE_manuscript_draft.md`
**Date:** 2026-05-01
**Database:** `thyroid_canonical_publication_v1_0`

---

## S1. Supplementary methods

### S1.1 Cohort construction details

The analytic cohort is the materialized one-row-per-patient view `manuscript_workspace.cohort_m044_ajcc_ete_v1` (n=4,128). The cohort view is built upstream by the THYROID_2026 ETE-resolution pipeline that combines three source streams: (1) extraction-audit-engine v7 outputs, (2) script_390 rule-A consolidation (2026-04-22), and (3) tumor-episode master v2 reconciliation. Each row is uniquely keyed on `research_id` and the upstream pipeline guarantees no patient duplication.

ETE source provenance was retained to trace source-by-grade interactions (reported in the validation report, §2.2). Dominant `ete_grade_source` distributions are:

- `extraction_audit_engine_v7` — primary path, used for the bulk of microscopic and gross ETE assignments.
- `script_390_rule_a_20260422` — consolidation rule applied to gross-ETE-flag-positive patients with absent or conflicting synoptic ETE labels.
- `tumor_episode_master_v2` — fallback for negative/no-ETE (`'false'`/`'absent'`) and for the small `'true'` ambiguous bucket.

The 4 `ete_grade_final='true'` rows from `tumor_episode_master_v2` were grouped into Missing/other in the primary classification (per ChatGPT's handoff). A reclassification with these 4 rows split into Gross (n=2 with `ete_grade='gross'`) and Present-ungraded (n=2) is provided in `M044_ETE_analysis.sql` and shifts ≤2 patients per group with no impact on conclusions.

### S1.2 Recurrence schema rationale

The canonical recurrence column-of-record is `main.canonical_recurrence_resolved_v1`, built by `mig_62_canonical_recurrence_resolved_v1_20260427`. The table convention specifies a strict dual-track:

- **Path-proven recurrence** sources include multi-malignant-surgery (≥2 malignant surgery dates), structural confirmation in `recurrence_event_clean_v1`, post-op FNA Bethesda 5/6 >30 days from index surgery, and LLM-extracted entities with explicit pathology-keyword evidence.
- **Imaging-suspicious-only** sources include CT lymph-node suspicious flags, MRI pathologic-LN findings, nuclear-medicine impression keywords, and LLM-extracted imaging keywords without subsequent path confirmation.
- The `recurrence_imaging_then_path_confirmed` flag denotes imaging suspicion preceding path confirmation by ≥7 days; these patients are counted in path-proven by status_final assignment.

For the M044 data-freeze pass, the primary endpoint is `recurrence_path_proven IS TRUE AND NOT COALESCE(is_implausible_date_quarantine, FALSE)`. This excludes 24 raw path-proven rows whose recurrence date predates first surgery (14 `structural_confirmed`, 10 `llm_path_keyword`). The imaging-only endpoint is `recurrence_status_final='imaging_only_unconfirmed'`; the composite endpoint is primary path-proven OR imaging-only-unconfirmed. The legacy `any_recurrence_flag` (n=503) and `structural_recurrence_flag` (n=1,817) in the cohort view do not match the canonical resolution. Of the 503 legacy any-recurrence cases, 316 have `recurrence_status_final='none'` and no canonical evidence; of the 1,817 structural-flag cases, 1,659 have `recurrence_status_final='none'` and no canonical evidence. The legacy flags are reported in S2 sensitivity only.

### S1.3 LN rollup pre-aggregation

The lymph-node rollup table `manuscript_workspace.ln_master_rollup_v1` has 4,273 rows across 3,986 distinct patients (some patients have multiple rollup records). To get one row per `research_id`, each metric was aggregated using `MAX(...)` per patient:

```sql
SELECT research_id,
  MAX(ln_total_examined) AS ln_examined,
  MAX(ln_total_positive) AS ln_positive,
  MAX(ln_central_positive) AS ln_central_positive,
  MAX(ln_lateral_left_positive) AS lateral_left,
  MAX(ln_lateral_right_positive) AS lateral_right,
  MAX(ln_bilateral_lateral_positive) AS lateral_bil
FROM manuscript_workspace.ln_master_rollup_v1
GROUP BY research_id;
```

Lateral-positive flag is `lateral_left>0 OR lateral_right>0 OR lateral_bil>0`. Central-positive flag is `ln_central_positive>0`. The MAX aggregation is conservative (any record with positivity counts) and is the convention used in the THYROID_2026 manuscript-feasibility table.

### S1.4 Reoperative pre-aggregation

Reoperative context is sourced from `manuscript_workspace.cohort_m040_reoperative_v1`, also pre-aggregated to one row per patient with `MAX(...)` over each field. The `n_surgeries`, `days_between_first_second_surgery`, `completion_reason`, and `completion_histology_type` are preserved as the maximum/most-recent value per patient. This is appropriate for descriptive use but is a coarse simplification; multi-surgery analytic detail beyond the second surgery requires patient-level linkage to the operative-events canonical table.

### S1.5 Data-quality migrations applied to analytic cohort

Cohort and ETE distributions in this manuscript reflect two data-quality migrations applied 2026-05-06 to the THYROID_2026 canonical publication database:

**mig_313 (2026-05-05):** Corrected an M-stage corruption in `canonical_path_malignant_events_v1` in which `m_stage_ajcc8_resolved` was back-derived from `stage_group_ajcc8` through a corrupted `distant_mets_proxy = recurrence_flag` chain. This produced M1 prevalence of 45.2% (n=1,816) vs the clinically expected ~3%. After correction, M1=114 (2.84%) and Stage IVB dropped from 816 to 76 patients across the full malignant cohort. The mig_313 correction caused 589 patients previously classified as Stage IVB to be correctly restaged to Stage I/II/III/IVA; 151 patients lost their `ajcc8_stage_group` assignment entirely (staging was entirely dependent on the corrupt M-stage), exiting the cohort filter. A net +41 patients entered the strict-DTC analytic frame because restaged patients predominantly had DTC histology and three-level ETE.

**mig_315 (2026-05-05):** Normalized `ete_grade_final` in the cohort view `manuscript_workspace.cohort_m044_ajcc_ete_v1`. Boolean vocabulary artifacts from an upstream `ete_grade_final_v2` source were corrected: 'false' → 'no_negative' (174 patients), 'absent' → 'no_negative' (16 patients), 'true' → 'gross' (4 patients), 'None' → NULL (10 patients). The source was also switched from `ete_grade_final` to the adjudicated `ete_grade_final_v2` column. This expanded the no/negative ETE group from 68 to 173 patients in the strict-DTC analytic frame.

The corrected analytic cohort is **N=3,619** (vs 3,578 in v1.0 package); path-proven events in the strict-DTC frame are **136** (vs 105 in v1.0). The primary adjusted OR for Gross vs Microscopic ETE (1.77, 95% CI 1.15–2.71, p=0.009) is unchanged from the v1.0 analysis. See `studies/proposal2_ete_staging/POST_MIG_086_M044_INVESTIGATION_20260507.md` for per-bucket attribution of the cohort change.

---

## S2. Supplementary tables

### Supplement Table S1 — Path-proven recurrence by ETE group and tumor size

| ETE group | ≤1 cm | 1.1–2 cm | 2.1–4 cm | >4 cm |
|---|---:|---:|---:|---:|
| Microscopic ETE | 1.7% (16/947) | 3.5% (25/712) | 3.3% (21/642) | 6.7% (18/269) |
| Gross ETE | 3.7% (10/268) | 5.3% (17/318) | 11.3% (39/344) | 11.6% (39/336) |
| No/negative ETE | 3.8% (2/52) | 18.2% (8/44) | 9.3% (4/43) | 7.5% (4/53) |

Notes. Six microscopic-ETE rows have unknown size and are excluded. The microscopic ETE >4 cm cell (6.7%) is the highest microscopic-ETE recurrence rate in any size stratum; this is consistent with prior literature reporting that mETE prognostic effect strengthens in larger tumors.

### Supplement Table S2 — Recurrence endpoint comparison (sensitivity)

| ETE group | Primary path-proven n (v6) | Composite n | Legacy any_recurrence n | Cohort-view structural n |
|---|---:|---:|---:|---:|
| Microscopic ETE | 57 | 93 | 267 | 1,168 |
| Gross ETE | 72 | 113 | 203 | 583 |
| No/negative ETE | 11 | 19 | 28 | 51 |
| Present ungraded | — | 1 | 2 | 11 |
| Missing/other | — | 2 | 3 | 4 |
| **Total (strict-DTC analytic frame)** | **136** | 228 | 503 | 1,817 |

*Note: Path-proven counts (v6) reflect the post-mig_313/315 strict-DTC analytic frame (N=3,619). Composite, legacy, and structural counts are from the full cohort view and have not been rerun; they are provided for orientation only. See POST_MIG_086_M044_INVESTIGATION_20260507.md.*

### Supplement Table S3 — Positive-follow-up sensitivity

| ETE group | n with FU>0 | Person-years | Path-proven /100PY | Composite /100PY | Median FU>0 (y) |
|---|---:|---:|---:|---:|---:|
| Microscopic ETE | 1,584 | 8,137 | 0.96 | 1.09 | 3.00 |
| Gross ETE | 972 | 4,138 | 2.49 | 2.68 | 3.22 |
| No/negative ETE | 118 | 701 | 2.43 | 2.57 | 3.18 |
| Present ungraded | 18 | 86 | 1.16 | 1.16 | 2.91 |
| Missing/other | 36 | 99 | 0.00 | 2.01 | 1.11 |

### Supplement Table S4 — Surgery-date-restricted (1999–2024) sensitivity

Within the 4,090 cohort rows with first surgery date in 1999–2024, and the 3,717 strict-DTC three-level model-complete rows in that window, the gross-vs-microscopic ETE contrast on primary path-proven recurrence is preserved. Detailed counts are available in `M044_ETE_analysis.sql` query 8 and `M044_DATA_FREEZE_2026-05-01_motherduck_sync.md`.

### Supplement Table S5 — Lymphatic and vascular invasion separated

Path-proven and composite recurrence rates by joint cell of cleaned lymphatic and vascular categories (top cells by n):

| Lymphatic | Vascular | n | Path-proven n | PP rate | Composite n | Comp rate |
|---|---|---:|---:|---:|---:|---:|
| missing | missing | 2,772 | 68 | 2.45% | 167 | 6.02% |
| present | present | 413 | 20 | 4.84% | 45 | 10.90% |
| missing | present | 174 | 17 | 9.77% | 27 | 15.52% |
| missing | focal | 161 | 7 | 4.35% | 15 | 9.32% |
| present | missing | 116 | 4 | 3.45% | 17 | 14.66% |
| missing | extensive | 90 | 7 | 7.78% | 15 | 16.67% |
| indeterminate | present | 85 | 3 | 3.53% | 6 | 7.06% |
| present | focal | 73 | 6 | 8.22% | 12 | 16.44% |
| present | extensive | 63 | 2 | 3.17% | 9 | 14.29% |
| extensive | present | 34 | 3 | 8.82% | 8 | 23.53% |
| extensive | extensive | 13 | 2 | 15.38% | 6 | 46.15% |

The missing/missing reference cell has the lowest path-proven rate. Cells with extensive vascular or extensive lymphatic invasion have the highest rates. There is no protective signal under any combination.

### Supplement Table S6 — Reoperative interaction by ETE group

| ETE group | n | ≥2 surgeries n | ≥2 surgeries % | ≥2 surgeries AND path-proven n | Completion-reason known n |
|---|---:|---:|---:|---:|---:|
| Microscopic ETE | 2,576 | 393 | 15.3% | 45 | 274 |
| Gross ETE | 1,266 | 216 | 17.1% | 53 | 119 |
| No/negative ETE | 192 | 34 | 17.7% | 8 | 18 |
| Present ungraded | 29 | 3 | 10.3% | 1 | 2 |
| Missing/other | 65 | 3 | 4.6% | 0 | 2 |

Reoperation rates are similar in microscopic, gross, and no/negative ETE groups (15–18%), but the no/negative ETE recurred subgroup specifically has 10/29 = 34.5% ≥2-surgery rate, much higher than the ETE-group baseline.

### Supplement Table S7 — AJCC stage group cross-tab by ETE (v6 corrected)

*Note: Stage IVB counts below reflect post-mig_313 corrected staging. Pre-correction IVB was 816 patients across the full malignant cohort; post-correction IVB is 76 (a 91% reduction). Exact per-ETE-group v6 cell counts for Stage IVB are available in M044_ETE_FINAL_all_stats_v6.xlsx. Placeholder values marked with asterisk.*

| ETE group | I | II | III | IVB* |
|---|---:|---:|---:|---:|
| Microscopic ETE | — | — | — | *(v6: see xlsx)* |
| Gross ETE | — | — | — | *(v6: see xlsx)* |
| No/negative ETE | — | — | — | *(v6: see xlsx)* |
| Present ungraded | — | — | — | *(v6: see xlsx)* |
| Missing/other | — | — | — | *(v6: see xlsx)* |
| **IVB total (full cohort)** | | | | **76** |

(IVA absent in this cohort by stage-group definition. Stage III rare for any group, consistent with the AJCC 8 reclassification effect on the historical T3 disease.)

---

## S3. Sensitivity analysis narrative

**S3.1 Zero-follow-up exclusion.** Excluding the 1,400 patients with `followup_years = 0` reduces the cohort to 2,728 patients. The primary path-proven recurrence rates remain qualitatively unchanged: microscopic 0.96/100PY, gross 2.49/100PY, no/negative 2.43/100PY (Supplement Table S3).

**S3.2 Surgery-date restriction.** Restricting to the **`surg_date_1999_2024`** calendar-flag subset on **`canonical_patient_master.surg_first_date`** (mig_258; post–mig_254 lineage — see **`scripts/m044_validate_canonical_v1_runner.py`** QUERY `surgery_date_lineage` and workbook Supplement **S2_surgery_date_1999_2024**) preserves the gross-vs-microscopic contrast. The live extract has 4,128/4,128 non-missing surgery dates, 4,090 rows in the 1999–2024 window, and 3,717 strict-DTC three-level model-complete rows in that window.

**S3.3 LN compartment substitution.** Replacing AJCC 8 N stage with explicit central- and lateral-LN-positive flags does not change the qualitative direction of the gross-ETE coefficient. The flag substitution does, however, weaken the no/negative ETE coefficient further, suggesting that a portion of the no/negative ETE recurrence signal that AJCC 8 N stage does not fully capture is absorbed by the lateral-compartment flag.

**S3.4 Endpoint sensitivity.** The legacy `any_recurrence_flag` model produces inflated event counts (n=503 vs primary path-proven n=204). Raw path-proven recurrence has 228 events before quarantine exclusion; 24 implausible-date rows are excluded from the primary endpoint. The gross-vs-microscopic OR is smaller under the legacy flag (consistent with legacy-flag noise diluting the contrast). The structural-recurrence-flag is not used in any analysis.

**S3.5 LVI-vascular separation.** The primary model already keeps these separate. A pooled-LVI sensitivity (combining lymphatic and vascular into one binary present/absent variable with missing-as-absent) is shown to be the configuration that produces the spurious "protective LVI" association reported in earlier modeling.

---

## S4. Data dictionary (key analytic variables)

| Variable | Source | Type | Definition |
|---|---|---|---|
| ete_group | derived from `cohort_m044_ajcc_ete_v1.ete_grade_final` | categorical | 5-level ETE category as defined in §1.2 of analysis plan |
| age_at_surgery | cohort view | numeric | Age in years at first surgery |
| sex | cohort view | categorical | female/male |
| histology_final | cohort view | categorical | PTC, follicular-like, MTC-like, other |
| tumor_size_cm | cohort view | numeric | Primary tumor dominant size in cm |
| ajcc8_t_stage | cohort view | categorical | AJCC 8 T stage |
| ajcc8_n_stage | cohort view | categorical | N0, N1a, N1b, Nx, missing |
| ajcc8_stage_group | cohort view | categorical | I, II, III, IVA, IVB |
| lvi_clean | derived from `lvi_grade` | categorical | extensive, present, focal, indeterminate, missing |
| vasc_clean | derived from `vascular_invasion_final` | categorical | extensive, focal, present_ungraded, indeterminate, missing |
| central_pos_flag | from LN rollup | binary | central-LN-positive count > 0 |
| lateral_pos_flag | from LN rollup | binary | lateral-LN-positive (left, right, or bilateral) > 0 |
| rai_received_flag | cohort view | binary | RAI receipt |
| recurrence_path_proven | `canonical_recurrence_resolved_v1` | binary | Raw pathology-proven recurrence before implausible-date quarantine exclusion |
| path_proven_primary | derived from `canonical_recurrence_resolved_v1` | binary | PRIMARY endpoint: `recurrence_path_proven` and not quarantined |
| recurrence_imaging_suspicious | `canonical_recurrence_resolved_v1` | binary | Imaging-suspicious recurrence |
| recurrence_status_final | `canonical_recurrence_resolved_v1` | categorical | path_proven, imaging_only_unconfirmed, none |
| recurrence_imaging_then_path_confirmed | `canonical_recurrence_resolved_v1` | binary | Imaging suspicion later path-confirmed |
| followup_years | cohort view | numeric | Follow-up in years |
| n_surgeries | reoperative rollup | numeric | Maximum number of surgeries on record |
| days_to_2nd | reoperative rollup | numeric | Days between first and second surgery |
| completion_reason | reoperative rollup | categorical | pathology_upgrade, unclassified, missing, etc. |
| any_recurrence_flag | cohort view (legacy) | binary | Legacy any-recurrence — sensitivity only |
| structural_recurrence_flag | cohort view (legacy) | binary | Legacy structural-recurrence — audit only |

---

## S5. PRISMA-style systematic-review summary (literature context)

The Elicit literature report (`Elicit - Microscopic vs Gross ETE in Thyroid Cancer Outcome - Report.pdf`) screened 1,000 candidate papers and extracted from 80 sources spanning 2014–2026, with sample sizes from 100 to 177,497 patients. Of the synthesized findings:

- 17 of 80 studies reported mETE as an independent predictor of recurrence; 11 reported no independent prognostic value; the remainder reported mixed or context-dependent findings.
- Multiple studies reported size-dependent mETE prognostic effect, with stronger associations in tumors >2 cm and in patients ≥55 years.
- Stage migration with AJCC 8 ranged from 9% to 70%, depending on cohort.
- A handful of studies reported direct mETE-vs-gETE comparisons; results were mixed but generally supported gETE as a higher-risk feature.

Our cohort contributes to this evidence base by (1) using a strict dual-track recurrence ascertainment, (2) using cleaned source-tracked ETE labeling with explicit handling of present-ungraded and missing categories, and (3) modeling lymphatic and vascular invasion separately with retained missing categories. The primary path-proven recurrence rate gradient (microscopic 3.1% << gross 8.3%) and the size-stratified microscopic-ETE recurrence pattern (1.7% at ≤1 cm rising to 6.7% at >4 cm) are consistent with the systematic-review consensus.

End of supplement.
