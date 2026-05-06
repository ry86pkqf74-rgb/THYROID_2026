---
type: feedback
description: Standing rule for reporting hypoparathyroidism (transient/permanent split) and hypocalcemia / RLN injury / VC paralysis (preop yes/no flag) in every thyroid-surgery manuscript.
---

# Manuscript standing rule: complication temporality reporting

**Rule (set by Logan, 2026-05-01):** Across every thyroid-surgery manuscript (M001–M083 and any future addition), perioperative complications must be reported with the following temporality structure. This rule supersedes the v1/v2 M038 Table 4 layout that reported a single "Confirmed hypoparathyroidism" row and gave no preop context for the voice-related and metabolic complications.

## The rule

| Complication | Reporting structure |
|---|---|
| **Hypoparathyroidism** | **Two distinct rows: transient (<6 months from surgery) and permanent (>6 months from surgery).** If the underlying canonicalization cannot classify a confirmed case as transient or permanent, label that case as "hypoparathyroidism postop all" with an explicit caveat in the table footnote. |
| **Hypocalcemia** | One row for "postop confirmed" (existing strict rollup) plus a "preop yes/no" flag row reporting how many patients had hypocalcemia present before surgery. |
| **RLN injury** | One row for "postop confirmed" plus a "preop yes/no" flag row. |
| **VC paralysis** | One row for "postop confirmed" plus a "preop yes/no" flag row. |
| **VC paresis** | Same pattern as VC paralysis (preop yes/no flag where data permits). |

## Why

**Hypoparathyroidism (transient vs permanent).** The clinical literature reports transient and permanent hypoparathyroidism as distinct outcomes with very different significance (transient is common after total thyroidectomy and self-resolves; permanent — defined by the field as persistence beyond ~6 months — is the meaningful long-term morbidity). Lumping them obscures the comparison. Reviewers expect the split.

**Hypocalcemia / RLN injury / VC paralysis (preop flag).** The "postop confirmed" count is ambiguous if a fraction of those patients had the condition before surgery. A preop yes/no flag lets the reader subtract preexisting cases from the postop attribution. This is especially important for RLN injury and VC paralysis after thyroid reoperation or large-cohort series with a heterogeneous referral mix.

## What the data currently supports (as of 2026-05-01, post-mig_252 / mig_253)

A column-review pass against `main.canonical_patient_master` shows the following encoding state:

| Complication | Postop strict rollup column | Preop / temporality columns available | Reportable now? |
|---|---|---|---|
| Hypoparathyroidism | `comp_hypoparathyroidism_confirmed` | `comp_hypoparathyroidism_transient` (BOOLEAN), `comp_hypoparathyroidism_permanent` (BOOLEAN), `comp_hypoparathyroidism_days_postop` (BIGINT), `comp_hypoparathyroidism_timing_window` (VARCHAR with `pre_surgery`/`0_30d`/`31_180d`/`181_365d`/`gt_365d`/`unknown`), `comp_hypoparathyroidism_preexisting` (BOOLEAN), `comp_hypoparathyroidism_new_postop` (BOOLEAN), `comp_hypopara_permanent_limitation_note` (VARCHAR) | **YES — full transient/permanent split.** Cohort-wide validation: 296 confirmed → 280 transient (94.6%) + 16 permanent (5.4%); zero unclassified. |
| Hypocalcemia | `comp_hypocalcemia_confirmed` | `comp_hypocalcemia_clinical_preexisting` (BOOLEAN; sparse), `comp_hypocalcemia_timing_window` (VARCHAR with `pre_surgery` value) | **YES — preop yes/no via `timing_window='pre_surgery'`.** The `_clinical_preexisting` BOOLEAN was empty in the M038 cohort; the timing_window value is the broader and more reliable signal. |
| RLN injury | `comp_rln_injury_confirmed` | `comp_rln_injury_preop` (BOOLEAN), `comp_rln_injury_preop_source` (VARCHAR; `source_laryngoscopy` / `none`), `comp_rln_injury_timing_window` (VARCHAR; values `0_30d`/`31_180d`/`181_365d` only — no `pre_surgery` bucket observed), `rln_temporality` (VARCHAR), `rln_permanent_flag` / `rln_transient_flag` (BOOLEAN) | **PARTIAL — preop yes/no from sparse laryngoscopy source.** `mig_080_h2_preop_rln_vc_columns` (2026-05-06) populated 119 patients from Snowflake `OPS_PREOP_LARYNGOSCOPY` via `AI_CLASSIFY` (8 RLN-preop positive). Report this as "preop RLN injury documented on preop laryngoscopy" and footnote that broader H&P/voice-note preop status is not yet encoded because the Snowflake note-search table lacks note dates. |
| VC paralysis | `comp_vc_paralysis_confirmed` | `comp_vc_paralysis_preop` (BOOLEAN), `comp_vc_paralysis_preop_source` (VARCHAR; `source_laryngoscopy` / `none`), `comp_vc_paralysis_timing_window` (VARCHAR; no `pre_surgery` bucket), `ops_preop_laryngoscopy` (source text), `mri_vocal_cords_normal` (outcome flag), `comp_voice_permanence_noted` / `comp_voice_resolution_noted` (outcome only) | **PARTIAL — preop yes/no from sparse laryngoscopy source.** `mig_080_h2_preop_rln_vc_columns` populated 119 patients from Snowflake `OPS_PREOP_LARYNGOSCOPY` via `AI_CLASSIFY` (3 VC-paralysis-preop positive). Report as laryngoscopy-documented preop VC paralysis; do not imply full preop voice-clinic coverage. |
| VC paresis | `comp_vc_paresis_confirmed` | Same pattern as VC paralysis | **PARTIAL — post-`mig_081` (2026-05-06).** BigQuery `canonical_patient_master` now has **1** `comp_vc_paresis_confirmed` patient (`research_id` **8616**) promoted from Snowflake `AI_CLASSIFY` on targeted contrast-language notes (`comp_vc_paresis_evidence_tier=2`). Cohort-wide prevalence remains negligible for Table 4; **preop yes/no is still not structured** — carry-forward `CF-VC-PARALYSIS-PREOP-FLAG` / paresis preop still open until MIG-002-class work extends to paresis. |

## Operationalization for every manuscript draft

For any manuscript whose Table N (complications) includes hypoparathyroidism, hypocalcemia, RLN injury, VC paresis, or VC paralysis:

**Step 1 — Hypoparathyroidism rows.** Always emit two rows in the table, even if the count of one is zero:

```sql
-- Postop transient hypoparathyroidism (<6mo)
SELECT COUNT(*) FILTER (WHERE comp_hypoparathyroidism_confirmed AND comp_hypoparathyroidism_transient)
FROM <cohort_view>;

-- Postop permanent hypoparathyroidism (>6mo)
SELECT COUNT(*) FILTER (WHERE comp_hypoparathyroidism_confirmed AND comp_hypoparathyroidism_permanent)
FROM <cohort_view>;
```

If a confirmed case is neither transient nor permanent (`AND NOT _transient AND NOT _permanent`), report it under a third "postop unclassified" row. Cohort-wide as of 2026-05-01 there are zero such cases.

**Step 2 — Hypocalcemia preop flag.** Add a "Hypocalcemia — present preop" row reporting the count of patients with `comp_hypocalcemia_timing_window = 'pre_surgery'` OR `comp_hypocalcemia_clinical_preexisting = TRUE`. Footnote that the postop-confirmed row excludes these preop cases.

**Step 3 — RLN injury / VC paralysis preop flag.** For BQ canonical analyses after `mig_080_h2_preop_rln_vc_columns` (2026-05-06), add a separate "preop documented on laryngoscopy" row using `comp_rln_injury_preop = TRUE` and/or `comp_vc_paralysis_preop = TRUE`. Footnote that these fields are sparse and source-limited to `OPS_PREOP_LARYNGOSCOPY`; broader H&P/voice-clinic preop status remains a future enhancement because the Snowflake note-search table used for H2 lacks note dates.

**Step 4 — Permanence-classification limitations.** If the `comp_hypopara_permanent_limitation_note` column is non-NULL for any case in the cohort, surface this in the Methods or Limitations section. Cohort-wide as of 2026-05-01: 14 cases have a limitation note (`followup_too_short_for_permanence_classification`, `reset_20260417:confirmed_duration_unknown`, `confirmed_hypopara_no_persistent_biochem_evidence_followup_gt_6mo`).

## Cohort-view exposure note

**Update 2026-05-02 (`mig_255`):** `qc_framework_v1/migrations/255_cohort_m038_complication_temporality_columns_20260502.sql` extends `manuscript_workspace.cohort_m038_massive_goiter_v1` with temporality/preop-aligned CPM passthroughs (`comp_hypoparathyroidism_transient`, `comp_hypocalcemia_timing_window`, RLN/VC `timing_window`, etc.).

**Update 2026-05-02 (`mig_256`):** `qc_framework_v1/migrations/256_cohort_complication_temporality_propagation_20260502.sql` propagates the same **12-column** passthrough bundle to the manuscript spine and parathyroid-heavy cohort views: `cohort_descriptive_full_cohort_v1`, `cohort_m009_parathyroid_final_path_v1`, `cohort_m017_eucalcemic_hypopara_v1`, `cohort_m032_descriptive_25yr_v1` (also fixes `tumor_size_cm` / `multifocal_flag` to `path_tumor_size_cm AS tumor_size_cm` and `multifocal_flag_path AS multifocal_flag` for live CPM), `cohort_m039_pth_calcium_v1`, `cohort_m040_reoperative_v1` (`tumor_size_cm` alias), `cohort_m042_incidental_parathyroid_v1`, `cohort_m066_parathyroid_id_v1` (thin projection from descriptive — column list extended), `cohort_m079_eucalcemic_outcomes_v1`, `cohort_m082_parathyroid_tumors_v1`.

## Underlying definition (per Logan)

- **Transient hypoparathyroidism:** confirmed postop hypoparathyroidism that resolves before 6 months from surgery (`comp_hypoparathyroidism_confirmed = TRUE AND comp_hypoparathyroidism_transient = TRUE`).
- **Permanent hypoparathyroidism:** confirmed postop hypoparathyroidism persisting beyond 6 months from surgery (`comp_hypoparathyroidism_confirmed = TRUE AND comp_hypoparathyroidism_permanent = TRUE`).
- **Postop all (hypoparathyroidism, fallback):** if neither flag is encoded for a confirmed case, the row label should explicitly read "Hypoparathyroidism postop all (timing not classified)" with a footnote.
- **Preop yes/no (hypocalcemia, RLN injury, VC paralysis, VC paresis):** present at any time before the index thyroid surgery date (`comp_*_timing_window = 'pre_surgery'` or `comp_*_clinical_preexisting = TRUE` where available). Reported as a separate row with a denominator equal to the arm n.

## Scope

Applies to: every manuscript-tier complication table for thyroid-surgery analyses, including descriptive cohort papers (M032, M038-A), definition / exposure papers (M038-B), and substudy / outcome papers (M039 PTH/Calcium, M044 ETE, M046 NIFTP, M047 Frozen Section, M025 TIRADS) where complications are reported.

Does not apply to: registry-tier or governance-tier objects, or to non-complication outcome reporting (recurrence, survival, etc.).

## Carry-forwards opened by this rule

- **`CF-RLN-PREOP-FLAG`** — **PARTIAL CLOSED for laryngoscopy-only source (2026-05-06, `mig_080`)**: BQ `canonical_patient_master.comp_rln_injury_preop` / `_source` populated for 119 patients with non-null `OPS_PREOP_LARYNGOSCOPY`; 8 RLN-preop positives. Remaining enhancement: dated preop H&P / voice-clinic note extraction once a note table with reliable `note_date` is available.
- **`CF-VC-PARALYSIS-PREOP-FLAG`** — **PARTIAL CLOSED for laryngoscopy-only source (2026-05-06, `mig_080`)**: BQ `canonical_patient_master.comp_vc_paralysis_preop` / `_source` populated for the same 119 laryngoscopy patients; 3 VC-paralysis-preop positives. Remaining enhancement: broader dated voice/laryngoscopy note extraction and VC paresis preop handling.
- **`CF-COHORT-VIEW-COMPLICATION-TEMPORALITY-COLUMNS`** — **CLOSED for complication-exposed spine + parathyroid/readout cohorts (2026-05-02):** `mig_255` (M038 massive) + `mig_256` (descriptive spine + cohorts listed above). Remaining `cohort_*` views that omit complications entirely do not require this bundle; join `canonical_patient_master` or descriptive spine if a new manuscript needs passthrough elsewhere.

## Trigger to revisit

Revisit this rule when:
- A dated Snowflake or BigQuery clinical-note table becomes available for H&P / voice-clinic notes — at that point, extend the laryngoscopy-only `mig_080` preop rows to broader preoperative note coverage.
- The hypoparathyroidism `_timing_window` column gains better post-2026-05-01 follow-up coverage — the `unknown` bucket currently dominates (87% of confirmed cases) and the trans/perm classification leans on supporting signals (treatment_req, biochem persistence, NSQIP recovered_flag) rather than pure timing. Better timing data would tighten the trans/perm assignment.

---

**Set by Logan via Cowork session, 2026-05-01.**
