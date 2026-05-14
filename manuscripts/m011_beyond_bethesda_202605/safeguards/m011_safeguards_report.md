# M011 — Iterative-Build Safeguards Report (v1 baseline)

Generated 2026-05-14 · project `thyroid-canonical-pub-2026` · build = M011 v1 (first iteration)
Tables: `pub_workspace.m011_provenance_manifest`, `m011_column_source_audit`, `m011_cohort_qc`; snapshots in `pub_archive.m011_*_v1_baseline_20260514`.

This is the first iteration, so there is no prior build to diff against — this report **establishes the v1 baseline** that every future iteration (v2, v3 …) must diff against. See `sql/m011_iteration_diff.sql` for the diff template.

---

## 1. Snapshot-before-overwrite ✅

Seven locked-number tables frozen to `pub_archive` before any future rebuild can overwrite them:

| Snapshot (pub_archive) | Source (pub_workspace) | Rows |
|---|---|---|
| `m011_frame_b_v1_baseline_20260514` | `m011_frame_b` | 10,871 |
| `m011_frame_a_primary_v1_baseline_20260514` | `m011_frame_a_primary` | 9,846 |
| `m011_patient_base_v1_baseline_20260514` | `m011_patient_base` | 10,871 |
| `m011_model_data_v1_baseline_20260514` | `m011_model_data` | 2,412 |
| `m011_model_metrics_v1_baseline_20260514` | `m011_model_metrics` | 16 |
| `m011_cohort_audit_v1_baseline_20260514` | `m011_cohort_audit` | 35 |
| `m011_sensitivity_metrics_v1_baseline_20260514` | `m011_sensitivity_metrics` | 12 |

**Rule for v2+:** snapshot again with the new date suffix *before* re-running `m011_build_pipeline.sql`, then run `m011_iteration_diff.sql` against the most recent baseline.

## 2. Provenance manifest ✅ — `pub_workspace.m011_provenance_manifest`

Every canonical source that fed M011 v1, with its `last_modified` at build time:

| Source | Type | Rows | Last modified (at build) | Consumed for |
|---|---|---:|---|---|
| `manuscript_cohort_v1` | TABLE | 10,871 | 2026-05-07 08:31 | demographics, surgery dates, histology, path outcomes, mol flags |
| `canonical_fna_events_v1` | TABLE | 8,050 | 2026-05-06 03:58 | preoperative Bethesda (first/last/highest) |
| `canonical_us_nodule_tirads_multisystem_v1` | TABLE | 37,579 | 2026-05-08 18:59 | multisystem TI-RADS + park feature booleans |
| `canonical_us_nodule_v2` | TABLE | 37,579 | 2026-05-08 09:33 | ETE-on-US feature |
| `molecular_test_episode_v2` | VIEW→`pub_legacy_source_20260416` | — | 2026-05-06 14:42 | preoperative Afirma/ThyroSeq results |
| `imaging_fna_linkage_v3` | TABLE | 9,911 | 2026-05-06 04:03 | US-nodule ↔ FNA linkage + tier |
| `imaging_nodule_long_v2` | VIEW→`pub_legacy_source_20260416` | — | 2026-05-06 14:42 | legacy↔hash nodule_id bridge |
| `fna_episode_master_v2` | VIEW→`pub_legacy_source_20260416` | — | 2026-05-06 14:42 | FNA episode → Bethesda (Frame A) |
| `canonical_histology_lookup_v1` | TABLE | 38 | 2026-05-05 23:57 | histology classifier reference |

If a reviewer asks "where did N=2,479 come from," this manifest + `m011_build_pipeline.sql` is the answer. **For v2+:** if any source `last_modified` is newer than this manifest, the cohort *may* have drifted — run the iteration diff.

## 3. Competing-source column audit ✅ — `pub_workspace.m011_column_source_audit`

All M011 columns were checked against `pub_signoff.canonical_column_verification_registry_v1` and `deprecation_registry_v1`. **Every column M011 uses is registry-`verified`. No deprecated column is used** (the deprecated `ln_level_*_examined` columns are *not* used by M011). Three columns carry an open source-of-truth question and are flagged:

| Column | SoT status | Competing-source flag | Linked issue | Note |
|---|---|---|---|---|
| `surgery_date` / `first_surgery_date` / `surg_first_date` | verified | ⚠️ **YES** | THY-87 | M011 uses `COALESCE(surgery_date, first_surgery_date, surg_first_date)`. `surg_first_date` == `surgery_date`; `first_surgery_date` diverges in 171 patients. Re-confirm if THY-87 picks a different authoritative column. |
| `ln_positive_final` | verified | ⚠️ **YES** | THY-89 | Used in `aggressive_feature_flag` + `clin_sig_malignancy`. Project-wide LN issue: 51 raw-vs-final disagreements + 38 impossible rows. Column is verified but the SoT decision is pending. |
| `histology_final` | verified | ⚠️ **YES** | (none) | Free-text; M011 applies a regex classifier. Needs pathologist review before submission. |
| `fna_bethesda_final`, `imaging_tirads_best` | verified | no | — | Present in master but **not used** — M011 re-derives Bethesda and TI-RADS from event/nodule-level tables for control over definitions. |
| `path_tumor_size_cm`, `path_ete_final`, `vascular_invasion_final`, `margin_status_final`, `age_at_surgery`, `demo_sex_final` | verified | no | — | No competing-source conflict. |

**Impact on M011 conclusions:** `ln_positive_final` and `surgery_date` feed the *clinically-significant-malignancy* outcome and the preoperative window. The headline TI-RADS finding is on *any malignancy* (independent of LN) and was shown robust across outcome definitions in Table 8, so the THY-89 exposure is low — but it should be disclosed in Limitations and re-checked when THY-87/THY-89 close.

## 4. Cohort-scoped QC assertions ✅ — `pub_workspace.m011_cohort_qc`

The M011-relevant subset of `pub_signoff.qc_assertions_v1`, plus temporal/linkage checks, run **filtered to the M011 cohort** (not project-wide). **All 12 checks PASS:**

`QC_BETHESDA_ENUM` · `QC_SURGERY_DATE_RANGE` · `QC_RESEARCH_ID_UNIQUE` · `QC_RESEARCH_ID_IN_MASTER` · `QC_FNA_PREOP_TEMPORAL` · `QC_US_PREOP_TEMPORAL` · `QC_MOLECULAR_PREOP_TEMPORAL` · `QC_LN01_NONNEGATIVE` · `QC_LN02_PLAUSIBLE_MAX` · `QC_OUTCOME_NONNULL_MODELED` · `QC_FRAMEA_LINK_TIER_VALID` · `QC_TIRADS_ENUM` — all 0 violations.

The build began from a known-clean cohort slice. **For v2+:** re-run this table first; any new FAIL must be resolved before numbers are locked.

## 5. Iteration diff — N/A for v1

This is the first build. `sql/m011_iteration_diff.sql` is the template that, for v2+, reports: patients added/dropped vs the prior baseline, locked metric values that moved (with magnitude), and which source table's `last_modified` changed since the prior manifest — so a "builder didn't retrigger" or "legacy carry-over broke" failure is caught before a co-author sees it.

## 6. Linear routing ✅

The two competing-source flags map to **existing** Linear issues — **THY-87** (surgery-date authoritative source, Backlog/Medium) and **THY-89** (LN raw-vs-final, Backlog/High), both in the *Database Reconciliation & QA* project. M011's downstream dependency, exposure assessment, and the re-check action have been posted as a comment on each:
- THY-87 comment: https://linear.app/rostemp/issue/THY-87 (M011 uses `COALESCE(surgery_date, …)` — opposite of the issue's `first_surgery_date` lean)
- THY-89 comment: https://linear.app/rostemp/issue/THY-89 (M011 uses `ln_positive_final`; cohort-scoped LN QC passed, so none of the 38 critical rows land in the M011 cohort)

No new issues filed — nothing M011-specific is broken; the dependencies are on pre-existing open decisions.

## 7. Google Cloud AI column verification — registry check ✅, AI-agent step wired (connection not yet provisioned)

The **authoritative** verification was done: every M011 column was cross-checked against `pub_signoff.canonical_column_verification_registry_v1` — the project's human-ratified column sign-off registry — and **all are `verified`**, none deprecated. This is a stronger check than an LLM opinion because it is the project's own ratified source of truth.

The requested second, independent pass via a Google Cloud AI agent was **attempted but the BigQuery→Vertex AI connection is not provisioned** in `thyroid-canonical-pub-2026` (no remote `MODEL` objects; `AI.GENERATE` returns "Connection not found"). The `AI.GENERATE` cross-check over `pub_canonical.data_dictionary_v279` is wired and ready as **Step E in `sql/m011_safeguards.sql`** — it will run as soon as a Vertex connection (or the console custom Agent) is created. Until then, the registry check in Step D is the column-sourcing verification of record.
