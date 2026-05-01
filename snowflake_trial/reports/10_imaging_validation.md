# Snowflake Cortex Validation — Prompt 10: Imaging Coverage & Date Sanity
**Generated:** 2026-05-01 (post-handoff)
**Source:** MD-direct via MCP (`thyroid_canonical_publication_v1_0`); equivalent script `snowflake_trial/scripts/16_prompt10_imaging.py` will produce the same numbers from Snowflake once Logan re-exports.
**Tables probed:** `imaging_exam_master_v1` (13,347 × 20), `canonical_us_exam_master_VIEW_v2` (11,880 × 24), `canonical_us_patient_master_VIEW_v2` (4,385 × 28), `canonical_cervical_ln_clinical_*`.

---

## Summary

Imaging coverage is **partial and source-dependent**. 4,385/10,871 patients (40.3%) have any US exam recorded. The largest data-quality issues are concentrated in one source: **`raw_imaging_12_slots_v1` accounts for all 2,050 NULL exam dates** (43% of its rows) plus the two extreme-outlier dates (year 0202 and year 3022). The two TIRADS-bearing sources (`raw_us_tirads_excel_v1`, `raw_us_tirads_scored_v1`) have clean dates with only 4 plausibility-flagged future exams.

Major architectural finding: **the live publication-DB CPM has only 5 NLP-derived TIRADS columns**. The 28+ rich TIRADS columns referenced in earlier prompts (e.g. `tirads_best_category_v12`, `imaging_tirads_best`, `preop_tirads_best`) live in `cpm_tirads_legacy_20260421` and `views_readable.Patient_Master_Canonical` (42 cols), but **not on `main.canonical_patient_master`**. The Snowflake `CANONICAL_PATIENT_MASTER_FLAT` mirror still has them because the export captured an earlier state — this will diverge on next re-export.

Net new findings:
- **CF-mig260e-IMAGING-12SLOTS-DATE-QUALITY** — 2,050/4,816 (43%) NULL exam_date in `raw_imaging_12_slots_v1`; 2 extreme outliers (year 0202, year 3022)
- **CF-mig260f-CPM-TIRADS-MIGRATION-DRIFT** — live CPM lost 28+ TIRADS columns vs Snowflake mirror; rebuild path is `views_readable.Patient_Master_Canonical` (42 cols) or `canonical_us_patient_master_VIEW_v2` (7 cols)
- **CF-mig260g-US-LN-SUSPICIOUS-FLAG-UNDERFIRE** — `any_suspicious_us_ln_ever=TRUE` for only 8 patients despite 4,077 patients with US LN findings ever and 974 with NLP-positive LN-level mentions

---

## 1. Imaging master coverage by source

| Source | n_exams | n_pts | n_null_dates | min_date | max_date | n_with_tirads | avg_nodules |
| --- | --- | --- | --- | --- | --- | --- | --- |
| raw_us_tirads_excel_v1 | 6,025 | 3,439 | 0 | 2001-08-09 | 2026-06-19 | 6,025 | 3.30 |
| raw_imaging_12_slots_v1 | 4,816 | 3,454 | **2,050** | **0202-08-29** | **3022-03-03** | 0 | 1.03 |
| raw_us_tirads_scored_v1 | 2,506 | 1,386 | 0 | 2002-08-07 | 2028-07-24 | 2,506 | 4.84 |

**`raw_imaging_12_slots_v1` is the date-quality outlier**: NULL rate 43% and two extreme date typos. Probably 2-digit-year ingestion bug — per the established `2-digit year convention` (Logan-ratified 2026-04-27, all YY → 20YY), `0202-08-29` is plausibly 2002-08-29 and `3022-03-03` likely 2022-03-03. The 2026/2028 outliers in `raw_us_tirads_*` could be true future scheduled exams (only 5 rows total, defer).

---

## 2. Implausible-date distribution (imaging_exam_master_v1)

| Date bucket | n |
| --- | --- |
| NULL | 2,050 |
| pre-1990 | 1 |
| 2000s | 66 |
| 2010s | 4,010 |
| 2020-now | 7,216 |
| 2026-05-01 to 2030-01-01 (post-today) | 3 |
| post-2030 | 1 |

The 5 implausible-future-or-past rows (1 pre-1990 + 3 post-today + 1 post-2030):

| exam_date | source | rid | n_nodules | max_tirads |
| --- | --- | --- | --- | --- |
| 0202-08-29 | raw_imaging_12_slots_v1 | 12048 | 1 | — |
| 2026-05-18 | raw_us_tirads_excel_v1 | 12177 | 1 | TR4 |
| 2026-06-19 | raw_us_tirads_excel_v1 | 11666 | 1 | TR1 |
| 2028-07-24 | raw_us_tirads_scored_v1 | 4936 | 3 | TR3 |
| 3022-03-03 | raw_imaging_12_slots_v1 | 10511 | 3 | — |

---

## 3. US exam coverage (canonical_us_patient_master_VIEW_v2 + CPM rollup)

| Metric | n |
| --- | --- |
| Patients with any US (canonical view) | 4,385 |
| Patients with preop US available | 3,816 |
| Patients with TIRADS-categorized exam | 3,396 |
| Patients with bilateral disease ever | 2,153 |
| Patients with multifocal flag ever | 2,865 |
| Patients with any suspicious US LN ever | **8** |
| Patients with first_us_date > last_us_date | 0 |
| Patients with NLP backfill pending | 4,074 |

| CPM-side rollup | n |
| --- | --- |
| Patients with `us_n_reports > 0` (CPM rollup) | 4,074 |
| Patients with US only post-surgery (no preop) | 543 |
| Patients with US only pre-surgery (no postop surveillance) | 2,375 |
| us_first_exam_date < 1990-01-01 | 1 |
| us_last_exam_date > 2030-01-01 | 1 |

**CPM rollup `us_n_reports > 0` (4,074) lags the canonical view (4,385) by 311 patients** — likely because the CPM rollup counts `us_n_reports` only when actual US-report rows exist, while the view's `has_any_us` is broader (includes exam_master entries without report-row text). Worth confirming in the next round.

**`any_suspicious_us_ln_ever=TRUE` fires for only 8 patients** — but `has_us_ln_findings_ever=TRUE` for 4,077 patients and the cervical-LN-clinical NLP rollup flags 974 patients with positive-LN-level mentions. The "suspicious" flag is effectively dead — either the threshold is too tight or the column never got backfilled. **CF-mig260g**.

---

## 4. TIRADS distribution × malignancy (`max_tirads_category_ever`, n=3,396 patients)

| TIRADS | n_pts | n_malignant | ROM% | ACR-expected |
| --- | --- | --- | --- | --- |
| TR1 | 346 | 105 | 30.3% | ~0% |
| TR2 | 300 | 101 | 33.7% | <2% |
| TR3 | 852 | 255 | 29.9% | <5% |
| TR4 | 495 | 244 | 49.3% | 5–20% |
| TR5 | 1,403 | 843 | 60.1% | >20% |

**Same operative-bias enrichment as Prompt 7's Bethesda finding**, here at the canonical-rebuild-stable layer (not legacy v12 cols). Every TIRADS bucket including TR1 has 5–10× the ACR-expected ROM because these are surgically-resected nodules. Manuscript footnote already drafted in M037 / M025; this confirms the finding holds against the post-mig265-cleanup CPM.

The 30.3% ROM for TR1 is conspicuous — 105 malignancies in 346 TR1 patients. Either:
- TR1 was assigned to nodules that ultimately became dominant, but the surgically-removed nodule was different (multinodular cohort)
- TR1 categorization is mis-recorded
- True false-negative rate of TIRADS in a tertiary surgical referral cohort

`canonical_us_nodule_v2_filtered` and `vw_us_nodule_tirads_strict_acr2017_VIEW_v1` in `manuscript_workspace` likely already attribute correctly per-nodule. M025 cohort view should be re-checked at the per-nodule grain.

---

## 5. Cervical LN clinical (NLP) vs US LN findings

| Metric | n |
| --- | --- |
| Patients with cervical LN clinical events (NLP) | 1,643 |
| Patients with `has_positive_ln_level=TRUE` (NLP rollup) | 974 |
| Patients with `has_us_ln_findings_ever=TRUE` (US imaging) | 4,077 |
| Patients with `any_suspicious_us_ln_ever=TRUE` (US imaging) | 8 |
| `n_positive_ln_level_mentions` total events | (in 4,493 LN clinical events) |

**The two LN-finding pipelines barely overlap conceptually**: NLP cervical-LN-clinical events (1,643 pts, of which 974 had positive level mentions) come from clinical-note free text. US LN findings (4,077 pts) come from radiology imaging. The `any_suspicious_us_ln_ever` flag fires for 8 — practically dead. M037 and M076 (LN surveillance) cohort selection should rely on `n_positive_ln_level_mentions > 0` from the LN clinical rollup, not the suspicious flag.

---

## 6. CPM TIRADS migration drift (architectural)

| Schema / Table | TIRADS cols |
| --- | --- |
| `main.canonical_patient_master` (live) | **5** (NLP-only) |
| `main.canonical_us_nodule_v2` | 8 |
| `main.canonical_us_patient_master_VIEW_v2` | 7 |
| `main.imaging_patient_summary_v1` | 2 |
| `main.manuscript_cohort_v1` | 4 |
| `views_readable.Patient_Master_Canonical` | **42** |
| `views_readable.US_Nodules_Wide_v2` | 80 |
| `cpm_tirads_legacy_20260421.canonical_patient_master_pre_partB` | (legacy archive) |
| Snowflake `CANONICAL_PATIENT_MASTER_FLAT` | 28+ (mirror of pre-cleanup state) |

**Snowflake CPM_FLAT is stale** — it was exported before mig_265-style cleanup that pruned the v12/imaging/preop TIRADS columns from the live CPM. When Logan re-runs `01_export_md_to_parquet.py` and `04_build_flat_views.py`, those columns will disappear from the FLAT view and any Prompt 7-style query referring to them will fail.

**Migration paths for affected manuscripts:**
- Prompt 7 / M037 / M025 should switch from `tirads_best_category_v12` → `max_tirads_category_ever` on `canonical_us_patient_master_VIEW_v2` (7 cols, live) or `views_readable.Patient_Master_Canonical` (42 cols, reflective view)
- Bethesda is unaffected (lives on `canonical_fna_events_v1`)

---

## 7. Reusable patterns

- **Source-stratified date-quality probe**: bucketing exam_date by source surfaces the bad-source quickly. `raw_imaging_12_slots_v1` is the imaging-side bad apple here.
- **Live-vs-Snowflake CPM column drift detection**: probe `information_schema.columns` against the live publication DB filtered to `table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main' AND table_name='canonical_patient_master'`. The bare `WHERE table_name=…` (no DB filter) pulls in `readonly_share` + archive copies and gives a falsely-rich answer.
- **Flag-utilization sanity** (`any_suspicious_us_ln_ever`): if a boolean rollup flag fires for <1% of the eligible cohort, probe the threshold definition.

---

## 8. Carry-forwards (new)

| CF | Description | Severity | Action |
| --- | --- | --- | --- |
| CF-mig260e-IMAGING-12SLOTS-DATE-QUALITY | 2,050/4,816 NULL exam_date in raw_imaging_12_slots_v1; 2 extreme YY-typo dates | MED | Date-cleanup migration; reconcile via 2-digit-year convention |
| CF-mig260f-CPM-TIRADS-MIGRATION-DRIFT | Live CPM has only 5 NLP TIRADS cols; Snowflake mirror has 28+ from pre-mig_265 export | HIGH | Update Prompt 7 / M025 / M037 to use `canonical_us_patient_master_VIEW_v2` cols on next Snowflake re-export |
| CF-mig260g-US-LN-SUSPICIOUS-FLAG-UNDERFIRE | `any_suspicious_us_ln_ever`=TRUE for 8 patients vs 4,077 with any US LN findings | MED | Audit threshold definition or rebuild from `canonical_us_thyroid_gland_v2`/per-nodule level |
