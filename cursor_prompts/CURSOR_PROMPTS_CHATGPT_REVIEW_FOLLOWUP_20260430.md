# ChatGPT Review Followup — Cursor/Cline Handoff Prompts (2026-04-30)

After Cowork-direct apply of mig_209 (P1 registry reconciliation) + mig_210 (P7 rid 610 single-row fix), 5 of ChatGPT's 7 issue areas remain. Each has a recommended agent + prompt below. Use whichever agent is most convenient — model choice notes attached.

**Status snapshot:** 5-gate 175/0/0/0/0; cohort 10,871; PM events 6,469; HEAD post-mig_209/210 to be committed. None of these 5 follow-ups blocks v1.0 manuscript readiness; they are tighten-the-screws lanes.

---

## Lane A: P2 — Verify deferred analytic composites (10 tables not_started)
**Recommended agent:** **Cursor composer** (multi-file IDE; needs to look at builder scripts + write per-table verify reports)
**Why composer:** 10 tables × ~60 cols avg = ~600 col-rows to register. Scripts that built these tables live in `scripts/` and need to be cross-checked. Multi-file lane.
**Prompt:**

> ChatGPT review identified 10 analytic composite tables in `thyroid_canonical_publication_v1_0` that are `table_status='not_started'` with NULL `signoff_migration` in `canonical_table_signoff_registry_v1`. Each was written by a build lane (e.g., `mig_204` populated `manuscript_cohort_v1`) but never registered.
>
> **Tables:** `manuscript_cohort_v1` (150 cols), `patient_analysis_resolved_v1` (146 cols), `ln_master_rollup_v1` (78 cols), `episode_analysis_resolved_v1_dedup` (46 cols), `lesion_analysis_resolved_v1` (28 cols), `imaging_fna_linkage_v3` (20 cols), `tumor_stage_heterogeneity_v1` (17 cols), `imaging_patient_summary_v1` (13 cols), `recurrence_event_clean_v1` (11 cols), `patient_cross_domain_timeline_v2` (6 cols).
>
> **Step 1 (Logan input):** Confirm which of the 10 are in v1.0 manuscript scope vs post-v1.0/extension scope. Likely v1.0: `manuscript_cohort_v1`, `patient_analysis_resolved_v1`, `ln_master_rollup_v1`, `tumor_stage_heterogeneity_v1`. Likely post-v1.0: `lesion_analysis_resolved_v1`, `episode_analysis_resolved_v1_dedup`, `imaging_fna_linkage_v3`, `patient_cross_domain_timeline_v2`.
>
> **Step 2:** For in-v1.0 tables, find the build script (search `scripts/` and `qc_framework_v1/migrations/` for the populating lane), document upstream sources + grain + key uniqueness, then write a `mig_211a_verify_<table>` registering each col as `verified` (extraction-faithful) or `na` (helper/provenance). Pre-snapshot signoff_registry + col_registry. Follow `mig_205_us_gland_v2_signoff_registry_inserts_20260430.sql` template.
>
> **Step 3:** For out-of-v1.0 tables, mark `table_status='deferred_post_v1_0'` with note "out of v1.0 manuscript scope; verification deferred." Single-statement signoff_registry update.
>
> **Acceptance criteria:** post-apply, every `canonical_*` and `val_*`-style composite table in `main` and `manuscript_workspace` has either `table_status='verified'` or an explicit `'deferred_post_v1_0'` status. Cowork verification suite §12 stays at 0 ungoverned.

---

## Lane B: P3 — Manuscript-safe deduplicated PM view
**Recommended agent:** **Cline Sonnet 4.6** (mechanical CREATE VIEW; rule already documented)
**Why Cline Sonnet 4.6:** Pattern-following work; the dedup rule is already established in mig_185b memory; just needs a `canonical_*_VIEW_v1` on top of the existing table.
**Prompt:**

> Per `feedback_findings_vs_staging.md` and `project_2026-04-30_v11_round_complete.md`, `canonical_path_malignant_events_v1` preserves 525 source-distinct duplicate-grain rows (`is_source_distinct_duplicate_grain=TRUE`) by design. Filtering `WHERE is_source_distinct_duplicate_grain=FALSE OR IS NULL` yields 5,944 rows / 4,022 patients with 0 remaining duplicates by `(research_id, path_surgery_id, tumor_ordinal)`.
>
> Create `main.canonical_path_malignant_events_dedup_VIEW_v1` per `reference_view_naming_convention.md`:
>
> ```sql
> CREATE OR REPLACE VIEW main.canonical_path_malignant_events_dedup_VIEW_v1 AS
> SELECT * FROM main.canonical_path_malignant_events_v1
> WHERE is_source_distinct_duplicate_grain = FALSE OR is_source_distinct_duplicate_grain IS NULL;
> ```
>
> Then register the view in `canonical_table_signoff_registry_v1` per the VIEW-naming-convention pattern (mig_205 used this for `val_mig194`-derivative tables). Mark all cols as `verified` with `verification_method='view_filter_inheritance_from_canonical_path_malignant_events_v1'`. Pre-snapshot. One-shot lane = mig_211b.
>
> **Acceptance:** post-apply, view returns 5,944 rows / 4,022 distinct research_ids; verification suite §12 stays clean.

---

## Lane C: P4 — Recurrence implausible-date quarantine flag
**Recommended agent:** **Cline Sonnet 4.6** (single ALTER TABLE ADD COLUMN + scoped UPDATE)
**Why Cline Sonnet 4.6:** Low-risk schema change. Logan should approve the flag name + semantics first.
**Prompt:**

> ChatGPT review found 130 distinct patients in `canonical_recurrence_resolved_v1` with implausible recurrence dates (year < 1990): 128 with `recurrence_imaging_suspicious_date` years 0001-0012 (parsing failures — 4-digit-year corruption) + 2 with `recurrence_path_proven_date` (1× '0202-12-30' rid 12057, 1× '1950-06-10' rid 10622). All 130 already correctly classified as `recurrence_status_final='imaging_only_unconfirmed'` so they don't corrupt path-proven analyses, but raw-date queries are at risk.
>
> Also: 24 rows with `days_to_path_proven < 0` (most are imaging dates correctly preceding path-proven follow-up; 2 rows with `recurrence_path_proven=TRUE` and negative days are real anomalies — rid 9182 -87 days, rid 8203 -18 days).
>
> **Step 1 (Logan approval):** Confirm flag name + semantics. Recommended:
> - Add `is_implausible_date_quarantine BOOLEAN DEFAULT FALSE` to `canonical_recurrence_resolved_v1`
> - UPDATE to `TRUE` for the 130 + 2 = 132 rows: `EXTRACT(YEAR FROM recurrence_imaging_suspicious_date) < 1990 OR EXTRACT(YEAR FROM recurrence_path_proven_date) < 1990 OR (recurrence_path_proven=TRUE AND days_to_path_proven < 0)`
>
> **Step 2:** Apply via mig_211c following mig_209 pattern (pre-snapshot, ALTER TABLE, UPDATE, INSERT col_registry row, bump signoff). Verify: post-apply, query returns same 132 quarantined rows; analytic queries should add `WHERE is_implausible_date_quarantine=FALSE` for time-dependent recurrence analyses.
>
> **Acceptance:** Cowork verification suite stays clean; new col registered as `verified`; gate3 math holds.

---

## Lane D: P5 — Molecular `is_patient_level_only_evidence` flag
**Recommended agent:** **Cline Sonnet 4.6** (mirror of Lane C — same pattern)
**Prompt:**

> 525 of 1,384 rows in `canonical_molecular_genetics_v2` have `molecular_episode_id IS NULL`. All 525 sourced from `script_269_backfill` (443 thyroseq enrichment + 46 BRAF recovery + 36 RET adjudicated v226). These are patient-level mutation/fusion findings without per-test/per-date episode anchors — safe for "ever positive" cohort flags but unsafe for per-assay or platform-version analyses.
>
> **Step 1 (Logan approval):** Confirm flag name. Recommended:
> - Add `is_patient_level_only_evidence BOOLEAN DEFAULT FALSE` to `canonical_molecular_genetics_v2`
> - UPDATE to `TRUE` for `WHERE molecular_episode_id IS NULL` (525 rows / 520 distinct patients)
>
> **Step 2:** Apply via mig_211d following mig_209 pattern. Pre-snapshot.
>
> **Acceptance:** 525 rows flagged; new col registered as `verified` with `verification_method='derivation_from_molecular_episode_id_null_indicator'`; gate3 holds.

---

## Lane E: P6 — TIRADS outliers + conceptual scoping
**Recommended agent:** **Cursor composer** (needs Logan domain decision + source-record review)
**Why composer:** 20 of 21 size outliers are concentrated in research_id 8931 (size_cm_max=48 across 20 nodule rows on 2 exam dates). Likely a unit-conversion or wrong-field bug in the source extractor — needs Cursor + IDE + raw US report review.
**Prompt:**

> ChatGPT review on `canonical_us_nodule_v2` (37,579 rows total):
>
> 1. **21 size outliers** (`size_cm_max <= 0 OR > 20`). 20 are research_id 8931 with `size_cm_max=48` on 20 different `nodule_master_id`s spanning 2 `us_exam_id`s (2017-03-21 and 2019-08-29). 1 is research_id 8613 with `size_cm_max=21` (borderline). Recommended: review raw US reports for rid 8931 — likely a single-field extractor bug. Either correct the values via mig or quarantine via flag.
>
> 2. **3,067 rows with `acr2017_tirads_points=1`** (per ACR 2017, points=1 doesn't map to any category — TR1=0, TR2=2, TR3=3, TR4=4-6, TR5≥7). Distribution: 7,324 with points=0/TR1 (correct); 23 with points=1 mapping to TR1/TR2/TR3 (rule violation); 3,044 with points=1/NULL category (intermediate calculations or unparseable). Recommended: Logan domain decision — drop the 23 rule-violation rows from manuscript-safe view OR document as known noise.
>
> 3. **17,938 rows with `updated_tirads_category` differing from `acr2017_tirads_category`**. This is likely by-design — different category schemas (updated may be ACR 2024 or local Emory rule). Logan should explicitly document which category is manuscript-facing per `feedback_findings_vs_staging.md` style. Recommended: write a memory note `feedback_tirads_category_canonical.md` documenting Logan's choice + add a comment to the `acr2017_tirads_category` col_registry row.
>
> **Step 1:** Logan reviews the 21 size outliers (likely 8931 raw report check) — decide correct vs quarantine.
> **Step 2:** Logan ratifies which TIRADS category column is manuscript-facing; document.
> **Step 3:** Apply via mig_211e if any data corrections; otherwise documentation-only PR.

---

## Suggested execution order

1. **Lane A** (P2) first — biggest scope, longest runtime, requires Logan input on which tables are v1.0
2. **Lane B** (P3 dedup view) in parallel with A — quick CREATE VIEW
3. **Lanes C + D** (P4/P5 flags) in parallel — both require Logan flag-name approval, then mechanical
4. **Lane E** (P6 TIRADS) last — most domain-judgment-heavy, lowest data risk

After all 5 lanes apply, re-run Cowork verification suite — expect 175 → 175 + n_lane-A-verified-tables on gate1; everything else stable.

---

## What Cowork already did this round (mig_209 + mig_210)

**mig_209 — P1 registry reconciliation:** Registered 9 missing `canonical_path_malignant_events_v1` cols (AJCC resolved + dup flag) + 1 `canonical_us_exam_master_VIEW_v2` col (exam_id_source) + deprecated 8 stale `canonical_invasion_patient_rollup_v1` cols. All cols were data-verified by prior lanes; mig_209 closed the governance gap only.

**mig_210 — P7 rid 610 fix:** UPDATE `canonical_patient_master.first_surgery_date` 1945-07-13 → 2004-07-13 for research_id 610. Pre-snapshot in archive. Confirmed only such pre-1990 first_surgery_date in entire CPM.

Final state: 5-gate 175/0/0/0/0; pre_1990 first_surgery_dates: 0; cohort/event invariants unchanged.
