# Cursor Agent Task — `canonical_psh_patient_rollup_v1` Sign-off (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 20-40 minutes (smallest of the 3-rollup batch)
**Run order:** Lane 6 (run after complications-rollup lane finishes)

---

## 1. Goal

Close the **PSH family** by signing off `canonical_psh_patient_rollup_v1`. Events table was verified by you in lane 3 (mig_104, commit d971cdc). Rollup `build_ts = 2026-04-22` — built before events were verified, but events haven't been MODIFIED (extraction-faithfulness verification doesn't touch data), so rollup should still be consistent. Verify and sign off.

---

## 2. Methodology — derivation re-derivation against verified events

Pattern: **mig_106 parathyroid rollup** — `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql`. Adapt for PSH.

### 2a. Pre-signoff probe
- Cohort parity: should be 10,871 rows = `canonical_patient_master` row count (CHANGE J from Script 365)
- Per-patient aggregate re-derivation against `canonical_psh_events_v1` (your mig_104 verified)
- Build SQL is in `scripts/365_psh_pmh_meds_consolidation.py` — find the PSH rollup CREATE OR REPLACE block

### 2b. Schema (28 cols total = 26 derivable + 2 na)
- 6 metadata: anchor_source, n_findings_any, n_findings_present, n_findings_definitive, n_findings_probable_or_better, first/last_finding_date, n_distinct_findings_norm
- 6 phenotypes × 3 tiers = 18 cols (psh_prior_thyroidectomy, psh_prior_neck_surgery, psh_prior_parathyroidectomy, psh_prior_rai, psh_prior_fna, psh_prior_neck_dissection)
- 2 na: research_id, build_ts

### 2c. Sign-off SQL
2-section mirror of mig_106:
- a: flip cleanly-matching cols via `verification_method='derivation_re_derivation_against_verified_events'`
- b: any drifted cols (STRING_AGG ordering — psh_phenotypes don't have STRING_AGG, but autotransplant-style cols might; check)
- c: recompute counts + flip table_status='verified'

---

## 3. Acceptance gates

- 26 not_started cols flipped
- 2 na cols carry over
- table_status='verified'
- Cohort parity (10,871 = CPM)
- 0 drift on all aggregate cols (events haven't been modified since rollup build, so rollup should still be consistent)

---

## 4. Don't touch (active parallel lanes)

- `canonical_pathology_clinical_events_v1` / `canonical_cervical_ln_clinical_events_v1` — Cowork's lane
- Any table touched by sibling Cursor lanes 5 + 7 (complications rollup, PMH rollup)

---

## 5. Reference reading

- Auto-memory: `feedback_motherduck_direct_check.md`, `feedback_surgical_git_add.md`
- Repo: `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` (template; same pattern: events-not-modified → no rebuild needed)
- Auto-memory: `project_medications_parathyroid_families_complete_2026-04-29.md`

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Migration filename: `qc_framework_v1/migrations/<next-N>_psh_patient_rollup_signoff.sql`
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP

---

## 7. If something unexpected surfaces

- Drift on BOOL_OR cols → STOP; events may have been modified after all (check) — fall back to mig_101 rebuild pattern
- Cohort parity off → STOP, investigate

---

End of prompt. Lane 6 of 3-prompt batch.
