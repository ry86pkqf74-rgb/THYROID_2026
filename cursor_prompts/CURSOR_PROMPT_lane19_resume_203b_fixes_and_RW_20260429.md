# Cursor Agent Task — Lane 19 RESUME: Apply Spot-check Fixes + RW Execute (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post spot-check)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting (current tip after Lane 20 mig_128 + Cowork mig_127)
**Estimated effort:** 60-90 minutes (fix script, re-run dry-run, RW, mig_123)
**Run order:** Lane 19 RESUME (continues from `08325dc`)

---

## 1. Status

Lane 19 paused at the §3d gate awaiting Logan's spot-check approval. Cowork ran the spot-check (10-20 patients across tiers in `scripts/output/canonical_recurrence_v1_preview_203b.parquet`) and surfaced 3 findings. Logan approved with **Option 1: Fix #1 (year-202 typo) + investigate #2 (legacy completions); approve RW after**.

**This prompt:** apply the spot-check-derived fixes to `scripts/203b_canonical_recurrence_harmonized_20260429.py`, re-run dry-run, then execute the RW rebuild + mig_123.

---

## 2. Spot-check findings to address

### Finding #1 — Bad upstream date typo (data bug)
- Patient 12057: `manuscript_workspace.recurrence_path_proven_candidates_v1.path_proven_date = '0202-12-30'` (year 202)
- Patient 10622: `path_proven_date = '1950-06-10'` (year 1950 — also suspicious)

### Finding #2 — Legacy structural_confirmed misclassifications
Of 30 `structural_confirmed_legacy` rows from `recurrence_event_clean_v1`:
- **5 with TTR=0** (rids 8252, 10237, 2210, 6436, 7539): `recurrence_date = first_surgery_date` — likely initial cancer dx, NOT recurrence
- **8 with negative TTR** (rids 6343, 7145, 8916, 7512, 8097, 10153, 12172, 6251): `recurrence_date < first_surgery_date` — bad date or misclassified initial dx
- **1 with TTR=34d** (rid 6674): `sep 8133` (2018-06-21) + `sep 8134` (2018-07-25) — possibly planned 2-stage thyroidectomy / completion
- **1 with year-202** (rid 12057): same as Finding #1
- **1 with year-1950** (rid 10622): same as Finding #1

15 of 30 legacy entries (50%) have date-integrity issues.

### Finding #3 — 9 negative TTR (already clipped to NULL by 203b)
Logan-approved: clip-to-NULL + open `CF-mig123-NEGATIVE-TTR-9-PATIENTS` for follow-up investigation.

---

## 3. Fixes to apply in `scripts/203b_canonical_recurrence_harmonized_20260429.py`

### 3a. Date sanity filter on Tier 1 source rollups
Wherever the script joins/reads `manuscript_workspace.recurrence_path_proven_candidates_v1`, add:

```sql
WHERE path_proven_date BETWEEN DATE '1990-01-01' AND DATE '2027-01-01'
```

This drops the 2 bad-year rows (12057, 10622) without touching upstream. Document inline as `# CF-mig123-UPSTREAM-DATE-202-TYPO defensive filter`.

### 3b. Legacy structural_confirmed TTR-positivity filter
Wherever the script reads `recurrence_event_clean_v1` (legacy fallback), add:

```sql
WHERE recurrence_date > first_surgery_date  -- only true post-op recurrences
```

This drops the 5 TTR=0 + 8 negative-TTR rows. Document inline as `# Filter completion-thyroidectomy / initial-dx misclassifications per spot-check 2026-04-29`.

### 3c. Patient 6674 disposition
Keep patient 6674 (34d) in the legacy structural_confirmed bucket — clinical review is out of scope for this lane. Add inline note. Open `CF-mig123-LEGACY-COMPLETION-CHECK-6674` for future review.

### 3d. Re-run dry-run, expect:
- structural_confirmed_legacy: 30 → 15 (15 filtered)
- Total confirmed_true: 528 → ~513
- Negative TTR clipped: 9 → 0 (or near-zero, since the negative-TTR legacy rows are now filtered upstream)
- All other tiers unchanged

### 3e. Update dry-run report
Regenerate `scripts/output/canonical_recurrence_203b_dry_run_report_20260429.{json,md}` with new counts. Document the filter additions in the report header.

---

## 4. RW execution (after dry-run validates)

### 4a. Pre-rebuild snapshot
```sql
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig123_20260429 AS
SELECT * FROM main.canonical_recurrence_v1;
```

### 4b. Run 203b with `--write` (or do_writes=True)
- Verify row count = 10,871 (cohort preserved)
- Verify confirmed_true ≈ 513 (slight tolerance OK)
- Verify per-tier counts match dry-run

### 4c. Author + apply mig_123
File: `qc_framework_v1/migrations/123_canonical_recurrence_v1_rebuild_signoff.sql`

The migration should:
- Document the rebuild + spot-check-derived filters
- Re-flip the 11 cols (previously verified shell-degenerate via mig_122):
  - New `verification_method='derivation_re_derivation_post_script_203b_harmonized_rebuild'`
  - Replaces mig_122's `cohort_wide_shell_derivation_verification_degenerate_case`
- Update batch_id to `mig_123_canonical_recurrence_v1_rebuild_signoff_20260429`
- Close `CF-mig122-RECURRENCE-203-REBUILD-PENDING`
- Open the following CFs:
  - `CF-mig123-UPSTREAM-DATE-202-TYPO` (rid 12057 + rid 10622 dropped via defensive filter; upstream still has bad rows)
  - `CF-mig123-NEGATIVE-TTR-9-PATIENTS` (the original 9 negative-TTR cases — investigate first_surgery_date semantics)
  - `CF-mig123-LEGACY-COMPLETION-CHECK-6674` (patient 6674's 34-day "recurrence")
  - `CF-mig124-RECURRENCE-PATH-CANONICAL-LINEAGE` (Tier 1 currently uses path_synoptics; phase-2 migration to canonical_path_malignant_events_v1 + canonical_pathology_clinical_events_v1 union — Logan-approved follow-up within 1-2 sessions)
- Update table_signoff_registry notes
- Update `qc_framework_v1/REMAINING_WORK_INVENTORY.md`

### 4d. Re-run 5-gate audit
Expected: gate 1 unchanged at 71, gate 5 unchanged at 20 (the existing CF-mig122 row for `first_surgery_date` may need updating to point to mig_123 batch; otherwise unchanged).

---

## 5. Deliverables

1. Updated `scripts/203b_canonical_recurrence_harmonized_20260429.py` with 3 filter additions
2. Regenerated dry-run report
3. Pre-snapshot in `archive_pub_v1_0`
4. Live `canonical_recurrence_v1` rebuilt (528 → ~513 confirmed)
5. `qc_framework_v1/migrations/123_canonical_recurrence_v1_rebuild_signoff.sql`
6. 4 CFs opened (per §4c)
7. `qc_framework_v1/REMAINING_WORK_INVENTORY.md` updated
8. Memory close-out: `project_canonical_recurrence_v1_mig_123_rebuild_closeout.md`
9. Single commit, surgical add, push to origin/main

---

## 6. Don't touch (active parallel lanes)

- `canonical_patient_master` operative cluster — Lane 22 (in flight)
- `manuscript_workspace.*` tier3_helper batch — Lane 21 (in flight)
- Lanes 23/24/25 patient_master pathology/lymph_node/labs clusters — sibling lanes (use stable recurrence_v1 post-rebuild)

---

## 7. Reference reading

Required:
- Auto-memory: `project_canonical_recurrence_v1_mig_122_closeout.md` (Cursor 14's shell signoff context)
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Repo: `scripts/203b_canonical_recurrence_harmonized_20260429.py` (the script to fix)
- Repo: `scripts/output/canonical_recurrence_203b_dry_run_report_20260429.{json,md}` (current dry-run state)
- Repo: `qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql` (verification template post-rebuild)

---

## 8. If something unexpected surfaces

- Filter drops MORE than ~15 legacy rows → investigate; some legitimate recurrences may share short TTR
- Confirmed_true drops below 500 → unexpectedly aggressive filter; investigate
- Pre-snapshot already exists from prior attempt → suffix `_v2` and document
- mig_123 SQL fails on the existing CF-mig122 first_surgery_date col registry row → may need a separate UPDATE to redirect that CF to mig_123

---

End of prompt. Lane 19 RESUME. After this lands, canonical_recurrence_v1 transitions from shell to fully populated; CF-mig122-RECURRENCE-203-REBUILD-PENDING closes; CF-mig124-RECURRENCE-PATH-CANONICAL-LINEAGE remains open as scheduled phase-2.
