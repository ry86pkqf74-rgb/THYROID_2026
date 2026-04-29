# Cursor Agent Task — `canonical_recurrence_v1` Script 203 REBUILD (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-Cursor-14 mig_122)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting (current tip `8810385`)
**Estimated effort:** 2-4 hours (Script 203 harmonization + rebuild + reverification)
**Run order:** Lane 19 of next 3-prompt batch (run last — substantive RW work, paused for Logan approval at dry-run gate)

---

## 1. Goal

Address `CF-mig122-RECURRENCE-203-REBUILD-PENDING` opened by Cursor 14 mig_122.

Currently `canonical_recurrence_v1` is a **degenerate shell** — 10,871 rows all `recurrence_confirmed=FALSE`, `recurrence_type='none'`, no real evidence. Script 203 (the SSOT builder) expects upstream tables (`operative_episode_detail_v2`, `gold_master_patient_facts_v1`) that don't exist on the publication catalog. The shell is the result of Script 203 falling back to cohort-padding when its primary spine is missing.

This lane:
1. Harmonizes Script 203 to use **current verified canonicals** as its spine
2. Pre-snapshots the existing recurrence_v1 to `archive_pub_v1_0`
3. Rebuilds `canonical_recurrence_v1` with non-shell data
4. Re-verifies under Protocol v2 (replacing mig_122's shell verification)

⚠️ **PAUSE GATE:** This is a substantial rewrite. After dry-run + report, **stop and surface findings to Logan** before any RW operation. Logan's approval required for the `query_rw` step that replaces `canonical_recurrence_v1`.

---

## 2. Spine harmonization plan

Script 203 originally expected:
- `operative_episode_detail_v2` (legacy operative spine)
- `gold_master_patient_facts_v1` (legacy patient anchor)
- `path_synoptics` raw (fallback)

Per `project_op_procedure_consolidation_script_362_closeout.md`, the operative spine has been consolidated into:
- `canonical_operative_events_v1` (verified mig_362) — surgery dates + surgical type
- `canonical_operative_patient_rollup_v1` — per-patient surgery summary
- `canonical_operative_procedure_codes_v1` (verified mig_118) — procedure details

Per current verified state:
- `canonical_path_malignant_events_v1` (verified mig_89) — path-proven events with dates + histology
- `canonical_pathology_clinical_events_v1` (verified mig_110) — clinical-note path findings
- `canonical_cervical_ln_clinical_events_v1` (verified mig_111) — cervical LN findings
- Imaging upstream: TBD (locate during exploration; possibly `canonical_us_*_v2` for ultrasound recurrence imaging)
- `canonical_patient_master` — cohort anchor (1,592 cols, mostly not_started; provides research_id list)

### 2a. Source-by-source mapping

| Script 203 reference | Current verified replacement |
|---|---|
| `operative_episode_detail_v2.surgery_date_native` | `canonical_operative_events_v1.surgery_date_native` |
| `operative_episode_detail_v2.surgery_episode_id` | `canonical_operative_events_v1.surgery_episode_id` |
| `gold_master_patient_facts_v1.research_id` | `canonical_patient_master.research_id` (or fallback to operative_events DISTINCT) |
| `path_synoptics` raw | `canonical_path_malignant_events_v1` + `canonical_pathology_clinical_events_v1` (use raw only as last fallback) |
| Recurrence event evidence | UNION of: `canonical_pathology_clinical_events_v1` (path recurrence findings) + `canonical_path_malignant_events_v1` (when post-first-surgery) |

### 2b. Tier definitions
Script 203 uses tiers 1–5 (see comments in `scripts/203_canonical_recurrence.py`). Read the script to understand tier definitions, then map to current data sources.

---

## 3. Workflow

### 3a. Read + harmonize Script 203
- Read `scripts/203_canonical_recurrence.py` end-to-end
- Identify every reference to `operative_episode_detail_v2` / `gold_master_patient_facts_v1` / `path_synoptics`
- Plan replacements per §2a
- Write `scripts/203b_canonical_recurrence_harmonized_20260429.py` (or revise 203 in-place — Cursor decides; in-place is OK if backward-compat'd via comment)

### 3b. Dry-run (no DB writes)
Run the harmonized script with `do_writes=False` (or equivalent flag). Output:
- Patient cohort scope (expect 10,871)
- Per-tier counts (e.g., 1,247 patients with tier-1 path-proven; 891 with tier-2 imaging; etc.)
- Distribution of `recurrence_status_final` after rebuild
- List of dropped/changed rows vs current shell

### 3c. Pre-rebuild snapshot
```sql
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig123_20260429 AS
SELECT * FROM main.canonical_recurrence_v1;
```

### 3d. Report back to Logan with dry-run findings
Surface:
- Estimated number of patients flipping from `recurrence_confirmed=FALSE` to `TRUE`
- Source-by-source contribution
- Any unmapped Script 203 logic that needs further harmonization
- Specific concerns (date mismatches, cohort drift, etc.)

**STOP HERE — wait for Logan approval before §3e.**

### 3e. RW rebuild (after Logan approval)
- Run harmonized Script 203 with `do_writes=True`
- Verify row count = 10,871 (cohort parity preserved)
- Verify per-tier counts match dry-run

### 3f. Re-verification migration (mig_123)
File: `qc_framework_v1/migrations/123_canonical_recurrence_v1_rebuild_signoff.sql`
- Document the rebuild + spine harmonization
- Re-flip 11 cols (previously verified shell-degenerate via mig_122):
  - New verification_method: `derivation_re_derivation_post_script_203_harmonized_rebuild`
  - Replaces mig_122's `cohort_wide_shell_derivation_verification_degenerate_case`
- Update batch_id to mig_123
- Close `CF-mig122-RECURRENCE-203-REBUILD-PENDING`
- Open `CF-mig123-RECURRENCE-203-HARMONIZED-LINEAGE` if any tier definition was lossy
- Update table_signoff_registry notes
- Update `qc_framework_v1/REMAINING_WORK_INVENTORY.md`

---

## 4. Acceptance gates

- Pre-snapshot present in archive_pub_v1_0
- Dry-run report delivered to Logan + approval received before RW
- Post-rebuild row count = 10,871 (cohort preserved)
- `recurrence_confirmed=TRUE` count > 0 (non-shell)
- Per-tier evidence sources populated (recurrence_evidence_source NOT NULL on TRUE rows)
- Internal consistency: TRUE rows have non-null `recurrence_date` + valid `recurrence_evidence_source`; FALSE rows have NULL `recurrence_date`
- mig_123 closes CF-mig122-RECURRENCE-203-REBUILD-PENDING
- 5-gate audit re-run green after mig_123

---

## 5. Don't touch (active parallel lanes)

- `canonical_survival_followup_v1` — Lane 15 (still in flight)
- `canonical_molecular_genetics_from_notes_v2` — Lane 16 (still in flight)
- `canonical_recurrence_resolved_v1` — Sibling Lane 17 (depends on this rebuild for cross-validation post-rebuild)
- `canonical_table_signoff_registry_v1` / `canonical_column_verification_registry_v1` — Sibling Lane 18

⚠️ Lane 17 (recurrence_resolved) and this lane have a soft dependency: post-rebuild, Lane 17's verification of recurrence_resolved should re-confirm against the rebuilt recurrence_v1. If Lane 17 lands first, its result remains valid (recurrence_resolved is independent), but the cross-validation between the two will need a re-run.

---

## 6. Reference reading

Required:
- Auto-memory: `project_canonical_recurrence_v1_mig_122_closeout.md` (Cursor 14's shell-finding + 3 CFs)
- Auto-memory: `project_op_procedure_consolidation_script_362_closeout.md` (operative spine context)
- Auto-memory: `project_op_path_consolidation_script_361_closeout.md` (path family context)
- Auto-memory: `feedback_clinical_dates_calendar_only.md` (or 413 / clinical_date_retype_20260428.md per AGENTS.md)
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Repo: `scripts/203_canonical_recurrence.py` (SSOT builder — primary read)
- Repo: `qc_framework_v1/migrations/122_canonical_recurrence_v1_signoff.sql` (Cursor 14's mig_122)
- Repo: `qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql` (verification template post-script-rebuild)
- Repo: `qc_framework_v1/reports/date_normalization_pass1_report.md` (notes on `first_surgery_date` TIMESTAMP behavior)

---

## 7. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- 3 commits expected:
  1. Harmonized Script 203 + dry-run report (no DB writes)
  2. Pre-snapshot + RW rebuild (after Logan approval)
  3. mig_123 verification + memory updates
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Pre-snapshot: `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig123_20260429`
- Surgical git add (no `-A`, no `scripts/output/` sweeps)

---

## 8. If something unexpected surfaces

- Script 203 has unmappable logic (>1 path that can't be cleanly substituted) → stop, document, ask Logan how to proceed
- Cohort drift (rebuild produces ≠ 10,871 rows) → STOP — likely indicates Script 203's cohort-padding fallback isn't engaging correctly with new spine
- recurrence_confirmed=TRUE count seems implausibly high (e.g., > 30% of cohort) → likely a join exploding; investigate
- Pre-snapshot already exists in archive (re-run scenario) → suffix with `_v2` and document why
- Logan declines RW after dry-run → close lane with mig_123-DRY-RUN-ONLY documentation; CF-mig122-RECURRENCE-203-REBUILD-PENDING stays open
- New CFs surface during rebuild (e.g., date format drift in operative_events) → document but do not block the rebuild

---

End of prompt. Lane 19 of new 3-prompt batch. **Substantial RW lane — explicit Logan-approval gate at §3d.** Closes the most significant carry-forward in the recurrence family. After this lands, recurrence_v1 transitions from shell to fully populated; downstream consumers (ete_event_resolved, patient_master) can rely on real recurrence values.
