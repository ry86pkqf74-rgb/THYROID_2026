# Cursor Prompt — mig_202 Script 366 Python source audit + fix (CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION)

**Date:** 2026-04-30
**Lane:** mig_202 / script366_python_source_audit_fix
**Batch (proposed):** `mig_202_script366_python_source_audit_fix_20260430`
**Predecessor:** mig_187 chain apply (`51e201a`) — Cowork patched live VIEW directly when Script 366 redeploy regressed by dropping `exam_date IS NOT NULL` filter from `nodule_agg`/`gland_agg`/`ln_agg` WHERE clauses. **Python source still has the bug.**
**Posture:** **READ-ONLY scoping + Python source fix.** No execute against MotherDuck (Cowork or Logan applies the fix via local Python redeploy when ready).
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** `scripts/366_canonical_us_exam_master_v2.py` (Python source).
**Tool recommendation:** **Cline + GPT-5.5** — diagnostic + Python authoring. Reasoning-heavy: trace the regression to the source line, propose minimal-impact fix.

---

## Background — what happened during mig_187

Cowork applied mig_187's R-A diff to `scripts/366_canonical_us_exam_master_v2.py`, ran the script with `--commit`, and observed the deployed VIEW had **18,672 rows** instead of expected ~11,880. Diagnostic probe found:

- 6,792 NULL-date rows leaked through because the deployed VIEW was missing the `exam_date IS NOT NULL` filter from 3 source aggregations (`nodule_agg`, `gland_agg`, `ln_agg`).
- The PRE-mig_187 VIEW (archived at `archive_pub_v1_0.canonical_us_exam_master_VIEW_v2_pre_mig187_20260430`) DID have those filters.
- The diff `scripts/366_canonical_us_exam_master_v2_patch_mig187_RA.diff` did NOT explicitly remove those filters.
- Cowork issued `CREATE OR REPLACE VIEW main.canonical_us_exam_master_VIEW_v2 AS …` directly to add the filters back. Live VIEW now correct: 11,880 rows.
- **Python source still has the bug.** Next `python scripts/366_canonical_us_exam_master_v2.py --commit` would re-introduce the bug.

---

## Mission

Audit `scripts/366_canonical_us_exam_master_v2.py` to find where the `exam_date IS NOT NULL` filter was lost, propose a minimal-diff fix that restores it for `nodule_agg`/`gland_agg`/`ln_agg`, and produce a verification protocol.

---

## Required scope

### §1 Diagnostic — find the regression source

Read `scripts/366_canonical_us_exam_master_v2.py` end-to-end. Specifically the `build_sql()` function or wherever the SQL CTE strings are assembled.

Look for:
1. The `nodule_agg` CTE — what's its WHERE clause? Should be `WHERE (CAST(is_aggregate_row AS BOOLEAN) IS DISTINCT FROM true) AND exam_date IS NOT NULL`. Currently emits without the `AND exam_date IS NOT NULL`.
2. The `nodule_2nd` CTE — same issue.
3. The `gland_agg` CTE — should be `WHERE exam_date IS NOT NULL`. Currently emits no WHERE clause.
4. The `ln_agg` CTE — same issue.

Determine if the bug is:
- (A) A specific commit/edit in repo history that dropped these clauses (use `git blame` / `git log -L`).
- (B) An f-string template variable that was supposed to interpolate but came out empty.
- (C) Conditional logic that evaluates to "skip filter" under some flag.
- (D) The mig_187 R-A diff itself somehow tangentially affected these (unlikely but verify).

### §2 Compare against the pre-mig_187 archived VIEW SQL

The Cowork CREATE OR REPLACE VIEW SQL that fixed the live VIEW (in `qc_framework_v1/reports/chain_188b_186b_185b_187_closeout_20260430.md`) is the canonical "what the VIEW should look like after Script 366 deploy". Use it as the diff target.

### §3 Author Python fix as unified diff

Author `scripts/366_canonical_us_exam_master_v2_fix_exam_date_filter_20260430.diff`:

- Restores `AND exam_date IS NOT NULL` on `nodule_agg` WHERE
- Restores `AND exam_date IS NOT NULL` on `nodule_2nd` WHERE
- Adds `WHERE exam_date IS NOT NULL` on `gland_agg`
- Adds `WHERE exam_date IS NOT NULL` on `ln_agg`
- Optionally cleans up: ensure `is_aggregate_row` filter consistent across nodule_agg/nodule_2nd
- Header: `# READY FOR LOGAN/COWORK APPLY; FIXES CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION`

### §4 Verification protocol

Author a verification recipe:
1. Apply the fix diff to Python source
2. Run `python scripts/366_canonical_us_exam_master_v2.py --commit` (Logan or Cowork)
3. Probe live MD: `SELECT COUNT(*) FROM main.canonical_us_exam_master_VIEW_v2` — expect **11,880** (NOT 18,672)
4. Probe `SELECT COUNT(*) FILTER (WHERE exam_date IS NULL) FROM main.canonical_us_exam_master_VIEW_v2` — expect **0**
5. Probe `SELECT exam_id_source, COUNT(*) FROM main.canonical_us_exam_master_VIEW_v2 GROUP BY 1` — expect 121 ln_nlp_only + 11,759 NULL (structured)
6. Re-run mig_171b §B/C/D to confirm no fallback events emerge

### §5 Audit/report

Author `qc_framework_v1/reports/mig_202_script366_python_source_audit_fix_20260430.md`:
- §1 root cause analysis (what changed, when, by what mechanism)
- §2 fix description (line-by-line)
- §3 verification protocol (above)
- §4 close-out: `CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION` will be CLOSED after Logan/Cowork applies the fix and verifies live VIEW state matches expectations.

---

## Governance reminders

- Read-only investigation against MD. No `query_rw`.
- Author = `Logan Glosser <logan.glosser@gmail.com>`.
- The Python diff is for Logan/Cowork apply OUTSIDE this lane.
- Do NOT actually run `python scripts/366_canonical_us_exam_master_v2.py --commit` from this lane — Cowork/Logan controls that step.

---

## Deliverables

1. `scripts/366_canonical_us_exam_master_v2_fix_exam_date_filter_20260430.diff`
2. `qc_framework_v1/reports/mig_202_script366_python_source_audit_fix_20260430.md`
3. `exports/mig202_script366_audit_20260430/git_blame_evidence.csv`

Commit message: `qc: mig_202 Script 366 Python source audit + fix (CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION; restores exam_date IS NOT NULL filters in 3+ CTEs)`

---

End of prompt.
