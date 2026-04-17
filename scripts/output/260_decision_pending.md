# Script 260 — Decision Pending: B III/IV molecular linkage coverage gap

**Status:** dry-run only. No mutations applied. No snapshot written. No new columns added.
**Run date (UTC):** 2026-04-17
**Branch:** `cleanup/v1_1_finalization-20260416`

## What was verified live (CPM hash baseline)

`fna_episode_master_v2`: 8,119 rows / 5,266 patients (matches ground truth in prompt).

`molecular_test_episode_v2`: 10,126 rows / 10,026 patients. Of these:
- **846** rows have a usable test date (`test_date_native` non-NULL OR
  `TRY_CAST(resolved_test_date AS DATE)` non-NULL).
- **9,280** rows have BOTH date columns NULL — undated.

Bethesda III/IV in `fna_episode_master_v2`:
- Rows: **1,920** across **1,685** patients (matches prompt).
- Patients with at least one MTE row (any date status): **1,613 / 1,685 = 95.7%** (matches prompt).
- Patients with at least one **dated** MTE row: **624 / 1,685 = 37.0%**.

## Achievable strict-90d coverage (per prompt invariant spec)

Building the linkage as the prompt specifies — `(research_id, +-90 days on test_date_native)` —
the actual achievable coverage is:

| Window  | Matched B III/IV rows | Pct of 1,920 |
|---------|-----------------------|--------------|
| 0 days  | small subset          | included in below |
| ±90 d   | **570**               | **29.69%**   |
| ±180 d  | 581                   | 30.26%       |
| ±365 d  | 597                   | 31.09%       |

Even relaxing the window to a full year only raises coverage to ~31% because the
underlying MTE date sparsity (9,280 / 10,126 rows undated) is the binding constraint.
Total candidate `linked_molecular_episode_id` writes across the **entire** FEM table
(not just B III/IV) is 841 with the ±90d rule.

## Why the prompt's 85% floor is not achievable as specified

The prompt's invariant requires:

> Coverage on Bethesda III/IV rows ≥ 85% (target 95%; report actual).

The same prompt also restricts the link-method enum to:

> values: `'date_window_90d'`, `'same_day'`, `'none'`

and demands:

> 0 FNA rows with `linked_molecular_episode_id` but NULL `molecular_link_confidence_days`.

These three rules are mutually inconsistent given the live MTE state. Date-window
matching alone cannot exceed ~31% because most MTE rows are undated. Hitting 85%
requires extending the link method enum to include a patient-level fallback that
necessarily produces NULL `molecular_link_confidence_days`. This was previously
flagged in `FINALIZATION_REPORT_v1_1.md §10` as a v1_2 candidate ("Molecular test
date imputation — 9,280/10,126 episodes lack `test_date_native` AND
`resolved_test_date`").

## Decision required

Pick one and reply in chat; Script 260 will then re-execute under the chosen path:

- **Path A (strict, low-coverage commit):** Apply with `--force`. Land the 570
  strict-90d links + the imaging hydration (delta computed at apply time).
  Coverage closes at ~29.7% B III/IV rows. Add a row to
  `manuscript_workspace.__conventions` with `convention_id='molecular_link_coverage'`
  documenting that the 85% target is blocked by upstream MTE date sparsity.
  Defer full coverage to v1_2 once `molecular_test_episode_v2` dates are imputed.

- **Path B (extend method enum, high-coverage commit):** Add a fourth allowed
  value `patient_fallback_no_date` to `molecular_link_method`. For B III/IV FNA
  rows where the patient has at least one dateless MTE row, link to one chosen
  for determinism (lowest `molecular_episode_id`) with NULL
  `molecular_link_confidence_days`. Relax invariant 4.b to allow NULL
  confidence_days when method = `patient_fallback_no_date`. Expected coverage:
  ~91-95% of B III/IV rows. Convention row also added documenting the relaxation.

- **Path C (defer entirely):** No mutation in this round. The two new columns
  (`molecular_link_confidence_days`, `molecular_link_method`) are not added.
  `linked_molecular_episode_id` remains 0/8,119 hydrated. The molecular linkage
  becomes a v1_2 task contingent on first imputing MTE dates (the §10 candidate).
  Imaging hydration (Script 260 step 5) still runs in a follow-up since it is
  independent and unblocked.

## What this script DID do (no mutation)

- Verified live ground truth above.
- Wrote `scripts/output/260_mol_linkage_coverage.json` with the dry-run breakdown.
- Wrote `scripts/output/260_run.log` with the full preview pipeline.
- Wrote `scripts/output/260_decision_log.json` flagging `floor_breach_note`.
- Did **not** create any `*_pre260_*` archive snapshot.
- Did **not** alter `fna_episode_master_v2` schema or rows.

To execute Path A, re-run as: `python3 scripts/260_hydrate_fna_links.py --apply --force`.
To execute Path B or C, the script needs a small edit; please pick the path first.
