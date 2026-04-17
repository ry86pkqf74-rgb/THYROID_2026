# Script 270 Phase A Step 3 — Literal-'nan' Repair Recon

_generated: 2026-04-17T05:50:00+00:00 (corrected after `repair_action`-vs-`action` field-name fix)_

Read-only reconnaissance of `manuscript_workspace.nan_string_audit_v1_1` to
calibrate the `PRESERVE_RAW → REPAIRED_V270` decision rule before Step 3
executes any UPDATE. Per user direction (Q1): inspect actual distributions;
the prompt's `≤50 distinct + enum-name regex + ≥1 real NULL` rule is a
starting hypothesis, not approved.

## 1. Audit table headline

- **table:** `thyroid_canonical_publication_v1_0.manuscript_workspace.nan_string_audit_v1_1`
- **schema:** `column_name`, `n_literal_nan`, `n_true_null`, `n_real_values`, `n_distinct_real`, `repair_action`, `repaired_at`, `repaired_by`
- **rows:** 476 (one row per CPM VARCHAR column — exhaustive coverage)

**`repair_action` distribution:**

| repair_action | n |
|---|---|
| `NO_ACTION` | **475** |
| `PRESERVE_RAW` | **1** |

**Audit table totals:**

| metric | value |
|---|---:|
| total nan cells across all CPM VARCHAR cols | 9,517 |
| total true NULLs across all CPM VARCHAR cols | 3,822,107 |
| total real values across all CPM VARCHAR cols | 1,342,972 |
| cols with `n_literal_nan > 0` (in audit) | 1 |
| `repaired_at` MAX | NULL (no row has been repaired) |

## 2. The single PRESERVE_RAW row (the only Step 3 candidate)

| column | n_literal_nan | n_true_null | n_real_values | n_distinct_real | repair_action | repaired_at | repaired_by |
|---|---:|---:|---:|---:|---|---|---|
| `syn_margin_distance_mm_raw_str` | **9,517** | 2 | 1,352 | 52 | `PRESERVE_RAW` | NULL | NULL |

**Interpretation:** the `_raw_str` suffix explicitly signals
"preserve source representation". Repairing `'nan'` → SQL NULL would
destroy the audit-trail signal that the source document literally wrote
"nan" in this field. The column's purpose is to retain raw string form;
the cleaned/parsed numeric companion lives elsewhere (likely
`syn_margin_distance_mm` or `syn_margin_distance_mm_v*`).
**PRESERVE_RAW is correct by design.** This is not a missed-repair; it is
an intentional preservation.

## 3. Audit staleness verification (LIVE recompute)

Re-ran `COUNT(*) FILTER (WHERE col = 'nan')` for the first 25 audit rows
sorted by `column_name` and compared to the audit's stored `n_literal_nan`:

- **mismatches:** 0/25 (audit values match live state exactly)
- **last `repaired_at` timestamp:** NULL across all 476 rows (no actions taken — meaning the audit reflects the current natural state)

Then scanned **all 476 CPM VARCHAR columns** (full sweep, 28 seconds):

- **cols with literal `nan` (live):** 1 — only `syn_margin_distance_mm_raw_str`
- **total literal-nan cells (live):** 9,517 (matches audit-stored sum exactly)
- **cols with nan but NOT in audit table:** 0 (no drift since the audit was built)

**Conclusion: the audit is exhaustive, current, and correctly resolved.**

## 4. Verdict — Step 3 is a NO-OP

The prompt's mental model of "476 PRESERVE_RAW rows that need a categorical
vs free-text decision rule" is wrong. Reality:

- 475 cols have zero literal-nan cells → already `NO_ACTION` → nothing to do
- 1 col has 9,517 literal-nan cells → tagged `PRESERVE_RAW` by design (`_raw_str` suffix is the contract)

There are zero columns where the prompt's rule (`VARCHAR + ≤50 distinct +
enum-name regex + ≥1 real NULL co-exists`) would fire to convert any
`PRESERVE_RAW` row to `REPAIRED_V270`. The single PRESERVE_RAW col fails
the rule because:

- ✓ data_type = VARCHAR
- ✗ enum-name regex does NOT match (`raw_str` is not in the suffix list)
- ✓ `n_distinct_real = 52` — exceeds the `≤50` cutoff slightly
- ✓ `n_real_null = 2` — at least 1 real NULL co-exists
- AND its name explicitly carries `_raw_str` — strong free-text signal

Even with the prompt's rule loosened (`n_distinct ≤ 100`), the `_raw_str`
suffix should keep this column out of any auto-repair path.

## 5. Recommended Phase A Step 3 action

**Skip the `270b_phase_a_step_3_nan.py` script entirely.** Instead, fold a
single audit-table INSERT into Phase A's audit-writing step:

```sql
INSERT INTO manuscript_workspace.v1_1_finalization_audit_v1 VALUES (
  CURRENT_TIMESTAMP, '270', 'step_3_nan_repair_already_resolved',
  'cpm_varchar_cols_with_literal_nan',
  1,                       -- count_before: 1 PRESERVE_RAW col with 9517 nan cells
  1,                       -- count_after: same (no repairs warranted)
  0,                       -- target_after: 0 repairable (the 1 PRESERVE_RAW is by-design)
  'DOCUMENTED_NOOP',
  'Audit verified live by Script 270 nan recon (28-sec full scan): '
  '475 cols NO_ACTION (zero literal-nan); 1 col PRESERVE_RAW '
  '(syn_margin_distance_mm_raw_str, 9517 nan cells, n_distinct_real=52, '
  'n_real_null=2). The _raw_str suffix preserves source representation '
  'by design — repair would destroy provenance. The prompt''s '
  '"REPAIR if VARCHAR AND cardinality<=50 AND enum-name AND >=1 real NULL" '
  'rule does not fire on this column (suffix does not match enum regex; '
  'cardinality 52 just over cutoff; intentional preserve). 0 cells '
  'repaired; nan_string_audit_v1_1 retains current resolved state.'
);
```

This shrinks Phase A's commit count from two (`270b_phase_a_step_2_registry.py`
+ `270b_phase_a_step_3_nan.py`) to one — Step 3 becomes a 1-line audit
INSERT inside the Step 2 script (or a tiny `270c_phase_a_step_3_audit.py`
if you prefer file-per-step discipline).

## 6. STOP gate

Human reviewer to confirm:

1. The verdict (Step 3 = NO-OP, single audit row).
2. Whether the audit-row INSERT lives inside `270b_phase_a_step_2_registry.py`
   or as its own tiny `270c_phase_a_step_3_audit.py`.
3. Single-use script `scripts/_recon_270b_nan.py` to be deleted after the
   Step 3 INSERT lands (per single-use probe convention).
