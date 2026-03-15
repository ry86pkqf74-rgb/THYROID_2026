# Materialization MAP Deduplication — 2026-03-14

## Status

**No data-layer changes required.**  
The `MATERIALIZATION_MAP` in `scripts/26_motherduck_materialize_v2.py` currently
contains **220 unique entries** with zero md_\* target duplicates and zero source
duplicates.

The 6 names flagged by `val_materialization_perf_v1` were **false positives** caused
by a bug in `check_map_duplicates()` (script 85).

---

## Root Cause Analysis

### The 6 Names Reported as Duplicates

| md_\* target | Source table | Canonical MAP line |
|---|---|---|
| `md_pathology_recon_review_v2` | `pathology_reconciliation_review_v2` | line 66 |
| `md_molecular_linkage_review_v2` | `molecular_linkage_review_v2` | line 67 |
| `md_rai_adjudication_review_v2` | `rai_adjudication_review_v2` | line 68 |
| `md_imaging_path_concordance_v2` | `imaging_pathology_concordance_review_v2` | line 69 |
| `md_op_path_recon_review_v2` | `operative_pathology_reconciliation_review_v2` | line 70 |
| `md_lineage_audit_v1` | `lineage_audit_v1` | line 186 |

Each name appears **exactly once** in the MAP list literal.  They also appear a
second time in the file — but _outside the MAP_, inside `.replace()` calls used
to rewrite SQL template strings for the cross-DB materialization step
(`MANUAL_REVIEW_QUEUE_SUMMARY_SQL`, lines 598–613; `SURVIVAL_COHORT_ENRICHED_SQL`
replacement at line 629).

### The Bug in `check_map_duplicates()`

Script 85's original implementation used a line-by-line scan with a stopping
condition of `line.strip() == "]"`:

```python
if in_map and line.strip() == "]":
    break
```

This condition fired correctly when the function ran on the version that generated
the CSV — at that time the function **did not break** (evidence: the without-break
simulation found both line 304 and line 676 as break points, confirming the
function "fell through" and scanned into the code body).  Results were stored in
`exports/final_md_optimization_20260314/materialization_performance_20260314_2022.csv`.

Whether the break failed due to a trailing whitespace or comment on the `]` line in
the on-disk version at that moment, or due to a transient encoding issue, the result
is the same: the scanner reached lines 598–629 and counted those string literals as
additional MAP entries.

---

## Fixes Applied

### 1. Module-level uniqueness guard — `scripts/26_motherduck_materialize_v2.py`

Added immediately after the MAP's closing `]`:

```python
_mm_md_names  = [t[0] for t in MATERIALIZATION_MAP]
_mm_src_names = [t[1] for t in MATERIALIZATION_MAP]
if len(_mm_md_names) != len(set(_mm_md_names)):
    _dup_md = [k for k in set(_mm_md_names) if _mm_md_names.count(k) > 1]
    raise ValueError(
        f"MATERIALIZATION_MAP: duplicate md_* target names detected — "
        f"fix before running: {_dup_md}"
    )
if len(_mm_src_names) != len(set(_mm_src_names)):
    _dup_src = [k for k in set(_mm_src_names) if _mm_src_names.count(k) > 1]
    raise ValueError(
        f"MATERIALIZATION_MAP: duplicate source names detected — "
        f"same table would be materialized twice: {_dup_src}"
    )
del _mm_md_names, _mm_src_names
```

**Effect:** any future accidental duplicate will raise `ValueError` at import time,
preventing the script from running at all.

### 2. Rewritten `check_map_duplicates()` — `scripts/85_materialization_performance_audit.py`

Replaced the fragile line-by-line scan with **bracket-depth counting** on the raw
file text.  The function now:

1. Locates the opening `[` of the MAP literal using the full annotation marker string.
2. Walks characters incrementing/decrementing a depth counter.
3. Stops precisely at the matching `]` — regardless of indentation, trailing
   comments, or other `]` characters elsewhere in the file.
4. Extracts only `("md_name", "src_name")` pairs within that slice.
5. Returns duplicates for **both** md_\* names and source names
   (source dupes prefixed with `"src:"`).

### 3. New test — `tests/test_materialization_map.py`

Six `pytest` tests that run without a database connection:

| Test | What it checks |
|---|---|
| `test_map_is_parseable` | MAP has ≥1 entry and was parsed successfully |
| `test_no_duplicate_md_targets` | Every md_\* target appears exactly once |
| `test_no_duplicate_source_names` | Every source table maps to exactly one md_\* name |
| `test_all_md_targets_have_md_prefix` | Naming-convention enforcement |
| `test_no_self_mapping` | No entry maps a name to itself |
| `test_entry_count_not_below_baseline` | Silent-deletion guard (baseline = 220) |
| `test_script26_imports_cleanly` | Module-load guard fires on actual duplicates |

Run with:
```
.venv/bin/python -m pytest tests/test_materialization_map.py -v
```

---

## Final MAP State

| Metric | Value |
|---|---|
| Total entries | 220 |
| Unique md_\* targets | 220 |
| Unique source names | 220 |
| Duplicate md_\* targets | **0** |
| Duplicate source names | **0** |
| False positives in prior CSV | 6 (all resolved to code-body `.replace()` calls) |

---

## Materialization Contract

The `MATERIALIZATION_MAP` is the **single source of truth** for what gets pushed
to MotherDuck. The following rules are now enforced at multiple levels:

| Level | Mechanism |
|---|---|
| Module load | `ValueError` guard in script 26 |
| CI / local test | `tests/test_materialization_map.py` (pytest) |
| Audit run | `check_map_duplicates()` in script 85 (now bracket-depth based) |
| Code review | Every new MAP entry must have a unique md_\* name and a unique source |

### Rules for Adding a New Entry

1. Choose an `md_` name not already in the MAP — search script 26 with `grep "md_name"`.
2. Confirm the source table/view name is not already mapped — search for it on the
   right-hand side.
3. Add as a 2-tuple with a section comment identifying the originating script.
4. Run `pytest tests/test_materialization_map.py` before committing.

### Rules for Removing an Entry

1. Remove the tuple from the MAP.
2. Update `_BASELINE_COUNT` in `tests/test_materialization_map.py` (decrement by
   the number of removed entries).
3. If the md_\* table is referenced in `.replace()` calls or dashboard queries,
   update those references too.

---

## Artefacts

| File | Contents |
|---|---|
| `exports/materialization_map_dedup_20260314/map_manifest.csv` | Full MAP dump with duplicate flags |
| `exports/materialization_map_dedup_20260314/dedup_report.json` | Machine-readable summary |
| `tests/test_materialization_map.py` | Pytest guard (6 tests) |

---

_Generated: 2026-03-14 | Author: GitHub Copilot_
