# mig_202 — Script 366 Python source audit + fix

**Carry-forward:** `CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION`
**Target:** `scripts/366_canonical_us_exam_master_v2.py`
**Database:** `thyroid_canonical_publication_v1_0`
**Run date:** 2026-04-30

## §1 Root cause analysis

The live `canonical_us_exam_master_VIEW_v2` had been hand-patched after mig_187 because redeploying Script 366 produced 18,672 rows rather than the expected 11,880 rows. The excess 6,792 rows were NULL-date source rows that entered through the Python-generated SQL for these CTEs:

- `nodule_agg`
- `nodule_2nd`
- `gland_agg`
- `ln_agg`

The Python source had retained the nodule `is_aggregate_row` guard but did not carry the defensive `exam_date IS NOT NULL` guard used by the corrected live VIEW / Script 389 pattern. `git blame` of the affected CTE range showed the core CTE template originated in the initial Script 366 build (`aea40436`, 2026-04-21), with later edits not intentionally removing the date filter. The mig_187 R-A change extended the exam universe with LN-NLP-only dates but did not itself require NULL-date shell rows. The failure mode was therefore a stale source-template omission, not an f-string interpolation or conditional branch issue.

Evidence artifacts:

- `exports/mig202_script366_audit_20260430/git_blame_evidence.csv`
- `exports/mig202_script366_audit_20260430/git_blame_115_171_porcelain.txt`

## §2 Fix description

Applied the minimal Python source fix in `scripts/366_canonical_us_exam_master_v2.py`:

1. `nodule_agg`: added `AND exam_date IS NOT NULL` after `WHERE is_aggregate_row IS NOT TRUE`.
2. `nodule_2nd`: added the same `AND exam_date IS NOT NULL` guard.
3. `gland_agg`: added `WHERE exam_date IS NOT NULL` before grouping.
4. `ln_agg`: added `WHERE exam_date IS NOT NULL` before grouping.

A ready-to-apply patch artifact was also emitted:

- `scripts/366_canonical_us_exam_master_v2_fix_exam_date_filter_20260430.diff`

## §3 Verification protocol and executed evidence

Executed checks:

```text
.venv/bin/python -m py_compile scripts/366_canonical_us_exam_master_v2.py
ruff check scripts/366_canonical_us_exam_master_v2.py
```

Both passed.

Static SQL-generation check confirmed all expected guards are present:

```text
{'nodule_agg_filter': True, 'nodule_2nd_filter': True, 'gland_agg_filter': True, 'ln_agg_filter': True, 'ln_events_filter': True}
```

The fixed Script 366 was redeployed with `--commit`:

```text
[04:22:15.376Z]   CREATE OR REPLACE VIEW thyroid_canonical_publication_v1_0.main.canonical_us_exam_master_VIEW_v2
[04:22:16.785Z]   rows=11880  pts=4385  preop=7933  with_ln=6801  ln_nlp_only_exams=121
```

Post-deploy live VIEW probes:

```text
row_count = 11,880
null_exam_date_count = 0
exam_id_source counts:
  <NULL>      11,759
  ln_nlp_only    121
```

mig_171b G8/G9 validation status after the redeploy remained green:

```text
G8_events_resolve_existing_exam_master: PASS observed=0 expected=0
G9_fallback_exam_ids_pending_rebuild:  PASS observed=0 expected=0
canonical_us_lymph_node_events_v2 exam_id_source: exam_master_reused=6,973
```

## §4 Close-out

`CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION` is closed by mig_202. The Python source now reproduces the corrected live VIEW behavior: 11,880 rows, zero NULL exam dates, and the expected 11,759 structured-shell + 121 LN-NLP-only split.
