# Cursor Composer Dispatch — mig_260: Re-point M037/M025/Prompt-7 to live CPM TIRADS columns (post-mig_265 cleanup)

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex round-6, MD-direct).
**Lane:** mig_260 — live `main.canonical_patient_master` has only **5 NLP TIRADS columns**, but Snowflake `CANONICAL_PATIENT_MASTER_FLAT` (cached pre-mig_265 export) has **28+**. M037/M025 cohort definitions and Prompt 7 still reference the legacy column names (`tirads_best_category_v12`, `imaging_tirads_best`, `preop_tirads_best`) which no longer exist on live CPM. On next Snowflake re-export, every dependent query will silently break or return NULL.
**Recommended agent:** **Cursor Composer** — mechanical re-point. The replacement source columns already exist (`canonical_us_patient_master_VIEW_v2.max_tirads_category_ever` is the cleanest substitute).
**Estimated runtime:** 60–75 min
**Triggered by:** Round 6 CF-mig260f-CPM-TIRADS-MIGRATION-DRIFT.
**Severity:** HIGH. M037 (LN predictors) Table 1 + M025 (TIRADS performance) cohort + Prompt 7 (TIRADS×ROM) all break on next Snowflake re-export.
**Closes carry-forward:** CF-mig260f.

---

## §0 — First message to paste into Cursor Composer

> mig_260 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_260_CPM_TIRADS_DRIFT_REPOINT_20260501.md` end-to-end. The fix is NOT a CPM update — the live CPM is correct (post-mig_265 cleanup). The fix is to re-point downstream code: M037 cohort view, M025 cohort definition, snowflake_trial Prompt 7 + M037 Table 1 scripts, and any `manuscript_workspace.cohort_*` views that reference removed TIRADS columns. Source-of-truth substitute is `main.canonical_us_patient_master_VIEW_v2.max_tirads_category_ever`.

---

## §1 — Why this lane exists

mig_265-style cleanup pruned 28+ TIRADS columns from `main.canonical_patient_master`. The live CPM now has only 5 NLP TIRADS columns:
- `nlp_tirads_has_data`
- `nlp_tirads_max_score`
- (3 more NLP cols)

The columns that downstream code references (and Snowflake mirror still has) are gone:
- `tirads_best_category_v12` ❌ removed
- `imaging_tirads_best` ❌ removed
- `preop_tirads_best` ❌ removed
- `tirads_best_*` cluster ❌ removed

Replacement live sources:
- `main.canonical_us_patient_master_VIEW_v2` — 7 TIRADS cols including `max_tirads_category_ever`, `worst_tirads_ever`, `count_us_with_tirads_categorization`
- `views_readable.Patient_Master_Canonical` — 42 TIRADS cols (full reflective view; preferred if many cols needed)

## §2 — Pre-task probes

```sql
-- Probe 1: confirm live CPM TIRADS state
SELECT column_name FROM information_schema.columns
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND table_schema = 'main' AND table_name = 'canonical_patient_master'
  AND LOWER(column_name) LIKE '%tirads%'
ORDER BY column_name;

-- Probe 2: confirm replacement source
SELECT column_name FROM information_schema.columns
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND table_schema = 'main' AND table_name = 'canonical_us_patient_master_VIEW_v2'
  AND LOWER(column_name) LIKE '%tirads%'
ORDER BY column_name;

-- Probe 3: which downstream artifacts reference removed columns?
-- (run in repo, not MD)
-- grep -rE "tirads_best_category_v12|imaging_tirads_best|preop_tirads_best" \
--   manuscript_workspace/ scripts/ snowflake_trial/ M044*/ M037*/ M025*/
```

Surface the grep hits + replacement-column list to Logan before any edit.

## §3 — Apply (downstream code edits, not MD DML)

### 3a. snowflake_trial scripts to re-point
- `snowflake_trial/scripts/13_prompt7_tirads_bethesda.py` — currently auto-picks `NLP_TIRADS_HAS_COMPONENT_DETAIL`. Change to JOIN against `canonical_us_patient_master_VIEW_v2_FLAT` and use `MAX_TIRADS_CATEGORY_EVER`.
- `snowflake_trial/scripts/09_m037_table1.py` — does not currently reference TIRADS, but if the M037 Table 1 ever adds a TIRADS column, source from the view.

### 3b. cohort views to update in MD
```sql
-- COHORT_M037_LN_PREDICTORS already lives in Snowflake — re-derive it
-- with TIRADS pulled from canonical_us_patient_master_VIEW_v2 if needed.
-- NOT REQUIRED for v1 — only if M037 adds TIRADS as a predictor.

-- M025 cohort (TIRADS performance manuscript)
-- — search manuscript_workspace for cohort_m025_*; if it references
-- removed CPM TIRADS cols, rebuild against canonical_us_patient_master_VIEW_v2
```

### 3c. Snowflake re-export cascade
After Logan re-runs `01_export_md_to_parquet.py`, the live CPM (5 NLP TIRADS cols) will overwrite the Snowflake mirror. The 28+ legacy TIRADS columns disappear from `CANONICAL_PATIENT_MASTER_FLAT`. Any persisted cohort view in Snowflake that depends on them needs to be rebuilt against the new flat schema.

Add a new Snowflake table for the US patient master:
```python
# Add to snowflake_trial/scripts/01_export_md_to_parquet.py TABLES list:
"canonical_us_patient_master_VIEW_v2"  # ← replaces lost TIRADS cols
```
Re-run the 3-script pipeline and a new flat view will appear.

## §4 — Verify

```bash
# After Logan re-exports + reloads:
SNOWFLAKE_PAT='...' python -c "
import sys; sys.path.insert(0,'snowflake_trial/scripts')
from _sf_client import get_cursor
ctx, cur = get_cursor()
cur.execute('SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \\'CANONICAL_PATIENT_MASTER_FLAT\\' AND LOWER(COLUMN_NAME) LIKE \\'%tirads%\\'')
print([r[0] for r in cur.fetchall()])
ctx.close()
"
# Expect: 5 NLP TIRADS columns only
```

Then re-run Prompt 7:
```bash
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/13_prompt7_tirads_bethesda.py
# Expect a real TIRADS×ROM table grouped by max_tirads_category_ever (TR1-TR5)
# matching round-6 §4 numbers (TR1=346 / TR2=300 / TR3=852 / TR4=495 / TR5=1,403)
```

## §5 — Carry-forwards
- CF-mig260f-CPM-TIRADS-MIGRATION-DRIFT → CLOSED on apply
- CF-mig260-M025-COHORT-REBUILD (open if M025 cohort view existed and referenced removed cols)

## §6 — Surgical git add
```
snowflake_trial/scripts/13_prompt7_tirads_bethesda.py  (re-point edits)
snowflake_trial/scripts/01_export_md_to_parquet.py     (add canonical_us_patient_master_VIEW_v2)
manuscript_workspace/cohort_m025_*.sql                  (if exists; re-point)
```
