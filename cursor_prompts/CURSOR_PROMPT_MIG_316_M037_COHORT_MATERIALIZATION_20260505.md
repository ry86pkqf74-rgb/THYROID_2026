# Cursor prompt — mig_316: M037 cohort materialization or naming reconciliation

**Agent:** cursor_composer
**Estimated time:** 30–60 min (1 SQL + verification + signoff)
**Priority:** P2 — blocks M037 follow-on work but M037 v1 submission is already shipped
**Closes:** `CF-M037-COHORT-MISSING`

## Resolution (applied 2026-05-05)

- **Frozen M037 v1 SSOT:** `manuscript_workspace.cohort_m037_ln_metastasis_v1` — malignant with **LN examined > 0 OR `ln_positive_flag = TRUE`** (see `M037_submission_package_v1_0/00_README.md`, mig_280). The logistic outcome is LN-positive (N1+) *within* that cohort (~50% LN+), **not** a cohort restricted to LN+ only.
- **Deliverable:** `manuscript_workspace.cohort_m037_ln_predictors_v1` **TABLE** = `cohort_m043_ln_predictors_v1` filtered with the **identical mig_280 predicate**. Verified: **n = 2,234**; `research_id` matches `cohort_m037_ln_metastasis_v1` with 0 symmetric diff.
- **Filter (MotherDuck `ln_positive_flag` is INTEGER — use mig_280 boolean comparison, not `IS TRUE`):**

```sql
WHERE (m.ln_total_examined > 0)
   OR (m.ln_positive_flag = CAST('t' AS BOOLEAN));
```

- **Repo SQL + signoff:** `qc_framework_v1/migrations/316_m037_cohort_ln_predictors_reconcile_20260505.sql` and `main.signoff_migration.mig_316`.

---

## Problem (historical)

The handoff brief and several signoff summaries reference `manuscript_workspace.cohort_m037_ln_predictors_v1`. That table does **not exist**. Querying it returns:

```
Catalog Error: Table with name cohort_m037_ln_predictors_v1 does not exist!
Did you mean "cohort_m043_ln_predictors_v1"?
```

What exists: `manuscript_workspace.cohort_m043_ln_predictors_v1` with N=4,019 (all malignant). **Actual M037 analytic cohort:** LN examined > 0 OR LN-positive *flag in CPM sense* (= mig_280), N=2,234 — broader than LN-positive-only (~1,124 N1+ within M037).

**Stale draft in §Recipe below incorrectly guessed “LN-positive only.” Ignore that; use the Resolution-block filter.**

## Recipe

### Step 1 — Confirm Option A vs Option B by inspecting the M037 submission package

```bash
cd /Users/loganglosser/THYROID_2026
ls M037_submission_package_v1_0/
grep -rn "cohort_m037\|cohort_m043\|N = 2,234\|N=2234" M037_submission_package_v1_0/ scripts/ 2>/dev/null | head -30
```

Confirm whether the frozen package uses **`cohort_m037_ln_metastasis_v1`** and the LN-eligibility wording (LN examined > 0 or LN+ flag), not `cohort_m037_ln_predictors_v1`.

### Step 2 — Materialize `cohort_m037_ln_predictors_v1` (implemented)

```sql
CREATE OR REPLACE TABLE manuscript_workspace.cohort_m037_ln_predictors_v1 AS
SELECT *
FROM manuscript_workspace.cohort_m043_ln_predictors_v1 AS m
WHERE (m.ln_total_examined > 0)
   OR (m.ln_positive_flag = CAST('t' AS BOOLEAN));
```

Validate:

```sql
SELECT COUNT(*) FROM manuscript_workspace.cohort_m037_ln_predictors_v1;
-- Expect ~2234 after CPM-aligned passes; symmetric diff vs cohort_m037_ln_metastasis_v1 should be 0.

WITH p AS (
  SELECT CAST(research_id AS VARCHAR) AS rid FROM manuscript_workspace.cohort_m037_ln_predictors_v1
), m AS (
  SELECT CAST(research_id AS VARCHAR) AS rid FROM manuscript_workspace.cohort_m037_ln_metastasis_v1
)
SELECT
  (SELECT COUNT(*) FROM p LEFT JOIN m ON p.rid = m.rid WHERE m.rid IS NULL) AS only_predictors,
  (SELECT COUNT(*) FROM m LEFT JOIN p ON p.rid = m.rid WHERE p.rid IS NULL) AS only_metastasis;
```

### Step 3 — Update brief template

Edit any file under `cursor_prompts/`, `studies/`, or root `*.md` that still references `cohort_m037_ln_predictors_v1` to either accept the new materialized table (Option A) or to use the canonical M043 name (Option B). `grep -rn "cohort_m037" /Users/loganglosser/THYROID_2026 --include="*.md"` to find them.

### Step 4 — Signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT 'mig_316', CURRENT_TIMESTAMP, 'cursor_composer_mig316',
  'mig_316: M037 cohort naming reconciliation. M037 v1 SSOT remains cohort_m037_ln_metastasis_v1 (mig_280 LN eligibility). Materialized manuscript_workspace.cohort_m037_ln_predictors_v1 TABLE as subset of cohort_m043_ln_predictors_v1 with same predicate; n=2234; rids match metastasis view. Closes CF-M037-COHORT-MISSING.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id='mig_316');
```
```

## Carry-forwards

Closes `CF-M037-COHORT-MISSING`. No new carry-forwards opened.

## Out of scope

- Do NOT republish or modify the M037 v1 submission package (`M037_submission_package_v1_0/`) — that's frozen.
- Do NOT modify `cohort_m043_ln_predictors_v1` itself.
- Do NOT run new analyses on M037 — this prompt is purely about resolving the naming gap.
