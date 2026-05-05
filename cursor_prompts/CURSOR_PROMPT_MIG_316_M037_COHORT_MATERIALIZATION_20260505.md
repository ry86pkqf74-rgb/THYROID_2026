# Cursor prompt — mig_316: M037 cohort materialization or naming reconciliation

**Agent:** cursor_composer
**Estimated time:** 30–60 min (1 SQL + verification + signoff)
**Priority:** P2 — blocks M037 follow-on work but M037 v1 submission is already shipped
**Closes:** `CF-M037-COHORT-MISSING`

## Problem

The handoff brief and several signoff summaries reference `manuscript_workspace.cohort_m037_ln_predictors_v1`. That table does **not exist**. Querying it returns:

```
Catalog Error: Table with name cohort_m037_ln_predictors_v1 does not exist!
Did you mean "cohort_m043_ln_predictors_v1"?
```

What does exist: `manuscript_workspace.cohort_m043_ln_predictors_v1` with N=4,019 (matches malignant cohort row count). M037 is described in the manuscript inventory as "LN predictors" with cohort N=2,234, while M043 is "LN multivariate" with TBD cohort. The brief framing suggests M037 is a **subset of M043** (likely the LN-positive subset — patients with positive lymph nodes who are eligible for LN-burden modeling).

Two plausible resolutions:

**Option A — M037 is genuinely a subset of M043.** Materialize `cohort_m037_ln_predictors_v1` as a filtered view over `cohort_m043_ln_predictors_v1` (LN-positive patients only).

**Option B — M037 was an earlier name for what is now M043.** Naming has drifted; the cohort_m037 reference in the handoff brief is stale and should be replaced with cohort_m043 everywhere.

The M037 v1 submission package (`M037_submission_package_v1_0/`) was frozen with cohort N=2,234, so **Option A is more likely correct** — the LN-positive subset of the broader N=4,019 malignant cohort.

## Recipe

### Step 1 — Confirm Option A vs Option B by inspecting the M037 submission package

```bash
cd /Users/loganglosser/THYROID_2026
ls M037_submission_package_v1_0/
grep -rn "cohort_m037\|cohort_m043\|N = 2,234\|N=2234" M037_submission_package_v1_0/ scripts/ 2>/dev/null | head -30
```

Confirm whether the M037 v1 manuscript text describes its cohort as "LN-positive subset" (Option A) or "all malignant patients" (Option B with naming drift).

### Step 2 (Option A) — Materialize cohort_m037_ln_predictors_v1

```sql
CREATE OR REPLACE TABLE manuscript_workspace.cohort_m037_ln_predictors_v1 AS
SELECT *
FROM manuscript_workspace.cohort_m043_ln_predictors_v1
WHERE ln_positive_flag = 1
   OR (ln_total_positive IS NOT NULL AND ln_total_positive > 0);
-- Acceptance: row count near 2,234 (frozen submission package N)
```

Validate:
```sql
SELECT COUNT(*) FROM manuscript_workspace.cohort_m037_ln_predictors_v1;
-- Acceptance: 2,100 ≤ n ≤ 2,400 (within ±10% of frozen 2,234 — small drift OK from CPM updates)
```

### Step 2 (Option B, alternative) — Naming reconciliation

If the M037 v1 manuscript actually used the broader N=4,019 cohort, then the cohort name was renamed M037 → M043 sometime between submission and the handoff brief. In that case:

```sql
-- Add a friendly alias so legacy queries don't break
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m037_ln_predictors_v1 AS
SELECT * FROM manuscript_workspace.cohort_m043_ln_predictors_v1;
```

And update `manuscript_inventory.md` (if it exists) and the handoff brief template to clarify the alias.

### Step 3 — Update brief template

Edit any file under `cursor_prompts/`, `studies/`, or root `*.md` that still references `cohort_m037_ln_predictors_v1` to either accept the new materialized table (Option A) or to use the canonical M043 name (Option B). `grep -rn "cohort_m037" /Users/loganglosser/THYROID_2026 --include="*.md"` to find them.

### Step 4 — Signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT 'mig_316', CURRENT_TIMESTAMP, 'cursor_composer_mig316',
  'mig_316: M037 cohort naming reconciliation. Investigation: M037 v1 submission used <N=...> cohort = <Option A: LN-positive subset / Option B: broader malignant cohort>. Resolution: <Option A: materialized cohort_m037_ln_predictors_v1 as LN-positive filtered table over cohort_m043; row count = <N> / Option B: created VIEW alias>. Updated handoff brief references. Closes CF-M037-COHORT-MISSING.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id='mig_316');
```

## Carry-forwards

Closes `CF-M037-COHORT-MISSING`. No new carry-forwards opened.

## Out of scope

- Do NOT republish or modify the M037 v1 submission package (`M037_submission_package_v1_0/`) — that's frozen.
- Do NOT modify `cohort_m043_ln_predictors_v1` itself.
- Do NOT run new analyses on M037 — this prompt is purely about resolving the naming gap.
