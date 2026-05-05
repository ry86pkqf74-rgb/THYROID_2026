# Cortex Analyst bind walkthrough — M025 nodule-level semantic model

**Date:** 2026-05-05
**Closes:** CF-CORTEX-ANALYST-NEEDS-BIND
**Estimated time:** 10–15 minutes

## Why this matters

The M025 nodule-level Cortex Analyst semantic model is staged at:
```
snowflake_trial/semantic_models/m025_nodule_level_semantic_model.yaml
```

Until it's bound in Snowsight, `cortex analyst query "..."` (CLI) and any chat-style natural-language query over the M025 grain returns "no semantic model found." Once bound, you get NL→SQL over `COHORT_M025_NODULE_LEVEL_V1_FLAT` without writing SQL.

## Why Cowork can't do this for you

Two reasons:
1. The bind requires Snowsight UI clicks (no public API for `CREATE SEMANTIC MODEL` from a YAML on local disk that I have access to).
2. Your PAT is Cortex-scoped and doesn't have the warehouse role needed to upload to a Snowflake stage from CLI.

So this is a **5-step manual walk** through the Snowsight UI. Cursor agent could do it via Snowflake's Python SDK if it has a non-Cortex PAT, but the cleanest path is you doing it once.

## Step 1 — Upload the YAML to a Snowflake stage

In Snowsight:
1. Navigate to **Data → Databases → THYROID_VALIDATION → PUBLIC → Stages**
2. If `CORTEX_ANALYST_STAGE` doesn't exist, create it:
   ```sql
   CREATE STAGE IF NOT EXISTS THYROID_VALIDATION.PUBLIC.CORTEX_ANALYST_STAGE
     ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
   ```
3. Click into the stage → **+ Files** → upload `m025_nodule_level_semantic_model.yaml` from your local checkout.

CLI alternative (if `snow sql` ever gets warehouse scope):
```bash
snow stage copy snowflake_trial/semantic_models/m025_nodule_level_semantic_model.yaml \
  @THYROID_VALIDATION.PUBLIC.CORTEX_ANALYST_STAGE/
```

## Step 2 — Bind in Cortex Analyst

In Snowsight:
1. Top-left waffle menu → **AI & ML → Cortex Analyst**
2. Click **+ Semantic model**
3. Pick **Use existing semantic model file**
4. Stage path: `@THYROID_VALIDATION.PUBLIC.CORTEX_ANALYST_STAGE/m025_nodule_level_semantic_model.yaml`
5. Display name: `M025 nodule-level (TI-RADS performance)`
6. Click **Save**

Snowsight validates the YAML (catches column-name typos, missing tables, malformed verified queries). If validation fails, fix the YAML, re-upload, retry.

## Step 3 — Smoke test

In the Cortex Analyst chat panel (right side after bind):
```
What's the per-TR ROM in the strict-eligible cohort?
```

Expected answer: SQL generated against `COHORT_M025_NODULE_LEVEL_V1_FLAT`, results include TR1 N=51 / TR2 31 / TR3 1,141 / TR4 1,464 / TR5 1,000 with ROM percentages matching the locked Wilson CIs (TR4 18.7%, TR5 26.1%).

If the SQL is wrong, edit the YAML's `verified_queries` block and re-upload.

## Step 4 — CLI verification

From terminal:
```bash
cortex analyst query "what is the AUC for the nodule-level cohort"
# Expected: 0.6399 (or routing to m025_auc_summary table if you exposed it)
```

If `cortex analyst query` returns "no semantic model bound to current role," check that your PAT's role has `USAGE` on the semantic model object. Per docs, the role that creates the semantic model auto-owns it.

## Step 5 — Sign off

Once Step 3 returns correct numbers, mark the carry-forward closed by either:
- Telling me ("bound, smoke test passed") and I'll insert the signoff row
- Or run yourself:
  ```sql
  INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
  VALUES ('mig_311', CURRENT_TIMESTAMP, 'logan_manual',
    'mig_311: M025 nodule-level Cortex Analyst semantic model bound in Snowsight. Stage = @THYROID_VALIDATION.PUBLIC.CORTEX_ANALYST_STAGE. Smoke test PASS (per-TR ROM and AUC reproduce locked numbers). Closes CF-CORTEX-ANALYST-NEEDS-BIND.');
  ```

## After this works — replicate the pattern

Once one bind works end-to-end, you can produce semantic models for the other 4 grains in a single afternoon:

| Grain | Driving cohort | Headline NL query to test |
|---|---|---|
| M025 patient-level | `cohort_m025_tirads_performance_v1` | "patient-level ROM by TR1-TR5" |
| M032 25-yr | `cohort_m032_descriptive_25yr_v1` | "malignancy rate by year of surgery" |
| M037 LN | `cohort_m037_ln_metastasis_v1` | "LN-positive rate by tumor size band" |
| M038 massive goiter | `cohort_m038_massive_goiter_v1` | "complication rate massive vs non-massive" |
| M044 ETE | `cohort_m044_ajcc_ete_v1` | "recurrence rate by ETE grade" |

The cursor agent can scaffold those 5 YAML files from the M025 template in one cursor prompt batch (mig_312).

## Troubleshooting

- **"Database THYROID_VALIDATION not found"**: your PAT role lacks `USAGE` on the database. Check role grants in Snowsight → Admin → Users & Roles.
- **"Table COHORT_M025_NODULE_LEVEL_V1_FLAT not found"**: the SF mirror of the MD nodule-level cohort hasn't been built yet. Run `scripts/01_export_canonical_to_sf.py` (per `snowflake_trial/SF_INFRASTRUCTURE_REGISTRY.md`).
- **"Semantic model validation failed: missing measure"**: open the YAML, ensure every measure references a column that exists in the SF flat table. Mismatches between MD column names and SF mirror column names are the most common cause (SF uppercases everything).
