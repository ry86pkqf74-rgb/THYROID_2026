# MIG-003 — VC paresis Cortex Search re-validation (2026-05-06)

## Purpose

Re-validate whether `comp_vc_paresis_*` on `canonical_patient_master` should be deprecated (empty encoding) or repopulated (NLP gap), using Snowflake Cortex Search + BigQuery cross-check. **No clinical note text is stored in this file.**

## Step 1 — Cortex Search (Snowflake)

**Status:** **NOT RUN** — Snowflake CLI connection failed with `250001 Programmatic access token is invalid` (`snow sql -c thyroid_2026`).

**Operator remediation (required before this step can complete):**

1. Snowsight → Profile → Settings → Programmatic access tokens → create **generic-scope** PAT (not Cortex-only).
2. Update `~/.snowflake/config.toml` under `[connections.thyroid_2026]` → `password=<new-PAT>`.
3. Verify: `snow sql -c thyroid_2026 -q "SELECT CURRENT_TIMESTAMP() AS ok;"`

**SQL to run after auth succeeds** (results must be reviewed only inside Snowflake / secure exports; do not paste chunks into Cursor, commits, DFL notes, or `bq_migration_log_v1.notes`):

```sql
SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'THYROID_VALIDATION.PUBLIC.THYROID_NOTES_SEARCH',
  '{"query": "vocal cord paresis without paralysis OR vocal fold paresis OR right vocal cord paresis OR left vocal cord paresis", "limit": 50}'
)) AS hits;
```

**Classification rule (after rerun):** Count distinct `research_id` where the returned chunk(s) **explicitly** document paresis as distinct from paralysis, or paresis-only (no paralysis). Do not count boilerplate risk lists without a patient-specific finding.

## Step 1b — BigQuery structured baseline (completed)

Source: `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`

| Metric | Value |
|--------|------:|
| n_cpm | 10,871 |
| n_paresis_confirmed | 0 |
| n_paralysis_confirmed | 23 |
| n_paresis_only (paresis TRUE, paralysis not TRUE) | 0 |

SQL:

```sql
SELECT
  COUNT(*) AS n_cpm,
  COUNTIF(comp_vc_paresis_confirmed IS TRUE) AS n_paresis_confirmed,
  COUNTIF(comp_vc_paralysis_confirmed IS TRUE) AS n_paralysis_confirmed,
  COUNTIF(comp_vc_paresis_confirmed IS TRUE AND COALESCE(comp_vc_paralysis_confirmed, FALSE) IS NOT TRUE) AS n_paresis_only
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`;
```

## Step 2 — Decision (pending Step 1)

- **2A Deprecate:** If, after note review, **0** distinct genuine paretic cases → archive snapshot in **BigQuery** dataset `pub_archive` (not MotherDuck name `archive_pub_v1_0`): e.g. `comp_vc_paresis_columns_pre_archive_YYYYMMDD`, then standing-rule + H2 Limitations updates + follow-up issue to drop columns after grace period.
- **2B Repopulate:** If **≥1** distinct case → AI_CLASSIFY / promotion path; populate `comp_vc_paresis_confirmed` with signed migration.

**Dry-run policy:** Before any `CREATE TABLE` over CPM-derived snapshots, run `bq query --dry_run` and record estimated bytes processed.

## Current outcome

| Field | Value |
|-------|-------|
| n_distinct_paretic_from_notes | **Pending** (Cortex Search not executed) |
| Archive snapshot created | **No** |
| CPM `comp_vc_paresis_confirmed` updated | **No** |

## References

- `studies/hypothesis2_goiter_sdoh/canonical_gaps_report_20260506.md` §6
- `memory skill_snowflake_cortex_2026_05_04.md` — PAT scope note
- Linear: **THY-15**
