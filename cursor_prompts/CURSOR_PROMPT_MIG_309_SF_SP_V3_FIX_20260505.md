# Cursor Prompt — mig_309: VALIDATE_ALL_COHORTS SP v3 fix

**Agent:** Cursor Composer
**Estimated time:** 1–2 hours
**Date:** 2026-05-05
**Supersedes:** `CURSOR_PROMPT_MIG_305_SF_BASELINE_V3_SP_FIX_20260504.md` (mig_305 hung; this is the surgical fix)

## Problem statement

Snowflake stored procedure `VALIDATE_ALL_COHORTS()` is currently at v2 (17 checks, all PASS). v3 was supposed to add information_schema-based row-count drift checks but **hangs on `INFORMATION_SCHEMA.TABLES` queries inside the SP body** when iterating across 7 cohort tables. The hang appears related to query pruning / metadata cache misses inside SP execution context.

**Carry-forward:** CF-mig_305-SP-V3-HANG.

## Fix strategy (apply in this order)

### Option A (preferred): materialize once, then loop
Pre-compute the metadata snapshot **outside** the iteration loop, store in a session table, and reference that table inside the loop body. Avoids per-iteration `INFORMATION_SCHEMA` cost.

```sql
CREATE OR REPLACE PROCEDURE VALIDATE_ALL_COHORTS_V3()
RETURNS TABLE (cohort_id VARCHAR, check_name VARCHAR, status VARCHAR, observed BIGINT, expected BIGINT, delta BIGINT)
LANGUAGE SQL
AS
$$
DECLARE
  -- ... same 17 v2 checks ...
BEGIN
  -- v3 ADDITION: pre-materialize metadata
  CREATE OR REPLACE TEMP TABLE _cohort_meta AS
  SELECT table_name, row_count
  FROM INFORMATION_SCHEMA.TABLES
  WHERE table_schema = 'PUBLIC'
    AND table_name IN (
      'CANONICAL_PATIENT_MASTER_FLAT',
      'COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT',
      'COHORT_M032_DESCRIPTIVE_25YR_V1_FLAT',
      'COHORT_M037_LN_METASTASIS_V1_FLAT',
      'COHORT_M038_MASSIVE_GOITER_V1_FLAT',
      'COHORT_M044_AJCC_ETE_V1_FLAT',
      'COHORT_M025_NODULE_LEVEL_V1_FLAT'
    );

  -- Now iterate _cohort_meta instead of INFORMATION_SCHEMA inside the loop
  -- ...
END
$$;
```

### Option B (fallback): SHOW TABLES + RESULT_SCAN
If Option A still hangs (metadata pruning issue inside SP), use `SHOW TABLES IN SCHEMA PUBLIC` + `RESULT_SCAN(LAST_QUERY_ID())`:

```sql
SHOW TABLES IN SCHEMA PUBLIC;
INSERT INTO _cohort_meta
SELECT "name", "rows"::BIGINT FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" IN ('CANONICAL_PATIENT_MASTER_FLAT', ...);
```

### Option C (last resort): hardcode expected rows
If both fail, drop the metadata join entirely and embed the 7 expected row counts as literals in the SP. Pros: fastest. Cons: drift must be re-locked manually after each refresh. Acceptable for a v3 ship if A and B both fail.

## Validation

After deploy:
```sql
CALL VALIDATE_ALL_COHORTS_V3();
```

Expected: 24 rows (17 v2 checks + 7 row-count drift checks), all status='PASS'. If any FAIL, halt and report.

## Mirror to MotherDuck

Per mig_293b pattern, mirror the v3 run log to MotherDuck:
```sql
-- MotherDuck side
INSERT INTO main.cowork_sf_validation_log_v1
SELECT * FROM TABLE_FROM_SF_RESULT;  -- via 35_pull_sf_validation_log.py
```

## Signoff

After all 24 checks PASS:
```sql
-- MotherDuck thyroid_canonical_publication_v1_0
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_309', CURRENT_TIMESTAMP, 'cursor_composer_mig309',
  'mig_309: VALIDATE_ALL_COHORTS_V3 SP deployed via [Option A|B|C]. 24 checks PASS (17 v2 + 7 row-count drift). Mirrored to main.cowork_sf_validation_log_v1. Closes CF-mig_305-SP-V3-HANG.');

-- Then update mig_305 retroactive note
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_305', CURRENT_TIMESTAMP, 'cursor_composer_mig309_retro',
  'mig_305: SF SP v3 attempt hung on INFORMATION_SCHEMA.TABLES iteration inside SP body. Superseded by mig_309 ([Option] strategy). v2 SP (17 checks) remained the floor between 2026-05-04 and mig_309 deploy.');
```

## Connection

Snowflake: `~/.snowflake/config.toml` `[connections.thyroid_2026]`. PAT is Cortex-scoped; SP creation requires a non-Cortex PAT or `snow sql` via SSO. **Logan must run the CREATE PROCEDURE step manually if PAT lacks DDL scope.** Cursor agent can pre-write the SQL and stage it in `snowflake_trial/sql_drops/mig_309_sp_v3.sql`.

## Out-of-scope

- Adding new checks beyond row-count drift — defer to mig_311+
- Cross-platform validation (SF↔MD) — already covered by mig_293b mirror
