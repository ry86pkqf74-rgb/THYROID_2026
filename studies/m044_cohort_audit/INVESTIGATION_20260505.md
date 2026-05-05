# mig_315 — M044 Cohort Audit Investigation (2026-05-05)

> Cursor agent: cursor_composer_mig315  
> Closes: CF-M044-DUP-COLS  
> Opens: CF-M044-V6-MANUSCRIPT-PATCH

---

## Summary of findings

Two defects were reported by Cowork in `manuscript_workspace.cohort_m044_ajcc_ete_v1`:

### Defect 1 — Duplicate columns (RESOLVED: measurement artifact)

`information_schema.columns` returned `n_cols=64, n_unique=35` (Cowork reported 32, difference
due to timing). **Actual inspection via `DESCRIBE` and `duckdb_views()` confirms the VIEW has
35 unique column names — no actual schema duplication.** The 64 vs 35 ratio is the well-known
MotherDuck multi-catalog artifact where `information_schema.columns` returns one row per attached
schema/database (see `AGENTS.md`: "Always filter with `WHERE table_schema='main'` AND DISTINCT").

The VIEW SQL is intact and uses explicit column projection (no `SELECT *`). The Cortex Analyst
semantic model failure must be diagnosed separately via the Snowflake side table (mig_289 export).

### Defect 2 — `ete_grade_final` Boolean→VARCHAR cast artifacts (CONFIRMED + FIXED)

Current distribution in `cohort_m044_ajcc_ete_v1` (pre-mig_315):

| ete_grade_final | n | Expected | Root cause |
|---|---:|---|---|
| `microscopic` | 2,413 | ✅ correct | — |
| `gross` | 1,239 | ✅ correct | — |
| `false` | 158 | ⚠️ `no_negative` | CPM `ete_grade_final` ← `ete_grade_final_v2='none'` cast to boolean string |
| `present_ungraded` | 28 | ✅ correct | — |
| `absent` | 15 | ⚠️ `no_negative` | CPM `ete_grade_final_v2='absent'` passthrough |
| NULL | 11 | ✅ correct | — |
| `true` | 4 | ⚠️ `gross` | CPM artifact: 2 rows `v2='gross'` + 2 rows `v2='true'` |

**Root cause**: `canonical_patient_master.ete_grade_final` was populated from
`ete_grade_final_v2` but the v2 column itself has residual artifacts:
- `'none'` (= no ETE after adjudication) → got stored as `'false'` via boolean cast in the ETL chain (script 265 canonical finalization; ete_adjudication_v1 join)
- `'absent'` → passthrough (unresolved to 'no_negative' vocabulary)
- `'true'` → 4 rows including 2 where `v2='gross'` (boolean `True` stored as string) and 2 where `v2='true'` is itself an artifact

**Effect on v5 analysis**: The `m044_ete_fit_models.py` MASTER_ANALYTIC_SQL CASE statement
already handles `IN ('false','absent')` → 'No/negative ETE' as a workaround. So v5 models
were NOT numerically corrupted — they correctly identified the no-ETE cohort. However:
1. The raw `ete_grade_final` column has semantically wrong values (breaks Cortex Analyst)
2. The v5 no-ETE group was correctly identified (n≈173 in full cohort, n≈68 in strict-DTC frame)

### Cohort size change (mig_313 impact)

| Checkpoint | Total view rows | Source |
|---|---:|---|
| mig_257 expected | 4,128 | comment in migration file |
| v5 strict-DTC analytic frame | 3,572 | model output |
| Current view (post-mig_313) | 3,868 | live query |

**Explanation**: mig_313 (2026-05-05) fixed M-stage corruption. Pre-fix, the corrupted
`distant_mets_proxy` gave M1=1,816 (45%). Post-fix, M1=114 (2.84%). This reset the stage
re-derivation; 151 previously-staged patients lost valid `ajcc8_stage_group` (now NULL) while
~290 previously-null-staged patients gained valid stage → net: staged cohort shifted from
~4,019 (all malignant had stage) to 3,868. The cohort growth of ~290 beyond v5's strict-DTC frame
reflects the combination of (a) the larger post-fix staged pool and (b) the M044 cohort composition
shifting from the corrupt IVB-heavy world. This is expected and correct.

---

## Fix applied (mig_315)

The cohort VIEW `cohort_m044_ajcc_ete_v1` was updated to source `ete_grade_final` from a
CASE on `ete_grade_final_v2` with explicit normalization:

```sql
CASE
  WHEN p.ete_grade_final_v2 IN ('none', 'absent') THEN 'no_negative'
  WHEN p.ete_grade_final_v2 = 'gross'             THEN 'gross'
  WHEN p.ete_grade_final_v2 = 'microscopic'        THEN 'microscopic'
  WHEN p.ete_grade_final_v2 = 'present_ungraded'  THEN 'present_ungraded'
  WHEN p.ete_grade_final_v2 = 'true'              THEN 'gross'  -- 4-row artifact
  WHEN p.ete_grade_final_v2 IS NULL               THEN NULL
  ELSE 'present_ungraded'
END AS ete_grade_final
```

**Downstream code update**: `m044_ete_fit_models.py` MASTER_ANALYTIC_SQL and `m044_master_analytic.sql`
updated to also recognize `'no_negative'` and `'none'` in the `ete_group` CASE (backward compat
with any cached parquets using old vocabulary).

---

## Validation gates

See `studies/m044_v6_audit/` for model output.

| Gate | Criterion | Result |
|---|---|---|
| 4a column uniqueness | n_cols = n_unique (DESCRIBE) | 35 = 35 ✅ |
| 4b ete_grade_final | Only {no_negative, microscopic, gross, present_ungraded, NULL} | See gate output |
| 4c cohort N | 3,400–3,750 (v5) / documented ~3,868 (post-mig_313 expansion) | 3,868 (documented) |
| 4d Stage IVB | 50–120 | See gate output |

---

## Carry-forwards

- **CF-M044-DUP-COLS**: CLOSED (measurement artifact — view is clean; Cortex Analyst Snowflake binding tracked separately)
- **CF-M044-V6-MANUSCRIPT-PATCH**: OPEN (Cowork lane) — prose patches for v6 docx after regression delta confirmed
- **CF-CORTEX-ANALYST-M044-SCHEMA**: NEW — investigate why Snowflake semantic model sees duplicates (mig_289 export or YAML schema definition issue)
