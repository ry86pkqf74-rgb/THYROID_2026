# Cursor Composer Dispatch — mig_289: Sync key MD cohort views to Snowflake (consumer convenience)

**Generated:** 2026-05-04 by Cowork.
**Lane:** mig_289 — Cowork SF scripts have to MD-roundtrip via duckdb to query `manuscript_workspace.cohort_*` views (e.g., M037 logreg). Sync these views as SF tables once per round so SF-native scripts can query directly. Read-only mirror; refreshed on each MD→SF export cycle.
**Recommended agent:** **Cursor Composer** — script + cron-eligible.
**Estimated runtime:** 30 min.
**Severity:** LOW (convenience). Doesn't unblock manuscripts but reduces analytic friction.

---

## §0 — First message to paste into Cursor Composer

> mig_289 dispatch. Add 5 manuscript_workspace cohort views to the existing snowflake_trial/scripts/01_export_md_to_parquet.py + 02_load_to_snowflake.py + 04_build_flat_views.py pipeline. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Cohort views to add

| MD view | Target SF table |
|---|---|
| `manuscript_workspace.cohort_m044_ajcc_ete_v1` | `THYROID_VALIDATION.PUBLIC.COHORT_M044_AJCC_ETE_V1` |
| `manuscript_workspace.cohort_m037_ln_metastasis_v1` | `COHORT_M037_LN_METASTASIS_V1` |
| `manuscript_workspace.cohort_m025_tirads_performance_v1` | `COHORT_M025_TIRADS_PERFORMANCE_V1` |
| `manuscript_workspace.cohort_m032_descriptive_25yr_v1` | `COHORT_M032_DESCRIPTIVE_25YR_V1` |
| `main.cohort_m038_massive_goiter_v1` | `COHORT_M038_MASSIVE_GOITER_V1` |

---

## §2 — Apply

### §2a — Edit `01_export_md_to_parquet.py`

Add to the export manifest (after the existing canonical_* exports):

```python
COHORT_VIEWS = [
    ("manuscript_workspace.cohort_m044_ajcc_ete_v1", "cohort_m044_ajcc_ete_v1"),
    ("manuscript_workspace.cohort_m037_ln_metastasis_v1", "cohort_m037_ln_metastasis_v1"),
    ("manuscript_workspace.cohort_m025_tirads_performance_v1", "cohort_m025_tirads_performance_v1"),
    ("manuscript_workspace.cohort_m032_descriptive_25yr_v1", "cohort_m032_descriptive_25yr_v1"),
    ("main.cohort_m038_massive_goiter_v1", "cohort_m038_massive_goiter_v1"),
]
for src, name in COHORT_VIEWS:
    out = OUT / f"{name}.parquet"
    md.execute(f"COPY (SELECT * FROM {src}) TO '{out}' (FORMAT 'parquet')")
```

### §2b — Edit `02_load_to_snowflake.py`

The existing PUT/COPY loop iterates over parquet files; should pick up the new ones automatically. Verify col-count handling for views.

### §2c — Edit `04_build_flat_views.py`

Add the 5 cohort views to the flat-view builder. They're already flat (not VARIANT), so the build script may need a simpler "passthrough" branch:

```python
COHORT_VIEW_TABLES = ['COHORT_M044_AJCC_ETE_V1', 'COHORT_M037_LN_METASTASIS_V1',
                      'COHORT_M025_TIRADS_PERFORMANCE_V1', 'COHORT_M032_DESCRIPTIVE_25YR_V1',
                      'COHORT_M038_MASSIVE_GOITER_V1']
for t in COHORT_VIEW_TABLES:
    cur.execute(f"CREATE OR REPLACE VIEW {t}_FLAT AS SELECT * FROM {t}")
```

### §2d — Test full refresh cycle

```bash
python snowflake_trial/scripts/01_export_md_to_parquet.py
python snowflake_trial/scripts/02_load_to_snowflake.py
python snowflake_trial/scripts/04_build_flat_views.py
# Verify SF: SELECT COUNT(*) FROM COHORT_M044_AJCC_ETE_V1_FLAT;
# Expected: 4012 (or whatever current MD value is)
```

### §2e — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_289', CURRENT_TIMESTAMP, 'cursor_composer_mig289',
 'mig_289: Added 5 manuscript_workspace cohort views (m044/m037/m025/m032/m038) to SF refresh pipeline. SF-native scripts no longer need MD roundtrip. Cohort views refresh per cycle. Verified row counts match MD post-load.');
```

---

## §3 — Surgical git add

```
snowflake_trial/scripts/01_export_md_to_parquet.py
snowflake_trial/scripts/02_load_to_snowflake.py
snowflake_trial/scripts/04_build_flat_views.py
qc_framework_v1/migrations/289_cohort_views_to_sf_20260504.sql  (just the signoff INSERT)
scripts/output/mig_289_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_289_COHORT_VIEWS_TO_SF_EXPORT_20260504.md
```

---

**End of mig_289 dispatch.**
