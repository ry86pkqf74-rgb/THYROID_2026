# Cursor Composer Dispatch — mig_270: Re-point Snowflake scripts to canonical_histology_lookup_v1 (post mig_267)

**Generated:** 2026-05-02 by Cowork.
**Lane:** mig_270 — After mig_267 lands `canonical_histology_lookup_v1` SSOT in MD, ~10 Snowflake scripts that have inline `CASE WHEN histology_final ILIKE 'PTC%'` need to JOIN the SSOT instead. Mechanical multi-file edit.
**Recommended agent:** **Cursor Composer** — sed-style replacement.
**Estimated runtime:** 30 min
**Triggered by:** mig_267 landing.
**Severity:** MED (manuscript consistency).
**Closes:** CF-mig267-DOWNSTREAM-REPOINT.

---

## §0 — First message to paste into Cursor Composer

> mig_270 dispatch. After mig_267 lands `main.canonical_histology_lookup_v1`, re-point ~10 Snowflake scripts to JOIN the SSOT instead of inline CASE. List in §1. Edits are mechanical.

---

## §1 — Files to edit

```
snowflake_trial/scripts/08_cohort_views.py            # COHORT_M037 + COHORT_M032 + COHORT_M004
snowflake_trial/scripts/09_m037_table1.py
snowflake_trial/scripts/19_m044_table1.py
snowflake_trial/scripts/21_m004_table1.py
snowflake_trial/scripts/22_m037_table2_logreg.py
snowflake_trial/scripts/24_m044_cox_ph.py
snowflake_trial/scripts/25_m037_sensitivity_ln_both.py
snowflake_trial/scripts/29_m044_cox_sensitivity_ln_clean.py
snowflake_trial/scripts/30_m044_km_forest_data.py
snowflake_trial/scripts/31_m038_massive_goiter_table1.py
```

## §2 — Add canonical_histology_lookup_v1 to export

Append to `snowflake_trial/scripts/01_export_md_to_parquet.py` TABLES list:
```python
"canonical_histology_lookup_v1",
```

## §3 — Replacement pattern

Wherever a script has:
```python
CASE WHEN HISTOLOGY_FINAL ILIKE 'PTC%' THEN 'PTC'
     WHEN HISTOLOGY_FINAL ILIKE '%follicular%' THEN 'FTC'
     WHEN HISTOLOGY_FINAL ILIKE 'MTC%' THEN 'MTC'
     ...
END AS HISTOLOGY_GROUP
```

Replace with a JOIN to the lookup:
```python
SELECT ..., lookup.HISTOLOGY_GROUP, lookup.IS_METASTATIC, lookup.IS_RECURRENT
FROM CANONICAL_PATIENT_MASTER_FLAT cpm
LEFT JOIN CANONICAL_HISTOLOGY_LOOKUP_V1_FLAT lookup
  ON cpm.HISTOLOGY_FINAL = lookup.HISTOLOGY_FINAL_RAW
```

## §4 — Verify

Re-run each script after edit; output Tables should show same row counts but with consistent histology grouping (no more `MTC` vs `MTC OR medullary` drift between scripts).

```bash
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/01_export_md_to_parquet.py  # adds histology lookup
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/02_load_to_snowflake.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/04_build_flat_views.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/09_m037_table1.py            # smoke test
```

## §5 — Surgical git add
```
snowflake_trial/scripts/01_export_md_to_parquet.py
snowflake_trial/scripts/{08,09,19,21,22,24,25,29,30,31}_*.py
```
