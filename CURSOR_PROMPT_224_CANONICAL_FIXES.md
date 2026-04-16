# Cursor Agent Prompt — Canonical Dataset ETL Fixes (Script 224)

**Repo:** `https://github.com/ry86pkqf74-rgb/THYROID_2026` (branch: `main`)
**Authoritative DB:** `thyroid_canonical_publication_v1_0` — the CLEAN canonical master
**Target table:** `thyroid_canonical_publication_v1_0.main.canonical_patient_master` (N=10,871; 1,423 columns)
**Legacy/messy source DB:** `Thyroid 2026 UPdated` — READ-ONLY reference for pulling raw source data when needed
**Date:** 2026-04-16

---

## DATABASE CONTRACT — READ BEFORE ANY QUERY

`thyroid_canonical_publication_v1_0` is the **clean canonical master** that we are continuing to build, clean, extend, and finalize. This is where all reads and writes go.

`Thyroid 2026 UPdated` is the **messy legacy/working DB**. It is the historical source that the publication DB was built from. You may **read** from it when a raw source table is needed (e.g., `path_synoptics`, `operative_episode_detail_v2`) — but you **must never write** to it, and every ETL output lands in `thyroid_canonical_publication_v1_0`.

**Pre-flight assertions — run as your very first action:**

```sql
SELECT
  (SELECT COUNT(*) FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master) AS pub_n_patients,
  (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_catalog='thyroid_canonical_publication_v1_0'
       AND table_schema='main' AND table_name='canonical_patient_master') AS pub_n_cols,
  (SELECT COUNT(*) FROM duckdb_tables()
     WHERE database_name='thyroid_canonical_publication_v1_0' AND schema_name='main') AS pub_n_tables;
-- Expected: 10871, 1423, 110.  Anything else -> STOP and report to user.
```

**Canonical invariants — verify after every change:**

```sql
SELECT
  COUNT(*)                                          AS n,
  COUNT(*) - 10871                                  AS row_delta,
  COUNT(DISTINCT research_id)                       AS distinct_rids,
  COUNT(*) FILTER (WHERE research_id IS NULL)       AS null_rids,
  COUNT(*) FILTER (WHERE fna_path_outcome IS NULL)  AS null_fna,
  COUNT(*) FILTER (WHERE is_malignant IS NULL)      AS null_malignant,
  COUNT(*) FILTER (WHERE diagnosis_primary IS NULL) AS null_dx
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;
-- Expected: 10871, 0, 10871, 0, 0, 0, 0
```

**Hard rules:**
1. No writes to `Thyroid 2026 UPdated`. Reads only.
2. No `DROP`/`CREATE OR REPLACE` on existing publication tables without showing the user the delta first.
3. Every new column is additive. Never rename or overwrite existing column values in a single pass — build new columns (`_v2` suffix if needed), validate, then deprecate the old ones in a follow-up commit.
4. All destructive/overwriting ops wait for user confirmation. Read-only ops, new-column additions, and validation tables do not.

---

## ISSUES ADDRESSED

### ISSUE 1 — distant_mets_proxy IS LITERALLY recurrence_flag (CRITICAL)
- `distant_mets_proxy` in `thyroid_scoring_py_v1` was assigned the value of `recurrence_flag` (1:1 across all 10,871 patients).
- Fixed: now derived from `path_m_stage_raw` + `pet_distant_mets_ever`. M1 count drops from 1,818 to ~30-80.

### ISSUE 2 — T4a/T4b DROPPED BY AJCC 8 ALGORITHM (HIGH)
- `compute_t_stage()` had no T4a/T4b branches, downstaging 69 T4a + 10 T4b patients.
- Fixed: pathologist T4 designation is now authoritative and checked first.

### ISSUE 3 — ADD AJCC 7TH EDITION STAGING (MEDIUM)
- New columns: `ajcc7_t_stage`, `ajcc7_n_stage`, `ajcc7_m_stage`, `ajcc7_stage_group`, `stage_migration_7_to_8`.

### ISSUE 4 — n_surgeries MASSIVELY UNDERCOUNTS (MEDIUM-HIGH)
- Only 2 patients had n_surgeries > 1 vs 761 multi-surgery patients in path_synoptics.
- Fixed: rebuilt from `path_synoptics.surg_date` with 7-day dedup.

### ISSUE 5 — N-STAGE NULL RESOLUTION (MEDIUM)
- 273 malignant patients had NULL `ajcc8_n_stage`. Fixed via `path_n_stage_raw` cascade.

### ISSUE 6 — BMI (DOCUMENT AS GENUINELY MISSING)
- Added `bmi_missingness_reason` column. No data fabrication.

### ISSUE 7 (+ ADDENDUM) — RECURRENCE RECLASSIFICATION
- Recurrence = pathology-proven ONLY. Imaging suspicion and biochemical concern are separate entities.
- New column families: `recurrence_*`, `imaging_suspicious_recurrence_*`, `biochemical_concern_*`.

---

## EXECUTION ORDER

1. Pre-flight check
2. Issue 1 — distant_mets_proxy / M-stage fix
3. Issue 2 — T4 preservation
4. Issue 5 — N-stage null resolution
5. Rebuild thyroid_scoring_py_v1
6. Issue 3 — AJCC 7th edition staging
7. Issue 4 — n_surgeries rebuild
8. Issue 7 — Recurrence reclassification
9. Issue 6 — BMI documentation
10. Rebuild canonical_patient_master
11. Run all validation queries
12. Dedup and archive cleanup
13. Git commit + push
