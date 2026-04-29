# Cursor Agent Task — GLOBAL CALENDAR-DATE RETYPE (gate-5 closure)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (21 cols across 6 verified canonicals)
**Run order:** Lane 48 of next 4-prompt batch (mig_160)

---

## 0. Cleanliness & safety preamble (MUST READ)

This is a **structural data write** lane (in-place ALTER COLUMN with TRY_CAST), not a registry-only lane. Higher risk than verification-only migrations:
1. **AGENTS governance** — agent commits SQL only; Logan/Cowork applies. NO MD writes from agent.
2. Pre-snapshot ALL affected tables before any ALTER (full table snapshot, not just registry).
3. Verify TRY_CAST round-trips cleanly on every value before committing the retype.
4. **2-digit year convention** (`reference_2digit_year_convention.md`, Logan-ratified 2026-04-27): all YY → 20YY (00=2000, 25=2025).
5. **Clinical event dates only** — provenance/audit timestamps (`build_ts`, `extracted_at`, `verified_ts`, etc.) are exempt per allowlist.
6. Surgical git add.

Lane-specific risk: ALTER COLUMN failures can corrupt verified canonicals. Use TRY_CAST + COALESCE patterns; pre-validate on a probe table; use BEGIN/COMMIT transactions; rollback path documented.

---

## 1. Goal

Close the 5-gate audit's gate 5 (currently 21) to **0** by retyping clinical event dates currently stored as TIMESTAMP or VARCHAR to DATE on 6 verified canonicals.

### 1a. Pre-flight probe (must return exactly 21)

```sql
WITH verified_tables AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
  WHERE table_status='verified' AND table_name LIKE 'canonical_%'
),
audit_allowlist AS (
  SELECT col_name FROM (VALUES
    ('build_ts'),('built_at'),('extracted_at'),('llm_build_ts'),('llm_extracted_at'),
    ('verified_ts'),('signed_off_ts'),('registered_ts'),('updated_at'),('created_at'),
    ('promoted_at'),('completed_at'),('started_at'),('ended_at'),('ingested_at_utc'),
    ('ingestion_date'),('lab_datetime')
  ) v(col_name)
)
SELECT c.table_name, c.column_name, c.data_type
FROM information_schema.columns c
JOIN verified_tables v ON c.table_name = v.table_name
LEFT JOIN main.canonical_column_verification_registry_v1 r
  ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
  AND c.column_name NOT LIKE '%_status' AND c.column_name NOT LIKE '%_source'
  AND c.column_name NOT LIKE '%_keyword' AND c.column_name NOT LIKE '%_raw'
  AND COALESCE(r.verification_status,'unknown') != 'na'
  AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
       OR (c.data_type='VARCHAR' AND (regexp_matches(c.column_name, '(^|_)dates?(_|$)')
                                       OR regexp_matches(c.column_name, '(^|_)dt(_|$)'))))
ORDER BY c.table_name, c.column_name;
```

Confirm count is **exactly 21**.

### 1b. Scope (live MD 2026-04-29 enumeration)

| Table | Column | Current type | Target |
|---|---|---|---|
| canonical_ete_event_resolved_v1 | last_known_alive_date | TIMESTAMP | DATE |
| canonical_frozen_section_patient_rollup_v1 | frozen_1_date through frozen_12_date | VARCHAR | DATE |
| canonical_frozen_section_patient_rollup_v1 | frozen_section_first_date, frozen_section_last_date | VARCHAR | DATE |
| canonical_molecular_genetics_v2 | resolved_test_date | VARCHAR | DATE |
| canonical_molecular_genetics_v2 | test_date_native | TIMESTAMP | DATE |
| canonical_path_malignant_patient_rollup_v1 | earliest_malignant_path_date, latest_malignant_path_date | TIMESTAMP | DATE |
| canonical_recurrence_v1 | first_surgery_date, recurrence_date | TIMESTAMP | DATE |

7 TIMESTAMP cols + 14 VARCHAR cols = 21 total.

---

## 2. Methodology

### 2a. Format inventory pass (DO FIRST)

For each VARCHAR col, sample value distribution:

```sql
-- Template
SELECT '<table.col>' AS src, value, COUNT(*) AS n
FROM main.<table>
WHERE <col> IS NOT NULL
GROUP BY value ORDER BY n DESC LIMIT 10;
```

Watch for:
- 'MM/DD/YYYY' (4-digit year)
- 'MM/DD/YY' (2-digit year — apply 20YY convention)
- 'YYYY-MM-DD' (ISO)
- 'nan' literal (Script 248 PRESERVE_RAW pattern)
- mixed formats within a single column

For each TIMESTAMP col, sample to confirm time portion is `00:00:00` (calendar-only) before retyping. If non-zero time portions exist, that's a CF requiring investigation (not just a retype).

### 2b. Retype SQL pattern (per col)

Standard pattern for VARCHAR ('MM/DD/YYYY'):
```sql
ALTER TABLE main.<table>
ALTER COLUMN <col> SET DATA TYPE DATE
USING CASE
  WHEN <col> IS NULL OR LOWER(TRIM(<col>)) IN ('','nan','none','null') THEN NULL
  ELSE TRY_STRPTIME(<col>, '%m/%d/%Y')::DATE
END;
```

Standard pattern for VARCHAR ('MM/DD/YY' with 20YY rule):
```sql
ALTER TABLE main.<table>
ALTER COLUMN <col> SET DATA TYPE DATE
USING CASE
  WHEN <col> IS NULL OR LOWER(TRIM(<col>)) IN ('','nan','none','null') THEN NULL
  ELSE TRY_STRPTIME(<col>, '%m/%d/%y')::DATE  -- DuckDB %y maps YY → 20YY automatically? VERIFY first.
END;
```

**VERIFY** the 2-digit-year mapping behavior. If DuckDB's `%y` doesn't auto-apply 20YY, build the year explicitly:
```sql
USING CASE
  WHEN ... THEN ...
  ELSE MAKE_DATE(
    2000 + CAST(SPLIT_PART(<col>, '/', 3) AS INTEGER),
    CAST(SPLIT_PART(<col>, '/', 1) AS INTEGER),
    CAST(SPLIT_PART(<col>, '/', 2) AS INTEGER)
  )
END;
```

Standard pattern for TIMESTAMP → DATE:
```sql
ALTER TABLE main.<table>
ALTER COLUMN <col> SET DATA TYPE DATE
USING <col>::DATE;
```

### 2c. Round-trip validation (REQUIRED before applying)

For every col, run:
```sql
-- Pre-retype: count nulls + count non-null + sample 5 distinct
SELECT COUNT(*) AS total, COUNT(<col>) AS non_null FROM main.<table>;

-- TRY_CAST validation (for VARCHAR sources)
SELECT COUNT(*) AS would_become_null
FROM main.<table>
WHERE <col> IS NOT NULL
  AND TRY_STRPTIME(<col>, '<format>') IS NULL;
-- Expect 0. If >0, those rows would silently lose data — investigate before retype.
```

If `would_become_null > 0` for any col, DO NOT retype that col. Instead open `CF-mig160-RETYPE-BLOCKED-<col>` documenting the unparseable values, and leave the col VARCHAR.

### 2d. Pre-snapshot (REQUIRED — table-level, not just registry)

```sql
-- Per affected table
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.<table>_pre_mig160_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig160_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.<table>;
```

6 tables → 6 snapshot tables.

### 2e. Registry update (after successful retype)

For each retyped col, append a CF appendix to its registry row:
```sql
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_160: Calendar-DATE retype applied. Was <was_type>, now DATE. ' ||
            'Format <format> validated round-trip with 0 unparseable values. ' ||
            'Pre-snapshot at archive_pub_v1_0.<table>_pre_mig160_20260429.'
WHERE table_name='<table>' AND column_name='<col>';
```

`data_type` column on the registry should also be refreshed if Logan's pattern (per `mig_126`) — verify whether the registry separately stores data_type.

---

## 3. Sign-off SQL

File: `qc_framework_v1/migrations/160_global_clinical_date_retype_20260429.sql`

Structure:
- Section A — Pre-snapshots (6 CREATE TABLE statements)
- Section B — Format-inventory probes (commented; for runbook reference)
- Section C — Per-col ALTER COLUMN ... SET DATA TYPE DATE (21 statements)
- Section D — Registry note appendices (21 UPDATE statements)
- Section E — Verify post-state: re-run gate-5 audit; expect 0

Wrap C+D in BEGIN/COMMIT.

---

## 4. Required CFs

- `CF-mig160-RETYPE-BLOCKED-<col>` — for any col where TRY_STRPTIME yields would_become_null > 0
- `CF-mig160-2DIGIT-YEAR-NORMALIZATION-APPLIED` — list cols where 20YY rule was used
- `CF-mig160-TIMESTAMP-TIME-PORTION-NONZERO-<col>` — for any TIMESTAMP col where time portion isn't 00:00:00
- `CF-mig160-GATE-5-CLOSURE` — informational on success: "21 → 0"

---

## 5. Apply + verify (Logan-only after Cowork independent verification)

NO MD writes from agent. Logan applies. Cowork independently re-runs gate-5 audit post-apply; expect 0.

Rollback path: each affected table has a pre-snapshot; in case of corruption, restore via:
```sql
CREATE OR REPLACE TABLE main.<table> AS
SELECT * FROM "Thyroid 2026 UPdated".archive_pub_v1_0.<table>_pre_mig160_20260429
WHERE pre_mig160_snapshot_ts IS NOT NULL;  -- exclude snapshot col on restore via SELECT *EXCLUDE if needed
```

---

## 6. Git workflow

```bash
git add qc_framework_v1/migrations/160_global_clinical_date_retype_20260429.sql
git -c user.name="Logan Glosser" -c user.email="logan.glosser@gmail.com" commit -m "qc: mig_160 global clinical-date retype (gate-5 closure, 21 cols × 6 tables)"
git push origin main
```

---

## 7. Done definition

- [ ] Pre-flight gate-5 probe returns exactly 21
- [ ] Format-inventory pass complete; no surprising values
- [ ] Round-trip TRY_STRPTIME validation passes 0 unparseable for every VARCHAR col (or CF-RETYPE-BLOCKED opened)
- [ ] TIMESTAMP cols verified to have 00:00:00 time portions (or CF opened)
- [ ] Pre-snapshot tables created in archive_pub_v1_0 for all 6 affected tables
- [ ] Per-col ALTER + registry note SQL drafted
- [ ] Post-apply gate-5 audit projected to return 0
- [ ] Rollback path documented in migration header
- [ ] SQL file committed + pushed; NO MD writes from agent
