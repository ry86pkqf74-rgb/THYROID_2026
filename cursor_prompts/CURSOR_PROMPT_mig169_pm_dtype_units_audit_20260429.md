# Cursor Prompt — mig_169 PM Data-Type & Units Sanity Audit

**Lane:** 57 / mig_169
**Batch_id:** `mig_169_pm_dtype_units_audit_20260429`
**Generated:** 2026-04-29 (late evening)
**Type:** Read-only audit. Output is a Markdown report + commented probe SQL stub. No data writes.

---

## §0 Why this lane exists

Prior rounds have repeatedly shipped with data-type bugs that the gate-5 audit doesn't catch:
- mig_144 shipped with 4 VARCHAR measurement columns (numeric stored as text)
- mig_146/147 shipped with VARCHAR date cols
- mig_154 shipped with synoptic distance triple as VARCHAR (CF-mig154-MARGIN-MM-VARCHAR-RETYPE later cleared)
- mig_157 shipped with 2 TIMESTAMP date cols
- mig_159 verified DOUBLE / BIGINT correctness on 27 cols this round

This lane runs a **comprehensive data-type sanity audit** across every verified analytic column on canonical_patient_master, looking for:

1. **VARCHAR-with-units** — cols that look like measurements (`*_size`, `*_dose`, `*_volume`, `*_mm`, `*_cm`, `*_ml`, `*_mci`, `*_kg`, `*_pg_ml`, etc.) stored as VARCHAR with embedded units (`'2.5 cm'`, `'120 mg'`, `'15 mCi'`)
2. **TIMESTAMP-where-DATE-expected** — clinical event date cols (`*_date`, `*_dt`) stored as TIMESTAMP (per `feedback_clinical_dates_calendar_only.md`); already partially covered by gate-5 / mig_160 but cross-check with allowlist
3. **DOUBLE-where-INTEGER-expected** — count-like cols (`n_*`, `*_count`, `*_n_*`) stored as DOUBLE for no good reason
4. **VARCHAR-where-numeric** — numeric measurement cols stored as VARCHAR without unit suffix (just `'2.5'` / `'15'`)
5. **VARCHAR-where-BOOLEAN** — yes/no flags stored as VARCHAR (`'true'`/`'false'`/`'yes'`/`'no'`/`'1'`/`'0'`) instead of BOOLEAN
6. **Date-cols-as-VARCHAR** — `*_date` cols stored as VARCHAR (gate 5 catches these on verified tables but only with word boundary; might miss some patterns)

## §1 Governance posture

- Read-only against MotherDuck. No `query_rw`.
- Output: `qc_framework_v1/reports/mig_169_pm_dtype_units_audit_20260429.md` + `qc_framework_v1/migrations/169_pm_dtype_units_audit_probes_20260429.sql` (commented probe SQL).
- Findings open `CF-mig169-DTYPE-<col>` for follow-up retype migrations (mig_169b cluster).

## §2 Required scope

Filter to all verified analytic cols on PM (exclude provenance/source/keyword/raw):

```sql
SELECT c.column_name, c.data_type
FROM information_schema.columns c
JOIN main.canonical_column_verification_registry_v1 r
  ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND c.table_name='canonical_patient_master'
  AND r.verification_status='verified'
  AND c.column_name NOT LIKE '%_source'
  AND c.column_name NOT LIKE '%_keyword%'
  AND c.column_name NOT LIKE '%_raw'
  AND c.column_name NOT LIKE '%_note_ref'
  AND c.column_name NOT LIKE '%_evidence%'
  AND c.column_name NOT LIKE 'nlp_%_key_finding'
  AND c.column_name NOT LIKE '%_text%'
ORDER BY c.column_name
```

## §3 Per-bucket detection probes

### §3.1 VARCHAR-with-units detection

For each VARCHAR col whose name matches measurement patterns (`*_size`, `*_dose_mci`, `*_volume_ml`, `*_weight_kg`, `*_mm`, `*_cm`, `*_pg_ml`, etc.):

```sql
SELECT '<col>' AS col,
  COUNT(*) AS n_nonnull,
  SUM(CASE WHEN regexp_matches(<col>, '[a-zA-Z]') THEN 1 ELSE 0 END) AS n_with_alpha,
  STRING_AGG(DISTINCT <col>, ' | ' ORDER BY <col>) FILTER (WHERE regexp_matches(<col>, '[a-zA-Z]')) AS sample_alpha_values
FROM main.canonical_patient_master
WHERE <col> IS NOT NULL;
```

If `n_with_alpha > 0` AND the col name suggests a measurement, flag as VARCHAR-WITH-UNITS.

### §3.2 TIMESTAMP-where-DATE detection

```sql
SELECT c.column_name, c.data_type,
  (SELECT MIN(EXTRACT(HOUR FROM c.column_name) <> 0 OR EXTRACT(MINUTE FROM c.column_name) <> 0 OR EXTRACT(SECOND FROM c.column_name) <> 0) FROM main.canonical_patient_master) AS has_subday
FROM information_schema.columns c
WHERE c.table_name='canonical_patient_master'
  AND c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
  AND c.column_name LIKE '%_date'
```

(The above is sketchy SQL — agent should adapt to do per-col EXTRACT scans.) Flag any clinical event date col stored as TIMESTAMP.

### §3.3 DOUBLE-where-INTEGER detection

For each DOUBLE col whose name matches count patterns (`n_*`, `*_count`, `*_n_*`, `total_*`, `num_*`):

```sql
SELECT '<col>' AS col,
  COUNT(*) AS n_nonnull,
  SUM(CASE WHEN <col> = ROUND(<col>) THEN 1 ELSE 0 END) AS n_integer_valued,
  COUNT(DISTINCT <col>) AS n_distinct
FROM main.canonical_patient_master
WHERE <col> IS NOT NULL;
```

If `n_integer_valued = n_nonnull` AND the col name suggests a count, flag as DOUBLE-WHERE-INTEGER.

### §3.4 VARCHAR-where-numeric

Same as §3.1 but flag cols where `n_with_alpha = 0` AND values are all parseable as numeric. These can be cleanly cast to DOUBLE.

```sql
SELECT '<col>' AS col,
  COUNT(*) FILTER (WHERE <col> IS NOT NULL) AS n_nonnull,
  COUNT(*) FILTER (WHERE TRY_CAST(<col> AS DOUBLE) IS NOT NULL) AS n_numeric_parseable,
  COUNT(*) FILTER (WHERE <col> IS NOT NULL AND TRY_CAST(<col> AS DOUBLE) IS NULL) AS n_unparseable
FROM main.canonical_patient_master;
```

### §3.5 VARCHAR-where-BOOLEAN

For each VARCHAR col whose distinct values are a subset of `{'true','false','yes','no','y','n','t','f','1','0'}` (case-insensitive):

```sql
SELECT '<col>' AS col, COUNT(DISTINCT LOWER(TRIM(<col>))) AS n_distinct,
  STRING_AGG(DISTINCT LOWER(TRIM(<col>)), '|' ORDER BY LOWER(TRIM(<col>))) AS distinct_values
FROM main.canonical_patient_master
WHERE <col> IS NOT NULL
HAVING n_distinct <= 4;
```

Then check if all distinct_values are in the boolean-like set.

### §3.6 Date-cols-stored-as-VARCHAR

```sql
SELECT c.column_name FROM information_schema.columns c
WHERE c.table_name='canonical_patient_master'
  AND c.data_type='VARCHAR'
  AND (regexp_matches(c.column_name, '(^|_)dates?(_|$)') OR regexp_matches(c.column_name, '(^|_)dt(_|$)'));
```

For each: probe TRY_STRPTIME parseability per mig_160 ladder.

## §4 Findings table (in report)

| col | data_type | bucket | evidence | sample | proposed_action |
|---|---|---|---|---|---|
| <col> | VARCHAR | VARCHAR-with-units | n_with_alpha=384/1296 | `'2.5 cm'`, `'15 mm'` | retype DOUBLE + extract numeric in mig_169b |
| ... | | | | | |

Sort by bucket, then by impact (n_nonnull descending).

## §5 Required CF tags

For each finding, suggest a CF tag:
- `CF-mig169-DTYPE-VARCHAR-WITH-UNITS-<col>` (highest priority — analytic numeric data corrupted)
- `CF-mig169-DTYPE-TIMESTAMP-WHERE-DATE-<col>` (already covered by gate-5 if `*_date` named; CF only if missed)
- `CF-mig169-DTYPE-DOUBLE-WHERE-INTEGER-<col>` (low priority — cosmetic)
- `CF-mig169-DTYPE-VARCHAR-WHERE-NUMERIC-<col>` (medium priority — analytic loss)
- `CF-mig169-DTYPE-VARCHAR-WHERE-BOOLEAN-<col>` (medium priority — analytic loss)
- `CF-mig169-DTYPE-VARCHAR-DATE-<col>` (high priority — gate-5 should catch but this lane double-checks)

## §6 SQL stub structure

`169_pm_dtype_units_audit_probes_20260429.sql` — fully commented probes (one block per bucket), agent does NOT write CF appendices in this lane (mig_169b will).

## §7 Git workflow

- Files: 169 SQL stub + 169 report
- Commit: `qc: mig_169 PM data-type & units sanity audit (read-only)`
- Push.

## §8 Out of scope

- DO NOT execute any retypes in this lane. mig_169b after Logan ratifies findings.
- DO NOT touch BOOLEAN cols (mig_167 covers cohort-uniformity; this lane is dtype-only).
- DO NOT touch other tables — PM only.
- DO NOT propose new SSOT enums (that's mig_168 territory).
