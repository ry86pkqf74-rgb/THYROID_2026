# Cursor Agent Task — Clinical Date Type Cleanup (Protocol v2)

**Generated:** 2026-04-28 (Cowork session, post-mig_101)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `9a470e2` — `mig_101: canonical_path_gland_patient_rollup_v1 verified — path_gland family complete (19th table)`
**Estimated effort:** 1-2 hours, mechanical, autonomous (no Logan touchpoints expected mid-task)

---

## 1. Goal

Apply the just-ratified **clinical-dates-calendar-only** rule (Logan, 2026-04-28) across all currently-verified Tier 1 events tables. Retype 6 columns from VARCHAR/TIMESTAMP to DATE. The rule:

> Clinical event date cols (surgery_date, fna_date, path_date, etc.) MUST be DATE type — never VARCHAR storing date-like strings, never TIMESTAMP. Timestamps add no clinical info; scrub them. Audit/provenance timestamps (build_ts, extracted_at, llm_build_ts, verified_ts, etc.) are exempt.

Stored at `feedback_clinical_dates_calendar_only.md` in Cowork auto-memory.

---

## 2. Confirmed scope (audit run 2026-04-28)

Six column repairs across four canonical tables:

| Table | Column | Current type | Target type |
|---|---|---|---|
| `canonical_esophageal_invasion_events_v1` | `note_date` | VARCHAR | DATE |
| `canonical_frozen_section_events_v1` | `frozen_section_date` | VARCHAR | DATE |
| `canonical_operative_events_v1` | `surgery_date_native` | TIMESTAMP | DATE |
| `canonical_operative_events_v1` | `resolved_surgery_date` | VARCHAR | DATE |
| `canonical_operative_events_v1` | `note_date_resolved` | TIMESTAMP | DATE |
| `canonical_path_malignant_events_v1` | `surgery_date` | TIMESTAMP | DATE |

**Audit allowlist** (stays as-is — these are NOT clinical-event date cols):
- `_status`, `_source`, `_keyword` suffixes (metadata about the date, not the date itself)
- `_raw` suffix (preserved upstream string for provenance)
- `ingested_at_utc`, `extracted_at`, `build_ts`, `llm_build_ts`, `verified_ts`, `signed_off_ts`, `registered_ts`, `updated_at`, `created_at`, `promoted_at`, `completed_at`, `started_at`, `ended_at` (audit/provenance timestamps)

**Re-run the audit yourself before starting** — new tables may have been verified between this prompt's authoring and your start time. Audit query:

```sql
WITH verified_tables AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
  WHERE table_status='verified' AND table_name LIKE 'canonical_%'
),
audit_allowlist AS (
  SELECT col_name FROM (VALUES
    ('build_ts'),('extracted_at'),('llm_build_ts'),('verified_ts'),
    ('signed_off_ts'),('registered_ts'),('llm_extracted_at'),('updated_at'),
    ('created_at'),('promoted_at'),('completed_at'),('started_at'),('ended_at'),
    ('ingested_at_utc')
  ) v(col_name)
)
SELECT c.table_name, c.column_name, c.data_type
FROM information_schema.columns c
JOIN verified_tables v ON c.table_name = v.table_name
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
  AND c.column_name NOT LIKE '%_status'
  AND c.column_name NOT LIKE '%_source'
  AND c.column_name NOT LIKE '%_keyword'
  AND c.column_name NOT LIKE '%_raw'
  AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
       OR (c.data_type='VARCHAR'
           AND (c.column_name ILIKE '%date%' OR c.column_name ILIKE '%dt')))
ORDER BY c.table_name, c.ordinal_position;
```

---

## 3. Don't touch (active parallel lanes)

- `canonical_parathyroid_events_v1` — Cowork is verifying this in parallel.
- `canonical_medications_events_v1` — second Cursor lane (separate prompt) will handle this.
- Any table with `path_gland` in its name — recently closed; do not recheck/repair.
- The `canonical_table_signoff_registry_v1` row for those three names — leave alone.

---

## 4. Methodology

For each col in scope:

### 4a. Pre-snapshot
```sql
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.<table_name>_pre_date_retype_20260428 AS
SELECT * FROM main.<table_name>;
```

### 4b. Retype via CREATE OR REPLACE TABLE … SELECT * REPLACE pattern
DuckDB supports column-level REPLACE in SELECT *, which preserves column order without ALTER gymnastics:
```sql
CREATE OR REPLACE TABLE main.<table_name> AS
SELECT * REPLACE (
  TRY_STRPTIME(<col>, '%m/%d/%Y')::DATE AS <col>      -- or other format spec
) FROM main.<table_name>;
```
For TIMESTAMP → DATE: `<col>::DATE AS <col>`.

For VARCHAR with mixed formats, probe distinct formats first and write a COALESCE chain of TRY_STRPTIME variants. Example for frozen_section_date (sole format is 'MM/DD/YYYY'):
```sql
SELECT * REPLACE (
  COALESCE(
    TRY_STRPTIME(frozen_section_date,'%m/%d/%Y')::DATE,
    TRY_STRPTIME(frozen_section_date,'%-m/%-d/%Y')::DATE
  ) AS frozen_section_date
)
```

### 4c. Acceptance gates per col
After each retype, verify:
1. Row count unchanged: `(SELECT COUNT(*) FROM main.<table>)` matches pre-snapshot
2. Column type now DATE: `information_schema.columns` lookup
3. NULL count not increased due to parse failures: pre-NULL = post-NULL (use the snapshot to compare)
4. No dependent VIEW broke: `SELECT * FROM main.<view> LIMIT 1` for every view in `information_schema.views` whose `view_definition` references the table

If parse fails on any non-NULL row, **stop and flag** — do not silently lose data. Investigate the format and add another TRY_STRPTIME variant.

### 4d. Update column verification registry notes
For each repaired col, append a note to the registry row:
```sql
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | clinical_date_retype_20260428: <type-before> -> DATE; '
            || 'pre-snapshot at archive_pub_v1_0.<table>_pre_date_retype_20260428; '
            || 'parse method: <strptime spec>; row count + non-null count preserved.',
    verified_ts = CURRENT_TIMESTAMP
WHERE schema_name='main'
  AND table_name='<table_name>'
  AND column_name='<col_name>';
```
Do NOT change `verification_status` — these cols are already 'verified'. Only annotate.

---

## 5. Deliverable

One repair script + one migration markdown:

- `scripts/<next-id>_clinical_date_retype.py` — Python script with `--dry-run` and `--apply` modes. Dry-run reports what it WOULD do (per-col plan, row counts, NULL counts). Apply executes the snapshot + retype + post-checks for each col. Use `motherduck_client.py` for connection (account `.eras`, see `motherduck.local.toml`).
- `qc_framework_v1/migrations/clinical_date_retype_20260428.md` — short report (pre/post for each col, snapshot table names, dependent-view count, any anomalies).

Acceptance gates for the whole batch:
- Zero TIMESTAMP-typed clinical date cols remain on verified canonical_* tables (re-run the audit query — should return only the allowlisted cols, none in scope).
- All 6 retyped cols have row count + non-null count preserved exactly.
- All dependent VIEWs still resolve (test each with `LIMIT 1`).
- Registry rows updated with retype notes.

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Surgical `git add` by explicit path — never `-A`, never `git add scripts/output/` (memory: `feedback_surgical_git_add.md`).
- Lint Python with `python3 -m py_compile <script.py>` before commit (memory: `feedback_commit_workflow.md`).
- Commit message: short title + per-col summary table + carry-forward list (if any).
- Push to `origin/main` after successful apply + verify.
- Stage only:
  - `scripts/<id>_clinical_date_retype.py`
  - `qc_framework_v1/migrations/clinical_date_retype_20260428.md`
  - Any new dry-run/apply log under `scripts/output/<id>_*.log` if explicitly committed (default: do NOT commit logs).

---

## 7. Carry-forwards to surface

If during repair you discover:
- A 7th violation not in the original audit → add to scope, repair it, document.
- A clinical date col that fails to parse cleanly (>0 non-NULL rows go NULL) → STOP, flag, ask Logan.
- A dependent VIEW that breaks → repair it in the same migration via `CREATE OR REPLACE VIEW` (memory: `feedback_alter_view_dependents.md`).

Open carry-forwards from prior work, status check post-repair:
- **CF-100-DATE-RETYPE** (frozen_section_date VARCHAR → DATE) → CLOSED by this migration.

---

## 8. Reference reading (auto-memory at `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`)

Before starting:
- `feedback_clinical_dates_calendar_only.md` — the rule + scope
- `reference_protocol_v2_md_accounts.md` — `.eras` account hosts the publication DB
- `reference_duckdb_timestamp_tz.md` — DuckDB timestamp TZ pitfalls
- `feedback_motherduck_direct_check.md` — re-query MD before recommending changes
- `feedback_alter_view_dependents.md` — dependent VIEW handling pattern

Also useful:
- `qc_framework_v1/migrations/100_frozen_section_table_signoff.sql` — recent close-out of frozen_section, references the CF
- `cursor_prompts/CURSOR_PROMPT_path_gland_repair_20260428.md` — most recent Cursor prompt; close-pattern reference

---

End of prompt. When done, push commit and update `MEMORY.md` with a one-line index entry referencing the migration markdown.
