# Cursor Prompt — mig_168 PM Controlled-Vocabulary Standardization Audit

**Lane:** 56 / mig_168
**Batch_id:** `mig_168_pm_controlled_vocabulary_audit_20260429`
**Generated:** 2026-04-29 (late evening)
**Type:** Read-only audit. Output is a Markdown report + a candidate registry-notes-only SQL file. No data writes.

---

## §0 Why this lane exists

`canonical_patient_master` has hundreds of categorical / VARCHAR analytic columns that should follow controlled vocabularies (SSOT enums). Across the prior 60+ migrations, no comprehensive pass has confirmed the vocabularies are consistent. Manuscript-grade analysis requires:

- Every categorical col has a defined enum (closed set of allowed values)
- No casing drift (`'Low'` vs `'low'` vs `'LOW'`)
- No whitespace drift (`'high '` vs `'high'`)
- No deprecated synonyms still in the data (`'undetermined'` vs `'indeterminate'`)
- Cross-canonical consistency (PM's `ata_risk_category` vocabulary == canonical_recurrence_resolved_v1's vocabulary if both encode the same concept)

This lane catalogs the vocabularies, finds drift, and proposes either (a) registry CF appendices (informational drift, doc only) or (b) data-fix migrations for genuine corruption.

## §1 Governance posture

- Read-only against MotherDuck. No `query_rw`.
- Output: `qc_framework_v1/reports/mig_168_pm_controlled_vocabulary_audit_20260429.md` (the catalog + findings) AND `qc_framework_v1/migrations/168_pm_controlled_vocabulary_audit_probes_20260429.sql` (commented SSOT enum + probe SQL; no apply).
- If this lane finds genuine corruption (e.g., 5 rows with `'low'` instead of `'Low'` due to a builder bug), DO NOT fix in this lane — open `CF-mig168-CASING-DRIFT-<col>` for follow-up in mig_168b.

## §2 Required scope — the categorical / VARCHAR cols to audit

Filter to the analytic VARCHAR columns on PM (excluding obvious provenance/source/keyword/raw text fields):

```sql
SELECT c.column_name, c.data_type
FROM information_schema.columns c
JOIN main.canonical_column_verification_registry_v1 r
  ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND c.table_name='canonical_patient_master'
  AND c.data_type='VARCHAR'
  AND r.verification_status='verified'
  AND c.column_name NOT LIKE '%_source'
  AND c.column_name NOT LIKE '%_source_note_ref'
  AND c.column_name NOT LIKE '%_keyword%'
  AND c.column_name NOT LIKE '%_raw'
  AND c.column_name NOT LIKE '%_note_ref'
  AND c.column_name NOT LIKE '%_evidence%'
  AND c.column_name NOT LIKE '%_text%'
ORDER BY c.column_name
```

Expected: ~150-300 columns. Agent should print the count in §1 of the report.

## §3 Per-column probe (run in batches; produce a CSV-style output)

For each col, capture:

```sql
WITH probe AS (
  SELECT
    '<col>' AS column_name,
    COUNT(*) AS total_rows,
    COUNT(<col>) AS n_nonnull,
    COUNT(DISTINCT <col>) AS n_distinct,
    COUNT(DISTINCT TRIM(<col>)) AS n_distinct_trimmed,
    COUNT(DISTINCT LOWER(TRIM(<col>))) AS n_distinct_caseinsensitive,
    SUM(CASE WHEN <col> IS NOT NULL AND <col> <> TRIM(<col>) THEN 1 ELSE 0 END) AS n_whitespace_drift,
    SUM(CASE WHEN <col> IS NOT NULL AND <col> <> LOWER(TRIM(<col>)) AND <col> <> UPPER(TRIM(<col>)) AND <col> <> INITCAP(TRIM(<col>)) THEN 1 ELSE 0 END) AS n_mixed_case,
    STRING_AGG(DISTINCT <col>, ' | ' ORDER BY <col>) FILTER (WHERE <col> IS NOT NULL) AS distinct_values_sample
  FROM main.canonical_patient_master
)
SELECT * FROM probe;
```

Use the `n_distinct vs n_distinct_caseinsensitive` mismatch to detect casing drift (e.g., 4 distinct values but 3 case-insensitive distinct → casing drift on at least one value). Use `n_whitespace_drift` for trim issues.

Cap the `distinct_values_sample` to the first 20 distinct values per col; truncate the rest.

## §4 Required findings to surface

For each col, classify into one of:

- **CLEAN** — n_distinct == n_distinct_caseinsensitive, n_whitespace_drift = 0, all values are in a recognized enum
- **CASING-DRIFT** — n_distinct > n_distinct_caseinsensitive
- **WHITESPACE-DRIFT** — n_whitespace_drift > 0
- **HIGH-CARDINALITY** — n_distinct > 50 (likely free-text leak; not a real enum)
- **DEGENERATE-1-VALUE** — n_distinct = 1 (already covered by mig_142b / mig_161 if known; flag if new)
- **MULTI-FORMAT** — same value with different formatting (`'2.5 cm'` vs `'2.5cm'` vs `'2.5'`)
- **ROGUE-VALUE-CANDIDATE** — distinct values include `'unknown'`, `'?', 'tbd', '<unknown>'`, etc.

Group findings by category. For each category, list the affected cols + sample distinct values + count.

## §5 Cross-canonical enum consistency check

For high-value categorical cols on PM, find any same-name col on a verified Tier-2 events/rollup table and compare distinct value sets:

| PM column | Tier-2 table | PM-only values | Tier-2-only values | Both |
|---|---|---|---|---|
| ata_risk_category | (not on a Tier-2) | — | — | — |
| recurrence_type | canonical_recurrence_v1 | ? | ? | ? |
| histology_final | canonical_path_malignant_events_v1 | ? | ? | ? |
| margin_status_final | canonical_invasion_events_v1 | ? | ? | ? |
| ... (~20 high-value pairs)

Open `CF-mig168-ENUM-DRIFT-<pm_col>-VS-<tier2_col>` for any pair with non-empty diff.

## §6 Recommended SSOT enum dictionary (output draft)

For each col with a discoverable enum (n_distinct ≤ 20), output a JSON-style dictionary in the report:

```yaml
ata_risk_category:
  status: CLEAN
  values: [low, intermediate, high]
  null_meaning: not_applicable_or_not_calculable
  ssot_owner: mig_155 risk-scoring cluster
recurrence_type:
  status: CASING-DRIFT
  values_observed: [Distant, distant, Structural, structural, ...]
  proposed_canonical: [distant, structural, biochemical, fna_confirmed, ...]
  fix_lane: mig_168b
```

This dictionary becomes the manuscript-pipeline SSOT vocabulary reference.

## §7 SQL stub (commented; do NOT apply)

`168_pm_controlled_vocabulary_audit_probes_20260429.sql` should contain the §3 probes (one block per col) + the §5 enum-consistency probe SQL — all commented out — so future runs can re-execute the audit without rebuilding the SQL.

## §8 Git workflow

- Files: 168 SQL stub + 168 Markdown report
- Commit: `qc: mig_168 PM controlled-vocabulary standardization audit (read-only)`
- Push.

## §9 Out of scope

- DO NOT fix any drift detected — that's mig_168b after Logan ratifies the proposed corrections.
- DO NOT add columns to the registry.
- DO NOT touch BOOLEAN cols (mig_167 / mig_159 / mig_154b cover those).
- DO NOT touch numeric cols (mig_169 covers data-type sanity).
- DO NOT modify SSOT enum lists in any existing migration files.
