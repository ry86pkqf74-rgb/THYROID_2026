# Date & Timeline System Prompt for Thyroid Research 2026

Use this prompt as context when working on date association, timeline construction,
recurrence endpoints, time-to-RAI calculations, or any analysis requiring
event-level temporal resolution.

---

## Database Targeting

- **All writes** (CREATE VIEW, ALTER TABLE, UPDATE, INSERT) target `thyroid_research_2026` using bare table names.
- **Note entity reads** in enriched views may come from `thyroid_share.note_entities_*` (read-only share) or bare names in the RW database.
- **Never write** to `thyroid_share`; it is read-only.

## Date Column Reference

| Table | Date Column | Type | Quoting | Notes |
|-------|-------------|------|---------|-------|
| `clinical_notes_long` | `note_date` | VARCHAR | No | YYYY-MM-DD from note header |
| `note_entities_*` (6) | `entity_date` | VARCHAR | No | Extracted near entity; high null rate |
| `note_entities_*` (6) | `inferred_event_date` | DATE | No | Backfilled by script 27 |
| `molecular_testing` | `"date"` | VARCHAR | Yes — reserved word | Day-level or year-only |
| `genetic_testing` | `"date"` | VARCHAR | Yes — reserved word | Same source as molecular_testing |
| `path_synoptics` | `surg_date` | VARCHAR | No | NOT `surgery_date` |
| `operative_details` | `surg_date` | VARCHAR | No | Same naming as path_synoptics |
| `fna_history` | `fna_date_parsed` | VARCHAR | No | YYYY-MM-DD; `fna_date` is a computed alias |
| `fna_cytology` | `fna_date` | VARCHAR | No | Different table from fna_history |

## Fallback Precedence (enforced in scripts 15 and 27)

```
entity_date (100, day)
  → note_date (70, day)
    → surg_date (60, day)
      → molecular_testing."date" (60 day / 50 year)
        → fna_date_parsed (55, day)
          → unrecoverable (0, NULL)
```

## Year-Only Molecular Dates

When `molecular_testing."date"` is a bare 4-digit year (e.g., `'2022'`):
- Convert to `YYYY-01-01` placeholder.
- Set `date_granularity = 'year'` and `date_confidence = 50`.
- Filter dirty values: `'x'`, `'X'`, `''`, `'None'`, `'maybe?'`.

## Date Parsing Rules

- Always use `TRY_CAST(col AS DATE)` — never bare `CAST` on potentially dirty VARCHAR dates.
- For `note_date` (VARCHAR), use `TRY_CAST(note_date AS DATE)`.
- For molecular date year detection: `regexp_matches(CAST("date" AS VARCHAR), '^\d{4}$')`.
- Dedup anchors with `ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY ...)`.

## Provenance Columns on Base Tables

Added by `scripts/27_date_provenance_formalization.sql`:

| Column | Type | Description |
|--------|------|-------------|
| `inferred_event_date` | DATE | Best date from fallback chain |
| `date_source` | VARCHAR | `entity_date`, `note_date`, `surg_date`, `molecular_testing_date`, `fna_date_parsed`, `unrecoverable` |
| `date_granularity` | VARCHAR | `day` or `year` |
| `date_confidence` | INTEGER | 0–100 |

## Date Status Taxonomy (from script 17, V3 enriched views)

| Status | Meaning | Confidence |
|--------|---------|------------|
| `exact_source_date` | entity_date available | 100 |
| `inferred_day_level_date` | note_date fallback | 70 |
| `coarse_anchor_date` | surg/FNA/molecular fallback | 35–60 |
| `unresolved_date` | no source recoverable | 0 |

## Key Views for Date Work

| View | Purpose |
|------|---------|
| `enriched_note_entities_*` (6) | Script 15: provenance computed at query time |
| `missing_date_associations_audit` | Script 15: union audit across all domains |
| `date_recovery_summary` | Script 15: aggregate stats by domain x source |
| `enriched_master_timeline` | Script 27: audit minus unrecoverable |
| `date_rescue_rate_summary` | Script 27: rescue rate KPI per domain |
| `validation_failures_v3` | Script 17: date quality issues |
| `event_date_audit_v2` | Script 22: V2 canonical date audit |

## Safety Rules

- **Never overwrite** `entity_date` or `note_date` — these are source-of-truth.
- **Never invent dates** — if no fallback exists, mark as `unrecoverable`.
- **Preserve original values** when adding inferred fields.
- **No fuzzy matching** on evidence_span or free text without explicit approval.
- **PHI safety**: never print full clinical note text in logs; use truncated snippets.

## Multi-Surgery Patients

- Use `master_timeline` for correct surgery-date resolution.
- `path_synoptics` may have duplicate `research_id` for re-operations; dedup with `ROW_NUMBER()`.
- Select earliest surgery (`ORDER BY surg_date ASC`) unless analysis requires a specific operation.

## Smoke Test Queries

```sql
-- Overall rescue rate
SELECT * FROM date_rescue_rate_summary;

-- Unrecoverable count (should be minimized)
SELECT COUNT(*)
FROM missing_date_associations_audit
WHERE date_source = 'unrecoverable';

-- Verify base table backfill
SELECT
    COUNT(*) AS total,
    COUNT(inferred_event_date) AS has_date,
    COUNT(*) FILTER (WHERE date_source = 'unrecoverable') AS unrecoverable
FROM note_entities_genetics;
```
