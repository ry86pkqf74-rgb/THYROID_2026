# Script 389.1 — Registry Schema Migration

**Stamp:** 2026-04-22
**Type:** Standalone schema migration (discrete, not dependent on 389 / 390)
**DB:** `thyroid_canonical_publication_v1_0`
**Target:** `main.detail_table_registry_v1`
**Prereq:** none — runnable now or after 389 / 390 close in any order

---

## Problem

`detail_table_registry_v1` is the authoritative catalog of tier-2 canonical tables, but it has no columns to record when a table was archived, renamed, or superseded. Across Scripts 358, 361, 362, 363, 386, 387, and 388, entries have been archived or renamed and the only audit trail lives in commit messages + `archive_pub_v1_0.*` snapshot names + ad-hoc `__readme` rows. That's not queryable.

## Scope

Add two columns to `main.detail_table_registry_v1`:

1. `superseded_by` (VARCHAR, NULLable) — the canonical table name that replaced this one (e.g., `canonical_operative_events_v1`). NULL = still live.
2. `renamed_by_script` (VARCHAR, NULLable) — the script number that retired/renamed the row (e.g., `361`, `387`). NULL = never renamed.

Backfill values from existing evidence:
- `archive_pub_v1_0.*` snapshot table names (the `_preNNN_` stamp encodes the script)
- `archive_move_log_v1` rows (built by 387 / 389)
- Close-out markdown files in `scripts/output/*_close_out.md`

### Out-of-scope
- No row deletions from the registry — superseded rows stay with a non-NULL `superseded_by`
- No other column additions (keep the migration minimal)
- No writes to `manuscript_workspace.*` or `archive_pub_v1_0.*`
- No touching of any other canonical table

## Execution phases

### Phase 0 — Probe
- List current registry row count + column set
- Cross-join against `archive_pub_v1_0.*` table names → derive candidate (detail_table_name, script, stamp) tuples
- Build a dry-run backfill map: `{detail_table_name → (superseded_by, renamed_by_script)}` — print before writing
- Halt file: `scripts/output/389_1_probe_report.md` with the proposed map

### Phase 1 — Plan-review gate
Logan reviews the probe report. No approval file needed — the mapping is deterministic from archive names. Proceed if probe looks clean.

### Phase 2 — Apply
2A. **Snapshot:**
```sql
CREATE OR REPLACE TABLE archive_pub_v1_0.detail_table_registry_pre389_1_<STAMP> AS
SELECT * FROM main.detail_table_registry_v1;
```

2B. **ALTER + backfill:**
```sql
ALTER TABLE main.detail_table_registry_v1 ADD COLUMN IF NOT EXISTS superseded_by VARCHAR;
ALTER TABLE main.detail_table_registry_v1 ADD COLUMN IF NOT EXISTS renamed_by_script VARCHAR;

-- Backfill from the map produced in Phase 0. Example shape:
UPDATE main.detail_table_registry_v1
SET superseded_by = 'canonical_operative_events_v1',
    renamed_by_script = '361'
WHERE detail_table_name = 'operative_episode_detail_v2';
-- ... (one UPDATE per mapped row)
```

2C. **`__readme` provenance row:**
```sql
INSERT INTO main.__readme(content, updated_at) VALUES
('Script 389.1: detail_table_registry_v1 schema migration — added superseded_by + renamed_by_script columns; backfilled N rows from archive_pub_v1_0 snapshot names and archive_move_log_v1. Snapshot: archive_pub_v1_0.detail_table_registry_pre389_1_<STAMP>.',
 CAST(CURRENT_TIMESTAMP AS TIMESTAMP));
```

### Phase 3 — Verify
- Registry row count unchanged
- New columns present and typed VARCHAR
- Every registry row whose `detail_table_name` matches an `archive_pub_v1_0.*` snapshot pattern has a non-NULL `renamed_by_script`
- Every row with non-NULL `superseded_by` has a live table in `main.*` matching that name (or a documented exception)
- `__readme` row landed

### Phase 4 — Commit + tag
- Staged: `scripts/389_1_registry_schema_migration.py`, `scripts/output/389_1_probe_report.md`, `scripts/output/389_1_run.log`, `scripts/output/389_1_close_out.md`
- Commit message: `Script 389.1: detail_table_registry_v1 schema migration — superseded_by + renamed_by_script backfill`
- Tag: `v1_0-registry-migrated-<stamp>`

## Idempotency
- If both new columns already exist AND `__readme` row with `Script 389.1:` prefix exists → exit 0, NO-OP
- If columns exist but `__readme` row missing → halt with "partial migration detected; manual review required"

## Non-goals
- Don't add a `deprecated_at` timestamp (YAGNI — `renamed_by_script` + commit date give it)
- Don't backfill pre-v1_0 archives (scope = what happened since 2026-04-17)
- Don't change row ordering or primary-key-adjacent columns

## First action for the agent
Run Phase 0 probe. Print the proposed backfill map. Halt. If map looks reasonable (no more than ~15 mapped rows, every mapping traceable to a specific archive snapshot), proceed to Phase 2.
