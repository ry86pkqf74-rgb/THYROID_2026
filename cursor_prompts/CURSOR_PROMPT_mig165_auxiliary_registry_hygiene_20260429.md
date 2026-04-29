# Cursor Prompt — mig_165 Auxiliary Registry Hygiene (mass auto-na + stale-row cleanup)

**Lane:** 53 / mig_165
**Batch_id:** `mig_165_auxiliary_registry_hygiene_20260429`
**Generated:** 2026-04-29 (late evening)
**Type:** Registry-only writes. **No data writes**. Path C apply via Cowork after Cursor SQL ships.

---

## §0 Governance

- Read + author SQL only; no `query_rw` from agent.
- One SQL file: `qc_framework_v1/migrations/165_auxiliary_registry_hygiene_20260429.sql`.
- One audit Markdown: `qc_framework_v1/reports/mig_165_aux_registry_classification_20260429.md` listing every affected row with the disposition (auto_na vs stale_drop vs needs_real_verification).
- Pre-snapshot the registry slice for every affected row.

## §1 Why this lane

Cowork ground-truth probe (2026-04-29):

- `canonical_table_signoff_registry_v1` has **175 rows total**: 88 verified, 1 in_progress (PM), **86 not_started**.
- Only **1** of the 86 not_started has prefix `canonical_*` (`canonical_cleanup_audit_v1` — handled in mig_166).
- The other **85 not_started rows** are:
  - **2** archive_*: `archive_candidate_review_v1`, `archive_move_log_v1`
  - **83** "other" (no `canonical_` / `note_entities_` prefix): cpm_*_audit_v1, *_imaging, ln_*_v1, registry_v2_*, qc_*, schema_reorg_*, etc.
- Of those 83, **only 30 have a physical table backing them** in `main`. The remaining **53 are stale registry rows** with no underlying table (likely casualties of script 387 / 388 / 389 archive moves).

Plus there's one **physical orphan**: `note_entities_llm_presenting_symptoms` (BASE TABLE, NOT in registry) — needs registration as a Tier-1 raw mirror.

This lane (a) drops or auto-na's the 53 stale rows, (b) auto-na's the 30 phys-backed feeder/audit/queue tables with the appropriate Tier-1 / governance methodology, (c) registers `note_entities_llm_presenting_symptoms` as `na` Tier-1 raw mirror, and (d) handles the 2 archive_* rows.

## §2 Required pre-flight probes (paste counts into SQL header)

```sql
-- §2a Stale registry rows (no phys backing) — count & list
WITH ns AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
  WHERE table_status='not_started' AND table_name NOT LIKE 'canonical_%'
)
SELECT
  ns.table_name,
  CASE WHEN i.table_name IS NULL THEN 'STALE' ELSE 'PHYS_BACKED' END AS phys_status
FROM ns
LEFT JOIN information_schema.tables i
  ON i.table_catalog='thyroid_canonical_publication_v1_0' AND i.table_schema='main' AND i.table_name=ns.table_name
ORDER BY phys_status, ns.table_name;

-- §2b For phys-backed rows, classify into:
--   * tier1_raw_mirror      — note_entities_*, clinical_notes_long, clinical_note_ln_extracted_v1, path_synoptics, ct_imaging, mri_imaging, nuclear_med, thyroid_sizes, thyroid_weights
--   * cpm_audit_or_queue    — cpm_*_audit_v1, *_review_queue_v1, *_review_v1, qc_*, *_conflict_v1, *_disagreements_v1, *_discordance_v1
--   * registry_governance   — registry_v2_*, schema_reorg_*, script_38[789]_*, object_domain_map_v1, detail_table_registry_v1, data_dictionary_v279, main_schema_keep_list_v1
--   * imaging_or_specimen   — imaging_*, specimen_*, lesion_*, episode_analysis_*, patient_analysis_*
--   * lab_orphan_or_tg      — lab_orphan_*, tg_*
--   * recurrence_event      — recurrence_event_clean_v1, recurrence_imaging_suspicious_candidates_v1
--   * v1_1_or_other         — v1_1_finalization_audit_v1, *_v1 not otherwise classified

-- §2c Stale archive_* rows — confirm zero phys backing
SELECT * FROM information_schema.tables
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name IN ('archive_candidate_review_v1','archive_move_log_v1');

-- §2d Confirm note_entities_llm_presenting_symptoms is BASE TABLE in main and absent from both registries
```

## §3 Disposition rules (Logan-ratified vocabulary)

For each phys-backed `not_started` row:

| Bucket | verification_method (table-level note) | Action |
|---|---|---|
| tier1_raw_mirror | `auto_tier1_raw_mirror_skip` | Flip every column row to `na` with this method; flip table_status to `verified` |
| cpm_audit_or_queue | `auto_governance_audit_table_skip` | Same |
| registry_governance | `auto_registry_governance_skip` | Same |
| imaging_or_specimen | classify per table — feeder vs analytic | Feeders → `auto_tier1_raw_mirror_skip`; analytic → leave `not_started` and emit `CF-mig165-AUX-NEEDS-REAL-VERIFY-<table>` |
| lab_orphan_or_tg | `auto_governance_audit_table_skip` | Same |
| recurrence_event | recurrence_event_clean_v1 → CF-mig165-RECURRENCE-EVENT-CLEAN-NEEDS-REAL-VERIFY (this is a real Tier-2 builder feeder; do NOT auto-na — refer to mig_163 lane) | Leave `not_started`; do NOT flip |
| v1_1_or_other | per-table judgment in Markdown report; default to `auto_governance_audit_table_skip` | Same |

For each stale (no-phys) `not_started` row:

- **Drop the registry row** if table doesn't exist anywhere in the publication DB (verify via `duckdb_tables()` cross-DB). Document drop in `qc_framework_v1/reports/mig_165_aux_registry_classification_20260429.md`.
- **OR** if the table exists in `archive_pub_v1_0` (archived intentionally per script 387/388/389), flip the column rows to `na` with method `auto_archived_in_archive_pub_v1_0_<script>` and flip table_status to `verified`. Open `CF-mig165-STALE-MIGRATED-TO-ARCHIVE-<table>` informational note.

For the 2 `archive_*` rows: verify zero phys backing; drop registry rows; document in report.

For `note_entities_llm_presenting_symptoms`: INSERT registry rows for every column; flip every col to `na` with method `auto_tier1_raw_mirror_skip`; INSERT signoff row with status `verified`.

## §4 SQL structure for `165_auxiliary_registry_hygiene_20260429.sql`

### Section A — Pre-snapshots
Two snapshots: full slice of `canonical_table_signoff_registry_v1` and `canonical_column_verification_registry_v1` for every affected row.

### Section B — Disposition application
- One UPDATE block per disposition bucket, batched by `table_name IN (...)`.
- One DELETE block for stale registry drops (only if Logan-ratified — the report should list each drop and the SQL should leave them commented until Logan signs off).
- Use `verification_status='na'` for auto-classified rows.

### Section C — `note_entities_llm_presenting_symptoms` registration
INSERT for column registry (one row per phys col), INSERT for signoff registry, all set to `na` / `verified`.

### Section D — Resync signoff registry
After all column-level changes, recompute n_verified / n_na / n_not_started / n_failed for every affected table_name (mig_159 §159g pattern).

### Section E — Required CFs
- `CF-mig165-AUX-NEEDS-REAL-VERIFY-<table>` for any analytic table the agent decided not to auto-na.
- `CF-mig165-STALE-MIGRATED-TO-ARCHIVE-<table>` for archive-only stale rows that got `na`-flipped instead of dropped.
- `CF-mig165-RECURRENCE-EVENT-CLEAN-NEEDS-REAL-VERIFY` if `recurrence_event_clean_v1` is left `not_started`.

## §5 Markdown report — `qc_framework_v1/reports/mig_165_aux_registry_classification_20260429.md`

A table with one row per affected registry entry: `table_name | phys_backed | bucket | disposition | reason`. This is the audit trail Logan needs to ratify each `na` flip and especially each DELETE.

## §6 Expected gate1 impact

Pre-mig_165: gate1 = 88 (or 89 if mig_162 has applied).
Post-mig_165: gate1 should jump to **88 + N**, where N is the number of phys-backed auxiliary tables that got auto-na'd to verified status. Report the expected delta in the SQL header.

If stale registry rows are also auto-na'd (instead of dropped), gate1 grows further; if they're dropped, the registry denominator shrinks instead.

## §7 Git workflow

- Files: `qc_framework_v1/migrations/165_auxiliary_registry_hygiene_20260429.sql` + `qc_framework_v1/reports/mig_165_aux_registry_classification_20260429.md`
- Commit: `qc: mig_165 auxiliary registry hygiene (mass auto-na + stale row classification)`
- Push to `origin/main`.

## §8 Out of scope

- Do NOT touch any `canonical_*` table column registry entries (those are core Tier-2; not in scope).
- Do NOT mutate any base table data; registry-only.
- Do NOT execute DELETE blocks unless explicitly Logan-ratified after report review (leave commented and ask).
- Do NOT auto-na `recurrence_event_clean_v1` — it's a real Tier-2 builder feeder; defer to mig_163 lane.
- Do NOT auto-na anything whose name matches `cpm_*` if it's actually used by `canonical_patient_master` — verify via `duckdb_views()` referencing patterns; if uncertain, leave `not_started` + open CF.
