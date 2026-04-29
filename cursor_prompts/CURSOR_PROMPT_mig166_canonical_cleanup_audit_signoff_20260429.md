# Cursor Prompt — mig_166 canonical_cleanup_audit_v1 Sign-off

**Lane:** 54 / mig_166
**Batch_id:** `mig_166_canonical_cleanup_audit_v1_signoff_20260429`
**Generated:** 2026-04-29 (late evening)
**Type:** Single-table sign-off. Registry-only writes. Path C apply via Cowork after Cursor SQL ships.

---

## §0 Governance

- Read + author SQL only; no `query_rw` from agent.
- Single SQL file: `qc_framework_v1/migrations/166_canonical_cleanup_audit_v1_signoff_20260429.sql`.
- Pre-snapshot the registry slice for `canonical_cleanup_audit_v1`.

## §1 Why this lane

`canonical_cleanup_audit_v1` is the **last remaining `not_started` row in the canonical_* namespace** (per Cowork 2026-04-29 probe — every other `canonical_*` table is verified or in_progress).

Registry state:
- `n_columns_total` = 18
- `n_verified` = 0
- `n_na` = 2
- `n_not_started` = **16**
- `table_status` = `not_started`
- `signoff_migration` = NULL

Closing this table out brings the canonical_* layer (excluding PM) to **89/89 verified** and is small enough for one focused lane.

## §2 Required pre-flight probes (paste counts into SQL header)

```sql
-- §2a Confirm physical existence + dtypes
SELECT column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_cleanup_audit_v1'
ORDER BY ordinal_position;

-- §2b Row count + cohort presence
SELECT COUNT(*) AS n_rows, COUNT(DISTINCT research_id) AS n_distinct_rids
FROM main.canonical_cleanup_audit_v1;
-- (If no research_id col, replace with the natural key revealed in §2a.)

-- §2c Existing registry rows for this table
SELECT column_name, verification_status, verification_method, batch_id, notes
FROM main.canonical_column_verification_registry_v1
WHERE table_name='canonical_cleanup_audit_v1'
ORDER BY column_name;

-- §2d Build provenance — find the script that builds this table
--   Grep `scripts/` for `canonical_cleanup_audit_v1` (CREATE OR REPLACE TABLE statements)
--   to identify the methodology lineage.

-- §2e BOOLEAN cohort-uniformity sweep on every BOOLEAN col (BOTH directions per Cowork rule).
-- §2f Date-type check on every *_date col (DATE only — no TIMESTAMP/VARCHAR per
--     feedback_clinical_dates_calendar_only.md).
-- §2g Any VARCHAR-with-units? Numeric measurements stored as VARCHAR is a CF.
```

## §3 Verification methodology

`canonical_cleanup_audit_v1` is most likely an **audit/governance table** (built by some cleanup script). The natural verification methodology is:

- **`auto_governance_audit_table_skip`** for cols that are pure provenance (build_ts, audit_ts, script_id, etc.) → flip to `na`
- **`derivation_vs_<source_script>_<column>`** for cols that record meaningful audit evidence → flip to `verified`
- **`auto_provenance_skip`** for any standard timestamp / extracted_at / built_at cols

If §2d reveals the table is purely a one-shot audit log with no analytic columns, all 16 not_started cols can be auto-na'd to `verified` table_status. If it carries real audit signal, classify per-column.

## §4 SQL structure for `166_canonical_cleanup_audit_v1_signoff_20260429.sql`

### Section A — Pre-snapshot
Registry slice snapshot: `CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig166_20260429 AS SELECT * + pre_mig166_snapshot_ts FROM ... WHERE table_name='canonical_cleanup_audit_v1';`

### Section B — Cohort-uniformity findings (commented)
Paste sweep results from §2e/§2f/§2g into header as comments (TRUE/FALSE/NULL counts per BOOLEAN, dtype audit per col).

### Section C — UPDATE blocks
One UPDATE per disposition bucket (verified vs na). Use methodology vocabulary from §3. Set `batch_id`, `verified_by='logan'`, `verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP)`.

### Section D — Resync signoff registry
mig_159 §159g pattern. Flip table_status to `verified` if `n_not_started + n_failed = 0` post-update.

### Section E — CFs (open only if findings)
- `CF-mig166-COHORT-UNIFORM-FALSE-<col>` (Type-B placeholder; reclassify col verified→na in mig_166b)
- `CF-mig166-COHORT-NEAR-UNIFORM-TRUE-<col>` (Type-A presence flag; keep verified, informational)
- `CF-mig166-DATE-RETYPE-<col>` (TIMESTAMP/VARCHAR date col; defer to next global retype lane)
- `CF-mig166-VARCHAR-WITH-UNITS-<col>` (numeric stored as VARCHAR with embedded units; defer to mig_144b-pattern retype lane)

## §5 Expected post-apply state

- `canonical_cleanup_audit_v1` → table_status = verified
- gate1 increases by 1
- gate5 unchanged (audit table cols allowlisted via existing audit_allowlist where appropriate)
- All other gates unchanged at 0

## §6 Git workflow

- File: `qc_framework_v1/migrations/166_canonical_cleanup_audit_v1_signoff_20260429.sql`
- Commit: `qc: mig_166 canonical_cleanup_audit_v1 sign-off (18 cols)`
- Push to `origin/main`.

## §7 Out of scope

- Do NOT modify the underlying `canonical_cleanup_audit_v1` data.
- Do NOT touch any other table.
- Do NOT apply to MD; ship SQL only.
- Do NOT carry mig_166 work into the auxiliary mass-na lane (mig_165 handles those).
