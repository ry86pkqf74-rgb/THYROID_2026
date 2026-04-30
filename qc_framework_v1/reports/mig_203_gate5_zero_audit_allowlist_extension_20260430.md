# mig_203 close-out — gate5 → 0 (v11 audit) + PM registry refresh

**Date:** 2026-04-30  
**Batch:** `mig_203_gate5_zero_audit_allowlist_extension_20260430`  
**CF closed:** `CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION`

## 1. Problem

- **Gate5 (legacy audit):** Six `canonical_patient_master` columns were counted as date-type violations despite correct semantics: allowlist omitted explicit stamp names (`cpm_built_at`, `rollup_built_at`, `resolved_at`, …) and did not exclude `_derived_at` / `_resolved_at` / `_confidence` suffix patterns.
- **Registry drift:** mig_188b added ten AJCC `*_resolved` / resolution metadata columns on PM without `canonical_column_verification_registry_v1` rows; `canonical_table_signoff_registry_v1` counts were stale vs physical columns.

## 2. Fix

1. **`qc_framework_v1/queries/cleanliness_audit_v11.sql`** — v11 audit template: extended `audit_allowlist` + `regexp_matches` exclusions for `_built_at$`, `_derived_at$`, `_resolved_at$`, `_confidence$`.
2. **`qc_framework_v1/migrations/203_gate5_zero_audit_allowlist_extension_20260430.sql`** — idempotent INSERT of ten column registry rows (joined to `information_schema.columns` for `data_type` / `ordinal_position`), resync PM `n_columns_total` / `n_verified` from live counts, idempotent provenance row, inline v11 five-gate SELECT.

## 3. Expected post-state

- Five-gate audit (v11): **gate1 = 172**, **gate2 = gate3 = gate4 = gate5 = 0** (subject to live verified-table inventory).
- PM signoff rollup: **~1,606 verified / 24 na / 0 not_started / ~1,630 total** (exact verified/na follow live registry; total matches `information_schema` column count for PM).

## 4. Future-proofing

When adding PM columns via `ALTER TABLE … ADD`, append matching rows to `canonical_column_verification_registry_v1` in the same migration batch and resync `canonical_table_signoff_registry_v1` counts (or extend the v11 audit allowlist if the column is an intentional TIMESTAMP/VARCHAR stamp excluded from manuscript date hygiene).
