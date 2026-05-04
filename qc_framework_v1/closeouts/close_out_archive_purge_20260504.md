# Close-out — MotherDuck archive purge (mig_archive_purge_20260504)

**Date:** 2026-05-04  
**Executor:** cowork (out-of-band MotherDuck session, `.eras` publication account)  
**Publication DB:** `thyroid_canonical_publication_v1_0` — **no canonical `canonical_*_v1` tables modified**  
**Target:** attached DB `"Thyroid 2026 UPdated"`  
**Method:** `drop_schema_cascade_audited` (pre-flight dependency check, then ordered `DROP TABLE` / `DROP SCHEMA … CASCADE`)  
**Audit trail (repo):** `archive_dropped_20260504/README.md` + `archive_dropped_20260504/manifest.csv`

---

## §1 What was dropped

All eleven archive/legacy schemas under `"Thyroid 2026 UPdated"`, plus three orphan `main.*_archived_20260422` tables — **791 tables total**, ~6M rows (manifest-only audit; contents not exported to git).

| Schema / scope | n_tables | total_rows | total_cols |
|----------------|---------:|-----------:|-----------:|
| archive_pub_v1_0 | 584 | 4,482,198 | 78,517 |
| archive_legacy | 121 | 868,243 | 4,196 |
| us_legacy_20260421 | 18 | 222,830 | 368 |
| note_entities_llm_legacy_20260422 | 9 | 121,164 | 203 |
| tier2_legacy_20260422 | 12 | 73,303 | 567 |
| molecular_legacy_20260421 | 13 | 70,584 | 378 |
| llm_invasion_legacy_20260425 | 2 | 68,705 | 46 |
| manuscript_workspace_legacy_20260422 | 12 | 56,461 | 108 |
| cpm_tirads_legacy_20260421 | 15 | 21,755 | 3,292 |
| main (3 orphan tables) | 3 | 17,906 | 1,654 |
| verify_legacy_20260422 | 2 | 13,117 | 25 |
| **TOTAL** | **791** | **~6.0M** | **89,354** |

Replay SQL and narrative are recorded in `archive_dropped_20260504/README.md`.

---

## §2 What was preserved

- **`thyroid_canonical_publication_v1_0`** — entire publication database unchanged (all `main.canonical_*` / manuscript / semantic / raw objects as-before).
- **`thyroid_canonical_publication_v1_0.archive_pub_v1_0.canonical_recurrence_resolved_v1_pre_restore_20260504`** — intentional same-day pre-snapshot (recurrence migration), left in place.
- **`"Thyroid 2026 UPdated"`** database shell remains attached; post-cleanup it exposes **only** `main` (empty after orphan table drops).

---

## §3 Safety checks

1. **Zero PUB-view dependencies:** Query against `thyroid_canonical_publication_v1_0.information_schema.views` for definitions referencing `"Thyroid 2026 UPdated"`, `archive_pub_v1_0`, `archive_legacy`, or `*_legacy_2026*` returned **0 rows** (see README).
2. **No publication canonical DDL:** This engagement did not `ALTER` / `UPDATE` / replace any `canonical_*_v1` table in the publication DB.
3. **Reversibility:** Re-creation is from repo migrations and history, not from this manifest (Parquet export was explicitly out-of-scope per README).

---

## §4 Registry sign-off (MotherDuck)

One row inserted into `thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1`:

| Field | Value |
|-------|--------|
| `schema_name` | `main` |
| `table_name` | `OPERATION_thyroid_2026_updated_archive_purge_20260504` |
| `signoff_migration` | `mig_archive_purge_20260504` |
| `table_status` | `verified` |
| `priority_tier` | `tier3_qc` |
| `n_columns_total` / `n_verified` / `n_na` | `1` / `1` / `0` (single operational sign-off unit; 791 drops detailed in notes + audit folder) |

---

## §5 Carry-forwards

- None required for publication analytics: PUB DB was untouched.
- Any future script that still referenced `"Thyroid 2026 UPdated".archive_pub_v1_0` (or sibling legacy schemas) must be treated as **broken by design** until retargeted to PUB or to git history.

---

## §6 References

- `archive_dropped_20260504/README.md` — full context, pre-flight SQL, replay order.
- `archive_dropped_20260504/manifest.csv` — per-table row/column counts.
- Memory / playbook: `feedback_alter_view_dependents` — avoided because no live view bodies pointed at dropped objects.
