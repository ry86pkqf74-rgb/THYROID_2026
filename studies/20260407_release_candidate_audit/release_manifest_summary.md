# Release manifest summary (live MotherDuck)

**Source:** `qa.release_manifest` on database **Thyroid 2026**, read via fail-closed `connect_md_or_file(..., md=True, fail_closed=True)` on 2026-04-07.

## Rows (ordered by `release_tag` descending)

| release_tag | created_at (stored) | created_by |
|-------------|---------------------|------------|
| 20260409 | 2026-04-07 02:05:07.189573 | scripts/126_final_master_release.py |
| 20260408 | 2026-04-07 02:03:20.732093 | scripts/126_final_master_release.py |
| 20260407 | 2026-04-07 01:09:57.717289 | scripts/115_release_snapshot.py |
| 20260406 | 2026-04-07 04:07:52.519215 | scripts/115_release_snapshot.py |

## Ordering nuance

- **Latest row by `created_at`:** `20260406` (last insert wins for `main.master_*_verified_v1` scalar `release_tag`, which uses `ORDER BY created_at DESC LIMIT 1`).
- **Largest calendar tag present in manifest:** `20260409`.
- **Immutable snapshot schemas** on this database (from `information_schema.tables`): `release_20260406`, `release_20260407`, `release_20260408`, `release_20260409`.

**Reconciliation:** Manifest chronology does not sort by tag name. The analyst views intentionally pin `release_tag` to the **newest manifest insert**, not the maximum `release_YYYYMMDD` suffix. Snapshot schemas can exist for tags that are not the scalar `release_tag` shown on master views.
