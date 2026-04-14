# Checked-in release manifest pointers

## Authoritative live source

**Do not** treat files in this directory as the current promotion state unless they were **just** regenerated from MotherDuck.

- **Live SSOT:** `qa.release_manifest` on the MotherDuck catalog (same database as `main`; typically database name `Thyroid 2026` or your org default).
- **Repo checkpoint:** `LATEST_MANIFEST.json` may lag live by weeks — it embeds `role`, `authoritative_live_source`, and `do_not_use_as_current_without_regeneration` when present.

## Refreshing `LATEST_MANIFEST.json`

Run (requires RW MotherDuck token via `motherduck_client.get_token()` / `motherduck.local.toml`):

```bash
.venv/bin/python scripts/145_export_release_manifest_pointer.py --md
```

This overwrites `exports/release_manifests/LATEST_MANIFEST.json` with a snapshot of the **latest** row from `qa.release_manifest` (ordering aligned with `scripts/125_master_verified_views.py`).

Historical JSON files with other names are **not** deleted; they remain for audit.
