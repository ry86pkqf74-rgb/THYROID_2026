# MotherDuck artifacts (git-tracked)

This folder holds **non-PHI aggregate exports** produced after running scripts against
MotherDuck so results are reproducible in Git without copying clinical rows.

- `exports/imaging_fna_linkage_mm_v1_audit.json` — written by
  `scripts/129_imaging_fna_linkage_mm_v1.py --md`. When linkage runs successfully,
  includes counts from `val_imaging_fna_linkage_audit_v1`. If `fna_episode_master_v2`
  is not present in the target MotherDuck database (`status` = blocked), the file
  still records connectivity + imaging row counts so the gap is visible in PRs.

- `paths.py` — importable constants for export locations (no I/O).

Authentication uses `.streamlit/secrets.toml` (`MOTHERDUCK_TOKEN` / `MD_SA_TOKEN`) or
the same vars in the environment; run from the repo root so TOML resolves.

Regenerate:

```bash
cd /path/to/THYROID_2026
.venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py --md
```
