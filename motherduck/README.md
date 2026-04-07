# MotherDuck artifacts (git-tracked)

This folder holds **non-PHI aggregate exports** produced after running scripts against
MotherDuck so results are reproducible in Git without copying clinical rows.

## Bootstrap `fna_episode_master_v2` (when script 22 parquets are unavailable)

Canonical episode table **22** normally builds `fna_episode_master_v2` from
`fna_history` loaded out of `processed/*.parquet`. If those files are not on the
machine that connects to MotherDuck, seed a compatible table from the patient-level
refined layer (first/last FNA date + Bethesda):

```bash
cd /path/to/THYROID_2026
.venv/bin/python scripts/motherduck_seed_fna_episode_master_v2.py
```

Then run linkage and refresh the audit export:

```bash
.venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py --md
```

For **full** multi-FNA fidelity (up to 12 rows per patient), materialize `fna_history`
locally, run `22_canonical_episodes_v2.py --md`, and replace the seeded table.

## Exports

- `exports/imaging_fna_linkage_mm_v1_audit.json` — written by
  `scripts/129_imaging_fna_linkage_mm_v1.py --md` (`status: ok` when linkage ran).
  If `fna_episode_master_v2` was missing, `status` is `blocked_*` and counts still
  record the gap.

- `paths.py` — importable constants for export locations (no I/O).

Authentication uses `.streamlit/secrets.toml` (`MOTHERDUCK_TOKEN` / `MD_SA_TOKEN`) or
the same vars in the environment; run from the repo root so TOML resolves.

## Unblocking local HOLDs (129 / 128)

If local `thyroid_master.duckdb` lacks episode/imaging/FNA tables, use the reusable agent prompt and command cheatsheet in [`reports/prompt_investigate_multimodal_holds.md`](../reports/prompt_investigate_multimodal_holds.md).
