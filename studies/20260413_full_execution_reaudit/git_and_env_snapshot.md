# Git and environment snapshot

| Item | Value |
|------|--------|
| pwd | `/Users/ros/THyroid 2026` |
| git branch | `main` |
| git HEAD | `9f3e41f809b070d02d32b2be3be872a92b708c11` |
| Python | 3.14.4 |
| `.venv` | present |
| MotherDuck token | SET via `motherduck.local.toml` (`token_mode`: `motherduck.local.toml:MOTHERDUCK_TOKEN`; length not printed per AGENTS.md) |
| Raw workbooks (repo `raw/`) | `COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx`, `US Nodules TIRADS 12_1_25.xlsx`, `Imaging_12_1_25.xlsx`, `FNAs 12_5_2025.xlsx` — all present locally |
| `thyroid_master.dvc` | absent at repo root (no DVC pointer for local duckdb file in this workspace) |

**Preflight git status (snapshot):** numerous untracked paths under repo (see `commands_run.log`); audit artifacts are confined to `studies/20260413_full_execution_reaudit/`.
