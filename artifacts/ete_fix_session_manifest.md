# ETE Fix Session Manifest

| Field | Value |
|---|---|
| Start timestamp (UTC) | 2026-04-13 21:49:21Z |
| Local date | 2026-04-13 |
| Git SHA (base) | 4795d42df5f7b63b1c94a69395a4db8300657a9d |
| Base branch | main (fast-forwarded from origin/main) |
| Active branch | ete-remediation-20260413 |
| Python version | 3.14.2 |
| DuckDB (local) | 1.1.3 |
| DuckDB (MotherDuck server) | v1.5.1 |
| MotherDuck region | aws-us-east-1 |
| MotherDuck principal | logan.glosser@gmail.com (PAT token) |
| Token types loaded | read_write PAT + read_scaling PAT (env vars only; never written to artifacts) |

## Service-account status

No dedicated `ete_fix_rw` service account exists yet. MotherDuck service account provisioning is not exposed via SQL on this plan — it must be created through the org admin UI. For this session we operate under the logged-in PAT, logging every DDL in this manifest and in `ete_md_snapshots.json`. **Governance follow-up (Phase 7):** create a dedicated service account before the next release rerun.

## Frozen ETE export SHA-256 (captured before any edits)

| File | Size bytes | mtime | SHA-256 |
|---|---|---|---|
| exports/ptc_full.csv | 5,573,899 | 2026-03-10 20:30 | 8583a6456cdec0734f7f069292e65e66eca830742d2db5fe95b4a1768e3906e0 |
| exports/recurrence_full.csv | 623,400 | 2026-03-10 20:30 | bb3ebf6f536100037afb87dd2ab0aca70e46ecd28f4b38ab86a27c0c0d7e38f7 |
| exports/imaging_correlation.csv | 141,113 | 2026-03-10 20:30 | fe337f98da394244066dbabd7de6149fa05979d3feb9971b1066739fe424ac42 |

## MotherDuck inventory at session start

12 attached databases; 1 share (`thyroid_research_ro_v2` → 376 objects).

- `Thyroid 2026` (DUCKLAKE, 2026-04-02) — primary production lake.
- `Thyroid 2026 Molecular QA 20260407` (DEFAULT, 2026-04-13) — latest QA.
- `Thyroid 2026 Molecular PrePromote specimen_fhir_replay_20260413_1625`.
- `Thyroid 2026 Molecular PrePromote 20260408_full_081638_exec` — 20260408 release rehearsal.
- Plus 4 older PrePromote + 1 Dev DB.
- Default snapshot retention: 7 days.

## Session will create

- Scratch DB `thyroid_ete_fix_20260413` (zero-copy clone of the declared promoted source).
- Named snapshots: `ete_pre_export_decision`, `ete_pre_ajcc7_unification`, `ete_pre_psm_policy`, `ete_pre_manuscript_packaging`.

## Non-negotiable safety rails

1. No writes to `Thyroid 2026` or `thyroid_research_ro_v2`.
2. All destructive SQL runs only in `thyroid_ete_fix_20260413`.
3. Manuscript numerics propagated only from the declared source-of-truth branch chosen in Phase 3.
4. Freshness claim ("latest live reanalysis") blocked until release-governance gate is green.
5. Snapshots captured before each risky phase; IDs recorded in `ete_md_snapshots.json`.
6. No tokens, secrets, or PATs are written to any artifact.
