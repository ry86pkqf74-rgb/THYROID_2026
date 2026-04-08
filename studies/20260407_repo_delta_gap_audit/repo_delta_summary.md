## What is newly present (repo + live vs older signoff context)

- **Top-level `README.md`** already aligns April 2026 posture: institutional non-Tg wave **closed** (`final_institutional_20260407`), automation `119` **PASS WITH WARN** in lineage audit folder, **governance** caveat on MRQ/human review (wording evolved as live data changed).
- **`docs/motherduck_database_contract_v1.md`** is the **operational MotherDuck contract** (schemas, promotion, labs, specimen/FHIR pointers) — supersede ad-hoc table lists in older notes.
- **Multimodal operator path** is documented (`docs/multimodal_contract_runbook.md`, `docs/multimodal_release_gate.md`, scripts **128** / **129**, workflow `.github/workflows/motherduck_episode_pipeline.yml`).
- **Makefile `check_md_rw_token`** calls `motherduck_client.get_token()`, which **includes** `.streamlit/secrets.toml` — secrets-only RW tokens work for `make md-*` without exporting env vars (see `motherduck_capability_audit.md`).
- **Fresh live query (2026-04-08):** `main.longitudinal_lab_canonical_v1` includes **`final_institutional_20260407`** (989 rows) alongside Tg waves — supersedes the **wave table** in [`../20260407_publication_signoff_live/lab_coverage_memo.md`](../20260407_publication_signoff_live/lab_coverage_memo.md) (which listed only Tg-family waves).

## What is still blocking (this workspace + live)

- **`119 --release-mode` on this machine:** **BLOCKED** with **3 FAIL** — canonical row-count parity vs **local** parquet snapshots reports `local=-1` for `canonical_extracted_fact_long_v2`, `canonical_fact_quarantine_v2`, `note_extraction_runs` (files missing or not materialized under `processed/` in this checkout). **MotherDuck-side checks** in the same run largely **PASS** (including governance 5b, specimen/FHIR, molecular contract). *Fix:* materialize local canonical parquets (e.g. run fact-lineage materialization) before treating local-vs-MD parity as meaningful on a laptop checkout.
- **Read-scaling Business token:** not configured in secrets/env (`read_scaling_token_mode: none`). **RW** token works; **dedicated read-scaling attach** not exercised.

## What is scaffolding / partial

- **End-to-end deterministic multimodal chain** (US/TIRADS → FNA/Bethesda → molecular → surgery/pathology) as a **single promoted `main.*` contract** — **not** present; **129** is **imaging ↔ FNA**; **128** builds **star-schema + validation** primarily under **`mm_contract_dev`** (see `multimodal_status.md`).
- **`data_dictionary.md`** — legacy local DuckDB / provenance narrative; use **`docs/motherduck_database_contract_v1.md`** + live `information_schema` for MotherDuck.
- **Script `124` dry-run** via `make md-live-release-dryrun` was **still running without stdout** after ~11+ minutes in this session (likely long MotherDuck scan or buffering) — treat **exit status unknown** here; re-run with `python -u` if logs are needed.

## Code fix shipped in this audit

- **`tests/test_publication_governance.py`:** mypy-safe `fetchone()` null checks (no behavior change).
