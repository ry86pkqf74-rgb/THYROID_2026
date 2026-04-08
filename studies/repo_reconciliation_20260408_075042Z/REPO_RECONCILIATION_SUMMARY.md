# Repo reconciliation summary (UTC 2026-04-08T07:50:42Z)

## Scope

Minimal patches for operator-misleading drift: MotherDuck env mapping docs, “current state” narrative clarity, and explicit alignment of specimen/FHIR gates with **126** vs **115**/**118** release artifacts.

## Changes made

1. **`docs/motherduck_v2_staging_runbook.md`** — Removed false claim that all `MOTHERDUCK_ENV` values map to `Thyroid 2026`. Documented real mapping via `config/motherduck_environments.yml` and sandbox runbook.
2. **`docs/motherduck_canonical_upload_20260402.md`** — Historical note: YAML later gained separate dev/qa DBs; body unchanged as April 2 archive.
3. **`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`** — Clarified that the filename is the default 144 output path, not “fresh by definition”; linked publication signoff README; recorded HEAD vs embedded SHA mismatch at reconciliation time.
4. **`docs/specimen_fhir_contract_review.md`** — Documented that **126** runs the same specimen/FHIR gate as **124** before **`115 --final-master`** / **`118 --final-master`**.
5. **`docs/release_runbook.md`** — Same **126** vs Check 13 vs manuscript-only snapshot scope, cross-linked to specimen contract §Scope vs 115/118.
6. **`tests/test_126_final_master_release_contract.py`** — Ordering and CLI flags for **126** (no cloud).

## Validation

Run before commit (from repo root):

- `ruff check scripts app utils llm_extraction motherduck_client.py dashboard.py --select F`
- `mypy` (project config)
- `pytest tests/test_126_final_master_release_contract.py tests/test_motherduck_release_surface_invariants.py tests/test_release_final_master_surface.py`

## `.env.motherduck.example`

No edit required: it already documents distinct dev/qa/prod databases consistent with `config/motherduck_environments.yml`.

## Git record

- **Commits:** `6f08da6` (reconciliation patch), `ad687d7` (study summary git record); **tip** on `origin/main`: `ad687d7`.
