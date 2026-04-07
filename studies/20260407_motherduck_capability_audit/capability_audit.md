# MotherDuck capability audit — THYROID_2026

**Date:** 2026-04-07  
**Scope:** Read-only / dry-run checks only. No production mutations. No token values logged.

## Token sources (labels only)

| Function | Result |
|----------|--------|
| `token_mode()` | `secrets.toml:MOTHERDUCK_TOKEN` |
| `read_scaling_token_mode()` | `none` |
| `resolve_database_for_env("dev")` | `Thyroid 2026 Molecular Dev 20260407` |
| `resolve_database_for_env("qa")` | `Thyroid 2026 Molecular QA 20260407` |
| `resolve_database_for_env("prod")` | `Thyroid 2026` |

## Capability matrix (evidence-based)

| Question | Answer | Evidence |
|----------|--------|----------|
| RW token available? | **Yes** | `token_mode()` ≠ `none`; smoke + 130 + 116/112/124 connect |
| Read-scaling token available? | **No** | `read_scaling_token_mode()` = `none`; `connect_read_scaling()` raised `RuntimeError` |
| `connect_rw` / fail-closed MotherDuck attach validated? | **Yes** | `scripts/smoke_test_md_connection.py --md` → **EXIT 0**; “MotherDuck connection verified (fail-closed gate passed)”; `current_database()` = `Thyroid 2026` |
| `connect_read_scaling` validated? | **No** | No read-scaling secret configured |
| Writer/reader freshness helper (`136`) path available? | **Dry-run SQL only** | `reader`/`writer` `--dry-run` **EXIT 0** (prints `REFRESH DATABASE` / `CREATE SNAPSHOT`); live reader path not exercised |
| Likely read-scaling (scaled reader) capability present? | **Unknown / not configured** | No read-scaling token; no live reader attach |

## Catalog / storage (from `130_md_env_bootstrap.py inspect`, EXIT 0)

- **Prod** `Thyroid 2026`: **`DUCKLAKE`** (not `DEFAULT`).
- **Dev / QA / PrePromote** clones: **`DEFAULT`** in the `DATABASES` listing returned during this session.
- Thyroid-related database names visible: `Thyroid 2026`, `Thyroid 2026 Molecular Dev 20260407`, `Thyroid 2026 Molecular QA 20260407`, `Thyroid 2026 Molecular PrePromote agent_20260407_workflow`.
- Recent rows from `MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS` for prod were listed (unnamed snapshots); interpret per DuckLake policy (see contract §8).

## Makefile dry-run trio

| Target | Result |
|--------|--------|
| `make md-smoke` | **FAIL** — **EXIT 1** — `check_md_rw_token`: no `MOTHERDUCK_TOKEN`/`MD_SA_TOKEN` in **environment** (secrets.toml not consulted by Make) |
| `make md-v2-gate-md-dryrun` | Not run (same guard) |
| `make md-live-release-dryrun` | Not run (same guard) |

**Equivalent Python path (same repo intent):**

| Step | EXIT |
|------|------|
| `116_md_stage_loader.py --md --dry-run` | 0 |
| `112_v2_domain_promotion_gate.py … --motherduck-check` (label `make_md_formalization_dryrun_audit`) | 0 — gate **PASS** |
| `119_md_formalization_validate.py --md` | 0 — **22 PASS / 2 WARN / 1 FAIL** (`note_extraction_runs` local vs md parity) |
| `124_md_live_release_audit.py --md --dry-run --tag 20260407` | **1** — `release_20260407` already exists (115 dry-run) |
| `124_md_live_release_audit.py --md --dry-run --tag 20991231` | Completed end-to-end **PASS** (verdict line in log) |

## Fail-closed smoke behavior

`--md` smoke uses `connect_md_fail_closed` → `PRAGMA database_list` must show MotherDuck attachment (`md:` or `md_information_schema` per `utils/md_connect.py`). Session output confirms verification passed and catalog matches prod database name.

## Credential separation (step 6)

**N/A:** no read-scaling token present — see `fail_closed_separation_test.txt`.

## Recommendation (dev/qa dry-run & release rehearsals)

- **Safe to run dev/qa-oriented dry-runs and MotherDuck read-only validation** with the current **RW** credential resolution path (as exercised by Python scripts).
- **Export RW tokens to the environment** (or use `.env.motherduck` with exported vars) if you want **`make md-*`** targets to work without changing the Makefile.
- **124 dry-run:** use an unused `MD_RELEASE_TAG` when today’s `release_YYYYMMDD` already exists on the catalog.
- **Before treating validations as green:** resolve the **`note_extraction_runs`** canonical parity FAIL in `119` (local 5 vs md 3) if that check is gate-critical for your process.
- **Read-scaling:** add a read-scaling token and re-audit if you rely on `connect_read_scaling()` or `136 reader` in production.

Artifacts: `commands_run.md`, `token_source_modes.txt`, `md_inspect_output.txt`, `md_smoke_output.txt`, `read_scaling_validation.txt`, `fail_closed_separation_test.txt`, `repo_recommendations.md`.
