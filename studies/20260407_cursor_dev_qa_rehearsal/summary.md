# Dev/QA MotherDuck rehearsal — 2026-04-07

Cursor agent run: Business-tier dev/QA rehearsal using repo MotherDuck env split. **Production canonical tables were not mutated** (prod used for read-only inspect, `QUERY_HISTORY` probe, and **136 dry-run** SQL only).

## Token modes (step 1)

Recorded in `token_modes.txt`:

- `rw_token_mode=secrets.toml:MOTHERDUCK_TOKEN`
- `read_scaling_token_mode=none` — **no** `MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` in this environment

## Query attribution (step 2)

Exports used for MotherDuck connections in this session:

- `MOTHERDUCK_CUSTOM_USER_AGENT=cursor-agent/1.0(dev_qa_rehearsal;thyroid_2026)`
- `MOTHERDUCK_SESSION_HINT=cursor_dev_qa_rehearsal`

## Commands that **mutated** state

| Step | Target | Mutating? | Evidence |
|------|--------|-----------|----------|
| `130_md_env_bootstrap.py --execute --date-tag 20260407 refresh-dev --latest` | **dev** only | **Yes** — `DROP DATABASE` + `CREATE DATABASE … FROM` prod | `studies/20260407_cursor_dev_refresh/130_refresh_dev_execute.log` |
| `116_md_stage_loader.py --md` with `MOTHERDUCK_ENV=dev` | **dev** `v2_stage` | **Yes** — transactional load + `load_inventory` | `116_stage_loader_dev.log` |
| `112_v2_domain_promotion_gate.py … --motherduck-check` with `MOTHERDUCK_ENV=dev` | **dev** | **No** — read-only parity + local artifact writes under `promotion_gate_dev/` | `112_promotion_gate_full.log` |
| `119_md_formalization_validate.py --md --md-env qa --release-mode` | **qa** | **Maybe** — validator may append QA governance rows (standard 119 behavior); treat as **qa-plane** work | `119_qa_release_mode.log`, `studies/20260407_cursor_qa_rehearsal_release_mode/` |
| `130 inspect`, `smoke_test_md_connection.py --md` | **prod** attach | **No** (read-only) | `smoke_test_md.txt`, `130_inspect.txt` |
| `136_md_read_scaling_snapshot_refresh.py … --dry-run` | n/a | **No** | `136_writer_dryrun.txt`, `136_reader_dryrun.txt` |
| `QUERY_HISTORY` sample queries | **prod** | **No** | `query_history_attribution_sample.txt` |

**Prod:** no `124 --final-release`, no `137 promote`, no `136` execute.

## Dev refresh (step 4)

- **Succeeded.** SQL applied:

  `DROP DATABASE IF EXISTS "Thyroid 2026 Molecular Dev 20260407";`  
  `CREATE DATABASE "Thyroid 2026 Molecular Dev 20260407" FROM "Thyroid 2026";`

- **Note:** Global flags (`--execute`, `--date-tag`) must appear **before** the `refresh-dev` subcommand (see `130_md_env_bootstrap.py` epilog).

## Stage load (step 5)

- **Succeeded** (`MOTHERDUCK_ENV=dev`). Log: `116_stage_loader_dev.log` — 30 tables loaded, `COMMIT` OK, `v2_stage.load_inventory` at 210 rows (historical loads + this run).

## Promotion gate (step 5)

- **Succeeded — verdict PASS**, all gates G1–G8 PASS including **G8 MotherDuck v2_stage parity** against **dev**. Artifacts: `promotion_gate_dev/`.

## QA release-mode validation (step 6)

- **Succeeded:** **22 PASS / 0 WARN / 0 FAIL** on **qa** catalog. Report: `studies/20260407_cursor_qa_rehearsal_release_mode/validation_report.md`.
- Checks 12–13 **skipped** (molecular contract / specimen-FHIR prerequisites not present on QA snapshot — reported as PASS with skip messaging in script output).

## Read-scaling rehearsal (step 7)

- **Not exercised end-to-end:** no read-scaling token configured (`read_scaling_token_mode=none`).
- **Dry-run SQL** (prod catalog name in script output):
  - **Writer:** `CREATE SNAPSHOT OF "Thyroid 2026"` — `136_writer_dryrun.txt`
  - **Reader:** `REFRESH DATABASE "Thyroid 2026"` — `136_reader_dryrun.txt`

## Query-history attribution (step 8)

- **`MD_INFORMATION_SCHEMA.QUERY_HISTORY` is readable** with the RW credential used here (row counts returned for full table and `RECENT_QUERIES`).
- **`session_name`** shows `cursor_dev_qa_rehearsal` on recent queries (session hint visible).
- **`user_agent`** column shows the default DuckDB Python client string (`duckdb/v1.4.4…`), not the `MOTHERDUCK_CUSTOM_USER_AGENT` substring — custom UA may be mapped elsewhere or not duplicated into `user_agent`; **session hint attribution is clearly visible**.
- **`duckling_id`:** present on rows (see `query_history_attribution_sample.txt`).

## Code fix from rehearsal

- `scripts/112_v2_domain_promotion_gate.py` — `check_md_parity()` now uses **`get_token(prefer_service_account=True)`** and **`MotherDuckClient.for_env(..., use_service_account=True)`** so G8 aligns with automation preference for `MD_SA_TOKEN` when configured (still works when only `MOTHERDUCK_TOKEN` / secrets personal key exists).

## Static checks run

- `ruff check … --select F` — pass  
- `mypy` — pass (project scope)  
- `pytest` — 33 passed (listed MotherDuck transaction/hardening tests)  
- `py_compile` on touched `112` script — OK  

## Next step

**Proceed to prod-only steps only after explicit operator sign-off** — current posture: dev refresh + stage load + gate + QA **119** are green; **read-scaling token absent** so dashboard reader rehearsal was not live; manuscript/human gates remain outside this automation. **Stop** here for prod mutations; **fix** only if you need read-scaling tokens or want custom UA reflected in `QUERY_HISTORY.user_agent` (vendor behavior); **proceed** to prod promotion path only via runbook (`124` / `137`) when approved.
