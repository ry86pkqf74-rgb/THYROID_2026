# MotherDuck sandbox rehearsal — summary

**UTC run folder:** `studies/20260408T040740Z_cursor_md_sandbox_rehearsal/`  
**Date:** 2026-04-08 (Cursor agent)

## Token modes (no secrets)

| Diagnostic | Value |
|------------|--------|
| `token_mode()` | `secrets.toml:MOTHERDUCK_TOKEN` |
| `read_scaling_token_mode()` | `none` |
| RW token length | 467 |
| Read-scaling token length | 0 |

`MD_SA_TOKEN` was not in use for this session; RW token resolved from `.streamlit/secrets.toml` per repo contract. Prefer setting `MD_SA_TOKEN` in secrets or env for automation alignment with runbooks.

## Catalogs (`config/motherduck_environments.yml`)

| Env | Database name |
|-----|-----------------|
| **dev** | `Thyroid 2026 Molecular Dev 20260407` |
| **qa** | `Thyroid 2026 Molecular QA 20260407` |
| **prod** | `Thyroid 2026` (DUCKLAKE) |

## Sandboxes: reused or created?

**Reused.** `130 inspect` and `validate` showed existing zero-copy dev/qa catalogs from 20260407 with healthy `main` table counts (dev: 140, qa: 91). No new `clone --execute` was required; YAML was already aligned.

## Step results

### 1) `116_md_stage_loader.py --md` (dev)

- **Result:** Success (exit 0).  
- **Artifacts:** `dev_stage_loader/116_md_stage_loader.log`  
- **Context:** `MOTHERDUCK_ENV=dev`, `MOTHERDUCK_CUSTOM_USER_AGENT=cursor_dev_stage_rehearsal_v1`, `MOTHERDUCK_SESSION_HINT=cursor_dev_stage_20260408T040740Z`  
- 30 v2 domain tables loaded to `v2_stage`; row parity 100% with local parquets; `load_inventory` 240 rows.

### 2) `112_v2_domain_promotion_gate.py` (`--motherduck-check`, dev)

- **Result:** Logical **PASS** (all gates G1–G8 PASS).  
- **Note:** Host shell reported **exit 139** after the script printed the final banner — likely DuckDB/Python teardown on this machine; artifacts are complete.  
- **Artifacts:** `dev_gate/` (`promotion_recommendation.md`, `motherduck_promote.sql`, `promotion_scorecard.csv`, `manifest.json`, etc.)  
- **Run label:** `cursor_dev_gate_20260408T040740Z`

### 3) `119_md_formalization_validate.py --md --md-env qa --release-mode`

- **Result:** **FAIL** (exit 1) — expected for rehearsal until publication governance + local parity are fixed.  
- **Artifacts:** `qa_release_mode/validation_report.md`, `qa_release_mode/119_console.log`  
- **Summary:** 19 PASS / 0 WARN / 6 FAIL  

**Failure themes (prod-safe blockers for a real release):**

1. **Check 2 — Local vs MD canonical row parity:** `canonical_extracted_fact_long_v2`, `canonical_fact_quarantine_v2`, `note_extraction_runs` report `local=-1` vs MD counts (runner could not see those tables in local `thyroid_master.duckdb` / parity path — treat as operator workstation alignment, not prod mutation).  
2. **Check 5b — Publication governance:** ~5,620 MRQ rows still carry synthetic `verification_status`; `promotion_review_decisions` rows missing `decision_batch_id`. Requires human/128 tier policy + 126 batch provenance before release-mode can pass.  
3. **Check 12b — Molecular spine:** `main.molecular_testing` missing on QA catalog while `molecular_test_episode_v2` is populated — load spine + rerun script 22 / 49 / 140 on the target catalog per validation text.

### 4) `137_md_molecular_release_workflow.py promote` (no `--execute`)

- **Result:** Partial — see `workflow_manifest_partial.json` (canonical step list for this rehearsal) and `workflow_rehearsal/137_promote_rehearsal.log`.  
- **Completed in log:** Printed DuckLake-safe prepromote SQL; named snapshot skipped (DUCKLAKE); 136 writer dry-run path; **119 QA** ran (failed as above).  
- **124 prod audit (`--final-release --dry-run`):** Started but **not allowed to finish** in this session (15+ minute wall time, read-heavy even in dry-run). **No prod mutations** were performed; processes were stopped to complete documentation.  
- **136 reader refresh:** Not reached.

### 5) Telemetry (`QUERY_HISTORY` / `RECENT_QUERIES`)

- **Evidence:** `telemetry/diagnostics_dev.log`, `telemetry/recent_molecular_dev.log`  
- Queries attributable to this rehearsal show `session_name` / user-agent prefix **`cursor_dev_stage_20260408T040740Z`** and v2_stage `COUNT(*)` parity checks from gate G8.

## Code fix applied in-repo

- **`scripts/137_md_molecular_release_workflow.py`:** When `promote` is run with `--output-dir`, nested **119** output now defaults to `<output-dir>/qa_release_mode` so QA validation artifacts stay inside the rehearsal folder (avoids dropping only under `studies/<tag>_molecular_qa_release_mode/`).

## Exact next steps toward a real release

1. **QA / governance:** Clear 119 Check 5b (non-synthetic MRQ statuses + `decision_batch_id` on promotion decisions).  
2. **Canonical parity:** Ensure local runner has the same canonical tables as MD for Check 2, or run 119 from a defined CI image with local `thyroid_master.duckdb` / parquet sync.  
3. **Molecular spine on QA (then prod):** Materialize `molecular_testing` and rerun episodes + linkage (script **22**, **49**, **140**) on the promotion target.  
4. **Full `137 promote --execute`:** Only after 119 `--release-mode` is green on **qa**, with explicit ops approval; keep using Duck Lake–safe clone + prepromote backup pattern (no named prod snapshot).  
5. **`124` timing:** Plan a long window or CI job for `--final-release` dry-run / live audit; optionally use `prod-audit --relaxed` only for faster smoke (not equivalent to final release).

## Related: full 124 dry-run (prod rehearsal, 2026-04-08)

A separate run produced **119 PASS** on the full release-audit pipeline (dry-run). High-signal exports retained here (large duplicate gate CSVs/parquets from that run were **not** kept in git):

- `motherduck_live_release_audit_dryrun_20260408_summary.md` — step table + verdict  
- `live_release_audit_119_validation_report_20260408.md` — 119 report from that run (contrasts with QA **release-mode** failures in `qa_release_mode/`)

## References

- `docs/motherduck_sandbox_clone_runbook.md`  
- `docs/release_runbook.md`  
- `docs/motherduck_database_contract_v1.md`
