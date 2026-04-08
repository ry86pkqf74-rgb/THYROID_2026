# Final master release — evidence memo (2026-04-08 UTC)

## Token / attribution

- **RW token:** loaded from `.streamlit/secrets.toml` key `MOTHERDUCK_TOKEN` (mode `secrets.toml:MOTHERDUCK_TOKEN`; length not printed).
- **MD_SA_TOKEN:** not present; `--md-sa` omitted.
- **Read-scaling token:** not configured (`MD_READ_SCALING_TOKEN`); **136 reader** not run.
- **Attribution env (operator session):**
  - `MOTHERDUCK_CUSTOM_USER_AGENT=cursor_final_master_release_v1`
  - `MOTHERDUCK_SESSION_HINT=cursor_final_master_release_<UTCSTAMP>` (varied per step)

## Inputs

| Input | Path |
|--------|------|
| MRQ hydrate (tier policy, non-synthetic verification) | `studies/20260407_tier_policy_review_gate/` |
| `manual_review_queue.csv` | same |
| `promotion_review_decisions.csv` | `studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv` |
| Institutional lab wave | *(not used — no `--lab-csv` / `--ingestion-wave`)* |

## Commands (exact sequence)

```bash
cd THYROID_2026
export MOTHERDUCK_CUSTOM_USER_AGENT=cursor_final_master_release_v1
export MOTHERDUCK_SESSION_HINT=cursor_final_master_release_$(date -u +%Y%m%dT%H%M%SZ)
RELEASE_DATE=$(date -u +%Y%m%d)   # 20260408
STUDY_ROOT="studies/${RELEASE_DATE}_final_master_release"

mkdir -p "$STUDY_ROOT/triage" "$STUDY_ROOT/logs"
.venv/bin/python scripts/120_review_queue_triage.py --md --output-root "$STUDY_ROOT/triage"

.venv/bin/python scripts/126_final_master_release.py --md \
  --release-date "$RELEASE_DATE" --dry-run \
  --hydrate-mrq-from studies/20260407_tier_policy_review_gate \
  --decisions-csv studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv

.venv/bin/python -u scripts/126_final_master_release.py --md \
  --release-date "$RELEASE_DATE" \
  --hydrate-mrq-from studies/20260407_tier_policy_review_gate \
  --decisions-csv studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv
# → 115 failed: release_20260408 already existed (see blockers / completion below)

REL_TAG=20260408r2   # first completion snapshot after 126 chain (103/117/125 had succeeded)
.venv/bin/python scripts/115_release_snapshot.py --md --tag "$REL_TAG" --final-master \
  --created-by scripts/126_completion_after_collision
.venv/bin/python scripts/118_parquet_release_bundle.py --md --tag "$REL_TAG" --final-master

# Post–119 failures: MotherDuck fixes + local parity, then 125 + re-snapshot
# (one-off Python: UPDATE qa.promotion_review_decisions …; UPDATE canonical … extraction_run_id;
#  COPY note_extraction_runs → processed/note_extraction_runs.parquet)

.venv/bin/python scripts/125_master_verified_views.py --md

REL_TAG=20260408r3
.venv/bin/python scripts/115_release_snapshot.py --md --tag "$REL_TAG" --final-master \
  --created-by scripts/post_backfill_resnapshot
.venv/bin/python scripts/118_parquet_release_bundle.py --md --tag "$REL_TAG" --final-master

export MOTHERDUCK_ENV=prod
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env prod --release-mode \
  --output-dir "$STUDY_ROOT/post_release_validation"

.venv/bin/python scripts/144_md_repo_current_state_summary.py --md \
  --output "$STUDY_ROOT/CURRENT_MOTHERDUCK_REPO_STATE.md"

.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod
```

## Release tags / artifacts

| Artifact | Notes |
|----------|--------|
| **126 intended tag** | `20260408` → blocked at **115** because schema `release_20260408` already existed on prod. |
| **Adopted immutable snapshot** | **`release_20260408r3`** (+ parquet bundle `exports/final_master_release_20260408r3/`) after `extraction_run_id` backfill and `125` refresh. |
| **Prior attempt** | `release_20260408r2` + `exports/final_master_release_20260408r2/` — superseded by **r3** (main had further updates). |
| **Triage bundle** | `triage/review_queue_triage_20260408_044608/` |
| **Logs** | `logs/126_live_stdout.log`, `logs/115.log`, `logs_completion/*` |
| **119 (final)** | `post_release_validation/validation_report.md` — **36 PASS / 3 WARN / 0 FAIL** |

## Step outcomes

| Step | Result |
|------|--------|
| **120** triage | **OK** |
| **126** dry-run | **OK** (MRQ preflight after code fix) |
| **126** live | **Partial:** 114 / 103 / 117 / 125 / specimen gate **OK**; **115 FAIL** (duplicate `release_20260408`) |
| **115 / 118** completion | **OK** with `20260408r2`, then **r3** after backfills |
| **119** | **FAIL** then **PASS** after DB + local parity fixes (see below) |
| **136** writer | **OK** (`CREATE SNAPSHOT OF "Thyroid 2026"`) |
| **136** reader | **Skipped** (no read-scaling token) |
| **144** | **OK** → `CURRENT_MOTHERDUCK_REPO_STATE.md` |
| **Telemetry** | `MD_INFORMATION_SCHEMA.QUERY_HISTORY` / `RECENT_QUERIES` returned **0 rows** for `%cursor%` user_agent (plan/storage latency or attribution not surfaced); see `telemetry_query_history_cursor_release.txt` |

## Blockers remediated (live)

1. **`release_20260408` collision** — used new tags `20260408r2` / `20260408r3`.
2. **119 Check 5b** — one legacy `qa.promotion_review_decisions` row had NULL `decision_batch_id`; set to `20260407_tier_policy_legacy`.
3. **119 Check 2** — missing `processed/note_extraction_runs.parquet`; exported from MotherDuck (file gitignored).
4. **119 Check 10** — backfilled NULL `extraction_run_id` on `main.canonical_extracted_fact_long_v2` using latest successful `note_extraction_runs.run_id`, then re-ran **125** and **115/118** as **r3**.

## Residual / warnings (119)

- Molecular assay/panel_version and dictionary WARNs (expected per ThyroSeq / non-Afirma panels).
- Specimen-adjacent review burden WARNs (open genomic link rows).

## Rollback notes

- **Catalog-level:** MotherDuck automatic snapshots + pre-promote clone policy (`docs/release_runbook.md` §3.1).
- **Schema-scoped consumption:** Analysts may point queries at **`release_20260408r3`** (or prior `release_*` per `qa.release_manifest`) if `main` must be avoided.
- **126 partial run:** `main` was mutated by 103/117/125 before 115 failed; completion used additional snapshots **r2/r3**. Coordinate with ownership before dropping or overwriting schemas.

## Code / repo changes (this session)

- `scripts/126_final_master_release.py` — dry-run MRQ validation; defensive `decision_batch_id` UPDATE after CSV append.
- `.streamlit/secrets.toml.example` —-doc template for local tokens (never commit real `secrets.toml`).
