# Final-release blocker — 2026-04-07

> **Update (same day):** Queue adjudication + release-mode re-validation completed. See **`MANUAL_QUEUE_RESOLUTION.md`**, **`validation_report_release_mode.md`**, and refreshed **`release_validation_strict.json`**. The narrative below documents the **original** first-attempt halt.

## Status (original attempt)

- **Dry-run** (`--md --dry-run --tag 20260406`): **PASS** (see `dry_run_console.log` and `audit_summary.md`).
- **Final-release** (`--md --final-release --tag 20260406`): **BLOCKED** after promotion gate.

## Failing command

```bash
cd THYROID_2026
# MOTHERDUCK_TOKEN must be set in the environment (e.g. from `.streamlit/secrets.toml` — do not commit tokens).
.venv/bin/python scripts/124_md_live_release_audit.py \
  --md --final-release --tag 20260406 \
  --output-dir studies/md_live_release_20260406
```

## Failure point

Post-step **Promotion gate (112)**, during orchestrator **pending manual review check**:

```
[review queue] total=11,244  pending=5,622
HALT: --final-release mode requires all review rows to be resolved; 5,622 pending row(s) remain. Resolve them before re-running with --final-release.
```

Exit code: **1**

Full console capture: `final_release_console.log`

## Steps completed before halt

| Step | Status |
|------|--------|
| Preflight | PASS |
| Stage refresh (116) | PASS |
| Promotion gate (112) | PASS (G1–G8) |
| Pending-review gate (`qa.manual_review_queue` where `verification_status IS NULL`) | **FAIL** |
| Canonical (103), QA (114), contracts (117), views (125), release (115), bundle (118), validation (119) | **Not run** |

## Not produced (blocked)

- Schema **`release_20260406`** — snapshot step (115) did not execute.
- **`qa.release_manifest`** row for tag `20260406` — not inserted.
- **`release_validation_strict.json`**, post-release manifest dumps — not produced by this final-release run.

## Manual review unblocker

Resolve the **5,622** pending rows in `qa.manual_review_queue` (`verification_status IS NULL`), then re-run the same final-release command.

## Related code fix (dry-run / local 103)

`clinical_notes_long.parquet` in this workspace has no `note_row_id` column; `103_fact_lineage_materialize.py` previously crashed with `KeyError: Index(['note_row_id'], ...)`. Guard added to skip clinical-note merge when `note_row_id` is absent (see git history for `scripts/103_fact_lineage_materialize.py`).
