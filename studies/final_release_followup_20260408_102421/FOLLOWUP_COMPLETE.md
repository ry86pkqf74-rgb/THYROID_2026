# MotherDuck follow-up — executed 2026-04-08 ~10:24–10:26 UTC

**Token:** Read/write MotherDuck via repo-root **`motherduck.local.toml`** (gitignored) per `motherduck_client.get_token()` — same path as prior audit [`../final_release_inputs_20260408_075437/`](../final_release_inputs_20260408_075437/).

## Commands run (all exit 0)

| Step | Command | Result |
|------|---------|--------|
| 1 | `scripts/120_review_queue_triage.py --md --output-root studies/final_release_followup_20260408_102421` | Bundle `review_queue_triage_20260408_102433/` — **5,622** rows, **0** pending |
| 2 | `scripts/119_md_formalization_validate.py --md --release-mode --output-dir .../119_release_validation` | **36 PASS / 3 WARN / 0 FAIL** — report `119_release_validation/validation_report.md` |
| 3 | `scripts/126_final_master_release.py --md ... --dry-run` (tier-policy gate + decisions + lab paths) | Preflight OK; MRQ CSV passes publication synthetic check |
| 4 | `scripts/127_analyst_institutional_lab_append.py --dry-run` (incoming chemistry CSV) | **989** rows prepared |

## Live governance deltas vs prior audit snapshot

- **`119` Check 5b — promotion decisions:** **5** row(s) with non-empty `decision_batch_id` (prior memo cited **2**; cloud state has evolved).
- **Manuscript quality tiers** (unchanged pattern): **5,620** `C_automation_tier_policy_only`, **2** `E_reviewed_status_without_reviewer_timestamp` — see `review_queue_triage_20260408_102433/counts_manuscript_quality_tiers.csv`.

## WARN-only items (`119`)

- Molecular `panel_version` / assay dictionary pairing (non-blocking per validator).
- Specimen-adjacent **review burden** (genomic_link_review / specimen_merge_review open counts) — structural PASS on QA diagnostics.

## Not executed (by design)

- **`126` live** (mutates MotherDuck) — not requested; dry-run only.
- **`127` live append** — wave already on catalog per project docs; dry-run validates CSV contract only.

## Artifacts in this folder

- `120_console.log`, `119_console.log`, `126_dryrun_console.log`, `127_lab_dryrun_console.log`
- `119_release_validation/validation_report.md`
- `review_queue_triage_20260408_102433/*` (counts, summary, worklists)
