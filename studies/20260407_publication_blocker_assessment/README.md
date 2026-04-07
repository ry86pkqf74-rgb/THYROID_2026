# Publication blocker — human review governance

**Date:** 2026-04-07  
**Status:** Open

## Blocker

Manuscript sign-off requires **human-reviewed** `qa.manual_review_queue` adjudication, not NULL-padding or synthetic automation-only statuses.

## Live counts

- `qa.manual_review_queue`: **5,622** rows; **0** with NULL `verification_status`.
- **5,620** rows: `verification_status = SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF`
- **2** rows: `confirmed_correct`
- `qa.promotion_review_decisions`: **2** rows; `decision_batch_id` NULL

## Checked-in evidence

- [`studies/20260409_final_master_release/EVIDENCE_PACK.md`](../20260409_final_master_release/EVIDENCE_PACK.md) documents `--synthetic-fill-mrq-verification` — consistent with live MRQ.

## Additional gate

`scripts/119_md_formalization_validate.py --md --md-sa --release-mode` latest run **FAIL** on specimen/FHIR QA diagnostics — must PASS for a clean release-mode signoff.

## Next commands (human + automation)

1. Produce reviewed `manual_review_queue.csv` (all rows real statuses + reviewers) in a gate folder.  
2. Prepare `promotion_review_decisions.csv` with real batch metadata if required by org policy.  
3. Re-hydrate MRQ per `scripts/126_final_master_release.py` / `114` workflow **without** `--synthetic-fill-mrq-verification`.  

```bash
unset LOCAL_DB_PATH
export MD_SA_TOKEN='…'
export MOTHERDUCK_CUSTOM_USER_AGENT='THYROID_2026_publication_signoff/1.0'
export MOTHERDUCK_SESSION_HINT='publication_signoff_<UTC_YYYYMMDD_HHMM>'
.venv/bin/python scripts/126_final_master_release.py --md \
  --release-date YYYYMMDD \
  --hydrate-mrq-from <reviewed_gate_dir> \
  --decisions-csv <real_decisions.csv>
# Add --lab-csv / --ingestion-wave when final institutional labs are ready
```

4. Re-run `119 --md --md-sa --release-mode` until PASS (including specimen/FHIR checks).
