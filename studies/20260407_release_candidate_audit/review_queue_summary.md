# Manual review queue summary (live MotherDuck)

**Table:** `qa.manual_review_queue`

## Totals (release gate posture)

| Metric | Value |
|--------|------:|
| Total rows | 16,866 |
| Rows with non-null `verification_status` (reviewed) | 16,866 |
| Rows with null `verification_status` (pending) | 0 |

Release-mode formalization (`scripts/119_md_formalization_validate.py --md --release-mode`) **PASS** on review-queue gate at 2026-04-07T04:37Z UTC.

## By `algorithm_status` (live)

| algorithm_status | n |
|------------------|------:|
| existing_missing_fill_candidate | 16,860 |
| discordant_existing | 6 |

## By `run_label` / `domain`

Detailed breakouts are captured in `snapshot_metadata.md` (section *manual_review_queue by run_label and domain*). No pending rows in any `(run_label, domain)` slice at audit time.

**PHI:** This summary intentionally excludes free-text reviewer comment fields; no raw note text is exported.
