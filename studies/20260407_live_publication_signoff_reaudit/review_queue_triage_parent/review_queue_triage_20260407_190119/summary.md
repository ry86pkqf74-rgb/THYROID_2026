# Manual review queue triage bundle

Generated (UTC): `2026-04-07T19:01:23Z`

## Scope

- Source table: `qa.manual_review_queue` (read-only export).
- **No raw note text**: `review_reason` / note-adjacent snippets are **not** exported; 
  `entity_value_norm`, `reviewer_comment`, and `reviewer_evidence_span` are **truncated**.
- **Promotion semantics unchanged**: this script does not UPDATE MotherDuck or alter gate logic.

## Headline counts

- Total rows (after filter): **11,244**
- `verification_status` present (reviewed): **11,244**
- Pending (`verification_status` NULL) — **blocks strict `119 --release-mode`**: **0**

## Files

| File | Purpose |
|------|---------|
| `counts_by_domain.csv` | Rows / pending / reviewed by `domain` |
| `counts_by_verification_status.csv` | Histogram of `verification_status` |
| `counts_promotable_blocking.csv` | Blocking vs cleared + pending algorithm breakdown |
| `domains_highest_pending_volume.csv` | Domains ranked by pending count |
| `oldest_pending_rows.csv` | Stale pending rows by `loaded_at` |
| `worklists/worklist__<domain>__tier_<tier>.csv` | Pending-only slices for reviewers |

## Run labels in this export

- `20260407_tier_policy_review_gate`: 5,622 rows
- `promotion_gate`: 5,622 rows

## Top domains by pending volume

- **airway_invasion**: pending 0 (total 4,090)
- **pathology**: pending 0 (total 1,366)
- **imaging**: pending 0 (total 1,202)
- **rai_detailed**: pending 0 (total 1,128)
- **vascular_invasion**: pending 0 (total 740)
- **functional_outcomes**: pending 0 (total 692)
- **survival_followup**: pending 0 (total 558)
- **physical_exam**: pending 0 (total 494)
- **past_surgical_hx**: pending 0 (total 462)
- **labs**: pending 0 (total 198)
