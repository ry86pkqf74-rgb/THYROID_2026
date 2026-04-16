# Manual review queue triage bundle

Generated (UTC): `2026-04-13T17:03:21Z`

## Scope

- Source table: `qa.manual_review_queue` (read-only export).
- **No raw note text**: `review_reason` / note-adjacent snippets are **not** exported; 
  `entity_value_norm`, `reviewer_comment`, and `reviewer_evidence_span` are **truncated**.
- **Promotion semantics unchanged**: this script does not UPDATE MotherDuck or alter gate logic.

## Headline counts

- Total rows (after filter): **5,622**
- `verification_status` present (reviewed): **5,622**
- Pending (`verification_status` NULL) — **blocks strict `119 --release-mode`**: **0**

## Files

| File | Purpose |
|------|---------|
| `counts_by_domain.csv` | Rows / pending / reviewed by `domain` |
| `counts_by_verification_status.csv` | Histogram of `verification_status` |
| `counts_manuscript_quality_tiers.csv` | Pending vs synthetic vs automation vs human identity (sign-off posture) |
| `counts_mrq_three_bucket_signoff.csv` | **Governance:** `unresolved_pending` / `synthetic_automation_only` / `true_human_reviewed` / `automation_tier_or_incomplete_non_human` |
| `counts_promotable_blocking.csv` | Blocking vs cleared + pending algorithm breakdown |
| `domains_highest_pending_volume.csv` | Domains ranked by pending count |
| `oldest_pending_rows.csv` | Stale pending rows by `loaded_at` |
| `worklists/worklist__<domain>__tier_<tier>.csv` | Pending-only slices for reviewers |

## Run labels in this export

- `20260407_tier_policy_review_gate`: 5,622 rows

## Top domains by pending volume

- **airway_invasion**: pending 0 (total 2,045)
- **pathology**: pending 0 (total 683)
- **imaging**: pending 0 (total 601)
- **rai_detailed**: pending 0 (total 564)
- **vascular_invasion**: pending 0 (total 370)
- **functional_outcomes**: pending 0 (total 346)
- **survival_followup**: pending 0 (total 279)
- **physical_exam**: pending 0 (total 247)
- **past_surgical_hx**: pending 0 (total 231)
- **labs**: pending 0 (total 99)

## Manuscript sign-off quality tiers

Counts are **mutually exclusive** (first matching branch wins). `119 --release-mode` **fails** CHECK **5b** when tier **B** (synthetic placeholders) has **any** rows. When B is empty, automation tier **C** can still be **non-manuscript** from a governance lens (see `counts_mrq_three_bucket_signoff.csv`). Tier **D** is the conservative "human reviewer identity" bucket (non-empty `reviewer` + `reviewed_at`).

| Tier | n_rows |
|------|--------:|
| `C_automation_tier_policy_only` | 5,620 |
| `E_reviewed_status_without_reviewer_timestamp` | 2 |

## Three-bucket manuscript sign-off (governance)

| Bucket | n_rows |
|--------|--------:|
| `automation_tier_or_incomplete_non_human` | 5,622 |
