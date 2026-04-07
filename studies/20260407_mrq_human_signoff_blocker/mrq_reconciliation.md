# MRQ reconciliation — repo gate vs live MotherDuck

## Repo candidate gate (`20260407_tier_policy_review_gate`)

| Metric | Value |
|--------|-------|
| Rows | 5,622 |
| `run_label` (CSV) | `20260407_tier_policy_review_gate` |
| Pending null `verification_status` | 0 |
| `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` | 0 |
| `auto_accepted_*` | 5,620 |
| `discordant_existing` | 2 → `confirmed_correct` |

## Live `qa.manual_review_queue` (Thyroid 2026, read after connect)

| Metric | Value |
|--------|-------|
| Total rows | **11,244** |
| `verification_status IS NULL` | 0 |
| `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` | 0 |
| `auto_accepted_*` | 11,240 |

### Duplicated logical cohort (two `run_label`s)

| `run_label` | Rows | `auto_accepted_*` | `algorithm_status = discordant_existing` |
|-------------|------|-------------------|-------------------------------------------|
| `promotion_gate` | 5,622 | 5,620 | 2 |
| `20260407_tier_policy_review_gate` | 5,622 | 5,620 | 2 |

**Interpretation:** The same 5,622-row cohort appears **twice** under different `run_label` values. Any claim of “single hydrated MRQ truth” is **false** until operators delete/replace to a **single** coherent snapshot (final release path in `126` intends `DELETE FROM qa.manual_review_queue` then one hydrate — verify post-run).

## `qa.promotion_review_decisions`

Live history is **non-empty** and **append-only by convention** (3 rows / 2 batches in this probe). It does **not** contain row-level critical-tier sample documentation required by `docs/domain_mapping_rules.md` (critical-tier spot-check rule).

## Alignment with publication memos

- `studies/20260407_publication_signoff_live/final_verdict_memo.md` — manuscript blocked on governance (synthetic / lab wave). This run **confirms** automation-heavy MRQ is **not** interchangeable with manuscript human sign-off without the documented critical sample.
- `studies/20260409_final_master_release/EVIDENCE_PACK.md` — still cites synthetic MRQ fill for that cut; **not** updated in Branch A.
