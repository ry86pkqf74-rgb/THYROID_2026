# Tier-policy batch — fill-candidate MRQ

**Batch id:** `20260407_tier_policy`
**Generated (UTC):** 2026-04-07T15:21:19Z

## Scope

- Rewrote **5,620** MRQ rows from `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` to registry tier `verification_status` values.
- **Algorithm gate:** only rows with `algorithm_comparison_status = existing_missing_fill_candidate` (enrichment / fill). **No** synthetic discordant rows were present in source.

## Counts by `qa_tier` (registry)

- **critical:** 1,646 row(s) → `auto_accepted_critical_sample_ok`
- **informational:** 893 row(s) → `auto_accepted_informational`
- **standard:** 3,081 row(s) → `auto_accepted_standard`

## Governance

- Authoritative policy: `docs/domain_mapping_rules.md` (Fill-Candidate Triage Policy).
- Critical-tier rows here are **fill-candidates only** (not value discordance). Organization batch acceptance is recorded with `auto_accepted_critical_sample_ok` and this memo as `evidence_ref` on `qa.promotion_review_decisions`.
