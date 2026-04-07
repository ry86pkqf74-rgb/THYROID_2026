# Release verdict — `20260407_tier`

**Verdict:** Release-mode validation **PASS** (26 pass, 1 warn, 0 fail). MRQ no longer uses `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF`.

## Governance

- **MRQ:** Fill-candidate rows only (`existing_missing_fill_candidate`). Tier mapping applied via `scripts/128_mrq_tier_policy_gate_build.py` into gate folder `studies/20260407_tier_policy_review_gate/`. See `TIER_POLICY_BATCH_MEMO.md` there.
- **Promotion decisions:** Append-only history; `decision_batch_id` and `evidence_ref` backfilled on MotherDuck where prior inserts did not persist trailing columns (see `scripts/126_final_master_release.py` `append_promotion_decisions` fix).
- **Snapshot tag:** `20260407_tier` (new schema; `release_20260407` already existed from an earlier run).

## Caveats

- **119 WARN:** Specimen-adjacent review burden (genomic linkage / merge queues) — documented in validation report; not a release-mode failure.
- **Policy:** Critical-tier bulk used `auto_accepted_critical_sample_ok` for **fill-candidates only** (no discordant rows in this batch). Organization risk acceptance is recorded in the tier-policy memo.

## Artifacts

- Evidence: `EVIDENCE_PACK.md`, `evidence_pack.json`, `validation_run/validation_report.md`
- Parquet: `exports/final_master_release_20260407_tier/`
- MotherDuck snapshot schema: `release_20260407_tier`
