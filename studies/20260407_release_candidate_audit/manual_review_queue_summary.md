# Manual review queue summary (live snapshot)

From `scripts/126_release_candidate_motherduck_audit.py` / MotherDuck:

| run_label | Notes |
|-----------|--------|
| `mrq_hydrate_gate` | 5,622 rows; **0 pending** at audit time (`verification_status` populated for all). |

| algorithm_status | n |
|------------------|--:|
| `existing_missing_fill_candidate` | 5,620 |
| `discordant_existing` | 2 |

**Prior review policy:** Discordant rows first; tiered fill-candidate acceptance per `docs/domain_mapping_rules.md` and `MANUAL_REVIEW_PLAYBOOK.md`. **Automation:** `scripts/127_qa_tier_batch_adjudicate.py --md --md-sa --apply` (optional `--include-critical-after-sample` after sample sign-off).
