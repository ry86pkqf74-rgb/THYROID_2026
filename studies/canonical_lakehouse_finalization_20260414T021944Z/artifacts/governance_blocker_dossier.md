# Governance Blocker Dossier

**Generated:** 2026-04-14 (canonical lakehouse finalization)

## Executive summary

The technical SSOT is complete. The governance layer — named human reviewer sign-off on MRQ rows — has **not** been performed. This is a documented blocker for manuscript-grade validation claims.

## Evidence reviewed

### qa.manual_review_queue state

- **Total rows:** 5,622
- **Pending (NULL verification_status):** 0
- **Verification status breakdown:**
  - `auto_accepted_standard`: 3,081
  - `auto_accepted_critical_sample_ok`: 1,646
  - `auto_accepted_informational`: 893
  - `confirmed_correct`: 2
- **Run label:** All 5,622 from `20260407_tier_policy_review_gate` (single automation batch)
- **True human-reviewed rows:** 0 (the 2 `confirmed_correct` rows have no reviewer identity or timestamp indicating manual adjudication)

### qa.promotion_review_decisions state

- **Total decision rows:** 6
- **Decision batch IDs:** `legacy_rc_tier_20260407` (2), `20260407_tier_policy` (1), `20260407_tier_policy_legacy` (1), `20260408` (1), `20260411` (1)
- **Nature:** Batch-level policy decisions recording tier-policy automation, not per-fact clinician adjudication

### Repo search for human review inputs

- `studies/20260413_targeted_remediation_pack/human_review_packet.csv` — 2,027 machine-generated worklist rows with `proposed_value` and `why_human_review_is_needed` fields. This is a **worklist for review**, not completed review decisions. No `reviewed_by` or `decision` columns are populated.
- `studies/manuscript_human_review_release_20260413T170316Z/report.md` — Report confirming "No new human review inputs were imported."
- No other files with actual human reviewer decisions, timestamps, or identities were found.

## Conclusion

**No real human review inputs exist in the repository or in live MotherDuck.** All 5,622 MRQ rows are automation-tier. The 2 `confirmed_correct` rows lack reviewer identity metadata. The worklist CSV is an unpopulated template.

## Impact on manuscript claims

- **"Technically validated" claim:** ALLOWED — `119 --release-mode` can pass with 0 FAIL.
- **"Human-reviewed" or "manuscript-grade validated" claim:** NOT ALLOWED — no evidence of named reviewer sign-off exists.
- **Recommended manuscript language:** "Data were technically validated through automated structural, parity, and lineage checks. Clinical content review is pending per institutional governance requirements."

## Resolution path

To close this blocker:
1. Assign named reviewer(s) to the 5,622 MRQ rows (or a statistically representative sample)
2. Import review decisions via `scripts/126_final_master_release.py --decisions-csv <file>` or `scripts/114_qa_schema_setup.py --hydrate-from <dir>`
3. Re-run `scripts/119_md_formalization_validate.py --md --release-mode` to confirm governance checks pass
4. Update `MANUSCRIPT_DATA_START_HERE.md` answer to "Can we start manuscripts now?" to YES (unconditional)
