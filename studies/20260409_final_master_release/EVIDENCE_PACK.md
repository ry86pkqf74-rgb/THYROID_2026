> **HISTORICAL / SUPERSEDED:** This document is a point-in-time snapshot from its generation date. For current canonical state, see [`docs/final_source_of_truth_contract.md`](../../docs/final_source_of_truth_contract.md) and [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md). Row counts cited here may no longer match live MotherDuck.

# Final master release — manuscript readiness evidence

> **Status: superseded operator snapshot.** Prefer [`../20260411_final_master_release/EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md) for **current** MotherDuck row counts, `release_20260411`, and post–lab-wave posture. **Repo headline:** **Technically passing but blocked by synthetic MRQ** — see [`../../docs/REPO_STATUS.md`](../../docs/REPO_STATUS.md).

> **Evidence refresh (2026-04-07 UTC):** Prefer [`../20260411_final_master_release/EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md) for row counts and `release_20260411` after **`final_institutional_20260407`** lab ingest and non-synthetic MRQ validation (`119` **PASS**, WARN only on specimen-adjacent review burden). The table below is a **historical** capture.

> **Supersession pointer:** Live MotherDuck reconciliation (2026-04-07) supersedes static row-count claims here for **governance**: [`studies/20260407_publication_signoff_live/final_verdict_memo.md`](../20260407_publication_signoff_live/final_verdict_memo.md). The synthetic MRQ warning below applied to the **20260409** point-in-time capture; live MotherDuck MRQ may differ — see **20260411** pack.

- **Release tag:** `20260409`
- **Git SHA:** `b77b4be8a3a4f194d0e2556828073afdf7dda962`
- **Captured (UTC):** 2026-04-07T02:05:20.810771+00:00
- **Parquet bundle:** `/Users/ros/THyroid 2026/THYROID_2026/exports/final_master_release_20260409` (no raw note text in this profile)

## MRQ warning

**`--synthetic-fill-mrq-verification`** was used with status `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF`. This is **not** human manuscript sign-off. Replace with a truly reviewed CSV for publication.

## Row counts

| main.canonical_extracted_fact_long_v2 | 123577 |
| main.canonical_fact_quarantine_v2 | 199 |
| main.note_extraction_runs | 3 |
| main.longitudinal_lab_canonical_v1 | 76971 |
| main.longitudinal_lab_deduped_v | 55210 |
| main.master_fact_long_verified_v1 | 123577 |
| main.master_patient_rollup_verified_v1 | 5574 |
| main.master_source_lineage_v1 | 123577 |

## Lineage completeness (master facts)

```json
{
  "facts_with_source_object": 123577,
  "facts_total": 123577
}
```

## Review queue

```json
{
  "total": 5622,
  "pending_verification": 0
}
```

## Documented source-limited burdens

- Operative NLP enrichment (berry ligament, frozen section, EBL) may remain sparse by design.
- Recurrence dates: large unresolved fraction is documented as source-limited.
- RAI dose recovery ceiling unless nuclear medicine notes / structured feeds improve.
- Non-Tg lab panel (TSH, PTH, Ca, vit D) depends on institutional lab extract coverage.

## MotherDuck named snapshot

Create a cloud snapshot from the MotherDuck UI or your organization runbook after this release.
The immutable `release_<tag>` schema copy is created by `scripts/115_release_snapshot.py --final-master`.
