> **HISTORICAL / SUPERSEDED:** This document is a point-in-time snapshot from its generation date. For current canonical state, see [`docs/final_source_of_truth_contract.md`](../../docs/final_source_of_truth_contract.md) and [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md). Row counts cited here may no longer match live MotherDuck.

# Final master release — manuscript readiness evidence

> **Historical evidence pack (2026-04-07 capture):** Row counts and MRQ posture here are **point-in-time** and may **differ materially** from live MotherDuck today. **Live SSOT:** [`docs/final_source_of_truth_contract.md`](../../docs/final_source_of_truth_contract.md); current repo mirror: [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md) after `scripts/144_md_repo_current_state_summary.py --md`. Do not cite this table as live prod without reconciliation.

- **Release tag:** `20260411`
- **Git SHA:** `559da67d0afcc27dcff6fea0a2fbb196b161cce4` (commit introducing this evidence folder; row snapshot from run at 2026-04-07T19:16Z UTC)
- **Captured (UTC):** 2026-04-07T19:16:06.987268+00:00
- **Parquet bundle:** `/Users/ros/THyroid 2026/THYROID_2026/exports/final_master_release_20260411` (no raw note text in this profile)

## Row counts

| main.canonical_extracted_fact_long_v2 | 123577 |
| main.canonical_fact_quarantine_v2 | 199 |
| main.note_extraction_runs | 3 |
| main.longitudinal_lab_canonical_v1 | 77960 |
| main.longitudinal_lab_deduped_v | 56198 |
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
  "total": 11244,
  "pending_verification": 0
}
```

## Documented source-limited burdens

- Operative NLP enrichment (berry ligament, frozen section, EBL) may remain sparse by design.
- Recurrence dates: large unresolved fraction is documented as source-limited.
- RAI dose recovery ceiling unless nuclear medicine notes / structured feeds improve.
- Non-Tg lab panel (TSH, PTH, Ca, vit D): **`final_institutional_20260407`** wave is present in `main.longitudinal_lab_canonical_v1` / `longitudinal_lab_deduped_v`; residual gaps are source-limited / dedup-rank edge cases, not a missing-ingest caveat for this wave.

## MotherDuck named snapshot

Create a cloud snapshot from the MotherDuck UI or your organization runbook after this release.
The immutable `release_<tag>` schema copy is created by `scripts/115_release_snapshot.py --final-master`.
