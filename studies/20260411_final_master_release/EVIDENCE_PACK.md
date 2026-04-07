# Final master release — manuscript readiness evidence

- **Release tag:** `20260411`
- **Git SHA:** `de13c3365341c5025fc9fbbfd38df163475f4c60`
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
