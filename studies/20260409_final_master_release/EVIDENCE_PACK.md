# Final master release — manuscript readiness evidence

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
