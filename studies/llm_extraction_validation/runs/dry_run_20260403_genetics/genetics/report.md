# LLM Extraction Validation Report

- Input parquet: `/Users/ros/THyroid 2026/THYROID_2026/processed/note_entities_genetics.parquet`
- Generated at: `2026-04-03T12:50:00.718432+00:00`
- Total LLM rows: `1,738`
- Unique patients: `605`
- Gold rows (`verification_status` concordant | existing_missing_fill_candidate, per policy): `1,600`

## Domain / algorithm status

| Domain | Algorithm status | Rows | Patients | Structured matches | Baseline matches | Fill candidates | Review conflicts |
|---|---:|---:|---:|---:|---:|---:|---:|
| genetics | concordant_existing_extraction_only | 1,668 | 580 | 0 | 1,668 | 0 | 0 |
| genetics | existing_missing_fill_candidate | 70 | 44 | 0 | 0 | 70 | 0 |
