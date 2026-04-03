# LLM Extraction Validation Report

- Input parquet: `/Users/ros/THyroid 2026/THYROID_2026/processed/note_entities_llm_pathology.parquet`
- Generated at: `2026-04-03T12:50:26.797666+00:00`
- Total LLM rows: `10,894`
- Unique patients: `2,220`
- Gold rows (`verification_status` concordant | existing_missing_fill_candidate, per policy): `644`

## Domain / algorithm status

| Domain | Algorithm status | Rows | Patients | Structured matches | Baseline matches | Fill candidates | Review conflicts |
|---|---:|---:|---:|---:|---:|---:|---:|
| complications | concordant_existing_extraction_only | 2 | 2 | 0 | 2 | 0 | 0 |
| genetics | concordant_existing_extraction_only | 464 | 351 | 0 | 464 | 0 | 0 |
| genetics | existing_missing_fill_candidate | 58 | 53 | 0 | 0 | 58 | 0 |
| medications | concordant_existing_extraction_only | 5 | 5 | 0 | 5 | 0 | 0 |
| medications | existing_missing_fill_candidate | 1 | 1 | 0 | 0 | 1 | 0 |
| operative_detail | concordant_existing_extraction_only | 6 | 6 | 0 | 6 | 0 | 0 |
| operative_detail | existing_missing_fill_candidate | 32 | 30 | 0 | 0 | 32 | 0 |
| problem_list | concordant_existing_extraction_only | 3 | 3 | 0 | 3 | 0 | 0 |
| problem_list | source_limited | 9 | 8 | 0 | 0 | 0 | 0 |
| procedures | concordant_existing_extraction_only | 168 | 140 | 0 | 168 | 0 | 0 |
| procedures | existing_missing_fill_candidate | 92 | 82 | 0 | 0 | 92 | 0 |
| staging | concordant_existing_extraction_only | 32 | 30 | 0 | 32 | 0 | 0 |
| staging | existing_missing_fill_candidate | 500 | 423 | 0 | 0 | 500 | 0 |
| unmapped | source_limited | 9,522 | 2,208 | 0 | 0 | 0 | 0 |
