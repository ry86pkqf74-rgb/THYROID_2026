# Artifact Salvage 2026-04-02

Ran at: 2026-04-02T06:22:13.201847+00:00
MotherDuck verification session database: my_db
Verification mode: plain_md_session_read_parquet

## Labs

- Fixed parquet: /Users/loganglosser/THYROID_2026/processed/output/v2_parquets/note_entities_llm_labs.parquet
- Backup parquet: /tmp/note_entities_llm_labs_pre_dedup_20260402.parquet
- Local rows: 11037
- Local unique note_row_id: 11037
- MotherDuck rows: 11037
- MotherDuck unique note_row_id: 11037
- MotherDuck empty result_json: 0

## Complications

- Materialized parquet: /Users/loganglosser/THYROID_2026/processed/output/v2_parquets/note_entities_llm_complications.parquet
- Local rows: 11037
- Local unique note_row_id: 11037
- MotherDuck rows: 11037
- MotherDuck unique note_row_id: 11037
- MotherDuck empty result_json: 0

## Applied Fixes

- Labs: deduplicated 21 duplicate note_row_id collisions by preferring richer non-empty result_json, then latest extracted_at.
- Complications: materialized the complete checkpoint into final parquet without rerunning extraction.

