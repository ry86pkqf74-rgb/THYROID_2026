-- MotherDuck / DuckDB: paste into MotherDuck SQL editor after uploading Parquet or using a path DuckDB can read.
-- Do not commit MOTHERDUCK_TOKEN. Example attach:
--   ATTACH 'md:YOUR_DATABASE?motherduck_token=<SECRET>' AS md (TYPE DUCKDB);

ATTACH 'md:YOUR_DATABASE' AS md (TYPE DUCKDB);

CREATE OR REPLACE TABLE md.val_llm_concordance_summary AS
SELECT * FROM read_parquet('/Users/ros/THyroid 2026/THYROID_2026/studies/llm_extraction_validation/runs/dry_run_20260403_pathology/pathology/val_llm_concordance_summary.parquet');

-- SELECT * FROM md.val_llm_concordance_summary;

-- Optional (large):
-- CREATE OR REPLACE TABLE md.llm_side_by_side AS SELECT * FROM read_parquet('/Users/ros/THyroid 2026/THYROID_2026/studies/llm_extraction_validation/runs/dry_run_20260403_pathology/pathology/llm_side_by_side.parquet');
-- CREATE OR REPLACE TABLE md.gold_llm_verified_facts AS SELECT * FROM read_parquet('/Users/ros/THyroid 2026/THYROID_2026/studies/llm_extraction_validation/runs/dry_run_20260403_pathology/pathology/gold_llm_verified_facts.parquet');

