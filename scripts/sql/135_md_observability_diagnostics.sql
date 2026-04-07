-- THYROID_2026 — MotherDuck observability reference queries
-- Run with: duckdb "md:<your_database>" -readonly < scripts/sql/135_md_observability_diagnostics.sql
-- Or: python scripts/135_md_molecular_observability.py diagnostics --md
--
-- Note: QUERY_HISTORY / RECENT_QUERIES are org-admin / Business-plan preview features
-- (see MotherDuck docs). DATABASES / DATABASE_SNAPSHOTS are widely available.

-- Current attachment context
SELECT current_database() AS database_name, current_schema() AS schema_name;

-- Catalogs you own (retention, type)
SELECT name, uuid, created_ts, transient, historical_snapshot_retention, type
FROM MD_INFORMATION_SCHEMA.DATABASES
ORDER BY name;

-- Snapshots for the attached database (time-travel / recovery lineage)
SELECT snapshot_id, snapshot_name, database_name, created_ts, active_bytes
FROM MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS
WHERE database_name = current_database()
ORDER BY created_ts DESC
LIMIT 25;

-- Recent query history sample (attribution via user_agent + session_name)
SELECT query_id, start_time, end_time, total_elapsed_time, error_type,
       user_agent, session_name, query_type,
       substring(query_text, 1, 200) AS query_preview
FROM MD_INFORMATION_SCHEMA.QUERY_HISTORY
ORDER BY start_time DESC
LIMIT 15;

-- Hot path: shorter-delay recent queries
SELECT query_id, start_time, end_time, error_type, user_agent, session_name, query_type,
       substring(query_text, 1, 200) AS query_preview
FROM MD_INFORMATION_SCHEMA.RECENT_QUERIES
ORDER BY start_time DESC
LIMIT 15;
