-- Live catalog facts (run against MotherDuck; catalog = Thyroid 2026 only).
-- Session: set MOTHERDUCK_CUSTOM_USER_AGENT + MOTHERDUCK_SESSION_HINT per operator policy.

SELECT current_database() AS current_database;

SELECT name, type, transient, CAST(historical_snapshot_retention AS VARCHAR) AS historical_snapshot_retention
FROM md_information_schema.databases
WHERE name = current_database();

SELECT release_tag, created_at, created_by, tables_included, registry_version
FROM qa.release_manifest
ORDER BY TRY_CAST(release_tag AS BIGINT) DESC NULLS LAST, created_at DESC
LIMIT 20;

SELECT table_schema, COUNT(*) AS n_objects
FROM information_schema.tables
WHERE table_catalog = current_database()
  AND table_schema IN ('main', 'qa', 'v2_stage')
   OR table_schema LIKE 'release_%'
GROUP BY 1
ORDER BY 1;
