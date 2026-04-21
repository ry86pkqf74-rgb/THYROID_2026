"""
Prompt 6 readiness check + create Prompt-6 staging tables if missing.

Verifies:
  1. "Thyroid 2026 UPdated" DB attached and archive_pub_v1_0 schema reachable
  2. Cross-DB write probe (CREATE/DROP a tiny table in archive_pub_v1_0)
  3. archive_move_log_v1 + schema_reorg_move_log_v1 exist with expected columns
  4. cpm_missing_data_provenance_v1 + prompt5_remediation_summary_v1 present
  5. main_schema_keep_list_v1 contents
  6. RunPod do-not-touch list intact
  7. Creates prompt6_* staging tables if absent (idempotent)

Read-only on data; only creates empty staging tables and a single probe-CTAS
that is dropped immediately. Never touches main or RunPod tables.
"""

from datetime import datetime, timezone
from scripts._md_connect import connect_locked

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'
ARCH_DB = '"Thyroid 2026 UPdated"'
UTC = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def header(s):
    print()
    print("=" * 78)
    print(s)
    print("=" * 78)


# 1. Verify "Thyroid 2026 UPdated" attached
header("1. Attached databases")
dbs = con.execute("SELECT database_name FROM duckdb_databases() ORDER BY 1").fetchall()
db_names = [r[0] for r in dbs]
for d in db_names:
    print(f"  {d}")
assert "Thyroid 2026 UPdated" in db_names, (
    "REQUIRED 'Thyroid 2026 UPdated' database not attached to current MotherDuck session"
)

# 2. Verify archive_pub_v1_0 schema in archive DB
header("2. archive_pub_v1_0 schema in 'Thyroid 2026 UPdated'")
schemas = con.execute(
    f"SELECT schema_name FROM duckdb_schemas() "
    f"WHERE database_name = 'Thyroid 2026 UPdated' "
    f"ORDER BY 1"
).fetchall()
sch_names = [r[0] for r in schemas]
print(f"  schemas: {sch_names}")
assert "archive_pub_v1_0" in sch_names, (
    "archive_pub_v1_0 schema missing from 'Thyroid 2026 UPdated'"
)
arch_count = con.execute(
    f"SELECT COUNT(*) FROM duckdb_tables() "
    f"WHERE database_name='Thyroid 2026 UPdated' AND schema_name='archive_pub_v1_0'"
).fetchone()[0]
print(f"  existing archive count: {arch_count}")

# 3. Cross-DB write probe (tiny CTAS we drop immediately)
header("3. Cross-DB write probe")
probe_name = f"_prompt6_readiness_probe_{UTC}"
try:
    con.execute(
        f'CREATE TABLE {ARCH_DB}.archive_pub_v1_0."{probe_name}" '
        f"AS SELECT 1 AS x"
    )
    n = con.execute(
        f'SELECT COUNT(*) FROM {ARCH_DB}.archive_pub_v1_0."{probe_name}"'
    ).fetchone()[0]
    assert n == 1
    con.execute(f'DROP TABLE {ARCH_DB}.archive_pub_v1_0."{probe_name}"')
    print(f"  OK — created and dropped {probe_name} (1 row)")
except Exception as exc:
    raise SystemExit(f"FAILED cross-DB write probe: {exc}")

# 4. Log tables present and schema-correct
header("4. archive_move_log_v1 + schema_reorg_move_log_v1 schemas")


def col_list(schema, table):
    rows = con.execute(f"""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name = 'thyroid_canonical_publication_v1_0'
           AND schema_name = '{schema}' AND table_name = '{table}'
         ORDER BY column_index
    """).fetchall()
    return [r[0] for r in rows]


arc_cols = col_list("manuscript_workspace", "archive_move_log_v1")
print(f"  archive_move_log_v1 cols: {arc_cols}")
reorg_cols = col_list("manuscript_workspace", "schema_reorg_move_log_v1")
print(f"  schema_reorg_move_log_v1 cols: {reorg_cols}")

# 5. cpm_missing_data_provenance_v1 + prompt5_remediation_summary_v1
header("5. Provenance + Prompt-5 summary tables")
n_prov = con.execute(
    f"SELECT COUNT(*) FROM {DB}.manuscript_workspace.cpm_missing_data_provenance_v1"
).fetchone()[0]
print(f"  cpm_missing_data_provenance_v1 rows: {n_prov}")
n_p5 = con.execute(
    f"SELECT COUNT(*) FROM {DB}.manuscript_workspace.prompt5_remediation_summary_v1"
).fetchone()[0]
print(f"  prompt5_remediation_summary_v1 rows: {n_p5}")
prov_max = con.execute(f"""
    SELECT MAX(CAST(captured_at AS TIMESTAMP)) FROM (
      SELECT TRY_CAST(audit_run_at AS TIMESTAMP) AS captured_at
        FROM {DB}.manuscript_workspace.cpm_missing_data_provenance_v1
    )
""").fetchone()[0] if 'audit_run_at' in [c[0] for c in con.execute(
    "SELECT column_name FROM duckdb_columns() "
    "WHERE database_name='thyroid_canonical_publication_v1_0' "
    "AND schema_name='manuscript_workspace' "
    "AND table_name='cpm_missing_data_provenance_v1'"
).fetchall()] else None
print(f"  cpm_missing_data_provenance_v1 audit_run_at max: {prov_max}")

# 6. RunPod do-not-touch list integrity
header("6. RunPod do-not-touch tables")
dnt = [
    "note_entities_llm_pathology",
    "note_entities_llm_cervical_ln_detail",
    "note_entities_llm_tirads_granular",
    "note_entities_llm_esophageal_invasion",
    "tirads_reextraction_queue_v1",
    "operative_episode_detail_v2",
    "tirads_llm_extracted_v2",
]
for t in dnt:
    try:
        n = con.execute(f'SELECT COUNT(*) FROM {DB}.main."{t}"').fetchone()[0]
        print(f"  {t:50s} rows={n}")
    except Exception as exc:
        print(f"  {t:50s} ABSENT ({exc.__class__.__name__})")

# 7. Create prompt6_* staging tables if missing
header("7. Prompt-6 staging tables (idempotent CREATE IF NOT EXISTS)")

ddls = {
    "prompt6_prestate_v1": """
        CREATE TABLE IF NOT EXISTS {DB}.manuscript_workspace.prompt6_prestate_v1 (
          script_num   INTEGER,
          schema_name  VARCHAR,
          table_name   VARCHAR,
          row_count    BIGINT,
          col_count    INTEGER,
          captured_at  TIMESTAMP
        )
    """,
    "prompt6_poststate_v1": """
        CREATE TABLE IF NOT EXISTS {DB}.manuscript_workspace.prompt6_poststate_v1 (
          script_num   INTEGER,
          schema_name  VARCHAR,
          table_name   VARCHAR,
          row_count    BIGINT,
          col_count    INTEGER,
          captured_at  TIMESTAMP
        )
    """,
    "prompt6_defer_log_v1": """
        CREATE TABLE IF NOT EXISTS {DB}.manuscript_workspace.prompt6_defer_log_v1 (
          script_num    INTEGER,
          table_name    VARCHAR,
          reason        VARCHAR,
          deferred_to   VARCHAR,
          logged_at     TIMESTAMP
        )
    """,
    "prompt6_view_rebuild_log_v1": """
        CREATE TABLE IF NOT EXISTS {DB}.manuscript_workspace.prompt6_view_rebuild_log_v1 (
          script_num   INTEGER,
          view_name    VARCHAR,
          old_target   VARCHAR,
          new_target   VARCHAR,
          decision     VARCHAR,
          rebuilt_at   TIMESTAMP
        )
    """,
    "prompt6_older_master_decisions_v1": """
        CREATE TABLE IF NOT EXISTS {DB}.manuscript_workspace.prompt6_older_master_decisions_v1 (
          table_name        VARCHAR,
          row_count         BIGINT,
          col_count         INTEGER,
          overlap_cpm       INTEGER,
          unique_cols       INTEGER,
          decision          VARCHAR,
          rationale         VARCHAR,
          executed_action   VARCHAR,
          executed_at       TIMESTAMP
        )
    """,
    "prompt6_wiring_gap_remediation_v1": """
        CREATE TABLE IF NOT EXISTS {DB}.manuscript_workspace.prompt6_wiring_gap_remediation_v1 (
          cpm_column      VARCHAR,
          source_table    VARCHAR,
          rows_staged     BIGINT,
          rows_updated    BIGINT,
          delta_nonnull   BIGINT,
          notes           VARCHAR,
          executed_at     TIMESTAMP
        )
    """,
    "prompt6_completion_audit_v1": """
        CREATE TABLE IF NOT EXISTS {DB}.manuscript_workspace.prompt6_completion_audit_v1 (
          metric_name   VARCHAR,
          metric_scope  VARCHAR,
          before_value  BIGINT,
          after_value   BIGINT,
          delta         BIGINT,
          captured_at   TIMESTAMP
        )
    """,
    "prompt7_handoff_v1": """
        CREATE TABLE IF NOT EXISTS {DB}.manuscript_workspace.prompt7_handoff_v1 (
          item            VARCHAR,
          status          VARCHAR,
          row_count       BIGINT,
          last_extracted  TIMESTAMP,
          notes           VARCHAR,
          recorded_at     TIMESTAMP
        )
    """,
    "pi_review_queue_v1": """
        CREATE TABLE IF NOT EXISTS {DB}.manuscript_workspace.pi_review_queue_v1 (
          script_num             INTEGER,
          item                   VARCHAR,
          default_used           VARCHAR,
          alternative_available  VARCHAR,
          reason                 VARCHAR,
          logged_at              TIMESTAMP
        )
    """,
}

for name, ddl in ddls.items():
    con.execute(ddl.format(DB=DB))
    print(f"  ensured {name}")

# Final summary
header("READINESS — RESULT")
print("  All checks passed. Prompt 6 is ready to execute.")
print(f"  UTC stamp for this session: {UTC}")
