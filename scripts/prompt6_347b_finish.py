"""
Script 347b — continuation/finish.

Recovers from the partial run of prompt6_347b_patient_analysis_resolved.py
where the CPM backfill, defer log, PI flag, and view DROP succeeded but the
view CREATE was attempted before the CTAS to manuscript_workspace.

Picks up from current live state:
  - CPM has imaging_nodule_size_cm (+ _source) and margin_status_final (+ _source)
    with values populated.
  - views_readable.Analysis_Patient_Resolved is absent (was dropped).
  - main.patient_analysis_resolved_v1 still present (10,871 rows, 146 cols).
  - manuscript_workspace.patient_analysis_resolved_v1 absent.

Steps:
  1. Re-verify CPM backfill state matches expectations (idempotency check)
  2. CTAS main.patient_analysis_resolved_v1 -> manuscript_workspace
  3. Row-count parity assertion
  4. Recreate view pointing at workspace location
  5. View read-back assertion
  6. Drop main source
  7. Log to schema_reorg_move_log_v1 + prompt6_view_rebuild_log_v1
  8. Final assertions
"""

from datetime import datetime, timezone
from scripts._md_connect import connect_locked

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'
SCRIPT_NUM = 347
SCRIPT_TAG = "347b_patient_analysis_resolved_backfill_and_move"
EXPECTED_ROWS = 10871


def header(s):
    print()
    print("=" * 78)
    print(s)
    print("=" * 78)


def main_object_count() -> int:
    return con.execute("""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='thyroid_canonical_publication_v1_0' AND schema_name='main'
    """).fetchone()[0]


def cpm_nonnull(col: str) -> int:
    return con.execute(
        f'SELECT COUNT("{col}") FROM {DB}.main.canonical_patient_master'
    ).fetchone()[0]


def table_present(schema: str, name: str) -> bool:
    return con.execute("""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name=? AND table_name=?
    """, [schema, name]).fetchone()[0] > 0


def view_present(schema: str, name: str) -> bool:
    return con.execute("""
        SELECT COUNT(*) FROM duckdb_views()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name=? AND view_name=?
    """, [schema, name]).fetchone()[0] > 0


# 1. Verify CPM backfill carried over
header("1. Verify CPM backfill state (idempotency)")
nn_imaging = cpm_nonnull("imaging_nodule_size_cm")
nn_margin = cpm_nonnull("margin_status_final")
print(f"  CPM.imaging_nodule_size_cm nonnull: {nn_imaging}")
print(f"  CPM.margin_status_final    nonnull: {nn_margin}")
assert nn_imaging == 3439, f"imaging backfill drift: {nn_imaging}"
assert nn_margin  == 3957, f"margin backfill drift: {nn_margin}"


# 2. Move source to manuscript_workspace
header("2. CTAS main -> manuscript_workspace")
assert table_present("main", "patient_analysis_resolved_v1"), "main copy missing"
if table_present("manuscript_workspace", "patient_analysis_resolved_v1"):
    print("  manuscript_workspace copy already present; skipping CTAS")
    src_rows = con.execute(
        f"SELECT COUNT(*) FROM {DB}.main.patient_analysis_resolved_v1"
    ).fetchone()[0]
    dest_rows = con.execute(
        f"SELECT COUNT(*) FROM {DB}.manuscript_workspace.patient_analysis_resolved_v1"
    ).fetchone()[0]
else:
    src_rows = con.execute(
        f"SELECT COUNT(*) FROM {DB}.main.patient_analysis_resolved_v1"
    ).fetchone()[0]
    con.execute(f"""
        CREATE TABLE {DB}.manuscript_workspace.patient_analysis_resolved_v1
        AS SELECT * FROM {DB}.main.patient_analysis_resolved_v1
    """)
    dest_rows = con.execute(
        f"SELECT COUNT(*) FROM {DB}.manuscript_workspace.patient_analysis_resolved_v1"
    ).fetchone()[0]
print(f"  src={src_rows} dest={dest_rows}")
if src_rows != dest_rows or src_rows != EXPECTED_ROWS:
    raise SystemExit(
        f"PARITY FAIL: src={src_rows} dest={dest_rows} expected={EXPECTED_ROWS}"
    )


# 3. Recreate view pointing at workspace location
header("3. Recreate views_readable.Analysis_Patient_Resolved")
con.execute(
    f'DROP VIEW IF EXISTS {DB}.views_readable."Analysis_Patient_Resolved"'
)
con.execute(f"""
    CREATE VIEW {DB}.views_readable."Analysis_Patient_Resolved" AS
    SELECT * FROM {DB}.manuscript_workspace.patient_analysis_resolved_v1
""")
view_rows = con.execute(
    f'SELECT COUNT(*) FROM {DB}.views_readable."Analysis_Patient_Resolved"'
).fetchone()[0]
print(f"  view returns {view_rows} rows")
assert view_rows == EXPECTED_ROWS, f"VIEW PARITY FAIL: {view_rows}"


# 4. Drop main + log everything
header("4. Drop main + log moves")
con.execute(f"DROP TABLE {DB}.main.patient_analysis_resolved_v1")
print("  dropped main.patient_analysis_resolved_v1")

# Insert schema_reorg_move_log entry (idempotent guard)
already = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.manuscript_workspace.schema_reorg_move_log_v1
     WHERE script = ? AND source_name = 'patient_analysis_resolved_v1'
""", [SCRIPT_TAG]).fetchone()[0]
if not already:
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.schema_reorg_move_log_v1
        (moved_at, source_schema, source_name, dest_schema, dest_name,
         action, rowcount_src, rowcount_dest, script)
        VALUES (NOW(), 'main', 'patient_analysis_resolved_v1',
                'manuscript_workspace', 'patient_analysis_resolved_v1',
                'move', ?, ?, ?)
    """, [src_rows, dest_rows, SCRIPT_TAG])
    print("  schema_reorg_move_log_v1: +1 row")
else:
    print("  schema_reorg_move_log_v1: row already present (idempotent)")

# View rebuild log (idempotent guard)
already_v = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.manuscript_workspace.prompt6_view_rebuild_log_v1
     WHERE view_name = 'views_readable.Analysis_Patient_Resolved'
       AND script_num = ?
""", [SCRIPT_NUM]).fetchone()[0]
if not already_v:
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt6_view_rebuild_log_v1
        VALUES (?, ?, ?, ?, ?, NOW())
    """, [SCRIPT_NUM, "views_readable.Analysis_Patient_Resolved",
          "main.patient_analysis_resolved_v1",
          "manuscript_workspace.patient_analysis_resolved_v1",
          "repointed_to_workspace_after_347b_move"])
    print("  prompt6_view_rebuild_log_v1: +1 row")
else:
    print("  prompt6_view_rebuild_log_v1: row already present (idempotent)")


# 5. Final assertions
header("5. Final assertions")
post_main = main_object_count()
print(f"  main object count: {post_main}")
assert not table_present("main", "patient_analysis_resolved_v1")
assert table_present("manuscript_workspace", "patient_analysis_resolved_v1")
assert view_present("views_readable", "Analysis_Patient_Resolved")
print("  all checks passed")

con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_poststate_v1
    VALUES (?, 'main', '__main_object_count__', ?, NULL, NOW())
""", [SCRIPT_NUM, post_main])

print()
print("DONE. 347b finished. CPM cells delta = 7,396; view repointed; table moved.")
