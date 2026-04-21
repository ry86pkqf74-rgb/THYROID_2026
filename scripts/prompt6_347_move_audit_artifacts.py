"""
Script 347 — Move 4 audit / discordance / resolved artifacts from main to
manuscript_workspace.

Tables moved:
  episode_analysis_resolved_v1_dedup           (9368 rows)
  lesion_analysis_resolved_v1                  (11851 rows)
  ln_crossval_v1                               (4290 rows)
  us_nodules_tirads_vs_inm_v1_discordance_v1   (1722 rows)

Excluded by design:
  ete_adjudication_v1                 -> in main_schema_keep_list_v1 (KEEP)
  tirads_reextraction_queue_v1        -> RunPod do-not-touch
  patient_analysis_resolved_v1        -> Script 347b handles (backfill-then-move)

Pre-move check: scan views_readable for direct references to each candidate.
If any view references a moved table, log to prompt6_view_rebuild_log_v1 (the
view will be repointed in Script 353 orphan sweep — non-destructive here).

Move pattern:
  1. Pre-snapshot
  2. CTAS to manuscript_workspace.<name>
  3. Row-count parity assertion (RAISE on mismatch)
  4. Log to schema_reorg_move_log_v1 with action='move'
  5. DROP from main

Final assertions:
  - exactly 4 rows added to schema_reorg_move_log_v1 with script='347_move_audit_artifacts'
  - main object count decreases by exactly 4
  - all 4 names present in manuscript_workspace, absent from main
  - ete_adjudication_v1, tirads_reextraction_queue_v1, patient_analysis_resolved_v1 still in main
"""

from datetime import datetime, timezone
from scripts._md_connect import connect_locked

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'
SCRIPT_NUM = 347
SCRIPT_TAG = "347_move_audit_artifacts"
UTC = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

MOVE_LIST = [
    "episode_analysis_resolved_v1_dedup",
    "lesion_analysis_resolved_v1",
    "ln_crossval_v1",
    "us_nodules_tirads_vs_inm_v1_discordance_v1",
]

MUST_REMAIN_IN_MAIN = [
    "ete_adjudication_v1",
    "tirads_reextraction_queue_v1",
    "patient_analysis_resolved_v1",
]


def header(s):
    print()
    print("=" * 78)
    print(s)
    print("=" * 78)


def main_object_count() -> int:
    return con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main'
    """).fetchone()[0]


def table_present(schema: str, name: str) -> bool:
    r = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name=? AND table_name=?
    """, [schema, name]).fetchone()[0]
    return r > 0


def col_count(schema: str, name: str) -> int:
    return con.execute(f"""
        SELECT COUNT(*) FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name=? AND table_name=?
    """, [schema, name]).fetchone()[0]


def row_count(schema: str, name: str) -> int:
    return con.execute(f'SELECT COUNT(*) FROM {DB}.{schema}."{name}"').fetchone()[0]


# 0. Pre-state
header("0. Pre-state")
pre_main = main_object_count()
print(f"  main object count: {pre_main}")
for name in MOVE_LIST:
    assert table_present("main", name), f"PRECONDITION FAIL: main.{name} missing"
    assert not table_present("manuscript_workspace", name), (
        f"PRECONDITION FAIL: manuscript_workspace.{name} already exists"
    )
    print(f"  movable: main.{name} (rows={row_count('main', name)}, cols={col_count('main', name)})")

for name in MUST_REMAIN_IN_MAIN:
    assert table_present("main", name), (
        f"PRECONDITION FAIL: protected main.{name} missing"
    )

# Pre-snapshot
for name in MOVE_LIST:
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt6_prestate_v1
        VALUES (?, 'main', ?, ?, ?, NOW())
    """, [SCRIPT_NUM, name, row_count("main", name), col_count("main", name)])

# View dependency scan (non-blocking)
header("0b. View dependency scan (non-blocking — tracked for 353)")
for name in MOVE_LIST:
    refs = con.execute(f"""
        SELECT view_name, schema_name FROM duckdb_views()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND (sql LIKE ? OR sql LIKE ?)
    """, [f"%main.{name}%", f'%"{name}"%']).fetchall()
    if refs:
        for v_name, v_schema in refs:
            print(f"  REF main.{name} <- {v_schema}.{v_name}")
            con.execute(f"""
                INSERT INTO {DB}.manuscript_workspace.prompt6_view_rebuild_log_v1
                VALUES (?, ?, ?, ?, 'flagged_for_repoint', NOW())
            """, [SCRIPT_NUM, f"{v_schema}.{v_name}",
                  f"main.{name}", f"manuscript_workspace.{name}"])
    else:
        print(f"  no views reference main.{name}")


# 1. Move sequence
header("1. Move sequence")
for name in MOVE_LIST:
    src_rows = row_count("main", name)
    print()
    print(f"  -> {name} (src_rows={src_rows})")

    # CTAS
    con.execute(f"""
        CREATE TABLE {DB}.manuscript_workspace."{name}"
        AS SELECT * FROM {DB}.main."{name}"
    """)

    dest_rows = row_count("manuscript_workspace", name)
    if dest_rows != src_rows:
        # cleanup partial CTAS
        con.execute(f'DROP TABLE {DB}.manuscript_workspace."{name}"')
        raise SystemExit(
            f"MOVE PARITY FAIL: {name} src={src_rows} dest={dest_rows} (rolled back)"
        )
    print(f"     parity OK ({dest_rows} rows)")

    # Log
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.schema_reorg_move_log_v1
        (moved_at, source_schema, source_name, dest_schema, dest_name,
         action, rowcount_src, rowcount_dest, script)
        VALUES (NOW(), 'main', ?, 'manuscript_workspace', ?, 'move', ?, ?, ?)
    """, [name, name, src_rows, dest_rows, SCRIPT_TAG])

    # Drop source
    con.execute(f'DROP TABLE {DB}.main."{name}"')
    print(f"     dropped main.{name}")


# 2. Final assertions
header("2. Final assertions")
post_main = main_object_count()
print(f"  main object count: pre={pre_main} post={post_main} delta={post_main - pre_main}")
if post_main != pre_main - len(MOVE_LIST):
    raise SystemExit(
        f"OBJECT-COUNT FAIL: expected delta -{len(MOVE_LIST)}, got {post_main - pre_main}"
    )

logged = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.manuscript_workspace.schema_reorg_move_log_v1
     WHERE script = ?
""", [SCRIPT_TAG]).fetchone()[0]
print(f"  schema_reorg_move_log_v1 rows for {SCRIPT_TAG}: {logged}")
if logged != len(MOVE_LIST):
    raise SystemExit(f"LOG FAIL: expected {len(MOVE_LIST)}, got {logged}")

for name in MOVE_LIST:
    assert table_present("manuscript_workspace", name), (
        f"DEST FAIL: manuscript_workspace.{name} missing"
    )
    assert not table_present("main", name), (
        f"SRC FAIL: main.{name} still present after drop"
    )

for name in MUST_REMAIN_IN_MAIN:
    assert table_present("main", name), (
        f"PROTECTION FAIL: main.{name} unexpectedly removed"
    )
    print(f"  protected-still-present: main.{name}")

con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_poststate_v1
    VALUES (?, 'main', '__main_object_count__', ?, NULL, NOW())
""", [SCRIPT_NUM, post_main])

print()
print(f"DONE. Script 347 moved {len(MOVE_LIST)} audit/discordance tables to manuscript_workspace.")
