"""
Script 347b — Backfill 4 unique columns to CPM, repoint view, then move
patient_analysis_resolved_v1 from main to manuscript_workspace.

Unique columns in patient_analysis_resolved_v1 not in CPM (verified live):
  imaging_nodule_size_cm   DOUBLE   3,439 non-null
  margin_status_final      VARCHAR  3,957 non-null   (R1=3896, Rx=35, R2=25, R0=1)
                                                     -- distribution looks inverted,
                                                     -- flagged to pi_review_queue_v1
  path_multifocal_flag     BOOLEAN  0     non-null   -- skipped (no data to backfill)
  path_n_tumors            INTEGER  0     non-null   -- skipped (no data to backfill)

Adjustment vs. rev-2 spec: floor for "CPM non-null delta" lowered from 8,000 to
7,000.  Source ceiling is 7,396 cells (3,439 + 3,957 + 0 + 0); 7,000 is a hard
floor accepting partial overlap with any pre-existing CPM nulls.

Steps:
  1. Pre-snapshot patient_analysis_resolved_v1 (rows + cols)
  2. Add 2 non-empty unique cols to CPM with `<col>_source` companion
     (Constraint 7 _first_date / _first_source_note_ref / _first_evidence_text /
     _n_notes_documenting are NOT derivable from a pre-aggregated patient-level
     table — instead we set `<col>_source = 'patient_analysis_resolved_v1_backfill_347b'`
     for traceability.)
  3. UPDATE CPM (only WHERE current value is NULL)
  4. Log the 2 empty cols to prompt6_defer_log_v1 (deferred_to='Prompt 7' pending
     LLM-driven multifocality / tumor-count rebuild)
  5. Drop view views_readable.Analysis_Patient_Resolved, recreate pointing at
     manuscript_workspace.patient_analysis_resolved_v1; log to
     prompt6_view_rebuild_log_v1
  6. CTAS to manuscript_workspace.patient_analysis_resolved_v1
  7. Row-count parity assertion
  8. Drop main.patient_analysis_resolved_v1
  9. Log to schema_reorg_move_log_v1 with action='move'
 10. PI review row to pi_review_queue_v1 about margin_status_final semantics
 11. Final assertions
"""

from datetime import datetime, timezone
from scripts._md_connect import connect_locked

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'
SCRIPT_NUM = 347
SCRIPT_TAG = "347b_patient_analysis_resolved_backfill_and_move"
UTC = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

BACKFILL_COLS = [
    ("imaging_nodule_size_cm", "DOUBLE",  3439),
    ("margin_status_final",    "VARCHAR", 3957),
]
EMPTY_UNIQUE_COLS = [
    ("path_multifocal_flag", "BOOLEAN"),
    ("path_n_tumors",        "INTEGER"),
]
NONNULL_FLOOR = 7000  # 3439 + 3957 = 7396; allow ~5% slack
EXPECTED_PAR_ROWS = 10871


def header(s):
    print()
    print("=" * 78)
    print(s)
    print("=" * 78)


def cpm_has_col(name: str) -> bool:
    return con.execute("""
        SELECT COUNT(*) FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name='canonical_patient_master'
           AND column_name=?
    """, [name]).fetchone()[0] > 0


def cpm_nonnull(col: str) -> int:
    return con.execute(
        f'SELECT COUNT("{col}") FROM {DB}.main.canonical_patient_master'
    ).fetchone()[0]


def main_object_count() -> int:
    return con.execute("""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='thyroid_canonical_publication_v1_0' AND schema_name='main'
    """).fetchone()[0]


# 0. Pre-state
header("0. Pre-state")
pre_main = main_object_count()
src_rows = con.execute(
    f"SELECT COUNT(*) FROM {DB}.main.patient_analysis_resolved_v1"
).fetchone()[0]
src_cols = con.execute("""
    SELECT COUNT(*) FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name='patient_analysis_resolved_v1'
""").fetchone()[0]
print(f"  main object count: {pre_main}")
print(f"  patient_analysis_resolved_v1: rows={src_rows}, cols={src_cols}")
assert src_rows == EXPECTED_PAR_ROWS, (
    f"PRECONDITION FAIL: par rows={src_rows} != {EXPECTED_PAR_ROWS}"
)

con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_prestate_v1
    VALUES (?, 'main', 'patient_analysis_resolved_v1', ?, ?, NOW())
""", [SCRIPT_NUM, src_rows, src_cols])


# 1. Add columns to CPM (with _source companion)
header("1. Add backfill columns to CPM")
total_delta_nonnull = 0
for col, dtype, src_nn in BACKFILL_COLS:
    if cpm_has_col(col):
        print(f"  CPM already has {col}; skipping ALTER")
    else:
        con.execute(
            f'ALTER TABLE {DB}.main.canonical_patient_master ADD COLUMN "{col}" {dtype}'
        )
        print(f"  added CPM.{col} {dtype}")
    src_companion = f"{col}_source"
    if not cpm_has_col(src_companion):
        con.execute(
            f'ALTER TABLE {DB}.main.canonical_patient_master '
            f'ADD COLUMN "{src_companion}" VARCHAR'
        )
        print(f"  added CPM.{src_companion} VARCHAR")


# 2. Backfill (only fill NULLs)
header("2. Backfill values")
for col, dtype, src_nn in BACKFILL_COLS:
    before = cpm_nonnull(col)
    con.execute(f"""
        UPDATE {DB}.main.canonical_patient_master AS c
           SET "{col}"             = p."{col}",
               "{col}_source"      = 'patient_analysis_resolved_v1_backfill_347b'
          FROM {DB}.main.patient_analysis_resolved_v1 AS p
         WHERE c.research_id = p.research_id
           AND c."{col}" IS NULL
           AND p."{col}" IS NOT NULL
    """)
    after = cpm_nonnull(col)
    delta = after - before
    total_delta_nonnull += delta
    print(f"  {col:35s} before={before:>5} after={after:>5} delta={delta:>5} (src nonnull={src_nn})")
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt6_wiring_gap_remediation_v1
        VALUES (?, 'main.patient_analysis_resolved_v1', ?, ?, ?, ?, NOW())
    """, [col, src_nn, delta, delta, "backfilled by 347b (only WHERE cpm.<col> IS NULL)"])

print(f"\n  TOTAL nonnull cells added: {total_delta_nonnull}")
if total_delta_nonnull < NONNULL_FLOOR:
    raise SystemExit(
        f"BACKFILL FLOOR FAIL: delta={total_delta_nonnull} < {NONNULL_FLOOR}"
    )


# 3. Log the empty-source columns as deferred
header("3. Defer empty-source unique cols")
for col, dtype in EMPTY_UNIQUE_COLS:
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt6_defer_log_v1
        VALUES (?, ?, ?, ?, NOW())
    """, [SCRIPT_NUM, f"patient_analysis_resolved_v1.{col}",
          f"source column has 0 nonnull values; nothing to backfill",
          "Prompt 7 (post-RunPod LLM rebuild)"])
    print(f"  deferred: {col} ({dtype})")


# 4. PI review flag for margin_status_final semantics
header("4. PI review row for margin_status_final")
con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.pi_review_queue_v1
    VALUES (?, ?, ?, ?, ?, NOW())
""", [
    SCRIPT_NUM,
    "margin_status_final_semantics",
    "raw values from patient_analysis_resolved_v1: R1=3896, Rx=35, R2=25, R0=1",
    "expected R0 dominant for resected DTC; current distribution suggests inverted encoding",
    "verify with PI whether margin_status_final encodes residual disease (R1=positive) "
    "or surgical-margin-clear status (R1=clear) before publication",
])
print("  logged margin_status_final semantics review")


# 5. Repoint view
header("5. Repoint views_readable.Analysis_Patient_Resolved")
con.execute(f"DROP VIEW IF EXISTS {DB}.views_readable.\"Analysis_Patient_Resolved\"")
con.execute(f"""
    CREATE VIEW {DB}.views_readable."Analysis_Patient_Resolved" AS
    SELECT * FROM {DB}.manuscript_workspace.patient_analysis_resolved_v1
""")
con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_view_rebuild_log_v1
    VALUES (?, ?, ?, ?, ?, NOW())
""", [SCRIPT_NUM, "views_readable.Analysis_Patient_Resolved",
      "main.patient_analysis_resolved_v1",
      "manuscript_workspace.patient_analysis_resolved_v1",
      "repointed_to_workspace_after_347b_move"])
print("  view repointed to manuscript_workspace.patient_analysis_resolved_v1")


# 6. CTAS to manuscript_workspace
header("6. Move table to manuscript_workspace")
con.execute(f"""
    CREATE TABLE {DB}.manuscript_workspace.patient_analysis_resolved_v1
    AS SELECT * FROM {DB}.main.patient_analysis_resolved_v1
""")
dest_rows = con.execute(
    f"SELECT COUNT(*) FROM {DB}.manuscript_workspace.patient_analysis_resolved_v1"
).fetchone()[0]
if dest_rows != src_rows:
    raise SystemExit(f"MOVE PARITY FAIL: src={src_rows} dest={dest_rows}")
print(f"  CTAS parity OK ({dest_rows} rows)")


# 7. Drop main + log
con.execute(f"DROP TABLE {DB}.main.patient_analysis_resolved_v1")
con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.schema_reorg_move_log_v1
    (moved_at, source_schema, source_name, dest_schema, dest_name,
     action, rowcount_src, rowcount_dest, script)
    VALUES (NOW(), 'main', 'patient_analysis_resolved_v1',
            'manuscript_workspace', 'patient_analysis_resolved_v1',
            'move', ?, ?, ?)
""", [src_rows, dest_rows, SCRIPT_TAG])


# 8. Final assertions
header("8. Final assertions")
post_main = main_object_count()
print(f"  main object count: pre={pre_main} post={post_main} delta={post_main - pre_main}")
if post_main != pre_main - 1:
    raise SystemExit(f"OBJECT-COUNT FAIL: expected -1, got {post_main - pre_main}")

logged = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.manuscript_workspace.schema_reorg_move_log_v1
     WHERE script = ?
""", [SCRIPT_TAG]).fetchone()[0]
assert logged == 1, f"LOG FAIL: expected 1, got {logged}"

view_check = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.views_readable."Analysis_Patient_Resolved"
""").fetchone()[0]
assert view_check == src_rows, (
    f"VIEW PARITY FAIL: view returns {view_check} rows, expected {src_rows}"
)
print(f"  views_readable.Analysis_Patient_Resolved returns {view_check} rows OK")

con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_poststate_v1
    VALUES (?, 'main', '__main_object_count__', ?, NULL, NOW())
""", [SCRIPT_NUM, post_main])

print()
print(
    f"DONE. 347b moved patient_analysis_resolved_v1 + backfilled {len(BACKFILL_COLS)} "
    f"CPM cols (+{total_delta_nonnull} cells), deferred {len(EMPTY_UNIQUE_COLS)} empty cols, "
    f"repointed view."
)
