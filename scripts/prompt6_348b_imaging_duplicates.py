"""
Script 348b — Imaging "duplicate" reconciliation.

REV-2 SPEC ASSUMPTION:
  imaging_exam_master_v1 / imaging_nodule_master_v1 mirror canonical_us_*
  with FEWER columns (canonical_us_* is the superset) and identical rows.
  Plan: archive both via parity check.

LIVE FINDING (2026-04-21):
  Both pairs share row counts (13,347 / 37,016) but the schemas are
  PARALLEL, not subset/superset:

    imaging_exam_master_v1 (10 cols):
      6 cols NOT in canonical_us_exam_master_v1:
        dominant_nodule_id, exam_id, has_suspicious_nodule,
        max_tirads, n_nodules, source

    imaging_nodule_master_v1 (25 cols):
      19 cols NOT in canonical_us_nodule_master_v1:
        exam_id, nodule_id, nodule_number, location_raw, margins,
        calcifications, max_dimension_cm, length_mm, width_mm,
        height_mm, volume_ml, dominant_nodule_flag, suspicious_flag,
        exam_date_quality, source_table, tirads_acr_recalculated,
        tirads_category, tirads_concordant_flag, tirads_reported

  Archiving these would lose unique columns (exam_id linkage keys,
  per-nodule physical dimensions, alternate TIRADS scoring).

DECISION:
  DEFER both. Log finding to prompt6_defer_log_v1 with full unique-col
  inventory. Recommend Prompt 7 either:
    (a) move both to manuscript_workspace (preserves data, removes from main)
    (b) backfill unique cols into canonical_us_*_master_v1 (more invasive)
    (c) document as parallel imaging views and keep in main

  No destructive operation in 348b.

Final assertions:
  - 0 archives logged for 348b
  - 2 defer rows added to prompt6_defer_log_v1 with detailed unique-col list
  - main object count unchanged
"""

from datetime import datetime, timezone
from scripts._md_connect import connect_locked

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'
SCRIPT_NUM = 348
SCRIPT_TAG = "348b_imaging_duplicate_finding_no_action"

PAIRS = [
    ("imaging_exam_master_v1",   "canonical_us_exam_master_v1"),
    ("imaging_nodule_master_v1", "canonical_us_nodule_master_v1"),
]


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


def cols(name: str) -> list[str]:
    return [r[0] for r in con.execute("""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name=?
         ORDER BY column_index
    """, [name]).fetchall()]


def row_count(name: str) -> int:
    return con.execute(f'SELECT COUNT(*) FROM {DB}.main."{name}"').fetchone()[0]


# 0. Pre-state
pre_main = main_object_count()
header("0. Pre-state")
print(f"  main object count: {pre_main}")


# 1. Per-pair analysis + defer log
header("1. Imaging pair analysis (defer-only)")
for src, dst in PAIRS:
    src_cols = cols(src)
    dst_cols = cols(dst)
    src_n = row_count(src)
    dst_n = row_count(dst)
    extra_src = sorted(set(src_cols) - set(dst_cols))
    extra_dst = sorted(set(dst_cols) - set(src_cols))
    common = sorted(set(src_cols) & set(dst_cols))

    print()
    print(f"  -- {src} (n_cols={len(src_cols)}, rows={src_n}) "
          f"vs {dst} (n_cols={len(dst_cols)}, rows={dst_n})")
    print(f"     common cols ({len(common)}): {common}")
    print(f"     unique to src ({len(extra_src)}): {extra_src}")
    print(f"     unique to dst ({len(extra_dst)}): {extra_dst}")

    reason = (
        f"Spec assumed canonical_us_* superset of imaging_*. "
        f"Live: imaging_{src.split('_')[1]}_master_v1 has "
        f"{len(extra_src)} unique cols not in canonical_us; "
        f"archive would lose: {extra_src[:6]}{'...' if len(extra_src)>6 else ''}. "
        f"Recommend Prompt 7: move to manuscript_workspace OR backfill "
        f"unique cols into canonical_us_*."
    )
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt6_defer_log_v1
        VALUES (?, ?, ?, ?, NOW())
    """, [SCRIPT_NUM, src, reason, "Prompt 7"])
    print(f"     -> deferred")


# 2. Final assertions
header("2. Final assertions")
post_main = main_object_count()
print(f"  main object count: pre={pre_main} post={post_main} delta={post_main - pre_main}")
assert post_main == pre_main, (
    f"OBJECT COUNT CHANGED unexpectedly: {pre_main} -> {post_main}"
)

deferred_348b = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.manuscript_workspace.prompt6_defer_log_v1
     WHERE script_num = ? AND deferred_to = 'Prompt 7'
       AND table_name LIKE 'imaging_%'
""", [SCRIPT_NUM]).fetchone()[0]
print(f"  imaging defer rows: {deferred_348b}")
assert deferred_348b == 2, f"expected 2 imaging defers, got {deferred_348b}"

archived_348b = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.manuscript_workspace.archive_move_log_v1
     WHERE script = ?
""", [SCRIPT_TAG]).fetchone()[0]
assert archived_348b == 0, f"expected 0 archives for 348b, got {archived_348b}"

con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_poststate_v1
    VALUES (?, 'main', '__main_object_count__', ?, NULL, NOW())
""", [SCRIPT_NUM, post_main])

print()
print("DONE. 348b deferred both imaging tables (parallel schemas, not duplicates).")
