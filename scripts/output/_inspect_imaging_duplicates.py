"""Compare imaging_*_master_v1 vs canonical_us_*_master_v1."""
from scripts._md_connect import connect_locked
con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'

PAIRS = [
    ("imaging_exam_master_v1",   "canonical_us_exam_master_v1"),
    ("imaging_nodule_master_v1", "canonical_us_nodule_master_v1"),
]

for src, dst in PAIRS:
    print(f"\n=== {src} vs {dst} ===")
    src_cols = [r[0] for r in con.execute("""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name=?
         ORDER BY column_index
    """, [src]).fetchall()]
    dst_cols = [r[0] for r in con.execute("""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name=?
         ORDER BY column_index
    """, [dst]).fetchall()]
    print(f"  src: {len(src_cols)} cols | dst: {len(dst_cols)} cols")

    src_rows = con.execute(f'SELECT COUNT(*) FROM {DB}.main."{src}"').fetchone()[0]
    dst_rows = con.execute(f'SELECT COUNT(*) FROM {DB}.main."{dst}"').fetchone()[0]
    print(f"  src rows: {src_rows} | dst rows: {dst_rows}")

    # Find a join key candidate
    common = sorted(set(src_cols) & set(dst_cols))
    print(f"  common cols ({len(common)}): {common}")
    extra_in_dst = sorted(set(dst_cols) - set(src_cols))
    extra_in_src = sorted(set(src_cols) - set(dst_cols))
    print(f"  extra in dst: {extra_in_dst}")
    print(f"  extra in src: {extra_in_src}")

    # Identify likely join keys — e.g. exam_id / nodule_id / research_id
    candidates = [c for c in src_cols if c in dst_cols and any(k in c.lower() for k in ('id', 'key', 'date'))]
    print(f"  join-key candidates: {candidates}")
