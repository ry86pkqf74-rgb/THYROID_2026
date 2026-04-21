"""
Script 348 — Decision-driven older-master cleanup.

Per-table decisions (verified live 2026-04-21):

  patient_tumor_rollup_v1            (8422 rows, 41/41 col overlap with CPM)
    Action: row-level parity check (≥95% per column with IS NOT DISTINCT FROM).
            If all pass, archive to "Thyroid 2026 UPdated".archive_pub_v1_0
            and drop from main.  If any fail, abort archive and log to
            prompt6_defer_log_v1 (no destructive change).

  tirads_v2_nodule_patient_rollup_v1 (2465 rows, 8/9 col overlap)
    Action: backfill the 1 unique column (tirads_v2_worst_rank) to CPM,
            then archive + drop.

  tirads_v2_report_patient_rollup_v1 (4073 rows, 4/5 col overlap)
    Action: backfill the 1 unique column (tirads_v2_any_fna_recommended_report)
            to CPM, then archive + drop.

  ln_master_rollup_v1                (4273 rows, 25/78 col overlap)
    Action: MOVE to manuscript_workspace.  53 unique cols; full backfill
            plan deferred to Prompt 7 after RunPod cervical_ln_detail
            re-extraction lands.

  tirads_llm_extracted_v2            (5636 rows, 1/26 col overlap)
    Action: DEFER. RunPod is actively re-extracting tirads_granular;
            revisit after tier2.tirads_granular_patient_wide_v1 rebuild.

All decisions logged to manuscript_workspace.prompt6_older_master_decisions_v1.

Final assertions:
  - 3 archives logged (or fewer with explicit defer rows if parity fails)
  - 1 move logged for ln_master_rollup_v1
  - 1 defer logged for tirads_llm_extracted_v2
  - prompt6_older_master_decisions_v1 has 5 rows (one per candidate)
  - Either 3 or fewer archives + matching defers; never both archive AND defer for same table
"""

from datetime import datetime, timezone
from scripts._md_connect import connect_locked

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'
ARCH_DB = '"Thyroid 2026 UPdated"'
SCRIPT_NUM = 348
SCRIPT_TAG_ARCHIVE = "348_archive_older_masters"
SCRIPT_TAG_MOVE = "348_move_ln_master_rollup"
SCRIPT_TAG_DEFER = "348_defer_tirads_llm_extracted_v2"
UTC = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

PARITY_THRESHOLD = 0.95


def header(s):
    print()
    print("=" * 78)
    print(s)
    print("=" * 78)


def col_list(schema: str, name: str) -> list[str]:
    return [r[0] for r in con.execute("""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name=? AND table_name=?
         ORDER BY column_index
    """, [schema, name]).fetchall()]


def row_count(schema: str, name: str) -> int:
    return con.execute(f'SELECT COUNT(*) FROM {DB}.{schema}."{name}"').fetchone()[0]


def main_object_count() -> int:
    return con.execute("""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='thyroid_canonical_publication_v1_0' AND schema_name='main'
    """).fetchone()[0]


def cpm_nonnull(col: str) -> int:
    return con.execute(
        f'SELECT COUNT("{col}") FROM {DB}.main.canonical_patient_master'
    ).fetchone()[0]


def cpm_has_col(col: str) -> bool:
    return con.execute("""
        SELECT COUNT(*) FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name='canonical_patient_master'
           AND column_name=?
    """, [col]).fetchone()[0] > 0


def log_decision(name, n_rows, n_cols, overlap, unique_cols,
                 decision, rationale, executed_action):
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt6_older_master_decisions_v1
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW())
    """, [name, n_rows, n_cols, overlap, unique_cols, decision, rationale,
          executed_action])


def archive_table(name: str, reason: str, script_tag: str):
    """Archive + drop pattern.  Caller is responsible for parity checks."""
    rows = row_count("main", name)
    archive_table = f"{name}_pre348_{UTC}"
    archive_fq = f'"Thyroid 2026 UPdated".archive_pub_v1_0."{archive_table}"'
    con.execute(f"""
        CREATE TABLE {ARCH_DB}.archive_pub_v1_0."{archive_table}"
        AS SELECT * FROM {DB}.main."{name}"
    """)
    arc_rows = con.execute(
        f'SELECT COUNT(*) FROM {ARCH_DB}.archive_pub_v1_0."{archive_table}"'
    ).fetchone()[0]
    if arc_rows != rows:
        raise SystemExit(f"ARCHIVE PARITY FAIL {name}: src={rows} arc={arc_rows}")
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.archive_move_log_v1
        (moved_at, src_schema, src_table, archive_fq, n_rows, reason, script)
        VALUES (NOW(), 'main', ?, ?, ?, ?, ?)
    """, [name, archive_fq, rows, f"Prompt 6 cleanup — {reason}", script_tag])
    con.execute(f'DROP TABLE {DB}.main."{name}"')
    print(f"     archived + dropped main.{name} (rows={rows}, archive={archive_table})")


# 0. Pre-state
header("0. Pre-state")
pre_main = main_object_count()
print(f"  main object count: {pre_main}")
cpm_cols = set(col_list("main", "canonical_patient_master"))


# ============================================================================
# 1. patient_tumor_rollup_v1 — row-level parity check
# ============================================================================
header("1. patient_tumor_rollup_v1 — row-level parity check")
PTR = "patient_tumor_rollup_v1"
ptr_cols = col_list("main", PTR)
ptr_rows = row_count("main", PTR)
print(f"  rows={ptr_rows}, cols={len(ptr_cols)}")

# Column-by-column parity: build a single aggregated query
parity_cols = [c for c in ptr_cols if c != "research_id"]
parity_select = ",\n  ".join(
    f'SUM(CASE WHEN r."{c}" IS NOT DISTINCT FROM cpm."{c}" THEN 1 ELSE 0 END) AS m_{i}'
    for i, c in enumerate(parity_cols)
)
parity_sql = f"""
    SELECT
      {parity_select},
      COUNT(*) AS total_pairs
    FROM {DB}.main."{PTR}" r
    JOIN {DB}.main.canonical_patient_master cpm USING (research_id)
"""
row = con.execute(parity_sql).fetchone()
total_pairs = row[-1]
print(f"  joined pairs: {total_pairs}")

per_col_match = {}
failing = []
for i, c in enumerate(parity_cols):
    m = row[i]
    rate = m / total_pairs if total_pairs else 0.0
    per_col_match[c] = (m, rate)
    flag = " <-- FAIL" if rate < PARITY_THRESHOLD else ""
    print(f"    {c:45s} match={m:>5}/{total_pairs:>5} rate={rate:.3f}{flag}")
    if rate < PARITY_THRESHOLD:
        failing.append((c, m, rate))

if failing:
    log_decision(
        PTR, ptr_rows, len(ptr_cols), len(set(ptr_cols) & cpm_cols),
        len(set(ptr_cols) - cpm_cols),
        "DEFER", f"row-level parity below {PARITY_THRESHOLD:.0%} for {len(failing)} columns",
        "deferred — see prompt6_defer_log_v1",
    )
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt6_defer_log_v1
        VALUES (?, ?, ?, ?, NOW())
    """, [SCRIPT_NUM, PTR,
          f"row-level parity check failed for {len(failing)} columns: " +
          ", ".join(f"{c}({r:.2f})" for c, _, r in failing[:10]),
          "Prompt 7 (manual reconciliation)"])
    print(f"  -> DEFER: {len(failing)} cols below threshold; archive aborted")
else:
    print(f"  All {len(parity_cols)} cols pass {PARITY_THRESHOLD:.0%} threshold; archiving")
    archive_table(PTR, "row-level parity ≥95% across all 41 cols vs CPM",
                  SCRIPT_TAG_ARCHIVE)
    log_decision(
        PTR, ptr_rows, len(ptr_cols), len(set(ptr_cols) & cpm_cols),
        len(set(ptr_cols) - cpm_cols),
        "ARCHIVE", "100% column-name overlap; ≥95% row-level value parity",
        "archived to archive_pub_v1_0; dropped from main",
    )


# ============================================================================
# 2. tirads_v2_nodule_patient_rollup_v1 — backfill 1 col + archive
# ============================================================================
header("2. tirads_v2_nodule_patient_rollup_v1 — backfill + archive")
TVN = "tirads_v2_nodule_patient_rollup_v1"
tvn_cols = col_list("main", TVN)
tvn_rows = row_count("main", TVN)
unique_tvn = sorted(set(tvn_cols) - cpm_cols)
print(f"  rows={tvn_rows}, unique-to-this: {unique_tvn}")
assert unique_tvn == ["tirads_v2_worst_rank"], (
    f"unexpected unique cols: {unique_tvn}"
)

src_dt = con.execute("""
    SELECT data_type FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name=?
       AND column_name='tirads_v2_worst_rank'
""", [TVN]).fetchone()[0]
print(f"  source col data_type: {src_dt}")

if not cpm_has_col("tirads_v2_worst_rank"):
    con.execute(
        f'ALTER TABLE {DB}.main.canonical_patient_master '
        f'ADD COLUMN "tirads_v2_worst_rank" {src_dt}'
    )
    con.execute(
        f'ALTER TABLE {DB}.main.canonical_patient_master '
        f'ADD COLUMN "tirads_v2_worst_rank_source" VARCHAR'
    )
    print("  added CPM.tirads_v2_worst_rank + _source")

before = cpm_nonnull("tirads_v2_worst_rank")
con.execute(f"""
    UPDATE {DB}.main.canonical_patient_master AS c
       SET "tirads_v2_worst_rank"        = r."tirads_v2_worst_rank",
           "tirads_v2_worst_rank_source" = '{TVN}_backfill_348'
      FROM {DB}.main."{TVN}" AS r
     WHERE c.research_id = r.research_id
       AND c."tirads_v2_worst_rank" IS NULL
       AND r."tirads_v2_worst_rank" IS NOT NULL
""")
after = cpm_nonnull("tirads_v2_worst_rank")
print(f"  CPM.tirads_v2_worst_rank: {before} -> {after} (delta={after - before})")
con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_wiring_gap_remediation_v1
    VALUES (?, ?, ?, ?, ?, ?, NOW())
""", ["tirads_v2_worst_rank", f"main.{TVN}", tvn_rows, after - before,
      after - before, "backfilled from tirads_v2_nodule rollup before archive"])

archive_table(
    TVN, "8/9 cols mirrored in CPM; unique col tirads_v2_worst_rank backfilled",
    SCRIPT_TAG_ARCHIVE,
)
log_decision(
    TVN, tvn_rows, len(tvn_cols), len(set(tvn_cols) & cpm_cols),
    len(unique_tvn),
    "ARCHIVE_AFTER_BACKFILL",
    "1 unique col backfilled to CPM (tirads_v2_worst_rank); rollup archived",
    "archived to archive_pub_v1_0; dropped from main",
)


# ============================================================================
# 3. tirads_v2_report_patient_rollup_v1 — backfill 1 col + archive
# ============================================================================
header("3. tirads_v2_report_patient_rollup_v1 — backfill + archive")
TVR = "tirads_v2_report_patient_rollup_v1"
tvr_cols = col_list("main", TVR)
tvr_rows = row_count("main", TVR)
unique_tvr = sorted(set(tvr_cols) - cpm_cols)
print(f"  rows={tvr_rows}, unique-to-this: {unique_tvr}")
assert unique_tvr == ["tirads_v2_any_fna_recommended_report"], (
    f"unexpected unique cols: {unique_tvr}"
)
src_dt2 = con.execute("""
    SELECT data_type FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name=?
       AND column_name='tirads_v2_any_fna_recommended_report'
""", [TVR]).fetchone()[0]
print(f"  source col data_type: {src_dt2}")

if not cpm_has_col("tirads_v2_any_fna_recommended_report"):
    con.execute(
        f'ALTER TABLE {DB}.main.canonical_patient_master '
        f'ADD COLUMN "tirads_v2_any_fna_recommended_report" {src_dt2}'
    )
    con.execute(
        f'ALTER TABLE {DB}.main.canonical_patient_master '
        f'ADD COLUMN "tirads_v2_any_fna_recommended_report_source" VARCHAR'
    )
    print("  added CPM.tirads_v2_any_fna_recommended_report + _source")

before2 = cpm_nonnull("tirads_v2_any_fna_recommended_report")
con.execute(f"""
    UPDATE {DB}.main.canonical_patient_master AS c
       SET "tirads_v2_any_fna_recommended_report"        = r."tirads_v2_any_fna_recommended_report",
           "tirads_v2_any_fna_recommended_report_source" = '{TVR}_backfill_348'
      FROM {DB}.main."{TVR}" AS r
     WHERE c.research_id = r.research_id
       AND c."tirads_v2_any_fna_recommended_report" IS NULL
       AND r."tirads_v2_any_fna_recommended_report" IS NOT NULL
""")
after2 = cpm_nonnull("tirads_v2_any_fna_recommended_report")
print(f"  CPM.tirads_v2_any_fna_recommended_report: {before2} -> {after2} (delta={after2 - before2})")
con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_wiring_gap_remediation_v1
    VALUES (?, ?, ?, ?, ?, ?, NOW())
""", ["tirads_v2_any_fna_recommended_report", f"main.{TVR}", tvr_rows,
      after2 - before2, after2 - before2,
      "backfilled from tirads_v2_report rollup before archive"])

archive_table(
    TVR, "4/5 cols mirrored in CPM; unique col tirads_v2_any_fna_recommended_report backfilled",
    SCRIPT_TAG_ARCHIVE,
)
log_decision(
    TVR, tvr_rows, len(tvr_cols), len(set(tvr_cols) & cpm_cols),
    len(unique_tvr),
    "ARCHIVE_AFTER_BACKFILL",
    "1 unique col backfilled to CPM (tirads_v2_any_fna_recommended_report); rollup archived",
    "archived to archive_pub_v1_0; dropped from main",
)


# ============================================================================
# 4. ln_master_rollup_v1 — MOVE to manuscript_workspace
# ============================================================================
header("4. ln_master_rollup_v1 — MOVE (53 unique cols; backfill plan deferred)")
LMR = "ln_master_rollup_v1"
lmr_cols = col_list("main", LMR)
lmr_rows = row_count("main", LMR)
unique_lmr = sorted(set(lmr_cols) - cpm_cols)
print(f"  rows={lmr_rows}, n_cols={len(lmr_cols)}, unique={len(unique_lmr)}")

con.execute(f"""
    CREATE TABLE {DB}.manuscript_workspace."{LMR}"
    AS SELECT * FROM {DB}.main."{LMR}"
""")
dest_rows = row_count("manuscript_workspace", LMR)
if dest_rows != lmr_rows:
    raise SystemExit(f"MOVE PARITY FAIL: src={lmr_rows} dest={dest_rows}")
con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.schema_reorg_move_log_v1
    (moved_at, source_schema, source_name, dest_schema, dest_name,
     action, rowcount_src, rowcount_dest, script)
    VALUES (NOW(), 'main', ?, 'manuscript_workspace', ?, 'move', ?, ?, ?)
""", [LMR, LMR, lmr_rows, dest_rows, SCRIPT_TAG_MOVE])
con.execute(f'DROP TABLE {DB}.main."{LMR}"')
print(f"  moved {LMR} ({lmr_rows} rows) -> manuscript_workspace; main dropped")
log_decision(
    LMR, lmr_rows, len(lmr_cols), len(set(lmr_cols) & cpm_cols),
    len(unique_lmr),
    "MOVE_TO_WORKSPACE",
    "53 unique LN-level cols not in CPM; backfill plan deferred to Prompt 7 "
    "after RunPod cervical_ln_detail re-extraction",
    "moved to manuscript_workspace; dropped from main",
)


# ============================================================================
# 5. tirads_llm_extracted_v2 — DEFER (RunPod active)
# ============================================================================
header("5. tirads_llm_extracted_v2 — DEFER (RunPod active)")
TLE = "tirads_llm_extracted_v2"
tle_cols = col_list("main", TLE)
tle_rows = row_count("main", TLE)
unique_tle = sorted(set(tle_cols) - cpm_cols)
print(f"  rows={tle_rows}, n_cols={len(tle_cols)}, unique={len(unique_tle)}")
con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_defer_log_v1
    VALUES (?, ?, ?, ?, NOW())
""", [SCRIPT_NUM, TLE,
      "RunPod actively re-extracting tirads_granular; revisit after "
      "tier2.tirads_granular_patient_wide_v1 rebuild",
      "Prompt 7"])
log_decision(
    TLE, tle_rows, len(tle_cols), len(set(tle_cols) & cpm_cols),
    len(unique_tle),
    "DEFER",
    "RunPod re-extracting tirads_granular; per-nodule grain with 25 unique cols",
    "deferred — see prompt6_defer_log_v1",
)
print("  deferred")


# ============================================================================
# 6. Final assertions
# ============================================================================
header("6. Final assertions")
post_main = main_object_count()
delta = post_main - pre_main
print(f"  main object count: pre={pre_main} post={post_main} delta={delta}")

archived = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.manuscript_workspace.archive_move_log_v1
     WHERE script = ?
""", [SCRIPT_TAG_ARCHIVE]).fetchone()[0]
moved = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.manuscript_workspace.schema_reorg_move_log_v1
     WHERE script = ?
""", [SCRIPT_TAG_MOVE]).fetchone()[0]
deferred = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.manuscript_workspace.prompt6_defer_log_v1
     WHERE script_num = ?
""", [SCRIPT_NUM]).fetchone()[0]
decisions = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.manuscript_workspace.prompt6_older_master_decisions_v1
""").fetchone()[0]
print(f"  archived: {archived}, moved: {moved}, deferred: {deferred}, decisions: {decisions}")

# Allow either archived=3 (ptr passes parity) or archived=2 + ptr in defer
assert archived in (2, 3), f"archive count out of range: {archived}"
assert moved == 1, f"move count != 1: {moved}"
assert deferred >= 1, f"defer count < 1: {deferred}"
assert decisions == 5, f"decision count != 5: {decisions}"

con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_poststate_v1
    VALUES (?, 'main', '__main_object_count__', ?, NULL, NOW())
""", [SCRIPT_NUM, post_main])

print()
print(f"DONE. Script 348 — archived={archived}, moved={moved}, deferred={deferred}.")
