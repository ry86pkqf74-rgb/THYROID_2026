"""
Pre-flight verification for CURSOR PROMPT 6 (scripts 346-348).

Goal: Before any table in `main` is archived, moved to manuscript_workspace, or
dropped, confirm that the data inside that table is housed elsewhere in the
canonical V1_0 motherduck database `thyroid_canonical_publication_v1_0`.

Scope:
  - Script 346 candidates: `extracted_*_v1` legacy pipeline outputs
  - Script 347 candidates: audit / queue / discordance / resolved tables
  - Script 348 candidates: older `*_rollup_v1` / `*_extracted_v2` masters

For each candidate the script reports:
  (a) Existence + row count + research_id coverage in main
  (b) Best-match canonical sibling(s) in main / tier2 / verify by name lineage
  (c) research_id overlap between candidate and best-match sibling
  (d) Whether equivalent column data exists in canonical_patient_master
  (e) Verdict: SAFE_TO_ARCHIVE / KEEP_AS_DECISION_LOG / NEEDS_REVIEW

Read-only.  Does NOT mutate any data.

Usage:
    python -m scripts.output._verify_prompt6_data_elsewhere \
        > scripts/output/_verify_prompt6_data_elsewhere.txt 2>&1
"""

from scripts._md_connect import connect_locked
from collections import defaultdict

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'


def header(s, ch="="):
    print()
    print(ch * 78)
    print(s)
    print(ch * 78)


def list_tables(schema: str) -> list[str]:
    rows = con.execute(f"""
        SELECT table_name FROM duckdb_tables()
         WHERE database_name = 'thyroid_canonical_publication_v1_0'
           AND schema_name   = '{schema}'
         ORDER BY table_name
    """).fetchall()
    return [r[0] for r in rows]


def list_views(schema: str) -> list[str]:
    rows = con.execute(f"""
        SELECT view_name FROM duckdb_views()
         WHERE database_name = 'thyroid_canonical_publication_v1_0'
           AND schema_name   = '{schema}'
         ORDER BY view_name
    """).fetchall()
    return [r[0] for r in rows]


def col_set(schema: str, table: str) -> set[str]:
    rows = con.execute(f"""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name = 'thyroid_canonical_publication_v1_0'
           AND schema_name = '{schema}' AND table_name = '{table}'
    """).fetchall()
    return {r[0] for r in rows}


def safe_count(schema: str, table: str):
    """Return (row_count, distinct_research_id_or_None)."""
    try:
        n = con.execute(
            f'SELECT COUNT(*) FROM {DB}.{schema}."{table}"'
        ).fetchone()[0]
    except Exception as exc:
        return (None, None, str(exc))
    cols = col_set(schema, table)
    if "research_id" in cols:
        try:
            d = con.execute(
                f'SELECT COUNT(DISTINCT research_id) '
                f'FROM {DB}.{schema}."{table}"'
            ).fetchone()[0]
            return (n, d, None)
        except Exception as exc:
            return (n, None, str(exc))
    return (n, None, None)


def overlap(schema_a: str, table_a: str, schema_b: str, table_b: str):
    """Return distinct-rid overlap between two tables (both must have rid)."""
    try:
        n = con.execute(f"""
            SELECT COUNT(DISTINCT a.research_id)
              FROM {DB}.{schema_a}."{table_a}" a
              JOIN {DB}.{schema_b}."{table_b}" b
                ON a.research_id = b.research_id
        """).fetchone()[0]
        return n
    except Exception as exc:
        return f"ERR: {exc}"


# =============================================================================
# 0. SANITY: confirm the schemas referenced by Prompt 6 actually exist
# =============================================================================
header("0. Schemas present in publication DB")
schemas = con.execute("""
    SELECT schema_name, COUNT(*) AS object_count
      FROM duckdb_tables()
     WHERE database_name='thyroid_canonical_publication_v1_0'
     GROUP BY schema_name
     UNION ALL
    SELECT schema_name, COUNT(*) AS object_count
      FROM duckdb_views()
     WHERE database_name='thyroid_canonical_publication_v1_0'
     GROUP BY schema_name
     ORDER BY 1
""").fetchall()
agg = defaultdict(int)
for s, c in schemas:
    agg[s] += c
for s in sorted(agg):
    print(f"  {s:35s} {agg[s]:>5} objects")

main_tables = set(list_tables("main"))
main_views = set(list_views("main"))
print(f"\n  main: tables={len(main_tables)}, views={len(main_views)}")

# Confirm the keep-list referenced by Script 347 exists
ws_tables = set(list_tables("manuscript_workspace"))
print(
    f"  manuscript_workspace.main_schema_keep_list_v1 present: "
    f"{'main_schema_keep_list_v1' in ws_tables}"
)
print(
    f"  manuscript_workspace.cpm_missing_data_provenance_v1 present: "
    f"{'cpm_missing_data_provenance_v1' in ws_tables}"
)
print(
    f"  manuscript_workspace.archive_move_log_v1 present: "
    f"{'archive_move_log_v1' in ws_tables}"
)
print(
    f"  manuscript_workspace.schema_reorg_move_log_v1 present: "
    f"{'schema_reorg_move_log_v1' in ws_tables}"
)


# =============================================================================
# 1. KEEP-LIST CONTENTS (so we know which candidates to skip outright)
# =============================================================================
header("1. main_schema_keep_list_v1 contents (skip-these list)")
keep_list = set()
if "main_schema_keep_list_v1" in ws_tables:
    try:
        rows = con.execute(f"""
            SELECT * FROM {DB}.manuscript_workspace.main_schema_keep_list_v1
        """).fetchall()
        cols = [d[0] for d in con.description]
        print(f"  cols: {cols}")
        for r in rows:
            print(f"    {dict(zip(cols, r))}")
            # Heuristic: first column is the table name
            if r and r[0]:
                keep_list.add(r[0])
    except Exception as exc:
        print(f"  ERROR reading keep-list: {exc}")
else:
    print("  (table not present — Prompt 6 §2.347 cross-check will be skipped)")


# =============================================================================
# 2. SCRIPT 346 CANDIDATES — extracted_*_v1
# =============================================================================
header("2. SCRIPT 346 candidates — extracted_*_v1 legacy outputs")
ext_v1 = sorted(t for t in main_tables if t.startswith("extracted_") and t.endswith("_v1"))
print(f"  Found {len(ext_v1)} extracted_*_v1 tables in main:")
for t in ext_v1:
    n, d, err = safe_count("main", t)
    print(f"    {t:50s} rows={n} rids={d} {('ERR='+err) if err else ''}")

# For each, look for canonical / note_entities_llm sibling that supersedes it.
LLM_TABLES = sorted(t for t in main_tables if t.startswith("note_entities_llm_"))
CANONICAL_TABLES = sorted(t for t in main_tables if t.startswith("canonical_"))
TIER2_TABLES = sorted(list_tables("tier2"))
VERIFY_TABLES = sorted(list_tables("verify"))

print()
print("  Per-table lineage check (what supersedes each extracted_*_v1):")
for t in ext_v1:
    stem = t[len("extracted_"):-len("_v1")]
    # Likely lineage candidates (substring match on stem token)
    tokens = [w for w in stem.split("_") if w not in {"the", "of", "v"}]
    sib_main = [
        x for x in (LLM_TABLES + CANONICAL_TABLES)
        if any(tok in x for tok in tokens)
    ]
    sib_tier2 = [x for x in TIER2_TABLES if any(tok in x for tok in tokens)]
    sib_verify = [x for x in VERIFY_TABLES if any(tok in x for tok in tokens)]
    print()
    print(f"  -- {t} (stem='{stem}', tokens={tokens})")
    print(f"     main lineage candidates : {sib_main[:6]}{' …' if len(sib_main)>6 else ''}")
    print(f"     tier2 lineage candidates: {sib_tier2[:6]}{' …' if len(sib_tier2)>6 else ''}")
    print(f"     verify lineage candidates: {sib_verify[:6]}{' …' if len(sib_verify)>6 else ''}")
    # rid overlap with the strongest-looking sibling (heuristic = first match)
    if "research_id" in col_set("main", t):
        for cand_schema, cand in (
            [("main", x) for x in sib_main[:2]]
            + [("tier2", x) for x in sib_tier2[:1]]
            + [("verify", x) for x in sib_verify[:1]]
        ):
            if "research_id" in col_set(cand_schema, cand):
                ov = overlap("main", t, cand_schema, cand)
                _, d_src, _ = safe_count("main", t)
                _, d_cand, _ = safe_count(cand_schema, cand)
                print(
                    f"     rid_overlap[main.{t} ∩ {cand_schema}.{cand}] = "
                    f"{ov}  (src_rids={d_src}, cand_rids={d_cand})"
                )


# =============================================================================
# 3. SCRIPT 347 CANDIDATES — adjudication / queue / discordance / resolved
# =============================================================================
header("3. SCRIPT 347 candidates — audit / queue / discordance / resolved")
candidates_347 = []
patterns_347 = ("adjudication", "crossval", "discordance", "resolved", "queue")
DO_NOT_TOUCH_347 = {"tirads_reextraction_queue_v1"}  # RunPod is reading this
for t in sorted(main_tables):
    low = t.lower()
    if any(p in low for p in patterns_347) and t not in DO_NOT_TOUCH_347:
        candidates_347.append(t)
print(f"  Found {len(candidates_347)} candidate tables (RunPod-reserved excluded):")
for t in candidates_347:
    n, d, err = safe_count("main", t)
    cols = col_set("main", t)
    rid_col = "research_id" in cols
    keep_flag = "(KEEP — in keep-list)" if t in keep_list else ""
    print(
        f"    {t:55s} rows={n} rids={d} cols={len(cols)} {keep_flag}"
    )

print()
print("  Per-table data-elsewhere check:")
for t in candidates_347:
    cols = col_set("main", t)
    n, d, _ = safe_count("main", t)
    print()
    print(f"  -- {t} (n_rows={n}, rids={d}, n_cols={len(cols)})")
    if t in keep_list:
        print(f"     KEEP — listed in main_schema_keep_list_v1")
    # Show first 8 columns to characterize content
    sample_cols = sorted(list(cols))[:12]
    print(f"     sample columns: {sample_cols}")
    # If it has research_id, confirm those rids exist in CPM (canonical patient registry)
    if "research_id" in cols and n is not None and n > 0:
        try:
            rid_in_cpm = con.execute(f"""
                SELECT COUNT(DISTINCT a.research_id)
                  FROM {DB}.main."{t}" a
                  JOIN {DB}.main.canonical_patient_master b
                    ON a.research_id = b.research_id
            """).fetchone()[0]
            print(
                f"     rids present in canonical_patient_master: "
                f"{rid_in_cpm} / {d} candidate rids"
            )
        except Exception as exc:
            print(f"     CPM rid check failed: {exc}")
    # If it references note_id / note_row_id, check those notes still exist
    note_id_col = next(
        (c for c in ("note_row_id", "note_id", "source_note_ref") if c in cols),
        None,
    )
    if note_id_col:
        try:
            nrows = con.execute(f"""
                SELECT COUNT(*)
                  FROM {DB}.main."{t}" a
                  JOIN {DB}.main.clinical_notes_long b
                    ON a.{note_id_col} = b.note_row_id
            """).fetchone()[0]
            tot = con.execute(
                f'SELECT COUNT(*) FROM {DB}.main."{t}" '
                f'WHERE {note_id_col} IS NOT NULL'
            ).fetchone()[0]
            print(
                f"     note linkage via {note_id_col}: "
                f"{nrows}/{tot} rows resolve in clinical_notes_long"
            )
        except Exception as exc:
            print(f"     note linkage check failed: {exc}")


# =============================================================================
# 4. SCRIPT 348 CANDIDATES — older masters (*_rollup_v1, *_extracted_v2)
# =============================================================================
header("4. SCRIPT 348 candidates — older rollups / extracted_v2 masters")
candidates_348 = sorted(
    t for t in main_tables
    if (
        t.endswith("_rollup_v1")
        or t.endswith("_extracted_v2")
        or t in {"tirads_llm_extracted_v2", "patient_tumor_rollup_v1"}
    )
)
print(f"  Found {len(candidates_348)} older-master candidates:")
for t in candidates_348:
    n, d, _ = safe_count("main", t)
    cols = col_set("main", t)
    print(f"    {t:50s} rows={n} rids={d} cols={len(cols)}")

# Check column-name overlap with CPM
cpm_cols = col_set("main", "canonical_patient_master")
print(f"\n  CPM has {len(cpm_cols)} columns total.")
print()
print("  Per-table CPM-overlap analysis:")
for t in candidates_348:
    cols = col_set("main", t)
    overlap_cols = sorted(cols & cpm_cols)
    nonoverlap = sorted(cols - cpm_cols)
    print()
    print(f"  -- {t}")
    print(f"     n_cols={len(cols)}  CPM-overlap={len(overlap_cols)}  unique-to-this={len(nonoverlap)}")
    print(f"     overlap (first 15) : {overlap_cols[:15]}")
    print(f"     unique  (first 15) : {nonoverlap[:15]}")
    # rid overlap with CPM
    if "research_id" in cols:
        try:
            ov = con.execute(f"""
                SELECT COUNT(DISTINCT a.research_id)
                  FROM {DB}.main."{t}" a
                  JOIN {DB}.main.canonical_patient_master b
                    ON a.research_id = b.research_id
            """).fetchone()[0]
            _, d, _ = safe_count("main", t)
            print(f"     rids present in CPM: {ov} / {d}")
        except Exception as exc:
            print(f"     rid overlap check failed: {exc}")


# =============================================================================
# 5. RUNPOD DO-NOT-TOUCH — confirm these are still present and untouched
# =============================================================================
header("5. RunPod DO-NOT-TOUCH list — must remain in main untouched")
dnt = [
    "note_entities_llm_pathology",
    "note_entities_llm_cervical_ln_detail",
    "note_entities_llm_tirads_granular",
    "note_entities_llm_esophageal_invasion",  # may not exist yet
    "tirads_reextraction_queue_v1",
    "operative_episode_detail_v2",
]
for t in dnt:
    present = t in main_tables
    n, d, err = safe_count("main", t) if present else (None, None, "absent")
    print(f"  {t:50s} present={present} rows={n} rids={d} {('('+err+')') if err else ''}")


# =============================================================================
# 6. ARCHIVE TARGET — confirm archive_pub_v1_0 schema exists
# =============================================================================
header("6. archive_pub_v1_0 schema check")
arch_db_present = (
    con.execute(
        "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name='archive_pub_v1_0'"
    ).fetchone()[0]
    > 0
)
print(f"  archive_pub_v1_0 attached as a database: {arch_db_present}")
arch_schemas = con.execute(
    "SELECT schema_name FROM duckdb_schemas() WHERE database_name='thyroid_canonical_publication_v1_0' AND schema_name='archive_pub_v1_0'"
).fetchall()
print(f"  archive_pub_v1_0 as schema in publication DB: {[r[0] for r in arch_schemas]}")


# =============================================================================
# 7. VERDICT TABLE
# =============================================================================
header("7. VERDICT — per-table recommendation for Prompt 6")

print(
    f"{'table':50s}  {'script':6s}  {'rows':>7s}  {'rids':>6s}  verdict / rationale"
)
print("-" * 110)


def verdict_for_extracted(t: str) -> str:
    n, d, _ = safe_count("main", t)
    return (
        f"SAFE_TO_ARCHIVE — superseded by note_entities_llm_*/canonical_*; "
        f"snapshot to archive_pub_v1_0 then drop"
    )


def verdict_for_audit(t: str) -> str:
    if t in keep_list:
        return "KEEP — in main_schema_keep_list_v1"
    return (
        "MOVE_TO_MANUSCRIPT_WORKSPACE — decision/audit log content is unique "
        "but underlying clinical data is preserved in canonical_*/note_entities_llm_*"
    )


def verdict_for_older_master(t: str) -> str:
    cols = col_set("main", t)
    overlap_pct = len(cols & cpm_cols) / max(len(cols), 1)
    if overlap_pct >= 0.9:
        return f"REVIEW_FOR_ARCHIVE — {overlap_pct:.0%} of columns also in CPM"
    if overlap_pct >= 0.4:
        return (
            f"REVIEW_FOR_MOVE — {overlap_pct:.0%} of columns in CPM; "
            "remainder may need backfill before archive"
        )
    return (
        f"NEEDS_REVIEW — only {overlap_pct:.0%} of columns in CPM; "
        "may still hold unique data — keep in main or move to ws with note"
    )


for t in ext_v1:
    n, d, _ = safe_count("main", t)
    print(f"{t:50s}  {'346':>6s}  {n if n is not None else 'NA':>7}  {d if d is not None else 'NA':>6}  {verdict_for_extracted(t)}")

for t in candidates_347:
    n, d, _ = safe_count("main", t)
    print(f"{t:50s}  {'347':>6s}  {n if n is not None else 'NA':>7}  {d if d is not None else 'NA':>6}  {verdict_for_audit(t)}")

for t in candidates_348:
    n, d, _ = safe_count("main", t)
    print(f"{t:50s}  {'348':>6s}  {n if n is not None else 'NA':>7}  {d if d is not None else 'NA':>6}  {verdict_for_older_master(t)}")


print("\nDONE.")
