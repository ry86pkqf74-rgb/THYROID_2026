"""Script 237 — Close the 7 registry→CPM pointer gaps surfaced in Script 236 Phase 6.

Database: ``thyroid_canonical_publication_v1_0`` on MotherDuck.  Archive
destination: ``"Thyroid 2026 UPdated".archive_pub_v1_0``.

Resolves ``scripts/output/236_missing_canonical_columns.csv``:

    ADD + DERIVE  (genuine missing CPM columns)
        • ete_adjudicated_flag          from ete_adjudication_v1
        • ret_adjudicated_flag          from ret_patient_adjudicated_v226
        • ret_evidence_source           from ret_patient_adjudicated_v226 +
                                         _molecular_patient_rollup_v227

    REGISTRY TEXT FIX  (typo: column exists under different name)
        • _molecular_patient_rollup_v227    molecular_rollup_version   -> rollup_script_version
        • canonical_benign_diagnosis_v1     has_follicular_adenoma     -> syn_follicular_adenoma
        • canonical_molecular_tested_v1     braf_positive_canonical    -> braf_positive_final
        • complication_patient_summary_v1   n_analysis_eligible_complication
                                                                       -> any_analysis_eligible_complication

Phases:
    0  Snapshot canonical_patient_master -> canonical_patient_master_pre237_backup
    1  Add + derive ete_adjudicated_flag, ret_adjudicated_flag, ret_evidence_source
    2  Update registry feeds_master_columns text (4 rows)
    3  Re-verify: 0 registry→CPM pointer misses + Phase-236-style invariants
    4  Archive pre237 backup -> Thyroid 2026 UPdated.archive_pub_v1_0
    5  Run 5 Script-236 confirmation queries
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import datetime
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from motherduck_client import get_token  # noqa: E402

OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_DATE = "20260416"
SCRIPT_TAG = "Script 237"

N_EXPECTED_PATIENTS = 10_871

REGISTRY_FIXES: list[tuple[str, str, str]] = [
    # (detail_table_name, old_token, new_token)
    ("_molecular_patient_rollup_v227", "molecular_rollup_version", "rollup_script_version"),
    ("canonical_benign_diagnosis_v1",  "has_follicular_adenoma",   "syn_follicular_adenoma"),
    ("canonical_molecular_tested_v1",  "braf_positive_canonical",  "braf_positive_final"),
    ("complication_patient_summary_v1","n_analysis_eligible_complication",
                                       "any_analysis_eligible_complication"),
]


# ---------------------------------------------------------------------------
# Helpers (mirrors Script 236)
# ---------------------------------------------------------------------------


def banner(text: str) -> None:
    print("\n" + "=" * 78)
    print(text)
    print("=" * 78)


def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        raise RuntimeError("No MotherDuck token")
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


def column_exists(con: duckdb.DuckDBPyConnection, table: str, column: str) -> bool:
    row = con.execute(
        f"""SELECT 1 FROM information_schema.columns
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND table_name='{table}' AND column_name='{column}' LIMIT 1"""
    ).fetchone()
    return row is not None


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        f"""SELECT 1 FROM information_schema.tables
            WHERE table_catalog='{DB}' AND table_schema='{schema}' AND table_name='{table}' LIMIT 1"""
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Phase 0 — snapshot
# ---------------------------------------------------------------------------


def phase0(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 0 — Snapshot canonical_patient_master -> canonical_patient_master_pre237_backup")
    con.execute(
        """CREATE OR REPLACE TABLE canonical_patient_master_pre237_backup
           AS SELECT * FROM canonical_patient_master"""
    )
    n = con.execute("SELECT COUNT(*) FROM canonical_patient_master_pre237_backup").fetchone()[0]
    assert n == N_EXPECTED_PATIENTS, f"expected {N_EXPECTED_PATIENTS} rows, got {n}"
    print(f"  snapshot rows: {n}")


# ---------------------------------------------------------------------------
# Phase 1 — add + derive 3 missing columns
# ---------------------------------------------------------------------------


def phase1(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 1 — Add + derive 3 missing canonical columns")

    # --- 1A. ete_adjudicated_flag ---
    if not column_exists(con, "canonical_patient_master", "ete_adjudicated_flag"):
        con.execute(
            "ALTER TABLE canonical_patient_master ADD COLUMN ete_adjudicated_flag BOOLEAN"
        )
    con.execute("UPDATE canonical_patient_master SET ete_adjudicated_flag = FALSE")
    con.execute(
        """
        UPDATE canonical_patient_master cpm
        SET ete_adjudicated_flag = TRUE
        FROM (SELECT DISTINCT research_id FROM ete_adjudication_v1
              WHERE research_id IS NOT NULL) e
        WHERE cpm.research_id = e.research_id
        """
    )
    con.execute(
        f"""COMMENT ON COLUMN canonical_patient_master.ete_adjudicated_flag IS
            '{SCRIPT_TAG} (2026-04-16): TRUE if patient was LLM-adjudicated for ETE grade '
            'in ete_adjudication_v1 (Script 232). Use ete_grade_adjudicated / ete_grade_final_v2 '
            'for the adjudicated grade itself.'"""
    )
    n_ete = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE ete_adjudicated_flag = TRUE"
    ).fetchone()[0]
    print(f"  ete_adjudicated_flag=TRUE patients: {n_ete}  (expect 45)")
    assert n_ete == 45, f"expected 45 ete-adjudicated patients, got {n_ete}"

    # --- 1B. ret_adjudicated_flag ---
    if not column_exists(con, "canonical_patient_master", "ret_adjudicated_flag"):
        con.execute(
            "ALTER TABLE canonical_patient_master ADD COLUMN ret_adjudicated_flag BOOLEAN"
        )
    con.execute("UPDATE canonical_patient_master SET ret_adjudicated_flag = FALSE")
    con.execute(
        """
        UPDATE canonical_patient_master cpm
        SET ret_adjudicated_flag = TRUE
        FROM (SELECT DISTINCT research_id FROM ret_patient_adjudicated_v226
              WHERE research_id IS NOT NULL) r
        WHERE cpm.research_id = r.research_id
        """
    )
    con.execute(
        f"""COMMENT ON COLUMN canonical_patient_master.ret_adjudicated_flag IS
            '{SCRIPT_TAG} (2026-04-16): TRUE if patient was manually adjudicated for RET '
            'fusion in ret_patient_adjudicated_v226 (Script 226). Distinguishes "was reviewed" '
            'from the outcome flag ret_note_adjudicated_positive.'"""
    )
    n_ret = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE ret_adjudicated_flag = TRUE"
    ).fetchone()[0]
    print(f"  ret_adjudicated_flag=TRUE patients: {n_ret}  (expect 66)")
    assert n_ret == 66, f"expected 66 ret-adjudicated patients, got {n_ret}"

    # --- 1C. ret_evidence_source (provenance) ---
    if not column_exists(con, "canonical_patient_master", "ret_evidence_source"):
        con.execute(
            "ALTER TABLE canonical_patient_master ADD COLUMN ret_evidence_source VARCHAR"
        )
    # Use a CTE that combines ret_patient_adjudicated_v226 + _molecular_patient_rollup_v227.
    con.execute(
        """
        UPDATE canonical_patient_master cpm
        SET ret_evidence_source = CASE
            WHEN r.research_id IS NOT NULL AND r.ret_note_true_positive = TRUE
                THEN 'note_adjudicated_positive'
            WHEN r.research_id IS NOT NULL AND r.ret_note_true_positive = FALSE
                THEN 'note_adjudicated_negative'
            WHEN m.ret_positive_v7_new = TRUE
                THEN 'molecular_rollup_v227'
            WHEN cpm.ret_positive_v7 = TRUE
                THEN 'molecular_rollup_v7'
            ELSE NULL
          END
        FROM (SELECT * FROM canonical_patient_master) base
        LEFT JOIN ret_patient_adjudicated_v226 r ON r.research_id = base.research_id
        LEFT JOIN _molecular_patient_rollup_v227 m ON m.research_id = base.research_id
        WHERE cpm.research_id = base.research_id
        """
    )
    con.execute(
        f"""COMMENT ON COLUMN canonical_patient_master.ret_evidence_source IS
            '{SCRIPT_TAG} (2026-04-16): provenance of RET positivity signal. '
            'Values: note_adjudicated_positive (Script 226 manual review TP), '
            'note_adjudicated_negative (Script 226 manual review FP), '
            'molecular_rollup_v227 (Script 227 rollup), '
            'molecular_rollup_v7 (legacy v7 rollup), NULL (no evidence).'"""
    )
    dist = con.execute(
        """SELECT COALESCE(ret_evidence_source, '(null)') AS src, COUNT(*)
           FROM canonical_patient_master
           GROUP BY 1 ORDER BY 2 DESC"""
    ).fetchall()
    print("  ret_evidence_source distribution:")
    for s, n in dist:
        print(f"    {s:<30} {n}")


# ---------------------------------------------------------------------------
# Phase 2 — registry text fixes
# ---------------------------------------------------------------------------


def phase2(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 2 — Update detail_table_registry_v1 feeds_master_columns (4 rows)")

    for table_name, old, new in REGISTRY_FIXES:
        row = con.execute(
            """SELECT feeds_master_columns
               FROM manuscript_workspace.detail_table_registry_v1
               WHERE detail_table_name = ?""",
            [table_name],
        ).fetchone()
        if row is None:
            print(f"  SKIP (not in registry): {table_name}")
            continue
        feeds = row[0] or ""
        if old not in feeds:
            print(f"  NOTE: '{old}' not present in {table_name} feeds (maybe already fixed)")
            continue
        updated = feeds.replace(old, new)
        con.execute(
            """UPDATE manuscript_workspace.detail_table_registry_v1
               SET feeds_master_columns = ?
               WHERE detail_table_name = ?""",
            [updated, table_name],
        )
        print(f"  {table_name}")
        print(f"    - {old}")
        print(f"    + {new}")


# ---------------------------------------------------------------------------
# Phase 3 — re-verify
# ---------------------------------------------------------------------------


_SKIP_FEED_MARKERS = (
    "(", "no direct", "audit only", "provenance", "reference", "upstream",
    "crosslink", "crosswalk", "subset view", "manuscript-ready", "TODO",
    "dedup crosswalk", "specimen->assay", "specimen-level", "level-specific",
    "episode-level", "lesion-level", "exam-level", "component-level",
)


def _parse_feeds(s: str) -> list[str]:
    if not s:
        return []
    result: list[str] = []
    for tok in re.split(r"[;,\n]+", s):
        tok = tok.strip().rstrip(".;:")
        if not tok:
            continue
        if "*" in tok or "(" in tok or ")" in tok:
            continue
        if " " in tok or "\t" in tok:
            continue
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", tok):
            result.append(tok)
    return result


def phase3(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 3 — Re-verify registry→CPM pointers + canonical invariants")

    rows = con.execute(
        """SELECT detail_table_name, feeds_master_columns, domain
           FROM manuscript_workspace.detail_table_registry_v1
           ORDER BY detail_table_name"""
    ).fetchall()
    cpm_cols = {
        r[0]
        for r in con.execute(
            f"""SELECT column_name FROM information_schema.columns
                WHERE table_catalog='{DB}' AND table_schema='main'
                  AND table_name='canonical_patient_master'"""
        ).fetchall()
    }

    missing: list[tuple[str, str, str]] = []
    checked = 0
    for table_name, feeds, domain in rows:
        if not feeds:
            continue
        if any(m.lower() in feeds.lower() for m in _SKIP_FEED_MARKERS):
            continue
        if "*" in feeds:
            continue
        for col in _parse_feeds(feeds):
            checked += 1
            if col not in cpm_cols:
                missing.append((table_name, col, domain or ""))

    out_path = OUTPUT_DIR / "237_missing_canonical_columns.csv"
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["detail_table_name", "feeds_column", "domain"])
        for r in missing:
            w.writerow(r)
    print(f"  tokens checked: {checked}  missing: {len(missing)}")
    print(f"  wrote {out_path.name}")
    for tbl, col, dom in missing:
        print(f"    {tbl:<40} -> {col}  ({dom})")
    if missing:
        raise SystemExit(
            f"PHASE 3 FAILED — still {len(missing)} registry→CPM pointer gaps; see CSV."
        )
    print("  [PASS] 0 registry→CPM pointer gaps")

    # Canonical invariants (subset of Script 236 Phase 7)
    checks: list[tuple[str, bool]] = []
    checks.append((
        "canonical_patient_master row count == 10,871",
        con.execute("SELECT COUNT(*) FROM canonical_patient_master").fetchone()[0]
        == N_EXPECTED_PATIENTS,
    ))
    checks.append((
        "distinct research_id == 10,871",
        con.execute("SELECT COUNT(DISTINCT research_id) FROM canonical_patient_master").fetchone()[0]
        == N_EXPECTED_PATIENTS,
    ))
    checks.append((
        "no NULL research_id",
        con.execute(
            "SELECT COUNT(*) FROM canonical_patient_master WHERE research_id IS NULL"
        ).fetchone()[0] == 0,
    ))
    checks.append((
        "fna_path_outcome fully populated",
        con.execute(
            "SELECT COUNT(*) FROM canonical_patient_master WHERE fna_path_outcome IS NULL"
        ).fetchone()[0] == 0,
    ))
    checks.append((
        "ete_adjudicated_flag present",
        column_exists(con, "canonical_patient_master", "ete_adjudicated_flag"),
    ))
    checks.append((
        "ret_adjudicated_flag present",
        column_exists(con, "canonical_patient_master", "ret_adjudicated_flag"),
    ))
    checks.append((
        "ret_evidence_source present",
        column_exists(con, "canonical_patient_master", "ret_evidence_source"),
    ))

    all_ok = True
    for label, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
        if not ok:
            all_ok = False
    if not all_ok:
        raise SystemExit("PHASE 3 INVARIANTS FAILED — aborting before Phase 4.")


# ---------------------------------------------------------------------------
# Phase 4 — archive pre237 backup
# ---------------------------------------------------------------------------


def phase4(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 4 — Archive canonical_patient_master_pre237_backup")

    if not table_exists(con, "main", "canonical_patient_master_pre237_backup"):
        print("  nothing to archive")
        return
    dest = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.canonical_patient_master_pre237_backup_{ARCHIVE_DATE}'
    src_rc = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master_pre237_backup"
    ).fetchone()[0]
    con.execute(f"CREATE OR REPLACE TABLE {dest} AS SELECT * FROM canonical_patient_master_pre237_backup")
    dst_rc = con.execute(f"SELECT COUNT(*) FROM {dest}").fetchone()[0]
    assert src_rc == dst_rc, f"archive copy mismatch {src_rc} vs {dst_rc}"
    con.execute("DROP TABLE canonical_patient_master_pre237_backup")
    print(f"  archived {src_rc} rows -> canonical_patient_master_pre237_backup_{ARCHIVE_DATE}")

    n_backup = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND table_name LIKE '%pre%backup%'"""
    ).fetchone()[0]
    assert n_backup == 0, f"still have {n_backup} pre*_backup tables"

    # Rebuild __readme so the dropped pre237 backup row is gone and count still matches.
    print("  rebuilding __readme post-drop...")
    rebuild_readme(con)


def rebuild_readme(con: duckdb.DuckDBPyConnection) -> None:
    main_tables = [
        r[0]
        for r in con.execute(
            f"""SELECT table_name FROM information_schema.tables
                WHERE table_catalog='{DB}' AND table_schema='main'
                  AND table_type='BASE TABLE' ORDER BY table_name"""
        ).fetchall()
    ]
    existing = {
        r[0]: r[2]
        for r in con.execute("SELECT table_name, rows, description FROM __readme").fetchall()
    }
    rows: list[tuple[str, int, str]] = []
    for t in main_tables:
        n = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        desc = existing.get(t) or "TODO: describe"
        rows.append((t, n, desc))
    con.execute("DROP TABLE IF EXISTS __readme")
    con.execute("CREATE TABLE __readme (table_name VARCHAR, rows BIGINT, description VARCHAR)")
    con.executemany("INSERT INTO __readme VALUES (?,?,?)", rows)
    readme_n = con.execute("SELECT COUNT(*) FROM __readme").fetchone()[0]
    print(f"  __readme rows: {readme_n}")


# ---------------------------------------------------------------------------
# Phase 5 — run 5 confirmation queries
# ---------------------------------------------------------------------------


def phase5(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 5 — 5 confirmation queries")

    r = con.execute(
        f"""SELECT COUNT(*) AS patients,
                   (SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_catalog='{DB}' AND table_name='canonical_patient_master'
                      AND table_schema='main') AS columns
            FROM canonical_patient_master"""
    ).fetchone()
    print(f"  1) canonical shape: patients={r[0]}  columns={r[1]}")

    leftovers = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND (table_name LIKE '%pre%backup%'
                   OR table_name IN ('data_dictionary_v221','data_dictionary_v235'))"""
    ).fetchone()[0]
    print(f"  2) lingering backup/deprecated tables: {leftovers}  (expect 0)")

    r3 = con.execute(
        """SELECT COUNT(*) AS registered,
                   COUNT(*) FILTER (WHERE feeds_master_columns IS NULL OR feeds_master_columns = '') AS unmapped
           FROM manuscript_workspace.detail_table_registry_v1"""
    ).fetchone()
    print(f"  3) registry: registered={r3[0]}  unmapped={r3[1]}")

    q4a = con.execute("SELECT COUNT(*) FROM __readme").fetchone()[0]
    q4b = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{DB}' AND table_schema='main' AND table_type='BASE TABLE'"""
    ).fetchone()[0]
    print(f"  4) __readme rows={q4a}  BASE TABLEs={q4b}  (equal? {q4a==q4b})")

    r5 = con.execute(
        f"""SELECT
              (SELECT COUNT(*) FROM information_schema.columns
               WHERE table_catalog='{DB}' AND table_name='canonical_patient_master'
                 AND table_schema='main'
                 AND column_name LIKE 'comp\\_%\\_days\\_postop\\_v2' ESCAPE '\\') AS new_timing_cols,
              (SELECT COUNT(*) FROM information_schema.columns
               WHERE table_catalog='{DB}' AND table_name='canonical_patient_master'
                 AND table_schema='main'
                 AND column_name='nlp_path_multifocal_concordance_v2') AS new_multifocal_col,
              (SELECT COUNT(*) FROM manuscript_workspace.nlp_rollup_promotion_audit_v1) AS nlp_audit_domains,
              (SELECT COUNT(*) FROM information_schema.columns
               WHERE table_catalog='{DB}' AND table_name='canonical_patient_master'
                 AND table_schema='main'
                 AND column_name IN ('ete_adjudicated_flag','ret_adjudicated_flag','ret_evidence_source')) AS phase237_cols"""
    ).fetchone()
    print(
        f"  5) audit fixes: timing={r5[0]}  multifocal={r5[1]}  "
        f"nlp_audit={r5[2]}  phase237_cols={r5[3]} (expect 3)"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


PHASES = {
    "0": phase0,
    "1": phase1,
    "2": phase2,
    "3": phase3,
    "4": phase4,
    "5": phase5,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", default="all")
    args = ap.parse_args()

    con = connect()

    order = ["0", "1", "2", "3", "4", "5"] if args.phase == "all" else [
        p.strip() for p in args.phase.split(",")
    ]

    t0 = datetime.now()
    for p in order:
        if p not in PHASES:
            raise SystemExit(f"unknown phase: {p}")
        PHASES[p](con)
    print(f"\nTotal elapsed: {(datetime.now() - t0).total_seconds():.1f}s")


if __name__ == "__main__":
    main()
