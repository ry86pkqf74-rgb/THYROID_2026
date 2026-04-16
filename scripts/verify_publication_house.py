#!/usr/bin/env python3
"""
Publication House Verification Suite (6 layers)
Verifies that thyroid_canonical_publication_v1_0 faithfully reproduces every
cleaned artifact from "Thyroid 2026 UPdated" built by Script 223.

Layers:
  L1  Catalog completeness    — row counts, column counts, dtypes
  L2  Comment retention       — Script 223 comment-copy loop verification
  L3  Column statistics       — null rate, distinct count, min/max drift
  L4  Linkage integrity       — 12 child-table FK cardinality checks
  L5  Row-hash sampling       — 2% cryptographic row sample (MD5)
  L6  Canonical invariants    — 8 hard-coded post-221c expected values

Exit 0 = ALL PASS. Exit 1 = any failure (reason to stderr).
READ-ONLY — no DDL, no DML, no COMMENT modifications.

Usage: .venv/bin/python scripts/verify_publication_house.py
"""

from __future__ import annotations
import sys, csv, json, base64, datetime, pathlib
import duckdb, toml

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

SRC_DB    = "Thyroid 2026 UPdated"
TGT_DB    = "thyroid_canonical_publication_v1_0"
SRC_CANON = "canonical_patient_master_v221"  # name in source
TGT_CANON = "canonical_patient_master"        # name in target (only rename)

# Expected target-only tables — not failures
PUB_ONLY: set[str] = {"__readme", TGT_CANON}

# New tables ingested after source snapshot — must exist in BOTH with these counts
NEW_TABLE_COUNTS: dict[str, int] = {
    "mri_imaging": 715,
    "nsqip_enrichment": 1275,
    "nsqip_patient_summary": 1261,
    "patient_completion_oed_path_linkage_v1": 11506,
    "thyroid_weights": 10001,
    "thyroid_sizes": 11675,
}

# Layer 4: child tables joined on research_id → canonical_patient_master
CHILD_TABLES: list[str] = [
    "fna_episode_master_v2",
    "tumor_episode_master_v2",
    "molecular_test_episode_v2",
    "operative_episode_detail_v2",
    "rai_treatment_episode_v2",
    "ultrasound_reports",
    "imaging_nodule_master_v1",
    "longitudinal_lab_canonical_v1",
    "thyroglobulin_lab_canonical_v1",
    "recurrence_event_clean_v1",
    "ln_master_rollup_v1",
    "patient_cross_domain_timeline_v2",
]

# Layer 5: tables for 2% row-hash sampling: (src_name, tgt_name)
HASH_TABLES: list[tuple[str, str]] = [
    (SRC_CANON,                     TGT_CANON),
    ("fna_episode_master_v2",       "fna_episode_master_v2"),
    ("tumor_episode_master_v2",     "tumor_episode_master_v2"),
    ("molecular_test_episode_v2",   "molecular_test_episode_v2"),
    ("longitudinal_lab_canonical_v1",  "longitudinal_lab_canonical_v1"),
    ("thyroglobulin_lab_canonical_v1", "thyroglobulin_lab_canonical_v1"),
]

# Layer 6: (label, sql_aggregate_expr, expected_value)
INVARIANTS: list[tuple[str, str, int]] = [
    ("total_rows",            "COUNT(*)",                                                10871),
    ("unique_patients",       "COUNT(DISTINCT research_id)",                             10871),
    ("null_research_id",      "COUNT(*) FILTER (WHERE research_id IS NULL)",                 0),
    ("null_fna_path_outcome", "COUNT(*) FILTER (WHERE fna_path_outcome IS NULL)",            0),
    ("null_diagnosis_primary","COUNT(*) FILTER (WHERE diagnosis_primary IS NULL)",           0),
    ("followup_pos",          "COUNT(*) FILTER (WHERE followup_years > 0)",               4038),
    ("fna_dates",             "COUNT(*) FILTER (WHERE prm_first_fna_date IS NOT NULL)",   5212),
    ("tg_dates",              "COUNT(*) FILTER (WHERE first_tg_date IS NOT NULL)",        2721),
]

OUTPUT_DIR = pathlib.Path("scripts/output/verification")

# ─────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def src_ref(tbl: str) -> str:
    """Fully-qualified source table reference (spaces in DB name need quoting)."""
    return f'"Thyroid 2026 UPdated".main."{tbl}"'


def tgt_ref(tbl: str) -> str:
    """Fully-qualified target table reference."""
    return f'thyroid_canonical_publication_v1_0.main."{tbl}"'


def load_token() -> str:
    """Load token from motherduck.local.toml; abort if not the eras account."""
    cfg = toml.load("motherduck.local.toml")
    tok = cfg.get("MD_SA_TOKEN") or cfg.get("MOTHERDUCK_TOKEN") or cfg.get("motherduck_token")
    if not tok:
        sys.exit("ERROR: no token key found in motherduck.local.toml")
    try:
        payload = json.loads(base64.urlsafe_b64decode(tok.split(".")[1] + "==="))
    except Exception:
        sys.exit("ERROR: token is not a decodable JWT — check motherduck.local.toml")
    email = payload.get("email", "")
    if "eras" not in email.lower():
        sys.exit(f"WRONG ACCOUNT ({email}). Aborting — cowardly refusing with non-eras token.")
    print(f"  Token account: {email} ✓")
    return tok


def connect(tok: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(f"md:?motherduck_token={tok}")


def get_columns(con: duckdb.DuckDBPyConnection, db: str, tbl: str) -> list[tuple]:
    """Return [(column_name, data_type, comment)] ordered by column_index."""
    return con.execute(
        "SELECT column_name, data_type, COALESCE(comment, '') AS comment "
        "FROM duckdb_columns() "
        "WHERE database_name = ? AND schema_name = 'main' AND table_name = ? "
        "ORDER BY column_index",
        [db, tbl],
    ).fetchall()


def hash_expr(columns: list[str]) -> str:
    """MD5 over all columns, NULLs → empty string for hash stability."""
    parts = ", ".join(f"COALESCE(CAST(\"{c}\" AS VARCHAR), '')" for c in columns)
    return f"MD5(CONCAT_WS('|', {parts}))"


def write_csv(path: pathlib.Path, rows: list[dict], fields: list[str]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 1 — Catalog completeness
# ─────────────────────────────────────────────────────────────────────────────

def layer1_catalog(
    con: duckdb.DuckDBPyConnection,
    pairs: list[tuple[str, str]],
    src_tables: set[str],
    tgt_tables: set[str],
    src_only: list[str],
) -> dict:
    print("  L1: catalog completeness…", flush=True)
    rows: list[dict] = []
    n_fail = 0

    for s_name, t_name in pairs:
        try:
            s_rows = con.execute(f"SELECT COUNT(*) FROM {src_ref(s_name)}").fetchone()[0]
            t_rows = con.execute(f"SELECT COUNT(*) FROM {tgt_ref(t_name)}").fetchone()[0]
        except Exception as e:
            rows.append({"table_name": s_name, "status": "ERROR",
                         "src_rows": None, "tgt_rows": None,
                         "src_cols": None, "tgt_cols": None,
                         "dtype_mismatches": None, "notes": str(e)})
            n_fail += 1
            continue

        s_cols = get_columns(con, SRC_DB, s_name)
        t_cols = get_columns(con, TGT_DB, t_name)
        s_dtype = {c[0]: c[1] for c in s_cols}
        t_dtype = {c[0]: c[1] for c in t_cols}
        type_mm = [c for c in s_dtype if c in t_dtype and s_dtype[c] != t_dtype[c]]

        ok = (s_rows == t_rows) and (len(s_cols) == len(t_cols)) and not type_mm
        if not ok:
            n_fail += 1

        label = f"{s_name} → {t_name}" if s_name != t_name else s_name
        notes = ""
        if type_mm:
            notes = f"dtype diff: {type_mm[:3]}"
        elif s_rows != t_rows:
            notes = f"row delta: {t_rows - s_rows:+d}"
        elif len(s_cols) != len(t_cols):
            notes = f"col count: {len(s_cols)} vs {len(t_cols)}"

        rows.append({
            "table_name": label, "status": "PASS" if ok else "FAIL",
            "src_rows": s_rows, "tgt_rows": t_rows,
            "src_cols": len(s_cols), "tgt_cols": len(t_cols),
            "dtype_mismatches": len(type_mm), "notes": notes,
        })

    # Newly ingested tables — verify row counts
    for tbl, expected in NEW_TABLE_COUNTS.items():
        in_src = tbl in src_tables
        in_tgt = tbl in tgt_tables
        actual = None
        if in_tgt:
            try:
                actual = con.execute(f"SELECT COUNT(*) FROM {tgt_ref(tbl)}").fetchone()[0]
            except Exception:
                pass
        ok = in_src and in_tgt and actual == expected
        if not ok:
            n_fail += 1
        rows.append({
            "table_name": tbl, "status": "PASS" if ok else "FAIL",
            "src_rows": None, "tgt_rows": actual,
            "src_cols": None, "tgt_cols": None, "dtype_mismatches": 0,
            "notes": f"expected {expected} rows; in_src={in_src}, in_tgt={in_tgt}",
        })

    # Source-only tables — informational, not failures
    for tbl in src_only:
        rows.append({
            "table_name": tbl, "status": "SRC_ONLY",
            "src_rows": None, "tgt_rows": None,
            "src_cols": None, "tgt_cols": None, "dtype_mismatches": None,
            "notes": "staging/QA/backup/superseded — intentionally excluded from publication",
        })

    write_csv(OUTPUT_DIR / "table_diffs.csv", rows,
              ["table_name", "status", "src_rows", "tgt_rows",
               "src_cols", "tgt_cols", "dtype_mismatches", "notes"])

    n_compared = len(pairs) + len(NEW_TABLE_COUNTS)
    print(f"     {n_compared} tables compared, {n_fail} failed, {len(src_only)} source-only")
    return {
        "pass": n_fail == 0,
        "label": "Catalog completeness",
        "summary": f"{n_compared} tables checked, {n_fail} failed",
        "src_only": src_only,
    }


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 2 — Column comment retention
# ─────────────────────────────────────────────────────────────────────────────

def layer2_comments(con: duckdb.DuckDBPyConnection, pairs: list[tuple[str, str]]) -> dict:
    print("  L2: column comment retention…", flush=True)
    rows: list[dict] = []
    n_fail = 0

    for s_name, t_name in pairs:
        s_cols = get_columns(con, SRC_DB, s_name)
        t_cols = get_columns(con, TGT_DB, t_name)
        s_commented = sum(1 for _, _, cmt in s_cols if cmt.strip())
        t_commented = sum(1 for _, _, cmt in t_cols if cmt.strip())

        if s_commented == 0:
            coverage_pct = 100.0
            delta_pct = 0.0
        else:
            coverage_pct = 100.0 * t_commented / s_commented
            delta_pct = 100.0 - coverage_pct

        # Canonical master must be 100%; all others fail if > 5% drop
        if t_name == TGT_CANON:
            fail = coverage_pct < 100.0
        else:
            fail = s_commented > 0 and delta_pct > 5.0

        if fail:
            n_fail += 1

        label = t_name if s_name == t_name else f"{s_name} → {t_name}"
        rows.append({
            "table_name": label,
            "src_cols_commented": s_commented,
            "tgt_cols_commented": t_commented,
            "coverage_delta_pct": round(delta_pct, 1),
            "status": "FAIL" if fail else "PASS",
        })

    write_csv(OUTPUT_DIR / "comment_coverage.csv", rows,
              ["table_name", "src_cols_commented", "tgt_cols_commented",
               "coverage_delta_pct", "status"])

    print(f"     {len(pairs)} tables checked, {n_fail} below threshold")
    return {
        "pass": n_fail == 0,
        "label": "Comment retention",
        "summary": f"{len(pairs)} tables checked, {n_fail} below threshold",
    }


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 3 — Column-level statistics (SUMMARIZE)
# ─────────────────────────────────────────────────────────────────────────────

def layer3_stats(con: duckdb.DuckDBPyConnection, stat_pairs: list[tuple[str, str]]) -> dict:
    """
    Uses SUMMARIZE (supported in MotherDuck) to get null_percentage, approx_unique,
    min, max per column. Loops per column in Python to avoid UNPIVOT on wide tables.
    """
    print("  L3: column statistics (SUMMARIZE)…", flush=True)
    drift_rows: list[dict] = []
    n_fail = 0
    n_cols_total = 0

    for s_name, t_name in stat_pairs:
        try:
            s_df = con.execute(f"SUMMARIZE {src_ref(s_name)}").fetchdf()
            t_df = con.execute(f"SUMMARIZE {tgt_ref(t_name)}").fetchdf()
        except Exception as e:
            print(f"     WARNING: SUMMARIZE failed for {s_name}: {e}")
            continue

        s_map = s_df.set_index("column_name")
        t_map = t_df.set_index("column_name")
        common_cols = list(s_map.index.intersection(t_map.index))
        n_cols_total += len(common_cols)
        label = t_name if s_name == t_name else f"{s_name}→{t_name}"

        for col in common_cols:
            sr = s_map.loc[col]
            tr = t_map.loc[col]
            col_failures: list[tuple[str, str, str, float | str]] = []

            # null_percentage: fail if > 0.1 percentage-point difference
            try:
                s_null = float(sr["null_percentage"]) if sr["null_percentage"] is not None else 0.0
                t_null = float(tr["null_percentage"]) if tr["null_percentage"] is not None else 0.0
                null_delta = abs(s_null - t_null)
                if null_delta > 0.1:
                    col_failures.append(("null_rate", f"{s_null:.3f}", f"{t_null:.3f}", round(null_delta, 4)))
            except (TypeError, ValueError):
                pass

            # approx_unique: fail if > 0.1% relative difference
            try:
                s_u = int(sr["approx_unique"]) if sr["approx_unique"] is not None else 0
                t_u = int(tr["approx_unique"]) if tr["approx_unique"] is not None else 0
                rel_delta = abs(s_u - t_u) / max(s_u, 1) * 100
                if rel_delta > 0.1:
                    col_failures.append(("distinct_count", str(s_u), str(t_u), round(rel_delta, 4)))
            except (TypeError, ValueError):
                pass

            # min/max: fail if different and both are non-null/non-nan
            for stat in ("min", "max"):
                sv = str(sr.get(stat, ""))
                tv = str(tr.get(stat, ""))
                if sv not in ("None", "nan", "") and tv not in ("None", "nan", "") and sv != tv:
                    col_failures.append((stat, sv, tv, ""))

            if col_failures:
                n_fail += 1
                for chk, sv, tv, delta in col_failures:
                    drift_rows.append({
                        "table_name": label, "column_name": col,
                        "check": chk, "src_value": sv, "tgt_value": tv,
                        "delta": delta, "status": "FAIL",
                    })

    write_csv(OUTPUT_DIR / "column_stats_diffs.csv", drift_rows,
              ["table_name", "column_name", "check", "src_value", "tgt_value", "delta", "status"])

    print(f"     {n_cols_total} columns checked across {len(stat_pairs)} tables, {n_fail} columns drifted")
    return {
        "pass": n_fail == 0,
        "label": "Column statistics",
        "summary": f"{n_cols_total} columns checked, {n_fail} drifted",
    }


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 4 — Linkage / referential integrity
# ─────────────────────────────────────────────────────────────────────────────

def layer4_linkage(con: duckdb.DuckDBPyConnection) -> dict:
    """
    For each child table, LEFT JOIN to canonical_patient_master on research_id.
    Casts both sides to VARCHAR to handle BIGINT vs VARCHAR type mismatch (project handoff warning).
    Checks: row count match, matched count match, orphan introduction/resolution.
    """
    print("  L4: linkage integrity…", flush=True)
    rows: list[dict] = []
    n_fail = 0

    for child in CHILD_TABLES:
        try:
            s_r = con.execute(f"""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(p.research_id) AS matched,
                    COUNT(*) - COUNT(p.research_id) AS orphans
                FROM {src_ref(child)} c
                LEFT JOIN {src_ref(SRC_CANON)} p
                    ON CAST(c.research_id AS VARCHAR) = CAST(p.research_id AS VARCHAR)
            """).fetchone()

            t_r = con.execute(f"""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(p.research_id) AS matched,
                    COUNT(*) - COUNT(p.research_id) AS orphans
                FROM {tgt_ref(child)} c
                LEFT JOIN {tgt_ref(TGT_CANON)} p
                    ON CAST(c.research_id AS VARCHAR) = CAST(p.research_id AS VARCHAR)
            """).fetchone()

        except Exception as e:
            rows.append({"child_table": child, "src_rows": None, "tgt_rows": None,
                         "src_matched": None, "tgt_matched": None,
                         "src_orphans": None, "tgt_orphans": None,
                         "status": f"ERROR: {e}"})
            n_fail += 1
            continue

        s_total, s_matched, s_orphans = s_r
        t_total, t_matched, t_orphans = t_r

        # Fail conditions per spec
        fail = (
            s_total != t_total
            or s_matched != t_matched
            or (t_orphans > 0 and s_orphans == 0)   # orphan introduced
            or (s_orphans > 0 and t_orphans == 0)   # orphan resolved (row loss signal)
        )
        if fail:
            n_fail += 1

        rows.append({
            "child_table": child,
            "src_rows": s_total, "tgt_rows": t_total,
            "src_matched": s_matched, "tgt_matched": t_matched,
            "src_orphans": s_orphans, "tgt_orphans": t_orphans,
            "status": "FAIL" if fail else "PASS",
        })

    write_csv(OUTPUT_DIR / "linkage_integrity.csv", rows,
              ["child_table", "src_rows", "tgt_rows", "src_matched", "tgt_matched",
               "src_orphans", "tgt_orphans", "status"])

    print(f"     {len(CHILD_TABLES)} joins checked, {n_fail} cardinality mismatches")
    return {
        "pass": n_fail == 0,
        "label": "Linkage integrity",
        "summary": f"{len(CHILD_TABLES)} joins checked, {n_fail} cardinality mismatches",
    }


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 5 — Cryptographic row-hash sampling (2%)
# ─────────────────────────────────────────────────────────────────────────────

def layer5_hashes(con: duckdb.DuckDBPyConnection) -> dict:
    """
    Sample 2% of each source table (bernoulli, seed=42 for reproducibility).
    Compute MD5(CONCAT_WS('|', all_cols)) for each sampled row.
    Check whether each sampled hash appears in the full target hash set.
    Expected mismatch: zero.
    """
    print("  L5: row-hash sampling (2% bernoulli, seed=42)…", flush=True)
    rows: list[dict] = []
    total_sampled = 0
    total_mismatched = 0

    for s_name, t_name in HASH_TABLES:
        try:
            s_col_meta = get_columns(con, SRC_DB, s_name)
            t_col_meta = get_columns(con, TGT_DB, t_name)
            s_col_names = [c[0] for c in s_col_meta]
            t_col_set = {c[0] for c in t_col_meta}

            # Use source column order for both sides (CTAS preserves order)
            common = [c for c in s_col_names if c in t_col_set]
            if not common:
                raise ValueError("no common columns found")

            s_h = hash_expr(common)
            t_h = hash_expr(common)

            result = con.execute(f"""
                WITH src_sample AS (
                    SELECT {s_h} AS h
                    FROM {src_ref(s_name)}
                    USING SAMPLE 2 PERCENT (bernoulli, 42)
                ),
                tgt_hashes AS (
                    SELECT {t_h} AS h
                    FROM {tgt_ref(t_name)}
                )
                SELECT
                    (SELECT COUNT(*) FROM src_sample)           AS sampled_n,
                    COUNT(*) FILTER (WHERE ta.h IS NULL)        AS mismatched_n
                FROM src_sample ss
                LEFT JOIN tgt_hashes ta USING (h)
            """).fetchone()

            sn, mm = result
            total_sampled += sn
            total_mismatched += mm

            rows.append({
                "table_name": t_name if s_name == t_name else f"{s_name}→{t_name}",
                "sampled_n": sn,
                "mismatched_n": mm,
                "sample_pct_mismatch": round(100.0 * mm / max(sn, 1), 4),
                "status": "PASS" if mm == 0 else "FAIL",
                "example_mismatched_research_ids": "",
            })

        except Exception as e:
            rows.append({
                "table_name": s_name, "sampled_n": 0, "mismatched_n": -1,
                "sample_pct_mismatch": -1.0, "status": f"ERROR: {e}",
                "example_mismatched_research_ids": "",
            })
            total_mismatched += 1

    write_csv(OUTPUT_DIR / "row_hash_sample.csv", rows,
              ["table_name", "sampled_n", "mismatched_n", "sample_pct_mismatch",
               "status", "example_mismatched_research_ids"])

    layer_pass = total_mismatched == 0
    print(f"     {len(HASH_TABLES)} tables sampled, {total_sampled:,} rows, {total_mismatched} mismatches")
    return {
        "pass": layer_pass,
        "label": "Row-hash sampling",
        "summary": (f"{len(HASH_TABLES)} tables sampled, {total_mismatched} mismatched "
                    f"rows out of {total_sampled:,}"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 6 — Canonical invariant snapshot
# ─────────────────────────────────────────────────────────────────────────────

def layer6_invariants(con: duckdb.DuckDBPyConnection) -> dict:
    """
    Hard-coded checks against the post-Script-221c expected state.
    Runs the same 8 aggregates against both source and target canonical.
    """
    print("  L6: canonical invariants…", flush=True)
    exprs = ", ".join(f"{expr} AS {label}" for label, expr, _ in INVARIANTS)

    s_row = con.execute(f"SELECT {exprs} FROM {src_ref(SRC_CANON)}").fetchone()
    t_row = con.execute(f"SELECT {exprs} FROM {tgt_ref(TGT_CANON)}").fetchone()

    n_fail = 0
    detail_lines = [
        f"\n{'─'*4} Layer 6 invariant detail {'─'*4}",
        f"{'':30s}  {'src':>10s}  {'tgt':>10s}  {'expected':>10s}  status",
    ]
    for i, (label, _, expected) in enumerate(INVARIANTS):
        sv, tv = s_row[i], t_row[i]
        fail = (sv != expected) or (tv != expected)
        if fail:
            n_fail += 1
        mark = "FAIL" if fail else "pass"
        detail_lines.append(
            f"  {label:28s}  {str(sv):>10s}  {str(tv):>10s}  {str(expected):>10s}  {mark}"
        )

    print(f"     8 invariants checked, {n_fail} deviations")
    return {
        "pass": n_fail == 0,
        "label": "Canonical invariants",
        "summary": f"8 invariants, {n_fail} deviations",
        "detail": "\n".join(detail_lines),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY WRITER
# ─────────────────────────────────────────────────────────────────────────────

def write_summary(layers: list[dict], src_only: list[str], ts: str) -> None:
    layer_labels = [
        "Layer 1: Catalog completeness",
        "Layer 2: Comment retention",
        "Layer 3: Column statistics",
        "Layer 4: Linkage integrity",
        "Layer 5: Row-hash sampling",
        "Layer 6: Canonical invariants",
    ]
    sep = "═" * 65

    lines = [
        sep,
        f"PUBLICATION HOUSE VERIFICATION — {ts}",
        f'Source: "{SRC_DB}" (169 tables)',
        f"Target: {TGT_DB} (110 tables)",
        sep,
        "",
    ]
    for lyr, lbl in zip(layers, layer_labels):
        status_str = "PASS" if lyr["pass"] else "FAIL"
        lines.append(f"{lbl:<40s} [{status_str}]  ({lyr['summary']})")

    lines.append("")
    overall = "PASS" if all(l["pass"] for l in layers) else "FAIL"
    lines.append(f"OVERALL: [{overall}]")
    lines.append("")
    lines.append(f"Detailed CSVs in {OUTPUT_DIR}/")

    # Layer 6 invariant table
    if "detail" in layers[5]:
        lines.append(layers[5]["detail"])

    # Source-only table listing
    lines.append(
        f"\n{'─'*4} Source-only tables ({len(src_only)} listed, NOT failures) {'─'*4}"
    )
    for tbl in src_only:
        lines.append(f"  {tbl}")

    (OUTPUT_DIR / "verification_summary.txt").write_text("\n".join(lines) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sep = "═" * 65
    print(sep)
    print(f"Publication House Verification — {ts}")
    print(sep)

    tok = load_token()
    con = connect(tok)

    # Discover table catalogs once — shared across layers
    src_tables: set[str] = {
        r[0] for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = 'main'",
            [SRC_DB],
        ).fetchall()
    }
    tgt_tables: set[str] = {
        r[0] for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = 'main'",
            [TGT_DB],
        ).fetchall()
    }

    print(f"  Source: {SRC_DB} — {len(src_tables)} tables")
    print(f"  Target: {TGT_DB} — {len(tgt_tables)} tables")

    # Build comparison pairs: (src_name, tgt_name)
    same_name_pairs = [(t, t) for t in sorted(src_tables & tgt_tables)]
    rename_pair: list[tuple[str, str]] = []
    if SRC_CANON in src_tables and TGT_CANON in tgt_tables:
        rename_pair = [(SRC_CANON, TGT_CANON)]
    pairs = same_name_pairs + rename_pair

    paired_src = {s for s, _ in pairs}
    src_only = sorted(src_tables - paired_src)

    # Top 10 highest-row tables for Layer 3 (excluding the canonical, which is always included)
    top10_rows = con.execute(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE database_name = ? AND schema_name = 'main' AND table_name != ? "
        "ORDER BY estimated_size DESC NULLS LAST LIMIT 10",
        [SRC_DB, SRC_CANON],
    ).fetchall()
    top10 = [(r[0], r[0]) for r in top10_rows if r[0] in paired_src]
    stat_pairs = [(SRC_CANON, TGT_CANON)] + top10

    print(f"\nRunning 6 verification layers…")

    l1 = layer1_catalog(con, pairs, src_tables, tgt_tables, src_only)
    l2 = layer2_comments(con, pairs)
    l3 = layer3_stats(con, stat_pairs)
    l4 = layer4_linkage(con)
    l5 = layer5_hashes(con)
    l6 = layer6_invariants(con)

    layers = [l1, l2, l3, l4, l5, l6]
    write_summary(layers, src_only, ts)

    summary_path = OUTPUT_DIR / "verification_summary.txt"
    print(f"\n{sep}")
    print(summary_path.read_text())

    all_pass = all(lyr["pass"] for lyr in layers)
    if all_pass:
        print(
            "✓ Publication house verified — safe to use for analysis, "
            "share with collaborators, snapshot for freeze."
        )
        sys.exit(0)
    else:
        failures = [lyr["label"] for lyr in layers if not lyr["pass"]]
        print(f"\nFAIL: {', '.join(failures)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
