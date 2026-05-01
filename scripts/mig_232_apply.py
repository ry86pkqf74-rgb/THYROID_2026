#!/usr/bin/env python3
"""
mig_232 apply script — narrow ACR-missing view (CF-mig219 follow-up)

Applies qc_framework_v1/migrations/232_vw_us_nodule_tirads_derived_acr_missing_20260501.sql
to thyroid_canonical_publication_v1_0 on MotherDuck.

Expected post-apply row count: 7,200 – 7,400
(Copilot crosstab: any_reported_descriptor_incomplete_and_derived_missing = 7,304)

Usage:
    .venv/bin/python scripts/mig_232_apply.py
"""

import sys
import pathlib
from urllib.parse import quote_plus

# ---------------------------------------------------------------------------
# Repo root — consistent with all other mig scripts
# ---------------------------------------------------------------------------
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import duckdb
from motherduck_client import get_token  # noqa: E402


def connect_md() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        raise RuntimeError("No MotherDuck token found — set MOTHERDUCK_TOKEN or MD_SA_TOKEN")
    q_tok = quote_plus(token)
    con = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={q_tok}")
    return con


def run() -> None:
    print("=== mig_232 apply — narrow ACR-missing view ===")

    sql_path = REPO_ROOT / "qc_framework_v1" / "migrations" / "232_vw_us_nodule_tirads_derived_acr_missing_20260501.sql"
    if not sql_path.exists():
        print(f"[ERROR] SQL file not found: {sql_path}", file=sys.stderr)
        sys.exit(1)

    sql_text = sql_path.read_text()

    # Split on statement boundaries (semicolons NOT inside string literals)
    # Simple split on '; -- ' or trailing ';' is sufficient for this file
    # since there are no semicolons inside string values.
    import re
    # Strip comment-only lines and blank lines, then split on statement-ending semicolons
    statements = [s.strip() for s in re.split(r";\s*\n", sql_text) if s.strip()]
    # Filter out pure-comment blocks (start with --)
    statements = [s for s in statements if s and not all(
        line.lstrip().startswith("--") or line.strip() == ""
        for line in s.splitlines()
    )]

    print("[INFO] Connecting to MotherDuck …")
    con = connect_md()
    print("[INFO] Connected.")

    # ---------------------------------------------------------------------------
    # Execute each statement
    # ---------------------------------------------------------------------------
    EXPECTED_MIN = 7200
    EXPECTED_MAX = 7400

    for i, stmt in enumerate(statements, 1):
        # Skip USE statements — handled implicitly by connection URL
        if stmt.upper().lstrip().startswith("USE "):
            print(f"[SKIP] Statement {i}: USE (no-op in MotherDuck SDK)")
            continue
        preview = stmt[:80].replace("\n", " ")
        print(f"[RUN ] Statement {i}: {preview} …")
        try:
            con.execute(stmt)
            print(f"[OK  ] Statement {i} succeeded.")
        except Exception as exc:
            print(f"[ERROR] Statement {i} failed: {exc}", file=sys.stderr)
            print(f"       Full statement:\n{stmt[:400]}", file=sys.stderr)
            con.close()
            sys.exit(1)

    # ---------------------------------------------------------------------------
    # §1 Verify row count
    # ---------------------------------------------------------------------------
    print("\n--- Post-apply verification ---")
    row = con.execute(
        "SELECT COUNT(*) AS n "
        "FROM thyroid_canonical_publication_v1_0.manuscript_workspace"
        ".vw_us_nodule_tirads_derived_acr_missing_VIEW_v1"
    ).fetchone()
    n = row[0] if row else -1
    print(f"[CHECK] vw_us_nodule_tirads_derived_acr_missing_VIEW_v1 row count = {n}")
    if EXPECTED_MIN <= n <= EXPECTED_MAX:
        print(f"[PASS ] Row count {n} is within expected range [{EXPECTED_MIN}, {EXPECTED_MAX}].")
    else:
        print(
            f"[WARN ] Row count {n} is OUTSIDE expected range [{EXPECTED_MIN}, {EXPECTED_MAX}]. "
            "Surface delta to Logan before using in manuscript.",
            file=sys.stderr,
        )

    # ---------------------------------------------------------------------------
    # §2 Verify gate1 count
    # ---------------------------------------------------------------------------
    gate1_row = con.execute(
        "SELECT COUNT(*) AS total, COUNT(DISTINCT (schema_name, table_name)) AS distinct_cnt "
        "FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1 "
        "WHERE table_status = 'verified'"
    ).fetchone()
    gate1_total, gate1_distinct = (gate1_row[0], gate1_row[1]) if gate1_row else (-1, -1)
    print(f"[CHECK] 5-gate gate1: total={gate1_total}, distinct={gate1_distinct}")
    if gate1_total == gate1_distinct:
        print(f"[PASS ] gate1_total == gate1_distinct ({gate1_total}) — no duplicates.")
    else:
        print(f"[WARN ] gate1_total ({gate1_total}) != gate1_distinct ({gate1_distinct}) — duplicate rows present.")

    # ---------------------------------------------------------------------------
    # §3 Confirm provenance row inserted
    # ---------------------------------------------------------------------------
    prov_row = con.execute(
        "SELECT run_id, started_at "
        "FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "WHERE run_id = 'mig_232_narrow_acr_v15' "
        "ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    if prov_row:
        print(f"[PASS ] Provenance row found: run_id={prov_row[0]}, started_at={prov_row[1]}")
    else:
        print("[WARN ] Provenance row NOT found — check §E INSERT.", file=sys.stderr)

    # ---------------------------------------------------------------------------
    # §4 Confirm col registry
    # ---------------------------------------------------------------------------
    col_row = con.execute(
        "SELECT COUNT(*) AS n "
        "FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1 "
        "WHERE schema_name = 'manuscript_workspace' "
        "  AND table_name  = 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1'"
    ).fetchone()
    n_cols = col_row[0] if col_row else -1
    print(f"[CHECK] Column registry rows for this view: {n_cols}")
    if n_cols == 11:
        print("[PASS ] 11 column rows registered as expected.")
    else:
        print(f"[WARN ] Expected 11 col rows, found {n_cols}.", file=sys.stderr)

    con.close()
    print("\n=== mig_232 apply complete ===")
    print(f"View row count     : {n}")
    print(f"Gate1 total/distinct: {gate1_total}/{gate1_distinct}")
    print(f"Col registry rows  : {n_cols}")


if __name__ == "__main__":
    run()
