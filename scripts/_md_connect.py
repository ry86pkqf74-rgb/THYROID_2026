"""
Shared MotherDuck connection helper for scripts 230-233.

Creates a duckdb connection and issues USE thyroid_canonical_publication_v1_0
so unqualified table names always resolve to the publication canonical
database — avoiding cross-database name collisions when multiple databases
(e.g. 'Thyroid 2026 UPdated') are attached to the same account.
"""
import os
import sys

import duckdb

PUBLICATION_DB = "thyroid_canonical_publication_v1_0"


def get_motherduck_token():
    """Prefer the repo's motherduck_client.get_token if available, else env var."""
    try:
        # Add repo root to path
        here = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.dirname(here)
        sys.path.insert(0, repo_root)
        from motherduck_client import get_token  # type: ignore
        return get_token()
    except ImportError:
        tok = os.environ.get("MOTHERDUCK_TOKEN")
        if not tok:
            raise SystemExit(
                "No MotherDuck token. Set MOTHERDUCK_TOKEN or provide motherduck_client.py"
            )
        return tok


def connect_locked():
    """Open a connection with the search path locked to the publication DB.

    All unqualified table names resolve to
    thyroid_canonical_publication_v1_0.main.*.

    Also asserts that the publication DB is attached and that the expected
    source tables exist and are non-duplicative.
    """
    token = get_motherduck_token()
    con = duckdb.connect(f"md:{PUBLICATION_DB}?motherduck_token={token}")

    # Lock search path
    con.execute(f'USE "{PUBLICATION_DB}"')
    con.execute(f'USE "{PUBLICATION_DB}".main')

    # Integrity check: confirm the publication database is attached
    dbs = {r[0] for r in con.execute(
        "SELECT database_name FROM duckdb_databases()").fetchall()}
    if PUBLICATION_DB not in dbs:
        raise SystemExit(f"Expected database '{PUBLICATION_DB}' is not attached")

    # Integrity check: confirm canonical_patient_master resolves as a single
    # table with the expected row count
    row = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) "
        f"FROM {PUBLICATION_DB}.main.canonical_patient_master"
    ).fetchone()
    n_rows, n_distinct = row
    if n_rows != 10871:
        raise SystemExit(
            f"canonical_patient_master row count {n_rows} != 10871. "
            f"Aborting to avoid corruption."
        )
    if n_distinct != 10871:
        raise SystemExit(
            f"canonical_patient_master has {n_rows - n_distinct} duplicate "
            f"research_id rows. Aborting."
        )
    return con


def assert_row_count(con, table_fq: str, expected: int, tolerance: int = 0):
    """Assert fully-qualified table has exactly `expected` rows (±tolerance)."""
    n = con.execute(f"SELECT COUNT(*) FROM {table_fq}").fetchone()[0]
    low, high = expected - tolerance, expected + tolerance
    if not (low <= n <= high):
        raise SystemExit(
            f"Row count assertion failed: {table_fq} has {n} rows, "
            f"expected {expected}±{tolerance}"
        )
    return n


def assert_distinct_rids(con, table_fq: str, rid_col: str = "research_id"):
    """Assert no duplicate research_id values in a table."""
    row = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT {rid_col}) FROM {table_fq}"
    ).fetchone()
    if row[0] != row[1]:
        raise SystemExit(
            f"{table_fq} has {row[0] - row[1]} duplicate {rid_col} rows"
        )
    return row[0]
