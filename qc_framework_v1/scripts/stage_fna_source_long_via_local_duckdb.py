"""
Local-DuckDB direct upload of _source_long.parquet to MotherDuck.

Run AFTER signing out of any other MD account in the browser and signing
in as logan.glosser.eras@gmail.com (the account that holds
thyroid_canonical_publication_v1_0).

What this does:
  1. Connect via `duckdb.connect('md:')` -- triggers SSO if no cached token.
  2. Verify we can see thyroid_canonical_publication_v1_0.
  3. Read the local parquet at:
       verification_csvs/canonical_fna_events_v1/_source_long.parquet
     (8,120 rows, full long-form: research_id, fna_index, source_row,
      source_col_*, source_workbook, source_sheet, source_col_name_date,
      and 5 raw text fields)
  4. CREATE OR REPLACE table:
       manuscript_workspace.fna_source_long_v1_step_b
     with the FULL long-form contents (so the next 4 source-column
     compares -- specimen, path, history, bethesda -- can also use it).
  5. Print rowcount.

If we end up here it means we successfully bypassed the chunked-INSERT
approach -- no more 17-call paginated inserts via query_rw.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_PARQUET = REPO_ROOT / "verification_csvs" / "canonical_fna_events_v1" / "_source_long.parquet"
DEST_DB = "thyroid_canonical_publication_v1_0"
DEST_TABLE = "manuscript_workspace.fna_source_long_v1_step_b"


def main() -> None:
    if not SRC_PARQUET.exists():
        print(f"ERROR: parquet not found at {SRC_PARQUET}", file=sys.stderr)
        sys.exit(2)

    print(f"[stage] connecting to MotherDuck via 'md:' ...")
    con = duckdb.connect("md:")

    print(f"[stage] account databases visible:")
    for (name,) in con.execute("SHOW DATABASES").fetchall():
        marker = "  <-- TARGET" if name == DEST_DB else ""
        print(f"  - {name}{marker}")

    if DEST_DB not in [d for (d,) in con.execute("SHOW DATABASES").fetchall()]:
        print(
            f"\nERROR: {DEST_DB} not visible from this MD account.\n"
            f"You probably authed as the wrong account. Sign out at\n"
            f"  https://app.motherduck.com/\n"
            f"and re-run signed in as logan.glosser.eras@gmail.com.",
            file=sys.stderr,
        )
        sys.exit(2)

    con.execute(f"USE {DEST_DB}")
    print(f"[stage] using database {DEST_DB}")

    print(f"[stage] reading {SRC_PARQUET}")
    src_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{SRC_PARQUET}')"
    ).fetchone()[0]
    print(f"[stage] parquet row count: {src_count:,}")

    print(f"[stage] creating {DEST_TABLE} (DROP+CREATE for idempotence)")
    con.execute(f"DROP TABLE IF EXISTS {DEST_TABLE}")
    con.execute(
        f"""
        CREATE TABLE {DEST_TABLE} AS
        SELECT * FROM read_parquet('{SRC_PARQUET}')
        """
    )

    n = con.execute(f"SELECT COUNT(*) FROM {DEST_TABLE}").fetchone()[0]
    print(f"[stage] {DEST_TABLE} rowcount after load: {n:,}")
    print(
        f"[stage] columns: "
        f"{[c[0] for c in con.execute(f'DESCRIBE {DEST_TABLE}').fetchall()]}"
    )

    if n != src_count:
        print(
            f"WARNING: row count mismatch (parquet={src_count}, table={n})",
            file=sys.stderr,
        )
        sys.exit(2)

    print("[stage] done.")


if __name__ == "__main__":
    main()
