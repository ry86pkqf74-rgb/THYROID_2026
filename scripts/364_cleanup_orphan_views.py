#!/usr/bin/env python3
"""Script 364 final cleanup — drop 1 orphan table + 2 stale views.

Three operations to fully close out the Script 364 cascade:
  1. Snapshot main.survival_cohort_enriched (61,134 rows / 10,507 patients,
     28 cols) to archive_pub_v1_0.survival_cohort_enriched_pre364_20260422_054500
  2. Drop the broken view views_readable."Complications" (referenced
     deprecated complication tables that were dropped in --phase 7)
  3. Drop the stale view views_readable."Survival_Cohort_Enriched" and the
     underlying main.survival_cohort_enriched (legacy survival pipeline
     superseded by canonical_survival_followup_v1 from Script 364B)

Idempotent: the snapshot uses CREATE TABLE (errors if the snapshot already
exists), the drops use DROP IF EXISTS.

PHI rule: research_id only.

Usage::

    python scripts/364_cleanup_orphan_views.py --dry-run
    python scripts/364_cleanup_orphan_views.py --commit
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from motherduck_client import get_token, token_mode  # noqa: E402

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
SNAPSHOT_NAME = "survival_cohort_enriched_pre364_20260422_054500"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Drop orphan complications/survival objects after 364 cascade"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--commit", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    do_writes = bool(args.commit)

    tok = get_token()
    if not tok:
        raise SystemExit(f"No MotherDuck RW token (mode={token_mode()})")
    print(f"Connecting md:{CANONICAL_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")
    con.execute(f'USE "{CANONICAL_DB}"')

    # 1. Snapshot the orphan table.
    src_fq = f'"{CANONICAL_DB}"."main"."survival_cohort_enriched"'
    dst_fq = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"."{SNAPSHOT_NAME}"'
    src_n = con.execute(f"SELECT COUNT(*) FROM {src_fq}").fetchone()[0]
    print(f"  source main.survival_cohort_enriched: {src_n:,} rows")
    print(f"  snapshot plan: {SNAPSHOT_NAME}")

    if not do_writes:
        print("  [dry-run] skipping snapshot + drops")
        return 0

    # Idempotency: refuse to overwrite an existing snapshot.
    already = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [ARCHIVE_DB, ARCHIVE_SCHEMA, SNAPSHOT_NAME],
    ).fetchone()
    if already:
        n_dst = con.execute(f"SELECT COUNT(*) FROM {dst_fq}").fetchone()[0]
        if n_dst != src_n:
            raise RuntimeError(
                f"Existing snapshot {SNAPSHOT_NAME} has {n_dst:,} rows but "
                f"live has {src_n:,}. Refusing to overwrite."
            )
        print(f"  snapshot already exists: {SNAPSHOT_NAME} ({n_dst:,} rows)")
    else:
        con.execute(f"CREATE TABLE {dst_fq} AS SELECT * FROM {src_fq}")
        n_dst = con.execute(f"SELECT COUNT(*) FROM {dst_fq}").fetchone()[0]
        if n_dst != src_n:
            raise RuntimeError(
                f"Snapshot row count mismatch: src={src_n:,} dst={n_dst:,}"
            )
        try:
            con.execute(
                f"COMMENT ON TABLE {dst_fq} IS "
                f"'Script 364 final cleanup (2026-04-22) snapshot of "
                f"main.survival_cohort_enriched before drop.'"
            )
        except Exception as exc:
            print(f"  (COMMENT ON failed, non-fatal: {exc})")
        print(f"  snapshot created: {SNAPSHOT_NAME} ({n_dst:,} rows)")

    # 2-4. Drop the 3 objects.
    targets = [
        ("VIEW",  f'"{CANONICAL_DB}"."views_readable"."Complications"'),
        ("VIEW",  f'"{CANONICAL_DB}"."views_readable"."Survival_Cohort_Enriched"'),
        ("TABLE", src_fq),
    ]
    for kind, name in targets:
        print(f"  DROP {kind} IF EXISTS {name}")
        con.execute(f"DROP {kind} IF EXISTS {name}")

    print()
    print("Verifying drops:")
    for table_schema, table_name in (
        ("views_readable", "Complications"),
        ("views_readable", "Survival_Cohort_Enriched"),
        ("main",          "survival_cohort_enriched"),
    ):
        gone = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
            [CANONICAL_DB, table_schema, table_name],
        ).fetchone()[0]
        status = "GONE ✓" if gone == 0 else "STILL PRESENT ✗"
        print(f"  {table_schema}.{table_name}: {status}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
