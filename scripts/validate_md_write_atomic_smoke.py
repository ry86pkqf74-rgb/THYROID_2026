#!/usr/bin/env python3
"""Local DuckDB smoke test: explicit transaction + ROLLBACK leaves no partial DDL.

Mirrors the BEGIN / COMMIT / ROLLBACK pattern used in 115/116/117 MotherDuck write
scripts (no cloud credentials required).

Usage:
  .venv/bin/python scripts/validate_md_write_atomic_smoke.py
"""
from __future__ import annotations

import duckdb


def main() -> int:
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS baseline")
    con.execute("CREATE TABLE baseline.intact(x INT)")
    con.execute("INSERT INTO baseline.intact VALUES (99)")

    print("  [smoke] BEGIN transactional DDL + injected failure → ROLLBACK")
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("CREATE SCHEMA staged")
        con.execute("CREATE TABLE staged.partial AS SELECT 1 AS v")
        con.execute("INSERT INTO staged.partial VALUES (2)")
        raise RuntimeError("injected failure before COMMIT")
    except RuntimeError as exc:
        print(f"  [smoke] caught: {exc}")
        con.execute("ROLLBACK")
        print("  [smoke] ROLLBACK completed (partial staged objects should be gone)")

    try:
        n = con.execute("SELECT COUNT(*) FROM staged.partial").fetchone()[0]
        print(f"  [smoke] FAIL: staged.partial still visible after ROLLBACK (rows={n})")
        return 1
    except Exception:
        print("  [smoke] OK: staged.partial absent after ROLLBACK")

    intact = con.execute("SELECT COUNT(*) FROM baseline.intact").fetchone()[0]
    if intact != 1:
        print(f"  [smoke] FAIL: baseline data corrupted (count={intact})")
        return 1

    print("  [smoke] OK: pre-transaction data preserved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
