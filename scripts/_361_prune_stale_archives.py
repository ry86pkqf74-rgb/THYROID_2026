"""Prune 4 stale pre361_* archive sets from archive_pub_v1_0.

Keeps the parity-matched set: pre361_20260422_002245 (28 DROP TABLE)
"""
from __future__ import annotations

import sys

sys.path.insert(0, "/Users/ros/THyroid 2026")
sys.path.insert(0, "/Users/ros/THyroid 2026/scripts")

from _md_connect import connect_locked

KEEP_TS = "20260422_002245"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"

con = connect_locked()

# Find all pre361_* in archive_pub_v1_0
rows = con.execute(
    """
    SELECT table_name FROM information_schema.tables
    WHERE table_catalog = ? AND table_schema = ?
      AND table_name LIKE '%_pre361_%'
    ORDER BY table_name
    """,
    [ARCHIVE_DB, ARCHIVE_SCHEMA],
).fetchall()

drop_targets: list[str] = []
keep_targets: list[str] = []
for (tn,) in rows:
    if KEEP_TS in tn:
        keep_targets.append(tn)
    else:
        drop_targets.append(tn)

print(f"Found {len(rows)} pre361_* archives total")
print(f"  KEEP ({KEEP_TS}): {len(keep_targets)} tables")
print(f"  DROP (4 stale build sets): {len(drop_targets)} tables")
print()

if len(keep_targets) != 7:
    raise SystemExit(
        f"REFUSING: keep set has {len(keep_targets)} tables, expected 7. "
        f"Aborting prune to preserve safety net."
    )

print("Keep set verified: 7 tables (one per deprecated source).")
print()
print("Dropping stale snapshots:")
for t in drop_targets:
    fq = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"."{t}"'
    n = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
    print(f"  DROP {t} ({n:,} rows)")
    con.execute(f"DROP TABLE {fq}")
print()

# Verify final state
remaining = con.execute(
    """
    SELECT table_name FROM information_schema.tables
    WHERE table_catalog = ? AND table_schema = ?
      AND table_name LIKE '%_pre361_%'
    ORDER BY table_name
    """,
    [ARCHIVE_DB, ARCHIVE_SCHEMA],
).fetchall()
print(f"After prune: {len(remaining)} pre361_* archives remain")
for (tn,) in remaining:
    print(f"  {tn}")
