"""Prune stale pre362_* archive snapshots from archive_pub_v1_0.

Keeps the parity-matched set: pre362_20260422_005646 (the snapshot whose
row count was verified equal to live operative_episode_detail_v2 at drop
time, per the Step 5 archive-parity gate in Script 362).
"""
from __future__ import annotations

import sys

sys.path.insert(0, "/Users/ros/THyroid 2026")
sys.path.insert(0, "/Users/ros/THyroid 2026/scripts")

from _md_connect import connect_locked

KEEP_TS = "20260422_005646"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
EXPECTED_KEEP_COUNT = 1  # only operative_episode_detail_v2 archived in Script 362

con = connect_locked()
rows = con.execute(
    """
    SELECT table_name FROM information_schema.tables
    WHERE table_catalog = ? AND table_schema = ?
      AND table_name LIKE '%_pre362_%'
    ORDER BY table_name
    """,
    [ARCHIVE_DB, ARCHIVE_SCHEMA],
).fetchall()

drop_targets: list[str] = []
keep_targets: list[str] = []
for (tn,) in rows:
    (keep_targets if KEEP_TS in tn else drop_targets).append(tn)

print(f"Found {len(rows)} pre362_* archives total")
print(f"  KEEP ({KEEP_TS}): {len(keep_targets)} table(s)")
print(f"  DROP: {len(drop_targets)} table(s)")
print()

if len(keep_targets) != EXPECTED_KEEP_COUNT:
    raise SystemExit(
        f"REFUSING: keep set has {len(keep_targets)} tables, expected "
        f"{EXPECTED_KEEP_COUNT}. Aborting prune to preserve safety net."
    )

print(f"Keep set verified: {EXPECTED_KEEP_COUNT} table(s).")
print()
print("Dropping stale snapshots:")
for t in drop_targets:
    fq = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"."{t}"'
    n = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
    print(f"  DROP {t} ({n:,} rows)")
    con.execute(f"DROP TABLE {fq}")
print()

remaining = con.execute(
    """
    SELECT table_name FROM information_schema.tables
    WHERE table_catalog = ? AND table_schema = ?
      AND table_name LIKE '%_pre362_%'
    ORDER BY table_name
    """,
    [ARCHIVE_DB, ARCHIVE_SCHEMA],
).fetchall()
print(f"After prune: {len(remaining)} pre362_* archives remain")
for (tn,) in remaining:
    print(f"  {tn}")
