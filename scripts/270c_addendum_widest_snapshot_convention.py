#!/usr/bin/env python3
"""Script 270c addendum — register convention restore_test_prefers_widest_schema.

Codifies the post-270c-review fix: the round-trip restore test in
Phase B planning/execute scripts must select the widest available
snapshot (most columns first, ties broken by most rows). Round-tripping
a narrow snapshot under-exercises type, ordering, and length-inference
edge cases. The 270c initial run picked
canonical_patient_master_prev233_snapshot (7 cols) by lex DESC sort over
the much wider canonical_patient_master_pre270_* (1499 cols) — a latent
bug, not an acceptable substitute.

Idempotent: aborts if convention_id already present. Conventions table
goes 15 -> 16.

Mode: writes one row to manuscript_workspace.__conventions, no other
side effects.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSON = OUT_DIR / "270c_addendum_widest_snapshot_convention.json"
OUT_LOG = OUT_DIR / "270c_addendum_widest_snapshot_convention.log"

WS = f'"{PUBLICATION_DB}".manuscript_workspace'
CONVENTIONS_FQ = f"{WS}.__conventions"

CONVENTION_ID = "restore_test_prefers_widest_schema"

CONVENTION_ROW = (
    CONVENTION_ID,
    "archival_discipline",
    (
        "Round-trip restore tests select the archive target with the "
        "widest schema available (highest column count preferred, then "
        "highest row count). Narrow tests give false confidence; wide "
        "tests exercise type, ordering, and length-inference edge cases. "
        "Concretely: prefer canonical_patient_master_pre270_* (1499 cols) "
        "over canonical_patient_master_prev233_snapshot (7 cols), and "
        "any snapshot with explicit column counts > the median snapshot "
        "width over snapshots picked by lex sort."
    ),
    (
        "Script 270c v1 selected canonical_patient_master_prev233_"
        "snapshot_20260417T010115Z by ORDER BY table_name DESC, getting "
        "a 7-col round-trip instead of the available 1499-col pre270 "
        "CPM snapshot. Test passed but exercised a fraction of the "
        "schema surface that Phase B execute will touch. 270d enforces "
        "the wide-first selector."
    ),
    "script_270c_post_review",
)


def main() -> int:
    log_lines: list[str] = []

    def log(msg: str) -> None:
        line = msg if msg.endswith("\n") else msg + "\n"
        log_lines.append(line)
        print(msg)

    started_at = datetime.now(timezone.utc)
    log("=== START 270c addendum — widest-snapshot convention ===")
    log(f"started_at: {started_at.isoformat()}")

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    n_pre = con.execute(f"SELECT COUNT(*) FROM {CONVENTIONS_FQ}").fetchone()[0]
    log(f"  conventions pre-state: {n_pre}")

    existing = con.execute(
        f"SELECT COUNT(*) FROM {CONVENTIONS_FQ} WHERE convention_id = ?",
        [CONVENTION_ID],
    ).fetchone()[0]
    if existing:
        log(f"  convention {CONVENTION_ID!r} already present — no-op")
        inserted = False
    else:
        con.execute(
            f"""
            INSERT INTO {CONVENTIONS_FQ}
                (convention_id, category, rule, exemplar, established_in)
            VALUES (?, ?, ?, ?, ?)
            """,
            CONVENTION_ROW,
        )
        log(f"  inserted convention {CONVENTION_ID!r}")
        inserted = True

    n_post = con.execute(f"SELECT COUNT(*) FROM {CONVENTIONS_FQ}").fetchone()[0]
    log(f"  conventions post-state: {n_post}")

    if n_post != 16:
        log(
            f"WARNING: post-state convention count {n_post} != 16 expected. "
            "Check whether prior addendums also ran or whether Phase A baseline "
            "was different from 15."
        )

    payload = {
        "started_at": started_at.isoformat(),
        "publication_db": PUBLICATION_DB,
        "convention_id": CONVENTION_ID,
        "inserted": inserted,
        "conventions_pre": int(n_pre),
        "conventions_post": int(n_post),
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str))
    OUT_LOG.write_text("".join(log_lines))
    log(f"  wrote {OUT_JSON}")
    log(f"  wrote {OUT_LOG}")
    log("=== END 270c addendum ===")
    OUT_LOG.write_text("".join(log_lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
