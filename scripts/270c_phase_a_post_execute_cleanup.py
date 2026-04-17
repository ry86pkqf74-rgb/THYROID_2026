#!/usr/bin/env python3
"""Script 270c — Phase A post-execute cleanup.

Tightens the audit-trail and convention surface immediately after
Step 2 --execute landed. Three writes (idempotent):

  1. UPDATE one audit row status from DOCUMENTED_NOOP -> DOCUMENTED_GAP
     (step_2_registry_null_residual). The 34-row residual is accepted-
     for-v1_1 open backlog, not a no-op.
  2. INSERT 2 new conventions: audit_status_taxonomy (codifies the
     status semantics so the noop-vs-gap distinction never gets
     relitigated) and tech_debt_aggregation (one entry per decision-
     to-be-made, not per row).
  3. INSERT 1 tech_debt row: registry_null_residual_v1_1 (one aggregate
     entry for the 34 homogeneous residual rows).

After this script lands, Phase A's final state is:
  conventions:     15
  tech_debt:        2
  keep_list:        2
  audit table:     43 rows, 4 DOCUMENTED_GAP (was 3), 3 DOCUMENTED_NOOP (was 4)

Idempotent: each write checks current state before applying. Re-runs
are no-ops with explicit log lines per skipped action.
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

OUT_JSON = OUT_DIR / "270c_post_execute_cleanup.json"
OUT_LOG = OUT_DIR / "270c_post_execute_cleanup.log"

WS = f'"{PUBLICATION_DB}".manuscript_workspace'
AUDIT_FQ = f'{WS}.v1_1_finalization_audit_v1'
CONVENTIONS_FQ = f'{WS}.__conventions'
TECH_DEBT_FQ = f'{WS}.v1_1_tech_debt_v1'

NEW_CONVENTIONS = [
    (
        "audit_status_taxonomy",
        "audit_discipline",
        "v1_1_finalization_audit_v1 status values have specific semantics: "
        "OK = check passed, no action needed; "
        "DOCUMENTED_NOOP = action evaluated, confirmed nothing to do; "
        "DOCUMENTED_GAP = known open work explicitly accepted for this "
        "release, tracked for v1_1+ closure; "
        "OK_DEFERRED_HUMAN = work routed to human review worksheet, no "
        "auto-apply; "
        "OK_BACKFILLED_<release> = retroactive audit row reconstructed "
        "post-hoc; "
        "FAIL = check failed, script halted. "
        "Reviewers filter by status to find release backlog (GAP), "
        "completed work (OK), or deferred human decisions "
        "(DEFERRED_HUMAN). Do not use NOOP for open work, or GAP for "
        "completed work.",
        "Script 270b step_2_registry_null_residual originally tagged "
        "NOOP, flipped to GAP post-landing (Script 270c) to match "
        "semantics of other v1_1-follow-up registry rows.",
        "script_270c_post_execute",
    ),
    (
        "tech_debt_aggregation",
        "tech_debt_discipline",
        "Tech debt entries in v1_1_tech_debt_v1 aggregate homogeneous "
        "backlogs into one entry. Per-row tech_debt entries create "
        "noise; one entry per *decision to be made* keeps the table "
        "actionable. If rows are heterogeneous (different decisions "
        "required per row), register separately.",
        "Script 270c registry_null_residual_v1_1 aggregates 34 "
        "homogeneous registry rows into one tech_debt entry rather "
        "than 34 per-row entries.",
        "script_270c_post_execute",
    ),
]

TECH_DEBT_ROW = (
    "registry_null_residual_v1_1",
    "registry_completeness",
    "34 rows in detail_table_registry_v1 carry NULL "
    "feeds_master_columns_normalized after Script 270b Step 2 execute. "
    "These are analysis tables, dictionaries, and cohort summaries that "
    "are registered because they are queryable main-schema base tables, "
    "but they do not feed canonical_patient_master columns. They are "
    "consumed downstream (Dives, manuscripts) or are governance-adjacent "
    "(dictionaries). Concrete list in "
    "scripts/output/270b_registry_null_post_execute.csv.",
    "Decide in v1_1 whether the registry should (a) drop non-feeder "
    "tables from detail_table_registry_v1 and register them in a "
    "separate downstream_consumer_registry_v1, or (b) keep them in "
    "the detail registry with a new relationship_type column "
    "('feeder' | 'downstream_consumer' | 'dictionary' | 'cohort_view'). "
    "Option (b) preserves a single registry; option (a) enforces the "
    "convention that the detail registry = feeders only.",
    "270b_step_2_post_execute",
    None,  # registered_at — set at runtime
    "v1_1",
    "OPEN",
    None,
    None,
)

NOTES_APPEND = (
    " | status flipped from DOCUMENTED_NOOP per audit_status_taxonomy "
    "convention (GAP = accepted-for-v1_1 open backlog; NOOP = confirmed-"
    "nothing-to-do)"
)


def main() -> int:
    log_lines: list[str] = []

    def log(msg: str) -> None:
        line = msg if msg.endswith("\n") else msg + "\n"
        log_lines.append(line)
        print(msg)

    started_at = datetime.now(timezone.utc)
    log(f"=== START Script 270c (post-execute cleanup) ===")
    log(f"started_at: {started_at.isoformat()}")

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # ----- 1. Pre-state -----
    log("\n--- pre-state ---")
    pre_status_dist = dict(con.execute(f"""
        SELECT status, COUNT(*) FROM {AUDIT_FQ}
        GROUP BY status ORDER BY 2 DESC
    """).fetchall())
    n_audit = con.execute(f"SELECT COUNT(*) FROM {AUDIT_FQ}").fetchone()[0]
    n_conventions = con.execute(f"SELECT COUNT(*) FROM {CONVENTIONS_FQ}").fetchone()[0]
    n_tech_debt = con.execute(f"SELECT COUNT(*) FROM {TECH_DEBT_FQ}").fetchone()[0]
    log(f"  audit rows:        {n_audit}")
    log(f"  audit status dist: {pre_status_dist}")
    log(f"  conventions:       {n_conventions}")
    log(f"  tech_debt:         {n_tech_debt}")

    # Find the target audit row
    target_row = con.execute(f"""
        SELECT script_num, status FROM {AUDIT_FQ}
        WHERE finding_id = 'step_2_registry_null_residual'
    """).fetchone()
    if not target_row:
        log("FAIL: step_2_registry_null_residual not found in audit table")
        OUT_LOG.write_text("".join(log_lines))
        return 1
    log(f"  target audit row found: script={target_row[0]} status={target_row[1]!r}")

    # ----- 2. Flip status (idempotent) -----
    log("\n--- flip step_2_registry_null_residual status ---")
    if target_row[1] == "DOCUMENTED_GAP":
        log("  already DOCUMENTED_GAP, skipping flip")
        flipped = False
    elif target_row[1] == "DOCUMENTED_NOOP":
        con.execute(f"""
            UPDATE {AUDIT_FQ}
            SET status = 'DOCUMENTED_GAP',
                notes = notes || ?
            WHERE finding_id = 'step_2_registry_null_residual'
              AND status = 'DOCUMENTED_NOOP'
        """, [NOTES_APPEND])
        new_status = con.execute(f"""
            SELECT status FROM {AUDIT_FQ}
            WHERE finding_id = 'step_2_registry_null_residual'
        """).fetchone()[0]
        log(f"  flipped DOCUMENTED_NOOP -> {new_status}")
        flipped = True
    else:
        log(f"FAIL: target row is in unexpected status {target_row[1]!r}; refusing")
        OUT_LOG.write_text("".join(log_lines))
        return 1

    # ----- 3. Insert 2 conventions (idempotent) -----
    log("\n--- insert 2 new conventions ---")
    convs_inserted = 0
    for conv in NEW_CONVENTIONS:
        cid = conv[0]
        n = con.execute(f"""
            SELECT COUNT(*) FROM {CONVENTIONS_FQ}
            WHERE convention_id = ?
        """, [cid]).fetchone()[0]
        if n:
            log(f"  convention {cid!r} already present, skipping")
            continue
        con.execute(f"""
            INSERT INTO {CONVENTIONS_FQ}
                (convention_id, category, rule, exemplar, established_in)
            VALUES (?, ?, ?, ?, ?)
        """, conv)
        convs_inserted += 1
        log(f"  inserted convention {cid!r}")

    # ----- 4. Insert 1 tech_debt row (idempotent) -----
    log("\n--- insert tech_debt row registry_null_residual_v1_1 ---")
    debt_id = TECH_DEBT_ROW[0]
    n = con.execute(f"""
        SELECT COUNT(*) FROM {TECH_DEBT_FQ}
        WHERE debt_id = ?
    """, [debt_id]).fetchone()[0]
    if n:
        log(f"  tech_debt {debt_id!r} already present, skipping")
        debt_inserted = False
    else:
        # Substitute registered_at timestamp
        row = list(TECH_DEBT_ROW)
        row[6] = started_at  # registered_at index in the original v1_1_tech_debt_v1 schema
        # Schema: debt_id, category, description, recommendation,
        #         registered_by, registered_at, target_version, status,
        #         resolved_at, resolved_by_script
        # Indices: 0,1,2,3,4,5,6,7,8,9 — registered_at is index 5
        row = list(TECH_DEBT_ROW)
        row[5] = started_at
        # row[6] is target_version, leave as 'v1_1'
        con.execute(f"""
            INSERT INTO {TECH_DEBT_FQ}
                (debt_id, category, description, recommendation,
                 registered_by, registered_at, target_version,
                 status, resolved_at, resolved_by_script)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row)
        debt_inserted = True
        log(f"  inserted tech_debt {debt_id!r}")

    # ----- 5. Post-state verification -----
    log("\n--- post-state verification ---")
    post_status_dist = dict(con.execute(f"""
        SELECT status, COUNT(*) FROM {AUDIT_FQ}
        GROUP BY status ORDER BY 2 DESC
    """).fetchall())
    n_audit_post = con.execute(f"SELECT COUNT(*) FROM {AUDIT_FQ}").fetchone()[0]
    n_conv_post = con.execute(f"SELECT COUNT(*) FROM {CONVENTIONS_FQ}").fetchone()[0]
    n_tech_debt_post = con.execute(f"SELECT COUNT(*) FROM {TECH_DEBT_FQ}").fetchone()[0]
    log(f"  audit rows:        {n_audit_post}")
    log(f"  audit status dist: {post_status_dist}")
    log(f"  conventions:       {n_conv_post}")
    log(f"  tech_debt:         {n_tech_debt_post}")

    # Hard assertions
    failed = []
    if n_conv_post != 15:
        failed.append(f"convention count {n_conv_post} != 15")
    if n_tech_debt_post != 2:
        failed.append(f"tech_debt count {n_tech_debt_post} != 2")
    if n_audit_post != n_audit:
        failed.append(f"audit row count changed unexpectedly {n_audit} -> {n_audit_post}")
    if post_status_dist.get("DOCUMENTED_GAP", 0) != 4:
        failed.append(
            f"DOCUMENTED_GAP count {post_status_dist.get('DOCUMENTED_GAP', 0)} != 4"
        )
    if failed:
        log("FAIL: post-state assertions:")
        for f in failed:
            log(f"  - {f}")
        OUT_LOG.write_text("".join(log_lines))
        return 1
    log("  pass: all post-state assertions hold")

    # ----- 6. Persist -----
    payload = {
        "started_at": started_at.isoformat(),
        "publication_db": PUBLICATION_DB,
        "actions": {
            "audit_status_flipped": flipped,
            "conventions_inserted": convs_inserted,
            "tech_debt_inserted": debt_inserted,
        },
        "pre_state": {
            "audit_rows": n_audit,
            "audit_status_distribution": {k: int(v) for k, v in pre_status_dist.items()},
            "conventions": n_conventions,
            "tech_debt": n_tech_debt,
        },
        "post_state": {
            "audit_rows": n_audit_post,
            "audit_status_distribution": {k: int(v) for k, v in post_status_dist.items()},
            "conventions": n_conv_post,
            "tech_debt": n_tech_debt_post,
        },
        "new_convention_ids": [c[0] for c in NEW_CONVENTIONS],
        "new_tech_debt_id": TECH_DEBT_ROW[0],
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str))
    OUT_LOG.write_text("".join(log_lines))
    log(f"\nwrote {OUT_JSON}")
    log(f"wrote {OUT_LOG}")
    log(f"=== END Script 270c ===")
    OUT_LOG.write_text("".join(log_lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
