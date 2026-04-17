#!/usr/bin/env python3
"""Script 270c — document divergent reclassification in MotherDuck.

Writes:
  1. One audit row to v1_1_finalization_audit_v1 explaining the
     reclassification of the 3 DIVERGENT rows to DROP_ALREADY_SNAPSHOTTED.
  2. One tech_debt row (stray_subset_matcher_v1_1) describing the v1_1
     matcher improvement: "stray ⊆ snapshot" should auto-classify as
     DROP_ALREADY_SNAPSHOTTED without human review.

Idempotent: skips each write if finding_id / debt_id already present.
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

OUT_JSON = OUT_DIR / "270c_divergent_reclassification_audit.json"
OUT_LOG = OUT_DIR / "270c_divergent_reclassification_audit.log"

WS = f'"{PUBLICATION_DB}".manuscript_workspace'
AUDIT_FQ = f"{WS}.v1_1_finalization_audit_v1"
TECH_DEBT_FQ = f"{WS}.v1_1_tech_debt_v1"

AUDIT_FINDING_ID = "divergent_reclassified_to_drop_3_rows"

AUDIT_ROW = (
    "270c",
    AUDIT_FINDING_ID,
    "divergent_rows_reclassified",
    3,   # count_before
    0,   # count_after
    0,   # target_after
    "OK",
    (
        "3 rows initially tagged DIVERGENT by the 270c snapshot-suffix "
        "matcher (canonical_diagnosis_unified_v1 stray=11259 vs snap=11028; "
        "ln_master_rollup_v1 stray=4290 vs snap=4273; serial_imaging_us "
        "stray=0 vs snap=4162) were reclassified to DROP_ALREADY_SNAPSHOTTED "
        "after Claude direct-query MotherDuck verification: "
        "(a) canonical == snapshot for all three (byte-equivalent row counts "
        "and distinct RIDs); "
        "(b) the stray extras on rows 1-2 are true same-source duplicates "
        "verified via source_table='tumor_pathology' equality — stray is a "
        "pre-dedup leftover; "
        "(c) row 3 is an empty shell with 0 rows. "
        "No unique information in stray for any of the three. "
        "See tech_debt stray_subset_matcher_v1_1 for v1_1 improvement to "
        "auto-classify stale-subset strays without requiring human review."
    ),
)

TECH_DEBT_ID = "stray_subset_matcher_v1_1"

TECH_DEBT_ROW = (
    TECH_DEBT_ID,
    "archival_discipline",
    (
        "The 270c name-collision matcher flags row-count inequality as "
        "DIVERGENT, requiring human review. This is correct for truly "
        "divergent content but over-triggers on the common case where stray "
        "is a stale pre-dedup or pre-purge subset/superset of the snapshot, "
        "which already captures the relevant clinical state. In Phase B, "
        "3 of 3 DIVERGENT rows were actually stale-stray safe to drop after "
        "a manual MotherDuck verification session."
    ),
    (
        "In v1_1, extend matcher to compute (a) is canonical == snapshot "
        "(same row count, same distinct RIDs), and (b) is stray content a "
        "strict subset of snapshot (same RIDs, same source_table, same "
        "clinical columns up to duplication). If both hold, classify as "
        "DROP_ALREADY_SNAPSHOTTED_STALE_STRAY automatically and record "
        "the evidence in the audit row notes. Reserve DIVERGENT for cases "
        "where canonical != snapshot OR stray has content provably absent "
        "from both (genuinely new rows with distinct RIDs not present in "
        "canonical or snapshot)."
    ),
    "270c_phase_b_post_divergent_review",
    None,  # registered_at — set at runtime
    "v1_1",
    "OPEN",
    None,  # resolved_at
    None,  # resolved_by_script
)


def main() -> int:
    log_lines: list[str] = []

    def log(msg: str) -> None:
        line = msg if msg.endswith("\n") else msg + "\n"
        log_lines.append(line)
        print(msg)

    started_at = datetime.now(timezone.utc)
    log("=== START 270c divergent reclassification audit ===")
    log(f"started_at: {started_at.isoformat()}")

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # 1. Audit row
    log("\n--- audit row ---")
    existing_audit = con.execute(
        f"SELECT COUNT(*) FROM {AUDIT_FQ} WHERE finding_id = ?",
        [AUDIT_FINDING_ID],
    ).fetchone()[0]
    if existing_audit:
        log(f"  audit row {AUDIT_FINDING_ID!r} already present — skipping")
        audit_inserted = False
    else:
        con.execute(
            f"""
            INSERT INTO {AUDIT_FQ}
                (run_ts, script_num, finding_id, metric,
                 count_before, count_after, target_after, status, notes)
            VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            AUDIT_ROW,
        )
        audit_inserted = True
        log(f"  inserted audit row finding_id={AUDIT_FINDING_ID!r}")

    # 2. Tech debt row
    log("\n--- tech_debt row ---")
    existing_td = con.execute(
        f"SELECT COUNT(*) FROM {TECH_DEBT_FQ} WHERE debt_id = ?",
        [TECH_DEBT_ID],
    ).fetchone()[0]
    if existing_td:
        log(f"  tech_debt {TECH_DEBT_ID!r} already present — skipping")
        td_inserted = False
    else:
        row = list(TECH_DEBT_ROW)
        row[5] = started_at  # registered_at
        con.execute(
            f"""
            INSERT INTO {TECH_DEBT_FQ}
                (debt_id, category, description, recommendation,
                 registered_by, registered_at, target_version,
                 status, resolved_at, resolved_by_script)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row,
        )
        td_inserted = True
        log(f"  inserted tech_debt {TECH_DEBT_ID!r}")

    # 3. Verify post-state
    n_audit = con.execute(f"SELECT COUNT(*) FROM {AUDIT_FQ}").fetchone()[0]
    n_td = con.execute(f"SELECT COUNT(*) FROM {TECH_DEBT_FQ}").fetchone()[0]
    log(f"\n  audit table rows: {n_audit}")
    log(f"  tech_debt rows:   {n_td}")
    if n_td != 3:
        log(f"  WARNING: tech_debt count {n_td} != 3 expected "
            "(prior tech_debt rows: laterality_bare_column_name_v1_1, "
            "registry_null_residual_v1_1, stray_subset_matcher_v1_1)")

    payload = {
        "started_at": started_at.isoformat(),
        "publication_db": PUBLICATION_DB,
        "audit_row_inserted": audit_inserted,
        "tech_debt_row_inserted": td_inserted,
        "audit_finding_id": AUDIT_FINDING_ID,
        "tech_debt_id": TECH_DEBT_ID,
        "audit_table_rows_post": int(n_audit),
        "tech_debt_rows_post": int(n_td),
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str))
    OUT_LOG.write_text("".join(log_lines))
    log(f"\n  wrote {OUT_JSON}")
    log(f"  wrote {OUT_LOG}")
    log("=== END 270c divergent reclassification audit ===")
    OUT_LOG.write_text("".join(log_lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
