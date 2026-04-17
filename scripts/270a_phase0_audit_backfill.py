#!/usr/bin/env python3
"""Script 270a (Phase 0) — backfill v1_1_finalization_audit_v1 rows for 267/268/269.

Scripts 267, 268, 269 ran successfully on 2026-04-17 (commit 8c971e6) but did
NOT emit rows into manuscript_workspace.v1_1_finalization_audit_v1. This
breaks the audit-trail invariant. Script 270 Phase A's gate I requires every
row in that table to have status IN ('OK','DOCUMENTED_NOOP','HUMAN_REVIEW_REQUIRED');
without these backfilled rows the gate trivially passes only because the rows
don't exist — which is dishonest.

This script:
  1. Snapshots v1_1_finalization_audit_v1 before any write
  2. Inserts 3 rows with status='OK_BACKFILLED_270' (a NEW status value
     introduced precisely to mark retroactive entries; downstream gates
     should treat it the same as 'OK' but a reviewer can see at a glance
     that these are reconstructions)
  3. Verifies post-state row count = pre-state + 3
  4. Emits scripts/output/270a_audit_backfill.json + .log

The actual deltas (count_before / count_after) are reconstructed from each
script's run-log + the committed source. Notes include the git commit SHA
and the original run timestamp so a future reviewer can chase provenance.

Idempotent: re-running detects existing 'backfill_267_*' / 'backfill_268_*' /
'backfill_269_*' rows and aborts with a clear message.
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

OUT_JSON = OUT_DIR / "270a_audit_backfill.json"
OUT_LOG = OUT_DIR / "270a_audit_backfill.log"

AUDIT_FQ = (
    f'"{PUBLICATION_DB}".manuscript_workspace.v1_1_finalization_audit_v1'
)

GIT_COMMIT_SHA = "8c971e6c8c961ca5797d6faf7452da4b507da82a"
GIT_COMMIT_TS = "2026-04-17T05:13:01Z"

# (script_num, finding_id, metric, count_before, count_after, target_after,
#  status, notes, run_ts_iso)
BACKFILL_ROWS: list[tuple] = [
    (
        "267",
        "backfill_267_drop_legacy_molecular_cols",
        "cpm_column_count",
        1495,
        1491,
        1491,
        "OK_BACKFILLED_270",
        (
            "Audit row backfilled by Script 270 Phase 0 because the original "
            "Script 267 did not emit. Column deltas reconstructed from "
            "scripts/output/267_run.log and 267_post.json. Dropped 4 legacy "
            "molecular columns: molecular_tested_v7, mol_test_count, "
            "molecular_platforms_v7, n_molecular_tests_v7. Pinned feeders "
            "(molecular_tested_confirmed, mol_n_tests, mol_platform) remain "
            "authoritative. 65/65 manuscript_workspace views still compile. "
            f"git_commit={GIT_COMMIT_SHA[:12]} commit_ts={GIT_COMMIT_TS} "
            "run_ts=2026-04-17T04:53:40Z."
        ),
        "2026-04-17T04:53:40+00:00",
    ),
    (
        "268",
        "backfill_268_bethesda_semantics",
        "cpm_column_count",
        1491,
        1499,
        1499,
        "OK_BACKFILLED_270",
        (
            "Audit row backfilled by Script 270 Phase 0 because the original "
            "Script 268 did not emit. Column deltas reconstructed from "
            "scripts/output/268_run.log and 268_decision_log.json. Locked "
            "convention 'bethesda_semantics' (preop_worst_calculated_from_"
            "morphology_era_preserved). Added 9 bethesda_* cols to CPM "
            "(bethesda_final, bethesda_max_preop_2010/2015/2023, n_bethesda_"
            "calculated_fnas, n_bethesda_number_only_fnas, bethesda_derivation_"
            "methods, bethesda_index_nodule, bethesda_index_nodule_linkage_"
            "source) populated on 5,037 patients. Updated detail_table_"
            "registry_v1 normalized pins for fna_cytology + fna_episode_"
            "master_v2 + extracted_fna_bethesda_v1 + specimen_tumor_focus_v1 "
            "+ imaging_fna_linkage_v3. canonical_detail_pointer_v1 confirms "
            "every Bethesda column has exactly 1 authoritative feeder. "
            f"git_commit={GIT_COMMIT_SHA[:12]} commit_ts={GIT_COMMIT_TS} "
            "run_ts=2026-04-17T04:58:58Z."
        ),
        "2026-04-17T04:58:58+00:00",
    ),
    (
        "269",
        "backfill_269_molecular_episode_backfill",
        "molecular_test_episode_v2_rows",
        10125,
        10650,
        10650,
        "OK_BACKFILLED_270",
        (
            "Audit row backfilled by Script 270 Phase 0 because the original "
            "Script 269 did not emit. Row deltas reconstructed from "
            "scripts/output/269_run.log and 269_decision_log.json. Added "
            "ingestion_source VARCHAR column to molecular_test_episode_v2; "
            "backfilled 525 episodes (within budget [500,555]): ThyroSeq=443, "
            "NGS-BRAF=46, RET=36. Concordance vs pinned feeders 94.82% — "
            "intentional NOT-fail; pinned feeders remain authoritative per "
            "v1_0 decision. Live mte_v2 row count verified at 10,650 by "
            "Script 270 baseline probe. "
            f"git_commit={GIT_COMMIT_SHA[:12]} commit_ts={GIT_COMMIT_TS} "
            "run_ts=2026-04-17T05:01:19Z."
        ),
        "2026-04-17T05:01:19+00:00",
    ),
]


def main() -> int:
    log_lines: list[str] = []

    def log(msg: str) -> None:
        line = msg if msg.endswith("\n") else msg + "\n"
        log_lines.append(line)
        print(msg)

    log("=== START Script 270a (Phase 0 audit backfill) ===")
    log(f"started_at: {datetime.now(timezone.utc).isoformat()}")

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # Idempotency guard
    existing = con.execute(f"""
        SELECT finding_id FROM {AUDIT_FQ}
        WHERE finding_id IN ({','.join(repr(r[1]) for r in BACKFILL_ROWS)})
    """).fetchall()
    if existing:
        log(f"ABORT: {len(existing)} backfill rows already present:")
        for (f,) in existing:
            log(f"  {f}")
        log("Script 270a is idempotent — refusing to insert duplicates.")
        OUT_LOG.write_text("".join(log_lines))
        return 0

    # Pre-state snapshot
    n_pre = con.execute(f"SELECT COUNT(*) FROM {AUDIT_FQ}").fetchone()[0]
    log("\n--- PRE-STATE ---")
    log(f"  audit table rows: {n_pre}")

    pre_status_dist = dict(con.execute(f"""
        SELECT status, COUNT(*) FROM {AUDIT_FQ}
        GROUP BY status ORDER BY 2 DESC
    """).fetchall())
    log(f"  pre status distribution: {pre_status_dist}")

    # INSERT
    log("\n--- INSERT 3 BACKFILL ROWS ---")
    for row in BACKFILL_ROWS:
        run_ts_iso = row[8]
        params = (
            run_ts_iso, row[0], row[1], row[2], row[3], row[4], row[5],
            row[6], row[7],
        )
        con.execute(f"""
            INSERT INTO {AUDIT_FQ}
                (run_ts, script_num, finding_id, metric,
                 count_before, count_after, target_after, status, notes)
            VALUES (CAST(? AS TIMESTAMP), ?, ?, ?, ?, ?, ?, ?, ?)
        """, params)
        log(f"  inserted: script={row[0]} finding_id={row[1]!r} "
            f"metric={row[2]!r} {row[3]}->{row[4]} status={row[6]!r}")

    # Post-state assertions
    n_post = con.execute(f"SELECT COUNT(*) FROM {AUDIT_FQ}").fetchone()[0]
    log("\n--- POST-STATE ---")
    log(f"  audit table rows: {n_post}")
    expected_post = n_pre + len(BACKFILL_ROWS)
    if n_post != expected_post:
        log(f"FAIL: expected {expected_post} rows post-insert, got {n_post}")
        OUT_LOG.write_text("".join(log_lines))
        return 1
    log(f"  pass: {n_pre} + {len(BACKFILL_ROWS)} = {n_post}")

    # Verify each row landed correctly
    landed = con.execute(f"""
        SELECT script_num, finding_id, count_before, count_after, status
        FROM {AUDIT_FQ}
        WHERE finding_id IN ({','.join(repr(r[1]) for r in BACKFILL_ROWS)})
        ORDER BY script_num
    """).fetchall()
    log(f"  landed rows ({len(landed)}):")
    for r in landed:
        log(f"    {r}")
    if len(landed) != len(BACKFILL_ROWS):
        log(f"FAIL: expected {len(BACKFILL_ROWS)} landed, got {len(landed)}")
        OUT_LOG.write_text("".join(log_lines))
        return 1

    # Status distribution after
    post_status_dist = dict(con.execute(f"""
        SELECT status, COUNT(*) FROM {AUDIT_FQ}
        GROUP BY status ORDER BY 2 DESC
    """).fetchall())
    log(f"  post status distribution: {post_status_dist}")

    # Persist
    payload = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "publication_db": PUBLICATION_DB,
        "audit_table": AUDIT_FQ,
        "git_commit": GIT_COMMIT_SHA,
        "git_commit_ts": GIT_COMMIT_TS,
        "rows_pre": n_pre,
        "rows_post": n_post,
        "rows_inserted": len(BACKFILL_ROWS),
        "pre_status_distribution": {k: int(v) for k, v in pre_status_dist.items()},
        "post_status_distribution": {k: int(v) for k, v in post_status_dist.items()},
        "backfill_rows": [
            {
                "script_num": r[0],
                "finding_id": r[1],
                "metric": r[2],
                "count_before": r[3],
                "count_after": r[4],
                "target_after": r[5],
                "status": r[6],
                "run_ts": r[8],
            }
            for r in BACKFILL_ROWS
        ],
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str))
    OUT_LOG.write_text("".join(log_lines))
    log(f"\nwrote {OUT_JSON}")
    log(f"wrote {OUT_LOG}")
    log("=== END Script 270a ===")
    OUT_LOG.write_text("".join(log_lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
