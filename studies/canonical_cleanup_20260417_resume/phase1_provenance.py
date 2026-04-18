"""Insert per-phase provenance row for Phase 1.

Per Logan's request: insert a row NOW so Phase 1 outcomes are durably
logged before Phase 3 risks anything.

cpm_reconciliation_provenance_v1 schema (all VARCHAR for the
*_findings_cleared and held columns due to legacy):
  run_id, started_at, ended_at, phases_applied,
  critical_findings_cleared, high_findings_cleared,
  med_findings_cleared, held_for_adjudication
"""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

# Phase 1 wall-clock start (from phase1_run.log first line)
PHASE1_START = "2026-04-18 02:59:05.490551+00:00"
RUN_ID = "canonical_cleanup_resume_20260417_phase1"


def main() -> int:
    con = connect_locked()

    # Idempotency: delete any prior row with the same run_id, then insert.
    n_before = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "WHERE run_id = ?",
        [RUN_ID],
    ).fetchone()[0]
    if n_before:
        print(f"  Removing {n_before} prior row(s) for run_id={RUN_ID}")
        con.execute(
            "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
            "WHERE run_id = ?",
            [RUN_ID],
        )

    con.execute(
        """
        INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
          (run_id, started_at, ended_at, phases_applied,
           critical_findings_cleared, high_findings_cleared,
           med_findings_cleared, held_for_adjudication)
        VALUES (?, ?::TIMESTAMPTZ, CURRENT_TIMESTAMP,
                'hypopara_adjudication',
                '0', '0', '0', '4')
        """,
        [RUN_ID, PHASE1_START],
    )
    n = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1"
    ).fetchone()[0]
    row = con.execute(
        "SELECT * FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "WHERE run_id = ?",
        [RUN_ID],
    ).fetchone()
    cols = [d[0] for d in con.description]
    print(f"  cpm_reconciliation_provenance_v1 total rows now: {n}")
    print(f"  Inserted: {dict(zip(cols, row))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
