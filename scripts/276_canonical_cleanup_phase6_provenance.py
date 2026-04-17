"""Canonical cleanup 20260417 — Phase 6 (build provenance).

  ALTER TABLE main.canonical_patient_master
    ADD COLUMN IF NOT EXISTS cpm_built_at TIMESTAMP;
  UPDATE main.canonical_patient_master SET cpm_built_at = CURRENT_TIMESTAMP;

Plus manuscript_workspace.cpm_reconciliation_provenance_v1 with one row per run.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HERE = REPO / "studies" / "canonical_cleanup_20260417"
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

LOG_PATH = HERE / "phase6_run.log"
DECISIONS_PATH = HERE / "phase6_decision_log.json"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def main() -> int:
    HERE.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("")
    con = connect_locked()
    log("=== Phase 6 driver start ===")

    # 6.1 cpm_built_at on CPM
    con.execute(
        "ALTER TABLE main.canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS cpm_built_at TIMESTAMP"
    )
    log("[6] ADD COLUMN IF NOT EXISTS cpm_built_at ok")
    con.execute(
        "UPDATE main.canonical_patient_master SET cpm_built_at = CURRENT_TIMESTAMP"
    )
    n_with_ts = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE cpm_built_at IS NOT NULL"
    ).fetchone()[0]
    log(f"[6] cpm_built_at populated for {n_with_ts} rows (expected 10871)")
    if n_with_ts != 10871:
        raise SystemExit(f"cpm_built_at populated only {n_with_ts}/10871")

    # 6.2 cpm_reconciliation_provenance_v1 in manuscript_workspace
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS manuscript_workspace.cpm_reconciliation_provenance_v1 (
          run_id                       VARCHAR PRIMARY KEY,
          started_at                   TIMESTAMP WITH TIME ZONE,
          ended_at                     TIMESTAMP WITH TIME ZONE,
          phases_applied               VARCHAR,
          critical_findings_cleared    VARCHAR,
          high_findings_cleared        VARCHAR,
          med_findings_cleared         VARCHAR,
          held_for_adjudication        VARCHAR
        )
        """
    )
    run_id = "canonical_cleanup_20260417"
    started_at = "2026-04-17T09:25:54+00:00"  # preflight start
    ended_at = datetime.now(timezone.utc).isoformat()
    phases_applied = ",".join([
        "1.1", "1.2", "1.3", "1.4-verify", "1.5a", "1.5b",
        "1.6-verify", "1.7-verify", "1.8",
        "2.1", "2.2", "2.3",
        "3.1-audit", "3.2-doc",
        "4.1", "4.2", "4.3-verify", "4.4", "4.5",
        "4.6-pregate-FAILED",
        "5.1-inventory", "5.2-classify", "5.3-noop",
        "6.1-cpm_built_at", "6.2-provenance",
    ])
    critical_cleared = ",".join([
        "PART2-1.1-tirads(verify-only,already-canonical)",
        "PART2-2.1-vc-s236(1.1+1.2+1.8)",
        "PART2-3.1-rai-dose(1.5)",
        "PROMPT18-2.1-vc-crossref(1.1+1.2)",
    ])
    high_cleared = ",".join([
        "PART2-2.1-fna-broadcast(1.7-verify-already-canonical)",
        "PART2-2.2-bethesda(4.3-verify)",
        "PART2-3.3-tg-counts(1.6-verify-already-canonical)",
        "PART2-3.4-tg-peak-nadir(1.6-verify-already-canonical)",
        "PART2-5.3-any-confirmed-flag(1.8)",
        "PROMPT18-3.1-lateral-nd(1.3)",
        "PROMPT18-6-hypopara-permanence(2.1+2.2+2.3)",
    ])
    med_cleared = ",".join([
        "PART2-1.2/2.3-orphans(3.1-audit;3.2-doc-HOLD)",
        "PART2-1.4-n_us_exams(4.5-COMMENT)",
        "PART2-1.5-imaging-exam-date(4.2)",
        "PART2-2.8-fna-date(4.5-COMMENT)",
        "PART2-3.5/3.6-rai-date(4.5-COMMENT)",
        "PART2-4.2-multifocal(4.1)",
        "PART2-4.3-tumor-size(4.4-COMMENT+invariant-view)",
        "PART2-5.4-ln-counts(NOT-EXPLICITLY-ADDRESSED)",
    ])
    held = ",".join([
        "Phase-2.2-4-hypopara-contradictions(9765,7487,6447,10743)",
        "Phase-3.1-403-tg-lab-orphans(all-likely_non_cancer)",
        "Phase-3.2-3-us-nodule-placeholders(2332,2445,7744)",
        "Phase-4.4-80-path_tumor_size_cm-violators",
        "Phase-4.6-9-cohort-views-with-bare-ajcc8_t_stage-refs",
    ])
    # Idempotent INSERT: replace if existing
    con.execute(
        "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 WHERE run_id = ?",
        [run_id],
    )
    con.execute(
        """
        INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
          (run_id, started_at, ended_at, phases_applied,
           critical_findings_cleared, high_findings_cleared,
           med_findings_cleared, held_for_adjudication)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [run_id, started_at, ended_at, phases_applied,
         critical_cleared, high_cleared, med_cleared, held],
    )
    n_rows = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1"
    ).fetchone()[0]
    log(f"[6] provenance row count: {n_rows}")

    # Final invariant
    n_cpm, n_dist = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()
    if n_cpm != 10871 or n_dist != 10871:
        raise SystemExit(f"invariant breach: rows={n_cpm} dist={n_dist}")
    log(f"[6] invariants OK: rows={n_cpm} distinct={n_dist}")

    DECISIONS_PATH.write_text(json.dumps({
        "run_id": run_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "n_with_cpm_built_at": n_with_ts,
        "provenance_table_rows": n_rows,
    }, indent=2))
    log("=== Phase 6 driver end ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
