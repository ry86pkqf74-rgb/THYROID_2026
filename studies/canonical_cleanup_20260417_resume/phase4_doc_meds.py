"""Phase 4 — close remaining documentation MEDs.

  4.1 COMMENT ON COLUMN main.canonical_patient_master.path_tumor_size_cm
  4.2 CREATE OR REPLACE VIEW manuscript_workspace.path_tumor_size_invariant_v1
       (expect 0 rows; surface to Logan if non-zero)
  4.3 COMMENT ON COLUMN main.canonical_patient_master.worst_bethesda_num

Plus Phase 4 provenance row.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG_PATH = HERE / "phase4_run.log"
RUN_ID = "canonical_cleanup_resume_20260417_phase4"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def main() -> int:
    LOG_PATH.write_text("")
    started_at = datetime.now(timezone.utc).isoformat()
    con = connect_locked()
    log("Phase 4 — closing remaining documentation MEDs.")

    # Pre-check: required columns exist
    cpm_cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_patient_master'"
        ).fetchall()
    }
    for c in ("path_tumor_size_cm", "tumor_size_cm_max", "worst_bethesda_num"):
        if c not in cpm_cols:
            raise SystemExit(f"STOP: required CPM column {c!r} missing")
    log("  required CPM columns present: path_tumor_size_cm, "
        "tumor_size_cm_max, worst_bethesda_num")

    # ---------- 4.1 path_tumor_size_cm comment ----------
    pts_comment = (
        "Dominant tumor size (not MAX). For multifocal patients, dominant "
        "focus is reported; use tumor_size_cm_max for the largest focus. "
        "Semantics clarified 2026-04-17."
    )
    log("4.1 COMMENT ON main.canonical_patient_master.path_tumor_size_cm ...")
    con.execute(
        f"COMMENT ON COLUMN main.canonical_patient_master.path_tumor_size_cm "
        f"IS '{pts_comment.replace(chr(39), chr(39) + chr(39))}'"
    )

    # ---------- 4.2 invariant view ----------
    log("4.2 CREATE OR REPLACE VIEW manuscript_workspace."
        "path_tumor_size_invariant_v1 ...")
    con.execute(
        """
        CREATE OR REPLACE VIEW manuscript_workspace.path_tumor_size_invariant_v1 AS
          SELECT research_id, path_tumor_size_cm, tumor_size_cm_max
          FROM main.canonical_patient_master
          WHERE path_tumor_size_cm > tumor_size_cm_max + 0.01
        """
    )
    n_inv = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.path_tumor_size_invariant_v1"
    ).fetchone()[0]
    log(f"  invariant view row count: {n_inv} (expected 0)")
    if n_inv != 0:
        sample = con.execute(
            "SELECT * FROM manuscript_workspace.path_tumor_size_invariant_v1 "
            "ORDER BY (path_tumor_size_cm - tumor_size_cm_max) DESC LIMIT 5"
        ).fetchall()
        log(f"  *** SURFACE TO LOGAN: invariant violated; top 5 = {sample}")

    # ---------- 4.3 worst_bethesda_num comment ----------
    wb_comment = (
        "Patient-level worst Bethesda unified across episode master, "
        "cytology, and NLP. Provenance in worst_bethesda_source. 672 "
        "CPM-over cases vs fna_episode_master_v2 are by design — see PART2 "
        "§2.2 (2026-04-16)."
    )
    log("4.3 COMMENT ON main.canonical_patient_master.worst_bethesda_num ...")
    con.execute(
        f"COMMENT ON COLUMN main.canonical_patient_master.worst_bethesda_num "
        f"IS '{wb_comment.replace(chr(39), chr(39) + chr(39))}'"
    )

    # Verify comments landed
    chk_cols = con.execute(
        """
        SELECT column_name, comment
        FROM duckdb_columns()
        WHERE database_name='thyroid_canonical_publication_v1_0'
          AND schema_name='main'
          AND table_name='canonical_patient_master'
          AND column_name IN ('path_tumor_size_cm','worst_bethesda_num')
        ORDER BY column_name
        """
    ).fetchall()
    for col, cm in chk_cols:
        log(f"  COMMENT verify: {col} -> {((cm or '')[:80]).replace(chr(10),' ')}...")

    # ---------- Phase 4 provenance row ----------
    log("Inserting Phase 4 provenance row...")
    n_before = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "WHERE run_id = ?",
        [RUN_ID],
    ).fetchone()[0]
    if n_before:
        log(f"  removing {n_before} prior row(s) for run_id={RUN_ID}")
        con.execute(
            "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
            "WHERE run_id = ?",
            [RUN_ID],
        )
    held = "1" if n_inv != 0 else "0"
    med_cleared = "3" if n_inv == 0 else "2"
    con.execute(
        """
        INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
          (run_id, started_at, ended_at, phases_applied,
           critical_findings_cleared, high_findings_cleared,
           med_findings_cleared, held_for_adjudication)
        VALUES (?, ?::TIMESTAMPTZ, CURRENT_TIMESTAMP,
                'doc_meds__path_tumor_size_comment__invariant_view__worst_bethesda_comment',
                '0', '0', ?, ?)
        """,
        [RUN_ID, started_at, med_cleared, held],
    )
    n_total = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1"
    ).fetchone()[0]
    log(f"  cpm_reconciliation_provenance_v1 total rows now: {n_total}")

    # CPM invariant
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit(
            f"CPM invariant regressed: {n_rows}/{n_distinct} != 10871/10871"
        )
    log(f"  CPM invariant re-asserted: {n_rows}/{n_distinct}")
    log("Phase 4 complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
