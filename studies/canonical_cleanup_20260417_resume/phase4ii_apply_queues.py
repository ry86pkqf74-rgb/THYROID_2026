"""Phase 4 (ii) apply — build the 3 queue tables and refine column COMMENTs.

Strictly NO modification of canonical_patient_master values. Only:
  - CREATE/REPLACE 3 manuscript_workspace queue tables
  - COMMENT ON path_tumor_size_cm (refined) + tumor_size_cm_max (new)
  - INSERT one Phase 4 (ii) provenance row reflecting held_for_adjudication
    = F + E = 60 + 7 = 67 (D=13 is documented, not held).

Tables created:
  manuscript_workspace.path_tumor_size_correction_queue_v1
    columns: research_id, bucket, current_path_tumor_size_cm,
             current_tumor_size_cm_max, observed_max_tumor_focus,
             delta_cm, broken_column, proposed_corrected_value,
             proposed_corrected_source, evidence, classifier_version,
             status, created_at
    rows: F (60) + (A+B+C if any)
    status='awaiting_approval'

  manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1
    columns: research_id, current_path_tumor_size_cm,
             current_tumor_size_cm_max, delta_cm, observed_max_tumor_focus,
             notes, classifier_version, created_at
    rows: D (13)

  manuscript_workspace.path_tumor_size_chart_review_queue_v1
    columns: research_id, current_path_tumor_size_cm,
             current_tumor_size_cm_max, delta_cm, observed_max_tumor_focus,
             evidence, classifier_version, status, created_at
    rows: E (7)
    status='awaiting_chart_review'
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG = HERE / "phase4ii_apply.log"
JSON_PATH = HERE / "phase4ii_classification.json"

CLASSIFIER_VERSION = "v1_phase4ii_signal_based_20260418"

PATH_TUMOR_SIZE_CM_COMMENT_NEW = (
    "Dominant tumor size (not MAX). For multifocal patients, dominant "
    "focus is reported; pair with tumor_size_cm_max for the largest "
    "focus across surgeries. CAVEAT (2026-04-18): a 60-patient bug was "
    "identified where tumor_size_cm_max under-reports (the max-aggregator "
    "misses tumors from second surgeries) — for those patients, "
    "path_tumor_size_cm is the correct largest-focus value. See "
    "manuscript_workspace.path_tumor_size_correction_queue_v1 for the "
    "affected rids and proposed corrections. Original comment retained: "
    "Semantics clarified 2026-04-17."
)

TUMOR_SIZE_CM_MAX_COMMENT = (
    "Maximum tumor focus size across surgeries. KNOWN BUG (2026-04-18): "
    "for 60 multi-surgery patients (typically primary + completion "
    "thyroidectomy with a larger tumor at the second surgery), this "
    "column under-reports because the max-aggregator reads from feeders "
    "that only see the first surgery's tumor focus(es). Affected rids and "
    "proposed corrections are queued in "
    "manuscript_workspace.path_tumor_size_correction_queue_v1 with "
    "status='awaiting_approval'. Use path_tumor_size_cm as the more "
    "reliable dominant-focus value pending row-by-row correction approval. "
    "13 additional patients show small (<=1cm) multifocal-enumeration "
    "drift documented in "
    "manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1; "
    "7 patients are queued for chart review in "
    "manuscript_workspace.path_tumor_size_chart_review_queue_v1."
)

PHASE4II_RUN_ID = "canonical_cleanup_resume_20260417_phase4ii"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG.open("a") as f:
        f.write(line + "\n")


def main() -> int:
    LOG.write_text("")
    started_at = datetime.now(timezone.utc).isoformat()
    con = connect_locked()
    log("Phase 4 (ii) apply starting (no CPM value modification)...")

    rows = json.loads(JSON_PATH.read_text())
    log(f"  loaded {len(rows)} classification rows")
    by_bucket: dict[str, list[dict]] = {b: [] for b in "ABCDEF"}
    for r in rows:
        by_bucket[r["bucket"]].append(r)
    log(
        f"  bucket counts: "
        f"A={len(by_bucket['A'])} B={len(by_bucket['B'])} "
        f"C={len(by_bucket['C'])} D={len(by_bucket['D'])} "
        f"E={len(by_bucket['E'])} F={len(by_bucket['F'])}"
    )

    # ---------- Correction queue (A + B + C + F) ----------
    log("Creating manuscript_workspace.path_tumor_size_correction_queue_v1...")
    con.execute(
        "DROP TABLE IF EXISTS "
        "manuscript_workspace.path_tumor_size_correction_queue_v1"
    )
    con.execute(
        """
        CREATE TABLE manuscript_workspace.path_tumor_size_correction_queue_v1 (
            research_id                      VARCHAR,
            bucket                           VARCHAR,
            broken_column                    VARCHAR,
            current_path_tumor_size_cm       DOUBLE,
            current_tumor_size_cm_max        DOUBLE,
            observed_max_tumor_focus         DOUBLE,
            delta_cm                         DOUBLE,
            proposed_corrected_value         DOUBLE,
            proposed_corrected_source        VARCHAR,
            evidence                         VARCHAR,
            classifier_version               VARCHAR,
            status                           VARCHAR,
            created_at                       TIMESTAMP WITH TIME ZONE
        )
        """
    )
    correction_rows = (
        by_bucket["A"] + by_bucket["B"] + by_bucket["C"] + by_bucket["F"]
    )
    bucket_to_broken = {
        "A": "path_tumor_size_cm",
        "B": "path_tumor_size_cm",
        "C": "path_tumor_size_cm",
        "F": "tumor_size_cm_max",
    }
    inserts_corr = []
    for r in correction_rows:
        inserts_corr.append(
            (
                r["research_id"],
                r["bucket"],
                bucket_to_broken[r["bucket"]],
                r["path_tumor_size_cm"],
                r["tumor_size_cm_max"],
                r["observed_max_tumor_focus"],
                r["delta_cm"],
                r["proposed_corrected_value"],
                r["proposed_corrected_source"],
                r["evidence"],
                CLASSIFIER_VERSION,
                "awaiting_approval",
            )
        )
    if inserts_corr:
        con.executemany(
            """
            INSERT INTO manuscript_workspace.path_tumor_size_correction_queue_v1
              (research_id, bucket, broken_column,
               current_path_tumor_size_cm, current_tumor_size_cm_max,
               observed_max_tumor_focus, delta_cm,
               proposed_corrected_value, proposed_corrected_source,
               evidence, classifier_version, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            inserts_corr,
        )
    n_corr = con.execute(
        "SELECT COUNT(*) FROM "
        "manuscript_workspace.path_tumor_size_correction_queue_v1"
    ).fetchone()[0]
    log(f"  correction_queue rows: {n_corr} (expected {len(correction_rows)})")

    con.execute(
        "COMMENT ON TABLE "
        "manuscript_workspace.path_tumor_size_correction_queue_v1 IS "
        "'Per-rid corrections proposed by the Phase 4 (ii) classifier "
        "2026-04-18 for the path_tumor_size_invariant_v1 violations. "
        "Each row is awaiting_approval and represents a SINGLE patient-data "
        "value change pending Logan''s row-by-row sign-off. NO rows in "
        "main.canonical_patient_master have been modified.'"
    )

    # ---------- Multifocal enumeration notes (D) ----------
    log("Creating manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1...")
    con.execute(
        "DROP TABLE IF EXISTS "
        "manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1"
    )
    con.execute(
        """
        CREATE TABLE
          manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1 (
            research_id                  VARCHAR,
            current_path_tumor_size_cm   DOUBLE,
            current_tumor_size_cm_max    DOUBLE,
            delta_cm                     DOUBLE,
            observed_max_tumor_focus     DOUBLE,
            notes                        VARCHAR,
            classifier_version           VARCHAR,
            created_at                   TIMESTAMP WITH TIME ZONE
        )
        """
    )
    inserts_d = []
    for r in by_bucket["D"]:
        inserts_d.append(
            (
                r["research_id"],
                r["path_tumor_size_cm"],
                r["tumor_size_cm_max"],
                r["delta_cm"],
                r["observed_max_tumor_focus"],
                (
                    f"Dominant focus and max focus differ by "
                    f"{r['delta_cm']:.2f}cm but both are within the feeder "
                    f"set's tumor focus values. Difference reflects which "
                    f"focus the dominant-picker chose vs which the max-"
                    f"aggregator chose, not a data error. "
                    f"observed_max_tumor_focus={r['observed_max_tumor_focus']}."
                ),
                CLASSIFIER_VERSION,
            )
        )
    if inserts_d:
        con.executemany(
            """
            INSERT INTO
              manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1
              (research_id, current_path_tumor_size_cm,
               current_tumor_size_cm_max, delta_cm,
               observed_max_tumor_focus, notes, classifier_version,
               created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            inserts_d,
        )
    n_mf = con.execute(
        "SELECT COUNT(*) FROM "
        "manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1"
    ).fetchone()[0]
    log(f"  multifocal_enumeration_notes rows: {n_mf} (expected {len(by_bucket['D'])})")

    con.execute(
        "COMMENT ON TABLE "
        "manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1 IS "
        "'Patients where path_tumor_size_cm and tumor_size_cm_max differ "
        "by <=1cm and the difference is attributable to which tumor focus "
        "the dominant-picker vs max-aggregator chose. NOT a data error; "
        "documented for query writers who need to understand the semantics. "
        "Identified by Phase 4 (ii) classifier 2026-04-18.'"
    )

    # ---------- Chart-review queue (E) ----------
    log("Creating manuscript_workspace.path_tumor_size_chart_review_queue_v1...")
    con.execute(
        "DROP TABLE IF EXISTS "
        "manuscript_workspace.path_tumor_size_chart_review_queue_v1"
    )
    con.execute(
        """
        CREATE TABLE manuscript_workspace.path_tumor_size_chart_review_queue_v1 (
            research_id                  VARCHAR,
            current_path_tumor_size_cm   DOUBLE,
            current_tumor_size_cm_max    DOUBLE,
            delta_cm                     DOUBLE,
            observed_max_tumor_focus     DOUBLE,
            evidence                     VARCHAR,
            classifier_version           VARCHAR,
            status                       VARCHAR,
            created_at                   TIMESTAMP WITH TIME ZONE
        )
        """
    )
    inserts_e = []
    for r in by_bucket["E"]:
        inserts_e.append(
            (
                r["research_id"],
                r["path_tumor_size_cm"],
                r["tumor_size_cm_max"],
                r["delta_cm"],
                r["observed_max_tumor_focus"],
                r["evidence"],
                CLASSIFIER_VERSION,
                "awaiting_chart_review",
            )
        )
    if inserts_e:
        con.executemany(
            """
            INSERT INTO manuscript_workspace.path_tumor_size_chart_review_queue_v1
              (research_id, current_path_tumor_size_cm,
               current_tumor_size_cm_max, delta_cm,
               observed_max_tumor_focus, evidence, classifier_version,
               status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            inserts_e,
        )
    n_e = con.execute(
        "SELECT COUNT(*) FROM "
        "manuscript_workspace.path_tumor_size_chart_review_queue_v1"
    ).fetchone()[0]
    log(f"  chart_review_queue rows: {n_e} (expected {len(by_bucket['E'])})")
    con.execute(
        "COMMENT ON TABLE "
        "manuscript_workspace.path_tumor_size_chart_review_queue_v1 IS "
        "'Patients where path_tumor_size_cm > tumor_size_cm_max but neither "
        "the F-pattern (max under-reports) nor the D-pattern (small "
        "multifocal drift) cleanly applies. Need chart review to determine "
        "which value is correct or whether both need revision.'"
    )

    # ---------- Refined COMMENTs on CPM columns ----------
    log("Refining COMMENT ON COLUMN main.canonical_patient_master.path_tumor_size_cm...")
    con.execute(
        f"COMMENT ON COLUMN main.canonical_patient_master.path_tumor_size_cm "
        f"IS '{PATH_TUMOR_SIZE_CM_COMMENT_NEW.replace(chr(39), chr(39) + chr(39))}'"
    )
    log("Adding COMMENT ON COLUMN main.canonical_patient_master.tumor_size_cm_max...")
    con.execute(
        f"COMMENT ON COLUMN main.canonical_patient_master.tumor_size_cm_max "
        f"IS '{TUMOR_SIZE_CM_MAX_COMMENT.replace(chr(39), chr(39) + chr(39))}'"
    )

    # ---------- Phase 4 (ii) provenance row ----------
    held = n_corr + n_e  # F + E (D is documented, not held)
    log(f"Inserting Phase 4 (ii) provenance row (held={held})...")
    n_before = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "WHERE run_id = ?",
        [PHASE4II_RUN_ID],
    ).fetchone()[0]
    if n_before:
        con.execute(
            "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
            "WHERE run_id = ?",
            [PHASE4II_RUN_ID],
        )
    con.execute(
        """
        INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
          (run_id, started_at, ended_at, phases_applied,
           critical_findings_cleared, high_findings_cleared,
           med_findings_cleared, held_for_adjudication)
        VALUES (?, ?::TIMESTAMPTZ, CURRENT_TIMESTAMP,
                'phase4ii_invariant_violation_trace_classify_queues__'
                'tumor_size_cm_max_under_report_bug_documented',
                '0', '0', '0', ?)
        """,
        [PHASE4II_RUN_ID, started_at, str(held)],
    )
    n_total = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1"
    ).fetchone()[0]
    log(f"  cpm_reconciliation_provenance_v1 total rows now: {n_total}")

    # ---------- CPM invariant ----------
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit(f"CPM invariant regressed: {n_rows}/{n_distinct}")
    log(f"  CPM invariant re-asserted: {n_rows}/{n_distinct}")
    log("Phase 4 (ii) apply complete (queues + comments only; no CPM modifications).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
