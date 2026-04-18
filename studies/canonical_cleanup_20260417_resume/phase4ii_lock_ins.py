"""Phase 4 (ii) lock-ins per Logan's go-ahead.

  Lock 1 — DONE in phase4ii_step1_scope_check.py (hidden_both_under = 0,
           but TEM-scope check expands the F-bucket to 75; combined with
           5 non-TEM-feeder F-cases the true F set is 80 = all invariant
           violations).

  Lock 2 — Replace correction queue with 80 rids (= entire invariant
           violation set), each tagged with sub-bucket F1 (TEM-confirmed,
           n=75) or F2 (non-TEM feeder confirmed, n=5). proposed_corrected_value
           comes from TEM true_max for F1, from cross-feeder rollup for F2.
           D and E queues now empty (cases reclassified into correction
           queue) but multifocal_enumeration_notes_v1 RETAINED as semantic
           overlay for the 13 small-Δ multifocal patients.

  Lock 3 — Re-COMMENT main.canonical_patient_master.tumor_size_cm_max
           with the GREATEST() interim workaround, N=80, explicit join-to-
           queue path for authoritative values.

  Lock 4 — ALTER TABLE path_tumor_size_correction_queue_v1 ADD COLUMN
           upstream_fix_target VARCHAR; populate per sub-bucket.

  Lock 5 — UPDATE Phase 4(ii) provenance row phases_applied to encode
           F80 / E0 / D13_semantic / hidden_both_under_0.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG = HERE / "phase4ii_lock_ins.log"
SCOPE_PATH = HERE / "phase4ii_scope_check.json"
CLASS_PATH = HERE / "phase4ii_classification.json"

CLASSIFIER_VERSION_LOCKED = "v2_phase4ii_lockins_20260418"
PHASE4II_RUN_ID = "canonical_cleanup_resume_20260417_phase4ii"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG.open("a") as f:
        f.write(line + "\n")


def main() -> int:
    LOG.write_text("")
    con = connect_locked()
    log("Phase 4 (ii) lock-ins starting (no CPM value modification)...")

    scope = json.loads(SCOPE_PATH.read_text())
    clas = json.loads(CLASS_PATH.read_text())

    # Index TEM scope by rid
    scope_by_rid = {str(d["research_id"]): d for d in scope["detail"]}
    class_by_rid = {r["research_id"]: r for r in clas}

    scope_75_rids = set(scope_by_rid.keys())
    invariant_80_rids = set(class_by_rid.keys())
    log(
        f"  scope_75 size={len(scope_75_rids)} "
        f"invariant_80 size={len(invariant_80_rids)} "
        f"hidden_both_under={scope['aggregate']['hidden_both_under']}"
    )
    assert scope["aggregate"]["hidden_both_under"] == 0, (
        "STOP: hidden_both_under must be 0 to proceed with current framing"
    )

    f1_rids = scope_75_rids & invariant_80_rids       # TEM-confirmed
    f2_rids = invariant_80_rids - scope_75_rids       # non-TEM feeder
    log(
        f"  F1 (TEM-confirmed, n={len(f1_rids)}); "
        f"F2 (non-TEM, n={len(f2_rids)}); "
        f"F_total={len(f1_rids) + len(f2_rids)}"
    )

    # ------ Lock 4 — add upstream_fix_target column (idempotent) ------
    log("Lock 4: ALTER correction queue ADD COLUMN upstream_fix_target...")
    con.execute(
        "ALTER TABLE manuscript_workspace.path_tumor_size_correction_queue_v1 "
        "ADD COLUMN IF NOT EXISTS upstream_fix_target VARCHAR"
    )
    con.execute(
        "ALTER TABLE manuscript_workspace.path_tumor_size_correction_queue_v1 "
        "ADD COLUMN IF NOT EXISTS subbucket VARCHAR"
    )
    con.execute(
        "ALTER TABLE manuscript_workspace.path_tumor_size_correction_queue_v1 "
        "ADD COLUMN IF NOT EXISTS true_max_across_all_surgeries DOUBLE"
    )
    con.execute(
        "ALTER TABLE manuscript_workspace.path_tumor_size_correction_queue_v1 "
        "ADD COLUMN IF NOT EXISTS n_surg_episodes INTEGER"
    )

    # ------ Lock 2 — Rebuild correction queue with all 80 rids ------
    log("Lock 2: rebuilding correction queue with 80 rids (F1 + F2)...")
    con.execute(
        "DELETE FROM manuscript_workspace.path_tumor_size_correction_queue_v1"
    )

    upstream_f1 = (
        "tumor_size_cm_max max-aggregator: feeder set populates from "
        "first-surgery-only tables (synoptic_tumor_long_v1, "
        "canonical_tumor_characteristics_v1, path_synoptics, tumor_pathology) "
        "and excludes second-surgery TEM rows. Fix: re-aggregate from "
        "MAX(main.tumor_episode_master_v2.tumor_size_cm) per research_id."
    )
    upstream_f2 = (
        "tumor_size_cm_max under-reports from a non-TEM feeder mismatch. "
        "Path side has the larger value from a feeder (path_synoptics, "
        "tumor_pathology, or canonical_tumor_characteristics_v1) that the "
        "max-aggregator did not consume. Fix: trace per-rid which feeder "
        "supplied path's larger value and ensure max-aggregator includes "
        "that feeder."
    )

    inserts = []
    for rid in sorted(invariant_80_rids, key=lambda x: int(x)):
        c = class_by_rid[rid]
        path_v = c["path_tumor_size_cm"]
        max_v = c["tumor_size_cm_max"]
        delta = c["delta_cm"]
        observed_max_feeder = c["observed_max_tumor_focus"]
        evidence = c.get("evidence") or ""

        if rid in f1_rids:
            s = scope_by_rid[rid]
            true_max = s["true_max_across_all_surgeries"]
            n_surg = s["n_surg_episodes"]
            proposed = max(true_max or 0, observed_max_feeder or 0)
            proposed_source = (
                f"GREATEST(TEM true_max_across_all_surgeries={true_max}, "
                f"observed_max_tumor_focus_across_all_feeders="
                f"{observed_max_feeder})"
            )
            subbucket = "F1"
            upstream = upstream_f1
            row_evidence = (
                f"[F1 TEM-confirmed] TEM true_max={true_max} across "
                f"{n_surg} surgeries; CPM tumor_size_cm_max={max_v} "
                f"(under-report Δ={s['max_under_report_delta_cm']}); "
                f"original Phase 4(ii) bucket={c['bucket']}. {evidence}"
            )
        else:
            true_max = None
            n_surg = None
            proposed = observed_max_feeder
            proposed_source = (
                f"observed_max_tumor_focus across all 5 feeders="
                f"{observed_max_feeder} (TEM scope-check did NOT confirm; "
                f"non-TEM source for the larger value)"
            )
            subbucket = "F2"
            upstream = upstream_f2
            row_evidence = (
                f"[F2 non-TEM feeder] observed_max_tumor_focus="
                f"{observed_max_feeder}; CPM tumor_size_cm_max={max_v}; "
                f"path={path_v}. TEM does not confirm a larger value. "
                f"Original Phase 4(ii) bucket={c['bucket']}. {evidence}"
            )

        inserts.append(
            (
                rid,
                "F",                                  # bucket (top-level)
                subbucket,                            # F1 / F2
                "tumor_size_cm_max",
                path_v,
                max_v,
                observed_max_feeder,
                delta,
                proposed,
                proposed_source,
                row_evidence,
                CLASSIFIER_VERSION_LOCKED,
                "awaiting_approval",
                upstream,
                true_max,
                n_surg,
            )
        )

    con.executemany(
        """
        INSERT INTO manuscript_workspace.path_tumor_size_correction_queue_v1
          (research_id, bucket, subbucket, broken_column,
           current_path_tumor_size_cm, current_tumor_size_cm_max,
           observed_max_tumor_focus, delta_cm,
           proposed_corrected_value, proposed_corrected_source,
           evidence, classifier_version, status, upstream_fix_target,
           true_max_across_all_surgeries, n_surg_episodes,
           created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                CURRENT_TIMESTAMP)
        """,
        inserts,
    )
    n_corr = con.execute(
        "SELECT COUNT(*) FROM "
        "manuscript_workspace.path_tumor_size_correction_queue_v1"
    ).fetchone()[0]
    log(f"  correction queue rebuilt: {n_corr} rows (expected 80)")
    if n_corr != 80:
        raise SystemExit(f"STOP: expected 80 rows in correction queue, got {n_corr}")

    sub_dist = con.execute(
        "SELECT subbucket, COUNT(*) FROM "
        "manuscript_workspace.path_tumor_size_correction_queue_v1 "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()
    log(f"  subbucket distribution: {sub_dist}")

    # Empty out chart-review queue (E now 0 under reclassification)
    log("Empty chart-review queue (all originally-E rids reclassified to F1)...")
    con.execute(
        "DELETE FROM manuscript_workspace.path_tumor_size_chart_review_queue_v1"
    )
    n_e = con.execute(
        "SELECT COUNT(*) FROM "
        "manuscript_workspace.path_tumor_size_chart_review_queue_v1"
    ).fetchone()[0]
    log(f"  chart_review_queue rows now: {n_e} (expected 0)")
    con.execute(
        "COMMENT ON TABLE "
        "manuscript_workspace.path_tumor_size_chart_review_queue_v1 IS "
        "'Reserved for future cases where the structured-data classifier "
        "cannot resolve the discrepancy. Currently empty: all 80 "
        "invariant violations from the 2026-04-17 sweep were resolvable as "
        "F-pattern (max under-reports) and queued in "
        "path_tumor_size_correction_queue_v1.'"
    )

    # multifocal_enumeration_notes_v1 retained as semantic overlay — these
    # 13 patients have legitimate dominant ≠ max small-Δ semantics in
    # addition to needing the F-bucket correction.
    log("Updating multifocal_notes_v1 COMMENT to reflect dual classification...")
    con.execute(
        "COMMENT ON TABLE "
        "manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1 IS "
        "'13 patients where path_tumor_size_cm and tumor_size_cm_max differ "
        "by <=1cm AND the dominant-picker chose a different focus from the "
        "max-aggregator. As of the Phase 4(ii) lock-in 2026-04-18 these 13 "
        "rids are ALSO in the correction queue (subbucket=F1) because the "
        "max-aggregator under-reports per TEM. Documented separately as "
        "semantic overlay: even after the queue correction is applied, "
        "these patients legitimately have dominant!=max because dominant "
        "and max can be different valid tumor foci across surgeries.'"
    )

    # ------ Lock 3 — refine tumor_size_cm_max COMMENT ------
    log("Lock 3: refining tumor_size_cm_max COMMENT with GREATEST() workaround...")
    new_max_comment = (
        "Maximum tumor focus size across surgeries. KNOWN BUG (2026-04-18): "
        "for multi-surgery patients, feeder tables that populate this "
        "column include surgery-1 tumors only. N=80 patients identified "
        "where later-surgery foci exceed this value — see "
        "manuscript_workspace.path_tumor_size_correction_queue_v1 (75 "
        "TEM-confirmed under-reports as subbucket=F1; 5 non-TEM-feeder "
        "under-reports as subbucket=F2; status='awaiting_approval'). "
        "Until corrections are applied, use "
        "GREATEST(path_tumor_size_cm, tumor_size_cm_max) for true-max "
        "queries, OR join to the correction queue for the authoritative "
        "value (proposed_corrected_value column). Dominant-focus queries "
        "should use path_tumor_size_cm. 13 of the 80 are documented as "
        "semantic overlay in "
        "manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1 "
        "where dominant!=max even after the bug is fixed (different valid "
        "foci across surgeries). Generalized scope check 2026-04-18 "
        "confirmed hidden_both_under=0 (no cases escape the "
        "path_tumor_size_invariant_v1 view)."
    )
    con.execute(
        f"COMMENT ON COLUMN main.canonical_patient_master.tumor_size_cm_max "
        f"IS '{new_max_comment.replace(chr(39), chr(39) + chr(39))}'"
    )

    # ------ Also refine path_tumor_size_cm comment to reflect N=80 ------
    log("Refining path_tumor_size_cm COMMENT to reflect N=80 scope...")
    new_path_comment = (
        "Dominant tumor size (not MAX). For multifocal patients, dominant "
        "focus is reported; pair with tumor_size_cm_max for the largest "
        "focus across surgeries. CAVEAT (2026-04-18): an 80-patient bug "
        "was identified where tumor_size_cm_max under-reports (the max-"
        "aggregator misses tumors from later surgeries). For those "
        "patients path_tumor_size_cm reflects the larger value correctly. "
        "See manuscript_workspace.path_tumor_size_correction_queue_v1 "
        "(F1=75 TEM-confirmed, F2=5 non-TEM); semantic overlay for 13 "
        "small-Δ multifocal cases in "
        "manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1. "
        "Original semantics clarification: 2026-04-17."
    )
    con.execute(
        f"COMMENT ON COLUMN main.canonical_patient_master.path_tumor_size_cm "
        f"IS '{new_path_comment.replace(chr(39), chr(39) + chr(39))}'"
    )

    # ------ Lock 5 — Update Phase 4(ii) provenance row phases_applied ------
    log("Lock 5: updating Phase 4(ii) provenance phases_applied + held count...")
    new_phases = (
        "phase4ii__invariant_trace_classify_queues__"
        "F80_under_report_queued_correction__"
        "E0_chart_review_empty__"
        "D13_semantics_documented__"
        "hidden_both_under_0__"
        "GREATEST_workaround_documented_in_column_comments"
    )
    con.execute(
        "UPDATE manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "SET phases_applied = ?, "
        "    held_for_adjudication = ?, "
        "    ended_at = CURRENT_TIMESTAMP "
        "WHERE run_id = ?",
        [new_phases, "80", PHASE4II_RUN_ID],
    )
    chk = con.execute(
        "SELECT phases_applied, held_for_adjudication "
        "FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "WHERE run_id = ?",
        [PHASE4II_RUN_ID],
    ).fetchone()
    log(f"  Phase 4(ii) provenance row updated: phases={chk[0][:80]}... held={chk[1]}")

    # CPM invariant
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit(f"CPM invariant regressed: {n_rows}/{n_distinct}")
    log(f"  CPM invariant re-asserted: {n_rows}/{n_distinct}")
    log("Phase 4 (ii) lock-ins complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
