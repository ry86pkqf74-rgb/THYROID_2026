"""Phase 1 — Hypoparathyroidism adjudication for 4 queued rids.

Strict adjudication per Logan's 2026-04-17 spec:

  (A) phenotype is correct (transient): cleanly-supported transient evidence
      => CPM reset.
  (B) CPM is correct (permanent): ALL three must hold:
        - PTH < 15 pg/mL at any timepoint > day 180 postop
        - calcium or calcitriol active in NLP > day 180 postop
        - no resolution evidence in any note type
      Any one missing => (C).
  (C) Indeterminate => leave both, queue status =
      'indeterminate_requires_chart_review'.

Pulls per rid:
  - all complication_phenotype_v1 rows
  - all extracted_postop_labs_expanded_v1 rows within 400 days post-surgery
  - CPM hypopara aggregates (already in queue probe)

Writes:
  - studies/canonical_cleanup_20260417_resume/phase1_evidence.json
  - studies/canonical_cleanup_20260417_resume/phase1_outcomes.md
  - manuscript_workspace.cpm_hypopara_adjudication_log_v1
      (research_id, action_taken, evidence_summary, decided_at)
  - UPDATE manuscript_workspace.cpm_hypopara_adjudication_queue_v1.status

Will NOT touch canonical_patient_master since (B) requires explicit and
overwhelming evidence; (A) requires clean transient evidence.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))

from _md_connect import connect_locked  # type: ignore  # noqa: E402

RIDS = [6447, 7487, 9765, 10743]
PROMPT18_HELD = {7487, 9765}

EVIDENCE_PATH = HERE / "phase1_evidence.json"
OUTCOMES_PATH = HERE / "phase1_outcomes.md"
LOG_PATH = HERE / "phase1_run.log"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def fetch_dicts(con, sql: str, params=None) -> list[dict]:
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def classify(rid: int, ev: dict) -> tuple[str, str, str]:
    """Return (action_letter, action_taken, decision_basis_sentence).

    action_letter ∈ {'A','B','C'}
    action_taken  ∈ {'reset_cpm_to_transient',
                     'upgrade_phenotype_to_permanent',
                     'indeterminate_requires_chart_review'}
    """
    pth_low_post180 = ev["pth_values_post_180d_lt_15"]
    med_active_post180 = ev["active_replacement_med_at_day_180"]
    resolution_evidence = ev["resolution_evidence_present"]
    pth_normal_post180 = ev["pth_values_post_180d_normal_or_high"]

    # (B) permanent persists: ALL three required
    if (
        len(pth_low_post180) >= 1
        and med_active_post180 is True
        and resolution_evidence is False
    ):
        if rid in PROMPT18_HELD:
            return (
                "C",
                "indeterminate_requires_chart_review",
                "PROMPT 18 explicit hold; even though structured (B) "
                "criteria met, defer to chart review.",
            )
        return (
            "B",
            "upgrade_phenotype_to_permanent",
            "PTH<15 after day 180, active replacement at day>180, no "
            "resolution evidence — all three (B) criteria met.",
        )

    # (A) transient wins: clean transient evidence
    # Definition of clean: at least one PTH measurement >= 15 after day 180
    # AND no active replacement med after day 180 AND nadir not deeply low.
    if (
        len(pth_normal_post180) >= 1
        and med_active_post180 is False
        and len(pth_low_post180) == 0
    ):
        if rid in PROMPT18_HELD:
            return (
                "C",
                "indeterminate_requires_chart_review",
                "PROMPT 18 explicit hold; even though structured (A) "
                "criteria met, defer to chart review.",
            )
        return (
            "A",
            "reset_cpm_to_transient",
            "PTH normalised after day 180 with no active replacement; "
            "phenotype 'transient' supported.",
        )

    # Default
    return (
        "C",
        "indeterminate_requires_chart_review",
        "Insufficient or mixed structured evidence to flip CPM under "
        "strict criteria; deferring to chart review.",
    )


def main() -> int:
    LOG_PATH.write_text("")
    log("Opening locked MotherDuck connection...")
    con = connect_locked()

    evidence: dict[str, dict] = {}
    classifications: dict[str, dict] = {}

    for rid in RIDS:
        log(f"Pulling phenotype + lab evidence for rid {rid}...")
        # All hypopara phenotype rows (no entity filter — but record entity)
        phen = fetch_dicts(
            con,
            """
            SELECT *
            FROM main.complication_phenotype_v1
            WHERE research_id = ?
              AND complication_entity ILIKE '%hypopara%'
            ORDER BY detection_date NULLS LAST
            """,
            [rid],
        )
        # Fallback: if no hypopara-tagged rows, take all entities
        if not phen:
            phen = fetch_dicts(
                con,
                """
                SELECT *
                FROM main.complication_phenotype_v1
                WHERE research_id = ?
                ORDER BY detection_date NULLS LAST
                """,
                [rid],
            )

        # Postop labs within 400 days post first surgery
        labs = fetch_dicts(
            con,
            """
            SELECT *
            FROM main.extracted_postop_labs_expanded_v1
            WHERE research_id = ?
              AND days_postop IS NOT NULL
              AND days_postop BETWEEN 0 AND 400
            ORDER BY days_postop, lab_type
            """,
            [rid],
        )

        # CPM hypopara aggregates (subset of cols we need)
        cpm = fetch_dicts(
            con,
            """
            SELECT
              research_id,
              first_surgery_date_v2,
              hypoparathyroidism_status,
              comp_hypoparathyroidism_permanent,
              comp_hypoparathyroidism_transient,
              comp_hypoparathyroidism_days_postop,
              comp_hypoparathyroidism_timing_window,
              comp_hypopara_permanent_source,
              lab_pth_n_measurements,
              lab_pth_min,
              lab_pth_max,
              lab_pth_most_recent,
              lab_pth_last_date,
              lab_pth_last_days_from_surg,
              lab_calcium_n_measurements,
              lab_calcium_min,
              lab_calcium_most_recent,
              lab_calcium_last_date,
              lab_calcium_last_days_from_surg,
              med_nlp_calcitriol,
              med_nlp_calcitriol_date,
              med_nlp_calcitriol_days_from_surg,
              med_nlp_calcitriol_n_mentions,
              med_nlp_calcium_supplement,
              med_nlp_calcium_supplement_date,
              med_nlp_calcium_supplement_days_from_surg,
              med_nlp_calcium_supplement_n_mentions,
              nsqip_hypoparathyroidism_recovered_flag,
              nsqip_calcium_vitd_replacement
            FROM main.canonical_patient_master
            WHERE research_id = ?
            """,
            [rid],
        )
        cpm_row = cpm[0] if cpm else {}

        # ---------- Evidence aggregation ----------
        # Postop labs: PTH and calcium values
        pth_post180_lt_15 = []
        pth_post180_normal_or_high = []
        ca_post180 = []
        # Use both extracted_postop_labs_expanded_v1 AND CPM aggregates.
        # First, the lab rows in 400d window:
        for r in labs:
            lt = (r.get("lab_type") or "").strip().lower()
            v = r.get("value_corrected") or r.get("value")
            d = r.get("days_postop")
            if v is None or d is None:
                continue
            if d < 180:
                continue
            if "pth" in lt or "parathyroid" in lt:
                if v < 15:
                    pth_post180_lt_15.append({"days_postop": d, "value": v})
                else:
                    pth_post180_normal_or_high.append(
                        {"days_postop": d, "value": v}
                    )
            elif "calcium" in lt or lt in ("ca", "ionca", "ical"):
                ca_post180.append({"days_postop": d, "value": v})

        # ALSO consider CPM-aggregated PTH most-recent / min if days_from_surg known
        # (CPM aggregates pull from broader sources than postop_labs_expanded.)
        cpm_lab_pth = []
        if (
            cpm_row.get("lab_pth_n_measurements")
            and cpm_row.get("lab_pth_n_measurements") > 0
        ):
            mr = cpm_row.get("lab_pth_most_recent")
            mrd = cpm_row.get("lab_pth_last_days_from_surg")
            mn = cpm_row.get("lab_pth_min")
            cpm_lab_pth.append(
                {
                    "most_recent_value": mr,
                    "most_recent_days_from_surg": mrd,
                    "min": mn,
                }
            )
            # If days unknown but we know the patient had a measurement and the
            # most-recent value is < 15 we still treat it as a low PTH; we just
            # cannot confirm > 180d. For strict criteria we REQUIRE > 180d
            # confirmation, so we only count it when days_from_surg is known.
            if mrd is not None and mrd > 180 and mr is not None:
                if mr < 15:
                    pth_post180_lt_15.append(
                        {
                            "days_postop": mrd,
                            "value": mr,
                            "source": "CPM lab_pth_most_recent",
                        }
                    )
                else:
                    pth_post180_normal_or_high.append(
                        {
                            "days_postop": mrd,
                            "value": mr,
                            "source": "CPM lab_pth_most_recent",
                        }
                    )

        # Active replacement med at day > 180?
        # We treat 'active' as: CPM med_nlp_* TRUE AND date OR
        # days_from_surg > 180. If days_from_surg is unknown but n_mentions
        # is high (>=3) and the only date is unknown, mark UNKNOWN, not True.
        cal_days = cpm_row.get("med_nlp_calcium_supplement_days_from_surg")
        ctriol_days = cpm_row.get("med_nlp_calcitriol_days_from_surg")
        cal_active = cpm_row.get("med_nlp_calcium_supplement") is True and (
            cal_days is not None and cal_days > 180
        )
        ctriol_active = cpm_row.get("med_nlp_calcitriol") is True and (
            ctriol_days is not None and ctriol_days > 180
        )
        # If both date columns unknown but mentions exist, flag as UNKNOWN.
        if cpm_row.get("med_nlp_calcitriol") is True and ctriol_days is None:
            ctriol_status = "unknown_date"
        else:
            ctriol_status = "active>180" if ctriol_active else "not_active>180"
        if (
            cpm_row.get("med_nlp_calcium_supplement") is True
            and cal_days is None
        ):
            cal_status = "unknown_date"
        else:
            cal_status = "active>180" if cal_active else "not_active>180"

        if cal_active or ctriol_active:
            med_active = True
        elif ctriol_status == "unknown_date" or cal_status == "unknown_date":
            med_active = "unknown"
        else:
            med_active = False

        # Resolution evidence:
        #   - phenotype voice_resolution_noted irrelevant (vocal cord)
        #   - nsqip_hypoparathyroidism_recovered_flag = TRUE
        #   - any phenotype row with final_complication_status containing
        #     'resolved' or 'recovered'
        resolution = False
        if cpm_row.get("nsqip_hypoparathyroidism_recovered_flag") is True:
            resolution = True
        for p in phen:
            fcs = (p.get("final_complication_status") or "").lower()
            if "resolved" in fcs or "recovered" in fcs:
                resolution = True

        latest_phen = phen[-1] if phen else None
        ev = {
            "phenotype_rows": len(phen),
            "phenotype_entities_seen": sorted(
                {(p.get("complication_entity") or "") for p in phen}
            ),
            "latest_phenotype_status": (
                latest_phen.get("final_complication_status")
                if latest_phen
                else None
            ),
            "latest_phenotype_date_days_postop": (
                latest_phen.get("timing_days_post_surgery")
                if latest_phen
                else None
            ),
            "latest_phenotype_window": (
                latest_phen.get("timing_window") if latest_phen else None
            ),
            "pth_values_post_180d": pth_post180_lt_15
            + pth_post180_normal_or_high,
            "pth_values_post_180d_lt_15": pth_post180_lt_15,
            "pth_values_post_180d_normal_or_high": pth_post180_normal_or_high,
            "calcium_values_post_180d": ca_post180,
            "cpm_lab_pth_aggregate": cpm_lab_pth,
            "active_replacement_med_at_day_180": med_active,
            "calcium_supplement_status": cal_status,
            "calcitriol_status": ctriol_status,
            "resolution_evidence_present": resolution,
            "first_surgery_date": str(cpm_row.get("first_surgery_date_v2")),
            "cpm_says_permanent": cpm_row.get(
                "comp_hypoparathyroidism_permanent"
            ),
            "cpm_phenotype_status_field": cpm_row.get(
                "hypoparathyroidism_status"
            ),
            "cpm_permanent_source": cpm_row.get(
                "comp_hypopara_permanent_source"
            ),
            "n_postop_labs_in_400d": len(labs),
            "phen_rows_dump": phen,
            "labs_in_400d_dump": labs,
            "cpm_hypopara_subset": cpm_row,
        }

        action_letter, action_taken, basis = classify(rid, ev)
        ev["decision_basis"] = basis
        evidence[str(rid)] = ev
        classifications[str(rid)] = {
            "action_letter": action_letter,
            "action_taken": action_taken,
            "decision_basis": basis,
        }
        log(
            f"  rid={rid}: action={action_letter} ({action_taken}); "
            f"phen_rows={len(phen)}, labs_400d={len(labs)}, "
            f"med_active>180={med_active}, resolution={resolution}, "
            f"pth<15>180={len(pth_post180_lt_15)}, "
            f"pth>=15>180={len(pth_post180_normal_or_high)}"
        )

    EVIDENCE_PATH.write_text(json.dumps(evidence, indent=2, default=str))
    log(f"Evidence dump -> {EVIDENCE_PATH}")

    # Build outcomes markdown
    md = ["# Phase 1 — Hypoparathyroidism adjudication outcomes", ""]
    md.append(f"_Generated {datetime.now(timezone.utc).isoformat()}_  ")
    md.append(
        "_Strict bar: (B) requires PTH<15 pg/mL > day 180 AND active "
        "replacement med > day 180 AND no resolution evidence — any one "
        "missing => (C). PROMPT 18 holds (rids 7487, 9765) default to "
        "(C) regardless._"
    )
    md.append("")
    md.append("| rid | action | basis (one-line) |")
    md.append("|---:|:---|:---|")
    for rid in RIDS:
        c = classifications[str(rid)]
        md.append(
            f"| {rid} | **{c['action_letter']}** "
            f"`{c['action_taken']}` | {c['decision_basis']} |"
        )
    md.append("")
    md.append("## Per-patient evidence summary (structured)")
    md.append("")
    for rid in RIDS:
        ev = evidence[str(rid)]
        c = classifications[str(rid)]
        compact = {
            "phenotype_rows": ev["phenotype_rows"],
            "latest_phenotype_status": ev["latest_phenotype_status"],
            "latest_phenotype_date_days_postop": ev[
                "latest_phenotype_date_days_postop"
            ],
            "pth_values_post_180d": ev["pth_values_post_180d"],
            "calcium_values_post_180d": ev["calcium_values_post_180d"],
            "active_replacement_med_at_day_180": ev[
                "active_replacement_med_at_day_180"
            ],
            "decision_basis": c["decision_basis"],
        }
        md.append(f"### rid {rid} — action **{c['action_letter']}**")
        md.append("")
        md.append("```json")
        md.append(json.dumps(compact, indent=2, default=str))
        md.append("```")
        md.append("")
    OUTCOMES_PATH.write_text("\n".join(md) + "\n")
    log(f"Outcomes md -> {OUTCOMES_PATH}")

    # ---------- WRITE PHASE: queue update + log table ----------
    # 1. Create log table if absent (idempotent)
    log("Creating cpm_hypopara_adjudication_log_v1 (if not exists)...")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS manuscript_workspace.cpm_hypopara_adjudication_log_v1 (
            research_id        BIGINT,
            action_taken       VARCHAR,
            evidence_summary   JSON,
            decided_at         TIMESTAMP WITH TIME ZONE,
            run_id             VARCHAR
        )
        """
    )

    # 2. Upsert one row per rid (delete then insert is fine — idempotent)
    rid_csv = ",".join(str(r) for r in RIDS)
    con.execute(
        f"DELETE FROM manuscript_workspace.cpm_hypopara_adjudication_log_v1 "
        f"WHERE research_id IN ({rid_csv}) "
        f"  AND run_id = 'canonical_cleanup_resume_20260417'"
    )
    insert_rows = []
    for rid in RIDS:
        ev = evidence[str(rid)]
        c = classifications[str(rid)]
        compact = {
            "phenotype_rows": ev["phenotype_rows"],
            "latest_phenotype_status": ev["latest_phenotype_status"],
            "latest_phenotype_date_days_postop": ev[
                "latest_phenotype_date_days_postop"
            ],
            "pth_values_post_180d": ev["pth_values_post_180d"],
            "calcium_values_post_180d": ev["calcium_values_post_180d"],
            "active_replacement_med_at_day_180": ev[
                "active_replacement_med_at_day_180"
            ],
            "calcium_supplement_status": ev["calcium_supplement_status"],
            "calcitriol_status": ev["calcitriol_status"],
            "resolution_evidence_present": ev["resolution_evidence_present"],
            "decision_basis": c["decision_basis"],
        }
        insert_rows.append(
            (rid, c["action_taken"], json.dumps(compact, default=str))
        )
    con.executemany(
        """
        INSERT INTO manuscript_workspace.cpm_hypopara_adjudication_log_v1
            (research_id, action_taken, evidence_summary, decided_at, run_id)
        VALUES (?, ?, ?::JSON, CURRENT_TIMESTAMP,
                'canonical_cleanup_resume_20260417')
        """,
        insert_rows,
    )
    n_log = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_hypopara_adjudication_log_v1 "
        "WHERE run_id = 'canonical_cleanup_resume_20260417'"
    ).fetchone()[0]
    log(f"  Inserted {n_log} log rows for run_id=canonical_cleanup_resume_20260417")

    # 3. Update queue status
    for rid in RIDS:
        action = classifications[str(rid)]["action_taken"]
        # queue.research_id is VARCHAR
        con.execute(
            "UPDATE manuscript_workspace.cpm_hypopara_adjudication_queue_v1 "
            "SET status = ? WHERE research_id = ?",
            [action, str(rid)],
        )
    queue_after = fetch_dicts(
        con,
        "SELECT research_id, status FROM manuscript_workspace."
        "cpm_hypopara_adjudication_queue_v1 ORDER BY research_id",
    )
    log(f"  Queue after update: {queue_after}")

    # 4. CPM updates: only for (A) and (B). Currently all (C) → no CPM change.
    n_cpm_updates = sum(
        1
        for rid in RIDS
        if classifications[str(rid)]["action_letter"] in ("A", "B")
    )
    if n_cpm_updates == 0:
        log(
            "  No (A) or (B) classifications => CPM untouched. "
            "comp_hypopara_permanent_limitation_note unchanged."
        )
    else:
        # Safety rail: this would require >500-row dry-run for big updates.
        # Here it's <=4 rows, so we still log a count.
        log(
            f"  {n_cpm_updates} CPM updates required across (A)/(B) — "
            f"see report; dry-run protocol gating not required at this scale."
        )

    # Re-assert CPM invariant
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit(
            f"CPM invariant regressed: {n_rows}/{n_distinct} != 10871/10871"
        )
    log(
        f"  CPM invariant re-asserted: {n_rows} rows / {n_distinct} distinct "
        "research_id."
    )

    log("Phase 1 complete (queue + log written; no CPM changes).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
