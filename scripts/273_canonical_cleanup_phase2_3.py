"""Canonical cleanup 20260417 — Phase 2 (hypopara) + Phase 3 (cohort registry audits).

Phase 2 (per Logan 2026-04-17):
  2.1 Reset the 14 (was 16) duration_unknown: comp_hypoparathyroidism_permanent
      = FALSE, comp_hypopara_permanent_limitation_note = 'reset_20260417:confirmed_duration_unknown'
  2.2 Queue ALL 4 contradictions (9765, 7487, 6447, 10743) into a new
      manuscript_workspace.cpm_hypopara_adjudication_queue_v1
  2.3 ADD COLUMN comp_hypopara_permanent_source; populate
      'lab_persistence' if prm_hypoparathyroidism_lab_flag=TRUE else
      'unknown_override' for current permanent=TRUE rows.

Phase 3 (audit only, no writes to CPM):
  3.1 Tg-lab orphans audit table manuscript_workspace.lab_orphan_audit_v1
      with the 5-table cancer-evidence cross-reference.
  3.2 Document the 3 us_nodules_tirads orphans (2332, 2445, 7744). HOLD,
      no DELETE.

Writes only to thyroid_canonical_publication_v1_0 (CPM + manuscript_workspace).
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

LOG_PATH = HERE / "phase2_3_run.log"
DECISIONS_PATH = HERE / "phase2_3_decision_log.json"

DECISIONS: list[dict] = []


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def record(entry: dict) -> None:
    DECISIONS.append(entry)
    DECISIONS_PATH.write_text(json.dumps(DECISIONS, indent=2, default=str))


def stop(msg: str) -> None:
    log(f"STOP: {msg}")
    DECISIONS_PATH.write_text(json.dumps(DECISIONS, indent=2, default=str))
    raise SystemExit(2)


def assert_invariants(con) -> None:
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        stop(f"Invariant breach: rows={n_rows} distinct={n_distinct}")
    log("invariants OK")


# ---------------------------------------------------------------------------
# Phase 2
# ---------------------------------------------------------------------------

PHENOTYPE_HYPOPARA = """
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         final_complication_status, permanent_flag, transient_flag
  FROM main.complication_phenotype_v1
  WHERE complication_entity = 'hypoparathyroidism'
"""


def phase_2_1(con) -> None:
    log("=== Phase 2.1 — reset 14 duration_unknown patients ===")
    pre = con.execute(
        f"""
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        JOIN ({PHENOTYPE_HYPOPARA}) p USING (research_id)
        WHERE cpm.comp_hypoparathyroidism_permanent IS TRUE
          AND p.final_complication_status = 'confirmed_duration_unknown'
        """
    ).fetchone()[0]
    log(f"[2.1] pre-count: {pre} (expected 14, gate ±5 -> [9,19])")
    if not (9 <= pre <= 19):
        stop(f"[2.1] pre {pre} outside window [9,19]")

    rids = [r[0] for r in con.execute(
        f"""
        SELECT cpm.research_id
        FROM main.canonical_patient_master cpm
        JOIN ({PHENOTYPE_HYPOPARA}) p USING (research_id)
        WHERE cpm.comp_hypoparathyroidism_permanent IS TRUE
          AND p.final_complication_status = 'confirmed_duration_unknown'
        ORDER BY cpm.research_id
        """
    ).fetchall()]
    log(f"[2.1] reset rids: {rids}")

    update_sql = f"""
        UPDATE main.canonical_patient_master AS cpm
        SET comp_hypoparathyroidism_permanent = FALSE,
            comp_hypopara_permanent_limitation_note =
              'reset_20260417:confirmed_duration_unknown'
        WHERE cpm.research_id IN (
          SELECT cpm2.research_id
          FROM main.canonical_patient_master cpm2
          JOIN ({PHENOTYPE_HYPOPARA}) p USING (research_id)
          WHERE cpm2.comp_hypoparathyroidism_permanent IS TRUE
            AND p.final_complication_status = 'confirmed_duration_unknown'
        )
    """
    con.execute(update_sql)
    post = con.execute(
        f"""
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        JOIN ({PHENOTYPE_HYPOPARA}) p USING (research_id)
        WHERE cpm.comp_hypoparathyroidism_permanent IS TRUE
          AND p.final_complication_status = 'confirmed_duration_unknown'
        """
    ).fetchone()[0]
    log(f"[2.1] post-count (should be 0): {post}")
    if post != 0:
        stop(f"[2.1] post {post} != 0")
    record({
        "step": "2.1",
        "expected": 14, "pre_count": pre, "post_count_should_be_0": post,
        "rids_reset": rids,
        "drift_note": "Prompt cited 16; live found 14. Logan approved -2 drift.",
    })


def phase_2_2(con) -> None:
    log("=== Phase 2.2 — queue 4 hypopara contradictions ===")
    # Idempotent CREATE
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS manuscript_workspace.cpm_hypopara_adjudication_queue_v1 (
          research_id        VARCHAR,
          cpm_says           VARCHAR,
          phenotype_says     VARCHAR,
          note               VARCHAR,
          flagged_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
          status             VARCHAR DEFAULT 'awaiting_manual_review'
        )
        """
    )

    # Find live contradictions
    rows = con.execute(
        f"""
        SELECT cpm.research_id,
               'permanent=TRUE' AS cpm_says,
               p.final_complication_status AS phenotype_says
        FROM main.canonical_patient_master cpm
        JOIN ({PHENOTYPE_HYPOPARA}) p USING (research_id)
        WHERE cpm.comp_hypoparathyroidism_permanent IS TRUE
          AND p.final_complication_status = 'confirmed_transient'
        ORDER BY cpm.research_id
        """
    ).fetchall()
    log(f"[2.2] contradictions found: {rows}")
    if len(rows) != 4:
        stop(f"[2.2] expected 4 contradictions; found {len(rows)}")

    # Idempotent insert: only insert rids not already in the queue with the same
    # transient/permanent contradiction.
    inserted = 0
    for rid, cpm_says, ph_says in rows:
        already = con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.cpm_hypopara_adjudication_queue_v1 "
            "WHERE research_id = ? AND cpm_says = ? AND phenotype_says = ?",
            [rid, cpm_says, ph_says],
        ).fetchone()[0]
        if already == 0:
            con.execute(
                """
                INSERT INTO manuscript_workspace.cpm_hypopara_adjudication_queue_v1
                  (research_id, cpm_says, phenotype_says, note)
                VALUES (?, ?, ?, ?)
                """,
                [rid, cpm_says, ph_says,
                 "Phase 2.2 of canonical cleanup 20260417"],
            )
            inserted += 1
    log(f"[2.2] inserted {inserted} new queue rows (idempotent)")

    # Verify
    n_in_queue = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_hypopara_adjudication_queue_v1"
    ).fetchone()[0]
    log(f"[2.2] total rows now in queue: {n_in_queue}")
    record({
        "step": "2.2",
        "contradictions_found": [list(r) for r in rows],
        "inserted_this_run": inserted,
        "queue_total_rows": n_in_queue,
        "drift_note": ("Prompt cited 2 (9765, 7487); live found 4. "
                       "Logan approved queueing all 4 (6447, 10743 newly identified)."),
    })


def phase_2_3(con) -> None:
    log("=== Phase 2.3 — add comp_hypopara_permanent_source provenance column ===")
    con.execute(
        "ALTER TABLE main.canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS comp_hypopara_permanent_source VARCHAR"
    )
    # Populate for every patient whose current comp_hypoparathyroidism_permanent IS TRUE
    n_perm_true = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE comp_hypoparathyroidism_permanent IS TRUE"
    ).fetchone()[0]
    log(f"[2.3] permanent=TRUE rows to backfill: {n_perm_true}")

    update_sql = """
        UPDATE main.canonical_patient_master AS cpm
        SET comp_hypopara_permanent_source =
          CASE WHEN cpm.prm_hypoparathyroidism_lab_flag IS TRUE
               THEN 'lab_persistence'
               ELSE 'unknown_override'
          END
        WHERE cpm.comp_hypoparathyroidism_permanent IS TRUE
    """
    con.execute(update_sql)

    dist = con.execute(
        "SELECT comp_hypopara_permanent_source, COUNT(*) "
        "FROM main.canonical_patient_master "
        "WHERE comp_hypoparathyroidism_permanent IS TRUE "
        "GROUP BY 1 ORDER BY 1 NULLS FIRST"
    ).fetchall()
    log(f"[2.3] post-distribution: {dist}")
    record({
        "step": "2.3",
        "perm_true_rows_backfilled": n_perm_true,
        "post_distribution": dist,
    })


# ---------------------------------------------------------------------------
# Phase 3 — audit only
# ---------------------------------------------------------------------------

def phase_3_1(con) -> None:
    """Tg-lab orphan audit. No deletes; no CPM writes."""
    log("=== Phase 3.1 — Tg-lab orphan audit ===")

    # All distinct rids in Tg lab
    n_lab_rids = con.execute(
        "SELECT COUNT(DISTINCT research_id) "
        "FROM main.thyroglobulin_lab_canonical_v1"
    ).fetchone()[0]
    n_cpm_rids = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()[0]
    n_cpm_with_lab = con.execute(
        """
        SELECT COUNT(DISTINCT cpm.research_id)
        FROM main.canonical_patient_master cpm
        JOIN (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
          FROM main.thyroglobulin_lab_canonical_v1
        ) tg USING (research_id)
        """
    ).fetchone()[0]
    n_orphans = con.execute(
        """
        SELECT COUNT(DISTINCT lab.research_id)
        FROM (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
              FROM main.thyroglobulin_lab_canonical_v1) lab
        LEFT JOIN main.canonical_patient_master cpm USING (research_id)
        WHERE cpm.research_id IS NULL
        """
    ).fetchone()[0]
    log(f"[3.1] tg_lab distinct rids: {n_lab_rids}, cpm distinct: {n_cpm_rids}, "
        f"cpm-with-lab: {n_cpm_with_lab}, orphans: {n_orphans}")

    # Build/refresh the audit table
    con.execute("DROP TABLE IF EXISTS manuscript_workspace.lab_orphan_audit_v1")
    con.execute(
        """
        CREATE TABLE manuscript_workspace.lab_orphan_audit_v1 AS
        WITH lab AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 COUNT(*) AS n_lab_rows,
                 MIN(specimen_collect_dt) AS first_lab,
                 MAX(specimen_collect_dt) AS last_lab,
                 COUNT(DISTINCT analyte) AS n_analytes
          FROM main.thyroglobulin_lab_canonical_v1
          GROUP BY 1
        ),
        orphans AS (
          SELECT lab.*
          FROM lab
          LEFT JOIN main.canonical_patient_master cpm USING (research_id)
          WHERE cpm.research_id IS NULL
        ),
        ev_fna AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
          FROM main.fna_episode_master_v2
          WHERE research_id IS NOT NULL
        ),
        ev_tem AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
          FROM main.tumor_episode_master_v2
          WHERE research_id IS NOT NULL
        ),
        ev_stl AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
          FROM main.synoptic_tumor_long_v1
          WHERE research_id IS NOT NULL
        ),
        ev_path AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
          FROM main.path_synoptics
          WHERE research_id IS NOT NULL
        ),
        ev_inm AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
          FROM main.imaging_nodule_master_v1
          WHERE research_id IS NOT NULL
        )
        SELECT
          o.research_id,
          o.n_lab_rows,
          o.first_lab,
          o.last_lab,
          o.n_analytes,
          (ev_fna.research_id IS NOT NULL)  AS has_fna_episode,
          (ev_tem.research_id IS NOT NULL)  AS has_tumor_episode,
          (ev_stl.research_id IS NOT NULL)  AS has_synoptic_tumor,
          (ev_path.research_id IS NOT NULL) AS has_path_synoptic,
          (ev_inm.research_id IS NOT NULL)  AS has_imaging_nodule,
          CASE
            WHEN ev_fna.research_id IS NOT NULL
              OR ev_tem.research_id IS NOT NULL
              OR ev_stl.research_id IS NOT NULL
              OR ev_path.research_id IS NOT NULL
              OR ev_inm.research_id IS NOT NULL
            THEN 'likely_dropped_from_CPM'
            ELSE 'likely_non_cancer'
          END AS classification
        FROM orphans o
        LEFT JOIN ev_fna  USING (research_id)
        LEFT JOIN ev_tem  USING (research_id)
        LEFT JOIN ev_stl  USING (research_id)
        LEFT JOIN ev_path USING (research_id)
        LEFT JOIN ev_inm  USING (research_id)
        """
    )
    classes = con.execute(
        "SELECT classification, COUNT(*) FROM manuscript_workspace.lab_orphan_audit_v1 "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()
    log(f"[3.1] classification counts: {classes}")

    samples = {}
    for cls in ("likely_non_cancer", "likely_dropped_from_CPM"):
        samples[cls] = con.execute(
            f"SELECT research_id, n_lab_rows, n_analytes, first_lab, last_lab, "
            f"has_fna_episode, has_tumor_episode, has_synoptic_tumor, "
            f"has_path_synoptic, has_imaging_nodule "
            f"FROM manuscript_workspace.lab_orphan_audit_v1 "
            f"WHERE classification = '{cls}' "
            f"ORDER BY n_lab_rows DESC "
            f"LIMIT 10"
        ).fetchall()

    record({
        "step": "3.1",
        "n_lab_distinct_rids": n_lab_rids,
        "n_cpm_distinct_rids": n_cpm_rids,
        "n_cpm_with_lab": n_cpm_with_lab,
        "n_orphans": n_orphans,
        "classification_counts": classes,
        "samples": samples,
        "audit_table": "manuscript_workspace.lab_orphan_audit_v1",
    })


def phase_3_2(con) -> None:
    """Document the 3 us_nodules_tirads orphans (2332, 2445, 7744). HOLD."""
    log("=== Phase 3.2 — us_nodules_tirads placeholder orphans (HOLD) ===")
    placeholders = ["2332", "2445", "7744"]

    # Confirm presence/absence in CPM
    cpm_status = con.execute(
        "SELECT research_id, 1 FROM main.canonical_patient_master "
        f"WHERE research_id IN ({','.join('?'*len(placeholders))})",
        placeholders,
    ).fetchall()
    cpm_rids = {r[0] for r in cpm_status}

    # Confirm presence in canonical_us_nodule_characteristics_v1
    us_status = con.execute(
        "SELECT research_id, COUNT(*) AS n_rows FROM main.canonical_us_nodule_characteristics_v1 "
        f"WHERE research_id::VARCHAR IN ({','.join('?'*len(placeholders))}) GROUP BY 1",
        placeholders,
    ).fetchall()
    us_map = {r[0]: r[1] for r in us_status}

    rows = []
    for rid in placeholders:
        rows.append({
            "research_id": rid,
            "in_cpm": rid in cpm_rids,
            "n_us_nodule_rows": us_map.get(rid, 0),
        })
    log(f"[3.2] placeholders: {rows}")
    record({"step": "3.2", "placeholders": rows, "action": "HOLD pending Logan approval"})


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main() -> int:
    HERE.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("")
    DECISIONS_PATH.write_text("[]")
    log("=== Phase 2+3 driver start ===")
    con = connect_locked()
    assert_invariants(con)
    phase_2_1(con); assert_invariants(con)
    phase_2_2(con); assert_invariants(con)
    phase_2_3(con); assert_invariants(con)
    phase_3_1(con); assert_invariants(con)
    phase_3_2(con); assert_invariants(con)
    log("=== Phase 2+3 driver end ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
