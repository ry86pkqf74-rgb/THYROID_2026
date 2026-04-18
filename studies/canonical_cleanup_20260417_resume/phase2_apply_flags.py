"""Phase 2 writes — flag (don't delete) per Logan's 2026-04-17 decision.

Steps (all idempotent):
  1. ALTER TABLE main.thyroglobulin_lab_canonical_v1
       ADD COLUMN IF NOT EXISTS is_in_canonical_cancer_cohort BOOLEAN
  2. ALTER TABLE main.longitudinal_lab_canonical_v1
       ADD COLUMN IF NOT EXISTS is_in_canonical_cancer_cohort BOOLEAN
  3. Dry-run UPDATE counts (safety rail) — log expected splits.
  4. Execute UPDATEs from CPM.
  5. Sanity-check the splits.
  6. COMMENT ON COLUMN both new flag columns.
  7. CREATE OR REPLACE cancer-only convenience views.
  8. UPDATE manuscript_workspace.lab_orphan_audit_v1.classification ->
     'likely_non_cancer_flagged_retained'; ADD COLUMN resolution; populate.
  9. INSERT cpm_reconciliation_provenance_v1 row for Phase 2.
 10. Re-assert CPM invariant (10,871/10,871).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG_PATH = HERE / "phase2_apply.log"

PHASE2_START = "2026-04-18 03:04:29.078764+00:00"  # from phase2 classify run
RUN_ID = "canonical_cleanup_resume_20260417_phase2"

EXPECTED_TG_TRUE = 60385
EXPECTED_TG_FALSE = 13873
EXPECTED_LONG_TRUE = 61374
EXPECTED_LONG_FALSE = 13873

TOLERANCE_PCT = 0.05  # ±5% per safety rail


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def main() -> int:
    LOG_PATH.write_text("")
    con = connect_locked()
    log("Phase 2 apply starting (flag-and-retain).")

    # ---------- 1+2 ALTER ----------
    log("ALTER TABLE main.thyroglobulin_lab_canonical_v1 ADD COLUMN IF NOT EXISTS...")
    con.execute(
        "ALTER TABLE main.thyroglobulin_lab_canonical_v1 "
        "ADD COLUMN IF NOT EXISTS is_in_canonical_cancer_cohort BOOLEAN"
    )
    log("ALTER TABLE main.longitudinal_lab_canonical_v1 ADD COLUMN IF NOT EXISTS...")
    con.execute(
        "ALTER TABLE main.longitudinal_lab_canonical_v1 "
        "ADD COLUMN IF NOT EXISTS is_in_canonical_cancer_cohort BOOLEAN"
    )

    # ---------- 3 Dry-run counts ----------
    log("Dry-run: counting projected splits...")
    tg_total = con.execute(
        "SELECT COUNT(*) FROM main.thyroglobulin_lab_canonical_v1"
    ).fetchone()[0]
    long_total = con.execute(
        "SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1"
    ).fetchone()[0]
    cpm_n = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()[0]
    log(f"  table totals: tg={tg_total} long={long_total} cpm_distinct={cpm_n}")

    # Project counts via JOIN-style EXISTS, mirroring the UPDATE we will run.
    # CPM.research_id is VARCHAR; cast both sides explicitly.
    tg_proj = con.execute(
        """
        WITH cpm AS (
          SELECT TRY_CAST(research_id AS BIGINT) AS rid
          FROM main.canonical_patient_master
        )
        SELECT
          SUM(CASE WHEN TRY_CAST(t.research_id AS BIGINT) IN (SELECT rid FROM cpm)
              THEN 1 ELSE 0 END) AS n_true,
          SUM(CASE WHEN TRY_CAST(t.research_id AS BIGINT) NOT IN (SELECT rid FROM cpm)
              THEN 1 ELSE 0 END) AS n_false_or_unmatched
        FROM main.thyroglobulin_lab_canonical_v1 t
        """
    ).fetchone()
    long_proj = con.execute(
        """
        WITH cpm AS (
          SELECT TRY_CAST(research_id AS BIGINT) AS rid
          FROM main.canonical_patient_master
        )
        SELECT
          SUM(CASE WHEN TRY_CAST(l.research_id AS BIGINT) IN (SELECT rid FROM cpm)
              THEN 1 ELSE 0 END) AS n_true,
          SUM(CASE WHEN TRY_CAST(l.research_id AS BIGINT) NOT IN (SELECT rid FROM cpm)
              THEN 1 ELSE 0 END) AS n_false_or_unmatched
        FROM main.longitudinal_lab_canonical_v1 l
        """
    ).fetchone()
    log(f"  projected tg: true={tg_proj[0]} false={tg_proj[1]}")
    log(f"  projected long: true={long_proj[0]} false={long_proj[1]}")

    # Tolerance check vs Logan's expected splits
    def within(actual: int, expected: int) -> bool:
        return abs(actual - expected) <= max(1, int(expected * TOLERANCE_PCT))

    if not within(tg_proj[0], EXPECTED_TG_TRUE):
        raise SystemExit(
            f"STOP: projected tg TRUE={tg_proj[0]} outside ±5% of "
            f"expected {EXPECTED_TG_TRUE}"
        )
    if not within(tg_proj[1], EXPECTED_TG_FALSE):
        raise SystemExit(
            f"STOP: projected tg FALSE={tg_proj[1]} outside ±5% of "
            f"expected {EXPECTED_TG_FALSE}"
        )
    if not within(long_proj[0], EXPECTED_LONG_TRUE):
        raise SystemExit(
            f"STOP: projected long TRUE={long_proj[0]} outside ±5% of "
            f"expected {EXPECTED_LONG_TRUE}"
        )
    if not within(long_proj[1], EXPECTED_LONG_FALSE):
        raise SystemExit(
            f"STOP: projected long FALSE={long_proj[1]} outside ±5% of "
            f"expected {EXPECTED_LONG_FALSE}"
        )
    log("  dry-run within tolerance — proceeding to UPDATE.")

    # ---------- 4 UPDATEs ----------
    log("Executing UPDATE on main.thyroglobulin_lab_canonical_v1...")
    con.execute(
        """
        UPDATE main.thyroglobulin_lab_canonical_v1 AS t
        SET is_in_canonical_cancer_cohort =
          (TRY_CAST(t.research_id AS BIGINT) IN
            (SELECT TRY_CAST(research_id AS BIGINT)
             FROM main.canonical_patient_master))
        """
    )
    log("Executing UPDATE on main.longitudinal_lab_canonical_v1...")
    con.execute(
        """
        UPDATE main.longitudinal_lab_canonical_v1 AS l
        SET is_in_canonical_cancer_cohort =
          (TRY_CAST(l.research_id AS BIGINT) IN
            (SELECT TRY_CAST(research_id AS BIGINT)
             FROM main.canonical_patient_master))
        """
    )

    # ---------- 5 Sanity ----------
    tg_split = con.execute(
        "SELECT "
        "SUM(CASE WHEN is_in_canonical_cancer_cohort THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN NOT is_in_canonical_cancer_cohort THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN is_in_canonical_cancer_cohort IS NULL THEN 1 ELSE 0 END) "
        "FROM main.thyroglobulin_lab_canonical_v1"
    ).fetchone()
    long_split = con.execute(
        "SELECT "
        "SUM(CASE WHEN is_in_canonical_cancer_cohort THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN NOT is_in_canonical_cancer_cohort THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN is_in_canonical_cancer_cohort IS NULL THEN 1 ELSE 0 END) "
        "FROM main.longitudinal_lab_canonical_v1"
    ).fetchone()
    log(
        f"  TG split: true={tg_split[0]} false={tg_split[1]} null={tg_split[2]} "
        f"(expected ~{EXPECTED_TG_TRUE}/~{EXPECTED_TG_FALSE}/0)"
    )
    log(
        f"  LONG split: true={long_split[0]} false={long_split[1]} null={long_split[2]} "
        f"(expected ~{EXPECTED_LONG_TRUE}/~{EXPECTED_LONG_FALSE}/0)"
    )
    if tg_split[2] or long_split[2]:
        raise SystemExit(
            f"STOP: NULL is_in_canonical_cancer_cohort rows exist "
            f"(tg={tg_split[2]}, long={long_split[2]})"
        )

    # ---------- 6 COMMENTs ----------
    log("Adding COMMENT ON COLUMN for both flag columns...")
    tg_comment = (
        "TRUE if research_id is in main.canonical_patient_master (thyroid "
        "cancer cohort). FALSE = benign-thyroidectomy patient on post-op "
        "Tg surveillance (403 patients, classified non-cancer on 2026-04-17 "
        "via 5-table evidence check: FNA, tumor_episode, synoptic_tumor, "
        "path_synoptic, imaging_nodule — all empty). Rows preserved for "
        "future benign-comparator analyses. See studies/"
        "canonical_cleanup_20260417_resume/tg_orphan_decisions.md."
    )
    long_comment = tg_comment.replace("benign comparator", "benign comparator")
    con.execute(
        f"COMMENT ON COLUMN main.thyroglobulin_lab_canonical_v1."
        f"is_in_canonical_cancer_cohort IS '{tg_comment.replace(chr(39), chr(39)+chr(39))}'"
    )
    con.execute(
        f"COMMENT ON COLUMN main.longitudinal_lab_canonical_v1."
        f"is_in_canonical_cancer_cohort IS '{long_comment.replace(chr(39), chr(39)+chr(39))}'"
    )

    # ---------- 7 Convenience VIEWs ----------
    log("Creating cancer-only convenience VIEWs...")
    con.execute(
        """
        CREATE OR REPLACE VIEW main.thyroglobulin_lab_canonical_cancer_only_v1 AS
          SELECT * FROM main.thyroglobulin_lab_canonical_v1
          WHERE is_in_canonical_cancer_cohort = TRUE
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW main.longitudinal_lab_canonical_cancer_only_v1 AS
          SELECT * FROM main.longitudinal_lab_canonical_v1
          WHERE is_in_canonical_cancer_cohort = TRUE
        """
    )
    n_tg_v = con.execute(
        "SELECT COUNT(*) FROM main.thyroglobulin_lab_canonical_cancer_only_v1"
    ).fetchone()[0]
    n_long_v = con.execute(
        "SELECT COUNT(*) FROM main.longitudinal_lab_canonical_cancer_only_v1"
    ).fetchone()[0]
    log(f"  view counts: tg_cancer_only={n_tg_v} long_cancer_only={n_long_v}")

    # ---------- 8 audit table reclass + resolution column ----------
    log(
        "Updating manuscript_workspace.lab_orphan_audit_v1 classification + "
        "adding resolution column..."
    )
    con.execute(
        "UPDATE manuscript_workspace.lab_orphan_audit_v1 "
        "SET classification = 'likely_non_cancer_flagged_retained' "
        "WHERE classification = 'likely_non_cancer'"
    )
    con.execute(
        "ALTER TABLE manuscript_workspace.lab_orphan_audit_v1 "
        "ADD COLUMN IF NOT EXISTS resolution VARCHAR"
    )
    con.execute(
        "UPDATE manuscript_workspace.lab_orphan_audit_v1 "
        "SET resolution = "
        "'flagged_is_in_canonical_cancer_cohort_FALSE_20260417'"
    )
    audit_state = con.execute(
        "SELECT classification, resolution, COUNT(*) "
        "FROM manuscript_workspace.lab_orphan_audit_v1 "
        "GROUP BY 1, 2 ORDER BY 1, 2"
    ).fetchall()
    log(f"  lab_orphan_audit_v1 state: {audit_state}")

    # ---------- 9 Provenance row for Phase 2 ----------
    log("Inserting Phase 2 provenance row...")
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
    con.execute(
        """
        INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
          (run_id, started_at, ended_at, phases_applied,
           critical_findings_cleared, high_findings_cleared,
           med_findings_cleared, held_for_adjudication)
        VALUES (?, ?::TIMESTAMPTZ, CURRENT_TIMESTAMP,
                'tg_orphan_classification_and_flag',
                '0', '0', '1', '0')
        """,
        [RUN_ID, PHASE2_START],
    )
    n_total = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1"
    ).fetchone()[0]
    log(f"  cpm_reconciliation_provenance_v1 total rows now: {n_total}")

    # ---------- 10 CPM invariant ----------
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit(
            f"CPM invariant regressed: {n_rows}/{n_distinct} != 10871/10871"
        )
    log(f"  CPM invariant re-asserted: {n_rows} rows / {n_distinct} distinct.")
    log("Phase 2 apply complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
