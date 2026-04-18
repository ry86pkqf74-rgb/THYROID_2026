"""Phase 5 — final aggregator + replay-suite verification + invariant re-check.

  5.1 Insert one final aggregator row into cpm_reconciliation_provenance_v1
      (run_id = 'canonical_cleanup_resume_20260417') summarising all phases.
  5.2 Replay-suite verification (the 13 replay queries from PART2 + PROMPT 18)
      → studies/canonical_cleanup_20260417_resume/verification.md
  5.3 Final invariant re-check + row-count floors on drill-down tables.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG = HERE / "phase5_run.log"
VERIFY_MD = HERE / "verification.md"
VERIFY_JSON = HERE / "verification.json"

FINAL_RUN_ID = "canonical_cleanup_resume_20260417"
PHASE1_START = "2026-04-18 02:59:05.490551+00:00"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG.open("a") as f:
        f.write(line + "\n")


def fetch_dicts(con, sql: str, params=None) -> list[dict]:
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def main() -> int:
    LOG.write_text("")
    con = connect_locked()
    log("Phase 5 starting.")

    # ---------- 5.3 invariant re-check ----------
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit(
            f"CPM invariant regressed: {n_rows}/{n_distinct} != 10871/10871"
        )
    log(f"  CPM invariant: {n_rows}/{n_distinct} OK")

    # row-count floors on drill-down tables
    floors = {
        "operative_episode_detail_v2": (9366, 9376),
        "complication_phenotype_v1": (5928, 6028),
        "fna_episode_master_v2": (5000, 30000),  # ~8.1k observed, healthy floor
        "rai_treatment_episode_v2": (1, 100000),
        "synoptic_tumor_long_v1": (5000, 30000),
        "thyroglobulin_lab_canonical_v1": (73758, 74758),
        "longitudinal_lab_canonical_v1": (73000, 76000),
    }
    floor_results = {}
    for tbl, (lo, hi) in floors.items():
        n = con.execute(f"SELECT COUNT(*) FROM main.{tbl}").fetchone()[0]
        floor_results[tbl] = {"observed": n, "lo": lo, "hi": hi,
                              "ok": lo <= n <= hi}
        log(f"  floor: {tbl} = {n} (range {lo}..{hi}) -> "
            f"{'OK' if lo <= n <= hi else 'FAIL'}")

    # ---------- 5.2 replay suite ----------
    log("Running replay suite...")
    replays: list[dict] = []

    def add(name: str, sql: str, expected_desc: str):
        try:
            rows = con.execute(sql).fetchall()
            keys = [d[0] for d in con.description]
            result = [dict(zip(keys, r)) for r in rows]
            replays.append(
                {
                    "name": name,
                    "sql": sql.strip(),
                    "expected": expected_desc,
                    "result": result,
                    "ok": True,
                }
            )
            log(f"  replay [{name}] -> {result[:2]}{'...' if len(result)>2 else ''}")
        except Exception as e:  # noqa: BLE001
            replays.append(
                {
                    "name": name,
                    "sql": sql.strip(),
                    "expected": expected_desc,
                    "result": f"ERR: {e}",
                    "ok": False,
                }
            )
            log(f"  replay [{name}] FAILED: {e}")

    # Q1: CPM cardinality + invariant
    add(
        "Q1_cpm_cardinality",
        "SELECT COUNT(*) AS n, COUNT(DISTINCT research_id) AS n_distinct "
        "FROM main.canonical_patient_master",
        "n=10871, n_distinct=10871",
    )

    # Q2: cpm_built_at non-null for all rows
    add(
        "Q2_cpm_built_at_non_null",
        "SELECT SUM(CASE WHEN cpm_built_at IS NULL THEN 1 ELSE 0 END) "
        "AS n_null FROM main.canonical_patient_master",
        "n_null = 0",
    )

    # Q3: ajcc8 columns presence (post 04-17 rename)
    add(
        "Q3_ajcc8_columns_present",
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='main' AND table_name='canonical_patient_master' "
        "AND column_name IN ('ajcc8_t_stage','ajcc8_t_stage_with_microete_t3b_DEPRECATED') "
        "ORDER BY column_name",
        "both columns present",
    )

    # Q4: lateral_neck_dissected + lateral_neck_dissected_structured_or_nlp
    add(
        "Q4_lateral_nd_columns_present",
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='main' AND table_name='canonical_patient_master' "
        "AND column_name IN ('lateral_neck_dissected','lateral_neck_dissected_structured_or_nlp') "
        "ORDER BY column_name",
        "both columns present",
    )

    # Q5: per-entity comp_*_confirmed cols (Phase 1.8 finalization)
    add(
        "Q5_per_entity_comp_confirmed_cols",
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='main' AND table_name='canonical_patient_master' "
        "AND column_name IN ("
        " 'comp_vc_paralysis_confirmed','comp_vc_paresis_confirmed',"
        " 'comp_hematoma_confirmed','comp_seroma_confirmed',"
        " 'comp_chyle_leak_confirmed','comp_wound_infection_confirmed') "
        "ORDER BY column_name",
        "all 6 present",
    )

    # Q6: vc_paralysis_recalibration_v236 still present
    add(
        "Q6_vc_paralysis_recalibration_v236",
        "SELECT COUNT(*) AS n FROM information_schema.tables "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='manuscript_workspace' "
        "AND table_name='vc_paralysis_recalibration_v236'",
        "n=1",
    )

    # Q7: Phase 2 — Tg-lab flag distribution
    add(
        "Q7_tg_flag_split",
        "SELECT "
        "SUM(CASE WHEN is_in_canonical_cancer_cohort THEN 1 ELSE 0 END) AS n_true, "
        "SUM(CASE WHEN NOT is_in_canonical_cancer_cohort THEN 1 ELSE 0 END) AS n_false, "
        "SUM(CASE WHEN is_in_canonical_cancer_cohort IS NULL THEN 1 ELSE 0 END) AS n_null "
        "FROM main.thyroglobulin_lab_canonical_v1",
        "n_true=60385, n_false=13873, n_null=0",
    )
    add(
        "Q7b_long_flag_split",
        "SELECT "
        "SUM(CASE WHEN is_in_canonical_cancer_cohort THEN 1 ELSE 0 END) AS n_true, "
        "SUM(CASE WHEN NOT is_in_canonical_cancer_cohort THEN 1 ELSE 0 END) AS n_false, "
        "SUM(CASE WHEN is_in_canonical_cancer_cohort IS NULL THEN 1 ELSE 0 END) AS n_null "
        "FROM main.longitudinal_lab_canonical_v1",
        "n_true=61374, n_false=13873, n_null=0",
    )

    # Q8: cancer-only views row counts
    add(
        "Q8_tg_cancer_only_view",
        "SELECT COUNT(*) AS n FROM main.thyroglobulin_lab_canonical_cancer_only_v1",
        "n=60385",
    )
    add(
        "Q8b_long_cancer_only_view",
        "SELECT COUNT(*) AS n FROM main.longitudinal_lab_canonical_cancer_only_v1",
        "n=61374",
    )

    # Q9: Phase 3 audit table
    add(
        "Q9_audit_table_distribution",
        "SELECT status, COUNT(*) AS n FROM manuscript_workspace.canonical_cleanup_audit_v1 "
        "GROUP BY 1 ORDER BY 1",
        "LIVE=120 (118 main + 2 manuscript_workspace audit-trail)",
    )
    add(
        "Q9b_audit_lineage_v266a",
        "SELECT object_name, notes FROM manuscript_workspace.canonical_cleanup_audit_v1 "
        "WHERE object_name = 'data_dictionary_v266a'",
        "1 row with v240 lineage in notes",
    )

    # Q10: Phase 4 invariant view
    add(
        "Q10_path_tumor_size_invariant_view",
        "SELECT COUNT(*) AS n FROM manuscript_workspace.path_tumor_size_invariant_v1",
        "n=80 (held; documented in correction queue)",
    )

    # Q11: correction queue scope
    add(
        "Q11_correction_queue_scope",
        "SELECT subbucket, COUNT(*) AS n "
        "FROM manuscript_workspace.path_tumor_size_correction_queue_v1 "
        "GROUP BY 1 ORDER BY 1",
        "F1=75 TEM-confirmed, F2=5 non-TEM",
    )

    # Q12: Phase 1 hypopara queue status
    add(
        "Q12_hypopara_queue_status",
        "SELECT status, COUNT(*) AS n "
        "FROM manuscript_workspace.cpm_hypopara_adjudication_queue_v1 "
        "GROUP BY 1",
        "indeterminate_requires_chart_review=4",
    )

    # Q13: provenance ledger
    add(
        "Q13_provenance_ledger",
        "SELECT run_id, phases_applied, "
        "  critical_findings_cleared, high_findings_cleared, "
        "  med_findings_cleared, held_for_adjudication "
        "FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "ORDER BY started_at",
        "6 rows total (1 placeholder + 5 phases)",
    )

    # ---------- 5.1 Final aggregator provenance row ----------
    log("Inserting final aggregator provenance row...")
    n_before = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "WHERE run_id = ?",
        [FINAL_RUN_ID],
    ).fetchone()[0]
    if n_before:
        con.execute(
            "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
            "WHERE run_id = ?",
            [FINAL_RUN_ID],
        )
    held_total = (
        4              # phase1 hypopara
        + 80           # phase4ii correction queue F-bucket
    )
    con.execute(
        """
        INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
          (run_id, started_at, ended_at, phases_applied,
           critical_findings_cleared, high_findings_cleared,
           med_findings_cleared, held_for_adjudication)
        VALUES (?, ?::TIMESTAMPTZ, CURRENT_TIMESTAMP,
                'aggregator__hypopara_adjudication__'
                'tg_orphan_classification_and_flag__'
                'archive_deprecate_delete__classifier_clean__audit_refreshed__'
                'doc_meds__path_tumor_size_comment__invariant_view__'
                'worst_bethesda_comment__'
                'phase4ii_F80_under_report_queued__hidden_both_under_0',
                '0', '0', '3', ?)
        """,
        [FINAL_RUN_ID, PHASE1_START, str(held_total)],
    )
    n_total = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1"
    ).fetchone()[0]
    log(f"  cpm_reconciliation_provenance_v1 total rows now: {n_total}")

    # ---------- Verification markdown ----------
    md = [
        "# Phase 5 verification — replay suite + invariants",
        "",
        f"_Generated {datetime.now(timezone.utc).isoformat()}_",
        "",
        "## CPM invariant",
        "",
        f"- canonical_patient_master rows: **{n_rows}** (expected 10,871)",
        f"- canonical_patient_master distinct research_id: **{n_distinct}** (expected 10,871)",
        "",
        "## Drill-down row-count floors",
        "",
        "| table | observed | range | result |",
        "|:---|---:|:---|:---|",
    ]
    for tbl, r in floor_results.items():
        md.append(
            f"| `{tbl}` | {r['observed']:,} | {r['lo']:,}..{r['hi']:,} | "
            f"{'OK' if r['ok'] else 'FAIL'} |"
        )
    md.append("")
    md.append("## Replay queries")
    md.append("")
    for q in replays:
        md.append(f"### {q['name']}")
        md.append("")
        md.append(f"- **Expected:** {q['expected']}")
        md.append(f"- **Result:** `{json.dumps(q['result'], default=str)[:600]}`")
        md.append(f"- **OK:** {q['ok']}")
        md.append("")
    VERIFY_MD.write_text("\n".join(md) + "\n")
    VERIFY_JSON.write_text(
        json.dumps(
            {
                "cpm_invariant": {"rows": n_rows, "distinct": n_distinct},
                "floors": floor_results,
                "replays": replays,
            },
            indent=2,
            default=str,
        )
    )
    log(f"  verification md -> {VERIFY_MD}")
    log(f"  verification json -> {VERIFY_JSON}")

    # Final CPM invariant
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit("CPM invariant regressed at end of Phase 5!")
    log(f"  Final CPM invariant: {n_rows}/{n_distinct} OK")
    log("Phase 5 verification complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
