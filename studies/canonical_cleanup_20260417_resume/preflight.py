"""Preflight for canonical cleanup RESUME 20260417.

Asserts the resume-specific invariants from Logan's prompt:

  - canonical_patient_master = 10,871 x 10,871 distinct research_ids
  - cpm_built_at IS NOT NULL for all rows
  - ajcc8_t_stage (corrected) AND ajcc8_t_stage_with_microete_t3b_DEPRECATED
    both exist in CPM
  - comp_vc_paralysis_confirmed, comp_vc_paresis_confirmed,
    comp_hematoma_confirmed, comp_seroma_confirmed,
    comp_chyle_leak_confirmed, comp_wound_infection_confirmed all exist
  - manuscript_workspace has:
      canonical_cleanup_audit_v1 (115 rows)
      cpm_hypopara_adjudication_queue_v1 (4 rows)
      lab_orphan_audit_v1 (403 rows)
      cpm_reconciliation_provenance_v1

Read-only. Writes only studies/canonical_cleanup_20260417_resume/preflight.{json,log}.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG_PATH = HERE / "preflight.log"
JSON_PATH = HERE / "preflight.json"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def main() -> int:
    LOG_PATH.write_text("")
    log("Opening locked MotherDuck connection (token resolution via _md_connect)...")
    con = connect_locked()
    log("Connection ok; CPM 10,871 / distinct invariant confirmed by connect_locked().")

    results: dict = {"checks": {}}
    failures: list[str] = []

    def chk(key: str, ok: bool, observed, expected, note: str = "") -> None:
        results["checks"][key] = {
            "ok": bool(ok),
            "observed": observed,
            "expected": expected,
            "note": note,
        }
        status = "PASS" if ok else "FAIL"
        log(f"  [{status}] {key}: observed={observed!r} expected={expected!r} {note}")
        if not ok:
            failures.append(key)

    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    chk("cpm_rowcount", n_rows == 10871, n_rows, 10871)
    chk("cpm_distinct_research_id", n_distinct == 10871, n_distinct, 10871)

    cpm_cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_patient_master'"
        ).fetchall()
    }

    n_built = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master WHERE cpm_built_at IS NULL"
    ).fetchone()[0]
    chk("cpm_built_at_non_null_for_all_rows", n_built == 0, n_built, 0)

    # Per Logan's 04-17 cleanup: corrected version was promoted to bare name
    # `ajcc8_t_stage`; original is now `..._with_microete_t3b_DEPRECATED`.
    chk(
        "col_ajcc8_t_stage_corrected_exists",
        "ajcc8_t_stage" in cpm_cols,
        "ajcc8_t_stage" in cpm_cols,
        True,
        note="ajcc8_t_stage IS the corrected one after 04-17 phase-4.6 rename",
    )
    chk(
        "col_ajcc8_t_stage_with_microete_t3b_DEPRECATED_exists",
        "ajcc8_t_stage_with_microete_t3b_DEPRECATED" in cpm_cols,
        "ajcc8_t_stage_with_microete_t3b_DEPRECATED" in cpm_cols,
        True,
    )

    required_comp_cols = [
        "comp_vc_paralysis_confirmed",
        "comp_vc_paresis_confirmed",
        "comp_hematoma_confirmed",
        "comp_seroma_confirmed",
        "comp_chyle_leak_confirmed",
        "comp_wound_infection_confirmed",
    ]
    for c in required_comp_cols:
        chk(f"col_{c}_exists", c in cpm_cols, c in cpm_cols, True)

    mw_tables = {
        r[0]: r[1]
        for r in con.execute(
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='manuscript_workspace'"
        ).fetchall()
    }

    def mw_count(name: str) -> int | str:
        if name not in mw_tables:
            return "MISSING"
        try:
            return con.execute(
                f'SELECT COUNT(*) FROM manuscript_workspace."{name}"'
            ).fetchone()[0]
        except Exception as e:  # noqa: BLE001
            return f"ERR: {e}"

    n_audit = mw_count("canonical_cleanup_audit_v1")
    chk(
        "manuscript_workspace.canonical_cleanup_audit_v1_rows",
        n_audit == 115,
        n_audit,
        115,
    )

    n_hypopara = mw_count("cpm_hypopara_adjudication_queue_v1")
    chk(
        "manuscript_workspace.cpm_hypopara_adjudication_queue_v1_rows",
        n_hypopara == 4,
        n_hypopara,
        4,
    )

    n_lab = mw_count("lab_orphan_audit_v1")
    chk(
        "manuscript_workspace.lab_orphan_audit_v1_rows",
        n_lab == 403,
        n_lab,
        403,
    )

    chk(
        "manuscript_workspace.cpm_reconciliation_provenance_v1_exists",
        "cpm_reconciliation_provenance_v1" in mw_tables,
        "cpm_reconciliation_provenance_v1" in mw_tables,
        True,
    )

    # Bonus: count rows in provenance and orphan-cohort-review (for context only).
    extras = {
        "cpm_reconciliation_provenance_v1_rows": (
            mw_count("cpm_reconciliation_provenance_v1")
            if "cpm_reconciliation_provenance_v1" in mw_tables
            else "MISSING"
        ),
        "lab_orphan_cohort_review_v1_present": (
            "lab_orphan_cohort_review_v1" in mw_tables
        ),
    }
    if extras["lab_orphan_cohort_review_v1_present"]:
        extras["lab_orphan_cohort_review_v1_rows"] = mw_count(
            "lab_orphan_cohort_review_v1"
        )
    results["extras"] = extras
    log(f"  Extras: {extras}")

    JSON_PATH.write_text(json.dumps(results, indent=2, default=str))
    log(f"JSON results -> {JSON_PATH}")

    if failures:
        log(f"PREFLIGHT FAILED: {failures}")
        return 2
    log("PREFLIGHT PASSED -- awaiting go/no-go before Phase 1.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
