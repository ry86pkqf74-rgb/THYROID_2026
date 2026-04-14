#!/usr/bin/env python3
"""Live MotherDuck SQL probes for canonical lakehouse finalization audit.

Fail-closed: aborts if MotherDuck connection fails.
Saves all results to CSV/JSON under the same directory as this script.
"""
import csv, json, os, sys, pathlib, datetime as _dt

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from motherduck_client import get_token

OUT = pathlib.Path(__file__).resolve().parent
SUMMARY = {}

def connect():
    import duckdb
    tok = get_token()
    if not tok:
        sys.exit("FAIL-CLOSED: MotherDuck token not available")
    con = duckdb.connect(f"md:Thyroid 2026?motherduck_token={tok}")
    try:
        con.execute("SET enable_progress_bar = false")
    except Exception:
        pass
    return con

def run_query(con, name, sql, *, save_csv=True, save_json=False):
    print(f"  [{name}] running...")
    try:
        rows = con.execute(sql).fetchall()
        cols = [d[0] for d in con.description]
    except Exception as e:
        print(f"  [{name}] ERROR: {e}")
        SUMMARY[name] = {"error": str(e)}
        return []
    dicts = [dict(zip(cols, r)) for r in rows]
    if save_csv:
        p = OUT / f"{name}.csv"
        with open(p, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(dicts)
    if save_json:
        p = OUT / f"{name}.json"
        with open(p, "w") as f:
            json.dump(dicts, f, indent=2, default=str)
    SUMMARY[name] = {"rows": len(dicts), "columns": cols}
    for d in dicts[:5]:
        print(f"    {d}")
    if len(dicts) > 5:
        print(f"    ... ({len(dicts)} total rows)")
    return dicts

def main():
    con = connect()
    print("=== Live MotherDuck SQL Probes ===\n")

    run_query(con, "01_release_ledger", """
        SELECT release_tag, git_sha, created_at
        FROM qa.release_manifest
        ORDER BY created_at DESC NULLS LAST
        LIMIT 20
    """)

    run_query(con, "02_core_row_counts", """
        SELECT 'canonical_extracted_fact_long_v2' AS obj, COUNT(*) AS n FROM main.canonical_extracted_fact_long_v2
        UNION ALL SELECT 'canonical_fact_quarantine_v2', COUNT(*) FROM main.canonical_fact_quarantine_v2
        UNION ALL SELECT 'note_extraction_runs', COUNT(*) FROM main.note_extraction_runs
        UNION ALL SELECT 'master_fact_long_verified_v1', COUNT(*) FROM main.master_fact_long_verified_v1
        UNION ALL SELECT 'master_patient_rollup_verified_v1', COUNT(*) FROM main.master_patient_rollup_verified_v1
        UNION ALL SELECT 'master_source_lineage_v1', COUNT(*) FROM main.master_source_lineage_v1
        UNION ALL SELECT 'longitudinal_lab_canonical_v1', COUNT(*) FROM main.longitudinal_lab_canonical_v1
        UNION ALL SELECT 'longitudinal_lab_deduped_v', COUNT(*) FROM main.longitudinal_lab_deduped_v
        UNION ALL SELECT 'specimen_master_v1', COUNT(*) FROM main.specimen_master_v1
        UNION ALL SELECT 'specimen_tumor_focus_v1', COUNT(*) FROM main.specimen_tumor_focus_v1
        UNION ALL SELECT 'specimen_genomic_assay_v1', COUNT(*) FROM main.specimen_genomic_assay_v1
        UNION ALL SELECT 'fhir_bundle_specimen_export_v1', COUNT(*) FROM main.fhir_bundle_specimen_export_v1
    """)

    run_query(con, "03_presentation_parity", """
        SELECT
          (SELECT COUNT(*) FROM main.canonical_extracted_fact_long_v2) AS canonical_facts,
          (SELECT COUNT(*) FROM main.master_fact_long_verified_v1) AS master_facts,
          (SELECT COUNT(*) FROM main.master_source_lineage_v1) AS lineage_rows,
          (SELECT COUNT(DISTINCT research_id) FROM main.master_fact_long_verified_v1) AS distinct_patients_from_master,
          (SELECT COUNT(*) FROM main.master_patient_rollup_verified_v1) AS patient_rollup_rows
    """)

    run_query(con, "04_lineage_completeness", """
        SELECT
          COUNT(*) AS facts_total,
          COUNT(*) FILTER (WHERE source_object_id IS NOT NULL) AS facts_with_source_object,
          COUNT(*) FILTER (WHERE source_domain IS NOT NULL) AS facts_with_source_domain,
          COUNT(*) FILTER (WHERE note_row_id IS NOT NULL) AS facts_with_note_row_id
        FROM main.master_source_lineage_v1
    """)

    run_query(con, "05_duplicate_fact_id", """
        SELECT fact_id, COUNT(*) AS n
        FROM main.master_fact_long_verified_v1
        GROUP BY 1
        HAVING COUNT(*) > 1
        ORDER BY n DESC, fact_id
        LIMIT 100
    """)

    run_query(con, "06_duplicate_natural_key", """
        SELECT
          research_id, source_domain, source_object_id,
          entity_type, entity_value_norm, entity_date,
          COUNT(*) AS n
        FROM main.master_fact_long_verified_v1
        GROUP BY 1,2,3,4,5,6
        HAVING COUNT(*) > 1
        ORDER BY n DESC
        LIMIT 100
    """)

    run_query(con, "07a_mrq_totals", """
        SELECT COUNT(*) AS total_rows FROM qa.manual_review_queue
    """)
    run_query(con, "07b_mrq_pending", """
        SELECT COUNT(*) AS pending_rows FROM qa.manual_review_queue WHERE verification_status IS NULL
    """)
    run_query(con, "07c_mrq_by_status", """
        SELECT verification_status, COUNT(*) AS n
        FROM qa.manual_review_queue
        GROUP BY 1 ORDER BY n DESC, 1
    """)
    run_query(con, "07d_mrq_by_run_label", """
        SELECT run_label, COUNT(*) AS n
        FROM qa.manual_review_queue
        GROUP BY 1 ORDER BY n DESC, 1
    """)
    run_query(con, "07e_mrq_by_domain", """
        SELECT domain, COUNT(*) AS n
        FROM qa.manual_review_queue
        GROUP BY 1 ORDER BY n DESC, 1
    """)

    run_query(con, "08_governance_decisions", """
        SELECT decision_batch_id, COUNT(*) AS n
        FROM qa.promotion_review_decisions
        GROUP BY 1 ORDER BY n DESC, 1
    """)

    run_query(con, "09_extraction_linkage", """
        SELECT
          r.run_id,
          r.run_label,
          r.domain,
          r.created_at,
          (SELECT COUNT(*) FROM main.canonical_extracted_fact_long_v2 f
           WHERE f.extraction_run_id = r.run_id) AS facts_linked
        FROM main.note_extraction_runs r
        ORDER BY r.created_at DESC
    """)

    try:
        run_query(con, "10a_specimen_broken_refs", """
            SELECT * FROM qa.v_diag_specimen_fhir_broken_refs_v1 LIMIT 100
        """)
    except Exception:
        SUMMARY["10a_specimen_broken_refs"] = {"error": "view not found"}

    try:
        run_query(con, "10b_specimen_review_burden", """
            SELECT * FROM qa.v_diag_specimen_review_burden_v1
        """)
    except Exception:
        SUMMARY["10b_specimen_review_burden"] = {"error": "view not found"}

    # Save summary
    with open(OUT / "probe_summary.json", "w") as f:
        json.dump({
            "timestamp": _dt.datetime.utcnow().isoformat() + "Z",
            "probes": SUMMARY,
        }, f, indent=2, default=str)

    print(f"\n=== Done: {len(SUMMARY)} probes saved to {OUT} ===")

if __name__ == "__main__":
    main()
