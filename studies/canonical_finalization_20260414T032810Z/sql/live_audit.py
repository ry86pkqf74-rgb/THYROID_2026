"""Comprehensive live MotherDuck audit for canonical finalization."""
import csv
import json
import os
import sys
import datetime as dt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from motherduck_client import get_token

WORKDIR = os.path.dirname(os.path.dirname(__file__))
ARTIFACTS = os.path.join(WORKDIR, 'artifacts')
LOGS = os.path.join(WORKDIR, 'logs')
SQL_DIR = os.path.join(WORKDIR, 'sql')

def _save_csv(rows, cols, name):
    path = os.path.join(ARTIFACTS, name)
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print(f"  -> {path} ({len(rows)} rows)")

def _save_json(obj, name):
    path = os.path.join(ARTIFACTS, name)
    with open(path, 'w') as f:
        json.dump(obj, f, indent=2, default=str)
    print(f"  -> {path}")

def _safe_query(con, sql, label):
    try:
        result = con.execute(sql)
        cols = [d[0] for d in result.description]
        rows = result.fetchall()
        return cols, rows
    except Exception as e:
        print(f"  WARNING: {label} failed: {e}")
        return [], []

def main():
    token = get_token()
    if not token:
        print("FAIL-CLOSED: No MotherDuck token")
        sys.exit(1)

    import duckdb
    con = duckdb.connect(f'md:Thyroid 2026?motherduck_token={token}')
    print(f"Connected: {con.execute('SELECT current_database()').fetchone()[0]}")
    stamp = dt.datetime.now(dt.timezone.utc).isoformat()
    results = {"audit_timestamp": stamp, "sections": {}}

    # ── A) RELEASE LEDGER ──
    print("\n=== A) RELEASE LEDGER ===")
    cols, rows = _safe_query(con, """
        SELECT release_tag, git_sha, created_at
        FROM qa.release_manifest
        ORDER BY created_at DESC NULLS LAST
        LIMIT 20
    """, "release_manifest")
    _save_csv(rows, cols, "release_manifest_latest.csv")
    results["sections"]["release_manifest"] = {"rows": len(rows), "latest_tag": str(rows[0][0]) if rows else None}

    cols2, rows2 = _safe_query(con, "SELECT DISTINCT git_sha FROM qa.release_manifest", "distinct_sha")
    results["sections"]["release_manifest_distinct_sha"] = [str(r[0]) for r in rows2]

    cols3, rows3 = _safe_query(con, """
        SELECT release_tag, COUNT(*) AS n FROM qa.release_manifest
        GROUP BY 1 HAVING COUNT(*) > 1 ORDER BY n DESC
    """, "duplicate_tags")
    results["sections"]["duplicate_release_tags"] = [{"tag": str(r[0]), "count": r[1]} for r in rows3]

    # ── B) CORE CANONICAL OBJECT COUNTS ──
    print("\n=== B) CORE CANONICAL OBJECT COUNTS ===")
    count_queries = [
        ("canonical_extracted_fact_long_v2", "main.canonical_extracted_fact_long_v2"),
        ("canonical_fact_quarantine_v2", "main.canonical_fact_quarantine_v2"),
        ("note_extraction_runs", "main.note_extraction_runs"),
        ("master_fact_long_verified_v1", "main.master_fact_long_verified_v1"),
        ("master_patient_rollup_verified_v1", "main.master_patient_rollup_verified_v1"),
        ("master_source_lineage_v1", "main.master_source_lineage_v1"),
        ("longitudinal_lab_canonical_v1", "main.longitudinal_lab_canonical_v1"),
        ("longitudinal_lab_deduped_v", "main.longitudinal_lab_deduped_v"),
        ("imaging_nodule_master_v1", "main.imaging_nodule_master_v1"),
        ("fna_episode_master_v2", "main.fna_episode_master_v2"),
        ("v_fna_episode_bethesda_resolved_v1", "main.v_fna_episode_bethesda_resolved_v1"),
        ("specimen_master_v1", "main.specimen_master_v1"),
        ("specimen_tumor_focus_v1", "main.specimen_tumor_focus_v1"),
        ("specimen_genomic_assay_v1", "main.specimen_genomic_assay_v1"),
        ("fhir_bundle_specimen_export_v1", "main.fhir_bundle_specimen_export_v1"),
    ]
    canonical_counts = {}
    count_rows = []
    for label, table in count_queries:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except Exception as e:
            n = f"ERROR: {e}"
        canonical_counts[label] = n
        count_rows.append((label, n))
        print(f"  {label}: {n}")
    _save_csv(count_rows, ["object", "row_count"], "canonical_counts_before.csv")
    _save_json(canonical_counts, "canonical_counts_before.json")
    results["sections"]["canonical_counts"] = canonical_counts

    # ── C) PRESENTATION-LAYER PARITY ──
    print("\n=== C) PRESENTATION-LAYER PARITY ===")
    cols, rows = _safe_query(con, """
        SELECT
          (SELECT COUNT(*) FROM main.canonical_extracted_fact_long_v2) AS canonical_facts,
          (SELECT COUNT(*) FROM main.master_fact_long_verified_v1) AS master_facts,
          (SELECT COUNT(*) FROM main.master_source_lineage_v1) AS lineage_rows,
          (SELECT COUNT(DISTINCT research_id) FROM main.master_fact_long_verified_v1) AS distinct_patients,
          (SELECT COUNT(*) FROM main.master_patient_rollup_verified_v1) AS patient_rollup_rows
    """, "parity")
    if rows:
        parity = dict(zip(cols, rows[0]))
        print(f"  Parity: {parity}")
        results["sections"]["parity"] = parity
        _save_json(parity, "parity_check.json")

    # ── D) LLM EXTRACTION RUN INVENTORY ──
    print("\n=== D) LLM EXTRACTION RUN INVENTORY ===")
    cols, rows = _safe_query(con, """
        SELECT run_id, success, extractor_build_version, started_at, finished_at
        FROM main.note_extraction_runs
        ORDER BY TRY_CAST(started_at AS TIMESTAMPTZ) DESC NULLS LAST, run_id DESC
    """, "extraction_runs")
    _save_csv(rows, cols, "run_inventory_before.csv")
    results["sections"]["extraction_runs"] = len(rows)

    cols2, rows2 = _safe_query(con, """
        SELECT
          COALESCE(CAST(extraction_run_id AS VARCHAR), '__NULL__') AS extraction_run_id,
          COUNT(*) AS n_facts,
          COUNT(DISTINCT research_id) AS n_patients,
          COUNT(DISTINCT fact_domain) AS n_domains,
          MIN(extracted_at) AS min_extracted_at,
          MAX(extracted_at) AS max_extracted_at
        FROM main.canonical_extracted_fact_long_v2
        GROUP BY 1 ORDER BY n_facts DESC, extraction_run_id
    """, "facts_by_run")
    _save_csv(rows2, cols2, "facts_by_extraction_run.csv")

    cols3, rows3 = _safe_query(con, """
        SELECT
          f.extraction_run_id, r.success, r.extractor_build_version, COUNT(*) AS n_facts
        FROM main.canonical_extracted_fact_long_v2 f
        LEFT JOIN main.note_extraction_runs r
          ON CAST(f.extraction_run_id AS VARCHAR) = CAST(r.run_id AS VARCHAR)
        GROUP BY 1,2,3 ORDER BY n_facts DESC
    """, "facts_joined_runs")
    _save_csv(rows3, cols3, "facts_joined_runs.csv")

    # NULL extraction_run_id facts
    cols4, rows4 = _safe_query(con, """
        SELECT COUNT(*) AS null_run_id_facts
        FROM main.canonical_extracted_fact_long_v2
        WHERE extraction_run_id IS NULL
    """, "null_run_id")
    null_run = rows4[0][0] if rows4 else "ERROR"
    results["sections"]["null_extraction_run_id_facts"] = null_run
    print(f"  Facts with NULL extraction_run_id: {null_run}")

    # Orphan runs (runs with zero canonical facts)
    cols5, rows5 = _safe_query(con, """
        SELECT r.run_id, r.success, r.extractor_build_version
        FROM main.note_extraction_runs r
        LEFT JOIN main.canonical_extracted_fact_long_v2 f
          ON CAST(r.run_id AS VARCHAR) = CAST(f.extraction_run_id AS VARCHAR)
        WHERE f.extraction_run_id IS NULL
    """, "orphan_runs")
    results["sections"]["orphan_runs"] = len(rows5)
    print(f"  Orphan runs (no matching facts): {len(rows5)}")

    # ── E) DOMAIN / TABLE INVENTORY ──
    print("\n=== E) DOMAIN / TABLE INVENTORY ===")
    cols, rows = _safe_query(con, """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog = 'Thyroid 2026'
        ORDER BY table_schema, table_name
    """, "table_inventory")
    _save_csv(rows, cols, "domain_inventory_before.csv")
    results["sections"]["total_tables_views"] = len(rows)
    print(f"  Total tables/views: {len(rows)}")

    # Row counts for key schemas
    for schema in ['main', 'qa', 'v2_stage']:
        schema_tables = [r for r in rows if r[0] == schema]
        print(f"  Schema '{schema}': {len(schema_tables)} objects")

    # ── F) LINEAGE COMPLETENESS ──
    print("\n=== F) LINEAGE COMPLETENESS ===")
    cols, rows = _safe_query(con, """
        SELECT
          COUNT(*) AS facts_total,
          COUNT(*) FILTER (WHERE source_object_id IS NOT NULL) AS facts_with_source_object,
          COUNT(*) FILTER (WHERE source_domain IS NOT NULL) AS facts_with_source_domain,
          COUNT(*) FILTER (WHERE extraction_run_id IS NOT NULL) AS facts_with_extraction_run_id
        FROM main.master_fact_long_verified_v1
    """, "lineage_facts")
    if rows:
        lineage_facts = dict(zip(cols, rows[0]))
        print(f"  Facts lineage: {lineage_facts}")
        results["sections"]["lineage_facts"] = lineage_facts

    cols2, rows2 = _safe_query(con, """
        SELECT
          COUNT(*) AS lineage_total,
          COUNT(*) FILTER (WHERE source_object_id IS NOT NULL) AS lineage_with_source_object,
          COUNT(*) FILTER (WHERE source_domain IS NOT NULL) AS lineage_with_source_domain,
          COUNT(*) FILTER (WHERE extraction_run_id IS NOT NULL) AS lineage_with_extraction_run_id
        FROM main.master_source_lineage_v1
    """, "lineage_source")
    if rows2:
        lineage_source = dict(zip(cols2, rows2[0]))
        print(f"  Source lineage: {lineage_source}")
        results["sections"]["lineage_source"] = lineage_source

    _save_json({"facts": lineage_facts if rows else {}, "source": lineage_source if rows2 else {}},
               "lineage_completeness.json")

    # 1:1 fact-to-lineage check
    cols3, rows3 = _safe_query(con, """
        SELECT
          (SELECT COUNT(*) FROM main.master_fact_long_verified_v1) AS fact_rows,
          (SELECT COUNT(*) FROM main.master_source_lineage_v1) AS lineage_rows
    """, "1to1_check")
    if rows3:
        results["sections"]["fact_lineage_1to1"] = {
            "fact_rows": rows3[0][0], "lineage_rows": rows3[0][1],
            "match": rows3[0][0] == rows3[0][1]
        }
        print(f"  1:1 check: facts={rows3[0][0]} lineage={rows3[0][1]} match={rows3[0][0]==rows3[0][1]}")

    _save_csv(
        [(lineage_facts.get('facts_total',''), lineage_facts.get('facts_with_source_object',''),
          lineage_facts.get('facts_with_source_domain',''), lineage_facts.get('facts_with_extraction_run_id',''),
          lineage_source.get('lineage_total',''), lineage_source.get('lineage_with_source_object',''))],
        ["facts_total", "facts_w_source_obj", "facts_w_source_domain", "facts_w_run_id",
         "lineage_total", "lineage_w_source_obj"],
        "lineage_audit_before.csv"
    )

    # ── G) DUPLICATE AUDITS ──
    print("\n=== G) DUPLICATE AUDITS ===")
    cols, rows = _safe_query(con, """
        SELECT fact_id, COUNT(*) AS n
        FROM main.master_fact_long_verified_v1
        GROUP BY 1 HAVING COUNT(*) > 1
        ORDER BY n DESC, fact_id LIMIT 200
    """, "dup_fact_id")
    results["sections"]["duplicate_fact_ids"] = len(rows)
    print(f"  Duplicate fact_id groups: {len(rows)}")

    cols2, rows2 = _safe_query(con, """
        SELECT research_id, source_domain, source_object_id, entity_type, entity_value_norm,
               entity_date, COUNT(*) AS n
        FROM main.master_fact_long_verified_v1
        GROUP BY 1,2,3,4,5,6 HAVING COUNT(*) > 1
        ORDER BY n DESC LIMIT 200
    """, "dup_natural_key")
    results["sections"]["duplicate_natural_key_groups"] = len(rows2)
    print(f"  Duplicate natural key groups: {len(rows2)}")
    if rows2:
        _save_csv(rows2, cols2, "duplicate_natural_keys.csv")

    cols3, rows3 = _safe_query(con, """
        SELECT research_id, source_domain, source_object_id, entity_type, entity_date,
               extraction_run_id, COUNT(*) AS n
        FROM main.master_source_lineage_v1
        GROUP BY 1,2,3,4,5,6 HAVING COUNT(*) > 1
        ORDER BY n DESC LIMIT 200
    """, "dup_lineage")
    results["sections"]["duplicate_lineage_groups"] = len(rows3)
    print(f"  Duplicate lineage groups: {len(rows3)}")

    _save_csv(
        [("fact_id_dups", len(rows)), ("natural_key_dups", len(rows2)), ("lineage_dups", len(rows3))],
        ["audit_type", "groups_found"],
        "duplicate_audit.csv"
    )

    # ── H) REVIEW / GOVERNANCE STATE ──
    print("\n=== H) REVIEW / GOVERNANCE STATE ===")
    review_data = {}
    for label, sql in [
        ("mrq_total", "SELECT COUNT(*) FROM qa.manual_review_queue"),
        ("mrq_pending", "SELECT COUNT(*) FROM qa.manual_review_queue WHERE verification_status IS NULL"),
    ]:
        _, r = _safe_query(con, sql, label)
        review_data[label] = r[0][0] if r else "ERROR"
        print(f"  {label}: {review_data[label]}")

    cols, rows = _safe_query(con, """
        SELECT verification_status, COUNT(*) AS n
        FROM qa.manual_review_queue GROUP BY 1 ORDER BY n DESC, 1
    """, "mrq_status")
    _save_csv(rows, cols, "review_queue_status_before.csv")
    review_data["status_histogram"] = {str(r[0]): r[1] for r in rows}
    print(f"  Status histogram: {review_data['status_histogram']}")

    cols2, rows2 = _safe_query(con, """
        SELECT run_label, COUNT(*) AS n
        FROM qa.manual_review_queue GROUP BY 1 ORDER BY n DESC, 1
    """, "mrq_run_labels")
    _save_csv(rows2, cols2, "review_queue_run_labels.csv")
    review_data["run_labels"] = {str(r[0]): r[1] for r in rows2}
    print(f"  Run labels: {review_data['run_labels']}")

    cols3, rows3 = _safe_query(con, """
        SELECT domain, COUNT(*) AS n
        FROM qa.manual_review_queue GROUP BY 1 ORDER BY n DESC, 1
    """, "mrq_domains")
    _save_csv(rows3, cols3, "review_queue_domains.csv")

    cols4, rows4 = _safe_query(con, """
        SELECT decision_batch_id, COUNT(*) AS n
        FROM qa.promotion_review_decisions GROUP BY 1 ORDER BY n DESC, 1
    """, "prd_batches")
    _save_csv(rows4, cols4, "promotion_decisions_batches.csv")
    review_data["promotion_batches"] = {str(r[0]): r[1] for r in rows4}

    _save_csv(
        [(review_data['mrq_total'], review_data['mrq_pending'],
          json.dumps(review_data['status_histogram']),
          json.dumps(review_data['run_labels']))],
        ["total", "pending", "status_histogram", "run_labels"],
        "review_queue_before.csv"
    )
    results["sections"]["review"] = review_data

    # ── I) REVIEW GRAIN AUDIT ──
    print("\n=== I) REVIEW GRAIN AUDIT ===")
    cols, rows = _safe_query(con, """
        SELECT review_grain, review_status_source, COUNT(*) AS n
        FROM main.master_fact_long_verified_v1
        GROUP BY 1, 2 ORDER BY n DESC
    """, "review_grain")
    if rows:
        results["sections"]["review_grain"] = [{"grain": str(r[0]), "source": str(r[1]), "n": r[2]} for r in rows]
        print(f"  Review grain distribution: {results['sections']['review_grain']}")
    else:
        # columns may not exist yet
        print("  WARNING: review_grain columns not found in master_fact_long_verified_v1")
        results["sections"]["review_grain"] = "columns_not_found"

    # ── J) IMAGING / FNA LINKAGE ──
    print("\n=== J) IMAGING / FNA LINKAGE ===")
    for label, sql in [
        ("imaging_linkage_total", "SELECT COUNT(*) FROM main.v_imaging_nodule_linkage_classification_v1"),
        ("fna_bethesda_total", "SELECT COUNT(*) FROM main.v_fna_episode_bethesda_resolved_v1"),
    ]:
        _, r = _safe_query(con, sql, label)
        val = r[0][0] if r else "NOT_FOUND"
        results["sections"][label] = val
        print(f"  {label}: {val}")

    cols, rows = _safe_query(con, """
        SELECT unresolved_linkage_gap, COUNT(*) AS n
        FROM main.v_imaging_nodule_linkage_classification_v1
        GROUP BY 1 ORDER BY 2 DESC, 1
    """, "imaging_gaps")
    if rows:
        _save_csv(rows, cols, "imaging_linkage_gaps.csv")
        results["sections"]["imaging_linkage_gaps"] = {str(r[0]): r[1] for r in rows}

    cols2, rows2 = _safe_query(con, """
        SELECT bethesda_numeric_resolved, COUNT(*) AS n
        FROM main.v_fna_episode_bethesda_resolved_v1
        GROUP BY 1 ORDER BY 2 DESC, 1
    """, "fna_bethesda_dist")
    if rows2:
        _save_csv(rows2, cols2, "fna_bethesda_distribution.csv")

    _save_csv(
        [("imaging_linkage", results["sections"].get("imaging_linkage_total", "")),
         ("fna_bethesda", results["sections"].get("fna_bethesda_total", ""))],
        ["surface", "row_count"],
        "linkage_audit_before.csv"
    )

    # ── K) SPECIMEN / FHIR ──
    print("\n=== K) SPECIMEN / FHIR ===")
    specimen_objects = [
        ("broken_fhir_refs", "qa.v_diag_specimen_fhir_broken_refs_v1"),
        ("review_burden", "qa.v_diag_specimen_review_burden_v1"),
        ("genomic_link_review", "qa.specimen_genomic_link_review_v1"),
        ("focus_qa_metrics", "qa.t_diag_specimen_focus_qa_metrics_v1"),
    ]
    for label, obj in specimen_objects:
        _, r = _safe_query(con, f"SELECT COUNT(*) FROM {obj}", label)
        val = r[0][0] if r else "NOT_FOUND"
        results["sections"][f"specimen_{label}"] = val
        print(f"  {label}: {val}")

    # Export broken refs if non-zero
    if results["sections"].get("specimen_broken_fhir_refs", 0) not in (0, "NOT_FOUND"):
        cols, rows = _safe_query(con, f"SELECT * FROM qa.v_diag_specimen_fhir_broken_refs_v1 LIMIT 100", "broken_refs_detail")
        if rows:
            _save_csv(rows, cols, "specimen_broken_refs_detail.csv")

    # Review burden summary
    cols, rows = _safe_query(con, "SELECT * FROM qa.v_diag_specimen_review_burden_v1", "review_burden_detail")
    if rows:
        _save_csv(rows, cols, "specimen_review_burden.csv")

    # ── L) QUERY HISTORY TELEMETRY ──
    print("\n=== L) QUERY HISTORY TELEMETRY ===")
    cols, rows = _safe_query(con, """
        SELECT
          user_agent,
          COUNT(*) AS n_queries,
          MIN(start_time) AS earliest,
          MAX(start_time) AS latest
        FROM md_information_schema.query_history
        WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '30 days'
          AND (user_agent LIKE 'THYROID_2026%'
               OR user_agent LIKE '%specimen_fhir%')
        GROUP BY 1 ORDER BY n_queries DESC
    """, "query_history")
    if rows:
        _save_csv(rows, cols, "query_history_before.csv")
        results["sections"]["query_history_agents"] = {str(r[0]): r[1] for r in rows}
        print(f"  Recent query agents: {len(rows)} agents found")
    else:
        print("  No matching query history or table not accessible")
        results["sections"]["query_history_agents"] = "not_available"

    # ── M) LOCAL VS LIVE RECONCILIATION (counts comparison) ──
    print("\n=== M) RECONCILIATION SUMMARY ===")
    checked_in_counts = {
        "canonical_extracted_fact_long_v2": 55500,
        "canonical_fact_quarantine_v2": 199,
        "note_extraction_runs": 3,
        "master_fact_long_verified_v1": 55500,
        "master_patient_rollup_verified_v1": 5141,
        "master_source_lineage_v1": 55500,
        "longitudinal_lab_canonical_v1": 77960,
        "longitudinal_lab_deduped_v": 56198,
        "specimen_master_v1": 10139,
        "specimen_tumor_focus_v1": 11103,
        "specimen_genomic_assay_v1": 10370,
        "fhir_bundle_specimen_export_v1": 10139,
    }
    recon = []
    for obj, checked_in in checked_in_counts.items():
        live = canonical_counts.get(obj, "MISSING")
        match = live == checked_in if isinstance(live, int) else False
        recon.append((obj, checked_in, live, "MATCH" if match else "DRIFT"))
        if not match:
            print(f"  DRIFT: {obj} checked_in={checked_in} live={live}")
    _save_csv(recon, ["object", "checked_in_count", "live_count", "status"], "reconciliation_before.csv")
    results["sections"]["reconciliation_drift"] = [r for r in recon if r[3] == "DRIFT"]

    # ── SAVE MASTER RESULTS ──
    _save_json(results, "before_state.json")
    print(f"\n=== AUDIT COMPLETE === {stamp}")
    print(f"Artifacts saved to: {ARTIFACTS}")

    con.close()

if __name__ == "__main__":
    main()
