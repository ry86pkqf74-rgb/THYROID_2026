#!/usr/bin/env python3
"""Live local DuckDB schema audit for proposal_2to4cm study. Writes inventory + variable map."""
from __future__ import annotations

import csv
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

STUDY_DIR = Path(__file__).resolve().parent

CANDIDATES = [
    "manuscript_cohort_v1",
    "manuscript_patient_summary_v",
    "advanced_features_v3",
    "risk_enriched_mv",
    "tumor_episode_master_v2",
    "operative_episode_detail_v2",
    "fna_episode_master_v2",
    "molecular_test_episode_v2",
    "imaging_exam_summary_v2",
    "imaging_fna_linkage_v3",
    "imaging_fna_linkage_v2",
    "fna_molecular_linkage_v2",
    "fna_molecular_linkage_v3",
    "preop_surgery_linkage_v2",
    "preop_surgery_linkage_v3",
    "surgery_pathology_linkage_v2",
    "surgery_pathology_linkage_v3",
    "patient_cross_domain_timeline_v2",
    "tumor_pathology",
    "benign_pathology",
    "path_synoptics",
    "molecular_testing",
    "genetic_testing",
    "fna_history",
    "us_nodules_tirads",
    "serial_imaging_us",
    "clinical_notes_long",
    "imaging_nodule_long_v2",
    "imaging_nodule_master_v1",
    "patient_level_summary_mv",
    "patient_analysis_resolved_v1",
    "ct_imaging",
    "mri_imaging",
    "linkage_summary_v3",
]


def connect():
    return MotherDuckClient(MotherDuckConfig()).connect_rw()


def main() -> None:
    con = connect()
    rows = []
    for table in CANDIDATES:
        rec = {
            "table_name": table,
            "exists": False,
            "row_count": "",
            "column_count": "",
            "error": "",
        }
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            cols = con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position",
                [table],
            ).fetchall()
            rec["exists"] = True
            rec["row_count"] = int(n)
            rec["column_count"] = len(cols)
            rec["columns_sample"] = ",".join(c[0] for c in cols[:60])
            if len(cols) > 60:
                rec["columns_sample"] += ",..."
        except Exception as e:
            rec["error"] = str(e)[:500]
        rows.append(rec)

    inv_path = STUDY_DIR / "source_inventory.csv"
    with inv_path.open("w", newline="") as f:
        fieldnames = sorted({k for r in rows for k in r.keys()})
        wr = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        wr.writeheader()
        wr.writerows(rows)

    # Variable source map (analysis variables)
    var_rows = [
        {
            "variable": "index_path_size_cm",
            "source_primary": "surgery_pathology_linkage_v3.path_size_cm",
            "source_fallback": "tumor_episode_master_v2.tumor_size_cm; tumor_pathology.histology_1_largest_tumor_cm",
            "join_rule": "surgery_episode_id + score_rank=1 / tumor_ordinal=1 / research_id",
        },
        {
            "variable": "index_preop_nodule_size_cm",
            "source_primary": "imaging_nodule_long_v2.size_cm_max",
            "source_fallback": "imaging_fna_linkage_v3.img_size_cm; imaging_nodule_master_v1.max_dimension_cm",
            "join_rule": "resolved_exam_date <= index_surgery_date; prefer imaging_fna_linkage_v3 score_rank=1",
        },
        {
            "variable": "canonical_preop_imaging_nodule_table",
            "source_primary": "imaging_nodule_long_v2",
            "source_fallback": "imaging_nodule_master_v1",
            "join_reason": "long_v2 has FNA/molecular link IDs + tirads + laterality; v1 higher row count",
        },
        {
            "variable": "initial_surgery_extent",
            "source_primary": "operative_episode_detail_v2.procedure_normalized",
            "source_audit": "path_synoptics.thyroid_procedure (if present)",
            "join_rule": "first qualifying episode by resolved_surgery_date per research_id",
        },
        {
            "variable": "bethesda_category",
            "source_primary": "fna_episode_master_v2.bethesda_category",
            "source_fallback": "fna_history.merged columns if needed",
            "join_rule": "linked_fna_episode_id or temporal preop",
        },
        {
            "variable": "molecular_platform_result",
            "source_primary": "molecular_test_episode_v2",
            "source_fallback": "molecular_testing / genetic_testing",
            "join_rule": "linked_fna_episode_id; exclude inadequate/cancelled/stub Other/other class",
        },
        {
            "variable": "linkage_imaging_fna",
            "source_primary": "imaging_fna_linkage_v3",
            "source_note": "imaging_fna_linkage_v2 absent in live DB",
            "join_rule": "analysis_eligible_link_flag; score_rank",
        },
        {
            "variable": "linkage_preop_surgery",
            "source_primary": "preop_surgery_linkage_v3",
            "source_note": "preop_surgery_linkage_v2 empty (0 rows) in live",
            "join_rule": "preop_episode_id -> surgery_episode_id",
        },
        {
            "variable": "strict_preop_ln_positive",
            "source_primary": "ct_imaging.pathologic_lymph_nodes; mri_imaging.pathologic_lymph_nodes",
            "source_fallback": "fna malignant cervical node (Bethesda/VI + site); see cohort SQL",
            "join_rule": "exam_date <= surgery_anchor",
        },
        {
            "variable": "broad_suspicious_node",
            "source_primary": "imaging_exam_summary_v2.any_suspicious_node",
            "source_fallback": "imaging_nodule_long_v2.suspicious_node_flag",
            "join_rule": "preop exam",
        },
    ]
    var_path = STUDY_DIR / "variable_source_map.csv"
    var_fields = sorted({k for row in var_rows for k in row.keys()})
    with var_path.open("w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=var_fields, extrasaction="ignore")
        wr.writeheader()
        wr.writerows(var_rows)

    # schema_notes.md
    notes = f"""# Schema notes (live local DuckDB audit)

Generated: {datetime.now(timezone.utc).isoformat()}

## Catalog

- Database: `thyroid_master.duckdb` (read-write connection used for SELECT only in this study).

## Discrepancy vs repo docs (AGENTS / pipeline_architecture)

| Doc expectation | Live finding |
|-----------------|--------------|
| `imaging_fna_linkage_v2` | **Missing** — use `imaging_fna_linkage_v3` ({next((r['row_count'] for r in rows if r.get('table_name')=='imaging_fna_linkage_v3'), '?')} rows). |
| `preop_surgery_linkage_v2` | Present but **0 rows** — use `preop_surgery_linkage_v3` ({next((r['row_count'] for r in rows if r.get('table_name')=='preop_surgery_linkage_v3'), '?')} rows). |
| `surgery_pathology_linkage_v2` | **Missing** — use `surgery_pathology_linkage_v3`. |
| `fna_molecular_linkage_v2` | 0 rows in live — prefer `fna_molecular_linkage_v3` where populated. |

## Canonical preop nodule table for this study

**Primary:** `imaging_nodule_long_v2`

- Row count: {next((r['row_count'] for r in rows if r.get('table_name')=='imaging_nodule_long_v2'), 'N/A')}
- Rationale: episode-level grain with `size_cm_max`, `resolved_exam_date`, `laterality`, `tirads_score`, `linked_fna_episode_id`, `linked_molecular_episode_id`, `suspicious_node_flag`, aligned with `imaging_exam_summary_v2`.

**Secondary / cross-check:** `imaging_nodule_master_v1` (higher row count; `max_dimension_cm`, `fna_link_score_v3`) for size fallback when long table lacks a row.

## Inventory file

See `source_inventory.csv` for all candidate objects, row counts, and errors.
"""
    (STUDY_DIR / "schema_notes.md").write_text(notes)

    manifest = {
        "audit_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "duckdb_version": con.execute("SELECT version()").fetchone()[0],
        "git_sha": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "inventory_csv": str(inv_path.relative_to(ROOT)),
        "variable_map_csv": str(var_path.relative_to(ROOT)),
    }
    (STUDY_DIR / "analysis_manifest.json").write_text(json.dumps(manifest, indent=2))
    print("Wrote:", inv_path, var_path, STUDY_DIR / "schema_notes.md")


if __name__ == "__main__":
    main()
