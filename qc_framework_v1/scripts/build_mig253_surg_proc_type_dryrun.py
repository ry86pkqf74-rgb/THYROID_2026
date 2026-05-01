#!/usr/bin/env python3
"""Build mig_253 dry-run artifacts for CPM surgical procedure type fill.

This script is read-only against MotherDuck production tables. It creates only
session-scoped TEMP tables, then writes local CSV/JSON/Markdown artifacts with
aggregate counts and non-PHI residual review rows.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts._md_connect import connect_locked  # noqa: E402


RUN_ID = "mig_253_surg_procedure_type_fill_20260501"
MIGRATION_PATH = "qc_framework_v1/migrations/253_surg_procedure_type_fill_20260501.sql"


RESOLUTION_SQL = r"""
CREATE TEMP TABLE _mig253_null_pts AS
SELECT
  CAST(research_id AS VARCHAR) AS research_id,
  first_surgery_date,
  n_surgeries,
  gland_weight_final_g,
  histology_final,
  nsqip_thyroidectomy_has_data,
  nsqip_cpt_code,
  nsqip_cpt_description
FROM main.canonical_patient_master
WHERE surg_procedure_type IS NULL
  AND surg_total_thyroidectomy IS NULL
  AND surg_hemithyroidectomy IS NULL;

CREATE TEMP TABLE _mig253_event_source AS
SELECT
  n.research_id,
  COALESCE(BOOL_OR(
    regexp_matches(LOWER(COALESCE(op.procedure_normalized, op.procedure_raw, '')),
      'total[^a-z0-9]*thyroidectomy|total or complete|completion|removal of all remaining|substernal thyroid|thyroidectomy including substernal')
  ), FALSE) AS has_total,
  COALESCE(BOOL_OR(
    regexp_matches(LOWER(COALESCE(op.procedure_normalized, op.procedure_raw, '')),
      'hemi[^a-z0-9]*thyroidectomy|lobectomy|partial[^a-z0-9]*thyroid')
  ), FALSE) AS has_hemi,
  COALESCE(BOOL_OR(
    regexp_matches(LOWER(COALESCE(op.procedure_normalized, op.procedure_raw, '')), 'isthmusectomy')
  ), FALSE) AS has_isthmus,
  COUNT(*) FILTER (WHERE COALESCE(op.procedure_normalized, op.procedure_raw) IS NOT NULL) > 0 AS has_other,
  COUNT(*) FILTER (WHERE COALESCE(op.procedure_normalized, op.procedure_raw) IS NOT NULL) AS n_evidence_rows,
  STRING_AGG(DISTINCT COALESCE(op.procedure_normalized, op.procedure_raw), ' | ')
    FILTER (WHERE COALESCE(op.procedure_normalized, op.procedure_raw) IS NOT NULL) AS evidence_values
FROM _mig253_null_pts n
LEFT JOIN main.canonical_operative_events_v1 op
  ON CAST(op.research_id AS VARCHAR) = n.research_id
GROUP BY n.research_id;

CREATE TEMP TABLE _mig253_cpt_source AS
SELECT
  research_id,
  CASE
    WHEN TRY_CAST(nsqip_cpt_code AS INTEGER) IN (60240,60252,60254,60260,60270,60271)
      OR regexp_matches(LOWER(COALESCE(nsqip_cpt_description, '')),
        'total or complete|total or subtotal|removal of all remaining|substernal thyroid')
      THEN TRUE ELSE FALSE END AS has_total,
  CASE
    WHEN TRY_CAST(nsqip_cpt_code AS INTEGER) IN (60210,60212,60220,60225)
      OR regexp_matches(LOWER(COALESCE(nsqip_cpt_description, '')),
        'lobectomy|hemithyroidectomy|partial thyroid')
      THEN TRUE ELSE FALSE END AS has_hemi,
  FALSE AS has_isthmus,
  CASE WHEN nsqip_cpt_code IS NOT NULL OR nsqip_cpt_description IS NOT NULL THEN 1 ELSE 0 END AS n_evidence_rows,
  FALSE AS has_other,
  CONCAT(COALESCE(CAST(nsqip_cpt_code AS VARCHAR), ''), ' ', COALESCE(nsqip_cpt_description, '')) AS evidence_values
FROM _mig253_null_pts;

CREATE TEMP TABLE _mig253_proc_code_source AS
WITH normalized AS (
  SELECT
    n.research_id,
    pc.linked_surgery_episode_id,
    LOWER(COALESCE(pc.procedure_normalized, pc.procedure_raw, '')) AS proc_text,
    COALESCE(pc.procedure_normalized, pc.procedure_raw) AS proc_value
  FROM _mig253_null_pts n
  JOIN main.canonical_operative_procedure_codes_v1 pc
    ON CAST(pc.research_id AS VARCHAR) = n.research_id
  WHERE COALESCE(pc.procedure_normalized, pc.procedure_raw) IS NOT NULL
), patient_counts AS (
  SELECT
    research_id,
    COUNT(*) AS n_evidence_rows,
    STRING_AGG(DISTINCT proc_value, ' | ') AS evidence_values,
    COALESCE(BOOL_OR(
      regexp_matches(proc_text,
        'total[^a-z0-9]*thyroidectomy|completion[^a-z0-9]*thyroidectomy|removal of all remaining|substernal thyroid')
    ), FALSE) AS has_total_text,
    COALESCE(BOOL_OR(regexp_matches(proc_text, 'hemi[^a-z0-9]*thyroidectomy|lobectomy|partial[^a-z0-9]*thyroid')), FALSE) AS has_hemi_text,
    COALESCE(BOOL_OR(regexp_matches(proc_text, 'isthmusectomy')), FALSE) AS has_isthmus_text,
    COUNT(DISTINCT linked_surgery_episode_id) FILTER (
      WHERE linked_surgery_episode_id IS NOT NULL
        AND regexp_matches(proc_text, 'hemi[^a-z0-9]*thyroidectomy|lobectomy|partial[^a-z0-9]*thyroid')
    ) AS n_distinct_hemi_episodes,
    COUNT(*) FILTER (
      WHERE regexp_matches(proc_text, 'hemi[^a-z0-9]*thyroidectomy|lobectomy|partial[^a-z0-9]*thyroid')
    ) AS n_hemi_mentions
  FROM normalized
  GROUP BY research_id
)
SELECT
  n.research_id,
  COALESCE(pc.has_total_text, FALSE)
    OR COALESCE(pc.n_distinct_hemi_episodes, 0) >= 2 AS has_total,
  COALESCE(pc.has_hemi_text, FALSE) AS has_hemi,
  COALESCE(pc.has_isthmus_text, FALSE) AS has_isthmus,
  COALESCE(pc.n_evidence_rows, 0) > 0 AS has_other,
  COALESCE(pc.n_evidence_rows, 0) AS n_evidence_rows,
  pc.evidence_values,
  COALESCE(pc.n_distinct_hemi_episodes, 0) AS n_distinct_hemi_episodes,
  COALESCE(pc.n_hemi_mentions, 0) AS n_hemi_mentions
FROM _mig253_null_pts n
LEFT JOIN patient_counts pc USING (research_id);

CREATE TEMP TABLE _mig253_path_source AS
WITH path_values AS (
  SELECT
    n.research_id,
    LOWER(COALESCE(ps.thyroid_procedure, '')) AS proc_text,
    LOWER(COALESCE(ps.procedure_other_description, '')) AS other_text,
    CONCAT(COALESCE(ps.thyroid_procedure, ''),
           CASE WHEN ps.procedure_other_description IS NOT NULL THEN CONCAT(' / ', ps.procedure_other_description) ELSE '' END) AS proc_value
  FROM _mig253_null_pts n
  JOIN main.path_synoptics ps
    ON CAST(ps.research_id AS VARCHAR) = n.research_id
  WHERE ps.thyroid_procedure IS NOT NULL
     OR ps.procedure_other_description IS NOT NULL
)
SELECT
  n.research_id,
  COALESCE(BOOL_OR(regexp_matches(proc_text, 'total[^a-z0-9]*thyroidectomy')), FALSE) AS has_total,
  COALESCE(BOOL_OR(regexp_matches(proc_text, 'hemi[^a-z0-9]*thyroidectomy|lobectomy')), FALSE) AS has_hemi,
  COALESCE(BOOL_OR(regexp_matches(proc_text, 'isthmusectomy')), FALSE) AS has_isthmus,
  COUNT(p.proc_value) > 0 AS has_other,
  COUNT(p.proc_value) AS n_evidence_rows,
  STRING_AGG(DISTINCT p.proc_value, ' | ') FILTER (WHERE p.proc_value IS NOT NULL AND p.proc_value <> '') AS evidence_values
FROM _mig253_null_pts n
LEFT JOIN path_values p USING (research_id)
GROUP BY n.research_id;

CREATE TEMP TABLE _mig253_op_detail_source AS
WITH op_detail_text AS (
  SELECT
    n.research_id,
    LOWER(COALESCE(d.entity_value_norm, d.entity_value_raw, '')) AS detail_text,
    COALESCE(d.entity_value_norm, d.entity_value_raw) AS detail_value
  FROM _mig253_null_pts n
  JOIN main.note_entities_operative_detail d
    ON CAST(d.research_id AS VARCHAR) = n.research_id
  WHERE COALESCE(d.present_or_negated, 'present') = 'present'
    AND COALESCE(d.entity_value_norm, d.entity_value_raw) IS NOT NULL
)
SELECT
  n.research_id,
  COALESCE(BOOL_OR(regexp_matches(detail_text,
    'total[^a-z0-9]*thyroidectomy|completion[^a-z0-9]*thyroidectomy|removal of all remaining|substernal thyroid')), FALSE) AS has_total,
  COALESCE(BOOL_OR(regexp_matches(detail_text, 'hemi[^a-z0-9]*thyroidectomy|lobectomy|partial[^a-z0-9]*thyroid')), FALSE) AS has_hemi,
  COALESCE(BOOL_OR(regexp_matches(detail_text, 'isthmusectomy')), FALSE) AS has_isthmus,
  FALSE AS has_other,
  COUNT(detail_value) AS n_evidence_rows,
  STRING_AGG(DISTINCT detail_value, ' | ') FILTER (WHERE detail_value IS NOT NULL) AS evidence_values
FROM _mig253_null_pts n
LEFT JOIN op_detail_text d USING (research_id)
GROUP BY n.research_id;

CREATE TEMP TABLE _mig253_resolution AS
WITH chosen AS (
  SELECT
    n.research_id,
    n.first_surgery_date,
    n.n_surgeries,
    n.gland_weight_final_g,
    n.histology_final,
    n.nsqip_cpt_code,
    n.nsqip_cpt_description,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN 'canonical_operative_events_v1'
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN 'nsqip_cpt'
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN 'canonical_operative_procedure_codes_v1'
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN 'path_synoptics'
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN 'note_entities_operative_detail'
      ELSE 'unresolved'
    END AS resolution_source,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.has_total
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.has_total
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.has_total
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.has_total
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.has_total
      ELSE NULL
    END AS src_has_total,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.has_hemi
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.has_hemi
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.has_hemi
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.has_hemi
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.has_hemi
      ELSE NULL
    END AS src_has_hemi,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.has_isthmus
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.has_isthmus
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.has_isthmus
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.has_isthmus
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.has_isthmus
      ELSE NULL
    END AS src_has_isthmus,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.has_other
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.has_other
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.has_other
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.has_other
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.has_other
      ELSE NULL
    END AS src_has_other,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.n_evidence_rows
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.n_evidence_rows
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.n_evidence_rows
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.n_evidence_rows
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.n_evidence_rows
      ELSE 0
    END AS chosen_evidence_rows,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.evidence_values
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.evidence_values
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.evidence_values
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.evidence_values
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.evidence_values
      ELSE NULL
    END AS chosen_evidence_values,
    pc.n_distinct_hemi_episodes,
    pc.n_hemi_mentions
  FROM _mig253_null_pts n
  LEFT JOIN _mig253_event_source ev USING (research_id)
  LEFT JOIN _mig253_cpt_source cpt USING (research_id)
  LEFT JOIN _mig253_proc_code_source pc USING (research_id)
  LEFT JOIN _mig253_path_source ps USING (research_id)
  LEFT JOIN _mig253_op_detail_source od USING (research_id)
)
SELECT
  *,
  CASE
    WHEN resolution_source = 'unresolved' THEN NULL
    WHEN src_has_total THEN 'total_thyroidectomy'
    WHEN src_has_hemi THEN 'hemithyroidectomy'
    WHEN src_has_isthmus THEN 'isthmusectomy'
    WHEN src_has_other THEN 'other'
    ELSE NULL
  END AS proposed_surg_procedure_type,
  CASE
    WHEN resolution_source = 'unresolved' THEN NULL
    WHEN src_has_total THEN TRUE
    ELSE FALSE
  END AS proposed_surg_total_thyroidectomy,
  CASE
    WHEN resolution_source = 'unresolved' THEN NULL
    WHEN src_has_total THEN FALSE
    WHEN src_has_hemi THEN TRUE
    ELSE FALSE
  END AS proposed_surg_hemithyroidectomy
FROM chosen;
"""


SUMMARY_QUERIES = {
    "baseline_gap": """
        SELECT COUNT(*) AS n_total,
               SUM(CASE WHEN surg_procedure_type IS NULL THEN 1 ELSE 0 END) AS null_proc_type,
               SUM(CASE WHEN surg_procedure_type IS NULL
                         AND surg_total_thyroidectomy IS NULL
                         AND surg_hemithyroidectomy IS NULL THEN 1 ELSE 0 END) AS null_all_three
        FROM main.canonical_patient_master
    """,
    "resolution_by_source": """
        SELECT resolution_source,
               proposed_surg_procedure_type,
               proposed_surg_total_thyroidectomy,
               proposed_surg_hemithyroidectomy,
               COUNT(*) AS n_patients
        FROM _mig253_resolution
        GROUP BY 1,2,3,4
        ORDER BY n_patients DESC
    """,
    "proposed_distribution": """
        SELECT proposed_surg_procedure_type, COUNT(*) AS n_pts_resolved
        FROM _mig253_resolution
        GROUP BY 1
        ORDER BY n_pts_resolved DESC
    """,
    "post_gap_simulation": """
        SELECT
          COUNT(*) AS cpm_rows,
          SUM(CASE WHEN COALESCE(r.proposed_surg_procedure_type, pm.surg_procedure_type) IS NULL THEN 1 ELSE 0 END) AS simulated_null_proc_type,
          SUM(CASE WHEN COALESCE(r.proposed_surg_procedure_type, pm.surg_procedure_type) IS NULL
                    AND COALESCE(r.proposed_surg_total_thyroidectomy, pm.surg_total_thyroidectomy) IS NULL
                    AND COALESCE(r.proposed_surg_hemithyroidectomy, pm.surg_hemithyroidectomy) IS NULL THEN 1 ELSE 0 END) AS simulated_null_all_three
        FROM main.canonical_patient_master pm
        LEFT JOIN _mig253_resolution r
          ON CAST(pm.research_id AS VARCHAR) = r.research_id
    """,
    "m038_simulation": """
        SELECT
          COALESCE(r.proposed_surg_procedure_type, c.surg_procedure_type) AS surg_procedure_type,
          COALESCE(r.proposed_surg_total_thyroidectomy, c.surg_total_thyroidectomy) AS surg_total_thyroidectomy,
          COALESCE(r.proposed_surg_hemithyroidectomy, c.surg_hemithyroidectomy) AS surg_hemithyroidectomy,
          COUNT(*) AS n
        FROM manuscript_workspace.cohort_m038_massive_goiter_v1 c
        LEFT JOIN _mig253_resolution r
          ON CAST(c.research_id AS VARCHAR) = r.research_id
        WHERE c.gland_weight_final_g >= 200
        GROUP BY 1,2,3
        ORDER BY n DESC
    """,
    "consistency_checks": """
        SELECT
          SUM(CASE WHEN proposed_surg_procedure_type='total_thyroidectomy' AND proposed_surg_total_thyroidectomy IS NOT TRUE THEN 1 ELSE 0 END) AS total_type_but_total_flag_not_true,
          SUM(CASE WHEN proposed_surg_procedure_type='hemithyroidectomy' AND proposed_surg_hemithyroidectomy IS NOT TRUE THEN 1 ELSE 0 END) AS hemi_type_but_hemi_flag_not_true,
          SUM(CASE WHEN proposed_surg_total_thyroidectomy IS TRUE AND proposed_surg_hemithyroidectomy IS TRUE THEN 1 ELSE 0 END) AS both_total_and_hemi_true,
          SUM(CASE WHEN proposed_surg_procedure_type IS NOT NULL AND proposed_surg_total_thyroidectomy IS NULL THEN 1 ELSE 0 END) AS resolved_type_null_total_flag,
          SUM(CASE WHEN proposed_surg_procedure_type IS NOT NULL AND proposed_surg_hemithyroidectomy IS NULL THEN 1 ELSE 0 END) AS resolved_type_null_hemi_flag
        FROM _mig253_resolution
    """,
    "source_coverage": """
        SELECT
          COUNT(*) AS null_patients,
          SUM(CASE WHEN first_surgery_date IS NOT NULL THEN 1 ELSE 0 END) AS has_first_surgery_date,
          SUM(CASE WHEN n_surgeries IS NOT NULL THEN 1 ELSE 0 END) AS has_n_surgeries,
          SUM(CASE WHEN gland_weight_final_g IS NOT NULL THEN 1 ELSE 0 END) AS has_gland_weight,
          SUM(CASE WHEN histology_final IS NOT NULL THEN 1 ELSE 0 END) AS has_histology,
          SUM(CASE WHEN nsqip_cpt_code IS NOT NULL THEN 1 ELSE 0 END) AS has_nsqip_cpt
        FROM _mig253_resolution
    """,
    "qc_pre": """
        SELECT gate1_verified_tables, gate2_missing_signoff, gate3_count_mismatch,
               gate4_verified_cols_missing_metadata, gate5_clinical_date_violations,
               cohort_parity_ok
        FROM semantic_publication.vw_publication_qc_status_VIEW_v1
    """,
}


def fetch_dicts(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, object]]:
    cols = [d[0] for d in con.execute(sql).description]
    return [dict(zip(cols, row)) for row in con.fetchall()]


def write_csv(con: duckdb.DuckDBPyConnection, sql: str, path: Path) -> None:
    safe = str(path).replace("'", "''")
    con.execute(f"COPY ({sql}) TO '{safe}' (HEADER, DELIMITER ',')")


def markdown_table(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "_(no rows)_\n"
    cols = list(rows[0].keys())
    out = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(row.get(col, "")) for col in cols) + " |")
    return "\n".join(out) + "\n"


def main() -> int:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    export_dir = REPO / "exports" / f"mig253_surg_proc_type_dryrun_{ts}"
    audit_dir = REPO / "manuscript_outputs" / "audit"
    output_dir = REPO / "scripts" / "output"
    qa_dir = REPO / "qa"
    for path in (export_dir, audit_dir, output_dir, qa_dir):
        path.mkdir(parents=True, exist_ok=True)

    con = connect_locked()
    con.execute(RESOLUTION_SQL)

    summaries = {name: fetch_dicts(con, sql) for name, sql in SUMMARY_QUERIES.items()}

    write_csv(con, "SELECT * FROM _mig253_resolution ORDER BY TRY_CAST(research_id AS BIGINT)", export_dir / "mig253_resolution.csv")
    write_csv(con, "SELECT * FROM _mig253_resolution WHERE proposed_surg_procedure_type IS NULL ORDER BY TRY_CAST(research_id AS BIGINT)", export_dir / "mig253_residual_review.csv")
    write_csv(con, "SELECT * FROM _mig253_resolution WHERE proposed_surg_procedure_type IS NULL ORDER BY TRY_CAST(research_id AS BIGINT)", audit_dir / "mig253_residual_surg_proc_type_review.csv")
    write_csv(con, SUMMARY_QUERIES["resolution_by_source"], export_dir / "resolution_by_source.csv")
    write_csv(con, SUMMARY_QUERIES["m038_simulation"], export_dir / "m038_ge200_simulated_distribution.csv")

    manifest = {
        "run_id": RUN_ID,
        "run_timestamp_utc": ts,
        "migration_path": MIGRATION_PATH,
        "database": "thyroid_canonical_publication_v1_0",
        "dry_run_only": True,
        "summaries": summaries,
        "artifacts": {
            "export_dir": str(export_dir.relative_to(REPO)),
            "resolution_csv": str((export_dir / "mig253_resolution.csv").relative_to(REPO)),
            "residual_review_csv": str((export_dir / "mig253_residual_review.csv").relative_to(REPO)),
            "audit_residual_review_csv": str((audit_dir / "mig253_residual_surg_proc_type_review.csv").relative_to(REPO)),
        },
    }
    (export_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    (qa_dir / f"qa_mig253_surg_proc_type_dryrun_{ts}.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n")

    report = [
        "# mig_253 surgical procedure type dry-run",
        "",
        f"Run timestamp UTC: `{ts}`",
        f"Migration path: `{MIGRATION_PATH}`",
        "",
        "No `main.*` tables were mutated. All derivations were computed in session-scoped TEMP tables.",
        "",
        "## Baseline gap",
        markdown_table(summaries["baseline_gap"]),
        "## Source coverage among all-three-NULL patients",
        markdown_table(summaries["source_coverage"]),
        "## Proposed resolution by source",
        markdown_table(summaries["resolution_by_source"]),
        "## Proposed procedure distribution",
        markdown_table(summaries["proposed_distribution"]),
        "## Simulated post-mig_253 CPM gap",
        markdown_table(summaries["post_gap_simulation"]),
        "## Simulated M038 >=200g distribution",
        markdown_table(summaries["m038_simulation"]),
        "## Consistency checks",
        markdown_table(summaries["consistency_checks"]),
        "## Pre-apply QC gate state",
        markdown_table(summaries["qc_pre"]),
        "## Residual follow-up",
        "Residual unresolved rows are exported to `manuscript_outputs/audit/mig253_residual_surg_proc_type_review.csv`.",
        "",
    ]
    report_path = output_dir / f"mig253_surg_proc_type_dryrun_{ts}.md"
    report_path.write_text("\n".join(report))

    print(f"Wrote {report_path.relative_to(REPO)}")
    print(f"Wrote {export_dir.relative_to(REPO)}")
    print(json.dumps(summaries, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())