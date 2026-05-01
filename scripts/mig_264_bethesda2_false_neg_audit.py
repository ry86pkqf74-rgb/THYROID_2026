#!/usr/bin/env python3
"""mig_264 — Bethesda-2 × malignant audit (decision pass, read-only).

Runs §2 probes from cursor_prompts/CURSOR_PROMPT_MIG_264_BETHESDA2_FALSE_NEG_AUDIT_20260501.md
against MotherDuck ``thyroid_canonical_publication_v1_0`` via ``connect_locked()``.

No UPDATEs / archives — disposition CSV + markdown for Logan review.

Usage:
  .venv/bin/python scripts/mig_264_bethesda2_false_neg_audit.py
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))


def _rid_join(alias_a: str, alias_b: str) -> str:
    return f"CAST({alias_a}.research_id AS VARCHAR) = CAST({alias_b}.research_id AS VARCHAR)"


COHORT_COUNT = """
SELECT
  COUNT(*) AS n_bethesda2_malig,
  (SELECT COUNT(*) FROM main.canonical_patient_master WHERE bethesda_final = 2) AS n_bethesda2_all,
  (SELECT COUNT(*) FROM main.canonical_patient_master
   WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)) AS n_bethesda2_malig_repeat
FROM main.canonical_patient_master cpm
WHERE cpm.bethesda_final = 2 AND COALESCE(cpm.is_malignant, FALSE)
"""

PROBE_2A = f"""
WITH bethesda2_malig AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
)
SELECT
  COUNT_IF(n_fna = 1) AS single_fna,
  COUNT_IF(n_fna = 2) AS two_fna,
  COUNT_IF(n_fna >= 3) AS three_plus_fna,
  COUNT_IF(n_fna = 0) AS zero_fna_events
FROM (
  SELECT b.research_id, COUNT(f.fna_event_id) AS n_fna
  FROM bethesda2_malig b
  LEFT JOIN main.canonical_fna_events_v1 f ON {_rid_join('b', 'f')}
  GROUP BY b.research_id
)
"""

PROBE_2B = """
SELECT COALESCE(bethesda_index_nodule_linkage_source, '(null)') AS src, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
GROUP BY 1 ORDER BY n DESC
"""

PROBE_2C = """
WITH bethesda2_malig AS (
  SELECT CAST(research_id AS VARCHAR) AS rid
  FROM main.canonical_patient_master
  WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
),
ps_agg AS (
  SELECT CAST(research_id AS VARCHAR) AS rid,
         MAX(tumor_2_size_greatest_dimension_cm) AS t2_cm
  FROM main.path_synoptics
  GROUP BY 1
)
SELECT
  COUNT_IF(ps_agg.t2_cm IS NOT NULL) AS multi_tumor_path_synoptics,
  COUNT_IF(ps_agg.t2_cm IS NULL AND ps_agg.rid IS NOT NULL) AS single_slot_missing_t2,
  COUNT_IF(ps_agg.rid IS NULL) AS no_path_synoptics_row
FROM bethesda2_malig b
LEFT JOIN ps_agg ON b.rid = ps_agg.rid
"""

PROBE_2D = """
SELECT histology_final, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
GROUP BY 1 ORDER BY n DESC LIMIT 30
"""

PROBE_2E = f"""
WITH per_patient AS (
  SELECT
    cpm.research_id,
    MIN(
      DATE_DIFF(
        'day',
        CAST(f.fna_date_resolved AS DATE),
        CAST(cpm.first_surgery_date AS DATE)
      )
    ) AS days_fna_to_surg
  FROM main.canonical_patient_master cpm
  JOIN main.canonical_fna_events_v1 f ON {_rid_join('cpm', 'f')}
  WHERE cpm.bethesda_final = 2 AND COALESCE(cpm.is_malignant, FALSE)
    AND f.fna_date_resolved IS NOT NULL
    AND cpm.first_surgery_date IS NOT NULL
  GROUP BY cpm.research_id
)
SELECT
  COUNT(*) AS n_with_both_dates,
  median(days_fna_to_surg) AS median_days,
  COUNT_IF(days_fna_to_surg < 30) AS within_30d,
  COUNT_IF(days_fna_to_surg BETWEEN 30 AND 365) AS one_to_12mo,
  COUNT_IF(days_fna_to_surg > 365) AS over_1yr,
  COUNT_IF(days_fna_to_surg < 0) AS negative_days_fna_after_surgery
FROM per_patient
"""

PROBE_HIGHER_BETHESDA = f"""
WITH bethesda2_malig AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
),
agg AS (
  SELECT research_id, MAX(bethesda_final_num) AS max_b
  FROM main.canonical_fna_events_v1
  WHERE bethesda_final_num IS NOT NULL
  GROUP BY 1
)
SELECT
  COUNT(*) AS n_cohort,
  COUNT_IF(COALESCE(a.max_b, 0) > 2) AS n_with_some_fna_gt2,
  COUNT_IF(a.max_b IS NULL) AS n_no_fna_bethesda_num
FROM bethesda2_malig b
LEFT JOIN agg a ON {_rid_join('b', 'a')}
"""

# Heuristic disposition flags per patient (non–mutually-exclusive for review CSV)
DISPOSITION_DETAIL = f"""
WITH cohort AS (
  SELECT
    research_id,
    bethesda_final,
    bethesda_final_name,
    bethesda_index_nodule,
    bethesda_index_nodule_linkage_source,
    histology_final,
    first_surgery_date,
    is_malignant
  FROM main.canonical_patient_master
  WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
),
n_fna AS (
  SELECT c.research_id, COUNT(f.fna_event_id) AS n_fna
  FROM cohort c
  LEFT JOIN main.canonical_fna_events_v1 f ON {_rid_join('c', 'f')}
  GROUP BY c.research_id
),
max_b AS (
  SELECT research_id, MAX(bethesda_final_num) AS max_bethesda_num
  FROM main.canonical_fna_events_v1
  WHERE bethesda_final_num IS NOT NULL
  GROUP BY 1
),
min_interval AS (
  SELECT
    c.research_id,
    MIN(
      DATE_DIFF(
        'day',
        CAST(f.fna_date_resolved AS DATE),
        CAST(c.first_surgery_date AS DATE)
      )
    ) AS days_fna_to_surg
  FROM cohort c
  JOIN main.canonical_fna_events_v1 f ON {_rid_join('c', 'f')}
  WHERE f.fna_date_resolved IS NOT NULL AND c.first_surgery_date IS NOT NULL
  GROUP BY c.research_id
),
path_t2 AS (
  SELECT
    CAST(research_id AS VARCHAR) AS research_id,
    MAX(tumor_2_size_greatest_dimension_cm) AS t2_cm
  FROM main.path_synoptics
  GROUP BY 1
)
SELECT
  c.research_id::VARCHAR AS research_id,
  c.bethesda_index_nodule_linkage_source,
  c.histology_final,
  nf.n_fna,
  mb.max_bethesda_num,
  mi.days_fna_to_surg,
  path_t2.t2_cm IS NOT NULL AS multi_tumor_path_synoptics,
  (mb.max_bethesda_num IS NOT NULL AND mb.max_bethesda_num > 2) AS flag_fna_chain_has_gt2,
  (nf.n_fna >= 2) AS flag_multi_fna,
  (path_t2.t2_cm IS NOT NULL) AS flag_multi_tumor,
  (mi.days_fna_to_surg IS NOT NULL AND mi.days_fna_to_surg > 365) AS flag_gt_365d,
  regexp_matches(
    LOWER(COALESCE(c.bethesda_index_nodule_linkage_source, '')),
    'unlinked|fallback|uncertain|unknown'
  ) AS flag_linkage_text_suspect,
  CASE
    WHEN mb.max_bethesda_num IS NOT NULL AND mb.max_bethesda_num > 2
      THEN 'heuristic_stale_bethesda2_vs_events'
    WHEN path_t2.t2_cm IS NOT NULL
      AND regexp_matches(
        LOWER(COALESCE(c.bethesda_index_nodule_linkage_source, '')),
        'unlinked|fallback|uncertain|unknown'
      )
      THEN 'heuristic_multi_tumor_linkage_suspect'
    WHEN mi.days_fna_to_surg IS NOT NULL AND mi.days_fna_to_surg > 365
      THEN 'heuristic_long_interval'
    ELSE 'heuristic_default_review'
  END AS suggested_disposition_bucket
FROM cohort c
LEFT JOIN n_fna nf ON CAST(c.research_id AS VARCHAR) = CAST(nf.research_id AS VARCHAR)
LEFT JOIN max_b mb ON CAST(c.research_id AS VARCHAR) = CAST(mb.research_id AS VARCHAR)
LEFT JOIN min_interval mi ON CAST(c.research_id AS VARCHAR) = CAST(mi.research_id AS VARCHAR)
LEFT JOIN path_t2 ON CAST(c.research_id AS VARCHAR) = path_t2.research_id
ORDER BY c.research_id::BIGINT
"""


def _md_table(df, title: str) -> str:
    if df is None or df.empty:
        return f"### {title}\n\n*(no rows)*\n"
    return f"### {title}\n\n{df.to_markdown(index=False)}\n"


def main() -> int:
    from _md_connect import connect_locked  # noqa: E402

    out_dir = REPO_ROOT / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    con = connect_locked()
    lines: list[str] = []
    utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines.append("# mig_264 Bethesda-2 false-negative audit (read-only)")
    lines.append(f"Generated UTC: {utc}")
    lines.append("Database: thyroid_canonical_publication_v1_0 (connect_locked)")
    lines.append("")

    def run(title: str, sql: str):
        df = con.execute(sql).fetchdf()
        lines.append(_md_table(df, title))

    run("§2.0 Cohort verification", COHORT_COUNT)
    run("§2a FNA episode counts per patient", PROBE_2A)
    run("§2b bethesda_index_nodule_linkage_source", PROBE_2B)
    run("§2c path_synoptics tumor_2 size present (multifocal proxy)", PROBE_2C)
    run("§2d histology_final (top 30)", PROBE_2D)
    run("§2e FNA-to-first-surgery interval", PROBE_2E)
    run("§2f Any FNA episode with Bethesda > 2 (patient-level)", PROBE_HIGHER_BETHESDA)

    detail_df = con.execute(DISPOSITION_DETAIL).fetchdf()
    bucket_summary = (
        detail_df.groupby("suggested_disposition_bucket", dropna=False)
        .size()
        .reset_index(name="n_patients")
        .sort_values("n_patients", ascending=False)
    )
    lines.append("### Disposition bucket summary (heuristic — Logan adjudicates)\n")
    lines.append(bucket_summary.to_markdown(index=False))
    lines.append("")
    lines.append(
        "Per-patient detail: ``scripts/output/mig_264_disposition_table.csv`` "
        f"(rows={len(detail_df)})."
    )

    md_path = out_dir / f"mig_264_bethesda2_audit_{stamp}.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    csv_path = out_dir / "mig_264_disposition_table.csv"
    detail_df.to_csv(csv_path, index=False)

    # Stable symlink-style latest copy for git surgical add (overwrite)
    latest = out_dir / "mig_264_bethesda2_audit_latest.md"
    latest.write_text(md_path.read_text(encoding="utf-8"), encoding="utf-8")

    print(f"Wrote {md_path}")
    print(f"Wrote {csv_path}")
    print(f"Wrote {latest}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
