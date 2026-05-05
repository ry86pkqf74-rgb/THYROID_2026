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

# M025 per-nodule spine (manuscript_workspace.cohort_m025_nodule_level_v1): bridges US↔FNA↔path.
# Note: COUNT(v.nodule_master_id) undercounts spine rows because some view rows have NULL nodule_master_id;
# use EXISTS / COUNT(*) on view rows for spine membership.
PROBE_M025_SPINE_COVERAGE = """
WITH cohort AS (
  SELECT CAST(research_id AS VARCHAR) AS rid
  FROM main.canonical_patient_master
  WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
),
spine AS (
  SELECT DISTINCT CAST(v.research_id AS VARCHAR) AS rid
  FROM manuscript_workspace.cohort_m025_nodule_level_v1 v
),
joined AS (
  SELECT c.rid,
         MAX(CASE WHEN v.research_id IS NOT NULL THEN 1 ELSE 0 END) AS has_any_row,
         COUNT(*) AS n_rows_all,
         COUNT(v.nodule_master_id) AS n_rows_nonempty_nid,
         SUM(CASE WHEN v.nodule_master_id IS NULL THEN 1 ELSE 0 END) AS n_rows_null_nid,
         SUM(CASE WHEN CAST(v.bethesda_final_num AS INTEGER) = 2 THEN 1 ELSE 0 END)
           AS rows_bethesda2_any,
         SUM(
           CASE WHEN CAST(v.bethesda_final_num AS INTEGER) = 2
             AND v.nodule_master_id IS NOT NULL
             THEN 1 ELSE 0 END
         ) AS rows_bethesda2_nonempty_nid,
         SUM(CASE WHEN v.nodule_path_proven_malignant THEN 1 ELSE 0 END) AS rows_path_mal
  FROM cohort c
  LEFT JOIN manuscript_workspace.cohort_m025_nodule_level_v1 v
    ON CAST(v.research_id AS VARCHAR) = c.rid
  GROUP BY c.rid
)
SELECT
  COUNT(*) AS n_patients,
  COUNT_IF(has_any_row = 1) AS pts_with_any_m025_row,
  COUNT_IF(has_any_row = 0) AS pts_no_m025_row,
  COUNT_IF(has_any_row = 1 AND rows_bethesda2_any = 0)
    AS pts_in_spine_but_no_bethesda2_row_any,
  COUNT_IF(has_any_row = 1 AND rows_bethesda2_nonempty_nid = 0)
    AS pts_in_spine_but_no_bethesda2_on_nonempty_nid,
  COUNT_IF(rows_bethesda2_nonempty_nid > 0) AS pts_with_bethesda2_on_nonempty_nid,
  COUNT_IF(rows_bethesda2_any > 0 AND rows_bethesda2_nonempty_nid = 0)
    AS pts_bethesda2_only_on_null_nid_rows,
  SUM(n_rows_null_nid) AS total_rows_null_nid_in_cohort,
  SUM(CASE WHEN has_any_row = 1 THEN n_rows_null_nid ELSE 0 END) AS null_nid_rows_among_pts_with_rows
FROM joined
"""

# Among cohort patients with usable nodule_master_id: same-side path malignancy on a *different* nid than any B2-linked nid.
PROBE_M025_SAME_SIDE_ALT_MALIGNANT = """
WITH cohort AS (
  SELECT CAST(research_id AS VARCHAR) AS rid
  FROM main.canonical_patient_master
  WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
),
nodule_agg AS (
  SELECT CAST(v.research_id AS VARCHAR) AS rid,
         v.nodule_master_id,
         MAX(v.laterality_norm) AS lat,
         BOOL_OR(CAST(v.bethesda_final_num AS INTEGER) = 2) AS has_b2_linked,
         BOOL_OR(v.nodule_path_proven_malignant) AS is_path_mal
  FROM manuscript_workspace.cohort_m025_nodule_level_v1 v
  INNER JOIN cohort c ON CAST(v.research_id AS VARCHAR) = c.rid
  WHERE v.nodule_master_id IS NOT NULL
  GROUP BY 1, 2
),
b2 AS (SELECT DISTINCT rid, nodule_master_id AS nid, lat FROM nodule_agg WHERE has_b2_linked),
mal AS (SELECT DISTINCT rid, nodule_master_id AS nid, lat FROM nodule_agg WHERE is_path_mal),
flags AS (
  SELECT DISTINCT b.rid
  FROM b2 b
  INNER JOIN mal m ON b.rid = m.rid AND b.nid <> m.nid
    AND b.lat IS NOT NULL AND m.lat IS NOT NULL AND b.lat = m.lat
)
SELECT
  COUNT(*) AS n_cohort,
  (SELECT COUNT(*) FROM flags) AS pts_same_side_malignant_diff_nid_than_b2,
  (SELECT COUNT(DISTINCT rid) FROM b2) AS pts_with_any_b2_nid,
  (SELECT COUNT(DISTINCT rid) FROM mal) AS pts_with_any_path_mal_nid
FROM cohort
"""

M025_BRIDGE_PATTERN = """
WITH cohort AS (
  SELECT CAST(research_id AS VARCHAR) AS rid
  FROM main.canonical_patient_master
  WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
),
spine AS (
  SELECT DISTINCT CAST(v.research_id AS VARCHAR) AS rid
  FROM manuscript_workspace.cohort_m025_nodule_level_v1 v
),
nodule_agg AS (
  SELECT CAST(v.research_id AS VARCHAR) AS rid,
         v.nodule_master_id,
         MAX(v.laterality_norm) AS lat,
         BOOL_OR(CAST(v.bethesda_final_num AS INTEGER) = 2) AS has_b2_linked,
         BOOL_OR(v.nodule_path_proven_malignant) AS is_path_mal
  FROM manuscript_workspace.cohort_m025_nodule_level_v1 v
  INNER JOIN cohort c ON CAST(v.research_id AS VARCHAR) = c.rid
  WHERE v.nodule_master_id IS NOT NULL
  GROUP BY 1, 2
),
b2 AS (SELECT DISTINCT rid, nodule_master_id AS nid, lat FROM nodule_agg WHERE has_b2_linked),
mal AS (SELECT DISTINCT rid, nodule_master_id AS nid, lat FROM nodule_agg WHERE is_path_mal),
pat_sets AS (
  SELECT c.rid,
         spine.rid IS NOT NULL AS in_m025_spine,
         EXISTS (SELECT 1 FROM b2 WHERE b2.rid = c.rid) AS has_b2_nid,
         EXISTS (SELECT 1 FROM mal WHERE mal.rid = c.rid) AS has_mal_nid,
         EXISTS (
           SELECT 1 FROM b2 b
           INNER JOIN mal m ON b.rid = m.rid AND b.nid <> m.nid
           WHERE b.rid = c.rid
             AND b.lat IS NOT NULL AND m.lat IS NOT NULL AND b.lat = m.lat
         ) AS same_side_diff_nodule,
         EXISTS (
           SELECT 1 FROM b2 b
           INNER JOIN mal m ON b.rid = m.rid AND b.nid = m.nid
           WHERE b.rid = c.rid
         ) AS overlap_b2_mal_same_nid
  FROM cohort c
  LEFT JOIN spine ON spine.rid = c.rid
)
SELECT rid AS research_id,
  CASE
    WHEN NOT in_m025_spine THEN 'A_not_in_m025_us_spine'
    WHEN NOT has_b2_nid THEN 'B_in_spine_no_pernodule_bethesda2_bridge'
    WHEN same_side_diff_nodule THEN 'C_same_side_malignant_not_on_b2_linked_nodule'
    WHEN overlap_b2_mal_same_nid AND NOT same_side_diff_nodule
      THEN 'D_true_FN_candidate_same_nid_b2_and_mal'
    WHEN has_mal_nid AND NOT overlap_b2_mal_same_nid THEN 'E_malignant_disjoint_b2_nodules'
    WHEN NOT has_mal_nid THEN 'F_spine_plus_b2_but_no_nodule_path_mal_flag'
    ELSE 'G_other_review'
  END AS m025_bridge_pattern
FROM pat_sets
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
    run(
        "§2g M025 nodule spine coverage (cohort_m025_nodule_level_v1)",
        PROBE_M025_SPINE_COVERAGE,
    )
    run(
        "§2h M025 same-side malignant different nid than B2-linked nid",
        PROBE_M025_SAME_SIDE_ALT_MALIGNANT,
    )

    bridge_df = con.execute(
        "SELECT m025_bridge_pattern, COUNT(*) AS n_patients FROM ("
        + M025_BRIDGE_PATTERN
        + ") t GROUP BY 1 ORDER BY n_patients DESC"
    ).fetchdf()
    lines.append("### §2i M025 bridge pattern (mutually exclusive buckets)\n")
    lines.append(
        bridge_df.to_markdown(index=False)
        + "\n\n"
        "**Interpretation (decision pass — not mutually exclusive with §2a–f):**\n\n"
        "| Pattern | Suggested mapping to §0 causes (a)(b)(c) |\n"
        "|---|---|\n"
        "| A — not in M025 US spine | Nodule-level adjudication unavailable; "
        "rely on §2a–f / patient-level linkage queues. |\n"
        "| B — spine without per-nodule Bethesda 2 bridge | Strong **(c)** "
        "coverage/linkage gap OR institutional bridge limits (CPM B2 not "
        "projected onto `bethesda_final_num=2` rows). |\n"
        "| C — same-side malignant, different nid than B2-linked nid | "
        "**(b)** non-index / dual-nodule AND/OR **(c)** Bethesda mis-map "
        "(answers headline per-nodule TR question). |\n"
        "| D — same nid B2-linked and path malignant | **(a)** "
        "true false-negative cytology candidate (same targeted nodule). |\n"
        "| E — malignant nodules disjoint from B2-linked nids | "
        "**(b)/(c)** cross-side / mis-attribution (laterality mismatch vs "
        "strict same-side rule). |\n"
        "| F — B2 bridged but no `nodule_path_proven_malignant` | Path bridge "
        "window/laterality mismatch — manual review; not a clean (a)/(b)/(c). |\n"
    )

    detail_df = con.execute(DISPOSITION_DETAIL).fetchdf()
    m025_pat = con.execute(M025_BRIDGE_PATTERN).fetchdf()
    detail_df["research_id"] = detail_df["research_id"].astype(str)
    m025_pat["research_id"] = m025_pat["research_id"].astype(str)
    detail_df = detail_df.merge(m025_pat, on="research_id", how="left")

    ct = (
        detail_df.groupby(["m025_bridge_pattern", "suggested_disposition_bucket"])
        .size()
        .reset_index(name="n_patients")
        .sort_values(["m025_bridge_pattern", "n_patients"], ascending=[True, False])
    )
    lines.append("### Cross-tab: M025 bridge pattern × heuristic disposition\n")
    lines.append(ct.to_markdown(index=False))
    lines.append("")
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
