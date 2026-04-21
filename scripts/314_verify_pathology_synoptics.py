"""
Script 314 — Build verify_pathology_synoptics_v1 + summary.

Grain: per (research_id) — joins path_synoptics (Excel, tumor_1_* columns)
with synoptic_tumor_long_v1 (LLM-parsed, tumor_index=1) side-by-side.

Usage:
    python 314_verify_pathology_synoptics.py            # dry-run
    python 314_verify_pathology_synoptics.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "314_verify_pathology_synoptics"

FIELDS = [
    "histologic_type",
    "size_greatest_dimension_cm",
    "extrathyroidal_extension",
    "margin_status",
    "lymphatic_invasion",
    "angioinvasion",
    "perineural_invasion",
    "t_stage",
    "n_stage",
    "ln_examined",
    "ln_involved",
]


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def cpm_invariants(con, label=""):
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
          FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        raise SystemExit("CPM invariant violation")


VERIFY_SQL = """
WITH excel AS (
    SELECT research_id,
           MAX(tumor_1_histologic_type) AS histologic_type_excel,
           MAX(CAST(tumor_1_size_greatest_dimension_cm AS VARCHAR)) AS size_greatest_dimension_cm_excel,
           MAX(tumor_1_extrathyroidal_extension) AS extrathyroidal_extension_excel,
           MAX(tumor_1_margin_status) AS margin_status_excel,
           MAX(tumor_1_lymphatic_invasion) AS lymphatic_invasion_excel,
           MAX(tumor_1_angioinvasion) AS angioinvasion_excel,
           MAX(tumor_1_perineural_invasion) AS perineural_invasion_excel,
           MAX(tumor_1_t_stage_ajcc8) AS t_stage_excel,
           MAX(tumor_1_n_stage_ajcc8) AS n_stage_excel,
           MAX(CAST(tumor_1_ln_examined AS VARCHAR)) AS ln_examined_excel,
           MAX(CAST(tumor_1_ln_involved AS VARCHAR)) AS ln_involved_excel,
           MAX(source_workbook) AS excel_source_workbook
    FROM main.path_synoptics
    GROUP BY research_id
),
llm AS (
    SELECT research_id,
           MAX(histologic_type) AS histologic_type_llm,
           MAX(CAST(size_greatest_dimension_cm AS VARCHAR)) AS size_greatest_dimension_cm_llm,
           MAX(extrathyroidal_extension) AS extrathyroidal_extension_llm,
           MAX(margin_status) AS margin_status_llm,
           MAX(lymphatic_invasion) AS lymphatic_invasion_llm,
           MAX(angioinvasion) AS angioinvasion_llm,
           MAX(perineural_invasion) AS perineural_invasion_llm,
           NULL AS t_stage_llm,
           NULL AS n_stage_llm,
           MAX(CAST(ln_examined AS VARCHAR)) AS ln_examined_llm,
           MAX(CAST(ln_involved AS VARCHAR)) AS ln_involved_llm
    FROM main.synoptic_tumor_long_v1
    WHERE tumor_index = 1
    GROUP BY research_id
)
SELECT
    COALESCE(CAST(e.research_id AS VARCHAR), CAST(l.research_id AS VARCHAR)) AS research_id,
    e.excel_source_workbook,
"""

# Build field columns dynamically
_conc_template = """
    e.{f}_excel, l.{f}_llm,
    CASE WHEN e.{f}_excel IS NULL AND l.{f}_llm IS NULL THEN 'both_null'
         WHEN e.{f}_excel IS NULL THEN 'llm_only'
         WHEN l.{f}_llm IS NULL THEN 'excel_only'
         WHEN LOWER(TRIM(CAST(e.{f}_excel AS VARCHAR))) = LOWER(TRIM(CAST(l.{f}_llm AS VARCHAR))) THEN 'agree'
         ELSE 'disagree' END AS {f}_concordance"""

_field_clauses = ",\n".join([_conc_template.format(f=f) for f in FIELDS])

VERIFY_SQL += _field_clauses + """

FROM excel e
FULL OUTER JOIN llm l ON CAST(l.research_id AS VARCHAR) = CAST(e.research_id AS VARCHAR)
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 314 — verify_pathology_synoptics "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    preview = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id) FROM ({VERIFY_SQL}) t
    """).fetchone()
    log(f"  Verify preview: {preview[0]} rows, {preview[1]} patients")

    if not args.commit:
        cpm_invariants(con, "post-dryrun")
        log("(dry-run — re-run with --commit to apply)")
        return

    con.execute(f"CREATE OR REPLACE TABLE main.verify_pathology_synoptics_v1 AS {VERIFY_SQL}")
    n = con.execute("SELECT COUNT(*) FROM main.verify_pathology_synoptics_v1").fetchone()[0]
    log(f"  Created verify_pathology_synoptics_v1: {n} rows")

    # Build summary
    summary_parts = []
    for f in FIELDS:
        summary_parts.append(f"""
            SELECT 'pathology_synoptics' AS domain, '{f}' AS field_name,
                   COUNT(*) AS n_total,
                   SUM(CASE WHEN {f}_concordance='agree' THEN 1 ELSE 0 END) AS n_agree,
                   SUM(CASE WHEN {f}_concordance='disagree' THEN 1 ELSE 0 END) AS n_disagree,
                   SUM(CASE WHEN {f}_concordance='excel_only' THEN 1 ELSE 0 END) AS n_excel_only,
                   SUM(CASE WHEN {f}_concordance='llm_only' THEN 1 ELSE 0 END) AS n_llm_only,
                   SUM(CASE WHEN {f}_concordance='both_null' THEN 1 ELSE 0 END) AS n_both_null,
                   SUM(CASE WHEN {f}_excel IS NOT NULL THEN 1 ELSE 0 END) AS n_excel_nonnull,
                   SUM(CASE WHEN {f}_llm IS NOT NULL THEN 1 ELSE 0 END) AS n_llm_nonnull,
                   ROUND(SUM(CASE WHEN {f}_concordance='agree' THEN 1.0 ELSE 0 END) /
                         NULLIF(SUM(CASE WHEN {f}_concordance IN ('agree','disagree') THEN 1 ELSE 0 END), 0), 4) AS pct_agree
            FROM main.verify_pathology_synoptics_v1
        """)
    summary_sql = " UNION ALL ".join(summary_parts)
    con.execute(f"CREATE OR REPLACE TABLE main.verify_pathology_synoptics_summary_v1 AS {summary_sql}")
    log("  Created verify_pathology_synoptics_summary_v1")

    rows = con.execute("SELECT field_name, n_agree, n_disagree, n_excel_only, n_llm_only, pct_agree FROM main.verify_pathology_synoptics_summary_v1").fetchall()
    for r in rows:
        log(f"    {r[0]:35s} agree={r[1]:>5} disagree={r[2]:>5} excel_only={r[3]:>5} llm_only={r[4]:>5} pct={r[5]}")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 314 complete.")


if __name__ == "__main__":
    main()
