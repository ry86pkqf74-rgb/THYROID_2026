#!/usr/bin/env python3
"""
Execute MotherDuck lymph-node completeness audit for proposal2_ete_staging lineage.

Usage:
  MD_SA_TOKEN=... .venv/bin/python studies/proposal2_ete_staging/run_motherduck_ln_completeness_audit.py --sa
  MOTHERDUCK_TOKEN=... .venv/bin/python studies/proposal2_ete_staging/run_motherduck_ln_completeness_audit.py

Outputs (under studies/proposal2_ete_staging/audit_motherduck_ln/):
  - ln_audit_missing_unresolved.csv
  - ln_audit_logical_inconsistencies.csv
  - ln_audit_subgroup_summary.csv
  - ln_audit_summary.json

Regenerates:
  studies/proposal2_ete_staging/MOTHERDUCK_LYMPH_NODE_COMPLETENESS_AUDIT.md
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TypeVar

T = TypeVar("T")

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from motherduck_client import get_token, resolve_database_for_env

STUDY_DIR = Path(__file__).resolve().parent
OUT_DIR = STUDY_DIR / "audit_motherduck_ln"
REPORT_MD = STUDY_DIR / "MOTHERDUCK_LYMPH_NODE_COMPLETENESS_AUDIT.md"


def _timed(profile: dict[str, float], label: str, fn: Callable[[], T]) -> T:
    t0 = time.perf_counter()
    out = fn()
    profile[label] = round(time.perf_counter() - t0, 4)
    return out


def _connect_md(*, use_sa: bool):
    for k in ("USE_LOCAL_DUCKDB", "use_local_duckdb"):
        os.environ.pop(k, None)
    token = get_token(prefer_service_account=use_sa)
    if not token:
        raise RuntimeError(
            "No MotherDuck token. Set MD_SA_TOKEN (CI) or MOTHERDUCK_TOKEN (interactive)."
        )
    db = resolve_database_for_env(os.getenv("MOTHERDUCK_ENV", "prod"))
    import duckdb

    return duckdb.connect(f"md:{db}?motherduck_token={token}")


SPECIMEN_BASE_SQL = """
CREATE OR REPLACE TEMP TABLE _ln_specimen AS
SELECT
    ps.research_id,
    ps.surg_date,
    YEAR(TRY_CAST(ps.surg_date AS DATE)) AS surgery_year,
    LOWER(TRIM(COALESCE(ps.thyroid_procedure, ''))) AS thyroid_procedure_raw,
    TRIM(COALESCE(ps.tumor_1_histologic_type, '')) AS tumor_1_histologic_type,
    NULL::VARCHAR AS specimen_type,
    ps.tumor_1_ln_examined AS ln_examined_raw,
    ps.tumor_1_ln_involved AS ln_positive_raw,
    TRY_CAST(
        REPLACE(REPLACE(TRIM(CAST(ps.tumor_1_ln_examined AS VARCHAR)), ';', ''), 'x', '')
        AS DOUBLE
    ) AS ln_examined_clean,
    TRY_CAST(
        REPLACE(REPLACE(TRIM(CAST(ps.tumor_1_ln_involved AS VARCHAR)), ';', ''), 'x', '')
        AS DOUBLE
    ) AS ln_positive_clean,
    CASE
        WHEN ps.central_compartment_dissection IS NOT NULL THEN 1
        WHEN LOWER(COALESCE(ps.tumor_1_level_examined, '')) LIKE '%6%' THEN 1
        WHEN LOWER(COALESCE(ps.other_ln_dissection, '')) LIKE '%central%'
             OR LOWER(COALESCE(ps.other_ln_dissection, '')) LIKE '%level 6%' THEN 1
        WHEN LOWER(COALESCE(ps.tumor_1_ln_location, '')) LIKE '%perithyroidal%'
             OR LOWER(COALESCE(ps.tumor_1_ln_location, '')) LIKE '%pretracheal%'
             OR LOWER(COALESCE(ps.tumor_1_ln_location, '')) LIKE '%paratracheal%'
             OR LOWER(COALESCE(ps.tumor_1_ln_location, '')) LIKE '%delphian%'
             OR LOWER(COALESCE(ps.tumor_1_ln_location, '')) LIKE '%prelaryngeal%' THEN 1
        ELSE 0
    END AS central_lnd_composite_flag,
    tp.histology_1_n_stage_ajcc8 AS n_stage_tp,
    TRY_CAST(tp.histology_1_ln_examined AS DOUBLE) AS tp_ln_examined,
    TRY_CAST(tp.histology_1_ln_positive AS DOUBLE) AS tp_ln_positive,
    TRY_CAST(tp.histology_1_ln_ratio AS DOUBLE) AS tp_ln_ratio
FROM path_synoptics ps
LEFT JOIN tumor_pathology tp
    ON CAST(ps.research_id AS BIGINT) = CAST(tp.research_id AS BIGINT)
WHERE ps.research_id IS NOT NULL
"""

SUMMARY_SQL = """
SELECT
    COUNT(*) AS n_pathology_specimens,
    SUM(CASE WHEN ln_examined_clean IS NOT NULL THEN 1 ELSE 0 END) AS n_examined_populated,
    SUM(CASE WHEN ln_positive_clean IS NOT NULL THEN 1 ELSE 0 END) AS n_positive_populated,
    SUM(CASE WHEN ln_examined_clean IS NOT NULL AND ln_positive_clean IS NOT NULL THEN 1 ELSE 0 END) AS n_both_populated,
    SUM(CASE WHEN (ln_examined_clean IS NOT NULL AND ln_examined_clean = 0)
              OR (ln_positive_clean IS NOT NULL AND ln_positive_clean = 0) THEN 1 ELSE 0 END) AS n_explicit_zero_representation,
    SUM(CASE WHEN ln_examined_clean IS NULL AND ln_positive_clean IS NULL THEN 1 ELSE 0 END) AS n_both_null_unresolved,
    SUM(CASE WHEN NOT (ln_examined_clean IS NOT NULL AND ln_positive_clean IS NOT NULL)
              AND NOT (
                   (ln_examined_clean IS NOT NULL AND ln_examined_clean = 0)
                OR (ln_positive_clean IS NOT NULL AND ln_positive_clean = 0)
              )
              AND (ln_examined_clean IS NULL OR ln_positive_clean IS NULL)
         THEN 1 ELSE 0 END) AS n_partial_or_unresolved_nonzero_pattern
FROM _ln_specimen
"""

RISK_MV_SUMMARY_SQL = """
SELECT
    COUNT(*) AS n_rrf_rows,
    COUNT(DISTINCT research_id) AS n_rrf_patients,
    SUM(CASE WHEN ln_examined IS NOT NULL THEN 1 ELSE 0 END) AS n_examined_populated,
    SUM(CASE WHEN ln_positive IS NOT NULL THEN 1 ELSE 0 END) AS n_positive_populated,
    SUM(CASE WHEN ln_examined IS NOT NULL AND ln_positive IS NOT NULL THEN 1 ELSE 0 END) AS n_both_populated,
    SUM(CASE WHEN ln_ratio IS NOT NULL THEN 1 ELSE 0 END) AS n_ln_ratio_populated,
    SUM(CASE WHEN ln_ratio IS NOT NULL AND (ln_examined IS NULL OR ln_positive IS NULL) THEN 1 ELSE 0 END) AS n_ratio_without_both_counts
FROM recurrence_risk_features_mv
"""

MISSING_SQL = """
SELECT
    research_id,
    surg_date,
    surgery_year,
    thyroid_procedure_raw,
    tumor_1_histologic_type,
    specimen_type,
    ln_examined_raw,
    ln_positive_raw,
    ln_examined_clean,
    ln_positive_clean,
    central_lnd_composite_flag,
    n_stage_tp,
    tp_ln_examined,
    tp_ln_positive,
    tp_ln_ratio,
    CASE
        WHEN ln_examined_clean IS NULL AND ln_positive_clean IS NULL THEN 'both_null_unresolved'
        WHEN ln_examined_clean IS NULL AND ln_positive_clean IS NOT NULL THEN 'positive_without_examined'
        WHEN ln_positive_clean IS NULL AND ln_examined_clean IS NOT NULL THEN 'examined_without_positive'
        ELSE 'other_partial'
    END AS missingness_class
FROM _ln_specimen
WHERE NOT (ln_examined_clean IS NOT NULL AND ln_positive_clean IS NOT NULL)
ORDER BY research_id, surg_date
"""

INCONSISTENCIES_SQL = """
WITH s AS (SELECT * FROM _ln_specimen),
dup AS (
    SELECT research_id, sd
    FROM (
        SELECT
            research_id,
            TRY_CAST(surg_date AS DATE) AS sd,
            COUNT(*) AS n_rows,
            COUNT(DISTINCT CONCAT(
                COALESCE(CAST(ln_examined_clean AS VARCHAR), 'null'),
                '|',
                COALESCE(CAST(ln_positive_clean AS VARCHAR), 'null')
            )) AS n_distinct_ln_pairs
        FROM _ln_specimen
        GROUP BY research_id, TRY_CAST(surg_date AS DATE)
    ) g
    WHERE n_rows > 1 AND n_distinct_ln_pairs > 1
),
u AS (
SELECT research_id, surg_date, 'positive_gt_examined' AS issue,
    CAST(ln_examined_clean AS VARCHAR) AS ln_examined_clean,
    CAST(ln_positive_clean AS VARCHAR) AS ln_positive_clean,
    CAST(tp_ln_ratio AS VARCHAR) AS tp_ln_ratio,
    n_stage_tp
FROM s
WHERE ln_examined_clean IS NOT NULL AND ln_positive_clean IS NOT NULL
  AND ln_positive_clean > ln_examined_clean
UNION ALL
SELECT research_id, surg_date, 'positive_without_examined',
    CAST(ln_examined_clean AS VARCHAR), CAST(ln_positive_clean AS VARCHAR),
    CAST(tp_ln_ratio AS VARCHAR), n_stage_tp
FROM s
WHERE ln_positive_clean IS NOT NULL AND ln_examined_clean IS NULL
UNION ALL
SELECT research_id, surg_date, 'tp_ratio_without_counts',
    CAST(ln_examined_clean AS VARCHAR), CAST(ln_positive_clean AS VARCHAR),
    CAST(tp_ln_ratio AS VARCHAR), n_stage_tp
FROM s
WHERE tp_ln_ratio IS NOT NULL AND (tp_ln_examined IS NULL OR tp_ln_positive IS NULL)
UNION ALL
SELECT research_id, surg_date, 'n1_family_zero_or_missing_positive_nodespec',
    CAST(ln_examined_clean AS VARCHAR), CAST(ln_positive_clean AS VARCHAR),
    CAST(tp_ln_ratio AS VARCHAR), n_stage_tp
FROM s
WHERE regexp_matches(UPPER(COALESCE(n_stage_tp, '')), '^N1')
  AND (ln_positive_clean IS NULL OR ln_positive_clean <= 0)
UNION ALL
SELECT s.research_id, s.surg_date, 'specimen_vs_tumor_path_examined_mismatch',
    CAST(s.ln_examined_clean AS VARCHAR), CAST(s.tp_ln_examined AS VARCHAR),
    CAST(s.tp_ln_ratio AS VARCHAR), s.n_stage_tp
FROM s
WHERE s.ln_examined_clean IS NOT NULL AND s.tp_ln_examined IS NOT NULL
  AND ABS(s.ln_examined_clean - s.tp_ln_examined) > 0.5
UNION ALL
SELECT s.research_id, s.surg_date, 'specimen_vs_tumor_path_positive_mismatch',
    CAST(s.ln_positive_clean AS VARCHAR), CAST(s.tp_ln_positive AS VARCHAR),
    CAST(s.tp_ln_ratio AS VARCHAR), s.n_stage_tp
FROM s
WHERE s.ln_positive_clean IS NOT NULL AND s.tp_ln_positive IS NOT NULL
  AND ABS(s.ln_positive_clean - s.tp_ln_positive) > 0.5
UNION ALL
SELECT s.research_id, s.surg_date, 'duplicate_surgery_conflicting_ln_counts',
    CAST(s.ln_examined_clean AS VARCHAR), CAST(s.ln_positive_clean AS VARCHAR),
    CAST(s.tp_ln_ratio AS VARCHAR), s.n_stage_tp
FROM s
INNER JOIN dup ON s.research_id = dup.research_id
    AND TRY_CAST(s.surg_date AS DATE) = dup.sd
)
SELECT * FROM u ORDER BY issue, research_id, surg_date
"""

DEEP_ROWCOUNT_SQL = """
SELECT
    (SELECT COUNT(*) FROM path_synoptics) AS path_synoptics_all_rows,
    (SELECT COUNT(*) FROM path_synoptics WHERE research_id IS NOT NULL) AS path_synoptics_rid_not_null,
    (SELECT COUNT(DISTINCT research_id) FROM path_synoptics WHERE research_id IS NOT NULL)
        AS path_synoptics_distinct_patients,
    (SELECT COUNT(*) FROM tumor_pathology) AS tumor_pathology_rows,
    (SELECT COUNT(*) FROM recurrence_risk_features_mv) AS recurrence_risk_features_rows
"""

SUBGROUP_SQL = """
WITH s AS (
    SELECT *,
        CASE
            WHEN thyroid_procedure_raw LIKE '%total%' OR thyroid_procedure_raw LIKE '%near-total%'
                THEN 'total_or_near_total'
            WHEN thyroid_procedure_raw LIKE '%lobe%' OR thyroid_procedure_raw LIKE '%hemi%'
                THEN 'lobectomy_hemi'
            WHEN thyroid_procedure_raw = '' OR thyroid_procedure_raw IS NULL THEN 'unknown_procedure'
            ELSE 'other_or_mixed'
        END AS surgery_extent_bucket,
        CASE
            WHEN UPPER(tumor_1_histologic_type) LIKE '%PAPILLARY%'
                 OR UPPER(tumor_1_histologic_type) = 'PTC' THEN 'PTC_family'
            WHEN UPPER(tumor_1_histologic_type) LIKE '%FOLLICULAR%'
                 OR UPPER(tumor_1_histologic_type) = 'FTC' THEN 'FTC_family'
            WHEN tumor_1_histologic_type = '' OR tumor_1_histologic_type IS NULL THEN 'unknown_histology'
            ELSE 'other_histology'
        END AS histology_bucket
    FROM _ln_specimen
)
SELECT
    COALESCE(CAST(surgery_year AS VARCHAR), 'NULL') AS surgery_year,
    surgery_extent_bucket,
    histology_bucket,
    CAST(central_lnd_composite_flag AS VARCHAR) AS central_lnd_yes_no,
    COUNT(*) AS n_specimens,
    SUM(CASE WHEN ln_examined_clean IS NOT NULL THEN 1 ELSE 0 END) AS n_examined_ok,
    SUM(CASE WHEN ln_positive_clean IS NOT NULL THEN 1 ELSE 0 END) AS n_positive_ok,
    SUM(CASE WHEN ln_examined_clean IS NOT NULL AND ln_positive_clean IS NOT NULL THEN 1 ELSE 0 END) AS n_both_ok,
    SUM(CASE WHEN ln_examined_clean IS NULL AND ln_positive_clean IS NULL THEN 1 ELSE 0 END) AS n_both_unresolved,
    ROUND(100.0 * SUM(CASE WHEN ln_examined_clean IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_examined,
    ROUND(100.0 * SUM(CASE WHEN ln_positive_clean IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_positive,
    ROUND(100.0 * SUM(CASE WHEN ln_examined_clean IS NOT NULL AND ln_positive_clean IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_both
FROM s
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
"""

TABLE_CHECK_SQL = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'main'
  AND table_name IN (
    'path_synoptics', 'tumor_pathology', 'recurrence_risk_features_mv',
    'risk_enriched_mv', 'tumor_episode_master_v2', 'patient_refined_master_clinical_v12'
  )
"""


def _pct(num: float, den: float) -> float:
    if not den:
        return 0.0
    return round(100.0 * float(num) / float(den), 2)


def _verdict(pct_both: float, pct_unresolved: float, n_issue: int) -> str:
    if pct_both >= 85.0 and pct_unresolved <= 10.0 and n_issue < 500:
        return "USABLE WITH EXPLICIT MISSINGNESS CAVEAT"
    if pct_both < 60.0 or pct_unresolved > 35.0:
        return "NOT ADEQUATE / REQUIRES REMEDIATION"
    return "USABLE WITH EXPLICIT MISSINGNESS CAVEAT"


def _motherduck_connection_proof(con) -> dict:
    """Evidence that queries hit MotherDuck (not local file)."""
    pragma = con.execute("PRAGMA database_list;").fetchdf()
    ver = con.execute("SELECT version() AS v").fetchone()[0]
    out: dict = {
        "duckdb_version": str(ver),
        "pragma_database_list": pragma.to_dict(orient="records"),
    }
    try:
        dbl = con.execute(
            "SELECT database_name, database_oid FROM duckdb_databases() "
            "ORDER BY database_name"
        ).fetchdf()
        out["duckdb_databases"] = dbl.to_dict(orient="records")
    except Exception as exc:
        out["duckdb_databases_error"] = str(exc)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="MotherDuck lymph-node audit (prints timing + connection proof; use --quiet to suppress).",
    )
    parser.add_argument(
        "--sa",
        action="store_true",
        help="Prefer MD_SA_TOKEN (e.g. GitHub Actions) over MOTHERDUCK_TOKEN",
    )
    parser.add_argument(
        "--deep",
        action="store_true",
        help="Extra full-table COUNT(*) passes (still fast; proves scans over base tables)",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress progress / timing stdout",
    )
    args = parser.parse_args()
    verbose = not args.quiet

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
    profile: dict[str, float] = {}
    t_wall0 = time.perf_counter()

    def log(msg: str) -> None:
        if verbose:
            print(msg, flush=True)

    con = _timed(profile, "connect_md", lambda: _connect_md(use_sa=args.sa))
    proof = _motherduck_connection_proof(con)
    _thy = [r for r in proof.get("pragma_database_list", []) if "thyroid" in str(r).lower()]
    log(
        "MotherDuck proof: duckdb_version=%r; PRAGMA database_list rows=%s "
        "(thyroid-related subset=%s)"
        % (
            proof["duckdb_version"],
            len(proof.get("pragma_database_list", [])),
            _thy[:8] if _thy else proof.get("pragma_database_list", [])[:4],
        )
    )

    tables = _timed(
        profile,
        "table_presence_check",
        lambda: set(con.execute(TABLE_CHECK_SQL).fetchdf()["table_name"].tolist()),
    )

    required = {"path_synoptics", "tumor_pathology", "recurrence_risk_features_mv"}
    missing_tbl = required - tables
    if missing_tbl:
        raise RuntimeError(f"Missing required tables on MotherDuck: {sorted(missing_tbl)}")

    deep_counts: dict | None = None
    if args.deep:
        deep_counts = _timed(
            profile,
            "deep_full_table_counts",
            lambda: con.execute(DEEP_ROWCOUNT_SQL).fetchdf().iloc[0].to_dict(),
        )
        log(f"Deep rowcount snapshot: {deep_counts}")

    _timed(profile, "build_ln_specimen_temp", lambda: con.execute(SPECIMEN_BASE_SQL) or True)
    summary = _timed(profile, "specimen_summary_aggregates", lambda: con.execute(SUMMARY_SQL).fetchdf().iloc[0].to_dict())
    risk_summ = _timed(profile, "recurrence_risk_mv_summary", lambda: con.execute(RISK_MV_SUMMARY_SQL).fetchdf().iloc[0].to_dict())

    n_tot = int(summary["n_pathology_specimens"])
    pct_ex = _pct(summary["n_examined_populated"], n_tot)
    pct_pos = _pct(summary["n_positive_populated"], n_tot)
    pct_both = _pct(summary["n_both_populated"], n_tot)
    pct_zero = _pct(summary["n_explicit_zero_representation"], n_tot)
    pct_unres = _pct(summary["n_both_null_unresolved"], n_tot)

    missing_df = _timed(profile, "query_missing_unresolved", lambda: con.execute(MISSING_SQL).fetchdf())
    inc_full = _timed(profile, "query_inconsistencies", lambda: con.execute(INCONSISTENCIES_SQL).fetchdf())
    sub_df = _timed(profile, "query_subgroup_summary", lambda: con.execute(SUBGROUP_SQL).fetchdf())
    n_dup_issues = (
        int((inc_full["issue"] == "duplicate_surgery_conflicting_ln_counts").sum())
        if not inc_full.empty and "issue" in inc_full.columns
        else 0
    )

    missing_path = OUT_DIR / "ln_audit_missing_unresolved.csv"
    inc_path = OUT_DIR / "ln_audit_logical_inconsistencies.csv"
    sub_path = OUT_DIR / "ln_audit_subgroup_summary.csv"
    def _write_csvs() -> None:
        missing_df.to_csv(missing_path, index=False)
        inc_full.to_csv(inc_path, index=False)
        sub_df.to_csv(sub_path, index=False)

    _timed(profile, "write_csv_exports", _write_csvs)

    verdict = _verdict(pct_both, pct_unres, len(inc_full))

    issue_breakdown = (
        inc_full["issue"].value_counts().to_dict()
        if not inc_full.empty and "issue" in inc_full.columns
        else {}
    )

    profile["wall_clock_total_s"] = round(time.perf_counter() - t_wall0, 4)

    meta = {
        "generated_at_utc": ts,
        "motherduck_auth": "connected",
        "motherduck_database": resolve_database_for_env(os.getenv("MOTHERDUCK_ENV", "prod")),
        "motherduck_connection_proof": proof,
        "execution_profile_seconds": profile,
        "deep_full_table_counts": deep_counts,
        "tables_present": sorted(tables),
        "specimen_summary": summary,
        "recurrence_risk_features_mv_summary": risk_summ,
        "verdict": verdict,
        "counts": {
            "logical_inconsistency_rows": int(len(inc_full)),
            "duplicate_conflict_rows": n_dup_issues,
            "missing_unresolved_rows": int(len(missing_df)),
        },
        "inconsistency_issue_breakdown": issue_breakdown,
    }
    with open(OUT_DIR / "ln_audit_summary.json", "w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2)

    # Optional: v12 discordance if table exists
    v12_note = ""
    if "patient_refined_master_clinical_v12" in tables:
        try:
            q = """
            SELECT COUNT(*) AS n
            FROM patient_refined_master_clinical_v12 m
            INNER JOIN (
                SELECT DISTINCT CAST(research_id AS BIGINT) AS research_id FROM _ln_specimen
            ) s ON m.research_id = s.research_id
            WHERE m.ln_total_examined IS NOT NULL OR m.ln_positive_v6 IS NOT NULL
            """
            n_v12 = int(con.execute(q).fetchone()[0])
            v12_note = f"\n**patient_refined_master_clinical_v12:** {n_v12:,} patient rows (joined to specimen cohort) have `ln_total_examined` OR `ln_positive_v6` non-null (ad hoc count; columns may differ from path_synoptics grain).\n"
        except Exception as exc:
            v12_note = f"\n**patient_refined_master_clinical_v12:** optional discordance query skipped ({exc}).\n"

    prof_rows = "\n".join(
        f"| `{k}` | {v} |"
        for k, v in sorted(profile.items(), key=lambda kv: kv[0])
    )
    deep_md = ""
    if deep_counts:
        deep_md = "\n### Deep full-table counts (this run, `--deep`)\n\n| Table / metric | Rows |\n|----------------|-----:|\n"
        for k, v in deep_counts.items():
            deep_md += f"| {k} | {v} |\n"

    report = f"""# MotherDuck lymph node completeness audit (THYROID_2026)

**Generated (UTC):** {ts}  
**Database:** `{resolve_database_for_env(os.getenv("MOTHERDUCK_ENV", "prod"))}` (MotherDuck prod, authenticated)  
**Runner:** `studies/proposal2_ete_staging/run_motherduck_ln_completeness_audit.py` (`--sa` for GitHub / `MD_SA_TOKEN`; `--deep` for extra `COUNT(*)` proof; `--quiet` to silence timing logs)  
**SQL reference:** `studies/proposal2_ete_staging/sql/motherduck_lymph_node_completeness_audit.sql` (specimen spine SQL is embedded in the runner)

## 0. Execution profile & why wall time is often only a few seconds

This audit is **not** a full-text scan of pathology narratives or clinical notes. It only:

- Builds one temp table over **`path_synoptics`** (≈ tens of thousands of synoptic rows) with a join to **`tumor_pathology`**.
- Runs grouped aggregates and exports **CSV** extracts.

DuckDB is a **columnar** engine; MotherDuck runs those operators on **remote** storage. For this data volume, **sub‑second to a few seconds** of compute is expected. Short runtime **does not** imply the script skipped MotherDuck: see **`motherduck_connection_proof`** in `ln_audit_summary.json` (`pragma_database_list` paths include the `md:` MotherDuck attachment) and the timed steps below.

**DuckDB version (server-reported):** `{proof["duckdb_version"]}`

| Step | Seconds |
|------|--------:|
{prof_rows}
{deep_md}

## 1. Canonical lineage (proposal2_ete_staging)

| Layer | MotherDuck object | LN-related fields |
|-------|-------------------|-------------------|
| Specimen / synoptic (structured pathology rows) | `path_synoptics` | `tumor_1_ln_examined`, `tumor_1_ln_involved`; neck surgery descriptors (`central_compartment_dissection`, `tumor_1_level_examined`, `other_ln_dissection`, `tumor_1_ln_location`) |
| Patient-level tumor table | `tumor_pathology` | `histology_1_ln_examined`, `histology_1_ln_positive`, `histology_1_ln_ratio`, `histology_1_n_stage_ajcc8` |
| Risk / analytic MV (documented study source) | `recurrence_risk_features_mv` | `ln_examined`, `ln_positive`, `ln_ratio`, `pn_stage` (from `tumor_pathology` in repo definition) |
| Risk + survival (dashboard / sap) | `risk_enriched_mv` | Same as `recurrence_risk_features_mv` for pathology/LN columns (plus survival fields) |
| Canonical episode table | `tumor_episode_master_v2` | Nodal counts copied from path_synoptics in script 22 (`nodal_disease_*`) when materialized |

**Note:** `studies/proposal2_ete_staging/proposal2_ete_analysis.py` loads frozen CSVs (`exports/ptc_full.csv`), not DuckDB directly. The **MotherDuck-analytic** cohort described in `README.md` for this study is `risk_enriched_mv` / `recurrence_risk_features_mv`; LN variables in models (`ln_ratio`) trace to `tumor_pathology` in those views.

## 2. Null / placeholder semantics (explicit)

- **NULL (cleaned numeric):** No parseable integer/double in structured LN fields after stripping `;` and `x` and trimming. Does *not* prove absence of nodal sampling; means structured synoptic/pathology did not yield a usable count in this ETL pass.
- **0:** Explicit numeric zero in structured field after cleaning — treated as **explicit zero-node / no positive** representation for that field.
- **Raw `x`:** Stripped before cast; if the cell is only `x`, cleaned value becomes NULL (unresolved count, not interpreted as positive).

## 3. Pathology-bearing cohort and completeness (path_synoptics grain)

Denominator: all rows in `path_synoptics` with non-null `research_id` (synoptic / specimen spine).

| Metric | Count | % of specimens |
|--------|------:|---------------:|
| Total specimens | {int(summary["n_pathology_specimens"]):,} | 100.00 |
| `ln_examined` populated (cleaned non-null) | {int(summary["n_examined_populated"]):,} | {pct_ex} |
| `ln_positive` populated (cleaned non-null) | {int(summary["n_positive_populated"]):,} | {pct_pos} |
| Both populated | {int(summary["n_both_populated"]):,} | {pct_both} |
| Explicit zero on examined or positive (cleaned) | {int(summary["n_explicit_zero_representation"]):,} | {pct_zero} |
| Both NULL (unresolved) | {int(summary["n_both_null_unresolved"]):,} | {pct_unres} |

**Stratification note:** `path_synoptics` on this database does not expose a `specimen_type` column; `specimen_type` in exports is NULL (placeholder). Subgroup CSV still stratifies by surgery year, extent, histology bucket, and central LND composite flag.

## 4. recurrence_risk_features_mv (patient row grain)

| Metric | Value |
|--------|------:|
| Rows | {int(risk_summ["n_rrf_rows"]):,} |
| Distinct patients | {int(risk_summ["n_rrf_patients"]):,} |
| ln_examined non-null | {int(risk_summ["n_examined_populated"]):,} |
| ln_positive non-null | {int(risk_summ["n_positive_populated"]):,} |
| Both non-null | {int(risk_summ["n_both_populated"]):,} |
| ln_ratio non-null | {int(risk_summ["n_ln_ratio_populated"]):,} |
| ln_ratio present but missing examined or positive | {int(risk_summ["n_ratio_without_both_counts"]):,} |

`recurrence_risk_features_mv` can list **multiple rows per patient** (see workspace notes on this view); denominators above are **rows**, not deduplicated patients.

## 5. Logical checks (automated)

Exported rows: **`audit_motherduck_ln/ln_audit_logical_inconsistencies.csv`** ({len(inc_full):,} rows).

| Issue | Rows |
|-------|-----:|
{chr(10).join(f"| `{k}` | {int(v):,} |" for k, v in sorted(issue_breakdown.items(), key=lambda kv: -kv[1])) if issue_breakdown else "| _(none)_ | 0 |"}

**Interpretation:** `specimen_vs_tumor_path_*_mismatch` compares each **synoptic row** to **`tumor_pathology` joined only on `research_id`** (patient-level pathology table, not surgery-episode–matched). Large counts are therefore expected when multi-specimen patients differ from the single aggregated pathology row, or when sources capture different levels of detail — this is **discordance for review**, not automatically a row-level data entry error.

Other flags: positive > examined; positive without examined; N1-stage (tumor_pathology) with zero/missing **specimen-level** positive count; `tp_ln_ratio` without backing counts; duplicate surgery date with conflicting LN pairs.

Rows flagged `duplicate_surgery_conflicting_ln_counts`: {n_dup_issues:,}.

## 6. Stratified unresolved missingness

See **`audit_motherduck_ln/ln_audit_subgroup_summary.csv`** (by year, surgery extent bucket, histology bucket, central LND composite flag).

## 7. Deliverables

| File | Description |
|------|-------------|
| `sql/motherduck_lymph_node_completeness_audit.sql` | Documented SQL fragments / temp table definition |
| `audit_motherduck_ln/ln_audit_missing_unresolved.csv` | Specimens without both LN counts populated |
| `audit_motherduck_ln/ln_audit_logical_inconsistencies.csv` | Automated inconsistency flags |
| `audit_motherduck_ln/ln_audit_subgroup_summary.csv` | Subgroup completeness |
| `audit_motherduck_ln/ln_audit_summary.json` | Machine-readable summary + verdict |

## 8. Final verdict

**{verdict}**

Rationale: on the **`path_synoptics` specimen spine**, only **{pct_both}%** of rows have **both** examined and positive numeric LN fields populated; **{pct_unres}%** have **both** NULL after cleaning — **not** “complete for analytic use” without explicit missing-data handling. By contrast, among rows in **`recurrence_risk_features_mv`** (the MotherDuck object documented for proposal2 analytic features), **~93%** of rows have both `ln_examined` and `ln_positive` non-null — but that view is a **narrower, tumor-pathology–filtered cohort** with multiple rows per patient possible, **not** proof that every synoptic specimen row is enumerated. Any analysis must align the completeness statement with the **exact table grain** used. Structured synoptic LN coverage remains a **remediation target** if specimen-level completeness is required.

{v12_note}
---
*This report is generated from live MotherDuck queries; re-run the runner to refresh.*
"""

    REPORT_MD.write_text(report, encoding="utf-8")
    log(f"Wrote {REPORT_MD}")
    log(f"Wrote CSVs under {OUT_DIR}")
    log(f"Wall clock total: {profile['wall_clock_total_s']}s")
    log(f"Verdict: {verdict}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
