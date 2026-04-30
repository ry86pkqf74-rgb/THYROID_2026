"""
mig_204 — Populate Table 1 + cohort flow + 5 analytic template CSVs from live MotherDuck.

Deliverables written:
  qc_framework_v1/manuscript/table_1_cohort_characteristics.csv
  qc_framework_v1/manuscript/cohort_flow_diagram.csv
  qc_framework_v1/manuscript/analytic_templates/previews/01_overall_survival_preview.csv
  qc_framework_v1/manuscript/analytic_templates/previews/02_recurrence_free_survival_preview.csv
  qc_framework_v1/manuscript/analytic_templates/previews/03_stage_group_by_histology_preview.csv
  qc_framework_v1/manuscript/analytic_templates/previews/04_complication_rate_by_surgery_type_preview.csv
  qc_framework_v1/manuscript/analytic_templates/previews/05_cohort_flow_rid_lists_preview.csv

Usage:
  .venv/bin/python qc_framework_v1/scripts/build_mig204_populate_manuscript_csvs.py
"""

import os
import sys
import csv
import io
import textwrap
import traceback
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Token loading (mirrors motherduck_client.py pattern)
# ---------------------------------------------------------------------------
def _get_token() -> str:
    for env_key in ("MOTHERDUCK_TOKEN", "MD_SA_TOKEN"):
        v = os.environ.get(env_key, "")
        if v:
            print(f"[token] loaded from env {env_key}, length={len(v)}")
            return v
    try:
        import toml
        d = toml.load("motherduck.local.toml")
        for k in ("MOTHERDUCK_TOKEN", "MD_SA_TOKEN"):
            v = d.get(k, "")
            if v:
                print(f"[token] loaded from motherduck.local.toml key={k}, length={len(v)}")
                return v
    except Exception as e:
        print(f"[token] toml load failed: {e}")
    raise RuntimeError("No MotherDuck token found in env or motherduck.local.toml")


# ---------------------------------------------------------------------------
# SQL definitions (inline — avoids file-path issues)
# ---------------------------------------------------------------------------

PREFLIGHT_SQL = """
USE thyroid_canonical_publication_v1_0;
SELECT batch_id, COUNT(*) AS n_registry_hits
FROM main.canonical_column_verification_registry_v1
WHERE batch_id IN (
  'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430',
  'mig186b_apply_RD_niftp_exclusion_no_gate3_break_20260430',
  'mig185b_apply_rollup_only_patch_no_transaction_20260430',
  'mig_187_apply_RA_script366_extension_ratified_20260430'
)
GROUP BY 1
ORDER BY 1;
"""

TABLE1_SQL = open(
    Path(__file__).parent.parent / "manuscript" / "table_1_cohort_characteristics.sql"
).read()

COHORT_FLOW_SQL = open(
    Path(__file__).parent.parent / "manuscript" / "cohort_flow_diagram.sql"
).read()

TEMPLATE_SQLS = {
    "01_overall_survival": open(
        Path(__file__).parent.parent / "manuscript" / "analytic_templates" / "01_overall_survival.sql"
    ).read(),
    "02_recurrence_free_survival": open(
        Path(__file__).parent.parent / "manuscript" / "analytic_templates" / "02_recurrence_free_survival.sql"
    ).read(),
    "03_stage_group_by_histology": open(
        Path(__file__).parent.parent / "manuscript" / "analytic_templates" / "03_stage_group_by_histology.sql"
    ).read(),
    "04_complication_rate_by_surgery_type": open(
        Path(__file__).parent.parent / "manuscript" / "analytic_templates" / "04_complication_rate_by_surgery_type.sql"
    ).read(),
}

# Template 05 has two queries (QUERY A = cohort flow counts, QUERY B = rid lists).
# We run QUERY A (counts) for the preview CSV — QUERY B is large and optional.
TEMPLATE_05_QUERY_A_SQL = open(
    Path(__file__).parent.parent / "manuscript" / "analytic_templates" / "05_cohort_flow_and_exclusions.sql"
).read().split("-- =============================================================================\n-- QUERY B")[0]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip_use(sql: str) -> str:
    """Remove 'USE <db>;' lines — duckdb Python API uses connect() for DB selection."""
    lines = [l for l in sql.splitlines() if not l.strip().upper().startswith("USE ")]
    return "\n".join(lines)


def _run_query(con, sql: str, label: str):
    """Execute a single-statement SQL and return a list of dicts."""
    clean = _strip_use(sql).strip()
    # Remove trailing semicolons
    while clean.endswith(";"):
        clean = clean[:-1].strip()
    print(f"  [query] {label} — executing ({len(clean)} chars)...")
    try:
        rel = con.execute(clean)
        rows = rel.fetchall()
        cols = [d[0] for d in rel.description]
        result = [dict(zip(cols, row)) for row in rows]
        print(f"  [query] {label} — {len(result)} rows returned")
        return result
    except Exception as e:
        print(f"  [ERROR] {label}: {e}")
        traceback.print_exc()
        return None


def _write_csv(path: Path, rows: list, fieldnames: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  [csv] wrote {len(rows)} rows → {path}")


def _preview_rows(rows: list, n: int = 5) -> list:
    """Return first n rows for preview CSVs (OS/RFS are large — cap at 200)."""
    return rows[:n]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    token = _get_token()
    os.environ["motherduck_token"] = token  # duckdb uses this env var

    import duckdb
    print(f"[connect] duckdb version={duckdb.__version__}")

    con = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}")
    con.execute("USE thyroid_canonical_publication_v1_0")
    print("[connect] connected to thyroid_canonical_publication_v1_0")

    repo_root = Path(__file__).parent.parent.parent  # THyroid 2026/
    manuscript_dir = repo_root / "qc_framework_v1" / "manuscript"
    previews_dir = manuscript_dir / "analytic_templates" / "previews"

    results = {}

    # ------------------------------------------------------------------
    # Pre-flight: registry check
    # ------------------------------------------------------------------
    print("\n=== Pre-flight: registry check ===")
    pf_rows = _run_query(con, PREFLIGHT_SQL, "preflight_registry")
    if pf_rows is not None:
        print(f"  Registry hits: {pf_rows}")
        if len(pf_rows) < 4:
            print("  WARNING: fewer than 4 batch_ids found in registry — some prerequisite migrations may not be applied.")
        else:
            print("  PASS: all 4 prerequisite batch_ids present in registry.")
    results["preflight"] = pf_rows

    # ------------------------------------------------------------------
    # Table 1
    # ------------------------------------------------------------------
    print("\n=== Table 1: cohort characteristics ===")
    t1_rows = _run_query(con, TABLE1_SQL, "table_1")
    if t1_rows:
        _write_csv(
            manuscript_dir / "table_1_cohort_characteristics.csv",
            t1_rows,
            ["sort_key", "characteristic", "level", "n", "pct", "statistic"],
        )
    results["table_1"] = t1_rows

    # ------------------------------------------------------------------
    # Cohort flow
    # ------------------------------------------------------------------
    print("\n=== Cohort flow diagram ===")
    cf_rows = _run_query(con, COHORT_FLOW_SQL, "cohort_flow")
    if cf_rows:
        _write_csv(
            manuscript_dir / "cohort_flow_diagram.csv",
            cf_rows,
            ["step", "description", "n_excluded", "n_remaining"],
        )
    results["cohort_flow"] = cf_rows

    # ------------------------------------------------------------------
    # Analytic templates 01–04 (full rows → preview CSVs)
    # ------------------------------------------------------------------
    template_meta = {
        "01_overall_survival": {
            "sql": TEMPLATE_SQLS["01_overall_survival"],
            "fields": ["research_id", "time_to_event_years", "event_indicator", "strata_var", "strata_role"],
            "preview_n": 200,
        },
        "02_recurrence_free_survival": {
            "sql": TEMPLATE_SQLS["02_recurrence_free_survival"],
            "fields": ["research_id", "time_to_event_years", "event_indicator", "strata_var", "strata_role"],
            "preview_n": 200,
        },
        "03_stage_group_by_histology": {
            "sql": TEMPLATE_SQLS["03_stage_group_by_histology"],
            "fields": ["histology_bucket", "stage_group_resolved", "n_patients", "pct_within_histology", "histology_row_total"],
            "preview_n": None,  # small result — write all
        },
        "04_complication_rate_by_surgery_type": {
            "sql": TEMPLATE_SQLS["04_complication_rate_by_surgery_type"],
            "fields": ["surgery_type_label", "complication_category", "analysis_window",
                       "n_with_complication", "n_patients_in_surgery_bucket",
                       "proportion", "ci95_lower_wald", "ci95_upper_wald"],
            "preview_n": None,  # small result — write all
        },
    }

    for key, meta in template_meta.items():
        print(f"\n=== Template {key} ===")
        rows = _run_query(con, meta["sql"], key)
        if rows:
            n = meta["preview_n"]
            out_rows = rows if n is None else rows[:n]
            _write_csv(
                previews_dir / f"{key}_preview.csv",
                out_rows,
                meta["fields"],
            )
            results[key] = rows

    # ------------------------------------------------------------------
    # Template 05 — QUERY A only (cohort flow counts)
    # ------------------------------------------------------------------
    print("\n=== Template 05: cohort flow + exclusions (QUERY A counts) ===")
    t05_rows = _run_query(con, TEMPLATE_05_QUERY_A_SQL, "05_cohort_flow_query_a")
    if t05_rows:
        _write_csv(
            previews_dir / "05_cohort_flow_rid_lists_preview.csv",
            t05_rows,
            ["step", "description", "n_excluded", "n_remaining"],
        )
    results["05_cohort_flow"] = t05_rows

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n=== mig_204 summary ===")
    for k, v in results.items():
        status = f"{len(v)} rows" if v else "FAILED/EMPTY"
        print(f"  {k}: {status}")

    con.close()
    print("\n[done] mig_204 complete.")
    return results


if __name__ == "__main__":
    main()
