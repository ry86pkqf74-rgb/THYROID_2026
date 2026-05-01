#!/usr/bin/env python3
"""mig_252 dry-run: strict complication confirmed rollup for CPM.

Read-only against MotherDuck main.* tables. The script creates only session TEMP
objects, then writes local aggregate artifacts for Logan sign-off before the
live migration updates main.canonical_patient_master.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from motherduck_client import get_token, token_mode  # noqa: E402

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
RUN_ID = "mig_252_comp_confirmed_rollup_fix_20260501"

EVENT_TYPES = (
    "airway_complication",
    "chyle_leak",
    "hematoma",
    "hypocalcemia_clinical",
    "hypoparathyroidism",
    "mortality",
    "pneumothorax",
    "rln_injury",
    "seroma",
    "vocal_cord_paralysis",
    "wound_dehiscence",
    "wound_infection",
)

CONFIRMED_ALIASES = {
    "comp_chyle_leak_confirmed": "chyle_leak",
    "comp_hematoma_confirmed": "hematoma",
    "comp_hypocalcemia_confirmed": "hypocalcemia_clinical",
    "comp_hypoparathyroidism_confirmed": "hypoparathyroidism",
    "comp_rln_injury_confirmed": "rln_injury",
    "comp_seroma_confirmed": "seroma",
    "comp_vc_paralysis_confirmed": "vocal_cord_paralysis",
    "comp_wound_infection_confirmed": "wound_infection",
}

# Source-strict legacy alias: canonical_complications_events_v1 does not retain
# a separate vocal_cord_paresis complication_type after Script 364 vocabulary
# consolidation. This therefore dry-runs to FALSE for all patients; Logan must
# explicitly approve that policy before apply.
SOURCE_ABSENT_CONFIRMED_ALIASES = {
    "comp_vc_paresis_confirmed": "vc_paresis_source_absent_in_canonical_events",
}

SUSPECTED_ALIASES = {
    "comp_chyle_leak_suspected": "chyle_leak",
    "comp_hematoma_suspected": "hematoma",
    "comp_hypocalcemia_suspected": "hypocalcemia_clinical",
    "comp_hypoparathyroidism_suspected": "hypoparathyroidism",
    "comp_rln_injury_suspected": "rln_injury",
    "comp_seroma_suspected": "seroma",
    "comp_vc_paralysis_suspected": "vocal_cord_paralysis",
    "comp_wound_infection_suspected": "wound_infection",
}

SOURCE_ABSENT_SUSPECTED_ALIASES = {
    "comp_vc_paresis_suspected": "vc_paresis_source_absent_in_canonical_events",
}


def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        raise SystemExit(
            f"No MotherDuck token available (token_mode={token_mode()})."
        )
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={token}")
    con.execute(f'USE "{CANONICAL_DB}"')
    return con


def fetch_dicts(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, object]]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]


def existing_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        """
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE table_schema='main'
          AND table_name='canonical_patient_master'
        """
    ).fetchall()
    return {str(row[0]) for row in rows}


def bool_expr(event_type: str, flavor: str) -> str:
    base = (
        f"LOWER(e.complication_type) = '{event_type}' "
        "AND e.finding_status = 'present'"
    )
    if flavor == "definitive":
        return f"{base} AND e.evidence_strength = 'definitive'"
    if flavor in {"probable_or_better", "confirmed"}:
        return f"{base} AND e.evidence_strength IN ('definitive','probable')"
    if flavor == "any_evidence":
        return base
    if flavor == "suspected":
        return f"{base} AND e.evidence_strength = 'possible'"
    raise ValueError(flavor)


def add_target(targets: list[dict[str, str]], column: str, event_type: str, flavor: str) -> None:
    targets.append({"column_name": column, "event_type": event_type, "flavor": flavor})


def build_targets(columns: set[str]) -> list[dict[str, str]]:
    targets: list[dict[str, str]] = []

    for event_type in EVENT_TYPES:
        for flavor in ("definitive", "probable_or_better", "any_evidence"):
            column = f"comp_{event_type}_{flavor}"
            if column in columns:
                add_target(targets, column, event_type, flavor)

    for column, event_type in CONFIRMED_ALIASES.items():
        if column in columns:
            add_target(targets, column, event_type, "confirmed")

    for column, reason in SOURCE_ABSENT_CONFIRMED_ALIASES.items():
        if column in columns:
            targets.append({"column_name": column, "event_type": reason, "flavor": "source_absent_false"})

    for column, event_type in SUSPECTED_ALIASES.items():
        if column in columns:
            add_target(targets, column, event_type, "suspected")

    for column, reason in SOURCE_ABSENT_SUSPECTED_ALIASES.items():
        if column in columns:
            targets.append({"column_name": column, "event_type": reason, "flavor": "source_absent_false"})

    return sorted(targets, key=lambda row: row["column_name"])


def create_temp_rollup(con: duckdb.DuckDBPyConnection, targets: Iterable[dict[str, str]]) -> None:
    target_exprs: list[str] = []
    for target in targets:
        column = target["column_name"]
        flavor = target["flavor"]
        if flavor == "source_absent_false":
            target_exprs.append(f"FALSE AS {column}")
            continue
        target_exprs.append(
            "COALESCE(BOOL_OR("
            + bool_expr(target["event_type"], flavor)
            + f"), FALSE) AS {column}"
        )

    any_expr = (
        "COALESCE(BOOL_OR(e.finding_status = 'present' "
        "AND e.evidence_strength IN ('definitive','probable')), FALSE)"
    )
    n_expr = (
        "COUNT(DISTINCT CASE WHEN e.finding_status = 'present' "
        "AND e.evidence_strength IN ('definitive','probable') "
        "THEN LOWER(e.complication_type) END)"
    )

    sql = f"""
    CREATE TEMP TABLE _mig252_corrected_rollup AS
    SELECT
      CAST(pm.research_id AS VARCHAR) AS research_id,
      {any_expr} AS any_confirmed_complication_flag,
      {any_expr} AS any_confirmed_complication,
      {n_expr} AS n_confirmed_complications,
      {",\n      ".join(target_exprs)}
    FROM main.canonical_patient_master pm
    LEFT JOIN main.canonical_complications_events_v1 e
      ON CAST(e.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
    GROUP BY 1
    """
    con.execute(sql)


def diff_sql(column: str, dtype: str = "BOOLEAN") -> str:
    if dtype == "BIGINT":
        return f"""
        SELECT
          '{column}' AS column_name,
          'count_distinct_strict_confirmed_types' AS proposed_definition,
          SUM(CASE WHEN COALESCE(pm.{column}, 0) > 0 THEN 1 ELSE 0 END) AS current_true_or_positive_patients,
          SUM(CASE WHEN COALESCE(dr.{column}, 0) > 0 THEN 1 ELSE 0 END) AS proposed_true_or_positive_patients,
          SUM(CASE WHEN COALESCE(pm.{column}, 0) <> COALESCE(dr.{column}, 0) THEN 1 ELSE 0 END) AS mismatched_patients,
          SUM(COALESCE(pm.{column}, 0)) AS current_sum,
          SUM(COALESCE(dr.{column}, 0)) AS proposed_sum,
          SUM(CASE WHEN COALESCE(pm.{column}, 0) > COALESCE(dr.{column}, 0) THEN 1 ELSE 0 END) AS decreases,
          SUM(CASE WHEN COALESCE(pm.{column}, 0) < COALESCE(dr.{column}, 0) THEN 1 ELSE 0 END) AS increases
        FROM main.canonical_patient_master pm
        JOIN _mig252_corrected_rollup dr ON CAST(pm.research_id AS VARCHAR)=dr.research_id
        """
    return f"""
    SELECT
      '{column}' AS column_name,
      'see target_map' AS proposed_definition,
      SUM(CASE WHEN COALESCE(pm.{column}, FALSE) THEN 1 ELSE 0 END) AS current_true_or_positive_patients,
      SUM(CASE WHEN COALESCE(dr.{column}, FALSE) THEN 1 ELSE 0 END) AS proposed_true_or_positive_patients,
      SUM(CASE WHEN COALESCE(pm.{column}, FALSE) <> COALESCE(dr.{column}, FALSE) THEN 1 ELSE 0 END) AS mismatched_patients,
      NULL AS current_sum,
      NULL AS proposed_sum,
      SUM(CASE WHEN COALESCE(pm.{column}, FALSE) AND NOT COALESCE(dr.{column}, FALSE) THEN 1 ELSE 0 END) AS decreases,
      SUM(CASE WHEN NOT COALESCE(pm.{column}, FALSE) AND COALESCE(dr.{column}, FALSE) THEN 1 ELSE 0 END) AS increases
    FROM main.canonical_patient_master pm
    JOIN _mig252_corrected_rollup dr ON CAST(pm.research_id AS VARCHAR)=dr.research_id
    """


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = args.out_dir or (REPO_ROOT / "exports" / f"mig252_comp_rollup_dryrun_{run_ts}")
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    con = connect()
    columns = existing_columns(con)
    targets = build_targets(columns)
    create_temp_rollup(con, targets)

    target_map = [
        {
            "column_name": target["column_name"],
            "event_type": target["event_type"],
            "flavor": target["flavor"],
            "predicate": "FALSE because canonical source type is absent"
            if target["flavor"] == "source_absent_false"
            else bool_expr(target["event_type"], target["flavor"]),
        }
        for target in targets
    ]
    aggregate_targets = [
        {"column_name": "any_confirmed_complication_flag", "event_type": "any", "flavor": "confirmed", "predicate": "any present definitive/probable event"},
        {"column_name": "any_confirmed_complication", "event_type": "any", "flavor": "confirmed", "predicate": "any present definitive/probable event"},
        {"column_name": "n_confirmed_complications", "event_type": "any", "flavor": "confirmed", "predicate": "count distinct complication_type with present definitive/probable event"},
    ]

    diff_queries = [diff_sql("any_confirmed_complication_flag"), diff_sql("any_confirmed_complication"), diff_sql("n_confirmed_complications", "BIGINT")]
    for target in targets:
        diff_queries.append(diff_sql(target["column_name"]))
    diff_rows = fetch_dicts(con, " UNION ALL ".join(diff_queries) + " ORDER BY column_name")

    event_distribution = fetch_dicts(
        con,
        """
        SELECT LOWER(complication_type) AS complication_type, finding_status, evidence_strength,
               COUNT(*) AS n_events, COUNT(DISTINCT research_id) AS n_patients
        FROM main.canonical_complications_events_v1
        GROUP BY 1,2,3
        ORDER BY 1,4 DESC
        """,
    )

    current_counts = fetch_dicts(
        con,
        """
        SELECT COUNT(*) AS n_patients,
               SUM(CASE WHEN any_confirmed_complication_flag THEN 1 ELSE 0 END) AS current_any_confirmed,
               (SELECT SUM(CASE WHEN any_confirmed_complication_flag THEN 1 ELSE 0 END) FROM _mig252_corrected_rollup) AS proposed_any_confirmed,
               (SELECT SUM(n_confirmed_complications) FROM _mig252_corrected_rollup) AS proposed_n_confirmed_sum
        FROM main.canonical_patient_master
        """,
    )

    m038_rows: list[dict[str, object]] = []
    has_m038 = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='manuscript_workspace'
          AND table_name='cohort_m038_massive_goiter_v1'
        """
    ).fetchone()
    if has_m038:
        m038_rows = fetch_dicts(
            con,
            """
            SELECT
              CASE
                WHEN gland_weight_final_g >= 200 THEN 'ge_200g'
                WHEN gland_weight_final_g < 200 THEN 'lt_200g'
                ELSE 'weight_null'
              END AS subset,
              COUNT(*) AS n,
              SUM(CASE WHEN c.any_confirmed_complication_flag THEN 1 ELSE 0 END) AS current_any_confirmed,
              SUM(CASE WHEN dr.any_confirmed_complication_flag THEN 1 ELSE 0 END) AS proposed_any_confirmed
            FROM manuscript_workspace.cohort_m038_massive_goiter_v1 c
            JOIN _mig252_corrected_rollup dr
              ON CAST(c.research_id AS VARCHAR) = dr.research_id
            GROUP BY 1
            ORDER BY 1
            """,
        )

    qc_status: list[dict[str, object]] = []
    try:
        qc_status = fetch_dicts(con, "SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1")
    except Exception as exc:  # read-only diagnostic should not block dry-run diff
        qc_status = [{"error": repr(exc)}]

    write_csv(out_dir / "target_map.csv", aggregate_targets + target_map)
    write_csv(out_dir / "dryrun_diff.csv", diff_rows)
    write_csv(out_dir / "event_distribution.csv", event_distribution)
    write_csv(out_dir / "m038_subset_impact.csv", m038_rows)
    write_csv(out_dir / "qc_status.csv", qc_status)
    manifest = {
        "run_id": RUN_ID,
        "run_ts": run_ts,
        "database": CANONICAL_DB,
        "token_mode": token_mode(),
        "out_dir": str(out_dir.relative_to(REPO_ROOT)),
        "target_columns": len(target_map),
        "main_mutations": 0,
        "temp_table": "_mig252_corrected_rollup",
        "summary": current_counts,
        "vocal_paresis_policy": "source-strict FALSE because canonical_complications_events_v1 has no vc_paresis complication_type",
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print(json.dumps(manifest, indent=2, default=str))
    print("\nTop diffs:")
    for row in diff_rows:
        if int(row.get("mismatched_patients") or 0) > 0:
            print(
                f"{row['column_name']}: {row['current_true_or_positive_patients']} -> "
                f"{row['proposed_true_or_positive_patients']} "
                f"(mismatch={row['mismatched_patients']}, "
                f"down={row['decreases']}, up={row['increases']})"
            )
    if m038_rows:
        print("\nM038 subset impact:")
        for row in m038_rows:
            print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())