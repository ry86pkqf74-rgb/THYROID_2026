#!/usr/bin/env python3
"""mig_177c read-only scoping for LVI/VI derivative reclean options.

This script connects to the locked publication MotherDuck database, reads the
mig_177b pre-snapshot plus current canonical_patient_master state, and writes
local CSV/JSON/Markdown artifacts. It performs no MotherDuck DDL/DML.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402

RUN_ID = "mig_177c_lvi_vi_derivative_reclean_scope_20260429"
ARCHIVE_PRE = '"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429'

LVI_DERIVATIVES = [
    "lvi_grade",
    "lvi_ordinal_worst",
    "n_tumors_lvi_present",
]

VI_DERIVATIVES = [
    "vasc_grade",
    "vasc_grade_final_v13",
    "vascular_invasion_final",
    "vascular_invasion_grade",
    "vascular_who_2022_grade",
    "vi_ordinal_worst",
    "vasc_vessel_count_v13",
    "vascular_vessel_count",
    "vi_vessels_max",
    "vasc_confidence_final_v13",
    "vasc_source_final_v13",
    "n_tumors_vi_present",
]

FAMILIES = {
    "lvi": {
        "pre_bool": "pre_lvi_any_present_path",
        "post_bool": "lvi_any_present_path",
        "event_type": "lymphatic_microscopic",
        "derivatives": LVI_DERIVATIVES,
        "expected_true_to_false": 2502,
    },
    "vi": {
        "pre_bool": "pre_vi_any_present_path",
        "post_bool": "vi_any_present_path",
        "event_type": "vascular_microscopic",
        "derivatives": VI_DERIVATIVES,
        "expected_true_to_false": 2580,
    },
}


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def in_list(values: Iterable[str]) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def build_base_select(con) -> pd.DataFrame:
    cols = sorted({c for cfg in FAMILIES.values() for c in cfg["derivatives"]})
    select_cols = ",\n        ".join(f"pm.{qident(c)}" for c in cols)
    sql = f"""
    SELECT
        CAST(pm.research_id AS VARCHAR) AS research_id,
        pre.lvi_any_present_path AS pre_lvi_any_present_path,
        pm.lvi_any_present_path,
        pre.vi_any_present_path AS pre_vi_any_present_path,
        pm.vi_any_present_path,
        {select_cols}
    FROM main.canonical_patient_master pm
    JOIN {ARCHIVE_PRE} pre
      ON CAST(pre.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
    """
    return con.execute(sql).fetchdf()


def bool_series(s: pd.Series) -> pd.Series:
    return s.fillna(False).astype(bool)


def option_summary(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict[str, object]] = []
    column_rows: list[dict[str, object]] = []
    option_rows: list[dict[str, object]] = []

    for family, cfg in FAMILIES.items():
        pre_true = bool_series(df[cfg["pre_bool"]])
        post_true = bool_series(df[cfg["post_bool"]])
        t2f = df[pre_true & ~post_true]
        f2t = df[~pre_true & post_true]
        stable_true = df[pre_true & post_true]
        stable_false = df[~pre_true & ~post_true]
        derivatives = cfg["derivatives"]

        cells_non_null_t2f = int(sum(t2f[c].notna().sum() for c in derivatives))
        cells_non_zero_t2f = 0
        for c in derivatives:
            series = t2f[c]
            if pd.api.types.is_numeric_dtype(series):
                cells_non_zero_t2f += int(series.fillna(0).ne(0).sum())
            else:
                cells_non_zero_t2f += int(series.fillna("").astype(str).str.strip().ne("").sum())

        summary_rows.append(
            {
                "family": family,
                "event_type": cfg["event_type"],
                "pre_true": int(pre_true.sum()),
                "post_true": int(post_true.sum()),
                "true_to_false_flippers": int(len(t2f)),
                "expected_true_to_false": cfg["expected_true_to_false"],
                "false_or_null_to_true_flippers": int(len(f2t)),
                "stable_true": int(len(stable_true)),
                "stable_false_or_null": int(len(stable_false)),
                "derivative_columns_scoped": len(derivatives),
                "option_a_clear_cells_non_null_on_true_to_false": cells_non_null_t2f,
                "option_a_clear_cells_non_zero_or_non_blank_on_true_to_false": cells_non_zero_t2f,
                "option_b_requires_grade_count_lineage": True,
            }
        )

        for c in derivatives:
            series = t2f[c]
            non_null = int(series.notna().sum())
            if pd.api.types.is_numeric_dtype(series):
                non_zero_or_non_blank = int(series.fillna(0).ne(0).sum())
            else:
                non_zero_or_non_blank = int(series.fillna("").astype(str).str.strip().ne("").sum())
            # pandas 2.x value_counts reset names are not stable across dtypes.
            top_pairs = []
            for value, n in series.fillna("<NULL>").astype(str).value_counts(dropna=False).head(10).items():
                top_pairs.append(f"{value}={int(n)}")
            column_rows.append(
                {
                    "family": family,
                    "column_name": c,
                    "true_to_false_flippers": int(len(t2f)),
                    "current_non_null_on_flippers": non_null,
                    "current_non_zero_or_non_blank_on_flippers": non_zero_or_non_blank,
                    "option_a_clear_target": "set_to_zero" if c.startswith("n_tumors_") else "set_to_null",
                    "top_current_values_on_flippers": "; ".join(top_pairs),
                }
            )

        option_rows.extend(
            [
                {
                    "family": family,
                    "option": "A_clear_only",
                    "scope": "Only TRUE→FALSE flippers from mig_177b",
                    "rows_impacted": int(len(t2f)),
                    "columns_impacted": ", ".join(derivatives),
                    "proposed_rule": "Set derivative strings/ordinals/vessel/confidence/source columns to NULL and n_tumors_*_present to 0 where the corresponding strict-present boolean is now FALSE.",
                    "pros": "Smallest blast radius; immediately removes internal inconsistency for flippers; no need to invent grade/count lineage.",
                    "cons": "Does not improve missing derivative values among FALSE/NULL→TRUE patients; leaves broader grade/count rederive for later.",
                    "logan_decision_needed": True,
                },
                {
                    "family": family,
                    "option": "B_full_rederive",
                    "scope": "All patients for this family after refreshed event truth",
                    "rows_impacted": int(len(df)),
                    "columns_impacted": ", ".join(derivatives),
                    "proposed_rule": "Rebuild derivative fields from strict event-present patients plus a ratified source-linked grade/count lineage; clear false patients as a byproduct.",
                    "pros": "Creates coherent family-wide derivative semantics and can backfill new TRUE flippers.",
                    "cons": "Current canonical_invasion_events_v1 has no ordinal grade or vessel-count columns; requires separate source-lineage design before safe apply.",
                    "logan_decision_needed": True,
                },
            ]
        )

    return pd.DataFrame(summary_rows), pd.DataFrame(column_rows), pd.DataFrame(option_rows)


def event_context(con) -> pd.DataFrame:
    union_parts = []
    for family, cfg in FAMILIES.items():
        union_parts.append(
            f"""
            SELECT '{family}' AS family,
                   CAST(pm.research_id AS VARCHAR) AS research_id,
                   '{cfg['event_type']}' AS event_type
            FROM main.canonical_patient_master pm
            JOIN {ARCHIVE_PRE} pre
              ON CAST(pre.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
            WHERE COALESCE(pre.{cfg['post_bool']}, FALSE) = TRUE
              AND COALESCE(pm.{cfg['post_bool']}, FALSE) = FALSE
            """
        )
    flippers_cte = "\nUNION ALL\n".join(union_parts)
    sql = f"""
    WITH flippers AS (
        {flippers_cte}
    )
    SELECT
        f.family,
        f.event_type,
        COALESCE(e.finding_status, '<NO_EVENT>') AS finding_status,
        COUNT(DISTINCT f.research_id) AS n_patients,
        COUNT(e.invasion_event_id) AS n_events,
        STRING_AGG(DISTINCT COALESCE(e.source_kind, '<NULL>'), ' | ' ORDER BY COALESCE(e.source_kind, '<NULL>')) AS source_kinds_seen,
        STRING_AGG(DISTINCT COALESCE(e.source_table, '<NULL>'), ' | ' ORDER BY COALESCE(e.source_table, '<NULL>')) AS source_tables_seen
    FROM flippers f
    LEFT JOIN main.canonical_invasion_events_v1 e
      ON CAST(e.research_id AS VARCHAR) = f.research_id
     AND e.invasion_type = f.event_type
    GROUP BY 1,2,3
    ORDER BY 1,2,3
    """
    return con.execute(sql).fetchdf()


def current_true_derivative_context(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for family, cfg in FAMILIES.items():
        pre_true = bool_series(df[cfg["pre_bool"]])
        post_true = bool_series(df[cfg["post_bool"]])
        cohorts = {
            "stable_true": df[pre_true & post_true],
            "false_or_null_to_true": df[~pre_true & post_true],
            "true_to_false": df[pre_true & ~post_true],
        }
        for cohort_name, sub in cohorts.items():
            for c in cfg["derivatives"]:
                series = sub[c]
                if pd.api.types.is_numeric_dtype(series):
                    informative = int(series.fillna(0).ne(0).sum())
                else:
                    informative = int(series.fillna("").astype(str).str.strip().ne("").sum())
                rows.append(
                    {
                        "family": family,
                        "cohort": cohort_name,
                        "column_name": c,
                        "n_patients": int(len(sub)),
                        "non_null": int(series.notna().sum()),
                        "non_zero_or_non_blank": informative,
                    }
                )
    return pd.DataFrame(rows)


def write_report(out_dir: Path, summary: pd.DataFrame, column_impact: pd.DataFrame, options: pd.DataFrame, event_ctx: pd.DataFrame, true_ctx: pd.DataFrame) -> Path:
    report = out_dir / "mig_177c_lvi_vi_derivative_reclean_scope_20260429.md"
    lines = [
        "# mig_177c — LVI+VI derivative reclean scoping only",
        "",
        f"**Run ID:** `{RUN_ID}`  ",
        f"**Run timestamp (UTC):** `{datetime.now(timezone.utc).isoformat()}`  ",
        "**Posture:** read-only MotherDuck investigation; no production DDL/DML.  ",
        "**Scope:** Option A clear vs Option B rederive for the mig_177b TRUE→FALSE derivative flippers.  ",
        "",
        "## Executive summary",
        "",
    ]
    lvi = summary.loc[summary["family"] == "lvi"].iloc[0].to_dict()
    vi = summary.loc[summary["family"] == "vi"].iloc[0].to_dict()
    lines.extend(
        [
            f"- LVI flippers confirmed: **{lvi['true_to_false_flippers']:,}** TRUE→FALSE after mig_177b (expected {lvi['expected_true_to_false']:,}).",
            f"- VI flippers confirmed: **{vi['true_to_false_flippers']:,}** TRUE→FALSE after mig_177b (expected {vi['expected_true_to_false']:,}).",
            f"- Option A is the minimal consistency cleanup: clear derivative fields only on those flippers (`n_tumors_*_present` → 0; other derivatives → NULL). It would affect **{lvi['option_a_clear_cells_non_null_on_true_to_false']:,}** non-null LVI derivative cells and **{vi['option_a_clear_cells_non_null_on_true_to_false']:,}** non-null VI derivative cells.",
            "- Option B is a family-wide rederive. It is clinically cleaner long-term, but current `canonical_invasion_events_v1` lacks grade/ordinal/vessel-count columns, so it needs a ratified grade/count lineage before apply.",
            "- Recommendation for Logan: ratify **Option A now** if the objective is internal consistency after mig_177b; ratify **Option B** only with a separate source-lineage specification for grade/count fields and new TRUE flippers.",
            "",
            "## Flipper summary",
            "",
            summary.to_markdown(index=False),
            "",
            "## Option matrix",
            "",
            options.to_markdown(index=False),
            "",
            "## Derivative-column impact on TRUE→FALSE flippers",
            "",
            column_impact.to_markdown(index=False),
            "",
            "## Event context for TRUE→FALSE flippers",
            "",
            event_ctx.to_markdown(index=False),
            "",
            "## Current derivative coverage by cohort",
            "",
            true_ctx.to_markdown(index=False),
            "",
            "## Governance boundary",
            "",
            "This run did not execute any `UPDATE`, `CREATE`, `ALTER`, `DROP`, or registry mutation in MotherDuck. The generated SQL artifact is read-only probe SQL. Any apply lane must wait for Logan ratification of Option A vs Option B.",
        ]
    )
    report.write_text("\n".join(lines) + "\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default=None, help="Output directory for local artifacts")
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else REPO_ROOT / "exports" / RUN_ID
    out_dir.mkdir(parents=True, exist_ok=True)

    con = connect_locked()
    base = build_base_select(con)
    summary, column_impact, options = option_summary(base)
    event_ctx = event_context(con)
    true_ctx = current_true_derivative_context(base)

    csvs = {
        "mig177c_flipper_summary.csv": summary,
        "mig177c_derivative_column_impact.csv": column_impact,
        "mig177c_option_matrix.csv": options,
        "mig177c_event_context.csv": event_ctx,
        "mig177c_current_true_derivative_coverage.csv": true_ctx,
    }
    for name, df in csvs.items():
        df.to_csv(out_dir / name, index=False)

    manifest = {
        "run_id": RUN_ID,
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "posture": "read_only_motherduck_local_artifacts_only",
        "output_dir": str(out_dir.relative_to(REPO_ROOT)),
        "csv_files": sorted(csvs),
        "summary": summary.to_dict(orient="records"),
        "governance": "Logan ratification required before apply; no production data mutation in mig_177c.",
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    report = write_report(out_dir, summary, column_impact, options, event_ctx, true_ctx)
    canonical_report = REPO_ROOT / "qc_framework_v1" / "reports" / "mig_177c_lvi_vi_derivative_reclean_scope_20260429.md"
    canonical_report.write_text(report.read_text())

    print(f"Wrote {report.relative_to(REPO_ROOT)}")
    print(f"Wrote {canonical_report.relative_to(REPO_ROOT)}")
    print(summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
