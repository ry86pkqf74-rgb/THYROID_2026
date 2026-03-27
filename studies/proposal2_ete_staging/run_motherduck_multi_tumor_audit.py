#!/usr/bin/env python3
"""MotherDuck (or local parquet) multi-tumor pathology audit for proposal2_ete_staging lineage.

Usage:
  MOTHERDUCK_TOKEN=... .venv/bin/python studies/proposal2_ete_staging/run_motherduck_multi_tumor_audit.py
  MD_SA_TOKEN=... .venv/bin/python studies/proposal2_ete_staging/run_motherduck_multi_tumor_audit.py --sa
  .venv/bin/python studies/proposal2_ete_staging/run_motherduck_multi_tumor_audit.py --local

Outputs:
  studies/proposal2_ete_staging/MOTHERDUCK_MULTI_TUMOR_AUDIT.md
  studies/proposal2_ete_staging/motherduck_multi_tumor_summary_counts.csv
  studies/proposal2_ete_staging/motherduck_multi_tumor_discrepant_cases.csv
  studies/proposal2_ete_staging/sql/motherduck_multi_tumor_audit_generated.sql
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
STUDY = Path(__file__).resolve().parent
SQL_DIR = STUDY / "sql"
SQL_OUT = SQL_DIR / "motherduck_multi_tumor_audit_generated.sql"
REPORT = STUDY / "MOTHERDUCK_MULTI_TUMOR_AUDIT.md"
CSV_SUMMARY = STUDY / "motherduck_multi_tumor_summary_counts.csv"
CSV_DISC = STUDY / "motherduck_multi_tumor_discrepant_cases.csv"
PQ_PS = ROOT / "processed" / "path_synoptics.parquet"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from motherduck_client import get_token, resolve_database_for_env, token_mode  # noqa: E402


def _load_slot_map() -> dict[int, dict[str, str]]:
    p108 = ROOT / "scripts" / "108_synoptic_tumor_long_v1.py"
    spec = importlib.util.spec_from_file_location("syn108", p108)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {p108}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return dict(getattr(mod, "SLOT_MAP"))


def _nonempty_or_chain(table_alias: str, cols: list[str]) -> str:
    parts = [
        f"(TRIM(COALESCE(CAST({table_alias}.{c} AS VARCHAR), '')) "
        f"NOT IN ('', 'nan', 'none', 'null'))"
        for c in cols
    ]
    return " OR ".join(parts)


def _nonempty_or_clause(table_alias: str, cols: list[str]) -> str:
    return "(" + _nonempty_or_chain(table_alias, cols) + ")"


def build_mt_ps_view_sql(slot_map: dict[int, dict[str, str]], ps_ref: str = "path_synoptics") -> str:
    lines = [
        "CREATE OR REPLACE TEMP VIEW _mt_ps AS",
        "SELECT",
        f"  CAST({ps_ref}.research_id AS BIGINT) AS research_id,",
        f"  {ps_ref}.surg_date,",
        f"  TRY_CAST({ps_ref}.surg_date AS DATE) AS surg_d,",
        f"  ROW_NUMBER() OVER (ORDER BY CAST({ps_ref}.research_id AS BIGINT), "
        f"TRY_CAST({ps_ref}.surg_date AS DATE)) AS audit_row_ix,",
    ]
    hlines: list[str] = []
    for i in range(1, 6):
        cols = list(slot_map[i].values())
        ne = _nonempty_or_clause(ps_ref, cols)
        lines.append(f"  {ne} AS slot_{i}_any_nonempty,")
        col_h = slot_map[i]["histologic_type"]
        hlines.append(
            f"(CASE WHEN TRIM(COALESCE(CAST({ps_ref}.{col_h} AS VARCHAR), '')) "
            f"NOT IN ('', 'nan', 'none', 'null') THEN 1 ELSE 0 END)"
        )
    sum_any = " + ".join(
        f"(CASE WHEN ({_nonempty_or_chain(ps_ref, list(slot_map[j].values()))}) THEN 1 ELSE 0 END)"
        for j in range(1, 6)
    )
    lines.append(f"  ({sum_any}) AS n_slots_any,")
    sum_h = " + ".join(hlines)
    lines.append(f"  ({sum_h}) AS n_slots_histology_only,")
    lines.append(
        f"  ROW_NUMBER() OVER (PARTITION BY CAST({ps_ref}.research_id AS BIGINT) "
        f"ORDER BY TRY_CAST({ps_ref}.surg_date AS DATE) DESC NULLS LAST) AS rn_latest_specimen,"
    )
    lines.append(f"  {ps_ref}.tumor_1_multiple_tumor AS tumor_1_multiple_tumor_raw,")
    lines.append(
        "  CASE WHEN LOWER(COALESCE(CAST("
        f"{ps_ref}.tumor_1_multiple_tumor AS VARCHAR), '')) LIKE '%yes%' "
        "OR LOWER(COALESCE(CAST("
        f"{ps_ref}.tumor_1_multiple_tumor AS VARCHAR), '')) LIKE '%multi%' "
        "THEN TRUE ELSE FALSE END AS ps_multifocal_text_flag,"
    )
    sz = [
        f"TRY_CAST(REPLACE(TRIM(CAST({ps_ref}.tumor_{k}_size_greatest_dimension_cm AS VARCHAR)), ';', '') AS DOUBLE)"
        for k in range(1, 6)
    ]
    lines.append(f"  GREATEST({', '.join(['COALESCE(' + s + ', 0)' for s in sz])}) AS max_size_cm_slots,")
    lines.append(f"  {sz[0]} AS tumor_1_size_cm_clean,")
    lines.append(f"FROM {ps_ref}")
    lines.append("WHERE research_id IS NOT NULL")
    lines.append(";")
    return "\n".join(lines)


def _connect(*, local: bool, use_sa: bool) -> tuple[duckdb.DuckDBPyConnection, str]:
    if local:
        con = duckdb.connect(":memory:")
        if not PQ_PS.exists():
            raise SystemExit(f"Missing {PQ_PS}")
        con.execute(f"CREATE TABLE path_synoptics AS SELECT * FROM read_parquet('{PQ_PS.as_posix()}')")
        p108_path = ROOT / "scripts" / "108_synoptic_tumor_long_v1.py"
        spec = importlib.util.spec_from_file_location("syn108b", p108_path)
        assert spec and spec.loader
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        df_long = m.build_long_frame(pd.read_parquet(PQ_PS))
        con.register("synoptic_tumor_long_v1", df_long)
        tem = con.execute(
            """
            SELECT CAST(research_id AS INTEGER) AS research_id,
                   surgery_episode_id,
                   TRY_CAST(surgery_date AS DATE) AS surgery_date,
                   tumor_ordinal,
                   tumor_size_cm,
                   extrathyroidal_extension_raw,
                   extrathyroidal_extension
            FROM (
              SELECT research_id,
                     ROW_NUMBER() OVER (
                       PARTITION BY CAST(research_id AS INTEGER)
                       ORDER BY TRY_CAST(surg_date AS DATE)
                     ) AS surgery_episode_id,
                     TRY_CAST(surg_date AS DATE) AS surgery_date,
                     1 AS tumor_ordinal,
                     TRY_CAST(REPLACE(TRIM(CAST(tumor_1_size_greatest_dimension_cm AS VARCHAR)), ';', '') AS DOUBLE)
                       AS tumor_size_cm,
                     tumor_1_extrathyroidal_extension AS extrathyroidal_extension_raw,
                     CASE
                       WHEN LOWER(COALESCE(tumor_1_extrathyroidal_extension, ''))
                            IN ('', 'no', 'none', 'absent', 'not identified', 'not present', 'negative')
                            THEN 'none'
                       WHEN LOWER(COALESCE(tumor_1_extrathyroidal_extension, '')) ~ '(gross|extensive|pT4)'
                            THEN 'gross'
                       WHEN LOWER(COALESCE(tumor_1_extrathyroidal_extension, '')) ~ '(minimal|microscop|minor|focal)'
                            THEN 'microscopic'
                       WHEN tumor_1_extrathyroidal_extension IS NOT NULL THEN 'present'
                       ELSE NULL
                     END AS extrathyroidal_extension
              FROM path_synoptics
              WHERE research_id IS NOT NULL
            ) x
            """
        ).fetchdf()
        con.register("_tem_df", tem)
        con.execute("CREATE TABLE tumor_episode_master_v2 AS SELECT * FROM _tem_df")
        con.execute(
            """
            CREATE TABLE extracted_multi_tumor_aggregate_v1 AS
            WITH mt AS (
              SELECT DISTINCT CAST(research_id AS BIGINT) AS research_id
              FROM path_synoptics
              WHERE TRIM(COALESCE(CAST(tumor_2_histologic_type AS VARCHAR), '')) <> ''
                 OR TRIM(COALESCE(CAST(tumor_3_histologic_type AS VARCHAR), '')) <> ''
                 OR TRIM(COALESCE(CAST(tumor_4_histologic_type AS VARCHAR), '')) <> ''
                 OR TRIM(COALESCE(CAST(tumor_5_histologic_type AS VARCHAR), '')) <> ''
            )
            SELECT ps.research_id,
              (CASE WHEN TRIM(COALESCE(CAST(ps.tumor_1_histologic_type AS VARCHAR), '')) <> '' THEN 1 ELSE 0 END +
               CASE WHEN TRIM(COALESCE(CAST(ps.tumor_2_histologic_type AS VARCHAR), '')) <> '' THEN 1 ELSE 0 END +
               CASE WHEN TRIM(COALESCE(CAST(ps.tumor_3_histologic_type AS VARCHAR), '')) <> '' THEN 1 ELSE 0 END +
               CASE WHEN TRIM(COALESCE(CAST(ps.tumor_4_histologic_type AS VARCHAR), '')) <> '' THEN 1 ELSE 0 END +
               CASE WHEN TRIM(COALESCE(CAST(ps.tumor_5_histologic_type AS VARCHAR), '')) <> '' THEN 1 ELSE 0 END
              ) AS n_tumors,
              CURRENT_TIMESTAMP AS refined_at
            FROM path_synoptics ps
            INNER JOIN mt ON CAST(ps.research_id AS BIGINT) = mt.research_id
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY ps.research_id ORDER BY TRY_CAST(ps.surg_date AS DATE) DESC NULLS LAST
            ) = 1
            """
        )
        return con, "local:memory+path_synoptics.parquet(synthetic tumor_episode + aggregate)"

    for k in ("USE_LOCAL_DUCKDB", "use_local_duckdb"):
        os.environ.pop(k, None)
    tok = get_token(prefer_service_account=use_sa)
    if not tok:
        raise SystemExit("No token: set MOTHERDUCK_TOKEN or MD_SA_TOKEN, or use --local")
    db = resolve_database_for_env(os.getenv("MOTHERDUCK_ENV", "prod"))
    uri = f"md:{db}?motherduck_token={tok}"
    return duckdb.connect(uri), f"motherduck:{db}"


def _resolve_long_rel(con: duckdb.DuckDBPyConnection) -> str | None:
    for name in ("synoptic_tumor_long_v1", "md_synoptic_tumor_long_v1"):
        r = con.execute(
            f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = '{name}'
            """
        ).fetchone()
        if r and r[0]:
            return name
    return None


def _resolve_tbl(con: duckdb.DuckDBPyConnection, *names: str) -> str | None:
    for name in names:
        r = con.execute(
            f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = '{name}'
            """
        ).fetchone()
        if r and r[0]:
            return name
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true", help="Use path_synoptics.parquet + synthetic deps")
    ap.add_argument("--sa", action="store_true", help="Prefer MD_SA_TOKEN")
    args = ap.parse_args()

    slot_map = _load_slot_map()
    mt_view = build_mt_ps_view_sql(slot_map, "path_synoptics")

    con, source_label = _connect(local=args.local, use_sa=args.sa)
    sql_parts: list[str] = ["-- Generated", mt_view]

    con.execute(mt_view)
    sql_parts.append("\n")

    long_tbl = _resolve_long_rel(con)
    tem_tbl = _resolve_tbl(con, "tumor_episode_master_v2", "md_tumor_episode_master_v2")
    agg_tbl = _resolve_tbl(con, "extracted_multi_tumor_aggregate_v1", "md_extracted_multi_tumor_aggregate_v1")
    tp_tbl = _resolve_tbl(con, "tumor_pathology", "md_tumor_pathology")
    les_tbl = _resolve_tbl(con, "lesion_analysis_resolved_v1", "md_lesion_analysis_resolved_v1")

    long_join = ""
    if long_tbl:
        long_join = f"""
        LEFT JOIN (
          SELECT CAST(research_id AS BIGINT) AS research_id,
                 TRY_CAST(CAST(surg_date AS VARCHAR) AS DATE) AS surg_d,
                 COUNT(*) AS n_long_rows
          FROM {long_tbl}
          GROUP BY 1, 2
        ) lon
          ON lon.research_id = p.research_id
         AND (lon.surg_d IS NOT DISTINCT FROM p.surg_d)
        """
    else:
        long_join = "LEFT JOIN (SELECT NULL::BIGINT AS research_id, NULL::DATE AS surg_d, NULL::BIGINT AS n_long_rows WHERE FALSE) lon ON FALSE"

    tem_join = ""
    if tem_tbl:
        tem_join = f"""
        LEFT JOIN (
          SELECT CAST(research_id AS BIGINT) AS research_id,
                 TRY_CAST(surgery_date AS DATE) AS surg_d,
                 MAX(tumor_size_cm) AS tem_tumor1_size_cm,
                 MAX(tumor_ordinal) AS max_tumor_ordinal,
                 COUNT(*) AS n_tem_rows
          FROM {tem_tbl}
          GROUP BY 1, 2
        ) te
          ON te.research_id = p.research_id
         AND (te.surg_d IS NOT DISTINCT FROM p.surg_d)
        """
    else:
        tem_join = """
        LEFT JOIN (SELECT NULL::BIGINT AS research_id, NULL::DATE AS surg_d,
                          NULL::DOUBLE AS tem_tumor1_size_cm, NULL::INT AS max_tumor_ordinal,
                          NULL::BIGINT AS n_tem_rows WHERE FALSE) te ON FALSE
        """

    tp_join = ""
    if tp_tbl:
        tp_join = f"""
        LEFT JOIN (
          SELECT CAST(research_id AS BIGINT) AS research_id,
                 TRY_CAST(histology_1_largest_tumor_cm AS DOUBLE) AS tp_largest_cm
          FROM {tp_tbl}
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY histology_1_largest_tumor_cm DESC NULLS LAST
          ) = 1
        ) tp ON tp.research_id = p.research_id
        """
    else:
        tp_join = """
        LEFT JOIN (SELECT NULL::BIGINT AS research_id, NULL::DOUBLE AS tp_largest_cm WHERE FALSE) tp ON FALSE
        """

    agg_join = ""
    if agg_tbl:
        agg_join = f"""
        LEFT JOIN {agg_tbl} mta ON mta.research_id = p.research_id
        """
    else:
        agg_join = "LEFT JOIN (SELECT NULL::BIGINT AS research_id, NULL::INT AS n_tumors WHERE FALSE) mta ON FALSE"

    disc_sql = f"""
CREATE OR REPLACE TEMP VIEW _mt_disc AS
SELECT
  p.*,
  lon.n_long_rows,
  te.n_tem_rows,
  te.max_tumor_ordinal,
  te.tem_tumor1_size_cm,
  tp.tp_largest_cm,
  mta.n_tumors AS aggregate_n_tumors_histology,
  CASE WHEN lon.n_long_rows IS NOT NULL AND lon.n_long_rows <> p.n_slots_any
       THEN TRUE ELSE FALSE END AS long_count_mismatch,
  CASE WHEN p.n_slots_any >= 2 AND COALESCE(lon.n_long_rows, 0) < p.n_slots_any
            AND lon.n_long_rows IS NOT NULL
       THEN TRUE ELSE FALSE END AS long_undercount_vs_slots,
  CASE WHEN COALESCE(te.max_tumor_ordinal, 1) <= 1 AND p.n_slots_any >= 2
       THEN TRUE ELSE FALSE END AS canonical_episode_single_ordinal_only,
  CASE WHEN p.max_size_cm_slots > COALESCE(p.tumor_1_size_cm_clean, 0) + 0.001
       THEN TRUE ELSE FALSE END AS max_size_exceeds_tumor1_slot,
  CASE WHEN COALESCE(tp.tp_largest_cm, -1) >= 0
            AND p.max_size_cm_slots > COALESCE(tp.tp_largest_cm, 0) + 0.05
       THEN TRUE ELSE FALSE END AS max_slot_size_gt_tumor_pathology_largest,
  CASE WHEN p.rn_latest_specimen = 1
            AND mta.n_tumors IS NOT NULL
            AND mta.n_tumors <> p.n_slots_histology_only
       THEN TRUE ELSE FALSE END AS aggregate_histology_mismatch_vs_ps_row
FROM _mt_ps p
{long_join}
{tem_join}
{tp_join}
{agg_join}
"""
    con.execute(disc_sql)
    sql_parts.append(disc_sql)

    dist = con.execute(
        """
        SELECT n_slots_any AS n_populated_tumor_slots, COUNT(*) AS n_pathology_records
        FROM _mt_ps
        GROUP BY 1 ORDER BY 1
        """
    ).fetchdf()

    dist_h = con.execute(
        """
        SELECT n_slots_histology_only AS n_histology_slots, COUNT(*) AS n_pathology_records
        FROM _mt_ps
        GROUP BY 1 ORDER BY 1
        """
    ).fetchdf()

    nrow_long = 0
    if long_tbl:
        nrow_long = con.execute(f"SELECT COUNT(*) FROM {long_tbl}").fetchone()[0]

    n_multi_flag = con.execute(
        "SELECT COUNT(*) FROM _mt_ps WHERE ps_multifocal_text_flag IS TRUE"
    ).fetchone()[0]

    n_ge2_slots = con.execute(
        "SELECT COUNT(*) FROM _mt_ps WHERE n_slots_any >= 2"
    ).fetchone()[0]

    n_ge2_hist = con.execute(
        "SELECT COUNT(*) FROM _mt_ps WHERE n_slots_histology_only >= 2"
    ).fetchone()[0]

    n_ps = con.execute("SELECT COUNT(*) FROM _mt_ps").fetchone()[0]

    les_n = None
    if les_tbl:
        les_n = con.execute(
            f"""
            SELECT
              (SELECT COUNT(*) FROM {les_tbl}) AS n_lesion_rows,
              (SELECT COUNT(*) FROM (
                 SELECT DISTINCT research_id, surgery_episode_id FROM {les_tbl}
              ) u) AS n_episode_keys
            """
        ).fetchdf().to_dict("records")

    summary_rows = [
        {"metric": "connection_source", "value": source_label},
        {"metric": "token_mode", "value": token_mode() if not args.local else "local"},
        {"metric": "path_synoptics_rows", "value": str(n_ps)},
        {"metric": "specimens_n_slots_ge_2_any_field", "value": str(n_ge2_slots)},
        {"metric": "specimens_n_histology_slots_ge_2", "value": str(n_ge2_hist)},
        {"metric": "specimens_multifocal_text_flag", "value": str(n_multi_flag)},
        {"metric": "synoptic_tumor_long_table", "value": long_tbl or "(absent)"},
        {"metric": "synoptic_tumor_long_rowcount", "value": str(nrow_long)},
        {"metric": "tumor_episode_table", "value": tem_tbl or "(absent)"},
        {"metric": "multi_tumor_aggregate_table", "value": agg_tbl or "(absent)"},
        {"metric": "tumor_pathology_table", "value": tp_tbl or "(absent)"},
        {"metric": "lesion_resolved_table", "value": les_tbl or "(absent)"},
        {"metric": "lesion_kpis_json", "value": json.dumps(les_n) if les_n else ""},
    ]
    for _, r in dist.iterrows():
        summary_rows.append(
            {
                "metric": f"dist_n_slots_any_{int(r['n_populated_tumor_slots'])}",
                "value": str(int(r["n_pathology_records"])),
            }
        )

    pd.DataFrame(summary_rows).to_csv(CSV_SUMMARY, index=False)

    if long_tbl:
        disc_full = con.execute(
            """
            SELECT *
            FROM _mt_disc
            WHERE long_count_mismatch
               OR long_undercount_vs_slots
               OR max_size_exceeds_tumor1_slot
               OR max_slot_size_gt_tumor_pathology_largest
               OR aggregate_histology_mismatch_vs_ps_row
               OR (n_slots_any >= 2 AND n_long_rows IS NULL)
            ORDER BY research_id, surg_d
            """
        ).fetchdf()
    else:
        disc_full = con.execute(
            """
            SELECT *
            FROM _mt_disc
            WHERE long_count_mismatch
               OR long_undercount_vs_slots
               OR max_size_exceeds_tumor1_slot
               OR max_slot_size_gt_tumor_pathology_largest
               OR aggregate_histology_mismatch_vs_ps_row
            ORDER BY research_id, surg_d
            """
        ).fetchdf()

    info = con.execute(
        """
        SELECT
          SUM(CASE WHEN canonical_episode_single_ordinal_only THEN 1 ELSE 0 END) AS n_episodes_design_limit,
          SUM(CASE WHEN long_undercount_vs_slots THEN 1 ELSE 0 END) AS n_long_undercount,
          SUM(CASE WHEN max_size_exceeds_tumor1_slot THEN 1 ELSE 0 END) AS n_size_t1_lt_max,
          SUM(CASE WHEN max_slot_size_gt_tumor_pathology_largest THEN 1 ELSE 0 END) AS n_size_gt_tp_largest
        FROM _mt_disc
        """
    ).fetchone()

    disc_full.to_csv(CSV_DISC, index=False)

    with SQL_OUT.open("w") as fh:
        fh.write("\n\n".join(sql_parts))

    completeness = (
        "Multi-tumor completeness is **proven** only if: (1) `synoptic_tumor_long_v1` exists on MotherDuck, "
        "(2) `n_long_rows` equals `n_slots_any` for every pathology row key `(research_id, surg_d)`, and "
        "(3) the table was built from the same `path_synoptics` snapshot as production. "
        "Canonical `tumor_episode_master_v2` remains **single-ordinal-by-design** (tumor 1 spine); absence of "
        "additional ordinals is not a load bug. **`ptc_cohort` / `exports/ptc_full.csv` / proposal2** use "
        "**tumor_1** ETE and pathology-linked largest size; secondary-foci ETE or larger focus in slots 2–5 can "
        "differ from `tumor_1_*` — see `max_size_exceeds_tumor1_slot` and discrepancy export."
    )
    if not long_tbl:
        completeness += (
            "\n\n**This run:** long-table object absent on connection — long-vs-slot reconciliation could not be executed. "
            "Run `scripts/108_synoptic_tumor_long_v1.py --md` then re-run this audit."
        )

    if args.local:
        completeness += (
            "\n\n**This run used `--local`** (parquet + synthetic tumor_episode/aggregate). "
            "Re-run without `--local` against MotherDuck for production lineage."
        )

    md = f"""# MotherDuck multi-tumor pathology audit

**Generated:** {datetime.now(timezone.utc).isoformat()}
**Connection:** {source_label}
**Token mode:** {token_mode() if not args.local else "n/a (local)"}

## Lineage (canonical)

| Layer | Object | Role |
|-------|--------|------|
| Source-derived wide | `path_synoptics` | One row per pathology/synoptic specimen (slots 1–5 wide) |
| Long foci | `synoptic_tumor_long_v1` (or `md_*`) | One row per slot with any field nonempty (script 108, SLOT_MAP) |
| Episode spine | `tumor_episode_master_v2` | **Only** `tumor_ordinal = 1` per surgery (script 22) |
| Patient rollup | `extracted_multi_tumor_aggregate_v1` | Latest PS row per patient; `n_tumors` = histology-nonempty slots; worst ETE/margin/size |
| Lesion export | `lesion_analysis_resolved_v1` | Derived from `tumor_episode_master_v2` → inherits single ordinals |
| Proposal 2 | `ptc_cohort` → `exports/ptc_full.csv` | `tumor_1_extrathyroidal_ext`, `largest_tumor_cm` — **tumor_1 / tumor_pathology** centric |

## Resolved dependencies

- `synoptic_tumor_long_v1`: **{long_tbl or "MISSING"}**
- `tumor_episode_master_v2`: **{tem_tbl or "MISSING"}**
- `extracted_multi_tumor_aggregate_v1`: **{agg_tbl or "MISSING"}**
- `tumor_pathology`: **{tp_tbl or "MISSING"}**
- `lesion_analysis_resolved_v1`: **{les_tbl or "MISSING"}**

## Key counts

- Pathology rows: **{n_ps:,}**
- Specimens with ≥2 nonempty slots (any SLOT_MAP field): **{n_ge2_slots:,}**
- Specimens with ≥2 histology slots: **{n_ge2_hist:,}**
- `tumor_1_multiple_tumor` text multifocal flag: **{n_multi_flag:,}**
- Long-table rows: **{nrow_long:,}** (table ` {long_tbl or "n/a"} `)

### Distribution `n_slots_any` (nonempty OR across SLOT_MAP columns)

{dist.to_markdown(index=False)}

### Distribution `n_slots_histology_only`

{dist_h.to_markdown(index=False)}

## Discrepancy flags (from `_mt_disc`)

- Rows with canonical-episode design limit (≥2 slots but TE max ordinal ≤1): **{info[0] or 0:,}** (expected — not a drop)
- Long undercount vs slots: **{info[1] or 0:,}**
- Max size across slots > tumor_1 slot size: **{info[2] or 0:,}**
- Max slot size > `tumor_pathology.histology_1_largest_tumor_cm`: **{info[3] or 0:,}**

Exported **{len(disc_full):,}** high-signal discrepancy rows to `motherduck_multi_tumor_discrepant_cases.csv`.

## Completeness verdict

{completeness}

## Proposal2 (ETE staging) impact

Variables in `proposal2_ete_analysis.py` come from **`ptc_full.csv`**: `tumor_1_extrathyroidal_ext`, `largest_tumor_cm`, staging from `tumor_pathology`. They do **not** automatically incorporate worst-of-all-foci from slots 2–5. When `max_size_exceeds_tumor1_slot` or secondary-site ETE is present, **reported ETE/size can be incomplete relative to full synoptic multi-tumor data** unless augmented from `extracted_multi_tumor_aggregate_v1` or `synoptic_tumor_long_v1`.

## SQL artifacts

- Skeleton: `sql/motherduck_multi_tumor_audit.sql`
- Generated run: `sql/motherduck_multi_tumor_audit_generated.sql`
"""

    REPORT.write_text(md)
    print(f"Wrote {REPORT}")
    print(f"Wrote {CSV_SUMMARY} ({len(summary_rows)} rows)")
    print(f"Wrote {CSV_DISC} ({len(disc_full)} rows)")
    print(f"Wrote {SQL_OUT}")
    con.close()


if __name__ == "__main__":
    main()
