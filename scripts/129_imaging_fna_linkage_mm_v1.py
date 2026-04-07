#!/usr/bin/env python3
"""
129_imaging_fna_linkage_mm_v1.py — Imaging ↔ FNA linkage (multimodal v1)

Canonical imaging nodule rows: imaging_nodule_master_v1 (see README / AGENTS).

Builds:
  imaging_fna_linkage_mm_v1
  val_imaging_fna_linkage_audit_v1
  review_queue_imaging_fna_mm_v1

Root cause (legacy imaging_fna_linkage_v3 empty / incomplete):
  - Script 23 imaging_fna_linkage_v2 reads only imaging_nodule_long_v2; an empty
    or undated long table yields no rows.
  - Script 49 UNION logic can under-use v1 when v2 partially overlaps; it also
    referenced fna_episode_master_v2.nodule_size_cm before that column existed in
    the canonical CREATE from fna_history.
  - Hardening (78) nulls FNA sizes but still requires dated imaging + valid FNA dates.

Run:
  .venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py
  .venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py --md

MotherDuck (--md): token via motherduck_client.get_token (env or .streamlit/secrets.toml).
The script changes cwd to the repo root before connecting. connect_md_or_file uses
fail_closed=True. Writes motherduck/exports/imaging_fna_linkage_mm_v1_audit.json.
If fna_episode_master_v2 is missing in the cloud catalog, status=blocked in that JSON.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
MOTHERDUCK_EXPORT_DIR = ROOT / "motherduck" / "exports"
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))

from utils.imaging_fna_linkage_mm_v1 import normalize_specimen_key_sql  # noqa: E402


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}\n")


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False


def pragma_cols(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    try:
        rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
        return {str(r[1]).lower(): str(r[1]) for r in rows}
    except Exception:
        return {}


def pick_first(cols: dict[str, str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    return None


_IFNA_OUTPUT_TABLES: tuple[str, ...] = (
    "imaging_fna_linkage_mm_v1",
    "review_queue_imaging_fna_mm_v1",
    "val_imaging_fna_linkage_audit_v1",
)


def qualify_ifna_output_sql(sql: str, schema: str | None) -> str:
    """Prefix script-129 output tables with *schema* so writes stay off main during automation."""
    if not schema or not str(schema).strip():
        return sql
    sch = str(schema).strip()
    out = sql
    for base in _IFNA_OUTPUT_TABLES:
        out = re.sub(rf"(?<!\.)\b{re.escape(base)}\b", f"{sch}.{base}", out)
    return out


def build_temp_wide_sql(con: duckdb.DuckDBPyConnection) -> str:
    img_cols = pragma_cols(con, "imaging_nodule_master_v1")
    if not img_cols:
        raise RuntimeError("imaging_nodule_master_v1 is required")

    acc_col = pick_first(
        img_cols,
        [
            "accession_norm",
            "accession_number",
            "accession",
            "specimen_id",
            "specimen_received",
            "accession_no",
        ],
    )
    img_key_sql = (
        normalize_specimen_key_sql(f"i.{acc_col}") if acc_col else "NULL::VARCHAR"
    )

    has_fh = table_exists(con, "fna_history")
    fh_cols = pragma_cols(con, "fna_history") if has_fh else {}
    fh_idx = pick_first(fh_cols, ["fna_index", "fna_idx", "fna_number"])
    specimen_col = pick_first(fh_cols, ["specimen_received", "specimen", "accession", "accession_number"])

    fna_size_inner = "NULL::DOUBLE"
    if has_fh:
        sz = pick_first(
            fh_cols,
            ["nodule_size_cm", "fna_nodule_size_cm", "nodule_size", "size_cm", "largest_nodule_cm"],
        )
        if sz:
            fna_size_inner = f"TRY_CAST(h.{sz} AS DOUBLE)"

    fh_join = ""
    fna_spec_expr = "NULL::VARCHAR"
    fna_size_expr = "NULL::DOUBLE"
    if has_fh and fh_idx:
        fh_join = f"""
        LEFT JOIN fna_history h
            ON CAST(f.research_id AS BIGINT) = CAST(h.research_id AS BIGINT)
           AND CAST(f.fna_episode_id AS BIGINT) = CAST(h.{fh_idx} AS BIGINT)
        """
        fna_size_expr = fna_size_inner
        if specimen_col:
            fna_spec_expr = normalize_specimen_key_sql(f"h.{specimen_col}")

    has_tumor = table_exists(con, "tumor_episode_master_v2")
    surg_sql = ""
    surg_join = "LEFT JOIN surg sg ON CAST(i.research_id AS BIGINT) = CAST(sg.research_id AS BIGINT)"
    preop_filter = """(
        sg.first_surgery_date IS NULL
        OR (i.exam_date <= sg.first_surgery_date AND fe.fna_date <= sg.first_surgery_date)
    )"""
    first_surg_col = "sg.first_surgery_date"
    if has_tumor:
        surg_sql = """
,
surg AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        MIN(TRY_CAST(surgery_date AS DATE)) AS first_surgery_date
    FROM tumor_episode_master_v2
    WHERE research_id IS NOT NULL
    GROUP BY 1
)
"""
    else:
        surg_join = ""
        preop_filter = "TRUE"
        first_surg_col = "NULL::DATE"

    return f"""
CREATE OR REPLACE TEMP TABLE tt_ifna_mm_wide_pre_v1 AS
WITH
img AS (
    SELECT
        CAST(i.research_id AS INTEGER) AS research_id,
        CAST(i.nodule_id AS VARCHAR) AS nodule_id,
        CAST(i.exam_id AS VARCHAR) AS imaging_exam_id,
        CAST(i.exam_date AS DATE) AS exam_date,
        i.laterality AS img_laterality,
        TRY_CAST(i.max_dimension_cm AS DOUBLE) AS img_size_cm,
        {img_key_sql} AS img_specimen_key
    FROM imaging_nodule_master_v1 i
    WHERE i.exam_date IS NOT NULL
)
{surg_sql}
,
fna_enriched AS (
    SELECT
        CAST(f.research_id AS INTEGER) AS research_id,
        CAST(f.fna_episode_id AS INTEGER) AS fna_episode_id,
        f.fna_date_native AS fna_date,
        f.laterality AS fna_laterality,
        f.specimen_site_raw,
        {fna_size_expr} AS fna_size_cm,
        {fna_spec_expr} AS fna_specimen_key
    FROM fna_episode_master_v2 f
    {fh_join}
    WHERE f.fna_date_native IS NOT NULL
)
SELECT
    i.research_id,
    i.nodule_id,
    i.imaging_exam_id,
    i.exam_date,
    i.img_laterality,
    i.img_size_cm,
    i.img_specimen_key,
    fe.fna_episode_id,
    fe.fna_date,
    fe.fna_laterality,
    fe.fna_size_cm,
    fe.fna_specimen_key,
    fe.specimen_site_raw,
    {first_surg_col} AS first_surgery_date,
    DATEDIFF('day', i.exam_date, fe.fna_date) AS day_gap_us_before_fna,
    CASE
        WHEN i.img_specimen_key IS NOT NULL
             AND fe.fna_specimen_key IS NOT NULL
             AND i.img_specimen_key = fe.fna_specimen_key
        THEN TRUE
        ELSE FALSE
    END AS specimen_match_raw,
    CASE
        WHEN i.img_laterality IS NULL OR fe.fna_laterality IS NULL THEN TRUE
        WHEN LOWER(CAST(i.img_laterality AS VARCHAR))
             = LOWER(CAST(fe.fna_laterality AS VARCHAR)) THEN TRUE
        ELSE FALSE
    END AS side_ok,
    CASE
        WHEN i.img_size_cm IS NULL OR fe.fna_size_cm IS NULL THEN TRUE
        WHEN ABS(i.img_size_cm - fe.fna_size_cm)
               / GREATEST(i.img_size_cm, fe.fna_size_cm, 1e-9) <= 0.20
        THEN TRUE
        ELSE FALSE
    END AS size_ok
FROM img i
INNER JOIN fna_enriched fe
    ON CAST(i.research_id AS BIGINT) = CAST(fe.research_id AS BIGINT)
{surg_join}
WHERE
    {preop_filter}
    AND NOT (
        i.img_specimen_key IS NOT NULL
        AND fe.fna_specimen_key IS NOT NULL
        AND i.img_specimen_key <> fe.fna_specimen_key
    )
    AND (
        (i.img_specimen_key IS NOT NULL
            AND fe.fna_specimen_key IS NOT NULL
            AND i.img_specimen_key = fe.fna_specimen_key)
        OR (
            fe.fna_date >= i.exam_date
            AND DATEDIFF('day', i.exam_date, fe.fna_date) BETWEEN 0 AND 90
        )
    )
"""


LINK_TABLE_SQL = """
CREATE OR REPLACE TABLE imaging_fna_linkage_mm_v1 AS
WITH
eligible AS (
    SELECT *
    FROM tt_ifna_mm_wide_pre_v1 wp
    WHERE wp.side_ok
      AND (wp.specimen_match_raw OR wp.size_ok)
)
,
tagged AS (
    SELECT
        research_id,
        nodule_id,
        imaging_exam_id,
        exam_date,
        fna_episode_id,
        fna_date,
        img_laterality,
        fna_laterality,
        img_size_cm,
        fna_size_cm,
        img_specimen_key,
        fna_specimen_key,
        specimen_site_raw,
        first_surgery_date,
        day_gap_us_before_fna,
        specimen_match_raw AS specimen_match_flag,
        CASE specimen_match_raw
            WHEN TRUE THEN 'specimen_key'::VARCHAR
            ELSE 'temporal_us_90d_pre_fna'::VARCHAR
        END AS match_path,
        CASE
            WHEN img_size_cm IS NOT NULL AND fna_size_cm IS NOT NULL
            THEN ROUND(
                ABS(img_size_cm - fna_size_cm)
                / GREATEST(img_size_cm, fna_size_cm, 1e-9),
                4)
            ELSE NULL
        END AS size_drift_ratio
    FROM eligible
)
,
ord AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, nodule_id
            ORDER BY fna_date ASC, fna_episode_id ASC
        ) AS ordinal_in_nodule,
        COUNT(*) OVER (PARTITION BY research_id, nodule_id) AS n_candidates_for_nodule,
        SUM(CASE WHEN specimen_match_flag THEN 1 ELSE 0 END)
            OVER (PARTITION BY research_id, nodule_id) AS n_specimen_matches_on_nodule
    FROM tagged
)
,
prim AS (
    SELECT
        *,
        CASE
            WHEN n_specimen_matches_on_nodule >= 2 THEN FALSE
            WHEN specimen_match_flag AND n_specimen_matches_on_nodule = 1 THEN TRUE
            WHEN n_candidates_for_nodule = 1 THEN TRUE
            ELSE FALSE
        END AS is_primary_link
    FROM ord
)
SELECT
    lower(md5(concat(
        CAST(research_id AS VARCHAR), '|',
        CAST(nodule_id AS VARCHAR), '|',
        CAST(imaging_exam_id AS VARCHAR), '|',
        CAST(fna_episode_id AS VARCHAR)
    ))) AS link_mm_id,
    research_id,
    nodule_id,
    imaging_exam_id,
    exam_date AS imaging_exam_date,
    fna_episode_id,
    fna_date AS fna_event_date,
    match_path,
    specimen_match_flag,
    day_gap_us_before_fna,
    img_laterality,
    fna_laterality,
    img_size_cm,
    fna_size_cm,
    size_drift_ratio,
    ordinal_in_nodule,
    is_primary_link,
    n_candidates_for_nodule,
    n_specimen_matches_on_nodule,
    specimen_site_raw,
    first_surgery_date,
    CURRENT_TIMESTAMP AS built_at
FROM prim
"""

REVIEW_SQL = """
CREATE OR REPLACE TABLE review_queue_imaging_fna_mm_v1 AS
SELECT * FROM (
    SELECT
        w.research_id,
        w.nodule_id,
        CAST(w.imaging_exam_id AS VARCHAR) AS imaging_exam_id,
        CAST(w.exam_date AS DATE) AS imaging_exam_date,
        CAST(w.fna_episode_id AS INTEGER) AS fna_episode_id,
        CAST(w.fna_date AS DATE) AS fna_event_date,
        'ambiguous_multimatch'::VARCHAR AS review_reason,
        CONCAT(
            'n_link_rows=', CAST((
                SELECT COUNT(*) FROM imaging_fna_linkage_mm_v1 x
                WHERE x.research_id = w.research_id AND x.nodule_id = w.nodule_id
            ) AS VARCHAR)
        ) AS detail,
        CURRENT_TIMESTAMP AS queued_at
    FROM tt_ifna_mm_wide_pre_v1 w
    INNER JOIN (
        SELECT research_id, nodule_id
        FROM imaging_fna_linkage_mm_v1
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
           AND SUM(CASE WHEN is_primary_link THEN 1 ELSE 0 END) = 0
    ) q
        ON w.research_id = q.research_id
       AND w.nodule_id = q.nodule_id
    WHERE w.side_ok
      AND (w.specimen_match_raw OR w.size_ok)

    UNION ALL

    SELECT
        research_id,
        nodule_id,
        CAST(imaging_exam_id AS VARCHAR),
        CAST(exam_date AS DATE),
        CAST(fna_episode_id AS INTEGER),
        CAST(fna_date AS DATE),
        'discordant_laterality'::VARCHAR,
        CONCAT('gap=', CAST(day_gap_us_before_fna AS VARCHAR)),
        CURRENT_TIMESTAMP
    FROM tt_ifna_mm_wide_pre_v1
    WHERE NOT side_ok

    UNION ALL

    SELECT
        research_id,
        nodule_id,
        CAST(imaging_exam_id AS VARCHAR),
        CAST(exam_date AS DATE),
        CAST(fna_episode_id AS INTEGER),
        CAST(fna_date AS DATE),
        'size_drift_gt_20pct'::VARCHAR,
        CONCAT('gap=', CAST(day_gap_us_before_fna AS VARCHAR)),
        CURRENT_TIMESTAMP
    FROM tt_ifna_mm_wide_pre_v1
    WHERE side_ok
      AND NOT size_ok
      AND NOT specimen_match_raw
) z
"""

AUDIT_SQL = """
CREATE OR REPLACE TABLE val_imaging_fna_linkage_audit_v1 AS
WITH
img_n AS (
    SELECT research_id, nodule_id
    FROM imaging_nodule_master_v1
    WHERE exam_date IS NOT NULL
)
SELECT
    (SELECT COUNT(*) FROM img_n) AS imaging_nodule_rows_with_exam_date,
    (SELECT COUNT(DISTINCT concat(
        CAST(research_id AS VARCHAR), '|', CAST(nodule_id AS VARCHAR))) FROM img_n)
        AS distinct_imaging_nodules,
    (SELECT COUNT(*) FROM imaging_fna_linkage_mm_v1) AS linkage_mm_rows,
    (SELECT COUNT(*) FROM imaging_fna_linkage_mm_v1 WHERE is_primary_link) AS primary_link_rows,
    (SELECT COUNT(DISTINCT concat(
        CAST(research_id AS VARCHAR), '|', CAST(nodule_id AS VARCHAR)))
        FROM imaging_fna_linkage_mm_v1 WHERE is_primary_link) AS nodules_with_primary,
    (SELECT COUNT(*) FROM imaging_fna_linkage_mm_v1 WHERE n_candidates_for_nodule > 1)
        AS rows_in_ambiguous_nodules,
    (SELECT COUNT(*) FROM imaging_fna_linkage_mm_v1 WHERE specimen_match_flag)
        AS specimen_key_rows,
    (SELECT COUNT(*) FROM review_queue_imaging_fna_mm_v1) AS review_queue_rows,
    CURRENT_TIMESTAMP AS built_at
"""


def _fq_table(name: str, output_schema: str | None) -> str:
    if not output_schema or not str(output_schema).strip():
        return name
    return f"{str(output_schema).strip()}.{name}"


def run(
    con: duckdb.DuckDBPyConnection,
    *,
    dry_run: bool,
    motherduck: bool = False,
    output_schema: str | None = None,
) -> dict:
    has_fna = table_exists(con, "fna_episode_master_v2")
    has_img = table_exists(con, "imaging_nodule_master_v1")
    if not has_fna:
        reason = (
            "fna_episode_master_v2 not found in this database. "
            "Materialize script 22 into MotherDuck main (or use a local file DB) before linkage."
        )
        if motherduck:
            section("MotherDuck precheck — blocked")
            print(f"  {reason}")
            n_img = 0
            if has_img:
                n_img = con.execute(
                    "SELECT COUNT(*) FROM imaging_nodule_master_v1 WHERE exam_date IS NOT NULL"
                ).fetchone()[0]
            print(f"  imaging_nodule_master_v1 (dated rows): {n_img:,}")
            v3 = None
            if table_exists(con, "imaging_fna_linkage_v3"):
                v3 = con.execute("SELECT COUNT(*) FROM imaging_fna_linkage_v3").fetchone()[0]
            return {
                "status": "blocked_missing_fna_episode_master_v2",
                "reason": reason,
                "before": {
                    "imaging_nodule_master_dated_rows": n_img,
                    "fna_episode_dated_rows": None,
                    "imaging_fna_linkage_v3_rows": v3,
                    "imaging_fna_linkage_mm_v1_rows": None,
                },
                "after": None,
                "audit": None,
            }
        raise RuntimeError("fna_episode_master_v2 is required (run script 22)")

    out: dict = {"status": "ok"}

    section("Counts before")
    n_img = con.execute(
        "SELECT COUNT(*) FROM imaging_nodule_master_v1 WHERE exam_date IS NOT NULL"
    ).fetchone()[0]
    n_fna = con.execute(
        "SELECT COUNT(*) FROM fna_episode_master_v2 WHERE fna_date_native IS NOT NULL"
    ).fetchone()[0]
    v3 = None
    if table_exists(con, "imaging_fna_linkage_v3"):
        v3 = con.execute("SELECT COUNT(*) FROM imaging_fna_linkage_v3").fetchone()[0]
    link_tbl = _fq_table("imaging_fna_linkage_mm_v1", output_schema)
    mm_before = None
    if table_exists(con, link_tbl):
        mm_before = con.execute(f"SELECT COUNT(*) FROM {link_tbl}").fetchone()[0]
    print(f"  imaging_nodule_master_v1 (dated): {n_img:,}")
    print(f"  fna_episode_master_v2 (dated):    {n_fna:,}")
    print(f"  imaging_fna_linkage_v3 rows:       {v3 if v3 is not None else 'N/A (missing)'}")
    print(f"  imaging_fna_linkage_mm_v1 (prior): {mm_before if mm_before is not None else 'N/A (missing)'}")
    out["before"] = {
        "imaging_nodule_master_dated_rows": n_img,
        "fna_episode_dated_rows": n_fna,
        "imaging_fna_linkage_v3_rows": v3,
        "imaging_fna_linkage_mm_v1_rows": mm_before,
    }

    wide_sql = build_temp_wide_sql(con)
    if dry_run:
        print("\n[DRY-RUN] Wide pre SQL:\n", wide_sql[:500], "...\n")
        return out

    con.execute(wide_sql)
    wide_n = con.execute("SELECT COUNT(*) FROM tt_ifna_mm_wide_pre_v1").fetchone()[0]
    print(f"\n  tt_ifna_mm_wide_pre_v1 candidate pairs: {wide_n:,}")
    out["wide_pre_candidate_pairs"] = wide_n

    link_sql = qualify_ifna_output_sql(LINK_TABLE_SQL, output_schema)
    review_sql = qualify_ifna_output_sql(REVIEW_SQL, output_schema)
    audit_sql = qualify_ifna_output_sql(AUDIT_SQL, output_schema)
    con.execute(link_sql)
    con.execute(review_sql)
    con.execute(audit_sql)

    section("Counts after")
    after_mm = con.execute(f"SELECT COUNT(*) FROM {link_tbl}").fetchone()[0]
    after_primary = con.execute(
        f"SELECT COUNT(*) FROM {link_tbl} WHERE is_primary_link"
    ).fetchone()[0]
    rev_tbl = _fq_table("review_queue_imaging_fna_mm_v1", output_schema)
    after_rev = con.execute(f"SELECT COUNT(*) FROM {rev_tbl}").fetchone()[0]
    for label, val in [
        ("imaging_fna_linkage_mm_v1", after_mm),
        ("  primary links", after_primary),
        ("review_queue_imaging_fna_mm_v1", after_rev),
    ]:
        print(f"  {label}: {val:,}")

    audit_tbl = _fq_table("val_imaging_fna_linkage_audit_v1", output_schema)
    print(f"\n  {audit_tbl}:")
    audit_df = con.execute(f"SELECT * FROM {audit_tbl}").fetchdf()
    print(audit_df.to_string(index=False))
    audit_row = audit_df.iloc[0].to_dict() if len(audit_df) else {}
    # JSON-serialize timestamps
    for k, v in list(audit_row.items()):
        if hasattr(v, "isoformat"):
            audit_row[k] = v.isoformat()
    out["after"] = {
        "imaging_fna_linkage_mm_v1_rows": after_mm,
        "primary_link_rows": after_primary,
        "review_queue_rows": after_rev,
    }
    out["audit"] = audit_row
    if output_schema:
        out["output_schema"] = str(output_schema).strip()
    return out


def _export_motherduck_audit(payload: dict, *, token_mode_label: str) -> Path:
    MOTHERDUCK_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = MOTHERDUCK_EXPORT_DIR / "imaging_fna_linkage_mm_v1_audit.json"
    body = {
        "script": "129_imaging_fna_linkage_mm_v1.py",
        "target": "motherduck",
        "token_mode": token_mode_label,
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    path.write_text(json.dumps(body, indent=2), encoding="utf-8")
    print(f"\n  Wrote aggregate audit export: {path.relative_to(ROOT)}")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--md", action="store_true", help="Use MotherDuck / connect_md_or_file")
    parser.add_argument(
        "--sa",
        action="store_true",
        help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN (CI / release automation)",
    )
    parser.add_argument(
        "--contract-schema",
        metavar="SCHEMA",
        default=None,
        help=(
            "Write imaging–FNA linkage outputs to this schema (e.g. mm_contract_dev). "
            "Avoids creating tables in main during automated dev runs. "
            "If omitted, MM_IFNA_OUTPUT_SCHEMA is used when set. "
            "Default: unqualified (main catalog)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print wide-pre SQL only; do not write tables or export",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="With --md, skip writing motherduck/exports/imaging_fna_linkage_mm_v1_audit.json",
    )
    args = parser.parse_args()
    out_schema = (
        (args.contract_schema or "").strip()
        or os.environ.get("MM_IFNA_OUTPUT_SCHEMA", "").strip()
        or None
    )

    os.chdir(ROOT)
    if args.md:
        from motherduck_client import token_mode as md_token_mode
        from utils.md_connect import connect_md_or_file

        print(f"  MotherDuck token source: {md_token_mode()}")
        con = connect_md_or_file(
            DB_PATH,
            md=True,
            fail_closed=True,
            prefer_service_account=args.sa,
        )
    else:
        con = duckdb.connect(str(DB_PATH))

    try:
        result = run(
            con,
            dry_run=args.dry_run,
            motherduck=args.md,
            output_schema=out_schema,
        )
        if args.md and not args.dry_run and not args.no_export:
            from motherduck_client import token_mode as md_token_mode

            _export_motherduck_audit(result, token_mode_label=md_token_mode())
    finally:
        con.close()


if __name__ == "__main__":
    main()
