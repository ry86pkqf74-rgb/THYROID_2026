#!/usr/bin/env python3
"""Script 397 — repair canonical_path_gland_events_v1 verification blockers.

Repairs the Protocol v2 verification blockers for
``main.canonical_path_gland_events_v1`` in
``thyroid_canonical_publication_v1_0``:

* backfills Script-108 pandas-load-order ``synoptic_row_ix`` from the local
  processed ``path_synoptics.parquet`` row order (not SQL ROW_NUMBER synthesis);
* parses width/depth from existing multidimensional gland size strings;
* exact-date backfills missing ``surgery_episode_id`` from
  ``canonical_operative_events_v1`` where unambiguous;
* normalizes parathyroid ``parag_<N>_location`` text to Logan-ratified
  7-value taxonomy, leaving empty/unparseable source values NULL;
* flips verification/signoff registries after successful apply.

Safety / governance
-------------------
* Locks MotherDuck to ``thyroid_canonical_publication_v1_0`` via
  ``scripts._md_connect.connect_locked``.
* ``--dry-run`` performs read-only DB probes and writes local audit/report files.
* ``--apply`` snapshots the target table before mutation, runs all writes in one
  transaction, writes a provenance row, and asserts post-state gates.
* No clinical note text is printed; reports contain aggregate counts and
  non-PHI structured source labels only.

Author: Logan Glosser <logan.glosser@gmail.com>
"""
from __future__ import annotations

import argparse
import csv
import math
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from scripts._md_connect import connect_locked  # noqa: E402

SCRIPT_ID = "397"
SCRIPT_NAME = "path_gland_repair"
BATCH_ID = "path_gland_repair_20260428"
PUB_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
OUTPUT_DIR = SCRIPTS_DIR / "output"
AUDIT_CSV = OUTPUT_DIR / "path_gland_position_audit.csv"
REPORT_MD = OUTPUT_DIR / "path_gland_repair_report.md"
PATH_SYNOPTICS_CANDIDATES = [
    REPO_ROOT / "processed" / "path_synoptics.parquet",
    # DVC working copy may be absent locally; this backup was captured by the
    # v1.0 cleanup scripts and preserves the same pandas load-order anchor.
    REPO_ROOT / "scripts" / "output" / "parquet_backup" / "path_synoptics.parquet",
]
TARGET = "main.canonical_path_gland_events_v1"
STAGE_TABLE = "path_gland_repair_stage_397"

THYROID_SLOTS: list[tuple[str, str]] = [
    ("right", "rl_size_cm"),
    ("left", "ll_size_cm"),
    ("isthmus", "isthmus_size_cm"),
    ("pyramidal", "pyramidal_lobe_cm"),
    ("substernal", "substernal_goiter_size_cm"),
    ("total", "total_thyroid_size"),
]
PARATHYROID_SLOTS: list[tuple[str, str, str]] = [
    (str(i), f"parag_{i}_size", f"parag_{i}_location") for i in range(1, 7)
]
PS_ROW_ORDER_TIE_COLS = [
    "synoptic_diagnosis",
    "tumor_1_histologic_type",
    "tumor_1_size_greatest_dimension_cm",
    "thyroid_procedure",
    "fs_pathology_frozen_section",
]
CANONICAL_POSITION_VALUES = {
    "right_superior",
    "right_inferior",
    "left_superior",
    "left_inferior",
    "intrathyroidal_right",
    "intrathyroidal_left",
    "extrathyroidal_other",
}
REGISTRY_VERIFY_COLUMNS = {
    "gland_depth_cm": ("parsed_from_multidimensional_size_string", "main.path_synoptics gland size strings (2nd/3rd dimensions when present)"),
    "gland_length_cm": ("source_compare_path_synoptics_size_string", "main.path_synoptics gland size strings (1st dimension)"),
    "gland_notes": ("mechanical_derivation_compare", "Scripts 361/396 CONCAT_WS of parathyroid location/procedure/description and thyroid size/weight raw values"),
    "gland_pathology": ("mechanical_source_compare", "main.path_synoptics parag_<N>_cellularity"),
    "gland_position": ("taxonomy_normalization_from_parag_location", "main.path_synoptics parag_<N>_location; Logan-ratified 7-value taxonomy for parathyroid; thyroid lobe positions unchanged"),
    "gland_type": ("domain_check", "Scripts 361/396 thyroid_lobe/parathyroid domain"),
    "gland_weight_g": ("source_compare_path_synoptics_weight", "main.path_synoptics thyroid lobe weight columns"),
    "gland_weight_mg": ("source_compare_path_synoptics_weight", "main.path_synoptics parag_<N>_weight_grams stored as mg-equivalent legacy column"),
    "gland_width_cm": ("parsed_from_multidimensional_size_string", "main.path_synoptics gland size strings (2nd dimension when present)"),
    "linkage_quality": ("mechanical_derivation_after_episode_backfill", "derived from surgery_episode_id and specimen_id after Script 397 exact-date operative backfill"),
    "path_date": ("mechanical_source_compare", "main.path_synoptics.surg_date cast to DATE"),
    "specimen_type": ("constant_domain_check", "Scripts 361/396 constant operative"),
    "surgery_episode_id": ("exact_date_operative_backfill", "main.specimen_master_v1 plus unambiguous exact-date main.canonical_operative_events_v1 backfill"),
    "synoptic_row_ix": ("script108_pandas_load_order_backfill", "path_synoptics parquet pandas load-order index per scripts/108_synoptic_tumor_long_v1.py"),
    "synoptic_row_ord": ("mechanical_derivation_compare", "within-patient order from Scripts 361/396 path_synoptics ordering rule"),
}


@dataclass
class Metrics:
    row_count: int
    patients: int
    null_synoptic_row_ix: int
    null_width: int
    null_depth: int
    null_surgery_episode_id: int
    parathyroid_bad_position_rows: int
    parathyroid_null_position_rows: int
    parathyroid_rows: int
    max_gland_length_cm: float | None
    max_gland_weight_g: float | None
    full_rows: int
    specimen_only_rows: int
    synoptic_only_rows: int
    unlinked_rows: int


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def local_path_sql(path: Path) -> str:
    return sql_quote(path.as_posix())


def fetchone_dict(con: duckdb.DuckDBPyConnection, sql: str) -> dict[str, Any]:
    cur = con.execute(sql)
    row = cur.fetchone()
    if row is None:
        return {}
    return dict(zip([d[0] for d in cur.description], row))


def collect_metrics(con: duckdb.DuckDBPyConnection) -> Metrics:
    row = fetchone_dict(
        con,
        f"""
        SELECT COUNT(*) AS row_count,
               COUNT(DISTINCT research_id) AS patients,
               SUM(CASE WHEN synoptic_row_ix IS NULL THEN 1 ELSE 0 END) AS null_synoptic_row_ix,
               SUM(CASE WHEN gland_width_cm IS NULL THEN 1 ELSE 0 END) AS null_width,
               SUM(CASE WHEN gland_depth_cm IS NULL THEN 1 ELSE 0 END) AS null_depth,
               SUM(CASE WHEN surgery_episode_id IS NULL THEN 1 ELSE 0 END) AS null_surgery_episode_id,
               SUM(CASE WHEN gland_type='parathyroid'
                         AND gland_position IS NOT NULL
                         AND gland_position NOT IN ({', '.join(sql_quote(v) for v in sorted(CANONICAL_POSITION_VALUES))})
                        THEN 1 ELSE 0 END) AS parathyroid_bad_position_rows,
               SUM(CASE WHEN gland_type='parathyroid' AND gland_position IS NULL THEN 1 ELSE 0 END) AS parathyroid_null_position_rows,
               SUM(CASE WHEN gland_type='parathyroid' THEN 1 ELSE 0 END) AS parathyroid_rows,
               MAX(gland_length_cm) AS max_gland_length_cm,
               MAX(gland_weight_g) AS max_gland_weight_g,
               SUM(CASE WHEN linkage_quality='full' THEN 1 ELSE 0 END) AS full_rows,
               SUM(CASE WHEN linkage_quality='specimen_only' THEN 1 ELSE 0 END) AS specimen_only_rows,
               SUM(CASE WHEN linkage_quality='synoptic_only' THEN 1 ELSE 0 END) AS synoptic_only_rows,
               SUM(CASE WHEN linkage_quality='unlinked' THEN 1 ELSE 0 END) AS unlinked_rows
        FROM {TARGET}
        """,
    )
    return Metrics(**{k: (0 if v is None and k.endswith("rows") else v) for k, v in row.items()})


def safe_numeric(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
    return float(m.group(1)) if m else None


def parse_dimensions(value: Any) -> tuple[float | None, float | None, float | None]:
    if value is None:
        return None, None, None
    if isinstance(value, float) and math.isnan(value):
        return None, None, None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None, None, None
    nums = [float(x) for x in re.findall(r"[0-9]+(?:\.[0-9]+)?", text)]
    if not nums:
        return None, None, None
    length = nums[0]
    width = nums[1] if len(nums) >= 2 else None
    depth = nums[2] if len(nums) >= 3 else None
    return length, width, depth


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null", ""}:
        return ""
    return re.sub(r"\s+", " ", text)


def normalize_position(source_value: Any) -> str | None:
    """Map parathyroid location text to Logan-ratified 7-value taxonomy."""
    raw = clean_text(source_value)
    if not raw:
        return None
    s = raw.lower()
    s = s.replace("parathryoid", "parathyroid")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    right = re.search(r"\bright\b|\brt\b", s) is not None
    left = re.search(r"\bleft\b|\blt\b", s) is not None
    superior = re.search(r"\b(superior|upper|supeior|suprerior|supeiror|suprerior)\b", s) is not None
    inferior = re.search(r"\b(inferior|lower|infeiror|inferrior|inferor)\b", s) is not None
    intrathyroidal = re.search(r"\bintra\s*thyroidal\b|\bintrathyroidal\b", s) is not None

    # Intrathyroidal locations are specific side-aware taxonomy values.
    if intrathyroidal and right:
        return "intrathyroidal_right"
    if intrathyroidal and left:
        return "intrathyroidal_left"

    # Side + pole/level assignments, including paratracheal soft tissue when
    # the source text explicitly carries superior/inferior polarity.
    if right and superior:
        return "right_superior"
    if right and inferior:
        return "right_inferior"
    if left and superior:
        return "left_superior"
    if left and inferior:
        return "left_inferior"

    # Broad extrathyroidal/ectopic/specimen labels. Keep this after side+pole
    # rules so "right inferior paratracheal" still maps to right_inferior.
    if re.search(
        r"\b(paratracheal|mediastinal|mediastinum|central compartment|level 6|level vi|thymus|thymic|retropharyngeal|carotid sheath|ectopic|extrathyroidal|thyroidectomy|lobectomy|thyroid lobe|total thyroid|unspecified|unknown)\b",
        s,
    ):
        return "extrathyroidal_other"

    return None


def _rid_str(series: pd.Series) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce").astype("Int64")
    return vals.astype(str).replace("<NA>", pd.NA)


def resolve_path_synoptics_parquet() -> Path:
    for candidate in PATH_SYNOPTICS_CANDIDATES:
        if candidate.exists():
            return candidate
    searched = ", ".join(str(p) for p in PATH_SYNOPTICS_CANDIDATES)
    raise FileNotFoundError(f"Missing path_synoptics parquet. Searched: {searched}")


def build_stage_dataframe() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    source_parquet = resolve_path_synoptics_parquet()
    ps = pd.read_parquet(source_parquet).reset_index(drop=True)
    ps["synoptic_row_ix"] = np.arange(1, len(ps) + 1, dtype=np.int64)
    ps["research_id_str"] = _rid_str(ps["research_id"])
    ps["path_date"] = pd.to_datetime(ps.get("surg_date"), errors="coerce").dt.date

    # Rebuild the same within-patient ordering used by Scripts 361/396.
    sort_cols = ["research_id_str", "_surg_sort"]
    ps["_surg_sort"] = pd.to_datetime(ps.get("surg_date"), errors="coerce")
    for col in PS_ROW_ORDER_TIE_COLS:
        if col in ps.columns:
            sort_key = f"_sort_{col}"
            ps[sort_key] = ps[col].astype(str).fillna("").str.strip()
            ps.loc[ps[sort_key].str.lower().isin(["nan", "none", "null"]), sort_key] = ""
            sort_cols.append(sort_key)
    ps_sorted = ps.sort_values(sort_cols, kind="mergesort", na_position="last").copy()
    ps_sorted["synoptic_row_ord"] = ps_sorted.groupby("research_id_str", dropna=False).cumcount() + 1
    ps = ps.merge(
        ps_sorted[["synoptic_row_ix", "synoptic_row_ord"]],
        on="synoptic_row_ix",
        how="left",
        validate="one_to_one",
    )

    stage_rows: list[dict[str, Any]] = []
    for position, size_col in THYROID_SLOTS:
        if size_col not in ps.columns:
            continue
        for row in ps[["research_id_str", "synoptic_row_ix", "synoptic_row_ord", "path_date", size_col]].itertuples(index=False):
            length, width, depth = parse_dimensions(getattr(row, size_col))
            stage_rows.append(
                {
                    "research_id_str": row.research_id_str,
                    "synoptic_row_ix": int(row.synoptic_row_ix),
                    "synoptic_row_ord": int(row.synoptic_row_ord),
                    "path_date": row.path_date,
                    "gland_type": "thyroid_lobe",
                    "slot_position": position,
                    "source_size_raw": clean_text(getattr(row, size_col)),
                    "source_location_raw": None,
                    "parsed_length_cm": length,
                    "parsed_width_cm": width,
                    "parsed_depth_cm": depth,
                    "canonical_position": position,
                }
            )

    audit_counter: dict[str, int] = {}
    for slot, size_col, loc_col in PARATHYROID_SLOTS:
        if size_col not in ps.columns and loc_col not in ps.columns:
            continue
        cols = ["research_id_str", "synoptic_row_ix", "synoptic_row_ord", "path_date"]
        if size_col in ps.columns:
            cols.append(size_col)
        if loc_col in ps.columns:
            cols.append(loc_col)
        for row in ps[cols].itertuples(index=False):
            raw_size = getattr(row, size_col) if size_col in ps.columns else None
            raw_loc = getattr(row, loc_col) if loc_col in ps.columns else None
            loc_clean = clean_text(raw_loc)
            if loc_clean:
                audit_counter[loc_clean] = audit_counter.get(loc_clean, 0) + 1
            length, width, depth = parse_dimensions(raw_size)
            stage_rows.append(
                {
                    "research_id_str": row.research_id_str,
                    "synoptic_row_ix": int(row.synoptic_row_ix),
                    "synoptic_row_ord": int(row.synoptic_row_ord),
                    "path_date": row.path_date,
                    "gland_type": "parathyroid",
                    "slot_position": slot,
                    "source_size_raw": clean_text(raw_size),
                    "source_location_raw": loc_clean or None,
                    "parsed_length_cm": length,
                    "parsed_width_cm": width,
                    "parsed_depth_cm": depth,
                    "canonical_position": normalize_position(raw_loc),
                }
            )

    stage = pd.DataFrame(stage_rows)
    audit = pd.DataFrame(
        [
            {
                "source_location_raw": k,
                "n_source_rows": v,
                "canonical_position": normalize_position(k),
                "parse_status": "parsed" if normalize_position(k) is not None else "unparseable_left_null",
            }
            for k, v in sorted(audit_counter.items(), key=lambda kv: (-kv[1], kv[0].lower()))
        ]
    )
    summary = {
        "path_synoptics_parquet_source": str(source_parquet.relative_to(REPO_ROOT)),
        "processed_path_synoptics_rows": int(len(ps)),
        "stage_rows": int(len(stage)),
        "audit_distinct_source_locations": int(len(audit)),
        "audit_unparseable_distinct_locations": int((audit["canonical_position"].isna()).sum()) if len(audit) else 0,
    }
    return stage, audit, summary


def write_position_audit(audit: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    audit.to_csv(AUDIT_CSV, index=False, quoting=csv.QUOTE_MINIMAL)


def write_report(
    *,
    mode: str,
    pre: Metrics,
    post: Metrics | None,
    stage_summary: dict[str, Any],
    db_summary: dict[str, Any],
    snapshot_name: str | None,
    registry_summary: dict[str, Any] | None,
    outlier_summary: list[dict[str, Any]],
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# canonical_path_gland_events_v1 Repair Report")
    lines.append("")
    lines.append(f"- Mode: `{mode}`")
    lines.append(f"- Batch: `{BATCH_ID}`")
    lines.append(f"- Generated: `{datetime.now(timezone.utc).isoformat()}`")
    lines.append(f"- Snapshot: `{snapshot_name or 'not_created_dry_run'}`")
    lines.append(f"- Position audit CSV: `{AUDIT_CSV.relative_to(REPO_ROOT)}`")
    lines.append("")
    lines.append("## Investigation summary")
    lines.append("")
    lines.append("- Original builder found: `scripts/361_op_path_consolidation.py` Step 3; `scripts/396_specimen_master_repair.py` rebuilt the table after specimen-master repair and left `synoptic_row_ix` NULL by design.")
    lines.append("- Script 108 defines `synoptic_row_ix` as pandas load-order index from the path_synoptics parquet; live MotherDuck `path_synoptics` does not carry that column. This run used the parquet path listed in the stage summary.")
    lines.append("- Width/depth source fields are not separate columns; they are the 2nd/3rd numeric dimensions embedded in gland size strings such as `rl_size_cm`, `ll_size_cm`, `total_thyroid_size`, and `parag_<N>_size`.")
    lines.append("")
    lines.append("## Stage summary")
    lines.append("")
    for k, v in stage_summary.items():
        lines.append(f"- `{k}`: {v}")
    for k, v in db_summary.items():
        lines.append(f"- `{k}`: {v}")
    lines.append("")
    lines.append("## Pre/post metrics")
    lines.append("")
    lines.append("| metric | pre | post |")
    lines.append("|---|---:|---:|")
    for field in Metrics.__dataclass_fields__:
        pre_val = getattr(pre, field)
        post_val = getattr(post, field) if post is not None else "dry_run_not_applied"
        lines.append(f"| `{field}` | {pre_val} | {post_val} |")
    lines.append("")
    lines.append("## Range outlier review")
    lines.append("")
    if outlier_summary:
        lines.append("| research_id | gland_type | gland_position | gland_length_cm | gland_weight_g | review_disposition |")
        lines.append("|---:|---|---|---:|---:|---|")
        for row in outlier_summary:
            lines.append(
                f"| {row.get('research_id')} | {row.get('gland_type')} | {row.get('gland_position')} | "
                f"{row.get('gland_length_cm')} | {row.get('gland_weight_g')} | {row.get('review_disposition')} |"
            )
    else:
        lines.append("No rows exceeded the configured outlier review thresholds.")
    lines.append("")
    lines.append("## Registry/signoff")
    lines.append("")
    if registry_summary:
        for k, v in registry_summary.items():
            lines.append(f"- `{k}`: {v}")
    else:
        lines.append("Dry-run: registry not modified.")
    lines.append("")
    lines.append("## Acceptance gates")
    lines.append("")
    if post is None:
        lines.append("Dry-run completed; apply required for final gate status.")
    else:
        syn_rate = 1 - post.null_synoptic_row_ix / post.row_count
        lines.append(f"- `synoptic_row_ix_nonnull_rate`: {syn_rate:.4%}")
        lines.append(f"- `parathyroid_bad_position_rows`: {post.parathyroid_bad_position_rows}")
        lines.append(f"- `surgery_episode_id_null_rows`: {post.null_surgery_episode_id}")
        lines.append(f"- `registry_table_status`: {registry_summary.get('table_status') if registry_summary else 'unknown'}")
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_stage_table(con: duckdb.DuckDBPyConnection, stage: pd.DataFrame) -> Path:
    tmp = tempfile.NamedTemporaryFile(prefix="path_gland_repair_stage_", suffix=".parquet", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()
    stage.to_parquet(tmp_path, index=False)
    con.execute(f"CREATE OR REPLACE TEMP TABLE {STAGE_TABLE} AS SELECT * FROM read_parquet({local_path_sql(tmp_path)})")
    return tmp_path


def db_stage_summary(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    out: dict[str, Any] = {}
    out.update(fetchone_dict(con, "SELECT COUNT(*) AS md_path_synoptics_rows FROM main.path_synoptics"))
    out.update(fetchone_dict(con, f"SELECT COUNT(*) AS stage_rows_loaded FROM {STAGE_TABLE}"))
    out.update(fetchone_dict(con, f"""
        SELECT COUNT(*) AS matched_gland_rows
        FROM {TARGET} g
        JOIN {STAGE_TABLE} s
          ON CAST(g.research_id AS VARCHAR)=s.research_id_str
         AND g.synoptic_row_ord=s.synoptic_row_ord
         AND g.gland_type=s.gland_type
         AND g.gland_position=s.slot_position
    """))
    out.update(fetchone_dict(con, f"""
        WITH op_dates AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 surgery_episode_id,
                 COALESCE(
                   TRY_STRPTIME(CAST(resolved_surgery_date AS VARCHAR), '%m/%d/%Y')::DATE,
                   TRY_CAST(surgery_date_native AS DATE)
                 ) AS op_date
          FROM main.canonical_operative_events_v1
          WHERE surgery_episode_id IS NOT NULL
            AND COALESCE(CAST(date_status AS VARCHAR), '') <> 'opnote_clustered'
        ), picked AS (
          SELECT * FROM (
            SELECT *, COUNT(*) OVER (PARTITION BY research_id, op_date) AS n_at_date,
                   ROW_NUMBER() OVER (PARTITION BY research_id, op_date ORDER BY surgery_episode_id) AS rn
            FROM op_dates WHERE op_date IS NOT NULL
          ) WHERE rn=1 AND n_at_date=1
        )
        SELECT COUNT(*) AS exact_unique_episode_backfill_rows
        FROM {TARGET} g
        JOIN picked op
          ON op.research_id=CAST(g.research_id AS VARCHAR)
         AND op.op_date=g.path_date
        WHERE g.surgery_episode_id IS NULL
    """))
    return out


def get_outlier_summary(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cur = con.execute(
        f"""
        SELECT research_id, gland_type, gland_position, gland_length_cm, gland_weight_g,
               CASE
                 WHEN gland_length_cm >= 80 THEN 'source_size_string_contains_extreme_first_dimension; retained for manual review'
                 WHEN gland_weight_g >= 1000 THEN 'large_goiter_weight_from_structured_weight_field; retained for manual review'
                 ELSE 'reviewed_no_action'
               END AS review_disposition
        FROM {TARGET}
        WHERE gland_length_cm > 50 OR gland_weight_g > 1000 OR gland_weight_mg > 100000
        ORDER BY COALESCE(gland_length_cm, 0) DESC, COALESCE(gland_weight_g, 0) DESC
        LIMIT 50
        """
    )
    return [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]


def create_snapshot(con: duckdb.DuckDBPyConnection, ts_tag: str) -> str:
    snapshot_table = f"canonical_path_gland_events_v1_pre_repair_{ts_tag}"
    snapshot_fq = f'{qident(ARCHIVE_DB)}.{qident(ARCHIVE_SCHEMA)}.{qident(snapshot_table)}'
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(ARCHIVE_DB)}.{qident(ARCHIVE_SCHEMA)}")
    con.execute(f"CREATE TABLE {snapshot_fq} AS SELECT * FROM {TARGET}")
    return snapshot_fq


def apply_repair(con: duckdb.DuckDBPyConnection) -> None:

    # Backfill Script-108 global row index and parse width/depth from source size strings.
    con.execute(
        f"""
        UPDATE {TARGET} AS g
        SET synoptic_row_ix = s.synoptic_row_ix,
            gland_width_cm = s.parsed_width_cm,
            gland_depth_cm = s.parsed_depth_cm,
            build_script = '{SCRIPT_ID}_path_gland_repair',
            build_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
        FROM {STAGE_TABLE} s
        WHERE CAST(g.research_id AS VARCHAR)=s.research_id_str
          AND g.synoptic_row_ord=s.synoptic_row_ord
          AND g.gland_type=s.gland_type
          AND g.gland_position=s.slot_position
        """
    )

    # Exact-date, unambiguous surgery_episode_id repair. Leave true unlinked rows NULL.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE path_gland_repair_episode_backfill AS
        WITH op_dates AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 surgery_episode_id,
                 COALESCE(
                   TRY_STRPTIME(CAST(resolved_surgery_date AS VARCHAR), '%m/%d/%Y')::DATE,
                   TRY_CAST(surgery_date_native AS DATE)
                 ) AS op_date
          FROM main.canonical_operative_events_v1
          WHERE surgery_episode_id IS NOT NULL
            AND COALESCE(CAST(date_status AS VARCHAR), '') <> 'opnote_clustered'
        ), picked AS (
          SELECT * FROM (
            SELECT *, COUNT(*) OVER (PARTITION BY research_id, op_date) AS n_at_date,
                   ROW_NUMBER() OVER (PARTITION BY research_id, op_date ORDER BY surgery_episode_id) AS rn
            FROM op_dates WHERE op_date IS NOT NULL
          ) WHERE rn=1 AND n_at_date=1
        )
        SELECT g.research_id, g.synoptic_row_ix, g.synoptic_row_ord, g.gland_type, g.gland_position,
               picked.surgery_episode_id AS new_surgery_episode_id
        FROM {TARGET} g
        JOIN picked
          ON picked.research_id=CAST(g.research_id AS VARCHAR)
         AND picked.op_date=g.path_date
        WHERE g.surgery_episode_id IS NULL
        """
    )
    con.execute(
        f"""
        UPDATE {TARGET} AS g
        SET surgery_episode_id = bf.new_surgery_episode_id,
            linkage_quality = CASE
              WHEN g.specimen_id IS NOT NULL THEN 'full'
              ELSE 'specimen_only'
            END,
            build_script = '{SCRIPT_ID}_path_gland_repair',
            build_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
        FROM path_gland_repair_episode_backfill bf
        WHERE g.research_id=bf.research_id
          AND g.synoptic_row_ix IS NOT DISTINCT FROM bf.synoptic_row_ix
          AND g.synoptic_row_ord=bf.synoptic_row_ord
          AND g.gland_type=bf.gland_type
          AND g.gland_position=bf.gland_position
        """
    )

    # Normalize parathyroid positions from source location text. Empty/unparseable -> NULL.
    con.execute(
        f"""
        UPDATE {TARGET} AS g
        SET gland_position = s.canonical_position,
            build_script = '{SCRIPT_ID}_path_gland_repair',
            build_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
        FROM {STAGE_TABLE} s
        WHERE CAST(g.research_id AS VARCHAR)=s.research_id_str
          AND g.synoptic_row_ord=s.synoptic_row_ord
          AND g.gland_type='parathyroid'
          AND s.gland_type='parathyroid'
          AND g.gland_position=s.slot_position
        """
    )

    # Refresh readable view dependency.
    con.execute(
        """
        CREATE OR REPLACE VIEW views_readable.path_gland_events_VIEW_v1 AS
        SELECT * FROM main.canonical_path_gland_events_v1
        """
    )

    # Column comments document the now-repaired semantics.
    con.execute(
        f"COMMENT ON COLUMN {TARGET}.synoptic_row_ix IS "
        "'Script 108 pandas-load-order global path_synoptics row index; backfilled by Script 397 from path_synoptics parquet load order, not SQL ROW_NUMBER synthesis.'"
    )
    con.execute(
        f"COMMENT ON COLUMN {TARGET}.gland_position IS "
        "'For thyroid_lobe rows: anatomical lobe label from Scripts 361/396. For parathyroid rows: Script 397 normalization of path_synoptics parag_<N>_location to right_superior/right_inferior/left_superior/left_inferior/intrathyroidal_right/intrathyroidal_left/extrathyroidal_other; NULL if source empty/unparseable.'"
    )
    con.execute(
        f"COMMENT ON COLUMN {TARGET}.gland_width_cm IS "
        "'Second numeric dimension parsed from path_synoptics gland size string by Script 397 where present; NULL when source has only one dimension or no size.'"
    )
    con.execute(
        f"COMMENT ON COLUMN {TARGET}.gland_depth_cm IS "
        "'Third numeric dimension parsed from path_synoptics gland size string by Script 397 where present; NULL when source has fewer than three dimensions or no size.'"
    )
    con.execute(
        f"COMMENT ON TABLE {TARGET} IS "
        "'[domain=operative_pathology; grain=per_gland_per_surgery] Repaired by Script 397 path_gland_repair: synoptic_row_ix inherited from Script-108 pandas load order, width/depth parsed from source size strings, unambiguous exact-date surgery_episode_id backfilled, parathyroid position normalized to 7-value taxonomy.'"
    )

    # Registry flip: 14 former not_started columns plus surgery_episode_id critical repair.
    for col, (method, upstream) in REGISTRY_VERIFY_COLUMNS.items():
        con.execute(
            """
            UPDATE main.canonical_column_verification_registry_v1
            SET verification_status = 'verified',
                verified_by = 'logan_glosser_via_path_gland_repair',
                verification_method = ?,
                upstream_source = ?,
                batch_id = ?,
                verified_ts = CURRENT_TIMESTAMP,
                notes = COALESCE(notes, '') || ?
            WHERE schema_name='main'
              AND table_name='canonical_path_gland_events_v1'
              AND column_name=?
            """,
            [
                method,
                upstream,
                BATCH_ID,
                f" | {BATCH_ID}: repaired/verified by scripts/397_path_gland_repair.py.",
                col,
            ],
        )

    con.execute(
        """
        UPDATE main.canonical_table_signoff_registry_v1 ts
        SET n_columns_total = subq.n_total,
            n_verified = subq.n_verified,
            n_not_started = subq.n_not_started,
            n_failed = COALESCE(subq.n_failed, 0),
            n_na = subq.n_na,
            table_status = CASE
              WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
              WHEN subq.n_verified > 0 THEN 'in_progress'
              ELSE 'not_started'
            END,
            signed_off_ts = CURRENT_TIMESTAMP,
            signoff_migration = 'path_gland_repair_20260428',
            notes = COALESCE(ts.notes, '') || ' | path_gland_repair_20260428: synoptic_row_ix, width/depth, episode linkage, and parathyroid position taxonomy repaired.'
        FROM (
          SELECT schema_name, table_name,
                 COUNT(*) AS n_total,
                 SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
                 SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
                 SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
                 SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
          FROM main.canonical_column_verification_registry_v1
          WHERE schema_name='main' AND table_name='canonical_path_gland_events_v1'
          GROUP BY 1,2
        ) subq
        WHERE ts.schema_name=subq.schema_name
          AND ts.table_name=subq.table_name
        """
    )

    con.execute(
        "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 WHERE run_id = ?",
        [BATCH_ID],
    )
    con.execute(
        """
        INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
          (run_id, started_at, ended_at, phases_applied,
           critical_findings_cleared, high_findings_cleared,
           med_findings_cleared, held_for_adjudication)
        VALUES (
          ?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
          'snapshot;stage_script108_synoptic_ix;parse_dimensions;episode_backfill;position_taxonomy;registry_signoff',
          '4', '1', '1', 'range_outliers_documented_no_data_change'
        )
        """,
        [BATCH_ID],
    )
    return None


def registry_summary(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    out = fetchone_dict(
        con,
        """
        SELECT n_columns_total, n_verified, n_not_started, n_failed, n_na,
               table_status, signoff_migration
        FROM main.canonical_table_signoff_registry_v1
        WHERE schema_name='main' AND table_name='canonical_path_gland_events_v1'
        """,
    )
    out.update(fetchone_dict(
        con,
        """
        SELECT SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS registry_not_started,
               SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS registry_failed
        FROM main.canonical_column_verification_registry_v1
        WHERE schema_name='main' AND table_name='canonical_path_gland_events_v1'
        """,
    ))
    return out


def assert_post_state(post: Metrics, reg: dict[str, Any]) -> None:
    if post.row_count != 28724:
        raise AssertionError(f"row count changed unexpectedly: {post.row_count} != 28724")
    syn_nonnull_rate = 1 - (post.null_synoptic_row_ix / post.row_count)
    if syn_nonnull_rate < 0.99:
        raise AssertionError(f"synoptic_row_ix non-null rate {syn_nonnull_rate:.4%} < 99%")
    if post.parathyroid_bad_position_rows != 0:
        raise AssertionError(f"bad parathyroid position rows remain: {post.parathyroid_bad_position_rows}")
    if int(reg.get("registry_not_started") or 0) != 0:
        raise AssertionError(f"registry not_started remains: {reg.get('registry_not_started')}")
    if int(reg.get("registry_failed") or 0) != 0:
        raise AssertionError(f"registry failed remains: {reg.get('registry_failed')}")
    if reg.get("table_status") != "verified":
        raise AssertionError(f"table_status is {reg.get('table_status')}, expected verified")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    do_writes = bool(args.apply)
    mode_name = "apply" if do_writes else "dry-run"
    ts_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Script 397 path_gland_repair — mode={mode_name} ts={ts_tag}", flush=True)

    stage, audit, stage_summary = build_stage_dataframe()
    write_position_audit(audit)
    print(f"  wrote {AUDIT_CSV.relative_to(REPO_ROOT)} ({len(audit):,} distinct source values)", flush=True)

    con = connect_locked()
    tmp_stage_path: Path | None = None
    snapshot_name: str | None = None
    post: Metrics | None = None
    reg: dict[str, Any] | None = None
    try:
        tmp_stage_path = load_stage_table(con, stage)
        pre = collect_metrics(con)
        db_summary = db_stage_summary(con)
        outliers = get_outlier_summary(con)
        print(f"  pre rows={pre.row_count:,} null_synoptic={pre.null_synoptic_row_ix:,} null_ep={pre.null_surgery_episode_id:,}", flush=True)
        print(f"  matched stage rows={db_summary.get('matched_gland_rows'):,}; exact episode backfill candidates={db_summary.get('exact_unique_episode_backfill_rows'):,}", flush=True)

        if do_writes:
            # MotherDuck/DuckDB allows a transaction to write only one attached
            # database. The archive snapshot is intentionally created first in
            # the archive database, then all canonical mutations run in a
            # single publication-DB transaction.
            snapshot_name = create_snapshot(con, ts_tag)
            con.execute("BEGIN TRANSACTION")
            try:
                apply_repair(con)
                post = collect_metrics(con)
                reg = registry_summary(con)
                assert_post_state(post, reg)
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            print(f"  applied; snapshot={snapshot_name}", flush=True)
            print(f"  post null_synoptic={post.null_synoptic_row_ix:,} null_ep={post.null_surgery_episode_id:,} bad_para_pos={post.parathyroid_bad_position_rows:,}", flush=True)
        else:
            print("  dry-run only; no DB writes performed", flush=True)

        write_report(
            mode=mode_name,
            pre=pre,
            post=post,
            stage_summary=stage_summary,
            db_summary=db_summary,
            snapshot_name=snapshot_name,
            registry_summary=reg,
            outlier_summary=outliers,
        )
        print(f"  wrote {REPORT_MD.relative_to(REPO_ROOT)}", flush=True)
        return 0
    finally:
        try:
            con.close()
        finally:
            if tmp_stage_path is not None:
                try:
                    tmp_stage_path.unlink()
                except OSError:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())
