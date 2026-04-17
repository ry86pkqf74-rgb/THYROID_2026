#!/usr/bin/env python3
"""
LN Master Rollup — build the definitive per-patient lymph node table.

Primary source: tumor_pathology (4,290 rows, 249 columns)
Fallback: path_synoptics for patients missing from tumor_pathology or with NULL values

Output:
  output/ln_master_rollup.parquet — one row per patient with full LN profile

Usage:
  .venv/bin/python scripts/ln_master_rollup.py          # parquet backup (local)
  .venv/bin/python scripts/ln_master_rollup.py --md     # MotherDuck cloud
"""
from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

OUTPUT_DIR = REPO / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def _connect(use_md: bool) -> duckdb.DuckDBPyConnection:
    if use_md:
        from motherduck_client import MotherDuckClient
        client = MotherDuckClient.for_env("prod")
        return client.connect_rw()
    return duckdb.connect()


def _register_parquets(con: duckdb.DuckDBPyConnection) -> None:
    backup = REPO / "output" / "parquet_backup"
    for name in ("tumor_pathology", "path_synoptics", "patient_refined_master_clinical_v12"):
        pq = backup / f"{name}.parquet"
        if not pq.exists():
            raise FileNotFoundError(f"Missing parquet backup: {pq}")
        con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{pq}')")


ROLLUP_SQL = """
WITH tp AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,

        -- Summary counts
        primary_ln_ln_total_examined,
        primary_ln_ln_total_positive,
        primary_ln_ln_ratio,
        primary_ln_ln_any_positive,
        primary_ln_ln_largest_deposit_cm,
        primary_ln_ln_extranodal_extension,
        primary_ln_ln_central_positive,
        primary_ln_ln_lateral_positive,
        primary_ln_ln_location_detail,
        primary_ln_ln_levels_examined,
        primary_ln_ln_levels_involved,
        primary_ln_ln_comment,

        -- Regional breakdown (direct parsed)
        ln_central_examined,
        ln_central_positive,
        ln_lateral_left_examined,
        ln_lateral_left_positive,
        ln_lateral_right_examined,
        ln_lateral_right_positive,
        ln_bilateral_lateral_examined,
        ln_bilateral_lateral_positive,
        ln_other_examined,
        ln_other_positive,
        ln_total_examined_from_locations,
        ln_total_positive_from_locations,

        -- Per-level breakdown
        ln_level_i_examined,   ln_level_i_positive,
        ln_level_ii_examined,  ln_level_ii_positive,
        ln_level_iii_examined, ln_level_iii_positive,
        ln_level_iv_examined,  ln_level_iv_positive,
        ln_level_v_examined,   ln_level_v_positive,
        ln_level_vi_examined,  ln_level_vi_positive,
        ln_level_vii_examined, ln_level_vii_positive,
        ln_level_unspecified_examined, ln_level_unspecified_positive,

        -- Per-region aggregated
        ln_region_central_examined,  ln_region_central_positive,
        ln_region_lateral_left_examined, ln_region_lateral_left_positive,
        ln_region_lateral_right_examined, ln_region_lateral_right_positive,
        ln_region_other_examined, ln_region_other_positive,

        -- Per-cancer-type metastasis flags
        ln_mets_ptc,
        ln_mets_ptc_variant,
        ln_mets_ftc,
        ln_mets_hurthle,
        ln_mets_mtc,
        ln_mets_atc,
        ln_mets_pdtc,
        ln_mets_micrometastasis,
        ln_mets_extranodal_extension,
        ln_mets_cystic,

        -- LN histology source
        ln_histology_source,
        ln_histology_raw_text,

        -- Parsed JSON data
        ln_parsed_locations_json,
        ln_parsed_data_json,
        ln_locations_parsed_count,
        ln_total_locations_parsed,
        ln_total_levels_involved,

        -- Histology context from primary tumor
        dominant_histology_type,
        num_tumors_identified,
        histology_1_type,
        histology_1_n_stage_ajcc8,
        histology_1_ln_examined,
        histology_1_ln_positive,

        -- Build tumor type array from boolean flags
        list_filter(
            [
                CASE WHEN ln_mets_ptc IS TRUE THEN 'PTC' END,
                CASE WHEN ln_mets_ftc IS TRUE THEN 'FTC' END,
                CASE WHEN ln_mets_hurthle IS TRUE THEN 'Hurthle' END,
                CASE WHEN ln_mets_mtc IS TRUE THEN 'MTC' END,
                CASE WHEN ln_mets_atc IS TRUE THEN 'ATC' END,
                CASE WHEN ln_mets_pdtc IS TRUE THEN 'PDTC' END
            ],
            x -> x IS NOT NULL
        ) AS _tumor_types_list

    FROM tumor_pathology
),
ps AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        TRY_CAST(tumor_1_ln_examined AS INTEGER) AS ps_ln_examined,
        TRY_CAST(tumor_1_ln_involved AS INTEGER) AS ps_ln_positive,
        tumor_1_ln_examined_comment AS ps_ln_comment,
        tumor_1_ln_location AS ps_ln_location,
        tumor_1_histologic_type AS ps_histologic_type
    FROM path_synoptics
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CAST(research_id AS VARCHAR)
        ORDER BY TRY_CAST(tumor_1_ln_examined AS INTEGER) DESC NULLS LAST
    ) = 1
)
SELECT
    tp.research_id,

    -- === Summary counts (COALESCE tp → ps fallback) ===
    COALESCE(tp.primary_ln_ln_total_examined, ps.ps_ln_examined)
        AS ln_total_examined,
    COALESCE(tp.primary_ln_ln_total_positive, ps.ps_ln_positive)
        AS ln_total_positive,
    COALESCE(
        tp.primary_ln_ln_ratio,
        CASE
            WHEN COALESCE(tp.primary_ln_ln_total_examined, ps.ps_ln_examined) > 0
            THEN ROUND(
                CAST(COALESCE(tp.primary_ln_ln_total_positive, ps.ps_ln_positive) AS DOUBLE)
                / CAST(COALESCE(tp.primary_ln_ln_total_examined, ps.ps_ln_examined) AS DOUBLE),
                4
            )
        END
    ) AS ln_ratio,
    CASE
        WHEN tp.primary_ln_ln_any_positive IS NOT NULL
            THEN tp.primary_ln_ln_any_positive > 0
        WHEN COALESCE(tp.primary_ln_ln_total_positive, ps.ps_ln_positive) IS NOT NULL
            THEN COALESCE(tp.primary_ln_ln_total_positive, ps.ps_ln_positive) > 0
    END AS ln_any_positive,
    tp.primary_ln_ln_largest_deposit_cm AS ln_largest_deposit_cm,

    -- === Regional breakdown ===
    tp.ln_central_examined,
    tp.ln_central_positive,
    tp.ln_lateral_left_examined,
    tp.ln_lateral_left_positive,
    tp.ln_lateral_right_examined,
    tp.ln_lateral_right_positive,
    tp.ln_bilateral_lateral_examined,
    tp.ln_bilateral_lateral_positive,
    tp.ln_other_examined,
    tp.ln_other_positive,
    tp.ln_total_examined_from_locations,
    tp.ln_total_positive_from_locations,

    -- === Per-level breakdown (I-VII + unspecified) ===
    tp.ln_level_i_examined,   tp.ln_level_i_positive,
    tp.ln_level_ii_examined,  tp.ln_level_ii_positive,
    tp.ln_level_iii_examined, tp.ln_level_iii_positive,
    tp.ln_level_iv_examined,  tp.ln_level_iv_positive,
    tp.ln_level_v_examined,   tp.ln_level_v_positive,
    tp.ln_level_vi_examined,  tp.ln_level_vi_positive,
    tp.ln_level_vii_examined, tp.ln_level_vii_positive,
    tp.ln_level_unspecified_examined, tp.ln_level_unspecified_positive,

    -- === Per-region aggregated ===
    tp.ln_region_central_examined,  tp.ln_region_central_positive,
    tp.ln_region_lateral_left_examined, tp.ln_region_lateral_left_positive,
    tp.ln_region_lateral_right_examined, tp.ln_region_lateral_right_positive,
    tp.ln_region_other_examined, tp.ln_region_other_positive,

    -- === Extranodal extension ===
    tp.primary_ln_ln_extranodal_extension AS ln_extranodal_extension,
    tp.ln_mets_extranodal_extension AS ln_mets_extranodal_extension,

    -- === Per-cancer-type metastasis flags ===
    tp.ln_mets_ptc,
    tp.ln_mets_ptc_variant,
    tp.ln_mets_ftc,
    tp.ln_mets_hurthle,
    tp.ln_mets_mtc,
    tp.ln_mets_atc,
    tp.ln_mets_pdtc,
    CAST(tp._tumor_types_list AS VARCHAR) AS ln_mets_tumor_types_array,
    len(tp._tumor_types_list) AS ln_mets_n_tumor_types,
    tp.ln_mets_micrometastasis,
    tp.ln_mets_cystic,

    -- === LN histology source ===
    tp.ln_histology_source,
    tp.ln_histology_raw_text,

    -- === Parsed JSON ===
    tp.ln_parsed_locations_json,
    tp.ln_parsed_data_json,
    tp.ln_locations_parsed_count,
    tp.ln_total_locations_parsed,
    tp.ln_total_levels_involved,

    -- === Histology context ===
    tp.dominant_histology_type,
    tp.num_tumors_identified,
    tp.histology_1_type,
    tp.histology_1_n_stage_ajcc8,

    -- === Central / lateral summary flags ===
    COALESCE(tp.primary_ln_ln_central_positive, 0) AS ln_central_positive_summary,
    COALESCE(tp.primary_ln_ln_lateral_positive, 0) AS ln_lateral_positive_summary,

    -- === Data quality flags ===
    CASE
        WHEN tp.primary_ln_ln_total_examined IS NOT NULL THEN 'tumor_pathology'
        WHEN ps.ps_ln_examined IS NOT NULL THEN 'path_synoptics_fallback'
        ELSE 'no_data'
    END AS ln_source,

    CASE
        WHEN tp.primary_ln_ln_total_examined IS NULL
             AND tp.ln_total_examined_from_locations IS NULL
            THEN 'insufficient_data'
        WHEN tp.ln_total_examined_from_locations IS NULL
            THEN 'no_location_breakdown'
        WHEN tp.primary_ln_ln_total_examined IS NULL
            THEN 'location_only'
        WHEN ABS(tp.primary_ln_ln_total_examined - tp.ln_total_examined_from_locations) <= 1
            THEN 'ok'
        ELSE 'mismatch'
    END AS ln_internal_consistency,

    CASE
        WHEN tp.primary_ln_ln_total_examined IS NOT NULL
             AND ps.ps_ln_examined IS NOT NULL
             AND ABS(tp.primary_ln_ln_total_examined - ps.ps_ln_examined) <= 1
            THEN 'agree'
        WHEN tp.primary_ln_ln_total_examined IS NOT NULL
             AND ps.ps_ln_examined IS NOT NULL
             AND ABS(tp.primary_ln_ln_total_examined - ps.ps_ln_examined) > 1
            THEN 'discordant'
        WHEN tp.primary_ln_ln_total_examined IS NOT NULL
            THEN 'single_source_only'
        ELSE 'no_data'
    END AS ln_crossval_status,

    (tp.ln_level_i_examined IS NOT NULL
     OR tp.ln_level_ii_examined IS NOT NULL
     OR tp.ln_level_iii_examined IS NOT NULL
     OR tp.ln_level_iv_examined IS NOT NULL
     OR tp.ln_level_v_examined IS NOT NULL
     OR tp.ln_level_vi_examined IS NOT NULL
     OR tp.ln_level_vii_examined IS NOT NULL
    ) AS has_per_level_data,

    (tp.ln_parsed_data_json IS NOT NULL
     AND LENGTH(tp.ln_parsed_data_json) > 2
    ) AS has_parsed_json

FROM tp
LEFT JOIN ps ON tp.research_id = ps.research_id
ORDER BY tp.research_id
"""


def _parse_json_locations(df: pd.DataFrame) -> pd.DataFrame:
    """Parse ln_parsed_locations_json and ln_parsed_data_json into summary columns.

    Uses positional assignment (not merge) to avoid cartesian products
    when research_id has duplicates (multi-surgery patients).
    """
    json_n_locations = []
    json_n_data_items = []
    json_total_examined = []
    json_total_positive = []
    json_location_summary = []

    def _safe_parse(raw: str) -> list[dict]:
        """Parse JSON or Python repr (set(), dict with single quotes)."""
        if not raw or str(raw).strip() in ("", "null", "[]", "None"):
            return []
        s = str(raw).strip()
        for parser in (json.loads, ast.literal_eval):
            try:
                result = parser(s)
                if isinstance(result, list):
                    return result
                if isinstance(result, dict):
                    return [result]
            except Exception:
                continue
        return []

    for _, row in df.iterrows():
        loc_json = row.get("ln_parsed_locations_json")
        data_json = row.get("ln_parsed_data_json")

        locations = _safe_parse(loc_json) if pd.notna(loc_json) else []
        data_items = _safe_parse(data_json) if pd.notna(data_json) else []

        tot_examined = 0
        tot_positive = 0
        loc_details = []

        for item in data_items:
            if isinstance(item, dict):
                examined = item.get("examined") or item.get("total_examined") or 0
                positive = item.get("positive") or item.get("total_positive") or 0
                loc_name = item.get("location") or item.get("name") or "unknown"
                try:
                    examined = int(examined) if examined else 0
                    positive = int(positive) if positive else 0
                except (ValueError, TypeError):
                    examined = 0
                    positive = 0
                tot_examined += examined
                tot_positive += positive
                if examined > 0 or positive > 0:
                    loc_details.append(f"{loc_name}:{positive}/{examined}")

        json_n_locations.append(len(locations))
        json_n_data_items.append(len(data_items))
        json_total_examined.append(tot_examined if tot_examined > 0 else None)
        json_total_positive.append(tot_positive if tot_positive > 0 else None)
        json_location_summary.append("; ".join(loc_details) if loc_details else None)

    df = df.copy()
    df["json_n_locations"] = json_n_locations
    df["json_n_data_items"] = json_n_data_items
    df["json_total_examined"] = json_total_examined
    df["json_total_positive"] = json_total_positive
    df["json_location_summary"] = json_location_summary
    return df


def run(use_md: bool = False) -> pd.DataFrame:
    con = _connect(use_md)
    if not use_md:
        _register_parquets(con)

    print("Building LN master rollup...")
    df = con.execute(ROLLUP_SQL).fetchdf()
    print(f"  Raw rollup rows: {len(df)}")

    # Parse JSON location detail
    has_json = df["has_parsed_json"].sum()
    print(f"  Rows with parsed JSON: {has_json}")
    if has_json > 0:
        print("  Parsing JSON location details...")
        df = _parse_json_locations(df)

    out_path = OUTPUT_DIR / "ln_master_rollup.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  Saved: {out_path}")

    # --- Summary stats ---
    print("\n" + "=" * 70)
    print("LN MASTER ROLLUP SUMMARY")
    print("=" * 70)

    print(f"\nTotal patients: {len(df)}")

    # LN data availability
    has_examined = df["ln_total_examined"].notna().sum()
    has_positive = df["ln_total_positive"].notna().sum()
    has_any_pos = df["ln_any_positive"].sum() if "ln_any_positive" in df else 0
    print(f"  With LN examined count: {has_examined} ({has_examined/len(df)*100:.1f}%)")
    print(f"  With LN positive count: {has_positive} ({has_positive/len(df)*100:.1f}%)")
    print(f"  With any LN positive:   {has_any_pos}")

    # Source breakdown
    print("\n--- Data Source ---")
    vc = df["ln_source"].value_counts()
    for k, v in sorted(vc.items()):
        print(f"    {k:30s}  {v:>5d}")

    # Internal consistency
    print("\n--- Internal Consistency ---")
    vc = df["ln_internal_consistency"].value_counts()
    for k, v in sorted(vc.items()):
        print(f"    {k:30s}  {v:>5d}")

    # Cross-val status
    print("\n--- Cross-Validation Status ---")
    vc = df["ln_crossval_status"].value_counts()
    for k, v in sorted(vc.items()):
        print(f"    {k:30s}  {v:>5d}")

    # Per-level data completeness (count non-zero, since 0 means "not assessed")
    print("\n--- Per-Level Data Completeness (non-zero values) ---")
    level_cols = [c for c in df.columns if c.startswith("ln_level_") and c.endswith("_examined")]
    any_level_mask = pd.Series(False, index=df.index)
    for col in level_cols:
        n = (df[col].fillna(0) > 0).sum()
        level_name = col.replace("ln_level_", "").replace("_examined", "")
        pos_col = col.replace("_examined", "_positive")
        n_pos = (df[pos_col].fillna(0) > 0).sum() if pos_col in df.columns else 0
        any_level_mask = any_level_mask | (df[col].fillna(0) > 0)
        print(f"    Level {level_name:15s}  examined: {n:>5d}  positive: {n_pos:>5d}")

    has_level = any_level_mask.sum()
    print(f"\n  Patients with ANY per-level data: {has_level} ({has_level/len(df)*100:.1f}%)")

    # Regional breakdown completeness (non-zero)
    print("\n--- Regional Breakdown Completeness (non-zero) ---")
    region_cols = {
        "central": "ln_central_examined",
        "lateral_left": "ln_lateral_left_examined",
        "lateral_right": "ln_lateral_right_examined",
        "bilateral_lateral": "ln_bilateral_lateral_examined",
        "other": "ln_other_examined",
    }
    for region, col in region_cols.items():
        n = (df[col].fillna(0) > 0).sum()
        pos_col = col.replace("_examined", "_positive")
        n_pos = (df[pos_col].fillna(0) > 0).sum() if pos_col in df.columns else 0
        print(f"    {region:25s}  examined: {n:>5d}  positive: {n_pos:>5d}")

    # Cancer-type metastasis
    print("\n--- Per-Cancer-Type LN Metastasis ---")
    mets_cols = {
        "PTC": "ln_mets_ptc",
        "FTC": "ln_mets_ftc",
        "Hurthle": "ln_mets_hurthle",
        "MTC": "ln_mets_mtc",
        "ATC": "ln_mets_atc",
        "PDTC": "ln_mets_pdtc",
    }
    for label, col in mets_cols.items():
        n_true = (df[col] == True).sum()  # noqa: E712
        n_any = df[col].notna().sum()
        print(f"    {label:10s}  positive: {n_true:>5d}  (of {n_any} assessed)")

    # Multi-type metastasis distribution
    print("\n--- Multi-Type LN Metastasis Distribution ---")
    if "ln_mets_n_tumor_types" in df:
        vc = df["ln_mets_n_tumor_types"].value_counts().sort_index()
        for k, v in vc.items():
            label = "types" if k != 1 else "type "
            print(f"    {k} {label}: {v:>5d}")
        multi = (df["ln_mets_n_tumor_types"] > 1).sum()
        print(f"\n  Patients with multi-type LN metastasis (>1 type): {multi}")

    # Micrometastasis / cystic
    print("\n--- Special LN Features ---")
    for label, col in [
        ("Micrometastasis", "ln_mets_micrometastasis"),
        ("Cystic metastasis", "ln_mets_cystic"),
        ("Extranodal extension (mets flag)", "ln_mets_extranodal_extension"),
    ]:
        n_true = (df[col] == True).sum()  # noqa: E712
        print(f"    {label:40s}  {n_true:>5d}")

    # JSON parsing stats
    if "json_n_data_items" in df:
        print("\n--- JSON Parsed Location Detail ---")
        has_json_detail = (df["json_n_data_items"] > 0).sum()
        print(f"    Patients with parsed JSON data: {has_json_detail}")
        if has_json_detail > 0:
            avg_locs = df.loc[df["json_n_data_items"] > 0, "json_n_data_items"].mean()
            print(f"    Avg locations per patient (when present): {avg_locs:.1f}")

    # Histology distribution among LN-positive patients
    print("\n--- Dominant Histology (LN-positive patients only) ---")
    pos_df = df[df["ln_any_positive"] == True]  # noqa: E712
    if len(pos_df) > 0:
        vc = pos_df["dominant_histology_type"].value_counts().head(10)
        for k, v in vc.items():
            print(f"    {str(k):25s}  {v:>5d}")
    else:
        print("    (no LN-positive patients found)")

    con.close()
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LN master rollup from tumor_pathology")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck instead of local parquet")
    args = parser.parse_args()
    run(use_md=args.md)
