#!/usr/bin/env python3
"""Emit PROMPT3_ln_master_rollup_field_mapping.csv (78 rows)."""

from __future__ import annotations

import csv
from pathlib import Path

COLS = [
    "research_id",
    "ln_total_examined",
    "ln_total_positive",
    "ln_ratio",
    "ln_any_positive",
    "ln_largest_deposit_cm",
    "ln_central_examined",
    "ln_central_positive",
    "ln_lateral_left_examined",
    "ln_lateral_left_positive",
    "ln_lateral_right_examined",
    "ln_lateral_right_positive",
    "ln_bilateral_lateral_examined",
    "ln_bilateral_lateral_positive",
    "ln_other_examined",
    "ln_other_positive",
    "ln_total_examined_from_locations",
    "ln_total_positive_from_locations",
    "ln_level_i_examined",
    "ln_level_i_positive",
    "ln_level_ii_examined",
    "ln_level_ii_positive",
    "ln_level_iii_examined",
    "ln_level_iii_positive",
    "ln_level_iv_examined",
    "ln_level_iv_positive",
    "ln_level_v_examined",
    "ln_level_v_positive",
    "ln_level_vi_examined",
    "ln_level_vi_positive",
    "ln_level_vii_examined",
    "ln_level_vii_positive",
    "ln_level_unspecified_examined",
    "ln_level_unspecified_positive",
    "ln_region_central_examined",
    "ln_region_central_positive",
    "ln_region_lateral_left_examined",
    "ln_region_lateral_left_positive",
    "ln_region_lateral_right_examined",
    "ln_region_lateral_right_positive",
    "ln_region_other_examined",
    "ln_region_other_positive",
    "ln_extranodal_extension",
    "ln_mets_extranodal_extension",
    "ln_mets_ptc",
    "ln_mets_ptc_variant",
    "ln_mets_ftc",
    "ln_mets_hurthle",
    "ln_mets_mtc",
    "ln_mets_atc",
    "ln_mets_pdtc",
    "ln_mets_tumor_types_array",
    "ln_mets_n_tumor_types",
    "ln_mets_micrometastasis",
    "ln_mets_cystic",
    "ln_histology_source",
    "ln_histology_raw_text",
    "ln_parsed_locations_json",
    "ln_parsed_data_json",
    "ln_locations_parsed_count",
    "ln_total_locations_parsed",
    "ln_total_levels_involved",
    "dominant_histology_type",
    "num_tumors_identified",
    "histology_1_type",
    "histology_1_n_stage_ajcc8",
    "ln_central_positive_summary",
    "ln_lateral_positive_summary",
    "ln_source",
    "ln_internal_consistency",
    "ln_crossval_status",
    "has_per_level_data",
    "has_parsed_json",
    "json_n_locations",
    "json_n_data_items",
    "json_total_examined",
    "json_total_positive",
    "json_location_summary",
]


def mapping_for(col: str) -> tuple[str, str]:
    tp = "pub_canonical.tumor_pathology"
    cpm = "pub_canonical.canonical_patient_master"
    ctc = "pub_canonical.canonical_tumor_characteristics_v1"
    pme = "pub_canonical.canonical_path_malignant_events_v1"

    if col == "research_id":
        return "REPRESENTED", f"{cpm}.{col}; join key to pathology tables"

    roll = {
        "ln_total_examined": (
            f"{cpm}.ln_rollup_total_examined (+ {cpm}.ln_total_examined); "
            f"{ctc}.ln_examined (per-resected-tumor grain)"
        ),
        "ln_total_positive": (
            f"{cpm}.ln_rollup_total_positive (+ {cpm}.ln_total_positive); "
            f"{ctc}.ln_involved (tumor grain surrogate for positive nodal count)"
        ),
        "ln_ratio": (f"{cpm}.ln_ratio (+ {cpm}.ln_rollup_ratio)"),
        "ln_any_positive": (f"{cpm}.ln_rollup_any_positive (+ {cpm}.ln_positive_flag bridge)"),
        "ln_largest_deposit_cm": (f"{cpm}.ln_rollup_largest_deposit_cm (+ {cpm}.tp_ln_largest_deposit_cm)"),
        "ln_central_examined": f"{cpm}.ln_rollup_central_examined",
        "ln_central_positive": f"{cpm}.ln_rollup_central_positive (+ {cpm}.tp_ln_central_positive)",
        "ln_lateral_left_examined": f"{cpm}.ln_rollup_lateral_left_examined",
        "ln_lateral_left_positive": f"{cpm}.ln_rollup_lateral_left_positive",
        "ln_lateral_right_examined": f"{cpm}.ln_rollup_lateral_right_examined",
        "ln_lateral_right_positive": f"{cpm}.ln_rollup_lateral_right_positive",
        "ln_bilateral_lateral_examined": f"{cpm}.ln_rollup_bilateral_lateral_examined",
        "ln_bilateral_lateral_positive": f"{cpm}.ln_rollup_bilateral_lateral_positive",
        "ln_other_examined": f"{cpm}.ln_rollup_other_examined",
        "ln_other_positive": f"{cpm}.ln_rollup_other_positive",
        "ln_total_examined_from_locations": f"{cpm}.syn_ln_total_examined_from_locations_legacy",
        "ln_total_positive_from_locations": f"{cpm}.syn_ln_total_positive_from_locations_legacy",
        "ln_histology_source": f"{tp}.ln_histology_source",
        "ln_histology_raw_text": f"{tp}.ln_histology_raw_text",
        "ln_parsed_locations_json": f"{tp}.ln_parsed_locations_json",
        "ln_parsed_data_json": f"{tp}.ln_parsed_data_json",
        "ln_locations_parsed_count": f"{tp}.ln_locations_parsed_count",
        "ln_total_locations_parsed": f"{tp}.ln_total_locations_parsed",
        "ln_total_levels_involved": f"{tp}.ln_total_levels_involved (+ {cpm}.ln_rollup_total_levels_involved)",
        "dominant_histology_type": f"{tp}.dominant_histology_type",
        "num_tumors_identified": f"{tp}.num_tumors_identified",
        "histology_1_type": f"{tp}.histology_1_type",
        "histology_1_n_stage_ajcc8": f"{tp}.histology_1_n_stage_ajcc8; compare {pme} nodal AJCC staging",
        "ln_central_positive_summary": (
            f"{tp}.primary_ln_ln_central_positive (rollup-derived summary feeds {cpm} tp_ln_* columns)"
        ),
        "ln_lateral_positive_summary": f"{tp}.primary_ln_ln_lateral_positive (+ {cpm}.tp_ln_lateral_positive rollup)",
        "ln_source": f"{cpm}.ln_rollup_source",
        "ln_internal_consistency": (
            f"{cpm}.ln_rollup_internal_consistency (captures ln_master vs path_synoptic audits; replaces raw ln_crossval_v1 linkage)"
        ),
        "ln_crossval_status": f"{cpm}.ln_rollup_crossval_status",
        "has_per_level_data": f"{cpm}.ln_rollup_has_per_level_data",
    }
    if col in roll:
        return "REPRESENTED", roll[col]

    if col.startswith("ln_level_"):
        return "REPRESENTED", f"{tp}.{col}; {cpm} mirrors examined counts via ln_level_*_examined (+ _v2 columns for secondary QC grain)"

    if col.startswith("ln_region_"):
        return "REPRESENTED", f"{tp}.{col} (regional LN parser duplicates compartment columns; analytic MAX-per-patient lives on {cpm} ln_rollup_*)"

    if col.startswith("ln_mets_") and col not in ("ln_mets_tumor_types_array", "ln_mets_n_tumor_types"):
        return "REPRESENTED", f"{tp}.{col}; {cpm} ln_rollup_{col}"

    if col == "ln_mets_extranodal_extension":
        return "REPRESENTED", f"{tp}.ln_mets_extranodal_extension; {cpm}.ln_rollup_mets_ene"

    if col == "ln_extranodal_extension":
        return (
            "REPRESENTED",
            (
                f"{pme}.extranodal_extension (CAP synoptic tumour grain via CTC lineage); "
                f"{ctc}.extranodal_extension; {cpm}.ln_rollup_ene aggregates worst-case LN ENE phenotype"
            ),
        )

    if col == "ln_mets_tumor_types_array":
        return (
            "INTENTIONALLY_DROPPED",
            (
                "VARCHAR-encoded list duplicated by boolean ln_mets_* + ln_rollup_mets_* mutually exclusive flags "
                "on tumour_pathology and CPM — array not stored as JSON string in canonical_patient_master"
            ),
        )

    if col == "ln_mets_n_tumor_types":
        return (
            "INTENTIONALLY_DROPPED_AS_SCALAR",
            "Recompute COUNT over ln_rollup_mets_* TRUE flags when needed; tumour_pathology preserves individual booleans instead of scalar count.",
        )

    if col == "has_parsed_json":
        return (
            "REPRESENTED_DERIVED",
            (
                "LOGICAL: (ln_parsed_data_json IS NOT NULL AND LENGTH(JSON)>2) evaluated in MotherDuck build; "
                "no standalone BOOL column — use tumour_pathology JSON presence in SQL predicates",
            ),
        )

    if col.startswith("json_"):
        return (
            "INTENTIONALLY_DROPPED_ARTIFACT",
            (
                "Pandas rollup digest emitted only by offline scripts/ln_master_rollup.py — not propagated to BigQuery "
                "(re-materialize locally from parquet or re-parse ln_parsed_data_json)."
            ),
        )

    raise KeyError(col)


def main() -> None:
    out = Path(__file__).resolve().parent / "PROMPT3_ln_master_rollup_field_mapping.csv"
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["idx", "ln_master_rollup_column", "status", "canonical_location_or_resolution"])
        if len(COLS) != 78:
            raise RuntimeError(f"Expected 78 columns, got {len(COLS)}")
        for i, col in enumerate(COLS, start=1):
            st, loc = mapping_for(col)
            w.writerow([i, col, st, loc])
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
