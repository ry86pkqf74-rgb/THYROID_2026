#!/usr/bin/env python3
"""Script 376 — Phase 1: build us_llm_absorption_mapping_v1.

For each held LLM entity table, parse result_json (DuckDB JSON funcs) to
extract distinct entity_types and assign them to canonical_us_nodule_v2
columns. Outputs a row per (entity_source_table, entity_type) with target
column + absorption rule + observed row count.

Entities with no clean target column are flagged 'gap — document only';
non-US entities (ct_neck, nuclear_med, pet_ct, mri_neck, ct_chest,
chest_xray, ultrasound_lymph_node, lymph_node_level, lymph_node) are
excluded from absorption — they belong in future modality tables.
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
SCRIPT_TAG = "Script 376"
TARGET = f"{PUB}.manuscript_workspace.us_llm_absorption_mapping_v1"
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"376_us_llm_mapping_{RUN_TS}.json"

LLM_TABLES = [
    "note_entities_llm_tirads_granular",
    "note_entities_llm_us_nodule_dynamics",
    "note_entities_llm_imaging",
]

# entity_type → (target column on canonical_us_nodule_v2, rule)
# Curated by inspecting probe output (3,266 ultrasound_thyroid, 2,087
# nodule_size, etc.) and v2 column inventory. Entities with no clean
# target are marked '<gap>' / 'gap — document only' and not absorbed.
MAPPING: dict[str, tuple[str, str]] = {
    # tirads_granular and us_nodule_dynamics overlap on these:
    "composition":                  ("composition",            "COALESCE existing"),
    "echogenicity":                 ("echogenicity",           "COALESCE existing"),
    "shape":                        ("shape",                  "COALESCE existing"),
    "margins":                      ("margins",                "COALESCE existing"),
    "margin":                       ("margins",                "COALESCE existing"),
    "echogenic_foci":               ("echogenic_foci",         "COALESCE existing"),
    "tirads_component_composition": ("composition",            "COALESCE existing"),
    "tirads_component_echogenicity":("echogenicity",           "COALESCE existing"),
    "tirads_component_shape":       ("shape",                  "COALESCE existing"),
    "tirads_component_margin":      ("margins",                "COALESCE existing"),
    "tirads_component_foci":        ("echogenic_foci",         "COALESCE existing"),

    # imaging-table US-relevant types
    "ultrasound_thyroid":           ("comparison_statement",   "COALESCE existing (free-text holding pen)"),
    "nodule_size":                  ("size_cm_max",            "parse 'X.Y cm|mm' → DOUBLE; COALESCE"),
    "nodule_location":              ("location_raw",           "COALESCE existing"),
    "tirads_score":                 ("tirads_reported_in_text", "parse 'TR[1-5]' or 'TI-RADS [1-5]' → INTEGER; COALESCE"),
    "tirads":                       ("tirads_reported_in_text", "parse 'TR[1-5]' → INTEGER; COALESCE"),
    "tirads_category":              ("updated_tirads_category", "passthrough TR1-TR5; COALESCE"),
    "nodule_volume":                ("volume_ml",              "parse 'X.Y cm³|mL' → DOUBLE; COALESCE"),
    "nodule_dimensions":            ("size_cm_max",            "parse first dimension; COALESCE"),
    "nodule_identifier":            ("location_raw",           "COALESCE existing"),
    "nodule":                       ("location_raw",           "COALESCE existing (low confidence)"),
    "thyromegaly":                  ("<gap>",                  "gland-level finding; belongs in canonical_us_thyroid_gland_v2"),
    "fna":                          ("fna_recommended_this_nodule", "TRUE if entity present; COALESCE"),

    # imaging-table NON-US types (deliberately excluded — future modality work)
    "ct_neck":                      ("<non_us>", "future canonical_ct_lymph_node_v2"),
    "ct_chest":                     ("<non_us>", "future canonical_ct_chest_v2"),
    "pet_ct":                       ("<non_us>", "future canonical_petct_v2"),
    "mri_neck":                     ("<non_us>", "future canonical_mr_v2"),
    "chest_xray":                   ("<non_us>", "future canonical_cxr_v2"),
    "nuclear_med":                  ("<non_us>", "future canonical_nucmed_v2"),
    "ultrasound_lymph_node":        ("<gap>",    "belongs in canonical_us_lymph_node_v2 (already exists)"),
    "lymph_node_level":             ("<gap>",    "belongs in canonical_us_lymph_node_v2"),
    "lymph_node":                   ("<gap>",    "belongs in canonical_us_lymph_node_v2"),
    "tracheal_compression":         ("<gap>",    "non-nodule symptom; future tracheal table"),
    "tracheal_deviation":           ("<gap>",    "non-nodule symptom"),
    "pathology":                    ("<non_us>", "non-imaging entity"),
}


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def extract_entity_types(con, tbl: str) -> dict[str, int]:
    """Parse result_json safely. Some result_json values are non-array
    scalars (e.g. plain '5.5'); skip them.
    """
    fq = f"{PUB}.main.{tbl}"
    # Use list_transform to safely parse the entities array
    try:
        rows = con.execute(f"""
WITH parsed AS (
  SELECT
    TRY_CAST(result_json AS JSON) AS j
  FROM {fq}
  WHERE result_json IS NOT NULL
    AND TRIM(result_json) <> ''
    AND result_json LIKE '%entities%'
    AND result_json NOT LIKE '%"entities": []%'
),
exploded AS (
  SELECT json_extract(j, '$.entities') AS arr FROM parsed
  WHERE json_type(j, '$.entities') = 'ARRAY'
),
entities AS (
  SELECT UNNEST(CAST(arr AS JSON[])) AS e FROM exploded
)
SELECT json_extract_string(e, '$.entity_type') AS et, COUNT(*) AS n
FROM entities GROUP BY 1 ORDER BY 2 DESC
""").fetchall()
        return {r[0]: r[1] for r in rows if r[0]}
    except Exception as e:
        log(f"  WARN: parse error on {tbl}: {e}")
        return {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    # 1. extract entity_type counts per table
    type_counts: dict[str, dict[str, int]] = {}
    for tbl in LLM_TABLES:
        type_counts[tbl] = extract_entity_types(con, tbl)
        log(f"  {tbl}: {len(type_counts[tbl])} distinct entity types, "
            f"{sum(type_counts[tbl].values())} total entity rows")

    # 2. build mapping table
    mapping_rows: list[dict] = []
    for tbl, types in type_counts.items():
        for et, n in types.items():
            mapped = MAPPING.get(et, ("<unmapped>",
                                      "gap — document only"))
            mapping_rows.append({
                "entity_source_table": tbl,
                "entity_type":         et,
                "row_count":           n,
                "target_v2_column":    mapped[0],
                "absorption_rule":     mapped[1],
            })
    mapping_rows.sort(key=lambda r: (-r["row_count"], r["entity_source_table"]))

    log("\n=== Phase 1 mapping table preview (top 30 by row count) ===")
    log(f"  {'source_table':37s} {'entity_type':30s} {'rows':>6s}  "
        f"{'target_col':25s}  rule")
    for r in mapping_rows[:30]:
        log(f"  {r['entity_source_table']:37s} {r['entity_type']:30s} "
            f"{r['row_count']:>6}  {r['target_v2_column']:25s}  "
            f"{r['absorption_rule']}")

    if not args.commit:
        log("dry-run — pass --commit to write mapping table.")
        return 0

    # 3. write mapping table
    con.execute(f"""
CREATE OR REPLACE TABLE {TARGET} (
    entity_source_table   VARCHAR,
    entity_type           VARCHAR,
    row_count             BIGINT,
    target_v2_column      VARCHAR,
    absorption_rule       VARCHAR
)""")
    for r in mapping_rows:
        con.execute(
            f"INSERT INTO {TARGET} VALUES (?, ?, ?, ?, ?)",
            [r["entity_source_table"], r["entity_type"],
             r["row_count"], r["target_v2_column"], r["absorption_rule"]],
        )
    n = con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()[0]
    n_mapped = con.execute(
        f"SELECT COUNT(*) FROM {TARGET} "
        f"WHERE target_v2_column NOT IN ('<gap>','<non_us>','<unmapped>')"
    ).fetchone()[0]
    n_gap = n - n_mapped
    log(f"  mapping table written: {n} rows total, {n_mapped} mapped, "
        f"{n_gap} gap/non_us/unmapped")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "type_counts": type_counts,
        "mapping_rows": mapping_rows,
        "n_total": n, "n_mapped": n_mapped, "n_gap": n_gap,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
