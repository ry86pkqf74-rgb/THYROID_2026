#!/usr/bin/env python3
"""Script 377 — Phase 2: absorb LLM entities into canonical_us_nodule_v2.

Reads us_llm_absorption_mapping_v1 (Script 376), parses entities from
result_json, and applies absorption per Case A/B/C:

  Case A — patient has exactly 1 v2 nodule row:
            UPDATE the row with COALESCE on every mapped target column.
  Case B — patient has 0 v2 nodule rows:
            INSERT one new row per distinct (research_id, exam_date) seen
            in the LLM entities, with parsed structured fields. Use an
            existing v2 us_exam_id when one exists for that (rid, date),
            otherwise derive via gland/LN recipe md5(rid||'|'||date).
  Case C — patient has >= 2 v2 nodule rows:
            Defer — write to manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1.

Source-of-truth: only the 18 mapped entity types from Script 376.
Non-US (<non_us>) and gap (<gap>) types are NOT absorbed.

PHI safety: only research_id, exam_date, and structured entity_value strings
flow through. evidence_text is held verbatim only as needed by parsing and
never logged.
"""
from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
SCRIPT_TAG = "Script 377"
V2 = f"{PUB}.main.canonical_us_nodule_v2"
MAPPING = f"{PUB}.manuscript_workspace.us_llm_absorption_mapping_v1"
DEFERRED = f"{PUB}.manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1"
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"377_absorb_llm_{RUN_TS}.json"

LLM_TABLES = [
    "note_entities_llm_tirads_granular",
    "note_entities_llm_us_nodule_dynamics",
    "note_entities_llm_imaging",
]


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def build_entities_view(con) -> None:
    """Parse all entities from all 3 LLM tables into a single staging table.

    Some result_json values are non-array scalars (e.g. plain "5.5" or
    {"error": ...}); we pre-filter via a CTE so the CAST AS JSON[] never
    sees those rows.
    """
    union_sql = " UNION ALL ".join(f"""
SELECT
  '{tbl}' AS source_table,
  TRY_CAST(research_id AS INTEGER) AS research_id,
  TRY_CAST(linkage_date AS DATE)   AS linkage_date,
  note_row_id,
  json_extract_string(e, '$.entity_type')        AS entity_type,
  json_extract_string(e, '$.entity_value')       AS entity_value,
  json_extract_string(e, '$.entity_date')        AS entity_date_text,
  json_extract_string(e, '$.present_or_negated') AS present_or_negated,
  TRY_CAST(json_extract_string(e, '$.confidence') AS DOUBLE) AS confidence
FROM (
  SELECT research_id, linkage_date, note_row_id,
         TRY_CAST(result_json AS JSON) AS j
  FROM {PUB}.main.{tbl}
  WHERE result_json IS NOT NULL
    AND TRY_CAST(result_json AS JSON) IS NOT NULL
    AND json_type(TRY_CAST(result_json AS JSON), '$.entities') = 'ARRAY'
    AND json_array_length(json_extract(TRY_CAST(result_json AS JSON), '$.entities')) > 0
    AND TRY_CAST(research_id AS INTEGER) IS NOT NULL
) v
   , LATERAL (
       SELECT UNNEST(CAST(json_extract(v.j, '$.entities') AS JSON[])) AS e
     )
""" for tbl in LLM_TABLES)

    con.execute(f"""
CREATE OR REPLACE TEMP TABLE _llm_entities_staging AS
SELECT s.*,
       TRY_CAST(s.entity_date_text AS DATE) AS entity_date
FROM ({union_sql}) s
WHERE s.entity_type IS NOT NULL
""")
    n = con.execute(
        "SELECT COUNT(*) FROM _llm_entities_staging"
    ).fetchone()[0]
    log(f"  staged {n:,} parsed entity rows across 3 LLM tables")


# ──────────────────────────────────────────────────────────────────────────
# Parsers (Python-side because regex on entity_value is needed)
# ──────────────────────────────────────────────────────────────────────────

_RE_SIZE_CM = re.compile(r"(\d+\.?\d*)\s*(cm|mm)", re.I)
_RE_TIRADS = re.compile(r"\b(?:TR|TI[- ]?RADS)\s*([1-5])\b", re.I)


def parse_size_to_cm(value: str | None) -> float | None:
    if not value:
        return None
    m = _RE_SIZE_CM.search(value)
    if not m:
        return None
    val = float(m.group(1))
    unit = m.group(2).lower()
    return val / 10.0 if unit == "mm" else val


def parse_tirads_int(value: str | None) -> int | None:
    if not value:
        return None
    m = _RE_TIRADS.search(value)
    if not m:
        # Maybe just a digit
        if value.strip().isdigit():
            n = int(value.strip())
            if 1 <= n <= 5:
                return n
        return None
    return int(m.group(1))


def parse_tirads_category(value: str | None) -> str | None:
    n = parse_tirads_int(value)
    return f"TR{n}" if n is not None else None


# Each entry: entity_type → (target_col, parser_callable, dtype_label)
# parser_callable returns the value to write (or None to skip)
ABSORB_RULES: dict[str, tuple[str, callable, str]] = {
    # tirads point components → string features
    "composition":                  ("composition",            lambda v: v, "VARCHAR"),
    "echogenicity":                 ("echogenicity",           lambda v: v, "VARCHAR"),
    "shape":                        ("shape",                  lambda v: v, "VARCHAR"),
    "margins":                      ("margins",                lambda v: v, "VARCHAR"),
    "margin":                       ("margins",                lambda v: v, "VARCHAR"),
    "echogenic_foci":               ("echogenic_foci",         lambda v: v, "VARCHAR"),
    "tirads_component_composition": ("composition",            lambda v: v, "VARCHAR"),
    "tirads_component_echogenicity":("echogenicity",           lambda v: v, "VARCHAR"),
    "tirads_component_shape":       ("shape",                  lambda v: v, "VARCHAR"),
    "tirads_component_margin":      ("margins",                lambda v: v, "VARCHAR"),
    "tirads_component_foci":        ("echogenic_foci",         lambda v: v, "VARCHAR"),
    # imaging-table US-relevant
    "ultrasound_thyroid":           ("comparison_statement",   lambda v: v, "VARCHAR"),
    "nodule_size":                  ("size_cm_max",            parse_size_to_cm, "DOUBLE"),
    "nodule_dimensions":            ("size_cm_max",            parse_size_to_cm, "DOUBLE"),
    "nodule_location":              ("location_raw",           lambda v: v, "VARCHAR"),
    "tirads_score":                 ("tirads_reported_in_text", parse_tirads_int, "INTEGER"),
    "tirads":                       ("tirads_reported_in_text", parse_tirads_int, "INTEGER"),
    "tirads_category":              ("updated_tirads_category", parse_tirads_category, "VARCHAR"),
    "nodule_volume":                ("volume_ml",              parse_size_to_cm, "DOUBLE"),
    "nodule_identifier":            ("location_raw",           lambda v: v, "VARCHAR"),
    "nodule":                       ("location_raw",           lambda v: v, "VARCHAR"),
    "fna":                          ("fna_recommended_this_nodule",
                                     lambda v: True, "BOOLEAN"),
}


# ──────────────────────────────────────────────────────────────────────────
# Phase 2 main
# ──────────────────────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    v2_count_before = con.execute(f"SELECT COUNT(*) FROM {V2}").fetchone()[0]
    v2_pts_before = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {V2}"
    ).fetchone()[0]
    log(f"  v2 baseline: rows={v2_count_before:,} pts={v2_pts_before:,}")

    log("stage parsed entities (in-session temp table)")
    build_entities_view(con)

    # Restrict to mapped entity types only
    mapped_types = list(ABSORB_RULES.keys())
    log(f"  mapped entity types in scope: {len(mapped_types)}")

    # Compute case classification per (research_id, source_table_kind)
    log("classify patients by Case A/B/C (relative to v2 nodule count)")
    con.execute(f"""
CREATE OR REPLACE TEMP TABLE _v2_pt_counts AS
SELECT research_id, COUNT(*) AS n_v2_rows
FROM {V2} GROUP BY 1
""")
    case_breakdown = con.execute(f"""
WITH llm_pts AS (
    SELECT DISTINCT research_id FROM _llm_entities_staging
    WHERE entity_type IN ({','.join("'" + t + "'" for t in mapped_types)})
)
SELECT
  CASE
    WHEN c.n_v2_rows IS NULL OR c.n_v2_rows = 0 THEN 'B_zero_v2_rows'
    WHEN c.n_v2_rows = 1 THEN 'A_single_v2_row'
    ELSE 'C_multi_v2_rows'
  END AS classification,
  COUNT(*) AS n_pts
FROM llm_pts l
LEFT JOIN _v2_pt_counts c USING (research_id)
GROUP BY 1 ORDER BY 1
""").fetchall()
    case_dict = dict(case_breakdown)
    log(f"  case breakdown (patients): {case_dict}")

    if not args.commit:
        log("dry-run only.")
        return 0

    # ── Case C: defer multi-nodule patients ────────────────────────────────
    log("write Case C deferrals → us_llm_absorption_deferred_multi_nodule_v1")
    con.execute(f"""
CREATE OR REPLACE TABLE {DEFERRED} AS
WITH llm_pts AS (
    SELECT research_id, COUNT(*) AS n_llm_entities
    FROM _llm_entities_staging
    WHERE entity_type IN ({','.join("'" + t + "'" for t in mapped_types)})
    GROUP BY 1
)
SELECT
  l.research_id,
  l.n_llm_entities,
  c.n_v2_rows AS n_v2_rows,
  CURRENT_TIMESTAMP AS deferred_at
FROM llm_pts l
JOIN _v2_pt_counts c USING (research_id)
WHERE c.n_v2_rows >= 2
""")
    n_deferred = con.execute(f"SELECT COUNT(*) FROM {DEFERRED}").fetchone()[0]
    log(f"  deferred patients (Case C): {n_deferred:,}")

    # ── Case A: COALESCE UPDATE for single-nodule patients ─────────────────
    log("absorb Case A entities (single-nodule patients) via COALESCE UPDATEs")
    case_a_updates = 0
    case_a_per_field: dict[str, int] = {}
    for et, (target_col, parser, dtype) in ABSORB_RULES.items():
        # Pull all rows for this entity_type for Case A patients
        rows = con.execute("""
SELECT s.research_id, s.entity_value
FROM _llm_entities_staging s
JOIN _v2_pt_counts c USING (research_id)
WHERE s.entity_type = ?
  AND c.n_v2_rows = 1
  AND COALESCE(s.present_or_negated, 'present') <> 'negated'
  AND s.entity_value IS NOT NULL
""", [et]).fetchall()
        if not rows:
            continue
        # Group by research_id → take first parseable value
        per_pt: dict[int, object] = {}
        for rid, val in rows:
            if rid in per_pt:
                continue
            parsed = parser(val)
            if parsed is None:
                continue
            per_pt[rid] = parsed
        if not per_pt:
            continue
        # Apply UPDATEs
        n_apply = 0
        for rid, val in per_pt.items():
            con.execute(
                f"""UPDATE {V2}
                       SET {target_col} = COALESCE({target_col}, ?)
                     WHERE research_id = ?
                       AND {target_col} IS NULL""",
                [val, rid],
            )
            n_apply += 1
        case_a_updates += n_apply
        case_a_per_field[target_col] = case_a_per_field.get(target_col, 0) + n_apply
    log(f"  Case A absorbed: {case_a_updates:,} field-level updates "
        f"({case_a_per_field})")

    # ── Case B: INSERT new v2 rows for zero-v2 patients ────────────────────
    log("absorb Case B entities (zero-v2 patients) via INSERTs")
    # For each (rid, exam_date) seen in LLM entities for Case B patients,
    # build one v2 row aggregating mapped fields. exam_date may be NULL
    # if the LLM never extracted a date.
    con.execute(f"""
CREATE OR REPLACE TEMP TABLE _case_b_groups AS
SELECT
  s.research_id,
  s.entity_date AS exam_date,
  ANY_VALUE(CASE WHEN s.entity_type IN ('composition','tirads_component_composition')
                 THEN s.entity_value END) AS composition,
  ANY_VALUE(CASE WHEN s.entity_type IN ('echogenicity','tirads_component_echogenicity')
                 THEN s.entity_value END) AS echogenicity,
  ANY_VALUE(CASE WHEN s.entity_type IN ('shape','tirads_component_shape')
                 THEN s.entity_value END) AS shape,
  ANY_VALUE(CASE WHEN s.entity_type IN ('margins','margin','tirads_component_margin')
                 THEN s.entity_value END) AS margins,
  ANY_VALUE(CASE WHEN s.entity_type IN ('echogenic_foci','tirads_component_foci')
                 THEN s.entity_value END) AS echogenic_foci,
  ANY_VALUE(CASE WHEN s.entity_type IN ('nodule_location','nodule_identifier','nodule')
                 THEN s.entity_value END) AS location_raw,
  ANY_VALUE(CASE WHEN s.entity_type = 'ultrasound_thyroid'
                 THEN s.entity_value END) AS comparison_statement,
  ANY_VALUE(CASE WHEN s.entity_type IN ('nodule_size','nodule_dimensions')
                 THEN s.entity_value END) AS size_text,
  ANY_VALUE(CASE WHEN s.entity_type IN ('tirads_score','tirads')
                 THEN s.entity_value END) AS tirads_text,
  ANY_VALUE(CASE WHEN s.entity_type = 'tirads_category'
                 THEN s.entity_value END) AS tirads_cat_text
FROM _llm_entities_staging s
WHERE s.entity_type IN ({','.join("'" + t + "'" for t in mapped_types)})
  AND s.research_id NOT IN (SELECT research_id FROM _v2_pt_counts)
GROUP BY 1, 2
""")

    n_b_groups = con.execute(
        "SELECT COUNT(*) FROM _case_b_groups"
    ).fetchone()[0]
    log(f"  Case B: {n_b_groups:,} (research_id, exam_date) groups to insert")

    # Build INSERT — we need to compute size_cm_max and tirads ints in Python
    rows = con.execute("""
SELECT research_id, exam_date,
       composition, echogenicity, shape, margins, echogenic_foci,
       location_raw, comparison_statement,
       size_text, tirads_text, tirads_cat_text
FROM _case_b_groups
""").fetchall()
    case_b_inserted = 0
    for r in rows:
        (rid, exam_date, composition, echogenicity, shape, margins,
         echogenic_foci, location_raw, comparison_statement,
         size_text, tirads_text, tirads_cat_text) = r
        size_cm = parse_size_to_cm(size_text)
        tirads_int = parse_tirads_int(tirads_text)
        tirads_cat = parse_tirads_category(tirads_cat_text)
        # Derive us_exam_id: prefer existing v2 hash for (rid,date)
        us_exam_id = None
        if exam_date is not None:
            existing = con.execute(
                f"SELECT us_exam_id FROM {V2} "
                f"WHERE research_id = ? AND exam_date = ? LIMIT 1",
                [rid, exam_date],
            ).fetchone()
            if existing:
                us_exam_id = existing[0]
            else:
                us_exam_id = con.execute(
                    "SELECT md5(? || '|' || ?)",
                    [str(rid), exam_date.isoformat()],
                ).fetchone()[0]
        else:
            us_exam_id = con.execute(
                "SELECT md5(? || '|' || '')", [str(rid)],
            ).fetchone()[0]
        nodule_id = con.execute(
            "SELECT md5(? || '|' || ? || '|nod1|llm_absorbed')",
            [str(rid), exam_date.isoformat() if exam_date else ""],
        ).fetchone()[0]
        # INSERT minimal v2 row (only provided cols + provenance flags)
        con.execute(f"""
INSERT INTO {V2} (
    research_id, us_exam_id, exam_date, nodule_index_within_exam, nodule_id,
    composition, echogenicity, shape, margins, echogenic_foci,
    size_cm_max, location_raw, comparison_statement,
    tirads_reported_in_text, updated_tirads_category,
    source_base, source_tirads_v2, source_tirads_llm,
    source_dynamics_llm, source_fna_linkage, source_us_nodules_tirads,
    is_aggregate_row, nlp_backfill_pending
) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          FALSE, FALSE, TRUE, FALSE, FALSE, FALSE,
          FALSE, FALSE)
""", [rid, us_exam_id, exam_date, nodule_id,
      composition, echogenicity, shape, margins, echogenic_foci,
      size_cm, location_raw, comparison_statement,
      tirads_int, tirads_cat])
        case_b_inserted += 1
    log(f"  Case B inserted: {case_b_inserted:,} new v2 rows")

    # Final summary
    v2_count_after = con.execute(f"SELECT COUNT(*) FROM {V2}").fetchone()[0]
    v2_pts_after = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {V2}"
    ).fetchone()[0]
    log(f"  v2 after: rows={v2_count_after:,} pts={v2_pts_after:,}  "
        f"(Δrows={v2_count_after - v2_count_before:+,d}, "
        f"Δpts={v2_pts_after - v2_pts_before:+,d})")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "v2_before": {"rows": v2_count_before, "patients": v2_pts_before},
        "v2_after":  {"rows": v2_count_after,  "patients": v2_pts_after},
        "case_breakdown_patients": case_dict,
        "case_a_updates_count": case_a_updates,
        "case_a_per_field": case_a_per_field,
        "case_b_inserted": case_b_inserted,
        "case_c_deferred": n_deferred,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
