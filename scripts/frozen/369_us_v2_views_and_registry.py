#!/usr/bin/env python3
"""Script 369 — Wide views + detail_table_registry_v1 entries (Phase 7).

Generates three wide pivot views in views_readable schema:
  * US_Nodules_Wide_v2        — exams 1-5 × nodules 1-8 = 40 cells × 8 fields
  * US_Thyroid_Gland_Wide_v2  — one row per patient, exams 1-5
  * US_Lymph_Nodes_Wide_v2    — exams 1-5 × LNs 1-8, all us_ prefixed

And inserts registry rows for the 5 new canonical tables. Registry has 13
columns (probe-confirmed) — feeds_master_columns_array (VARCHAR[]) +
needs_manual_review (BOOLEAN) beyond the older 247/236 pattern.
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

SCRIPT_TAG = "Script 369"
PUB = PUBLICATION_DB
VIEWS_SCHEMA = "views_readable"
REGISTRY = f"{PUB}.manuscript_workspace.detail_table_registry_v1"

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"369_us_v2_views_registry_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


# ──────────────────────────────────────────────────────────────────────────
# Wide nodule view: exam_rank 1..5 × nodule_index 1..8 × 8 fields
# ──────────────────────────────────────────────────────────────────────────

NODULE_FIELDS = [
    ("size_cm",     "size_cm_max",         "DOUBLE"),
    ("laterality",  "laterality",          "VARCHAR"),
    ("tirads",      "tirads_category_v2",  "VARCHAR"),
    ("composition", "composition",         "VARCHAR"),
    ("echogenicity", "echogenicity",       "VARCHAR"),
    ("shape",       "shape",               "VARCHAR"),
    ("margins",     "margins",             "VARCHAR"),
    ("calcifications", "calcifications",   "VARCHAR"),
]
EXAM_RANKS = (1, 2, 3, 4, 5)
NODULE_INDICES = tuple(range(1, 9))


def _nodules_wide_sql() -> str:
    select_parts: list[str] = ["research_id"]
    # exam date columns
    for r in EXAM_RANKS:
        select_parts.append(
            f"MAX(CASE WHEN us_exam_rank={r} THEN exam_date END) "
            f"AS us_{r}_date"
        )
    # per (exam, nodule) field columns
    for r in EXAM_RANKS:
        for k in NODULE_INDICES:
            for short, source_col, _ in NODULE_FIELDS:
                col = f"us_{r}_nodule_{k}_{short}"
                select_parts.append(
                    f"MAX(CASE WHEN us_exam_rank={r} AND nodule_index_within_exam={k} "
                    f"THEN {source_col} END) AS {col}"
                )
    select_clause = ",\n  ".join(select_parts)
    # Join on (research_id, exam_date) — sub-tables generated us_exam_id
    # independently with different hashes, so we use the date as the
    # canonical exam key here.
    return f"""
CREATE OR REPLACE VIEW {PUB}.{VIEWS_SCHEMA}.US_Nodules_Wide_v2 AS
WITH ranked AS (
    SELECT n.*, e.exam_rank_for_patient AS us_exam_rank
    FROM {PUB}.main.canonical_us_nodule_v2 n
    JOIN {PUB}.main.canonical_us_exam_master_v2 e
      ON n.research_id = e.research_id
     AND n.exam_date  = e.exam_date
    WHERE n.is_aggregate_row IS NOT TRUE
)
SELECT
  {select_clause}
FROM ranked
GROUP BY research_id;
"""


# ──────────────────────────────────────────────────────────────────────────
# Wide gland view: exams 1-5 × gland fields
# ──────────────────────────────────────────────────────────────────────────

GLAND_FIELDS = [
    ("rl_volume_ml",         "rl_volume_ml",         "DOUBLE"),
    ("ll_volume_ml",         "ll_volume_ml",         "DOUBLE"),
    ("isthmus_thickness_mm", "isthmus_thickness_mm", "DOUBLE"),
    ("total_volume_ml",      "total_thyroid_volume_ml", "DOUBLE"),
    ("clinical_impression",  "clinical_impression_text", "VARCHAR"),
    ("recommendation",       "recommendation_text",  "VARCHAR"),
    ("radiologist",          "radiologist",          "VARCHAR"),
]


def _gland_wide_sql() -> str:
    parts: list[str] = ["research_id"]
    for r in EXAM_RANKS:
        parts.append(
            f"MAX(CASE WHEN us_exam_rank={r} THEN exam_date END) "
            f"AS us_{r}_date"
        )
    for r in EXAM_RANKS:
        for short, source_col, _ in GLAND_FIELDS:
            col = f"us_{r}_gland_{short}"
            parts.append(
                f"MAX(CASE WHEN us_exam_rank={r} THEN {source_col} END) AS {col}"
            )
    sel = ",\n  ".join(parts)
    return f"""
CREATE OR REPLACE VIEW {PUB}.{VIEWS_SCHEMA}.US_Thyroid_Gland_Wide_v2 AS
WITH ranked AS (
    SELECT g.*, e.exam_rank_for_patient AS us_exam_rank
    FROM {PUB}.main.canonical_us_thyroid_gland_v2 g
    LEFT JOIN {PUB}.main.canonical_us_exam_master_v2 e
      ON g.research_id = e.research_id
     AND g.exam_date  = e.exam_date
    WHERE e.exam_rank_for_patient IS NOT NULL
)
SELECT
  {sel}
FROM ranked
GROUP BY research_id;
"""


# ──────────────────────────────────────────────────────────────────────────
# Wide LN view (US-only by construction): exams 1-5 × LN 1-8 × fields
# ──────────────────────────────────────────────────────────────────────────

LN_FIELDS = [
    ("size_cm",            "size_cm_max",          "DOUBLE"),
    ("laterality",         "laterality",           "VARCHAR"),
    ("neck_level",         "neck_level",           "VARCHAR"),
    ("suspicion",          "suspicion_level",      "VARCHAR"),
    ("suspicious_flag",    "suspicious_flag",      "BOOLEAN"),
    ("source_note_type",   "source_note_type",     "VARCHAR"),
    ("evidence_text",      "evidence_text",        "VARCHAR"),
]
LN_INDICES = tuple(range(1, 9))


def _ln_wide_sql() -> str:
    parts: list[str] = ["research_id"]
    for r in EXAM_RANKS:
        parts.append(
            f"MAX(CASE WHEN us_exam_rank={r} THEN exam_date END) "
            f"AS us_{r}_date"
        )
    for r in EXAM_RANKS:
        for j in LN_INDICES:
            for short, source_col, _ in LN_FIELDS:
                col = f"us_{r}_ln_{j}_{short}"
                parts.append(
                    f"MAX(CASE WHEN us_exam_rank={r} AND us_ln_index_within_exam={j} "
                    f"THEN {source_col} END) AS {col}"
                )
    sel = ",\n  ".join(parts)
    return f"""
CREATE OR REPLACE VIEW {PUB}.{VIEWS_SCHEMA}.US_Lymph_Nodes_Wide_v2 AS
WITH ranked AS (
    SELECT l.*, e.exam_rank_for_patient AS us_exam_rank
    FROM {PUB}.main.canonical_us_lymph_node_v2 l
    LEFT JOIN {PUB}.main.canonical_us_exam_master_v2 e
      ON l.research_id = e.research_id
     AND l.exam_date  = e.exam_date
    WHERE e.exam_rank_for_patient IS NOT NULL
)
SELECT
  {sel}
FROM ranked
GROUP BY research_id;
"""


# ──────────────────────────────────────────────────────────────────────────
# Registry rows
# ──────────────────────────────────────────────────────────────────────────

REGISTRY_ROWS: list[dict] = [
    {
        "detail_table_name": "canonical_us_nodule_v2",
        "schema_name": "main",
        "join_key": "research_id, us_exam_id, nodule_index_within_exam",
        "grain": "one row per (research_id, us_exam_id, nodule_index_within_exam)",
        "domain": "imaging_us_nodule",
        "feeds_master_columns": (
            "n_us_nodules_total_v2, dominant_nodule_size_cm_v2, "
            "imaging_tirads_best_v2, imaging_tirads_worst_v2, "
            "imaging_tirads_category_v2_v2, imaging_laterality_rollup_v2, "
            "max_tirads_ever_v2"
        ),
        "feeds_master_columns_secondary": "preop_tirads_best_v2, preop_tirads_category_v2",
        "feeds_master_columns_array": [
            "n_us_nodules_total_v2", "dominant_nodule_size_cm_v2",
            "imaging_tirads_best_v2", "imaging_tirads_worst_v2",
            "imaging_tirads_category_v2_v2", "imaging_laterality_rollup_v2",
            "max_tirads_ever_v2",
        ],
        "description": (
            "US v2 master per-nodule. Built 2026-04-21 by Script 362 from "
            "cunc_v1 + cunm_v1 + tirads_v2_nodules_raw. nlp_backfill_pending "
            "flags rows with no parsed-source coverage. No LLM run."
        ),
        "canonical_version": "v2",
        "needs_manual_review": True,
    },
    {
        "detail_table_name": "canonical_us_thyroid_gland_v2",
        "schema_name": "main",
        "join_key": "research_id, us_exam_id, exam_date",
        "grain": "one row per (research_id, us_exam_id, exam_date)",
        "domain": "imaging_us_gland",
        "feeds_master_columns": "us_v2_any_nlp_backfill_pending",
        "feeds_master_columns_secondary": "",
        "feeds_master_columns_array": ["us_v2_any_nlp_backfill_pending"],
        "description": (
            "US gland (non-nodule) per-exam findings from ultrasound_reports "
            "regex parse + us_nodules_tirads shell rows. Parenchyma fields "
            "NULL with nlp_backfill_pending=TRUE on every row. No LLM run."
        ),
        "canonical_version": "v2",
        "needs_manual_review": True,
    },
    {
        "detail_table_name": "canonical_us_lymph_node_v2",
        "schema_name": "main",
        "join_key": "research_id, us_exam_id, us_ln_index_within_exam",
        "grain": "one row per US LN observation",
        "domain": "imaging_us_lymph_node",
        "feeds_master_columns": (
            "lnus_abnormal_any_exam_v2, lnus_n_us_with_ln_assessment_v2, "
            "lnus_first_abnormal_us_ln_date_v2"
        ),
        "feeds_master_columns_secondary": "",
        "feeds_master_columns_array": [
            "lnus_abnormal_any_exam_v2",
            "lnus_n_us_with_ln_assessment_v2",
            "lnus_first_abnormal_us_ln_date_v2",
        ],
        "description": (
            "Ultrasound-sourced LN findings only. source_modality=US enforced "
            "via CHECK constraint. NOT for path/CT/PET/MR/nucmed LN — those "
            "live in parallel canonical_<modality>_lymph_node_v2 tables."
        ),
        "canonical_version": "v2",
        "needs_manual_review": True,
    },
    {
        "detail_table_name": "canonical_us_exam_master_v2",
        "schema_name": "main",
        "join_key": "research_id, us_exam_id, exam_date",
        "grain": "one row per (research_id, us_exam_id, exam_date)",
        "domain": "imaging_us_exam",
        "feeds_master_columns": "n_us_exams_v2",
        "feeds_master_columns_secondary": "",
        "feeds_master_columns_array": ["n_us_exams_v2"],
        "description": (
            "US v2 per-exam master. LN columns US-prefixed for future "
            "modality compatibility (us_ln_*)."
        ),
        "canonical_version": "v2",
        "needs_manual_review": False,
    },
    {
        "detail_table_name": "canonical_us_patient_master_v2",
        "schema_name": "main",
        "join_key": "research_id",
        "grain": "one row per research_id (with any US exam)",
        "domain": "imaging_us_patient",
        "feeds_master_columns": "n_us_exams_v2",
        "feeds_master_columns_secondary": "",
        "feeds_master_columns_array": ["n_us_exams_v2"],
        "description": (
            "US v2 per-patient master. has_us_ln_findings_ever, "
            "any_suspicious_us_ln_ever, first_abnormal_us_ln_date are "
            "US-prefixed for future modality patient masters."
        ),
        "canonical_version": "v2",
        "needs_manual_review": False,
    },
]


def _get_table_metrics(con, table_name: str) -> tuple[int, int]:
    fq = f"{PUB}.main.{table_name}"
    n = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
    cols = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = 'main' AND table_name = ?",
        [PUB, table_name],
    ).fetchall()
    has_rid = any(c[0] == "research_id" for c in cols)
    if has_rid:
        npts = con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM {fq}"
        ).fetchone()[0]
    else:
        npts = 0
    return n, npts


def upsert_registry_rows(con) -> int:
    upserted = 0
    for row in REGISTRY_ROWS:
        n, npts = _get_table_metrics(con, row["detail_table_name"])
        # delete any existing row for this canonical+version
        con.execute(
            f"DELETE FROM {REGISTRY} "
            f"WHERE detail_table_name = ? AND canonical_version = ?",
            [row["detail_table_name"], row["canonical_version"]],
        )
        con.execute(
            f"""INSERT INTO {REGISTRY} (
                detail_table_name, schema_name, join_key, grain,
                total_rows, total_patients, domain,
                feeds_master_columns, description, canonical_version,
                feeds_master_columns_secondary, feeds_master_columns_array,
                needs_manual_review
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                row["detail_table_name"],
                row["schema_name"],
                row["join_key"],
                row["grain"],
                n,
                npts,
                row["domain"],
                row["feeds_master_columns"],
                row["description"],
                row["canonical_version"],
                row["feeds_master_columns_secondary"],
                row["feeds_master_columns_array"],
                row["needs_manual_review"],
            ],
        )
        upserted += 1
    return upserted


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    if not args.commit:
        log("dry-run only.")
        return 0

    log(f"  CREATE SCHEMA IF NOT EXISTS {PUB}.{VIEWS_SCHEMA}")
    con.execute(f'CREATE SCHEMA IF NOT EXISTS {PUB}.{VIEWS_SCHEMA}')

    log("  CREATE OR REPLACE VIEW US_Nodules_Wide_v2")
    con.execute(_nodules_wide_sql())
    log("  CREATE OR REPLACE VIEW US_Thyroid_Gland_Wide_v2")
    con.execute(_gland_wide_sql())
    log("  CREATE OR REPLACE VIEW US_Lymph_Nodes_Wide_v2")
    con.execute(_ln_wide_sql())

    log("  upsert detail_table_registry_v1 rows")
    n = upsert_registry_rows(con)
    log(f"    registry rows upserted: {n}")

    # Smoke verify
    counts = {
        v: con.execute(
            f"SELECT COUNT(*) FROM {PUB}.{VIEWS_SCHEMA}.{v}"
        ).fetchone()[0]
        for v in (
            "US_Nodules_Wide_v2",
            "US_Thyroid_Gland_Wide_v2",
            "US_Lymph_Nodes_Wide_v2",
        )
    }
    log(f"  view row counts (one per patient): {counts}")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "registry_upserts": n,
        "view_row_counts": counts,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
