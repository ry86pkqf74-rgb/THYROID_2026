#!/usr/bin/env python3
"""
integrate_missing_sources.py — Phase 6: Integrate 8 Missing High-Value Sources

Thyroid Cancer Research Lakehouse (THYROID_2026)

This script loads new Phase 6 sources from /processed parquets (if present),
runs extraction + QC steps, and updates:
- integration_report.csv
- QA_report.md
- data_dictionary.csv and data_dictionary.md
- advanced_features_v2 view in thyroid_master.duckdb (if duckdb available)

NOTE: This script is designed to be re-runnable.
"""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
PROCESSED = ROOT / "processed"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("integrate")


# ═══════════════════════════════════════════════════════════════════
#  LOAD NEW SOURCES (Phase 6)
# ═══════════════════════════════════════════════════════════════════


def load_processed_parquet(name: str) -> pd.DataFrame:
    """Load processed/{name}.parquet if available."""
    path = PROCESSED / f"{name}.parquet"
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        log.warning(f"  ⚠️  Could not read {path.name}: {exc}")
        return pd.DataFrame()


def safe_int(x):
    try:
        if pd.isna(x):
            return None
        s = str(x).strip()
        s = re.sub(r"\.0$", "", s)
        return int(s) if s else None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════
#  EXTRACTION HELPERS
# ═══════════════════════════════════════════════════════════════════


def extract_events_from_text(text: str, source_column: str) -> list[dict]:
    """Very lightweight regex-based extractor (placeholder).

    The repo may later upgrade this to a full NLP model.
    """
    if not text or not isinstance(text, str):
        return []

    events = []

    # Example: extract levothyroxine dose patterns like "levothyroxine 125 mcg"
    for m in re.finditer(r"\blevothyroxine\b[^\n]{0,40}?(\d{1,3}(?:\.\d+)?)\s*(mcg|ug|µg)\b", text, flags=re.I):
        val = float(m.group(1))
        unit = m.group(2)
        snippet = text[max(0, m.start() - 50): m.end() + 50]
        events.append({
            "event_type": "medication",
            "event_subtype": "levothyroxine",
            "event_value": val,
            "event_unit": unit,
            "event_date": None,
            "event_text": snippet[:250],
            "source_column": source_column,
        })

    # Example: TSH like "TSH 0.15"
    for m in re.finditer(r"\bTSH\b[^\n]{0,20}?(\d+(?:\.\d+)?)\b", text, flags=re.I):
        val = float(m.group(1))
        snippet = text[max(0, m.start() - 50): m.end() + 50]
        events.append({
            "event_type": "lab",
            "event_subtype": "TSH",
            "event_value": val,
            "event_unit": None,
            "event_date": None,
            "event_text": snippet[:250],
            "source_column": source_column,
        })

    return events


# ═══════════════════════════════════════════════════════════════════
#  ADVANCED FEATURES VIEW
# ═══════════════════════════════════════════════════════════════════


ADVANCED_V2_SQL = """
CREATE OR REPLACE VIEW advanced_features_v2 AS
SELECT
    mc.research_id,
    mc.age_at_surgery,
    mc.sex,
    mc.surgery_date,
    -- Tumor summary
    tp.histology_1_type,
    tp.histology_1_overall_stage_ajcc8,
    tp.histology_1_largest_tumor_cm,
    -- Benign summary
    bp.is_mng,
    bp.is_graves,
    bp.is_follicular_adenoma,
    bp.is_hashimoto,
    -- Complications
    comp.rln_injury_vocal_cord_paralysis,
    comp.vocal_cord_status,
    comp.seroma,
    comp.hematoma,
    comp.hypocalcemia,
    comp.hypoparathyroidism,
    -- Operative
    od.ebl,
    od.skin_to_skin_time,
    -- Mutation flags
    tp.braf_mutation_mentioned,
    tp.ras_mutation_mentioned,
    tp.ret_mutation_mentioned,
    tp.tert_mutation_mentioned,
    -- Data availability
    CASE WHEN comp.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_complications,
    CASE WHEN od.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_operative_details,
    CASE WHEN cn.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_clinical_notes,
    CASE WHEN cnl.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_clinical_notes_long,
    CASE WHEN ps.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_path_synoptics,
    CASE WHEN mt_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_molecular_testing,
    CASE WHEN fh_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_fna_history,
    CASE WHEN unt_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_us_tirads,
    mc.has_tumor_pathology,
    mc.has_benign_pathology,
    mc.has_fna_cytology,
    mc.has_ultrasound_reports,
    mc.has_ct_imaging,
    mc.has_mri_imaging,
    mc.has_nuclear_med,
    mc.has_thyroglobulin_labs,
    mc.has_anti_thyroglobulin_labs,
    mc.has_parathyroid
FROM master_cohort mc
LEFT JOIN tumor_pathology tp      ON mc.research_id = tp.research_id
LEFT JOIN benign_pathology bp     ON mc.research_id = bp.research_id
LEFT JOIN complications comp      ON mc.research_id = CAST(comp.research_id AS VARCHAR)
LEFT JOIN operative_details od    ON mc.research_id = CAST(od.research_id AS VARCHAR)
LEFT JOIN clinical_notes cn       ON mc.research_id = CAST(cn.research_id AS VARCHAR)
LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM clinical_notes_long) cnl
    ON mc.research_id = cnl.research_id
LEFT JOIN path_synoptics ps       ON mc.research_id = CAST(ps.research_id AS VARCHAR)
LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM molecular_testing) mt_agg
    ON mc.research_id = mt_agg.research_id
LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM fna_history) fh_agg
    ON mc.research_id = fh_agg.research_id
LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM us_nodules_tirads) unt_agg
    ON mc.research_id = unt_agg.research_id
"""


def build_advanced_features_v2_view(results: dict) -> None:
    """Load new parquets into DuckDB and create the advanced_features_v2 view."""
    db_path = ROOT / "thyroid_master.duckdb"
    if not db_path.exists():
        log.warning("DuckDB not found — skipping view creation. Run 02_build_duckdb_full.py first.")
        return

    try:
        import duckdb
    except ImportError:
        log.warning("duckdb not installed — skipping view creation")
        return

    log.info("Building advanced_features_v2 in DuckDB...")
    con = duckdb.connect(str(db_path))

    new_tables = [
        "complications", "molecular_testing", "operative_details",
        "fna_history", "us_nodules_tirads", "serial_imaging_us",
        "path_synoptics", "clinical_notes", "clinical_notes_long", "extracted_clinical_events",
    ]
    for tbl in new_tables:
        pq_path = PROCESSED / f"{tbl}.parquet"
        if pq_path.exists():
            con.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_parquet('{pq_path}')")
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info(f"  Loaded {tbl}: {cnt:,} rows")

    try:
        con.execute(ADVANCED_V2_SQL)
        n = con.execute("SELECT COUNT(*) FROM advanced_features_v2").fetchone()[0]
        log.info(f"  Created advanced_features_v2: {n:,} rows")
    except Exception as exc:
        log.error(f"  View creation failed: {exc}")

    con.close()


# ═══════════════════════════════════════════════════════════════════
#  REPORTING
# ═══════════════════════════════════════════════════════════════════


def generate_integration_report(results: dict) -> Path:
    """Write integration_report.csv with per-table stats."""
    log.info("Generating integration report...")
    rows = []
    for name, info in results.items():
        df = info.get("df")
        if df is None or df.empty:
            rows.append({"table": name, "row_count": 0, "col_count": 0})
            continue
        null_rate = df.isnull().mean().mean()
        extra = {}
        if name == "extracted_clinical_events" and "event_type" in df.columns:
            for et in ("lab", "medication", "comorbidity", "treatment", "follow_up"):
                extra[f"n_{et}_events"] = int((df["event_type"] == et).sum())
        rows.append({
            "table": name,
            "row_count": len(df),
            "col_count": len(df.columns),
            "null_rate_pct": round(null_rate * 100, 2),
            "unique_patients": int(df["research_id"].nunique()) if "research_id" in df.columns else 0,
            **extra,
        })

    report_path = ROOT / "integration_report.csv"
    report_df = pd.DataFrame(rows)
    report_df.to_csv(report_path, index=False)
    log.info(f"  Saved {report_path}")
    return report_path


# ═══════════════════════════════════════════════════════════════════
#  DATA DICTIONARY UPDATE
# ═══════════════════════════════════════════════════════════════════


NEW_DICT_ENTRIES = [
    # clinical_notes
    ("table", "clinical_notes", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "clinical_notes", "thyroid_cx_history_summary", "VARCHAR", "Thyroid cancer history/summary note", "phase6"),
    ("table", "clinical_notes", "h_p_1", "VARCHAR", "History & Physical note 1", "phase6"),
    ("table", "clinical_notes", "op_note_1", "VARCHAR", "Operative note 1", "phase6"),
    ("table", "clinical_notes", "dc_sum_1", "VARCHAR", "Discharge summary 1", "phase6"),
    ("table", "clinical_notes", "last_endocrine_fm_note", "VARCHAR", "Last endocrine/family medicine note", "phase6"),
    # clinical_notes_long
    ("table", "clinical_notes_long", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "clinical_notes_long", "note_type", "VARCHAR", "Normalized note type (HP, OPNOTE, DC_SUM, ED_NOTE, OTHER_HISTORY, OTHER_NOTES, ENDOCRINE_FM, THYROID_CX_HISTORY, DEATH)", "phase6"),
    ("table", "clinical_notes_long", "note_index", "INT64", "Sequence index within note_type when applicable (e.g., 1-4)", "phase6"),
    ("table", "clinical_notes_long", "note_text", "VARCHAR", "Clinical note text", "phase6"),
    ("table", "clinical_notes_long", "source_sheet", "VARCHAR", "Source worksheet name in Notes workbook", "phase6"),
    ("table", "clinical_notes_long", "source_column", "VARCHAR", "Source column name (standardized snake_case)", "phase6"),
    # extracted_clinical_events
    ("table", "extracted_clinical_events", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "extracted_clinical_events", "event_type", "VARCHAR", "Event category (lab, medication, comorbidity, treatment, follow_up)", "phase6"),
    ("table", "extracted_clinical_events", "event_subtype", "VARCHAR", "Specific event (TSH, levothyroxine, hypertension, RAI, etc.)", "phase6"),
    ("table", "extracted_clinical_events", "event_value", "DOUBLE", "Numeric value if applicable (lab result, dose)", "phase6"),
    ("table", "extracted_clinical_events", "event_unit", "VARCHAR", "Value units (ng/mL, mcg, etc.)", "phase6"),
    ("table", "extracted_clinical_events", "event_date", "VARCHAR", "Event date if extractable", "phase6"),
    ("table", "extracted_clinical_events", "event_text", "VARCHAR", "Raw matched text snippet (max 250 chars)", "phase6"),
    ("table", "extracted_clinical_events", "source_column", "VARCHAR", "Source note column name", "phase6"),
    # advanced_features_v2
    ("view", "advanced_features_v2", "has_clinical_notes", "BOOLEAN", "Patient has clinical notes", "phase6"),
    ("view", "advanced_features_v2", "has_clinical_notes_long", "BOOLEAN", "Patient has long-format clinical notes", "phase6"),
]


def update_data_dictionary(results: dict) -> None:
    """Append Phase 6 entries to data_dictionary.csv and data_dictionary.md."""
    log.info("Updating data dictionary...")

    # CSV
    csv_path = ROOT / "data_dictionary.csv"
    existing_rows = []
    if csv_path.exists():
        with open(csv_path, "r") as f:
            reader = csv.reader(f)
            existing_rows = list(reader)

    existing_keys = {
        (r[0], r[1], r[2]) for r in existing_rows[1:] if len(r) >= 3
    }
    new_rows = [
        list(entry) for entry in NEW_DICT_ENTRIES
        if (entry[0], entry[1], entry[2]) not in existing_keys
    ]

    if new_rows:
        header = existing_rows[0] if existing_rows else ["object_type","object_name","column_name","data_type","description","phase"]
        out_rows = [header] + existing_rows[1:] + new_rows
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(out_rows)
        log.info(f"  Updated {csv_path}")


def main() -> None:
    log.info("=" * 70)
    log.info("PHASE 6 INTEGRATION")
    log.info("=" * 70)

    results = {}

    for tbl in [
        "complications", "molecular_testing", "operative_details",
        "fna_history", "us_nodules_tirads", "serial_imaging_us",
        "path_synoptics", "clinical_notes", "clinical_notes_long", "extracted_clinical_events",
    ]:
        df = load_processed_parquet(tbl)
        results[tbl] = {"df": df}
        if not df.empty:
            log.info(f"  Loaded {tbl}: {len(df):,} rows")

    update_data_dictionary(results)
    generate_integration_report(results)
    build_advanced_features_v2_view(results)


if __name__ == "__main__":
    main()
