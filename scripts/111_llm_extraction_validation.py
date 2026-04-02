#!/usr/bin/env python3
"""Build local lineage and side-by-side validation artifacts for LLM extraction.

Primary workflow:
  1. Read a completed LLM extraction parquet (default: processed/note_entities_llm.parquet)
  2. Join every extracted row back to clinical_notes_long via note_row_id
  3. Route each value into a comparison domain/token
  4. Compare against structured DuckDB tables and note_entities_* baseline
  5. Emit auditable artifacts under studies/llm_extraction_validation/runs/<label>/

Gold policy (see --gold-require-manual, --trust-fill-candidate-auto):
  - verification_status concordant: may enter gold with verified_by=auto unless --gold-require-manual
  - verification_status existing_missing_fill_candidate: enters gold only if verified_by=manual,
    unless --trust-fill-candidate-auto

MotherDuck: generates motherduck_setup.sql (ATTACH + read_parquet template); no cloud writes by default.

Outputs:
  - llm_lineage.parquet / .csv
  - llm_side_by_side.parquet / .csv (includes original_source_link, extraction_ts, verification_status)
  - llm_manual_review_queue.csv
  - gold_llm_verified_facts.parquet (after review / per policy)
  - val_llm_concordance_summary.parquet / .csv
  - llm_validation_summary.csv, motherduck_setup.sql, manifest.json, report.md
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.vocab import ENTITY_SCHEMA_COLUMNS
from utils.text_helpers import save_parquet

LOG = logging.getLogger("llm_validation")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

PROCESSED = ROOT / "processed"
DEFAULT_DB_PATH = ROOT / "thyroid_master.duckdb"
WORKSPACE_DIR = ROOT / "studies" / "llm_extraction_validation"
RUNS_DIR = WORKSPACE_DIR / "runs"

STRUCTURED_SOURCE_LIMITED_DOMAINS = {"problem_list", "unmapped"}

ORIGINAL_VALUE_PREVIEW_MAX = 500
MANUAL_QUEUE_ORIGINAL_MAX = 2000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build local lineage and validation artifacts for LLM extraction output."
    )
    parser.add_argument(
        "--input",
        default=str(PROCESSED / "note_entities_llm.parquet"),
        help="Input parquet to validate. Defaults to processed/note_entities_llm.parquet.",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Local DuckDB path used for structured/canonical comparisons.",
    )
    parser.add_argument(
        "--run-label",
        default=None,
        help="Optional stable run label. Defaults to a UTC timestamp.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Explicit output directory. Defaults to studies/llm_extraction_validation/runs/<label>/.",
    )
    parser.add_argument(
        "--review-csv",
        default=None,
        help="Merged manual decisions (llm_entity_id, verification_status, ...). "
        "Default: <output_dir>/llm_manual_review_queue.csv if that file exists.",
    )
    parser.add_argument(
        "--review-include-source-limited",
        action="store_true",
        help="Include source_limited rows in llm_manual_review_queue.csv.",
    )
    parser.add_argument(
        "--gold-require-manual",
        action="store_true",
        help="Require verified_by=manual for gold concordant rows (strict).",
    )
    parser.add_argument(
        "--trust-fill-candidate-auto",
        action="store_true",
        help="Allow existing_missing_fill_candidate into gold with verified_by=auto (not recommended).",
    )
    parser.add_argument(
        "--write-motherduck-sql",
        default=None,
        help="Optional path for MotherDuck setup SQL (default: <output_dir>/motherduck_setup.sql).",
    )
    parser.add_argument(
        "--motherduck-attach",
        action="store_true",
        help="If MOTHERDUCK_TOKEN is set, ATTACH md: and create/replace val_llm_concordance_summary.",
    )
    return parser.parse_args()


def stable_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def snippet_around_evidence(note_text: object, evidence_span: object, total_max: int = 200) -> str:
    """Return up to total_max chars centered on evidence_span; PHI-safe snippet only."""
    if not isinstance(note_text, str) or not note_text.strip():
        return ""
    span = ""
    if evidence_span is not None and not (isinstance(evidence_span, float) and pd.isna(evidence_span)):
        span = str(evidence_span).strip()
    if span and span in note_text:
        idx = note_text.index(span)
        half = max((total_max - len(span)) // 2, 0)
        start = max(0, idx - half)
        end = min(len(note_text), idx + len(span) + (total_max - len(span) - (idx - start)))
        chunk = note_text[start:end]
        if start > 0:
            chunk = "…" + chunk
        if end < len(note_text):
            chunk = chunk + "…"
        return chunk[:total_max]
    return (note_text[:total_max] + ("…" if len(note_text) > total_max else ""))[:total_max]


def coalesce_extraction_ts(row: pd.Series) -> str:
    for key in ("extraction_timestamp_utc", "extracted_at"):
        val = row.get(key)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        s = str(val).strip()
        if s:
            return s
    return ""


def canonical_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip().lower()
    text = text.replace("μ", "u").replace("µ", "u")
    text = re.sub(r"[^a-z0-9\.\+]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def truthy(value: object) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return canonical_text(value) in {"true", "1", "yes", "y", "x", "present", "positive"}


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except Exception:
        return False


def table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    except Exception:
        return set()
    return {row[1] for row in rows}


def safe_read_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    if not table_exists(con, table_name):
        return pd.DataFrame()
    if columns is None:
        return con.execute(f"SELECT * FROM {table_name}").fetchdf()

    available = table_columns(con, table_name)
    selected = [column for column in columns if column in available]
    if not selected:
        return pd.DataFrame()
    return con.execute(f"SELECT {', '.join(selected)} FROM {table_name}").fetchdf()


def normalize_stage_token(entity_type: str, raw_value: str) -> str:
    entity_text = canonical_text(entity_type)
    value_text = canonical_text(raw_value)
    combined = f"{entity_text} {value_text}".strip()

    t_match = re.search(r"\bp?t\s*([0-4](?:is|[abx])?)\b", combined)
    if t_match:
        return f"t_stage:t{t_match.group(1)}"

    n_match = re.search(r"\bp?n\s*([0-3](?:[abx])?)\b", combined)
    if n_match:
        return f"n_stage:n{n_match.group(1)}"

    m_match = re.search(r"\bp?m\s*([01x])\b", combined)
    if m_match:
        return f"m_stage:m{m_match.group(1)}"

    overall_match = re.search(r"\bstage\s+([ivx]+|[0-4](?:[abc])?)\b", combined)
    if overall_match:
        return f"overall_stage:stage {overall_match.group(1)}"

    if "extrathyroidal" in combined or re.search(r"\bete\b", combined):
        if any(token in combined for token in {"gross", "macroscopic", "strap", "trachea", "esophagus"}):
            return "extrathyroidal_extension:gross"
        if any(token in combined for token in {"micro", "minimal", "focal", "perithyroid"}):
            return "extrathyroidal_extension:microscopic"
        if any(token in combined for token in {"none", "absent", "negative", "no ete"}):
            return "extrathyroidal_extension:none"
        return "extrathyroidal_extension:present"

    return ""


def normalize_gene_token(raw_value: str) -> str:
    value_text = canonical_text(raw_value)
    if "braf" in value_text:
        return "braf"
    if "nras" in value_text:
        return "nras"
    if "hras" in value_text:
        return "hras"
    if "kras" in value_text:
        return "kras"
    if re.search(r"\bras\b", value_text):
        return "ras"
    if "tert" in value_text:
        return "tert"
    if "ret" in value_text:
        return "ret"
    if "ntrk" in value_text:
        return "ntrk"
    if "alk" in value_text:
        return "alk"
    if "tp53" in value_text:
        return "tp53"
    return ""


def normalize_procedure_token(raw_value: str) -> str:
    value_text = canonical_text(raw_value)
    if any(token in value_text for token in {"completion thyroid", "completion thyro"}):
        return "completion_thyroidectomy"
    if any(token in value_text for token in {"near total thyroid", "bilateral thyroid", "total thyroid"}):
        return "total_thyroidectomy"
    if any(token in value_text for token in {"lobectom", "hemithyroid"}):
        return "hemithyroidectomy"
    if any(token in value_text for token in {"central neck", "central compartment", "level vi", "paratracheal", "pretracheal", "delphian", "prelaryngeal"}):
        return "central_neck_dissection"
    if any(token in value_text for token in {"lateral neck", "modified radical", "mrnd", "level ii", "level iii", "level iv", "level v", "jugular"}):
        return "lateral_neck_dissection"
    if "laryngosc" in value_text:
        return "laryngoscopy"
    if "parathyroid" in value_text and any(token in value_text for token in {"auto", "transplant", "autograft"}):
        return "parathyroid_autotransplant"
    return ""


def normalize_operative_token(raw_value: str) -> str:
    value_text = canonical_text(raw_value)
    if any(token in value_text for token in {"rln monitoring", "nerve monitoring", "rln monitor"}):
        return "rln_monitoring"
    if any(token in value_text for token in {"drain", "jp drain"}):
        return "drain_placed"
    if any(token in value_text for token in {"gross ete", "gross extrathyroidal"}):
        return "gross_ete"
    if "local invasion" in value_text:
        return "local_invasion"
    if "trachea" in value_text or "tracheal" in value_text:
        return "tracheal_involvement"
    if "esophagus" in value_text or "esophageal" in value_text:
        return "esophageal_involvement"
    if "strap muscle" in value_text:
        return "strap_muscle_involvement"
    if any(token in value_text for token in {"reoperative", "redo field", "prior scar field"}):
        return "reoperative_field"
    if "parathyroid" in value_text and any(token in value_text for token in {"autograft", "autotransplant"}):
        return "parathyroid_autograft"
    return ""


def normalize_complication_token(raw_value: str) -> str:
    value_text = canonical_text(raw_value)
    if any(token in value_text for token in {"vocal cord paralysis", "cord paralysis", "cord palsy", "vcp"}):
        return "vocal_cord_paralysis"
    if any(token in value_text for token in {"vocal cord paresis", "cord paresis", "cord weakness", "hypomobil", "immobil"}):
        return "vocal_cord_paresis"
    if any(token in value_text for token in {"rln injury", "recurrent laryngeal nerve"}):
        return "rln_injury"
    if "hypocalc" in value_text or "low calcium" in value_text:
        return "hypocalcemia"
    if "hypoparathy" in value_text:
        return "hypoparathyroidism"
    if "hematoma" in value_text:
        return "hematoma"
    if "seroma" in value_text:
        return "seroma"
    if "wound infection" in value_text or "surgical site infection" in value_text or value_text == "ssi":
        return "wound_infection"
    if "chyle leak" in value_text or "chylous fistula" in value_text:
        return "chyle_leak"
    return ""


def normalize_medication_token(raw_value: str) -> str:
    value_text = canonical_text(raw_value)
    if any(token in value_text for token in {"radioactive iodine", " rai", "i 131", "i131", "131 i"}):
        return "rai_therapy"
    if any(token in value_text for token in {"levothyroxine", "synthroid", "levoxyl", "l thyroxine"}):
        return "levothyroxine"
    if any(token in value_text for token in {"calcium carbonate", "calcium citrate", "caltrate", "tums", "oscal", "citracal"}):
        return "calcium_supplement"
    if any(token in value_text for token in {"calcitriol", "rocaltrol"}):
        return "calcitriol"
    return ""


def normalize_problem_token(raw_value: str) -> str:
    value_text = canonical_text(raw_value)
    mapping = {
        "hypertension": "hypertension",
        "htn": "hypertension",
        "t2dm": "diabetes_type2",
        "type 2 diabet": "diabetes_type2",
        "dm 2": "diabetes_type2",
        "diabet": "diabetes",
        "obes": "obesity",
        "coronary artery disease": "cad",
        "cad": "cad",
        "atrial fibrillat": "atrial_fibrillation",
        "a fib": "atrial_fibrillation",
        "hypothyroid": "hypothyroidism",
        "hyperthyroid": "hyperthyroidism",
        "breast cancer": "breast_cancer",
        "lung cancer": "lung_cancer",
        "gerd": "gerd",
        "reflux": "gerd",
        "chronic kidney": "ckd",
        "renal insufficiency": "ckd",
        "depression": "depression",
        "mdd": "depression",
        "asthma": "asthma",
        "copd": "copd",
        "chronic obstructive": "copd",
    }
    for needle, normalized in mapping.items():
        if needle in value_text:
            return normalized
    return ""


def classify_value(entity_type: object, raw_value: object, normalized_value: object) -> tuple[str, str, str]:
    entity_text = canonical_text(entity_type)
    raw_text = canonical_text(raw_value)
    normalized_text = canonical_text(normalized_value)
    combined = " ".join(part for part in [entity_text, raw_text, normalized_text] if part).strip()

    stage_token = normalize_stage_token(entity_text, raw_text or normalized_text)
    if stage_token:
        return "staging", stage_token, "strong"

    for resolver, domain, support in [
        (normalize_gene_token, "genetics", "strong"),
        (normalize_operative_token, "operative_detail", "moderate"),
        (normalize_procedure_token, "procedures", "moderate"),
        (normalize_complication_token, "complications", "moderate"),
        (normalize_medication_token, "medications", "mixed"),
        (normalize_problem_token, "problem_list", "limited"),
    ]:
        token = resolver(combined)
        if token:
            return domain, token, support

    if entity_text in {"staging", "genetics", "procedure", "complication", "medication", "problem"}:
        fallback_domain = {
            "staging": "staging",
            "genetics": "genetics",
            "procedure": "procedures",
            "complication": "complications",
            "medication": "medications",
            "problem": "problem_list",
        }[entity_text]
        return fallback_domain, raw_text or normalized_text, "limited"

    return "unmapped", raw_text or normalized_text or entity_text, "source_limited"


def build_lineage(llm_df: pd.DataFrame, notes_df: pd.DataFrame) -> pd.DataFrame:
    notes_cols = [
        "note_row_id",
        "research_id",
        "note_index",
        "source_sheet",
        "source_column",
        "char_count",
        "note_text",
    ]
    lineage = llm_df.merge(notes_df[notes_cols], on=["note_row_id", "research_id"], how="left")
    lineage["source_note_exists_flag"] = lineage["note_text"].notna()
    lineage["evidence_span_found_flag"] = lineage.apply(
        lambda row: bool(row["evidence_span"]) and isinstance(row["note_text"], str) and row["evidence_span"] in row["note_text"],
        axis=1,
    )
    lineage["comparison_domain"], lineage["comparison_token"], lineage["support_level"] = zip(
        *lineage.apply(
            lambda row: classify_value(row["entity_type"], row["entity_value_raw"], row["entity_value_norm"]),
            axis=1,
        )
    )
    lineage["token_parse_success_flag"] = lineage["comparison_domain"] != "unmapped"
    lineage["original_source_link"] = lineage.apply(
        lambda row: snippet_around_evidence(row["note_text"], row.get("evidence_span")),
        axis=1,
    )
    lineage["extraction_ts"] = lineage.apply(coalesce_extraction_ts, axis=1)
    lineage = lineage.drop(columns=["note_text"])
    return lineage


def load_source_notes(
    con: duckdb.DuckDBPyConnection,
    llm_df: pd.DataFrame,
    notes_path: Path,
) -> pd.DataFrame:
    key_df = llm_df[["note_row_id", "research_id"]].drop_duplicates().copy()
    key_df["research_id"] = key_df["research_id"].astype(int)

    note_columns = [
        "note_row_id",
        "research_id",
        "note_index",
        "source_sheet",
        "source_column",
        "char_count",
        "note_text",
    ]

    if table_exists(con, "clinical_notes_long"):
        con.register("llm_note_keys", key_df)
        try:
            return con.execute(
                """
                SELECT
                    c.note_row_id,
                    CAST(c.research_id AS INTEGER) AS research_id,
                    c.note_index,
                    c.source_sheet,
                    c.source_column,
                    c.char_count,
                    c.note_text
                FROM clinical_notes_long c
                INNER JOIN llm_note_keys k
                    ON c.note_row_id = k.note_row_id
                   AND CAST(c.research_id AS INTEGER) = k.research_id
                """
            ).fetchdf()
        finally:
            con.unregister("llm_note_keys")

    notes_df = pd.read_parquet(notes_path, columns=note_columns)
    notes_df["research_id"] = notes_df["research_id"].astype(int)
    return notes_df.merge(key_df, on=["note_row_id", "research_id"], how="inner")


def existing_note_entities(
    con: duckdb.DuckDBPyConnection,
    target_domains: set[str] | None = None,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    target_domains = target_domains or {
        "staging",
        "genetics",
        "procedures",
        "operative_detail",
        "complications",
        "medications",
        "problem_list",
    }
    tables = []
    if "staging" in target_domains:
        tables.append("note_entities_staging")
    if "genetics" in target_domains:
        tables.append("note_entities_genetics")
    if "procedures" in target_domains:
        tables.append("note_entities_procedures")
    if "operative_detail" in target_domains:
        tables.append("note_entities_operative_detail")
    if "complications" in target_domains:
        tables.append("note_entities_complications")
    if "medications" in target_domains:
        tables.append("note_entities_medications")
    if "problem_list" in target_domains:
        tables.append("note_entities_problem_list")

    for table_name in tables:
        if not table_exists(con, table_name):
            continue
        df = con.execute(
            f"""
            SELECT research_id, entity_type, entity_value_raw, entity_value_norm, present_or_negated
            FROM {table_name}
            WHERE present_or_negated = 'present'
            """
        ).fetchdf()
        if df.empty:
            continue
        df["comparison_domain"], df["comparison_token"], _ = zip(
            *df.apply(
                lambda row: classify_value(row["entity_type"], row["entity_value_raw"], row["entity_value_norm"]),
                axis=1,
            )
        )
        df = df[df["comparison_domain"] != "unmapped"].copy()
        df["source_table"] = table_name
        df["source_column"] = "entity_value_norm"
        df["value_display"] = df["entity_value_norm"].fillna(df["entity_value_raw"])
        rows.append(df[["research_id", "comparison_domain", "comparison_token", "source_table", "source_column", "value_display"]])
    if not rows:
        return pd.DataFrame(columns=["research_id", "comparison_domain", "comparison_token", "source_table", "source_column", "value_display"])
    return pd.concat(rows, ignore_index=True)


def normalize_research_id(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        return int(stripped)
    return int(str(value))


def add_structured_record(records: list[dict], research_id: object, domain: str, token: str, source_table: str, source_column: str, value_display: str) -> None:
    normalized_research_id = normalize_research_id(research_id)
    if not token or normalized_research_id is None:
        return
    records.append(
        {
            "research_id": normalized_research_id,
            "comparison_domain": domain,
            "comparison_token": token,
            "source_table": source_table,
            "source_column": source_column,
            "value_display": value_display,
        }
    )


def build_structured_values(
    con: duckdb.DuckDBPyConnection,
    target_domains: set[str] | None = None,
) -> pd.DataFrame:
    records: list[dict] = []
    target_domains = target_domains or {
        "staging",
        "genetics",
        "procedures",
        "operative_detail",
        "complications",
        "medications",
    }

    if "staging" in target_domains:
        tumor_df = safe_read_table(
            con,
            "tumor_episode_master_v2",
            [
                "research_id",
                "t_stage",
                "n_stage",
                "m_stage",
                "overall_stage",
                "extrathyroidal_extension",
            ],
        )
        for row in tumor_df.to_dict("records"):
            add_structured_record(records, row.get("research_id"), "staging", normalize_stage_token("T_stage", row.get("t_stage")), "tumor_episode_master_v2", "t_stage", str(row.get("t_stage") or ""))
            add_structured_record(records, row.get("research_id"), "staging", normalize_stage_token("N_stage", row.get("n_stage")), "tumor_episode_master_v2", "n_stage", str(row.get("n_stage") or ""))
            add_structured_record(records, row.get("research_id"), "staging", normalize_stage_token("M_stage", row.get("m_stage")), "tumor_episode_master_v2", "m_stage", str(row.get("m_stage") or ""))
            add_structured_record(records, row.get("research_id"), "staging", normalize_stage_token("overall_stage", row.get("overall_stage")), "tumor_episode_master_v2", "overall_stage", str(row.get("overall_stage") or ""))
            add_structured_record(records, row.get("research_id"), "staging", normalize_stage_token("extrathyroidal_extension", row.get("extrathyroidal_extension")), "tumor_episode_master_v2", "extrathyroidal_extension", str(row.get("extrathyroidal_extension") or ""))

    if "genetics" in target_domains:
        molecular_cols = table_columns(con, "molecular_test_episode_v2")
        molecular_df = safe_read_table(
            con,
            "molecular_test_episode_v2",
            [
                "research_id",
                "braf_flag",
                "braf_variant",
                "ras_flag",
                "ras_subtype",
                "ret_flag",
                "tert_flag",
                "ntrk_flag",
                "alk_flag",
                "tp53_flag",
            ],
        )
        for row in molecular_df.to_dict("records"):
            if truthy(row.get("braf_flag")):
                add_structured_record(records, row.get("research_id"), "genetics", "braf", "molecular_test_episode_v2", "braf_flag", "BRAF")
            if row.get("braf_variant"):
                add_structured_record(records, row.get("research_id"), "genetics", normalize_gene_token(row.get("braf_variant")), "molecular_test_episode_v2", "braf_variant", str(row.get("braf_variant")))
            if truthy(row.get("ras_flag")):
                add_structured_record(records, row.get("research_id"), "genetics", "ras", "molecular_test_episode_v2", "ras_flag", "RAS")
            if row.get("ras_subtype"):
                add_structured_record(records, row.get("research_id"), "genetics", normalize_gene_token(row.get("ras_subtype")), "molecular_test_episode_v2", "ras_subtype", str(row.get("ras_subtype")))
            for column_name, token in [("ret_flag", "ret"), ("tert_flag", "tert"), ("ntrk_flag", "ntrk"), ("tp53_flag", "tp53"), ("alk_flag", "alk")]:
                if column_name in molecular_cols and truthy(row.get(column_name)):
                    add_structured_record(records, row.get("research_id"), "genetics", token, "molecular_test_episode_v2", column_name, token.upper())

    if "procedures" in target_domains or "operative_detail" in target_domains:
        path_df = safe_read_table(
            con,
            "path_synoptics",
            [
                "research_id",
                "thyroid_procedure",
                "completion",
                "central_compartment_dissection",
                "right_neck_dissection",
                "left_neck_dissection",
                "bilateral_neck_dissection",
                "other_ln_dissection",
                "parathyroid_operation",
                "io_rln_monitoring",
            ],
        )
        for row in path_df.to_dict("records"):
            if "procedures" in target_domains:
                add_structured_record(records, row.get("research_id"), "procedures", normalize_procedure_token(row.get("thyroid_procedure")), "path_synoptics", "thyroid_procedure", str(row.get("thyroid_procedure") or ""))
                if truthy(row.get("completion")):
                    add_structured_record(records, row.get("research_id"), "procedures", "completion_thyroidectomy", "path_synoptics", "completion", "completion")
                if truthy(row.get("central_compartment_dissection")):
                    add_structured_record(records, row.get("research_id"), "procedures", "central_neck_dissection", "path_synoptics", "central_compartment_dissection", "central compartment dissection")
                if any(truthy(row.get(column_name)) for column_name in ["right_neck_dissection", "left_neck_dissection", "bilateral_neck_dissection"]):
                    add_structured_record(records, row.get("research_id"), "procedures", "lateral_neck_dissection", "path_synoptics", "*_neck_dissection", "lateral neck dissection")
                add_structured_record(records, row.get("research_id"), "procedures", normalize_procedure_token(row.get("other_ln_dissection")), "path_synoptics", "other_ln_dissection", str(row.get("other_ln_dissection") or ""))
                add_structured_record(records, row.get("research_id"), "procedures", normalize_procedure_token(row.get("parathyroid_operation")), "path_synoptics", "parathyroid_operation", str(row.get("parathyroid_operation") or ""))
            if "operative_detail" in target_domains and truthy(row.get("io_rln_monitoring")):
                add_structured_record(records, row.get("research_id"), "operative_detail", "rln_monitoring", "path_synoptics", "io_rln_monitoring", "intraoperative RLN monitoring")

    if "procedures" in target_domains or "operative_detail" in target_domains:
        operative_cols = table_columns(con, "operative_episode_detail_v2")
        operative_df = safe_read_table(
            con,
            "operative_episode_detail_v2",
            [
                "research_id",
                "procedure_normalized",
                "rln_monitoring_flag",
                "drain_flag",
                "gross_ete_flag",
                "local_invasion_flag",
                "tracheal_involvement_flag",
                "esophageal_involvement_flag",
                "strap_muscle_involvement_flag",
                "reoperative_field_flag",
                "parathyroid_autograft_flag",
            ],
        )
        for row in operative_df.to_dict("records"):
            if "procedures" in target_domains:
                add_structured_record(records, row.get("research_id"), "procedures", normalize_procedure_token(row.get("procedure_normalized")), "operative_episode_detail_v2", "procedure_normalized", str(row.get("procedure_normalized") or ""))
            if "operative_detail" in target_domains:
                for column_name, token, domain in [
                    ("rln_monitoring_flag", "rln_monitoring", "operative_detail"),
                    ("drain_flag", "drain_placed", "operative_detail"),
                    ("gross_ete_flag", "gross_ete", "operative_detail"),
                    ("local_invasion_flag", "local_invasion", "operative_detail"),
                    ("tracheal_involvement_flag", "tracheal_involvement", "operative_detail"),
                    ("esophageal_involvement_flag", "esophageal_involvement", "operative_detail"),
                    ("strap_muscle_involvement_flag", "strap_muscle_involvement", "operative_detail"),
                    ("reoperative_field_flag", "reoperative_field", "operative_detail"),
                    ("parathyroid_autograft_flag", "parathyroid_autograft", "operative_detail"),
                ]:
                    if column_name in operative_cols and truthy(row.get(column_name)):
                        add_structured_record(records, row.get("research_id"), domain, token, "operative_episode_detail_v2", column_name, token)

    if "medications" in target_domains:
        rai_df = safe_read_table(
            con,
            "rai_treatment_episode_v2",
            ["research_id", "rai_assertion_status", "dose_mci", "resolved_rai_date"],
        )
        for row in rai_df.to_dict("records"):
            if canonical_text(row.get("rai_assertion_status")) in {"definite received", "likely received", "planned", "historical"}:
                display = f"RAI dose={row.get('dose_mci')} date={row.get('resolved_rai_date')}"
                add_structured_record(records, row.get("research_id"), "medications", "rai_therapy", "rai_treatment_episode_v2", "rai_assertion_status", display)

    if "complications" in target_domains:
        complications_df = safe_read_table(
            con,
            "complications",
            [
                "research_id",
                "rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy",
                "vocal_cord_status",
                "seroma",
                "hematoma",
                "hypocalcemia",
                "hypoparathyroidism",
                "wound_infection",
            ],
        )
        for row in complications_df.to_dict("records"):
            if truthy(row.get("rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy")):
                add_structured_record(records, row.get("research_id"), "complications", "rln_injury", "complications", "rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy", "RLN injury")
            vocal_cord_status = canonical_text(row.get("vocal_cord_status"))
            if "paral" in vocal_cord_status:
                add_structured_record(records, row.get("research_id"), "complications", "vocal_cord_paralysis", "complications", "vocal_cord_status", str(row.get("vocal_cord_status")))
            elif "pares" in vocal_cord_status:
                add_structured_record(records, row.get("research_id"), "complications", "vocal_cord_paresis", "complications", "vocal_cord_status", str(row.get("vocal_cord_status")))
            for column_name, token in [
                ("seroma", "seroma"),
                ("hematoma", "hematoma"),
                ("hypocalcemia", "hypocalcemia"),
                ("hypoparathyroidism", "hypoparathyroidism"),
                ("wound_infection", "wound_infection"),
            ]:
                if truthy(row.get(column_name)):
                    add_structured_record(records, row.get("research_id"), "complications", token, "complications", column_name, token)

    if not records:
        return pd.DataFrame(columns=["research_id", "comparison_domain", "comparison_token", "source_table", "source_column", "value_display"])
    return pd.DataFrame.from_records(records).drop_duplicates()


def aggregate_comparators(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "research_id",
                "comparison_domain",
                f"{prefix}_value_count",
                f"{prefix}_tokens_json",
                f"{prefix}_display_json",
                f"{prefix}_sources_json",
            ]
        )

    grouped = (
        df.groupby(["research_id", "comparison_domain"], dropna=False)
        .agg(
            tokens=("comparison_token", lambda series: sorted({value for value in series if value})),
            displays=("value_display", lambda series: sorted({str(value) for value in series if value not in (None, "")})),
            sources=("source_table", lambda series: sorted({str(value) for value in series if value not in (None, "")})),
        )
        .reset_index()
    )
    grouped[f"{prefix}_value_count"] = grouped["tokens"].apply(len)
    grouped[f"{prefix}_tokens_json"] = grouped["tokens"].apply(json.dumps)
    grouped[f"{prefix}_display_json"] = grouped["displays"].apply(json.dumps)
    grouped[f"{prefix}_sources_json"] = grouped["sources"].apply(json.dumps)
    return grouped.drop(columns=["tokens", "displays", "sources"])


def parse_json_array(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    try:
        parsed = json.loads(str(value))
    except Exception:
        return []
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return []


def build_side_by_side(
    lineage_df: pd.DataFrame,
    baseline_agg: pd.DataFrame,
    structured_agg: pd.DataFrame,
) -> pd.DataFrame:
    comparison = lineage_df.merge(
        baseline_agg,
        on=["research_id", "comparison_domain"],
        how="left",
    ).merge(
        structured_agg,
        on=["research_id", "comparison_domain"],
        how="left",
    )

    comparison["baseline_match_flag"] = comparison.apply(
        lambda row: row["comparison_token"] in set(parse_json_array(row.get("baseline_tokens_json"))),
        axis=1,
    )
    comparison["structured_match_flag"] = comparison.apply(
        lambda row: row["comparison_token"] in set(parse_json_array(row.get("structured_tokens_json"))),
        axis=1,
    )
    comparison["baseline_value_count"] = comparison["baseline_value_count"].fillna(0).astype(int)
    comparison["structured_value_count"] = comparison["structured_value_count"].fillna(0).astype(int)

    def status_for_row(row: pd.Series) -> str:
        if row["comparison_domain"] == "unmapped":
            return "source_limited"
        if row["structured_match_flag"]:
            return "concordant_existing"
        if row["structured_value_count"] > 0:
            return "discordant_existing"
        if row["baseline_match_flag"]:
            return "concordant_existing_extraction_only"
        if row["comparison_domain"] in STRUCTURED_SOURCE_LIMITED_DOMAINS or row["support_level"] in {"limited", "source_limited"}:
            return "source_limited"
        return "existing_missing_fill_candidate"

    comparison["algorithm_comparison_status"] = comparison.apply(status_for_row, axis=1)
    comparison["confirmation_status"] = comparison["algorithm_comparison_status"]
    comparison["candidate_fill_missing"] = comparison["algorithm_comparison_status"] == "existing_missing_fill_candidate"
    comparison["candidate_review_conflict"] = comparison["algorithm_comparison_status"] == "discordant_existing"
    comparison["candidate_hold_source_limited"] = comparison["algorithm_comparison_status"] == "source_limited"
    comparison["needs_review"] = comparison["candidate_review_conflict"] | comparison["candidate_fill_missing"]
    comparison["existing_values_preview"] = comparison.apply(
        lambda row: row["structured_display_json"] if row["structured_value_count"] > 0 else row["baseline_display_json"],
        axis=1,
    )
    comparison["existing_source_tables_preview"] = comparison.apply(
        lambda row: row["structured_sources_json"] if row["structured_value_count"] > 0 else row["baseline_sources_json"],
        axis=1,
    )
    return comparison


def compute_original_value_vs_llm_diff(row: pd.Series) -> str:
    llm_tok = str(row.get("comparison_token") or "")
    if row.get("structured_match_flag") or row.get("baseline_match_flag"):
        return "match"
    s_count = int(row.get("structured_value_count") or 0)
    b_count = int(row.get("baseline_value_count") or 0)
    if s_count > 0 or b_count > 0:
        preview = row.get("existing_values_preview")
        if isinstance(preview, str) and preview:
            trimmed = preview[:ORIGINAL_VALUE_PREVIEW_MAX]
            return f"mismatch|llm_token={llm_tok}|existing_preview={trimmed}"
        return f"mismatch|llm_token={llm_tok}"
    if row.get("comparison_domain") == "unmapped":
        return "no_comparator_domain"
    return "no_structured_baseline"


def default_verification_status(algorithm_status: object) -> str:
    s = str(algorithm_status or "")
    if s in ("concordant_existing", "concordant_existing_extraction_only"):
        return "concordant"
    if s == "existing_missing_fill_candidate":
        return "existing_missing_fill_candidate"
    if s == "discordant_existing":
        return "pending_review"
    if s == "source_limited":
        return "source_limited"
    return "pending_review"


def enrich_side_by_side_verification(side_by_side_df: pd.DataFrame) -> pd.DataFrame:
    df = side_by_side_df.copy()
    df["original_value_vs_llm_diff"] = df.apply(compute_original_value_vs_llm_diff, axis=1)
    df["verification_status"] = df["algorithm_comparison_status"].map(default_verification_status)
    df["verified_by"] = "auto"
    return df


def merge_manual_review(side_by_side_df: pd.DataFrame, review_path: Path) -> pd.DataFrame:
    rev = pd.read_csv(review_path)
    if "llm_entity_id" not in rev.columns:
        LOG.warning("Review CSV has no llm_entity_id; skip merge")
        return side_by_side_df
    if "verification_status" not in rev.columns:
        LOG.warning("Review CSV has no verification_status; skip merge")
        return side_by_side_df

    df = side_by_side_df.copy()
    merged = 0
    for _, r in rev.iterrows():
        raw_eid = r.get("llm_entity_id")
        if raw_eid is None or (isinstance(raw_eid, float) and pd.isna(raw_eid)):
            continue
        eid = int(raw_eid)
        vs = r.get("verification_status")
        if vs is None or (isinstance(vs, float) and pd.isna(vs)):
            continue
        status = str(vs).strip()
        if not status:
            continue
        mask = df["llm_entity_id"] == eid
        if not mask.any():
            continue
        if "note_row_id" in rev.columns and pd.notna(r.get("note_row_id")):
            expected = str(df.loc[mask, "note_row_id"].iloc[0])
            if str(r.get("note_row_id")).strip() != expected:
                LOG.warning("note_row_id mismatch for llm_entity_id=%s; skip merge for this row", eid)
                continue
        df.loc[mask, "verification_status"] = status
        df.loc[mask, "verified_by"] = "manual"
        merged += 1
    LOG.info("Merged %s manual verification_status rows from %s", merged, review_path.name)
    return df


def build_manual_review_queue(side_by_side_df: pd.DataFrame, include_source_limited: bool) -> pd.DataFrame:
    mask = side_by_side_df["algorithm_comparison_status"] == "discordant_existing"
    if include_source_limited:
        mask = mask | (side_by_side_df["algorithm_comparison_status"] == "source_limited")
    q = side_by_side_df.loc[mask].copy()

    def llm_val(row: pd.Series) -> str:
        return str(row.get("entity_value_norm") or row.get("entity_value_raw") or "")

    def orig_val(row: pd.Series) -> str:
        p = row.get("existing_values_preview")
        if p is None:
            return ""
        s = str(p)
        return s[:MANUAL_QUEUE_ORIGINAL_MAX] if len(s) > MANUAL_QUEUE_ORIGINAL_MAX else s

    out = pd.DataFrame(
        {
            "llm_entity_id": q["llm_entity_id"],
            "research_id": q["research_id"],
            "note_row_id": q["note_row_id"],
            "llm_value": q.apply(llm_val, axis=1),
            "original_value": q.apply(orig_val, axis=1),
            "evidence_span": q["evidence_span"].fillna("").astype(str),
            "algorithm_comparison_status": q["algorithm_comparison_status"],
            "comparison_domain": q["comparison_domain"],
            "comparison_token": q["comparison_token"],
            "verification_status": "",
        }
    )
    return out


def build_gold_facts(
    side_by_side_df: pd.DataFrame,
    gold_require_manual: bool,
    trust_fill_candidate_auto: bool,
) -> pd.DataFrame:
    def eligible(row: pd.Series) -> bool:
        vs = row.get("verification_status")
        if vs not in ("concordant", "existing_missing_fill_candidate"):
            return False
        pol = row.get("present_or_negated")
        if str(pol or "").strip().lower() != "present":
            return False
        vb = str(row.get("verified_by") or "")
        if vs == "concordant":
            if gold_require_manual:
                return vb == "manual"
            return True
        if trust_fill_candidate_auto:
            return True
        return vb == "manual"

    mask = side_by_side_df.apply(eligible, axis=1)
    return side_by_side_df.loc[mask].copy()


def build_val_llm_concordance_summary(side_by_side_df: pd.DataFrame) -> pd.DataFrame:
    def count_auto(s: pd.Series) -> int:
        return int((s.astype(str) == "auto").sum())

    def count_manual(s: pd.Series) -> int:
        return int((s.astype(str) == "manual").sum())

    summary = (
        side_by_side_df.groupby(["comparison_domain", "verification_status"], dropna=False)
        .agg(
            llm_rows=("llm_entity_id", "count"),
            unique_patients=("research_id", pd.Series.nunique),
            evidence_spans_found=("evidence_span_found_flag", "sum"),
            verified_auto=("verified_by", count_auto),
            verified_manual=("verified_by", count_manual),
        )
        .reset_index()
        .sort_values(["comparison_domain", "verification_status"])
    )
    return summary


def resolve_review_csv_path(explicit: str | None, output_dir: Path) -> Path | None:
    if explicit:
        p = Path(explicit)
        return p if p.is_file() else None
    default = output_dir / "llm_manual_review_queue.csv"
    if not default.is_file():
        return None
    try:
        rev = pd.read_csv(default)
    except Exception:
        return None
    if "verification_status" not in rev.columns:
        return None
    vs = rev["verification_status"].dropna().astype(str).str.strip()
    if vs.eq("").all() or vs.eq("nan").all():
        return None
    return default


def write_motherduck_sql(
    output_path: Path,
    side_by_side_parquet_abs: Path,
    summary_parquet_abs: Path,
    gold_parquet_abs: Path,
) -> None:
    p_side = str(side_by_side_parquet_abs.resolve()).replace("\\", "/").replace("'", "''")
    p_sum = str(summary_parquet_abs.resolve()).replace("\\", "/").replace("'", "''")
    p_gold = str(gold_parquet_abs.resolve()).replace("\\", "/").replace("'", "''")
    lines = [
        "-- MotherDuck / DuckDB: paste into MotherDuck SQL editor after uploading Parquet or using a path DuckDB can read.",
        "-- Do not commit MOTHERDUCK_TOKEN. Example attach:",
        "--   ATTACH 'md:YOUR_DATABASE?motherduck_token=<SECRET>' AS md (TYPE DUCKDB);",
        "",
        "ATTACH 'md:YOUR_DATABASE' AS md (TYPE DUCKDB);",
        "",
        "CREATE OR REPLACE TABLE md.val_llm_concordance_summary AS",
        f"SELECT * FROM read_parquet('{p_sum}');",
        "",
        "-- SELECT * FROM md.val_llm_concordance_summary;",
        "",
        "-- Optional (large):",
        f"-- CREATE OR REPLACE TABLE md.llm_side_by_side AS SELECT * FROM read_parquet('{p_side}');",
        f"-- CREATE OR REPLACE TABLE md.gold_llm_verified_facts AS SELECT * FROM read_parquet('{p_gold}');",
        "",
    ]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def maybe_motherduck_attach(summary_df: pd.DataFrame) -> None:
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        LOG.warning("MOTHERDUCK_TOKEN unset; skip --motherduck-attach")
        return
    db = os.environ.get("MOTHERDUCK_DATABASE", "")
    if not db:
        LOG.warning("MOTHERDUCK_DATABASE unset; skip --motherduck-attach")
        return
    con = duckdb.connect(f"md:{db}?motherduck_token={token}")
    try:
        con.register("_val_llm_summary", summary_df)
        con.execute("CREATE OR REPLACE TABLE val_llm_concordance_summary AS SELECT * FROM _val_llm_summary")
        LOG.info("Created/replaced val_llm_concordance_summary on MotherDuck database %s", db)
    finally:
        con.close()


def build_summary(side_by_side_df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        side_by_side_df.groupby(["comparison_domain", "algorithm_comparison_status"], dropna=False)
        .agg(
            llm_rows=("llm_entity_id", "count"),
            unique_patients=("research_id", pd.Series.nunique),
            evidence_spans_found=("evidence_span_found_flag", "sum"),
            source_note_links=("source_note_exists_flag", "sum"),
            structured_matches=("structured_match_flag", "sum"),
            baseline_matches=("baseline_match_flag", "sum"),
            fill_candidates=("candidate_fill_missing", "sum"),
            review_conflicts=("candidate_review_conflict", "sum"),
        )
        .reset_index()
        .sort_values(["comparison_domain", "algorithm_comparison_status"])
    )
    return summary


def write_report(
    output_dir: Path,
    input_path: Path,
    side_by_side_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    gold_df: pd.DataFrame,
) -> None:
    lines = [
        "# LLM Extraction Validation Report",
        "",
        f"- Input parquet: `{input_path}`",
        f"- Generated at: `{datetime.now(timezone.utc).isoformat()}`",
        f"- Total LLM rows: `{len(side_by_side_df):,}`",
        f"- Unique patients: `{side_by_side_df['research_id'].nunique():,}`",
        f"- Gold rows (`verification_status` concordant | existing_missing_fill_candidate, per policy): `{len(gold_df):,}`",
        "",
        "## Domain / algorithm status",
        "",
        "| Domain | Algorithm status | Rows | Patients | Structured matches | Baseline matches | Fill candidates | Review conflicts |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary_df.to_dict("records"):
        lines.append(
            f"| {row['comparison_domain']} | {row['algorithm_comparison_status']} | {row['llm_rows']:,} | {row['unique_patients']:,} | {row['structured_matches']:,} | {row['baseline_matches']:,} | {row['fill_candidates']:,} | {row['review_conflicts']:,} |"
        )

    top_conflicts = side_by_side_df[side_by_side_df["candidate_review_conflict"]].head(20)
    if not top_conflicts.empty:
        lines.extend([
            "",
            "## Sample Review Conflicts",
            "",
            "| research_id | domain | llm_value | existing_values | source_sheet | source_column |",
            "|---:|---|---|---|---|---|",
        ])
        for row in top_conflicts.to_dict("records"):
            lines.append(
                "| {research_id} | {comparison_domain} | {llm_value} | {existing_values} | {source_sheet} | {source_column} |".format(
                    research_id=row["research_id"],
                    comparison_domain=row["comparison_domain"],
                    llm_value=str(row["entity_value_norm"] or row["entity_value_raw"]),
                    existing_values=str(row["existing_values_preview"]),
                    source_sheet=str(row.get("source_sheet") or ""),
                    source_column=str(row.get("source_column") or ""),
                )
            )

    (output_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(
            f"Input parquet not found: {input_path}. Wait for the LLM extraction run to finish, "
            "or pass --input to an existing note_entities parquet for a dry validation pass."
        )

    output_label = args.run_label or stable_timestamp()
    output_dir = Path(args.output_dir) if args.output_dir else RUNS_DIR / output_label
    output_dir.mkdir(parents=True, exist_ok=True)

    llm_df = pd.read_parquet(input_path)
    missing = [column for column in ENTITY_SCHEMA_COLUMNS if column not in llm_df.columns]
    if missing:
        raise SystemExit(f"Input parquet is missing required extraction columns: {missing}")

    llm_df = llm_df[ENTITY_SCHEMA_COLUMNS].copy()
    rename_map = {}
    if "verification_status" in llm_df.columns:
        rename_map["verification_status"] = "llm_verification_status"
    if "verification_step" in llm_df.columns:
        rename_map["verification_step"] = "llm_verification_step"
    if rename_map:
        llm_df = llm_df.rename(columns=rename_map)

    llm_df.insert(0, "llm_entity_id", range(1, len(llm_df) + 1))
    llm_df["research_id"] = llm_df["research_id"].astype(int)

    con = duckdb.connect(str(args.db_path), read_only=True)
    notes_path = PROCESSED / "clinical_notes_long.parquet"
    if not notes_path.exists() and not table_exists(con, "clinical_notes_long"):
        con.close()
        raise SystemExit(f"Source notes not found in local DuckDB or parquet: {notes_path}")

    notes_df = load_source_notes(con, llm_df, notes_path)
    LOG.info("Loaded %s extracted rows from %s", f"{len(llm_df):,}", input_path.name)
    LOG.info("Loaded %s source notes linked to extraction input", f"{len(notes_df):,}")

    lineage_df = build_lineage(llm_df, notes_df)
    target_domains = {domain for domain in lineage_df["comparison_domain"].dropna().unique().tolist() if domain != "unmapped"}
    LOG.info("Comparison domains in input: %s", ", ".join(sorted(target_domains)) or "none")

    LOG.info("Building baseline extraction comparators")
    baseline_df = existing_note_entities(con, target_domains=target_domains)
    LOG.info("Building structured/canonical comparators")
    structured_df = build_structured_values(con, target_domains=target_domains)
    con.close()

    baseline_agg = aggregate_comparators(baseline_df, "baseline")
    structured_agg = aggregate_comparators(structured_df, "structured")
    side_by_side_df = build_side_by_side(lineage_df, baseline_agg, structured_agg)
    side_by_side_df = enrich_side_by_side_verification(side_by_side_df)

    review_path = resolve_review_csv_path(args.review_csv, output_dir)
    if review_path:
        side_by_side_df = merge_manual_review(side_by_side_df, review_path)

    summary_df = build_summary(side_by_side_df)
    concordance_df = build_val_llm_concordance_summary(side_by_side_df)
    review_queue_df = build_manual_review_queue(side_by_side_df, args.review_include_source_limited)
    gold_df = build_gold_facts(
        side_by_side_df,
        gold_require_manual=args.gold_require_manual,
        trust_fill_candidate_auto=args.trust_fill_candidate_auto,
    )

    save_parquet(lineage_df, output_dir / "llm_lineage.parquet")
    lineage_df.to_csv(output_dir / "llm_lineage.csv", index=False)
    save_parquet(side_by_side_df, output_dir / "llm_side_by_side.parquet")
    side_by_side_df.to_csv(output_dir / "llm_side_by_side.csv", index=False)
    summary_df.to_csv(output_dir / "llm_validation_summary.csv", index=False)

    save_parquet(concordance_df, output_dir / "val_llm_concordance_summary.parquet")
    concordance_df.to_csv(output_dir / "val_llm_concordance_summary.csv", index=False)

    queue_path = output_dir / "llm_manual_review_queue.csv"
    pending_path = output_dir / "llm_manual_review_queue_pending.csv"
    if queue_path.is_file():
        try:
            existing_q = pd.read_csv(queue_path)
            has_manual = (
                "verification_status" in existing_q.columns
                and existing_q["verification_status"].dropna().astype(str).str.strip().ne("").any()
            )
        except Exception:
            has_manual = False
        if has_manual:
            review_queue_df.to_csv(pending_path, index=False)
            LOG.info(
                "Preserving %s with manual verification_status; wrote fresh discordant list to %s",
                queue_path.name,
                pending_path.name,
            )
        else:
            review_queue_df.to_csv(queue_path, index=False)
    else:
        review_queue_df.to_csv(queue_path, index=False)
    save_parquet(gold_df, output_dir / "gold_llm_verified_facts.parquet")

    md_sql = Path(args.write_motherduck_sql) if args.write_motherduck_sql else output_dir / "motherduck_setup.sql"
    write_motherduck_sql(
        md_sql,
        output_dir / "llm_side_by_side.parquet",
        output_dir / "val_llm_concordance_summary.parquet",
        output_dir / "gold_llm_verified_facts.parquet",
    )

    if args.motherduck_attach:
        maybe_motherduck_attach(concordance_df)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path.relative_to(ROOT)) if input_path.is_relative_to(ROOT) else str(input_path),
        "source_notes_path": str(notes_path.relative_to(ROOT)),
        "db_path": str(Path(args.db_path)),
        "workspace_dir": str(output_dir.relative_to(ROOT)) if output_dir.is_relative_to(ROOT) else str(output_dir),
        "review_csv_merged": str(review_path) if review_path else None,
        "gold_require_manual": bool(args.gold_require_manual),
        "trust_fill_candidate_auto": bool(args.trust_fill_candidate_auto),
        "llm_rows": int(len(llm_df)),
        "lineage_rows": int(len(lineage_df)),
        "side_by_side_rows": int(len(side_by_side_df)),
        "gold_rows": int(len(gold_df)),
        "manual_review_queue_rows": int(len(review_queue_df)),
        "unique_patients": int(side_by_side_df["research_id"].nunique()),
        "domains": sorted(side_by_side_df["comparison_domain"].dropna().unique().tolist()),
        "algorithm_status_counts": summary_df.to_dict("records"),
        "verification_status_counts": concordance_df.to_dict("records"),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    write_report(output_dir, input_path, side_by_side_df, summary_df, gold_df)

    LOG.info("Validation workspace written to %s", output_dir)


if __name__ == "__main__":
    main()