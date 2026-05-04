#!/usr/bin/env python3
"""mig_281 — promote Snowflake AI_CLASSIFY NLP results into MD canonicals.

Pulls the full-scale Snowflake NLP result tables for smoking, thyroid-family
history, and vascular invasion, appends them to the live MotherDuck NLP tables,
and rebuilds the corresponding canonical_patient_master rollup columns.

Secrets are read from macOS Keychain / environment for Snowflake and from
motherduck.local.toml via motherduck_client for MotherDuck. Token values are
never logged.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))
sys.path.insert(0, str(REPO_ROOT / "snowflake_trial" / "scripts"))

from _md_connect import PUBLICATION_DB, connect_locked  # noqa: E402
from _sf_client import get_cursor  # noqa: E402
from motherduck_client import token_mode  # noqa: E402


ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
MODEL_TAG = "AI_CLASSIFY_snowflake_cortex_20260503"
RUN_STAMP = "20260503"
OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = OUTPUT_DIR / "mig_281_apply_log.txt"
REPORT_PATH = OUTPUT_DIR / "mig_281_coverage_uplift_report.csv"
STAGE_DIR = OUTPUT_DIR / "mig_281_sf_stage"
STAGE_DIR.mkdir(parents=True, exist_ok=True)


SF_TABLES = {
    "smoking": ("NLP_SMOKING_FULL_RESULTS_v1", "SMOKING_STATUS", 3541),
    "family": ("NLP_FAMILY_HX_THYROID_FULL_RESULTS_v1", "FAMILY_HX_STATUS", 3534),
    "vascular": ("NLP_VASC_INVASION_FULL_RESULTS_v1", "VASC_INVASION_STATUS", 806),
}


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def gate(cond: bool, msg: str) -> None:
    if not cond:
        raise SystemExit(f"GATE FAILED: {msg}")
    log(f"gate OK: {msg}")


def ensure_snowflake_pat() -> None:
    if os.environ.get("SNOWFLAKE_PAT"):
        return
    try:
        pat = subprocess.check_output(
            [
                "security",
                "find-generic-password",
                "-a",
                os.environ.get("USER", ""),
                "-s",
                "THYROID_2026_SNOWFLAKE_PAT",
                "-w",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception as exc:
        raise SystemExit(
            "SNOWFLAKE_PAT missing and Keychain service "
            "THYROID_2026_SNOWFLAKE_PAT is unavailable"
        ) from exc
    gate(bool(pat), "Snowflake PAT loaded from Keychain (value suppressed)")
    os.environ["SNOWFLAKE_PAT"] = pat


def stable_note_row_id(domain: str, rid: Any, note_type: Any, note_index: Any) -> str:
    raw = f"mig281|{domain}|{rid}|{note_type}|{note_index}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def json_dumps(value: Any) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str, sort_keys=True)
    except TypeError:
        return json.dumps(str(value))


def fetch_sf_results() -> dict[str, pd.DataFrame]:
    ensure_snowflake_pat()
    ctx, cur = get_cursor()
    out: dict[str, pd.DataFrame] = {}
    try:
        cur.execute("SHOW TABLES LIKE 'NLP_%_RESULTS_v1' IN THYROID_VALIDATION.PUBLIC")
        log(f"Snowflake NLP result table inventory rows={len(cur.fetchall())}")
        for domain, (table, status_col, expected_min) in SF_TABLES.items():
            cur.execute(
                f"""
                SELECT
                    RESEARCH_ID::VARCHAR AS research_id,
                    NOTE_TYPE::VARCHAR AS note_type,
                    NOTE_INDEX::VARCHAR AS note_index,
                    TO_VARCHAR(CLASSIFICATION_RAW) AS classification_raw,
                    {status_col}::VARCHAR AS status,
                    CLASSIFIED_AT::TIMESTAMP_NTZ AS classified_at,
                    LLM_MODEL::VARCHAR AS sf_llm_model
                FROM THYROID_VALIDATION.PUBLIC.{table}
                """
            )
            rows = cur.fetchall()
            cols = [c[0].lower() for c in cur.description]
            df = pd.DataFrame(rows, columns=cols)
            log(f"Snowflake pull {table}: rows={len(df)}")
            gate(len(df) >= expected_min, f"{table} rows >= {expected_min}")
            out[domain] = df
    finally:
        cur.close()
        ctx.close()
    return out


def normalize_timestamp(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def build_pmh_rows(smoking: pd.DataFrame, family: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for domain, df in (("smoking", smoking), ("family_hx_thyroid", family)):
        for rec in df.to_dict(orient="records"):
            status = rec["status"]
            entities: list[dict[str, Any]] = []
            if status and "unknown_or_not_mentioned" not in str(status):
                if domain == "smoking":
                    entity = {
                        "entity_type": "smoking_status",
                        "entity_value": status,
                        "present_or_negated": "present",
                        "confidence": 0.93,
                        "source": "snowflake_ai_classify",
                    }
                else:
                    entity = {
                        "entity_type": "family_hx_thyroid_cancer",
                        "entity_value": status,
                        "present_or_negated": (
                            "present" if str(status).endswith("_present") else "negated"
                        ),
                        "confidence": 0.84,
                        "source": "snowflake_ai_classify",
                    }
                entity["classification_raw"] = json.loads(json_dumps(rec["classification_raw"]))
                entities.append(entity)

            rid = str(rec["research_id"])
            note_type = str(rec["note_type"])
            note_index = str(rec["note_index"])
            rows.append(
                {
                    "note_row_id": stable_note_row_id(domain, rid, note_type, note_index),
                    "research_id": rid,
                    "note_type": note_type,
                    "note_date": None,
                    "domain": "past_medical_hx",
                    "llm_model": MODEL_TAG,
                    "llm_base_url": "snowflake_cortex_ai_classify",
                    "extracted_at": normalize_timestamp(rec["classified_at"]),
                    "result_json": json.dumps(
                        {"entities": entities, "classification_label": status},
                        sort_keys=True,
                    ),
                    "linkage_date": None,
                    "source_workbook": "THYROID_VALIDATION",
                    "source_sheet": SF_TABLES["smoking" if domain == "smoking" else "family"][0],
                    "source_column": domain,
                    "note_index": note_index,
                    "preprocess_batch_id": "mig_281_sf_to_md",
                    "preprocessed_at_utc": datetime.now(timezone.utc).isoformat(),
                    "preprocess_script_version": "mig_281",
                    "entity_domain": domain,
                    "llm_provider": "snowflake_cortex",
                    "llm_sdk": "snowflake_ai_classify",
                    "llm_sdk_version": None,
                    "provider_returned_model": rec["sf_llm_model"],
                    "provider_system_fingerprint": None,
                }
            )
    return pd.DataFrame(rows)


def vasc_status_to_json(status: str | None, raw: Any) -> dict[str, Any]:
    label = status or "vascular_invasion_unknown_or_not_mentioned"
    value = "unknown"
    extent = None
    if label == "vascular_invasion_absent":
        value = "absent"
    elif label == "vascular_invasion_present":
        value = "present"
    elif label == "vascular_invasion_focal":
        value = "present"
        extent = "focal"
    elif label == "vascular_invasion_extensive":
        value = "present"
        extent = "extensive"
    return {
        "vascular_invasion": value,
        "vascular_invasion_extent": extent,
        "lvi_collapsed": value if value in {"present", "absent"} else "unknown",
        "confidence": "snowflake_ai_classify",
        "classification_label": label,
        "classification_raw": json.loads(json_dumps(raw)),
    }


def build_vascular_rows(vascular: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    build_ts = datetime.now(timezone.utc).isoformat()
    for rec in vascular.to_dict(orient="records"):
        rid = str(rec["research_id"])
        note_type = str(rec["note_type"])
        note_index = str(rec["note_index"])
        parsed = vasc_status_to_json(rec["status"], rec["classification_raw"])
        rows.append(
            {
                "note_row_id": stable_note_row_id("vascular_invasion", rid, note_type, note_index),
                "research_id": rid,
                "note_type": note_type,
                "note_index": note_index,
                "source_workbook": "THYROID_VALIDATION",
                "source_sheet": SF_TABLES["vascular"][0],
                "source_column": "vascular_invasion",
                "parsed_json": json.dumps(parsed, sort_keys=True),
                "raw_llm_response": json_dumps(rec["classification_raw"]),
                "error": 0,
                "extracted_at": normalize_timestamp(rec["classified_at"]),
                "llm_model": MODEL_TAG,
                "elapsed_s": None,
                "build_ts": build_ts,
            }
        )
    return pd.DataFrame(rows)


def write_stage_files(sf: dict[str, pd.DataFrame]) -> tuple[Path, Path]:
    pmh = build_pmh_rows(sf["smoking"], sf["family"])
    vasc = build_vascular_rows(sf["vascular"])
    pmh_path = STAGE_DIR / "mig_281_pmhx.parquet"
    vasc_path = STAGE_DIR / "mig_281_vascular.parquet"
    pmh.to_parquet(pmh_path, index=False)
    vasc.to_parquet(vasc_path, index=False)
    log(f"staged PMH rows={len(pmh)} to {pmh_path}")
    log(f"staged vascular rows={len(vasc)} to {vasc_path}")
    return pmh_path, vasc_path


def fq(name: str) -> str:
    return f'"{PUBLICATION_DB}".main."{name}"'


def fq_archive(name: str) -> str:
    return f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"."{name}"'


def archive_if_absent(con: Any, archive_name: str, select_sql: str) -> None:
    exists = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_catalog=? AND table_schema=? AND table_name=?
        """,
        [ARCHIVE_DB, ARCHIVE_SCHEMA, archive_name],
    ).fetchone()
    if exists:
        log(f"archive exists, leaving intact: {archive_name}")
        return
    con.execute(f"CREATE TABLE {fq_archive(archive_name)} AS {select_sql}")
    log(f"created archive: {archive_name}")


def apply_to_md(pmh_path: Path, vasc_path: Path) -> pd.DataFrame:
    con = connect_locked()
    log(f"Connected MotherDuck publication DB (token_mode={token_mode()})")
    archive_if_absent(
        con,
        f"note_entities_llm_past_medical_hx_pre_mig281_{RUN_STAMP}",
        f"SELECT * FROM {fq('note_entities_llm_past_medical_hx')}",
    )
    archive_if_absent(
        con,
        f"note_entities_llm_vascular_invasion_v2_pre_mig281_{RUN_STAMP}",
        f"SELECT * FROM {fq('note_entities_llm_vascular_invasion_v2')}",
    )
    archive_if_absent(
        con,
        f"cpm_nlp_cols_pre_mig281_{RUN_STAMP}",
        f"""
        SELECT research_id,
               pmhx_nlp_smoking_status, nsqip_smoker,
               pmhx_nlp_family_hx_thyroid, pmhx_nlp_family_hx_cancer,
               vascular_invasion_final,
               cpm_built_at
        FROM {fq('canonical_patient_master')}
        """,
    )

    con.execute(
        "DELETE FROM main.note_entities_llm_past_medical_hx WHERE llm_model = ?",
        [MODEL_TAG],
    )
    con.execute(
        "DELETE FROM main.note_entities_llm_vascular_invasion_v2 WHERE llm_model = ?",
        [MODEL_TAG],
    )
    log("removed prior mig_281 rows by llm_model tag (idempotence)")

    con.execute(
        f"""
        INSERT INTO main.note_entities_llm_past_medical_hx BY NAME
        SELECT * FROM read_parquet('{pmh_path.as_posix()}')
        """
    )
    con.execute(
        f"""
        INSERT INTO main.note_entities_llm_vascular_invasion_v2 BY NAME
        SELECT
            note_row_id, research_id, note_type, note_index,
            source_workbook, source_sheet, source_column,
            CAST(parsed_json AS JSON) AS parsed_json,
            raw_llm_response, error,
            CAST(extracted_at AS TIMESTAMP) AS extracted_at,
            llm_model, elapsed_s,
            CAST(build_ts AS TIMESTAMP) AS build_ts
        FROM read_parquet('{vasc_path.as_posix()}')
        """
    )
    log("inserted staged rows into MD NLP canonical tables")

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE mig281_smoking_rollup AS
        WITH parsed AS (
            SELECT research_id,
                   json_extract_string(CAST(ent AS JSON), '$.entity_value') AS val
            FROM main.note_entities_llm_past_medical_hx,
                 UNNEST(CAST(json_extract(CAST(result_json AS JSON), '$.entities') AS JSON[])) AS u(ent)
            WHERE llm_model = ?
              AND entity_domain = 'smoking'
        )
        SELECT research_id,
               CASE
                   WHEN COUNT(*) FILTER (WHERE val='current_smoker') > 0 THEN 'current'
                   WHEN COUNT(*) FILTER (WHERE val='former_smoker') > 0 THEN 'former'
                   WHEN COUNT(*) FILTER (WHERE val='never_smoker') > 0 THEN 'never'
                   ELSE NULL
               END AS smoking_status
        FROM parsed
        GROUP BY research_id
        """,
        [MODEL_TAG],
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE mig281_family_rollup AS
        WITH parsed AS (
            SELECT research_id,
                   json_extract_string(CAST(ent AS JSON), '$.entity_value') AS val
            FROM main.note_entities_llm_past_medical_hx,
                 UNNEST(CAST(json_extract(CAST(result_json AS JSON), '$.entities') AS JSON[])) AS u(ent)
            WHERE llm_model = ?
              AND entity_domain = 'family_hx_thyroid'
        )
        SELECT research_id,
               CASE
                   WHEN COUNT(*) FILTER (WHERE val='family_hx_thyroid_cancer_present') > 0 THEN TRUE
                   WHEN COUNT(*) FILTER (WHERE val='family_hx_thyroid_cancer_absent') > 0 THEN FALSE
                   ELSE NULL
               END AS family_hx_thyroid
        FROM parsed
        GROUP BY research_id
        """,
        [MODEL_TAG],
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE mig281_vasc_rollup AS
        SELECT research_id,
               CASE
                   WHEN COUNT(*) FILTER (WHERE json_extract_string(parsed_json, '$.classification_label')='vascular_invasion_extensive') > 0 THEN 'extensive'
                   WHEN COUNT(*) FILTER (WHERE json_extract_string(parsed_json, '$.classification_label')='vascular_invasion_focal') > 0 THEN 'focal'
                   WHEN COUNT(*) FILTER (WHERE json_extract_string(parsed_json, '$.classification_label')='vascular_invasion_present') > 0 THEN 'present_ungraded'
                   WHEN COUNT(*) FILTER (WHERE json_extract_string(parsed_json, '$.classification_label')='vascular_invasion_absent') > 0 THEN 'absent'
                   ELSE NULL
               END AS vascular_invasion_final
        FROM main.note_entities_llm_vascular_invasion_v2
        WHERE llm_model = ?
        GROUP BY research_id
        """,
        [MODEL_TAG],
    )

    con.execute(
        """
        UPDATE main.canonical_patient_master AS pm
        SET pmhx_nlp_smoking_status = r.smoking_status,
            cpm_built_at = CURRENT_TIMESTAMP
        FROM mig281_smoking_rollup AS r
        WHERE pm.research_id = CAST(r.research_id AS VARCHAR)
          AND r.smoking_status IS NOT NULL
        """
    )
    con.execute(
        """
        UPDATE main.canonical_patient_master AS pm
        SET pmhx_nlp_family_hx_thyroid = r.family_hx_thyroid,
            cpm_built_at = CURRENT_TIMESTAMP
        FROM mig281_family_rollup AS r
        WHERE pm.research_id = CAST(r.research_id AS VARCHAR)
          AND r.family_hx_thyroid IS NOT NULL
        """
    )
    con.execute(
        """
        UPDATE main.canonical_patient_master AS pm
        SET vascular_invasion_final = r.vascular_invasion_final,
            cpm_built_at = CURRENT_TIMESTAMP
        FROM mig281_vasc_rollup AS r
        WHERE pm.research_id = CAST(r.research_id AS VARCHAR)
          AND pm.vascular_invasion_final IS NULL
          AND r.vascular_invasion_final IN ('present_ungraded','focal','extensive')
        """
    )
    log("rebuilt CPM NLP rollup columns")

    report = con.execute(
        f"""
        SELECT 'smoking' AS slice,
               COUNT(*) FILTER (WHERE pmhx_nlp_smoking_status IS NOT NULL) AS n_known_post,
               (SELECT COUNT(*) FILTER (WHERE pmhx_nlp_smoking_status IS NOT NULL)
                FROM {fq_archive('cpm_nlp_cols_pre_mig281_' + RUN_STAMP)}) AS n_known_pre
        FROM main.canonical_patient_master
        UNION ALL
        SELECT 'family_hx_thyroid',
               COUNT(*) FILTER (WHERE pmhx_nlp_family_hx_thyroid IS NOT NULL),
               (SELECT COUNT(*) FILTER (WHERE pmhx_nlp_family_hx_thyroid IS NOT NULL)
                FROM {fq_archive('cpm_nlp_cols_pre_mig281_' + RUN_STAMP)})
        FROM main.canonical_patient_master
        UNION ALL
        SELECT 'vasc_invasion',
               COUNT(*) FILTER (WHERE vascular_invasion_final IS NOT NULL),
               (SELECT COUNT(*) FILTER (WHERE vascular_invasion_final IS NOT NULL)
                FROM {fq_archive('cpm_nlp_cols_pre_mig281_' + RUN_STAMP)})
        FROM main.canonical_patient_master
        """
    ).fetchdf()
    report.to_csv(REPORT_PATH, index=False)
    log(f"coverage report written: {REPORT_PATH}")

    cpm_rows, cpm_distinct, cpm_built_nulls = con.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               COUNT(*) FILTER (WHERE cpm_built_at IS NULL)
        FROM main.canonical_patient_master
        """
    ).fetchone()
    gate(cpm_rows == 10871, "CPM row count remains 10,871")
    gate(cpm_distinct == 10871, "CPM distinct research_id remains 10,871")
    gate(cpm_built_nulls == 0, "CPM cpm_built_at has no NULLs")

    pmh_added = con.execute(
        "SELECT COUNT(*) FROM main.note_entities_llm_past_medical_hx WHERE llm_model=?",
        [MODEL_TAG],
    ).fetchone()[0]
    vasc_added = con.execute(
        "SELECT COUNT(*) FROM main.note_entities_llm_vascular_invasion_v2 WHERE llm_model=?",
        [MODEL_TAG],
    ).fetchone()[0]
    gate(pmh_added == 7075, "PMH appended row count is 7,075")
    gate(vasc_added == 806, "vascular appended row count is 806")

    summary = (
        "mig_281: Promoted Snowflake AI_CLASSIFY NLP results to MD canonicals. "
        f"Appended {pmh_added} PMH rows and {vasc_added} vascular rows. "
        "Rebuilt CPM cols pmhx_nlp_smoking_status, "
        "pmhx_nlp_family_hx_thyroid, vascular_invasion_final. "
        "llm_model=AI_CLASSIFY_snowflake_cortex_20260503. "
        "Closes CF-VASC-INVASION-749-UNDERFIRES + CF-SMOKING-COVERAGE-GAP + "
        "CF-FAMILY-HX-COVERAGE-GAP at canonical level."
    )
    con.execute("DELETE FROM main.signoff_migration WHERE mig_id='mig_281'")
    con.execute(
        """
        INSERT INTO main.signoff_migration
            (mig_id, signed_off_at, by_actor, summary)
        VALUES ('mig_281', CURRENT_TIMESTAMP, 'cursor_composer_mig281', ?)
        """,
        [summary],
    )
    log("inserted signoff_migration row for mig_281")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Pull sources and stage files only")
    args = parser.parse_args()
    if LOG_PATH.exists():
        LOG_PATH.unlink()
    log("mig_281 starting")
    sf = fetch_sf_results()
    pmh_path, vasc_path = write_stage_files(sf)
    if args.dry_run:
        log("dry-run complete; MD not modified")
        return
    report = apply_to_md(pmh_path, vasc_path)
    log("coverage uplift:")
    for rec in report.to_dict(orient="records"):
        log(f"  {rec['slice']}: {rec['n_known_pre']} -> {rec['n_known_post']}")
    log("mig_281 complete")


if __name__ == "__main__":
    main()
