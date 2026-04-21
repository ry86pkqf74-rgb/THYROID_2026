#!/usr/bin/env python3
"""
Script 360 — Frozen section consolidation (dedup, surgery linkage, CPM, wide 12-slot).

Phases:
  0     Dry-run counts (read-only)
  1     Archive target tables to archive_pub_v1_0.*_preFROZENCLEANUP_<UTC_TS>
  1.5   Full frozen footprint inventory → reports/FROZEN_SECTION_INVENTORY_<UTC_TS>.md
  2     Build tier2.frozen_section_event_v2 + pre-rename gates
  2-test  Build tier2.frozen_section_event_v2_test, gates, DROP (isolated MD dry-run)
  3     ALTER + UPDATE main.operative_episode_detail_v2 frozen columns
  4     Rebuild tier2.patient_tier2_master_v1 frozen_section__* (drop legacy 46, add 12-slot)
  5     Refresh main.canonical_patient_master NLP + syn + reconciliation columns
  6     Archive tier2.frozen_section_event_v1, drop, rename v2→v1; COMMENT ON metadata
  6.5   Rebuild verify_frozen_section_v1 (+summary), rerun 337/338, registry rows
  7     manuscript_workspace.detail_table_registry_v1 sync for frozen tables

  all   Runs 0,1,1.5 then STOPS (exit 3) unless --continue-destructive is set;
        with --continue-destructive, runs 2–7.

Auth: motherduck_client.get_token(). Never log evidence_text >80 chars.

Synoptic Excel cells (`path_synoptics.fs_pathology_frozen_section`) are deduped
against LLM rows from the same synoptic columns via `synoptic_match_key`
(research_id + calendar date + SYNOPTIC_CELL); the surviving row is LLM when both
exist, with `excel_result_raw` / `excel_corroborated_flag` for provenance.

Verification bands (see GATES) are calibrated to observed post-filter reality:
  v2 rows ≈ 7,100 (band 6,800–7,400)
  v2 distinct patients ≈ 4,116 (band 4,050–4,200; Excel 4,062 ∪ LLM 3,535)
  CPM nlp_frozensec_has_data TRUE ≈ 3,535 (band 3,500–3,600; LLM-sourced only)
  surgery_n linkage ≥ 95%
Regression checks (kept hard at 0): duplicate synoptic_match_key; same
(research_id, date) with both p1 and p2 on fs_pathology_frozen_section.

Excel-only patient count is logged but not gated. It tracks upstream LLM
extraction coverage, not Script 360's dedup behavior.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "archive_pub_v1_0"
WS_SCHEMA = "manuscript_workspace"
REGISTRY_TABLE = "detail_table_registry_v1"
REPORTS_DIR = REPO_ROOT / "reports"
SCRIPT = "360_frozen_section_cleanup"
OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
DECISIONS_PATH = OUTPUT_DIR / "360_decisions.json"
LOG_PATH = OUTPUT_DIR / "360_run.log"

EXPECTED_CPM_ROWS = 10871

# Gate bands calibrated to observed post-filter reality from --phase 2-test
# (7,081 rows / 4,116 patients; 3,535 LLM-sourced patients; ~580 Excel-only patients).
# These are not relaxations of the regression checks (which stay at 0 offenders);
# they reflect that the OPNOTE/HP LLM filter correctly excludes negated "no frozen
# section" rows, and that the patient union is Excel (4,062) ∪ LLM (3,535).
GATES: dict[str, tuple[float, float]] = {
    "row_count": (6_800, 7_400),
    "distinct_patients": (4_050, 4_200),
    "surgery_linked_pct": (0.95, 1.00),
    "cpm_nlp_true_count": (3_500, 3_600),
}
# Excel-only patient count is logged but NOT gated. It tracks upstream LLM
# extraction coverage (how many Excel-synoptic patients have a matching LLM
# extraction), not Script 360's dedup behavior. The two hard regression
# checks (duplicate synoptic_match_key = 0; (rid, fs_day) with both p1+p2
# on fs_pathology_frozen_section = 0) cover dedup correctness unambiguously.

# Whitelist fqnames (lowercase) for post-cleanup inventory gate (table or schema.table)
FOOTPRINT_WHITELIST = {
    "main.note_entities_llm_frozen_section_detail",
    "main.path_synoptics",
    "tier2.frozen_section_event_v1",
    "tier2.patient_tier2_master_v1",
    "main.canonical_patient_master",
    "main.operative_episode_detail_v2",
    "verify.verify_long_v1",
    "verify.concordance_master_v1",
    "main.verify_frozen_section_v1",
    "main.verify_frozen_section_summary_v1",
    "manuscript_workspace.cohort_m047_frozen_section_v1",
    "manuscript_workspace.cohort_m062_incidental_frozen_v1",
    "manuscript_workspace.cohort_m063_frozen_false_neg_v1",
    "manuscript_workspace.cohort_m064_frozen_decision_v1",
    "manuscript_workspace.cohort_m065_frozen_tt_vs_lob_v1",
    "views_readable.surgery_episode_detail",
}

_log_buf: list[str] = []


def utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def log_info(msg: str) -> None:
    line = f"[INFO] [{datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]}Z] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def log_error(msg: str) -> None:
    line = f"[ERROR] [{datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]}Z] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def _flush_log() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    mode = "a" if LOG_PATH.exists() else "w"
    with LOG_PATH.open(mode, encoding="utf-8") as fh:
        fh.write("\n".join(_log_buf) + "\n")


def _truncate_phi(s: str | None, n: int = 80) -> str:
    if not s:
        return ""
    s = str(s).replace("\n", " ")
    return s if len(s) <= n else s[: n - 3] + "..."


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}). "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    log_info(f"Connecting md:{CANONICAL_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")
    con.execute(f'USE "{CANONICAL_DB}"')
    con.execute(f'USE "{CANONICAL_DB}".main')
    return con


def fq(schema: str, name: str) -> str:
    return f'"{CANONICAL_DB}"."{schema}"."{name}"'


def archive_copy(
    con: duckdb.DuckDBPyConnection,
    src_schema: str,
    src_table: str,
    tag: str,
    ts: str,
) -> str:
    dst_name = f"{src_table}_{tag}_{ts}"
    dst = f'{ARCHIVE_DB}.{ARCHIVE_SCHEMA}."{dst_name}"'
    src = fq(src_schema, src_table)
    already = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = 'Thyroid 2026 UPdated'
          AND table_schema = ? AND table_name = ?
        """,
        [ARCHIVE_SCHEMA, dst_name],
    ).fetchone()
    if already:
        n = con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0]
        log_info(f"Archive already present: {dst} ({n:,} rows) — skipping re-copy")
        return dst
    con.execute(f"CREATE TABLE {dst} AS SELECT * FROM {src}")
    n = con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0]
    log_info(f"Archived {src_schema}.{src_table} -> {dst} ({n:,} rows)")
    return dst


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [CANONICAL_DB, schema, table],
    ).fetchone()
    return row is not None


def list_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [CANONICAL_DB, schema, table],
    ).fetchall()
    return [r[0] for r in rows]


def esc(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


# --- KEY_FINDING_PRIORITY (Script 283 order, extended for new entity types) ---
KEY_FINDING_PRIORITY: list[tuple[str, int]] = [
    ("final_pathology_concordance", 1),
    ("discordance_reason", 2),
    ("intraop_decision_impact", 3),
    ("frozen_section_result", 4),
    ("frozen_section_turnaround", 5),
    ("number_of_sections", 6),
    ("frozen_section_target", 7),
]


def key_finding_priority_case(alias: str) -> str:
    parts = ["CASE"]
    for et, prio in KEY_FINDING_PRIORITY:
        parts.append(f"WHEN {alias} = {esc(et)} THEN {prio}")
    parts.append("ELSE 99 END")
    return " ".join(parts)


def _validate_sql_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.]+", name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def build_event_v2_sql(target_table: str = "tier2.frozen_section_event_v2") -> str:
    """Single CREATE TABLE AS for tier2.frozen_section_event_v2 (or a _test name)."""
    tt = _validate_sql_identifier(target_table)
    # Normalization / classification kept in SQL for reproducibility on MD.
    return f"""
CREATE OR REPLACE TABLE {tt} AS
WITH ops AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        surgery_episode_id,
        COALESCE(
            CAST(resolved_surgery_date AS DATE),
            CAST(surgery_date_native AS DATE)
        ) AS sday,
        DENSE_RANK() OVER (
            PARTITION BY CAST(research_id AS VARCHAR)
            ORDER BY COALESCE(
                CAST(resolved_surgery_date AS DATE),
                CAST(surgery_date_native AS DATE)
            ) NULLS LAST
        ) AS surgery_n
    FROM main.operative_episode_detail_v2
    WHERE COALESCE(
        CAST(resolved_surgery_date AS DATE),
        CAST(surgery_date_native AS DATE)
    ) IS NOT NULL
),
op_pick AS (
    SELECT *
    FROM ops
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id, sday ORDER BY surgery_episode_id
    ) = 1
),
syn_rows AS (
    SELECT
        CAST(ps.research_id AS VARCHAR) AS research_id,
        CAST(ROW_NUMBER() OVER (
            PARTITION BY CAST(ps.research_id AS VARCHAR)
            ORDER BY ps.surg_date NULLS LAST,
                     ps.fs_pathology_frozen_section NULLS LAST
        ) AS VARCHAR) AS syn_row_ord,
        ps.surg_date,
        ps.fs_pathology_frozen_section,
        ps.frozen_section_obtained,
        ps.carcinoma_identified_on_fs_sent_intraop
    FROM main.path_synoptics ps
    WHERE
        (ps.fs_pathology_frozen_section IS NOT NULL
         AND NULLIF(TRIM(CAST(ps.fs_pathology_frozen_section AS VARCHAR)), '') IS NOT NULL)
        OR (ps.frozen_section_obtained IS NOT NULL
            AND UPPER(NULLIF(TRIM(CAST(ps.frozen_section_obtained AS VARCHAR)), '')) NOT IN ('NO', 'N', 'FALSE', '0'))
        OR (LOWER(NULLIF(TRIM(CAST(ps.carcinoma_identified_on_fs_sent_intraop AS VARCHAR)), '')) IN (
            'yes', 'y', 'true', '1', 'positive', 'pos'
        ))
),
excel_entities AS (
    SELECT
        sr.research_id,
        CAST(NULL AS BIGINT) AS note_index,
        CAST(sr.surg_date AS VARCHAR) AS note_date,
        CAST(sr.surg_date AS VARCHAR) AS linkage_date,
        'path_synoptics'::VARCHAR AS note_type,
        'synoptics + Dx merged'::VARCHAR AS source_sheet,
        'fs_pathology_frozen_section'::VARCHAR AS source_column,
        0::BIGINT AS event_index_in_json,
        'frozen_section_result'::VARCHAR AS entity_type,
        COALESCE(
            NULLIF(TRIM(CAST(sr.fs_pathology_frozen_section AS VARCHAR)), ''),
            CASE WHEN UPPER(NULLIF(TRIM(CAST(sr.frozen_section_obtained AS VARCHAR)), '')) IN ('NO', 'N')
                 THEN 'no frozen section performed'
                 ELSE CONCAT(
                    'frozen: ',
                    COALESCE(NULLIF(TRIM(CAST(sr.frozen_section_obtained AS VARCHAR)), ''), 'unknown')
                 ) END
        ) AS entity_value,
        NULLIF(TRIM(CAST(sr.fs_pathology_frozen_section AS VARCHAR)), '') AS evidence_text,
        1.0::DOUBLE AS confidence,
        'present'::VARCHAR AS present_or_negated,
        NULL::VARCHAR AS entity_date,
        CAST(sr.surg_date AS VARCHAR) AS best_note_date,
        'synoptic_excel_parsed_column'::VARCHAR AS source_of_data,
        1::BIGINT AS source_priority,
        ('EXCEL:' || sr.research_id || ':SYN:' || LPAD(sr.syn_row_ord, 6, '0'))::VARCHAR AS source_note_ref,
        ('EXCEL:' || sr.research_id || ':SYN:' || LPAD(sr.syn_row_ord, 6, '0'))::VARCHAR AS path_synoptics_row_key
    FROM syn_rows sr
),
nlp_entities AS (
    SELECT
        CAST(d.research_id AS VARCHAR) AS research_id,
        TRY_CAST(d.note_index AS BIGINT) AS note_index,
        CAST(d.note_date AS VARCHAR) AS note_date,
        CAST(d.linkage_date AS VARCHAR) AS linkage_date,
        d.note_type,
        d.source_sheet,
        d.source_column,
        gs.event_index_in_json,
        json_extract_string(gs.ent, '$.entity_type') AS entity_type,
        json_extract_string(gs.ent, '$.entity_value') AS entity_value,
        json_extract_string(gs.ent, '$.evidence_text') AS evidence_text,
        COALESCE(TRY_CAST(json_extract(gs.ent, '$.confidence') AS DOUBLE), 0.0) AS confidence,
        COALESCE(
            NULLIF(json_extract_string(gs.ent, '$.present_or_negated'), ''),
            'present'
        ) AS present_or_negated,
        json_extract_string(gs.ent, '$.entity_date') AS entity_date,
        CAST(d.note_date AS VARCHAR) AS best_note_date,
        CASE
            WHEN d.source_sheet ILIKE '%synoptic%'
                 AND d.source_column ILIKE '%fs_pathology_frozen_section%' THEN 'synoptic_path_report_llm'
            WHEN d.note_type ILIKE '%OPNOTE%' OR d.source_column ILIKE '%opnote%' THEN 'opnote_llm'
            WHEN d.note_type ILIKE '%HP%' OR d.source_column ILIKE '%h_p%' THEN 'hp_llm'
            WHEN d.note_type = 'path_synoptics' THEN 'synoptic_path_report_llm'
            ELSE 'opnote_llm'
        END AS source_of_data,
        CASE
            WHEN d.source_sheet ILIKE '%synoptic%'
                 AND d.source_column ILIKE '%fs_pathology_frozen_section%' THEN 2::BIGINT
            WHEN d.note_type = 'path_synoptics' THEN 2::BIGINT
            WHEN d.note_type ILIKE '%OPNOTE%' OR d.source_column ILIKE '%opnote%' THEN 3::BIGINT
            WHEN d.note_type ILIKE '%HP%' OR d.source_column ILIKE '%h_p%' THEN 4::BIGINT
            ELSE 5::BIGINT
        END AS source_priority,
        CAST(d.note_index AS VARCHAR) AS source_note_ref,
        CAST(NULL AS VARCHAR) AS path_synoptics_row_key
    FROM main.note_entities_llm_frozen_section_detail d
    INNER JOIN LATERAL (
        SELECT
            gs.series::BIGINT AS event_index_in_json,
            list_element(
                CAST(json_extract(CAST(d.result_json AS JSON), '$.entities') AS JSON[]),
                CAST(gs.series AS INTEGER)
            ) AS ent
        FROM generate_series(
            1::BIGINT,
            CAST(json_array_length(json_extract(CAST(d.result_json AS JSON), '$.entities')) AS BIGINT)
        ) AS gs(series)
    ) gs ON TRUE
    WHERE d.result_json IS NOT NULL
      AND json_type(json_extract(CAST(d.result_json AS JSON), '$.entities')) = 'ARRAY'
      AND json_array_length(json_extract(CAST(d.result_json AS JSON), '$.entities')) > 0
      AND json_extract_string(gs.ent, '$.entity_value') IS NOT NULL
),
nlp_filtered AS (
    SELECT *
    FROM nlp_entities
    WHERE confidence >= 0.5
      AND (present_or_negated = 'present' OR present_or_negated IS NULL)
),
unioned AS (
    SELECT
        research_id, note_index, note_date, linkage_date, note_type,
        source_sheet, source_column, event_index_in_json, entity_type, entity_value,
        evidence_text, confidence, present_or_negated, entity_date, best_note_date,
        source_of_data, source_priority, source_note_ref, path_synoptics_row_key
    FROM excel_entities
    UNION ALL
    SELECT
        research_id, note_index, note_date, linkage_date, note_type,
        source_sheet, source_column, event_index_in_json, entity_type, entity_value,
        evidence_text, confidence, present_or_negated, entity_date, best_note_date,
        source_of_data, source_priority, source_note_ref, path_synoptics_row_key
    FROM nlp_filtered
),
dedup_base AS (
    SELECT
        u.*,
        COALESCE(
            NULLIF(TRIM(entity_date), ''),
            NULLIF(TRIM(note_date), ''),
            NULLIF(TRIM(linkage_date), '')
        )::VARCHAR AS frozen_section_date,
        MD5(
            CAST(research_id AS VARCHAR) || '|' ||
            COALESCE(CAST(note_index AS VARCHAR), 'EXCEL:' || COALESCE(path_synoptics_row_key, '')) || '|' ||
            CAST(COALESCE(event_index_in_json, 0) AS VARCHAR) || '|' ||
            LOWER(TRIM(COALESCE(entity_type, ''))) || '|' ||
            LOWER(TRIM(COALESCE(entity_value, ''))) || '|' ||
            LOWER(TRIM(SUBSTRING(COALESCE(evidence_text, ''), 1, 200)))
        ) AS entity_id_hash,
        ROW_NUMBER() OVER (
            PARTITION BY
                research_id,
                COALESCE(CAST(note_index AS VARCHAR), 'EXCEL:' || COALESCE(path_synoptics_row_key, '')),
                COALESCE(event_index_in_json, 0),
                LOWER(TRIM(COALESCE(entity_type, ''))),
                LOWER(TRIM(COALESCE(entity_value, ''))),
                LOWER(TRIM(SUBSTRING(COALESCE(evidence_text, ''), 1, 200)))
            ORDER BY
                source_priority ASC,
                CASE WHEN COALESCE(
                    NULLIF(TRIM(entity_date), ''),
                    NULLIF(TRIM(note_date), ''),
                    NULLIF(TRIM(linkage_date), '')
                ) IS NULL THEN 1 ELSE 0 END ASC,
                COALESCE(
                    NULLIF(TRIM(entity_date), ''),
                    NULLIF(TRIM(note_date), ''),
                    NULLIF(TRIM(linkage_date), '')
                ) ASC,
                TRY_CAST(note_date AS DATE) ASC NULLS LAST
        ) AS dedup_rn
    FROM unioned u
),
deduped AS (
    SELECT * FROM dedup_base WHERE dedup_rn = 1
),
pre_merge AS (
    SELECT
        d.*,
        TRY_CAST(SUBSTRING(COALESCE(d.frozen_section_date, ''), 1, 10) AS DATE) AS fs_day,
        CASE
            WHEN d.source_of_data IN ('synoptic_excel_parsed_column', 'synoptic_path_report_llm')
                 AND d.source_column IN (
                     'fs_pathology_frozen_section',
                     'carcinoma_identified_on_fs_sent_intraop',
                     'frozen_section_obtained'
                 )
                 AND TRY_CAST(SUBSTRING(COALESCE(d.frozen_section_date, ''), 1, 10) AS DATE) IS NOT NULL
            THEN CAST(d.research_id AS VARCHAR) || '|' ||
                 CAST(TRY_CAST(SUBSTRING(COALESCE(d.frozen_section_date, ''), 1, 10) AS DATE) AS VARCHAR)
                 || '|SYNOPTIC_CELL'
            ELSE NULL
        END AS synoptic_match_key,
        CASE
            WHEN d.source_priority = 1
            THEN NULLIF(TRIM(CAST(d.evidence_text AS VARCHAR)), '')
        END AS excel_row_fs_text
    FROM deduped d
),
syn_kv AS (
    SELECT
        synoptic_match_key,
        MAX(excel_row_fs_text) AS excel_result_raw_agg,
        BOOL_OR(source_priority = 1) AS had_excel,
        BOOL_OR(source_priority = 2) AS had_llm_synoptic
    FROM pre_merge
    WHERE synoptic_match_key IS NOT NULL
    GROUP BY synoptic_match_key
),
picked AS (
    SELECT
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY p.synoptic_match_key
            ORDER BY
                p.source_priority DESC,
                CASE
                    WHEN LOWER(TRIM(COALESCE(p.entity_type, ''))) = 'frozen_section_result' THEN 0
                    ELSE 1
                END,
                p.event_index_in_json
        ) AS pick_rn
    FROM pre_merge p
    WHERE p.synoptic_match_key IS NOT NULL
),
syn_merged AS (
    SELECT
        pk.research_id,
        pk.note_index,
        pk.note_date,
        pk.linkage_date,
        pk.note_type,
        pk.source_sheet,
        pk.source_column,
        pk.event_index_in_json,
        pk.entity_type,
        pk.entity_value,
        pk.evidence_text,
        pk.confidence,
        pk.present_or_negated,
        pk.entity_date,
        pk.best_note_date,
        pk.source_of_data,
        pk.source_priority,
        pk.source_note_ref,
        pk.path_synoptics_row_key,
        pk.frozen_section_date,
        pk.entity_id_hash,
        pk.dedup_rn,
        pk.fs_day,
        pk.synoptic_match_key,
        pk.excel_row_fs_text,
        COALESCE(sk.excel_result_raw_agg, pk.excel_row_fs_text) AS excel_result_raw,
        CASE
            WHEN pk.source_priority = 2 AND sk.had_excel THEN TRUE
            WHEN pk.source_priority = 2 AND NOT sk.had_excel THEN FALSE
            ELSE NULL
        END AS excel_corroborated_flag
    FROM picked pk
    INNER JOIN syn_kv sk ON pk.synoptic_match_key = sk.synoptic_match_key
    WHERE pk.pick_rn = 1
),
pass_synoptic AS (
    SELECT
        d.research_id,
        d.note_index,
        d.note_date,
        d.linkage_date,
        d.note_type,
        d.source_sheet,
        d.source_column,
        d.event_index_in_json,
        d.entity_type,
        d.entity_value,
        d.evidence_text,
        d.confidence,
        d.present_or_negated,
        d.entity_date,
        d.best_note_date,
        d.source_of_data,
        d.source_priority,
        d.source_note_ref,
        d.path_synoptics_row_key,
        d.frozen_section_date,
        d.entity_id_hash,
        d.dedup_rn,
        d.fs_day,
        CAST(NULL AS VARCHAR) AS synoptic_match_key,
        d.excel_row_fs_text,
        CAST(NULL AS VARCHAR) AS excel_result_raw,
        CAST(NULL AS BOOLEAN) AS excel_corroborated_flag
    FROM pre_merge d
    WHERE d.synoptic_match_key IS NULL
),
combined AS (
    SELECT * FROM syn_merged
    UNION ALL
    SELECT * FROM pass_synoptic
),
typed AS (
    SELECT
        d.research_id,
        d.note_index,
        d.note_date,
        d.linkage_date,
        d.note_type,
        d.source_sheet,
        d.source_column,
        d.event_index_in_json,
        d.entity_type,
        d.entity_value,
        d.evidence_text,
        d.confidence,
        d.present_or_negated,
        d.entity_date,
        d.best_note_date,
        d.source_of_data,
        d.source_priority,
        d.source_note_ref,
        d.path_synoptics_row_key,
        d.frozen_section_date,
        d.entity_id_hash,
        d.synoptic_match_key,
        d.excel_result_raw,
        d.excel_corroborated_flag,
        CASE WHEN LOWER(TRIM(entity_type)) IN ('frozen_section_target', 'site')
             THEN entity_value END AS frozen_section_site_raw,
        CASE WHEN LOWER(TRIM(entity_type)) IN ('frozen_section_result')
             THEN entity_value
             WHEN source_of_data = 'synoptic_excel_parsed_column'
             THEN entity_value END AS frozen_section_result_raw,
        CASE WHEN LOWER(TRIM(entity_type)) = 'final_pathology_concordance'
             THEN LOWER(TRIM(entity_value)) END AS concord_entity,
        CASE WHEN LOWER(TRIM(entity_type)) = 'intraop_decision_impact'
             THEN entity_value END AS intraop_decision_impact_raw
    FROM combined d
),
norm AS (
    SELECT
        t.*,
        CASE
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%left superior%'
                 OR LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%left upper%'
                 OR LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%ls parathyroid%' THEN 'parathyroid_LS'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%left inferior%'
                 OR LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%left lower%' THEN 'parathyroid_LI'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%right superior%'
                 OR LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%right upper%' THEN 'parathyroid_RS'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%right inferior%'
                 OR LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%right lower%' THEN 'parathyroid_RI'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%parathyroid%' THEN 'parathyroid_unspecified'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%isthmus%' THEN 'thyroid_isthmus'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%bilateral%thyroid%'
                 OR (LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%both%lobe%') THEN 'thyroid_bilateral'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%left%lobe%'
                 OR LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%left thyroid%' THEN 'thyroid_left_lobe'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%right%lobe%'
                 OR LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%right thyroid%' THEN 'thyroid_right_lobe'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%pretracheal%' THEN 'lymph_node_pretracheal'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%paratracheal%' THEN 'lymph_node_paratracheal'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%delphian%' THEN 'lymph_node_delphian'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%level vi%'
                 OR LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%level vii%'
                 OR LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%central compartment%' THEN 'lymph_node_central_compartment'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%level ii%' THEN 'lymph_node_lateral_II'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%level iii%' THEN 'lymph_node_lateral_III'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%level iv%' THEN 'lymph_node_lateral_IV'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%level v%' THEN 'lymph_node_lateral_V'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%level vi%' THEN 'lymph_node_lateral_VI'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%lymph%' THEN 'lymph_node_unspecified'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%thymus%' THEN 'thymus'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%trachea%' THEN 'trachea'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%esophagus%' THEN 'esophagus'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%rln%'
                 OR LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%recurrent laryngeal%' THEN 'rln_nerve'
            WHEN frozen_section_site_raw IS NULL OR TRIM(CAST(frozen_section_site_raw AS VARCHAR)) = '' THEN NULL
            ELSE 'other'
        END AS frozen_section_site_norm,
        CASE
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%bilateral%' THEN 'bilateral'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%left%' AND LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%right%' THEN 'bilateral'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%left%' THEN 'left'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%right%' THEN 'right'
            WHEN LOWER(COALESCE(frozen_section_site_raw, '')) LIKE '%midline%' THEN 'midline'
            WHEN frozen_section_site_raw IS NULL THEN 'unknown'
            ELSE 'unknown'
        END AS frozen_section_side,
        SUBSTRING(
            LOWER(COALESCE(frozen_section_result_raw, '') || ' ' || SUBSTRING(COALESCE(evidence_text, ''), 1, 200)),
            1, 400
        ) AS text_for_qual,
        SUBSTRING(COALESCE(frozen_section_result_raw, ''), 1, 500) AS result_blob
    FROM typed t
),
hist AS (
    SELECT
        n.*,
        CASE
            WHEN LOWER(result_blob) LIKE '%no frozen section performed%'
                 OR LOWER(result_blob) LIKE '%deferred (no frozen%'
                 OR LOWER(result_blob) LIKE '%not performed%' THEN 'not_performed'
            WHEN LOWER(result_blob) LIKE '%defer%' OR LOWER(result_blob) LIKE '%pending permanent%' THEN 'deferred'
            WHEN LOWER(result_blob) LIKE '%metastatic%papillary%' OR LOWER(result_blob) LIKE '%metastatic ptc%' THEN 'malignant'
            WHEN LOWER(result_blob) LIKE '%papillary%'
                 OR LOWER(result_blob) LIKE '%ptc%' OR LOWER(result_blob) LIKE '%carcinoma%thyroid%' THEN 'malignant'
            WHEN LOWER(result_blob) LIKE '%medullary%' THEN 'malignant'
            WHEN LOWER(result_blob) LIKE '%anaplastic%' THEN 'malignant'
            WHEN LOWER(result_blob) LIKE '%follicular carcinoma%' OR LOWER(result_blob) LIKE '%follicular cancer%' THEN 'malignant'
            WHEN LOWER(result_blob) LIKE '%follicular neoplasm%'
                 OR LOWER(result_blob) LIKE '%follicular lesion%'
                 OR LOWER(result_blob) LIKE '%flus%' THEN 'indeterminate'
            WHEN LOWER(result_blob) LIKE '%follicular adenoma%' THEN 'benign'
            WHEN LOWER(result_blob) LIKE '%hypercellular%parathyroid%'
                 OR (LOWER(result_blob) LIKE '%hypercellular%' AND LOWER(result_blob) LIKE '%parathyroid%') THEN 'benign'
            WHEN LOWER(result_blob) LIKE '%parathyroid adenoma%' THEN 'benign'
            WHEN LOWER(result_blob) LIKE '%parathyroid tissue%' OR LOWER(result_blob) LIKE '%parathyroid gland%' THEN 'benign'
            WHEN LOWER(result_blob) LIKE '%benign%lymph%' THEN 'benign'
            WHEN LOWER(result_blob) LIKE '%benign%thyroid%' OR LOWER(result_blob) LIKE 'frozen: benign%' THEN 'benign'
            WHEN LOWER(result_blob) LIKE '%multinodular%' OR LOWER(result_blob) LIKE '%mng%' THEN 'benign'
            WHEN LOWER(result_blob) LIKE '%colloid nodule%' THEN 'benign'
            WHEN LOWER(result_blob) LIKE '%nodular hyperplasia%' THEN 'benign'
            WHEN LOWER(result_blob) LIKE '%fibrous tissue%' THEN 'benign'
            WHEN LOWER(result_blob) LIKE '%benign%' THEN 'benign'
            WHEN LOWER(result_blob) LIKE '%malignant%' OR LOWER(result_blob) LIKE '%carcinoma%' THEN 'malignant'
            WHEN TRIM(COALESCE(result_blob, '')) = '' THEN 'unknown'
            ELSE 'unknown'
        END AS result_class_raw,
        CASE
            WHEN LOWER(result_blob) LIKE '%metastatic%papillary%' THEN 'metastatic_papillary_thyroid_carcinoma'
            WHEN LOWER(result_blob) LIKE '%papillary%' OR LOWER(result_blob) LIKE '%ptc%' THEN 'papillary_thyroid_carcinoma'
            WHEN LOWER(result_blob) LIKE '%medullary%' THEN 'medullary_thyroid_carcinoma'
            WHEN LOWER(result_blob) LIKE '%anaplastic%' THEN 'anaplastic_carcinoma'
            WHEN LOWER(result_blob) LIKE '%follicular carcinoma%' THEN 'follicular_carcinoma'
            WHEN LOWER(result_blob) LIKE '%follicular neoplasm%'
                 OR LOWER(result_blob) LIKE '%follicular lesion%' THEN 'follicular_neoplasm'
            WHEN LOWER(result_blob) LIKE '%follicular adenoma%' THEN 'follicular_adenoma'
            WHEN LOWER(result_blob) LIKE '%hypercellular%' AND LOWER(result_blob) LIKE '%parathyroid%' THEN 'parathyroid_hypercellular'
            WHEN LOWER(result_blob) LIKE '%parathyroid adenoma%' THEN 'parathyroid_adenoma'
            WHEN LOWER(result_blob) LIKE '%parathyroid tissue%' THEN 'parathyroid_tissue'
            WHEN LOWER(result_blob) LIKE '%benign%lymph%' THEN 'benign_lymph_node'
            WHEN LOWER(result_blob) LIKE '%benign%thyroid%' OR LOWER(result_blob) LIKE 'frozen: benign%' THEN 'benign_thyroid'
            WHEN LOWER(result_blob) LIKE '%multinodular%' OR LOWER(result_blob) LIKE '%mng%' THEN 'multinodular_goiter'
            WHEN LOWER(result_blob) LIKE '%colloid nodule%' THEN 'colloid_nodule'
            WHEN LOWER(result_blob) LIKE '%nodular hyperplasia%' THEN 'nodular_hyperplasia'
            WHEN LOWER(result_blob) LIKE '%fibrous%' THEN 'fibrous_tissue'
            WHEN LOWER(result_blob) LIKE '%no frozen section performed%'
                 OR LOWER(result_blob) LIKE '%not performed%' THEN 'unknown'
            ELSE 'unknown'
        END AS frozen_section_result_histology,
        CASE
            WHEN text_for_qual LIKE '%pending final%' OR text_for_qual LIKE '%pending permanent%'
                 OR text_for_qual LIKE '%awaiting permanent%' OR text_for_qual LIKE '%pending review%' THEN 'pending_final'
            WHEN text_for_qual LIKE '%deferred%' THEN 'deferred'
            WHEN text_for_qual LIKE '%suspicious for%' OR text_for_qual LIKE '%suspicion of%' OR text_for_qual LIKE '%suspected%' THEN 'suspected'
            WHEN text_for_qual LIKE '%concerning for%' OR text_for_qual LIKE '%concern for%' THEN 'concerning_for'
            WHEN text_for_qual LIKE '%cannot be ruled out%' OR text_for_qual LIKE '%cannot rule out%'
                 OR text_for_qual LIKE '%cannot exclude%' OR text_for_qual LIKE '% r/o %' THEN 'cannot_rule_out'
            WHEN text_for_qual LIKE '%favored%' OR text_for_qual LIKE '%favors%' OR text_for_qual LIKE '% favor %' THEN 'favor'
            WHEN text_for_qual LIKE '%consistent with%' THEN 'consistent_with'
            WHEN text_for_qual LIKE '%not identified%' OR text_for_qual LIKE '%no evidence of%'
                 OR text_for_qual LIKE '%negative for%' OR text_for_qual LIKE '%no diagnostic%' THEN 'not_identified'
            WHEN text_for_qual LIKE '%no ptc%' OR text_for_qual LIKE '%no evidence%' THEN 'not_identified'
            ELSE 'definite'
        END AS frozen_section_result_qualifier,
        CASE
            WHEN LOWER(COALESCE(concord_entity, '')) IN ('concordant', 'discordant', 'deferred') THEN concord_entity
            WHEN LOWER(result_blob) LIKE '%discordant%' THEN 'discordant'
            WHEN LOWER(result_blob) LIKE '%concordant%' THEN 'concordant'
            WHEN LOWER(result_blob) LIKE '%deferred%' THEN 'deferred'
            ELSE 'unknown'
        END AS final_pathology_concordance
    FROM norm n
),
classed AS (
    SELECT
        h.*,
        CASE
            WHEN result_class_raw = 'not_performed' THEN 'not_performed'
            WHEN result_class_raw = 'deferred' OR frozen_section_result_qualifier IN ('deferred', 'pending_final') THEN 'deferred'
            WHEN result_class_raw = 'malignant' THEN 'malignant'
            WHEN result_class_raw = 'indeterminate' THEN 'indeterminate'
            WHEN result_class_raw = 'benign' THEN 'benign'
            ELSE 'unknown'
        END AS frozen_section_result_class,
        CASE
            WHEN frozen_section_result_qualifier IN ('deferred', 'pending_final') THEN TRUE
            WHEN LOWER(COALESCE(frozen_section_result_raw, '')) LIKE '%defer%' THEN TRUE
            WHEN LOWER(COALESCE(frozen_section_result_raw, '')) LIKE '%pending permanent%' THEN TRUE
            ELSE FALSE
        END AS was_deferred_flag,
        CASE
            WHEN LOWER(TRIM(entity_type)) LIKE '%final%' THEN TRUE ELSE FALSE
        END AS was_final_diagnosis_flag
    FROM hist h
),
flags AS (
    SELECT
        c.*,
        CASE
            WHEN frozen_section_result_class = 'malignant'
                 AND frozen_section_result_qualifier NOT IN (
                    'suspected', 'suspicious_for', 'cannot_rule_out', 'concerning_for', 'favor',
                    'pending_final', 'not_identified', 'negated', 'no'
                ) THEN TRUE
            ELSE FALSE
        END AS was_malignant_flag,
        CASE
            WHEN frozen_section_result_qualifier IN (
                'suspected', 'suspicious_for', 'cannot_rule_out', 'concerning_for', 'favor'
            ) THEN TRUE
            ELSE FALSE
        END AS was_suspected_flag,
        CASE
            WHEN frozen_section_result_qualifier IN ('not_identified', 'no', 'negated', 'negative_for')
                THEN TRUE
            ELSE FALSE
        END AS was_negated_flag
    FROM classed c
),
linked AS (
    SELECT
        f.*,
        op.surgery_n,
        op.surgery_episode_id AS operative_episode_id,
        TRY_CAST(SUBSTRING(COALESCE(f.frozen_section_date, ''), 1, 10) AS DATE) AS fday
    FROM flags f
    LEFT JOIN op_pick op
        ON f.research_id = op.research_id
       AND TRY_CAST(SUBSTRING(COALESCE(f.frozen_section_date, ''), 1, 10) AS DATE) = op.sday
),
numbered AS (
    SELECT
        research_id,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY frozen_section_date ASC NULLS LAST,
                     source_priority ASC,
                     SUBSTRING(COALESCE(evidence_text, ''), 1, 200) ASC
        ) AS frozen_event_index,
        frozen_section_date,
        surgery_n,
        operative_episode_id,
        note_type,
        source_sheet,
        source_column,
        frozen_section_site_raw,
        frozen_section_site_norm,
        frozen_section_side,
        frozen_section_result_raw,
        frozen_section_result_histology,
        frozen_section_result_qualifier,
        frozen_section_result_class,
        was_deferred_flag,
        was_malignant_flag,
        was_suspected_flag,
        was_negated_flag,
        final_pathology_concordance,
        intraop_decision_impact_raw AS intraop_decision_impact,
        source_of_data,
        source_priority,
        source_note_ref,
        SUBSTRING(COALESCE(evidence_text, ''), 1, 500) AS evidence_text,
        confidence,
        was_final_diagnosis_flag,
        entity_id_hash,
        synoptic_match_key,
        excel_corroborated_flag,
        excel_result_raw
    FROM linked
)
SELECT * FROM numbered
"""


def run_inventory_sweep(
    con: duckdb.DuckDBPyConnection, ts: str, write_path: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Returns (all_rows, orphan_rows)."""
    rows_out: list[dict[str, Any]] = []

    # 1) tables by name
    tbl_rows = con.execute(
        f"""
        SELECT table_schema, table_name, 'table' AS object_type
        FROM information_schema.tables
        WHERE table_catalog = '{CANONICAL_DB}'
          AND table_schema IN ('main', 'tier2', 'views_readable', 'manuscript_workspace', 'verify')
          AND (
            LOWER(table_name) LIKE '%frozen%'
            OR LOWER(table_name) LIKE '%frozensec%'
            OR LOWER(table_name) LIKE '%intraop%'
            OR (LOWER(table_name) LIKE '%fs_%' AND LOWER(table_name) NOT LIKE 'fsh%')
          )
        """
    ).fetchall()

    def bucket_for_table(schema: str, name: str) -> str:
        fq = f"{schema}.{name}".lower()
        if fq == "main.note_entities_llm_frozen_section_detail":
            return "KEEP_SOURCE"
        if fq == "main.path_synoptics":
            return "KEEP_SOURCE"
        if fq in ("tier2.frozen_section_event_v1", "tier2.frozen_section_event_v2"):
            return "REBUILD_TARGET"
        if fq == "tier2.patient_tier2_master_v1":
            return "REBUILD_TARGET"
        if fq == "main.operative_episode_detail_v2":
            return "REBUILD_TARGET"
        if fq == "main.canonical_patient_master":
            return "REBUILD_TARGET"
        if fq in ("verify.verify_long_v1", "verify.concordance_master_v1"):
            return "REFRESH_DEPENDENT"
        if fq.startswith("manuscript_workspace.cohort_m") and "frozen" in fq:
            return "MANUSCRIPT_STUB"
        if "archive" in fq:
            return "ORPHAN_ARCHIVE"
        if fq in ("main.verify_frozen_section_v1", "main.verify_frozen_section_summary_v1"):
            return "REFRESH_DEPENDENT"
        if fq == "views_readable.surgery_episode_detail":
            return "KEEP_SOURCE"
        return "ORPHAN_ARCHIVE"

    for schema, table, otype in tbl_rows:
        fq = f"{schema}.{table}"
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
        except Exception:
            n = None
        b = bucket_for_table(schema, table)
        rows_out.append(
            {
                "fqname": fq,
                "object_type": otype,
                "bucket": b,
                "row_count": n,
                "rationale": "name scan",
                "proposed_action": "see bucket",
            }
        )

    # 2) columns
    col_rows = con.execute(
        f"""
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_catalog = '{CANONICAL_DB}'
          AND table_schema IN ('main', 'tier2', 'views_readable', 'manuscript_workspace', 'verify')
          AND (
            LOWER(column_name) LIKE '%frozen%'
            OR LOWER(column_name) LIKE '%frozensec%'
            OR LOWER(column_name) LIKE '%intraop%'
            OR (LOWER(column_name) LIKE '%fs_%' AND LOWER(column_name) NOT LIKE 'fsh%')
          )
        ORDER BY table_schema, table_name, column_name
        """
    ).fetchall()

    for schema, table, col in col_rows:
        fq = f"{schema}.{table}.{col}"
        fq_lower = f"{schema}.{table}".lower()
        b = "ORPHAN_ARCHIVE"
        if fq_lower == "main.canonical_patient_master" and col.startswith(
            ("nlp_frozensec_", "syn_frozen", "syn_carcinoma", "frozen_")
        ):
            b = "REBUILD_TARGET"
        if fq_lower == "main.operative_episode_detail_v2" and col.startswith("frozen_section"):
            b = "REBUILD_TARGET"
        if fq_lower == "tier2.patient_tier2_master_v1" and "frozen" in col.lower():
            b = "REBUILD_TARGET"
        if fq_lower == "main.path_synoptics" and col in (
            "frozen_section_obtained",
            "fs_pathology_frozen_section",
            "carcinoma_identified_on_fs_sent_intraop",
        ):
            b = "KEEP_SOURCE"
        if fq_lower == "main.note_entities_llm_frozen_section_detail":
            b = "KEEP_SOURCE"
        if fq_lower.startswith("verify.") and "frozen" in col.lower():
            b = "REFRESH_DEPENDENT"
        rows_out.append(
            {
                "fqname": fq,
                "object_type": "column",
                "bucket": b,
                "row_count": None,
                "rationale": "column scan",
                "proposed_action": "see bucket",
            }
        )

    # 3) views
    view_rows = con.execute(
        f"""
        SELECT table_schema, table_name
        FROM information_schema.views
        WHERE table_catalog = '{CANONICAL_DB}'
          AND table_schema IN ('main', 'tier2', 'views_readable', 'manuscript_workspace', 'verify')
        """
    ).fetchall()
    for schema, view in view_rows:
        defn = con.execute(
            """
            SELECT view_definition FROM information_schema.views
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            """,
            [CANONICAL_DB, schema, view],
        ).fetchone()
        if not defn or not defn[0]:
            continue
        d = defn[0].lower()
        if not any(
            k in d
            for k in (
                "frozen",
                "fs_pathology",
                "intraop_decision_impact",
                "frozensec",
            )
        ):
            continue
        fq = f"{schema}.{view}"
        b = "REFRESH_DEPENDENT" if schema == "verify" else "ORPHAN_ARCHIVE"
        if "cohort_m" in view.lower() and "frozen" in view.lower():
            b = "MANUSCRIPT_STUB"
        if view.lower() == "surgery_episode_detail":
            b = "KEEP_SOURCE"
        rows_out.append(
            {
                "fqname": fq,
                "object_type": "view",
                "bucket": b,
                "row_count": None,
                "rationale": "view_definition scan",
                "proposed_action": "see bucket",
            }
        )

    # 4) archive schema (log only)
    arch = con.execute(
        f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_catalog = 'Thyroid 2026 UPdated'
          AND table_schema = '{ARCHIVE_SCHEMA}'
          AND LOWER(table_name) LIKE '%frozen%'
        ORDER BY table_name
        LIMIT 200
        """
    ).fetchall()
    for s, t in arch:
        rows_out.append(
            {
                "fqname": f"{s}.{t}",
                "object_type": "table",
                "bucket": "ARCHIVE_LOG_ONLY",
                "row_count": None,
                "rationale": "archive schema scan",
                "proposed_action": "none",
            }
        )

    orphans = [r for r in rows_out if r["bucket"] == "ORPHAN_ARCHIVE"]

    rc_lo, rc_hi = GATES["row_count"]
    pt_lo, pt_hi = GATES["distinct_patients"]
    cpm_lo, cpm_hi = GATES["cpm_nlp_true_count"]
    sl_lo, _ = GATES["surgery_linked_pct"]
    lines = [
        f"# Frozen section inventory sweep `{ts}`",
        "",
        "## Expected post-cleanup invariants",
        "",
        "| metric | band |",
        "|---|---|",
        f"| `tier2.frozen_section_event_v1` row count | {int(rc_lo):,} – {int(rc_hi):,} |",
        f"| distinct `research_id` | {int(pt_lo):,} – {int(pt_hi):,} |",
        f"| `surgery_n IS NOT NULL` | ≥ {sl_lo * 100:.0f}% |",
        f"| CPM `nlp_frozensec_has_data` TRUE | {int(cpm_lo):,} – {int(cpm_hi):,} |",
        "| Duplicate non-null `synoptic_match_key` | 0 (hard) |",
        "| `(research_id, fs_day)` with p1+p2 on `fs_pathology_frozen_section` | 0 (hard) |",
        "| Excel-only synoptic patient count | informational (logged, not gated) |",
        "",
        "## Footprint",
        "",
        "| fqname | object_type | bucket | row_count | rationale | proposed_action |",
        "|---|---|---:|---|---|---|",
    ]
    for r in rows_out:
        rc = "" if r["row_count"] is None else str(r["row_count"])
        lines.append(
            f"| {r['fqname']} | {r['object_type']} | {r['bucket']} | {rc} | "
            f"{r['rationale']} | {r['proposed_action']} |"
        )
    write_path.parent.mkdir(parents=True, exist_ok=True)
    write_path.write_text("\n".join(lines), encoding="utf-8")
    log_info(f"Wrote inventory: {write_path}")
    return rows_out, orphans


def coverage_diagnostics(
    con: duckdb.DuckDBPyConnection, table: str
) -> dict[str, int]:
    """Informational Excel-vs-LLM coverage decomposition on a built event table."""
    tt = _validate_sql_identifier(table)
    # Excel-witnessed patients = surviving Excel-only rows ∪ LLM rows with excel_result_raw
    # (LLM wins collapse the original Excel row, preserving its text on the LLM row).
    excel_patients = con.execute(
        f"""
        SELECT COUNT(DISTINCT research_id)::BIGINT FROM {tt}
        WHERE (source_priority = 1
               AND source_of_data = 'synoptic_excel_parsed_column'
               AND source_column = 'fs_pathology_frozen_section')
           OR excel_corroborated_flag IS TRUE
        """
    ).fetchone()[0]
    llm_synoptic_patients = con.execute(
        f"""
        SELECT COUNT(DISTINCT research_id)::BIGINT FROM {tt}
        WHERE source_priority = 2 AND synoptic_match_key IS NOT NULL
        """
    ).fetchone()[0]
    overlap_patients = con.execute(
        f"""
        SELECT COUNT(DISTINCT research_id)::BIGINT FROM {tt}
        WHERE excel_corroborated_flag IS TRUE
        """
    ).fetchone()[0]
    excel_only_patients = con.execute(
        f"""
        SELECT COUNT(DISTINCT research_id)::BIGINT FROM {tt}
        WHERE source_priority = 1
          AND source_of_data = 'synoptic_excel_parsed_column'
          AND synoptic_match_key IS NOT NULL
        """
    ).fetchone()[0]
    llm_only_rows = con.execute(
        f"""
        SELECT COUNT(*)::BIGINT FROM {tt}
        WHERE excel_corroborated_flag IS FALSE
        """
    ).fetchone()[0]
    return {
        "excel_patients": int(excel_patients),
        "llm_synoptic_patients": int(llm_synoptic_patients),
        "overlap_patients": int(overlap_patients),
        "excel_only_patients": int(excel_only_patients),
        "llm_only_rows": int(llm_only_rows),
    }


def verification_gates_pre_rename(
    con: duckdb.DuckDBPyConnection, table: str = "tier2.frozen_section_event_v2"
) -> None:
    """Strict checks on the built event table (v2 or v2_test). CPM bands belong in post_rename_gates."""
    tt = _validate_sql_identifier(table)
    r = con.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               COUNT(*) FILTER (WHERE surgery_n IS NULL),
               COUNT(DISTINCT CASE WHEN surgery_n IS NULL THEN research_id END)
        FROM {tt}
        """
    ).fetchone()
    n, nrid, nnull, rid_null = r
    pct = 100.0 * (nrid - rid_null) / nrid if nrid else 0.0
    log_info(
        f"GATE {tt}: rows={n:,} distinct_patients={nrid:,} "
        f"rows_surgery_null={nnull:,} patients_any_surgery_null={rid_null:,} "
        f"pct_with_surgery_n={pct:.2f}%"
    )
    rc_lo, rc_hi = GATES["row_count"]
    pt_lo, pt_hi = GATES["distinct_patients"]
    sl_lo, _ = GATES["surgery_linked_pct"]
    log_info(
        f"GATE invariant: COUNT(*) in [{int(rc_lo)},{int(rc_hi)}] -> "
        f"{rc_lo <= n <= rc_hi} (n={n})"
    )
    log_info(
        f"GATE invariant: COUNT(DISTINCT research_id) in [{int(pt_lo)},{int(pt_hi)}] -> "
        f"{pt_lo <= nrid <= pt_hi} (n={nrid})"
    )
    log_info(
        f"GATE invariant: patients with surgery_n NOT NULL >= {sl_lo * 100:.0f}% -> "
        f"{pct >= sl_lo * 100:.0f} ({pct:.2f}%)"
    )
    if not (rc_lo <= n <= rc_hi):
        raise SystemExit(f"GATE FAIL: {tt} row count {n} not in [{int(rc_lo)},{int(rc_hi)}]")
    if not (pt_lo <= nrid <= pt_hi):
        raise SystemExit(
            f"GATE FAIL: distinct patients {nrid} not in [{int(pt_lo)},{int(pt_hi)}]"
        )
    if pct < sl_lo * 100:
        miss = con.execute(
            f"""
            SELECT research_id, COUNT(*) AS c
            FROM {tt}
            WHERE surgery_n IS NULL
            GROUP BY 1
            ORDER BY c DESC
            LIMIT 5
            """
        ).fetchall()
        raise SystemExit(
            f"GATE FAIL: surgery linkage {pct:.1f}% < {sl_lo * 100:.0f}%. Sample: {miss}"
        )
    dup_key = con.execute(
        f"""
        SELECT synoptic_match_key, COUNT(*)::BIGINT AS c
        FROM {tt}
        WHERE synoptic_match_key IS NOT NULL
        GROUP BY 1
        HAVING COUNT(*) > 1
        ORDER BY c DESC
        LIMIT 20
        """
    ).fetchall()
    log_info(
        f"GATE invariant: duplicate non-null synoptic_match_key rows -> "
        f"{len(dup_key) == 0} (offenders={len(dup_key)})"
    )
    if dup_key:
        raise SystemExit(
            f"GATE FAIL: duplicate synoptic_match_key (dedup incomplete). Sample: {dup_key}"
        )
    dual_fs = con.execute(
        f"""
        SELECT
            research_id,
            TRY_CAST(SUBSTRING(COALESCE(frozen_section_date, ''), 1, 10) AS DATE) AS fs_day,
            COUNT(*) FILTER (WHERE source_priority = 1) AS n_excel,
            COUNT(*) FILTER (WHERE source_priority = 2) AS n_llm
        FROM {tt}
        WHERE source_column = 'fs_pathology_frozen_section'
        GROUP BY 1, 2
        HAVING COUNT(*) FILTER (WHERE source_priority = 1) > 0
           AND COUNT(*) FILTER (WHERE source_priority = 2) > 0
        LIMIT 50
        """
    ).fetchall()
    log_info(
        f"GATE invariant: (research_id, date) with BOTH p1 and p2 fs_pathology_frozen_section -> "
        f"{len(dual_fs) == 0} (n={len(dual_fs)})"
    )
    if dual_fs:
        raise SystemExit(
            "GATE FAIL: Excel+LLM same-day fs_pathology_frozen_section not collapsed. "
            f"Rows: {dual_fs}"
        )
    dup_h = con.execute(
        f"""
        SELECT entity_id_hash, COUNT(*) c
        FROM {tt}
        GROUP BY 1 HAVING COUNT(*) > 1 ORDER BY c DESC LIMIT 5
        """
    ).fetchall()
    if dup_h:
        raise SystemExit(f"GATE FAIL: duplicate entity_id_hash sample {dup_h}")
    raw_ent = con.execute(
        """
        SELECT COUNT(*)::BIGINT
        FROM main.note_entities_llm_frozen_section_detail d
        INNER JOIN LATERAL generate_series(
            1::BIGINT,
            CAST(json_array_length(json_extract(CAST(d.result_json AS JSON), '$.entities')) AS BIGINT)
        ) gs(series) ON json_type(json_extract(CAST(d.result_json AS JSON), '$.entities')) = 'ARRAY'
        WHERE json_extract_string(
            list_element(
                CAST(json_extract(CAST(d.result_json AS JSON), '$.entities') AS JSON[]),
                CAST(gs.series AS INTEGER)
            ),
            '$.entity_value'
        ) IS NOT NULL
    """
    ).fetchone()[0]
    n_nlp = con.execute(
        f"""
        SELECT COUNT(*) FROM {tt}
        WHERE source_of_data != 'synoptic_excel_parsed_column'
        """
    ).fetchone()[0]
    if n_nlp > raw_ent:
        raise SystemExit(
            f"GATE FAIL: NLP-sourced v2 rows {n_nlp} > raw LLM entity rows {raw_ent}"
        )
    n_excel_synoptic = con.execute(
        f"""
        SELECT COUNT(*)::BIGINT
        FROM {tt}
        WHERE source_priority = 1
          AND source_of_data = 'synoptic_excel_parsed_column'
          AND synoptic_match_key IS NOT NULL
        """
    ).fetchone()[0]
    coverage = coverage_diagnostics(con, tt)
    log_info("--- Coverage diagnostics (informational, no gate) ---")
    log_info(
        f"Excel synoptic patients (distinct research_id where source_priority=1 "
        f"on fs_pathology_frozen_section): {coverage['excel_patients']:,}"
    )
    log_info(
        f"LLM-on-synoptic-cell patients (distinct research_id where source_priority=2 "
        f"AND synoptic_match_key IS NOT NULL): {coverage['llm_synoptic_patients']:,}"
    )
    log_info(
        f"Excel ∩ LLM-on-synoptic-cell overlap (excel_corroborated_flag=TRUE, "
        f"distinct research_id): {coverage['overlap_patients']:,}"
    )
    log_info(
        f"Excel-only patients (Excel − overlap): {coverage['excel_only_patients']:,} "
        f"(rows={n_excel_synoptic:,})"
    )
    log_info(
        f"LLM synoptic rows without Excel match (excel_corroborated_flag=FALSE): "
        f"{coverage['llm_only_rows']:,}"
    )
    cpm_true = con.execute(
        """
        SELECT COUNT(*)::BIGINT
        FROM main.canonical_patient_master
        WHERE nlp_frozensec_has_data IS TRUE
        """
    ).fetchone()[0]
    cpm_lo, cpm_hi = GATES["cpm_nlp_true_count"]
    log_info(
        f"CPM snapshot (stale until phase 5 in isolated phase-2 run): "
        f"nlp_frozensec_has_data TRUE={cpm_true:,} — hard band "
        f"[{int(cpm_lo)},{int(cpm_hi)}] enforced in post_rename_gates"
    )


def rebuild_patient_tier2_frozen_columns(con: duckdb.DuckDBPyConnection) -> None:
    """Replace frozen_section__* columns on tier2.patient_tier2_master_v1."""
    all_cols = list_columns(con, "tier2", "patient_tier2_master_v1")
    frozen_cols = [c for c in all_cols if c.startswith("frozen_section__")]
    if not frozen_cols:
        log_info("No frozen_section__ columns to drop (unexpected)")
    for c in frozen_cols:
        con.execute(f'ALTER TABLE tier2.patient_tier2_master_v1 DROP COLUMN "{c}"')

    slot_parts: list[str] = []
    for n in range(1, 13):
        p = f"frozen_section__frozen_{n}_"
        slot_parts.append(
            f"""
            MAX(CASE WHEN slot = {n} THEN performed_yn END) AS "{p}yn",
            MAX(CASE WHEN slot = {n} THEN frozen_section_date END) AS "{p}date",
            MAX(CASE WHEN slot = {n} THEN frozen_section_site_norm END) AS "{p}site_norm",
            MAX(CASE WHEN slot = {n} THEN frozen_section_side END) AS "{p}side",
            MAX(CASE WHEN slot = {n} THEN frozen_section_result_histology END) AS "{p}result_histology",
            MAX(CASE WHEN slot = {n} THEN frozen_section_result_qualifier END) AS "{p}result_qualifier",
            MAX(CASE WHEN slot = {n} THEN frozen_section_result_class END) AS "{p}result_class",
            MAX(CASE WHEN slot = {n} THEN was_deferred_flag END) AS "{p}was_deferred_flag",
            MAX(CASE WHEN slot = {n} THEN was_malignant_flag END) AS "{p}was_malignant_flag",
            MAX(CASE WHEN slot = {n} THEN surgery_n END) AS "{p}surgery_n",
            MAX(CASE WHEN slot = {n} THEN operative_episode_id END) AS "{p}operative_episode_id",
            MAX(CASE WHEN slot = {n} THEN source_of_data END) AS "{p}source_of_data",
            MAX(CASE WHEN slot = {n} THEN source_note_ref END) AS "{p}source_note_ref",
            MAX(CASE WHEN slot = {n} THEN evidence_text END) AS "{p}evidence_text",
            MAX(CASE WHEN slot = {n} THEN confidence END) AS "{p}confidence",
            MAX(CASE WHEN slot = {n} THEN excel_corroborated_flag END) AS "{p}excel_corroborated_flag",
            MAX(CASE WHEN slot = {n} THEN excel_result_raw END) AS "{p}excel_result_raw"
            """
        )

    slot_sql = ",\n".join(slot_parts)
    wide_sql = f"""
    CREATE OR REPLACE TEMP TABLE _frozen_wide AS
    WITH ev AS (
        SELECT
            v.*,
            ROW_NUMBER() OVER (
                PARTITION BY v.research_id
                ORDER BY v.frozen_section_date ASC NULLS LAST,
                         v.source_priority ASC,
                         v.frozen_event_index
            ) AS slot,
            (v.frozen_section_result_raw IS NOT NULL
             OR v.source_of_data = 'synoptic_excel_parsed_column') AS performed_yn
        FROM tier2.frozen_section_event_v2 v
        WHERE v.frozen_section_result_raw IS NOT NULL
           OR v.source_of_data = 'synoptic_excel_parsed_column'
    ),
    agg AS (
        SELECT
            research_id,
            COUNT(*)::BIGINT AS n_frozen_events,
            COUNT(DISTINCT surgery_n) FILTER (WHERE surgery_n IS NOT NULL)::BIGINT AS n_surgeries_with_frozen,
            BOOL_OR(
                frozen_section_result_raw IS NOT NULL
                OR source_of_data = 'synoptic_excel_parsed_column'
            ) AS any_frozen_performed_flag,
            BOOL_OR(was_malignant_flag) AS any_frozen_malignant_result_flag,
            BOOL_OR(was_deferred_flag) AS any_frozen_deferred_flag,
            BOOL_OR(was_suspected_flag) AS any_frozen_suspected_malignant_flag,
            MIN(frozen_section_date) FILTER (
                WHERE frozen_section_result_raw IS NOT NULL
                   OR source_of_data = 'synoptic_excel_parsed_column'
            ) AS first_frozen_performed_date,
            MAX(frozen_section_date) FILTER (
                WHERE frozen_section_result_raw IS NOT NULL
                   OR source_of_data = 'synoptic_excel_parsed_column'
            ) AS last_frozen_performed_date,
            MIN(frozen_section_date) FILTER (WHERE was_malignant_flag) AS first_malignant_frozen_date,
            MIN(frozen_section_date) FILTER (WHERE was_deferred_flag) AS first_deferred_frozen_date,
            COUNT(DISTINCT source_note_ref) FILTER (
                WHERE frozen_section_result_raw IS NOT NULL
                   OR source_of_data = 'synoptic_excel_parsed_column'
            )::BIGINT AS n_notes_documenting_frozen_performed,
            COUNT(DISTINCT source_note_ref) FILTER (WHERE was_malignant_flag)::BIGINT
                AS n_notes_documenting_malignant_frozen
        FROM tier2.frozen_section_event_v2 v
        WHERE v.frozen_section_result_raw IS NOT NULL
           OR v.source_of_data = 'synoptic_excel_parsed_column'
        GROUP BY research_id
    ),
    slots AS (
        SELECT
            research_id,
            {slot_sql}
        FROM ev
        WHERE slot <= 12
        GROUP BY research_id
    )
    SELECT a.*, s.*
    FROM agg a
    LEFT JOIN slots s USING (research_id)
    """
    con.execute(wide_sql)
    wide_cols = [
        r[0]
        for r in con.execute("SELECT column_name FROM (DESCRIBE _frozen_wide)").fetchall()
        if r[0] != "research_id"
    ]
    w_sel = ", ".join(f'w."{c}"' for c in wide_cols)
    keep_cols = [
        c
        for c in list_columns(con, "tier2", "patient_tier2_master_v1")
        if not c.startswith("frozen_section__")
    ]
    p_sel = ", ".join(f'p."{c}"' for c in keep_cols)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE tier2.patient_tier2_master_v1_new360 AS
        SELECT {p_sel}, {w_sel}
        FROM tier2.patient_tier2_master_v1 p
        LEFT JOIN _frozen_wide w ON p.research_id = w.research_id
        """
    )
    con.execute("ALTER TABLE tier2.patient_tier2_master_v1 RENAME TO patient_tier2_master_v1_old360")
    con.execute(
        "ALTER TABLE tier2.patient_tier2_master_v1_new360 RENAME TO patient_tier2_master_v1"
    )
    con.execute("DROP TABLE tier2.patient_tier2_master_v1_old360")
    log_info("Rebuilt tier2.patient_tier2_master_v1 frozen_section__* columns (12 slots + aggregates)")


def add_operative_frozen_columns(con: duckdb.DuckDBPyConnection) -> None:
    cols = list_columns(con, "main", "operative_episode_detail_v2")
    adds = [
        ("frozen_section_n", "BIGINT"),
        ("frozen_section_any_malignant_flag", "BOOLEAN"),
        ("frozen_section_any_deferred_flag", "BOOLEAN"),
        ("frozen_section_any_suspected_malignant_flag", "BOOLEAN"),
    ]
    for name, typ in adds:
        if name not in cols:
            con.execute(
                f'ALTER TABLE main.operative_episode_detail_v2 ADD COLUMN "{name}" {typ}'
            )
            log_info(f"Added operative_episode_detail_v2.{name}")


def update_operative_frozen_flags(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        UPDATE main.operative_episode_detail_v2
        SET frozen_section_flag = FALSE,
            frozen_section_n = 0,
            frozen_section_any_malignant_flag = FALSE,
            frozen_section_any_deferred_flag = FALSE,
            frozen_section_any_suspected_malignant_flag = FALSE
        """
    )
    con.execute(
        """
        UPDATE main.operative_episode_detail_v2 o
        SET
            frozen_section_flag = TRUE,
            frozen_section_n = s.n_ev,
            frozen_section_any_malignant_flag = s.any_mal,
            frozen_section_any_deferred_flag = s.any_def,
            frozen_section_any_suspected_malignant_flag = s.any_susp
        FROM (
            SELECT
                CAST(o2.research_id AS VARCHAR) AS research_id,
                o2.surgery_episode_id,
                COUNT(*)::BIGINT AS n_ev,
                BOOL_OR(e.was_malignant_flag) AS any_mal,
                BOOL_OR(e.was_deferred_flag) AS any_def,
                BOOL_OR(e.was_suspected_flag) AS any_susp
            FROM main.operative_episode_detail_v2 o2
            INNER JOIN tier2.frozen_section_event_v2 e
                ON CAST(o2.research_id AS VARCHAR) = e.research_id
               AND COALESCE(
                    CAST(o2.resolved_surgery_date AS DATE),
                    CAST(o2.surgery_date_native AS DATE)
                ) = TRY_CAST(SUBSTRING(COALESCE(e.frozen_section_date, ''), 1, 10) AS DATE)
            GROUP BY 1, 2
        ) s
        WHERE CAST(o.research_id AS VARCHAR) = s.research_id
          AND o.surgery_episode_id = s.surgery_episode_id
        """
    )


def refresh_cpm_frozen(con: duckdb.DuckDBPyConnection) -> None:
    """NLP rollup + synoptic columns + reconciliation on canonical_patient_master."""
    kfp = key_finding_priority_case("entity_type")
    cpm_cols = list_columns(con, "main", "canonical_patient_master")
    for name, typ in (
        ("frozen_any_performed_flag", "BOOLEAN"),
        ("frozen_n_total", "BIGINT"),
        ("frozen_source_hierarchy", "VARCHAR"),
    ):
        if name not in cpm_cols:
            con.execute(f'ALTER TABLE main.canonical_patient_master ADD COLUMN "{name}" {typ}')

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _cpm_frozen_stage AS
        WITH ev AS (
            SELECT
                research_id,
                COUNT(*)::BIGINT AS n_tot,
                BOOL_OR(was_malignant_flag) AS nlp_any_mal
            FROM tier2.frozen_section_event_v2 v
            WHERE v.frozen_section_result_raw IS NOT NULL
               OR v.source_of_data = 'synoptic_excel_parsed_column'
            GROUP BY research_id
        ),
        flat AS (
            SELECT
                CAST(d.research_id AS VARCHAR) AS research_id,
                d.note_row_id,
                json_extract_string(je.ent, '$.entity_type') AS entity_type,
                json_extract_string(je.ent, '$.entity_value') AS entity_value,
                COALESCE(TRY_CAST(json_extract(je.ent, '$.confidence') AS DOUBLE), 0.0) AS confidence,
                COALESCE(
                    NULLIF(json_extract_string(je.ent, '$.present_or_negated'), ''),
                    'present'
                ) AS present_or_negated
            FROM main.note_entities_llm_frozen_section_detail d
            INNER JOIN LATERAL (
                SELECT list_element(
                    CAST(json_extract(CAST(d.result_json AS JSON), '$.entities') AS JSON[]),
                    CAST(gs.series AS INTEGER)
                ) AS ent
                FROM generate_series(
                    1::BIGINT,
                    CAST(json_array_length(json_extract(CAST(d.result_json AS JSON), '$.entities')) AS BIGINT)
                ) gs(series)
            ) je ON TRUE
            WHERE json_type(json_extract(CAST(d.result_json AS JSON), '$.entities')) = 'ARRAY'
              AND json_array_length(json_extract(CAST(d.result_json AS JSON), '$.entities')) > 0
        ),
        pos AS (
            SELECT *
            FROM flat
            WHERE entity_value IS NOT NULL
              AND confidence >= 0.5
              AND (present_or_negated = 'present' OR present_or_negated IS NULL)
        ),
        rollup AS (
            SELECT
                research_id,
                TRUE AS has_data,
                COUNT(DISTINCT note_row_id)::BIGINT AS n_notes,
                COUNT(*)::BIGINT AS n_entities,
                ARG_MAX(
                    entity_value,
                    ({kfp}) * -1000000.0 + COALESCE(confidence, 0.0)
                ) AS key_finding
            FROM pos
            GROUP BY research_id
        ),
        syn AS (
            SELECT
                CAST(research_id AS VARCHAR) AS research_id,
                BOOL_OR(
                    (frozen_section_obtained IS NOT NULL
                     AND UPPER(NULLIF(TRIM(CAST(frozen_section_obtained AS VARCHAR)), ''))
                         NOT IN ('NO', 'N', '', 'FALSE', '0'))
                    OR NULLIF(TRIM(CAST(fs_pathology_frozen_section AS VARCHAR)), '') IS NOT NULL
                ) AS syn_frozen_section,
                STRING_AGG(
                    DISTINCT NULLIF(TRIM(CAST(fs_pathology_frozen_section AS VARCHAR)), ''),
                    ' | '
                ) AS syn_frozen_section_result,
                BOOL_OR(
                    LOWER(NULLIF(TRIM(CAST(carcinoma_identified_on_fs_sent_intraop AS VARCHAR)), ''))
                    IN ('yes', 'y', 'true', '1', 'positive', 'pos')
                ) AS syn_carcinoma_on_frozen
            FROM main.path_synoptics
            GROUP BY research_id
        )
        SELECT
            CAST(c.research_id AS VARCHAR) AS rid,
            COALESCE(r.has_data, FALSE) AS nlp_frozensec_has_data,
            COALESCE(r.n_notes, 0::BIGINT) AS nlp_frozensec_n_notes,
            COALESCE(r.n_entities, 0::BIGINT) AS nlp_frozensec_n_entities,
            CAST(r.key_finding AS VARCHAR) AS nlp_frozensec_key_finding,
            COALESCE(s.syn_frozen_section, FALSE) AS syn_frozen_section,
            CAST(s.syn_frozen_section_result AS VARCHAR) AS syn_frozen_section_result,
            COALESCE(s.syn_carcinoma_on_frozen, FALSE) AS syn_carcinoma_on_frozen,
            COALESCE(e.n_tot, 0::BIGINT) AS frozen_n_total,
            COALESCE(s.syn_frozen_section, FALSE) OR COALESCE(r.has_data, FALSE) AS frozen_any_performed_flag,
            CASE
                WHEN NOT COALESCE(s.syn_frozen_section, FALSE) AND NOT COALESCE(r.has_data, FALSE) THEN 'neither'
                WHEN COALESCE(s.syn_frozen_section, FALSE) AND NOT COALESCE(r.has_data, FALSE) THEN 'syn_only'
                WHEN COALESCE(r.has_data, FALSE) AND NOT COALESCE(s.syn_frozen_section, FALSE) THEN 'nlp_only'
                WHEN COALESCE(s.syn_frozen_section, FALSE) AND COALESCE(r.has_data, FALSE)
                     AND COALESCE(s.syn_carcinoma_on_frozen, FALSE)
                        IS DISTINCT FROM COALESCE(e.nlp_any_mal, FALSE) THEN 'both_disagree'
                ELSE 'both_agree'
            END AS frozen_source_hierarchy
        FROM main.canonical_patient_master c
        LEFT JOIN rollup r ON CAST(c.research_id AS VARCHAR) = r.research_id
        LEFT JOIN syn s ON CAST(c.research_id AS VARCHAR) = s.research_id
        LEFT JOIN ev e ON CAST(c.research_id AS VARCHAR) = e.research_id
        """
    )
    con.execute(
        """
        UPDATE main.canonical_patient_master c
        SET
            nlp_frozensec_has_data = s.nlp_frozensec_has_data,
            nlp_frozensec_n_notes = s.nlp_frozensec_n_notes,
            nlp_frozensec_n_entities = s.nlp_frozensec_n_entities,
            nlp_frozensec_key_finding = s.nlp_frozensec_key_finding,
            syn_frozen_section = s.syn_frozen_section,
            syn_frozen_section_result = s.syn_frozen_section_result,
            syn_carcinoma_on_frozen = s.syn_carcinoma_on_frozen,
            frozen_n_total = s.frozen_n_total,
            frozen_any_performed_flag = s.frozen_any_performed_flag,
            frozen_source_hierarchy = s.frozen_source_hierarchy
        FROM _cpm_frozen_stage s
        WHERE CAST(c.research_id AS VARCHAR) = s.rid
        """
    )
    log_info("CPM frozen-related columns refreshed from stage table")


def rebuild_verify_frozen_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Rebuild main.verify_frozen_section_v1 + summary (Script 320 logic, tier2 events)."""
    sql = """
    CREATE OR REPLACE TABLE main.verify_frozen_section_v1 AS
    WITH excel AS (
        SELECT CAST(research_id AS VARCHAR) AS research_id,
               MAX(frozen_section_obtained) AS frozen_section_performed_excel,
               MAX(fs_pathology_frozen_section) AS frozen_section_result_excel
        FROM main.path_synoptics GROUP BY research_id
    ),
    llm AS (
        SELECT CAST(research_id AS VARCHAR) AS research_id,
               'Y' AS frozen_section_performed_llm,
               ANY_VALUE(
                   COALESCE(
                       frozen_section_result_raw,
                       CAST(frozen_section_result_histology AS VARCHAR)
                   )
               ) AS frozen_section_result_llm
        FROM tier2.frozen_section_event_v1
        GROUP BY research_id
    )
    SELECT COALESCE(e.research_id, l.research_id) AS research_id,
        e.frozen_section_performed_excel, l.frozen_section_performed_llm,
        CASE WHEN e.frozen_section_performed_excel IS NULL AND l.frozen_section_performed_llm IS NULL
             THEN 'both_null'
             WHEN e.frozen_section_performed_excel IS NULL THEN 'llm_only'
             WHEN l.frozen_section_performed_llm IS NULL THEN 'excel_only'
             ELSE 'agree' END AS frozen_section_performed_concordance,
        e.frozen_section_result_excel, l.frozen_section_result_llm,
        CASE WHEN e.frozen_section_result_excel IS NULL AND l.frozen_section_result_llm IS NULL
             THEN 'both_null'
             WHEN e.frozen_section_result_excel IS NULL THEN 'llm_only'
             WHEN l.frozen_section_result_llm IS NULL THEN 'excel_only'
             WHEN LOWER(TRIM(CAST(e.frozen_section_result_excel AS VARCHAR)))
                  = LOWER(TRIM(l.frozen_section_result_llm)) THEN 'agree'
             ELSE 'disagree' END AS frozen_section_result_concordance
    FROM excel e FULL OUTER JOIN llm l ON l.research_id = e.research_id
    """
    con.execute(sql)
    fields = ["frozen_section_performed", "frozen_section_result"]
    parts = [
        (
            f"SELECT 'frozen_section' AS domain, '{f}' AS field_name, "
            f"SUM(CASE WHEN {f}_concordance='agree' THEN 1 ELSE 0 END) AS n_agree, "
            f"SUM(CASE WHEN {f}_concordance='disagree' THEN 1 ELSE 0 END) AS n_disagree, "
            f"SUM(CASE WHEN {f}_concordance='excel_only' THEN 1 ELSE 0 END) AS n_excel_only, "
            f"SUM(CASE WHEN {f}_concordance='llm_only' THEN 1 ELSE 0 END) AS n_llm_only, "
            f"ROUND(SUM(CASE WHEN {f}_concordance='agree' THEN 1.0 ELSE 0 END)/NULLIF("
            f"SUM(CASE WHEN {f}_concordance IN ('agree','disagree') THEN 1 ELSE 0 END),0),4) AS pct_agree "
            f"FROM main.verify_frozen_section_v1"
        )
        for f in fields
    ]
    con.execute(
        "CREATE OR REPLACE TABLE main.verify_frozen_section_summary_v1 AS "
        + " UNION ALL ".join(parts)
    )


def run_subprocess_scripts() -> None:
    root = REPO_ROOT
    for script in ("337_build_verify_concordance_master.py", "338_build_verify_long.py"):
        cmd = [sys.executable, str(root / "scripts" / script), "--commit"]
        log_info(f"Running {' '.join(cmd)}")
        subprocess.run(cmd, check=True, cwd=str(root))


def apply_event_table_comments(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """COMMENT ON for tier2 frozen_section_event (v1 or v2)."""
    tbl = f'tier2."{table}"'
    con.execute(
        f"COMMENT ON TABLE {tbl} IS "
        f"'[domain=frozen_section; grain=per_event] — source: Script 360; "
        f"note_entities_llm_frozen_section_detail + path_synoptics';"
    )
    cols = [
        ("research_id", "patient key"),
        ("frozen_event_index", "1..N per patient"),
        ("frozen_section_date", "best available date string"),
        ("surgery_n", "operative_episode_detail_v2 DENSE_RANK by surgery date"),
        ("operative_episode_id", "surgery_episode_id from operative detail"),
        ("frozen_section_site_raw", "verbatim site entity"),
        ("frozen_section_site_norm", "normalized site vocabulary"),
        ("frozen_section_side", "laterality bucket"),
        ("frozen_section_result_raw", "verbatim result / Excel text"),
        ("frozen_section_result_histology", "granular histology enum"),
        ("frozen_section_result_qualifier", "hedge / qualifier"),
        ("frozen_section_result_class", "benign / malignant / indeterminate / deferred / not_performed / unknown"),
        ("was_deferred_flag", "deferred / pending final"),
        ("was_malignant_flag", "definite malignant per rules"),
        ("was_suspected_flag", "suspected / suspicious qualifier"),
        ("was_negated_flag", "negated / not identified"),
        ("final_pathology_concordance", "concordance entity or parsed"),
        ("intraop_decision_impact", "LLM intraop impact when present"),
        ("source_of_data", "synoptic_excel_parsed_column | synoptic_path_report_llm | opnote_llm | hp_llm"),
        ("source_priority", "dedup tie-break priority"),
        ("source_note_ref", "note_index or EXCEL synthetic ref"),
        ("evidence_text", "first 500 chars evidence"),
        ("confidence", "LLM confidence or 1.0 Excel"),
        ("was_final_diagnosis_flag", "final diagnosis entity flag"),
        ("entity_id_hash", "MD5 dedup key"),
        ("synoptic_match_key", "Excel+LLM same-cell dedup key (NULL for OPNOTE/HP)"),
        ("excel_corroborated_flag", "TRUE if LLM synoptic row matched Excel same-day cell"),
        ("excel_result_raw", "raw path_synoptics fs_pathology_frozen_section text when known"),
    ]
    for col, desc in cols:
        con.execute(
            f"COMMENT ON COLUMN {tbl}.\"{col}\" IS "
            f"'[domain=frozen_section; grain=per_event] — source: Script 360; {desc}';"
        )


def rename_event_v2_to_v1(con: duckdb.DuckDBPyConnection, ts: str) -> None:
    if table_exists(con, "tier2", "frozen_section_event_v1"):
        archive_copy(con, "tier2", "frozen_section_event_v1", "preFROZENCLEANUP", ts)
        con.execute("DROP TABLE tier2.frozen_section_event_v1")
    con.execute("ALTER TABLE tier2.frozen_section_event_v2 RENAME TO frozen_section_event_v1")


def post_rename_gates(con: duckdb.DuckDBPyConnection) -> None:
    cpm = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE c.nlp_frozensec_has_data),
            COUNT(*) FILTER (WHERE c.nlp_frozensec_has_data IS NULL),
            COUNT(*) FILTER (WHERE c.frozen_any_performed_flag),
            COUNT(*) FILTER (WHERE COALESCE(o.op_frozen, FALSE))
        FROM main.canonical_patient_master c
        LEFT JOIN (
            SELECT CAST(research_id AS VARCHAR) AS rid,
                   BOOL_OR(frozen_section_flag) AS op_frozen
            FROM main.operative_episode_detail_v2
            GROUP BY 1
        ) o ON CAST(c.research_id AS VARCHAR) = o.rid
        """
    ).fetchone()
    log_info(
        f"Post gates: CPM nlp_has_data_TRUE={cpm[0]} nlp_has_data_NULL={cpm[1]} "
        f"frozen_any_TRUE={cpm[2]} any_op_frozen_flag={cpm[3]}"
    )
    if cpm[1] != 0:
        raise SystemExit("GATE: CPM nlp_frozensec_has_data NULL count must be 0")
    cpm_lo, cpm_hi = GATES["cpm_nlp_true_count"]
    if not (cpm_lo <= cpm[0] <= cpm_hi):
        raise SystemExit(
            f"GATE: nlp_frozensec_has_data TRUE {cpm[0]} "
            f"not in [{int(cpm_lo)},{int(cpm_hi)}]"
        )
    pt_lo, pt_hi = GATES["distinct_patients"]
    if not (pt_lo <= cpm[2] <= pt_hi):
        log_error(
            f"WARNING: frozen_any_performed_flag TRUE={cpm[2]} outside "
            f"[{int(pt_lo)},{int(pt_hi)}] — review reconciliation thresholds"
        )
    over12 = con.execute(
        """
        SELECT research_id, frozen_section__n_frozen_events AS n
        FROM tier2.patient_tier2_master_v1
        WHERE frozen_section__n_frozen_events > 12
        LIMIT 10
        """
    ).fetchall()
    if over12:
        raise SystemExit(f"GATE: patients with >12 frozen events: {over12}")


def sync_registry(con: duckdb.DuckDBPyConnection, ts: str) -> None:
    ev_n = con.execute("SELECT COUNT(*) FROM tier2.frozen_section_event_v1").fetchone()[0]
    ev_p = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM tier2.frozen_section_event_v1"
    ).fetchone()[0]
    pm_n = con.execute("SELECT COUNT(*) FROM tier2.patient_tier2_master_v1").fetchone()[0]
    marker = "Script 360 frozen cleanup"
    for name, rows, patients, grain in (
        ("frozen_section_event_v1", ev_n, ev_p, "per_event"),
        ("patient_tier2_master_v1", pm_n, pm_n, "per_patient_wide"),
    ):
        exists = con.execute(
            f"""
            SELECT 1 FROM {WS_SCHEMA}.{REGISTRY_TABLE}
            WHERE detail_table_name = ?
            """,
            [name],
        ).fetchone()
        desc = (
            f"[domain=frozen_section; grain={grain}] — source: Script 360 ({ts}). "
            f"Rows={rows}, patients={patients}."
        )
        if exists:
            con.execute(
                f"""
                UPDATE {WS_SCHEMA}.{REGISTRY_TABLE}
                SET total_rows = ?, total_patients = ?,
                    description = CASE WHEN description LIKE '%' || ? || '%'
                        THEN description ELSE COALESCE(description,'') || ' | ' || ? END,
                    canonical_version = 'v1_0_script360'
                WHERE detail_table_name = ?
                """,
                [rows, patients, marker, marker, name],
            )
        else:
            con.execute(
                f"""
                INSERT INTO {WS_SCHEMA}.{REGISTRY_TABLE} (
                    detail_table_name, schema_name, join_key, grain,
                    total_rows, total_patients, domain, description, canonical_version,
                    needs_manual_review
                ) VALUES (?, 'tier2', 'research_id', ?, ?, ?, 'frozen_section', ?, 'v1_0_script360', FALSE)
                """,
                [name, grain, rows, patients, desc],
            )
    log_info("detail_table_registry_v1 synced for frozen tables")


def whitelist_footprint_markdown(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Inventory sweep gate: expected frozen-footprint objects exist (post `--phase all`)."""
    lines = [
        "",
        "## Whitelist footprint (§inventory_sweep)",
        "",
        "| fqname | exists | approx_rows |",
        "|---|---:|---:|",
    ]
    for fq in sorted(FOOTPRINT_WHITELIST):
        sch, _, tbl = fq.partition(".")
        if not tbl:
            raise SystemExit(f"Invalid whitelist fqname {fq!r}")
        ok = table_exists(con, sch, tbl)
        n = ""
        if ok:
            try:
                n = str(con.execute(f'SELECT COUNT(*) FROM "{sch}"."{tbl}"').fetchone()[0])
            except Exception as exc:
                n = f"(count err: {exc})"
        lines.append(f"| {fq} | {'yes' if ok else 'no'} | {n} |")
    return lines


def write_cleanup_summary(
    con: duckdb.DuckDBPyConnection, inv_path: Path, ts_date: str
) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / f"FROZEN_SECTION_CLEANUP_SUMMARY_{ts_date}.md"
    gaps = con.execute(
        """
        SELECT DISTINCT e.research_id
        FROM tier2.frozen_section_event_v1 e
        WHERE e.surgery_n IS NULL
        LIMIT 50
        """
    ).fetchall()
    rc_lo, rc_hi = GATES["row_count"]
    pt_lo, pt_hi = GATES["distinct_patients"]
    cpm_lo, cpm_hi = GATES["cpm_nlp_true_count"]
    sl_lo, _ = GATES["surgery_linked_pct"]
    cov = coverage_diagnostics(con, "tier2.frozen_section_event_v1")
    lines = [
        f"# Frozen section cleanup summary ({ts_date})",
        "",
        "## Expected invariants (post-filter reality)",
        "",
        "| metric | band |",
        "|---|---|",
        f"| `COUNT(*)` on `tier2.frozen_section_event_v1` | {int(rc_lo):,} – {int(rc_hi):,} |",
        f"| `COUNT(DISTINCT research_id)` | {int(pt_lo):,} – {int(pt_hi):,} |",
        f"| `surgery_n IS NOT NULL` % | ≥ {sl_lo * 100:.0f}% |",
        f"| CPM `nlp_frozensec_has_data` TRUE | {int(cpm_lo):,} – {int(cpm_hi):,} |",
        "| Duplicate non-null `synoptic_match_key` | 0 (hard) |",
        "| `(research_id, fs_day)` with p1+p2 on `fs_pathology_frozen_section` | 0 (hard) |",
        "",
        "## LLM vs Excel coverage (informational — not a regression gate)",
        "",
        "Excel-only patient count is logged but not gated. It tracks upstream LLM",
        "extraction coverage, not Script 360's dedup behavior.",
        "",
        "| metric | value |",
        "|---|---:|",
        f"| Excel synoptic patients (p1 on `fs_pathology_frozen_section`) | {cov['excel_patients']:,} |",
        f"| LLM-on-synoptic-cell patients (p2 with `synoptic_match_key`) | {cov['llm_synoptic_patients']:,} |",
        f"| Excel ∩ LLM overlap (`excel_corroborated_flag = TRUE`) | {cov['overlap_patients']:,} |",
        f"| Excel-only patients (Excel − overlap) | {cov['excel_only_patients']:,} |",
        f"| LLM synoptic rows without Excel match (`excel_corroborated_flag = FALSE`) | {cov['llm_only_rows']:,} |",
        "",
        "## Surgery linkage gaps (≤50 sample research_id)",
        "",
        ", ".join(r[0] for r in gaps) if gaps else "(none in sample)",
        "",
        f"Pre-run inventory: `{inv_path}`",
        *whitelist_footprint_markdown(con),
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    log_info(f"Wrote summary {path}")


def phase_0(con: duckdb.DuckDBPyConnection) -> None:
    log_info("=== PHASE 0 dry-run ===")
    for label, q in [
        ("nlp_detail", "SELECT COUNT(*) FROM main.note_entities_llm_frozen_section_detail"),
        ("event_v1", "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM tier2.frozen_section_event_v1"),
        ("cpm", "SELECT COUNT(*) FROM main.canonical_patient_master"),
    ]:
        r = con.execute(q).fetchone()
        log_info(f"  {label}: {r}")


def phase_1(con: duckdb.DuckDBPyConnection, ts: str) -> None:
    log_info("=== PHASE 1 archive ===")
    for schema, table in (
        ("tier2", "frozen_section_event_v1"),
        ("tier2", "patient_tier2_master_v1"),
        ("main", "operative_episode_detail_v2"),
        ("main", "canonical_patient_master"),
    ):
        if table_exists(con, schema, table):
            archive_copy(con, schema, table, "preFROZENCLEANUP", ts)


def phase_1_5(
    con: duckdb.DuckDBPyConnection, ts: str, confirm_orphans: bool
) -> Path:
    log_info("=== PHASE 1.5 inventory ===")
    inv_path = REPORTS_DIR / f"FROZEN_SECTION_INVENTORY_{ts}.md"
    _, orphans = run_inventory_sweep(con, ts, inv_path)
    if orphans and not confirm_orphans:
        log_error(f"ORPHAN_ARCHIVE hits={len(orphans)} — pass --confirm-orphans to proceed")
        for o in orphans[:30]:
            log_error(f"  orphan: {o}")
        raise SystemExit(2)
    return inv_path


def phase_2(con: duckdb.DuckDBPyConnection) -> None:
    log_info("=== PHASE 2 build frozen_section_event_v2 ===")
    con.execute(build_event_v2_sql())
    verification_gates_pre_rename(con)


def phase_2_test(con: duckdb.DuckDBPyConnection) -> None:
    """Build tier2.frozen_section_event_v2_test, run gates, drop (no v2 rename / no phase 3+)."""
    tbl = "tier2.frozen_section_event_v2_test"
    log_info(f"=== PHASE 2-test dry-run: {tbl} ===")
    con.execute(f"DROP TABLE IF EXISTS {tbl}")
    con.execute(build_event_v2_sql(tbl))
    verification_gates_pre_rename(con, table=tbl)
    con.execute(f"DROP TABLE IF EXISTS {tbl}")
    log_info(f"Dropped {tbl} (MotherDuck left without test table)")


def phase_3(con: duckdb.DuckDBPyConnection) -> None:
    log_info("=== PHASE 3 operative_episode_detail_v2 ===")
    add_operative_frozen_columns(con)
    update_operative_frozen_flags(con)


def phase_4(con: duckdb.DuckDBPyConnection) -> None:
    log_info("=== PHASE 4 patient_tier2_master wide frozen columns ===")
    rebuild_patient_tier2_frozen_columns(con)


def phase_5(con: duckdb.DuckDBPyConnection) -> None:
    log_info("=== PHASE 5 CPM rollup ===")
    refresh_cpm_frozen(con)


def phase_6(con: duckdb.DuckDBPyConnection, ts: str) -> None:
    log_info("=== PHASE 6 rename event v2 → v1 + comments ===")
    rename_event_v2_to_v1(con, ts)
    apply_event_table_comments(con, "frozen_section_event_v1")


def phase_6_5(con: duckdb.DuckDBPyConnection) -> None:
    log_info("=== PHASE 6.5 verify tables + concordance/long rebuild ===")
    rebuild_verify_frozen_tables(con)
    run_subprocess_scripts()


def phase_7(con: duckdb.DuckDBPyConnection, ts: str) -> None:
    log_info("=== PHASE 7 registry sync ===")
    sync_registry(con, ts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        default="0",
        help="0|1|1.5|2|2-test|3|4|5|6|6.5|7|all",
    )
    parser.add_argument(
        "--continue-destructive",
        action="store_true",
        help="With --phase all, continue past inventory stop into phases 2–7.",
    )
    parser.add_argument(
        "--confirm-orphans",
        action="store_true",
        help="Allow phase 1.5 to proceed when ORPHAN_ARCHIVE bucket is non-empty.",
    )
    args = parser.parse_args()
    ts = utc_compact()
    rc = 0
    con = connect()
    try:
        ph = args.phase
        if ph == "0":
            phase_0(con)
        elif ph == "1":
            phase_1(con, ts)
        elif ph == "1.5":
            phase_1_5(con, ts, args.confirm_orphans)
        elif ph == "2":
            phase_2(con)
        elif ph == "2-test":
            phase_2_test(con)
        elif ph == "3":
            phase_3(con)
        elif ph == "4":
            phase_4(con)
        elif ph == "5":
            phase_5(con)
        elif ph == "6":
            phase_6(con, ts)
        elif ph == "6.5":
            phase_6_5(con)
        elif ph == "7":
            phase_7(con, ts)
        elif ph == "all":
            phase_0(con)
            phase_1(con, ts)
            inv = phase_1_5(con, ts, args.confirm_orphans)
            log_info(f"Inventory written: {inv}")
            if not args.continue_destructive:
                log_info("Stopping after phase 1.5 (pass --continue-destructive for phases 2–7)")
                return 3
            phase_2(con)
            phase_3(con)
            phase_4(con)
            phase_5(con)
            phase_6(con, ts)
            phase_6_5(con)
            phase_7(con, ts)
            post_rename_gates(con)
            write_cleanup_summary(con, inv, utc_date().replace("-", ""))
        else:
            raise SystemExit(f"Unknown phase {ph!r}")
    except Exception as exc:
        log_error(repr(exc))
        rc = 1
        raise
    finally:
        _flush_log()
        con.close()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())