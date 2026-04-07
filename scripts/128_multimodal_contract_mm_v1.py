#!/usr/bin/env python3
"""
128_multimodal_contract_mm_v1.py — Verified multimodal release layer (contract v1)

Creates schema mm_contract_dev with star-schema style tables mapping legacy episode
and linkage objects to deterministic surrogate IDs. Does not modify upstream tables.

Local:
  .venv/bin/python scripts/128_multimodal_contract_mm_v1.py
MotherDuck (writes ONLY to schema mm_contract_dev):
  .venv/bin/python scripts/128_multimodal_contract_mm_v1.py --md

CI / release (fail-closed upstream + blocking validation gates):
  .venv/bin/python scripts/128_multimodal_contract_mm_v1.py --md --strict-release

Local dev without full upstream (stubs in mm_contract_dev — not for release):
  .venv/bin/python scripts/128_multimodal_contract_mm_v1.py --allow-bootstrap-dev

Environment:
  MM_CONTRACT_SCHEMA — override schema name (default mm_contract_dev). For --md,
  the schema is forced to mm_contract_dev unless MM_CONTRACT_MD_SCHEMA_OVERRIDE=1.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
EXPORT_DIR = ROOT / "exports"

sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_or_file  # noqa: E402

from scripts.mm_contract_upstream import (  # noqa: E402
    ensure_upstream_sources,
    validate_upstream_schema_for_strict,
)

CONTRACT_VERSION = "mm_contract_v1.0.0"
SCRIPT_NAME = "128_multimodal_contract_mm_v1.py"

# In --strict-release, every listed validation table must have row count 0.
STRICT_BLOCKING_VALIDATION_TABLES: tuple[str, ...] = (
    "val_contract_required_join_keys_mm_v1",
    "val_nodes_invariant_mm_v1",
    "val_multitumor_expansion_mm_v1",
    "val_side_lobe_mismatch_mm_v1",
    "val_preop_temporal_order_mm_v1",
    "val_ambiguous_multimodal_linkage_mm_v1",
    "val_imaging_fna_contract_blockers_mm_v1",
)

_IFNA_TABLES_TO_QUALIFY: tuple[str, ...] = (
    "val_imaging_fna_linkage_audit_v1",
    "review_queue_imaging_fna_mm_v1",
    "imaging_fna_linkage_mm_v1",
)

# Hash namespace — must remain stable for deterministic IDs
H_NS = "THYROID_MM_CONTRACT_V1"


def _load_ifna129():
    path = ROOT / "scripts" / "129_imaging_fna_linkage_mm_v1.py"
    spec = importlib.util.spec_from_file_location("mm_ifna129", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def _ifna_schema_qualify(sql: str, sch: str) -> str:
    out = sql
    for base in _IFNA_TABLES_TO_QUALIFY:
        out = re.sub(rf"(?<!\.)\b{re.escape(base)}\b", f"{sch}.{base}", out)
    return out


def sql_val_contract_required_join_keys(schema: str, src: dict[str, str]) -> str:
    """NULLs in primary join keys used by the contract (strict blocking when non-empty)."""
    lm = src["linkage_master_v1"]
    oed = src["operative_episode_detail_v2"]
    tum = src["tumor_episode_master_v2"]
    fna = src["fna_episode_master_v2"]
    mol = src["molecular_test_episode_v2"]
    img = src["imaging_nodule_master_v1"]
    eda = src["event_date_audit_v2"]
    return f"""
CREATE OR REPLACE TABLE {schema}.val_contract_required_join_keys_mm_v1 AS
SELECT 'linkage_master_v1'::VARCHAR AS upstream_table,
       'null_research_id'::VARCHAR AS violation_kind,
       CAST(research_id AS VARCHAR) AS bad_key
FROM {lm} WHERE research_id IS NULL
UNION ALL
SELECT 'linkage_master_v1', 'null_canonical_research_id',
       CAST(canonical_research_id AS VARCHAR)
FROM {lm} WHERE canonical_research_id IS NULL
UNION ALL
SELECT 'operative_episode_detail_v2', 'null_research_id', CAST(research_id AS VARCHAR)
FROM {oed} WHERE research_id IS NULL
UNION ALL
SELECT 'operative_episode_detail_v2', 'null_surgery_episode_id',
       CAST(surgery_episode_id AS VARCHAR)
FROM {oed} WHERE surgery_episode_id IS NULL
UNION ALL
SELECT 'tumor_episode_master_v2', 'null_tumor_keys',
       CAST(research_id AS VARCHAR) || '|' || CAST(surgery_episode_id AS VARCHAR) || '|' ||
       CAST(tumor_ordinal AS VARCHAR)
FROM {tum}
WHERE research_id IS NULL OR surgery_episode_id IS NULL OR tumor_ordinal IS NULL
UNION ALL
SELECT 'fna_episode_master_v2', 'null_fna_keys',
       CAST(research_id AS VARCHAR) || '|' || CAST(fna_episode_id AS VARCHAR)
FROM {fna} WHERE research_id IS NULL OR fna_episode_id IS NULL
UNION ALL
SELECT 'molecular_test_episode_v2', 'null_molecular_keys',
       CAST(research_id AS VARCHAR) || '|' || CAST(molecular_episode_id AS VARCHAR)
FROM {mol} WHERE research_id IS NULL OR molecular_episode_id IS NULL
UNION ALL
SELECT 'imaging_nodule_master_v1', 'null_imaging_nodule_keys',
       CAST(img_core.research_id AS VARCHAR) || '|' || CAST(img_core.exam_id AS VARCHAR) || '|' ||
       CAST(img_core.nodule_id AS VARCHAR)
FROM {img} AS img_core
WHERE img_core.research_id IS NULL OR img_core.exam_id IS NULL OR img_core.nodule_id IS NULL OR (
    img_core.exam_date IS NULL AND NOT EXISTS (
        SELECT 1 FROM {eda} e
        WHERE e.research_id = img_core.research_id AND e.domain = 'imaging'
    )
);
""".strip()


def sql_link_imaging_fna_mm_v1(schema: str, built_ts: str, src: dict[str, str]) -> str:
    lm = src["linkage_master_v1"]
    pid = person_id_sql("lm.canonical_research_id")
    up_note = f"{schema}.imaging_fna_linkage_mm_v1+{schema}.fact_imaging_mm_v1+{schema}.fact_fna_mm_v1"
    return f"""
CREATE OR REPLACE TABLE {schema}.link_imaging_fna_mm_v1 AS
SELECT
    'mmv1_ifna_' || lower(md5(concat(
        '{H_NS}|ifna|', cast(l.research_id AS VARCHAR), '|',
        cast(l.nodule_id AS VARCHAR), '|',
        cast(l.imaging_exam_id AS VARCHAR), '|',
        cast(l.fna_episode_id AS VARCHAR)
    ))) AS link_imaging_fna_id,
    fi.imaging_fact_id AS imaging_id,
    ff.fna_fact_id AS fna_id,
    'mmv1_p_' || {pid} AS person_id,
    CAST(l.research_id AS BIGINT) AS research_id,
    CAST(lm.canonical_research_id AS BIGINT) AS canonical_research_id,
    CAST(l.nodule_id AS VARCHAR) AS legacy_nodule_id,
    CAST(l.imaging_exam_id AS VARCHAR) AS legacy_imaging_exam_id,
    CAST(l.fna_episode_id AS BIGINT) AS fna_episode_id,
    CASE l.match_path
        WHEN 'specimen_key' THEN 1.0::DOUBLE
        WHEN 'temporal_us_90d_pre_fna' THEN 0.85::DOUBLE
        ELSE 0.75::DOUBLE
    END AS link_confidence,
    l.is_primary_link AS is_primary_link,
    rq.review_reason AS review_reason,
    (l.n_candidates_for_nodule > 1) AS flag_multi_fna_nodule,
    ((l.n_candidates_for_nodule > 1 AND NOT l.is_primary_link)
        OR (COALESCE(l.n_specimen_matches_on_nodule, 0) > 1)) AS flag_ambiguous_linkage,
    (rq.review_reason = 'discordant_laterality') AS flag_discordant_side,
    ((l.size_drift_ratio IS NOT NULL AND l.size_drift_ratio > 0.20)
        OR rq.review_reason = 'size_drift_gt_20pct') AS flag_size_drift,
    l.match_path AS match_path,
    l.specimen_match_flag AS specimen_match_flag,
    CAST(l.ordinal_in_nodule AS INTEGER) AS ordinal_in_nodule,
    CAST(l.n_candidates_for_nodule AS INTEGER) AS n_candidates_for_nodule,
    CAST(l.n_specimen_matches_on_nodule AS INTEGER) AS n_specimen_matches_on_nodule,
    l.size_drift_ratio AS size_drift_ratio,
    CAST(l.day_gap_us_before_fna AS INTEGER) AS day_gap_us_before_fna,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{up_note}'::VARCHAR AS mm_upstream_tables,
    'Imaging↔FNA linkage (script 129 rules) joined to contract fact IDs.'::VARCHAR AS mm_lineage_note
FROM {schema}.imaging_fna_linkage_mm_v1 l
INNER JOIN {lm} lm ON l.research_id = lm.research_id
INNER JOIN {schema}.fact_imaging_mm_v1 fi
  ON fi.research_id = l.research_id
 AND fi.nodule_id = ('mmv1_n_' || lower(md5(concat(
     '{H_NS}|nodule|', cast(l.research_id AS VARCHAR), '|',
     cast(l.imaging_exam_id AS VARCHAR), '|', cast(l.nodule_id AS VARCHAR)
 ))))
INNER JOIN {schema}.fact_fna_mm_v1 ff
  ON ff.research_id = l.research_id
 AND ff.fna_episode_id = l.fna_episode_id
LEFT JOIN (
    SELECT research_id,
           nodule_id,
           imaging_exam_id,
           CAST(fna_episode_id AS BIGINT) AS fna_episode_id,
           MAX(review_reason) AS review_reason,
           MAX(detail) AS detail
    FROM {schema}.review_queue_imaging_fna_mm_v1
    GROUP BY 1, 2, 3, 4
) rq
  ON rq.research_id = l.research_id
 AND CAST(rq.nodule_id AS VARCHAR) = CAST(l.nodule_id AS VARCHAR)
 AND rq.imaging_exam_id = l.imaging_exam_id
 AND rq.fna_episode_id = l.fna_episode_id;
""".strip()


def sql_val_imaging_fna_contract_blockers(schema: str) -> str:
    """Strict-release blockers: unresolved multi-match only.

    Rows with review_reason = 'discordant_laterality' remain in
    review_queue_imaging_fna_mm_v1 for operators but do not fail the 148
    multimodal blocker table once laterality normalization + deterministic
    primary selection run in script 129.
    """
    return f"""
CREATE OR REPLACE TABLE {schema}.val_imaging_fna_contract_blockers_mm_v1 AS
SELECT
    CAST(research_id AS VARCHAR) AS research_id,
    CAST(nodule_id AS VARCHAR) AS nodule_id,
    imaging_exam_id,
    CAST(fna_episode_id AS VARCHAR) AS fna_episode_id,
    review_reason,
    detail,
    queued_at
FROM {schema}.review_queue_imaging_fna_mm_v1
WHERE review_reason = 'ambiguous_multimatch';
""".strip()


def _build_imaging_fna_contract_tables(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    *,
    built_ts: str,
    section_fn: Callable[[str], None],
    src: dict[str, str],
) -> None:
    ifna = _load_ifna129()
    section_fn("imaging_fna_linkage — wide candidates (129 logic)")
    con.execute(ifna.build_temp_wide_sql(con))
    wide_n = con.execute("SELECT COUNT(*) FROM tt_ifna_mm_wide_pre_v1").fetchone()[0]
    print(f"  tt_ifna_mm_wide_pre_v1 candidate pairs: {wide_n:,}")
    con.execute(_ifna_schema_qualify(ifna.LINK_TABLE_SQL, schema))
    con.execute(_ifna_schema_qualify(ifna.REVIEW_SQL, schema))
    con.execute(_ifna_schema_qualify(ifna.AUDIT_SQL, schema))
    section_fn("link_imaging_fna_mm_v1 (contract fact IDs + flags)")
    con.execute(sql_link_imaging_fna_mm_v1(schema, built_ts, src))
    con.execute(sql_val_imaging_fna_contract_blockers(schema))
    n_link = con.execute(f"SELECT COUNT(*) FROM {schema}.link_imaging_fna_mm_v1").fetchone()[0]
    print(f"  link_imaging_fna_mm_v1 rows: {n_link:,}")
    n_blk = con.execute(
        f"SELECT COUNT(*) FROM {schema}.val_imaging_fna_contract_blockers_mm_v1"
    ).fetchone()[0]
    print(f"  val_imaging_fna_contract_blockers_mm_v1 rows: {n_blk:,}")


def assert_strict_release_passes(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    *,
    bootstrapped_upstream: list[str],
) -> None:
    if bootstrapped_upstream:
        raise RuntimeError(
            "Strict release failed: dev bootstrap was used (or upstream pointed at stubs). "
            f"Bootstrapped keys: {bootstrapped_upstream}. "
            "Run with native upstream tables only."
        )
    failures: list[str] = []
    for v in STRICT_BLOCKING_VALIDATION_TABLES:
        n = con.execute(f"SELECT COUNT(*) FROM {schema}.{v}").fetchone()[0]
        if n:
            failures.append(f"{schema}.{v}={n}")
    if failures:
        raise RuntimeError(
            "Strict release failed: blocking validation tables must be empty. "
            + "; ".join(failures)
        )


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}\n")


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def resolve_schema(*, md: bool) -> str:
    default = "mm_contract_dev"
    if md:
        if os.environ.get("MM_CONTRACT_MD_SCHEMA_OVERRIDE") == "1":
            return os.environ.get("MM_CONTRACT_SCHEMA", default).strip() or default
        return default
    return os.environ.get("MM_CONTRACT_SCHEMA", default).strip() or default


def person_id_sql(canonical_col: str) -> str:
    return (
        f"lower(md5(concat('{H_NS}|person|', cast({canonical_col} AS VARCHAR))))"
    )


def sql_dim_patient(schema: str, built_ts: str, src: dict[str, str]) -> str:
    lm = src["linkage_master_v1"]
    pid = person_id_sql("canonical_research_id")
    return f"""
CREATE OR REPLACE TABLE {schema}.dim_patient_mm_v1 AS
SELECT
    'mmv1_p_' || {pid} AS person_id,
    CAST(canonical_research_id AS BIGINT) AS canonical_research_id,
    ANY_VALUE(euh_mrn) AS euh_mrn,
    ANY_VALUE(linkage_method) AS linkage_method,
    MAX(confidence) AS linkage_confidence,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{lm}'::VARCHAR AS mm_upstream_tables,
    'One row per canonical_research_id (person spine).'::VARCHAR AS mm_lineage_note
FROM {lm}
GROUP BY canonical_research_id;
""".strip()


def sql_map_identifiers(schema: str, built_ts: str, src: dict[str, str]) -> str:
    lm = src["linkage_master_v1"]
    xc = src["mrn_crosswalk_v1"]
    pid = person_id_sql("lm.canonical_research_id")
    return f"""
CREATE OR REPLACE TABLE {schema}.map_patient_identifier_mm_v1 AS
WITH rid_map AS (
    SELECT
        lm.research_id,
        lm.canonical_research_id,
        'mmv1_p_' || {pid} AS person_id
    FROM {lm} lm
)
SELECT
    person_id,
    'research_id'::VARCHAR AS identifier_type,
    CAST(r.research_id AS VARCHAR) AS identifier_value,
    r.canonical_research_id AS resolved_canonical_research_id,
    TRUE::BOOLEAN AS is_spine_row,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{lm}'::VARCHAR AS mm_upstream_tables,
    NULL::VARCHAR AS mm_lineage_note
FROM rid_map r
UNION ALL
SELECT DISTINCT
    'mmv1_p_' || lower(md5(concat('{H_NS}|person|', CAST(x.canonical_research_id AS VARCHAR)))) AS person_id,
    'euh_mrn'::VARCHAR,
    CAST(x.euh_mrn AS VARCHAR),
    x.canonical_research_id,
    FALSE,
    '{CONTRACT_VERSION}'::VARCHAR,
    '{SCRIPT_NAME}'::VARCHAR,
    CAST('{built_ts}' AS TIMESTAMP),
    '{xc}'::VARCHAR,
    NULL::VARCHAR
FROM {xc} x
WHERE x.euh_mrn IS NOT NULL AND CAST(x.euh_mrn AS VARCHAR) <> '';
""".strip()


def sql_fact_surgery(schema: str, built_ts: str, src: dict[str, str]) -> str:
    oed = src["operative_episode_detail_v2"]
    lm = src["linkage_master_v1"]
    tum = src["tumor_episode_master_v2"]
    pid = "lower(md5(concat('{H_NS}|person|', cast(lm.canonical_research_id AS VARCHAR))))".format(H_NS=H_NS)
    return f"""
CREATE OR REPLACE TABLE {schema}.fact_surgery_mm_v1 AS
WITH oed_rows AS (
    SELECT
        'mmv1_s_' || lower(md5(concat('{H_NS}|surgery|', cast(o.research_id AS VARCHAR), '|',
            cast(o.surgery_episode_id AS VARCHAR)))) AS surgery_id,
        'mmv1_p_' || {pid} AS person_id,
        CAST(o.research_id AS BIGINT) AS research_id,
        CAST(lm.canonical_research_id AS BIGINT) AS canonical_research_id,
        CAST(o.surgery_episode_id AS BIGINT) AS surgery_episode_id,
        o.surgery_date_native AS event_time,
        CASE
            WHEN o.surgery_date_native IS NOT NULL THEN 'operative_surgery_date_native'
            ELSE 'missing_native_date'
        END::VARCHAR AS event_time_src,
        o.procedure_raw,
        o.procedure_normalized,
        o.laterality AS surgery_laterality,
        o.central_neck_dissection_flag,
        o.lateral_neck_dissection_flag,
        '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
        '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
        CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
        '{oed},{lm}'::VARCHAR AS mm_upstream_tables,
        NULL::VARCHAR AS mm_lineage_note
    FROM {oed} o
    INNER JOIN {lm} lm ON o.research_id = lm.research_id
),
tumor_stub_keys AS (
    SELECT
        CAST(t.research_id AS BIGINT) AS research_id,
        CAST(t.surgery_episode_id AS BIGINT) AS surgery_episode_id,
        MAX(lm.canonical_research_id) AS canonical_research_id,
        MIN(TRY_CAST(t.surgery_date AS DATE)) AS tumor_anchor_date
    FROM {tum} t
    INNER JOIN {lm} lm ON t.research_id = lm.research_id
    GROUP BY 1, 2
)
SELECT * FROM oed_rows
UNION ALL
SELECT
    'mmv1_s_' || lower(md5(concat(
        '{H_NS}|surgery|', cast(ts.research_id AS VARCHAR), '|', cast(ts.surgery_episode_id AS VARCHAR)
    ))) AS surgery_id,
    'mmv1_p_' || lower(md5(concat(
        '{H_NS}|person|', cast(ts.canonical_research_id AS VARCHAR)
    ))) AS person_id,
    ts.research_id,
    ts.canonical_research_id,
    ts.surgery_episode_id,
    ts.tumor_anchor_date AS event_time,
    'pathology_episode_only_no_operative_row'::VARCHAR AS event_time_src,
    NULL::VARCHAR AS procedure_raw,
    NULL::VARCHAR AS procedure_normalized,
    NULL::VARCHAR AS surgery_laterality,
    NULL::BOOLEAN AS central_neck_dissection_flag,
    NULL::BOOLEAN AS lateral_neck_dissection_flag,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{tum}|pathology_stub'::VARCHAR AS mm_upstream_tables,
    'Stub surgery row: tumor_episode row exists without operative_episode_detail_v2 episode.'::VARCHAR AS mm_lineage_note
FROM tumor_stub_keys ts
WHERE NOT EXISTS (
    SELECT 1 FROM oed_rows o2
    WHERE o2.research_id = ts.research_id AND o2.surgery_episode_id = ts.surgery_episode_id
);
""".strip()


def sql_dim_nodule(schema: str, built_ts: str, src: dict[str, str]) -> str:
    img = src["imaging_nodule_master_v1"]
    return f"""
CREATE OR REPLACE TABLE {schema}.dim_nodule_mm_v1 AS
SELECT DISTINCT
    'mmv1_n_' || lower(md5(concat(
        '{H_NS}|nodule|', cast(i.research_id AS VARCHAR), '|',
        cast(i.exam_id AS VARCHAR), '|', cast(i.nodule_id AS VARCHAR)
    ))) AS nodule_id,
    CAST(i.research_id AS BIGINT) AS research_id,
    i.exam_id AS legacy_exam_id,
    i.nodule_id AS legacy_nodule_id_key,
    i.exam_date,
    i.laterality AS nodule_laterality,
    i.max_dimension_cm,
    i.tirads_reported,
    i.suspicious_flag,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{img}'::VARCHAR AS mm_upstream_tables,
    NULL::VARCHAR AS mm_lineage_note
FROM {img} i;
""".strip()


def sql_fact_imaging(schema: str, built_ts: str, src: dict[str, str]) -> str:
    img = src["imaging_nodule_master_v1"]
    lm = src["linkage_master_v1"]
    eda = src["event_date_audit_v2"]
    pid = "lower(md5(concat('{H_NS}|person|', cast(lm.canonical_research_id AS VARCHAR))))".format(H_NS=H_NS)
    return f"""
CREATE OR REPLACE TABLE {schema}.fact_imaging_mm_v1 AS
SELECT
    'mmv1_i_' || lower(md5(concat(
        '{H_NS}|imaging|', cast(i.research_id AS VARCHAR), '|',
        cast(i.exam_id AS VARCHAR), '|', cast(i.nodule_id AS VARCHAR)
    ))) AS imaging_fact_id,
    'mmv1_p_' || {pid} AS person_id,
    'mmv1_n_' || lower(md5(concat(
        '{H_NS}|nodule|', cast(i.research_id AS VARCHAR), '|',
        cast(i.exam_id AS VARCHAR), '|', cast(i.nodule_id AS VARCHAR)
    ))) AS nodule_id,
    CAST(i.research_id AS BIGINT) AS research_id,
    CAST(lm.canonical_research_id AS BIGINT) AS canonical_research_id,
    CAST(i.nodule_number AS INTEGER) AS nodule_number_within_exam,
    COALESCE(i.exam_date,
        (SELECT MAX(TRY_CAST(resolved_date AS DATE)) FROM {eda} e
         WHERE e.research_id = i.research_id AND e.domain = 'imaging')) AS event_time,
    CASE
        WHEN i.exam_date IS NOT NULL THEN 'imaging_exam_date_native'
        ELSE 'event_date_audit_v2_fallback'
    END::VARCHAR AS event_time_src,
    i.composition,
    i.echogenicity,
    i.shape,
    i.margins,
    i.calcifications,
    i.tirads_category,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{img},{lm},{eda}'::VARCHAR AS mm_upstream_tables,
    NULL::VARCHAR AS mm_lineage_note
FROM {img} i
INNER JOIN {lm} lm ON i.research_id = lm.research_id;
""".strip()


def sql_fact_fna(schema: str, built_ts: str, src: dict[str, str]) -> str:
    fna = src["fna_episode_master_v2"]
    lm = src["linkage_master_v1"]
    eda = src["event_date_audit_v2"]
    pid = "lower(md5(concat('{H_NS}|person|', cast(lm.canonical_research_id AS VARCHAR))))".format(H_NS=H_NS)
    return f"""
CREATE OR REPLACE TABLE {schema}.fact_fna_mm_v1 AS
SELECT
    'mmv1_fn_' || lower(md5(concat(
        '{H_NS}|fna|', cast(f.research_id AS VARCHAR), '|', cast(f.fna_episode_id AS VARCHAR)
    ))) AS fna_fact_id,
    'mmv1_p_' || {pid} AS person_id,
    CAST(f.research_id AS BIGINT) AS research_id,
    CAST(lm.canonical_research_id AS BIGINT) AS canonical_research_id,
    CAST(f.fna_episode_id AS BIGINT) AS fna_episode_id,
    COALESCE(f.fna_date_native,
        TRY_CAST(f.resolved_fna_date AS DATE)) AS event_time,
    CASE
        WHEN f.fna_date_native IS NOT NULL THEN 'fna_date_native'
        WHEN f.resolved_fna_date IS NOT NULL THEN 'fna_resolved_date'
        ELSE 'event_date_audit_v2_fallback'
    END::VARCHAR AS event_time_src,
    f.bethesda_category,
    f.specimen_site_raw,
    f.laterality AS fna_laterality,
    f.pathology_diagnosis,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{fna},{lm}'::VARCHAR AS mm_upstream_tables,
    NULL::VARCHAR AS mm_lineage_note
FROM {fna} f
INNER JOIN {lm} lm ON f.research_id = lm.research_id;

UPDATE {schema}.fact_fna_mm_v1 AS t
SET event_time = ed.audit_date,
    event_time_src = 'event_date_audit_v2_fallback',
    mm_lineage_note = COALESCE(t.mm_lineage_note, '') || 'date_from_event_date_audit_v2;'
FROM (
    SELECT research_id,
           MAX(TRY_CAST(resolved_date AS DATE)) AS audit_date
    FROM {eda}
    WHERE domain = 'fna'
    GROUP BY research_id
) ed
WHERE t.event_time IS NULL
  AND t.research_id = ed.research_id;
""".strip()


def sql_fact_genetics(schema: str, built_ts: str, src: dict[str, str]) -> str:
    mol = src["molecular_test_episode_v2"]
    lm = src["linkage_master_v1"]
    eda = src["event_date_audit_v2"]
    pid = "lower(md5(concat('{H_NS}|person|', cast(lm.canonical_research_id AS VARCHAR))))".format(H_NS=H_NS)
    return f"""
CREATE OR REPLACE TABLE {schema}.fact_genetics_mm_v1 AS
SELECT
    'mmv1_g_' || lower(md5(concat(
        '{H_NS}|mol|', cast(m.research_id AS VARCHAR), '|', cast(m.molecular_episode_id AS VARCHAR)
    ))) AS genetics_fact_id,
    'mmv1_p_' || {pid} AS person_id,
    CAST(m.research_id AS BIGINT) AS research_id,
    CAST(lm.canonical_research_id AS BIGINT) AS canonical_research_id,
    CAST(m.molecular_episode_id AS BIGINT) AS molecular_episode_id,
    COALESCE(m.test_date_native, TRY_CAST(m.resolved_test_date AS DATE)) AS event_time,
    CASE
        WHEN m.test_date_native IS NOT NULL THEN 'molecular_test_date_native'
        WHEN m.resolved_test_date IS NOT NULL THEN 'molecular_resolved_date'
        ELSE 'event_date_audit_v2_fallback'
    END::VARCHAR AS event_time_src,
    m.platform,
    m.overall_result_class,
    m.braf_flag,
    m.ras_flag,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{mol},{lm}'::VARCHAR AS mm_upstream_tables,
    NULL::VARCHAR AS mm_lineage_note
FROM {mol} m
INNER JOIN {lm} lm ON m.research_id = lm.research_id;

UPDATE {schema}.fact_genetics_mm_v1 AS t
SET event_time = ed.audit_date,
    event_time_src = 'event_date_audit_v2_fallback',
    mm_lineage_note = COALESCE(t.mm_lineage_note, '') || 'date_from_event_date_audit_v2;'
FROM (
    SELECT research_id,
           MAX(TRY_CAST(resolved_date AS DATE)) AS audit_date
    FROM {eda}
    WHERE domain = 'molecular'
    GROUP BY research_id
) ed
WHERE t.event_time IS NULL
  AND t.research_id = ed.research_id;
""".strip()


def sql_fact_tumor(schema: str, built_ts: str, src: dict[str, str]) -> str:
    tum = src["tumor_episode_master_v2"]
    lm = src["linkage_master_v1"]
    pid = "lower(md5(concat('{H_NS}|person|', cast(lm.canonical_research_id AS VARCHAR))))".format(H_NS=H_NS)
    return f"""
CREATE OR REPLACE TABLE {schema}.fact_tumor_mm_v1 AS
SELECT
    'mmv1_t_' || lower(md5(concat(
        '{H_NS}|tumor|', cast(t.research_id AS VARCHAR), '|',
        cast(t.surgery_episode_id AS VARCHAR), '|', cast(t.tumor_ordinal AS VARCHAR), '|',
        coalesce(cast(t.surgery_date AS VARCHAR), '')
    ))) AS tumor_instance_id,
    'mmv1_p_' || {pid} AS person_id,
    'mmv1_s_' || lower(md5(concat(
        '{H_NS}|surgery|', cast(t.research_id AS VARCHAR), '|', cast(t.surgery_episode_id AS VARCHAR)
    ))) AS surgery_id,
    CAST(t.research_id AS BIGINT) AS research_id,
    CAST(lm.canonical_research_id AS BIGINT) AS canonical_research_id,
    CAST(t.surgery_episode_id AS BIGINT) AS surgery_episode_id,
    CAST(t.tumor_ordinal AS INTEGER) AS tumor_ordinal,
    TRY_CAST(t.surgery_date AS DATE) AS event_time,
    CASE
        WHEN t.surgery_date IS NOT NULL AND TRY_CAST(t.surgery_date AS DATE) IS NOT NULL
            THEN 'pathology_surgery_date'
        WHEN t.date_status = 'exact_source_date' THEN 'pathology_date_exact'
        ELSE 'pathology_episode_resolved'
    END::VARCHAR AS event_time_src,
    t.primary_histology,
    t.tumor_size_cm,
    t.t_stage,
    t.n_stage,
    t.overall_stage,
    t.laterality AS tumor_laterality,
    t.multifocality_flag,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{tum},{lm}'::VARCHAR AS mm_upstream_tables,
    'One row per tumor per surgery; never collapsed.'::VARCHAR AS mm_lineage_note
FROM {tum} t
INNER JOIN {lm} lm ON t.research_id = lm.research_id;
""".strip()


def sql_fact_path_report(schema: str, built_ts: str, src: dict[str, str]) -> str:
    tum = src["tumor_episode_master_v2"]
    lm = src["linkage_master_v1"]
    return f"""
CREATE OR REPLACE TABLE {schema}.fact_path_report_mm_v1 AS
SELECT
    'mmv1_pr_' || lower(md5(concat(
        '{H_NS}|pathrep|', cast(t.research_id AS VARCHAR), '|', cast(t.surgery_episode_id AS VARCHAR), '|',
        coalesce(cast(min(t.surgery_date) AS VARCHAR), '')
    ))) AS path_report_id,
    'mmv1_p_' || lower(md5(concat(
        '{H_NS}|person|', cast(MAX(lm.canonical_research_id) AS VARCHAR)
    ))) AS person_id,
    'mmv1_s_' || lower(md5(concat(
        '{H_NS}|surgery|', cast(t.research_id AS VARCHAR), '|', cast(t.surgery_episode_id AS VARCHAR)
    ))) AS surgery_id,
    CAST(t.research_id AS BIGINT) AS research_id,
    CAST(MAX(lm.canonical_research_id) AS BIGINT) AS canonical_research_id,
    CAST(t.surgery_episode_id AS BIGINT) AS surgery_episode_id,
    MIN(TRY_CAST(t.surgery_date AS DATE)) AS event_time,
    'pathology_synoptic_aggregated_per_surgery'::VARCHAR AS event_time_src,
    COUNT(*)::BIGINT AS tumor_row_count_in_source,
    STRING_AGG(CAST(t.tumor_ordinal AS VARCHAR), ',' ORDER BY t.tumor_ordinal) AS tumor_ordinals_in_report,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{tum},{lm}'::VARCHAR AS mm_upstream_tables,
    'Report grain = surgery episode; tumors enumerated in tumor_ordinals_in_report.'::VARCHAR AS mm_lineage_note
FROM {tum} t
INNER JOIN {lm} lm ON t.research_id = lm.research_id
GROUP BY t.research_id, t.surgery_episode_id;
""".strip()


def sql_link_surgery_path(schema: str, built_ts: str, src: dict[str, str]) -> str:
    spv3 = src["surgery_pathology_linkage_v3"]
    return f"""
CREATE OR REPLACE TABLE {schema}.link_surgery_path_mm_v1 AS
SELECT
    'mmv1_lsp_' || lower(md5(concat(
        '{H_NS}|lsp|', cast(sp.research_id AS VARCHAR), '|', cast(sp.surgery_episode_id AS VARCHAR), '|',
        cast(sp.path_surgery_id AS VARCHAR), '|', cast(sp.tumor_ordinal AS VARCHAR)
    ))) AS link_row_id,
    'mmv1_s_' || lower(md5(concat(
        '{H_NS}|surgery|', cast(sp.research_id AS VARCHAR), '|', cast(sp.surgery_episode_id AS VARCHAR)
    ))) AS surgery_id,
    ft.tumor_instance_id AS tumor_instance_id,
    CAST(sp.research_id AS BIGINT) AS research_id,
    CAST(sp.surgery_episode_id AS BIGINT) AS surgery_episode_id,
    CAST(sp.path_surgery_id AS BIGINT) AS path_surgery_episode_id,
    CAST(sp.tumor_ordinal AS INTEGER) AS tumor_ordinal,
    sp.linkage_score,
    sp.linkage_confidence_tier,
    sp.score_rank,
    sp.n_candidates,
    sp.analysis_eligible_link_flag,
    (sp.score_rank = 1
        AND sp.analysis_eligible_link_flag
        AND sp.linkage_confidence_tier IN ('exact_match', 'high_confidence', 'plausible')
        AND sp.n_candidates = 1
        AND ft.tumor_instance_id IS NOT NULL
    ) AS is_primary_link,
    sp.day_gap,
    sp.surg_lat,
    sp.path_lat,
    sp.linkage_reason_summary,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{spv3},{schema}.fact_tumor_mm_v1'::VARCHAR AS mm_upstream_tables,
    CASE WHEN sp.n_candidates > 1 OR sp.linkage_confidence_tier IN ('weak', 'unlinked')
         THEN 'ambiguous_or_weak_excluded_from_primary'
         WHEN ft.tumor_instance_id IS NULL THEN 'no_matching_fact_tumor_row'
         ELSE NULL
    END::VARCHAR AS context_flags
FROM {spv3} sp
LEFT JOIN {schema}.fact_tumor_mm_v1 ft
  ON sp.research_id = ft.research_id
 AND sp.path_surgery_id = ft.surgery_episode_id
 AND sp.tumor_ordinal = ft.tumor_ordinal;
""".strip()


def sql_link_surgery_context(schema: str, built_ts: str, src: dict[str, str]) -> str:
    spv3 = src["surgery_pathology_linkage_v3"]
    psv3 = src["preop_surgery_linkage_v3"]
    fmv3 = src["fna_molecular_linkage_v3"]
    tl_v2 = src["patient_cross_domain_timeline_v2"]
    rai_v3 = src["pathology_rai_linkage_v3"]
    oed = src["operative_episode_detail_v2"]
    lm = src["linkage_master_v1"]
    up = f"{spv3},{psv3},{fmv3},{rai_v3},{tl_v2},{oed},{lm}"
    return f"""
CREATE OR REPLACE TABLE {schema}.link_surgery_context_mm_v1 AS
WITH sp AS (
    SELECT research_id, surgery_episode_id,
           MAX(CASE WHEN score_rank = 1 THEN linkage_score END) AS best_path_score,
           MAX(CASE WHEN score_rank = 1 THEN linkage_confidence_tier END) AS best_path_tier,
           MAX(n_candidates) AS max_path_candidates
    FROM {spv3}
    GROUP BY research_id, surgery_episode_id
),
preop AS (
    SELECT research_id, surgery_episode_id,
           MAX(CASE WHEN score_rank = 1 THEN linkage_score END) AS best_preop_score,
           MAX(CASE WHEN score_rank = 1 THEN linkage_confidence_tier END) AS best_preop_tier,
           MAX(n_candidates) AS max_preop_candidates,
           MAX(preop_type) AS sample_preop_type
    FROM {psv3}
    GROUP BY research_id, surgery_episode_id
),
fm AS (
    SELECT research_id,
           MAX(CASE WHEN score_rank = 1 THEN linkage_score END) AS best_fm_score,
           MAX(n_candidates) AS max_fm_candidates
    FROM {fmv3}
    GROUP BY research_id
),
tl AS (
    SELECT research_id, COUNT(*) AS timeline_event_cnt
    FROM {tl_v2}
    GROUP BY research_id
),
prai AS (
    SELECT research_id, surgery_episode_id,
           MAX(CASE WHEN score_rank = 1 THEN linkage_score END) AS best_rai_score,
           MAX(n_candidates) AS max_rai_candidates
    FROM {rai_v3}
    GROUP BY research_id, surgery_episode_id
)
SELECT
    'mmv1_ctx_' || lower(md5(concat(
        '{H_NS}|ctx|', cast(o.research_id AS VARCHAR), '|', cast(o.surgery_episode_id AS VARCHAR)
    ))) AS context_id,
    'mmv1_s_' || lower(md5(concat(
        '{H_NS}|surgery|', cast(o.research_id AS VARCHAR), '|', cast(o.surgery_episode_id AS VARCHAR)
    ))) AS surgery_id,
    CAST(o.research_id AS BIGINT) AS research_id,
    CAST(lm.canonical_research_id AS BIGINT) AS canonical_research_id,
    sp.best_path_score,
    sp.best_path_tier,
    sp.max_path_candidates,
    pr.best_preop_score,
    pr.best_preop_tier,
    pr.max_preop_candidates,
    pr.sample_preop_type,
    fm.best_fm_score,
    fm.max_fm_candidates,
    rai.best_rai_score,
    rai.max_rai_candidates,
    tl.timeline_event_cnt,
    '{CONTRACT_VERSION}'::VARCHAR AS mm_contract_version,
    '{SCRIPT_NAME}'::VARCHAR AS mm_source_script,
    CAST('{built_ts}' AS TIMESTAMP) AS mm_built_at,
    '{up}'::VARCHAR AS mm_upstream_tables,
    NULL::VARCHAR AS mm_lineage_note
FROM {oed} o
INNER JOIN {lm} lm ON o.research_id = lm.research_id
LEFT JOIN sp ON sp.research_id = o.research_id AND sp.surgery_episode_id = o.surgery_episode_id
LEFT JOIN preop pr ON pr.research_id = o.research_id AND pr.surgery_episode_id = o.surgery_episode_id
LEFT JOIN fm ON fm.research_id = o.research_id
LEFT JOIN prai rai ON rai.research_id = o.research_id AND rai.surgery_episode_id = o.surgery_episode_id
LEFT JOIN tl ON tl.research_id = o.research_id;
""".strip()


def sql_validations(schema: str, src: dict[str, str]) -> str:
    psv3 = src["preop_surgery_linkage_v3"]
    fmv3 = src["fna_molecular_linkage_v3"]
    tum = src["tumor_episode_master_v2"]
    return f"""
CREATE OR REPLACE TABLE {schema}.val_nodes_invariant_mm_v1 AS
SELECT 'orphan_fact_surgery_person'::VARCHAR AS violation_type,
       CAST(fs.research_id AS VARCHAR) AS entity_a,
       fs.surgery_id::VARCHAR AS entity_b,
       fs.person_id::VARCHAR AS entity_c,
       NULL::VARCHAR AS detail
FROM {schema}.fact_surgery_mm_v1 fs
LEFT JOIN {schema}.dim_patient_mm_v1 dp ON fs.person_id = dp.person_id
WHERE dp.person_id IS NULL
UNION ALL
SELECT 'orphan_fact_tumor_person',
       CAST(ft.research_id AS VARCHAR), ft.surgery_id, ft.person_id, NULL
FROM {schema}.fact_tumor_mm_v1 ft
LEFT JOIN {schema}.dim_patient_mm_v1 dp ON ft.person_id = dp.person_id
WHERE dp.person_id IS NULL
UNION ALL
SELECT 'orphan_fact_tumor_surgery',
       CAST(ft.research_id AS VARCHAR), ft.surgery_id, ft.tumor_instance_id,
       'surgery_id not in fact_surgery'::VARCHAR
FROM {schema}.fact_tumor_mm_v1 ft
LEFT JOIN {schema}.fact_surgery_mm_v1 fs ON ft.surgery_id = fs.surgery_id
WHERE fs.surgery_id IS NULL
UNION ALL
SELECT 'orphan_link_surgery_path_tumor',
       CAST(sp.research_id AS VARCHAR), sp.surgery_id,
       COALESCE(sp.tumor_instance_id::VARCHAR, 'NULL'),
       'primary_requires_resolved_tumor'::VARCHAR
FROM {schema}.link_surgery_path_mm_v1 sp
WHERE sp.is_primary_link
  AND sp.tumor_instance_id IS NULL
UNION ALL
SELECT 'primary_link_tumor_surgery_mismatch',
       CAST(sp.research_id AS VARCHAR), sp.surgery_id, sp.tumor_instance_id,
       'path_surgery_episode_id != fact_tumor.surgery_episode_id'::VARCHAR
FROM {schema}.link_surgery_path_mm_v1 sp
INNER JOIN {schema}.fact_tumor_mm_v1 ft ON sp.tumor_instance_id = ft.tumor_instance_id
WHERE sp.is_primary_link
  AND ft.surgery_episode_id IS DISTINCT FROM sp.path_surgery_episode_id;

CREATE OR REPLACE TABLE {schema}.val_side_lobe_mismatch_mm_v1 AS
SELECT 'surgery_pathology_lat_mismatch'::VARCHAR AS issue_type,
       sp.research_id,
       sp.surgery_episode_id,
       sp.surg_lat AS lat_a,
       sp.path_lat AS lat_b,
       sp.linkage_confidence_tier AS tier,
       sp.is_primary_link AS is_primary_bridge
FROM {schema}.link_surgery_path_mm_v1 sp
WHERE sp.is_primary_link
  AND sp.linkage_confidence_tier IN ('exact_match', 'high_confidence')
  AND sp.surg_lat IS NOT NULL AND sp.path_lat IS NOT NULL
  AND lower(sp.surg_lat) <> lower(sp.path_lat)
  AND lower(sp.surg_lat) NOT IN ('isthmus')
  AND lower(sp.path_lat) NOT IN ('isthmus')
  AND lower(sp.surg_lat) NOT LIKE '%bilateral%'
  AND lower(sp.path_lat) NOT LIKE '%bilateral%'
UNION ALL
SELECT 'preop_surgery_lat_mismatch'::VARCHAR,
       ps.research_id,
       ps.surgery_episode_id,
       ps.preop_lat,
       ps.surg_lat,
       ps.linkage_confidence_tier,
       (ps.score_rank = 1 AND ps.analysis_eligible_link_flag
        AND ps.linkage_confidence_tier IN ('exact_match', 'high_confidence', 'plausible')
        AND ps.n_candidates = 1)
FROM {psv3} ps
WHERE ps.score_rank = 1
  AND ps.analysis_eligible_link_flag
  AND ps.preop_lat IS NOT NULL AND ps.surg_lat IS NOT NULL
  AND lower(ps.preop_lat) <> lower(ps.surg_lat)
  AND lower(ps.preop_lat) NOT IN ('isthmus')
  AND lower(ps.surg_lat) NOT IN ('isthmus')
  AND lower(ps.preop_lat) NOT LIKE '%bilateral%'
  AND lower(ps.surg_lat) NOT LIKE '%bilateral%';

CREATE OR REPLACE TABLE {schema}.val_preop_temporal_order_mm_v1 AS
SELECT 'preop_after_surgery_calendar'::VARCHAR AS issue_type,
       CAST(research_id AS BIGINT) AS research_id,
       CAST(preop_episode_id AS VARCHAR) AS ref_a,
       CAST(surgery_episode_id AS VARCHAR) AS ref_b,
       CAST(preop_date AS VARCHAR) AS time_a,
       CAST(surgery_date AS VARCHAR) AS time_b,
       CAST(day_gap AS VARCHAR) AS metric
FROM {psv3}
WHERE preop_date > surgery_date + INTERVAL 7 DAY
UNION ALL
SELECT 'molecular_before_fna_excess'::VARCHAR,
       CAST(research_id AS BIGINT),
       CAST(fna_episode_id AS VARCHAR),
       CAST(molecular_episode_id AS VARCHAR),
       CAST(fna_date_native AS VARCHAR),
       CAST(test_date_native AS VARCHAR),
       CAST(day_gap AS VARCHAR)
FROM {fmv3}
WHERE score_rank = 1
  AND analysis_eligible_link_flag
  AND day_gap < -8;

CREATE OR REPLACE TABLE {schema}.val_ambiguous_multimodal_linkage_mm_v1 AS
SELECT 'surgery_pathology'::VARCHAR AS domain,
       CAST(sp.research_id AS VARCHAR) AS research_id,
       CAST(sp.surgery_episode_id AS VARCHAR) AS surgery_episode_id,
       sp.linkage_confidence_tier,
       sp.n_candidates,
       sp.score_rank,
       sp.analysis_eligible_link_flag,
       sp.is_primary_link,
       sp.linkage_reason_summary AS detail
FROM {schema}.link_surgery_path_mm_v1 sp
WHERE sp.score_rank = 1
  AND sp.linkage_confidence_tier = 'unlinked';

CREATE OR REPLACE TABLE {schema}.val_multitumor_expansion_mm_v1 AS
WITH src AS (
    SELECT research_id, surgery_episode_id, COUNT(*)::BIGINT AS n_src
    FROM {tum}
    GROUP BY research_id, surgery_episode_id
),
dst AS (
    SELECT research_id, surgery_episode_id, COUNT(*)::BIGINT AS n_contract
    FROM {schema}.fact_tumor_mm_v1
    GROUP BY research_id, surgery_episode_id
)
SELECT s.research_id,
       s.surgery_episode_id,
       s.n_src,
       COALESCE(d.n_contract, 0::BIGINT) AS n_contract,
       'tumor_row_count_mismatch'::VARCHAR AS issue
FROM src s
LEFT JOIN dst d ON s.research_id = d.research_id AND s.surgery_episode_id = d.surgery_episode_id
WHERE s.n_src <> COALESCE(d.n_contract, 0::BIGINT);

CREATE OR REPLACE VIEW {schema}.val_multi_tumor_report_mm_v1 AS
SELECT * FROM {schema}.val_multitumor_expansion_mm_v1;
""".strip()


def apply_comments(con: duckdb.DuckDBPyConnection, schema: str) -> None:
    comments: list[tuple[str, str | None]] = []

    def tbl(name: str, text: str) -> None:
        comments.append((f"COMMENT ON TABLE {schema}.{name} IS '{text.replace(chr(39), chr(39)+chr(39))}';", None))

    def col(table: str, column: str, text: str) -> None:
        q = text.replace("'", "''")
        comments.append(
            (f"COMMENT ON COLUMN {schema}.{table}.{column} IS '{q}';", None),
        )

    tbl("dim_patient_mm_v1", "Person dimension; one row per canonical_research_id (MRN-resolved spine).")
    col("dim_patient_mm_v1", "person_id", "Deterministic surrogate; stable hash of canonical_research_id.")
    col("dim_patient_mm_v1", "canonical_research_id", "MRN-linked canonical patient key from linkage_master_v1.")

    tbl("map_patient_identifier_mm_v1", "Maps institutional identifiers and research_id to person_id.")
    col("map_patient_identifier_mm_v1", "identifier_type", "research_id | euh_mrn | ...")
    col("map_patient_identifier_mm_v1", "person_id", "FK logical to dim_patient_mm_v1.")

    tbl("fact_surgery_mm_v1", "Surgical episode facts from operative_episode_detail_v2 + person linkage.")
    col("fact_surgery_mm_v1", "surgery_id", "Deterministic surrogate per (research_id, surgery_episode_id).")
    col("fact_surgery_mm_v1", "event_time", "Surgery date (native).")
    col("fact_surgery_mm_v1", "event_time_src", "Provenance of event_time.")

    tbl("dim_nodule_mm_v1", "Imaging nodule dimension from imaging_nodule_master_v1.")
    col("dim_nodule_mm_v1", "nodule_id", "Contract nodule key; hash of legacy keys.")

    tbl("fact_imaging_mm_v1", "Per-nodule imaging facts.")
    col("fact_imaging_mm_v1", "imaging_fact_id", "Surrogate per imaging/nodule row.")
    col("fact_imaging_mm_v1", "event_time", "Exam date with audit fallback.")

    tbl("fact_fna_mm_v1", "FNA episode facts.")
    col("fact_fna_mm_v1", "fna_fact_id", "Surrogate per FNA episode.")

    tbl("fact_genetics_mm_v1", "Molecular / genomics test facts.")
    col("fact_genetics_mm_v1", "genetics_fact_id", "Surrogate per molecular episode.")

    tbl("fact_path_report_mm_v1", "Pathology report header grain per surgery episode.")
    col("fact_path_report_mm_v1", "path_report_id", "Surrogate per surgery episode pathology encounter.")

    tbl("fact_tumor_mm_v1", "Tumor-level pathology; one row per tumor row in tumor_episode_master_v2.")
    col("fact_tumor_mm_v1", "tumor_instance_id", "Deterministic surrogate per tumor ordinal within surgery.")

    tbl("link_surgery_path_mm_v1", "Surgery-to-pathology tumor linkage with primary flag.")
    col("link_surgery_path_mm_v1", "is_primary_link", "TRUE only for unambiguous, non-weak eligible best rank.")

    tbl("link_surgery_context_mm_v1", "Per-surgery multimodal linkage context and timeline density.")
    col("link_surgery_context_mm_v1", "best_rai_score", "Best rank-1 pathology–RAI linkage score from pathology_rai_linkage_v3.")
    col("link_surgery_context_mm_v1", "max_rai_candidates", "Ambiguity count for pathology–RAI candidates.")

    tbl("val_nodes_invariant_mm_v1", "Fail-closed graph invariant violations (empty = pass).")
    tbl("val_side_lobe_mismatch_mm_v1", "Laterality mismatches on primary bridges.")
    tbl("val_preop_temporal_order_mm_v1", "Temporal ordering issues in preop/molecular chains.")
    tbl(
        "val_ambiguous_multimodal_linkage_mm_v1",
        "Strict release: rank-1 surgery–pathology rows still in unlinked tier (preop ambiguity tracked in linkage metrics, not here).",
    )
    tbl("val_multitumor_expansion_mm_v1", "Tumor row count parity vs tumor_episode_master_v2 per surgery.")
    tbl("link_imaging_fna_mm_v1", "Imaging nodule ↔ FNA episode linkage with contract surrogate IDs and review flags.")
    col("link_imaging_fna_mm_v1", "imaging_id", "FK to fact_imaging_mm_v1.imaging_fact_id.")
    col("link_imaging_fna_mm_v1", "fna_id", "FK to fact_fna_mm_v1.fna_fact_id.")
    col("link_imaging_fna_mm_v1", "link_confidence", "Numeric confidence; 1.0 specimen key, 0.85 temporal 90d path.")
    col("link_imaging_fna_mm_v1", "flag_ambiguous_linkage", "True when multi-match or no primary under 129 rules.")
    tbl("imaging_fna_linkage_mm_v1", "Raw imaging–FNA pairs from script 129 rules (contract schema copy).")
    tbl("review_queue_imaging_fna_mm_v1", "Imaging–FNA pairs requiring manual review (blocker source in strict mode).")
    tbl("val_imaging_fna_linkage_audit_v1", "Aggregate audit metrics for imaging–FNA linkage build.")
    tbl("val_contract_required_join_keys_mm_v1", "NULL or missing-date imaging rows violating required join keys.")
    tbl(
        "val_imaging_fna_contract_blockers_mm_v1",
        "Subsets review_queue to ambiguous_multimatch pairs with no deterministic primary after script 129.",
    )
    # val_multi_tumor_report_mm_v1 is a VIEW; DuckDB COMMENT ON TABLE does not apply.

    for stmt in comments:
        if stmt[0]:
            try:
                con.execute(stmt[0])
            except Exception as e:
                print(f"  [WARN] COMMENT skipped: {e}")


def build_all(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    *,
    allow_bootstrap_dev: bool = False,
    strict_release: bool = False,
) -> list[str]:
    if strict_release and allow_bootstrap_dev:
        raise ValueError("--strict-release cannot be combined with --allow-bootstrap-dev")
    built_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    section(f"CREATE SCHEMA {schema}")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    src = ensure_upstream_sources(
        con, schema, section=section, allow_bootstrap=allow_bootstrap_dev
    )
    if strict_release:
        validate_upstream_schema_for_strict(con, src)
    bootstrapped = sorted(k for k, v in src.items() if v != k)

    steps = [
        ("dim_patient_mm_v1", sql_dim_patient(schema, built_ts, src)),
        ("map_patient_identifier_mm_v1", sql_map_identifiers(schema, built_ts, src)),
        ("fact_surgery_mm_v1", sql_fact_surgery(schema, built_ts, src)),
        ("dim_nodule_mm_v1", sql_dim_nodule(schema, built_ts, src)),
        ("fact_imaging_mm_v1", sql_fact_imaging(schema, built_ts, src)),
        ("fact_fna_mm_v1", sql_fact_fna(schema, built_ts, src)),
        ("fact_genetics_mm_v1", sql_fact_genetics(schema, built_ts, src)),
        ("fact_tumor_mm_v1", sql_fact_tumor(schema, built_ts, src)),
        ("fact_path_report_mm_v1", sql_fact_path_report(schema, built_ts, src)),
        ("link_surgery_path_mm_v1", sql_link_surgery_path(schema, built_ts, src)),
        ("link_surgery_context_mm_v1", sql_link_surgery_context(schema, built_ts, src)),
    ]

    for label, sql in steps:
        section(label)
        con.execute(sql)
        n = con.execute(f"SELECT COUNT(*) FROM {schema}.{label}").fetchone()[0]
        print(f"  rows: {n:,}")

    _build_imaging_fna_contract_tables(
        con, schema, built_ts=built_ts, section_fn=section, src=src
    )

    section("val_contract_required_join_keys_mm_v1")
    con.execute(sql_val_contract_required_join_keys(schema, src))
    n_jk = con.execute(
        f"SELECT COUNT(*) FROM {schema}.val_contract_required_join_keys_mm_v1"
    ).fetchone()[0]
    print(f"  val_contract_required_join_keys_mm_v1: {n_jk:,} rows")

    section("validation tables (legacy multimodal audits)")
    con.execute(sql_validations(schema, src))
    for v in (
        "val_contract_required_join_keys_mm_v1",
        "val_nodes_invariant_mm_v1",
        "val_side_lobe_mismatch_mm_v1",
        "val_preop_temporal_order_mm_v1",
        "val_ambiguous_multimodal_linkage_mm_v1",
        "val_multitumor_expansion_mm_v1",
        "val_imaging_fna_contract_blockers_mm_v1",
    ):
        n = con.execute(f"SELECT COUNT(*) FROM {schema}.{v}").fetchone()[0]
        print(f"  {v}: {n:,} rows")
    try:
        nrep = con.execute(f"SELECT COUNT(*) FROM {schema}.val_multi_tumor_report_mm_v1").fetchone()[0]
        print(f"  val_multi_tumor_report_mm_v1 (view): {nrep:,} rows")
    except Exception as e:
        print(f"  [WARN] val_multi_tumor_report_mm_v1: {e}")

    section("COMMENT ON metadata")
    apply_comments(con, schema)
    return bootstrapped


def collect_release_validation_metrics(
    con: duckdb.DuckDBPyConnection,
    schema: str,
) -> dict:
    """Structured counts for operators and CI artifacts (blocking tables + interpretive breakdowns)."""
    blocking = {}
    for v in STRICT_BLOCKING_VALIDATION_TABLES:
        blocking[v] = int(
            con.execute(f"SELECT COUNT(*) FROM {schema}.{v}").fetchone()[0]
        )
    amb = con.execute(
        f"""
        SELECT
          COUNT(*) FILTER (WHERE flag_ambiguous_linkage)::BIGINT,
          COUNT(*) FILTER (WHERE flag_multi_fna_nodule)::BIGINT,
          COUNT(*) FILTER (WHERE flag_discordant_side)::BIGINT,
          COUNT(*)::BIGINT
        FROM {schema}.link_imaging_fna_mm_v1
        """
    ).fetchone()
    by_dom = con.execute(
        f"""
        SELECT domain::VARCHAR, COUNT(*)::BIGINT
        FROM {schema}.val_ambiguous_multimodal_linkage_mm_v1
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    lat = con.execute(
        f"""
        SELECT COALESCE(issue_type::VARCHAR, '') AS issue_type, COUNT(*)::BIGINT
        FROM {schema}.val_side_lobe_mismatch_mm_v1
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    tmp = con.execute(
        f"""
        SELECT COALESCE(issue_type::VARCHAR, '') AS issue_type, COUNT(*)::BIGINT
        FROM {schema}.val_preop_temporal_order_mm_v1
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    node = con.execute(
        f"""
        SELECT COALESCE(violation_type::VARCHAR, '') AS violation_type, COUNT(*)::BIGINT
        FROM {schema}.val_nodes_invariant_mm_v1
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    rq = con.execute(
        f"""
        SELECT COALESCE(review_reason::VARCHAR, ''), COUNT(*)::BIGINT
        FROM {schema}.review_queue_imaging_fna_mm_v1
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    imaging_fna_audit: dict = {}
    try:
        audit_df = con.execute(
            f"SELECT * FROM {schema}.val_imaging_fna_linkage_audit_v1"
        ).fetchdf()
        if len(audit_df):
            imaging_fna_audit = audit_df.iloc[0].to_dict()
            for k, v in list(imaging_fna_audit.items()):
                if hasattr(v, "isoformat"):
                    imaging_fna_audit[k] = v.isoformat()
    except Exception:
        pass
    return {
        "blocking_validation_row_counts": blocking,
        "imaging_fna_link_flags": {
            "ambiguous_link_rows": int(amb[0] or 0),
            "multi_fna_nodule_rows": int(amb[1] or 0),
            "discordant_side_rows": int(amb[2] or 0),
            "total_link_rows": int(amb[3] or 0),
        },
        "ambiguous_multimodal_by_domain": {str(r[0]): int(r[1]) for r in by_dom},
        "laterality_mismatch_by_issue_type": {str(r[0]): int(r[1]) for r in lat},
        "temporal_violation_by_issue_type": {str(r[0]): int(r[1]) for r in tmp},
        "node_invariant_by_violation_type": {str(r[0]): int(r[1]) for r in node},
        "review_queue_by_reason": {str(r[0]): int(r[1]) for r in rq},
        "imaging_fna_audit": imaging_fna_audit,
    }


def load_prior_gate_artifact(path: Path) -> dict | None:
    """Load a prior multimodal_release_gate_v1 JSON for review-queue deltas."""
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def compute_review_queue_deltas(
    current_by_reason: dict[str, int],
    prior_body: dict | None,
) -> dict:
    """Delta = current_count - prior_count per review_reason (prior may be missing)."""
    prior_reason: dict[str, int] | None = None
    if prior_body:
        metrics = prior_body.get("release_validation_metrics")
        if isinstance(metrics, dict):
            pr = metrics.get("review_queue_by_reason")
            if isinstance(pr, dict):
                prior_reason = {str(k): int(v) for k, v in pr.items()}
        if prior_reason is None:
            legacy = prior_body.get("review_queue_breakdown")
            if isinstance(legacy, dict):
                prior_reason = {str(k): int(v) for k, v in legacy.items()}
    if not prior_reason:
        return {
            "available": False,
            "note": "No prior release_validation_metrics.review_queue_by_reason in artifact",
            "by_reason": {},
        }
    keys = set(current_by_reason) | set(prior_reason)
    by_reason = {k: int(current_by_reason.get(k, 0)) - int(prior_reason.get(k, 0)) for k in keys}
    return {
        "available": True,
        "prior_review_queue_total": int(sum(prior_reason.values())),
        "current_review_queue_total": int(sum(current_by_reason.values())),
        "net_change_review_queue": int(sum(current_by_reason.values()) - sum(prior_reason.values())),
        "by_reason": dict(sorted(by_reason.items(), key=lambda kv: (-abs(kv[1]), kv[0]))),
    }


def build_workflow_gate_artifact(
    summary: dict,
    *,
    strict_release_requested: bool,
    bootstrapped_upstream: list[str],
) -> dict:
    """Single JSON shape for GitHub Actions upload (multimodal release gate)."""
    rc = summary["row_counts"]
    multimodal_tables = {
        k: int(v)
        for k, v in rc.items()
        if not k.startswith("val_") and not k.startswith("review_queue")
    }
    validation_tables = {k: int(v) for k, v in rc.items() if k.startswith("val_")}
    review_queues = {k: int(v) for k, v in rc.items() if k.startswith("review_queue")}
    blockers = {t: int(rc.get(t, 0)) for t in STRICT_BLOCKING_VALIDATION_TABLES}
    blocker_total = int(sum(blockers.values()))
    if strict_release_requested:
        strict_pass = blocker_total == 0 and not bootstrapped_upstream
    else:
        strict_pass = None
    body: dict = {
        "artifact_version": "multimodal_release_gate_v1",
        "github_sha": os.environ.get("GITHUB_SHA", ""),
        "github_run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "schema": summary["schema"],
        "contract_version": summary["contract_version"],
        "multimodal_tables": multimodal_tables,
        "validation_tables": validation_tables,
        "review_queues": review_queues,
        "strict_release": {
            "requested": strict_release_requested,
            "pass": strict_pass,
            "blocking_row_counts": blockers,
            "blocker_total": blocker_total,
            "bootstrapped_upstream": list(bootstrapped_upstream),
        },
    }
    rvm = summary.get("release_validation_metrics")
    if isinstance(rvm, dict):
        body["release_validation_metrics"] = rvm
    rqd = summary.get("review_queue_deltas")
    if isinstance(rqd, dict):
        body["review_queue_deltas"] = rqd
    return body


def emit_workflow_gate_artifact(
    path: Path,
    summary: dict,
    *,
    strict_release_requested: bool,
    bootstrapped_upstream: list[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = build_workflow_gate_artifact(
        summary,
        strict_release_requested=strict_release_requested,
        bootstrapped_upstream=bootstrapped_upstream,
    )
    path.write_text(json.dumps(body, indent=2), encoding="utf-8")
    print(f"\n  Wrote workflow gate artifact: {path}")


def summarize(con: duckdb.DuckDBPyConnection, schema: str) -> dict:
    tables = [
        "dim_patient_mm_v1",
        "map_patient_identifier_mm_v1",
        "fact_surgery_mm_v1",
        "dim_nodule_mm_v1",
        "fact_imaging_mm_v1",
        "fact_fna_mm_v1",
        "fact_genetics_mm_v1",
        "fact_path_report_mm_v1",
        "fact_tumor_mm_v1",
        "link_surgery_path_mm_v1",
        "link_surgery_context_mm_v1",
        "imaging_fna_linkage_mm_v1",
        "link_imaging_fna_mm_v1",
        "review_queue_imaging_fna_mm_v1",
        "val_imaging_fna_linkage_audit_v1",
        "val_contract_required_join_keys_mm_v1",
        "val_imaging_fna_contract_blockers_mm_v1",
        "val_nodes_invariant_mm_v1",
        "val_side_lobe_mismatch_mm_v1",
        "val_preop_temporal_order_mm_v1",
        "val_ambiguous_multimodal_linkage_mm_v1",
        "val_multitumor_expansion_mm_v1",
        "val_multi_tumor_report_mm_v1",
    ]
    out: dict = {"schema": schema, "contract_version": CONTRACT_VERSION, "row_counts": {}, "validation_fail_rows": {}}
    for t in tables:
        cnt = con.execute(f"SELECT COUNT(*) FROM {schema}.{t}").fetchone()[0]
        out["row_counts"][t] = cnt
        if t.startswith("val_") and t not in (
            "val_multi_tumor_report_mm_v1",
            "val_imaging_fna_linkage_audit_v1",
        ):
            out["validation_fail_rows"][t] = cnt
    out["release_validation_metrics"] = collect_release_validation_metrics(con, schema)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--md", action="store_true", help="Write to MotherDuck (schema mm_contract_dev only)")
    ap.add_argument(
        "--sa",
        action="store_true",
        help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN when using --md (CI / release automation)",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print schema and exit")
    ap.add_argument(
        "--strict-release",
        action="store_true",
        help=(
            "Fail closed: required column schema + no bootstrap, zero-row blocking val_* tables "
            "(see docs/multimodal_release_gate.md)."
        ),
    )
    ap.add_argument(
        "--allow-bootstrap-dev",
        action="store_true",
        help=(
            "Allow dev-only upstream stubs in mm_contract_dev (script 49 fragments, empty FNA, etc.). "
            "Do not use for CI/release."
        ),
    )
    ap.add_argument(
        "--emit-ci-artifact",
        type=Path,
        metavar="PATH",
        default=None,
        help=(
            "Write multimodal_release_gate_v1 JSON for workflow upload (row counts + strict-release summary)."
        ),
    )
    ap.add_argument(
        "--prior-gate-artifact",
        type=Path,
        metavar="PATH",
        default=None,
        help=(
            "Optional prior multimodal_release_gate_v1 JSON; emits review_queue_deltas vs "
            "release_validation_metrics.review_queue_by_reason."
        ),
    )
    args = ap.parse_args()
    if args.strict_release and args.allow_bootstrap_dev:
        ap.error("--strict-release cannot be combined with --allow-bootstrap-dev")
    schema = resolve_schema(md=args.md)

    if args.dry_run:
        print(f"Schema: {schema} (md={args.md})")
        return

    if args.md:
        os.environ.setdefault("MOTHERDUCK_SESSION_HINT", "THYROID_2026")
        os.environ.setdefault(
            "MOTHERDUCK_CUSTOM_USER_AGENT",
            "THYROID_2026_molecular/128_multimodal_contract_mm_v1;kind=contract",
        )
        con = connect_md_or_file(
            DB_PATH,
            md=True,
            fail_closed=True,
            prefer_service_account=args.sa,
        )
    else:
        con = duckdb.connect(str(DB_PATH))

    try:
        bootstrapped = build_all(
            con,
            schema,
            allow_bootstrap_dev=args.allow_bootstrap_dev,
            strict_release=args.strict_release,
        )
        summary = summarize(con, schema)
        section("ROW COUNT SUMMARY")
        print(json.dumps(summary["row_counts"], indent=2))
        section("VALIDATION TABLE COUNTS (non-zero = investigate)")
        print(json.dumps(summary["validation_fail_rows"], indent=2))
        section("RELEASE VALIDATION METRICS (interpretive breakdowns)")
        print(json.dumps(summary.get("release_validation_metrics", {}), indent=2))
        prior_body = (
            load_prior_gate_artifact(args.prior_gate_artifact)
            if args.prior_gate_artifact
            else None
        )
        rvm = summary.get("release_validation_metrics") or {}
        rq_map = rvm.get("review_queue_by_reason") if isinstance(rvm, dict) else None
        if isinstance(rq_map, dict):
            summary["review_queue_deltas"] = compute_review_queue_deltas(
                {str(k): int(v) for k, v in rq_map.items()},
                prior_body,
            )
            section("REVIEW QUEUE DELTAS (vs --prior-gate-artifact when provided)")
            print(json.dumps(summary["review_queue_deltas"], indent=2))
        if bootstrapped:
            section("BOOTSTRAPPED UPSTREAM (native table missing; used schema stubs)")
            print(json.dumps(bootstrapped, indent=2))

        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        summary_path = EXPORT_DIR / f"mm_contract_summary_{ts}.json"
        gaps: dict = {
            "missing_upstream": [],
            "bootstrapped_upstream": bootstrapped,
            "strict_release": args.strict_release,
            "notes": [
                "With --strict-release, all blocking val_* tables in the contract doc must be empty.",
                "Without --allow-bootstrap-dev, native upstream tables must exist (fail-closed).",
            ],
        }
        summary["gaps"] = gaps
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"\n  Wrote {summary_path}")
        if args.emit_ci_artifact:
            emit_workflow_gate_artifact(
                args.emit_ci_artifact,
                summary,
                strict_release_requested=args.strict_release,
                bootstrapped_upstream=bootstrapped,
            )
        if args.strict_release:
            assert_strict_release_passes(con, schema, bootstrapped_upstream=bootstrapped)
    finally:
        con.close()


if __name__ == "__main__":
    main()
