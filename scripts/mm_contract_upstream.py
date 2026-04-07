"""Resolve multimodal contract upstream tables; bootstrap gaps in mm_contract_dev."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Callable

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


CORE_TABLES = [
    "operative_episode_detail_v2",
    "tumor_episode_master_v2",
    "molecular_test_episode_v2",
    "imaging_nodule_master_v1",
]

UPSTREAM_KEYS = [
    "linkage_master_v1",
    "mrn_crosswalk_v1",
    "operative_episode_detail_v2",
    "tumor_episode_master_v2",
    "fna_episode_master_v2",
    "molecular_test_episode_v2",
    "imaging_nodule_master_v1",
    "event_date_audit_v2",
    "patient_cross_domain_timeline_v2",
    "preop_surgery_linkage_v3",
    "surgery_pathology_linkage_v3",
    "fna_molecular_linkage_v3",
    "pathology_rai_linkage_v3",
]


def _load_script49():
    path = ROOT / "scripts" / "49_enhanced_linkage_v3.py"
    spec = importlib.util.spec_from_file_location("scr49", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def _substitute_sql(sql: str, src: dict[str, str]) -> str:
    out = sql
    for key in sorted(src.keys(), key=len, reverse=True):
        fq = src[key]
        if fq != key:
            out = out.replace(key, fq)
    return out


def _boot(schema: str, logical: str) -> str:
    return f"{schema}._bootstrap_{logical}"


def _bootstrap_linkage_identity(con: duckdb.DuckDBPyConnection, dest: str) -> None:
    con.execute(f"""
CREATE OR REPLACE TABLE {dest} AS
SELECT DISTINCT CAST(u.research_id AS BIGINT) AS research_id,
       CAST(u.research_id AS BIGINT) AS canonical_research_id,
       CAST(NULL AS VARCHAR) AS euh_mrn,
       'identity'::VARCHAR AS linkage_method,
       1.0::DOUBLE AS confidence
FROM (
  SELECT research_id FROM operative_episode_detail_v2
  UNION SELECT research_id FROM tumor_episode_master_v2
  UNION SELECT research_id FROM molecular_test_episode_v2
  UNION SELECT research_id FROM imaging_nodule_master_v1
) u
WHERE u.research_id IS NOT NULL;
""")


def _bootstrap_mrn_empty(con: duckdb.DuckDBPyConnection, dest: str) -> None:
    con.execute(f"""
CREATE OR REPLACE TABLE {dest} (
    research_id INTEGER,
    euh_mrn VARCHAR,
    tec_mrn VARCHAR,
    canonical_research_id INTEGER,
    linkage_method VARCHAR
);
""")


def _bootstrap_fna_empty(con: duckdb.DuckDBPyConnection, dest: str) -> None:
    con.execute(f"""
CREATE OR REPLACE TABLE {dest} AS
SELECT CAST(NULL AS INTEGER) AS research_id,
       CAST(NULL AS INTEGER) AS fna_episode_id,
       CAST(NULL AS DATE) AS fna_date_native,
       CAST(NULL AS DATE) AS resolved_fna_date,
       CAST(NULL AS INTEGER) AS bethesda_category,
       CAST(NULL AS VARCHAR) AS specimen_site_raw,
       CAST(NULL AS VARCHAR) AS laterality,
       CAST(NULL AS VARCHAR) AS pathology_diagnosis
WHERE FALSE;
""")


def _bootstrap_event_date_audit(con: duckdb.DuckDBPyConnection, dest: str, fna_rel: str) -> None:
    con.execute(f"""
CREATE OR REPLACE TABLE {dest} AS
SELECT 'tumor' AS domain, CAST(research_id AS INTEGER) AS research_id,
       CAST(surgery_date AS VARCHAR) AS native_date,
       CAST(surgery_date AS VARCHAR) AS resolved_date,
       date_status, date_confidence,
       histology_source AS anchor_source,
       source_tables AS source_table
FROM tumor_episode_master_v2
UNION ALL
SELECT 'molecular', CAST(research_id AS INTEGER),
       CAST(test_date_native AS VARCHAR), resolved_test_date,
       date_status, date_confidence,
       platform, COALESCE(source_table, 'molecular_test_episode_v2')
FROM molecular_test_episode_v2
UNION ALL
SELECT 'rai', CAST(research_id AS INTEGER),
       CAST(rai_date_native AS VARCHAR), CAST(resolved_rai_date AS VARCHAR),
       date_status, date_confidence,
       COALESCE(source_note_type, 'rai'), COALESCE(source_table, 'rai_treatment_episode_v2')
FROM rai_treatment_episode_v2
UNION ALL
SELECT 'imaging', CAST(research_id AS INTEGER),
       CAST(exam_date AS VARCHAR), CAST(exam_date AS VARCHAR),
       'exact_source_date'::VARCHAR, 100,
       'US'::VARCHAR, COALESCE(source_table, 'imaging_nodule_master_v1')
FROM imaging_nodule_master_v1
WHERE exam_date IS NOT NULL
UNION ALL
SELECT 'fna', CAST(research_id AS INTEGER),
       CAST(fna_date_native AS VARCHAR), CAST(resolved_fna_date AS VARCHAR),
       'exact_source_date'::VARCHAR, 100,
       'fna'::VARCHAR, 'fna_episode_master_v2'
FROM {fna_rel}
WHERE fna_date_native IS NOT NULL;
""")


def _bootstrap_patient_timeline(con: duckdb.DuckDBPyConnection, dest: str, fna_rel: str) -> None:
    con.execute(f"""
CREATE OR REPLACE TABLE {dest} AS
SELECT * FROM (
    SELECT research_id, 'surgery' AS event_type, 'tumor' AS domain,
           CAST(surgery_date AS DATE) AS event_date, surgery_episode_id AS episode_id,
           primary_histology AS event_detail
    FROM tumor_episode_master_v2
    UNION ALL
    SELECT research_id, 'molecular_test', 'molecular',
           test_date_native, molecular_episode_id,
           COALESCE(platform, '') || ': ' || COALESCE(overall_result_class, '')
    FROM molecular_test_episode_v2
    UNION ALL
    SELECT research_id, 'rai_treatment', 'rai',
           CAST(resolved_rai_date AS DATE), rai_episode_id,
           COALESCE(rai_assertion_status, '')
    FROM rai_treatment_episode_v2
    UNION ALL
    SELECT research_id, 'imaging', 'imaging',
           exam_date,
           ROW_NUMBER() OVER (PARTITION BY research_id, exam_date ORDER BY nodule_number),
           'us nodule'::VARCHAR
    FROM imaging_nodule_master_v1
    WHERE exam_date IS NOT NULL
    UNION ALL
    SELECT research_id, 'surgery', 'operative',
           surgery_date_native, surgery_episode_id,
           procedure_normalized
    FROM operative_episode_detail_v2
    UNION ALL
    SELECT research_id, 'fna', 'fna',
           fna_date_native, fna_episode_id,
           'Bethesda ' || COALESCE(CAST(bethesda_category AS VARCHAR), '?')
    FROM {fna_rel}
    WHERE fna_date_native IS NOT NULL
) u
ORDER BY u.research_id, u.event_date NULLS LAST, u.event_type;
""")


def _run_49_fragment(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    dest_short: str,
    sql_attr: str,
    src: dict[str, str],
) -> None:
    s49 = _load_script49()
    raw = getattr(s49, sql_attr)
    target = _boot(schema, dest_short)
    needle = f"CREATE OR REPLACE TABLE {dest_short} AS"
    if needle not in raw:
        raise RuntimeError(f"Expected {needle!r} in script 49 {sql_attr}")
    sql = raw.replace(needle, f"CREATE OR REPLACE TABLE {target} AS", 1)
    sql = _substitute_sql(sql, src)
    con.execute(sql)


def _empty_pathology_rai(con: duckdb.DuckDBPyConnection, dest: str) -> None:
    con.execute(f"""
CREATE OR REPLACE TABLE {dest} AS
SELECT CAST(NULL AS INTEGER) AS research_id,
       CAST(NULL AS INTEGER) AS surgery_episode_id,
       CAST(NULL AS INTEGER) AS rai_episode_id,
       CAST(NULL AS DATE) AS surgery_date,
       CAST(NULL AS DATE) AS rai_date,
       CAST(NULL AS INTEGER) AS days_post_surgery,
       CAST(NULL AS INTEGER) AS abs_days,
       CAST(NULL AS VARCHAR) AS rai_assertion_status,
       CAST(NULL AS DOUBLE) AS dose_mci,
       CAST(NULL AS INTEGER) AS n_candidates,
       CAST(NULL AS DOUBLE) AS linkage_score,
       CAST(NULL AS INTEGER) AS score_rank,
       CAST(NULL AS VARCHAR) AS linkage_confidence_tier,
       CAST(NULL AS VARCHAR) AS linkage_reason_summary,
       CAST(NULL AS BOOLEAN) AS analysis_eligible_link_flag
WHERE FALSE;
""")


def ensure_upstream_sources(
    con: duckdb.DuckDBPyConnection, schema: str, *, section: Callable[[str], None]
) -> dict[str, str]:
    for t in CORE_TABLES:
        if not table_available(con, t):
            raise RuntimeError(
                f"Missing core table {t!r} — cannot bootstrap contract upstreams."
            )

    src: dict[str, str] = {k: k for k in UPSTREAM_KEYS}

    if not table_available(con, "linkage_master_v1"):
        section("bootstrap linkage_master_v1 (identity spine)")
        d = _boot(schema, "linkage_master_v1")
        _bootstrap_linkage_identity(con, d)
        src["linkage_master_v1"] = d

    if not table_available(con, "mrn_crosswalk_v1"):
        section("bootstrap mrn_crosswalk_v1 (empty)")
        d = _boot(schema, "mrn_crosswalk_v1")
        _bootstrap_mrn_empty(con, d)
        src["mrn_crosswalk_v1"] = d

    if not table_available(con, "fna_episode_master_v2"):
        section("bootstrap fna_episode_master_v2 (empty)")
        d = _boot(schema, "fna_episode_master_v2")
        _bootstrap_fna_empty(con, d)
        src["fna_episode_master_v2"] = d

    fna_rel = src["fna_episode_master_v2"]

    if not table_available(con, "event_date_audit_v2"):
        section("bootstrap event_date_audit_v2 (script-22-style unions)")
        d = _boot(schema, "event_date_audit_v2")
        _bootstrap_event_date_audit(con, d, fna_rel)
        src["event_date_audit_v2"] = d

    if not table_available(con, "patient_cross_domain_timeline_v2"):
        section("bootstrap patient_cross_domain_timeline_v2")
        d = _boot(schema, "patient_cross_domain_timeline_v2")
        _bootstrap_patient_timeline(con, d, fna_rel)
        src["patient_cross_domain_timeline_v2"] = d

    if not table_available(con, "surgery_pathology_linkage_v3"):
        section("bootstrap surgery_pathology_linkage_v3 (script 49)")
        _run_49_fragment(con, schema, "surgery_pathology_linkage_v3", "LINK_SURGERY_PATHOLOGY_V3_SQL", src)
        src["surgery_pathology_linkage_v3"] = _boot(schema, "surgery_pathology_linkage_v3")

    if not table_available(con, "pathology_rai_linkage_v3"):
        d = _boot(schema, "pathology_rai_linkage_v3")
        if table_available(con, "rai_treatment_episode_v2"):
            section("bootstrap pathology_rai_linkage_v3 (script 49)")
            _run_49_fragment(con, schema, "pathology_rai_linkage_v3", "LINK_PATHOLOGY_RAI_V3_SQL", src)
        else:
            section("bootstrap pathology_rai_linkage_v3 (empty)")
            _empty_pathology_rai(con, d)
        src["pathology_rai_linkage_v3"] = d

    if not table_available(con, "preop_surgery_linkage_v3"):
        section("bootstrap preop_surgery_linkage_v3 (script 49)")
        _run_49_fragment(con, schema, "preop_surgery_linkage_v3", "LINK_PREOP_SURGERY_V3_SQL", src)
        src["preop_surgery_linkage_v3"] = _boot(schema, "preop_surgery_linkage_v3")

    if not table_available(con, "fna_molecular_linkage_v3"):
        section("bootstrap fna_molecular_linkage_v3 (script 49)")
        _run_49_fragment(con, schema, "fna_molecular_linkage_v3", "LINK_FNA_MOLECULAR_V3_SQL", src)
        src["fna_molecular_linkage_v3"] = _boot(schema, "fna_molecular_linkage_v3")

    return src
