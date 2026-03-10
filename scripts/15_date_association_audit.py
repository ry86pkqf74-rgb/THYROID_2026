#!/usr/bin/env python3
"""
15_date_association_audit.py — Date Association Enrichment (Phase 1 + Phase 2)

Phase 1: Creates enriched views with provenance columns for all note_entities
         tables, preserving original entity_date and note_date alongside new
         inferred columns. Uses deterministic date-aware joins (not evidence_span
         text matching) and episode-aware joins for tables with re-operations.

Phase 2: Creates reconciliation materialized views:
         - histology_reconciliation_mv
         - molecular_episode_mv
         - rai_episode_mv
         - validation_failures_mv

Modes:
  --local   : Uses local DuckDB with parquets from processed/ (default)
  --md      : Uses MotherDuck (requires MOTHERDUCK_TOKEN)

Run after 09b_motherduck_upload_notes_entities.py.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))

NOTE_ENTITY_TABLES = [
    "note_entities_genetics",
    "note_entities_staging",
    "note_entities_procedures",
    "note_entities_complications",
    "note_entities_medications",
    "note_entities_problem_list",
]

ANCHOR_TABLES = [
    "path_synoptics",
    "fna_history",
    "molecular_testing",
    "clinical_notes_long",
]


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def register_parquets(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Register all available parquets as tables. Returns list of registered names."""
    registered: list[str] = []
    all_tables = NOTE_ENTITY_TABLES + ANCHOR_TABLES
    for tbl in all_tables:
        pq = PROCESSED / f"{tbl}.parquet"
        if pq.exists():
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  Registered {tbl:<45} {cnt:>8,} rows")
            registered.append(tbl)
        else:
            print(f"  SKIP      {tbl:<45} (parquet not found)")
    return registered


def discover_genetics_date_col(con: duckdb.DuckDBPyConnection) -> str | None:
    """Find the actual date column in molecular_testing, if any usable values exist."""
    try:
        con.execute("SELECT 1 FROM molecular_testing LIMIT 1")
    except Exception:
        return None

    cols = con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'molecular_testing'
          AND (column_name ILIKE '%date%' OR column_name ILIKE '%year%'
            OR column_name ILIKE '%time%')
    """).fetchall()

    if not cols:
        return None

    for (col_name,) in cols:
        valid_ct = con.execute(f"""
            SELECT COUNT(*)
            FROM molecular_testing
            WHERE "{col_name}" IS NOT NULL
              AND CAST("{col_name}" AS VARCHAR) NOT IN ('x', 'X', '', 'None', 'maybe?')
              AND TRY_CAST("{col_name}" AS DATE) IS NOT NULL
        """).fetchone()[0]
        if valid_ct > 0:
            print(f"  genetics date col: '{col_name}' ({valid_ct} valid date values)")
            return col_name

    for (col_name,) in cols:
        year_ct = con.execute(f"""
            SELECT COUNT(*)
            FROM molecular_testing
            WHERE "{col_name}" IS NOT NULL
              AND regexp_matches(CAST("{col_name}" AS VARCHAR), '^[0-9]{{4}}$')
        """).fetchone()[0]
        if year_ct > 0:
            print(f"  genetics date col: '{col_name}' ({year_ct} year-only values)")
            return col_name

    print("  genetics date col: NONE usable")
    return None


def profile_null_rates(con: duckdb.DuckDBPyConnection, label: str) -> dict[str, dict]:
    """Profile entity_date and note_date null rates. Returns dict for comparison."""
    section(f"NULL-RATE PROFILING ({label})")
    header = (
        f"{'table':<42} {'total':>6}  {'ed_null':>7} {'ed_null%':>8}  "
        f"{'nd_null':>7}  {'recover_nd':>10}"
    )
    print(header)
    print("-" * len(header))

    results = {}
    for tbl in NOTE_ENTITY_TABLES:
        try:
            r = con.execute(f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN entity_date IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN note_date IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN entity_date IS NULL AND note_date IS NOT NULL THEN 1 ELSE 0 END),
                    ROUND(100.0 * SUM(CASE WHEN entity_date IS NULL THEN 1 ELSE 0 END)
                          / NULLIF(COUNT(*), 0), 1)
                FROM {tbl}
            """).fetchone()
            results[tbl] = {
                "total": r[0], "ed_null": r[1], "nd_null": r[2],
                "recover_nd": r[3], "ed_null_pct": r[4],
            }
            print(
                f"  {tbl:<42} {r[0]:>6}  {r[1]:>7} ({r[4]:>5.1f}%)  "
                f"{r[2]:>7}  {r[3]:>10}"
            )
        except Exception as e:
            print(f"  {tbl:<42} ERROR: {e}")
    return results


# ── Phase 1 SQL: Enriched entity views ──────────────────────────────────


PS_PRIMARY_CTE = """
ps_primary AS (
    SELECT
        research_id,
        TRY_CAST(surg_date AS DATE) AS surg_date_parsed,
        surg_date AS surg_date_raw,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST
        ) AS op_seq
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
)
"""

FNA_PRIMARY_CTE = """
fna_primary AS (
    SELECT
        research_id,
        fna_date_parsed,
        TRY_CAST(fna_date_parsed AS DATE) AS fna_date,
        fna_index,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY TRY_CAST(fna_date_parsed AS DATE) DESC NULLS LAST,
                     fna_index DESC
        ) AS fna_seq
    FROM fna_history
    WHERE fna_date_parsed IS NOT NULL AND fna_date_parsed != ''
)
"""


def mt_valid_cte(date_col: str) -> str:
    return f"""
mt_valid AS (
    SELECT
        research_id,
        CASE
            WHEN TRY_CAST("{date_col}" AS DATE) IS NOT NULL
                THEN TRY_CAST("{date_col}" AS DATE)
            WHEN regexp_matches(CAST("{date_col}" AS VARCHAR), '^\\d{{4}}$')
                THEN TRY_CAST(CAST("{date_col}" AS VARCHAR) || '-01-01' AS DATE)
            ELSE NULL
        END AS mt_date_parsed,
        CASE
            WHEN regexp_matches(CAST("{date_col}" AS VARCHAR), '^\\d{{4}}$')
                THEN 'year'
            ELSE 'day'
        END AS mt_date_granularity,
        "{date_col}" AS mt_date_raw,
        test_index,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY
                CASE WHEN TRY_CAST("{date_col}" AS DATE) IS NOT NULL THEN 0 ELSE 1 END,
                TRY_CAST("{date_col}" AS DATE) DESC NULLS LAST,
                test_index DESC
        ) AS mt_seq
    FROM molecular_testing
    WHERE "{date_col}" IS NOT NULL
      AND CAST("{date_col}" AS VARCHAR) NOT IN ('x', 'X', '', 'None', 'maybe?')
)
"""


def build_enriched_view_sql(
    domain: str,
    has_path_synoptics: bool,
    has_fna_history: bool,
    has_molecular_testing: bool,
    genetics_date_col: str | None,
) -> str:
    """Build CREATE VIEW SQL for an enriched note_entities view."""
    tbl = f"note_entities_{domain}"
    view_name = f"enriched_{tbl}"

    use_genetics = domain == "genetics" and has_molecular_testing and genetics_date_col
    use_fna = domain in ("genetics",) and has_fna_history
    use_surgery = domain in (
        "genetics", "staging", "procedures", "complications",
    ) and has_path_synoptics

    cte_parts = []
    if use_surgery:
        cte_parts.append(PS_PRIMARY_CTE.strip())
    if use_fna:
        cte_parts.append(FNA_PRIMARY_CTE.strip())
    if use_genetics and genetics_date_col:
        cte_parts.append(mt_valid_cte(genetics_date_col).strip())

    cte_block = ""
    if cte_parts:
        cte_block = "WITH " + ",\n".join(cte_parts)

    join_clauses = []
    if use_surgery:
        join_clauses.append(
            "LEFT JOIN ps_primary ps\n"
            "    ON CAST(e.research_id AS BIGINT) = ps.research_id AND ps.op_seq = 1"
        )
    if use_genetics and genetics_date_col:
        join_clauses.append(
            "LEFT JOIN mt_valid mt\n"
            "    ON CAST(e.research_id AS BIGINT) = mt.research_id AND mt.mt_seq = 1"
        )
    if use_fna:
        join_clauses.append(
            "LEFT JOIN fna_primary fna\n"
            "    ON CAST(e.research_id AS BIGINT) = fna.research_id AND fna.fna_seq = 1"
        )

    coalesce_parts = ["TRY_CAST(e.entity_date AS DATE)"]
    case_parts = [
        "WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL\n"
        "            THEN 'entity_date'"
    ]
    case_anchor_type = [
        "WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL\n"
        "            THEN 'extracted'"
    ]
    case_anchor_table = [
        "WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL\n"
        f"            THEN '{tbl}'"
    ]
    case_granularity = [
        "WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL\n"
        "            THEN 'day'"
    ]
    case_confidence = [
        "WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL\n"
        "            THEN 100"
    ]

    coalesce_parts.append("TRY_CAST(e.note_date AS DATE)")
    case_parts.append(
        "WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL\n"
        "            THEN 'note_date'"
    )
    case_anchor_type.append(
        "WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL\n"
        "            THEN 'encounter'"
    )
    case_anchor_table.append(
        "WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL\n"
        "            THEN 'clinical_notes_long'"
    )
    case_granularity.append(
        "WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL\n"
        "            THEN 'day'"
    )
    case_confidence.append(
        "WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL\n"
        "            THEN 70"
    )

    if use_surgery:
        coalesce_parts.append("ps.surg_date_parsed")
        case_parts.append(
            "WHEN ps.surg_date_parsed IS NOT NULL THEN 'surg_date'"
        )
        case_anchor_type.append(
            "WHEN ps.surg_date_parsed IS NOT NULL THEN 'surgical'"
        )
        case_anchor_table.append(
            "WHEN ps.surg_date_parsed IS NOT NULL THEN 'path_synoptics'"
        )
        case_granularity.append(
            "WHEN ps.surg_date_parsed IS NOT NULL THEN 'day'"
        )
        case_confidence.append(
            "WHEN ps.surg_date_parsed IS NOT NULL THEN 60"
        )

    if use_genetics and genetics_date_col:
        coalesce_parts.append("mt.mt_date_parsed")
        case_parts.append(
            "WHEN mt.mt_date_parsed IS NOT NULL THEN 'molecular_testing_date'"
        )
        case_anchor_type.append(
            "WHEN mt.mt_date_parsed IS NOT NULL THEN 'molecular'"
        )
        case_anchor_table.append(
            "WHEN mt.mt_date_parsed IS NOT NULL THEN 'molecular_testing'"
        )
        case_granularity.append(
            "WHEN mt.mt_date_parsed IS NOT NULL THEN mt.mt_date_granularity"
        )
        case_confidence.append(
            "WHEN mt.mt_date_parsed IS NOT NULL\n"
            "            THEN CASE WHEN mt.mt_date_granularity = 'year' THEN 50 ELSE 60 END"
        )

    if use_fna:
        coalesce_parts.append("fna.fna_date")
        case_parts.append(
            "WHEN fna.fna_date IS NOT NULL THEN 'fna_date_parsed'"
        )
        case_anchor_type.append(
            "WHEN fna.fna_date IS NOT NULL THEN 'cytology'"
        )
        case_anchor_table.append(
            "WHEN fna.fna_date IS NOT NULL THEN 'fna_history'"
        )
        case_granularity.append(
            "WHEN fna.fna_date IS NOT NULL THEN 'day'"
        )
        case_confidence.append(
            "WHEN fna.fna_date IS NOT NULL THEN 55"
        )

    coalesce_expr = ",\n            ".join(coalesce_parts)
    source_case = "\n            ".join(f"WHEN {c}" if not c.startswith("WHEN") else c for c in case_parts)
    anchor_type_case = "\n            ".join(f"WHEN {c}" if not c.startswith("WHEN") else c for c in case_anchor_type)
    anchor_table_case = "\n            ".join(f"WHEN {c}" if not c.startswith("WHEN") else c for c in case_anchor_table)
    granularity_case = "\n            ".join(f"WHEN {c}" if not c.startswith("WHEN") else c for c in case_granularity)
    confidence_case = "\n            ".join(f"WHEN {c}" if not c.startswith("WHEN") else c for c in case_confidence)

    join_block = "\n".join(join_clauses)

    sql = f"""CREATE OR REPLACE VIEW {view_name} AS
{cte_block}
SELECT
    e.*,
    COALESCE(
            {coalesce_expr}
    ) AS inferred_event_date,
    CASE
            {source_case}
            ELSE 'unrecoverable'
    END AS date_source,
    CASE
            {granularity_case}
            ELSE NULL
    END AS date_granularity,
    CASE
            {confidence_case}
            ELSE 0
    END AS date_confidence,
    CASE
            {anchor_type_case}
            ELSE 'none'
    END AS date_anchor_type,
    CASE
            {anchor_table_case}
            ELSE 'none'
    END AS date_anchor_table
FROM {tbl} e
{join_block}
"""
    return sql.strip() + ";\n"


# ── Phase 2 SQL: Reconciliation materialized views ──────────────────────


HISTOLOGY_RECONCILIATION_SQL = """
CREATE OR REPLACE VIEW histology_reconciliation_mv AS
WITH ps_tumors AS (
    SELECT
        ps.research_id,
        TRY_CAST(ps.surg_date AS DATE) AS surg_date,
        ps.tumor_1_histologic_type,
        ps.tumor_1_variant,
        ps.tumor_1_extrathyroidal_extension,
        ps.tumor_1_margin_status,
        ps.tumor_1_pt,
        ps.tumor_1_pn,
        ps.tumor_1_pm,
        ps.tumor_1_size_greatest_dimension_cm,
        ps.tumor_1_ln_involved,
        ps.tumor_1_ln_examined,
        ps.tumor_1_angioinvasion,
        ps.tumor_1_lymphatic_invasion,
        ps.tumor_1_perineural_invasion,
        ps.tumor_1_capsular_invasion,
        ps.reop,
        ROW_NUMBER() OVER (
            PARTITION BY ps.research_id
            ORDER BY TRY_CAST(ps.surg_date AS DATE) ASC NULLS LAST
        ) AS op_seq
    FROM path_synoptics ps
    WHERE ps.surg_date IS NOT NULL AND ps.surg_date != ''
),
staging_entities AS (
    SELECT
        e.research_id,
        e.entity_type,
        e.entity_value_norm,
        e.present_or_negated,
        e.confidence,
        TRY_CAST(COALESCE(e.entity_date, e.note_date) AS DATE) AS entity_resolved_date
    FROM note_entities_staging e
    WHERE e.present_or_negated = 'present'
)
SELECT
    ps.research_id,
    ps.surg_date,
    ps.op_seq,
    ps.tumor_1_histologic_type,
    ps.tumor_1_variant,
    ps.tumor_1_extrathyroidal_extension,
    ps.tumor_1_margin_status,
    ps.tumor_1_pt,
    ps.tumor_1_pn,
    ps.tumor_1_pm,
    ps.tumor_1_size_greatest_dimension_cm,
    ps.tumor_1_ln_involved,
    ps.tumor_1_ln_examined,
    ps.tumor_1_angioinvasion,
    ps.tumor_1_lymphatic_invasion,
    ps.tumor_1_perineural_invasion,
    ps.tumor_1_capsular_invasion,
    ps.reop,
    se_t.entity_value_norm AS note_t_stage,
    se_n.entity_value_norm AS note_n_stage,
    se_m.entity_value_norm AS note_m_stage,
    se_ov.entity_value_norm AS note_overall_stage,
    CASE
        WHEN ps.tumor_1_pt IS NOT NULL AND se_t.entity_value_norm IS NOT NULL
             AND LOWER(REPLACE(ps.tumor_1_pt, ' ', '')) != LOWER(REPLACE(se_t.entity_value_norm, ' ', ''))
            THEN TRUE ELSE FALSE
    END AS t_stage_discordant,
    CASE
        WHEN ps.tumor_1_pn IS NOT NULL AND se_n.entity_value_norm IS NOT NULL
             AND LOWER(REPLACE(ps.tumor_1_pn, ' ', '')) != LOWER(REPLACE(se_n.entity_value_norm, ' ', ''))
            THEN TRUE ELSE FALSE
    END AS n_stage_discordant,
    CASE
        WHEN ps.tumor_1_pt IS NULL AND ps.tumor_1_pn IS NULL THEN 'path_missing'
        WHEN se_t.entity_value_norm IS NULL AND se_n.entity_value_norm IS NULL THEN 'notes_missing'
        ELSE 'both_present'
    END AS reconciliation_status
FROM ps_tumors ps
LEFT JOIN staging_entities se_t
    ON CAST(ps.research_id AS BIGINT) = CAST(se_t.research_id AS BIGINT)
    AND se_t.entity_type = 'T_stage'
LEFT JOIN staging_entities se_n
    ON CAST(ps.research_id AS BIGINT) = CAST(se_n.research_id AS BIGINT)
    AND se_n.entity_type = 'N_stage'
LEFT JOIN staging_entities se_m
    ON CAST(ps.research_id AS BIGINT) = CAST(se_m.research_id AS BIGINT)
    AND se_m.entity_type = 'M_stage'
LEFT JOIN staging_entities se_ov
    ON CAST(ps.research_id AS BIGINT) = CAST(se_ov.research_id AS BIGINT)
    AND se_ov.entity_type = 'overall_stage'
WHERE ps.op_seq = 1;
"""


def build_molecular_episode_sql(genetics_date_col: str | None) -> str:
    mt_date_expr = "NULL::DATE"
    mt_gran_expr = "'none'"

    if genetics_date_col:
        mt_date_expr = f"""CASE
            WHEN TRY_CAST(mt."{genetics_date_col}" AS DATE) IS NOT NULL
                THEN TRY_CAST(mt."{genetics_date_col}" AS DATE)
            WHEN regexp_matches(CAST(mt."{genetics_date_col}" AS VARCHAR), '^\\d{{4}}$')
                THEN TRY_CAST(CAST(mt."{genetics_date_col}" AS VARCHAR) || '-01-01' AS DATE)
            ELSE NULL
        END"""
        mt_gran_expr = f"""CASE
            WHEN regexp_matches(CAST(mt."{genetics_date_col}" AS VARCHAR), '^\\d{{4}}$')
                THEN 'year'
            WHEN TRY_CAST(mt."{genetics_date_col}" AS DATE) IS NOT NULL THEN 'day'
            ELSE 'none'
        END"""

    return f"""CREATE OR REPLACE VIEW molecular_episode_mv AS
WITH fna_dedup AS (
    SELECT
        research_id,
        TRY_CAST(fna_date_parsed AS DATE) AS fna_date,
        bethesda,
        fna_index,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, fna_index
            ORDER BY TRY_CAST(fna_date_parsed AS DATE) DESC NULLS LAST
        ) AS rn
    FROM fna_history
    WHERE fna_date_parsed IS NOT NULL AND fna_date_parsed != ''
),
genetics_notes AS (
    SELECT
        research_id,
        entity_value_norm AS gene,
        present_or_negated,
        confidence,
        entity_date,
        note_date,
        TRY_CAST(COALESCE(entity_date, note_date) AS DATE) AS resolved_date,
        evidence_span
    FROM note_entities_genetics
    WHERE present_or_negated = 'present'
),
mt_base AS (
    SELECT
        mt.research_id,
        mt.thyroseq_afirma,
        mt.genetic_test_performed,
        mt.genetic_test,
        mt.result,
        mt.mutation,
        mt.detailed_findings,
        mt.nodule_info,
        mt.fna_bethesda,
        mt.test_index,
        {mt_date_expr} AS mt_date,
        {mt_gran_expr} AS mt_date_granularity
    FROM molecular_testing mt
)
SELECT
    COALESCE(mt.research_id, gn.research_id) AS research_id,
    mt.thyroseq_afirma,
    mt.genetic_test_performed,
    mt.genetic_test,
    mt.result AS mt_result,
    mt.mutation AS mt_mutation,
    mt.detailed_findings,
    mt.nodule_info,
    mt.fna_bethesda AS mt_fna_bethesda,
    mt.test_index,
    mt.mt_date,
    mt.mt_date_granularity,
    gn.gene AS note_gene,
    gn.confidence AS note_gene_confidence,
    gn.resolved_date AS note_gene_date,
    fd.fna_date AS closest_fna_date,
    fd.bethesda AS fna_bethesda,
    fd.fna_index,
    COALESCE(mt.mt_date, gn.resolved_date, fd.fna_date) AS best_episode_date,
    CASE
        WHEN mt.mt_date IS NOT NULL THEN 'molecular_testing'
        WHEN gn.resolved_date IS NOT NULL THEN 'note_entities_genetics'
        WHEN fd.fna_date IS NOT NULL THEN 'fna_history'
        ELSE 'unresolved'
    END AS episode_date_source
FROM mt_base mt
FULL OUTER JOIN genetics_notes gn
    ON CAST(mt.research_id AS BIGINT) = CAST(gn.research_id AS BIGINT)
LEFT JOIN fna_dedup fd
    ON COALESCE(CAST(mt.research_id AS BIGINT), CAST(gn.research_id AS BIGINT)) = fd.research_id
    AND fd.rn = 1;
"""


RAI_EPISODE_SQL = """
CREATE OR REPLACE VIEW rai_episode_mv AS
WITH rai_meds AS (
    SELECT
        e.research_id,
        e.entity_value_norm,
        e.entity_value_raw,
        e.present_or_negated,
        e.confidence,
        e.entity_date,
        e.note_date,
        TRY_CAST(COALESCE(e.entity_date, e.note_date) AS DATE) AS resolved_date,
        e.evidence_span
    FROM note_entities_medications e
    WHERE e.present_or_negated = 'present'
      AND (
          LOWER(e.entity_value_norm) LIKE '%rai%'
          OR LOWER(e.entity_value_norm) LIKE '%radioactive%'
          OR LOWER(e.entity_value_norm) LIKE '%i-131%'
          OR LOWER(e.entity_value_norm) LIKE '%i131%'
          OR LOWER(e.entity_value_norm) LIKE '%iodine%'
          OR LOWER(e.entity_value_norm) LIKE '%thyrogen%'
      )
),
rai_procedures AS (
    SELECT
        e.research_id,
        e.entity_value_norm,
        e.entity_value_raw,
        e.present_or_negated,
        e.confidence,
        e.entity_date,
        e.note_date,
        TRY_CAST(COALESCE(e.entity_date, e.note_date) AS DATE) AS resolved_date,
        e.evidence_span
    FROM note_entities_procedures e
    WHERE e.present_or_negated = 'present'
      AND (
          LOWER(e.entity_value_norm) LIKE '%ablation%'
          OR LOWER(e.entity_value_raw) LIKE '%rai%'
          OR LOWER(e.entity_value_raw) LIKE '%radioactive%'
          OR LOWER(e.entity_value_raw) LIKE '%i-131%'
      )
),
rai_combined AS (
    SELECT research_id, entity_value_norm, entity_value_raw, present_or_negated,
           confidence, entity_date, note_date, resolved_date, evidence_span,
           'medications' AS source_domain
    FROM rai_meds
    UNION ALL
    SELECT research_id, entity_value_norm, entity_value_raw, present_or_negated,
           confidence, entity_date, note_date, resolved_date, evidence_span,
           'procedures' AS source_domain
    FROM rai_procedures
),
ps_primary AS (
    SELECT
        research_id,
        TRY_CAST(surg_date AS DATE) AS surg_date,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST
        ) AS op_seq
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
)
SELECT
    r.research_id,
    r.entity_value_norm AS rai_mention,
    r.entity_value_raw,
    r.source_domain,
    r.confidence,
    r.entity_date,
    r.note_date,
    r.resolved_date AS rai_resolved_date,
    ps.surg_date AS primary_surg_date,
    CASE
        WHEN r.resolved_date IS NOT NULL AND ps.surg_date IS NOT NULL
            THEN DATE_DIFF('day', ps.surg_date, r.resolved_date)
        ELSE NULL
    END AS days_surgery_to_rai,
    CASE
        WHEN r.resolved_date IS NULL THEN 'date_missing'
        WHEN ps.surg_date IS NULL THEN 'surgery_date_missing'
        WHEN DATE_DIFF('day', ps.surg_date, r.resolved_date) < 0 THEN 'pre_surgical'
        WHEN DATE_DIFF('day', ps.surg_date, r.resolved_date) <= 180 THEN 'within_6mo'
        WHEN DATE_DIFF('day', ps.surg_date, r.resolved_date) <= 365 THEN 'within_1yr'
        ELSE 'after_1yr'
    END AS rai_timing_category,
    r.evidence_span
FROM rai_combined r
LEFT JOIN ps_primary ps
    ON CAST(r.research_id AS BIGINT) = ps.research_id
    AND ps.op_seq = 1;
"""


VALIDATION_FAILURES_SQL = """
CREATE OR REPLACE VIEW validation_failures_mv AS
WITH all_enriched AS (
    SELECT 'genetics' AS domain, research_id, entity_date, note_date,
           inferred_event_date, date_source, date_confidence, date_anchor_type
    FROM enriched_note_entities_genetics
    UNION ALL
    SELECT 'staging', research_id, entity_date, note_date,
           inferred_event_date, date_source, date_confidence, date_anchor_type
    FROM enriched_note_entities_staging
    UNION ALL
    SELECT 'procedures', research_id, entity_date, note_date,
           inferred_event_date, date_source, date_confidence, date_anchor_type
    FROM enriched_note_entities_procedures
    UNION ALL
    SELECT 'complications', research_id, entity_date, note_date,
           inferred_event_date, date_source, date_confidence, date_anchor_type
    FROM enriched_note_entities_complications
    UNION ALL
    SELECT 'medications', research_id, entity_date, note_date,
           inferred_event_date, date_source, date_confidence, date_anchor_type
    FROM enriched_note_entities_medications
    UNION ALL
    SELECT 'problem_list', research_id, entity_date, note_date,
           inferred_event_date, date_source, date_confidence, date_anchor_type
    FROM enriched_note_entities_problem_list
)
SELECT
    domain,
    CASE
        WHEN inferred_event_date IS NULL THEN 'no_date_recoverable'
        WHEN inferred_event_date > CURRENT_DATE THEN 'future_date'
        WHEN inferred_event_date < DATE '1990-01-01' THEN 'implausible_past'
        WHEN entity_date IS NOT NULL AND note_date IS NOT NULL
             AND ABS(DATE_DIFF('day',
                     TRY_CAST(entity_date AS DATE),
                     TRY_CAST(note_date AS DATE))) > 365
            THEN 'entity_note_date_mismatch_gt_1yr'
        WHEN date_confidence < 50 THEN 'low_confidence'
        ELSE NULL
    END AS failure_type,
    research_id,
    entity_date,
    note_date,
    inferred_event_date,
    date_source,
    date_confidence,
    date_anchor_type
FROM all_enriched
WHERE
    inferred_event_date IS NULL
    OR inferred_event_date > CURRENT_DATE
    OR inferred_event_date < DATE '1990-01-01'
    OR (entity_date IS NOT NULL AND note_date IS NOT NULL
        AND ABS(DATE_DIFF('day',
                TRY_CAST(entity_date AS DATE),
                TRY_CAST(note_date AS DATE))) > 365)
    OR date_confidence < 50;
"""


# ── Phase 1+2 Audit and Summary Views ──────────────────────────────────


AUDIT_VIEW_SQL = """
CREATE OR REPLACE VIEW missing_date_associations_audit AS
SELECT 'genetics' AS entity_table, research_id, entity_date, note_date,
       inferred_event_date, date_source, date_granularity, date_confidence,
       date_anchor_type, date_anchor_table
FROM enriched_note_entities_genetics
UNION ALL
SELECT 'staging', research_id, entity_date, note_date,
       inferred_event_date, date_source, date_granularity, date_confidence,
       date_anchor_type, date_anchor_table
FROM enriched_note_entities_staging
UNION ALL
SELECT 'procedures', research_id, entity_date, note_date,
       inferred_event_date, date_source, date_granularity, date_confidence,
       date_anchor_type, date_anchor_table
FROM enriched_note_entities_procedures
UNION ALL
SELECT 'complications', research_id, entity_date, note_date,
       inferred_event_date, date_source, date_granularity, date_confidence,
       date_anchor_type, date_anchor_table
FROM enriched_note_entities_complications
UNION ALL
SELECT 'medications', research_id, entity_date, note_date,
       inferred_event_date, date_source, date_granularity, date_confidence,
       date_anchor_type, date_anchor_table
FROM enriched_note_entities_medications
UNION ALL
SELECT 'problem_list', research_id, entity_date, note_date,
       inferred_event_date, date_source, date_granularity, date_confidence,
       date_anchor_type, date_anchor_table
FROM enriched_note_entities_problem_list;
"""

RECOVERY_SUMMARY_SQL = """
CREATE OR REPLACE VIEW date_recovery_summary AS
SELECT
    entity_table,
    date_source,
    date_anchor_type,
    COUNT(*) AS rows_affected,
    COUNT(DISTINCT research_id) AS unique_patients,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY entity_table), 2)
        AS pct_of_table,
    ROUND(AVG(date_confidence), 1) AS avg_confidence
FROM missing_date_associations_audit
GROUP BY 1, 2, 3
ORDER BY entity_table, rows_affected DESC;
"""


def profile_enriched_rates(con: duckdb.DuckDBPyConnection) -> dict[str, dict]:
    """Profile inferred_event_date recovery rates from enriched views."""
    section("ENRICHED VIEW RECOVERY RATES (AFTER)")
    header = (
        f"{'table':<42} {'total':>6}  {'inferred_not_null':>17} "
        f"{'recovery%':>10}  {'still_null':>10}"
    )
    print(header)
    print("-" * len(header))

    results = {}
    for tbl in NOTE_ENTITY_TABLES:
        view = f"enriched_{tbl}"
        try:
            r = con.execute(f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN inferred_event_date IS NOT NULL THEN 1 ELSE 0 END),
                    ROUND(100.0 * SUM(CASE WHEN inferred_event_date IS NOT NULL THEN 1 ELSE 0 END)
                          / NULLIF(COUNT(*), 0), 1),
                    SUM(CASE WHEN inferred_event_date IS NULL THEN 1 ELSE 0 END)
                FROM {view}
            """).fetchone()
            results[tbl] = {
                "total": r[0], "inferred_not_null": r[1],
                "recovery_pct": r[2], "still_null": r[3],
            }
            print(
                f"  {tbl:<42} {r[0]:>6}  {r[1]:>17} "
                f"{r[2]:>9.1f}%  {r[3]:>10}"
            )
        except Exception as e:
            print(f"  {tbl:<42} ERROR: {e}")
    return results


def print_recovery_summary(con: duckdb.DuckDBPyConnection) -> None:
    """Print the date_recovery_summary view."""
    section("DATE RECOVERY SUMMARY BY SOURCE")
    try:
        rows = con.execute("""
            SELECT entity_table, date_source, date_anchor_type,
                   rows_affected, unique_patients, pct_of_table, avg_confidence
            FROM date_recovery_summary
        """).fetchall()
        header = (
            f"{'entity_table':<15} {'date_source':<25} {'anchor_type':<12} "
            f"{'rows':>7} {'patients':>9} {'pct%':>7} {'avg_conf':>9}"
        )
        print(header)
        print("-" * len(header))
        for r in rows:
            print(
                f"  {str(r[0]):<15} {str(r[1]):<25} {str(r[2]):<12} "
                f"{r[3]:>7} {r[4]:>9} {r[5]:>6.1f}% {r[6]:>8.1f}"
            )
    except Exception as e:
        print(f"  ERROR: {e}")


def print_phase2_summaries(con: duckdb.DuckDBPyConnection) -> None:
    """Print summaries for Phase 2 materialized views."""
    section("PHASE 2: HISTOLOGY RECONCILIATION SUMMARY")
    try:
        rows = con.execute("""
            SELECT
                reconciliation_status,
                COUNT(*) AS n,
                SUM(CASE WHEN t_stage_discordant THEN 1 ELSE 0 END) AS t_discord,
                SUM(CASE WHEN n_stage_discordant THEN 1 ELSE 0 END) AS n_discord
            FROM histology_reconciliation_mv
            GROUP BY 1 ORDER BY n DESC
        """).fetchall()
        print(f"  {'status':<20} {'count':>8} {'T_discord':>10} {'N_discord':>10}")
        print("  " + "-" * 50)
        for r in rows:
            print(f"  {str(r[0]):<20} {r[1]:>8} {r[2]:>10} {r[3]:>10}")
    except Exception as e:
        print(f"  ERROR: {e}")

    section("PHASE 2: MOLECULAR EPISODE SUMMARY")
    try:
        rows = con.execute("""
            SELECT
                episode_date_source,
                COUNT(*) AS n,
                COUNT(DISTINCT research_id) AS patients,
                SUM(CASE WHEN best_episode_date IS NOT NULL THEN 1 ELSE 0 END) AS dated
            FROM molecular_episode_mv
            GROUP BY 1 ORDER BY n DESC
        """).fetchall()
        print(f"  {'source':<25} {'rows':>8} {'patients':>10} {'dated':>8}")
        print("  " + "-" * 53)
        for r in rows:
            print(f"  {str(r[0]):<25} {r[1]:>8} {r[2]:>10} {r[3]:>8}")
    except Exception as e:
        print(f"  ERROR: {e}")

    section("PHASE 2: RAI EPISODE SUMMARY")
    try:
        rows = con.execute("""
            SELECT
                rai_timing_category,
                COUNT(*) AS n,
                COUNT(DISTINCT research_id) AS patients
            FROM rai_episode_mv
            GROUP BY 1 ORDER BY n DESC
        """).fetchall()
        print(f"  {'timing':<25} {'mentions':>10} {'patients':>10}")
        print("  " + "-" * 47)
        for r in rows:
            print(f"  {str(r[0]):<25} {r[1]:>10} {r[2]:>10}")
    except Exception as e:
        print(f"  ERROR: {e}")

    section("PHASE 2: VALIDATION FAILURES SUMMARY")
    try:
        rows = con.execute("""
            SELECT
                domain,
                failure_type,
                COUNT(*) AS n,
                COUNT(DISTINCT research_id) AS patients
            FROM validation_failures_mv
            GROUP BY 1, 2 ORDER BY n DESC
        """).fetchall()
        print(f"  {'domain':<15} {'failure_type':<35} {'rows':>7} {'patients':>9}")
        print("  " + "-" * 68)
        for r in rows:
            print(f"  {str(r[0]):<15} {str(r[1]):<35} {r[2]:>7} {r[3]:>9}")
    except Exception as e:
        print(f"  ERROR: {e}")


def print_remaining_gaps(con: duckdb.DuckDBPyConnection) -> None:
    """Print known remaining gaps for histology, RAI, genetics, timeline."""
    section("REMAINING GAPS")

    gap_queries = {
        "Histology (no path_synoptics linkage)": """
            SELECT COUNT(DISTINCT e.research_id) AS patients, COUNT(*) AS rows
            FROM enriched_note_entities_staging e
            WHERE e.date_source = 'unrecoverable'
        """,
        "RAI (no resolved date)": """
            SELECT COUNT(DISTINCT research_id) AS patients, COUNT(*) AS rows
            FROM rai_episode_mv WHERE rai_resolved_date IS NULL
        """,
        "Genetics (no date from any source)": """
            SELECT COUNT(DISTINCT e.research_id) AS patients, COUNT(*) AS rows
            FROM enriched_note_entities_genetics e
            WHERE e.date_source = 'unrecoverable'
        """,
        "Timeline (all entities still unrecoverable)": """
            SELECT COUNT(DISTINCT research_id) AS patients, COUNT(*) AS rows
            FROM missing_date_associations_audit
            WHERE date_source = 'unrecoverable'
        """,
    }
    for label, sql in gap_queries.items():
        try:
            r = con.execute(sql).fetchone()
            print(f"  {label:<50} patients={r[0]:>5}  rows={r[1]:>6}")
        except Exception as e:
            print(f"  {label:<50} ERROR: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Date Association Audit — Phase 1+2"
    )
    parser.add_argument(
        "--md", action="store_true",
        help="Use MotherDuck instead of local DuckDB"
    )
    args = parser.parse_args()

    print("=" * 80)
    print("  DATE ASSOCIATION ENRICHMENT — Phase 1 + Phase 2")
    print("  Mode: " + ("MotherDuck" if args.md else "Local DuckDB"))
    print("=" * 80)

    if args.md:
        from motherduck_client import MotherDuckClient
        client = MotherDuckClient()
        con = client.connect_rw()
        share_ro = "md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c"
        try:
            con.execute(f"ATTACH '{share_ro}' AS thyroid_share")
        except Exception:
            pass
    else:
        con = duckdb.connect(str(DB_PATH))

    # ── Register data ──
    section("REGISTERING DATA SOURCES")
    if args.md:
        available = []
        for tbl in NOTE_ENTITY_TABLES + ANCHOR_TABLES:
            try:
                con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
                cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                print(f"  Available {tbl:<45} {cnt:>8,} rows")
                available.append(tbl)
            except Exception:
                print(f"  MISSING   {tbl}")
    else:
        available = register_parquets(con)

    has_ps = "path_synoptics" in available
    has_fna = "fna_history" in available
    has_mt = "molecular_testing" in available

    # ── Discover genetics date column ──
    section("GENETICS DATE COLUMN DISCOVERY")
    genetics_date_col = discover_genetics_date_col(con) if has_mt else None

    # ── Phase 1a: Before profiling ──
    before = profile_null_rates(con, "BEFORE")

    # ── Phase 1b: Create enriched views ──
    section("CREATING ENRICHED VIEWS (Phase 1)")
    view_sql_log: list[tuple[str, str]] = []
    for domain in ["genetics", "staging", "procedures", "complications",
                    "medications", "problem_list"]:
        tbl = f"note_entities_{domain}"
        if tbl not in available:
            print(f"  SKIP {tbl} (not available)")
            continue
        sql = build_enriched_view_sql(
            domain, has_ps, has_fna, has_mt, genetics_date_col,
        )
        try:
            con.execute(sql)
            view_name = f"enriched_{tbl}"
            cnt = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
            print(f"  Created {view_name:<50} {cnt:>8,} rows")
            view_sql_log.append((view_name, sql))
        except Exception as e:
            print(f"  FAILED enriched_{tbl}: {e}")
            print(f"  SQL:\n{sql[:500]}")

    # ── Phase 1c: Audit + summary views ──
    section("CREATING AUDIT VIEWS (Phase 1)")
    for name, sql in [
        ("missing_date_associations_audit", AUDIT_VIEW_SQL),
        ("date_recovery_summary", RECOVERY_SUMMARY_SQL),
    ]:
        try:
            con.execute(sql)
            cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"  Created {name:<50} {cnt:>8,} rows")
            view_sql_log.append((name, sql))
        except Exception as e:
            print(f"  FAILED {name}: {e}")

    # ── Phase 1d: After profiling ──
    after = profile_enriched_rates(con)
    print_recovery_summary(con)

    # ── Before/After comparison ──
    section("BEFORE / AFTER COMPARISON")
    header = (
        f"{'table':<42} {'ed_null_before':>14} {'still_null_after':>16} "
        f"{'recovered':>10} {'recovery%':>10}"
    )
    print(header)
    print("-" * len(header))
    for tbl in NOTE_ENTITY_TABLES:
        if tbl in before and tbl in after:
            b = before[tbl]
            a = after[tbl]
            recovered = b["ed_null"] - a["still_null"]
            pct = 100.0 * recovered / b["ed_null"] if b["ed_null"] > 0 else 0
            print(
                f"  {tbl:<42} {b['ed_null']:>14,} {a['still_null']:>16,} "
                f"{recovered:>10,} {pct:>9.1f}%"
            )

    # ── Phase 2: Reconciliation views ──
    section("CREATING PHASE 2 VIEWS")
    phase2_views = [
        ("histology_reconciliation_mv", HISTOLOGY_RECONCILIATION_SQL),
        ("molecular_episode_mv", build_molecular_episode_sql(genetics_date_col)),
        ("rai_episode_mv", RAI_EPISODE_SQL),
        ("validation_failures_mv", VALIDATION_FAILURES_SQL),
    ]
    for name, sql in phase2_views:
        deps_ok = True
        if "path_synoptics" in sql and not has_ps:
            deps_ok = False
        if "fna_history" in sql and not has_fna:
            deps_ok = False
        if not deps_ok:
            print(f"  SKIP {name} (missing dependencies)")
            continue
        try:
            con.execute(sql)
            cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"  Created {name:<50} {cnt:>8,} rows")
            view_sql_log.append((name, sql))
        except Exception as e:
            print(f"  FAILED {name}: {e}")
            print(f"  SQL excerpt:\n{sql[:400]}")

    print_phase2_summaries(con)
    print_remaining_gaps(con)

    # ── Output SQL definitions ──
    section("SQL VIEW DEFINITIONS (for MotherDuck deployment)")
    sql_output_path = ROOT / "scripts" / "15_date_association_views.sql"
    with open(sql_output_path, "w") as f:
        f.write("-- Date Association Enrichment Views\n")
        f.write("-- Generated by 15_date_association_audit.py\n")
        f.write("-- Deploy to thyroid_research_2026 via: USE thyroid_research_2026;\n\n")
        for name, sql in view_sql_log:
            f.write(f"-- === {name} ===\n")
            f.write(sql.strip())
            f.write("\n\n")
    print(f"  SQL definitions saved to: {sql_output_path}")

    # ── Files changed summary ──
    section("FILES CHANGED")
    print(f"  scripts/15_date_association_audit.py  (this script)")
    print(f"  scripts/15_date_association_views.sql (generated SQL)")
    if not args.md:
        print(f"  {DB_PATH.name}  (local DuckDB with views)")

    con.close()
    print(f"\n{'=' * 80}")
    print("  DONE — Phase 1 + Phase 2 complete")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
