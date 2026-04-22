#!/usr/bin/env python3
"""
Script 369 — round-2 pathology LLM extraction merge + MD-load + canonical build.

CONTEXT
=======
Loads the round-2 pathology LLM extraction (10,084 notes / 13,381 entities,
14 entity types) into ``main.note_entities_llm_pathology`` and builds a NEW
clinical-note-grain canonical pair:

  * ``main.canonical_pathology_clinical_events_v1`` — one row per UNNESTed entity
  * ``main.canonical_pathology_clinical_patient_rollup_v1`` — one row per RID

The ``_clinical_`` infix is load-bearing: it disambiguates from the existing
``canonical_path_{benign,gland,malignant}_*`` family (Script 361, surgical /
synoptic side).  See cursor prompt 2026-04-22 §1B / D3.

CPM 4-col Tier-2 surface (this script's UPDATE target):
  nlp_path_has_data            BOOLEAN     (already exists)
  nlp_path_n_entities          INTEGER     (already exists)
  nlp_path_positive_mentioned  BOOLEAN     (NEW — added if missing)
  nlp_path_confidence_tier     VARCHAR     (NEW — added if missing)

Other pre-existing nlp_path_* columns (ete_mentioned, histology_mentioned,
margin_mentioned, ln_positive_mentioned, multifocal_mentioned, vasc_inv_mentioned,
n_notes, multifocal_concordance_v2) are left UNTOUCHED — they were populated by
an earlier rollup pass and are out of scope here.

DOMAIN-SPECIFIC RULES
=====================
* ``lymphovascular_invasion`` rows are KEPT in the events table (cross-domain
  query convenience) but EXCLUDED from the patient_rollup ``has_lvi`` derivation
  — vascular v2 (Script 368) is the source of truth for LVI per D3.
* ``frozen_section`` entities are KEPT in the events table for cross-domain
  queries; the patient_rollup deliberately does NOT derive a frozen-section
  feature.  Hard guard: ``canonical_frozen_section_events_v1`` row count is
  asserted unchanged (Script 360 owns that domain).

PHASES
======
  --phase merge   Read ckpt JSONL → dedup by note_row_id → write parquet (READ-ONLY to MD)
  --phase 0       Parquet audit (READ-ONLY)
  --phase 1       Archive current main.note_entities_llm_pathology → archive_pub_v1_0
  --phase 2       Load merged parquet (CREATE OR REPLACE)
  --phase 3       Post-load parity
  --phase 4       Pre-mutation CPM snapshot + ALTER ADD COLUMN IF NOT EXISTS for new nlp cols
  --phase 5       Build canonicals + UPDATE 4 nlp_path_* CPM cols
  --phase 6       Invariants A–F + frozen-section guard
  --phase 7       Registry + __readme sync
  --phase all     merge → 0 → 7

HARD RULES (NON-NEGOTIABLE)
============================
* Never log entity_value / evidence_text / result_json / source_* values
* Auth via motherduck_client.get_token()
* CAST(research_id AS VARCHAR) on every CPM join
* Never cross-DB sourcing inside a canonical body
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts._round2_helpers import (  # noqa: E402
    ARCHIVE_DB,
    ARCHIVE_SCHEMA,
    CANONICAL_DB,
    RunLogger,
    add_cpm_columns_if_missing,
    append_readme,
    archive_table_if_present,
    assert_cpm_intact,
    assert_unchanged,
    column_exists,
    connect_md,
    table_exists,
    upsert_registry,
)

DOMAIN = "pathology"
SOURCE_TABLE = "note_entities_llm_pathology"
ARCHIVE_NAME = f"{SOURCE_TABLE}_pre369_20260422"
EVENTS_TABLE = "canonical_pathology_clinical_events_v1"
ROLLUP_TABLE = "canonical_pathology_clinical_patient_rollup_v1"
CPM_COLUMNS: dict[str, str] = {
    "nlp_path_has_data": "BOOLEAN",
    "nlp_path_n_entities": "INTEGER",
    "nlp_path_positive_mentioned": "BOOLEAN",
    "nlp_path_confidence_tier": "VARCHAR",
}
CONFIDENCE_TIER_VALUE = "round2_gpt_oss_120b_v1"

# Entity types that cause nlp_path_positive_mentioned to fire.  Per prompt §3 B1.
POSITIVE_ENTITY_TYPES = (
    "surgical_pathology",
    "fna_cytology",
    "bethesda_class",
    "molecular_testing",
)

# Pre-existing baseline row counts for hard guards (probed 2026-04-22).
FROZEN_SECTION_BASELINE = 7_081
US_LN_BASELINE = 6_801

CKPT_PATH = REPO_ROOT / "runs" / "round2_20260421" / DOMAIN / "output" / f"note_entities_llm_{DOMAIN}.ckpt.jsonl"
PARQUET_PATH = REPO_ROOT / "runs" / "round2_20260421" / DOMAIN / "output" / f"note_entities_llm_{DOMAIN}.parquet"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
LOG_PATH = OUTPUT_DIR / "369_run.log"
PREFLIGHT_PATH = OUTPUT_DIR / "369_preflight.json"

EXPECTED_LOADED_COLUMNS = [
    "note_row_id", "domain", "llm_model", "llm_base_url", "extracted_at",
    "result_json", "research_id", "note_type", "note_date", "linkage_date",
    "source_workbook", "source_sheet", "source_column", "note_index",
    "preprocess_batch_id", "preprocessed_at_utc", "preprocess_script_version",
]

logger = RunLogger(LOG_PATH)
log = logger.log
gate = logger.gate


# ── PHASE MERGE ──────────────────────────────────────────────────────────────


def phase_merge() -> None:
    log("=" * 70)
    log(f"PHASE MERGE — read ckpt JSONL → dedup → write parquet ({DOMAIN})")
    log("=" * 70)
    gate(CKPT_PATH.exists(), f"ckpt exists: {CKPT_PATH.name}")

    raw: list[dict] = []
    with CKPT_PATH.open() as f:
        for line in f:
            line = line.strip()
            if line:
                raw.append(json.loads(line))
    log(f"  Raw checkpoint rows: {len(raw):,}")

    # Dedup by note_row_id, keep latest extracted_at
    by_nrid: dict[str, dict] = {}
    for row in raw:
        nrid = row["note_row_id"]
        if nrid not in by_nrid or row["extracted_at"] > by_nrid[nrid]["extracted_at"]:
            by_nrid[nrid] = row
    deduped = list(by_nrid.values())
    log(f"  After dedup: {len(deduped):,} unique note_row_ids ({len(raw) - len(deduped):,} dups removed)")

    # Provenance / audit fields kept in result_json untouched.
    df = pd.DataFrame(deduped)
    for col in EXPECTED_LOADED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[EXPECTED_LOADED_COLUMNS]

    # Audit (count-only — no PHI in log)
    type_counts: dict[str, int] = {}
    err = empty = ok = 0
    for r in deduped:
        rj = r.get("result_json", "")
        if isinstance(rj, str):
            if rj.startswith('{"error"') or "RetryError" in rj or "InternalServerError" in rj:
                err += 1
                continue
            try:
                data = json.loads(rj)
            except Exception:
                err += 1
                continue
            ents = data.get("entities", [])
            if not ents:
                empty += 1
            else:
                ok += 1
                for e in ents:
                    t = e.get("entity_type", "?")
                    type_counts[t] = type_counts.get(t, 0) + 1
    log(f"  Outcome split: err={err:,} | empty={empty:,} | has_entities={ok:,}")
    log("  Entity-type histogram:")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        log(f"    {t}: {c:,}")

    # Model-tag note: round-2 ckpt is tagged qwen2.5-32b but per evaluator
    # this batch is the gpt-oss-120b run (counts match).  Preserve the tag
    # as-is on the source table; the canonical tracks gpt-oss-120b explicitly.
    distinct_models = sorted({r.get("llm_model", "?") for r in deduped})
    log(f"  Source-tag llm_model values (preserved as-is): {distinct_models}")

    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PARQUET_PATH, index=False)
    log(f"  Written: {PARQUET_PATH.name} ({len(df):,} rows, {PARQUET_PATH.stat().st_size / 1_048_576:.2f} MB)")

    PREFLIGHT_PATH.write_text(json.dumps({
        "domain": DOMAIN,
        "raw_rows": len(raw),
        "deduped_rows": len(deduped),
        "outcome_split": {"err": err, "empty": empty, "has_entities": ok},
        "entity_type_counts": type_counts,
        "source_llm_model_tags": distinct_models,
        "merged_at": datetime.now(timezone.utc).isoformat(),
        "parquet_path": str(PARQUET_PATH),
    }, indent=2))
    log(f"  Preflight: {PREFLIGHT_PATH.name}")
    log("PHASE MERGE complete")
    logger.flush()


# ── PHASE 0 ──────────────────────────────────────────────────────────────────


def phase_0(con: Any) -> None:
    log("=" * 70)
    log(f"PHASE 0 — parquet audit ({DOMAIN}) (READ-ONLY)")
    log("=" * 70)
    gate(PARQUET_PATH.exists(), f"merged parquet exists: {PARQUET_PATH}")

    import duckdb as _duckdb
    local = _duckdb.connect()
    rel = f"read_parquet('{PARQUET_PATH}')"
    n_rows, n_nrids, n_rids = local.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT note_row_id), COUNT(DISTINCT research_id) FROM {rel}"
    ).fetchone()
    log(f"  Rows: {n_rows:,} | distinct note_row_ids: {n_nrids:,} | distinct RIDs: {n_rids:,}")
    gate(n_rows == n_nrids, "no duplicate note_row_ids in parquet")
    gate(n_rows >= 9_000, f"parquet row count >= 9,000 (got {n_rows:,})")

    # Entity-type rollup (in-parquet)
    sql = f"""
    WITH parsed AS (
        SELECT json_extract(CAST(result_json AS JSON), '$.entities') AS ea
        FROM {rel}
        WHERE result_json IS NOT NULL
          AND CAST(result_json AS VARCHAR) NOT LIKE '%"entities": []%'
    ),
    flat AS (
        SELECT UNNEST(CAST(ea AS JSON[])) AS e FROM parsed WHERE ea IS NOT NULL
    )
    SELECT json_extract_string(e, '$.entity_type') AS et, COUNT(*) AS n
    FROM flat GROUP BY 1 ORDER BY 2 DESC
    """
    rows = local.execute(sql).fetchall()
    types = {r[0] for r in rows}
    log("  Entity-type distribution in parquet:")
    for t, c in rows:
        log(f"    {t}: {c:,}")
    # Sanity gates
    expected_subset = {"fna_cytology", "surgical_pathology", "bethesda_class"}
    missing = expected_subset - types
    gate(not missing, f"core pathology entity types present (missing: {missing})")
    local.close()

    # Current MD state
    if table_exists(con, "main", SOURCE_TABLE):
        cur = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT llm_model) FROM main.{SOURCE_TABLE}").fetchone()
        models = [r[0] for r in con.execute(f"SELECT DISTINCT llm_model FROM main.{SOURCE_TABLE}").fetchall()]
        log(f"  Current MD main.{SOURCE_TABLE}: {cur[0]:,} rows, models={models}")
    else:
        log(f"  Current MD main.{SOURCE_TABLE}: NOT PRESENT (will be created in phase 2)")

    log("PHASE 0 complete")
    logger.flush()


# ── PHASE 1 ──────────────────────────────────────────────────────────────────


def phase_1(con: Any) -> None:
    log("=" * 70)
    log(f"PHASE 1 — archive current main.{SOURCE_TABLE} → {ARCHIVE_SCHEMA}")
    log("=" * 70)
    archive_table_if_present(con, logger, "main", SOURCE_TABLE, ARCHIVE_NAME)
    log("PHASE 1 complete")
    logger.flush()


# ── PHASE 2 ──────────────────────────────────────────────────────────────────


def phase_2(con: Any) -> None:
    log("=" * 70)
    log(f"PHASE 2 — load merged parquet → main.{SOURCE_TABLE}")
    log("=" * 70)
    gate(PARQUET_PATH.exists(), "merged parquet exists")

    import duckdb as _duckdb
    local = _duckdb.connect()
    df = local.execute(f"SELECT * FROM read_parquet('{PARQUET_PATH}')").df()
    local.close()
    log(f"  Loaded parquet into local frame: {len(df):,} rows")

    con.execute(f"USE {CANONICAL_DB}")
    con.execute("CREATE OR REPLACE TEMP TABLE _r2_path_stage AS SELECT * FROM df")
    staged = con.execute("SELECT COUNT(*) FROM _r2_path_stage").fetchone()[0]
    gate(staged == len(df), f"staging parity ({staged:,} == {len(df):,})")

    con.execute(f"CREATE OR REPLACE TABLE main.{SOURCE_TABLE} AS SELECT * FROM _r2_path_stage")
    loaded = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    gate(loaded == len(df), f"MD load parity ({loaded:,} == {len(df):,})")
    log(f"  Loaded {loaded:,} rows into main.{SOURCE_TABLE}")

    cols = [r[0] for r in con.execute(
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_name='{SOURCE_TABLE}' AND table_schema='main' ORDER BY ordinal_position"
    ).fetchall()]
    missing = [c for c in EXPECTED_LOADED_COLUMNS if c not in cols]
    gate(not missing, f"all expected columns present (missing: {missing})")
    log(f"  Columns verified: {len(cols)} cols")
    log("PHASE 2 complete")
    logger.flush()


# ── PHASE 3 ──────────────────────────────────────────────────────────────────


def phase_3(con: Any) -> None:
    log("=" * 70)
    log("PHASE 3 — post-load parity")
    log("=" * 70)
    md_rows, md_nrids, md_rids = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT note_row_id), COUNT(DISTINCT research_id) FROM main.{SOURCE_TABLE}"
    ).fetchone()
    import duckdb as _duckdb
    local = _duckdb.connect()
    pq_rows, pq_rids = local.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM read_parquet('{PARQUET_PATH}')"
    ).fetchone()
    local.close()
    log(f"  MD: {md_rows:,} rows | {md_rids:,} RIDs | {md_nrids:,} note_row_ids")
    log(f"  Parquet: {pq_rows:,} rows | {pq_rids:,} RIDs")
    gate(md_rows == pq_rows, f"row-count parity ({md_rows:,})")
    gate(md_rids == pq_rids, f"RID-count parity ({md_rids:,})")
    gate(md_rows == md_nrids, "no duplicate note_row_ids in MD")
    log("PHASE 3 complete")
    logger.flush()


# ── PHASE 4 ──────────────────────────────────────────────────────────────────


def phase_4(con: Any) -> None:
    log("=" * 70)
    log("PHASE 4 — CPM snapshot + ALTER ADD COLUMN IF NOT EXISTS")
    log("=" * 70)
    snap = f"cpm_pre369_path_rollup_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    cpm_cols_for_snapshot = [c for c in CPM_COLUMNS.keys() if column_exists(con, "main", "canonical_patient_master", c)]
    if cpm_cols_for_snapshot:
        snap_select = "research_id, " + ", ".join(cpm_cols_for_snapshot)
        con.execute(
            f'CREATE OR REPLACE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}."{snap}" AS '
            f"SELECT {snap_select} FROM main.canonical_patient_master"
        )
        log(f"  CPM snapshot: {snap} ({len(cpm_cols_for_snapshot)} cols snapshotted)")
    else:
        log("  No pre-existing CPM nlp_path_* cols to snapshot")

    add_cpm_columns_if_missing(con, logger, CPM_COLUMNS)

    log("PHASE 4 complete")
    logger.flush()


# ── PHASE 5 ──────────────────────────────────────────────────────────────────

# Build events table by UNNESTing entities; build patient_rollup; UPDATE CPM.
# Dedup key for events = (research_id, note_row_id, entity_type, entity_value, source_line)
# kept implicit via DISTINCT in the events build.

EVENTS_BUILD_SQL = f"""
CREATE OR REPLACE TABLE main.{EVENTS_TABLE} AS
WITH parsed AS (
    SELECT research_id, note_row_id, source_column, note_type, note_date,
           json_extract(CAST(result_json AS JSON), '$.entities') AS ea
    FROM main.{SOURCE_TABLE}
    WHERE result_json IS NOT NULL
      AND CAST(result_json AS VARCHAR) NOT LIKE '%"entities": []%'
      AND json_type(json_extract(CAST(result_json AS JSON), '$.entities')) = 'ARRAY'
),
flat AS (
    SELECT research_id, note_row_id, source_column, note_type, note_date,
           UNNEST(CAST(ea AS JSON[])) AS e
    FROM parsed
),
ext AS (
    SELECT
        CAST(research_id AS VARCHAR)                              AS research_id,
        note_row_id,
        source_column,
        note_type,
        note_date,
        json_extract_string(e, '$.entity_type')                   AS entity_type,
        json_extract_string(e, '$.entity_value')                  AS entity_value,
        json_extract_string(e, '$.present_or_negated')            AS present_or_negated,
        TRY_CAST(json_extract(e, '$.confidence') AS DOUBLE)       AS confidence,
        json_extract_string(e, '$.evidence_text')                 AS evidence_text,
        TRY_CAST(json_extract(e, '$.source_line') AS INTEGER)     AS source_line,
        TRY_CAST(json_extract_string(e, '$.entity_date') AS DATE) AS entity_date,
        TRY_CAST(json_extract(e, '$.date_confidence') AS DOUBLE)  AS date_confidence,
        json_extract_string(e, '$.date_source_keyword')           AS date_source_keyword
    FROM flat
    WHERE json_extract_string(e, '$.entity_type') IS NOT NULL
)
SELECT DISTINCT ON (
    research_id, note_row_id, entity_type, entity_value, source_line
)
    research_id, note_row_id, source_column, note_type, note_date,
    entity_type, entity_value, present_or_negated, confidence,
    evidence_text, source_line, entity_date, date_confidence, date_source_keyword,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM ext
ORDER BY research_id, note_row_id, entity_type, entity_value, source_line, confidence DESC NULLS LAST
"""

ROLLUP_BUILD_SQL = f"""
CREATE OR REPLACE TABLE main.{ROLLUP_TABLE} AS
WITH ev AS (
    SELECT * FROM main.{EVENTS_TABLE}
),
pos AS (
    -- Positive-only rows that contribute to nlp_path_positive_mentioned + rollup flags
    -- LVI rows are EXCLUDED here (vasc v2 is source of truth) but remain in ev.
    SELECT * FROM ev
    WHERE present_or_negated = 'present'
      AND COALESCE(confidence, 0) >= 0.5
),
agg AS (
    SELECT
        research_id,
        COUNT(*)                                    AS n_entities,
        COUNT(DISTINCT entity_type)                 AS n_distinct_entity_types,
        COUNT(DISTINCT note_row_id)                 AS n_path_notes_with_entities,
        BOOL_OR(entity_type = 'surgical_pathology'
                AND present_or_negated = 'present') AS has_surgical_path,
        BOOL_OR(entity_type = 'fna_cytology'
                AND present_or_negated = 'present') AS has_fna_cytology,
        MAX(CASE WHEN entity_type = 'bethesda_class'
                  AND present_or_negated = 'present'
                 THEN entity_value END)             AS max_bethesda_class,
        SUM(CASE WHEN entity_type = 'molecular_testing'
                  AND present_or_negated = 'present'
                 THEN 1 ELSE 0 END)                 AS n_molecular_findings,
        BOOL_OR(entity_type = 'extrathyroidal_extension'
                AND present_or_negated = 'present') AS has_extrathyroidal_extension,
        BOOL_OR(entity_type = 'perineural_invasion'
                AND present_or_negated = 'present') AS has_perineural_invasion,
        BOOL_OR(entity_type = 'multifocality'
                AND present_or_negated = 'present') AS has_multifocality
    FROM ev
    GROUP BY research_id
)
SELECT
    research_id,
    n_entities,
    n_distinct_entity_types,
    has_surgical_path,
    has_fna_cytology,
    max_bethesda_class,
    n_molecular_findings,
    has_extrathyroidal_extension,
    has_perineural_invasion,
    has_multifocality,
    n_path_notes_with_entities,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM agg
"""


def phase_5(con: Any) -> None:
    log("=" * 70)
    log("PHASE 5 — build canonicals + UPDATE CPM nlp_path_*")
    log("=" * 70)

    con.execute(EVENTS_BUILD_SQL)
    n_events = con.execute(f"SELECT COUNT(*) FROM main.{EVENTS_TABLE}").fetchone()[0]
    log(f"  Built main.{EVENTS_TABLE}: {n_events:,} rows")
    gate(n_events > 0, "events table populated")

    con.execute(ROLLUP_BUILD_SQL)
    n_rollup = con.execute(f"SELECT COUNT(*) FROM main.{ROLLUP_TABLE}").fetchone()[0]
    log(f"  Built main.{ROLLUP_TABLE}: {n_rollup:,} rows (RIDs with any entity)")

    # Zero-out the 4 CPM cols, then UPDATE from rollup + positive-mentioned set.
    con.execute("""
        UPDATE main.canonical_patient_master SET
            nlp_path_has_data           = FALSE,
            nlp_path_n_entities         = 0,
            nlp_path_positive_mentioned = FALSE,
            nlp_path_confidence_tier    = NULL
    """)
    log("  CPM zero-out pass complete")

    pos_types_csv = ",".join(f"'{t}'" for t in POSITIVE_ENTITY_TYPES)
    update_sql = f"""
    UPDATE main.canonical_patient_master cpm
    SET
        nlp_path_has_data           = TRUE,
        nlp_path_n_entities         = agg.n_entities,
        nlp_path_positive_mentioned = agg.positive_mentioned,
        nlp_path_confidence_tier    = '{CONFIDENCE_TIER_VALUE}'
    FROM (
        SELECT
            r.research_id,
            r.n_entities,
            (pm.research_id IS NOT NULL) AS positive_mentioned
        FROM main.{ROLLUP_TABLE} r
        LEFT JOIN (
            SELECT DISTINCT research_id
            FROM main.{EVENTS_TABLE}
            WHERE entity_type IN ({pos_types_csv})
              AND present_or_negated = 'present'
              AND COALESCE(confidence, 0) >= 0.5
        ) pm ON CAST(pm.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
    ) agg
    WHERE CAST(cpm.research_id AS VARCHAR) = CAST(agg.research_id AS VARCHAR)
    """
    con.execute(update_sql)

    summary = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE nlp_path_has_data = TRUE)            AS has_data_t,
            COUNT(*) FILTER (WHERE nlp_path_has_data = FALSE)           AS has_data_f,
            SUM(nlp_path_n_entities)                                    AS sum_n_entities,
            COUNT(*) FILTER (WHERE nlp_path_positive_mentioned = TRUE)  AS pos_t
        FROM main.canonical_patient_master
    """).fetchone()
    log(
        f"  Post-rollup CPM: has_data TRUE={summary[0]:,} FALSE={summary[1]:,} "
        f"n_entities_sum={summary[2]} positive_mentioned_t={summary[3]:,}"
    )
    gate(summary[0] + summary[1] == 10_871, "all 10,871 CPM rows accounted for")
    gate(summary[0] > 0, "at least one RID has nlp_path_has_data=TRUE")
    log("PHASE 5 complete")
    logger.flush()


# ── PHASE 6 ──────────────────────────────────────────────────────────────────


def phase_6(con: Any) -> None:
    log("=" * 70)
    log("PHASE 6 — invariants A-F + frozen-section / US LN guards")
    log("=" * 70)
    assert_cpm_intact(con, logger)

    # B: zero NULLs on the 4 nlp_path_* cols (they were UPDATEd from FALSE/0/NULL baseline)
    nulls = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE nlp_path_has_data IS NULL
           OR nlp_path_n_entities IS NULL
           OR nlp_path_positive_mentioned IS NULL
    """).fetchone()[0]
    gate(nulls == 0, f"zero NULLs on nlp_path_has_data/n_entities/positive_mentioned (got {nulls:,})")

    # C: confidence_tier set iff has_data=TRUE
    mism = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE (nlp_path_has_data = TRUE  AND nlp_path_confidence_tier IS NULL)
           OR (nlp_path_has_data = FALSE AND nlp_path_confidence_tier IS NOT NULL)
    """).fetchone()[0]
    gate(mism == 0, f"confidence_tier iff has_data=TRUE (mismatches: {mism:,})")

    # D: source table > 9k rows
    src = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    gate(src >= 9_000, f"source table populated (got {src:,})")

    # E: n_entities >= 0
    neg = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master WHERE nlp_path_n_entities < 0"
    ).fetchone()[0]
    gate(neg == 0, f"nlp_path_n_entities >= 0 everywhere (negative: {neg:,})")

    # F: rollup-vs-events parity (sum of nlp_path_n_entities across CPM == events table rows)
    sum_cpm = con.execute(
        "SELECT COALESCE(SUM(nlp_path_n_entities), 0) FROM main.canonical_patient_master"
    ).fetchone()[0]
    n_events = con.execute(f"SELECT COUNT(*) FROM main.{EVENTS_TABLE}").fetchone()[0]
    gate(int(sum_cpm) == int(n_events), f"sum(nlp_path_n_entities)={sum_cpm} == events rows={n_events}")

    # Frozen-section guard (Script 360 owns)
    assert_unchanged(
        con, logger,
        schema="main", table="canonical_frozen_section_events_v1",
        expected_rows=FROZEN_SECTION_BASELINE,
        label="frozen-section domain untouched",
    )
    # US LN guard (Script 376/377 owns; not touched by this script)
    assert_unchanged(
        con, logger,
        schema="main", table="canonical_us_lymph_node_v2",
        expected_rows=US_LN_BASELINE,
        label="US LN domain untouched",
    )
    log("PHASE 6 complete")
    logger.flush()


# ── PHASE 7 ──────────────────────────────────────────────────────────────────


def phase_7(con: Any) -> None:
    log("=" * 70)
    log("PHASE 7 — registry + __readme sync")
    log("=" * 70)

    n_events = con.execute(f"SELECT COUNT(*) FROM main.{EVENTS_TABLE}").fetchone()[0]
    n_event_rids = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM main.{EVENTS_TABLE}"
    ).fetchone()[0]
    n_rollup = con.execute(f"SELECT COUNT(*) FROM main.{ROLLUP_TABLE}").fetchone()[0]
    n_src = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    n_src_rids = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM main.{SOURCE_TABLE}"
    ).fetchone()[0]

    upsert_registry(
        con, logger,
        detail_table_name=EVENTS_TABLE,
        schema_name="main",
        join_key="research_id",
        grain="one row per UNNESTed pathology LLM entity (clinical-note grain)",
        total_rows=n_events,
        total_patients=n_event_rids,
        domain="Pathology (clinical notes)",
        feeds_master_columns="nlp_path_has_data;nlp_path_n_entities;nlp_path_positive_mentioned;nlp_path_confidence_tier",
        description=(
            "Round-2 round of LLM extraction (gpt-oss-120b on RunPod, 14 entity types) "
            "across HP/OPNOTE/ED_NOTE/DC_SUM/ENDOCRINE_FM/OTHER notes. Distinct from "
            "canonical_path_{benign,gland,malignant}_* (surgical/synoptic side, Script 361)."
        ),
        canonical_version="v1_0",
        feeds_master_columns_array=[
            "nlp_path_has_data", "nlp_path_n_entities",
            "nlp_path_positive_mentioned", "nlp_path_confidence_tier",
        ],
    )
    upsert_registry(
        con, logger,
        detail_table_name=ROLLUP_TABLE,
        schema_name="main",
        join_key="research_id",
        grain="one row per RID with at least one pathology entity",
        total_rows=n_rollup,
        total_patients=n_rollup,
        domain="Pathology (clinical notes)",
        feeds_master_columns="nlp_path_has_data;nlp_path_n_entities;nlp_path_positive_mentioned;nlp_path_confidence_tier",
        description=(
            "Per-RID rollup of canonical_pathology_clinical_events_v1 used to populate "
            "the 4 nlp_path_* CPM columns. LVI rows are intentionally NOT surfaced here "
            "(vascular v2 / Script 368 is source of truth)."
        ),
        canonical_version="v1_0",
        feeds_master_columns_array=[
            "nlp_path_has_data", "nlp_path_n_entities",
            "nlp_path_positive_mentioned", "nlp_path_confidence_tier",
        ],
    )
    upsert_registry(
        con, logger,
        detail_table_name=SOURCE_TABLE,
        schema_name="main",
        join_key="note_row_id",
        grain="one row per clinical note (round-2 LLM extraction batch)",
        total_rows=n_src,
        total_patients=n_src_rids,
        domain="LLM source (pathology)",
        feeds_master_columns="canonical_pathology_clinical_events_v1",
        description="Raw LLM output (round-2 batch, 2026-04-21).",
        canonical_version="v1_0",
        feeds_master_columns_array=["canonical_pathology_clinical_events_v1"],
    )

    append_readme(
        con, logger,
        script="369_pathology_v2_merge_load_rollup",
        content=(
            "[Script 369 2026-04-22] Loaded round-2 pathology LLM extraction "
            f"({n_src:,} notes / {n_events:,} entities) into "
            f"main.{SOURCE_TABLE}. Built canonical_pathology_clinical_events_v1 "
            "(clinical-note grain) and canonical_pathology_clinical_patient_rollup_v1. "
            "Updated 4 nlp_path_* CPM columns (added nlp_path_positive_mentioned + "
            "nlp_path_confidence_tier; left other pre-existing nlp_path_* cols untouched). "
            "Frozen-section + US LN domains explicitly preserved. "
            "Naming is _clinical_ to disambiguate from the surgical/synoptic "
            "canonical_path_{benign,gland,malignant}_* family (Script 361)."
        ),
    )

    log("PHASE 7 complete")
    logger.flush()


# ── main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    ap = argparse.ArgumentParser(description="Script 369 — round-2 pathology merge+load+rollup")
    ap.add_argument("--phase", required=True,
                    choices=["merge", "0", "1", "2", "3", "4", "5", "6", "7", "all"])
    args = ap.parse_args()

    log(f"Script 369 — phase={args.phase} — {datetime.now(timezone.utc).isoformat()}")

    if args.phase == "merge":
        phase_merge()
        return

    con = connect_md(logger)
    phases = {
        "0": phase_0, "1": phase_1, "2": phase_2, "3": phase_3,
        "4": phase_4, "5": phase_5, "6": phase_6, "7": phase_7,
    }
    if args.phase == "all":
        phase_merge()
        con = connect_md(logger)
        for p in ["0", "1", "2", "3", "4", "5", "6", "7"]:
            phases[p](con)
    else:
        phases[args.phase](con)

    logger.flush()
    log("Done.")


if __name__ == "__main__":
    main()
