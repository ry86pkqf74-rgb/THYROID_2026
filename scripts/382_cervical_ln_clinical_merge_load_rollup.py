#!/usr/bin/env python3
"""
Script 382 — round-2 cervical_ln_detail LLM extraction merge + MD-load + canonical build.

CONTEXT
=======
Loads the round-2 cervical_ln_detail batch (10,084 notes / 4,499 entities,
12 entity types — top: ln_level, ln_size, fna_of_ln) into
``main.note_entities_llm_cervical_ln_detail`` and builds a NEW
clinical-note-grain canonical pair:

  * ``main.canonical_cervical_ln_clinical_events_v1``
  * ``main.canonical_cervical_ln_clinical_patient_rollup_v1``

The ``_clinical_`` infix is load-bearing (D4): this is the CLINICAL-NOTE side
of cervical-LN documentation and is intentionally distinct from the
US-imaging side ``canonical_us_lymph_node_v2`` (Scripts 376/377/378).
The exam-id portability lock means we must NOT extend the US LN canonical
with note-side rows.  See cursor prompt 2026-04-22 §1B / D4.

CPM 4-col Tier-2 surface (all NEW):
  nlp_cervln_has_data            BOOLEAN
  nlp_cervln_n_entities          INTEGER
  nlp_cervln_positive_mentioned  BOOLEAN
  nlp_cervln_confidence_tier     VARCHAR

PHASES — same shape as Script 369.
"""

from __future__ import annotations

import argparse
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

DOMAIN = "cervical_ln_detail"
SOURCE_TABLE = "note_entities_llm_cervical_ln_detail"
ARCHIVE_NAME = f"{SOURCE_TABLE}_pre382_20260422"
EVENTS_TABLE = "canonical_cervical_ln_clinical_events_v1"
ROLLUP_TABLE = "canonical_cervical_ln_clinical_patient_rollup_v1"
CPM_COLUMNS: dict[str, str] = {
    "nlp_cervln_has_data": "BOOLEAN",
    "nlp_cervln_n_entities": "INTEGER",
    "nlp_cervln_positive_mentioned": "BOOLEAN",
    "nlp_cervln_confidence_tier": "VARCHAR",
}
CONFIDENCE_TIER_VALUE = "round2_gpt_oss_120b_v1"

# Per prompt §3 B2, positive_mentioned fires when present_or_negated='present'
# on the ln_level entity (cervical-LN positivity signal in clinical notes).
POSITIVE_ENTITY_TYPES = ("ln_level",)

# Hard-guard baselines (probed 2026-04-22)
US_LN_BASELINE = 6_801
FROZEN_SECTION_BASELINE = 7_081

CKPT_PATH = REPO_ROOT / "runs" / "round2_20260421" / DOMAIN / "output" / f"note_entities_llm_{DOMAIN}.ckpt.jsonl"
PARQUET_PATH = REPO_ROOT / "runs" / "round2_20260421" / DOMAIN / "output" / f"note_entities_llm_{DOMAIN}.parquet"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
LOG_PATH = OUTPUT_DIR / "382_run.log"
PREFLIGHT_PATH = OUTPUT_DIR / "382_preflight.json"

EXPECTED_LOADED_COLUMNS = [
    "note_row_id", "domain", "llm_model", "llm_base_url", "extracted_at",
    "result_json", "research_id", "note_type", "note_date", "linkage_date",
    "source_workbook", "source_sheet", "source_column", "note_index",
    "preprocess_batch_id", "preprocessed_at_utc", "preprocess_script_version",
]

logger = RunLogger(LOG_PATH)
log = logger.log
gate = logger.gate


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

    by_nrid: dict[str, dict] = {}
    for row in raw:
        nrid = row["note_row_id"]
        if nrid not in by_nrid or row["extracted_at"] > by_nrid[nrid]["extracted_at"]:
            by_nrid[nrid] = row
    deduped = list(by_nrid.values())
    log(f"  After dedup: {len(deduped):,} unique note_row_ids ({len(raw) - len(deduped):,} dups removed)")

    df = pd.DataFrame(deduped)
    for col in EXPECTED_LOADED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[EXPECTED_LOADED_COLUMNS]

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

    sql = f"""
    WITH parsed AS (
        SELECT json_extract(CAST(result_json AS JSON), '$.entities') AS ea
        FROM {rel}
        WHERE result_json IS NOT NULL
          AND CAST(result_json AS VARCHAR) NOT LIKE '%"entities": []%'
    ),
    flat AS (SELECT UNNEST(CAST(ea AS JSON[])) AS e FROM parsed WHERE ea IS NOT NULL)
    SELECT json_extract_string(e, '$.entity_type') AS et, COUNT(*) AS n
    FROM flat GROUP BY 1 ORDER BY 2 DESC
    """
    rows = local.execute(sql).fetchall()
    types = {r[0] for r in rows}
    log("  Entity-type distribution in parquet:")
    for t, c in rows:
        log(f"    {t}: {c:,}")
    gate("ln_level" in types, "ln_level entity type present (top type for cervical_ln_detail)")
    local.close()

    if table_exists(con, "main", SOURCE_TABLE):
        cur = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
        models = [r[0] for r in con.execute(f"SELECT DISTINCT llm_model FROM main.{SOURCE_TABLE}").fetchall()]
        log(f"  Current MD main.{SOURCE_TABLE}: {cur:,} rows, models={models}")
    else:
        log(f"  Current MD main.{SOURCE_TABLE}: NOT PRESENT (will be created)")

    log("PHASE 0 complete")
    logger.flush()


def phase_1(con: Any) -> None:
    log("=" * 70)
    log(f"PHASE 1 — archive current main.{SOURCE_TABLE} → {ARCHIVE_SCHEMA}")
    log("=" * 70)
    archive_table_if_present(con, logger, "main", SOURCE_TABLE, ARCHIVE_NAME)
    log("PHASE 1 complete")
    logger.flush()


def phase_2(con: Any) -> None:
    log("=" * 70)
    log(f"PHASE 2 — load merged parquet → main.{SOURCE_TABLE}")
    log("=" * 70)
    gate(PARQUET_PATH.exists(), "merged parquet exists")

    import duckdb as _duckdb
    local = _duckdb.connect()
    df = local.execute(f"SELECT * FROM read_parquet('{PARQUET_PATH}')").df()
    local.close()
    log(f"  Loaded parquet: {len(df):,} rows")

    con.execute(f"USE {CANONICAL_DB}")
    con.execute("CREATE OR REPLACE TEMP TABLE _r2_cervln_stage AS SELECT * FROM df")
    staged = con.execute("SELECT COUNT(*) FROM _r2_cervln_stage").fetchone()[0]
    gate(staged == len(df), f"staging parity ({staged:,} == {len(df):,})")

    con.execute(f"CREATE OR REPLACE TABLE main.{SOURCE_TABLE} AS SELECT * FROM _r2_cervln_stage")
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


def phase_4(con: Any) -> None:
    log("=" * 70)
    log("PHASE 4 — CPM snapshot + ALTER ADD COLUMN IF NOT EXISTS")
    log("=" * 70)
    snap = f"cpm_pre382_cervln_rollup_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    cpm_cols_present = [c for c in CPM_COLUMNS.keys() if column_exists(con, "main", "canonical_patient_master", c)]
    if cpm_cols_present:
        snap_select = "research_id, " + ", ".join(cpm_cols_present)
        con.execute(
            f'CREATE OR REPLACE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}."{snap}" AS '
            f"SELECT {snap_select} FROM main.canonical_patient_master"
        )
        log(f"  CPM snapshot: {snap} ({len(cpm_cols_present)} cols)")
    else:
        log("  No pre-existing CPM nlp_cervln_* cols (all new)")

    add_cpm_columns_if_missing(con, logger, CPM_COLUMNS)
    log("PHASE 4 complete")
    logger.flush()


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
SELECT DISTINCT ON (research_id, note_row_id, entity_type, entity_value, source_line)
    research_id, note_row_id, source_column, note_type, note_date,
    entity_type, entity_value, present_or_negated, confidence,
    evidence_text, source_line, entity_date, date_confidence, date_source_keyword,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM ext
ORDER BY research_id, note_row_id, entity_type, entity_value, source_line, confidence DESC NULLS LAST
"""

ROLLUP_BUILD_SQL = f"""
CREATE OR REPLACE TABLE main.{ROLLUP_TABLE} AS
WITH ev AS (SELECT * FROM main.{EVENTS_TABLE}),
agg AS (
    SELECT
        research_id,
        COUNT(*)                                  AS n_entities,
        COUNT(DISTINCT entity_type)               AS n_distinct_entity_types,
        COUNT(DISTINCT note_row_id)               AS n_notes_with_entities,
        SUM(CASE WHEN entity_type = 'ln_level'
                   AND present_or_negated = 'present' THEN 1 ELSE 0 END) AS n_positive_ln_level_mentions,
        SUM(CASE WHEN entity_type = 'ln_size'    THEN 1 ELSE 0 END)      AS n_ln_size_mentions,
        SUM(CASE WHEN entity_type = 'fna_of_ln'  THEN 1 ELSE 0 END)      AS n_fna_of_ln_mentions,
        BOOL_OR(entity_type = 'ln_level'
                AND present_or_negated = 'present')                       AS has_positive_ln_level
    FROM ev
    GROUP BY research_id
)
SELECT
    research_id,
    n_entities,
    n_distinct_entity_types,
    n_notes_with_entities,
    n_positive_ln_level_mentions,
    n_ln_size_mentions,
    n_fna_of_ln_mentions,
    has_positive_ln_level,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM agg
"""


def phase_5(con: Any) -> None:
    log("=" * 70)
    log("PHASE 5 — build canonicals + UPDATE CPM nlp_cervln_*")
    log("=" * 70)

    con.execute(EVENTS_BUILD_SQL)
    n_events = con.execute(f"SELECT COUNT(*) FROM main.{EVENTS_TABLE}").fetchone()[0]
    log(f"  Built main.{EVENTS_TABLE}: {n_events:,} rows")
    gate(n_events > 0, "events table populated")

    con.execute(ROLLUP_BUILD_SQL)
    n_rollup = con.execute(f"SELECT COUNT(*) FROM main.{ROLLUP_TABLE}").fetchone()[0]
    log(f"  Built main.{ROLLUP_TABLE}: {n_rollup:,} rows (RIDs with any entity)")

    con.execute("""
        UPDATE main.canonical_patient_master SET
            nlp_cervln_has_data           = FALSE,
            nlp_cervln_n_entities         = 0,
            nlp_cervln_positive_mentioned = FALSE,
            nlp_cervln_confidence_tier    = NULL
    """)
    log("  CPM zero-out pass complete")

    pos_types_csv = ",".join(f"'{t}'" for t in POSITIVE_ENTITY_TYPES)
    update_sql = f"""
    UPDATE main.canonical_patient_master cpm
    SET
        nlp_cervln_has_data           = TRUE,
        nlp_cervln_n_entities         = agg.n_entities,
        nlp_cervln_positive_mentioned = agg.positive_mentioned,
        nlp_cervln_confidence_tier    = '{CONFIDENCE_TIER_VALUE}'
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
            COUNT(*) FILTER (WHERE nlp_cervln_has_data = TRUE)            AS has_data_t,
            COUNT(*) FILTER (WHERE nlp_cervln_has_data = FALSE)           AS has_data_f,
            SUM(nlp_cervln_n_entities)                                    AS sum_n_entities,
            COUNT(*) FILTER (WHERE nlp_cervln_positive_mentioned = TRUE)  AS pos_t
        FROM main.canonical_patient_master
    """).fetchone()
    log(f"  Post-rollup CPM: has_data TRUE={summary[0]:,} FALSE={summary[1]:,} "
        f"n_entities_sum={summary[2]} positive_mentioned_t={summary[3]:,}")
    gate(summary[0] + summary[1] == 10_871, "all 10,871 CPM rows accounted for")
    gate(summary[0] > 0, "at least one RID has nlp_cervln_has_data=TRUE")
    log("PHASE 5 complete")
    logger.flush()


def phase_6(con: Any) -> None:
    log("=" * 70)
    log("PHASE 6 — invariants A-F + US LN / frozen-section guards")
    log("=" * 70)
    assert_cpm_intact(con, logger)

    nulls = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE nlp_cervln_has_data IS NULL
           OR nlp_cervln_n_entities IS NULL
           OR nlp_cervln_positive_mentioned IS NULL
    """).fetchone()[0]
    gate(nulls == 0, f"zero NULLs on nlp_cervln_has_data/n_entities/positive_mentioned (got {nulls:,})")

    mism = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE (nlp_cervln_has_data = TRUE  AND nlp_cervln_confidence_tier IS NULL)
           OR (nlp_cervln_has_data = FALSE AND nlp_cervln_confidence_tier IS NOT NULL)
    """).fetchone()[0]
    gate(mism == 0, f"confidence_tier iff has_data=TRUE (mismatches: {mism:,})")

    src = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    gate(src >= 9_000, f"source table populated (got {src:,})")

    neg = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master WHERE nlp_cervln_n_entities < 0"
    ).fetchone()[0]
    gate(neg == 0, f"nlp_cervln_n_entities >= 0 everywhere (negative: {neg:,})")

    sum_cpm = con.execute(
        "SELECT COALESCE(SUM(nlp_cervln_n_entities), 0) FROM main.canonical_patient_master"
    ).fetchone()[0]
    n_events = con.execute(f"SELECT COUNT(*) FROM main.{EVENTS_TABLE}").fetchone()[0]
    gate(int(sum_cpm) == int(n_events), f"sum(nlp_cervln_n_entities)={sum_cpm} == events rows={n_events}")

    # Hard guard: US LN (US-imaging side) untouched.  Exam-id portability lock.
    assert_unchanged(
        con, logger,
        schema="main", table="canonical_us_lymph_node_v2",
        expected_rows=US_LN_BASELINE,
        label="US LN domain untouched (exam-id portability lock)",
    )
    # Frozen-section guard
    assert_unchanged(
        con, logger,
        schema="main", table="canonical_frozen_section_events_v1",
        expected_rows=FROZEN_SECTION_BASELINE,
        label="frozen-section domain untouched",
    )
    log("PHASE 6 complete")
    logger.flush()


def phase_7(con: Any) -> None:
    log("=" * 70)
    log("PHASE 7 — registry + __readme sync")
    log("=" * 70)

    n_events = con.execute(f"SELECT COUNT(*) FROM main.{EVENTS_TABLE}").fetchone()[0]
    n_event_rids = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM main.{EVENTS_TABLE}").fetchone()[0]
    n_rollup = con.execute(f"SELECT COUNT(*) FROM main.{ROLLUP_TABLE}").fetchone()[0]
    n_src = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    n_src_rids = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM main.{SOURCE_TABLE}").fetchone()[0]

    upsert_registry(
        con, logger,
        detail_table_name=EVENTS_TABLE, schema_name="main", join_key="research_id",
        grain="one row per UNNESTed cervical-LN clinical-note LLM entity",
        total_rows=n_events, total_patients=n_event_rids,
        domain="Cervical lymph node (clinical notes)",
        feeds_master_columns="nlp_cervln_has_data;nlp_cervln_n_entities;nlp_cervln_positive_mentioned;nlp_cervln_confidence_tier",
        description=(
            "Round-2 round of LLM extraction (gpt-oss-120b on RunPod, 12 entity types). "
            "Clinical-note grain — distinct from US-imaging-side canonical_us_lymph_node_v2 "
            "(Scripts 376/377/378). Top entities: ln_level, ln_size, fna_of_ln."
        ),
        canonical_version="v1_0",
        feeds_master_columns_array=list(CPM_COLUMNS.keys()),
    )
    upsert_registry(
        con, logger,
        detail_table_name=ROLLUP_TABLE, schema_name="main", join_key="research_id",
        grain="one row per RID with at least one cervical-LN clinical-note entity",
        total_rows=n_rollup, total_patients=n_rollup,
        domain="Cervical lymph node (clinical notes)",
        feeds_master_columns="nlp_cervln_has_data;nlp_cervln_n_entities;nlp_cervln_positive_mentioned;nlp_cervln_confidence_tier",
        description="Per-RID rollup of canonical_cervical_ln_clinical_events_v1; populates 4 nlp_cervln_* CPM cols.",
        canonical_version="v1_0",
        feeds_master_columns_array=list(CPM_COLUMNS.keys()),
    )
    upsert_registry(
        con, logger,
        detail_table_name=SOURCE_TABLE, schema_name="main", join_key="note_row_id",
        grain="one row per clinical note (round-2 LLM extraction batch)",
        total_rows=n_src, total_patients=n_src_rids,
        domain="LLM source (cervical_ln_detail)",
        feeds_master_columns="canonical_cervical_ln_clinical_events_v1",
        description="Raw LLM output (round-2 batch, 2026-04-21).",
        canonical_version="v1_0",
        feeds_master_columns_array=["canonical_cervical_ln_clinical_events_v1"],
    )

    append_readme(
        con, logger,
        script="382_cervical_ln_clinical_merge_load_rollup",
        content=(
            "[Script 382 2026-04-22] Loaded round-2 cervical_ln_detail LLM extraction "
            f"({n_src:,} notes / {n_events:,} entities) into main.{SOURCE_TABLE}. "
            "Built canonical_cervical_ln_clinical_events_v1 + "
            "canonical_cervical_ln_clinical_patient_rollup_v1 (clinical-note grain). "
            "Added 4 nlp_cervln_* CPM columns. US LN imaging-side canonical "
            "(canonical_us_lymph_node_v2) preserved unchanged — exam-id portability lock."
        ),
    )
    log("PHASE 7 complete")
    logger.flush()


def main() -> None:
    ap = argparse.ArgumentParser(description="Script 382 — round-2 cervical_ln_detail merge+load+rollup")
    ap.add_argument("--phase", required=True,
                    choices=["merge", "0", "1", "2", "3", "4", "5", "6", "7", "all"])
    args = ap.parse_args()

    log(f"Script 382 — phase={args.phase} — {datetime.now(timezone.utc).isoformat()}")

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
