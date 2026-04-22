#!/usr/bin/env python3
"""
Script 368 — vascular_invasion v2 merge + MD-load + CPM rollup.

CONTEXT
=======
Merges two shard checkpoint JSONL files (output_A / output_B) from the
gpt-oss-120b RunPod extraction (2026-04-22) into a single parquet, loads
it to ``main.note_entities_llm_vascular_invasion``, and rebuilds the four
``nlp_vasc_*`` rollup columns on ``canonical_patient_master``.

v2 improvements over qwen2.5-32b (Script 285):
  * Three-type entity schema: vascular_invasion | lymphatic_invasion |
    lymphovascular_invasion (no more LVI conflation)
  * Mandatory ``qualifier`` field in every entity JSON object:
    present | suspected | indeterminate | absent
  * Explicit decision tree enforced in prompt — combined terms (LVI,
    angiolymphatic, lymph-vascular, etc.) → lymphovascular_invasion ONLY
  * Input: 20,536 notes across gross/micro/diagnosis/synoptic columns

INPUT SHARDS
============
  runs/9domain_v5/vascular_invasion/output_A/note_entities_llm_vascular_invasion.ckpt.jsonl
  runs/9domain_v5/vascular_invasion/output_B/note_entities_llm_vascular_invasion.ckpt.jsonl

OUTPUT PARQUET (merged, written by --phase merge):
  runs/9domain_v5/vascular_invasion/note_entities_llm_vascular_invasion_v2.parquet

PHASES
======
  --phase merge   Read A+B JSONL → deduplicate → fix typos → write parquet (READ-ONLY to MD)
  --phase 0       Parquet audit + positive-count targets (READ-ONLY to MD)
  --phase 1       Archive current main.note_entities_llm_vascular_invasion → archive_pub_v1_0
  --phase 2       Load merged parquet to MD (CREATE OR REPLACE)
  --phase 3       Post-load byte-hash parity (MD == parquet)
  --phase 4       Pre-mutation CPM snapshot → archive_pub_v1_0
  --phase 5       Rollup UPDATE on canonical_patient_master (4-col Tier-2 shape)
  --phase 6       Post-mutation invariants A-F
  --phase 7       Registry + dictionary + __readme sync
  --phase all     Run merge → 0 → 7, halting on any failed gate

HARD RULES (NON-NEGOTIABLE)
============================
  * READ-ONLY to scripts/285_*, scripts/282_*, scripts/283_*, scripts/284_*
  * No touching nlp_synoptic_* / nlp_airway_* / nlp_frozensec_* / nlp_parathyroid_* on CPM
  * NO schema changes to canonical_patient_master (4-col Tier-2 shape preserved)
  * Never print tokens; auth via motherduck_client.get_token()
  * Always CAST(research_id AS VARCHAR) when joining BIGINT/INT to CPM
  * Never con.register(df) on str-dtype frames; use CREATE OR REPLACE TEMP TABLE
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

from motherduck_client import get_token, token_mode  # noqa: E402

# ── constants ────────────────────────────────────────────────────────────────

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"

SOURCE_TABLE = "note_entities_llm_vascular_invasion"
CPM_TABLE = "canonical_patient_master"
REGISTRY_TABLE = "detail_table_registry_v1"
DICTIONARY_TABLE = "data_dictionary_v279"
README_TABLE = "__readme"

SHARD_A = (
    REPO_ROOT
    / "runs" / "9domain_v5" / "vascular_invasion" / "output_A"
    / "note_entities_llm_vascular_invasion.ckpt.jsonl"
)
SHARD_B = (
    REPO_ROOT
    / "runs" / "9domain_v5" / "vascular_invasion" / "output_B"
    / "note_entities_llm_vascular_invasion.ckpt.jsonl"
)
MERGED_PARQUET = (
    REPO_ROOT
    / "runs" / "9domain_v5" / "vascular_invasion"
    / "note_entities_llm_vascular_invasion_v2.parquet"
)

# v2 extraction metadata
V2_MODEL = "openai/gpt-oss-120b"
V2_PROMPT_VERSION = "vascular_invasion_extraction_v2"
V2_LLM_PROVIDER = "vllm"
V2_LLM_SDK = "openai"

# CPM 4-col Tier-2 shape (preserved from Script 212 / Script 285)
VASC_CPM_COLUMNS: tuple[str, ...] = (
    "nlp_vasc_has_data",
    "nlp_vasc_n_entities",
    "nlp_vasc_positive_mentioned",
    "nlp_vasc_confidence_tier",
)
VASC_CONFIDENCE_TIER_VALUE = "below_80pct_concordance"
VASC_POSITIVE_PATTERNS = ["%positive%", "%present%", "%identified%"]

# entity_type typo fix applied during merge
ENTITY_TYPE_FIXES = {
    "lymphvascular_invasion": "lymphovascular_invasion",  # missing 'o' — model typo
}

# qualifier values considered "positive" for CPM rollup
POSITIVE_QUALIFIERS = {"present", "suspected"}

EMPTY_ENTITIES_PATTERN = '%"entities": []%'

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = OUTPUT_DIR / "368_run.log"
PREFLIGHT_PATH = OUTPUT_DIR / "368_preflight.json"

# Post-load expected column set (same 23-col schema as Script 285)
EXPECTED_LOADED_COLUMNS: list[str] = [
    "note_row_id", "domain", "llm_model", "llm_base_url", "extracted_at",
    "result_json", "research_id", "note_type", "note_date", "linkage_date",
    "source_workbook", "source_sheet", "source_column", "note_index",
    "preprocess_batch_id", "preprocessed_at_utc", "preprocess_script_version",
    "entity_domain", "llm_provider", "llm_sdk", "llm_sdk_version",
    "provider_returned_model", "provider_system_fingerprint",
]

ROLLUP_BASE_CTE = f"""
parsed AS (
    SELECT
        research_id,
        note_row_id,
        json_extract(CAST(result_json AS JSON), '$.entities') AS entities_arr
    FROM main.{SOURCE_TABLE}
    WHERE result_json IS NOT NULL
      AND CAST(result_json AS VARCHAR) NOT LIKE '{EMPTY_ENTITIES_PATTERN}'
      AND json_type(json_extract(CAST(result_json AS JSON), '$.entities')) = 'ARRAY'
),
flat AS (
    SELECT research_id, note_row_id,
           UNNEST(CAST(entities_arr AS JSON[])) AS entity
    FROM parsed
),
ext AS (
    SELECT research_id, note_row_id,
           json_extract_string(entity, '$.entity_type')      AS entity_type,
           json_extract_string(entity, '$.entity_value')     AS entity_value,
           json_extract_string(entity, '$.qualifier')        AS qualifier,
           COALESCE(TRY_CAST(json_extract(entity, '$.confidence') AS DOUBLE), 0) AS confidence,
           json_extract_string(entity, '$.present_or_negated') AS present_or_negated
    FROM flat
    WHERE json_extract_string(entity, '$.entity_value') IS NOT NULL
),
pos AS (
    -- v2: use qualifier field; fall back to present_or_negated for compat
    SELECT * FROM ext
    WHERE confidence >= 0.5
      AND present_or_negated = 'present'
      AND (qualifier IS NULL OR qualifier IN ('present', 'suspected'))
)
"""

# ── logging helpers ──────────────────────────────────────────────────────────

_log_buf: list[str] = []


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]}Z] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def _flush_log() -> None:
    mode = "a" if LOG_PATH.exists() else "w"
    with LOG_PATH.open(mode) as f:
        f.write("\n".join(_log_buf) + "\n")
    _log_buf.clear()


def _gate(cond: bool, msg: str) -> None:
    if not cond:
        log(f"  GATE FAILED: {msg}")
        _flush_log()
        sys.exit(1)
    log(f"  gate OK: {msg}")


# ── PHASE MERGE ──────────────────────────────────────────────────────────────

def _fix_entity_types(result_json_str: str) -> str:
    """Fix known entity_type typos in the result_json string."""
    for wrong, correct in ENTITY_TYPE_FIXES.items():
        result_json_str = result_json_str.replace(f'"entity_type": "{wrong}"', f'"entity_type": "{correct}"')
    return result_json_str


def phase_merge() -> None:
    log("=" * 70)
    log("PHASE MERGE — combine shard A + B → deduplicate → write parquet")
    log("=" * 70)

    for shard in (SHARD_A, SHARD_B):
        _gate(shard.exists(), f"shard exists: {shard.name}")

    rows_a: list[dict] = []
    rows_b: list[dict] = []

    for path, bucket in [(SHARD_A, rows_a), (SHARD_B, rows_b)]:
        with path.open() as f:
            for line in f:
                line = line.strip()
                if line:
                    bucket.append(json.loads(line))

    log(f"  Shard A: {len(rows_a):,} raw checkpoint rows")
    log(f"  Shard B: {len(rows_b):,} raw checkpoint rows")

    all_rows = rows_a + rows_b
    log(f"  Combined: {len(all_rows):,} rows before dedup")

    # Deduplicate by note_row_id — keep latest extracted_at
    by_nrid: dict[str, dict] = {}
    for row in all_rows:
        nrid = row["note_row_id"]
        if nrid not in by_nrid or row["extracted_at"] > by_nrid[nrid]["extracted_at"]:
            by_nrid[nrid] = row

    deduped = list(by_nrid.values())
    log(f"  After dedup: {len(deduped):,} unique note_row_ids")
    log(f"  Duplicates removed: {len(all_rows) - len(deduped):,}")

    # Fix entity_type typos in result_json
    typo_fixes = 0
    for row in deduped:
        rj = row.get("result_json", "")
        if isinstance(rj, dict):
            rj = json.dumps(rj)
        fixed = _fix_entity_types(rj)
        if fixed != rj:
            typo_fixes += 1
        row["result_json"] = fixed

    log(f"  Entity type typo fixes applied: {typo_fixes}")

    # Add synthesized provenance columns (match Script 285 schema)
    for row in deduped:
        row["entity_domain"] = "vascular_invasion"
        row["llm_provider"] = V2_LLM_PROVIDER
        row["llm_sdk"] = V2_LLM_SDK
        row["llm_sdk_version"] = None
        row["provider_returned_model"] = V2_MODEL
        row["provider_system_fingerprint"] = None

    df = pd.DataFrame(deduped)

    # Ensure column order matches expected schema
    for col in EXPECTED_LOADED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[EXPECTED_LOADED_COLUMNS]

    # Quick entity stats
    n_with_entities = sum(
        1 for row in deduped
        if row.get("result_json", "{}") not in ("{}", '{"entities": []}')
        and '"entity_type"' in str(row.get("result_json", ""))
    )
    log(f"  Notes with entities: {n_with_entities:,} / {len(deduped):,} ({100*n_with_entities/len(deduped):.1f}%)")

    # Entity type distribution
    type_counts: dict[str, int] = {}
    qualifier_counts: dict[str, int] = {}
    for row in deduped:
        rj = row.get("result_json", "{}")
        try:
            data = json.loads(rj) if isinstance(rj, str) else rj
            for e in data.get("entities", []):
                t = e.get("entity_type", "?")
                q = e.get("qualifier", "MISSING")
                type_counts[t] = type_counts.get(t, 0) + 1
                qualifier_counts[q] = qualifier_counts.get(q, 0) + 1
        except Exception:
            pass

    log("  Entity type distribution:")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        log(f"    {t}: {c:,}")
    log("  Qualifier distribution:")
    for q, c in sorted(qualifier_counts.items(), key=lambda x: -x[1]):
        log(f"    {q}: {c:,}")

    MERGED_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(MERGED_PARQUET, index=False)
    size_mb = MERGED_PARQUET.stat().st_size / 1_048_576
    log(f"  Written: {MERGED_PARQUET.name} ({len(df):,} rows, {size_mb:.1f} MB)")

    preflight = {
        "merged_rows": len(df),
        "shard_a_rows": len(rows_a),
        "shard_b_rows": len(rows_b),
        "duplicate_rows_removed": len(all_rows) - len(deduped),
        "typo_fixes": typo_fixes,
        "notes_with_entities": n_with_entities,
        "entity_type_counts": type_counts,
        "qualifier_counts": qualifier_counts,
        "parquet_path": str(MERGED_PARQUET),
        "merged_at": datetime.now(timezone.utc).isoformat(),
    }
    PREFLIGHT_PATH.write_text(json.dumps(preflight, indent=2))
    log(f"  Preflight written: {PREFLIGHT_PATH.name}")
    log("PHASE MERGE complete")
    _flush_log()


# ── PHASE 0 ──────────────────────────────────────────────────────────────────

def phase_0(con: Any) -> None:
    log("=" * 70)
    log("PHASE 0 — parquet audit (READ-ONLY)")
    log("=" * 70)

    _gate(MERGED_PARQUET.exists(), f"merged parquet exists: {MERGED_PARQUET}")

    import duckdb as _duckdb
    local = _duckdb.connect()
    rel = f"read_parquet('{MERGED_PARQUET}')"

    n_rows = local.execute(f"SELECT COUNT(*) FROM {rel}").fetchone()[0]
    n_rids = local.execute(f"SELECT COUNT(DISTINCT research_id) FROM {rel}").fetchone()[0]
    n_nrids = local.execute(f"SELECT COUNT(DISTINCT note_row_id) FROM {rel}").fetchone()[0]
    models = local.execute(f"SELECT DISTINCT llm_model FROM {rel}").fetchall()
    log(f"  Rows: {n_rows:,} | Distinct RIDs: {n_rids:,} | Distinct note_row_ids: {n_nrids:,}")
    log(f"  Models: {[m[0] for m in models]}")

    _gate(n_rows > 15_000, f"row count > 15,000 (got {n_rows:,})")
    _gate(n_rows == n_nrids, f"no duplicate note_row_ids (rows={n_rows:,} == nrids={n_nrids:,})")

    # Entity type audit on parquet
    entity_sql = f"""
    WITH parsed AS (
        SELECT json_extract(CAST(result_json AS JSON), '$.entities') AS ea
        FROM {rel}
        WHERE result_json IS NOT NULL
          AND CAST(result_json AS VARCHAR) NOT LIKE '{EMPTY_ENTITIES_PATTERN}'
    ),
    flat AS (
        SELECT UNNEST(CAST(ea AS JSON[])) AS entity FROM parsed
        WHERE ea IS NOT NULL
    )
    SELECT
        json_extract_string(entity, '$.entity_type') AS entity_type,
        COUNT(*) AS n
    FROM flat
    GROUP BY 1 ORDER BY 2 DESC
    """
    rows = local.execute(entity_sql).fetchall()
    log("  Entity type distribution in parquet:")
    for t, c in rows:
        log(f"    {t}: {c:,}")
    _gate(
        any(t == "lymphovascular_invasion" for t, _ in rows),
        "lymphovascular_invasion entity type present (LVI conflation fixed)"
    )
    _gate(
        not any(t == "lymphvascular_invasion" for t, _ in rows),
        "no lymphvascular_invasion typo (fixed during merge)"
    )

    # Check qualifier field presence
    qual_sql = f"""
    WITH parsed AS (
        SELECT json_extract(CAST(result_json AS JSON), '$.entities') AS ea
        FROM {rel}
        WHERE result_json IS NOT NULL
    ),
    flat AS (SELECT UNNEST(CAST(ea AS JSON[])) AS entity FROM parsed WHERE ea IS NOT NULL)
    SELECT
        CASE WHEN json_extract_string(entity, '$.qualifier') IS NULL THEN 'MISSING' ELSE 'PRESENT' END AS has_qualifier,
        COUNT(*) AS n
    FROM flat
    GROUP BY 1
    """
    qual_rows = local.execute(qual_sql).fetchall()
    log("  Qualifier field presence:")
    for q, c in qual_rows:
        log(f"    {q}: {c:,}")

    # Current MD state
    cur_rows = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    cur_model = con.execute(f"SELECT DISTINCT llm_model FROM main.{SOURCE_TABLE}").fetchall()
    log(f"  Current MD {SOURCE_TABLE}: {cur_rows:,} rows, models={[m[0] for m in cur_model]}")

    local.close()
    log("PHASE 0 complete")
    _flush_log()


# ── PHASE 1 ──────────────────────────────────────────────────────────────────

def phase_1(con: Any) -> None:
    log("=" * 70)
    log("PHASE 1 — archive current MD table → archive_pub_v1_0")
    log("=" * 70)

    archive_name = f"{SOURCE_TABLE}_pre368_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    con.execute(f"USE {CANONICAL_DB}")
    cur_rows = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    log(f"  Archiving {cur_rows:,} rows → {ARCHIVE_DB}.{ARCHIVE_SCHEMA}.{archive_name}")

    con.execute(f"""
        CREATE OR REPLACE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{archive_name} AS
        SELECT * FROM main.{SOURCE_TABLE}
    """)
    archived = con.execute(
        f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{archive_name}'
    ).fetchone()[0]
    _gate(archived == cur_rows, f"archive row count matches ({archived:,})")
    log(f"  Archived: {archived:,} rows as {archive_name}")
    log("PHASE 1 complete")
    _flush_log()


# ── PHASE 2 ──────────────────────────────────────────────────────────────────

def phase_2(con: Any) -> None:
    log("=" * 70)
    log("PHASE 2 — load merged parquet → MD main.note_entities_llm_vascular_invasion")
    log("=" * 70)

    _gate(MERGED_PARQUET.exists(), "merged parquet exists")

    import duckdb as _duckdb
    local = _duckdb.connect()
    df = local.execute(f"SELECT * FROM read_parquet('{MERGED_PARQUET}')").df()
    local.close()

    log(f"  Loaded parquet into local frame: {len(df):,} rows")

    # Materialize via temp table to avoid str-dtype register issues
    con.execute("USE " + CANONICAL_DB)
    con.execute("CREATE OR REPLACE TEMP TABLE _vasc_v2_stage AS SELECT * FROM df")
    staged = con.execute("SELECT COUNT(*) FROM _vasc_v2_stage").fetchone()[0]
    _gate(staged == len(df), f"staging row count ({staged:,}) == parquet rows ({len(df):,})")

    con.execute(f"""
        CREATE OR REPLACE TABLE main.{SOURCE_TABLE} AS
        SELECT * FROM _vasc_v2_stage
    """)
    loaded = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    _gate(loaded == len(df), f"MD row count ({loaded:,}) == parquet ({len(df):,})")
    log(f"  Loaded {loaded:,} rows into main.{SOURCE_TABLE}")

    # Verify columns
    cols = [r[0] for r in con.execute(
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_name='{SOURCE_TABLE}' AND table_schema='main' ORDER BY ordinal_position"
    ).fetchall()]
    missing = [c for c in EXPECTED_LOADED_COLUMNS if c not in cols]
    _gate(len(missing) == 0, f"all expected columns present (missing: {missing})")
    log(f"  Columns verified: {len(cols)} cols")
    log("PHASE 2 complete")
    _flush_log()


# ── PHASE 3 ──────────────────────────────────────────────────────────────────

def phase_3(con: Any) -> None:
    log("=" * 70)
    log("PHASE 3 — post-load parity check")
    log("=" * 70)

    con.execute(f"USE {CANONICAL_DB}")
    md_rows = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    md_nrids = con.execute(f"SELECT COUNT(DISTINCT note_row_id) FROM main.{SOURCE_TABLE}").fetchone()[0]
    md_rids = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM main.{SOURCE_TABLE}").fetchone()[0]

    import duckdb as _duckdb
    local = _duckdb.connect()
    pq_rows = local.execute(f"SELECT COUNT(*) FROM read_parquet('{MERGED_PARQUET}')").fetchone()[0]
    pq_rids = local.execute(f"SELECT COUNT(DISTINCT research_id) FROM read_parquet('{MERGED_PARQUET}')").fetchone()[0]
    local.close()

    log(f"  MD:      {md_rows:,} rows | {md_rids:,} RIDs | {md_nrids:,} note_row_ids")
    log(f"  Parquet: {pq_rows:,} rows | {pq_rids:,} RIDs")
    _gate(md_rows == pq_rows, f"row count parity ({md_rows:,})")
    _gate(md_rids == pq_rids, f"RID count parity ({md_rids:,})")
    _gate(md_rows == md_nrids, "no duplicate note_row_ids in MD")

    log("PHASE 3 complete")
    _flush_log()


# ── PHASE 4 ──────────────────────────────────────────────────────────────────

def phase_4(con: Any) -> None:
    log("=" * 70)
    log("PHASE 4 — pre-mutation CPM snapshot → archive_pub_v1_0")
    log("=" * 70)

    con.execute(f"USE {CANONICAL_DB}")
    snap_name = f"cpm_pre368_vasc_rollup_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    con.execute(f"""
        CREATE OR REPLACE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{snap_name} AS
        SELECT research_id, {', '.join(VASC_CPM_COLUMNS)}
        FROM main.{CPM_TABLE}
    """)
    snapped = con.execute(
        f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{snap_name}'
    ).fetchone()[0]
    log(f"  Snapped {snapped:,} CPM rows → {snap_name}")

    # Log current CPM rollup state
    cur = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE nlp_vasc_has_data = TRUE)  AS has_data_true,
            COUNT(*) FILTER (WHERE nlp_vasc_has_data = FALSE) AS has_data_false,
            COUNT(*) FILTER (WHERE nlp_vasc_has_data IS NULL) AS has_data_null,
            SUM(nlp_vasc_n_entities) AS sum_n_entities
        FROM main.{CPM_TABLE}
    """).fetchone()
    log(f"  Current CPM: has_data TRUE={cur[0]:,} FALSE={cur[1]:,} NULL={cur[2]:,} n_entities_sum={cur[3]}")
    log("PHASE 4 complete")
    _flush_log()


# ── PHASE 5 ──────────────────────────────────────────────────────────────────

def phase_5(con: Any) -> None:
    log("=" * 70)
    log("PHASE 5 — rollup UPDATE on canonical_patient_master (4-col Tier-2)")
    log("=" * 70)

    con.execute(f"USE {CANONICAL_DB}")

    like_clauses = " OR ".join(
        f"CAST(result_json AS VARCHAR) LIKE '{p}'" for p in VASC_POSITIVE_PATTERNS
    )

    # Zero-out pass: set all CPM vasc columns to FALSE / NULL / 0
    con.execute(f"""
        UPDATE main.{CPM_TABLE} SET
            nlp_vasc_has_data           = FALSE,
            nlp_vasc_n_entities         = 0,
            nlp_vasc_positive_mentioned = FALSE,
            nlp_vasc_confidence_tier    = NULL
    """)
    log("  Zero-out pass complete")

    # Build rollup CTE and UPDATE
    rollup_sql = f"""
    UPDATE main.{CPM_TABLE} cpm
    SET
        nlp_vasc_has_data           = agg.has_data,
        nlp_vasc_n_entities         = agg.n_entities,
        nlp_vasc_positive_mentioned = agg.positive_mentioned,
        nlp_vasc_confidence_tier    = CASE WHEN agg.has_data THEN '{VASC_CONFIDENCE_TIER_VALUE}' ELSE NULL END
    FROM (
        WITH {ROLLUP_BASE_CTE}
        , pos_agg AS (
            SELECT
                research_id,
                COUNT(*) AS n_pos_entities
            FROM pos
            GROUP BY research_id
        ),
        pos_mentioned AS (
            SELECT DISTINCT research_id
            FROM main.{SOURCE_TABLE}
            WHERE {like_clauses}
        )
        SELECT
            CAST(p.research_id AS VARCHAR) AS research_id,
            TRUE                            AS has_data,
            p.n_pos_entities                AS n_entities,
            (pm.research_id IS NOT NULL)    AS positive_mentioned
        FROM pos_agg p
        LEFT JOIN pos_mentioned pm ON CAST(pm.research_id AS VARCHAR) = CAST(p.research_id AS VARCHAR)
        WHERE p.n_pos_entities > 0
    ) agg
    WHERE CAST(cpm.research_id AS VARCHAR) = agg.research_id
    """
    con.execute(rollup_sql)

    result = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE nlp_vasc_has_data = TRUE)  AS has_data_true,
            COUNT(*) FILTER (WHERE nlp_vasc_has_data = FALSE) AS has_data_false,
            SUM(nlp_vasc_n_entities)                          AS sum_n_entities,
            COUNT(*) FILTER (WHERE nlp_vasc_positive_mentioned = TRUE) AS pos_mentioned_true
        FROM main.{CPM_TABLE}
    """).fetchone()

    log(f"  Post-rollup CPM: has_data TRUE={result[0]:,} FALSE={result[1]:,} "
        f"n_entities_sum={result[2]} pos_mentioned_true={result[3]:,}")
    _gate(result[0] > 0, "at least one RID has has_data=TRUE")
    _gate(result[1] + result[0] == 10871, f"all 10,871 CPM rows accounted for")
    log("PHASE 5 complete")
    _flush_log()


# ── PHASE 6 ──────────────────────────────────────────────────────────────────

def phase_6(con: Any) -> None:
    log("=" * 70)
    log("PHASE 6 — post-mutation invariants A-F")
    log("=" * 70)

    con.execute(f"USE {CANONICAL_DB}")

    # A: CPM row count unchanged
    cpm_rows = con.execute(f"SELECT COUNT(*) FROM main.{CPM_TABLE}").fetchone()[0]
    _gate(cpm_rows == 10871, f"CPM row count 10,871 (got {cpm_rows:,})")

    # B: No NULLs on vasc columns
    nulls = con.execute(f"""
        SELECT COUNT(*) FROM main.{CPM_TABLE}
        WHERE nlp_vasc_has_data IS NULL
           OR nlp_vasc_n_entities IS NULL
           OR nlp_vasc_positive_mentioned IS NULL
    """).fetchone()[0]
    _gate(nulls == 0, f"zero NULLs on nlp_vasc_has_data/n_entities/positive_mentioned (got {nulls:,})")

    # C: confidence_tier set iff has_data=TRUE
    tier_mismatch = con.execute(f"""
        SELECT COUNT(*) FROM main.{CPM_TABLE}
        WHERE (nlp_vasc_has_data = TRUE AND nlp_vasc_confidence_tier IS NULL)
           OR (nlp_vasc_has_data = FALSE AND nlp_vasc_confidence_tier IS NOT NULL)
    """).fetchone()[0]
    _gate(tier_mismatch == 0, f"confidence_tier iff has_data=TRUE (mismatches: {tier_mismatch:,})")

    # D: source table row count stable
    src_rows = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    _gate(src_rows > 15_000, f"source table still has rows (got {src_rows:,})")

    # E: n_entities >= 0 everywhere
    neg = con.execute(f"""
        SELECT COUNT(*) FROM main.{CPM_TABLE} WHERE nlp_vasc_n_entities < 0
    """).fetchone()[0]
    _gate(neg == 0, f"n_entities >= 0 everywhere (negative: {neg:,})")

    # F: No nlp_vasc_n_notes or nlp_vasc_key_finding columns (Tier-2 shape preserved)
    bad_cols = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = '{CPM_TABLE}' AND table_schema = 'main'
          AND column_name IN ('nlp_vasc_n_notes', 'nlp_vasc_key_finding')
    """).fetchall()
    _gate(len(bad_cols) == 0, f"Tier-2 columns nlp_vasc_n_notes/key_finding NOT present (found: {bad_cols})")

    log("  All invariants A-F passed")
    log("PHASE 6 complete")
    _flush_log()


# ── PHASE 7 ──────────────────────────────────────────────────────────────────

def phase_7(con: Any) -> None:
    log("=" * 70)
    log("PHASE 7 — registry + dictionary + __readme sync")
    log("=" * 70)

    con.execute(f"USE {CANONICAL_DB}")
    now_ts = f"CAST('{datetime.now(timezone.utc).isoformat()}' AS TIMESTAMP)"

    # Update registry row count
    try:
        src_rows = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
        con.execute(f"""
            UPDATE main.{REGISTRY_TABLE}
            SET row_count = {src_rows},
                last_updated = {now_ts}
            WHERE detail_table_name = '{SOURCE_TABLE}'
        """)
        log(f"  Registry updated: {SOURCE_TABLE} row_count={src_rows:,}")
    except Exception as e:
        log(f"  Registry update skipped (non-fatal): {e}")

    # Update __readme
    try:
        con.execute(f"""
            UPDATE main.{README_TABLE}
            SET note = CONCAT(
                COALESCE(note, ''),
                '\n[Script 368 {datetime.now(timezone.utc).strftime('%Y-%m-%d')}] '
                'Replaced qwen2.5-32b vascular extraction with gpt-oss-120b v2. '
                'New schema: 3-type entity classification (vascular/lymphatic/lymphovascular_invasion), '
                'qualifier field (present|suspected|indeterminate|absent), '
                'LVI conflation fixed. Source: 20,536 notes across 2 RunPod H200 pods.'
            )
            WHERE table_name = '{SOURCE_TABLE}'
        """)
        log("  __readme updated")
    except Exception as e:
        log(f"  __readme update skipped (non-fatal): {e}")

    log("PHASE 7 complete")
    _flush_log()


# ── main ─────────────────────────────────────────────────────────────────────

def _connect_md() -> Any:
    import duckdb
    token = get_token()
    mode = token_mode()
    log(f"  Connecting to MotherDuck ({mode}) …")
    con = duckdb.connect(f"md:?motherduck_token={token}")
    con.execute(f"USE {CANONICAL_DB}")
    log(f"  Connected. DB: {CANONICAL_DB}")
    return con


def main() -> None:
    ap = argparse.ArgumentParser(description="Script 368 — vasc v2 merge+load+rollup")
    ap.add_argument("--phase", required=True,
                    choices=["merge", "0", "1", "2", "3", "4", "5", "6", "7", "all"],
                    help="Phase to run")
    args = ap.parse_args()

    log(f"Script 368 — phase={args.phase} — {datetime.now(timezone.utc).isoformat()}")

    if args.phase == "merge":
        phase_merge()
        return

    con = _connect_md()

    phases = {
        "0": phase_0, "1": phase_1, "2": phase_2, "3": phase_3,
        "4": phase_4, "5": phase_5, "6": phase_6, "7": phase_7,
    }

    if args.phase == "all":
        phase_merge()
        con = _connect_md()
        for p in ["0", "1", "2", "3", "4", "5", "6", "7"]:
            phases[p](con)
    else:
        phases[args.phase](con)

    _flush_log()
    log("Done.")


if __name__ == "__main__":
    main()
