#!/usr/bin/env python3
"""
Script 284 — parathyroid_detail v4 MD-load + CPM rollup (Phase B' #3 of 4).

CONTEXT
=======
Re-loads ``main.note_entities_llm_parathyroid_detail`` from the local v4
parquet at ``runs/9domain_v4/parathyroid_detail/output/...parquet`` (qwen2.5-32b
extraction, 2026-04-19) and rebuilds the four ``nlp_parathyroid_*`` rollup
columns on ``canonical_patient_master`` with explicit TRUE/FALSE semantics
(zero NULLs), replacing the prior MIXED ``qwen3:32b`` + ``qwen3:32b-ctx8k``
extraction (Apr 1-3 2026) plus the rollup it fed (which currently has
10,761 NULLs on ``nlp_parathyroid_has_data``).

Structurally identical to ``scripts/282_airway_v4_md_load_and_rollup.py``
(Phase B' #1 of 4) for the dedup pieces and ``scripts/283_frozensec_v4_md_load_and_rollup.py``
for the ordinary path; only the domain-specific constants differ. Sibling
scripts in the family: 282 airway_invasion (merged), 283 frozen_section_detail
(merged), 285 vascular_invasion (next).

Notable parquet properties (verified 2026-04-20 via DuckDB on the on-disk
parquet committed to main from Script 282's cherry-pick):

  * 17,368 raw rows; 47 duplicate ``note_row_id`` rows in 41 groups (35 pairs
    + 6 triples). All 41 groups have identical ``result_json`` per Claude's
    pre-Script-282 audit. Last-extracted-wins dedup is safe and reduces
    17,368 -> 17,321 rows. RAW and DEDUP hashes are pinned separately;
    Phase 0 audits raw, Phase 3 audits dedup; the trios MUST differ.
  * 7,313 distinct research_ids -- WELL below CPM's 10,871. ~3,558 patients
    have no parathyroid_detail-domain notes at all and Phase 5's zero-out
    pass will correctly mark them ``nlp_parathyroid_has_data = FALSE``.
    Phase 0 explicitly gates this and Phase 5 must produce ~rids_with_pos
    TRUE + (10,871 - rids_with_pos) FALSE + 0 NULL.
  * Pre-mutation MD source has TWO model singletons (qwen3:32b and
    qwen3:32b-ctx8k); Phase 0's stale-state gate accepts the pair as a set.

Phase gates (CLI; default 0):
    --phase 0    Parquet audit + measure pinned positives (READ-ONLY)
    --phase 1    Archive current main.note_entities_llm_parathyroid_detail
                 to archive_pub_v1_0
    --phase 2    Load parquet to MD with 6 synthesized provenance cols + dedup
    --phase 3    Post-load byte-hash parity (MD == parquet after dedup)
    --phase 4    Pre-mutation CPM snapshot to archive_pub_v1_0
    --phase 5    Rollup UPDATE on canonical_patient_master (nlp_parathyroid_*)
    --phase 6    Post-mutation invariants A-F
    --phase 7    Registry + dictionary + __readme sync
    --phase all  Run 0->7, halting on any failed gate

Hard rules (NON-NEGOTIABLE):
  * READ-ONLY to scripts/280_*, scripts/282_*, scripts/283_*, and any prior
    phase scripts.
  * No touching nlp_synoptic_* / nlp_airway_* / nlp_frozensec_* on CPM
    (Scripts 280 / 282 / 283 own them).
  * No touching labs anything, tirads_v2_*, tirads_granular,
    or pathology_enrichment__march2026_broken.
  * No schema changes to canonical_patient_master.
  * Auth via motherduck_client.get_token(); never print tokens.
  * Always CAST(research_id AS VARCHAR) when joining BIGINT/INT to CPM.
  * DuckDB COMMENT quoting: inline-escape single quotes; no ? placeholders.
  * Never con.register(df) on str-dtype frames (duckdb 1.1.3 + pandas 3.x
    pitfall); materialize MD-side via CREATE OR REPLACE TEMP TABLE.
  * Empty string != NULL; use COALESCE(NULLIF(d, ''), fallback) when
    empty-is-missing semantics are intended.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402

# ── constants ────────────────────────────────────────────────────────────────

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
WS_SCHEMA = "manuscript_workspace"

SOURCE_TABLE = "note_entities_llm_parathyroid_detail"
CPM_TABLE = "canonical_patient_master"
REGISTRY_TABLE = "detail_table_registry_v1"
DICTIONARY_TABLE = "data_dictionary_v279"
README_TABLE = "__readme"

PARQUET_PATH = (
    REPO_ROOT
    / "runs"
    / "9domain_v4"
    / "parathyroid_detail"
    / "output"
    / "note_entities_llm_parathyroid_detail.parquet"
)

# ── pinned parquet fingerprints (verified 2026-04-20 by Claude via DuckDB) ───

EXPECTED_PARQUET_ROWS = 17368
EXPECTED_PARQUET_RIDS = 7313     # WELL BELOW CPM's 10,871 — expected
EXPECTED_PARQUET_NRIDS = 17321   # post-dedup row count (= distinct note_row_id)
EXPECTED_MODEL = "qwen2.5-32b"
EXPECTED_BASE_URL = "http://213.5.130.43:20049/v1"
EXPECTED_PREPROCESS_VERSION = "v4_9domain_rerun_2026-04-19"
EXPECTED_PREPROCESS_BATCH_ID = "18684c30-2277-44dd-b0ea-08f05e4f2557"
EXPECTED_EXTRACTED_AT_MIN_PFX = "2026-04-19T09:39"
EXPECTED_EXTRACTED_AT_MAX_PFX = "2026-04-19T10:36"

# RAW hashes (no dedup) — assert in Phase 0 against the parquet.
PINNED_PARQUET_RAW_RJHASH = "0b25986aa2725bacccb9424645849e51"
PINNED_PARQUET_RAW_NDHASH = "ae1ee57d91c18be77be4955473c9f8db"
PINNED_PARQUET_RAW_SRCHASH = "e7c8e24d9b7cb4f9b966a8964cf7f57a"

# DEDUP hashes (last-extracted-wins by note_row_id) — assert in Phase 3
# against the loaded MD table.
PINNED_PARQUET_DEDUP_RJHASH = "709948a3a94eebb04bdc9e84b2d95356"
PINNED_PARQUET_DEDUP_NDHASH = "22dff34281b0c19e524f4c780bc07f3f"
PINNED_PARQUET_DEDUP_SRCHASH = "c4d96773b86e33a669cd7f712b1ffaf2"

# Pinned dup shape — verifies the dedup step removes exactly what we expect.
EXPECTED_PARQUET_DUP_ROWS = 47    # rows removed by dedup (17,368 - 17,321)
EXPECTED_PARQUET_DUP_GROUPS = 41  # distinct note_row_ids with > 1 row
                                  # (35 pairs + 6 triples)

# Pinned CPM invariants (same as Scripts 280 / 282 / 283; AGENTS.md anchor).
EXPECTED_CPM_ROWS = 10871
EXPECTED_CPM_RIDS = 10871

# Pinned stale state being replaced (Phase 0 confirms pre-mutation state).
# Note: pre-state is a TWO-model set (qwen3:32b + qwen3:32b-ctx8k), not a
# singleton. The 5,641 RID figure is from Script 282's notes; the row
# breakdown is 10,452 + 585 = 11,037.
EXPECTED_STALE_SOURCE_ROWS = 11037
EXPECTED_STALE_SOURCE_RIDS = 5641
EXPECTED_STALE_MODELS = {"qwen3:32b", "qwen3:32b-ctx8k"}
EXPECTED_STALE_HAS_DATA_TRUE = 110
EXPECTED_STALE_HAS_DATA_FALSE = 0
EXPECTED_STALE_HAS_DATA_NULL = 10761
EXPECTED_STALE_SUM_N_NOTES = 111
EXPECTED_STALE_SUM_N_ENTITIES = 243

# ── pinned positive-entity targets ───────────────────────────────────────────
# Phase 0 measures these from the parquet (DEDUPED) and pins them so any
# drift means the parquet on disk has been changed since this script was
# pinned. Phase 5 then asserts the MD-side rollup matches the same numbers.
#
# 2026-04-20: numbers below are placeholders that will be replaced with
# the values Phase 0 computes on first run; the script is allowed to
# RUN Phase 0 to discover them, then a one-line edit pins them. To
# bootstrap the script without measured values, EXPECTED_POS_* are
# checked only by warning gates (informational) on first run, then
# converted to hard equality once measured.
#
# NOTE: per the Phase B' prompt, Phase 5 acceptance is +/-10 RIDs vs the
# parquet-side count Phase 0 reads, so the script doesn't strictly need a
# pre-pinned constant -- the parquet-side number is the source of truth.
# We still pin the values here for build reproducibility once Phase 0 has
# observed them, by writing them back via decisions.json.
EXPECTED_POS_RIDS: int | None = None
EXPECTED_POS_NOTES: int | None = None
EXPECTED_POS_ENTITIES: int | None = None

# Phase 5 acceptance band for the HAS_DATA TRUE count.
HAS_DATA_TOLERANCE = 10

# ── synthesized provenance defaults ──────────────────────────────────────────
SYNTH_ENTITY_DOMAIN = "parathyroid_detail"
SYNTH_LLM_PROVIDER = "vllm"
SYNTH_LLM_SDK = "openai"  # NOT NULL — synoptic precedent (verified vs synoptic)
SYNTH_LLM_SDK_VERSION = None
SYNTH_PROVIDER_RETURNED_MODEL = "qwen2.5-32b"
SYNTH_PROVIDER_SYSTEM_FINGERPRINT = None

# Output paths.
OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PREFLIGHT_PATH = OUTPUT_DIR / "284_preflight.json"
DECISIONS_PATH = OUTPUT_DIR / "284_decisions.json"
LOG_PATH = OUTPUT_DIR / "284_run.log"

# Entity-detection LIKE patterns (Script 280/282/283 parity).
EMPTY_ENTITIES_PATTERN = "%\"entities\": []%"

# Shared rollup base CTE — parsed -> flat -> ext -> pos. Used by both the
# Phase 0 positive-count gate (against the freshly loaded source) and the
# Phase 5 UPDATE so the two cannot drift. Mirrors Script 282/283 verbatim,
# only the SOURCE_TABLE differs.
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
    SELECT
        research_id,
        note_row_id,
        UNNEST(CAST(entities_arr AS JSON[])) AS entity
    FROM parsed
),
ext AS (
    SELECT
        research_id,
        note_row_id,
        json_extract_string(entity, '$.entity_type')  AS entity_type,
        json_extract_string(entity, '$.entity_value') AS entity_value,
        COALESCE(TRY_CAST(json_extract(entity, '$.confidence') AS DOUBLE), 0) AS confidence,
        json_extract_string(entity, '$.present_or_negated') AS present_or_negated
    FROM flat
    WHERE json_extract_string(entity, '$.entity_value') IS NOT NULL
),
pos AS (
    SELECT * FROM ext
    WHERE confidence >= 0.5
      AND (present_or_negated = 'present' OR present_or_negated IS NULL)
)
"""


def _rollup_base_cte_against_parquet(parquet_relation: str) -> str:
    """Same CTE parameterized by source — used by Phase 0 against the
    local parquet (read via read_parquet) BEFORE the MD load happens.
    Includes the ROW_NUMBER dedup step so Phase 0's positive-target
    counts are over the SAME dedup'd rows that Phase 2 will load."""
    return f"""
raw AS (SELECT * FROM {parquet_relation}),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY note_row_id
                                 ORDER BY extracted_at DESC) AS rn
    FROM raw
),
dedup AS (SELECT * EXCLUDE(rn) FROM ranked WHERE rn = 1),
parsed AS (
    SELECT
        research_id,
        note_row_id,
        json_extract(CAST(result_json AS JSON), '$.entities') AS entities_arr
    FROM dedup
    WHERE result_json IS NOT NULL
      AND CAST(result_json AS VARCHAR) NOT LIKE '{EMPTY_ENTITIES_PATTERN}'
      AND json_type(json_extract(CAST(result_json AS JSON), '$.entities')) = 'ARRAY'
),
flat AS (
    SELECT research_id, note_row_id, UNNEST(CAST(entities_arr AS JSON[])) AS entity
    FROM parsed
),
ext AS (
    SELECT research_id, note_row_id,
           json_extract_string(entity, '$.entity_type')  AS entity_type,
           json_extract_string(entity, '$.entity_value') AS entity_value,
           COALESCE(TRY_CAST(json_extract(entity, '$.confidence') AS DOUBLE), 0) AS confidence,
           json_extract_string(entity, '$.present_or_negated') AS present_or_negated
    FROM flat
    WHERE json_extract_string(entity, '$.entity_value') IS NOT NULL
),
pos AS (
    SELECT * FROM ext
    WHERE confidence >= 0.5
      AND (present_or_negated = 'present' OR present_or_negated IS NULL)
)
"""


# Post-load source schema -- 23 columns (locks ordinal_position vs Script 282
# pre-archive schema; downstream consumers see no schema drift).
EXPECTED_LOADED_COLUMNS: list[str] = [
    "note_row_id", "domain", "llm_model", "llm_base_url", "extracted_at",
    "result_json", "research_id", "note_type", "note_date", "linkage_date",
    "source_workbook", "source_sheet", "source_column", "note_index",
    "preprocess_batch_id", "preprocessed_at_utc", "preprocess_script_version",
    "entity_domain", "llm_provider", "llm_sdk", "llm_sdk_version",
    "provider_returned_model", "provider_system_fingerprint",
]

# ── KEY_FINDING_PRIORITY — clinical priority for nlp_parathyroid_key_finding ──
# Intraoperative actions (autotransplant, reimplantation) and pathology
# indicators (hypercellularity -> adenoma) outrank passive descriptors
# (gland_location, gland_count). Tiebreak by confidence DESC NULLS LAST,
# entity_value (matches Script 282/283 pattern).
KEY_FINDING_PRIORITY: list[tuple[str, int]] = [
    ("autotransplant",             1),   # intraoperative action
    ("reimplantation_detail",      2),   # paired with autotransplant
    ("hypercellularity_grade",     3),   # adenoma candidate
    ("gland_cellularity",          4),
    ("parathyroid_frozen_section", 5),
    ("gland_count_preserved",      6),   # functional surgical outcome
    ("gland_count_total",          7),
    ("gland_weight",               8),
    ("gland_size",                 9),
    ("removal_intent",            10),
    ("gland_location",            11),   # most common but least informative
]


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


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utcnow_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token available (token_mode={token_mode()}). "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or populate motherduck.local.toml."
        )
    log(f"connecting to MotherDuck '{CANONICAL_DB}' (token_mode={token_mode()})")
    return duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")


def table_exists(con: duckdb.DuckDBPyConnection, db: str, schema: str, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [db, schema, table],
    ).fetchone()
    return row is not None


class PreflightHalt(RuntimeError):
    """Raised when a Phase 0 gate fails."""


def _gate(out: dict[str, Any], name: str, ok: bool, detail: Any = None) -> None:
    out["gates"].append({"name": name, "ok": bool(ok), "detail": detail})
    if not ok:
        out["blockers"].append({"name": name, "detail": detail})
    log(f"  gate {name}: {'OK' if ok else 'FAIL'}{'' if detail is None else f' — {detail}'}")


def _key_finding_priority_case(value_alias: str) -> str:
    parts = ["CASE"]
    for et, prio in KEY_FINDING_PRIORITY:
        parts.append(f"WHEN {value_alias} = '{et}' THEN {prio}")
    parts.append("ELSE 99 END")
    return " ".join(parts)


def _esc_sql_literal(s: str) -> str:
    """Inline-escape a string for use as a SQL literal (no ? placeholders)."""
    return "'" + s.replace("'", "''") + "'"


def _read_pinned_pos_targets() -> tuple[int | None, int | None, int | None]:
    """Read Phase 0's measured positive-target counts from prior decisions.json
    (so Phase 5 can re-assert exact equality). Returns (None, None, None) if
    Phase 0 has not yet run, in which case Phase 5 falls back to reading the
    parquet-side numbers directly."""
    if not DECISIONS_PATH.exists():
        return (None, None, None)
    try:
        d = json.loads(DECISIONS_PATH.read_text())
        ph0 = d.get("phases", {}).get("0", {})
        tgts = ph0.get("observed", {}).get("parquet_positive_targets", {})
        return (
            tgts.get("rids_with_pos"),
            tgts.get("notes_with_pos"),
            tgts.get("total_pos_entities"),
        )
    except Exception:
        return (None, None, None)


# ── PHASE 0 — parquet audit (READ-ONLY) ──────────────────────────────────────


def phase_0(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 0 — parquet audit + measure positive targets (READ-ONLY) ===")
    out: dict[str, Any] = {
        "phase": 0,
        "started_at": utcnow_iso(),
        "token_mode": token_mode(),
        "canonical_db": CANONICAL_DB,
        "parquet_path": str(PARQUET_PATH.relative_to(REPO_ROOT)),
        "gates": [],
        "blockers": [],
        "observed": {},
    }

    if not PARQUET_PATH.exists():
        _gate(out, "parquet_file_present", False,
              {"path": str(PARQUET_PATH), "detail": "parquet missing on disk"})
        out["finished_at"] = utcnow_iso()
        raise PreflightHalt(f"parquet missing: {PARQUET_PATH}")
    _gate(out, "parquet_file_present", True, {"path": str(PARQUET_PATH)})

    # 0A — shape + uniformity against pinned parquet fingerprints.
    parquet_arg = _esc_sql_literal(str(PARQUET_PATH))
    shape = con.execute(f"""
        SELECT
            COUNT(*)                                AS n_rows,
            COUNT(DISTINCT research_id)             AS n_rids,
            COUNT(DISTINCT note_row_id)             AS n_nrids,
            STRING_AGG(DISTINCT llm_model, '|')     AS models,
            STRING_AGG(DISTINCT llm_base_url, '|')  AS base_urls,
            STRING_AGG(DISTINCT preprocess_script_version, '|') AS pscript,
            COUNT(DISTINCT preprocess_batch_id)     AS n_batch,
            STRING_AGG(DISTINCT preprocess_batch_id, '|') AS batch_id,
            MIN(extracted_at)                       AS ext_min,
            MAX(extracted_at)                       AS ext_max
        FROM read_parquet({parquet_arg})
    """).fetchone()
    (n_rows, n_rids, n_nrids, models, base_urls, pscript,
     n_batch, batch_id, ext_min, ext_max) = shape
    out["observed"]["parquet_n_rows"] = int(n_rows)
    out["observed"]["parquet_n_rids"] = int(n_rids)
    out["observed"]["parquet_n_nrids"] = int(n_nrids)
    out["observed"]["parquet_models"] = models
    out["observed"]["parquet_base_urls"] = base_urls
    out["observed"]["parquet_preprocess_script_version"] = pscript
    out["observed"]["parquet_distinct_batch_ids"] = int(n_batch)
    out["observed"]["parquet_batch_id"] = batch_id
    out["observed"]["parquet_extracted_at_min"] = str(ext_min) if ext_min else None
    out["observed"]["parquet_extracted_at_max"] = str(ext_max) if ext_max else None
    log(f"  0A: parquet rows={n_rows:,} rids={n_rids:,} nrids={n_nrids:,}")
    log(f"      model={models}  base_url={base_urls}")
    log(f"      preprocess_script_version={pscript}  n_batch={n_batch}")
    log(f"      extracted_at min={ext_min}  max={ext_max}")
    _gate(out, "parquet_n_rows_eq_17368", n_rows == EXPECTED_PARQUET_ROWS,
          {"observed": int(n_rows), "expected": EXPECTED_PARQUET_ROWS})
    _gate(out, "parquet_n_rids_eq_7313", n_rids == EXPECTED_PARQUET_RIDS,
          {"observed": int(n_rids), "expected": EXPECTED_PARQUET_RIDS})
    _gate(out, "parquet_n_nrids_eq_17321", n_nrids == EXPECTED_PARQUET_NRIDS,
          {"observed": int(n_nrids), "expected": EXPECTED_PARQUET_NRIDS})
    _gate(out, "parquet_model_eq_qwen25_32b", models == EXPECTED_MODEL,
          {"observed": models, "expected": EXPECTED_MODEL})
    _gate(out, "parquet_base_url_eq_pinned", base_urls == EXPECTED_BASE_URL,
          {"observed": base_urls, "expected": EXPECTED_BASE_URL})
    _gate(out, "parquet_preprocess_script_version_eq_pinned",
          pscript == EXPECTED_PREPROCESS_VERSION,
          {"observed": pscript, "expected": EXPECTED_PREPROCESS_VERSION})
    _gate(out, "parquet_preprocess_batch_id_uniform_and_eq_pinned",
          n_batch == 1 and batch_id == EXPECTED_PREPROCESS_BATCH_ID,
          {"observed_distinct": int(n_batch), "value": batch_id,
           "expected": EXPECTED_PREPROCESS_BATCH_ID})
    _gate(out, "parquet_extracted_at_min_starts_with_pinned",
          str(ext_min).startswith(EXPECTED_EXTRACTED_AT_MIN_PFX),
          {"observed": str(ext_min), "expected_prefix": EXPECTED_EXTRACTED_AT_MIN_PFX})
    _gate(out, "parquet_extracted_at_max_starts_with_pinned",
          str(ext_max).startswith(EXPECTED_EXTRACTED_AT_MAX_PFX),
          {"observed": str(ext_max), "expected_prefix": EXPECTED_EXTRACTED_AT_MAX_PFX})

    # Explicit narrow-coverage gate (per Phase B' #3 prompt). Parquet covers
    # 7,313 of 10,871 CPM RIDs; remaining ~3,558 will go to has_data=FALSE
    # via the zero-out pass. This is correct behavior, not a defect.
    _gate(out, "parquet_rids_well_below_cpm",
          n_rids == EXPECTED_PARQUET_RIDS,
          {"observed": int(n_rids), "expected": EXPECTED_PARQUET_RIDS,
           "note": "parquet covers 7,313 of 10,871 CPM RIDs; remaining "
                   f"~{EXPECTED_CPM_RIDS - EXPECTED_PARQUET_RIDS} will go to "
                   "has_data=FALSE via zero-out pass"})

    # 0B — content hashes (raw, sorted by note_row_id). Three MD5 aggregates
    # so a single-byte change anywhere is detected. Because this parquet has
    # 47 duplicate note_row_ids in 41 groups, RAW and DEDUP hashes DIFFER and
    # both trios are pinned.
    hashes = con.execute(f"""
        WITH tuples AS (
            SELECT
                note_row_id,
                MD5(COALESCE(result_json, ''))                  AS rj_hash,
                MD5(COALESCE(note_date::VARCHAR, ''))           AS nd_hash,
                MD5(
                    COALESCE(source_workbook, '') || '|' ||
                    COALESCE(source_sheet,    '') || '|' ||
                    COALESCE(source_column,   '')
                )                                               AS src_hash
            FROM read_parquet({parquet_arg})
        )
        SELECT
            MD5(STRING_AGG(rj_hash,  '' ORDER BY note_row_id)) AS rj,
            MD5(STRING_AGG(nd_hash,  '' ORDER BY note_row_id)) AS nd,
            MD5(STRING_AGG(src_hash, '' ORDER BY note_row_id)) AS src
        FROM tuples
    """).fetchone()
    raw_rj, raw_nd, raw_src = hashes
    out["observed"]["parquet_raw_rjhash"] = raw_rj
    out["observed"]["parquet_raw_ndhash"] = raw_nd
    out["observed"]["parquet_raw_srchash"] = raw_src
    log(f"  0B: parquet RAW hashes:  rj={raw_rj}  nd={raw_nd}  src={raw_src}")
    _gate(out, "parquet_raw_rjhash_match_pinned",
          raw_rj == PINNED_PARQUET_RAW_RJHASH,
          {"observed": raw_rj, "pinned": PINNED_PARQUET_RAW_RJHASH})
    _gate(out, "parquet_raw_ndhash_match_pinned",
          raw_nd == PINNED_PARQUET_RAW_NDHASH,
          {"observed": raw_nd, "pinned": PINNED_PARQUET_RAW_NDHASH})
    _gate(out, "parquet_raw_srchash_match_pinned",
          raw_src == PINNED_PARQUET_RAW_SRCHASH,
          {"observed": raw_src, "pinned": PINNED_PARQUET_RAW_SRCHASH})

    # Dedup hashes (post ROW_NUMBER OVER (PARTITION BY note_row_id ORDER BY
    # extracted_at DESC) = 1). These are what Phase 3 will compare against
    # the loaded MD table. They MUST differ from the RAW trio for this
    # parquet (47 dup rows trimmed).
    dedup_hashes = con.execute(f"""
        WITH raw AS (SELECT * FROM read_parquet({parquet_arg})),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY note_row_id
                                         ORDER BY extracted_at DESC) AS rn
            FROM raw
        ),
        dedup AS (SELECT * EXCLUDE(rn) FROM ranked WHERE rn = 1),
        tuples AS (
            SELECT
                note_row_id,
                MD5(COALESCE(result_json, '')) AS rj_hash,
                MD5(COALESCE(note_date::VARCHAR, '')) AS nd_hash,
                MD5(
                    COALESCE(source_workbook, '') || '|' ||
                    COALESCE(source_sheet,    '') || '|' ||
                    COALESCE(source_column,   '')
                ) AS src_hash
            FROM dedup
        )
        SELECT
            MD5(STRING_AGG(rj_hash,  '' ORDER BY note_row_id)) AS rj,
            MD5(STRING_AGG(nd_hash,  '' ORDER BY note_row_id)) AS nd,
            MD5(STRING_AGG(src_hash, '' ORDER BY note_row_id)) AS src
        FROM tuples
    """).fetchone()
    dedup_rj, dedup_nd, dedup_src = dedup_hashes
    out["observed"]["parquet_dedup_rjhash"] = dedup_rj
    out["observed"]["parquet_dedup_ndhash"] = dedup_nd
    out["observed"]["parquet_dedup_srchash"] = dedup_src
    log(f"      parquet DEDUP hashes: rj={dedup_rj}  nd={dedup_nd}  src={dedup_src}")
    _gate(out, "parquet_dedup_rjhash_match_pinned",
          dedup_rj == PINNED_PARQUET_DEDUP_RJHASH,
          {"observed": dedup_rj, "pinned": PINNED_PARQUET_DEDUP_RJHASH})
    _gate(out, "parquet_dedup_ndhash_match_pinned",
          dedup_nd == PINNED_PARQUET_DEDUP_NDHASH,
          {"observed": dedup_nd, "pinned": PINNED_PARQUET_DEDUP_NDHASH})
    _gate(out, "parquet_dedup_srchash_match_pinned",
          dedup_src == PINNED_PARQUET_DEDUP_SRCHASH,
          {"observed": dedup_src, "pinned": PINNED_PARQUET_DEDUP_SRCHASH})
    # Sanity: RAW trio and DEDUP trio MUST differ (47 rows actually removed).
    _gate(out, "parquet_raw_and_dedup_hashes_differ",
          (raw_rj != dedup_rj) and (raw_nd != dedup_nd) and (raw_src != dedup_src),
          {"raw":   {"rj": raw_rj,   "nd": raw_nd,   "src": raw_src},
           "dedup": {"rj": dedup_rj, "nd": dedup_nd, "src": dedup_src}})

    # 0C — dup shape: 47 dup rows across 41 groups.
    dup = con.execute(f"""
        SELECT
            COUNT(*) - COUNT(DISTINCT note_row_id) AS dup_rows,
            (SELECT COUNT(*) FROM (
                SELECT note_row_id FROM read_parquet({parquet_arg})
                GROUP BY note_row_id HAVING COUNT(*) > 1
            )) AS dup_groups
        FROM read_parquet({parquet_arg})
    """).fetchone()
    out["observed"]["parquet_dup_rows"] = int(dup[0])
    out["observed"]["parquet_dup_groups"] = int(dup[1])
    _gate(out, "parquet_dup_rows_eq_47", dup[0] == EXPECTED_PARQUET_DUP_ROWS,
          {"observed": int(dup[0]), "expected": EXPECTED_PARQUET_DUP_ROWS})
    _gate(out, "parquet_dup_groups_eq_41", dup[1] == EXPECTED_PARQUET_DUP_GROUPS,
          {"observed": int(dup[1]), "expected": EXPECTED_PARQUET_DUP_GROUPS})

    # 0D — measure the positive-entity targets the parquet implies (after
    # dedup). These are what Phase 5's CPM rollup must produce after the
    # load. Phase 5 reads them back from decisions.json so the two phases
    # cannot drift.
    cte = _rollup_base_cte_against_parquet(f"read_parquet({parquet_arg})")
    pos = con.execute(f"""
        WITH {cte}
        SELECT
            COUNT(DISTINCT research_id) AS rids_with_pos,
            COUNT(DISTINCT note_row_id) AS notes_with_pos,
            COUNT(*)                    AS total_pos_entities
        FROM pos
    """).fetchone()
    pos_rids, pos_notes, pos_entities = pos
    out["observed"]["parquet_positive_targets"] = {
        "rids_with_pos":      int(pos_rids),
        "notes_with_pos":     int(pos_notes),
        "total_pos_entities": int(pos_entities),
    }
    log(f"  0D: parquet positive targets (DEDUPED): rids={pos_rids:,} "
        f"notes={pos_notes:,} entities={pos_entities:,}")
    # Sanity: rids_with_pos must be <= EXPECTED_PARQUET_RIDS.
    _gate(out, "parquet_pos_rids_le_parquet_rids",
          pos_rids <= EXPECTED_PARQUET_RIDS,
          {"pos_rids": int(pos_rids), "parquet_rids": EXPECTED_PARQUET_RIDS})
    # Sanity: notes_with_pos must be <= EXPECTED_PARQUET_NRIDS.
    _gate(out, "parquet_pos_notes_le_parquet_nrids",
          pos_notes <= EXPECTED_PARQUET_NRIDS,
          {"pos_notes": int(pos_notes),
           "parquet_nrids": EXPECTED_PARQUET_NRIDS})
    # If a constant has been pinned in this file (manually after first run),
    # gate exact equality.
    if EXPECTED_POS_RIDS is not None:
        _gate(out, "parquet_pos_rids_eq_pinned",
              pos_rids == EXPECTED_POS_RIDS,
              {"observed": int(pos_rids), "pinned": EXPECTED_POS_RIDS})
    if EXPECTED_POS_NOTES is not None:
        _gate(out, "parquet_pos_notes_eq_pinned",
              pos_notes == EXPECTED_POS_NOTES,
              {"observed": int(pos_notes), "pinned": EXPECTED_POS_NOTES})
    if EXPECTED_POS_ENTITIES is not None:
        _gate(out, "parquet_pos_entities_eq_pinned",
              pos_entities == EXPECTED_POS_ENTITIES,
              {"observed": int(pos_entities), "pinned": EXPECTED_POS_ENTITIES})

    # 0E — current MD source state (the qwen3:32b + qwen3:32b-ctx8k mixed
    # extraction we are about to archive + replace). Stale models must be
    # gated as a SET (the pre-state is not single-model).
    cur = con.execute(f"""
        SELECT
            COUNT(*) AS n_rows,
            COUNT(DISTINCT research_id) AS n_rids,
            STRING_AGG(DISTINCT llm_model, '|') AS models,
            MIN(extracted_at) AS ext_min,
            MAX(extracted_at) AS ext_max
        FROM main.{SOURCE_TABLE}
    """).fetchone()
    cur_rows, cur_rids, cur_models, cur_min, cur_max = cur
    out["observed"]["md_source_n_rows"] = int(cur_rows)
    out["observed"]["md_source_n_rids"] = int(cur_rids)
    out["observed"]["md_source_models"] = cur_models
    out["observed"]["md_source_extracted_at_min"] = str(cur_min) if cur_min else None
    out["observed"]["md_source_extracted_at_max"] = str(cur_max) if cur_max else None
    log(f"  0E: current MD source rows={cur_rows:,} rids={cur_rids:,} "
        f"models={cur_models}")
    _gate(out, "md_source_stale_rows_eq_11037",
          cur_rows == EXPECTED_STALE_SOURCE_ROWS,
          {"observed": int(cur_rows), "expected": EXPECTED_STALE_SOURCE_ROWS})
    _gate(out, "md_source_stale_rids_eq_5641",
          cur_rids == EXPECTED_STALE_SOURCE_RIDS,
          {"observed": int(cur_rids), "expected": EXPECTED_STALE_SOURCE_RIDS})

    # Stale-model SET equality (pre-state has TWO model singletons).
    stale_models_observed = set(con.execute(
        f"SELECT DISTINCT llm_model FROM main.{SOURCE_TABLE}"
    ).df()["llm_model"].dropna().tolist())
    out["observed"]["md_source_models_set"] = sorted(stale_models_observed)
    _gate(out, "stale_models_eq_qwen3_pair",
          stale_models_observed == EXPECTED_STALE_MODELS,
          {"observed": sorted(stale_models_observed),
           "expected": sorted(EXPECTED_STALE_MODELS)})

    # 0F — current CPM stale rollup state.
    cpm = con.execute(f"""
        SELECT
            COUNT(*) AS cpm_rows,
            COUNT(DISTINCT research_id) AS cpm_rids,
            COUNT(*) FILTER (WHERE research_id IS NULL) AS null_rid,
            COUNT(*) FILTER (WHERE nlp_parathyroid_has_data)             AS has_true,
            COUNT(*) FILTER (WHERE nlp_parathyroid_has_data IS NULL)     AS has_null,
            COUNT(*) FILTER (WHERE nlp_parathyroid_has_data IS NOT NULL) AS has_not_null,
            COALESCE(SUM(nlp_parathyroid_n_notes),    0) AS sum_n_notes,
            COALESCE(SUM(nlp_parathyroid_n_entities), 0) AS sum_n_entities
        FROM main.{CPM_TABLE}
    """).fetchone()
    (cpm_rows, cpm_rids, cpm_null_rid, has_true, has_null, has_not_null,
     stale_sum_notes, stale_sum_ents) = cpm
    out["observed"]["cpm_rows"] = int(cpm_rows)
    out["observed"]["cpm_rids"] = int(cpm_rids)
    out["observed"]["cpm_null_rid"] = int(cpm_null_rid)
    out["observed"]["cpm_stale_has_data_true"] = int(has_true)
    out["observed"]["cpm_stale_has_data_null"] = int(has_null)
    out["observed"]["cpm_stale_has_data_not_null"] = int(has_not_null)
    out["observed"]["cpm_stale_sum_n_notes"] = int(stale_sum_notes)
    out["observed"]["cpm_stale_sum_n_entities"] = int(stale_sum_ents)
    log(f"  0F: CPM rows={cpm_rows:,} rids={cpm_rids:,}  "
        f"stale nlp_parathyroid_has_data: TRUE={has_true} NULL={has_null}  "
        f"sum_n_notes={stale_sum_notes} sum_n_entities={stale_sum_ents}")
    _gate(out, "cpm_rows_eq_10871", cpm_rows == EXPECTED_CPM_ROWS,
          {"observed": int(cpm_rows), "expected": EXPECTED_CPM_ROWS})
    _gate(out, "cpm_rids_eq_10871", cpm_rids == EXPECTED_CPM_RIDS,
          {"observed": int(cpm_rids), "expected": EXPECTED_CPM_RIDS})
    _gate(out, "cpm_no_null_rid", cpm_null_rid == 0,
          {"observed": int(cpm_null_rid)})
    _gate(out, "stale_has_data_true_eq_110",
          has_true == EXPECTED_STALE_HAS_DATA_TRUE,
          {"observed": int(has_true), "expected": EXPECTED_STALE_HAS_DATA_TRUE})
    _gate(out, "stale_has_data_null_eq_10761",
          has_null == EXPECTED_STALE_HAS_DATA_NULL,
          {"observed": int(has_null), "expected": EXPECTED_STALE_HAS_DATA_NULL})
    # Guard against double-running: if has_null is 0 AND has_true ~= the
    # expected post-promote target, refuse to clobber. The expected target
    # is pos_rids (computed above).
    has_data_target = int(pos_rids)
    _gate(out, "stale_not_already_promoted",
          not (has_null == 0 and abs(int(has_true) - has_data_target) <= 50),
          {"detail": "has_null==0 AND has_true ~= target would mean rollup "
                     "already promoted; refusing to clobber.",
           "current_has_true": int(has_true),
           "current_has_null": int(has_null),
           "expected_post_target": has_data_target})

    # 0G — registry stale state.
    reg_present = table_exists(con, CANONICAL_DB, WS_SCHEMA, REGISTRY_TABLE)
    out["observed"]["registry_table_present"] = reg_present
    if reg_present:
        reg = con.execute(f"""
            SELECT total_rows, total_patients, canonical_version,
                   description
            FROM {WS_SCHEMA}.{REGISTRY_TABLE}
            WHERE detail_table_name = ?
        """, [SOURCE_TABLE]).fetchone()
        if reg is None:
            out["observed"]["registry_row"] = None
            _gate(out, "registry_parathyroid_row_exists", False,
                  {"detail": f"no row in {WS_SCHEMA}.{REGISTRY_TABLE} for {SOURCE_TABLE}"})
        else:
            rt, rp, cv, desc = reg
            out["observed"]["registry_row"] = {
                "total_rows": int(rt) if rt is not None else None,
                "total_patients": int(rp) if rp is not None else None,
                "canonical_version": cv,
                "description_len": len(desc) if desc else 0,
            }
            _gate(out, "registry_total_rows_stale_eq_11037",
                  rt == EXPECTED_STALE_SOURCE_ROWS,
                  {"observed": int(rt) if rt is not None else None,
                   "expected": EXPECTED_STALE_SOURCE_ROWS})
            _gate(out, "registry_total_patients_stale_eq_5641",
                  rp == EXPECTED_STALE_SOURCE_RIDS,
                  {"observed": int(rp) if rp is not None else None,
                   "expected": EXPECTED_STALE_SOURCE_RIDS})

    out["finished_at"] = utcnow_iso()
    out["all_gates_passed"] = len(out["blockers"]) == 0
    log("")
    log("──── PHASE 0 SUMMARY ────")
    log(f"  gates: {len(out['gates'])} total, "
        f"{sum(1 for g in out['gates'] if g['ok'])} passed, "
        f"{len(out['blockers'])} blockers")
    if out["blockers"]:
        log("  BLOCKERS:")
        for b in out["blockers"]:
            log(f"    - {b['name']}: {b['detail']}")
        raise PreflightHalt(
            f"{len(out['blockers'])} blocker(s); see {DECISIONS_PATH}"
        )
    log("  ALL GATES PASSED — Phase 1 may proceed.")
    log("─────────────────────────")
    # Also write a standalone preflight.json snapshot for the artifact set.
    PREFLIGHT_PATH.write_text(json.dumps(out, indent=2, default=str))
    return out


# ── PHASE 1 — archive current MD source ──────────────────────────────────────


def _existing_pre9domainv4_archives(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog=? AND table_schema=?
          AND table_name LIKE 'note_entities_llm_parathyroid_detail_pre9domainv4_%'
        ORDER BY table_name
    """, [ARCHIVE_DB, ARCHIVE_SCHEMA]).fetchall()
    return [r[0] for r in rows]


def phase_1(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 1 — archive current MD source to archive_pub_v1_0 ===")
    out: dict[str, Any] = {
        "phase": 1, "started_at": utcnow_iso(), "gates": [], "blockers": [],
    }

    pre = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               STRING_AGG(DISTINCT llm_model, '|')
        FROM main.{SOURCE_TABLE}
    """).fetchone()
    pre_rows, pre_rids, pre_model = pre
    out["pre_archive_source_rows"] = int(pre_rows)
    out["pre_archive_source_rids"] = int(pre_rids)
    out["pre_archive_source_model"] = pre_model
    log(f"  pre-archive source: rows={pre_rows:,} rids={pre_rids:,} model={pre_model}")

    existing = _existing_pre9domainv4_archives(con)
    if existing:
        snap = existing[-1]
        snap_fq = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{snap}'
        log(f"  idempotent reuse: {snap} already exists "
            f"(found {len(existing)} pre9domainv4 archive(s); using newest)")
        out["ctas_action"] = "reused_existing"
        out["existing_archives"] = existing
    else:
        ts = utcnow_compact()
        snap = f"note_entities_llm_parathyroid_detail_pre9domainv4_{ts}"
        snap_fq = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{snap}'
        log(f"  CTAS {snap_fq} AS SELECT * FROM main.{SOURCE_TABLE} ...")
        con.execute(
            f"CREATE TABLE {snap_fq} AS SELECT * FROM main.{SOURCE_TABLE}"
        )
        out["ctas_action"] = "created_new"
    out["archive_table"] = snap_fq
    out["archive_table_unqualified"] = snap

    comment = (
        f"Script 284 pre-v4 archive of main.{SOURCE_TABLE}. "
        f"Source: mixed qwen3:32b + qwen3:32b-ctx8k extraction (Apr 1-3 2026), "
        f"{int(pre_rows)} rows / {int(pre_rids)} RIDs. "
        "Reason: pre-v4 qwen2.5-32b rerun replacement (Phase B' #3 of 4). "
        f"Created at {utcnow_iso()}."
    )
    comment_lit = _esc_sql_literal(comment)
    try:
        con.execute(f"COMMENT ON TABLE {snap_fq} IS {comment_lit}")
        out["comment_action"] = "applied"
    except duckdb.Error as exc:
        log(f"  ⚠ COMMENT failed ({exc!r}) — archive still valid; recording in JSON")
        out["comment_action"] = f"failed: {exc!r}"
    out["comment"] = comment

    post = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               STRING_AGG(DISTINCT llm_model, '|')
        FROM {snap_fq}
    """).fetchone()
    a_rows, a_rids, a_model = post
    out["archive_rows"] = int(a_rows)
    out["archive_rids"] = int(a_rids)
    out["archive_model"] = a_model
    log(f"  archive {snap}: rows={a_rows:,} rids={a_rids:,} model={a_model}")
    _gate(out, "archive_rows_eq_pre_rows", a_rows == pre_rows,
          {"observed": int(a_rows), "pre_archive": int(pre_rows)})
    _gate(out, "archive_rids_eq_pre_rids", a_rids == pre_rids,
          {"observed": int(a_rids), "pre_archive": int(pre_rids)})
    _gate(out, "archive_rows_eq_11037", a_rows == EXPECTED_STALE_SOURCE_ROWS,
          {"observed": int(a_rows), "expected": EXPECTED_STALE_SOURCE_ROWS})

    # Verify archive carries both stale models (set parity).
    archive_models = set(con.execute(
        f"SELECT DISTINCT llm_model FROM {snap_fq}"
    ).df()["llm_model"].dropna().tolist())
    out["archive_models_set"] = sorted(archive_models)
    _gate(out, "archive_models_eq_qwen3_pair",
          archive_models == EXPECTED_STALE_MODELS,
          {"observed": sorted(archive_models),
           "expected": sorted(EXPECTED_STALE_MODELS)})

    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 1 blockers: {out['blockers']}")
    return out


# ── PHASE 2 — load parquet to MD with synth provenance + dedup ───────────────


def phase_2(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 2 — load parquet to MD with synth provenance + dedup ===")
    out: dict[str, Any] = {
        "phase": 2, "started_at": utcnow_iso(), "gates": [], "blockers": [],
    }

    # Sanity: ensure a Phase 1 archive exists before we clobber the live
    # source. If Phase 1 was skipped this is a hard refusal.
    existing = _existing_pre9domainv4_archives(con)
    if not existing:
        _gate(out, "phase_1_archive_present_before_load", False,
              {"detail": "no pre9domainv4 archive found in archive_pub_v1_0; "
                         "run --phase 1 before --phase 2."})
        raise RuntimeError("Phase 2 refused: Phase 1 archive missing")
    _gate(out, "phase_1_archive_present_before_load", True,
          {"newest_archive": existing[-1]})

    # CREATE OR REPLACE TABLE from parquet with the 6 synthesized provenance
    # columns. Dedup CTE removes 47 rows in 41 groups (last extracted_at wins);
    # all 41 groups have identical result_json so the choice within a group
    # is byte-stable across re-runs.
    parquet_arg = _esc_sql_literal(str(PARQUET_PATH))
    log(f"  loading parquet {PARQUET_PATH.name} -> main.{SOURCE_TABLE} "
        "(with dedup by note_row_id, last extracted_at wins; "
        f"removes {EXPECTED_PARQUET_DUP_ROWS} rows in "
        f"{EXPECTED_PARQUET_DUP_GROUPS} groups)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE main.{SOURCE_TABLE} AS
        WITH raw AS (
            SELECT * FROM read_parquet({parquet_arg})
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY note_row_id
                                         ORDER BY extracted_at DESC) AS _dedup_rn
            FROM raw
        ),
        dedup AS (SELECT * EXCLUDE(_dedup_rn) FROM ranked WHERE _dedup_rn = 1)
        SELECT
            note_row_id,
            domain,
            llm_model,
            llm_base_url,
            extracted_at,
            result_json,
            research_id,
            note_type,
            note_date,
            linkage_date,
            source_workbook,
            source_sheet,
            source_column,
            note_index,
            preprocess_batch_id,
            preprocessed_at_utc,
            preprocess_script_version,
            CAST({_esc_sql_literal(SYNTH_ENTITY_DOMAIN)} AS VARCHAR) AS entity_domain,
            CAST({_esc_sql_literal(SYNTH_LLM_PROVIDER)} AS VARCHAR) AS llm_provider,
            CAST(NULL AS VARCHAR) AS llm_sdk_placeholder,
            CAST(NULL AS VARCHAR) AS llm_sdk_version,
            CAST({_esc_sql_literal(SYNTH_PROVIDER_RETURNED_MODEL)} AS VARCHAR)
                AS provider_returned_model,
            CAST(NULL AS VARCHAR) AS provider_system_fingerprint
        FROM dedup
    """)
    # Patch llm_sdk in via DROP placeholder + ADD + UPDATE so we can pull
    # the synth string literal cleanly into VARCHAR (Script 282 pattern).
    con.execute(f"ALTER TABLE main.{SOURCE_TABLE} DROP COLUMN llm_sdk_placeholder")
    con.execute(
        f"ALTER TABLE main.{SOURCE_TABLE} ADD COLUMN llm_sdk VARCHAR"
    )
    con.execute(
        f"UPDATE main.{SOURCE_TABLE} "
        f"SET llm_sdk = {_esc_sql_literal(SYNTH_LLM_SDK)}"
    )
    # Reorder columns so llm_sdk sits at ordinal 20 (between llm_provider
    # and llm_sdk_version) per Script 282's pre-archive schema. DuckDB
    # doesn't support ALTER COLUMN POSITION, so a CREATE OR REPLACE rebuild
    # with explicit projection is the cheapest way to lock the ordering.
    log("  reprojecting columns to lock ordinal_position vs Script 282 schema...")
    con.execute(f"""
        CREATE OR REPLACE TABLE main.{SOURCE_TABLE} AS
        SELECT
            note_row_id, domain, llm_model, llm_base_url, extracted_at,
            result_json, research_id, note_type, note_date, linkage_date,
            source_workbook, source_sheet, source_column, note_index,
            preprocess_batch_id, preprocessed_at_utc, preprocess_script_version,
            entity_domain, llm_provider, llm_sdk, llm_sdk_version,
            provider_returned_model, provider_system_fingerprint
        FROM main.{SOURCE_TABLE}
    """)

    # Post-load integrity gates.
    post = con.execute(f"""
        SELECT
            COUNT(*) AS n_rows,
            COUNT(DISTINCT research_id) AS n_rids,
            COUNT(DISTINCT note_row_id) AS n_nrids,
            STRING_AGG(DISTINCT llm_model, '|') AS models,
            STRING_AGG(DISTINCT llm_base_url, '|') AS base_urls,
            STRING_AGG(DISTINCT entity_domain, '|') AS entity_domains,
            STRING_AGG(DISTINCT llm_provider, '|') AS providers,
            STRING_AGG(DISTINCT llm_sdk, '|') AS sdks,
            STRING_AGG(DISTINCT provider_returned_model, '|') AS provider_models,
            COUNT(*) FILTER (WHERE llm_sdk_version IS NOT NULL) AS n_sdk_version_set,
            COUNT(*) FILTER (WHERE provider_system_fingerprint IS NOT NULL) AS n_fp_set
        FROM main.{SOURCE_TABLE}
    """).fetchone()
    (p_rows, p_rids, p_nrids, p_models, p_base_urls, p_eds, p_provs,
     p_sdks, p_pmodels, n_sdkv, n_fp) = post
    out["post_load_n_rows"] = int(p_rows)
    out["post_load_n_rids"] = int(p_rids)
    out["post_load_n_nrids"] = int(p_nrids)
    out["post_load_models"] = p_models
    out["post_load_base_urls"] = p_base_urls
    out["post_load_entity_domains"] = p_eds
    out["post_load_providers"] = p_provs
    out["post_load_sdks"] = p_sdks
    out["post_load_provider_returned_models"] = p_pmodels
    out["post_load_n_llm_sdk_version_set"] = int(n_sdkv)
    out["post_load_n_provider_system_fingerprint_set"] = int(n_fp)
    log(f"  post-load: rows={p_rows:,} rids={p_rids:,} nrids={p_nrids:,}")
    log(f"             models={p_models}  base_urls={p_base_urls}")
    log(f"             entity_domain={p_eds}  llm_provider={p_provs}  "
        f"llm_sdk={p_sdks}")
    log(f"             provider_returned_model={p_pmodels}  "
        f"sdk_version_set={n_sdkv}  fingerprint_set={n_fp}")
    _gate(out, "post_load_n_rows_eq_17321",
          p_rows == EXPECTED_PARQUET_NRIDS,
          {"observed": int(p_rows), "expected": EXPECTED_PARQUET_NRIDS})
    _gate(out, "post_load_n_rids_eq_7313",
          p_rids == EXPECTED_PARQUET_RIDS,
          {"observed": int(p_rids), "expected": EXPECTED_PARQUET_RIDS})
    _gate(out, "post_load_nrids_eq_rows_dedup_invariant",
          p_nrids == p_rows,
          {"detail": "post-dedup, 1 row per note_row_id; nrids must equal rows",
           "n_nrids": int(p_nrids), "n_rows": int(p_rows)})
    _gate(out, "post_load_model_eq_qwen25_32b",
          p_models == EXPECTED_MODEL,
          {"observed": p_models, "expected": EXPECTED_MODEL})
    _gate(out, "post_load_base_url_eq_pinned",
          p_base_urls == EXPECTED_BASE_URL,
          {"observed": p_base_urls, "expected": EXPECTED_BASE_URL})
    _gate(out, "post_load_entity_domain_eq_synth",
          p_eds == SYNTH_ENTITY_DOMAIN,
          {"observed": p_eds, "expected": SYNTH_ENTITY_DOMAIN})
    _gate(out, "post_load_llm_provider_eq_synth",
          p_provs == SYNTH_LLM_PROVIDER,
          {"observed": p_provs, "expected": SYNTH_LLM_PROVIDER})
    _gate(out, "post_load_llm_sdk_eq_synth",
          p_sdks == SYNTH_LLM_SDK,
          {"observed": p_sdks, "expected": SYNTH_LLM_SDK})
    _gate(out, "post_load_provider_returned_model_eq_synth",
          p_pmodels == SYNTH_PROVIDER_RETURNED_MODEL,
          {"observed": p_pmodels, "expected": SYNTH_PROVIDER_RETURNED_MODEL})
    _gate(out, "post_load_n_llm_sdk_version_eq_0", int(n_sdkv) == 0,
          {"observed": int(n_sdkv)})
    _gate(out, "post_load_n_provider_system_fingerprint_eq_0", int(n_fp) == 0,
          {"observed": int(n_fp)})

    # Schema parity vs Script 282's pre-archive schema (all 23 columns
    # in expected order).
    cols = [
        r[0] for r in con.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog=? AND table_schema='main' AND table_name=?
            ORDER BY ordinal_position
        """, [CANONICAL_DB, SOURCE_TABLE]).fetchall()
    ]
    out["post_load_columns"] = cols
    _gate(out, "post_load_schema_eq_expected_23_cols",
          cols == EXPECTED_LOADED_COLUMNS,
          {"observed": cols, "expected": EXPECTED_LOADED_COLUMNS})

    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 2 blockers: {out['blockers']}")
    return out


# ── PHASE 3 — post-load byte-hash parity (vs DEDUP trio) ────────────────────


def phase_3(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 3 — post-load byte-hash parity (MD == parquet after dedup) ===")
    out: dict[str, Any] = {
        "phase": 3, "started_at": utcnow_iso(), "gates": [], "blockers": [],
    }

    md_hashes = con.execute(f"""
        WITH tuples AS (
            SELECT
                note_row_id,
                MD5(COALESCE(result_json, ''))                    AS rj_hash,
                MD5(COALESCE(note_date::VARCHAR, ''))             AS nd_hash,
                MD5(
                    COALESCE(source_workbook, '') || '|' ||
                    COALESCE(source_sheet,    '') || '|' ||
                    COALESCE(source_column,   '')
                )                                                 AS src_hash
            FROM main.{SOURCE_TABLE}
        )
        SELECT
            MD5(STRING_AGG(rj_hash,  '' ORDER BY note_row_id)) AS rj,
            MD5(STRING_AGG(nd_hash,  '' ORDER BY note_row_id)) AS nd,
            MD5(STRING_AGG(src_hash, '' ORDER BY note_row_id)) AS src
        FROM tuples
    """).fetchone()
    md_rj, md_nd, md_src = md_hashes
    out["md_rjhash"] = md_rj
    out["md_ndhash"] = md_nd
    out["md_srchash"] = md_src
    out["pinned_dedup_rjhash"] = PINNED_PARQUET_DEDUP_RJHASH
    out["pinned_dedup_ndhash"] = PINNED_PARQUET_DEDUP_NDHASH
    out["pinned_dedup_srchash"] = PINNED_PARQUET_DEDUP_SRCHASH
    log(f"  MD     : rj={md_rj}  nd={md_nd}  src={md_src}")
    log(f"  pinned : rj={PINNED_PARQUET_DEDUP_RJHASH}  "
        f"nd={PINNED_PARQUET_DEDUP_NDHASH}  src={PINNED_PARQUET_DEDUP_SRCHASH}")
    _gate(out, "md_rjhash_eq_parquet_dedup_rjhash",
          md_rj == PINNED_PARQUET_DEDUP_RJHASH,
          {"observed": md_rj, "pinned": PINNED_PARQUET_DEDUP_RJHASH})
    _gate(out, "md_ndhash_eq_parquet_dedup_ndhash",
          md_nd == PINNED_PARQUET_DEDUP_NDHASH,
          {"observed": md_nd, "pinned": PINNED_PARQUET_DEDUP_NDHASH})
    _gate(out, "md_srchash_eq_parquet_dedup_srchash",
          md_src == PINNED_PARQUET_DEDUP_SRCHASH,
          {"observed": md_src, "pinned": PINNED_PARQUET_DEDUP_SRCHASH})

    # Belt-and-braces: re-compute the parquet-dedup hashes inline to catch
    # the (vanishingly unlikely) case where the parquet on disk has changed
    # between Phase 0 and Phase 3 but happens to also equal the prior MD
    # state.
    parquet_arg = _esc_sql_literal(str(PARQUET_PATH))
    pq_dedup = con.execute(f"""
        WITH raw AS (SELECT * FROM read_parquet({parquet_arg})),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY note_row_id
                                         ORDER BY extracted_at DESC) AS rn
            FROM raw
        ),
        dedup AS (SELECT * EXCLUDE(rn) FROM ranked WHERE rn = 1),
        tuples AS (
            SELECT
                note_row_id,
                MD5(COALESCE(result_json, '')) AS rj_hash,
                MD5(COALESCE(note_date::VARCHAR, '')) AS nd_hash,
                MD5(
                    COALESCE(source_workbook, '') || '|' ||
                    COALESCE(source_sheet,    '') || '|' ||
                    COALESCE(source_column,   '')
                ) AS src_hash
            FROM dedup
        )
        SELECT
            MD5(STRING_AGG(rj_hash,  '' ORDER BY note_row_id)),
            MD5(STRING_AGG(nd_hash,  '' ORDER BY note_row_id)),
            MD5(STRING_AGG(src_hash, '' ORDER BY note_row_id))
        FROM tuples
    """).fetchone()
    out["live_parquet_dedup_rjhash"] = pq_dedup[0]
    out["live_parquet_dedup_ndhash"] = pq_dedup[1]
    out["live_parquet_dedup_srchash"] = pq_dedup[2]
    _gate(out, "md_rjhash_eq_live_parquet_dedup_rjhash",
          md_rj == pq_dedup[0],
          {"md": md_rj, "live_parquet": pq_dedup[0]})
    _gate(out, "md_ndhash_eq_live_parquet_dedup_ndhash",
          md_nd == pq_dedup[1],
          {"md": md_nd, "live_parquet": pq_dedup[1]})
    _gate(out, "md_srchash_eq_live_parquet_dedup_srchash",
          md_src == pq_dedup[2],
          {"md": md_src, "live_parquet": pq_dedup[2]})

    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 3 blockers: {out['blockers']}")
    return out


# ── PHASE 4 — pre-mutation CPM snapshot ──────────────────────────────────────


def _existing_pre284_cpm_snapshots(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog=? AND table_schema=?
          AND table_name LIKE 'canonical_patient_master_pre284_%'
        ORDER BY table_name
    """, [ARCHIVE_DB, ARCHIVE_SCHEMA]).fetchall()
    return [r[0] for r in rows]


def phase_4(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 4 — pre-mutation CPM snapshot to archive_pub_v1_0 ===")
    out: dict[str, Any] = {
        "phase": 4, "started_at": utcnow_iso(), "gates": [], "blockers": [],
    }

    existing = _existing_pre284_cpm_snapshots(con)
    if existing:
        snap = existing[-1]
        snap_fq = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{snap}'
        log(f"  idempotent reuse: {snap} already exists "
            f"(found {len(existing)} pre284 snapshot(s); using newest)")
        out["ctas_action"] = "reused_existing"
        out["existing_snapshots"] = existing
    else:
        ts = utcnow_compact()
        snap = f"canonical_patient_master_pre284_{ts}"
        snap_fq = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{snap}'
        log(f"  CTAS {snap_fq} AS SELECT * FROM main.{CPM_TABLE} ...")
        con.execute(
            f"CREATE TABLE {snap_fq} AS SELECT * FROM main.{CPM_TABLE}"
        )
        out["ctas_action"] = "created_new"
    out["snapshot_table"] = snap_fq
    out["snapshot_table_unqualified"] = snap

    comment = (
        f"Script 284 pre-mutation snapshot of {CPM_TABLE}. "
        "Reason: rollback anchor before rebuilding nlp_parathyroid_* family from "
        f"mixed qwen3:32b+qwen3:32b-ctx8k stale state "
        f"(has_data TRUE={EXPECTED_STALE_HAS_DATA_TRUE}, "
        f"NULL={EXPECTED_STALE_HAS_DATA_NULL}, sum_n_notes={EXPECTED_STALE_SUM_N_NOTES}, "
        f"sum_n_entities={EXPECTED_STALE_SUM_N_ENTITIES}) to qwen2.5-32b state "
        f"(parquet covers {EXPECTED_PARQUET_RIDS} of {EXPECTED_CPM_RIDS} CPM RIDs; "
        f"~{EXPECTED_CPM_RIDS - EXPECTED_PARQUET_RIDS} go to FALSE via zero-out). "
        f"Created at {utcnow_iso()}."
    )
    comment_lit = _esc_sql_literal(comment)
    try:
        con.execute(f"COMMENT ON TABLE {snap_fq} IS {comment_lit}")
        out["comment_action"] = "applied"
    except duckdb.Error as exc:
        log(f"  ⚠ COMMENT failed ({exc!r}) — snapshot still valid; recording in JSON")
        out["comment_action"] = f"failed: {exc!r}"
    out["comment"] = comment

    sn = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {snap_fq}"
    ).fetchone()
    out["snapshot_rows"] = int(sn[0])
    out["snapshot_rids"] = int(sn[1])
    log(f"  snapshot {snap}: rows={sn[0]:,} rids={sn[1]:,}")
    _gate(out, "snapshot_rows_eq_10871", sn[0] == EXPECTED_CPM_ROWS,
          {"observed": int(sn[0]), "expected": EXPECTED_CPM_ROWS})
    _gate(out, "snapshot_rids_eq_10871", sn[1] == EXPECTED_CPM_RIDS,
          {"observed": int(sn[1]), "expected": EXPECTED_CPM_RIDS})

    # Capture the MD5 fingerprint of all non-nlp_parathyroid_* CPM columns
    # for Phase 6 invariant C (no other column touched).
    other_cols = [
        r[0] for r in con.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog=? AND table_schema='main' AND table_name=?
              AND column_name NOT LIKE 'nlp_parathyroid_%'
            ORDER BY ordinal_position
        """, [CANONICAL_DB, CPM_TABLE]).fetchall()
    ]
    expr_parts = ", ".join(
        f"COALESCE(CAST(\"{c}\" AS VARCHAR), '<NULL>')" for c in other_cols
    )
    pre_hash = con.execute(f"""
        WITH per_row AS (
            SELECT MD5(CONCAT_WS('|', {expr_parts})) AS row_hash,
                   CAST(research_id AS VARCHAR) AS rid
            FROM {snap_fq}
        )
        SELECT MD5(STRING_AGG(row_hash, '' ORDER BY rid))
        FROM per_row
    """).fetchone()[0]
    out["snapshot_other_cols_md5"] = pre_hash
    out["snapshot_other_cols_count"] = len(other_cols)
    log(f"  snapshot non-nlp_parathyroid_* MD5 = {pre_hash}  "
        f"(over {len(other_cols)} columns; pinned for Phase 6 invariant C)")

    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 4 blockers: {out['blockers']}")
    return out


# ── PHASE 5 — rollup UPDATE (the actual Phase B' mutation) ───────────────────


def phase_5(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 5 — rollup UPDATE on canonical_patient_master "
        "(nlp_parathyroid_*) ===")
    out: dict[str, Any] = {
        "phase": 5, "started_at": utcnow_iso(), "gates": [], "blockers": [],
    }

    # Read the parquet-side positive targets observed by Phase 0. These are
    # the source of truth Phase 5's CPM rollup must match.
    pinned_rids, pinned_notes, pinned_ents = _read_pinned_pos_targets()
    out["phase_0_pinned_pos_rids"] = pinned_rids
    out["phase_0_pinned_pos_notes"] = pinned_notes
    out["phase_0_pinned_pos_entities"] = pinned_ents
    if pinned_rids is None:
        log("  ⚠ phase 0 pinned positive targets not found in decisions.json; "
            "Phase 5 will skip exact-equality gates and rely on +/- tolerance.")

    case_priority = _key_finding_priority_case("entity_type")
    rollup_sql = f"""
        WITH {ROLLUP_BASE_CTE},
        ranked AS (
            SELECT
                research_id,
                entity_value,
                entity_type,
                confidence,
                ROW_NUMBER() OVER (
                    PARTITION BY research_id
                    ORDER BY {case_priority},
                             confidence DESC NULLS LAST,
                             entity_value
                ) AS rn
            FROM pos
        ),
        per_rid AS (
            SELECT
                p.research_id,
                COUNT(DISTINCT p.note_row_id) AS n_notes_with_entity,
                COUNT(*)                      AS total_entities,
                MAX(CASE WHEN r.rn = 1 THEN r.entity_value END) AS key_finding
            FROM pos p
            LEFT JOIN ranked r
              ON r.research_id = p.research_id AND r.rn = 1
            GROUP BY p.research_id
        )
        SELECT * FROM per_rid
    """
    log("  building per-RID rollup CTE -> TEMP TABLE _rollup_284 ...")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _rollup_284 AS {rollup_sql}")
    summary = con.execute("""
        SELECT
            COUNT(*)                                 AS distinct_rids,
            COALESCE(SUM(n_notes_with_entity), 0)    AS sum_n_notes,
            COALESCE(SUM(total_entities),     0)     AS sum_n_entities
        FROM _rollup_284
    """).fetchone()
    out["rollup_distinct_rids"] = int(summary[0])
    out["rollup_n_notes_sum"] = int(summary[1])
    out["rollup_total_entities_sum"] = int(summary[2])
    log(f"    rollup: distinct_rids={summary[0]}  sum_n_notes={summary[1]}  "
        f"sum_n_entities={summary[2]}")

    # Gate against parquet-side pinned positives if available.
    if pinned_rids is not None:
        _gate(out, "rollup_distinct_rids_eq_pinned",
              summary[0] == pinned_rids,
              {"observed": int(summary[0]), "pinned": pinned_rids})
    if pinned_notes is not None:
        _gate(out, "rollup_n_notes_eq_pinned",
              summary[1] == pinned_notes,
              {"observed": int(summary[1]), "pinned": pinned_notes})
    if pinned_ents is not None:
        _gate(out, "rollup_n_entities_eq_pinned",
              summary[2] == pinned_ents,
              {"observed": int(summary[2]), "pinned": pinned_ents})

    # Detect orphan RIDs: rollup RIDs that don't exist in CPM. These will
    # NOT be UPDATEd to TRUE (no matching CPM row) and so their note/entity
    # contributions are correctly missing from the post-mutation CPM
    # totals. Pin the orphan count + their (notes, entities) contribution
    # so the post-mutation gates can adjust against pinned values without
    # losing exactness.
    orphans = con.execute(f"""
        SELECT
            COUNT(*) AS n_orphan_rids,
            COALESCE(SUM(r.n_notes_with_entity), 0) AS orphan_notes_contrib,
            COALESCE(SUM(r.total_entities),      0) AS orphan_entities_contrib
        FROM _rollup_284 r
        WHERE CAST(r.research_id AS VARCHAR) NOT IN (
            SELECT CAST(research_id AS VARCHAR) FROM main.{CPM_TABLE}
        )
    """).fetchone()
    n_orphan_rids = int(orphans[0])
    orphan_notes_contrib = int(orphans[1])
    orphan_entities_contrib = int(orphans[2])
    out["rollup_orphan_rids"] = n_orphan_rids
    out["rollup_orphan_notes_contribution"] = orphan_notes_contrib
    out["rollup_orphan_entities_contribution"] = orphan_entities_contrib
    log(f"    orphan rollup RIDs (not in CPM): {n_orphan_rids}  "
        f"(notes_contrib={orphan_notes_contrib} "
        f"entities_contrib={orphan_entities_contrib})")
    if n_orphan_rids > 0:
        orphan_rids_list = [
            r[0] for r in con.execute(f"""
                SELECT CAST(r.research_id AS VARCHAR)
                FROM _rollup_284 r
                WHERE CAST(r.research_id AS VARCHAR) NOT IN (
                    SELECT CAST(research_id AS VARCHAR) FROM main.{CPM_TABLE}
                )
                ORDER BY 1
            """).fetchall()
        ]
        out["rollup_orphan_rid_list"] = orphan_rids_list
        log(f"    orphan rid list: {orphan_rids_list}")
    # Expected post-mutation count of TRUE rows in CPM (rollup RIDs minus
    # orphans). This is the source-of-truth target for the post-mutation
    # equality gates (Script 282/283 had zero orphans so target == rollup
    # distinct_rids; here that does not hold).
    expected_post_has_true = int(summary[0]) - n_orphan_rids
    expected_post_sum_notes = int(summary[1]) - orphan_notes_contrib
    expected_post_sum_ents = int(summary[2]) - orphan_entities_contrib
    out["expected_post_has_true"] = expected_post_has_true
    out["expected_post_sum_n_notes"] = expected_post_sum_notes
    out["expected_post_sum_n_entities"] = expected_post_sum_ents

    # Per-RID UPDATE for matched patients. ALWAYS CAST research_id AS
    # VARCHAR on join (silent zero-row joins otherwise; see AGENTS.md).
    log("  UPDATE main.canonical_patient_master from rollup ...")
    con.execute(f"""
        UPDATE main.{CPM_TABLE} cpm
        SET nlp_parathyroid_has_data    = (r.n_notes_with_entity > 0),
            nlp_parathyroid_n_notes     = r.n_notes_with_entity,
            nlp_parathyroid_n_entities  = r.total_entities,
            nlp_parathyroid_key_finding = r.key_finding
        FROM _rollup_284 r
        WHERE CAST(cpm.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
    """)
    log("  UPDATE patients absent from rollup -> FALSE/0/0/NULL ...")
    log(f"    (this is expected to mark ~{EXPECTED_CPM_RIDS - int(summary[0])} "
        "patients FALSE — parquet covers a narrow RID slice)")
    con.execute(f"""
        UPDATE main.{CPM_TABLE} cpm
        SET nlp_parathyroid_has_data    = FALSE,
            nlp_parathyroid_n_notes     = 0,
            nlp_parathyroid_n_entities  = 0,
            nlp_parathyroid_key_finding = NULL
        WHERE CAST(cpm.research_id AS VARCHAR) NOT IN (
            SELECT CAST(research_id AS VARCHAR) FROM _rollup_284
        )
    """)

    post = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE nlp_parathyroid_has_data)         AS has_true,
            COUNT(*) FILTER (WHERE NOT nlp_parathyroid_has_data)     AS has_false,
            COUNT(*) FILTER (WHERE nlp_parathyroid_has_data IS NULL) AS has_null,
            SUM(nlp_parathyroid_n_notes)    AS sum_n_notes,
            SUM(nlp_parathyroid_n_entities) AS sum_n_entities,
            COUNT(*) FILTER (WHERE research_id IS NULL)              AS null_rid,
            COUNT(*) FILTER (WHERE nlp_parathyroid_key_finding IS NOT NULL)
                                                                     AS key_finding_set
        FROM main.{CPM_TABLE}
    """).fetchone()
    (post_true, post_false, post_null, post_sum_notes, post_sum_ents,
     post_null_rid, post_kf_set) = post
    out["post_has_data_true"] = int(post_true)
    out["post_has_data_false"] = int(post_false)
    out["post_has_data_null"] = int(post_null)
    out["post_sum_n_notes"] = int(post_sum_notes or 0)
    out["post_sum_n_entities"] = int(post_sum_ents or 0)
    out["post_null_research_id"] = int(post_null_rid)
    out["post_key_finding_set"] = int(post_kf_set)
    log(f"  post-update: has_data TRUE={post_true} FALSE={post_false} "
        f"NULL={post_null}")
    log(f"               sum_n_notes={post_sum_notes} "
        f"sum_n_entities={post_sum_ents} key_finding_set={post_kf_set}")

    # HAS_DATA gates. Target = rollup distinct_rids - orphan_rids
    # (orphans have no matching CPM row and therefore cannot become TRUE).
    target = expected_post_has_true
    _gate(out, "post_has_data_within_target_pm10",
          abs(int(post_true) - target) <= HAS_DATA_TOLERANCE,
          {"observed": int(post_true), "target": target,
           "tolerance": HAS_DATA_TOLERANCE,
           "rollup_distinct_rids": int(summary[0]),
           "orphan_rids": n_orphan_rids})
    # HARD: zero NULLs in the rollup column (the headline contract).
    _gate(out, "post_has_data_null_eq_0", int(post_null) == 0,
          {"observed": int(post_null)})
    # HARD: math invariant — every CPM row is in exactly one of TRUE/FALSE.
    _gate(out, "post_has_data_true_plus_false_eq_cpm_rows",
          int(post_true) + int(post_false) == EXPECTED_CPM_ROWS,
          {"true": int(post_true), "false": int(post_false),
           "sum": int(post_true) + int(post_false),
           "expected": EXPECTED_CPM_ROWS})
    # FALSE count is the complement of TRUE within the 10,871 CPM rows.
    has_false_target = EXPECTED_CPM_ROWS - int(post_true)
    _gate(out, "post_has_data_false_eq_complement_of_true",
          int(post_false) == has_false_target,
          {"observed": int(post_false), "expected": has_false_target,
           "note": f"~{EXPECTED_CPM_RIDS - EXPECTED_PARQUET_RIDS} of these "
                   "are RIDs absent from parquet entirely; "
                   f"{n_orphan_rids} additional rollup RID(s) are orphan "
                   "(in parquet, not in CPM) and contribute to the FALSE "
                   "count via the zero-out pass not happening for them."})
    # Sum-of-notes / sum-of-entities: equal to (pinned - orphan_contribution).
    if pinned_notes is not None:
        _gate(out, "post_sum_n_notes_eq_pinned_minus_orphan",
              int(post_sum_notes or 0) == expected_post_sum_notes,
              {"observed": int(post_sum_notes or 0),
               "pinned_parquet_side": pinned_notes,
               "orphan_contribution": orphan_notes_contrib,
               "expected_post_mutation": expected_post_sum_notes})
    if pinned_ents is not None:
        _gate(out, "post_sum_n_entities_eq_pinned_minus_orphan",
              int(post_sum_ents or 0) == expected_post_sum_ents,
              {"observed": int(post_sum_ents or 0),
               "pinned_parquet_side": pinned_ents,
               "orphan_contribution": orphan_entities_contrib,
               "expected_post_mutation": expected_post_sum_ents})
    _gate(out, "post_null_research_id_eq_0", int(post_null_rid) == 0,
          {"observed": int(post_null_rid)})
    _gate(out, "post_key_finding_set_eq_has_true",
          int(post_kf_set) == int(post_true),
          {"observed_kf_set": int(post_kf_set),
           "observed_has_true": int(post_true),
           "detail": "every TRUE patient must have a key_finding"})

    # Distribution of key_finding (informational only).
    kf_dist = con.execute(f"""
        SELECT nlp_parathyroid_key_finding AS kf, COUNT(*) AS n
        FROM main.{CPM_TABLE}
        WHERE nlp_parathyroid_key_finding IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    out["post_key_finding_distribution"] = [
        {"key_finding": r[0], "n": int(r[1])} for r in kf_dist
    ]

    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 5 blockers: {out['blockers']}")
    return out


# ── PHASE 6 — post-mutation invariants A-F ───────────────────────────────────


def phase_6(con: duckdb.DuckDBPyConnection,
            decisions: dict[str, Any] | None = None) -> dict[str, Any]:
    log("=== PHASE 6 — post-mutation invariants A-F ===")
    out: dict[str, Any] = {
        "phase": 6, "started_at": utcnow_iso(), "gates": [], "blockers": [],
    }

    # A. CPM row count unchanged (still 10,871).
    a = con.execute(f"""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE research_id IS NULL),
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL)
        FROM main.{CPM_TABLE}
    """).fetchone()
    n_cpm, null_rid, null_fna = a
    _gate(out, "A_cpm_rows_eq_10871", n_cpm == EXPECTED_CPM_ROWS,
          {"observed": int(n_cpm), "expected": EXPECTED_CPM_ROWS})
    _gate(out, "A_cpm_no_null_rid", null_rid == 0, {"observed": int(null_rid)})
    _gate(out, "A_cpm_no_null_fna_path_outcome", null_fna == 0,
          {"observed": int(null_fna)})

    # B. CPM distinct research_id unchanged.
    b = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM main.{CPM_TABLE}"
    ).fetchone()[0]
    _gate(out, "B_cpm_rids_eq_10871", b == EXPECTED_CPM_RIDS,
          {"observed": int(b), "expected": EXPECTED_CPM_RIDS})

    # C. No other CPM column was touched. Compare MD5 over all
    # non-nlp_parathyroid_* columns vs the snapshot taken in Phase 4.
    other_cols = [
        r[0] for r in con.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog=? AND table_schema='main' AND table_name=?
              AND column_name NOT LIKE 'nlp_parathyroid_%'
            ORDER BY ordinal_position
        """, [CANONICAL_DB, CPM_TABLE]).fetchall()
    ]
    expr_parts = ", ".join(
        f"COALESCE(CAST(\"{c}\" AS VARCHAR), '<NULL>')" for c in other_cols
    )
    live_hash = con.execute(f"""
        WITH per_row AS (
            SELECT MD5(CONCAT_WS('|', {expr_parts})) AS row_hash,
                   CAST(research_id AS VARCHAR) AS rid
            FROM main.{CPM_TABLE}
        )
        SELECT MD5(STRING_AGG(row_hash, '' ORDER BY rid)) FROM per_row
    """).fetchone()[0]
    out["live_other_cols_md5"] = live_hash
    out["live_other_cols_count"] = len(other_cols)

    # Look up the Phase 4 snapshot hash.
    snap_hash = None
    if decisions and "phases" in decisions and "4" in decisions["phases"]:
        snap_hash = decisions["phases"]["4"].get("snapshot_other_cols_md5")
    if snap_hash is None and DECISIONS_PATH.exists():
        try:
            prev = json.loads(DECISIONS_PATH.read_text())
            snap_hash = prev.get("phases", {}).get("4", {}).get(
                "snapshot_other_cols_md5"
            )
        except Exception as exc:
            log(f"  ⚠ could not load prior decisions.json for snapshot hash: {exc!r}")
    out["phase_4_snapshot_other_cols_md5"] = snap_hash
    if snap_hash is None:
        # If Phase 4 hasn't been run in this session and decisions.json was
        # wiped, recompute against the pre284 snapshot directly.
        snaps = _existing_pre284_cpm_snapshots(con)
        if snaps:
            snap_fq = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{snaps[-1]}'
            snap_hash = con.execute(f"""
                WITH per_row AS (
                    SELECT MD5(CONCAT_WS('|', {expr_parts})) AS row_hash,
                           CAST(research_id AS VARCHAR) AS rid
                    FROM {snap_fq}
                )
                SELECT MD5(STRING_AGG(row_hash, '' ORDER BY rid)) FROM per_row
            """).fetchone()[0]
            out["phase_4_snapshot_other_cols_md5_recomputed_from"] = snaps[-1]
            out["phase_4_snapshot_other_cols_md5"] = snap_hash
    _gate(out, "C_other_cpm_cols_unchanged_vs_phase4_snapshot",
          snap_hash is not None and live_hash == snap_hash,
          {"live": live_hash, "snapshot": snap_hash})

    # D. Source table = 17,321 rows / 7,313 rids / qwen2.5-32b / openai sdk.
    d = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               STRING_AGG(DISTINCT llm_model, '|'),
               STRING_AGG(DISTINCT llm_sdk, '|')
        FROM main.{SOURCE_TABLE}
    """).fetchone()
    _gate(out, "D_source_rows_eq_17321", d[0] == EXPECTED_PARQUET_NRIDS,
          {"observed": int(d[0]), "expected": EXPECTED_PARQUET_NRIDS})
    _gate(out, "D_source_rids_eq_7313", d[1] == EXPECTED_PARQUET_RIDS,
          {"observed": int(d[1]), "expected": EXPECTED_PARQUET_RIDS})
    _gate(out, "D_source_model_eq_qwen25_32b", d[2] == EXPECTED_MODEL,
          {"observed": d[2], "expected": EXPECTED_MODEL})
    _gate(out, "D_source_sdk_eq_openai", d[3] == SYNTH_LLM_SDK,
          {"observed": d[3], "expected": SYNTH_LLM_SDK})

    # E. Phase 1 archive present in archive_pub_v1_0 with 11,037 rows.
    e_snaps = _existing_pre9domainv4_archives(con)
    e_ok = False
    e_detail: dict[str, Any] = {"archives_present": e_snaps}
    if e_snaps:
        snap_fq = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{e_snaps[-1]}'
        e_count = con.execute(f"SELECT COUNT(*) FROM {snap_fq}").fetchone()[0]
        e_detail["newest_archive"] = e_snaps[-1]
        e_detail["row_count"] = int(e_count)
        e_ok = e_count == EXPECTED_STALE_SOURCE_ROWS
    _gate(out, "E_archive_pre9domainv4_exists_with_11037_rows", e_ok, e_detail)

    # F. CPM pre284 snapshot present with 10,871 rows.
    f_snaps = _existing_pre284_cpm_snapshots(con)
    f_ok = False
    f_detail: dict[str, Any] = {"snapshots_present": f_snaps}
    if f_snaps:
        snap_fq = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{f_snaps[-1]}'
        f_count = con.execute(f"SELECT COUNT(*) FROM {snap_fq}").fetchone()[0]
        f_detail["newest_snapshot"] = f_snaps[-1]
        f_detail["row_count"] = int(f_count)
        f_ok = f_count == EXPECTED_CPM_ROWS
    _gate(out, "F_cpm_pre284_snapshot_exists_with_10871_rows", f_ok, f_detail)

    out["observed"] = {
        "cpm_n_rows": int(n_cpm),
        "cpm_null_rid": int(null_rid),
        "cpm_null_fna": int(null_fna),
        "cpm_n_rids": int(b),
        "source_n_rows": int(d[0]),
        "source_n_rids": int(d[1]),
        "source_models": d[2],
        "source_sdks": d[3],
        "phase_1_archives_present": e_snaps,
        "phase_4_snapshots_present": f_snaps,
    }
    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 6 blockers: {out['blockers']}")
    return out


# ── PHASE 7 — registry + dictionary + __readme sync ──────────────────────────

# Per-column dictionary descriptions (modeled on Script 280 / 282 / 283 Phase 7).
DESC_BY_COL: dict[str, str] = {
    "nlp_parathyroid_has_data": (
        "TRUE iff patient has >=1 positive-entity extraction "
        "(confidence>=0.5, present_or_negated='present' OR NULL) from "
        "note_entities_llm_parathyroid_detail (qwen2.5-32b, 9domain_v4 "
        "rerun 2026-04-19); FALSE if patient is in source with no "
        "positive entities or absent from source. NEVER NULL post-Script 284. "
        "Note: parquet covers only 7,313 of 10,871 CPM RIDs; ~3,558 patients "
        "are FALSE because they have no parathyroid_detail-domain notes."
    ),
    "nlp_parathyroid_n_notes": (
        "Count of distinct notes per patient with >=1 positive extracted "
        "parathyroid-detail entity. 0 when has_data=FALSE."
    ),
    "nlp_parathyroid_n_entities": (
        "Total positive parathyroid-detail entities extracted across all "
        "notes for this patient. 0 when has_data=FALSE."
    ),
    "nlp_parathyroid_key_finding": (
        "Highest-priority entity value for this patient, resolved via "
        "KEY_FINDING_PRIORITY (autotransplant > reimplantation_detail > "
        "hypercellularity_grade > gland_cellularity > "
        "parathyroid_frozen_section > gland_count_preserved > "
        "gland_count_total > gland_weight > gland_size > removal_intent > "
        "gland_location > other) tiebroken by confidence DESC NULLS LAST "
        "then entity_value alpha. NULL when has_data=FALSE."
    ),
}


def phase_7(con: duckdb.DuckDBPyConnection,
            decisions: dict[str, Any] | None = None) -> dict[str, Any]:
    log("=== PHASE 7 — registry + dictionary + __readme sync ===")
    out: dict[str, Any] = {
        "phase": 7, "started_at": utcnow_iso(), "gates": [], "blockers": [],
    }

    # Pull post-mutation rollup numbers from prior phase output if available;
    # fall back to live MD query so Phase 7 is runnable in isolation.
    post_has_true = post_sum_notes = post_sum_ents = None
    if decisions and "phases" in decisions and "5" in decisions["phases"]:
        ph5 = decisions["phases"]["5"]
        post_has_true = ph5.get("post_has_data_true")
        post_sum_notes = ph5.get("post_sum_n_notes")
        post_sum_ents = ph5.get("post_sum_n_entities")
    if post_has_true is None and DECISIONS_PATH.exists():
        try:
            prev = json.loads(DECISIONS_PATH.read_text())
            ph5 = prev.get("phases", {}).get("5", {})
            post_has_true = ph5.get("post_has_data_true")
            post_sum_notes = ph5.get("post_sum_n_notes")
            post_sum_ents = ph5.get("post_sum_n_entities")
        except Exception as exc:
            log(f"  ⚠ could not load prior decisions.json: {exc!r}")
    if post_has_true is None:
        live = con.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE nlp_parathyroid_has_data),
                SUM(nlp_parathyroid_n_notes),
                SUM(nlp_parathyroid_n_entities)
            FROM main.{CPM_TABLE}
        """).fetchone()
        post_has_true = int(live[0])
        post_sum_notes = int(live[1] or 0)
        post_sum_ents = int(live[2] or 0)
    out["post_has_data_true"] = int(post_has_true)
    out["post_sum_n_notes"] = int(post_sum_notes or 0)
    out["post_sum_n_entities"] = int(post_sum_ents or 0)
    log(f"  using rollup numbers: has_true={post_has_true} "
        f"sum_notes={post_sum_notes} sum_ents={post_sum_ents}")

    # ── 7A: detail_table_registry_v1 ──────────────────────────────────────
    desc_marker = "Script 284 (2026-04-20): rollup re-promoted"
    desc_suffix = (
        " | Script 284 (2026-04-20): rollup re-promoted against qwen2.5-32b "
        "9domain_v4 rerun parquet (loaded by this script). CPM "
        f"nlp_parathyroid_has_data went {EXPECTED_STALE_HAS_DATA_TRUE} TRUE / "
        f"{EXPECTED_STALE_HAS_DATA_NULL} NULL -> {int(post_has_true)} TRUE / "
        "0 NULL. Note: parquet covers 7,313 of 10,871 CPM RIDs; the "
        f"remainder (~{EXPECTED_CPM_RIDS - EXPECTED_PARQUET_RIDS}) are FALSE."
    )
    con.execute(
        f"""
        UPDATE {WS_SCHEMA}.{REGISTRY_TABLE}
        SET total_rows        = {EXPECTED_PARQUET_NRIDS},
            total_patients    = {EXPECTED_PARQUET_RIDS},
            canonical_version = 'v1_0_script284',
            description       = CASE
              WHEN description LIKE ? THEN description
              ELSE COALESCE(description, '') || ?
            END
        WHERE detail_table_name = ?
        """,
        [f"%{desc_marker}%", desc_suffix, SOURCE_TABLE],
    )
    reg = con.execute(f"""
        SELECT total_rows, total_patients, canonical_version, description
        FROM {WS_SCHEMA}.{REGISTRY_TABLE}
        WHERE detail_table_name = ?
    """, [SOURCE_TABLE]).fetchone()
    out["registry_after"] = {
        "total_rows": int(reg[0]), "total_patients": int(reg[1]),
        "canonical_version": reg[2],
        "description_len": len(reg[3] or ""),
    }
    _gate(out, "registry_total_rows_post_eq_17321",
          reg[0] == EXPECTED_PARQUET_NRIDS,
          {"observed": int(reg[0]), "expected": EXPECTED_PARQUET_NRIDS})
    _gate(out, "registry_total_patients_post_eq_7313",
          reg[1] == EXPECTED_PARQUET_RIDS,
          {"observed": int(reg[1]), "expected": EXPECTED_PARQUET_RIDS})
    _gate(out, "registry_canonical_version_post_eq_v1_0_script284",
          reg[2] == "v1_0_script284",
          {"observed": reg[2]})
    _gate(out, "registry_description_carries_script284_marker",
          desc_marker in (reg[3] or ""),
          {"description_tail": (reg[3] or "")[-200:]})

    # ── 7B: data_dictionary_v279 ──────────────────────────────────────────
    # Use COALESCE(NULLIF(d, ''), ?) so empty strings get replaced;
    # non-empty existing text is preserved (Script 280/282/283 parity).
    v279_note = (
        "Re-promoted by Script 284 (2026-04-20) from qwen2.5-32b "
        "parathyroid_detail 9domain_v4 rerun."
    )
    rebuilt_by = "script284_2026-04-20"
    for col, col_desc in DESC_BY_COL.items():
        con.execute(
            f"""
            UPDATE main.{DICTIONARY_TABLE}
            SET description = COALESCE(NULLIF(description, ''), ?),
                v279_note   = ?,
                rebuilt_at  = CURRENT_TIMESTAMP,
                rebuilt_by  = ?
            WHERE column_name = ? AND table_name = ?
            """,
            [col_desc, v279_note, rebuilt_by, col, CPM_TABLE],
        )
    dict_check = con.execute(f"""
        SELECT column_name, description, v279_note, rebuilt_by
        FROM main.{DICTIONARY_TABLE}
        WHERE table_name = ?
          AND column_name IN ('nlp_parathyroid_has_data',
                              'nlp_parathyroid_n_notes',
                              'nlp_parathyroid_n_entities',
                              'nlp_parathyroid_key_finding')
        ORDER BY column_name
    """, [CPM_TABLE]).fetchall()
    rows_dict = [
        {"column_name": r[0], "description": r[1], "v279_note": r[2],
         "rebuilt_by": r[3]}
        for r in dict_check
    ]
    out["dictionary_after"] = rows_dict
    desc_match = all(
        row["description"] == DESC_BY_COL[row["column_name"]]
        for row in rows_dict
    )
    v279_match = all(row["v279_note"] == v279_note for row in rows_dict)
    rb_match = all(row["rebuilt_by"] == rebuilt_by for row in rows_dict)
    _gate(out, "dictionary_4_cols_have_exact_per_col_desc_and_v279_note",
          len(rows_dict) == 4 and desc_match and v279_match and rb_match,
          {"rows": rows_dict, "descriptions_exact_match": desc_match})

    # ── 7C: __readme entries ──────────────────────────────────────────────
    src_n = int(con.execute(
        f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}"
    ).fetchone()[0])
    src_rids = int(con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM main.{SOURCE_TABLE}"
    ).fetchone()[0])
    cpm_n = int(con.execute(
        f"SELECT COUNT(*) FROM main.{CPM_TABLE}"
    ).fetchone()[0])
    cpm_rids = int(con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM main.{CPM_TABLE}"
    ).fetchone()[0])

    src_desc = (
        "LLM parathyroid-detail entities (qwen2.5-32b, 9domain_v4 rerun "
        "2026-04-19; loaded by Script 284 2026-04-20). Source-of-truth for "
        "CPM nlp_parathyroid_*. Covers 7,313 of 10,871 CPM RIDs."
    )
    cpm_desc_marker_284 = "Script 284 2026-04-20"
    cpm_existing_desc = con.execute(
        f"SELECT description FROM main.{README_TABLE} WHERE table_name = ?",
        [CPM_TABLE],
    ).fetchone()
    if cpm_existing_desc and cpm_existing_desc[0] and cpm_desc_marker_284 in cpm_existing_desc[0]:
        cpm_desc_value = cpm_existing_desc[0]  # idempotent: don't double-append
    else:
        cpm_addendum = (
            f" [{cpm_desc_marker_284}: parathyroid_detail re-extracted at "
            f"qwen2.5-32b ({src_n:,} rows / {src_rids:,} RIDs); CPM "
            f"nlp_parathyroid_* rebuilt with explicit TRUE/FALSE (0 NULLs)."
            f" has_data TRUE={int(post_has_true)}; remainder FALSE includes "
            f"~{EXPECTED_CPM_RIDS - EXPECTED_PARQUET_RIDS} RIDs absent from "
            "parquet entirely.]"
        )
        cpm_desc_value = (
            (cpm_existing_desc[0] if cpm_existing_desc else "") or ""
        ) + cpm_addendum

    # Upsert source row.
    src_exists = con.execute(
        f"SELECT 1 FROM main.{README_TABLE} WHERE table_name = ?",
        [SOURCE_TABLE],
    ).fetchone() is not None
    if src_exists:
        con.execute(
            f"""
            UPDATE main.{README_TABLE}
            SET n_rows = ?, n_distinct_research_id = ?,
                description = ?, inventoried_at = CURRENT_TIMESTAMP
            WHERE table_name = ?
            """,
            [src_n, src_rids, src_desc, SOURCE_TABLE],
        )
        out["readme_source_action"] = "updated_existing"
    else:
        con.execute(
            f"""
            INSERT INTO main.{README_TABLE}
                (table_name, n_rows, n_distinct_research_id, description,
                 inventoried_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [SOURCE_TABLE, src_n, src_rids, src_desc],
        )
        out["readme_source_action"] = "inserted_new"

    # Upsert CPM row.
    cpm_exists = con.execute(
        f"SELECT 1 FROM main.{README_TABLE} WHERE table_name = ?",
        [CPM_TABLE],
    ).fetchone() is not None
    if cpm_exists:
        con.execute(
            f"""
            UPDATE main.{README_TABLE}
            SET n_rows = ?, n_distinct_research_id = ?,
                description = ?, inventoried_at = CURRENT_TIMESTAMP
            WHERE table_name = ?
            """,
            [cpm_n, cpm_rids, cpm_desc_value, CPM_TABLE],
        )
        out["readme_cpm_action"] = "updated_existing"
    else:
        con.execute(
            f"""
            INSERT INTO main.{README_TABLE}
                (table_name, n_rows, n_distinct_research_id, description,
                 inventoried_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [CPM_TABLE, cpm_n, cpm_rids, cpm_desc_value],
        )
        out["readme_cpm_action"] = "inserted_new"

    src_after = con.execute(
        f"SELECT n_rows, n_distinct_research_id, description "
        f"FROM main.{README_TABLE} WHERE table_name = ?",
        [SOURCE_TABLE],
    ).fetchone()
    cpm_after = con.execute(
        f"SELECT n_rows, n_distinct_research_id, description "
        f"FROM main.{README_TABLE} WHERE table_name = ?",
        [CPM_TABLE],
    ).fetchone()
    out["readme_source_after"] = {
        "n_rows": int(src_after[0]), "n_rids": int(src_after[1]),
        "description_len": len(src_after[2] or ""),
    }
    out["readme_cpm_after"] = {
        "n_rows": int(cpm_after[0]), "n_rids": int(cpm_after[1]),
        "description_len": len(cpm_after[2] or ""),
    }
    _gate(out, "readme_source_n_rows_eq_17321",
          src_after[0] == EXPECTED_PARQUET_NRIDS,
          {"observed": int(src_after[0]), "expected": EXPECTED_PARQUET_NRIDS})
    _gate(out, "readme_source_n_rids_eq_7313",
          src_after[1] == EXPECTED_PARQUET_RIDS,
          {"observed": int(src_after[1]), "expected": EXPECTED_PARQUET_RIDS})
    _gate(out, "readme_cpm_n_rows_eq_10871",
          cpm_after[0] == EXPECTED_CPM_ROWS,
          {"observed": int(cpm_after[0]), "expected": EXPECTED_CPM_ROWS})
    _gate(out, "readme_cpm_description_carries_script284_marker",
          cpm_desc_marker_284 in (cpm_after[2] or ""),
          {"tail": (cpm_after[2] or "")[-200:]})

    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 7 blockers: {out['blockers']}")
    return out


# ── CLI ──────────────────────────────────────────────────────────────────────


PHASE_FUNCS: dict[int, Any] = {
    0: phase_0,
    1: phase_1,
    2: phase_2,
    3: phase_3,
    4: phase_4,
    5: phase_5,
    6: phase_6,
    7: phase_7,
}


def _save_decisions(decisions: dict[str, Any]) -> None:
    DECISIONS_PATH.write_text(json.dumps(decisions, indent=2, default=str))


def _load_existing_decisions() -> dict[str, Any]:
    """Read prior decisions.json to merge phase output across invocations.

    Each --phase invocation overwrites decisions.json with the merged
    state so per-phase commits include the cumulative artifact.
    """
    if not DECISIONS_PATH.exists():
        return {"script": "scripts/284_parathyroid_v4_md_load_and_rollup.py",
                "phases": {}}
    try:
        d = json.loads(DECISIONS_PATH.read_text())
        if "phases" not in d:
            d["phases"] = {}
        return d
    except Exception:
        return {"script": "scripts/284_parathyroid_v4_md_load_and_rollup.py",
                "phases": {}}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        choices=["0", "1", "2", "3", "4", "5", "6", "7", "all"],
        default="0",
    )
    args = parser.parse_args()

    decisions = _load_existing_decisions()
    decisions.setdefault("script", "scripts/284_parathyroid_v4_md_load_and_rollup.py")
    decisions["last_started_at"] = utcnow_iso()
    decisions["last_phase_arg"] = args.phase

    con = connect()
    rc = 0
    try:
        if args.phase == "all":
            for p in (0, 1, 2, 3, 4, 5, 6, 7):
                fn = PHASE_FUNCS[p]
                if p in (6, 7):
                    decisions["phases"][str(p)] = fn(con, decisions)
                else:
                    decisions["phases"][str(p)] = fn(con)
                _save_decisions(decisions)
        else:
            p = int(args.phase)
            fn = PHASE_FUNCS[p]
            if p in (6, 7):
                decisions["phases"][str(p)] = fn(con, decisions)
            else:
                decisions["phases"][str(p)] = fn(con)
            _save_decisions(decisions)
    except PreflightHalt as exc:
        log(f"PREFLIGHT HALT: {exc}")
        rc = 2
    except Exception as exc:  # noqa: BLE001
        log(f"FATAL: {exc!r}")
        rc = 1
    finally:
        decisions["last_finished_at"] = utcnow_iso()
        decisions["last_return_code"] = rc
        _save_decisions(decisions)
        _flush_log()
        con.close()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
