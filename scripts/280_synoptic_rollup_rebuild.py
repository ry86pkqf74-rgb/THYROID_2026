#!/usr/bin/env python3
"""
Script 280 — Synoptic Rollup Re-Promotion (Phase B).

PROVENANCE NOTE
===============
This script runs AFTER an ad-hoc MotherDuck load of the synoptic rerun that
happened outside of a tracked phase script (a coworker loaded the parquet
directly into ``main.note_entities_llm_synoptic_pathology_enrichment`` in late
April 2026, replacing the older qwen3:32b extraction with the qwen2.5-32b
Vast.ai vLLM rerun; data + 6 synthesized provenance cols were written, but
the downstream CPM rollup, the ``detail_table_registry_v1`` row, and the
``data_dictionary_v279`` entries were NOT updated).

A full content-integrity audit (Claude, 2026-04-19) confirmed that the MD
table is byte-identical to the local parquet at
``processed/remaining/9domain_v4/output/note_entities_llm_synoptic_pathology_enrichment.parquet``
via 3 independent MD5 aggregates over (note_row_id ordered) tuples:
    result_json   md_agg_rjhash  = 786e05480dec4590494bcf63114603fd
    note_date     md_agg_ndhash  = 6723fea873c8b1e92f7fa2bcfa6d8fca
    source_tuple  md_agg_srchash = ad3654550fd09b4bf0649ce5a0cf67d4

The audit also verified:
  * 26,584 rows / 10,862 distinct RIDs (matches parquet exactly)
  * Zero NULLs on note_row_id, research_id, result_json, extracted_at, note_date
  * Zero duplicate note_row_ids
  * 100% valid JSON
  * Single llm_model (qwen2.5-32b), single llm_base_url (Vast.ai vLLM endpoint),
    single preprocess_batch_id, single preprocess_script_version
    ('v4_9domain_rerun_2026-04-18', traced to scripts/build_synoptic_input_v4.py
    committed in 6502160)
  * 10,862 / 10,862 source RIDs link to canonical_patient_master (zero orphans)
  * 9 CPM RIDs absent from source (pre-filtered at preprocessing; all benign,
    no synoptic-relevant content in path_synoptics) — will correctly promote
    to nlp_synoptic_has_data=FALSE
  * 3 API-timeout error rows (RIDs 7805, 7139, 8116) left as-is; the strict
    rollup pattern excludes them correctly

This script therefore treats the in-place MD state as the authoritative
post-load state. Phase 0 re-verifies these invariants via pinned hashes
before any writes.

Phase gates (CLI; default 0):
    --phase 0    Pre-flight audit (READ-ONLY) — pinned-hash + invariant gates
    --phase 1    Pre-mutation snapshot of CPM in archive_pub_v1_0
    --phase 2    Rollup re-promotion on canonical_patient_master
    --phase 3    detail_table_registry_v1 + data_dictionary_v279 sync
    --phase 4    main-schema hygiene sweep
    --phase 5    __readme + dictionary audit rebuild
    --phase 6    End-to-end validation (invariants A–G)
    --phase all  Run 0→6, halting on any failed gate

Hard rules (NON-NEGOTIABLE):
  * READ-ONLY to ``main.note_entities_llm_synoptic_pathology_enrichment`` —
    do NOT re-load it (already at qwen2.5-32b state) and do NOT modify rows
    or schema.
  * No touching pathology / tirads_granular / cervical_ln_detail / imaging /
    past_surgical_hx — those are Phases D/E/F.
  * No touching tirads_v2_* anything (Phase C).
  * 24 thin_wrapper rows with filter_type_provisional=TRUE are OFF-LIMITS.
  * Auth via motherduck_client.get_token(); never hard-code or print tokens.
  * No structural changes to canonical_patient_master — only UPDATE the 4
    existing nlp_synoptic_* columns.

Filter semantics (Script 212 parity)
====================================
Earlier guidance for this script said "leave negated entities in" but was
given before the author had read Script 212's rollup pattern. Script 212
(used by pathology, tirads, cervln, tgkin, recurrence, vascular, and the
tier-3 LLM domains) applies a present-only + confidence>=0.5 filter (the
``_pos`` CTE). Script 280 matches that pattern for cross-domain consistency.

Two distinct counts therefore appear in this script:
  * Phase 0's ``source_rids_with_entity_eq_4992``  — source-integrity
    count via LIKE-pattern shortcut on ``result_json``. Counts every RID
    whose source rows carry any ``entity_value`` regardless of confidence
    or negation. This is the byte-level audit anchor.
  * Phase 0's ``source_rids_with_positive_entity_eq_4835`` and
    Phase 2's promotion target — clinical count via the ``_pos`` CTE.
    Counts only RIDs with at least one extracted entity at confidence>=0.5
    AND (present_or_negated='present' OR present_or_negated IS NULL).
    This is the rollup that lands on canonical_patient_master.

Both Phase 0's positive-count gate and Phase 2's UPDATE share the SAME
``ROLLUP_BASE_CTE`` SQL string (parsed → flat → ext → pos), so any drift
between gate and mutation is impossible by construction.
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

SOURCE_TABLE = "note_entities_llm_synoptic_pathology_enrichment"
CPM_TABLE = "canonical_patient_master"
REGISTRY_TABLE = "detail_table_registry_v1"
DICTIONARY_TABLE = "data_dictionary_v279"

# Pinned post-rerun expected values (Claude audit, 2026-04-19).
EXPECTED_SOURCE_ROWS = 26584
EXPECTED_SOURCE_RIDS = 10862
EXPECTED_SOURCE_MODEL = "qwen2.5-32b"
EXPECTED_SOURCE_BASE_URL = "http://93.91.156.83:53202/v1"
EXPECTED_PREPROCESS_SCRIPT_VERSION = "v4_9domain_rerun_2026-04-18"
EXPECTED_EXTRACTED_AT_MIN_PREFIX = "2026-04-19T03:07"
EXPECTED_EXTRACTED_AT_MAX_PREFIX = "2026-04-19T04:22"
EXPECTED_API_TIMEOUT_ROWS = 3

# Source-integrity (LIKE-pattern shortcut): any RID with any extracted entity.
EXPECTED_RIDS_WITH_ENTITY = 4992

# Clinical/positive (Script 212 _pos filter): confidence>=0.5 AND
# (present_or_negated='present' OR present_or_negated IS NULL).
# These are the Phase 2 promotion targets and Phase 0 positive-count gates.
EXPECTED_RIDS_WITH_POSITIVE_ENTITY = 4835
EXPECTED_NOTES_WITH_POSITIVE_ENTITY = 10801

# Pinned content-hash gates (CRITICAL — drift means table changed since audit).
PINNED_HASH_RJ = "786e05480dec4590494bcf63114603fd"
PINNED_HASH_ND = "6723fea873c8b1e92f7fa2bcfa6d8fca"
PINNED_HASH_SRC = "ad3654550fd09b4bf0649ce5a0cf67d4"

# Pinned canonical_patient_master invariants.
EXPECTED_CPM_ROWS = 10871
EXPECTED_CPM_RIDS = 10871

# Pinned stale state we are about to fix.
EXPECTED_STALE_HAS_DATA = 8
EXPECTED_STALE_REGISTRY_ROWS = 11037
EXPECTED_STALE_REGISTRY_PATIENTS = 5641

# Phase 2 acceptance band: post-promotion has_data count must be 4835 ± 10.
HAS_DATA_TARGET = EXPECTED_RIDS_WITH_POSITIVE_ENTITY
HAS_DATA_TOLERANCE = 10

# Phase 6 invariant tolerances.
EXPECTED_THIN_WRAPPER_PROVISIONAL = 24

# Phase 6 Gate G — manuscript_workspace baseline view allowlist.
# Pinned by NAME (same pattern as Phase 5's KNOWN_UNDOCUMENTED_CPM_COLS) so
# the workspace can keep growing (new cohort views land routinely) while we
# still detect deletion of any baseline view. The gate asserts:
#   missing := BASELINE_WS_VIEWS - actual_ws_views == ∅
# Additions beyond baseline are recorded for audit but do NOT fail the gate.
#
# Snapshot taken 2026-04-19 by Claude during Phase 6 review; contains all 67
# views from the post-272 finalization (THYROID_2026_FINALIZATION_20260418.md)
# minus 'tirads_llm_haiku_vs_qwen_v1' which was added between 4/18 and 4/19.
BASELINE_WS_VIEWS: frozenset[str] = frozenset({
    "canonical_detail_pointer_v1",
    "cohort_descriptive_full_cohort_v1",
    "cohort_m001_indeterminate_genetics_v1",
    "cohort_m004_graves_hashimoto_cancer_v1",
    "cohort_m006_molecular_surg_decision_v1",
    "cohort_m007_rss_reclassification_v1",
    "cohort_m009_parathyroid_final_path_v1",
    "cohort_m011_tirads_fna_genetics_v1",
    "cohort_m016_graves_carcinoma_v1",
    "cohort_m017_eucalcemic_hypopara_v1",
    "cohort_m018_molecular_beth56_v1",
    "cohort_m019_rai_outcomes_v1",
    "cohort_m023_preop_genetics_v1",
    "cohort_m025_tirads_performance_v1",
    "cohort_m028_bethesda_iii_iv_v1",
    "cohort_m029_fna_concordance_v1",
    "cohort_m030_genetic_predictive_v1",
    "cohort_m031_nuclear_medicine_v1",
    "cohort_m032_descriptive_25yr_v1",
    "cohort_m033_afirma_thyroseq_v1",
    "cohort_m035_bethesda_v_v1",
    "cohort_m036_ata_risk_comparison_v1",
    "cohort_m037_ln_metastasis_v1",
    "cohort_m038_massive_goiter_v1",
    "cohort_m039_pth_calcium_v1",
    "cohort_m040_reoperative_v1",
    "cohort_m042_incidental_parathyroid_v1",
    "cohort_m043_ln_predictors_v1",
    "cohort_m044_ajcc_ete_v1",
    "cohort_m045_multimodal_risk_v1",
    "cohort_m046_niftp_era_bethesda_v1",
    "cohort_m047_frozen_section_v1",
    "cohort_m048_tnm_multifocal_v1",
    "cohort_m049_pyramidal_lobe_v1",
    "cohort_m050_tumor_size_volume_v1",
    "cohort_m051_ete_ln_v1",
    "cohort_m052_mrlnd_ln_count_v1",
    "cohort_m053_nondiagnostic_fna_v1",
    "cohort_m054_niftp_reclass_v1",
    "cohort_m055_recurrence_rai_v1",
    "cohort_m056_age_epidemiology_v1",
    "cohort_m057_risk_stratification_v1",
    "cohort_m058_thyroid_size_weight_v1",
    "cohort_m059_prognostic_scoring_v1",
    "cohort_m060_adenoma_ftump_v1",
    "cohort_m061_thyroiditis_outcomes_v1",
    "cohort_m062_incidental_frozen_v1",
    "cohort_m063_frozen_false_neg_v1",
    "cohort_m064_frozen_decision_v1",
    "cohort_m065_frozen_tt_vs_lob_v1",
    "cohort_m066_parathyroid_id_v1",
    "cohort_m067_tsh_tg_tumorigenesis_v1",
    "cohort_m068_mutation_labs_v1",
    "cohort_m069_graves_hashimoto_v1",
    "cohort_m070_hereditary_v1",
    "cohort_m071_immunologic_meds_v1",
    "cohort_m072_molecular_surg_impact_v1",
    "cohort_m073_tg_lob_vs_tt_v1",
    "cohort_m075_tirads_multi_nodule_v1",
    "cohort_m076_ln_surveillance_v1",
    "cohort_m078_graves_survival_v1",
    "cohort_m079_eucalcemic_outcomes_v1",
    "cohort_m080_molecular_beth56_v1",
    "cohort_m081_rai_resistant_v1",
    "cohort_m082_parathyroid_tumors_v1",
    "imaging_nodule_master_clean_v1",
    "path_tumor_size_invariant_v1",
})

# Output paths.
OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PREFLIGHT_PATH = OUTPUT_DIR / "280_preflight.json"
DECISIONS_PATH = OUTPUT_DIR / "280_decisions.json"
LOG_PATH = OUTPUT_DIR / "280_run.log"
REPORT_MD = REPO_ROOT / "THYROID_2026_SCRIPT_280_REPORT.md"

# Entity-detection LIKE pattern (source-integrity shortcut — used in Phase 0's
# all-extracted gate and as the fast pre-filter in the parsed CTE below).
ENTITY_LIKE_PRESENT = "%\"entity_value\":%"
EMPTY_ENTITIES_PATTERN = "%\"entities\": []%"

# Shared rollup base CTE — parsed → flat → ext → pos. Used by BOTH the Phase 0
# positive-count gate and the Phase 2 UPDATE so the two cannot drift. The
# ``_pos`` filter mirrors Script 212's LLM_ENTITY_PARSE_CTE: confidence>=0.5
# AND (present_or_negated='present' OR present_or_negated IS NULL).
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

# ── logging helpers ──────────────────────────────────────────────────────────

_log_buf: list[str] = []


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]}Z] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def _flush_log() -> None:
    LOG_PATH.write_text("\n".join(_log_buf) + "\n")


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


# ── PHASE 0 — pre-flight audit (READ-ONLY) ───────────────────────────────────


class PreflightHalt(RuntimeError):
    """Raised when a Phase 0 gate fails."""


def _gate(out: dict[str, Any], name: str, ok: bool, detail: Any = None) -> None:
    out["gates"].append({"name": name, "ok": bool(ok), "detail": detail})
    if not ok:
        out["blockers"].append({"name": name, "detail": detail})
    log(f"  gate {name}: {'OK' if ok else 'FAIL'}{'' if detail is None else f' — {detail}'}")


def phase_0(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 0 — pre-flight audit (READ-ONLY) ===")
    out: dict[str, Any] = {
        "phase": 0,
        "started_at": utcnow_iso(),
        "token_mode": token_mode(),
        "canonical_db": CANONICAL_DB,
        "source_table": f"main.{SOURCE_TABLE}",
        "gates": [],
        "blockers": [],
        "observed": {},
    }

    # ── 0A — source table shape: rows / RIDs / model / extracted_at window ──
    row = con.execute(
        f"""
        SELECT
            COUNT(*)                                         AS n_rows,
            COUNT(DISTINCT research_id)                      AS n_rids,
            STRING_AGG(DISTINCT llm_model, ',' ORDER BY llm_model) AS models,
            MIN(extracted_at)                                AS ext_min,
            MAX(extracted_at)                                AS ext_max
        FROM main.{SOURCE_TABLE}
        """
    ).fetchone()
    n_rows, n_rids, models, ext_min, ext_max = row
    out["observed"]["source_n_rows"] = int(n_rows)
    out["observed"]["source_n_rids"] = int(n_rids)
    out["observed"]["source_models"] = models
    out["observed"]["source_extracted_at_min"] = str(ext_min) if ext_min is not None else None
    out["observed"]["source_extracted_at_max"] = str(ext_max) if ext_max is not None else None
    log(f"  0A: source rows={n_rows:,} rids={n_rids:,} models={models}")
    log(f"      extracted_at min={ext_min}  max={ext_max}")
    _gate(out, "source_n_rows_eq_26584", n_rows == EXPECTED_SOURCE_ROWS,
          {"observed": int(n_rows), "expected": EXPECTED_SOURCE_ROWS})
    _gate(out, "source_n_rids_eq_10862", n_rids == EXPECTED_SOURCE_RIDS,
          {"observed": int(n_rids), "expected": EXPECTED_SOURCE_RIDS})
    _gate(out, "source_model_eq_qwen25_32b", models == EXPECTED_SOURCE_MODEL,
          {"observed": models, "expected": EXPECTED_SOURCE_MODEL})
    _gate(out, "source_extracted_at_min_starts_with_2026-04-19T03:07",
          str(ext_min).startswith(EXPECTED_EXTRACTED_AT_MIN_PREFIX),
          {"observed": str(ext_min), "expected_prefix": EXPECTED_EXTRACTED_AT_MIN_PREFIX})
    _gate(out, "source_extracted_at_max_starts_with_2026-04-19T04:22",
          str(ext_max).startswith(EXPECTED_EXTRACTED_AT_MAX_PREFIX),
          {"observed": str(ext_max), "expected_prefix": EXPECTED_EXTRACTED_AT_MAX_PREFIX})

    # ── 0B — content-hash pin (cryptographic byte-identity vs local parquet) ──
    log("  0B: computing pinned content hashes (md_agg_rjhash / ndhash / srchash)…")
    hashes = con.execute(
        f"""
        WITH tuples AS (
            SELECT
                note_row_id,
                MD5(COALESCE(result_json, ''))             AS rj_hash,
                MD5(COALESCE(note_date::VARCHAR, ''))      AS nd_hash,
                MD5(
                    COALESCE(source_workbook, '') || '|' ||
                    COALESCE(source_sheet,    '') || '|' ||
                    COALESCE(source_column,   '')
                )                                          AS src_hash
            FROM main.{SOURCE_TABLE}
        )
        SELECT
            MD5(STRING_AGG(rj_hash,  '' ORDER BY note_row_id)) AS md_agg_rjhash,
            MD5(STRING_AGG(nd_hash,  '' ORDER BY note_row_id)) AS md_agg_ndhash,
            MD5(STRING_AGG(src_hash, '' ORDER BY note_row_id)) AS md_agg_srchash
        FROM tuples
        """
    ).fetchone()
    md_rj, md_nd, md_src = hashes
    out["observed"]["md_agg_rjhash"] = md_rj
    out["observed"]["md_agg_ndhash"] = md_nd
    out["observed"]["md_agg_srchash"] = md_src
    out["observed"]["pinned_rjhash"] = PINNED_HASH_RJ
    out["observed"]["pinned_ndhash"] = PINNED_HASH_ND
    out["observed"]["pinned_srchash"] = PINNED_HASH_SRC
    log(f"      md_agg_rjhash  observed={md_rj}  pinned={PINNED_HASH_RJ}")
    log(f"      md_agg_ndhash  observed={md_nd}  pinned={PINNED_HASH_ND}")
    log(f"      md_agg_srchash observed={md_src} pinned={PINNED_HASH_SRC}")
    _gate(out, "pinned_md_agg_rjhash_match", md_rj == PINNED_HASH_RJ,
          {"observed": md_rj, "pinned": PINNED_HASH_RJ})
    _gate(out, "pinned_md_agg_ndhash_match", md_nd == PINNED_HASH_ND,
          {"observed": md_nd, "pinned": PINNED_HASH_ND})
    _gate(out, "pinned_md_agg_srchash_match", md_src == PINNED_HASH_SRC,
          {"observed": md_src, "pinned": PINNED_HASH_SRC})

    # ── 0C — uniformity / integrity gates ──
    n_err = con.execute(
        f"""
        SELECT COUNT(*) FROM main.{SOURCE_TABLE}
        WHERE result_json LIKE '%APITimeoutError%'
        """
    ).fetchone()[0]
    out["observed"]["api_timeout_rows"] = int(n_err)
    _gate(out, "api_timeout_rows_eq_3", n_err == EXPECTED_API_TIMEOUT_ROWS,
          {"observed": int(n_err), "expected": EXPECTED_API_TIMEOUT_ROWS})

    n_psv = con.execute(
        f"SELECT COUNT(DISTINCT preprocess_script_version) FROM main.{SOURCE_TABLE}"
    ).fetchone()[0]
    psv_value = con.execute(
        f"SELECT STRING_AGG(DISTINCT preprocess_script_version, '|') FROM main.{SOURCE_TABLE}"
    ).fetchone()[0]
    out["observed"]["distinct_preprocess_script_version"] = int(n_psv)
    out["observed"]["preprocess_script_version_values"] = psv_value
    _gate(out, "preprocess_script_version_uniform", n_psv == 1,
          {"observed_distinct_count": int(n_psv), "values": psv_value})
    _gate(out, "preprocess_script_version_value_match",
          psv_value == EXPECTED_PREPROCESS_SCRIPT_VERSION,
          {"observed": psv_value, "expected": EXPECTED_PREPROCESS_SCRIPT_VERSION})

    n_pbid = con.execute(
        f"SELECT COUNT(DISTINCT preprocess_batch_id) FROM main.{SOURCE_TABLE}"
    ).fetchone()[0]
    out["observed"]["distinct_preprocess_batch_id"] = int(n_pbid)
    _gate(out, "preprocess_batch_id_uniform", n_pbid == 1,
          {"observed_distinct_count": int(n_pbid)})

    base_urls = con.execute(
        f"SELECT STRING_AGG(DISTINCT llm_base_url, '|') FROM main.{SOURCE_TABLE}"
    ).fetchone()[0]
    out["observed"]["llm_base_url_values"] = base_urls
    _gate(out, "llm_base_url_eq_vllm_endpoint",
          base_urls == EXPECTED_SOURCE_BASE_URL,
          {"observed": base_urls, "expected": EXPECTED_SOURCE_BASE_URL})

    # ── 0D — null gates on identifier columns ──
    null_row = con.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE note_row_id  IS NULL) AS null_note_row_id,
            COUNT(*) FILTER (WHERE research_id  IS NULL) AS null_research_id,
            COUNT(*) FILTER (WHERE result_json  IS NULL) AS null_result_json,
            COUNT(*) FILTER (WHERE extracted_at IS NULL) AS null_extracted_at,
            COUNT(*) FILTER (WHERE note_date    IS NULL) AS null_note_date
        FROM main.{SOURCE_TABLE}
        """
    ).fetchone()
    null_cols = {
        "null_note_row_id":  int(null_row[0]),
        "null_research_id":  int(null_row[1]),
        "null_result_json":  int(null_row[2]),
        "null_extracted_at": int(null_row[3]),
        "null_note_date":    int(null_row[4]),
    }
    out["observed"]["null_columns"] = null_cols
    _gate(out, "source_no_null_identifier_cols", all(v == 0 for v in null_cols.values()),
          null_cols)

    dup_row = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT note_row_id FROM main.{SOURCE_TABLE}
            GROUP BY note_row_id HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    out["observed"]["duplicate_note_row_id_groups"] = int(dup_row[0])
    _gate(out, "source_no_duplicate_note_row_ids", dup_row[0] == 0,
          {"observed_dup_groups": int(dup_row[0])})

    # ── 0E — CPM invariant ──
    cpm_row = con.execute(
        f"""
        SELECT
            COUNT(*)                          AS n_rows,
            COUNT(DISTINCT research_id)       AS n_rids,
            COUNT(*) FILTER (WHERE research_id IS NULL)        AS null_rid,
            COUNT(*) FILTER (WHERE fna_path_outcome IS NULL)   AS null_fna
        FROM main.{CPM_TABLE}
        """
    ).fetchone()
    cpm_n_rows, cpm_n_rids, cpm_null_rid, cpm_null_fna = cpm_row
    out["observed"]["cpm_n_rows"] = int(cpm_n_rows)
    out["observed"]["cpm_n_rids"] = int(cpm_n_rids)
    out["observed"]["cpm_null_research_id"] = int(cpm_null_rid)
    out["observed"]["cpm_null_fna_path_outcome"] = int(cpm_null_fna)
    log(f"  0E: CPM rows={cpm_n_rows:,} rids={cpm_n_rids:,} "
        f"null_rid={cpm_null_rid} null_fna={cpm_null_fna}")
    _gate(out, "cpm_rows_eq_10871", cpm_n_rows == EXPECTED_CPM_ROWS,
          {"observed": int(cpm_n_rows), "expected": EXPECTED_CPM_ROWS})
    _gate(out, "cpm_rids_eq_10871", cpm_n_rids == EXPECTED_CPM_RIDS,
          {"observed": int(cpm_n_rids), "expected": EXPECTED_CPM_RIDS})
    _gate(out, "cpm_no_null_rid", cpm_null_rid == 0, {"observed": int(cpm_null_rid)})
    _gate(out, "cpm_no_null_fna_path_outcome", cpm_null_fna == 0, {"observed": int(cpm_null_fna)})

    # ── 0F — current STALE rollup state on CPM (the thing we are fixing) ──
    stale = con.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE nlp_synoptic_has_data)  AS has_true,
            COUNT(*) FILTER (WHERE nlp_synoptic_has_data IS NOT NULL) AS not_null,
            COUNT(*) FILTER (WHERE nlp_synoptic_has_data IS NULL)     AS is_null,
            SUM(CASE WHEN nlp_synoptic_n_notes IS NULL THEN 0 ELSE nlp_synoptic_n_notes END) AS sum_n_notes
        FROM main.{CPM_TABLE}
        """
    ).fetchone()
    stale_has_data, stale_not_null, stale_is_null, stale_sum_n_notes = stale
    out["observed"]["cpm_stale_has_data_true"] = int(stale_has_data)
    out["observed"]["cpm_nlp_synoptic_has_data_not_null"] = int(stale_not_null)
    out["observed"]["cpm_nlp_synoptic_has_data_is_null"] = int(stale_is_null)
    out["observed"]["cpm_stale_sum_n_notes"] = int(stale_sum_n_notes or 0)
    log(f"  0F: STALE nlp_synoptic_has_data TRUE count = {stale_has_data} "
        f"(expected {EXPECTED_STALE_HAS_DATA}; >100 means someone else already promoted)")
    _gate(out, "stale_has_data_eq_8", stale_has_data == EXPECTED_STALE_HAS_DATA,
          {"observed": int(stale_has_data), "expected": EXPECTED_STALE_HAS_DATA})
    _gate(out, "stale_has_data_not_already_promoted", stale_has_data <= 100,
          {"observed": int(stale_has_data),
           "guard": ">100 would mean rollup was promoted by another script"})

    # ── 0G — registry stale state ──
    reg_present = table_exists(con, CANONICAL_DB, WS_SCHEMA, REGISTRY_TABLE)
    out["observed"]["registry_table_present"] = reg_present
    if reg_present:
        reg_row = con.execute(
            f"""
            SELECT total_rows, total_patients, canonical_version, description
            FROM {WS_SCHEMA}.{REGISTRY_TABLE}
            WHERE detail_table_name = ?
            """,
            [SOURCE_TABLE],
        ).fetchone()
    else:
        reg_row = None
    if reg_row is None:
        out["observed"]["registry_synoptic_row"] = None
        _gate(out, "registry_synoptic_row_exists", False,
              {"detail": f"no row in {WS_SCHEMA}.{REGISTRY_TABLE} for {SOURCE_TABLE}"})
    else:
        rt, rp, cv, desc = reg_row
        out["observed"]["registry_synoptic_row"] = {
            "total_rows": int(rt) if rt is not None else None,
            "total_patients": int(rp) if rp is not None else None,
            "canonical_version": cv,
            "description_len": len(desc) if desc else 0,
        }
        log(f"  0G: registry synoptic row total_rows={rt} total_patients={rp} "
            f"canonical_version={cv}")
        _gate(out, "registry_total_rows_stale_eq_11037", rt == EXPECTED_STALE_REGISTRY_ROWS,
              {"observed": int(rt) if rt is not None else None,
               "expected": EXPECTED_STALE_REGISTRY_ROWS})
        _gate(out, "registry_total_patients_stale_eq_5641", rp == EXPECTED_STALE_REGISTRY_PATIENTS,
              {"observed": int(rp) if rp is not None else None,
               "expected": EXPECTED_STALE_REGISTRY_PATIENTS})
        _gate(out, "registry_not_already_updated_to_post_rerun",
              not (rt == EXPECTED_SOURCE_ROWS and rp == EXPECTED_SOURCE_RIDS),
              {"detail": "registry already shows post-rerun counts — someone updated it"})

    # ── 0H — count rids_with_entity_value (post-promotion target) ──
    rids_w_ent = con.execute(
        f"""
        SELECT COUNT(DISTINCT research_id)
        FROM main.{SOURCE_TABLE}
        WHERE result_json LIKE '{ENTITY_LIKE_PRESENT}'
          AND result_json NOT LIKE '{EMPTY_ENTITIES_PATTERN}'
        """
    ).fetchone()[0]
    rows_w_ent = con.execute(
        f"""
        SELECT COUNT(*)
        FROM main.{SOURCE_TABLE}
        WHERE result_json LIKE '{ENTITY_LIKE_PRESENT}'
          AND result_json NOT LIKE '{EMPTY_ENTITIES_PATTERN}'
        """
    ).fetchone()[0]
    out["observed"]["source_rids_with_entity_value"] = int(rids_w_ent)
    out["observed"]["source_rows_with_entity_value"] = int(rows_w_ent)
    log(f"  0H: source rids_with_entity_value={rids_w_ent} "
        f"(expected {EXPECTED_RIDS_WITH_ENTITY}; source-integrity, all extracted)")
    log(f"      source rows_with_entity_value={rows_w_ent} "
        f"(used by Phase 2 sum-gate sanity check)")
    _gate(out, "source_rids_with_entity_eq_4992", rids_w_ent == EXPECTED_RIDS_WITH_ENTITY,
          {"observed": int(rids_w_ent), "expected": EXPECTED_RIDS_WITH_ENTITY})

    # 0H' — positive-entity counts via the SHARED ROLLUP_BASE_CTE (the exact
    # CTE Phase 2's UPDATE will use). Pins Phase 2's promotion target at
    # Phase 0 so any drift between gate and mutation is impossible.
    pos_row = con.execute(
        f"""
        WITH {ROLLUP_BASE_CTE}
        SELECT
            COUNT(DISTINCT research_id)                     AS pos_rids,
            COUNT(DISTINCT (research_id || '|' || note_row_id)) AS pos_notes
        FROM pos
        """
    ).fetchone()
    pos_rids, pos_notes = pos_row
    out["observed"]["source_rids_with_positive_entity"] = int(pos_rids)
    out["observed"]["source_notes_with_positive_entity"] = int(pos_notes)
    log(f"      source rids_with_positive_entity={pos_rids} "
        f"(expected {EXPECTED_RIDS_WITH_POSITIVE_ENTITY}; Phase 2 promotion target)")
    log(f"      source notes_with_positive_entity={pos_notes} "
        f"(expected {EXPECTED_NOTES_WITH_POSITIVE_ENTITY}; Phase 2 sum_n_notes target)")
    _gate(out, "source_rids_with_positive_entity_eq_4835",
          pos_rids == EXPECTED_RIDS_WITH_POSITIVE_ENTITY,
          {"observed": int(pos_rids), "expected": EXPECTED_RIDS_WITH_POSITIVE_ENTITY})

    # ── 0I — known archive snapshot of the qwen3:32b pre-rerun source ──
    pre_archive_present = con.execute(
        f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog=? AND table_schema=?
          AND table_name LIKE 'note_entities_llm_synoptic_pathology_enrichment_pre%'
        LIMIT 1
        """,
        [ARCHIVE_DB, ARCHIVE_SCHEMA],
    ).fetchone() is not None
    out["observed"]["pre_rerun_source_archive_snapshot_present"] = pre_archive_present
    _gate(out, "pre_rerun_source_archive_snapshot_present", pre_archive_present,
          {"detail": (
              f"expected at least one snapshot in "
              f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA} matching '
              "note_entities_llm_synoptic_pathology_enrichment_pre%")})

    # ── done ──
    out["finished_at"] = utcnow_iso()
    out["all_gates_passed"] = len(out["blockers"]) == 0
    PREFLIGHT_PATH.write_text(json.dumps(out, indent=2, default=str))
    log("")
    log("──── PHASE 0 SUMMARY ────")
    log(f"  gates: {len(out['gates'])} total, "
        f"{sum(1 for g in out['gates'] if g['ok'])} passed, "
        f"{len(out['blockers'])} blockers")
    log(f"  preflight JSON: {PREFLIGHT_PATH.relative_to(REPO_ROOT)}")
    if out["blockers"]:
        log("  BLOCKERS:")
        for b in out["blockers"]:
            log(f"    - {b['name']}: {b['detail']}")
        raise PreflightHalt(
            f"{len(out['blockers'])} blocker(s) — see {PREFLIGHT_PATH}"
        )
    log("  ALL GATES PASSED — Phase 1 may proceed.")
    log("─────────────────────────")
    return out


# ── PHASE 1 — pre-mutation snapshot of CPM ───────────────────────────────────


def _existing_pre280_snapshots(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog=? AND table_schema=?
          AND table_name LIKE 'canonical_patient_master_pre280_%'
        ORDER BY table_name
        """,
        [ARCHIVE_DB, ARCHIVE_SCHEMA],
    ).fetchall()
    return [r[0] for r in rows]


def phase_1(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 1 — pre-mutation snapshot of CPM ===")
    out: dict[str, Any] = {"phase": 1, "started_at": utcnow_iso(), "gates": [], "blockers": []}

    existing = _existing_pre280_snapshots(con)
    if existing:
        snap_table = existing[-1]
        snap_fq = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{snap_table}'
        log(f"  idempotent reuse: {snap_table} already exists "
            f"(found {len(existing)} pre280 snapshot(s); using newest)")
        out["ctas_action"] = "reused_existing"
        out["existing_pre280_snapshots"] = existing
    else:
        ts = utcnow_compact()
        snap_table = f"canonical_patient_master_pre280_{ts}"
        snap_fq = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{snap_table}'
        log(f"  CTAS {snap_fq} AS SELECT * FROM main.{CPM_TABLE} …")
        con.execute(
            f"CREATE TABLE {snap_fq} AS SELECT * FROM main.{CPM_TABLE}"
        )
        out["ctas_action"] = "created_new"

    # Build COMMENT inline (DuckDB does not accept ? placeholder in COMMENT IS).
    # Single quotes in the comment text get doubled per SQL escape rules.
    comment = (
        f"Script 280 pre-mutation snapshot of {CPM_TABLE}. "
        "Reason: pre-rollup snapshot before re-promoting the nlp_synoptic_* "
        f"family from {EXPECTED_STALE_HAS_DATA} patients to "
        f"~{HAS_DATA_TARGET} patients. Source: "
        f"{SOURCE_TABLE} (qwen2.5-32b, 2026-04-19 load). "
        f"Created at {utcnow_iso()}."
    )
    comment_sql_literal = "'" + comment.replace("'", "''") + "'"
    log(f"  COMMENT ON TABLE {snap_fq} IS {comment_sql_literal[:80]}…")
    try:
        con.execute(f"COMMENT ON TABLE {snap_fq} IS {comment_sql_literal}")
        out["comment_action"] = "applied"
    except duckdb.Error as exc:
        log(f"  ⚠ COMMENT failed ({exc!r}) — snapshot still valid; recording in JSON")
        out["comment_action"] = f"failed: {exc!r}"
    out["comment"] = comment

    snap_row = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {snap_fq}"
    ).fetchone()
    snap_rows, snap_rids = snap_row
    log(f"  snapshot {snap_table}: rows={snap_rows:,} rids={snap_rids:,}")
    out["snapshot_table"] = snap_fq
    out["snapshot_table_unqualified"] = snap_table
    out["snapshot_rows"] = int(snap_rows)
    out["snapshot_rids"] = int(snap_rids)
    _gate(out, "snapshot_rows_eq_10871", snap_rows == EXPECTED_CPM_ROWS,
          {"observed": int(snap_rows), "expected": EXPECTED_CPM_ROWS})
    _gate(out, "snapshot_rids_eq_10871", snap_rids == EXPECTED_CPM_RIDS,
          {"observed": int(snap_rids), "expected": EXPECTED_CPM_RIDS})

    out["source_table_snapshot_skipped"] = True
    out["source_table_snapshot_skipped_reason"] = (
        "Phase B is read-only to the source table; the qwen3:32b pre-rerun state "
        f"is already preserved in '{ARCHIVE_DB}'.{ARCHIVE_SCHEMA}."
        "note_entities_llm_synoptic_pathology_enrichment_pre%."
    )
    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 1 blockers: {out['blockers']}")
    return out


# ── PHASE 2 — rollup re-promotion on CPM ─────────────────────────────────────

# Priority ordering for key_finding selection (Phase A spec).
KEY_FINDING_PRIORITY = [
    ("tumor_variant",  1),
    ("pT_stage",       2),
    ("pN_stage",       3),
    ("margin_status",  4),
    ("lvi_present",    5),
    ("ete",            6),
    ("multifocality",  7),
]


def _key_finding_priority_case(value_alias: str) -> str:
    """Build CASE WHEN entity_type = ... THEN <priority> END string."""
    parts = ["CASE"]
    for et, prio in KEY_FINDING_PRIORITY:
        parts.append(f"WHEN {value_alias} = '{et}' THEN {prio}")
    parts.append("ELSE 99 END")
    return " ".join(parts)


def phase_2(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 2 — rollup re-promotion on canonical_patient_master ===")
    out: dict[str, Any] = {"phase": 2, "started_at": utcnow_iso(), "gates": [], "blockers": []}

    # Build per-RID rollup using the SHARED ROLLUP_BASE_CTE (parsed → flat →
    # ext → pos), extended with the prompt's explicit key_finding priority
    # ordering. The downstream `ranked` and `per_rid` CTEs read from `pos`,
    # so Script-212-parity present-only + confidence>=0.5 filtering is in
    # effect here. Phase 0's source_rids_with_positive_entity_eq_4835 gate
    # already pinned the expected RID/note count using this same CTE.
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
    log("  building per-RID rollup CTE → TEMP TABLE _rollup_280 …")
    # Materialize the rollup directly on the MotherDuck side (avoids a
    # pandas round-trip; duckdb 1.1.3 + pandas 3.x register() rejects the
    # 'str' dtype that .df() produces for VARCHAR columns).
    con.execute(f"CREATE OR REPLACE TEMP TABLE _rollup_280 AS {rollup_sql}")
    rollup_summary = con.execute(
        """
        SELECT
            COUNT(*)                          AS distinct_rids,
            COALESCE(SUM(n_notes_with_entity), 0) AS sum_n_notes,
            COALESCE(SUM(total_entities),     0) AS sum_n_entities
        FROM _rollup_280
        """
    ).fetchone()
    out["rollup_distinct_rids"]      = int(rollup_summary[0])
    out["rollup_n_notes_sum"]        = int(rollup_summary[1])
    out["rollup_total_entities_sum"] = int(rollup_summary[2])
    log(f"    rollup distinct RIDs={out['rollup_distinct_rids']}  "
        f"sum_n_notes={out['rollup_n_notes_sum']}  "
        f"sum_n_entities={out['rollup_total_entities_sum']}")

    log("  UPDATE main.canonical_patient_master from rollup …")
    con.execute(
        f"""
        UPDATE main.{CPM_TABLE} cpm
        SET nlp_synoptic_has_data    = (r.n_notes_with_entity > 0),
            nlp_synoptic_n_notes     = r.n_notes_with_entity,
            nlp_synoptic_n_entities  = r.total_entities,
            nlp_synoptic_key_finding = r.key_finding
        FROM _rollup_280 r
        WHERE cpm.research_id = r.research_id
        """
    )
    log("  UPDATE patients absent from rollup → FALSE/0/0/NULL …")
    con.execute(
        f"""
        UPDATE main.{CPM_TABLE} cpm
        SET nlp_synoptic_has_data    = FALSE,
            nlp_synoptic_n_notes     = 0,
            nlp_synoptic_n_entities  = 0,
            nlp_synoptic_key_finding = NULL
        WHERE cpm.research_id NOT IN (SELECT research_id FROM _rollup_280)
        """
    )

    post = con.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE nlp_synoptic_has_data) AS has_true,
            COUNT(*) FILTER (WHERE nlp_synoptic_has_data IS NULL) AS still_null,
            SUM(nlp_synoptic_n_notes) AS sum_n_notes,
            COUNT(*) FILTER (WHERE research_id IS NULL) AS null_rid
        FROM main.{CPM_TABLE}
        """
    ).fetchone()
    has_true, still_null, sum_n_notes, null_rid = post
    out["post_has_data_true"] = int(has_true)
    out["post_has_data_null"] = int(still_null)
    out["post_sum_n_notes"]   = int(sum_n_notes or 0)
    log(f"  post-update: has_data TRUE={has_true} "
        f"sum_n_notes={sum_n_notes} null_rid={null_rid}")
    _gate(out, "post_has_data_within_4835_pm10",
          abs(int(has_true) - HAS_DATA_TARGET) <= HAS_DATA_TOLERANCE,
          {"observed": int(has_true),
           "target": HAS_DATA_TARGET, "tolerance": HAS_DATA_TOLERANCE})
    _gate(out, "post_no_null_has_data", still_null == 0, {"observed": int(still_null)})
    _gate(out, "post_sum_n_notes_eq_source_pos_notes",
          int(sum_n_notes or 0) == EXPECTED_NOTES_WITH_POSITIVE_ENTITY,
          {"observed": int(sum_n_notes or 0),
           "expected": EXPECTED_NOTES_WITH_POSITIVE_ENTITY})
    _gate(out, "post_no_null_research_id", null_rid == 0, {"observed": int(null_rid)})

    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 2 blockers: {out['blockers']}")
    return out


# ── PHASE 3 — registry + dictionary update ───────────────────────────────────


def phase_3(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 3 — registry + dictionary sync ===")
    out: dict[str, Any] = {"phase": 3, "started_at": utcnow_iso(), "gates": [], "blockers": []}

    desc_suffix = (
        " | Script 280 (2026-04-19): rollup re-promoted against qwen2.5-32b "
        "rerun data (previously re-loaded from parquet outside of phase scripts). "
        f"CPM.nlp_synoptic_has_data went {EXPECTED_STALE_HAS_DATA} → ~{HAS_DATA_TARGET}."
    )
    desc_marker = "Script 280 (2026-04-19): rollup re-promoted"
    # Always update the count/version fields (idempotent — same value).
    # Append description suffix ONLY if the marker isn't already present, so
    # repeated --phase 3 invocations don't double-append.
    #
    # NOTE on future re-extraction: EXPECTED_SOURCE_ROWS / EXPECTED_SOURCE_RIDS
    # are pinned to the 2026-04-19 qwen2.5-32b rerun (26584 / 10862). If a
    # future re-extraction grows the source table, this UPDATE will silently
    # understate the registry until Phase 0's source-hash gate catches the
    # drift and HALTs. That guard is intentional — Phase 0 is the single
    # source-of-truth for "the source table matches what the rollup expects".
    con.execute(
        f"""
        UPDATE {WS_SCHEMA}.{REGISTRY_TABLE}
        SET total_rows        = {EXPECTED_SOURCE_ROWS},
            total_patients    = {EXPECTED_SOURCE_RIDS},
            canonical_version = 'v1_0_script280',
            description       = CASE
              WHEN description LIKE ? THEN description
              ELSE COALESCE(description, '') || ?
            END
        WHERE detail_table_name = ?
        """,
        [f"%{desc_marker}%", desc_suffix, SOURCE_TABLE],
    )
    reg = con.execute(
        f"""
        SELECT total_rows, total_patients, canonical_version
        FROM {WS_SCHEMA}.{REGISTRY_TABLE}
        WHERE detail_table_name = ?
        """,
        [SOURCE_TABLE],
    ).fetchone()
    rt, rp, cv = reg
    out["registry_after"] = {
        "total_rows": int(rt), "total_patients": int(rp), "canonical_version": cv,
    }
    _gate(out, "registry_total_rows_post_eq_26584",  rt == EXPECTED_SOURCE_ROWS,
          {"observed": int(rt), "expected": EXPECTED_SOURCE_ROWS})
    _gate(out, "registry_total_patients_post_eq_10862", rp == EXPECTED_SOURCE_RIDS,
          {"observed": int(rp), "expected": EXPECTED_SOURCE_RIDS})

    # Per-column descriptions (verbatim from the user's review on 2026-04-19;
    # see THYROID_2026 chat log "Phase 3 dict spec"). Each row gets its own
    # text — the 4 columns have distinct semantics that one blanket string
    # would obscure.
    DESC_BY_COL: dict[str, str] = {
        "nlp_synoptic_has_data": (
            "TRUE iff patient has >=1 positive-entity extraction "
            "(confidence>=0.5, present_or_negated='present' OR NULL) from "
            "note_entities_llm_synoptic_pathology_enrichment; FALSE if "
            "patient is in source with no positive entities or absent from "
            "source; NULL for no CPM patient."
        ),
        "nlp_synoptic_n_notes": (
            "Count of distinct notes per patient with >=1 positive extracted "
            "entity. 0 when has_data=FALSE."
        ),
        "nlp_synoptic_n_entities": (
            "Total positive entities extracted across all notes for this "
            "patient. 0 when has_data=FALSE."
        ),
        "nlp_synoptic_key_finding": (
            "Highest-priority entity value for this patient, resolved via "
            "KEY_FINDING_PRIORITY (tumor_variant > pT_stage > pN_stage > "
            "margin_status > lvi_present > ete > multifocality > other) "
            "tiebroken by confidence DESC then entity_value alpha. NULL "
            "when has_data=FALSE."
        ),
    }
    # data_dictionary_v279 has no `provenance_note` column; the project's
    # convention is to record script-era notes in the `v279_note` text field
    # (initialized to '' by script 271b on 2026-04-18). Also touch
    # rebuilt_at/rebuilt_by so the audit trail of which script last populated
    # these rows stays accurate.
    v279_note = (
        "Re-promoted by Script 280 (2026-04-19) from qwen2.5-32b synoptic rerun."
    )
    rebuilt_by = "script280_2026-04-19"
    # CRITICAL: existing rows have description = '' (empty string), NOT NULL.
    # COALESCE('', ?) would return '' (no-op). Use COALESCE(NULLIF(d,''), ?)
    # so empty strings are treated as "needs default" while non-empty existing
    # text is preserved. v279_note is overwritten unconditionally — it should
    # always reflect the latest script that touched the row.
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
    dict_check = con.execute(
        f"""
        SELECT column_name,
               description AS description,
               v279_note   AS v279_note,
               rebuilt_by  AS rebuilt_by
        FROM main.{DICTIONARY_TABLE}
        WHERE table_name = ?
          AND column_name IN ('nlp_synoptic_has_data', 'nlp_synoptic_n_notes',
                              'nlp_synoptic_n_entities', 'nlp_synoptic_key_finding')
        ORDER BY column_name
        """,
        [CPM_TABLE],
    ).df()
    # Drift-resistant gate: assert each row's description equals the EXACT
    # expected per-column string, plus v279_note + rebuilt_by match. Catches
    # the COALESCE-on-empty-string bug because empty descriptions would not
    # equal the expected non-empty text.
    expected_desc_per_row = dict_check["column_name"].map(DESC_BY_COL)
    descriptions_exact_match = (dict_check["description"] == expected_desc_per_row).all()
    out["dictionary_after"] = dict_check.to_dict(orient="records")
    out["dictionary_descriptions_match_expected"] = bool(descriptions_exact_match)
    _gate(out, "dictionary_4_cols_have_exact_per_col_desc_and_v279_note",
          len(dict_check) == 4
          and descriptions_exact_match
          and (dict_check["v279_note"] == v279_note).all()
          and (dict_check["rebuilt_by"] == rebuilt_by).all(),
          {"rows": dict_check.to_dict(orient="records"),
           "descriptions_exact_match": bool(descriptions_exact_match)})

    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 3 blockers: {out['blockers']}")
    return out


# ── PHASE 4 — main-schema hygiene sweep ──────────────────────────────────────

DEPRECATED_PATTERNS = [
    "%_deprecated%",
    "%_legacy%",
    "%_backup%",
    "%_predup%",
    "%_dropped_%",
    "%_obsolete%",
    "%_tmp%",
]
DEPRECATED_REGEX = "_pre[0-9]{3,}|_v[0-9]+_old"


def phase_4(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 4 — main-schema hygiene sweep ===")
    out: dict[str, Any] = {"phase": 4, "started_at": utcnow_iso(), "gates": [], "blockers": []}

    where_clauses = [
        f"LOWER(table_name) LIKE '{p}'" for p in DEPRECATED_PATTERNS
    ]
    where_clauses.append(f"REGEXP_MATCHES(LOWER(table_name), '{DEPRECATED_REGEX}')")
    where_sql = " OR ".join(where_clauses)
    rows = con.execute(
        f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog=? AND table_schema='main'
          AND ({where_sql})
        ORDER BY table_name
        """,
        [CANONICAL_DB],
    ).fetchall()
    out["deprecated_named_tables_in_main"] = [r[0] for r in rows]
    log(f"  found {len(rows)} candidate deprecated/legacy tables in main")
    actions: list[dict[str, Any]] = []
    for (name,) in rows:
        archive_present = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_catalog=? AND table_schema=? AND table_name=?",
            [ARCHIVE_DB, ARCHIVE_SCHEMA, name],
        ).fetchone() is not None
        action = {"table": name, "archive_present": archive_present}
        if not archive_present:
            con.execute(
                f'CREATE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{name} AS '
                f"SELECT * FROM main.{name}"
            )
            action["archived"] = True
        else:
            action["archived"] = "already"
        con.execute(f"DROP TABLE main.{name}")
        action["dropped_from_main"] = True
        actions.append(action)
        log(f"    {name}: archive_present={archive_present}, dropped from main")
    out["actions"] = actions

    pairs = con.execute(
        f"""
        WITH base AS (
            SELECT table_name,
                   regexp_replace(table_name, '_v[0-9]+$', '') AS stem,
                   regexp_extract(table_name, '_v([0-9]+)$', 1) AS ver
            FROM information_schema.tables
            WHERE table_catalog=? AND table_schema='main'
              AND regexp_matches(table_name, '_v[0-9]+$')
        )
        SELECT stem, COUNT(*) AS n_versions, STRING_AGG(table_name, ',' ORDER BY ver) AS members
        FROM base
        GROUP BY stem
        HAVING COUNT(*) > 1
        ORDER BY stem
        """,
        [CANONICAL_DB],
    ).fetchall()
    out["duplicate_version_pairs"] = [
        {"stem": s, "n_versions": int(n), "members": m.split(",")}
        for s, n, m in pairs
    ]
    if pairs:
        log(f"  ⚠ {len(pairs)} duplicate version pair(s) — flagged, not auto-dropped")
    _gate(out, "no_duplicate_version_pairs", len(pairs) == 0,
          {"pairs": out["duplicate_version_pairs"]})

    rows_after = con.execute(
        f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog=? AND table_schema='main'
          AND ({where_sql})
        """,
        [CANONICAL_DB],
    ).fetchone()[0]
    out["deprecated_named_tables_in_main_after"] = int(rows_after)
    _gate(out, "main_clean_of_deprecated_after_sweep", rows_after == 0,
          {"observed": int(rows_after)})

    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 4 blockers: {out['blockers']}")
    return out


# ── PHASE 5 — __readme + dictionary audit rebuild ────────────────────────────


# Pre-existing dictionary-completeness gaps that Script 280 inherits but does
# NOT own. Pinned by NAME (not by delta magnitude) so any drift — a new
# undocumented column or a new orphan dict table — fails the gate loudly.
#
# CPM TIRADS Part B (2026-04-21): the 10 tirads_v2_* CPM columns previously
# listed here were dropped from canonical_patient_master. Canonical TIRADS now
# lives on canonical_us_patient_master_VIEW_v2 (cupm_v2). The frozenset is empty
# post-Part-B because there are no longer "undocumented CPM tirads_v2_* cols"
# to allowlist — they're gone, not undocumented.
# Future cupm_v2 dict gaps (if any) get tracked in a separate allowlist on
# the cupm_v2 dictionary generator (out of scope here).
KNOWN_UNDOCUMENTED_CPM_COLS: frozenset[str] = frozenset()

# Whole-table orphan: dict has rows for a table that doesn't exist in main.
# Will be cleaned by a future dict-rebuild pass (Script 272-style).
KNOWN_ORPHAN_DICT_TABLES: frozenset[str] = frozenset({
    "canonical_patient_master_archived",
})


def phase_5(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 5 — __readme + dictionary audit ===")
    out: dict[str, Any] = {"phase": 5, "started_at": utcnow_iso(), "gates": [], "blockers": []}

    # Record the known-gap allowlists so downstream tracking sees what Script
    # 280 expects to be true today.
    out["known_undocumented_cpm_cols"] = sorted(KNOWN_UNDOCUMENTED_CPM_COLS)
    out["known_orphan_dict_tables"] = sorted(KNOWN_ORPHAN_DICT_TABLES)
    out["allowlist_resolution_owners"] = {
        "known_undocumented_cpm_cols": (
            "RESOLVED 2026-04-21: 10 tirads_v2_* columns dropped from CPM via "
            "CPM TIRADS Part B; canonical TIRADS on canonical_us_patient_master_VIEW_v2."
        ),
        "known_orphan_dict_tables": (
            "Future dictionary-rebuild pass (Script 272-style) — drops dict "
            "rows whose table_name no longer exists in main."
        ),
    }

    # Top-level totals (informational only, not gated — preserves the prior
    # observed values for reference even though we no longer assert equality).
    base_tables = con.execute(
        f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog=? AND table_schema='main' AND table_type='BASE TABLE'
        """,
        [CANONICAL_DB],
    ).fetchone()[0]
    info_cols_total = con.execute(
        f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog=? AND table_schema='main'
        """,
        [CANONICAL_DB],
    ).fetchone()[0]
    dict_rows = con.execute(
        f"SELECT COUNT(*) FROM main.{DICTIONARY_TABLE}"
    ).fetchone()[0]
    out["base_tables_in_main"] = int(base_tables)
    out["info_schema_columns_for_main_tables"] = int(info_cols_total)
    out["dictionary_row_count"] = int(dict_rows)
    log(f"  totals: base_tables={base_tables}  "
        f"info_schema_cols_total={info_cols_total}  dict_rows={dict_rows}")

    # Per-documented-table consistency. For each table the dict claims to
    # document, compare its column set against information_schema.columns:
    #   info_only := info_cols - dict_cols  (CPM cols missing from dict)
    #     → must be ⊆ KNOWN_UNDOCUMENTED_CPM_COLS for canonical_patient_master,
    #       == ∅ for any other table
    #   dict_only := dict_cols - info_cols  (dict rows for a column that no
    #                                         longer exists on the table)
    #     → must always be == ∅ (strict)
    documented_tables = [
        r[0]
        for r in con.execute(
            f"""
            SELECT DISTINCT table_name
            FROM main.{DICTIONARY_TABLE}
            ORDER BY table_name
            """
        ).fetchall()
    ]
    out["documented_tables_in_dict"] = documented_tables

    per_table_audit: list[dict[str, Any]] = []
    any_unexpected_undocumented = False
    any_unexpected_orphan_col = False

    for tn in documented_tables:
        if tn in KNOWN_ORPHAN_DICT_TABLES:
            # Whole-table orphan handled by the next gate; skip per-column.
            continue
        info_set = {
            r[0]
            for r in con.execute(
                f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_catalog=? AND table_schema='main' AND table_name=?
                """,
                [CANONICAL_DB, tn],
            ).fetchall()
        }
        dict_set = {
            r[0]
            for r in con.execute(
                f"SELECT column_name FROM main.{DICTIONARY_TABLE} "
                "WHERE table_name=?",
                [tn],
            ).fetchall()
        }
        info_only = info_set - dict_set
        dict_only = dict_set - info_set

        # Allowlist applies to CPM only; other tables must be 0/0.
        allowed = (
            KNOWN_UNDOCUMENTED_CPM_COLS if tn == "canonical_patient_master" else frozenset()
        )
        undocumented_unexpected = sorted(info_only - allowed)
        orphan_unexpected = sorted(dict_only)

        per_table_audit.append({
            "table": tn,
            "info_cols": len(info_set),
            "dict_cols": len(dict_set),
            "info_only_count": len(info_only),
            "info_only_sample": sorted(info_only)[:20],
            "dict_only_count": len(dict_only),
            "dict_only_sample": sorted(dict_only)[:20],
            "undocumented_unexpected": undocumented_unexpected,
            "orphan_unexpected": orphan_unexpected,
        })
        if undocumented_unexpected:
            any_unexpected_undocumented = True
        if orphan_unexpected:
            any_unexpected_orphan_col = True
        log(f"  {tn}: info_cols={len(info_set)} dict_cols={len(dict_set)} "
            f"info_only={len(info_only)} dict_only={len(dict_only)} "
            f"unexpected_undoc={len(undocumented_unexpected)} "
            f"unexpected_orphan_col={len(orphan_unexpected)}")

    out["per_table_audit"] = per_table_audit
    _gate(out,
          "documented_tables_no_unexpected_undocumented_cols",
          not any_unexpected_undocumented,
          {"per_table": per_table_audit,
           "allowlist": sorted(KNOWN_UNDOCUMENTED_CPM_COLS),
           "note": (
               "(info_cols - dict_cols) ⊆ KNOWN_UNDOCUMENTED_CPM_COLS for "
               "canonical_patient_master; == ∅ for other documented tables."
           )})
    _gate(out,
          "documented_tables_no_orphan_dict_cols",
          not any_unexpected_orphan_col,
          {"per_table": per_table_audit,
           "note": "(dict_cols - info_cols) == ∅ for every documented table."})

    # Whole-table orphan gate: dict claims to document a table that does not
    # exist in main. Allowlist == KNOWN_ORPHAN_DICT_TABLES; anything else fails.
    actual_main_tables = {
        r[0]
        for r in con.execute(
            f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_catalog=? AND table_schema='main'
            """,
            [CANONICAL_DB],
        ).fetchall()
    }
    orphan_tables = sorted(set(documented_tables) - actual_main_tables)
    unexpected_orphan_tables = sorted(set(orphan_tables) - KNOWN_ORPHAN_DICT_TABLES)
    out["orphan_dict_tables"] = orphan_tables
    out["unexpected_orphan_tables"] = unexpected_orphan_tables
    _gate(out,
          "no_unexpected_orphan_dict_tables",
          not unexpected_orphan_tables,
          {"orphan_tables_observed": orphan_tables,
           "allowlist": sorted(KNOWN_ORPHAN_DICT_TABLES),
           "unexpected": unexpected_orphan_tables})

    out["readme_regeneration_skipped"] = True
    out["readme_regeneration_skipped_reason"] = (
        "Script 280 is rollup-only; __readme regeneration belongs to Script 272."
    )
    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 5 blockers: {out['blockers']}")
    return out


# ── PHASE 6 — end-to-end validation ──────────────────────────────────────────


def phase_6(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=== PHASE 6 — end-to-end validation ===")
    out: dict[str, Any] = {"phase": 6, "started_at": utcnow_iso(), "gates": [], "blockers": []}

    # A
    cpm_row = con.execute(
        f"""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE research_id IS NULL),
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL)
        FROM main.{CPM_TABLE}
        """
    ).fetchone()
    n_cpm, null_rid, null_fna = cpm_row
    _gate(out, "A_cpm_rows_eq_10871", n_cpm == EXPECTED_CPM_ROWS, {"observed": int(n_cpm)})
    _gate(out, "A_cpm_no_null_rid", null_rid == 0, {"observed": int(null_rid)})
    _gate(out, "A_cpm_no_null_fna", null_fna == 0, {"observed": int(null_fna)})

    # B
    src_row = con.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               STRING_AGG(DISTINCT llm_model, ',' ORDER BY llm_model)
        FROM main.{SOURCE_TABLE}
        """
    ).fetchone()
    s_rows, s_rids, s_models = src_row
    _gate(out, "B_source_rows_unchanged", s_rows == EXPECTED_SOURCE_ROWS,
          {"observed": int(s_rows)})
    _gate(out, "B_source_rids_unchanged", s_rids == EXPECTED_SOURCE_RIDS,
          {"observed": int(s_rids)})
    _gate(out, "B_source_model_unchanged", s_models == EXPECTED_SOURCE_MODEL,
          {"observed": s_models})

    # C
    has_true = con.execute(
        f"SELECT COUNT(*) FILTER (WHERE nlp_synoptic_has_data) FROM main.{CPM_TABLE}"
    ).fetchone()[0]
    _gate(out, "C_has_data_within_4835_pm10",
          abs(int(has_true) - HAS_DATA_TARGET) <= HAS_DATA_TOLERANCE,
          {"observed": int(has_true), "target": HAS_DATA_TARGET})

    # D
    where_clauses = [f"LOWER(table_name) LIKE '{p}'" for p in DEPRECATED_PATTERNS]
    where_clauses.append(f"REGEXP_MATCHES(LOWER(table_name), '{DEPRECATED_REGEX}')")
    deprecated_after = con.execute(
        f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog=? AND table_schema='main' AND ({" OR ".join(where_clauses)})
        """,
        [CANONICAL_DB],
    ).fetchone()[0]
    _gate(out, "D_no_deprecated_named_tables_in_main", deprecated_after == 0,
          {"observed": int(deprecated_after)})

    # E
    needs_review = con.execute(
        f"""
        SELECT COUNT(*) FROM {WS_SCHEMA}.{REGISTRY_TABLE}
        WHERE needs_manual_review = TRUE
        """
    ).fetchone()[0]
    _gate(out, "E_registry_needs_manual_review_eq_0", needs_review == 0,
          {"observed": int(needs_review)})

    # F — filter_type_provisional lives on manuscript_dive_map_v1, NOT on
    # detail_table_registry_v1 (the Phase B prompt pointed at the wrong
    # table; verified 2026-04-19 via information_schema.columns search).
    thin_provisional = con.execute(
        f"""
        SELECT COUNT(*) FROM {WS_SCHEMA}.manuscript_dive_map_v1
        WHERE filter_type_provisional = TRUE
        """
    ).fetchone()[0]
    _gate(out, "F_thin_wrapper_provisional_eq_24",
          thin_provisional == EXPECTED_THIN_WRAPPER_PROVISIONAL,
          {"observed": int(thin_provisional), "expected": EXPECTED_THIN_WRAPPER_PROVISIONAL,
           "source_table": f"{WS_SCHEMA}.manuscript_dive_map_v1"})

    # G — baseline-allowlist gate (replaces "count == 67" magnitude check).
    # Asserts every view in BASELINE_WS_VIEWS is still present; new views
    # added after the baseline snapshot are recorded for audit but allowed.
    actual_ws_views = {
        r[0]
        for r in con.execute(
            f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_catalog=? AND table_schema=? AND table_type='VIEW'
            """,
            [CANONICAL_DB, WS_SCHEMA],
        ).fetchall()
    }
    missing_baseline = sorted(BASELINE_WS_VIEWS - actual_ws_views)
    additions_beyond_baseline = sorted(actual_ws_views - BASELINE_WS_VIEWS)
    _gate(out, "G_baseline_ws_views_all_present",
          not missing_baseline,
          {"baseline_count": len(BASELINE_WS_VIEWS),
           "actual_count": len(actual_ws_views),
           "missing_baseline": missing_baseline,
           "additions_beyond_baseline": additions_beyond_baseline,
           "note": (
               "Workspace can grow (new cohort views land routinely); "
               "we only fail if a baseline view is deleted. Additions are "
               "logged for audit."
           )})

    out["observed"] = {
        "cpm_n_rows": int(n_cpm),
        "cpm_null_rid": int(null_rid),
        "cpm_null_fna": int(null_fna),
        "source_n_rows": int(s_rows),
        "source_n_rids": int(s_rids),
        "source_models": s_models,
        "post_has_data_true": int(has_true),
        "deprecated_named_tables_after": int(deprecated_after),
        "registry_needs_manual_review": int(needs_review),
        "thin_wrapper_provisional": int(thin_provisional),
        "manuscript_workspace_view_count_actual": len(actual_ws_views),
        "manuscript_workspace_view_count_baseline": len(BASELINE_WS_VIEWS),
        "manuscript_workspace_views_missing_from_baseline": missing_baseline,
        "manuscript_workspace_views_additions_beyond_baseline": additions_beyond_baseline,
    }
    out["finished_at"] = utcnow_iso()
    if out["blockers"]:
        raise RuntimeError(f"Phase 6 blockers: {out['blockers']}")
    return out


# ── CLI ──────────────────────────────────────────────────────────────────────


PHASE_FUNCS = {
    0: phase_0,
    1: phase_1,
    2: phase_2,
    3: phase_3,
    4: phase_4,
    5: phase_5,
    6: phase_6,
}


def _save_decisions(decisions: dict[str, Any]) -> None:
    DECISIONS_PATH.write_text(json.dumps(decisions, indent=2, default=str))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        choices=["0", "1", "2", "3", "4", "5", "6", "all"],
        default="0",
    )
    args = parser.parse_args()

    decisions: dict[str, Any] = {
        "script": "scripts/280_synoptic_rollup_rebuild.py",
        "started_at": utcnow_iso(),
        "phase_arg": args.phase,
        "phases": {},
    }

    con = connect()
    rc = 0
    try:
        if args.phase == "all":
            for p in (0, 1, 2, 3, 4, 5, 6):
                decisions["phases"][str(p)] = PHASE_FUNCS[p](con)
                _save_decisions(decisions)
        else:
            p = int(args.phase)
            decisions["phases"][str(p)] = PHASE_FUNCS[p](con)
            _save_decisions(decisions)
    except PreflightHalt as exc:
        log(f"PREFLIGHT HALT: {exc}")
        rc = 2
    except Exception as exc:  # noqa: BLE001
        log(f"FATAL: {exc!r}")
        rc = 1
    finally:
        decisions["finished_at"] = utcnow_iso()
        decisions["return_code"] = rc
        _save_decisions(decisions)
        _flush_log()
        con.close()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
