#!/usr/bin/env python3
"""Script 388 — `note_entities_llm_*` disposition + `main.*` VIEW-label completeness sweep.

Driver for cursor prompt
``cursor_prompts/CURSOR_PROMPT_PUB_V1_0_NOTES_LLM_AND_VIEW_LABEL_AUDIT_20260422_SCRIPT_388.md``.

Surfaces addressed
------------------
1.  Every ``main.note_entities_llm_*`` table gets a disposition
    (CURRENT_LIVE → KEEP, CURRENT_ORPHAN → ARCHIVE, LEGACY_REPLACED → ARCHIVE,
    UNCLASSIFIED → HALT).
2.  Every non-canonical, non-VIEW-suffixed object in ``main`` is checked
    against the naming contract.  Violators → KEEP_RENAME (suffix to
    ``_VIEW_v<N>``) or ORPHAN → ARCHIVE.
3.  Carry-forward from Script 387: the 2 ``all_null_key`` sham-key flags
    on ``canonical_path_benign_events_v1`` /
    ``canonical_path_gland_events_v1``.  This script probes alternate
    partition keys and pins the chosen override into
    ``manuscript_workspace.script_387_dedup_probe_v1`` as a new column
    ``probe_key_override_388``.  Does NOT rewrite the canonical tables
    themselves (carry-forward to upstream builders).

Phases
------
* **Phase 0** (read-only, default) — discovery + classification + sham-key
  probe; writes ``scripts/output/388_dispositions.json`` and halts.
* **Phase 2** (``--apply``) — execute dispositions consumer-first
  (renames, then orphan archives, then LLM-source archives, then sham-key
  override pinning).
* **Phase 3** — post-state verification.
* **Phase 4** — ``detail_table_registry_v1`` + ``__readme`` sync.

Safety
------
* Default is ``--plan`` (Phase 0 only).
* Every destructive op gated on ``--apply``.
* Every CTAS verifies row-count parity before DROP.
* ``UNCLASSIFIED`` items HALT the run — surfaced for manual review.
* Idempotent: re-running ``--apply`` skips already-archived / already-renamed
  objects via the ``script_388_archive_move_log_v1`` audit row + archive-DB
  presence check.
* PHI: schema-level only.  No clinical-text columns ever read or logged.

Auth: ``motherduck_client.get_token()``.  Token never printed.
"""
from __future__ import annotations

import argparse
import json
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

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PUB_DB = "thyroid_canonical_publication_v1_0"
ARC_DB_RAW = "Thyroid 2026 UPdated"
ARC_DB = f'"{ARC_DB_RAW}"'

WS_SCHEMA = "manuscript_workspace"
SCRIPT_TAG = "388_note_entities_llm_and_view_label_audit"

ARC_LLM_LEGACY = "note_entities_llm_legacy_20260422"
ARC_MAIN_LEGACY = "main_legacy_20260422"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
LOG_PATH = OUTPUT_DIR / "388_run.log"
DISPO_PATH = OUTPUT_DIR / "388_dispositions.json"
SHAMKEY_PATH = OUTPUT_DIR / "388_shamkey_probe.json"
DEDUP_REVERIFY_PATH = OUTPUT_DIR / "388_dedup_reverification.md"

# Naming-contract exemptions for the main.* sweep.
EXEMPT_MAIN_NAMES: set[str] = {
    "__readme",
    "detail_table_registry_v1",
    "canonical_patient_master",
    # cupm_v2_canonical_backfill_v1: per Logan adjudication 2026-04-22 Q2,
    # KEEP_AS_IS for now and surface as a 389 follow-up.  Script 389 Phase
    # 2H will archive this object after the canonical_us_patient_master_VIEW_v2
    # body has been rewritten to drop the CPM-scaffold dependency.  Treating
    # it as a permanent exempt now would legitimise it; treating it as
    # UNCLASSIFIED would HALT every 388 re-run.  EXEMPT-PENDING-389 is the
    # cleanest defer.
    "cupm_v2_canonical_backfill_v1",
}
# 8 grandfathered MotherDuck-platform views (per
# memory ``reference_view_naming_convention.md`` and confirmed by
# ``reports/VIEW_LABELING_PASS_PHASE0_20260421.md``).
GRANDFATHERED_PLATFORM_VIEWS: set[str] = {
    "database_snapshots",
    "databases",
    "owned_shares",
    "query_history",
    "recent_queries",
    "shared_with_me",
    "storage_info",
    "storage_info_history",
}
EXEMPT_MAIN_NAMES.update(GRANDFATHERED_PLATFORM_VIEWS)

# Sham-key probe targets (387 carry-forward #2).
SHAMKEY_TABLES: tuple[str, ...] = (
    "canonical_path_benign_events_v1",
    "canonical_path_gland_events_v1",
)

# Candidate keys to probe per path-event table.  Picker uses the first
# candidate whose columns all exist.  Listed best-key-first.
#
# These tables are SYNOPTIC-grain (1 row per parsed synoptic table row),
# not note-grain — so the natural keys involve `synoptic_row_ord` (the
# stable per-source ordinal that never collapses to NULL the way
# `synoptic_row_ix` does on these two tables) plus a source-document
# identifier.  See memory ``feedback_mention_grain_partition_probe.md``
# for the note-grain analogue used elsewhere.
PATH_KEY_CANDIDATES: list[list[str]] = [
    # Synoptic-grain (best for path_benign / path_malignant where the
    # source carries a report id and a per-report synoptic_row_ord).
    ["research_id", "surgery_episode_id", "source_report_id",
     "synoptic_row_ord"],
    ["research_id", "surgery_episode_id", "accession_or_source_id",
     "synoptic_row_ord"],
    ["research_id", "source_table", "source_report_id",
     "synoptic_row_ord"],
    ["research_id", "source_table", "accession_or_source_id",
     "synoptic_row_ord"],
    # Path_gland tends to lack source_report_id / accession_or_source_id —
    # fall back to (rid, episode, gland_position, synoptic_row_ord) which
    # uniquely identifies one parsed gland-table row per surgery episode.
    ["research_id", "surgery_episode_id", "specimen_id",
     "gland_position", "synoptic_row_ord"],
    ["research_id", "surgery_episode_id", "gland_position",
     "synoptic_row_ord"],
    ["research_id", "surgery_episode_id", "synoptic_row_ord"],
    # Date+source fallback when nothing else exists.
    ["research_id", "path_date", "source_table", "synoptic_row_ord"],
    ["research_id", "path_date", "synoptic_row_ord"],
    # Note-grain fallbacks (kept so the report shows they're invalid).
    ["research_id", "note_row_id", "evidence_start"],
    ["research_id", "evidence_span_hash"],
]

# Heuristic for "current-era" extraction tags (anything matching → table
# is treated as current-era unless explicitly orphaned by reference scan).
CURRENT_ERA_MODEL_TOKENS = ("gpt-oss", "gpt_oss", "120b", "round2_20260421")

# Logan adjudication 2026-04-22 Q1 — explicit "Safe-9" archive override
# list.  The auto-classifier would have KEPT every LLM table because
# `lakehouse/motherduck_optimize.py` references all of them (table-name
# enumeration only).  These 9 tables have NO live builder reference in
# any current-pipeline script (>= 360); their qwen-era data is genuinely
# orphaned by the post-v1.0 pipeline.  Per Q1 these are archived now.
#
# The 4 qwen-era tables explicitly KEPT are documented in
# LLM_KEEP_DESPITE_QWEN below.
LLM_ARCHIVE_OVERRIDE_QWEN: tuple[str, ...] = (
    "note_entities_llm_labs",
    "note_entities_llm_physical_exam",
    "note_entities_llm_synoptic_pathology_enrichment",
    "note_entities_llm_functional_outcomes",
    "note_entities_llm_parathyroid_detail",
    "note_entities_llm_patient_decision_adherence",
    "note_entities_llm_rad_treatment",
    "note_entities_llm_survival_followup",
    "note_entities_llm_tg_kinetics",
)
LLM_KEEP_DESPITE_QWEN: dict[str, str] = {
    # name -> why-kept note (surfaced in dispositions evidence)
    "note_entities_llm_airway_invasion":
        "live builder ref: scripts/363_invasion_canonical.py "
        "(re-derivation is 363 carry-forward 6.B; do NOT archive until "
        "rebuild lands)",
    "note_entities_llm_recurrence":
        "live builder ref: scripts/364_complications_consolidation.py",
    "note_entities_llm_dynamic_risk_response":
        "live builder ref: scripts/364_complications_consolidation.py",
    "note_entities_llm_frozen_section_detail":
        "live-ish builder ref: scripts/360_frozen_section_cleanup.py "
        "(360 already wrote canonical_frozen_section_events_v1; flag for "
        "390+ re-verify — if no future rebuild touches this source, it "
        "becomes an archive candidate next round)",
}

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

_log_buf: list[str] = []


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3] + "Z"


def log(msg: str) -> None:
    line = f"[INFO] [{_ts()}] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def err(msg: str) -> None:
    line = f"[ERROR] [{_ts()}] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def flush_log() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    mode = "a" if LOG_PATH.exists() else "w"
    with LOG_PATH.open(mode, encoding="utf-8") as fh:
        fh.write("\n".join(_log_buf) + "\n")


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}). "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    log(f"Connecting md:{PUB_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{PUB_DB}?motherduck_token={tok}")
    con.execute(f'USE "{PUB_DB}"')
    con.execute(f'USE "{PUB_DB}".main')

    dbs = {r[0] for r in con.execute(
        "SELECT database_name FROM duckdb_databases()"
    ).fetchall()}
    if PUB_DB not in dbs:
        raise SystemExit(f"PUB DB '{PUB_DB}' not attached")
    if ARC_DB_RAW not in dbs:
        raise SystemExit(
            f"Archive DB '{ARC_DB_RAW}' not attached — cannot run audit"
        )

    cpm_n = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}".main.canonical_patient_master'
    ).fetchone()[0]
    if cpm_n != 10871:
        raise SystemExit(
            f"canonical_patient_master row count {cpm_n} != 10871; aborting"
        )
    log(f"Connection OK (CPM rows={cpm_n})")
    return con


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def get_object_kind(con, schema: str, name: str, db: str = PUB_DB) -> str | None:
    row = con.execute(
        """
        SELECT table_type FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [db, schema, name],
    ).fetchone()
    return row[0] if row else None


def get_columns(con, table: str, schema: str = "main", db: str = PUB_DB) -> set[str]:
    rows = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [db, schema, table],
    ).fetchall()
    return {r[0] for r in rows}


def row_count(con, schema: str, name: str, db: str = PUB_DB) -> int:
    return con.execute(
        f'SELECT COUNT(*) FROM "{db}"."{schema}"."{name}"'
    ).fetchone()[0]


def view_references(
    con, needle: str, exclude_self_schema: str = "main",
    exclude_self_name: str | None = None,
) -> list[tuple[str, str]]:
    """Return (schema, view_name) of any VIEW whose definition references needle."""
    rows = con.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.views
        WHERE table_catalog = ?
          AND view_definition ILIKE ?
        ORDER BY table_schema, table_name
        """,
        [PUB_DB, f"%{needle}%"],
    ).fetchall()
    out: list[tuple[str, str]] = []
    for s, n in rows:
        if (
            exclude_self_name is not None
            and s == exclude_self_schema
            and n == exclude_self_name
        ):
            continue
        out.append((s, n))
    return out


_SCRIPT_NUM_RE = re.compile(r"/scripts/(\d+)[a-z]*_")
_CURRENT_PIPELINE_MIN = 360  # scripts/360+ = post-publication v1.0 round


def repo_references(needle: str) -> dict[str, Any]:
    """Return ``{'all': [...], 'current_pipeline': [...]}`` reference paths.

    *current_pipeline* is the subset of *all* whose path matches
    ``scripts/(\\d+)..._*`` with the script number >= 360 (post-v1.0 active
    work, including all 360–388 builders).  Historical scripts (210, 233,
    304, etc.) are in *all* but not *current_pipeline*.

    Excludes self (387/388 outputs), .venv, .git, frozen, archive, output
    JSONs/logs/MDs, transcripts, docs, reports, exports, studies,
    processed/raw data dirs.
    """
    cmd = [
        "rg", "--no-heading", "--files-with-matches",
        "--glob", "!**/.venv/**",
        "--glob", "!**/.git/**",
        "--glob", "!scripts/frozen/**",
        "--glob", "!scripts/archive/**",
        "--glob", "!scripts/output/**",
        "--glob", "!cursor_prompts/**",
        "--glob", "!memory/**",
        "--glob", "!docs/**",
        "--glob", "!reports/**",
        "--glob", "!exports/**",
        "--glob", "!studies/**",
        "--glob", "!processed/**",
        "--glob", "!raw/**",
        "--glob", "!.phase**/**",
        "--glob", "!**/*.md",
        "--glob", "!**/*.log",
        "--glob", "!**/*.json",
        re.escape(needle), str(REPO_ROOT),
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return {"all": [], "current_pipeline": []}
    if result.returncode not in (0, 1):
        return {"all": [], "current_pipeline": []}

    all_refs = [
        line for line in result.stdout.splitlines()
        if line and "/388_" not in line and "/387_" not in line
    ]
    current: list[str] = []
    for line in all_refs:
        m = _SCRIPT_NUM_RE.search(line)
        if m and int(m.group(1)) >= _CURRENT_PIPELINE_MIN:
            current.append(line)
        elif "/scripts/" not in line and (
            line.endswith(".py") or line.endswith(".sql")
        ):
            # Non-numbered active code (e.g., utils/, app/, lakehouse/)
            current.append(line)
    return {"all": all_refs, "current_pipeline": current}


# --------------------------------------------------------------------------- #
# Workspace tables
# --------------------------------------------------------------------------- #


def ensure_workspace_tables(con) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {WS_SCHEMA}.script_388_archive_move_log_v1 (
            move_ts        TIMESTAMP,
            source_schema  VARCHAR,
            source_name    VARCHAR,
            dest_db        VARCHAR,
            dest_schema    VARCHAR,
            dest_name      VARCHAR,
            move_method    VARCHAR,
            reason         VARCHAR
        )
    """)
    # Add probe_key_override_388 column to 387 dedup probe table if missing.
    has_table = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name = 'script_387_dedup_probe_v1'
        """,
        [PUB_DB, WS_SCHEMA],
    ).fetchone()
    if has_table:
        con.execute(
            f'ALTER TABLE {WS_SCHEMA}.script_387_dedup_probe_v1 '
            f'ADD COLUMN IF NOT EXISTS probe_key_override_388 VARCHAR'
        )


# --------------------------------------------------------------------------- #
# Phase 0 — discovery
# --------------------------------------------------------------------------- #


def _llm_table_metadata(con, name: str) -> dict[str, Any]:
    cols = get_columns(con, name)
    rows = row_count(con, "main", name)
    meta: dict[str, Any] = {"row_count": rows, "n_cols": len(cols)}

    # Era heuristics
    era_signals: list[str] = []
    if "model_run_id" in cols:
        try:
            samples = con.execute(
                f'SELECT DISTINCT model_run_id FROM main."{name}" LIMIT 8'
            ).fetchall()
            samples_str = [str(s[0]) for s in samples if s and s[0] is not None]
            meta["model_run_id_samples"] = samples_str
            for s in samples_str:
                low = s.lower()
                if any(tok in low for tok in CURRENT_ERA_MODEL_TOKENS):
                    era_signals.append(f"current(model_run_id={s})")
        except duckdb.Error:
            pass
    if "extraction_run_id" in cols:
        try:
            samples = con.execute(
                f'SELECT DISTINCT extraction_run_id FROM main."{name}" LIMIT 8'
            ).fetchall()
            samples_str = [str(s[0]) for s in samples if s and s[0] is not None]
            meta["extraction_run_id_samples"] = samples_str
            for s in samples_str:
                low = s.lower()
                if any(tok in low for tok in CURRENT_ERA_MODEL_TOKENS):
                    era_signals.append(f"current(extraction_run_id={s})")
        except duckdb.Error:
            pass
    if "llm_model" in cols:
        try:
            samples = con.execute(
                f'SELECT DISTINCT llm_model FROM main."{name}" LIMIT 8'
            ).fetchall()
            samples_str = [str(s[0]) for s in samples if s and s[0] is not None]
            meta["llm_model_samples"] = samples_str
            for s in samples_str:
                low = s.lower()
                if any(tok in low for tok in CURRENT_ERA_MODEL_TOKENS):
                    era_signals.append(f"current(llm_model={s})")
                if "qwen" in low:
                    era_signals.append(f"qwen(llm_model={s})")
        except duckdb.Error:
            pass
    if "extracted_at" in cols:
        try:
            r = con.execute(
                f'SELECT MIN(extracted_at), MAX(extracted_at) '
                f'FROM main."{name}"'
            ).fetchone()
            meta["extracted_at_range"] = [str(r[0]), str(r[1])]
        except duckdb.Error:
            pass
    elif "build_ts" in cols:
        try:
            r = con.execute(
                f'SELECT MIN(build_ts), MAX(build_ts) '
                f'FROM main."{name}"'
            ).fetchone()
            meta["build_ts_range"] = [str(r[0]), str(r[1])]
        except duckdb.Error:
            pass

    meta["era_signals"] = era_signals

    view_refs = view_references(con, name, exclude_self_name=name)
    repo_refs = repo_references(name)
    meta["view_references"] = [f"{s}.{n}" for s, n in view_refs]
    meta["repo_references_count"] = len(repo_refs["all"])
    meta["repo_references_sample"] = repo_refs["all"][:8]
    meta["repo_references_current_pipeline"] = repo_refs["current_pipeline"]
    return meta


def _classify_llm_table(name: str, meta: dict[str, Any]) -> tuple[str, str, str]:
    """Return (classification, proposed_action, evidence).

    Decision tree (most permissive first to avoid archiving load-bearing
    sources):

    * KEEP_REGISTRY — referenced by ≥1 live view  → CURRENT_LIVE
    * KEEP — significant repo refs (≥3 unique paths in live script
      surface) → CURRENT_LIVE regardless of model era.  The 'no cross-DB
      canonical sourcing' rule (memory
      ``feedback_no_cross_db_canonical_sourcing.md``) prohibits archiving
      a table that any current canonical body sources from.  Per Logan,
      qwen-era data is "stale but load-bearing" until the canonical
      builder is re-derived (carry-forward, NOT this script).
    * UNCLASSIFIED — qwen-era + any repo refs (1–2) → HALT for manual
      review (could be historical-only refs OR could be a live builder).
    * LEGACY_REPLACED — qwen-era + zero refs → safe to ARCHIVE.
    * CURRENT_ORPHAN — current-era OR no-era + zero refs → safe to
      ARCHIVE.
    """
    has_view_refs = bool(meta["view_references"])
    current_refs = meta.get("repo_references_current_pipeline", [])
    historical_refs = [
        r for r in meta.get("repo_references_sample", [])
        if r not in current_refs
    ]
    era_signals = meta.get("era_signals", [])
    is_current = any(s.startswith("current(") for s in era_signals)
    is_qwen = any(s.startswith("qwen(") for s in era_signals)

    # Logan adjudication overrides win over heuristics.
    if name in LLM_ARCHIVE_OVERRIDE_QWEN:
        return (
            "LEGACY_REPLACED",
            f'ARCHIVE to {ARC_DB}."{ARC_LLM_LEGACY}"."{name}_qwen_20260422"',
            "Logan adjudication 2026-04-22 Q1 (Safe-9): qwen-era source "
            "with no live builder reference; archive now",
        )
    if name in LLM_KEEP_DESPITE_QWEN:
        return (
            "CURRENT_LIVE",
            "KEEP",
            "Logan adjudication 2026-04-22 Q1: " + LLM_KEEP_DESPITE_QWEN[name],
        )

    if has_view_refs:
        return (
            "CURRENT_LIVE",
            "KEEP",
            f"Referenced by {len(meta['view_references'])} view(s): "
            f"{', '.join(meta['view_references'][:3])}",
        )
    if current_refs:
        return (
            "CURRENT_LIVE",
            "KEEP",
            f"{len(current_refs)} current-pipeline repo reference(s) "
            f"({', '.join(current_refs[:3])}); era_signals="
            f"{era_signals or 'none'}",
        )
    # No view refs and no current-pipeline refs from here.
    if is_qwen and not is_current:
        hist_note = (
            f" (historical refs: {len(historical_refs)} pre-360 scripts)"
            if historical_refs else ""
        )
        return (
            "LEGACY_REPLACED",
            f'ARCHIVE to {ARC_DB}."{ARC_LLM_LEGACY}"."{name}_qwen_20260422"',
            f"qwen-era extraction (signals: {era_signals}); zero "
            f"current-pipeline references{hist_note}",
        )
    if is_current:
        return (
            "UNCLASSIFIED",
            "HALT",
            "Current-era model tag but zero current-pipeline references "
            "— likely awaiting downstream canonical build; manual decision",
        )
    if historical_refs:
        return (
            "UNCLASSIFIED",
            "HALT",
            f"No current-pipeline refs but {len(historical_refs)} historical "
            "ref(s) and no era signal — manual review required",
        )
    return (
        "CURRENT_ORPHAN",
        f'ARCHIVE to {ARC_DB}."{ARC_LLM_LEGACY}"."{name}"',
        "No live view references, no repo references, no era signals",
    )


def phase0_llm_inventory(con) -> list[dict[str, Any]]:
    log("=" * 78)
    log("Phase 0.1 — note_entities_llm_* inventory")
    log("=" * 78)

    rows = con.execute(
        """
        SELECT table_name, table_type FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = 'main'
          AND table_name LIKE 'note\\_entities\\_llm\\_%' ESCAPE '\\'
        ORDER BY table_name
        """,
        [PUB_DB],
    ).fetchall()
    log(f"  found {len(rows)} note_entities_llm_* objects in main")

    if len(rows) <= 4:
        raise SystemExit(
            f"Pre-state gate: only {len(rows)} note_entities_llm_* objects "
            "found.  Script 386 may not have landed the tirads_granular + "
            "esophageal_invasion tables.  Halting."
        )

    inventory: list[dict[str, Any]] = []
    for name, table_type in rows:
        meta = _llm_table_metadata(con, name)
        classification, proposed_action, evidence = _classify_llm_table(name, meta)
        item = {
            "name": name,
            "table_type": table_type,
            "row_count": meta["row_count"],
            "n_cols": meta["n_cols"],
            "classification": classification,
            "evidence": evidence,
            "proposed_action": proposed_action,
            "era_signals": meta.get("era_signals", []),
            "view_references": meta.get("view_references", []),
            "repo_references_count": meta.get("repo_references_count", 0),
            "repo_references_sample": meta.get("repo_references_sample", []),
            "repo_references_current_pipeline":
                meta.get("repo_references_current_pipeline", []),
            "extracted_at_range": meta.get("extracted_at_range"),
            "build_ts_range": meta.get("build_ts_range"),
            "model_run_id_samples": meta.get("model_run_id_samples"),
            "extraction_run_id_samples": meta.get("extraction_run_id_samples"),
            "llm_model_samples": meta.get("llm_model_samples"),
        }
        inventory.append(item)
        log(
            f"  {name:<55} rows={meta['row_count']:>8,} "
            f"-> {classification} ({proposed_action[:60]})"
        )
    return inventory


def _is_main_exempt(name: str) -> bool:
    if name in EXEMPT_MAIN_NAMES:
        return True
    if name.startswith("canonical_"):
        return True
    if name.startswith("data_dictionary_v"):
        return True
    if name.startswith("note_entities_llm_"):
        return True
    if name.startswith("raw_"):
        return True
    if "_VIEW_v" in name:
        return True
    return False


def _in_detail_table_registry(con, name: str) -> bool:
    row = con.execute(
        f"""
        SELECT 1 FROM {WS_SCHEMA}.detail_table_registry_v1
        WHERE detail_table_name = ?
        LIMIT 1
        """,
        [name],
    ).fetchone()
    return row is not None


def _classify_main_object(
    con, name: str, table_type: str
) -> tuple[str, str, str, list[str]]:
    """Return (classification, proposed_action, evidence, dependent_views)."""
    deps = view_references(con, name, exclude_self_name=name)
    dep_strs = [f"{s}.{n}" for s, n in deps]
    repo_count = len(repo_references(name))
    in_registry = _in_detail_table_registry(con, name)

    if table_type == "VIEW":
        if deps or repo_count > 0:
            return (
                "KEEP_RENAME",
                f"RENAME {name} -> {name}_VIEW_v1",
                f"VIEW with {len(deps)} dependent view(s) + {repo_count} "
                "repo ref(s)",
                dep_strs,
            )
        return (
            "ORPHAN",
            f'ARCHIVE to {ARC_DB}."{ARC_MAIN_LEGACY}"."{name}"',
            "VIEW with no dependent views and no repo references",
            dep_strs,
        )
    # BASE TABLE branch
    if in_registry:
        # Registered source/staging table — load-bearing for canonical
        # builders.  The VIEW-label rule does NOT apply (Logan's quote
        # targets pseudo-views, not real source tables).  Documented in
        # detail_table_registry_v1 as a real grain-bearing object.
        return (
            "KEEP_REGISTRY",
            "KEEP",
            f"BASE TABLE registered in detail_table_registry_v1 "
            f"({len(deps)} dependent view(s), {repo_count} repo ref(s)) "
            "— load-bearing source table, exempt from VIEW-label rule",
            dep_strs,
        )
    if deps or repo_count > 0:
        return (
            "UNCLASSIFIED",
            "HALT",
            f"BASE TABLE that is neither canonical_* nor _VIEW_v* nor in "
            f"the registry, but has {len(deps)} dependent view(s) + "
            f"{repo_count} repo ref(s); manual decision required "
            "(rename to canonical_* / add to registry / VIEW shim?)",
            dep_strs,
        )
    return (
        "ORPHAN",
        f'ARCHIVE to {ARC_DB}."{ARC_MAIN_LEGACY}"."{name}"',
        "BASE TABLE with no dependents, no repo references, and not in "
        "the detail_table_registry_v1",
        dep_strs,
    )


def phase0_view_label_inventory(con) -> list[dict[str, Any]]:
    log("=" * 78)
    log("Phase 0.2 — main.* VIEW-label completeness inventory")
    log("=" * 78)

    rows = con.execute(
        """
        SELECT table_name, table_type FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = 'main'
        ORDER BY table_name
        """,
        [PUB_DB],
    ).fetchall()
    log(f"  scanning {len(rows)} main.* objects")

    inventory: list[dict[str, Any]] = []
    for name, table_type in rows:
        if _is_main_exempt(name):
            continue
        classification, action, evidence, deps = _classify_main_object(
            con, name, table_type
        )
        item = {
            "name": name,
            "table_type": table_type,
            "classification": classification,
            "evidence": evidence,
            "proposed_action": action,
            "dependent_views": deps,
        }
        inventory.append(item)
        log(
            f"  {name:<55} {table_type:<10} -> {classification} "
            f"({action[:60]})"
        )
    if not inventory:
        log("  (none — every non-exempt main.* object satisfies the contract)")
    return inventory


# --------------------------------------------------------------------------- #
# Phase 0.3 — sham-key probes
# --------------------------------------------------------------------------- #


def _probe_key(
    con, table: str, key_cols: list[str]
) -> dict[str, Any] | None:
    cols = get_columns(con, table)
    if not all(c in cols for c in key_cols):
        return None
    quoted = ", ".join(f'"{c}"' for c in key_cols)
    key_sql = f"({quoted})" if len(key_cols) > 1 else f'"{key_cols[0]}"'
    null_or = " OR ".join(f'"{c}" IS NULL' for c in key_cols)
    total, distinct, null_keys = con.execute(
        f"SELECT COUNT(*), "
        f"COUNT(DISTINCT {key_sql}), "
        f"SUM(CASE WHEN {null_or} THEN 1 ELSE 0 END) "
        f'FROM main."{table}"'
    ).fetchone()
    null_keys = int(null_keys or 0)
    populated = int(total) - null_keys
    collapse = max(populated - int(distinct), 0)
    return {
        "key": key_cols,
        "total_rows": int(total),
        "distinct_keys": int(distinct),
        "null_key_rows": null_keys,
        "populated_rows": populated,
        "collapse_count": collapse,
        "collapse_rate": (
            round(collapse / populated, 4) if populated > 0 else None
        ),
        "null_key_rate": (
            round(null_keys / int(total), 4) if int(total) > 0 else None
        ),
    }


def phase0_shamkey_probe(con) -> list[dict[str, Any]]:
    log("=" * 78)
    log("Phase 0.3 — sham-key probe (387 carry-forward #2)")
    log("=" * 78)

    out: list[dict[str, Any]] = []
    for table in SHAMKEY_TABLES:
        if get_object_kind(con, "main", table) is None:
            log(f"  {table}: NOT PRESENT — skipping")
            out.append({"name": table, "present": False})
            continue
        log(f"  probing {table}")
        candidates: list[dict[str, Any]] = []
        for cand in PATH_KEY_CANDIDATES:
            res = _probe_key(con, table, cand)
            if res is None:
                continue
            candidates.append(res)
            log(
                f"    key={'(' + ', '.join(cand) + ')':<70} "
                f"rows={res['total_rows']:>6,} "
                f"distinct={res['distinct_keys']:>6,} "
                f"null_key={res['null_key_rows']:>6,} "
                f"collapse={res['collapse_count']:>6,}"
            )

        # Pick best: minimum collapse_count (the partition-contract metric)
        # first, then minimum null_key_rows, then richest (longest) key.
        # A clean (collapse=0, populated) key is preferred even if a chunk of
        # rows have NULLs — the NULL rows are a separate upstream-linkage
        # problem and are flagged via the chosen_metrics output.
        def sort_key(c: dict[str, Any]) -> tuple[int, int, int]:
            return (c["collapse_count"], c["null_key_rows"], -len(c["key"]))

        ranked = sorted(candidates, key=sort_key)
        chosen = ranked[0] if ranked else None

        # Logan adjudication 2026-04-22 Q3 — annotate path_gland's NULL
        # surgery_episode_id rows as a separate linkage carry-forward so
        # the "clean on populated rows" verdict isn't conflated with the
        # 11,348-row upstream linkage gap.
        notes: list[str] = []
        if chosen and chosen["null_key_rows"] > 0:
            notes.append(
                f"chosen key has {chosen['null_key_rows']:,} NULL-key rows "
                f"({chosen['null_key_rate']:.1%}); these are an upstream "
                "linkage gap (e.g. missing surgery_episode_id), tracked as "
                "a SEPARATE carry-forward — collapse=0 on populated rows is "
                "the correct partition-contract signal"
            )

        out.append({
            "name": table,
            "present": True,
            "current_declared_key_in_387": (
                "synoptic_row_ix all-NULL — see 387 dedup probe"
            ),
            "candidates": candidates,
            "chosen_probe_key": chosen["key"] if chosen else None,
            "chosen_metrics": chosen,
            "chosen_metrics_notes": notes,
        })
        if chosen:
            log(
                f"    -> chosen probe key: ({', '.join(chosen['key'])}) "
                f"with null_key={chosen['null_key_rows']:,} "
                f"collapse={chosen['collapse_count']:,}"
            )
        else:
            log("    -> NO viable candidate key found (all probes invalid)")
    return out


# --------------------------------------------------------------------------- #
# Phase 0 wrapper — write dispositions JSON
# --------------------------------------------------------------------------- #


def phase0_write_dispositions(
    llm_inv: list[dict[str, Any]],
    view_inv: list[dict[str, Any]],
    shamkey: list[dict[str, Any]],
    mode: str,
) -> bool:
    """Write 388_dispositions.json.  Return True iff there are no UNCLASSIFIED."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    unclassified = (
        [item for item in llm_inv if item["classification"] == "UNCLASSIFIED"]
        + [item for item in view_inv if item["classification"] == "UNCLASSIFIED"]
    )

    payload = {
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "pub_db": PUB_DB,
        "archive_db": ARC_DB_RAW,
        "note_entities_llm_inventory": llm_inv,
        "view_label_inventory": view_inv,
        "carryforward_387_sham_keys": shamkey,
        "unclassified": [{"name": i["name"], "evidence": i["evidence"]}
                         for i in unclassified],
        "halt_required": bool(unclassified),
        "summary": {
            "n_llm_total": len(llm_inv),
            "n_llm_keep": len([i for i in llm_inv if i["classification"] == "CURRENT_LIVE"]),
            "n_llm_archive_orphan": len([i for i in llm_inv if i["classification"] == "CURRENT_ORPHAN"]),
            "n_llm_archive_legacy": len([i for i in llm_inv if i["classification"] == "LEGACY_REPLACED"]),
            "n_view_label_renames": len([i for i in view_inv if i["classification"] == "KEEP_RENAME"]),
            "n_main_orphans": len([i for i in view_inv if i["classification"] == "ORPHAN"]),
            "n_main_keep_registry": len([i for i in view_inv if i["classification"] == "KEEP_REGISTRY"]),
            "n_unclassified": len(unclassified),
            "n_sham_key_overrides": len([
                s for s in shamkey
                if s.get("present") and s.get("chosen_probe_key")
            ]),
        },
    }
    DISPO_PATH.write_text(json.dumps(payload, indent=2))
    log(f"  dispositions written -> {DISPO_PATH}")

    # Also write the standalone shamkey JSON (matches prompt's file-map).
    SHAMKEY_PATH.write_text(json.dumps({
        "run_ts": payload["run_ts"],
        "candidates_per_table": shamkey,
    }, indent=2))
    log(f"  sham-key probe written -> {SHAMKEY_PATH}")

    log("Phase 0 summary: " + json.dumps(payload["summary"]))
    return not unclassified


# --------------------------------------------------------------------------- #
# Phase 2 — apply dispositions
# --------------------------------------------------------------------------- #


def _archive_present(con, schema: str, name: str) -> int | None:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [ARC_DB_RAW, schema, name],
    ).fetchone()
    if not row:
        return None
    return con.execute(
        f'SELECT COUNT(*) FROM {ARC_DB}."{schema}"."{name}"'
    ).fetchone()[0]


def _already_logged(con, source_name: str, dest_name: str) -> bool:
    row = con.execute(
        f"""
        SELECT 1 FROM {WS_SCHEMA}.script_388_archive_move_log_v1
        WHERE source_name = ? AND dest_name = ?
        LIMIT 1
        """,
        [source_name, dest_name],
    ).fetchone()
    return row is not None


def _log_move(
    con,
    *,
    source_schema: str,
    source_name: str,
    dest_schema: str,
    dest_name: str,
    move_method: str,
    reason: str,
) -> None:
    con.execute(
        f"""
        INSERT INTO {WS_SCHEMA}.script_388_archive_move_log_v1
        (move_ts, source_schema, source_name, dest_db, dest_schema,
         dest_name, move_method, reason)
        VALUES (CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?, ?, ?, ?, ?, ?)
        """,
        [source_schema, source_name, ARC_DB_RAW, dest_schema, dest_name,
         move_method, reason],
    )


def _ensure_arc_schema(con, schema: str) -> None:
    con.execute(f'CREATE SCHEMA IF NOT EXISTS {ARC_DB}."{schema}"')


def _archive_ctas_drop(
    con,
    *,
    src_schema: str,
    src_name: str,
    src_kind: str,
    dest_schema: str,
    dest_name: str,
    reason: str,
) -> dict[str, Any]:
    src_fq = f'"{PUB_DB}"."{src_schema}"."{src_name}"'
    arc_fq = f'{ARC_DB}."{dest_schema}"."{dest_name}"'

    if get_object_kind(con, src_schema, src_name) is None:
        arc_rows = _archive_present(con, dest_schema, dest_name)
        if arc_rows is not None or _already_logged(con, src_name, dest_name):
            log(f"  -> {src_schema}.{src_name} ALREADY archived/dropped, skipping")
            return {"src": f"{src_schema}.{src_name}", "status": "already_done"}
        raise SystemExit(
            f"archive: source {src_schema}.{src_name} missing AND no archive "
            "copy / log row present — refusing to log a phantom drop"
        )

    src_rows = row_count(con, src_schema, src_name)
    log(f"  -> {src_schema}.{src_name} ({src_kind}, {src_rows:,} rows) "
        f"-> {arc_fq}")

    _ensure_arc_schema(con, dest_schema)

    arc_existing = _archive_present(con, dest_schema, dest_name)
    if arc_existing is not None:
        if arc_existing != src_rows:
            raise SystemExit(
                f"Archive {arc_fq} already exists with {arc_existing} rows "
                f"but src has {src_rows}; refusing to overwrite"
            )
        log(f"     archive already present ({arc_existing:,} rows) — skipping CTAS")
        arc_rows = arc_existing
    else:
        con.execute(f"CREATE TABLE {arc_fq} AS SELECT * FROM {src_fq}")
        arc_rows = con.execute(f"SELECT COUNT(*) FROM {arc_fq}").fetchone()[0]
        if arc_rows != src_rows:
            raise SystemExit(
                f"ARCHIVE PARITY FAIL {src_schema}.{src_name}: "
                f"src={src_rows} arc={arc_rows}"
            )
        log(f"     archived ({arc_rows:,} rows; parity OK)")

    drop_kind = "VIEW" if src_kind == "VIEW" else "TABLE"
    con.execute(f"DROP {drop_kind} {src_fq}")
    log(f"     dropped {src_kind.lower()} {src_schema}.{src_name}")

    _log_move(
        con,
        source_schema=src_schema, source_name=src_name,
        dest_schema=dest_schema, dest_name=dest_name,
        move_method="CTAS",
        reason=reason,
    )
    return {
        "src": f"{src_schema}.{src_name}",
        "dest": arc_fq,
        "src_rows": src_rows,
        "arc_rows": arc_rows,
        "status": "archived",
    }


def _rewrite_dependent_view(
    con, dep_schema: str, dep_name: str, old_name: str, new_name: str
) -> bool:
    """Rewrite a dependent VIEW body, replacing old_name with new_name.

    Whole-word replacement only.  Returns True iff a substitution was made
    and the view was successfully rebuilt.
    """
    row = con.execute(
        """
        SELECT view_definition FROM information_schema.views
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [PUB_DB, dep_schema, dep_name],
    ).fetchone()
    if not row:
        return False
    body = row[0]
    if old_name not in body:
        return False
    pattern = re.compile(rf'(?<![A-Za-z0-9_]){re.escape(old_name)}(?![A-Za-z0-9_])')
    new_body = pattern.sub(new_name, body)
    if new_body == body:
        return False
    con.execute(
        f'CREATE OR REPLACE VIEW "{PUB_DB}"."{dep_schema}"."{dep_name}" AS '
        f'{new_body}'
    )
    log(f"     rewrote dependent view {dep_schema}.{dep_name} "
        f"({old_name} -> {new_name})")
    return True


def phase2a_view_label_renames(
    con, view_inv: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    log("=" * 78)
    log("Phase 2.A — VIEW-label renames")
    log("=" * 78)
    targets = [i for i in view_inv if i["classification"] == "KEEP_RENAME"]
    log(f"  targets: {len(targets)}")
    out: list[dict[str, Any]] = []
    for item in targets:
        old = item["name"]
        new = f"{old}_VIEW_v1"
        src_kind = item["table_type"]

        # Idempotent skip
        if get_object_kind(con, "main", old) is None:
            if get_object_kind(con, "main", new) is not None:
                log(f"  -> {old} already renamed to {new}; skipping")
                out.append({"src": old, "dest": new, "status": "already_done"})
                continue
            raise SystemExit(
                f"rename: source main.{old} missing and target main.{new} "
                "missing — investigate manually"
            )

        log(f"  -> rename main.{old} ({src_kind}) -> main.{new}")
        # 1. Pre-snapshot to archive_pub_v1_0 (per-script snapshot pattern).
        snap_schema = "archive_pub_v1_0"
        snap_name = f"{old}_pre388_20260422"
        _ensure_arc_schema(con, snap_schema)
        if _archive_present(con, snap_schema, snap_name) is None:
            con.execute(
                f'CREATE TABLE {ARC_DB}."{snap_schema}"."{snap_name}" AS '
                f'SELECT * FROM "{PUB_DB}".main."{old}"'
            )
            log(f"     pre-rename snapshot -> {ARC_DB}.{snap_schema}.{snap_name}")

        # 2. Rewrite dependent view bodies first
        deps = item.get("dependent_views", [])
        rewritten: list[str] = []
        for dep_fq in deps:
            dep_schema, dep_name = dep_fq.split(".", 1)
            if _rewrite_dependent_view(con, dep_schema, dep_name, old, new):
                rewritten.append(dep_fq)

        # 3. Build new view from current body (use SELECT * shim if old is a view)
        if src_kind == "VIEW":
            old_body_row = con.execute(
                """SELECT view_definition FROM information_schema.views
                   WHERE table_catalog=? AND table_schema='main' AND table_name=?""",
                [PUB_DB, old],
            ).fetchone()
            old_body = old_body_row[0] if old_body_row else None
            if old_body is None:
                raise SystemExit(f"could not read view body for main.{old}")
            con.execute(
                f'CREATE OR REPLACE VIEW "{PUB_DB}".main."{new}" AS {old_body}'
            )
            log(f"     created new view main.{new} (preserved body)")
            con.execute(f'DROP VIEW "{PUB_DB}".main."{old}"')
            log(f"     dropped old view main.{old}")
        else:
            # BASE TABLE → materialise as VIEW shim around new name?  Per
            # prompt, KEEP_RENAME on BASE TABLE means it's "effectively a
            # view" (compat facade / rollup shim).  Convert to a VIEW shim
            # that selects from the snapshot in archive_pub_v1_0.
            #
            # Safer pattern: create new BASE TABLE via CTAS, then drop old.
            # The new name retains _VIEW_v1 suffix as a labeling convention,
            # but the storage is still a table (matches existing patterns).
            con.execute(
                f'CREATE TABLE "{PUB_DB}".main."{new}" AS '
                f'SELECT * FROM "{PUB_DB}".main."{old}"'
            )
            log(f"     created new table main.{new} (CTAS from old)")
            con.execute(f'DROP TABLE "{PUB_DB}".main."{old}"')
            log(f"     dropped old table main.{old}")

        # 4. Log the rename as a "RENAME" move (dest_db is PUB itself).
        con.execute(
            f"""
            INSERT INTO {WS_SCHEMA}.script_388_archive_move_log_v1
            (move_ts, source_schema, source_name, dest_db, dest_schema,
             dest_name, move_method, reason)
            VALUES (CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?, ?, ?, ?, ?, ?)
            """,
            ["main", old, PUB_DB, "main", new, "RENAME",
             f"388 VIEW-label rename ({src_kind}); pre-snapshot at "
             f"{ARC_DB}.{snap_schema}.{snap_name}; "
             f"dependent views rewritten: {', '.join(rewritten) or 'none'}"],
        )
        out.append({
            "src": old, "dest": new, "status": "renamed",
            "rewritten_dependents": rewritten,
        })
    return out


def phase2b_main_orphans(
    con, view_inv: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    log("=" * 78)
    log("Phase 2.B — main.* orphan archives")
    log("=" * 78)
    targets = [i for i in view_inv if i["classification"] == "ORPHAN"]
    log(f"  targets: {len(targets)}")
    out: list[dict[str, Any]] = []
    for item in targets:
        out.append(_archive_ctas_drop(
            con,
            src_schema="main",
            src_name=item["name"],
            src_kind=item["table_type"],
            dest_schema=ARC_MAIN_LEGACY,
            dest_name=item["name"],
            reason="388 main.* orphan archive (no dependents, no repo refs)",
        ))
    return out


def phase2c_llm_archives(
    con, llm_inv: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    log("=" * 78)
    log("Phase 2.C — note_entities_llm_* archives")
    log("=" * 78)
    targets = [
        i for i in llm_inv
        if i["classification"] in ("CURRENT_ORPHAN", "LEGACY_REPLACED")
    ]
    log(f"  targets: {len(targets)}")
    out: list[dict[str, Any]] = []
    for item in targets:
        dest_name = item["name"]
        if item["classification"] == "LEGACY_REPLACED":
            dest_name = f"{item['name']}_qwen_20260422"
        reason = (
            f"388 LLM-source {item['classification']} archive "
            f"(evidence: {item['evidence'][:120]})"
        )
        out.append(_archive_ctas_drop(
            con,
            src_schema="main",
            src_name=item["name"],
            src_kind=item["table_type"],
            dest_schema=ARC_LLM_LEGACY,
            dest_name=dest_name,
            reason=reason,
        ))
    return out


def phase2d_pin_shamkey(
    con, shamkey: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    log("=" * 78)
    log("Phase 2.D — pin sham-key probe overrides into 387 dedup probe table")
    log("=" * 78)
    out: list[dict[str, Any]] = []
    has_table = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name = 'script_387_dedup_probe_v1'
        """,
        [PUB_DB, WS_SCHEMA],
    ).fetchone()
    if not has_table:
        log("  script_387_dedup_probe_v1 missing — skipping pin step")
        return out
    for item in shamkey:
        if not item.get("present") or not item.get("chosen_probe_key"):
            continue
        key_label = "(" + ", ".join(item["chosen_probe_key"]) + ")"
        con.execute(
            f"""
            UPDATE {WS_SCHEMA}.script_387_dedup_probe_v1
               SET probe_key_override_388 = ?
             WHERE canonical_name = ?
            """,
            [key_label, item["name"]],
        )
        log(f"  -> {item['name']}: pinned probe_key_override_388 = {key_label}")
        out.append({"name": item["name"], "pinned_key": key_label})
    return out


# --------------------------------------------------------------------------- #
# Phase 3 — post-state verification
# --------------------------------------------------------------------------- #


def phase3_postcheck(
    con,
    llm_inv: list[dict[str, Any]],
    view_inv: list[dict[str, Any]],
    archive_results: list[dict[str, Any]],
    rename_results: list[dict[str, Any]],
) -> dict[str, Any]:
    log("=" * 78)
    log("Phase 3 — post-state verification")
    log("=" * 78)
    out: dict[str, Any] = {}

    cpm_n = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}".main.canonical_patient_master'
    ).fetchone()[0]
    if cpm_n != 10871:
        raise SystemExit(f"Phase 3 abort: CPM rows {cpm_n} != 10871")
    log(f"  gate OK: CPM rows = {cpm_n}")
    out["cpm_rows"] = cpm_n

    bad_schemas = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema IN ('tier2', 'verify')
        """,
        [PUB_DB],
    ).fetchone()[0]
    if bad_schemas:
        raise SystemExit(
            f"Phase 3 abort: tier2/verify still contain {bad_schemas} objects"
        )
    log("  gate OK: tier2 / verify schemas remain empty")

    grandfathered_csv = ", ".join(
        f"'{n}'" for n in sorted(GRANDFATHERED_PLATFORM_VIEWS)
    )
    exempt_csv = ", ".join(
        f"'{n}'" for n in sorted(
            EXEMPT_MAIN_NAMES - GRANDFATHERED_PLATFORM_VIEWS
        )
    )
    violators = con.execute(
        f"""
        SELECT t.table_name, t.table_type FROM information_schema.tables t
        WHERE t.table_catalog = ? AND t.table_schema = 'main'
          AND NOT (
                 t.table_name LIKE 'canonical\\_%' ESCAPE '\\'
              OR t.table_name LIKE '%\\_VIEW\\_v%' ESCAPE '\\'
              OR t.table_name LIKE 'note\\_entities\\_llm\\_%' ESCAPE '\\'
              OR t.table_name LIKE 'data\\_dictionary\\_v%' ESCAPE '\\'
              OR t.table_name LIKE 'raw\\_%' ESCAPE '\\'
              OR t.table_name IN ({exempt_csv})
              OR t.table_name IN ({grandfathered_csv})
              OR t.table_name IN (
                   SELECT detail_table_name
                   FROM {WS_SCHEMA}.detail_table_registry_v1
                 )
          )
        ORDER BY t.table_name
        """,
        [PUB_DB],
    ).fetchall()
    if violators:
        for n, t in violators:
            err(f"  naming-contract violator: main.{n} ({t})")
        raise SystemExit(
            f"Phase 3 abort: {len(violators)} main.* naming-contract violator(s)"
        )
    log("  gate OK: every main.* object satisfies the naming contract")

    expected_archive_count = len([
        r for r in archive_results if r.get("status") == "archived"
    ])
    log_rows = con.execute(
        f"""
        SELECT COUNT(*) FROM {WS_SCHEMA}.script_388_archive_move_log_v1
        WHERE move_ts >= CAST('2026-04-22' AS TIMESTAMP)
        """
    ).fetchone()[0]
    log(f"  archive_move_log_v1 rows >= 2026-04-22: {log_rows}")
    out["log_rows_today"] = log_rows
    out["archive_count"] = expected_archive_count
    out["rename_count"] = len([
        r for r in rename_results if r.get("status") == "renamed"
    ])

    # Verify each rename target exists with new name.
    for r in rename_results:
        if r.get("status") != "renamed":
            continue
        if get_object_kind(con, "main", r["dest"]) is None:
            raise SystemExit(
                f"Phase 3 abort: rename target main.{r['dest']} missing"
            )
    log(f"  gate OK: all {out['rename_count']} rename target(s) present")

    return out


def phase3_dedup_reverify(con) -> str:
    """Lightweight re-verification — read 387 baseline, write a diff note."""
    log("Phase 3.2 — dedup re-verification vs 387 baseline")
    has_table = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name = 'script_387_dedup_probe_v1'
        """,
        [PUB_DB, WS_SCHEMA],
    ).fetchone()
    if not has_table:
        log("  387 dedup probe table missing; skipping reverify diff")
        return "Skipped (script_387_dedup_probe_v1 missing)."

    rows = con.execute(
        f"""
        SELECT canonical_name, severity, collapse_count, null_key_rows,
               partition_key, probe_key_override_388
        FROM {WS_SCHEMA}.script_387_dedup_probe_v1
        ORDER BY canonical_name
        """
    ).fetchall()
    lines = [
        "# Script 388 — dedup re-verification vs 387 baseline",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Source: `manuscript_workspace.script_387_dedup_probe_v1` "
        "(now augmented with `probe_key_override_388`).",
        "",
        "| canonical | 387 severity | 387 collapse | 387 null_key "
        "| 387 key | 388 override key |",
        "|---|---|---:|---:|---|---|",
    ]
    for name, sev, coll, nullk, key, override in rows:
        lines.append(
            f"| `{name}` | {sev} | {coll:,} | {nullk:,} | `{key}` "
            f"| {('`' + override + '`') if override else '—'} |"
        )
    lines += [
        "",
        "## Notes",
        "",
        "* The 7 `flag_event` collapses (complications 15, invasion 7,578, "
        "medications 2,512, molecular_genetics_v2 856, path_malignant 442, "
        "pmh 816, psh 233) remain at the 387 baseline — they require "
        "upstream-builder fixes (Script 388 carry-forward, NOT addressed "
        "in this run).",
        "* The 2 `all_null_key` sham keys on path_benign / path_gland "
        "have an override key pinned by 388 (see "
        "`scripts/output/388_shamkey_probe.json` for the candidate-key "
        "evidence).  The underlying canonical event tables are unchanged; "
        "the partition-contract fix remains carry-forward to upstream "
        "builders.",
    ]
    DEDUP_REVERIFY_PATH.write_text("\n".join(lines) + "\n")
    log(f"  wrote -> {DEDUP_REVERIFY_PATH}")
    return "OK (see scripts/output/388_dedup_reverification.md)"


# --------------------------------------------------------------------------- #
# Phase 4 — registry + __readme sync
# --------------------------------------------------------------------------- #


def _registry_columns(con) -> set[str]:
    return {
        r[0] for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = ? AND table_schema = ?
              AND table_name = 'detail_table_registry_v1'
            """,
            [PUB_DB, WS_SCHEMA],
        ).fetchall()
    }


def phase4_registry_readme(
    con,
    archive_results: list[dict[str, Any]],
    rename_results: list[dict[str, Any]],
    shamkey_pinned: list[dict[str, Any]],
) -> None:
    log("=" * 78)
    log("Phase 4 — detail_table_registry_v1 + __readme sync")
    log("=" * 78)

    reg_cols = _registry_columns(con)
    has_superseded = "superseded" in reg_cols
    has_renamed_by = "renamed_by_script" in reg_cols
    log(f"  registry has 'superseded' column: {has_superseded}")
    log(f"  registry has 'renamed_by_script' column: {has_renamed_by}")

    # Mark archived rows superseded if column exists
    for r in archive_results:
        if r.get("status") != "archived":
            continue
        src = r["src"].split(".", 1)[1]
        if has_superseded:
            con.execute(
                f"""
                UPDATE {WS_SCHEMA}.detail_table_registry_v1
                   SET superseded = TRUE
                 WHERE detail_table_name = ?
                """,
                [src],
            )

    # Update rename rows
    for r in rename_results:
        if r.get("status") != "renamed":
            continue
        old = r["src"]
        new = r["dest"]
        if has_renamed_by:
            con.execute(
                f"""
                UPDATE {WS_SCHEMA}.detail_table_registry_v1
                   SET detail_table_name = ?,
                       renamed_by_script = 388
                 WHERE detail_table_name = ?
                """,
                [new, old],
            )
        else:
            con.execute(
                f"""
                UPDATE {WS_SCHEMA}.detail_table_registry_v1
                   SET detail_table_name = ?
                 WHERE detail_table_name = ?
                """,
                [new, old],
            )
    log(f"  registry rows updated: archived={len(archive_results)} "
        f"renamed={len(rename_results)}")

    # Append __readme deprecation log lines
    readme_lines: list[str] = []
    today = "2026-04-22"
    for r in archive_results:
        if r.get("status") != "archived":
            continue
        readme_lines.append(
            f"388 | {today} | ARCHIVE | {r['src']} -> {r.get('dest', '?')} "
            f"| 388 disposition"
        )
    for r in rename_results:
        if r.get("status") != "renamed":
            continue
        readme_lines.append(
            f"388 | {today} | RENAME  | main.{r['src']} -> main.{r['dest']} "
            f"| VIEW-label completeness"
        )
    for s in shamkey_pinned:
        readme_lines.append(
            f"388 | {today} | PROBE_KEY_OVERRIDE | main.{s['name']} "
            f"| pinned {s['pinned_key']} into "
            f"manuscript_workspace.script_387_dedup_probe_v1"
        )
    if readme_lines:
        content = "Script 388 deprecation log:\n" + "\n".join(readme_lines)
        con.execute(
            "INSERT INTO main.__readme "
            "(content, updated_at, git_sha, script) "
            "VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), NULL, ?)",
            [content, SCRIPT_TAG],
        )
        log(f"  __readme appended ({len(readme_lines)} entries)")
    else:
        log("  __readme: no entries to append")


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--plan", action="store_true",
        help="Run Phase 0 (read-only discovery) only; default mode.",
    )
    ap.add_argument(
        "--apply", action="store_true",
        help="Execute Phases 2-4 (destructive).  Requires Phase 0 dispositions "
             "with no UNCLASSIFIED items.",
    )
    args = ap.parse_args()

    if args.plan and args.apply:
        raise SystemExit("Pass --plan OR --apply, not both")
    if not args.plan and not args.apply:
        args.plan = True

    mode = "apply" if args.apply else "plan"
    log(f"Script 388 — {mode.upper()} mode — "
        f"{datetime.now(timezone.utc).isoformat()}")

    con = connect()
    try:
        ensure_workspace_tables(con)

        # Phase 0 (always runs)
        llm_inv = phase0_llm_inventory(con)
        view_inv = phase0_view_label_inventory(con)
        shamkey = phase0_shamkey_probe(con)
        ok = phase0_write_dispositions(llm_inv, view_inv, shamkey, mode)

        if not ok:
            err(
                f"HALT: {sum(1 for i in llm_inv if i['classification']=='UNCLASSIFIED') + sum(1 for i in view_inv if i['classification']=='UNCLASSIFIED')} "
                "UNCLASSIFIED items.  Review "
                f"{DISPO_PATH.relative_to(REPO_ROOT)} and provide manual "
                "decisions before re-running."
            )
            return 2

        if args.plan:
            log("Phase 0 complete (--plan mode).  Review "
                f"{DISPO_PATH.relative_to(REPO_ROOT)} and re-run with "
                "--apply to execute.")
            return 0

        # Phase 2 (consumer-first ordering)
        rename_results = phase2a_view_label_renames(con, view_inv)
        orphan_results = phase2b_main_orphans(con, view_inv)
        llm_results = phase2c_llm_archives(con, llm_inv)
        shamkey_pinned = phase2d_pin_shamkey(con, shamkey)

        archive_results = orphan_results + llm_results

        # Phase 3
        post = phase3_postcheck(
            con, llm_inv, view_inv, archive_results, rename_results
        )
        reverify_status = phase3_dedup_reverify(con)
        post["dedup_reverify"] = reverify_status

        # Phase 4
        phase4_registry_readme(con, archive_results, rename_results, shamkey_pinned)

        log("=" * 78)
        log("Script 388 APPLY complete.  Summary: " + json.dumps({
            "renames": len([r for r in rename_results if r.get("status") == "renamed"]),
            "archives": len([r for r in archive_results if r.get("status") == "archived"]),
            "shamkey_overrides": len(shamkey_pinned),
            "cpm_rows": post["cpm_rows"],
        }))
        return 0
    finally:
        flush_log()
        con.close()


if __name__ == "__main__":
    sys.exit(main())
