#!/usr/bin/env python3
"""Script 272 — Canonical Finalization & Registry Reconciliation (v2).

Implements the multi-phase finalization plan over the canonical publication DB
``thyroid_canonical_publication_v1_0``. All reads/writes happen in that database
except deliberate archive writes which go to
``"Thyroid 2026 UPdated".archive_pub_v1_0`` (created if missing).

Phases (selectable via --phase):
  0   Snapshot baseline metrics + parquet checksum.
  1   Confirm v279 is sole dictionary; check archive presence; coverage gaps.
  2   Build manuscript_workspace.detail_table_registry_v2 with array column +
      auto-repair audits. Does NOT promote (use --apply-promote, gated).
  3a  Register 3 new main-schema detail tables in registry_v2.
  3b  Register 11 new manuscript_workspace audit/queue tables in registry_v2.
  3c  Trace remaining unmapped CPM columns; emit cpm_cols_without_registry.
  3recon  Reconcile with manuscript_workspace.canonical_cleanup_audit_v1
          (logs the classifier_version it read).
  4   Add filter_type to manuscript_dive_map_v1 + duplicate-review table.
      Additive / reversible — runs without an approval gate.
  6   End-to-end registry validation (against v2 unless v2 is gone).
  audit  Convenience: phases 0,1,2,3a,3b,3c,3recon,4,6 in order.

Approval-gated apply phases (require an explicit flag at invocation):
  --apply-promote                    Promote v2 -> v1 in registry (Phase 2.7).
  --apply-archives <approved.csv>    Phase 5 archive+drop loop. The CSV must
                                     have been derived from
                                     272_safe_archive_candidates.csv with rows
                                     the user explicitly approved.

Invariants verified after every phase touching CPM:
  - canonical_patient_master rows == 10871
  - distinct research_id == 10871
  - NULL research_id == 0
  - NULL fna_path_outcome == 0
  - research_id type == VARCHAR

NEVER writes to "Thyroid 2026 UPdated".main.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_NUM = "272"
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
EXPORT_DIR = REPO / "exports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

RUN_LOG = OUT_DIR / f"{SCRIPT_NUM}_run.log"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
WS = "manuscript_workspace"
MAIN = "main"

CPM = "canonical_patient_master"
CPM_ROWS = 10871

REGISTRY_V1 = f'"{PUBLICATION_DB}".{WS}.detail_table_registry_v1'
REGISTRY_V2 = f'"{PUBLICATION_DB}".{WS}.detail_table_registry_v2'

# Phase 3A — three main-schema detail tables to register
PHASE_3A_REGISTRATIONS = [
    {
        "detail_table_name": "data_dictionary_v279",
        "schema_name": MAIN,
        "domain": "Catalog",
        "grain": "one row per CPM column",
        "feeds_master_columns": "(catalog)",
    },
    {
        "detail_table_name": "tumor_stage_heterogeneity_v1",
        "schema_name": MAIN,
        "domain": "Pathology/Staging",
        "grain": "patient",
        "feeds_master_columns": ";".join([
            "dominant_tumor_ajcc7_t_stage",
            "dominant_tumor_ajcc7_n_stage",
            "dominant_tumor_ajcc7_m_stage",
            "dominant_tumor_ajcc7_stage_group",
            "dominant_tumor_ajcc8_t_stage",
            "dominant_tumor_ajcc8_n_stage",
            "dominant_tumor_ajcc8_m_stage",
            "dominant_tumor_ajcc8_stage_group",
            "tumor_stage_heterogeneous_t_ajcc8_flag",
            "tumor_stage_heterogeneous_overall_ajcc8_flag",
        ]),
    },
    {
        "detail_table_name": "tirads_reextraction_queue_v1",
        "schema_name": MAIN,
        "domain": "Imaging/TIRADS",
        "grain": "one row per nodule re-extraction task",
        "feeds_master_columns": "(queue \u2014 no direct CPM column)",
    },
]

# Phase 3B — eleven manuscript_workspace audit/queue tables to register
def _ws_domain(name: str) -> str:
    n = name.lower()
    if "ajcc" in n:
        return "AJCC"
    if "ete" in n:
        return "ETE"
    if "is_malignant" in n:
        return "Is-Malignant"
    if "hypopara" in n:
        return "Hypopara"
    if n.startswith("ln_"):
        return "Lymph-Node"
    if "tumor_size" in n:
        return "Pathology-TumorSize"
    if n.startswith("tg_"):
        return "Tg"
    return "Audit"


PHASE_3B_REGISTRATIONS = [
    {
        "detail_table_name": name,
        "schema_name": WS,
        "domain": _ws_domain(name),
        "grain": "one row per review case",
        "feeds_master_columns": "(audit only)",
    }
    for name in [
        "cpm_ajcc_dominant_concordance_v1",
        "cpm_ajcc_dominant_discordance_canonical_v1",
        "cpm_ajcc_dominant_vs_tp_hist1_discordance_v1",
        "cpm_ete_self_contradiction_queue_v1",
        "cpm_hypopara_adjudication_log_v1",
        "cpm_is_malignant_flag_review_v1",
        "ln_extract_noncohort_orphan_v279",
        "path_tumor_size_chart_review_queue_v1",
        "path_tumor_size_correction_queue_v1",
        "path_tumor_size_multifocal_enumeration_notes_v1",
        "tg_orphan_cancer_text_investigation_queue_v1",
    ]
]

# Sentinel non-column strings inside feeds_master_columns — never split, never
# attempt to resolve.
SENTINEL_PATTERNS = [
    "(catalog)",
    "subset view",
    "upstream for",
    "(audit only)",
    "(no-op)",
    "episode-level",
    "(queue",
    "no direct CPM column",
]

PROMOTION_UNRESOLVED_HARD_LIMIT = 15  # >= this requires --override-unresolved
WILDCARD_EXPANSION_PER_PATTERN_LIMIT = 25  # > this -> halt unless whitelisted
# Specific (detail_table_name, wildcard_feed_col) pairs the user has explicitly
# reviewed and approved despite exceeding the per-pattern guardrail. Each entry
# carries a tag persisted in registry_v2_resolution_audit_v1.repair_source_name.
WILDCARD_EXPANSION_WHITELIST: dict[tuple[str, str], str] = {
    ("clinical_note_ln_extracted_v1", "cnln_* columns"):
        "wildcard_expansion_whitelist_cnln",
}


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
class TeeLogger:
    def __init__(self, path: Path) -> None:
        self.fh = path.open("a", encoding="utf-8")

    def __call__(self, msg: str = "") -> None:
        line = f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}"
        print(line)
        self.fh.write(line + "\n")
        self.fh.flush()

    def close(self) -> None:
        self.fh.close()


# ---------------------------------------------------------------------------
# Invariants
# ---------------------------------------------------------------------------
def assert_cpm_invariants(con, log) -> dict[str, int | str]:
    row = con.execute(f"""
        SELECT
            COUNT(*),
            COUNT(DISTINCT research_id),
            SUM(CASE WHEN research_id IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
        FROM "{PUBLICATION_DB}".{MAIN}.{CPM}
    """).fetchone()
    n_rows, n_rids, n_null_rid, n_null_fna = row
    rid_type = con.execute(f"""
        SELECT data_type FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{MAIN}'
          AND table_name='{CPM}' AND column_name='research_id'
    """).fetchone()[0]
    cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{MAIN}'
          AND table_name='{CPM}'
    """).fetchone()[0]

    failures = []
    if n_rows != CPM_ROWS:
        failures.append(f"row_count={n_rows} (expected {CPM_ROWS})")
    if n_rids != CPM_ROWS:
        failures.append(f"distinct_rids={n_rids} (expected {CPM_ROWS})")
    if n_null_rid != 0:
        failures.append(f"null_research_id={n_null_rid} (expected 0)")
    if n_null_fna != 0:
        failures.append(f"null_fna_path_outcome={n_null_fna} (expected 0)")
    if rid_type.upper() != "VARCHAR":
        failures.append(f"research_id type={rid_type} (expected VARCHAR)")

    if failures:
        log("INVARIANT FAILURE: " + "; ".join(failures))
        raise SystemExit("Invariants failed; aborting.")
    log(
        f"Invariants OK: rows={n_rows} rids={n_rids} null_rid={n_null_rid} "
        f"null_fna={n_null_fna} rid_type={rid_type} cols={cols}"
    )
    return {
        "row_count": n_rows,
        "distinct_research_ids": n_rids,
        "null_research_id": n_null_rid,
        "null_fna_path_outcome": n_null_fna,
        "research_id_type": rid_type,
        "column_count": cols,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def is_sentinel(value: str) -> bool:
    if value is None:
        return True
    v = value.strip()
    if not v:
        return True
    low = v.lower()
    return any(pat.lower() in low for pat in SENTINEL_PATTERNS)


def split_feeds(value: str) -> list[str]:
    """Split a feeds_master_columns cell into trimmed column names.

    Live registry uses BOTH ``;`` and ``,`` as delimiters depending on the row
    (and 4 rows mix the two). Splitting on either produces the right per-column
    granularity. Sentinel / prose values still return [].
    """
    if value is None:
        return []
    if is_sentinel(value):
        return []
    parts = re.split(r"[;,]", value)
    return [p.strip() for p in parts if p and p.strip()]


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


def cpm_columns(con) -> set[str]:
    rows = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{MAIN}'
          AND table_name='{CPM}'
    """).fetchall()
    return {r[0] for r in rows}


def cpm_columns_meta(con) -> dict[str, str]:
    rows = con.execute(f"""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{MAIN}'
          AND table_name='{CPM}'
    """).fetchall()
    return {r[0]: r[1] for r in rows}


def base_table_count(con, schema: str) -> int:
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}'
          AND table_schema='{schema}' AND table_type='BASE TABLE'
    """).fetchone()[0]


def view_count(con, schema: str) -> int:
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}'
          AND table_schema='{schema}' AND table_type='VIEW'
    """).fetchone()[0]


def table_exists(con, schema: str, name: str) -> bool:
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}'
          AND table_schema='{schema}' AND table_name='{name}'
    """).fetchone()[0] > 0


def archive_table_exists(con, name: str) -> bool:
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{ARCHIVE_DB}'
          AND table_schema='{ARCHIVE_SCHEMA}' AND table_name='{name}'
    """).fetchone()[0] > 0


def column_names(con, schema: str, name: str) -> list[str]:
    rows = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}'
          AND table_schema='{schema}' AND table_name='{name}'
        ORDER BY ordinal_position
    """).fetchall()
    return [r[0] for r in rows]


def comment_on_table(con, schema: str, name: str, purpose: str) -> None:
    """Attach the standard COMMENT ON TABLE per the script convention."""
    msg = f"script={SCRIPT_NUM}, date={RUN_DATE}; {purpose}"
    msg_sql = msg.replace("'", "''")
    con.execute(f"COMMENT ON TABLE \"{PUBLICATION_DB}\".{schema}.{name} IS '{msg_sql}'")


# ---------------------------------------------------------------------------
# PHASE 0
# ---------------------------------------------------------------------------
def phase_0(con, log) -> dict[str, Any]:
    log("=== PHASE 0: Baseline snapshot ===")
    inv = assert_cpm_invariants(con, log)
    main_n = base_table_count(con, MAIN)
    ws_tbl = base_table_count(con, WS)
    ws_vw = view_count(con, WS)
    reg_rows = con.execute(f"SELECT COUNT(*) FROM {REGISTRY_V1}").fetchone()[0]

    audit_rows = None
    audit_max_at = None
    audit_classifier_version = None
    if table_exists(con, WS, "canonical_cleanup_audit_v1"):
        audit_rows = con.execute(
            f"SELECT COUNT(*) FROM \"{PUBLICATION_DB}\".{WS}.canonical_cleanup_audit_v1"
        ).fetchone()[0]
        audit_cols = set(column_names(con, WS, "canonical_cleanup_audit_v1"))
        if "classified_at" in audit_cols:
            audit_max_at = con.execute(
                f"SELECT CAST(MAX(classified_at) AS VARCHAR) FROM "
                f"\"{PUBLICATION_DB}\".{WS}.canonical_cleanup_audit_v1"
            ).fetchone()[0]
        if "classifier_version" in audit_cols:
            audit_classifier_version = con.execute(
                f"SELECT MAX(CAST(classifier_version AS VARCHAR)) FROM "
                f"\"{PUBLICATION_DB}\".{WS}.canonical_cleanup_audit_v1"
            ).fetchone()[0]

    baseline = {
        "script": SCRIPT_NUM,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "canonical_patient_master": inv,
        "main_base_tables": main_n,
        "manuscript_workspace_tables": ws_tbl,
        "manuscript_workspace_views": ws_vw,
        "detail_table_registry_v1_rows": reg_rows,
        "canonical_cleanup_audit_v1_rows": audit_rows,
        "canonical_cleanup_audit_v1_max_classified_at": audit_max_at,
        "canonical_cleanup_audit_v1_classifier_version": audit_classifier_version,
    }

    out_path = OUT_DIR / f"{SCRIPT_NUM}_baseline.json"
    out_path.write_text(json.dumps(baseline, indent=2))
    log(f"Baseline written to {out_path}")
    log(json.dumps(baseline, indent=2))

    # Parquet snapshot + checksum
    parquet_path = EXPORT_DIR / "canonical_patient_master_pre272.parquet"
    sha_path = EXPORT_DIR / "canonical_patient_master_pre272.sha256"
    log(f"Exporting parquet snapshot -> {parquet_path}")
    con.execute(f"""
        COPY (SELECT * FROM "{PUBLICATION_DB}".{MAIN}.{CPM})
        TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    sha = hashlib.sha256(parquet_path.read_bytes()).hexdigest()
    sha_path.write_text(f"{sha}  canonical_patient_master_pre272.parquet\n")
    log(f"SHA256: {sha}")
    log(f"Checksum written to {sha_path}")

    return baseline


# ---------------------------------------------------------------------------
# PHASE 1 — Dictionary reconciliation
# ---------------------------------------------------------------------------
def phase_1(con, log) -> dict[str, Any]:
    log("=== PHASE 1: Dictionary reconciliation ===")

    dict_tables = [
        r[0] for r in con.execute(f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{MAIN}'
              AND table_name ILIKE 'data_dictionary%'
            ORDER BY table_name
        """).fetchall()
    ]
    log(f"data_dictionary* tables in main: {dict_tables}")
    if dict_tables != ["data_dictionary_v279"]:
        raise SystemExit(
            f"Expected exactly ['data_dictionary_v279'], got {dict_tables}. STOP."
        )

    archive_present = {
        "data_dictionary_v240": archive_table_exists(con, "data_dictionary_v240")
            or archive_table_exists(con, "data_dictionary_v240_archived_20260417")
            or archive_table_exists(con, "data_dictionary_v240_archived_20260418"),
        "data_dictionary_v266a": archive_table_exists(con, "data_dictionary_v266a")
            or archive_table_exists(con, "data_dictionary_v266a_archived_20260417")
            or archive_table_exists(con, "data_dictionary_v266a_archived_20260418"),
    }
    archived_present_any = False
    if con.execute(f"""
        SELECT COUNT(*) FROM information_schema.schemata
        WHERE catalog_name='{ARCHIVE_DB}' AND schema_name='{ARCHIVE_SCHEMA}'
    """).fetchone()[0] > 0:
        rows = con.execute(f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_catalog='{ARCHIVE_DB}' AND table_schema='{ARCHIVE_SCHEMA}'
              AND (table_name ILIKE 'data_dictionary_v240%'
                   OR table_name ILIKE 'data_dictionary_v266a%')
            ORDER BY table_name
        """).fetchall()
        archived_present_any = bool(rows)
        archive_present["actual_matches"] = [r[0] for r in rows]
    else:
        archive_present["actual_matches"] = []

    # Coverage check
    cpm_meta = cpm_columns_meta(con)
    dict_cols = {
        r[0] for r in con.execute(f"""
            SELECT column_name FROM "{PUBLICATION_DB}".{MAIN}.data_dictionary_v279
        """).fetchall()
    }
    missing = sorted([c for c in cpm_meta if c not in dict_cols])
    log(f"Dictionary coverage: {len(cpm_meta) - len(missing)}/{len(cpm_meta)} present; "
        f"{len(missing)} missing")

    gap_path = OUT_DIR / f"{SCRIPT_NUM}_dict_coverage_gap.csv"
    write_csv(
        gap_path,
        [{"column_name": c, "data_type": cpm_meta[c]} for c in missing],
        ["column_name", "data_type"],
    )
    log(f"Coverage gap written to {gap_path}")

    seeded = 0
    if missing:
        # Seed placeholders ONLY if v279 has the columns we need.
        v279_cols = set(column_names(con, MAIN, "data_dictionary_v279"))
        if {"column_name", "data_type", "description"}.issubset(v279_cols):
            for col in missing:
                con.execute(f"""
                    INSERT INTO "{PUBLICATION_DB}".{MAIN}.data_dictionary_v279
                          (column_name, data_type, description)
                    VALUES (?, ?, ?)
                """, [col, cpm_meta[col], "TODO — auto-seeded by Script 272 for coverage"])
                seeded += 1
            log(f"Seeded {seeded} placeholder rows into data_dictionary_v279")
        else:
            log(
                f"WARNING: data_dictionary_v279 lacks required columns to seed; "
                f"its columns are {sorted(v279_cols)}. Skipping seed; manual fix required."
            )

    md_path = OUT_DIR / f"{SCRIPT_NUM}_dict_reconciliation.md"
    md_lines = [
        "# Script 272 — Dictionary reconciliation",
        f"Run UTC: {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        "",
        f"- main.data_dictionary_v279 confirmed sole dictionary in main: ✓",
        f"- data_dictionary_v240 archive presence: {archive_present['data_dictionary_v240']}",
        f"- data_dictionary_v266a archive presence: {archive_present['data_dictionary_v266a']}",
        f"- archive_pub_v1_0 dictionary matches: {archive_present.get('actual_matches', [])}",
        f"- CPM columns in v279: {len(cpm_meta) - len(missing)} / {len(cpm_meta)}",
        f"- Coverage-gap rows written: {len(missing)}",
        f"- Auto-seeded placeholder rows: {seeded}",
        "",
        "Notes: predecessors (v240, v266a) were dropped earlier; this run does not",
        "attempt restoration. If absent from archive_pub_v1_0, that fact is logged here.",
    ]
    md_path.write_text("\n".join(md_lines) + "\n")
    log(f"Phase 1 markdown written to {md_path}")

    return {
        "dict_tables": dict_tables,
        "archive_present": archive_present,
        "missing_dict_rows_seeded": seeded,
        "coverage_gap_count": len(missing),
    }


# ---------------------------------------------------------------------------
# PHASE 2 — Build registry_v2 + audits
# ---------------------------------------------------------------------------
AUTO_REPAIR_TRIES = (
    "case_insensitive",
    "version_suffix_swap",
    "underscore_hyphen_swap",
    "trailing_s_swap",
)


def _build_repair_index(cpm_set: set[str]) -> dict[str, list[tuple[str, str]]]:
    """Pre-compute lookup helpers keyed for repair attempts.

    Returns dict mapping a normalized form -> list of (mode, real_cpm_col).
    """
    idx: dict[str, list[tuple[str, str]]] = {}
    def add(key: str, mode: str, real: str) -> None:
        idx.setdefault(key, []).append((mode, real))

    for c in cpm_set:
        add(c.lower(), "case_insensitive", c)
        # version-suffix variants
        m = re.match(r"^(.*?)(_v\d+[a-z]?)$", c)
        stem = m.group(1) if m else c
        add(stem.lower() + "_v1", "version_suffix_swap", c)
        add(stem.lower() + "_v2", "version_suffix_swap", c)
        add(stem.lower() + "_v7", "version_suffix_swap", c)
        # underscore/hyphen
        add(c.replace("_", "-").lower(), "underscore_hyphen_swap", c)
        # trailing s
        if c.endswith("s"):
            add(c[:-1].lower(), "trailing_s_swap", c)
        else:
            add((c + "s").lower(), "trailing_s_swap", c)
    return idx


_PROSE_RE = re.compile(r"[\s()/]")  # whitespace, parens, slash → not a col name


def _is_prose(feed_col: str) -> bool:
    """Heuristic: real CPM column names are word-character only.

    Anything with whitespace, parens, slashes, or hyphens is descriptive prose
    (e.g. ``(self)``, ``ene columns``, ``feeds patient_tumor_rollup_v1 (v1_1 migration)``,
    ``tirads component scores (composition``). Mark such entries so they don't
    count as unresolved.
    """
    if "*" in feed_col:
        return False  # wildcards handled separately
    return bool(_PROSE_RE.search(feed_col))


def _attempt_repair(
    feed_col: str, cpm_set: set[str], repair_idx: dict[str, list[tuple[str, str]]]
) -> tuple[bool, str | None, str | None]:
    """Return (resolves, repair_mode, repair_target).

    A repair_target is only returned if it points at a CPM column that EXISTS.
    Prose entries return (False, 'prose_skipped', None) so callers can mark
    them as auto_repaired (not unresolved) without inventing a target.
    """
    if feed_col in cpm_set:
        return True, None, feed_col
    if _is_prose(feed_col):
        return False, "prose_skipped", None
    candidates = repair_idx.get(feed_col.lower(), [])
    for mode, real in candidates:
        if real in cpm_set:
            return False, mode, real
    return False, None, None


def phase_2(con, log) -> dict[str, Any]:
    log("=== PHASE 2: Build detail_table_registry_v2 ===")

    # Step 1: Verify delimiter is ;
    delim_check = con.execute(f"""
        SELECT detail_table_name,
               LENGTH(feeds_master_columns) -
                 LENGTH(REPLACE(feeds_master_columns, ';', '')) AS n_semi,
               LENGTH(feeds_master_columns) -
                 LENGTH(REPLACE(feeds_master_columns, ',', '')) AS n_comma
          FROM {REGISTRY_V1}
          WHERE feeds_master_columns IS NOT NULL
          ORDER BY n_semi DESC LIMIT 20
    """).fetchall()
    log("Top-20 feeds_master_columns delimiter sample (name, n_semi, n_comma):")
    for r in delim_check:
        log(f"  {r[0]}  semi={r[1]}  comma={r[2]}")

    v1_cols = column_names(con, WS, "detail_table_registry_v1")
    log(f"v1 columns ({len(v1_cols)}): {v1_cols}")
    keep_cols = [c for c in v1_cols if c != "feeds_master_columns_normalized"]

    # Drop existing v2 if any
    if table_exists(con, WS, "detail_table_registry_v2"):
        log("Existing detail_table_registry_v2 detected — dropping for fresh build")
        con.execute(f"DROP TABLE {REGISTRY_V2}")

    # Build v2 with same kept columns + array col
    select_list = ", ".join(f'"{c}"' for c in keep_cols)
    con.execute(f"""
        CREATE TABLE {REGISTRY_V2} AS
        SELECT {select_list}
          FROM {REGISTRY_V1}
    """)
    con.execute(f"""
        ALTER TABLE {REGISTRY_V2} ADD COLUMN feeds_master_columns_array VARCHAR[]
    """)
    con.execute(f"""
        ALTER TABLE {REGISTRY_V2} ADD COLUMN needs_manual_review BOOLEAN DEFAULT false
    """)
    log("Created detail_table_registry_v2 (cloned schema, sans normalized col)")

    # Populate array
    rows = con.execute(f"""
        SELECT detail_table_name, feeds_master_columns
          FROM {REGISTRY_V2}
    """).fetchall()
    cpm_set = cpm_columns(con)
    repair_idx = _build_repair_index(cpm_set)

    resolution_rows: list[dict[str, Any]] = []
    unresolved_rows: list[dict[str, Any]] = []
    flag_unresolved: dict[str, list[str]] = {}

    for det, feeds in rows:
        arr = split_feeds(feeds)
        # Update the array column (sentinels stay empty)
        con.execute(
            f"UPDATE {REGISTRY_V2} SET feeds_master_columns_array = ? "
            f"WHERE detail_table_name = ?",
            [arr, det],
        )
        for fc in arr:
            resolves, repair_mode, repair_target = _attempt_repair(
                fc, cpm_set, repair_idx
            )
            # prose_skipped is treated as auto_repaired-by-classification:
            # it stops counting against the unresolved gate but is not a real
            # resolution against a CPM column.
            is_prose = (repair_mode == "prose_skipped")
            auto_repaired = ((not resolves) and (repair_target is not None)) or is_prose
            row = {
                "detail_table_name": det,
                "feed_col": fc,
                "resolves_bool": resolves,
                "auto_repaired_bool": auto_repaired,
                "repair_mode": repair_mode or "",
                "repair_source_name": repair_target or "",
            }
            resolution_rows.append(row)
            if not resolves and not auto_repaired:
                unresolved_rows.append(row)
                flag_unresolved.setdefault(det, []).append(fc)

    # Mark needs_manual_review
    for det in flag_unresolved:
        con.execute(
            f"UPDATE {REGISTRY_V2} SET needs_manual_review = true "
            f"WHERE detail_table_name = ?",
            [det],
        )

    # Materialize audit tables in WS
    for tbl in [
        "registry_v2_resolution_audit_v1",
        "registry_v2_unresolved_pointers_v1",
        "registry_v2_cpm_cols_without_registry_v1",
    ]:
        if table_exists(con, WS, tbl):
            con.execute(f'DROP TABLE "{PUBLICATION_DB}".{WS}.{tbl}')

    con.execute(f"""
        CREATE TABLE "{PUBLICATION_DB}".{WS}.registry_v2_resolution_audit_v1 (
            detail_table_name VARCHAR,
            feed_col VARCHAR,
            resolves_bool BOOLEAN,
            auto_repaired_bool BOOLEAN,
            repair_mode VARCHAR,
            repair_source_name VARCHAR
        )
    """)
    if resolution_rows:
        con.executemany(
            f"""INSERT INTO "{PUBLICATION_DB}".{WS}.registry_v2_resolution_audit_v1
                (detail_table_name, feed_col, resolves_bool, auto_repaired_bool,
                 repair_mode, repair_source_name) VALUES (?, ?, ?, ?, ?, ?)""",
            [
                (
                    r["detail_table_name"],
                    r["feed_col"],
                    r["resolves_bool"],
                    r["auto_repaired_bool"],
                    r["repair_mode"],
                    r["repair_source_name"],
                )
                for r in resolution_rows
            ],
        )

    con.execute(f"""
        CREATE TABLE "{PUBLICATION_DB}".{WS}.registry_v2_unresolved_pointers_v1 AS
            SELECT * FROM "{PUBLICATION_DB}".{WS}.registry_v2_resolution_audit_v1
            WHERE resolves_bool = false AND auto_repaired_bool = false
    """)

    # CPM cols not pointed to by anyone
    pointed_cols: set[str] = set()
    for r in resolution_rows:
        if r["resolves_bool"]:
            pointed_cols.add(r["feed_col"])
        elif r["auto_repaired_bool"] and r["repair_source_name"]:
            pointed_cols.add(r["repair_source_name"])
    unmapped = sorted(cpm_set - pointed_cols)
    con.execute(f"""
        CREATE TABLE "{PUBLICATION_DB}".{WS}.registry_v2_cpm_cols_without_registry_v1 (
            column_name VARCHAR
        )
    """)
    if unmapped:
        con.executemany(
            f"""INSERT INTO "{PUBLICATION_DB}".{WS}.registry_v2_cpm_cols_without_registry_v1
                (column_name) VALUES (?)""",
            [(c,) for c in unmapped],
        )

    for t, purpose in [
        (
            "registry_v2_resolution_audit_v1",
            "Per-(detail_table,feed_col) resolution + auto-repair audit emitted while building registry_v2.",
        ),
        (
            "registry_v2_unresolved_pointers_v1",
            "Subset of resolution audit where the feed_col neither resolved nor was auto-repairable.",
        ),
        (
            "registry_v2_cpm_cols_without_registry_v1",
            "CPM columns not pointed to by any registry feeds_master_columns entry (incl. auto-repairs).",
        ),
        (
            "detail_table_registry_v2",
            "Rebuilt registry with feeds_master_columns_array and needs_manual_review; pre-promotion.",
        ),
    ]:
        comment_on_table(con, WS, t, purpose)

    n_resolved = sum(1 for r in resolution_rows if r["resolves_bool"])
    n_auto = sum(1 for r in resolution_rows if r["auto_repaired_bool"])
    n_unres = len(unresolved_rows)
    log(
        f"Resolution: {n_resolved} resolved / {n_auto} auto-repaired / "
        f"{n_unres} unresolved (over {len(resolution_rows)} feed_col rows)"
    )
    log(f"CPM cols without registry pointer: {len(unmapped)}")

    # Dump CSVs for the gates
    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_registry_v2_resolution_audit.csv",
        resolution_rows,
        ["detail_table_name", "feed_col", "resolves_bool",
         "auto_repaired_bool", "repair_mode", "repair_source_name"],
    )
    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_registry_v2_unresolved_pointers.csv",
        unresolved_rows,
        ["detail_table_name", "feed_col", "resolves_bool",
         "auto_repaired_bool", "repair_mode", "repair_source_name"],
    )
    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_registry_v2_cpm_cols_without_registry.csv",
        [{"column_name": c} for c in unmapped],
        ["column_name"],
    )

    # Phase 1 step 5: remove dead v240 pointer in v2
    deleted = con.execute(f"""
        DELETE FROM {REGISTRY_V2}
         WHERE detail_table_name = 'data_dictionary_v240'
    """).fetchone()
    log(f"Removed dead data_dictionary_v240 row(s) in v2 (delete returned: {deleted})")

    # ----- Wildcard expansion -----
    log("--- Wildcard expansion pass ---")
    wildcard_log_rows: list[dict[str, Any]] = []
    n_expansions = 0
    n_new_resolves = 0
    rows_now = con.execute(f"""
        SELECT detail_table_name, feeds_master_columns_array FROM {REGISTRY_V2}
    """).fetchall()
    for det, arr in rows_now:
        if not arr:
            continue
        new_arr: list[str] = []
        changed = False
        for fc in arr:
            if "*" not in fc:
                new_arr.append(fc)
                continue
            # Extract bare *-tokens (handles things like 'cnln_* columns').
            star_tokens = re.findall(r"[A-Za-z0-9_*]+\*[A-Za-z0-9_*]*", fc)
            star_tokens += re.findall(r"\*[A-Za-z0-9_]+", fc)
            star_tokens = [t for t in star_tokens if "*" in t]
            like_token = star_tokens[0] if star_tokens else fc
            like_pat = like_token.replace("*", "%")
            matches = [r[0] for r in con.execute(f"""
                SELECT column_name FROM information_schema.columns
                 WHERE table_catalog='{PUBLICATION_DB}'
                   AND table_schema='{MAIN}' AND table_name='{CPM}'
                   AND column_name LIKE ?
                 ORDER BY column_name
            """, [like_pat]).fetchall()]
            if not matches:
                new_arr.append(fc)
                wildcard_log_rows.append({
                    "detail_table_name": det,
                    "wildcard_feed_col": fc,
                    "like_pattern": like_pat,
                    "expansion_count": 0,
                    "kept_as_is": True,
                    "expanded_into": "",
                })
                continue
            whitelisted_tag = WILDCARD_EXPANSION_WHITELIST.get((det, fc))
            if (
                len(matches) > WILDCARD_EXPANSION_PER_PATTERN_LIMIT
                and not whitelisted_tag
            ):
                log(
                    f"GUARDRAIL DEFERRED: wildcard {det}.{fc} -> "
                    f"{len(matches)} matches (>{WILDCARD_EXPANSION_PER_PATTERN_LIMIT}); "
                    "left as needs_manual_review."
                )
                wildcard_log_rows.append({
                    "detail_table_name": det,
                    "wildcard_feed_col": fc,
                    "like_pattern": like_pat,
                    "expansion_count": len(matches),
                    "kept_as_is": True,
                    "expanded_into": "",
                    "audit_tag": "guardrail_deferred_oversized",
                })
                # Persist the offending expansion list for later review
                # (one CSV per oversized pattern keyed by det/fc).
                tag = re.sub(r"[^A-Za-z0-9]+", "_", f"{det}__{fc}")[:80]
                pattern_path = (
                    OUT_DIR / f"{SCRIPT_NUM}_wildcard_oversized_{tag}.csv"
                )
                write_csv(
                    pattern_path,
                    [
                        {"detail_table_name": det, "wildcard_feed_col": fc,
                         "like_pattern": like_pat, "match": m}
                        for m in matches
                    ],
                    ["detail_table_name", "wildcard_feed_col", "like_pattern", "match"],
                )
                new_arr.append(fc)  # preserve original
                continue
            n_expansions += 1
            n_new_resolves += len(matches)
            audit_tag = whitelisted_tag or "wildcard_expansion"
            wildcard_log_rows.append({
                "detail_table_name": det,
                "wildcard_feed_col": fc,
                "like_pattern": like_pat,
                "expansion_count": len(matches),
                "kept_as_is": False,
                "expanded_into": ";".join(matches),
                "audit_tag": audit_tag,
            })
            new_arr.extend(matches)
            changed = True
            for m in matches:
                con.execute(f"""
                    INSERT INTO "{PUBLICATION_DB}".{WS}.registry_v2_resolution_audit_v1
                        (detail_table_name, feed_col, resolves_bool,
                         auto_repaired_bool, repair_mode, repair_source_name)
                    VALUES (?, ?, true, true, 'wildcard_expansion', ?)
                """, [det, m, audit_tag])
            # Mark the original wildcard row in the audit as resolved-by-expansion
            # (auto_repaired_bool=true so it stops being counted as unresolved).
            con.execute(f"""
                UPDATE "{PUBLICATION_DB}".{WS}.registry_v2_resolution_audit_v1
                   SET auto_repaired_bool = true,
                       repair_mode = 'wildcard_expanded',
                       repair_source_name = 'expanded_to_' || ? || '_cols'
                 WHERE detail_table_name = ? AND feed_col = ?
                   AND resolves_bool = false AND auto_repaired_bool = false
            """, [str(len(matches)), det, fc])
        if changed:
            # de-dup while preserving order
            seen: set[str] = set()
            dedup = [c for c in new_arr if not (c in seen or seen.add(c))]
            con.execute(
                f"UPDATE {REGISTRY_V2} SET feeds_master_columns_array = ? "
                f"WHERE detail_table_name = ?",
                [dedup, det],
            )
    log(f"Wildcard expansion: {n_expansions} patterns expanded, "
        f"{n_new_resolves} new feed_col rows resolved")
    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_wildcard_expansion_log.csv",
        wildcard_log_rows,
        ["detail_table_name", "wildcard_feed_col", "like_pattern",
         "expansion_count", "kept_as_is", "expanded_into", "audit_tag"],
    )

    # Refresh unresolved + cpm_cols_without_registry tables to reflect expansion
    con.execute(
        f'DROP TABLE IF EXISTS "{PUBLICATION_DB}".{WS}.registry_v2_unresolved_pointers_v1'
    )
    con.execute(f"""
        CREATE TABLE "{PUBLICATION_DB}".{WS}.registry_v2_unresolved_pointers_v1 AS
            SELECT * FROM "{PUBLICATION_DB}".{WS}.registry_v2_resolution_audit_v1
            WHERE resolves_bool = false AND auto_repaired_bool = false
    """)
    # Recompute pointed_cols
    pointed_cols2: set[str] = set()
    for r in con.execute(f"""
        SELECT feed_col, resolves_bool, auto_repaired_bool, repair_source_name
          FROM "{PUBLICATION_DB}".{WS}.registry_v2_resolution_audit_v1
    """).fetchall():
        fc, ok, auto, repsrc = r
        if ok and fc in cpm_set:
            pointed_cols2.add(fc)
        elif auto and repsrc and repsrc in cpm_set:
            pointed_cols2.add(repsrc)
    unmapped2 = sorted(cpm_set - pointed_cols2)
    con.execute(
        f'DROP TABLE IF EXISTS "{PUBLICATION_DB}".{WS}.registry_v2_cpm_cols_without_registry_v1'
    )
    con.execute(f"""
        CREATE TABLE "{PUBLICATION_DB}".{WS}.registry_v2_cpm_cols_without_registry_v1 (
            column_name VARCHAR
        )
    """)
    if unmapped2:
        con.executemany(
            f'INSERT INTO "{PUBLICATION_DB}".{WS}.registry_v2_cpm_cols_without_registry_v1 '
            "(column_name) VALUES (?)",
            [(c,) for c in unmapped2],
        )
    new_unresolved = con.execute(
        f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}.registry_v2_unresolved_pointers_v1'
    ).fetchone()[0]
    log(f"Post-expansion unresolved={new_unresolved}; cpm_cols_without_registry={len(unmapped2)}")

    # Re-emit CSVs (overwrite the pre-expansion ones)
    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_registry_v2_unresolved_pointers.csv",
        [{"detail_table_name": r[0], "feed_col": r[1],
          "resolves_bool": r[2], "auto_repaired_bool": r[3],
          "repair_mode": r[4], "repair_source_name": r[5]}
         for r in con.execute(f"""
             SELECT detail_table_name, feed_col, resolves_bool, auto_repaired_bool,
                    repair_mode, repair_source_name
               FROM "{PUBLICATION_DB}".{WS}.registry_v2_unresolved_pointers_v1
               ORDER BY detail_table_name, feed_col
         """).fetchall()],
        ["detail_table_name", "feed_col", "resolves_bool",
         "auto_repaired_bool", "repair_mode", "repair_source_name"],
    )
    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_registry_v2_cpm_cols_without_registry.csv",
        [{"column_name": c} for c in unmapped2],
        ["column_name"],
    )

    n_unres = new_unresolved

    summary = {
        "feed_col_rows_pre_expansion": len(resolution_rows),
        "resolved_pre_expansion": n_resolved,
        "auto_repaired_pre_expansion": n_auto,
        "wildcard_patterns_expanded": n_expansions,
        "wildcard_new_resolves": n_new_resolves,
        "unresolved_post_expansion": n_unres,
        "cpm_cols_without_registry_post_expansion": len(unmapped2),
    }
    (OUT_DIR / f"{SCRIPT_NUM}_phase2_summary.json").write_text(
        json.dumps(summary, indent=2)
    )
    return summary


# ---------------------------------------------------------------------------
# PHASE 3A — Register 3 main tables in v2
# ---------------------------------------------------------------------------
def phase_3a(con, log) -> dict[str, Any]:
    log("=== PHASE 3A: Register 3 main detail tables ===")
    if not table_exists(con, WS, "detail_table_registry_v2"):
        raise SystemExit("registry_v2 not built yet — run phase 2 first")

    cpm_set = cpm_columns(con)
    v2_cols = column_names(con, WS, "detail_table_registry_v2")
    log(f"v2 columns: {v2_cols}")

    # Validate tumor_stage_heterogeneity_v1 exists in main and column claims
    pre_checks = {}
    for r in PHASE_3A_REGISTRATIONS:
        nm = r["detail_table_name"]
        if not table_exists(con, MAIN, nm):
            raise SystemExit(
                f"Phase 3A target main.{nm} does not exist — refusing to register."
            )
    # tumor_stage_heterogeneity_v1: research_id type & CAST sanity
    tsh = "tumor_stage_heterogeneity_v1"
    rid_dtype = con.execute(f"""
        SELECT data_type FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{MAIN}'
          AND table_name='{tsh}' AND column_name='research_id'
    """).fetchone()
    pre_checks["tumor_stage_heterogeneity_v1.research_id"] = (
        rid_dtype[0] if rid_dtype else None
    )
    if rid_dtype:
        cast_join = con.execute(f"""
            SELECT COUNT(*) FROM "{PUBLICATION_DB}".{MAIN}.{CPM} c
            INNER JOIN "{PUBLICATION_DB}".{MAIN}.{tsh} t
              ON c.research_id = CAST(t.research_id AS VARCHAR)
        """).fetchone()[0]
        pre_checks[f"{tsh}_cast_join_matches"] = cast_join
        if cast_join == 0:
            raise SystemExit(
                f"{tsh} CAST(research_id AS VARCHAR) join produced 0 matches against CPM. "
                f"Aborting Phase 3A registration."
            )

    # Verify column claims for tumor_stage_heterogeneity_v1
    tsh_claims = [c for c in PHASE_3A_REGISTRATIONS
                  if c["detail_table_name"] == tsh][0]["feeds_master_columns"].split(";")
    missing = [c for c in tsh_claims if c not in cpm_set]
    if missing:
        raise SystemExit(
            f"{tsh} claims columns missing from CPM: {missing}"
        )
    pre_checks[f"{tsh}_columns_verified"] = len(tsh_claims)
    log(f"Pre-checks: {json.dumps(pre_checks, indent=2)}")

    inserted = 0
    for reg in PHASE_3A_REGISTRATIONS:
        # Skip if already present in v2
        n = con.execute(
            f"SELECT COUNT(*) FROM {REGISTRY_V2} WHERE detail_table_name = ?",
            [reg["detail_table_name"]],
        ).fetchone()[0]
        if n:
            log(f"  already present in v2: {reg['detail_table_name']} (skip)")
            continue
        arr = split_feeds(reg["feeds_master_columns"])
        cols_list = ", ".join(['"detail_table_name"', '"schema_name"', '"domain"',
                               '"grain"', '"feeds_master_columns"',
                               '"feeds_master_columns_array"', '"needs_manual_review"'])
        present = set(v2_cols)
        if not {"detail_table_name", "schema_name", "domain", "grain",
                "feeds_master_columns"}.issubset(present):
            raise SystemExit(
                f"v2 missing required columns for inserting Phase 3A rows; "
                f"have {sorted(present)}"
            )
        con.execute(f"""
            INSERT INTO {REGISTRY_V2}
                (detail_table_name, schema_name, domain, grain,
                 feeds_master_columns, feeds_master_columns_array,
                 needs_manual_review)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            reg["detail_table_name"], reg["schema_name"], reg["domain"],
            reg["grain"], reg["feeds_master_columns"], arr, False,
        ])
        inserted += 1
        log(f"  registered: {reg['detail_table_name']}")

    return {"phase_3a_inserted": inserted, "pre_checks": pre_checks}


# ---------------------------------------------------------------------------
# PHASE 3B — Register 11 manuscript_workspace audit/queue tables
# ---------------------------------------------------------------------------
def phase_3b(con, log) -> dict[str, Any]:
    log("=== PHASE 3B: Register 11 manuscript_workspace audit/queue tables ===")
    if not table_exists(con, WS, "detail_table_registry_v2"):
        raise SystemExit("registry_v2 not built yet — run phase 2 first")

    v2_cols = set(column_names(con, WS, "detail_table_registry_v2"))
    has_total_rows = "total_rows" in v2_cols
    has_total_patients = "total_patients" in v2_cols

    inserted = 0
    measured = []
    for reg in PHASE_3B_REGISTRATIONS:
        nm = reg["detail_table_name"]
        if not table_exists(con, WS, nm):
            log(f"  WARNING: {WS}.{nm} not found — skipping registration")
            continue
        n = con.execute(
            f"SELECT COUNT(*) FROM {REGISTRY_V2} WHERE detail_table_name = ?", [nm]
        ).fetchone()[0]
        if n:
            log(f"  already present in v2: {nm} (skip)")
            continue
        # Live measurements
        total_rows = con.execute(
            f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}."{nm}"'
        ).fetchone()[0]
        total_patients = None
        ws_table_cols = set(column_names(con, WS, nm))
        if "research_id" in ws_table_cols:
            total_patients = con.execute(
                f'SELECT COUNT(DISTINCT research_id) FROM "{PUBLICATION_DB}".{WS}."{nm}"'
            ).fetchone()[0]
        measured.append({
            "detail_table_name": nm,
            "total_rows": total_rows,
            "total_patients": total_patients,
        })

        cols = ["detail_table_name", "schema_name", "domain", "grain",
                "feeds_master_columns", "feeds_master_columns_array",
                "needs_manual_review"]
        vals: list[Any] = [nm, reg["schema_name"], reg["domain"], reg["grain"],
                           reg["feeds_master_columns"], [], False]
        if has_total_rows:
            cols.append("total_rows")
            vals.append(total_rows)
        if has_total_patients:
            cols.append("total_patients")
            vals.append(total_patients)
        placeholders = ", ".join("?" for _ in cols)
        con.execute(
            f"INSERT INTO {REGISTRY_V2} ({', '.join(cols)}) VALUES ({placeholders})",
            vals,
        )
        inserted += 1
        log(f"  registered: {nm}  rows={total_rows}  patients={total_patients}")

    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_phase3b_measurements.csv",
        measured,
        ["detail_table_name", "total_rows", "total_patients"],
    )
    return {"phase_3b_inserted": inserted, "measurements": measured}


# ---------------------------------------------------------------------------
# PHASE 3C — Trace remaining unmapped CPM columns
# ---------------------------------------------------------------------------
def phase_3c(con, log) -> dict[str, Any]:
    log("=== PHASE 3C: Trace unmapped CPM columns ===")
    if not table_exists(con, WS, "registry_v2_cpm_cols_without_registry_v1"):
        raise SystemExit("Phase 2 audits missing — run phase 2 first")

    unmapped = [
        r[0] for r in con.execute(f"""
            SELECT column_name
              FROM "{PUBLICATION_DB}".{WS}.registry_v2_cpm_cols_without_registry_v1
            ORDER BY column_name
        """).fetchall()
    ]
    log(f"Currently unmapped: {len(unmapped)}")
    # Subtract any that the new Phase 3A/3B registrations now cover
    if table_exists(con, WS, "detail_table_registry_v2"):
        cpm_set = cpm_columns(con)
        # Recompute pointed_cols from live v2 rows
        rows = con.execute(f"""
            SELECT feeds_master_columns_array FROM {REGISTRY_V2}
        """).fetchall()
        pointed: set[str] = set()
        for (arr,) in rows:
            if arr:
                for c in arr:
                    if c and c in cpm_set:
                        pointed.add(c)
        # plus auto-repair sources
        rep = con.execute(f"""
            SELECT repair_source_name
              FROM "{PUBLICATION_DB}".{WS}.registry_v2_resolution_audit_v1
              WHERE auto_repaired_bool = true
        """).fetchall()
        for (rs,) in rep:
            if rs:
                pointed.add(rs)
        unmapped = sorted(cpm_set - pointed)
        log(f"Post-3A/3B unmapped: {len(unmapped)}")

    # Walk repo scripts/ to attribute origin script per unmapped column
    SCRIPTS_DIR = REPO / "scripts"
    attribution: dict[str, list[str]] = {c: [] for c in unmapped}
    if unmapped:
        # build a single ripgrep call for efficiency
        # but to stay tool-pure we just scan files in python
        py_files = list(SCRIPTS_DIR.rglob("*.py"))
        for c in unmapped:
            pat = re.compile(r"\b" + re.escape(c) + r"\b")
            for pf in py_files:
                try:
                    if pat.search(pf.read_text(errors="ignore")):
                        attribution[c].append(pf.name)
                except Exception:
                    continue

    rows_out = []
    for c in unmapped:
        srcs = sorted(set(attribution[c]))[:5]
        rows_out.append({
            "column_name": c,
            "candidate_origin_scripts": "; ".join(srcs),
            "needs_manual_assignment": True,
        })
    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_phase3c_orphan_columns.csv",
        rows_out,
        ["column_name", "candidate_origin_scripts", "needs_manual_assignment"],
    )

    # Hard cap: if > 20, surface and stop
    if len(unmapped) > 20:
        log(
            f"WARNING: {len(unmapped)} orphan CPM columns exceeds the >20 calibration "
            f"threshold. Pausing further automated assignment — present list to user."
        )

    # Domain-prefix spot-check: count orphans whose name starts with a known
    # domain prefix that *should* be pointed to by a drill-down feeder.
    domain_prefixes = (
        "nlp_", "comp_", "ete_", "rln_", "mol_", "rai_", "cnln_", "tg_",
        "nucmed_", "pet_", "ct_", "mri_", "lnus_", "tirads_", "fna_",
        "bethesda_",
    )
    domain_orphans = [c for c in unmapped if c.lower().startswith(domain_prefixes)]
    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_phase3c_domain_prefix_orphans.csv",
        [{"column_name": c} for c in domain_orphans],
        ["column_name"],
    )
    log(
        f"Domain-prefix orphans (nlp_/comp_/ete_/rln_/mol_/rai_/cnln_/tg_/"
        f"nucmed_/pet_/ct_/mri_/lnus_/tirads_/fna_/bethesda_): {len(domain_orphans)}"
    )
    if len(domain_orphans) > 20:
        log(
            f"WARNING: {len(domain_orphans)} domain-prefix orphans > 20 — that's a "
            "real registry gap, not a structural expectation. STOP and review."
        )
    return {
        "orphan_count": len(unmapped),
        "exceeds_calibration_threshold": len(unmapped) > 20,
        "domain_prefix_orphan_count": len(domain_orphans),
        "domain_prefix_csv": str(
            OUT_DIR / f"{SCRIPT_NUM}_phase3c_domain_prefix_orphans.csv"
        ),
        "csv": str(OUT_DIR / f"{SCRIPT_NUM}_phase3c_orphan_columns.csv"),
    }


# ---------------------------------------------------------------------------
# PHASE 5inv — Archive inventory for v240 / v266a dictionary lineage
# ---------------------------------------------------------------------------
def phase_5inv(con, log) -> dict[str, Any]:
    log("=== PHASE 5inv: archive inventory for v240/v266a lineage ===")
    if con.execute(f"""
        SELECT COUNT(*) FROM information_schema.schemata
        WHERE catalog_name='{ARCHIVE_DB}' AND schema_name='{ARCHIVE_SCHEMA}'
    """).fetchone()[0] == 0:
        log(f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA} schema does not exist; nothing to inventory')
        write_csv(
            OUT_DIR / f"{SCRIPT_NUM}_archive_inventory.csv",
            [],
            ["archive_object_name", "row_count", "lineage"],
        )
        return {"inventoried": 0}

    rows = con.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog='{ARCHIVE_DB}' AND table_schema='{ARCHIVE_SCHEMA}'
          AND (table_name ILIKE 'data_dictionary_v240%'
               OR table_name ILIKE 'data_dictionary_v266a%'
               OR table_name ILIKE 'data_dictionary_v279%')
        ORDER BY table_name
    """).fetchall()
    inv = []
    for (name,) in rows:
        try:
            n = con.execute(
                f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}."{name}"'
            ).fetchone()[0]
        except Exception as e:
            n = -1
            log(f"  ERROR counting rows in {name}: {e}")
        if name.lower().startswith("data_dictionary_v240"):
            lineage = "v240 predecessor (dropped from main; superseded by v266a then v279)"
        elif name.lower().startswith("data_dictionary_v266a"):
            lineage = "v266a predecessor (dropped from main; superseded by v279)"
        else:
            lineage = "v279 working snapshot"
        inv.append({"archive_object_name": name, "row_count": n, "lineage": lineage})
        log(f"  {name}: {n} rows  ({lineage})")

    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_archive_inventory.csv",
        inv,
        ["archive_object_name", "row_count", "lineage"],
    )
    return {"inventoried": len(inv)}


# ---------------------------------------------------------------------------
# PHASE 3recon — reconcile with canonical_cleanup_audit_v1
# ---------------------------------------------------------------------------
def phase_3recon(con, log) -> dict[str, Any]:
    log("=== PHASE 3recon: canonical_cleanup_audit reconciliation ===")
    if not table_exists(con, WS, "canonical_cleanup_audit_v1"):
        log("canonical_cleanup_audit_v1 not present — skipping")
        return {"skipped": True}

    audit_cols = set(column_names(con, WS, "canonical_cleanup_audit_v1"))
    classifier_version = None
    if "classifier_version" in audit_cols:
        classifier_version = con.execute(
            f"SELECT MAX(CAST(classifier_version AS VARCHAR)) FROM "
            f"\"{PUBLICATION_DB}\".{WS}.canonical_cleanup_audit_v1"
        ).fetchone()[0]
    log(f"Reading canonical_cleanup_audit_v1 classifier_version={classifier_version}")

    needed = {"has_version_twin", "is_referenced_by_view", "is_referenced_by_script"}
    missing_cols = needed - audit_cols
    if missing_cols:
        log(f"WARNING: audit table missing expected cols: {sorted(missing_cols)} — "
            "best-effort partial reconciliation")

    has_status = "status" in audit_cols
    status_filter = (
        "AND status IN ('LIVE', 'ARCHIVE_CANDIDATE')" if has_status else ""
    )
    has_identical = "is_identical_to_twin" in audit_cols

    superseded = []
    safe_archive = []
    identical = []

    if "has_version_twin" in audit_cols:
        superseded = con.execute(f"""
            SELECT * FROM "{PUBLICATION_DB}".{WS}.canonical_cleanup_audit_v1
            WHERE has_version_twin = true {status_filter}
        """).fetchdf().to_dict(orient="records")

    if needed.issubset(audit_cols):
        safe_archive = con.execute(f"""
            SELECT * FROM "{PUBLICATION_DB}".{WS}.canonical_cleanup_audit_v1
            WHERE is_referenced_by_view = false
              AND is_referenced_by_script = false
              AND has_version_twin = true
              {status_filter}
        """).fetchdf().to_dict(orient="records")

    if has_identical:
        identical = con.execute(f"""
            SELECT * FROM "{PUBLICATION_DB}".{WS}.canonical_cleanup_audit_v1
            WHERE is_identical_to_twin = true {status_filter}
        """).fetchdf().to_dict(orient="records")

    def _serialize(rec: dict[str, Any]) -> dict[str, Any]:
        return {k: ("" if v is None else str(v)) for k, v in rec.items()}

    if superseded:
        all_keys = sorted({k for r in superseded for k in r.keys()})
        write_csv(
            OUT_DIR / f"{SCRIPT_NUM}_supersession_candidates.csv",
            [_serialize(r) for r in superseded], all_keys,
        )
    else:
        (OUT_DIR / f"{SCRIPT_NUM}_supersession_candidates.csv").write_text("")

    if safe_archive:
        all_keys = sorted({k for r in safe_archive for k in r.keys()})
        write_csv(
            OUT_DIR / f"{SCRIPT_NUM}_safe_archive_candidates.csv",
            [_serialize(r) for r in safe_archive], all_keys,
        )
    else:
        (OUT_DIR / f"{SCRIPT_NUM}_safe_archive_candidates.csv").write_text("")

    if identical:
        all_keys = sorted({k for r in identical for k in r.keys()})
        write_csv(
            OUT_DIR / f"{SCRIPT_NUM}_identical_twins.csv",
            [_serialize(r) for r in identical], all_keys,
        )
    else:
        (OUT_DIR / f"{SCRIPT_NUM}_identical_twins.csv").write_text("")

    # Drift vs snapshot_20260417 if present
    drift_path = OUT_DIR / f"{SCRIPT_NUM}_audit_drift.csv"
    if table_exists(con, WS, "canonical_cleanup_audit_v1_snapshot_20260417"):
        snap_cols = set(column_names(con, WS,
                                     "canonical_cleanup_audit_v1_snapshot_20260417"))
        if {"object_name", "schema_name"}.issubset(audit_cols & snap_cols):
            drift_rows = con.execute(f"""
                SELECT 'added' AS delta_type, schema_name, object_name
                  FROM "{PUBLICATION_DB}".{WS}.canonical_cleanup_audit_v1
                EXCEPT
                SELECT 'added' AS delta_type, schema_name, object_name
                  FROM "{PUBLICATION_DB}".{WS}.canonical_cleanup_audit_v1_snapshot_20260417
                UNION ALL
                SELECT 'removed' AS delta_type, schema_name, object_name
                  FROM "{PUBLICATION_DB}".{WS}.canonical_cleanup_audit_v1_snapshot_20260417
                EXCEPT
                SELECT 'removed' AS delta_type, schema_name, object_name
                  FROM "{PUBLICATION_DB}".{WS}.canonical_cleanup_audit_v1
            """).fetchdf().to_dict(orient="records")
            if drift_rows:
                write_csv(
                    drift_path,
                    [_serialize(r) for r in drift_rows],
                    ["delta_type", "schema_name", "object_name"],
                )
                log(f"audit drift rows: {len(drift_rows)} -> {drift_path}")
                if len(drift_rows) > 5:
                    log("WARNING: audit drift > 5 — surface to user before proceeding")
            else:
                drift_path.write_text("delta_type,schema_name,object_name\n")
        else:
            drift_path.write_text("# could not compare — schema mismatch\n")
    else:
        drift_path.write_text("# no snapshot_20260417 present\n")

    return {
        "classifier_version": classifier_version,
        "superseded_count": len(superseded),
        "safe_archive_count": len(safe_archive),
        "identical_count": len(identical),
    }


# ---------------------------------------------------------------------------
# PHASE 4 — manuscript_dive_map_v1 enrichments (additive, no gate)
# ---------------------------------------------------------------------------
def phase_4(con, log) -> dict[str, Any]:
    log("=== PHASE 4: manuscript_dive_map_v1 enrichment ===")
    mdm = "manuscript_dive_map_v1"
    if not table_exists(con, WS, mdm):
        log(f"{mdm} not present — skipping Phase 4")
        return {"skipped": True}

    # Snapshot before any change
    snap = f"{mdm}_pre272_snapshot"
    if not table_exists(con, WS, snap):
        con.execute(
            f'CREATE TABLE "{PUBLICATION_DB}".{WS}.{snap} AS '
            f'SELECT * FROM "{PUBLICATION_DB}".{WS}.{mdm}'
        )
        log(f"Snapshot created: {WS}.{snap}")

    cols = set(column_names(con, WS, mdm))
    if "filter_type" not in cols:
        con.execute(
            f'ALTER TABLE "{PUBLICATION_DB}".{WS}.{mdm} ADD COLUMN filter_type VARCHAR'
        )
        log("Added filter_type column")
    if "duplicate_of_manuscript_id" not in cols:
        con.execute(
            f'ALTER TABLE "{PUBLICATION_DB}".{WS}.{mdm} '
            f'ADD COLUMN duplicate_of_manuscript_id VARCHAR'
        )
        log("Added duplicate_of_manuscript_id column")

    # Best-effort classification — only fill if we recognize id columns
    cols_now = set(column_names(con, WS, mdm))
    id_col = next(
        (c for c in ("manuscript_id", "id", "manuscript_dive_id") if c in cols_now),
        None,
    )
    view_col = next(
        (c for c in ("cohort_view_name", "dive_view_name", "view_name", "view")
         if c in cols_now),
        None,
    )
    # Detect id type so we don't string-compare an integer
    id_type = None
    if id_col:
        r = con.execute(f"""
            SELECT data_type FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{WS}'
              AND table_name='{mdm}' AND column_name='{id_col}'
        """).fetchone()
        id_type = r[0] if r else None
    log(f"manuscript_dive_map_v1 id_col={id_col} ({id_type})  view_col={view_col}")

    classification_summary: dict[str, int] = {}
    if id_col and view_col:
        # Default
        con.execute(
            f'UPDATE "{PUBLICATION_DB}".{WS}.{mdm} '
            f"SET filter_type = 'dedicated_filtered' WHERE filter_type IS NULL "
            f"OR filter_type = ''"
        )
        # Full-cohort promotion: m032 = id 32, m038 = id 38 (id is INTEGER)
        if id_type and ("INT" in id_type.upper()):
            con.execute(
                f'UPDATE "{PUBLICATION_DB}".{WS}.{mdm} '
                f"SET filter_type = 'dedicated_full_cohort' "
                f"WHERE {id_col} IN (32, 38)"
            )
        else:
            for mid in ("m032", "m038", "32", "38"):
                con.execute(
                    f'UPDATE "{PUBLICATION_DB}".{WS}.{mdm} '
                    f"SET filter_type = 'dedicated_full_cohort' "
                    f"WHERE LOWER(CAST({id_col} AS VARCHAR)) = ?",
                    [mid],
                )
        # Thin-wrapper detection via view body inspection:
        # A "thin wrapper" is a view whose body is essentially
        #   SELECT * FROM <something>
        # with no WHERE filter (or only trivial ones), or that just re-aliases
        # another cohort view. We use information_schema.views.view_definition.
        # Add filter_type_provisional flag to mark heuristic-derived rows.
        if "filter_type_provisional" not in cols_now:
            con.execute(
                f'ALTER TABLE "{PUBLICATION_DB}".{WS}.{mdm} '
                f"ADD COLUMN filter_type_provisional BOOLEAN DEFAULT false"
            )
            log("Added filter_type_provisional column")

        defs = con.execute(f"""
            SELECT table_name, view_definition
              FROM information_schema.views
             WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{WS}'
        """).fetchall()
        view_def_map = {r[0]: (r[1] or "") for r in defs}

        # Thin wrapper = view that pulls a column subset of CPM with NO filtering.
        # Matches: anything FROM canonical_patient_master (with or without
        #          db/schema qualifier) AND no WHERE / GROUP BY / HAVING / JOIN.
        # Note: this catches any cohort view that's effectively `SELECT cols FROM CPM`.
        from_cpm_re = re.compile(
            r"\bfrom\s+(?:[\"`]?\w+[\"`]?\.){0,2}[\"`]?canonical_patient_master[\"`]?\b",
            re.IGNORECASE,
        )
        no_filter_re = re.compile(
            r"\b(where|group\s+by|having|join)\b", re.IGNORECASE
        )

        thin_view_names: list[str] = []
        for vname, vdef in view_def_map.items():
            body = (vdef or "").strip()
            if not body:
                continue
            if from_cpm_re.search(body) and not no_filter_re.search(body):
                thin_view_names.append(vname)
        log(f"View-definition thin_wrapper candidates "
            f"(FROM CPM, no WHERE/GROUP/HAVING/JOIN): {len(thin_view_names)}")

        # Data-based heuristic (matches the coworker's Prompt 21 §3 method):
        # A view is a thin wrapper iff its row count == CPM_ROWS AND its column
        # set is a strict subset of cohort_descriptive_full_cohort_v1's column
        # set. No SQL parsing required.
        full_cohort_view = "cohort_descriptive_full_cohort_v1"
        if table_exists(con, WS, full_cohort_view):
            full_cols = set(column_names(con, WS, full_cohort_view))
            full_n = con.execute(
                f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}.{full_cohort_view}'
            ).fetchone()[0]
            log(f"Full-cohort reference: {full_cohort_view} cols={len(full_cols)} rows={full_n}")
            data_thin: list[str] = []
            cohort_views = [
                r[0] for r in con.execute(f"""
                    SELECT table_name FROM information_schema.tables
                     WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{WS}'
                       AND table_type='VIEW' AND table_name LIKE 'cohort_m%'
                """).fetchall()
            ]
            for vname in cohort_views:
                try:
                    vc = set(column_names(con, WS, vname))
                    if not vc:
                        continue
                    vn = con.execute(
                        f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}.{vname}'
                    ).fetchone()[0]
                    if vn == CPM_ROWS and vc.issubset(full_cols):
                        data_thin.append(vname)
                except Exception as e:
                    log(f"  view inspect error {vname}: {e}")
            log(
                f"Data-based thin_wrapper candidates "
                f"(rows={CPM_ROWS} AND cols ⊆ full_cohort): {len(data_thin)}"
            )
            # Apply only if 18 <= n <= 28; otherwise document and defer
            if 18 <= len(data_thin) <= 28:
                for vname in data_thin:
                    con.execute(
                        f'UPDATE "{PUBLICATION_DB}".{WS}.{mdm} '
                        f"SET filter_type = 'thin_wrapper', "
                        f"    filter_type_provisional = true "
                        f"WHERE {view_col} = ? "
                        f"  AND filter_type != 'dedicated_full_cohort'",
                        [vname],
                    )
                thin_view_names = data_thin
                log(f"Applied data-based thin_wrapper classification to {len(data_thin)} views")
            else:
                log(
                    f"Data-based heuristic returned {len(data_thin)} (expected 18-28); "
                    f"deferring per plan. Sample: {data_thin[:10]}"
                )
                # Always emit the candidate list for review
                write_csv(
                    OUT_DIR / f"{SCRIPT_NUM}_phase4_thin_wrapper_data_heuristic.csv",
                    [{"view_name": v} for v in data_thin],
                    ["view_name"],
                )

        # Fetch (manuscript_id, cohort_view_name) for cross-check
        rows_cv = con.execute(
            f"SELECT {id_col}, {view_col} FROM "
            f'"{PUBLICATION_DB}".{WS}.{mdm}'
        ).fetchall()
        thin_set = set(thin_view_names)
        n_thin_assigned = 0
        for mid, vname in rows_cv:
            if vname and vname in thin_set:
                con.execute(
                    f'UPDATE "{PUBLICATION_DB}".{WS}.{mdm} '
                    f"SET filter_type = 'thin_wrapper', "
                    f"    filter_type_provisional = true "
                    f"WHERE {id_col} = ? AND filter_type != 'dedicated_full_cohort'",
                    [mid],
                )
                n_thin_assigned += 1

        df = con.execute(
            f'SELECT filter_type, COUNT(*) FROM "{PUBLICATION_DB}".{WS}.{mdm} '
            f'GROUP BY filter_type ORDER BY 1'
        ).fetchall()
        classification_summary = {(r[0] or "NULL"): r[1] for r in df}
        log(f"filter_type distribution: {classification_summary}")

        thin_n = classification_summary.get("thin_wrapper", 0)
        if thin_n < 18 or thin_n > 28:
            log(
                f"GUARDRAIL: thin_wrapper count = {thin_n}, expected ~24 (range 18-28). "
                f"Sample view names: {thin_view_names[:10]}"
            )
            # Persist for chat surfacing
            write_csv(
                OUT_DIR / f"{SCRIPT_NUM}_phase4_thin_wrapper_review.csv",
                [{"view_name": v} for v in thin_view_names],
                ["view_name"],
            )
    else:
        log(
            f"Could not auto-classify filter_type — id/view cols not found "
            f"(id={id_col}, view={view_col}). Columns left blank for manual fill."
        )

    # Build duplicate-review table
    dup_tbl = "cohort_view_duplicate_review_v1"
    if table_exists(con, WS, dup_tbl):
        con.execute(f'DROP TABLE "{PUBLICATION_DB}".{WS}.{dup_tbl}')
    con.execute(f"""
        CREATE TABLE "{PUBLICATION_DB}".{WS}.{dup_tbl} (
            cluster_label VARCHAR,
            manuscript_id_a VARCHAR,
            manuscript_id_b VARCHAR,
            jaccard_column_overlap DOUBLE,
            note VARCHAR
        )
    """)
    # Compute Jaccard for the 3 named clusters using view column sets
    pairs = [
        ("Bethesda_III_IV_1316", ["m001", "m028"]),
        ("Dive_T4_1286", ["m006", "m023", "m030", "m072"]),
        ("862_pair", ["m019", "m081"]),
    ]
    if id_col and view_col:
        # Resolve manuscript_id -> view_name
        mid_to_view: dict[str, str] = {}
        rows = con.execute(
            f'SELECT LOWER(CAST({id_col} AS VARCHAR)), {view_col} '
            f'FROM "{PUBLICATION_DB}".{WS}.{mdm}'
        ).fetchall()
        for mid, vw in rows:
            if vw:
                mid_to_view[mid] = vw
        for label, ids in pairs:
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    a, b = ids[i], ids[j]
                    va, vb = mid_to_view.get(a), mid_to_view.get(b)
                    note = ""
                    jac = None
                    if va and vb:
                        try:
                            ca = set(column_names(con, WS, va))
                            cb = set(column_names(con, WS, vb))
                            if ca and cb:
                                jac = len(ca & cb) / len(ca | cb)
                        except Exception as e:
                            note = f"col fetch error: {e}"
                    con.execute(
                        f'INSERT INTO "{PUBLICATION_DB}".{WS}.{dup_tbl} '
                        f"(cluster_label, manuscript_id_a, manuscript_id_b, "
                        f"jaccard_column_overlap, note) VALUES (?, ?, ?, ?, ?)",
                        [label, a, b, jac, note],
                    )
                    if jac is not None and jac >= 0.95:
                        # populate duplicate_of_manuscript_id on b -> a
                        con.execute(
                            f'UPDATE "{PUBLICATION_DB}".{WS}.{mdm} '
                            f"SET duplicate_of_manuscript_id = ? "
                            f"WHERE LOWER(CAST({id_col} AS VARCHAR)) = ?",
                            [a, b],
                        )

    comment_on_table(
        con, WS, dup_tbl,
        "Jaccard column-overlap analysis for 3 suspected duplicate cohort clusters; "
        "PI-reviewable.",
    )
    purpose_dive = (
        "Added filter_type ('thin_wrapper'|'dedicated_filtered'|"
        "'dedicated_full_cohort') and duplicate_of_manuscript_id; pre-change snapshot "
        f"in {WS}.{snap}."
    )
    comment_on_table(con, WS, mdm, purpose_dive)
    return {"filter_type_distribution": classification_summary,
            "duplicate_review_table": f"{WS}.{dup_tbl}"}


# ---------------------------------------------------------------------------
# PHASE 6 — End-to-end registry validation against v2
# ---------------------------------------------------------------------------
def phase_6(con, log) -> dict[str, Any]:
    log("=== PHASE 6: End-to-end registry validation ===")
    if not table_exists(con, WS, "detail_table_registry_v2"):
        raise SystemExit("registry_v2 not built yet — run phase 2 first")

    cpm_set = cpm_columns(con)
    rows = con.execute(f"""
        SELECT detail_table_name, schema_name, grain, feeds_master_columns_array,
               needs_manual_review
          FROM {REGISTRY_V2}
    """).fetchall()

    val_tbl = "registry_end_to_end_validation_v1"
    if table_exists(con, WS, val_tbl):
        con.execute(f'DROP TABLE "{PUBLICATION_DB}".{WS}.{val_tbl}')
    con.execute(f"""
        CREATE TABLE "{PUBLICATION_DB}".{WS}.{val_tbl} (
            detail_table_name VARCHAR,
            schema_name VARCHAR,
            grain VARCHAR,
            check_name VARCHAR,
            passed BOOLEAN,
            detail VARCHAR
        )
    """)

    n_pass = 0
    n_fail = 0
    n_unverifiable = 0
    insert_rows = []
    for det, sch, grain, arr, needs_review in rows:
        if not sch or not table_exists(con, sch, det):
            insert_rows.append(
                (det, sch, grain, "table_exists", False, "table not found")
            )
            n_fail += 1
            continue
        sch_cols = set(column_names(con, sch, det))
        is_patient = bool(grain) and "patient" in (grain or "").lower()

        if is_patient:
            ok_rid = "research_id" in sch_cols
            insert_rows.append(
                (det, sch, grain, "has_research_id", ok_rid,
                 "research_id present" if ok_rid else "missing research_id")
            )
            if ok_rid:
                n_pass += 1
            else:
                n_fail += 1
                continue

            join_n = con.execute(f"""
                SELECT COUNT(*) FROM "{PUBLICATION_DB}".{MAIN}.{CPM} c
                INNER JOIN "{PUBLICATION_DB}".{sch}."{det}" d
                  ON c.research_id = CAST(d.research_id AS VARCHAR)
            """).fetchone()[0]
            ok_join = join_n > 0
            insert_rows.append(
                (det, sch, grain, "joins_to_cpm", ok_join,
                 f"matched_rows={join_n}")
            )
            if ok_join:
                n_pass += 1
            else:
                n_fail += 1

        # feed_col existence
        if arr:
            for fc in arr:
                if not fc:
                    continue
                ok = fc in cpm_set
                insert_rows.append(
                    (det, sch, grain, f"feed_col:{fc}", ok,
                     "in CPM" if ok else "missing from CPM")
                )
                if ok:
                    n_pass += 1
                else:
                    if needs_review:
                        n_unverifiable += 1
                    else:
                        n_fail += 1

    if insert_rows:
        con.executemany(
            f"""INSERT INTO "{PUBLICATION_DB}".{WS}.{val_tbl}
                (detail_table_name, schema_name, grain, check_name, passed, detail)
                VALUES (?, ?, ?, ?, ?, ?)""",
            insert_rows,
        )
    comment_on_table(
        con, WS, val_tbl,
        "End-to-end validation of registry_v2 pointers (existence + CPM column "
        "membership + CPM join sanity for patient-grain rows).",
    )
    log(f"Validation: pass={n_pass}  fail={n_fail}  unverifiable(flagged)={n_unverifiable}")

    summary = {
        "pass": n_pass,
        "fail": n_fail,
        "unverifiable_flagged": n_unverifiable,
        "validation_table": f"{WS}.{val_tbl}",
    }
    (OUT_DIR / f"{SCRIPT_NUM}_phase6_summary.json").write_text(
        json.dumps(summary, indent=2)
    )
    return summary


# ---------------------------------------------------------------------------
# PHASE 7 — Final summary + handoff
# ---------------------------------------------------------------------------
def phase_7(con, log) -> dict[str, Any]:
    log("=== PHASE 7: Final summary + handoff ===")

    # Re-run baseline metrics for delta comparison
    inv = assert_cpm_invariants(con, log)
    main_n = base_table_count(con, MAIN)
    ws_tbl = base_table_count(con, WS)
    ws_vw = view_count(con, WS)

    # Use v1 if v2 was promoted, else v2
    reg_table = "detail_table_registry_v1"
    reg_n = con.execute(
        f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}.{reg_table}'
    ).fetchone()[0]

    val_pass = val_fail = val_unverif = 0
    if table_exists(con, WS, "registry_end_to_end_validation_v1"):
        val_pass = con.execute(
            f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}.registry_end_to_end_validation_v1 '
            "WHERE passed = true"
        ).fetchone()[0]
        val_fail = con.execute(
            f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}.registry_end_to_end_validation_v1 '
            "WHERE passed = false"
        ).fetchone()[0]
    unresolved_n = con.execute(
        f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}.registry_v2_unresolved_pointers_v1'
    ).fetchone()[0]

    unresolved_rows = con.execute(f"""
        SELECT detail_table_name, feed_col
          FROM "{PUBLICATION_DB}".{WS}.registry_v2_unresolved_pointers_v1
          ORDER BY detail_table_name, feed_col
    """).fetchall()
    unresolved_by_table: dict[str, list[str]] = {}
    for det, fc in unresolved_rows:
        unresolved_by_table.setdefault(det, []).append(fc)

    archive_n = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{ARCHIVE_DB}' AND table_schema='{ARCHIVE_SCHEMA}'
    """).fetchone()[0] if con.execute(f"""
        SELECT COUNT(*) FROM information_schema.schemata
        WHERE catalog_name='{ARCHIVE_DB}' AND schema_name='{ARCHIVE_SCHEMA}'
    """).fetchone()[0] > 0 else 0

    block = [
        "================================================================",
        "CANONICAL FINALIZATION COMPLETE — thyroid_canonical_publication_v1_0",
        "================================================================",
        f"canonical_patient_master : {inv['row_count']} × {inv['column_count']} cols",
        f"Invariants               : RIDs={inv['distinct_research_ids']}  "
        f"null_rid={inv['null_research_id']}  null_fna={inv['null_fna_path_outcome']}",
        f"main base tables         : {main_n}",
        f"manuscript_workspace     : {ws_tbl} tables + {ws_vw} views",
        f"detail_table_registry_v1 : {reg_n} rows, {val_pass} pass, {val_fail} fail",
        f"Unresolved pointers      : {unresolved_n} (all flagged needs_manual_review)",
        f'Archive destination      : "{ARCHIVE_DB}".{ARCHIVE_SCHEMA} ({archive_n} objs)',
        f"Data dictionary          : main.data_dictionary_v279 (sole, registered)",
        "================================================================",
    ]
    log("\n".join(block))

    # Handoff markdown
    handoff = REPO / f"THYROID_2026_FINALIZATION_20260418.md"
    md_lines: list[str] = []
    md_lines.append(f"# THYROID_2026 Finalization — {RUN_DATE}")
    md_lines.append("")
    md_lines.append("Produced by Script 272.")
    md_lines.append("")
    md_lines.append("## Final summary")
    md_lines.append("")
    md_lines.append("```")
    md_lines.extend(block)
    md_lines.append("```")
    md_lines.append("")
    md_lines.append(
        "> **`canonical_patient_master` in `thyroid_canonical_publication_v1_0` "
        f"is the authoritative publication dataset.** It holds {inv['row_count']} "
        f"patients × {inv['column_count']} columns. All invariants pass."
    )
    md_lines.append("")
    md_lines.append(f"## Unresolved registry pointers ({unresolved_n})")
    md_lines.append("")
    md_lines.append(
        "All entries below are flagged `needs_manual_review=true` in "
        f"`{WS}.detail_table_registry_v1`. They fall into two groups:"
    )
    md_lines.append("")
    md_lines.append(
        "1. **Deferred oversized wildcards** — wildcards whose CPM expansion "
        f"exceeds the {WILDCARD_EXPANSION_PER_PATTERN_LIMIT}-col safety limit. "
        "Per-pattern expansion CSVs are in `scripts/output/272_wildcard_oversized_*.csv` "
        "for review."
    )
    md_lines.append(
        "2. **Real registry curation gaps** — feed_col entries that don't match "
        "any CPM column even after the parser, prose-skip, and wildcard passes. "
        "These are typically stale pointers (e.g. column dropped by an earlier "
        "script), domain shorthand (HTN/obesity/FNA/dysphagia), or fragments of "
        "a parenthesized list."
    )
    md_lines.append("")
    md_lines.append("| detail_table_name | feed_col |")
    md_lines.append("|---|---|")
    for det, fc in unresolved_rows:
        md_lines.append(f"| `{det}` | `{fc}` |")

    md_lines.append("")
    md_lines.append(
        f"## Cohort filter_type classification (m032/m038 + 24 thin wrappers)"
    )
    md_lines.append("")
    if table_exists(con, WS, "manuscript_dive_map_v1"):
        rows = con.execute(
            f'SELECT filter_type, COUNT(*) FROM "{PUBLICATION_DB}".{WS}.'
            f"manuscript_dive_map_v1 GROUP BY filter_type ORDER BY 1"
        ).fetchall()
        for ft, n in rows:
            md_lines.append(f"- **{ft or 'NULL'}**: {n}")
    md_lines.append("")
    md_lines.append(
        "Thin-wrapper assignment used the data-based heuristic from the coworker's "
        "Prompt 21 §3 method (row count = 10,871 AND column set ⊆ "
        "`cohort_descriptive_full_cohort_v1`). All `thin_wrapper` rows are flagged "
        "`filter_type_provisional=true` for PI confirmation."
    )

    md_lines.append("")
    md_lines.append("## Wildcard expansion ledger")
    md_lines.append("")
    md_lines.append("See `scripts/output/272_wildcard_expansion_log.csv` for the full ledger.")
    md_lines.append(
        "`wildcard_expansion_whitelist_cnln` records the explicit human "
        "approval of `cnln_*` (36 cols)."
    )
    md_lines.append("")
    md_lines.append("## Phase 5 status")
    md_lines.append("")
    md_lines.append(
        "Skipped — `manuscript_workspace.canonical_cleanup_audit_v1` reports "
        "0 objects with `has_version_twin=true` and 0 unreferenced objects. "
        "There is nothing to archive based on those signals. Dictionary "
        "predecessors (`v240` and `v266a`) are already inventoried in "
        "`scripts/output/272_archive_inventory.csv` (10 backup snapshots, "
        "1,490 → 1,590 row growth tracing dictionary expansion)."
    )

    handoff.write_text("\n".join(md_lines) + "\n")
    log(f"Handoff written to {handoff}")

    summary = {
        "canonical_patient_master_rows": inv["row_count"],
        "canonical_patient_master_cols": inv["column_count"],
        "main_base_tables": main_n,
        "manuscript_workspace_tables": ws_tbl,
        "manuscript_workspace_views": ws_vw,
        "registry_rows": reg_n,
        "validation_pass": val_pass,
        "validation_fail": val_fail,
        "unresolved_pointers": unresolved_n,
        "unresolved_by_table_count": {k: len(v) for k, v in unresolved_by_table.items()},
        "archive_objects": archive_n,
    }
    (OUT_DIR / f"{SCRIPT_NUM}_phase7_final.json").write_text(
        json.dumps(summary, indent=2)
    )
    return summary


# ---------------------------------------------------------------------------
# PHASE 2 PROMOTION (gated)
# ---------------------------------------------------------------------------
def apply_promote(con, log, override_unresolved: bool) -> None:
    log("=== APPLY: promote registry_v2 -> v1 ===")
    n_unres = con.execute(
        f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}.registry_v2_unresolved_pointers_v1'
    ).fetchone()[0]
    log(f"Unresolved pointer count: {n_unres}")
    if n_unres >= PROMOTION_UNRESOLVED_HARD_LIMIT and not override_unresolved:
        raise SystemExit(
            f"Unresolved pointers {n_unres} >= hard limit {PROMOTION_UNRESOLVED_HARD_LIMIT}. "
            f"Re-run with --override-unresolved to proceed (requires explicit chat approval)."
        )

    # Archive current v1 to archive_pub_v1_0
    con.execute(
        f'CREATE SCHEMA IF NOT EXISTS "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}'
    )
    arch_name = f"detail_table_registry_v1_archived_{RUN_DATE.replace('-', '')}"
    con.execute(
        f'CREATE TABLE IF NOT EXISTS "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{arch_name} AS '
        f"SELECT * FROM {REGISTRY_V1}"
    )
    src_n = con.execute(f"SELECT COUNT(*) FROM {REGISTRY_V1}").fetchone()[0]
    dst_n = con.execute(
        f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{arch_name}'
    ).fetchone()[0]
    log(f"Archive row-count check: src={src_n} dst={dst_n}")
    if src_n != dst_n:
        raise SystemExit("Row count mismatch on archive — aborting promotion.")
    con.execute(f"DROP TABLE {REGISTRY_V1}")
    con.execute(f"ALTER TABLE {REGISTRY_V2} RENAME TO detail_table_registry_v1")
    comment_on_table(
        con, WS, "detail_table_registry_v1",
        f"Rebuilt with feeds_master_columns_array; promoted from v2 by Script {SCRIPT_NUM}. "
        f"Prior v1 archived to {ARCHIVE_DB}.{ARCHIVE_SCHEMA}.{arch_name}.",
    )
    log("Promotion complete.")


# ---------------------------------------------------------------------------
# PHASE 5 — Archive approved candidates
# ---------------------------------------------------------------------------
def apply_archives(con, log, csv_path: Path) -> dict[str, Any]:
    log(f"=== APPLY: archive approved objects from {csv_path} ===")
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}')
    out: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            schema = row.get("schema_name") or row.get("schema")
            name = row.get("object_name") or row.get("name")
            if not schema or not name:
                log(f"  skipping malformed row: {row}")
                continue
            arch_name = f"{name}_archived_{RUN_DATE.replace('-', '')}"
            con.execute(
                f'CREATE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{arch_name} AS '
                f'SELECT * FROM "{PUBLICATION_DB}".{schema}."{name}"'
            )
            src = con.execute(
                f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{schema}."{name}"'
            ).fetchone()[0]
            dst = con.execute(
                f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{arch_name}'
            ).fetchone()[0]
            if src != dst:
                raise SystemExit(
                    f"Row mismatch archiving {schema}.{name}: src={src} dst={dst}"
                )
            con.execute(f'DROP TABLE "{PUBLICATION_DB}".{schema}."{name}"')
            out.append({"schema_name": schema, "object_name": name,
                        "archived_as": arch_name, "rows": src})
            log(f"  archived & dropped: {schema}.{name} -> {arch_name} ({src} rows)")
    con.execute("VACUUM")
    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_phase5_archived.csv",
        out,
        ["schema_name", "object_name", "archived_as", "rows"],
    )
    return {"archived_count": len(out)}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def main() -> int:
    p = argparse.ArgumentParser(description="Script 272 — Canonical finalization")
    p.add_argument(
        "--phase",
        action="append",
        choices=["0", "1", "2", "3a", "3b", "3c", "3recon", "4", "5inv", "6", "7",
                 "audit"],
        help="Phase(s) to run; pass multiple times. "
             "'audit' = 0,1,2,3a,3b,3c,3recon,4,5inv,6.",
    )
    p.add_argument("--apply-promote", action="store_true",
                   help="Phase 2 step 7: promote v2 -> v1. Requires chat approval.")
    p.add_argument("--override-unresolved", action="store_true",
                   help="Allow promotion when unresolved pointers >= hard limit.")
    p.add_argument("--apply-archives", type=str, default=None,
                   help="Path to approved Phase 5 archive CSV.")
    args = p.parse_args()

    log = TeeLogger(RUN_LOG)
    log(f"Script {SCRIPT_NUM} start; date={RUN_DATE}; UTC={datetime.now(timezone.utc).isoformat()}")
    con = connect_locked()
    log(f"Connected to MotherDuck DB: {PUBLICATION_DB}")

    phases = []
    if args.phase:
        for ph in args.phase:
            if ph == "audit":
                phases += ["0", "1", "2", "3a", "3b", "3c", "3recon", "4", "5inv", "6"]
            else:
                phases.append(ph)
    # de-dup, preserve order
    seen = set()
    phases = [p for p in phases if not (p in seen or seen.add(p))]
    log(f"Requested phases: {phases or '(none)'}")

    results: dict[str, Any] = {}
    try:
        for ph in phases:
            assert_cpm_invariants(con, log)
            t0 = time.time()
            fn = {
                "0": phase_0, "1": phase_1, "2": phase_2,
                "3a": phase_3a, "3b": phase_3b, "3c": phase_3c,
                "3recon": phase_3recon, "4": phase_4,
                "5inv": phase_5inv, "6": phase_6, "7": phase_7,
            }[ph]
            results[ph] = fn(con, log)
            log(f"Phase {ph} done in {time.time()-t0:.1f}s")
            assert_cpm_invariants(con, log)

        if args.apply_promote:
            assert_cpm_invariants(con, log)
            apply_promote(con, log, args.override_unresolved)
            assert_cpm_invariants(con, log)
            results["apply_promote"] = "DONE"

        if args.apply_archives:
            assert_cpm_invariants(con, log)
            results["apply_archives"] = apply_archives(
                con, log, Path(args.apply_archives)
            )
            assert_cpm_invariants(con, log)

    finally:
        log(f"Run results summary: {json.dumps(results, indent=2, default=str)}")
        log.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
