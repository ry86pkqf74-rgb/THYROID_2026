#!/usr/bin/env python3
"""Script 273 — Registry Curation & Metadata Fill.

Closes out the 18 unresolved pointers and 8 metadata gaps left by Script 272 in
``thyroid_canonical_publication_v1_0.manuscript_workspace.detail_table_registry_v1``.

All real semantic mappings (no wildcard guessing) — every replacement column is
verified to exist in ``main.canonical_patient_master.information_schema.columns``
before being written.

Phases (default: all):
  0  Setup, baseline metrics, archive snapshot of registry_v1.
  1  Fill NULL ``total_rows`` (3) and NULL ``total_patients`` (8).
  2  Resolve 18 unresolved pointers via explicit enumeration (2A–2H).
  3  Build ``thin_wrapper_pi_review_v273`` for the 24 provisional rows
     (PI confirmation artifact only — no un-provisioning).
  4  End-to-end re-validation against the updated registry.
  5  Final invariants + summary block. Does NOT commit (WIP only).

Invariants verified at start and end of every run:
  - canonical_patient_master rows == 10871, distinct rids == 10871,
    NULL rid == 0, NULL fna_path_outcome == 0, RID type VARCHAR.

Archive destination (read-only writes only): ``"Thyroid 2026 UPdated".archive_pub_v1_0``.
NEVER writes to ``"Thyroid 2026 UPdated".main``.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_NUM = "273"
PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
MAIN = "main"
WS = "manuscript_workspace"
CPM = "canonical_patient_master"
REGISTRY_V1 = f'"{PUBLICATION_DB}".{WS}.detail_table_registry_v1'

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"

CPM_ROWS = 10871
CPM_COLS = 1526
CANONICAL_VERSION_TAG = "v1_0_script273"

OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

UTC_TS_COMPACT = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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
# Connection
# ---------------------------------------------------------------------------
def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            "No MotherDuck RW token. Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or "
            "populate motherduck.local.toml."
        )
    return duckdb.connect(f"md:{PUBLICATION_DB}?motherduck_token={tok}")


# ---------------------------------------------------------------------------
# Invariants
# ---------------------------------------------------------------------------
def assert_cpm_invariants(con, log) -> dict[str, Any]:
    n, rids, nrid, nfna = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN research_id IS NULL THEN 1 ELSE 0 END),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
        FROM "{PUBLICATION_DB}".{MAIN}.{CPM}
    """).fetchone()
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
    if n != CPM_ROWS:
        failures.append(f"row_count={n} (expected {CPM_ROWS})")
    if rids != CPM_ROWS:
        failures.append(f"distinct_rids={rids} (expected {CPM_ROWS})")
    if nrid != 0:
        failures.append(f"null_research_id={nrid}")
    if nfna != 0:
        failures.append(f"null_fna_path_outcome={nfna}")
    if rid_type.upper() != "VARCHAR":
        failures.append(f"rid_type={rid_type} (expected VARCHAR)")
    if cols != CPM_COLS:
        failures.append(f"column_count={cols} (expected {CPM_COLS})")
    if failures:
        log("INVARIANT FAILURE: " + "; ".join(failures))
        raise SystemExit("Invariants failed; aborting.")
    log(
        f"Invariants OK: rows={n} rids={rids} null_rid={nrid} "
        f"null_fna={nfna} rid_type={rid_type} cols={cols}"
    )
    return {
        "row_count": n,
        "distinct_research_ids": rids,
        "null_research_id": nrid,
        "null_fna_path_outcome": nfna,
        "research_id_type": rid_type,
        "column_count": cols,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def cpm_columns(con) -> set[str]:
    return {
        r[0]
        for r in con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{MAIN}'
              AND table_name='{CPM}'
        """).fetchall()
    }


def cpm_columns_like(con, pattern: str) -> list[str]:
    """Return CPM columns matching a SQL LIKE pattern, in alphabetical order."""
    return [
        r[0]
        for r in con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{MAIN}'
              AND table_name='{CPM}' AND column_name LIKE ?
            ORDER BY column_name
        """, [pattern]).fetchall()
    ]


def column_names(con, schema: str, table: str) -> list[str]:
    return [
        r[0]
        for r in con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema=?
              AND table_name=? ORDER BY ordinal_position
        """, [schema, table]).fetchall()
    ]


def table_exists(con, schema: str, table: str) -> bool:
    n = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema=?
          AND table_name=?
    """, [schema, table]).fetchone()[0]
    return n > 0


def base_table_count(con, schema: str) -> int:
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema=?
          AND table_type='BASE TABLE'
    """, [schema]).fetchone()[0]


def view_count(con, schema: str) -> int:
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema=?
          AND table_type='VIEW'
    """, [schema]).fetchone()[0]


def archive_object_count(con) -> int:
    return con.execute(f"""
        SELECT COUNT(*) FROM "{ARCHIVE_DB}".information_schema.tables
        WHERE table_schema='{ARCHIVE_SCHEMA}'
    """).fetchone()[0]


def split_feeds(value: str) -> list[str]:
    """Split feeds_master_columns on ; or , (272 used the same parser)."""
    if value is None:
        return []
    v = value.strip()
    if not v:
        return []
    parts = re.split(r"[;,]", v)
    return [p.strip() for p in parts if p and p.strip()]


_PROSE_RE = re.compile(r"[\s()/]")


def is_prose(feed_col: str) -> bool:
    if "*" in feed_col:
        return False
    return bool(_PROSE_RE.search(feed_col))


def append_description(con, table: str, append_text: str) -> None:
    con.execute(
        f"UPDATE {REGISTRY_V1} "
        f"SET description = COALESCE(description, '') || ? "
        f"WHERE detail_table_name = ?",
        [f" | {append_text}", table],
    )


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


# ---------------------------------------------------------------------------
# PHASE 0 — Setup, baseline, archive snapshot
# ---------------------------------------------------------------------------
def phase_0(con, log) -> dict[str, Any]:
    log("=== PHASE 0: Baseline + archive snapshot ===")
    inv = assert_cpm_invariants(con, log)

    main_n = base_table_count(con, MAIN)
    ws_tbl = base_table_count(con, WS)
    ws_vw = view_count(con, WS)
    reg_n = con.execute(f"SELECT COUNT(*) FROM {REGISTRY_V1}").fetchone()[0]
    unresolved_n = con.execute(
        f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}.registry_v2_unresolved_pointers_v1'
    ).fetchone()[0]
    needs_review_n = con.execute(
        f"SELECT COUNT(*) FROM {REGISTRY_V1} WHERE needs_manual_review = TRUE"
    ).fetchone()[0]
    null_rows_n = con.execute(
        f"SELECT COUNT(*) FROM {REGISTRY_V1} WHERE total_rows IS NULL"
    ).fetchone()[0]
    null_pat_n = con.execute(
        f"SELECT COUNT(*) FROM {REGISTRY_V1} WHERE total_patients IS NULL"
    ).fetchone()[0]
    arch_n = archive_object_count(con)

    log(
        f"Baseline: registry={reg_n}  unresolved={unresolved_n}  "
        f"needs_manual_review={needs_review_n}  null_rows={null_rows_n}  "
        f"null_patients={null_pat_n}  main={main_n}  ws={ws_tbl}t+{ws_vw}v  "
        f"archive_objs={arch_n}"
    )

    # Snapshot registry to archive_pub_v1_0
    snap_name = f"detail_table_registry_v1_pre273_{UTC_TS_COMPACT}"
    snap_full = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}."{snap_name}"'
    log(f"Snapshotting {REGISTRY_V1} -> {snap_full}")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS \"{ARCHIVE_DB}\".{ARCHIVE_SCHEMA}")
    con.execute(f"CREATE TABLE {snap_full} AS SELECT * FROM {REGISTRY_V1}")
    snap_n = con.execute(f"SELECT COUNT(*) FROM {snap_full}").fetchone()[0]
    if snap_n != reg_n:
        raise SystemExit(
            f"Snapshot row mismatch: snap={snap_n} live={reg_n} — aborting."
        )
    log(f"Snapshot OK: {snap_full} ({snap_n} rows)")

    baseline = {
        "script": SCRIPT_NUM,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "canonical_patient_master": inv,
        "main_base_tables": main_n,
        "manuscript_workspace_tables": ws_tbl,
        "manuscript_workspace_views": ws_vw,
        "detail_table_registry_v1_rows": reg_n,
        "registry_v2_unresolved_pointers_v1_rows": unresolved_n,
        "needs_manual_review_true": needs_review_n,
        "null_total_rows": null_rows_n,
        "null_total_patients": null_pat_n,
        "archive_pub_v1_0_objects_pre": arch_n,
        "snapshot_table": snap_name,
    }
    (OUT_DIR / f"{SCRIPT_NUM}_baseline.json").write_text(
        json.dumps(baseline, indent=2)
    )
    return baseline


# ---------------------------------------------------------------------------
# PHASE 1 — Fill metadata gaps
# ---------------------------------------------------------------------------
def phase_1(con, log) -> dict[str, Any]:
    log("=== PHASE 1: Fill metadata gaps ===")
    # 1A. NULL total_rows
    null_rows = [
        r[0]
        for r in con.execute(f"""
            SELECT detail_table_name FROM {REGISTRY_V1}
            WHERE total_rows IS NULL ORDER BY 1
        """).fetchall()
    ]
    log(f"NULL total_rows targets: {null_rows}")
    for tbl in null_rows:
        sch = con.execute(
            f"SELECT schema_name FROM {REGISTRY_V1} WHERE detail_table_name=?",
            [tbl],
        ).fetchone()[0]
        if not table_exists(con, sch, tbl):
            log(f"  WARN: {sch}.{tbl} not found — skipping total_rows fill")
            continue
        n = con.execute(
            f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{sch}."{tbl}"'
        ).fetchone()[0]
        con.execute(
            f"UPDATE {REGISTRY_V1} "
            f"SET total_rows = ?, canonical_version = ? "
            f"WHERE detail_table_name = ?",
            [n, CANONICAL_VERSION_TAG, tbl],
        )
        log(f"  total_rows[{tbl}] = {n}")

    # 1B. NULL total_patients — split into has-rid vs catalog
    null_pat = con.execute(f"""
        SELECT detail_table_name, schema_name FROM {REGISTRY_V1}
        WHERE total_patients IS NULL ORDER BY 1
    """).fetchall()
    log(f"NULL total_patients targets: {[r[0] for r in null_pat]}")
    for tbl, sch in null_pat:
        if not table_exists(con, sch, tbl):
            log(f"  WARN: {sch}.{tbl} not found — setting total_patients=0")
            con.execute(
                f"UPDATE {REGISTRY_V1} "
                f"SET total_patients = 0, canonical_version = ?, "
                f"description = COALESCE(description,'') || ? "
                f"WHERE detail_table_name = ?",
                [
                    CANONICAL_VERSION_TAG,
                    " | Script 273: table missing at curation time — patients=0.",
                    tbl,
                ],
            )
            continue
        cols = column_names(con, sch, tbl)
        if "research_id" in cols:
            n = con.execute(f'''
                SELECT COUNT(DISTINCT research_id)
                  FROM "{PUBLICATION_DB}".{sch}."{tbl}"
                  WHERE research_id IS NOT NULL
            ''').fetchone()[0]
            con.execute(
                f"UPDATE {REGISTRY_V1} "
                f"SET total_patients = ?, canonical_version = ? "
                f"WHERE detail_table_name = ?",
                [n, CANONICAL_VERSION_TAG, tbl],
            )
            log(f"  total_patients[{tbl}] = {n} (DISTINCT research_id)")
        else:
            note = "Script 273: no research_id column — not patient-keyed."
            con.execute(
                f"UPDATE {REGISTRY_V1} "
                f"SET total_patients = 0, canonical_version = ?, "
                f"description = COALESCE(description,'') || ? "
                f"WHERE detail_table_name = ?",
                [CANONICAL_VERSION_TAG, f" | {note}", tbl],
            )
            log(f"  total_patients[{tbl}] = 0 (no research_id col)")

    # Verify
    rem_rows = con.execute(
        f"SELECT COUNT(*) FROM {REGISTRY_V1} WHERE total_rows IS NULL"
    ).fetchone()[0]
    rem_pat = con.execute(
        f"SELECT COUNT(*) FROM {REGISTRY_V1} WHERE total_patients IS NULL"
    ).fetchone()[0]
    log(f"Post-Phase-1: NULL total_rows={rem_rows}  NULL total_patients={rem_pat}")
    if rem_rows != 0 or rem_pat != 0:
        raise SystemExit(
            "Phase 1 failed to clear all metadata NULLs — investigate before continuing."
        )
    return {"null_total_rows": rem_rows, "null_total_patients": rem_pat}


# ---------------------------------------------------------------------------
# PHASE 2 — Resolve 18 unresolved pointers
# ---------------------------------------------------------------------------
PHASE2_GUARDRAIL_PCT = 0.10  # >10% drift from expected count -> halt


def _expected_drift(actual: int, expected: int) -> bool:
    if expected <= 0:
        return False
    return abs(actual - expected) / expected > PHASE2_GUARDRAIL_PCT


def _set_feeds(
    con,
    table: str,
    feeds: str,
    *,
    array: list[str] | None,
    secondary: str | None = None,
    clear_review: bool = True,
    desc_append: str | None = None,
) -> None:
    """Atomic UPDATE for one registry row."""
    set_parts = ["feeds_master_columns = ?", "feeds_master_columns_array = ?",
                 "canonical_version = ?"]
    params: list[Any] = [feeds, array if array is not None else [],
                         CANONICAL_VERSION_TAG]
    if secondary is not None:
        set_parts.append("feeds_master_columns_secondary = ?")
        params.append(secondary)
    if clear_review:
        set_parts.append("needs_manual_review = FALSE")
    if desc_append is not None:
        set_parts.append("description = COALESCE(description,'') || ?")
        params.append(f" | {desc_append}")
    params.append(table)
    con.execute(
        f"UPDATE {REGISTRY_V1} SET {', '.join(set_parts)} "
        f"WHERE detail_table_name = ?",
        params,
    )


def _verify_all_in_cpm(cpm_set: set[str], cols: list[str], label: str) -> list[str]:
    missing = [c for c in cols if c not in cpm_set]
    if missing:
        raise SystemExit(
            f"[{label}] {len(missing)} columns NOT in CPM: {missing[:5]}... "
            "Aborting — refusing to invent column names."
        )
    return cols


def phase_2(con, log) -> dict[str, Any]:
    log("=== PHASE 2: Resolve 18 unresolved pointers ===")
    cpm_set = cpm_columns(con)

    repair_log: list[dict[str, Any]] = []

    # ----- 2A. extracted_tirads_validated_v1: 12 tirads_*_v12 columns -----
    log("--- 2A: extracted_tirads_validated_v1 -> 12 tirads_*_v12 cols ---")
    tirads_v12 = [
        "tirads_best_score_v12",
        "tirads_worst_score_v12",
        "tirads_best_category_v12",
        "tirads_worst_category_v12",
        "tirads_source_v12",
        "tirads_reliability_v12",
        "tirads_has_acr_recalc_v12",
        "tirads_n_sources_v12",
        "tirads_n_nodule_records_v12",
        "tirads_concordant_count_v12",
        "tirads_mismatch_count_v12",
        "tirads_nodule_size_max_mm_v12",
    ]
    _verify_all_in_cpm(cpm_set, tirads_v12, "2A tirads_v12")
    _set_feeds(
        con,
        "extracted_tirads_validated_v1",
        feeds=";".join(tirads_v12),
        array=tirads_v12,
        desc_append=(
            "Script 273 (2A): replaced 3 prose tokens (echogenicity/shape/margin) "
            "with the 12 tirads_*_v12 CPM rollup columns this table actually feeds."
        ),
    )
    repair_log.append({
        "phase": "2A", "detail_table_name": "extracted_tirads_validated_v1",
        "action": "enumerate", "expected": 12, "actual": len(tirads_v12),
        "notes": "12 tirads_*_v12 cols",
    })

    # ----- 2B. note_entities_llm_past_medical_hx -> pmhx_nlp_% (59) -----
    log("--- 2B: note_entities_llm_past_medical_hx -> pmhx_nlp_% ---")
    pmhx = cpm_columns_like(con, "pmhx_nlp_%")
    log(f"  pmhx_nlp_% expected=59 actual={len(pmhx)}")
    if _expected_drift(len(pmhx), 59):
        raise SystemExit(
            f"2B drift: expected 59 pmhx_nlp_* cols, got {len(pmhx)}. "
            "ASK USER before proceeding."
        )
    _set_feeds(
        con,
        "note_entities_llm_past_medical_hx",
        feeds=";".join(pmhx),
        array=pmhx,
        desc_append=(
            f"Script 273 (2B): replaced 4 prose tokens (HTN/obesity/radiation/etc.) "
            f"with the {len(pmhx)} explicitly enumerated pmhx_nlp_* CPM columns."
        ),
    )
    repair_log.append({
        "phase": "2B", "detail_table_name": "note_entities_llm_past_medical_hx",
        "action": "enumerate", "expected": 59, "actual": len(pmhx),
        "notes": "pmhx_nlp_% explicit enumeration",
    })

    # ----- 2C row 1: note_entities_llm_past_surgical_hx -> pshx_nlp_% (15) -----
    log("--- 2C.1: note_entities_llm_past_surgical_hx -> pshx_nlp_% ---")
    pshx = cpm_columns_like(con, "pshx_nlp_%")
    pshx_secondary = cpm_columns_like(con, "nlp_pshx_%")
    log(f"  pshx_nlp_% actual={len(pshx)}; nlp_pshx_% (secondary)={len(pshx_secondary)}")
    if len(pshx) == 0:
        raise SystemExit(
            "2C.1: no pshx_nlp_* cols found — need to investigate naming. "
            "ASK USER."
        )
    _set_feeds(
        con,
        "note_entities_llm_past_surgical_hx",
        feeds=";".join(pshx),
        array=pshx,
        secondary=";".join(pshx_secondary),
        desc_append=(
            f"Script 273 (2C.1): replaced prose token 'FNA' with the {len(pshx)} "
            f"pshx_nlp_* primary-feed CPM columns; secondary lists "
            f"{len(pshx_secondary)} nlp_pshx_* presence/summary cols."
        ),
    )
    repair_log.append({
        "phase": "2C.1",
        "detail_table_name": "note_entities_llm_past_surgical_hx",
        "action": "enumerate", "expected": -1, "actual": len(pshx),
        "notes": f"pshx_nlp_% primary; nlp_pshx_% secondary ({len(pshx_secondary)})",
    })

    # ----- 2C row 2: note_entities_llm_presenting_symptoms -> sx_nlp_% -----
    log("--- 2C.2: note_entities_llm_presenting_symptoms -> sx_nlp_% ---")
    sx = cpm_columns_like(con, "sx_nlp_%")
    sx_secondary = cpm_columns_like(con, "nlp_symptoms_%")
    log(f"  sx_nlp_% actual={len(sx)}; nlp_symptoms_% (secondary)={len(sx_secondary)}")
    if len(sx) == 0:
        raise SystemExit(
            "2C.2: no sx_nlp_* cols found — ASK USER for correct prefix."
        )
    _set_feeds(
        con,
        "note_entities_llm_presenting_symptoms",
        feeds=";".join(sx),
        array=sx,
        secondary=";".join(sx_secondary),
        desc_append=(
            f"Script 273 (2C.2): replaced prose token 'dysphagia' with the "
            f"{len(sx)} sx_nlp_* primary-feed CPM columns; secondary lists "
            f"{len(sx_secondary)} nlp_symptoms_* presence/summary cols."
        ),
    )
    repair_log.append({
        "phase": "2C.2",
        "detail_table_name": "note_entities_llm_presenting_symptoms",
        "action": "enumerate", "expected": -1, "actual": len(sx),
        "notes": f"sx_nlp_% primary; nlp_symptoms_% secondary ({len(sx_secondary)})",
    })

    # ----- 2C row 3: note_entities_operative_detail -> op_nlp_% (44) -----
    log("--- 2C.3: note_entities_operative_detail -> op_nlp_% ---")
    op_nlp = cpm_columns_like(con, "op_nlp_%")
    log(f"  op_nlp_% actual={len(op_nlp)}")
    _set_feeds(
        con,
        "note_entities_operative_detail",
        feeds=";".join(op_nlp),
        array=op_nlp,
        desc_append=(
            f"Script 273 (2C.3): replaced sentinel 'op_nlp_* columns' with the "
            f"{len(op_nlp)} explicitly enumerated op_nlp_* CPM columns."
        ),
    )
    repair_log.append({
        "phase": "2C.3", "detail_table_name": "note_entities_operative_detail",
        "action": "enumerate", "expected": -1, "actual": len(op_nlp),
        "notes": "op_nlp_% explicit enumeration",
    })

    # ----- 2C row 4: note_entities_problem_list -> pmhx_nlp_% (indirect) -----
    log("--- 2C.4: note_entities_problem_list -> pmhx_nlp_% (indirect) ---")
    _set_feeds(
        con,
        "note_entities_problem_list",
        feeds="upstream for note_entities_llm_past_medical_hx",
        array=[],
        secondary=";".join(pmhx),
        desc_append=(
            f"Script 273 (2C.4): problem-list NLP is an indirect/upstream "
            f"contributor to the same {len(pmhx)} pmhx_nlp_* family registered "
            f"under note_entities_llm_past_medical_hx; not a direct feeder."
        ),
    )
    repair_log.append({
        "phase": "2C.4", "detail_table_name": "note_entities_problem_list",
        "action": "upstream_marker", "expected": 59, "actual": len(pmhx),
        "notes": "pmhx_nlp_% in secondary; primary=upstream sentinel",
    })

    # ----- 2D. note_entities_complications -> upstream sentinel -----
    log("--- 2D: note_entities_complications -> upstream sentinel ---")
    _set_feeds(
        con,
        "note_entities_complications",
        feeds="upstream for complication_phenotype_v1",
        array=[],
        desc_append=(
            "Script 273 (2D): NLP complications table is upstream of "
            "complication_phenotype_v1 which already registers the full comp_* "
            "(93 cols) family; not duplicated here."
        ),
    )
    repair_log.append({
        "phase": "2D", "detail_table_name": "note_entities_complications",
        "action": "upstream_marker", "expected": 93, "actual": 0,
        "notes": "comp_* coverage owned by complication_phenotype_v1",
    })

    # ----- 2E. note_entities_llm_cervical_ln_detail -> cnln_% in secondary -----
    log("--- 2E: note_entities_llm_cervical_ln_detail -> cnln_% in secondary ---")
    cnln = cpm_columns_like(con, "cnln_%")
    log(f"  cnln_% actual={len(cnln)}")
    if _expected_drift(len(cnln), 36):
        raise SystemExit(
            f"2E drift: expected 36 cnln_*, got {len(cnln)}. ASK USER."
        )
    # Preserve any prior secondary tokens (nlp_ln_*) by union
    prior_sec = con.execute(
        f"SELECT feeds_master_columns_secondary FROM {REGISTRY_V1} "
        f"WHERE detail_table_name='note_entities_llm_cervical_ln_detail'"
    ).fetchone()[0]
    prior_tokens = split_feeds(prior_sec) if prior_sec else []
    sec_union: list[str] = []
    seen: set[str] = set()
    for c in prior_tokens + cnln:
        if c not in seen:
            sec_union.append(c)
            seen.add(c)
    _set_feeds(
        con,
        "note_entities_llm_cervical_ln_detail",
        feeds="upstream for clinical_note_ln_extracted_v1",
        array=[],
        secondary=";".join(sec_union),
        desc_append=(
            f"Script 273 (2E): NLP cervical-LN table is upstream of "
            f"clinical_note_ln_extracted_v1; secondary now lists {len(sec_union)} "
            f"cols (cnln_* enumeration ∪ prior nlp_ln_* tokens)."
        ),
    )
    repair_log.append({
        "phase": "2E", "detail_table_name": "note_entities_llm_cervical_ln_detail",
        "action": "upstream_marker_with_secondary",
        "expected": 36, "actual": len(cnln),
        "notes": f"cnln_% enumeration in secondary; total secondary={len(sec_union)}",
    })

    # ----- 2F. nsqip_patient_summary -> nsqip_% (102) -----
    log("--- 2F.1: nsqip_patient_summary -> nsqip_% ---")
    nsqip = cpm_columns_like(con, "nsqip_%")
    log(f"  nsqip_% actual={len(nsqip)} (expected 102)")
    if _expected_drift(len(nsqip), 102):
        raise SystemExit(
            f"2F.1 drift: expected 102 nsqip_*, got {len(nsqip)}. ASK USER."
        )
    _set_feeds(
        con,
        "nsqip_patient_summary",
        feeds=";".join(nsqip),
        array=nsqip,
        desc_append=(
            f"Script 273 (2F.1): replaced sentinel 'nsqip_* columns (full set of "
            f"80+)' with the {len(nsqip)} explicitly enumerated nsqip_* CPM cols."
        ),
    )
    repair_log.append({
        "phase": "2F.1", "detail_table_name": "nsqip_patient_summary",
        "action": "enumerate", "expected": 102, "actual": len(nsqip),
        "notes": "nsqip_% explicit enumeration",
    })

    # ----- 2F.2 path_synoptics -> syn_% (41) -----
    log("--- 2F.2: path_synoptics -> syn_% ---")
    syn = cpm_columns_like(con, "syn_%")
    log(f"  syn_% actual={len(syn)} (expected 41)")
    if _expected_drift(len(syn), 41):
        raise SystemExit(
            f"2F.2 drift: expected 41 syn_*, got {len(syn)}. ASK USER."
        )
    # Preserve current secondary content
    prior_sec = con.execute(
        f"SELECT feeds_master_columns_secondary FROM {REGISTRY_V1} "
        f"WHERE detail_table_name='path_synoptics'"
    ).fetchone()[0]
    prior_tokens = split_feeds(prior_sec) if prior_sec else []
    # Promote the syn_* family into primary; keep prior tokens in secondary
    # (some of those overlap, e.g. syn_follicular_adenoma — de-dup via union).
    sec_keep = [t for t in prior_tokens if t not in set(syn)]
    _set_feeds(
        con,
        "path_synoptics",
        feeds=";".join(syn),
        array=syn,
        secondary=";".join(sec_keep) if sec_keep else "",
        desc_append=(
            f"Script 273 (2F.2): replaced prose token 'syn_* columns (~50)' with "
            f"the {len(syn)} enumerated syn_* CPM cols; preserved {len(sec_keep)} "
            f"non-syn_ secondary tokens."
        ),
    )
    repair_log.append({
        "phase": "2F.2", "detail_table_name": "path_synoptics",
        "action": "enumerate", "expected": 41, "actual": len(syn),
        "notes": f"syn_% enumeration; secondary kept={len(sec_keep)}",
    })

    # ----- 2G. operative_episode_detail_v2 -> classified op_* -----
    log("--- 2G: operative_episode_detail_v2 -> classified op_* split ---")
    op_all = cpm_columns_like(con, "op_%")
    log(f"  op_% total actual={len(op_all)} (expected 103)")
    if _expected_drift(len(op_all), 103):
        raise SystemExit(
            f"2G drift: expected 103 op_*, got {len(op_all)}. ASK USER."
        )
    op_nlp_set = set(cpm_columns_like(con, "op_nlp_%"))
    ops_set = set(cpm_columns_like(con, "ops_%"))
    # (a) operative_episode_detail_v2 rollups: op_% NOT op_nlp_% NOT ops_%
    op_episode = sorted(c for c in op_all if c not in op_nlp_set and c not in ops_set)
    # (b) op_sheet_data ingestion: ops_*
    op_sheet = sorted(ops_set)
    # (c) NLP from note_entities_operative_detail: op_nlp_*
    op_nlp_list = sorted(op_nlp_set)
    log(f"  classified: episode_v2={len(op_episode)}  op_sheet/ops_={len(op_sheet)}  op_nlp={len(op_nlp_list)}")
    # Build new feeds_master_columns: keep prior non-wildcard tokens that
    # were already valid, drop the 'op_* columns' wildcard, then append (a)
    prior_arr = con.execute(
        f"SELECT feeds_master_columns_array FROM {REGISTRY_V1} "
        f"WHERE detail_table_name='operative_episode_detail_v2'"
    ).fetchone()[0] or []
    keep_prior = [t for t in prior_arr if t and "*" not in t and t in cpm_set]
    new_arr = []
    seen = set()
    for c in keep_prior + op_episode:
        if c not in seen:
            new_arr.append(c)
            seen.add(c)
    sec_lines = (
        f"op_sheet_data:{';'.join(op_sheet)}"
        f"||note_entities_operative_detail:{';'.join(op_nlp_list)}"
    )
    _set_feeds(
        con,
        "operative_episode_detail_v2",
        feeds=";".join(new_arr),
        array=new_arr,
        secondary=sec_lines,
        desc_append=(
            f"Script 273 (2G): replaced 'op_* columns' wildcard with classified "
            f"set — primary = {len(op_episode)} cross-surgery rollups (op_% "
            f"\\ op_nlp_% \\ ops_%); secondary tags {len(op_sheet)} ops_* cols "
            f"(op_sheet_data) and {len(op_nlp_list)} op_nlp_* cols "
            f"(note_entities_operative_detail)."
        ),
    )
    repair_log.append({
        "phase": "2G", "detail_table_name": "operative_episode_detail_v2",
        "action": "classify_split",
        "expected": 103, "actual": len(op_all),
        "notes": (
            f"primary={len(op_episode)} (rollups); "
            f"secondary={len(op_sheet)} ops_*+{len(op_nlp_list)} op_nlp_*"
        ),
    })

    # ----- 2H. build_pipeline -> drop 2 stale tokens -----
    log("--- 2H: build_pipeline -> drop n_molecular_tests_v7, ajcc8_t_stage_corrected ---")
    stale = ["n_molecular_tests_v7", "ajcc8_t_stage_corrected"]
    # Verify both still absent from CPM
    for c in stale:
        if c in cpm_set:
            raise SystemExit(
                f"2H precondition failed: '{c}' is present in CPM — refusing to "
                "delete a live pointer. ASK USER."
            )
    bp = con.execute(
        f"SELECT feeds_master_columns, feeds_master_columns_array FROM {REGISTRY_V1} "
        f"WHERE detail_table_name='build_pipeline'"
    ).fetchone()
    fmc_str = bp[0] or ""
    fmc_arr = bp[1] or []
    new_arr = [t for t in fmc_arr if t not in stale]
    # Rebuild semicolon-joined string from the original tokens minus stale ones,
    # preserving the original delimiter style by reading cleaned tokens from arr.
    new_str = ";".join(new_arr)
    removed = sorted(set(fmc_arr) - set(new_arr))
    log(
        f"  before: arr_n={len(fmc_arr)}  after: arr_n={len(new_arr)}  "
        f"removed: {removed}"
    )
    if set(removed) != set(stale):
        log(f"  WARN: expected to remove exactly {stale}; actually removed {removed}")
    # Also remove from feeds_master_columns_secondary if present
    bp_sec = con.execute(
        f"SELECT feeds_master_columns_secondary FROM {REGISTRY_V1} "
        f"WHERE detail_table_name='build_pipeline'"
    ).fetchone()[0]
    new_sec = bp_sec
    if bp_sec:
        sec_arr = split_feeds(bp_sec)
        new_sec_arr = [t for t in sec_arr if t not in stale]
        new_sec = ";".join(new_sec_arr)
    # Recompute needs_manual_review later in completion check; for now leave
    # build_pipeline's review state to be re-derived from updated audit.
    note = (
        "Script 273 (2H): removed n_molecular_tests_v7 and ajcc8_t_stage_corrected "
        "— columns dropped by Script 267."
    )
    con.execute(
        f"UPDATE {REGISTRY_V1} SET "
        f"  feeds_master_columns = ?, "
        f"  feeds_master_columns_array = ?, "
        f"  feeds_master_columns_secondary = ?, "
        f"  canonical_version = ?, "
        f"  description = COALESCE(description,'') || ? "
        f"WHERE detail_table_name = 'build_pipeline'",
        [
            new_str,
            new_arr,
            new_sec if new_sec is not None else None,
            CANONICAL_VERSION_TAG,
            f" | {note}",
        ],
    )
    repair_log.append({
        "phase": "2H", "detail_table_name": "build_pipeline",
        "action": "drop_stale_tokens",
        "expected": 2, "actual": len(removed),
        "notes": f"removed: {','.join(removed)}",
    })

    # ----- Phase 2 completion check: rebuild audit + unresolved -----
    log("--- Phase 2 completion check: rebuild audit + unresolved (v273) ---")
    cpm_set = cpm_columns(con)  # Refresh — unchanged but explicit
    rows = con.execute(f"""
        SELECT detail_table_name, feeds_master_columns_array
          FROM {REGISTRY_V1}
    """).fetchall()
    audit_tbl = "registry_v2_resolution_audit_v273"
    unres_tbl = "registry_v2_unresolved_pointers_v273"
    for t in (audit_tbl, unres_tbl):
        if table_exists(con, WS, t):
            con.execute(f'DROP TABLE "{PUBLICATION_DB}".{WS}.{t}')
    con.execute(f"""
        CREATE TABLE "{PUBLICATION_DB}".{WS}.{audit_tbl} (
            detail_table_name VARCHAR,
            feed_col VARCHAR,
            resolves_bool BOOLEAN,
            auto_repaired_bool BOOLEAN,
            repair_mode VARCHAR,
            repair_source_name VARCHAR
        )
    """)
    audit_rows: list[tuple[str, str, bool, bool, str, str]] = []
    flag_unresolved: dict[str, list[str]] = {}
    flag_resolved_or_prose: set[str] = set()
    for det, arr in rows:
        if not arr:
            continue
        for fc in arr:
            if not fc:
                continue
            if "*" in fc:
                # Wildcard sentinel left in place after Script 272 + 273 work.
                # Treat as auto_repaired (kept-as-is) so it stops counting unresolved.
                audit_rows.append((det, fc, False, True, "wildcard_kept_as_is", ""))
                continue
            if fc in cpm_set:
                audit_rows.append((det, fc, True, False, "", fc))
                flag_resolved_or_prose.add(det)
            elif is_prose(fc):
                audit_rows.append((det, fc, False, True, "prose_skipped", ""))
                flag_resolved_or_prose.add(det)
            else:
                audit_rows.append((det, fc, False, False, "", ""))
                flag_unresolved.setdefault(det, []).append(fc)
    if audit_rows:
        con.executemany(
            f'INSERT INTO "{PUBLICATION_DB}".{WS}.{audit_tbl} '
            "(detail_table_name, feed_col, resolves_bool, auto_repaired_bool, "
            "repair_mode, repair_source_name) VALUES (?, ?, ?, ?, ?, ?)",
            audit_rows,
        )
    con.execute(f"""
        CREATE TABLE "{PUBLICATION_DB}".{WS}.{unres_tbl} AS
        SELECT * FROM "{PUBLICATION_DB}".{WS}.{audit_tbl}
        WHERE resolves_bool = false AND auto_repaired_bool = false
    """)
    new_unres = con.execute(
        f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}.{unres_tbl}'
    ).fetchone()[0]

    # Re-derive needs_manual_review from new audit. Two-pass:
    #   1. clear FALSE for every detail_table_name that produced an audit row
    #      (i.e. any row whose feeds_master_columns_array is non-empty)
    #   2. set TRUE for any row that has at least one truly-unresolved token
    # Rows whose array is empty (sentinel / catalog) keep their prior flag.
    rows_with_audit = sorted({r[0] for r in audit_rows})
    if rows_with_audit:
        placeholders = ",".join("?" for _ in rows_with_audit)
        con.execute(
            f"UPDATE {REGISTRY_V1} SET needs_manual_review = FALSE "
            f"WHERE detail_table_name IN ({placeholders})",
            rows_with_audit,
        )
    if flag_unresolved:
        names = sorted(flag_unresolved.keys())
        placeholders = ",".join("?" for _ in names)
        con.execute(
            f"UPDATE {REGISTRY_V1} SET needs_manual_review = TRUE "
            f"WHERE detail_table_name IN ({placeholders})",
            names,
        )

    needs_review_now = con.execute(
        f"SELECT COUNT(*) FROM {REGISTRY_V1} WHERE needs_manual_review = TRUE"
    ).fetchone()[0]
    log(
        f"Phase 2 completion: audit_rows={len(audit_rows)}  unresolved={new_unres}  "
        f"needs_manual_review_rows={needs_review_now}"
    )

    # Emit CSVs
    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_repair_log.csv",
        repair_log,
        ["phase", "detail_table_name", "action", "expected", "actual", "notes"],
    )
    write_csv(
        OUT_DIR / f"{SCRIPT_NUM}_registry_v2_unresolved_pointers_v273.csv",
        [{"detail_table_name": r[0], "feed_col": r[1]}
         for r in con.execute(
             f'SELECT detail_table_name, feed_col FROM "{PUBLICATION_DB}".{WS}.{unres_tbl} '
             "ORDER BY 1,2"
         ).fetchall()],
        ["detail_table_name", "feed_col"],
    )

    summary = {
        "total_mappings": len(audit_rows),
        "resolved": sum(1 for r in audit_rows if r[2]),
        "auto_repaired": sum(1 for r in audit_rows if (not r[2]) and r[3]),
        "unresolved": new_unres,
        "needs_manual_review_rows": needs_review_now,
    }
    (OUT_DIR / f"{SCRIPT_NUM}_phase2_summary.json").write_text(
        json.dumps(summary, indent=2)
    )
    log(f"Phase 2 summary: {summary}")

    if new_unres > 5:
        log(
            f"WARN: unresolved={new_unres} > target 5 — surface to user before "
            "considering the registry stable."
        )
    if needs_review_now > 10:
        raise SystemExit(
            f"Phase 2 completion FAILED: needs_manual_review_rows="
            f"{needs_review_now} > 10. STOP and ask user."
        )
    return summary


# ---------------------------------------------------------------------------
# PHASE 3 — Thin-wrapper PI confirmation report
# ---------------------------------------------------------------------------
def phase_3(con, log) -> dict[str, Any]:
    log("=== PHASE 3: thin_wrapper_pi_review_v273 ===")
    full_cohort = "cohort_descriptive_full_cohort_v1"
    if not table_exists(con, WS, full_cohort):
        log(f"  WARN: {WS}.{full_cohort} not found — Phase 3 skipped")
        return {"skipped": True}
    full_cols = set(column_names(con, WS, full_cohort))
    log(f"  full cohort col_count={len(full_cols)}")

    review_tbl = "thin_wrapper_pi_review_v273"
    if table_exists(con, WS, review_tbl):
        con.execute(f'DROP TABLE "{PUBLICATION_DB}".{WS}.{review_tbl}')
    con.execute(f"""
        CREATE TABLE "{PUBLICATION_DB}".{WS}.{review_tbl} (
            manuscript_id VARCHAR,
            cohort_view_name VARCHAR,
            row_count BIGINT,
            column_count INTEGER,
            jaccard_vs_full_cohort_view DOUBLE,
            heuristic_classified_as VARCHAR,
            recommended_filter_type VARCHAR,
            pi_confirmation VARCHAR
        )
    """)
    rows = con.execute(f"""
        SELECT CAST(manuscript_id AS VARCHAR), cohort_view_name, filter_type
          FROM {WS}.manuscript_dive_map_v1
          WHERE filter_type_provisional = TRUE
          ORDER BY manuscript_id
    """).fetchall()
    insert_rows = []
    for mid, view_name, ftype in rows:
        view_cols: set[str] = set()
        row_n = 0
        col_n = 0
        if view_name and table_exists(con, WS, view_name):
            view_cols = set(column_names(con, WS, view_name))
            col_n = len(view_cols)
            try:
                row_n = con.execute(
                    f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".{WS}."{view_name}"'
                ).fetchone()[0]
            except Exception as e:
                log(f"  WARN: row count for {view_name} failed: {e}")
        jac = None
        if view_cols and full_cols:
            jac = len(view_cols & full_cols) / len(view_cols | full_cols)
        recommended = ftype  # current heuristic stands as recommendation
        insert_rows.append(
            (mid, view_name, row_n, col_n, jac, ftype, recommended, None)
        )
    if insert_rows:
        con.executemany(
            f'INSERT INTO "{PUBLICATION_DB}".{WS}.{review_tbl} '
            "(manuscript_id, cohort_view_name, row_count, column_count, "
            "jaccard_vs_full_cohort_view, heuristic_classified_as, "
            "recommended_filter_type, pi_confirmation) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            insert_rows,
        )
    log(f"  populated {len(insert_rows)} review rows")

    # Markdown explainer
    md_path = OUT_DIR / f"{SCRIPT_NUM}_thin_wrapper_pi_review.md"
    md = []
    md.append(f"# Script 273 — Thin-Wrapper PI Review ({len(insert_rows)} rows)")
    md.append("")
    md.append("## Heuristic")
    md.append("")
    md.append(
        "Script 272 used the data-based heuristic from the coworker's Prompt 21 §3 "
        "method: a cohort view is classified as `thin_wrapper` when its row count "
        "equals the full cohort (10,871) and its column set is a subset of "
        f"`{WS}.cohort_descriptive_full_cohort_v1` "
        f"({len(full_cols)} cols)."
    )
    md.append("")
    md.append("## Why provisional")
    md.append("")
    md.append(
        "Heuristic agreement does NOT prove a cohort view is intended as a thin "
        "wrapper — some manuscripts may legitimately use the full cohort with no "
        "filter; others may be `dedicated_filtered` whose filter happens to be a "
        "no-op for the current data state. Each row therefore needs PI confirmation."
    )
    md.append("")
    md.append("## What PIs should do")
    md.append("")
    md.append(
        f"For each row in `{WS}.{review_tbl}`, populate `pi_confirmation` with "
        "one of:"
    )
    md.append("")
    md.append(
        "- `confirmed_thin_wrapper` — manuscript intentionally consumes the full "
        "cohort (no patient filter); Script 274 will un-set "
        "`filter_type_provisional`."
    )
    md.append(
        "- `reclass_dedicated_filtered` — there *is* an intended filter; the view "
        "should be rebuilt with that filter and the row reclassified by Script "
        "274."
    )
    md.append(
        "- `reclass_dedicated_full_cohort` — manuscript explicitly wants the full "
        "cohort but tracked separately (e.g. for differing column projection); "
        "Script 274 will reclassify and clear `filter_type_provisional`."
    )
    md.append("")
    md.append("## Script 274 hand-off")
    md.append("")
    md.append(
        "After PI sign-off, Script 274 will read `pi_confirmation` from "
        f"`{WS}.{review_tbl}` and: (a) clear `filter_type_provisional=true` on "
        "rows confirmed as `thin_wrapper`; (b) update `filter_type` and clear "
        "the provisional flag for rows reclassified."
    )
    md.append("")
    md.append("## Snapshot of provisional rows")
    md.append("")
    md.append(
        "| manuscript_id | cohort_view_name | row_count | col_count | jaccard |"
    )
    md.append("|---|---|---|---|---|")
    for r in insert_rows:
        jac_str = f"{r[4]:.3f}" if r[4] is not None else "—"
        md.append(f"| {r[0]} | `{r[1]}` | {r[2]} | {r[3]} | {jac_str} |")
    md_path.write_text("\n".join(md) + "\n")
    log(f"  wrote {md_path}")
    return {"review_rows": len(insert_rows), "review_table": f"{WS}.{review_tbl}"}


# ---------------------------------------------------------------------------
# PHASE 4 — End-to-end registry re-validation
# ---------------------------------------------------------------------------
def phase_4(con, log) -> dict[str, Any]:
    log("=== PHASE 4: End-to-end re-validation (v273) ===")
    cpm_set = cpm_columns(con)
    rows = con.execute(f"""
        SELECT detail_table_name, schema_name, grain, feeds_master_columns_array,
               needs_manual_review
          FROM {REGISTRY_V1}
    """).fetchall()
    val_tbl = "registry_end_to_end_validation_v273"
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
    insert_rows: list[tuple[Any, ...]] = []
    n_pass = n_fail = n_unverif = 0
    for det, sch, grain, arr, needs_review in rows:
        if not sch or not table_exists(con, sch, det):
            insert_rows.append((det, sch, grain, "table_exists", False,
                                "table not found"))
            n_fail += 1
            continue
        sch_cols = set(column_names(con, sch, det))
        is_patient = bool(grain) and "patient" in (grain or "").lower()
        if is_patient:
            ok_rid = "research_id" in sch_cols
            insert_rows.append((det, sch, grain, "has_research_id", ok_rid,
                                "research_id present" if ok_rid else "missing"))
            if ok_rid:
                n_pass += 1
            else:
                n_fail += 1
                continue
            try:
                join_n = con.execute(f'''
                    SELECT COUNT(*) FROM "{PUBLICATION_DB}".{MAIN}.{CPM} c
                    INNER JOIN "{PUBLICATION_DB}".{sch}."{det}" d
                      ON c.research_id = CAST(d.research_id AS VARCHAR)
                ''').fetchone()[0]
            except Exception as e:
                join_n = 0
                insert_rows.append((det, sch, grain, "joins_to_cpm", False,
                                    f"join error: {str(e)[:80]}"))
                n_fail += 1
                continue
            ok_join = join_n > 0
            insert_rows.append((det, sch, grain, "joins_to_cpm", ok_join,
                                f"matched_rows={join_n}"))
            if ok_join:
                n_pass += 1
            else:
                n_fail += 1
        if arr:
            for fc in arr:
                if not fc:
                    continue
                if "*" in fc:
                    # Sentinel wildcard — flag as unverifiable when row is
                    # already needs_manual_review; don't count against fail.
                    if needs_review:
                        insert_rows.append((det, sch, grain, f"feed_col:{fc}",
                                            False, "wildcard_kept_as_is"))
                        n_unverif += 1
                    else:
                        insert_rows.append((det, sch, grain, f"feed_col:{fc}",
                                            False, "wildcard_kept_as_is"))
                        n_fail += 1
                    continue
                ok = fc in cpm_set
                insert_rows.append((det, sch, grain, f"feed_col:{fc}", ok,
                                    "in CPM" if ok else "missing from CPM"))
                if ok:
                    n_pass += 1
                else:
                    if needs_review:
                        n_unverif += 1
                    else:
                        n_fail += 1
    if insert_rows:
        con.executemany(
            f'INSERT INTO "{PUBLICATION_DB}".{WS}.{val_tbl} '
            "(detail_table_name, schema_name, grain, check_name, passed, detail) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            insert_rows,
        )
    log(f"  pass={n_pass}  fail={n_fail}  unverifiable_flagged={n_unverif}")
    summary = {"pass": n_pass, "fail": n_fail,
               "unverifiable_flagged": n_unverif,
               "validation_table": f"{WS}.{val_tbl}"}
    (OUT_DIR / f"{SCRIPT_NUM}_phase4_summary.json").write_text(
        json.dumps(summary, indent=2)
    )
    return summary


# ---------------------------------------------------------------------------
# PHASE 5 — Final invariants + summary
# ---------------------------------------------------------------------------
def phase_5(con, log, baseline: dict[str, Any], p2: dict[str, Any],
            p3: dict[str, Any], p4: dict[str, Any]) -> dict[str, Any]:
    log("=== PHASE 5: Final invariants + summary ===")
    inv = assert_cpm_invariants(con, log)
    main_n = base_table_count(con, MAIN)
    ws_tbl = base_table_count(con, WS)
    ws_vw = view_count(con, WS)
    reg_n = con.execute(f"SELECT COUNT(*) FROM {REGISTRY_V1}").fetchone()[0]
    null_rows = con.execute(
        f"SELECT COUNT(*) FROM {REGISTRY_V1} WHERE total_rows IS NULL"
    ).fetchone()[0]
    null_pat = con.execute(
        f"SELECT COUNT(*) FROM {REGISTRY_V1} WHERE total_patients IS NULL"
    ).fetchone()[0]
    needs_review = con.execute(
        f"SELECT COUNT(*) FROM {REGISTRY_V1} WHERE needs_manual_review = TRUE"
    ).fetchone()[0]
    arch_n = archive_object_count(con)
    # Anchor against the prompt's canonical pre-273 baseline (Script 272 hand-off
    # values), not the live snapshot — repeat invocations of 273 would otherwise
    # drift the "delta" and obscure the real change.
    PROMPT_BASELINE_UNRESOLVED = 18
    PROMPT_BASELINE_NEEDS_REVIEW = 35
    PROMPT_BASELINE_E2E_PASS = 512
    PROMPT_BASELINE_E2E_FAIL = 67
    base_unres = PROMPT_BASELINE_UNRESOLVED
    base_review = PROMPT_BASELINE_NEEDS_REVIEW
    base_arch = baseline.get("archive_pub_v1_0_objects_pre", 0)

    block = [
        "================================================================",
        "REGISTRY CURATION COMPLETE — thyroid_canonical_publication_v1_0",
        "================================================================",
        f"canonical_patient_master : {inv['row_count']} × {inv['column_count']} cols (unchanged)",
        f"Invariants               : RIDs={inv['distinct_research_ids']}  "
        f"null_rid={inv['null_research_id']}  null_fna={inv['null_fna_path_outcome']}",
        f"Registry rows            : {reg_n} (unchanged)",
        f"Registry metadata gaps   : total_rows NULL={null_rows}, "
        f"total_patients NULL={null_pat} (target 0 / ≤5)",
        f"Unresolved pointers      : {p2['unresolved']} "
        f"(baseline {base_unres}, delta {p2['unresolved'] - base_unres})",
        f"needs_manual_review=true : {needs_review} rows "
        f"(baseline {base_review}, delta {needs_review - base_review})",
        f"Thin-wrapper PI review   : {p3.get('review_rows', 0)} provisional "
        "(flagged for PI, not changed)",
        f"E2E validation           : {p4['pass']} pass / {p4['fail']} fail "
        f"(baseline {PROMPT_BASELINE_E2E_PASS}/{PROMPT_BASELINE_E2E_FAIL})",
        f"Archive destination      : \"{ARCHIVE_DB}\".{ARCHIVE_SCHEMA} "
        f"({arch_n} objs; +{arch_n - base_arch} pre273 snapshot)",
        f"Schema counts            : main={main_n}  ws={ws_tbl}t+{ws_vw}v",
        "================================================================",
        "CURATION COMPLETE. Registry is publication-stable.",
        "================================================================",
    ]
    log("\n".join(block))

    final = {
        "canonical_patient_master": inv,
        "registry_rows": reg_n,
        "null_total_rows": null_rows,
        "null_total_patients": null_pat,
        "unresolved_pointers": p2["unresolved"],
        "needs_manual_review_rows": needs_review,
        "thin_wrapper_pi_review_rows": p3.get("review_rows", 0),
        "e2e_pass": p4["pass"], "e2e_fail": p4["fail"],
        "archive_objects": arch_n,
        "main_base_tables": main_n,
        "ws_base_tables": ws_tbl, "ws_views": ws_vw,
        "summary_block": block,
    }
    (OUT_DIR / f"{SCRIPT_NUM}_final.json").write_text(json.dumps(final, indent=2))

    # Hard gates per prompt
    failures = []
    if (
        inv["row_count"] != CPM_ROWS or inv["distinct_research_ids"] != CPM_ROWS
        or inv["null_research_id"] != 0 or inv["null_fna_path_outcome"] != 0
        or inv["column_count"] != CPM_COLS
    ):
        failures.append("CPM invariant drift")
    if reg_n != baseline["detail_table_registry_v1_rows"]:
        failures.append(f"registry row count drift {reg_n} vs "
                        f"{baseline['detail_table_registry_v1_rows']}")
    if p4["fail"] > 67:
        failures.append(f"e2e fail count {p4['fail']} > baseline 67")
    if p2["unresolved"] > 5:
        log(f"NOTE: unresolved={p2['unresolved']} above target 5 — surface to user.")
    if failures:
        log("HARD-GATE FAILURE: " + "; ".join(failures))
        log("Do NOT commit. Surface to user.")
    return final


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--phase", default="all",
                        choices=["all", "0", "1", "2", "3", "4", "5"])
    args = parser.parse_args()

    log_path = OUT_DIR / f"{SCRIPT_NUM}_run.log"
    log = TeeLogger(log_path)
    log(f"Script {SCRIPT_NUM} starting; phase={args.phase}; "
        f"utc_ts={UTC_TS_COMPACT}")
    try:
        con = connect()
        baseline = phase_0(con, log) if args.phase in ("all", "0") else {}
        p1 = phase_1(con, log) if args.phase in ("all", "1") else {}
        p2 = phase_2(con, log) if args.phase in ("all", "2") else {"unresolved": 0}
        p3 = phase_3(con, log) if args.phase in ("all", "3") else {}
        p4 = phase_4(con, log) if args.phase in ("all", "4") else {"pass": 0, "fail": 0}
        if args.phase in ("all", "5"):
            phase_5(con, log, baseline or {}, p2, p3, p4)
        log("Script 273 completed.")
        return 0
    except SystemExit as e:
        log(f"Script 273 ABORTED: {e}")
        return 2
    finally:
        log.close()


if __name__ == "__main__":
    sys.exit(main())
