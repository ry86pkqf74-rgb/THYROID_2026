#!/usr/bin/env python3
"""Script 389 — US zombie cleanup + view-body rewrites + complications aggregation audit.

Driver for cursor prompt
``cursor_prompts/CURSOR_PROMPT_US_ZOMBIE_VIEW_REWRITES_COMPLICATIONS_AUDIT_20260422_SCRIPT_389.md``.

Two structural fixes + one read-only audit packaged into one wave:

1.  **`canonical_us_nodule_v2` source-flag partition probe** (RETIRED
    Phase 0B classifier 2026-04-22).  The original prompt's baselines
    (18,310 / 17,090 / 2,152 / 27 = ``clean_llm_parsed`` /
    ``clean_non_llm`` / ``zombie_parent`` / ``llm_parsed_but_blob``)
    were not reproducible against live state — direct MotherDuck probe
    showed no combination of source-flags or ``location_raw`` content
    signals yields that partition.  The classifier is now grounded in
    the four boolean flags that actually do partition the table, with
    baselines frozen from the 2026-04-22 probe (26,402 / 8,919 / 2,117
    / 141 = ``clean_dual_source`` / ``clean_base_only`` /
    ``needs_backfill`` / ``aggregate_rollup``).  None of the four
    buckets are "zombie" in the structural sense; the prior Phase 2
    DELETE step is replaced by a single read-only INSERT into
    ``main.__readme`` documenting the reset.  If a content-based
    multi-nodule-blob audit is still wanted, draft as Script 389b.
2.  **US view-stack phantoms** — rewrite ``canonical_us_exam_master_VIEW_v2``
    to filter NULL ``exam_date`` rows in the upstream source CTEs
    (~6,792 phantom ``(research_id, NULL)`` pairs eliminated; CPM CTE
    retained — it's load-bearing for ``is_preop_exam``, NOT a row-shape
    driver), and rewrite ``canonical_us_patient_master_VIEW_v2`` to
    replace the hardcoded ``CAST('t' AS BOOLEAN) AS has_any_us`` with a
    real derivation.
3.  **Complications rollup over-aggregation** — rebuild
    ``canonical_complications_patient_rollup_v1`` under one of three
    operator-selected reconciliation rules:
      * Rule A — status-aware: any_evidence requires ``finding_status='present'``.
      * Rule B — kind-weighted: any_evidence excludes
        ``source_kind='entity_legacy' + source_evidence_type='nlp_proxy'``.
      * Rule C — A AND B (strictest).

Phases
------
* **Phase 0** (read-only, default) — preflight + re-probe + audit; writes
  ``scripts/output/389_prestate_probe_report.md`` and halts.
* **Phase 1** (``--apply`` without approval file) — print plan summary,
  ask operator to write ``scripts/output/389_plan_approval.txt`` with
  ``RULE=A|B|C`` and re-run.
* **Phase 2** (``--apply`` + approval file) — execute all destructive ops
  consumer-first.
* **Phase 3** — post-state verification + ``389_close_out_report.md``.

Safety
------
* Default mode runs Phase 0 only.
* Every drop / rebuild is preceded by an archive-DB CTAS parity check.
* Idempotent: re-running ``--apply`` skips already-archived / already-built
  objects via the per-script log table.
* PHI-safe: never reads / logs ``location_raw``, ``evidence_text``,
  ``source_row_id`` bodies, or any clinical text.  Only ``research_id``,
  bucket counts, and hashes appear in logs.
* Archive zone is **PUB-resident** ``archive_pub_v1_0`` schema (matches
  the actual 387/388 landing pattern, despite their close-out memos
  saying ``"Thyroid 2026 UPdated"`` — the cross-DB CTAS path was never
  taken; everything lives inside PUB under ``*_legacy_<stamp>`` /
  ``archive_pub_v1_0``).  Rollup rebuilds archive the prior body to
  ``archive_pub_v1_0`` first; restore via CTAS-back if regression
  detected.

Auth: ``motherduck_client.get_token()``.  Token never printed.
"""
from __future__ import annotations

import argparse
import json
import re
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

# Archive zone is PUB-resident — direct state probe on 2026-04-22 confirmed
# 387/388 landed all archives inside PUB under their `*_legacy_<stamp>` /
# `archive_pub_v1_0` schemas, NOT in `"Thyroid 2026 UPdated"` despite the
# close-out memos.  389 follows the actual pattern.
ARC_DB_RAW = PUB_DB
ARC_DB = f'"{PUB_DB}"'

# Cross-DB archive (informational only — populated when the user *also*
# wants to mirror to the external archive DB; currently unused).
EXTERNAL_ARC_DB_RAW = "Thyroid 2026 UPdated"

WS_SCHEMA = "manuscript_workspace"
SCRIPT_TAG = "389_us_zombie_view_rewrites_and_complications_audit"
SCRIPT_ID = "389"
RUN_STAMP = datetime.now(timezone.utc).strftime("%Y%m%d")  # e.g. 20260422

# Archive destinations (per-script suffix keeps rollback unambiguous).
ARC_PUB_SCHEMA = "archive_pub_v1_0"  # mirrors 388 snapshot pattern (PUB-resident)
# (ARC_ZOMBIE_DELETED retired 2026-04-22 with the zombie-DELETE phase.)
ARC_COMPLICATIONS_LEGACY = (
    f"canonical_complications_patient_rollup_v1_legacy_{RUN_STAMP}"
)
ARC_EXAM_MASTER_LEGACY = (
    f"canonical_us_exam_master_VIEW_v2_legacy_{RUN_STAMP}"
)
ARC_PATIENT_MASTER_LEGACY = (
    f"canonical_us_patient_master_VIEW_v2_legacy_{RUN_STAMP}"
)

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
LOG_PATH = OUTPUT_DIR / "389_run.log"
PRESTATE_PATH = OUTPUT_DIR / "389_prestate_probe_report.md"
CLOSEOUT_PATH = OUTPUT_DIR / "389_close_out_report.md"
APPROVAL_PATH = OUTPUT_DIR / "389_plan_approval.txt"

# --- Bug 1 (RETIRED): canonical_us_nodule_v2 partition probe -------------- #
#
# Phase 0B classifier reset 2026-04-22.  The original prompt's baselines
# (18,310 / 17,090 / 2,152 / 27 = clean_llm_parsed / clean_non_llm /
# zombie_parent / llm_parsed_but_blob) were not reproducible from live
# state — direct MotherDuck probe (this session) showed no combination
# of source-flags or `location_raw` content signals yields that
# partition.  Treating those as phantom from compressed context.
#
# The replacement classifier is a 4-bucket partition derived purely
# from existing boolean flags on the table — the only signals that
# partition the row set consistently and reproducibly.  None of the
# four buckets are "zombie" in the structural sense; `needs_backfill`
# rows are legitimate entries awaiting NLP extraction.  The Phase 2
# DELETE step that the prior classifier targeted is RETIRED; the new
# Phase 2 zombie-step writes a single provenance row to `main.__readme`
# (see phase2_classifier_provenance below).
EXPECTED_BUCKETS: dict[str, int] = {
    "clean_dual_source":  26_402,   # source_tirads_llm=T AND source_base=T
    "clean_base_only":     8_919,   # exactly one of (tirads_llm, base) TRUE
    "needs_backfill":      2_117,   # neither flag set (2,061 pending + 56 orphan)
    "aggregate_rollup":      141,   # is_aggregate_row=TRUE
}
EXPECTED_TOTAL = 37_579             # sum of all four buckets
BUCKET_DRIFT_TOLERANCE = 0.02       # ±2%; per-bucket gate

# Required columns for the source-flag classifier.  Probed at runtime;
# halts if any are missing.
NODULE_CLASSIFIER_REQUIRED_COLS: tuple[str, ...] = (
    "is_aggregate_row",
    "source_tirads_llm",
    "source_base",
    "nlp_backfill_pending",
)

# --- Bug 2: view-stack rewrites ------------------------------------------- #
US_EXAM_MASTER_VIEW = "canonical_us_exam_master_VIEW_v2"
US_PATIENT_MASTER_VIEW = "canonical_us_patient_master_VIEW_v2"
# Sentinel literal that flags the hardcoded has_any_us bug.  Phase 0C
# halts if ABSENT in the patient_master view body (means patch plan is
# stale).  We accept either ``CAST('t' AS BOOLEAN)`` (DuckDB serialises
# this from `TRUE`) or a literal `TRUE` — both indicate the same bug.
HAS_ANY_US_BUG_LITERAL = "CAST('t' AS BOOLEAN) AS has_any_us"
HAS_ANY_US_BUG_LITERAL_ALT = "TRUE AS has_any_us"
# Informational marker — the CPM CTE in exam_master is load-bearing for
# the `is_preop_exam` column (NOT a row-shape driver).  We surface its
# presence/absence in the prestate report but do NOT treat it as a bug
# signal.  The actual phantom driver (per 2026-04-22 direct probe) is
# upstream NULL `exam_date` rows in the gland + nodule source tables —
# see US_SOURCE_TABLES below.
CPM_LOAD_BEARING_MARKERS: tuple[str, ...] = (
    "FROM canonical_patient_master cpm",
    "FROM main.canonical_patient_master cpm",
    f'FROM "{PUB_DB}".main.canonical_patient_master cpm',
    "FROM canonical_patient_master AS cpm",
    "FROM main.canonical_patient_master AS cpm",
    "canonical_patient_master cp",  # 366-style alias
    "canonical_patient_master AS cp",
)

# Source tables for canonical_us_exam_master_VIEW_v2.  Phase 0C probes
# each for NULL `exam_date` row counts; expected values are the
# 2026-04-22 baseline.  6,785 (gland) + 2,231 (nodule) - overlap =
# 6,792 phantom (research_id, NULL) pairs after UNION.
US_SOURCE_TABLES: tuple[tuple[str, int], ...] = (
    ("canonical_us_thyroid_gland_v2", 6_785),
    ("canonical_us_nodule_v2",        2_231),
    ("canonical_us_lymph_node_v2",        0),
)
EXPECTED_PHANTOM_ROWS = 6_792
EXPECTED_PATIENT_BUG_ROWS = 6_499  # has_any_us=TRUE, first_us_date NULL,
                                   # last_us_date NULL
PHANTOM_DRIFT_TOLERANCE = 0.05    # ±5% (view bodies are higher variance)

# --- Bug 3: complications rollup ------------------------------------------ #
COMPLICATION_TYPES: tuple[str, ...] = (
    "rln_injury",
    "vocal_cord_paralysis",
    "hypocalcemia_clinical",
    "hypoparathyroidism",
    "hematoma",
    "seroma",
    "chyle_leak",
    "wound_infection",
    "pneumothorax",
    "airway_complication",
    "wound_dehiscence",
    "mortality",
)
ROLLUP_TIERS: tuple[str, ...] = (
    "definitive", "probable_or_better", "any_evidence",
)
TEMPORAL_TYPES_FOR_ROLLUP: tuple[str, ...] = (
    "hypoparathyroidism", "hypocalcemia_clinical",
)
PREOP_PROXIMITY_BUFFER_DAYS = 30
TEMPORAL_RESOLUTION_WINDOW_DAYS = 180
LEGACY_NLP_PROXY_FILTER = (
    "NOT (source_kind = 'entity_legacy' "
    "AND source_evidence_type = 'nlp_proxy')"
)
RULE_NAMES = ("A", "B", "C")
AUDIT_PATIENT_RID = "9340"  # case study: prompt §0 / Phase 3B

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


def warn(msg: str) -> None:
    line = f"[WARN] [{_ts()}] {msg}"
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
            f"No MotherDuck RW token (token_mode={token_mode()}).  "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    log(f"Connecting md:{PUB_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{PUB_DB}?motherduck_token={tok}")
    con.execute(f'USE "{PUB_DB}"')
    con.execute(f'USE "{PUB_DB}".main')

    dbs = {
        r[0] for r in con.execute(
            "SELECT database_name FROM duckdb_databases()"
        ).fetchall()
    }
    if PUB_DB not in dbs:
        raise SystemExit(f"PUB DB '{PUB_DB}' not attached")
    if EXTERNAL_ARC_DB_RAW not in dbs:
        log(
            f"  note: external archive DB '{EXTERNAL_ARC_DB_RAW}' not "
            "attached — that's fine; archive zone for 389 is PUB-resident "
            f"({PUB_DB}.{ARC_PUB_SCHEMA})."
        )

    cpm_n = con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}".main.canonical_patient_master'
    ).fetchone()[0]
    if cpm_n != 10_871:
        raise SystemExit(
            f"canonical_patient_master row count {cpm_n} != 10,871; aborting"
        )
    log(f"Connection OK (CPM rows={cpm_n})")
    return con


# --------------------------------------------------------------------------- #
# SQL helpers
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


def get_columns(
    con, table: str, schema: str = "main", db: str = PUB_DB,
) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [db, schema, table],
    ).fetchall()
    return [r[0] for r in rows]


def column_exists(
    con, table: str, column: str, schema: str = "main", db: str = PUB_DB,
) -> bool:
    return column in get_columns(con, table, schema=schema, db=db)


def row_count(con, schema: str, name: str, db: str = PUB_DB) -> int:
    return con.execute(
        f'SELECT COUNT(*) FROM "{db}"."{schema}"."{name}"'
    ).fetchone()[0]


def view_body(con, schema: str, name: str, db: str = PUB_DB) -> str | None:
    row = con.execute(
        """
        SELECT view_definition FROM information_schema.views
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [db, schema, name],
    ).fetchone()
    return row[0] if row else None


_CREATE_VIEW_PREFIX_RE = re.compile(
    r"^\s*CREATE(?:\s+OR\s+REPLACE)?\s+(?:TEMPORARY\s+|TEMP\s+)?VIEW\s+"
    r"(?:[A-Za-z_][A-Za-z0-9_]*\.)?(?:[A-Za-z_][A-Za-z0-9_]*\.)?"
    r"\"?[A-Za-z_][A-Za-z0-9_]*\"?\s+AS\s+",
    re.IGNORECASE,
)


def view_select_body(
    con, schema: str, name: str, db: str = PUB_DB,
) -> str | None:
    """Return the SELECT body of a view (no leading `CREATE VIEW … AS`).

    `information_schema.views.view_definition` in MotherDuck/DuckDB
    returns the *full* CREATE statement (`CREATE VIEW <name> AS WITH …`).
    For `CREATE OR REPLACE VIEW {target} AS {body}` re-wraps we need to
    strip the leading prefix; otherwise we double-CREATE and get a
    parser error.  Trailing semicolons are also stripped.
    """
    raw = view_body(con, schema, name, db=db)
    if raw is None:
        return None
    stripped = _CREATE_VIEW_PREFIX_RE.sub("", raw, count=1).rstrip()
    if stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    return stripped


def find_view_dependents(
    con, target_name: str, exclude_self: bool = True,
) -> list[tuple[str, str]]:
    """Return (schema, view_name) for every VIEW whose body references target_name."""
    rows = con.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.views
        WHERE table_catalog = ?
          AND view_definition ILIKE ?
        ORDER BY table_schema, table_name
        """,
        [PUB_DB, f"%{target_name}%"],
    ).fetchall()
    out: list[tuple[str, str]] = []
    for s, n in rows:
        if exclude_self and s == "main" and n == target_name:
            continue
        out.append((s, n))
    return out


def schema_object_counts(con, schema: str) -> tuple[int, int]:
    rows = con.execute(
        """
        SELECT table_type, COUNT(*) FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
        GROUP BY table_type
        """,
        [PUB_DB, schema],
    ).fetchall()
    counts = dict(rows)
    return counts.get("BASE TABLE", 0), counts.get("VIEW", 0)


def total_pub_objects(con) -> int:
    rows = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog = ?
        """,
        [PUB_DB],
    ).fetchone()[0]
    return int(rows)


def archive_present(con, schema: str, name: str) -> int | None:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [ARC_DB_RAW, schema, name],
    ).fetchone()
    if not row:
        return None
    return int(con.execute(
        f'SELECT COUNT(*) FROM {ARC_DB}."{schema}"."{name}"'
    ).fetchone()[0])


# --------------------------------------------------------------------------- #
# Workspace tables (per-script log)
# --------------------------------------------------------------------------- #


def ensure_workspace_tables(con) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {WS_SCHEMA}.script_389_archive_move_log_v1 (
            script_id      VARCHAR,
            move_ts        TIMESTAMP,
            phase          VARCHAR,
            source_schema  VARCHAR,
            source_name    VARCHAR,
            dest_db        VARCHAR,
            dest_schema    VARCHAR,
            dest_name      VARCHAR,
            move_method    VARCHAR,
            n_rows         BIGINT,
            reason         VARCHAR
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {WS_SCHEMA}.script_389_prestate_v1 (
            kind          VARCHAR,
            target        VARCHAR,
            metric        VARCHAR,
            n             BIGINT,
            details       VARCHAR,
            build_ts      TIMESTAMP
        )
    """)


def log_move(
    con,
    *,
    phase: str,
    source_schema: str,
    source_name: str,
    dest_schema: str,
    dest_name: str,
    move_method: str,
    n_rows: int | None,
    reason: str,
    dest_db: str = ARC_DB_RAW,
) -> None:
    con.execute(
        f"""
        INSERT INTO {WS_SCHEMA}.script_389_archive_move_log_v1
        (script_id, move_ts, phase, source_schema, source_name,
         dest_db, dest_schema, dest_name, move_method, n_rows, reason)
        VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?, ?,
                ?, ?, ?, ?, ?, ?)
        """,
        [SCRIPT_ID, phase, source_schema, source_name,
         dest_db, dest_schema, dest_name, move_method, n_rows, reason],
    )


def already_logged(
    con, *, phase: str, source_name: str, dest_name: str,
) -> bool:
    row = con.execute(
        f"""
        SELECT 1 FROM {WS_SCHEMA}.script_389_archive_move_log_v1
        WHERE script_id = ?
          AND phase = ?
          AND source_name = ?
          AND dest_name = ?
        LIMIT 1
        """,
        [SCRIPT_ID, phase, source_name, dest_name],
    ).fetchone()
    return row is not None


# --------------------------------------------------------------------------- #
# Phase 0A — preflight
# --------------------------------------------------------------------------- #


def phase0a_preflight(con, ignore_preflight: bool) -> dict[str, Any]:
    log("=" * 78)
    log("Phase 0A — preflight (386 & 388 evidence + PUB count)")
    log("=" * 78)

    # 388 evidence — script_388_archive_move_log_v1 must exist with rows.
    has_388_log = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name = 'script_388_archive_move_log_v1'
        """,
        [PUB_DB, WS_SCHEMA],
    ).fetchone() is not None
    n_388 = (
        int(con.execute(
            f"SELECT COUNT(*) FROM {WS_SCHEMA}.script_388_archive_move_log_v1"
        ).fetchone()[0])
        if has_388_log else 0
    )

    # 386 evidence — 386 wrote within manuscript_workspace too.  We accept
    # any of: a 386-tagged script_386_* workspace table OR an
    # archive_move_log_v1 row whose `script` column references 386 OR a
    # close-out file on disk.
    n_386_log = 0
    has_386_legacy_log = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name = 'archive_move_log_v1'
        """,
        [PUB_DB, WS_SCHEMA],
    ).fetchone() is not None
    if has_386_legacy_log:
        n_386_log = int(con.execute(
            f"SELECT COUNT(*) FROM {WS_SCHEMA}.archive_move_log_v1 "
            f"WHERE script ILIKE '%386%' OR script ILIKE '%387%'"
        ).fetchone()[0])
    n_386_ws = int(con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name LIKE 'script_386_%'
        """,
        [PUB_DB, WS_SCHEMA],
    ).fetchone()[0])
    has_386_close_out = (REPO_ROOT / "scripts" / "output" / "386_close_out.md"
                         ).exists()

    pub_count = total_pub_objects(con)
    log(f"  PUB total objects: {pub_count}")
    log(f"  388 log rows: {n_388} (table present: {has_388_log})")
    log(f"  386 evidence — workspace tables: {n_386_ws}, "
        f"legacy log rows: {n_386_log}, close_out present: "
        f"{has_386_close_out}")

    missing: list[str] = []
    if not has_388_log or n_388 == 0:
        missing.append("388 (no script_388_archive_move_log_v1 rows)")
    if n_386_ws == 0 and n_386_log == 0 and not has_386_close_out:
        missing.append(
            "386 (no script_386_* workspace, no 386 log row, no close-out)"
        )

    if missing and not ignore_preflight:
        raise SystemExit(
            "Preflight FAIL: missing evidence for "
            f"{', '.join(missing)}.  Run those scripts first or override "
            "with --ignore-preflight."
        )

    return {
        "pub_object_count": pub_count,
        "n_388_log_rows": n_388,
        "n_386_workspace_tables": n_386_ws,
        "n_386_legacy_log_rows": n_386_log,
        "has_386_close_out": has_386_close_out,
        "preflight_missing": missing,
    }


# --------------------------------------------------------------------------- #
# Phase 0B — US nodule zombie classification
# --------------------------------------------------------------------------- #


def _check_classifier_columns(con) -> None:
    cols = set(get_columns(con, "canonical_us_nodule_v2"))
    missing = [c for c in NODULE_CLASSIFIER_REQUIRED_COLS if c not in cols]
    if missing:
        raise SystemExit(
            "Phase 0B abort: canonical_us_nodule_v2 missing required "
            f"classifier column(s): {missing}.  Update "
            "NODULE_CLASSIFIER_REQUIRED_COLS / classifier expression."
        )


def _classifier_case_sql(alias: str | None = None) -> str:
    """Source-flag partition of canonical_us_nodule_v2 (4 disjoint buckets).

    Buckets sum to 37,579 (2026-04-22 baseline):
      * clean_dual_source : both sources parsed, non-aggregate           (26,402)
      * clean_base_only   : exactly one of {tirads_llm, base} TRUE        (8,919)
      * needs_backfill    : neither flag set; pending or orphan           (2,117)
      * aggregate_rollup  : per-exam aggregate row, not per-nodule          (141)

    The optional `alias` parameter qualifies column references when the
    expression is embedded in a JOIN context where bare column names
    would be ambiguous.  When called bare it produces unqualified
    column references, which is correct for the Phase 0B / Phase 3
    single-table SELECT contexts.
    """
    qual = f"{alias}." if alias else ""
    return f"""
    CASE
      WHEN {qual}is_aggregate_row = TRUE
        THEN 'aggregate_rollup'
      WHEN {qual}source_tirads_llm = TRUE AND {qual}source_base = TRUE
        THEN 'clean_dual_source'
      WHEN ({qual}source_tirads_llm = TRUE
            AND COALESCE({qual}source_base, FALSE) = FALSE)
        OR (COALESCE({qual}source_tirads_llm, FALSE) = FALSE
            AND {qual}source_base = TRUE)
        THEN 'clean_base_only'
      ELSE
        'needs_backfill'
    END
    """


def _drift_within(actual: int, expected: int, tol: float) -> bool:
    if expected == 0:
        return actual == 0
    return abs(actual - expected) / expected <= tol


def phase0b_zombie_reprobe(con) -> dict[str, Any]:
    """Phase 0B — `canonical_us_nodule_v2` source-flag partition probe.

    Function name retained for back-compat with prior call sites; the
    "zombie" concept is RETIRED 2026-04-22 and the probe now reports
    a 4-bucket partition derived from boolean source flags.  See module
    docstring + the EXPECTED_BUCKETS comment for the reset rationale.
    """
    log("=" * 78)
    log("Phase 0B — canonical_us_nodule_v2 source-flag partition probe")
    log("=" * 78)
    _check_classifier_columns(con)

    case_sql = _classifier_case_sql()
    rows = con.execute(
        f"""
        SELECT bucket, COUNT(*) AS n
        FROM (
            SELECT {case_sql} AS bucket
            FROM main.canonical_us_nodule_v2
        ) t
        GROUP BY bucket
        ORDER BY bucket
        """
    ).fetchall()
    buckets: dict[str, int] = {r[0]: int(r[1]) for r in rows}
    actual_total = sum(buckets.values())
    log(f"  bucket counts: {buckets} (total={actual_total:,})")

    drift_fail: list[str] = []
    for name, expected in EXPECTED_BUCKETS.items():
        actual = buckets.get(name, 0)
        ok = _drift_within(actual, expected, BUCKET_DRIFT_TOLERANCE)
        delta_pct = (
            ((actual - expected) / expected * 100.0) if expected else 0.0
        )
        log(f"    {name:<22} actual={actual:>7,} expected={expected:>7,} "
            f"Δ={delta_pct:+.2f}%  {'ok' if ok else 'DRIFT'}")
        if not ok:
            drift_fail.append(
                f"{name}: actual={actual} expected={expected} "
                f"(>{BUCKET_DRIFT_TOLERANCE:.0%} drift)"
            )

    # Total-row sanity check (independent of the per-bucket gate).
    total_ok = _drift_within(
        actual_total, EXPECTED_TOTAL, BUCKET_DRIFT_TOLERANCE,
    )
    log(f"    {'TOTAL':<22} actual={actual_total:>7,} "
        f"expected={EXPECTED_TOTAL:>7,} {'ok' if total_ok else 'DRIFT'}")
    if not total_ok:
        drift_fail.append(
            f"TOTAL: actual={actual_total} expected={EXPECTED_TOTAL}"
        )

    if drift_fail:
        raise SystemExit(
            "Phase 0B abort: source-flag partition has drifted from the "
            "2026-04-22 baseline.\n  " + "\n  ".join(drift_fail)
        )
    log("  drift gate : PASS")

    return {
        "buckets": buckets,
        "actual_total": actual_total,
        "classifier_sql": case_sql.strip(),
    }


# --------------------------------------------------------------------------- #
# Phase 0C — view-stack body probe
# --------------------------------------------------------------------------- #


def _has_marker(body: str, markers: tuple[str, ...]) -> bool:
    body_lc = body.lower()
    for m in markers:
        if m.lower() in body_lc:
            return True
    return False


# Census every hardcoded boolean literal in a view body so the operator
# can spot latent bugs like the original `has_any_us=TRUE`.  Returns a
# list of (literal_text, immediately_following_alias) tuples.  The
# alias is the name after a nearby ``AS <name>`` (within ~64 chars
# downstream of the literal); ``None`` means no alias on the same column.
_BOOL_LITERAL_RE = re.compile(
    r"CAST\s*\(\s*'[tf]'\s+AS\s+BOOLEAN\s*\)"
    r"|\bTRUE\b|\bFALSE\b",
    re.IGNORECASE,
)
_FOLLOWING_ALIAS_RE = re.compile(
    r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)


def _bool_literal_census(body: str) -> list[dict[str, Any]]:
    """Enumerate boolean literals in a view body with downstream alias hints.

    Surfaced for Phase 0C reporting; informational, never gating.
    """
    out: list[dict[str, Any]] = []
    for m in _BOOL_LITERAL_RE.finditer(body):
        # Look ahead up to 64 chars for an `AS <alias>` and grab the
        # nearest one — heuristic, but reliable for one-line column
        # definitions in DuckDB-serialised view bodies.
        tail = body[m.end():m.end() + 64]
        alias_match = _FOLLOWING_ALIAS_RE.search(tail)
        out.append({
            "literal": m.group(0),
            "downstream_alias": (
                alias_match.group(1) if alias_match else None
            ),
            "offset": m.start(),
        })
    return out


def phase0c_view_probe(con) -> dict[str, Any]:
    log("=" * 78)
    log("Phase 0C — US view-stack body probe")
    log("=" * 78)

    exam_body = view_body(con, "main", US_EXAM_MASTER_VIEW)
    pat_body = view_body(con, "main", US_PATIENT_MASTER_VIEW)
    if not exam_body:
        raise SystemExit(
            f"Phase 0C abort: main.{US_EXAM_MASTER_VIEW} missing or not a VIEW"
        )
    if not pat_body:
        raise SystemExit(
            f"Phase 0C abort: main.{US_PATIENT_MASTER_VIEW} missing or not a VIEW"
        )

    cpm_load_bearing = _has_marker(exam_body, CPM_LOAD_BEARING_MARKERS)
    has_has_any_us_bug = (
        HAS_ANY_US_BUG_LITERAL in pat_body
        or HAS_ANY_US_BUG_LITERAL_ALT in pat_body
    )

    log(f"  exam_master CPM CTE present (load-bearing for is_preop_exam): "
        f"{cpm_load_bearing}")
    log(f"  patient_master hardcoded has_any_us literal present: "
        f"{has_has_any_us_bug}")

    # The has_any_us literal is the patcher's only handle in Phase 2E —
    # halt if absent so we don't silently no-op.
    if not has_has_any_us_bug:
        raise SystemExit(
            "Phase 0C abort: hardcoded 'has_any_us=TRUE' literal not "
            f"found in {US_PATIENT_MASTER_VIEW}.  View has changed under "
            "us; review before re-running."
        )

    # Phantom counts — defensive: enumerate findings columns from the
    # current view's column list rather than hardcoding.
    exam_cols = get_columns(con, US_EXAM_MASTER_VIEW)
    finding_cols = [
        c for c in exam_cols
        if c.endswith("_findings") or c.startswith("has_") or c.startswith("n_")
    ]
    nullness_predicates = " AND ".join(
        f"\"{c}\" IS NULL" for c in finding_cols
    )
    phantom_sql = (
        f'SELECT COUNT(*) FROM "{PUB_DB}".main."{US_EXAM_MASTER_VIEW}" '
        f'WHERE exam_date IS NULL'
    )
    if finding_cols:
        phantom_sql += f" AND ({nullness_predicates})"
    n_phantoms = int(con.execute(phantom_sql).fetchone()[0])
    n_exam_total = row_count(con, "main", US_EXAM_MASTER_VIEW)
    log(f"  exam_master rows total={n_exam_total:,} "
        f"phantoms (NULL exam_date + NULL findings)={n_phantoms:,}")

    n_patient_total = row_count(con, "main", US_PATIENT_MASTER_VIEW)
    n_patient_bug = int(con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}".main."{US_PATIENT_MASTER_VIEW}" '
        f'WHERE has_any_us = TRUE '
        f'  AND first_us_date IS NULL '
        f'  AND last_us_date IS NULL'
    ).fetchone()[0])
    n_patient_has_any_us_true = int(con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}".main."{US_PATIENT_MASTER_VIEW}" '
        f'WHERE has_any_us = TRUE'
    ).fetchone()[0])
    log(f"  patient_master rows total={n_patient_total:,} "
        f"has_any_us=TRUE={n_patient_has_any_us_true:,} "
        f"bug_rows (TRUE + both dates NULL)={n_patient_bug:,}")

    # Phantom root-cause probe: count NULL exam_date rows on each
    # upstream source table (per 2026-04-22 direct probe).  After the
    # GROUP BY (research_id, exam_date) in each source CTE, NULL-date
    # rows collapse into (research_id, NULL) pairs; the UNION of those
    # pairs across the three sources is exactly the phantom set.
    null_date_breakdown: list[dict[str, Any]] = []
    for tbl, expected_n in US_SOURCE_TABLES:
        if get_object_kind(con, "main", tbl) is None:
            warn(f"  source table main.{tbl} missing — skipping probe")
            null_date_breakdown.append({
                "source": tbl,
                "n_total": None,
                "n_null_date": None,
                "expected_null_date": expected_n,
                "drift_ok": False,
            })
            continue
        if not column_exists(con, tbl, "exam_date"):
            warn(f"  source table main.{tbl} has no exam_date column — "
                 "skipping NULL-date probe")
            null_date_breakdown.append({
                "source": tbl,
                "n_total": None,
                "n_null_date": None,
                "expected_null_date": expected_n,
                "drift_ok": False,
            })
            continue
        n_total = row_count(con, "main", tbl)
        n_null = int(con.execute(
            f'SELECT COUNT(*) FROM "{PUB_DB}".main."{tbl}" '
            "WHERE exam_date IS NULL"
        ).fetchone()[0])
        ok = _drift_within(n_null, expected_n, PHANTOM_DRIFT_TOLERANCE) \
            if expected_n > 0 else (n_null == 0)
        log(f"  source main.{tbl}: rows={n_total:,} "
            f"NULL-exam_date={n_null:,} (expected≈{expected_n:,}) "
            f"{'ok' if ok else 'DRIFT'}")
        null_date_breakdown.append({
            "source": tbl,
            "n_total": n_total,
            "n_null_date": n_null,
            "expected_null_date": expected_n,
            "drift_ok": ok,
        })

    null_date_total = sum(
        (b["n_null_date"] or 0) for b in null_date_breakdown
    )
    log(f"  total NULL-exam_date source rows: {null_date_total:,} "
        f"(expected ≈ {EXPECTED_PHANTOM_ROWS:,} after UNION collapse)")

    if not _drift_within(
        n_phantoms, EXPECTED_PHANTOM_ROWS, PHANTOM_DRIFT_TOLERANCE,
    ):
        warn(
            f"  phantom count {n_phantoms:,} drifted from expected "
            f"{EXPECTED_PHANTOM_ROWS:,} (>{PHANTOM_DRIFT_TOLERANCE:.0%}); "
            "compare against source NULL-date breakdown above before applying."
        )

    # Boolean-literal census — flags hardcoded TRUE/FALSE / CAST('t' AS
    # BOOLEAN) inside both view bodies.  has_any_us is the known bug;
    # anything else (e.g. `CAST('t' AS BOOLEAN) AS has_gland_findings`
    # inside an aggregation CTE) is intentional but worth surfacing.
    exam_bool_literals = _bool_literal_census(exam_body)
    pat_bool_literals = _bool_literal_census(pat_body)
    exam_aliases = sorted({
        b["downstream_alias"] for b in exam_bool_literals
        if b["downstream_alias"]
    })
    pat_aliases = sorted({
        b["downstream_alias"] for b in pat_bool_literals
        if b["downstream_alias"]
    })
    log(f"  exam_master bool literals: {len(exam_bool_literals)} "
        f"(downstream aliases: {exam_aliases})")
    log(f"  patient_master bool literals: {len(pat_bool_literals)} "
        f"(downstream aliases: {pat_aliases})")

    # Anything in exam_master labelled `has_any_us` would be a brand-new
    # latent bug — none expected.  Halt loudly if found.
    exam_bug_aliases = [
        b for b in exam_bool_literals
        if b["downstream_alias"] == "has_any_us"
    ]
    if exam_bug_aliases:
        raise SystemExit(
            "Phase 0C abort: exam_master_VIEW_v2 contains a hardcoded "
            f"bool literal aliased to `has_any_us` ({exam_bug_aliases}); "
            "scope of 389 does not cover this and the patcher would "
            "miss it.  Investigate before proceeding."
        )

    return {
        "exam_body": exam_body,
        "patient_body": pat_body,
        "exam_cpm_load_bearing": cpm_load_bearing,
        "patient_has_has_any_us_bug": has_has_any_us_bug,
        "n_exam_rows_total": n_exam_total,
        "n_exam_phantom_rows": n_phantoms,
        "n_patient_rows_total": n_patient_total,
        "n_patient_has_any_us_true": n_patient_has_any_us_true,
        "n_patient_bug_rows": n_patient_bug,
        "exam_finding_cols": finding_cols,
        "exam_bool_literals": exam_bool_literals,
        "patient_bool_literals": pat_bool_literals,
        "exam_bool_aliases": exam_aliases,
        "patient_bool_aliases": pat_aliases,
        "null_date_breakdown": null_date_breakdown,
        "null_date_total_source_rows": null_date_total,
    }


# --------------------------------------------------------------------------- #
# Phase 0D — dependents enumeration
# --------------------------------------------------------------------------- #


def phase0d_dependents(con) -> dict[str, list[str]]:
    log("=" * 78)
    log("Phase 0D — enumerate dependent views")
    log("=" * 78)
    deps_exam = find_view_dependents(con, US_EXAM_MASTER_VIEW)
    deps_pat = find_view_dependents(con, US_PATIENT_MASTER_VIEW)
    deps_rollup = find_view_dependents(
        con, "canonical_complications_patient_rollup_v1",
    )

    def _fmt(deps: list[tuple[str, str]]) -> list[str]:
        return [f"{s}.{n}" for s, n in deps]

    out = {
        US_EXAM_MASTER_VIEW: _fmt(deps_exam),
        US_PATIENT_MASTER_VIEW: _fmt(deps_pat),
        "canonical_complications_patient_rollup_v1": _fmt(deps_rollup),
    }
    for target, names in out.items():
        log(f"  {target} dependents: {len(names)}")
        for nm in names[:8]:
            log(f"    - {nm}")
        if len(names) > 8:
            log(f"    ... ({len(names) - 8} more)")
    return out


# --------------------------------------------------------------------------- #
# Phase 0E — complications events audit + rule deltas
# --------------------------------------------------------------------------- #


_RULE_PREDICATES: dict[str, str] = {
    # Rule A: status-aware — `any_evidence` requires finding_status='present'.
    # (This is also the current behaviour; Rule A surfaces the no-op case
    # for explicit operator confirmation.)
    "A": "finding_status = 'present'",
    # Rule B: kind-weighted — exclude entity_legacy + nlp_proxy rows.
    "B": (
        "finding_status = 'present' "
        f"AND {LEGACY_NLP_PROXY_FILTER}"
    ),
    # Rule C: A AND B.
    "C": (
        "finding_status = 'present' "
        f"AND {LEGACY_NLP_PROXY_FILTER}"
    ),
}


def _events_table_columns(con) -> set[str]:
    return set(get_columns(con, "canonical_complications_events_v1"))


def phase0e_complications_audit(con) -> dict[str, Any]:
    log("=" * 78)
    log("Phase 0E — complications events audit + 3-rule deltas")
    log("=" * 78)
    cols = _events_table_columns(con)
    needed = {"source_kind", "source_evidence_type", "finding_status",
              "evidence_strength", "complication_type", "research_id"}
    missing = needed - cols
    if missing:
        raise SystemExit(
            "Phase 0E abort: canonical_complications_events_v1 missing "
            f"required column(s): {missing}"
        )

    log("  per-(source_table x source_kind x source_evidence_type x "
        "finding_status x evidence_strength) breakdown")
    breakdown_rows = con.execute(
        """
        SELECT
            source_table,
            source_kind,
            source_evidence_type,
            finding_status,
            evidence_strength,
            COUNT(*) AS n_rows,
            COUNT(DISTINCT research_id) AS n_patients,
            COUNT(DISTINCT (research_id, complication_type)) AS n_pt_type
        FROM main.canonical_complications_events_v1
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY n_rows DESC
        """
    ).fetchall()
    log(f"    {len(breakdown_rows)} (kind x type x status x strength) cells")

    # For each candidate rule, compute per-complication_type delta vs the
    # current rollup's any_evidence column.
    rule_deltas: dict[str, list[dict[str, Any]]] = {r: [] for r in RULE_NAMES}

    rollup_kind = get_object_kind(
        con, "main", "canonical_complications_patient_rollup_v1",
    )
    if rollup_kind is None:
        raise SystemExit(
            "Phase 0E abort: canonical_complications_patient_rollup_v1 "
            "not found in main"
        )

    for ct in COMPLICATION_TYPES:
        col = f"ever_{ct}_any_evidence"
        if not column_exists(
            con, "canonical_complications_patient_rollup_v1", col,
        ):
            warn(
                f"  rollup column {col} missing — skipping {ct} delta"
            )
            continue
        cur_true = int(con.execute(
            f'SELECT SUM(CASE WHEN "{col}" THEN 1 ELSE 0 END) '
            "FROM main.canonical_complications_patient_rollup_v1"
        ).fetchone()[0] or 0)

        for rule, pred in _RULE_PREDICATES.items():
            cand_true_set = con.execute(
                f"""
                SELECT COUNT(DISTINCT research_id)
                FROM main.canonical_complications_events_v1
                WHERE complication_type = ?
                  AND ({pred})
                """,
                [ct],
            ).fetchone()[0]
            cand_true = int(cand_true_set or 0)

            # Patients flipping TRUE -> FALSE: rollup TRUE, rule FALSE.
            flip_t2f = int(con.execute(
                f"""
                WITH rule_true AS (
                    SELECT DISTINCT research_id
                    FROM main.canonical_complications_events_v1
                    WHERE complication_type = ?
                      AND ({pred})
                )
                SELECT COUNT(*)
                FROM main.canonical_complications_patient_rollup_v1 r
                WHERE r."{col}" = TRUE
                  AND r.research_id NOT IN (SELECT research_id FROM rule_true)
                """,
                [ct],
            ).fetchone()[0])
            # FALSE -> TRUE should always be 0 if the rule is a subset of
            # current any_evidence semantics; we still verify.
            flip_f2t = int(con.execute(
                f"""
                WITH rule_true AS (
                    SELECT DISTINCT research_id
                    FROM main.canonical_complications_events_v1
                    WHERE complication_type = ?
                      AND ({pred})
                )
                SELECT COUNT(*)
                FROM main.canonical_complications_patient_rollup_v1 r
                JOIN rule_true rt ON rt.research_id = r.research_id
                WHERE r."{col}" = FALSE
                """,
                [ct],
            ).fetchone()[0])
            rule_deltas[rule].append({
                "complication_type": ct,
                "current_any_evidence_true": cur_true,
                "rule_any_evidence_true": cand_true,
                "flip_true_to_false": flip_t2f,
                "flip_false_to_true": flip_f2t,
            })

    for rule in RULE_NAMES:
        total_t2f = sum(r["flip_true_to_false"] for r in rule_deltas[rule])
        total_f2t = sum(r["flip_false_to_true"] for r in rule_deltas[rule])
        log(f"  Rule {rule}: total flips TRUE→FALSE={total_t2f:,} "
            f"FALSE→TRUE={total_f2t:,}")

    # Specific 9340 audit (case study from prompt §0).
    audit_9340: dict[str, Any] = {}
    for ct in ("rln_injury", "hypoparathyroidism"):
        col = f"ever_{ct}_any_evidence"
        if not column_exists(
            con, "canonical_complications_patient_rollup_v1", col,
        ):
            continue
        rollup_val = con.execute(
            f"""
            SELECT "{col}"
            FROM main.canonical_complications_patient_rollup_v1
            WHERE research_id = ?
            """,
            [AUDIT_PATIENT_RID],
        ).fetchone()
        events_breakdown = con.execute(
            """
            SELECT source_kind, source_evidence_type, finding_status,
                   evidence_strength, COUNT(*)
            FROM main.canonical_complications_events_v1
            WHERE research_id = ?
              AND complication_type = ?
            GROUP BY 1, 2, 3, 4
            ORDER BY 5 DESC
            """,
            [AUDIT_PATIENT_RID, ct],
        ).fetchall()
        audit_9340[ct] = {
            "rollup_any_evidence": (
                bool(rollup_val[0]) if rollup_val and rollup_val[0] is not None
                else None
            ),
            "events_breakdown": [
                {
                    "source_kind": r[0],
                    "source_evidence_type": r[1],
                    "finding_status": r[2],
                    "evidence_strength": r[3],
                    "n": int(r[4]),
                }
                for r in events_breakdown
            ],
        }

    return {
        "n_breakdown_cells": len(breakdown_rows),
        "breakdown_rows": [
            {
                "source_table": r[0],
                "source_kind": r[1],
                "source_evidence_type": r[2],
                "finding_status": r[3],
                "evidence_strength": r[4],
                "n_rows": int(r[5]),
                "n_patients": int(r[6]),
                "n_pt_type": int(r[7]),
            }
            for r in breakdown_rows
        ],
        "rule_deltas": rule_deltas,
        "audit_9340": audit_9340,
    }


# --------------------------------------------------------------------------- #
# Phase 0F — write prestate report
# --------------------------------------------------------------------------- #


def _md_table_header(cols: list[str]) -> list[str]:
    return [
        "| " + " | ".join(cols) + " |",
        "|" + "|".join(["---"] * len(cols)) + "|",
    ]


def phase0f_write_prestate(
    *,
    preflight: dict[str, Any],
    zombie: dict[str, Any],
    views: dict[str, Any],
    deps: dict[str, list[str]],
    audit: dict[str, Any],
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Script 389 — Pre-state probe report")
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"PUB DB: `{PUB_DB}` · Archive DB: `{ARC_DB_RAW}`")
    lines.append("")

    lines.append("## 0A · Preflight")
    lines.append("")
    for k, v in preflight.items():
        lines.append(f"* `{k}` = `{v}`")
    lines.append("")

    lines.append("## 0B · `canonical_us_nodule_v2` source-flag partition")
    lines.append("")
    lines.append(
        "**Classifier reset 2026-04-22.** The original prompt's baselines "
        "(`18,310 / 17,090 / 2,152 / 27` for `clean_llm_parsed / "
        "clean_non_llm / zombie_parent / llm_parsed_but_blob`) were not "
        "reproducible against live state — direct MotherDuck probe showed "
        "no combination of source-flags or `location_raw` content signals "
        "yields that partition.  They were phantom from compressed "
        "context.  The classifier is now grounded in the four boolean "
        "flags that actually do partition the table: `is_aggregate_row`, "
        "`source_tirads_llm`, `source_base`, `nlp_backfill_pending`.  "
        "Baselines below are frozen from the 2026-04-22 probe.  The "
        "Phase 2 DELETE step that the prior classifier targeted is "
        "RETIRED — none of the four buckets are \"zombie\" in the "
        "structural sense; `needs_backfill` rows are legitimate entries "
        "awaiting NLP extraction, not remnants to delete."
    )
    lines.append("")
    lines += _md_table_header(["bucket", "actual", "expected", "Δ %"])
    for name, expected in EXPECTED_BUCKETS.items():
        actual = zombie["buckets"].get(name, 0)
        delta_pct = (
            ((actual - expected) / expected * 100.0) if expected else 0.0
        )
        lines.append(
            f"| `{name}` | {actual:,} | {expected:,} | {delta_pct:+.2f}% |"
        )
    lines.append(
        f"| **TOTAL** | **{zombie['actual_total']:,}** "
        f"| **{EXPECTED_TOTAL:,}** | "
        f"{((zombie['actual_total'] - EXPECTED_TOTAL) / EXPECTED_TOTAL * 100.0):+.2f}% |"
    )
    lines.append("")
    lines.append("Classifier expression (source-flag partition; PHI-safe):")
    lines.append("")
    lines.append("```sql")
    lines.append(zombie["classifier_sql"])
    lines.append("```")
    lines.append("")

    lines.append("## 0C · US view-stack probe")
    lines.append("")
    lines.append(f"* `{US_EXAM_MASTER_VIEW}` total rows: "
                 f"{views['n_exam_rows_total']:,}")
    lines.append(f"* `{US_EXAM_MASTER_VIEW}` phantom rows "
                 f"(NULL exam_date + NULL findings): "
                 f"{views['n_exam_phantom_rows']:,} "
                 f"(expected ≈ {EXPECTED_PHANTOM_ROWS:,})")
    lines.append(
        "* CPM CTE in exam_master body (load-bearing for "
        f"`is_preop_exam`, NOT a row-shape driver): "
        f"`{views['exam_cpm_load_bearing']}`"
    )
    lines.append(f"* `{US_PATIENT_MASTER_VIEW}` total rows: "
                 f"{views['n_patient_rows_total']:,}")
    lines.append(f"* `has_any_us=TRUE` patients: "
                 f"{views['n_patient_has_any_us_true']:,}")
    lines.append(f"* Bug rows (has_any_us=TRUE + both dates NULL): "
                 f"{views['n_patient_bug_rows']:,} "
                 f"(expected ≈ {EXPECTED_PATIENT_BUG_ROWS:,})")
    lines.append("")
    lines.append("### Phantom root cause — NULL `exam_date` in source tables")
    lines.append("")
    lines.append(
        "After the GROUP BY `(research_id, exam_date)` in each source "
        "CTE, NULL-date rows collapse into `(research_id, NULL)` pairs.  "
        "The UNION of those pairs is exactly the phantom set in "
        f"`{US_EXAM_MASTER_VIEW}`.  Phase 2D fix: add "
        "`WHERE exam_date IS NOT NULL` to every source CTE BEFORE "
        "aggregation; CPM CTE + `is_preop_exam` column stay intact."
    )
    lines.append("")
    lines += _md_table_header([
        "source table", "total rows", "NULL exam_date", "expected", "drift OK",
    ])
    for b in views["null_date_breakdown"]:
        total_s = f"{b['n_total']:,}" if b["n_total"] is not None else "—"
        null_s = f"{b['n_null_date']:,}" if b["n_null_date"] is not None else "—"
        exp_s = f"{b['expected_null_date']:,}"
        drift_s = "ok" if b["drift_ok"] else "DRIFT"
        lines.append(
            f"| `{b['source']}` | {total_s} | {null_s} | {exp_s} | {drift_s} |"
        )
    lines.append(
        f"| **total source NULL-exam_date** | — | "
        f"**{views['null_date_total_source_rows']:,}** | "
        f"≈ {EXPECTED_PHANTOM_ROWS:,} (after UNION) | — |"
    )
    lines.append("")
    lines.append("### Boolean-literal census")
    lines.append("")
    lines.append(
        "Every `CAST('t' AS BOOLEAN)` / `CAST('f' AS BOOLEAN)` / bare "
        "`TRUE` / `FALSE` in the live view bodies, with the nearest "
        "downstream `AS <alias>` (heuristic, ≤64 chars).  `has_any_us` "
        "in patient_master is the known bug; everything else is "
        "expected (the gland_agg / ln_agg CTEs intentionally emit "
        "`TRUE AS has_gland_findings` / `TRUE AS has_us_ln_findings` "
        "inside aggregations that group by `(research_id, exam_date)`, "
        "so each row by construction represents an exam where that "
        "modality had findings)."
    )
    lines.append("")
    lines.append(f"#### `{US_EXAM_MASTER_VIEW}` "
                 f"({len(views['exam_bool_literals'])} literals; "
                 f"aliases: {views['exam_bool_aliases']})")
    lines.append("")
    if views["exam_bool_literals"]:
        lines += _md_table_header(["literal", "downstream_alias", "offset"])
        for b in views["exam_bool_literals"]:
            lines.append(
                f"| `{b['literal']}` "
                f"| `{b['downstream_alias'] or '—'}` "
                f"| {b['offset']} |"
            )
    lines.append("")
    lines.append(f"#### `{US_PATIENT_MASTER_VIEW}` "
                 f"({len(views['patient_bool_literals'])} literals; "
                 f"aliases: {views['patient_bool_aliases']})")
    lines.append("")
    if views["patient_bool_literals"]:
        lines += _md_table_header(["literal", "downstream_alias", "offset"])
        for b in views["patient_bool_literals"]:
            lines.append(
                f"| `{b['literal']}` "
                f"| `{b['downstream_alias'] or '—'}` "
                f"| {b['offset']} |"
            )
    lines.append("")
    lines.append("### Live view body — exam_master")
    lines.append("```sql")
    lines.append(views["exam_body"])
    lines.append("```")
    lines.append("")
    lines.append("### Live view body — patient_master")
    lines.append("```sql")
    lines.append(views["patient_body"])
    lines.append("```")
    lines.append("")

    lines.append("## 0D · Dependent views")
    lines.append("")
    for target, names in deps.items():
        lines.append(f"### `{target}` ({len(names)} dependents)")
        if not names:
            lines.append("* (no dependents)")
        for nm in names:
            lines.append(f"* `{nm}`")
        lines.append("")

    lines.append("## 0E · Complications events audit")
    lines.append("")
    lines.append(f"Cells in (source_table × source_kind × "
                 f"source_evidence_type × finding_status × "
                 f"evidence_strength) breakdown: "
                 f"**{audit['n_breakdown_cells']:,}**")
    lines.append("")
    lines += _md_table_header([
        "source_table", "source_kind", "source_evidence_type",
        "finding_status", "evidence_strength", "n_rows", "n_patients",
        "n_pt_type",
    ])
    for r in audit["breakdown_rows"]:
        lines.append(
            f"| `{r['source_table']}` | `{r['source_kind']}` "
            f"| `{r['source_evidence_type']}` | `{r['finding_status']}` "
            f"| `{r['evidence_strength']}` | {r['n_rows']:,} "
            f"| {r['n_patients']:,} | {r['n_pt_type']:,} |"
        )
    lines.append("")

    lines.append("### Rule-vs-current deltas (by complication_type)")
    lines.append("")
    for rule in RULE_NAMES:
        lines.append(f"#### Rule {rule}")
        lines.append("")
        lines += _md_table_header([
            "complication_type", "current any_evidence TRUE",
            f"rule {rule} any_evidence TRUE",
            "flip TRUE→FALSE", "flip FALSE→TRUE",
        ])
        for delta in audit["rule_deltas"][rule]:
            lines.append(
                f"| `{delta['complication_type']}` "
                f"| {delta['current_any_evidence_true']:,} "
                f"| {delta['rule_any_evidence_true']:,} "
                f"| {delta['flip_true_to_false']:,} "
                f"| {delta['flip_false_to_true']:,} |"
            )
        total_t2f = sum(d["flip_true_to_false"]
                        for d in audit["rule_deltas"][rule])
        total_f2t = sum(d["flip_false_to_true"]
                        for d in audit["rule_deltas"][rule])
        lines.append("")
        lines.append(
            f"**Rule {rule} totals:** "
            f"TRUE→FALSE = {total_t2f:,} · FALSE→TRUE = {total_f2t:,}"
        )
        lines.append("")

    lines.append(f"### Audit case — research_id `{AUDIT_PATIENT_RID}`")
    lines.append("")
    for ct, info in audit["audit_9340"].items():
        lines.append(f"#### `{ct}`")
        lines.append(
            f"* Rollup `ever_{ct}_any_evidence` = "
            f"`{info['rollup_any_evidence']}`"
        )
        if info["events_breakdown"]:
            lines += _md_table_header([
                "source_kind", "source_evidence_type",
                "finding_status", "evidence_strength", "n",
            ])
            for r in info["events_breakdown"]:
                lines.append(
                    f"| `{r['source_kind']}` | `{r['source_evidence_type']}` "
                    f"| `{r['finding_status']}` "
                    f"| `{r['evidence_strength']}` | {r['n']:,} |"
                )
        else:
            lines.append("* (no events for this patient/type)")
        lines.append("")

    lines.append("## Plan-review gate")
    lines.append("")
    lines.append(
        "To apply, write a plan-approval file at "
        f"`{APPROVAL_PATH.relative_to(REPO_ROOT)}` containing one of:"
    )
    lines.append("")
    lines.append("```")
    lines.append("RULE=A")
    lines.append("# or RULE=B  /  RULE=C")
    lines.append("INCLUDE_FINDING_STATUS_ABSENT_IN_any_evidence=FALSE")
    lines.append("INCLUDE_ENTITY_LEGACY_NLP_PROXY_IN_any_evidence=FALSE")
    lines.append("```")
    lines.append("")
    lines.append("Then re-run with `--apply`.")
    lines.append("")

    PRESTATE_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"  wrote prestate report -> {PRESTATE_PATH}")


# --------------------------------------------------------------------------- #
# Phase 1 — plan-review gate
# --------------------------------------------------------------------------- #


def parse_approval_file() -> dict[str, str] | None:
    if not APPROVAL_PATH.exists():
        return None
    out: dict[str, str] = {}
    for raw in APPROVAL_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise SystemExit(
                f"Approval file malformed at line: {line!r}"
            )
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    if "RULE" not in out:
        raise SystemExit(
            f"Approval file missing required `RULE=` line ({APPROVAL_PATH})"
        )
    if out["RULE"] not in RULE_NAMES:
        raise SystemExit(
            f"Approval file RULE={out['RULE']!r} invalid; expected "
            f"one of {RULE_NAMES}"
        )
    # Sanity-check optional keys against the implied semantics of the
    # selected rule (defaults intended by the prompt).
    if out["RULE"] in ("A", "C"):
        v = out.get("INCLUDE_FINDING_STATUS_ABSENT_IN_any_evidence", "FALSE")
        if v.upper() != "FALSE":
            raise SystemExit(
                f"Rule {out['RULE']} requires "
                "INCLUDE_FINDING_STATUS_ABSENT_IN_any_evidence=FALSE; "
                f"got {v!r}"
            )
    if out["RULE"] in ("B", "C"):
        v = out.get("INCLUDE_ENTITY_LEGACY_NLP_PROXY_IN_any_evidence", "FALSE")
        if v.upper() != "FALSE":
            raise SystemExit(
                f"Rule {out['RULE']} requires "
                "INCLUDE_ENTITY_LEGACY_NLP_PROXY_IN_any_evidence=FALSE; "
                f"got {v!r}"
            )
    return out


def phase1_plan_summary(
    *,
    zombie: dict[str, Any],
    views: dict[str, Any],
    deps: dict[str, list[str]],
    audit: dict[str, Any],
) -> None:
    log("=" * 78)
    log("Phase 1 — plan summary (no approval file present)")
    log("=" * 78)
    log(f"  Planned writes:")
    log("  · Phase 2 (classifier-reset provenance): single read-only "
        "INSERT into main.__readme; no nodule rows touched")
    log(f"    (Phases 2A/2B/2C retired 2026-04-22 — see "
        "EXPECTED_BUCKETS comment for the classifier reset rationale.)")
    log(f"  · Phase 2D: CREATE OR REPLACE VIEW main.{US_EXAM_MASTER_VIEW} "
        f"(filter NULL exam_date in source CTEs; eliminates "
        f"{views['n_exam_phantom_rows']:,} phantoms)")
    log(f"  · Phase 2E: CREATE OR REPLACE VIEW main.{US_PATIENT_MASTER_VIEW} "
        f"(derived has_any_us replaces literal TRUE)")
    log(f"  · Phase 2F: CREATE OR REPLACE TABLE "
        f"canonical_complications_patient_rollup_v1 (rule TBD by operator)")
    log(f"  · Phase 2G: re-bind "
        f"{sum(len(v) for v in deps.values())} dependent view(s)")
    log(f"  · Phase 2H: post-state count + registry update + readme bump")
    log("")
    log("Choose a reconciliation rule for the complications rollup:")
    for rule in RULE_NAMES:
        total_t2f = sum(d["flip_true_to_false"]
                        for d in audit["rule_deltas"][rule])
        log(f"  Rule {rule}: total TRUE→FALSE flips = {total_t2f:,}")
    log("")
    log("Write the approval file:")
    log(f"  {APPROVAL_PATH}")
    log("Containing one of:")
    log("  RULE=A")
    log("  RULE=B")
    log("  RULE=C")
    log("Plus optional default-affirming lines:")
    log("  INCLUDE_FINDING_STATUS_ABSENT_IN_any_evidence=FALSE")
    log("  INCLUDE_ENTITY_LEGACY_NLP_PROXY_IN_any_evidence=FALSE")
    log("Then re-run with --apply.")


# --------------------------------------------------------------------------- #
# Phase 2 — apply
# --------------------------------------------------------------------------- #


def _ensure_arc_schema(con, schema: str) -> None:
    con.execute(f'CREATE SCHEMA IF NOT EXISTS {ARC_DB}."{schema}"')


def _archive_table_ctas(
    con,
    *,
    src_fq: str,
    dest_schema: str,
    dest_name: str,
    expected_rows: int | None,
) -> int:
    """CTAS src_fq -> archive DB.  Returns archived row count.  Idempotent."""
    _ensure_arc_schema(con, dest_schema)
    arc_fq = f'{ARC_DB}."{dest_schema}"."{dest_name}"'
    arc_rows = archive_present(con, dest_schema, dest_name)
    src_rows = int(con.execute(f"SELECT COUNT(*) FROM {src_fq}").fetchone()[0])
    if arc_rows is not None:
        if arc_rows != src_rows:
            raise SystemExit(
                f"Archive {arc_fq} has {arc_rows:,} rows but src has "
                f"{src_rows:,}; refusing to overwrite"
            )
        log(f"     archive already present ({arc_rows:,} rows) — "
            "skipping CTAS")
        return arc_rows
    con.execute(f"CREATE TABLE {arc_fq} AS SELECT * FROM {src_fq}")
    arc_rows = int(con.execute(f"SELECT COUNT(*) FROM {arc_fq}").fetchone()[0])
    if arc_rows != src_rows:
        raise SystemExit(
            f"ARCHIVE PARITY FAIL {src_fq}: src={src_rows:,} arc={arc_rows:,}"
        )
    if expected_rows is not None and arc_rows != expected_rows:
        warn(
            f"     archived {arc_rows:,} rows; expected {expected_rows:,} "
            "(non-fatal; review)"
        )
    log(f"     archived to {arc_fq} ({arc_rows:,} rows; parity OK)")
    return arc_rows


# --- 2A (RETIRED): zombie DELETE -------------------------------------------#
#
# The original Phase 2 plan had three nodule-level operations
# (2A: DELETE supersedable zombie rows; 2B: snapshot needs_reextraction
# carry-forward; 2C: cosmetic location_raw trim).  All three depended on
# the prior `zombie_parent` / `llm_parsed_but_blob` classifier whose
# baselines were retired 2026-04-22 (see EXPECTED_BUCKETS comment at top
# of file).  Under the source-flag partition that replaces it, no row in
# `canonical_us_nodule_v2` is "zombie" in the structural sense —
# `needs_backfill` rows are legitimate entries awaiting NLP extraction,
# not remnants to delete.
#
# Phase 2A is replaced by `phase2_classifier_provenance` below: a
# single read-only INSERT into `main.__readme` documenting the reset.
# Phases 2B and 2C are removed entirely.  US view rewrites (2D / 2E),
# complications rebuild (2F), dependent re-bind (2G), and registry +
# readme bump (2H) are unchanged.


def phase2_classifier_provenance(con) -> dict[str, Any]:
    """Write a single provenance row to main.__readme documenting the
    Phase 0B classifier reset.  Idempotent: skips if a 389-tagged
    classifier-reset row is already present.
    """
    log("=" * 78)
    log("Phase 2 (classifier-reset provenance) — main.__readme")
    log("=" * 78)

    if get_object_kind(con, "main", "__readme") is None:
        warn("  main.__readme not present — skipping provenance row")
        return {"status": "skipped", "reason": "main.__readme missing"}

    readme_cols = set(get_columns(con, "__readme"))
    if not {"content", "updated_at"}.issubset(readme_cols):
        warn("  __readme schema missing content/updated_at — skipping")
        return {"status": "skipped", "reason": "schema mismatch"}

    marker = "Script 389 Phase 0B classifier reset 2026-04-22"
    existing = int(con.execute(
        "SELECT COUNT(*) FROM main.__readme WHERE content LIKE ?",
        [f"%{marker}%"],
    ).fetchone()[0])
    if existing:
        log(f"  ALREADY present ({existing} prior row(s)) — skipping insert")
        return {"status": "already_done", "n_existing": existing}

    content = (
        f"{marker}.  canonical_us_nodule_v2 partition frozen at "
        "26,402 / 8,919 / 2,117 / 141 (clean_dual_source / "
        "clean_base_only / needs_backfill / aggregate_rollup).  "
        "No rows deleted; prior \"zombie\" concept retired — "
        "source-flag partition is non-destructive.  Content-based "
        "blob audit deferred to optional Script 389b."
    )
    insert_cols = ["content", "updated_at"]
    value_exprs = ["?", "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)"]
    params: list[Any] = [content]
    if "script" in readme_cols:
        insert_cols.append("script")
        value_exprs.append("?")
        params.append(SCRIPT_TAG)
    if "git_sha" in readme_cols:
        insert_cols.append("git_sha")
        value_exprs.append("?")
        params.append(None)
    con.execute(
        f"INSERT INTO main.__readme ({', '.join(insert_cols)}) "
        f"VALUES ({', '.join(value_exprs)})",
        params,
    )
    log("  inserted classifier-reset provenance row")
    return {"status": "done", "n_existing": 0}


# --- 2D: rewrite canonical_us_exam_master_VIEW_v2 ------------------------- #


def _build_exam_master_view_sql(con) -> str:
    """Return CREATE OR REPLACE VIEW SQL for canonical_us_exam_master_VIEW_v2.

    US-grounded: FROM clause is a UNION of (research_id, exam_date) keys
    drawn from the 3 v2 source tables.  CPM is LEFT-JOINed at the end
    only to derive `is_preop_exam` — it does NOT drive row shape.  Exam
    grain.

    The phantom-row root cause (per direct probe 2026-04-22):
    upstream NULL `exam_date` rows in `canonical_us_thyroid_gland_v2`
    (6,785) and `canonical_us_nodule_v2` (2,231) collapse into a single
    `(research_id, NULL)` pair after the GROUP BY in each source CTE,
    and the UNION emits 6,792 such phantoms.  Fix: filter NULL
    `exam_date` *inside* every source CTE BEFORE aggregation.
    `canonical_us_lymph_node_v2` already has zero NULL-date rows
    (defensive filter applied anyway for symmetry).
    """
    target = f'"{PUB_DB}".main."{US_EXAM_MASTER_VIEW}"'
    cpm_cols = set(get_columns(con, "canonical_patient_master"))
    surg_col = next(
        (c for c in (
            "first_surgery_date_v2", "first_surgery_date",
            "surg_first_date", "surg_date_canonical",
            "surgery_date", "date_of_surgery",
        ) if c in cpm_cols),
        None,
    )
    is_preop_expr = (
        f"CASE WHEN cp.{surg_col} IS NOT NULL "
        f"AND exams.exam_date <= cp.{surg_col} THEN TRUE ELSE FALSE END"
        if surg_col else "FALSE"
    )
    cpm_join = (
        f"LEFT JOIN \"{PUB_DB}\".main.canonical_patient_master cp "
        f"  ON cp.research_id = exams.research_id"
        if surg_col else ""
    )
    return f"""
CREATE OR REPLACE VIEW {target} AS
WITH nodule_agg AS (
    SELECT
        research_id,
        exam_date,
        ANY_VALUE(us_exam_id) AS us_exam_id_nodule,
        COUNT(*) AS n_nodules_on_exam,
        MAX(size_cm_max) AS largest_nodule_cm,
        BOOL_OR(LOWER(COALESCE(laterality, '')) = 'right')
        AND BOOL_OR(LOWER(COALESCE(laterality, '')) = 'left')
            AS bilateral_flag,
        BOOL_OR(LOWER(COALESCE(laterality, '')) = 'isthmus')
        OR BOOL_OR(LOWER(COALESCE(location_raw, '')) LIKE '%isthmus%')
            AS isthmus_nodule_flag,
        MAX(acr2017_tirads_category) AS worst_tirads_category_this_exam,
        MAX(acr2017_tirads_points)   AS worst_tirads_points_this_exam,
        MIN(acr2017_tirads_category) AS best_tirads_category_this_exam,
        SUM(CASE WHEN UPPER(acr2017_tirads_category) = 'TR5' THEN 1 ELSE 0 END)
            AS count_tr5,
        SUM(CASE WHEN UPPER(acr2017_tirads_category) = 'TR4' THEN 1 ELSE 0 END)
            AS count_tr4,
        SUM(CASE WHEN UPPER(acr2017_tirads_category) = 'TR3' THEN 1 ELSE 0 END)
            AS count_tr3,
        SUM(CASE WHEN UPPER(acr2017_tirads_category) = 'TR2' THEN 1 ELSE 0 END)
            AS count_tr2,
        SUM(CASE WHEN UPPER(acr2017_tirads_category) = 'TR1' THEN 1 ELSE 0 END)
            AS count_tr1,
        BOOL_OR(nlp_backfill_pending) AS any_nodule_pending_on_exam
    FROM "{PUB_DB}".main.canonical_us_nodule_v2
    WHERE COALESCE(is_aggregate_row, FALSE) = FALSE
      AND exam_date IS NOT NULL  -- 389/2D phantom fix (upstream 2,231 NULL-date rows)
    GROUP BY research_id, exam_date
),
nodule_2nd AS (
    SELECT research_id, exam_date,
           NTH_VALUE(size_cm_max, 2) OVER (
               PARTITION BY research_id, exam_date
               ORDER BY size_cm_max DESC NULLS LAST
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           ) AS second_largest_nodule_cm
    FROM "{PUB_DB}".main.canonical_us_nodule_v2
    WHERE COALESCE(is_aggregate_row, FALSE) = FALSE
      AND exam_date IS NOT NULL  -- 389/2D phantom fix
    QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id, exam_date) = 1
),
gland_agg AS (
    SELECT research_id, exam_date,
           ANY_VALUE(us_exam_id) AS us_exam_id_gland,
           TRUE AS has_gland_findings,
           BOOL_OR(nlp_backfill_pending) AS any_gland_pending_on_exam
    FROM "{PUB_DB}".main.canonical_us_thyroid_gland_v2
    WHERE exam_date IS NOT NULL  -- 389/2D phantom fix (upstream 6,785 NULL-date rows)
    GROUP BY research_id, exam_date
),
ln_agg AS (
    SELECT research_id, exam_date,
           ANY_VALUE(us_exam_id) AS us_exam_id_ln,
           TRUE AS has_us_ln_findings,
           COUNT(*) AS n_us_ln_total_on_exam,
           SUM(CASE WHEN COALESCE(suspicious_flag, FALSE) THEN 1 ELSE 0 END)
               AS n_abnormal_us_ln_on_exam,
           BOOL_OR(nlp_backfill_pending) AS any_us_ln_pending_on_exam
    FROM "{PUB_DB}".main.canonical_us_lymph_node_v2
    WHERE exam_date IS NOT NULL  -- defensive (LN source already 0 NULLs in current state)
    GROUP BY research_id, exam_date
),
exams AS (
    SELECT research_id, exam_date FROM nodule_agg
    UNION
    SELECT research_id, exam_date FROM gland_agg
    UNION
    SELECT research_id, exam_date FROM ln_agg
)
SELECT
    exams.research_id,
    COALESCE(n.us_exam_id_nodule, g.us_exam_id_gland, l.us_exam_id_ln)
        AS us_exam_id,
    exams.exam_date,
    n.n_nodules_on_exam,
    n.largest_nodule_cm,
    n2.second_largest_nodule_cm,
    n.bilateral_flag,
    n.isthmus_nodule_flag,
    n.worst_tirads_category_this_exam,
    n.worst_tirads_points_this_exam,
    n.best_tirads_category_this_exam,
    n.count_tr5, n.count_tr4, n.count_tr3, n.count_tr2, n.count_tr1,
    COALESCE(g.has_gland_findings, FALSE) AS has_gland_findings,
    COALESCE(l.has_us_ln_findings, FALSE) AS has_us_ln_findings,
    l.n_us_ln_total_on_exam,
    l.n_abnormal_us_ln_on_exam,
    ROW_NUMBER() OVER (
        PARTITION BY exams.research_id
        ORDER BY exams.exam_date NULLS LAST
    ) AS exam_rank_for_patient,
    {is_preop_expr} AS is_preop_exam,
    (COALESCE(n.any_nodule_pending_on_exam, FALSE)
     OR COALESCE(g.any_gland_pending_on_exam, FALSE)
     OR COALESCE(l.any_us_ln_pending_on_exam, FALSE))
        AS any_nlp_backfill_pending_on_exam
FROM exams
LEFT JOIN nodule_agg n  USING (research_id, exam_date)
LEFT JOIN nodule_2nd n2 USING (research_id, exam_date)
LEFT JOIN gland_agg g   USING (research_id, exam_date)
LEFT JOIN ln_agg    l   USING (research_id, exam_date)
{cpm_join}
"""


def phase2d_rewrite_exam_master(
    con, views: dict[str, Any],
) -> dict[str, Any]:
    log("=" * 78)
    log(f"Phase 2D — rewrite main.{US_EXAM_MASTER_VIEW}")
    log("=" * 78)

    # Snapshot the live body to archive DB.  Literal SQL only — `?`
    # placeholders aren't always expanded inside CTAS bodies in DuckDB,
    # and the values here are catalog identifiers (not PHI), so this is
    # safe.
    _ensure_arc_schema(con, ARC_PUB_SCHEMA)
    body_dest_name = f"{ARC_EXAM_MASTER_LEGACY}_body"
    body_dest = f'{ARC_DB}."{ARC_PUB_SCHEMA}"."{body_dest_name}"'
    if archive_present(con, ARC_PUB_SCHEMA, body_dest_name) is None:
        con.execute(
            f"CREATE TABLE {body_dest} AS "
            "SELECT view_definition AS body, "
            "       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts "
            "FROM information_schema.views "
            f"WHERE table_catalog = '{PUB_DB}' AND table_schema = 'main' "
            f"      AND table_name = '{US_EXAM_MASTER_VIEW}'"
        )
        log(f"  snapshotted body -> {body_dest}")
    else:
        log(f"  body snapshot already present at {body_dest}")

    sql = _build_exam_master_view_sql(con)
    log("  CREATE OR REPLACE VIEW (filter NULL exam_date in source CTEs; "
        "CPM CTE retained for is_preop_exam)")
    con.execute(sql)

    new_n = row_count(con, "main", US_EXAM_MASTER_VIEW)
    log(f"  new row count: {new_n:,} "
        f"(prior: {views['n_exam_rows_total']:,}, "
        f"phantoms eliminated: ~{views['n_exam_phantom_rows']:,})")

    log_move(
        con, phase="2D",
        source_schema="main", source_name=US_EXAM_MASTER_VIEW,
        dest_schema=ARC_PUB_SCHEMA,
        dest_name=f"{ARC_EXAM_MASTER_LEGACY}_body",
        move_method="VIEW-snapshot+CREATE OR REPLACE",
        n_rows=new_n,
        reason="389/2D: filter NULL exam_date from source CTEs "
               "(nodule_agg / nodule_2nd / gland_agg / ln_agg); CPM CTE + "
               "is_preop_exam retained.  ~"
               f"{views['n_exam_phantom_rows']:,} phantom rows eliminated.",
    )
    return {"status": "done", "n_rows_new": new_n,
            "n_phantoms_eliminated": views["n_exam_phantom_rows"]}


# --- 2E: rewrite canonical_us_patient_master_VIEW_v2 ---------------------- #


def _patch_patient_master_body(body: str) -> str:
    """Replace the hardcoded has_any_us literal with a derived expression.

    The replacement uses ``COUNT(*) FILTER (WHERE us_exam_id IS NOT NULL)
    > 0`` against the same FROM source — already available in the
    surrounding aggregation CTE.  Because the surrounding context is
    always ``GROUP BY research_id``, the FILTER aggregate is well-defined.
    """
    derived = (
        "(COUNT(*) FILTER (WHERE us_exam_id IS NOT NULL "
        "OR exam_date IS NOT NULL) > 0) AS has_any_us"
    )
    new_body = body
    if HAS_ANY_US_BUG_LITERAL in new_body:
        new_body = new_body.replace(HAS_ANY_US_BUG_LITERAL, derived)
    elif HAS_ANY_US_BUG_LITERAL_ALT in new_body:
        new_body = new_body.replace(HAS_ANY_US_BUG_LITERAL_ALT, derived)
    else:
        # Defensive whole-word fallback.
        pat = re.compile(
            r"CAST\(\s*'t'\s+AS\s+BOOLEAN\s*\)\s+AS\s+has_any_us",
            flags=re.IGNORECASE,
        )
        new_body, n_subs = pat.subn(derived, new_body)
        if n_subs == 0:
            raise SystemExit(
                "Phase 2E abort: could not locate hardcoded has_any_us "
                "literal in the live patient_master body to patch"
            )
    if new_body == body:
        raise SystemExit(
            "Phase 2E abort: patch produced identical body — "
            "no substitution made"
        )
    return new_body


def phase2e_rewrite_patient_master(
    con, views: dict[str, Any],
) -> dict[str, Any]:
    log("=" * 78)
    log(f"Phase 2E — rewrite main.{US_PATIENT_MASTER_VIEW}")
    log("=" * 78)

    _ensure_arc_schema(con, ARC_PUB_SCHEMA)
    body_dest_name = f"{ARC_PATIENT_MASTER_LEGACY}_body"
    if archive_present(con, ARC_PUB_SCHEMA, body_dest_name) is None:
        con.execute(
            f'CREATE TABLE {ARC_DB}."{ARC_PUB_SCHEMA}"."{body_dest_name}" '
            "AS SELECT view_definition AS body, "
            "       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts "
            "FROM information_schema.views "
            f"WHERE table_catalog = '{PUB_DB}' AND table_schema = 'main' "
            f"      AND table_name = '{US_PATIENT_MASTER_VIEW}'"
        )
        log(f"  snapshotted body -> {ARC_DB}.{ARC_PUB_SCHEMA}.{body_dest_name}")
    else:
        log(f"  body snapshot already present at "
            f"{ARC_DB}.{ARC_PUB_SCHEMA}.{body_dest_name}")

    body = view_select_body(con, "main", US_PATIENT_MASTER_VIEW)
    if body is None:
        raise SystemExit(
            f"Phase 2E abort: could not read live body for {US_PATIENT_MASTER_VIEW}"
        )
    new_body = _patch_patient_master_body(body)
    target = f'"{PUB_DB}".main."{US_PATIENT_MASTER_VIEW}"'
    log("  CREATE OR REPLACE VIEW with patched has_any_us derivation")
    con.execute(f"CREATE OR REPLACE VIEW {target} AS {new_body}")

    new_total = row_count(con, "main", US_PATIENT_MASTER_VIEW)
    new_true = int(con.execute(
        f'SELECT COUNT(*) FROM {target} WHERE has_any_us = TRUE'
    ).fetchone()[0])
    new_false = int(con.execute(
        f'SELECT COUNT(*) FROM {target} WHERE has_any_us = FALSE'
    ).fetchone()[0])
    log(f"  new total: {new_total:,}; has_any_us TRUE={new_true:,} "
        f"FALSE={new_false:,}")

    log_move(
        con, phase="2E",
        source_schema="main", source_name=US_PATIENT_MASTER_VIEW,
        dest_schema=ARC_PUB_SCHEMA, dest_name=body_dest_name,
        move_method="VIEW-snapshot+CREATE OR REPLACE",
        n_rows=new_total,
        reason="389/2E: replace hardcoded has_any_us=TRUE with derived "
               "value from corrected exam_master",
    )
    return {"status": "done", "n_rows_new": new_total,
            "n_has_any_us_true": new_true,
            "n_has_any_us_false": new_false}


# --- 2F: rebuild complications rollup per rule ---------------------------- #


def _rule_predicate(rule: str, ct: str, tier: str) -> str:
    base = (
        f"complication_type = '{ct}' "
    )
    if tier == "definitive":
        return (
            base
            + "AND finding_status = 'present' "
            + "AND evidence_strength = 'definitive'"
        )
    if tier == "probable_or_better":
        return (
            base
            + "AND finding_status = 'present' "
            + "AND evidence_strength IN ('definitive', 'probable')"
        )
    # any_evidence — apply chosen rule
    if rule == "A":
        return base + "AND finding_status = 'present'"
    if rule == "B":
        return (
            base
            + "AND finding_status = 'present' "
            + f"AND {LEGACY_NLP_PROXY_FILTER}"
        )
    # Rule C: A AND B
    return (
        base
        + "AND finding_status = 'present' "
        + f"AND {LEGACY_NLP_PROXY_FILTER}"
    )


def _build_rollup_sql(rule: str) -> str:
    target = f'"{PUB_DB}".main.canonical_complications_patient_rollup_v1'
    src = f'"{PUB_DB}".main.canonical_complications_events_v1'

    ever_cols: list[str] = []
    rollup_cols_in_order: list[str] = []
    for ct in COMPLICATION_TYPES:
        for tier in ROLLUP_TIERS:
            col_name = f"ever_{ct}_{tier}"
            rollup_cols_in_order.append(col_name)
            pred = _rule_predicate(rule, ct, tier)
            ever_cols.append(
                f"        COALESCE(BOOL_OR({pred}), FALSE) AS {col_name}"
            )

    temporal_min_max_cols: list[str] = []
    for ct in TEMPORAL_TYPES_FOR_ROLLUP:
        # Temporal aggregates use Rule A semantics by definition
        # (status='present'); no rule B exclusion here so derived
        # transient/permanent/etc. flags stay anchored to present events.
        temporal_min_max_cols.append(
            f"        MIN(CASE WHEN complication_type = '{ct}' "
            f"AND finding_status = 'present' THEN finding_date END) "
            f"            AS {ct}_min_present_date"
        )
        temporal_min_max_cols.append(
            f"        MAX(CASE WHEN complication_type = '{ct}' "
            f"AND finding_status = 'present' THEN finding_date END) "
            f"            AS {ct}_max_present_date"
        )

    inner_select_block = ",\n".join(ever_cols + temporal_min_max_cols)

    temporal_outer_cols: list[str] = []
    for ct in TEMPORAL_TYPES_FOR_ROLLUP:
        temporal_outer_cols.append(f"""
    CASE
      WHEN e.{ct}_min_present_date IS NOT NULL
           AND fs.first_surgery_date IS NOT NULL
           AND e.{ct}_min_present_date <
               fs.first_surgery_date - INTERVAL '{PREOP_PROXIMITY_BUFFER_DAYS} days'
        THEN TRUE ELSE FALSE
    END AS {ct}_preexisting,
    CASE
      WHEN e.{ct}_min_present_date IS NOT NULL
           AND fs.first_surgery_date IS NOT NULL
           AND e.{ct}_min_present_date >= fs.first_surgery_date
           AND NOT (
                e.{ct}_min_present_date <
                fs.first_surgery_date - INTERVAL '{PREOP_PROXIMITY_BUFFER_DAYS} days'
           )
        THEN TRUE ELSE FALSE
    END AS {ct}_new_postop,
    CASE
      WHEN e.{ct}_min_present_date IS NOT NULL
           AND fs.first_surgery_date IS NOT NULL
           AND e.{ct}_min_present_date >= fs.first_surgery_date
           AND e.{ct}_max_present_date IS NOT NULL
           AND e.{ct}_max_present_date <
               fs.first_surgery_date + INTERVAL '{TEMPORAL_RESOLUTION_WINDOW_DAYS} days'
        THEN TRUE ELSE FALSE
    END AS {ct}_transient,
    CASE
      WHEN e.{ct}_min_present_date IS NOT NULL
           AND fs.first_surgery_date IS NOT NULL
           AND e.{ct}_min_present_date >= fs.first_surgery_date
           AND e.{ct}_max_present_date IS NOT NULL
           AND e.{ct}_max_present_date >=
               fs.first_surgery_date + INTERVAL '{TEMPORAL_RESOLUTION_WINDOW_DAYS} days'
        THEN TRUE ELSE FALSE
    END AS {ct}_permanent""")
    temporal_outer_block = ",\n".join(temporal_outer_cols)

    coalesce_block = ",\n    ".join(
        f"COALESCE(e.{c}, FALSE) AS {c}" for c in rollup_cols_in_order
    )

    return f"""
CREATE OR REPLACE TABLE {target} AS
WITH all_patients AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id
    FROM "{PUB_DB}".main.canonical_patient_master
),
first_surgery AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(CAST(surgery_date_native AS DATE)) AS first_surgery_date
    FROM "{PUB_DB}".main.canonical_operative_events_v1
    WHERE surgery_date_native IS NOT NULL
    GROUP BY research_id
),
events_agg AS (
    SELECT
        research_id,
{inner_select_block},
        COUNT(DISTINCT CASE WHEN finding_status = 'present'
                            THEN complication_type END)
            AS n_complication_types_present,
        SUM(CASE WHEN finding_status = 'present' THEN 1 ELSE 0 END)
            AS n_complication_findings_total,
        MIN(CASE WHEN finding_status = 'present' THEN finding_date END)
            AS first_complication_date,
        MAX(CASE WHEN finding_status = 'present' THEN finding_date END)
            AS last_complication_date
    FROM {src}
    GROUP BY research_id
)
SELECT
    p.research_id,
    {coalesce_block},
    COALESCE(e.n_complication_types_present, 0) AS n_complication_types_present,
    COALESCE(e.n_complication_findings_total, 0) AS n_complication_findings_total,
    e.first_complication_date,
    e.last_complication_date,
{temporal_outer_block},
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts,
    CAST('389/Rule {rule}' AS VARCHAR) AS rebuild_lineage_v1
FROM all_patients p
LEFT JOIN events_agg   e  ON e.research_id  = p.research_id
LEFT JOIN first_surgery fs ON fs.research_id = p.research_id
"""


def phase2f_rebuild_complications_rollup(
    con, rule: str,
) -> dict[str, Any]:
    log("=" * 78)
    log(f"Phase 2F — rebuild complications rollup under Rule {rule}")
    log("=" * 78)

    target = "canonical_complications_patient_rollup_v1"
    if get_object_kind(con, "main", target) is None:
        raise SystemExit(f"Phase 2F abort: main.{target} missing")

    pre_rows = row_count(con, "main", target)
    pre_true_per_type: dict[str, int] = {}
    for ct in COMPLICATION_TYPES:
        col = f"ever_{ct}_any_evidence"
        if column_exists(con, target, col):
            pre_true_per_type[ct] = int(con.execute(
                f'SELECT SUM(CASE WHEN "{col}" THEN 1 ELSE 0 END) '
                f"FROM main.{target}"
            ).fetchone()[0] or 0)

    # Archive prior body to archive DB.
    log("  archive prior rollup table to archive DB")
    src_fq = f'"{PUB_DB}".main."{target}"'
    _archive_table_ctas(
        con,
        src_fq=src_fq,
        dest_schema=ARC_PUB_SCHEMA,
        dest_name=ARC_COMPLICATIONS_LEGACY,
        expected_rows=pre_rows,
    )

    log("  CREATE OR REPLACE TABLE under selected rule")
    sql = _build_rollup_sql(rule)
    con.execute(sql)
    new_rows = row_count(con, "main", target)
    if new_rows != pre_rows:
        warn(
            f"  rollup row count changed: {pre_rows:,} -> {new_rows:,} "
            "(should be == count of canonical_patient_master)"
        )

    new_true_per_type: dict[str, int] = {}
    for ct in COMPLICATION_TYPES:
        col = f"ever_{ct}_any_evidence"
        if column_exists(con, target, col):
            new_true_per_type[ct] = int(con.execute(
                f'SELECT SUM(CASE WHEN "{col}" THEN 1 ELSE 0 END) '
                f"FROM main.{target}"
            ).fetchone()[0] or 0)

    flips: dict[str, int] = {}
    for ct in COMPLICATION_TYPES:
        if ct in pre_true_per_type and ct in new_true_per_type:
            flips[ct] = pre_true_per_type[ct] - new_true_per_type[ct]
            log(f"  {ct:<26} pre={pre_true_per_type[ct]:>5} "
                f"new={new_true_per_type[ct]:>5} "
                f"Δ={flips[ct]:+d}")

    log_move(
        con, phase="2F",
        source_schema="main", source_name=target,
        dest_schema=ARC_PUB_SCHEMA, dest_name=ARC_COMPLICATIONS_LEGACY,
        move_method="CTAS-archive+CREATE OR REPLACE",
        n_rows=new_rows,
        reason=f"389/2F: rebuild under Rule {rule}",
    )
    return {
        "status": "done",
        "rule": rule,
        "n_rows_pre": pre_rows,
        "n_rows_new": new_rows,
        "any_evidence_pre": pre_true_per_type,
        "any_evidence_new": new_true_per_type,
        "flips_t2f_per_type": flips,
        "total_flips_t2f": sum(flips.values()),
    }


# --- 2G: dependent re-bind pass ------------------------------------------- #


def phase2g_rebind_dependents(con, deps: dict[str, list[str]]) -> dict[str, Any]:
    log("=" * 78)
    log("Phase 2G — dependent-view re-bind pass")
    log("=" * 78)
    all_deps: list[tuple[str, str]] = []
    for target, names in deps.items():
        for fq in names:
            schema, name = fq.split(".", 1)
            all_deps.append((schema, name))
    # De-dup, preserving order.
    seen: set[tuple[str, str]] = set()
    ordered: list[tuple[str, str]] = []
    for sn in all_deps:
        if sn not in seen:
            ordered.append(sn)
            seen.add(sn)
    log(f"  re-binding {len(ordered)} unique dependent view(s)")

    rebound: list[str] = []
    failed: list[tuple[str, str]] = []
    for schema, name in ordered:
        body = view_select_body(con, schema, name)
        if body is None:
            warn(f"  {schema}.{name}: no body found; skipping")
            continue
        try:
            con.execute(
                f'CREATE OR REPLACE VIEW "{PUB_DB}"."{schema}"."{name}" AS '
                f"{body}"
            )
            rebound.append(f"{schema}.{name}")
        except duckdb.Error as exc:
            err(f"  {schema}.{name}: REBIND FAILED — {exc!r}")
            failed.append((f"{schema}.{name}", str(exc)))
    if failed:
        raise SystemExit(
            f"Phase 2G abort: {len(failed)} dependent view(s) failed to "
            "recompile.  Investigate before retrying.  "
            "First failure: " + str(failed[0])
        )
    log(f"  rebound {len(rebound)} view(s) successfully")
    return {"status": "done", "n_rebound": len(rebound),
            "rebound": rebound, "n_failed": 0}


# --- 2H: post-state object count + registry + readme ---------------------- #


def phase2h_registry_readme(
    con, *,
    rebuild_rule: str,
    exam_result: dict[str, Any],
    patient_result: dict[str, Any],
    rollup_result: dict[str, Any],
) -> dict[str, Any]:
    log("=" * 78)
    log("Phase 2H — registry + __readme + post-state count")
    log("=" * 78)

    pub_count = total_pub_objects(con)
    log(f"  PUB total objects (post-apply): {pub_count}")

    # detail_table_registry_v1 — Phase 2B's carry-forward table is
    # retired alongside the zombie classifier reset (2026-04-22), so
    # there's nothing to register.  Phase 2H now only refreshes
    # __readme.

    # __readme bump.
    has_readme = get_object_kind(con, "main", "__readme")
    if has_readme:
        readme_lines = [
            f"389 | {RUN_STAMP} | REWRITE | "
            f"main.{US_EXAM_MASTER_VIEW} | filter NULL exam_date in "
            "source CTEs (CPM CTE retained for is_preop_exam); "
            f"~{exam_result.get('n_phantoms_eliminated', 0):,} phantom rows "
            "eliminated",
            f"389 | {RUN_STAMP} | REWRITE | "
            f"main.{US_PATIENT_MASTER_VIEW} | replace hardcoded "
            f"has_any_us=TRUE with derived value; new TRUE count: "
            f"{patient_result.get('n_has_any_us_true', 0):,}",
            f"389 | {RUN_STAMP} | REBUILD | "
            "main.canonical_complications_patient_rollup_v1 | "
            f"Rule {rebuild_rule}; "
            f"{rollup_result.get('total_flips_t2f', 0):,} patients flipped "
            "TRUE→FALSE at any_evidence tier",
        ]
        readme_cols = set(get_columns(con, "__readme"))
        if {"content", "updated_at"}.issubset(readme_cols):
            content = (
                "Script 389 deprecation/rebuild log:\n"
                + "\n".join(readme_lines)
            )
            # `updated_at` is sourced via inline SQL cast to avoid the
            # DuckDB TIMESTAMPTZ pytz pull-in (memory: never bind raw
            # CURRENT_TIMESTAMP).  Build an INSERT whose value list mixes
            # placeholders (for content + optional cols) with the inline
            # CAST(CURRENT_TIMESTAMP AS TIMESTAMP) literal.
            insert_cols_r: list[str] = ["content", "updated_at"]
            value_exprs: list[str] = ["?", "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)"]
            params: list[Any] = [content]
            if "script" in readme_cols:
                insert_cols_r.append("script")
                value_exprs.append("?")
                params.append(SCRIPT_TAG)
            if "git_sha" in readme_cols:
                insert_cols_r.append("git_sha")
                value_exprs.append("?")
                params.append(None)
            con.execute(
                f"INSERT INTO main.__readme ({', '.join(insert_cols_r)}) "
                f"VALUES ({', '.join(value_exprs)})",
                params,
            )
            log(f"  __readme: appended {len(readme_lines)} entry(ies)")
        else:
            warn("  __readme schema missing content/updated_at — skipping append")
    else:
        warn("  main.__readme not present — skipping readme bump")

    return {"pub_count_post": pub_count}


# --------------------------------------------------------------------------- #
# Phase 3 — post-state verification + close-out
# --------------------------------------------------------------------------- #


def phase3_postcheck(
    con,
    *,
    rule: str,
    preflight: dict[str, Any],
    zombie: dict[str, Any],
    views: dict[str, Any],
    deps: dict[str, list[str]],
    audit: dict[str, Any],
    apply_results: dict[str, Any],
) -> None:
    log("=" * 78)
    log("Phase 3 — post-state verification + close-out report")
    log("=" * 78)

    # 3A — re-probe key invariants (subset of Phase 0).
    new_buckets: dict[str, int] = {}
    case_sql = _classifier_case_sql()
    rows = con.execute(
        f"""
        SELECT bucket, COUNT(*) FROM (
            SELECT {case_sql} AS bucket
            FROM main.canonical_us_nodule_v2
        ) t GROUP BY bucket ORDER BY bucket
        """
    ).fetchall()
    for b, n in rows:
        new_buckets[b] = int(n)
    log(f"  post buckets: {new_buckets}")

    # 3B — 9340 audit re-check.
    audit_post: dict[str, Any] = {}
    for ct in ("rln_injury", "hypoparathyroidism"):
        col_any = f"ever_{ct}_any_evidence"
        col_pob = f"ever_{ct}_probable_or_better"
        col_def = f"ever_{ct}_definitive"
        vals: dict[str, Any] = {}
        for c in (col_any, col_pob, col_def):
            if column_exists(con, "canonical_complications_patient_rollup_v1", c):
                row = con.execute(
                    f'SELECT "{c}" '
                    "FROM main.canonical_complications_patient_rollup_v1 "
                    "WHERE research_id = ?",
                    [AUDIT_PATIENT_RID],
                ).fetchone()
                vals[c] = bool(row[0]) if row and row[0] is not None else None
        audit_post[ct] = vals
        log(f"  9340 {ct} -> {vals}")

    # 3C — has_any_us distribution.
    new_has_total = row_count(con, "main", US_PATIENT_MASTER_VIEW)
    new_has_true = int(con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}".main."{US_PATIENT_MASTER_VIEW}" '
        f'WHERE has_any_us = TRUE'
    ).fetchone()[0])
    new_has_false = new_has_total - new_has_true
    log(f"  patient_master post: total={new_has_total:,} "
        f"TRUE={new_has_true:,} FALSE={new_has_false:,}")

    # 3D — exam_master phantom recheck.
    new_exam_total = row_count(con, "main", US_EXAM_MASTER_VIEW)
    new_phantoms = int(con.execute(
        f'SELECT COUNT(*) FROM "{PUB_DB}".main."{US_EXAM_MASTER_VIEW}" '
        f'WHERE exam_date IS NULL'
    ).fetchone()[0])
    log(f"  exam_master post: total={new_exam_total:,} "
        f"phantoms (NULL exam_date)={new_phantoms:,}")

    # 3E — rollup invariant: one row per patient.
    n_rollup = row_count(con, "main", "canonical_complications_patient_rollup_v1")
    n_distinct = int(con.execute(
        "SELECT COUNT(DISTINCT research_id) "
        "FROM main.canonical_complications_patient_rollup_v1"
    ).fetchone()[0])
    if n_rollup != n_distinct:
        raise SystemExit(
            f"Phase 3 abort: rollup invariant broken — rows={n_rollup:,} "
            f"distinct_research_ids={n_distinct:,}"
        )
    log(f"  rollup invariant OK ({n_rollup:,} rows / "
        f"{n_distinct:,} distinct rids)")

    # Carry-forward table retired 2026-04-22 — no row count to report.
    n_cf = 0

    # Object count delta.  After the classifier reset, Phase 2 makes no
    # new BASE TABLES in `main` — expected delta is 0 (the per-script
    # workspace tables in manuscript_workspace are created in Phase 0).
    pub_count_post = total_pub_objects(con)
    pub_count_pre = preflight["pub_object_count"]
    log(f"  PUB object count: pre={pub_count_pre} post={pub_count_post} "
        f"Δ={pub_count_post - pub_count_pre:+d} "
        "(expected 0; carry-forward table retired)")

    write_close_out(
        rule=rule,
        preflight=preflight,
        zombie=zombie,
        views=views,
        deps=deps,
        audit=audit,
        apply_results=apply_results,
        post={
            "buckets": new_buckets,
            "audit_9340": audit_post,
            "patient_master_total": new_has_total,
            "patient_master_true": new_has_true,
            "patient_master_false": new_has_false,
            "exam_master_total": new_exam_total,
            "exam_master_phantoms": new_phantoms,
            "rollup_rows": n_rollup,
            "rollup_distinct_rids": n_distinct,
            "carry_forward_rows": n_cf,
            "pub_count_pre": pub_count_pre,
            "pub_count_post": pub_count_post,
        },
    )


def write_close_out(
    *,
    rule: str,
    preflight: dict[str, Any],
    zombie: dict[str, Any],
    views: dict[str, Any],
    deps: dict[str, list[str]],
    audit: dict[str, Any],
    apply_results: dict[str, Any],
    post: dict[str, Any],
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    rebuild = apply_results.get("phase2f", {})
    exam = apply_results.get("phase2d", {})
    patient = apply_results.get("phase2e", {})
    provenance = apply_results.get("phase2_provenance", {})
    rebind = apply_results.get("phase2g", {})

    lines: list[str] = []
    lines.append("# Script 389 — close-out report")
    lines.append("")
    lines.append(
        f"**Date:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}"
    )
    lines.append(f"**Script:** `scripts/{SCRIPT_TAG}.py`")
    lines.append(f"**Rule selected:** `{rule}`")
    lines.append(
        f"**Pre-state probe:** `{PRESTATE_PATH.relative_to(REPO_ROOT)}`"
    )
    lines.append("")

    lines.append("## Apply summary")
    lines.append("")
    lines += _md_table_header(["phase", "summary"])
    lines.append(
        f"| 2 (provenance) | classifier reset 2026-04-22 logged via "
        f"`main.__readme` ({provenance.get('status', 'unknown')}; "
        f"prior rows: {provenance.get('n_existing', 0)}) — "
        "Phases 2A/2B/2C retired |"
    )
    lines.append(
        f"| 2D | rewrite `{US_EXAM_MASTER_VIEW}`: filter NULL exam_date "
        "in source CTEs (CPM CTE retained for `is_preop_exam`); "
        f"{exam.get('n_rows_new', 0):,} rows post (was "
        f"{views['n_exam_rows_total']:,}); "
        f"~{exam.get('n_phantoms_eliminated', 0):,} phantoms eliminated |"
    )
    lines.append(
        f"| 2E | rewrite `{US_PATIENT_MASTER_VIEW}`: "
        f"has_any_us TRUE={patient.get('n_has_any_us_true', 0):,} "
        f"FALSE={patient.get('n_has_any_us_false', 0):,} "
        f"(was 100% TRUE / "
        f"{views['n_patient_has_any_us_true']:,}) |"
    )
    lines.append(
        f"| 2F | rebuild complications rollup under Rule {rule}; "
        f"{rebuild.get('total_flips_t2f', 0):,} patients flipped "
        "TRUE→FALSE at any_evidence tier "
        f"({len([t for t, v in rebuild.get('flips_t2f_per_type', {}).items() if v != 0])} "
        "complication types affected) |"
    )
    lines.append(
        f"| 2G | re-bound {rebind.get('n_rebound', 0)} dependent view(s) |"
    )
    lines.append(
        f"| 2H | PUB object count: {post['pub_count_pre']} -> "
        f"{post['pub_count_post']} (Δ "
        f"{post['pub_count_post'] - post['pub_count_pre']:+d}) |"
    )
    lines.append("")

    lines.append("## Phase 3 — post-state verification")
    lines.append("")
    lines += _md_table_header(["bucket", "pre", "post"])
    for name in EXPECTED_BUCKETS:
        pre_n = zombie["buckets"].get(name, 0)
        post_n = post["buckets"].get(name, 0)
        lines.append(f"| `{name}` | {pre_n:,} | {post_n:,} |")
    lines.append("")
    lines.append(
        f"* `{US_EXAM_MASTER_VIEW}` rows: "
        f"{views['n_exam_rows_total']:,} → "
        f"{post['exam_master_total']:,} "
        f"(phantoms: {views['n_exam_phantom_rows']:,} → "
        f"{post['exam_master_phantoms']:,})"
    )
    lines.append(
        f"* `{US_PATIENT_MASTER_VIEW}` has_any_us TRUE: "
        f"{views['n_patient_has_any_us_true']:,} → "
        f"{post['patient_master_true']:,}"
    )
    lines.append(
        f"* Rollup invariant: {post['rollup_rows']:,} rows / "
        f"{post['rollup_distinct_rids']:,} distinct research_ids "
        f"({'OK' if post['rollup_rows'] == post['rollup_distinct_rids'] else 'BROKEN'})"
    )
    lines.append("")

    lines.append(f"### Audit case research_id `{AUDIT_PATIENT_RID}`")
    lines.append("")
    for ct, vals in post["audit_9340"].items():
        lines.append(f"#### `{ct}` post-rebuild")
        for c, v in vals.items():
            lines.append(f"* `{c}` = `{v}`")
        lines.append("")

    lines.append("## Per-complication TRUE→FALSE flips (Rule "
                 f"{rule} applied)")
    lines.append("")
    lines += _md_table_header([
        "complication_type", "any_evidence pre", "any_evidence post",
        "Δ (TRUE→FALSE)",
    ])
    for ct in COMPLICATION_TYPES:
        pre_n = rebuild.get("any_evidence_pre", {}).get(ct)
        new_n = rebuild.get("any_evidence_new", {}).get(ct)
        flip = rebuild.get("flips_t2f_per_type", {}).get(ct)
        if pre_n is None and new_n is None:
            continue
        pre_s = f"{pre_n:,}" if isinstance(pre_n, int) else "—"
        new_s = f"{new_n:,}" if isinstance(new_n, int) else "—"
        flip_s = f"{flip:+d}" if isinstance(flip, int) else "—"
        lines.append(f"| `{ct}` | {pre_s} | {new_s} | {flip_s} |")
    lines.append("")

    lines.append("## Carry-forwards (declared, not auto-fixed)")
    lines.append("")
    lines.append(
        "1. **Complications rollup rule choice** — Rule "
        f"{rule} was selected; the other two rules' deltas remain in "
        "the pre-state probe report for reviewer reference."
    )
    lines.append(
        "2. **Upstream complication event de-duplication** — "
        "`complication_phenotype_v1` (structured) and "
        "`note_entities_complications` (legacy_entity) emit contradictory "
        "rows for the same (research_id, complication_type, finding_date) "
        "in some cases.  This is a builder-layer issue Script 389 does "
        "not address; flag for Script 390+."
    )
    lines.append(
        f"3. **US view-stack column compatibility** — the rewritten "
        f"`{US_EXAM_MASTER_VIEW}` preserves the full original column "
        "list (CPM CTE retained, only the `WHERE exam_date IS NOT NULL` "
        "guard added to source aggregations); column drops are not "
        "expected, but Phase 0D's dependent list should still be "
        "reviewed for behavioural changes downstream of the phantom "
        "elimination."
    )
    lines.append(
        "4. **Upstream NULL `exam_date` rows in US source tables** — "
        "Phase 0C measured ~6,785 NULL-date rows in "
        "`canonical_us_thyroid_gland_v2` and ~2,231 in "
        "`canonical_us_nodule_v2` (`canonical_us_lymph_node_v2` is "
        "clean).  Phase 2D filters them at the view layer, but the "
        "upstream data state is worth investigating in 390+: "
        "(a) legitimate \"date unavailable\" that shouldn't propagate, "
        "(b) ingestion bug, or (c) intentional pre-LLM backfill "
        "placeholders.  No row counts in `main.canonical_us_*_v2` are "
        "modified by 389."
    )
    lines.append(
        "5. **Pre-387 flag_event key collapses (7 tables)** — still "
        "carry-forward from Script 387; Script 389 does not touch "
        "these (separate upstream-builder fix)."
    )
    lines.append(
        "6. **CF-7 — Phase 0B classifier reset 2026-04-22** — Script "
        "389's classifier was reset from phantom baselines "
        "(18,310 / 17,090 / 2,152 / 27 = `clean_llm_parsed / "
        "clean_non_llm / zombie_parent / llm_parsed_but_blob`) to a "
        "live-derived source-flag partition (26,402 / 8,919 / 2,117 "
        "/ 141 = `clean_dual_source / clean_base_only / "
        "needs_backfill / aggregate_rollup`).  The original "
        "\"zombie / blob\" concept is retired.  If a content-based "
        "multi-nodule-blob audit is still wanted (e.g. rows with "
        "`length(location_raw) >= 400 OR semicolons >= 2` that were "
        "never split by the v2 nodule splitter — ~750 candidates), "
        "draft as a standalone Script 389b after 389 closes."
    )
    lines.append(
        "7. **CF-8 — `needs_backfill` orphan cohort** — 2,117 rows on "
        "`canonical_us_nodule_v2` have neither `source_tirads_llm` nor "
        "`source_base` parsed: 2,061 with `nlp_backfill_pending=TRUE` "
        "(legitimate awaiting-NLP) plus 56 with `nlp_backfill_pending"
        "=FALSE` (orphan cohort — why were these ingested without "
        "any source flag set?).  The 56-row orphan cohort needs a "
        "separate probe in 390+; Script 389 makes no changes to "
        "either subset."
    )
    lines.append("")

    lines.append("## Roll-back")
    lines.append("")
    lines.append(
        "Archive zone is **PUB-resident** (matches actual 387/388 "
        "landing pattern; no cross-DB CTAS used)."
    )
    lines.append("")
    lines.append(
        "* `canonical_us_nodule_v2` was NOT modified by Script 389 "
        "(zombie phases retired 2026-04-22); no rollback needed for "
        "the nodule table."
    )
    lines.append(
        f"* Prior complications rollup body: "
        f"`{PUB_DB}.{ARC_PUB_SCHEMA}.{ARC_COMPLICATIONS_LEGACY}` (CTAS "
        "back to restore)."
    )
    lines.append(
        f"* Prior exam_master view body: "
        f"`{PUB_DB}.{ARC_PUB_SCHEMA}.{ARC_EXAM_MASTER_LEGACY}_body` "
        "(execute the snapshot's SQL via CREATE OR REPLACE VIEW)."
    )
    lines.append(
        f"* Prior patient_master view body: "
        f"`{PUB_DB}.{ARC_PUB_SCHEMA}.{ARC_PATIENT_MASTER_LEGACY}_body` "
        "(same)."
    )
    lines.append(
        "* Classifier-reset provenance row in `main.__readme`: "
        "`DELETE FROM main.__readme WHERE content LIKE '%Phase 0B "
        "classifier reset 2026-04-22%'`."
    )
    lines.append("")

    CLOSEOUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"  wrote close-out -> {CLOSEOUT_PATH}")


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--apply", action="store_true",
        help="Execute Phases 2-3 (destructive).  Requires "
             f"{APPROVAL_PATH.relative_to(REPO_ROOT)} to be present.",
    )
    ap.add_argument(
        "--ignore-preflight", action="store_true",
        help="Skip the 386/388 evidence preflight (use only when those "
             "scripts wrote to a non-standard log location).",
    )
    args = ap.parse_args()

    log(f"Script 389 — {'APPLY' if args.apply else 'PROBE'} mode — "
        f"{datetime.now(timezone.utc).isoformat()}")

    con = connect()
    try:
        ensure_workspace_tables(con)

        # ---------- Phase 0 (always) ----------
        preflight = phase0a_preflight(con, args.ignore_preflight)
        zombie = phase0b_zombie_reprobe(con)
        views = phase0c_view_probe(con)
        deps = phase0d_dependents(con)
        audit = phase0e_complications_audit(con)
        phase0f_write_prestate(
            preflight=preflight, zombie=zombie, views=views, deps=deps,
            audit=audit,
        )

        if not args.apply:
            log("Phase 0 complete (probe-only mode).  Review "
                f"{PRESTATE_PATH.relative_to(REPO_ROOT)} and re-run with "
                "--apply (after writing the plan-approval file).")
            return 0

        # ---------- Phase 1 — gate ----------
        approval = parse_approval_file()
        if approval is None:
            phase1_plan_summary(
                zombie=zombie, views=views, deps=deps, audit=audit,
            )
            return 2
        rule = approval["RULE"]
        log(f"Plan approval parsed: RULE={rule}")

        # ---------- Phase 2 — apply ----------
        # Do all writes in one transaction-style session; halt on any
        # error.  Order is important:
        #   2A,2B,2C — touch canonical_us_nodule_v2 directly.
        #   2D       — rewrite exam_master VIEW (downstream of nodule).
        #   2E       — rewrite patient_master VIEW (downstream of exam).
        #   2F       — rebuild complications rollup (independent).
        #   2G       — re-bind all dependents (catches catalog breaks).
        #   2H       — registry + readme + PUB count delta.
        apply_results: dict[str, Any] = {}
        # Phase 2A retired 2026-04-22; replaced by classifier-reset
        # provenance row in main.__readme.  Phases 2B and 2C removed.
        apply_results["phase2_provenance"] = phase2_classifier_provenance(con)
        apply_results["phase2d"] = phase2d_rewrite_exam_master(con, views)
        apply_results["phase2e"] = phase2e_rewrite_patient_master(con, views)
        apply_results["phase2f"] = phase2f_rebuild_complications_rollup(
            con, rule,
        )
        apply_results["phase2g"] = phase2g_rebind_dependents(con, deps)
        apply_results["phase2h"] = phase2h_registry_readme(
            con,
            rebuild_rule=rule,
            exam_result=apply_results["phase2d"],
            patient_result=apply_results["phase2e"],
            rollup_result=apply_results["phase2f"],
        )

        # ---------- Phase 3 — verify + close-out ----------
        phase3_postcheck(
            con,
            rule=rule,
            preflight=preflight,
            zombie=zombie,
            views=views,
            deps=deps,
            audit=audit,
            apply_results=apply_results,
        )

        log("=" * 78)
        log("Script 389 APPLY complete.  Summary: " + json.dumps({
            "rule": rule,
            "classifier_provenance": apply_results["phase2_provenance"].get(
                "status", "unknown",
            ),
            "exam_master_rows_new": apply_results["phase2d"].get(
                "n_rows_new", 0,
            ),
            "patient_master_has_any_us_true": apply_results["phase2e"].get(
                "n_has_any_us_true", 0,
            ),
            "rollup_total_flips_t2f": apply_results["phase2f"].get(
                "total_flips_t2f", 0,
            ),
            "dependents_rebound": apply_results["phase2g"].get(
                "n_rebound", 0,
            ),
            "pub_count_post": apply_results["phase2h"].get(
                "pub_count_post", 0,
            ),
        }))
        return 0
    finally:
        flush_log()
        con.close()


if __name__ == "__main__":
    sys.exit(main())
