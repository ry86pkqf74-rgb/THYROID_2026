#!/usr/bin/env python3
"""Script 387 — `thyroid_canonical_publication_v1_0` full cleanup & dedup pass.

End-to-end driver for the cleanup described in
``cursor_prompts/CURSOR_PROMPT_PUB_V1_0_CLEANUP_20260422.md``.

Phases
------
1.  Pre-state snapshot + dep scan (read-only).
2.  Archive + drop the entire ``tier2`` schema (12 objects;
    ``frozen_section_event_v1`` is a VIEW that is materialised to a table).
3.  Archive + drop the entire ``verify`` schema (2 objects).
4.  Drop ``views_readable.survival_followup_VIEW_v1`` (no archive — it is a
    pure ``SELECT *`` shim around ``main.canonical_survival_followup_v1``).
5.  Archive + drop 13 stale ``manuscript_workspace`` artefacts
    (pre-s376 snapshot, candidate_* views, prompt5/6/7 logs).
6.  Within-canonical event-row dedup probe (~36 canonicals).  Read-only;
    halts and writes a markdown report if any ``*_patient_rollup_v1`` /
    ``canonical_patient_master`` row collapses, but never auto-fixes.
7.  Post-state verification (object counts, archive parity, log rows).
8.  Close-out markdown.

Safety
------
* Default mode is dry-run (Phase 1 only).  Pass ``--commit`` to run Phases 2-7.
* Every drop is preceded by an archive parity check
  (``COUNT(*)`` source == archive copy) — assertion failure aborts the run.
* All archives go to the internal MotherDuck DB ``"Thyroid 2026 UPdated"``
  in fresh schemas ``tier2_legacy_20260422``, ``verify_legacy_20260422`` and
  ``manuscript_workspace_legacy_20260422`` — never to public storage.
* No PHI: only ``research_id`` and aggregate counts are ever logged.
* No live-view reads from the archive DB: archives are reference-only.

Auth: ``motherduck_client.get_token()``.  Token never printed.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PUB_DB = "thyroid_canonical_publication_v1_0"
ARC_DB_RAW = "Thyroid 2026 UPdated"  # quoted at use-site
ARC_DB = f'"{ARC_DB_RAW}"'

WS_SCHEMA = "manuscript_workspace"
SCRIPT_TAG = "387_pub_v1_0_cleanup"

ARC_TIER2 = "tier2_legacy_20260422"
ARC_VERIFY = "verify_legacy_20260422"
ARC_WS = "manuscript_workspace_legacy_20260422"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
LOG_PATH = OUTPUT_DIR / "387_run.log"
DEDUP_REPORT_PATH = OUTPUT_DIR / "387_dedup_probe_report.md"
CLOSE_OUT_PATH = OUTPUT_DIR / "387_close_out.md"

# (src_schema, src_name, expected_kind)  — `kind` confirmed at runtime against
# information_schema; mismatch is fatal.
TIER2_TARGETS: list[tuple[str, str, str]] = [
    ("tier2", "airway_invasion_event_v1", "BASE TABLE"),
    ("tier2", "frozen_section_event_v1", "VIEW"),
    ("tier2", "past_surgical_hx_event_v1", "BASE TABLE"),
    ("tier2", "patient_tier2_master_v1", "BASE TABLE"),
    ("tier2", "vascular_invasion_event_v1", "BASE TABLE"),
    ("tier2", "dynamic_risk_response_event_v1", "BASE TABLE"),
    ("tier2", "functional_outcomes_event_v1", "BASE TABLE"),
    ("tier2", "parathyroid_detail_event_v1", "BASE TABLE"),
    ("tier2", "patient_decision_adherence_event_v1", "BASE TABLE"),
    ("tier2", "physical_exam_event_v1", "BASE TABLE"),
    ("tier2", "presenting_symptoms_event_v1", "BASE TABLE"),
    ("tier2", "rad_treatment_event_v1", "BASE TABLE"),
]

VERIFY_TARGETS: list[tuple[str, str, str]] = [
    ("verify", "concordance_master_v1", "BASE TABLE"),
    ("verify", "verify_long_v1", "BASE TABLE"),
]

VIEWS_READABLE_DUP = ("views_readable", "survival_followup_VIEW_v1", "VIEW")

WS_LEGACY_TARGETS: list[tuple[str, str, str]] = [
    (WS_SCHEMA, "canonical_us_nodule_v2_pre_s376_snapshot", "BASE TABLE"),
    # NOTE: candidate_us_patient_master_v2 (consumer view) must be archived
    # BEFORE candidate_us_exam_master_v2 (its source view) so the consumer
    # can still resolve while we materialise it.
    (WS_SCHEMA, "candidate_us_patient_master_v2", "VIEW"),
    (WS_SCHEMA, "candidate_us_exam_master_v2", "VIEW"),
    (WS_SCHEMA, "prompt5_remediation_log_v1", "BASE TABLE"),
    (WS_SCHEMA, "prompt5_remediation_summary_v1", "BASE TABLE"),
    (WS_SCHEMA, "prompt6_completion_audit_v1", "BASE TABLE"),
    (WS_SCHEMA, "prompt6_defer_log_v1", "BASE TABLE"),
    (WS_SCHEMA, "prompt6_older_master_decisions_v1", "BASE TABLE"),
    (WS_SCHEMA, "prompt6_poststate_v1", "BASE TABLE"),
    (WS_SCHEMA, "prompt6_prestate_v1", "BASE TABLE"),
    (WS_SCHEMA, "prompt6_view_rebuild_log_v1", "BASE TABLE"),
    (WS_SCHEMA, "prompt6_wiring_gap_remediation_v1", "BASE TABLE"),
    (WS_SCHEMA, "prompt7_handoff_v1", "BASE TABLE"),
]

# Targets that may be "drop-only with provenance pointer" when their
# upstream has already been archived (the source data is preserved in the
# archive DB; only the view body needs cleanup).  Maps source name to the
# fully-qualified upstream archive location for the log.
DROP_ONLY_FALLBACK: dict[str, str] = {
    "candidate_us_patient_master_v2":
        '"Thyroid 2026 UPdated".manuscript_workspace_legacy_20260422'
        '."candidate_us_exam_master_v2"',
}

# Phase 6 dedup probe: 36 canonicals
CANONICALS_TO_PROBE: list[str] = [
    "canonical_complications_events_v1",
    "canonical_complications_patient_rollup_v1",
    "canonical_fna_events_v1",
    "canonical_fna_patient_rollup_v1",
    "canonical_frozen_section_events_v1",
    "canonical_frozen_section_patient_rollup_v1",
    "canonical_invasion_events_v1",
    "canonical_invasion_patient_rollup_v1",
    "canonical_labs_calcium_v1",
    "canonical_labs_pth_v1",
    "canonical_labs_thyroglobulin_v1",
    "canonical_labs_tsh_v1",
    "canonical_labs_vitamin_d_v1",
    "canonical_medications_events_v1",
    "canonical_medications_patient_rollup_v1",
    "canonical_molecular_genetics_v2",
    "canonical_molecular_genetics_from_notes_v2",
    "canonical_operative_events_v1",
    "canonical_operative_patient_rollup_v1",
    "canonical_operative_procedure_codes_v1",
    "canonical_path_benign_events_v1",
    "canonical_path_benign_patient_rollup_v1",
    "canonical_path_gland_events_v1",
    "canonical_path_gland_patient_rollup_v1",
    "canonical_path_malignant_events_v1",
    "canonical_path_malignant_patient_rollup_v1",
    "canonical_patient_master",
    "canonical_pmh_events_v1",
    "canonical_pmh_patient_rollup_v1",
    "canonical_psh_events_v1",
    "canonical_psh_patient_rollup_v1",
    "canonical_recurrence_v1",
    "canonical_survival_followup_v1",
    "canonical_us_lymph_node_v2",
    "canonical_us_nodule_v2",
    "canonical_us_thyroid_gland_v2",
]

# Per-table key candidate stacks (best key first).  Each candidate is a list of
# columns; the first whose columns *all* exist on the table is used.
#
# Generic candidate stack for `*_events_v1` tables that don't have a per-table
# override.  Note-derived event tables get the richer
# (research_id, note_row_id, evidence_start, ...) keys; legacy synoptic/labs
# event rows fall back to (research_id, finding_date, value_raw).
EVENT_KEY_CANDIDATES: list[list[str]] = [
    ["research_id", "note_row_id", "evidence_start", "evidence_end"],
    ["research_id", "note_row_id", "evidence_start"],
    ["research_id", "source_table", "source_row_id", "finding_date"],
    ["research_id", "source_table", "source_row_id"],
    ["research_id", "event_date", "value_raw"],
    ["research_id", "event_date"],
    ["research_id", "finding_date", "finding_value_norm"],
    ["research_id", "finding_date"],
]
LAB_KEY_CANDIDATES: list[list[str]] = [
    ["research_id", "lab_datetime", "analyte", "value_raw", "source"],
    ["research_id", "lab_datetime", "value_raw", "source"],
    ["research_id", "lab_datetime", "value_numeric", "source"],
    ["research_id", "lab_datetime", "source"],
    ["research_id", "lab_datetime"],
]

# Per-table overrides — picked in preference to the generic stack for the
# table.  Each override is itself a candidate stack: best key first.  Keys are
# chosen using the same column-existence test, so adding a richer fallback is
# always safe.
PER_TABLE_KEY_OVERRIDES: dict[str, list[list[str]]] = {
    # --- events with a single deterministic identifier ----------------------
    "canonical_fna_events_v1": [["fna_event_id"]],
    "canonical_invasion_events_v1": [["invasion_event_id"]],
    "canonical_operative_events_v1": [["surgery_episode_id"]],
    "canonical_operative_procedure_codes_v1": [["procedure_mention_id"]],
    # --- events keyed via evidence_span_hash (note-grain) -------------------
    "canonical_complications_events_v1": [
        ["research_id", "evidence_span_hash"],
        ["research_id", "source_table", "source_row_id",
         "complication_type", "finding_date"],
    ],
    "canonical_medications_events_v1": [
        ["research_id", "evidence_span_hash"],
        ["research_id", "source_table", "source_row_id",
         "finding_value_norm", "finding_date"],
    ],
    "canonical_pmh_events_v1": [
        ["research_id", "evidence_span_hash"],
        ["research_id", "source_table", "source_row_id",
         "finding_value_norm", "finding_date"],
    ],
    "canonical_psh_events_v1": [
        ["research_id", "evidence_span_hash"],
        ["research_id", "source_table", "source_row_id",
         "finding_value_norm", "finding_date"],
    ],
    # --- events keyed via per-row index on (rid, frozen_event_index, ...) ---
    "canonical_frozen_section_events_v1": [
        ["entity_id_hash"],
        ["research_id", "frozen_event_index"],
    ],
    # --- pathology events: synoptic-row + specimen grain --------------------
    "canonical_path_benign_events_v1": [
        ["research_id", "surgery_episode_id", "synoptic_row_ix",
         "specimen_id"],
        ["research_id", "surgery_episode_id", "synoptic_row_ix"],
    ],
    "canonical_path_gland_events_v1": [
        ["research_id", "surgery_episode_id", "synoptic_row_ix",
         "specimen_id", "gland_position"],
        ["research_id", "surgery_episode_id", "synoptic_row_ix",
         "specimen_id"],
    ],
    "canonical_path_malignant_events_v1": [
        ["specimen_focus_id"],
        ["research_id", "surgery_episode_id", "tumor_ordinal"],
    ],
    # --- molecular ----------------------------------------------------------
    "canonical_molecular_genetics_v2": [
        ["molecular_episode_id"],
        ["research_id", "resolved_test_date", "platform"],
        ["research_id", "test_date_native", "platform"],
    ],
    "canonical_molecular_genetics_from_notes_v2": [
        ["research_id", "note_row_id", "entity_type", "evidence_start"],
        ["research_id", "note_row_id", "entity_value_norm", "evidence_start"],
    ],
    # --- US (per-structure) -------------------------------------------------
    "canonical_us_lymph_node_v2": [
        ["research_id", "us_exam_id", "us_ln_id"],
        ["research_id", "us_exam_id", "us_ln_index_within_exam"],
    ],
    "canonical_us_nodule_v2": [
        ["research_id", "us_exam_id", "nodule_id"],
        ["research_id", "us_exam_id", "nodule_index"],
    ],
    "canonical_us_thyroid_gland_v2": [
        ["research_id", "us_exam_id", "laterality"],
        ["research_id", "us_exam_id"],
    ],
    # --- recurrence + survival (per-patient) --------------------------------
    # canonical_recurrence_v1 is per-patient (1 row per rid), and
    # `recurrence_date` is NULL for the no-recurrence majority — use
    # research_id directly.
    "canonical_recurrence_v1": [["research_id"]],
    "canonical_survival_followup_v1": [["research_id"]],
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
            f"Archive DB '{ARC_DB_RAW}' not attached — cannot run cleanup"
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


def get_object_kind(con, schema: str, name: str) -> str | None:
    """Return ``BASE TABLE``, ``VIEW`` or ``None`` (object missing)."""
    row = con.execute(
        """
        SELECT table_type FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [PUB_DB, schema, name],
    ).fetchone()
    return row[0] if row else None


def schema_exists(con, schema: str, db: str = PUB_DB) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.schemata
        WHERE catalog_name = ? AND schema_name = ?
        """,
        [db, schema],
    ).fetchone()
    return row is not None


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


def schema_object_counts(con, schema: str) -> tuple[int, int]:
    """Return ``(base_table_count, view_count)`` for one schema in PUB."""
    rows = con.execute(
        """
        SELECT table_type, COUNT(*) FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
        GROUP BY table_type
        """,
        [PUB_DB, schema],
    ).fetchall()
    counts = dict(rows)
    return (counts.get("BASE TABLE", 0), counts.get("VIEW", 0))


# --------------------------------------------------------------------------- #
# Workspace tables
# --------------------------------------------------------------------------- #


def ensure_workspace_tables(con) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.archive_move_log_v1 (
            moved_at   TIMESTAMP,
            src_schema VARCHAR,
            src_table  VARCHAR,
            archive_fq VARCHAR,
            n_rows     BIGINT,
            reason     VARCHAR,
            script     VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.script_387_prestate_v1 (
            src_schema       VARCHAR,
            src_name         VARCHAR,
            obj_type         VARCHAR,
            row_count        BIGINT,
            archive_schema   VARCHAR,
            archive_fq       VARCHAR,
            archive_row_count BIGINT,
            build_ts         TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.script_387_dedup_probe_v1 (
            canonical_name VARCHAR,
            partition_key  VARCHAR,
            total_rows     BIGINT,
            distinct_keys  BIGINT,
            collapse_count BIGINT,
            null_key_rows  BIGINT,
            severity       VARCHAR,
            build_ts       TIMESTAMP
        )
    """)
    # Schema migration: earlier development versions of this script created
    # the table without `null_key_rows` / `severity`.  Add them on resume.
    for col, ddl in (
        ("null_key_rows", "BIGINT"),
        ("severity", "VARCHAR"),
    ):
        con.execute(
            f'ALTER TABLE manuscript_workspace.script_387_dedup_probe_v1 '
            f'ADD COLUMN IF NOT EXISTS {col} {ddl}'
        )


# --------------------------------------------------------------------------- #
# Phase 1 — pre-state snapshot + dep scan
# --------------------------------------------------------------------------- #


def all_targets() -> list[tuple[str, str, str, str]]:
    """Return ``(src_schema, src_name, expected_kind, archive_schema)``."""
    return (
        [(s, n, k, ARC_TIER2) for s, n, k in TIER2_TARGETS]
        + [(s, n, k, ARC_VERIFY) for s, n, k in VERIFY_TARGETS]
        + [(VIEWS_READABLE_DUP[0], VIEWS_READABLE_DUP[1],
            VIEWS_READABLE_DUP[2], "<dropped, no archive>")]
        + [(s, n, k, ARC_WS) for s, n, k in WS_LEGACY_TARGETS]
    )


def archive_present(con, archive_schema: str, src_name: str) -> int | None:
    """Return row count of the archive copy if present, else None."""
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [ARC_DB_RAW, archive_schema, src_name],
    ).fetchone()
    if not row:
        return None
    return con.execute(
        f'SELECT COUNT(*) FROM {ARC_DB}."{archive_schema}"."{src_name}"'
    ).fetchone()[0]


def already_logged(con, src_schema: str, src_name: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM manuscript_workspace.archive_move_log_v1
        WHERE script = ? AND src_schema = ? AND src_table = ?
        LIMIT 1
        """,
        [SCRIPT_TAG, src_schema, src_name],
    ).fetchone()
    return row is not None


def phase1_prestate(con, commit: bool) -> list[dict]:
    log("=" * 78)
    log(f"Phase 1 — pre-state snapshot (commit={commit})")
    log("=" * 78)

    targets = all_targets()
    snapshots: list[dict] = []
    missing: list[str] = []
    kind_mismatches: list[str] = []

    for src_schema, src_name, expected_kind, archive_schema in targets:
        kind = get_object_kind(con, src_schema, src_name)
        if kind is None:
            arc_n = (
                archive_present(con, archive_schema, src_name)
                if archive_schema != "<dropped, no archive>"
                else None
            )
            if arc_n is not None or already_logged(con, src_schema, src_name):
                snapshots.append({
                    "src_schema": src_schema,
                    "src_name": src_name,
                    "obj_type": expected_kind,
                    "row_count": arc_n if arc_n is not None else 0,
                    "status": "already_archived",
                })
                log(
                    f"  ALREADY  {src_schema:>22}.{src_name:<48} "
                    f"{expected_kind:<10} arc_rows="
                    f"{(arc_n if arc_n is not None else 0):,} "
                    "(idempotent skip)"
                )
                continue
            missing.append(f"{src_schema}.{src_name}")
            continue

        if kind != expected_kind:
            kind_mismatches.append(
                f"{src_schema}.{src_name}: expected={expected_kind} actual={kind}"
            )
        try:
            n = row_count(con, src_schema, src_name)
        except duckdb.Error as exc:
            err(f"  row_count failed for {src_schema}.{src_name}: {exc}")
            n = -1
        snapshots.append({
            "src_schema": src_schema,
            "src_name": src_name,
            "obj_type": kind,
            "row_count": n,
            "status": "pending",
        })
        log(f"  present  {src_schema:>22}.{src_name:<48} {kind:<10} rows={n:,}")

    if missing:
        err("Missing targets (no archive copy found either):")
        for m in missing:
            err(f"  {m}")
        raise SystemExit("Phase 1 abort: target objects missing")

    if kind_mismatches:
        err("Object-kind mismatches (cannot proceed):")
        for m in kind_mismatches:
            err(f"  {m}")
        raise SystemExit("Phase 1 abort: kind mismatch — investigate manually")

    log("Dep scan: live VIEWs that reference tier2.* or verify.*")
    rows = con.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.views
        WHERE table_catalog = ?
          AND (view_definition ILIKE '%tier2.%'
               OR view_definition ILIKE '%verify.%')
          AND NOT (table_schema = 'tier2'
                   AND table_name = 'frozen_section_event_v1')
        ORDER BY table_schema, table_name
        """,
        [PUB_DB],
    ).fetchall()
    if rows:
        err("Live views still reference tier2.* / verify.*:")
        for s, t in rows:
            err(f"  {s}.{t}")
        raise SystemExit(
            "Phase 1 abort: dependents added since prompt was authored"
        )
    log("  dep scan clean (0 rows)")

    log("Dep scan: views that reference views_readable.survival_followup_VIEW_v1")
    rows = con.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.views
        WHERE table_catalog = ?
          AND view_definition ILIKE '%survival_followup_VIEW_v1%'
          AND NOT (table_schema = 'views_readable'
                   AND table_name = 'survival_followup_VIEW_v1')
        ORDER BY table_schema, table_name
        """,
        [PUB_DB],
    ).fetchall()
    if rows:
        err("Live views reference views_readable.survival_followup_VIEW_v1:")
        for s, t in rows:
            err(f"  {s}.{t}")
        raise SystemExit("Phase 1 abort: VIEW has live dependents")
    log("  dep scan clean (0 rows)")

    if commit:
        # Idempotent upsert: keep existing rows for already-archived items
        # (they already have archive_schema / archive_fq / archive_row_count
        # populated from a prior run); only refresh the still-pending rows.
        for s in snapshots:
            existing = con.execute(
                """
                SELECT 1 FROM manuscript_workspace.script_387_prestate_v1
                WHERE src_schema = ? AND src_name = ?
                """,
                [s["src_schema"], s["src_name"]],
            ).fetchone()
            if existing:
                if s["status"] == "pending":
                    con.execute(
                        """
                        UPDATE manuscript_workspace.script_387_prestate_v1
                           SET obj_type = ?, row_count = ?,
                               build_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
                         WHERE src_schema = ? AND src_name = ?
                        """,
                        [s["obj_type"], s["row_count"],
                         s["src_schema"], s["src_name"]],
                    )
            else:
                con.execute(
                    """
                    INSERT INTO manuscript_workspace.script_387_prestate_v1
                      (src_schema, src_name, obj_type, row_count,
                       archive_schema, archive_fq, archive_row_count, build_ts)
                    VALUES (?, ?, ?, ?, NULL, NULL, NULL,
                            CAST(CURRENT_TIMESTAMP AS TIMESTAMP))
                    """,
                    [s["src_schema"], s["src_name"],
                     s["obj_type"], s["row_count"]],
                )
        log(
            f"  upserted {len(snapshots)} pre-state rows into "
            "manuscript_workspace.script_387_prestate_v1"
        )

    return snapshots


# --------------------------------------------------------------------------- #
# Phase 2-5 — archive_and_drop helper
# --------------------------------------------------------------------------- #


def archive_and_drop(
    con,
    src_schema: str,
    src_name: str,
    obj_kind: str,
    archive_schema: str,
    reason: str,
    commit: bool,
) -> dict:
    """Materialise -> CTAS to archive -> parity check -> drop -> log.

    Returns ``{src_schema, src_name, archive_fq, src_rows, arc_rows, status}``.

    Idempotent: if ``src_schema.src_name`` is missing from PUB but the
    archive copy is present, returns the prior archive row count without
    re-running anything.

    Drop-only fallback: if the source is a VIEW whose upstream has already
    been archived (so ``COUNT(*)`` raises a ``CatalogException``) and the
    name appears in ``DROP_ONLY_FALLBACK``, the view is dropped without an
    archive copy and the upstream archive pointer is recorded in the log.
    """
    src_fq = f'"{PUB_DB}"."{src_schema}"."{src_name}"'
    arc_fq = f'{ARC_DB}."{archive_schema}"."{src_name}"'

    if get_object_kind(con, src_schema, src_name) is None:
        arc_rows_existing = archive_present(con, archive_schema, src_name)
        if arc_rows_existing is not None:
            log(
                f"  -> {src_schema}.{src_name} ({obj_kind}) — "
                "ALREADY archived & dropped, skipping"
            )
            return {
                "src_schema": src_schema,
                "src_name": src_name,
                "archive_fq": arc_fq,
                "src_rows": arc_rows_existing,
                "arc_rows": arc_rows_existing,
                "status": "already_done",
            }
        if already_logged(con, src_schema, src_name):
            log(
                f"  -> {src_schema}.{src_name} ({obj_kind}) — "
                "ALREADY logged in archive_move_log_v1 (drop-only?), skipping"
            )
            return {
                "src_schema": src_schema,
                "src_name": src_name,
                "archive_fq": arc_fq,
                "src_rows": 0,
                "arc_rows": 0,
                "status": "already_logged",
            }
        raise SystemExit(
            f"archive_and_drop: source {src_schema}.{src_name} missing and "
            "no archive copy present — refusing to log a phantom drop"
        )

    try:
        src_rows = row_count(con, src_schema, src_name)
    except duckdb.CatalogException as exc:
        if obj_kind == "VIEW" and src_name in DROP_ONLY_FALLBACK:
            upstream = DROP_ONLY_FALLBACK[src_name]
            log(
                f"  -> {src_schema}.{src_name} (VIEW) — broken (upstream "
                "archived). Drop-only fallback."
            )
            log(f"     CatalogException details: {exc!r}")
            if not commit:
                return {
                    "src_schema": src_schema,
                    "src_name": src_name,
                    "archive_fq": upstream,
                    "src_rows": -1,
                    "arc_rows": -1,
                    "status": "drop_only_dryrun",
                }
            con.execute(f'DROP VIEW {src_fq}')
            log(f"     dropped broken view {src_schema}.{src_name}")
            con.execute(
                """
                INSERT INTO manuscript_workspace.archive_move_log_v1
                  (moved_at, src_schema, src_table, archive_fq, n_rows,
                   reason, script)
                VALUES (CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
                        ?, ?, ?, NULL, ?, ?)
                """,
                [src_schema, src_name, upstream,
                 f"{reason}; drop-only fallback (upstream already archived "
                 f"at {upstream})",
                 SCRIPT_TAG],
            )
            con.execute(
                """
                UPDATE manuscript_workspace.script_387_prestate_v1
                   SET archive_schema = ?, archive_fq = ?,
                       archive_row_count = row_count
                 WHERE src_schema = ? AND src_name = ?
                """,
                [archive_schema, upstream, src_schema, src_name],
            )
            return {
                "src_schema": src_schema,
                "src_name": src_name,
                "archive_fq": upstream,
                "src_rows": 0,
                "arc_rows": 0,
                "status": "drop_only",
            }
        raise

    log(f"  -> {src_schema}.{src_name} ({obj_kind}, {src_rows:,} rows)")

    if not commit:
        log(f"     dry-run: would CTAS to {arc_fq}, drop {obj_kind}")
        return {
            "src_schema": src_schema,
            "src_name": src_name,
            "archive_fq": arc_fq,
            "src_rows": src_rows,
            "arc_rows": -1,
            "status": "dryrun",
        }

    con.execute(f'CREATE SCHEMA IF NOT EXISTS {ARC_DB}."{archive_schema}"')

    already = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [ARC_DB_RAW, archive_schema, src_name],
    ).fetchone()
    if already:
        arc_rows_existing = con.execute(
            f"SELECT COUNT(*) FROM {arc_fq}"
        ).fetchone()[0]
        if arc_rows_existing != src_rows:
            raise SystemExit(
                f"Archive {arc_fq} already exists with "
                f"{arc_rows_existing} rows but src has {src_rows}; refusing"
            )
        log(
            f"     archive already present ({arc_rows_existing:,} rows) — "
            "skipping CTAS"
        )
        arc_rows = arc_rows_existing
    else:
        con.execute(f"CREATE TABLE {arc_fq} AS SELECT * FROM {src_fq}")
        arc_rows = con.execute(f"SELECT COUNT(*) FROM {arc_fq}").fetchone()[0]
        if arc_rows != src_rows:
            raise SystemExit(
                f"ARCHIVE PARITY FAIL {src_schema}.{src_name}: "
                f"src={src_rows} arc={arc_rows}"
            )
        log(f"     archived ({arc_rows:,} rows; parity OK)")

    drop_kind = "VIEW" if obj_kind == "VIEW" else "TABLE"
    con.execute(f'DROP {drop_kind} {src_fq}')
    log(f"     dropped {obj_kind.lower()} {src_schema}.{src_name}")

    con.execute(
        """
        INSERT INTO manuscript_workspace.archive_move_log_v1
          (moved_at, src_schema, src_table, archive_fq, n_rows, reason, script)
        VALUES (CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?, ?, ?, ?, ?)
        """,
        [src_schema, src_name, arc_fq, src_rows, reason, SCRIPT_TAG],
    )

    con.execute(
        """
        UPDATE manuscript_workspace.script_387_prestate_v1
           SET archive_schema = ?,
               archive_fq = ?,
               archive_row_count = ?
         WHERE src_schema = ? AND src_name = ?
        """,
        [archive_schema, arc_fq, arc_rows, src_schema, src_name],
    )

    return {
        "src_schema": src_schema,
        "src_name": src_name,
        "archive_fq": arc_fq,
        "src_rows": src_rows,
        "arc_rows": arc_rows,
        "status": "archived",
    }


def phase2_tier2(con, commit: bool) -> list[dict]:
    log("=" * 78)
    log(f"Phase 2 — tier2 archive ({len(TIER2_TARGETS)} objects)")
    log("=" * 78)
    results = [
        archive_and_drop(
            con, s, n, k, ARC_TIER2,
            "Script 387: collapse legacy tier2 schema (canonical replacements live in main.*)",
            commit,
        )
        for s, n, k in TIER2_TARGETS
    ]
    if commit:
        remaining = con.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = 'tier2'
            """,
            [PUB_DB],
        ).fetchone()[0]
        if remaining:
            raise SystemExit(
                f"Phase 2 abort: tier2 still has {remaining} objects after archive"
            )
        if schema_exists(con, "tier2"):
            con.execute('DROP SCHEMA "tier2" CASCADE')
            log("  DROP SCHEMA tier2 CASCADE — done")
    return results


def phase3_verify(con, commit: bool) -> list[dict]:
    log("=" * 78)
    log(f"Phase 3 — verify archive ({len(VERIFY_TARGETS)} objects)")
    log("=" * 78)
    results = [
        archive_and_drop(
            con, s, n, k, ARC_VERIFY,
            "Script 387: drop legacy verify schema (concordance pipeline retired)",
            commit,
        )
        for s, n, k in VERIFY_TARGETS
    ]
    if commit:
        remaining = con.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = 'verify'
            """,
            [PUB_DB],
        ).fetchone()[0]
        if remaining:
            raise SystemExit(
                f"Phase 3 abort: verify still has {remaining} objects after archive"
            )
        if schema_exists(con, "verify"):
            con.execute('DROP SCHEMA "verify" CASCADE')
            log("  DROP SCHEMA verify CASCADE — done")
    return results


def phase4_views_readable_dup(con, commit: bool) -> dict:
    log("=" * 78)
    log("Phase 4 — drop views_readable.survival_followup_VIEW_v1 duplicate")
    log("=" * 78)
    s, n, _ = VIEWS_READABLE_DUP
    src_fq = f'"{PUB_DB}"."{s}"."{n}"'

    if get_object_kind(con, s, n) is None:
        if already_logged(con, s, n):
            log(f"  -> {s}.{n} — ALREADY dropped & logged, skipping")
            return {"src_schema": s, "src_name": n, "src_rows": 0,
                    "status": "already_done"}
        raise SystemExit(
            f"Phase 4: {s}.{n} missing from PUB but no log row — investigate"
        )

    rows = row_count(con, s, n)
    log(f"  -> {s}.{n} ({rows:,} rows; pure SELECT * shim)")
    if not commit:
        log("     dry-run: would DROP VIEW (no archive needed)")
        return {"src_schema": s, "src_name": n, "src_rows": rows}
    con.execute(f"DROP VIEW {src_fq}")
    log(f"     dropped VIEW {s}.{n}")
    con.execute(
        """
        INSERT INTO manuscript_workspace.archive_move_log_v1
          (moved_at, src_schema, src_table, archive_fq, n_rows, reason, script)
        VALUES (CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?, NULL, ?, ?, ?)
        """,
        [s, n, rows,
         "Script 387: drop duplicate VIEW (canonical kept as views_readable.Survival_Followup)",
         SCRIPT_TAG],
    )
    con.execute(
        """
        UPDATE manuscript_workspace.script_387_prestate_v1
           SET archive_schema = '<dropped, no archive>',
               archive_fq = NULL,
               archive_row_count = 0
         WHERE src_schema = ? AND src_name = ?
        """,
        [s, n],
    )
    return {"src_schema": s, "src_name": n, "src_rows": rows}


def phase5_workspace_legacy(con, commit: bool) -> list[dict]:
    log("=" * 78)
    log(f"Phase 5 — manuscript_workspace legacy archive "
        f"({len(WS_LEGACY_TARGETS)} objects)")
    log("=" * 78)
    return [
        archive_and_drop(
            con, s, n, k, ARC_WS,
            "Script 387: archive stale prompt5/6/7 + candidate_* + pre-s376 snapshot",
            commit,
        )
        for s, n, k in WS_LEGACY_TARGETS
    ]


# --------------------------------------------------------------------------- #
# Phase 6 — within-canonical dedup probe
# --------------------------------------------------------------------------- #


def _pick_key(cols: set[str], candidates: list[list[str]]) -> list[str] | None:
    for cand in candidates:
        if all(c in cols for c in cand):
            return cand
    return None


def choose_partition_key(
    con, canonical_name: str
) -> tuple[list[str] | None, set[str]]:
    """Pick the richest applicable partition key for ``canonical_name``.

    Resolution order:
      1. Hard rule for `*_patient_rollup_v1` / `canonical_patient_master`
         (must be `research_id`).
      2. Per-table override stack in PER_TABLE_KEY_OVERRIDES.
      3. Generic stack: labs / events.
    """
    cols = get_columns(con, canonical_name)
    if not cols:
        return None, cols

    if (
        canonical_name.endswith("_patient_rollup_v1")
        or canonical_name == "canonical_patient_master"
    ):
        return ["research_id"] if "research_id" in cols else None, cols

    if canonical_name in PER_TABLE_KEY_OVERRIDES:
        key = _pick_key(cols, PER_TABLE_KEY_OVERRIDES[canonical_name])
        if key is not None:
            return key, cols

    if canonical_name.startswith("canonical_labs_"):
        return _pick_key(cols, LAB_KEY_CANDIDATES), cols

    if canonical_name.endswith("_events_v1"):
        return _pick_key(cols, EVENT_KEY_CANDIDATES), cols

    return None, cols


def _key_sql(cols: list[str]) -> str:
    quoted = ", ".join(f'"{c}"' for c in cols)
    return f"({quoted})" if len(cols) > 1 else f'"{cols[0]}"'


def _key_label(cols: list[str]) -> str:
    return "(" + ", ".join(cols) + ")"


def severity_for(
    canonical_name: str,
    collapse_count: int,
    total_rows: int,
    null_key_rows: int,
) -> str:
    if total_rows > 0 and null_key_rows == total_rows:
        # The chosen partition key is NULL for every row — picker found a
        # sham key (e.g. synoptic_row_ix is entirely NULL on a path event
        # table).  Surface for review; treat as no-key rather than ok.
        return "all_null_key"
    if collapse_count == 0:
        return "ok"
    if (
        canonical_name.endswith("_patient_rollup_v1")
        or canonical_name == "canonical_patient_master"
    ):
        return "fail_rollup"
    return "flag_event"


def phase6_dedup(con, commit: bool) -> list[dict]:
    log("=" * 78)
    log(f"Phase 6 — dedup probe across {len(CANONICALS_TO_PROBE)} canonicals")
    log("=" * 78)

    if commit:
        con.execute(
            "DELETE FROM manuscript_workspace.script_387_dedup_probe_v1"
        )

    results: list[dict] = []
    for canonical in CANONICALS_TO_PROBE:
        cols_check = get_columns(con, canonical)
        if not cols_check:
            err(f"  {canonical}: NOT FOUND in main; skipping")
            results.append({
                "canonical_name": canonical,
                "partition_key": None,
                "total_rows": -1,
                "distinct_keys": -1,
                "collapse_count": -1,
                "severity": "missing",
            })
            continue

        key, _ = choose_partition_key(con, canonical)
        if key is None:
            err(
                f"  {canonical}: no usable partition key from candidate stacks; "
                "skipping"
            )
            results.append({
                "canonical_name": canonical,
                "partition_key": "<no_key_chosen>",
                "total_rows": -1,
                "distinct_keys": -1,
                "collapse_count": -1,
                "severity": "no_key",
            })
            continue

        null_clause = " OR ".join(f'"{c}" IS NULL' for c in key)
        sql = (
            f"SELECT COUNT(*) AS total_rows, "
            f"COUNT(DISTINCT {_key_sql(key)}) AS distinct_keys, "
            f"SUM(CASE WHEN {null_clause} THEN 1 ELSE 0 END) AS null_key_rows "
            f'FROM "{PUB_DB}".main."{canonical}"'
        )
        total, distinct, null_keys = con.execute(sql).fetchone()
        null_keys = int(null_keys or 0)
        # COUNT(DISTINCT ...) ignores NULLs, so the populated-row count is
        # what we should compare against to detect a *real* collapse.
        populated = total - null_keys
        collapse = max(populated - distinct, 0)
        sev = severity_for(canonical, collapse, total, null_keys)
        marker = "ok" if sev == "ok" else sev.upper()
        log(
            f"  {canonical:<48} key={_key_label(key):<55} "
            f"rows={total:>8,} distinct={distinct:>8,} "
            f"null_key={null_keys:>6,} collapse={collapse:>6,} [{marker}]"
        )
        results.append({
            "canonical_name": canonical,
            "partition_key": _key_label(key),
            "total_rows": total,
            "distinct_keys": distinct,
            "collapse_count": collapse,
            "null_key_rows": null_keys,
            "severity": sev,
        })

        if commit:
            con.execute(
                """
                INSERT INTO manuscript_workspace.script_387_dedup_probe_v1
                  (canonical_name, partition_key, total_rows, distinct_keys,
                   collapse_count, null_key_rows, severity, build_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?,
                        CAST(CURRENT_TIMESTAMP AS TIMESTAMP))
                """,
                [canonical, _key_label(key), total, distinct,
                 collapse, null_keys, sev],
            )

    write_dedup_report(results)
    return results


def write_dedup_report(results: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fail_rollup = [r for r in results if r["severity"] == "fail_rollup"]
    flag_event = [r for r in results if r["severity"] == "flag_event"]
    all_null = [r for r in results if r["severity"] == "all_null_key"]
    no_key = [r for r in results if r["severity"] in ("no_key", "missing")]
    ok = [r for r in results if r["severity"] == "ok"]

    lines = [
        "# Script 387 — Within-Canonical Dedup Probe Report",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Tables probed: {len(results)}  ·  ok: {len(ok)}  ·  "
        f"event-flag: {len(flag_event)}  ·  rollup-fail: {len(fail_rollup)}  "
        f"·  all_null_key: {len(all_null)}  ·  no_key/missing: {len(no_key)}",
        "",
        "Severity legend:",
        "* `ok` — collapse_count == 0 on a populated partition key",
        "* `flag_event` — collapse_count > 0 on an `*_events_v1` "
        "(richer key may be needed; manual review)",
        "* `fail_rollup` — collapse_count > 0 on a `*_patient_rollup_v1` "
        "or `canonical_patient_master` (HARD FAIL; rollup invariant violated)",
        "* `all_null_key` — every row has NULL in at least one column of "
        "the chosen key (the picker found a sham key; needs an alternate "
        "partition key)",
        "* `no_key` / `missing` — could not choose a partition key (table "
        "missing or no candidate columns matched)",
        "",
        "## Summary table",
        "",
        "Note: `collapse` is computed as `(total_rows - null_key_rows) - "
        "distinct_keys`, so a non-zero `null_key` column is what to "
        "investigate when collapse looks suspicious — `COUNT(DISTINCT)` "
        "discards NULLs and would otherwise overstate the collapse.",
        "",
        "| canonical | partition key | rows | distinct | null_key | collapse "
        "| severity |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for r in results:
        lines.append(
            f"| `{r['canonical_name']}` | `{r['partition_key']}` "
            f"| {r['total_rows']:,} | {r['distinct_keys']:,} "
            f"| {r.get('null_key_rows', 0):,} | {r['collapse_count']:,} "
            f"| {r['severity']} |"
        )

    if fail_rollup:
        lines += ["", "## ⚠ Rollup invariant violations"]
        for r in fail_rollup:
            lines.append(
                f"* **{r['canonical_name']}** — key `{r['partition_key']}`, "
                f"{r['collapse_count']:,} duplicate-key rows out of "
                f"{r['total_rows']:,}"
            )

    if flag_event:
        lines += ["", "## Event-table collapse flags (review-only)"]
        for r in flag_event:
            null_kr = r.get("null_key_rows", 0)
            null_note = (
                f" · {null_kr:,} rows have NULL in a key column (likely "
                "sparse-key artefact, not a true collapse)"
                if null_kr > 0 else ""
            )
            lines.append(
                f"* `{r['canonical_name']}` — key `{r['partition_key']}`, "
                f"{r['collapse_count']:,} collapses out of "
                f"{r['total_rows']:,} rows{null_note}"
            )

    if all_null:
        lines += ["", "## Sham keys (key column entirely NULL)"]
        for r in all_null:
            lines.append(
                f"* `{r['canonical_name']}` — chosen key "
                f"`{r['partition_key']}` is NULL on all "
                f"{r['total_rows']:,} rows; pick another key for this "
                "table"
            )

    if no_key:
        lines += ["", "## Tables skipped (no key / missing)"]
        for r in no_key:
            lines.append(f"* `{r['canonical_name']}` — severity `{r['severity']}`")

    DEDUP_REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"  wrote dedup report -> {DEDUP_REPORT_PATH}")


# --------------------------------------------------------------------------- #
# Phase 7 — post-state verification
# --------------------------------------------------------------------------- #


def phase7_postcheck(
    con,
    archive_results: list[dict],
    drop_only_results: list[dict],
    dedup_results: list[dict],
) -> dict:
    log("=" * 78)
    log("Phase 7 — post-state verification")
    log("=" * 78)

    counts = {}
    for sch in (
        "main", "manuscript_workspace", "raw", "tier2",
        "verify", "views_readable",
    ):
        b, v = schema_object_counts(con, sch)
        counts[sch] = {"BASE TABLE": b, "VIEW": v, "total": b + v}
        log(f"  {sch:<22} BASE={b:>4} VIEW={v:>4} total={b + v:>4}")

    pub_total = sum(c["total"] for c in counts.values())
    log(f"  PUB total objects: {pub_total}")

    for sch in ("tier2", "verify"):
        if counts[sch]["total"] != 0:
            raise SystemExit(
                f"Phase 7 abort: schema {sch} still has "
                f"{counts[sch]['total']} objects"
            )

    log_rows = con.execute(
        """
        SELECT COUNT(*) FROM manuscript_workspace.archive_move_log_v1
        WHERE script = ?
        """,
        [SCRIPT_TAG],
    ).fetchone()[0]
    log(f"  archive_move_log_v1 rows for script={SCRIPT_TAG}: {log_rows}")

    # On an idempotent re-run, archive_move_log_v1 will already contain rows
    # for the items archived in the prior partial run; we only insert new
    # rows for items still in 'pending' state at the start of this run.  So
    # the log row count is >= len(archive_results) + len(drop_only_results)
    # but never less.
    minimum_expected = (
        len([r for r in archive_results if r.get("status") != "already_done"])
        + len([r for r in drop_only_results if r.get("status") != "already_done"])
    )
    if log_rows < minimum_expected:
        raise SystemExit(
            f"Phase 7 abort: archive_move_log_v1 has only {log_rows} rows "
            f"for script={SCRIPT_TAG}, expected at least {minimum_expected}"
        )

    parity_violations = con.execute(
        """
        SELECT src_schema, src_name, row_count, archive_row_count
        FROM manuscript_workspace.script_387_prestate_v1
        WHERE archive_fq IS NOT NULL
          AND row_count IS DISTINCT FROM archive_row_count
        """
    ).fetchall()
    if parity_violations:
        err("Archive parity violations in script_387_prestate_v1:")
        for r in parity_violations:
            err(f"  {r[0]}.{r[1]}: src={r[2]} arc={r[3]}")
        raise SystemExit("Phase 7 abort: archive parity drift")
    log("  archive parity check OK (all archived sources match copies)")

    rollup_fails = [r for r in dedup_results if r["severity"] == "fail_rollup"]
    if rollup_fails:
        err("Rollup invariant failures detected:")
        for r in rollup_fails:
            err(
                f"  {r['canonical_name']}: collapse={r['collapse_count']:,} "
                f"(key={r['partition_key']})"
            )
        raise SystemExit(
            "Phase 7 abort: *_patient_rollup_v1 / canonical_patient_master "
            "row collapses detected"
        )

    return {"counts": counts, "pub_total": pub_total, "log_rows": log_rows}


# --------------------------------------------------------------------------- #
# Phase 8 — close-out
# --------------------------------------------------------------------------- #


def phase8_close_out(
    archive_results: list[dict],
    drop_only_results: list[dict],
    dedup_results: list[dict],
    post_state: dict,
) -> None:
    log("=" * 78)
    log("Phase 8 — close-out")
    log("=" * 78)

    archived_by_schema: dict[str, list[dict]] = {}
    for r in archive_results:
        archived_by_schema.setdefault(r["src_schema"], []).append(r)

    flag_event = [r for r in dedup_results if r["severity"] == "flag_event"]
    fail_rollup = [r for r in dedup_results if r["severity"] == "fail_rollup"]
    all_null = [r for r in dedup_results if r["severity"] == "all_null_key"]
    no_key = [r for r in dedup_results if r["severity"] in ("no_key", "missing")]

    counts = post_state["counts"]
    cnt = lambda s, t: counts.get(s, {}).get(t, 0)  # noqa: E731

    lines = [
        "# Script 387 — `thyroid_canonical_publication_v1_0` cleanup close-out",
        "",
        f"**Date:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}",
        f"**Script:** `scripts/{SCRIPT_TAG}.py`",
        "**Prompt:** `cursor_prompts/CURSOR_PROMPT_PUB_V1_0_CLEANUP_20260422.md`",
        "",
        "## End-state object counts (PUB)",
        "",
        "| Schema | BASE | VIEW | total |",
        "|---|---:|---:|---:|",
        f"| main | {cnt('main', 'BASE TABLE')} | {cnt('main', 'VIEW')} "
        f"| {counts['main']['total']} |",
        f"| manuscript_workspace | {cnt('manuscript_workspace', 'BASE TABLE')} "
        f"| {cnt('manuscript_workspace', 'VIEW')} "
        f"| {counts['manuscript_workspace']['total']} |",
        f"| raw | {cnt('raw', 'BASE TABLE')} | {cnt('raw', 'VIEW')} "
        f"| {counts['raw']['total']} |",
        f"| tier2 | {cnt('tier2', 'BASE TABLE')} | {cnt('tier2', 'VIEW')} "
        f"| {counts['tier2']['total']} |",
        f"| verify | {cnt('verify', 'BASE TABLE')} | {cnt('verify', 'VIEW')} "
        f"| {counts['verify']['total']} |",
        f"| views_readable | {cnt('views_readable', 'BASE TABLE')} "
        f"| {cnt('views_readable', 'VIEW')} "
        f"| {counts['views_readable']['total']} |",
        f"| **TOTAL** | | | **{post_state['pub_total']}** |",
        "",
        "Note: `manuscript_workspace.script_387_prestate_v1` and "
        "`manuscript_workspace.script_387_dedup_probe_v1` were created by this "
        "script and remain for audit; they are included in the BASE count "
        "above (so the net delta differs from the prompt's idealised -11 by "
        "+2 in BASE).",
        "",
        "## Archive DB delta (`md:\"Thyroid 2026 UPdated\"`)",
        "",
        "| Archive schema | objects archived |",
        "|---|---:|",
        f"| `{ARC_TIER2}` | {len(archived_by_schema.get('tier2', []))} |",
        f"| `{ARC_VERIFY}` | {len(archived_by_schema.get('verify', []))} |",
        f"| `{ARC_WS}` | {len(archived_by_schema.get(WS_SCHEMA, []))} |",
        "",
        "## `archive_move_log_v1` rows (script = `387_pub_v1_0_cleanup`)",
        "",
        f"* Archived (CTAS + DROP): {len(archive_results)}",
        f"* Drop-only (no archive): {len(drop_only_results)} "
        "(the views_readable duplicate)",
        f"* Total log rows for this script: {post_state['log_rows']}",
        "",
        "## Phase 6 — dedup probe outcomes",
        "",
        f"* Tables probed: {len(dedup_results)}",
        f"* `ok` (collapse=0): "
        f"{len([r for r in dedup_results if r['severity'] == 'ok'])}",
        f"* `flag_event` (events with key collapse — review only): "
        f"{len(flag_event)}",
        f"* `fail_rollup` (rollup invariant violation — HARD FAIL): "
        f"{len(fail_rollup)}",
        f"* `all_null_key` (chosen key NULL on every row — needs alternate "
        f"key): {len(all_null)}",
        f"* `no_key` / `missing`: {len(no_key)}",
        "",
        "Full table-by-table report: "
        "`scripts/output/387_dedup_probe_report.md`.",
        "",
    ]

    if flag_event:
        lines += ["### Event-table flags (carry-forward; not auto-fixed)", ""]
        for r in flag_event:
            lines.append(
                f"* `{r['canonical_name']}` — key `{r['partition_key']}`, "
                f"{r['collapse_count']:,} duplicate-key rows out of "
                f"{r['total_rows']:,}"
            )
        lines.append("")
    else:
        lines += ["All event tables are dedup-clean under their chosen keys.", ""]

    lines += [
        "## Reusable patterns (for the next tier-2 / verify-style close-out)",
        "",
        "* Cross-DB CTAS works in a single MotherDuck session because the "
        "archive DB is auto-attached; no explicit `ATTACH` needed.",
        "* `frozen_section_event_v1` was a VIEW — CTAS materialises the "
        "result-set into a TABLE in the archive DB; drop the source with "
        "`DROP VIEW` (never `DROP TABLE` on a view).",
        "* `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` is mandatory for any new "
        "`build_ts` column to avoid the DuckDB TIMESTAMPTZ → pytz pull-in.",
        "* `archive_and_drop` is idempotent: pre-existing archive copies are "
        "row-count verified and the CTAS step skipped.",
        "* Pre-state snapshot tables (`script_387_prestate_v1`, "
        "`script_387_dedup_probe_v1`) are intentionally retained — they are "
        "the post-mortem record for this run.",
        "",
        "## Carry-forward items",
        "",
    ]
    if fail_rollup or flag_event or all_null:
        if fail_rollup:
            lines.append(
                "* HARD FAIL: rollup invariant violations above must be "
                "investigated before any further canonical work."
            )
        if flag_event:
            lines.append(
                "* Event-table key collapses above need manual key-richness "
                "review (e.g. add `note_row_id` / `evidence_start` to the "
                "partition key in upstream rollup builders)."
            )
        if all_null:
            lines.append(
                "* Sham-key tables above (`all_null_key`) need an alternate "
                "partition key choice in `PER_TABLE_KEY_OVERRIDES` for the "
                "next probe pass."
            )
    else:
        lines.append("* None — all archive parity checks and rollup probes "
                     "are clean.")
    lines.append("")

    CLOSE_OUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"  wrote close-out -> {CLOSE_OUT_PATH}")


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--commit",
        action="store_true",
        help="Execute Phases 2-7 (destructive). Without this, runs Phase 1 + "
             "Phase 6 (read-only probe) only.",
    )
    ap.add_argument(
        "--skip-dedup",
        action="store_true",
        help="Skip Phase 6 (useful when only verifying the archive flow).",
    )
    args = ap.parse_args()

    con = connect()
    try:
        ensure_workspace_tables(con)

        phase1_prestate(con, commit=args.commit)

        if args.commit:
            tier2_results = phase2_tier2(con, commit=True)
            verify_results = phase3_verify(con, commit=True)
            ws_dup_result = phase4_views_readable_dup(con, commit=True)
            ws_legacy_results = phase5_workspace_legacy(con, commit=True)
            archive_results = (
                tier2_results + verify_results + ws_legacy_results
            )
            drop_only_results = [ws_dup_result]
        else:
            log("Skipping Phases 2-5 (dry-run; pass --commit to execute)")
            archive_results = []
            drop_only_results = []

        if args.skip_dedup:
            dedup_results: list[dict] = []
        else:
            dedup_results = phase6_dedup(con, commit=args.commit)

        if args.commit:
            post_state = phase7_postcheck(
                con, archive_results, drop_only_results, dedup_results,
            )
            phase8_close_out(
                archive_results, drop_only_results, dedup_results, post_state,
            )
        else:
            log("Skipping Phases 7-8 (dry-run)")

        return 0
    finally:
        flush_log()
        con.close()


if __name__ == "__main__":
    sys.exit(main())
