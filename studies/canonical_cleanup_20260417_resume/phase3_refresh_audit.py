"""Phase 3b — refresh canonical_cleanup_audit_v1 with signal-based classifier.

Steps:
  1. Snapshot the existing audit table to
     manuscript_workspace.canonical_cleanup_audit_v1_snapshot_20260417,
     with a COMMENT noting it preserves the placeholder classification.
  2. DROP & recreate manuscript_workspace.canonical_cleanup_audit_v1 with
     the richer schema:
       object_name, object_type, status, destination, reason,
       row_count, n_distinct_research_id,
       is_referenced_by_view, is_referenced_by_script,
       is_identical_to_twin,
       n_view_refs, n_script_refs,
       has_version_twin, twin_name,
       classifier_version, last_modified_in_db, notes,
       classified_at
  3. INSERT 118 rows for main.* (loaded from phase3_object_signals.json),
     marking data_dictionary_v266a's notes with the v240 lineage.
  4. INSERT 2 explicit audit-trail rows for manuscript_workspace.*:
       lab_orphan_audit_v1, lab_orphan_cohort_review_v1
     with notes 'audit trail for 2026-04-17 Tg orphan cohort decision (Phase 2)'.
  5. INSERT cpm_reconciliation_provenance_v1 row for Phase 3.
  6. Re-assert CPM invariant.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG_PATH = HERE / "phase3_refresh.log"
SIGNALS_PATH = HERE / "phase3_object_signals.json"

PHASE3_START = "2026-04-18 03:59:31.758720+00:00"
RUN_ID = "canonical_cleanup_resume_20260417_phase3"
CLASSIFIER_VERSION = "v2_signal_based_20260417"

LINEAGE_NOTE_V266A = (
    "replaces data_dictionary_v240 (archived to "
    '"Thyroid 2026 UPdated".archive_pub_v1_0 by 266c Phase 5 archive sweep '
    "2026-04-18; lineage preserved here)"
)
NOTE_AUDIT_TRAIL = (
    "audit trail for 2026-04-17 Tg orphan cohort decision (Phase 2)"
)


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def find_at_columns(con) -> dict[str, str]:
    """Return {table_name: latest_at_col_name} for tables in main with a
    TIMESTAMP column ending in _at."""
    rows = con.execute(
        """
        SELECT table_name, column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_catalog='thyroid_canonical_publication_v1_0'
          AND table_schema='main'
          AND column_name LIKE '%_at'
          AND data_type ILIKE '%timestamp%'
        ORDER BY table_name, ordinal_position
        """
    ).fetchall()
    out: dict[str, str] = {}
    # Prefer cpm_built_at / phenotyped_at / refined_at / rebuilt_at / built_at /
    # snapshotted_at / created_at / updated_at / classified_at — else first found
    pref = (
        "cpm_built_at",
        "phenotyped_at",
        "refined_at",
        "rebuilt_at",
        "built_at",
        "snapshotted_at",
        "created_at",
        "updated_at",
        "classified_at",
    )
    by_table: dict[str, list[str]] = {}
    for r in rows:
        by_table.setdefault(r[0], []).append(r[1])
    for tbl, cols in by_table.items():
        chosen = next((c for c in pref if c in cols), cols[0])
        out[tbl] = chosen
    return out


def main() -> int:
    LOG_PATH.write_text("")
    con = connect_locked()
    log("Phase 3b — refresh canonical_cleanup_audit_v1 starting...")

    # ---------- 0. Load signals ----------
    if not SIGNALS_PATH.exists():
        raise SystemExit(f"Signals JSON missing at {SIGNALS_PATH}")
    signals = json.loads(SIGNALS_PATH.read_text())
    log(f"  loaded {len(signals)} per-object signal rows from JSON")

    # ---------- 0a. Lineage check ----------
    n_v266a = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='main' AND table_name='data_dictionary_v266a'"
    ).fetchone()[0]
    if n_v266a != 1:
        raise SystemExit(
            f"STOP: data_dictionary_v266a not present in main "
            f"(count={n_v266a})"
        )
    n_v240 = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='main' AND table_name='data_dictionary_v240'"
    ).fetchone()[0]
    if n_v240 != 0:
        raise SystemExit(
            f"STOP: data_dictionary_v240 still present in main "
            f"(count={n_v240}); lineage assumption broken"
        )
    log(
        "  Lineage check OK: data_dictionary_v240 absent, "
        "data_dictionary_v266a present (1 row in info_schema)"
    )

    # ---------- 1. Snapshot ----------
    log(
        "Snapshotting current audit table to "
        "canonical_cleanup_audit_v1_snapshot_20260417..."
    )
    n_pre = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.canonical_cleanup_audit_v1"
    ).fetchone()[0]
    log(f"  pre-snapshot audit row count: {n_pre}")
    con.execute(
        "DROP TABLE IF EXISTS "
        "manuscript_workspace.canonical_cleanup_audit_v1_snapshot_20260417"
    )
    con.execute(
        """
        CREATE TABLE
          manuscript_workspace.canonical_cleanup_audit_v1_snapshot_20260417 AS
        SELECT *, CURRENT_TIMESTAMP AS snapshotted_at
        FROM manuscript_workspace.canonical_cleanup_audit_v1
        """
    )
    n_snap = con.execute(
        "SELECT COUNT(*) FROM "
        "manuscript_workspace.canonical_cleanup_audit_v1_snapshot_20260417"
    ).fetchone()[0]
    if n_snap != n_pre:
        raise SystemExit(
            f"STOP: snapshot row count mismatch: snap={n_snap} pre={n_pre}"
        )
    con.execute(
        "COMMENT ON TABLE "
        "manuscript_workspace.canonical_cleanup_audit_v1_snapshot_20260417 IS "
        "'Pre-refresh snapshot of canonical_cleanup_audit_v1 (115 "
        "placeholder-classification rows, including the stale "
        "data_dictionary_v240 row whose object was archived to "
        "\"Thyroid 2026 UPdated\".archive_pub_v1_0 by 266c Phase 5) taken "
        "before the 2026-04-17 signal-based refresh. Retained for "
        "provenance.'"
    )
    log(f"  snapshot ok ({n_snap} rows captured + snapshotted_at column)")

    # ---------- 2. DROP & recreate ----------
    log("DROP & recreate manuscript_workspace.canonical_cleanup_audit_v1...")
    con.execute(
        "DROP TABLE IF EXISTS manuscript_workspace.canonical_cleanup_audit_v1"
    )
    con.execute(
        """
        CREATE TABLE manuscript_workspace.canonical_cleanup_audit_v1 (
            object_name              VARCHAR,
            object_type              VARCHAR,
            status                   VARCHAR,
            destination              VARCHAR,
            reason                   VARCHAR,
            row_count                BIGINT,
            n_distinct_research_id   BIGINT,
            is_referenced_by_view    BOOLEAN,
            is_referenced_by_script  BOOLEAN,
            is_identical_to_twin     BOOLEAN,
            n_view_refs              INTEGER,
            n_script_refs            INTEGER,
            has_version_twin         BOOLEAN,
            twin_name                VARCHAR,
            classifier_version       VARCHAR,
            last_modified_in_db      TIMESTAMP,
            notes                    VARCHAR,
            classified_at            TIMESTAMP WITH TIME ZONE
        )
        """
    )

    # ---------- 3. last_modified_in_db probe ----------
    log("Probing each main table for a *_at TIMESTAMP column for "
        "last_modified_in_db...")
    at_cols = find_at_columns(con)
    log(f"  {len(at_cols)} main tables have a *_at TIMESTAMP column")
    last_mod: dict[str, str | None] = {}
    for name, rec in signals.items():
        col = at_cols.get(name)
        if col is None:
            last_mod[name] = None
            continue
        try:
            v = con.execute(
                f'SELECT MAX("{col}") FROM main."{name}"'
            ).fetchone()[0]
            last_mod[name] = str(v) if v is not None else None
        except Exception:
            last_mod[name] = None
    log(
        f"  resolved last_modified_in_db for "
        f"{sum(1 for v in last_mod.values() if v)} tables"
    )

    # ---------- 4. INSERT main rows ----------
    log("INSERTing 118 main.* rows...")
    insert_rows = []
    for name in sorted(signals.keys()):
        rec = signals[name]
        notes = None
        if name == "data_dictionary_v266a":
            notes = LINEAGE_NOTE_V266A
        insert_rows.append(
            (
                name,
                rec["object_type"],
                rec["action"],          # status (LIVE for all 118)
                rec.get("destination") or None,
                rec.get("reason") or None,
                rec.get("row_count"),
                rec.get("n_distinct_research_id"),
                bool(rec["is_referenced_by_view"]),
                bool(rec["is_referenced_by_script"]),
                rec.get("is_identical_to_twin"),
                int(rec.get("n_view_refs", 0)),
                int(rec.get("n_script_refs", 0)),
                bool(rec["has_version_twin"]),
                rec.get("twin_name"),
                CLASSIFIER_VERSION,
                last_mod.get(name),
                notes,
            )
        )
    con.executemany(
        """
        INSERT INTO manuscript_workspace.canonical_cleanup_audit_v1
          (object_name, object_type, status, destination, reason,
           row_count, n_distinct_research_id,
           is_referenced_by_view, is_referenced_by_script,
           is_identical_to_twin,
           n_view_refs, n_script_refs,
           has_version_twin, twin_name,
           classifier_version, last_modified_in_db, notes,
           classified_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                CAST(? AS TIMESTAMP), ?, CURRENT_TIMESTAMP)
        """,
        insert_rows,
    )

    # ---------- 5. INSERT 2 audit-trail rows for manuscript_workspace ----------
    log("INSERTing 2 manuscript_workspace audit-trail rows...")
    for tbl in ("lab_orphan_audit_v1", "lab_orphan_cohort_review_v1"):
        n = con.execute(
            f'SELECT COUNT(*) FROM manuscript_workspace."{tbl}"'
        ).fetchone()[0]
        # research_id distinct (cast)
        try:
            n_rid = con.execute(
                f'SELECT COUNT(DISTINCT TRY_CAST(research_id AS BIGINT)) '
                f'FROM manuscript_workspace."{tbl}"'
            ).fetchone()[0]
        except Exception:
            n_rid = None
        con.execute(
            """
            INSERT INTO manuscript_workspace.canonical_cleanup_audit_v1
              (object_name, object_type, status, destination, reason,
               row_count, n_distinct_research_id,
               is_referenced_by_view, is_referenced_by_script,
               is_identical_to_twin,
               n_view_refs, n_script_refs,
               has_version_twin, twin_name,
               classifier_version, last_modified_in_db, notes,
               classified_at)
            VALUES (?, 'BASE TABLE', 'LIVE', NULL,
                    'audit trail for 2026-04-17 Tg orphan cohort decision',
                    ?, ?, FALSE, TRUE, NULL, 0, 1,
                    FALSE, NULL, ?, NULL, ?, CURRENT_TIMESTAMP)
            """,
            [f"manuscript_workspace.{tbl}", n, n_rid, CLASSIFIER_VERSION,
             NOTE_AUDIT_TRAIL],
        )

    n_audit_after = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.canonical_cleanup_audit_v1"
    ).fetchone()[0]
    log(f"  audit table refreshed: {n_audit_after} rows total (expected 120)")
    if n_audit_after != 120:
        raise SystemExit(f"STOP: audit row count {n_audit_after} != 120")

    # Quick verification dump
    by_status = con.execute(
        "SELECT status, COUNT(*) "
        "FROM manuscript_workspace.canonical_cleanup_audit_v1 "
        "GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    log(f"  status distribution after refresh: {by_status}")

    # ---------- 6. Phase 3 provenance row ----------
    log("Inserting Phase 3 provenance row...")
    n_before = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "WHERE run_id = ?",
        [RUN_ID],
    ).fetchone()[0]
    if n_before:
        log(f"  removing {n_before} prior row(s) for run_id={RUN_ID}")
        con.execute(
            "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
            "WHERE run_id = ?",
            [RUN_ID],
        )
    con.execute(
        """
        INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
          (run_id, started_at, ended_at, phases_applied,
           critical_findings_cleared, high_findings_cleared,
           med_findings_cleared, held_for_adjudication)
        VALUES (?, ?::TIMESTAMPTZ, CURRENT_TIMESTAMP,
                'archive_deprecate_delete__classifier_clean__audit_refreshed',
                '0', '0', '0', '0')
        """,
        [RUN_ID, PHASE3_START],
    )
    n_total = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1"
    ).fetchone()[0]
    log(f"  cpm_reconciliation_provenance_v1 total rows now: {n_total}")

    # ---------- 7. CPM invariant ----------
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit(
            f"CPM invariant regressed: {n_rows}/{n_distinct} != 10871/10871"
        )
    log(f"  CPM invariant re-asserted: {n_rows}/{n_distinct}")
    log("Phase 3b complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
