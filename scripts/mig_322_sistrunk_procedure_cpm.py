#!/usr/bin/env python3
"""mig_322 — Sistrunk keyword pass from operative notes → canonical_patient_master.

Populates:
  * main.extracted_sistrunk_procedure_opnote_v1 — one row per operative-note hit
    (no note body stored; evidence_summary is a fixed paraphrase template).
  * main.canonical_patient_master — sistrunk_procedure BOOLEAN + provenance cols.

DFL (before apply): DFL-20260506-SISTRUNKPARSE | action_type=schema-add+extract

Usage:
  .venv/bin/python scripts/mig_322_sistrunk_procedure_cpm.py --dry-run
  .venv/bin/python scripts/mig_322_sistrunk_procedure_cpm.py --apply
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(REPO_ROOT))

PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
def _note_surrogate_key(research_id: str, note_text: str) -> str:
    blob = f"{research_id}\0{(note_text or '')[:8000]}"
    return hashlib.sha256(blob.encode("utf-8", errors="replace")).hexdigest()[:32]


NEW_CPM_COLS: tuple[tuple[str, str], ...] = (
    ("sistrunk_procedure", "BOOLEAN"),
    ("sistrunk_procedure_evidence_summary", "VARCHAR"),
    ("sistrunk_procedure_match_kind", "VARCHAR"),
    ("sistrunk_procedure_match_offset", "BIGINT"),
    ("sistrunk_procedure_parser_rule_id", "VARCHAR"),
    ("sistrunk_procedure_evidence_note_row_id", "VARCHAR"),
)


def _col_exists(con, col: str) -> bool:
    row = con.execute(
        """
SELECT COUNT(*) FROM information_schema.columns
WHERE table_catalog = ?
  AND table_schema = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = ?
""",
        [PUBLICATION_DB, col],
    ).fetchone()
    return bool(row and row[0] > 0)


def _ensure_cpm_columns(con, log) -> None:
    for name, typ in NEW_CPM_COLS:
        if _col_exists(con, name):
            continue
        log(f"ALTER TABLE ADD COLUMN {name} {typ}")
        con.execute(f'ALTER TABLE "{PUBLICATION_DB}".main.canonical_patient_master '
                    f"ADD COLUMN {name} {typ}")


def _reset_cpm_sistrunk(con) -> None:
    con.execute(
        f"""
UPDATE "{PUBLICATION_DB}".main.canonical_patient_master
SET
  sistrunk_procedure = FALSE,
  sistrunk_procedure_evidence_summary = NULL,
  sistrunk_procedure_match_kind = NULL,
  sistrunk_procedure_match_offset = NULL,
  sistrunk_procedure_parser_rule_id = NULL,
  sistrunk_procedure_evidence_note_row_id = NULL
"""
    )


def _tgdc_sistrunk_audit(con) -> tuple[int, int] | None:
    """Return (cohort_n, sistrunk_true_n) or None if cohort/CPM unavailable."""
    needed = "sistrunk_procedure"
    if not _col_exists(con, needed):
        return None
    try:
        row = con.execute(
            f"""
SELECT
  COUNT(*) AS n_cohort,
  SUM(CASE WHEN p.sistrunk_procedure IS TRUE THEN 1 ELSE 0 END) AS n_sistrunk
FROM "{PUBLICATION_DB}".pub_workspace.cohort_tgdc_primary_v1 AS c
INNER JOIN "{PUBLICATION_DB}".main.canonical_patient_master AS p
  ON p.research_id = c.research_id
"""
        ).fetchone()
    except Exception:
        return None
    if row is None:
        return None
    n_cohort, n_sis = row[0], row[1]
    return int(n_cohort), int(n_sis or 0)


def main() -> int:
    ap = argparse.ArgumentParser(description="mig_322: Sistrunk procedure from op notes")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if args.apply == args.dry_run:
        print("Specify exactly one of --apply | --dry-run", file=sys.stderr)
        return 2

    from _md_connect import connect_locked  # noqa: E402

    from pipelines.extraction.sistrunk_parser import parse_sistrunk_in_note, pick_best_per_patient  # noqa: E402

    con = connect_locked()

    log_lines: list[str] = []

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    do_writes = bool(args.apply)

    # Phase A — column guards
    if do_writes:
        _ensure_cpm_columns(con, log)
    else:
        missing = [n for n, _ in NEW_CPM_COLS if not _col_exists(con, n)]
        log(f"DRY-RUN: CPM columns missing (would ADD): {missing or 'none'}")

    # Phase B — scan operative notes (chunked; keep only hits in memory)
    # Note: some MotherDuck builds expose clinical_notes_long as a view whose
    # note_row_id cannot be projected alongside note_text — scan text only and
    # stamp a deterministic surrogate for audit linkage (no PHI in the digest).
    sql = f"""
SELECT
  CAST(research_id AS VARCHAR) AS research_id,
  CAST(note_text AS VARCHAR) AS note_text
FROM "{PUBLICATION_DB}".main.clinical_notes_long
WHERE note_type = 'op_note'
  AND note_text IS NOT NULL
  AND LENGTH(TRIM(CAST(note_text AS VARCHAR))) > 0
"""
    cur = con.execute(sql)
    chunk_size = 4000
    all_hits: list = []
    n_rows_scanned = 0
    while True:
        batch = cur.fetchmany(chunk_size)
        if not batch:
            break
        n_rows_scanned += len(batch)
        for row in batch:
            rid, ntext = row[0], row[1]
            nid = _note_surrogate_key(str(rid), str(ntext) if ntext is not None else "")
            hit = parse_sistrunk_in_note(
                str(ntext) if ntext is not None else "",
                research_id=str(rid).strip(),
                note_row_id=nid,
            )
            if hit is not None:
                all_hits.append(hit)

    log(f"Scanned op_note rows: {n_rows_scanned}")
    log(f"Raw parser hits (all notes): {len(all_hits)}")

    best = pick_best_per_patient(all_hits)
    log(f"Patients with ≥1 Sistrunk hit: {len(best)}")

    audit = _tgdc_sistrunk_audit(con)
    if audit:
        n_c, n_s = audit
        log(f"TGDC cohort rows (join CPM): {n_c}; sistrunk_procedure=TRUE: {n_s}")

    if not do_writes:
        log("DRY-RUN complete (no writes).")
        con.close()
        return 0

    # Phase C — materialize extract table
    con.execute(
        f'DROP TABLE IF EXISTS "{PUBLICATION_DB}".main.extracted_sistrunk_procedure_opnote_v1'
    )
    con.execute(
        f"""
CREATE TABLE "{PUBLICATION_DB}".main.extracted_sistrunk_procedure_opnote_v1 (
  research_id VARCHAR NOT NULL,
  note_row_id VARCHAR,
  parser_rule_id VARCHAR NOT NULL,
  match_kind VARCHAR NOT NULL,
  match_offset BIGINT NOT NULL,
  evidence_summary VARCHAR NOT NULL,
  built_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""
    )
    if all_hits:
        con.executemany(
            f"""
INSERT INTO "{PUBLICATION_DB}".main.extracted_sistrunk_procedure_opnote_v1
  (research_id, note_row_id, parser_rule_id, match_kind, match_offset, evidence_summary)
VALUES (?, ?, ?, ?, ?, ?)
""",
            [
                (
                    h.research_id,
                    h.note_row_id,
                    h.rule_id,
                    h.sistrunk_match_kind,
                    h.match_offset,
                    h.sistrunk_text_evidence,
                )
                for h in all_hits
            ],
        )

    # Phase D — update CPM
    _reset_cpm_sistrunk(con)

    upsert_stmt = f"""
UPDATE "{PUBLICATION_DB}".main.canonical_patient_master AS cpm
SET
  sistrunk_procedure = TRUE,
  sistrunk_procedure_evidence_summary = ?,
  sistrunk_procedure_match_kind = ?,
  sistrunk_procedure_match_offset = ?,
  sistrunk_procedure_parser_rule_id = ?,
  sistrunk_procedure_evidence_note_row_id = ?
WHERE cpm.research_id = ?
"""
    params = [
        (
            h.sistrunk_text_evidence,
            h.sistrunk_match_kind,
            h.match_offset,
            h.rule_id,
            h.note_row_id,
            h.research_id,
        )
        for h in best.values()
    ]
    if params:
        con.executemany(upsert_stmt, params)

    n_true = con.execute(
        f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".main.canonical_patient_master '
        f"WHERE sistrunk_procedure IS TRUE"
    ).fetchone()[0]
    log(f"CPM sistrunk_procedure=TRUE count: {n_true}")

    audit2 = _tgdc_sistrunk_audit(con)
    if audit2:
        n_c, n_s = audit2
        log(f"POST TGDC cohort n={n_c}; sistrunk TRUE within cohort: {n_s}")
        if n_s != 161:
            log(
                f"WARNING: expected 161 TGDC Sistrunk positives for VC-TGDC-009 parity; got {n_s}"
            )

    # Phase E — signoff
    summary = (
        "mig_322: Operative-note Sistrunk keyword parser → "
        f"canonical_patient_master.sistrunk_procedure. "
        f"Op notes scanned={n_rows_scanned}, note-level hits={len(all_hits)}, "
        f"patients_positive={len(best)}, cpm_true={n_true}. "
        f"Parser: pipelines/extraction/sistrunk_parser.py. "
        f"Closes THY-4 / automates VC-TGDC-009 Sistrunk arm."
    )
    try:
        con.execute(
            f"""
INSERT INTO "{PUBLICATION_DB}".main.signoff_migration
  (mig_id, signed_off_at, by_actor, summary)
VALUES
  ('mig_322', CURRENT_TIMESTAMP, 'cursor_composer_mig322', ?)
""",
            [summary],
        )
        log("Inserted signoff_migration row mig_322")
    except Exception as e:
        log(f"WARN: signoff_migration insert failed: {e}")

    try:
        con.execute(
            f"""
INSERT INTO "{PUBLICATION_DB}".manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_322_sistrunk_procedure_20260506',
   CURRENT_TIMESTAMP,
   CURRENT_TIMESTAMP,
   'add_cpm_columns_extract_table_reset_cpm_update_cpm_signoff',
   'THY-4', 'VC-TGDC-009_automation', 'tgdc_sistrunk_text_arm', 'none')
"""
        )
        log("Inserted cpm_reconciliation_provenance_v1 row")
    except Exception as e:
        log(f"WARN: provenance insert failed: {e}")

    for cn, _ in NEW_CPM_COLS:
        try:
            con.execute(
                f'COMMENT ON COLUMN "{PUBLICATION_DB}".main.canonical_patient_master.{cn} '
                f"IS 'mig_322 THY-4: operative-note Sistrunk parser; see extracted_sistrunk_procedure_opnote_v1.'"
            )
        except Exception:
            pass

    out = REPO_ROOT / "scripts" / "output" / "mig_322_apply_log.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out.write_text(f"{stamp}\n" + "\n".join(log_lines) + "\n", encoding="utf-8")

    con.close()
    log(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
