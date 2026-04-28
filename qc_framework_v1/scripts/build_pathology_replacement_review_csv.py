"""
qc_framework_v1/scripts/build_pathology_replacement_review_csv.py
=================================================================

Builds a review CSV for Logan covering the divergent rows where DB
pathology_diagnosis or pathology_extended doesn't match the source
workbook (FNA Bethesda > History / Path extended cells).

For each divergent row, the CSV shows:
  - The DB's current content (full text)
  - The source workbook's content (full text)
  - A "what to look for" hint per row

Logan scans the CSV. If the source content looks reasonable, he gives
the green light and Claude bulk-replaces in mig_73.

Output:
  verification_csvs/canonical_fna_events_v1/
    pathology_replacement_review.csv
"""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_CSV = (
    REPO_ROOT / "verification_csvs" / "canonical_fna_events_v1"
    / "pathology_replacement_review.csv"
)
DEST_DB = "thyroid_canonical_publication_v1_0"

PREAMBLE = """\
# FNA pathology replacement review -- canonical_fna_events_v1
# Generated 2026-04-27 by qc_framework_v1/scripts/build_pathology_replacement_review_csv.py
#
# 3,320 rows in canonical_fna_events_v1 have DB pathology_diagnosis or
# pathology_extended that diverges from the source workbook (FNAs 12_5_2025.xlsx
# > FNA Bethesda > History cell / Path extended cell). The divergent content
# does NOT come from note_entities_llm_pathology (that's operative notes) or
# clinical_notes_long (no FNA cytopath reports). Provenance unknown -- likely
# older workbook version or deprecated pipeline.
#
# What to look for as you scan:
#   - Does the source content (col current_source_*) look like a legitimate
#     FNA cytopath report fragment for that patient?
#   - If yes, accept the row-level replacement (no action needed).
#   - If no, mark `your_decision` = REJECT and add a note. Those rows will be
#     left as-is in mig_73 and tracked as carry-forward.
#
# Also flag any case where the source content is GROSSLY shorter than the
# DB content -- that means the workbook lost detail that the DB still has.
# In that case mark your_decision = KEEP_DB and add a note.
#
# Decision values (default = ACCEPT for all rows; only mark exceptions):
#   ACCEPT   -- replace DB content with source workbook content (default if blank)
#   REJECT   -- leave DB as-is; do not replace
#   KEEP_DB  -- DB has detail source doesn't; keep DB content
"""


def main() -> None:
    print(f"[review_csv] connecting MD ({DEST_DB})")
    con = duckdb.connect("md:")
    con.execute(f"USE {DEST_DB}")

    # Pull divergent rows for both columns
    print("[review_csv] pulling divergent rows...")
    rows = con.execute("""
        WITH joined AS (
          SELECT
            db.research_id, db.fna_index, db.fna_event_id,
            db.fna_date_raw,
            db.bethesda_calculated_num,
            db.pathology_diagnosis AS db_diag,
            db.pathology_extended  AS db_ext,
            src.history_raw        AS src_history,
            src.path_raw           AS src_path
          FROM main.canonical_fna_events_v1 db
          LEFT JOIN manuscript_workspace.fna_source_long_v1_step_b src
            ON db.research_id = src.research_id AND db.fna_index = src.fna_index
        )
        SELECT
          research_id, fna_index, fna_event_id,
          fna_date_raw, bethesda_calculated_num,
          db_diag, src_history, db_ext, src_path,
          (COALESCE(TRIM(db_diag), '') <> COALESCE(TRIM(src_history), '')
            AND NOT (db_diag IS NULL AND src_history IS NULL)) AS diag_differs,
          (COALESCE(TRIM(db_ext), '') <> COALESCE(TRIM(src_path), '')
            AND NOT (db_ext IS NULL AND src_path IS NULL))     AS ext_differs
        FROM joined
        WHERE
          (COALESCE(TRIM(db_diag), '') <> COALESCE(TRIM(src_history), '')
            AND NOT (db_diag IS NULL AND src_history IS NULL))
          OR
          (COALESCE(TRIM(db_ext), '') <> COALESCE(TRIM(src_path), '')
            AND NOT (db_ext IS NULL AND src_path IS NULL))
        ORDER BY research_id, fna_index
    """).fetchall()

    print(f"[review_csv] divergent rows: {len(rows)}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="") as f:
        f.write(PREAMBLE)
        w = csv.writer(f)
        w.writerow([
            "research_id", "fna_index", "fna_date_raw", "bethesda",
            "diag_differs", "ext_differs",
            "current_db_pathology_diagnosis",
            "current_source_history_cell",
            "current_db_pathology_extended",
            "current_source_path_extended_cell",
            "your_decision_diagnosis",
            "your_decision_extended",
            "your_note",
        ])
        for r in rows:
            (rid, idx, eid, date_raw, beth,
             db_diag, src_hist, db_ext, src_path,
             diag_diff, ext_diff) = r
            w.writerow([
                rid, idx, date_raw or "", beth if beth is not None else "",
                "Y" if diag_diff else "",
                "Y" if ext_diff else "",
                (db_diag or "").replace("\r", " ").replace("\n", "\\n"),
                (src_hist or "").replace("\r", " ").replace("\n", "\\n"),
                (db_ext or "").replace("\r", " ").replace("\n", "\\n"),
                (src_path or "").replace("\r", " ").replace("\n", "\\n"),
                "",  # your_decision_diagnosis
                "",  # your_decision_extended
                "",  # your_note
            ])

    print(f"[review_csv] wrote {OUT_CSV}")


if __name__ == "__main__":
    main()
