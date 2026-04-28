"""
qc_framework_v1/scripts/build_laterality_review_csv.py
======================================================

Builds a review CSV for Logan covering the 602 rows where the laterality
re-derivation from specimen_location either:
  (a) DISAGREES with the previously-stored laterality value (530 rows), OR
  (b) has a previously-stored value but the rule can't derive (72 rows;
      specimen_location lacks left/right/isthmus keywords).

Decision values per row:
  KEEP_CURRENT     -- keep current_laterality value
  USE_DERIVED      -- apply derived_laterality value
  <left|right|isthmus|NULL>  -- override with this value
"""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_CSV = (
    REPO_ROOT / "verification_csvs" / "canonical_fna_events_v1"
    / "laterality_review_round1.csv"
)
DEST_DB = "thyroid_canonical_publication_v1_0"

PREAMBLE = """\
# Laterality review -- canonical_fna_events_v1
# Generated 2026-04-27 by qc_framework_v1/scripts/build_laterality_review_csv.py
#
# 602 rows where the rule-based re-derivation from specimen_location either
# (a) disagrees with the current Logan-adjudicated value, or
# (b) the rule can't determine but current value is set.
#
# 2,600 rows where the rule could fill a previously-NULL laterality were
# already applied in mig_74 (mechanical, unambiguous).
#
# Decision values:
#   KEEP_CURRENT  -- keep current laterality (default if blank)
#   USE_DERIVED   -- replace with derived_laterality value
#   left          -- override with 'left'
#   right         -- override with 'right'
#   isthmus       -- override with 'isthmus'
#   NULL          -- set to NULL
"""


def main() -> None:
    print(f"[laterality_review] connecting MD ({DEST_DB})")
    con = duckdb.connect("md:")
    con.execute(f"USE {DEST_DB}")

    rows = con.execute("""
        WITH derived AS (
          SELECT
            db.research_id, db.fna_index, db.fna_event_id,
            db.laterality                    AS current_lat,
            db.specimen_location             AS spec,
            db.bethesda_calculated_num       AS beth,
            db.fna_pathology_report          AS path_report,
            CASE
              WHEN LOWER(COALESCE(db.specimen_location, '')) LIKE '%isthmus%' THEN 'isthmus'
              WHEN (LOWER(db.specimen_location) LIKE '%left%'
                 OR LOWER(db.specimen_location) LIKE '%ll fna%'
                 OR LOWER(db.specimen_location) LIKE '%ll nodule%'
                 OR LOWER(db.specimen_location) LIKE 'll %'
                 OR LOWER(db.specimen_location) LIKE '%-ll-%')
               AND (LOWER(db.specimen_location) LIKE '%right%'
                 OR LOWER(db.specimen_location) LIKE '%rl fna%'
                 OR LOWER(db.specimen_location) LIKE '%rl nodule%'
                 OR LOWER(db.specimen_location) LIKE 'rl %'
                 OR LOWER(db.specimen_location) LIKE '%-rl-%')
                THEN 'BOTH_LEFT_AND_RIGHT'
              WHEN LOWER(db.specimen_location) LIKE '%left%'
                OR LOWER(db.specimen_location) LIKE '%ll fna%'
                OR LOWER(db.specimen_location) LIKE '%ll nodule%'
                OR LOWER(db.specimen_location) LIKE 'll %'
                OR LOWER(db.specimen_location) LIKE '%-ll-%'
                THEN 'left'
              WHEN LOWER(db.specimen_location) LIKE '%right%'
                OR LOWER(db.specimen_location) LIKE '%rl fna%'
                OR LOWER(db.specimen_location) LIKE '%rl nodule%'
                OR LOWER(db.specimen_location) LIKE 'rl %'
                OR LOWER(db.specimen_location) LIKE '%-rl-%'
                THEN 'right'
              ELSE 'NO_KEYWORD'
            END AS derived_lat
          FROM main.canonical_fna_events_v1 db
        )
        SELECT research_id, fna_index, fna_event_id,
               current_lat, derived_lat, beth,
               spec, path_report
        FROM derived
        WHERE current_lat IS NOT NULL
          AND derived_lat IN ('left','right','isthmus','BOTH_LEFT_AND_RIGHT','NO_KEYWORD')
          AND current_lat <> derived_lat
        ORDER BY derived_lat, research_id, fna_index
    """).fetchall()

    print(f"[laterality_review] rows: {len(rows)}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="") as f:
        f.write(PREAMBLE)
        w = csv.writer(f)
        w.writerow([
            "research_id", "fna_index", "fna_event_id",
            "current_laterality", "derived_laterality",
            "bethesda", "specimen_location_excerpt",
            "fna_pathology_report_snippet",
            "your_decision", "your_note",
        ])
        for r in rows:
            (rid, idx, eid, cur_lat, der_lat, beth, spec, path) = r
            w.writerow([
                rid, idx, eid,
                cur_lat or "", der_lat or "",
                beth if beth is not None else "",
                (spec or "")[:120].replace("\r", " ").replace("\n", "\\n"),
                (path or "")[:200].replace("\r", " ").replace("\n", "\\n"),
                "",  # your_decision
                "",  # your_note
            ])
    print(f"[laterality_review] wrote {OUT_CSV}")


if __name__ == "__main__":
    main()
