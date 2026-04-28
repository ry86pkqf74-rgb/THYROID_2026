"""
Builds the second airway-invasion review CSV: only the 138 rows still
labeled t4a_implication='pT4a' after mig_81. These are the actual pT4a
candidates that need Logan's individual evidence-quote review.

Sort order (most-concerning first):
  1. Number of full-thickness 'present' findings DESC
     (laryngeal/cricoid/rln/tracheal=present/esophageal=present)
  2. confidence DESC
  3. research_id, note_type, note_index
"""
from __future__ import annotations
import csv
from pathlib import Path
import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT = REPO_ROOT / "verification_csvs" / "canonical_airway_invasion_events_v1" / "pT4a_candidates_review.csv"
DB = "thyroid_canonical_publication_v1_0"

PREAMBLE = """\
# Airway invasion pT4a candidates review (post-mig_81)
# Generated 2026-04-28
#
# 138 rows currently labeled t4a_implication='pT4a'. Sorted with the most
# concerning rows (multiple full-thickness 'present' findings, high
# confidence) at the top.
#
# n_full_thickness column counts how many of these are 'present':
#   tracheal_invasion='present', laryngeal_invasion='present',
#   cricoid_invasion='present', rln_invasion='present',
#   esophageal_invasion='present'
# (rln_paralysis_preop and 'shaved' do NOT count as full-thickness.)
#
# For each row, valid override values are:
#   tracheal_invasion:   absent | shaved | present | unknown
#   laryngeal_invasion:  absent | present | unknown
#   cricoid_invasion:    absent | present | unknown
#   rln_invasion:        absent | present | unknown
#   rln_paralysis_preop: absent | present | unknown
#   esophageal_invasion: absent | shaved | present | unknown
#   t4a_implication:     pT4a | not_pT4a | unable_to_determine
#
# Decision values:
#   your_row_decision:
#     blank      -- ACCEPT_ALL (default; row stays as pT4a)
#     ACCEPT_ALL -- explicit accept
#     DELETE     -- drop row (false positive / out of scope)
#     NOT_PT4A   -- keep row but flip t4a to not_pT4a
#   your_<finding>:
#     blank      -- keep current value
#     <value>    -- override with this value (must be from valid list)
#   your_note     -- free text
"""


def main() -> None:
    con = duckdb.connect("md:")
    con.execute(f"USE {DB}")
    rows = con.execute("""
        WITH ranked AS (
          SELECT
            airway_event_id, research_id, note_type, note_index,
            tracheal_invasion, tracheal_invasion_depth,
            laryngeal_invasion, cricoid_invasion,
            rln_invasion, rln_paralysis_preop,
            esophageal_invasion, t4a_implication,
            confidence, evidence_quote, reasoning,
            ((CASE WHEN tracheal_invasion='present'   THEN 1 ELSE 0 END)
            +(CASE WHEN laryngeal_invasion='present'  THEN 1 ELSE 0 END)
            +(CASE WHEN cricoid_invasion='present'    THEN 1 ELSE 0 END)
            +(CASE WHEN rln_invasion='present'        THEN 1 ELSE 0 END)
            +(CASE WHEN esophageal_invasion='present' THEN 1 ELSE 0 END)
            ) AS n_full_thickness
          FROM main.canonical_airway_invasion_events_v1
          WHERE t4a_implication = 'pT4a'
        )
        SELECT * FROM ranked
        ORDER BY n_full_thickness DESC,
                 CASE confidence WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 ELSE 3 END,
                 research_id, note_type, note_index
    """).fetchall()
    print(f"pT4a candidate rows: {len(rows)}")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="") as f:
        f.write(PREAMBLE)
        w = csv.writer(f)
        w.writerow([
            "research_id", "note_type", "note_index", "airway_event_id",
            "n_full_thickness",
            "tracheal_invasion", "tracheal_invasion_depth",
            "laryngeal_invasion", "cricoid_invasion",
            "rln_invasion", "rln_paralysis_preop",
            "esophageal_invasion", "t4a_implication", "confidence",
            "evidence_quote", "reasoning",
            "your_row_decision",
            "your_tracheal", "your_laryngeal", "your_cricoid",
            "your_rln", "your_rln_par", "your_esophageal", "your_t4a",
            "your_note",
        ])
        for r in rows:
            (eid, rid, nt, nidx,
             trach, trach_depth, laryn, cric, rln, rln_par, esoph, t4a,
             conf, evidence, reasoning, n_ft) = r
            w.writerow([
                rid, nt, nidx, eid,
                n_ft,
                trach or "", trach_depth or "",
                laryn or "", cric or "", rln or "", rln_par or "",
                esoph or "", t4a or "", conf or "",
                (evidence or "").replace("\r", " ").replace("\n", "\\n"),
                (reasoning or "").replace("\r", " ").replace("\n", "\\n"),
                "", "", "", "", "", "", "", "", "",
            ])
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
