"""
Builds a review CSV with all 196 positive airway-invasion findings (after
mig_80 filtered to OPNOTE + synoptic_pathology only). Each row shows the
clinical findings + evidence_quote + reasoning so Logan can confirm or
correct each LLM-extracted positive.
"""
from __future__ import annotations
import csv
from pathlib import Path
import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT = REPO_ROOT / "verification_csvs" / "canonical_airway_invasion_events_v1" / "positive_findings_review.csv"
DB = "thyroid_canonical_publication_v1_0"

PREAMBLE = """\
# Airway invasion positive-findings review -- canonical_airway_invasion_events_v1
# Generated 2026-04-28 (post mig_80; OPNOTE + synoptic_pathology only)
#
# 196 rows where at least one clinical finding is positive. Each row carries
# the LLM's evidence_quote and reasoning so you can confirm/correct.
#
# For each clinical finding column, valid values are:
#   tracheal_invasion:   absent | shaved | present | unknown
#   laryngeal_invasion:  absent | present | unknown
#   cricoid_invasion:    absent | present | unknown
#   rln_invasion:        absent | present | unknown
#   rln_paralysis_preop: absent | present | unknown
#   esophageal_invasion: absent | shaved | present | unknown
#   t4a_implication:     pT4a | not_pT4a | unable_to_determine
#
# Decision values per column ('your_*' fields):
#   ACCEPT     -- keep current value (default if blank)
#   <value>    -- override with this value (must be from valid list above)
#
# Or for the row-level decision:
#   DELETE     -- remove this row (false positive, not a real airway invasion event)
#   ACCEPT_ALL -- accept all current values for the row
"""


def main() -> None:
    con = duckdb.connect("md:")
    con.execute(f"USE {DB}")
    rows = con.execute("""
        SELECT
          airway_event_id, research_id, note_type, note_index,
          tracheal_invasion, tracheal_invasion_depth,
          laryngeal_invasion, cricoid_invasion,
          rln_invasion, rln_paralysis_preop,
          esophageal_invasion, t4a_implication,
          confidence, evidence_quote, reasoning
        FROM main.canonical_airway_invasion_events_v1
        WHERE tracheal_invasion IN ('present','shaved')
           OR laryngeal_invasion = 'present'
           OR cricoid_invasion = 'present'
           OR rln_invasion = 'present'
           OR rln_paralysis_preop = 'present'
           OR esophageal_invasion IN ('present','shaved')
           OR t4a_implication = 'pT4a'
        ORDER BY t4a_implication = 'pT4a' DESC, research_id, note_type, note_index
    """).fetchall()
    print(f"positive rows: {len(rows)}")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="") as f:
        f.write(PREAMBLE)
        w = csv.writer(f)
        w.writerow([
            "research_id", "note_type", "note_index", "airway_event_id",
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
             conf, evidence, reasoning) = r
            w.writerow([
                rid, nt, nidx, eid,
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
