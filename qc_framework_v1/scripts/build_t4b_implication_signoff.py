"""
qc_framework_v1/scripts/build_t4b_implication_signoff.py
========================================================

ONE CSV / ONE COLUMN to review for canonical_t4b_invasion_events_v1.

Column under review:  t4b_implication
Rule (findings-vs-staging, feedback_findings_vs_staging.md):
    t4b_implication = 'pT4b' iff at least ONE of
      {prevertebral_fascia_invasion, carotid_encasement,
       mediastinal_vessel_invasion} = 'present'.
    Otherwise t4b_implication should be 'unable_to_determine'
    (or 'not_pT4b' if the LLM saw negative findings).

Subset shown: 47 rows
    - 18 rows with t4b_implication='pT4b'
    - the 'absent'-edge rows (some anatomic finding = 'absent',
      letting you sanity-check that t4b_implication wasn't wrongly
      bumped to pT4b on negative findings)

For each row you decide ONE thing in column `your_decision`:
    ACCEPT   -- (default; blank == ACCEPT) the t4b_implication value
                is consistent with the anatomic findings + evidence_quote
    RECLASS  -- override; specify replacement value in your_note
                (valid: pT4b | not_pT4b | unable_to_determine)

Nothing else to fill in. evidence_quote + reasoning + the 3 anatomic
columns are the basis for your decision.

Output:
    verification_csvs/canonical_t4b_invasion_events_v1/
      t4b_implication_signoff__mig_91.csv
"""
from __future__ import annotations

import csv
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT = (
    REPO_ROOT
    / "verification_csvs"
    / "canonical_t4b_invasion_events_v1"
    / "t4b_implication_signoff__mig_91.csv"
)
DB = "thyroid_canonical_publication_v1_0"

PREAMBLE = """\
# t4b_implication SIGN-OFF -- canonical_t4b_invasion_events_v1
# ONE column to review (t4b_implication). Rule:
#   t4b_implication = 'pT4b' iff at least ONE anatomic finding =
#   'present' (prevertebral_fascia / carotid / mediastinal).
#
# Set your_decision per row:
#   ACCEPT   -- blank or 'ACCEPT'; current value is correct
#   RECLASS  -- override; put the replacement value
#              (pT4b | not_pT4b | unable_to_determine) in your_note
#
# Sort: positives first, then absent-edge rows.
"""


def main() -> None:
    con = duckdb.connect("md:")
    con.execute(f'USE "{DB}"')
    rows = con.execute(
        """
        SELECT
          research_id, note_type, note_index,
          prevertebral_fascia_invasion AS pf,
          carotid_encasement           AS car,
          mediastinal_vessel_invasion  AS med,
          t4b_implication              AS t4b_implication,
          evidence_quote, reasoning,
          CASE
            WHEN pf='present' OR car='present' OR med='present' THEN 1
            ELSE 0 END AS any_anatomic_present
        FROM main.canonical_t4b_invasion_events_v1
        WHERE prevertebral_fascia_invasion IN ('present','absent')
           OR carotid_encasement           IN ('present','absent')
           OR mediastinal_vessel_invasion  IN ('present','absent')
           OR t4b_implication              = 'pT4b'
        ORDER BY any_anatomic_present DESC, t4b_implication, research_id
        """
    ).fetchall()
    print(f"rows: {len(rows)}")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="") as f:
        f.write(PREAMBLE)
        w = csv.writer(f)
        w.writerow([
            "research_id", "note_type", "note_index",
            "prevertebral_fascia_invasion", "carotid_encasement",
            "mediastinal_vessel_invasion", "t4b_implication",
            "evidence_quote", "reasoning", "any_anatomic_present",
            "your_decision", "your_note",
        ])
        for r in rows:
            w.writerow(list(r) + ["", ""])
    print(f"-> wrote {OUT}")


if __name__ == "__main__":
    main()
