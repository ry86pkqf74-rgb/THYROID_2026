"""ETE subgrade candidate notes — 167 PTC unspec_remaining patients.

Sources: clinical_notes_long (op/HP/DC) + path_synoptics free-text.
"""
from __future__ import annotations

import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from _base import connect, fetch_notes, write_jsonl

OUT_PATH = pathlib.Path(__file__).parent.parent / "output" / "ete_subgrade_notes.jsonl"

COHORT_CTE = """
SELECT DISTINCT research_id
FROM manuscript_workspace.ete_manuscript_analytic_v2
WHERE cohort_ptc AND analytic_eligible
  AND ete_grade_final = 'unspec_remaining'
"""

CLINICAL_NOTE_TYPES = ["OPNOTE", "HP", "DC_SUM", "ED_NOTE", "ENDOCRINE_FM", "OTHER_HISTORY", "OTHER_NOTES"]
KEYWORDS = [
    "extrathyroidal", "extra-thyroidal", " ete ", "perithyroidal",
    "strap muscle", "tracheal invasion", "extension into", "extends into",
    "pt3a", "pt3b", "pt4", "recurrent laryngeal", "substernal",
]


def main() -> None:
    con = connect()
    rows = fetch_notes(
        con, cohort_cte=COHORT_CTE, keywords=KEYWORDS,
        include_clinical=True, include_path=True,
        clinical_note_types=CLINICAL_NOTE_TYPES,
    )
    write_jsonl(rows, OUT_PATH)


if __name__ == "__main__":
    main()
