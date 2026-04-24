"""Airway invasion v2 candidate notes — narrow re-prompt for T4a granularity.

Sources: path_synoptics (shave / tracheal cartilage / esoph), clinical OPNOTE
(shave excision / sleeve resection / RLN sacrificed), CT (tracheal narrowing /
airway compromise).
"""
from __future__ import annotations

import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from _base import connect, fetch_notes, write_jsonl

OUT_PATH = pathlib.Path(__file__).parent.parent / "output" / "airway_invasion_v2_notes.jsonl"

COHORT_CTE = """
SELECT DISTINCT research_id FROM main.canonical_path_malignant_events_v1
"""

CLINICAL_NOTE_TYPES = ["OPNOTE", "HP", "DC_SUM"]
KEYWORDS = [
    "trachea", "tracheal", "tracheal wall", "tracheal cartilage",
    "shaved off", "shave excision", "shave ",
    "larynx", "laryngeal", "cricoid", "thyroid cartilage",
    "recurrent laryngeal", " rln ", " rln,", " rln.",
    "vocal cord", "vocal cord paralysis",
    "window resection", "tracheal resection", "sleeve resection",
    "esophagus", "esophageal",
]


def main() -> None:
    con = connect()
    rows = fetch_notes(
        con, cohort_cte=COHORT_CTE, keywords=KEYWORDS,
        include_clinical=True, include_path=True, include_ct=True,
        clinical_note_types=CLINICAL_NOTE_TYPES,
        limit_per_patient=2,
    )
    write_jsonl(rows, OUT_PATH)


if __name__ == "__main__":
    main()
