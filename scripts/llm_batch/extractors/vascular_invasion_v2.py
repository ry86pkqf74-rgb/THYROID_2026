"""Vascular invasion v2 candidate notes — narrow re-prompt.

Cohort: all path_malignant events. Sources: path_synoptics (synoptic pathology
is where the LVI/VI/PNI language lives).
"""
from __future__ import annotations

import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from _base import connect, fetch_notes, write_jsonl

OUT_PATH = pathlib.Path(__file__).parent.parent / "output" / "vascular_invasion_v2_notes.jsonl"

COHORT_CTE = """
SELECT DISTINCT research_id FROM main.canonical_path_malignant_events_v1
"""

KEYWORDS = [
    "vascular invasion", "lymphovascular invasion", " lvi ", " lvi,", " lvi.",
    "angioinvasion", "angio-invasion", "angio invasion",
    "vessel invasion", "vessels involved", "tumor thrombus",
    "intravascular", "perineural invasion", " pni ", " pni,", " pni.",
    "lymphatic invasion",
    "focally present", "extensive", "widely invasive", "minimally invasive",
    "encapsulated angioinvasive",
]


def main() -> None:
    con = connect()
    rows = fetch_notes(
        con, cohort_cte=COHORT_CTE, keywords=KEYWORDS,
        include_clinical=False, include_path=True,
        limit_per_patient=2,
    )
    write_jsonl(rows, OUT_PATH)


if __name__ == "__main__":
    main()
