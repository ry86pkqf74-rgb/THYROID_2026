"""T4b invasion candidate notes — prevertebral / carotid / mediastinal vessels.

Cohort: path_malignant with gross_ete=1 OR reported T-stage T4* OR any patient
whose notes mention the T4b-specific keywords.

Sources: ALL — clinical notes + path synoptics + CT + MRI.
"""
from __future__ import annotations

import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from _base import connect, fetch_notes, write_jsonl

OUT_PATH = pathlib.Path(__file__).parent.parent / "output" / "t4b_invasion_notes.jsonl"

COHORT_CTE = """
SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
FROM main.canonical_path_malignant_events_v1
WHERE gross_ete = 1
   OR UPPER(t_stage_ajcc8) LIKE 'T4%'
   OR UPPER(t_stage_ajcc8) LIKE 'PT4%'
   OR UPPER(t_stage_ajcc7) LIKE 'T4%'
"""

CLINICAL_NOTE_TYPES = ["OPNOTE", "HP", "DC_SUM", "ED_NOTE", "OTHER_HISTORY", "OTHER_NOTES", "ENDOCRINE_FM"]
KEYWORDS = [
    "prevertebral", "longus colli", "paraspinal",
    "carotid", "carotid sheath",
    "mediastinal", "mediastinum", "great vessels",
    "innominate", "brachiocephalic", "subclavian",
    "superior vena cava", " svc ", "aortic arch",
    "encasement", "encases", "encased", "unresectable",
    "circumferential", "pt4b", " t4b",
]


def main() -> None:
    con = connect()
    rows = fetch_notes(
        con, cohort_cte=COHORT_CTE, keywords=KEYWORDS,
        include_clinical=True, include_path=True, include_ct=True, include_mri=True,
        clinical_note_types=CLINICAL_NOTE_TYPES,
        limit_per_patient=5,
    )
    write_jsonl(rows, OUT_PATH)


if __name__ == "__main__":
    main()
