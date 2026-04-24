"""Parathyroid detail candidate notes — TIGHTENED cohort.

Rather than pulling from every operative patient (10,871 pts, would yield ~11k
routine-postop-calcium-check notes), we gate on evidence that the note or
synoptic actually has parathyroid-relevant content:

Cohort = patients satisfying ANY of:
  (A) path_synoptics.parathyroid_gland_or_tissue_included_in_resected_specimen
      populated (not null/no/none/n/a) — actual gland in specimen
  (B) path_synoptics.parag_N_location populated — per-gland entries exist
  (C) path_synoptics.parathyroid_gland_findings populated
  (D) clinical note (OPNOTE/HP/DC_SUM/ENDOCRINE_FM) contains autotransplant /
      reimplant language
  (E) clinical note mentions hypocalcemia, hypoparathyroid, permanent-hypo
  (F) clinical note mentions parathyroid adenoma / hyperplasia /
      parathyroidectomy / parathyromatosis / incidental parathyroid

Yields ~5,386 patients (was 10,871).

Sources: clinical notes + path synoptics. No imaging — parathyroid detail
lives in the op note (autotransplant language) and path (gland IDs).
"""
from __future__ import annotations

import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from _base import connect, fetch_notes, write_jsonl

OUT_PATH = pathlib.Path(__file__).parent.parent / "output" / "parathyroid_detail_notes.jsonl"

COHORT_CTE = """
WITH path_signal AS (
  SELECT DISTINCT research_id FROM main.path_synoptics
  WHERE (parathyroid_gland_or_tissue_included_in_resected_specimen IS NOT NULL
         AND LOWER(parathyroid_gland_or_tissue_included_in_resected_specimen) NOT IN
             ('', 'no', 'none', 'not included', 'n/a'))
     OR COALESCE(parag_1_location, parag_2_location, parag_3_location,
                 parag_4_location, parag_5_location, parag_6_location) IS NOT NULL
     OR parathyroid_gland_findings IS NOT NULL
),
clinical_signal AS (
  SELECT DISTINCT research_id FROM main.clinical_notes_long
  WHERE note_type IN ('OPNOTE', 'HP', 'DC_SUM', 'ENDOCRINE_FM')
    AND (LOWER(note_text) LIKE '%autotransplant%'
         OR LOWER(note_text) LIKE '%auto transplant%'
         OR LOWER(note_text) LIKE '%reimplant%'
         OR LOWER(note_text) LIKE '%hypocalcemia%'
         OR LOWER(note_text) LIKE '%hypoparathyroid%'
         OR LOWER(note_text) LIKE '%parathyroid adenoma%'
         OR LOWER(note_text) LIKE '%parathyroid hyperplasia%'
         OR LOWER(note_text) LIKE '%parathyroidectomy%'
         OR LOWER(note_text) LIKE '%parathyromatosis%'
         OR LOWER(note_text) LIKE '%incidental parathyroid%')
)
SELECT research_id FROM path_signal
UNION
SELECT research_id FROM clinical_signal
"""

CLINICAL_NOTE_TYPES = ["OPNOTE", "HP", "DC_SUM", "ENDOCRINE_FM"]
KEYWORDS = [
    "parathyroid", "superior parathyroid", "inferior parathyroid",
    "parathyroid identified", "parathyroid preserved", "autotransplant",
    "reimplanted", "parathyroidectomy", "incidental parathyroid",
    "parathyroid adenoma", "parathyroid hyperplasia",
    "hyperparathyroidism", "hypoparathyroidism", "hypocalcemia",
    "intact pth", "parathyroid hormone", " pth ", " pth,", " pth.",
    "parathyromatosis",
]


def main() -> None:
    con = connect()
    rows = fetch_notes(
        con, cohort_cte=COHORT_CTE, keywords=KEYWORDS,
        include_clinical=True, include_path=True,
        clinical_note_types=CLINICAL_NOTE_TYPES,
        limit_per_patient=3,
    )
    write_jsonl(rows, OUT_PATH)


if __name__ == "__main__":
    main()
