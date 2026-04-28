"""
CSV for the 17 rows (13 unclear + 4 NULL) where fna_site couldn't be derived.
Logan picks fna_site value, NULL, or DELETE (for the truly-empty phantom rows).
"""
from __future__ import annotations
import csv
from pathlib import Path
import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT = REPO_ROOT / "verification_csvs" / "canonical_fna_events_v1" / "fna_site_unclear_review.csv"
DB = "thyroid_canonical_publication_v1_0"

VOCAB = (
    "thyroid_left_lobe / thyroid_right_lobe / thyroid_isthmus / thyroid_bilateral / "
    "thyroid_left_lobe_isthmus / thyroid_right_lobe_isthmus / thyroid_unspecified / "
    "lymph_node_<left|right|unspecified|bilateral>_<neck|level_1..7|paratracheal|"
    "supraclavicular|submandibular|mediastinal|central> / "
    "parathyroid_<left|right|unspecified> / "
    "other_neck_cyst / thyroglossal_duct_cyst / NULL / DELETE"
)

PREAMBLE = f"""\
# fna_site unclear review -- canonical_fna_events_v1
# Generated 2026-04-27 after mig_75
# 17 rows (13 unclear + 4 truly-empty) where fna_site couldn't be derived.
# Logan picks fna_site value, NULL, or DELETE (for phantom rows).
#
# Valid fna_site vocabulary (extend as needed):
#   {VOCAB}
"""


def main() -> None:
    con = duckdb.connect("md:")
    con.execute(f"USE {DB}")
    rows = con.execute("""
        SELECT research_id, fna_index, fna_event_id, fna_site,
               laterality, specimen_location, fna_pathology_report,
               bethesda_calculated_num
        FROM main.canonical_fna_events_v1
        WHERE fna_site IN ('unclear') OR fna_site IS NULL
        ORDER BY fna_site NULLS FIRST, research_id, fna_index
    """).fetchall()
    print(f"rows: {len(rows)}")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="") as f:
        f.write(PREAMBLE)
        w = csv.writer(f)
        w.writerow(["research_id", "fna_index", "fna_event_id",
                    "current_fna_site", "current_laterality", "bethesda",
                    "specimen_location", "fna_pathology_report_snippet",
                    "your_fna_site_decision", "your_note"])
        for r in rows:
            (rid, idx, eid, site, lat, spec, path, beth) = r
            w.writerow([
                rid, idx, eid, site or "<NULL>", lat or "",
                beth if beth is not None else "",
                (spec or "").replace("\r", " ").replace("\n", "\\n"),
                (path or "")[:300].replace("\r", " ").replace("\n", "\\n"),
                "", "",
            ])
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
