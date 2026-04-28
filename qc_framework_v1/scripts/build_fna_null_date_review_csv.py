"""
qc_framework_v1/scripts/build_fna_null_date_review_csv.py
=========================================================

Builds two outputs covering the 62 rows where fna_date_raw is NULL:

  (1) verification_csvs/canonical_fna_events_v1/
        fna_null_date_review_round3.csv
      The 41 rows that DO have FNA content (specimen, bethesda, path text)
      but no date. Logan supplies a date or DELETE per row.

  (2) Inline list of the 21 phantom rows (no FNA content at all). These
      will be DELETEd as part of mig_69 alongside the round-3 decisions.

For the 41-row CSV the script extracts useful context for each row so
Logan can pick the correct date without leaving the spreadsheet:
  bethesda_calculated_num, bethesda_2023_num, laterality, subtype,
  specimen_site_raw (or specimen_location), and short snippets of
  bethesda_original_text + pathology_extended + pathology_diagnosis.
"""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_REVIEW_CSV = (
    REPO_ROOT / "verification_csvs" / "canonical_fna_events_v1"
    / "fna_null_date_review_round3.csv"
)
OUT_PHANTOMS_TXT = (
    REPO_ROOT / "verification_csvs" / "canonical_fna_events_v1"
    / "fna_null_date_phantoms_round3.txt"
)
DEST_DB = "thyroid_canonical_publication_v1_0"

PREAMBLE = """\
# FNA cleanup round 3 -- canonical_fna_events_v1.fna_date_raw NULL rows
# Generated 2026-04-27 by qc_framework_v1/scripts/build_fna_null_date_review_csv.py
#
# These 41 rows have FNA content (specimen / bethesda / pathology) but no
# date. Logan supplies a date in MM/DD/YYYY format, or writes DELETE if the
# row should be removed.
#
# 21 OTHER NULL-date rows had no FNA content at all -- those will be DELETEd
# unconditionally in the same migration (see fna_null_date_phantoms_round3.txt
# for the list).
#
# Decision values:
#   <MM/DD/YYYY> -- set fna_date_raw to this value
#   NULL         -- leave fna_date_raw as NULL (date genuinely unknown but
#                   the FNA event itself is real and should stay)
#   DELETE       -- remove the row entirely
"""


def truncate(s: str | None, n: int = 200) -> str:
    if s is None:
        return ""
    s = str(s).replace("\r", " ").replace("\n", " | ").strip()
    if len(s) > n:
        s = s[:n] + "..."
    return s


def main() -> None:
    print(f"[round3] connecting MD ({DEST_DB})")
    con = duckdb.connect("md:")
    con.execute(f"USE {DEST_DB}")

    # Pull all 62 NULL-date rows with content classification
    print("[round3] pulling NULL-date rows...")
    rows = con.execute("""
        WITH null_rows AS (
          SELECT *
          FROM main.canonical_fna_events_v1
          WHERE fna_date_raw IS NULL
        )
        SELECT
          research_id, fna_event_id, fna_index, fna_seq_n,
          bethesda_calculated_num, bethesda_2023_num, bethesda_2023_name,
          laterality, subtype,
          specimen_location, specimen_site_raw,
          bethesda_original_text, pathology_extended, pathology_diagnosis,
          CASE
            WHEN NULLIF(TRIM(COALESCE(specimen_location, '')), '')      IS NULL
             AND NULLIF(TRIM(COALESCE(specimen_site_raw, '')), '')      IS NULL
             AND NULLIF(TRIM(COALESCE(pathology_extended, '')), '')     IS NULL
             AND NULLIF(TRIM(COALESCE(pathology_diagnosis, '')), '')    IS NULL
             AND NULLIF(TRIM(COALESCE(bethesda_original_text, '')), '') IS NULL
             AND bethesda_calculated_num IS NULL
             AND bethesda_2023_num IS NULL
             AND laterality IS NULL
             AND subtype IS NULL
            THEN 'PHANTOM' ELSE 'REAL_FNA'
          END AS classification
        FROM null_rows
        ORDER BY research_id, fna_index
    """).fetchall()

    cols = [
        "research_id", "fna_event_id", "fna_index", "fna_seq_n",
        "bethesda_calculated_num", "bethesda_2023_num", "bethesda_2023_name",
        "laterality", "subtype",
        "specimen_location", "specimen_site_raw",
        "bethesda_original_text", "pathology_extended", "pathology_diagnosis",
        "classification",
    ]
    real_fnas = [dict(zip(cols, r)) for r in rows if r[14] == "REAL_FNA"]
    phantoms  = [dict(zip(cols, r)) for r in rows if r[14] == "PHANTOM"]
    print(f"[round3] real-FNA rows for Logan review: {len(real_fnas)}")
    print(f"[round3] phantom rows to DELETE:         {len(phantoms)}")

    # ---- Write 41-row review CSV ----
    OUT_REVIEW_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_REVIEW_CSV, "w", newline="") as f:
        f.write(PREAMBLE)
        w = csv.writer(f)
        w.writerow([
            "research_id", "fna_event_id", "fna_index", "fna_seq_n",
            "bethesda", "bethesda_2023", "laterality", "subtype",
            "specimen", "bethesda_text_snippet",
            "path_extended_snippet", "path_diagnosis_snippet",
            "your_decision", "your_note",
        ])
        for r in real_fnas:
            beth_str = ""
            if r["bethesda_calculated_num"] is not None:
                beth_str = str(r["bethesda_calculated_num"])
            w.writerow([
                r["research_id"], r["fna_event_id"], r["fna_index"], r["fna_seq_n"],
                beth_str,
                r["bethesda_2023_name"] or "",
                r["laterality"] or "",
                r["subtype"] or "",
                truncate(r["specimen_site_raw"] or r["specimen_location"], 100),
                truncate(r["bethesda_original_text"], 200),
                truncate(r["pathology_extended"], 200),
                truncate(r["pathology_diagnosis"], 200),
                "",  # your_decision
                "",  # your_note
            ])
    print(f"[round3] wrote {OUT_REVIEW_CSV}")

    # ---- Write phantoms list as plain text (audit reference) ----
    with open(OUT_PHANTOMS_TXT, "w") as f:
        f.write("# 21 phantom rows (NULL fna_date_raw + no FNA content) -- mig_69 will DELETE\n")
        f.write("# columns: research_id, fna_index, fna_event_id\n")
        for r in phantoms:
            f.write(f"{r['research_id']}\t{r['fna_index']}\t{r['fna_event_id']}\n")
    print(f"[round3] wrote {OUT_PHANTOMS_TXT}")


if __name__ == "__main__":
    main()
