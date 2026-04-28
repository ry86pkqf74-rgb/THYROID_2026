"""
qc_framework_v1/scripts/build_fna_date_raw_cleanup_csv.py
=========================================================

Builds the round-2 cleanup CSV for canonical_fna_events_v1.fna_date_raw,
covering the 53 rows that aren't cleanly date-parseable after the round-1
mechanical_source_compare verification.

Snapshot-based version (no live MD connection): the 53 problem rows were
captured 2026-04-27 18:30 UTC immediately after Step A cleanup (5 phantom
rows + 8 audit rows deleted). To regenerate from MD, see the
duckdb-md flavor of this script in the git history.

For each row Claude proposes a correction in MM/DD/YYYY format. Logan
fills `your_decision` with ACCEPT, an override, NULL, or KEEP. Claude
then bulk-applies via mig_67.

Output:
  verification_csvs/canonical_fna_events_v1/fna_date_raw__cleanup_round2.csv
"""

from __future__ import annotations

import csv
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_CSV = (
    REPO_ROOT
    / "verification_csvs"
    / "canonical_fna_events_v1"
    / "fna_date_raw__cleanup_round2.csv"
)

# Snapshot: 53 problem rows pulled 2026-04-27 post-Step-A.
# Tuple = (research_id, fna_event_id, fna_index, fna_seq_n, current_db_value,
#          bucket, proposed_correction, proposed_note)
ROWS: list[tuple[str, str, int, int, str, str, str | None, str]] = [
    # EXCEL_SERIAL
    ("6171",  "50a6419227cefc3b199a50d69ab526f5", 2, 2,
     "2", "EXCEL_SERIAL",
     None, "raw is just digit '2'; not a date; propose NULL"),

    # FREE_TEXT_LIKELY_PATH
    ("5594",  "da76e6487c96a0f0c102c154f84571bf", 2, 2,
     "B.  Thyroid isthmic nodule, fine needle aspiration (ThinPrep slide and aspirate smears): ",
     "FREE_TEXT_LIKELY_PATH",
     None, "pathology header text, no date present; propose NULL"),
    ("6103",  "5dc883506df78ed6df0a9335972f4538", 2, 2,
     '"A: Right Neck, Fine Needle Aspiration (10 Slides; OSC#CN17-916, DOS 5/22/17): ',
     "FREE_TEXT_LIKELY_PATH",
     "05/22/2017", "DOS 5/22/17 embedded in path text; extract"),
    ("7332",  "c17eca173948ba8b001eda813254365a", 2, 2,
     "A:: Thyroid, Right, Fine Needle Aspiration: ",
     "FREE_TEXT_LIKELY_PATH",
     None, "pathology header text, no date present; propose NULL"),
    ("8745",  "7490ae8a4e2a3f79602ae4dafb53803e", 2, 2,
     "3/1/2021\n2. LEFT THYROID NODULE (THINPREP CYTOLOGY, SMEARS, AND CELL BLOCK):",
     "FREE_TEXT_LIKELY_PATH",
     "03/01/2021", "date '3/1/2021' at start of path text; extract"),
    ("9003",  "e3d14eada70ed861f93a5673db6977f7", 1, 1,
     "LEFT THYROID NODULE (ASPIRATION CYTOLOGY):",
     "FREE_TEXT_LIKELY_PATH",
     None, "pathology header text, no date present; propose NULL"),
    ("9852",  "19775b0351d4039f76ba23864186baf0", 2, 2,
     "\n2. Thyroid, Isthmus, Fine  needle aspiration",
     "FREE_TEXT_LIKELY_PATH",
     None, "pathology header text, no date present; propose NULL"),

    # NULL_EQUIVALENT_TEXT (all -> NULL)
    ("1121",  "4b47aeb629bc6c77f400092a4c75f76d", 1, 1, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s' (not specified); propose NULL"),
    ("1422",  "ff6cd3c16e09f12f9a7e77c1141447cb", 1, 1, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s' (not specified); propose NULL"),
    ("1614",  "bca16d44246212a2e555f96c1ec6d54f", 1, 1, "Date unknown:\n", "NULL_EQUIVALENT_TEXT",
     None, "raw='Date unknown:'; propose NULL"),
    ("1614",  "d778e0017b99090dd0f542ba175a190f", 2, 2, "Date unknown:\n", "NULL_EQUIVALENT_TEXT",
     None, "raw='Date unknown:'; propose NULL"),
    ("1614",  "4d4deaabdc1b8a149d8bbc11ab7cd518", 3, 3, "Date unknown:\n", "NULL_EQUIVALENT_TEXT",
     None, "raw='Date unknown:'; propose NULL"),
    ("1959",  "d642eeeefaf4c150242cfb6d9f58a3ed", 1, 1, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s'; propose NULL"),
    ("2113",  "3dfda4128562f0ff191c78ecbc5e0db9", 1, 1, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s'; propose NULL"),
    ("2210",  "787a183dfc6e9aa03990df1fb48a9676", 1, 1, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s'; propose NULL"),
    ("2212",  "6489170b53c7bd82c45f09e74b508cfb", 1, 1, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s'; propose NULL"),
    ("2610",  "5f4db5cca7ce22750bd25fb762a11944", 1, 1, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s'; propose NULL"),
    ("2799",  "80ab082e3006138941c2d4c217baa41e", 1, 1, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s'; propose NULL"),
    ("3221",  "8df106e953d3a4c3efe864b59927b678", 1, 1, "not specified", "NULL_EQUIVALENT_TEXT",
     None, "raw='not specified'; propose NULL"),
    ("4064",  "8c98aee78db61571d8e09046a97680c4", 1, 1, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s'; propose NULL"),
    ("4064",  "031f3f727c3c831be86964bbecce0ab0", 2, 2, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s'; propose NULL"),
    ("4064",  "78996a94b092b495a9a5e4a8a1b6a7c1", 3, 3, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s'; propose NULL"),
    ("8803",  "fb034a6d0606c54fa2da1e001b1a4fd0", 1, 1, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s'; propose NULL"),
    ("9201",  "863f6bf2a45e37a5cb749b032483f088", 1, 1, "n/s", "NULL_EQUIVALENT_TEXT",
     None, "raw='n/s'; propose NULL"),

    # PUNCT_TYPO
    ("11032", "c052dafc62c879da64fb2c50936e54cd", 1, 1, "1/8.2024", "PUNCT_TYPO",
     "01/08/2024", "raw='1/8.2024' (period instead of slash)"),

    # YEAR_TYPO_5+_DIGITS
    ("10377", "8ccbffab44ec95760cee6c77011dcb95", 1, 1, "7/5/20222", "YEAR_TYPO_5+_DIGITS",
     "07/05/2022", "raw='7/5/20222' (extra '2' in year)"),
    ("10377", "058badca8bb85928698dbb72bef8d253", 2, 2, "7/5/20222", "YEAR_TYPO_5+_DIGITS",
     "07/05/2022", "raw='7/5/20222' (extra '2' in year)"),
    ("10756", "24679586f599d88e9f50d10faaca0b72", 1, 1, "1/2/42023", "YEAR_TYPO_5+_DIGITS",
     "01/02/2023", "raw='1/2/42023' (extra '4' in year)"),

    # UNKNOWN_FORMAT
    ("1169",  "d6d9181af76b5420b53a38fd92600cb4", 1, 1, "8/22006", "UNKNOWN_FORMAT",
     "08/22/2006", "raw='8/22006' (missing slash); propose 8/22/2006"),
    ("1817",  "7bec60821b1039c610d5bdba4c9cf5a5", 1, 1, "1/20/2010\n - ", "UNKNOWN_FORMAT",
     "01/20/2010", "raw='1/20/2010\\n - ' (trailing junk); strip"),
    ("1980",  "9709d1b16a23576b955edfe4d7f0cec2", 2, 2, "45/2012", "UNKNOWN_FORMAT",
     None, "raw='45/2012' (only month?/year); date unrecoverable; propose NULL"),
    ("2004",  "2c34578e94c129ed72e290dcae3f14d0", 4, 4, "FNA Isthmus:", "UNKNOWN_FORMAT",
     None, "raw='FNA Isthmus:' (free text, no date); propose NULL"),
    ("2221",  "a2ddb4ce39d091b01a5261064fbe1605", 1, 1, "10/04/2019\n -  ", "UNKNOWN_FORMAT",
     "10/04/2019", "raw='10/04/2019\\n -  ' (trailing junk); strip"),
    ("234",   "dd4cb69921880ee0983b2996e2a92a2f", 3, 4, "\n\n9/23/2005\n-", "UNKNOWN_FORMAT",
     "09/23/2005", "raw='\\n\\n9/23/2005\\n-' (whitespace + trailing dash); strip"),
    ("2723",  "c0a1bef57e6e9cc53722e7d983fb2470", 2, 2, "9/14//2015", "UNKNOWN_FORMAT",
     "09/14/2015", "raw='9/14//2015' (extra slash); fix"),
    ("3061",  "1b796597964f2b70fe52ccb293ea9199", 1, 1, "OSH (not specified)", "UNKNOWN_FORMAT",
     None, "raw='OSH (not specified)' (free text); propose NULL"),
    ("3593",  "ad7425717fccccc6c975667cd5472698", 1, 1, "0/7/2011", "UNKNOWN_FORMAT",
     None, "raw='0/7/2011' (month=0 invalid); date unrecoverable; propose NULL or override"),
    ("3730",  "78464b20bc8ea0c4a00d2f5c49ca282e", 3, 3, "RL FNA", "UNKNOWN_FORMAT",
     None, "raw='RL FNA' (free text); propose NULL"),
    ("3891",  "a33c53864791718cdd610b5750051b03", 2, 2, "3/302018\n", "UNKNOWN_FORMAT",
     "03/30/2018", "raw='3/302018\\n' (slash missing between day and year); propose 3/30/2018"),
    ("4418",  "4b656ec61280110869753b7811148ee6", 1, 1, "5/30/2014?? - inaccurate", "UNKNOWN_FORMAT",
     None, "raw='5/30/2014?? - inaccurate' -- Logan flagged inaccurate; propose NULL or override with 5/30/2014"),
    ("4418",  "74b0d2fc4c7d1e972ca9dc2a64d5f8a7", 2, 2, "5/30/2014?? - inaccurate", "UNKNOWN_FORMAT",
     None, "raw='5/30/2014?? - inaccurate' -- Logan flagged inaccurate; propose NULL or override"),
    ("4460",  "f7b626322a189049f74a399d419bee4d", 1, 1, "918/2015", "UNKNOWN_FORMAT",
     "09/18/2015", "raw='918/2015' (missing slash); propose 9/18/2015"),
    ("4709",  "8c0c806f5531fe1a499409fb6c2a73b3", 1, 1, "111/17/2014", "UNKNOWN_FORMAT",
     "11/17/2014", "raw='111/17/2014' (extra '1'); propose 11/17/2014"),
    ("4952",  "633af853cea504dcb088c78fba4add94", 2, 2, "\n10/06/2011\n-", "UNKNOWN_FORMAT",
     "10/06/2011", "raw='\\n10/06/2011\\n-' (whitespace + trailing dash)"),
    ("5536",  "61997fcce5ac1931d8cc72d05307074c", 2, 2, "1/29/2016\n:", "UNKNOWN_FORMAT",
     "01/29/2016", "raw='1/29/2016\\n:' (trailing colon); strip"),
    ("5777",  "5a0e2bc24e4ebf607729dd7da155f819", 2, 2, "3/8/2016\n: ", "UNKNOWN_FORMAT",
     "03/08/2016", "raw='3/8/2016\\n: ' (trailing colon); strip"),
    ("5804",  "eaac68a32f341cd72c8e1e76ac21c1f7", 2, 2, "8/10/2016\n:", "UNKNOWN_FORMAT",
     "08/10/2016", "raw='8/10/2016\\n:' (trailing colon); strip"),
    ("5907",  "94d24ce24b732f8376ecdd4c7836e3df", 1, 1, "3//14/2017", "UNKNOWN_FORMAT",
     "03/14/2017", "raw='3//14/2017' (extra slash); fix"),
    ("5937",  "2ada4b50581698fd05c16296f1633b8d", 2, 2, "8/8/2016\n:", "UNKNOWN_FORMAT",
     "08/08/2016", "raw='8/8/2016\\n:' (trailing colon); strip"),
    ("6077",  "b28894ec14638aeb40df3fe28db30391", 3, 3, "3/29/2016\n:", "UNKNOWN_FORMAT",
     "03/29/2016", "raw='3/29/2016\\n:' (trailing colon); strip"),
    ("6865",  "d246cb396f3396a7400e167c0c8b4dfe", 4, 4, '"5/1/2012', "UNKNOWN_FORMAT",
     "05/01/2012", "raw='\"5/1/2012' (leading quote); strip"),
    ("7332",  "6dd6dc944390da2d5d7ccfdd1ee0dd1b", 1, 1, "c", "UNKNOWN_FORMAT",
     None, "raw='c' (single char, no info); propose NULL"),
    ("9049",  "b06dc027cb7234e421a86ca56dae9314", 1, 1, "7/13//2021", "UNKNOWN_FORMAT",
     "07/13/2021", "raw='7/13//2021' (extra slash); fix"),
]


PREAMBLE = """\
# FNA cleanup round 2 -- canonical_fna_events_v1.fna_date_raw
# Generated 2026-04-27 by qc_framework_v1/scripts/build_fna_date_raw_cleanup_csv.py
# Source: 53 rows in canonical_fna_events_v1 with non-parseable fna_date_raw values
# (after Step A cleanup that removed 5 phantom rows: 10637/3, 1640/1, 1701/1, 1964/1, 2904/1)
#
# Each row has a `proposed_correction` field with Claude's best guess (in MM/DD/YYYY
# format) plus rationale in `proposed_note`. To accept the proposal, fill `your_decision`
# with ACCEPT. To override, write the corrected value (MM/DD/YYYY) or NULL.
# To reject and leave as-is, write KEEP.
#
# After you save the filled CSV, Claude will bulk-apply via query_rw and capture in
# qc_framework_v1/migrations/67_fna_date_raw_cleanup_round2.sql.
#
# Decision values:
#   ACCEPT       -- use proposed_correction
#   <MM/DD/YYYY> -- override with this value
#   NULL         -- set to NULL
#   KEEP         -- leave fna_date_raw unchanged (rare)
"""


def main() -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="") as f:
        f.write(PREAMBLE)
        w = csv.writer(f)
        w.writerow([
            "research_id", "fna_event_id", "fna_index", "fna_seq_n",
            "current_db_value", "bucket", "proposed_correction",
            "proposed_note", "your_decision", "your_note",
        ])
        for rid, eid, idx, seq, raw, bucket, prop, note in ROWS:
            w.writerow([
                rid, eid, idx, seq,
                raw, bucket,
                prop if prop is not None else "NULL",
                note,
                "",  # your_decision (Logan fills)
                "",  # your_note      (Logan fills)
            ])
    print(f"[cleanup_csv] wrote {OUT_CSV}")
    print(f"[cleanup_csv]   rows: {len(ROWS)}")
    by_bucket: dict[str, int] = {}
    for r in ROWS:
        by_bucket[r[5]] = by_bucket.get(r[5], 0) + 1
    print(f"[cleanup_csv]   by bucket:")
    for b, n in sorted(by_bucket.items()):
        print(f"    {b:24s} {n:4d}")


if __name__ == "__main__":
    main()
