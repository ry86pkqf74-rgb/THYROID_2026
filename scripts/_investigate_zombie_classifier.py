#!/usr/bin/env python3
"""Investigate canonical_us_nodule_v2 to derive the right zombie classifier.

PHI-safe: never prints location_raw values; only counts and patterns.
Target buckets (from operator's prior probe, must match):
    clean_llm_parsed:    18,310
    clean_non_llm:       17,090
    zombie_parent:        2,152
    llm_parsed_but_blob:     27
    aggregate_row:         141 (already matched)
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import duckdb  # noqa: E402

from motherduck_client import get_token  # noqa: E402

PUB = "thyroid_canonical_publication_v1_0"
TBL = f'"{PUB}".main.canonical_us_nodule_v2'

EXPECTED = {
    "clean_llm_parsed": 18_310,
    "clean_non_llm":    17_090,
    "zombie_parent":     2_152,
    "llm_parsed_but_blob":  27,
    "aggregate_row":      141,
}


def hr(label: str) -> None:
    print(f"\n{'='*78}\n{label}\n{'='*78}")


def main() -> int:
    tok = get_token()
    if not tok:
        raise SystemExit("no token")
    con = duckdb.connect(f"md:{PUB}?motherduck_token={tok}")

    hr("0. Column list")
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog=? AND table_schema='main' "
        "AND table_name='canonical_us_nodule_v2' "
        "ORDER BY ordinal_position",
        [PUB],
    ).fetchall()]
    for c in cols:
        print(f"  {c}")
    print(f"\n  total cols: {len(cols)}")

    hr("1. source_* flag distribution (TRUE counts)")
    bool_flags = [
        "source_base", "source_tirads_v2", "source_tirads_llm",
        "source_dynamics_llm", "source_fna_linkage",
        "source_us_nodules_tirads",
    ]
    for f in bool_flags:
        n_true = int(con.execute(
            f'SELECT COUNT(*) FROM {TBL} WHERE "{f}" = TRUE'
        ).fetchone()[0])
        n_false = int(con.execute(
            f'SELECT COUNT(*) FROM {TBL} WHERE "{f}" = FALSE'
        ).fetchone()[0])
        n_null = int(con.execute(
            f'SELECT COUNT(*) FROM {TBL} WHERE "{f}" IS NULL'
        ).fetchone()[0])
        print(f"  {f:35s} TRUE={n_true:>7,} FALSE={n_false:>7,} "
              f"NULL={n_null:>7,}")

    hr("1b. source_tables_cunc_legacy (STRING) value distribution")
    rows = con.execute(
        f"""
        SELECT source_tables_cunc_legacy, COUNT(*) AS n
        FROM {TBL}
        WHERE COALESCE(is_aggregate_row,FALSE)=FALSE
        GROUP BY 1
        ORDER BY n DESC
        """
    ).fetchall()
    print(f"  distinct values: {len(rows)}")
    for v, n in rows[:30]:
        print(f"  {n:>7,}  {v!r}")
    if len(rows) > 30:
        print(f"  ... ({len(rows) - 30} more)")

    hr("1c. Pattern probes on source_tables_cunc_legacy")
    pats = {
        "contains 'llm'": "%llm%",
        "contains 'imaging_nodule_master'": "%imaging_nodule_master%",
        "contains 'tirads'": "%tirads%",
        "contains 'cunc'": "%cunc%",
        "contains 'us_nodules'": "%us_nodules%",
        "is NULL or empty": None,
    }
    for label, pat in pats.items():
        if pat is None:
            n = int(con.execute(
                f"SELECT COUNT(*) FROM {TBL} "
                "WHERE COALESCE(is_aggregate_row,FALSE)=FALSE "
                "  AND (source_tables_cunc_legacy IS NULL "
                "       OR source_tables_cunc_legacy = '')"
            ).fetchone()[0])
        else:
            n = int(con.execute(
                f"SELECT COUNT(*) FROM {TBL} "
                "WHERE COALESCE(is_aggregate_row,FALSE)=FALSE "
                "  AND LOWER(COALESCE(source_tables_cunc_legacy,'')) "
                f"LIKE '{pat}'"
            ).fetchone()[0])
        print(f"  {label:50s} {n:>7,}")

    hr("2. Total + aggregate_row split")
    n_total = int(con.execute(f"SELECT COUNT(*) FROM {TBL}").fetchone()[0])
    n_agg = int(con.execute(
        f"SELECT COUNT(*) FROM {TBL} WHERE COALESCE(is_aggregate_row,FALSE)=TRUE"
    ).fetchone()[0])
    n_non_agg = n_total - n_agg
    print(f"  total rows:           {n_total:,}")
    print(f"  aggregate_row=TRUE:   {n_agg:,}  (expected 141)")
    print(f"  non-aggregate:        {n_non_agg:,}  (expected 37,579)")

    hr("3. location_raw shape (PHI-safe: pattern counts only)")
    n_loc_null = int(con.execute(
        f"SELECT COUNT(*) FROM {TBL} WHERE location_raw IS NULL"
    ).fetchone()[0])
    n_loc_empty = int(con.execute(
        f"SELECT COUNT(*) FROM {TBL} WHERE location_raw = ''"
    ).fetchone()[0])
    n_semi = int(con.execute(
        f"SELECT COUNT(*) FROM {TBL} WHERE location_raw LIKE '%;%'"
    ).fetchone()[0])
    n_2plus_semi = int(con.execute(
        f"SELECT COUNT(*) FROM {TBL} "
        "WHERE LENGTH(location_raw) - LENGTH(REPLACE(location_raw,';','')) >= 2"
    ).fetchone()[0])
    n_3plus_semi = int(con.execute(
        f"SELECT COUNT(*) FROM {TBL} "
        "WHERE LENGTH(location_raw) - LENGTH(REPLACE(location_raw,';','')) >= 3"
    ).fetchone()[0])
    n_both_lat = int(con.execute(
        f"SELECT COUNT(*) FROM {TBL} "
        "WHERE LOWER(location_raw) LIKE '%right%' "
        "  AND LOWER(location_raw) LIKE '%left%'"
    ).fetchone()[0])
    n_long = int(con.execute(
        f"SELECT COUNT(*) FROM {TBL} WHERE LENGTH(location_raw) > 200"
    ).fetchone()[0])
    n_long500 = int(con.execute(
        f"SELECT COUNT(*) FROM {TBL} WHERE LENGTH(location_raw) > 500"
    ).fetchone()[0])
    print(f"  location_raw NULL:                 {n_loc_null:>7,}")
    print(f"  location_raw '':                   {n_loc_empty:>7,}")
    print(f"  has 1+ semicolon:                  {n_semi:>7,}")
    print(f"  has 2+ semicolons:                 {n_2plus_semi:>7,}")
    print(f"  has 3+ semicolons:                 {n_3plus_semi:>7,}")
    print(f"  contains both 'right' and 'left':  {n_both_lat:>7,}")
    print(f"  length > 200 chars:                {n_long:>7,}")
    print(f"  length > 500 chars:                {n_long500:>7,}")

    hr("4. Candidate LLM-detector counts (non-aggregate rows only)")
    candidates = {
        "tirads_llm OR dynamics_llm":
            "(source_tirads_llm=TRUE OR source_dynamics_llm=TRUE)",
        "tirads_llm only":
            "source_tirads_llm=TRUE",
        "dynamics_llm only":
            "source_dynamics_llm=TRUE",
        "any source_*_llm":
            "(source_tirads_llm=TRUE OR source_dynamics_llm=TRUE)",
        "us_nodules_tirads":
            "source_us_nodules_tirads=TRUE",
        "tirads_v2":
            "source_tirads_v2=TRUE",
        "fna_linkage":
            "source_fna_linkage=TRUE",
        "base":
            "source_base=TRUE",
    }
    for name, pred in candidates.items():
        n = int(con.execute(
            f"SELECT COUNT(*) FROM {TBL} "
            f"WHERE COALESCE(is_aggregate_row,FALSE)=FALSE AND {pred}"
        ).fetchone()[0])
        print(f"  {name:35s} {n:>7,}")

    hr("5. Try classifier candidates, score against expected")
    # Candidate A: my current
    cand_a = {
        "name": "A (current script)",
        "is_llm": "(COALESCE(source_dynamics_llm,FALSE) "
                  "OR COALESCE(source_tirads_llm,FALSE) "
                  "OR COALESCE(source_us_nodules_tirads,FALSE))",
        "is_blob": "COALESCE(location_raw,'') LIKE '%;%'",
    }
    # Candidate B: drop us_nodules_tirads from LLM
    cand_b = {
        "name": "B (LLM = tirads_llm OR dynamics_llm only)",
        "is_llm": "(COALESCE(source_dynamics_llm,FALSE) "
                  "OR COALESCE(source_tirads_llm,FALSE))",
        "is_blob": "COALESCE(location_raw,'') LIKE '%;%'",
    }
    # Candidate C: blob = both lateralities present
    cand_c = {
        "name": "C (LLM=B; blob = right+left lateralities)",
        "is_llm": "(COALESCE(source_dynamics_llm,FALSE) "
                  "OR COALESCE(source_tirads_llm,FALSE))",
        "is_blob": "(LOWER(COALESCE(location_raw,'')) LIKE '%right%' "
                   "AND LOWER(COALESCE(location_raw,'')) LIKE '%left%')",
    }
    # Candidate D: blob = 2+ semicolons OR both lateralities
    cand_d = {
        "name": "D (LLM=B; blob = 2+ semicolons OR both lateralities)",
        "is_llm": "(COALESCE(source_dynamics_llm,FALSE) "
                  "OR COALESCE(source_tirads_llm,FALSE))",
        "is_blob": "((LENGTH(COALESCE(location_raw,'')) "
                   "- LENGTH(REPLACE(COALESCE(location_raw,''),';','')) >= 2) "
                   "OR (LOWER(COALESCE(location_raw,'')) LIKE '%right%' "
                   "AND LOWER(COALESCE(location_raw,'')) LIKE '%left%'))",
    }
    # Candidate E: blob = length > 100
    cand_e = {
        "name": "E (LLM=B; blob = length > 100)",
        "is_llm": "(COALESCE(source_dynamics_llm,FALSE) "
                  "OR COALESCE(source_tirads_llm,FALSE))",
        "is_blob": "LENGTH(COALESCE(location_raw,'')) > 100",
    }
    # Candidate F: blob = length > 200
    cand_f = {
        "name": "F (LLM=B; blob = length > 200)",
        "is_llm": "(COALESCE(source_dynamics_llm,FALSE) "
                  "OR COALESCE(source_tirads_llm,FALSE))",
        "is_blob": "LENGTH(COALESCE(location_raw,'')) > 200",
    }

    hr("4b. Other potentially-discriminating columns")
    for col, where in [
        ("nlp_backfill_pending TRUE",
         "nlp_backfill_pending=TRUE"),
        ("nlp_backfill_pending FALSE",
         "nlp_backfill_pending=FALSE"),
        ("nlp_backfill_pending NULL",
         "nlp_backfill_pending IS NULL"),
        ("nodule_index_within_exam IS NULL",
         "nodule_index_within_exam IS NULL"),
        ("nodule_index_within_exam = 1",
         "nodule_index_within_exam = 1"),
        ("nodule_index_within_exam > 1",
         "nodule_index_within_exam > 1"),
        ("source_tables_cunc_legacy IS NULL",
         "source_tables_cunc_legacy IS NULL"),
        ("source_tables_cunc_legacy LIKE '%llm%'",
         "LOWER(COALESCE(source_tables_cunc_legacy,'')) LIKE '%llm%'"),
        ("source_tables_cunc_legacy 'imaging_nodule_master_v1' only",
         "source_tables_cunc_legacy = 'imaging_nodule_master_v1'"),
        ("location_raw IS NULL",
         "location_raw IS NULL"),
        ("nodule_master_id IS NULL",
         "nodule_master_id IS NULL"),
        ("nodule_master_id IS NOT NULL",
         "nodule_master_id IS NOT NULL"),
    ]:
        n = int(con.execute(
            f"SELECT COUNT(*) FROM {TBL} "
            "WHERE COALESCE(is_aggregate_row,FALSE)=FALSE "
            f"  AND ({where})"
        ).fetchone()[0])
        print(f"  {col:55s} {n:>7,}")

    hr("4c. Cross-tab: source_tables_cunc_legacy x nlp_backfill_pending")
    rows = con.execute(
        f"""
        SELECT
          COALESCE(source_tables_cunc_legacy,'<NULL>') AS prov,
          COALESCE(CAST(nlp_backfill_pending AS VARCHAR),'<NULL>') AS pend,
          COALESCE(CAST(is_aggregate_row AS VARCHAR),'<NULL>') AS agg,
          COUNT(*) AS n
        FROM {TBL}
        GROUP BY 1, 2, 3
        ORDER BY n DESC
        """
    ).fetchall()
    print(f"  {'provenance':<55s} pending agg n")
    for prov, pend, agg, n in rows:
        print(f"  {prov:<55s} {pend:<7s} {agg:<5s} {n:>7,}")

    candidates_to_try = [cand_a, cand_b, cand_c, cand_d, cand_e, cand_f]
    for cand in candidates_to_try:
        sql = f"""
        SELECT
          CASE
            WHEN COALESCE(is_aggregate_row,FALSE) THEN 'aggregate_row'
            WHEN {cand['is_llm']} AND {cand['is_blob']} THEN 'llm_parsed_but_blob'
            WHEN {cand['is_llm']} THEN 'clean_llm_parsed'
            WHEN {cand['is_blob']} THEN 'zombie_parent'
            ELSE 'clean_non_llm'
          END AS bucket,
          COUNT(*) AS n
        FROM {TBL}
        GROUP BY bucket
        """
        rows = {b: int(n) for b, n in con.execute(sql).fetchall()}
        print(f"\n  {cand['name']}")
        score = 0
        for k, exp in EXPECTED.items():
            actual = rows.get(k, 0)
            ok = "ok" if abs(actual - exp) <= max(2, exp * 0.02) else "DRIFT"
            print(f"    {k:25s} actual={actual:>7,} exp={exp:>7,} {ok}")
            if ok == "ok":
                score += 1
        print(f"    score: {score}/5")

    return 0


if __name__ == "__main__":
    sys.exit(main())
