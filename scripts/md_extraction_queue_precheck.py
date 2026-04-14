#!/usr/bin/env python3
"""
High-ROI MotherDuck pre-checks before TI-RADS / LN / Bethesda batch extraction.

1. Anti-join each queue RID list against canonical_extracted_fact_long_v2 so we only
   send RIDs that lack promoted facts (shrinks qwen3:32b cost).
2. Report distinct RIDs already covered in canonical for TIRADS / LN / Bethesda-like
   entity_types (validates whether Excel queue counts are inflated vs promoted gap).
3. For RIDs in-queue but absent from local notes parquet, count how many exist in
   MotherDuck clinical_notes_long (ingested vs preprocess drop).

Requires RW MotherDuck token (motherduck.local.toml or env). Never prints secrets.

Usage:
  .venv/bin/python scripts/md_extraction_queue_precheck.py --md
  .venv/bin/python scripts/md_extraction_queue_precheck.py --md --notes-parquet path/to/clinical_notes_long.parquet
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402


def _read_rid_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    out: list[str] = []
    for line in path.read_text().splitlines():
        s = line.strip()
        if s:
            out.append(s)
    return out


def _norm_rid_series(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    return x


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--md",
        action="store_true",
        help="Connect to MotherDuck (fail-closed).",
    )
    ap.add_argument(
        "--queues-dir",
        type=Path,
        default=ROOT / "queues",
        help="Directory with ids_tirads.txt, ids_ln.txt, ids_fna.txt",
    )
    ap.add_argument(
        "--notes-parquet",
        type=Path,
        default=ROOT / "processed" / "remaining" / "clinical_notes_long.parquet",
        help="Local notes parquet used for missing-RID logic (same as filter_notes_for_rerun).",
    )
    ap.add_argument(
        "--out-json",
        type=Path,
        default=ROOT / "studies" / "md_extraction_queue_precheck_latest.json",
        help="Write machine-readable results here.",
    )
    args = ap.parse_args()
    if not args.md:
        print("This script is intended for MotherDuck; pass --md", file=sys.stderr)
        sys.exit(2)

    tirads_path = args.queues_dir / "ids_tirads.txt"
    ln_path = args.queues_dir / "ids_ln.txt"
    fna_path = args.queues_dir / "ids_fna.txt"

    tirads = _read_rid_file(tirads_path)
    ln_ids = _read_rid_file(ln_path)
    fna_ids = _read_rid_file(fna_path)

    union = sorted(set(tirads) | set(ln_ids) | set(fna_ids))

    missing_from_notes: list[str] = []
    notes_rid_n: int | None = None
    if args.notes_parquet.exists():
        notes = pd.read_parquet(args.notes_parquet)
        notes["research_id"] = _norm_rid_series(notes["research_id"])
        notes_rids = set(notes["research_id"].unique())
        notes_rid_n = len(notes_rids)
        missing_from_notes = sorted(set(union) - notes_rids, key=lambda x: (len(x), x))
    else:
        print(
            f"  WARN: notes parquet missing: {args.notes_parquet} — "
            "skip local missing-list vs MD cross-check (md_clinical_notes_long_coverage still runs)."
        )

    con = connect_md_fail_closed(
        ROOT / "thyroid_master.duckdb",
        custom_user_agent="md_extraction_queue_precheck.py",
    )

    # Entity sets (domain_mapping + user request)
    tirads_entities_narrow = ("tirads_category", "tirads_derived")
    tirads_entities_wide = tirads_entities_narrow + ("tirads_score",)
    ln_entities = ("ultrasound_lymph_node",)
    bethesda_entities = ("bethesda_class",)

    def _already_promoted_count(entity_tuple: tuple[str, ...]) -> int:
        ents = ", ".join(f"'{e}'" for e in entity_tuple)
        row = con.execute(
            f"""
            SELECT COUNT(DISTINCT research_id)::BIGINT
            FROM canonical_extracted_fact_long_v2
            WHERE entity_type IN ({ents})
            """
        ).fetchone()
        return int(row[0]) if row else 0

    # Register queue temp tables
    def _register(name: str, ids: list[str]) -> None:
        df = pd.DataFrame({"research_id": ids})
        con.register(name, df)

    _register("q_tirads", tirads)
    _register("q_ln", ln_ids)
    _register("q_fna", fna_ids)

    results: dict = {
        "queues_dir": str(args.queues_dir),
        "queue_sizes": {
            "ids_tirads": len(tirads),
            "ids_ln": len(ln_ids),
            "ids_fna": len(fna_ids),
            "union_distinct": len(union),
        },
        "notes_parquet": str(args.notes_parquet),
        "notes_parquet_distinct_rids": notes_rid_n,
        "missing_from_notes_parquet_count": (
            len(missing_from_notes) if notes_rid_n is not None else None
        ),
        "canonical_distinct_rids_with_facts": {},
        "anti_join_still_needed_after_promoted": {},
        "missing_notes_md_clinical_notes_long": None,
    }

    for label, ents in [
        ("tirads_narrow_tirads_category_derived", tirads_entities_narrow),
        ("tirads_plus_tirads_score", tirads_entities_wide),
        ("ln_ultrasound_lymph_node", ln_entities),
        ("bethesda_bethesda_class", bethesda_entities),
    ]:
        results["canonical_distinct_rids_with_facts"][label] = _already_promoted_count(ents)

    # Anti-join: RIDs in queue that still need extraction (no matching promoted fact)
    for qname, ents, tbl in [
        ("tirads", tirads_entities_narrow, "q_tirads"),
        ("tirads_wide", tirads_entities_wide, "q_tirads"),
        ("ln", ln_entities, "q_ln"),
        ("fna_bethesda", bethesda_entities, "q_fna"),
    ]:
        ents_sql = ", ".join(f"'{e}'" for e in ents)
        key = f"still_needed_{qname}"
        sql = f"""
        SELECT COUNT(DISTINCT q.research_id)::BIGINT AS n
        FROM {tbl} q
        WHERE NOT EXISTS (
          SELECT 1
          FROM canonical_extracted_fact_long_v2 f
          WHERE TRY_CAST(TRIM(CAST(q.research_id AS VARCHAR)) AS BIGINT)
                = TRY_CAST(f.research_id AS BIGINT)
            AND f.entity_type IN ({ents_sql})
        )
        """
        n = int(con.execute(sql).fetchone()[0])
        results["anti_join_still_needed_after_promoted"][key] = n

    # Per-queue: how many RIDs have ≥1 row in MD clinical_notes_long (batch feasibility)
    def _md_note_coverage(tbl: str) -> int:
        return int(
            con.execute(
                f"""
                SELECT COUNT(DISTINCT q.research_id)::BIGINT
                FROM {tbl} q
                INNER JOIN clinical_notes_long c
                  ON TRY_CAST(TRIM(CAST(q.research_id AS VARCHAR)) AS BIGINT)
                   = TRY_CAST(c.research_id AS BIGINT)
                """
            ).fetchone()[0]
        )

    results["md_clinical_notes_long_coverage"] = {
        "ids_tirads": _md_note_coverage("q_tirads"),
        "ids_ln": _md_note_coverage("q_ln"),
        "ids_fna": _md_note_coverage("q_fna"),
        "union_distinct": int(
            con.execute(
                """
                WITH u AS (
                  SELECT research_id FROM q_tirads
                  UNION SELECT research_id FROM q_ln
                  UNION SELECT research_id FROM q_fna
                )
                SELECT COUNT(DISTINCT u.research_id)::BIGINT
                FROM u
                INNER JOIN clinical_notes_long c
                  ON TRY_CAST(TRIM(CAST(u.research_id AS VARCHAR)) AS BIGINT)
                   = TRY_CAST(c.research_id AS BIGINT)
                """
            ).fetchone()[0]
        ),
        "note": "Counts RIDs in each queue that appear in MotherDuck clinical_notes_long (any note row).",
    }
    cov = results["md_clinical_notes_long_coverage"]
    u_tot = results["queue_sizes"]["union_distinct"]
    u_cov = cov["union_distinct"]
    results["md_clinical_notes_long_coverage"][
        "union_rids_with_no_md_notes_estimate"
    ] = max(0, u_tot - u_cov)

    # Missing-from-parquet × MD notes: how many of those RIDs have rows in clinical_notes_long?
    if missing_from_notes:
        miss_df = pd.DataFrame({"research_id": missing_from_notes})
        con.register("q_missing_from_local_parquet", miss_df)
        try:
            md_hit = con.execute(
                """
                SELECT COUNT(DISTINCT m.research_id)::BIGINT
                FROM q_missing_from_local_parquet m
                INNER JOIN clinical_notes_long c
                  ON TRY_CAST(TRIM(CAST(m.research_id AS VARCHAR)) AS BIGINT)
                   = TRY_CAST(c.research_id AS BIGINT)
                """
            ).fetchone()[0]
            results["missing_notes_md_clinical_notes_long"] = {
                "missing_from_local_parquet": len(missing_from_notes),
                "also_present_in_md_clinical_notes_long": int(md_hit),
                "interpretation": (
                    "If also_present > 0, local preprocess/filter dropped notes that exist in MD — "
                    "refresh/export notes parquet or align preprocess. If 0, RIDs were never ingested to MD notes."
                ),
            }
        except Exception as e:
            results["missing_notes_md_clinical_notes_long"] = {
                "error": type(e).__name__ + ": " + str(e)[:500],
            }
        finally:
            try:
                con.unregister("q_missing_from_local_parquet")
            except Exception:
                pass

    for reg in ("q_tirads", "q_ln", "q_fna"):
        try:
            con.unregister(reg)
        except Exception:
            pass
    con.close()

    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))
    print(f"\nWrote {args.out_json}")


if __name__ == "__main__":
    main()
