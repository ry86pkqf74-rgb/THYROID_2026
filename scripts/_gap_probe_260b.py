#!/usr/bin/env python3
"""Verify F1 (episode ingestion gap) and F7 (legacy column sweep) gaps live on MotherDuck.

Read-only. Uses _md_connect.connect_locked() which targets
thyroid_canonical_publication_v1_0.main.

Outputs JSON to stdout summarizing:
  * CPM dimensions
  * Presence and disagreement counts for the 4 legacy molecular columns
  * molecular_test_episode_v2 row count
  * Patients with mol_has_thyroseq=TRUE that have NO row in MTE (target: 443)
  * NGS-BRAF-positive patients with no MTE row (target: 46)
  * detail_table_registry_v1: rows with empty/NULL feeds_master_columns_normalized
  * Whether canonical_molecular_tested_v1 still exists & row count
  * Whether mte_tier column exists on MTE (Script 265 prompt-mission)
"""
from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402


def col_exists(con, table: str, col: str) -> bool:
    q = f"""
        SELECT 1 FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='{table}' AND column_name='{col}'
        LIMIT 1
    """
    return con.execute(q).fetchone() is not None


def table_exists(con, schema: str, table: str) -> bool:
    q = f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{schema}'
          AND table_name='{table}'
        LIMIT 1
    """
    return con.execute(q).fetchone() is not None


def safe_scalar(con, sql: str):
    try:
        return con.execute(sql).fetchone()[0]
    except Exception as e:
        return f"ERROR: {e}"


def main() -> int:
    con = connect_locked()
    out: dict = {"db": PUBLICATION_DB}

    # Dimensions
    out["cpm"] = {
        "n_rows": safe_scalar(con, "SELECT COUNT(*) FROM canonical_patient_master"),
        "n_cols": safe_scalar(
            con,
            f"SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
            f"AND table_name='canonical_patient_master'",
        ),
    }

    # F7: Legacy molecular columns presence + disagreement
    legacy_cols = [
        "molecular_tested_v7",
        "mol_test_count",
        "molecular_platforms_v7",
        "n_molecular_tests_v7",
    ]
    f7 = {}
    for c in legacy_cols:
        present = col_exists(con, "canonical_patient_master", c)
        info = {"present": present}
        if present:
            info["n_nulls"] = safe_scalar(
                con, f'SELECT COUNT(*) FROM canonical_patient_master WHERE "{c}" IS NULL'
            )
        f7[c] = info

    # Pairwise disagreement vs canonical replacements (only if both present)
    pairs = [
        ("molecular_tested_v7", "molecular_tested_confirmed"),
        ("mol_test_count", "mol_n_tests"),
        ("n_molecular_tests_v7", "mol_n_tests"),
    ]
    for legacy, canonical in pairs:
        if col_exists(con, "canonical_patient_master", legacy) and col_exists(
            con, "canonical_patient_master", canonical
        ):
            disagreement = safe_scalar(
                con,
                f"""
                SELECT COUNT(*) FROM canonical_patient_master
                WHERE COALESCE(CAST("{legacy}" AS VARCHAR), '__NULL__')
                   != COALESCE(CAST("{canonical}" AS VARCHAR), '__NULL__')
                """,
            )
            f7[f"disagreement::{legacy}_vs_{canonical}"] = disagreement
    out["F7_legacy_columns"] = f7

    # cpm_unmapped_triage_v265 catalog check (in main? in workspace?)
    triage_locations = []
    for sch in ("main", "manuscript_workspace"):
        if table_exists(con, sch, "cpm_unmapped_triage_v265"):
            triage_locations.append(sch)
    out["cpm_unmapped_triage_v265_in"] = triage_locations
    if triage_locations:
        sch = triage_locations[0]
        try:
            rows = con.execute(
                f"SELECT column_name FROM {PUBLICATION_DB}.{sch}.cpm_unmapped_triage_v265"
            ).fetchall()
            cat = [r[0] for r in rows]
            out["cpm_unmapped_triage_v265_legacy_present"] = sorted(
                [c for c in legacy_cols if c in cat]
            )
            out["cpm_unmapped_triage_v265_legacy_missing"] = sorted(
                [c for c in legacy_cols if c not in cat]
            )
        except Exception as e:
            out["cpm_unmapped_triage_v265_error"] = str(e)

    # F1: episode ingestion gap
    f1: dict = {}
    f1["mte_v2_present"] = table_exists(con, "main", "molecular_test_episode_v2")
    if f1["mte_v2_present"]:
        f1["mte_v2_rows"] = safe_scalar(
            con, "SELECT COUNT(*) FROM molecular_test_episode_v2"
        )
        f1["mte_v2_distinct_research_id"] = safe_scalar(
            con, "SELECT COUNT(DISTINCT research_id) FROM molecular_test_episode_v2"
        )
        f1["mte_v2_has_mte_tier_col"] = col_exists(
            con, "molecular_test_episode_v2", "mte_tier"
        )

        # 443 patients claim: mol_has_thyroseq=TRUE on CPM but no MTE row.
        # Need to detect the right column name on CPM.
        candidates = [
            "mol_has_thyroseq",
            "molecular_has_thyroseq",
            "thyroseq_tested",
            "has_thyroseq",
        ]
        thyroseq_col = next(
            (c for c in candidates if col_exists(con, "canonical_patient_master", c)),
            None,
        )
        f1["thyroseq_flag_col_on_cpm"] = thyroseq_col
        if thyroseq_col:
            f1["thyroseq_flag_TRUE_count"] = safe_scalar(
                con,
                f'SELECT COUNT(*) FROM canonical_patient_master WHERE "{thyroseq_col}"=TRUE',
            )
            f1["thyroseq_TRUE_no_mte_row"] = safe_scalar(
                con,
                f"""
                SELECT COUNT(*) FROM canonical_patient_master cpm
                WHERE cpm."{thyroseq_col}"=TRUE
                  AND cpm.research_id NOT IN (
                    SELECT DISTINCT research_id::VARCHAR FROM molecular_test_episode_v2
                  )
                """,
            )

        # ThyroSeq enrichment table — patients in enrichment with no episode
        if table_exists(con, "main", "thyroseq_molecular_enrichment"):
            f1["thyroseq_enrichment_rows"] = safe_scalar(
                con,
                "SELECT COUNT(*) FROM thyroseq_molecular_enrichment",
            )
            f1["thyroseq_enrichment_distinct_rid"] = safe_scalar(
                con,
                "SELECT COUNT(DISTINCT research_id) FROM thyroseq_molecular_enrichment",
            )
            f1["thyroseq_enrichment_rid_no_mte"] = safe_scalar(
                con,
                """
                SELECT COUNT(DISTINCT te.research_id)
                FROM thyroseq_molecular_enrichment te
                WHERE te.research_id NOT IN (
                    SELECT DISTINCT research_id FROM molecular_test_episode_v2
                )
                """,
            )

        # 46 NGS-BRAF-positive missing — try to find a likely table
        for cand in ("extracted_braf_recovery_v1",):
            if table_exists(con, "main", cand):
                f1[f"{cand}_rows"] = safe_scalar(con, f"SELECT COUNT(*) FROM {cand}")
                # find columns
                cols = [
                    r[0]
                    for r in con.execute(
                        f"""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                          AND table_name='{cand}'
                        """
                    ).fetchall()
                ]
                f1[f"{cand}_cols"] = cols

    out["F1_episode_gap"] = f1

    # canonical_molecular_tested_v1
    if table_exists(con, "main", "canonical_molecular_tested_v1"):
        out["canonical_molecular_tested_v1_rows"] = safe_scalar(
            con, "SELECT COUNT(*) FROM canonical_molecular_tested_v1"
        )

    # Registry normalization
    reg_loc = None
    for sch in ("manuscript_workspace", "main"):
        if table_exists(con, sch, "detail_table_registry_v1"):
            reg_loc = sch
            break
    out["detail_table_registry_v1_in"] = reg_loc
    if reg_loc:
        n_total = safe_scalar(
            con, f"SELECT COUNT(*) FROM {PUBLICATION_DB}.{reg_loc}.detail_table_registry_v1"
        )
        # Look at column names for registry
        reg_cols = [
            r[0]
            for r in con.execute(
                f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{reg_loc}'
                  AND table_name='detail_table_registry_v1'
                """
            ).fetchall()
        ]
        out["registry_columns"] = reg_cols
        out["registry_total_rows"] = n_total
        for c in (
            "feeds_master_columns",
            "feeds_master_columns_normalized",
        ):
            if c in reg_cols:
                out[f"registry_{c}_empty_or_null"] = safe_scalar(
                    con,
                    f"""
                    SELECT COUNT(*) FROM {PUBLICATION_DB}.{reg_loc}.detail_table_registry_v1
                    WHERE "{c}" IS NULL OR TRIM("{c}")='' OR LOWER("{c}") IN ('todo','(unset)','nan')
                    """,
                )

    # Conventions table for bethesda_semantics
    if table_exists(con, "manuscript_workspace", "__conventions"):
        out["bethesda_semantics_in_conventions"] = safe_scalar(
            con,
            f"""
            SELECT COUNT(*) FROM {PUBLICATION_DB}.manuscript_workspace."__conventions"
            WHERE convention_id='bethesda_semantics'
            """,
        )

    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
