#!/usr/bin/env python3
"""Phase 0c — specific recurrence + followup source checks."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import duckdb  # noqa: E402

from motherduck_client import get_token  # noqa: E402

PUB = "thyroid_canonical_publication_v1_0"
REF = "Thyroid 2026 UPdated"
OUT = REPO / "scripts" / "output" / "233"


def main() -> None:
    con = duckdb.connect(f"md:?motherduck_token={get_token()}")
    con.execute(f'USE "{PUB}"')
    con.execute(f'USE "{PUB}".main')

    rep: dict[str, object] = {}

    # Check which key columns exist in CPM
    want_cols = [
        "any_recurrence_flag", "recurrence_flag_v2", "recurrence_flag_scoring",
        "structural_recurrence_flag", "recurrence_definition",
        "time_to_recurrence_days", "recurrence_days_from_surg",
        "recurrence_site", "recurrence_site_text",
        "followup_days", "followup_years", "followup_or_death_date",
        "death_date", "death_occurred",
        "last_contact_date", "last_contact_source",
        "last_tg_date", "last_tsh_date", "last_imaging_date",
        "first_surgery_date",
        "fna_path_outcome", "mortality_type",
    ]
    rows = con.execute(
        f"""SELECT column_name FROM information_schema.columns
            WHERE table_catalog = '{PUB}' AND table_schema = 'main'
              AND table_name = 'canonical_patient_master'
              AND column_name IN ({','.join(f"'{c}'" for c in want_cols)})"""
    ).fetchall()
    present = {r[0] for r in rows}
    rep["cpm_column_presence"] = {c: (c in present) for c in want_cols}

    # Phantom recurrence breakdown by source cols
    sql = """
        SELECT
          COUNT(*) FILTER (WHERE any_recurrence_flag = TRUE) n_any,
          COUNT(*) FILTER (WHERE any_recurrence_flag = TRUE AND recurrence_definition = 'no_recurrence_evidence') n_phantom,
          COUNT(*) FILTER (WHERE recurrence_flag_v2 = TRUE) n_v2,
          COUNT(*) FILTER (WHERE recurrence_flag_scoring = TRUE) n_scoring,
          COUNT(*) FILTER (WHERE structural_recurrence_flag = TRUE) n_struct,
          COUNT(*) FILTER (WHERE
            (COALESCE(recurrence_flag_v2, FALSE) = TRUE
             OR COALESCE(recurrence_flag_scoring, FALSE) = TRUE
             OR COALESCE(structural_recurrence_flag, FALSE) = TRUE)
            AND recurrence_definition <> 'no_recurrence_evidence'
          ) n_proposed
        FROM canonical_patient_master
    """
    try:
        rep["recurrence_counts"] = dict(
            zip(["n_any", "n_phantom", "n_v2", "n_scoring", "n_struct", "n_proposed"],
                con.execute(sql).fetchone())
        )
    except Exception as e:
        rep["recurrence_err"] = str(e)

    # Negative rds breakdown
    try:
        rep["neg_rds_examples"] = con.execute(
            """SELECT research_id, first_surgery_date, recurrence_date,
                      recurrence_days_from_surg, time_to_recurrence_days,
                      recurrence_definition, recurrence_type
               FROM canonical_patient_master
               WHERE recurrence_days_from_surg < 0
               LIMIT 15"""
        ).fetchdf().to_dict(orient="records")
    except Exception as e:
        rep["neg_rds_err"] = str(e)

    # Zero-followup breakdown
    try:
        rep["zero_fu_breakdown"] = con.execute(
            """SELECT
                 COUNT(*) FILTER (WHERE COALESCE(followup_days,0) = 0) n_zero,
                 COUNT(*) FILTER (WHERE COALESCE(followup_days,0) = 0 AND first_surgery_date IS NULL) n_zero_nosurg,
                 COUNT(*) FILTER (WHERE COALESCE(followup_days,0) = 0 AND last_contact_date = first_surgery_date) n_zero_sameday,
                 COUNT(*) FILTER (WHERE COALESCE(followup_days,0) = 0 AND followup_or_death_date IS NOT NULL) n_zero_have_fod,
                 COUNT(*) FILTER (WHERE COALESCE(followup_days,0) = 0 AND death_date IS NOT NULL) n_zero_have_death,
                 COUNT(*) FILTER (WHERE COALESCE(followup_days,0) = 0 AND last_tg_date IS NOT NULL) n_zero_have_tg
               FROM canonical_patient_master"""
        ).fetchone()
    except Exception as e:
        rep["zero_fu_err"] = str(e)

    # thyroid_scoring_py_v1 check
    try:
        gold = con.execute(
            f"""SELECT view_definition FROM information_schema.views
                WHERE table_catalog = '{REF}' AND table_name = 'gold_master_patient_facts_v1'"""
        ).fetchone()
        rep["gold_master_uses_scoring_py"] = (
            bool(gold) and "thyroid_scoring_py" in (gold[0] or "")
        )
        rep["gold_master_def_head"] = (gold[0][:600] if gold else None)
    except Exception as e:
        rep["gold_err"] = str(e)

    # md_synoptic_tumor_long_v1 vs synoptic_tumor_long_v1 identity check
    try:
        a = con.execute(
            f'SELECT COUNT(*) FROM "{REF}".main.md_synoptic_tumor_long_v1'
        ).fetchone()[0]
        b = con.execute(
            f'SELECT COUNT(*) FROM "{PUB}".main.synoptic_tumor_long_v1'
        ).fetchone()[0]
        rep["md_synoptic_vs_pub"] = {"ref_md": a, "pub_v1": b}
    except Exception as e:
        rep["md_synoptic_err"] = str(e)
    try:
        a = con.execute(
            f'SELECT COUNT(*) FROM "{REF}".main.md_extracted_fna_bethesda_v1'
        ).fetchone()[0]
        b = con.execute(
            f'SELECT COUNT(*) FROM "{PUB}".main.extracted_fna_bethesda_v1'
        ).fetchone()[0]
        rep["md_fna_vs_pub"] = {"ref_md": a, "pub_v1": b}
    except Exception as e:
        rep["md_fna_err"] = str(e)

    # Check data_dictionary_v221 columns
    try:
        rep["dict_v221_columns"] = [
            r[0] for r in con.execute(
                f"""SELECT column_name FROM information_schema.columns
                    WHERE table_catalog = '{REF}' AND table_schema = 'main'
                      AND table_name = 'data_dictionary_v221'
                    ORDER BY ordinal_position"""
            ).fetchall()
        ]
        rep["dict_v221_sample"] = con.execute(
            f'SELECT * FROM "{REF}".main.data_dictionary_v221 LIMIT 3'
        ).fetchdf().to_dict(orient="records")
    except Exception as e:
        rep["dict_v221_err"] = str(e)

    # Extension sources for followup recovery: do they exist and have data?
    ext_sources = [
        ("rai_treatment_episode_v2", "resolved_rai_date"),
        ("note_entities_llm_survival_followup", "note_date"),
        ("note_entities_llm_recurrence", "note_date"),
        ("tg_postop_surveillance_windows_v1", "window_end_date"),
        ("ultrasound_reports", "ultrasound_date"),
        ("ct_imaging", "date_of_exam"),
        ("mri_imaging", "date_of_exam"),
        ("nuclear_med", "scandate"),
        ("operative_episode_detail_v2", "surgery_date_native"),
        ("nsqip_enrichment", "nsqip_opdate"),
        ("note_entities_llm_past_surgical_hx", "note_date"),
    ]
    rep["ext_sources"] = {}
    for t, c in ext_sources:
        try:
            cnt = con.execute(
                f'SELECT COUNT(*) FROM "{PUB}".main."{t}" WHERE "{c}" IS NOT NULL'
            ).fetchone()[0]
            rep["ext_sources"][f"{t}.{c}"] = {"rows": cnt}
        except Exception as e:
            rep["ext_sources"][f"{t}.{c}"] = {"error": str(e)[:120]}

    # Is registry.total_rows stale? Sample
    try:
        rep["registry_with_null_rows"] = con.execute(
            f"""SELECT COUNT(*) FROM "{PUB}".manuscript_workspace.detail_table_registry_v1
                WHERE total_rows IS NULL"""
        ).fetchone()[0]
        rep["registry_with_null_feeds"] = con.execute(
            f"""SELECT COUNT(*) FROM "{PUB}".manuscript_workspace.detail_table_registry_v1
                WHERE feeds_master_columns IS NULL OR feeds_master_columns = ''"""
        ).fetchone()[0]
    except Exception as e:
        rep["registry_stale_err"] = str(e)

    (OUT / "phase0c_derivation.json").write_text(json.dumps(rep, default=str, indent=2))
    print(f"[P0C] report: {OUT / 'phase0c_derivation.json'}")
    print("[P0C] CPM column presence:")
    for k, v in rep["cpm_column_presence"].items():
        print(f"    {k:<35s}  {'YES' if v else 'NO'}")
    print(f"[P0C] recurrence counts: {rep.get('recurrence_counts')}")
    print(f"[P0C] zero-followup breakdown: {rep.get('zero_fu_breakdown')}")
    print(f"[P0C] gold_master uses scoring_py: {rep.get('gold_master_uses_scoring_py')}")
    print(f"[P0C] md_synoptic vs pub: {rep.get('md_synoptic_vs_pub')}")
    print(f"[P0C] md_fna vs pub: {rep.get('md_fna_vs_pub')}")
    print(f"[P0C] registry null_rows: {rep.get('registry_with_null_rows')}, null_feeds: {rep.get('registry_with_null_feeds')}")
    print(f"[P0C] data_dictionary_v221 columns: {rep.get('dict_v221_columns')}")
    print("[P0C] ext source row counts:")
    for k, v in rep.get("ext_sources", {}).items():
        print(f"    {k:<60s}  {v}")

    con.close()


if __name__ == "__main__":
    main()
