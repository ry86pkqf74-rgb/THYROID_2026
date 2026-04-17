#!/usr/bin/env python3
"""Phase 0b — targeted slices from live pub DB for 233 planning."""
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

    # __readme full dump
    try:
        readme = con.execute(f'SELECT * FROM "{PUB}".main."__readme"').fetchdf()
        rep["readme_columns"] = list(readme.columns)
        rep["readme"] = readme.to_dict(orient="records")
    except Exception as e:
        rep["readme_error"] = str(e)

    # detail_table_registry_v1
    try:
        reg_cols = con.execute(
            f"""SELECT column_name FROM information_schema.columns
                WHERE table_catalog = '{PUB}' AND table_schema = 'manuscript_workspace'
                AND table_name = 'detail_table_registry_v1'
                ORDER BY ordinal_position"""
        ).fetchall()
        rep["registry_columns"] = [c[0] for c in reg_cols]
        reg = con.execute(
            f'SELECT * FROM "{PUB}".manuscript_workspace.detail_table_registry_v1'
        ).fetchdf()
        rep["registry"] = reg.to_dict(orient="records")
    except Exception as e:
        rep["registry_error"] = str(e)

    # CPM column subset
    patterns = [
        "recurr", "follow", "last_contact", "death", "mortality",
        "first_surg", "surgery_date", "tg_", "tsh_", "imaging_date",
        "rai_", "nlp_", "ete_", "margin", "lvi", "multifocal",
        "r_class", "fna_path", "pmhx", "pshx", "fusion", "ret_",
    ]
    all_cpm_cols = con.execute(
        f"""SELECT column_name, data_type FROM information_schema.columns
            WHERE table_catalog = '{PUB}' AND table_schema = 'main'
            AND table_name = 'canonical_patient_master'
            ORDER BY ordinal_position"""
    ).fetchall()
    matches = {}
    for pat in patterns:
        matches[pat] = sorted([c[0] for c in all_cpm_cols if pat in c[0].lower()])
    rep["cpm_matching_cols"] = matches

    # __readme non-existent pointers and missing entries
    try:
        readme = con.execute(f'SELECT * FROM "{PUB}".main."__readme"').fetchdf()
        rd_col = [c for c in readme.columns if "table" in c.lower()][0]
        all_pub_tables = set()
        for r in con.execute(
            f"""SELECT table_name, table_schema FROM information_schema.tables
                WHERE table_catalog = '{PUB}' AND table_schema = 'main'"""
        ).fetchall():
            all_pub_tables.add(r[0])
        rep["readme_column_name"] = rd_col
        rep["readme_nonexistent"] = sorted(
            [str(t) for t in readme[rd_col].tolist() if t not in all_pub_tables]
        )
        rep["tables_missing_from_readme"] = sorted(
            [t for t in all_pub_tables if t not in set(readme[rd_col].tolist())]
        )
    except Exception as e:
        rep["readme_analysis_error"] = str(e)

    # Reference DB main schema tables (to find sources)
    try:
        rep["ref_main_tables"] = [
            r[0] for r in con.execute(
                f"""SELECT table_name FROM information_schema.tables
                    WHERE table_catalog = '{REF}' AND table_schema = 'main'
                    ORDER BY table_name"""
            ).fetchall()
        ]
    except Exception as e:
        rep["ref_main_error"] = str(e)

    # Suspect table locations
    suspects = [
        "thyroid_scoring_py_v1", "md_synoptic_tumor_long_v1",
        "md_extracted_fna_bethesda_v1", "data_dictionary_v221",
        "data_dictionary_v2", "data_dictionary_parquet_v221",
    ]
    rep["suspect_locations"] = {}
    for s in suspects:
        pub_found = con.execute(
            f"""SELECT table_schema FROM information_schema.tables
                WHERE table_catalog = '{PUB}' AND table_name = '{s}'"""
        ).fetchall()
        ref_found = con.execute(
            f"""SELECT table_schema FROM information_schema.tables
                WHERE table_catalog = '{REF}' AND table_name = '{s}'"""
        ).fetchall()
        rep["suspect_locations"][s] = {
            "in_pub": [r[0] for r in pub_found],
            "in_ref": [r[0] for r in ref_found],
        }

    # Suspicious pub tables (by naming)
    markers = ["_prev", "_bak", "_backup", "_tmp", "_legacy", "_old", "_draft"]
    suspicious = []
    for t in con.execute(
        f"""SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_catalog = '{PUB}' AND table_type = 'BASE TABLE'"""
    ).fetchall():
        name = t[1].lower()
        if any(m in name for m in markers) or name.startswith("archive_") or name.startswith("deprecated_"):
            suspicious.append({"schema": t[0], "name": t[1]})
    rep["suspicious_pub_tables"] = suspicious

    # qa_fusion_parse_triage_v1 references in views
    views = con.execute(
        f"""SELECT table_schema, table_name, view_definition
            FROM information_schema.views
            WHERE table_catalog = '{PUB}'"""
    ).fetchall()
    rep["qa_triage_refs"] = sorted(
        [f"{v[0]}.{v[1]}" for v in views if v[2] and "qa_fusion_parse_triage_v1" in v[2]]
    )

    # gold_master_patient_facts_v1 — does it exist as view in ref DB?
    try:
        rep["gold_master_view_in_ref"] = [
            r[0] for r in con.execute(
                f"""SELECT table_schema FROM information_schema.tables
                    WHERE table_catalog = '{REF}' AND table_name = 'gold_master_patient_facts_v1'"""
            ).fetchall()
        ]
    except Exception as e:
        rep["gold_master_err"] = str(e)

    # canonical_detail_pointer_v1 view definition
    try:
        rep["pointer_view_def"] = con.execute(
            f"""SELECT view_definition FROM information_schema.views
                WHERE table_catalog = '{PUB}' AND table_schema = 'manuscript_workspace'
                  AND table_name = 'canonical_detail_pointer_v1'"""
        ).fetchone()[0]
    except Exception as e:
        rep["pointer_err"] = str(e)

    (OUT / "phase0b_details.json").write_text(json.dumps(rep, default=str, indent=2))
    print(f"[P0B] report: {OUT / 'phase0b_details.json'}")
    print(f"[P0B] __readme rows: {len(rep.get('readme', []))}")
    print(f"[P0B] readme nonexistent refs: {rep.get('readme_nonexistent', [])}")
    print(f"[P0B] tables missing from __readme: {len(rep.get('tables_missing_from_readme', []))}")
    print(f"[P0B] registry rows: {len(rep.get('registry', []))}")
    print(f"[P0B] registry columns: {rep.get('registry_columns', [])}")
    print(f"[P0B] suspicious pub tables: {rep.get('suspicious_pub_tables', [])}")
    print(f"[P0B] qa_triage_refs: {rep.get('qa_triage_refs', [])}")
    print("[P0B] suspect locations:")
    for s, loc in rep.get("suspect_locations", {}).items():
        print(f"    {s}: pub={loc['in_pub']}  ref={loc['in_ref']}")

    con.close()


if __name__ == "__main__":
    main()
