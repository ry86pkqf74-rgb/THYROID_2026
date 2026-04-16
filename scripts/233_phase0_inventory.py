#!/usr/bin/env python3
"""Phase-0 pre-flight inventory for Script 233. Read-only."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import duckdb  # noqa: E402

from motherduck_client import get_token  # noqa: E402

PUB_DB = "thyroid_canonical_publication_v1_0"
REF_DB = "Thyroid 2026 UPdated"
OUT = REPO / "scripts" / "output" / "233"
OUT.mkdir(parents=True, exist_ok=True)


def main() -> None:
    token = get_token()
    con = duckdb.connect(f"md:?motherduck_token={token}")
    con.execute(f'USE "{PUB_DB}"')
    con.execute(f'USE "{PUB_DB}".main')

    report: dict[str, object] = {}

    # Invariants
    inv = con.execute(
        f"""SELECT COUNT(*) r, COUNT(DISTINCT research_id) d,
             COUNT(*) FILTER (WHERE research_id IS NULL) nr,
             COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) nf
           FROM "{PUB_DB}".main.canonical_patient_master"""
    ).fetchone()
    report["invariants"] = dict(zip(["rows", "distinct_rid", "null_rid", "null_fna"], inv))
    print(f"[PHASE0] Invariants: {report['invariants']}")

    assert inv[0] == 10871, f"Row count broken: {inv}"
    assert inv[1] == 10871, f"Duplicate rid: {inv}"
    assert inv[2] == 0 and inv[3] == 0, f"NULLs: {inv}"

    # Publication DB table list
    tables = con.execute(
        f"""SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_catalog = '{PUB_DB}'
            ORDER BY table_schema, table_name"""
    ).fetchall()
    print(f"[PHASE0] Publication DB: {len(tables)} tables/views")
    report["pub_tables"] = [dict(zip(["schema", "name", "type"], t)) for t in tables]

    # canonical_patient_master column catalog
    cols = con.execute(
        f"""SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_catalog = '{PUB_DB}' AND table_schema = 'main'
              AND table_name = 'canonical_patient_master'
            ORDER BY ordinal_position"""
    ).fetchall()
    print(f"[PHASE0] canonical_patient_master: {len(cols)} columns")
    report["cpm_columns"] = [{"name": c[0], "type": c[1]} for c in cols]

    # __readme
    try:
        readme = con.execute(
            f'SELECT * FROM "{PUB_DB}".main."__readme"'
        ).fetchdf()
        report["readme_rows"] = readme.to_dict(orient="records")
        print(f"[PHASE0] __readme: {len(readme)} rows")
    except Exception as e:
        report["readme_error"] = str(e)
        print(f"[PHASE0] __readme not found: {e}")

    # detail_table_registry
    try:
        reg = con.execute(
            f'SELECT * FROM "{PUB_DB}".manuscript_workspace.detail_table_registry_v1'
        ).fetchdf()
        report["registry_rows"] = reg.to_dict(orient="records")
        print(f"[PHASE0] detail_table_registry_v1: {len(reg)} rows")
    except Exception as e:
        report["registry_error"] = str(e)
        print(f"[PHASE0] registry not found: {e}")

    # Issue baselines
    q = con.execute(
        f"""SELECT
              COUNT(*) FILTER (WHERE any_recurrence_flag = TRUE
                               AND recurrence_definition = 'no_recurrence_evidence') AS phantom_recur,
              COUNT(*) FILTER (WHERE time_to_recurrence_days < 0) AS neg_t2r,
              COUNT(*) FILTER (WHERE recurrence_days_from_surg < 0) AS neg_rds,
              COUNT(*) FILTER (WHERE COALESCE(followup_days, 0) = 0) AS zero_fu,
              COUNT(*) FILTER (WHERE first_surgery_date IS NULL) AS null_surg,
              COUNT(*) FILTER (WHERE recurrence_site IS NULL
                               AND recurrence_site_text IS NOT NULL) AS issue4_residual
           FROM "{PUB_DB}".main.canonical_patient_master"""
    ).fetchone()
    report["issue_baselines"] = dict(
        zip(
            ["phantom_recur", "neg_t2r", "neg_rds", "zero_fu", "null_surg", "issue4_residual"],
            q,
        )
    )
    print(f"[PHASE0] Issue baselines: {report['issue_baselines']}")

    # mortality_type present?
    has_mortality = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog = '{PUB_DB}' AND table_schema = 'main'
              AND table_name = 'canonical_patient_master'
              AND column_name = 'mortality_type'"""
    ).fetchone()[0]
    report["mortality_type_exists"] = bool(has_mortality)

    # Reference DB schemas (read-only)
    try:
        ref_schemas = con.execute(
            f"""SELECT DISTINCT table_schema FROM information_schema.tables
                WHERE table_catalog = '{REF_DB}'"""
        ).fetchall()
        report["ref_schemas"] = [s[0] for s in ref_schemas]
        print(f"[PHASE0] Reference DB schemas: {report['ref_schemas']}")
    except Exception as e:
        report["ref_error"] = str(e)

    # Does archive_pub_v1_0 already exist in ref db?
    try:
        apv = con.execute(
            f"""SELECT table_name FROM information_schema.tables
                WHERE table_catalog = '{REF_DB}' AND table_schema = 'archive_pub_v1_0'"""
        ).fetchall()
        report["archive_pub_v1_0_exists"] = True
        report["archive_pub_v1_0_tables"] = [t[0] for t in apv]
    except Exception as e:
        report["archive_pub_v1_0_exists"] = False
        report["archive_pub_v1_0_error"] = str(e)

    (OUT / "phase0_inventory.json").write_text(
        json.dumps(report, default=str, indent=2)
    )
    print(f"[PHASE0] Report written to {OUT / 'phase0_inventory.json'}")
    con.close()


if __name__ == "__main__":
    main()
