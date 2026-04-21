#!/usr/bin/env python3
"""One-shot probe: capture column lists + row counts for all US/TIRADS source tables
plus detail_table_registry_v1 schema. Read-only; no writes."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

MAIN_TABLES = [
    "us_nodules_tirads",
    "ultrasound_reports",
    "tirads_v2_nodules_raw",
    "canonical_us_nodule_master_v1",
    "canonical_us_nodule_characteristics_v1",
    "imaging_nodule_master_v1",
    "extracted_tirads_validated_v1",
    "imaging_fna_linkage_v3",
    "note_entities_llm_tirads_granular",
    "note_entities_llm_us_nodule_dynamics",
    "tirads_llm_extracted_v2",
    "serial_imaging_us",
    "canonical_us_exam_master_v1",
    "canonical_us_patient_master_v1",
    "canonical_patient_master",
]

WS_TABLES = [
    "tirads_granular_parsed_v1",
    "us_nodule_dynamics_parsed_v1",
    "imaging_nodule_master_clean_v1",
    "detail_table_registry_v1",
]

ARCHIVE_TARGET = '"Thyroid 2026 UPdated"'


def probe_table(con, schema: str, table: str) -> dict:
    fq = f'{PUBLICATION_DB}.{schema}.{table}' if schema != "raw" else f"{PUBLICATION_DB}.{table}"
    info: dict = {"schema": schema, "table": table, "exists": False}
    cols = con.execute(
        "SELECT column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position",
        [PUBLICATION_DB, schema, table],
    ).fetchall()
    if not cols:
        return info
    info["exists"] = True
    info["columns"] = [(c[0], c[1]) for c in cols]
    try:
        n = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
        info["row_count"] = n
    except Exception as e:
        info["row_count_error"] = str(e)
    try:
        if any(c[0] == "research_id" for c in cols):
            np = con.execute(
                f"SELECT COUNT(DISTINCT research_id) FROM {fq}"
            ).fetchone()[0]
            info["distinct_research_id"] = np
    except Exception as e:
        info["distinct_rid_error"] = str(e)
    return info


def main() -> int:
    con = connect_locked()
    out: dict = {"main": {}, "manuscript_workspace": {}}
    for t in MAIN_TABLES:
        out["main"][t] = probe_table(con, "main", t)
    for t in WS_TABLES:
        out["manuscript_workspace"][t] = probe_table(con, "manuscript_workspace", t)

    dbs = con.execute(
        "SELECT database_name FROM duckdb_databases() ORDER BY 1"
    ).fetchall()
    out["attached_databases"] = [d[0] for d in dbs]
    schemas_in_updated = []
    try:
        rows = con.execute(
            'SELECT schema_name FROM "Thyroid 2026 UPdated".information_schema.schemata'
        ).fetchall()
        schemas_in_updated = sorted({r[0] for r in rows})
    except Exception as e:
        out["updated_db_error"] = str(e)
    out["thyroid_updated_schemas"] = schemas_in_updated

    out_path = HERE / "_us_v2_probe.json"
    out_path.write_text(json.dumps(out, indent=2, default=str))
    print(f"wrote {out_path}")
    for t, d in out["main"].items():
        print(f"main.{t}: exists={d.get('exists')} rows={d.get('row_count')} pts={d.get('distinct_research_id')}")
    for t, d in out["manuscript_workspace"].items():
        print(f"ws.{t}: exists={d.get('exists')} rows={d.get('row_count')} pts={d.get('distinct_research_id')}")
    print(f"attached dbs: {out['attached_databases']}")
    print(f"updated schemas: {schemas_in_updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
