"""Step 3a — Schema discovery for nodule->FNA->specimen->path linkage chain."""
from __future__ import annotations

import json
from collections import defaultdict
from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"

TABLES_OF_INTEREST = [
    # core nodule + imaging
    "canonical_us_nodule_v2",
    "imaging_nodule_long_v2",
    # imaging<->FNA linkage
    "imaging_fna_linkage_v3",
    "imaging_fna_linkage_v4",
    # FNA canonical
    "canonical_fna_events_v1",
    "canonical_fna_patient_rollup_v1",
    # specimen layer
    "specimen_master_v1",
    "specimen_source_xref_v1",
    "specimen_tumor_focus_v1",
    # path outcome
    "canonical_path_malignant_events_v1",
    "canonical_path_benign_events_v1",
    "canonical_diagnosis_unified_v1",
    "path_focus_link_v1",
    # nodule_id ⇆ tumor mapping (if any)
    "nodule_tumor_link_v1",
]


def main() -> None:
    client = bigquery.Client(project=PROJECT)

    # Pull union of column metadata from BOTH pub_canonical and pub_workspace.
    # Some of these tables live in workspace.
    schemas = ["pub_canonical", "pub_workspace"]
    rows: list[bigquery.Row] = []
    for schema in schemas:
        sql = f"""
        SELECT '{schema}' AS schema_name, table_name, column_name, data_type, ordinal_position
        FROM `{PROJECT}.{schema}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name IN UNNEST(@names)
        ORDER BY table_name, ordinal_position
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("names", "STRING", TABLES_OF_INTEREST)
            ]
        )
        rows.extend(client.query(sql, job_config=job_config).result())

    # Group: schema -> table -> [(col, type)]
    grouped: dict[str, dict[str, list[tuple[str, str]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for r in rows:
        grouped[r.schema_name][r.table_name].append((r.column_name, r.data_type))

    # Pretty print
    for schema, tabs in grouped.items():
        for tab, cols in tabs.items():
            print(f"\n=== {schema}.{tab} ({len(cols)} cols) ===")
            for c, t in cols:
                marker = ""
                if c.endswith("_id") or c in ("research_id",):
                    marker = " <-- id"
                if "date" in c.lower():
                    marker += " <-- date"
                print(f"  {c:50s} {t:20s}{marker}")

    # Coverage report — which target tables exist?
    found = {tab for s in grouped.values() for tab in s.keys()}
    missing = sorted(set(TABLES_OF_INTEREST) - found)
    print(f"\n--- Coverage ---")
    print(f"Found ({len(found)}): {sorted(found)}")
    print(f"Missing ({len(missing)}): {missing}")

    # Save raw to JSON for reference
    out = {
        s: {t: [{"col": c, "type": ty} for c, ty in cs] for t, cs in tabs.items()}
        for s, tabs in grouped.items()
    }
    with open("scripts/_phase_b6_step3a_schema_discovery.json", "w") as f:
        json.dump(out, f, indent=2)
    print("\nSaved: scripts/_phase_b6_step3a_schema_discovery.json")


if __name__ == "__main__":
    main()
