"""Step 9 - Rename legacy molecular tables with _DEPRECATED_20260421 suffix.

Note: views may depend on some legacy tables. ALTER TABLE ... RENAME breaks
those views. We therefore drop dependent views first (with a manifest written
to the archive), perform the renames, and report any failures.
"""
from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, REPO_ROOT)

import duckdb
from motherduck_client import get_token  # type: ignore

ARCHIVE_DIR = "/Users/ros/THyroid 2026/archive/molecular_legacy_20260421"
SUFFIX = "_DEPRECATED_20260421"

LEGACY = [
    "genetics_per_patient_master_v1",
    "molecular_results",
    "thyroseq_molecular_enrichment",
    "molecular_testing",
    "molecular_test_episode_v2",
    "analysis_molecular_subset_v1",
    "genetics_per_test_master_v1",
    "note_entities_genetics",
    "molecular_variant_long",
    "canonical_molecular_tested_v1",
    "molecular_code_crosswalk",
    "molecular_assay_dictionary",
    "molecular_ingestion_runs",
]


def find_dependent_views(con: duckdb.DuckDBPyConnection, tables: list[str]) -> list[tuple[str, str]]:
    """Return (view_name, view_definition) tuples that reference any legacy table.

    DuckDB / MotherDuck information_schema does not have a native dependency
    graph, so we substring-match the SQL definition.
    """
    df = con.execute(
        """
        SELECT table_name AS view_name, view_definition
        FROM information_schema.views
        WHERE table_schema = current_schema()
        """
    ).fetchdf()
    out: list[tuple[str, str]] = []
    for _, row in df.iterrows():
        defn = row["view_definition"] or ""
        for t in tables:
            if t in defn:
                out.append((row["view_name"], defn))
                break
    return out


def main() -> None:
    os.environ["motherduck_token"] = get_token() or ""
    con = duckdb.connect("md:thyroid_canonical_publication_v1_0")

    deps = find_dependent_views(con, LEGACY)
    print(f"Dependent views to drop and recapture ({len(deps)}):")
    for v, _ in deps:
        print(f"  - {v}")

    dep_manifest_path = os.path.join(ARCHIVE_DIR, "DROPPED_VIEWS.json")
    with open(dep_manifest_path, "w") as f:
        json.dump([{"view": v, "definition": d} for v, d in deps], f, indent=2)
    print(f"\nview definitions saved -> {dep_manifest_path}")

    print("\nDropping dependent views ...")
    for v, _ in deps:
        try:
            con.execute(f'DROP VIEW IF EXISTS "{v}"')
            print(f"  dropped {v}")
        except Exception as e:
            print(f"  ERROR dropping {v}: {e}")

    print("\nRenaming legacy tables ...")
    rename_results = []
    for t in LEGACY:
        new_name = f"{t}{SUFFIX}"
        try:
            con.execute(f'ALTER TABLE "{t}" RENAME TO "{new_name}"')
            n = con.execute(f'SELECT COUNT(*) FROM "{new_name}"').fetchone()[0]
            print(f"  {t:40s} -> {new_name}  rows={n:,}")
            rename_results.append({"original": t, "renamed_to": new_name, "rows": int(n), "status": "ok"})
        except Exception as e:
            print(f"  ERROR renaming {t}: {e}")
            rename_results.append({"original": t, "renamed_to": new_name, "status": "error", "error": str(e)})

    rename_manifest_path = os.path.join(ARCHIVE_DIR, "RENAMES.json")
    with open(rename_manifest_path, "w") as f:
        json.dump(rename_results, f, indent=2)
    print(f"\nrename manifest -> {rename_manifest_path}")

    print("\n--- final state: new artefacts present? ---")
    for t in [
        "molecular_genetics_test_v2",
        "molecular_genetics_from_notes_v2",
        "molecular_variant_flat_v2",
        "molecular_fusion_flat_v2",
    ]:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t:40s} rows={n:,}")
        except Exception as e:
            print(f"  {t:40s} ERROR: {e}")


if __name__ == "__main__":
    main()
