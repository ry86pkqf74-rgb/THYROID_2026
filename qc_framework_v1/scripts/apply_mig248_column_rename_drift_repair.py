#!/usr/bin/env python3
"""mig_248: repair column-rename drift in manuscript cohort views.

This script scans manuscript_workspace views one-by-one, repairs only verified
canonical_patient_master column-renames, and writes the required migration and
memory closeout artifacts.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # noqa: E402

RUN_ID = "mig_248_column_rename_drift_repair_20260501"
MIGRATION_PATH = REPO / "qc_framework_v1" / "migrations" / "248_column_rename_drift_repair_20260501.sql"
MEMORY_PATH = REPO / "memory" / "project_mig_248_column_rename_drift_repair_20260501.md"
EXPORT_DIR = REPO / "exports" / RUN_ID

RENAMES: dict[str, dict[str, str]] = {
    "syn_right_lobe_size_cm": {
        "replacement": "syn_right_lobe_size_cm_legacy_raw",
        "source": "mig_173_syn_size_cm_dtype_reform_20260429",
        "treatment": "legacy_raw_preserves_original VARCHAR 3-axis manuscript-view semantics; typed axis/volume siblings exist for numeric analysis.",
    },
    "syn_left_lobe_size_cm": {
        "replacement": "syn_left_lobe_size_cm_legacy_raw",
        "source": "mig_173_syn_size_cm_dtype_reform_20260429",
        "treatment": "legacy_raw_preserves_original VARCHAR 3-axis manuscript-view semantics; typed axis/volume siblings exist for numeric analysis.",
    },
    "syn_isthmus_size_cm": {
        "replacement": "syn_isthmus_size_cm_legacy_raw",
        "source": "mig_173_syn_size_cm_dtype_reform_20260429",
        "treatment": "legacy_raw_preserves_original VARCHAR 3-axis manuscript-view semantics; typed axis/volume siblings exist for numeric analysis.",
    },
}


@dataclass
class ScanResult:
    schema_name: str
    view_name: str
    view_group: str
    status: str
    row_count: int | None = None
    error: str = ""


@dataclass
class Repair:
    schema_name: str
    view_name: str
    old_column: str
    new_column: str
    ddl: str
    row_count_after: int | None = None


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def fetch_views(con) -> tuple[list[str], list[str], list[str]]:
    cohort_views = [
        row[0]
        for row in con.execute(
            """
            SELECT table_name FROM information_schema.views
            WHERE table_schema='manuscript_workspace' AND table_name LIKE 'cohort_m0%'
            ORDER BY table_name
            """
        ).fetchall()
    ]
    other_views = [
        row[0]
        for row in con.execute(
            """
            SELECT table_name FROM information_schema.views
            WHERE table_schema='manuscript_workspace' AND table_name NOT LIKE 'cohort_m0%'
            ORDER BY table_name
            """
        ).fetchall()
    ]
    dive_views = [
        row[0]
        for row in con.execute(
            """
            SELECT DISTINCT cohort_view_name
            FROM manuscript_workspace.manuscript_dive_map_v1
            WHERE cohort_view_name IS NOT NULL
            ORDER BY cohort_view_name
            """
        ).fetchall()
    ]
    return cohort_views, other_views, dive_views


def count_view(con, schema_name: str, view_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {qident(schema_name)}.{qident(view_name)}").fetchone()[0])


def scan_views(con, schema_name: str, view_names: list[str], view_group: str) -> list[ScanResult]:
    results: list[ScanResult] = []
    for view_name in view_names:
        try:
            results.append(
                ScanResult(schema_name, view_name, view_group, "ok", row_count=count_view(con, schema_name, view_name))
            )
        except Exception as exc:  # noqa: BLE001 - scanner must retain raw DB errors.
            results.append(
                ScanResult(schema_name, view_name, view_group, "error", error=str(exc).replace("\n", " | "))
            )
    return results


def fetch_view_definition(con, schema_name: str, view_name: str) -> str:
    row = con.execute(
        """
        SELECT view_definition
        FROM information_schema.views
        WHERE table_schema=? AND table_name=?
        """,
        [schema_name, view_name],
    ).fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"No view_definition found for {schema_name}.{view_name}")
    return str(row[0])


def to_create_or_replace(ddl: str) -> str:
    fixed = re.sub(r"^\s*CREATE\s+VIEW\s+", "CREATE OR REPLACE VIEW ", ddl, flags=re.IGNORECASE)
    if fixed == ddl:
        raise RuntimeError("view_definition did not start with CREATE VIEW")
    return fixed.rstrip().rstrip(";") + ";"


def build_repairs(con, scan_results: list[ScanResult]) -> tuple[list[Repair], list[ScanResult]]:
    repairs: list[Repair] = []
    unresolved: list[ScanResult] = []
    for result in scan_results:
        if result.status != "error":
            continue
        ddl = fetch_view_definition(con, result.schema_name, result.view_name)
        new_ddl = ddl
        matched: list[tuple[str, str]] = []
        for old_col, meta in RENAMES.items():
            if re.search(rf"\b{re.escape(old_col)}\b", new_ddl):
                new_col = meta["replacement"]
                new_ddl = re.sub(rf"\b{re.escape(old_col)}\b", new_col, new_ddl)
                matched.append((old_col, new_col))
        if not matched:
            unresolved.append(result)
            continue
        create_or_replace = to_create_or_replace(new_ddl)
        for old_col, new_col in matched:
            repairs.append(Repair(result.schema_name, result.view_name, old_col, new_col, create_or_replace))
    return repairs, unresolved


def repair_sort_key(repair: Repair) -> tuple[int, str]:
    # Rebuild the common descriptive base view before thin wrappers that select
    # from it; otherwise CREATE OR REPLACE can validate against stale columns.
    if repair.view_name == "cohort_descriptive_full_cohort_v1":
        return (0, repair.view_name)
    return (1, repair.view_name)


def unique_view_repairs(repairs: list[Repair]) -> list[Repair]:
    out: list[Repair] = []
    seen: set[tuple[str, str]] = set()
    for repair in sorted(repairs, key=repair_sort_key):
        key = (repair.schema_name, repair.view_name)
        if key in seen:
            continue
        seen.add(key)
        out.append(repair)
    return out


def write_scan_csv(scan_results: list[ScanResult]) -> Path:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = EXPORT_DIR / "view_queryability_scan.csv"
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["schema_name", "view_name", "view_group", "status", "row_count", "error"],
        )
        writer.writeheader()
        for result in scan_results:
            writer.writerow(result.__dict__)
    return path


def write_migration(
    *,
    repairs: list[Repair],
    unresolved: list[ScanResult],
    cohort_views: list[str],
    dive_views: list[str],
) -> None:
    lines: list[str] = []
    lines.extend(
        [
            "-- =============================================================================",
            "-- mig_248 — Column-rename drift repair across manuscript cohort views",
            "-- Date:    2026-05-01",
            "-- Lane:    mig_248",
            "-- =============================================================================",
            "--",
            "-- Scope:",
            "--   Repairs manuscript_workspace views that failed at query time because their",
            "--   view bodies referenced canonical_patient_master columns renamed by mig_173.",
            "--   This is view-DDL only: no canonical base-table, registry, or signoff writes.",
            "--",
            "-- Rename treatment:",
        ]
    )
    for old_col, meta in RENAMES.items():
        lines.append(f"--   * {old_col} -> {meta['replacement']} ({meta['source']}); {meta['treatment']}")
    lines.extend(["--", "-- =============================================================================", ""])

    if repairs:
        seen: set[tuple[str, str]] = set()
        for repair in sorted(repairs, key=repair_sort_key):
            key = (repair.schema_name, repair.view_name)
            if key in seen:
                continue
            seen.add(key)
            related = [r for r in repairs if (r.schema_name, r.view_name) == key]
            lines.append(f"-- Repair: {repair.schema_name}.{repair.view_name}")
            for item in related:
                lines.append(f"--   {item.old_column} -> {item.new_column}")
            lines.append(related[0].ddl)
            lines.append("")
    else:
        lines.append("-- No in-scope repair DDL was generated by the live scan.")
        lines.append("")

    lines.extend(
        [
            "-- =============================================================================",
            "-- Post-repair cohort-size table (run after all cohort_m0% views are queryable)",
            "-- =============================================================================",
            "",
            "CREATE OR REPLACE TABLE manuscript_workspace.dive_cohort_size_v1 AS",
        ]
    )
    branches = [
        f"SELECT '{view}' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at\nFROM manuscript_workspace.{view}"
        for view in dive_views
    ]
    lines.append("\nUNION ALL\n".join(branches) + ";")
    lines.append("")
    lines.extend(
        [
            "-- =============================================================================",
            "-- Verification",
            "-- =============================================================================",
            "-- 1) Previously broken in-scope repaired views:",
        ]
    )
    if repairs:
        seen = []
        for repair in sorted(repairs, key=repair_sort_key):
            view = f"{repair.schema_name}.{repair.view_name}"
            if view not in seen:
                seen.append(view)
        lines.append("-- " + "\n-- UNION ALL\n-- ".join(
            [f"SELECT '{view}' AS view_name, COUNT(*) AS row_count FROM {view}" for view in seen]
        ) + ";")
    else:
        lines.append("-- No repaired views to count.")
    lines.extend(
        [
            "-- 2) dive_cohort_size_v1 row count should match distinct non-null cohort_view_name count in manuscript_dive_map_v1.",
            "-- 3) semantic_publication.vw_publication_qc_status_VIEW_v1 gate1 should remain unchanged by this manuscript_workspace-only lane.",
        ]
    )
    if unresolved:
        lines.extend(["", "-- Out-of-scope unresolved manuscript_workspace failures observed during scan:"])
        for result in unresolved:
            lines.append(f"--   {result.schema_name}.{result.view_name}: {result.error}")
    MIGRATION_PATH.write_text("\n".join(lines) + "\n")


def write_memory(
    *,
    pre_qc: str,
    post_qc: str | None,
    cohort_results: list[ScanResult],
    other_results: list[ScanResult],
    repairs: list[Repair],
    unresolved: list[ScanResult],
    dive_table_rows: int | None,
    scan_csv: Path,
) -> None:
    def counts(results: list[ScanResult]) -> tuple[int, int]:
        return sum(r.status == "ok" for r in results), sum(r.status == "error" for r in results)

    cohort_ok, cohort_err = counts(cohort_results)
    other_ok, other_err = counts(other_results)
    lines = [
        "# mig_248 Closeout — Column-Rename Drift Repair",
        "",
        f"**Date:** {datetime.now(UTC).date().isoformat()}",
        f"**Batch ID:** `{RUN_ID}`",
        f"**Migration file:** `qc_framework_v1/migrations/248_column_rename_drift_repair_20260501.sql`",
        f"**Scan artifact:** `exports/{RUN_ID}/view_queryability_scan.csv`",
        "",
        "## Preflight QC",
        "",
        "```text",
        pre_qc.strip(),
        "```",
        "",
        "## Per-view scan summary",
        "",
        f"- Cohort views scanned: {len(cohort_results)} ({cohort_ok} OK, {cohort_err} error).",
        f"- Adjacent manuscript_workspace views scanned: {len(other_results)} ({other_ok} OK, {other_err} error).",
        f"- Scan CSV: `{scan_csv.relative_to(REPO)}`.",
        "",
        "## Repairs applied",
        "",
    ]
    if repairs:
        seen: set[tuple[str, str, str, str]] = set()
        for repair in repairs:
            key = (repair.schema_name, repair.view_name, repair.old_column, repair.new_column)
            if key in seen:
                continue
            seen.add(key)
            lines.append(
                f"- `{repair.schema_name}.{repair.view_name}`: `{repair.old_column}` -> `{repair.new_column}`; "
                f"post-repair rows = {repair.row_count_after}. Semantic treatment: preserve original raw VARCHAR view semantics via mig_173 `_legacy_raw`; typed axis/volume columns remain available for numeric analysis."
            )
    else:
        lines.append("- None.")
    lines.extend(["", "## Unresolved / out-of-scope failures", ""])
    if unresolved:
        for result in unresolved:
            lines.append(f"- `{result.schema_name}.{result.view_name}`: {result.error}")
    else:
        lines.append("- None requiring Logan review for column-rename drift.")
    lines.extend(["", "## dive_cohort_size_v1", ""])
    if dive_table_rows is None:
        lines.append("- Not built in this run.")
    else:
        lines.append(f"- Built successfully with {dive_table_rows} rows.")
    if post_qc is not None:
        lines.extend(["", "## Post-apply QC", "", "```text", post_qc.strip(), "```"])
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This lane is manuscript_workspace view-DDL only; it does not update `canonical_patient_master`, canonical registry tables, or signoff tables.",
            "- `gate1` is expected to remain unchanged because these cohort views are not registered publication objects.",
            "- No PHI-bearing notes/entity text was queried; the scan used `information_schema` and `COUNT(*)` only.",
        ]
    )
    MEMORY_PATH.write_text("\n".join(lines) + "\n")


def qc_status_text(con) -> str:
    return con.execute("SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1").fetchdf().to_string(index=False)


def build_dive_cohort_size(con, dive_views: list[str]) -> int:
    branches = [
        f"SELECT '{view}' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at FROM manuscript_workspace.{view}"
        for view in dive_views
    ]
    con.execute("CREATE OR REPLACE TABLE manuscript_workspace.dive_cohort_size_v1 AS\n" + "\nUNION ALL\n".join(branches))
    return int(con.execute("SELECT COUNT(*) FROM manuscript_workspace.dive_cohort_size_v1").fetchone()[0])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Apply generated CREATE OR REPLACE VIEW and dive_cohort_size_v1 DDL to MotherDuck.")
    args = parser.parse_args()

    con = connect_locked()
    pre_qc = qc_status_text(con)
    cohort_views, other_views, dive_views = fetch_views(con)
    cohort_results = scan_views(con, "manuscript_workspace", cohort_views, "cohort_m0")
    other_results = scan_views(con, "manuscript_workspace", other_views, "adjacent")
    all_results = cohort_results + other_results
    scan_csv = write_scan_csv(all_results)
    repairs, unresolved = build_repairs(con, all_results)

    dive_table_rows: int | None = None
    post_qc: str | None = None
    if args.apply:
        for repair in unique_view_repairs(repairs):
            key = (repair.schema_name, repair.view_name)
            con.execute(repair.ddl)
            row_count = count_view(con, repair.schema_name, repair.view_name)
            for item in repairs:
                if (item.schema_name, item.view_name) == key:
                    item.row_count_after = row_count

        # Re-scan cohort views before building the live cohort-size table.
        cohort_results = scan_views(con, "manuscript_workspace", cohort_views, "cohort_m0_post_apply")
        other_results = scan_views(con, "manuscript_workspace", other_views, "adjacent_post_apply")
        unresolved = [r for r in cohort_results + other_results if r.status == "error"]
        cohort_failures = [r for r in cohort_results if r.status == "error"]
        if cohort_failures:
            print("Cohort failures remain after repairs; not building dive_cohort_size_v1.")
            for failure in cohort_failures:
                print(f"ERROR {failure.view_name}: {failure.error}")
        else:
            dive_table_rows = build_dive_cohort_size(con, dive_views)
        post_qc = qc_status_text(con)

    write_migration(repairs=repairs, unresolved=unresolved, cohort_views=cohort_views, dive_views=dive_views)
    write_memory(
        pre_qc=pre_qc,
        post_qc=post_qc,
        cohort_results=cohort_results,
        other_results=other_results,
        repairs=repairs,
        unresolved=unresolved,
        dive_table_rows=dive_table_rows,
        scan_csv=scan_csv,
    )

    print(f"cohort_views={len(cohort_views)} other_views={len(other_views)} dive_views={len(dive_views)}")
    print(f"cohort_errors={sum(r.status == 'error' for r in cohort_results)} other_errors={sum(r.status == 'error' for r in other_results)}")
    print(f"repairs={len({(r.schema_name, r.view_name) for r in repairs})} unresolved={len(unresolved)}")
    for repair in repairs:
        print(f"REPAIR {repair.schema_name}.{repair.view_name}: {repair.old_column}->{repair.new_column} rows_after={repair.row_count_after}")
    for result in unresolved:
        print(f"UNRESOLVED {result.schema_name}.{result.view_name}: {result.error}")
    if dive_table_rows is not None:
        print(f"dive_cohort_size_v1_rows={dive_table_rows}")
    print(f"wrote {MIGRATION_PATH.relative_to(REPO)}")
    print(f"wrote {MEMORY_PATH.relative_to(REPO)}")
    print(f"wrote {scan_csv.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())