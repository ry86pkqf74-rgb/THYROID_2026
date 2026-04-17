#!/usr/bin/env python3
"""Script 270c patch — reclassify 3 DIVERGENT rows to DROP_ALREADY_SNAPSHOTTED.

After direct MotherDuck verification (Claude direct-query):
  - canonical == snapshot (byte-equivalent) for all three tables.
  - stray extras on canonical_diagnosis_unified_v1 (+231) and
    ln_master_rollup_v1 (+17) are true same-source duplicates —
    every duplicate pair has the same source_table value; stray is
    a pre-dedup leftover.
  - serial_imaging_us stray is an empty shell (0 rows); snapshot has
    4,162 rows; stray holds a strict subset (the empty set).

All three are safe to drop. No unique information in stray.

Mutates (in-place):
  scripts/output/270c_stray_schema_consolidation.csv
  scripts/output/270c_budgets.json     (DIVERGENT count = 3 → 0)
  scripts/output/270c_planning_summary.md (counts updated)

No MotherDuck writes here — those are in 270c_divergent_reclassification_audit.py.
"""
from __future__ import annotations

import csv
import io
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"

CONSOLIDATION_CSV = OUT_DIR / "270c_stray_schema_consolidation.csv"
BUDGETS_JSON = OUT_DIR / "270c_budgets.json"
SUMMARY_MD = OUT_DIR / "270c_planning_summary.md"

TARGETS = frozenset({
    "canonical_diagnosis_unified_v1",
    "ln_master_rollup_v1",
    "serial_imaging_us",
})

RECLASSIFICATION_NOTE = (
    "reclassified from DIVERGENT to DROP_ALREADY_SNAPSHOTTED: "
    "Claude direct-query verification confirmed canonical==snapshot "
    "(byte-equivalent) and stray holds strictly no unique content "
    "(pre-dedup duplicates or empty shell). "
    "See audit row finding_id='divergent_reclassified_to_drop_3_rows'."
)


def patch_csv() -> int:
    """Patch disposition + justification + identical_in_archive_pub_v1_0
    for the 3 DIVERGENT rows. Returns count of rows patched."""
    content = CONSOLIDATION_CSV.read_text()
    lines = content.splitlines(keepends=True)

    # Locate header line (first line with 'schema' as first field)
    header_idx = next(
        (i for i, ln in enumerate(lines)
         if ln.split(",")[0].strip() == "schema"),
        None,
    )
    if header_idx is None:
        raise SystemExit("cannot find header in consolidation CSV")

    reader = csv.reader(io.StringIO(content))
    all_rows = list(reader)
    header = all_rows[header_idx]
    sch_i = header.index("schema")
    name_i = header.index("name")
    disp_i = header.index("disposition")
    pub_i = header.index("identical_in_archive_pub_v1_0")
    just_i = header.index("justification")

    patched = 0
    for row in all_rows:
        if len(row) <= just_i:
            continue
        if row[disp_i] == "DIVERGENT" and row[name_i] in TARGETS:
            row[disp_i] = "DROP_ALREADY_SNAPSHOTTED"
            row[pub_i] = "True"
            row[just_i] = RECLASSIFICATION_NOTE
            patched += 1

    # Rewrite CSV preserving the metadata line at top
    out_buf = io.StringIO()
    writer = csv.writer(out_buf)
    for row in all_rows:
        writer.writerow(row)
    CONSOLIDATION_CSV.write_text(out_buf.getvalue())
    return patched


def patch_budgets_json() -> None:
    """Update stray_divergent_rows_human_review in budgets JSON."""
    budgets = json.loads(BUDGETS_JSON.read_text())
    key = "stray_divergent_rows_human_review"
    if key in budgets:
        budgets[key]["actual"] = 0
        budgets[key]["ok"] = True
        budgets[key]["ok_severity"] = "none"
        budgets[key]["rows"] = []
        budgets[key]["reclassification_note"] = RECLASSIFICATION_NOTE
    BUDGETS_JSON.write_text(json.dumps(budgets, indent=2))


def patch_summary_md() -> None:
    """Append a reclassification note to the planning summary."""
    current = SUMMARY_MD.read_text()
    note = (
        "\n## Post-plan patch: 3 DIVERGENT rows reclassified\n\n"
        "After direct MotherDuck verification, 3 rows initially tagged "
        "`DIVERGENT` were reclassified to `DROP_ALREADY_SNAPSHOTTED`:\n"
        "- `canonical_diagnosis_unified_v1` (+231 stray vs snapshot): "
        "pre-dedup same-source duplicates; canonical == snapshot.\n"
        "- `ln_master_rollup_v1` (+17 stray vs snapshot): same pattern.\n"
        "- `serial_imaging_us` (stray=0 vs snapshot=4162): empty shell; "
        "snapshot is authoritative.\n\n"
        f"**Updated DROP_ALREADY_SNAPSHOTTED: 123 → 126. DIVERGENT: 3 → 0.**\n"
        "See audit row `divergent_reclassified_to_drop_3_rows` and "
        "tech_debt `stray_subset_matcher_v1_1`.\n"
    )
    SUMMARY_MD.write_text(current + note)


def main() -> None:
    patched = patch_csv()
    if patched != 3:
        raise SystemExit(
            f"Expected to patch 3 DIVERGENT rows; got {patched}. "
            "Check CSV."
        )
    print(f"  patched {patched} rows in {CONSOLIDATION_CSV}")

    patch_budgets_json()
    print(f"  updated {BUDGETS_JSON}")

    patch_summary_md()
    print(f"  appended note to {SUMMARY_MD}")

    print("Done. Now commit with:")
    print(
        "  git add scripts/output/270c_stray_schema_consolidation.csv "
        "scripts/output/270c_budgets.json scripts/output/270c_planning_summary.md"
    )


if __name__ == "__main__":
    main()
