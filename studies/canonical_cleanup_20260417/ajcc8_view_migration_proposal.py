"""Generate the 9 view DDLs + proposed migrated versions for ajcc8_t_stage rename.

Read-only: writes only the proposal markdown to studies/.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from _md_connect import connect_locked  # type: ignore

OUT = Path(__file__).resolve().parent / "ajcc8_t_stage_view_migration_proposal.md"


VIEWS = [
    "cohort_descriptive_full_cohort_v1",
    "cohort_m007_rss_reclassification_v1",
    "cohort_m036_ata_risk_comparison_v1",
    "cohort_m043_ln_predictors_v1",
    "cohort_m044_ajcc_ete_v1",
    "cohort_m048_tnm_multifocal_v1",
    "cohort_m050_tumor_size_volume_v1",
    "cohort_m051_ete_ln_v1",
    "cohort_m059_prognostic_scoring_v1",
]


def migrate_definition(d: str) -> str:
    """Replace bare 'ajcc8_t_stage' with 'ajcc8_t_stage_corrected'.

    Strategy: case-insensitive replace 'ajcc8_t_stage' -> 'ajcc8_t_stage_corrected'
    only where the next char is NOT '_' (which would form '_corrected' or '_v2').
    Implemented with a simple regex against the case-preserved definition.
    """
    pattern = re.compile(r"ajcc8_t_stage(?!_)", re.IGNORECASE)
    return pattern.sub("ajcc8_t_stage_corrected", d)


def main() -> int:
    con = connect_locked()
    lines = [
        "# ajcc8_t_stage view migration proposal",
        "",
        "Generated 2026-04-17 by canonical cleanup Phase 4.6 PRE-GATE.",
        "",
        "Each section shows the live view DDL and the proposed migrated DDL.",
        "Migration: bare `ajcc8_t_stage` -> `ajcc8_t_stage_corrected`.",
        "After applying these CREATE OR REPLACE VIEW statements, the rename in",
        "`scripts/274b_canonical_cleanup_phase4_6_rename.py` becomes safe.",
        "",
    ]
    for v in VIEWS:
        row = con.execute(
            "SELECT view_definition FROM information_schema.views "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='manuscript_workspace' AND table_name=?",
            [v],
        ).fetchone()
        if not row:
            lines += [f"## `manuscript_workspace.{v}` — NOT FOUND", ""]
            continue
        d = row[0]
        d_new = migrate_definition(d)
        lines += [
            f"## `manuscript_workspace.{v}`",
            "",
            "### Live definition",
            "",
            "```sql",
            d.strip(),
            "```",
            "",
            "### Proposed migrated definition",
            "",
            "```sql",
            f"CREATE OR REPLACE VIEW manuscript_workspace.{v} AS",
            d_new.strip(),
            "```",
            "",
        ]
    OUT.write_text("\n".join(lines) + "\n")
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
