"""Re-wire the 9 cohort views to bare `ajcc8_t_stage` after the Phase 4.6
RENAME made `ajcc8_t_stage_corrected` disappear (renamed to `ajcc8_t_stage`).

Prior to the rename, script 278 explicitly migrated the views to reference
`ajcc8_t_stage_corrected`; after the rename that column was renamed to
`ajcc8_t_stage`, leaving the views broken. The intended end-state is bare
`ajcc8_t_stage` referencing the (now corrected) column.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

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

_CREATE_VIEW_PREFIX = re.compile(
    r"^\s*CREATE\s+(OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?VIEW\s+"
    r"(?:[\w\".]+)\s+AS\s+",
    re.IGNORECASE | re.DOTALL,
)


def strip_create_prefix(d: str) -> str:
    m = _CREATE_VIEW_PREFIX.match(d)
    return d[m.end():] if m else d


def main() -> int:
    con = connect_locked()
    for v in VIEWS:
        row = con.execute(
            "SELECT view_definition FROM information_schema.views "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='manuscript_workspace' AND table_name=?",
            [v],
        ).fetchone()
        if not row:
            raise SystemExit(f"view not found: {v}")
        body = strip_create_prefix(row[0])
        # Replace any occurrence of ajcc8_t_stage_corrected with bare ajcc8_t_stage.
        new_body = re.sub(
            r"ajcc8_t_stage_corrected", "ajcc8_t_stage", body, flags=re.IGNORECASE
        )
        if new_body == body:
            print(f"[{v}] no _corrected refs; skipping")
            continue
        n = len(re.findall(r"ajcc8_t_stage_corrected", body, flags=re.IGNORECASE))
        print(f"[{v}] reverting {n} _corrected ref(s) -> bare ajcc8_t_stage")
        con.execute(
            f'CREATE OR REPLACE VIEW manuscript_workspace."{v}" AS {new_body}'
        )
        # Sanity probe
        cnt = con.execute(f'SELECT COUNT(*) FROM manuscript_workspace."{v}"').fetchone()[0]
        print(f"[{v}] OK rows={cnt}")
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
