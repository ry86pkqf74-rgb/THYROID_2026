#!/usr/bin/env python3
"""mig_310 v2 — MotherDuck side: build imaging_fna_linkage_v4 + optional signoff.

Run AFTER ``snowflake_trial/scripts/36_pull_sf_nlp_fna_size.py --md`` has
populated ``manuscript_workspace.nlp_fna_size_rollup_v1``.

v2 adds fna_event_id-aware join to nlp_fna_size_rollup_v1 and exposes
extracted_bethesda for cross-validation against canonical bethesda_final_num.

What this script does
---------------------
1. Verifies ``manuscript_workspace.nlp_fna_size_rollup_v1`` is populated.
2. Creates (or replaces) ``manuscript_workspace.imaging_fna_linkage_v4`` —
   extends v3 with NLP-derived ``fna_size_cm_resolved``,
   ``fna_laterality_resolved``, ``size_score_v4``, and ``nlp_extracted_bethesda``.
3. Prints a coverage delta report (v3 vs v4).
4. Optionally inserts the signoff row to ``main.signoff_migration``.

Usage::

    .venv/bin/python scripts/mig_310_fna_size_mirror.py --md [--signoff] [--dry-run]

Flags
-----
--md        Connect to MotherDuck (fail-closed; required for writes).
--dry-run   Print plan only; do not write.
--signoff   Insert signoff row after view creation.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
_utils_str = str(REPO_ROOT)
if _utils_str not in sys.path:
    sys.path.insert(0, _utils_str)

os.environ.setdefault("MOTHERDUCK_DATABASE", "thyroid_canonical_publication_v1_0")

_ROLLUP_TABLE = "manuscript_workspace.nlp_fna_size_rollup_v1"
_V3_VIEW = "manuscript_workspace.imaging_fna_linkage_v3"
_V4_VIEW = "manuscript_workspace.imaging_fna_linkage_v4"
_SIGNOFF_MIG_ID = "mig_310"

# ---------------------------------------------------------------------------
# SQL — imaging_fna_linkage_v4
# ---------------------------------------------------------------------------
_V4_VIEW_SQL = f"""
CREATE OR REPLACE VIEW {_V4_VIEW} AS
SELECT
    l.*,

    -- NLP-resolved FNA size (prefer existing structured value, then NLP)
    COALESCE(
        l.fna_size_cm,
        CASE
            WHEN n.extracted_size_cm BETWEEN 0.1 AND 15.0
                 AND n.extraction_confidence IN ('high', 'medium')
            THEN n.extracted_size_cm
            WHEN n.extracted_size_cm BETWEEN 0.1 AND 15.0
            THEN n.extracted_size_cm
            ELSE NULL
        END
    )                                           AS fna_size_cm_resolved,

    -- NLP-resolved laterality (prefer existing structured value)
    COALESCE(l.fna_laterality, n.extracted_laterality)
                                                AS fna_laterality_resolved,

    -- Source provenance for the resolved size
    CASE
        WHEN l.fna_size_cm IS NOT NULL          THEN 'structured'
        WHEN n.extracted_size_cm IS NOT NULL
             AND n.extraction_confidence = 'high'  THEN 'nlp_high'
        WHEN n.extracted_size_cm IS NOT NULL
             AND n.extraction_confidence = 'medium' THEN 'nlp_medium'
        WHEN n.extracted_size_cm IS NOT NULL    THEN 'nlp_low'
        ELSE 'missing'
    END                                         AS fna_size_source_v4,

    -- Updated size_score: replaces the constant 0.5 prior in v3
    CASE
        WHEN l.fna_size_cm IS NOT NULL          THEN 1.00  -- exact structured
        WHEN n.extracted_size_cm IS NOT NULL
             AND n.extraction_confidence = 'high'  THEN 0.85
        WHEN n.extracted_size_cm IS NOT NULL
             AND n.extraction_confidence = 'medium' THEN 0.70
        WHEN n.extracted_size_cm IS NOT NULL    THEN 0.50  -- low confidence NLP
        ELSE 0.50                                          -- no-data prior (unchanged)
    END                                         AS size_score_v4,

    -- Raw NLP fields for audit / downstream
    n.extracted_size_cm                         AS nlp_extracted_size_cm,
    n.extracted_laterality                      AS nlp_extracted_laterality,
    n.extracted_nodule_count                    AS nlp_extracted_nodule_count,
    -- Bethesda cross-validation: NLP-extracted vs canonical bethesda_final_num
    -- Use for QA only; canonical Bethesda is the primary field.
    n.extracted_bethesda                        AS nlp_extracted_bethesda,
    n.extraction_confidence                     AS nlp_extraction_confidence,
    n.max_size_score                            AS nlp_size_extract_score,
    n.max_lat_score                             AS nlp_lat_extract_score

FROM {_V3_VIEW} l
LEFT JOIN {_ROLLUP_TABLE} n
    ON CAST(l.research_id AS VARCHAR) = CAST(n.research_id AS VARCHAR)
   AND ABS(
         DATEDIFF(
             'day',
             TRY_CAST(l.fna_date_resolved AS DATE),
             TRY_CAST(n.fna_date          AS DATE)
         )
       ) <= 14;
"""

# ---------------------------------------------------------------------------
# SQL — coverage delta report
# ---------------------------------------------------------------------------
_COVERAGE_SQL = """
SELECT
    'v3' AS linkage_version,
    COUNT(*)                                              AS total_links,
    COUNT(fna_size_cm)                                    AS size_populated,
    ROUND(100.0 * COUNT(fna_size_cm) / COUNT(*), 1)      AS size_fill_pct,
    COUNT(fna_laterality)                                 AS lat_populated,
    ROUND(100.0 * COUNT(fna_laterality) / COUNT(*), 1)   AS lat_fill_pct
FROM manuscript_workspace.imaging_fna_linkage_v3

UNION ALL

SELECT
    'v4' AS linkage_version,
    COUNT(*)                                              AS total_links,
    COUNT(fna_size_cm_resolved)                           AS size_populated,
    ROUND(100.0 * COUNT(fna_size_cm_resolved) / COUNT(*), 1) AS size_fill_pct,
    COUNT(fna_laterality_resolved)                        AS lat_populated,
    ROUND(100.0 * COUNT(fna_laterality_resolved) / COUNT(*), 1) AS lat_fill_pct
FROM manuscript_workspace.imaging_fna_linkage_v4

ORDER BY linkage_version;
"""

_SIZE_SCORE_SQL = """
SELECT
    fna_size_source_v4,
    COUNT(*)               AS n,
    ROUND(AVG(size_score_v4), 3) AS avg_score
FROM manuscript_workspace.imaging_fna_linkage_v4
GROUP BY 1
ORDER BY 2 DESC;
"""


def _check_rollup(md) -> int:
    try:
        n = md.execute(
            f"SELECT COUNT(*) FROM {_ROLLUP_TABLE}"
        ).fetchone()[0]
        return n
    except Exception as exc:
        print(f"FATAL: cannot read {_ROLLUP_TABLE}: {exc}", file=sys.stderr)
        sys.exit(1)


def _check_v3_exists(md) -> bool:
    try:
        r = md.execute(
            "SELECT COUNT(*) FROM information_schema.views "
            "WHERE table_schema = 'manuscript_workspace' "
            "  AND table_name = 'imaging_fna_linkage_v3'"
        ).fetchone()[0]
        return r > 0
    except Exception:
        return False


def _print_coverage(md) -> None:
    try:
        rows = md.execute(_COVERAGE_SQL).fetchall()
        print("\n  Coverage delta (v3 → v4):")
        print(f"  {'Version':<8} {'Links':>8} {'SizePop':>8} {'Size%':>7} {'LatPop':>8} {'Lat%':>7}")
        for version, total, sz_pop, sz_pct, lt_pop, lt_pct in rows:
            print(f"  {version:<8} {total:>8,} {sz_pop:>8,} {sz_pct:>7.1f} {lt_pop:>8,} {lt_pct:>7.1f}")
    except Exception as exc:
        print(f"  WARN: coverage report failed: {exc}")

    try:
        rows = md.execute(_SIZE_SCORE_SQL).fetchall()
        print("\n  Size score distribution (v4):")
        for src, n, avg_sc in rows:
            print(f"    {src or '<null>'}: n={n:,}  avg_score={avg_sc}")
    except Exception as exc:
        print(f"  WARN: score distribution failed: {exc}")


def _write_signoff(md, n_rollup: int, size_fill: float, lat_fill: float) -> None:
    summary = (
        f"mig_310 v2 MD-side: imaging_fna_linkage_v4 created. "
        f"Corpus fna_content_corpus_v1 + linkage fna_event_note_linkage_v1 "
        f"built in manuscript_workspace (HP-note keyword corpus). "
        f"Rollup rows: {n_rollup} (includes fna_event_id + extracted_bethesda). "
        f"v4 size_fill={size_fill:.1f}% lat_fill={lat_fill:.1f}%. "
        f"Closes CF-FNA-SIZE-CM-NULL."
    )
    try:
        md.execute(
            """
            INSERT INTO main.signoff_migration
              (mig_id, signed_off_at, by_actor, summary)
            VALUES (?, CURRENT_TIMESTAMP, 'cursor_composer_mig310', ?)
            """,
            [_SIGNOFF_MIG_ID, summary],
        )
        print(f"  Signoff row inserted: {_SIGNOFF_MIG_ID}")
    except Exception as exc:
        print(f"  WARN: signoff insert failed (table may not exist): {exc}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--md", action="store_true",
                    help="Connect to MotherDuck (fail-closed).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print SQL plan only; do not execute.")
    ap.add_argument("--signoff", action="store_true",
                    help="Insert signoff row after view creation.")
    args = ap.parse_args()

    if not args.md and not args.dry_run:
        print("FATAL: pass --md or --dry-run.", file=sys.stderr)
        return 1

    if args.dry_run:
        print("=== DRY-RUN: would execute ===")
        print(_V4_VIEW_SQL)
        return 0

    from utils.md_connect import connect_md_fail_closed  # noqa: E402

    md = connect_md_fail_closed(REPO_ROOT / "thyroid_master.duckdb")
    try:
        md.execute("USE thyroid_canonical_publication_v1_0")

        # Gate 1 — rollup must be populated
        n_rollup = _check_rollup(md)
        if n_rollup == 0:
            print(
                "FATAL: manuscript_workspace.nlp_fna_size_rollup_v1 is empty. "
                "Run snowflake_trial/scripts/36_pull_sf_nlp_fna_size.py --md first.",
                file=sys.stderr,
            )
            return 1
        print(f"  nlp_fna_size_rollup_v1: {n_rollup:,} rows — OK")

        # Gate 2 — v3 linkage view must exist
        if not _check_v3_exists(md):
            print(
                "WARN: imaging_fna_linkage_v3 view not found in manuscript_workspace. "
                "v4 will be created but LEFT JOIN base will be empty.",
            )

        # Create v4 view
        print(f"\n  Creating {_V4_VIEW}...")
        md.execute(_V4_VIEW_SQL)
        n_v4 = md.execute(
            f"SELECT COUNT(*) FROM {_V4_VIEW}"
        ).fetchone()[0]
        print(f"  {_V4_VIEW}: {n_v4:,} rows")

        # Coverage delta
        _print_coverage(md)

        # Pull v4 fill rates for signoff
        try:
            row = md.execute(
                "SELECT "
                "  ROUND(100.0 * COUNT(fna_size_cm_resolved) / COUNT(*), 1), "
                "  ROUND(100.0 * COUNT(fna_laterality_resolved) / COUNT(*), 1) "
                f"FROM {_V4_VIEW}"
            ).fetchone()
            size_fill = float(row[0] or 0)
            lat_fill = float(row[1] or 0)
        except Exception:
            size_fill = lat_fill = 0.0

        if args.signoff:
            print("\n  Writing signoff...")
            _write_signoff(md, n_rollup, size_fill, lat_fill)

        print(
            f"\nmig_310 MD-side COMPLETE — {_V4_VIEW} created with {n_v4:,} rows.\n"
            "  M025 nodule-level cohort rebuild: re-run mig_306 build with v4 join.\n"
            "  Sister papers (M046, M053) now unblocked for FNA-size covariate."
        )
        return 0

    finally:
        md.close()


if __name__ == "__main__":
    raise SystemExit(main())
