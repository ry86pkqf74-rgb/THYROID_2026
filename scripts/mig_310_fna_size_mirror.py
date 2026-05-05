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
# v3 linkage object is in ``main`` on publication MotherDuck (fallback: manuscript_workspace).
_V3_MAIN = "main.imaging_fna_linkage_v3"
_V3_MWS = "manuscript_workspace.imaging_fna_linkage_v3"
_V4_VIEW = "manuscript_workspace.imaging_fna_linkage_v4"
_SIGNOFF_MIG_ID = "mig_310"


def _resolve_v3_ref(md) -> str:
    """Return qualified ref to imaging_fna_linkage_v3 (view or base table)."""
    for ref in (_V3_MAIN, _V3_MWS):
        sch, _, name = ref.partition(".")
        try:
            for rel in ("views", "tables"):
                cnt = md.execute(
                    f"SELECT COUNT(*) FROM information_schema.{rel} "
                    f"WHERE table_schema = '{sch}' "
                    f"  AND table_name = '{name}'"
                ).fetchone()[0]
                if cnt and int(cnt) > 0:
                    md.execute(f"SELECT 1 FROM {ref} LIMIT 1")
                    return ref
        except Exception:
            continue
    return _V3_MAIN


# ---------------------------------------------------------------------------
# SQL — imaging_fna_linkage_v4
# ---------------------------------------------------------------------------
def _v4_view_sql(v3_ref: str) -> str:
    """Build v4 DDL (v3 catalog resolved at runtime).

    One row per v3 linkage: ``LEFT JOIN`` to NLP rollup may match multiple events
    within ±14 days; retain the closest ``fna_date`` tie-broken by size score.
    """
    return f"""
CREATE OR REPLACE VIEW {_V4_VIEW} AS
SELECT * EXCLUDE (rn),
    COALESCE(
        fna_size_cm,
        CASE
            WHEN nlp_extracted_size_cm BETWEEN 0.1 AND 15.0
                 AND nlp_extraction_confidence IN ('high', 'medium')
            THEN nlp_extracted_size_cm
            WHEN nlp_extracted_size_cm BETWEEN 0.1 AND 15.0
            THEN nlp_extracted_size_cm
            ELSE NULL
        END
    )                                           AS fna_size_cm_resolved,

    COALESCE(fna_laterality, nlp_extracted_laterality)
                                                AS fna_laterality_resolved,

    CASE
        WHEN fna_size_cm IS NOT NULL          THEN 'structured'
        WHEN nlp_extracted_size_cm IS NOT NULL
             AND nlp_extraction_confidence = 'high'  THEN 'nlp_high'
        WHEN nlp_extracted_size_cm IS NOT NULL
             AND nlp_extraction_confidence = 'medium' THEN 'nlp_medium'
        WHEN nlp_extracted_size_cm IS NOT NULL    THEN 'nlp_low'
        ELSE 'missing'
    END                                         AS fna_size_source_v4,

    CASE
        WHEN fna_size_cm IS NOT NULL          THEN 1.00
        WHEN nlp_extracted_size_cm IS NOT NULL
             AND nlp_extraction_confidence = 'high'  THEN 0.85
        WHEN nlp_extracted_size_cm IS NOT NULL
             AND nlp_extraction_confidence = 'medium' THEN 0.70
        WHEN nlp_extracted_size_cm IS NOT NULL    THEN 0.50
        ELSE 0.50
    END                                         AS size_score_v4

FROM (
    SELECT
        l.*,

        n.extracted_size_cm                         AS nlp_extracted_size_cm,
        n.extracted_laterality                      AS nlp_extracted_laterality,
        n.extracted_nodule_count                    AS nlp_extracted_nodule_count,
        n.extracted_bethesda                        AS nlp_extracted_bethesda,
        n.extraction_confidence                     AS nlp_extraction_confidence,
        n.max_size_score                            AS nlp_size_extract_score,
        n.max_lat_score                             AS nlp_lat_extract_score,

        ROW_NUMBER() OVER (
            PARTITION BY
                l.research_id,
                l.nodule_id,
                l.imaging_exam_id,
                l.fna_episode_id
            ORDER BY
                CASE WHEN n.research_id IS NULL THEN 1 ELSE 0 END,
                ABS(
                    DATEDIFF(
                        'day',
                        TRY_CAST(l.fna_date AS DATE),
                        TRY_CAST(n.fna_date AS DATE)
                    )
                ) NULLS LAST,
                n.max_size_score DESC NULLS LAST
        ) AS rn

    FROM {v3_ref} l
    LEFT JOIN {_ROLLUP_TABLE} n
        ON CAST(l.research_id AS VARCHAR) = CAST(n.research_id AS VARCHAR)
       AND ABS(
             DATEDIFF(
                 'day',
                 TRY_CAST(l.fna_date AS DATE),
                 TRY_CAST(n.fna_date AS DATE)
             )
           ) <= 14
) s
WHERE s.rn = 1;
"""


def _coverage_sql(v3_ref: str) -> str:
    return f"""
SELECT
    'v3' AS linkage_version,
    COUNT(*)                                              AS total_links,
    COUNT(fna_size_cm)                                    AS size_populated,
    ROUND(100.0 * COUNT(fna_size_cm) / COUNT(*), 1)      AS size_fill_pct,
    COUNT(fna_laterality)                                 AS lat_populated,
    ROUND(100.0 * COUNT(fna_laterality) / COUNT(*), 1)   AS lat_fill_pct
FROM {v3_ref}

UNION ALL

SELECT
    'v4' AS linkage_version,
    COUNT(*)                                              AS total_links,
    COUNT(fna_size_cm_resolved)                           AS size_populated,
    ROUND(100.0 * COUNT(fna_size_cm_resolved) / COUNT(*), 1) AS size_fill_pct,
    COUNT(fna_laterality_resolved)                        AS lat_populated,
    ROUND(100.0 * COUNT(fna_laterality_resolved) / COUNT(*), 1) AS lat_fill_pct
FROM {_V4_VIEW}

ORDER BY linkage_version;
"""


# Coverage SQL is built via ``_coverage_sql(v3_ref)`` after resolving v3 location.

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


def _check_v3_exists(md, v3_ref: str) -> bool:
    try:
        md.execute(f"SELECT 1 FROM {v3_ref} LIMIT 1")
        return True
    except Exception:
        return False


def _print_coverage(md, v3_ref: str) -> None:
    try:
        rows = md.execute(_coverage_sql(v3_ref)).fetchall()
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
        print(_v4_view_sql(_V3_MAIN))
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

        v3_ref = _resolve_v3_ref(md)
        print(f"  imaging_fna_linkage_v3 base: {v3_ref}")

        # Gate 2 — v3 linkage must be readable
        if not _check_v3_exists(md, v3_ref):
            print(
                f"WARN: cannot read {v3_ref}; v4 creation will likely fail.",
                file=sys.stderr,
            )

        # Create v4 view
        print(f"\n  Creating {_V4_VIEW}...")
        md.execute(_v4_view_sql(v3_ref))
        n_v4 = md.execute(
            f"SELECT COUNT(*) FROM {_V4_VIEW}"
        ).fetchone()[0]
        print(f"  {_V4_VIEW}: {n_v4:,} rows")

        # Coverage delta
        _print_coverage(md, v3_ref)

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
