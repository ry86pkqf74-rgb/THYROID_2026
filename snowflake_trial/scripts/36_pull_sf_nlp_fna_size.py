#!/usr/bin/env python3
"""mig_310 — FNA NLP size extraction (Cortex EXTRACT_ANSWER) + MotherDuck mirror.

Pipeline
--------
A. Probe Snowflake for existing FNA corpus tables.
B. Export FNA-type notes from MotherDuck ``clinical_notes_long`` to parquet.
C. Upload parquet to SF COWORK_STAGE, COPY INTO ``FNA_NOTES_MIG310``.
D. Run Cortex ``EXTRACT_ANSWER`` → ``NLP_FNA_SIZE_FULL_RESULTS_v1`` +
   ``NLP_FNA_SIZE_PATIENT_ROLLUP_v1``.
E. Sample-200 validation probe (print precision estimates).
F. Mirror patient-rollup to MotherDuck
   ``manuscript_workspace.nlp_fna_size_rollup_v1``.
G. (Optional with --signoff) insert ``main.signoff_migration`` row.

Usage (repo root, .venv)::

    SNOWFLAKE_PAT=... .venv/bin/python snowflake_trial/scripts/36_pull_sf_nlp_fna_size.py \\
        --md [--signoff] [--dry-run] [--pilot]

Flags
-----
--md        Write mirror to MotherDuck (requires RW token).
--dry-run   Export + upload to SF; skip Cortex call and MotherDuck write.
--pilot     Limit corpus to 200 random notes for a fast QA run.
--signoff   After mirror, insert signoff row to ``main.signoff_migration``.

PHI policy: note text processed in-database (Snowflake); never written to
local disk or MotherDuck.  Only research_id, dates, extracted numeric/
categorical values are transmitted.
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_DIR = Path(__file__).resolve().parent
for _p in (REPO_ROOT, _SCRIPT_DIR):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)

os.environ.setdefault("MOTHERDUCK_DATABASE", "thyroid_canonical_publication_v1_0")

from _sf_client import get_cursor  # noqa: E402

# ---------------------------------------------------------------------------
# FNA note-type filter (all likely FNA / cytology note_type values)
# ---------------------------------------------------------------------------
_FNA_NOTE_TYPES = (
    "fna_cytology",
    "fna",
    "cytology",
    "fna_report",
    "cytology_report",
    "fine_needle_aspiration",
    "fna_cytopathology",
)

_FNA_TYPE_SQL = " OR ".join(
    f"LOWER(note_type) = '{t}'" for t in _FNA_NOTE_TYPES
)

# Keyword fallback: any note mentioning aspirat*/biopsy/cytolog* in the
# context of thyroid, used when structured note_type classification is absent.
_FNA_KEYWORD_SQL = (
    "(("
    + _FNA_TYPE_SQL
    + ") OR ("
    "LOWER(note_text) LIKE '%fine needle aspiration%' OR "
    "LOWER(note_text) LIKE '%fna%' OR "
    "LOWER(note_text) LIKE '%thyroid biopsy%' OR "
    "LOWER(note_text) LIKE '%thyroid cytolog%' OR "
    "LOWER(note_text) LIKE '%aspirate%'"
    "))"
)

_SF_FNA_TABLE = "FNA_NOTES_MIG310"
_SF_RESULTS_TABLE = "NLP_FNA_SIZE_FULL_RESULTS_v1"
_SF_ROLLUP_TABLE = "NLP_FNA_SIZE_PATIENT_ROLLUP_v1"
_SF_STAGE = "COWORK_STAGE"
_SF_STAGE_PREFIX = "fna_mig310"

_MD_TARGET_TABLE = "manuscript_workspace.nlp_fna_size_rollup_v1"
_SIGNOFF_MIG_ID = "mig_310"

_PLAUSIBLE_SIZE_RANGE = (0.1, 15.0)


# ---------------------------------------------------------------------------
# Phase A — Probe SF for existing FNA tables
# ---------------------------------------------------------------------------

def _probe_sf_tables(cur) -> dict[str, bool]:
    """Return existence flags for key SF objects."""
    result: dict[str, bool] = {}
    for obj in (_SF_FNA_TABLE, _SF_RESULTS_TABLE, _SF_ROLLUP_TABLE):
        cur.execute(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = '{obj}'"
        )
        result[obj] = cur.fetchone()[0] > 0  # type: ignore[index]
    return result


def _probe_note_types(cur) -> None:
    """Print note_type distribution in CLINICAL_NOTES_SEARCH_V1 if present."""
    try:
        cur.execute(
            "SELECT DISTINCT NOTE_TYPE, COUNT(*) AS n "
            "FROM CLINICAL_NOTES_SEARCH_V1 "
            "GROUP BY 1 ORDER BY 2 DESC LIMIT 20"
        )
        rows = cur.fetchall()
        print("  SF CLINICAL_NOTES_SEARCH_V1 note_type distribution:")
        for nt, n in rows:
            print(f"    {nt or '<null>'}: {n:,}")
    except Exception as exc:
        print(f"  WARN: CLINICAL_NOTES_SEARCH_V1 probe failed: {exc}")


# ---------------------------------------------------------------------------
# Phase B — Export corpus from MotherDuck
# ---------------------------------------------------------------------------

def _export_from_md(parq_path: Path, pilot: bool) -> int:
    """Export FNA notes from MotherDuck; return row count."""
    import duckdb

    token = (
        os.environ.get("MD_SA_TOKEN")
        or os.environ.get("MOTHERDUCK_TOKEN")
        or os.environ.get("motherduck_token")
    )
    if not token:
        # Try TOML
        try:
            import toml as _toml
            _cfg = _toml.load(REPO_ROOT / "motherduck.local.toml")
            token = (
                _cfg.get("MD_SA_TOKEN")
                or _cfg.get("MOTHERDUCK_TOKEN")
                or _cfg.get("motherduck_token")
            )
        except Exception:
            pass
    if not token:
        # Try .streamlit/secrets.toml
        try:
            import toml as _toml
            _cfg = _toml.load(REPO_ROOT / ".streamlit" / "secrets.toml")
            token = (
                _cfg.get("MD_SA_TOKEN")
                or _cfg.get("MOTHERDUCK_TOKEN")
                or _cfg.get("motherduck_token")
            )
        except Exception:
            pass
    if not token:
        print("FATAL: no MotherDuck RW token found.", file=sys.stderr)
        sys.exit(1)

    limit_clause = "ORDER BY RANDOM() LIMIT 200" if pilot else ""
    md = duckdb.connect(
        f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}"
    )
    try:
        # First, check which note_types are present
        try:
            nt_rows = md.execute(
                f"SELECT LOWER(note_type) AS nt, COUNT(*) AS n "
                f"FROM main.clinical_notes_long "
                f"WHERE {_FNA_KEYWORD_SQL} "
                f"GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
            ).fetchall()
            print("  MD FNA-candidate note_type distribution:")
            for nt, n in nt_rows:
                print(f"    {nt or '<null>'}: {n:,}")
        except Exception as exc:
            print(f"  WARN: note_type probe failed: {exc}")

        md.execute(
            f"""
            COPY (
                SELECT
                    CAST(research_id AS VARCHAR)            AS RESEARCH_ID,
                    COALESCE(note_type, 'unknown')          AS NOTE_TYPE,
                    CAST(note_index AS INTEGER)             AS NOTE_INDEX,
                    CAST(note_date AS VARCHAR)              AS NOTE_DATE,
                    -- Truncate at 12000 chars; EXTRACT_ANSWER handles long text
                    -- but SF free-tier has per-call limits.
                    SUBSTR(note_text, 1, 12000)             AS NOTE_TEXT
                FROM main.clinical_notes_long
                WHERE {_FNA_KEYWORD_SQL}
                  AND note_text IS NOT NULL
                  AND LENGTH(TRIM(note_text)) > 50
                {limit_clause}
            )
            TO '{parq_path}' (FORMAT 'parquet')
            """
        )
        count = duckdb.connect().execute(
            f"SELECT COUNT(*) FROM '{parq_path}'"
        ).fetchone()[0]  # type: ignore[index]
        print(f"  Exported {count:,} FNA notes to {parq_path.name}")
        return count
    finally:
        md.close()


# ---------------------------------------------------------------------------
# Phase C — Upload to Snowflake
# ---------------------------------------------------------------------------

def _upload_to_sf(cur, parq_path: Path, n_notes: int) -> int:
    """PUT parquet → COWORK_STAGE; COPY INTO FNA_NOTES_MIG310; return loaded rows."""
    cur.execute(f"CREATE OR REPLACE TABLE {_SF_FNA_TABLE} ("
                "RESEARCH_ID VARCHAR, NOTE_TYPE VARCHAR, NOTE_INDEX INTEGER, "
                "NOTE_DATE VARCHAR, NOTE_TEXT VARCHAR)")
    cur.execute(f"CREATE STAGE IF NOT EXISTS {_SF_STAGE}")
    cur.execute(
        f"PUT 'file://{parq_path}' @{_SF_STAGE}/{_SF_STAGE_PREFIX}/ "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    parq_name = parq_path.name
    cur.execute(
        f"""
        COPY INTO {_SF_FNA_TABLE}
          (RESEARCH_ID, NOTE_TYPE, NOTE_INDEX, NOTE_DATE, NOTE_TEXT)
        FROM (
            SELECT
                $1:RESEARCH_ID::VARCHAR,
                $1:NOTE_TYPE::VARCHAR,
                $1:NOTE_INDEX::INTEGER,
                $1:NOTE_DATE::VARCHAR,
                $1:NOTE_TEXT::VARCHAR
            FROM @{_SF_STAGE}/{_SF_STAGE_PREFIX}/{parq_name}
        )
        FILE_FORMAT = (TYPE = PARQUET)
        """
    )
    cur.execute(f"SELECT COUNT(*) FROM {_SF_FNA_TABLE}")
    n_loaded: int = cur.fetchone()[0]  # type: ignore[index]
    print(f"  SF {_SF_FNA_TABLE}: {n_loaded:,} rows loaded (exported {n_notes:,})")
    return n_loaded


# ---------------------------------------------------------------------------
# Phase D — Cortex EXTRACT_ANSWER extraction
# ---------------------------------------------------------------------------

def _run_extraction(cur) -> None:
    """Create NLP_FNA_SIZE_FULL_RESULTS_v1 and NLP_FNA_SIZE_PATIENT_ROLLUP_v1."""
    print("  Running Cortex EXTRACT_ANSWER (may take several minutes)...")
    t0 = datetime.now()

    cur.execute(
        f"""
        CREATE OR REPLACE TABLE {_SF_RESULTS_TABLE} AS
        WITH extracted AS (
            SELECT
                RESEARCH_ID,
                NOTE_TYPE,
                NOTE_INDEX,
                NOTE_DATE,
                SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
                    NOTE_TEXT,
                    'What is the size (largest dimension) of the aspirated thyroid nodule '
                    'in centimeters? Provide only the numeric value as a decimal (e.g. 1.5). '
                    'Convert mm to cm if needed. If multiple nodules, report the largest. '
                    'Return NULL if not stated.'
                ) AS _size_raw,
                SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
                    NOTE_TEXT,
                    'What is the laterality (side) of the thyroid nodule sampled in this '
                    'FNA? Answer with exactly one word: right, left, isthmus, or bilateral. '
                    'Return NULL if not stated.'
                ) AS _lat_raw,
                SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
                    NOTE_TEXT,
                    'How many distinct thyroid nodules were sampled in this FNA procedure? '
                    'Answer with a whole number; default to 1 if a single nodule is '
                    'described. Return NULL if completely unclear.'
                ) AS _count_raw,
                CURRENT_TIMESTAMP AS extracted_at,
                'cortex_extract_answer_mig_310' AS extraction_source
            FROM {_SF_FNA_TABLE}
        )
        SELECT
            RESEARCH_ID,
            NOTE_TYPE,
            NOTE_INDEX,
            NOTE_DATE,
            TRY_TO_DOUBLE(
                NULLIF(TRIM(_size_raw[0]:answer::VARCHAR), '')
            )                                                       AS extracted_size_cm,
            CASE
                WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR)) LIKE '%right%'     THEN 'right'
                WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR)) LIKE '%left%'      THEN 'left'
                WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR)) LIKE '%isthmus%'   THEN 'isthmus'
                WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR)) LIKE '%bilateral%' THEN 'bilateral'
                ELSE NULL
            END                                                     AS extracted_laterality,
            TRY_TO_NUMBER(
                NULLIF(TRIM(_count_raw[0]:answer::VARCHAR), ''), 10, 0
            )                                                       AS extracted_nodule_count,
            CASE
                WHEN _size_raw[0]:score::FLOAT  > 0.80
                 AND _lat_raw[0]:score::FLOAT   > 0.80  THEN 'high'
                WHEN _size_raw[0]:score::FLOAT  > 0.50
                  OR _lat_raw[0]:score::FLOAT   > 0.50  THEN 'medium'
                ELSE 'low'
            END                                                     AS extraction_confidence,
            _size_raw[0]:score::FLOAT                               AS size_extract_score,
            _lat_raw[0]:score::FLOAT                                AS lat_extract_score,
            _count_raw[0]:score::FLOAT                              AS count_extract_score,
            extracted_at,
            extraction_source
        FROM extracted
        """
    )
    elapsed = (datetime.now() - t0).total_seconds()
    cur.execute(f"SELECT COUNT(*) FROM {_SF_RESULTS_TABLE}")
    n_rows: int = cur.fetchone()[0]  # type: ignore[index]
    print(f"  EXTRACT_ANSWER: {n_rows:,} rows in {elapsed:.1f}s")

    # Patient-level rollup
    cur.execute(
        f"""
        CREATE OR REPLACE TABLE {_SF_ROLLUP_TABLE} AS
        SELECT
            RESEARCH_ID,
            NOTE_DATE                                                AS fna_date,
            -- Best size: prefer high-confidence record; tiebreak = largest size
            FIRST_VALUE(extracted_size_cm) OVER (
                PARTITION BY RESEARCH_ID, NOTE_DATE
                ORDER BY
                    CASE extraction_confidence WHEN 'high' THEN 2
                                               WHEN 'medium' THEN 1
                                               ELSE 0 END DESC,
                    size_extract_score DESC NULLS LAST,
                    extracted_size_cm DESC NULLS LAST
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )                                                        AS extracted_size_cm,
            FIRST_VALUE(extracted_laterality) OVER (
                PARTITION BY RESEARCH_ID, NOTE_DATE
                ORDER BY
                    CASE extraction_confidence WHEN 'high' THEN 2
                                               WHEN 'medium' THEN 1
                                               ELSE 0 END DESC,
                    lat_extract_score DESC NULLS LAST
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )                                                        AS extracted_laterality,
            MAX(COALESCE(extracted_nodule_count, 1)) OVER (
                PARTITION BY RESEARCH_ID, NOTE_DATE
            )                                                        AS extracted_nodule_count,
            FIRST_VALUE(extraction_confidence) OVER (
                PARTITION BY RESEARCH_ID, NOTE_DATE
                ORDER BY
                    CASE extraction_confidence WHEN 'high' THEN 2
                                               WHEN 'medium' THEN 1
                                               ELSE 0 END DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )                                                        AS extraction_confidence,
            COUNT(*) OVER (PARTITION BY RESEARCH_ID, NOTE_DATE)      AS n_notes_aggregated,
            MAX(size_extract_score) OVER (
                PARTITION BY RESEARCH_ID, NOTE_DATE
            )                                                        AS max_size_score,
            MAX(lat_extract_score) OVER (
                PARTITION BY RESEARCH_ID, NOTE_DATE
            )                                                        AS max_lat_score,
            'cortex_extract_answer_mig_310'                          AS extraction_source,
            CURRENT_TIMESTAMP                                        AS rollup_built_at
        FROM {_SF_RESULTS_TABLE}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY RESEARCH_ID, NOTE_DATE ORDER BY size_extract_score DESC NULLS LAST
        ) = 1
        """
    )
    cur.execute(f"SELECT COUNT(*) FROM {_SF_ROLLUP_TABLE}")
    n_rollup: int = cur.fetchone()[0]  # type: ignore[index]
    print(f"  Patient-date rollup: {n_rollup:,} rows in {_SF_ROLLUP_TABLE}")


# ---------------------------------------------------------------------------
# Phase E — Sample-200 validation
# ---------------------------------------------------------------------------

def _run_validation(cur) -> dict[str, float]:
    """Pull a sample for QA and print precision proxies."""
    cur.execute(
        f"""
        SELECT
            COUNT(*)                                        AS total,
            COUNT(extracted_size_cm)                        AS size_populated,
            COUNT(extracted_laterality)                     AS lat_populated,
            ROUND(100.0 * COUNT(extracted_size_cm)    / COUNT(*), 1) AS size_fill_pct,
            ROUND(100.0 * COUNT(extracted_laterality) / COUNT(*), 1) AS lat_fill_pct,
            COUNT_IF(extracted_size_cm BETWEEN 0.1 AND 15.0)         AS size_plausible_n,
            COUNT_IF(extraction_confidence = 'high')                 AS high_conf_n,
            COUNT_IF(extraction_confidence = 'medium')               AS med_conf_n,
            COUNT_IF(extraction_confidence = 'low')                  AS low_conf_n,
            ROUND(AVG(max_size_score), 3)                            AS avg_size_score,
            ROUND(AVG(max_lat_score), 3)                             AS avg_lat_score
        FROM {_SF_ROLLUP_TABLE}
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if row is None:
        print("  WARN: rollup table empty; skipping validation.")
        return {}

    (
        total, size_pop, lat_pop, size_pct, lat_pct,
        plausible, high_c, med_c, low_c, avg_sz, avg_lat,
    ) = row

    total = total or 0
    size_pop = size_pop or 0
    lat_pop = lat_pop or 0
    plausible = plausible or 0

    print("\n  === mig_310 Validation Summary ===")
    print(f"  Patient-date rows : {total:,}")
    print(f"  Size populated    : {size_pop:,} ({size_pct}%)")
    print(f"  Lat populated     : {lat_pop:,} ({lat_pct}%)")
    if size_pop:
        plaus_pct = round(100.0 * plausible / size_pop, 1)
        print(f"  Size plausible    : {plausible:,} / {size_pop:,} ({plaus_pct}%)")
    print(f"  Confidence: high={high_c} med={med_c} low={low_c}")
    print(f"  Avg scores: size={avg_sz}  lat={avg_lat}")

    # Plausibility gate: warn if >10% implausible sizes
    if size_pop and plausible / size_pop < 0.90:
        print(
            f"  WARN: plausibility <90% ({plaus_pct}%). "
            "Consider re-extracting or excluding extreme values."
        )
    return {
        "total": total,
        "size_fill_pct": float(size_pct or 0),
        "lat_fill_pct": float(lat_pct or 0),
        "size_plausible_pct": round(100.0 * plausible / size_pop, 1) if size_pop else 0.0,
        "avg_size_score": float(avg_sz or 0),
        "avg_lat_score": float(avg_lat or 0),
    }


# ---------------------------------------------------------------------------
# Phase F — Mirror to MotherDuck
# ---------------------------------------------------------------------------

def _mirror_to_md(cur) -> int:
    """Pull patient rollup from SF → MotherDuck; return mirrored row count."""
    cur.execute(f"SELECT * FROM {_SF_ROLLUP_TABLE}")
    sf_df = cur.fetch_pandas_all()
    sf_df.columns = [str(c).upper() for c in sf_df.columns]
    n = len(sf_df)
    print(f"  Fetched {n:,} rows from SF for MD mirror.")

    from utils.md_connect import connect_md_fail_closed  # noqa: E402

    md = connect_md_fail_closed(REPO_ROOT / "thyroid_master.duckdb")
    try:
        md.execute("USE thyroid_canonical_publication_v1_0")
        md.execute(
            """
            CREATE TABLE IF NOT EXISTS manuscript_workspace.nlp_fna_size_rollup_v1 (
                research_id            VARCHAR,
                fna_date               VARCHAR,
                extracted_size_cm      DOUBLE,
                extracted_laterality   VARCHAR,
                extracted_nodule_count INTEGER,
                extraction_confidence  VARCHAR,
                n_notes_aggregated     BIGINT,
                max_size_score         DOUBLE,
                max_lat_score          DOUBLE,
                extraction_source      VARCHAR,
                rollup_built_at        TIMESTAMP,
                mirrored_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        md.execute("DELETE FROM manuscript_workspace.nlp_fna_size_rollup_v1")
        md.register("_sf_fna_size_rollup", sf_df)
        try:
            md.execute(
                """
                INSERT INTO manuscript_workspace.nlp_fna_size_rollup_v1
                  (research_id, fna_date, extracted_size_cm, extracted_laterality,
                   extracted_nodule_count, extraction_confidence, n_notes_aggregated,
                   max_size_score, max_lat_score, extraction_source, rollup_built_at)
                SELECT
                    RESEARCH_ID::VARCHAR,
                    FNA_DATE::VARCHAR,
                    EXTRACTED_SIZE_CM::DOUBLE,
                    EXTRACTED_LATERALITY::VARCHAR,
                    EXTRACTED_NODULE_COUNT::INTEGER,
                    EXTRACTION_CONFIDENCE::VARCHAR,
                    N_NOTES_AGGREGATED::BIGINT,
                    MAX_SIZE_SCORE::DOUBLE,
                    MAX_LAT_SCORE::DOUBLE,
                    EXTRACTION_SOURCE::VARCHAR,
                    ROLLUP_BUILT_AT::TIMESTAMP
                FROM _sf_fna_size_rollup
                """
            )
        finally:
            md.unregister("_sf_fna_size_rollup")

        cnt = md.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.nlp_fna_size_rollup_v1"
        ).fetchone()[0]
        print(f"  Mirrored {cnt:,} rows → manuscript_workspace.nlp_fna_size_rollup_v1")
        return cnt
    finally:
        md.close()


# ---------------------------------------------------------------------------
# Phase G — Signoff
# ---------------------------------------------------------------------------

def _write_signoff(stats: dict[str, float], n_rollup: int) -> None:
    """Insert mig_310 signoff row to main.signoff_migration."""
    from utils.md_connect import connect_md_fail_closed  # noqa: E402

    size_pct = stats.get("size_fill_pct", 0.0)
    lat_pct = stats.get("lat_fill_pct", 0.0)
    plaus_pct = stats.get("size_plausible_pct", 0.0)
    avg_sz_sc = stats.get("avg_size_score", 0.0)

    summary = (
        f"mig_310: FNA NLP size extraction. "
        f"SF NLP_FNA_SIZE_FULL_RESULTS_v1 + NLP_FNA_SIZE_PATIENT_ROLLUP_v1 "
        f"built via Cortex EXTRACT_ANSWER. "
        f"Rollup: {n_rollup} patient-date rows. "
        f"size_fill={size_pct}% lat_fill={lat_pct}% "
        f"size_plausible={plaus_pct}% avg_size_score={avg_sz_sc:.3f}. "
        f"Mirrored to manuscript_workspace.nlp_fna_size_rollup_v1. "
        f"Run scripts/mig_310_fna_size_mirror.py --md to build imaging_fna_linkage_v4. "
        f"Closes CF-FNA-SIZE-CM-NULL."
    )

    md = connect_md_fail_closed(REPO_ROOT / "thyroid_master.duckdb")
    try:
        md.execute("USE thyroid_canonical_publication_v1_0")
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
    finally:
        md.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--md", action="store_true",
                    help="Mirror results to MotherDuck (fail-closed).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Export + SF upload only; skip Cortex + MotherDuck write.")
    ap.add_argument("--pilot", action="store_true",
                    help="Limit corpus to 200 random notes for a fast QA run.")
    ap.add_argument("--signoff", action="store_true",
                    help="Insert signoff row after successful mirror.")
    args = ap.parse_args()

    if not args.md and not args.dry_run:
        print("FATAL: pass --md to write MotherDuck or --dry-run to preview.",
              file=sys.stderr)
        return 1

    pat = os.environ.get("SNOWFLAKE_PAT")
    if not pat:
        print("FATAL: SNOWFLAKE_PAT is not set.", file=sys.stderr)
        return 1

    run_ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    print(f"mig_310 FNA NLP size extraction — {run_ts}")
    print(f"  mode: {'pilot' if args.pilot else 'full-scale'} | "
          f"{'--dry-run' if args.dry_run else '--md'}")

    ctx, cur = get_cursor()
    try:
        cur.execute("USE DATABASE THYROID_VALIDATION")
        cur.execute("USE SCHEMA PUBLIC")

        # Phase A — Probe
        print("\n[A] Probing Snowflake tables...")
        existing = _probe_sf_tables(cur)
        for tbl, exists in existing.items():
            print(f"  {tbl}: {'EXISTS' if exists else 'absent'}")
        _probe_note_types(cur)

        # Phase B — Export from MD
        print("\n[B] Exporting FNA corpus from MotherDuck...")
        with tempfile.TemporaryDirectory() as tmpdir:
            parq_path = Path(tmpdir) / f"fna_notes_mig310_{run_ts}.parquet"
            n_notes = _export_from_md(parq_path, pilot=args.pilot)

            if n_notes == 0:
                print("FATAL: no FNA notes found in clinical_notes_long. "
                      "Check note_type values and keyword filter.", file=sys.stderr)
                return 1

            # Phase C — Upload to SF
            print("\n[C] Uploading to Snowflake...")
            n_loaded = _upload_to_sf(cur, parq_path, n_notes)

        if args.dry_run:
            print("\n--dry-run: skipping Cortex call and MotherDuck write.")
            return 0

        if n_loaded == 0:
            print("FATAL: 0 rows loaded to SF; aborting extraction.", file=sys.stderr)
            return 1

        # Phase D — Cortex extraction
        print("\n[D] Running Cortex EXTRACT_ANSWER...")
        _run_extraction(cur)

        # Phase E — Validation
        print("\n[E] Validation summary...")
        stats = _run_validation(cur)

        # Phase F — Mirror to MD
        print("\n[F] Mirroring to MotherDuck...")
        n_mirrored = _mirror_to_md(cur)

        # Phase G — Signoff (optional)
        if args.signoff:
            print("\n[G] Writing signoff...")
            _write_signoff(stats, n_mirrored)

        print(
            f"\nmig_310 COMPLETE — {n_mirrored:,} rows in "
            f"manuscript_workspace.nlp_fna_size_rollup_v1"
        )
        print(
            "  Next: run  .venv/bin/python scripts/mig_310_fna_size_mirror.py --md"
            "  to build imaging_fna_linkage_v4 view."
        )
        return 0

    finally:
        ctx.close()


if __name__ == "__main__":
    raise SystemExit(main())
