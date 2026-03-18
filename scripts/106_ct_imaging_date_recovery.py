#!/usr/bin/env python3
"""
106_ct_imaging_date_recovery.py — CT Imaging Date Recovery & Surgery Timing

Fixes:
  1. Recovers 50 missing ct_imaging.date_of_exam values from original_report text
     - 45 in M/D/YY format (parsed via TRY_STRPTIME '%m/%d/%y')
     - 5 in YYYY-MM-DD HH:MM:SS format (parsed via TRY_STRPTIME '%Y-%m-%d')
     - Result: 100% date coverage (was 99.4%)

  2. Creates ct_imaging_surgery_timing table (7,701 rows, 3,086 patients)
     - Joins ct_imaging.date_of_exam with path_synoptics first surgery date
     - Computes days_from_surgery, timing_category, post_30d/1yr flags

  3. Creates ptc_ct_imaging_events table (3,018 rows, 650 PTC patients)
     - PTC cohort subset with CT imaging + pathologic LN flags + timing

Supports: --md (MotherDuck), --local, --dry-run
"""
import argparse
import sys
import os

def get_connection(args):
    import duckdb
    if args.md:
        try:
            import toml
            token = toml.load('.streamlit/secrets.toml')['MOTHERDUCK_TOKEN']
        except Exception:
            token = os.environ.get('MOTHERDUCK_TOKEN', '')
        if not token:
            print("ERROR: No MOTHERDUCK_TOKEN found", file=sys.stderr)
            sys.exit(1)
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    else:
        return duckdb.connect(args.local_db)


def run(args):
    con = get_connection(args)

    # Phase 1: Recover M/D/YY dates from original_report
    print("Phase 1: Recovering M/D/YY dates from original_report ...")
    before = con.execute("SELECT COUNT(*) FROM ct_imaging WHERE date_of_exam IS NULL").fetchone()[0]
    print(f"  NULL dates before: {before}")

    if not args.dry_run:
        con.execute(r"""
            UPDATE ct_imaging
            SET date_of_exam = COALESCE(
                TRY_STRPTIME(regexp_extract(CAST(original_report AS VARCHAR),
                    '^\s*(\d{1,2}/\d{1,2}/\d{2,4})', 1), '%m/%d/%Y'),
                TRY_STRPTIME(regexp_extract(CAST(original_report AS VARCHAR),
                    '^\s*(\d{1,2}/\d{1,2}/\d{2,4})', 1), '%m/%d/%y')
            )::DATE
            WHERE date_of_exam IS NULL
              AND original_report IS NOT NULL
              AND regexp_matches(CAST(original_report AS VARCHAR), '^\s*\d{1,2}/\d{1,2}/\d{2,4}')
              AND COALESCE(
                  TRY_STRPTIME(regexp_extract(CAST(original_report AS VARCHAR),
                      '^\s*(\d{1,2}/\d{1,2}/\d{2,4})', 1), '%m/%d/%Y'),
                  TRY_STRPTIME(regexp_extract(CAST(original_report AS VARCHAR),
                      '^\s*(\d{1,2}/\d{1,2}/\d{2,4})', 1), '%m/%d/%y')
              ) IS NOT NULL
        """)

    # Phase 2: Recover YYYY-MM-DD dates
    print("Phase 2: Recovering ISO dates from original_report ...")
    if not args.dry_run:
        con.execute(r"""
            UPDATE ct_imaging
            SET date_of_exam = TRY_STRPTIME(
                regexp_extract(CAST(original_report AS VARCHAR),
                    '^\s*(\d{4}-\d{2}-\d{2})', 1), '%Y-%m-%d'
            )::DATE
            WHERE date_of_exam IS NULL
              AND original_report IS NOT NULL
              AND regexp_matches(CAST(original_report AS VARCHAR), '^\s*\d{4}-\d{2}-\d{2}')
        """)

    after = con.execute("SELECT COUNT(*) FROM ct_imaging WHERE date_of_exam IS NULL").fetchone()[0]
    total = con.execute("SELECT COUNT(*) FROM ct_imaging").fetchone()[0]
    print(f"  NULL dates after: {after} of {total} ({100*(total-after)/total:.1f}% coverage)")

    # Phase 3: Create ct_imaging_surgery_timing
    print("\nPhase 3: Creating ct_imaging_surgery_timing table ...")
    if not args.dry_run:
        con.execute("DROP TABLE IF EXISTS ct_imaging_surgery_timing")
        con.execute("""
            CREATE TABLE ct_imaging_surgery_timing AS
            WITH surgery_dates AS (
                SELECT
                    CAST(research_id AS VARCHAR) as research_id,
                    MIN(TRY_CAST(surg_date AS DATE)) as first_surgery_date
                FROM path_synoptics
                WHERE surg_date IS NOT NULL
                GROUP BY research_id
            )
            SELECT
                ct.research_id,
                ct.ct_column,
                TRY_CAST(ct.date_of_exam AS DATE) as ct_date,
                sd.first_surgery_date as surgery_date,
                DATEDIFF('day', sd.first_surgery_date,
                    TRY_CAST(ct.date_of_exam AS DATE)) as days_from_surgery,
                CASE
                    WHEN sd.first_surgery_date IS NULL THEN 'no_surgery_date'
                    WHEN TRY_CAST(ct.date_of_exam AS DATE) < sd.first_surgery_date
                        THEN 'preoperative'
                    WHEN DATEDIFF('day', sd.first_surgery_date,
                        TRY_CAST(ct.date_of_exam AS DATE)) < 30
                        THEN 'perioperative_0_30d'
                    WHEN DATEDIFF('day', sd.first_surgery_date,
                        TRY_CAST(ct.date_of_exam AS DATE)) < 365
                        THEN 'postop_30d_1yr'
                    ELSE 'postop_gt_1yr'
                END as timing_category,
                CASE WHEN DATEDIFF('day', sd.first_surgery_date,
                    TRY_CAST(ct.date_of_exam AS DATE)) >= 30 THEN 1 ELSE 0
                END as post_30d_flag,
                CASE WHEN DATEDIFF('day', sd.first_surgery_date,
                    TRY_CAST(ct.date_of_exam AS DATE)) >= 365 THEN 1 ELSE 0
                END as post_1yr_flag,
                ct.pathologic_lymph_nodes as ct_pathologic_ln_flag,
                ct.thyroid_nodule as ct_nodule_flag,
                ct.exam_type_normalized,
                ct.confidence as ct_confidence
            FROM ct_imaging ct
            LEFT JOIN surgery_dates sd ON ct.research_id = sd.research_id
            WHERE ct.date_of_exam IS NOT NULL
              AND TRY_CAST(ct.date_of_exam AS DATE) IS NOT NULL
        """)

    rows = con.execute("SELECT COUNT(*) FROM ct_imaging_surgery_timing").fetchone()[0]
    pts = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM ct_imaging_surgery_timing"
    ).fetchone()[0]
    print(f"  ct_imaging_surgery_timing: {rows} rows, {pts} patients")

    print("\n  Timing distribution:")
    print(con.execute("""
        SELECT timing_category, COUNT(*) as n,
            SUM(CASE WHEN ct_pathologic_ln_flag THEN 1 ELSE 0 END) as with_patho_ln
        FROM ct_imaging_surgery_timing
        GROUP BY timing_category ORDER BY n DESC
    """).df().to_string(index=False))

    # Phase 4: Create ptc_ct_imaging_events
    print("\nPhase 4: Creating ptc_ct_imaging_events table ...")
    if not args.dry_run:
        con.execute("DROP TABLE IF EXISTS ptc_ct_imaging_events")
        con.execute("""
            CREATE TABLE ptc_ct_imaging_events AS
            SELECT
                cst.research_id,
                cst.ct_column,
                cst.ct_date,
                cst.surgery_date,
                cst.days_from_surgery,
                cst.timing_category,
                cst.post_30d_flag,
                cst.post_1yr_flag,
                cst.ct_pathologic_ln_flag,
                cst.ct_nodule_flag,
                cst.exam_type_normalized,
                pc.histology_1_type,
                pc.overall_stage_ajcc8,
                pc.ln_positive,
                pc.ln_examined
            FROM ct_imaging_surgery_timing cst
            INNER JOIN ptc_cohort pc
                ON cst.research_id = CAST(pc.research_id AS VARCHAR)
        """)

    ptc_rows = con.execute("SELECT COUNT(*) FROM ptc_ct_imaging_events").fetchone()[0]
    ptc_pts = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM ptc_ct_imaging_events"
    ).fetchone()[0]
    print(f"  ptc_ct_imaging_events: {ptc_rows} rows, {ptc_pts} patients")

    # Phase 5: ANALYZE
    if not args.dry_run:
        print("\nPhase 5: Running ANALYZE ...")
        con.execute("ANALYZE ct_imaging")
        con.execute("ANALYZE ct_imaging_surgery_timing")
        con.execute("ANALYZE ptc_ct_imaging_events")
        print("  Done")

    con.close()
    print("\n=== All fixes deployed successfully ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true", help="Use MotherDuck")
    parser.add_argument("--local-db", default="thyroid_master.duckdb")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not args.md:
        args.md = True
    run(args)
