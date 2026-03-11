#!/usr/bin/env python3
"""
23_cross_domain_linkage_v2.py -- Cross-domain episode linkage

Links episodes across clinical domains using dates, laterality, temporal
windows, and confidence tiers.  Populates the NULL linkage columns created
by script 22 (canonical episode tables).

Linkage pairs:
  A. imaging_nodule -> fna_episode      (laterality + temporal window)
  B. fna_episode    -> molecular_test   (date proximity + specimen)
  C. fna/molecular  -> surgery_episode  (temporal ordering + laterality)
  D. surgery        -> pathology_tumor  (date + laterality)
  E. pathology      -> rai_episode      (post-surgery temporal window)
  F. imaging_exam   -> surgery_timeline (chronological ordering)

Confidence tiers:
  exact_match      -- same date AND same laterality
  high_confidence  -- date within 7 days OR same date different laterality
  plausible        -- date within 90 days AND compatible laterality
  weak             -- date within 365 days
  unlinked         -- no linkable counterpart found

Run after script 22.
Supports --md flag for MotherDuck deployment.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
PROCESSED = ROOT / "processed"

sys.path.insert(0, str(ROOT))


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def register_parquets(con: duckdb.DuckDBPyConnection) -> None:
    for tbl in ["path_synoptics", "tumor_pathology", "operative_details",
                "molecular_testing", "fna_history", "fna_cytology",
                "ultrasound_reports", "us_nodules_tirads", "ct_imaging",
                "mri_imaging", "nuclear_med", "note_entities_medications",
                "note_entities_procedures", "note_entities_genetics"]:
        pq = PROCESSED / f"{tbl}.parquet"
        if pq.exists() and not table_available(con, tbl):
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )


# ---------------------------------------------------------------------------
# A. Imaging nodule -> FNA episode
# ---------------------------------------------------------------------------
LINK_IMAGING_FNA_SQL = """
CREATE OR REPLACE TABLE imaging_fna_linkage_v2 AS
WITH img AS (
    SELECT research_id, nodule_id, imaging_exam_id, modality,
           exam_date_native, laterality, size_cm_max
    FROM imaging_nodule_long_v2
    WHERE exam_date_native IS NOT NULL
),
fna AS (
    SELECT research_id, fna_episode_id, fna_date_native, laterality AS fna_lat,
           bethesda_category, specimen_site_raw
    FROM fna_episode_master_v2
    WHERE fna_date_native IS NOT NULL
),
candidates AS (
    SELECT
        img.research_id,
        img.nodule_id,
        img.imaging_exam_id,
        fna.fna_episode_id,
        img.exam_date_native AS img_date,
        fna.fna_date_native AS fna_date,
        ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) AS day_gap,
        CASE
            WHEN img.laterality = fna.fna_lat THEN TRUE
            WHEN img.laterality IS NULL OR fna.fna_lat IS NULL THEN NULL
            ELSE FALSE
        END AS laterality_match,
        CASE
            WHEN img.exam_date_native = fna.fna_date_native
                 AND COALESCE(img.laterality = fna.fna_lat, TRUE) THEN 'exact_match'
            WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 7
                 THEN 'high_confidence'
            WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 90
                 AND COALESCE(img.laterality = fna.fna_lat, TRUE) THEN 'plausible'
            WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 365
                 THEN 'weak'
            ELSE 'unlinked'
        END AS linkage_confidence
    FROM img
    JOIN fna ON img.research_id = fna.research_id
)
SELECT * FROM candidates
WHERE linkage_confidence != 'unlinked'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY research_id, nodule_id
    ORDER BY CASE linkage_confidence
        WHEN 'exact_match' THEN 1
        WHEN 'high_confidence' THEN 2
        WHEN 'plausible' THEN 3
        WHEN 'weak' THEN 4
        ELSE 5
    END, day_gap
) = 1
"""

# ---------------------------------------------------------------------------
# B. FNA -> Molecular test
# ---------------------------------------------------------------------------
LINK_FNA_MOLECULAR_SQL = """
CREATE OR REPLACE TABLE fna_molecular_linkage_v2 AS
WITH fna AS (
    SELECT research_id, fna_episode_id, fna_date_native, laterality
    FROM fna_episode_master_v2
    WHERE fna_date_native IS NOT NULL
),
mol AS (
    SELECT research_id, molecular_episode_id, test_date_native, platform
    FROM molecular_test_episode_v2
    WHERE test_date_native IS NOT NULL
),
candidates AS (
    SELECT
        fna.research_id,
        fna.fna_episode_id,
        mol.molecular_episode_id,
        fna.fna_date_native,
        mol.test_date_native,
        DATEDIFF('day', fna.fna_date_native, mol.test_date_native) AS day_gap,
        CASE
            WHEN fna.fna_date_native = mol.test_date_native THEN 'exact_match'
            WHEN ABS(DATEDIFF('day', fna.fna_date_native, mol.test_date_native)) <= 14
                 AND DATEDIFF('day', fna.fna_date_native, mol.test_date_native) >= -7
                 THEN 'high_confidence'
            WHEN DATEDIFF('day', fna.fna_date_native, mol.test_date_native) BETWEEN 0 AND 90
                 THEN 'plausible'
            WHEN DATEDIFF('day', fna.fna_date_native, mol.test_date_native) BETWEEN -7 AND 180
                 THEN 'weak'
            ELSE 'unlinked'
        END AS linkage_confidence
    FROM fna
    JOIN mol ON fna.research_id = mol.research_id
)
SELECT * FROM candidates
WHERE linkage_confidence != 'unlinked'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY research_id, fna_episode_id
    ORDER BY CASE linkage_confidence
        WHEN 'exact_match' THEN 1
        WHEN 'high_confidence' THEN 2
        WHEN 'plausible' THEN 3
        WHEN 'weak' THEN 4
        ELSE 5
    END, ABS(day_gap)
) = 1
"""

# ---------------------------------------------------------------------------
# C. FNA/Molecular -> Surgery episode
# ---------------------------------------------------------------------------
LINK_PREOP_SURGERY_SQL = """
CREATE OR REPLACE TABLE preop_surgery_linkage_v2 AS
WITH preop AS (
    SELECT research_id, fna_episode_id AS preop_episode_id, 'fna' AS preop_type,
           fna_date_native AS preop_date, laterality
    FROM fna_episode_master_v2
    WHERE fna_date_native IS NOT NULL
    UNION ALL
    SELECT research_id, molecular_episode_id, 'molecular',
           test_date_native, NULL
    FROM molecular_test_episode_v2
    WHERE test_date_native IS NOT NULL
),
surg AS (
    SELECT research_id, surgery_episode_id, surgery_date_native, laterality AS surg_lat
    FROM operative_episode_detail_v2
    WHERE surgery_date_native IS NOT NULL
),
candidates AS (
    SELECT
        preop.research_id,
        preop.preop_episode_id,
        preop.preop_type,
        surg.surgery_episode_id,
        preop.preop_date,
        surg.surgery_date_native AS surgery_date,
        DATEDIFF('day', preop.preop_date, surg.surgery_date_native) AS day_gap,
        CASE
            WHEN preop.laterality = surg.surg_lat THEN TRUE
            WHEN preop.laterality IS NULL OR surg.surg_lat IS NULL THEN NULL
            ELSE FALSE
        END AS laterality_match,
        CASE
            WHEN DATEDIFF('day', preop.preop_date, surg.surgery_date_native) BETWEEN 0 AND 7
                 AND COALESCE(preop.laterality = surg.surg_lat, TRUE)
                 THEN 'exact_match'
            WHEN DATEDIFF('day', preop.preop_date, surg.surgery_date_native) BETWEEN 0 AND 30
                 THEN 'high_confidence'
            WHEN DATEDIFF('day', preop.preop_date, surg.surgery_date_native) BETWEEN 0 AND 180
                 THEN 'plausible'
            WHEN DATEDIFF('day', preop.preop_date, surg.surgery_date_native) BETWEEN -7 AND 365
                 THEN 'weak'
            ELSE 'unlinked'
        END AS linkage_confidence
    FROM preop
    JOIN surg ON preop.research_id = surg.research_id
)
SELECT * FROM candidates
WHERE linkage_confidence != 'unlinked'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY research_id, preop_episode_id, preop_type
    ORDER BY CASE linkage_confidence
        WHEN 'exact_match' THEN 1
        WHEN 'high_confidence' THEN 2
        WHEN 'plausible' THEN 3
        WHEN 'weak' THEN 4
        ELSE 5
    END, ABS(day_gap)
) = 1
"""

# ---------------------------------------------------------------------------
# D. Surgery -> Pathology tumor (via tumor_episode_master_v2)
# ---------------------------------------------------------------------------
LINK_SURGERY_PATHOLOGY_SQL = """
CREATE OR REPLACE TABLE surgery_pathology_linkage_v2 AS
SELECT
    o.research_id,
    o.surgery_episode_id,
    t.surgery_episode_id AS tumor_episode_id,
    t.tumor_ordinal,
    o.surgery_date_native,
    t.surgery_date AS tumor_surgery_date,
    CASE
        WHEN o.surgery_date_native = t.surgery_date THEN 'exact_match'
        WHEN o.surgery_date_native IS NULL OR t.surgery_date IS NULL THEN 'weak'
        ELSE 'plausible'
    END AS linkage_confidence,
    CASE
        WHEN o.laterality = t.laterality THEN TRUE
        WHEN o.laterality IS NULL OR t.laterality IS NULL THEN NULL
        ELSE FALSE
    END AS laterality_match
FROM operative_episode_detail_v2 o
JOIN tumor_episode_master_v2 t ON o.research_id = t.research_id
WHERE o.surgery_date_native = t.surgery_date
   OR o.surgery_date_native IS NULL
   OR t.surgery_date IS NULL
"""

# ---------------------------------------------------------------------------
# E. Pathology -> RAI episode
# ---------------------------------------------------------------------------
LINK_PATHOLOGY_RAI_SQL = """
CREATE OR REPLACE TABLE pathology_rai_linkage_v2 AS
WITH surg AS (
    SELECT research_id, surgery_episode_id,
           surgery_date AS surgery_date_val
    FROM tumor_episode_master_v2
),
rai AS (
    SELECT research_id, rai_episode_id, resolved_rai_date,
           rai_assertion_status
    FROM rai_treatment_episode_v2
    WHERE resolved_rai_date IS NOT NULL
),
candidates AS (
    SELECT
        surg.research_id,
        surg.surgery_episode_id,
        rai.rai_episode_id,
        surg.surgery_date_val,
        rai.resolved_rai_date,
        DATEDIFF('day', surg.surgery_date_val, rai.resolved_rai_date) AS days_surg_to_rai,
        CASE
            WHEN DATEDIFF('day', surg.surgery_date_val, rai.resolved_rai_date) BETWEEN 14 AND 180
                 THEN 'high_confidence'
            WHEN DATEDIFF('day', surg.surgery_date_val, rai.resolved_rai_date) BETWEEN 1 AND 365
                 THEN 'plausible'
            WHEN DATEDIFF('day', surg.surgery_date_val, rai.resolved_rai_date) < 0
                 AND rai.rai_assertion_status IN ('historical', 'ambiguous')
                 THEN 'weak'
            ELSE 'unlinked'
        END AS linkage_confidence
    FROM surg
    JOIN rai ON surg.research_id = rai.research_id
    WHERE surg.surgery_date_val IS NOT NULL
)
SELECT * FROM candidates
WHERE linkage_confidence != 'unlinked'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY research_id, rai_episode_id
    ORDER BY CASE linkage_confidence
        WHEN 'high_confidence' THEN 1
        WHEN 'plausible' THEN 2
        WHEN 'weak' THEN 3
        ELSE 4
    END, ABS(days_surg_to_rai)
) = 1
"""

# ---------------------------------------------------------------------------
# F. Linkage summary per patient
# ---------------------------------------------------------------------------
LINKAGE_SUMMARY_SQL = """
CREATE OR REPLACE TABLE linkage_summary_v2 AS
SELECT
    'imaging_fna' AS linkage_type,
    0 AS total_links,
    0 AS exact_ct,
    0 AS high_ct,
    0 AS plausible_ct,
    0 AS weak_ct
UNION ALL
SELECT 'fna_molecular', COUNT(*),
    COUNT(*) FILTER (WHERE linkage_confidence = 'exact_match'),
    COUNT(*) FILTER (WHERE linkage_confidence = 'high_confidence'),
    COUNT(*) FILTER (WHERE linkage_confidence = 'plausible'),
    COUNT(*) FILTER (WHERE linkage_confidence = 'weak')
FROM fna_molecular_linkage_v2
UNION ALL
SELECT 'preop_surgery', COUNT(*),
    COUNT(*) FILTER (WHERE linkage_confidence = 'exact_match'),
    COUNT(*) FILTER (WHERE linkage_confidence = 'high_confidence'),
    COUNT(*) FILTER (WHERE linkage_confidence = 'plausible'),
    COUNT(*) FILTER (WHERE linkage_confidence = 'weak')
FROM preop_surgery_linkage_v2
UNION ALL
SELECT 'surgery_pathology', COUNT(*),
    COUNT(*) FILTER (WHERE linkage_confidence = 'exact_match'),
    COUNT(*) FILTER (WHERE linkage_confidence IS NOT NULL),
    0, COUNT(*) FILTER (WHERE linkage_confidence = 'weak')
FROM surgery_pathology_linkage_v2
UNION ALL
SELECT 'pathology_rai', COUNT(*),
    0,
    COUNT(*) FILTER (WHERE linkage_confidence = 'high_confidence'),
    COUNT(*) FILTER (WHERE linkage_confidence = 'plausible'),
    COUNT(*) FILTER (WHERE linkage_confidence = 'weak')
FROM pathology_rai_linkage_v2
"""

ALL_LINKAGE_SQL = [
    ("imaging_fna_linkage_v2", LINK_IMAGING_FNA_SQL),
    ("fna_molecular_linkage_v2", LINK_FNA_MOLECULAR_SQL),
    ("preop_surgery_linkage_v2", LINK_PREOP_SURGERY_SQL),
    ("surgery_pathology_linkage_v2", LINK_SURGERY_PATHOLOGY_SQL),
    ("pathology_rai_linkage_v2", LINK_PATHOLOGY_RAI_SQL),
    ("linkage_summary_v2", LINKAGE_SUMMARY_SQL),
]


def build_linkage(con: duckdb.DuckDBPyConnection) -> None:
    for name, sql in ALL_LINKAGE_SQL:
        section(name)
        try:
            con.execute(sql)
            cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"  Created {name:<45} {cnt:>8,} rows")
        except Exception as e:
            print(f"  WARN: {name} skipped -- {e}")


def backfill_linked_ids(con: duckdb.DuckDBPyConnection) -> None:
    """Backfill linked episode IDs into canonical tables where linkage exists."""
    section("Backfill linked IDs into canonical tables")

    backfill_stmts = [
        (
            "fna_episode_master_v2.linked_molecular_episode_id",
            """UPDATE fna_episode_master_v2 f
               SET linked_molecular_episode_id = CAST(l.molecular_episode_id AS VARCHAR)
               FROM fna_molecular_linkage_v2 l
               WHERE f.research_id = l.research_id
                 AND f.fna_episode_id = l.fna_episode_id
                 AND f.linked_molecular_episode_id IS NULL""",
        ),
        (
            "fna_episode_master_v2.linked_surgery_episode_id",
            """UPDATE fna_episode_master_v2 f
               SET linked_surgery_episode_id = CAST(l.surgery_episode_id AS VARCHAR)
               FROM preop_surgery_linkage_v2 l
               WHERE f.research_id = l.research_id
                 AND CAST(f.fna_episode_id AS VARCHAR) = l.preop_episode_id
                 AND l.preop_type = 'fna'
                 AND f.linked_surgery_episode_id IS NULL""",
        ),
        (
            "molecular_test_episode_v2.linked_fna_episode_id",
            """UPDATE molecular_test_episode_v2 m
               SET linked_fna_episode_id = CAST(l.fna_episode_id AS VARCHAR)
               FROM fna_molecular_linkage_v2 l
               WHERE m.research_id = l.research_id
                 AND m.molecular_episode_id = l.molecular_episode_id
                 AND m.linked_fna_episode_id IS NULL""",
        ),
        (
            "molecular_test_episode_v2.linked_surgery_episode_id",
            """UPDATE molecular_test_episode_v2 m
               SET linked_surgery_episode_id = CAST(l.surgery_episode_id AS VARCHAR)
               FROM preop_surgery_linkage_v2 l
               WHERE m.research_id = l.research_id
                 AND CAST(m.molecular_episode_id AS VARCHAR) = l.preop_episode_id
                 AND l.preop_type = 'molecular'
                 AND m.linked_surgery_episode_id IS NULL""",
        ),
        (
            "rai_treatment_episode_v2.linked_surgery_episode_id",
            """UPDATE rai_treatment_episode_v2 r
               SET linked_surgery_episode_id = CAST(l.surgery_episode_id AS VARCHAR)
               FROM pathology_rai_linkage_v2 l
               WHERE r.research_id = l.research_id
                 AND r.rai_episode_id = l.rai_episode_id
                 AND r.linked_surgery_episode_id IS NULL""",
        ),
        (
            "imaging_nodule_long_v2.linked_fna_episode_id",
            """UPDATE imaging_nodule_long_v2 i
               SET linked_fna_episode_id = CAST(l.fna_episode_id AS VARCHAR)
               FROM imaging_fna_linkage_v2 l
               WHERE i.research_id = l.research_id
                 AND i.nodule_id = l.nodule_id
                 AND i.linked_fna_episode_id IS NULL""",
        ),
    ]

    for desc, sql in backfill_stmts:
        try:
            con.execute(sql)
            print(f"  Backfilled {desc}")
        except Exception as e:
            print(f"  WARN: {desc} -- {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true",
                        help="Deploy to MotherDuck")
    args = parser.parse_args()

    section("23 -- Cross-Domain Linkage v2")

    if args.md:
        try:
            from motherduck_client import MotherDuckClient, MotherDuckConfig
            cfg = MotherDuckConfig(database="thyroid_research_2026")
            client = MotherDuckClient(cfg)
            con = client.connect_rw()
            print("  Connected to MotherDuck (RW)")
        except Exception as e:
            print(f"  MotherDuck unavailable: {e}")
            con = duckdb.connect(str(DB_PATH))
    else:
        con = duckdb.connect(str(DB_PATH))
        print(f"  Using local DuckDB: {DB_PATH}")

    register_parquets(con)

    for tbl in ["tumor_episode_master_v2", "molecular_test_episode_v2",
                "rai_treatment_episode_v2", "imaging_nodule_long_v2",
                "operative_episode_detail_v2", "fna_episode_master_v2"]:
        if not table_available(con, tbl):
            print(f"\n  ERROR: {tbl} not found. Run script 22 first.")
            sys.exit(1)

    build_linkage(con)
    backfill_linked_ids(con)

    section("Linkage Summary")
    try:
        rows = con.execute("SELECT * FROM linkage_summary_v2").fetchall()
        print(f"  {'Type':<25} {'Total':>8} {'Exact':>8} {'High':>8} {'Plaus':>8} {'Weak':>8}")
        print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
        for r in rows:
            print(f"  {r[0]:<25} {r[1]:>8,} {r[2]:>8,} {r[3]:>8,} {r[4]:>8,} {r[5]:>8,}")
    except Exception:
        pass

    con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
