"""Probe per-patient + laterality + temporal match strategy for nodule-level path labels."""
from __future__ import annotations
from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
client = bigquery.Client(project=PROJECT)


def q(sql: str, label: str) -> None:
    print(f"\n=== {label} ===")
    rows = list(client.query(sql).result())
    for r in rows:
        print("  ", dict(r))


# 1. Laterality coverage on canonical_us_nodule_v2
q(
    """
    SELECT laterality, COUNT(*) n
    FROM `pub_canonical.canonical_us_nodule_v2`
    GROUP BY 1 ORDER BY n DESC
    """,
    "1. canonical_us_nodule_v2 laterality distribution",
)

# 2. Laterality coverage on path malignant
q(
    """
    SELECT laterality, COUNT(*) n
    FROM `pub_canonical.canonical_path_malignant_events_v1`
    GROUP BY 1 ORDER BY n DESC
    """,
    "2. canonical_path_malignant_events_v1 laterality distribution",
)

# 3. Laterality coverage on path benign
q(
    """
    SELECT
      COUNT(*) n,
      COUNT(DISTINCT research_id) n_patients,
      MIN(path_date) min_dt, MAX(path_date) max_dt
    FROM `pub_canonical.canonical_path_benign_events_v1`
    """,
    "3. canonical_path_benign_events_v1 stats (no laterality column)",
)

# 4. Multi-tumor-per-patient cases — does laterality differ between malignant tumors?
q(
    """
    WITH per_patient AS (
      SELECT
        research_id, surgery_episode_id,
        COUNT(*) n_tumors,
        COUNT(DISTINCT laterality) n_distinct_lat,
        STRING_AGG(DISTINCT laterality, '|' ORDER BY laterality) lats
      FROM `pub_canonical.canonical_path_malignant_events_v1`
      GROUP BY 1,2
    )
    SELECT n_tumors, n_distinct_lat, COUNT(*) n_episodes
    FROM per_patient
    GROUP BY 1,2 ORDER BY 1,2
    """,
    "4. Multi-tumor episodes: how many have distinct laterality?",
)

# 5. Test the join: nodule -> path within ±90d, with laterality match
q(
    """
    WITH nodules AS (
      SELECT research_id, nodule_id, exam_date, laterality, size_cm_max
      FROM `pub_canonical.canonical_us_nodule_v2`
      WHERE laterality IS NOT NULL AND laterality != 'unknown'
    ),
    -- Per-laterality malignancy verdict per patient per surgery window
    mal_lat AS (
      SELECT
        research_id, surgery_date, laterality,
        COUNT(*) n_mal_tumors,
        ARRAY_AGG(specimen_id IGNORE NULLS) specimens
      FROM `pub_canonical.canonical_path_malignant_events_v1`
      WHERE surgery_date IS NOT NULL AND laterality IS NOT NULL
      GROUP BY 1,2,3
    ),
    ben_lat AS (
      SELECT
        b.research_id, b.path_date,
        -- benign laterality not directly available; use specimen_master if present
        COUNT(*) n_ben
      FROM `pub_canonical.canonical_path_benign_events_v1` b
      WHERE b.path_date IS NOT NULL
      GROUP BY 1,2
    ),
    nodule_x_mal AS (
      SELECT
        n.nodule_id, n.research_id, n.exam_date, n.laterality,
        m.surgery_date, m.laterality AS mal_lat, m.n_mal_tumors,
        ABS(DATE_DIFF(m.surgery_date, n.exam_date, DAY)) AS day_gap
      FROM nodules n
      LEFT JOIN mal_lat m
        ON m.research_id = n.research_id
       AND ABS(DATE_DIFF(m.surgery_date, n.exam_date, DAY)) <= 365
       AND ( -- laterality compatibility:
         m.laterality = n.laterality
         OR m.laterality IN ('bilateral', 'both')
         OR n.laterality IN ('bilateral', 'both')
       )
    )
    SELECT
      COUNT(DISTINCT nodule_id) n_nodules_total,
      COUNTIF(surgery_date IS NOT NULL) n_with_lateralized_mal_match,
      COUNT(DISTINCT IF(surgery_date IS NOT NULL, nodule_id, NULL)) n_distinct_nodules_with_mal
    FROM nodule_x_mal
    """,
    "5. Per-laterality nodule-malignancy match within 365d",
)

# 6. Sample a multinodular goiter case to confirm laterality split works
q(
    """
    WITH lat_mal_per_patient AS (
      SELECT
        research_id,
        STRING_AGG(DISTINCT laterality ORDER BY laterality) lats_with_malignancy,
        COUNT(DISTINCT laterality) n_lats_mal
      FROM `pub_canonical.canonical_path_malignant_events_v1`
      WHERE laterality IN ('left','right','isthmus','bilateral','both')
      GROUP BY 1
    )
    SELECT n_lats_mal, lats_with_malignancy, COUNT(*) n_patients
    FROM lat_mal_per_patient
    GROUP BY 1,2 ORDER BY 1,2
    """,
    "6. Per-patient: how many have multi-laterality malignancy?",
)

# 7. Laterality-only nodule split, by side, per patient
q(
    """
    WITH nodule_lat_per_patient AS (
      SELECT
        research_id, exam_date,
        STRING_AGG(DISTINCT laterality ORDER BY laterality) all_lats,
        COUNT(DISTINCT laterality) n_distinct_lats
      FROM `pub_canonical.canonical_us_nodule_v2`
      WHERE laterality IN ('left','right','isthmus')
      GROUP BY 1,2
    )
    SELECT n_distinct_lats, COUNT(*) n_exams
    FROM nodule_lat_per_patient
    GROUP BY 1 ORDER BY 1
    """,
    "7. Per-patient-per-exam: nodules across how many sides?",
)
