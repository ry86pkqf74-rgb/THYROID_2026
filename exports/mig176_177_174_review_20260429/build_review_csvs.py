"""
Build CSVs for Logan's mig_176/177/174 ratification review.

mig_176 outputs:
  * mig176_v2_only_166pts_with_us_source.csv (166 rows; all V2_MATCHES_US_SOURCE confirmed)
  * mig176_extreme_mismatches_v2_implausible.csv (top by abs_diff with US note text)

mig_177 outputs:
  * mig177_lvi_pm_t_event_f_with_evidence.csv (~2,614; PM has TRUE but no event-present row)
  * mig177_lvi_rollup_only_positives.csv (120; rollup=T but event=F, with sub-disposition)

Connects to MotherDuck via DuckDB. Uses logan.glosser.eras@gmail.com SSO if needed.
"""

import duckdb
import csv
import os
import sys

OUTDIR = os.path.dirname(os.path.abspath(__file__))

con = duckdb.connect()
con.sql("INSTALL motherduck; LOAD motherduck;")
con.sql("USE thyroid_canonical_publication_v1_0;")


def write_query_to_csv(name, sql, conn=con):
    rel = conn.sql(sql)
    cols = rel.columns
    rows = rel.fetchall()
    path = os.path.join(OUTDIR, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(cols)
        for r in rows:
            w.writerow([("" if v is None else v) for v in r])
    print(f"WROTE {path} ({len(rows)} rows)")


# ===== mig_176 =====

# 166 v2-only patients with US source verification
write_query_to_csv(
    "mig176_v2_only_166pts_with_us_source.csv",
    """
    WITH v2_only AS (
      SELECT CAST(research_id AS BIGINT) AS rid,
             dominant_nodule_size_cm AS v1, dominant_nodule_size_cm_v2 AS v2
      FROM main.canonical_patient_master
      WHERE dominant_nodule_size_cm IS NULL AND dominant_nodule_size_cm_v2 IS NOT NULL
    ),
    nodule_ev AS (
      SELECT CAST(n.research_id AS BIGINT) AS rid,
             MAX(n.size_cm_max)::DOUBLE AS us_max_size,
             STRING_AGG(DISTINCT n.location_raw, ' | ') AS locations,
             STRING_AGG(DISTINCT n.acr2017_tirads_category, '/') AS tirads,
             COUNT(*) AS n_nodules,
             MIN(n.exam_date) AS first_exam, MAX(n.exam_date) AS last_exam,
             BOOL_OR(n.suspicious_flag) AS any_suspicious
      FROM main.canonical_us_nodule_v2 n WHERE n.is_aggregate_row=FALSE GROUP BY 1
    ),
    imaging_check AS (
      SELECT CAST(iem.research_id AS BIGINT) AS rid,
             MAX(iem.largest_nodule_cm) AS imaging_max_nodule_cm
      FROM main.imaging_exam_master_v1 iem GROUP BY 1
    )
    SELECT v.rid AS research_id,
           v.v2 AS v2_size_cm,
           ne.us_max_size,
           ne.imaging_max_nodule_cm,
           ic.imaging_max_nodule_cm AS imaging_max_check,
           ne.locations,
           ne.tirads,
           ne.n_nodules,
           ne.first_exam, ne.last_exam,
           ne.any_suspicious,
           CASE WHEN ne.us_max_size IS NULL THEN 'NO_US_SOURCE_FOR_VERIFICATION'
                WHEN ABS(ne.us_max_size - v.v2) < 0.05 THEN 'V2_MATCHES_US_SOURCE'
                ELSE 'V2_DRIFTS_FROM_US_SOURCE' END AS verification_status
    FROM v2_only v
    LEFT JOIN nodule_ev ne USING (rid)
    LEFT JOIN imaging_check ic USING (rid)
    ORDER BY verification_status, v.rid
    """,
)

# Extreme mismatches: v2 > 10 cm OR abs_diff > 5 cm — full list with US note text
write_query_to_csv(
    "mig176_extreme_mismatches_v2_implausible.csv",
    """
    WITH mm AS (
      SELECT CAST(research_id AS BIGINT) AS rid,
             dominant_nodule_size_cm AS v1, dominant_nodule_size_cm_v2 AS v2,
             ABS(dominant_nodule_size_cm - dominant_nodule_size_cm_v2) AS abs_diff
      FROM main.canonical_patient_master
      WHERE dominant_nodule_size_cm IS NOT NULL AND dominant_nodule_size_cm_v2 IS NOT NULL
        AND dominant_nodule_size_cm IS DISTINCT FROM dominant_nodule_size_cm_v2
    ),
    nodule_ev AS (
      SELECT CAST(n.research_id AS BIGINT) AS rid,
             MAX(n.size_cm_max)::DOUBLE AS us_v2_max,
             COUNT(*) AS n_us_nodules,
             STRING_AGG(DISTINCT n.location_raw, ' | ') AS us_locations,
             STRING_AGG(DISTINCT n.acr2017_tirads_category, '/') AS us_tirads
      FROM main.canonical_us_nodule_v2 n WHERE n.is_aggregate_row=FALSE GROUP BY 1
    ),
    imaging_check AS (
      SELECT CAST(iem.research_id AS BIGINT) AS rid,
             MAX(iem.largest_nodule_cm) AS imaging_v1_max
      FROM main.imaging_exam_master_v1 iem GROUP BY 1
    ),
    rad_notes AS (
      SELECT CAST(cn.research_id AS BIGINT) AS rid,
             STRING_AGG(SUBSTRING(cn.note_text, 1, 1500), E'\\n----\\n')
               FILTER (WHERE cn.note_type ILIKE '%us%' OR cn.note_type ILIKE '%ultrasound%'
                            OR cn.note_type ILIKE '%radiology%' OR cn.note_type ILIKE '%imaging%') AS rad_note_excerpts
      FROM main.clinical_notes_long cn GROUP BY 1
    )
    SELECT m.rid AS research_id, m.v1 AS v1_size_cm, m.v2 AS v2_size_cm, m.abs_diff,
           ne.us_v2_max, ic.imaging_v1_max, ne.n_us_nodules, ne.us_locations, ne.us_tirads,
           CASE WHEN ABS(ne.us_v2_max - m.v2) < 0.05 THEN 'V2_REPLAYS_FROM_US' ELSE 'V2_DRIFT' END AS v2_replay,
           CASE WHEN ABS(ic.imaging_v1_max - m.v1) < 0.05 THEN 'V1_REPLAYS_FROM_IMAGING' ELSE 'V1_DRIFT' END AS v1_replay,
           rn.rad_note_excerpts
    FROM mm m
    LEFT JOIN nodule_ev ne USING (rid)
    LEFT JOIN imaging_check ic USING (rid)
    LEFT JOIN rad_notes rn USING (rid)
    WHERE m.v2 > 10.0 OR m.abs_diff > 5.0
    ORDER BY m.abs_diff DESC
    """,
)

# ===== mig_177 LVI =====

# Full list of PM=T but no event-present row (the 2,614 patients)
write_query_to_csv(
    "mig177_lvi_pm_t_event_f_with_evidence.csv",
    """
    WITH pm_lvi AS (
      SELECT CAST(research_id AS VARCHAR) AS rid,
             lvi_any_present_path,
             lvi_grade,
             lvi_ordinal_worst
      FROM main.canonical_patient_master
      WHERE lvi_any_present_path = TRUE
    ),
    ev_lvi AS (
      SELECT CAST(research_id AS VARCHAR) AS rid,
             BOOL_OR(invasion_type='lymphatic_microscopic' AND finding_status='present') AS ev_lvi_present,
             STRING_AGG(DISTINCT
               CASE WHEN invasion_type='lymphatic_microscopic'
                    THEN finding_status ELSE NULL END, '/'
             ) AS ev_lvi_statuses
      FROM main.canonical_invasion_events_v1
      GROUP BY 1
    ),
    rollup_lvi AS (
      SELECT CAST(research_id AS VARCHAR) AS rid,
             any_lymphatic_microscopic_anywhere AS rollup_lvi_anywhere
      FROM main.canonical_invasion_patient_rollup_v1
    ),
    path_lvi AS (
      SELECT CAST(research_id AS VARCHAR) AS rid,
             STRING_AGG(DISTINCT lymphatic_invasion, ' | ') AS path_event_lvi_raw,
             STRING_AGG(DISTINCT vascular_invasion, ' | ') AS path_event_vasc_raw,
             COUNT(*) AS n_path_events
      FROM main.canonical_path_malignant_events_v1
      GROUP BY 1
    ),
    syn_lvi AS (
      SELECT CAST(research_id AS VARCHAR) AS rid,
             STRING_AGG(DISTINCT
               COALESCE(tumor_1_lymphatic_invasion,'')||'/'||
               COALESCE(tumor_2_lymphatic_invasion,'')||'/'||
               COALESCE(tumor_3_lymphatic_invasion,'')||'/'||
               COALESCE(tumor_4_lymphatic_invasion,'')||'/'||
               COALESCE(tumor_5_lymphatic_invasion,''), ' | ') AS syn_lvi_raw,
             STRING_AGG(DISTINCT
               COALESCE(tumor_1_angioinvasion,'')||'/'||
               COALESCE(tumor_2_angioinvasion,'')||'/'||
               COALESCE(tumor_3_angioinvasion,''), ' | ') AS syn_angio_raw
      FROM main.path_synoptics
      GROUP BY 1
    )
    SELECT pm.rid AS research_id,
           pm.lvi_any_present_path AS pm_lvi_present,
           pm.lvi_grade AS pm_lvi_grade,
           pm.lvi_ordinal_worst AS pm_lvi_ordinal,
           COALESCE(ev.ev_lvi_present, FALSE) AS ev_lvi_present,
           ev.ev_lvi_statuses,
           ru.rollup_lvi_anywhere,
           pl.path_event_lvi_raw,
           pl.path_event_vasc_raw,
           pl.n_path_events,
           sy.syn_lvi_raw,
           sy.syn_angio_raw
    FROM pm_lvi pm
    LEFT JOIN ev_lvi ev USING (rid)
    LEFT JOIN rollup_lvi ru USING (rid)
    LEFT JOIN path_lvi pl USING (rid)
    LEFT JOIN syn_lvi sy USING (rid)
    WHERE COALESCE(ev.ev_lvi_present, FALSE) = FALSE
    ORDER BY CAST(pm.rid AS BIGINT)
    """,
)

# Rollup-only positives (120) split by sub-disposition
write_query_to_csv(
    "mig177_lvi_rollup_only_positives.csv",
    """
    WITH ev_lvi AS (
      SELECT CAST(research_id AS VARCHAR) AS rid,
             BOOL_OR(invasion_type='lymphatic_microscopic' AND finding_status='present') AS ev_lvi_present,
             BOOL_OR(invasion_type='lymphatic_microscopic' AND finding_status='absent') AS ev_lvi_absent,
             BOOL_OR(invasion_type='lymphatic_microscopic' AND finding_status='indeterminate') AS ev_lvi_indet,
             COUNT(*) FILTER (WHERE invasion_type='lymphatic_microscopic') AS n_lvi_events,
             STRING_AGG(DISTINCT CASE WHEN invasion_type='lymphatic_microscopic' THEN finding_status END, '/') AS lvi_statuses
      FROM main.canonical_invasion_events_v1 GROUP BY 1
    ),
    rollup AS (
      SELECT CAST(research_id AS VARCHAR) AS rid,
             any_lymphatic_microscopic_anywhere AS rollup_lvi_anywhere
      FROM main.canonical_invasion_patient_rollup_v1
      WHERE any_lymphatic_microscopic_anywhere = TRUE
    ),
    pm AS (
      SELECT CAST(research_id AS VARCHAR) AS rid,
             lvi_any_present_path AS pm_lvi_present,
             lvi_grade AS pm_lvi_grade
      FROM main.canonical_patient_master
    ),
    path AS (
      SELECT CAST(research_id AS VARCHAR) AS rid,
             STRING_AGG(DISTINCT lymphatic_invasion, ' | ') AS path_lvi_raw,
             COUNT(*) AS n_path_events
      FROM main.canonical_path_malignant_events_v1 GROUP BY 1
    ),
    syn AS (
      SELECT CAST(research_id AS VARCHAR) AS rid,
             STRING_AGG(DISTINCT
               COALESCE(tumor_1_lymphatic_invasion,'')||' | '||
               COALESCE(tumor_2_lymphatic_invasion,''), ' || ') AS syn_lvi_raw
      FROM main.path_synoptics GROUP BY 1
    )
    SELECT r.rid AS research_id,
           r.rollup_lvi_anywhere,
           pm.pm_lvi_present, pm.pm_lvi_grade,
           ev.n_lvi_events, ev.lvi_statuses,
           CASE
             WHEN ev.n_lvi_events IS NULL OR ev.n_lvi_events=0 THEN 'NO_EVENT_ROWS'
             WHEN ev.ev_lvi_absent AND NOT ev.ev_lvi_present AND NOT ev.ev_lvi_indet THEN 'ABSENT_ONLY'
             WHEN ev.ev_lvi_indet AND NOT ev.ev_lvi_present THEN 'INDETERMINATE_ONLY'
             ELSE 'OTHER'
           END AS sub_disposition,
           pa.path_lvi_raw,
           pa.n_path_events,
           sy.syn_lvi_raw
    FROM rollup r
    LEFT JOIN ev_lvi ev USING (rid)
    LEFT JOIN pm USING (rid)
    LEFT JOIN path pa USING (rid)
    LEFT JOIN syn sy USING (rid)
    WHERE COALESCE(ev.ev_lvi_present, FALSE) = FALSE
    ORDER BY sub_disposition, CAST(r.rid AS BIGINT)
    """,
)

print("Done.")
