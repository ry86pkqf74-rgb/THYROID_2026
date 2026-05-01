-- Lane M mig_234 — Table 4 recurrence (semantic safe view = non-quarantined) + survival SSOT join

USE thyroid_canonical_publication_v1_0;

SELECT
  r.research_id,
  r.release_id AS recurrence_release_id,
  r.first_surg_date,
  COALESCE(r.recurrence_path_proven, FALSE) AS recurrence_path_proven,
  r.recurrence_path_proven_date,
  COALESCE(r.recurrence_imaging_suspicious, FALSE) AS recurrence_imaging_suspicious,
  r.recurrence_status_final,
  s.first_surgery_date AS survival_first_surgery_date,
  s.last_known_alive_date,
  s.vital_status_current,
  s.days_from_first_surgery_to_last_contact,
  COALESCE(s.followup_complete_at_5yr, FALSE) AS followup_complete_at_5yr,
  COALESCE(s.followup_complete_at_10yr, FALSE) AS followup_complete_at_10yr
FROM semantic_publication.vw_recurrence_safe_VIEW_v1 AS r
LEFT JOIN main.canonical_survival_followup_v1 AS s
  ON CAST(r.research_id AS VARCHAR) = CAST(s.research_id AS VARCHAR);
