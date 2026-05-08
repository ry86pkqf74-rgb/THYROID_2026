-- Phase B.6 Step 3b — Nodule-level pathology outcome view
-- Replaces the patient-level path linkage that broke the cohort refit AUC.
--
-- Strategy (since linked_pathology_tumor_id is 0% populated and specimen_source_xref
-- has no FNA domain):
--   1. Normalize laterality on both nodule and path-malignant sides.
--   2. Per (research_id, surgery_window):
--        - If any malignancy with laterality matching the nodule's side → malignant
--        - Bilateral/'both' malignancy is treated as covering all real sides
--          (clinical reality: bilateral diagnosis = both lobes affected).
--        - Else if benign path event exists in window AND no matching-laterality
--          malignancy → benign (patient was operated; nothing malignant linked to
--          this nodule's laterality)
--        - Else → unlabeled (NULL)
--   3. Window: -90 to +365 days from US exam (asymmetric — US precedes path).
--
-- Acceptance gates (from prompt §3b):
--   * n_with_label >= 8,000
--   * frac_clean_singleton_link >= 0.60   (proxy: n_path_specimens_linked = 1)
--   * Multi-nodular goiter sanity: patients with both benign- and malignant-labeled
--     nodules on the same exam_date — must be > 0, target >= a few hundred.

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.us_nodule_path_outcome_v1` AS
WITH
-- ---------- 1. Normalized laterality on US nodule side ----------
nodule_lat AS (
  SELECT
    n.research_id,
    n.nodule_id,
    n.exam_date AS us_exam_date,
    n.laterality AS laterality_raw,
    CASE
      WHEN LOWER(IFNULL(n.laterality, '')) IN ('left', 'left lobe', 'left lobe.', 'left.', 'l') THEN 'left'
      WHEN STARTS_WITH(LOWER(IFNULL(n.laterality, '')), 'left') THEN 'left'
      WHEN LOWER(IFNULL(n.laterality, '')) IN ('right', 'right lobe', 'right lobe.', 'right.', 'r') THEN 'right'
      WHEN STARTS_WITH(LOWER(IFNULL(n.laterality, '')), 'right') THEN 'right'
      WHEN LOWER(IFNULL(n.laterality, '')) IN ('isthmus', 'isthmus.') THEN 'isthmus'
      WHEN LOWER(IFNULL(n.laterality, '')) IN ('bilateral', 'both', 'left and right', 'right and left') THEN 'bilateral'
      ELSE 'unknown'
    END AS lat_norm
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2` n
),

-- ---------- 2. Normalized laterality on path-malignant side ----------
path_mal_lat AS (
  SELECT
    p.research_id,
    p.surgery_episode_id,
    p.tumor_ordinal,
    p.surgery_date,
    p.specimen_id,
    p.specimen_focus_id,
    p.laterality AS laterality_raw,
    CASE
      WHEN LOWER(IFNULL(p.laterality, '')) IN ('left', 'left lobe', 'left lobectomy', 'left mid lobe', 'l') THEN 'left'
      WHEN STARTS_WITH(LOWER(IFNULL(p.laterality, '')), 'left') AND NOT REGEXP_CONTAINS(LOWER(IFNULL(p.laterality, '')), r'\bright\b') THEN 'left'
      WHEN LOWER(IFNULL(p.laterality, '')) IN ('right', 'right lobe', 'r') THEN 'right'
      WHEN STARTS_WITH(LOWER(IFNULL(p.laterality, '')), 'right') AND NOT REGEXP_CONTAINS(LOWER(IFNULL(p.laterality, '')), r'\bleft\b') THEN 'right'
      WHEN LOWER(IFNULL(p.laterality, '')) IN ('isthmus') THEN 'isthmus'
      WHEN LOWER(IFNULL(p.laterality, '')) IN ('bilateral', 'both') THEN 'bilateral'
      WHEN REGEXP_CONTAINS(LOWER(IFNULL(p.laterality, '')), r'\bleft\b') AND REGEXP_CONTAINS(LOWER(IFNULL(p.laterality, '')), r'\bright\b') THEN 'bilateral'
      ELSE 'unknown'
    END AS path_lat_norm
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_malignant_events_v1` p
  WHERE p.surgery_date IS NOT NULL
),

-- ---------- 3. Benign path events per patient (no laterality available) ----------
path_ben AS (
  SELECT
    research_id,
    path_date AS surgery_date,
    specimen_id,
    'benign' AS evt_type
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_benign_events_v1`
  WHERE path_date IS NOT NULL
),

-- ---------- 4. For each nodule, find linked malignant events ----------
nodule_mal_links AS (
  SELECT
    n.nodule_id,
    n.research_id,
    n.us_exam_date,
    n.lat_norm,
    p.surgery_date,
    p.path_lat_norm,
    p.specimen_id,
    p.specimen_focus_id,
    DATE_DIFF(p.surgery_date, n.us_exam_date, DAY) AS day_gap,
    -- Laterality compatibility: nodule side matches path side, OR either is bilateral,
    -- OR nodule is isthmus and path is the same lobe-side (skip — keep strict).
    CASE
      WHEN p.path_lat_norm = n.lat_norm THEN TRUE
      WHEN p.path_lat_norm = 'bilateral' AND n.lat_norm IN ('left', 'right', 'isthmus') THEN TRUE
      WHEN n.lat_norm = 'bilateral' AND p.path_lat_norm IN ('left', 'right', 'isthmus', 'bilateral') THEN TRUE
      ELSE FALSE
    END AS laterality_compatible
  FROM nodule_lat n
  INNER JOIN path_mal_lat p
    ON p.research_id = n.research_id
   AND DATE_DIFF(p.surgery_date, n.us_exam_date, DAY) BETWEEN -90 AND 365
),

-- ---------- 5. Aggregate to per-nodule label ----------
nodule_mal_agg AS (
  SELECT
    nodule_id,
    research_id,
    us_exam_date,
    lat_norm,
    -- Per the strict rule: malignant only if a laterality-compatible match exists.
    LOGICAL_OR(laterality_compatible) AS has_compatible_mal,
    LOGICAL_OR(NOT laterality_compatible) AS has_other_lat_mal,
    COUNT(DISTINCT IF(laterality_compatible, specimen_id, NULL)) AS n_path_specimens_linked,
    MIN(IF(laterality_compatible, surgery_date, NULL)) AS first_compatible_surgery_date,
    STRING_AGG(DISTINCT IF(laterality_compatible, path_lat_norm, NULL), '|' ORDER BY IF(laterality_compatible, path_lat_norm, NULL)) AS compatible_path_lats
  FROM nodule_mal_links
  GROUP BY 1,2,3,4
),

-- ---------- 6. Benign path within window per patient ----------
patient_ben_in_window AS (
  SELECT DISTINCT
    n.nodule_id,
    n.research_id,
    n.us_exam_date,
    TRUE AS has_benign_in_window
  FROM nodule_lat n
  INNER JOIN path_ben b
    ON b.research_id = n.research_id
   AND DATE_DIFF(b.surgery_date, n.us_exam_date, DAY) BETWEEN -90 AND 365
)

-- ---------- 7. Final per-nodule label ----------
SELECT
  n.research_id,
  n.nodule_id,
  n.us_exam_date,
  n.laterality_raw,
  n.lat_norm,
  -- Did this patient have ANY malignant path event in the window (regardless of laterality)?
  -- Used to distinguish "operated, all benign" from "not operated".
  EXISTS(
    SELECT 1 FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_malignant_events_v1` p
    WHERE p.research_id = n.research_id
      AND p.surgery_date IS NOT NULL
      AND DATE_DIFF(p.surgery_date, n.us_exam_date, DAY) BETWEEN -90 AND 365
  ) AS patient_has_any_mal_in_window,
  COALESCE(b.has_benign_in_window, FALSE) AS has_benign_in_window,
  COALESCE(m.has_compatible_mal, FALSE) AS has_lat_compatible_mal,
  COALESCE(m.has_other_lat_mal, FALSE) AS has_other_lat_mal,
  m.n_path_specimens_linked,
  m.first_compatible_surgery_date AS path_event_date,
  m.compatible_path_lats,
  -- Label rules (revised for cohort-refit training corpus):
  --   1 = malignant: laterality-compatible malignant event exists
  --   0 = benign: patient was operated in window (any path event) AND no laterality-
  --             compatible malignancy attached to this nodule. This includes the
  --             "contralateral nodule of a unilateral cancer" case — clinically the
  --             contralateral lobe may or may not have been resected, but for training
  --             purposes the absence of documented same-side malignancy is the
  --             strongest available negative signal.
  --   NULL = unlabeled: no path activity in window at all (patient never operated, or
  --             operation was outside the ±90/+365d window).
  CASE
    WHEN COALESCE(m.has_compatible_mal, FALSE) THEN 1
    WHEN COALESCE(b.has_benign_in_window, FALSE) THEN 0
    WHEN COALESCE(m.has_other_lat_mal, FALSE) AND NOT COALESCE(m.has_compatible_mal, FALSE) THEN 0
    ELSE NULL
  END AS nodule_path_malignant,
  CASE
    WHEN COALESCE(m.has_compatible_mal, FALSE) THEN TRUE
    WHEN COALESCE(b.has_benign_in_window, FALSE) THEN TRUE
    WHEN COALESCE(m.has_other_lat_mal, FALSE) AND NOT COALESCE(m.has_compatible_mal, FALSE) THEN TRUE
    ELSE FALSE
  END AS path_label_present,
  -- Linkage method label (audit)
  CASE
    WHEN COALESCE(m.has_compatible_mal, FALSE) THEN
      CONCAT('malignant_via_laterality_match:', m.compatible_path_lats)
    WHEN COALESCE(b.has_benign_in_window, FALSE)
         AND NOT COALESCE(m.has_other_lat_mal, FALSE) THEN
      'benign_via_patient_benign_in_window'
    WHEN COALESCE(m.has_other_lat_mal, FALSE) AND NOT COALESCE(m.has_compatible_mal, FALSE) THEN
      'benign_via_contralateral_malignancy_only'
    WHEN COALESCE(b.has_benign_in_window, FALSE) THEN
      'benign_via_patient_benign_in_window'
    ELSE 'unlabeled_no_path_in_window'
  END AS linkage_method
FROM nodule_lat n
LEFT JOIN nodule_mal_agg m USING (nodule_id, research_id, us_exam_date, lat_norm)
LEFT JOIN patient_ben_in_window b USING (nodule_id, research_id, us_exam_date)
;
