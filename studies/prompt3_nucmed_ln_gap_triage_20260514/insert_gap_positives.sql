-- Prompt 3 triage positives (2 patients) → pub_canonical.canonical_nucmed_lymph_node_v1
-- Run via: bq query --use_legacy_sql=false --project_id=thyroid-canonical-pub-2026 < insert_gap_positives.sql

INSERT INTO `thyroid-canonical-pub-2026.pub_canonical.canonical_nucmed_lymph_node_v1`
(
  research_id, exam_id, exam_date, ln_index_within_exam, ln_id, source_modality,
  laterality, neck_level, neck_level_subdivision, region,
  size_short_mm, size_long_mm, size_max_mm, size_short_long_ratio,
  shape, echogenicity, hilum_preserved, cortex_thickness,
  necrosis_present, matting, conglomerate, calcifications, cystic_component,
  extranodal_extension, margins, suspicious_flag, suspicion_level,
  evidence_text, source_note_type, source_report_id, llm_model, confidence,
  extracted_at, nlp_backfill_pending, radiotracer, uptake_present, uptake_intensity,
  distinguished_from_thyroid_bed, spect_ct_localization
)
VALUES
(
  '10342', '0267cbe95959e63890c1c9ebcf50535d', DATE '2024-06-06', 1, '56ad051281494dccc3008fa35d620eb5', 'NUCMED',
  NULL, NULL, NULL, 'cervical',
  NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, TRUE, 'suspicious',
  'whole body planar images demonstrate expected focal uptake in the thyroid bedresidual remnant with 2 additional focal areas of increased uptake anterior to the residual remnant, likely representing uptake in neck nodes. there is otherwise normal biodistribution of the radiotracer. physiologic diffuse liver activity is ',
  'nuclear_med_gap_triage', 'nucmed_legacy:10342:3', 'regex_triage_prompt3_20260514', 0.55,
  TIMESTAMP '2026-05-14T09:37:57Z', TRUE, 'I-131_or_I-123', TRUE, NULL,
  NULL, FALSE
),
(
  '9038', 'f571d53da70eb6b1de2eaccdfb645047', DATE '2021-11-17', 1, '0a6b6b39a8b483d5408c6e6ba2ee32d8', 'NUCMED',
  NULL, NULL, NULL, 'cervical',
  NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, TRUE, 'suspicious',
  'i-123 images demonstrate linear midline increased uptake in the neck (this may represent thyroglossal duct remnant) as well as 2-3 foci of uptake in the neck on both sides, likely representing residual remnant with probable uptake in one of the central neck nodes. the uptake in the thyroid bed is 3.6%.there is otherwis',
  'nuclear_med_gap_triage', 'nucmed_legacy:9038:1', 'regex_triage_prompt3_20260514', 0.55,
  TIMESTAMP '2026-05-14T09:37:57Z', TRUE, 'I-131_or_I-123', TRUE, NULL,
  NULL, FALSE
);
