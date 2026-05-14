-- BigQuery: backfill all-negative ThyroSeq section statuses on partial-parse rows.
-- Run AFTER deploying parser fixes; respects empty-string and NULL status slots.
-- Project: thyroid-canonical-pub-2026
--
-- Excludes manually adjudicated rows (do not run on those without approval).

UPDATE `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2` c
SET
  gene_mutations_status = CASE
    WHEN REGEXP_CONTAINS(src.txt, r'(?i)gene\s+m[a-z]{1,5}tations?\s*[:\n\r]+\s*(negative|none|not detected)')
      AND (c.gene_mutations_status = '' OR c.gene_mutations_status IS NULL) THEN 'Negative'
    ELSE c.gene_mutations_status END,
  gene_fusions_status = CASE
    WHEN REGEXP_CONTAINS(src.txt, r'(?i)gene\s+fusions?\s*[:\n\r]+\s*(negative|none|not detected)')
      AND (c.gene_fusions_status = '' OR c.gene_fusions_status IS NULL) THEN 'Negative'
    ELSE c.gene_fusions_status END,
  cna_status = CASE
    WHEN REGEXP_CONTAINS(src.txt, r'(?i)c[ao]py\s+number(\s+\w+)?\s*[:\n\r]+\s*(negative|none|not detected)')
      AND (c.cna_status = '' OR c.cna_status IS NULL) THEN 'Negative'
    ELSE c.cna_status END,
  gep_status = CASE
    WHEN REGEXP_CONTAINS(src.txt, r'(?i)gene\s+expression(\s+profile)?\s*[:\n\r]+\s*(negative|none|not detected)')
      AND (c.gep_status = '' OR c.gep_status IS NULL) THEN 'Negative'
    ELSE c.gep_status END,
  parse_status = CASE WHEN c.parse_status = 'partial' AND c.braf_flag IS NOT TRUE
                           AND c.ras_flag IS NOT TRUE AND c.tert_flag IS NOT TRUE THEN 'ok'
                      ELSE c.parse_status END,
  built_at = CURRENT_TIMESTAMP()
FROM (
  SELECT research_id, molecular_episode_id, detailed_findings_raw AS txt
  FROM `thyroid-canonical-pub-2026.pub_canonical.molecular_test_episode_v2`
  WHERE detailed_findings_raw IS NOT NULL
) AS src
WHERE c.research_id = src.research_id
  AND c.molecular_episode_id = src.molecular_episode_id
  AND c.parse_status = 'partial'
  AND c.platform = 'ThyroSeq'
  AND (c.gene_mutations_status = '' OR c.gene_mutations_status IS NULL)
  AND c.braf_flag IS NOT TRUE AND c.ras_flag IS NOT TRUE AND c.tert_flag IS NOT TRUE
  AND (c.adjudication_status IS NULL OR c.adjudication_status <> 'manually_reviewed');
