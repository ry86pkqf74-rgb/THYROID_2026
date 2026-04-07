-- MotherDuck validation — run against dev: "Thyroid 2026 Molecular Dev 20260407"
USE "Thyroid 2026 Molecular Dev 20260407";

SELECT 'main.note_entities_genetics' AS slice, COUNT(*) AS n FROM main.note_entities_genetics
UNION ALL SELECT 'main.molecular_results', COUNT(*) FROM main.molecular_results
UNION ALL SELECT 'main.molecular_variant_long', COUNT(*) FROM main.molecular_variant_long
UNION ALL SELECT 'main.molecular_assay_dictionary', COUNT(*) FROM main.molecular_assay_dictionary
UNION ALL SELECT 'main.molecular_code_crosswalk', COUNT(*) FROM main.molecular_code_crosswalk;

-- Genetics note QC / verification
SELECT verification_status, COUNT(*) AS n
FROM main.note_entities_genetics
GROUP BY 1 ORDER BY n DESC NULLS LAST;

-- Assay dictionary by platform (post-131 seed)
SELECT platform, COUNT(*) AS n
FROM main.molecular_assay_dictionary
GROUP BY 1 ORDER BY n DESC;

-- Top "variants" from note genetics (proxy — structured variant long empty)
SELECT entity_value_norm, COUNT(*) AS n
FROM main.note_entities_genetics
WHERE entity_type ILIKE '%variant%' OR entity_domain ILIKE '%gen%'
GROUP BY 1
ORDER BY n DESC
LIMIT 15;

-- QC flags on governed layer (empty until ingest)
SELECT 'molecular_results rows with qc_flags' AS metric,
       COUNT(*) FILTER (WHERE qc_flags IS NOT NULL AND len(json_extract(qc_flags, '$')) > 2) AS n
FROM main.molecular_results;

SELECT 'molecular_variant_long rows with qc_flags' AS metric,
       COUNT(*) FILTER (WHERE qc_flags IS NOT NULL AND len(json_extract(qc_flags, '$')) > 2) AS n
FROM main.molecular_variant_long;

-- Assay names that would fall outside dictionary (when results populated)
SELECT COUNT(DISTINCT assay_name) AS distinct_assay_names_not_in_dict
FROM main.molecular_results r
WHERE r.assay_name IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM main.molecular_assay_dictionary d
    WHERE COALESCE(d.assay_name, '') = COALESCE(r.assay_name, '')
  );
