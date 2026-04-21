-- Archived: 2026-04-21
-- Source:   thyroid_canonical_publication_v1_0.main.molecular_variants_unnested_v2
-- New name: thyroid_canonical_publication_v1_0.main.molecular_variants_unnested_VIEW_v2
-- Reason:   view labeling pass — add _VIEW suffix

CREATE VIEW molecular_variants_unnested_v2 AS SELECT t.research_id, t.molecular_episode_id, t.test_date_native, t.platform, v.gene AS gene, v.protein AS protein, v.cdna AS cdna, v.af_pct AS af_pct, v.source_call AS source_call, t.gene_mutations_status AS mutations_status_for_test, t.tert_present, t.rom_percent_point, t.rom_descriptor FROM main.canonical_molecular_genetics_v2 AS t , unnest(t.gene_mutations_variants) AS u(v) WHERE ((t.gene_mutations_variants IS NOT NULL) AND (len(t.gene_mutations_variants) > 0));
