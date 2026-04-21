-- Archived: 2026-04-21
-- Source:   thyroid_canonical_publication_v1_0.main.molecular_fusions_unnested_v2
-- New name: thyroid_canonical_publication_v1_0.main.molecular_fusions_unnested_VIEW_v2
-- Reason:   view labeling pass — add _VIEW suffix

CREATE VIEW molecular_fusions_unnested_v2 AS SELECT t.research_id, t.molecular_episode_id, t.test_date_native, t.platform, f.gene1 AS gene1, f.gene2 AS gene2, ((f.gene1 || '-') || f.gene2) AS fusion_pair, f.source_call AS source_call, t.gene_fusions_status AS fusions_status_for_test, t.rom_percent_point, t.rom_descriptor FROM main.canonical_molecular_genetics_v2 AS t , unnest(t.gene_fusions_list) AS u(f) WHERE ((t.gene_fusions_list IS NOT NULL) AND (len(t.gene_fusions_list) > 0));
