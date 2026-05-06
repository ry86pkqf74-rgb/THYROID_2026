-- Source: MotherDuck thyroid_canonical_publication_v1_0.manuscript_workspace.canonical_path_malignant_events_v1_ln_denominator_flag
-- Captured: 2026-05-06 for THY-19 BQ migration

CREATE VIEW manuscript_workspace.canonical_path_malignant_events_v1_ln_denominator_flag AS SELECT e.*, ((e.ln_involved > 0) AND (e.ln_examined IS NULL)) AS ln_synoptic_denom_missing_flag, ((e.nodal_disease_positive_count > 0) AND ((e.nodal_disease_total_count IS NULL) OR (e.nodal_disease_total_count = 0))) AS ln_detail_denom_missing_flag, (((e.ln_involved > 0) AND (e.ln_examined IS NULL)) OR ((e.nodal_disease_positive_count > 0) AND ((e.nodal_disease_total_count IS NULL) OR (e.nodal_disease_total_count = 0)))) AS ln_denom_missing_any_flag FROM main.canonical_path_malignant_events_v1 AS e;
