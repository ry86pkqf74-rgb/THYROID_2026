<!-- READY FOR LOGAN MANUSCRIPT REFINEMENT -->

# `canonical_patient_master`

**Grain:** One row per patient (`research_id`)

**Total rows:** `10,871`

**Distinct patients:** `10,871`

**Verification status:** verified

**Signoff migration:** `qc_framework_v1/migrations/174b_cnln_img_laterality_per_side_boolean_20260429.sql`

## Purpose

| mig126: reconcile n_columns_total/n_not_started to match column-registry information_schema cardinality (+6 cols vs stale signoff row; 3c drift fix). | mig_130: Operative thematic cluster CLOSED (233 cols). Pathology / lymph_node / labs / pmh_psh / us_imaging / rai / recurrence / fna / ete / survival / medications / molecular / complications / frozen_section / demographics / other remain. Gate-1 table-verified tally unchanged until all CPM cols closed. | mig_132: Pathology thematic cluster CLOSED (106 cols). Lymph_node / labs / pmh_psh / us_imaging / …

## Build pipeline

Rolling signoff `qc_framework_v1/migrations/174b_cnln_img_laterality_per_side_boolean_20260429.sql`; Tier `tier1_anchor`. Rebuild per latest Path-C batch; derivation scripts referenced alongside column-level verification_method entries in canonical_column_verification_registry_v1.

## Key columns

- Column `any_ete_present_not_further_specified_anywhere` — verification category `derived` registry seed
- Column `any_ete_present_not_further_specified_in_op_or_path` — verification category `derived` registry seed
- Column `any_ete_anywhere` — verification category `derived` registry seed
- Column `any_ete_in_op_or_path` — verification category `derived` registry seed
- Column `any_ete_in_imaging` — verification category `derived` registry seed
- Column `age_at_surgery` — verification category `adjudicated` registry seed
- Column `ages_calculable_flag` — verification category `adjudicated` registry seed
- Column `ages_score` — verification category `derived` registry seed
- Column `aggressive_variant_flag` — verification category `adjudicated` registry seed
- Column `ajcc8_calculable_flag` — verification category `adjudicated` registry seed

## Known limitations

- **`CF-100`:** carry-forward / limitation referenced in registry notes.
- **`CF-100-DATE-RETYPE`:** carry-forward / limitation referenced in registry notes.
- **`CF-DUP`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig123-RECURRENCE-DATE-RETYPE`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig132-PM-PATH-STAGE-DERIVED-AT-RETYPE`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig133-PM-CNCLN-DATE-PARSE`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig133-PM-LN-COUNT-INTEGRITY`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig134-PM-LAB-DATE-ANCHOR`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig134-PM-TG-N-DUAL-FEED`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig135-PM-COMPL-ROLLUP-SEMANTICS`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig136-104-ONTOLOGY`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig136-104-ontology`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig136-DAYS-SEMANTIC`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig137-PM-MOL-DATE-RETYPE`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig137-PM-MOL-TESTED-V2-GAP`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig138-CPM-RECURRENCE-SPINE-RESYNC-PENDING`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig138-PM-RECURRENCE-DATE-RETYPE`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig140-COHORT-INVARIANT-microscopic_ete_t3b_corrected`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig140-EXPAND-UPSTREAM-IMAGING-NFS-ETE`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig140-PM-ETE-IMAGING-UPSTREAM-PENDING`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig141-COHORT-NEAR-UNIFORM-TRUE-prm_followup_has_complications`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig141-COHORT-NEAR-UNIFORM-TRUE-survival_eligible_flag`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig141-CPM-VITAL-vs-SSOT-PARITY`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig142-COHORT-NEAR-UNIFORM-TRUE-nlp_raidetail_has_data`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig142-COHORT-NEAR-UNIFORM-TRUE-rai_has_adjudication`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig142-COHORT-NEAR-UNIFORM-TRUE-rai_has_completion_status`:** carry-forward / limitation referenced in registry notes.
- **`CF-mig142-COHORT-NEAR-UNIFORM-TRUE-rai_received_reconciled`:** carry-forward / limitation referenced in registry notes.
- **Representative excerpts (verbatim trims):**
  - | mig_130 operative cluster (Lane 22). age_at_surgery — demographics at surgery anchor (operative probe boundary).
  - | mig_143 AGES shard. matches thyroid_scoring_python `compute_ages` (**0.1** age scaling tier, not obsolete 0.05 vignette); CF-mig143-AGES-CALC-ALLTRUE: flags all TRUE on cohort.
  - | mig_143 AGES shard. matches thyroid_scoring_python `compute_ages` (**0.1** age scaling tier, not obsolete 0.05 vignette); CF-mig143-AGES-CALC-ALLTRUE: flags all TRUE on cohort.
  - | mig_157 Lane 46 (157k). histologic_* STRING_AGG + aggressive_variant ladder; dominant_nodule v1 vs v2 drift CF. | CF-mig157-AGGRESSIVE-VARIANT-LADDER: low TRUE rate (43/10871) — strict tall/columnar/hobnail ladder.
  - | mig_143 AJCC-component shard STRING_AGG payloads; **`ajcc8_calculable_flag` NOT naive “(T+N+M+age) ALL non-null”** — see CF-mig143-AJCC8-CALC-NOT-NAIVE4 vs naive probe **112**. Ordering-independent equality via list_s…
  - | mig_132 pathology cluster (Lane 23). AJCC7/8 + dominant tumor staging columns vs mig_266b manuscript adjudication spine.
  - | mig_143 AJCC-component shard STRING_AGG payloads; **`ajcc8_calculable_flag` NOT naive “(T+N+M+age) ALL non-null”** — see CF-mig143-AJCC8-CALC-NOT-NAIVE4 vs naive probe **112**. Ordering-independent equality via list_s…
  - | mig_132 pathology cluster (Lane 23). AJCC7/8 + dominant tumor staging columns vs mig_266b manuscript adjudication spine.


## Verification methods used

- `Path C: PM nlp cluster lineage + source-discovery + cohort-uniformity sweep`
- `audit_companion_to_resolved_col`
- `auto_identifier_skip`
- `auto_passthrough_raw_str`
- `auto_provenance_skip`
- `auto_provenance_skip_audit_metadata_v1`
- `commercial_panel_passthrough_afirma_thyroseq`
- `cross_check_event_grain_inner_join_cast_varchar`
- `cross_check_mortality_crossover_survival_complications`
- `cross_domain_aggregation_any_overlap_rules_v1`
- `cross_modal_imaging_aggregate`
- `cross_source_pathology_gland_weight_resolution`
- `cross_source_resolution_r_ln_margin_truth_v150_v154_v133`
- `cross_validate_dose_nucmed_vs_rai`
- `cross_validate_vs_canonical_molecular_genetics_v2`
- `demographics_age_at_surgery_anchor_multi_source`
- `derivation_ages_arithmetic`
- `derivation_ajcc_calculability_check`
- `derivation_bmi_hierarchy`
- `derivation_canonical_labs_rollups_mig115_script347`
- `derivation_canonical_labs_thyroglobulin_script347_merge`
- `derivation_cervical_ln_multi_source_mig111_113_path_us`
- `derivation_coalesce_v1_v2_with_audit_rule`
- `derivation_ene_multisource_mig114_gm_path_raw`
- `derivation_fna_path_concordance_chain`

---
_Starter generated by `render_mig197_data_dictionary_readonly.py` (thyroid_canonical_publication_v1_0). Logan refines voice._
