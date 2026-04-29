# mig_165 — Auxiliary registry classification audit trail

**batch_id:** `mig_165_auxiliary_registry_hygiene_20260429`  
**Lane:** 53 / mig_165  
**Prompt:** `cursor_prompts/CURSOR_PROMPT_mig165_auxiliary_registry_hygiene_20260429.md`  
**Applied:** MotherDuck RW `thyroid_canonical_publication_v1_0` — SQL file  
`qc_framework_v1/migrations/165_auxiliary_registry_hygiene_20260429.sql`

## Physical-backing correction vs draft Cowork probe

The draft probe joined `canonical_table_signoff_registry_v1` rows to **`main.information_schema.tables` only**, which labeled **`manuscript_workspace`** auxiliary tables as “STALE.” Live classification keyed **`registry.schema_name`** correctly:

| Metric | Draft (main-only join) | Correct (schema-qualified) |
|--------|-------------------------|------------------------------|
| Phys-backed auxiliary rows | 30 | **85** |
| Orphan “no table” rows | 53–55 | **0** |

**DELETE registry rows:** **not applicable** — every `not_started` auxiliary row resolves to an existing BASE TABLE under `main` or `manuscript_workspace`.

## Gate uplift (verified table rows)

| Checkpoint | Value |
|------------|-------|
| Gate1 verified tables (pre-mig_165 probe) | **88** |
| Gate1 verified tables (post-apply probe) | **165** |
| Delta | **+77** (= **76** mass auto-na tables + **1** new Tier-1 registration) |

## Disposition summary

| disposition | count | action |
|-------------|-------|--------|
| **auto_na → verified table_status** | **76** | All columns flipped `not_started`→`na` with Lane-appropriate `verification_method`; table rollup → `verified`. |
| **CF — analytic / Tier-2 deferral** | **10** | Column registry untouched (`not_started`); **`canonical_table_signoff_registry_v1.notes`** appended only. |
| **Tier-1 orphan registration** | **1** | **`note_entities_llm_presenting_symptoms`** — INSERT column rows + INSERT table signoff (`na`-only verified). |

## Per-row classification (`table_name | phys_backed | bucket | disposition`)

All **85** pre-existing auxiliary rows were **phys_backed=TRUE** after schema-qualified existence checks.

### main.* — auto_na / tier1_raw_mirror (`auto_tier1_raw_mirror_skip`)

| table_name | disposition |
|------------|----------------|
| clinical_notes_long | auto_na verified |
| clinical_note_ln_extracted_v1 | auto_na verified |
| path_synoptics | auto_na verified |
| ct_imaging | auto_na verified |
| mri_imaging | auto_na verified |
| nuclear_med | auto_na verified |
| thyroid_sizes | auto_na verified |
| thyroid_weights | auto_na verified |
| note_entities_operative_detail | auto_na verified |
| note_entities_procedures | auto_na verified |
| imaging_exam_master_v1 | auto_na verified |

### main.* — registry_governance (`auto_registry_governance_skip`)

| table_name | disposition |
|------------|----------------|
| __readme | auto_na verified |
| data_dictionary_v279 | auto_na verified |

### main.* — governance_audit / specimen / Tg satellite (`auto_governance_audit_table_skip`)

| table_name | disposition |
|------------|----------------|
| cupm_v2_canonical_backfill_v1 | auto_na verified |
| ete_adjudication_v1 | auto_na verified |
| patient_completion_oed_path_linkage_v1 | auto_na verified |
| nsqip_enrichment | auto_na verified |
| nsqip_patient_summary | auto_na verified |
| specimen_genomic_assay_v1 | auto_na verified |
| specimen_master_v1 | auto_na verified |
| specimen_source_xref_v1 | auto_na verified |
| specimen_tumor_focus_v1 | auto_na verified |
| tg_postop_surveillance_windows_v1 | auto_na verified |
| tg_timeline_patient_summary_v1 | auto_na verified |

### main.* — CF (`CF-mig165-AUX-NEEDS-REAL-VERIFY-<table>` **or** recurrence deferral)

| table_name | disposition |
|------------|----------------|
| imaging_fna_linkage_v3 | **CF** — analytic linkage layer |
| imaging_patient_summary_v1 | **CF** — patient rollup analytic |
| manuscript_cohort_v1 | **CF** — Tier-2 analytic composite |
| patient_cross_domain_timeline_v2 | **CF** — analytic spine |
| tumor_stage_heterogeneity_v1 | **CF** — analytic heterogeneity |
| recurrence_event_clean_v1 | **`CF-mig165-RECURRENCE-EVENT-CLEAN-NEEDS-REAL-VERIFY`** — defer real Tier-2 verification (**mig_163** lane); registry cols remain **`not_started`** |

### manuscript_workspace.* — registry_governance (`auto_registry_governance_skip`)

detail_table_registry_v1 · main_schema_keep_list_v1 · object_domain_map_v1 · registry_end_to_end_validation_v1 · registry_v2_resolution_audit_v1 · registry_v2_unresolved_pointers_v1 → **auto_na verified**

### manuscript_workspace.* — governance_audit (`auto_governance_audit_table_skip`)

All remaining auto-classified workspace tables except the four CF analytic rows below → **auto_na verified** (full enumeration in migration SQL batches **165e–165f**).

### manuscript_workspace.* — CF (`CF-mig165-AUX-NEEDS-REAL-VERIFY-<table>`)

| table_name | disposition |
|------------|----------------|
| episode_analysis_resolved_v1_dedup | **CF** |
| lesion_analysis_resolved_v1 | **CF** |
| patient_analysis_resolved_v1 | **CF** |
| ln_master_rollup_v1 | **CF** |

## New registration — Tier-1 orphan mirror

| table_name | disposition |
|------------|----------------|
| note_entities_llm_presenting_symptoms | NEW **`main`** BASE TABLE — INSERT column registry (**23** cols `na`) + INSERT table signoff **`verified`** (`auto_tier1_raw_mirror_skip`). |

## Carry-forward CF tokens (informational)

- **`CF-mig165-AUX-NEEDS-REAL-VERIFY-*`** — analytic / Tier-2 tables intentionally left **`not_started`** at column granularity pending independent replay verification.
- **`CF-mig165-RECURRENCE-EVENT-CLEAN-NEEDS-REAL-VERIFY`** — **`recurrence_event_clean_v1`** aligned with mig_163 recurrence lane (**prompt §8**).

---

*Classification audit trail generated for Logan ratification (mass **`na`** tier-skip vs analytic deferrals).*
