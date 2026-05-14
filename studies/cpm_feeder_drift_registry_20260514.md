# CPM feeder drift registry + dependency-ordered port plan

**Date:** 2026-05-14  
**Scope:** Phase 1 CPM DAG — every feeder the assembly chain (`scripts/200`–`217`) and promotion bridge (`221`+ where it reads structured inputs) consumes, compared to BigQuery `pub_canonical` / `pub_workspace` vs MotherDuck-export reference `pub_legacy_source_20260416`.  
**Policy:** Audit and plan only — no table rebuilds in this artifact.

**Machine-readable registry:** `studies/cpm_feeder_drift_registry_20260514.tsv` (one row per feeder or bundled domain).

**Related:** `studies/cpm_bq_native_rebuild_phase1_plan_20260514.md`, `studies/cpm_stage_asm204_20260514.sql`, `studies/bq_pub_authoritative_builders_table_20260514.tsv`, `bq_migrations/mig_086_legacy_promotion_sweep_views.sql`.

---

## 1. Drift classification rubric

| Class | Meaning | Action before CPM Phase 2 |
|--------|---------|------------------------------|
| **REGRESSION** | Rebuilt `pub_canonical` object lost columns or row coverage that downstream assembly still `SELECT`s from MotherDuck-era SQL | Fix BQ feeder to legacy parity, *or* explicitly retire column with signed provenance change |
| **INTENTIONAL_RESTRUCTURE** | Feeder deliberately redesigned (new semantic contract) | Update CPM assembly SQL (204-analog, 205+, migrations) + column-provenance documentation |
| **VIEW_PASSTHROUGH** | `pub_canonical.X` is `CREATE VIEW … AS SELECT * FROM pub_legacy_source_20260416.X` (`mig_086`) | Parity with snapshot is automatic for columns present in snapshot; MotherDuck-independence requires a future BQ-native builder |
| **SPINE_PARITY** | `patient_analysis_resolved_v1` replaces MotherDuck `gold_master_patient_facts_v1` on BQ | Maintain cohort invariants + name mapping for six PAR-vs-CPM gaps (Phase 1.5.1) |
| **MD_ONLY** | Feeder exists on MotherDuck (or attach DB) but not as a tracked `pub_canonical` object in the 2026-05-14 inventory | Materialize mirror or fold logic into PAR before BQ-native step |
| **MISSING_OR_FACADE** | Object absent from `studies/bq_pub_object_list_snapshot_20260514.json` or builder unclear | Verify live BQ `INFORMATION_SCHEMA` before port |

---

## 2. Verified drift (live BigQuery / operator)

From your verification (and `cpm_stage_asm204` workaround code):

### 2.1 `canonical_recurrence_v1` — **REGRESSION**

- **Legacy reference:** `pub_legacy_source_20260416.canonical_recurrence_v1` — **12** columns (includes `recurrence_histology`, `recurrence_evidence_source`).
- **Live BQ rebuild:** **10** columns — those two dropped.
- **Consumers:** `scripts/204_canonical_master_assembly.py` and `205_canonical_consolidation.py` recur CTE uses `SELECT *` then projects `r.recurrence_histology`, `r.recurrence_evidence_source`. `studies/cpm_stage_asm204_20260514.sql` **NULL-fills** those two columns — acceptable only as a scratch guard, not production parity.
- **Authoritative MD builder:** `scripts/203_canonical_recurrence.py`, `scripts/203b_canonical_recurrence_harmonized_20260429.py` (+ post-build QC migrations on MotherDuck, e.g. 269 family — see repo SQL under `qc_framework_v1/migrations/`).
- **BQ builder:** **ORPHAN** in `studies/bq_pub_authoritative_builders_table_20260514.tsv` — no single checked-in `CREATE TABLE` for the rebuilt table (not in `mig_086` facades).

### 2.2 `canonical_survival_followup_v1` — **INTENTIONAL_RESTRUCTURE**

- **Legacy reference (`201` contract):** `last_lab_date`, `last_tg_date`, `last_us_date`, `last_ct_date`, `last_nuclear_date`, `last_fna_date`, `last_contact_date`, `last_contact_source`, `followup_days`, `followup_years`, `followup_category` (see `scripts/201_canonical_survival_followup.py` `SURVIVAL_SQL`).
- **BQ / ASM204 contract:** `last_known_alive_date`, `last_followup_source`, `days_from_first_surgery_to_last_contact`, derived `followup_years`; `followup_category` **dropped** (ASM204 uses `CAST(NULL AS STRING)`).
- **Consumers:** 204/205 `surv` CTE orders by `followup_days`; ASM204 orders by `days_from_first_surgery_to_last_contact`.
- **Authoritative MD builder:** `scripts/201_canonical_survival_followup.py`.
- **BQ builder:** **ORPHAN** — aligns with newer survival SSOT comments in `qc_framework_v1/migrations/244_vw_patient_domain_wide_safe_VIEW_v1_20260501.sql` (`vital_status_current`, `last_known_alive_date`, etc.).

### 2.3 `canonical_diagnosis_unified_v1` — **VIEW_PASSTHROUGH**

- **BQ:** `CREATE VIEW … AS SELECT * FROM pub_legacy_source_20260416.canonical_diagnosis_unified_v1` (`mig_086` lines 255–256).
- **MD builder:** `scripts/200_canonical_diagnosis_standardization.py`.

### 2.4 `canonical_molecular_tested_v1` — **VIEW_PASSTHROUGH**

- **BQ:** `mig_086` lines 261–262.
- **MD builder:** `scripts/202_canonical_molecular_tested.py`.

---

## 3. Complete feeder inventory (DAG)

### 3.1 Tier A — Spine and four canonical sub-models (blocks CPM core join)

| Feeder | MD builder | BQ expectation | Assembly `SELECT`s (phase-1 analogue) |
|--------|------------|----------------|--------------------------------------|
| `patient_analysis_resolved_v1` | `48` + `86` | `pub_workspace` | ASM204: full row as `p` — replaces `gold_master_patient_facts_v1` |
| `gold_master_patient_facts_v1` | VIEW on PAR (MD) | N/A | 204/205/207/214/215/221: `g.*` wide block |
| `canonical_diagnosis_unified_v1` | `200` | `pub_canonical` VIEW | diag: `is_malignant`, `diagnosis_*`, `n_tumors`, `source_table` |
| `canonical_recurrence_v1` | `203`, `203b` | `pub_canonical` TABLE | recur: histology + evidence + biochem + TTR fields (**regression gap**) |
| `canonical_survival_followup_v1` | `201` | `pub_canonical` TABLE | surv: legacy contact/followup vs BQ survival SSOT (**restructure**) |
| `canonical_molecular_tested_v1` | `202` | `pub_canonical` VIEW | `molecular_*` / platform / gene flags |

### 3.2 Tier B — Script `201` inputs (survival / contact anchors)

`operative_episode_detail_v2`, `path_synoptics`, `longitudinal_lab_canonical_v1`, `thyroglobulin_lab_canonical_v1`, `ultrasound_reports`, `ct_imaging`, `nuclear_med`, `fna_episode_master_v2`.

### 3.3 Tier C — Script `202` inputs (molecular tested)

`molecular_testing`, `molecular_variant_long`, `extracted_braf_recovery_v1`, `extracted_ras_patient_summary_v1`, `thyroseq_molecular_enrichment`, `note_entities_genetics`.

### 3.4 Tier D — Script `203` / `203b` inputs (recurrence)

`operative_episode_detail_v2`, `fna_episode_master_v2`, `thyroglobulin_lab_VIEW_v1`, `thyroglobulin_lab_canonical_v1` (via views), `note_entities_llm_recurrence` (optional path), `path_synoptics`, `recurrence_event_clean_v1`, `gold_master_patient_facts_v1` / cohort enumeration, `canonical_fna_events_v1` (203b), `manuscript_workspace.recurrence_path_proven_candidates_v1` (203b, MD).

### 3.5 Tier E — Script `205` incremental joins (beyond 204)

`imaging_nodule_master_v1`, `tirads_llm_extracted_v2`, `fna_cytology`, `tumor_pathology` (enhanced LN CTE), `ultrasound_reports` (imaging LN), `patient_refined_master_clinical_v12`.

### 3.6 Tier F — Script `207` expansion

Prior `canonical_patient_master_v1` as `old_canon`, `gold_master_patient_facts_v1`, `patient_refined_master_clinical_v12`, `thyroid_scoring_py_v1`, `ct_imaging`, `nuclear_med`, `complication_patient_summary_v1`, `imaging_patient_summary_v1`, `tg_timeline_patient_summary_v1`.

### 3.7 Tier G — Scripts `208`–`217`

- **208:** `ln_master_rollup_v1` (copied from `thyroid_research_ro_v2` on MD; **not** in 2026-05-14 `pub_canonical` snapshot).
- **211:** `complication_phenotype_v1`, `extracted_rln_injury_refined_v2`, `extracted_ete_subgraded_v1`, `extracted_postop_labs_expanded_v1`, `rai_treatment_episode_v2`, `recurrence_event_clean_v1`, `survival_cohort_enriched`, `molecular_variant_long`.
- **212:** Bundled `note_entities_llm_*` (23 domains) + `note_entities_*` base tables per `scripts/212_nlp_entity_rollup.py` `DOMAIN_MAP`.
- **214:** `gold_master_patient_facts_v1`, `patient_refined_master_clinical_v12`, `longitudinal_lab_canonical_v1`, `ultrasound_reports`, `path_synoptics`.
- **215:** `note_entities_operative_detail`, `note_entities_medications`, `note_entities_problem_list`, `note_entities_procedures`.
- **216:** `longitudinal_lab_canonical_v1`, parquet/staging tables `mri_imaging`, `thyroid_weight_data` (MD) / `thyroid_weights` (BQ), NSQIP parquet → `nsqip_*`, operative sheet parquet.
- **217:** `longitudinal_lab_canonical_v1`, `clinical_note_ln_extracted_v1`, `canonical_patient_master_v1`.

### 3.8 Tier H — Promotion `221_eras_canonical_sync.py`

Reads parquet of `canonical_patient_master_v1`, merges `gold_master_patient_facts_v1`, `canonical_patient_master_v218`, and uses `operative_episode_detail_v2` for temporal columns — **bridge artefacts**, not additional `pub_canonical` feeders for ASM204.

---

## 4. Dependency-ordered port plan (feeders before CPM Phase 2 resumes)

Execute in order; later steps assume earlier parity or explicit SQL remapping.

1. **Spine:** `pub_workspace.patient_analysis_resolved_v1` — row/column SSOT vs MotherDuck PAR (invariant 10,871).
2. **VIEW facades sanity:** All `mig_086` `pub_canonical` views that feed 200-series — `INFORMATION_SCHEMA.TABLE_TYPE` + `COUNT(*)` vs legacy (spot-check).
3. **Survival SSOT:** `canonical_survival_followup_v1` — finalize BQ schema as authoritative; **rewrite** 204-analog / downstream to use `last_known_alive_date`, `days_from_first_surgery_to_last_contact`, `vital_status_*` per migration 244 patterns; re-derive or drop `followup_category` with documentation.
4. **Recurrence:** `canonical_recurrence_v1` — **restore** `recurrence_histology`, `recurrence_evidence_source` *or* amend assembly to stop selecting them with signoff; remove ASM204 `CAST(NULL AS STRING)` hacks after decision.
5. **Diagnosis / molecular tested:** Keep VIEW facades until explicit BQ-native `200`/`202` replaces them (optional MotherDuck-independence track).
6. **205 parity tables:** Confirm `patient_refined_master_clinical_v12` exists in live BQ (absent from 20260514 snapshot file); `imaging_nodule_master_v1`, `tirads_llm_extracted_v2`, `fna_cytology` facades.
7. **208 LN rollup:** Materialize `ln_master_rollup_v1` into `pub_canonical` **or** precompute LN rollup columns on PAR.
8. **211–217 chain:** Port in script order after Tier A–G feeders stable; 364 complications rollup policy may supersede `complication_patient_summary_v1` / `complication_phenotype_v1` for some BOOL columns — reconcile with `scripts/364_cpm_feeder_repoint.py`.

---

## 5. BigQuery verification snippets (run for evidence columns)

**Column diff vs legacy (example: recurrence):**

```sql
SELECT column_name, data_type
FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'canonical_recurrence_v1'
ORDER BY ordinal_position;

SELECT column_name, data_type
FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'canonical_recurrence_v1'
ORDER BY ordinal_position;
```

**Row counts (pairwise):**

```sql
SELECT 'pub_canonical' src, COUNT(*) n FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1`
UNION ALL
SELECT 'legacy', COUNT(*) FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.canonical_recurrence_v1`;
```

---

## 6. ORPHAN summary (Prompt 10 index)

Feeders with **ORPHAN_BUILDER** on `pub_canonical` in `studies/bq_pub_authoritative_builders_table_20260514.tsv` require either discovered strong evidence (CREATE / bq load) or curating `CURATED_LINEAGE` in `studies/bq_pub_authoritative_builders_20260514.py`. High-priority for CPM: **`canonical_recurrence_v1`**, **`canonical_survival_followup_v1`**, **`clinical_note_ln_extracted_v1`**, **`complication_patient_summary_v1`**, imaging summary tables, NSQIP landing tables.

---

*End of registry. For per-feeder columns and builders, prefer the TSV join to authoritative builders TSV.*
