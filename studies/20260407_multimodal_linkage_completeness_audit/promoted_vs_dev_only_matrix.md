# Promoted vs dev-only vs local — matrix

## `main.*` — promoted / canonical query plane

**From `docs/motherduck_database_contract_v1.md` and pipeline READMEs (when materialized on MotherDuck):**

| Category | Examples |
|----------|----------|
| Episode spine | `tumor_episode_master_v2`, `molecular_test_episode_v2`, `operative_episode_detail_v2`, `fna_episode_master_v2`, `rai_treatment_episode_v2` |
| Imaging (canonical) | `imaging_nodule_master_v1`, `imaging_exam_master_v1`, `imaging_patient_summary_v1` |
| v3 scored linkages | `preop_surgery_linkage_v3`, `fna_molecular_linkage_v3`, `surgery_pathology_linkage_v3`, `pathology_rai_linkage_v3`, `imaging_fna_linkage_v3` (legacy / parallel to mm_v1) |
| Imaging–FNA (129) | `imaging_fna_linkage_mm_v1` **when** script 129 targets `main` (default local; MD operators often redirect to `mm_contract_dev`) |
| Fact lineage / molecular layer | `canonical_extracted_fact_long_v2`, `molecular_results`, `molecular_variant_long`, views from **132** |
| Labs | `thyroglobulin_lab_canonical_v1`, `longitudinal_lab_canonical_v1` |
| Specimen + FHIR | `specimen_master_v1`, `specimen_tumor_focus_v1`, `specimen_genomic_assay_v1`, `fhir_*` per contract |
| Synoptic long / encounter QC | `synoptic_tumor_long_v1`, `path_synoptics_encounter_qc_v1` (VIEW) |
| Optional study VIEW | `canonical_nodule_linkage_study_v1` — **only** after **149** `--materialize-view` (explicit prod opt-in) |

**Distinction:** These are **pieces** of the chain; the contract doc does **not** define a **single** `main` table that is the nodule-level US→path **wide** chain.

## `qa.*` — governance / diagnostics

| Examples | Role |
|----------|------|
| `promotion_scorecard`, `manual_review_queue`, `release_manifest` | Promotion / review |
| `val_specimen_contract_v1`, `specimen_merge_review_queue_v1`, `v_diag_specimen_*` | Specimen/FHIR QA (**142** family) |

## `mm_contract_dev.*` — dev / CI multimodal contract (default **128** on MotherDuck)

| Objects | Role |
|---------|------|
| `dim_patient_mm_v1`, `fact_imaging_mm_v1`, `fact_fna_mm_v1`, `fact_genetics_mm_v1`, `fact_tumor_mm_v1`, `link_imaging_fna_mm_v1`, `link_surgery_path_mm_v1`, `link_surgery_context_mm_v1` | Star schema + surrogate IDs |
| `imaging_fna_linkage_mm_v1` (+ audit/review) | Re-materialized **inside** schema when **128** runs |
| `val_*_mm_v1` | Strict-release blockers (must be empty for clean CI) |

**Per `scripts/128_multimodal_contract_mm_v1.py`:** MotherDuck schema is **forced to `mm_contract_dev`** unless `MM_CONTRACT_MD_SCHEMA_OVERRIDE=1` and `MM_CONTRACT_SCHEMA` set — i.e. multimodal contract is **not** the same as “everything lives in `main`.”

## Local-only / historical / deprecated

| Object | Note |
|--------|------|
| `thyroid_master.duckdb` (workspace file) | **Not** guaranteed to mirror MotherDuck `main`; this audit’s read-only probe showed **missing** `imaging_nodule_master_v1`, v3 linkages, `imaging_fna_linkage_mm_v1`, specimen tables — **slim local clone** |
| `imaging_nodule_long_v2` | Deprecated stub per `docs/imaging_layer_v3_design.md` |
| Legacy `imaging_fna_linkage_v2` | Superseded by v3 / mm v1 paths per architecture notes |

## Release readiness (multimodal)

- **128/129** multimodal work is **dev-schema scoped by default** (`mm_contract_dev`) and **gated** by strict `val_*` tables and **148**.
- **`reports/motherduck_read_only_audit.md`** (repo) records **non-empty** multimodal blockers on some environments — treat as **not release-silent** until those tables are empty under operator policy.
