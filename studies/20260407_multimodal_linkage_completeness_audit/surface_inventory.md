# Surface inventory — multimodal + downstream chain

## Design / contract documentation

| Path | Role |
|------|------|
| `docs/imaging_layer_v3_design.md` | Canonical US nodule table: `imaging_nodule_master_v1`; `imaging_nodule_long_v2` deprecated / empty analytics |
| `docs/motherduck_database_contract_v1.md` | `main` / `v2_stage` / `qa` / `release_*`; episode + linkage + specimen/FHIR catalog |
| `docs/multimodal_contract_v1.md` | Multimodal contract v1 objects under `mm_contract_dev`, upstream keys, strict-release `val_*` |
| `docs/multimodal_contract_runbook.md` | Operator order: **129 → 128**, schema alignment (`mm_contract_dev`) |
| `docs/multimodal_release_gate.md` | Fail-closed conditions for multimodal promotion |
| `data_dictionary.md` | `fna_episode_master_v2`, `molecular_test_episode_v2`, molecular results layer (131) |

## Core scripts (chain-related)

| Script | What it builds / does |
|--------|------------------------|
| `scripts/50_multinodule_imaging.py` (referenced in imaging doc) | `imaging_nodule_master_v1`, exam/patient summaries |
| `scripts/22_canonical_episodes_v2.py` | Episode spine tables including `fna_episode_master_v2`, `molecular_test_episode_v2`, `tumor_episode_master_v2`, legacy imaging long v2 |
| `scripts/49_enhanced_linkage_v3.py` | `preop_surgery_linkage_v3`, `fna_molecular_linkage_v3`, `surgery_pathology_linkage_v3`, `pathology_rai_linkage_v3`, etc. |
| `scripts/129_imaging_fna_linkage_mm_v1.py` | **`imaging_fna_linkage_mm_v1`**, review queue, audit — **imaging ↔ FNA only** |
| `scripts/128_multimodal_contract_mm_v1.py` | Schema **`mm_contract_dev`** (default on MD): dims/facts/links + **re-runs 129 SQL in-schema** + `link_imaging_fna_mm_v1` + multimodal `val_*` |
| `scripts/mm_contract_upstream.py` | Resolves upstream table names; strict column checks for `--strict-release` |
| `utils/imaging_fna_linkage_mm_v1.py` | `normalize_specimen_key_sql` for deterministic specimen-key normalization |
| `utils/canonical_nodule_linkage.py` | **Study-layer** DuckDB SQL: nodule spine → primary IFNA → FNA row → rank-1 `fna_molecular_linkage_v3` → preop FNA→surgery → rank-1 `surgery_pathology_linkage_v3` → `tumor_episode_master_v2` |
| `scripts/149_md_canonical_nodule_linkage_study.py` | Runs study exports; optional **`main.canonical_nodule_linkage_study_v1`** VIEW (`--materialize-view`; prod needs `--confirm-prod-view`) |
| `scripts/131_molecular_results_layer.py` | `main.molecular_results` / variant long — governed molecular assay layer (alongside episodes) |
| `scripts/132_molecular_fact_lineage_views.py` | `main.molecular_fact_long_v` and related views |
| `scripts/138_md_specimen_fhir_layer.py` | `main.specimen_master_v1`, tumor focus, FHIR export tables — **encounter/specimen** spine, not the same grain as nodule chain |
| `scripts/140_md_specimen_genomics_binding.py` | `main.specimen_genomic_assay_v1` (referenced in contract doc) |
| `scripts/109_synoptic_encounter_qc.py` | `path_synoptics_encounter_qc_v1` (VIEW) + encounter isolation QA |
| `scripts/117_md_contract_views.py` | MotherDuck contract / episode surfaces (per README / contract doc) |
| `scripts/148_thyroid2026_release_gate.py` | Checks multimodal **`val_*`** emptiness in `mm_contract_dev` or `main` |

## Tests (multimodal + specimen)

| Path |
|------|
| `tests/test_imaging_fna_linkage_mm_v1.py` |
| `tests/test_multimodal_contract_mm_v1.py` |
| `tests/test_specimen_identity_layer.py` |
| `tests/test_specimen_genomics_binding.py` |
| `tests/test_specimen_fhir_layer.py` |

## Connection / token helpers

| Path | Role |
|------|------|
| `motherduck_client.py` | Token resolution: `MD_SA_TOKEN`, `MOTHERDUCK_TOKEN`, `.streamlit/secrets.toml` |
| `utils/md_connect.py` | `connect_md_or_file`, fail-closed MotherDuck attach |

## SQL helpers

| Path |
|------|
| `sql/mm_contract_v1_promotion_gate.sql` | Row counts / validator smoke against `mm_contract_dev` |
