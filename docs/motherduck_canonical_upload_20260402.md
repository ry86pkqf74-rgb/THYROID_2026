# MotherDuck Canonical Upload — 2026-04-02

> **Post-2026-04-02 config:** `config/motherduck_environments.yml` now maps **dev** and **qa** to dedicated sandbox databases (see [`motherduck_sandbox_clone_runbook.md`](motherduck_sandbox_clone_runbook.md)). The historical note below (“all envs point to `Thyroid 2026`”) reflects **only** the state of this upload session, not the current default env mapping.

## Summary

Uploaded all 27 Tier 1 + Tier 2 manuscript freeze v1 tables from local parquet
(`exports/manuscript_freeze_v1/data/`) into MotherDuck `"Thyroid 2026".main`.

- **Database**: `Thyroid 2026` (consolidated — single catalog for all thyroid data)
- **Schema**: `main`
- **Source**: `exports/manuscript_freeze_v1/data/*.parquet` (freeze created 2026-03-15)
- **Freeze git SHA**: `975cfa2bb0a29262bb457953d4c5af23ce51d242`
- **Token source**: `secrets.toml:MOTHERDUCK_TOKEN`
- **Result**: 27/27 tables uploaded with full row and column parity

## Architecture decision

The legacy `thyroid_research_2026` database name (hardcoded in `motherduck_client.py`)
never existed for the current token. The `Thyroid 2026` catalog was already writable and
contained related thyroid data (`canonical_extracted_fact_long_v1`, `gold_llm_verified_facts`,
and 15 `v2_stage` LLM entity tables). Rather than splitting data across two databases,
all manuscript freeze tables were consolidated into `Thyroid 2026.main`.

Changes made:
- `motherduck_client.py`: default database → `Thyroid 2026`; `connect_rw()` handles
  space-in-name via `USE` statement; stale `_SHARE_PATH_PROD` cleared
- `config/motherduck_environments.yml`: created (all envs point to `Thyroid 2026`)
- Empty `thyroid_research_2026` database dropped

## Parity report — Tier 1 (11 tables)

| Table | Local rows | MD rows | Cols | Status |
| --- | ---: | ---: | ---: | --- |
| `manuscript_cohort_v1` | 10,871 | 10,871 | 150 | MATCH |
| `patient_analysis_resolved_v1` | 10,871 | 10,871 | 145 | MATCH |
| `episode_analysis_resolved_v1_dedup` | 9,368 | 9,368 | 45 | MATCH |
| `lesion_analysis_resolved_v1` | 11,851 | 11,851 | 27 | MATCH |
| `thyroid_scoring_py_v1` | 10,871 | 10,871 | 51 | MATCH |
| `analysis_cancer_cohort_v1` | 4,136 | 4,136 | 136 | MATCH |
| `complication_phenotype_v1` | 5,928 | 5,928 | 28 | MATCH |
| `complication_patient_summary_v1` | 2,892 | 2,892 | 18 | MATCH |
| `recurrence_event_clean_v1` | 1,946 | 1,946 | 11 | MATCH |
| `longitudinal_lab_canonical_v1` | 39,961 | 39,961 | 18 | MATCH |
| `survival_cohort_enriched` | 61,134 | 61,134 | 27 | MATCH |

## Parity report — Tier 2 (16 tables)

| Table | Local rows | MD rows | Cols | Status |
| --- | ---: | ---: | ---: | --- |
| `analysis_molecular_subset_v1` | 10,025 | 10,025 | 126 | MATCH |
| `analysis_tirads_subset_v1` | 3,474 | 3,474 | 131 | MATCH |
| `analysis_recurrence_subset_v1` | 1,946 | 1,946 | 133 | MATCH |
| `patient_refined_master_clinical_v12` | 12,886 | 12,886 | 272 | MATCH |
| `extracted_tirads_validated_v1` | 3,474 | 3,474 | 15 | MATCH |
| `extracted_braf_recovery_v1` | 730 | 730 | 7 | MATCH |
| `extracted_ras_patient_summary_v1` | 321 | 321 | 7 | MATCH |
| `extracted_rln_injury_refined_v2` | 92 | 92 | 12 | MATCH |
| `extracted_complications_refined_v5` | 358 | 358 | 9 | MATCH |
| `operative_episode_detail_v2` | 9,371 | 9,371 | 39 | MATCH |
| `rai_treatment_episode_v2` | 1,857 | 1,857 | 32 | MATCH |
| `molecular_test_episode_v2` | 10,126 | 10,126 | 42 | MATCH |
| `tumor_episode_master_v2` | 11,691 | 11,691 | 37 | MATCH |
| `imaging_nodule_master_v1` | 19,891 | 19,891 | 25 | MATCH |
| `extracted_ete_subgraded_v1` | 3,558 | 3,558 | 9 | MATCH |
| `extracted_postop_labs_expanded_v1` | 1,395 | 1,395 | 12 | MATCH |

## Pre-existing tables preserved

| Schema | Table | Rows | Note |
| --- | --- | ---: | --- |
| `main` | `canonical_extracted_fact_long_v1` | 66,694 | Pre-existing canonical extraction |
| `main` | `gold_llm_verified_facts` | 178 | Pre-existing LLM gold |
| `v2_stage` | 15 `note_entities_llm_*` tables | 11,037 each | Created 2026-04-02, preserved |

## Final inventory — `Thyroid 2026`

- **`main` schema**: 29 tables (27 freeze + 2 pre-existing)
- **`v2_stage` schema**: 15 tables (LLM entity staging)
- **Total**: 44 tables

## RO share

Created `thyroid_research_ro_v2` (UPDATE AUTOMATIC) from `Thyroid 2026`.

- **Share path**: `md:_share/thyroid_research_ro_v2/2558f066-1c5d-46a5-afbc-800fd5f7568d`
- **Scope**: ORGANIZATION / DISCOVERABLE
- **Previous (stale)**: `md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c`
- **Updated in**: `motherduck_client.py` `_SHARE_PATH_PROD`
- **Verified**: `connect_ro_share()` reads `manuscript_cohort_v1` (10,871 rows) and
  `survival_cohort_enriched` (61,134 rows) correctly
