# Multimodal analysis contract v1 (`mm_contract_dev`)

This release layer sits **on top of** legacy episode and v3 linkage tables. It does **not** replace scripts 22–25 / 49 or rename `research_id`. All contract tables retain lineage (`research_id`, `canonical_research_id` where applicable).

## Build

```bash
# Local DuckDB file (./thyroid_master.duckdb) — requires all upstream tables present
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py

# MotherDuck — writes **only** to schema `mm_contract_dev` (override requires
# MM_CONTRACT_MD_SCHEMA_OVERRIDE=1 and explicit MM_CONTRACT_SCHEMA)
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py --md
```

## Surrogate ID recipes (deterministic)

| ID | Formula (DuckDB) |
|----|-------------------|
| `person_id` | `'mmv1_p_' \|\| lower(md5(concat('THYROID_MM_CONTRACT_V1\|person\|', cast(canonical_research_id AS VARCHAR))))` |
| `surgery_id` | `'mmv1_s_' \|\| lower(md5(concat('THYROID_MM_CONTRACT_V1\|surgery\|', cast(research_id AS VARCHAR), '\|', cast(surgery_episode_id AS VARCHAR))))` |
| `nodule_id` (dim) | `'mmv1_n_' \|\| lower(md5(concat('THYROID_MM_CONTRACT_V1\|nodule\|', … research_id, exam_id, legacy nodule_id …)))` |
| `tumor_instance_id` | `'mmv1_t_' \|\| lower(md5(concat('THYROID_MM_CONTRACT_V1\|tumor\|', research_id, surgery_episode_id, tumor_ordinal, surgery_date)))` |
| `path_report_id` | `'mmv1_pr_' \|\| lower(md5(concat('THYROID_MM_CONTRACT_V1\|pathrep\|', research_id, surgery_episode_id, min surgery_date)))` |
| Imaging / FNA / genetics fact IDs | Prefix `mmv1_i_`, `mmv1_fn_`, `mmv1_g_` with stable concatenation of episode keys (see script 128). |

Changing `H_NS` / prefixes breaks cross-release joins — treat as a semver bump.

## Legacy → contract mapping

| Contract object | Primary upstream sources |
|-----------------|--------------------------|
| `dim_patient_mm_v1` | `linkage_master_v1` (grain: `canonical_research_id`) |
| `map_patient_identifier_mm_v1` | `linkage_master_v1` + `mrn_crosswalk_v1` |
| `fact_surgery_mm_v1` | `operative_episode_detail_v2`, `linkage_master_v1` |
| `dim_nodule_mm_v1` | `imaging_nodule_master_v1` |
| `fact_imaging_mm_v1` | `imaging_nodule_master_v1`, `linkage_master_v1`, `event_date_audit_v2` (date fallback) |
| `fact_fna_mm_v1` | `fna_episode_master_v2`, `linkage_master_v1`, `event_date_audit_v2` |
| `fact_genetics_mm_v1` | `molecular_test_episode_v2`, `linkage_master_v1`, `event_date_audit_v2` |
| `fact_tumor_mm_v1` | `tumor_episode_master_v2`, `linkage_master_v1` (one row per tumor row — never collapsed) |
| `fact_path_report_mm_v1` | `tumor_episode_master_v2` aggregated per `(research_id, surgery_episode_id)` |
| `link_surgery_path_mm_v1` | `surgery_pathology_linkage_v3` JOIN `fact_tumor_mm_v1` for `tumor_instance_id` |
| `link_surgery_context_mm_v1` | `operative_episode_detail_v2`, `surgery_pathology_linkage_v3`, `preop_surgery_linkage_v3`, `fna_molecular_linkage_v3`, `pathology_rai_linkage_v3`, `patient_cross_domain_timeline_v2`, `linkage_master_v1` |

## Primary linkage rule

In `link_surgery_path_mm_v1`, `is_primary_link` is **TRUE** only when the v3 row is rank 1, analysis-eligible, tier ∈ {`exact_match`, `high_confidence`, `plausible`}, **`n_candidates = 1`**, and a matching `fact_tumor_mm_v1` row exists. Weak tiers, ambiguity, or missing tumor rows are excluded from primary (and surface in `val_ambiguous_multimodal_linkage_mm_v1` or context flags).

## Fail-closed validation tables

| Table | Meaning |
|-------|---------|
| `val_nodes_invariant_mm_v1` | Orphan person/surgery/tumor or primary link without tumor / episode mismatch |
| `val_side_lobe_mismatch_mm_v1` | Primary surgery–path or preop–surgery laterality conflict (isthmus excluded from mismatch) |
| `val_preop_temporal_order_mm_v1` | Preop after surgery on calendar; molecular ≫8d before FNA on scored primary rows |
| `val_ambiguous_multimodal_linkage_mm_v1` | Non-primary or ambiguous surgery–path and preop–surgery edges (review queue) |
| `val_multitumor_expansion_mm_v1` | Tumor counts per surgery: `tumor_episode_master_v2` vs `fact_tumor_mm_v1` |

**Promotion gate (example):**

```sql
SELECT
  (SELECT COUNT(*) FROM mm_contract_dev.val_nodes_invariant_mm_v1) AS n_node,
  (SELECT COUNT(*) FROM mm_contract_dev.val_multitumor_expansion_mm_v1) AS n_mt;
-- Require n_node = 0 AND n_mt = 0 for strict release; other val_* tables may be non-zero for operational review.
```

## Provenance and time columns

Every contract table includes: `mm_contract_version`, `mm_source_script`, `mm_built_at`, `mm_upstream_tables`, `mm_lineage_note`. Fact tables add `event_time` and `event_time_src` (`*_native`, `*_resolved`, `event_date_audit_v2_fallback`, etc.).

`COMMENT ON` metadata is applied by script 128 for tables and key columns.

## Rollback

```sql
DROP SCHEMA mm_contract_dev CASCADE;
```

No legacy tables are altered.

## Tests

`tests/test_multimodal_contract_mm_v1.py` — deterministic IDs, multitumor parity, node invariants, ambiguity routing, laterality and temporal audits.

## Unresolved / operational gaps

- Local or MotherDuck runs **fail fast** if any upstream table is missing — run scripts 22+ and 49 (and linkage repair stack) first.
- `val_ambiguous_multimodal_linkage_mm_v1` is expected to be **non-empty** in real data until manual review resolves multi-candidate pairs.
- Imaging–FNA v3 linkage is **not** re-materialized here; imaging facts remain keyed off `imaging_nodule_master_v1` only.
