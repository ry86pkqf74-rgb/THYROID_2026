# Multimodal analysis contract v1 (`mm_contract_dev`)

This release layer sits **on top of** legacy episode and v3 linkage tables. It does **not** replace scripts 22–25 / 49 or rename `research_id`. All contract tables retain lineage (`research_id`, `canonical_research_id` where applicable).

## Build

```bash
# Local DuckDB file (./thyroid_master.duckdb) — requires all native upstream tables (fail-closed default)
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py

# MotherDuck — writes **only** to schema `mm_contract_dev` (override requires
# MM_CONTRACT_MD_SCHEMA_OVERRIDE=1 and explicit MM_CONTRACT_SCHEMA)
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py --md

# CI / release gate: strict column + join-key checks; no bootstrap; blocking val_* must be empty
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py --md --strict-release

# Local dev stubs (schema-prefixed bootstrap tables — never for release)
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py --allow-bootstrap-dev
```

`--strict-release` cannot be combined with `--allow-bootstrap-dev`.

## Surrogate ID recipes (deterministic)

| ID | Formula (DuckDB) |
|----|-------------------|
| `person_id` | `'mmv1_p_' \|\| lower(md5(concat('THYROID_MM_CONTRACT_V1\|person\|', cast(canonical_research_id AS VARCHAR))))` |
| `surgery_id` | `'mmv1_s_' \|\| lower(md5(concat('THYROID_MM_CONTRACT_V1\|surgery\|', cast(research_id AS VARCHAR), '\|', cast(surgery_episode_id AS VARCHAR))))` |
| `nodule_id` (dim) | `'mmv1_n_' \|\| lower(md5(concat('THYROID_MM_CONTRACT_V1\|nodule\|', … research_id, exam_id, legacy nodule_id …)))` |
| `tumor_instance_id` | `'mmv1_t_' \|\| lower(md5(concat('THYROID_MM_CONTRACT_V1\|tumor\|', research_id, surgery_episode_id, tumor_ordinal, surgery_date)))` |
| `path_report_id` | `'mmv1_pr_' \|\| lower(md5(concat('THYROID_MM_CONTRACT_V1\|pathrep\|', research_id, surgery_episode_id, min surgery_date)))` |
| Imaging / FNA / genetics fact IDs | Prefix `mmv1_i_`, `mmv1_fn_`, `mmv1_g_` with stable concatenation of episode keys (see script 128). |
| Imaging–FNA link row | `'mmv1_ifna_' \|\| lower(md5(concat('THYROID_MM_CONTRACT_V1\|ifna\|', … research_id, nodule_id, imaging_exam_id, fna_episode_id …)))` |

Changing `H_NS` / prefixes breaks cross-release joins — treat as a semver bump.

## Required upstream tables (native catalog)

Without `--allow-bootstrap-dev`, **every** table below must exist on the connection before script 128 runs (see `mm_contract_upstream.UPSTREAM_KEYS`). Missing any table → **hard fail**.

| Logical name |
|--------------|
| `linkage_master_v1` |
| `mrn_crosswalk_v1` |
| `operative_episode_detail_v2` |
| `tumor_episode_master_v2` |
| `fna_episode_master_v2` |
| `molecular_test_episode_v2` |
| `imaging_nodule_master_v1` |
| `event_date_audit_v2` |
| `patient_cross_domain_timeline_v2` |
| `preop_surgery_linkage_v3` |
| `surgery_pathology_linkage_v3` |
| `fna_molecular_linkage_v3` |
| `pathology_rai_linkage_v3` |

Core tables (`operative_episode_detail_v2`, `tumor_episode_master_v2`, `molecular_test_episode_v2`, `imaging_nodule_master_v1`) are always required first.

**Optional (not in UPSTREAM_KEYS)** for richer imaging–FNA specimen matching: `fna_history` and accession/specimen columns on imaging, as in script 129.

## Required join keys (strict – non-NULL / resolvable date)

Script 128 materializes `val_contract_required_join_keys_mm_v1`. Rows appear when:

| Upstream | Rule |
|----------|------|
| `linkage_master_v1` | `research_id` or `canonical_research_id` IS NULL |
| `operative_episode_detail_v2` | `research_id` or `surgery_episode_id` IS NULL |
| `tumor_episode_master_v2` | any of `research_id`, `surgery_episode_id`, `tumor_ordinal` IS NULL |
| `fna_episode_master_v2` | `research_id` or `fna_episode_id` IS NULL |
| `molecular_test_episode_v2` | `research_id` or `molecular_episode_id` IS NULL |
| `imaging_nodule_master_v1` | `research_id`, `exam_id`, or `nodule_id` IS NULL; OR `exam_date` IS NULL **and** there is no `event_date_audit_v2` row for that `research_id` with `domain = 'imaging'` |

With `--strict-release`, this table must be **empty**.

**Column existence (strict):** `UPSTREAM_REQUIRED_COLUMNS` in `scripts/mm_contract_upstream.py` lists columns that must be present on each upstream relation (verified via `DESCRIBE`).

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
| `imaging_fna_linkage_mm_v1` | **Re-materialized in-schema** using the same rules as script 129 (wide candidates → ranked links). |
| `link_imaging_fna_mm_v1` | `imaging_fna_linkage_mm_v1` joined to `fact_imaging_mm_v1` / `fact_fna_mm_v1` for `imaging_id` / `fna_id` + contract flags. |
| `review_queue_imaging_fna_mm_v1` | Built with 129 review logic (ambiguous / discordant side / size drift). |
| `val_imaging_fna_linkage_audit_v1` | Aggregate audit counts for the imaging–FNA step. |
| `val_imaging_fna_contract_blockers_mm_v1` | **Subset** of `review_queue_imaging_fna_mm_v1`: rows with `review_reason = 'ambiguous_multimatch'` only. Empty under strict-release when every multimatch nodule has a deterministic primary link after script 129 rules. Discordant laterality and size drift stay in the review queue for operators but do **not** populate this blocker table. |

Running script **129** alone is optional for exploration or MotherDuck exports; the contract build does **not** depend on a pre-existing `imaging_fna_linkage_mm_v1` in the default catalog — it writes `{schema}.imaging_fna_linkage_mm_v1` as part of 128.

### Imaging → FNA contract columns (`link_imaging_fna_mm_v1`)

| Column | Meaning |
|--------|---------|
| `imaging_id` | `fact_imaging_mm_v1.imaging_fact_id` |
| `fna_id` | `fact_fna_mm_v1.fna_fact_id` |
| `link_confidence` | `1.0` specimen key match; `0.85` temporal (0–90d US before FNA) path; else `0.75` |
| `is_primary_link` | Primary under 129: unique specimen match when unambiguous; else single candidate; else (when `n_specimen_matches_on_nodule <= 1`) deterministic choice = first row per nodule by `fna_date`, `fna_episode_id`. Suppressed when two or more specimen-key ties exist on the nodule. |
| `review_reason` | Joined from `review_queue_imaging_fna_mm_v1` when present |
| `flag_multi_fna_nodule` | `n_candidates_for_nodule > 1` |
| `flag_ambiguous_linkage` | True on a row when that row is not primary while the nodule has multiple candidates, or when multiple specimen matches exist on the nodule (per-row; a nodule may still have exactly one primary link). |
| `flag_discordant_side` | Review reason `discordant_laterality` |
| `flag_size_drift` | `size_drift_ratio > 0.20` or review `size_drift_gt_20pct` |

## Primary linkage rule

In `link_surgery_path_mm_v1`, `is_primary_link` is **TRUE** only when the v3 row is rank 1, analysis-eligible, tier ∈ {`exact_match`, `high_confidence`, `plausible`}, **`n_candidates = 1`**, and a matching `fact_tumor_mm_v1` row exists. Weak tiers, multi-candidate rank-1 rows, or missing tumor rows are excluded from primary; use `context_flags` on the link row (e.g. `ambiguous_or_weak_excluded_from_primary`, `no_matching_fact_tumor_row`) for explanation. **`val_ambiguous_multimodal_linkage_mm_v1`** does not list those cases; it is reserved for **rank-1 edges still in the `unlinked` tier** (hard pathology–surgery linkage gaps).

Script 129 uses **TRIM** on imaging/FNA laterality and treats **`isthmus` / `isthmus only`** as side-compatible for eligibility (`side_ok`).

## Fail-closed validation tables

| Table | Meaning |
|-------|---------|
| `val_contract_required_join_keys_mm_v1` | NULL / unresolved join keys (see above) |
| `val_nodes_invariant_mm_v1` | Orphan person/surgery/tumor or primary link without tumor / episode mismatch |
| `val_side_lobe_mismatch_mm_v1` | Primary surgery–path or preop–surgery laterality conflict (**isthmus** and **bilateral** wording on either side excluded — bilateral is treated as compatible with lobe-specific preop/path labels) |
| `val_preop_temporal_order_mm_v1` | Preop **more than 7 days after** surgery on calendar; molecular ≫8d before FNA on scored primary rows |
| `val_ambiguous_multimodal_linkage_mm_v1` | **Strict:** rank-1 `link_surgery_path_mm_v1` rows with `linkage_confidence_tier = 'unlinked'` only. Preop ambiguity is not duplicated here (see linkage metrics / `link_surgery_context_mm_v1`). |
| `val_multitumor_expansion_mm_v1` | Tumor counts per surgery: `tumor_episode_master_v2` vs `fact_tumor_mm_v1` |
| `val_imaging_fna_contract_blockers_mm_v1` | **Strict:** `ambiguous_multimatch` review pairs only (see mapping table above). |

### Strict-release acceptance criteria

`--strict-release` is intended for CI / tagged releases. The build **fails** if **any** of the following hold:

1. **Bootstrap / stubs:** any dev-only upstream substitution was used (equivalent: any key in `ensure_upstream_sources` resolve target ≠ bare table name). With native upstreams only, this list is empty.
2. **Missing upstream table:** any member of `UPSTREAM_KEYS` is absent when `allow_bootstrap=False` (default).
3. **Missing required column:** `DESCRIBE` on any resolved upstream lacks a column listed in `UPSTREAM_REQUIRED_COLUMNS`.
4. **Join-key audit:** `val_contract_required_join_keys_mm_v1` has **one or more rows**.
5. **Blocking validation tables non-empty:** **all** of the following must have **row count 0**:
   - `val_contract_required_join_keys_mm_v1`
   - `val_nodes_invariant_mm_v1`
   - `val_multitumor_expansion_mm_v1`
   - `val_side_lobe_mismatch_mm_v1`
   - `val_preop_temporal_order_mm_v1`
   - `val_ambiguous_multimodal_linkage_mm_v1`
   - `val_imaging_fna_contract_blockers_mm_v1`

**Operational note:** `review_queue_imaging_fna_mm_v1` may still carry discordant laterality, size drift, etc., while strict-release passes — only the seven `val_*` tables above must be empty. Residual **`unlinked`** rank-1 surgery–path rows or **true** multimatch-without-primary imaging–FNA cases will still fail `val_ambiguous_multimodal_linkage_mm_v1` or `val_imaging_fna_contract_blockers_mm_v1`.

**Non-strict promotion example (legacy):**

```sql
SELECT
  (SELECT COUNT(*) FROM mm_contract_dev.val_nodes_invariant_mm_v1) AS n_node,
  (SELECT COUNT(*) FROM mm_contract_dev.val_multitumor_expansion_mm_v1) AS n_mt;
-- Older workflow: n_node = 0 AND n_mt = 0; other val_* could be non-zero.
-- Strict workflow: all seven blocking tables above must be zero.
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

- `tests/test_multimodal_contract_mm_v1.py` — deterministic IDs, multitumor parity, node invariants, ambiguity routing, laterality and temporal audits, **strict-release gates**, **imaging–FNA contract integration** (specimen, multi-FNA, discordant side, size drift, ordinals).
- `tests/test_imaging_fna_linkage_mm_v1.py` — isolated script 129 linkage logic.

## Unresolved / operational gaps

- Local or MotherDuck runs without `--allow-bootstrap-dev` **fail fast** if any upstream table is missing — run scripts 22+ and 49 (and linkage repair stack) first.
- If upstream linkage leaves **rank-1 surgery–path** rows in **`unlinked`** or imaging–FNA pairs in **`ambiguous_multimatch`** with no deterministic primary, `--strict-release` will still fail until those upstream rules or data are fixed.
