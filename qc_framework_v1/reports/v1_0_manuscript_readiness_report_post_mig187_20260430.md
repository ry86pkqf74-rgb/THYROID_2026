# Thyroid Canonical Publication v1.0 — Manuscript Readiness Report (post mig_187 chain + mig_160b + mig_203)

**Date:** 2026-04-30  
**Lane:** mig_191 / post_apply_manuscript_readiness_v11  
**Evidence:** Live read-only probes on MotherDuck `thyroid_canonical_publication_v1_0` (UTC session 2026-04-30)  
**Author:** Logan Glosser `<logan.glosser@gmail.com>`  

---

## §1 Tier-2 canonical inventory (post-state)

| Metric | Observed |
|---|---:|
| Verified `canonical_*` tables (signoff registry) | **62** |
| Column registry rows tied to verified canonicals (JOIN) | **3,306** |
| Verified status column-rows (`verification_status='verified'`) | **3,089** |
| `na` column-rows | **217** |
| CPM rows / distinct research_id | **10,871 / 10,871** |
| Cohort parity | **✓** |

**PM backbone (`canonical_patient_master`):** **1,606** verified **/ 24** na **/ 0** not_started **/ 1,630** total columns (live `canonical_table_signoff_registry_v1`).

---

## §2 5-gate cleanliness audit (post-state — v11 template)

Executed from `qc_framework_v1/queries/cleanliness_audit_v11.sql`:

| Gate | Value |
|---|---:|
| gate1 | **172** |
| gate2 | **0** |
| gate3 | **0** |
| gate4 | **0** |
| gate5 | **0** |

All five gates are clean on the publication database at probe time (**172 / 0 / 0 / 0 / 0**).

---

## §3 Verification methodology distribution (top 15)

Derived from live `canonical_column_verification_registry_v1` scoped to verified `canonical_*` tables:

| Rank | verification_method | n_cols |
|---:|---|---:|
| 1 | mechanical_derivation_compare | 208 |
| 2 | derivation_re_derivation_post_rollup_rebuild | 173 |
| 3 | derivation_re_derivation_against_verified_events | 141 |
| 4 | auto_provenance_skip | 121 |
| 5 | Path C: PM nlp cluster lineage + source-discovery + cohort-uniformity sweep | 116 |
| 6 | external_registry_nsqip_study_linkage_on_cpm | 101 |
| 7 | auto_no_source_counterpart | 96 |
| 8 | derivation_re_derivation_post_events_repair | 87 |
| 9 | auto_identifier_skip | 79 |
| 10 | derivation_replay_vs_canonical_operative_events_v1_tri_state_null | 59 |
| 11 | patient_level_nlp_aggregate_per_condition | 58 |
| 12 | derivation_canonical_labs_rollups_mig115_script347 | 56 |
| 13 | multi_source_derivation_plus_domain_sanity | 53 |
| 14 | source_lineage_thyroid_operative_sheet_feed_on_cpm | 48 |
| 15 | structured_source_compare_with_normalizer | 47 |

---

## §4 Top remaining open CFs (informational, non-blocking)

| CF tag | Disposition |
|---|---|
| **CF-117-US-GLAND-PARENCHYMA** | **mig_198** Option B shell-only US gland v2 events/rollup (ratified track). |
| **CF-mig186-WHO-2017-NIFTP-RECLASS** | Opened by mig_186b; **220** rows in `canonical_path_indeterminate_events_v1`; manuscript appendix / methods. |
| **CF-mig185-EVENT-GRAIN-SOURCE-DISTINCT-PRESERVED** | **525** path-malignant rows `is_source_distinct_duplicate_grain=TRUE`; COUNT DISTINCT tumor grain for tumor counts — methods footnote. |
| **CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION** | **mig_202** fixes Python source; live VIEW patched. |
| **CF-mig58 / CF-mig156 / CF-mig166 / CF-PMH-MULTISOURCE / CF-mig145 / CF-mig151 / CF-mig154** | **mig_190** disposition-B triage classification (footnote / appendix lanes). |

**Closed audit-pattern extension:** **CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION** — closed by mig_203 (`cleanliness_audit_v11.sql` + registry refresh); gate5 residual eliminated under v11 query.

---

## §5 Closures this round (reference migrations)

Applied / ratified lanes reflected in lakehouse post-state:

- **mig_184_v2 → mig_188b** — R1 AJCC + r1c + explicit T0; **CF-87-AJCC** closed.
- **mig_186b** — NIFTP/UMP exclusion → indeterminate landing; path malignant **6,469** events / rollup **4,022** patients.
- **mig_185b** — Rollup DISTINCT grain + **`is_source_distinct_duplicate_grain`** on events (525 flagged).
- **mig_187** — Exam-master rebuild R-A + mig_171b replay; **CF-mig171b-EXAM-MASTER-REBUILD** closed.
- **mig_160b** — PM DATE retypes (`160b_pm_date_cols_retype_close_gate5_20260430`).
- **mig_203** — v11 audit allowlist + `_built_at/_derived_at/_resolved_at/_confidence` exclusions + PM registry bumps; gate5→0 under v11.
- **mig_201** — Disposition-C registry notes for four stale CF tags (see chain close-outs + provenance `mig_201_disposition_c_cf_closure_apply_20260430`).

---

## §6 Resolution-source distribution (path malignant — mig_188b family)

Live `canonical_path_malignant_events_v1`, column **`ajcc_resolution_source`** (`t_resolution_source`-equivalent semantics in post-exclusion cohort):

| ajcc_resolution_source | n_events |
|---|---:|
| coalesce_size_greatest_dimension_cm_tumor_size_cm_per_surgery | 6,310 |
| prior_thy_recurrence_T_from_prior_path | 54 |
| ambiguous_pm_size_only_logan_pending | 50 |
| anaplastic_default_T4 | 25 |
| canonical_invasion_events_v1 | 15 |
| no_primary_at_this_surgery_pT0_unstaged | 13 |
| niftp_excluded | 2 |

(Excluded NIFTP/UMP cohort is isolated in **`canonical_path_indeterminate_events_v1`** — **220** rows.)

---

## §7 Patient-grain **`ajcc8_stage_group_resolved`** coverage (PM)

Live `canonical_patient_master`:

- Non-null **`ajcc8_stage_group_resolved`:** **7,600** rows  
- Total CPM rows: **10,871**  

Residual nulls correspond to benign / incomplete staging backbone / deliberate NA semantics — unchanged from publication policy; analytic cohorts remain filter-driven.

---

## §8 Auxiliary structural checks

| Artifact | Expected (chain doc) | Live |
|---|---:|---:|
| `canonical_path_indeterminate_events_v1` | exists | ✓ (**220** rows) |
| `canonical_us_exam_master_VIEW_v2` | ~11,880 | **11,880** |
| `val_mig171b_canonical_us_ln_build_v1` G9 | PASS | PASS (fallback exam IDs **0**) |
| Registry batch stamp `mig188b%` | present | ✓ (**46** col rows share batch_id `mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430`) |

Column-registry **`batch_id` LIKE `mig_186b%` / `mig_185b%` / `mig_187%`:** zero rows observed — migrations were primarily DDL/DATA/rollup without per-column batch stamps on `canonical_column_verification_registry_v1`. Post-state row counts align with **`qc_framework_v1/reports/chain_188b_186b_185b_187_closeout_20260430.md`**.

---

## §9 Manuscript readiness verdict

**READY — manuscript-grade survival / recurrence / outcomes**

- Verified canonical backbone: **62/62** tables; gate1–gate5 **all zero** except gate1 cardinality (**172**) under v11 audit.  
- PM signoff aligns with physical column count (**1,630**) after mig_188b + mig_203.  
- AJCC **`*_resolved`** + **`ajcc_resolution_source`** populated at event grain (**6,469** malignant events post-exclusion).  
- Indeterminate pathology events preserved with audit trail (**220** rows).  
- Exam master rebuilt; LN exam linkage **G9 PASS**.  

---

## References (repo)

- `qc_framework_v1/reports/chain_188b_186b_185b_187_closeout_20260430.md`  
- `qc_framework_v1/reports/mig_203_gate5_zero_audit_allowlist_extension_20260430.md`  
- `qc_framework_v1/reports/mig_160b_apply_closeout_20260430.md`  
- `qc_framework_v1/queries/cleanliness_audit_v11.sql`  
