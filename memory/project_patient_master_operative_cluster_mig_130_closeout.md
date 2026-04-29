# `canonical_patient_master` operative cluster — mig_130 close-out (Lane 22)

**Date:** 2026-04-29  
**Migration:** `qc_framework_v1/migrations/130_patient_master_operative_cluster_signoff_20260429.sql`  
**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck, `MOTHERDUCK_DATABASE`)

## Scope

- **233 columns** verified under the Lane-22 probe predicate (`op_%`, `surg%`, `%surgery%`, `operative_%`, `%procedure%`, `nlp_ne_procedures%`, `nsqip_%`).
- Live count exceeds the historic **~125** estimate: `LIKE 'op_%'` also matches **`ops_*`** thyroid operative sheet fields (**48**), and CPM carries **102** `nsqip_*` linkage columns (not ~4).

## Buckets

| Bucket | n | verification_method (abbrev) |
|--------|---|------------------------------|
| `op_*` excl. `ops_*` | 60 | derivation replay vs `canonical_operative_events_v1` (tri-state NULL bulk; material TRUE/FALSE agreement 0 drift on probed flags) |
| `ops_*` | 48 | thyroid operative sheet feed lineage |
| `nsqip_*` | 102 | external NSQIP study/registry provenance |
| `surg_*` | 6 | procedure spine + mig_118 family |
| `nlp_ne_procedures_*` | 2 | rollup vs procedures cluster |
| `pshx_nlp_*` | 3 | PMH prior-neck NLP (probe bleed-through) |
| Surgery spine / cross-domain | 12 | dates, intervals, `age_at_surgery`, `biochemical_tg_nadir_after_surgery` |

## CPM table status

- `canonical_table_signoff_registry_v1`: **233 verified**, **1361 not_started**, **4 na**, **table_status = `in_progress`**.  
- **`signoff_migration` not overwritten** (partial slice; only counts + notes + `signed_off_ts` updated in 130h).

## Carry-forwards

- **CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE** — `first_surgery_date`, `surg_first_date` remain **TIMESTAMP**; calendar SSOT is **`first_surgery_date_v2` (DATE)**. Notes appended in **130i**; umbrella **CF-100-DATE-RETYPE**.
- **Multi-source spine drift** — `first_surgery_date_v2` vs `MIN(canonical_operative_events_v1.surgery_date_native)::DATE` differs on **102** patients (expected: spine ≠ operative-events-only).

## 5-gate audit

- **Gate 4** (verified tables only): **0** missing metadata on verified-table columns; CPM is not `verified` so new CPM cols do not affect this gate until full CPM close-out.

## Next slices

Pathology, lymph_node, labs, pmh_psh, us_imaging, rai, recurrence (defer if Script 203 lands), fna, ete, survival, medications, molecular, complications, frozen_section, demographics, **other** (~975 residual).
