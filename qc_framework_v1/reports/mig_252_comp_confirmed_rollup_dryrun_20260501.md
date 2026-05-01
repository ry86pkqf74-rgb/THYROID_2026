# mig_252 Complication Confirmed Rollup Dry-Run

Date: 2026-05-01  
Database: `thyroid_canonical_publication_v1_0`  
Dry-run helper: `qc_framework_v1/scripts/build_mig252_comp_rollup_dryrun.py`  
Artifact directory: `exports/mig252_comp_rollup_dryrun_20260501T101111Z/`  
Persistent `main.*` mutations performed: 0

## Lineage Finding

The verified canonical rollup builder in `scripts/364_complications_consolidation.py` uses the correct predicate for tiered rollup columns:

- definitive: `finding_status = 'present' AND evidence_strength = 'definitive'`
- probable_or_better: `finding_status = 'present' AND evidence_strength IN ('definitive','probable')`
- any_evidence: `finding_status = 'present'`

The CPM defect is downstream: `scripts/364_cpm_feeder_repoint.py` populated `any_confirmed_complication_flag` from `n_complication_types_present > 0`, where `n_complication_types_present` means any present evidence, not strict confirmed evidence. CPM also retains older alias columns whose values drifted from the canonical event predicates.

## Source Event Pattern

Live `main.canonical_complications_events_v1`: 5,050 rows, 2,481 patients.

Dominant contamination pattern confirmed:

| complication_type | dominant non-confirming pattern | patients |
|---|---:|---:|
| chyle_leak | absent + possible | 1,575 |
| seroma | absent + possible | 843 |
| rln_injury | absent + possible | 672 |
| hematoma | absent + possible | 205 |
| hypoparathyroidism | absent + possible | 378 |

## Dry-Run Aggregate Impact

| metric | current | proposed strict | delta |
|---|---:|---:|---:|
| `any_confirmed_complication_flag` patients | 2,490 | 400 | -2,090 |
| `any_confirmed_complication` patients | 2,490 | 400 | -2,090 |
| `n_confirmed_complications` positive patients | 2,490 | 400 | -2,090 |
| `n_confirmed_complications` sum | 3,869 | 460 | -3,409 |

## Key Per-Column Diffs

| column | current | proposed strict | note |
|---|---:|---:|---|
| `comp_seroma_confirmed` | 621 | 39 | fixes absent/possible contamination |
| `comp_hematoma_confirmed` | 28 | 68 | CPM was underfilled vs strict canonical events |
| `comp_rln_injury_confirmed` | 39 | 21 | drops non-strict cases, adds 15 strict misses |
| `comp_chyle_leak_confirmed` | 0 | 3 | CPM was underfilled vs strict canonical events |
| `comp_hypocalcemia_confirmed` | 5 | 9 | adds strict canonical misses |
| `comp_hypoparathyroidism_confirmed` | 1 | 296 | CPM was severely underfilled vs strict canonical events |
| `comp_vc_paralysis_confirmed` | 19 | 23 | canonical vocal-cord strict events |
| `comp_vc_paresis_confirmed` | 13 | 0 | source-strict policy: no separate `vc_paresis` event type exists |
| `comp_wound_infection_confirmed` | 0 | 0 | unchanged |

The proposed migration also repairs `_definitive`, `_probable_or_better`, `_any_evidence`, and `_suspected` variants where CPM columns exist, using the predicates in the dispatch.

## M038 Cohort Impact

| subset | n | current any confirmed | proposed strict any confirmed |
|---|---:|---:|---:|
| gland weight >= 200g | 475 | 146 | 10 |
| gland weight < 200g | 8,655 | 1,844 | 307 |
| weight NULL | 1,741 | 500 | 83 |

The primary M038 acceptance check is therefore satisfied in dry-run: the >=200g cohort drops from 146 to 10 events.

## Migration Artifact

Unapplied migration authored at `qc_framework_v1/migrations/252_comp_confirmed_rollup_fix_20260501.sql`.

The migration will, after sign-off:

- Snapshot `main.canonical_patient_master` to `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre252_comp_rollup_20260501`.
- Build a session TEMP corrected rollup from `main.canonical_complications_events_v1`.
- Update CPM columns in place via `UPDATE`, preserving all unrelated columns/signoff rows.
- Refresh `canonical_column_verification_registry_v1` and `canonical_table_signoff_registry_v1` for affected CPM columns.
- Insert one provenance row into `manuscript_workspace.cpm_reconciliation_provenance_v1`.

## Sign-Off Decision Needed

Approve or revise the vocal-cord paresis alias policy before apply:

- Proposed source-strict policy: `comp_vc_paresis_confirmed = FALSE` and `comp_vc_paresis_suspected = FALSE`, because `canonical_complications_events_v1` contains no separate `vc_paresis` complication type.
- Alternative: map `comp_vc_paresis_*` to `vocal_cord_paralysis` aliases, which preserves a broader voice-injury bucket but is not source-strict.

No live `main.*` update should run until this policy and the aggregate diff are signed off.

## Apply Result

Logan approved apply on 2026-05-01 with the source-strict vocal-cord paresis policy (`comp_vc_paresis_confirmed = FALSE`, `comp_vc_paresis_suspected = FALSE`). The migration was applied to MotherDuck after moving the cross-database archive snapshot outside the canonical-DB mutation transaction, because MotherDuck permits writes to only one database within a transaction.

Post-apply artifact directory: `exports/mig252_comp_rollup_postapply_20260501T101901Z/`.

Post-apply verification:

| check | result |
|---|---:|
| CPM rows | 10,871 |
| CPM distinct `research_id` | 10,871 |
| `any_confirmed_complication_flag` patients | 400 |
| `n_confirmed_complications` sum | 460 |
| CPM rows with NULL `cpm_built_at` | 0 |
| M038 gland weight >=200g denominator | 475 |
| M038 gland weight >=200g confirmed events | 10 |
| Column registry rows stamped to mig_252 | 57 verified |
| CPM table signoff | verified |
| CPM provenance rows for mig_252 | 1 |

Publication QC gates from the post-apply packet remained green: gate1 verified tables/distinct objects = 218/218; gates 2-5 = 0; cohort parity = TRUE; latest column registry batch = `mig_252_comp_confirmed_rollup_fix_20260501`.