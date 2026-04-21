# Lab Consolidation — Script 347 Report

Run timestamp (UTC): `20260421T164325Z`

## Pre-state inventory

| Object | Type | Rows | Patients |
|---|---|---:|---:|
| main.longitudinal_lab_canonical_v1 | TABLE | 75,291 | 3,581 |
| main.thyroglobulin_lab_canonical_v1 | TABLE | 74,258 | 3,124 |
| main.lab_cross_wave_dedup_map_v1 | TABLE | 21,761 | 1,796 |

## Post-state inventory

| Object | Type | Rows | Patients |
|---|---|---:|---:|
| main.canonical_labs_thyroglobulin_v1 | TABLE | 53,006 | 3,124 |
| main.canonical_labs_tsh_v1 | TABLE | 556 | 449 |
| main.canonical_labs_pth_v1 | TABLE | 200 | 184 |
| main.canonical_labs_calcium_v1 | TABLE | 187 | 166 |
| main.canonical_labs_vitamin_d_v1 | TABLE | 86 | 82 |
| main.longitudinal_lab_VIEW_v1 | VIEW | 54,035 | 3,581 |
| main.thyroglobulin_lab_VIEW_v1 | VIEW | 53,006 | 3,124 |
| views_readable.Labs_Thyroglobulin | VIEW | 53,006 | — |
| views_readable.Labs_TSH | VIEW | 556 | — |
| views_readable.Labs_PTH | VIEW | 200 | — |
| views_readable.Labs_Calcium | VIEW | 187 | — |
| views_readable.Labs_VitaminD | VIEW | 86 | — |
| views_readable.Labs_Longitudinal | VIEW | 54,035 | — |

## Net delta

- Pre rows (longitudinal): **75,291**
- Post rows (sum of 5 per-analyte tables): **54,035**
- Cross-wave dedup removed: **21,256** rows

## Per-analyte distribution

| Analyte | Rows | Patients | Min datetime | Max datetime |
|---|---:|---:|---|---|
| Tg | 26,224 | 2,925 | 2001-01-04 10:20:00 | 2025-11-19 19:41:00 |
| TgAb | 26,782 | 3,038 | 2001-01-29 16:00:00 | 2025-11-19 19:41:00 |
| tsh | 556 | 449 | 1990-04-27 00:00:00 | 2025-02-18 00:00:00 |
| pth | 200 | 184 | 1990-06-25 00:00:00 | 2025-11-04 00:00:00 |
| calcium | 187 | 166 | 1990-06-25 00:00:00 | 2025-09-16 00:00:00 |
| vitamin_d | 86 | 82 | 2018-01-01 00:00:00 | 2024-12-20 00:00:00 |

## Source breakdown (longitudinal view)

| Analyte | Source | Rows |
|---|---|---:|
| anti_thyroglobulin | structured_ehr_tg | 26,782 |
| calcium | institutional_append | 187 |
| pth | institutional_append | 200 |
| thyroglobulin | structured_ehr_tg | 26,224 |
| tsh | institutional_append | 514 |
| tsh | clinical_note | 42 |
| vitamin_d | institutional_append | 86 |

## value_correction_note frequencies

| Table | Note | Rows |
|---|---|---:|
| canonical_labs_thyroglobulin_v1 | none | 51,077 |
| canonical_labs_thyroglobulin_v1 | unparseable_string | 1,926 |
| canonical_labs_thyroglobulin_v1 | titer_denominator_extracted | 3 |
| canonical_labs_tsh_v1 | unit_suffix_stripped | 331 |
| canonical_labs_tsh_v1 | none | 219 |
| canonical_labs_tsh_v1 | unparseable_string | 4 |
| canonical_labs_tsh_v1 | divided_by_10 | 2 |
| canonical_labs_pth_v1 | none | 165 |
| canonical_labs_pth_v1 | unit_suffix_stripped | 35 |
| canonical_labs_calcium_v1 | none | 104 |
| canonical_labs_calcium_v1 | unit_suffix_stripped | 53 |
| canonical_labs_calcium_v1 | divided_by_100 | 18 |
| canonical_labs_calcium_v1 | nulled_unrecoverable_implausible | 7 |
| canonical_labs_calcium_v1 | divided_by_10 | 3 |
| canonical_labs_calcium_v1 | unparseable_string | 1 |
| canonical_labs_calcium_v1 | unit_suffix_stripped,nulled_unrecoverable_implausible | 1 |
| canonical_labs_vitamin_d_v1 | unit_suffix_stripped | 43 |
| canonical_labs_vitamin_d_v1 | none | 40 |
| canonical_labs_vitamin_d_v1 | divided_by_10 | 1 |
| canonical_labs_vitamin_d_v1 | divided_by_100 | 1 |
| canonical_labs_vitamin_d_v1 | unparseable_string | 1 |

## Patched downstream consumer scripts

| Script | Pyflakes | AST parse |
|---|:---:|:---:|
| scripts/203_canonical_recurrence.py | ✓ | ✓ |
| scripts/223_ingest_and_publish.py | ✓ | ✓ |
| scripts/223_publish_canonical.py | ✓ | ✓ |
| scripts/253_lab_orphan_triage.py | ✓ | ✓ |
| scripts/255_rebuild_rai_tg_rollups.py | ✓ | ✓ |
| scripts/272_canonical_cleanup_phase1.py | ✓ | ✓ |
| scripts/273_canonical_cleanup_phase2_3.py | ✓ | ✓ |
| scripts/277_canonical_cleanup_phase7_verification.py | ✓ | ✓ |
| scripts/286_cpm_missing_data_backfill.py | ✓ | ✓ |
| scripts/prompt6_349_max_stimulated_tg.py | ✓ | ✓ |
| scripts/prompt6_352_wiring_gap_sweep.py | ✓ | ✓ |
| scripts/113_tg_lab_ingestion.py | ✓ | ✓ |

Script 113 (`scripts/113_tg_lab_ingestion.py`) is the legacy ingestion builder. It is FROZEN pending Script 348 refactor to write directly to the 5 per-analyte canonicals.

## Verification (PASS/FAIL)

- [PASS] canonical_labs_thyroglobulin_v1 rows=53,006 in [50,000,54,500]
- [PASS] canonical_labs_tsh_v1 rows=556 in [500,800]
- [PASS] canonical_labs_pth_v1 rows=200 in [180,240]
- [PASS] canonical_labs_calcium_v1 rows=187 in [170,220]
- [PASS] canonical_labs_vitamin_d_v1 rows=86 in [80,110]
- [PASS] canonical_labs_thyroglobulin_v1: 0 rows with analyte NOT IN (Tg,TgAb) (got 0)
- [PASS] canonical_labs_thyroglobulin_v1: Tg rows always ng/mL (violations=0)
- [PASS] canonical_labs_thyroglobulin_v1: TgAb rows always IU/mL (violations=0)
- [PASS] canonical_labs_tsh_v1: 100% unit_standardized=mIU/L (violations=0)
- [PASS] canonical_labs_pth_v1: 100% unit_standardized=pg/mL (violations=0)
- [PASS] canonical_labs_calcium_v1: 100% unit_standardized=mg/dL (violations=0)
- [PASS] canonical_labs_vitamin_d_v1: 100% unit_standardized=ng/mL (violations=0)
- [PASS] canonical_labs_thyroglobulin_v1: research_id/lab_datetime/source all NOT NULL (nulls=(0, 0, 0))
- [PASS] canonical_labs_tsh_v1: research_id/lab_datetime/source all NOT NULL (nulls=(0, 0, 0))
- [PASS] canonical_labs_pth_v1: research_id/lab_datetime/source all NOT NULL (nulls=(0, 0, 0))
- [PASS] canonical_labs_calcium_v1: research_id/lab_datetime/source all NOT NULL (nulls=(0, 0, 0))
- [PASS] canonical_labs_vitamin_d_v1: research_id/lab_datetime/source all NOT NULL (nulls=(0, 0, 0))
- [PASS] all rows in valid source set (violations=0)
- [PASS] 0 rows with source='other_structured' (got 0)
- [PASS] Tg structured_ehr_tg HH:MM count >= 26000 (got 52999)
- [PASS] cross-wave dedup removed 21,256 rows (target 21000-21500)
- [PASS] is_censored=TRUE rows have value_numeric NOT NULL (violations=0)
- [PASS] canonical_labs_tsh_v1 NULL numeric rows < 20 (got 4)
- [PASS] canonical_labs_thyroglobulin_v1 Tg max(value_numeric) (uncensored)=8917.9 <= 10000.0
- [PASS] canonical_labs_thyroglobulin_v1 TgAb max(value_numeric) (uncensored)=25600.0 <= 40000.0
- [PASS] canonical_labs_tsh_v1 max(value_numeric) (uncensored)=150.0 <= 150.0
- [PASS] canonical_labs_pth_v1 max(value_numeric) (uncensored)=1399.0 <= 3000.0
- [PASS] canonical_labs_calcium_v1 max(value_numeric) (uncensored)=20.0 <= 20.0
- [PASS] canonical_labs_vitamin_d_v1 max(value_numeric) (uncensored)=83.0 <= 200.0
- [PASS] calcium min=4.0 >= 4
- [PASS] pth min=1.0 > 0
- [PASS] vitamin_d min=10.7 > 0
- [PASS] no negative value_numeric anywhere (got 0)
- [PASS] non-unit-strip correction notes in [15, 5000] (got 1968)
- [PASS] titer rows are TgAb with proper note (violations=0)
- [PASS] longitudinal_lab_VIEW_v1 rows (54,035) == sum tables (54,035)
- [PASS] thyroglobulin_lab_VIEW_v1 rows (53,006) == canonical_labs_thyroglobulin_v1 (53,006)
- [PASS] main.longitudinal_lab_canonical_v1 no longer exists (count=0)
- [PASS] main.thyroglobulin_lab_canonical_v1 no longer exists (count=0)
- [PASS] main.lab_cross_wave_dedup_map_v1 no longer exists (count=0)
- [PASS] cancer-cohort key coverage: 0 archived keys with no surviving row
- [PASS] CPM invariant: (10871, 10871, 0)

## CPM invariant

- pre-build: (10871, 10871, 0) ✓
- post-build: (10871, 10871, 0) ✓

## Archive snapshots

- `"Thyroid 2026 UPdated"."archive_pub_v1_0"."longitudinal_lab_canonical_v1_pre347_20260421T164325Z"`
- `"Thyroid 2026 UPdated"."archive_pub_v1_0"."thyroglobulin_lab_canonical_v1_pre347_20260421T164325Z"`
- `"Thyroid 2026 UPdated"."archive_pub_v1_0"."lab_cross_wave_dedup_map_v1_pre347_20260421T164325Z"`
