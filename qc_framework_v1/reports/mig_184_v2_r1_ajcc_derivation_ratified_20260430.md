# mig_184_v2 — R1 AJCC derivation RATIFIED

**Run ID:** `mig_184_v2_r1_ajcc_derivation_ratified_20260430`
**Run timestamp (UTC):** `2026-04-30T01:55:11.890471+00:00`
**Posture:** read-only MotherDuck SELECTs + local artifact authoring only; no MotherDuck DDL/DML executed.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Supersedes:** `17b5d8a` / old mig_184 scoping artifacts.

## 1. Ratified 8-rule spec (Logan-locked)

| # | Rule | Decision |
|---:|---|---|
| 1 | AJCC version | AJCC 8 (2018 revision) |
| 2 | gross_ete=1 + microscopic-text contradiction | Trust qualifier → no upgrade |
| 3 | N1 unspecified | Keep as N1 at path-event grain; split only at PM grain from upstream central/lateral evidence |
| 4 | Stage-group computation grain | PM grain only; path-event grain holds T/N/M only |
| 5 | Mixed histology | Track components separately; manuscript-default stage_group_resolved uses more aggressive component MTC > PTC > FTC |
| 6 | T4 invasion rules | gross_ete=1 → T3b; laryngeal/tracheal/esophageal/RLN → T4a; prevertebral/mediastinal/carotid → T4b |
| 7 | Size-unavailable | COALESCE(size_greatest_dimension_cm, tumor_size_cm_per_surgery); PTMC without size → T1a; NIFTP exclude; anaplastic → T4; residual stays pending |
| 8 | Age-unknown | No issue; age_at_surgery is complete |

Implementation posture in the SQL artifact is marked `LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY`. The SQL is authored but not executed by this lane.

## 2. Cross-source drift cohort under R1 derivation

Baseline from mig_182: path-event stored-vs-findings AJCC8 T-stage mismatch was 28.81%; malignant patient-grain CPM vs dominant AJCC8 stage-group shifts were 2.97%. The v2 R1 dry derivation below uses the ratified rules and reports legacy→resolved shifts without mutating source tables.

### Patient grain (malignant CPM patients)

| legacy_column     | resolved_column            |   rows_total |   legacy_non_null |   resolved_non_null |   paired_non_null |   paired_changes |   paired_change_pct |   resolved_null |
|:------------------|:---------------------------|-------------:|------------------:|--------------------:|------------------:|-----------------:|--------------------:|----------------:|
| ajcc8_t_stage     | t_stage_ajcc8_resolved     |         4137 |              4128 |                3983 |              3981 |              588 |               14.77 |             154 |
| ajcc8_n_stage     | n_stage_ajcc8_resolved     |         4137 |              4077 |                4077 |              4077 |                0 |                0    |              60 |
| ajcc8_m_stage     | m_stage_ajcc8_resolved     |         4137 |              4137 |                4137 |              4137 |                0 |                0    |               0 |
| ajcc8_stage_group | stage_group_ajcc8_resolved |         4137 |              4128 |                3981 |              3977 |              166 |                4.17 |             156 |
| ajcc7_t_stage     | t_stage_ajcc7_resolved     |         4137 |              4127 |                3983 |              3980 |              924 |               23.22 |             154 |
| ajcc7_n_stage     | n_stage_ajcc7_resolved     |         4137 |              4137 |                4077 |              4077 |             2055 |               50.4  |              60 |
| ajcc7_m_stage     | m_stage_ajcc7_resolved     |         4137 |              4137 |                4137 |              4137 |             1798 |               43.46 |               0 |
| ajcc7_stage_group | stage_group_ajcc7_resolved |         4137 |              3882 |                3869 |              3633 |             2137 |               58.82 |             268 |

### Path-event grain

| legacy_column   | resolved_column        |   rows_total |   legacy_non_null |   resolved_non_null |   paired_non_null |   paired_changes |   paired_change_pct |   resolved_null |
|:----------------|:-----------------------|-------------:|------------------:|--------------------:|------------------:|-----------------:|--------------------:|----------------:|
| t_stage_ajcc8   | t_stage_ajcc8_resolved |         6689 |              6443 |                6394 |              6197 |             1434 |               23.14 |             295 |
| n_stage_ajcc8   | n_stage_ajcc8_resolved |         6689 |              6632 |                6632 |              6632 |                0 |                0    |              57 |
| m_stage_ajcc8   | m_stage_ajcc8_resolved |         6689 |              6689 |                6689 |              6689 |                0 |                0    |               0 |
| t_stage_ajcc7   | t_stage_ajcc7_resolved |         6689 |              6443 |                6394 |              6197 |             1622 |               26.17 |             295 |
| n_stage_ajcc7   | n_stage_ajcc7_resolved |         6689 |              6632 |                6632 |              6632 |                0 |                0    |              57 |
| m_stage_ajcc7   | m_stage_ajcc7_resolved |         6689 |              6689 |                6689 |              6689 |                0 |                0    |               0 |

### T-resolution source distribution

| t_resolution_source                     |   n_events |
|:----------------------------------------|-----------:|
| size_greatest_dimension_cm              |       4735 |
| gross_ete_to_T3b_strap_assumption       |       1183 |
| tumor_size_cm_per_surgery               |        403 |
| niftp_excluded                          |        213 |
| size_residual_logan_pending             |         82 |
| microcarcinoma_without_size_default_T1a |         32 |
| anaplastic_default_T4                   |         26 |
| canonical_invasion_events_v1:T4A        |         11 |
| canonical_invasion_events_v1:T4B        |          4 |

### PM N-resolution source distribution

| n_resolution_source   |   n_patients |
|:----------------------|-------------:|
| pm_existing_n_stage   |        10871 |

### Stage component distribution

| component_for_stage                         |   n_patients |
|:--------------------------------------------|-------------:|
| UNKNOWN_OR_NO_PATH_EVENT                    |         6734 |
| PTC                                         |         3334 |
| FTC                                         |          415 |
| MTC                                         |          167 |
| NIFTP                                       |          117 |
| FTUMP                                       |           32 |
| POORLY DIFFERENTIATED THYROID CARCINOMA     |           29 |
| ATC                                         |           26 |
| DIFFERENTIATED HIGH GRADE THYROID CARCINOMA |            9 |
| DTC (NOS)                                   |            6 |
| NEUROENDOCRINE TUMOR                        |            1 |
| ANGIOSARCOMA                                |            1 |

## 3. Adjudication CSV inventory

| CSV | rows | status | purpose |
|---|---:|---|---|
| r1a_ete_t_stage_upgrade_review.csv | 258 | pre-existing, preserved | ETE/T-stage upgrade review resolved by Rules #1/#2/#6. |
| r1c_size_unavailable_residual_121events.csv | 121 | pre-existing, preserved | Size-unavailable residuals under Rule #7. |
| r1b_n1_unspecified_pm_grain.csv | 0 | generated v2 | PM-grain N1 split candidates with central/lateral evidence. |
| r1d_t4_invasion_evidence_review.csv | 374 | generated v2 | T4a/T4b invasion candidates from canonical invasion events. |
| r1e_mixed_histology_stage_group.csv | 168 | generated v2 | Mixed-component histology cases for aggressive-component stage grouping. |

### r1c residual disposition breakdown

| suggested_disposition   |   n_rows |
|:------------------------|---------:|
| review_size_unavailable |       85 |
| T1a_default_PTMC        |       32 |
| review_anaplastic       |        3 |
| EXCLUDE_not_malignant   |        1 |

## 4. Remaining row-level decisions

- **Size residuals:** the preserved r1c CSV has 121 rows. Per Logan Rule #7, PTMC rows default to T1a, NIFTP is excluded, anaplastic defaults T4, and the residual hand-curation subset remains `size_residual_logan_pending` in the SQL logic.
- **N1 PM split candidates:** 0 patients have N1 plus central/lateral evidence and need final review before Path-C apply.
- **T4 invasion candidates:** 374 rows have T4a/T4b candidate evidence from `canonical_invasion_events_v1`.
- **Mixed histology:** 168 patients have multi-component histology and are staged by the aggressive-component rule (MTC > PTC > FTC; ATC highest if present).
- **Registry closure target:** live read-only probe found 36 registry rows carrying CF-87/AJCC notes or batch IDs. The skeleton SQL updates matching rows and appends the v2 closure note.

## 5. Unblocking checklist

1. Logan reviews the five CSVs, especially r1b/r1d/r1e plus the r1c residual hand-curation subset.
2. Cowork applies `qc_framework_v1/migrations/184_v2_r1_ajcc_derivation_ratified_20260430.sql` via Path C if review passes.
3. Post-apply, rerun §J probes, CPM invariants, and registry checks.
4. Manuscript SQL should prefer the new `*_resolved` columns while preserving legacy stored columns unchanged.

## Governance boundary

This run did not execute `ALTER`, `UPDATE`, `CREATE`, `DROP`, registry mutation, or provenance insert against MotherDuck. All database interactions were SELECT-only via `connect_locked()`. The SQL file is an apply skeleton for Cowork Path-C.
