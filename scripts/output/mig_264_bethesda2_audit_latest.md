# mig_264 Bethesda-2 false-negative audit (read-only)
Generated UTC: 2026-05-01T22:06:47Z
Database: thyroid_canonical_publication_v1_0 (connect_locked)

### §2.0 Cohort verification

|   n_bethesda2_malig |   n_bethesda2_all |   n_bethesda2_malig_repeat |
|--------------------:|------------------:|---------------------------:|
|                 385 |              2033 |                        385 |

### §2a FNA episode counts per patient

|   single_fna |   two_fna |   three_plus_fna |   zero_fna_events |
|-------------:|----------:|-----------------:|------------------:|
|          219 |       118 |               48 |                 0 |

### §2b bethesda_index_nodule_linkage_source

| src     |   n |
|:--------|----:|
| surgery | 385 |

### §2c path_synoptics tumor_2 size present (multifocal proxy)

|   multi_tumor_path_synoptics |   single_slot_missing_t2 |   no_path_synoptics_row |
|-----------------------------:|-------------------------:|------------------------:|
|                           92 |                      293 |                       0 |

### §2d histology_final (top 30)

| histology_final                             |   n |
|:--------------------------------------------|----:|
| PTC                                         | 286 |
| follicular carcinoma                        |  53 |
| NIFTP                                       |  22 |
| MTC                                         |   6 |
| FTUMP                                       |   6 |
| metastatic PTC                              |   4 |
| follicular adenoma                          |   2 |
| poorly differentiated thyroid carcinoma     |   2 |
| metastatic thyroid carcinoma                |   1 |
| anaplastic carcinoma                        |   1 |
| metastatic follicular carcinoma             |   1 |
| differentiated high grade thyroid carcinoma |   1 |

### §2e FNA-to-first-surgery interval

|   n_with_both_dates |   median_days |   within_30d |   one_to_12mo |   over_1yr |   negative_days_fna_after_surgery |
|--------------------:|--------------:|-------------:|--------------:|-----------:|----------------------------------:|
|                 385 |           145 |           60 |           205 |        120 |                                19 |

### §2f Any FNA episode with Bethesda > 2 (patient-level)

|   n_cohort |   n_with_some_fna_gt2 |   n_no_fna_bethesda_num |
|-----------:|----------------------:|------------------------:|
|        385 |                    59 |                       0 |

### Disposition bucket summary (heuristic — Logan adjudicates)

| suggested_disposition_bucket        |   n_patients |
|:------------------------------------|-------------:|
| heuristic_default_review            |          212 |
| heuristic_long_interval             |          114 |
| heuristic_stale_bethesda2_vs_events |           59 |

Per-patient detail: ``scripts/output/mig_264_disposition_table.csv`` (rows=385).
