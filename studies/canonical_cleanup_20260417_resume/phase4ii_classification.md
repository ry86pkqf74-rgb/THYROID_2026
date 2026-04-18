# Phase 4 (ii) — 80-rid invariant violation classification

_Generated 2026-04-18T04:46:42.679914+00:00_  
_Strictly read-only on `canonical_patient_master`. No size values modified._  

## Bucket counts

| bucket | description | broken column | n |
|:---|:---|:---|---:|
| **A** | Unit / decimal error (10× or 0.1× ratio) | path_tumor_size_cm | 0 |
| **B** | Wrong source (matches anatomic / non-tumor value) | path_tumor_size_cm | 0 |
| **C** | NLP / free-text contamination (no feeder match) | path_tumor_size_cm | 0 |
| **D** | Multi-focus enumeration drift (small Δ, path matches a focus) | neither (semantics) | 13 |
| **E** | Unresolvable from structured data | unknown | 7 |
| **F** | tumor_size_cm_max under-reports (incomplete feeder set, multi-surgery) | tumor_size_cm_max | 60 |
| **TOTAL** | | | 80 |

## Cross-tab: bucket × delta band

| bucket | extreme(>5) | moderate(1<Δ≤5) | small(≤1) |
|:---|---:|---:|---:|
| A | 0 | 0 | 0 |
| B | 0 | 0 | 0 |
| C | 0 | 0 | 0 |
| D | 0 | 0 | 13 |
| E | 0 | 7 | 0 |
| F | 13 | 32 | 15 |

## Per-bucket samples (up to 10 each)

### Bucket D (n=13, recommend → multifocal_notes)

| rid | path | max | Δ | observed_max_tumor_focus | evidence (truncated) | proposed_corrected_value |
|---:|---:|---:|---:|---:|:---|---:|
| 544 | 2.2 | 1.3 | 0.9 | 6.2 | small Δ=0.90cm; path=2.2 matches focus(es): ['tumor_episode_master_v2(surg=3,ord=1)=2.2', 'tumor_pathology(tumor_1_size_ |  |
| 11378 | 0.8 | 0.3 | 0.5 | 7.2 | small Δ=0.50cm; path=0.8 matches focus(es): ['tumor_episode_master_v2(surg=1,ord=1)=0.8', 'tumor_pathology(tumor_1_size_ |  |
| 558 | 2.0 | 1.5 | 0.5 | 2.5 | small Δ=0.50cm; path=2.0 matches focus(es): ['tumor_episode_master_v2(surg=1,ord=1)=2.0', 'canonical_tumor_characteristi |  |
| 4903 | 2.0 | 1.5 | 0.5 | 3.4 | small Δ=0.50cm; path=2.0 matches focus(es): ['tumor_episode_master_v2(surg=2,ord=1)=2.0', 'tumor_pathology(tumor_1_size_ |  |
| 10157 | 1.9 | 1.6 | 0.3 | 2.4 | small Δ=0.30cm; path=1.9 matches focus(es): ['tumor_episode_master_v2(surg=2,ord=1)=1.9', 'tumor_pathology(tumor_1_size_ |  |
| 9199 | 1.2 | 0.9 | 0.3 | 4.2 | small Δ=0.30cm; path=1.2 matches focus(es): ['tumor_episode_master_v2(surg=1,ord=1)=1.2', 'canonical_tumor_characteristi |  |
| 7944 | 2.6 | 2.3 | 0.3 | 4.0 | small Δ=0.30cm; path=2.6 matches focus(es): ['tumor_episode_master_v2(surg=2,ord=1)=2.6', 'tumor_pathology(tumor_1_size_ |  |
| 7110 | 1.7 | 1.5 | 0.2 | 2.0 | small Δ=0.20cm; path=1.7 matches focus(es): ['tumor_episode_master_v2(surg=2,ord=1)=1.7', 'tumor_pathology(tumor_1_size_ |  |
| 10205 | 4.2 | 4.0 | 0.2 | 4.5 | small Δ=0.20cm; path=4.2 matches focus(es): ['tumor_episode_master_v2(surg=2,ord=1)=4.2', 'tumor_pathology(tumor_1_size_ |  |
| 4852 | 2.3 | 2.2 | 0.1 | 2.3 | small Δ=0.10cm; path=2.3 matches focus(es): ['synoptic_tumor_long_v1(tumor_index=1)=2.2', 'tumor_episode_master_v2(surg= |  |

### Bucket E (n=7, recommend → chart_review_queue)

| rid | path | max | Δ | observed_max_tumor_focus | evidence (truncated) | proposed_corrected_value |
|---:|---:|---:|---:|---:|:---|---:|
| 5772 | 5.5 | 1.6 | 3.9 | 7.9 | no clean classifier match: path=5.5, max=1.6, delta=3.90, observed_max_tumor_focus=7.9, n_tumor_focus_values=9, n_anatom |  |
| 11219 | 4.5 | 1.5 | 3.0 | 5.3 | no clean classifier match: path=4.5, max=1.5, delta=3.00, observed_max_tumor_focus=5.3, n_tumor_focus_values=15, n_anato |  |
| 4336 | 3.0 | 0.3 | 2.7 | 4.0 | no clean classifier match: path=3.0, max=0.3, delta=2.70, observed_max_tumor_focus=4.0, n_tumor_focus_values=9, n_anatom |  |
| 8482 | 3.8 | 1.1 | 2.7 | 3.9 | no clean classifier match: path=3.8, max=1.1, delta=2.70, observed_max_tumor_focus=3.9, n_tumor_focus_values=25, n_anato |  |
| 9102 | 3.0 | 0.7 | 2.3 | 5.7 | no clean classifier match: path=3.0, max=0.7, delta=2.30, observed_max_tumor_focus=5.7, n_tumor_focus_values=9, n_anatom |  |
| 5107 | 4.0 | 1.9 | 2.1 | 6.5 | no clean classifier match: path=4.0, max=1.9, delta=2.10, observed_max_tumor_focus=6.5, n_tumor_focus_values=19, n_anato |  |
| 7337 | 3.9 | 2.8 | 1.1 | 4.0 | no clean classifier match: path=3.9, max=2.8, delta=1.10, observed_max_tumor_focus=4.0, n_tumor_focus_values=14, n_anato |  |

### Bucket F (n=60, recommend → CORRECTION queue (tumor_size_cm_max — re-aggregate))

| rid | path | max | Δ | observed_max_tumor_focus | evidence (truncated) | proposed_corrected_value |
|---:|---:|---:|---:|---:|:---|---:|
| 2378 | 12.7 | 1.6 | 11.1 | 12.7 | path=12.7 matches the HIGHEST tumor focus across feeders (12.7); tumor_size_cm_max=1.6 corresponds to a smaller focus se | 12.7 |
| 4728 | 12.7 | 2.5 | 10.2 | 12.7 | path=12.7 matches the HIGHEST tumor focus across feeders (12.7); tumor_size_cm_max=2.5 corresponds to a smaller focus se | 12.7 |
| 593 | 17.0 | 7.0 | 10.0 | 17.0 | path=17.0 matches the HIGHEST tumor focus across feeders (17.0); tumor_size_cm_max=7.0 corresponds to a smaller focus se | 17.0 |
| 2201 | 11.0 | 1.2 | 9.8 | 11.0 | path=11.0 matches the HIGHEST tumor focus across feeders (11.0); tumor_size_cm_max=1.2 corresponds to a smaller focus se | 11.0 |
| 2194 | 10.2 | 0.9 | 9.3 | 10.2 | path=10.2 matches the HIGHEST tumor focus across feeders (10.2); tumor_size_cm_max=0.9 corresponds to a smaller focus se | 10.2 |
| 10994 | 10.2 | 1.5 | 8.7 | 10.2 | path=10.2 matches the HIGHEST tumor focus across feeders (10.2); tumor_size_cm_max=1.5 corresponds to a smaller focus se | 10.2 |
| 5726 | 7.6 | 0.03 | 7.57 | 7.6 | path=7.6 matches the HIGHEST tumor focus across feeders (7.6); tumor_size_cm_max=0.03 corresponds to a smaller focus see | 7.6 |
| 6922 | 9.0 | 2.0 | 7.0 | 9.0 | path=9.0 matches the HIGHEST tumor focus across feeders (9.0); tumor_size_cm_max=2.0 corresponds to a smaller focus seen | 9.0 |
| 10363 | 10.0 | 3.0 | 7.0 | 10.0 | path=10.0 matches the HIGHEST tumor focus across feeders (10.0); tumor_size_cm_max=3.0 corresponds to a smaller focus se | 10.0 |
| 2423 | 11.0 | 4.2 | 6.8 | 11.0 | path=11.0 matches the HIGHEST tumor focus across feeders (11.0); tumor_size_cm_max=4.2 corresponds to a smaller focus se | 11.0 |

_Full per-rid trace_: [`phase4ii_classification.csv`](./phase4ii_classification.csv) (JSON variant alongside)
