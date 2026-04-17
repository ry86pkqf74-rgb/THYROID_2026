# 266b ETE pre-apply check (read-only)

## Decision

### Any-tumor metric (Logan's original threshold)

- CPM-says-absent patients with per-tumor data: **16**
- ... of which ANY tumor in `unclassified_present`: **4 (25.00%)**
- Threshold: **5%** -> **HALT**

### Dominant-tumor-only metric (post-Tier-4)

- CPM-says-absent patients with dominant tumor in per-tumor frame: **16**
- ... dominant tumor classified via CPM overlay -> absent: **14**
- ... dominant tumor still in `unclassified_present`: **0 (0.00%)**
- Threshold: **5%** -> **PROCEED**

## CPM ete_grade_final_v2 distribution

| value | n |
|---|---:|
| `__NULL__` | 6796 |
| `microscopic` | 3643 |
| `gross` | 190 |
| `false` | 187 |
| `present_ungraded` | 32 |
| `absent` | 16 |
| `true` | 7 |

## Per-tumor ete_source distribution

| source | n_tumors |
|---|---:|
| `uncalculable:no_stl_no_adjudication` | 6309 |
| `cpm_patient_level:broadcast_to_dominant:microscopic` | 3335 |
| `stl_per_tumor:unclassified_present` | 930 |
| `stl_per_tumor:microscopic` | 296 |
| `cpm_patient_level:broadcast_to_dominant:gross` | 186 |
| `ete_adjudication_v1:unable_to_determine` | 16 |
| `cpm_patient_level:broadcast_to_dominant:absent` | 14 |
| `ete_adjudication_v1:absent` | 13 |
| `ete_adjudication_v1:gross` | 3 |
| `ete_adjudication_v1:microscopic` | 1 |

## Crosstab

CPM `ete_grade_final_v2` (rows) x patient-level agg (cols).

```
patient_agg         all_uncalculable  any_absent_sig  any_gross  any_micro  any_unable_only  any_unclassified_present  no_tumor_data    ALL
ete_grade_final_v2                                                                                                                         
__NULL__                        4347               0          0          0                0                         0           2449   6796
absent                             0              12          0          0                0                         4              0     16
false                            183               0          0          0                0                         4              0    187
gross                              0               0        186          0                0                         4              0    190
microscopic                        0               0          0       3598                0                        45              0   3643
present_ungraded                   0               0          0          0                0                        32              0     32
true                               3               0          0          0                2                         2              0      7
ALL                             4533              12        186       3598                2                        91           2449  10871
```

## Top 10 `unclassified_present` text buckets (by n_tumors)

| stl_ete_text | n_tumors |
|---|---:|
| `x` | 4218 |
| `present` | 306 |
| `c/a` | 43 |
| `indeterminate` | 22 |
| `yes` | 20 |
| `Yes;` | 13 |
| `extesive` | 12 |
| `extensive` | 9 |
| `X` | 2 |
| `nan` | 2 |

## Sample 10 CPM-says-absent rids with `any_unclassified_present`

| research_id | ete_grade_final_v2 | n_tumors | any_gross | any_micro | any_unclassified | any_absent_sig |
|---|---|---:|---|---|---|---|
| 9424 | absent | 3.0 | False | False | True | True |
| 3601 | absent | 2.0 | False | False | True | True |
| 4085 | absent | 3.0 | False | False | True | True |
| 5315 | absent | 4.0 | False | False | True | True |

