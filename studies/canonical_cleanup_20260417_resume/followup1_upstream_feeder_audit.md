# Follow-up 1 — Upstream feeder audit (80 F-bucket rids)

_Generated 2026-04-18T06:02:48.646502+00:00_  
_Strictly read-only on `canonical_patient_master`. Audits whether the surgery-1-only feeder issue that breaks `tumor_size_cm_max` also affects ETE / LVI / margin / multifocal / n_tumors._  

## Summary by column

| column | AGREES_WORST | UNDER_REPORTS_LIKE_TUMOR_SIZE | AMBIGUOUS | N/A_NO_TEM_DATA |
|:---|---:|---:|---:|---:|
| `ete_grade_final_v2` | 15 | **2** | 11 | 52 |
| `lvi_ordinal_worst` | 34 | **0** | 39 | 7 |
| `margin_involved_any` | 51 | **0** | 27 | 2 |
| `multifocal_flag_path` | 0 | **0** | 0 | 80 |
| `n_tumors_path` | 0 | **0** | 0 | 80 |

**Rids with ≥1 `UNDER_REPORTS_LIKE_TUMOR_SIZE` flag** on a non-tumor-size column: **2 / 80**.

## Interpretation

The same surgery-1-only feeder pattern affects multiple patient-level worst-case columns. The `tumor_size_cm_max` correction queue is therefore the visible tip of a broader pattern. Recommend a multi-column upstream feeder fix before row-by-row sign-off on the existing 80-rid queue.

## Per-rid sample (first 15)

| rid | n_surg | ETE | LVI | margin | multifocal | n_tumors |
|---:|---:|:---|:---|:---|:---|:---|
| 156 | 2 | TEM_NO_ETE_DATA | ? | OK | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 544 | 3 | TEM_NO_ETE_DATA | TEM_NO_LVI_DATA | OK | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 558 | 3 | TEM_NO_ETE_DATA | TEM_NO_LVI_DATA | TEM_NO_MARGIN_DATA | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 593 | 2 | ? | OK | OK | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 625 | 3 | ? | ? | ? | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 762 | 2 | OK | ? | OK | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 1150 | 2 | TEM_NO_ETE_DATA | OK | OK | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 1294 | 5 | TEM_NO_ETE_DATA | TEM_NO_LVI_DATA | ? | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 1401 | 2 | TEM_NO_ETE_DATA | TEM_NO_LVI_DATA | OK | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 1632 | 2 | TEM_NO_ETE_DATA | ? | OK | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 1970 | 2 | TEM_NO_ETE_DATA | TEM_NO_LVI_DATA | OK | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 1978 | 2 | TEM_NO_ETE_DATA | OK | ? | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 2180 | 2 | TEM_NO_ETE_DATA | ? | OK | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 2194 | 2 | TEM_NO_ETE_DATA | ? | ? | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |
| 2201 | 3 | TEM_NO_ETE_DATA | OK | ? | TEM_NO_MF_DATA | TEM_NO_N_TUMORS |

_Full per-rid table_: [`followup1_upstream_feeder_audit.csv`](./followup1_upstream_feeder_audit.csv)
