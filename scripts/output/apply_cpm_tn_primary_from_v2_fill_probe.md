# Script 397 — Phase 0 probe (CPM T/N primary from v2 fill)

## Cohort (malignant allowlist only)

- **T NULL + v2 populated:** 26
- **N NULL + v2 populated:** 213
- **Distinct research_ids (union):** 236

- **CPM total (H5):** 10871
- **H6:** PASS — planned UPDATEs only on `ajcc8_t_stage` and `ajcc8_n_stage` (not `ajcc8_stage_group`).
- **H7 snapshot tables with `cpm_pre_tn_primary_from_v2_fill_*`:** 0
- **H3 M rescue (NULL primary + v2) among allowlist:** 0
- **H2 tautology (allowlist ∧ rescue ∧ NOT allowlist):** 0
- **Rows with (t or n) rescue and diagnosis outside allowlist:** 0

## H8 — Cross-source disagreements on rows matching each UPDATE (must be 0)

Per axis, the UPDATE predicate requires primary NULL for that column, so (both set ∧ differ) cannot hold. **T-predicate rows:** 0; **N-predicate rows:** 0.

## Halt gates (H1–H8 summary)

| all_pass | True |

## Per diagnosis (malignant rescue rows)

| diagnosis_primary | rows |
|---|---:|
| ATC | 4 |
| DTC_NOS | 1 |
| FTC | 44 |
| MTC | 9 |
| PTC | 173 |
| other_malignant | 5 |

## T-NULL with NULL `ajcc8_stage_group` (CF-397-1 follow-up candidates)

| research_id | diagnosis | ajcc8_stage_group |
|---|---|---|
| 106 | MTC | NULL |
| 111 | DTC_NOS | NULL |
| 4015 | MTC | NULL |
| 6768 | other_malignant | NULL |
| 9600 | MTC | NULL |

## Planned UPDATEs

```sql
UPDATE ... SET ajcc8_t_stage = ajcc8_t_stage_v2 WHERE diagnosis_primary IN ('PTC','FTC','HCC','DTC_NOS','MTC','ATC','other_malignant') AND ajcc8_t_stage IS NULL AND ajcc8_t_stage_v2 IS NOT NULL
UPDATE ... SET ajcc8_n_stage = ajcc8_n_stage_v2 WHERE diagnosis_primary IN ('PTC','FTC','HCC','DTC_NOS','MTC','ATC','other_malignant') AND ajcc8_n_stage IS NULL AND ajcc8_n_stage_v2 IS NOT NULL
```

---HASH-BOUNDARY---

## Generation footer (excluded from PROBE_REPORT_SHA256)

Written UTC: 2026-04-23T03:07:01.292548+00:00
