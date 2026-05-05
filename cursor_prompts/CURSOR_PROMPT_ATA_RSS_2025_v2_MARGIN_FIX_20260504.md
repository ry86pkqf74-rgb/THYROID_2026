# Cursor Prompt: 2025 ATA RSS — v2 CORRECTED (Margin Logic Fix)

**Agent:** GPT 5.5 (Composer 2.0)  
**Estimated time:** 3–4 hours  
**Date:** 2026-05-04  
**Supersedes:** `CURSOR_PROMPT_ATA_RSS_2025_IMPLEMENTATION_20260504.md` (v1 had critical margin bug)

## CRITICAL FIX FROM v1

The v1 run classified **2,440/4,019** (60.7%) patients as `high:incomplete_resection_r1_r2`. This happened because `margin_r_classification = 'R1'` applies to **3,802 of 4,019** malignant patients (94.6%). In thyroid surgery, R1 = microscopic positive margins at the thin thyroid capsule, which is extremely common and expected. **R1 does NOT mean "incomplete resection" in the ATA sense.**

### Corrected Rule:
- **R2 only** → triggers `high:incomplete_resection` (gross residual tumor left behind)
- **R1** → does NOT trigger high risk on its own. R1 may contribute as a modifier (e.g., R1 + gross ETE → high), but alone it is NOT an ATA high-risk feature
- **Rx** → treat as unknown margin status, NOT as incomplete resection
- Only 25 patients have R2. Only 1 patient has R0. The remaining 3,802 R1 and 35 Rx should be classified based on OTHER features (tumor size, ETE, LN, molecular, etc.)

### v1 Results (WRONG — for reference only):
| Category | N | Recurrence % |
|---|---|---|
| 2025-high | 3,721 | 12.5% |
| 2025-uncalculable | 236 | 13.1% |
| 2025-intermediate | 55 | 5.5% |
| 2025-low | 7 | 28.6% |

### Expected v2 Distribution (approximate):
After fixing the margin logic, expect a much more balanced distribution — roughly:
- Low: ~200–500 patients (small intrathyroidal PTC, NIFTP, minimally invasive FTC)
- Intermediate: ~800–1,500 patients (microscopic ETE, limited LN, BRAF alone)
- High: ~1,500–2,500 patients (gross ETE, extensive LN, R2, extensive vascular invasion)
- Uncalculable: ~200–500 patients (non-DTC, missing data)

## Context

MotherDuck database `thyroid_canonical_publication_v1_0`, 4,019 malignant patients in `manuscript_workspace.cohort_m036_ata_risk_comparison_v1`.

### Current ATA 2015 Distribution:
| Category | N | Recurred |
|---|---|---|
| High | 1,831 | 225 (12.3%) |
| Intermediate | 1,143 | 96 (8.4%) |
| Low | 55 | 3 (5.5%) |
| NULL | 990 | 178 (18.0%) |

### Margin Data:
| margin_r_classification | N |
|---|---|
| R1 | 3,802 |
| NULL | 156 |
| Rx | 35 |
| R2 | 25 |
| R0 | 1 |

## Available Columns

(Same as v1 — see `CURSOR_PROMPT_ATA_RSS_2025_IMPLEMENTATION_20260504.md` for full list)

Key columns for classification:
- `histology_final` — PTC, follicular carcinoma, MTC, NIFTP, etc.
- `tumor_size_cm_dominant` — continuous
- `multifocal_flag_path` — boolean
- `ete_grade_final` — None/microscopic/gross/present_ungraded/false/true/absent
- `gross_ete_flag` — boolean
- `vascular_invasion_final` — none/microscopic/extensive/present_ungraded
- `vascular_vessel_count` — integer
- `margin_r_classification` — R0/R1/R2/Rx/NULL
- `ln_positive_final` — count
- `ln_rollup_total_positive` — count
- `braf_positive_final` — boolean
- `ras_positive_final` — boolean
- `molecular_risk_tier` — high/intermediate/low_intermediate/wild_type
- `mol_has_fusion` — boolean
- `ajcc8_t_stage` — T1a/T1b/T2/T3a/T3b/T4a/T4b
- `ajcc8_n_stage` — N0/N1a/N1b
- `ajcc8_m_stage` — M0/M1

## 2025 ATA RSS Decision Tree (CORRECTED)

Implement in this exact priority order. First matching rule wins.

### Step 0: Exclude non-DTC
- If histology is MTC, anaplastic, poorly differentiated, NUT carcinoma, angiosarcoma, or any "metastatic" prefix → `uncalculable:non_dtc_histology`
- DTC includes: PTC, follicular carcinoma, NIFTP, FTUMP, Hürthle cell carcinoma

### Step 1: HIGH RISK (any one criterion)
1. `ajcc8_m_stage = 'M1'` → `high:distant_metastasis`
2. `margin_r_classification = 'R2'` → `high:incomplete_resection_r2` (NOTE: R1 does NOT qualify)
3. `gross_ete_flag = TRUE` OR `ete_grade_final = 'gross'` OR `ajcc8_t_stage IN ('T4a','T4b')` → `high:gross_ete_or_t4`
4. `vascular_invasion_final = 'extensive'` OR `vascular_vessel_count > 4` → `high:extensive_vascular_invasion`
5. `ln_rollup_total_positive >= 5` → `high:five_or_more_positive_ln`
6. (Cannot assess: "LN deposit >3 cm" — `ln_rollup_largest_deposit_cm` if available)
7. `molecular_risk_tier = 'high'` → `high:high_risk_molecular` (includes TERT, TP53, BRAF+TERT)

### Step 2: INTERMEDIATE RISK (any one criterion, if not already high)
1. `ete_grade_final = 'microscopic'` → `intermediate:microscopic_ete`
2. `vascular_invasion_final IN ('microscopic', 'focal', 'present_ungraded')` AND (`vascular_vessel_count <= 4` OR `vascular_vessel_count IS NULL`) → `intermediate:minor_vascular_invasion`
3. `ln_rollup_total_positive BETWEEN 1 AND 4` AND (no LN >3cm) → `intermediate:limited_nodal_metastases`
4. `braf_positive_final = TRUE` AND no other high/intermediate features → `intermediate:braf_v600e_alone`
5. `multifocal_flag_path = TRUE` AND `ete_grade_final = 'microscopic'` AND `tumor_size_cm_dominant <= 1.0` → `intermediate:multifocal_microptc_with_ete`

### Step 3: LOW RISK (all criteria must be met)
1. `histology_final = 'NIFTP'` → `low:niftp`
2. `histology_final = 'PTC'` AND `tumor_size_cm_dominant <= 1.0` AND `multifocal_flag_path IS NOT TRUE` AND ete=none AND vascular_invasion=none AND `ln_rollup_total_positive = 0` → `low:unifocal_papillary_microcarcinoma`
3. `histology_final = 'PTC'` AND `tumor_size_cm_dominant <= 4.0` AND ete=none AND vascular_invasion=none AND `ajcc8_n_stage = 'N0'` → `low:intrathyroidal_ptc_le4cm_n0`
4. `histology_final LIKE '%follicular%'` AND minimally invasive AND vascular_invasion=none → `low:minimally_invasive_ftc_no_vi`

### Step 4: UNCALCULABLE
- If none of the above matched and critical data is missing (e.g., no tumor size AND no ETE data AND no LN data) → `uncalculable:insufficient_data` with list of missing fields

### ETE value mapping (apply before classification):
- `ete_grade_final IN (NULL, 'None', 'false', 'absent')` → no ETE
- `ete_grade_final = 'microscopic'` → microscopic ETE
- `ete_grade_final = 'gross'` → gross ETE
- `ete_grade_final IN ('true', 'present_ungraded')` → ETE present, grade unknown → treat as microscopic for conservative classification

## Task

### 1. Delete the v1 table
```sql
DROP TABLE IF EXISTS manuscript_workspace.m036_ata_2025_rss_v1;
```

### 2. Run corrected classification
Same output structure as v1 but with corrected rules.

### 3. Validate the fix
- Confirm R1 patients are no longer auto-classified as high
- Confirm the distribution is more balanced
- Cross-check: 2025-low should have <10% recurrence, 2025-high should have >10%

### 4. Generate all outputs
Same as v1: reclassification crosstab, outcome validation with 95% CI, KM curves, NRI, C-statistic, LaTeX tables.

### 5. Upload corrected table
Create `manuscript_workspace.m036_ata_2025_rss_v2` with same schema as v1.

### 6. Save to same directory
Overwrite files in `studies/m036_ata_rss_comparison/` with corrected outputs.

## Connection
```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```
Use MotherDuck token from `.env.motherduck` or `MOTHERDUCK_TOKEN`.

## Important Notes
- `research_id` is VARCHAR
- Boolean columns: use `IS TRUE` / `IS NOT TRUE`
- Sex: lowercase `female`/`male`
- The 990 patients with NULL ATA-2015 include many MTC and metastatic cases — the non-DTC filter should handle most of these
- R1 (3,802 patients) is the normal state for thyroid cancer margins — DO NOT treat as incomplete resection
