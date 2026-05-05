# Cursor Prompt: Implement 2025 ATA Risk Stratification System (RSS)

**Agent:** GPT 5.5 (Composer 2.0) — complex rule engine with extensive clinical logic; GPT 5.5's strong structured reasoning is ideal for multi-branch decision trees  
**Estimated time:** 3–4 hours  
**Date:** 2026-05-04

## Context

We have a MotherDuck database `thyroid_canonical_publication_v1_0` with 10,871 thyroid surgery patients (4,019 malignant). The existing ATA risk classification (`ata_risk_category`) was computed using 2015 ATA guidelines. Manuscript M036 compares 2015 vs 2025 ATA risk stratification across the 4,019-patient malignant cohort (`manuscript_workspace.cohort_m036_ata_risk_comparison_v1`).

### Current ATA Risk Distribution (2015 rules, N=4,019 malignant)

| Category | N | % | Recurred |
|---|---|---|---|
| High | 1,831 | 45.6% | 225 (12.3%) |
| Intermediate | 1,143 | 28.4% | 96 (8.4%) |
| Low | 55 | 1.4% | 3 (5.5%) |
| NULL (uncalculable) | 990 | 24.6% | 178 (18.0%) |

### Key Observation
990 patients (24.6%) have NULL ATA risk — the 2025 implementation should attempt to classify as many as possible. The NULL group has the highest recurrence rate (18.0%), suggesting many are high-risk cases missing classification inputs.

## Available Columns in `canonical_patient_master`

### ATA-Related
- `ata_risk_category` — current 2015 classification (high/intermediate/low/NULL)
- `ata_initial_risk` — mirrors ata_risk_category
- `ata_calculable_flag` — boolean
- `ata_risk_calculable_flag` — boolean
- `ata_response_category` — dynamic risk (response to therapy)
- `ata_response_calculable_flag` — boolean
- `scoring_ata_flag` — boolean

### Tumor Characteristics (inputs to RSS)
- `histology_final` — PTC, follicular, MTC, NIFTP, etc.
- `tumor_size_cm_dominant` — continuous
- `multifocal_flag_path` — boolean
- `ete_grade_final` — none/microscopic/gross/minimal
- `gross_ete_flag` — boolean
- `vascular_invasion_final` — none/microscopic/extensive/present
- `vascular_invasion_grade` — WHO grading
- `vascular_vessel_count` — count of invaded vessels
- `margin_status_final` — positive/negative/close
- `margin_r_class` — R0/R1/R2

### Lymph Node Data
- `ln_positive_final` — count of positive nodes
- `ln_rollup_total_examined` — total examined
- `ln_rollup_total_positive` — total positive
- `ln_rollup_has_per_level_data` — boolean

### Molecular Markers
- `braf_positive_final` — boolean
- `ras_positive_final` — boolean
- `molecular_risk_tier` — high/intermediate/low_intermediate/wild_type
- `high_risk_molecular_v7` — boolean
- `mol_has_fusion` — boolean (RET/PTC, PAX8/PPARG, etc.)

### Staging
- `ajcc8_stage_group` — I/II/III/IVA/IVB
- `ajcc8_t_stage` — T1a/T1b/T2/T3a/T3b/T4a/T4b
- `ajcc8_n_stage` — N0/N1a/N1b

### RAI & Outcomes
- `rai_received_reconciled` — boolean (862 TRUE, preferred over rai_received_flag)
- `any_recurrence_flag` — boolean (514 TRUE overall)
- `recurrence_data_confidence` — quality tier

## Task

### 1. Implement 2025 ATA RSS Algorithm

Create a Python script that:

1. Connects to MotherDuck: `duckdb.connect('md:thyroid_canonical_publication_v1_0')`
2. Pulls all malignant patients (N=4,019) with relevant columns
3. Implements the 2025 ATA RSS rules as a deterministic decision tree

**2025 ATA RSS Categories (implement these rules):**

The 2025 ATA guidelines restructure risk into a points-based or tier-based system. The key changes from 2015:

#### Low Risk (2025 criteria):
- Papillary microcarcinoma (≤1 cm), unifocal, no ETE, no vascular invasion, no LN metastasis
- Intrathyroidal PTC ≤4 cm, no aggressive histology, no vascular invasion, N0
- Minimally invasive follicular thyroid cancer (FTC), no vascular invasion
- NIFTP

#### Intermediate Risk (2025 criteria):
- Microscopic ETE
- Minor vascular invasion (≤3 vessels for FTC, any for PTC)
- Clinical N1 with ≤5 pathologic LN, all <3 cm
- Multifocal papillary microcarcinoma with ETE
- BRAF V600E mutated PTC with no other high-risk features
- Aggressive histology variants without other high-risk features

#### High Risk (2025 criteria):
- Gross ETE (T4a/T4b)
- Incomplete tumor resection (R1/R2)
- Distant metastasis
- Extensive vascular invasion (>4 vessels)
- LN >3 cm
- ≥5 positive LN
- High-risk molecular: TERT promoter, TP53, BRAF + TERT co-mutation
- Widely invasive FTC
- Poorly differentiated thyroid cancer

#### Key 2025 vs 2015 Differences to Capture:
- 2025 downgrades BRAF V600E alone from intermediate to "low-intermediate" (contextual)
- 2025 uses vessel count thresholds for vascular invasion grading
- 2025 explicitly incorporates molecular risk tiers
- 2025 adds NIFTP as explicitly low-risk (was ambiguous in 2015)
- 2025 refines LN criteria: count AND size matter

### 2. Create Comparison Table

Generate a cross-tabulation:

| | 2025 Low | 2025 Intermediate | 2025 High | 2025 Uncalculable |
|---|---|---|---|---|
| 2015 Low | | | | |
| 2015 Intermediate | | | | |
| 2015 High | | | | |
| 2015 NULL | | | | |

### 3. Validate Against Outcomes

For each 2015 vs 2025 category:
- Recurrence rate with 95% CI
- Kaplan-Meier survival curves (if time-to-event data available via `fu_months_from_surg`)
- Net reclassification improvement (NRI)
- C-statistic comparison

### 4. Output

Save to `studies/m036_ata_rss_comparison/`:
- `ata_2025_rss_classification.csv` — patient-level 2015 vs 2025 categories
- `reclassification_crosstab.csv` — the comparison table
- `outcome_validation.csv` — recurrence rates by category
- `ata_2025_rules_audit.csv` — which rule triggered each classification + missing inputs
- LaTeX-formatted tables for manuscript

### 5. Upload to MotherDuck

Create `manuscript_workspace.m036_ata_2025_rss_v1` with columns:
- `research_id`, `ata_2015_category`, `ata_2025_category`, `ata_2025_rule_triggered`, `ata_2025_missing_inputs`, `reclassified_flag`, `reclassification_direction`

## Connection

```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```

Use the MotherDuck token from `.env.motherduck` or environment variable `MOTHERDUCK_TOKEN`.

## Important Notes

- `research_id` is VARCHAR in canonical_patient_master
- Boolean columns: use `IS TRUE` / `IS NOT TRUE`, never compare with strings
- Sex values are lowercase: `female`, `male`
- When a classification rule requires data that is NULL, mark as "uncalculable" with the specific missing field(s) logged
- The goal is to REDUCE the 24.6% uncalculable rate from the 2015 system
- Cross-reference the published 2025 ATA guidelines (search for "2025 ATA Management Guidelines for Differentiated Thyroid Cancer") for exact rule definitions
