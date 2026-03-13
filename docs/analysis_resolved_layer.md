# Analysis-Grade Resolved Layer — Documentation

**Version:** v1  
**Created:** 2026-03-13  
**Scripts:** 48–55  
**MotherDuck prefix:** `md_*`

---

## Overview

The analysis-grade resolved layer is a unified, versioned, provenance-aware set of tables
designed for manuscript-ready analysis. It merges all upstream extraction, refinement,
adjudication, linkage, scoring, complication phenotyping, and lab hardening outputs
into three canonical resolved views plus supporting scoring, complication, imaging, and lab tables.

No existing v1/v2 production tables are modified. All new tables are additive.

---

## Table Inventory

### Core Resolved Layer (script 48)

| Table | Grain | Rows (est.) | Status |
|-------|-------|-------------|--------|
| `patient_analysis_resolved_v1` | 1 row per patient | ~12,886 | **Manuscript-grade** |
| `episode_analysis_resolved_v1` | 1 row per surgery episode | ~13,000+ | Manuscript-grade |
| `lesion_analysis_resolved_v1` | 1 row per tumor/lesion | ~13,000+ | Manuscript-grade |

### Thyroid Scoring Systems (script 51)

| Table | Contents | Status |
|-------|----------|--------|
| `thyroid_scoring_systems_v1` | AJCC8, ATA, MACIS, AGES, AMES, LN burden, molecular risk | **Manuscript-grade** |
| `val_scoring_systems` | Scoring calculability and distribution summary | QA |

### Multi-Nodule Imaging (script 50)

| Table | Contents | Status |
|-------|----------|--------|
| `imaging_nodule_master_v1` | Long-format per-nodule per-exam | Analysis |
| `imaging_exam_master_v1` | Per-exam summary | Analysis |
| `imaging_patient_summary_v1` | Per-patient imaging summary | Analysis |

### Enhanced Linkage v3 (script 49)

| Table | Contents | Status |
|-------|----------|--------|
| `imaging_fna_linkage_v3` | Imaging → FNA with numeric score | Analysis |
| `fna_molecular_linkage_v3` | FNA → molecular with numeric score | Analysis |
| `preop_surgery_linkage_v3` | FNA/molecular → surgery with numeric score | Analysis |
| `surgery_pathology_linkage_v3` | Surgery → pathology with numeric score | Analysis |
| `pathology_rai_linkage_v3` | Pathology → RAI with numeric score | Analysis |
| `linkage_summary_v3` | Per-pair statistics | QA |
| `linkage_ambiguity_review_v1` | All multi-candidate linkages | Manual review |

### Complication Phenotyping v2 (script 52)

| Table | Contents | Status |
|-------|----------|--------|
| `complication_phenotype_v1` | Long-format per-patient per-entity | **Manuscript-grade** |
| `complication_patient_summary_v1` | Wide per-patient flags + statuses | **Manuscript-grade** |
| `complication_discrepancy_report_v1` | Raw vs confirmed counts | QA |

### Longitudinal Lab Hardening (script 53)

| Table | Contents | Status |
|-------|----------|--------|
| `longitudinal_lab_clean_v1` | Cleaned lab timeline, all types | **Manuscript-grade** |
| `longitudinal_lab_patient_summary_v1` | Per-patient Tg/TSH/PTH/Ca summaries | **Manuscript-grade** |
| `recurrence_event_clean_v1` | Structural vs biochemical recurrence | **Manuscript-grade** |

### Validation (script 55)

| Table | Contents |
|-------|----------|
| `val_analysis_resolved_v1` | Test results from validation suite (PASS/FAIL/SKIP) |

---

## Column Naming Convention

Every domain in `patient_analysis_resolved_v1` follows this pattern:

| Suffix | Meaning |
|--------|---------|
| `{domain}_{var}_raw` | Extracted raw value (as-is from source) |
| `{domain}_{var}_final` | Adjudicated / resolved best value |
| `{domain}_{var}_source` | Source table that provided the final value |
| `{domain}_{var}_confidence` | Numeric confidence 0–100 |

Analysis eligibility flags:
- `analysis_eligible_flag` — TRUE when histology + surgery_date present
- `molecular_eligible_flag` — TRUE when ≥1 molecular test with valid result
- `rai_eligible_flag` — TRUE when RAI assertion = definite or likely
- `survival_eligible_flag` — TRUE when surgery_date present
- `scoring_ajcc8_flag` — TRUE when AJCC8 is calculable
- `scoring_ata_flag` — TRUE when ATA risk is calculable
- `scoring_macis_flag` — TRUE when MACIS is calculable

---

## Linkage Confidence Computation

v3 linkage tables use a **weighted numeric score** (0.0–1.0):

```
linkage_score = 0.50 × temporal_score
              + 0.30 × laterality_score    (where applicable)
              + 0.20 × size_score          (imaging→FNA/pathology only)
              - ambiguity_penalty
```

### Temporal Score

| Day Gap | Score |
|---------|-------|
| 0 | 1.00 |
| 1–7 | 0.90 − 0.01 × days |
| 8–30 | 0.70 − 0.005 × (days−7) |
| 31–90 | 0.50 − 0.003 × (days−30) |
| 91–365 | 0.30 − 0.001 × (days−90) |
| >365 | 0.00 |

### Laterality Score

| Condition | Score |
|-----------|-------|
| Match | 1.00 |
| One or both NULL | 0.50 |
| Isthmus vs lobe | 0.30 |
| Mismatch | 0.00 |

### Ambiguity Penalty

| Candidate count | Penalty |
|-----------------|---------|
| 1 | 0.00 |
| 2 | 0.10 |
| ≥3 | 0.20 |

### Categorical Tier Mapping (backward compatible with v2)

| Score | Tier |
|-------|------|
| ≥0.85 | `exact_match` |
| ≥0.65 | `high_confidence` |
| ≥0.45 | `plausible` |
| >0.00 | `weak` |
| 0.00 | `unlinked` |

`analysis_eligible_link_flag` is TRUE when `linkage_score >= 0.50`.

---

## Thyroid Scoring System Formulas

### AJCC 8th Edition (DTC)

**T Stage:**
- T1a: ≤1 cm, no gross ETE
- T1b: >1–2 cm, no gross ETE
- T2: >2–4 cm, no gross ETE
- T3a: >4 cm, no ETE
- T3b: Gross ETE to perithyroidal soft tissue
- T4a: Gross ETE to major structures (larynx, trachea, RLN, esophagus)
- T4b: Very extensive ETE (prevertebral fascia, carotid, mediastinal)

**Note:** Microscopic ETE does NOT upstage T1–T2 under AJCC 8th Ed.

**N Stage:**
- N0: No LN metastasis
- N1a: Central compartment (level VI)
- N1b: Lateral/mediastinal compartment (levels II–V, VII)

**Stage Group (age-dependent DTC rules):**
- Age <55: Stage I (any T/N, M0); Stage II (M1)
- Age ≥55: I=T1-2/N0/M0; II=T1-2/N1 or T3; III=T4a or N1b; IVA=T4b; IVB=M1

Reference: AJCC Cancer Staging Manual, 8th Ed., Chapter 73.

### ATA 2015 Initial Recurrence Risk

**Low:** PTC without aggressive variant, ≤5 LN micrometastases (<0.2 cm),
no vascular invasion, no ETE, no distant mets.

**Intermediate:** Aggressive variant, vascular invasion, microscopic ETE,
>5 LN or LN 0.2–3 cm, RAI uptake in neck.

**High:** Gross ETE, incomplete resection, distant mets, Tg suggesting
distant mets, LN >3 cm, FTC with extensive vascular invasion.

Reference: Haugen BR et al., Thyroid 2016;26:1–133.

### MACIS (Mayo Clinic)

```
MACIS = 3.1 × age_factor + 0.3 × tumor_size_cm
       + 1.0 × incomplete_resection + 1.0 × local_invasion
       + 3.0 × distant_mets

age_factor = 0.08 × age (if age < 40)
           = 0.22 × age (if age ≥ 40)
```

Risk groups: <6.0 = low; 6.0–6.99 = intermediate; 7.0–7.99 = high; ≥8.0 = very high

Reference: Hay ID et al., Surgery 1993;114:1050–8.

### AGES (Mayo Clinic)

```
AGES = 0.05 × age + grade_points + 1.0 × ETE + 3.0 × distant_mets
      + 0.2 × tumor_size_cm

grade_points: grade 2 = 1.0; grade 3-4 = 3.0
```

Reference: Hay ID et al., Surgery 1987;102:1088–95.

### AMES (Lahey Clinic)

**High risk** if: older patient (M>40, F>50) AND (distant mets OR major ETE OR tumor >5 cm).
Otherwise **low risk**.

Reference: Cady B, Rossi R. Surgery 1988;104:947–53.

### Molecular Risk Composite

| Tier | Definition |
|------|-----------|
| `high` | BRAF V600E + TERT promoter co-mutation |
| `intermediate_braf` | BRAF V600E alone |
| `intermediate_ras` | RAS mutation alone |
| `low` | All tested negative |
| `unknown` | Not tested or insufficient data |

Reference: Xing M et al., Lancet Oncol 2014;15:1461–8.

---

## Complication Definitions

### Classification Hierarchy

```
note_mention_flag      -- any mention in clinical notes (high FP rate)
  → suspected_flag     -- refined positive context (no H&P boilerplate)
    → confirmed_flag   -- source-hierarchy confirmation (structured > NLP)
      → transient_flag -- documented resolution within 6 months
      → permanent_flag -- documented >6 months or explicit permanence language
```

### Hypocalcemia / Hypoparathyroidism

| Classification | Definition |
|---------------|-----------|
| `biochemical_only` | PTH <15 pg/mL or Ca <8.0 mg/dL within 30d of surgery, no treatment |
| `treatment_requiring` | Calcium/calcitriol supplements within 60d of surgery |
| `confirmed_transient` | Biochemical abnormality + supplement use, resolves within 6 months |
| `confirmed_permanent` | Persistent PTH <15 or supplement use >6 months |

Sources: `extracted_postop_labs_expanded_v1`, `note_entities_medications`,
`extracted_complications_refined_v5`, `complications` table.

### RLN Injury (3-tier)

- **Tier 1 (laryngoscopy-confirmed):** vocal_cord_status ∈ {paresis, paralysis} with laryngoscopy_date > surgery_date
- **Tier 2 (chart-documented):** rln_injury...palsy = 'yes' in structured complications table
- **Tier 3 (NLP-confirmed):** Context-filtered NLP (excludes H&P boilerplate, risk discussions, preservation language)

Note: Same-day H&P `rln_injury` mentions are excluded (consent boilerplate contamination).

### All Other Complications

Source priority: `extracted_complications_refined_v5` (Phase 2 pipeline) which excludes:
- All H&P note type mentions (consent boilerplate)
- "Lack of chyle leak" phrases (Valsalva hemostasis check)
- "SSI" sliding-scale insulin matches (wound_infection)

---

## Longitudinal Lab Date Precedence

Consistent with `scripts/46_provenance_audit.py`:

1. `specimen_collect_dt` from `thyroglobulin_labs` (priority 1.0 — exact lab date)
2. `entity_date` from enriched note entities (priority 0.7 — extracted date)
3. `note_date` from `clinical_notes_long` (priority 0.5 — note encounter date, fallback only)

`specimen_collect_dt` is used for 99.5% of thyroglobulin entries and 97.7% of anti-Tg entries.
TSH, PTH, calcium, and vitamin D have no structured collection date and fall back to entity/note dates.

---

## MotherDuck Verification

Run verification after deploying all new tables:

```bash
# Deploy new tables to MotherDuck
MOTHERDUCK_TOKEN=$(cat .streamlit/secrets.toml | grep MOTHERDUCK_TOKEN | cut -d'"' -f2) \
  .venv/bin/python scripts/26_motherduck_materialize_v2.py --md

# Run verification reports
.venv/bin/python scripts/54_motherduck_verification_reports.py --md

# Run validation suite
.venv/bin/python scripts/55_analysis_validation_suite.py --md
```

Reports are written to `exports/verification_reports/`.

---

## Deployment Order (Full Pipeline)

```
# Existing pipeline (unchanged):
scripts/15 → 16 → 17 → 18 → 19 → 20 → 22 → 23 → 24 → 25 → 27 → 46 → 47

# New analysis-grade pipeline (scripts 48-55):
scripts/51_thyroid_scoring_systems.py    # Phase 4 (no dependencies on new scripts)
scripts/50_multinodule_imaging.py        # Phase 3 (reads raw_us_tirads_excel_v1)
scripts/49_enhanced_linkage_v3.py        # Phase 2 (reads canonical v2 + multinodule)
scripts/52_complication_phenotyping_v2.py # Phase 5 (reads refined pipelines)
scripts/53_longitudinal_lab_hardening.py # Phase 6 (reads thyroglobulin_labs)
scripts/48_build_analysis_resolved_layer.py # Phase 1 (reads all upstream)
scripts/26_motherduck_materialize_v2.py --md # materialize ~28 new md_ tables
scripts/54_motherduck_verification_reports.py --md # generate QA reports
scripts/55_analysis_validation_suite.py --md # run assertion tests
```

---

## Provisional / Exploratory Flags

The following components are **provisional** and require expert clinical review before publication:

1. **imaging_nodule_master_v1** — nodule identity tracking across serial exams is heuristic only; cross-exam linkage based on laterality+size+TI-RADS stability requires manual verification for the ~12% of patients with >1 exam.

2. **ata_response_category** — ATA response-to-therapy requires explicit stimulated Tg thresholds and post-RAI imaging context. Current derivation uses suppressed Tg nadir as a proxy. Stimulated Tg data is not systematically available.

3. **recurrence_event_clean_v1 biochemical_recurrence** — "rising Tg > 2× nadir" is a simplified biochemical recurrence definition. Clinical interpretation requires review of anti-Tg antibody interference, assay change-overs, and imaging correlation.

4. **complication transient/permanent flags** — temporal classification based on note documentation patterns may underestimate permanence when follow-up notes are sparse (lab_completeness_score < 40).

5. **MACIS/AGES/AMES histologic grade** — The AGES score uses histologic grade from `path_synoptics.tumor_1_histologic_grade`. This field is sparsely populated (<30% coverage) and the calculated grade components should be verified against published histologic criteria.

---

## Known Limitations

1. **imaging_nodule_long_v2 size data is empty on MotherDuck** — The NLP-extracted imaging nodule tables have schema but zero populated size/TIRADS columns. The Excel-based Phase 12 data (`raw_us_tirads_excel_v1`) is the actual data source for TIRADS and nodule sizes.

2. **recurrence_risk_features_mv has multiple rows per patient** — Always aggregate with `BOOL_OR(recurrence_flag)` and `GROUP BY research_id` before joining to patient-level analyses.

3. **molecular_test_episode_v2 ras_flag bug** — `ras_flag` can be FALSE even when `ras_subtype` is populated. Phase 11 corrected this via subtype propagation. Use `ras_positive_final` from `patient_refined_master_clinical_v12` or `patient_analysis_resolved_v1`.

4. **Local DuckDB vs MotherDuck** — Many tables (thyroglobulin_labs, patient_refined_master_clinical_v12, recurrence_risk_features_mv) exist only on MotherDuck. Run analysis scripts with `--md` flag.
