# Notes Extraction Coverage Truth Audit

**Date:** 2026-03-13

---

## Clinical Notes Corpus

| Note Type | Notes | Patients | % of 10,871 |
|-----------|-------|----------|-------------|
| op_note | 4,680 | 4,439 | 40.8% |
| h_p | 4,221 | 3,999 | 36.8% |
| other_history | 525 | 525 | 4.8% |
| endocrine_note | 519 | 519 | 4.8% |
| ed_note | 498 | 495 | 4.6% |
| history_summary | 249 | 249 | 2.3% |
| dc_sum | 185 | 169 | 1.6% |
| other_notes | 160 | 160 | 1.5% |
| **Total** | **11,037** | **~5,500 unique** | **~50%** |

**Key limitation:** Approximately 50% of the surgical cohort has NO clinical notes in
the `clinical_notes_long` corpus. These patients have structured data only.

---

## NLP Entity Coverage

| Entity Table | Rows | Patients | % of Cohort | Primary Use |
|-------------|------|----------|-------------|-------------|
| note_entities_procedures | 21,942 | 4,723 | 43.4% | Surgery type, CLN/LND flags |
| note_entities_problem_list | 11,579 | 4,037 | 37.1% | Comorbidities |
| note_entities_complications | 9,359 | 2,840 | 26.1% | Complication extraction |
| note_entities_medications | 7,501 | 2,070 | 19.0% | Treatment tracking |
| note_entities_staging | 3,807 | 1,639 | 15.1% | TNM staging |
| note_entities_genetics | 1,738 | 605 | 5.6% | Molecular mentions |

---

## Note-Derived Variable Classification

### Formalized and Manuscript-Safe

| Variable | Source | Validation | Status |
|----------|--------|-----------|--------|
| RLN injury (refined) | note_entities_complications + structured | Intrinsic eval + 3-tier | MANUSCRIPT_SAFE |
| Complications (7 entities) | note_entities_complications | Refined pipeline, 3.3%→validated | MANUSCRIPT_SAFE |
| Recurrence flags | recurrence_risk_features_mv (structured) | Cross-validated | MANUSCRIPT_SAFE |
| BRAF/RAS/TERT status | Structured molecular tables + NLP confirmed | False-positive audit applied | MANUSCRIPT_SAFE |
| Bethesda category | fna_cytology (structured) + note_entities | Multi-source concordance | MANUSCRIPT_SAFE |
| ETE grade (v9) | path_synoptics (structured) + rule engine | AJCC8 microscopic rule | MANUSCRIPT_SAFE |
| Vascular invasion grade | path_synoptics + vessel count | WHO 2022 criteria | MANUSCRIPT_SAFE |

### Exploratory Only (Not Manuscript-Safe Without Caveats)

| Variable | Source | Issue |
|----------|--------|-------|
| Nodule sizes from NLP | h_p/op_note regex | 3,051 patients, no validation against imaging |
| TIRADS from NLP | Clinical note regex | 417 patients, superseded by Excel TIRADS (3,474) |
| Berry ligament | OperativeDetailExtractor | 0% materialized (pipeline gap) |
| Frozen section | OperativeDetailExtractor | 0% materialized (pipeline gap) |
| EBL (estimated blood loss) | OperativeDetailExtractor | 0% materialized (pipeline gap) |
| Voice outcomes | note_entities_complications | 25 patients total; extremely sparse |

### Source-Limited

| Variable | Issue |
|----------|-------|
| Nuclear medicine data | Zero nuclear medicine notes in corpus |
| Calcium/PTH from labs | Only NLP-extracted, no structured lab dates |
| Surgical drain details | Extractor exists but materialization gap |
| Parathyroid count | Extractor exists but materialization gap |

---

## Domain-by-Domain Coverage

### Operative Notes (4,680 notes, 4,439 patients)

- **Extraction coverage**: V2 OperativeDetailExtractor extracts 13 field groups
- **Materialization status**: Script 22 SQL fields populated (rln_monitoring 18.2%,
  drain 1.8%, ETE 0.2%); Phase 76A NLP fields at 0%
- **Manuscript-safe claims**: Procedure type, laterality, CND/LND flags (from structured)
- **Not safe**: Berry ligament, frozen section, EBL (materialization gap)

### H&P Notes (4,221 notes, 3,999 patients)

- **Extraction coverage**: Regex extractors for all 6 entity domains
- **Key contamination**: Consent boilerplate ("risks include...") produces false positives
  for ALL complication entities. Refined pipeline addresses this.
- **Manuscript-safe claims**: Pre-op diagnosis, presenting symptoms
- **Not safe**: Any raw NLP complication count without refinement

### Discharge Summaries (185 notes, 169 patients)

- **Extremely sparse**: Only 1.6% of cohort
- **Manuscript-safe claims**: None as standalone; only supplementary

### Nuclear Medicine Notes

- **ZERO notes in corpus**
- **Impact**: RAI dose coverage capped at 41% from other sources
- **Recommendation**: If institutional NM reports exist, ingestion would significantly
  improve RAI data quality

---

## Acceptance Criteria

- [x] Repo documentation no longer implies note completeness beyond verified
- [x] Manuscript-safe vs exploratory note-derived variables clearly separated
- [x] Source limitations explicitly documented
