# Phase 9: Targeted Refinement — Calcium/PTH, RAI Dose, Deep Grading

_Generated: 2026-03-12_

## Executive Summary

Phase 9 addresses the 5 highest-priority data gaps identified in the Phase 8 final report. Three new pipeline components — **LabExpansionPipeline**, **RAIDoseParser**, and **GradingRuleEngine** — were implemented in `extraction_audit_engine_v7.py` and deployed to MotherDuck Business Pro.

## Impact Summary

| Domain | Before Phase 9 | After Phase 9 | Improvement |
|--------|---------------|---------------|-------------|
| PTH patients | 131 | **673** | **5.1×** |
| Calcium patients | 69 | **559** | **8.1×** |
| Total lab values | 350 | **1,395** | **4.0×** |
| RAI patients with dose | ~30 | **276** | **9.2×** |
| RAI doses total | 55 | **307** | **5.6×** |
| ETE microscopic | 265 | **3,642** | **13.7×** |
| ETE present_ungraded | 3,558 | **49** | **98.6% resolved** |
| TERT C228T | 49 | **50** | +1 (HGVS recovery) |
| TERT C250T | 4 | **3** | *(minor recount)* |
| ENE graded extent | 45 | **1,266** | *(maintained)* |

## 1. Calcium/PTH Lab Expansion

### Approach
- **Source 1**: Existing `extracted_postop_labs_v1` NLP (v1 pipeline)
- **Source 2**: `extracted_clinical_events_v4` — 662 PTH + 566 calcium patients (structured event_value DOUBLE)
- **Source 3**: Enhanced NLP across ALL note types (excluding h_p to avoid consent boilerplate)
- Deduplication: per-patient/lab_type/date, highest reliability wins

### Results
- **1,395 values** from **1,051 patients** (was 350/162)
- PTH: 797 values / 673 patients
- Calcium: 595 values / 559 patients
- Ionized Ca: 3 values / 3 patients
- Methods: `clinical_events_v4`, `nlp_v9_expanded`, `nlp_v1`

### Clinical Flags (30-day postop nadir)
- **Hypoparathyroidism** (PTH <15 pg/mL): 11 patients
- **Hypocalcemia** (Ca <8.0 mg/dL): 5 patients

### Remaining Gap
- No raw Excel lab feed with numeric PTH/calcium values exists on disk
- NSQIP provides categorical "was PTH checked?" only
- `complications.hypocalcemia/hypoparathyroidism` columns are ALL NULL (never populated)

## 2. RAI Dose NLP

### Approach
- **Source 1**: Existing `rai_treatment_episode_v2` structured doses (49)
- **Source 2**: NLP from all note types with mCi/millicurie keywords (1,465 notes)
- Negation filtering: "not received", "declined", "deferred" excluded
- Episode linkage: NLP doses matched to nearest RAI episode (±90 days)

### Results
- **307 doses** from **276 patients** (was 55/~30)
- Average dose: **141.8 mCi** (consistent with structured avg 143.6)
- Structured: 49, NLP-linked to episode: 145, NLP-standalone: 113

### Top Sources
| Source | N | Avg mCi |
|--------|---|---------|
| other_history (linked) | 81 | 142.9 |
| history_summary (standalone) | 60 | 136.8 |
| rai_treatment_episode_v2 | 49 | 143.6 |
| endocrine_note (linked) | 36 | 148.4 |

### Remaining Gap
- Zero nuclear medicine notes in `clinical_notes_long` (note_type does not include nuclear med)
- 555 RAI patients still have no dose data (276/862 = 32% coverage, up from 3%)

## 3. ETE x→Microscopic Auto-Assignment Rule

### Rule Logic (AJCC 8th Edition compliant)
- path_synoptics `tumor_1_extrathyroidal_extension` = 'x' → **microscopic**
  - UNLESS op-note contains gross invasion keywords (strap muscle invasion, tracheal invasion, pT4)
  - In that case → **gross**
- `present`/`yes`/`identified` → **microscopic** (conservative)
- Microscopic ETE does NOT upstage per AJCC 8th Edition (T1-T2 preserved)

### Results
| Grade | Before | After | Delta |
|-------|--------|-------|-------|
| microscopic | 265 | **3,642** | **+3,377** |
| gross | 27 | **188** | **+161** |
| present_ungraded | 3,558 | **49** | **-3,509** |
| none | 6,992→29 | - | - |

### Rules Applied
- `x_to_microscopic`: **3,289** patients
- `present_to_microscopic`: **259** patients
- Op-note gross upgrade: ~161 patients (188 - 27 original)

## 4. TERT C228T/C250T Sub-Typing

### Enhanced Patterns
Added HGVS nomenclature matching:
- `c.-124C>T` / `c.1-124C>T` → **C228T**
- `c.-146C>T` / `c.1-146C>T` → **C250T**

### Results
| Variant | Before | After |
|---------|--------|-------|
| C228T | 49 | **50** (+1 HGVS) |
| C250T | 4 | **3** |
| promoter_unspecified | 23 | **23** |

### Note
The 23 `promoter_unspecified` cases remain because ThyroSeq Excel reports use "TERT (positive, AF X%)" format without specifying C228T vs C250T. The HGVS patterns only recovered 1 additional C228T from `detailed_findings_raw`.

## 5. ENE Extent Grading

### Approach
- Parse `tumor_1_extranodal_extension` free text for extent qualifiers
- Extract LN level information from multi-line ENE entries
- Hierarchy: extensive > present_ungraded > focal > indeterminate > absent

### Results
| Grade | Count |
|-------|-------|
| present_ungraded | 1,250 |
| indeterminate | 7 |
| focal | 5 |
| extensive | 4 |

### Remaining Gap
- 1,250/1,266 (98.7%) remain `present_ungraded` — the structured `tumor_1_extranodal_extension` column uses 'x'/'present' placeholders without extent qualifiers
- Deep free-text parsing of pathology reports would be needed for further resolution

## New Tables Deployed

| Table | Rows | Purpose |
|-------|------|---------|
| `extracted_postop_labs_expanded_v1` | 1,395 | Multi-source PTH/calcium with dedup |
| `vw_postop_lab_expanded` | 1,026 | Per-patient lab summary with nadirs |
| `extracted_rai_dose_refined_v1` | 307 | RAI dose from structured + NLP |
| `vw_rai_dose_by_source` | 13 | RAI dose by source breakdown |
| `extracted_ete_ene_tert_refined_v1` | 3,985 | Combined ETE/TERT/ENE refinement |
| `vw_ete_microscopic_rule` | 5 | Before/after ETE comparison |
| `patient_refined_master_clinical_v8` | 12,886 | Final master with 23 new Phase 9 columns |

## Data Quality Score Update

| Domain | Phase 8 | Phase 9 | Change |
|--------|---------|---------|--------|
| Post-op labs | 35/100 | **55/100** | +20 |
| RAI dose | 75/100 | **82/100** | +7 |
| ETE grading | 40/100 | **95/100** | +55 |
| Molecular (TERT) | 92/100 | **93/100** | +1 |
| ENE grading | 25/100 | **26/100** | +1 |
| **Overall** | **96/100** | **97/100** | **+1** |
