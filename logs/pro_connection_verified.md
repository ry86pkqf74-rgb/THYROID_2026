# MotherDuck Business Pro Connection Verification

**Date:** 2026-03-12  
**Database:** thyroid_research_2026  
**Schema:** main  
**Token:** Loaded from `.streamlit/secrets.toml` (467 chars)

## patient_refined_complication_flags_v2

- **Rows:** 287 patients with any refined complication
- **Columns:** 17 (research_id + 7 refined/confirmed pairs + 2 aggregate flags)

### Complication Flag Distribution

| Complication | Confirmed | Refined (incl. probable) |
|---|---|---|
| RLN Injury | 59 | 92 |
| Hypocalcemia | — | 82 |
| Hypoparathyroidism | — | 65 |
| Hematoma | — | 53 |
| Seroma | — | 32 |
| Chyle Leak | — | 20 |
| Wound Infection | — | 14 |

### Before/After: Old NLP vs New Refined

| Complication | Old NLP (patients) | New Refined | Reduction |
|---|---|---|---|
| RLN Injury | 679 (3-tier) / 654 (NLP) | 59 confirmed / 92 refined | 86-91% |
| Hypocalcemia | 1,846 | 82 | 95.6% |
| Hypoparathyroidism | 425 | 65 | 84.7% |
| Hematoma | 141 | 53 | 62.4% |
| Seroma | 845 | 32 | 96.2% |
| Chyle Leak | 1,576 | 20 | 98.7% |
| Wound Infection | 16 | 14 | 12.5% |

**Surgical cohort denominator:** 10,871 patients (path_synoptics)

## Source Tables Verified

| Table | Rows |
|---|---|
| note_entities_complications (old) | 9,359 |
| vw_patient_postop_rln_injury_detail (old) | 679 |
| extracted_rln_injury_refined_v2 | 92 |
| extracted_complications_refined_v5 | 358 |
| patient_refined_complication_flags_v2 | 287 |
