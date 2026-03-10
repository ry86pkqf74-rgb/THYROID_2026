# Clinical Notes Entity Extraction Specification

## Overview

This document describes the entity-extraction framework that operates on
`clinical_notes_long` and produces normalised derived tables in DuckDB /
MotherDuck.  Every extracted fact is traceable back to the source note via
`note_row_id` and an exact `evidence_span` substring.

## Derived Tables

| Table | Entity domain | Examples |
|-------|--------------|----------|
| `note_entities_problem_list` | Comorbidities / diagnoses | hypertension, diabetes, obesity, GERD |
| `note_entities_medications` | Drug mentions (+ dose) | levothyroxine 125 mcg, calcitriol |
| `note_entities_procedures` | Surgical procedures | total thyroidectomy, central neck dissection |
| `note_entities_complications` | Post-op complications | RLN injury, hypocalcemia, hematoma |
| `note_entities_staging` | AJCC T/N/M/overall stage | pT1a, N1b, Stage II |
| `note_entities_genetics` | Gene / mutation mentions | BRAF V600E, RET/PTC, TERT promoter |

## Common Schema

All six tables share these columns:

| Column | Type | Description |
|--------|------|-------------|
| `research_id` | INT | Patient identifier |
| `note_row_id` | VARCHAR | FK to `clinical_notes_long.note_row_id` |
| `note_type` | VARCHAR | Note category from controlled vocabulary |
| `entity_type` | VARCHAR | Domain-specific type (e.g. `complication`, `gene`) |
| `entity_value_raw` | VARCHAR | Raw matched string |
| `entity_value_norm` | VARCHAR | Normalised value from controlled vocabulary |
| `present_or_negated` | VARCHAR | `present` or `negated` |
| `confidence` | FLOAT | 0.0 - 1.0 (regex = 0.9 default, LLM = model confidence) |
| `evidence_span` | VARCHAR | Exact substring from `note_text` |
| `evidence_start` | INT | Character offset of span start |
| `evidence_end` | INT | Character offset of span end |
| `entity_date` | VARCHAR | Date found near the entity in note text (YYYY-MM-DD), if any |
| `note_date` | VARCHAR | Encounter/service date from the note header (YYYY-MM-DD), fallback |
| `extraction_method` | VARCHAR | `regex` or `llm_<model>` |
| `extracted_at` | VARCHAR | ISO-8601 timestamp |

## Controlled Vocabularies

### note_type

Canonical values (matches `config/notes_column_map.csv`):

`h_p`, `op_note`, `dc_sum`, `ed_note`, `endocrine_note`,
`history_summary`, `other_history`, `other_notes`

### procedure_type (note_entities_procedures)

| Normalised value | Aliases |
|-----------------|---------|
| `total_thyroidectomy` | total thyroidectomy, TT, bilateral thyroidectomy |
| `hemithyroidectomy` | hemithyroidectomy, thyroid lobectomy, lobectomy |
| `completion_thyroidectomy` | completion thyroidectomy, completion |
| `central_neck_dissection` | central neck dissection, CND, level VI |
| `lateral_neck_dissection` | lateral neck dissection, LND, levels II-V |
| `modified_radical_neck_dissection` | MRND, modified radical |
| `parathyroid_autotransplant` | parathyroid autotransplant, autotransplantation |
| `tracheostomy` | tracheostomy |
| `laryngoscopy` | laryngoscopy, flex laryngoscopy |

### complication_type (note_entities_complications)

| Normalised value | Aliases |
|-----------------|---------|
| `rln_injury` | RLN injury, recurrent laryngeal nerve injury |
| `vocal_cord_paralysis` | vocal cord paralysis, VCP, cord palsy |
| `vocal_cord_paresis` | vocal cord paresis, cord weakness |
| `hypocalcemia` | hypocalcemia, low calcium |
| `hypoparathyroidism` | hypoparathyroidism |
| `hematoma` | hematoma, neck hematoma |
| `seroma` | seroma |
| `wound_infection` | wound infection, SSI, surgical site infection |
| `chyle_leak` | chyle leak, chylous fistula |

### gene (note_entities_genetics)

`BRAF`, `NRAS`, `HRAS`, `KRAS`, `RET`, `TERT`, `NTRK`, `ALK`

### staging_component (note_entities_staging)

`T_stage`, `N_stage`, `M_stage`, `overall_stage`

### problem_type (note_entities_problem_list)

`hypertension`, `diabetes_type2`, `diabetes`, `obesity`, `CAD`,
`atrial_fibrillation`, `hypothyroidism`, `hyperthyroidism`,
`breast_cancer`, `lung_cancer`, `GERD`, `CKD`, `depression`,
`asthma`, `COPD`

### medication_type (note_entities_medications)

`levothyroxine`, `calcium_supplement`, `calcitriol`, `rai_dose`

## Extraction Methods

### Regex (high precision)

Each entity domain has a dedicated `*Extractor` class in
`notes_extraction/extract_regex.py`.  Patterns are designed for high
precision (low false-positive rate).  Negation is detected by scanning a
30-character window before the match for negation cues (`no`, `without`,
`denies`, `negative for`, `ruled out`, etc.).

### LLM (optional, higher recall)

The `LLMExtractor` in `notes_extraction/extract_llm.py` sends truncated
note chunks to an LLM with a structured output schema.  It is gated behind
the `OPENAI_API_KEY` environment variable and returns empty results if the
key is not set.  The LLM must return `evidence_span` that is an exact
substring of the input.

## Date Association Strategy

Every extracted entity carries two date fields:

1. **`entity_date`**: A date found within +-120 characters of the entity
   match in the note text.  This is the most specific date available.
   Dates must fall within 1990-2030 to be accepted.

2. **`note_date`**: The encounter/service date extracted from the note
   header (first 500 characters).  This serves as a fallback when no
   entity-specific date is found nearby.

The `clinical_notes_long` table also carries `note_date` so that
downstream consumers can join on it.

Date extraction priority for `note_date`:
1. Explicit label: "Date of Service:", "Admission Date:", etc.
2. Leading date at the very start of the note text
3. First plausible date (1990-2030) in the first 500 characters

## Reproducibility

- All outputs are deterministic from raw inputs + config.
- Random seeds fixed at 42 for any sampling operations.
- Timestamps use UTC.
- PHI guard: full note text is never logged; only the first 80 characters
  are shown in debug output.
