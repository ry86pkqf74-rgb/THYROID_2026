# Lab Layer Scaffold Plan — 2026-03-13

## Current State

The canonical lab table `longitudinal_lab_canonical_v1` unifies all current lab sources
into a single long-format table. Current coverage:

| Analyte | Patients | Measurements | Tier | Source |
|---------|----------|-------------|------|--------|
| thyroglobulin | 2,569 | 30,245 | current_structured | thyroglobulin_labs |
| anti_thyroglobulin | 2,127 | 14,314 | current_structured | anti_thyroglobulin_labs |
| parathyroid_hormone | 673 | 797 | current_nlp_partial | extracted_postop_labs_expanded_v1 |
| calcium_total | 559 | 595 | current_nlp_partial | extracted_postop_labs_expanded_v1 |
| calcium_ionized | 3 | 3 | current_nlp_partial | extracted_postop_labs_expanded_v1 |
| tsh | 0 | 0 | future_institutional_required | — |
| free_t4 | 0 | 0 | future_institutional_required | — |
| vitamin_d | 0 | 0 | future_institutional_required | — |
| albumin | 0 | 0 | future_institutional_required | — |

## Canonical Schema: `longitudinal_lab_canonical_v1`

| Column | Type | Description |
|--------|------|-------------|
| `research_id` | INTEGER | Patient key |
| `lab_date` | DATE | Collection/result date |
| `lab_date_status` | VARCHAR | exact_collection_date / extracted_date / unresolved_date |
| `lab_name_raw` | VARCHAR | Original lab name from source |
| `lab_name_standardized` | VARCHAR | Normalized name (thyroglobulin, parathyroid_hormone, etc.) |
| `analyte_group` | VARCHAR | Category (thyroid_tumor_markers, parathyroid, calcium_metabolism, etc.) |
| `value_raw` | VARCHAR | Original result string |
| `value_numeric` | DOUBLE | Parsed numeric value |
| `unit_raw` | VARCHAR | Original unit |
| `unit_standardized` | VARCHAR | Normalized unit (ng/mL, IU/mL, pg/mL, etc.) |
| `reference_range` | VARCHAR | Reference range if available |
| `abnormal_flag` | VARCHAR | H/L/N if available |
| `is_censored` | BOOLEAN | TRUE for "<0.2"-style values |
| `source_table` | VARCHAR | Source table name |
| `source_script` | VARCHAR | Script that produced this row |
| `ingestion_wave` | VARCHAR | Ingestion batch identifier |
| `data_completeness_tier` | VARCHAR | current_structured / current_nlp_partial / future_institutional_required |
| `provenance_note` | VARCHAR | Additional source context |

## Completeness Tiers

| Tier | Meaning |
|------|---------|
| `current_structured` | Available now from structured lab tables with specimen collection dates |
| `current_nlp_partial` | Extracted from clinical notes; limited coverage, lower date confidence |
| `future_institutional_required` | Not yet available; requires institutional lab feed |

## Future Institutional Lab Extract Contract

When the comprehensive institutional lab extract is available, it should:

### Required Fields
- `research_id` (INTEGER) — must match existing patient keys
- `specimen_collect_dt` (TIMESTAMP or DATE) — specimen collection datetime
- `result_report_dt` (TIMESTAMP or DATE) — result reporting datetime
- `lab_name` (VARCHAR) — standard lab test name
- `result` (VARCHAR) — result value as string
- `units` (VARCHAR) — measurement units
- `reference_range_low` / `reference_range_high` (DOUBLE) — normal ranges
- `abnormal_flag` (VARCHAR) — H/L/N/A flags
- `loinc_code` (VARCHAR) — LOINC code if available

### Matching Keys
- Primary: `research_id` (integer, same as all other tables)
- No MRN/DOB matching needed if research_id is populated

### Unit Normalization
Use `lab_normalization_dict_v1` (script 68) for standard mappings:
- Thyroglobulin: ng/mL
- Anti-Tg: IU/mL
- TSH: mIU/L
- Free T4: ng/dL
- PTH: pg/mL
- Calcium: mg/dL
- Ionized Calcium: mmol/L
- Vitamin D: ng/mL
- Albumin: g/dL

### Provenance Requirements
- `source_file` — name of the extract file
- `source_system` — originating lab system
- `extract_date` — when the extract was pulled
- `ingestion_wave` — set to `wave_N_institutional_YYYYMMDD`

### Validation Gates
Before promotion to canonical:
1. No NULL `research_id`
2. All `value_numeric` within plausibility bounds (see `lab_validation_rules_v1`)
3. No future dates in `specimen_collect_dt`
4. Deduplication: no same-patient, same-date, same-analyte, same-value duplicates
5. Unit consistency: all values in expected units per `lab_normalization_dict_v1`
6. Coverage report: at least 50% of surgical cohort for TSH/calcium

### Integration Steps
1. Ingest raw extract into `lab_staging_schema_v1`
2. Run validation rules from `lab_validation_rules_v1`
3. Normalize units via `lab_normalization_dict_v1`
4. Append to `longitudinal_lab_canonical_v1` with new `ingestion_wave` tag
5. Rebuild `val_lab_completeness_v1` to reflect updated coverage
6. Update downstream views/tables that depend on lab data

## Deployment

```bash
# Build on MotherDuck
.venv/bin/python scripts/77_lab_canonical_layer.py --md

# Build locally
.venv/bin/python scripts/77_lab_canonical_layer.py --local
```

Deployment order: after script 53 (longitudinal lab hardening), script 68 (lab scaffold).
