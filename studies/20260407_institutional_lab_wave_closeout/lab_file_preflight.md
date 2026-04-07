# Institutional lab CSV — preflight

## Source file (latest analyst deliverable)

- **Path:** `exports/incoming/final_institutional_chemistry_20260407.csv`
- **Rows:** 989 data rows (excluding header)
- **Wave label:** `final_institutional_20260407`

## Required schema (script 127)

| Column | Status |
|--------|--------|
| `research_id` | Present |
| `lab_date` | Present (ISO dates) |
| `lab_name_standardized` or `lab_name_raw` | Both present |
| `value_raw` | Present |
| `source_lineage_key` | Present; **989/989 unique** (no duplicates, no blanks) |

Optional columns present in this file: `value_numeric`, `unit_raw`, `unit_standardized`, `analyte_group`, `lab_date_status`, `source_table`, `provenance_note`.

## Intended analytes (content check)

Counts by `lab_name_standardized` (case-normalized in load):

| Analyte | Rows |
|---------|------|
| TSH (`tsh`) | 515 |
| PTH (`pth`) | 200 |
| Calcium (`calcium`) | 188 |
| Vitamin D (`vitamin_d`) | 86 |

All four required analytes are present; no extra standardized names in this extract.

## Dry-run

`scripts/127_analyst_institutional_lab_append.py --md --md-sa --input … --ingestion-wave final_institutional_20260407 --dry-run` completed successfully (“Prepared 989 lab row(s)”).
