# Labs family Protocol v2 close-out — mig_115

Date: 2026-04-29
Author: Logan Glosser (drafted by Copilot)

## Scope

Closed five Script-347 canonical lab tables under Protocol v2:

- `main.canonical_labs_thyroglobulin_v1`
- `main.canonical_labs_calcium_v1`
- `main.canonical_labs_pth_v1`
- `main.canonical_labs_tsh_v1`
- `main.canonical_labs_vitamin_d_v1`

## Migration

- Migration: `qc_framework_v1/migrations/115_labs_family_signoff.sql`
- Verification method: `structured_source_compare_with_normalizer`
- Batch: `mig_115_labs_family_signoff_20260429`
- Normalizer source of truth: `scripts/_lab_value_normalizer.py`

## Validation results

Normalizer regression suite:

- `tests/test_lab_value_normalizer.py`: 45 passed / 45

Per-row replay of `normalize_lab_value(value_raw, analyte)` against live canonical rows:

| Table | Rows | Patients | value_numeric drift | is_censored drift | correction-note drift |
|---|---:|---:|---:|---:|---:|
| canonical_labs_thyroglobulin_v1 | 53,006 | 3,124 | 0 | 0 | 0 |
| canonical_labs_calcium_v1 | 187 | 166 | 0 | 0 | 0 |
| canonical_labs_pth_v1 | 200 | 184 | 0 | 0 | 0 |
| canonical_labs_tsh_v1 | 556 | 449 | 0 | 0 | 0 |
| canonical_labs_vitamin_d_v1 | 86 | 82 | 0 | 0 | 0 |

Unit vocabulary and source enum checks were clean.

`lab_datetime` remains `TIMESTAMP` for labs. All current rows are midnight-valued, but the column is intentionally not retyped because future structured lab feeds may preserve draw-time fidelity.

## Registry final state

`canonical_column_verification_registry_v1`:

- `canonical_labs_thyroglobulin_v1`: 11 `verified` + 1 `na`
- Each non-Tg lab table: 9 `verified` + 1 `na`

`canonical_table_signoff_registry_v1`:

- All five tables: `table_status = verified`
- `signoff_migration = qc_framework_v1/migrations/115_labs_family_signoff.sql`

## Carry-forwards

Conservative clinical sanity ranges from the prompt are stricter than Script-347 normalizer plausibility bands for a few retained values:

- Calcium: 13 non-censored values outside 6–15 mg/dL but within the normalizer's 4–20 mg/dL plausible/corrected range.
- PTH: 1 value at 1399 pg/mL, within the normalizer's 0–3000 pg/mL plausible range.
- TSH: 6 non-censored values outside 0.01–100 mIU/L, within the normalizer's 0–150 mIU/L plausible range.

These were documented as informational carry-forwards, not verification failures.