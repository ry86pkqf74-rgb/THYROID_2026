# Frozen-section patient rollup Protocol v2 close-out — mig_119

Date: 2026-04-29
Author: Logan Glosser (drafted by Copilot)

## Scope

Closed `main.canonical_frozen_section_patient_rollup_v1` under Protocol v2.

The upstream events table, `main.canonical_frozen_section_events_v1`, was already closed by mig_100. Current event state is 7,081 rows across 4,116 patients with `frozen_section_date` as `DATE`.

## Migration

- Migration: `qc_framework_v1/migrations/119_frozen_section_patient_rollup_signoff.sql`
- Verification method: `derivation_re_derivation_post_rollup_rebuild`
- Batch: `mig_119_frozen_section_rollup_signoff_20260429`
- Build lineage: Script 360 Phase 8, wide 12-slot patient rollup from verified events

## Validation results

Pre-signoff probe found the stored rollup was stale after upstream event date/type normalization:

- Events: 7,081 rows / 4,116 patients / 0 duplicate `(research_id, frozen_event_index)` keys
- Rollup pre-rebuild: 4,116 rows / 4,116 patients / 0 duplicate `research_id`
- Schema: 188 columns; 12 visible `frozen_N_*` event slots
- Registry pre-state: 187 `not_started` + 1 `na` (`research_id`)
- Initial re-derivation surfaced more than 5 slot-level drifts, so the rebuild path was used.

The migration archived the pre-rebuild rollup to:

- `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_frozen_section_patient_rollup_v1_pre_mig119_20260429`

Post-rebuild checks:

| Check | Result |
|---|---:|
| Rollup rows | 4,116 |
| Distinct patients | 4,116 |
| Duplicate patients | 0 |
| Max frozen-section events per patient | 13 |
| Patients with event count > 12 slots | 2 |
| Fresh re-derivation drift | 0 columns |
| Archive rows / patients | 4,116 / 4,116 |

`frozen_section_count` preserves all events; the wide `frozen_N_*` columns remain capped at the Script 360 12-slot schema.

## Registry final state

`canonical_column_verification_registry_v1`:

- 187 `verified`
- 1 `na` (`research_id`)
- 0 `not_started`
- 0 `failed`

`canonical_table_signoff_registry_v1`:

- `table_status = verified`
- `signoff_migration = qc_framework_v1/migrations/119_frozen_section_patient_rollup_signoff.sql`

## Audit gates

Post-signoff Protocol v2 audit:

- Gate 1 verified tables total: 55
- Gate 2 missing signoff: 0
- Gate 3 count mismatches: 0
- Gate 4 verified cols missing metadata: 0
- Gate 5 date violations: 18 known CF rows = 4 inherited CF-117 rows + 14 new CF-119 rollup date columns

## Carry-forwards

- `CF-119-FROZEN-ROLLUP-DATE-RETYPE`: 14 rollup clinical date columns remain `VARCHAR` (`MM/DD/YYYY`) to preserve current rollup schema during sign-off: `frozen_section_first_date`, `frozen_section_last_date`, and `frozen_1_date` through `frozen_12_date`. Future batch date-retype migration should handle these with CF-100/117.

Frozen-section family is complete under Protocol v2: events (mig_100) + patient rollup (mig_119).
