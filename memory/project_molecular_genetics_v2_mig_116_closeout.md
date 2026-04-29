# Molecular genetics v2 Protocol v2 close-out — mig_116

Date: 2026-04-29
Author: Logan Glosser (drafted by Copilot)

## Scope

Closed `main.canonical_molecular_genetics_v2` under Protocol v2.

- Rows: 1,384
- Patients: 1,151
- Columns: 74 total = 69 verified + 5 `na`
- Migration: `qc_framework_v1/migrations/116_molecular_genetics_v2_signoff.sql`
- Batch: `mig_116_molecular_genetics_v2_signoff_20260429`

## Verification method

Source-family archive replay against:

- `"Thyroid 2026 UPdated".molecular_legacy_20260421.molecular_test_episode_v2`

Join keys:

- `molecular_testing`: `research_id + molecular_episode_id`
- script-269 backfill rows: `research_id + report_source_table + platform`

Source mix verified:

| Source family | Rows |
|---|---:|
| `molecular_testing` | 859 |
| `thyroseq_molecular_enrichment` | 443 |
| `extracted_braf_recovery_v1` | 46 |
| `ret_patient_adjudicated_v226` | 36 |

All 1,384 canonical rows joined back to the archived source with 0 no-join rows. Source-preserved fields had 0 drift; `resolved_test_date` matched the source ISO date after canonical `MM/DD/YYYY` formatting.

Parser/result fields were verified by provenance and internal non-regression checks:

- Expected source families only.
- `report_text_ref` present on 1,384/1,384 rows.
- Variant arrays: 738 tests / 936 variant structs.
- Fusion arrays: 48 tests / 60 fusion structs.
- Platform vocabulary clean: `ThyroSeq`, `Afirma`, `NGS_unspecified`.
- ROM ordering invariant `low <= point <= high`: 0 violations.
- GEN12 status-clean view had 0 nonstandard status rows.

## Registry final state

`canonical_column_verification_registry_v1`:

- 69 `verified`
- 5 `na` (`research_id`, `molecular_episode_id`, `linked_fna_episode_id`, `linked_nodule_id`, `linked_surgery_episode_id`)

`canonical_table_signoff_registry_v1`:

- `table_status = verified`
- `signoff_migration = qc_framework_v1/migrations/116_molecular_genetics_v2_signoff.sql`

## Carry-forwards

- `CF-mig116-MOL-DATE-RETYPE`: `test_date_native` is `TIMESTAMP`; `resolved_test_date` is `VARCHAR`. Both are clinical event dates and should be retyped to `DATE` in a future date-cleanup pass. Not a sign-off blocker.
- `CF-mig116-MOL-LINKAGE-ID`: `linked_fna_episode_id` is populated on 374 rows while current guidance prefers research_id plus governed linkage views. Use `manuscript_workspace.canonical_molecular_genetics_v2_fna_rebind`, not the stored legacy ID.
- `CF-GEN07-ROM-OCR`: 2 source-faithful ROM values are outside 0–100 (`599%`, `395%`) but ordering is valid; hand correction remains deferred.

## Validation evidence

Post-apply probes:

- Registry: 69 verified + 5 na; 0 not_started; 0 failed.
- Signoff: 74 total, table_status verified.
- Cohort: 1,384 rows / 1,151 patients.
- Platform counts: ThyroSeq 885, Afirma 417, NGS_unspecified 82.
- ROM ordering: 0 low>point, 0 point>high, 0 low>high; 2 out-of-range OCR carry-forwards.
- Linkage populated: `linked_fna_episode_id` 374, `linked_nodule_id` 0, `linked_surgery_episode_id` 0.