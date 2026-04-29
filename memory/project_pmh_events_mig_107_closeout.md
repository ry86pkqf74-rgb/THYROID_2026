# canonical_pmh_events_v1 Protocol v2 close-out — mig_107

Date: 2026-04-28
Author: Logan Glosser (drafted by Cursor/Copilot)

## Scope

Closed `main.canonical_pmh_events_v1` under Protocol v2.

Final state after `qc_framework_v1/migrations/107_pmh_events_table_signoff.sql`:

- Rows: 12,696
- Source rows:
  - `note_entities_problem_list`: 11,579 rows / 4,037 patients
  - `note_entities_llm_past_medical_hx`: 865 rows / 295 patients
  - `mig_98*_pmh_synthetic`: 246 rows / 221 patients
  - `mig_103_pmh_synthetic`: 6 rows / 4 patients
- Registry: 15 verified + 4 `na` = 19/19 columns closed
- Table signoff: `verified`

## Verification methodology

### Source 1 — legacy problem list

`main.note_entities_problem_list` was no longer live because Script 365 Phase 7 archived/dropped it. Verification used the archived source copy:

`"Thyroid 2026 UPdated".archive_pub_v1_0.note_entities_problem_list_pre365_20260422_064230`

The PMH build SQL was regenerated from `scripts/365_psh_pmh_meds_consolidation.py`, pointed at the archived source, and compared to canonical rows with `source_table='note_entities_problem_list'`.

Result:

- 11,579 expected rows
- 11,579 canonical rows
- 11,579 joined rows
- 0 missing rows
- 0 extra rows
- 0 mismatches across all 15 adjudicated columns under `IS DISTINCT FROM`

### Source 2 — LLM PMH source

The same regenerated Script 365 build SQL re-derived PMH rows from live `main.note_entities_llm_past_medical_hx` JSON entities.

Result:

- 865 expected rows
- 865 canonical rows
- 865 joined rows
- 0 missing rows
- 0 extra rows
- 0 mismatches across all 15 adjudicated columns under `IS DISTINCT FROM`

### Source 3 — Logan-curated synthetic attribution rows

The expected 246 `mig_98*_pmh_synthetic` rows were preserved unchanged and verified-as-injected.

Sanity checks passed for all 246 rows:

- `is_preexisting = TRUE`
- `anchor_source` contains `mig_98` / `logan_curated`
- `evidence_span_hash` is 64 characters
- `finding_value_norm` values: `chyle_leak`, `rln_injury`, `vocal_cord_paralysis`, `seroma`, `hematoma`, `hypoparathyroidism`

### Post-handoff synthetic addendum — mig_103

Live MotherDuck also contained 6 `mig_103_pmh_synthetic` rows from the already-verified medications closure. These were a post-handoff addition relative to the prompt's expected 3-source table. They were handled as a separate synthetic source family and verified-as-injected, not modified.

Sanity checks passed for all 6 rows:

- `is_preexisting = TRUE`
- traceable `anchor_source`
- 64-character `evidence_span_hash`
- values: `calcitriol`, `calcium_supplement`

## Carry-forwards

### CF-PMH-MULTISOURCE-DISAGREEMENT

4 patient/finding keys have multiple PMH sources recording the same normalized item with discordant statuses:

| research_id | finding_value_norm | sources | statuses |
|---|---|---|---|
| 4573 | diabetes_mellitus | legacy + LLM | absent + present |
| 4844 | hypertension | legacy + LLM | absent + present |
| 7555 | obesity | legacy + LLM | absent + present |
| 8002 | hypertension | legacy + LLM | absent + present |

These are cross-source CFs, not signoff blockers.

### CF-PMH-COMPLICATION-MISS

0 missing pairs. Under the available complications schema proxy (`onset_class` + `complication_type`), every present preexisting/prior/not-operative complication row has a corresponding present PMH row.

## Files

- Migration: `qc_framework_v1/migrations/107_pmh_events_table_signoff.sql`
- Verification results: summarized above from the read-only MotherDuck probe; temporary local probe files were not retained.

## Reusable pattern

For multi-source canonical verification:

1. Stratify by `source_table` and verify at row grain.
2. Re-derive deterministic build sources from the original builder when available.
3. If a source has been archived/dropped, use the archived source-of-truth copy named in the build/archive log.
4. Treat curated synthetic rows as `verify_as_injected`, with invariants rather than re-derivation.
5. Re-merge at table signoff using one registry method that explicitly names the per-source verification methods.
