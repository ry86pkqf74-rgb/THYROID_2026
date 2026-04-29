# Molecular genetics from notes v2 Protocol v2 close-out — mig_124

Date: 2026-04-29  
Author: Logan Glosser <logan.glosser@gmail.com>

## Scope

Closed `main.canonical_molecular_genetics_from_notes_v2` under Protocol v2 (Lane 16 — completes molecular family with mig_116 master).

- Rows: 1,738  
- Patients: 605  
- Columns: 28 total = 17 verified + 11 `na`  
- Migration: `qc_framework_v1/migrations/124_molecular_genetics_from_notes_v2_signoff.sql`  
- Batch: `mig_124_molecular_genetics_from_notes_v2_signoff_20260429`

## Upstream (extraction-faithfulness)

Publication `main` no longer holds `note_entities_genetics`; verified against archived snapshot:

`"Thyroid 2026 UPdated".molecular_legacy_20260421.note_entities_genetics` (1,738 rows).

- Natural key `(research_id, note_row_id, evidence_start, entity_value_raw)` after `CAST(research_id AS VARCHAR)` on upstream: multiset EXCEPT ALL vs canonical = **0** both directions.
- 14-column source cluster + `llm_prompt_version` + `verification_status` + `verification_step`: **0** drift both directions.
- `confidence`: uniform **0.9** (regex tier).
- `confidence_score`: **NULL** on all rows upstream and canonical (not equal to `confidence`; extraction-faithful).

## Vocabulary (live canonical)

| Column | Values |
|--------|--------|
| `entity_type` | `gene` only |
| `present_or_negated` | `present` (1,600), `negated` (138) |
| `verification_status` | `unverified` only |
| `extraction_method` | `regex` only |

## Master cross-check (`canonical_molecular_genetics_v2`, mig_116)

- Distinct mention patients: **605**.
- Distinct patients with ≥1 master row (`TRY_CAST(fn.research_id AS INTEGER) = master.research_id`): **372**.
- **233** mention-only patients have no structured master row — **expected** (GEN10 / `qc_framework_v1/README.md`: mentions layer ≠ peer of structured molecular genetics).

## Registry final state (post-apply)

- 17 `verified` (15 extraction-faithfulness + 2 build provenance)  
- 11 `na` unchanged  
- `canonical_table_signoff_registry_v1.table_status` → `verified`

## Carry-forwards

- **CF-mig124-MGFN-BUILT-AT-TZ-RETYPE** — `built_at` is `TIMESTAMP WITH TIME ZONE`; prefer plain `TIMESTAMP` in a future build_ts pass (mig_117 allowlist covers gate 5).
- **CF-mig124-MGFN-MASTER-OVERLAP** — partial overlap with `canonical_molecular_genetics_v2` is by design; use `manuscript_workspace.molecular_mentions_from_notes_v2` for consumption patterns that must not imply formal-test parity.
