# FHIR mapping policy (analytic export)

**Scope:** `main.fhir_*_v1` tables in [`scripts/sql/138_specimen_fhir_layer_ddl.sql`](../../scripts/sql/138_specimen_fhir_layer_ddl.sql).

## Goals

- Produce **de-identified**, **research-grade** JSON resources for interoperability experiments and bundling.  
- Preserve **internal traceability** to `specimen_id` / fingerprints without emitting PHI.

## Non-goals / disclaimers

- **Not** US Core IG complete, **not** asserted for clinical exchange or billing.  
- **No** LOINC / SNOMED binding in v1 — `code.text` and free-text type fields only where present.  
- **No** performer / organization — would require covered-entity metadata not in scope.  
- **Patient:** synthetic id from truncated hash (`Patient/` + 16 hex) — **low collision risk** but not a global unique id scheme; document collision check if expanding to multi-site.

## Resource mapping summary

| FHIR resource | Source | Key fields |
|---------------|--------|------------|
| Patient (reference only) | `research_id` | `patient_fhir_id` in map table; referenced as `Patient/{id}`. |
| Specimen | `specimen_master_v1` | `identifier` system `urn:oid:thyroid2026:specimen-fingerprint` value = full fingerprint hex; `type.text` = `specimen_role`; `receivedTime` ISO instant when day parseable. |
| Procedure | `specimen_master_v1` | `code.text` = `Thyroid specimen collection`; `performedDateTime` from procedure day. |
| Encounter | `specimen_master_v1` | `status` unknown; `class` AMB; `period.start` when day parseable. |
| EpisodeOfCare | `specimen_master_v1` | `status` active; analytic stub; ties to `surgery_episode_id` when present (id hash includes episode or specimen fallback). |
| Bundle | Join of above | `type` = `collection`; entries Specimen, Procedure, Encounter, EpisodeOfCare. |

## Date / time rules

- Only when `procedure_date_day` matches `^\d{4}-\d{2}-\d{2}$` append `T00:00:00Z`.  
- Non-ISO raw dates → **omit** FHIR datetimes (optional extension field could carry raw string in future — **not** in v1 JSON).

## Identifier policy

- **Specimen business identifier** = fingerprint SHA-256 (full hex) in `identifier[0].value`.  
- **Resource `id`** = truncated fingerprint / hash prefix (16 hex) — **not** globally unique across resource types; clients must use `resourceType` + `id` tuple.

## Validation

- Script 138 runs JSON path checks (e.g. `subject.reference` starts with `Patient/`).  
- `119_md_formalization_validate.py` Check 13 when anchor tables exist.

## Future enhancements (design-only)

- `Specimen.collection.bodySite` from parsed `site_text` with SNOMED CT when mapping table exists.  
- `ServiceRequest` linkage for ordered biopsy (requires order id crosswalk).  
- `DiagnosticReport` linking ThyroSeq results to `Specimen` via accession.
