# Specimen identity + analytic FHIR export — design audit (2026-04-07)

This folder is a **repo-aware design memo only**. No production DDL was applied as part of this audit beyond read-only MotherDuck queries and an attempted named snapshot (skipped on DuckLake-backed catalogs).

**Custom user agent (MotherDuck query attribution):** `specimen_fhir_design_audit_v1`

## Contents

| File | Purpose |
|------|---------|
| [`design_memo.md`](design_memo.md) | Consolidated narrative: goals, reconciliation, inventory, policies, sequence, risks |
| [`artifact_reconciliation.md`](artifact_reconciliation.md) | README vs signoff vs validation report vs live DB |
| [`source_to_target_inventory.md`](source_to_target_inventory.md) | Candidate sources → canonical specimen / FHIR layer |
| [`table_contracts_proposed.md`](table_contracts_proposed.md) | Additive column-level contracts (draft) |
| [`fingerprint_and_matching_policy.md`](fingerprint_and_matching_policy.md) | Exact keys, normalization, merge vs review |
| [`fhir_mapping_policy.md`](fhir_mapping_policy.md) | Analytic FHIR rules and non-goals |
| [`motherduck_audit_evidence.md`](motherduck_audit_evidence.md) | Live attach evidence (counts, snapshot attempt) |

## Relation to checked-in implementation

The repository already defines a materialization path in [`scripts/138_md_specimen_fhir_layer.py`](../../scripts/138_md_specimen_fhir_layer.py) and [`scripts/sql/138_specimen_fhir_layer_ddl.sql`](../../scripts/sql/138_specimen_fhir_layer_ddl.sql). This audit **does not** change those artifacts; it documents canonical identity design, reconciles status artifacts, and records live MotherDuck row counts as of the audit run.
