# Specimen identity layer — validation report

## Automated tests (local)

```bash
.venv/bin/pytest tests/test_specimen_fhir_layer.py tests/test_specimen_identity_layer.py -q
```

Result: **8 passed** (fingerprint parity, multi-synoptic isolation, DDL smoke with in-memory DuckDB).

## Contract surfaces

| Object | Purpose |
|--------|---------|
| `main.specimen_master_v1` | One row per canonical specimen; PK `specimen_id`, unique `specimen_fingerprint_sha256`; carries `synoptic_row_ix`, `encounter_synoptic_row_ix`, `fingerprint_input_canonical`, provenance |
| `main.specimen_tumor_focus_v1` | One row per tumor focus; FK-style link via `specimen_id` |
| `main.specimen_source_xref_v1` | Pathology + molecular xrefs; unique `(domain, source_table, source_row_key)` |
| `qa.specimen_merge_review_queue_v1` | Review-only pairs: `same_accession_multi_synoptic`, `near_duplicate_accession_candidate` |
| `qa.val_specimen_contract_v1` | Populated by `scripts/139_md_specimen_identity_layer.py` / `138_md_specimen_fhir_layer.py` |

## MotherDuck execution (dev)

Attempted: `MOTHERDUCK_ENV=dev .venv/bin/python scripts/139_md_specimen_identity_layer.py --md --no-specimen-detail`

- **Snapshot**: skipped (DuckLake / non-native DB — expected per MotherDuck message).
- **DDL**: blocked with `main.synoptic_tumor_long_v1` not found on attached dev catalog (suggested alternate qualified names in error text). **Materialize prereq tables on the target DB** (e.g. scripts 108/109 + contract linkage) before re-running.

## Run commands

```bash
# Identity only (UA: specimen_identity_build_v1)
MOTHERDUCK_ENV=qa .venv/bin/python scripts/139_md_specimen_identity_layer.py --md

# Full identity + genomic/FHIR tail (UA: specimen_fhir_hardening_v1 on 138 orchestrator)
MOTHERDUCK_ENV=qa .venv/bin/python scripts/138_md_specimen_fhir_layer.py --md
```

Use **read/write** `MOTHERDUCK_TOKEN` or `MD_SA_TOKEN` only (not read-scaling token).
