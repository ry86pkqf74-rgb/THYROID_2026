# Implementation report — specimen + FHIR hardening (pre-run template)

This file is **overwritten** when `scripts/138_md_specimen_fhir_layer.py` executes successfully (see generated `implementation_report.md` with `materialized_at` and git SHA).

**Source inventory**

- `scripts/sql/138_specimen_fhir_layer_ddl.sql` — views/tables/FHIR bundle
- `scripts/138_md_specimen_fhir_layer.py` — MotherDuck orchestration, snapshot preamble, validation rows
- `utils/specimen_fingerprint.py` — fingerprint parity with SQL (`sha256` over normalized `concat_ws`)

**Table contract** — see `docs/motherduck_database_contract_v1.md` § Specimen identity + analytic FHIR.

**Matching policy**

- Encounter specimen: deterministic fingerprint; full-table rebuild is idempotent.
- Merge review queue: same patient / `procedure_date_day` / `surgery_episode_id`, distinct fingerprint (no auto-merge).
- Genomics: `molecular_test_episode_v2` + v3 linkage chain; optional `genetic_testing` rows via **exact** platform string match to a molecular episode, then same chain.

**Tests:** `pytest tests/test_specimen_fhir_layer.py`

**Validation:** `scripts/119_md_formalization_validate.py` Check 13.

Run `138` with `--md` once to populate `audit_memo.md`, `query_history_telemetry.md`, and refresh this report with live counts.
