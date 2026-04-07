# Proposed additive table contracts (draft)

All objects are **additive** (`CREATE OR REPLACE` acceptable for derived analytics surfaces per [`docs/motherduck_database_contract_v1.md`](../../docs/motherduck_database_contract_v1.md)). This section is **design-level**; implementation already exists in [`scripts/sql/138_specimen_fhir_layer_ddl.sql`](../../scripts/sql/138_specimen_fhir_layer_ddl.sql) — proposed contracts note **intended invariants** and **optional future columns** (not yet in DDL).

## `main.specimen_master_v1`

| Column | Type | Required | Description / constraint |
|--------|------|----------|---------------------------|
| `specimen_id` | VARCHAR | Y | Stable opaque id; prefix `spm_` + fingerprint or hash per implementation. |
| `specimen_fingerprint_sha256` | VARCHAR | Y | **Natural key**; unique across table (validator check). |
| `research_id` | BIGINT | Y | Patient key. |
| `source_system` | VARCHAR | Y | Constant `pathology_synoptic_encounter` in v1. |
| `procedure_date_day` | VARCHAR | Y | ISO `YYYY-MM-DD` when `surg_date_canonical` present; else normalized raw date string (may be non-ISO — FHIR timestamps null). |
| `accession_or_source_id` | VARCHAR | N | From `path_surgery_id` lower/trim; empty string when NULL. |
| `specimen_role` | VARCHAR | Y | v1: `surgical_resection`. |
| `anatomic_site` | VARCHAR | Y | v1: `thyroid`. |
| `laterality` | VARCHAR | Y | v1: **empty at master** — laterality on focus rows (`site_text`). |
| `surgery_episode_id` | BIGINT | N | From linkage rank-1. |
| `encounter_synoptic_row_ix` | INTEGER | N | Disambiguates multiple synoptic lines same canonical day. |
| `materialized_at` | TIMESTAMP | Y | Build audit. |

**Future (optional, not in current DDL):** `release_tag`, `build_git_sha`, `ingest_batch_id` for snapshot alignment.

---

## `main.specimen_tumor_focus_v1`

| Column | Type | Required | Description / constraint |
|--------|------|----------|---------------------------|
| `specimen_focus_id` | VARCHAR | Y | Prefix `spf_` + focus fingerprint. |
| `focus_fingerprint_sha256` | VARCHAR | Y | Unique (validator). |
| `specimen_id` | VARCHAR | Y | FK logical to `specimen_master_v1`. |
| `master_fingerprint_sha256` | VARCHAR | Y | Denormalized master hash. |
| `synoptic_row_ix` | BIGINT | Y | Tumor long line id. |
| `tumor_index` | INTEGER | Y | Slot 1–5. |
| `research_id` | BIGINT | Y | Patient. |
| `surg_date` | VARCHAR | N | Raw synoptic date. |
| `surg_date_canonical` | DATE | N | Parsed canonical date. |
| `encounter_synoptic_row_ix` | INTEGER | N | Encounter disambiguation. |
| `site_text` | VARCHAR | N | Laterality / lobe text from synoptic slot. |
| `histologic_type` | VARCHAR | N | Slot histology. |
| `surgery_episode_id` | BIGINT | N | Episode. |
| `path_surgery_id` | VARCHAR | N | Accession-side id. |
| `tumor_ordinal` | INTEGER | N | From linkage. |
| `linkage_confidence_tier` | VARCHAR | N | From v3 linkage. |
| `linkage_score` | DOUBLE | N | Numeric score. |
| `materialized_at` | TIMESTAMP | Y | Build audit. |

---

## `main.specimen_genomic_assay_v1`

| Column | Type | Required | Description / constraint |
|--------|------|----------|---------------------------|
| `genomic_assay_id` | VARCHAR | Y | Prefix `sga_` + hash(research_id, molecular_episode_id, source). |
| `research_id` | BIGINT | Y | Patient. |
| `molecular_episode_id` | BIGINT | Y | Test episode. |
| `platform` | VARCHAR | N | Assay platform. |
| `test_date_native` | DATE | N | Test date. |
| `fna_episode_id` | VARCHAR | N | Linked FNA. |
| `surgery_episode_id` | BIGINT | N | Via preop chain. |
| `specimen_id` | VARCHAR | N | Bound specimen master. |
| `specimen_focus_id` | VARCHAR | N | Bound focus (**may be arbitrary min() in current DDL** — review for multifocal cases). |
| `fm_tier` | VARCHAR | N | FNA–molecular linkage tier. |
| `preop_tier` | VARCHAR | N | Preop–surgery tier. |
| `binding_confidence_tier` | VARCHAR | Y | `A_exact_high` / `B_specimen_only` / `C_review` / `D_unlinked`. |
| `review_flag` | BOOLEAN | Y | **TRUE** when linkage not exact/high or missing focus. |
| `binding_chain` | VARCHAR | Y | Provenance string of join path. |
| `materialized_at` | TIMESTAMP | Y | Build audit. |

**Design note:** Contract should require `review_flag` semantics documented in fingerprint policy; consider **NULL** `specimen_focus_id` when surgery maps to multiple foci unless deterministic rule selects one.

---

## `main.specimen_source_xref_v1`

| Column | Type | Required | Description |
|--------|------|-------------|-------------|
| `xref_id` | VARCHAR | Y | Opaque. |
| `specimen_id` | VARCHAR | Y | Master. |
| `specimen_focus_id` | VARCHAR | Y | Focus. |
| `domain` | VARCHAR | Y | v1: `pathology`. |
| `source_table` | VARCHAR | Y | e.g. `synoptic_tumor_long_v1`. |
| `source_row_key` | VARCHAR | Y | Composite textual key. |
| `created_at` | TIMESTAMP | Y | Insert audit. |

**Future:** rows for ThyroSeq / genetics / NLP with `domain` ∈ {`molecular_governed`, `nlp_fact`, …}.

---

## `qa.specimen_merge_review_queue_v1`

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `queue_ix` | BIGINT | Y | Surrogate key. |
| `specimen_id_a`, `specimen_id_b` | VARCHAR | Y | Distinct masters. |
| `fp_a`, `fp_b` | VARCHAR | Y | Fingerprints (lexicographic ordering constraint in builder). |
| `research_id` | BIGINT | Y | Shared patient. |
| `procedure_date_day` | VARCHAR | Y | Shared normalized day string. |
| `surgery_episode_id` | BIGINT | N | Shared episode. |
| `reason_code` | VARCHAR | Y | e.g. `same_patient_day_diff_fp`. |
| `evidence_summary` | VARCHAR | Y | Human-readable. |
| `queued_at` | TIMESTAMP | Y | Audit. |

**Future:** `resolution_status`, `resolver`, `resolution_note` for workflow (not in current DDL).

---

## `qa.val_specimen_contract_v1`

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `check_name` | VARCHAR | Y | e.g. `specimen_master_fingerprint_unique`. |
| `status` | VARCHAR | Y | `PASS` / `FAIL`. |
| `detail` | VARCHAR | N | Evidence string. |
| `measured_at` | TIMESTAMP | Y | UTC. |

---

## `main.fhir_patient_deid_map_v1`

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `research_id` | BIGINT | Y | Source id. |
| `patient_fhir_id` | VARCHAR | Y | Opaque short id; **not** reversible from public artifacts alone (salt in DDL string constant — rotate policy if compromised). |

---

## `main.fhir_specimen_v1`

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `fhir_id` | VARCHAR | Y | `Specimen/` + short hash. |
| `patient_fhir_id` | VARCHAR | Y | De-id patient. |
| `specimen_id` | VARCHAR | Y | Internal specimen id. |
| `resource_json` | JSON | Y | Analytic resource; **not** US Core complete. |
| `built_at` | TIMESTAMP | Y | Audit. |

---

## `main.fhir_procedure_collection_v1`

Procedure resource stub for collection context (text code `Thyroid specimen collection`).

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `fhir_id` | VARCHAR | Y | |
| `patient_fhir_id` | VARCHAR | Y | |
| `specimen_id` | VARCHAR | Y | |
| `resource_json` | JSON | Y | |
| `built_at` | TIMESTAMP | Y | |

---

## `main.fhir_encounter_v1`

Encounter stub with `class` AMB and period start from `procedure_date_day` when parseable.

---

## `main.fhir_episode_of_care_v1`

EpisodeOfCare stub; when `surgery_episode_id` null, hashes include `specimen_id` fallback per DDL.

---

## `main.fhir_bundle_specimen_export_v1` (optional export convenience)

Bundle type `collection` aggregating Specimen + Procedure + Encounter + EpisodeOfCare per specimen.
