# Source → target inventory (canonical specimen + FHIR)

Grain and identifier notes support **additive** `main.specimen_*_v1`, `qa.*_specimen_*_v1`, and `main.fhir_*_v1`. Columns listed are **specimen-relevant** (not full schemas).

## Legend

- **Seed primary:** directly feeds the current 138 spine.
- **Seed secondary:** can enrich or crosswalk; not sufficient alone for encounter-level specimen identity.
- **Do not auto-seed:** wrong grain or missing linkage; manual model or future work.

---

### `path_synoptics` (wide table; parquet / DB)

| Aspect | Detail |
|--------|--------|
| **Grain** | One row per synoptic pathology record (per research_id can have multiple rows). |
| **Specimen / tumor IDs** | Implicit line identity via row order → `synoptic_row_ix` in derivatives; tumor slots `tumor_1..5_*`. |
| **Dates** | `surg_date` (stringy); canonical parsing in encounter QC. |
| **Site / laterality** | `tumor_N_site*` / `tumor_1_site_laterality` (slot-dependent). |
| **Accession / source** | Not the primary key in current design; linkage uses `path_surgery_id` from `surgery_pathology_linkage_v3` where available. |
| **Episode IDs** | No native `surgery_episode_id`; joined via linkage + encounter disambiguation. |
| **Tumor-focus keys** | Slot index 1–5 + `synoptic_row_ix`. |
| **Genomics** | Not on this table. |
| **Review / confidence** | Encounter QC flags via `path_synoptics_encounter_qc_v1` / `val_path_synoptic_encounter_isolation_v1` (script 109). |
| **Seed?** | **Seed secondary** (ingest for `synoptic_tumor_long_v1`); not used raw in 138. |

---

### `synoptic_tumor_long_v1` (script [`108_synoptic_tumor_long_v1.py`](../../scripts/108_synoptic_tumor_long_v1.py))

| Aspect | Detail |
|--------|--------|
| **Grain** | One row per **populated** tumor slot (multifocal expansion). |
| **Specimen-related IDs** | `research_id`, `synoptic_row_ix`, `tumor_index`. |
| **Dates** | `surg_date` (raw); aligned to `surg_date_canonical` via encounter QC join. |
| **Site / laterality** | `site` (from slot-specific synoptic column, text). |
| **Accession / source** | `source_table`, `source_file`, `source_column_prefix`; accession filled via pathology linkage. |
| **Episode IDs** | `surgery_episode_id` via `_specimen_path_surgery_link_v1` (best rank linkage). |
| **Tumor-focus keys** | `(synoptic_row_ix, tumor_index)` unique within patient line context. |
| **Genomics** | None. |
| **Review / confidence** | `linkage_confidence_tier`, `linkage_score`, `score_rank` from `surgery_pathology_linkage_v3`. |
| **Seed?** | **Seed primary** for tumor-focus and spine join. |

---

### `path_synoptics_encounter_qc_v1` (script [`109_synoptic_encounter_qc.py`](../../scripts/109_synoptic_encounter_qc.py))

| Aspect | Detail |
|--------|--------|
| **Grain** | One row per `path_synoptics` row with parsed canonical surgery date + `encounter_synoptic_row_ix` within `(research_id, surg_date_canonical)`. |
| **Specimen-related IDs** | All path_synoptics columns + `encounter_synoptic_row_ix`. |
| **Dates** | `surg_date_canonical`, `surg_date_parse_tier`. |
| **Site / laterality** | Inherited tumor_1..5 fields. |
| **Accession / source** | Same as path_synoptics. |
| **Episode IDs** | Disambiguator for multiple synoptic lines same calendar day. |
| **Tumor-focus keys** | Used with long table for tie-break on histology match (138 spine view). |
| **Genomics** | None. |
| **Review / confidence** | `surg_date_parse_tier`; isolation val table for LN mismatch same encounter. |
| **Seed?** | **Seed primary** for encounter disambiguation in fingerprint (`encounter_synoptic_row_ix`). |

---

### `surgery_pathology_linkage_v3` (freeze + script 117; hardened in [`100_episode_linkage_v2_hardening.py`](../../scripts/100_episode_linkage_v2_hardening.py))

| Aspect | Detail |
|--------|--------|
| **Grain** | Candidate links per `(research_id, surgery_episode_id, pathology-side key)` — ranked; 138 takes rank-1 per focus. |
| **Specimen-related IDs** | `path_surgery_id`, `tumor_ordinal`, `surgery_episode_id`. |
| **Dates** | `surg_date`, `path_date`, `day_gap`. |
| **Site / laterality** | Path / surgery laterality fields as produced by linkage builder (see contract DDL). |
| **Accession / source** | `path_surgery_id` maps to accession_or_source_id in `specimen_master_v1`. |
| **Episode IDs** | `surgery_episode_id`. |
| **Tumor-focus keys** | `tumor_ordinal` ↔ `tumor_index` join in 138. |
| **Genomics** | None. |
| **Review / confidence** | `linkage_score`, `linkage_confidence_tier`, `score_rank`; multi-episode QA in script 101. |
| **Seed?** | **Seed primary** for binding synoptic tumor rows to episodes and accessions. |

---

### `tumor_episode_master_v2` (freeze / script 22 family)

| Aspect | Detail |
|--------|--------|
| **Grain** | One row per tumor / surgery episode (spine for surgery dates and episode IDs). |
| **Specimen-related IDs** | `research_id`, `surgery_episode_id`, surgery dates. |
| **Dates** | Surgery timeline. |
| **Site / laterality** | Episode-level pathology summaries (not slot-level). |
| **Accession / source** | Indirect. |
| **Episode IDs** | Canonical surgery episode key for downstream joins. |
| **Tumor-focus keys** | Not slot-level — **cannot** replace synoptic multi-tumor expansion alone. |
| **Genomics** | None. |
| **Seed?** | **Seed secondary** (episode spine context); 138 uses linkage table + synoptic long, not this table directly in DDL excerpt. |

---

### `molecular_test_episode_v2` (freeze / script 117)

| Aspect | Detail |
|--------|--------|
| **Grain** | One row per molecular test episode. |
| **Specimen-related IDs** | `research_id`, `molecular_episode_id`, platform, dates. |
| **Dates** | `test_date_native` etc. |
| **Site / laterality** | As available (often FNA-level). |
| **Accession / source** | Platform / workbook provenance varies. |
| **Episode IDs** | Molecular episode (distinct from surgery_episode_id). |
| **Genomics assay keys** | Platform + episode id. |
| **Review / confidence** |Linkage tiers from `fna_molecular_linkage_v3` / `preop_surgery_linkage_v3`. |
| **Seed?** | **Seed primary** for `specimen_genomic_assay_v1` binding chain. |

---

### `fna_molecular_linkage_v3`, `preop_surgery_linkage_v3` (freeze / script 117)

| Aspect | Detail |
|--------|--------|
| **Grain** | Ranked candidate links (FNA↔molecular; preop↔surgery). |
| **Specimen-related IDs** | `fna_episode_id`, `molecular_episode_id`, `preop_episode_id`, `surgery_episode_id`. |
| **Dates** | Temporal scoring inputs. |
| **Seed?** | **Seed primary** for genomic assay → specimen (via surgery_episode aggregation step in 138). |

---

### `genetic_testing` / `specimen_detail` (script [`07_phase3_genetics_specimen.py`](../../scripts/07_phase3_genetics_specimen.py))

| Aspect | Detail |
|--------|--------|
| **Grain** | **`genetic_testing`:** assay-level rows from Excel; **`specimen_detail`:** gross pathology summary per patient-level extract (not synoptic slot keyed in script header). |
| **Specimen-related IDs** | Genetics: gene flags + test context; specimen_detail: dimensions/margin/capsule — **no `synoptic_row_ix`**. |
| **Dates** | Year-only / test dates in genetics. |
| **Seed?** | **Do not auto-seed** canonical specimen layer without explicit crosswalk design: grain differs from synoptic spine; risk of double-counting or wrong session. **Secondary enrichment** candidate keyed by `research_id` + date/accession rules (future `specimen_source_xref_v1` domains). |

---

### `molecular_results` / `molecular_variant_long` (ThyroSeq governed layer; report in [`docs/THYROSEQ_INTEGRATION_REPORT.md`](../../docs/THYROSEQ_INTEGRATION_REPORT.md))

| Aspect | Detail |
|--------|--------|
| **Grain** | Assay envelope + variant long. |
| **Specimen-related IDs** | ThyroSeq specimen/accession columns in staging/governed tables (see script 131 family). |
| **Seed?** | **Secondary** for genomic assay binding **after** deterministic match policy (exact accession / patient / date window); **not** merged in current 138 SQL (uses `molecular_test_episode_v2` only). |

---

### `canonical_extracted_fact_long_v2` (note-derived facts)

| Aspect | Detail |
|--------|--------|
| **Grain** | One row per extracted entity instance. |
| **Specimen-related entity_type examples** | `specimen_detail`, `specimen_adequacy`, `specimen_type`, `preparation_method`, etc. (see validation report distribution). |
| **Seed?** | **Do not auto-seed** encounter-level `specimen_master_v1`: note entities lack stable synoptic accession join; use for **evidence / NLP cross-check** or future xref rows with `domain='nlp_fact'` and review flags. |

---

## Which existing tables seed the canonical specimen layer today?

| Layer | Seeded by (v1 design / script 138) |
|-------|-------------------------------------|
| `main.specimen_master_v1` | `_specimen_path_surgery_link_v1` ← synoptic spine + `surgery_pathology_linkage_v3` rank-1. |
| `main.specimen_tumor_focus_v1` | Same spine + master fingerprint join. |
| `main.specimen_source_xref_v1` | `specimen_tumor_focus_v1` rows (synoptic long). |
| `main.specimen_genomic_assay_v1` | `molecular_test_episode_v2` + FNA + preop + **min** focus per surgery_episode (see DDL — aggregation may collapse multifocal). |
| FHIR tables | Derived from `specimen_master_v1` + de-id map. |

**Cannot** alone seed the current design: raw `path_synoptics` without long pivot; `tumor_episode_master_v2` without slot key; `genetic_testing` / ThyroSeq governed tables without linkage rules; `canonical_extracted_fact_long_v2` without structured accession alignment.
