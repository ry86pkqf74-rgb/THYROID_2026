# Fingerprint and matching policy

Aligned with [`utils/specimen_fingerprint.py`](../../utils/specimen_fingerprint.py) and [`scripts/sql/138_specimen_fhir_layer_ddl.sql`](../../scripts/sql/138_specimen_fhir_layer_ddl.sql). **AGENTS.md:** no fuzzy matching without explicit approval; uncertain merges go to review.

## 1. Exact fingerprint fields (master)

**Input order** (pipe-separated, then SHA-256 hex):

1. `research_id` — lower(trim(string))  
2. `source_system` — lower(trim) — v1 constant `pathology_synoptic_encounter`  
3. `procedure_date_day` — ISO `YYYY-MM-DD` when canonical date exists; else normalized raw string (see `_norm_day` in Python helper)  
4. `accession_or_source_id` — lower(trim(`path_surgery_id`)) or empty  
5. `specimen_role` — lower(trim) — v1 `surgical_resection`  
6. `anatomic_site` — lower(trim) — v1 `thyroid`  
7. `laterality` — lower(trim) — **empty at master** (intentional; site on focus)  
8. `surgery_episode_id` — lower(trim string)  
9. `encounter_synoptic_row_ix` — lower(trim string)

**Identifier:** `specimen_id = 'spm_' || specimen_fingerprint_sha256` (DDL uses full hash hex).

**Invariant:** `COUNT(*) = COUNT(DISTINCT specimen_fingerprint_sha256)` on `main.specimen_master_v1` (validator + tests).

## 2. Exact fingerprint fields (tumor focus)

Payload: `master_fingerprint_sha256` || `synoptic_row_ix` || `tumor_index` || `site_text` || `histologic_type` (each lower/trim rules per SQL).

**Identifier:** `specimen_focus_id = 'spf_' || focus_fingerprint_sha256`.

## 3. Normalization rules

- Empty / NULL / `nan` / `none` / `null` → empty string for fingerprint participation.  
- Dates: prefer `surg_date_canonical` → `strftime('%Y-%m-%d')`; else fall back to trimmed varchar.  
- No case folding on histology beyond lower(trim) — **risk:** semantic duplicates (`PTC` vs `papillary`) remain distinct unless a vocabulary map is added later (**design-only**: optional `histology_norm` column future).  
- `accession_or_source_id`: linkage may be NULL → empty; collisions with empty accession are possible — see risks.

## 4. Deterministic merge rules

- **Auto-merge (implicit):** Rows that collapse to the **same** fingerprint are **one** master row (DDL uses `SELECT DISTINCT` on fingerprint inputs).  
- **No graph merge:** There is **no** transitive merge across different fingerprints (e.g. correcting two accessions to one) without manual data fix upstream.

## 5. Review-only fuzzy candidate rules

**Not implemented as auto-merge.** The following belong in `qa.specimen_merge_review_queue_v1` or future queues:

- Same patient + same calendar day + same `surgery_episode_id` but **different** fingerprints (already emitted as pairs in current DDL).  
- **Future:** Levenshtein / token similarity on `accession_or_source_id` with mismatching episode (requires explicit product approval per AGENTS.md).  
- **Future:** Synoptic row duplication with conflicting `path_surgery_id` after linkage correction.

## 6. Explicit “do not auto-merge” cases

| Situation | Policy |
|-----------|--------|
| Multifocal: multiple `specimen_focus_id` under one `specimen_id` | **Expected** — do not collapse foci. |
| Multiple molecular episodes → same surgery + multiple tumor foci | **Do not** auto-pick focus without rule; current DDL uses `min(specimen_focus_id)` in aggregation — **treat as known limitation**; prefer `review_flag` or NULL focus. |
| NLP `specimen_detail` vs synoptic accession mismatch | **Do not auto-merge**; xref + manual review. |
| ThyroSeq / `molecular_results` accession vs `path_surgery_id` format drift | **Exact match only** for auto-bind; else queue. |
| Patient-level `specimen_detail` parquet (script 07) without `synoptic_row_ix` | **Do not** merge into master without crosswalk. |
| Discordant linkage tiers `weak` / `unlinked` | **Do not** treat as pathology-proven specimen binding for assay; keep `binding_confidence_tier` + `review_flag`. |
| Any guess on laterality from free text when `site_text` null | **Do not** impute for fingerprint; optional nullable enrichment column only. |

## 7. Genomic binding tiers (deterministic only in SQL)

From DDL `CASE`:

- `A_exact_high`: focus present AND FNA–molecular tier ∈ {`exact_match`, `high_confidence`}.  
- `B_specimen_only`: specimen without exact/high FNA link.  
- `C_review`: intermediate / ambiguous.  
- `D_unlinked`: no FNA linkage row.

**No ML / fuzzy** in v1 binding.
