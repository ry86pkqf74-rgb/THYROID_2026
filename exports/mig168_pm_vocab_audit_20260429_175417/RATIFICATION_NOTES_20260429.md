# Histology Vocab Ratification — Notes for mig_172

**Date:** 2026-04-29
**Ratifier:** Logan (clinical decisions); Cowork (mechanical mapping authoring)
**Files in this folder relevant to mig_172:**
- `pm_ssot_enum_dictionary_draft.csv` — original 2,128-row dictionary draft from mig_168 audit (read-only reference)
- **`pm_ssot_enum_dictionary_draft_ratified.csv`** — Logan-ratified mappings for the 4 histology enum cols (97 raw value rows; awaiting your spot-check before Cursor proceeds)
- **`pm_histology_per_patient_overrides_20260429.csv`** — 4 per-patient overrides for catch-all labels resolved via `canonical_path_malignant_events_v1`
- This file (`RATIFICATION_NOTES_20260429.md`) — clinical rules + scope reduction explanation

---

## Scope reduction from original mig_172 prompt

The original `CURSOR_PROMPT_mig172_vocabulary_normalization_apply_20260429.md` listed 8 histology cols. Investigation 2026-04-29 (chat-2) showed only 4 are enum candidates per the mig_168 audit catalog:

| Col | enum_candidate | reason |
|---|---|---|
| `recurrence_histology` | YES | controlled_vocab_candidate, 33 raw → 12 canonical |
| `recurrence_histology_v2` | YES | controlled_vocab_candidate, 22 raw → ~10 canonical |
| `completion_prior_histology` | YES | controlled_vocab_candidate, 12 raw → 7 canonical |
| `completion_histology_type` | YES | controlled_vocab_candidate, 8 raw → 5 canonical |
| `histologic_types_all` | NO | high_cardinality_text (multi-label `'PTC; FTC'` style) — needs separate lane |
| `histologic_variants_all` | NO | high_cardinality_text — needs separate lane |
| `path_histology_raw` | NO | free_text_or_identifier — needs separate lane |
| `path_histology_variant_raw` | NO | free_text_or_identifier — needs separate lane |

**mig_172 should now scope to the 4 enum cols only.** The 4 multi-label / free-text cols belong in a future lane (call it mig_172b) that uses multi-label parsing similar to mig_174's cnln_img_laterality treatment.

---

## Logan's ratified clinical rules (2026-04-29)

### Rule 1: canonical_code = base tumor diagnosis (strip metastatic/recurrent prefixes)

Reason: metastatic-ness is a node-status fact derivable from the LN canonical, not a histology fact. The tumor diagnosis is per-tumor; if a patient has multiple tumors, they get multiple histology entries.

Examples:
- `'metastatic PTC'` → `canonical_code='ptc'`, `variant_subtype=NULL`
- `'Recurrent/metastatic follicular carcinoma'` → `canonical_code='ftc'`, `variant_subtype=NULL`
- `'metastatic PTC tall cell variant'` → `canonical_code='ptc'`, `variant_subtype='tall_cell'`

### Rule 2: canonical_code + variant_subtype, applied to ALL tumors (not just PTC)

The schema reform that mig_172 must perform on `canonical_patient_master`:

| Existing col | New col added (DOUBLE / VARCHAR) |
|---|---|
| `recurrence_histology` (VARCHAR) | `recurrence_histology_variant_subtype` (VARCHAR) |
| `recurrence_histology_v2` (VARCHAR) | `recurrence_histology_v2_variant_subtype` (VARCHAR) |
| `completion_prior_histology` (VARCHAR) | `completion_prior_histology_variant_subtype` (VARCHAR) |
| `completion_histology_type` (VARCHAR) | `completion_histology_type_variant_subtype` (VARCHAR) |

The existing 4 cols get rewritten to canonical_code values. The 4 new cols carry the variant_subtype values.

### Rule 3: DHGTC stays separate from PDTC

Per WHO 2022, Differentiated High Grade Thyroid Carcinoma (DHGTC) and Poorly Differentiated Thyroid Carcinoma (PDTC) are distinct entities — DHGTC retains differentiation, PDTC does not. The dictionary keeps separate canonical codes:

- `canonical_code='dhgtc'` for "differentiated high grade thyroid carcinoma" rows
- `canonical_code='pdtc'` for "poorly differentiated thyroid carcinoma" rows

### Rule 4: catch-all vague labels resolved via per-patient override (option i)

Three raw label families are too vague for a static dictionary mapping:
- `'metastatic carcinoma'` / `'Metastatic Carcinoma'`
- `'metastatic thyroid carcinoma'`
- `'differentiated thyroid carcinoma'`

For these, the dictionary maps to `canonical_code='dtc_nos'` as a placeholder. The 4 specific patients carrying these labels get overridden via `pm_histology_per_patient_overrides_20260429.csv`, which sets canonical_code + variant_subtype based on `canonical_path_malignant_events_v1` evidence.

| pt | raw label | path_event truth | resolution |
|---|---|---|---|
| 3129 | "Metastatic Carcinoma" | PTC + follicular variant | `ptc` / `follicular_variant` |
| 111 | "differentiated thyroid carcinoma" | DTC + Mixed Papillary & follicular | `ptc` / `mixed_papillary_follicular_dtc_label` (option a — treat as PTC family) |
| 3726 | "metastatic carcinoma" | PTC + classical/microcarcinoma | `ptc` / `classical_microcarcinoma` |
| 6922 | "metastatic thyroid carcinoma" | PTC + classical/follicular | `ptc` / `classical_with_follicular` |

The mig_172 apply SQL must run the per-patient overrides AFTER the dictionary mapping, so they overwrite the placeholder `dtc_nos`.

---

## Canonical tumor codes (final consolidation)

The 4 enum cols collapse to **12 base tumor diagnosis codes**:

| canonical_code | display label | scope |
|---|---|---|
| `ptc` | PTC | Papillary Thyroid Carcinoma (all subtypes via variant_subtype) |
| `ftc` | Follicular Carcinoma | Follicular Thyroid Carcinoma |
| `mtc` | MTC | Medullary Thyroid Carcinoma |
| `niftp` | NIFTP | Noninvasive Follicular Thyroid neoplasm with Papillary-like nuclear features |
| `ftump` | FTUMP | Follicular Tumor of Uncertain Malignant Potential |
| `pdtc` | PDTC | Poorly Differentiated Thyroid Carcinoma |
| `dhgtc` | DHGTC | Differentiated High Grade Thyroid Carcinoma (WHO 2022) |
| `atc` | ATC | Anaplastic Thyroid Carcinoma |
| `angiosarcoma` | Angiosarcoma | Angiosarcoma of the thyroid (rare non-thyroid-cell tumor) |
| `neuroendocrine` | Neuroendocrine Tumor | High-grade neuroendocrine tumor (rare) |
| `mtc_ptc_mixed` | MTC/PTC Mixed Composite | Composite tumor (rare) |
| `atypical_hurthle_neoplasm` | Atypical Hurthle Cell Neoplasm | WHO 2022 Hurthle cell UMP family |
| `dtc_nos` | DTC (NOS) | Catch-all when no specific tumor type identifiable; per-patient overrides apply |

---

## Variant subtype catalog (observed in this draft)

Variant subtypes recorded across the 4 enum cols (lowercase, snake_case, conservative scope):

| variant_subtype | scope |
|---|---|
| `classical` | PTC classical / classic |
| `tall_cell` | PTC tall cell variant / tall cell features |
| `follicular_variant` | PTC follicular variant |
| `oncocytic_classical` | PTC oncocytic classical |
| `high_grade` | PTC high grade or neuroendocrine high grade |
| `classical_with_focal_tall_cell` | PTC classical with focal tall cell features (<5%) |
| `classical_with_tall_cell_25pct` | PTC classical with tall cell ~25% (specific quantitative variant) |
| `classical_microcarcinoma` | PTC classical microcarcinoma (per-patient override) |
| `classical_with_follicular` | PTC classical with follicular (per-patient override) |
| `mixed_papillary_follicular_dtc_label` | Pt 111-specific: DTC label with mixed papillary/follicular variant; treated as PTC family (option a) |

---

## What Cursor (or Logan/Cowork) does next

1. Logan spot-checks `pm_ssot_enum_dictionary_draft_ratified.csv` (97 rows). Edit if any mapping looks wrong.
2. Logan spot-checks `pm_histology_per_patient_overrides_20260429.csv` (4 rows).
3. Logan signals "continue mig_172" to Cursor (or to next Cowork session).
4. Cursor / Cowork:
   - Updates `cursor_prompts/CURSOR_PROMPT_mig172_vocabulary_normalization_apply_20260429.md` to reflect 4-col scope + new variant_subtype col addition + per-patient override block
   - Authors `qc_framework_v1/migrations/172_vocabulary_normalization_apply_20260429.sql`:
     - Section A — pre-snapshot (4 col-slices archived to `archive_pub_v1_0`)
     - Section B — schema reform: ALTER ADD COLUMN ×4 new variant_subtype VARCHAR cols
     - Section C — INSERT into `main.histology_vocab_normalization_map_v1` from this ratified CSV
     - Section D — UPDATE the 4 base cols using the mapping table
     - Section E — UPDATE the 4 new variant_subtype cols using the mapping table
     - Section F — UPDATE per-patient overrides for the 4 catch-all patients
     - Section G — registry resync (4 cols: notes appendix; 4 new cols: register as not_started)
     - Section H — post-state verification probes (commented)
5. Cowork verifies post-state distribution; CFs `CF-mig168-VOCAB-DRIFT-RECURRENCE-HISTOLOGY-42-VALUES` etc. close; opens new informational CF for the 4 catch-all overrides.

---

## Things this lane does NOT close

- The ~228-pt **semantic** drift between `PM.histology_final` and `canonical_path_malignant_patient_rollup_v1.dominant_histology` (chat-2 finding) is largely separate. mig_172 normalizes recurrence + completion histology cols; it does not touch `histology_final`. That drift needs a separate reconcile lane (call it mig_178?) — could leverage the same per-patient override pattern.
- The 4 multi-label / free-text histology cols (`histologic_types_all`, `histologic_variants_all`, `path_histology_raw`, `path_histology_variant_raw`) need their own normalization lane (mig_172b suggested).

---

End of ratification notes.
