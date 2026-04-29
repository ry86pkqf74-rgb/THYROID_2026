# mig_174a — cnln/lateral/ENE multi-label parser design package

**Date:** 2026-04-29  
**Batch:** `mig_174_multilabel_laterality_parser_20260429`  
**Posture:** read-only MotherDuck profile and design package; no data writes.  
**Target:** `thyroid_canonical_publication_v1_0.main.canonical_patient_master` columns `cnln_img_laterality`, `lateral_levels_v10`, and `ene_levels_v9`.  
**Replay SQL:** `qc_framework_v1/migrations/174a_multilabel_laterality_probes_20260429.sql`

## Executive summary

This lane confirms that all three scoped columns need token-level parsing rather than whole-string enum normalization. The observed grammar differs by column:

- `cnln_img_laterality` is true semicolon-delimited multi-label laterality, including literal `null` sentinel tokens mixed with real labels.
- `lateral_levels_v10` is mostly **not** semicolon-delimited in current data. It uses blank strings, Arabic/Roman level labels, comma-separated lists, and ranges such as `levels II-V`.
- `ene_levels_v9` is single-token in current CPM data but still needs the same level parser because it includes Arabic/Roman labels, central-compartment synonyms, and a level range (`level 2-5`).

No `UPDATE`, `ALTER`, registry mutation, or `query_rw` action was performed in mig_174a. This report is the Logan decision package for a later governed mig_174b apply.

## Live profile

| Column | Non-null rows | Distinct raw values | Rows with literal `null` token | Max tokens per row | Key grammar finding |
|---|---:|---:|---:|---:|---|
| `cnln_img_laterality` | 272 | 31 | 12 | 3 | Semicolon-delimited laterality labels plus sentinel `null` |
| `lateral_levels_v10` | 88 | 22 | 0 | 1 by semicolon split | Blank string sentinel; levels encoded via comma/range text, not semicolon |
| `ene_levels_v9` | 44 | 15 | 0 | 1 by semicolon split | Single labels plus central/paratracheal/IJ/supraclavicular synonyms |

### `cnln_img_laterality` value spread

| Raw value | Rows |
|---|---:|
| `bilateral` | 94 |
| `right` | 49 |
| `left` | 48 |
| `central` | 14 |
| `left; right` | 14 |
| `null` | 8 |
| `right; left` | 8 |
| `right; central` | 4 |
| `central; bilateral` | 3 |
| `left; central` | 3 |
| `right; bilateral` | 3 |
| `lateral` | 2 |
| `left; bilateral` | 2 |
| `left; right; bilateral` | 2 |
| `null; bilateral` | 2 |
| Other one-row combinations | 16 |

### `cnln_img_laterality` token spread

| Normalized token | Token count | Proposed canonical handling |
|---|---:|---|
| `bilateral` | 116 | `bilateral` |
| `right` | 87 | `right` |
| `left` | 85 | `left` |
| `central` | 32 | `central` |
| `null` | 12 | drop from semantic labels; preserve `has_null_token` and `parse_status` |
| `lateral` | 4 | `lateral_neck` |
| `lateral neck` | 3 | `lateral_neck` |

Token-count distribution: 215 one-token rows, 47 two-token rows, and 10 three-token rows.

### `lateral_levels_v10` value spread

| Raw value | Rows | Parser implication |
|---|---:|---|
| blank string | 21 | sentinel/empty, not a valid level |
| `level V` | 15 | level V |
| `II` | 10 | level II |
| `level II` | 5 | level II |
| `levels II` | 5 | level II |
| `level 3` | 4 | level III |
| `level IV` | 4 | level IV |
| `levels 2, 3, 4` | 4 | levels II, III, IV |
| `level 2` | 3 | level II |
| `level 2,3, 4, 5` | 2 | levels II, III, IV, V |
| `levels II-V` | 2 | levels II, III, IV, V |
| Other one-row values | 10 | same Arabic/Roman/range grammar |

Semicolon tokenization alone is insufficient for `lateral_levels_v10`. The mig_174b parser should normalize prefixes (`level`, `levels`), parse Arabic and Roman forms, split comma lists, and expand hyphen ranges.

### `ene_levels_v9` value spread

| Raw value | Rows | Parser implication |
|---|---:|---|
| `level 4` | 10 | level IV |
| `level 3` | 9 | level III |
| `level 6` | 6 | level VI / central compartment |
| `Level 3` | 3 | level III |
| `central` | 3 | `central` and/or level VI flag |
| `paratracheal` | 3 | central-compartment synonym |
| `Level 2` | 2 | level II |
| `IJ` | 1 | lateral neck, unclassified level unless clinical owner maps to II/III/IV |
| `LEVEL II` | 1 | level II |
| `SUPRACLAVICULAR` | 1 | level IV/V-adjacent; recommend `lateral_unspec` unless ratified otherwise |
| `level 2-5` | 1 | levels II, III, IV, V |
| `level 2A` | 1 | level IIa |
| `pretracheal` | 1 | central-compartment synonym |

## Proposed canonical token dictionaries

### Laterality / compartment labels

| Canonical token | Accepted tokens | Notes |
|---|---|---|
| `left` | `left`, `lt`, `l` | Anatomical side |
| `right` | `right`, `rt`, `r` | Anatomical side |
| `bilateral` | `bilateral`, `bilat`, `both` | Preserve even when `left`/`right` also present |
| `central` | `central`, `ctr`, `pretracheal`, `paratracheal`, `level 6`, `level vi`, `vi` | Central compartment representation |
| `lateral_neck` | `lateral`, `lateral neck`, `lateral_neck`, `lat neck`, `ij`, `internal jugular`, `supraclavicular` | `ij`/`supraclavicular` should be flagged as lateral-unspecified confidence, not exact level |

Rejected semantic tokens: literal `null`, `n/a`, `na`, `none`, `unspecified`, blank string, and `-`. These should not become analytic positives.

### Cervical level labels

| Canonical token | Accepted tokens / patterns |
|---|---|
| `level_i` | `i`, `1`, `level i`, `level 1` |
| `level_ii` | `ii`, `2`, `level ii`, `level 2`, generic level 2 without sublevel |
| `level_iia` | `iia`, `2a`, `level iia`, `level 2a` |
| `level_iib` | `iib`, `2b`, `level iib`, `level 2b` |
| `level_iii` | `iii`, `3`, `level iii`, `level 3` |
| `level_iv` | `iv`, `4`, `level iv`, `level 4` |
| `level_v` | `v`, `5`, `level v`, `level 5`, generic level 5 without sublevel |
| `level_va` | `va`, `5a`, `level va`, `level 5a` |
| `level_vb` | `vb`, `5b`, `level vb`, `level 5b` |
| `level_vi` | `vi`, `6`, `level vi`, `level 6`, `central`, `pretracheal`, `paratracheal` |
| `level_vii` | `vii`, `7`, `level vii`, `level 7` |
| `lateral_unspec` | `lateral`, `ij`, `internal jugular`, `supraclavicular` when no exact level can be inferred |

Range expansion rules for mig_174b:

- `II-V`, `2-5`, `2 - 4`, and equivalent forms should expand inclusively to the ordered canonical level sequence.
- `II-IV` expands to `level_ii`, `level_iii`, `level_iv` unless sublevels are explicitly present.
- Generic `level II` should not imply `level_iia` or `level_iib`; it should populate the generic `level_ii` flag only.
- `central`, `pretracheal`, and `paratracheal` should populate central compartment / `level_vi` but should preserve a source token note for QC.

## Logan decision package

| Question | Option A | Option B | Evidence from profile | Recommendation |
|---|---|---|---|---|
| Multi-label representation for `cnln_img_laterality` | Add one canonical list column (`VARCHAR[]` / DuckDB `LIST<VARCHAR>`) | Add 5 booleans: `cnln_lat_left`, `cnln_lat_right`, `cnln_lat_bilateral`, `cnln_lat_central`, `cnln_lat_lateral_neck` plus parse status | Only 5 semantic labels and 57/272 rows are multi-token; analysts will commonly filter by side/compartment | **B**. Boolean flags are easiest for analytic filters and avoid list-query friction. |
| Multi-label representation for `lateral_levels_v10` | Add one canonical list column | Add one boolean per level token plus `lateral_levels_parse_status` | Current strings require comma/range expansion; downstream analyses need `level_ii`/`level_iv`/`level_v` filters | **B**. Use booleans for exact filters, with a compact provenance/status column. |
| Multi-label representation for `ene_levels_v9` | Add one canonical list column | Add one boolean per level token plus `ene_levels_parse_status` | Only 44 rows currently, but grammar overlaps with lateral level parsing and will be used for ENE-level cohort filters | **B**, aligned to `lateral_levels_v10` for schema symmetry. |
| Literal `null` tokens | Drop silently | Preserve `cnln_img_laterality_has_null_token` and parse status | 12 token occurrences across 12 rows; 4 rows combine `null` with real labels | **B**. Drop from semantic labels but preserve QC flag. |
| Sentinel-only rows | NULL all canonical cols | Mark with parse status | `cnln_img_laterality='null'` in 8 rows; `lateral_levels_v10=''` in 21 rows | **B**. Use `sentinel_only` / `empty_sentinel` statuses. |
| Original VARCHAR fate | Drop | Rename to `_legacy_raw` | Existing governance pattern from mig_173 preserves raw values before dtype/parser reform | **B**. Rename originals to `_legacy_raw` during mig_174b. |

### Recommended mig_174b representation

Use boolean flags plus status/provenance, not arrays, for the PM apply:

#### `cnln_img_laterality`

- `cnln_lat_left BOOLEAN`
- `cnln_lat_right BOOLEAN`
- `cnln_lat_bilateral BOOLEAN`
- `cnln_lat_central BOOLEAN`
- `cnln_lat_lateral_neck BOOLEAN`
- `cnln_img_laterality_has_null_token BOOLEAN`
- `cnln_img_laterality_parse_status VARCHAR`
- Rename original `cnln_img_laterality` to `cnln_img_laterality_legacy_raw`

Suggested `cnln_img_laterality_parse_status` values: `parsed`, `parsed_with_null_token`, `sentinel_only`, `unparsed_token`, `not_applicable`.

#### `lateral_levels_v10`

- `lateral_level_i BOOLEAN`
- `lateral_level_ii BOOLEAN`
- `lateral_level_iia BOOLEAN`
- `lateral_level_iib BOOLEAN`
- `lateral_level_iii BOOLEAN`
- `lateral_level_iv BOOLEAN`
- `lateral_level_v BOOLEAN`
- `lateral_level_va BOOLEAN`
- `lateral_level_vb BOOLEAN`
- `lateral_level_vi BOOLEAN`
- `lateral_level_vii BOOLEAN`
- `lateral_level_lateral_unspec BOOLEAN`
- `lateral_levels_v10_parse_status VARCHAR`
- Rename original `lateral_levels_v10` to `lateral_levels_v10_legacy_raw`

Suggested status values: `parsed`, `parsed_range`, `parsed_list`, `empty_sentinel`, `unparsed_token`, `not_applicable`.

#### `ene_levels_v9`

- `ene_level_i BOOLEAN`
- `ene_level_ii BOOLEAN`
- `ene_level_iia BOOLEAN`
- `ene_level_iib BOOLEAN`
- `ene_level_iii BOOLEAN`
- `ene_level_iv BOOLEAN`
- `ene_level_v BOOLEAN`
- `ene_level_va BOOLEAN`
- `ene_level_vb BOOLEAN`
- `ene_level_vi BOOLEAN`
- `ene_level_vii BOOLEAN`
- `ene_level_lateral_unspec BOOLEAN`
- `ene_levels_v9_parse_status VARCHAR`
- Rename original `ene_levels_v9` to `ene_levels_v9_legacy_raw`

Suggested status values mirror `lateral_levels_v10`.

## Apply guardrails for mig_174b

1. Snapshot the three raw columns before any DDL.
2. Add new parser columns with `IF NOT EXISTS` and no destructive mutations outside the three scoped fields.
3. Parse from legacy raw values in a single transaction.
4. Rename original raw strings to `_legacy_raw` only after all parser columns are populated and verified.
5. Update `cpm_built_at` for touched rows and insert a `cpm_reconciliation_provenance_v1` row only in the governed apply lane.
6. Re-run CPM invariants: 10,871 rows, 10,871 distinct `research_id`, non-null `cpm_built_at`, and registry resync.
7. Post-apply gates must show: all known tokens parsed, sentinel tokens not counted as positives, multi-token rows preserving all positive labels, and zero unexpected source-column drops.

## Carry-forwards

| Carry-forward | Status in mig_174a | Closure path |
|---|---|---|
| `CF-mig168-VOCAB-DRIFT-CNLN-LATERALITY-MULTILABEL` | Open | Close after mig_174b parser apply and post-apply drift gate. |
| `CF-mig174a-DESIGN-RATIFICATION-PENDING` | Open | Logan ratifies representation before mig_174b. |
| `CF-mig174a-LITERAL-NULL-TOKEN-PRESENT` | Informational open | Quantified as 12 rows/tokens in `cnln_img_laterality`; close or keep as QC note after parser flags land. |
| `CF-mig174a-LATERAL-LEVELS-V10-MULTILABEL` | Informational open | Close after comma/range level parser lands. |
| `CF-mig174a-ENE-LEVELS-V9-MULTILABEL` | Informational open | Close after ENE level parser lands. |

## Conclusion

The evidence supports a boolean-flag representation for all three columns, with raw-string preservation via `_legacy_raw` and parse-status columns for sentinel/unparsed cases. `lateral_levels_v10` and `ene_levels_v9` should not be implemented as simple semicolon splitters; their actual grammar requires prefix stripping, comma splitting, Roman/Arabic normalization, synonym mapping, and range expansion.
