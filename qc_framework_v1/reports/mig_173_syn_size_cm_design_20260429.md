# Migration 173 — `syn_*_size_cm` 3-axis dtype reform

**Batch:** `mig_173_syn_size_cm_dtype_reform_20260429`
**Lane:** 62
**Date:** 2026-04-29
**Artifact:** `qc_framework_v1/migrations/173_syn_size_cm_dtype_reform_20260429.sql`
**Posture:** Path-C schema reform artifact; production apply is section-gated.

## 1. Scope

This lane reforms three `main.canonical_patient_master` synoptic thyroid-size columns that are currently VARCHAR strings containing 3-axis measurements:

| Legacy column | Observed issue | New representation |
|---|---|---|
| `syn_right_lobe_size_cm` | VARCHAR with units / 3-axis text | `syn_right_lobe_length_cm`, `syn_right_lobe_width_cm`, `syn_right_lobe_height_cm`, `syn_right_lobe_volume_cc`, `syn_right_lobe_size_parse_status` |
| `syn_left_lobe_size_cm` | VARCHAR with units / 3-axis text | `syn_left_lobe_length_cm`, `syn_left_lobe_width_cm`, `syn_left_lobe_height_cm`, `syn_left_lobe_volume_cc`, `syn_left_lobe_size_parse_status` |
| `syn_isthmus_size_cm` | VARCHAR with units / 3-axis text | `syn_isthmus_length_cm`, `syn_isthmus_width_cm`, `syn_isthmus_height_cm`, `syn_isthmus_volume_cc`, `syn_isthmus_size_parse_status` |

The raw VARCHAR columns are preserved by one-time rename to:

- `syn_right_lobe_size_cm_legacy_raw`
- `syn_left_lobe_size_cm_legacy_raw`
- `syn_isthmus_size_cm_legacy_raw`

## 2. Inputs from the lane prompt

Cowork live probe counts from the mig_173 prompt:

| Column | Distinct values | Non-null rows | Sentinel `n/s` rows |
|---|---:|---:|---:|
| `syn_right_lobe_size_cm` | 6,599 | 7,058 | 39 |
| `syn_left_lobe_size_cm` | 6,715 | 7,204 | 33 |
| `syn_isthmus_size_cm` | 3,500 | 3,981 | 2 |

mig_169 classified all three fields as high-priority `VARCHAR-with-units`; mig_168 also flagged the family as vocabulary drift because numeric measurements were stored as text with units and axis delimiters.

## 3. Parser design

The SQL migration uses a single unpivoted parser CTE so the same rule set is applied to right lobe, left lobe, and isthmus values.

### 3.1 Normalization

For each raw value:

1. `CAST(raw_value AS VARCHAR)`
2. `TRIM()`
3. collapse whitespace using `REGEXP_REPLACE(..., '\s+', ' ', 'g')`
4. `LOWER()`

DuckDB-specific guard: every `REGEXP_EXTRACT()` capture is wrapped with `NULLIF(..., '')` before `TRY_CAST`, because DuckDB returns an empty string rather than `NULL` when a capture does not match.

### 3.2 Sentinel handling

The following normalized values map to numeric `NULL` and parse status `sentinel`:

```text
n/s, ns, none, null, empty string, x, c/a, -
```

Raw SQL `NULL` values retain `NULL` parse status rather than becoming `sentinel`.

### 3.3 Regex cascade

The parser tries these patterns in order:

1. **Clean 3-axis delimiter pattern:** `A x B x C`, allowing `x`, `×`, optional `cm`, optional trailing `)` / punctuation.
2. **Verbose by-pattern:** `A cm ... by B cm ... by C cm ...`.
3. **Narrative three-cm pattern:** first three `cm`-valued numbers in a string.
4. **Two-axis partial:** `A x B` using `x` or `×` delimiters.
5. **One-axis partial:** standalone `A cm` or `A`.

The conservative status hierarchy is:

| Status | Meaning |
|---|---|
| `parsed_3axis` | all three axes parsed as DOUBLE |
| `parsed_partial` | one or two axes parsed; not enough for volume |
| `sentinel` | sentinel raw value, numeric fields intentionally NULL |
| `unparsed` | non-sentinel non-null raw value did not match the parser cascade |

## 4. Volume formula

`*_volume_cc` is calculated only when all three axes are present:

$$
\text{volume\_cc} = \text{length\_cm} \times \text{width\_cm} \times \text{height\_cm}
$$

No ellipsoid correction factor is applied in this migration. This is intentionally documented as `CF-mig173-VOLUME-CALC-NO-ELLIPSOID-FACTOR` so a future clinical decision can add separate ellipsoid-corrected columns if needed without changing the meaning of the rectangular `*_volume_cc` fields.

## 5. Migration sections

The SQL artifact is intentionally sectioned so Cowork can apply and verify each step independently:

| Section | Action | Writes |
|---|---|---|
| A | Pre-snapshot legacy raw columns | 3 archive tables in `"Thyroid 2026 UPdated".archive_pub_v1_0` |
| B | Add typed axis/volume/status columns | 15 `ALTER TABLE ADD COLUMN IF NOT EXISTS` statements |
| C | Populate parser outputs | 1 deterministic `UPDATE ... FROM` using one row per `research_id` |
| D | Rename legacy raw columns | 3 `ALTER TABLE RENAME COLUMN` statements |
| E | Registry and provenance resync | column registry, table registry, CPM provenance row |
| F | Verification probes | commented read-only SQL |

Section D is intentionally one-time. If the migration is rerun after the legacy rename, skip Section D.

## 6. Registry and carry-forward disposition

### 6.1 Closed by this lane after successful apply and verification

- `CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_right_lobe_size_cm`
- `CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_left_lobe_size_cm`
- `CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_isthmus_size_cm`
- `CF-mig168-VOCAB-DRIFT-SYN-SIZE-3AXIS-VARCHAR`

### 6.2 Kept informational

- `CF-mig173-PARSE-COVERAGE-LT-100PCT-PER-COL` — tracks residual `unparsed` rows.
- `CF-mig173-VOLUME-CALC-NO-ELLIPSOID-FACTOR` — documents rectangular, not ellipsoid, volume.

### 6.3 Registry behavior

The SQL artifact:

1. renames existing registry rows for the three raw VARCHAR fields to the `_legacy_raw` names;
2. marks those raw fields as `na` with method `legacy_raw_preserved_after_typed_decomposition`;
3. inserts 15 new derived-column registry rows as `not_started` pending post-parse verification;
4. resyncs `main.canonical_table_signoff_registry_v1` for `canonical_patient_master`.

## 7. Required post-apply checks

Minimum acceptance checks after Sections A-E:

1. CPM invariant: 10,871 rows and 10,871 distinct `research_id` values.
2. `cpm_built_at` non-null for all CPM rows.
3. 15 new columns present with expected data types.
4. 3 `_legacy_raw` columns present and the three original VARCHAR names absent.
5. Parse-status distribution generated for all three source fields.
6. `parsed_3axis` coverage target: at least 85% of non-sentinel, non-null rows.
7. Volume sanity: no negative values and no values at or above 1000 cc without explicit clinical review.
8. Registry rows present for all 18 affected fields.
9. CPM reconciliation provenance row inserted.

## 8. Out of scope

This migration does **not** touch:

- `syn_margin_distance_mm_raw_str` or any non-size synoptic text fields;
- `ops_max_diameter_cm`, `ops_preop_nodules_count_size`, or `ops_dominant_nodule_size_us`;
- ellipsoid-corrected thyroid volume;
- manual review or LLM rescue for residual `unparsed` values;
- direct production apply outside the section-gated Path-C workflow.

## 9. Apply sequence

Recommended Cowork application order:

1. Run Section A snapshots; verify archive row counts.
2. Run Section B `ADD COLUMN`; verify 15 new columns.
3. Run Section C parser update; verify parse-status distributions.
4. Run Section D legacy renames; verify old/new column presence.
5. Run Section E registry/provenance resync.
6. Run Section F verification probes and capture outputs in the migration log.

## 10. Safety notes

- The parser CTE emits exactly one row per `(research_id, side_key)` and the update pivots to exactly one row per `research_id`, avoiding nondeterministic `UPDATE ... FROM` multi-match behavior.
- Legacy raw strings are never destroyed.
- `cpm_built_at` is refreshed during parser population to preserve CPM build-provenance convention.
- Residual `unparsed` rows are treated as an expected audit queue, not silently coerced.