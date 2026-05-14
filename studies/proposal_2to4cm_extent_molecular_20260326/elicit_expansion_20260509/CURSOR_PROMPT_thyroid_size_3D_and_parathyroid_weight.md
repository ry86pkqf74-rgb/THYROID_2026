# Cursor handoff: parse pathology-side thyroid 3D dimensions + parathyroid weight

**Recommendation: do this in Cursor.** Read-only audit complete (Cowork 2026-05-13). Two small additive structured-extraction fixes; no canonical-schema redesign, no big migration.

Builds on the audit at `studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/WEIGHT_SIZE_AUDIT_20260513.md`.

## Goal

1. Parse the 3 dimensions from `pub_canonical.thyroid_sizes.rl_formatted` / `ll_formatted` / `total_formatted` into separate numeric columns. 99% of strings match `# × # × # cm` — straightforward regex.
2. Extract parathyroid weight (mg) from LLM evidence text on `pub_canonical.canonical_parathyroid_events_v1`. ~150 hits globally, ~72 in the surgical cohort. Add a numeric `parathyroid_weight_mg` column.

Both fixes are additive: new columns, no modifications to existing values, no row count changes.

## Hard rules
1. No PHI in Airtable / Linear / committed code (regex patterns and counts only — no specific evidence_quote strings).
2. Append-only. Snapshot both target tables to `pub_archive.thyroid_sizes_pre_3d_parse_<YYYYMMDD>` and `pub_archive.canonical_parathyroid_events_v1_pre_weight_extract_<YYYYMMDD>` before MERGE.
3. Log DFL row BEFORE the canonical edit (`appJYOnUb7KrHKwpV.tblsiYKJtKcktkzze`, `target_type=column`, `change_type=schema_extension`).
4. Append MFL row `MFL-<YYYYMMDD>-EXT2-4-WEIGHT-SIZE-EXTENSION` linked to EXT2-4 (`rec1GJyrmKdKxjlaY`). Also note this benefits M084 (parathyroid manuscript) — link there too if a record exists.
5. Skill version bump v2.3.0 → **v2.3.1 (patch)** with CHANGELOG entry after acceptance.

## Phase 1 — Parse thyroid_sizes 3D

### Add columns
```sql
ALTER TABLE `pub_canonical.thyroid_sizes`
  ADD COLUMN IF NOT EXISTS rl_length_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS rl_width_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS rl_depth_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS rl_largest_dim_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS ll_length_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS ll_width_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS ll_depth_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS ll_largest_dim_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS total_length_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS total_width_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS total_depth_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS isthmus_length_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS isthmus_width_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS isthmus_depth_cm_path FLOAT64,
  ADD COLUMN IF NOT EXISTS dim_parse_status STRING,
  ADD COLUMN IF NOT EXISTS dim_parse_at TIMESTAMP;
```

### Regex extraction
Pure-SQL approach using `REGEXP_EXTRACT_ALL`:
```sql
UPDATE `pub_canonical.thyroid_sizes` SET
  rl_length_cm_path = SAFE_CAST(REGEXP_EXTRACT(rl_formatted, r'(\d+(?:\.\d+)?)\s*[x×]\s*\d+(?:\.\d+)?\s*[x×]\s*\d+(?:\.\d+)?\s*cm') AS FLOAT64),
  rl_width_cm_path  = SAFE_CAST(REGEXP_EXTRACT(rl_formatted, r'\d+(?:\.\d+)?\s*[x×]\s*(\d+(?:\.\d+)?)\s*[x×]\s*\d+(?:\.\d+)?\s*cm') AS FLOAT64),
  rl_depth_cm_path  = SAFE_CAST(REGEXP_EXTRACT(rl_formatted, r'\d+(?:\.\d+)?\s*[x×]\s*\d+(?:\.\d+)?\s*[x×]\s*(\d+(?:\.\d+)?)\s*cm') AS FLOAT64),
  dim_parse_status = CASE
    WHEN REGEXP_CONTAINS(rl_formatted, r'\d+(?:\.\d+)?\s*[x×]\s*\d+(?:\.\d+)?\s*[x×]\s*\d+(?:\.\d+)?\s*cm') THEN '3d_parsed'
    WHEN REGEXP_CONTAINS(rl_formatted, r'\d+(?:\.\d+)?\s*[x×]\s*\d+(?:\.\d+)?\s*cm') THEN '2d_only'
    WHEN REGEXP_CONTAINS(rl_formatted, r'\d+(?:\.\d+)?\s*cm') THEN '1d_only'
    WHEN rl_formatted IS NULL OR LENGTH(rl_formatted) = 0 THEN 'empty'
    ELSE 'unparseable'
  END,
  dim_parse_at = CURRENT_TIMESTAMP()
WHERE TRUE;
```

Repeat for `ll_formatted`, `total_formatted`, `isthmus_formatted`.

Then compute the largest-dim helper:
```sql
UPDATE `pub_canonical.thyroid_sizes` SET
  rl_largest_dim_cm_path = GREATEST(IFNULL(rl_length_cm_path,0), IFNULL(rl_width_cm_path,0), IFNULL(rl_depth_cm_path,0)),
  ll_largest_dim_cm_path = GREATEST(IFNULL(ll_length_cm_path,0), IFNULL(ll_width_cm_path,0), IFNULL(ll_depth_cm_path,0))
WHERE rl_length_cm_path IS NOT NULL OR ll_length_cm_path IS NOT NULL;
```

### Acceptance for Phase 1
- `COUNTIF(rl_length_cm_path IS NOT NULL) / COUNTIF(rl_formatted IS NOT NULL AND LENGTH(rl_formatted)>0)` ≥ 0.97 (~4,550+ of 4,690)
- Spot-check 20 random `dim_parse_status='unparseable'` rows manually to confirm they're truly unparseable (OCR noise, free-text variants)
- Spot-check 20 random `3d_parsed` rows to confirm the 3 numbers are extracted in the right slots (length should be the largest of the three in conventional reporting; if depth > length, the radiologist/pathologist convention may have been inverted — flag for review but don't block)

## Phase 2 — Extract parathyroid weight

### Add column
```sql
ALTER TABLE `pub_canonical.canonical_parathyroid_events_v1`
  ADD COLUMN IF NOT EXISTS parathyroid_weight_mg FLOAT64,
  ADD COLUMN IF NOT EXISTS parathyroid_weight_source STRING,
  ADD COLUMN IF NOT EXISTS parathyroid_weight_extracted_at TIMESTAMP;
```

### Regex extraction
The text patterns are mixed (mg, gm, g, gram, grams). Normalize all to mg.

```python
# Pseudocode for the extraction (run in Cursor with python-bq client)
import re

# Match "weight 320 mg", "320 mg", "wt: 0.4 g", "weighed 280mg", etc.
WEIGHT_RX = re.compile(
    r'(?:weight[s]?|wt|weighed|weighing)?\s*(?:of|=|:)?\s*'
    r'(\d+(?:\.\d+)?)\s*'
    r'(mg|gm|gram[s]?|\bg\b)',
    re.IGNORECASE
)

UNIT_MULTIPLIER = {'mg': 1.0, 'gm': 1000.0, 'gram': 1000.0, 'grams': 1000.0, 'g': 1000.0}

def extract_weight_mg(text: str) -> float | None:
    if not text:
        return None
    # Take the FIRST match that has the "weight" / "wt" keyword nearby (within 30 chars to the left)
    for m in WEIGHT_RX.finditer(text):
        start = m.start()
        context = text[max(0, start-30):start].lower()
        if any(k in context for k in ('weight', 'wt', 'weighed', 'weighing')):
            val = float(m.group(1))
            unit = m.group(2).lower().rstrip('s')
            return val * UNIT_MULTIPLIER[unit]
    return None
```

### Source-text concatenation
For each row, concatenate `evidence_quote || ' ' || reasoning || ' ' || parathyroid_pathology` and run the regex.

### Acceptance for Phase 2
- Expected yield in the global table: 100–200 rows populated (audit found 374 with the keyword in `evidence_quote` alone, but many are false positives like "weighs heavily on differential" — the keyword-proximity filter reduces FPs)
- Spot-check 20 populated rows manually to confirm the extracted value is the parathyroid weight (not a related lab, not a body weight, not a thyroid weight if the note is mixed)
- `parathyroid_weight_mg` populated for **at least 50 rows total**, **at least 30 of which are in the surgical cohort**
- Set `parathyroid_weight_source = 'llm_evidence_regex_v1'`

### Output a manual-review CSV
```sql
EXPORT DATA OPTIONS(uri='gs://...', format='CSV') AS
SELECT research_id, parathyroid_event_id, parathyroid_weight_mg, evidence_quote, reasoning
FROM `pub_canonical.canonical_parathyroid_events_v1`
WHERE parathyroid_weight_mg IS NOT NULL;
```
(Or use bq export. Keep this CSV out of git — PHI risk if research_ids are paired with evidence text on a chart-reviewer's laptop. Store on internal share only.)

## Phase 3 — Verification

```sql
-- Phase 1 verification
SELECT
  COUNTIF(rl_formatted IS NOT NULL AND LENGTH(rl_formatted)>0) AS n_rl_formatted,
  COUNTIF(rl_length_cm_path IS NOT NULL) AS n_rl_length_parsed,
  ROUND(SAFE_DIVIDE(COUNTIF(rl_length_cm_path IS NOT NULL),
                    COUNTIF(rl_formatted IS NOT NULL AND LENGTH(rl_formatted)>0)), 3) AS frac_rl_parsed,
  COUNT(DISTINCT dim_parse_status) AS n_parse_statuses,
  STRING_AGG(DISTINCT dim_parse_status ORDER BY dim_parse_status) AS parse_statuses
FROM `pub_canonical.thyroid_sizes`;

-- Phase 2 verification
SELECT
  COUNT(*) AS n_rows_total,
  COUNTIF(parathyroid_weight_mg IS NOT NULL) AS n_with_weight,
  ROUND(AVG(parathyroid_weight_mg), 1) AS mean_weight_mg,
  APPROX_QUANTILES(parathyroid_weight_mg, 100)[OFFSET(50)] AS median_weight_mg,
  MIN(parathyroid_weight_mg) AS min_weight_mg,
  MAX(parathyroid_weight_mg) AS max_weight_mg
FROM `pub_canonical.canonical_parathyroid_events_v1`;
```

Sanity checks on the parathyroid weight distribution:
- Median should be 200–800 mg (normal parathyroid 30–60 mg; adenomas 100–10000+ mg)
- Min should be ≥ 20 mg (anything smaller is likely a unit-conversion error)
- Max should be ≤ 50,000 mg (anything larger is probably a thyroid weight mis-attributed)
- If the distribution looks off, the regex caught the wrong field for some rows — flag and review

## Phase 4 — Airtable + skill bump

- DFL row appended pre-edit.
- MFL row `MFL-<YYYYMMDD>-EXT2-4-WEIGHT-SIZE-EXTENSION` post-edit with verification numbers.
- Skill bump v2.3.0 → v2.3.1 with CHANGELOG entry covering both columns added.
- No new Verification Check needed (these are additive extensions, not corrections to existing data).

## Manuscript impact (for context)

- **EXT2-4 v3**: no rebuild needed; Table 1 doesn't use per-lobe weight or pathology 3D.
- **M084 (parathyroid)**: directly benefits — adds a previously-unavailable parathyroid weight covariate.
- **Future substernal goiter analyses**: directly benefits — uses pathology-side 3D depth for substernal definition.

## Acceptance criteria

- [ ] `thyroid_sizes.rl_length_cm_path` populated on ≥ 97% of rows with non-empty `rl_formatted`
- [ ] Same threshold for `ll_length_cm_path` and `total_length_cm_path`
- [ ] `dim_parse_status` populated on 100% of rows
- [ ] `canonical_parathyroid_events_v1.parathyroid_weight_mg` populated on ≥ 50 rows globally, ≥ 30 in surgical cohort
- [ ] Median parathyroid weight in 200–800 mg range
- [ ] Snapshots present at `pub_archive.thyroid_sizes_pre_3d_parse_<YYYYMMDD>` and `pub_archive.canonical_parathyroid_events_v1_pre_weight_extract_<YYYYMMDD>`
- [ ] DFL, MFL rows logged
- [ ] Skill at v2.3.1 with CHANGELOG entry
- [ ] EXT2-4 lifecycle still Active

When done, hand back to Cowork only if M084 or a substernal-goiter manuscript needs an immediate refresh; otherwise the new columns sit available for future use.
