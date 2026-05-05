# Cursor prompt — mig_318: FNA NLP parse-layer fix

**Agent:** cursor_composer (Snowflake-capable)
**Estimated time:** 10–20 min (parse rebuild + re-mirror + re-build v4)
**Cost:** $0 (re-uses existing Cortex extraction; no new EXTRACT_ANSWER calls)
**Closes:** new `CF-FNA-SIZE-PARSE-LAYER` (opened post-mig_310 verification)

## Problem

mig_310 v2 commit `4fd6fc5` was signed off but the size extraction is functionally non-delivering:

| Metric | Target | Actual |
|---|---|---|
| `extracted_size_cm` populated in rollup | ≥60% (≥1,650 of 2,756) | **0.1% (3 rows)** |
| `extracted_bethesda` populated | ≥1,500 | **18 rows** |
| `imaging_fna_linkage_v4` `nlp_high`+`nlp_medium` resolved | ≥1,500 | **5 rows** |
| `extracted_laterality` populated (control) | ≥50% | 47.8% (works fine) |

### Root cause

Cortex EXTRACT_ANSWER ran successfully — 705 of 2,756 rows have `max_size_score > 0.85` (Cortex IS finding the answer). The defect is downstream in the SQL parser:

```sql
TRY_TO_DOUBLE(NULLIF(TRIM(_size_raw[0]:answer::VARCHAR), ''))
```

This fails the moment Cortex returns `"1.5 cm"`, `"1.5cm"`, `"1.5 centimeters"`, `"approximately 1.5"`, etc. — anything other than a bare numeric string. Same pattern for Bethesda: `TRY_TO_NUMBER(answer, 1, 0)` chokes on `"Category II"`, `"Bethesda VI"`, etc. Laterality works because `LIKE '%right%'` is tolerant of any string content.

**Critical:** the raw extraction results are persisted in `THYROID_VALIDATION.PUBLIC.NLP_FNA_SIZE_FULL_RESULTS_v1`. mig_318 only needs to **re-parse those existing answers** — no new Cortex calls.

## Recipe

### Step 1 — Inspect raw answer distribution

```sql
USE DATABASE THYROID_VALIDATION;
USE SCHEMA PUBLIC;

-- See what Cortex actually returned vs what TRY_TO_DOUBLE could parse
SELECT
  _size_raw[0]:answer::VARCHAR AS raw_size_answer,
  _size_raw[0]:score::FLOAT AS size_score,
  COUNT(*) AS n
FROM (
  SELECT
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
      NOTE_TEXT,
      'What is the size (largest dimension) of the aspirated thyroid nodule in centimeters? ...'
    ) AS _size_raw
  FROM FNA_NOTES_MIG310_V2
  -- WARNING: this re-runs Cortex; instead use the saved table NLP_FNA_SIZE_FULL_RESULTS_v1
  -- if it preserves the raw answer fields. If not, add them in mig_318a (see Step 2).
)
GROUP BY 1, 2
ORDER BY n DESC LIMIT 20;
```

**If `NLP_FNA_SIZE_FULL_RESULTS_v1` does NOT preserve the raw `_size_raw[0]:answer` and `_size_raw[0]:score` columns**, you'll need to re-run the extraction. **First check** by:

```sql
DESCRIBE TABLE NLP_FNA_SIZE_FULL_RESULTS_v1;
```

If `size_extract_score` and the answer text are stored, build the new parser against that. If not, see Step 2a.

### Step 2 — Replace size parser with regex extraction

```sql
CREATE OR REPLACE TABLE NLP_FNA_SIZE_FULL_RESULTS_v2 AS
SELECT
  RESEARCH_ID,
  FNA_EVENT_ID,
  NOTE_TYPE,
  NOTE_DATE,

  -- New parser: regex-extract first decimal number from answer
  CASE
    WHEN size_raw_answer IS NULL OR size_raw_answer = '' THEN NULL
    ELSE TRY_TO_DOUBLE(REGEXP_SUBSTR(size_raw_answer, '[0-9]+(\\\\.[0-9]+)?'))
  END AS size_parsed_raw,

  -- Apply unit detection: if answer contains "mm", divide by 10
  CASE
    WHEN LOWER(size_raw_answer) LIKE '%mm%'
         AND LOWER(size_raw_answer) NOT LIKE '%cm%'
         AND TRY_TO_DOUBLE(REGEXP_SUBSTR(size_raw_answer, '[0-9]+(\\\\.[0-9]+)?')) IS NOT NULL
    THEN TRY_TO_DOUBLE(REGEXP_SUBSTR(size_raw_answer, '[0-9]+(\\\\.[0-9]+)?')) / 10.0
    ELSE TRY_TO_DOUBLE(REGEXP_SUBSTR(size_raw_answer, '[0-9]+(\\\\.[0-9]+)?'))
  END AS size_parsed_cm,

  -- Clamp to plausible range
  CASE
    WHEN extracted_size_cm BETWEEN 0.1 AND 15.0 THEN extracted_size_cm
    ELSE NULL
  END AS extracted_size_cm,

  -- Laterality unchanged
  extracted_laterality,
  extracted_nodule_count,

  -- Bethesda: Roman numeral + "Category" prose handling
  CASE
    WHEN bethesda_raw_answer IS NULL OR bethesda_raw_answer = '' THEN NULL
    WHEN REGEXP_LIKE(LOWER(bethesda_raw_answer), '\\\\bvi\\\\b|category vi|bethesda vi|class vi') THEN 6
    WHEN REGEXP_LIKE(LOWER(bethesda_raw_answer), '\\\\bv\\\\b|category v|bethesda v|class v') THEN 5
    WHEN REGEXP_LIKE(LOWER(bethesda_raw_answer), '\\\\biv\\\\b|category iv|bethesda iv|class iv') THEN 4
    WHEN REGEXP_LIKE(LOWER(bethesda_raw_answer), '\\\\biii\\\\b|category iii|bethesda iii|class iii') THEN 3
    WHEN REGEXP_LIKE(LOWER(bethesda_raw_answer), '\\\\bii\\\\b|category ii|bethesda ii|class ii') THEN 2
    WHEN REGEXP_LIKE(LOWER(bethesda_raw_answer), '\\\\bi\\\\b|category i|bethesda i|class i') THEN 1
    ELSE TRY_TO_NUMBER(REGEXP_SUBSTR(bethesda_raw_answer, '[1-6]'))
  END AS extracted_bethesda,

  -- Confidence + scores: unchanged
  extraction_confidence,
  size_extract_score,
  lat_extract_score,
  count_extract_score,
  bethesda_extract_score,

  extracted_at,
  'cortex_extract_answer_mig_310_v2_reparsed_mig_318' AS extraction_source

FROM NLP_FNA_SIZE_FULL_RESULTS_v1
-- Adapt column names: cursor-mig_318 writer should DESCRIBE the source table first
;
```

**Important:** the SQL above assumes `NLP_FNA_SIZE_FULL_RESULTS_v1` preserves the raw answer strings as `size_raw_answer`, `bethesda_raw_answer`. If those names differ (or the raw answers were dropped), see Step 2a.

### Step 2a (fallback) — Re-extract from FNA_NOTES_MIG310_V2

If raw answers are lost, re-run EXTRACT_ANSWER and keep the raw answer strings this time:

```sql
CREATE OR REPLACE TABLE NLP_FNA_SIZE_FULL_RESULTS_v2 AS
WITH extracted AS (
  SELECT
    RESEARCH_ID,
    FNA_EVENT_ID,
    NOTE_TYPE,
    NOTE_DATE,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(NOTE_TEXT, '<size prompt>') AS _size_raw,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(NOTE_TEXT, '<lat prompt>')  AS _lat_raw,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(NOTE_TEXT, '<count prompt>') AS _count_raw,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(NOTE_TEXT, '<bethesda prompt>') AS _bethesda_raw
  FROM FNA_NOTES_MIG310_V2
)
SELECT
  RESEARCH_ID, FNA_EVENT_ID, NOTE_TYPE, NOTE_DATE,
  _size_raw[0]:answer::VARCHAR AS size_raw_answer,
  _size_raw[0]:score::FLOAT AS size_score,
  _bethesda_raw[0]:answer::VARCHAR AS bethesda_raw_answer,
  -- ... and so on; KEEP THE RAW ANSWERS
  TRY_TO_DOUBLE(REGEXP_SUBSTR(_size_raw[0]:answer::VARCHAR, '[0-9]+(\\\\.[0-9]+)?')) AS extracted_size_cm,
  -- ... new parser
FROM extracted;
```

(Cost: ~$5 for the full re-run, only if Step 2 isn't viable.)

### Step 3 — Rebuild rollup + mirror

```sql
CREATE OR REPLACE TABLE NLP_FNA_SIZE_PATIENT_ROLLUP_v1 AS
SELECT ...  -- same QUALIFY ROW_NUMBER + FIRST_VALUE windowing as v1, but using v2 source
FROM NLP_FNA_SIZE_FULL_RESULTS_v2;
```

Then re-run mirror script:

```bash
.venv/bin/python scripts/mig_310_fna_size_mirror.py --md  # without --signoff this time
```

### Step 4 — Validation gates (acceptance)

```sql
SELECT
  COUNT(*) AS n_rollup,
  COUNT(extracted_size_cm) AS n_size,
  ROUND(100.0 * COUNT(extracted_size_cm) / COUNT(*), 1) AS pct_size,
  COUNT(extracted_bethesda) AS n_beth,
  ROUND(100.0 * COUNT(extracted_bethesda) / COUNT(*), 1) AS pct_beth,
  AVG(extracted_size_cm) AS avg_size,
  STDDEV(extracted_size_cm) AS sd_size
FROM manuscript_workspace.nlp_fna_size_rollup_v1;
```

Acceptance:
- `pct_size ≥ 60%` (was 0.1%)
- `pct_beth ≥ 50%` (was 0.7%)
- `avg_size` between 1.0 and 4.0 cm (clinically expected median)
- `sd_size` between 0.5 and 3.0 cm

```sql
-- v4 source distribution
SELECT fna_size_source_v4, COUNT(*) AS n
FROM manuscript_workspace.imaging_fna_linkage_v4
GROUP BY 1 ORDER BY 2 DESC;
```

Acceptance: `nlp_high` + `nlp_medium` should sum to ≥1,500 (was 5).

### Step 5 — Update mig_310 signoff with addendum

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT 'mig_318', CURRENT_TIMESTAMP, 'cursor_composer_mig318',
  'mig_318: FNA NLP parse-layer fix. mig_310 size extraction failed substantively (0.1% size fill, 0.7% beth fill) due to TRY_TO_DOUBLE choking on "1.5 cm" answers. Fix: regex extraction + Roman-numeral parser. Pre-fix: <X> rows with valid size; post-fix: <Y> rows (<Z%>). Bethesda pre/post: <A>/<B>. v4 nlp_high+medium: <pre>→<post>. Closes CF-FNA-SIZE-PARSE-LAYER. M025 nodule cohort rebuild deferred to Cowork decision based on size coverage delta.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id='mig_318');
```

## Out of scope

- Do NOT re-run the M025 nodule analytic rebuild — that's Cowork's decision after seeing the new coverage numbers.
- Do NOT modify the M025 v2 submission package — Logan's other chat owns that.
- Do NOT add new EXTRACT_ANSWER prompts — the four-field schema is locked. The fix is purely in the parse layer.
- Do NOT modify `imaging_fna_linkage_v3` — only v4 changes.

## When done, ping Cowork

One-line message: `mig_318 complete; size_fill XX%, beth_fill YY%, v4 nlp_high+medium NN`. Cowork will then run the M025 nodule semantic-model smoke test and decide whether to rebuild the M025 nodule analytic master.
