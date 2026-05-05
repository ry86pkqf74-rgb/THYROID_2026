# Cursor Prompt — mig_310: FNA NLP size extraction (Cortex AI_EXTRACT)

**Agent:** Cursor Composer (Snowflake-side execution; MotherDuck-side mirror)
**Estimated time:** 4–6 hours (corpus-dependent)
**Date:** 2026-05-05
**Closes:** CF-FNA-SIZE-CM-NULL

## Problem statement

`canonical_us_nodule_v2` ↔ `canonical_fna_events_v1` linkage v1.0 leaves **per-nodule FNA size_cm flat NULL** by design — `size_score` for the `imaging_fna_linkage_v3` bridge is a constant 0.5 prior. Three downstream impacts:

1. **M025 v2 nodule-level Methods** (per-nodule TR4 18.7%, TR5 26.1%) cannot incorporate FNA size as a covariate; size discrimination AUC is bounded.
2. **M046 (NIFTP-era Bethesda)** and **M053 (nondiagnostic FNA)** lose a major confounder.
3. Imaging↔cytology size concordance is unmeasurable, blocking any future adequacy-vs-size paper.

## Proven pattern to mirror

mig_298 built `cohort_m004_autoimmune_cancer_v1` (Option 2 NLP-augmented) by:

1. Defining the SF table `NLP_HASHIMOTO_FULL_RESULTS_v1` / `NLP_GRAVES_FULL_RESULTS_v1` via `SNOWFLAKE.CORTEX.AI_CLASSIFY` over `THYROID_NOTES_SEARCH` corpus.
2. Materializing as a SF table.
3. Mirroring to MotherDuck via `35_pull_sf_validation_log.py` → `manuscript_workspace.m004_nlp_autoimmune_rollup_v1`.
4. Joining into the cohort view.

**This prompt applies the same recipe to FNA size_cm and laterality.**

## Step 1 — Define the FNA report corpus in Snowflake

Identify the source table. Likely candidates (probe one-by-one):
```sql
SHOW TABLES IN SCHEMA THYROID_VALIDATION.PUBLIC LIKE '%FNA%';
SHOW TABLES IN SCHEMA THYROID_VALIDATION.PUBLIC LIKE '%CYTOLOGY%';
SHOW TABLES IN SCHEMA THYROID_VALIDATION.PUBLIC LIKE '%NOTES%';
```

Target columns:
- `RESEARCH_ID` (VARCHAR)
- `NOTE_DATE` or `FNA_DATE` (DATE)
- `NOTE_TEXT` or `REPORT_TEXT` (VARCHAR/TEXT) — full FNA cytology report

Filter to FNA-cytology document types only (e.g., `note_type = 'FNA_CYTOLOGY'` or via Cortex Search semantic filter).

## Step 2 — Build extraction with AI_EXTRACT

```sql
CREATE OR REPLACE TABLE NLP_FNA_SIZE_FULL_RESULTS_v1 AS
WITH base AS (
  SELECT
    RESEARCH_ID,
    FNA_DATE,
    NOTE_TEXT
  FROM <source_table>
  WHERE <fna_filter>
)
SELECT
  RESEARCH_ID,
  FNA_DATE,
  -- AI_EXTRACT returns OBJECT; pull the keys we need
  result:size_cm::FLOAT          AS extracted_size_cm,
  result:laterality::VARCHAR      AS extracted_laterality,
  result:nodule_count::INTEGER    AS extracted_nodule_count,
  result:confidence::VARCHAR      AS extraction_confidence,
  CURRENT_TIMESTAMP               AS extracted_at,
  'cortex_ai_extract_mig_310'     AS extraction_source
FROM (
  SELECT
    RESEARCH_ID, FNA_DATE,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
      NOTE_TEXT,
      'Extract from this FNA cytology report:
       - size_cm: numeric size of the aspirated nodule in cm (largest dimension if multiple given). NULL if not specified.
       - laterality: ''right'', ''left'', ''isthmus'', or ''bilateral''. NULL if not specified.
       - nodule_count: integer count of distinct nodules sampled in this FNA. Default 1.
       - confidence: ''high'' if size and side both unambiguous; ''medium'' if one is inferred; ''low'' if either uses hedging language.

       Return as JSON object with these four keys.'
    ) AS result
  FROM base
);
```

**Validation probe:** sample 200 rows manually, verify size_cm extraction precision >85%, laterality precision >95%.

## Step 3 — Mirror to MotherDuck

Adapt `scripts/35_pull_sf_validation_log.py` pattern:

```python
# scripts/36_pull_sf_nlp_fna_size.py
import duckdb, snowflake.connector, os
sf = snowflake.connector.connect(connection_name='thyroid_2026')
df = sf.cursor().execute('SELECT * FROM NLP_FNA_SIZE_FULL_RESULTS_v1').fetch_pandas_all()
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}")
md.execute("CREATE OR REPLACE TABLE manuscript_workspace.nlp_fna_size_rollup_v1 AS SELECT * FROM df")
```

## Step 4 — Bridge into canonical_fna_events_v1

Update `imaging_fna_linkage_v3` (or create v4):

```sql
-- MotherDuck side
CREATE OR REPLACE VIEW manuscript_workspace.imaging_fna_linkage_v4 AS
SELECT
  l.*,
  COALESCE(l.fna_size_cm, n.extracted_size_cm) AS fna_size_cm_resolved,
  COALESCE(l.fna_laterality, n.extracted_laterality) AS fna_laterality_resolved,
  CASE
    WHEN l.fna_size_cm IS NOT NULL THEN 1.0           -- exact match
    WHEN n.extracted_size_cm IS NOT NULL
         AND n.extraction_confidence IN ('high','medium') THEN 0.85
    WHEN n.extracted_size_cm IS NOT NULL THEN 0.5     -- low confidence
    ELSE 0.5                                            -- prior, unchanged
  END AS size_score_v4
FROM manuscript_workspace.imaging_fna_linkage_v3 l
LEFT JOIN manuscript_workspace.nlp_fna_size_rollup_v1 n
  ON l.research_id = n.research_id
 AND ABS(DATEDIFF('day', l.fna_date_resolved, n.fna_date)) <= 14;
```

## Step 5 — Rebuild M025 nodule-level cohort view

After v4 linkage exists:
```sql
-- Re-run mig_306 build with v4 join in place
-- See qc_framework_v1/migrations/306_nodule_level_spine_20260504.sql
```

Expected: per-nodule FNA size_cm column now populated for ~70% of FNA-linked nodules. AUC re-computation may shift ±0.01.

## Step 6 — Sister-paper unblocks

After mig_310 lands, queue:
- M046 (NIFTP-era Bethesda): rerun stratification by FNA size cohort
- M053 (nondiagnostic FNA): adequacy by aspirated-nodule-size analysis becomes feasible
- M025 v2.1 supplement: add FNA-size covariate sensitivity arm to ROC

## Signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_310', CURRENT_TIMESTAMP, 'cursor_composer_mig310',
  'mig_310: FNA NLP size extraction. SF NLP_FNA_SIZE_FULL_RESULTS_v1 built via Cortex AI_EXTRACT over <N> FNA cytology reports. Sample-200 manual QA: size_cm precision <X>%, laterality precision <Y>%. Mirrored to manuscript_workspace.nlp_fna_size_rollup_v1 (<rows> rows). imaging_fna_linkage_v4 view created with size_score_v4 weighted prior. M025 nodule-level rebuild: AUC <new> vs <old> 0.6399. Closes CF-FNA-SIZE-CM-NULL.');
```

## Out-of-scope

- Tumor (path) size extraction — separate effort, NLP path size column is already populated
- US-imaging size NLP — already done via `runs/tirads_granular/` pipeline
- Backfilling pre-1999 FNAs (corpus is 1999+)
