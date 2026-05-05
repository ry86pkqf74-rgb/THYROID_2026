# Cursor Prompt — mig_310 v2: FNA NLP from HP-note corpus (corrected)

**Agent:** Cursor Composer
**Estimated time:** 3–4 hours
**Date:** 2026-05-05
**Supersedes:** `CURSOR_PROMPT_MIG_310_FNA_NLP_SIZE_EXTRACTION_20260505.md` (v1 assumed FNA-typed corpus, doesn't exist)

## What v1 got wrong (Cowork-discovered probe 2026-05-05)

The v1 prompt assumed a `note_type='FNA_CYTOLOGY'` (or similar) corpus existed in `clinical_notes_long` / `CLINICAL_NOTES_SEARCH_V1`. **It doesn't.** Probe results:

```
SF CLINICAL_NOTES_SEARCH_V1 note_type distribution:
  OPNOTE: 4,727
  HP: 4,280
  OTHER_HISTORY: 525
  ENDOCRINE_FM: 522
  ED_NOTE: 498
  DC_SUM: 185
  OTHER_NOTES: 160
  DEATH: 153

MD clinical_notes_long FNA-candidate note_type distribution:
  hp: 2,810
  opnote: 857
  other_history: 106
  endocrine_fm: 77
  dc_sum: 24
  other_notes: 8
  ed_note: 1
```

**No FNA-typed notes anywhere.** FNA cytology reports are *embedded* in HP notes. Probe of "bethesda" keyword shows: **top note_type containing "bethesda" is HP**. 1,259 of 11,050 notes (11.4%) contain FNA-related keywords.

Also v1 had a SQL bug: `note_date` referenced before defined in SELECT (line 187 of `36_pull_sf_nlp_fna_size.py`).

## Corrected pipeline

### Step 1 — Define the actual FNA corpus via keyword filter

```sql
-- MotherDuck side: build the FNA-content corpus
CREATE OR REPLACE VIEW manuscript_workspace.fna_content_corpus_v1 AS
WITH ranked AS (
  SELECT
    n.research_id,
    n.note_id,
    n.note_date,
    n.note_type,
    n.note_text,
    -- Score how "FNA-cytology-like" a note is
    (
      (CASE WHEN LOWER(n.note_text) LIKE '%bethesda%' THEN 3 ELSE 0 END) +
      (CASE WHEN LOWER(n.note_text) LIKE '%fine needle aspirat%' THEN 2 ELSE 0 END) +
      (CASE WHEN LOWER(n.note_text) LIKE '%cytopath%' THEN 2 ELSE 0 END) +
      (CASE WHEN LOWER(n.note_text) ILIKE '%fna%' THEN 1 ELSE 0 END) +
      (CASE WHEN LOWER(n.note_text) LIKE '%afirma%' OR LOWER(n.note_text) LIKE '%thyroseq%' THEN 1 ELSE 0 END)
    ) AS fna_relevance_score
  FROM main.clinical_notes_long n
  WHERE
    LOWER(n.note_type) IN ('hp','opnote','endocrine_fm','other_history')
    AND (
      LOWER(n.note_text) LIKE '%bethesda%' OR
      LOWER(n.note_text) LIKE '%fine needle aspirat%' OR
      LOWER(n.note_text) LIKE '%cytopath%' OR
      (LOWER(n.note_text) ILIKE '%fna%' AND LOWER(n.note_text) LIKE '%thyroid%')
    )
)
SELECT * FROM ranked
WHERE fna_relevance_score >= 1;
```

Verify: should produce ~1,200–1,500 notes covering ~85% of `canonical_fna_events_v1` patients via research_id+date proximity.

### Step 2 — Linkage map: FNA event → candidate notes

```sql
CREATE OR REPLACE VIEW manuscript_workspace.fna_event_note_linkage_v1 AS
SELECT
  fe.fna_event_id,
  fe.research_id,
  fe.fna_date_resolved,
  fe.laterality,
  fe.bethesda_final_num,
  c.note_id,
  c.note_date,
  c.fna_relevance_score,
  ABS(DATEDIFF('day', fe.fna_date_resolved, c.note_date)) AS day_gap
FROM main.canonical_fna_events_v1 fe
JOIN manuscript_workspace.fna_content_corpus_v1 c USING(research_id)
WHERE ABS(DATEDIFF('day', fe.fna_date_resolved, c.note_date)) <= 60
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY fe.fna_event_id
  ORDER BY day_gap ASC, c.fna_relevance_score DESC
) = 1;
```

This picks the nearest, most-FNA-relevant note per FNA event within 60 days.

### Step 3 — Export to SF + Cortex extraction

Same as v1 mig_310, but use the linkage view above as the source and **fix the SELECT bug**:

```sql
-- in 36_pull_sf_nlp_fna_size.py line ~187
-- WRONG: SELECT ... FROM (SELECT ..., note_date AS d) WHERE note_date BETWEEN ...
-- RIGHT: define note_date in inner alias before referencing
SELECT research_id, fna_event_id, note_id, note_date_inner AS note_date, note_text
FROM (
  SELECT research_id, fna_event_id, note_id, note_date AS note_date_inner, note_text
  FROM manuscript_workspace.fna_event_note_linkage_v1 lnk
  JOIN main.clinical_notes_long n USING(note_id, research_id)
)
WHERE note_date BETWEEN '2000-01-01' AND CURRENT_DATE
```

Then Cortex EXTRACT_ANSWER:

```sql
CREATE OR REPLACE TABLE THYROID_VALIDATION.PUBLIC.NLP_FNA_SIZE_FULL_RESULTS_v1 AS
SELECT
  research_id,
  fna_event_id,
  note_date,
  result:size_cm::FLOAT AS extracted_size_cm,
  result:laterality::VARCHAR AS extracted_laterality,
  result:nodule_count::INTEGER AS extracted_nodule_count,
  result:bethesda_match::INTEGER AS extracted_bethesda,
  result:confidence::VARCHAR AS extraction_confidence,
  CURRENT_TIMESTAMP AS extracted_at
FROM (
  SELECT research_id, fna_event_id, note_date,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
      note_text,
      'Extract from this clinical note about a thyroid FNA cytology result:
      - size_cm: numeric size in cm of the aspirated nodule (largest dimension if multiple).
                  Look for phrases like "1.2 cm nodule was aspirated", "FNA of 2.3-cm right lobe nodule".
                  Return null if size not stated.
      - laterality: "right", "left", "isthmus", "bilateral", or null if not stated.
                    Look for "right lobe FNA", "left thyroid biopsy", etc.
      - nodule_count: integer count of distinct nodules sampled in the FNA. Default 1 if FNA mentioned but count unclear.
      - bethesda_match: Bethesda category 1-6 if explicitly stated; null otherwise.
      - confidence: "high" if size and side both unambiguous from clear FNA-result language;
                    "medium" if one is inferred from context;
                    "low" if extracted from passing mention only.
      Return as JSON object with these five keys.'
    ) AS result
  FROM <staged_corpus>
);
```

### Step 4 — Sample-200 manual QA

Pull 200 random rows from `NLP_FNA_SIZE_FULL_RESULTS_v1` joined to source note text, write to CSV at `studies/mig_310_qa/sample_200.csv`. Manual review for: size_cm precision target ≥85%, laterality ≥95%. Document precision in signoff.

### Step 5 — MD mirror + linkage v4

Same as v1 mig_310 step 4–5: mirror to `manuscript_workspace.nlp_fna_size_rollup_v1`, build `imaging_fna_linkage_v4` with `size_score_v4` weighted prior.

### Step 6 — Rebuild M025 nodule-level + smoke test

```bash
# MD-side: rebuild mig_306 view with v4 join
# SF-side: re-export to COHORT_M025_NODULE_LEVEL_V1_FLAT (use load_m025_nodule_level_to_sf.py)
# CLI smoke: cortex analyst query "what's the per-tr ROM" should still hit TR4 18.7% / TR5 26.1%
# Bonus: query "how does ROM stratify by FNA size band" should now resolve
```

### Step 7 — Signoff

```sql
INSERT INTO main.signoff_migration VALUES
('mig_310', CURRENT_TIMESTAMP, 'cursor_composer_mig310_v2',
 'mig_310 v2: FNA NLP size + laterality + Bethesda + nodule_count via HP-note keyword corpus (v1 was wrong: no FNA note_type exists, content embedded in HP notes). Built fna_content_corpus_v1 (n=<X> notes), fna_event_note_linkage_v1 (matched <Y>/<8050> FNA events to nearest in-60d note). Cortex EXTRACT_ANSWER produced NLP_FNA_SIZE_FULL_RESULTS_v1 (n=<Z> rows). Sample-200 QA: size_cm precision <P1>%, laterality <P2>%. MD mirror nlp_fna_size_rollup_v1 (<W> patients). imaging_fna_linkage_v4 built with size_score_v4 prior. M025 nodule-level rebuilt; smoke test: TR4 ROM <new>%, TR5 <new>%. Closes CF-FNA-SIZE-CM-NULL.');
```

## Out-of-scope

- Pre-1999 FNAs (corpus starts 1999)
- Non-thyroid FNA mentions (filter by thyroid keyword in step 1)
- Bethesda extraction is bonus — primary key was already in `canonical_fna_events_v1.bethesda_final_num`; use as cross-validation
