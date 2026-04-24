# Cursor Prompt — Fresh LLM pass for ETE grade subclassification (167 PTC patients)

**Date:** 2026-04-24
**Author:** Logan Glosser (via Claude)
**Predecessor:** Migration 53 (2026-04-24) — layered existing `note_entities_llm_pathology` ETE grade onto 196 PTC present_unspecified patients. 29 reclassified; **167 remain as `ete_grade_final = 'unspec_remaining'`**.
**Goal:** Target those 167 patients with a narrow, grade-focused LLM prompt to push them into `gross` vs `microscopic` buckets. This is the final step needed to unblock the ETE manuscript analysis with full patient coverage.

## Context you need

- Live view: `manuscript_workspace.ete_manuscript_analytic_v2`
- Cohort grain: per path event, but the deliverable is patient-level
- Resolver column: `ete_grade_final` ∈ `{gross, microscopic, none, unspec_remaining, NULL}`
- Source provenance: `ete_grade_source` ∈ `{structured, llm_subgrade, unresolved, NULL}`
- The 167 unresolved patients are identified by: `cohort_ptc AND analytic_eligible AND ete_grade_source='unresolved'`
- Their path notes live in whatever source feeds `main.note_entities_llm_pathology` (`source_workbook`/`source_sheet`/`source_column` point at the originals — `preprocessed_at_utc`/`preprocess_batch_id` for the extraction lineage)

## Why the existing extract missed them

Looking at `note_entities_llm_pathology` output for the 167: most mentions are just `"entity_value": "present"` without a grade modifier. The generic pathology extraction prompt didn't force the model to commit to micro-vs-gross. We need a **narrow prompt** that:

1. Is given ONE note at a time with ETE context highlighted
2. Forces a 4-way classification: `gross` / `microscopic` / `absent` / `unable_to_determine`
3. Returns evidence quote + reasoning
4. Does NOT generalize — if the note says "extrathyroidal extension present" without further detail, the answer is `unable_to_determine`, not a guess

## Classification rules (put these IN the LLM prompt)

### Gross ETE
- "extension into strap muscles" / "sternothyroid" / "sternohyoid" / "omohyoid" / "sternocleidomastoid"
- "extension into trachea" / "tracheal invasion"
- "extension into esophagus" / "esophageal invasion"
- "extension into recurrent laryngeal nerve" / "RLN invasion"
- "gross extrathyroidal extension"
- "grossly extends beyond the thyroid"
- "macroscopic extension"
- "substernal extension"
- AJCC8 pT3b / pT4a / pT4b language

### Microscopic ETE
- "microscopic extrathyroidal extension"
- "minimal extrathyroidal extension"
- "focal extension into perithyroidal adipose tissue / fat"
- "extension into perithyroidal soft tissue" (without muscle invasion)
- "focally extends into the fibrous capsule"
- AJCC8 pT3a language (when referring to ETE, not size)
- Note: AJCC8 removed microscopic ETE from T3a in 2017. If a newer report lists pT3a WITHOUT mentioning microscopic ETE, that's a size-based T3a — NOT an ETE case. Prompt the model explicitly on this.

### Absent
- "no evidence of extrathyroidal extension"
- "ETE absent"
- "no ETE"
- "tumor confined to thyroid"
- "negative for ETE"

### Unable to determine
- "extrathyroidal extension present" / "ETE present" with no grade qualifier
- Only a staging code like "pT3" without the letter modifier (pre-AJCC8 pT3 = size OR microscopic ETE)
- Contradictory or ambiguous language
- **Do not guess. `unable_to_determine` is a legitimate and useful label.**

## Deliverable structure

### 1. New extraction table

```
main.note_entities_llm_ete_subgrade_v1
-----------------------------------------
note_row_id        VARCHAR
research_id        VARCHAR
note_date          VARCHAR
ete_grade_llm      VARCHAR  -- 'gross' / 'microscopic' / 'absent' / 'unable_to_determine'
confidence         VARCHAR  -- 'high' / 'medium' / 'low'
evidence_quote     VARCHAR
reasoning          VARCHAR
ajcc8_implication  VARCHAR  -- 'pT3b' / 'pT4a' / 'pT4b' / 'pT3a_size_only' / NULL
raw_llm_response   VARCHAR
error              INTEGER
extracted_at       VARCHAR
llm_model          VARCHAR  -- e.g., 'qwen3:32b'
```

### 2. Build migration 54

File: `qc_framework_v1/migrations/54_ete_subgrade_fresh_llm_layer.sql`

Build on top of migration 53:

```sql
CREATE OR REPLACE VIEW manuscript_workspace.ete_manuscript_analytic_v3 AS
SELECT
  v2.*,
  s.ete_grade_llm AS ete_grade_subgrade_llm,
  s.confidence    AS ete_subgrade_confidence,
  s.evidence_quote AS ete_subgrade_evidence,
  -- Update ete_grade_final: if v2 said unspec_remaining AND subgrade has a grade, use it
  CASE
    WHEN v2.ete_grade_final <> 'unspec_remaining'               THEN v2.ete_grade_final
    WHEN s.ete_grade_llm = 'gross'                              THEN 'gross'
    WHEN s.ete_grade_llm = 'microscopic'                        THEN 'microscopic'
    WHEN s.ete_grade_llm = 'absent'                             THEN 'none'
    ELSE 'unspec_remaining'  -- LLM also said unable_to_determine
  END AS ete_grade_final_v3,
  CASE
    WHEN v2.ete_grade_final <> 'unspec_remaining'               THEN v2.ete_grade_source
    WHEN s.ete_grade_llm IN ('gross','microscopic','absent')    THEN 'llm_fresh_subgrade'
    ELSE 'llm_unable'
  END AS ete_grade_source_v3
FROM manuscript_workspace.ete_manuscript_analytic_v2 v2
LEFT JOIN (
  -- Collapse per-patient: gross wins over micro wins over absent wins over unable
  SELECT research_id,
    CASE
      WHEN MAX(CASE WHEN ete_grade_llm='gross' THEN 1 ELSE 0 END)=1       THEN 'gross'
      WHEN MAX(CASE WHEN ete_grade_llm='microscopic' THEN 1 ELSE 0 END)=1 THEN 'microscopic'
      WHEN MAX(CASE WHEN ete_grade_llm='absent' THEN 1 ELSE 0 END)=1      THEN 'absent'
      ELSE 'unable_to_determine'
    END AS ete_grade_llm,
    MAX(confidence) AS confidence,
    STRING_AGG(evidence_quote, ' | ') AS evidence_quote
  FROM main.note_entities_llm_ete_subgrade_v1
  WHERE ete_grade_llm IS NOT NULL
  GROUP BY research_id
) s ON s.research_id = v2.research_id;
```

Plus:
- Deprecation log row with `closing_prompt='prompt_53'`, `issue_id='MANUSCRIPT_ETE_SUBGRADE_FRESH'`
- ISSUE_REGISTRY.md run log entry

### 3. Acceptance

Expected — based on the yield-rate from the existing generic LLM pass (14% grade-bearing among 196 unspec):

- Fresh narrow prompt targeted at grade should do much better — aim for **50-70% grade resolution** on the 167
- Minimum acceptance: ≥ 100 of 167 resolved (50 gross + 50 micro as rough split)
- `unable_to_determine` + LLM error: ≤ 67

Final manuscript N should end up at 200+ clean-graded PTC patients.

## Execution plan

### Where to run the LLM

Per memory: RunPod H200 or Vast.ai GPU instance with qwen3:32b (`OLLAMA_CONTEXT_LENGTH=8192`, `KV_CACHE_TYPE=q8_0`, `FLASH_ATTENTION=1`, NUM_PARALLEL per existing config). Target ~2-5 notes/min throughput.

167 patients × ~3-5 path notes each ≈ 500-800 notes → **roughly 3-6 hours of GPU time**.

### Script scaffolding

1. `scripts/410_ete_subgrade_patient_list.py` — emits `/Users/ros/THyroid 2026/scripts/output/ete_subgrade_patients_20260424.csv` (research_id + path notes)
2. `scripts/411_ete_subgrade_llm_run.py` — iterates the CSV, calls the LLM endpoint with the narrow prompt above, writes to a local parquet
3. `scripts/412_ete_subgrade_load_to_md.py` — `MotherDuck upload` the parquet to `main.note_entities_llm_ete_subgrade_v1`
4. Apply migration 54, probe acceptance, commit, push

### Commit pattern (surgical)

```
git add qc_framework_v1/migrations/54_ete_subgrade_fresh_llm_layer.sql \
        qc_framework_v1/ISSUE_REGISTRY.md \
        scripts/410_ete_subgrade_patient_list.py \
        scripts/411_ete_subgrade_llm_run.py \
        scripts/412_ete_subgrade_load_to_md.py
```

Do NOT `git add scripts/output/` or `-A`. Use Desktop Commander if sandbox .git index lock issues arise.

## Rules

- **View-layer only** on manuscript_workspace side. `main.canonical_path_malignant_events_v1` is NOT mutated.
- **TIMESTAMP casts**: `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for any build_ts columns
- **NULL-safe joins** if joining on nullable keys: `IS NOT DISTINCT FROM`
- **Idempotent**: `DELETE FROM ... WHERE closing_prompt='prompt_53'` before INSERT into deprecation log
- **No cross-DB sourcing**: `main.*` is live; never `FROM archive_pub_v1_0.*`
- **PHI safety**: never print clinical note text to Claude chat, logs, or commits. The LLM runs on the secured fleet (S1-S8, V1, or RunPod with PHI-cleared config). Output to MotherDuck carries `research_id` only, no note text beyond `evidence_quote` (which should be short and de-identified by the LLM prompt instruction: "quote evidence but redact any proper names or specific dates").

## Anti-goals

- Don't try to hand-grade in Python regex — the LLM pass is the point
- Don't relax the 4-way output schema to a 2-way; keeping `unable_to_determine` as a legitimate class prevents false precision
- Don't rebuild `path_malignant_overlay_ete_clean_w_fp_v1` — the subgrade is a patient-level enrichment, not an event-level overlay rewrite
- Don't touch the other migration 52 overlays (histology, invasion, laterality, etc.)

## Success marker

When migration 54 is applied, this should return a number ≥ 200:

```sql
SELECT COUNT(DISTINCT research_id)
FROM manuscript_workspace.ete_manuscript_analytic_v3
WHERE cohort_ptc
  AND analytic_eligible
  AND ete_grade_final_v3 IN ('gross','microscopic','none');
```
