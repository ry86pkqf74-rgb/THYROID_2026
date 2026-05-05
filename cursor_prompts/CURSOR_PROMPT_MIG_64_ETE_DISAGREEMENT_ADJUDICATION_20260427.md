# Cursor Prompt — Migration 64: Adjudicate the 187-patient ete_grade vs ete_grade_clean disagreement set against the All-Diagnoses & Synoptic source

**Date:** 2026-04-27
**Author:** Logan Glosser (drafted with Claude / Cowork)
**Target DB:** `thyroid_canonical_publication_v1_0` (MotherDuck)
**Issue ID:** `MANUSCRIPT_ETE_PM_DISAGREEMENT_ADJUDICATION`
**Predecessor:** mig_61 — `main.canonical_ete_event_resolved_v1.pm_disagreement_flag = TRUE` on **356 events / 187 patients**

## Why this prompt exists

`canonical_patient_master` carries TWO patient-level ETE columns that frequently disagree:

- `ete_grade` — older field, sourced from `tumor_episode_master_v2` rollup
- `ete_grade_clean` — newer cleaned field from `extraction_audit_engine_v7` and `script_390_rule_a_20260422`

Within the PTC subset, the dominant disagreement pattern is:

| ete_grade | ete_grade_clean | n PTC pts |
|---|---|---|
| gross | microscopic | 187 |
| present_ungraded | indeterminate | 24 |
| gross | indeterminate | 2 |

Plus a small number of mappings (absent → none, etc.) that aren't actionable. **The 187 gross↔microscopic conflicts are the actionable set** — the choice flips the AJCC8 T-stage between pT3b (gross→pT4a/pT4b downstream) and pT3a (microscopic, size-only). We need a focused LLM adjudication against the original synoptic + op-note text to commit each patient.

A second small batch — the 24+2 "indeterminate" rows — is included in the same run with the same prompt; output bucket may legitimately come back as `unable_to_determine`.

## Source of truth

Same as mig_63: original Excel `All Diagnoses & synoptic 12_1_2025.xlsx`, sheet `synoptics + Dx merged`, mirrored in `main.path_synoptics`. **Use `main.path_synoptics`** for batch processing.

For these 187 patients we ALSO want op-note context, since gross ETE is most often called intraoperatively. Pull op-note text from `main.clinical_notes_long` where `note_type IN ('op_note','op_note_addendum')` for the same patients.

## Phase 0 — pre-flight

```sql
-- (a) Confirm input set
SELECT COUNT(*) AS n_events, COUNT(DISTINCT research_id) AS n_pts
FROM main.canonical_ete_event_resolved_v1
WHERE pm_disagreement_flag;
-- Expected: 356 events / 187 patients

-- (b) Confirm available source rows for these patients
WITH d AS (SELECT DISTINCT research_id FROM main.canonical_ete_event_resolved_v1 WHERE pm_disagreement_flag)
SELECT
  (SELECT COUNT(*) FROM main.path_synoptics ps WHERE ps.research_id::VARCHAR IN (SELECT research_id::VARCHAR FROM d)) AS n_synoptic_rows,
  (SELECT COUNT(*) FROM main.clinical_notes_long cn
     WHERE cn.research_id::VARCHAR IN (SELECT research_id::VARCHAR FROM d)
       AND cn.note_type IN ('op_note','op_note_addendum')) AS n_op_note_rows;

-- (c) Show the disagreement detail at patient grain
SELECT
  pm.research_id,
  pm.ete_grade           AS pm_ete_grade,
  pm.ete_grade_clean     AS pm_ete_grade_clean,
  pm.ete_grade_source,
  pm.ete_grade_adjudicated,
  COUNT(DISTINCT er.path_surgery_id) AS n_path_events
FROM main.canonical_patient_master pm
JOIN main.canonical_ete_event_resolved_v1 er ON er.research_id = pm.research_id
WHERE er.pm_disagreement_flag
GROUP BY pm.research_id, pm.ete_grade, pm.ete_grade_clean, pm.ete_grade_source, pm.ete_grade_adjudicated
ORDER BY pm.research_id
LIMIT 10;
```

## Phase 1 — Build LLM input snippets

Create `scripts/430_ete_pm_disagreement_build_snippets.py`:

1. Materialise `main.note_entities_llm_ete_pm_disagreement_input_v1` — one row per (research_id, source_row), where source_row is either a path_synoptics row OR a clinical_notes_long op_note row.
2. For path_synoptics rows: assemble narrative as in mig_63 (gross + microscopic + diagnosis + per-tumor ETE/capsular/margin).
3. For op_note rows: include the full `note_text` (truncate at 6 000 chars).
4. Carry the disagreement metadata: `pm_ete_grade`, `pm_ete_grade_clean`, `pm_ete_grade_source` — pass these as hints in the prompt.

Expected output: 187 patients × ~2 (one synoptic + one op-note) ≈ 350–500 rows.

## Phase 2 — Run the LLM batch

Reuse mig_54 / mig_63 infrastructure — fleet S8, qwen3:14b, temperature 0.

Prompt template — `prompts/ete_pm_disagreement_v1.txt`:

```
You are a board-certified thyroid pathologist resolving a disagreement between two upstream pipelines about the ETE grade for a single patient. Two patient-level pipelines reported:

  - older pipeline (tumor_episode_master_v2):  {{pm_ete_grade}}
  - newer rule_a pipeline (script_390):        {{pm_ete_grade_clean}}

Read the report below and decide which call is correct, OR set unable_to_determine. Pick exactly ONE bucket for the dominant tumor focus:

  gross               — invasion of strap muscles, trachea, esophagus, RLN; "grossly extends"; macroscopic; explicit pT3b / pT4a / pT4b
  microscopic         — "microscopic ETE" / "minimal ETE" / focal extension into perithyroidal adipose / focal extension into fibrous capsule (no muscle invasion)
  none                — "no ETE" / "ETE absent" / "confined to thyroid"
  unable_to_determine — text says ETE present without micro/gross commitment, or text is ambiguous, contradictory, or silent

CRITICAL RULES:
- AJCC8 (2017) removed microscopic ETE from T3a. A bare pT3a WITHOUT mention of micro ETE means SIZE-BASED T3a — do NOT classify as ETE.
- Op-note language ("grossly extends into strap muscle") OUTWEIGHS a microscopic-only path call when present.
- Path synoptic "extrathyroidal extension: minimal" + microscopic_description mentioning gross strap muscle invasion → gross.
- A bare "extrathyroidal extension: present" with no further detail is unable_to_determine.
- Evidence MUST be a verbatim quote (≤120 chars) and you MUST cite which source (synoptic vs op_note).

Return ONLY valid JSON:
{
  "ete_grade":            "gross" | "microscopic" | "none" | "unable_to_determine",
  "ajcc8_implication":    "pT3a_size_only" | "pT3b" | "pT4a" | "pT4b" | "no_ete_implication" | null,
  "agrees_with":          "older_pipeline" | "newer_pipeline" | "neither" | null,
  "confidence":           "high" | "medium" | "low",
  "evidence_quote":       "...",
  "evidence_source":      "synoptic" | "op_note" | "both",
  "reasoning":            "one-sentence justification"
}

REPORT:
{{narrative}}

SOURCE_TYPE_HINT: {{source_type}}     -- synoptic or op_note
```

Loader: `scripts/431_ete_pm_disagreement_load_to_md.py`. Output table: `main.note_entities_llm_ete_pm_disagreement_v1`.

Expected runtime: ~30 min on S8.

## Phase 3 — Build canonical resolution table

`qc_framework_v1/migrations/64_ete_pm_disagreement_canonical.sql`:

```sql
CREATE OR REPLACE TABLE main.canonical_ete_pm_disagreement_resolution_v1 AS
WITH parsed AS (
  SELECT
    research_id::VARCHAR                                          AS research_id,
    json_extract_string(parsed_json, '$.ete_grade')               AS ete_grade,
    json_extract_string(parsed_json, '$.ajcc8_implication')       AS ajcc8_implication,
    json_extract_string(parsed_json, '$.agrees_with')             AS agrees_with,
    json_extract_string(parsed_json, '$.confidence')              AS confidence,
    json_extract_string(parsed_json, '$.evidence_quote')          AS evidence_quote,
    json_extract_string(parsed_json, '$.evidence_source')         AS evidence_source,
    json_extract_string(parsed_json, '$.reasoning')               AS reasoning,
    extracted_at, llm_model
  FROM main.note_entities_llm_ete_pm_disagreement_v1
  WHERE error = 0
)
SELECT
  research_id,
  -- Patient-level resolution: weighted by op_note > synoptic, gross > micro > none > unable
  CASE
    WHEN MAX(CASE WHEN ete_grade='gross' AND evidence_source IN ('op_note','both') THEN 1 ELSE 0 END)=1 THEN 'gross'
    WHEN MAX(CASE WHEN ete_grade='gross' THEN 1 ELSE 0 END)=1            THEN 'gross'
    WHEN MAX(CASE WHEN ete_grade='microscopic' THEN 1 ELSE 0 END)=1      THEN 'microscopic'
    WHEN MAX(CASE WHEN ete_grade='none' THEN 1 ELSE 0 END)=1             THEN 'none'
    ELSE 'unable_to_determine'
  END AS ete_grade_resolved,
  STRING_AGG(DISTINCT agrees_with, ',') AS pipeline_winner_seen,
  STRING_AGG(NULLIF(evidence_quote,''), ' | ') AS evidence_quotes,
  STRING_AGG(DISTINCT evidence_source, ',') AS evidence_sources,
  COUNT(*) AS n_rows_evaluated,
  MAX(extracted_at) AS last_extracted_at,
  MAX(llm_model) AS llm_model,
  'mig_64_ete_pm_disagreement_20260427' AS build_script,
  CURRENT_TIMESTAMP AS build_ts
FROM parsed
GROUP BY research_id;
```

## Phase 4 — Layer mig_64 into the manuscript view

Update the mig_63 v5 view (or chain a v6) so the source ladder includes:

```
1. structured (path event field)
2. llm_fresh_subgrade (mig_54)
3. llm_subgrade (mig_53 general LLM)
4. llm_fresh_absent
5. queue_closeout_llm (mig_63)
6. pm_disagreement_llm (mig_64)         <-- NEW; OVERRIDES patient_master_clean when both apply
7. patient_master_clean (Script 390)
8. patient_master_indeterminate
9. unresolved
```

The new branch in the v5/v6 CASE expression:

```sql
WHEN v4.ete_grade_pm_disagreement_flag
     AND dr.ete_grade_resolved IN ('gross','microscopic','none')   THEN dr.ete_grade_resolved
```

with source = `'pm_disagreement_llm'`.

After mig_64, refresh `main.canonical_ete_event_resolved_v1` to pick up the new column.

## Phase 5 — Acceptance probes

```sql
-- (a) Resolution coverage
SELECT
  (SELECT COUNT(DISTINCT research_id) FROM main.canonical_ete_event_resolved_v1 WHERE pm_disagreement_flag)
    AS n_input_pts,
  (SELECT COUNT(*) FROM main.canonical_ete_pm_disagreement_resolution_v1)                                   AS n_resolved,
  (SELECT COUNT(*) FROM main.canonical_ete_pm_disagreement_resolution_v1 WHERE ete_grade_resolved IN ('gross','microscopic','none'))
    AS n_committed,
  (SELECT COUNT(*) FROM main.canonical_ete_pm_disagreement_resolution_v1 WHERE ete_grade_resolved = 'unable_to_determine')
    AS n_unable;
-- Target: n_committed ≥ 170 of 187 (≥ 91 %).

-- (b) Distribution of agrees_with
SELECT pipeline_winner_seen, COUNT(*) FROM main.canonical_ete_pm_disagreement_resolution_v1 GROUP BY 1;

-- (c) Effect on manuscript view
SELECT
  COUNT(*) FILTER (WHERE pm_disagreement_flag) AS still_disagreement,
  COUNT(*) FILTER (WHERE ete_grade_source = 'pm_disagreement_llm') AS resolved_by_mig64
FROM main.canonical_ete_event_resolved_v1;
-- Target: resolved_by_mig64 ≥ 250 events (the 187-pt set explodes to 356 events).
```

## Deliverables checklist

- [ ] `scripts/430_ete_pm_disagreement_build_snippets.py`
- [ ] `scripts/431_ete_pm_disagreement_load_to_md.py`
- [ ] `prompts/ete_pm_disagreement_v1.txt`
- [ ] LLM batch run on S8 — ~400 rows
- [ ] `qc_framework_v1/migrations/64_ete_pm_disagreement_canonical.sql`
- [ ] `qc_framework_v1/migrations/64b_ete_resolved_v1_refresh.sql`
- [ ] `project_mig_64_ete_pm_disagreement_closeout.md` with Phase 5 numbers
- [ ] `qc_framework_v1/ISSUE_REGISTRY.md` updated to close `MANUSCRIPT_ETE_PM_DISAGREEMENT_ADJUDICATION`
- [ ] Commit message: `mig_64: ETE patient_master disagreement adjudication (187 pts via qwen3:14b on S8) + manuscript_v6/canonical_ete_event_resolved refresh`

## Source-of-truth pointers

- Original Excel: `All Diagnoses & synoptic 12_1_2025.xlsx`, sheet `synoptics + Dx merged`
- DB mirrors: `main.path_synoptics`, `main.clinical_notes_long` (op-notes)
- Adjudication output table: `main.note_entities_llm_ete_pm_disagreement_v1`
- Patient-level resolution: `main.canonical_ete_pm_disagreement_resolution_v1`
- Final manuscript-facing column of record (post-mig_64): `main.canonical_ete_event_resolved_v1.ete_grade`
