# Cursor Prompt — Migration 65: Full-cohort LLM re-extraction for recurrence with strict dual-track schema

**Date:** 2026-04-27
**Author:** Logan Glosser (drafted with Claude / Cowork)
**Target DB:** `thyroid_canonical_publication_v1_0` (MotherDuck)
**Issue ID:** `MANUSCRIPT_RECURRENCE_DUAL_TRACK_LLM_DEEP`
**Predecessor:** mig_62 — built `main.canonical_recurrence_resolved_v1` from existing signals (191 path-proven + 701 imaging-only).

## Why this prompt exists

mig_62 captured the high-confidence patients we could identify with deterministic rules:
- 191 path-proven (multi-malignant-surgery, structural_confirmed, post-op FNA Bethesda 5/6, LLM with path-keyword evidence)
- 701 imaging-only (CT/MRI/nucmed flagged + LLM with imaging-keyword evidence)

But the existing LLM pass (`note_entities_llm_recurrence`) only returned non-empty entities for 1,442 of 5,641 patients (and most of those were already empty entity arrays). 9,478 of 11,037 LLM rows had `"entities":[]`, and ~5% had `parse_error`. The LLM probably missed:
- Patients with op-notes for completion thyroidectomy / lateral neck dissection that mentioned recurrence
- Patients with surveillance endocrine notes describing imaging findings
- Patients where the LLM returned `parse_error` and we never re-tried

This prompt re-runs the LLM with a focused dual-track prompt over the FULL `main.clinical_notes_long` cohort, with explicit categorization rules so we don't have to disambiguate path vs imaging downstream.

## Source-of-truth

- Excel: `Notes 12_1_25.xlsx` (referenced as `source_workbook` in `note_entities_llm_recurrence`)
- DB mirror: `main.clinical_notes_long` (11,050 notes / 8 note types)
- Path synoptic mirror: `main.path_synoptics`
- Imaging tables: `main.ct_imaging`, `main.mri_imaging`, `main.nuclear_med`, `main.canonical_us_lymph_node_v2`

## Phase 0 — pre-flight

```sql
-- (a) Confirm mig_62 state
SELECT recurrence_status_final, COUNT(*) FROM main.canonical_recurrence_resolved_v1 GROUP BY 1;
-- Expected: path_proven=191, imaging_only_unconfirmed=701, none=9979

-- (b) Identify the 1,686 'structural_date_unknown' orphans (dropped in mig_62) for targeted re-LLM
WITH dated_signal AS (
  SELECT DISTINCT research_id FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final<>'none'
)
SELECT COUNT(DISTINCT research_id) FROM main.recurrence_event_clean_v1
WHERE recurrence_definition='structural_date_unknown'
  AND research_id NOT IN (SELECT research_id FROM dated_signal);
-- Expected: ~1,686 patients to be re-checked

-- (c) Cohort note coverage
WITH targets AS (
  SELECT DISTINCT research_id FROM main.recurrence_event_clean_v1
  WHERE recurrence_definition='structural_date_unknown'
  UNION
  SELECT research_id FROM main.canonical_patient_master WHERE diagnosis_primary='PTC'
)
SELECT note_type, COUNT(*) FROM main.clinical_notes_long
WHERE research_id IN (SELECT research_id FROM targets)
  AND note_type IN ('endocrine_note','op_note','op_note_addendum','dc_sum','h_p','progress_note')
GROUP BY 1;
```

## Phase 1 — Build LLM input snippets

`scripts/440_recurrence_dual_track_build_snippets.py`:

1. Materialise `main.note_entities_llm_recurrence_dual_track_input_v1` — one row per (research_id, clinical_notes_long row).
2. Filter to note types that plausibly contain recurrence evidence: `endocrine_note`, `progress_note`, `op_note`, `op_note_addendum`, `dc_sum`, `h_p`, `surveillance`, `outpatient`.
3. Carry note_date so we can date the resulting events.
4. Skip notes where the body is < 200 chars (low signal).
5. Carry hint metadata: did this patient have multi_malignant_surgery? structural_date_unknown flag? existing dated_signal? — pass as prompt context.

Expected: ~30,000 rows for full PTC + structural_date_unknown cohort.

## Phase 2 — LLM batch on S8 (qwen3:14b, temp 0)

Prompt template — `prompts/recurrence_dual_track_v1.txt`:

```
You are a thyroid cancer surveillance specialist categorizing potential recurrence
evidence in a clinical note. Read the note and return ALL distinct findings that
indicate or rule out recurrence.

For each finding, classify into ONE bucket:

  path_proven        — biopsy positive | FNA cytology Bethesda V or VI | operative
                       pathology positive on a post-treatment specimen | core needle
                       biopsy positive
  imaging_suspicious — imaging report flags a lesion as suspicious, recurrent, or
                       residual, with no path confirmation in the same note
  disease_free       — surveillance imaging clean | undetectable thyroglobulin |
                       explicit "no evidence of disease" / "NED"
  ambiguous          — text mentions recurrence/cancer/disease but cannot determine
                       if it is current finding vs historical reference vs differential

CRITICAL DISTINCTIONS (the manuscript depends on these never being collapsed):
  - "Suspicious lymph node on US" alone is imaging_suspicious (NOT path_proven).
  - "FNA of suspicious LN positive for PTC" is path_proven.
  - A surveillance ultrasound mentioning a stable post-treatment scar is disease_free.
  - An H&P documenting "history of thyroid cancer" with no current finding is NOT
    a recurrence event — return [] if there is no current/new finding.

For each path_proven finding:
  - Capture the specimen type (FNA / core_bx / op_path / cytology) in `path_specimen_type`
  - Capture the date the specimen was obtained in `entity_date`

For each imaging_suspicious finding:
  - Capture modality in `modality` (us | ct | mri | pet | nucmed)
  - Capture the verbatim finding text in `evidence_text`
  - Capture the imaging study date in `entity_date`

Return JSON only:
{
  "entities": [
    {
      "category":              "path_proven" | "imaging_suspicious" | "disease_free" | "ambiguous",
      "entity_date":           "YYYY-MM-DD" | null,
      "modality":              "us" | "ct" | "mri" | "pet" | "nucmed" | null,
      "path_specimen_type":    "fna" | "core_bx" | "op_path" | "cytology" | null,
      "evidence_text":         "verbatim quote, ≤200 chars",
      "confidence":            "high" | "medium" | "low",
      "reasoning":             "one sentence"
    },
    ...
  ]
}

If the note contains no recurrence-relevant findings, return {"entities": []}.
NEVER collapse path_proven and imaging_suspicious into a single entity.

NOTE:
{{narrative}}

CONTEXT_HINTS:
  - patient_first_surgery_date: {{first_surg_date}}
  - patient_already_in_path_proven_pool: {{flag}}
  - this_note_date: {{note_date}}
```

Loader: `scripts/441_recurrence_dual_track_load_to_md.py`. Output: `main.note_entities_llm_recurrence_dual_track_v1`.

Expected runtime: ~10 hours on S8 at 1 req/s with retries.

## Phase 3 — Build canonical_recurrence_resolved_v2

`qc_framework_v1/migrations/65_canonical_recurrence_resolved_v2.sql`:

1. Parse the new LLM output by category.
2. For each patient with at least one path_proven entity → add to recurrence_path_proven track. Use earliest entity_date as the date.
3. For each patient with at least one imaging_suspicious entity (and NO path_proven for that patient) → add to recurrence_imaging_suspicious track. Use earliest entity_date as the date. Carry modality + finding text per entity.
4. Layer onto canonical_recurrence_resolved_v1 (mig_62) using union with priority: new LLM > existing mig_62 sources where they conflict.
5. Build `main.canonical_recurrence_resolved_v2` with the same schema as v1 plus a `_source_set` field listing all contributing sources.
6. Refresh `main.canonical_ete_event_resolved_v1` to point at v2.

## Phase 4 — Acceptance probes

```sql
-- Coverage gain over mig_62
SELECT
  (SELECT COUNT(*) FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_path_proven) AS v1_path,
  (SELECT COUNT(*) FROM main.canonical_recurrence_resolved_v2 WHERE recurrence_path_proven) AS v2_path,
  (SELECT COUNT(*) FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_imaging_suspicious) AS v1_img,
  (SELECT COUNT(*) FROM main.canonical_recurrence_resolved_v2 WHERE recurrence_imaging_suspicious) AS v2_img;
-- Target: v2_path within ±20% of v1_path (191), v2_img within ±20% of v1_img (701).
-- LARGE deviations indicate prompt drift; investigate before promoting.

-- Disagreement audit
SELECT
  v1.recurrence_status_final AS v1_status,
  v2.recurrence_status_final AS v2_status,
  COUNT(*) AS n
FROM main.canonical_recurrence_resolved_v1 v1
JOIN main.canonical_recurrence_resolved_v2 v2 USING (research_id)
GROUP BY 1,2 ORDER BY n DESC;
```

## Deliverables checklist

- [ ] `scripts/440_recurrence_dual_track_build_snippets.py`
- [ ] `scripts/441_recurrence_dual_track_load_to_md.py`
- [ ] `prompts/recurrence_dual_track_v1.txt`
- [ ] LLM batch run on S8 — ~30,000 rows
- [ ] `qc_framework_v1/migrations/65_canonical_recurrence_resolved_v2.sql`
- [ ] `project_mig_65_recurrence_dual_track_closeout.md` with Phase 4 numbers
- [ ] `qc_framework_v1/ISSUE_REGISTRY.md` updated to close `MANUSCRIPT_RECURRENCE_DUAL_TRACK_LLM_DEEP`
- [ ] Commit message: `mig_65: full-cohort recurrence dual-track LLM re-extraction (qwen3:14b on S8) + canonical_recurrence_resolved_v2`

## Source-of-truth pointers

- Original Excel: `Notes 12_1_25.xlsx`
- DB mirror: `main.clinical_notes_long`
- Existing LLM (deprecated by this migration): `main.note_entities_llm_recurrence`
- New LLM table: `main.note_entities_llm_recurrence_dual_track_v1`
- Patient-level resolution: `main.canonical_recurrence_resolved_v2` (will SUPERSEDE v1)
