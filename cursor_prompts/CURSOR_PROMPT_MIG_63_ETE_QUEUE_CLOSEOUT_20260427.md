# Cursor Prompt — Migration 63: Close out the ETE self-contradiction queue (2,786 patients) via LLM adjudication against the All-Diagnoses & Synoptic source

**Date:** 2026-04-27
**Author:** Logan Glosser (drafted with Claude / Cowork)
**Target DB:** `thyroid_canonical_publication_v1_0` (MotherDuck)
**Issue ID:** `MANUSCRIPT_ETE_QUEUE_CLOSEOUT`
**Predecessors:**
- mig_53 — layered general-pathology LLM ETE entities onto unspec_remaining patients
- mig_54 — fresh narrow 4-way LLM pass over a curated 167-patient subset
- mig_61 — built `main.canonical_ete_event_resolved_v1` (manuscript column of record), surfaced 4,382 events as `open_self_contradiction_flag = TRUE` from the 2,786-patient queue

## Why this prompt exists

The ETE manuscript's biggest open-issue surface is `manuscript_workspace.cpm_ete_self_contradiction_queue_v1` — **2,786 patients** with `status = 'awaiting_manual_review'`, none resolved. Script 390 (`script_390_rule_a_20260422`) wrote the patient_master `ete_grade_clean` values BUT also queued these patients because the structured fields disagreed with the rule_a output. Reasons in the queue:

| reason | n |
|---|---|
| microscopic_no_invasion_signal | 2,579 |
| boolean_string_upstream_bug | 183 |
| adjudicator_unable_to_determine_rule_a_candidate | 26 |
| boolean_string_no_corroboration | 2 |

The queue rows have `cpm_ete_grade_final_v2` and `cpm_gross_ete_flag` mostly NULL — Script 390 enumerated patient IDs without filling the contradiction detail. **We need a clean LLM adjudication against the original source-of-truth pathology text** to either confirm Script 390's rule_a output, override it, or mark `unable_to_determine` with evidence.

## Source of truth

The original Excel file is **`All Diagnoses & synoptic 12_1_2025.xlsx`**, sheet `synoptics + Dx merged` (11,886 rows × 275 cols). It has been ingested into MotherDuck as `main.path_synoptics` (11,688 rows / 10,871 patients / 23 ETE-relevant cols — verified isomorphic).

For LLM adjudication, **read from `main.path_synoptics`** — it preserves the exact source rows but is queryable. The Excel file is the audit reference. Critical columns (per tumor 1–5):

- `tumor_X_extrathyroidal_extension` — free-text ETE call (the ambiguous column we're adjudicating)
- `tumor_X_capsular_invasion`
- `tumor_X_extranodal_extension`
- `tumor_X_margin_comment`

Plus the per-row narrative columns (cover ALL tumors on the row):

- `microscopic_description`
- `path_extended_gross_path`
- `synoptic_diagnosis`
- `path_diagnosis_summary`
- `path_diagnosis_comment`
- `clinical_information_pre_op_diagnosis`

## Phase 0 — pre-flight (do not skip)

Before running any LLM batch, probe MotherDuck:

```sql
-- (a) Confirm queue size matches the issue and pull current statuses
SELECT status, COUNT(*) AS n_rows, COUNT(DISTINCT research_id) AS n_pts
FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1
GROUP BY 1;
-- Expected: awaiting_manual_review = 2,790 rows / 2,786 patients (as of mig_61).

-- (b) Confirm canonical_ete_event_resolved_v1 exists and has 4,382 open_self_contradiction_flag events
SELECT COUNT(*) FROM main.canonical_ete_event_resolved_v1 WHERE open_self_contradiction_flag;
-- Expected: 4,382.

-- (c) Confirm path_synoptics coverage of the queue
WITH q AS (SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
           FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1
           WHERE status='awaiting_manual_review')
SELECT
  (SELECT COUNT(*) FROM q) AS n_queue_pts,
  (SELECT COUNT(*) FROM main.path_synoptics ps
     WHERE CAST(ps.research_id AS VARCHAR) IN (SELECT rid FROM q)) AS n_synoptic_rows;
-- Expected: 2,786 / 3,259 rows.
```

If counts drift > 0.5 % from the expected values, **stop and report** — there has been a registry edit since mig_61.

## Phase 1 — Build the LLM input snippets

Create `scripts/420_ete_queue_closeout_build_snippets.py`:

1. Materialise `main.note_entities_llm_ete_queue_closeout_input_v1` with one row per (research_id, path_synoptics row) for queue patients only.
2. Each row has the concatenated narrative — assemble in this order, separated by `\n---\n`:
   - `clinical_information_pre_op_diagnosis`
   - `synoptic_diagnosis`
   - `path_extended_gross_path` (gross description — primary signal for gross ETE)
   - `microscopic_description` (primary signal for microscopic ETE)
   - `path_diagnosis_summary`
   - `path_diagnosis_comment`
   - For each tumor 1–5 that exists: `tumor_X_extrathyroidal_extension`, `tumor_X_capsular_invasion`, `tumor_X_margin_comment`
3. Skip rows where the assembled narrative is < 50 chars (no signal).
4. Write `note_row_id` = MD5(research_id, path_synoptics rowid) so we can link back.
5. Carry the queue `reason` column as a hint.

Expected output: ~3,000–3,200 rows.

## Phase 2 — Run the LLM batch

Reuse `scripts/llm_batch/` infrastructure (fleet server S8, qwen3:14b — same as mig_54).

Prompt template — `prompts/ete_queue_closeout_v1.txt`:

```
You are a board-certified thyroid pathologist adjudicating extrathyroidal extension (ETE) on a final pathology report. Read the report below and classify ETE for the dominant tumor focus into ONE of:

  gross               — invasion of strap muscles, trachea, esophagus, RLN; macroscopic / "grossly extends"; explicit pT3b / pT4a / pT4b language; substernal extension
  microscopic         — "microscopic ETE" / "minimal ETE" / focal extension into perithyroidal adipose / focal extension into fibrous capsule / focal perithyroidal soft tissue WITHOUT muscle invasion
  none                — explicit "no ETE" / "ETE absent" / "no extrathyroidal extension" / "confined to the thyroid"
  unable_to_determine — text says ETE present but does not commit to micro vs gross, OR text is ambiguous / contradictory / silent

CRITICAL RULES:
- AJCC8 (2017) removed microscopic ETE from T3a. If a report shows pT3a WITHOUT mentioning microscopic ETE, it's a SIZE-BASED T3a — do NOT classify as ETE.
- A bare "extrathyroidal extension: present" with no further detail is unable_to_determine.
- Only one bucket per row; pick the dominant focus (largest, highest grade, or first listed).
- Evidence MUST be a verbatim quote from the report (≤120 chars).

Return ONLY valid JSON:
{
  "ete_grade": "gross" | "microscopic" | "none" | "unable_to_determine",
  "ajcc8_implication": "pT3a_size_only" | "pT3b" | "pT4a" | "pT4b" | "no_ete_implication" | null,
  "confidence": "high" | "medium" | "low",
  "evidence_quote": "...",
  "reasoning": "one-sentence justification"
}

PATHOLOGY REPORT:
{{narrative}}

QUEUE_REASON_HINT: {{reason}}
```

Batch settings:
- Temperature 0.0
- max_tokens 400
- Timeout 60 s, 3 retries
- Land raw output in `main.note_entities_llm_ete_queue_closeout_v1` with the same shape as `main.note_entities_llm_ete_subgrade_v1` (`parsed_json`, `raw_llm_response`, `error`, `extracted_at`, `llm_model`, `elapsed_s`).

Loader: `scripts/421_ete_queue_closeout_load_to_md.py`.

Expected runtime: ~3 hours on S8 at 1 req/s with retries.

## Phase 3 — Build canonical resolution table

Create `qc_framework_v1/migrations/63_ete_queue_closeout_canonical.sql`:

```sql
CREATE OR REPLACE TABLE main.canonical_ete_queue_resolution_v1 AS
WITH parsed AS (
  SELECT
    research_id::VARCHAR                                         AS research_id,
    note_row_id,
    json_extract_string(parsed_json, '$.ete_grade')              AS ete_grade,
    json_extract_string(parsed_json, '$.ajcc8_implication')      AS ajcc8_implication,
    json_extract_string(parsed_json, '$.confidence')             AS confidence,
    json_extract_string(parsed_json, '$.evidence_quote')         AS evidence_quote,
    json_extract_string(parsed_json, '$.reasoning')              AS reasoning,
    extracted_at, llm_model
  FROM main.note_entities_llm_ete_queue_closeout_v1
  WHERE error = 0
)
SELECT
  research_id,
  -- Patient-level resolution: gross > microscopic > none > unable
  CASE
    WHEN MAX(CASE WHEN ete_grade='gross' THEN 1 ELSE 0 END)=1            THEN 'gross'
    WHEN MAX(CASE WHEN ete_grade='microscopic' THEN 1 ELSE 0 END)=1      THEN 'microscopic'
    WHEN MAX(CASE WHEN ete_grade='none' THEN 1 ELSE 0 END)=1             THEN 'none'
    ELSE 'unable_to_determine'
  END AS ete_grade_resolved,
  STRING_AGG(DISTINCT ete_grade, ',' ORDER BY ete_grade) AS ete_grades_seen,
  STRING_AGG(DISTINCT ajcc8_implication, ',') AS ajcc8_implications_seen,
  STRING_AGG(NULLIF(evidence_quote,''), ' | ') AS evidence_quotes,
  COUNT(*) AS n_synoptic_rows_evaluated,
  MAX(extracted_at) AS last_extracted_at,
  MAX(llm_model) AS llm_model,
  'mig_63_ete_queue_closeout_20260427' AS build_script,
  CURRENT_TIMESTAMP AS build_ts
FROM parsed
GROUP BY research_id;

-- Mark queue rows resolved
UPDATE manuscript_workspace.cpm_ete_self_contradiction_queue_v1 q
SET status = 'resolved_by_mig_63',
    note   = COALESCE(note,'') || CASE
      WHEN r.ete_grade_resolved IS NOT NULL THEN '|mig_63=' || r.ete_grade_resolved
      ELSE '|mig_63=no_resolution' END
FROM main.canonical_ete_queue_resolution_v1 r
WHERE q.research_id::VARCHAR = r.research_id;
```

## Phase 4 — Layer mig_63 into the manuscript view

Append to migration 61 SQL or create `qc_framework_v1/migrations/63b_ete_resolved_v1_refresh.sql`:

```sql
-- Rebuild canonical_ete_event_resolved_v1 picking up the new mig_63 layer
-- Add new ete_grade_source value: 'queue_closeout_llm'

CREATE OR REPLACE VIEW manuscript_workspace.ete_manuscript_analytic_v5 AS
SELECT v4.*,
       qr.ete_grade_resolved AS mig63_queue_resolution,
       qr.evidence_quotes    AS mig63_evidence_quotes,
       CASE
         -- Trust the LLM closeout when v4 had no answer or had pm_disagreement
         WHEN v4.ete_grade_final_v4 IS NULL
              AND qr.ete_grade_resolved IN ('gross','microscopic','none')   THEN qr.ete_grade_resolved
         WHEN v4.ete_grade_pm_disagreement_flag
              AND qr.ete_grade_resolved IN ('gross','microscopic','none')   THEN qr.ete_grade_resolved
         WHEN v4.ete_grade_final_v4 = 'unspec_remaining'
              AND qr.ete_grade_resolved IN ('gross','microscopic','none')   THEN qr.ete_grade_resolved
         ELSE v4.ete_grade_final_v4
       END AS ete_grade_final_v5,
       CASE
         WHEN v4.ete_grade_final_v4 IS NULL
              AND qr.ete_grade_resolved IN ('gross','microscopic','none')   THEN 'queue_closeout_llm'
         WHEN v4.ete_grade_pm_disagreement_flag
              AND qr.ete_grade_resolved IN ('gross','microscopic','none')   THEN 'queue_closeout_llm'
         WHEN v4.ete_grade_final_v4 = 'unspec_remaining'
              AND qr.ete_grade_resolved IN ('gross','microscopic','none')   THEN 'queue_closeout_llm'
         ELSE v4.ete_grade_source_v4
       END AS ete_grade_source_v5
FROM manuscript_workspace.ete_manuscript_analytic_v4 v4
LEFT JOIN main.canonical_ete_queue_resolution_v1 qr
  ON qr.research_id = CAST(v4.research_id AS VARCHAR);

-- Refresh the materialized canonical
DROP TABLE IF EXISTS main.canonical_ete_event_resolved_v1;
CREATE TABLE main.canonical_ete_event_resolved_v1 AS
SELECT /* same projection as mig_61, swap _v4 fields for _v5 */ ...
FROM manuscript_workspace.ete_manuscript_analytic_v5 v5;
```

## Phase 5 — Acceptance probes (Claude/Logan run after mig_63 lands)

```sql
-- (a) Queue closeout coverage
SELECT
  COUNT(DISTINCT q.research_id) AS n_queue_pts,
  COUNT(DISTINCT r.research_id) AS n_resolved_pts,
  COUNT(DISTINCT r.research_id) FILTER (WHERE r.ete_grade_resolved IN ('gross','microscopic','none')) AS n_committed,
  COUNT(DISTINCT r.research_id) FILTER (WHERE r.ete_grade_resolved = 'unable_to_determine') AS n_unable
FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1 q
LEFT JOIN main.canonical_ete_queue_resolution_v1 r
  ON r.research_id = q.research_id::VARCHAR;
-- Target: n_committed / n_queue_pts ≥ 0.85.

-- (b) Effect on the manuscript view
SELECT
  COUNT(*) FILTER (WHERE open_self_contradiction_flag) AS still_open_contradiction,
  COUNT(*) FILTER (WHERE ete_grade IS NULL) AS still_no_ete_data,
  COUNT(*) FILTER (WHERE analytic_eligible) AS analytic_eligible
FROM main.canonical_ete_event_resolved_v1;
-- Target: still_open_contradiction = 0 (all queue rows now status='resolved_by_mig_63'),
--         still_no_ete_data ≤ 250, analytic_eligible ≥ 5,200.
```

## Deliverables checklist

- [ ] `scripts/420_ete_queue_closeout_build_snippets.py`
- [ ] `scripts/421_ete_queue_closeout_load_to_md.py`
- [ ] `prompts/ete_queue_closeout_v1.txt`
- [ ] LLM batch run on S8 — ~3,200 rows
- [ ] `qc_framework_v1/migrations/63_ete_queue_closeout_canonical.sql`
- [ ] `qc_framework_v1/migrations/63b_ete_resolved_v1_refresh.sql`
- [ ] `project_mig_63_ete_queue_closeout_closeout.md` with the Phase 5 numbers
- [ ] `qc_framework_v1/ISSUE_REGISTRY.md` updated to close `MANUSCRIPT_ETE_QUEUE_CLOSEOUT`
- [ ] Commit message: `mig_63: ETE self-contradiction queue closeout (2786 pts via qwen3:14b on S8) + manuscript_v5/canonical_ete_event_resolved refresh`

## Source-of-truth pointers

- Original Excel: `All Diagnoses & synoptic 12_1_2025.xlsx`, sheet `synoptics + Dx merged`
- DB mirror: `main.path_synoptics`
- Adjudication output table: `main.note_entities_llm_ete_queue_closeout_v1`
- Patient-level resolution: `main.canonical_ete_queue_resolution_v1`
- Final manuscript-facing column of record (post-mig_63): `main.canonical_ete_event_resolved_v1.ete_grade`
