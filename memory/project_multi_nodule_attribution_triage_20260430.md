# Project memory — mig_222 multi-nodule attribution triage (2026-04-30)

Lane F (`mig_222_multi_nodule_under_explosion_triage_20260430`) triaged:

- 448 multi-nodule under-explosion candidate exams from `manuscript_workspace.qc_tir03_llm_candidates_v1`
- 825 deferred LLM absorption patients from `manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1`

Conservative rule: **do not bulk-absorb LLM-derived nodule features without deterministic per-nodule attribution.** The live publication DB had exam/patient-level queues but no live per-nodule LLM mapping table (`tirads_llm_extracted_v2` absent), so all queued items were categorized as documented limitation / not safely absorbable.

DB changes:

- Added `main.canonical_us_nodule_v2.multi_nodule_attribution_unresolved BOOLEAN DEFAULT FALSE`.
- Created `manuscript_workspace.us_multi_nodule_attribution_triage_v1` as durable triage ledger.
- Flagged ~10,570 canonical nodule rows from queued exams/patients.
- Emptied both source QC queues after archive snapshots + triage ledger.

Manuscript rule: for nodule-level TIRADS phenotype analyses, exclude or sensitivity-stratify rows where `multi_nodule_attribution_unresolved IS TRUE` when exact per-nodule attribution is required. Patient/exam-level summaries may retain them with limitation wording.
