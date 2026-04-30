# Cursor Prompt — mig_194 thyroid US NLP source unblock (mig_189 prerequisite)

**Date:** 2026-04-30
**Lane:** mig_194 / thyroid_us_nlp_source_unblock_scoping
**Batch (proposed):** `mig_194_thyroid_us_nlp_source_unblock_20260430`
**Predecessor:** mig_189 (`a3d1091`) — blocked because `clinical_note_thyroid_us_extracted_v1` does not exist on MD.
**Posture:** **READ-ONLY scoping + diagnostic.** No execute against MotherDuck.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only); produces options + recommended path

---

## Background — Cowork live-MD probe found mig_189 blocker

Cowork verified at HEAD `f3d8d5d` that `information_schema.tables WHERE table_name LIKE 'clinical_note%'` returns only:
- `clinical_note_ln_extracted_v1` (used by mig_171b)
- `clinical_notes_long`

**`clinical_note_thyroid_us_extracted_v1` does NOT exist.** mig_189's §0d gate would fail at probe time, and §B-§F DDL would error on missing table.

This is a hard prerequisite. mig_194 investigates the unblock surface and proposes 3 ratification options.

---

## Mission

Investigate what NLP infrastructure exists for thyroid US notes, surface unblock options, recommend a path. Output a Logan-ratification decision card, NOT a build SQL.

---

## Required scope

### §1 NLP infrastructure inventory

Read-only probes against MD + repo:

1. **Raw note tables** — what raw notes are available for thyroid US extraction? Probe `information_schema.tables` for tables containing thyroid US reports (`raw.ultrasound_reports`, `raw.us_reports`, etc.).
2. **Existing canonical_us_thyroid_gland_v2** — what columns does it have? Are there free-text or NLP-derived columns indicating prior NLP work? (Cowork already confirmed: 13,578 rows / 10,859 patients via mig_117 lineage.)
3. **Script 364** — read `scripts/364_canonical_us_thyroid_gland_v2.py`. Does it ingest ANY NLP-derived data, or is it strictly structured-only?
4. **NLP extractor pattern** — read how `clinical_note_ln_extracted_v1` was built. Look in `scripts/` for the script that produced it (likely a script that runs an LLM extractor on raw notes). Understand: model used, prompt template, output schema, runtime.
5. **note_entities_llm_*** — `information_schema.tables WHERE table_name LIKE 'note_entities_llm%'`. Are there per-domain LLM extraction tables? If so, is there one for thyroid US gland findings, or only LN findings?
6. **runs/** — repo subfolder. Are there runs for thyroid US gland extraction? Or only for LN / invasion / pathology / TIRADS?
7. **clinical_note_ln_extracted_v1 schema** — what columns? (research_id, exam_date, evidence_text, finding_type, etc.)

### §2 Decision matrix

Based on §1, classify mig_189's unblock surface into one of three options:

**Option A — Build NLP source from scratch (heavy lift)**
- Requires: LLM extraction infrastructure setup, prompt design for gland parenchyma findings (heterogeneity, hashimoto, vasculature, calcs, etc.), runtime budget for ~13k+ notes
- Estimated effort: 2-5 days of NLP runtime + prompt iteration + QC
- Pros: closes CF-117-US-GLAND-PARENCHYMA properly with NLP-supplemental events
- Cons: time-consuming; may not be worth manuscript priority

**Option B — Shell-only build (drop NLP supplemental)**
- Rewrite mig_189 to build `canonical_us_thyroid_gland_events_v2` from `canonical_us_thyroid_gland_v2` shell only (no NLP supplemental events)
- Add `exam_id_source ∈ {structured, fallback}` only (no `nlp_supplemental`)
- 10-gate validation modified: G7 expects only 2 source values, G8 always WARN (no NLP rows to test)
- Pros: unblocks CF-117 closure trace at minimal effort; still produces events + rollup tables
- Cons: doesn't surface NLP-only gland findings (e.g., hashimoto mentioned in note but not in structured shell)

**Option C — Cancel mig_189 (defer indefinitely)**
- Leave CF-117-US-GLAND-PARENCHYMA tagged-but-verified; document in manuscript appendix as data-source limitation
- Pros: zero effort; CF-117 isn't manuscript-blocking
- Cons: leaves 28 cols flagged with stale CF; gland family stays at v1 while LN is v2

### §3 Recommendation

For each option, document:
- Effort estimate (hours/days)
- Manuscript impact (does it change any analysis denominator or definition?)
- Operational risk (does it touch verified canonicals?)
- Path forward if Logan ratifies

Cowork suggests **Option B** as the pragmatic default (closes CF-117 with minimal effort; if NLP-supplemental gland findings turn out to matter for the manuscript, can be added later via a future mig_18Xb lane).

### §4 Audit/report

Author `qc_framework_v1/reports/mig_194_thyroid_us_nlp_source_unblock_20260430.md`:
- §1 NLP infrastructure inventory (live MD + repo findings)
- §2 Decision matrix (3 options)
- §3 Recommendation
- §4 What Cowork applies if Logan picks each option (sketch — full apply prompt is a follow-up lane mig_194_apply_<option>)

### §5 Mark scoping READY

Header: `# READ-ONLY SCOPING; LOGAN RATIFICATION REQUIRED BEFORE ANY APPLY`

---

## Governance reminders

- Read-only investigation only. No `query_rw`.
- Author = `Logan Glosser <logan.glosser@gmail.com>`.
- Do NOT modify mig_189 SQL — preserve as historical artifact. Future apply lane will supersede.

---

## Deliverables

1. `qc_framework_v1/reports/mig_194_thyroid_us_nlp_source_unblock_20260430.md`
2. `exports/mig194_nlp_source_inventory_20260430/note_tables_inventory.csv`
3. `exports/mig194_nlp_source_inventory_20260430/script_364_lineage_trace.csv`
4. `exports/mig194_nlp_source_inventory_20260430/manifest.json`

Commit message: `qc: mig_194 thyroid US NLP source unblock scoping (mig_189 prerequisite blocker; 3 options surfaced; Cowork recommends Option B shell-only; pending Logan ratification)`

---

End of prompt.
