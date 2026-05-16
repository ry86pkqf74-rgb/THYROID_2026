# THYROID_2026 — Claude Project Context

This file is read automatically by Claude when working in this project. It guarantees the integration architecture is honored even when the `thyroid-integration` skill hasn't loaded.

**If you're a Claude session reading this for the first time: load the `thyroid-integration` skill RIGHT NOW. Everything below is a summary; the skill has the authoritative version.**

## What this project is

Logan's multi-year thyroid surgical / pathological / molecular research database, with **83 manuscripts** in `pub_workspace.manuscript_feasibility_v1` (MD-migrated master inventory). Of these, **27** are scaffolded into Airtable Manuscripts and **56** await scaffolding via the manuscripts backfill (see `manuscript_feasibility_full_20260506.csv`). Master analytical store is `thyroid_master.duckdb`. Full code list (M001–M083): `.cowork/skills/thyroid-integration/references/manuscript_inventory.md`.

## The integration

| Where | What | ID |
|---|---|---|
| Airtable workspace | "My First Workspace" | `wspDGHtW2HNuT20GQ` |
| Airtable base A | THYROID_DATA_REGISTRY (9 tables) | `appTGeB1jIizZbjnw` |
| Airtable base B | THYROID_MANUSCRIPT (7 tables) | `appJYOnUb7KrHKwpV` |
| Linear team | Thyroid Database (key THY) | `c4afb51b-8bca-413a-a53e-15eb825cffbd` |
| Daily sync anchor issue | THY-6 | https://linear.app/rostemp/issue/THY-6/ |
| Scheduled task | `thyroid-daily-sync` | runs daily at 7:04 AM local |

**Feedback log table locations (explicit, to prevent confusion):** Both feedback logs live in **THYROID_MANUSCRIPT** (`appJYOnUb7KrHKwpV`). **Data Feedback Log** table = `tblsiYKJtKcktkzze`; **Manuscript Feedback Log** = `tblYSCBzRFC4RGPMq`. All data-verification/BQ-infrastructure edits log to **Data Feedback Log** here — not in THYROID_DATA_REGISTRY (base A).

Full ID reference: `.cowork/skills/thyroid-integration/references/airtable_ids.md` and `linear_ids.md`.

## Hard rules — never break these

1. **No PHI in Airtable or Linear, ever.** `research_id` only. Pathology text excerpts, operative notes, MRNs, dates of service narrower than year, names, and DOB beyond year all stay in DuckDB and local files. Evidence in Override Decisions = Claude-summarized 1-2 sentences, never raw text.

2. **Nothing is ever deleted.** Linear issues close, never delete. Airtable records archive (`lifecycle = Archived`), never delete. Manuscript-Locked records cannot be edited at all without explicit unlock.

3. **Every change at user's request gets logged BEFORE the change.** Manuscript content edit → row in `Manuscript Feedback Log`. Data/column/verification/override/cohort edit → row in `Data Feedback Log`. The log row is created first; if logging fails, the edit doesn't happen.

4. **Pending Auto-Close, not auto-close.** When a Verification Check or Section reaches Verified/Finalized, set the linked Linear issue to state `In Review` and add label `auto-close:pending`. After 48h with no `/keep-open`, transition to Done with `resolution:resolved-verified`. `/close-now` skips the wait.

## Session Opening Protocol — RUN FIRST every fresh session

Before any other response when this project is touched:

1. Verify Airtable + Linear MCP connectors are alive.
2. Identify target manuscript / record and read its current state from Airtable.
3. Check `lifecycle` — if `Manuscript-Locked`, refuse edit until unlocked.
4. Pull last 24h of Issue Ledger for that record.
5. Sanity-check status (Drafting? Submitted? Accepted?) against what user is implying.
6. If user references a manuscript not yet in the Manuscripts table, propose adding it.
7. Append Feedback Log row, THEN edit, THEN update linked Linear issue.

## Triggers that should make you load `thyroid-integration` skill

Any of: thyroid, TGDC, M-codes (M001–M999), Mo36, H1, H2, manuscript, draft, abstract, methods, results, limitations, discussion, figure, table, caption, reviewer response, revision, submission, journal, IRB, cohort, research_id, Verification Check, Override Decision, Manuscript Snapshot, reconciliation, data registry, manuscript feedback, data feedback, lifecycle, Finalized, Manuscript-Locked, Pending Auto-Close, Issue Ledger, AI journal recommendation, ai_description, ai_readability_score, journal_chosen, thyroid_master.duckdb, parquet, MIG_, Sistrunk, Bethesda, TIRADS, ThyroSeq, Afirma, BRAF, RAI, ETE, hypocalcemia, RLN, papillary, follicular, Hurthle, parathyroid, goiter.

## What NOT to do

- Don't push raw note text into Airtable or Linear.
- Don't skip the Session Opening Protocol because the request "looks small".
- Don't edit a Manuscript-Locked record — require explicit unlock.
- Don't delete records on either side; close/archive instead.
- Don't create a Linear project for an Idea/Planned/Backlog manuscript — wait until status advances to active.
- Don't bulk-create issues without first checking Issue Ledger for duplicates.
- Don't change schema (add/rename/drop fields) without bumping skill version and updating `airtable_ids.md`.

## MLX local extraction stack (added 2026-05-16)

For any task that involves extracting structured data from clinical free text — pathology, molecular, ultrasound, imaging, FNA, complications, cause-of-death, risk factors — load the `thyroid-mlx-extract` skill at `.cowork/skills/thyroid-mlx-extract/`. It encodes:

- Model selection rules per task tier (MedGemma 1.5 27B for templated medical, Llama 3.3 70B for hard semantics, DeepSeek-R1 distill for adjudication)
- Gold-set evaluation discipline before any corpus run
- Workspace-first BQ writes preserving the `note_entities_llm_*` provenance pattern
- **Philter (NOT an LLM) as the only acceptable de-identification tool**

The runnable harness lives at `tools/thyroid_mlx_extract/` with a CLI: `thyroid-mlx pull|eval|run|push <task>`. Empirical gap analysis and the comprehensive model-task matrix are in `docs/mlx/`.

Triggers for this skill: MLX, on-device, local LLM, Llama 3.3, MedGemma, Qwen3, extraction harness, Ki-67, mitotic count, capsular invasion, ETE grade, ENE, raw_payload_json, synoptic_diagnosis, Bethesda subcategory, halo, microcalcification, hypoparathyroidism subtyping, RLN injury, cause of death, childhood radiation, ThyroSeq, Afirma.

## Where to find more

- Architecture: `INTEGRATION_PROPOSAL.md` (this folder)
- Skill: `.cowork/skills/thyroid-integration/SKILL.md`
- Schema: `.cowork/skills/thyroid-integration/references/airtable_schema.md`, `linear_schema.md`
- Daily sync prompt: `.cowork/skills/thyroid-integration/references/daily_sync_prompt.md`
- IDs: `.cowork/skills/thyroid-integration/references/airtable_ids.md`, `linear_ids.md`
- Manuscript inventory (snapshot): `.cowork/skills/thyroid-integration/references/manuscript_inventory.md`
- Changelog: `.cowork/skills/thyroid-integration/references/CHANGELOG.md`
- Reconciliation history: `TGDC_FINAL_RECONCILIATION_REPORT.md`, `TGDC_VERIFICATION_REPORT.md`
- Manuscript tracker (legacy text version): `MANUSCRIPT_TRACKER.md`
