# thyroid-integration skill — changelog

## v1.6.0 — 2026-05-08

Phase A.3 TI-RADS primitive backfill landed via hybrid regex → Flash → Pro approach.

- **A.3 hybrid pivot:** `ML.GENERATE_TEXT` with `response_schema` was blocked; `AI.GENERATE_TABLE` on Pro for all 37k rows exceeded budget. Logan approved Option C (hybrid) 2026-05-07. Three tiers: regex (script 411, free, 87.1% coverage), Gemini 2.5 Flash (script 412, ~16k residual rows), Gemini 2.5 Pro (script 412, ~1.5–2.5k re-route rows).
- **New scripts:** `scripts/411_tirads_primitive_regex_v1.py` (Tier 1 extractor + 67-test suite), `scripts/412_tirads_hybrid_pipeline.py` (C.2–C.9 orchestrator with cost guardrails).
- **New BQ tables:** `tirads_primitive_regex_v1_v1`, `tirads_primitive_residual_v1`, `tirads_primitive_flash_raw_v1`, `tirads_primitive_pro_reroute_v1`, `tirads_primitive_pro_raw_v1`, `note_entities_llm_us_nodule_primitives_hybrid_v1`, `gemini_25_flash` model.
- **Canonical impact:** `pub_canonical.canonical_us_nodule_v2` rebuilt with 20 new primitive backfill columns (composition_llm, echogenicity_llm, shape_llm, margins_llm, echogenic_foci_llm_jsonarray, halo_jsonb, vascularity_jsonb, ete_us_jsonb, and provenance). COALESCE existing-wins applied.
- **Cost guardrails:** Flash full-run extrapolation ≤ $80; Pro re-route extrapolation ≤ $40; total A.3 ≤ $60. Pipeline halts if any cap is breached.
- **PHI guard:** evidence_short ≤ 140 chars enforced at C.7 merge; overlong rows truncated or quarantined to `qc_phase_a_parse_failures_v1`.
- **Logged via:** DFL A.3 row flipped to `Applied`. THY-30 comment posted with hybrid breakdown.

## v1.5.0 — 2026-05-07

MotherDuck cloud trial expired; BigQuery is the only canonical layer.

- **`SKILL.md` description:** Replaced "thyroid_master, parquet" trigger fragment with "BigQuery, BQ, pub_canonical, pub_workspace, parquet, MIG_, mig_". Updated the (b) load-trigger from "opens/queries/modifies thyroid_master.duckdb" to BigQuery dataset references.
- **Hard rule #1 (PHI):** Reworded so PHI lives in **local PHI-restricted files** (8/11/25 Excel, local note-text caches) rather than "DuckDB and local files". Clarified that the BQ canonical layer holds only de-identified `research_id`-keyed data per HIPAA Safe Harbor.
- **Why this exists section:** Replaced "evolving DuckDB master" with "evolving BigQuery canonical layer (`pub_canonical.*`, `pub_workspace.*`, `pub_signoff.*`)" and added a one-sentence note that the MotherDuck migration is complete.
- **Daily sync phase 7 (drift detection):** Updated to "parquet / BigQuery (`pub_canonical`, `pub_workspace`) schema vs Columns table".
- **`CLAUDE.md`:** Same canonical-layer changes propagated. Trigger list now references BigQuery / pub_canonical / pub_workspace / pub_signoff. Hard rule #1 PHI language reworded to match SKILL.md. The "Master analytical store" line now points to BigQuery and notes the MotherDuck trial expiration.
- **Logged via:** DFL-20260507-005 (Data Feedback Log). No edits to airtable_ids.md, linear_ids.md, daily_sync_prompt.md, or schema files — those were already BQ-anchored.

## Reference inventory v1.1.0 / `CLAUDE.md` sync — 2026-05-06

- **Manuscript inventory:** Regenerated `references/manuscript_inventory.md` from `pub_workspace.manuscript_feasibility_v1` (83 manuscripts; mirrored in repo-root `manuscript_feasibility_full_20260506.csv`). Added verified status counts and a full table (code, title, status, feasibility color). Bumped inventory snapshot header to skill reference **v1.1.0**.
- **`CLAUDE.md`:** Corrected feedback-log placement (both logs only in THYROID_MANUSCRIPT, with explicit table IDs) and replaced stale “~90+ planned manuscripts” language with the MD-migrated feasibility inventory counts (83 total; 27 scaffolded in Airtable / 56 pending backfill).

## v1.4.0 — 2026-05-05 (later same day)

Tightened triggering and added a Session Opening Protocol.

- Description list now covers manuscript-writing verbs (draft, abstract, methods, results, limitations, discussion, figure, table, caption, reviewer response, revision, submission), all M-codes individually, clinical terms (Bethesda, TIRADS, BRAF, RAI, ETE, Sistrunk, etc.), and architecture identifiers (ai_description, ai_readability_score, journal_chosen, thyroid_master.duckdb, parquet, MIG_).
- Added a 6-step **Session Opening Protocol** that runs before any other response when the skill triggers in a fresh session: verify connectors, read target record state, check lifecycle gates, pull recent ledger, status sanity-check, propose new Manuscripts rows for unfamiliar references, then write Feedback Log row before editing.
- Added a decision tree clarifying when the protocol fires vs when a request is purely educational.
- Created `THYROID_2026/CLAUDE.md` as a fallback project-context file so the integration is honored even if the skill itself didn't load.

## v1.3.0 — 2026-05-05 (later same day)

THY-9 resolved via Chrome MCP automation.

- All 4 multilineText fields converted to Field Agents (Airtable AI Fields):
  - `Columns.ai_description` (auto-gen on column_name, source_file, data_type)
  - `Columns.allowed_values` (auto-gen on column_name, data_type)
  - `Manuscripts.ai_journal_recommendation` (auto-gen on short_title, aim, candidate_cohort_n, journal_chosen)
  - `Sections.ai_readability_score` (auto-gen on content_summary)
- Each prompt enforces the no-PHI rule and references upstream fields via @ chips.
- Closed THY-9 with `resolution:resolved-verified`.

Lesson learned for future Field Agent edits: Airtable's Add field button inserts the @ at current cursor position. Place the cursor explicitly at end of textarea (Cmd+End is unreliable in their contenteditable; click the visible end-of-text instead) before clicking Add field.

## v1.2.0 — 2026-05-05 (later same day)

THY-10 resolved without manual UI work.

- Replaced the three planned custom workflow states (Awaiting Chart Review, Awaiting Coauthor, Pending Auto-Close) with team-scoped labels: `awaiting:chart-review`, `awaiting:coauthor`, `auto-close:pending`.
- Updated daily_sync_prompt.md so phases 2-3 watch the `auto-close:pending` label rather than a state name.
- Closed THY-10 with `resolution:resolved-verified`.

Why labels won: filterable, audit-trail-preserving, no state-creation API needed, easy to evolve.

## v1.1.0 — 2026-05-05 (live system)

System is live. Live IDs in `airtable_ids.md` and `linear_ids.md`.

- 2 Airtable bases scaffolded: THYROID_DATA_REGISTRY (9 tables), THYROID_MANUSCRIPT (7 tables)
- 27 Manuscripts seeded, 22 Source Files, 21 TGDC Verification Checks, 2 Reconciliation Runs
- Linear team Thyroid Database (THY) created with 25 projects (6 workstream + 19 active manuscript)
- 32 team-scoped labels created
- 10 initial issues filed (THY-1 through THY-10), with Linear URLs cross-stamped onto Airtable Verification Checks and Manuscript records
- Scheduled daily sync (`thyroid-daily-sync`) live at 0 7 * * * local time
- Outstanding manual UI tasks: convert AI Fields (THY-9), add 3 custom workflow states (THY-10)

## v1.0.0 — 2026-05-05

Initial skill. Captures:
- Two Airtable bases (DATA_REGISTRY, MANUSCRIPT) with 13 tables total
- Linear team THYROID with 6 workstream projects + per-active-manuscript projects
- Lifecycle field (Active → In QA → Verified → Finalized → Manuscript-Locked → Archived)
- Issue Ledger (append-only audit trail)
- Manuscript Feedback Log + Data Feedback Log (append-only, for every chat-driven edit)
- Manuscript Snapshots (immutable evidence freeze on Submit/Accept)
- 10-phase daily sync prompt with Pending Auto-Close 48h buffer
- AI journal recommendation refresh (14-day cadence)
- 19 confirmed active manuscripts, 8 dormant M-codes, room for ~60 more
- HIPAA rule (research_id only, no raw note text in either tool)

Open seams that future versions will need to address:
- The other ~60 manuscripts the user mentioned but isn't yet listed on disk
- Co-author seat allocation (deferred per user)
- Possible future migration from per-day sync to event-driven webhooks if latency matters
