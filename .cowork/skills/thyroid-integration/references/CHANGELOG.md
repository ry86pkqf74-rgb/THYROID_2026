# thyroid-integration skill — changelog

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
