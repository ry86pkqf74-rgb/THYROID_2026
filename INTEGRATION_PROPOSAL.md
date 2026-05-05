# Airtable + Linear + Claude Integration Proposal

**For:** THYROID_2026 / TGDC manuscript program
**Author:** Claude (Cowork)
**Date:** 2026-05-05
**Revision:** v3 (scaled to 90+ manuscripts, added feedback log tables, AI journal recommendation, pending auto-resolve, persistent skill)
**Status:** DRAFT — awaiting your approval before any setup is executed

---

## 1. Executive Summary

You want a fully-integrated system that handles four workflows:

1. **Data column verification & QA** across all your Excel/parquet sources
2. **Reconciliation issue tracking** (every finding becomes traceable work)
3. **Manuscript section tracking** (drafting, review, journal targeting)
4. **Cohort patient-level tracking** (PHI-safe research IDs only)

**Recommended pattern: Hybrid orchestration.**

- **Airtable** = system of record for static inventory (data sources, columns, cohort metadata, manuscript structure, override decisions). It holds *what exists*.
- **Linear** = system of record for work-in-flight (drafting tasks, QA tickets, review cycles, blockers). It holds *what's happening*.
- **Claude** = orchestrator. A daily scheduled task syncs the two via their MCP servers and runs cross-system AI passes (column classification, drift detection, manuscript section summaries).

This avoids the three failure modes of single-tool approaches: Airtable alone has no good task UX, Linear alone has no good data registry UX, and a custom middleware adds infrastructure you'd have to debug solo.

---

## 2. Critical Caveats — Read Before Approving

These came out of the AI feature research and shape every decision below.

### 2.1 HIPAA / PHI — CONFIRMED OK FOR THIS PROJECT

You confirmed: Airtable Enterprise + AI, Linear Enterprise, and `research_id` is fully de-identified. That clears the HIPAA gates on both products.

| System | Status |
|---|---|
| Airtable Enterprise + AI | ✓ HIPAA-eligible. **Caveat still applies:** Airtable's terms forbid storing PHI in workspaces with AI enabled. Because `research_id` is already de-identified per Safe Harbor and no raw note text leaves DuckDB, we're inside the rules. |
| Linear Enterprise | ✓ HIPAA-eligible with BAA. Same de-identification rule applies. |

**Hard rule we'll bake in:** never push raw pathology text excerpts, operative notes, or anything else that could be PHI into either tool. Override decisions reference `research_id` and a Claude-summarized finding only; the evidence text stays in DuckDB / your local files.

**What this means for you:** if `research_id` values in your existing schema are *fully de-identified* (HIPAA Safe Harbor: no MRN, no name, no DOB beyond year, no dates of service narrower than year, no zip beyond first 3 digits, etc.), then non-Enterprise plans of Airtable + Linear are usable because you'd be storing only de-identified data + study metadata. Looking at your reconciliation report, you already pseudonymize via `research_id` and your DuckDB master holds the linkage offline — that's the right architecture for this approach.

**Hard rule we'll bake in:** never push raw pathology text excerpts, operative notes, or anything else that could be PHI into either tool. Override decisions reference `research_id` and a high-level finding only; the evidence text stays in DuckDB / your local files.

If your IRB requires you to treat *any* data in these tools as PHI-equivalent, you'll need Enterprise + BAA on both — please confirm before I proceed.

### 2.2 Plan tiers — already covered

You confirmed Airtable Enterprise + AI and Linear Enterprise. Every AI feature in this design is unlocked. No tier upgrades needed. Only ongoing cost is Claude orchestration tokens (~$5–10/mo at this scale).

### 2.3 No native Airtable↔Linear connector exists

Both products ship MCP servers (good), but neither has a first-party sync to the other. We use Claude as the bridge. No third-party SaaS (Zapier, Unito) needed.

---

## 3. Proposed Airtable Workspace

Two bases, distinct purposes.

### Base A: **THYROID_DATA_REGISTRY**

The single source of truth for what data exists, where, in what shape, and verified by whom.

| Table | Purpose | Key fields |
|---|---|---|
| **Source Files** | One row per raw Excel / parquet / DuckDB table | filename, domain (Pathology, FNA, US, CT, MRI, NucMed, Labs, Surgery, Demographics), row count, last modified, owner, ingest date, ingest notes, status [Active / Archived / Deprecated], DVC hash |
| **Columns** | One row per column per source file | column_name, source_file (link), data_type, allowed_values, business_definition, AI-generated description, verification_status [Unchecked / In QA / Verified / Disputed], owner, last_verified, gold_standard_value, links to verification checks |
| **Verification Checks** | One row per metric in your reconciliation matrix | metric_name (e.g. "Total patients (n)"), manuscript_value, db_value, verdict [MATCH / CLOSE / MISMATCH / IMPROVED / UNVERIFIABLE], severity, status, owner, fix_action, linked Linear issue |
| **Override Decisions** | Gold-standard chart-review overrides | research_id_pseudo, field, original_value, override_value, evidence_summary (NOT raw text), reviewer, decision_date, justification, linked manuscript section |
| **Cohort Patients** | Pseudo-IDs + availability metadata only | research_id, disease_group, malignancy_origin, has_us, has_ct, has_mri, has_fna, has_path, has_rai, n_sources, sources, included_in_manuscripts (multi-link), lifecycle |
| **Reconciliation Runs** | Each time you re-run your verification | run_date, n_cohort, n_malignant, n_match, n_close, n_mismatch, headline_findings, attached run report |
| **Issue Ledger** | Every Linear issue state transition mirrored from Linear via daily sync. Append-only, never deleted. | linear_issue_id, linear_url, linked airtable record, title, type, severity, state, transitioned_from, transitioned_to, transitioned_at, transition_actor, comment_summary, open_duration_minutes |
| **Manuscript Snapshots** | Immutable point-in-time freeze of evidence base when a manuscript hits Submitted / Accepted | snapshot_date, manuscript (link), trigger_event, n_cohort, n_malignant, attached snapshot bundle (JSON/parquet of all linked Verification Checks + Override Decisions + Cohort Patients at that instant), lifecycle = Locked |

**Lifecycle field** (added to Columns, Verification Checks, Override Decisions, Cohort Patients, and Manuscript Sections):

`Active → In QA → Verified → Finalized → Manuscript-Locked`

- **Active**: working state, fully editable
- **In QA**: open Linear issue exists
- **Verified**: QA closed, Claude has confirmed match
- **Finalized**: signed off, no further changes expected
- **Manuscript-Locked**: frozen as part of a Manuscript Snapshot. Airtable automation prevents AI Fields and any non-admin user from overwriting. Changes require explicit unlock from the Manuscript record.

### Base B: **THYROID_MANUSCRIPT**

This is the canonical inventory of your full pipeline (~90+ manuscripts at varying maturity). Linear only mirrors the *active* ones — see §4.

| Table | Purpose | Key fields |
|---|---|---|
| **Manuscripts** | One row per planned/active/submitted/published paper | code (M025, M048, H1, Mo36, etc.), short_title, full_title, status [Idea / Planned / Cohort Definition / Analysis / Drafting / Internal Review / Submitted / Revisions / Accepted / Published / Withdrawn / Backlog], aim, rationale, candidate_cohort_n, owner, IRB_number, study_dir (path), AI journal recommendation hierarchy (Claude-generated top 5 fits with rationale), journal_chosen [single-select: TBD / journal name], last_updated, links to sections, links to data feedback, links to manuscript feedback |
| **Sections** | Methods / Results / Tables / Figures / Limitations / Discussion / Abstract | manuscript (link), section_type, content_summary, draft_status, owner, last_updated, AI-generated readability score, blockers, linked Linear issue, lifecycle |
| **Tables & Figures** | Each numbered table/figure | manuscript (link), label, caption, source data file, generation_script, status, reviewer comments, last regenerated |
| **References** | Citation registry shared across all manuscripts | bibtex_key, title, authors, year, journal, DOI, used_in_manuscripts (multi-link) |
| **Co-Authors** | Team and review status | name, role, ORCID, manuscripts_owned (multi-link), review_status |
| **Submission Targets** | Journal pipeline per manuscript | manuscript (link), journal, scope_fit, IF, decision, response_due, next_action |
| **Manuscript Feedback Log** | Append-only log of every change you ask me to make to a manuscript | timestamp, manuscript (link), section (link, optional), change_type [edit / add / delete / restructure / clarify], your_request_summary, my_action_summary, before_excerpt, after_excerpt, source_chat (URL or session ID), lifecycle = always Logged (immutable) |
| **Data Feedback Log** | Same idea but for data/registry changes | timestamp, target [Column / Verification Check / Override Decision / Cohort Patient], record (link), change_type, your_request_summary, my_action_summary, before_value, after_value, source_chat, lifecycle = Logged |

---

## 4. Proposed Linear Workspace

### Team

Single team: **THYROID**.

### Projects — scaled to 90+ manuscripts

You said >90 manuscripts at various stages. We don't make 90 Linear projects — that's unmanageable. Instead, **the Manuscripts table in Airtable is the canonical inventory of all 90+; Linear only holds projects for *currently active* ones** (status ∈ {Cohort Definition, Analysis, Drafting, Internal Review, Submitted, Revisions}).

When a manuscript moves from Planned/Idea/Backlog → an active status, Claude auto-creates the Linear project. When it moves to Accepted/Published/Withdrawn, the Linear project is archived (not deleted). This keeps Linear focused on real work-in-flight.

**Workstream projects** (always-on, not tied to one paper) — created at scaffold time:

| Project | Purpose |
|---|---|
| **Database Reconciliation & QA** | Every Verification Check finding lands here |
| **Data Pipeline & Tooling** | DuckDB / parquet / notebook / extraction script maintenance, migrations (MIG_316–320 etc.) |
| **LLM Extraction & Refinement** | The phase4–phase11 extraction work, intrinsic eval, audits |
| **Cohort Curation** | Adds/drops, override decisions, chart-review work spanning all papers |
| **Submissions & Reviewer Defense** | Generic submission packaging, reviewer responses, revisions for any paper |
| **Manuscript Backlog Triage** | One project where ideas/planned manuscripts get periodically reviewed for promotion to active |

**Active manuscript projects** — created at scaffold time for manuscripts I found on disk that look genuinely active. Confirm or correct any:

| Manuscript Project | Code | Status (per your tracker / disk) | Source dir |
|---|---|---|---|
| ACR TI-RADS Operative Cohort | **M025** | Pending senior-author sign-off (ready to submit to *Thyroid*) | `THYROID_M025_v2.1_FINAL/` |
| Multimodal Prediction of Recurrence | — | Modeling complete, draft pending | `studies/proposal_multimodal_prediction_20260318/` |
| Mol + Imaging Discordance (Bethesda III/IV) | — | Planned | `studies/proposal_mol_imaging_discordance/` |
| Surgeon-Level Variability | — | Planned | `studies/proposal_surgeon_variability/` |
| ETE Staging & Recurrence (PSM) | — | Submitted — track revisions | `studies/proposal2_ete_staging/` |
| Central LN Dissection in Lobectomy | **H1** | Analysis complete | `studies/hypothesis1_cln_lobectomy/` |
| Goiter, Race & SDOH Disparities | **H2** | Analysis complete | `studies/hypothesis2_goiter_sdoh/` |
| 25yr ERA Stage Descriptive | **M032** | Submission package v1.0 staged | `M032_submission_package_v1_0/` |
| Cohort/LN Predictors | **M037** | Submission package scaffold | `studies/m037*` |
| AJCC ETE PSM / Recurrence | **M044** | Final package v6 | `M044_FINAL_PACKAGE_v6/` |
| Racial Disparities × TIRADS | **M048** | v3 active | `studies/m048_racial_disparities_tirads/v3/` |
| BRAF Discordance / Dual Platform | **M083** | Critical parser bug 5/5; active | `studies/m083_braf_discordance/` |
| (untitled) | **Mo36** | Manuscript v1 | `Mo36/Mo36_Manuscript_v1.md` |
| ATA RSS Comparison v3 | **M036** | Ready for writing as of 2026-05-05 | (no dir yet) |
| Massive Goiter / Parathyroid Adenoma | **M038** | Submission package v1.0 | `M038_submission_package_v1_0/` if exists |
| 2–4cm Extent / Molecular | — | Manuscript full draft v1 | `studies/proposal_2to4cm_extent_molecular_20260326/` |
| TGDC Primary | — | Reconciliation complete; drafting? | TGDC_*_REPORT.md |
| NSQIP-PTH Protocol | — | Active | `studies/nsqip_pth_protocol_manuscript/` |
| Lobectomy Molecular | — | Drafting | `studies/lobectomy_molecular_202603/` |

**Codes seeded as Manuscripts rows but NOT given Linear projects yet** (currently dormant on disk, waiting for status to advance):

`M004, M019, M027, M028, M029, M033, M043, M047`

**The other ~60 manuscripts you mentioned** — I don't have a list. Two ways to seed them:

- (a) You point me at the file/notes/spreadsheet where they live, I bulk-import.
- (b) You add them to the Manuscripts table in Airtable as you think of them; the daily sync will keep things consistent.

Tell me which in §7.

### Labels (color-coded)

- **type:** `data-finding`, `qa-check`, `manuscript`, `figure-table`, `infra`, `override-review`
- **severity:** `critical`, `high`, `medium`, `low`
- **source:** `airtable-sync`, `claude-flagged`, `manual`
- **section:** `methods`, `results`, `tables`, `figures`, `limitations`, `discussion`, `abstract`
- **stage:** `drafting`, `coauthor-review`, `revision`, `submitted`, `accepted`

### Custom workflow states

Backlog → **Awaiting Chart Review** → Todo → In Progress → **Awaiting Coauthor** → In Review → **Pending Auto-Close** → Done / Cancelled

**Pending Auto-Close** is the safety buffer you asked for. When a Verification Check resolves or a Section is accepted, Claude moves the linked Linear issue to Pending Auto-Close instead of closing it outright. After 48h with no objection, the daily sync transitions it to Done. You can override by:

- Commenting `/keep-open` on the issue → it returns to In Review
- Commenting `/close-now` → it closes immediately

We can shorten or remove the buffer once the system has proven itself for a couple of weeks.

### Issue templates

| Template | Use |
|---|---|
| Data Reconciliation Finding | Auto-created by Claude from any new Verification Check with verdict ≠ MATCH |
| Data Column QA | Per-column verification ticket |
| Override Decision Review | Coauthor sign-off on a gold-standard override |
| Manuscript Section Task | Drafting / revision work for a section |
| Figure / Table Task | Generation, caption, peer review |

---

## 5. Integration Mechanics

### Daily Claude scheduled task (single prompt, ~2 min runtime)

```
=== AUTO-LOG (Airtable → Linear) ===
1. Pull all Verification Checks where verdict ≠ MATCH and Linear_Issue_ID is empty.
2. For each, create a Linear issue in the right project (workstream or manuscript-specific
   based on the linked Manuscript) with:
   - title = "{metric_name}: {verdict}"
   - description = manuscript_value, db_value, severity, links back to the Airtable record
   - labels = type:data-finding + severity:* + source:airtable-sync
3. Write the Linear issue ID + URL back to the Airtable record.
4. Mirror new Cohort Override Decisions, Manuscript Section drafts, and Column QA needs
   the same way, into their matching project.

=== PENDING AUTO-RESOLVE (Airtable → Linear) ===
5. Find Airtable records whose Lifecycle just advanced to Verified / Finalized
   AND whose linked Linear issue is still open.
   Move those Linear issues to "Pending Auto-Close" state with a Claude-written
   comment citing: what changed, lifecycle transition, when, link back.
6. For issues already in "Pending Auto-Close" for >= 48h with no /keep-open comment,
   transition to Done. Honour /close-now to skip the wait.

=== AUTO-RESOLVE (Linear → Airtable) ===
7. Pull all Linear issues closed in last 24h with resolution label resolved-verified.
   Update the linked Airtable record:
     - Verification Check → status = Resolved, lifecycle = Verified
     - Manuscript Section → draft_status updated
     - Column → verification_status = Verified
   Issues closed with resolution label wont-fix or duplicate skip lifecycle advance.

=== ISSUE LEDGER (full audit trail) ===
8. For every issue created, updated, commented, or closed in last 24h, append a row
   to the Issue Ledger table in Airtable. Append-only — never delete or overwrite.
   Captures: issue_id, transition (e.g. todo → in_progress), timestamps, actor,
   open_duration_minutes when closed.

=== MANUSCRIPT LIFECYCLE ===
9. For each Manuscript with status changed since last run:
     - Idea / Planned → Active (any active sub-status): create Linear project for it,
       seed Sections + initial Linear issues from any existing draft files.
     - Accepted / Published / Withdrawn: archive Linear project (do not delete).

=== DRIFT DETECTION ===
10. Read the latest parquet/DuckDB schema. Compare to Airtable Columns.
    New / removed / renamed columns → create Verification Checks with severity:high.

=== AI JOURNAL RECOMMENDATION REFRESH ===
11. For any Manuscript with status ∈ {Drafting, Internal Review} and
    AI_journal_recommendation last updated >14d ago:
      - Recompute hierarchy of top 5 candidate journals from short_title + aim
        + cohort size + analysis type. Include scope_fit rationale + IF + likely
        editor preferences. Stamp with timestamp.

=== MANUSCRIPT SNAPSHOT (event-triggered, not daily) ===
12. If any Manuscript moved to status = Submitted or Accepted today:
    - Create a Manuscript Snapshot row.
    - Bundle current Verification Checks + Override Decisions + Cohort Patients
      linked to that manuscript into a JSON + parquet attachment.
    - Set lifecycle = Manuscript-Locked on every snapshotted record.
    - Lock those records against further AI-field overwrite.

=== DIGEST ===
13. Post a daily digest comment to a "Daily Sync" issue in Linear: counts of issues
    opened, moved to Pending Auto-Close, auto-closed, snapshots taken, drift
    findings, lifecycle transitions, journal-rec refreshes.
```

### Feedback logging behavior (every chat session, not just the daily sync)

Every time you ask me in a chat to change something — a manuscript section, a value in a Verification Check, an Override Decision, a column definition — I will, before completing the edit, append a row to the appropriate feedback log:

- Edits to manuscript content → **Manuscript Feedback Log**
- Edits to data, columns, verification, overrides, cohort metadata → **Data Feedback Log**

Each row captures: timestamp, target record, your_request_summary (a 1-line paraphrase of what you asked), my_action_summary, before/after excerpts, and a link to the source chat. The log is append-only and immutable. This gives you a permanent record of every change I make, attributable back to your request.

### One-time bootstrap pass

When we turn this on, Claude will:
- Read all 19 raw Excel files + processed parquets, populate the **Columns** table (this is where Airtable AI Fields earn their keep — auto-generating `business_definition` and `allowed_values` from the data itself).
- Import the verification matrix from `TGDC_VERIFICATION_REPORT.md` into **Verification Checks**.
- Import the override decisions from `TGDC_FINAL_RECONCILIATION_REPORT.md` into **Override Decisions**.
- Create initial Linear issues for the 5 unresolved action items at the bottom of the verification report (EMR demographics import, 14-patient gap, origin classification, Sistrunk parsing, RAI completion).

### ID-mapping, loop avoidance, and data preservation

- Each Airtable record stores its Linear Issue ID. The Issue Ledger is the audit-grade history.
- Linear issue descriptions embed the Airtable record URL.
- Claude only acts on records modified since last sync, never round-trips an update it just made.
- **Nothing is ever deleted in either system.**
  - Linear issues are closed, not deleted — closed issues remain queryable forever.
  - Airtable records archive to a `lifecycle = Archived` state instead of being removed.
  - Manuscript-Locked records cannot be edited at all without an explicit unlock action.
- The Issue Ledger captures every state transition, so even if Linear were lost the full audit trail lives in Airtable.

---

## 6. AI Feature Assignment Map

Who does what in this architecture:

| Need | Tool & feature |
|---|---|
| Auto-write column descriptions from sample data | **Airtable AI Field** on Columns table |
| Classify column verification severity | **Airtable AI Field** (single-select output) |
| Conversational exploration of the registry ("which Pathology columns are still unverified?") | **Airtable Omni** |
| Auto-suggest labels, dedupe, route incoming issues | **Linear Triage Intelligence** |
| Daily project status digest | **Linear Agent** + **AI summaries** |
| Capture findings from Slack/email into Linear | **Linear Asks** |
| Cross-system reconciliation, drift detection, structural QA on parquet schemas | **Claude scheduled task** (daily) |
| One-off bulk operations ("re-run reconciliation against new ingest") | **Claude on demand** in this chat |
| Manuscript section readability scoring + tone consistency | **Claude** invoked from Airtable automation via webhook |

---

## 6.5 Persistent skill (added per your request)

This integration is saved as a Cowork skill — `thyroid-integration` — so future Claude chats automatically know:

- The full Airtable + Linear schema (every table, field, lifecycle)
- The auto-log + pending-auto-resolve + feedback-log rules
- That every change to a manuscript or data record requires a feedback-log row
- The PHI rule (no raw note text in either tool)
- The skill triggers on keywords: `thyroid`, `airtable`, `linear`, `manuscript`, `verification check`, `override decision`, `data registry`, `M025/M032/M037/M044/M048/M083` and the H/Mo codes.

Effect: when you open a new chat next week and say "add the new BRAF discordance finding to M083" or "what's the lifecycle of the M044 ETE PSM cohort?", Claude immediately knows where things live and follows these rules without you re-explaining. The skill is updated whenever the architecture changes (you trigger it by asking me to update the integration; I edit the skill in place and bump its version).

---

## 7. Resolved Decisions and Remaining Open Items

**Resolved (your answers):**

1. ✓ Plans: Airtable Enterprise + AI, Linear Enterprise — covered.
2. ✓ PHI scope: `research_id` is de-identified; no raw note text in either tool.
3. ✓ Co-authors: skip for now, add later.
4. **Manuscript pipeline:** ~90+ planned. Architecture now treats Airtable Manuscripts table as the inventory of all 90+; Linear projects only for active ones (~19 confirmed from disk; see §4 table). The other ~60 — please tell me where they live (a notes file, a spreadsheet, your head) so I can bulk-import, OR you'll add them to Airtable as you go.
5. ✓ Journal targets: AI recommendation hierarchy column + journal_chosen column (TBD vs name). Manuscript Feedback Log + Data Feedback Log added.
6. ✓ Pending auto-resolve enabled (48h buffer, /keep-open and /close-now overrides).
7. ✓ MCP connector OAuth approved.

**Remaining (only one):**

**(A) Where are the other ~60 manuscripts?**

- Option a: Point me at a file/spreadsheet/notes location with the list.
- Option b: I scaffold the system with the 19 active + 8 dormant codes I found, and you add the rest to the Manuscripts table as you think of them (or paste them into a follow-up message and I bulk-import).

Pick one and reply, then I'll execute steps 1–10 in §8.

---

## 8. Execution Sequence (once you approve)

Roughly in order, with checkpoints:

1. **Connectors (manual step — Cowork registry doesn't have these for you yet):**
   - **Linear MCP:** Cowork Settings → MCP / Connectors → Add custom server →
     URL: `https://mcp.linear.app/mcp`, transport: HTTP/SSE, auth: OAuth 2.1 (you'll be prompted to log into Linear). Linear's MCP is officially hosted; just OAuth, no credentials to manage.
   - **Airtable MCP:** Two options.
     - (a) If Airtable Enterprise has rolled out their hosted MCP server to your workspace, use Cowork Settings → Add custom server → URL Airtable provides → OAuth.
     - (b) Otherwise install the community Airtable MCP via NPM (`@modelcontextprotocol/server-airtable` or `airtable-mcp-server`) and configure with a Personal Access Token from Airtable.com → Developer Hub → Create Token (scopes: `data.records:read`, `data.records:write`, `schema.bases:read`, `schema.bases:write`). Tell me which option you used so I know how to call it.
   - Verify Claude can read/write both: I'll ping each with a simple read after you confirm.
2. **Linear scaffolding:** Create THYROID team, the 4 projects, labels, custom workflow states, issue templates.
3. **Airtable scaffolding:** Build both bases with all tables and fields. Hand-tuned, not Cobuilder, so the schema matches your reconciliation reports exactly.
4. **AI fields:** Configure AI Fields on Columns + Verification Checks tables.
5. **Bootstrap import:** Run the one-time pass that loads Source Files, Columns, Verification Checks, Override Decisions, Cohort Patients from your existing data.
6. **Linear bootstrap:** Create issues for the 5 unresolved action items.
7. **Sync prompt:** Write and dry-run the daily Claude prompt against test records.
8. **Schedule:** Set up the daily scheduled task (you approve trigger time).
9. **Documentation:** Write a short `INTEGRATION_RUNBOOK.md` so future-you knows how it works.
10. **Verification pass:** Spot-check 10 random Airtable rows have correct Linear backlinks and vice versa.

Each step is checkpointable — if anything looks wrong, we stop and adjust before moving on.

---

## 9. What I Will Not Do Without Asking

- Push any free-text patient note, operative note, or pathology excerpt into either tool.
- Create issues that cascade-close other issues.
- Auto-delete records on either side.
- Enable AI on a base that contains anything PHI-equivalent.
- Run a destructive bulk operation without showing you a dry-run first.

---

*Reply with approvals / edits to the open decisions in §7 and I'll proceed step by step.*
