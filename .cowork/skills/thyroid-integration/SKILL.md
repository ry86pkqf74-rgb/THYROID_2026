---
name: thyroid-integration
metadata:
  version: 2.3.2
description: |
  MANDATORY playbook for THYROID_2026 — Logan's thyroid research program's Airtable + Linear + BigQuery + Claude integration. Load IMMEDIATELY before any other response when the user (a) works in THYROID_2026, (b) opens/queries/modifies BigQuery (`thyroid-canonical-pub-2026.pub_canonical.*`, `pub_workspace.*`, `pub_signoff.*`), or any parquet/notebook, or (c) drafts/edits/discusses ANY thyroid manuscript section (abstract, methods, results, discussion, figures, tables, reviewer response). Triggers: thyroid, TGDC, M025, M032, M036, M037, M038, M044, M048, M083, M-codes, Mo36, H1, H2, MULTIMODAL, MOLIMG, SURGEON, ETE, NSQIP-PTH, LOBMOL, manuscript, draft, abstract, methods, results, figure, table, reviewer, journal, IRB, cohort, research_id, Verification Check, Override Decision, Manuscript Snapshot, reconciliation, lifecycle, Manuscript-Locked, Issue Ledger, BigQuery, BQ, pub_canonical, pub_workspace, parquet, MIG_, mig_, Bethesda, TIRADS, ThyroSeq, Afirma, BRAF, RLN, parathyroid. Narrow requests need this too — Session Opening Protocol must run FIRST. Skipping = broken audit trail or PHI violation.
---

# THYROID_2026 Integration Skill

You are operating inside Logan's THYROID_2026 research program. This skill is the authoritative source for how data, manuscripts, and tasks flow between Airtable, Linear, and the local DuckDB / parquet / notebook stack. Read it carefully before acting on any thyroid-related request.

## Session Opening Protocol — RUN BEFORE ANY OTHER RESPONSE

When this skill triggers in a fresh session, the very first thing you do — before answering, planning, or generating any output for the user — is run this checklist silently and concisely. Do NOT skip it, even when the request looks trivial.

1. **Verify connectors.** Confirm both MCP connectors are alive:
   - Airtable: call `list_workspaces` (or any cheap read). If it errors, surface to user and stop.
   - Linear: call `list_teams`. If it errors, surface to user and stop.
   If a connector is missing, tell the user how to reconnect (Cowork Settings → MCP / Connectors) before continuing.

2. **Identify the target manuscript and lifecycle state.** Parse the user's request to determine which Manuscripts row(s), Verification Check(s), Override Decision(s), or Column(s) are being touched. For each:
   - Read the Airtable record (use the IDs in `references/airtable_ids.md`).
   - Note its `lifecycle` value. If `Manuscript-Locked`, stop and require explicit unlock before any edit.
   - Note its `linked_linear_issue_id` and `linked_linear_project`.

3. **Check recent activity.** Pull the last 24h of Issue Ledger entries for any record you're about to touch — gives you context on what the daily sync just did.

4. **Status sanity check.** For each target manuscript:
   - Read its current `status` field. If the user's request implies a status change (e.g. they say "the M025 abstract is final" but Airtable still says `Drafting`), surface the discrepancy and ask whether to advance status before editing.
   - Check if any pending Verification Check would block the change.

5. **Confirm scope additions.** Skim the Manuscripts table. If the user mentions a manuscript code or topic NOT yet in the Manuscripts table, flag it and ask if they want a new row created (`status = Idea` by default).

6. **Pre-write the feedback log row, then act.** Once steps 1-5 are clean, append the appropriate Feedback Log row (Manuscript or Data) BEFORE making the actual edit. Then make the edit. Then update the linked Linear issue.

This protocol takes ~15 seconds and prevents 95% of audit-trail breaks. If the user explicitly says "skip the protocol, just do X," still run steps 1-2 silently and tell them you skipped 3-6 at their request.

## Decision tree: what counts as "touching" the project

| User says... | Skill triggers? | Open protocol runs? |
|---|---|---|
| "fix this typo in M025 methods" | yes | yes (a manuscript edit needs Manuscript Feedback Log) |
| "what's the cohort N for M044?" | yes | yes (a read still needs to confirm record state) |
| "regenerate Table 2 for M048" | yes | yes |
| "let's brainstorm a new paper on TI-RADS reproducibility" | yes | yes — likely creates a new Manuscripts row |
| "rebuild the parquet for ultrasound" | yes | yes — Source Files row update + drift detection |
| "explain Bethesda categories" (general medical knowledge) | no | no — pure educational answer, no project state involved |
| "what does FNAB stand for" | no | no |
| "show me a code example for OAuth" | no | no |

## Why this exists

Logan runs ~90+ planned manuscripts at varying maturity, an evolving **BigQuery canonical layer** (`thyroid-canonical-pub-2026.pub_canonical.*`, `pub_workspace.*`, `pub_signoff.*`) with dozens of source tables, and a multi-year reconciliation effort between manuscripts and database. (Legacy MotherDuck cloud trial expired; the BQ migration completed and BQ is now the only canonical layer. Any local `thyroid_master.duckdb` artifacts are historical reference only.) To prevent drift, every column, verification check, override decision, manuscript section, and cohort metadata record lives in a structured Airtable system. Active work-in-flight lives in Linear. Claude orchestrates daily sync, drift detection, and feedback logging.

If you bypass this system — e.g. silently edit a manuscript without logging the change, close a Linear issue without updating Airtable, or push raw clinical text into either tool — you break the audit trail and potentially the HIPAA posture. Don't do that.

## Hard rules (in order of importance)

1. **No PHI in Airtable or Linear, ever.** `research_id` is fine (de-identified per HIPAA Safe Harbor) and acceptable for the BigQuery canonical layer. Pathology text excerpts, operative notes, MRNs, dates of service narrower than year, names, and DOB beyond year all stay in **local PHI-restricted files** (e.g., the 8/11/25 surgical-pathology Excel and any local note-text caches). The BQ canonical layer holds only de-identified data; even there, evidence fields cite Claude-summarized 1–2 sentence summaries, never raw source text.

2. **Nothing is ever deleted.** Linear issues close, never delete. Airtable records archive (`lifecycle = Archived`), never delete. Manuscript-Locked records cannot be edited at all without explicit unlock. The Issue Ledger and the Manuscript/Data Feedback Logs are append-only.

3. **Every change you make at the user's request gets logged.** Before completing an edit to a manuscript section, append a row to **Manuscript Feedback Log**. Before completing an edit to data/columns/verification/overrides/cohort, append a row to **Data Feedback Log**. Both rows capture: timestamp, target record, your_request_summary (1-line paraphrase of what they asked), my_action_summary, before/after excerpts, source chat. The log row is created **before** the edit completes — if logging fails, the edit doesn't happen.

4. **Pending Auto-Close, not auto-close.** When a Verification Check or Section reaches Verified/Finalized, move the linked Linear issue to state `In Review` and add label `auto-close:pending` with a Claude comment explaining what changed. After 48h with the label still attached and no `/keep-open` comment, transition to Done with `resolution:resolved-verified`. `/close-now` skips the wait. Don't close issues directly. (Label-based mechanism replaces the originally-planned custom workflow state, which Linear's MCP doesn't expose. See THY-10.)

5. **Manuscripts table = canonical inventory of all 90+.** Linear projects only exist for *active* manuscripts (status ∈ {Cohort Definition, Analysis, Drafting, Internal Review, Submitted, Revisions}). When a manuscript advances from Idea/Planned/Backlog → an active status, auto-create the Linear project. When it reaches Accepted/Published/Withdrawn, archive the Linear project (don't delete).

## System layout

### Airtable

Two bases. See `references/airtable_schema.md` for full field list.

**THYROID_DATA_REGISTRY** — Source Files, Columns, Verification Checks, Override Decisions, Cohort Patients, Reconciliation Runs, Issue Ledger, Manuscript Snapshots.

**THYROID_MANUSCRIPT** — Manuscripts, Sections, Tables & Figures, References, Co-Authors, Submission Targets, Manuscript Feedback Log, Data Feedback Log.

### Linear

Single team **THYROID**. Workstream projects (Database Reconciliation & QA, Data Pipeline & Tooling, LLM Extraction & Refinement, Cohort Curation, Submissions & Reviewer Defense, Manuscript Backlog Triage) plus per-active-manuscript projects.

Workflow states: Linear defaults (Backlog → Todo → In Progress → In Review → Done / Canceled). Three "wait" semantics layer on as labels:

- `awaiting:chart-review` (use with state `Backlog`)
- `awaiting:coauthor` (use with state `In Review`)
- `auto-close:pending` (use with state `In Review` — daily sync watches this for the 48h auto-close buffer)

Labels: `type:*`, `severity:*`, `source:*`, `section:*`, `stage:*`. See `references/linear_schema.md`.

### Lifecycle field (applies to Columns, Verification Checks, Override Decisions, Cohort Patients, Manuscript Sections)

`Active → In QA → Verified → Finalized → Manuscript-Locked` (plus `Archived` as a parallel terminal state)

- **Active**: working state, fully editable
- **In QA**: open Linear issue exists
- **Verified**: QA closed, match confirmed
- **Finalized**: signed off, no further changes expected
- **Manuscript-Locked**: frozen as part of a Manuscript Snapshot — Airtable automation prevents AI Fields and non-admin users from overwriting. Unlock requires explicit user request that names the manuscript and the reason.

## Daily sync prompt

Run via scheduled task. Full prompt is in `references/daily_sync_prompt.md`. Phases:

1. AUTO-LOG (Airtable → Linear): new findings/sections/overrides/columns spawn issues
2. PENDING AUTO-RESOLVE (Airtable → Linear): lifecycle advance moves issue to Pending Auto-Close
3. PENDING AUTO-RESOLVE TIMEOUT: issues in Pending Auto-Close ≥48h transition to Done
4. AUTO-RESOLVE (Linear → Airtable): closed issues with `resolved-verified` advance lifecycle
5. ISSUE LEDGER: every transition appended (immutable)
6. MANUSCRIPT LIFECYCLE: status changes create/archive Linear projects
7. DRIFT DETECTION: parquet / BigQuery (`pub_canonical`, `pub_workspace`) schema vs Columns table
8. AI JOURNAL REC REFRESH: top-5 hierarchy for active manuscripts
9. MANUSCRIPT SNAPSHOT (event): freezes evidence base on Submitted/Accepted
10. DIGEST: comment to "Daily Sync" issue with counts

## Behavior in chat (every session) — after Session Opening Protocol

When the user asks you to change something in this system:

1. **Identify which logs apply.**
   - Manuscript content edit → Manuscript Feedback Log
   - Column / Verification Check / Override Decision / Cohort Patient / data registry edit → Data Feedback Log
   - Both apply if the edit spans manuscript + data (e.g. updating a Results section because a Verification Check changed)

2. **Append the log row first.** Capture the user's request verbatim or paraphrased, your planned action, the before-state, and the after-state. Include a link to the source chat if you have one.

3. **Check lifecycle gates.** If the target record is `Manuscript-Locked`, refuse the edit and explain: "M025 was locked when it was submitted on YYYY-MM-DD. Editing requires you to first unlock by saying 'unlock M025 because [reason]'." Do NOT edit through.

4. **Make the edit.**

5. **Update the linked Linear issue.** If the change resolves an issue, move it to Pending Auto-Close. If it creates new work, file a new issue.

6. **Acknowledge.** Tell the user what you did, link the Airtable record, link the Linear issue, link the log entry.

## Behavior in chat: when the user reports a finding

E.g. "I just noticed M044 is using the wrong cohort definition." Don't fix silently — open a Verification Check with severity, link it to M044, the daily sync will spawn a Linear issue. Then ask the user what evidence to attach (no PHI), and update the Verification Check with the summary.

## Notable findings — when and how to log

Anytime an analysis turns up something more than a routine audit number — a result that could change how a manuscript is framed, a generalization failure of a published model, an unexpected effect size, a contradiction of a prior internal finding, a new hypothesis worth following up — log it as a Notable Finding **before continuing other work**.

**Triggers (any of these = log):**
- A scoring/risk model trained elsewhere produces near-random discrimination (AUC ~0.5) on this cohort.
- Two methods that should agree disagree by ≥10 percentage points on a primary outcome metric.
- A subgroup analysis reveals an effect size that contradicts a published external comparison.
- A data-integrity finding that changes how an existing or planned manuscript should report numbers.
- A hypothesis-generating signal worth tracking even if not yet causal.

**Procedure (≤5 minutes):**
1. Append a row to Airtable Notable Findings table (base `appJYOnUb7KrHKwpV`, table `tbl7GL0eFSiNPwabW`). `finding_id = NF-YYYY-MM-DD-<short-slug>`. Severity per the options. Cross-link to `applies_to_manuscripts` (M-codes).
2. File a Linear issue under the `Notable Findings & Research Insights` project, label `type:notable-finding`. Title = the finding's title. Body = full finding details paraphrased, with links to evidence artifacts. Issue ID back-fills `linked_linear_issue_id` in Airtable.
3. If the finding applies to a Manuscript-Locked manuscript, ALSO append a Manuscript Feedback Log row noting the finding and its potential implications.
4. Reference the discovery session (chat ID or git commit) so the audit trail is reproducible.

**Severity ladder, with examples:**
- `informational` — a verified data-coverage number worth recording but not novel. (e.g., "ACR feature complete rate is 67% post-Phase A.3.")
- `hypothesis_generating` — a signal that's interesting and worth pursuing but hasn't been validated. (e.g., "Substernal goiter patients with chronic laryngeal symptoms show higher RLN injury rate in retrospective sample.")
- `publishable` — a finding strong enough to anchor a paper or a section of one. (e.g., the Park 2009 generalization failure, NF-2026-05-07-park2009-noncalibration.)
- `contradicts_prior` — a finding that contradicts an external publication or an internal prior finding. Always log; treat as a Verification Check trigger too.
- `clinically_actionable` — a finding that should change clinical practice if validated. Highest bar; route to coauthor review immediately.

**Field IDs for Airtable Notable Findings table `tbl7GL0eFSiNPwabW` (base `appJYOnUb7KrHKwpV`):**

| Field | Field ID |
|---|---|
| finding_id (primary) | `fldt70RW16S8Mqqya` |
| title | `fldn0rtUzEEH6WvIS` |
| date_found | `fldg9Y24IYhLq4BgN` |
| domain | `fldTHzzbIFnw15wNe` |
| severity | `fldZShM8LcMblw2oA` |
| finding_summary | `fldIgknZbDHJFxaPl` |
| evidence_summary | `fld3Ehs4XEaoixhzW` |
| supporting_artifacts | `fldwOlWchkHky5lzG` |
| applies_to_manuscripts | `fldBz1hv5Xxexp7iF` |
| linked_linear_issue_id | `fld9jJXIFANzY2IiR` |
| discovery_session | `fldvtR7mZinElKjCJ` |
| next_action | `fldxnEB2BuNB5g0BH` |
| lifecycle | `fldHA2e1rjylgW80c` |
| created_by | `fldUvw5I7sP0StM0d` |

## Canonical patient_master version lineage (2026-05-13)

Per the v2.3.0 build, the patient-master canonical now spans 7 versions; each is preserved (never deleted):

| Table | Cols | Built | Purpose |
|---|---:|---|---|
| `canonical_patient_master` (v1) | ~1,400 | pre-existing | Original baseline |
| `canonical_patient_master_v1_1` | ~1,650 | pre-existing | Extension |
| `canonical_patient_master_v1_2` | 1,749 | 2026-05-13 | Dimensions/volume + LN levels + multifocality fix + per-tumor laterality |
| `canonical_patient_master_v1_3` | 1,967 | 2026-05-13 | Synoptic full fields (218 cols) |
| `canonical_patient_master_v1_4` | 2,110 | 2026-05-13 | CT / MRI / Nuclear Med / Frozen Section / OP Sheet / Complications |
| `canonical_patient_master_v1_5` | 2,153 | 2026-05-13 | US TIRADS / FNAs / ThyroSeq+Afirma / Parathyroid intent |
| **`canonical_patient_master_v1_6`** | **2,233** | **2026-05-13** | **Imaging catalog / Tg+TgAb / Legacy path drift / Notes (PHI-safe)** |

**Always default to v1_6** for new analyses. v1, v1_1, v1_2..v1_5 retained for back-compat.

**Downstream selection conventions:**
- TIRADS: `tirads_resolved` (NOT `syn_us_tr_*`, which may be outdated)
- Bethesda: `syn_fna_bethesda_2023_max` (rescored, current)
- Thyroid weight: `gland_weight_final_g_v2` + `gland_weight_source_v2` provenance
- Total thyroid volume: `total_thyroid_volume_cc_v2` (new)
- Multifocality: `multifocality_flag_v2` (fixed; legacy `multifocal_flag_path` is NLP-partial)
- LN levels: `ln_level_*_examined_v2` (richer regex over `Tumor_1_LN location`)
- Per-tumor 3-5: `syn_tumor{3,4,5}_*` (newly exposed)
- Parathyroid gland 1-6: `syn_paraG_{1..6}_*`
- Mortality: combine `nsqip_death_30d` + `syn_notes_death_flag_present_in_notes`

See `outputs/M084_gland_measurement_backfill/v1_6_DATA_DICTIONARY_20260513.md` for full field reference.

## Versioning

This skill is at v2.3.2. Bump:
- **patch** (1.0.x) for clarification edits
- **minor** (1.x.0) for new tables, fields, or sync phases
- **major** (x.0.0) for changes to the hard rules or lifecycle states

When you change anything, also bump the `version` in the YAML frontmatter and append a one-line CHANGELOG entry to `references/CHANGELOG.md`.

### Skill version bumps — required pre-checks

**MANDATORY before any minor or major skill version bump.** Discovered via NF-2026-05-07-tirads-pipeline-version-state-mismatch: skill versions v1.7.0 and v1.9.0 were bumped asserting Phase B/C closure without verifying actual BQ state, creating a false closure record. This resulted in 5 of 11 TIRADS system columns being NULL at the Phase E halt.

Before bumping skill version, run the following verified-state check in BigQuery for any table cited in the proposed CHANGELOG closure claim:

```sql
-- Template: verify every table cited as "closed" in the CHANGELOG entry
SELECT
  table_name,
  row_count,
  column_count
FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.TABLE_STORAGE`
WHERE table_name IN (
  -- List every table the CHANGELOG claims is complete
  'canonical_us_nodule_tirads_multisystem_v1',
  -- etc.
)
ORDER BY table_name;

-- For each table, verify non-zero row counts on the specific columns that
-- the CHANGELOG claims are populated:
SELECT
  COUNTIF(acr2017_category_imputed IS NOT NULL) AS n_acr,
  COUNTIF(kwak_category IS NOT NULL) AS n_kwak,
  -- etc.
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_tirads_multisystem_v1`;
```

**Assertion:** Every closure claim in the proposed CHANGELOG entry MUST have a corresponding verified BQ count ≥ the stated minimum before the version bump is applied. If any count is 0 or below minimum, the CHANGELOG entry must be revised or the bump deferred until the gap is fixed.

## Files in this skill

- `SKILL.md` (this file) — operational playbook
- `references/airtable_schema.md` — full Airtable schema
- `references/linear_schema.md` — full Linear schema (projects, labels, states, templates)
- `references/daily_sync_prompt.md` — the verbatim sync prompt
- `references/CHANGELOG.md` — version history
- `references/manuscript_inventory.md` — current best-known list of all manuscripts on disk

## Quick reference: M-code → likely manuscript map (as of v1.0.0)

| Code | Status | Source |
|---|---|---|
| M025 | Pending sign-off → *Thyroid* submission | THYROID_M025_v2.1_FINAL/ |
| M032 | Submission package staged | M032_submission_package_v1_0/ |
| M036 | Ready for writing | (no dir yet) |
| M037 | Submission scaffold | studies/m037*/ |
| M038 | Submission package v1 | M038_submission_package_v1_0/ |
| M044 | Final package v6 | M044_FINAL_PACKAGE_v6/ |
| M048 | v3 active | studies/m048_racial_disparities_tirads/v3/ |
| M083 | Active, parser-bug 5/5 | studies/m083_braf_discordance/ |
| Mo36 | Manuscript v1 draft | Mo36/Mo36_Manuscript_v1.md |
| H1 | Analysis complete | studies/hypothesis1_cln_lobectomy/ |
| H2 | Analysis complete | studies/hypothesis2_goiter_sdoh/ |
| TGDC primary | Reconciliation done; drafting? | TGDC_*_REPORT.md |
| Multimodal Prediction | Modeling complete, draft pending | studies/proposal_multimodal_prediction_20260318/ |
| Mol+Imaging Discordance | Planned | studies/proposal_mol_imaging_discordance/ |
| Surgeon Variability | Planned | studies/proposal_surgeon_variability/ |
| ETE Staging (PSM) | Submitted | studies/proposal2_ete_staging/ |
| 2–4cm Extent / Molecular | Draft v1 | studies/proposal_2to4cm_extent_molecular_20260326/ |
| NSQIP-PTH Protocol | Active | studies/nsqip_pth_protocol_manuscript/ |
| Lobectomy Molecular | Drafting | studies/lobectomy_molecular_202603/ |

| M085 | Idea, scaffolded 2026-05-08 | studies/m085_multisystem_tirads_comparison/ |

Other M-codes mentioned on disk but not yet given Linear projects: M004, M019, M027, M028, M029, M033, M043, M047. Treat as Manuscripts records in `Idea` / `Planned` until the user tells you otherwise.
