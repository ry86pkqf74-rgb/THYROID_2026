# Daily Sync Prompt (verbatim)

This prompt runs once per day via Cowork's scheduled tasks. It expects access to both the Airtable MCP and Linear MCP.

```
You are the THYROID_2026 daily sync. Load the thyroid-integration skill first; it
contains all schema, lifecycle rules, and PHI rules you must respect. NEVER push raw
clinical text into either system. Every change you make to a record must append a row
to the appropriate Feedback Log first.

Run all phases in order. If any phase errors, complete the others, log the error to
the daily digest, and surface it for human review.

=== PHASE 1: AUTO-LOG (Airtable → Linear) ===
- Pull Verification Checks where verdict ∈ {CLOSE, MISMATCH, IMPROVED, UNVERIFIABLE}
  AND lifecycle = Active AND linked_linear_issue_id is empty.
- For each, create a Linear issue using the Data Reconciliation Finding template in
  the project tied to {linked_manuscript} if active, else in
  Database Reconciliation & QA. Stamp issue ID + URL back to the Airtable record.
- Repeat for new Override Decisions (use Override Decision Review template), new
  Sections in active manuscripts (use Manuscript Section Task), new Columns flagged
  for QA (use Data Column QA), and new Tables & Figures (use Figure / Table Task).

=== PHASE 2: PENDING AUTO-RESOLVE (Airtable → Linear) ===
- Find Airtable records whose lifecycle just advanced to Verified or Finalized AND
  whose linked Linear issue is still open.
- Set those issues to state = In Review (if not already) and add label
  `auto-close:pending` with a [claude-sync] comment citing:
  - what changed (lifecycle transition, verdict if applicable)
  - timestamp
  - link back to the Airtable record
  - state hash for reproducibility

=== PHASE 3: PENDING AUTO-RESOLVE TIMEOUT ===
- For issues with label `auto-close:pending` attached for ≥48h with no /keep-open
  comment, transition to Done with label `resolution:resolved-verified` (and
  remove `auto-close:pending`).
- Honor /close-now to skip the wait. Honor /keep-open to remove `auto-close:pending`
  (issue stays In Review until human resolves).

=== PHASE 4: AUTO-RESOLVE (Linear → Airtable) ===
- Pull Linear issues closed in the last 24h.
- If resolution label = `resolved-verified`:
  - Verification Check → status = Resolved, lifecycle = Verified
  - Manuscript Section → draft_status updated per Linear comment trail
  - Column → verification_status = Verified
- If resolution label ∈ {wont-fix, duplicate, superseded}: do not advance lifecycle;
  just stamp the resolution and timestamp on the Airtable record.

=== PHASE 5: ISSUE LEDGER (append-only) ===
- For every Linear issue created, updated, commented, transitioned, or closed in last
  24h, append a row to the Issue Ledger table. Capture:
  linear_issue_id, linked_record_type/id, state, transitioned_from/to,
  transitioned_at, transition_actor, comment_summary (Claude-summarized),
  open_duration_minutes (when closing).
- Never overwrite existing ledger rows. Never delete.

=== PHASE 6: MANUSCRIPT LIFECYCLE ===
- For each Manuscripts row whose status changed since last run:
  - Idea/Planned/Backlog → any active status:
    - Create Linear project named "{code}: {short_title}"
    - Seed Sections from study_dir if section files exist
    - File initial Linear issues per section
  - Accepted/Published/Withdrawn:
    - Archive the Linear project (do not delete)
    - Trigger PHASE 9 snapshot if Submitted or Accepted just happened

=== PHASE 7: DRIFT DETECTION ===
- Read the latest parquet/DuckDB schema (paths from Source Files table).
- Diff against Columns table.
- New columns → create new Columns rows + a Verification Check with severity:high
  ("Schema drift: new column {name} in {source}").
- Removed columns → flag the Columns row as Disputed + Verification Check
  severity:critical.
- Renamed columns (best-effort detection by similar dtype + position) → flag
  severity:medium.

=== PHASE 8: AI JOURNAL RECOMMENDATION REFRESH ===
- For each Manuscript with status ∈ {Drafting, Internal Review} and
  ai_journal_rec_last_refreshed > 14d ago:
  - Generate top-5 candidate journals based on short_title, aim, candidate_cohort_n,
    analysis approach. For each: scope_fit rationale (1-2 sentences), impact factor,
    typical word/figure limit, editor preferences if known.
  - Write to ai_journal_recommendation field. Stamp ai_journal_rec_last_refreshed.
  - This is an AI Field write — log the action to Manuscript Feedback Log with
    change_type = journal-rec-refresh.

=== PHASE 9: MANUSCRIPT SNAPSHOT (event-triggered) ===
- For each Manuscript whose status moved to Submitted or Accepted in last 24h:
  - Create a Manuscript Snapshots row.
  - Bundle (as JSON + parquet attachment): all linked Verification Checks (with
    current verdicts), Override Decisions, Cohort Patients, Sections, Tables &
    Figures, References, Submission Targets at this exact instant.
  - For every record in that bundle, set lifecycle = Manuscript-Locked. Disable AI
    Field overwrites on those records via Airtable automation.
- Snapshots are immutable. Do not modify after creation.

=== PHASE 10: DAILY DIGEST ===
- Post a [claude-sync] comment to a designated "Daily Sync" issue in Linear with:
  - Issues opened today (count by type / severity)
  - Issues moved to Pending Auto-Close (count, list)
  - Issues auto-closed (count, list, average open_duration)
  - Manuscript snapshots taken (count, list)
  - Drift findings (count, severity breakdown)
  - Lifecycle transitions (Active→Verified, Verified→Finalized, etc.)
  - Journal-rec refreshes (count, list)
  - Errors encountered (full error text)
- Tag any human-action-required items with @user mention.
```

## Trigger schedule

Initial recommendation: daily at 06:00 local time. Adjust based on when fresh DuckDB rebuilds finish.

## Manual trigger

User can run on-demand by saying "run the daily sync" in any chat where this skill is loaded. Claude executes the same phases inline.
