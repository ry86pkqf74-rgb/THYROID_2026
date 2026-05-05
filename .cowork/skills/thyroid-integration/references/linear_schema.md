# Linear Schema — THYROID_2026

## Team

Single team: **THYROID**.

## Workstream Projects (always-on)

| Project | Description |
|---|---|
| Database Reconciliation & QA | Verification Check findings land here |
| Data Pipeline & Tooling | DuckDB / parquet / notebook / extraction maintenance, MIG_* migrations |
| LLM Extraction & Refinement | Phase 4-11 extraction, intrinsic eval, audits |
| Cohort Curation | Adds/drops, override decisions, chart-review work spanning all papers |
| Submissions & Reviewer Defense | Generic submission packaging, reviewer responses |
| Manuscript Backlog Triage | Periodic review of Idea/Planned manuscripts for promotion to active |

## Per-Active-Manuscript Projects (created on demand)

Created automatically when a Manuscripts row in Airtable transitions from Idea/Planned/Backlog to an active status. Archived (not deleted) when the manuscript reaches Accepted/Published/Withdrawn.

Project name format: `{code}: {short_title}` — e.g. `M025: ACR TI-RADS Operative Cohort`.

## Labels

### type:
- `data-finding`
- `qa-check`
- `manuscript`
- `figure-table`
- `infra`
- `override-review`
- `extraction`

### severity:
- `critical`
- `high`
- `medium`
- `low`

### source:
- `airtable-sync` (created by daily sync)
- `claude-flagged` (created by Claude on demand)
- `manual` (user-created in Linear directly)

### section:
- `methods`
- `results`
- `tables`
- `figures`
- `limitations`
- `discussion`
- `abstract`
- `supplement`

### stage:
- `drafting`
- `coauthor-review`
- `revision`
- `submitted`
- `accepted`

### resolution: (used at close time)
- `resolved-verified` — triggers Airtable lifecycle advance
- `wont-fix`
- `duplicate`
- `superseded`

## Workflow States (defaults + label overlay)

Linear's default states are used. The originally-planned custom states are replaced by labels (`awaiting:chart-review`, `awaiting:coauthor`, `auto-close:pending`) because Linear's MCP doesn't expose state creation. See THY-10 (closed) for rationale.

Pattern:

```
Backlog [+ awaiting:chart-review when waiting]
  → Todo
    → In Progress
      → In Review [+ awaiting:coauthor when waiting on coauthor]
                 [+ auto-close:pending when in 48h auto-close buffer]
        → Done [+ resolution:resolved-verified | resolution:wont-fix | etc.]
        → Canceled
```

Daily sync watches `auto-close:pending` (not a state name). 48h with the label and no `/keep-open` → transition to Done.

## Issue Templates

### Data Reconciliation Finding
```
Title: {metric_name}: {verdict}
Description:
  Manuscript value: {manuscript_value}
  DB value: {db_value}
  Severity: {severity}
  Linked Airtable record: {airtable_url}
  Linked manuscript: {manuscript_code}

  Proposed fix:
  {fix_action}
Labels: type:data-finding, severity:*, source:airtable-sync
```

### Data Column QA
```
Title: QA: {column_qualified_name}
Description:
  Source file: {source_file}
  Data type: {data_type}
  Definition: {business_definition}
  Open questions:
  - allowed values?
  - sample distribution sane?
  - reconciles to {linked manuscripts}?
Labels: type:qa-check, source:airtable-sync
```

### Override Decision Review
```
Title: Override Review: {field} for {research_id_pseudo}
Description:
  Original: {original_value}
  Override: {override_value}
  Reviewer: {reviewer}
  Justification: {justification}
  Evidence summary (PHI-free): {evidence_summary}
Labels: type:override-review, source:airtable-sync
```

### Manuscript Section Task
```
Title: {manuscript_code} {section_type}: {short_summary}
Description:
  Manuscript: {short_title}
  Section: {section_type}
  Status: {draft_status}
  Owner: {owner}
  Blockers: {blockers}
  Airtable section record: {url}
Labels: type:manuscript, section:*, stage:*
```

### Figure / Table Task
```
Title: {label} — {short_caption}
Description:
  Manuscript: {manuscript_code}
  Source data: {source_data_file}
  Generation script: {generation_script}
  Status: {status}
  Reviewer comments: {reviewer_comments}
Labels: type:figure-table, section:figures or tables
```

## Comment Conventions (Claude-authored)

When closing or transitioning issues via the daily sync, Claude prefixes its comments with `[claude-sync]` and includes:

- Reason for transition (lifecycle advance, manual close, drift detection, etc.)
- Backlink to the Airtable record
- Timestamp
- Hash of the data state at transition time (for reproducibility on Manuscript-Locked items)

User overrides:
- `/keep-open` — exit Pending Auto-Close, return to In Review
- `/close-now` — skip 48h buffer, close immediately
- `/snapshot {manuscript_code}` — trigger a Manuscript Snapshot for the named manuscript
- `/unlock {manuscript_code} because {reason}` — transition Manuscript-Locked records back to Finalized
