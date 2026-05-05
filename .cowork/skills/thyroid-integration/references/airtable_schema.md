# Airtable Schema — THYROID_2026

## Base A: THYROID_DATA_REGISTRY

### Source Files
One row per raw Excel / parquet / DuckDB table.

| Field | Type | Notes |
|---|---|---|
| filename | text (primary) | e.g. `FNAs 12_5_2025.xlsx` |
| domain | single select | Pathology / FNA / US / CT / MRI / NucMed / Labs / Surgery / Demographics / Synoptics |
| row_count | number | snapshot at last ingest |
| last_modified | date | from filesystem |
| owner | linked record (Co-Authors) | who maintains this source |
| ingest_date | date | when added to DuckDB |
| ingest_notes | long text | any caveats |
| status | single select | Active / Archived / Deprecated |
| dvc_hash | text | from `.dvc` sidecar |
| columns | linked record (Columns) | reverse link |

### Columns
One row per column per source file.

| Field | Type | Notes |
|---|---|---|
| qualified_name | formula | `{source_file} . {column_name}` (primary) |
| column_name | text | e.g. `final_path_diagnosis_original` |
| source_file | linked record (Source Files) | |
| data_type | single select | int / float / string / date / bool / categorical |
| allowed_values | long text | enum or range |
| business_definition | long text | human description |
| ai_description | AI field (long text) | auto-generated from sample data |
| verification_status | single select | Unchecked / In QA / Verified / Disputed |
| owner | linked record (Co-Authors) | |
| last_verified | date | |
| gold_standard_value | text | e.g. expected sentinel value |
| lifecycle | single select | Active / In QA / Verified / Finalized / Manuscript-Locked / Archived |
| linked_verification_checks | linked record (Verification Checks) | |
| linked_linear_issue_id | text | populated by daily sync |
| linked_linear_issue_url | URL | populated by daily sync |

### Verification Checks
One row per metric in your reconciliation matrix (see TGDC_VERIFICATION_REPORT.md, manuscript_metric_registry_v1.md).

| Field | Type | Notes |
|---|---|---|
| metric_name | text (primary) | e.g. `Total patients (n)` |
| linked_manuscript | linked record (Manuscripts) | which paper this metric belongs to |
| manuscript_value | text | what the manuscript says |
| db_value | text | what DuckDB returns now |
| verdict | single select | MATCH / CLOSE / MISMATCH / IMPROVED / UNVERIFIABLE |
| severity | single select | critical / high / medium / low |
| status | single select | Open / In QA / Resolved / Wont-fix |
| owner | linked record (Co-Authors) | |
| fix_action | long text | proposed fix |
| linked_linear_issue_id | text | |
| linked_linear_issue_url | URL | |
| lifecycle | single select | Active / In QA / Verified / Finalized / Manuscript-Locked / Archived |
| last_run_date | date | |

### Override Decisions
Gold-standard chart-review overrides (see TGDC_FINAL_RECONCILIATION_REPORT.md §3).

| Field | Type | Notes |
|---|---|---|
| decision_id | autonumber (primary) | |
| research_id_pseudo | text | de-identified ID only |
| field | text | which field was overridden |
| original_value | text | |
| override_value | text | |
| evidence_summary | long text | **Claude-summarized only — NEVER raw note text** |
| reviewer | linked record (Co-Authors) | |
| decision_date | date | |
| justification | long text | |
| linked_manuscript_section | linked record (Sections) | |
| lifecycle | single select | Active / Verified / Finalized / Manuscript-Locked |

### Cohort Patients
Pseudo-IDs + availability metadata only. NO PHI.

| Field | Type | Notes |
|---|---|---|
| research_id | text (primary) | de-identified |
| disease_group | single select | malignant / benign |
| malignancy_origin | single select | TGDC-C / CONCOMITANT / THYROIDAL / UNCLASSIFIED / N/A |
| has_us | bool | data availability flags |
| has_ct | bool | |
| has_mri | bool | |
| has_fna | bool | |
| has_path | bool | |
| has_rai | bool | |
| n_sources | number | |
| sources | text | pipe-delimited table list |
| included_in_manuscripts | linked record (Manuscripts, multi) | |
| lifecycle | single select | Active / Verified / Finalized / Manuscript-Locked / Archived |

### Reconciliation Runs
Each time the verification matrix is regenerated.

| Field | Type | Notes |
|---|---|---|
| run_date | date (primary) | |
| n_cohort | number | |
| n_malignant | number | |
| n_match | number | |
| n_close | number | |
| n_mismatch | number | |
| headline_findings | long text | |
| run_report | attachment | the .md output |
| triggered_by | text | manual / scheduled / drift |

### Issue Ledger (append-only, immutable)
Every Linear issue state transition mirrored from Linear.

| Field | Type | Notes |
|---|---|---|
| ledger_id | autonumber (primary) | |
| linear_issue_id | text | |
| linear_url | URL | |
| linked_record_type | single select | Verification Check / Section / Column / Override / Manuscript |
| linked_record_id | text | id of the linked Airtable record |
| issue_title | text | |
| label_type | text | |
| label_severity | text | |
| state | text | the new state after transition |
| transitioned_from | text | the previous state |
| transitioned_to | text | (= state) |
| transitioned_at | datetime | |
| transition_actor | text | user or 'claude-sync' |
| comment_summary | long text | Claude-summarized comment if any |
| open_duration_minutes | number | populated when closing |

### Manuscript Snapshots
Immutable point-in-time evidence freeze.

| Field | Type | Notes |
|---|---|---|
| snapshot_id | autonumber (primary) | |
| snapshot_date | datetime | |
| manuscript | linked record (Manuscripts) | |
| trigger_event | single select | Submitted / Accepted / Manual |
| n_cohort | number | at snapshot time |
| n_malignant | number | at snapshot time |
| snapshot_bundle | attachment | JSON + parquet of all linked records |
| lifecycle | single select | always `Locked` |

---

## Base B: THYROID_MANUSCRIPT

### Manuscripts
Canonical inventory of all 90+ planned/active/published.

| Field | Type | Notes |
|---|---|---|
| code | text (primary) | M025, M044, H1, Mo36, TGDC, etc. (or short slug for un-coded) |
| short_title | text | |
| full_title | long text | |
| status | single select | Idea / Planned / Cohort Definition / Analysis / Drafting / Internal Review / Submitted / Revisions / Accepted / Published / Withdrawn / Backlog |
| aim | long text | |
| rationale | long text | |
| candidate_cohort_n | number | |
| owner | linked record (Co-Authors) | |
| irb_number | text | |
| study_dir | text | path under THYROID_2026/ |
| ai_journal_recommendation | AI field (long text) | top 5 hierarchy with rationale, IF, scope_fit |
| ai_journal_rec_last_refreshed | date | |
| journal_chosen | single select | TBD / *Thyroid* / *JCEM* / *Endocrine Practice* / *Cancer* / etc. |
| target_journal_freeform | text | for journals not in the dropdown |
| linked_sections | linked record (Sections) | |
| linked_manuscript_feedback | linked record (Manuscript Feedback Log) | |
| linked_data_feedback | linked record (Data Feedback Log) | |
| linked_linear_project | text | |
| last_updated | last modified time | |

### Sections

| Field | Type | Notes |
|---|---|---|
| section_id | autonumber (primary) | |
| manuscript | linked record (Manuscripts) | |
| section_type | single select | Abstract / Introduction / Methods / Results / Discussion / Limitations / Conclusion / Tables / Figures / Supplement |
| content_summary | long text | |
| draft_status | single select | Outline / First Draft / Revising / Internal Review / Coauthor Review / Final |
| owner | linked record (Co-Authors) | |
| last_updated | last modified time | |
| ai_readability_score | AI field | |
| blockers | long text | |
| linked_linear_issue_id | text | |
| linked_linear_issue_url | URL | |
| lifecycle | single select | Active / Verified / Finalized / Manuscript-Locked / Archived |

### Tables & Figures

| Field | Type | Notes |
|---|---|---|
| label | text (primary) | "Table 2" / "Figure 1A" |
| manuscript | linked record (Manuscripts) | |
| caption | long text | |
| source_data_file | linked record (Source Files) | |
| generation_script | text | path |
| status | single select | Draft / Final / Submitted |
| reviewer_comments | long text | |
| last_regenerated | date | |

### References

| Field | Type | Notes |
|---|---|---|
| bibtex_key | text (primary) | |
| title | text | |
| authors | text | |
| year | number | |
| journal | text | |
| doi | URL | |
| used_in_manuscripts | linked record (Manuscripts, multi) | |

### Co-Authors

| Field | Type | Notes |
|---|---|---|
| name | text (primary) | |
| email | email | |
| role | single select | PI / Co-PI / Statistician / Resident / Fellow / Senior Author / Other |
| orcid | text | |
| manuscripts_owned | linked record (Manuscripts, multi) | |
| review_status_per_manuscript | long text | |

### Submission Targets

| Field | Type | Notes |
|---|---|---|
| target_id | autonumber (primary) | |
| manuscript | linked record (Manuscripts) | |
| journal | text | |
| scope_fit | single select | Strong / Moderate / Weak |
| impact_factor | number | |
| decision | single select | Pending / Under Review / Major Revision / Minor Revision / Accepted / Rejected / Withdrawn |
| response_due | date | |
| next_action | long text | |

### Manuscript Feedback Log (append-only, immutable)
Every change Claude makes to a manuscript at the user's request.

| Field | Type | Notes |
|---|---|---|
| feedback_id | autonumber (primary) | |
| timestamp | datetime | |
| manuscript | linked record (Manuscripts) | |
| section | linked record (Sections, optional) | |
| change_type | single select | edit / add / delete / restructure / clarify / journal-rec-refresh / unlock / other |
| your_request_summary | long text | 1-line paraphrase of user's request |
| my_action_summary | long text | what Claude actually did |
| before_excerpt | long text | |
| after_excerpt | long text | |
| source_chat | URL or text | session ID or chat link |
| lifecycle | single select | always `Logged` (immutable) |

### Data Feedback Log (append-only, immutable)
Every change Claude makes to data/registry at the user's request.

| Field | Type | Notes |
|---|---|---|
| feedback_id | autonumber (primary) | |
| timestamp | datetime | |
| target_type | single select | Column / Verification Check / Override Decision / Cohort Patient / Source File / Reconciliation Run |
| target_record | text | record ID in target table |
| change_type | single select | edit / add / delete / verify / dispute / archive / unlock / other |
| your_request_summary | long text | |
| my_action_summary | long text | |
| before_value | long text | |
| after_value | long text | |
| source_chat | URL or text | |
| lifecycle | single select | always `Logged` (immutable) |
