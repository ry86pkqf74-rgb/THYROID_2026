# Cursor Agent Task — `canonical_recurrence_resolved_v1` Verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-Cursor-14 mig_122)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting (current tip `8810385` after Cursor 14 mig_122)
**Estimated effort:** 60-90 minutes (19 cols, real Tier 2 recurrence adjudication)
**Run order:** Lane 17 of next 3-prompt batch (run first — closes Tier 2 recurrence + unblocks Lane 13 CFs)

---

## 1. Goal

Verify `canonical_recurrence_resolved_v1` under Protocol v2.

| Stat | Value |
|---|---|
| Rows | 10,871 |
| Patients | 10,871 (cohort-wide; one row per patient) |
| Cols total | 19 |
| not_started | 16 |
| na | 3 |
| Real-data signal | 191 path-proven recurrences, 768 imaging-suspicious, 3 distinct `recurrence_status_final` values |

This is the **Tier 2 resolved recurrence canonical** — the actual richer recurrence layer that downstream consumers (e.g., `canonical_ete_event_resolved_v1` mig_121) extract from. Distinct from the Tier 1 `canonical_recurrence_v1` that Cursor 14 verified as a degenerate shell on 2026-04-29.

---

## 2. Schema preview

| Col | Type | Category |
|---|---|---|
| research_id | VARCHAR | na |
| first_surg_date | DATE | adjudicated |
| recurrence_path_proven | BOOLEAN | adjudicated |
| recurrence_path_proven_date | DATE | adjudicated |
| recurrence_path_proven_source | VARCHAR | adjudicated |
| recurrence_path_proven_evidence | VARCHAR | adjudicated |
| days_to_path_proven | BIGINT | derived |
| recurrence_imaging_suspicious | BOOLEAN | adjudicated |
| recurrence_imaging_suspicious_date | DATE | adjudicated |
| recurrence_imaging_modality | VARCHAR | adjudicated |
| recurrence_imaging_modality_summary | VARCHAR | adjudicated |
| recurrence_imaging_source | VARCHAR | adjudicated |
| recurrence_imaging_finding_text | VARCHAR | adjudicated |
| recurrence_imaging_n_events | BIGINT | derived |
| days_to_imaging_suspicious | BIGINT | derived |
| recurrence_imaging_then_path_confirmed | BOOLEAN | adjudicated |
| recurrence_status_final | VARCHAR | adjudicated |
| build_script | VARCHAR | adjudicated |
| build_ts | TIMESTAMP WITH TIME ZONE | na (provenance) |

✓ **Dates clean:** All clinical date cols are DATE. `build_ts` TIMESTAMP WITH TIME ZONE is provenance (allowlist already covers via `built_at`/`build_ts` aliases — verify in audit).

---

## 3. Methodology — extraction-faithfulness + internal-consistency hybrid

Pattern reference: `qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql` (mig_118 hybrid pattern #9 from `feedback_audit_regex_word_boundary` family).

### 3a. Locate build SQL
```bash
grep -rn "canonical_recurrence_resolved_v1" scripts qc_framework_v1 | head -20
```
Likely a Tier 2 builder in the 200s or 300s range. Read the SQL to identify upstream feeds. Probable sources:
- `canonical_path_malignant_events_v1` (verified mig_89) — for `recurrence_path_proven` evidence
- `canonical_pathology_clinical_events_v1` (verified mig_110) — for path-clinical recurrence findings
- `canonical_cervical_ln_clinical_events_v1` (verified mig_111) — for LN imaging/path
- Imaging upstream — likely an imaging_findings table or US-related events

### 3b. Probe staleness
```sql
SELECT 
  (SELECT MAX(build_ts)::TIMESTAMP FROM main.canonical_recurrence_resolved_v1) AS rr_build,
  (SELECT MAX(build_ts) FROM main.canonical_path_malignant_events_v1) AS pme_build,
  (SELECT MAX(build_ts) FROM main.canonical_pathology_clinical_events_v1) AS pce_build;
```

### 3c. Per-col verification by category

**Source-cluster cols** (extraction-faithfulness vs verified upstream):
- `recurrence_path_proven`, `recurrence_path_proven_date`, `recurrence_path_proven_source`, `recurrence_path_proven_evidence` → faithful to path-events upstream
- `recurrence_imaging_*` (8 cols) → faithful to imaging upstream

**Derived cluster** (internal-consistency vs canonical's own values):
- `days_to_path_proven` = DATE_DIFF('day', first_surg_date, recurrence_path_proven_date) WHERE recurrence_path_proven=TRUE
- `days_to_imaging_suspicious` = same with recurrence_imaging_suspicious_date
- `recurrence_imaging_n_events` = count of imaging events per patient (from upstream)
- `recurrence_imaging_then_path_confirmed` = (imaging_suspicious AND path_proven AND imaging_date < path_date)

**Final status:**
- `recurrence_status_final` enum: probably {none, path_proven, imaging_only_suspicious} — 3 distinct values per probe
- Should be derivable from the BOOLEAN flags (path_proven OR imaging_then_path → 'path_proven', imaging-only → 'imaging_only_suspicious', else 'none')

### 3d. Cross-validation against Lane 13 (ete_event_resolved mig_121)
Cursor 13 already used `extraction_faithfulness_against_canonical_recurrence_resolved_v1_mig62` for 13 cols. Verify this faithfulness still holds:
```sql
WITH per_pt AS (
  SELECT er.research_id,
    BOOL_OR(er.recurrence_path_proven) AS er_path_proven,
    MAX(er.recurrence_path_proven_date) AS er_path_date
  FROM main.canonical_ete_event_resolved_v1 er
  GROUP BY er.research_id
)
SELECT 
  COUNT(*) FILTER (WHERE per_pt.er_path_proven IS DISTINCT FROM rr.recurrence_path_proven) AS path_proven_drift,
  COUNT(*) FILTER (WHERE per_pt.er_path_date IS DISTINCT FROM rr.recurrence_path_proven_date) AS path_date_drift
FROM main.canonical_recurrence_resolved_v1 rr
LEFT JOIN per_pt ON per_pt.research_id = rr.research_id;
```
Drift > 0 surfaces inconsistency between Cursor 13's labeled faithfulness and the recurrence_resolved current state.

### 3e. Cohort parity
```sql
SELECT 
  (SELECT COUNT(*) FROM main.canonical_recurrence_resolved_v1) = 10871 AS cohort_match,
  (SELECT COUNT(DISTINCT research_id) FROM main.canonical_recurrence_resolved_v1) = 10871 AS pts_match;
```

### 3f. Status mapping
```sql
-- Verify recurrence_status_final encoding rule
SELECT recurrence_status_final, 
  COUNT(*) AS n,
  COUNT(*) FILTER (WHERE recurrence_path_proven) AS n_path,
  COUNT(*) FILTER (WHERE recurrence_imaging_suspicious) AS n_imaging,
  COUNT(*) FILTER (WHERE recurrence_imaging_then_path_confirmed) AS n_then_path
FROM main.canonical_recurrence_resolved_v1
GROUP BY 1
ORDER BY n DESC;
```

### 3g. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_recurrence_resolved_v1_signoff.sql`
- 16 col flips with appropriate verification_method per cluster
- 3 already-na cols (research_id, build_script, build_ts) carry over
- table_status update

---

## 4. Acceptance gates

- All 16 not_started cols flipped to verified
- Source-cluster: 0 drift vs verified upstream feeds
- Derived cluster: 0 errors on internal-consistency probes
- Cross-validation against Lane 13 mig_121: 0 drift on path_proven + path_date (or document if drift > 0 → may indicate Cursor 13's faithfulness check is non-current)
- Cohort parity: 10,871 = patient_master count
- recurrence_status_final encoding rule: provable derivation from BOOLEANs (no manual rows where flags don't match status)
- 191 path_proven + 768 imaging_suspicious counts confirmed (per probe)

---

## 5. Don't touch (active parallel lanes)

- `canonical_survival_followup_v1` — Lane 15 (still in flight per MD probe at 2026-04-29)
- `canonical_molecular_genetics_from_notes_v2` — Lane 16 (still in flight)
- `canonical_table_signoff_registry_v1` / `canonical_column_verification_registry_v1` — Sibling Lane 18
- `canonical_recurrence_v1` rebuild — Sibling Lane 19 (Script 203 reconstruction)

---

## 6. Reference reading

Required:
- Auto-memory: `project_canonical_recurrence_v1_mig_122_closeout.md` (Cursor 14 close-out + the 3 CFs)
- Auto-memory: `project_op_procedure_codes_mig_118_closeout.md` (hybrid pattern #9)
- Auto-memory: `feedback_clinical_dates_calendar_only.md` (or `qc_framework_v1/migrations/clinical_date_retype_20260428.md` per AGENTS.md)
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_audit_regex_word_boundary.md`
- Repo: `qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql` (hybrid template)
- Repo: `qc_framework_v1/migrations/121_ete_event_resolved_inline_family_signoff.sql` (Cursor 13's recurrence-faithfulness reference)
- Repo: `qc_framework_v1/migrations/122_canonical_recurrence_v1_signoff.sql` (Cursor 14's shell-recurrence context)

---

## 7. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing recurrence_resolved_v1
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Surgical git add (no `-A`, no `scripts/output/` sweeps)

---

## 8. If something unexpected surfaces

- Path_proven count != 191 OR imaging_suspicious count != 768 → cohort drift since today's probe; re-baseline + document
- Cursor 13 cross-validation drift > 5 → indicates faithfulness mismatch; STOP and ask Logan whether to re-flip Lane 13 cols (post-mig_121 status_label revision)
- recurrence_status_final has a 4th value not in {none, path_proven, imaging_only_suspicious} → check encoding logic
- Imaging upstream source unclear → grep for `canonical_imaging_*` or `note_entities_imaging` in `scripts/`
- build_ts TIMESTAMP WITH TIME ZONE causes audit gate-5 hit → already covered by audit allowlist via `build_ts`; verify
- recurrence_imaging_then_path_confirmed=TRUE rows where imaging_date >= path_date → temporal violation; investigate

---

End of prompt. Lane 17 of new 3-prompt batch. Closes Tier 2 recurrence_resolved. When this lands, Cursor 13's `extraction_faithfulness_against_canonical_recurrence_resolved_v1_mig62` (13 cols on ete_event_resolved) is fully grounded in a verified upstream — superseding any pending labels.
