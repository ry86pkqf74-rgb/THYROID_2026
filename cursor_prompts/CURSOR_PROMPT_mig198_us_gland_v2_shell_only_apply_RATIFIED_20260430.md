# Cursor Prompt — mig_198 mig_194 Option B apply: shell-only US gland v2 events + rollup

**Date:** 2026-04-30
**Lane:** mig_198 / us_gland_v2_shell_only_apply_OptionB_RATIFIED
**Batch (proposed):** `mig_198_us_gland_v2_shell_only_apply_20260430`
**Predecessor:** mig_194 (`7324152`) — confirmed `clinical_note_thyroid_us_extracted_v1` does NOT exist on MD; recommends Option B shell-only build. **Logan-ratified Option B.**
**Posture:** **READ-ONLY scoping + apply skeleton SQL.** No execute against MotherDuck.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only); produces apply skeleton for Cowork Path-C.
**Tool recommendation:** **Cursor Composer** — pattern-following from mig_171b SQL; multi-file edit (apply SQL + report).

---

## Logan-ratified rule (LOCKED)

**Option B — shell-only build.** No NLP supplemental events. Build `canonical_us_thyroid_gland_events_v2` and `canonical_us_thyroid_gland_patient_rollup_v2` from `canonical_us_thyroid_gland_v2` shell only. `exam_id_source ∈ {structured, fallback}` only. 10-gate validation modified: G7 expects 2 source values, G8 → WARN/SKIP "no NLP supplemental source as of mig_198; documented limitation."

---

## Mission

Author apply skeleton + report. Mirrors mig_171b pattern but without NLP source.

---

## Required scope

### §1 Read mig_171b apply SQL + scoping report

`qc_framework_v1/migrations/171b_canonical_us_lymph_node_v2_build_20260429.sql` for the events/rollup/validation pattern.
`qc_framework_v1/reports/mig_194_thyroid_us_nlp_source_unblock_20260430.md` for the Option B contour.

### §2 Author apply SQL

`qc_framework_v1/migrations/198_us_gland_v2_shell_only_apply_20260430.sql`:

- §0 pre-flight invariants (CPM 10,871; canonical_us_thyroid_gland_v2 13,578 rows / 10,859 patients)
- §A pre-snapshot existing canonical_us_thyroid_gland_v2 to `archive_pub_v1_0` with timestamp
- §B `CREATE OR REPLACE TABLE main.canonical_us_thyroid_gland_events_v2 AS …` from shell only:
  - `exam_id_source = CASE WHEN source_ultrasound_reports=TRUE THEN 'structured' WHEN source_us_nodules_tirads=TRUE OR exam_date IS NULL THEN 'fallback' END`
  - Deterministic `us_exam_id` matching mig_171b recipe (`md5('US_EXAM_V2|' || rid || '|' || exam_date)` for fallback OR singleton-EM-reuse)
  - Carry through structured cols (right/left lobe dimensions, total volume, isthmus thickness)
  - Carry through `nlp_backfill_pending=TRUE` flag
  - Source-text cols: `clinical_impression_text`, `recommendation_text`, `radiologist`, `study_indication`
  - `gland_event_id = md5('US_GLAND_EVENT_V2|' || rid || '|' || us_exam_id || '|' || …)`
  - `build_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)`, `build_migration='mig_198'`
- §C `CREATE OR REPLACE TABLE main.canonical_us_thyroid_gland_patient_rollup_v2 AS …`:
  - PM spine LEFT JOIN event_agg pattern (mirrors mig_171b §C)
  - Aggregate cols: n_us_gland_exams, first/last exam dates, max_total_volume_ml, any_goiter, any_substernal, any_pyramidal, etc.
- §D `CREATE OR REPLACE TABLE main.val_mig198_canonical_us_thyroid_gland_build_v1 AS …` — 10-gate validation table:
  - G1 event_id unique
  - G2 event exam_date nonnull
  - G3 source modality consistent (US only)
  - G4 evidence text PHI-limited (≤240 chars)
  - G5 rollup row count = 10,871
  - G6 has_us_gland_findings bidirectional
  - G7 source distribution shows ONLY {structured, fallback} (NOT nlp_supplemental) — PASS expected
  - **G8 nlp supplemental SKIP** — informational note: "Option B; no NLP source"
  - G9 fallback alignment probe (acceptable if structured majority)
  - G10 measurement plausibility (lobe dims ≤25 cm; volume ≤600 mL)
- §E Registry note appendix — close `CF-117-US-GLAND-PARENCHYMA` on 28 cols:
  ```sql
  UPDATE main.canonical_column_verification_registry_v1
  SET notes = COALESCE(notes,'') || ' | mig_198 Option B 2026-04-30: shell-only US gland v2 events/rollup built; CF-117-US-GLAND-PARENCHYMA CLOSED via shell-only path; NLP supplemental deferred to future thyroid-US NLP extraction lane.'
  WHERE notes ILIKE '%CF-117-US-GLAND-PARENCHYMA%';
  ```
- §F `cpm_reconciliation_provenance_v1` insert
- §G Post-state probes + gate3 verify

### §3 Audit/report

Author `qc_framework_v1/reports/mig_198_us_gland_v2_shell_only_apply_ratified_20260430.md`:
- §1 Logan-ratified Option B (verbatim)
- §2 build SQL design rationale (shell-only)
- §3 expected post-state metrics (events row count ~13,578; rollup 10,871; G7 PASS with 2 source values)
- §4 manuscript footnote text — what to say in supplement about gland parenchyma being shell-only
- §5 Cowork Path-C apply checklist

### §4 Mark READY

Header: `-- LOGAN RATIFIED 2026-04-30 (Option B); READY FOR COWORK PATH-C APPLY`

---

## Governance reminders

- Read-only investigation only. No `query_rw`.
- Author = `Logan Glosser <logan.glosser@gmail.com>`.
- All `CREATE TABLE AS` use `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)`.
- No `BEGIN TRANSACTION`.

---

## Deliverables

1. `qc_framework_v1/migrations/198_us_gland_v2_shell_only_apply_20260430.sql`
2. `qc_framework_v1/reports/mig_198_us_gland_v2_shell_only_apply_ratified_20260430.md`
3. `exports/mig198_us_gland_apply_20260430/manifest.json`

Commit message: `qc: mig_198 mig_194 Option B apply RATIFIED (shell-only US gland v2 events + rollup; closes CF-117-US-GLAND-PARENCHYMA; no NLP supplemental)`

---

End of prompt.
