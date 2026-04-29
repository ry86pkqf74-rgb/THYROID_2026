# Cursor Agent Task — `canonical_complications_patient_rollup_v1` Sign-off (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 30-60 minutes
**Run order:** Lane 5 (run first of this 3-prompt batch — fewest moving parts)

---

## 1. Goal

Close the **complications family** by signing off `canonical_complications_patient_rollup_v1`. Events table was verified mig_99 (commit cbccd4a). Rollup `build_ts = 2026-04-28 19:36` (recent — likely rebuilt by a mig_98 closeout step or Script 365 step 2). Verify and sign off.

---

## 2. Methodology — derivation re-derivation against verified events

Pattern: **mig_104 medications rollup** — `qc_framework_v1/migrations/105_medications_patient_rollup_signoff.sql`. Adapt for complications.

### 2a. Pre-signoff probe
- Cohort parity: should be 10,871 rows = `canonical_patient_master` row count (CHANGE J)
- Per-patient aggregate re-derivation against `canonical_complications_events_v1` (mig_99 verified)
- For each derived col, use `BOOL_OR` / `COUNT` patterns from the existing build SQL
- Find build SQL: `grep -rn canonical_complications_patient_rollup_v1 qc_framework_v1 scripts | head` — likely `scripts/365_psh_pmh_meds_consolidation.py` or a sibling 364B script

### 2b. Schema (50 cols total = 49 derivable + 2 na)
- 11 phenotypes × 3 tiers (definitive / probable_or_better / any_evidence) = 33 cols (rln_injury, vocal_cord_paralysis, hypocalcemia_clinical, hypoparathyroidism, hematoma, seroma, chyle_leak, wound_infection, pneumothorax, airway_complication, wound_dehiscence, mortality)
- 4 metadata: n_complication_types_present, n_complication_findings_total, first_complication_date, last_complication_date
- 8 temporal classifications: hypoparathyroidism × {preexisting, new_postop, transient, permanent} + hypocalcemia_clinical × same
- 2 already-na: research_id, build_ts

### 2c. Sign-off SQL
3 sections (mirroring mig_104):
- a: flip cleanly-matching cols via `verification_method='derivation_re_derivation_against_verified_events'`
- b: any drifted cols via specific tag with note (likely STRING_AGG ordering — see CF-mig58-STRING-AGG-ORDER from mig_106)
- c: recompute counts + flip table_status='verified'

---

## 3. Acceptance gates

- 49 not_started cols flipped to verified
- 2 na cols carry over
- table_status='verified' in canonical_table_signoff_registry_v1
- Cohort parity confirmed (10,871 = CPM)
- Per-pt aggregate match for all derivable cols (expect 0 drift for BOOL_OR cols, possible STRING_AGG-ordering drift on first/last_finding_date — but these are MIN/MAX, not STRING_AGG, so should be 0 drift)

---

## 4. Don't touch (active parallel lanes)

- `canonical_pathology_clinical_events_v1` / `canonical_cervical_ln_clinical_events_v1` — Cowork's lane
- Any table touched by sibling Cursor lanes 6 + 7 (PSH rollup, PMH rollup)

---

## 5. Reference reading

- Auto-memory: `feedback_motherduck_direct_check.md`, `feedback_surgical_git_add.md`, `reference_protocol_v2_md_accounts.md`
- Repo: `qc_framework_v1/migrations/105_medications_patient_rollup_signoff.sql` (most recent rollup signoff template)
- Auto-memory: `project_medications_parathyroid_families_complete_2026-04-29.md` (3 reusable patterns from yesterday)
- Auto-memory: `project_complications_events_verified_2026-04-28.md` (events-side context)

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Migration filename: `qc_framework_v1/migrations/<next-N>_complications_patient_rollup_signoff.sql` (re-pull before to grab live next-N)
- Surgical `git add` (memory: `feedback_surgical_git_add.md`)
- Lint Python first if any (this should be SQL-only)
- DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ — `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for any new timestamp values

---

## 7. If something unexpected surfaces

- Rollup's build SQL is materially different from Script 365 pattern → STOP and ask Logan
- Cohort parity off (rollup row count ≠ 10,871) → STOP, investigate
- Drift on BOOL_OR cols → expected to be 0; if non-zero, that means rollup is stale relative to events; may need REBUILD via CREATE OR REPLACE pattern (see mig_101 path_gland_rollup template)

---

End of prompt. Update Cowork `MEMORY.md` close-out entry. Lane 5 of 3-prompt batch.
