# Cursor Agent Task — `canonical_frozen_section_patient_rollup_v1` Verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 90-120 minutes (188 cols, repeating frozen_N_* schema)
**Run order:** Lane 11 of next batch (run first — biggest single rollup, sets pattern)

---

## 1. Goal

Verify `canonical_frozen_section_patient_rollup_v1` under Protocol v2.

| Stat | Value |
|---|---|
| Rows | 4,116 |
| Patients | 4,116 (one row per patient with any frozen section) |
| Cols total | 188 |
| not_started | 187 |
| na | 1 (research_id) |

This is the biggest single rollup in the remaining backlog. Source events table `canonical_frozen_section_events_v1` was closed by mig_100 on 2026-04-28 (31 cols / 25 verified + 6 na) using CTC-equivalence vs pre-Script-360 archive (memory: `project_frozen_section_mig_100_closeout.md`).

---

## 2. Schema shape

The rollup is a wide pivot of frozen-section events into per-event slots:

- **Metadata cols** (~8): `frozen_section_count`, `frozen_section_any_performed_flag`, `frozen_section_any_malignant_flag`, `frozen_section_any_deferred_flag`, `frozen_section_any_suspected_flag`, `frozen_section_first_date`, `frozen_section_last_date`
- **Per-frozen-event cols** (15 cols × N events) — pattern `frozen_<N>_*` for N=1..K:
  - `frozen_N_yn`, `frozen_N_date`, `frozen_N_location`, `frozen_N_result_raw`, `frozen_N_result_histology`, `frozen_N_result_qualifier`, `frozen_N_result_class`
  - `frozen_N_was_deferred_flag`, `frozen_N_was_malignant_flag`, `frozen_N_was_suspected_flag`, `frozen_N_was_negated_flag`
  - `frozen_N_source_of_data`, `frozen_N_excel_corroborated_flag`, `frozen_N_excel_result_raw`, `frozen_N_surgery_n`

Probe to confirm K (number of frozen-event slots):
```sql
SELECT MAX(CAST(REGEXP_EXTRACT(column_name, '^frozen_(\d+)_yn$', 1) AS INTEGER)) AS max_n
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_frozen_section_patient_rollup_v1';
```

Likely K = 12 (12 × 15 = 180 + 8 metadata = 188 ✓).

---

## 3. Methodology — derivation re-derivation against verified events

Pattern reference: `qc_framework_v1/migrations/101_path_gland_patient_rollup_signoff.sql` (rebuild-then-verify) AND `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` (per-col derivation match).

### 3a. Probe staleness first
Compare `build_ts` of rollup vs `build_ts` of verified events. If events were touched after rollup build, REBUILD via Script 360 logic (mig_101 pattern). If rollup build_ts ≥ events build_ts, skip rebuild and go straight to per-col verification.

```sql
SELECT 
  (SELECT MAX(build_ts) FROM main.canonical_frozen_section_events_v1) AS events_build_ts,
  (SELECT MAX(build_ts) FROM main.canonical_frozen_section_patient_rollup_v1) AS rollup_build_ts;
```

### 3b. Derivation re-derivation (per-col)
For each metadata col, re-derive from events:
- `frozen_section_count` → `COUNT(*) FROM events GROUP BY research_id`
- `frozen_section_any_*_flag` → `BOOL_OR(events.<flag>)`
- `frozen_section_first_date` / `_last_date` → `MIN/MAX(events.<date>)` per patient

For each `frozen_N_*` slot: order events by `(research_id, frozen_section_date, surgery_n)` and use `ROW_NUMBER()` to assign N. Verify rollup matches.

For each `frozen_N_<col>` slot, use this template:
```sql
WITH ordered AS (
  SELECT research_id, ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY frozen_section_date, surgery_n) AS slot, <col>
  FROM main.canonical_frozen_section_events_v1
)
SELECT SUM(CASE WHEN r.frozen_<N>_<col> IS DISTINCT FROM o.<col> THEN 1 ELSE 0 END) AS drift
FROM main.canonical_frozen_section_patient_rollup_v1 r
LEFT JOIN ordered o ON r.research_id = o.research_id AND o.slot = <N>;
```
Drift should be 0 per slot per col.

### 3c. ⚠️ Date type CFs (likely many!)
Per `feedback_clinical_dates_calendar_only.md` (Logan-ratified 2026-04-28), clinical dates MUST be DATE.

This rollup has VARCHAR dates: `frozen_section_first_date`, `frozen_section_last_date`, `frozen_<N>_date` (×K). These violate the rule.

Disposition: **flag as CF-mig<N>-FROZEN-DATE-RETYPE** (joins existing CF-100-DATE-RETYPE). Don't block sign-off; future batch retype migration will address.

### 3d. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_frozen_section_patient_rollup_signoff.sql`
- 187 col flips: `verification_method='derivation_re_derivation'` (or `…_post_events_repair` if rebuild needed)
- 1 already-na: research_id (carry over)
- table_status update at the end

---

## 4. Acceptance gates

- All 187 not_started cols flipped to verified (or na for build_script/build_ts if not pre-na)
- Per-col drift = 0 against re-derived events
- table_status='verified', signoff_migration populated
- 5-gate audit re-run green (run after Cowork's mig_117 audit-fix lands)
- CF rows recorded for VARCHAR date violations

---

## 5. Don't touch (active parallel lanes)

- `canonical_operative_procedure_codes_v1` — Cowork's lane (mig_118)
- `canonical_path_malignant_patient_rollup_v1` / `canonical_path_benign_patient_rollup_v1` — Sibling Cursor lane 12
- `canonical_ete_event_resolved_v1` — Sibling Cursor lane 13
- `canonical_us_*_v2` — Cursor lane 10 if still running

---

## 6. Reference reading

Required:
- Auto-memory: `project_frozen_section_mig_100_closeout.md` (events close-out + 5 reusable patterns + carry-forwards)
- Auto-memory: `project_path_gland_family_complete_2026-04-28.md` (rebuild-then-verify pattern)
- Auto-memory: `project_medications_parathyroid_families_complete_2026-04-29.md` (3 reusable patterns)
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Repo: `qc_framework_v1/migrations/101_path_gland_patient_rollup_signoff.sql` (rebuild template)
- Repo: `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` (signoff template)

Build script source:
- `scripts/360_frozen_section_consolidation.py` (closed mig_100; rebuild logic if needed)

---

## 7. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing rollup
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Pre-snapshot table (only if rebuilding): `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_frozen_section_patient_rollup_v1_pre_mig<N>_20260429`
- Surgical git add (no `-A`, no `scripts/output/` sweeps)

---

## 8. If something unexpected surfaces

- Per-col drift > 0 on more than 5 cols → STOP, rollup likely stale; move to rebuild path
- Slot-N assignment mismatches across patients → events ordering may differ from build; check Script 360 ORDER BY clause
- Number of frozen-event slots K differs from 12 → adjust math; flag if K > 15 (unexpected high frozen-section count for a single patient)
- Date format inconsistency in VARCHAR dates → document in CF; do not normalize during this verification

---

End of prompt. Lane 11 of next 3-prompt batch. Closes the frozen_section family rollup. Update `MEMORY.md` with close-out entry following established pattern.
