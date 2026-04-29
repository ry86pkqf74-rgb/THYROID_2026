# Cursor Agent Task — `canonical_patient_master` ETE CLUSTER slice (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_136 PMH+PSH landing)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (~36 cols)
**Run order:** Lane 29 of new 4-prompt batch (next-batch numbering: mig_140 ETE / mig_141 Survival / mig_142 RAI / mig_143 small-clusters bundle)

---

## 1. Goal

Continue patient_master verification with the **extrathyroidal extension (ETE) cluster** (~36 unverified cols covering ETE detection across path/op/imaging sources, ETE adjudication chain, grade ladder, and PRM concordance flags).

Probe scope:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (regexp_matches(column_name, '(^|_)ete(_|$)')
       OR column_name ILIKE '%extrathyroidal%')
  AND column_name NOT LIKE '%completeness%'
  AND column_name NOT LIKE '%detection%'
  AND column_name NOT LIKE '%undetectable%'
  AND column_name NOT LIKE 'ct_thyroid_heterogeneous%'
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY column_name;
```

Confirm count is **exactly 36** before proceeding (Cowork survey 2026-04-29). The substring filter excludes false-positive `_ete_` matches inside `completeness`, `detection`, `undetectable`, and `ct_thyroid_heterogeneous` cols (those belong to other clusters).

Sub-clusters:

- **`any_ete_*` aggregate flags** (~7 cols): `any_ete_anywhere`, `any_ete_in_imaging`, `any_ete_in_op_or_path`, `any_ete_present_not_further_specified_anywhere/_in_imaging/_in_op_or_path`, `any_microscopic_ete_anywhere` — derived BOOL_OR aggregations
- **ETE adjudication chain** (~5 cols): `ete_adjudicated_flag`, `ete_adjudication_confidence`, `ete_adjudication_evidence`, `ete_adjudication_reasoning`, `ete_adjudication_t_adjustment` — from `canonical_ete_event_resolved_v1` (mig_121 verified, 57/62 cols)
- **ETE grade ladder** (~10 cols): `ete_grade`, `ete_grade_adjudicated`, `ete_grade_clean`, `ete_grade_final`, `ete_grade_final_v2`, `ete_grade_source`, `ete_op_note_confidence`, `ete_op_note_grade`, `ete_ordinal_worst`, `ete_original_grade`, `ete_original_source`, `ete_refined_grade`, `worst_ete_v10` — from `canonical_ete_subgrade_events_v1` (mig_114 verified, 5/17 cols) + `canonical_ete_event_resolved_v1`
- **ETE pathology / op detail** (~7 cols): `ete_any_present_path`, `ete_subgrade_method`, `ete_subgrade_note`, `gm_path_ete_raw`, `gross_ete_flag`, `microscopic_ete_t3b_corrected`, `nlp_path_ete_mentioned` — from `canonical_path_malignant_events_v1` (mig_89, 56/56 verified) + `canonical_invasion_events_v1` (mig_95)
- **PRM ETE rules** (~4 cols): `prm_ete_imaging_path_concordance`, `prm_ete_path_confirmed`, `prm_ete_rule_applied`, `prm_margin_with_gross_ete` — concordance rules; derive against existing PM build SSOT

---

## 2. Methodology — derivation re-derivation against verified ETE family + cross-table joins

Pattern reference: Lane 22 pathology template + `qc_framework_v1/migrations/121_ete_event_resolved_inline_family_signoff.sql` + `qc_framework_v1/migrations/114_ete_subgrade_family_signoff.sql`.

### 2a. Per-col derivation map (representative)

- `any_ete_in_op_or_path` → `BOOL_OR(canonical_invasion_events_v1.ete_present OR canonical_path_malignant_events_v1.ete_present)` per pt
- `any_ete_in_imaging` → from imaging-source ETE flags (CT/US ETE — note: imaging cluster cols may not yet be verified; cross-check at fact level)
- `any_microscopic_ete_anywhere` → BOOL_OR over microscopic-grade rows in ete_subgrade_events
- `ete_grade_final` / `ete_grade_final_v2` → from `canonical_ete_event_resolved_v1` (resolved ladder; v1 vs v2 schema generations — check the col description on the upstream registry)
- `ete_adjudicated_flag` → `BOOL_OR(canonical_ete_event_resolved_v1.adjudicated)` per pt
- `ete_adjudication_confidence` → MAX or aggregate per pt
- `gross_ete_flag` → from path_malignant `gross_ete` col
- `microscopic_ete_t3b_corrected` → derived per Logan-ratified rule (capsular-microscopic ETE recoded per AJCC 8 t3b distinction); check the SSOT script
- `prm_ete_imaging_path_concordance` → derive against imaging vs path source agreement; if imaging-source ETE not yet built, mark as `na` with `verification_method='upstream_imaging_ete_pending'` and open `CF-mig140-PM-ETE-IMAGING-UPSTREAM-PENDING`
- `prm_ete_rule_applied` → categorical rule-name from existing PM rule chain
- `worst_ete_v10` / `ete_grade_clean` / `ete_refined_grade` → version-pinned from event_resolved chain

### 2b. ⚠️ Findings vs staging (Logan-ratified)

Per `feedback_findings_vs_staging.md`: anatomic finding cols are primary; staging cols (e.g., AJCC `t4a` implication) follow findings. If `microscopic_ete_t3b_corrected` is a staging-derivation, document it as a derived-from-finding col, not as a primary finding.

### 2c. ⚠️ NULL vs FALSE caveat

Per `feedback_recurrence_imaging_n_events_null.md`: `any_ete_*` BOOLEANs should be NULL not FALSE for patients with no source data on the relevant axis. For PM rows where `pm.canonical_path_malignant_events_v1.research_id` doesn't exist (no path data), `any_ete_in_op_or_path` should be NULL, not FALSE. Drift probes use COALESCE before IS DISTINCT FROM.

### 2d. ⚠️ Cohort-uniformity sanity check (CRITICAL)

Per `feedback_motherduck_direct_check.md` and the mig_135 21-degenerate-col incident: for **every BOOLEAN col flipped** in this lane, run:

```sql
SELECT SUM(CASE WHEN <col> THEN 1 ELSE 0 END) AS n_true,
       SUM(CASE WHEN NOT <col> THEN 1 ELSE 0 END) AS n_false,
       SUM(CASE WHEN <col> IS NULL THEN 1 ELSE 0 END) AS n_null
FROM main.canonical_patient_master;
```

**If n_true=0 across the 10,871 cohort, do not pass-through verify.** Classify per the §8.2 taxonomy:
- Type A real cohort absence → keep verified, tag `CF-mig140-COHORT-INVARIANT-<col>`
- Type B upstream not extracted → flip to `na`, open `CF-mig140-EXPAND-UPSTREAM-<col>`
- Type C helper-script artifact → flip to `na`, doc the script

For ETE specifically: `any_ete_in_imaging` may be Type B (imaging-source ETE not yet structured). `any_microscopic_ete_anywhere` should have meaningful non-zero TRUE count in a thyroid-cancer cohort — if 0, that's a real bug.

### 2e. ⚠️ Date-type CFs

Watch for any TIMESTAMP / VARCHAR clinical date cols → open `CF-mig140-PM-ETE-DATE-RETYPE`.

### 2f. Sign-off SQL

File: `qc_framework_v1/migrations/140_patient_master_ete_cluster_signoff_20260429.sql`

```
batch_id = 'mig_140_patient_master_ete_cluster_20260429'
verification_method options:
  - 'derivation_vs_canonical_ete_event_resolved_v1'
  - 'derivation_vs_canonical_ete_subgrade_events_v1'
  - 'derivation_vs_canonical_path_malignant_events_v1'
  - 'derivation_vs_canonical_invasion_events_v1'
  - 'patient_level_aggregate_ete_per_axis'
  - 'prm_rule_concordance_chain'
```

If mig_140 collides (e.g. rid 68 fix took it), fall through to next available number; document collision in close-out memory.

---

## 3. Acceptance gates

- ~36 ETE-cluster cols flipped
- 0 drift on derivation re-derivation (or ≤5% with documented note)
- Cohort parity 10,871 confirmed
- Cohort-uniformity sweep run on every BOOLEAN; no degenerate cols pass through unflagged
- CF rows for any date-type violations
- gate 4 = 0
- PM `n_verified` advances by exactly the cluster count

---

## 4. Don't touch (active parallel lanes)

- `canonical_patient_master` MOLECULAR cluster — Lane 27 (in flight, expected mig_137; SCOPE REVISED to ~3 cols only — see updated prompt)
- RECURRENCE-RESPONSE cluster — Lane 28 (in flight, expected mig_138; SCOPE REVISED to ~4 cols only)
- SURVIVAL cluster — Sibling Lane 30 (mig_141)
- RAI cluster — Sibling Lane 31 (mig_142)
- SMALL-CLUSTERS bundle (FNA/Demographics/Frozen/Staging) — Sibling Lane 32 (mig_143)

---

## 5. Reference reading

Required:
- Auto-memory: `project_invasion_family_signoff_2026-04-28.md`
- Auto-memory: `feedback_findings_vs_staging.md`
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_recurrence_imaging_n_events_null.md`
- Auto-memory: `feedback_etevent_resolved_cross_check.md` (CAST(rid AS VARCHAR) for ete cross-checks)
- Auto-memory: `project_ete_documentation_rate.md` (raw `path_malignant.extrathyroidal_extension='x'` for 5,072/6,689 = 76%; clean overlay correctly drops `x`/`c/a`)
- Repo: `qc_framework_v1/migrations/121_ete_event_resolved_inline_family_signoff.sql`
- Repo: `qc_framework_v1/migrations/114_ete_subgrade_family_signoff.sql`
- Repo: `qc_framework_v1/migrations/95_ete_taxonomy_and_invasion_rollups.sql`
- Repo: `qc_framework_v1/migrations/89_path_malignant_table_signoff.sql`

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing ETE cluster
- Surgical git add (no `git add -A`); explicit paths only
- DuckDB `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts
- Explicit `WHERE verification_status='not_started'` so re-runs are no-ops
- Lint Python before commit: `python3 -m py_compile <file>` + `pyflakes`

---

## 7. If something unexpected surfaces

- Drift > 5% on any `ete_grade_*` col → check ete_event_resolved_v1 build_ts vs PM build_ts; PM may need a rebuild against fresh upstream
- `microscopic_ete_t3b_corrected` rule undocumented → STOP, ask Logan for the SSOT script reference
- `any_ete_in_imaging` cohort-uniform-FALSE → expected (imaging-source ETE not yet structured); tag CF-mig140-EXPAND-UPSTREAM-IMAGING-ETE; flip to `na`
- `prm_ete_*` cols don't have a clean SSOT rule definition → STOP, ask Logan
- More than 5 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 29 of 4-prompt batch (target: PM `n_verified` 769 → 805).
