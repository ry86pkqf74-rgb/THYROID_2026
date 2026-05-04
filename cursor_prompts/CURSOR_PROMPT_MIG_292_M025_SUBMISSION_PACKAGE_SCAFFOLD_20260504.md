# Cursor Composer Dispatch — mig_292: M025 TIRADS Performance submission package scaffold

**Generated:** 2026-05-04 by Cowork at HEAD `170ee3d`.
**Lane:** mig_292 — Build `M025_submission_package_v1_0/`. M025 cohort 3,375 + sens/spec/PPV/NPV per-TIRADS-bucket are locked. With mig_288 + the new `tirads_resolved` clean enum on CPM, build the full submission package.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer**.
**Estimated runtime:** 2-3 hours.
**Triggered by:** mig_288 + M025 ready-for-writing brief.
**Severity:** MED. Unblocks M025 writing chat.

---

## §0 — First message to paste into Cursor Chat

> mig_292 dispatch. Build `M025_submission_package_v1_0/` mirroring `M044_submission_package_v1_0/` structure. Use `manuscript_workspace.cohort_m025_tirads_performance_v1` + new `canonical_patient_master.tirads_resolved` (post-mig_288). MotherDuck DB is `thyroid_canonical_publication_v1_0`. Walk through table structure before generating .docx files.

---

## §1 — Pre-task: existing inputs

- [`snowflake_trial/reports/m025_tirads_performance.md`](computer:///Users/ros/THyroid 2026/snowflake_trial/reports/m025_tirads_performance.md) — sens/spec/PPV/NPV per TIRADS bucket
- [`manuscript_outputs/v1_0_20260501/M025_READY_FOR_WRITING_BRIEF.md`](computer:///Users/ros/THyroid 2026/manuscript_outputs/v1_0_20260501/M025_READY_FOR_WRITING_BRIEF.md) — Cowork ready-for-writing brief
- mig_288 added `canonical_patient_master.tirads_resolved` clean enum (TR1=340, TR2=299, TR3=845, TR4=496, TR5=1,402)
- M044 + M038 submission packages — pattern reference

## §2 — Tables (proposed)

| # | Title |
|---|---|
| 1 | Cohort demographics by TIRADS category (TR1/2/3/4/5) |
| 2 | Performance metrics per TIRADS cutoff (sens/spec/PPV/NPV/accuracy) |
| 3 | TIRADS × Bethesda cross-stratification |
| 4 | Sub-analyses by nodule size + composition |
| Supp S1 | ROC analysis with 95% CIs |
| Supp S2 | Comparison to literature benchmarks (Tessler 2017 / Middleton 2017) |

## §3 — Figures (proposed)

| # | Title |
|---|---|
| 1 | Cohort flow diagram |
| 2 | ROC curve with AUC |
| 3 | Per-bucket malignancy rate forest-style |
| 4 | TIRADS × Bethesda heatmap |

## §4 — Reproducibility SQL package

Mirror M044 style. Critical query: cohort cohort_m025_tirads_performance_v1 + LEFT JOIN canonical_patient_master tirads_resolved post-mig_288.

```sql
SELECT
  pm.tirads_resolved,
  COUNT(*) AS n,
  COUNT_IF(c.is_malignant) AS n_malig,
  -- Per-cutoff sens/spec
  -- (precompute per category)
FROM manuscript_workspace.cohort_m025_tirads_performance_v1 c
LEFT JOIN main.canonical_patient_master pm USING (research_id)
GROUP BY pm.tirads_resolved
ORDER BY pm.tirads_resolved;
```

## §5 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_292', CURRENT_TIMESTAMP, 'cursor_composer_mig292',
 'mig_292: M025 TIRADS Performance submission package v1.0 built. Tables 1-4 + Supp S1-S2 + 4 figures. Uses tirads_resolved enum from mig_288. Closes M025 ready-for-writing gate.');
```

---

## §6 — Surgical git add

```
M025_submission_package_v1_0/
qc_framework_v1/migrations/292_m025_submission_package_20260504.sql
scripts/output/mig_292_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_292_M025_SUBMISSION_PACKAGE_SCAFFOLD_20260504.md
```

---

**End of mig_292 dispatch.**
