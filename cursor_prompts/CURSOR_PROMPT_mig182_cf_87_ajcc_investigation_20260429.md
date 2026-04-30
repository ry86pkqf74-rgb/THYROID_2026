# Cursor Prompt — mig_182 CF-87-AJCC investigation (36 col-impact)

**Date:** 2026-04-29 (very late evening)
**Lane:** mig_182 / cf_87_ajcc_investigation
**Batch (proposed):** `mig_182_cf_87_ajcc_investigation_20260429`
**Predecessor:** CF-87-AJCC tagged on ~36 cols across canonical layer; tagged in mig_87 close-out (`canonical_path_malignant_events_v1`); status TBD per handoff §7.2 top open CFs
**Posture:** **READ-ONLY investigation only.** Cursor surfaces what the AJCC drift is, where it's tagged, what manuscript impact looks like. Logan reviews + ratifies fix plan in follow-up.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only)

---

## Mission

`CF-87-AJCC` shows up in the handoff §7.2 top open CFs list with **36 col-impact** but the open work item says "TBD". The CF was opened in mig_87 (`canonical_path_malignant_events_v1` close-out). mig_182 surfaces the actual scope, semantic content, and proposed fix plan.

---

## Required scope

### §1 Locate every CF-87-AJCC mention

```sql
SELECT schema_name, table_name, column_name, batch_id, SUBSTR(notes, 1, 400) AS notes_excerpt
FROM main.canonical_column_verification_registry_v1
WHERE notes ILIKE '%CF-87-AJCC%' OR notes ILIKE '%CF-87%AJCC%' OR notes ILIKE '%AJCC%drift%'
ORDER BY table_name, column_name;
```

Output count of matches; list of distinct (table_name, column_name) pairs; map to col families.

### §2 Read the original CF context

Search git log for `mig_87` close-out report (likely at `qc_framework_v1/reports/mig_87_*.md` or `project_*` memory). Quote the original CF rationale verbatim. Identify:
- What AJCC version is in use (7th edition vs 8th edition?)
- What col(s) hold the AJCC stage value
- What the "drift" actually is (e.g., differential between path-malignant-events stage assertion vs PM-rolled-up stage)

### §3 Quantify drift on live data

For the AJCC stage cols identified in §2, run:
```sql
-- For each pair of cols where drift might exist, count mismatches
SELECT
  COUNT(*) FILTER (WHERE col_a IS DISTINCT FROM col_b) AS n_mismatches,
  COUNT(*) FILTER (WHERE col_a IS NOT NULL AND col_b IS NOT NULL AND col_a <> col_b) AS n_both_nonnull_differ,
  -- distribution
  ...
FROM main.canonical_patient_master;
```

Surface counts per dimension (T-stage, N-stage, M-stage, group stage, AJCC version).

### §4 Cross-source reconciliation probe

If AJCC stage is computed from multiple sources (path events + clinical input + imaging), surface:
- Per-source coverage (n_pts with stage from each source)
- Pairwise concordance (path vs clinical, etc.)
- Patients with conflicting stages (manuscript-blocking)

### §5 Manuscript-impact assessment

Quantify how many patients/analyses would be affected by the drift:
- N analytic cohort patients with non-null AJCC stage
- N patients where AJCC stage differs across sources
- Risk: if a manuscript stratifies by AJCC stage, what % could shift bucket?

### §6 Fix plan recommendation

Propose 1-3 options (R1/R2/R3) for closing CF-87-AJCC. E.g.:
- R1: pick path-events-derived stage as canonical → write `*_resolved` cols + audit
- R2: COALESCE(path, clinical) by some priority rule
- R3: surface drift in a manuscript-flagged subset; otherwise leave both cols

---

## Deliverables

`qc_framework_v1/reports/mig_182_cf_87_ajcc_investigation_20260429.md` with:
1. CF-87-AJCC complete inventory (table + col list)
2. Original mig_87 context (verbatim)
3. Drift quantification tables
4. Cross-source reconciliation tables
5. Manuscript-impact assessment
6. Fix plan options + recommendation

No apply SQL in this lane (Logan ratification needed first).

Commit message: `qc: mig_182 CF-87-AJCC investigation (36 col-impact; scoping for fix plan ratification)`

---

## Governance reminders

- **Read-only investigation only.** No SQL apply.
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits.

---

End of prompt.
