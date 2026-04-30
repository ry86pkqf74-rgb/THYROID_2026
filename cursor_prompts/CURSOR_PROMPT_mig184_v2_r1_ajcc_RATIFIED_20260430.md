# Cursor Prompt — mig_184_v2 R1 AJCC derivation RATIFIED (replaces 17b5d8a)

**Date:** 2026-04-30
**Lane:** mig_184_v2 / r1_ajcc_derivation_RATIFIED
**Batch (proposed):** `mig_184_v2_r1_ajcc_derivation_ratified_20260430`
**Predecessor:** mig_184 (Cursor authored at `17b5d8a` against an OUTDATED prompt before Logan ratified the 8 rules — superseded by this prompt)
**Posture:** **READ-ONLY scoping + skeleton SQL.** No execute against MotherDuck.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only)

**IMPORTANT:** Discard the artifacts at commit `17b5d8a` (`qc_framework_v1/migrations/184_*.sql`, `qc_framework_v1/reports/mig_184_*.md`, and `exports/mig184_r1_adjudication_20260430/r1b/r1d/r1e CSVs`). Re-author from scratch using the **8 ratified rules** below.

---

## Logan-ratified derivation rules (LOCKED — apply these as the spec)

| # | Rule | Decision (verbatim) |
|---|---|---|
| 1 | AJCC version | **AJCC 8** (2018 revision) |
| 2 | `gross_ete=1` + microscopic-text contradiction (81 events) | **Trust qualifier** → no upgrade |
| 3 | N1 unspecified (2,378 events; no nodal location on path-event grain) | **Keep as N1** at path-event grain. Populate N1a/N1b only at PM grain using upstream `cervical_lymph_nodes` operative cols / LLM cnln_detail when available. |
| 4 | Stage-group computation grain | **PM grain only.** Path-event grain holds T/N/M only. |
| 5 | Mixed histology (e.g., MTC \| PTC) | Track each component separately. For manuscript-default `stage_group_resolved`, **use the more aggressive component** (MTC > PTC > FTC). |
| 6 | T4 invasion rules | `gross_ete=1` → **T3b** (strap muscle assumption). Cross-check `canonical_invasion_events_v1` for laryngeal/tracheal/esophageal/RLN invasion → **T4a**. Prevertebral/mediastinal/carotid → **T4b**. |
| 7 | Size-unavailable (Logan override) | **`COALESCE(size_greatest_dimension_cm, tumor_size_cm_per_surgery)`** recovers 480 of 601 size-null events. **Microcarcinoma without size → T1a default** (per AJCC 8 microcarcinoma definition: ≤1 cm). NIFTP → **EXCLUDE** (mig_186 lane handles this). Anaplastic → default T4. **72 truly-residual patients** in `exports/mig184_r1_adjudication_20260430/r1c_size_unavailable_residual_121events.csv` await Logan hand-curation. |
| 8 | Age-unknown (Logan override) | **No issue.** All 10,871 patients have `age_at_surgery` populated (0 nulls). Auto-resolves. |

---

## Mission

R1 closes CF-87-AJCC by re-deriving each stage component from verified findings into new `*_resolved` columns. Legacy stored cols remain unchanged. Manuscript SQL prefers `*_resolved` going forward.

---

## Required scope

### §1 Encode the 8 ratified rules as SQL CASE expressions

Author `qc_framework_v1/migrations/184_v2_r1_ajcc_derivation_ratified_20260430.sql`:

- §0 pre-flight invariants (CPM 10,871/10,871; no batch_id rows yet for mig_184_v2)
- §A pre-snapshot tables for the 36 path-malignant CFs + 9 ETE event_resolved CFs + the affected PM AJCC cols
- §B `ALTER TABLE ... ADD COLUMN` for the new `*_resolved` family on `canonical_path_malignant_events_v1`: `t_stage_ajcc8_resolved`, `n_stage_ajcc8_resolved`, `m_stage_ajcc8_resolved`, plus AJCC7 equivalents, plus `*_resolution_source` (VARCHAR), `*_resolution_confidence` (VARCHAR: high/medium/low/uncalculable)
- §C `ALTER TABLE ... ADD COLUMN` for the new `*_resolved` family on `canonical_patient_master`: same set + `stage_group_ajcc8_resolved`, `stage_group_ajcc7_resolved`
- §D **T-stage UPDATE** — encode Rules #1, #2, #6, #7. Microcarcinoma → T1a default. NIFTP → EXCLUDE (no resolved value). 85 hand-curate residuals from r1c CSV → leave NULL with `t_resolution_source='size_residual_logan_pending'`.
- §E **N-stage UPDATE** — Rule #3: at path-event grain, copy through n_stage_ajcc8 (keep N1 as N1). At PM grain, attempt N1 → N1a/N1b via JOIN to upstream `cervical_lymph_nodes` ops cols / LLM cnln_detail.
- §F **M-stage UPDATE** at PM grain (M0 default unless distant-mets evidence)
- §G **Stage group UPDATE** at PM grain encoding Rules #4 + #5 (use `age_at_surgery` for 55-cutoff; pick more aggressive component for mixed histology)
- §H Registry note appendix closing `CF-87-AJCC` on the 36 + 9 = 45 col rows
- §I `cpm_reconciliation_provenance_v1` row insert
- §J Post-state probes: counts per resolved col, drift cohort vs legacy

**Mark blocks as `-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY` (no longer pending ratification).**

### §2 Cross-source drift cohort under R1 derivation (read-only probe)

Compute T/N/M/sg shift counts from legacy → resolved per Rules. Compare to mig_182's drift baseline (28.81% T mismatch, 52.4% sg_v2 mismatch). Most should resolve under Rules.

### §3 Generate residual adjudication CSVs (3 more — already-existing CSVs are r1a + r1c)

**Already exist:**
- ✅ `exports/mig184_r1_adjudication_20260430/r1a_ete_t_stage_upgrade_review.csv` (213 rows; resolved by Rules #1, #2)
- ✅ `exports/mig184_r1_adjudication_20260430/r1c_size_unavailable_residual_121events.csv` (121 rows: 32 PTMC auto-T1a + 1 NIFTP exclude + 3 anaplastic + 85 hand-curate)

**Author these 3:**
- `r1b_n1_unspecified_pm_grain.csv` — patients where N1 unspecified can be split using upstream `cervical_lymph_nodes` ops cols / LLM cnln_detail. Show: rid, current PM `ajcc8_n_stage`, upstream lateral/central evidence available, proposed N1a vs N1b. Only include patients where evidence EXISTS.
- `r1d_t4_invasion_evidence_review.csv` — patients with `canonical_invasion_events_v1` evidence of laryngeal/tracheal/esophageal/RLN invasion (T4a candidates) or prevertebral/mediastinal/carotid invasion (T4b candidates).
- `r1e_mixed_histology_stage_group.csv` — patients with `histologic_types_all` containing multi-component (e.g., 'MTC | PTC'). Show: rid, components, T/N/M per component, proposed `stage_group_resolved` per Rule #5.

### §4 Audit/report

`qc_framework_v1/reports/mig_184_v2_r1_ajcc_derivation_ratified_20260430.md`:
- §1 ratified 8-rule spec (verbatim, Logan-locked)
- §2 cross-source drift cohort under R1 derivation
- §3 inventory of 5 adjudication CSVs (2 Cowork-pre-generated + 3 Cursor-generated)
- §4 list of remaining row-level decisions: 85 size hand-curate (r1c) + ~N from r1b/r1d/r1e
- §5 unblocking checklist: Cowork applies skeleton SQL once Cursor authors and Logan reviews the residuals

---

## Discard the prior mig_184 artifacts

The following files at commit `17b5d8a` are SUPERSEDED and must be removed in this commit:
- `qc_framework_v1/migrations/184_r1_ajcc_derivation_skeleton_20260429.sql` (or whatever Cursor authored against the old prompt)
- `qc_framework_v1/reports/mig_184_r1_ajcc_derivation_scoping_20260429.md` (or 20260430)
- Any r1b/r1d/r1e CSVs Cursor generated against the old rules

Rename to `_v2_*` if present, or `git rm` the obsolete versions.

---

## Governance reminders

- Read-only investigation only. No `query_rw`.
- Skeleton SQL is for Cowork to apply via Path C.
- Author = `Logan Glosser <logan.glosser@gmail.com>`.

---

## Deliverables

1. `qc_framework_v1/migrations/184_v2_r1_ajcc_derivation_ratified_20260430.sql`
2. `qc_framework_v1/reports/mig_184_v2_r1_ajcc_derivation_ratified_20260430.md`
3. `exports/mig184_r1_adjudication_20260430/r1b_n1_unspecified_pm_grain.csv`
4. `exports/mig184_r1_adjudication_20260430/r1d_t4_invasion_evidence_review.csv`
5. `exports/mig184_r1_adjudication_20260430/r1e_mixed_histology_stage_group.csv`

Commit message: `qc: mig_184_v2 R1 AJCC derivation RATIFIED (Logan-ratified 8 rules; supersedes 17b5d8a) + 3 adjudication CSVs + apply skeleton SQL`

---

End of prompt.
