# Cursor Prompt — mig_184 R1 AJCC derivation rule scoping (CF-87-AJCC closure track) — RATIFIED

**Date:** 2026-04-30 (revised, post-Logan ratification)
**Lane:** mig_184 / r1_ajcc_derivation_scoping
**Batch (proposed):** `mig_184_r1_ajcc_derivation_scoping_20260430`
**Predecessor:** mig_182 (CLOSED at `b837e67` — read-only investigation; R1 ratified by Logan)
**Posture:** **READ-ONLY scoping + skeleton SQL authoring.** Cursor surfaces full AJCC8/AJCC7 derivation spec, runs cross-source drift cohort under proposed rules, authors `mig_184_apply` skeleton SQL placeholder. **DO NOT execute against MotherDuck.**
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only)

---

## Logan-ratified derivation rules (LOCKED — apply these as the spec)

| # | Rule | Decision |
|---|---|---|
| 1 | AJCC version | **AJCC 8** (2018 revision) |
| 2 | `gross_ete=1` + microscopic-text contradiction (81 events) | **Trust qualifier** → no upgrade. The flag was set automatically when ETE was non-zero, not as a true gross/micro distinction. |
| 3 | N1 unspecified (2,378 events; no nodal location on path-event grain) | **Keep as N1** at path-event grain. Populate N1a/N1b only at PM grain using upstream `cervical_lymph_nodes` operative cols / LLM cnln_detail when available. |
| 4 | Stage-group computation grain | **PM grain only.** Path-event grain holds T/N/M only. Stage group lives at patient grain. |
| 5 | Mixed histology (e.g., MTC \| PTC) | Track each component separately. For manuscript-default `stage_group_resolved`, **use the more aggressive component** (MTC > PTC > FTC). |
| 6 | T4 invasion rules | `gross_ete=1` → **T3b** (strap muscle assumption). Cross-check `canonical_invasion_events_v1` for laryngeal/tracheal/esophageal/RLN invasion → upgrade to **T4a**. Prevertebral/mediastinal/carotid → **T4b**. |
| 7 | Size-unavailable (Logan override) | **Recover via `COALESCE(size_greatest_dimension_cm, tumor_size_cm_per_surgery)`** — recovers 480 of 601 size-null events. Auto-default microcarcinoma → T1a (32 events / 29 pts). NIFTP → exclude (1 event / 1 pt). Anaplastic → default T4 if no other staging info (3 events / 3 pts). **85 truly-unrecoverable events (72 patients)** logged in `exports/mig184_r1_adjudication_20260430/r1c_size_unavailable_residual_121events.csv` for Logan hand-curation. |
| 8 | Age-unknown (Logan override) | **No issue.** All 10,871 patients have `age_at_surgery` populated (0 nulls). Auto-resolves. |

---

## Mission

R1 closes CF-87-AJCC by re-deriving each stage component from the **verified findings** layer into a new family of `*_resolved` columns:

- `t_stage_ajcc8_resolved` (path-event + patient grain)
- `n_stage_ajcc8_resolved` (path-event + patient grain — N1a/N1b populated only at patient grain via upstream LN data per Rule #3)
- `m_stage_ajcc8_resolved` (patient grain only)
- `stage_group_ajcc8_resolved` (patient grain only — Rule #4)
- equivalent AJCC7 set (for migration compatibility)
- `*_resolution_source` audit cols (which finding(s) drove the stage)
- `*_resolution_confidence` (high / medium / low)

Legacy stored cols remain unchanged. Manuscript SQL prefers `*_resolved` going forward; legacy retained for audit.

---

## Required scope

### §1 Encode the 8 ratified rules as SQL CASE expressions

Author `qc_framework_v1/migrations/184_r1_ajcc_derivation_skeleton_20260430.sql` with the following block structure:

- §0 pre-flight invariants (CPM 10,871/10,871; no batch_id rows yet)
- §A pre-snapshot tables for affected cols on `canonical_path_malignant_events_v1` and `canonical_patient_master`
- §B `ALTER TABLE ... ADD COLUMN` for the new `*_resolved` family (path-event grain): `t_stage_ajcc8_resolved`, `n_stage_ajcc8_resolved`, `m_stage_ajcc8_resolved`, plus AJCC7 equivalents, plus `*_resolution_source`, `*_resolution_confidence`
- §C `ALTER TABLE ... ADD COLUMN` for the new `*_resolved` family (patient grain): same set + `stage_group_ajcc8_resolved`, `stage_group_ajcc7_resolved`, plus tumor-class disambiguation cols if needed for Rule #5
- §D **T-stage CASE** encoding Rules #1, #2, #6, #7 (with COALESCE size fallback). Microcarcinoma defaults to T1a. NIFTP excluded. 85 hand-curation events from r1c CSV: leave NULL with `t_resolution_source='size_residual_logan_pending'` until Logan provides the values.
- §E **N-stage CASE** encoding Rule #3 (N1 unspecified at path grain → keep as N1; lookup N1a/N1b from upstream at PM grain via JOIN to `cervical_lymph_nodes` and `note_entities_llm_cervical_ln_detail`)
- §F **M-stage CASE** at PM grain (default M0 unless distant-mets evidence found)
- §G **Stage group CASE** at PM grain encoding Rules #4 and #5 (use `age_at_surgery` for 55-cutoff; pick more aggressive histology component for mixed)
- §H Registry note appendix closing `CF-87-AJCC` on the 36 path-malignant col rows + 9 ETE event_resolved col rows
- §I `cpm_reconciliation_provenance_v1` row insert
- §J Post-state probes: counts per resolved col, drift cohort vs legacy

**Mark the apply SQL as `-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY` (no longer pending ratification).**

### §2 Cross-source drift cohort under proposed R1 derivation

Run **read-only**:

```sql
WITH derived AS (
  SELECT pm.research_id, <full CASE for t/n/m/sg under R1 rules> ...
  FROM main.canonical_patient_master pm
  LEFT JOIN ...
)
SELECT
  COUNT(*) FILTER (WHERE pm.ajcc8_t_stage IS DISTINCT FROM derived.t_resolved) AS t_changes,
  COUNT(*) FILTER (WHERE pm.ajcc8_n_stage IS DISTINCT FROM derived.n_resolved) AS n_changes,
  COUNT(*) FILTER (WHERE pm.ajcc8_stage_group IS DISTINCT FROM derived.sg_resolved) AS sg_changes,
  COUNT(DISTINCT pm.research_id) FILTER (WHERE derived.t_resolved IS NULL) AS t_uncalculable_pts,
  COUNT(DISTINCT pm.research_id) FILTER (WHERE derived.sg_resolved IS NULL) AS sg_uncalculable_pts
FROM main.canonical_patient_master pm LEFT JOIN derived USING (research_id);
```

Surface the impact of R1 derivation on the cohort. Compare to mig_182's drift counts (28.81% T mismatch, 52.4% sg_v2 mismatch) — most should resolve under the new rules.

### §3 Generate residual adjudication CSVs (3 more)

The first two CSVs already exist Cowork-generated:
- ✅ `exports/mig184_r1_adjudication_20260430/r1a_ete_t_stage_upgrade_review.csv` (213 rows; Logan-resolved via Rules #1, #2 — no per-row review needed)
- ✅ `exports/mig184_r1_adjudication_20260430/r1c_size_unavailable_residual_121events.csv` (121 rows: 32 PTMC auto-T1a + 1 NIFTP exclude + 3 anaplastic + 85 hand-curate)

Cursor generates these 3 additional CSVs:

- `r1b_n1_unspecified_pm_grain.csv` — patients at PM grain where N1 unspecified can be split using upstream `cervical_lymph_nodes` ops cols / LLM cnln_detail. Show: rid, current PM `ajcc8_n_stage`, upstream lateral/central evidence available, proposed N1a vs N1b. Only patients where upstream EVIDENCE EXISTS (so Logan has actionable data).
- `r1d_t4_invasion_evidence_review.csv` — patients with `canonical_invasion_events_v1` evidence of laryngeal/tracheal/esophageal/RLN invasion (T4a candidates) or prevertebral/mediastinal/carotid invasion (T4b candidates). Cross-tab: rid, invasion_type, finding_status, current PM T-stage, proposed resolved T-stage.
- `r1e_mixed_histology_stage_group.csv` — patients with `histologic_types_all` containing multi-component (e.g., 'MTC | PTC'). Show: rid, components, T-stage, N-stage, M-stage per component, proposed `stage_group_resolved` per Rule #5.

### §4 Audit/report

`qc_framework_v1/reports/mig_184_r1_ajcc_derivation_scoping_20260430.md`:
- §1 ratified 8-rule spec (verbatim, Logan-locked)
- §2 cross-source drift cohort under R1 derivation
- §3 inventory of 5 adjudication CSVs (2 Cowork-generated + 3 Cursor-generated)
- §4 list of remaining row-level decisions: 85 size hand-curate (r1c) + ~N from r1b/r1d/r1e
- §5 unblocking checklist: Cowork applies skeleton SQL once Cursor authors and Logan reviews the residuals

---

## Governance reminders

- **Read-only investigation only.** No `query_rw` against MotherDuck. No SQL apply.
- Skeleton SQL is for Cowork to apply, NOT for Cursor. Mark blocks `-- Cowork applies via Path C`.
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits.
- Surgical git add only.

---

## Deliverables

1. `qc_framework_v1/migrations/184_r1_ajcc_derivation_skeleton_20260430.sql`
2. `qc_framework_v1/reports/mig_184_r1_ajcc_derivation_scoping_20260430.md`
3. `exports/mig184_r1_adjudication_20260430/r1b_n1_unspecified_pm_grain.csv`
4. `exports/mig184_r1_adjudication_20260430/r1d_t4_invasion_evidence_review.csv`
5. `exports/mig184_r1_adjudication_20260430/r1e_mixed_histology_stage_group.csv`

Commit message: `qc: mig_184 R1 AJCC derivation scoping (Logan-ratified 8 rules) + 3 adjudication CSVs + apply skeleton SQL`

---

End of prompt.
