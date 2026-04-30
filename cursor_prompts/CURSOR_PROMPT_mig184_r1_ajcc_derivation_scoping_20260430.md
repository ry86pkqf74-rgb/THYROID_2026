# Cursor Prompt — mig_184 R1 AJCC derivation rule scoping (CF-87-AJCC closure track)

**Date:** 2026-04-30
**Lane:** mig_184 / r1_ajcc_derivation_scoping
**Batch (proposed):** `mig_184_r1_ajcc_derivation_scoping_20260430`
**Predecessor:** mig_182 (CLOSED at `b837e67` — read-only investigation; R1 ratified by Logan = re-derive AJCC from verified findings into new `*_resolved` cols)
**Posture:** **READ-ONLY scoping only.** Cursor surfaces the full AJCC8 derivation spec, identifies every adjudication gap, and authors placeholder skeleton SQL only. **DO NOT execute against MotherDuck.** Apply lane comes after Logan ratifies the rules.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only)

---

## Logan ratification — already given

Logan ratified **R1 — Re-derive AJCC from verified findings into new `*_resolved` cols** (preserve legacy cols unchanged until sign-off). Cowork has already produced the first adjudication CSV at:
- `exports/mig184_r1_adjudication_20260430/r1a_ete_t_stage_upgrade_review.csv` (213 ETE→T-stage candidates)

mig_184 expands this scoping into the full AJCC7/AJCC8 derivation rule spec, identifying every clinical-rule gap that requires Logan adjudication before the apply lane can run.

---

## Mission

The CF-87-AJCC drift summary from mig_182:
- 28.81% path-event T-stage stored-vs-findings drift (1,781 / 6,182 events)
- 25.8% path_worst_ajcc8_t_stage vs path_worst_inferred_ajcc8_t_from_findings (1,030 / 3,993 patients)
- 52.4% PM `ajcc8_stage_group_v2` vs canonical `ajcc8_stage_group` (2,160 / 4,122)
- 79.16% of malignant patients have *some* cross-source AJCC discordance

R1 closes this by re-deriving each stage component from the **verified findings** layer (`canonical_path_malignant_events_v1` columns Logan has signed off on: `extrathyroidal_extension`, `gross_ete`, `size_greatest_dimension_cm`, `ln_examined`, `ln_involved`, `extranodal_extension`, etc.) into a new family of `*_resolved` columns:

- `t_stage_ajcc8_resolved` (path-event + patient grain)
- `n_stage_ajcc8_resolved` (path-event + patient grain)
- `m_stage_ajcc8_resolved` (patient grain only)
- `stage_group_ajcc8_resolved` (patient grain — needs age and DTC vs MTC/ATC)
- equivalent AJCC7 set
- `*_resolution_source` audit cols (which finding(s) drove the stage)
- `*_resolution_confidence` (high / medium / low)
- `*_logan_adjudicated_flag` (BOOLEAN — which rows used a Logan-ratified rule for ambiguous cases)

Legacy stored cols (`t_stage_ajcc8`, `ajcc8_t_stage`, `dominant_tumor_ajcc8_t_stage`, etc.) remain unchanged. Manuscript SQL prefers the `*_resolved` cols going forward; legacy retained for audit/comparison.

---

## Required scope

### §1 Full AJCC7/8 rule survey

For each AJCC7/8 component (T, N, M, stage group), produce a verbatim derivation spec:

**T8 (AJCC8):**
- T1a: ≤1.0 cm AND no ETE
- T1b: 1.0 cm < size ≤ 2.0 cm AND no ETE
- T2: 2.0 cm < size ≤ 4.0 cm AND no ETE
- T3a: size > 4.0 cm, confined to thyroid, NO ETE
- T3a (alternate): size ≤ 4.0 cm WITH gross ETE into strap muscles (sternothyroid, sternohyoid, omohyoid, thyrohyoid)
- T3b: gross ETE into strap muscles regardless of size
- T4a: gross ETE into subcutaneous, larynx, trachea, esophagus, recurrent laryngeal nerve
- T4b: gross ETE into prevertebral fascia, mediastinal vessels, encasing carotid

**N8 (AJCC8):**
- N0: no nodal metastases (or `ln_involved=0` with `ln_examined>0`)
- N0a: 1+ cytologically/histologically negative nodes
- N0b: no clinical/imaging evidence of nodal disease
- N1a: metastases to level VI (pretracheal, paratracheal, prelaryngeal/Delphian) OR upper mediastinal level VII
- N1b: metastases to unilateral, bilateral, or contralateral lateral cervical (levels I, II, III, IV, V) OR retropharyngeal nodes

**M8 (AJCC8):** M0 / M1 (distant metastases)

**Stage group (AJCC8 — DTC):**
- Age <55: I (any T, any N, M0); II (any T, any N, M1)
- Age ≥55: I (T1/T2, N0/Nx, M0); II (T1/T2, N1, M0 OR T3a/T3b, any N, M0); III (T4a, any N, M0); IVA (T4b, any N, M0); IVB (any T, any N, M1)

**Stage group (MTC):** different rules
**Stage group (ATC):** all stage IV

Encode each rule as SQL CASE expressions in `qc_framework_v1/migrations/184_r1_ajcc_derivation_skeleton_20260430.sql` (placeholder skeleton, not for execution).

### §2 Adjudication-gap enumeration

For each rule, identify the gap on live MD:

| Rule | Required input col(s) | Available on `canonical_path_malignant_events_v1`? | Adjudication needed? |
|---|---|---|---|
| T1/T2/T3a size cutoffs | `size_greatest_dimension_cm` | ✓ DOUBLE | size_unavailable: 601 events (review what to do) |
| T3a microscopic ETE | `extrathyroidal_extension` text + `gross_ete` BIGINT | ✓ messy 45 distinct values incl. `'Yes;'`, `'extesive'`, free-text descriptions | **YES — Logan adjudicates ETE-text → micro/gross/none mapping** (see r1a_ete_t_stage_upgrade_review.csv 213 rows) |
| T3b gross ETE strap muscles | needs muscle-specific evidence | ✗ NOT on path_malignant_events; only `gross_ete` BIGINT 0/1 | **YES — without muscle specificity, T3b can't distinguish from T4a; Logan picks rule (treat all gross ETE as T3b? or punt to T3a-or-T3b ambiguous bucket?)** |
| T4a (subcutaneous/larynx/trachea/esophagus/RLN) | needs anatomic-structure invasion | partially on `canonical_invasion_events_v1` per CF-mig179 | needs Logan rule for which invasion_type values map to T4a |
| T4b (prevertebral/mediastinal/carotid) | needs deep-structure invasion | likely sparse | needs Logan rule |
| N1a vs N1b (level VI vs lateral) | nodal level/location | ✗ NOT on `canonical_path_malignant_events_v1`; only `ln_involved`, `extranodal_extension`, `nodal_disease_positive_count` | **YES — 2,378 events with `n_stage_ajcc8='N1'` (a/b unspecified). Logan ratifies: defer all to N1 (do not split) OR use upstream `cervical_lymph_nodes` operative cols where available** |
| Stage group age cutoff | `age_at_surgery` (BIGINT on PM) | ✓ on PM, NOT on path_event | **YES — Logan ratifies whether to broadcast PM `age_at_surgery` to path-event grain or compute stage_group only at PM grain** |
| Stage group DTC vs MTC vs ATC | `primary_histology` | ✓ on path_event; PM has `histologic_types_all` | **YES — Logan ratifies the histology→tumor-class mapping (e.g., "MTC | PTC" mixed cases)** |

### §3 Reproduce mig_182 cross-source drift cohort against new derivation

Run **read-only**:

```sql
WITH derived AS (
  SELECT pm.research_id,
         <CASE expressions for t_stage_ajcc8_resolved> AS t_resolved,
         <CASE expressions for n_stage_ajcc8_resolved> AS n_resolved,
         <CASE expressions for stage_group_ajcc8_resolved> AS sg_resolved
  FROM main.canonical_patient_master pm
  -- joining path events as needed
)
SELECT
  COUNT(*) FILTER (WHERE pm.ajcc8_t_stage IS NOT NULL AND derived.t_resolved IS NOT NULL AND pm.ajcc8_t_stage IS DISTINCT FROM derived.t_resolved) AS t_stage_changes,
  COUNT(*) FILTER (WHERE pm.ajcc8_stage_group IS NOT NULL AND derived.sg_resolved IS NOT NULL AND pm.ajcc8_stage_group IS DISTINCT FROM derived.sg_resolved) AS sg_changes,
  COUNT(DISTINCT pm.research_id) FILTER (WHERE derived.t_resolved IS NULL) AS t_uncalculable_pts,
  ...
FROM main.canonical_patient_master pm
LEFT JOIN derived USING (research_id);
```

Surface: how many patients would shift T-stage / N-stage / M-stage / stage group under R1 derivation, and how many would become uncalculable.

### §4 Author placeholder skeleton SQL

`qc_framework_v1/migrations/184_r1_ajcc_derivation_skeleton_20260430.sql`:
- §0 pre-flight (cohort 10,871/10,871)
- §A pre-snapshot of any cols mig_184_apply will write to
- §B (PLACEHOLDER): ALTER TABLE add new `*_resolved` cols on `canonical_path_malignant_events_v1` and `canonical_patient_master`
- §C (PLACEHOLDER): UPDATE per derivation rule with `<TBD_LOGAN_RATIFIED_RULE_<N>>` placeholders
- §D (PLACEHOLDER): registry note appendix closing `CF-87-AJCC` on the 36 col rows
- §E (PLACEHOLDER): provenance row insert
- All UPDATE/ALTER blocks marked `-- LOGAN MUST RATIFY RULES BEFORE EXECUTION (see §2 adjudication gaps)`

Cursor authors the skeleton structure; the actual CASE expressions are filled in by a follow-up `mig_184_apply` lane after Logan ratifies the rules.

### §5 Audit/report

`qc_framework_v1/reports/mig_184_r1_ajcc_derivation_scoping_20260430.md`:
- §1 full rule spec (verbatim)
- §2 adjudication-gap table per rule
- §3 cross-source drift cohort under proposed R1 derivation (T/N/M/sg shift counts)
- §4 specific patient cohorts requiring Logan adjudication, exported to CSVs in `exports/mig184_r1_adjudication_20260430/`:
  - `r1a_ete_t_stage_upgrade_review.csv` (already exists, 213 rows — Cowork-generated)
  - `r1b_n1_unspecified_no_location.csv` (~2,378 events)
  - `r1c_size_unavailable_t_uncalculable.csv` (~601 events)
  - `r1d_t4_invasion_evidence_review.csv` (cohort sized by §2 enumeration)
  - `r1e_stage_group_age_cutoff_review.csv` (PM patients with age_at_surgery boundary cases ±2 years of 55)
- §5 recommendation: rule-by-rule "Logan-ratify or auto-map" disposition for the apply lane

### §6 Logan-adjudication CSV columns

Each adjudication CSV must have these columns at minimum:
- domain identifiers (research_id, surgery_episode_id, tumor_ordinal, etc.)
- relevant clinical inputs (size, ETE text, ETE flag, ln_involved, primary_histology, etc.)
- proposed_action (auto-derived recommendation)
- `logan_decision` (blank — for Logan to fill)
- `logan_notes` (blank — for Logan to fill)
- staging_source_note (audit context)

---

## Governance reminders

- **Read-only investigation only.** No `query_rw` against MotherDuck. No SQL apply.
- The placeholder skeleton SQL has explicit `-- LOGAN MUST RATIFY RULES BEFORE EXECUTION` markers; do not remove them.
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits.
- Surgical git add only.

---

## Deliverables

1. `qc_framework_v1/migrations/184_r1_ajcc_derivation_skeleton_20260430.sql` (placeholder skeleton, not for execution)
2. `qc_framework_v1/reports/mig_184_r1_ajcc_derivation_scoping_20260430.md` (full rule spec + adjudication-gap enumeration + cross-source drift)
3. `exports/mig184_r1_adjudication_20260430/r1b_n1_unspecified_no_location.csv`
4. `exports/mig184_r1_adjudication_20260430/r1c_size_unavailable_t_uncalculable.csv`
5. `exports/mig184_r1_adjudication_20260430/r1d_t4_invasion_evidence_review.csv`
6. `exports/mig184_r1_adjudication_20260430/r1e_stage_group_age_cutoff_review.csv`

Commit message: `qc: mig_184 R1 AJCC derivation rule scoping (CF-87-AJCC closure track) + 4 adjudication CSVs`

---

End of prompt.
