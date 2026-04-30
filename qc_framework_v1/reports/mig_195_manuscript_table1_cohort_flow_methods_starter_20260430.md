# mig_195 — manuscript Table 1 + cohort flow + methods starter

**Date:** 2026-04-30  
**Lane:** mig_195 / `manuscript_table1_cohort_flow_methods_starter`  
**Batch:** `mig_195_manuscript_table1_cohort_flow_methods_starter_20260430`  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Posture:** READ-ONLY authoring in repo (no MotherDuck execute in this session).  
**Prompt:** `cursor_prompts/CURSOR_PROMPT_mig195_manuscript_table1_cohort_flow_methods_starter_20260430.md`  
**Author (template):** Logan Glosser <logan.glosser@gmail.com> (Cursor drafted SQL/MD)

`git fetch origin` was run; deliverables were added locally (prompt file not necessarily on `origin/main`).

---

## §1 Deliverables

| # | Path | Purpose |
|---|------|---------|
| 1 | `qc_framework_v1/manuscript/table_1_cohort_characteristics.sql` | Table 1 long-format SQL (analytic cohort = malignant CPM ∩ `canonical_path_malignant_events_v1`) |
| 2 | `qc_framework_v1/manuscript/table_1_cohort_characteristics.csv` | Placeholder — populate via `COPY` after MotherDuck run |
| 3 | `qc_framework_v1/manuscript/cohort_flow_diagram.sql` | Six-step CONSORT-style counts + analytic cross-check |
| 4 | `qc_framework_v1/manuscript/cohort_flow_diagram.csv` | Placeholder — populate via `COPY` after run |
| 5 | `qc_framework_v1/manuscript/methods_section_starter.md` | Methods starter (Logan voice pass) |
| 6 | `qc_framework_v1/reports/mig_195_manuscript_table1_cohort_flow_methods_starter_20260430.md` | This report |

Each artifact header includes `READY FOR LOGAN MANUSCRIPT REFINEMENT` (SQL `--` / MD HTML comment).

---

## §2 Conditional pre-flight (registry — run on MotherDuck)

If **any** of these return **0 rows**, defer population of CSVs and treat `*_resolved` / indeterminate landing / rollup semantics as **pre-apply**:

```sql
USE thyroid_canonical_publication_v1_0;

SELECT batch_id, COUNT(*) AS n_registry_hits
FROM main.canonical_column_verification_registry_v1
WHERE batch_id IN (
  'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430',
  'mig186b_apply_RD_niftp_exclusion_no_gate3_break_20260430',
  'mig185b_apply_rollup_only_patch_no_transaction_20260430',
  'mig_187_apply_RA_script366_extension_ratified_20260430'
)
GROUP BY 1
ORDER BY 1;
```

**Object existence:** `cohort_flow_diagram.sql` expects **`main.canonical_path_indeterminate_events_v1`** (created by **mig_186b**). Pre-mig_186b runs will error — intentional gate.

---

## §3 Logan-curatable placeholders (brackets / notes)

Methods starter **`methods_section_starter.md`** — replace or delete bracketed slots, including:

- `[start year]–[end year]`
- `[cite institutional IRB / registry charter]`
- `[N]`, `[N_verified]` table/column verification metrics (use live gate1/registry exports)
- Tiering explainer `[expand — Tier 1 raw / …]`
- Age / procedure inclusion text `[refine age rule]`, `[ Thyroid surgery / thyroid cancer care]`, `[site]`
- AJCC / T0 narrative `[internal rule memo / AJCC citation]`
- Histology ladder citation
- Statistical add-ons `[Schoenfeld / log-log]`, `[restricted mean survival time]`, `[Additional methods placeholders]`

Table 1 SQL — optional refinements:

- **Race:** currently raw `race` strings; collapse to analytic groups per institutional IRB categories.
- **Time to contact:** uses `DATE_DIFF` on `first_surgery_date` / `last_contact_date`; cast if TIMESTAMP/TEXT drift appears.
- **Vital:** `COALESCE(s.vital_status_current, c.vital_status)` — confirm CPM column presence on live MD.

Cohort flow — align **step 6** analytic **N** with **Table 1 denominator**; if mismatch, document whether exclusions 4–5 apply to manuscript or only sensitivity.

---

## §4 Unblocking checklist (Logan)

- [ ] Confirm mig_188b / mig_186b / mig_185b / mig_187 applied (registry probe §2).
- [ ] Run `table_1_cohort_characteristics.sql`; export CSV; spot-check denominator.
- [ ] Run `cohort_flow_diagram.sql`; reconcile step 6 vs Table 1 **N**.
- [ ] Pass methods starter through authorship edit; attach IRB / registry / AJCC citations.
- [ ] Move data-quality bullets into supplement / appendix; cite CF IDs from registry.

---

_End report._
