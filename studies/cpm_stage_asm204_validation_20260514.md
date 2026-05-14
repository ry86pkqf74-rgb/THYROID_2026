# ASM204 scratch validation — 2026-05-14

**Scratch table:** `thyroid-canonical-pub-2026.pub_workspace.cpm_stage_asm204_20260514`  
**Build:** `CREATE OR REPLACE TABLE` from `studies/cpm_stage_asm204_20260514.sql` (BigQuery only).  
**Guardrails:** No writes to `pub_canonical.canonical_patient_master` or `pub_archive.*`.

## Cohort invariant

| Check | Result |
|-------|--------|
| `COUNT(*)` | **10,871** |
| `COUNT(DISTINCT research_id)` | **10,871** |

Same counts on predecessor spine `pub_workspace.patient_analysis_resolved_v1`.

## Column inventory vs plan

| Metric | Value |
|--------|-------|
| ASM204 output columns | **96** (matches Phase 1 plan ~96-column script **204** base assemble) |
| PAR (S2) columns | **146** |

ASM204 is **not** a column superset of PAR: script **204** projects the first wide **`canonical_patient_master_v1` grain** (renamed core fields, e.g. `path_tumor_size_cm` → `tumor_size_cm`, `ete_grade_final` → `ete_grade`) and left-joins **`pub_canonical`** feeders (diagnosis, recurrence, survival, molecular_tested).

## Column-set diff (informational)

- **Names present on ASM204 but not on PAR (53):** Canonical/assemble aliases and feeder-backed fields (e.g. `is_malignant`, `diagnosis_primary`, `recurrence_*`, `followup_days`, `molecular_tested_confirmed`, `preop_tirads_*`, `tumor_size_cm`, `ete_grade`, …).
- **Names on PAR not selected onto ASM204 (103):** Resolved-layer paths, eligibility flags, audit columns, and analysis-only fields (e.g. `resolved_layer_version`, `path_histology_raw`, `analysis_eligible_flag`, extra `op_*` aggregates) — **expected** for this stage.

**pub_archive checkpoint:** ASM204 has **no** historic archive twin per DAG; parity is vs **S2 PAR** (row gate) + **204 column count** expectation.

## BQ feeder schema notes (vs MotherDuck script `204_canonical_master_assembly.py`)

Live **`pub_canonical.canonical_survival_followup_v1`** uses `last_known_alive_date`, `days_from_first_surgery_to_last_contact`, etc. The SQL maps these to **`last_contact_date`**, **`followup_days`**, **`followup_years`** (days / 365.25), and sets **`followup_category`** to NULL.

**Update 2026-05-14:** After **mig_098** (BQ) + **mig_332** (MD), `canonical_recurrence_v1` carries `recurrence_histology` and `recurrence_evidence_source` again; **ASM204** selects them from `r.*` (no `CAST(NULL)` guard).

## Unexpected issues

None for row/column-count acceptance. Semantic parity vs historical MotherDuck `canonical_survival_followup_v1` shapes is a **known cross-engine difference** and should be tracked in the rebuild diff registry when value-level compares run.
