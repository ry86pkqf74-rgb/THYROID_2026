# MotherDuck optimize + export (live catalog)

**Run tag:** `20260407_0416` (UTC)  
**Commands:**

1. Dry-run: `.venv/bin/python lakehouse/motherduck_optimize.py --dry-run --v2-stage`
2. Live: `.venv/bin/python lakehouse/motherduck_optimize.py --sa --v2-stage --export-dir exports/motherduck_gold_manual_20260407_0416`

**Git at export:** `da9a441739a902cbc76195c0aaab99b3fdbb6f05` (from `manifest.json`)

## 1. Token source and target catalog

| Item | Value |
|------|--------|
| `token_mode()` (generic label) | `secrets.toml:MOTHERDUCK_TOKEN` |
| Effective token for `--sa` | Service-account order was used: `MD_SA_TOKEN` unset → fell back to `MOTHERDUCK_TOKEN` from `.streamlit/secrets.toml` (shell had `MOTHERDUCK_TOKEN` / `MD_SA_TOKEN` unset) |
| Target catalog | `Thyroid 2026` (`resolve_database_for_env("prod")`; `MOTHERDUCK_DATABASE` / `MOTHERDUCK_DB` not set) |

**Note:** `lakehouse/motherduck_optimize.py --dry-run` exits before opening a MotherDuck connection; it only prints the registry-derived whitelist and intended steps.

## 2. Tables and views touched (live run)

**Views (DDL on catalog `main`):**

- `gold_master_patient_facts_v1` → `CREATE OR REPLACE VIEW` over `patient_analysis_resolved_v1`
- `gold_master_episode_events_v1` → `CREATE OR REPLACE VIEW` over `episode_analysis_resolved_v1_dedup`

**Hydration:** No parquet hydration ran (no `[hydrate]` lines): `canonical_extracted_fact_long_v1` and `gold_llm_verified_facts` already existed.

**v2_stage entity tables (23):** `ANALYZE` attempted on each; **Parquet `COPY` export** succeeded for all that exist (all 23).

## 3. ANALYZE

On MotherDuck, every `ANALYZE` on the v2_stage tables failed with:

`Not implemented Error: Vacuum is only implemented for DuckDB tables`

The script treats this as a skip (non-fatal). **Result: zero tables successfully ANALYZE’d** for this run; all 23 were attempted then skipped with the above message.

## 4. Parquet exports

**Directory:** `exports/motherduck_gold_manual_20260407_0416/`

| File | Rows (from job log / manifest) |
|------|-------------------------------|
| `v2_stage__note_entities_llm_airway_invasion.parquet` | 11,037 |
| `v2_stage__note_entities_llm_cervical_ln_detail.parquet` | 11,037 |
| `v2_stage__note_entities_llm_dynamic_risk_response.parquet` | 11,037 |
| `v2_stage__note_entities_llm_frozen_section_detail.parquet` | 11,037 |
| `v2_stage__note_entities_llm_functional_outcomes.parquet` | 11,037 |
| `v2_stage__note_entities_llm_imaging.parquet` | 11,037 |
| `v2_stage__note_entities_llm_labs.parquet` | 11,037 |
| `v2_stage__note_entities_llm_parathyroid_detail.parquet` | 11,037 |
| `v2_stage__note_entities_llm_past_medical_hx.parquet` | 11,037 |
| `v2_stage__note_entities_llm_past_surgical_hx.parquet` | 11,037 |
| `v2_stage__note_entities_llm_pathology.parquet` | 11,037 |
| `v2_stage__note_entities_llm_patient_decision_adherence.parquet` | 11,037 |
| `v2_stage__note_entities_llm_physical_exam.parquet` | 11,037 |
| `v2_stage__note_entities_llm_presenting_symptoms.parquet` | 11,037 |
| `v2_stage__note_entities_llm_rad_treatment.parquet` | 11,037 |
| `v2_stage__note_entities_llm_rai_detailed.parquet` | 11,037 |
| `v2_stage__note_entities_llm_recurrence.parquet` | 11,037 |
| `v2_stage__note_entities_llm_survival_followup.parquet` | 11,037 |
| `v2_stage__note_entities_llm_synoptic_pathology_enrichment.parquet` | 11,037 |
| `v2_stage__note_entities_llm_tg_kinetics.parquet` | 11,037 |
| `v2_stage__note_entities_llm_tirads_granular.parquet` | 11,037 |
| `v2_stage__note_entities_llm_us_nodule_dynamics.parquet` | 11,037 |
| `v2_stage__note_entities_llm_vascular_invasion.parquet` | 11,037 |
| `provenance_llm_gold_note_linkage.parquet` | **127** (aggregated linkage rows) |
| `manifest.json` | metadata only |

## 5. `gold_llm_verified_facts` provenance

- Table was present; columns `research_id` and `note_row_id` satisfied the script’s information_schema check (otherwise the job would have exited with `[provenance]` error).
- Provenance export completed: **`provenance_llm_gold_note_linkage.parquet`** with **127** rows reported.
- **Interpretation:** the script’s provenance path completed without error (structural gate + export). There is no separate boolean “PASS” flag in the tool; success is implicit in completion and non-empty export consistent with upstream data.

## 6. Artifact index

- Export bundle: `exports/motherduck_gold_manual_20260407_0416/`
- This report: `studies/md_optimize_20260407_0416/report.md`
