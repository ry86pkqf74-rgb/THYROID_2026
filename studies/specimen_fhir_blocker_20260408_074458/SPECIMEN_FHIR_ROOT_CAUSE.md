# Specimen / FHIR — root cause (THYROID_2026 session)

**Study folder:** `studies/specimen_fhir_blocker_20260408_074458/`  
**UTC probe:** 2026-04-08 (see logs for exact timestamps)

## Executive summary

There is **no live specimen/FHIR logic defect** on **production** MotherDuck: `119_md_formalization_validate.py --md --md-env prod --release-mode` completes with **Check 13 PASS** (clean `v_diag_*` aggregates, **`broken_fhir_refs=0`**). The historical **large `broken_fhir_refs`** outcome preserved under `studies/20260407_publication_signoff_live/` reflects **stale FHIR JSON vs current tail DDL** (deployment drift), remediated by rebuilding the analytic FHIR layer (138) and diagnostic surfaces (142/143), as already recorded in repo memos.

On **QA** (`Thyroid 2026 Molecular QA 20260407`), **`main.synoptic_tumor_long_v1` is absent** (scoped `information_schema` count = **0**). Validator **Check 13** therefore **skips** specimen/FHIR gates entirely. Any perception of a “specimen blocker” on QA is a **missing prerequisite / incomplete sandbox** issue, not a bug in 138/140/142/143 SQL or Python.

## What 119 checks for specimen/FHIR (exact sources)

From `scripts/119_md_formalization_validate.py`:

1. **Prerequisite:** `main.synoptic_tumor_long_v1` must exist; else Check 13 **PASS** with message that checks are **skipped**.
2. **Tables:** `SPECIMEN_FHIR_OBJECTS` — `specimen_master_v1`, `specimen_tumor_focus_v1`, `specimen_genomic_assay_v1`, `specimen_source_xref_v1`, `fhir_patient_deid_map_v1`, `fhir_specimen_v1`, `fhir_procedure_collection_v1`, `fhir_encounter_v1`, `fhir_episode_of_care_v1`, `fhir_bundle_specimen_export_v1`.
3. **Uniqueness:** `specimen_master_v1.specimen_fingerprint_sha256` all distinct.
4. **Contract validation tables (qa):** `val_specimen_contract_v1`, `val_specimen_genomic_binding_v1` — any `status = FAIL` fails in release mode.
5. **Diagnostic views (142):**  
   `v_diag_specimen_duplicate_master_fp_v1`, `v_diag_specimen_duplicate_focus_fp_v1`, `v_diag_specimen_orphan_focus_master_v1`, `v_diag_specimen_orphan_genomic_focus_v1`, `v_diag_specimen_orphan_genomic_master_v1`, `v_diag_specimen_fhir_broken_refs_v1`, `v_diag_specimen_provenance_master_v1`, `v_diag_specimen_provenance_focus_v1`, `v_diag_specimen_provenance_genomic_v1`, `v_diag_specimen_review_burden_v1`  
   plus aggregate table `t_diag_specimen_focus_qa_metrics_v1`.
6. **Release FAIL** when the **sum** of duplicate/orphan/broken-FHIR/provenance gap metrics **> 0**, or when 142 objects are missing while specimen tables exist. **Metrics mismatch** (table vs views) → **WARN**. **Review burden** (`v_diag_specimen_review_burden_v1`, merge queue) → **WARN** only.

## Live counts (prod reference)

See `prod_diag_counts_reference.csv` (read-only query against **`Thyroid 2026`**):

- `synoptic_tumor_long_v1`: 11,103 rows  
- `specimen_master_v1`: 10,139 rows  
- All `v_diag_*` structural defect counts **0**; **`broken_fhir_refs=0`**  
- **Review burden:** `genomic_link_review` open/pending-style rollup **10,705** (WARN only, not FAIL)

## QA state

- **No** `synoptic_tumor_long_v1` in QA’s `main` for `current_database()` → specimen layer **not evaluated**.
- `119 --release-mode` on QA **still BLOCKED** due to **non–specimen** checks in this session (canonical parquet vs QA row mismatch, MRQ synthetic placeholder governance, promotion `decision_batch_id`, missing `molecular_testing` spine). See `119_qa_release_mode_console.log` and `qa_release_mode_validation_report.md`.

## Classification (user matrix)

| Hypothesis | Verdict |
|------------|---------|
| Diagnostics stale vs data | **No** on prod (142 surfaces match; clean counts). **N/A** on QA (prerequisite table missing). |
| Upstream layer not rebuilt | **Was** the historical `broken_fhir_refs` story; **not** current on prod. |
| Linkage / Python logic defect | **No** — offline tests **20/20 PASS** (`test_specimen_fhir_layer`, `test_specimen_fhir_qa_diagnostics`, `test_specimen_genomics_binding`, `test_specimen_identity_layer`). |
| Validator expectation drift | **No**; Check 13 aligns with `142_specimen_fhir_qa_diagnostics_ddl.sql` and docs. |

## Token / config probe

See `token_source_probe.txt`. **`motherduck.local.toml`** was **not** present; RW token resolved from **env** (length logged only, no values).

## MotherDuck env resolution note

If multiple scripts set `MOTHERDUCK_DATABASE` in one process, **`resolve_database_for_env` returns the env override** first — clear it between connects when switching QA ↔ prod (documented here for future probes).
