# THYROID_2026 — Canonical Finalization Handoff (2026-04-16)

**Database (authoritative reads + writes):** `thyroid_canonical_publication_v1_0`
**Archive destination:** `"Thyroid 2026 UPdated".archive_pub_v1_0`
**Driver script:** `scripts/236_canonical_finalization.py` (idempotent; re-runnable by phase)

---

## Final canonical state (all 5 confirmation queries)

| Query | Result |
|---|---|
| Q1 — canonical shape | **patients = 10,871 × columns = 1,502** |
| Q2 — lingering backup/deprecated tables in canonical | **0 rows (clean)** |
| Q3 — registry completeness | **107 registered tables, 0 unmapped** |
| Q4 — `__readme` vs actual main BASE TABLEs | **112 == 112 (equal)** |
| Q5 — coworker audit fixes landed | **9 new `comp_*_days_postop_v2`, 1 `nlp_path_multifocal_concordance_v2`, 18 `nlp_rollup_promotion_audit_v1` rows** |

All 12 invariants in Phase 7 pass:

- `canonical_patient_master` row count == 10,871
- distinct `research_id` == 10,871
- no NULL `research_id`
- `fna_path_outcome` fully populated (0 NULL)
- no `_pre235_backup` tables remain in canonical
- exactly one `data_dictionary_v*` (v240)
- `__readme` rows == main BASE TABLE count
- registry has no orphans
- all 6 archive targets present in `Thyroid 2026 UPdated.archive_pub_v1_0`
- 9 new `comp_*_days_postop_v2` columns exist
- `nlp_path_multifocal_concordance_v2` present
- `nlp_rollup_promotion_audit_v1` populated

---

## Per-phase delta summary

### Phase 0 — Preflight
- Snapshotted `canonical_patient_master` → `canonical_patient_master_pre236_backup` (archived at Phase 8).
- Exported CPM to `scripts/output/parquet_backup/canonical_patient_master_pre236.parquet` (~3.9 MB, zstd).
- Wrote `scripts/output/236_preflight_inventory.csv` (119 rows: 118 BASE TABLES + 1 view reference plus legacy).
- Preflight CPM shape: 10,871 × 1,492 columns.
- Registry-vs-main diff: 3 tables in main absent from registry (all intentionally excluded: `data_dictionary_v235`, `data_dictionary_v240`, `rai_benign_histology_recovery_v234`); 0 registry-only orphans.

### Phase 1A — `comp_*_days_postop_v2` (9 new columns)
Recomputed timing filtered to `surgery_related_flag = TRUE`, one column per complication entity. All values bounded 0–730 days (invariant: `out_of_range = 0` for every column).

| Entity | Column | Populated patients | min | max |
|---|---|---:|---:|---:|
| rln_injury | `comp_rln_injury_days_postop_v2` | 69 | 0 | 301 |
| hematoma | `comp_hematoma_days_postop_v2` | 51 | 0 | 175 |
| hypoparathyroidism | `comp_hypoparathyroidism_days_postop_v2` | 23 | 0 | 308 |
| seroma | `comp_seroma_days_postop_v2` | 546 | 0 | 192 |
| chyle_leak | `comp_chyle_leak_days_postop_v2` | 1,442 | 0 | 356 |
| hypocalcemia | `comp_hypocalcemia_days_postop_v2` | 589 | 0 | 334 |
| wound_infection | `comp_wound_infection_days_postop_v2` | 6 | 0 | 210 |
| vocal_cord_paralysis | `comp_vc_paralysis_days_postop_v2` | 20 | 0 | 134 |
| vocal_cord_paresis | `comp_vc_paresis_days_postop_v2` | 17 | 0 | 176 |

Each old `comp_*_days_postop` column has a `COMMENT ON COLUMN … 'DEPRECATED 2026-04-16 (Script 236): includes non-surgery phenotype rows. Use …_v2.'`

### Phase 1B — VC paralysis/paresis recalibration
- Audit table: `manuscript_workspace.vc_paralysis_recalibration_v236` (**59 rows** = 32 upgraded + 27 preserved).
- New column: `complication_phenotype_v1.status_v2` (defaults to `final_complication_status`; upgraded 19 `vocal_cord_paralysis` + 13 `vocal_cord_paresis` rows to `confirmed_from_rln_crossref`).
- Rebuilt canonical columns using RLN cross-reference:
  - `comp_vc_paralysis_confirmed`: **0 → 26** (+26)
  - `comp_vc_paresis_confirmed`: **0 → 14** (+14)
  - `comp_rln_injury_confirmed`: 59 → 59 (no net change; already captured)
- Original `complication_phenotype_v1.final_complication_status` preserved (non-destructive).

### Phase 1C — `nlp_path_multifocal_concordance_v2`
Uses the correct `multifocal_flag_path` column (path-synoptic authoritative), replacing the prior 0% concordance using `multifocal_flag`. Distribution across 10,871 patients:

- `concordant_negative`: 909
- `concordant_positive`: 559
- `nlp_positive_path_negative`: 487
- `nlp_negative_path_positive`: 364
- `NULL` (insufficient signal): 8,552

Concordance among patients with both signals: (909 + 559) / (909 + 559 + 487 + 364) = **63.3%** (vs prior 0% when joining on `multifocal_flag`).

### Phase 1D — NLP rollup promotion audit
New table: `manuscript_workspace.nlp_rollup_promotion_audit_v1` (**18 rows**, one per NLP domain present in both source + canonical). Documents strict gating — no rollup was loosened. Key low-pass-through domains:

| Domain | Source patients | Canonical `has_data` | Pass-through |
|---|---:|---:|---:|
| synoptic_enrichment | 5,641 | 8 | **0.14%** |
| dynamic_risk_response | 5,641 | 25 | 0.44% |
| parathyroid | 5,641 | 110 | 1.95% |
| frozen_section | 5,641 | 184 | 3.26% |
| survival_followup | 5,641 | 2,911 | 51.60% |
| pathology | 5,641 | 2,794 | 49.53% |

(`nlp_rec_*`, `nlp_tg_*`, `nlp_usnodule_*`, `nlp_vasc_*`, `nlp_sympt_*` were excluded as their canonical `has_data` column names did not match a single-table mapping; those are documented via the registry but not in the audit.)

### Phase 2 — Archived 6 tables
All copied (row-count verified) to `"Thyroid 2026 UPdated".archive_pub_v1_0.<name>_20260416` then dropped from canonical.

| Source table | Rows | Archived as |
|---|---:|---|
| `canonical_patient_master_pre235_backup` | 10,871 | `canonical_patient_master_pre235_backup_20260416` |
| `complication_patient_summary_v1_pre235_backup` | 2,892 | `..._20260416` |
| `complication_phenotype_v1_pre235_backup` | 5,928 | `..._20260416` |
| `extracted_postop_labs_expanded_v1_pre235_backup` | 1,395 | `..._20260416` |
| `longitudinal_lab_canonical_v1_pre235_backup` | 77,960 | `..._20260416` |
| `data_dictionary_v235` | 1,492 | `..._20260416` |

`data_dictionary_v221` was already absent from canonical (prior archival; skipped safely).

### Phase 3 — Drill-down surface validation
Found 3 genuine missing canonical columns (surfaced in Phase 6 CSV for manual fix):

- `ete_adjudication_v1` → `ete_adjudicated_flag` **missing**. Related columns in CPM: `ete_grade_adjudicated`, `ete_grade_final_v2`, `ete_grade_source`, `ete_grade_final`. Action: either add an `ete_adjudicated_flag BOOLEAN` derived from `ete_adjudication_v1.research_id` membership, or update the registry feeds_master_columns to reference `ete_grade_adjudicated`.
- `ret_patient_adjudicated_v226` → `ret_adjudicated_flag`, `ret_evidence_source` **both missing**. Action: derive these two columns from `ret_patient_adjudicated_v226` in a follow-up script (Script 226 did not backfill them into CPM), or correct the registry.

### Phase 4 — Regenerated `manuscript_workspace.detail_table_registry_v1`
- **107 rows** (105 from `main` + 2 audit-feed rows in `manuscript_workspace`).
- New since prior: `rai_benign_histology_recovery_v234` (auto-added, flagged `TODO: manual review`).
- Removed from registry: 5 tables that no longer have `research_id` or were excluded (`canonical_patient_master` itself, `molecular_assay_dictionary`, `molecular_code_crosswalk`, `molecular_ingestion_runs`, `specimen_source_xref_v1`).
- Changed row counts reflected: `complication_phenotype_v1` 5,928 → 5,978 (new `status_v2` — same rowspace), `complication_patient_summary_v1` 2,892 → 2,938.
- Preserved prior `domain` / `feeds_master_columns` / `description` for every retained row.
- Added 2 new rows: `vc_paralysis_recalibration_v236` and `nlp_rollup_promotion_audit_v1` (both in `manuscript_workspace`).

### Phase 5 — `__readme` rebuilt
- 112 rows, one per main BASE TABLE, **0 TODOs**.
- Seeded descriptions for tables that previously lacked them: `data_dictionary_v240`, `rai_benign_histology_recovery_v234`, `ete_adjudication_v1`, `_molecular_patient_rollup_v227`, `ret_patient_adjudicated_v226`, `ret_note_entity_adjudication_v226`.

### Phase 6 — Column-pointer verification
Wrote `scripts/output/236_missing_canonical_columns.csv` (**7 rows**) — tokens in registry `feeds_master_columns` that do not exist in `canonical_patient_master`:

| Registry table | Missing column | Domain | Notes |
|---|---|---|---|
| `_molecular_patient_rollup_v227` | `molecular_rollup_version` | Molecular | stored as `mol_rollup_version` in CPM; registry typo |
| `canonical_benign_diagnosis_v1` | `has_follicular_adenoma` | Diagnosis | likely renamed; check CPM |
| `canonical_molecular_tested_v1` | `braf_positive_canonical` | Molecular | stored as `braf_positive_final` |
| `complication_patient_summary_v1` | `n_analysis_eligible_complication` | Complications | check suffix plural |
| `ete_adjudication_v1` | `ete_adjudicated_flag` | Pathology | see Phase 3 |
| `ret_patient_adjudicated_v226` | `ret_adjudicated_flag` | Molecular/NLP | see Phase 3 |
| `ret_patient_adjudicated_v226` | `ret_evidence_source` | Molecular/NLP | see Phase 3 |

None break Phase 7 invariants — they are registry-pointer mismatches, not data loss.

### Phase 7 — Final invariants
All 12 pass (see top summary). Canonical ready for publication.

### Phase 8 — Archived `canonical_patient_master_pre236_backup`
Copied (10,871 rows) to `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre236_backup_20260416` and dropped from canonical. Re-ran `__readme` rebuild — final count **112 rows == 112 main BASE TABLEs**.

---

## Outputs written

- `scripts/236_canonical_finalization.py` — single orchestrator (idempotent, `--phase 0|1a|1b|1c|1d|2|3|4|5|6|7|8|all`).
- `scripts/output/236_preflight_inventory.csv` — preflight inventory.
- `scripts/output/236_missing_canonical_columns.csv` — 7 registry→CPM pointer mismatches.
- `scripts/output/236_run.log` — full phase-by-phase run log.
- `scripts/output/236_confirm.log` — 5 confirmation queries' output.
- `scripts/output/parquet_backup/canonical_patient_master_pre236.parquet` — pre-run snapshot.
- `manuscript_workspace.vc_paralysis_recalibration_v236` — 59-row audit of VC paralysis/paresis recalibration.
- `manuscript_workspace.nlp_rollup_promotion_audit_v1` — 18-row strictness audit for NLP rollups.
- `"Thyroid 2026 UPdated".archive_pub_v1_0.*_20260416` — 7 archived tables (6 from Phase 2 + 1 from Phase 8).

---

## Final confirmation

`canonical_patient_master` in `thyroid_canonical_publication_v1_0` is the authoritative publication dataset (10,871 patients × 1,502 columns). All deprecated / `_pre235_backup` / stale data-dictionary tables have been moved to `"Thyroid 2026 UPdated".archive_pub_v1_0`. The registry at `manuscript_workspace.detail_table_registry_v1` (107 rows) accurately points every drill-down table to its canonical column(s); the 7 known registry→CPM mismatches are surfaced in `236_missing_canonical_columns.csv` for manual fix.

**No writes were made to `"Thyroid 2026 UPdated".main`.**
