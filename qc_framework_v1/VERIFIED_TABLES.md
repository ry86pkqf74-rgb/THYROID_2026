# Verified Tables — Append-Only Log

This file is appended to whenever a table reaches `table_status = 'verified'` in
`main.canonical_table_signoff_registry_v1`. Each entry records the date, the
sign-off migration, and a one-line summary.

Order: chronological (newest at the bottom).

---

## Format

```
### YYYY-MM-DD — schema.table_name

- Columns: N_total (N_verified verified / N_na auto-skipped)
- Sign-off migration: qc_framework_v1/migrations/NN_table_signoff_<table>.sql
- Notes: ...
```

---

### 2026-04-28 — main.canonical_fna_events_v1 (PILOT)

- **Rows:** 8,050 (started 8,119 → -69 phantom rows removed across mig_66/67/69/76/78)
- **Columns:** 38 verified + 1 deferred carry-forward (was 40 pre-cleanup; dropped specimen_site_raw, subtype, is_index_fna; renamed pathology_diagnosis→fna_history, pathology_extended→fna_pathology_report; added fna_site)
- **Sign-off migration:** `qc_framework_v1/migrations/78_fna_pilot_table_signoff.sql`
- **Notes:**
  - Verification spanned mig_65 through mig_78 (14 migrations).
  - All source columns aligned 100% with `raw/FNAs 12_5_2025.xlsx > FNA Bethesda`.
  - Bethesda category aligned 100% with rescore overlay `raw/FNAs_Rescored_Long_Format.xlsx`.
  - All dates normalized to `MM/DD/YYYY` with 20YY rule (mig_68 + mig_69b bug-fix).
  - `fna_site` is a NEW Logan-curated column with vocabulary covering thyroid (left/right/isthmus/bilateral/unspecified/cyst), lymph node (laterality + region: neck / level_1..7 / paratracheal / supraclavicular / submandibular / mediastinal / central), parathyroid, neck_mass, midline_cyst.
  - `laterality` re-derived from `fna_site` (mig_77b) for 100% consistency.
  - Carry-forward: `days_to_surgery` deferred (cross-table; awaits canonical_operative_events_v1 verification).
  - Carry-forward (open): `bethesda_calculated_num` 1,450 rows differ from source `bethesda_raw` (intentional rescore overlay; verified vs `fna_bethesda_rescore_staging_v1` instead).

### 2026-04-28 — main.canonical_airway_invasion_events_v1

- **Rows:** 3,155 / 2,622 patients (started 6,054 → -2,899 mig_80 scope filter)
- **Columns:** 23 verified (7 manual_source_review + 1 mechanical_derivation_compare + 15 auto_no_source_counterpart)
- **Sign-off migration:** `qc_framework_v1/migrations/83_airway_invasion_table_signoff.sql`
- **Notes:**
  - Verification spanned mig_80 through mig_83 (4 migrations).
  - **mig_80** scope filter: dropped 1,458 ct_imaging + 1,401 HP + 38 DC_SUM + 2 phantom rows. Logan: "operative reports/synoptic/gross pathology/micro pathology. Not from any imaging."
  - **mig_81** Logan Rule A: 56 rows (positive subset, no full-thickness invasion, currently `unable_to_determine`) → `not_pT4a`. Plus 11077 (pathologist staging override → `not_pT4a` per findings rule) and 8614 (RLN sacrifice → `pT4a`).
  - **mig_82** CAP template-echo cleanup: 18 synoptic rows whose evidence was the AJCC pT4a checklist option text only (`Invading subcutaneous soft tissues, larynx, trachea, esophagus or recurrent laryngeal nerve (i.e., pT4a)`) had their individual finding columns reset to `unknown` while keeping `t4a_implication=pT4a` (pathologist's stage selection IS the call).
  - **mig_83** Step D sign-off: 138 remaining pT4a candidates accepted as-is by Logan.
  - Final positive subset (196 rows): 138 `pT4a` / 58 `not_pT4a` / 0 `unable_to_determine`.
  - Architectural rule established (memory: `feedback_findings_vs_staging.md`): findings columns are primary, staging columns must follow findings, never the inverse.
  - Carry-forward CF-1: 6017 synoptic — pT4a anchored on non-airway "extrathyroidal extension into fat" rather than airway findings. Future call: should airway invasion table exclude rows whose only pT4a evidence is non-airway?
  - Carry-forward CF-2: `t4a_implication` is currently a stored LLM column; per findings-vs-staging rule, future cleanup may convert it to a deterministic post-derivation.
  - Carry-forward CF-3: 17 `pathologist_call_only` rows (all anatomic findings = `unknown`, `t4a=pT4a`); downstream views may want an evidence_grade flag.
