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
