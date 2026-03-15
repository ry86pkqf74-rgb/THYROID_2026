# Date & Source Provenance Hardening — Final Engineering Pass
> Updated: 2026-03-14 | Script: 82

---

## 1. Motivation

The manuscript data model uses a layered date-status taxonomy (from script 17 /
script 27) with confidence weights.  Three clinical domains had structural gaps
in date-coverage documentation:

| Domain | Pre-82 Gap |
|--------|-----------|
| Recurrence | 88.8% of recurrence flags had `unresolved_date` (no day-level date) |
| RAI receipt | ~59% of RAI episodes lacked `resolved_rai_date`; nuclear medicine notes absent |
| Molecular | Year-only dates and placeholder rows conflated across molecular sources |

Script 82 (`82_provenance_date_hardening.py`) creates four validation tables that
formally document coverage with `date_availability_class` and `completeness_tier`
columns, enabling reproducible monitoring in CI and the QA workbench.

---

## 2. Validation Tables Created

### `val_recurrence_provenance_v2`
Joins `extracted_recurrence_refined_v1` + `recurrence_risk_features_mv` +
`thyroglobulin_labs`.

| `date_availability_class` | Meaning | N |
|--------------------------|---------|---|
| `exact_source_date` | Confirmed event / exact date | ~54 |
| `biochemical_inflection_inferred` | Rising Tg >1.0 with calculable inflection date | ~168 |
| `tg_available_date_unknown` | Tg labs present but no inflection point | variable |
| `unresolved_date` | Recurrence flag set but no date source | ~1,764 |
| `no_recurrence` | Not flagged as recurrent | ~8,885 |

**Unresolved-date root cause**: `recurrence_risk_features_mv.recurrence_flag` is a
boolean without day-level date provenance.  Resolving these 1,764 records requires
a structured recurrence registry (follow-up clinic data or chart abstraction) — not
available in the current database.

### `val_rai_provenance_v2`
Joins `rai_treatment_episode_v2` + `extracted_rai_validated_v1`.

| `date_availability_class` | Meaning |
|--------------------------|---------|
| `exact_source_date` | `resolved_rai_date` is day-level from structured source |
| `inferred_note_date` | Date recovered from endocrine/DC note text |
| `coarse_anchor_date` | Surgery-anchored estimate |
| `no_date` | No anchor available |

**Dose `completeness_tier`**:
- `confirmed_with_dose`: 35 patients (4.1%)  
- `unconfirmed_with_dose`: 6 patients
- `linkage_failed_no_dose`: ~1,096 episodes  
- Root cause: Nuclear medicine reports are **not present** in `clinical_notes_long`.
  This is a first-class structural limitation documented in
  `docs/rai_structural_gap_maximization_20260313.md`.

### `val_molecular_provenance_v2`
Joins `molecular_test_episode_v2` + `patient_refined_master_clinical_v10`
(for braf_positive_final / ras_positive_final / tert).

| `completeness_tier` | Meaning |
|--------------------|---------|
| `day_level_structured` | Date from `fna_cytology.fna_date` or molecular_testing |
| `year_only_coarse` | Year-only date from molecular_testing or genetic_testing |
| `placeholder_excluded` | Row is a placeholder stub (date='x', platform='x') |
| `no_date` | No date source available |

**Placeholder row count**: 8,799 of the 10,126 `molecular_test_episode_v2` rows are
stubs (detected via `is_placeholder_row`).  Only 1,327 rows represent real tests.

### `val_provenance_hardening_summary_v1`
Single-table cross-domain summary with one row per domain:

| domain | n_total | n_date_exact | n_date_inferred | n_date_unresolved | n_no_date | date_exact_pct | date_any_pct |
|--------|---------|-------------|----------------|------------------|----------|----------------|--------------|
| recurrence | ... | ... | ... | ... | ... | ... | ... |
| rai | ... | ... | ... | ... | ... | ... | ... |
| molecular | ... | ... | ... | ... | ... | ... | ... |

---

## 3. Coverage Before / After Hardening Pass

### Recurrence Domain
| Metric | Before | After |
|--------|--------|-------|
| Date coverage documented | No formal table | `val_recurrence_provenance_v2` |
| Exact-date patients | Not tracked | 54 (2.7%) — TTE-eligible |
| Biochemical-inferred patients | Not tracked | 168 (8.5%) — conditional TTE |
| Unresolved patients | Not tracked | 1,764 (88.8%) — structural limitation |

### RAI Domain
| Metric | Before | After |
|--------|--------|-------|
| Dose coverage | ~20-41% | Formally documented |
| Date provenance | Informal | `val_rai_provenance_v2` |
| Source-limitation flag | Absent | `val_rai_source_limitation_v1` |

### Molecular Domain
| Metric | Before | After |
|--------|--------|-------|
| Placeholder row distinction | `is_placeholder_row` flag only | Formally documented |
| BRAF/RAS/TERT coverage | Script 29 only | `val_molecular_provenance_v2` |

---

## 4. Date Precedence Policy (unchanged)

Per `utils/text_helpers.py` and script 46:

```
Tier 1 (confidence 1.0): specimen_collect_dt (thyroglobulin labs)
Tier 2 (confidence 0.9): entity_date (NLP-extracted date)
Tier 3 (confidence 0.7): note_date (note_date from clinical_notes_long)
Tier 4 (confidence 0.5): note_text_inferred_date (body text extraction — script 39)
Tier 5 (confidence 0.35-0.6): coarse_anchor_date (surgery / FNA / molecular fallback)
Tier 6 (confidence 0): unresolved_date (no source)
```

This policy is enforced in `longitudinal_lab_canonical_v1` and documented in
`data_dictionary.md` ("Date Association & Provenance Policy" section).

---

## 5. Run Instructions

```bash
.venv/bin/python scripts/82_provenance_date_hardening.py --md

# Re-materialize new val_* tables
.venv/bin/python scripts/26_motherduck_materialize_v2.py --md
```

Exports written to `exports/final_md_optimization_20260314/provenance_*.csv`.
