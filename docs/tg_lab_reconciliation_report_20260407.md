# Tg/TgAb Lab Reconciliation Report

**Generated**: 2026-04-07 01:06
**Script**: `scripts/113_tg_lab_ingestion.py`

## Canonical Layer State (Post-Reconciliation)

| Metric | Value |
|--------|-------|
| Total canonical rows | 76,971 |
| Deduped rows (clean view) | 55,210 |
| Superseded exact-match duplicates | 21,761 |
| Cross-wave value mismatches → review | 0 |

## Ingestion Waves

| Wave | Rows | Patients |
|------|------|----------|
| wave_tg_structured_ehr | 37,966 | 3,057 |
| wave_tgab_structured_ehr | 39,005 | 3,170 |

**Dedup rule**: When the same (research_id, lab_date, analyte, value) appears in
multiple waves, the structured EHR wave (`wave_tg_structured_ehr` /
`wave_tgab_structured_ehr`) is preferred over the older legacy waves
(`wave_1_structured_tg` / `wave_2_structured_anti_tg`) because it carries richer
metadata (assay method, disambiguation provenance, temporal linkage).

## Derived Views

### Tg Trajectory Summary (`tg_timeline_patient_summary_v1`)

| Metric | Value |
|--------|-------|
| Patients | 3,258 |
| Rising Tg flag | 474 |
| TgAb interference flag | 548 |

#### Trajectory Distribution

| Class | Count |
|-------|-------|
| suppressed | 1,426 |
| detectable_stable | 759 |
| low_stable | 549 |
| rising | 317 |
| insufficient_data | 207 |

### Postop Surveillance Windows (`tg_postop_surveillance_windows_v1`)

| Metric | Value |
|--------|-------|
| Window-rows | 16,184 |
| Patients | 3,250 |

### Recurrence-Surveillance Linkage (`tg_recurrence_surveillance_linkage_v1`)

| Metric | Value |
|--------|-------|
| Rising-Tg patients | 0 |
| Confirmed biochemical + structural | 0 |

| Linkage Class | Count |
|---------------|-------|
| (no recurrence table available) | — |

## Unresolved Issues

- 548 patients with TgAb interference (TgAb > 1.0 IU/mL) — Tg values may be unreliable

## Tables Created/Updated

| Table | Type | Purpose |
|-------|------|---------|
| `longitudinal_lab_canonical_v1` | TABLE | Append-only canonical (all waves) |
| `longitudinal_lab_deduped_v` | VIEW | Deterministic dedup across waves |
| `lab_cross_wave_dedup_map_v1` | TABLE | Superseded-row audit log |
| `lab_cross_wave_review_v1` | TABLE | Value mismatches for manual review |
| `tg_timeline_patient_summary_v1` | TABLE | Per-patient Tg/TgAb trajectory |
| `tg_postop_surveillance_windows_v1` | TABLE | Per-patient × temporal window |
| `tg_recurrence_surveillance_linkage_v1` | TABLE | Rising Tg ↔ structural recurrence |
| `thyroglobulin_lab_canonical_v1` | TABLE | Script-113 canonical (Tg-specific) |
| `tg_lab_review_queue_v1` | TABLE | Disambiguation review queue |
