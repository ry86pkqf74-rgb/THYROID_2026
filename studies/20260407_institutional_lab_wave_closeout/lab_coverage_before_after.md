# Lab coverage — before / after institutional wave

Measurements target MotherDuck database **Thyroid 2026** (`main` schema).

## Table-level (longitudinal layers)

| Object | Before append | After append |
|--------|---------------|-------------|
| `main.longitudinal_lab_canonical_v1` row count | 76,971 | 77,960 |
| `main.longitudinal_lab_deduped_v` row count | 55,210 | 56,198 |

Delta: **+989** canonical rows for wave `final_institutional_20260407`; **+988** deduped rows (one row collapsed vs canonical due to deterministic dedup key).

## Distinct patients (current / after)

| Metric | After append |
|--------|--------------|
| Distinct `research_id` in `longitudinal_lab_canonical_v1` | 3,690 |
| Distinct `research_id` in `longitudinal_lab_deduped_v` | 3,690 |

(Before append, cohort size was already 3,690 patients on the canonical table; the wave adds coverage depth and new time points rather than new patients in this slice.)

## Panel coverage on `main.longitudinal_lab_deduped_v` (after ingest)

Distinct patients with at least one deduped row per analyte:

| Analyte | Patients |
|---------|----------|
| TSH | 413 |
| PTH | 184 |
| Calcium | 166 |
| Vitamin D | 82 |

These counts reflect post-ingest deduplication and wave priority (`final_institutional%` ranks ahead of Tg/legacy waves in `longitudinal_lab_deduped_v`).

## Release snapshot parity

`release_20260411.longitudinal_lab_canonical_v1` row count matched `main` at snapshot time (**77,960** rows).
