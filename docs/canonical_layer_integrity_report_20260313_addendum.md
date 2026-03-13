# Canonical Layer Integrity Report — Addendum (2026-03-13)

Generated: 2026-03-13T10:30:17.153738

## Canonical Table Fill Rates

| Table | Column | Filled | Total | % |
|-------|--------|-------:|------:|--:|

## Source Table Fill Rates

| Table | Column | Filled | Total | % |
|-------|--------|-------:|------:|--:|

## Assessment

Sparsity in canonical fields is primarily **source-limited**, not propagation-limited.
The resolved layer (`patient_analysis_resolved_v1`) queries sidecar tables directly,
so manuscript analyses are not affected by canonical-table sparsity.

## Operative Enhancement (Phase 2)


## Chronology Anomaly Classification (Phase 5)

Total anomalies: N/A

## Provenance Hardening (Phase 4)

All 4 analysis tables now have unified provenance columns:
`source_table`, `source_script`, `provenance_note`, `resolved_layer_version`