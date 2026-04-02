# Gold layer

Publication-ready, cohort-level, and dashboard-facing artifacts.

- **Primary locations:** [`exports/`](../exports/) (timestamped bundles, manuscript snapshots, validation exports), [`studies/`](../studies/) (hypothesis-specific tables and figures), manuscript views materialized in DuckDB (e.g. `manuscript_cohort_v1`), and frozen metric registries under `exports/manuscript_metric_registry_*` / project docs.
- **Policy:** Gold outputs must be reproducible from silver + scripts; never commit PHI. Prefer [`exports/`](../exports/) for release bundles and Zenodo alignment.
